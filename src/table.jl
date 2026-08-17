# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# =============================================================================
# The read facade: Arrow IPC -> Tables.jl columns.
#
# `Arrow.Table` materializes the selected columns into plain Julia vectors
# (the zero-copy typed-view layer, ViewPlan, is designed but deliberately
# deferred until the benchmark suite justifies its composite-eltype choice;
# it will slot in behind this same API). `Arrow.Stream` iterates record
# batches as one Table each. Scan pushdown routes through the ranged-scan
# adapter: on the file format, column pruning, statistics-based batch
# pruning, and window consumption all happen before decode.
# =============================================================================

"""
    Arrow.Table(source; scan=nothing, mmap=true) -> Table

Read Arrow IPC data as Tables.jl columns. `source` is a file path, an `IO`,
raw bytes (`Vector{UInt8}`), or an `Arrow.RangedSource` (byte-range reads —
see its docs). Both IPC formats are accepted: the file format (`ARROW1`
magic, random access, footer statistics) and the stream format.

`scan` is a `Tables.Scan` pushdown request: selected columns are the only
ones decoded, footer statistics prune batches no row of which can match the
filter, and exact limit/offset windows skip whole batches. On the file
format (and ranged sources) pruning happens before bytes are fetched or
decoded; on the stream format the scan is applied after decode.

Columns are materialized (plain `Vector`s): the returned table does not
borrow the source bytes, and [`Arrow.close!`](@ref) may be called at any
time afterward to release a memory-mapped file deterministically — do this
on Windows before deleting a mapped file.
"""
struct Table <: Tables.AbstractColumns
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol,Base.Int}
    schema::Union{Nothing,AC.Schema}
    regions::Vector{AC.OwnerRegion}
end

function _table(names::Vector{Symbol}, columns::Vector{AbstractVector},
    schema, regions)
    lookup = Dict{Symbol,Base.Int}(nm => i for (i, nm) in enumerate(names))
    return Table(names, columns, lookup, schema, regions)
end

Tables.istable(::Type{Table}) = true
Tables.columnaccess(::Type{Table}) = true
Tables.columns(t::Table) = t
Tables.columnnames(t::Table) = getfield(t, :names)
Tables.getcolumn(t::Table, i::Base.Int) = getfield(t, :columns)[i]
Tables.getcolumn(t::Table, nm::Symbol) =
    getfield(t, :columns)[getfield(t, :lookup)[nm]]
Tables.schema(t::Table) = Tables.Schema(getfield(t, :names),
    [eltype(c) for c in getfield(t, :columns)])
Base.propertynames(t::Table) = getfield(t, :names)
Base.getproperty(t::Table, nm::Symbol) = Tables.getcolumn(t, nm)

DataAPI.metadatasupport(::Type{Table}) = (read=true, write=false)
DataAPI.colmetadatasupport(::Type{Table}) = (read=true, write=false)
function DataAPI.metadatakeys(t::Table)
    sch = getfield(t, :schema)
    (sch === nothing || sch.metadata === nothing) && return ()
    return (String(first(kv)) for kv in sch.metadata)
end
function DataAPI.metadata(t::Table, key::AbstractString; style::Bool=false)
    sch = getfield(t, :schema)
    sch === nothing || sch.metadata === nothing && throw(KeyError(key))
    for kv in sch.metadata
        first(kv) == key && return style ? (last(kv), :default) : last(kv)
    end
    throw(KeyError(key))
end
function _schemafield(t::Table, col::Symbol)
    sch = getfield(t, :schema)
    sch === nothing && return nothing
    i = findfirst(f -> f.name == String(col), collect(sch.fields))
    return i === nothing ? nothing : sch.fields[i]
end
function DataAPI.colmetadatakeys(t::Table, col::Symbol)
    f = _schemafield(t, col)
    (f === nothing || f.metadata === nothing) && return ()
    return (String(first(kv)) for kv in f.metadata)
end
function DataAPI.colmetadata(t::Table, col::Symbol, key::AbstractString;
    style::Bool=false)
    f = _schemafield(t, col)
    f === nothing || f.metadata === nothing && throw(KeyError(key))
    for kv in f.metadata
        first(kv) == key && return style ? (last(kv), :default) : last(kv)
    end
    throw(KeyError(key))
end

"""
    Arrow.close!(t::Union{Table,Stream})

Deterministically release the source regions behind a read (a memory map
unmaps NOW; imported foreign buffers run their release callbacks). `Table`
columns are materialized copies, so a closed `Table` remains fully usable;
a closed `Stream` refuses further iteration cleanly. Idempotent.
"""
function AC.close!(t::Table)
    foreach(AC.close!, getfield(t, :regions))
    return nothing
end

# --- Dates conversion (the facade owns what Core deliberately does not) ----

_mapcol(f::F, col) where {F} =
    eltype(col) >: Missing ?
    [x === missing ? missing : f(x) for x in col] : map(f, col)

_postconvert(::AC.ArrowType, col) = col
_postconvert(t::AC.DateType, col) = t.unit == AC.DAY ?
    _mapcol(x -> Dates.Date(Dates.UTD(Int64(x) + _EPOCH_DAYS)), col) :
    _mapcol(x -> Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH)), col)
function _postconvert(t::AC.TimestampType, col)
    # DateTime is millisecond-precision. Finer units stay as their raw
    # storage integers rather than silently truncating.
    t.unit == AC.SECOND &&
        return _mapcol(x -> Dates.DateTime(Dates.UTM(Int64(x) * 1000 +
            Dates.UNIXEPOCH)), col)
    t.unit == AC.MILLISECOND &&
        return _mapcol(x -> Dates.DateTime(Dates.UTM(Int64(x) +
            Dates.UNIXEPOCH)), col)
    return col
end
function _postconvert(t::AC.TimeType, col)
    scale = t.unit == AC.SECOND ? Int64(1_000_000_000) :
        t.unit == AC.MILLISECOND ? Int64(1_000_000) :
        t.unit == AC.MICROSECOND ? Int64(1_000) : Int64(1)
    return _mapcol(x -> Dates.Time(Dates.Nanosecond(Int64(x) * scale)), col)
end
function _postconvert(t::AC.DurationType, col)
    P = t.unit == AC.SECOND ? Dates.Second :
        t.unit == AC.MILLISECOND ? Dates.Millisecond :
        t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    return _mapcol(x -> P(Int64(x)), col)
end
_postconvert(t::AC.DictionaryType, col) = _postconvert(t.valuetype, col)

function _facadecolumn(f::AC.Field, parts::Vector)
    col = length(parts) == 1 ? parts[1] : reduce(vcat, parts)
    # materialize returns Vector{Any} (the typed zero-copy layer is
    # ViewPlan's, later); narrow to the natural concrete eltype so
    # downstream consumers see Vector{Int64}, Vector{Union{Missing,T}}, ...
    return _postconvert(f.type, map(identity, col))
end

# --- source opening ---------------------------------------------------------

const _FILE_MAGIC = b"ARROW1"

_isfilebytes(bytes::Vector{UInt8}) =
    length(bytes) >= 6 && view(bytes, 1:6) == _FILE_MAGIC

function _openbytes(bytes::Vector{UInt8})
    return _isfilebytes(bytes) ? readfile(bytes) : readstream(bytes)
end

function _opensource(path::AbstractString; mmap::Bool=true)
    magic = open(io -> Base.read(io, 6), path)
    if magic == _FILE_MAGIC && mmap
        return readfile(mmapregion(path))
    end
    return _openbytes(Base.read(path))
end
_opensource(io::IO; mmap::Bool=true) = _openbytes(Base.read(io))
_opensource(bytes::Vector{UInt8}; mmap::Bool=true) = _openbytes(bytes)
_opensource(src::Union{IPCStream,ArrowFile}; mmap::Bool=true) = src

"Distinct owner regions reachable from a source's decoded batches."
function _sourceregions(s::IPCStream)
    seen = IdDict{AC.OwnerRegion,Nothing}()
    function walk(d::AC.ArrayData)
        for b in d.buffers
            b.region === nothing || (seen[b.region::AC.OwnerRegion] = nothing)
        end
        foreach(walk, d.children)
        d.dictionary === nothing || walk(d.dictionary::AC.ArrayData)
    end
    for batch in s.batches, col in batch.columns
        walk(col)
    end
    return collect(keys(seen))
end
_sourceregions(f::ArrowFile) = AC.OwnerRegion[f.region]

# --- Table construction ------------------------------------------------------

function Table(source; scan::Union{Nothing,Tables.Scan}=nothing,
    mmap::Bool=true)
    if source isa RangedSource || source isa RangedFile
        rf = source isa RangedSource ? RangedFile(source) : source
        got = Tables.scan(rf, scan === nothing ? Tables.Scan() : scan)
        return _wrapscanned(got, nothing)
    end
    src = _opensource(source; mmap=mmap)
    regions = _sourceregions(src)
    if scan !== nothing && src isa ArrowFile
        got = Tables.scan(src, scan)
        return _wrapscanned(got, src.schema; regions=regions)
    end
    t = _materialize_table(src, regions)
    scan === nothing && return t
    return _wrapscanned(Tables.finish(t, scan), _tableschema(src))
end

_tableschema(s::IPCStream) = s.schema
_tableschema(f::ArrowFile) = f.schema

function _materialize_table(src::IPCStream, regions)
    names = Symbol[Symbol(f.name) for f in src.schema.fields]
    cols = AbstractVector[
        _facadecolumn(f, [materialize(f, b.columns[i]) for b in src.batches])
        for (i, f) in enumerate(src.corefields)]
    return _table(names, cols, src.schema, regions)
end

function _materialize_table(src::ArrowFile, regions)
    names = Symbol[Symbol(f.name) for f in src.schema.fields]
    nb = length(src)
    batches = [src[i] for i = 1:nb]
    cols = AbstractVector[
        _facadecolumn(f, [materialize(f, b.columns[i]) for b in batches])
        for (i, f) in enumerate(src.fields)]
    return _table(names, cols, src.schema, regions)
end

"Wrap a scan/finish result (plain columns) into a Table."
function _wrapscanned(got, schema; regions=AC.OwnerRegion[])
    cols = Tables.columns(got)
    names = collect(Symbol, Tables.columnnames(cols))
    columns = AbstractVector[Tables.getcolumn(cols, nm) for nm in names]
    # Post-convert temporal columns by matching scanned names to schema
    # fields (scan output may be a renamed/typed subset).
    if schema !== nothing
        byname = Dict(f.name => f for f in schema.fields)
        for (i, nm) in enumerate(names)
            f = get(byname, String(nm), nothing)
            f === nothing && continue
            columns[i] = _postconvert(f.type, columns[i])
        end
    end
    return _table(names, columns, schema, AC.OwnerRegion[regions...])
end

# --- Stream ------------------------------------------------------------------

"""
    Arrow.Stream(source; mmap=true)

Iterate an IPC source one record batch at a time; each iteration yields an
[`Arrow.Table`](@ref) for that batch. Satisfies `Tables.partitions`, so
`Arrow.write(sink, Arrow.Stream(...))` streams batch-per-batch, and works
directly with partition-aware sinks.
"""
struct Stream
    src::Union{IPCStream,ArrowFile}
    regions::Vector{AC.OwnerRegion}
end

Stream(source; mmap::Bool=true) = begin
    src = _opensource(source; mmap=mmap)
    Stream(src, _sourceregions(src))
end

AC.close!(s::Stream) = (foreach(AC.close!, getfield(s, :regions)); nothing)

_nbatches(s::IPCStream) = length(s.batches)
_nbatches(f::ArrowFile) = length(f)
_batch(s::IPCStream, i) = s.batches[i]
_batch(f::ArrowFile, i) = f[i]
_batchfields(s::IPCStream) = s.corefields
_batchfields(f::ArrowFile) = f.fields

Base.length(s::Stream) = _nbatches(s.src)
Base.eltype(::Type{Stream}) = Table

function Base.iterate(s::Stream, i::Base.Int=1)
    i > _nbatches(s.src) && return nothing
    b = _batch(s.src, i)
    fields = _batchfields(s.src)
    names = Symbol[Symbol(f.name) for f in fields]
    cols = AbstractVector[_facadecolumn(f, [materialize(f, b.columns[j])])
                          for (j, f) in enumerate(fields)]
    return _table(names, cols, _tableschema(s.src), s.regions), i + 1
end

Tables.partitions(s::Stream) = s
