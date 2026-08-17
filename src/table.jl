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
    nrows::Base.Int   # authoritative even with zero columns
end

function _table(names::Vector{Symbol}, columns::Vector{AbstractVector},
    schema, regions, nrows::Integer)
    lookup = Dict{Symbol,Base.Int}(nm => i for (i, nm) in enumerate(names))
    return Table(names, columns, lookup, schema, regions, Base.Int(nrows))
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
Tables.rowcount(t::Table) = getfield(t, :nrows)
Base.length(t::Table) = getfield(t, :nrows)

DataAPI.metadatasupport(::Type{Table}) = (read=true, write=false)
DataAPI.colmetadatasupport(::Type{Table}) = (read=true, write=false)

const _NO_DEFAULT = gensym(:nodefault)

function DataAPI.metadatakeys(t::Table)
    sch = getfield(t, :schema)
    (sch === nothing || sch.metadata === nothing) && return ()
    return (String(first(kv)) for kv in sch.metadata)
end
function DataAPI.metadata(t::Table, key::AbstractString,
    default=_NO_DEFAULT; style::Bool=false)
    sch = getfield(t, :schema)
    if sch !== nothing && sch.metadata !== nothing
        for kv in sch.metadata
            first(kv) == key && return style ? (last(kv), :default) : last(kv)
        end
    end
    default === _NO_DEFAULT && throw(KeyError(key))
    return style ? (default, :default) : default
end
function _schemafield(t::Table, col::Symbol)
    sch = getfield(t, :schema)
    sch === nothing && return nothing
    i = findfirst(f -> f.name == String(col), collect(sch.fields))
    return i === nothing ? nothing : sch.fields[i]
end
_colsymbol(t::Table, col::Symbol) = col
_colsymbol(t::Table, col::Base.Int) = getfield(t, :names)[col]
function DataAPI.colmetadatakeys(t::Table, col::Union{Symbol,Base.Int})
    f = _schemafield(t, _colsymbol(t, col))
    (f === nothing || f.metadata === nothing) && return ()
    return (String(first(kv)) for kv in f.metadata)
end
DataAPI.colmetadatakeys(t::Table) =
    (nm => DataAPI.colmetadatakeys(t, nm) for nm in getfield(t, :names)
     if !isempty(DataAPI.colmetadatakeys(t, nm)))
function DataAPI.colmetadata(t::Table, col::Union{Symbol,Base.Int},
    key::AbstractString, default=_NO_DEFAULT; style::Bool=false)
    f = _schemafield(t, _colsymbol(t, col))
    if f !== nothing && f.metadata !== nothing
        for kv in f.metadata
            first(kv) == key && return style ? (last(kv), :default) : last(kv)
        end
    end
    default === _NO_DEFAULT && throw(KeyError(key))
    return style ? (default, :default) : default
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

# The Julia element type a Field materializes as at the facade — a CLOSED
# mapping from the descriptor (the schema authority), never from observed
# values: an all-missing nullable Utf8 column is Vector{Union{Missing,
# String}}, a zero-row Int64 column is Vector{Int64}.
function _facadebasetype(t::AC.ArrowType)
    t isa AC.DateType &&
        return t.unit == AC.DAY ? Dates.Date : Dates.DateTime
    if t isa AC.TimestampType
        return t.unit == AC.SECOND || t.unit == AC.MILLISECOND ?
            Dates.DateTime : Int64
    end
    t isa AC.TimeType && return Dates.Time
    if t isa AC.DurationType
        return t.unit == AC.SECOND ? Dates.Second :
            t.unit == AC.MILLISECOND ? Dates.Millisecond :
            t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    end
    t isa AC.DictionaryType && return _facadebasetype(t.valuetype)
    t isa AC.IntType && return AC.juliatype(t)
    t isa AC.FloatType && return AC.juliatype(t)
    t isa AC.BoolType && return Bool
    t isa AC.Utf8Type && return String
    (t isa AC.ViewType && t.utf8) && return String
    return Any
end
_facadeeltype(f::AC.Field) = f.nullable ?
    Union{Missing,_facadebasetype(f.type)} : _facadebasetype(f.type)

function _facadecolumn(f::AC.Field, parts::Vector)
    T = _facadeeltype(f)
    isempty(parts) && return T === Any ? Any[] : Vector{T}()
    col = length(parts) == 1 ? parts[1] : reduce(vcat, parts)
    converted = _postconvert(f.type, col)
    # materialize returns Vector{Any} (typed zero-copy views are ViewPlan's,
    # later); the FIELD decides the public eltype.
    return T === Any ? map(identity, converted) : collect(T, converted)
end

# --- scan value domain -------------------------------------------------------
# Pushdown and residual filtering run over PHYSICAL storage values; facade
# filter literals arrive in public Julia types. Lower every literal to the
# referenced field's storage domain BEFORE the scan, so file, ranged, and
# stream paths share one value domain; conversion back to public types then
# happens exactly once, on the scan OUTPUT (rename-aware via Tables.bind).

function _storagevalue(t::AC.ArrowType, v)
    t isa AC.DictionaryType && return _storagevalue(t.valuetype, v)
    if t isa AC.DateType && v isa Dates.Date
        t.unit == AC.DAY && return Int32(Dates.value(v) - _EPOCH_DAYS)
        return Int64(Dates.value(Dates.DateTime(v)) - Dates.UNIXEPOCH)
    end
    if v isa Dates.DateTime
        ms = Int64(Dates.value(v) - Dates.UNIXEPOCH)
        t isa AC.DateType && t.unit == AC.MILLISECOND && return ms
        if t isa AC.TimestampType
            t.unit == AC.MILLISECOND && return ms
            t.unit == AC.SECOND && return _exactdiv(ms, 1_000, v, "SECOND")
            t.unit == AC.MICROSECOND && return ms * Int64(1_000)
            return ms * Int64(1_000_000)
        end
    end
    if t isa AC.TimeType && v isa Dates.Time
        ns = Int64(Dates.value(v))
        t.unit == AC.NANOSECOND && return ns
        t.unit == AC.MICROSECOND && return _exactdiv(ns, 1_000, v, "MICROSECOND")
        t.unit == AC.MILLISECOND &&
            return _exactdiv(ns, 1_000_000, v, "MILLISECOND")
        return _exactdiv(ns, 1_000_000_000, v, "SECOND")
    end
    if t isa AC.DurationType && v isa Dates.Period
        target = t.unit == AC.SECOND ? Dates.Second :
            t.unit == AC.MILLISECOND ? Dates.Millisecond :
            t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
        return Int64(Dates.value(convert(target, v)))
    end
    return v
end

function _exactdiv(x::Int64, d::Integer, v, unit::String)
    q, r = divrem(x, Int64(d))
    r == 0 || throw(ArgumentError(
        "filter literal $v is not representable in the column's $unit unit"))
    return q
end

function _fieldfor(fields, ref, names)
    ref isa Base.Int && 1 <= ref <= length(fields) && return fields[ref]
    i = findfirst(==(Symbol(ref)), names)
    return i === nothing ? nothing : fields[i]
end

function _lowerexpr(e, fields, names)
    e === nothing && return nothing
    if e isa Tables.Cmp
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return e
        return Tables.Cmp(e.op, e.lhs, _storagevalue(f.type, e.rhs))
    elseif e isa Tables.In
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return e
        return Tables.In(e.lhs,
            Tuple(_storagevalue(f.type, v) for v in e.values))
    elseif e isa Tables.AndExpr
        return Tables.AndExpr(
            Tables.ScanExpr[_lowerexpr(a, fields, names) for a in e.args])
    elseif e isa Tables.OrExpr
        return Tables.OrExpr(
            Tables.ScanExpr[_lowerexpr(a, fields, names) for a in e.args])
    elseif e isa Tables.NotExpr
        return Tables.NotExpr(_lowerexpr(e.arg, fields, names))
    end
    return e
end

function _lowerscan(scan::Tables.Scan, fields)
    scan.filter === nothing && return scan
    names = Symbol[Symbol(f.name) for f in fields]
    return Tables.Scan(scan.select, _lowerexpr(scan.filter, fields, names),
        scan.limit, scan.offset, scan.validate)
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
        # One extra tail fetch buys the schema up front: literal lowering,
        # exactly-once output conversion, and DataAPI metadata all need it.
        sch, rfields = rangedschema(rf)
        theScan = scan === nothing ? Tables.Scan() : scan
        got = Tables.scan(rf, _lowerscan(theScan, rfields))
        return _wrapscanned(got, sch, rfields, theScan)
    end
    src = _opensource(source; mmap=mmap)
    regions = _sourceregions(src)
    fields = _corefields(src)
    if scan !== nothing && src isa ArrowFile
        got = Tables.scan(src, _lowerscan(scan, fields))
        return _wrapscanned(got, src.schema, fields, scan; regions=regions)
    end
    scan === nothing && return _materialize_table(src, regions)
    # Stream format: decode RAW columns, scan in the storage domain, then
    # convert the output once — the same value domain as the pushdown paths.
    names = Symbol[Symbol(f.name) for f in fields]
    raw = NamedTuple{Tuple(names)}(Tuple(_rawcolumn(src, i)
        for i = 1:length(fields)))
    got = Tables.finish(raw, _lowerscan(scan, fields))
    return _wrapscanned(got, _tableschema(src), fields, scan; regions=regions)
end

_corefields(s::IPCStream) = collect(AC.Field, s.corefields)
_corefields(f::ArrowFile) = collect(AC.Field, f.fields)

_rawcolumn(s::IPCStream, i::Base.Int) = begin
    parts = [materialize(s.corefields[i], b.columns[i]) for b in s.batches]
    isempty(parts) ? Any[] : reduce(vcat, parts)
end

_tableschema(s::IPCStream) = s.schema
_tableschema(f::ArrowFile) = f.schema

function _materialize_table(src::IPCStream, regions)
    names = Symbol[Symbol(f.name) for f in src.schema.fields]
    cols = AbstractVector[
        _facadecolumn(f, [materialize(f, b.columns[i]) for b in src.batches])
        for (i, f) in enumerate(src.corefields)]
    nrows = sum(Base.Int(b.nrows) for b in src.batches; init=0)
    return _table(names, cols, src.schema, regions, nrows)
end

function _materialize_table(src::ArrowFile, regions)
    names = Symbol[Symbol(f.name) for f in src.schema.fields]
    nb = length(src)
    batches = [src[i] for i = 1:nb]
    cols = AbstractVector[
        _facadecolumn(f, [materialize(f, b.columns[i]) for b in batches])
        for (i, f) in enumerate(src.fields)]
    nrows = sum(Base.Int(b.nrows) for b in batches; init=0)
    return _table(names, cols, src.schema, regions, nrows)
end

"Wrap a scan output (storage-domain columns) into a Table, converting once."
function _wrapscanned(got, schema, sourcefields, scan;
    regions=AC.OwnerRegion[])
    cols = Tables.columns(got)
    names = collect(Symbol, Tables.columnnames(cols))
    columns = AbstractVector[Tables.getcolumn(cols, nm) for nm in names]
    # The bound selection maps each OUTPUT column to its SOURCE field —
    # renames and positional references included — so conversion and the
    # public eltype are schema-driven even for renamed output.
    if scan !== nothing && !isempty(sourcefields)
        b = Tables.bind(scan, Symbol[Symbol(f.name) for f in sourcefields])
        for (i, bc) in enumerate(b.columns)
            i <= length(columns) || break
            f = sourcefields[bc.index]
            converted = _postconvert(f.type, columns[i])
            T = bc.type === nothing ? _facadeeltype(f) : bc.type
            columns[i] = T === Any ? map(identity, converted) :
                collect(T, converted)
        end
    end
    nrows = isempty(columns) ? _scanrowcount(got) : length(columns[1])
    return _table(names, columns, schema, AC.OwnerRegion[regions...], nrows)
end

_scanrowcount(got) = Base.Int(Tables.rowcount(Tables.columns(got)))

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
    return _table(names, cols, _tableschema(s.src), s.regions,
        Base.Int(b.nrows)), i + 1
end

Tables.partitions(s::Stream) = s
