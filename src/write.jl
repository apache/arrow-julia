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
# The write facade: Tables.jl source -> Arrow IPC bytes.
#
# This file owns partition binding, retained-field alignment, schema and batch
# assembly, compression selection, and IPC emission. Column construction is a
# deep module behind `_constructcolumn`.
# =============================================================================

"""
    Arrow.DictEncode(v)

Mark a column for dictionary encoding: the writer builds a category pool and
encodes slots as integer indices into it. Fresh pools coalesce exact Arrow
storage values. A retained rewrite may preserve unused or duplicate physical
categories because pool order and index meaning are part of the encoded data.
"""
struct DictEncode{T,V<:AbstractVector{T}} <: AbstractVector{T}
    data::V
end
Base.size(d::DictEncode) = size(d.data)
Base.getindex(d::DictEncode, i::Int) = d.data[i]

include("columnconstruction.jl")

"""
    Arrow.write(sink, table; file=true, compress=nothing,
                metadata=nothing, colmetadata=nothing)
    Arrow.write(sink; kwargs...)

Write any Tables.jl source as Arrow IPC. `sink` is a file path or an `IO`.
`file=true` emits the random-access file format (`ARROW1` magic + footer);
`file=false` the stream format. Each `Tables.partitions` partition becomes
one record batch. `compress` is `nothing`, `:lz4`, or `:zstd`.
`metadata`/`colmetadata` attach schema- and per-column key-value pairs
(a `Dict`, or pairs; `colmetadata` maps column name `Symbol`s to them).

Returns `sink`: the path for the path method, the `io` for the `IO` method.
The one-argument form curries for pipelines: `table |> Arrow.write(sink)`.

The writer is eager and whole-buffer: batches are encoded and validated in
memory, then written to the sink once.

Keywords the Arrow 2.x incremental writer accepted (`alignment`,
`dictencode`, `dictencodenested`, `denseunions`, `largelists`, `maxdepth`,
`ntasks`) are accepted with a one-time warning and ignored; wrap columns in
[`Arrow.DictEncode`](@ref) to dictionary-encode. See `docs/src/migration.md`.
"""
function write(path::AbstractString, tbl; kwargs...)
    bytes = _writebytes(tbl; kwargs...)
    open(path, "w") do io
        Base.write(io, bytes)
    end
    return path
end

function write(io::IO, tbl; kwargs...)
    bytes = _writebytes(tbl; kwargs...)
    Base.write(io, bytes)
    return io
end

write(sink::Union{AbstractString,IO}; kwargs...) = tbl -> write(sink, tbl; kwargs...)

"""
    Arrow.tobuffer(table; kwargs...)

Write `table` to a fresh `IOBuffer`, seeked to the start, in the IPC STREAM
format — the same bytes Arrow 2.x's `tobuffer` produced. Keyword arguments
are those of [`Arrow.write`](@ref) except `file`, which is `false` here.
"""
function tobuffer(tbl; kwargs...)
    io = IOBuffer()
    write(io, tbl; file=false, kwargs...)
    seekstart(io)
    return io
end

"""
    Arrow.Writer(sink; file=true, compress=nothing,
                 metadata=nothing, colmetadata=nothing)
    Arrow.Writer(f::Function, sink; kwargs...)

An incremental IPC writer: batches publish to `sink` as they are written,
so producing tables one at a time never holds more than the current table
in memory. `sink` is a file path (the writer opens and owns the handle) or
an `IO` (borrowed; `close` finishes the IPC output but leaves the `IO`
open). The function form runs `f(writer)` and always closes the writer.

    w = Arrow.Writer(path)
    for tbl in tables
        Arrow.write(w, tbl)
    end
    close(w)

The FIRST table written fixes the schema, with the same inference one
eager `Arrow.write` of that table would use; every later table must
conform to it (same column names and order, compatible types) or the
write is refused with the mismatch. Unlike the eager writer, no inference
crosses tables: a field is nullable iff the first table's column eltype
admits `Missing`, and later missing values under a non-nullable field are
refused. To pin a schema explicitly, write a zero-row table with fully
typed columns first.

Dictionary-encoded columns: the stream format (`file=false`) re-emits a
changed pool as a replacement dictionary batch. The file format carries
one dictionary batch per id, so every later table must produce the first
table's exact pool (the same categories in the same first-appearance
order); a changed pool is refused — use the stream format for changing
pools.

`close` finalizes what has been published — the sink is a valid IPC
output containing every batch written so far — and is idempotent. A
writer abandoned without `close` leaves a torn stream or an unfooted
file; closing a writer that never received a table just closes the sink
without producing valid IPC. One task owns a writer: overlapping calls
are not synchronized.
"""
mutable struct Writer
    const io::IO
    const ownio::Bool
    const file::Bool
    const compress::Symbol
    const metadata::Any
    const colmetadata::Any
    st::Union{Nothing,IPCWriteState}
    names::Vector{Symbol}
    schema::Union{Nothing,AC.Schema}
    @atomic closed::Bool
end

function _writer(
    io::IO,
    ownio::Bool;
    file::Bool=true,
    compress::Union{Nothing,Symbol}=nothing,
    metadata=nothing,
    colmetadata=nothing,
)
    codec = compress === nothing ? :none : compress
    if !haskey(CODEC_NAMES, codec)
        ownio && close(io)
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    end
    return Writer(
        io,
        ownio,
        file,
        codec,
        metadata,
        colmetadata,
        nothing,
        Symbol[],
        nothing,
        false,
    )
end

Writer(io::IO; kwargs...) = _writer(io, false; kwargs...)
Writer(path::AbstractString; kwargs...) = _writer(open(path, "w"), true; kwargs...)

function Writer(f::Function, sink::Union{AbstractString,IO}; kwargs...)
    w = Writer(sink; kwargs...)
    try
        return f(w)
    finally
        close(w)
    end
end

# The Arrow 2.x opening idiom: `open(Arrow.Writer, sink)` constructs the
# writer (the caller closes it), and the function form closes it after `f`.
Base.open(::Type{Writer}, sink::Union{AbstractString,IO}; kwargs...) =
    Writer(sink; kwargs...)
Base.open(f::Function, ::Type{Writer}, sink::Union{AbstractString,IO}; kwargs...) =
    Writer(f, sink; kwargs...)

Base.isopen(w::Writer) = !(@atomic w.closed)

"""
    Arrow.write(writer::Arrow.Writer, table)

Write one Tables.jl source through an incremental [`Arrow.Writer`](@ref):
each `Tables.partitions` partition becomes one record batch, published to
the sink before the call returns. Returns `writer`. See [`Arrow.Writer`](@ref)
for the schema rules.
"""
function write(w::Writer, tbl)
    (@atomic w.closed) && throw(ArgumentError("this Arrow.Writer is closed"))
    names, partcols, partpools, rowcounts = _collectparts(tbl)
    if w.st === nothing
        retained = _retainedschema(tbl)
        fields, coldata = _constructcolumns(
            names,
            partcols,
            partpools,
            _retainedfieldfn(retained, names),
            w.colmetadata,
        )
        schmeta =
            w.metadata !== nothing ? _metapairs(w.metadata) :
            (
                retained === nothing || retained.metadata === nothing ? nothing :
                collect(Pair{String,String}, retained.metadata)
            )
        sch = AC.Schema(fields; metadata=schmeta)
        st = beginwrite!(w.io, sch; file=w.file, compress=w.compress)
        w.st = st
        w.schema = sch
        w.names = names
        for batch in _tablebatches(sch, coldata, rowcounts)
            writebatch!(st, batch)
        end
    else
        names == w.names || throw(
            ArgumentError(
                "table column names $(names) do not match this writer's " *
                "schema columns $(w.names) (same names, same order)",
            ),
        )
        sch = w.schema::AC.Schema
        fields, coldata =
            _constructcolumns(names, partcols, partpools, j -> sch.fields[j], nothing)
        # A fresh Schema over the BUILT fields, so `writebatch!` re-checks
        # them against the writer's schema instead of trusting construction.
        batchsch = AC.Schema(fields; metadata=sch.metadata)
        for batch in _tablebatches(batchsch, coldata, rowcounts)
            writebatch!(w.st, batch)
        end
    end
    return w
end

function Base.close(w::Writer)
    (@atomic w.closed) && return nothing
    @atomic w.closed = true
    st = w.st
    if st !== nothing
        try
            finishwrite!(st)
        finally
            abortwrite!(st)
        end
    end
    w.ownio ? close(w.io) : flush(w.io)
    return nothing
end

"""
    Arrow.append(sink, table; compress=nothing)

Add `table`'s record batches to an existing IPC STREAM (`sink` is a file
path or a seekable read/write `IO`). The existing stream is validated in
full first, the new columns are constructed against its schema (same
names, order, and compatible types, or the append is refused), and the
new batches are published where the end-of-stream marker stood, followed
by a new end-of-stream marker.

A dictionary-encoded column whose pool matches the stream's current pool
reuses it; a changed pool is emitted as a replacement dictionary batch
when the stream's schema message declared the DictionaryReplacement
feature (streams this package writes declare it whenever the schema has a
dictionary field), and refused otherwise. The file format does not
support appending: rewrite the file, or produce it incrementally with
[`Arrow.Writer`](@ref).
"""
function append(path::AbstractString, tbl; kwargs...)
    bytes = Base.read(path)
    tail = _appendbytes(bytes, tbl; kwargs...)
    open(path, "r+") do io
        seek(io, length(bytes) - 8)   # overwrite the end-of-stream marker
        Base.write(io, tail)
    end
    return path
end

function append(io::IO, tbl; kwargs...)
    seekstart(io)
    bytes = Base.read(io)
    tail = _appendbytes(bytes, tbl; kwargs...)
    seek(io, length(bytes) - 8)       # overwrite the end-of-stream marker
    Base.write(io, tail)
    return io
end

function _appendbytes(bytes::Vector{UInt8}, tbl; compress::Union{Nothing,Symbol}=nothing)
    length(bytes) >= 6 &&
        view(bytes, 1:6) == FILE_MAGIC &&
        throw(
            ArgumentError(
                "Arrow.append supports the IPC stream format; this sink holds " *
                "the file format (ARROW1) — produce it incrementally with " *
                "Arrow.Writer, or rewrite it with Arrow.write",
            ),
        )
    codec = compress === nothing ? :none : compress
    # Validate the whole existing stream before extending it; a corrupt
    # prefix must refuse, not gain valid-looking bytes. `readstream` also
    # guarantees the trailing 8 bytes are the end-of-stream marker.
    s = readstream(bytes)
    sch = s.schema
    # The declared features gate replacement exactly as they do on read.
    msgs = framemessages(AC.heapregion(bytes), s.limits)
    features = Int64[Int64(x) for x in msgs[1].features]
    names, partcols, partpools, rowcounts = _collectparts(tbl)
    length(names) == length(sch.fields) &&
    all(j -> String(names[j]) == sch.fields[j].name, eachindex(names)) || throw(
        ArgumentError(
            "table column names $(names) do not match the existing stream's " *
            "schema fields $([f.name for f in sch.fields]) (same names, same order)",
        ),
    )
    fields, coldata =
        _constructcolumns(names, partcols, partpools, j -> sch.fields[j], nothing)
    batchsch = AC.Schema(fields; metadata=sch.metadata)
    # Prime the resumed state with the stream's last pool per id, so a
    # content-identical appended pool reuses the emitted dictionary batch.
    current = Dict{Int64,AC.ArrayData}()
    for batch in s.batches
        for (f, pool) in dictionarypools(sch.fields, batch.columns)
            current[s.fielddictids[f]] = pool
        end
    end
    io = IOBuffer()
    st = resumestream!(io, sch, s.fielddictids, features; compress=codec, current=current)
    try
        for batch in _tablebatches(batchsch, coldata, rowcounts)
            writebatch!(st, batch)
        end
        finishwrite!(st)
    finally
        abortwrite!(st)
    end
    return take!(io)
end

"Retained Arrow schema when the source is a facade read, else nothing."
_retainedschema(t::Table) = getfield(t, :schema)
_retainedschema(s::Stream) = _tableschema(getfield(s, :src))
_retainedschema(::Any) = nothing

"Dictionary pools retained by one facade partition, in column order."
_partitiondictpools(t::Table) = getfield(t, :retainedpools)
_partitiondictpools(::Any) = nothing

# One warning per removed-keyword name for the whole session, so a write
# loop does not flood the log.
function _warnremovedkwarg(name::Symbol)
    @warn "Arrow.write keyword `$(name)` was removed in Arrow 3.0 and is " *
          "ignored; see docs/src/migration.md" _id = Symbol(:arrow_removed_kwarg_, name) maxlog =
        1
    return nothing
end

function _writebytes(
    tbl;
    file::Bool=true,
    compress::Union{Nothing,Symbol}=nothing,
    metadata=nothing,
    colmetadata=nothing,
    # Arrow 2.x writer keywords: accepted and ignored (with a one-time
    # warning each) so 2.x call sites keep working during migration.
    alignment=nothing,
    dictencode=nothing,
    dictencodenested=nothing,
    denseunions=nothing,
    largelists=nothing,
    maxdepth=nothing,
    ntasks=nothing,
)
    for (name, value) in (
        (:alignment, alignment),
        (:dictencode, dictencode),
        (:dictencodenested, dictencodenested),
        (:denseunions, denseunions),
        (:largelists, largelists),
        (:maxdepth, maxdepth),
        (:ntasks, ntasks),
    )
        value === nothing || _warnremovedkwarg(name)
    end
    retained = _retainedschema(tbl)
    names, partcols, partpools, rowcounts = _collectparts(tbl)
    fields, coldata = _constructcolumns(
        names,
        partcols,
        partpools,
        _retainedfieldfn(retained, names),
        colmetadata,
    )
    schmeta =
        metadata !== nothing ? _metapairs(metadata) :
        (
            retained === nothing || retained.metadata === nothing ? nothing :
            collect(Pair{String,String}, retained.metadata)
        )
    sch = AC.Schema(fields; metadata=schmeta)
    batches = _tablebatches(sch, coldata, rowcounts)
    codec = compress === nothing ? :none : compress
    return file ? writefile(sch, batches; compress=codec) :
           writestream(sch, batches; compress=codec)
end

# Phase 1 of a write: materialize every partition's columns, validating
# name/order agreement — a drift here would silently bind data to the wrong
# fields.
function _collectparts(tbl)
    names = Symbol[]
    partcols = Vector{AbstractVector}[]
    partpools = Any[]
    rowcounts = Int[]
    for part in Tables.partitions(tbl)
        cols = Tables.columns(part)
        pnames = collect(Symbol, Tables.columnnames(cols))
        if isempty(partcols)
            names = pnames
        else
            pnames == names || throw(
                ArgumentError(
                    "partition $(length(partcols) + 1) column names $(pnames) " *
                    "do not match the first partition's $(names) (same names, " *
                    "same order); reorder or rename the partition's columns",
                ),
            )
        end
        # Arrow permits duplicate field names. Tables.getcolumn(cols, name)
        # cannot distinguish them, so bind every partition by position.
        push!(
            partcols,
            AbstractVector[Tables.getcolumn(cols, j) for j in eachindex(pnames)],
        )
        pools = _partitiondictpools(part)
        pools !== nothing &&
            length(pools) != length(pnames) &&
            throw(
                ArgumentError(
                    "retained dictionary pool count does not match partition width",
                ),
            )
        push!(partpools, pools)
        n = Int(Tables.rowcount(cols))
        # A zero-column partition has no column to count rows from, so ask
        # the partition itself: Arrow legally allows zero fields with a
        # positive row count (the reader carries that count on the Table).
        if n == 0 && isempty(pnames)
            n = Int(Tables.rowcount(part))
        end
        push!(rowcounts, n)
    end
    isempty(partcols) &&
        throw(ArgumentError("table has no partitions; cannot infer a schema"))
    return names, partcols, partpools, rowcounts
end

# Position is the only unambiguous identity for a retained field, because
# Arrow permits duplicate names. Fall back to a name match only when the
# name is unique on both sides; otherwise treat the column as un-retained.
function _retainedfieldfn(retained, names)
    ncols = length(names)
    retainedaligned =
        retained !== nothing &&
        length(retained.fields) == ncols &&
        all(j -> retained.fields[j].name == String(names[j]), 1:ncols)
    function retainedfield(j)
        retained === nothing && return nothing
        retainedaligned && return retained.fields[j]
        count(==(names[j]), names) == 1 || return nothing
        matches = findall(f -> f.name == String(names[j]), collect(retained.fields))
        return length(matches) == 1 ? retained.fields[only(matches)] : nothing
    end
    return retainedfield
end

# Phase 2: construct each complete logical column across the collected
# partitions. The column module owns inference, retained reconstruction,
# ArrowTypes, dictionary pooling, partition agreement, and field metadata.
function _constructcolumns(
    names,
    partcols,
    partpools,
    retainedfield::F,
    colmetadata,
) where {F}
    nparts = length(partcols)
    ncols = length(names)
    fields = Vector{AC.Field}(undef, ncols)
    coldata = Vector{Vector{AC.ArrayData}}(undef, ncols)
    colmetamap = colmetadata === nothing ? nothing : Dict(colmetadata)
    for j = 1:ncols
        parts = AbstractVector[partcols[k][j] for k = 1:nparts]
        poolhints = Any[pools === nothing ? nothing : pools[j] for pools in partpools]
        columnmeta = colmetamap === nothing ? nothing : get(colmetamap, names[j], nothing)
        fields[j], coldata[j] = _constructcolumn(
            names[j],
            parts;
            retained=retainedfield(j),
            poolhints=poolhints,
            metadata=_metapairs(columnmeta),
        )
    end
    return fields, coldata
end

function _tablebatches(sch::AC.Schema, coldata, rowcounts)
    ncols = length(sch.fields)
    return AC.RecordBatch[
        AC.RecordBatch(sch, AC.ArrayData[coldata[j][k] for j = 1:ncols], rowcounts[k]) for
        k in eachindex(rowcounts)
    ]
end

_metapairs(::Nothing) = nothing
function _metapairs(m)
    entries = m isa AbstractDict || m isa NamedTuple ? Base.pairs(m) : m
    out = Pair{String,String}[]
    for kv in entries
        kv isa Pair || throw(ArgumentError("metadata sequences must contain Pair values"))
        push!(out, String(first(kv)) => String(last(kv)))
    end
    return out
end
