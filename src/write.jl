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
    Arrow.write(sink, table; file=true, compress=nothing, compresslevel=nothing,
                batchsize=1_048_576, dictencode=false, largelists=false,
                stringview=false, metadata=nothing, colmetadata=nothing)
    Arrow.write(sink; kwargs...)

Write any Tables.jl source as Arrow IPC to `sink`, a file path or an `IO`, and
return `sink`. The one-argument form curries: `table |> Arrow.write(sink)`.

Keywords:
- `file`: the random-access file format (`ARROW1` magic and footer), or with
  `file=false` the stream format.
- `compress`: `nothing`, `:lz4`, or `:zstd`; `compresslevel` sets the codec's
  level (LZ4 up to 12, Zstandard up to 22; `nothing` keeps the codec's
  default, 0 for LZ4 and 3 for Zstandard).
- `batchsize`: rows per record batch. Each `Tables.partitions` partition
  becomes one or more batches of at most `batchsize` rows. A batch whose
  strings, bytes, or lists would pass the 2 GiB that 32-bit offsets address
  is cut smaller, so the declared types never change.
- `dictencode`: dictionary-encode every column (`true`) or the named ones (a
  collection of `Symbol`s or strings). Columns with a `DataAPI.refpool`
  (`PooledArray`, `CategoricalArray`) and [`Arrow.DictEncode`](@ref) columns
  are always dictionary-encoded.
- `largelists`: declare 64-bit offsets (LargeUtf8, LargeBinary, LargeList).
- `stringview`: write strings and bytes as Utf8View/BinaryView instead of
  Utf8/Binary.
- `metadata`, `colmetadata`: schema and per-column key-value pairs (a `Dict`
  or pairs; `colmetadata` maps column name `Symbol`s to them). Schema metadata
  is inherited from an `Arrow.Table` or `Arrow.Stream` when `metadata` is
  `nothing`, without inherited batch statistics, since values, columns, or
  batch boundaries may have changed.
- `ntasks` is accepted; encoding runs on one task.

Writing to an existing file writes a temporary file in the same directory
and renames it over the old one when the output is complete, so a failed
write leaves the old file unchanged, and an `Arrow.Table` or `Arrow.Stream`
that memory-maps the old file keeps reading it. If the operating system
refuses the rename, `write` throws a `Base.IOError` and the old file is kept.

The writer is eager and whole-buffer: batches are encoded and validated in
memory, then written to the sink once.

The Arrow 2.x keywords `alignment`, `dictencodenested`, `denseunions`, and
`maxdepth` are accepted with a one-time warning and ignored. See
`docs/src/migration.md`.
"""
function write(path::AbstractString, tbl; kwargs...)
    bytes = _writebytes(tbl; kwargs...)
    sink = _openpath(path)
    try
        Base.write(sink.io, bytes)
    catch
        _abortpath(sink)
        rethrow()
    end
    _commitpath(sink)
    return path
end

# An existing regular file is replaced by writing a temporary file in the
# same directory and renaming it over the destination once the output is
# complete. A live Arrow.Table or Arrow.Stream that memory-maps the old file
# keeps reading the old inode, and a failed write leaves the old file as it
# was. New paths and non-regular files (FIFOs, devices) are written in place.
struct _PathSink
    io::IO
    path::String                # the destination, symlinks resolved
    tmp::Union{Nothing,String}  # renamed over `path` on commit
    created::Bool               # `path` did not exist before this write
end

function _openpath(path::AbstractString)
    if !isfile(path)
        created = !ispath(path) && !islink(path)
        return _PathSink(open(path, "w"), String(path), nothing, created)
    end
    dest = realpath(path)
    tmp, io = mktemp(dirname(dest); cleanup=false)
    try
        chmod(tmp, filemode(dest) & 0o7777)
    catch
        close(io)
        rm(tmp; force=true)
        rethrow()
    end
    return _PathSink(io, dest, tmp, false)
end

function _commitpath(sink::_PathSink)
    try
        close(sink.io)
    catch
        sink.tmp === nothing || rm(sink.tmp; force=true)
        rethrow()
    end
    sink.tmp === nothing && return nothing
    err = ccall(:jl_fs_rename, Int32, (Cstring, Cstring), sink.tmp, sink.path)
    err < 0 || return nothing
    rm(sink.tmp; force=true)
    throw(Base.IOError("Arrow could not replace $(repr(sink.path)); it is unchanged", err))
end

# Error cleanup: the destination keeps its old bytes (or stays absent).
function _abortpath(sink::_PathSink)
    try
        close(sink.io)
    catch
        # Preserve the write error if closing also fails.
    end
    if sink.tmp !== nothing
        rm(sink.tmp; force=true)
    elseif sink.created
        rm(sink.path; force=true)
    end
    return nothing
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
    Arrow.Writer(sink; file=true, compress=nothing, compresslevel=nothing,
                 batchsize=1_048_576, largelists=false, stringview=false,
                 metadata=nothing, colmetadata=nothing, dictreplacement=false)
    Arrow.Writer(f::Function, sink; kwargs...)

An incremental IPC writer: batches publish to `sink` as they are written,
so producing tables one at a time never holds more than the current table
in memory. `sink` is a file path (the writer opens and owns the handle) or
an `IO` (borrowed; `close` finishes the IPC output but leaves the `IO`
open). The function form runs `f(writer)` and always closes the writer.
The keywords mean what they mean for [`Arrow.write`](@ref), whose metadata
inheritance rules apply too.
Constructor option checks run before opening a file path. If both `f` and
`close` fail, the function form preserves the error from `f`. An existing
file path is replaced as by [`Arrow.write`](@ref): the output goes to a
temporary file that `close` renames over the old file.

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

Dictionary-encoded columns: only `Arrow.DictEncode` columns are encoded
here (a `PooledArray` or `CategoricalArray` column is written as its
values), and every later table may reuse the first table's exact pool (the
same categories in the same first-appearance order). A CHANGED pool needs replacement dictionary batches, which the
schema message must declare up front: pass `dictreplacement=true`
(stream format only) to declare it. The declaration is a demand on
readers — strict ones such as nanoarrow refuse any stream declaring a
feature they do not support — so it is opt-in, and without it a changed
pool is refused. The file format carries one dictionary batch per id and
never replaces.

`close` finalizes what has been published — the sink is a valid IPC
output containing every batch written so far — and is idempotent. A
finalization failure still releases the owned sink. A
writer abandoned without `close` leaves a torn stream or an unfooted
file; closing a writer that never received a table just closes the sink
without producing valid IPC. One task owns a writer: overlapping calls
are not synchronized.
"""
mutable struct Writer
    const io::IO
    const ownio::Bool
    const path::Union{Nothing,_PathSink}
    const file::Bool
    const compress::Symbol
    const metadata::Any
    const colmetadata::Any
    const dictreplacement::Bool
    const compresslevel::Union{Nothing,Int}
    const batchsize::Int
    const largelists::Bool
    const stringview::Bool
    st::Union{Nothing,IPCWriteState}
    names::Vector{Symbol}
    schema::Union{Nothing,AC.Schema}
    @atomic closed::Bool
end

function _writer(
    sink::Union{AbstractString,IO};
    file::Bool=true,
    compress::Union{Nothing,Symbol}=nothing,
    metadata=nothing,
    colmetadata=nothing,
    dictreplacement::Bool=false,
    compresslevel::Union{Nothing,Integer}=nothing,
    batchsize::Integer=DEFAULT_BATCHSIZE,
    largelists::Bool=false,
    stringview::Bool=false,
    ntasks::Union{Nothing,Integer}=nothing,
)
    codec = _codec(compress, compresslevel)
    batchsize = _checkbatchsize(batchsize)
    if dictreplacement && file
        throw(
            ArgumentError(
                "dictreplacement is a stream-format feature; the IPC file " *
                "format carries one dictionary batch per id",
            ),
        )
    end
    # Open only after keyword dispatch and option checks.
    ownio = sink isa AbstractString
    path = ownio ? _openpath(sink) : nothing
    io = ownio ? path.io : sink
    try
        return Writer(
            io,
            ownio,
            path,
            file,
            codec,
            metadata,
            colmetadata,
            dictreplacement,
            compresslevel === nothing ? nothing : Int(compresslevel),
            batchsize,
            largelists,
            stringview,
            nothing,
            Symbol[],
            nothing,
            false,
        )
    catch
        ownio && _abortpath(path)
        rethrow()
    end
end

Writer(sink::Union{AbstractString,IO}; kwargs...) = _writer(sink; kwargs...)

function Writer(f::Function, sink::Union{AbstractString,IO}; kwargs...)
    w = Writer(sink; kwargs...)
    result = try
        f(w)
    catch
        try
            close(w)
        catch
            # Preserve the body error if finalization or sink cleanup fails.
        end
        rethrow()
    end
    close(w)
    return result
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
each `Tables.partitions` partition becomes one or more record batches of at
most the writer's `batchsize` rows, published to the sink before the call
returns. Returns `writer`. See [`Arrow.Writer`](@ref)
for the schema rules.
"""
function write(w::Writer, tbl)
    (@atomic w.closed) && throw(ArgumentError("this Arrow.Writer is closed"))
    names, partcols, partpools, rowcounts = _collectparts(tbl)
    if w.st === nothing
        retained = _retainedschema(tbl)
        # Later tables must reuse the first table's exact dictionary pool,
        # so only explicit DictEncode columns are encoded here.
        fields, coldata, batchrows = if isempty(partcols)
            (_schemaonly(retained, w.colmetadata)..., Int[])
        else
            retainedfield = _retainedfieldfn(retained, names)
            _constructbatches(partcols, partpools, rowcounts, w.batchsize) do cols, pools
                _constructcolumns(
                    names,
                    cols,
                    pools,
                    retainedfield,
                    w.colmetadata;
                    refpools=false,
                    stringview=w.stringview,
                    largelists=w.largelists,
                )
            end
        end
        # The bounded facade interning rule, as when the source was read.
        isempty(partcols) && (names = _fieldnamesymbols(fields))
        schmeta = _writermetadata(w.metadata, retained)
        sch = AC.Schema(fields; metadata=schmeta)
        st = beginwrite!(
            w.io,
            sch;
            file=w.file,
            compress=w.compress,
            compresslevel=w.compresslevel,
            dictreplacement=w.dictreplacement,
        )
        w.st = st
        w.schema = sch
        w.names = names
        for batch in _tablebatches(sch, coldata, batchrows)
            writebatch!(st, batch)
        end
    else
        isempty(partcols) && return w
        names == w.names || throw(
            ArgumentError(
                "table column names $(names) do not match this writer's " *
                "schema columns $(w.names) (same names, same order)",
            ),
        )
        sch = w.schema::AC.Schema
        fields, coldata, batchrows =
            _constructbatches(partcols, partpools, rowcounts, w.batchsize) do cols, pools
                _constructconforming(names, cols, pools, sch, tbl; w.stringview)
            end
        # A fresh Schema over the BUILT fields, so `writebatch!` re-checks
        # them against the writer's schema instead of trusting construction.
        batchsch = AC.Schema(fields; metadata=sch.metadata)
        for batch in _tablebatches(batchsch, coldata, batchrows)
            writebatch!(w.st, batch)
        end
    end
    return w
end

function Base.close(w::Writer)
    (@atomic w.closed) && return nothing
    @atomic w.closed = true
    st = w.st
    try
        st === nothing || finishwrite!(st)
    catch
        if st !== nothing
            try
                abortwrite!(st)
            catch
                # Preserve the finalization error if codec cleanup also fails.
            end
        end
        w.ownio && _abortpath(w.path)
        rethrow()
    end
    w.ownio ? _commitpath(w.path) : flush(w.io)
    return nothing
end

"""
    Arrow.append(sink, table; compress=nothing, compresslevel=nothing, batchsize=1_048_576)

Add `table`'s record batches to an existing IPC STREAM (`sink` is a file
path or a seekable read/write `IO`). The existing stream is validated in
full first, the new columns are constructed against its schema (same
names, order, and compatible types, or the append is refused), and the
new batches are published after the last existing message (over its
end-of-stream marker, if it has one), followed by a new end-of-stream marker.

A dictionary-encoded column whose pool matches the stream's latest dictionary
update reuses it, including updates after the last record batch; a changed
pool is emitted as a replacement dictionary batch
when the stream's schema message declared the DictionaryReplacement
feature (produce such a stream with
`Arrow.Writer(sink; file=false, dictreplacement=true)`), and refused
otherwise. The file format does not support appending: rewrite the file,
or produce it incrementally with [`Arrow.Writer`](@ref).
"""
function append(path::AbstractString, tbl; kwargs...)
    bytes = Base.read(path)
    keep, tail = _appendbytes(bytes, tbl; kwargs...)
    open(path, "r+") do io
        seek(io, keep)
        Base.write(io, tail)
    end
    return path
end

function append(io::IO, tbl; kwargs...)
    seekstart(io)
    bytes = Base.read(io)
    keep, tail = _appendbytes(bytes, tbl; kwargs...)
    seek(io, keep)
    Base.write(io, tail)
    return io
end

function _appendbytes(
    bytes::Vector{UInt8},
    tbl;
    compress::Union{Nothing,Symbol}=nothing,
    compresslevel::Union{Nothing,Integer}=nothing,
    batchsize::Integer=DEFAULT_BATCHSIZE,
)
    length(bytes) >= 6 &&
        view(bytes, 1:6) == FILE_MAGIC &&
        throw(
            ArgumentError(
                "Arrow.append supports the IPC stream format; this sink holds " *
                "the file format (ARROW1) — produce it incrementally with " *
                "Arrow.Writer, or rewrite it with Arrow.write",
            ),
        )
    codec = _codec(compress, compresslevel)
    batchsize = _checkbatchsize(batchsize)
    # Validate the whole existing stream before extending it; a corrupt
    # prefix must refuse, not gain valid-looking bytes.
    s = readstream(bytes)
    sch = s.schema
    # Retain the schema's declared features when resuming the writer.
    msgs = framemessages(AC.heapregion(bytes), s.limits)
    features = Int64[Int64(x) for x in msgs[1].features]
    # New messages start where the last framed message ends: over the
    # end-of-stream marker when the stream has one, else at the end.
    keep = last(msgs).body.offset + last(msgs).body.len
    names, partcols, partpools, rowcounts = _collectparts(tbl)
    namesmatch =
        length(names) == length(sch.fields) &&
        all(j -> String(names[j]) == sch.fields[j].name, eachindex(names))
    # A partition-free source (a zero-batch Arrow.Stream) appends nothing.
    isempty(partcols) ||
        namesmatch ||
        throw(
            ArgumentError(
                "table column names $(names) do not match the existing stream's " *
                "schema fields $([f.name for f in sch.fields]) (same names, same order)",
            ),
        )
    fields, coldata, batchrows =
        _constructbatches(partcols, partpools, rowcounts, batchsize) do cols, pools
            _constructconforming(names, cols, pools, sch, tbl)
        end
    batchsch = AC.Schema(fields; metadata=sch.metadata)
    io = IOBuffer()
    st = resumestream!(
        io,
        sch,
        s.fielddictids,
        features;
        compress=codec,
        compresslevel,
        current=s.dictionaries,
    )
    try
        for batch in _tablebatches(batchsch, coldata, batchrows)
            writebatch!(st, batch)
        end
        finishwrite!(st)
    finally
        abortwrite!(st)
    end
    return keep, take!(io)
end

"Retained Arrow schema when the source is a facade read, else nothing."
_retainedschema(t::Table) = getfield(t, :schema)
_retainedschema(s::Stream) = _tableschema(getfield(s, :src))
_retainedschema(::Any) = nothing

function _writermetadata(metadata, retained)
    metadata !== nothing && return _metapairs(metadata)
    (retained === nothing || retained.metadata === nothing) && return nothing
    # Statistics describe the source's values, field indexes, and batch
    # boundaries. Retaining its descriptors does not preserve that identity.
    # Keep user metadata in order, including duplicate keys.
    return Pair{String,String}[kv for kv in retained.metadata if first(kv) != STATS_KEY]
end

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
    compresslevel::Union{Nothing,Integer}=nothing,
    metadata=nothing,
    colmetadata=nothing,
    batchsize::Integer=DEFAULT_BATCHSIZE,
    dictencode=false,
    largelists::Bool=false,
    stringview::Bool=false,
    # Accepted for the API's shared concurrency keyword; writes run on one task.
    ntasks::Union{Nothing,Integer}=nothing,
    # Arrow 2.x writer keywords: accepted and ignored (with a one-time
    # warning each) so 2.x call sites keep working during migration.
    alignment=nothing,
    dictencodenested=nothing,
    denseunions=nothing,
    maxdepth=nothing,
)
    for (name, value) in (
        (:alignment, alignment),
        (:dictencodenested, dictencodenested),
        (:denseunions, denseunions),
        (:maxdepth, maxdepth),
    )
        value === nothing || _warnremovedkwarg(name)
    end
    codec = _codec(compress, compresslevel)
    batchsize = _checkbatchsize(batchsize)
    retained = _retainedschema(tbl)
    names, partcols, partpools, rowcounts = _collectparts(tbl)
    encoded = _dictencodeflags(names, dictencode)
    _applydictencode!(partcols, encoded)
    fields, coldata, batchrows = if isempty(partcols)
        (_schemaonly(retained, colmetadata)..., Int[])
    else
        retainedfield = _retainedfieldfn(retained, names, encoded)
        _constructbatches(partcols, partpools, rowcounts, batchsize) do cols, pools
            _constructcolumns(
                names,
                cols,
                pools,
                retainedfield,
                colmetadata;
                stringview,
                largelists,
            )
        end
    end
    schmeta = _writermetadata(metadata, retained)
    sch = AC.Schema(fields; metadata=schmeta)
    batches = _tablebatches(sch, coldata, batchrows)
    return file ? writefile(sch, batches; compress=codec, compresslevel) :
           writestream(sch, batches; compress=codec, compresslevel)
end

"Rows per record batch when `batchsize` is not given: 1,048,576 (as pyarrow's chunks)."
const DEFAULT_BATCHSIZE = 1 << 20

function _checkbatchsize(batchsize::Integer)
    batchsize >= 1 || throw(ArgumentError("batchsize must be at least 1, got $batchsize"))
    return Int(batchsize)
end

function _codec(compress, compresslevel)
    codec = compress === nothing ? :none : compress
    haskey(CODEC_NAMES, codec) || throw(
        ArgumentError("compress must be nothing, :lz4, or :zstd, got $(repr(compress))"),
    )
    compresslevel === nothing ||
        codec !== :none ||
        throw(ArgumentError("compresslevel needs compress=:lz4 or compress=:zstd"))
    return codec
end

# Which top-level columns `dictencode` asks to dictionary-encode: all of them
# (`true`), none (`false`), or those named (Symbols or Strings).
function _dictencodeflags(names::Vector{Symbol}, dictencode)
    dictencode isa Bool && return fill(dictencode, length(names))
    wanted = Set{Symbol}(Symbol(n) for n in dictencode)
    for n in wanted
        n in names || throw(
            ArgumentError("dictencode names column $n, which the table does not have"),
        )
    end
    return Bool[n in wanted for n in names]
end

function _applydictencode!(partcols, encoded::Vector{Bool})
    for cols in partcols, j in eachindex(cols)
        encoded[j] || continue
        col = cols[j]
        col isa DictEncode || _hasrefpool(col) || (cols[j] = DictEncode(col))
    end
    return partcols
end

"""
Construct columns over record batches of at most `batchsize` rows: each
partition is cut into consecutive row ranges, and `construct(cols, pools)`
builds every column across those batches. A builder with 32-bit offsets
whose data would pass the Int32 range throws `_OffsetOverflow`; the batches
are then cut smaller and construction restarts, so the declared types never
change. Returns the fields, the column data, and the row count per batch.
"""
function _constructbatches(
    construct::F,
    partcols,
    partpools,
    rowcounts,
    batchsize::Int,
) where {F}
    sizes = fill(batchsize, length(rowcounts))
    while true
        plan = _batchplan(rowcounts, sizes)
        cols = Vector{AbstractVector}[
            AbstractVector[_rowslice(col, r) for col in partcols[p]] for (p, r) in plan
        ]
        pools = Any[partpools[p] for (p, _) in plan]
        try
            fields, coldata = construct(cols, pools)
            return fields, coldata, Int[length(r) for (_, r) in plan]
        catch err
            err isa _OffsetOverflow || rethrow()
            _shrinkbatches!(sizes, plan, err)
        end
    end
end

"(partition, rows) of each record batch: consecutive ranges of `sizes[p]` rows."
function _batchplan(rowcounts, sizes)
    plan = Tuple{Int,UnitRange{Int}}[]
    for (p, n) in enumerate(rowcounts)
        n == 0 && push!(plan, (p, 1:0))
        for start = 1:sizes[p]:n
            push!(plan, (p, start:min(start + sizes[p] - 1, n)))
        end
    end
    return plan
end

# Every partition's batches shrink to the rows that fit before the overflow
# (half the batch when the row is unknown, as in nested data).
function _shrinkbatches!(sizes, plan, err::_OffsetOverflow)
    hint = "write it with largelists=true, which declares 64-bit offsets"
    err.part > 0 || throw(
        ArgumentError(
            "column $(err.column) holds more than 2 GiB of data that cannot be split " *
            "across record batches (a dictionary pool, say); $hint",
        ),
    )
    _, r = plan[err.part]
    (length(r) <= 1 || err.row == 1) &&
        throw(ArgumentError("column $(err.column) has a value of more than 2 GiB; $hint"))
    chunk = err.row > 1 ? err.row - 1 : cld(length(r), 2)
    sizes .= min.(sizes, chunk)
    return sizes
end

# Rows `r` (1-based positions) of a column, without copying when it can.
function _rowslice(col::AbstractVector, r::UnitRange{Int})
    length(r) == length(col) && return col
    return _rowview(col, (firstindex(col) - 1) .+ r)
end
_rowview(col::AbstractVector, r) = view(col, r)
_rowview(col::DictEncode, r) = DictEncode(_rowslice(col.data, r .- (firstindex(col) - 1)))
_rowview(col::DataStrings.StringVector{T}, r) where {T} =
    DataStrings.StringVector{T}(col.payloads[r], col.buffers, Val(:trusted))
_rowview(col::DataStrings.BytesVector{T}, r) where {T} =
    DataStrings.BytesVector{T}(col.payloads[r], col.buffers, Val(:trusted))

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
    return names, partcols, partpools, rowcounts
end

"""
Fields for a source with no partitions. A zero-batch `Arrow.Stream` still
carries its retained schema, so the output keeps that schema and has no
record batches; any other partition-free source has no schema to write.
"""
function _schemaonly(retained, colmetadata)
    retained === nothing &&
        throw(ArgumentError("table has no partitions; cannot infer a schema"))
    # Match colmetadata by name without interning the source's field names.
    colmetamap =
        colmetadata === nothing ? nothing :
        Dict(String(k) => v for (k, v) in Dict(colmetadata))
    fields = AC.Field[]
    for f in retained.fields
        meta =
            colmetamap === nothing ? nothing : _metapairs(get(colmetamap, f.name, nothing))
        push!(fields, meta === nothing ? f : _withfieldmetadata(f, meta))
    end
    return fields, Vector{AC.ArrayData}[AC.ArrayData[] for _ in fields]
end

# Position is the only unambiguous identity for a retained field, because
# Arrow permits duplicate names. Fall back to a name match only when the
# name is unique on both sides; otherwise treat the column as un-retained.
function _retainedfieldfn(retained, names, encoded=nothing)
    ncols = length(names)
    retainedaligned =
        retained !== nothing &&
        length(retained.fields) == ncols &&
        all(j -> retained.fields[j].name == String(names[j]), 1:ncols)
    function retainedfield(j)
        retained === nothing && return nothing
        f = _retainedfieldat(retained, names, retainedaligned, j)
        # A column `dictencode` encodes gets a fresh dictionary field.
        f === nothing ||
            encoded === nothing ||
            !encoded[j] ||
            f.type isa AC.DictionaryType ||
            return nothing
        return f
    end
    return retainedfield
end

function _retainedfieldat(retained, names, retainedaligned::Bool, j::Int)
    retainedaligned && return _unsharedfield(retained.fields[j])
    count(==(names[j]), names) == 1 || return nothing
    matches = findall(f -> f.name == String(names[j]), collect(retained.fields))
    return length(matches) == 1 ? _unsharedfield(retained.fields[only(matches)]) : nothing
end

# The written schema needs one Field object per position (dictionary ids key
# on Field identity), but a retained schema can share child subtrees between
# positions, as a scan that selects one column twice does.
_unsharedfield(f::AC.Field) = AC.Field(
    f.name,
    f.type;
    nullable=f.nullable,
    metadata=f.metadata,
    children=AC.Field[_unsharedfield(c) for c in f.children],
)

# Phase 2: construct each complete logical column across the collected
# partitions. The column module owns inference, retained reconstruction,
# ArrowTypes, dictionary pooling, partition agreement, and field metadata.
function _constructcolumns(
    names,
    partcols,
    partpools,
    retainedfield::F,
    colmetadata;
    refpools::Bool=true,
    stringview::Bool=false,
    largelists::Bool=false,
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
            refpools=refpools,
            stringview=stringview,
            largelists=largelists,
        )
    end
    return fields, coldata
end

# Construct the columns of a table that must conform to an existing schema:
# a later `Arrow.Writer` table or an `Arrow.append` table. A facade source
# (`Arrow.Table`, `Arrow.Stream`) holds public values, which the retained
# construction lowers. Any other source holds writer values (`DateTime`,
# `NamedTuple` rows, tuples, `Dict`s, any `AbstractString`): build each such
# column the way an eager write builds a later partition and use it when it
# conforms to the schema field. Dictionary columns keep the retained
# construction, which continues the existing pool, and so does a column whose
# own mapping differs from the field, such as an all-missing column.
function _constructconforming(
    names,
    partcols,
    partpools,
    sch::AC.Schema,
    tbl;
    stringview::Bool=false,
)
    retainedfield = j -> sch.fields[j]
    _retainedschema(tbl) === nothing ||
        return _constructcolumns(names, partcols, partpools, retainedfield, nothing)
    nparts = length(partcols)
    fields = Vector{AC.Field}(undef, length(names))
    coldata = Vector{Vector{AC.ArrayData}}(undef, length(names))
    for j in eachindex(names)
        expected = sch.fields[j]
        parts = AbstractVector[partcols[k][j] for k = 1:nparts]
        fresh =
            expected.type isa AC.DictionaryType ? nothing :
            _freshcolumn(names[j], parts; stringview)
        mismatch =
            fresh === nothing ? nothing : _conformancemismatch(expected, first(fresh))
        if fresh !== nothing && mismatch === nothing
            fields[j], coldata[j] = expected, last(fresh)
            continue
        end
        poolhints = Any[pools === nothing ? nothing : pools[j] for pools in partpools]
        try
            fields[j], coldata[j] =
                _constructcolumn(names[j], parts; retained=expected, poolhints=poolhints)
        catch err
            # Report the schema difference of a column the writer mapping could
            # build instead of the retained construction's value-domain error.
            (err isa ArgumentError && mismatch !== nothing) || rethrow()
            throw(
                ArgumentError(
                    "column $(names[j]) does not conform to the existing schema: " *
                    mismatch,
                ),
            )
        end
    end
    return fields, coldata
end

function _freshcolumn(name::Symbol, parts; stringview::Bool=false)
    try
        return _constructcolumn(name, parts; refpools=false, stringview)
    catch err
        # The retained construction reports why this column cannot be written.
        err isa ArgumentError || rethrow()
        return nothing
    end
end

"""
Why data built for `actual` is not valid under `expected`, or `nothing`. Valid
data has equal descriptors, child names, and extension types at every level,
with nullability only widened. Nested metadata must match; the top-level user
metadata is `expected`'s.
"""
function _conformancemismatch(
    expected::AC.Field,
    actual::AC.Field;
    path::String=expected.name,
    top::Bool=true,
)
    expected.name == actual.name || return "$path is named $(repr(actual.name))"
    AC.typeequal(expected.type, actual.type) ||
        return "$path maps to $(repr(actual.type)) where the schema has " *
               repr(expected.type)
    actual.nullable &&
        !expected.nullable &&
        return "$path admits missing values but the schema field is non-nullable"
    _arrowtypesextension(expected) == _arrowtypesextension(actual) ||
        return "$path has a different extension type than the schema field"
    top ||
        something(_fieldmetadata(expected), Pair{String,String}[]) ==
        something(_fieldmetadata(actual), Pair{String,String}[]) ||
        return "$path has different field metadata than the schema"
    length(expected.children) == length(actual.children) ||
        return "$path has $(length(actual.children)) children where the schema has " *
               "$(length(expected.children))"
    for i in eachindex(expected.children)
        child = expected.children[i]
        mismatch = _conformancemismatch(
            child,
            actual.children[i];
            path="$path.$(child.name)",
            top=false,
        )
        mismatch === nothing || return mismatch
    end
    return nothing
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
