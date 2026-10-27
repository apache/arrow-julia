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
                batchsize=1_048_576, dictencode=false, dictdeltas=false,
                largelists=false, stringview=false, metadata=nothing,
                colmetadata=nothing, ntasks=Threads.nthreads())
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
- `dictdeltas`: write the categories a dictionary gains in later partitions
  as delta dictionary batches (see [`Arrow.Writer`](@ref)).
- `largelists`: declare 64-bit offsets (LargeUtf8, LargeBinary, LargeList).
- `stringview`: write strings and bytes as Utf8View/BinaryView instead of
  Utf8/Binary.
- `metadata`, `colmetadata`: schema and per-column key-value pairs (a `Dict`
  or pairs; `colmetadata` maps column name `Symbol`s to them). They default to
  the table's DataAPI metadata (a `DataFrame`'s `metadata` and `colmetadata`,
  say), without keys under the reserved `ARROW:` prefix. An `Arrow.Table` or
  `Arrow.Stream` keeps its own schema and field metadata, without its batch
  statistics, since values, columns, or batch boundaries may have changed.
- `ntasks`: how many tasks build a batch's columns and compress its buffers
  (default `Threads.nthreads()`; `ntasks=1` runs on the calling task). The
  output bytes are the same for any `ntasks`.

Writing to an existing file writes a temporary file in the same directory
and renames it over the old one when the output is complete, so a failed
write leaves the old file unchanged, and an `Arrow.Table` or `Arrow.Stream`
that memory-maps the old file keeps reading it. If the operating system
refuses the rename, `write` throws a `Base.IOError` and the old file is kept.

Partitions are read, encoded, and published one at a time, so a partitioned
source such as `CSV.Chunks` writes in about one partition's memory. One
partition declares each field's nullability from its element type; several
partitions take their schema from the first, with every field nullable, and
later partitions conform to it as later tables of an [`Arrow.Writer`](@ref)
do. So a several-partition source of non-missing values reads back with
`Union{Missing,T}` element types, as pyarrow and Polars write it. If writing
fails, a path destination is left as it was; an `IO` sink may hold a partial
output.

The Arrow 2.x keywords `alignment`, `dictencodenested`, `denseunions`, and
`maxdepth` are accepted with a one-time warning and ignored. See
`docs/src/migration.md`.
"""
function write(path::AbstractString, tbl; kwargs...)
    _writeall!(_writer(path; oneshot=true, kwargs...), tbl)
    return path
end

function write(io::IO, tbl; kwargs...)
    _writeall!(_writer(io; oneshot=true, kwargs...), tbl)
    return io
end

# One table through a one-shot writer: an error abandons the output (a path
# destination keeps its old bytes or stays absent).
function _writeall!(w, @nospecialize(tbl))
    try
        _writeparts!(w, tbl)
    catch
        _abandon!(w)
        rethrow()
    end
    close(w)
    return nothing
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
                 nullable=true, metadata=nothing, colmetadata=nothing,
                 dictdeltas=false, dictreplacement=false, ntasks=Threads.nthreads())
    Arrow.Writer(f::Function, sink; kwargs...)

An incremental IPC writer: batches publish to `sink` as they are written,
so producing tables one at a time never holds more than one partition of
the current table in memory. `sink` is a file path (the writer opens and owns the handle) or
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

The first table written fixes the schema, with the inference one
`Arrow.write` of that table would use, except that every field is declared
nullable (as pyarrow and Polars do), so later missing values conform;
`nullable=false` keeps the first table's nullability instead. A table from
`Arrow.Table` or `Arrow.Stream` keeps its own schema. Every later table must
have the same column names in the same order and types that fit the schema:
numbers widen losslessly (`Int32` into an `Int64` column, `Float32` into
`Float64`), missing values fill nullable fields (nested ones too), and
anything else is refused with an error naming the column and both types. A
table's partitions publish one at a time: if a later partition is refused,
the batches of its earlier partitions stay in the output, and the writer
stays open.

Dictionary-encoded columns (`Arrow.DictEncode`, `PooledArray`,
`CategoricalArray`) keep one dictionary across tables: a later table's new
categories are added after the earlier ones, and its indices refer to the
combined categories. Such dictionaries use `Int32` indices. In the file
format each dictionary is written once, complete, when the writer closes
(after the record batches; the format allows a dictionary anywhere before
the footer). In the stream format a dictionary that gained categories is
written again whole before the batch that needs it. pyarrow and Polars read
both. `dictdeltas=true` writes only the new categories, as delta dictionary
batches (in both formats): smaller, read by pyarrow, refused by Polars 1.43.
With `dictreplacement=true` (stream format only), each table instead replaces
the dictionary with its own categories; the schema then declares the
DictionaryReplacement feature, which strict readers such as nanoarrow refuse.

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
    const dictdeltas::Bool
    const compresslevel::Union{Nothing,Int}
    const batchsize::Int
    const largelists::Bool
    const stringview::Bool
    const nullable::Bool
    const dictencode::Any
    const ntasks::Int
    # Arrow.write's writer: nullability and dictionary index types come from
    # the input (one partition or several) instead of the Writer rules.
    const oneshot::Bool
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
    dictdeltas::Bool=false,
    compresslevel::Union{Nothing,Integer}=nothing,
    batchsize::Integer=DEFAULT_BATCHSIZE,
    largelists::Bool=false,
    stringview::Bool=false,
    nullable::Bool=true,
    dictencode=false,
    oneshot::Bool=false,
    ntasks::Integer=Threads.nthreads(),
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
    ntasks >= 1 || throw(ArgumentError("ntasks must be at least 1, got $ntasks"))
    dictencode isa Bool ||
        all(n -> n isa Union{Symbol,AbstractString}, dictencode) ||
        throw(ArgumentError("dictencode must be true, false, or column names"))
    if dictreplacement && file
        throw(
            ArgumentError(
                "dictreplacement is a stream-format feature; the IPC file " *
                "format carries one dictionary per column, extended only by deltas",
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
            dictdeltas,
            compresslevel === nothing ? nothing : Int(compresslevel),
            batchsize,
            largelists,
            stringview,
            nullable,
            dictencode,
            Int(ntasks),
            oneshot,
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
    _writeparts!(w, tbl)
    return w
end

# Write `tbl` partition by partition: each partition is read, encoded, and
# published before the next one is requested, so memory holds one partition
# (and one record batch of encoded output) at a time.
# The table and partition types are not specialized on: columns dispatch on
# their own types, so a new table type compiles no new writer code.
function _writeparts!(w::Writer, @nospecialize(tbl))
    retained = _retainedschema(tbl)
    parts = Tables.partitions(tbl)
    next = iterate(parts)
    if next === nothing
        # A zero-batch Arrow.Stream still carries its schema.
        w.st === nothing &&
            _beginwrite!(w, first(_schemaonly(retained, w.colmetadata)), retained)
        return nothing
    end
    part, state = next
    next = iterate(parts, state)
    # Several partitions: their schema comes from the first, every field
    # nullable, and dictionaries may grow, as for an Arrow.Writer.
    several = next !== nothing
    while true
        _writepartition!(w, part, retained, several)
        next === nothing && return nothing
        part, state = next
        next = iterate(parts, state)
    end
end

function _writepartition!(w::Writer, @nospecialize(part), retained, several::Bool)
    cols = Tables.columns(part)
    names = collect(Symbol, Tables.columnnames(cols))
    w.st === nothing ||
        names == w.names ||
        throw(
            ArgumentError(
                "table column names $(names) do not match the schema columns " *
                "$(w.names) (same names, same order)",
            ),
        )
    # Arrow permits duplicate field names. Tables.getcolumn(cols, name)
    # cannot distinguish them, so bind every column by position.
    columns = AbstractVector[Tables.getcolumn(cols, j) for j in eachindex(names)]
    own = _partitiondictpools(part)
    own === nothing ||
        length(own) == length(names) ||
        throw(
            ArgumentError("retained dictionary pool count does not match partition width"),
        )
    n = Int(Tables.rowcount(cols))
    # A zero-column partition has no column to count rows from, so ask the
    # partition itself: Arrow legally allows zero fields with a positive
    # row count (the reader carries that count on the Table).
    n == 0 && isempty(names) && (n = Int(Tables.rowcount(part)))
    encoded = _dictencodeflags(names, w.dictencode)
    for j in eachindex(columns)
        col = columns[j]
        encoded[j] && !(col isa DictEncode) && !_hasrefpool(col) && (col = DictEncode(col))
        # Inference from values sees the whole partition, not one batch.
        columns[j] = _narrowcolumn(String(names[j]), col)
    end
    # The first partition's dictionaries hold all of its categories, so its
    # batches share one pool. A later partition re-indexes its dictionaries into
    # the written pools, except an Arrow source's, whose retained construction
    # continues them from these hints.
    hints =
        w.st === nothing || retained !== nothing ? _partitionhints(w, columns, own) :
        Any[nothing for _ in columns]
    # The partition is captured as `Any`, so `_eachbatch` compiles once, not
    # once per table type.
    anypart = Ref{Any}(part)
    _eachbatch(columns, n, w.batchsize) do batchcols, nrows
        _writebatch!(
            w,
            anypart[],
            names,
            batchcols,
            nrows,
            hints,
            retained,
            encoded,
            several,
        )
    end
    return nothing
end

"One record batch: the first fixes the schema; later ones must conform to it."
function _writebatch!(
    w::Writer,
    @nospecialize(part),
    names,
    cols,
    nrows,
    hints,
    retained,
    encoded,
    several,
)
    if w.st === nothing
        colmetadata = w.colmetadata
        colmetadata === nothing &&
            retained === nothing &&
            (colmetadata = _inputcolmetadata(part, names))
        fields, coldata = _constructcolumns(
            names,
            [cols],
            Any[hints],
            _retainedfieldfn(retained, names, encoded),
            colmetadata;
            stringview=w.stringview,
            largelists=w.largelists,
            growable=(!w.oneshot || several),
            ntasks=_batchtasks(w, nrows, length(names)),
        )
        # Fields are nullable so later missing values conform (a Writer, or
        # several input partitions); a retained schema keeps its own.
        nullable = (w.oneshot ? several : w.nullable) && retained === nothing
        nullable && (fields = AC.Field[_nullablefield(f) for f in fields])
        _beginwrite!(w, fields, retained, names; several, input=part)
        writebatch!(w.st::IPCWriteState, only(_tablebatches(w.schema, coldata, [nrows])))
        return nothing
    end
    sch = w.schema::AC.Schema
    st = w.st::IPCWriteState
    written = Any[
        f.type isa AC.DictionaryType && !w.dictreplacement ?
        get(st.current, st.ids[f], nothing) : nothing for f in sch.fields
    ]
    fields, coldata = _constructconforming(
        names,
        [cols],
        Any[hints],
        sch,
        part;
        w.stringview,
        w.largelists,
        written,
        ntasks=_batchtasks(w, nrows, length(names)),
    )
    # A fresh Schema over the BUILT fields, so `writebatch!` re-checks them
    # against the writer's schema instead of trusting construction.
    batch = only(_tablebatches(AC.Schema(fields; metadata=sch.metadata), coldata, [nrows]))
    writebatch!(st, batch; deltas=_dictionarydeltas(st, batch))
    return nothing
end

"Fix the schema and publish its message (and the file preamble)."
function _beginwrite!(
    w::Writer,
    fields,
    retained,
    names=nothing;
    several::Bool=false,
    input=nothing,
)
    # The bounded facade interning rule, as when the source was read.
    names === nothing && (names = _fieldnamesymbols(fields))
    sch = AC.Schema(fields; metadata=_writermetadata(w.metadata, retained, input))
    w.st = beginwrite!(
        w.io,
        sch;
        file=w.file,
        compress=w.compress,
        compresslevel=w.compresslevel,
        dictreplacement=w.dictreplacement,
        ntasks=w.ntasks,
        growth=_dictgrowth(w, several),
    )
    w.schema = sch
    w.names = names
    return nothing
end

"""
How a dictionary that gains categories is written (see `IPCWriteState`).
Defaults every reader accepts: files write each final dictionary once, at
the end; streams write a grown dictionary again whole. One table with one
partition has all its categories from the start, so it keeps its
dictionaries before the record batches.
"""
function _dictgrowth(w::Writer, several::Bool)
    w.dictdeltas && return :delta
    w.file || return :replace
    return w.oneshot && !several ? :delta : :defer
end

"""
Dictionary hints for the first partition, or a partition of an Arrow source:
the categories written so far, then the categories the partition brings (a
pool it retains from its Arrow source, a `PooledArray`'s pool, or a
`DictEncode` column's values), so every batch of the partition shares one
dictionary.
"""
function _partitionhints(w::Writer, columns, own)
    hints = Vector{Any}(undef, length(columns))
    written =
        w.st === nothing || w.dictreplacement ? nothing : _writtenpools(w.st::IPCWriteState)
    for j in eachindex(columns)
        prior = written === nothing ? nothing : written[j]
        retainedown = own === nothing ? nothing : own[j]
        hint = _extendpool(prior, retainedown)
        # The written categories (or else an Arrow source's own pool) are kept
        # exactly; the categories after them dedupe by storage value.
        prefix =
            prior !== nothing ? length(prior) :
            retainedown !== nothing ? length(retainedown) : 0
        col = columns[j]
        if col isa DictEncode
            T = Base.nonmissingtype(eltype(col.data))
            writetype = isconcretetype(T) ? T : nothing
            all(ismissing, col.data) ||
                (hint = _dictionarypool(AbstractVector[col.data], hint; writetype))
        elseif _hasrefpool(col) && (
            w.st === nothing || (w.schema::AC.Schema).fields[j].type isa AC.DictionaryType
        )
            hint = _extendpool(hint, last(_refpoolvalues(col)))
        end
        hints[j] = hint === nothing ? nothing : _PoolHint(hint, prefix)
    end
    return hints
end

# A column whose element type no Arrow type holds (`Any`, `Real`) narrowed
# from all its values, so every batch writes the same type.
function _narrowcolumn(name::String, col::AbstractVector)
    col isa DictEncode && return col
    eltype(col) === Any && (col = _narrowlists(col, name))
    narrowed = _narrowabstractwriterparts(name, AbstractVector[col], _WriterContext(name))
    return narrowed === nothing ? col : only(narrowed)
end

"""
Call `f(cols, nrows)` for consecutive record batches of at most `batchsize`
rows. A batch whose 32-bit offsets would pass the Int32 range throws
`_OffsetOverflow` before it is written; it is then cut at the row that did not
fit (every later batch of the partition too) and built again, so the declared
types never change. A zero-row partition is one empty batch.
"""
function _eachbatch(f::F, cols::Vector{AbstractVector}, n::Int, batchsize::Int) where {F}
    n == 0 && return f(AbstractVector[_rowslice(c, 1:0) for c in cols], 0)
    size = batchsize
    start = 1
    while start <= n
        r = start:min(start + size - 1, n)
        try
            f(AbstractVector[_rowslice(c, r) for c in cols], length(r))
        catch err
            err isa _OffsetOverflow || rethrow()
            size = only(_shrinkbatches!([size], [(1, r)], err))
            continue
        end
        start = last(r) + 1
    end
    return nothing
end

"Release a writer after an error without finishing its output."
function _abandon!(w::Writer)
    @atomic w.closed = true
    st = w.st
    st === nothing || abortwrite!(st)
    w.ownio && _abortpath(w.path)
    return nothing
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
    Arrow.append(sink, table; compress=nothing, compresslevel=nothing,
                 batchsize=1_048_576, dictdeltas=false)
    Arrow.append(sink; kwargs...)

Add `table`'s record batches to an existing IPC STREAM (`sink` is a file
path or a seekable read/write `IO`). The existing stream is validated in
full first, the new columns are constructed against its schema (same
names and order; types fit as for a later table of [`Arrow.Writer`](@ref),
or the append is refused), and the new batches are published after the last
existing message (over its end-of-stream marker, if it has one), followed by a
new end-of-stream marker.

A dictionary-encoded column continues the stream's latest dictionary, within
the range of its index type: a dictionary that gains categories is written
again whole (`dictdeltas=true` writes only the new categories, as a delta
dictionary batch). A stream whose schema declares DictionaryReplacement
(`Arrow.Writer(sink; file=false, dictreplacement=true)`) gets a replacement
dictionary instead. The file format does not support appending: rewrite the
file, or produce it incrementally with [`Arrow.Writer`](@ref). A missing or
empty `sink` starts a new stream, as `Arrow.write(sink, table; file=false)`
does. The one-argument form curries: `table |> Arrow.append(path)`.
"""
function append(path::AbstractString, tbl; kwargs...)
    # Appending to nothing starts a stream.
    isfile(path) && filesize(path) > 0 || return write(path, tbl; file=false, kwargs...)
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
    isempty(bytes) && return write(io, tbl; file=false, kwargs...)
    keep, tail = _appendbytes(bytes, tbl; kwargs...)
    seek(io, keep)
    Base.write(io, tail)
    return io
end

append(sink::Union{AbstractString,IO}; kwargs...) = tbl -> append(sink, tbl; kwargs...)

function _appendbytes(
    bytes::Vector{UInt8},
    tbl;
    compress::Union{Nothing,Symbol}=nothing,
    compresslevel::Union{Nothing,Integer}=nothing,
    batchsize::Integer=DEFAULT_BATCHSIZE,
    dictdeltas::Bool=false,
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
    # Dictionary columns extend the stream's latest categories; a delta
    # dictionary batch carries the new ones.
    hints = _extendpools(_streampools(s), partpools)
    written = Any[
        f.type isa AC.DictionaryType ? get(s.dictionaries, s.fielddictids[f], nothing) :
        nothing for f in sch.fields
    ]
    fields, coldata, batchrows =
        _constructbatches(partcols, hints, rowcounts, batchsize) do cols, pools
            _constructconforming(names, cols, pools, sch, tbl; written)
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
        growth=dictdeltas ? :delta : :replace,
    )
    try
        for batch in _tablebatches(batchsch, coldata, batchrows)
            writebatch!(st, batch; deltas=_dictionarydeltas(st, batch))
        end
        finishwrite!(st)
    finally
        abortwrite!(st)
    end
    return keep, take!(io)
end

# --- dictionaries that grow across tables -----------------------------------

"Each top-level dictionary column's written categories (public values), else nothing."
function _writtenpools(st::IPCWriteState)
    ctx = _ArrowTypesContext(; warn=false)
    return Any[
        f.type isa AC.DictionaryType ?
        _retainedsnapshotpool(f, get(st.current, st.ids[f], nothing), ctx) : nothing for
        f in st.schema.fields
    ]
end

"The latest categories of each top-level dictionary column of a stream."
function _streampools(s::IPCStream)
    ctx = _ArrowTypesContext(; warn=false)
    return Any[
        f.type isa AC.DictionaryType ?
        _retainedsnapshotpool(f, get(s.dictionaries, s.fielddictids[f], nothing), ctx) :
        nothing for f in s.schema.fields
    ]
end

# Pool hints for each partition: the categories written so far, followed by
# any categories the partition retains from its own source.
function _extendpools(pools::Vector{Any}, partpools)
    return Any[
        Any[
            _extendpool(pools[j], own === nothing ? nothing : own[j]) for
            j in eachindex(pools)
        ] for own in partpools
    ]
end
function _extendpool(written, own)
    written === nothing && return own
    own === nothing && return written
    T = _widenedtype(eltype(written), eltype(own))
    return _mergecategorypools(
        AbstractVector[T === eltype(written) ? written : collect(T, written), own],
    )
end

"""
Delta dictionaries for a batch whose pools extend, value by value, the pools
already written under their ids (`current`): the new categories as storage,
by id.
"""
function _dictionarydeltas(current, fields, ids, batch::AC.RecordBatch)
    deltas = Dict{Int64,AC.ArrayData}()
    for (f, pool) in dictionarypools(fields, batch.columns)
        id = ids[f]
        old = get(current, id, nothing)
        (old === nothing || old === pool || pool.len <= old.len) && continue
        haskey(deltas, id) && continue
        vf = AC.dictvaluefield(f, f.type::AC.DictionaryType)
        stored = _writerstoredcolumn(vf, pool)
        isequal(_writerstoredcolumn(vf, old), stored[1:(old.len)]) || continue
        _, delta = _rebuildwriterstorageselection(
            vf,
            stored,
            collect((old.len + 1):(pool.len)),
            _WriterContext(f.name),
        )
        deltas[id] = delta
    end
    return deltas
end
_dictionarydeltas(st::IPCWriteState, batch::AC.RecordBatch) =
    _dictionarydeltas(st.current, st.schema.fields, st.ids, batch)

"""
A schema field declared nullable, with its descendants, except the children
the format requires to be non-null (map entries and keys, run ends).
"""
function _nullablefield(f::AC.Field)
    t = f.type
    children = if t isa AC.MapType && length(f.children) == 1
        entries = only(f.children)
        kv = entries.children
        AC.Field[AC.Field(
            entries.name,
            entries.type;
            nullable=false,
            metadata=entries.metadata,
            children=AC.Field[kv[1], (_nullablefield(c) for c in kv[2:end])...],
        )]
    elseif t isa AC.RunEndEncodedType && length(f.children) == 2
        AC.Field[f.children[1], _nullablefield(f.children[2])]
    else
        AC.Field[_nullablefield(c) for c in f.children]
    end
    return AC.Field(f.name, t; nullable=true, metadata=f.metadata, children=children)
end

"Retained Arrow schema when the source is a facade read, else nothing."
_retainedschema(t::Table) = getfield(t, :schema)
_retainedschema(s::Stream) = _tableschema(getfield(s, :src))
_retainedschema(::Any) = nothing

function _writermetadata(metadata, retained, input=nothing)
    metadata !== nothing && return _metapairs(metadata)
    retained === nothing && return _inputmetadata(input)
    retained.metadata === nothing && return nothing
    # Statistics describe the source's values, field indexes, and batch
    # boundaries. Retaining its descriptors does not preserve that identity.
    # Keep user metadata in order, including duplicate keys.
    return Pair{String,String}[kv for kv in retained.metadata if first(kv) != STATS_KEY]
end

# A table's DataAPI metadata, as the schema's key-value pairs. Keys under the
# reserved `ARROW:` prefix (an extension label a column was read with, say)
# describe the source's encoding, not the table, and are left out.
function _inputmetadata(@nospecialize(tbl))
    (tbl === nothing || !DataAPI.metadatasupport(typeof(tbl)).read) && return nothing
    out = Pair{String,String}[
        String(string(k)) => string(DataAPI.metadata(tbl, k)) for
        k in DataAPI.metadatakeys(tbl) if !startswith(string(k), "ARROW:")
    ]
    return isempty(out) ? nothing : out
end

function _inputcolmetadata(@nospecialize(tbl), names)
    DataAPI.colmetadatasupport(typeof(tbl)).read || return nothing
    out = Dict{Symbol,Vector{Pair{String,String}}}()
    for name in names
        pairs = Pair{String,String}[
            String(string(k)) => string(DataAPI.colmetadata(tbl, name, k)) for
            k in DataAPI.colmetadatakeys(tbl, name) if !startswith(string(k), "ARROW:")
        ]
        isempty(pairs) || (out[name] = pairs)
    end
    return isempty(out) ? nothing : out
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

"The bytes `Arrow.write` writes for `tbl`."
_writebytes(tbl; kwargs...) = take!(write(IOBuffer(), tbl; kwargs...))

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
# A record batch that is exactly one of a chained column's arrays (one batch it
# was read from, say) is that array.
function _rowview(col::ChainedVector, r)
    start = 1
    for a in col.arrays
        start > first(r) && break
        stop = start + length(a) - 1
        start == first(r) && stop == last(r) && eltype(a) === eltype(col) && return a
        start = stop + 1
    end
    return view(col, r)
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
    growable::Bool=false,
    ntasks::Int=1,
) where {F}
    nparts = length(partcols)
    colmetamap = colmetadata === nothing ? nothing : Dict(colmetadata)
    built = _tasksmap(length(names), ntasks) do j, _
        parts = AbstractVector[partcols[k][j] for k = 1:nparts]
        poolhints = Any[pools === nothing ? nothing : pools[j] for pools in partpools]
        columnmeta =
            colmetamap === nothing ? nothing : get(colmetamap, names[j], nothing)
        return _constructcolumn(
            names[j],
            parts;
            retained=retainedfield(j),
            poolhints=poolhints,
            metadata=_metapairs(columnmeta),
            refpools=refpools,
            stringview=stringview,
            largelists=largelists,
            growable=growable,
        )
    end
    return AC.Field[first(b) for b in built], Vector{AC.ArrayData}[last(b) for b in built]
end

# Columns build in parallel when a batch holds enough values for the tasks to
# pay for themselves (writing many small tables stays on one task).
const _PARALLEL_CELLS = 1 << 16
_batchtasks(w::Writer, nrows::Int, ncols::Int) =
    nrows * ncols >= _PARALLEL_CELLS ? w.ntasks : 1

# Construct the columns of a table that must conform to an existing schema:
# a later `Arrow.Writer` table or an `Arrow.append` table. A facade source
# (`Arrow.Table`, `Arrow.Stream`) holds public values, which the retained
# construction lowers. Any other source holds writer values (`DateTime`,
# `NamedTuple` rows, tuples, `Dict`s, any `AbstractString`): build each such
# column the way the first batch was built and use it when it conforms to
# the schema field. A dictionary column built that way is re-indexed into
# `written[j]`, the pool its field last wrote (storage; `nothing` starts a
# replacement pool). Columns that cannot be built that way, such as one
# whose own mapping differs from the field, take the retained construction.
function _constructconforming(
    names,
    partcols,
    partpools,
    sch::AC.Schema,
    @nospecialize(tbl);
    stringview::Bool=false,
    largelists::Bool=false,
    written=nothing,
    ntasks::Int=1,
)
    retainedfield = j -> sch.fields[j]
    _retainedschema(tbl) === nothing ||
        return _constructcolumns(names, partcols, partpools, retainedfield, nothing; ntasks)
    built = _tasksmap(length(names), ntasks) do j, _
        parts = AbstractVector[p[j] for p in partcols]
        hints = Any[pools === nothing ? nothing : pools[j] for pools in partpools]
        w = written === nothing ? nothing : written[j]
        return _conformcolumn(
            names[j],
            parts,
            hints,
            sch.fields[j],
            w;
            stringview,
            largelists,
        )
    end
    return AC.Field[first(b) for b in built], Vector{AC.ArrayData}[last(b) for b in built]
end

"One column of `_constructconforming`: its field and its data per partition."
function _conformcolumn(
    name::Symbol,
    parts,
    poolhints,
    expected::AC.Field,
    written;
    stringview::Bool,
    largelists::Bool,
)
    dictmismatch = nothing
    if expected.type isa AC.DictionaryType && written !== nothing
        conformed, dictmismatch =
            _conformdictionary(name, parts, expected, written; stringview)
        conformed === nothing || return conformed
    end
    if expected.nullable &&
       !(expected.type isa AC.DictionaryType) &&
       all(p -> eltype(p) === Missing, parts)
        # An all-missing column is all nulls of the field's type.
        context = _WriterContext(String(name))
        return expected,
        AC.ArrayData[
            last(_constructhiddenpart(expected, length(p), context)) for p in parts
        ]
    end
    fresh =
        expected.type isa AC.DictionaryType ? nothing :
        _freshcolumn(name, parts; stringview, largelists)
    mismatch =
        fresh === nothing ? dictmismatch : _conformancemismatch(expected, first(fresh))
    if fresh !== nothing && mismatch !== nothing
        # Missing values conform wherever the field is nullable, nested too.
        fresh = _fillnulls(expected, fresh, _WriterContext(String(name)))
        mismatch = _conformancemismatch(expected, first(fresh))
    end
    if fresh !== nothing && mismatch !== nothing
        # Numbers widen losslessly into the field's type (Int32 into Int64).
        widened = _widenparts(expected, parts)
        if widened !== nothing
            fresh = _freshcolumn(name, widened; stringview, largelists)
            fresh === nothing ||
                _conformancemismatch(expected, first(fresh)) !== nothing ||
                (mismatch = nothing)
        end
    end
    fresh !== nothing && mismatch === nothing && return expected, last(fresh)
    try
        return _constructcolumn(name, parts; retained=expected, poolhints)
    catch err
        # Report the schema difference of a column the writer mapping could
        # build instead of the retained construction's value-domain error.
        (err isa ArgumentError && mismatch !== nothing) || rethrow()
        throw(
            ArgumentError(
                "column $name does not conform to the existing schema: " * mismatch,
            ),
        )
    end
end

"""
A later table's dictionary column built the way the first table's was (as its
own dictionary), then re-indexed into `written`, the categories its field
already wrote (storage; `nothing` for a replacement pool): those keep their
order and new categories follow, so every part refers to the extended pool.
Returns the field and data, or `nothing` and why the column does not conform
(`nothing` too when the values do not build as a dictionary at all).
"""
function _conformdictionary(
    name::Symbol,
    parts,
    expected::AC.Field,
    written;
    stringview::Bool=false,
)
    context = _WriterContext(String(name))
    et = expected.type::AC.DictionaryType
    vf = AC.dictvaluefield(expected, et)
    written === nothing && (written = last(_constructhiddenpart(vf, 0, context)))
    if all(p -> eltype(p) === Missing, parts)
        expected.nullable || return nothing, nothing
        return (
            expected,
            AC.ArrayData[_nulldictionary(et, length(p), written) for p in parts],
        ),
        nothing
    end
    dictparts = AbstractVector[
        p isa DictEncode || _hasrefpool(p) ? p : DictEncode(p) for p in parts
    ]
    fresh = try
        _constructcolumn(name, dictparts; stringview)
    catch err
        err isa ArgumentError || rethrow()
        return nothing, nothing
    end
    field, data = fresh
    ft = field.type
    ft isa AC.DictionaryType || return nothing, nothing
    localvf = AC.dictvaluefield(field, ft)
    mismatch = _conformancemismatch(vf, localvf)
    mismatch === nothing || return nothing, mismatch
    # A dictionary's extension label sits on the dictionary field itself.
    _arrowtypesextension(expected) == _arrowtypesextension(field) ||
        return nothing, "$name has a different extension type than the schema field"
    !expected.nullable &&
        any(d -> AC.nullcount(d) > 0, data) &&
        return nothing, "$name holds missing values, but the schema field is not nullable"
    localpool = first(data).dictionary::AC.ArrayData
    all(d -> d.dictionary === localpool, data) || return nothing, nothing
    # ponytail: the written pool is decoded again for every later batch
    # (O(categories) each); cache its storage keys if huge pools get slow.
    physical =
        vcat(_writerstoredcolumn(vf, written), _writerstoredcolumn(localvf, localpool))
    offset = Int(written.len)
    selected, remap = _compactstorage(physical, offset)
    pool =
        length(selected) == offset ? written :
        last(_rebuildwriterstorageselection(vf, physical, selected, context))
    IT = AC.juliatype(et.indextype)
    length(selected) - 1 <= typemax(IT) || throw(
        ArgumentError(
            "column $name has $(length(selected)) dictionary categories, more than " *
            "its $IT indices address",
        ),
    )
    reindexed =
        AC.ArrayData[_reindexdictionary(d, et, IT, remap, offset, pool) for d in data]
    return (expected, reindexed), nothing
end

"`n` null slots of dictionary type `t` that refer to `pool`."
function _nulldictionary(t::AC.DictionaryType, n::Int, pool::AC.ArrayData)
    IT = AC.juliatype(t.indextype)
    validity = n == 0 ? AC.BufferSlice() : AC._databuffer(zeros(UInt8, cld(n, 8)))
    return AC.ArrayData(
        t,
        n,
        [validity, AC._databuffer(zeros(IT, n))];
        dictionary=pool,
        nullcount=n,
    )
end

# A dictionary part's indices mapped through `remap` (offset by the written
# categories) into index type `IT`, referring to `pool`.
function _reindexdictionary(d::AC.ArrayData, t, ::Type{IT}, remap, offset, pool) where {IT}
    LT = AC.juliatype((d.type::AC.DictionaryType).indextype)
    validity = d.buffers[1]
    codes = reinterpret(LT, AC.slicebytes(d.buffers[2]))
    out = Vector{IT}(undef, d.len)
    for i = 1:(d.len)
        valid = AC.isempty_buffer(validity) || AC.getbit(validity, Int64(i - 1))
        out[i] = valid ? IT(remap[offset + Int(codes[i]) + 1]) : zero(IT)
    end
    return AC.ArrayData(
        t,
        d.len,
        [validity, AC._databuffer(out)];
        dictionary=pool,
        nullcount=AC.nullcount(d),
    )
end

"""
A fresh column (field and per-part data) whose all-missing values inferred the
null type where `expected` (or a descendant) is a nullable typed field: those
values become null slots of the expected type. Anything else is unchanged.
"""
function _fillnulls(expected::AC.Field, fresh, context)
    field, parts = fresh
    filled = [_fillnulls(expected, field, d, context) for d in parts]
    return first(first(filled)), AC.ArrayData[last(x) for x in filled]
end
function _fillnulls(expected::AC.Field, actual::AC.Field, d::AC.ArrayData, context)
    if actual.type isa AC.NullType
        (expected.type isa AC.NullType || !expected.nullable) && return actual, d
        return _constructhiddenpart(expected, Int(d.len), context)
    end
    n = length(actual.children)
    (
        actual.type isa AC.DictionaryType ||
        n != length(expected.children) ||
        n != length(d.children)
    ) && return actual, d
    fields = collect(AC.Field, actual.children)
    children = collect(AC.ArrayData, d.children)
    for i = 1:n
        fields[i], children[i] =
            _fillnulls(expected.children[i], fields[i], children[i], context)
    end
    all(i -> fields[i] === actual.children[i], 1:n) && return actual, d
    return AC.Field(
        actual.name,
        actual.type;
        nullable=actual.nullable,
        metadata=_fieldmetadata(actual),
        children=fields,
    ),
    AC.ArrayData(
        d.type,
        d.len,
        d.buffers;
        offset=d.offset,
        children,
        dictionary=d.dictionary,
        nullcount=AC.nullcount(d),
    )
end

"""
A later table's number columns converted to a schema field's wider number
type, when every value converts exactly (`Int32` into `Int64`, `Float32` into
`Float64`, `Int32` into `Float64`); nothing otherwise.
"""
function _widenparts(expected::AC.Field, parts)
    t = expected.type
    t isa Union{AC.IntType,AC.FloatType} || return nothing
    T = AC.juliatype(t)
    widened = AbstractVector[]
    for p in parts
        S = Base.nonmissingtype(eltype(p))
        if S === T
            push!(widened, p)
        elseif S === Union{} || _widens(S, T)
            push!(
                widened,
                Missing <: eltype(p) ? convert(Vector{Union{Missing,T}}, p) :
                convert(Vector{T}, p),
            )
        else
            return nothing
        end
    end
    return widened
end
_widens(::Type{S}, ::Type{T}) where {S<:Integer,T<:Integer} =
    S !== Bool && typemin(T) <= typemin(S) && typemax(S) <= typemax(T)
_widens(::Type{S}, ::Type{T}) where {S<:AbstractFloat,T<:AbstractFloat} =
    sizeof(S) <= sizeof(T)
_widens(::Type{S}, ::Type{T}) where {S<:Integer,T<:AbstractFloat} =
    S !== Bool && 8 * sizeof(S) < precision(T)
_widens(::Type, ::Type) = false

function _freshcolumn(name::Symbol, parts; stringview::Bool=false, largelists::Bool=false)
    try
        return _constructcolumn(name, parts; refpools=false, stringview, largelists)
    catch err
        # The retained construction reports why this column cannot be written.
        err isa ArgumentError || rethrow()
        return nothing
    end
end

"An Arrow type's name for messages, as pyarrow spells it."
function _typename(t::AC.ArrowType)
    unit(u) =
        u == AC.SECOND ? "s" :
        u == AC.MILLISECOND ? "ms" : u == AC.MICROSECOND ? "us" : "ns"
    t isa AC.IntType && return (t.signed ? "int" : "uint") * string(t.bits)
    t isa AC.FloatType &&
        return t.bits == 16 ? "halffloat" : t.bits == 32 ? "float" : "double"
    t isa AC.BoolType && return "bool"
    t isa AC.Utf8Type && return t.large ? "large_string" : "string"
    t isa AC.BinaryType && return t.large ? "large_binary" : "binary"
    t isa AC.ViewType && return t.utf8 ? "string_view" : "binary_view"
    t isa AC.FixedSizeBinaryType && return "fixed_size_binary[$(t.nbytes)]"
    t isa AC.DateType && return t.unit == AC.DAY ? "date32" : "date64"
    t isa AC.TimeType && return "time$(t.bits)[$(unit(t.unit))]"
    t isa AC.TimestampType &&
        return "timestamp[$(unit(t.unit))$(t.timezone === nothing ? "" : ", tz=$(t.timezone)")]"
    t isa AC.DurationType && return "duration[$(unit(t.unit))]"
    t isa AC.DecimalType && return "decimal$(t.bits)($(t.precision), $(t.scale))"
    t isa AC.IntervalType && return "interval"
    t isa AC.ListType && return t.large ? "large_list" : "list"
    t isa AC.ListViewType && return t.large ? "large_list_view" : "list_view"
    t isa AC.FixedSizeListType && return "fixed_size_list[$(t.listsize)]"
    t isa AC.StructType && return "struct"
    t isa AC.MapType && return "map"
    t isa AC.UnionType && return "union"
    t isa AC.DictionaryType && return "dictionary<$(_typename(t.valuetype))>"
    t isa AC.RunEndEncodedType && return "run_end_encoded"
    t isa AC.NullType && return "null (all missing)"
    return String(AC.descriptorname(t))
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
    if !AC.typeequal(expected.type, actual.type)
        expected.type isa AC.NullType &&
            return "$path writes as $(_typename(actual.type)), but the schema " *
                   "declared it null because it held only missing values (or no " *
                   "rows) when the schema was set; give it a concrete element type"
        expected.type isa AC.UnionType &&
            actual.type isa AC.UnionType &&
            return "$path writes as a union with other members than the schema's union"
        return "$path writes as $(_typename(actual.type)), but the schema has " *
               _typename(expected.type)
    end
    actual.nullable &&
        !expected.nullable &&
        return "$path holds missing values, but the schema field is not nullable " *
               "(an Arrow.Writer declares every field nullable unless created " *
               "with nullable=false)"
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
