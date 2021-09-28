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

struct ArrowBlob
    bytes::Vector{UInt8}
    pos::Int
    len::Int
end

ArrowBlob(bytes::Vector{UInt8}, pos::Int, len::Nothing) = ArrowBlob(bytes, pos, length(bytes))

tobytes(bytes::Vector{UInt8}) = bytes
tobytes(io::IO) = Base.read(io)
function tobytes(str)
    f = string(str)
    isfile(f) || throw(ArgumentError("$f is not a file"))
    return Mmap.mmap(f)
end

struct BatchIterator
    bytes::Vector{UInt8}
    startpos::Int
end

"""
    Arrow.Stream(io::IO; convert::Bool=true)
    Arrow.Stream(file::String; convert::Bool=true)
    Arrow.Stream(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)
    Arrow.Stream(inputs::Vector; convert::Bool=true)

Start reading an arrow formatted table, from:
 * `io`, bytes will be read all at once via `read(io)`
 * `file`, bytes will be read via `Mmap.mmap(file)`
 * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`
 * A `Vector` of any of the above, in which each input should be an IPC or arrow file and must match schema

Reads the initial schema message from the arrow stream/file, then returns an `Arrow.Stream` object
which will iterate over record batch messages, producing an [`Arrow.Table`](@ref) on each iteration.

By iterating [`Arrow.Table`](@ref), `Arrow.Stream` satisfies the `Tables.partitions` interface, and as such can
be passed to Tables.jl-compatible sink functions.

This allows iterating over extremely large "arrow tables" in chunks represented as record batches.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be
lazily converted to more friendly Julia defaults; by default, `convert=true`.
"""
mutable struct Stream
    inputs::Vector{ArrowBlob}
    inputindex::Int
    batchiterator::BatchIterator
    pos::Int
    names::Vector{Symbol}
    types::Vector{Type}
    schema::Meta.Schema
    dictencodings::Dict{Int64, DictEncoding} # dictionary id => DictEncoding
    dictencoded::Dict{Int64, Meta.Field} # dictionary id => field
    convert::Bool
    compression::Ref{Union{Symbol,Nothing}}
end

Tables.partitions(x::Stream) = x

Stream(input, pos::Integer=1, len=nothing; kw...) = Stream([ArrowBlob(tobytes(input), pos, len)]; kw...)
Stream(input::Vector{UInt8}, pos::Integer=1, len=nothing; kw...) = Stream([ArrowBlob(tobytes(input), pos, len)]; kw...)
Stream(inputs::Vector; kw...) = Stream([ArrowBlob(tobytes(x), 1, nothing) for x in inputs]; kw...)

# will detect whether we're reading a Stream from a file or stream
function Stream(inputs::Vector{ArrowBlob}; convert::Bool=true)
    dictencodings = Dict{Int64, DictEncoding}() # dictionary id => DictEncoding
    dictencoded = Dict{Int64, Meta.Field}() # dictionary id => field
    blob = inputs[1]
    bytes, pos, len = blob.bytes, blob.pos, blob.len
    if len > 24 &&
        _startswith(bytes, pos, FILE_FORMAT_MAGIC_BYTES) &&
        _endswith(bytes, pos + len - 1, FILE_FORMAT_MAGIC_BYTES)
        pos += 8 # skip past magic bytes + padding
    end
    batchiterator = BatchIterator(bytes, pos)
    state = iterate(batchiterator)
    state === nothing && throw(ArgumentError("no arrow ipc messages found in provided input"))
    batch, (pos, id) = state
    schema = batch.msg.header
    schema isa Meta.Schema || throw(ArgumentError("first arrow ipc message MUST be a schema message"))
    # assert endianness?
    # store custom_metadata?
    names = Symbol[]
    types = Type[]
    for (i, field) in enumerate(schema.fields)
        push!(names, Symbol(field.name))
        push!(types, juliaeltype(field, buildmetadata(field.custom_metadata), convert))
        # recursively find any dictionaries for any fields
        getdictionaries!(dictencoded, field)
        @debug 1 "parsed column from schema: field = $field"
    end
    return Stream(inputs, 1, batchiterator, pos, names, types, schema, dictencodings, dictencoded, convert, Ref{Union{Symbol,Nothing}}(nothing))
end

Base.IteratorSize(::Type{Stream}) = Base.SizeUnknown()

function Base.iterate(x::Stream, (pos, id)=(x.pos, 1))
    columns = AbstractVector[]
    compression = nothing

    while true
        state = iterate(x.batchiterator, (pos, id))
        if state === nothing
            # check for additional inputs
            while state === nothing
                x.inputindex += 1
                x.inputindex > length(x.inputs) && return nothing
                blob = x.inputs[x.inputindex]
                bytes, pos, len = blob.bytes, blob.pos, blob.len
                if len > 24 &&
                    _startswith(bytes, pos, FILE_FORMAT_MAGIC_BYTES) &&
                    _endswith(bytes, pos + len - 1, FILE_FORMAT_MAGIC_BYTES)
                    pos += 8 # skip past magic bytes + padding
                end
                x.batchiterator = BatchIterator(bytes, pos)
                state = iterate(x.batchiterator, (1, id))
            end
        end
        batch, (pos, id) = state
        header = batch.msg.header
        if header isa Meta.Schema
            if header != x.schema
                throw(ArgumentError("mismatched schemas between different arrow batches: $(x.schema) != $header"))
            end
        elseif header isa Meta.DictionaryBatch
            id = header.id
            recordbatch = header.data
            @debug 1 "parsing dictionary batch message: id = $id, compression = $(recordbatch.compression)"
            if recordbatch.compression !== nothing
                compression = recordbatch.compression
            end
            if haskey(x.dictencodings, id) && header.isDelta
                # delta
                field = x.dictencoded[id]
                values, _, _ = build(field, field.type, batch, recordbatch, x.dictencodings, Int64(1), Int64(1), x.convert)
                dictencoding = x.dictencodings[id]
                append!(dictencoding.data, values)
                continue
            end
            # new dictencoding or replace
            field = x.dictencoded[id]
            values, _, _ = build(field, field.type, batch, recordbatch, x.dictencodings, Int64(1), Int64(1), x.convert)
            A = ChainedVector([values])
            S = field.dictionary.indexType === nothing ? Int32 : juliaeltype(field, field.dictionary.indexType, false)
            x.dictencodings[id] = DictEncoding{eltype(A), S, typeof(A)}(id, A, field.dictionary.isOrdered, values.metadata)
            @debug 1 "parsed dictionary batch message: id=$id, data=$values\n"
        elseif header isa Meta.RecordBatch
            @debug 1 "parsing record batch message: compression = $(header.compression)"
            if header.compression !== nothing
                compression = header.compression
            end
            for vec in VectorIterator(x.schema, batch, x.dictencodings, x.convert)
                push!(columns, vec)
            end
            break
        else
            throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
        end
    end

    if compression !== nothing
        if compression.codec == Flatbuf.CompressionType.ZSTD
            x.compression[] = :zstd
        elseif compression.codec == Flatbuf.CompressionType.LZ4_FRAME
            x.compression[] = :lz4
        else
            throw(ArgumentError("unsupported compression codec: $(compression.codec)"))
        end
    end

    lookup = Dict{Symbol, AbstractVector}()
    types = Type[]
    for (nm, col) in zip(x.names, columns)
        lookup[nm] = col
        push!(types, eltype(col))
    end
    return Table(x.names, types, columns, lookup, Ref(x.schema)), (pos, id)
end

"""
    Arrow.Table(io::IO; convert::Bool=true)
    Arrow.Table(file::String; convert::Bool=true)
    Arrow.Table(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)
    Arrow.Table(inputs::Vector; convert::Bool=true)

Read an arrow formatted table, from:
 * `io`, bytes will be read all at once via `read(io)`
 * `file`, bytes will be read via `Mmap.mmap(file)`
 * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`
 * A `Vector` of any of the above, in which each input should be an IPC or arrow file and must match schema

Returns a `Arrow.Table` object that allows column access via `table.col1`, `table[:col1]`, or `table[1]`.

NOTE: the columns in an `Arrow.Table` are views into the original arrow memory, and hence are not easily
modifiable (with e.g. `push!`, `append!`, etc.). To mutate arrow columns, call `copy(x)` to materialize
the arrow data as a normal Julia array.

`Arrow.Table` also satisfies the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface, and so can easily be materialied via any supporting
sink function: e.g. `DataFrame(Arrow.Table(file))`, `SQLite.load!(db, "table", Arrow.Table(file))`, etc.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be
lazily converted to more friendly Julia defaults; by default, `convert=true`.
"""
struct Table <: Tables.AbstractColumns
    names::Vector{Symbol}
    types::Vector{Type}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol, AbstractVector}
    schema::Ref{Meta.Schema}
    metadata::Ref{Union{Nothing,Base.ImmutableDict{String,String}}}
end

Table() = Table(Symbol[], Type[], AbstractVector[], Dict{Symbol, AbstractVector}(), Ref{Meta.Schema}(), Ref{Union{Nothing,Base.ImmutableDict{String,String}}}(nothing))

function Table(names, types, columns, lookup, schema)
    m = isassigned(schema) ? buildmetadata(schema[]) : nothing
    return Table(names, types, columns, lookup, schema, Ref{Union{Nothing,Base.ImmutableDict{String,String}}}(m))
end

names(t::Table) = getfield(t, :names)
types(t::Table) = getfield(t, :types)
columns(t::Table) = getfield(t, :columns)
lookup(t::Table) = getfield(t, :lookup)
schema(t::Table) = getfield(t, :schema)

"""
    Arrow.getmetadata(x)

If `x isa Arrow.Table` return a `Base.ImmutableDict{String,String}` representation of `x`'s
`Schema` `custom_metadata`, or `nothing` if no such metadata exists.

If `x isa Arrow.ArrowVector`, return a `Base.ImmutableDict{String,String}` representation of `x`'s
`Field` `custom_metadata`, or `nothing` if no such metadata exists.

Otherwise, return `nothing`.

See [the official Arrow documentation for more details on custom application metadata](https://arrow.apache.org/docs/format/Columnar.html#custom-application-metadata).
"""
getmetadata(t::Table) = getfield(t, :metadata)[]
getmetadata(::Any) = nothing

Tables.istable(::Table) = true
Tables.columnaccess(::Table) = true
Tables.columns(t::Table) = Tables.CopiedColumns(t)
Tables.schema(t::Table) = Tables.Schema(names(t), types(t))
Tables.columnnames(t::Table) = names(t)
Tables.getcolumn(t::Table, i::Int) = columns(t)[i]
Tables.getcolumn(t::Table, nm::Symbol) = lookup(t)[nm]

# high-level user API functions
Table(input, pos::Integer=1, len=nothing; kw...) = Table([ArrowBlob(tobytes(input), pos, len)]; kw...)
Table(input::Vector{UInt8}, pos::Integer=1, len=nothing; kw...) = Table([ArrowBlob(tobytes(input), pos, len)]; kw...)
Table(inputs::Vector; kw...) = Table([ArrowBlob(tobytes(x), 1, nothing) for x in inputs]; kw...)

# will detect whether we're reading a Table from a file or stream
function Table(blobs::Vector{ArrowBlob}; convert::Bool=true)
    t = Table()
    sch = nothing
    dictencodings = Dict{Int64, DictEncoding}() # dictionary id => DictEncoding
    dictencoded = Dict{Int64, Meta.Field}() # dictionary id => field
    tsks = Channel{Task}(Inf)
    tsk = Threads.@spawn begin
        i = 1
        for tsk in tsks
            cols = fetch(tsk)
            if i == 1
                foreach(x -> push!(columns(t), x), cols)
            elseif i == 2
                foreach(1:length(cols)) do i
                    columns(t)[i] = ChainedVector([columns(t)[i], cols[i]])
                end
            else
                foreach(1:length(cols)) do i
                    append!(columns(t)[i], cols[i])
                end
            end
            i += 1
        end
    end
    anyrecordbatches = false
    for blob in blobs
        bytes, pos, len = blob.bytes, blob.pos, blob.len
        if len > 24 &&
            _startswith(bytes, pos, FILE_FORMAT_MAGIC_BYTES) &&
            _endswith(bytes, pos + len - 1, FILE_FORMAT_MAGIC_BYTES)
            pos += 8 # skip past magic bytes + padding
        end
        for batch in BatchIterator(bytes, pos)
            # store custom_metadata of batch.msg?
            header = batch.msg.header
            if header isa Meta.Schema
                @debug 1 "parsing schema message"
                # assert endianness?
                # store custom_metadata?
                if sch === nothing
                    for (i, field) in enumerate(header.fields)
                        push!(names(t), Symbol(field.name))
                        # recursively find any dictionaries for any fields
                        getdictionaries!(dictencoded, field)
                        @debug 1 "parsed column from schema: field = $field"
                    end
                    sch = header
                    schema(t)[] = sch
                elseif sch != header
                    throw(ArgumentError("mismatched schemas between different arrow batches: $sch != $header"))
                end
            elseif header isa Meta.DictionaryBatch
                id = header.id
                recordbatch = header.data
                @debug 1 "parsing dictionary batch message: id = $id, compression = $(recordbatch.compression)"
                if haskey(dictencodings, id) && header.isDelta
                    # delta
                    field = dictencoded[id]
                    values, _, _ = build(field, field.type, batch, recordbatch, dictencodings, Int64(1), Int64(1), convert)
                    dictencoding = dictencodings[id]
                    if typeof(dictencoding.data) <: ChainedVector
                        append!(dictencoding.data, values)
                    else
                        A = ChainedVector([dictencoding.data, values])
                        S = field.dictionary.indexType === nothing ? Int32 : juliaeltype(field, field.dictionary.indexType, false)
                        dictencodings[id] = DictEncoding{eltype(A), S, typeof(A)}(id, A, field.dictionary.isOrdered, values.metadata)
                    end
                    continue
                end
                # new dictencoding or replace
                field = dictencoded[id]
                values, _, _ = build(field, field.type, batch, recordbatch, dictencodings, Int64(1), Int64(1), convert)
                A = values
                S = field.dictionary.indexType === nothing ? Int32 : juliaeltype(field, field.dictionary.indexType, false)
                dictencodings[id] = DictEncoding{eltype(A), S, typeof(A)}(id, A, field.dictionary.isOrdered, values.metadata)
                @debug 1 "parsed dictionary batch message: id=$id, data=$values\n"
            elseif header isa Meta.RecordBatch
                anyrecordbatches = true
                @debug 1 "parsing record batch message: compression = $(header.compression)"
                put!(tsks, Threads.@spawn begin
                    collect(VectorIterator(sch, batch, dictencodings, convert))
                end)
            else
                throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
            end
        end
    end
    close(tsks)
    wait(tsk)
    lu = lookup(t)
    ty = types(t)
    # 158; some implementations may send 0 record batches
    if !anyrecordbatches
        for field in sch.fields
            T = juliaeltype(field, buildmetadata(field), convert)
            push!(columns(t), T[])
        end
    end
    for (nm, col) in zip(names(t), columns(t))
        lu[nm] = col
        push!(ty, eltype(col))
    end
    getfield(t, :metadata)[] = buildmetadata(sch)
    return t
end

function getdictionaries!(dictencoded, field)
    d = field.dictionary
    if d !== nothing
        dictencoded[d.id] = field
    end
    for child in field.children
        getdictionaries!(dictencoded, child)
    end
    return
end

struct Batch
    msg::Meta.Message
    bytes::Vector{UInt8}
    pos::Int
    id::Int
end

function Base.iterate(x::BatchIterator, (pos, id)=(x.startpos, 0))
    @debug 1 "checking for next arrow message: pos = $pos"
    if pos + 3 > length(x.bytes)
        @debug 1 "not enough bytes left for another batch message"
        return nothing
    end
    if readbuffer(x.bytes, pos, UInt32) != CONTINUATION_INDICATOR_BYTES
        @debug 1 "didn't find continuation byte to keep parsing messages: $(readbuffer(x.bytes, pos, UInt32))"
        return nothing
    end
    pos += 4
    if pos + 3 > length(x.bytes)
        @debug 1 "not enough bytes left to read length of another batch message"
        return nothing
    end
    msglen = readbuffer(x.bytes, pos, Int32)
    if msglen == 0
        @debug 1 "message has 0 length; terminating message parsing"
        return nothing
    end
    pos += 4
    msg = FlatBuffers.getrootas(Meta.Message, x.bytes, pos-1)
    pos += msglen
    # pos now points to message body
    @debug 1 "parsing message: pos = $pos, msglen = $msglen, bodyLength = $(msg.bodyLength)"
    return Batch(msg, x.bytes, pos, id), (pos + msg.bodyLength, id + 1)
end

struct VectorIterator
    schema::Meta.Schema
    batch::Batch # batch.msg.header MUST BE RecordBatch
    dictencodings::Dict{Int64, DictEncoding}
    convert::Bool
end

buildmetadata(f::Union{Meta.Field,Meta.Schema}) = buildmetadata(f.custom_metadata)
buildmetadata(meta) = toidict(String(kv.key) => String(kv.value) for kv in meta)
buildmetadata(::Nothing) = nothing
buildmetadata(x::AbstractDict) = x

function Base.iterate(x::VectorIterator, (columnidx, nodeidx, bufferidx)=(Int64(1), Int64(1), Int64(1)))
    columnidx > length(x.schema.fields) && return nothing
    field = x.schema.fields[columnidx]
    @debug 2 "building top-level column: field = $(field), columnidx = $columnidx, nodeidx = $nodeidx, bufferidx = $bufferidx"
    A, nodeidx, bufferidx = build(field, x.batch, x.batch.msg.header, x.dictencodings, nodeidx, bufferidx, x.convert)
    @debug 2 "built top-level column: A = $(typeof(A)), columnidx = $columnidx, nodeidx = $nodeidx, bufferidx = $bufferidx"
    @debug 3 A
    return A, (columnidx + 1, nodeidx, bufferidx)
end

Base.length(x::VectorIterator) = length(x.schema.fields)

const ListTypes = Union{Meta.Utf8, Meta.LargeUtf8, Meta.Binary, Meta.LargeBinary, Meta.List, Meta.LargeList}
const LargeLists = Union{Meta.LargeUtf8, Meta.LargeBinary, Meta.LargeList}

function build(field::Meta.Field, batch, rb, de, nodeidx, bufferidx, convert)
    d = field.dictionary
    if d !== nothing
        validity = buildbitmap(batch, rb, nodeidx, bufferidx)
        bufferidx += 1
        buffer = rb.buffers[bufferidx]
        S = d.indexType === nothing ? Int32 : juliaeltype(field, d.indexType, false)
        bytes, indices = reinterp(S, batch, buffer, rb.compression)
        encoding = de[d.id]
        A = DictEncoded(bytes, validity, indices, encoding, buildmetadata(field.custom_metadata))
        nodeidx += 1
        bufferidx += 1
    else
        A, nodeidx, bufferidx = build(field, field.type, batch, rb, de, nodeidx, bufferidx, convert)
    end
    return A, nodeidx, bufferidx
end

function buildbitmap(batch, rb, nodeidx, bufferidx)
    buffer = rb.buffers[bufferidx]
    voff = batch.pos + buffer.offset
    node = rb.nodes[nodeidx]
    if rb.compression === nothing
        return ValidityBitmap(batch.bytes, voff, node.length, node.null_count)
    else
        # compressed
        ptr = pointer(batch.bytes, voff)
        _, decodedbytes = uncompress(ptr, buffer, rb.compression)
        return ValidityBitmap(decodedbytes, 1, node.length, node.null_count)
    end
end

function uncompress(ptr::Ptr{UInt8}, buffer, compression)
    if buffer.length == 0
        return 0, UInt8[]
    end
    len = unsafe_load(convert(Ptr{Int64}, ptr))
    ptr += 8 # skip past uncompressed length as Int64
    encodedbytes = unsafe_wrap(Array, ptr, buffer.length - 8)
    if compression.codec === Meta.CompressionType.LZ4_FRAME
        decodedbytes = transcode(LZ4FrameDecompressor, encodedbytes)
    elseif compression.codec === Meta.CompressionType.ZSTD
        decodedbytes = transcode(ZstdDecompressor, encodedbytes)
    else
        error("unsupported compression type when reading arrow buffers: $(typeof(compression.codec))")
    end
    return len, decodedbytes
end

function reinterp(::Type{T}, batch, buf, compression) where {T}
    ptr = pointer(batch.bytes, batch.pos + buf.offset)
    if compression === nothing
        return batch.bytes, unsafe_wrap(Array, convert(Ptr{T}, ptr), div(buf.length, sizeof(T)))
    else
        # compressed
        len, decodedbytes = uncompress(ptr, buf, compression)
        return decodedbytes, unsafe_wrap(Array, convert(Ptr{T}, pointer(decodedbytes)), div(len, sizeof(T)))
    end
end

function build(f::Meta.Field, L::ListTypes, batch, rb, de, nodeidx, bufferidx, convert)
    @debug 2 "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    ooff = batch.pos + buffer.offset
    OT = L isa LargeLists ? Int64 : Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.Utf8 || L isa Meta.LargeUtf8 || L isa Meta.Binary || L isa Meta.LargeBinary
        buffer = rb.buffers[bufferidx]
        bytes, A = reinterp(UInt8, batch, buffer, rb.compression)
        bufferidx += 1
    else
        bytes = UInt8[]
        A, nodeidx, bufferidx = build(f.children[1], batch, rb, de, nodeidx, bufferidx, convert)
    end
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return List{T, OT, typeof(A)}(bytes, validity, offsets, A, len, meta), nodeidx, bufferidx
end

function build(f::Meta.Field, L::Union{Meta.FixedSizeBinary, Meta.FixedSizeList}, batch, rb, de, nodeidx, bufferidx, convert)
    @debug 2 "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.FixedSizeBinary
        buffer = rb.buffers[bufferidx]
        bytes, A = reinterp(UInt8, batch, buffer, rb.compression)
        bufferidx += 1
    else
        bytes = UInt8[]
        A, nodeidx, bufferidx = build(f.children[1], batch, rb, de, nodeidx, bufferidx, convert)
    end
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return FixedSizeList{T, typeof(A)}(bytes, validity, A, len, meta), nodeidx, bufferidx
end

function build(f::Meta.Field, L::Meta.Map, batch, rb, de, nodeidx, bufferidx, convert)
    @debug 2 "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    ooff = batch.pos + buffer.offset
    OT = Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    A, nodeidx, bufferidx = build(f.children[1], batch, rb, de, nodeidx, bufferidx, convert)
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return Map{T, OT, typeof(A)}(validity, offsets, A, len, meta), nodeidx, bufferidx
end

function build(f::Meta.Field, L::Meta.Struct, batch, rb, de, nodeidx, bufferidx, convert)
    @debug 2 "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    vecs = []
    nodeidx += 1
    for child in f.children
        A, nodeidx, bufferidx = build(child, batch, rb, de, nodeidx, bufferidx, convert)
        push!(vecs, A)
    end
    data = Tuple(vecs)
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return Struct{T, typeof(data)}(validity, data, len, meta), nodeidx, bufferidx
end

function build(f::Meta.Field, L::Meta.Union, batch, rb, de, nodeidx, bufferidx, convert)
    @debug 2 "building array: L = $L"
    buffer = rb.buffers[bufferidx]
    bytes, typeIds = reinterp(UInt8, batch, buffer, rb.compression)
    bufferidx += 1
    if L.mode == Meta.UnionMode.Dense
        buffer = rb.buffers[bufferidx]
        bytes2, offsets = reinterp(Int32, batch, buffer, rb.compression)
        bufferidx += 1
    end
    vecs = []
    nodeidx += 1
    for child in f.children
        A, nodeidx, bufferidx = build(child, batch, rb, de, nodeidx, bufferidx, convert)
        push!(vecs, A)
    end
    data = Tuple(vecs)
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    UT = UnionT(f, convert)
    if L.mode == Meta.UnionMode.Dense
        B = DenseUnion{T, UT, typeof(data)}(bytes, bytes2, typeIds, offsets, data, meta)
    else
        B = SparseUnion{T, UT, typeof(data)}(bytes, typeIds, data, meta)
    end
    return B, nodeidx, bufferidx
end

function build(f::Meta.Field, L::Meta.Null, batch, rb, de, nodeidx, bufferidx, convert)
    @debug 2 "building array: L = $L"
    meta = buildmetadata(f.custom_metadata)
    T = juliaeltype(f, meta, convert)
    return NullVector{maybemissing(T)}(MissingVector(rb.nodes[nodeidx].length), meta), nodeidx + 1, bufferidx
end

# primitives
function build(f::Meta.Field, ::L, batch, rb, de, nodeidx, bufferidx, convert) where {L}
    @debug 2 "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    meta = buildmetadata(f.custom_metadata)
    # get storage type (non-converted)
    T = juliaeltype(f, nothing, false)
    @debug 2 "storage type for primitive: T = $T"
    bytes, A = reinterp(Base.nonmissingtype(T), batch, buffer, rb.compression)
    len = rb.nodes[nodeidx].length
    T = juliaeltype(f, meta, convert)
    @debug 2 "final julia type for primitive: T = $T"
    return Primitive(T, bytes, validity, A, len, meta), nodeidx + 1, bufferidx + 1
end

function build(f::Meta.Field, L::Meta.Bool, batch, rb, de, nodeidx, bufferidx, convert)
    @debug 2 "building array: L = $L"
    validity = buildbitmap(batch, rb, nodeidx, bufferidx)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    meta = buildmetadata(f.custom_metadata)
    # get storage type (non-converted)
    T = juliaeltype(f, nothing, false)
    @debug 2 "storage type for primitive: T = $T"
    buffer = rb.buffers[bufferidx]
    voff = batch.pos + buffer.offset
    node = rb.nodes[nodeidx]
    if rb.compression === nothing
        decodedbytes = batch.bytes
        pos = voff
        # return ValidityBitmap(batch.bytes, voff, node.length, node.null_count)
    else
        # compressed
        ptr = pointer(batch.bytes, voff)
        _, decodedbytes = uncompress(ptr, buffer, rb.compression)
        pos = 1
        # return ValidityBitmap(decodedbytes, 1, node.length, node.null_count)
    end
    len = rb.nodes[nodeidx].length
    T = juliaeltype(f, meta, convert)
    return BoolVector{T}(decodedbytes, pos, validity, len, meta), nodeidx + 1, bufferidx + 1
end
