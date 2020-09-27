"""
    Arrow.Table(io::IO)
    Arrow.Table(file::String)
    Arrow.Table(bytes::Vector{UInt8}, pos=1, len=nothing)

Read an arrow formatted table, from:
 * `io`, bytes come from `read(io)`
 * `file`, bytes come from `Mmap.mmap(file)`
 # `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`

Returns a `Arrow.Table` object that allows column access via `table.col1`, `table[:col1]`, or `table[1]`.

`Arrow.Table` also satisfies the Tables.jl interface, and so can easily be materialied via any supporting
sink function: e.g. `DataFrame(Arrow.Table(file))`, `SQLite.load!(db, "table", Arrow.Table(file))`, etc.
"""
struct Table <: Tables.AbstractColumns
    names::Vector{Symbol}
    types::Vector{Type}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol, AbstractVector}
    schema::Ref{Any}
end

Table() = Table(Symbol[], Type[], AbstractVector[], Dict{Symbol, AbstractVector}(), Ref{Meta.Schema}())

names(t::Table) = getfield(t, :names)
types(t::Table) = getfield(t, :types)
columns(t::Table) = getfield(t, :columns)
lookup(t::Table) = getfield(t, :lookup)
schema(t::Table) = getfield(t, :schema)

Tables.istable(::Table) = true
Tables.columnaccess(::Table) = true
Tables.columns(t::Table) = Tables.CopiedColumns(t)
Tables.schema(t::Table) = Tables.Schema(names(t), types(t))
Tables.columnnames(t::Table) = names(t)
Tables.getcolumn(t::Table, i::Int) = columns(t)[i]
Tables.getcolumn(t::Table, nm::Symbol) = lookup(t)[nm]

# high-level user API functions
Table(io::IO, pos::Integer=1, len=nothing; debug::Bool=false, convert::Bool=true) = Table(Base.read(io), pos, len; debug=debug, convert=convert)
Table(str::String, pos::Integer=1, len=nothing; debug::Bool=false, convert::Bool=true) = Table(Mmap.mmap(str), pos, len; debug=debug, convert=convert)

# will detect whether we're reading a Table from a file or stream
function Table(bytes::Vector{UInt8}, off::Integer=1, tlen::Union{Integer, Nothing}=nothing; debug::Bool=false, convert::Bool=true)
    len = something(tlen, length(bytes))
    if len > 24 &&
        _startswith(bytes, off, FILE_FORMAT_MAGIC_BYTES) &&
        _endswith(bytes, off + len - 1, FILE_FORMAT_MAGIC_BYTES)
        off += 8 # skip past magic bytes + padding
    end
    t = Table()
    sch = nothing
    dictencodings = Dict{Int64, DictEncoding}()
    dictencoded = Dict{Int64, Tuple{Bool, Type, Meta.Field}}()
    fieldmetadata = Dict{Int, Dict{String, String}}()
    for batch in BatchIterator{debug}(bytes, off)
        # store custom_metadata of batch.msg?
        header = batch.msg.header
        if header isa Meta.Schema
            debug && println("parsing schema message")
            # assert endianness?
            # store custom_metadata?
            for (i, field) in enumerate(header.fields)
                push!(names(t), Symbol(field.name))
                T, metadata = juliaeltype(field)
                if metadata !== nothing
                    fieldmetadata[i] = metadata
                end
                push!(types(t), T)
                d = field.dictionary
                isencoded = false
                if d !== nothing
                    dictencoded[d.id] = (d.isOrdered, types(t)[end], field)
                    isencoded = true
                end
                debug && println("parsed column from schema: name=$(names(t)[end]), type=$(types(t)[end])$(isencoded ? " dictencoded" : "")")
            end
            sch = header
            schema(t)[] = sch
        elseif header isa Meta.DictionaryBatch
            debug && println("parsing dictionary batch message")
            id = header.id
            recordbatch = header.data
            if haskey(dictencodings, id) && header.isDelta
                # delta
                isOrdered, T, field = dictencoded[id]
                values, _, _ = build(T, field, batch, recordbatch, 1, 1, debug)
                dictencoding = dictencodings[id]
                append!(dictencoding.data, values)
                continue
            end
            # new dictencoding or replace
            isOrdered, T, field = dictencoded[id]
            values, _, _ = build(T, field, batch, recordbatch, 1, 1, debug)
            A = ChainedVector([values])
            dictencodings[id] = DictEncoding{eltype(A), typeof(A)}(id, A, isOrdered)
            debug && println("parsed dictionary batch message: id=$id, data=$values\n")
        elseif header isa Meta.RecordBatch
            debug && println("parsing record batch message")
            if isempty(columns(t))
                # first RecordBatch
                for vec in VectorIterator{debug}(types(t), sch, batch, dictencodings)
                    push!(columns(t), vec)
                end
                debug && println("parsed 1st record batch")
            elseif !(columns(t)[1] isa ChainedVector)
                # second RecordBatch
                for (i, vec) in enumerate(VectorIterator{debug}(types(t), sch, batch, dictencodings))
                    columns(t)[i] = ChainedVector([columns(t)[i], vec])
                end
                debug && println("parsed 2nd record batch")
            else
                # 2+ RecordBatch
                for (i, vec) in enumerate(VectorIterator{debug}(types(t), sch, batch, dictencodings))
                    append!(columns(t)[i], vec)
                end
                debug && println("parsed additional record batch")
            end
        else
            throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
        end
    end
    lu = lookup(t)
    for (i, (k, T, col)) in enumerate(zip(names(t), types(t), columns(t)))
        if haskey(fieldmetadata, i) && haskey(fieldmetadata[i], "ARROW:extension:name")
            if fieldmetadata[i]["ARROW:extension:name"] == "JuliaLang.Symbol"
                TT = finaljuliatype(Symbol)
                types(t)[i] = TT
                col = converter(TT, col)
                columns(t)[i] = col
            elseif fieldmetadata[i]["ARROW:extension:name"] == "JuliaLang.Char"
                TT = finaljuliatype(Char)
                types(t)[i] = TT
                col = converter(TT, col)
                columns(t)[i] = col
            end
        end
        if convert
            TT = finaljuliatype(T)
            if TT !== T
                types(t)[i] = TT
                col = converter(TT, col)
                columns(t)[i] = col
            end
        end
        lu[k] = col
    end
    meta = sch.custom_metadata
    if meta !== nothing
        setmetadata!(t, Dict(String(kv.key) => String(kv.value) for kv in meta))
    end
    return t
end

struct BatchIterator{debug}
    bytes::Vector{UInt8}
    startpos::Int
end

struct Batch
    msg::Meta.Message
    bytes::Vector{UInt8}
    pos::Int
end

function Base.iterate(x::BatchIterator{debug}, pos=x.startpos) where {debug}
    if pos + 3 > length(x.bytes)
        debug && println("not enough bytes left for another batch message")
        return nothing
    end
    if readbuffer(x.bytes, pos, UInt32) != CONTINUATION_INDICATOR_BYTES
        debug && println("didn't find continuation byte to keep parsing messages: $(readbuffer(x.bytes, pos, UInt32))")
        return nothing
    end
    pos += 4
    if pos + 3 > length(x.bytes)
        debug && println("not enough bytes left to read length of another batch message")
        return nothing
    end
    msglen = readbuffer(x.bytes, pos, Int32)
    if msglen == 0
        debug && println("message has 0 length; terminating message parsing")
        return nothing
    else
        debug && println("parsing message with msglen = $msglen")
    end
    pos += 4
    msg = FlatBuffers.getrootas(Meta.Message, x.bytes, pos-1)
    pos += msglen
    # pos now points to message body
    debug && @show msg.bodyLength
    return Batch(msg, x.bytes, pos), pos + msg.bodyLength
end

struct VectorIterator{debug}
    types::Vector{Type}
    schema::Meta.Schema
    batch::Batch # batch.msg.header MUST BE RecordBatch
    dictencodings::Dict{Int64, DictEncoding}
end

buildmetadata(meta) = Dict(String(kv.key) => String(kv.value) for kv in meta)
buildmetadata(::Nothing) = nothing

function Base.iterate(x::VectorIterator{debug}, (columnidx, nodeidx, bufferidx)=(1, 1, 1)) where {debug}
    columnidx > length(x.schema.fields) && return nothing
    field = x.schema.fields[columnidx]
    d = field.dictionary
    if d !== nothing
        validity = buildbitmap(x.batch, x.batch.msg.header, nodeidx, bufferidx, debug)
        bufferidx += 1
        buffer = x.batch.msg.header.buffers[bufferidx]
        debug && @show columnidx, buffer.offset, buffer.length
        S = d.indexType === nothing ? Int32 : juliaeltype(field, d.indexType)
        bytes, indices = reinterp(S, x.batch, buffer, x.batch.msg.header.compression)
        encoding = x.dictencodings[d.id]
        A = DictEncoded{x.types[columnidx], eltype(indices), typeof(encoding.data)}(bytes, validity, indices, encoding, buildmetadata(field.custom_metadata))
        nodeidx += 1
        bufferidx += 1
    else
        debug && println("parsing column=$columnidx, T=$(x.types[columnidx]), len=$(x.batch.msg.header.nodes[nodeidx].length)")
        A, nodeidx, bufferidx = build(x.types[columnidx], field, x.batch, x.batch.msg.header, nodeidx, bufferidx, debug)
    end
    return A, (columnidx + 1, nodeidx, bufferidx)
end

const ListTypes = Union{Meta.Utf8, Meta.LargeUtf8, Meta.Binary, Meta.LargeBinary, Meta.List, Meta.LargeList}
const LargeLists = Union{Meta.LargeUtf8, Meta.LargeBinary, Meta.LargeList}

build(T, f::Meta.Field, batch, rb, nodeidx, bufferidx, debug) =
    build(T, f, f.type, batch, rb, nodeidx, bufferidx, debug)

function buildbitmap(batch, rb, nodeidx, bufferidx, debug)
    buffer = rb.buffers[bufferidx]
    debug && @show :validity_bitmap, buffer.offset, buffer.length
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

function build(T, f::Meta.Field, L::ListTypes, batch, rb, nodeidx, bufferidx, debug)
    validity = buildbitmap(batch, rb, nodeidx, bufferidx, debug)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
    ooff = batch.pos + buffer.offset
    OT = L isa LargeLists ? Int64 : Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.Utf8 || L isa Meta.LargeUtf8 || L isa Meta.Binary || L isa Meta.LargeBinary
        buffer = rb.buffers[bufferidx]
        debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
        bytes, A = reinterp(UInt8, batch, buffer, rb.compression)
        bufferidx += 1
    else
        bytes = UInt8[]
        A, nodeidx, bufferidx = build(eltype(Base.nonmissingtype(T)), f.children[1], batch, rb, nodeidx, bufferidx, debug)
    end
    return List{T, OT, typeof(A)}(bytes, validity, offsets, A, len, buildmetadata(f.custom_metadata)), nodeidx, bufferidx
end

function build(T, f::Meta.Field, L::Union{Meta.FixedSizeBinary, Meta.FixedSizeList}, batch, rb, nodeidx, bufferidx, debug)
    validity = buildbitmap(batch, rb, nodeidx, bufferidx, debug)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.FixedSizeBinary
        buffer = rb.buffers[bufferidx]
        debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
        bytes, A = reinterp(UInt8, batch, buffer, rb.compression)
        bufferidx += 1
    else
        bytes = UInt8[]
        A, nodeidx, bufferidx = build(eltype(Base.nonmissingtype(T)), f.children[1], batch, rb, nodeidx, bufferidx, debug)
    end
    return FixedSizeList{T, typeof(A)}(bytes, validity, A, len, buildmetadata(f.custom_metadata)), nodeidx, bufferidx
end

function build(S, f::Meta.Field, L::Meta.Map, batch, rb, nodeidx, bufferidx, debug)
    T = Base.nonmissingtype(S)
    validity = buildbitmap(batch, rb, nodeidx, bufferidx, debug)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
    ooff = batch.pos + buffer.offset
    OT = Int32
    bytes, offs = reinterp(OT, batch, buffer, rb.compression)
    offsets = Offsets(bytes, offs)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    A, nodeidx, bufferidx = build(KeyValue{keytype(T), valtype(T)}, f.children[1], batch, rb, nodeidx, bufferidx, debug)
    return Map{S, OT, typeof(A)}(validity, offsets, A, len, buildmetadata(f.custom_metadata)), nodeidx, bufferidx
end

function build(T, f::Meta.Field, L::Meta.Struct, batch, rb, nodeidx, bufferidx, debug)
    validity = buildbitmap(batch, rb, nodeidx, bufferidx, debug)
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    NT = Base.nonmissingtype(T)
    N = fieldcount(NT)
    vecs = []
    nodeidx += 1
    for i = 1:N
        A, nodeidx, bufferidx = build(fieldtype(NT, i), f.children[i], batch, rb, nodeidx, bufferidx, debug)
        push!(vecs, A)
    end
    data = Tuple(vecs)
    return Struct{T, typeof(data)}(validity, data, len, buildmetadata(f.custom_metadata)), nodeidx, bufferidx
end

function build(T, f::Meta.Field, L::Meta.Union, batch, rb, nodeidx, bufferidx, debug)
    buffer = rb.buffers[bufferidx]
    debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
    bytes, typeIds = reinterp(UInt8, batch, buffer, rb.compression)
    debug && @show typeIds
    bufferidx += 1
    if L.mode == Meta.UnionMode.Dense
        buffer = rb.buffers[bufferidx]
        debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
        bytes2, offsets = reinterp(Int32, batch, buffer, rb.compression)
        bufferidx += 1
    end
    vecs = []
    nodeidx += 1
    types = eltype(T)
    for i = 1:fieldcount(types)
        A, nodeidx, bufferidx = build(fieldtype(types, i), f.children[i], batch, rb, nodeidx, bufferidx, debug)
        push!(vecs, A)
    end
    data = Tuple(vecs)
    if L.mode == Meta.UnionMode.Dense
        return DenseUnion{T, typeof(data)}(bytes, bytes2, typeIds, offsets, data, buildmetadata(f.custom_metadata)), nodeidx, bufferidx
    else
        return SparseUnion{T, typeof(data)}(bytes, typeIds, data, buildmetadata(f.custom_metadata)), nodeidx, bufferidx
    end
end

function build(T, f::Meta.Field, L::Meta.Null, batch, rb, nodeidx, bufferidx, debug)
    return MissingVector(rb.nodes[nodeidx].length), nodeidx + 1, bufferidx
end

# primitives
function build(T, f::Meta.Field, ::L, batch, rb, nodeidx, bufferidx, debug) where {L}
    validity = buildbitmap(batch, rb, nodeidx, bufferidx, debug)
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
    S = Base.nonmissingtype(T)
    bytes, A = reinterp(S, batch, buffer, rb.compression)
    len = rb.nodes[nodeidx].length
    return Primitive{T, S, typeof(A)}(bytes, validity, A, len, buildmetadata(f.custom_metadata)), nodeidx + 1, bufferidx + 1
end
