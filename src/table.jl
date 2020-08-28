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
end

Table() = Table(Symbol[], Type[], AbstractVector[], Dict{Symbol, AbstractVector}())

names(t::Table) = getfield(t, :names)
types(t::Table) = getfield(t, :types)
columns(t::Table) = getfield(t, :columns)
lookup(t::Table) = getfield(t, :lookup)

Tables.istable(::Table) = true
Tables.columnaccess(::Table) = true
Tables.columns(t::Table) = t
Tables.schema(t::Table) = Tables.Schema(names(t), types(t))
Tables.columnnames(t::Table) = names(t)
Tables.getcolumn(t::Table, i::Int) = columns(t)[i]
Tables.getcolumn(t::Table, nm::Symbol) = lookup(t)[nm]

# high-level user API functions
Table(io::IO, pos::Integer=1, len=nothing; debug::Bool=false) = Table(Base.read(io), pos, len; debug=debug)
Table(str::String, pos::Integer=1, len=nothing; debug::Bool=false) = Table(Mmap.mmap(str), pos, len; debug=debug)

# will detect whether we're reading a Table from a file or stream
function Table(bytes::Vector{UInt8}, off::Integer=1, tlen::Union{Integer, Nothing}=nothing; debug::Bool=false)
    len = something(tlen, length(bytes))
    if len > 24 &&
        _startswith(bytes, off, FILE_FORMAT_MAGIC_BYTES) &&
        _endswith(bytes, off + len - 1, FILE_FORMAT_MAGIC_BYTES)
        off += 8 # skip past magic bytes + padding
    end
    t = Table()
    schema = nothing
    dictencodings = Dict{Int64, DictEncoding}()
    dictencoded = Dict{Int64, Tuple{Bool, Type, Meta.Field}}()
    for batch in BatchIterator{debug}(bytes, off)
        # store custom_metadata of batch.msg?
        header = batch.msg.header
        if header isa Meta.Schema
            debug && println("parsing schema message")
            # assert endianness?
            # store custom_metadata?
            for field in header.fields
                push!(names(t), Symbol(field.name))
                push!(types(t), juliaeltype(field))
                d = field.dictionary
                isencoded = false
                if d !== nothing
                    dictencoded[d.id] = (d.isOrdered, types(t)[end], field)
                    isencoded = true
                end
                debug && println("parsed column from schema: name=$(names(t)[end]), type=$(types(t)[end])$(isencoded ? " dictencoded" : "")")
            end
            schema = header
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
            dictencodings[id] = DictEncoding(id, ChainedVector([values]), isOrdered)
            debug && println("parsed parsed dictionary batch message: id=$id, data=$values\n")
        elseif header isa Meta.RecordBatch
            debug && println("parsing record batch message")
            if isempty(columns(t))
                # first RecordBatch
                for vec in VectorIterator{debug}(types(t), schema, batch, dictencodings)
                    push!(columns(t), vec)
                end
                debug && println("parsed 1st record batch")
            elseif !(columns(t)[1] isa ChainedVector)
                # second RecordBatch
                for (i, vec) in enumerate(VectorIterator{debug}(types(t), schema, batch, dictencodings))
                    columns(t)[i] = ChainedVector([columns(t)[i], vec])
                end
                debug && println("parsed 2nd record batch")
            else
                # 2+ RecordBatch
                for (i, vec) in enumerate(VectorIterator{debug}(types(t), schema, batch, dictencodings))
                    append!(columns(t)[i], vec)
                end
                debug && println("parsed additional record batch")
            end
        else
            throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
        end
    end
    lu = lookup(t)
    for (k, v) in zip(names(t), columns(t))
        lu[k] = v
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
    pos + 3 > length(x.bytes) && return nothing
    readbuffer(x.bytes, pos, UInt32) == CONTINUATION_INDICATOR_BYTES || return nothing
    pos += 4
    pos + 3 > length(x.bytes) && return nothing
    msglen = readbuffer(x.bytes, pos, Int32)
    msglen == 0 && return nothing
    pos += 4
    msg = FlatBuffers.getrootas(Meta.Message, x.bytes, pos-1)
    pos += msglen
    # pos now points to message body
    return Batch(msg, x.bytes, pos), pos + msg.bodyLength
end

struct VectorIterator{debug}
    types::Vector{Type}
    schema::Meta.Schema
    batch::Batch # batch.msg.header MUST BE RecordBatch
    dictencodings::Dict{Int64, DictEncoding}
end

function Base.iterate(x::VectorIterator{debug}, (columnidx, nodeidx, bufferidx)=(1, 1, 1)) where {debug}
    columnidx > length(x.schema.fields) && return nothing
    field = x.schema.fields[columnidx]
    d = field.dictionary
    if d !== nothing
        validity = buildbitmap(x.batch, x.batch.msg.header, bufferidx, debug)
        bufferidx += 1
        buffer = x.batch.msg.header.buffers[bufferidx]
        debug && @show columnidx, buffer.offset, buffer.length
        S = d.indexType === nothing ? Int32 : juliaeltype(field, d.indexType)
        ptr = convert(Ptr{S}, pointer(x.batch.bytes, x.batch.pos + buffer.offset))
        indices = unsafe_wrap(Array, ptr, div(buffer.length, sizeof(S)))
        encoding = x.dictencodings[d.id]
        A = DictEncoded{x.types[columnidx], eltype(indices)}(validity, indices, encoding)
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

function buildbitmap(batch, rb, bufferidx, debug)
    buffer = rb.buffers[bufferidx]
    debug && @show :validity_bitmap, buffer.offset, buffer.length
    voff = batch.pos + buffer.offset
    return ValidityBitmap(batch.bytes, voff, buffer.length)
end

function build(T, f::Meta.Field, L::ListTypes, batch, rb, nodeidx, bufferidx, debug)
    validity = buildbitmap(batch, rb, bufferidx, debug)
    debug && @show validity
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
    ooff = batch.pos + buffer.offset
    OT = L isa LargeLists ? Int64 : Int32
    ptr = convert(Ptr{OT}, pointer(batch.bytes, ooff))
    offsets = Offsets(unsafe_wrap(Array, ptr, div(buffer.length, sizeof(OT))))
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.Utf8 || L isa Meta.LargeUtf8 || L isa Meta.Binary || L isa Meta.LargeBinary
        buffer = rb.buffers[bufferidx]
        debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
        bytesptr = pointer(batch.bytes, batch.pos + buffer.offset)
        A = unsafe_wrap(Array, bytesptr, buffer.length)
        bufferidx += 1
    else
        A, nodeidx, bufferidx = build(eltype(Base.nonmissingtype(T)), f.children[1], batch, rb, nodeidx, bufferidx, debug)
    end
    return List{T, OT, typeof(A)}(validity, offsets, A, len), nodeidx, bufferidx
end

function build(T, f::Meta.Field, L::Union{Meta.FixedSizeBinary, Meta.FixedSizeList}, batch, rb, nodeidx, bufferidx, debug)
    validity = buildbitmap(batch, rb, bufferidx, debug)
    debug && @show validity
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    nodeidx += 1
    if L isa Meta.FixedSizeBinary
        buffer = rb.buffers[bufferidx]
        debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
        bytesptr = pointer(batch.bytes, batch.pos + buffer.offset)
        A = unsafe_wrap(Array, bytesptr, buffer.length)
        bufferidx += 1
    else
        A, nodeidx, bufferidx = build(eltype(Base.nonmissingtype(T)), f.children[1], batch, rb, nodeidx, bufferidx, debug)
    end
    return FixedSizeList{T, typeof(A)}(validity, A, len), nodeidx, bufferidx
end

function build(S, f::Meta.Field, L::Meta.Map, batch, rb, nodeidx, bufferidx, debug)
    T = Base.nonmissingtype(S)
    A, nodeidx, bufferidx = build(_keytype(T), f.children[1].children[1], batch, rb, nodeidx + 2, bufferidx, debug)
    B, nodeidx, bufferidx = build(_valtype(T), f.children[1].children[2], batch, rb, nodeidx, bufferidx, debug)
    return Map{_keytype(T), _valtype(T), typeof(A), typeof(B)}(A, B), nodeidx, bufferidx
end

function build(T, f::Meta.Field, L::Meta.Struct, batch, rb, nodeidx, bufferidx, debug)
    validity = buildbitmap(batch, rb, bufferidx, debug)
    debug && @show validity
    bufferidx += 1
    len = rb.nodes[nodeidx].length
    NT = Base.nonmissingtype(T)
    N = getn(NT)
    vecs = []
    nodeidx += 1
    for i = 1:N
        A, nodeidx, bufferidx = build(fieldtype(NT, i), f.children[i], batch, rb, nodeidx, bufferidx, debug)
        push!(vecs, A)
    end
    data = Tuple(vecs)
    return Struct{T, typeof(data)}(validity, data, len), nodeidx, bufferidx
end

function build(T, f::Meta.Field, L::Meta.Union, batch, rb, nodeidx, bufferidx, debug)
    buffer = rb.buffers[bufferidx]
    debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
    typeidsptr = pointer(batch.bytes, batch.pos + buffer.offset)
    typeIds = unsafe_wrap(Array, typeidsptr, buffer.length)
    debug && @show typeIds
    bufferidx += 1
    if L.mode == Meta.UnionMode.Dense
        buffer = rb.buffers[bufferidx]
        debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
        ooff = batch.pos + buffer.offset
        ptr = convert(Ptr{Int32}, pointer(batch.bytes, ooff))
        offsets = unsafe_wrap(Array, ptr, div(buffer.length, 4))
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
        return DenseUnion{T, typeof(data)}(typeIds, offsets, data), nodeidx, bufferidx
    else
        return SparseUnion{T, typeof(data)}(typeIds, data), nodeidx, bufferidx
    end
end

function build(T, f::Meta.Field, L::Meta.Null, batch, rb, nodeidx, bufferidx, debug)
    return MissingVector(rb.nodes[nodeidx].length), nodeidx + 1, bufferidx
end

# primitives
function build(T, f::Meta.Field, ::L, batch, rb, nodeidx, bufferidx, debug) where {L}
    validity = buildbitmap(batch, rb, bufferidx, debug)
    debug && @show validity
    bufferidx += 1
    buffer = rb.buffers[bufferidx]
    debug && @show T, nodeidx, bufferidx, buffer.offset, buffer.length
    S = Base.nonmissingtype(T)
    ptr = convert(Ptr{S}, pointer(batch.bytes, batch.pos + buffer.offset))
    A = unsafe_wrap(Array, ptr, div(buffer.length, sizeof(S)))
    len = rb.nodes[nodeidx].length
    return Primitive{T, eltype(A)}(validity, A, len), nodeidx + 1, bufferidx + 1
end
