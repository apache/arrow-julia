"""
    Arrow.write(io::IO, tbl)
    Arrow.write(file::String, tbl)

Write any Tables.jl-compatible `tbl` out as arrow formatted data.
Providing an `io::IO` argument will cause the data to be written to it
in the "streaming" format, while providing a `file::String` argument
will result in the "file" format being written.

Multiple record batches will be written based on the number of
`Tables.partitions(tbl)` that are provided; by default, this is just
one for a given table, but some table sources support automatic
partitioning. Note you can turn multiple table objects into partitions
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but do remember that
each table must have the exact same `Tables.Schema`.
"""
function write end

function write(file::String, tbl; debug::Bool=false)
    write(open(file, "w"), tbl, true, debug)
    return file
end

function write(io::IO, tbl; debug::Bool=false)
    return write(io, tbl, false, debug)
end

if isdefined(Tables, :partitions)
    parts = Tables.partitions
else
    parts(x) = (x,)
    parts(x::Tuple) = x
end

function write(io, source, writetofile, debug)
    if writetofile
        Base.write(io, "ARROW1\0\0")
    end
    msgs = Channel{Message}(Inf)
    # build messages
    sch = Ref{Tables.Schema}()
    dictid = Ref(0)
    dictencodings = Dict{Int, Tuple{Int, Type, Any}}()
    # start message writing from channel
@static if VERSION >= v"1.3-DEV"
    tsk = Threads.@spawn for msg in msgs
        Base.write(io, msg)
    end
else
    tsk = @async for msg in msgs
        Base.write(io, msg)
    end
end
    @sync for (i, tbl) in enumerate(parts(source))
        if i == 1
            cols = Tables.columns(tbl)
            sch[] = Tables.schema(cols)
            for (i, col) in enumerate(Tables.Columns(cols))
                if col isa DictEncode
                    id = dictid[]
                    dictid[] += 1
                    values = unique(col)
                    dictencodings[i] = (id, encodingtype(length(values)), values)
                end
            end
            put!(msgs, makeschemamsg(sch[], cols, dictencodings))
            if !isempty(dictencodings)
                for (colidx, (id, T, values)) in dictencodings
                    dictsch = Tables.Schema((:col,), (eltype(values),))
                    put!(msgs, makedictionarybatchmsg(dictsch, (col=values,), id, false, debug))
                end
            end
            put!(msgs, makerecordbatchmsg(sch[], cols, dictencodings, debug))
        else
@static if VERSION >= v"1.3-DEV"
            Threads.@spawn begin
                try
                    cols = Tables.columns(tbl)
                    if !isempty(dictencodings)
                        for (colidx, (id, T, values)) in dictencodings
                            dictsch = Tables.Schema((:col,), (eltype(values),))
                            col = Tables.getcolumn(cols, colidx)
                            # get new values we haven't seen before for delta update
                            vals = setdiff(col, values)
                            put!(msgs, makedictionarybatchmsg(dictsch, (col=vals,), id, true, debug))
                            # add new values to existing set for future diffs
                            union!(values, vals)
                        end
                    end
                    put!(msgs, makerecordbatchmsg(sch[], cols, dictencodings, debug))
                catch e
                    showerror(stdout, e, catch_backtrace())
                    rethrow(e)
                end
            end
else
            @async begin
                try
                    cols = Tables.columns(tbl)
                    if !isempty(dictencodings)
                        for (colidx, (id, T, values)) in dictencodings
                            dictsch = Tables.Schema((:col,), (eltype(values),))
                            col = Tables.getcolumn(cols, colidx)
                            # get new values we haven't seen before for delta update
                            vals = setdiff(col, values)
                            put!(msgs, makedictionarybatchmsg(dictsch, (col=vals,), id, true, debug))
                            # add new values to existing set for future diffs
                            union!(values, vals)
                        end
                    end
                    put!(msgs, makerecordbatchmsg(sch[], cols, dictencodings, debug))
                catch e
                    showerror(stdout, e, catch_backtrace())
                    rethrow(e)
                end
            end
end
        end
    end
    close(msgs)
    wait(tsk)
    # write empty message
    Base.write(io, Message(UInt8[], nothing, nothing, 0))
    if writetofile
        # TODO: writefooter
        # TODO: write footersize
        Base.write(io, "ARROW1")
    end
    # close(io)
    return io
end

struct Message
    msgflatbuf
    columns
    dictencodings
    bodylen
end

struct DictEncoder{T, A, D} <: AbstractVector{T}
    values::A
    pool::D # Dict{eltype(A), I} where I is index type
end

DictEncoder(values::A, unique::B, ::Type{T}) where {A, B, T} =
    DictEncoder{T, A, Dict{eltype(A), T}}(values, Dict{eltype(A), T}(x => (i - 1) for (i, x) in enumerate(unique)))

Base.IndexStyle(::Type{<:DictEncoder}) = Base.IndexLinear()
Base.size(x::DictEncoder) = (length(x.values),)
Base.eltype(x::DictEncoder{T, A}) where {T, A} = T
Base.getindex(x::DictEncoder, i::Int) = x.pool[x.values[i]]

function Base.write(io::IO, msg::Message)
    # now write the final message spec out
    # continuation byte
    n = Base.write(io, 0xFFFFFFFF)
    # metadata length
    n += Base.write(io, Int32(padding(length(msg.msgflatbuf))))
    # message flatbuffer
    n += Base.write(io, msg.msgflatbuf)
    n += writezeros(io, paddinglength(n))
    # message body
    if msg.columns !== nothing
        # write out buffers
        for i = 1:length(Tables.columnnames(msg.columns))
            col = Tables.getcolumn(msg.columns, i)
            if msg.dictencodings !== nothing && haskey(msg.dictencodings, i)
                _, T, vals = msg.dictencodings[i]
                col = DictEncoder(col, vals, T)
            end
            writebuffer(io, eltype(col) === Missing ? Missing : Base.nonmissingtype(eltype(col)), col)
        end
    end
    return n
end

function makemessage(b, headerType, header, columns=nothing, dictencodings=nothing, bodylen=0)
    # write the message flatbuffer object
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V4)
    Meta.messageAddHeaderType(b, headerType)
    Meta.messageAddHeader(b, header)
    Meta.messageAddBodyLength(b, Int64(bodylen))
    # Meta.messageAddCustomMetadata(b, meta)
    # Meta.messageStartCustomMetadataVector(b, num_meta_elems)
    msg = Meta.messageEnd(b)
    FlatBuffers.finish!(b, msg)
    return Message(FlatBuffers.finishedbytes(b), columns, dictencodings, bodylen)
end

function makeschemamsg(sch::Tables.Schema{names, types}, columns, dictencodings) where {names, types}
    b = FlatBuffers.Builder(1024)
    # build Field objects
    N = length(names)
    fieldoffsets = [fieldoffset(b, i, names[i], fieldtype(types, i), dictencodings) for i = 1:N]
    Meta.schemaStartFieldsVector(b, N)
    for off in Iterators.reverse(fieldoffsets)
        FlatBuffers.prependoffset!(b, off)
    end
    fields = FlatBuffers.endvector!(b, N)
    # write schema object
    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fields)
    # Meta.schemaAddCustomMetadata(b, meta)
    schema = Meta.schemaEnd(b)
    return makemessage(b, Meta.Schema, schema)
end

function fieldoffset(b, colidx, name, T, dictencodings)
    nameoff = FlatBuffers.createstring!(b, String(name))
    nullable = T >: Missing
    # build dictionary
    if dictencodings !== nothing && haskey(dictencodings, colidx)
        id, encodingtype, _ = dictencodings[colidx]
        _, inttype, _ = arrowtype(b, encodingtype)
        Meta.dictionaryEncodingStart(b)
        Meta.dictionaryEncodingAddId(b, Int64(id))
        Meta.dictionaryEncodingAddIndexType(b, inttype)
        # TODO: support isOrdered?
        Meta.dictionaryEncodingAddIsOrdered(b, false)
        dict = Meta.dictionaryEncodingEnd(b)
    else
        dict = FlatBuffers.UOffsetT(0)
    end
    type, typeoff, children = arrowtype(b, maybemissing(T))
    if children !== nothing
        Meta.fieldStartChildrenVector(b, length(children))
        for off in Iterators.reverse(children)
            FlatBuffers.prependoffset!(b, off)
        end
        children = FlatBuffers.endvector!(b, length(children))
    else
        children = FlatBuffers.UOffsetT(0)
    end
    # build field object
    Meta.fieldStart(b)
    Meta.fieldAddName(b, nameoff)
    Meta.fieldAddNullable(b, nullable)
    Meta.fieldAddTypeType(b, type)
    Meta.fieldAddType(b, typeoff)
    Meta.fieldAddDictionary(b, dict)
    Meta.fieldAddChildren(b, children)
    # Meta.fieldAddCustomMetadata(b, meta)
    return Meta.fieldEnd(b)
end

struct FieldNode
    length::Int64
    null_count::Int64
end

struct Buffer
    offset::Int64
    length::Int64
end

function makerecordbatchmsg(sch::Tables.Schema{names, types}, columns, dictencodings, debug) where {names, types}
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, dictencodings, debug)
    return makemessage(b, Meta.RecordBatch, recordbatch, columns, dictencodings, bodylen)
end

function makerecordbatch(b, sch::Tables.Schema{names, types}, columns, dictencodings, debug) where {names, types}
    nrows = Tables.rowcount(columns)
    debug && println("building record batch message for $nrows rows")

    fieldnodes = FieldNode[]
    fieldbuffers = Buffer[]
    makenodesbuffers!(1, types, columns, dictencodings, fieldnodes, fieldbuffers, 0)
    debug && @show fieldnodes, fieldbuffers

    # write field nodes objects
    FN = length(fieldnodes)
    Meta.recordBatchStartNodesVector(b, FN)
    for fn in Iterators.reverse(fieldnodes)
        Meta.createFieldNode(b, fn.length, fn.null_count)
    end
    nodes = FlatBuffers.endvector!(b, FN)

    # write buffer objects
    bodylen = 0
    BN = length(fieldbuffers)
    Meta.recordBatchStartBuffersVector(b, BN)
    for buf in Iterators.reverse(fieldbuffers)
        Meta.createBuffer(b, buf.offset, buf.length)
        bodylen += padding(buf.length)
    end
    buffers = FlatBuffers.endvector!(b, BN)
    debug && @show bodylen

    # write record batch object
    Meta.recordBatchStart(b)
    Meta.recordBatchAddLength(b, Int64(nrows))
    Meta.recordBatchAddNodes(b, nodes)
    Meta.recordBatchAddBuffers(b, buffers)
    return Meta.recordBatchEnd(b), bodylen
end

function makedictionarybatchmsg(sch::Tables.Schema{names, types}, columns, id, isdelta, debug) where {names, types}
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, nothing, debug)
    Meta.dictionaryBatchStart(b)
    Meta.dictionaryBatchAddId(b, Int64(id))
    Meta.dictionaryBatchAddData(b, recordbatch)
    Meta.dictionaryBatchAddIsDelta(b, isdelta)
    dictionarybatch = Meta.dictionaryBatchEnd(b)
    return makemessage(b, Meta.DictionaryBatch, dictionarybatch, columns, nothing, bodylen)
end

function makenodesbuffers!(colidx, types, columns, dictencodings, fieldnodes, fieldbuffers, bufferoffset)
    colidx > fieldcount(types) && return
    T = fieldtype(types, colidx)
    col = Tables.getcolumn(columns, colidx)
    if dictencodings !== nothing && haskey(dictencodings, colidx)
        _, T, vals = dictencodings[colidx]
        col = DictEncoder(col, vals, T)
        len = _length(col)
        nc = nullcount(col)
        push!(fieldnodes, FieldNode(len, nc))
        # validity bitmap
        blen = nc == 0 ? 0 : bitpackedbytes(len)
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        # adjust buffer offset, make array buffer
        bufferoffset += blen
        blen = sizeof(T) * len
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        bufferoffset += padding(blen)
    else
        bufferoffset = makenodesbuffers!(maybemissing(T), col, fieldnodes, fieldbuffers, bufferoffset)
    end
    # make next column node/buffers
    return makenodesbuffers!(colidx + 1, types, columns, dictencodings, fieldnodes, fieldbuffers, bufferoffset)
end

function makenodesbuffers!(::Type{Missing}, col, fieldnodes, fieldbuffers, bufferoffset)
    len = _length(col)
    push!(fieldnodes, FieldNode(len, len))
    return bufferoffset
end

function writebuffer(io, ::Type{Missing}, col)
    return
end

function makenodesbuffers!(::Type{T}, col, fieldnodes, fieldbuffers, bufferoffset) where {T}
    len = _length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    # adjust buffer offset, make primitive array buffer
    bufferoffset += blen
    blen = len * sizeof(T)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    return bufferoffset + padding(blen)
end

makenodesbuffers!(::Type{Dates.Date}, col, fieldnodes, fieldbuffers, bufferoffset) =
    makenodesbuffers!(Date{Meta.DateUnit.DAY, Int32}, converter(Date{Meta.DateUnit.DAY, Int32}, col), fieldnodes, fieldbuffers, bufferoffset)
makenodesbuffers!(::Type{Dates.Time}, col, fieldnodes, fieldbuffers, bufferoffset) =
    makenodesbuffers!(Time{Meta.TimeUnit.NANOSECOND, Int64}, converter(Time{Meta.TimeUnit.NANOSECOND, Int64}, col), fieldnodes, fieldbuffers, bufferoffset)
makenodesbuffers!(::Type{Dates.DateTime}, col, fieldnodes, fieldbuffers, bufferoffset) =
    makenodesbuffers!(Date{Meta.DateUnit.MILLISECOND, Int64}, converter(Date{Meta.DateUnit.MILLISECOND, Int64}, col), fieldnodes, fieldbuffers, bufferoffset)
makenodesbuffers!(::Type{P}, col, fieldnodes, fieldbuffers, bufferoffset) where {P <: Dates.Period} =
    makenodesbuffers!(Duration{arrowperiodtype(P)}, converter(Duration{arrowperiodtype(P)}, col), fieldnodes, fieldbuffers, bufferoffset)

function writebitmap(io, col)
    nullcount(col) == 0 && return 0
    len = _length(col)
    i = 0
    n = 0
    st = iterate(col)
    for _ = 1:bitpackedbytes(len)
        b = 0x00
        for j = 1:8
            if (i + j) <= len
                x, state = st
                b = setbit(b, !ismissing(x), j)
                st = iterate(col, state)
            end
        end
        n += Base.write(io, b)
        i += 8
    end
    return n
end

function writebuffer(io, ::Type{T}, col) where {T}
    writebitmap(io, col)
    n = writearray(io, T, col)
    writezeros(io, paddinglength(n))
    return
end

writebuffer(io, ::Type{Dates.Date}, col) = writebuffer(io, Date{Meta.DateUnit.DAY, Int32}, converter(Date{Meta.DateUnit.DAY, Int32}, col))
writebuffer(io, ::Type{Dates.Time}, col) = writebuffer(io, Time{Meta.TimeUnit.NANOSECOND, Int64}, converter(Time{Meta.TimeUnit.NANOSECOND, Int64}, col))
writebuffer(io, ::Type{Dates.DateTime}, col) = writebuffer(io, Date{Meta.DateUnit.MILLISECOND, Int64}, converter(Date{Meta.DateUnit.MILLISECOND, Int64}, col))
writebuffer(io, ::Type{P}, col) where {P <: Dates.Period} = writebuffer(io, Duration{arrowperiodtype(P)}, converter(Duration{arrowperiodtype(P)}, col))

function makenodesbuffers!(::Type{T}, col, fieldnodes, fieldbuffers, bufferoffset) where {T <: Union{AbstractString, AbstractVector}}
    len = _length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    # adjust buffer offset, make array buffer
    bufferoffset += blen
    # TODO: support Large lists
    blen = sizeof(Int32) * (len + 1)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    bufferoffset += padding(blen)
    if T <: AbstractString || T <: AbstractVector{UInt8}
        blen = 0
        for x in col
            blen += sizeof(x)
        end
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        bufferoffset += padding(blen)
    else
        bufferoffset = makenodesbuffers!(maybemissing(eltype(T)), flatten(skipmissing(col)), fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, ::Type{T}, col) where {T <: Union{AbstractString, AbstractVector}}
    writebitmap(io, col)
    # write offsets
    off::Int32 = 0
    len = T <: AbstractString ? sizeof : length
    n = 0
    for x in col
        n += Base.write(io, off)
        if x !== missing
            off += len(x)
        end
    end
    n += Base.write(io, off)
    writezeros(io, paddinglength(n))
    # write values array
    if T <: AbstractString || T <: AbstractVector{UInt8}
        n = 0
        for x in col
            if x !== missing
                n += Base.write(io, x)
            end
        end
        writezeros(io, paddinglength(n))
    else
        writebuffer(io, maybemissing(eltype(T)), flatten(skipmissing(col)))
    end
    return
end

function makenodesbuffers!(::Type{NTuple{N, T}}, col, fieldnodes, fieldbuffers, bufferoffset) where {N, T}
    len = _length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    bufferoffset += blen
    if T === UInt8
        blen = N * len
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        bufferoffset += padding(blen)
    else
        bufferoffset = makenodesbuffers!(maybemissing(T), flatten(coalesce(x, default(NTuple{N, T})) for x in col), fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, ::Type{NTuple{N, T}}, col) where {N, T}
    writebitmap(io, col)
    # write values array
    if T === UInt8
        n = writearray(io, NTuple{N, T}, col)
        writezeros(io, paddinglength(n))
    else
        writebuffer(io, maybemissing(T), flatten(coalesce(x, default(NTuple{N, T})) for x in col))
    end
    return
end

function makenodesbuffers!(::Type{Pair{K, V}}, col, fieldnodes, fieldbuffers, bufferoffset) where {K, V}
    len = _length(col)
    # null_count must be 0
    push!(fieldnodes, FieldNode(len, 0))
    # validity bitmap, not relevant
    push!(fieldbuffers, Buffer(bufferoffset, 0))
    # Struct child node
    bufferoffset = makenodesbuffers!(NamedTuple{(:first, :second), Tuple{K, V}}, pairs(col), fieldnodes, fieldbuffers, bufferoffset)
    return bufferoffset
end

function writebuffer(io, ::Type{Pair{K, V}}, col) where {K, V}
    # write values array
    writebuffer(io, NamedTuple{(:first, :second), Tuple{K, V}}, pairs(col))
    return
end

function makenodesbuffers!(::Type{NamedTuple{names, types}}, col, fieldnodes, fieldbuffers, bufferoffset) where {names, types}
    len = _length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    bufferoffset += blen
    for i = 1:length(names)
        bufferoffset = makenodesbuffers!(maybemissing(fieldtype(types, i)), (getfield(x, names[i]) for x in col), fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, ::Type{NamedTuple{names, types}}, col) where {names, types}
    writebitmap(io, col)
    # write values arrays
    for i = 1:length(names)
        writebuffer(io, maybemissing(fieldtype(types, i)), (getfield(x, names[i]) for x in col))
    end
    return
end

function makenodesbuffers!(::Type{UnionT{T, typeIds, U}}, col, fieldnodes, fieldbuffers, bufferoffset) where {T, typeIds, U}
    len = _length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    # typeIds buffer
    push!(fieldbuffers, Buffer(bufferoffset, len))
    bufferoffset += padding(len)
    if T == Meta.UnionMode.Dense
        # offsets buffer
        blen = sizeof(Int32) * len
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        bufferoffset += padding(blen)
        # value arrays
        for i = 1:fieldcount(U)
            S = fieldtype(U, i)
            bufferoffset = makenodesbuffers!(maybemissing(S), filtered(i == 1 ? Union{S, Missing} : maybemissing(S), col), fieldnodes, fieldbuffers, bufferoffset)
        end
    else
        # value arrays
        for i = 1:fieldcount(U)
            S = fieldtype(U, i)
            bufferoffset = makenodesbuffers!(maybemissing(S), replaced(S, col), fieldnodes, fieldbuffers, bufferoffset)
        end
    end
    return bufferoffset
end

isatypeid(x::T, ::Type{types}) where {T, types} = isatypeid(x, fieldtype(types, 1), types, 1)
isatypeid(x::T, ::Type{S}, ::Type{types}, i) where {T, S, types} = x isa S ? i : isatypeid(x, fieldtype(types, i + 1), types, i + 1)

function writebuffer(io, ::Type{UnionT{T, typeIds, U}}, col) where {T, typeIds, U}
    # typeIds buffer
    typeids = typeIds === nothing ? (0:(fieldcount(U) - 1)) : typeIds
    n = 0
    for x in col
        typeid = x === missing ? Int8(0) : Int8(typeids[isatypeid(x, U)])
        n += Base.write(io, typeid)
    end
    writezeros(io, paddinglength(n))
    if T == Meta.UnionMode.Dense
        # offset buffer
        n = 0
        offs = zeros(Int32, fieldcount(U))
        for x in col
            idx = x === missing ? 1 : isatypeid(x, U)
            n += Base.write(io, offs[idx])
            offs[idx] += 1
        end
        writezeros(io, paddinglength(n))
        # write values arrays
        for i = 1:fieldcount(U)
            S = fieldtype(U, i)
            writebuffer(io, maybemissing(S), filtered(i == 1 ? Union{S, Missing} : maybemissing(S), col))
        end
    else
        # value arrays
        for i = 1:fieldcount(U)
            S = fieldtype(U, i)
            writebuffer(io, maybemissing(S), replaced(S, col))
        end
    end
    return
end
