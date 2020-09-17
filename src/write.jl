const OBJ_METADATA = IdDict{Any, Dict{String, String}}()

function setmetadata!(x, meta::Dict{String, String})
    OBJ_METADATA[x] = meta
    return
end

getmetadata(x, default=nothing) = get(OBJ_METADATA, x, default)

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
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that
each table must have the exact same `Tables.Schema`.
"""
function write end

function write(file::String, tbl; largelists::Bool=false, compress::Bool=false, debug::Bool=false)
    open(file, "w") do io
        write(io, tbl, true, largelists, compress, debug)
    end
    return file
end

function write(io::IO, tbl; largelists::Bool=false, compress::Bool=false, debug::Bool=false, file::Bool=false)
    return write(io, tbl, file, largelists, compress, debug)
end

if isdefined(Tables, :partitions)
    parts = Tables.partitions
else
    parts(x) = (x,)
    parts(x::Tuple) = x
end

@static if VERSION >= v"1.3"
    const Cond = Threads.Condition
else
    const Cond = Condition
end

struct OrderedChannel{T}
    chan::Channel{T}
    cond::Cond
    i::Ref{Int}
end

OrderedChannel{T}(sz) where {T} = OrderedChannel{T}(Channel{T}(sz), Threads.Condition(), Ref(1))
Base.iterate(ch::OrderedChannel, st...) = iterate(ch.chan, st...)

macro lock(obj, expr)
    esc(quote
    @static if VERSION >= v"1.3"
        lock($obj)
    end
        try
            $expr
        finally
            @static if VERSION >= v"1.3"
                unlock($obj)
            end
        end
    end)
end

function Base.put!(ch::OrderedChannel{T}, x::T, i::Integer, incr::Bool=false) where {T}
    @lock ch.cond begin
        while ch.i[] < i
            wait(ch.cond)
        end
        put!(ch.chan, x)
        if incr
            ch.i[] += 1
        end
        notify(ch.cond)
    end
    return
end

function Base.close(ch::OrderedChannel)
    @lock ch.cond begin
        while Base.n_waiters(ch.cond) > 0
            wait(ch.cond)
        end
        close(ch.chan)
    end
    return
end

function write(io, source, writetofile, largelists, compress, debug)
    if writetofile
        Base.write(io, "ARROW1\0\0")
    end
    msgs = OrderedChannel{Message}(Inf)
    # build messages
    sch = Ref{Tables.Schema}()
    firstcols = Ref{Any}()
    blocks = (Block[], Block[])
    dictid = Ref(0)
    dictencodings = Dict{Int, Tuple{Int, Type, Any}}()
    # start message writing from channel
@static if VERSION >= v"1.3-DEV"
    tsk = Threads.@spawn for msg in msgs
        Base.write(io, msg, blocks, sch, compress)
    end
else
    tsk = @async for msg in msgs
        Base.write(io, msg, blocks, sch, compress)
    end
end
    @sync for (i, tbl) in enumerate(parts(source))
        if i == 1
            cols = Tables.columns(toarrowtable(tbl, largelists))
            sch[] = Tables.schema(cols)
            firstcols[] = cols
            for (i, col) in enumerate(Tables.Columns(cols))
                if col isa DictEncode
                    id = dictid[]
                    dictid[] += 1
                    refpool = DataAPI.refpool(col.data)
                    if refpool !== nothing
                        values = refpool
                        IT = eltype(DataAPI.refarray(col.data))
                    else
                        values = unique(col)
                        IT = encodingtype(length(values))
                    end
                    dictencodings[i] = (id, IT, values)
                end
            end
            debug && @show sch[]
            put!(msgs, makeschemamsg(sch[], cols, dictencodings), i)
            if !isempty(dictencodings)
                for (colidx, (id, T, values)) in dictencodings
                    dictsch = Tables.Schema((:col,), (eltype(values),))
                    put!(msgs, makedictionarybatchmsg(dictsch, (col=values,), id, false, compress, debug), i)
                end
            end
            put!(msgs, makerecordbatchmsg(sch[], cols, dictencodings, compress, debug), i, true)
        else
@static if VERSION >= v"1.3-DEV"
            Threads.@spawn begin
                try
                    cols = Tables.columns(toarrowtable(tbl, largelists))
                    if !isempty(dictencodings)
                        for (colidx, (id, T, values)) in dictencodings
                            dictsch = Tables.Schema((:col,), (eltype(values),))
                            col = Tables.getcolumn(cols, colidx)
                            refpool = DataAPI.refpool(col.data)
                            if refpool !== nothing
                                newvals = refpool
                            else
                                newvals = col
                            end
                            # get new values we haven't seen before for delta update
                            vals = setdiff(newvals, values)
                            put!(msgs, makedictionarybatchmsg(dictsch, (col=vals,), id, true, compress, debug), i)
                            # add new values to existing set for future diffs
                            union!(values, vals)
                        end
                    end
                    put!(msgs, makerecordbatchmsg(sch[], cols, dictencodings, compress, debug), i, true)
                catch e
                    showerror(stdout, e, catch_backtrace())
                    rethrow(e)
                end
            end
else
            @async begin
                try
                    cols = Tables.columns(toarrowtable(tbl, largelists))
                    if !isempty(dictencodings)
                        for (colidx, (id, T, values)) in dictencodings
                            dictsch = Tables.Schema((:col,), (eltype(values),))
                            col = Tables.getcolumn(cols, colidx)
                            refpool = DataAPI.refpool(col.data)
                            if refpool !== nothing
                                newvals = refpool
                            else
                                newvals = col
                            end
                            # get new values we haven't seen before for delta update
                            vals = setdiff(newvals, values)
                            put!(msgs, makedictionarybatchmsg(dictsch, (col=vals,), id, true, compress, debug), i)
                            # add new values to existing set for future diffs
                            union!(values, vals)
                        end
                    end
                    put!(msgs, makerecordbatchmsg(sch[], cols, dictencodings, compress, debug), i, true)
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
    if !writetofile
        Base.write(io, Message(UInt8[], nothing, nothing, 0, true, false), blocks, sch, compress)
    end
    if writetofile
        b = FlatBuffers.Builder(1024)
        schfoot = makeschema(b, sch[], firstcols[], dictencodings)
        if !isempty(blocks[1])
            N = length(blocks[1])
            Meta.footerStartRecordBatchesVector(b, N)
            for blk in Iterators.reverse(blocks[1])
                Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
            end
            recordbatches = FlatBuffers.endvector!(b, N)
        else
            recordbatches = FlatBuffers.UOffsetT(0)
        end
        if !isempty(blocks[2])
            N = length(blocks[2])
            Meta.footerStartDictionariesVector(b, N)
            for blk in Iterators.reverse(blocks[2])
                Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
            end
            dicts = FlatBuffers.endvector!(b, N)
        else
            dicts = FlatBuffers.UOffsetT(0)
        end
        Meta.footerStart(b)
        Meta.footerAddVersion(b, Meta.MetadataVersion.V4)
        Meta.footerAddSchema(b, schfoot)
        Meta.footerAddDictionaries(b, dicts)
        Meta.footerAddRecordBatches(b, recordbatches)
        foot = Meta.footerEnd(b)
        FlatBuffers.finish!(b, foot)
        footer = FlatBuffers.finishedbytes(b)
        Base.write(io, footer)
        Base.write(io, Int32(length(footer)))
        Base.write(io, "ARROW1")
    end
    return io
end

struct ToArrowTable
    sch::Tables.Schema
    cols::Vector{Any}
    metadata::Union{Nothing, Dict{String, String}}
    fieldmetadata::Dict{Int, Dict{String, String}}
end

function toarrowtable(x, largelists)
    cols = Tables.columns(x)
    meta = getmetadata(cols)
    sch = Tables.schema(cols)
    types = collect(sch.types)
    N = length(types)
    newcols = Vector{Any}(undef, N)
    newtypes = Vector{Type}(undef, N)
    fieldmetadata = Dict{Int, Dict{String, String}}()
    Tables.eachcolumn(sch, cols) do col, i, nm
        colmeta = getmetadata(col)
        if colmeta !== nothing
            fieldmetadata[i] = colmeta
        end
        dictencode = false
        if col isa AbstractArray && DataAPI.refarray(col) !== col
            dictencode = true
            types[i] = eltype(DataAPI.refpool(col))
        end
        T, newcol = toarrow(types[i], i, col, fieldmetadata, largelists)
        newtypes[i] = T
        newcols[i] = dictencode ? DictEncode(newcol) : newcol
    end
    return ToArrowTable(Tables.Schema(sch.names, newtypes), newcols, meta, fieldmetadata)
end

toarrow(::Type{T}, i, col, fm, ll) where {T} = T, col
toarrow(::Type{Dates.Date}, i, col, fm, ll) = Date{Meta.DateUnit.DAY, Int32}, converter(Date{Meta.DateUnit.DAY, Int32}, col)
toarrow(::Type{Dates.Time}, i, col, fm, ll) = Time{Meta.TimeUnit.NANOSECOND, Int64}, converter(Time{Meta.TimeUnit.NANOSECOND, Int64}, col)
toarrow(::Type{Dates.DateTime}, i, col, fm, ll) = Date{Meta.DateUnit.MILLISECOND, Int64}, converter(Date{Meta.DateUnit.MILLISECOND, Int64}, col)
toarrow(::Type{P}, i, col, fm, ll) where {P <: Dates.Period} = Duration{arrowperiodtype(P)}, converter(Duration{arrowperiodtype(P)}, col)

function toarrow(::Type{T}, i, col, fm, ll) where {T <: Union{AbstractString, AbstractVector}}
    len = T <: AbstractString ? sizeof : length
    datalen = 0
    for x in col
        datalen += len(x)
    end
    if datalen > 2147483647 || ll
        return LargeList{T}, col
    end
    return T, col
end

function toarrow(::Type{Symbol}, i, col, fm, ll)
    meta = get!(() -> Dict{String, String}(), fm, i)
    meta["ARROW:extension:name"] = "JuliaLang.Symbol"
    meta["ARROW:extension:metadata"] = ""
    return String, converter(String, col)
end

function toarrow(::Type{Char}, i, col, fm, ll)
    meta = get!(() -> Dict{String, String}(), fm, i)
    meta["ARROW:extension:name"] = "JuliaLang.Char"
    meta["ARROW:extension:metadata"] = ""
    return String, converter(String, col)
end

Tables.columns(x::ToArrowTable) = x
Tables.rowcount(x::ToArrowTable) = length(x.cols) == 0 ? 0 : length(x.cols[1])
Tables.schema(x::ToArrowTable) = x.sch
Tables.columnnames(x::ToArrowTable) = x.sch.names
Tables.getcolumn(x::ToArrowTable, i::Int) = x.cols[i]

struct Message
    msgflatbuf
    columns
    dictencodings
    bodylen
    isrecordbatch::Bool
    blockmsg::Bool
end

struct Block
    offset::Int64
    metaDataLength::Int32
    bodyLength::Int64
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

function Base.write(io::IO, msg::Message, blocks, sch, compress)
    metalen = padding(length(msg.msgflatbuf))
    if msg.blockmsg
        push!(blocks[msg.isrecordbatch ? 1 : 2], Block(position(io), metalen + 8, msg.bodylen))
    end
    # now write the final message spec out
    # continuation byte
    n = Base.write(io, 0xFFFFFFFF)
    # metadata length
    n += Base.write(io, Int32(metalen))
    # message flatbuffer
    n += Base.write(io, msg.msgflatbuf)
    n += writezeros(io, paddinglength(n))
    # message body
    if msg.columns !== nothing
        types = sch[].types
        # write out buffers
        for i = 1:length(Tables.columnnames(msg.columns))
            col = Tables.getcolumn(msg.columns, i)
            T = types[i]
            # @show typeof(col), col
            if msg.dictencodings !== nothing && haskey(msg.dictencodings, i)
                refvals = DataAPI.refarray(col.data)
                if refvals !== col.data
                    T = eltype(refvals)
                    col = (x - one(T) for x in refvals)
                else
                    _, T, vals = msg.dictencodings[i]
                    col = DictEncoder(col, vals, T)
                end
            end
            writebuffer(io, T === Missing ? Missing : Base.nonmissingtype(T), col, compress)
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
    return Message(FlatBuffers.finishedbytes(b), columns, dictencodings, bodylen, headerType == Meta.RecordBatch, headerType == Meta.RecordBatch || headerType == Meta.DictionaryBatch)
end

function makeschema(b, sch::Tables.Schema{names, types}, columns, dictencodings) where {names, types}
    # build Field objects
    N = length(names)
    fieldoffsets = [fieldoffset(b, i, names[i], fieldtype(types, i), dictencodings, columns.fieldmetadata) for i = 1:N]
    Meta.schemaStartFieldsVector(b, N)
    for off in Iterators.reverse(fieldoffsets)
        FlatBuffers.prependoffset!(b, off)
    end
    fields = FlatBuffers.endvector!(b, N)
    if columns.metadata !== nothing
        kvs = columns.metadata
        kvoffs = Vector{FlatBuffers.UOffsetT}(undef, length(kvs))
        for (i, (k, v)) in enumerate(kvs)
            koff = FlatBuffers.createstring!(b, String(k))
            voff = FlatBuffers.createstring!(b, String(v))
            Meta.keyValueStart(b)
            Meta.keyValueAddKey(b, koff)
            Meta.keyValueAddValue(b, voff)
            kvoffs[i] = Meta.keyValueEnd(b)
        end
        Meta.schemaStartCustomMetadataVector(b, length(kvs))
        for off in Iterators.reverse(kvoffs)
            FlatBuffers.prependoffset!(b, off)
        end
        meta = FlatBuffers.endvector!(b, length(kvs))
    else
        meta = FlatBuffers.UOffsetT(0)
    end
    # write schema object
    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fields)
    Meta.schemaAddCustomMetadata(b, meta)
    return Meta.schemaEnd(b)
end

function makeschemamsg(sch::Tables.Schema{names, types}, columns, dictencodings) where {names, types}
    b = FlatBuffers.Builder(1024)
    schema = makeschema(b, sch, columns, dictencodings)
    return makemessage(b, Meta.Schema, schema)
end

function fieldoffset(b, colidx, name, T, dictencodings, metadata)
    nameoff = FlatBuffers.createstring!(b, String(name))
    nullable = T >: Missing
    # check for custom metadata
    if metadata !== nothing && haskey(metadata, colidx)
        kvs = metadata[colidx]
        kvoffs = Vector{FlatBuffers.UOffsetT}(undef, length(kvs))
        for (i, (k, v)) in enumerate(kvs)
            koff = FlatBuffers.createstring!(b, String(k))
            voff = FlatBuffers.createstring!(b, String(v))
            Meta.keyValueStart(b)
            Meta.keyValueAddKey(b, koff)
            Meta.keyValueAddValue(b, voff)
            kvoffs[i] = Meta.keyValueEnd(b)
        end
        Meta.fieldStartCustomMetadataVector(b, length(kvs))
        for off in Iterators.reverse(kvoffs)
            FlatBuffers.prependoffset!(b, off)
        end
        meta = FlatBuffers.endvector!(b, length(kvs))
    else
        meta = FlatBuffers.UOffsetT(0)
    end
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
        Meta.fieldStartChildrenVector(b, 0)
        children = FlatBuffers.endvector!(b, 0)
    end
    # build field object
    Meta.fieldStart(b)
    Meta.fieldAddName(b, nameoff)
    Meta.fieldAddNullable(b, nullable)
    Meta.fieldAddTypeType(b, type)
    Meta.fieldAddType(b, typeoff)
    Meta.fieldAddDictionary(b, dict)
    Meta.fieldAddChildren(b, children)
    Meta.fieldAddCustomMetadata(b, meta)
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

function makerecordbatchmsg(sch::Tables.Schema{names, types}, columns, dictencodings, compress, debug) where {names, types}
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, dictencodings, compress, debug)
    return makemessage(b, Meta.RecordBatch, recordbatch, columns, dictencodings, bodylen)
end

function makerecordbatch(b, sch::Tables.Schema{names, types}, columns, dictencodings, compress, debug) where {names, types}
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

function makedictionarybatchmsg(sch::Tables.Schema{names, types}, columns, id, isdelta, compress, debug) where {names, types}
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, nothing, compress, debug)
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
        refvals = DataAPI.refarray(col.data)
        if refvals !== col.data
            T = eltype(refvals)
            col = (x - one(T) for x in refvals)
        else
            _, T, vals = dictencodings[colidx]
            col = DictEncoder(col, vals, T)
        end
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

function writebuffer(io, ::Type{Missing}, col, compress)
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

function writebuffer(io, ::Type{T}, col, compress) where {T}
    writebitmap(io, col)
    n = writearray(io, T, col)
    writezeros(io, paddinglength(n))
    return
end

function makenodesbuffers!(::Type{T}, col, fieldnodes, fieldbuffers, bufferoffset) where {T <: Union{AbstractString, AbstractVector}}
    len = _length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    # adjust buffer offset, make array buffer
    bufferoffset += blen
    blen = sizeof(offsettype(T)) * (len + 1)
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

function writebuffer(io, ::Type{T}, col, compress) where {T <: Union{AbstractString, AbstractVector}}
    writebitmap(io, col)
    # write offsets
    OT = offsettype(T)
    off::OT = 0
    len = T <: AbstractString ? sizeof : length
    n = 0
    for x in col
        n += Base.write(io, off)
        if x !== missing
            off += OT(len(x))
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
        writebuffer(io, maybemissing(eltype(T)), flatten(skipmissing(col)), compress)
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

function writebuffer(io, ::Type{NTuple{N, T}}, col, compress) where {N, T}
    writebitmap(io, col)
    # write values array
    if T === UInt8
        n = writearray(io, NTuple{N, T}, col)
        writezeros(io, paddinglength(n))
    else
        writebuffer(io, maybemissing(T), flatten(coalesce(x, default(NTuple{N, T})) for x in col), compress)
    end
    return
end

function makenodesbuffers!(::Type{Pair{K, V}}, col, fieldnodes, fieldbuffers, bufferoffset) where {K, V}
    # Struct child node
    bufferoffset = makenodesbuffers!(Vector{KeyValue{K, V}}, ( KeyValue(k, v) for (k, v) in pairs(col) ), fieldnodes, fieldbuffers, bufferoffset)
    return bufferoffset
end

function writebuffer(io, ::Type{Pair{K, V}}, col, compress) where {K, V}
    # write values array
    writebuffer(io, Vector{KeyValue{K, V}}, ( KeyValue(k, v) for (k, v) in pairs(col) ), compress)
    return
end

function makenodesbuffers!(::Type{KeyValue{K, V}}, col, fieldnodes, fieldbuffers, bufferoffset) where {K, V}
    len = _length(col)
    push!(fieldnodes, FieldNode(len, 0))
    # validity bitmap
    push!(fieldbuffers, Buffer(bufferoffset, 0))
    # keys
    bufferoffset = makenodesbuffers!(maybemissing(K), (x.key for x in col), fieldnodes, fieldbuffers, bufferoffset)
    # values
    bufferoffset = makenodesbuffers!(maybemissing(V), (@miss_or(x, x.value) for x in col), fieldnodes, fieldbuffers, bufferoffset)
    return bufferoffset
end

function writebuffer(io, ::Type{KeyValue{K, V}}, col, compress) where {K, V}
    writebitmap(io, col)
    # write keys
    writebuffer(io, maybemissing(K), (x.key for x in col), compress)
    # write values
    writebuffer(io, maybemissing(V), (@miss_or(x, x.value) for x in col), compress)
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
        bufferoffset = makenodesbuffers!(maybemissing(fieldtype(types, i)), (@miss_or(x, getfield(x, names[i])) for x in col), fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, ::Type{NamedTuple{names, types}}, col, compress) where {names, types}
    writebitmap(io, col)
    # write values arrays
    for i = 1:length(names)
        writebuffer(io, maybemissing(fieldtype(types, i)), (@miss_or(x, getfield(x, names[i])) for x in col), compress)
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

function writebuffer(io, ::Type{UnionT{T, typeIds, U}}, col, compress) where {T, typeIds, U}
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
            writebuffer(io, maybemissing(S), filtered(i == 1 ? Union{S, Missing} : maybemissing(S), col), compress)
        end
    else
        # value arrays
        for i = 1:fieldcount(U)
            S = fieldtype(U, i)
            writebuffer(io, maybemissing(S), replaced(S, col), compress)
        end
    end
    return
end
