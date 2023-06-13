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

const DEFAULT_MAX_DEPTH = 6

"""
    Arrow.write(io::IO, tbl)
    Arrow.write(file::String, tbl)
    tbl |> Arrow.write(io_or_file)

Write any [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible `tbl` out as arrow formatted data.
Providing an `io::IO` argument will cause the data to be written to it
in the ["streaming" format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format), unless `file=true` keyword argument is passed.
Providing a `file::String` argument will result in the ["file" format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) being written.

Multiple record batches will be written based on the number of
`Tables.partitions(tbl)` that are provided; by default, this is just
one for a given table, but some table sources support automatic
partitioning. Note you can turn multiple table objects into partitions
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that
each table must have the exact same `Tables.Schema`.

By default, `Arrow.write` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8` or the `JULIA_NUM_THREADS` environment variable is set).

Supported keyword arguments to `Arrow.write` include:
  * `colmetadata=nothing`: the metadata that should be written as the table's columns' `custom_metadata` fields; must either be `nothing` or an `AbstractDict` of `column_name::Symbol => column_metadata` where `column_metadata` is an iterable of `<:AbstractString` pairs.
  * `compress`: possible values include `:lz4`, `:zstd`, or your own initialized `LZ4FrameCompressor` or `ZstdCompressor` objects; will cause all buffers in each record batch to use the respective compression encoding
  * `alignment::Int=8`: specify the number of bytes to align buffers to when written in messages; strongly recommended to only use alignment values of 8 or 64 for modern memory cache line optimization
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written; to dict encode specific columns, wrap the column/array in `Arrow.DictEncode(col)`
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; other language implementations [may not support this](https://arrow.apache.org/docs/status.html)
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `maxdepth::Int=$DEFAULT_MAX_DEPTH`: deepest allowed nested serialization level; this is provided by default to prevent accidental infinite recursion with mutually recursive data structures
  * `metadata=Arrow.getmetadata(tbl)`: the metadata that should be written as the table's schema's `custom_metadata` field; must either be `nothing` or an iterable of `<:AbstractString` pairs.
  * `ntasks::Int`: number of buffered threaded tasks to allow while writing input partitions out as arrow record batches; default is no limit; for unbuffered writing, pass `ntasks=0`
  * `file::Bool=false`: if a an `io` argument is being written to, passing `file=true` will cause the arrow file format to be written instead of just IPC streaming
"""
function write end

write(io_or_file; kw...) = x -> write(io_or_file, x; kw...)

function write(file_path, tbl; kwargs...)
    open(Writer, file_path; file=true, kwargs...) do writer
        write(writer, tbl)
    end
    file_path
end

struct Message
    msgflatbuf
    columns
    bodylen
    isrecordbatch::Bool
    blockmsg::Bool
    headerType
end

struct Block
    offset::Int64
    metaDataLength::Int32
    bodyLength::Int64
end

"""
    Arrow.Writer{T<:IO}

An object that can be used to incrementally write Arrow partitions

# Examples
```julia
julia> writer = open(Arrow.Writer, tempname())

julia> partition1 = (col1 = [1, 2], col2 = ["A", "B"])
(col1 = [1, 2], col2 = ["A", "B"])

julia> Arrow.write(writer, partition1)

julia> partition2 = (col1 = [3, 4], col2 = ["C", "D"])
(col1 = [3, 4], col2 = ["C", "D"])

julia> Arrow.write(writer, partition2)

julia> close(writer)
```

It's also possible to automatically close the Writer using a do-block:

```julia
julia> open(Arrow.Writer, tempname()) do writer
           partition2 = (col1 = [1, 2], col2 = ["A", "B"])
           Arrow.write(writer, partition1)
           partition2 = (col1 = [3, 4], col2 = ["C", "D"])
           Arrow.write(writer, partition1)
       end
```
"""
mutable struct Writer{T<:IO}
    io::T
    closeio::Bool
    compress::Union{Nothing,Symbol,LZ4FrameCompressor,ZstdCompressor}
    writetofile::Bool
    largelists::Bool
    denseunions::Bool
    dictencode::Bool
    dictencodenested::Bool
    threaded::Bool
    alignment::Int32
    maxdepth::Int64
    meta::Union{Nothing,Base.ImmutableDict{String,String}}
    colmeta::Union{Nothing,Base.ImmutableDict{Symbol,Base.ImmutableDict{String,String}}}
    sync::OrderedSynchronizer
    msgs::Channel{Message}
    schema::Ref{Tables.Schema}
    firstcols::Ref{Any}
    dictencodings::Dict{Int64,Any}
    blocks::NTuple{2,Vector{Block}}
    task::Task
    anyerror::Threads.Atomic{Bool}
    errorref::Ref{Any}
    partition_count::Int32
    isclosed::Bool
end

function Base.open(::Type{Writer}, io::T, compress::Union{Nothing,Symbol,LZ4FrameCompressor,ZstdCompressor}, writetofile::Bool, largelists::Bool, denseunions::Bool, dictencode::Bool, dictencodenested::Bool, alignment::Integer, maxdepth::Integer, ntasks::Integer, meta::Union{Nothing,Any}, colmeta::Union{Nothing,Any}, closeio::Bool) where {T<:IO}
    if compress isa Symbol && compress !== :lz4 && compress !== :zstd
        throw(ArgumentError("unsupported compress keyword argument value: $compress. Valid values include `:lz4` or `:zstd`"))
    end
    sync = OrderedSynchronizer(2)
    msgs = Channel{Message}(ntasks)
    schema = Ref{Tables.Schema}()
    firstcols = Ref{Any}()
    dictencodings = Dict{Int64,Any}() # Lockable{DictEncoding}
    blocks = (Block[], Block[])
    # start message writing from channel
    threaded = Threads.nthreads() > 1
    task = threaded ? (@wkspawn for msg in msgs
        Base.write(io, msg, blocks, schema, alignment)
    end) : (@async for msg in msgs
        Base.write(io, msg, blocks, schema, alignment)
    end)
    anyerror = Threads.Atomic{Bool}(false)
    errorref = Ref{Any}()
    meta = _normalizemeta(meta)
    colmeta = _normalizecolmeta(colmeta)
    return Writer{T}(io, closeio, compress, writetofile, largelists, denseunions, dictencode, dictencodenested, threaded, alignment, maxdepth, meta, colmeta, sync, msgs, schema, firstcols, dictencodings, blocks, task, anyerror, errorref, 1, false)
end

function Base.open(::Type{Writer}, io::IO; compress::Union{Nothing,Symbol,LZ4FrameCompressor,ZstdCompressor}=nothing, file::Bool=true, largelists::Bool=false, denseunions::Bool=true, dictencode::Bool=false, dictencodenested::Bool=false, alignment::Integer=8, maxdepth::Integer=DEFAULT_MAX_DEPTH, ntasks::Integer=typemax(Int32), metadata::Union{Nothing,Any}=nothing, colmetadata::Union{Nothing,Any}=nothing, closeio::Bool=false)
    open(Writer, io, compress, file, largelists, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks, metadata, colmetadata, closeio)
end

Base.open(::Type{Writer}, file_path; kwargs...) = open(Writer, open(file_path, "w"); kwargs..., closeio=true)

function check_errors(writer::Writer)
    if writer.anyerror[]
        errorref = writer.errorref[]
        @error "error writing arrow data on partition = $(errorref[3])" exception = (errorref[1], errorref[2])
        error("fatal error writing arrow data")
    end
end

function write(writer::Writer, source)
    @sync for tbl in Tables.partitions(source)
        check_errors(writer)
        @debugv 1 "processing table partition $(writer.partition_count)"
        tblcols = Tables.columns(tbl)
        if !isassigned(writer.firstcols)
            if writer.writetofile
                @debugv 1 "starting write of arrow formatted file"
                Base.write(writer.io, FILE_FORMAT_MAGIC_BYTES, b"\0\0")
            end
            meta = isnothing(writer.meta) ? getmetadata(source) : writer.meta
            cols = toarrowtable(tblcols, writer.dictencodings, writer.largelists, writer.compress, writer.denseunions, writer.dictencode, writer.dictencodenested, writer.maxdepth, meta, writer.colmeta)
            writer.schema[] = Tables.schema(cols)
            writer.firstcols[] = cols
            put!(writer.msgs, makeschemamsg(writer.schema[], cols))
            if !isempty(writer.dictencodings)
                des = sort!(collect(writer.dictencodings); by=x -> x.first, rev=true)
                for (id, delock) in des
                    # assign dict encoding ids
                    de = delock.value
                    dictsch = Tables.Schema((:col,), (eltype(de.data),))
                    dictbatchmsg = makedictionarybatchmsg(dictsch, (col=de.data,), id, false, writer.alignment)
                    put!(writer.msgs, dictbatchmsg)
                end
            end
            recbatchmsg = makerecordbatchmsg(writer.schema[], cols, writer.alignment)
            put!(writer.msgs, recbatchmsg)
        else
            if writer.threaded
                @wkspawn process_partition(tblcols, writer.dictencodings, writer.largelists, writer.compress, writer.denseunions, writer.dictencode, writer.dictencodenested, writer.maxdepth, writer.sync, writer.msgs, writer.alignment, $(writer.partition_count), writer.schema, writer.errorref, writer.anyerror, writer.meta, writer.colmeta)
            else
                @async process_partition(tblcols, writer.dictencodings, writer.largelists, writer.compress, writer.denseunions, writer.dictencode, writer.dictencodenested, writer.maxdepth, writer.sync, writer.msgs, writer.alignment, $(writer.partition_count), writer.schema, writer.errorref, writer.anyerror, writer.meta, writer.colmeta)
            end
        end
        writer.partition_count += 1
    end
    check_errors(writer)
    return
end

function Base.close(writer::Writer)
    writer.isclosed && return
    # close our message-writing channel, no further put!-ing is allowed
    close(writer.msgs)
    # now wait for our message-writing task to finish writing
    !istaskfailed(writer.task) && wait(writer.task)
    if (!isassigned(writer.schema) || !isassigned(writer.firstcols))
        writer.closeio && close(writer.io)
        writer.isclosed = true
        return
    end
    # write empty message
    if !writer.writetofile
        msg = Message(UInt8[], nothing, 0, true, false, Meta.Schema)
        Base.write(writer.io, msg, writer.blocks, writer.schema, writer.alignment)
        writer.closeio && close(writer.io)
        writer.isclosed = true
        return
    end
    b = FlatBuffers.Builder(1024)
    schfoot = makeschema(b, writer.schema[], writer.firstcols[])
    recordbatches = if !isempty(writer.blocks[1])
        N = length(writer.blocks[1])
        Meta.footerStartRecordBatchesVector(b, N)
        for blk in Iterators.reverse(writer.blocks[1])
            Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
        end
        FlatBuffers.endvector!(b, N)
    else
        FlatBuffers.UOffsetT(0)
    end
    dicts = if !isempty(writer.blocks[2])
        N = length(writer.blocks[2])
        Meta.footerStartDictionariesVector(b, N)
        for blk in Iterators.reverse(writer.blocks[2])
            Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
        end
        FlatBuffers.endvector!(b, N)
    else
        FlatBuffers.UOffsetT(0)
    end
    Meta.footerStart(b)
    Meta.footerAddVersion(b, Meta.MetadataVersion.V5)
    Meta.footerAddSchema(b, schfoot)
    Meta.footerAddDictionaries(b, dicts)
    Meta.footerAddRecordBatches(b, recordbatches)
    foot = Meta.footerEnd(b)
    FlatBuffers.finish!(b, foot)
    footer = FlatBuffers.finishedbytes(b)
    Base.write(writer.io, footer)
    Base.write(writer.io, Int32(length(footer)))
    Base.write(writer.io, "ARROW1")
    writer.closeio && close(writer.io)
    writer.isclosed = true
    nothing
end

function write(io::IO, tbl; kwargs...)
    open(Writer, io; file=false, kwargs...) do writer
        write(writer, tbl)
    end
    io
end

function write(io, source, writetofile, largelists, compress, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks, meta, colmeta)
    open(Writer, io, compress, writetofile, largelists, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks, meta, colmeta) do writer
        write(writer, source)
    end
    io
end

function process_partition(cols, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, sync, msgs, alignment, i, sch, errorref, anyerror, meta, colmeta)
    try
        cols = toarrowtable(cols, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, meta, colmeta)
        dictmsgs = nothing
        if !isempty(cols.dictencodingdeltas)
            dictmsgs = []
            for de in cols.dictencodingdeltas
                dictsch = Tables.Schema((:col,), (eltype(de.data),))
                push!(dictmsgs, makedictionarybatchmsg(dictsch, (col=de.data,), de.id, true, alignment))
            end
        end
        put!(sync, i) do
            if !isnothing(dictmsgs)
                foreach(msg -> put!(msgs, msg), dictmsgs)
            end
            put!(msgs, makerecordbatchmsg(sch[], cols, alignment))
        end
    catch e
        errorref[] = (e, catch_backtrace(), i)
        anyerror[] = true
    end
    return
end

struct ToArrowTable
    sch::Tables.Schema
    cols::Vector{Any}
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
    dictencodingdeltas::Vector{DictEncoding}
end

function toarrowtable(cols, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, meta, colmeta)
    @debugv 1 "converting input table to arrow formatted columns"
    sch = Tables.schema(cols)
    types = collect(sch.types)
    N = length(types)
    newcols = Vector{Any}(undef, N)
    newtypes = Vector{Type}(undef, N)
    dictencodingdeltas = DictEncoding[]
    Tables.eachcolumn(sch, cols) do col, i, nm
        oldcolmeta = getmetadata(col)
        newcolmeta = isnothing(colmeta) ? oldcolmeta : get(colmeta, nm, oldcolmeta)
        newcol = toarrowvector(col, i, dictencodings, dictencodingdeltas, newcolmeta; compression=compress, largelists=largelists, denseunions=denseunions, dictencode=dictencode, dictencodenested=dictencodenested, maxdepth=maxdepth)
        newtypes[i] = eltype(newcol)
        newcols[i] = newcol
    end
    minlen, maxlen = isempty(newcols) ? (0, 0) : extrema(length, newcols)
    minlen == maxlen || throw(ArgumentError("columns with unequal lengths detected: $minlen < $maxlen"))
    meta = _normalizemeta(meta)
    return ToArrowTable(Tables.Schema(sch.names, newtypes), newcols, meta, dictencodingdeltas)
end

Tables.columns(x::ToArrowTable) = x
Tables.rowcount(x::ToArrowTable) = length(x.cols) == 0 ? 0 : length(x.cols[1])
Tables.schema(x::ToArrowTable) = x.sch
Tables.columnnames(x::ToArrowTable) = x.sch.names
Tables.getcolumn(x::ToArrowTable, i::Int) = x.cols[i]

function Base.write(io::IO, msg::Message, blocks, sch, alignment)
    metalen = padding(length(msg.msgflatbuf), alignment)
    @debugv 1 "writing message: metalen = $metalen, bodylen = $(msg.bodylen), isrecordbatch = $(msg.isrecordbatch), headerType = $(msg.headerType)"
    if msg.blockmsg
        push!(blocks[msg.isrecordbatch ? 1 : 2], Block(position(io), metalen + 8, msg.bodylen))
    end
    # now write the final message spec out
    # continuation byte
    n = Base.write(io, CONTINUATION_INDICATOR_BYTES)
    # metadata length
    n += Base.write(io, Int32(metalen))
    # message flatbuffer
    n += Base.write(io, msg.msgflatbuf)
    n += writezeros(io, paddinglength(length(msg.msgflatbuf), alignment))
    # message body
    if msg.columns !== nothing
        # write out buffers
        for col in Tables.Columns(msg.columns)
            writebuffer(io, col, alignment)
        end
    end
    return n
end

function makemessage(b, headerType, header, columns=nothing, bodylen=0)
    # write the message flatbuffer object
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, headerType)
    Meta.messageAddHeader(b, header)
    Meta.messageAddBodyLength(b, Int64(bodylen))
    # Meta.messageAddCustomMetadata(b, meta)
    # Meta.messageStartCustomMetadataVector(b, num_meta_elems)
    msg = Meta.messageEnd(b)
    FlatBuffers.finish!(b, msg)
    return Message(FlatBuffers.finishedbytes(b), columns, bodylen, headerType == Meta.RecordBatch, headerType == Meta.RecordBatch || headerType == Meta.DictionaryBatch, headerType)
end

function makeschema(b, sch::Tables.Schema, columns)
    # build Field objects
    names = sch.names
    N = length(names)
    fieldoffsets = [fieldoffset(b, names[i], columns.cols[i]) for i = 1:N]
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

function makeschemamsg(sch::Tables.Schema, columns)
    @debugv 1 "building schema message: sch = $sch"
    b = FlatBuffers.Builder(1024)
    schema = makeschema(b, sch, columns)
    return makemessage(b, Meta.Schema, schema)
end

function fieldoffset(b, name, col)
    nameoff = FlatBuffers.createstring!(b, string(name))
    T = eltype(col)
    nullable = T >: Missing
    # check for custom metadata
    if getmetadata(col) !== nothing
        kvs = getmetadata(col)
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
    if isdictencoded(col)
        encodingtype = indtype(col)
        IT, inttype, _ = arrowtype(b, encodingtype)
        Meta.dictionaryEncodingStart(b)
        Meta.dictionaryEncodingAddId(b, Int64(getid(col)))
        Meta.dictionaryEncodingAddIndexType(b, inttype)
        # TODO: support isOrdered?
        Meta.dictionaryEncodingAddIsOrdered(b, false)
        dict = Meta.dictionaryEncodingEnd(b)
    else
        dict = FlatBuffers.UOffsetT(0)
    end
    type, typeoff, children = arrowtype(b, col)
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
    if isdictencoded(col)
        @debugv 1 "building field: name = $name, nullable = $nullable, T = $T, type = $type, inttype = $IT, dictionary id = $(getid(col))"
    else
        @debugv 1 "building field: name = $name, nullable = $nullable, T = $T, type = $type"
    end
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

function makerecordbatchmsg(sch::Tables.Schema{names,types}, columns, alignment) where {names,types}
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, alignment)
    return makemessage(b, Meta.RecordBatch, recordbatch, columns, bodylen)
end

function makerecordbatch(b, sch::Tables.Schema{names,types}, columns, alignment) where {names,types}
    nrows = Tables.rowcount(columns)

    compress = nothing
    fieldnodes = FieldNode[]
    fieldbuffers = Buffer[]
    bufferoffset = 0
    for col in Tables.Columns(columns)
        if col isa Compressed
            compress = compressiontype(col)
        end
        bufferoffset = makenodesbuffers!(col, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    @debugv 1 "building record batch message: nrows = $nrows, sch = $sch, compress = $compress"

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
        bodylen += padding(buf.length, alignment)
    end
    buffers = FlatBuffers.endvector!(b, BN)

    # compression
    if compress !== nothing
        Meta.bodyCompressionStart(b)
        Meta.bodyCompressionAddCodec(b, compress)
        Meta.bodyCompressionAddMethod(b, Meta.BodyCompressionMethod.BUFFER)
        compression = Meta.bodyCompressionEnd(b)
    else
        compression = FlatBuffers.UOffsetT(0)
    end

    # write record batch object
    @debugv 1 "built record batch message: nrows = $nrows, nodes = $fieldnodes, buffers = $fieldbuffers, compress = $compress, bodylen = $bodylen"
    Meta.recordBatchStart(b)
    Meta.recordBatchAddLength(b, Int64(nrows))
    Meta.recordBatchAddNodes(b, nodes)
    Meta.recordBatchAddBuffers(b, buffers)
    Meta.recordBatchAddCompression(b, compression)
    return Meta.recordBatchEnd(b), bodylen
end

function makedictionarybatchmsg(sch, columns, id, isdelta, alignment)
    @debugv 1 "building dictionary message: id = $id, sch = $sch, isdelta = $isdelta"
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, alignment)
    Meta.dictionaryBatchStart(b)
    Meta.dictionaryBatchAddId(b, Int64(id))
    Meta.dictionaryBatchAddData(b, recordbatch)
    Meta.dictionaryBatchAddIsDelta(b, isdelta)
    dictionarybatch = Meta.dictionaryBatchEnd(b)
    return makemessage(b, Meta.DictionaryBatch, dictionarybatch, columns, bodylen)
end
