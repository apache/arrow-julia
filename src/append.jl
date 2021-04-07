"""
    Arrow.append(file::String, tbl)
    tbl |> Arrow.append(io_or_file)

Append any [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible
`tbl` to an existing arrow formatted file.

Multiple record batches will be written based on the number of
`Tables.partitions(tbl)` that are provided; by default, this is just
one for a given table, but some table sources support automatic
partitioning. Note you can turn multiple table objects into partitions
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that
each table must have the exact same `Tables.Schema`.

By default, `Arrow.append` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8`
or the `JULIA_NUM_THREADS` environment variable is set).

Supported keyword arguments to `Arrow.append` include:
  * `alignment::Int=8`: specify the number of bytes to align buffers to when written in messages; strongly recommended to only use alignment values of 8 or 64 for modern memory cache line optimization
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written; to dict encode specific columns, wrap the column/array in `Arrow.DictEncode(col)`
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; other language implementations [may not support this](https://arrow.apache.org/docs/status.html)
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `maxdepth::Int=$DEFAULT_MAX_DEPTH`: deepest allowed nested serialization level; this is provided by default to prevent accidental infinite recursion with mutually recursive data structures
  * `ntasks::Int`: number of concurrent threaded tasks to allow while writing input partitions out as arrow record batches; default is no limit; to disable multithreaded writing, pass `ntasks=1`
"""
function append end

append(file::String; kw...) = x -> append(file::String, x; kw...)

function append(file::String, tbl;
        largelists::Bool=false,
        denseunions::Bool=true,
        dictencode::Bool=false,
        dictencodenested::Bool=false,
        alignment::Int=8,
        maxdepth::Int=DEFAULT_MAX_DEPTH,
        ntasks=Inf)
    if ntasks < 1
        throw(ArgumentError("ntasks keyword argument must be > 0; pass `ntasks=1` to disable multithreaded writing"))
    end

    if !is_stream_format(file)
        throw(ArgumentError("append is supported only to files in arrow stream format"))
    end

    open(file, "r+") do io
        bytes = Mmap.mmap(io)
        arrow_schema, dictencodings, compress = table_info(bytes)
        if compress === :lz4
            compress = LZ4_FRAME_COMPRESSOR
        elseif compress === :zstd
            compress = ZSTD_COMPRESSOR
        elseif compress isa Symbol
            throw(ArgumentError("unsupported compress keyword argument value: $compress. Valid values include `:lz4` or `:zstd`"))
        end
        seek(io, length(bytes) - 8) # overwrite last 8 bytes of last empty message footer
        append(io, tbl, arrow_schema, dictencodings, compress, largelists, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks)
    end

    return file
end

function append(io::IO, source, arrow_schema, dictencodings, compress, largelists, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks)
    sch = Ref{Tables.Schema}(arrow_schema)
    msgs = OrderedChannel{Message}(ntasks)
    # build messages
    blocks = (Block[], Block[])
    # start message writing from channel
    threaded = ntasks > 1
    tsk = threaded ? (Threads.@spawn for msg in msgs
        Base.write(io, msg, blocks, sch, alignment)
    end) : (@async for msg in msgs
        Base.write(io, msg, blocks, sch, alignment)
    end)
    anyerror = Threads.Atomic{Bool}(false)
    errorref = Ref{Any}()
    @sync for (i, tbl) in enumerate(Tables.partitions(source))
        if anyerror[]
            @error "error writing arrow data on partition = $(errorref[][3])" exception=(errorref[][1], errorref[][2])
            error("fatal error writing arrow data")
        end
        @debug 1 "processing table partition i = $i"
        tbl_schema = Tables.schema(tbl)

        if !is_equivalent_schema(arrow_schema, tbl_schema)
            throw(ArgumentError("Table schema does not match existing arrow file schema"))
        end

        if threaded
            Threads.@spawn process_partition(tbl, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, msgs, alignment, i, sch, errorref, anyerror)
        else
            @async process_partition(tbl, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, msgs, alignment, i, sch, errorref, anyerror)
        end
    end
    if anyerror[]
        @error "error writing arrow data on partition = $(errorref[][3])" exception=(errorref[][1], errorref[][2])
        error("fatal error writing arrow data")
    end
    # close our message-writing channel, no further put!-ing is allowed
    close(msgs)
    # now wait for our message-writing task to finish writing
    wait(tsk)

    Base.write(io, Message(UInt8[], nothing, 0, true, false, Meta.Schema), blocks, sch, alignment)

    return io
end

function table_info(file::String; kwargs...)
    open(file) do io
        table_info(io; file=true, kwargs...)
    end
end
table_info(io::IO; file::Bool=false, kwargs...) = table_info(file ? Mmap.mmap(io) : Base.read(io); kwargs...)
function table_info(bytes::Vector{UInt8}; convert::Bool=true)
    dictencodings = Dict{Int64, DictEncoding}() # dictionary id => DictEncoding
    names = []
    types = []
    compression = nothing
    stream_format = is_stream_format(bytes)
    for batch in BatchIterator(bytes, stream_format ? 1 : 9)
        # store custom_metadata of batch.msg?
        header = batch.msg.header
        if header isa Meta.Schema
            @debug 1 "parsing schema message"
            # assert endianness?
            # store custom_metadata?
            for (i, field) in enumerate(header.fields)
                push!(names, Symbol(field.name))
                d = field.dictionary
                if d === nothing
                    push!(types, juliaeltype(field, false))
                else
                    push!(types, d.indexType === nothing ? Int32 : juliaeltype(field, d.indexType, false))
                end
            end
        elseif header isa Meta.DictionaryBatch
            id = header.id
            recordbatch = header.data
            @debug 1 "parsing dictionary batch message: id = $id, compression = $(recordbatch.compression)"
            if recordbatch.compression !== nothing
                compression = recordbatch.compression
            end
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
            @debug 1 "parsing record batch message: compression = $(header.compression)"
            if header.compression !== nothing
                compression = header.compression
            end
        else
            throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
        end
    end

    compression_codec = nothing
    if compression !== nothing
        if compression.codec == Flatbuf.CompressionType.ZSTD
            compression_codec = :zstd
        elseif compression.codec == Flatbuf.CompressionType.LZ4_FRAME
            compression_codec = :lz4
        else
            throw(ArgumentError("unsupported compression codec: $(compression.codec)"))
        end
    end
    Tables.Schema(names, types), dictencodings, compression_codec
end

function is_stream_format(file::String)
    open(file) do io
        is_stream_format(io, true)
    end
end
is_stream_format(io::IO, file::Bool=false) = is_stream_format(file ? Mmap.mmap(io) : Base.read(io))
function is_stream_format(bytes::Vector{UInt8})
    len = length(bytes)
    off = 1
    if len > 24 &&
        _startswith(bytes, off, FILE_FORMAT_MAGIC_BYTES) &&
        _endswith(bytes, off + len - 1, FILE_FORMAT_MAGIC_BYTES)
        return false
    else
        return true
    end
end

function is_equivalent_schema(sch1::Tables.Schema, sch2::Tables.Schema)
    (sch1.names == sch2.names) || (return false)
    for (t1,t2) in zip(sch1.types, sch2.types)
        (t1 === t2) || (return false)
    end
    true
end