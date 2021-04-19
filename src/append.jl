"""
    Arrow.append(io::IO, tbl)
    Arrow.append(file::String, tbl)
    tbl |> Arrow.append(file)

Append any [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible `tbl`
to an existing arrow formatted file or IO. The existing arrow data must be in
IPC stream format. Note that appending to the "feather formatted file" is _not_
allowed, as this file format doesn't support appending. That means files written
like `Arrow.write(filename::String, tbl)` _cannot_ be appended to; instead, you
should write like `Arrow.write(filename::String, tbl; file=false)`.

When an IO object is provided to be written on to, it must support seeking. For
example, a file opened in `r+` mode or an `IOBuffer` that is readable, writable
and seekable can be appended to, but not a network stream.

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
  * `convert::Bool`: whether certain arrow primitive types in the schema of `file` should be converted to Julia defaults for matching them to the schema of `tbl`; by default, `convert=true`.
  * `file::Bool`: applicable when an `IO` is provided, whether it is a file; by default `file=false`.
"""
function append end

append(io_or_file; kw...) = x -> append(io_or_file, x; kw...)

function append(file::String, tbl; kwargs...)
    open(file, "r+") do io
        append(io, tbl; file=true, kwargs...)
    end

    return file
end

function append(io::IO, tbl;
        largelists::Bool=false,
        denseunions::Bool=true,
        dictencode::Bool=false,
        dictencodenested::Bool=false,
        alignment::Int=8,
        maxdepth::Int=DEFAULT_MAX_DEPTH,
        ntasks=Inf,
        convert::Bool=true,
        file::Bool=false)

    if ntasks < 1
        throw(ArgumentError("ntasks keyword argument must be > 0; pass `ntasks=1` to disable multithreaded writing"))
    end

    isstream, arrow_schema, compress = stream_properties(io; convert=convert)
    if !isstream
        throw(ArgumentError("append is supported only to files in arrow stream format"))
    end

    if compress === :lz4
        compress = LZ4_FRAME_COMPRESSOR
    elseif compress === :zstd
        compress = ZSTD_COMPRESSOR
    elseif compress isa Symbol
        throw(ArgumentError("unsupported compress keyword argument value: $compress. Valid values include `:lz4` or `:zstd`"))
    end

    append(io, tbl, arrow_schema, compress, largelists, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks)

    return io
end

function append(io::IO, source, arrow_schema, compress, largelists, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks)
    seekend(io)
    skip(io, -8) # overwrite last 8 bytes of last empty message footer

    sch = Ref{Tables.Schema}(arrow_schema)
    msgs = OrderedChannel{Message}(ntasks)
    dictencodings = Dict{Int64, Any}() # Lockable{DictEncoding}
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
        tbl_cols = Tables.columns(tbl)
        tbl_schema = Tables.schema(tbl_cols)

        if !is_equivalent_schema(arrow_schema, tbl_schema)
            throw(ArgumentError("Table schema does not match existing arrow file schema"))
        end

        if threaded
            Threads.@spawn process_partition(tbl_cols, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, msgs, alignment, i, sch, errorref, anyerror)
        else
            @async process_partition(tbl_cols, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, msgs, alignment, i, sch, errorref, anyerror)
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

function stream_properties(io::IO; convert::Bool=true)
    startpos = position(io)
    buff = similar(FILE_FORMAT_MAGIC_BYTES)
    start_magic = read!(io, buff) == FILE_FORMAT_MAGIC_BYTES
    seekend(io)
    len = position(io) - startpos
    skip(io, -length(FILE_FORMAT_MAGIC_BYTES))
    end_magic = read!(io, buff) == FILE_FORMAT_MAGIC_BYTES
    seek(io, startpos) # leave the stream position unchanged

    isstream = !(len > 24 && start_magic && end_magic)
    if isstream
        stream = Stream(io, convert=convert)
        for table in stream
            # no need to scan further once we get compression information
            (stream.compression[] !== nothing) && break
        end
        seek(io, startpos) # leave the stream position unchanged
        return isstream, Tables.Schema(stream.names, stream.types), stream.compression[]
    else
        return isstream, nothing, nothing
    end
end

function is_equivalent_schema(sch1::Tables.Schema, sch2::Tables.Schema)
    (sch1.names == sch2.names) || (return false)
    for (t1,t2) in zip(sch1.types, sch2.types)
        (t1 === t2) || (return false)
    end
    true
end