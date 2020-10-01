module Arrow

using Mmap
import Dates
using DataAPI, Tables, SentinelArrays, PooledArrays, CodecLz4, CodecZstd

using Base: @propagate_inbounds
import Base: ==

const DEBUG_LEVEL = Ref(0)

function setdebug!(level::Int)
    DEBUG_LEVEL[] = level
    return
end

function withdebug(f, level)
    lvl = DEBUG_LEVEL[]
    try
        setdebug!(level)
        f()
    finally
        setdebug!(lvl)
    end
end

macro debug(level, msg)
    esc(quote
        if DEBUG_LEVEL[] >= $level
            println(string("DEBUG: ", $(QuoteNode(__source__.file)), ":", $(QuoteNode(__source__.line)), " ", $msg))
        end
    end)
end

const ALIGNMENT = 8
const FILE_FORMAT_MAGIC_BYTES = b"ARROW1"
const CONTINUATION_INDICATOR_BYTES = 0xffffffff

# vendored flatbuffers code for now
include("FlatBuffers/FlatBuffers.jl")
using .FlatBuffers

include("metadata/Flatbuf.jl")
using .Flatbuf; const Meta = Flatbuf

include("arrowtypes.jl")
using .ArrowTypes
include("utils.jl")
include("arraytypes.jl")
include("eltypes.jl")
include("table.jl")
include("write.jl")

end  # module Arrow
