module Arrow

using Mmap
using Tables, SentinelArrays

using Base: @propagate_inbounds
import Base: ==

const ALIGNMENT = 8
const FILE_FORMAT_MAGIC_BYTES = b"ARROW1"
const CONTINUATION_INDICATOR_BYTES = 0xffffffff

# vendored flatbuffers code for now
include("FlatBuffers/FlatBuffers.jl")
using .FlatBuffers

include("metadata/Flatbuf.jl")
using .Flatbuf; const Meta = Flatbuf

include("utils.jl")
include("eltypes.jl")
include("arraytypes.jl")
include("table.jl")
include("write.jl")

end  # module Arrow
