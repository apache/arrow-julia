__precompile__(true)

module Arrow

using Missings

import Base: getindex, setindex!

const ALIGNMENT = 8
const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]


# TODO maybe make one version with just a pointer and one with actual vector
mutable struct Buffer
    data::Vector{UInt8}
end
export Buffer


include("utils.jl")
include("primitives.jl")


end  # module Arrow
