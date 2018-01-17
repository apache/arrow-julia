__precompile__(true)

module Arrow

using Missings

import Base: getindex, setindex!

const ALIGNMENT = 8
const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]

bytes_for_bits(n::Integer) = div(((n + 7) & ~7), 8)
paddedlength(n::Integer) = div((n + ALIGNMENT - 1), ALIGNMENT)*ALIGNMENT
getbit(byte::UInt8, i::Integer) = (byte & BITMASK[i] > 0x00) ? true : false
# temporary
export bytes_for_bits, paddedlength, getbit



# TODO maybe make one version with just a pointer and one with actual vector
mutable struct Buffer
    data::Vector{UInt8}
end
export Buffer


include("primitives.jl")


end  # module Arrow
