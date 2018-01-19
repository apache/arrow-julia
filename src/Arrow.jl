__precompile__(true)

module Arrow

using Missings, WeakRefStrings

import Base: getindex, setindex!

const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]


import Base: convert, show, unsafe_string
import Base: length, endof, size, eltype, start, next, done, getindex, isassigned
import Base.isnull # this will be removed in 0.7


# TODO maybe make one version with just a pointer and one with actual vector
mutable struct Buffer
    data::Vector{UInt8}
end
export Buffer

abstract type ArrowVector{T} <: AbstractVector{T} end


include("utils.jl")
include("primitives.jl")
include("lists.jl")
include("arrowvectors.jl")


#=~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TODO
    -support nested lists
    -bounds checking in constructors
    -bounds checking in accessing null bitmap
    -be sure null checking isn't too slow as it is
    -implement show functions

    -ensure that convert(Array{Union{T,Missing}}, A) is efficient in 0.7
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~=#


end  # module Arrow
