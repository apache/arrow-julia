__precompile__(true)

module Arrow

using Missings, WeakRefStrings, CategoricalArrays

import Base: getindex, setindex!

const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]


import Base: convert, show, unsafe_string
import Base: length, endof, size, eltype, start, next, done, getindex, isassigned
import Base.isnull # this will be removed in 0.7


"""
    Buffer

A data structure containing a `Vector{UInt8}` that can act as a buffer containing Arrow format data.
Use of this is optional and pointers can be used to construct objects directly instead.
"""
mutable struct Buffer
    data::Vector{UInt8}
end
export Buffer

length(b::Buffer) = length(b.data)
datapointer(b::Buffer) = pointer(b.data)
export datapointer


abstract type ArrowVector{T} <: AbstractVector{T} end


include("utils.jl")
include("primitives.jl")
include("lists.jl")
include("arrowvectors.jl")
include("datetime.jl")
include("dictencoding.jl")


#=~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TODO
    -support nested lists (think should work but haven't tested)
    -bounds checking in constructors
    -bounds checking in accessing null bitmap
    -be sure null checking isn't too slow as it is

    -ensure that convert(Array{Union{T,Missing}}, A) is efficient in 0.7
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~=#


end  # module Arrow
