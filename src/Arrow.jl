__precompile__(true)

module Arrow

using Missings, WeakRefStrings, CategoricalArrays

import Base: getindex, setindex!

const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]


import Base: convert, show, unsafe_string, checkbounds
import Base: length, endof, size, eltype, start, next, done, getindex, isassigned, view
import Base.isnull # this will be removed in 0.7


# TODO: in getting rid of pointers we've lost ability to refer to multiple distinct data buffers
#       how to deal with this???
struct Buffer end  # TODO delete


abstract type ArrowVector{T} <: AbstractVector{T} end


include("utils.jl")
include("primitives.jl")
include("lists.jl")
include("arrowvectors.jl")
include("datetime.jl")
include("dictencoding.jl")
include("vectorbuffer.jl")


#=~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TODO
    -support nested lists (think should work but haven't tested)
    -bounds checking in constructors
    -bounds checking in accessing null bitmap
    -be sure null checking isn't too slow as it is

    -ensure that convert(Array{Union{T,Missing}}, A) is efficient in 0.7
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~=#


end  # module Arrow
