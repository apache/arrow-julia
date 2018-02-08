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
    -clean up bounds checking so it's not redundant! right now it's all fucked up
    -support nested lists (think should work but haven't tested)
    -be sure null checking isn't too slow as it is
    -allow users to specify ordering of sub-buffers

    -ensure that convert(Array{Union{T,Missing}}, A) is efficient in 0.7
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~=#


end  # module Arrow
