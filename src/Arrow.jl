__precompile__(true)

module Arrow

using CategoricalArrays, Compat, Compat.Dates


const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
const ALIGNMENT = 8


import Base: getindex, setindex!
import Base: convert, show, unsafe_string, checkbounds, write, values, copy
import Base: length, endof, size, eltype, start, next, done, getindex, isassigned, view
import Base: IndexStyle
import Base: >, ≥, <, ≤, ==
import CategoricalArrays: levels


if VERSION ≥ v"0.7.0-"
else
    using Missings
    import Base: isnull
end


abstract type ArrowVector{T} <: AbstractVector{T} end
export ArrowVector


include("utils.jl")
include("primitives.jl")
include("lists.jl")
include("arrowvectors.jl")
include("datetime.jl")
include("dictencoding.jl")
include("bitprimitives.jl")


#=~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TODO
    -make primitives with args re-interpreted views (to what extent possible)
    -support Bools!!!
    -better support for converting arrow formats
    -get views working properly (only available in 0.7!)
    -clean up bounds checking so it's not redundant! right now it's all fucked up
    -support nested lists (think should work but haven't tested)
    -allow users to specify ordering of sub-buffers

    -are there any ways to get views of strings???
    -ensure that convert(Array{Union{T,Missing}}, A) is efficient in 0.7
    -investigate breaking due to new reinterpret in 0.7
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~=#


end  # module Arrow
