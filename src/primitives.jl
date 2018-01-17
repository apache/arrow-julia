
#=~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TODO
    bounds checking in constructors
    bounds checking in accessing null bitmap
    getindex for ranges
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~=#


# array interface imports, TODO finish
import Base: length, size, eltype
import Base: getindex
import Base.isnull # this will be removed in 0.7


abstract type AbstractPrimitiveArray{J} end
export AbstractPrimitiveArray


struct PrimitiveArray{J} <: AbstractPrimitiveArray{J}
    length::Int32
    data::Ptr{UInt8}
end
export PrimitiveArray

function PrimitiveArray{J}(b::Buffer, i::Integer, length::Integer) where J
    data_ptr = pointer(b.data, i)
    PrimitiveArray{J}(length, data_ptr)
end


struct NullablePrimitiveArray{J} <: AbstractPrimitiveArray{J}
    length::Int32
    null_count::Int32
    validity::Ptr{UInt8}
    data::Ptr{UInt8}
end
export NullablePrimitiveArray

function NullablePrimitiveArray{J}(b::Buffer, bitmask_loc::Integer, data_loc::Integer,
                                   length::Integer, null_count::Integer) where J
    val_ptr = pointer(b.data, bitmask_loc)
    data_ptr = pointer(b.data, data_loc)
    NullablePrimitiveArray{J}(length, null_count, val_ptr, data_ptr)
end


#================================================================================================
    common interface
================================================================================================#
nullcount(A::PrimitiveArray) = 0
nullcount(A::NullablePrimitiveArray) = Int(A.null_count)

checkbounds(A::AbstractPrimitiveArray, i::Integer) = (1 ≤ i ≤ A.length) || throw(BoundsError(A, i))

unsafe_isnull(A::PrimitiveArray, i::Integer) = false
# TODO is this too slow?
function unsafe_isnull(A::NullablePrimitiveArray, i::Integer)
    a, b = divrem(i, 8)
    !getbit(unsafe_load(A.validity + a), b)
end

isnull(A::AbstractPrimitiveArray, i::Integer) = (checkbounds(A,i); unsafe_isnull(A,i))
export isnull


function unsafe_getvalue(A::AbstractPrimitiveArray{J}, i::Integer)::J where J
    unsafe_load(convert(Ptr{J}, A.data), i)
end

#================================================================================================
    array interface
================================================================================================#
length(A::AbstractPrimitiveArray) = A.length

size(A::PrimitiveArray) = (A.length,)
function size(A::PrimitiveArray, i::Integer)
    if i == 1
        return A.length
    else
        return 1
    end
    throw(ArgumentError("arraysize: dimension $i out of range"))
end

eltype(A::PrimitiveArray{J}) where J = J
eltype(A::NullablePrimitiveArray{J}) where J = Union{J,Missing}


function getindex(A::PrimitiveArray{J}, i::Integer)::J where J
    @boundscheck checkbounds(A, i)
    unsafe_getvalue(A, i)
end

function getindex(A::NullablePrimitiveArray{J}, i::Integer)::Union{J,Missing} where J
    @boundscheck checkbounds(A, i)
    unsafe_isnull(A, i) ? missing : unsafe_getvalue(A, i)
end


