
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


abstract type AbstractPrimitive{J} end
export AbstractPrimitive


struct Primitive{J} <: AbstractPrimitive{J}
    length::Int32
    data::Ptr{UInt8}
end
export Primitive

function Primitive{J}(b::Buffer, i::Integer, length::Integer) where J
    data_ptr = pointer(b.data, i)
    Primitive{J}(length, data_ptr)
end


struct NullablePrimitive{J} <: AbstractPrimitive{J}
    length::Int32
    null_count::Int32
    validity::Ptr{UInt8}
    data::Ptr{UInt8}
end
export NullablePrimitive

function NullablePrimitive{J}(b::Buffer, bitmask_loc::Integer, data_loc::Integer,
                                   length::Integer, null_count::Integer) where J
    val_ptr = pointer(b.data, bitmask_loc)
    data_ptr = pointer(b.data, data_loc)
    NullablePrimitive{J}(length, null_count, val_ptr, data_ptr)
end


#================================================================================================
    common interface
================================================================================================#
nullcount(A::Primitive) = 0
nullcount(A::NullablePrimitive) = Int(A.null_count)

checkbounds(A::AbstractPrimitive, i::Integer) = (1 ≤ i ≤ A.length) || throw(BoundsError(A, i))

unsafe_isnull(A::Primitive, i::Integer) = false
# TODO is this too slow?
function unsafe_isnull(A::NullablePrimitive, i::Integer)
    a, b = divrem(i, 8)
    !getbit(unsafe_load(A.validity + a), b)
end

isnull(A::AbstractPrimitive, i::Integer) = (checkbounds(A,i); unsafe_isnull(A,i))
export isnull


function unsafe_getvalue(A::AbstractPrimitive{J}, i::Integer)::J where J
    unsafe_load(convert(Ptr{J}, A.data), i)
end

#================================================================================================
    array interface
================================================================================================#
length(A::AbstractPrimitive) = A.length

size(A::Primitive) = (A.length,)
function size(A::Primitive, i::Integer)
    if i == 1
        return A.length
    else
        return 1
    end
    throw(ArgumentError("arraysize: dimension $i out of range"))
end

eltype(A::Primitive{J}) where J = J
eltype(A::NullablePrimitive{J}) where J = Union{J,Missing}


function getindex(A::Primitive{J}, i::Integer)::J where J
    @boundscheck checkbounds(A, i)
    unsafe_getvalue(A, i)
end

function getindex(A::NullablePrimitive{J}, i::Integer)::Union{J,Missing} where J
    @boundscheck checkbounds(A, i)
    unsafe_isnull(A, i) ? missing : unsafe_getvalue(A, i)
end


