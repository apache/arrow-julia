

abstract type AbstractList{J} <: ArrowVector{J} end
export AbstractList


struct List{P<:AbstractPrimitive,J} <: AbstractList{J}
    length::Int32
    offsets::Ptr{UInt8}
    values::P
end
export List

function List{P,J}(b::Buffer, offset_loc::Integer, len::Integer, vals::P) where {P,J}
    ptr = pointer(b.data, i)
    List{P,J}(len, offset_loc, vals)
end


struct NullableList{P<:AbstractPrimitive,J} <: AbstractList{Union{Missing,J}}
    length::Int32
    null_count::Int32
    validity::Ptr{UInt8}
    offsets::Ptr{UInt8}
    values::P
end
export NullableList

function NullableList{P,J}(b::Buffer, bitmask_loc::Integer, offset_loc::Integer, len::Integer,
                           null_count::Integer, vals::P) where {P,J}
    bitmask_ptr = pointer(b, bitmask_loc)
    offset_ptr = pointer(b, offset_loc)
    NullableList{P,J}(len, null_count, bitmask_ptr, offset_ptr, val)
end


#====================================================================================================
    common interface
====================================================================================================#


#====================================================================================================
    array interface
====================================================================================================#

