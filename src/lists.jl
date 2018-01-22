

abstract type AbstractList{J} <: ArrowVector{J} end
export AbstractList


struct List{P<:AbstractPrimitive,J} <: AbstractList{J}
    length::Int32
    offsets::Ptr{UInt8}
    values::P
end
export List

function List{P,J}(ptr::Ptr, offset_loc::Integer, len::Integer, vals::P) where {P,J}
    List{P,J}(len, ptr+offset_loc-1, vals)
end
function List{P,J}(b::Buffer, offset_loc::Integer, len::Integer, vals::P) where {P,J}
    offset_ptr = pointer(b.data, offset_loc)
    List{P,J}(len, offset_ptr, vals)
end


struct NullableList{P<:AbstractPrimitive,J} <: AbstractList{Union{Missing,J}}
    length::Int32
    null_count::Int32
    validity::Ptr{UInt8}
    offsets::Ptr{UInt8}
    values::P
end
export NullableList

function NullableList{P,J}(ptr::Ptr, bitmask_loc::Integer, offset_loc::Integer, len::Integer,
                           null_count::Integer, vals::P) where {P,J}
    NullableList{P,J}(len, null_count, ptr+bitmask_loc-1, ptr+offset_loc-1, vals)
end
function NullableList{P,J}(b::Buffer, bitmask_loc::Integer, offset_loc::Integer, len::Integer,
                           null_count::Integer, vals::P) where {P,J}
    bitmask_ptr = pointer(b.data, bitmask_loc)
    offset_ptr = pointer(b.data, offset_loc)
    NullableList{P,J}(len, null_count, bitmask_ptr, offset_ptr, vals)
end


#====================================================================================================
    common interface
====================================================================================================#
# note that there are always n+1 offsets
unsafe_offset(l::AbstractList, i::Integer) = unsafe_load(convert(Ptr{Int32}, l.offsets), i)

unsafe_ellength(l::AbstractList, i::Integer) = unsafe_offset(l, i+1) - unsafe_offset(l, i)

# returns offset, length
function unsafe_elparams(l::AbstractList, i::Integer)
    off = unsafe_offset(l, i)
    off, unsafe_offset(l, i+1) - off
end


function unsafe_getvalue(l::Union{List{P,K},NullableList{P,K}}, i::Integer) where {P,K}
    off, len = unsafe_elparams(l, i)
    unsafe_construct(K, l.values, off+1, len)
end
function unsafe_getvalue(l::Union{List{P,K},NullableList{P,K}},
                         idx::AbstractVector{<:Integer}) where {P,K}
    String[unsafe_getvalue(l, i) for i ∈ idx]
end
function unsafe_getvalue(l::List{P,K}, idx::AbstractVector{Bool}) where {P,K}
    String[unsafe_getvalue(l, i) for i ∈ 1:length(l) if idx[i]]
end


