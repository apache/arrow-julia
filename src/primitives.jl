

abstract type AbstractPrimitive{J} <: ArrowVector{J} end
export AbstractPrimitive


struct Primitive{J} <: AbstractPrimitive{J}
    length::Int32
    data::Ptr{UInt8}
end
export Primitive

function Primitive{J}(ptr::Ptr, i::Integer, len::Integer) where J
    Primitive{J}(len, ptr + (i-1))
end
function Primitive{J}(b::Buffer, i::Integer, len::Integer) where J
    data_ptr = pointer(b.data, i)
    Primitive{J}(len, data_ptr)
end


struct NullablePrimitive{J} <: AbstractPrimitive{Union{J,Missing}}
    length::Int32
    null_count::Int32
    validity::Ptr{UInt8}
    data::Ptr{UInt8}
end
export NullablePrimitive

function NullablePrimitive{J}(ptr::Ptr, bitmask_loc::Integer, data_loc::Integer,
                              len::Integer, null_count::Integer) where J
    NullablePrimitive{J}(len, null_count, ptr+bitmask_loc-1, ptr+data_loc-1)
end
function NullablePrimitive{J}(b::Buffer, bitmask_loc::Integer, data_loc::Integer,
                              len::Integer, null_count::Integer) where J
    val_ptr = pointer(b.data, bitmask_loc)
    data_ptr = pointer(b.data, data_loc)
    NullablePrimitive{J}(len, null_count, val_ptr, data_ptr)
end


#================================================================================================
    common interface
================================================================================================#

function unsafe_getvalue(A::Union{Primitive{J},NullablePrimitive{J}}, i::Integer)::J where J
    unsafe_load(convert(Ptr{J}, A.data), i)
end
function unsafe_getvalue(A::Union{Primitive{J},NullablePrimitive{J}},
                         idx::AbstractVector{<:Integer}) where J
    ptr = convert(Ptr{J}, A.data) + (idx[1]-1)*sizeof(J)
    unsafe_wrap(Array, ptr, length(idx))
end
function unsafe_getvalue(A::Primitive{J}, idx::AbstractVector{Bool}) where J
    J[unsafe_getvalue(A, i) for i ∈ 1:length(A) if idx[1]]
end


function unsafe_construct(::Type{String}, A::Primitive{UInt8}, i::Integer, len::Integer)
    unsafe_string(convert(Ptr{UInt8}, A.data + (i-1)), len)
end
function unsafe_construct(::Type{WeakRefString{J}}, A::Primitive{J}, i::Integer, len::Integer) where J
    WeakRefString{J}(convert(Ptr{J}, A.data + (i-1)), len)
end

function unsafe_construct(::Type{T}, A::NullablePrimitive{J}, i::Integer, len::Integer) where {T,J}
    nullexcept_inrange(A, i, i+len-1)
    unsafe_construct(T, A, i, len)
end



