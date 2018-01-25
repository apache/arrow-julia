

abstract type AbstractPrimitive{J} <: ArrowVector{J} end
export AbstractPrimitive


"""
    Primitive{J} <: AbstractPrimitive{J}

    Primitive{J}(ptr::Ptr, i::Integer, len::Integer)
    Primitive{J}(b::Buffer, i::Integer, len::Integer)

An arrow primitive array containing no null values.  This is essentially just a wrapped pointer
to the data.  The index `i` should give the start of the array relative to `ptr` using 1-based indexing.
"""
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


"""
    NullablePrimitive{J} <: AbstractPrimitive{Union{J,Missing}}

    NullablePrimitive{J}(ptr::Ptr, bitmask_loc::Integer, data_loc::Integer, len::Integer)
    NullablePrimitive{J}(b::Buffer, bitmask_loc::Integer, data_loc::Integer, len::Integer)

An arrow primitive array possibly containing null values.  This is essentially a pair of wrapped
pointers: one to the data and one to the bitmask specifying whether each value is null.
The bitmask and data locations should be given relative to `ptr` using 1-based indexing.
"""
struct NullablePrimitive{J} <: AbstractPrimitive{Union{J,Missing}}
    length::Int32
    validity::Ptr{UInt8}
    data::Ptr{UInt8}
end
export NullablePrimitive

function NullablePrimitive{J}(ptr::Ptr, bitmask_loc::Integer, data_loc::Integer,
                              len::Integer) where J
    NullablePrimitive{J}(len, ptr+bitmask_loc-1, ptr+data_loc-1)
end
function NullablePrimitive{J}(b::Buffer, bitmask_loc::Integer, data_loc::Integer,
                              len::Integer) where J
    val_ptr = pointer(b.data, bitmask_loc)
    data_ptr = pointer(b.data, data_loc)
    NullablePrimitive{J}(len, val_ptr, data_ptr)
end


#================================================================================================
    common interface
================================================================================================#
"""
    unsafe_getvalue(A::ArrowVector, i)

Retrieve the value from memory location `i` using Julia 1-based indexing. `i` can be a single integer
index, an `AbstractVector` of integer indices, or an `AbstractVector{Bool}` mask.

This typically involves a call to `unsafe_load` or `unsafe_wrap`.
"""
function unsafe_getvalue(A::Union{Primitive{J},NullablePrimitive{J}}, i::Integer)::J where J
    unsafe_load(convert(Ptr{J}, A.data), i)
end
function unsafe_getvalue(A::Union{Primitive{J},NullablePrimitive{J}},
                         idx::AbstractVector{<:Integer}) where J
    ptr = convert(Ptr{J}, A.data) + (idx[1]-1)*sizeof(J)
    unsafe_wrap(Array, ptr, length(idx))
end
function unsafe_getvalue(A::Primitive{J}, idx::AbstractVector{Bool}) where J
    J[unsafe_getvalue(A, i) for i ∈ 1:length(A) if idx[i]]
end


"""
    unsafe_setvalue!(A::ArrowVector{J}, x, i)

Set the value at location `i` to `x`.  If `i` is a single integer, `x` should be an element of type
`J`.  Otherwise `i` can be an `AbstractVector{<:Integer}` or `AbstractVector{Bool}` in which case
`x` should be an appropriately sized `AbstractVector{J}`.
"""
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, x::J, i::Integer) where J
    unsafe_store!(convert(Ptr{J}, A.data), x, i)
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::AbstractVector{J},
                          idx::AbstractVector{<:Integer}) where J
    ptr = convert(Ptr{J}, A.data)
    for (x, i) ∈ zip(v, idx)
        unsafe_store!(ptr, x, i)
    end
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::AbstractVector{J},
                          idx::AbstractVector{Bool}) where J
    ptr = convert(Ptr{J}, A.data)
    j = 1
    for i ∈ 1:length(A)
        if idx[i]
            unsafe_store!(ptr, v[j], i)
            j += 1
        end
    end
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::Vector{J}, ::Colon) where J
    unsafe_copy!(convert(Ptr{J}, A.data), pointer(v), length(v))
end


"""
    unsafe_construct(::Type{T}, A::Primitive, i::Integer, len::Integer)

Construct an object of type `T` using `len` elements from `A` starting at index `i` (1-based indexing).
This is mostly used by `AbstractList` objects to construct variable length objects such as strings
from primitive arrays.

Users must define new methods for new types `T`.
"""
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


# TODO these are only here temporarily for testing, will move to arrowvectors.jl
function setindex!(A::Primitive{J}, x, i::Integer) where J
    @boundscheck checkbounds(A, i)
    unsafe_setvalue!(A, convert(J, x), i)
end
function setindex!(A::Primitive{J}, x::AbstractVector, idx::AbstractVector{<:Integer}) where J
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    unsafe_setvalue!(A, convert(AbstractVector{J}, x), idx)
end

# TODO this is fucked up, fix!!
function setindex!(A::NullablePrimitive{J}, x, i::Integer) where J
    @boundscheck checkbounds(A, i)
    o = unsafe_setvalue!(A, convert(J, x), i)
    unsafe_setnull!(A, false, i)  # important that this is last in case above fails
    o
end
function setindex!(A::NullablePrimitive{J}, x::Missing, i::Integer) where J
    @boundscheck checkbounds(A, i)
    unsafe_setnull!(A, true, i)
    missing
end
# TODO this is horribly inefficient but really hard to do right for non-consecutive
function setindex!(A::NullablePrimitive, x::AbstractVector, idx::AbstractVector{<:Integer})
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    for (ξ,i) ∈ zip(x, idx)
        @inbounds setindex!(A, ξ, i)
    end
end
function setindex!(A::NullablePrimitive, x::AbstractVector, idx::AbstractVector{Bool})
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    j = 1
    for i ∈ idx
        if idx[i]
            @inbounds setindex!(A, x[j], i)
            j += 1
        end
    end
    x
end

# TODO this probably isn't really much more efficient, should test
function setindex!(A::NullablePrimitive{J}, x::AbstractVector, ::Colon) where J
    @boundscheck checkinputsize(x, A)
    unsafe_setnulls!(A, ismissing.(x))
    for i ∈ 1:length(A)
        !ismissing(x[i]) && unsafe_setvalue!(A, convert(J, x[i]), i)
    end
    x
end
