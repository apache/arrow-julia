
abstract type AbstractList{J} <: ArrowVector{J} end
export AbstractList


# TODO add new constructors docs
"""
    List{P<:AbstractPrimitive,J} <: AbstractList{J}

    List{P,J}(ptr::Ptr, offset_loc::Integer, len::Integer, vals::P)
    List{P,J}(b::Buffer, offset_loc::Integer, len::Integer, vals::P)

An Arrow list of variable length objects such as strings, none of which are
null.  `vals` is the primitive array in which the underlying data is stored.
The `List` itself contains a pointer to the offsets, a buffer containing 32-bit
integers describing the offsets of each value.  The location should be given
relative to `ptr` using 1-based indexing.
"""
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
    p = List{P,J}(len, offset_ptr, vals)
    @boundscheck check_buffer_overrun(b, offset_loc, offsetsbytes(p), :offsets)
    p
end

function List(ptr::Union{Ptr,Buffer}, offset_loc::Integer, data_loc::Integer, ::Type{U},
              x::AbstractVector{J}) where {U,J}
    conv_data = [convert(Vector{U}, ξ) for ξ ∈ x]
    offs = offsets(conv_data)
    p = Primitive(ptr, data_loc, vcat(conv_data...))  # TODO this use of vcat is inefficient
    l = List{typeof(p),J}(ptr, offset_loc, length(x), p)
    unsafe_setoffsets!(l, offs)
    l
end


"""
    NullableList{P<:AbstractPrimitive,J} <: AbstractList{Union{Missing,J}}

    NullableList{P,J}(ptr::Ptr, bitmask_loc::Integer, offset_loc::Integer, len::Integer,
                      vals::P)
    NullableList{P,J}(b::Buffer, bitmask_loc::Integer, offset_loc::Integer, len::Integer,
                      vals::P)

An arrow list of variable length objects such as strings, some of which may be null.  `vals`
is the primitive array in which the underlying data is stored.  The `NullableList` itself contains
pointers to the offsets and null bit mask which the locations of which should be specified relative
to `ptr` using 1-based indexing.
"""
struct NullableList{P<:AbstractPrimitive,J} <: AbstractList{Union{Missing,J}}
    length::Int32
    validity::Ptr{UInt8}
    offsets::Ptr{UInt8}
    values::P
end
export NullableList

function NullableList{P,J}(ptr::Ptr, bitmask_loc::Integer, offset_loc::Integer, len::Integer,
                           vals::P) where {P,J}
    NullableList{P,J}(len, ptr+bitmask_loc-1, ptr+offset_loc-1, vals)
end
function NullableList{P,J}(b::Buffer, bitmask_loc::Integer, offset_loc::Integer, len::Integer,
                           vals::P) where {P,J}
    bitmask_ptr = pointer(b.data, bitmask_loc)
    offset_ptr = pointer(b.data, offset_loc)
    NullableList{P,J}(len, bitmask_ptr, offset_ptr, vals)
end

# TODO these are really inefficient but difficult to do right, rethink what needs to be here
function NullableList(ptr::Union{Ptr,Buffer}, bitmask_loc::Integer, offset_loc::Integer,
                      data_loc::Integer, ::Type{U}, x::AbstractVector{T}
                     ) where {U,J,T<:Union{Union{J,Missing},J}}
    bmask = bitpack(.!ismissing.(x))
    conv_data = Union{Vector{U},Missing}[ismissing(ξ) ? missing : convert(Vector{U}, ξ) for ξ ∈ x]
    offs = offsets(conv_data)
    conv_data = filter(ξ -> !ismissing(ξ), conv_data)
    p = Primitive(ptr, data_loc, vcat(conv_data...))
    l = NullableList{typeof(p),J}(ptr, bitmask_loc, offset_loc, length(x), p)
    unsafe_setnulls!(l, bmask)
    unsafe_setoffsets!(l, offs)
    l
end


#====================================================================================================
    common interface
====================================================================================================#
function valuesbytes(::Type{C}, A::AbstractVector{T}
                    ) where {C,K<:AbstractString,T<:Union{Union{K,Missing},K}}
    sum(ismissing(a) ? 0 : length(a)*sizeof(C) for a ∈ A)
end
valuesbytes(A::Union{List{P,J},NullableList{P,J}}) where {P,J} = valuesbytes(A.values)

minbitmaskbytes(A::List) = 0
minbitmaskbytes(A::NullableList) = bytesforbits(length(A))

offsetsbytes(A::AbstractVector) = (length(A)+1)*sizeof(Int32)
export offsetsbytes

function minbytes(::Type{C}, A::AbstractVector{T}
                 ) where {C,K<:AbstractString,T<:Union{Union{K,Missing},K}}
    valuesbytes(C, A) + minbitmaskbytes(A) + offsetsbytes(A)
end
minbytes(A::AbstractList) = valuesbytes(A) + minbitmaskbytes(A) + offsetsbytes(A)


# TODO how to deal with sizeof of Arrow objects such as lists?
"""
    offsets(v::AbstractVector)

Construct a `Vector{Int32}` of offsets appropriate for data appearing in `v`.
"""
function offsets(v::AbstractVector)
    off = Vector{Int32}(length(v)+1)
    off[1] = 0
    for i ∈ 2:length(off)
        off[i] = sizeof(v[i-1]) + off[i-1]
    end
    off
end
export offsets


# note that there are always n+1 offsets
"""
    unsafe_offset(l::AbstractList, i::Integer)

Get the offset for element `i`.  Contains a call to `unsafe_load`.
"""
unsafe_offset(l::AbstractList, i::Integer) = unsafe_load(convert(Ptr{Int32}, l.offsets), i)


"""
    unsafe_setoffset!(l::AbstractList, off::Int32, i::Integer)

Set offset `i` to `off`.  Contains a call to `unsafe_store!`.
"""
function unsafe_setoffset!(l::AbstractList, off::Int32, i::Integer)
    unsafe_store!(convert(Ptr{Int32}, l.offsets), off, i)
end


"""
    unsafe_setoffsets!(l::AbstractList, off::Vector{Int32})

Set all offsets to the `Vector{Int32}` `off`.  Contains a call to `unsafe_copy!` which copies the
entirety of `off`.
"""
function unsafe_setoffsets!(l::AbstractList, off::Vector{Int32})
    unsafe_copy!(convert(Ptr{Int32}, l.offsets), pointer(off), length(off))
end


"""
    unsafe_ellength(l::AbstractList, i::Integer)

Get the length of element `i`. Involves calls to `unsafe_load`.
"""
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


