
# TODO bounds checking for all indices

abstract type AbstractList{J} <: ArrowVector{J} end
export AbstractList


"""
    List{P<:AbstractPrimitive,J} <: AbstractList{J}

An Arrow list of variable length objects such as strings, none of which are
null.  `vals` is the primitive array in which the underlying data is stored.
The `List` itself contains a pointer to the offsets, a buffer containing 32-bit
integers describing the offsets of each value.  The location should be given
relative to `ptr` using 1-based indexing.

**WARNING** Because the Arrow format is very general, Arrow.jl cannot provide much help in organizing
your data buffer. It is up to *you* to ensure that your pointers are correct and don't overlap!

## Constructors

    List{P,J}(ptr, offset_loc::Integer, len::Integer, vals::P)
    List{P,J}(ptr, offset_loc::Integer, data_loc::Integer, ::Type{U}, x::AbstractVector{J})

### Arguments
- `ptr` an array pointer or Arrow `Buffer` object
- `offset_loc` the location of the offsets using 1-based indexing
- `data_loc` the location of the underlying values data using 1-based indexing
- `len` the number of elements in the list
- `vals` an `ArrowVector` containing the underlying data values
- `U` the encoding type of the underlying data. for instance, for UTF8 strings use `UInt8`
- `x` a vector that can be represented as an Arrow `List`
"""
struct List{J,P<:AbstractPrimitive} <: AbstractList{J}
    length::Int32
    offsets::Primitive{Int32}
    values::P
end
export List

# Primitive constructors
function List{J}(len::Integer, offs::Primitive{Int32}, vals::P) where {J,P<:AbstractPrimitive}
    List{J,P}(len, offs, vals)
end
function List{J}(offs::Primitive{Int32}, vals::P) where {J,P<:AbstractPrimitive}
    List{J,P}(length(offs)-1, offs, vals)
end

# all index constructor
function List{J}(data::Vector{UInt8}, offset_idx::Integer, values_idx::Integer, len::Integer, ::Type{C},
                 values_len::Integer) where {J,C}
    offs = Primitive{Int32}(data, offset_idx, len+1)
    vals = Primitive{C}(data, values_idx, values_len)
    List{J}(offs, vals)
end

# buffer with location constructors, with values arg
function List{J}(data::Vector{UInt8}, offset_idx::Integer, len::Integer, vals::P
                ) where {P<:AbstractPrimitive,J}
    offs = Primitive{Int32}(data, offset_idx, len+1)
    List{J,P}(len, offs, vals)
end

# buffer with location constructors
function List{J}(data::Vector{UInt8}, offset_idx::Integer, values_idx::Integer, ::Type{C},
                 x::AbstractVector) where {C,J}
    offs = Primitive{Int32}(data, offset_idx, offsets(C, x))
    p = Primitive(data, values_idx, encode(C, x))
    List{J}(offs, p)
end
function List(data::Vector{UInt8}, offset_idx::Integer, values_idx::Integer, ::Type{C},
              x::AbstractVector{J}) where {C,J}
    List{J}(data, offset_idx, values_idx, C, x)
end
# this puts offsets first
function List{J}(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector;
                 padding::Function=identity) where {C,J}
    offs = Primitive{Int32}(data, i, offsets(C, x))
    p = Primitive(data, i+padding(offsetsbytes(x)), encode(C, x))
    List{J}(offs, p)
end
function List(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector{J};
              padding::Function=identity) where {C,J}
    List{J}(data, i, C, x, padding=padding)
end

function List{J}(::Type{<:Array}, ::Type{C}, x::AbstractVector; padding::Function=identity) where {C,J}
    b = Vector{UInt8}(minbytes(C, x))
    List{J}(b, 1, C, x, padding=padding)
end
function List(::Type{<:Array}, ::Type{C}, x::AbstractVector{J}; padding::Function=identity) where {C,J}
    List{J}(Array, C, x, padding=padding)
end

function List{J}(::Type{C}, v::AbstractVector) where {J,C}
    offs = Primitive{Int32}(offsets(C, v))
    p = Primitive(encode(C, v))
    List{J}(offs, p)
end
List(::Type{C}, v::AbstractVector{J}) where {J,C} = List{J}(C, v)
List(v::AbstractVector{<:AbstractString}) = List{String}(UInt8, v)


"""
    NullableList{P<:AbstractPrimitive,J} <: AbstractList{Union{Missing,J}}

An arrow list of variable length objects such as strings, some of which may be null.  `vals`
is the primitive array in which the underlying data is stored.  The `NullableList` itself contains
pointers to the offsets and null bit mask which the locations of which should be specified relative
to `ptr` using 1-based indexing.

**WARNING** Because the Arrow format is very general, Arrow.jl cannot provide much help in organizing
your data buffer. It is up to *you* to ensure that your pointers are correct and don't overlap!

## Constructors

    NullableList{P,J}(ptr, bitmask_loc::Integer, offset_loc::Integer, len::Integer, vals::P)
    NullableList{P,J}(ptr, bitmask_loc::Integer, offset_loc::Integer, data_loc::Integer,
                      ::Type{U}, x::AbstractVector)

### Arguments
- `ptr` an array pointer or Arrow `Buffer` object
- `bitmask_loc` the location of the null bit mask using 1-based indexing
- `offset_loc` the location of the offsets using 1-based indexing
- `len` the length of the list
- `vals` an `ArrowVector` containing the underlying values data
- `U` the data type of the underlying values, for example, for UTF8 strings use `UInt8`
- `x` a vector that can be represented as an Arrow `NullableList`
"""
struct NullableList{J,P<:AbstractPrimitive} <: AbstractList{Union{Missing,J}}
    length::Int32
    bitmask::Primitive{UInt8}
    offsets::Primitive{Int32}
    values::P
end
export NullableList

# Primitive constructors
function NullableList{J}(len::Integer, bmask::Primitive{UInt8}, offs::Primitive{Int32},
                         vals::P) where {J,P}
    NullableList{J,P}(len, bmask, offs, vals)
end
function NullableList{J}(bmask::Primitive{UInt8}, offs::Primitive{Int32}, vals::P) where {J,P}
    NullableList{J,P}(length(offs)-1, bmask, offs, vals)
end

# all index constructor
function NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer,
                         values_idx::Integer, len::Integer, ::Type{C}, values_len::Integer) where {J,C}
    bmask = Primitive{UInt8}(data, bitmask_idx, bytesforbits(len))
    offs = Primitive{Int32}(data, offset_idx, len+1)
    vals = Primitive{C}(data, values_idx, values_len)
    NullableList{J}(bmask, offs, vals)
end

# buffer with location constructors, with values arg
function NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer,
                         values_idx::Integer, len::Integer, vals::P) where {J,P<:AbstractPrimitive}
    bmask = Primitive{UInt8}(data, bitmask_idx, bytesforbits(len))
    offs = Primitive{Int32}(data, offset_idx, len+1)
    NullableList{J,P}(bmask, offs, vals)
end

# buffer with location constructors
function NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer,
                         values_idx::Integer, ::Type{C}, x::AbstractVector) where {C,J}
    bmask = Primitive{UInt8}(data, bitmask_idx, bitmask(x))
    offs = Primitive{Int32}(data, offset_idx, offsets(x))
    vals = Primitive(data, values_idx, encode(C, x))
    NullableList{J}(bmask, offs, vals)
end
function NullableList(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer,
                      values_idx::Integer, ::Type{C}, x::AbstractVector{J}) where {C,J}
    NullableList{J}(data, bitmask_idx, offset_idx, values_idx, C, x)
end
# bitmask, offsets, values
function NullableList{J}(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector;
                         padding::Function=identity) where {C,J}
    bmask = Primitive{UInt8}(data, i, bitmask(x))
    offs = Primitive{Int32}(data, i+padding(minbitmaskbytes(x)), offsets(C, x))
    vals = Primitive(data, i+padding(minbitmaskbytes(x))+padding(offsetsbytes(x)), encode(C, x))
    NullableList{J}(bmask, offs, vals)
end
function NullableList(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector{Union{J,Missing}};
                      padding::Function=identity) where {C,J}
    NullableList{J}(data, i, C, x, padding=padding)
end
function NullableList(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector{J};
                      padding::Function=identity) where {C,J}
    NullableList{J}(data, i, C, x, padding=padding)
end

function NullableList{J}(::Type{<:Array}, ::Type{C}, x::AbstractVector;
                         padding::Function=identity) where {C,J}
    b = Vector{UInt8}(minbytes(C, x))
    NullableList{J}(b, 1, C, x, padding=padding)
end
function NullableList(::Type{<:Array}, ::Type{C}, x::AbstractVector{Union{J,Missing}};
                      padding::Function=identity) where {C,J}
    NullableList{J}(Array, C, x, padding=padding)
end
function NullableList(::Type{<:Array}, ::Type{C}, x::AbstractVector{J};
                      padding::Function=identity) where {C,J}
    NullableList{J}(Array, C, x, padding=padding)
end

function NullableList{J}(::Type{C}, v::AbstractVector) where {J,C}
    bmask = Primitive{UInt8}(bitmask(v))
    offs = Primitive{Int32}(offsets(C, v))
    vals = Primitive(encode(C, v))
    NullableList{J}(bmask, offs, vals)
end
function NullableList(::Type{C}, v::AbstractVector{T}) where {C,J,T<:Union{J,Union{J,Missing}}}
    NullableList{J}(C, v)
end
function NullableList(v::AbstractVector{T}) where {K<:AbstractString,T<:Union{K,Union{K,Missing}}}
    NullableList{String}(UInt8, v)
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
function minbytes(::Type{Union{J,Missing}}, ::Type{C}, A::AbstractVector{J}) where {C,J}
    valuesbytes(C, A) + minbitmaskbytes(Union{J,Missing}, A) + offsetsbytes(A)
end
function minbytes(::Type{Union{J,Missing}}, ::Type{C}, A::AbstractVector{Union{J,Missing}}) where {C,J}
    valuesbytes(C, A) + minbitmaskbytes(Union{J,Missing}, A) + offsetsbytes(A)
end
minbytes(A::AbstractList) = valuesbytes(A) + minbitmaskbytes(A) + offsetsbytes(A)


# helper function for offsets
_offsize(::Type{C}, x) where C = sizeof(x)
_offsize(::Type{C}, x::AbstractString) where C = sizeof(C)*length(x)

# TODO how to deal with sizeof of Arrow objects such as lists?
# note that this works fine with missings because sizeof(missing) == 0
"""
    offsets(v::AbstractVector)

Construct a `Vector{Int32}` of offsets appropriate for data appearing in `v`.
"""
function offsets(::Type{C}, v::AbstractVector) where C
    off = Vector{Int32}(length(v)+1)
    off[1] = 0
    for i ∈ 2:length(off)
        off[i] = _offsize(C, v[i-1]) + off[i-1]
    end
    off
end
offsets(v::AbstractVector{K}) where K = offsets(K, v)
function offsets(v::AbstractVector{<:AbstractString})
    throw(ArgumentError("must specify encoding type for computing string offsets"))
end
export offsets


function check_offset_bounds(l::AbstractList, i::Integer)
    if !(1 ≤ i ≤ length(l)+1)
        throw(ArgumentError("tried to access offset $i from list of length $(length(l))"))
    end
end


rawvalues(p::AbstractList, padding::Function=identity) = rawvalues(p.values, padding)


# note that there are always n+1 offsets
"""
    unsafe_getoffset(l::AbstractList, i::Integer)

Get the offset for element `i`.  Contains a call to `unsafe_load`.
"""
unsafe_getoffset(l::AbstractList, i::Integer) = unsafe_load(convert(Ptr{Int32}, l.offsets), i)


"""
    rawoffsets(p::AbstractList, padding::Function=identity)

Retreive the raw offstets for `p` as a `Vector{UInt8}`.

The function `padding` should take as its sole argument the number of bytes of the raw values
and return teh total number of bytes appropriate for the padding scheme.
"""
rawoffsets(p::AbstractList, padding::Function=identity) = rawpadded(p.offsets, offsetsbytes(p), padding)
export rawoffsets


"""
    getoffset(l::AbstractList, i::Integer)

Retrieve offset `i` for list `l`.  Includes bounds checking.

**WARNING** Bounds checking is not useful if pointers are misaligned!
"""
function getoffset(l::AbstractList, i::Integer)
    @boundscheck check_offset_bounds(l, i)
    unsafe_getoffset(l, i)
end
export getoffset


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
unsafe_ellength(l::AbstractList, i::Integer) = unsafe_getoffset(l, i+1) - unsafe_getoffset(l, i)

# returns offset, length
function unsafe_elparams(l::AbstractList, i::Integer)
    off = unsafe_getoffset(l, i)
    off, unsafe_getoffset(l, i+1) - off
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


