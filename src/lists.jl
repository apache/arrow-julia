
# TODO bounds checking for all indices

abstract type AbstractList{J} <: ArrowVector{J} end
export AbstractList


"""
    List{P<:AbstractPrimitive,J} <: AbstractList{J}

An Arrow formatted array of variable length objects such as strings. The `List` contains "value" data
as well as "offsets" which describe from which elements of the values data an element of `List`
should be constructed.  The offsets are necessarily a `Primitive{Int32}` while the values can
be any `ArrowVector` type (but in most circumstances should be `Primitive`).

## Constructors
    List{J}(len::Integer, offs::Primitive{Int32}, vals::AbstractPrimitive)
    List{J}(offs::Primitive{Int32}, vals::AbstractPrimitive)
    List{J}(data::Vector{UInt8}, offset_idx::Integer, len::Integer, vals::AbstractPrimitive)
    List{J}(data::Vector{UInt8}, offset_idx::Integer, values_idx::Integer, ::Type{C}, x::AbstractVector)
    List{J}(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector)
    List(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector)
    List{J}(Array, ::Type{C}, x::AbstractVector)
    List(Array, ::Type{C}, x::AbstractVector)
    List(::Type{C}, v::AbstractVector)
    List(v::AbstractVector{<:AbstractString})

Note that by default, `List`s of strings will be encoded in UTF-8.

### Arguments
- `len`: the length of the `List`
- `offs`: a `Primitive{Int32}` containing the offsets data
- `vals`: a `Primitive` containing the values data
- `data`: the data buffer in which the underlying data is stored
- `offset_idx`: location within `data` where the offset data is stored
- `values_idx`: location within `data` where the values data is stored
- `C`: the encoding type (type of values), e.g. for UTF-8 strings this is `UInt8`. defaults to `UInt8`
    if not given explicitly
- `i`: the location in `data` where all data should be stored (offsets, then values)
- `x`, `v`: array to be stored or converted
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

function List{J}(data::Vector{UInt8}, offset_idx::Integer, len::Integer, vals::P) where {J,P}
    List{J,P}(offs, vals)
end

# all index constructor
function List{J}(data::Vector{UInt8}, offset_idx::Integer, values_idx::Integer, len::Integer, ::Type{C},
                 values_len::Integer) where {J,C}
    vals = Primitive{C}(data, values_idx, values_len)
    List{J}(data, offset_idx, len, vals)
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
function List{J}(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector) where {C,J}
    offs = Primitive{Int32}(data, i, offsets(C, x))
    p = Primitive(data, i+offsetsbytes(x), encode(C, x))
    List{J}(offs, p)
end
function List(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector{J}) where {C,J}
    List{J}(data, i, C, x)
end

function List{J}(data::Vector{UInt8}, i::Integer, x::AbstractVector{<:AbstractString}) where J
    List{J}(data, i, UInt8, x)
end
List(data::Vector{UInt8}, i::Integer, x::AbstractVector{<:AbstractString}) = List(data, i, UInt8, x)

function List{J}(::Type{<:Array}, ::Type{C}, x::AbstractVector) where {C,J}
    b = Vector{UInt8}(uninitialized, totalbytes(C, x))
    List{J}(b, 1, C, x)
end
function List(::Type{<:Array}, ::Type{C}, x::AbstractVector{J}) where {C,J}
    List{J}(Array, C, x)
end

function List{J}(::Type{<:Array}, x::AbstractVector{<:AbstractString}) where J
    List{J}(Array, UInt8, x)
end
function List(::Type{<:Array}, x::AbstractVector{<:AbstractString})
    List(Array, UInt8, x)
end

function List{J}(::Type{C}, v::AbstractVector) where {J,C}
    offs = Primitive{Int32}(offsets(C, v))
    p = Primitive(encode(C, v))
    List{J}(offs, p)
end
List(::Type{C}, v::AbstractVector{J}) where {J,C} = List{J}(C, v)
List(v::AbstractVector{<:AbstractString}) = List{String}(UInt8, v)


List{J}(l::List{J}) where J = List{J}(l.length, l.offsets, l.values)
List{J}(l::List{T}) where {J,T} = List{J}(convert(AbstractVector{J}, l[:]))
List(l::List{J}) where J = List{J}(l)


"""
    NullableList{P<:AbstractPrimitive,J} <: AbstractList{Union{Missing,J}}

An Arrow formatted array of variable length objects such as strings which may be null. The `NullableList`
contains a bit mask specifying which values are null and "offsets" which describe from which elements of
the values data an element of the `NullableList` should be constructed.  The bitmask is contained in a
`Primitive{UInt8}` while the offsets data in a `Primitive{Int32}`. The values can be contained in any
`ArrowVector` type, but in most cases should be `Primitive`.

## Constructors
    NullableList{J}(len::Integer, bmask::Primitive, offs::Primitive, vals::AbstractPrimitive)
    NullableList{J}(bmask::Primitive, offs::Primitive, vals::AbstractPrimitive)
    NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer, len::Integer,
                    vals::AbstractPrimitive)
    NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer, values_idx::Integer,
                    len::Integer, ::Type{C}, values_len::Integer)
    NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer, values_idx::Integer,
                    ::Type{C}, x::AbstractVector)
    NullableList(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector)
    NullableList(Array, ::Type{C}, x::AbstractVector)
    NullableList(Array, x::AbstracVector)
    NullableList(::Type{C}, v::AbstractVector)
    NullableList(v::AbstractVector)

If `Array` is given as an argument, a contiguous array will be allocated to store the data.

### Arguments
- `len`: the length of the `NullableList`
- `bmask`: the `Primitive` providing the bit mask
- `offs`: the `Primitive` providing the offsets
- `vals`: the `AbstractPrimitive` providing the values
- `data`: a buffer for storing the data
- `bitmask_idx`: the location in `data` of the bit mask
- `offsets_idx`: the location in `data` of the offsets
- `values_idx`: the location in `data` of the values
- `values_len`: the total length of the values data (i.e. number of elements in the values array)
- `C`: the data type of the values data. defaults to `UInt8` when not provided
- `x`, `v`: array to be stored by the `NullableList`
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

function NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer,
                         len::Integer, vals::P) where {J,P}
    bmask = Primitive{UInt8}(data, bitmask_idx, bytesforbits(len))
    offs = Primitive{Int32}(data, offset_idx, len+1)
    NullableList{J}(bmask, offs, vals)
end

# all index constructor
function NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer,
                         values_idx::Integer, len::Integer, ::Type{C}, values_len::Integer) where {J,C}
    vals = Primitive{C}(data, values_idx, values_len)
    NullableList{J}(data, bitmask_idx, offset_idx, len, vals)
end

# buffer with location constructors, with values arg
function NullableList{J}(data::Vector{UInt8}, bitmask_idx::Integer, offset_idx::Integer,
                         values_idx::Integer, len::Integer, vals::P) where {J,P<:AbstractPrimitive}
    bmask = Primitive{UInt8}(data, bitmask_idx, bitmaskbytes(len))
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
function NullableList{J}(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector) where {C,J}
    bmask = Primitive{UInt8}(data, i, bitmask(x))
    offs = Primitive{Int32}(data, i+bitmaskbytes(x), offsets(C, x))
    vals = Primitive(data, i+bitmaskbytes(x)+offsetsbytes(x), encode(C, x))
    NullableList{J}(bmask, offs, vals)
end
function NullableList(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector{Union{J,Missing}}
                     ) where {C,J}
    NullableList{J}(data, i, C, x)
end
function NullableList(data::Vector{UInt8}, i::Integer, ::Type{C}, x::AbstractVector{J}) where {C,J}
    NullableList{J}(data, i, C, x)
end

function NullableList{J}(data::Vector{UInt8}, i::Integer, x::AbstractVector{T};
                        ) where {J,K<:AbstractString,T<:Union{K,Union{K,Missing}}}
    NullableList{J}(data, i, UInt8, x)
end
function NullableList(data::Vector{UInt8}, i::Integer, x::AbstractVector{T};
                     ) where {K<:AbstractString,T<:Union{K,Union{K,Missing}}}
    NullableList(data, i, UInt8, x)
end

function NullableList{J}(::Type{<:Array}, ::Type{C}, x::AbstractVector) where {C,J}
    b = Vector{UInt8}(uninitialized, minbytes(C, x))
    NullableList{J}(b, 1, C, x)
end
function NullableList(::Type{<:Array}, ::Type{C}, x::AbstractVector{Union{J,Missing}}) where {C,J}
    NullableList{J}(Array, C, x)
end
function NullableList(::Type{<:Array}, ::Type{C}, x::AbstractVector{J}) where {C,J}
    NullableList{J}(Array, C, x)
end

function NullableList{J}(::Type{<:Array}, x::AbstractVector{T}
                        ) where {J,K<:AbstractString,T<:Union{K,Union{K,Missing}}}
    NullableList{J}(Array, UInt8, x)
end
function NullableList(::Type{<:Array}, x::AbstractVector{T}
                     ) where {K<:AbstractString,T<:Union{K,Union{K,Missing}}}
    NullableList(Array, UInt8, x)
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


NullableList{J}(l::NullableList{J}) where J = NullableList{J}(p.length, p.bitmask, p.offsets, p.values)
NullableList{J}(l::NullableList{T}) where {J,T} = NullableList{J}(convert(AbstractVector{J}, p[:]))
NullableList(l::NullableList{J}) where J = NullableList{J}(l)


#====================================================================================================
    common interface
====================================================================================================#
function valuesbytes(::Type{C}, A::AbstractVector{T}
                    ) where {C,K<:AbstractString,T<:Union{Union{K,Missing},K}}
    padding(sum(ismissing(a) ? 0 : length(a)*sizeof(C) for a ∈ A))
end
valuesbytes(A::Union{List{P,J},NullableList{P,J}}) where {P,J} = valuesbytes(A.values)

bitmaskbytes(A::List) = 0
bitmaskbytes(A::NullableList) = bytesforbits(length(A))

offsetsbytes(len::Integer) = padding((len+1)*sizeof(Int32))
offsetsbytes(A::AbstractVector) = offsetsbytes(length(A))
export offsetsbytes

function totalbytes(::Type{C}, A::AbstractVector{T}
                   ) where {C,K<:AbstractString,T<:Union{Union{K,Missing},K}}
    valuesbytes(C, A) + bitmaskbytes(A) + offsetsbytes(A)
end
function totalbytes(::Type{Union{J,Missing}}, ::Type{C}, A::AbstractVector{J}) where {C,J}
    valuesbytes(C, A) + bitmaskbytes(Union{J,Missing}, A) + offsetsbytes(A)
end
function totalbytes(::Type{Union{J,Missing}}, ::Type{C}, A::AbstractVector{Union{J,Missing}}) where {C,J}
    valuesbytes(C, A) + bitmaskbytes(Union{J,Missing}, A) + offsetsbytes(A)
end
totalbytes(A::AbstractList) = valuesbytes(A) + minbitmaskbytes(A) + offsetsbytes(A)


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
    off = Vector{Int32}(uninitialized, length(v)+1)
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


rawvalues(p::AbstractList, i) = rawvalues(p.values, i)
rawvalues(p::AbstractList) = rawvalues(p.values)


# note that there are always n+1 offsets
"""
    unsafe_getoffset(l::AbstractList, i::Integer)

Get the offset for element `i`.  Contains a call to `unsafe_load`.
"""
unsafe_getoffset(l::AbstractList, i) = unsafe_getvalue(offsets(l), i)


"""
    unsafe_rawoffsets(p::AbstractList)

Retreive the raw offstets for `p` as a `Vector{UInt8}`.
"""
unsafe_rawoffsets(p::AbstractList) = unsafe_rawpadded(offsetspointer(p), offsetsbytes(p))


"""
    getoffset(l::AbstractList, i::Integer)

Retrieve offset `i` for list `l`.  Note that this retrieves the Arrow formated 0-based indexed raw
numbers!
"""
getoffset(l::AbstractList, i) = l.offsets[i]
export getoffset


"""
    unsafe_setoffset!(l::AbstractList, off::Int32, i::Integer)

Set offset `i` to `off`.  Contains a call to `unsafe_store!`.
"""
unsafe_setoffset!(l::AbstractList, off::Int32, i::Integer) = unsafe_setvalue!(offsets(l), off, i)


setoffset!(l::AbstractList, off::Int32, i::Integer) = setindex!(l.offsets, off, i)


"""
    unsafe_setoffsets!(l::AbstractList, off::Vector{Int32})

Set all offsets to the `Vector{Int32}` `off`.  Contains a call to `unsafe_copy!` which copies the
entirety of `off`.
"""
function unsafe_setoffsets!(l::AbstractList, off::Vector{Int32})
    unsafe_copy!(convert(Ptr{Int32}, l.offsets), pointer(off), length(off))
end


setoffsets!(l::AbstractList, off::AbstractVector{Int32}) = (l.offsets[:] = off)


"""
    unsafe_ellength(l::AbstractList, i::Integer)

Get the length of element `i`. Involves calls to `unsafe_load`.
"""
unsafe_ellength(l::AbstractList, i::Integer) = unsafe_getoffset(l, i+1) - unsafe_getoffset(l, i)


"""
    ellength(l::AbstractList, i::Integer)

Get the length of element `i`.
"""
ellength(l::AbstractList, i::Integer) = getoffset(l, i+1) - getoffset(l, i)


# returns offset, length
function unsafe_elparams(l::AbstractList, i::Integer)
    off = unsafe_getoffset(l, i)
    off, unsafe_getoffset(l, i+1) - off
end

function elparams(l::AbstractList, i::Integer)
    off = getoffset(l, i)
    off, getoffset(l, i+1) - off
end


function unsafe_getvalue(l::AbstractList{T}, i::Integer) where {J,T<:Union{J,Union{J,Missing}}}
    off, len = unsafe_elparams(l, i)
    unsafe_construct(J, values(l), off+1, len)
end
function unsafe_getvalue(l::AbstractList{T}, idx::AbstractVector{<:Integer}
                        ) where {J,T<:Union{J,Union{J,Missing}}}
    T[unsafe_getvalue(l, i) for i ∈ idx]
end
function unsafe_getvalue(l::AbstractList{T}, idx::AbstractVector{Bool}
                        ) where {J,T<:Union{J,Union{J,Missing}}}
    T[unsafe_getvalue(l, i) for i ∈ 1:length(l) if idx[i]]
end


function getvalue(l::AbstractList{T}, i::Integer) where {J,T<:Union{J,Union{J,Missing}}}
    off, len = elparams(l, i)
    construct(J, l.values, off+1, len)
end

function getvalue(l::AbstractList{T}, idx::AbstractVector{<:Integer}
                 ) where {J,T<:Union{J,Union{J,Missing}}}
    J[getvalue(l, i) for i ∈ idx]
end
function getvalue(l::AbstractList{T}, idx::AbstractVector{Bool}
                 ) where {J,T<:Union{J,Union{J,Missing}}}
    J[getvalue(l, i) for i ∈ 1:length(l) if idx[i]]
end

