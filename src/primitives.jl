
# TODO bounds checking in constructors for all indices

abstract type AbstractPrimitive{J} <: ArrowVector{J} end
export AbstractPrimitive


"""
    Primitive{J} <: AbstractPrimitive{J}

An arrow primitive array containing no null values.  This is essentially just a wrapped pointer
to the data.  The index `i` should give the start of the array relative to `ptr` using 1-based indexing.

**WARNING** Because the Arrow format is very general, Arrow.jl cannot provide much help in organizing
your data buffer. It is up to *you* to ensure that your pointers are correct and don't overlap!

## Constructors

    Primitive{J}(ptr, i::Integer, len::Integer)
    Primitive{J}(ptr, i::Integer, x::AbstractVector{J})

### Arguments
- `ptr` an array pointer or Arrow `Buffer` object
- `i` the location the data should be stored using 1-based indexing
- `x` a vector that can be represented as an Arrow `Primitive`
"""
struct Primitive{J} <: AbstractPrimitive{J}
    length::Int32
    values_idx::Int64
    data::Vector{UInt8}
end
export Primitive

function Primitive{J}(data::Vector{UInt8}, i::Integer, len::Integer) where J
    @boundscheck check_buffer_bounds(J, data, i, len)
    Primitive{J}(len, i, data)
end
function Primitive(data::Vector{UInt8}, i::Integer, x::AbstractVector{J}) where J
    p = Primitive{J}(data, i, length(x))
    p[:] = x
    p
end
function Primitive{J}(data::Vector{UInt8}, i::Integer, x::AbstractVector{K}) where {J,K}
    Primitive(data, i, convert(AbstractVector{J}, x))
end

# view of reinterpreted
function Primitive(v::AbstractVector{J}) where J
    b = reinterpret(UInt8, v)
    Primitive(b, 1, length(v))
end

# create own buffer
function Primitive{J}(::Type{<:Array}, v::AbstractVector; padding::Function=identity) where J
    b = Vector{UInt8}(padding(minbytes(v)))
    Primitive{J}(b, 1, v)
end
function Primitive(::Type{<:Array}, v::AbstractVector; padding::Function=identity)
    Primitive(v, padding=padding)
end


"""
    datapointer(A::Primitive)

Returns a pointer to the very start of the data buffer for `A` (i.e. does not depend on indices).
"""
datapointer(A::Primitive) = pointer(A.data)
export datapointer

valuespointer(A::Primitive) = datapointer(A) + A.values_idx - 1


#==================================================================================================
    NullablePrimitive
==================================================================================================#
"""
    NullablePrimitive{J} <: AbstractPrimitive{Union{J,Missing}}

An arrow primitive array possibly containing null values.  This is essentially a pair of wrapped
pointers: one to the data and one to the bitmask specifying whether each value is null.
The bitmask and data locations should be given relative to `ptr` using 1-based indexing.

**WARNING** Because the Arrow format is very general, Arrow.jl cannot provide much help in organizing
your data buffer. It is up to *you* to ensure that your pointers are correct and don't overlap!

## Constructors

    NullablePrimitive{J}(ptr, bitmask_loc::Integer, data_loc::Integer, len::Integer)
    NullablePrimitive{J}(ptr, bitmask_loc::Integer, data_loc::Integer, x::AbstractVector)

### Arguments
- `ptr` an array pointer or Arrow `Buffer` object
- `bitmask_loc` the location of the null bit mask using 1-based indexing
- `data_loc` the location of the data using 1-based indexing
- `len` the length of the `NullablePrimitive`
- `x` a vector that can be represented as an Arrow `NullablePrimitive`
"""
struct NullablePrimitive{J} <: AbstractPrimitive{Union{J,Missing}}
    length::Int32
    bitmask::Primitive{UInt8}
    values::Primitive{J}
end
export NullablePrimitive

# Primitive constructors
function NullablePrimitive{J}(bmask::Primitive{UInt8}, vals::Primitive{J}) where J
    NullablePrimitive{J}(length(vals), bmask, vals)
end
function NullablePrimitive(bmask::Primitive{UInt8}, vals::Primitive{J}) where J
    NullablePrimitive{J}(bmask, vals)
end

# buffer with location constructors
function NullablePrimitive{J}(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                              len::Integer) where J
    bmask = Primitive{UInt8}(data, bitmask_idx, bytesforbits(len))
    vals = Primitive{J}(data, values_idx, len)
    NullablePrimitive{J}(bmask, vals)
end
function NullablePrimitive(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                           x::AbstractVector{T}) where {J,T<:Union{Union{J,Missing},J}}
    bmask = Primitive{UInt8}(data, bitmask_idx, bitmask(x))
    vals = Primitive{J}(data, values_idx, length(x))
    setnonmissing!(vals, x)
    NullablePrimitive{J}(bmask, vals)
end
function NullablePrimitive(data::Vector{UInt8}, i::Integer, v::AbstractVector{T};
                           padding::Function=identity) where {J,T<:Union{J,Union{J,Missing}}}
    NullablePrimitive(data, i, i+padding(minbitmaskbytes(v)), v)
end
function NullablePrimitive{J}(data::Vector{UInt8}, i::Integer, v::AbstractVector{T};
                              padding::Function=identity) where {J,T}
    NullablePrimitive(data, i, convert(AbstractVector{J}, v), padding=padding)
end

# contiguous new buffer constructors
function NullablePrimitive(::Type{<:Array}, v::AbstractVector{Union{J,Missing}};
                           padding::Function=identity) where J
    b = Vector{UInt8}(minbytes(v))
    NullablePrimitive(b, 1, v, padding=padding)
end
function NullablePrimitive{J}(::Type{K}, v::AbstractVector{T};
                              padding::Function=identity) where {J,K<:Array,T}
    NullablePrimitive(K, convert(AbstractVector{Union{J,Missing}}, v), padding=padding)
end
function NullablePrimitive(::Type{K}, v::AbstractVector{J};
                           padding::Function=identity) where {K<:Array,J}
    NullablePrimitive(K, convert(AbstractVector{Union{J,Missing}}, v), padding=padding)
end

# new buffer constructors
function NullablePrimitive(v::AbstractVector{Union{J,Missing}}) where J
    bmask = Primitive(bitmask(v))
    vals = Primitive(replace_missing_vals(v))  # using first ensures exists
    NullablePrimitive(bmask, vals)
end
function NullablePrimitive(v::AbstractVector{J}) where J
    NullablePrimitive(convert(AbstractVector{Union{J,Missing}}, v))
end
function NullablePrimitive{J}(v::AbstractVector{K}) where {J,K}
    NullablePrimitive(convert(AbstractVector{Union{J,Missing}}, v))
end


#================================================================================================

    common interface

================================================================================================#
"""
    valuesbytes(A::AbstractVector)
    valuesbytes(::Type{C}, A::AbstractVector{<:AbstractString})

Computes the number of bytes needed to store the *values* of `A` (without converting the underlying
binary type). This does not include the number of bytes needed to store metadata such as a null
bitmask or offsets.

To obtain the number of values bytes needed to string data, one must input `C` the character encoding
type the string will be converted to (e.g. `UInt8`).
"""
valuesbytes(A::AbstractVector{J}) where J = length(A)*sizeof(J)
valuesbytes(A::AbstractVector{Union{J,Missing}}) where J = length(A)*sizeof(J)
export valuesbytes

"""
    minbitmaskbytes(A::AbstractVector)
    minbitmaskbytes(::Type{Union{J,Missing}}, A::AbstractVector)

Compute the minimum number of bytes needed to store a null bitmask for the data in `A`.  This is 0
unless `J <: Union{K,Missing}`. Note that this does not take into account scheme-dependent padding.
"""
minbitmaskbytes(A::AbstractVector) = 0
minbitmaskbytes(::Type{Union{J,Missing}}, A::AbstractVector{J}) where J = bytesforbits(length(A))
function minbitmaskbytes(::Type{Union{J,Missing}}, A::AbstractVector{Union{J,Missing}}) where J
    bytesforbits(length(A))
end
minbitmaskbytes(A::AbstractVector{Union{J,Missing}}) where J = bytesforbits(length(A))
export minbitmaskbytes

"""
    minbytes(A::AbstractVector)
    minbytes(::Type{Union{J,Missing}}, A::AbstractVector)
    minbytes(::Type{C}, A::AbstractVector)
    minbytes(::Type{Union{J,Missing}}, ::Type{C}, A::AbstractVector)

Computes the minimum number of bytes needed to store `A` as an Arrow formatted primitive array or list.

To obtain the minimum bytes to store string data, one must input `C` the character encoding type the
string will be converted to (e.g. `UInt8`).
"""
minbytes(A::AbstractVector) = minbitmaskbytes(A) + valuesbytes(A)
function minbytes(::Type{Union{J,Missing}}, A::AbstractVector{J}) where J
    minbitmaskbytes(Union{J,Missing}, A) + valuesbytes(A)
end
function minbytes(::Type{Union{J,Missing}}, A::AbstractVector{Union{J,Missing}}) where J
    minbitmaskbytes(Union{J,Missing}, A) + valuesbytes(A)
end
export minbytes



"""
    unsafe_getvalue(A::ArrowVector, i)

Retrieve the value from memory location `i` using Julia 1-based indexing. `i` can be a single integer
index, an `AbstractVector` of integer indices, or an `AbstractVector{Bool}` mask.

This typically involves a call to `unsafe_load` or `unsafe_wrap`.
"""
function unsafe_getvalue(A::Union{Primitive{J},NullablePrimitive{J}}, i::Integer)::J where J
    unsafe_load(convert(Ptr{J}, valuespointer(A)), i)
end
function unsafe_getvalue(A::Union{Primitive{J},NullablePrimitive{J}},
                         idx::AbstractVector{<:Integer}) where J
    ptr = convert(Ptr{J}, valuespointer(A)) + (idx[1]-1)*sizeof(J)
    unsafe_wrap(Array, ptr, length(idx))
end
function unsafe_getvalue(A::Primitive{J}, idx::AbstractVector{Bool}) where J
    J[unsafe_getvalue(A, i) for i ∈ 1:length(A) if idx[i]]
end


_rawvalueindex_start(A::Primitive{J}, i::Integer) where J = A.values_idx + (i-1)*sizeof(J)
_rawvalueindex_stop(A::Primitive{J}, i::Integer) where J = A.values_idx + i*sizeof(J) - 1

function rawvalueindex_contiguous(A::Primitive{J}, idx::AbstractVector{<:Integer}) where J
    a = _rawvalueindex_start(A, first(idx))
    b = _rawvalueindex_stop(A, last(idx))
    a:b
end


function rawvalueindex(A::Primitive{J}, i::Integer) where J
    a = _rawvalueindex_start(A, i)
    b = _rawvalueindex_stop(A, i)
    a:b
end
function rawvalueindex(A::Primitive, idx::AbstractVector{<:Integer})
    vcat((rawvalueindex(A, i) for i ∈ idx)...)  # TODO inefficient use of vcat
end
rawvalueindex(A::Primitive, idx::UnitRange{<:Integer}) = rawvalueindex_contiguous(A, idx)
function rawvalueindex(A::Primitive, idx::AbstractVector{Bool})
    rawvalueindex(A, [i for i ∈ 1:length(A) if idx[i]])
end


rawvalues(A::Primitive, i::Union{<:Integer,AbstractVector{<:Integer}}) = A.data[rawvalueindex(A, i)]
rawvalues(A::Primitive, ::Colon) = rawvalues(A, 1:length(A))
rawvalues(A::Primitive) = rawvalues(A, :)


"""
    getvalue(A::ArrowVector, idx)

Get the values for indices `idx` from `A`.
"""
function getvalue(A::AbstractPrimitive{T}, i::Integer) where {J,T<:Union{J,Union{J,Missing}}}
    reinterpret(J, rawvalues(A, i))[1]
end
function getvalue(A::AbstractPrimitive{T}, i::AbstractVector{<:Integer}
                 ) where {J,T<:Union{J,Union{J,Missing}}}
    reinterpret(J, rawvalues(A, i))
end


# TODO this should probably be renamed
"""
    rawvalues(p::ArrowVector, padding::Function=identity)

Retreive raw value data for `p` as a `Vector{UInt8}`.

The function `padding` should take as its sole argument the number of bytes of the raw values
and return the total number of bytes appropriate for the padding scheme.
"""
function unsafe_rawvalues(p::AbstractPrimitive, padding::Function=identity)
    unsafe_rawpadded(valuespointer(p), valuesbytes(p), padding)
end
export unsafe_rawvalues


function setvalue!(A::Primitive{J}, x::J, i::Integer) where J
    A.data[rawvalueindex(A, i)] = reinterpret(UInt8, [x])
end
function setvalue!(A::Primitive{J}, x::Vector{J}, idx::AbstractVector{<:Integer}) where J
    A.data[rawvalueindex(A, idx)] = reinterpret(UInt8, x)
end
function setvalue!(A::NullablePrimitive{J}, x::J, i::Integer) where J
    setvalue!(A.values, x, i)
end
function setvalue!(A::NullablePrimitive{J}, x::Vector{J}, idx::AbstractVector{<:Integer}) where J
    setvalue!(A.values, x, idx)
end


"""
    unsafe_setvalue!(A::ArrowVector{J}, x, i)

Set the value at location `i` to `x`.  If `i` is a single integer, `x` should be an element of type
`J`.  Otherwise `i` can be an `AbstractVector{<:Integer}` or `AbstractVector{Bool}` in which case
`x` should be an appropriately sized `AbstractVector{J}`.
"""
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, x::J, i::Integer) where J
    unsafe_store!(convert(Ptr{J}, valuespointer(A)), x, i)
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::AbstractVector{J},
                          idx::AbstractVector{<:Integer}) where J
    ptr = convert(Ptr{J}, valuespointer(A))
    for (x, i) ∈ zip(v, idx)
        unsafe_store!(ptr, x, i)
    end
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::AbstractVector{J},
                          idx::AbstractVector{Bool}) where J
    ptr = convert(Ptr{J}, valuespointer(A))
    j = 1
    for i ∈ 1:length(A)
        if idx[i]
            unsafe_store!(ptr, v[j], i)
            j += 1
        end
    end
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::Vector{J}, ::Colon) where J
    unsafe_copy!(convert(Ptr{J}, valuespointer(A)), pointer(v), length(v))
end


"""
    unsafe_construct(::Type{T}, A::Primitive, i::Integer, len::Integer)

Construct an object of type `T` using `len` elements from `A` starting at index `i` (1-based indexing).
This is mostly used by `AbstractList` objects to construct variable length objects such as strings
from primitive arrays.

Users must define new methods for new types `T`.
"""
function unsafe_construct(::Type{String}, A::Primitive{UInt8}, i::Integer, len::Integer)
    unsafe_string(convert(Ptr{UInt8}, valuespointer(A) + (i-1)), len)
end
function unsafe_construct(::Type{WeakRefString{J}}, A::Primitive{J}, i::Integer, len::Integer) where J
    WeakRefString{J}(convert(Ptr{J}, valuespointer(A) + (i-1)), len)
end

function unsafe_construct(::Type{T}, A::NullablePrimitive{J}, i::Integer, len::Integer) where {T,J}
    nullexcept_inrange(A, i, i+len-1)
    unsafe_construct(T, A, i, len)
end


"""
    construct(::Type{T}, A::AbstractPrimitive{J}, i::Integer, len::Integer)

Construct an object of type `T` from `len` values in `A` starting at index `i`.
For this to work requires the existence of a constructor of the form `T(Vector{J})`.
"""
function construct(::Type{T}, A::AbstractPrimitive, i::Integer, len::Integer) where T
    T(A[i:(i+len-1)])  # obviously this depends on the existence of this constructor
end


function setindex!(A::Primitive{J}, x, i::Integer) where J
    @boundscheck checkbounds(A, i)
    setvalue!(A, convert(J, x), i)  # should this conversion really be here?
end
# TODO inefficient in some cases because of conversion to Vector{J}
function setindex!(A::Primitive{J}, x::AbstractVector, idx::AbstractVector{<:Integer}) where J
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    setvalue!(A, convert(Vector{J}, x), idx)
end
setindex!(A::Primitive, x::AbstractVector, ::Colon) = (A[1:end] = x)

function setindex!(A::NullablePrimitive{J}, x, i::Integer) where J
    @boundscheck checkbounds(A, i)
    o = setvalue!(A, convert(J, x), i)
    setnull!(A, false, i)  # important that this is last in case above fails
    o
end
function setindex!(A::NullablePrimitive{J}, x::Missing, i::Integer) where J
    @boundscheck checkbounds(A, i)
    setnull!(A, true, i)
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
    for i ∈ 1:length(A)
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
    setnulls!(A, ismissing.(x))
    for i ∈ 1:length(A)
        !ismissing(x[i]) && setvalue!(A, convert(J, x[i]), i)
    end
    x
end
