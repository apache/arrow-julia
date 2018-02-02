
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

# constructor for own buffer
function Primitive(v::AbstractVector{J}) where J
    b = Vector{UInt8}(minbytes(v))
    Primitive(b, 1, v)
end


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
    bitmask_idx::Int64
    values_idx::Int64
    data::Vector{UInt8}
end
export NullablePrimitive

function NullablePrimitive{J}(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                              len::Integer) where J
    @boundscheck check_buffer_bounds(J, data, values_idx, len)
    NullablePrimitive{J}(len, bitmask_idx, values_idx, data)
end

function NullablePrimitive(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                           x::AbstractVector{T}) where {J,T<:Union{Union{J,Missing},J}}
    p = NullablePrimitive{J}(data, bitmask_idx, values_idx, length(x))
    p[:] = x
    p
end

function NullablePrimitive(v::AbstractVector{Union{J,Missing}}) where J
    b = Vector{UInt8}(minbytes(v))
    NullablePrimitive(b, 1, 1+minbitmaskbytes(v), v)
end
function NullablePrimitive(v::AbstractVector{J}) where J
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
    minbitmaskbytes(A::AbstractVector{J})

Compute the minimum number of bytes needed to store a null bitmask for the data in `A`.  This is 0
unless `J <: Union{K,Missing}`. Note that this does not take into account scheme-dependent padding.
"""
minbitmaskbytes(A::AbstractVector) = 0
minbitmaskbytes(A::AbstractVector{Union{J,Missing}}) where J = bytesforbits(length(A))
export minbitmaskbytes

"""
    minbytes(A::AbstractVector)
    minbytes(::Type{C}, A::AbstractVector{<:AbstractString})

Computes the minimum number of bytes needed to store `A` as an Arrow formatted primitive array or list.

To obtain the minimum bytes to store string data, one must input `C` the character encoding type the
string will be converted to (e.g. `UInt8`).
"""
minbytes(A::AbstractVector) = minbitmaskbytes(A) + valuesbytes(A)
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


# TODO these are broken for stepranges, it's really annoying
function getvalue(A::Union{Primitive{J},NullablePrimitive{J}}, i::Integer)::J where J
    a = A.values_idx + (i-1)*sizeof(J)
    b = a + sizeof(J) - 1
    @inbounds o = getindex(A.data, a:b)
    reinterpret(J, o)[1]
end
function getvalue(A::Union{Primitive{J},NullablePrimitive{J}}, idx::AbstractVector{<:Integer}) where J
    a = A.values_idx + (idx-1)*sizeof(J)
    b = a + sizeof(J) - 1
    @inbounds o = getindex(A.data, a:b)
    reinterpret(J, o)
end
function getvalue(A::Union{Primitive{J},NullablePrimitive{J}}, idx::AbstractVector{Bool}) where J
    J[getvalue(A, i) for i ∈ 1:length(A) if idx[i]]
end


"""
    rawvalues(p::ArrowVector, padding::Function=identity)

Retreive raw value data for `p` as a `Vector{UInt8}`.

The function `padding` should take as its sole argument the number of bytes of the raw values
and return the total number of bytes appropriate for the padding scheme.
"""
function rawvalues(p::AbstractPrimitive, padding::Function=identity)
    rawpadded(valuespointer(p), valuesbytes(p), padding)
end
export rawvalues


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


function setindex!(A::Primitive{J}, x, i::Integer) where J
    @boundscheck checkbounds(A, i)
    unsafe_setvalue!(A, convert(J, x), i)
end
# TODO inefficient in some cases because of conversion to Vector{J}
function setindex!(A::Primitive{J}, x::AbstractVector, idx::AbstractVector{<:Integer}) where J
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    unsafe_setvalue!(A, convert(Vector{J}, x), idx)
end
setindex!(A::Primitive, x::AbstractVector, ::Colon) = (A[1:end] = x)

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
    unsafe_setnulls!(A, ismissing.(x))
    for i ∈ 1:length(A)
        !ismissing(x[i]) && unsafe_setvalue!(A, convert(J, x[i]), i)
    end
    x
end
