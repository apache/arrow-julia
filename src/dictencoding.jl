
"""
    DictEncoding{P<:ArrowVector,J} <: ArrowVector{J}

An arrow formatted dictionary encoding of a vector.  This is essentially an arrow primitive or list
of unique values with a primitive array of `Int32` specifying the values.  The structure is similar to
Julia `CategoricalArray`s.

## Constructors

    DictEncoding{P,J}(ptr, ref_loc::Integer, len::Integer, pool::P)
    DictEncoding(ptr, ref_loc::Integer, pool_loc::Integer, x::CategoricalArray)
    DictEncoding(ptr, ref_loc::Integer, pool_loc::Integer, pool_bitmask_loc::Integer,
                 x::CategoricalArray)

### Arguments
- `ptr` an array pointer or Arrow `Buffer` object
- `ref_loc` the location of the reference values using 1-based indexing
- `pool` an `ArrowVector` containing the underlying data to which the references refer
- `pool_loc` the location of the pool values using 1-based indexing
- `pool_bitmask_loc` the location of the pool null bit mask using 1-based indexing
- `x` a `CategoricalArray` that can be represented as a `DictEncoding`
"""
struct DictEncoding{P<:ArrowVector,J} <: ArrowVector{J}
    refs::Primitive{Int32}
    pool::P
end
export DictEncoding

function DictEncoding{P,J}(ptr::Ptr, ref_loc::Integer, len::Integer, pool::P) where {J,P<:ArrowVector{J}}
    DictEncoding{P,J}(Primitive{Int32}(ptr, ref_loc, len), pool)
end
function DictEncoding{P,J}(b::Buffer, ref_loc::Integer, len::Integer,
                           pool::P) where {J,P<:ArrowVector{J}}
    DictEncoding{P,J}(pointer(b.data), ref_loc, len, pool)
end


function DictEncoding(ptr::Union{Ptr,Buffer}, ref_loc::Integer, pool_loc::Integer,
                      x::CategoricalArray{J,1,U}) where {J,U}
    refs = Primitive(ptr, ref_loc, convert(Vector{Int32}, x.refs) .- 1)
    pool = Primitive(ptr, pool_loc, levels(x))
    DictEncoding{typeof(pool),J}(refs, pool)
end

function DictEncoding(ptr::Union{Ptr,Buffer}, ref_loc::Integer, pool_loc::Integer,
                      pool_bitmask_loc::Integer,
                      x::CategoricalArray{Union{J,Missing},1,U}) where {J,U}
    refs = Primitive(ptr, ref_loc, convert(Vector{Int32}, x.refs))
    pool = NullablePrimitive(ptr, pool_bitmask_loc, pool_loc, vcat(missing, levels(x)))
    DictEncoding{typeof(pool),J}(refs, pool)
end


length(d::DictEncoding) = length(d.refs)


# TODO mark bounds checking (also, these throw errors from d.refs)
# note that we define all of these methods to avoid method ambiguity
getindex(d::DictEncoding, i::Integer) = d.pool[d.refs[i]+1]
getindex(d::DictEncoding, i::AbstractVector{<:Integer}) = d.pool[d.refs[i].+1]
getindex(d::DictEncoding, i::AbstractVector{Bool}) = d.pool[d.refs[i].+1]

