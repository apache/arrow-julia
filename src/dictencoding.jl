
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
struct DictEncoding{J,P<:ArrowVector} <: ArrowVector{J}
    refs::Primitive{Int32}
    pool::P
end
export DictEncoding

function DictEncoding{J}(refs::Primitive{Int32}, pool::P) where {J,P<:ArrowVector{J}}
    DictEncoding{J,P}(refs, pool)
end
function DictEncoding(refs::Primitive{Int32}, pool::P) where {J,P<:ArrowVector{J}}
    DictEncoding{J}(refs, pool)
end

function DictEncoding{J}(data::Vector{UInt8}, refs_idx::Integer, len::Integer, pool::P
                        ) where {J,P<:ArrowVector{J}}
    refs = Primitive{Int32}(data, refs_idx, len)
    DictEncoding{J}(refs, pool)
end
function DictEncoding(data::Vector{UInt8}, refs_idx::Integer, len::Integer, pool::P
                     ) where {J,P<:ArrowVector{J}}
    DictEncoding{J}(data, refs_idx, len, pool)
end

# TODO this primitive constructor for pool should be more general
function DictEncoding(data::Vector{UInt8}, refs_idx::Integer, pool_idx::Integer,
                      x::CategoricalArray{J,1,U}) where {J,U}
    refs = Primitive{Int32}(data, refs_idx, getrefs(x))
    pool = Primitive{J}(data, pool_idx, getlevels(x))
    DictEncoding{J}(refs, pool)
end
function DictEncoding(data::Vector{UInt8}, refs_idx::Integer, pool_bmask_idx::Integer,
                      pool_vals_idx::Integer,
                      x::CategoricalArray{Union{J,Missing},1,U}) where {J,U}
    refs = Primitive{Int32}(data, refs_idx, getrefs(x))
    pool = NullablePrimitive(data, pool_bmask_idx, pool_vals_idx, getlevels(x))
    DictEncoding{J}(refs, pools)
end

function DictEncoding(data::Vector{UInt8}, i::Integer, x::CategoricalArray{J,1,U};
                      padding::Function=identity) where {J,U}
    refs = Primitive{Int32}(data, i, getrefs(x))
    pool = Primitive{J}(data, i+padding(refsbytes(x)), getlevels(x))
    DictEncoding{J}(refs, pool)
end
function DictEncoding(data::Vector{UInt8}, i::Integer, x::CategoricalArray{Union{J,Missing},1,U};
                      padding::Function=identity) where {J,U}
    refs = Primitive{Int32}(data, i, getrefs(x))
    pool = NullablePrimitive{J}(data, i+padding(refsbytes(x)), getlevels(x), padding=padding)
    DictEncoding{J}(refs, pool)
end

function DictEncoding(::Type{<:Array}, x::CategoricalArray; padding::Function=identity)
    b = Vector{UInt8}(minbytes(x))
    DictEncoding(b, 1, x, padding=padding)
end

function DictEncoding(x::CategoricalArray{J,1,U}) where {J,U}
    refs = Primitive{Int32}(getrefs(x))
    pool = Primitive{J}(getlevels(x))
    DictEncoding{J}(refs, pool)
end
function DictEncoding(x::CategoricalArray{Union{J,Missing},1,U}) where {J,U}
    refs = Primitive{Int32}(getrefs(x))
    pool = NullablePrimitive{J}(getlevels(x))
    DictEncoding{J}(refs, pool)
end


length(d::DictEncoding) = length(d.refs)


# TODO mark bounds checking (also, these throw errors from d.refs)
# note that we define all of these methods to avoid method ambiguity
getindex(d::DictEncoding, i::Integer) = d.pool[d.refs[i]+1]
getindex(d::DictEncoding, i::AbstractVector{<:Integer}) = d.pool[d.refs[i].+1]
getindex(d::DictEncoding, i::AbstractVector{Bool}) = d.pool[d.refs[i].+1]


#====================================================================================================
    utilities specific to DictEncoding
====================================================================================================#
getrefs(x::CategoricalArray) = convert(Vector{Int32}, x.refs) .- 1
getrefs(x::CategoricalArray{Union{J,Missing},1,U}) where {J,U} = convert(Vector{Int32}, x.refs)

getlevels(x::CategoricalArray) = levels(x)
getlevels(x::CategoricalArray{Union{J,Missing},1,U}) where {J,U} = vcat(missing, levels(x))

minbytes(x::CategoricalArray) = refsbytes(x) + minbytes(levels(x))
function minbytes(x::CategoricalArray{Union{J,Missing},1,U}) where {J,U}
    refsbytes(x) + minbytes(Union{J,Missing}, levels(x))
end

refsbytes(x::AbstractVector) = sizeof(Int32)*length(x)
