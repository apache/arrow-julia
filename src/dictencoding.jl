
"""
    DictEncoding{P<:ArrowVector,J} <: ArrowVector{J}

An arrow formatted dictionary encoding of a vector.  This is essentially an arrow primitive or list
of unique values with a primitive array of `Int32` specifying the values.  The structure is similar to
Julia `CategoricalArray`s.

## Constructors
    DictEncoding(refs::Primitive{Int32}, pool::ArrowVector)
    DictEncoding(data::Vector{UInt8}, refs_idx::Integer, len::Integer, pool::ArrowVector)
    DictEncoding(data::Vector{UInt8}, refs_idx::Integer, pool_idx::Integer, x::CategoricalArray)
    DictEncoding(data::Vector{UInt8}, refs_idx::Integer, pool_bmask_idx::Integer, pool_vals_idx::Integer,
                 x::CategoricalArray)
    DictEncoding(data::Vector{UInt8}, i::Integer, x::CategoricalArray)
    DictEncoding(Array, x::CategoricalArray)
    DictEncoding(x::CategoricalArray)

If `Array` is passed a contiguous array will be allocated.

### Arguments
- `refs`: a `Primitive` providing references to values in `pool`
- `data`: a buffer for storing the data
- `refs_idx`: the location in `data` of the references
- `pool_idx`: the location in `data` of the value pool
- `x`: a `CategoricalArray` to be stored as a `DictEncoding`
"""
struct DictEncoding{J,P<:ArrowVector} <: ArrowVector{J}
    refs::Primitive{Int32}
    pool::P
end
export DictEncoding

function DictEncoding{J}(refs::Primitive{Int32}, pool::P) where {J,P<:ArrowVector}
    DictEncoding{J,P}(refs, pool)
end

function DictEncoding{J}(data::Vector{UInt8}, refs_idx::Integer, len::Integer, pool::P
                        ) where {J,P<:ArrowVector}
    refs = Primitive{Int32}(data, refs_idx, len)
    DictEncoding{J}(refs, pool)
end

function DictEncoding(data::Vector{UInt8}, refs_idx::Integer, pool_idx::Integer,
                      x::CategoricalArray{T,1,U}) where {J,U,T<:Union{J,Union{J,Missing}}}
    refs = Primitive{Int32}(data, refs_idx, getrefs(x))
    pool = Primitive{J}(data, pool_idx, getlevels(x))
    DictEncoding{T}(refs, pool)
end

function DictEncoding(data::Vector{UInt8}, i::Integer, x::CategoricalArray{T,1,U}
                     ) where {J,U,T<:Union{J,Union{J,Missing}}}
    refs = Primitive{Int32}(data, i, getrefs(x))
    pool = Primitive{J}(data, i+refsbytes(x), getlevels(x))
    DictEncoding{T}(refs, pool)
end

function DictEncoding(::Type{<:Array}, x::CategoricalArray)
    b = Vector{UInt8}(totalbytes(x))
    DictEncoding(b, 1, x)
end

function DictEncoding(x::CategoricalArray{J,1,U}) where {J,U}
    refs = Primitive{Int32}(getrefs(x))
    pool = arrowformat(getlevels(x))
    DictEncoding{J}(refs, pool)
end

DictEncoding(v::AbstractVector) = DictEncoding(CategoricalArray(v))


DictEncoding{J}(d::DictEncoding{J}) where J = DictEncoding{J}(d.refs, d.pools)
DictEncoding{J}(d::DictEncoding{T}) where {J,T} = DictEncoding{J}(convert(AbstractVector{J}, d[:]))
DictEncoding(d::DictEncoding{J}) where J = DictEncoding{J}(d)


length(d::DictEncoding) = length(d.refs)

references(d::DictEncoding) = d.refs
levels(d::DictEncoding) = d.pool
export references, levels


# TODO mark bounds checking (also, these throw errors from d.refs)
# note that we define all of these methods to avoid method ambiguity
getindex(d::DictEncoding, i::Integer) = d.pool[d.refs[i]+1]
getindex(d::DictEncoding, i::AbstractVector{<:Integer}) = d.pool[d.refs[i].+1]
getindex(d::DictEncoding, i::AbstractVector{Bool}) = d.pool[d.refs[i].+1]


function getindex(d::DictEncoding{Union{J,Missing}}, i::Integer)::Union{J,Missing} where J
    r = d.refs[i] + 1
    r == 0 ? missing : d.pool[r]
end
function getindex(d::DictEncoding{Union{J,Missing}}, idx::AbstractVector{<:Integer}) where J
    Union{J,Missing}[getindex(d, i) for i ∈ idx]
end
function getindex(d::DictEncoding{Union{J,Missing}}, idx::AbstractVector{Bool}) where J
    Union{J,Missing}[getindex(d, i) for i ∈ 1:length(d) if idx[i]]
end


nullcount(d::DictEncoding{Union{J,Missing}}) where J = sum(d.refs .< 0)


#====================================================================================================
    utilities specific to DictEncoding
====================================================================================================#
getrefs(x::CategoricalArray) = convert(Vector{Int32}, x.refs) .- Int32(1)

getlevels(x::CategoricalArray) = x.pool.index

refsbytes(len::Integer) = padding(sizeof(Int32)*len)
refsbytes(x::AbstractVector) = refsbytes(length(x))

totalbytes(x::CategoricalArray) = refsbytes(x) + totalbytes(levels(x))
function totalbytes(x::CategoricalArray{Union{J,Missing},1,U}) where {J,U}
    refsbytes(x) + totalbytes(Union{J,Missing}, levels(x))
end

