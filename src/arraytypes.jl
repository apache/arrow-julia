abstract type ArrowVector{T} <: AbstractVector{T} end

Base.IndexStyle(::Type{A}) where {A <: ArrowVector} = Base.IndexLinear()

struct ValidityBitmap <: ArrowVector{Bool}
    bytes::Vector{UInt8} # arrow memory blob
    pos::Int # starting byte of validity bitmap
    ℓ::Int # # of _elements_ (not bytes!) in bitmap (because bitpacking)
end

Base.size(p::ValidityBitmap) = (p.ℓ,)

@propagate_inbounds function Base.getindex(p::ValidityBitmap, i::Integer)
    # no boundscheck because parent array should do it
    # if a validity bitmap is empty, it either means:
    #   1) the parent array null_count is 0, so all elements are valid
    #   2) parent array is also empty, so "all" elements are valid
    p.ℓ == 0 && return true
    # translate element index to bitpacked byte index
    a, b = fldmod1(i, 8)
    @inbounds byte = p.bytes[p.pos + a - 1]
    # check individual bit of byte
    return getbit(byte, b)
end

@propagate_inbounds function Base.setindex!(p::ValidityBitmap, v, i::Integer)
    x = convert(Bool, v)
    p.ℓ == 0 && !x && throw(ArgumentError(o0))
    a, b = fldmod1(i, 8)
    @inbounds byte = p.bytes[p.pos + a - 1]
    @inbounds p.bytes[p.pos + a - 1] = setbit(byte, convert(Bool, v), b)
    v
end

struct Primitive{T, S} <: ArrowVector{T}
    validity::ValidityBitmap
    data::Vector{S}
    ℓ::Int
end

Base.size(p::Primitive) = (p.ℓ,)

@propagate_inbounds function Base.getindex(p::Primitive{T, S}, i::Integer) where {T, S}
    @boundscheck checkbounds(p, i)
    if T !== S
        return p.validity[i] ? p.data[i] : missing
    else
        return p.data[i]
    end
end

struct Offsets{T <: Union{Int32, Int64}} <: ArrowVector{Tuple{T, T}}
    offsets::Vector{T}
end

Base.size(o::Offsets) = (length(o.offsets) - 1,)

@propagate_inbounds function Base.getindex(o::Offsets, i::Integer)
    @boundscheck checkbounds(o, i)
    @inbounds lo = o.offsets[i] + 1
    @inbounds hi = o.offsets[i + 1]
    return lo, hi
end

struct List{T, O, A <: AbstractVector} <: ArrowVector{T}
    validity::ValidityBitmap
    offsets::Offsets{O}
    data::A
    ℓ::Int
end

Base.size(l::List) = (l.ℓ,)

@propagate_inbounds function Base.getindex(l::List{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    @inbounds lo, hi = l.offsets[i]
    if T === Union{String, Missing}
        return l.validity[i] ? unsafe_string(pointer(l.data, lo), hi - lo + 1) : missing
    elseif T === String
        return unsafe_string(pointer(l.data, lo), hi - lo + 1)
    elseif Base.nonmissingtype(T) !== T
        return l.validity[i] ? l.data[lo:hi] : missing
    else
        return l.data[lo:hi]
    end
end

struct FixedSizeList{T, A <: AbstractVector} <: ArrowVector{T}
    validity::ValidityBitmap
    data::A
    ℓ::Int
end

Base.size(l::FixedSizeList) = (l.ℓ,)
getn(::Type{NTuple{N, T}}) where {N, T} = N

@propagate_inbounds function Base.getindex(l::FixedSizeList{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    N = getn(Base.nonmissingtype(T))
    off = (i - 1) * N
    if Base.nonmissingtype(T) !== T
        return l.validity[i] ? ntuple(j->l.data[off + j], N) : missing
    else
        return ntuple(j->l.data[off + j], N)
    end
end

struct Map{K, V, A, B} <: ArrowVector{Pair{K, V}}
    keys::A
    values::B
end

Base.size(m::Map) = size(m.keys)
Base.keytype(::Type{Map{K, V, A, B}}) where {K, V, A, B} = K
Base.valtype(::Type{Map{K, V, A, B}}) where {K, V, A, B} = V

_keytype(::Type{T}) where {T} = keytype(T)
_valtype(::Type{T}) where {T} = valtype(T)
_keytype(::Pair{K, V}) where {K, V} = K
_valtype(::Pair{K, V}) where {K, V} = V

@propagate_inbounds function Base.getindex(m::Map, i::Integer)
    @boundscheck checkbounds(m, i)
    return m.keys[i] => m.values[i]
end

struct Struct{T, S} <: ArrowVector{T}
    validity::ValidityBitmap
    data::S # Tuple of ArrowVector
    ℓ::Int
end

Base.size(s::Struct) = (s.ℓ,)
getn(::Type{NamedTuple{names, T}}) where {names, T} = length(names)

@propagate_inbounds function Base.getindex(s::Struct{T}, i::Integer) where {T}
    @boundscheck checkbounds(s, i)
    NT = Base.nonmissingtype(T)
    if NT !== T
        return s.validity[i] ? NT(ntuple(j->s.data[j][i], getn(NT))) : missing
    else
        return NT(ntuple(j->s.data[j][i], getn(NT)))
    end
end

struct DenseUnion{T, S} <: ArrowVector{T}
    typeIds::Vector{UInt8}
    offsets::Vector{Int32}
    data::S # Tuple of ArrowVector
end

Base.size(s::DenseUnion) = size(s.typeIds)

@propagate_inbounds function Base.getindex(s::DenseUnion{T}, i::Integer) where {T}
    @boundscheck checkbounds(s, i)
    @inbounds typeId = s.typeIds[i]
    @inbounds off = s.offsets[i]
    @inbounds x = s.data[typeId + 1][off + 1]
    return x
end

struct SparseUnion{T, S} <: ArrowVector{T}
    typeIds::Vector{UInt8}
    data::S # Tuple of ArrowVector
end

Base.size(s::SparseUnion) = size(s.typeIds)

@propagate_inbounds function Base.getindex(s::SparseUnion{T}, i::Integer) where {T}
    @boundscheck checkbounds(s, i)
    @inbounds typeId = s.typeIds[i]
    @inbounds x = s.data[typeId + 1][i]
    return x
end

struct DictEncoding{T, A} <: ArrowVector{T}
    id::Int64
    data::ChainedVector{T, A}
    isOrdered::Bool
end

Base.size(d::DictEncoding) = size(d.data)

@propagate_inbounds function Base.getindex(d::DictEncoding, i::Integer)
    @boundscheck checkbounds(d, i)
    @inbounds x = d.data[i]
    return x
end

struct DictEncoded{T, S} <: ArrowVector{T}
    validity::ValidityBitmap
    indices::Vector{S}
    encoding::DictEncoding
end

Base.size(d::DictEncoded) = size(d.indices)

@propagate_inbounds function Base.getindex(d::DictEncoded{T}, i::Integer) where {T}
    @boundscheck checkbounds(d, i)
    @inbounds valid = d.validity[i]
    !valid && return missing
    @inbounds idx = d.indices[i]
    return d.encoding[idx + 1]
end
