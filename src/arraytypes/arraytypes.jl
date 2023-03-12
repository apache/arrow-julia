# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    Arrow.ArrowVector

An abstract type that subtypes `AbstractVector`. Each specific arrow array type
subtypes `ArrowVector`. See [`BoolVector`](@ref), [`Primitive`](@ref), [`List`](@ref),
[`Map`](@ref), [`FixedSizeList`](@ref), [`Struct`](@ref), [`DenseUnion`](@ref),
[`SparseUnion`](@ref), and [`DictEncoded`](@ref) for more details.
"""
abstract type ArrowVector{T} <: AbstractVector{T} end

Base.IndexStyle(::Type{A}) where {A <: ArrowVector} = Base.IndexLinear()
Base.similar(::Type{A}, dims::Dims) where {T, A <: ArrowVector{T}} = Vector{T}(undef, dims)
validitybitmap(x::ArrowVector) = x.validity
nullcount(x::ArrowVector) = validitybitmap(x).nc
getmetadata(x::ArrowVector) = x.metadata
Base.deleteat!(x::T, inds) where {T <: ArrowVector} = throw(ArgumentError("`$T` does not support `deleteat!`; arrow data is by nature immutable"))

function toarrowvector(x, i=1, de=Dict{Int64, Any}(), ded=DictEncoding[], meta=getmetadata(x); compression::Union{Nothing, Vector{LZ4FrameCompressor}, LZ4FrameCompressor, Vector{ZstdCompressor}, ZstdCompressor}=nothing, kw...)
    @debugv 2 "converting top-level column to arrow format: col = $(typeof(x)), compression = $compression, kw = $(kw.data)"
    @debugv 3 x
    A = arrowvector(x, i, 0, 0, de, ded, meta; compression=compression, kw...)
    tid=Threads.threadid()
    if compression isa LZ4FrameCompressor
        A = compress(Meta.CompressionTypes.LZ4_FRAME, compression, A)
    elseif compression isa Vector{LZ4FrameCompressor}
        A = lock(LZ4_FRAME_COMPRESSOR_LOCK[tid]) do
            compress(Meta.CompressionTypes.LZ4_FRAME, compression[tid], A)
        end
    elseif compression isa ZstdCompressor
        A = compress(Meta.CompressionTypes.ZSTD, compression, A)
    elseif compression isa Vector{ZstdCompressor}
        A = lock(ZSTD_COMPRESSOR_LOCK[tid]) do
            compress(Meta.CompressionTypes.ZSTD, compression[tid], A)
        end
    end
    @debugv 2 "converted top-level column to arrow format: $(typeof(A))"
    @debugv 3 A
    return A
end

function arrowvector(x, i, nl, fi, de, ded, meta; dictencoding::Bool=false, dictencode::Bool=false, maxdepth::Int=DEFAULT_MAX_DEPTH, kw...)
    if nl > maxdepth
        error("reached nested serialization level ($nl) deeper than provided max depth argument ($(maxdepth)); to increase allowed nesting level, pass `maxdepth=X`")
    end
    T = maybemissing(eltype(x))
    if !(x isa DictEncode) && !dictencoding && (dictencode || DataAPI.refarray(x) !== x)
        x = DictEncode(x, dictencodeid(i, nl, fi))
    elseif x isa DictEncoded
        return arrowvector(DictEncodeType, x, i, nl, fi, de, ded, meta; dictencode=dictencode, kw...)
    elseif !(x isa DictEncode)
        x = ToArrow(x)
    end
    S = maybemissing(eltype(x))
    if ArrowTypes.hasarrowname(T)
        meta = _arrowtypemeta(_normalizemeta(meta), String(ArrowTypes.arrowname(T)), String(ArrowTypes.arrowmetadata(T)))
    end
    return arrowvector(S, x, i, nl, fi, de, ded, meta; dictencode=dictencode, maxdepth=maxdepth, kw...)
end

_normalizemeta(::Nothing) = nothing
_normalizemeta(meta) = toidict(String(k) => String(v) for (k, v) in meta)

_normalizecolmeta(::Nothing) = nothing
_normalizecolmeta(colmeta) = toidict(Symbol(k) => toidict(String(v1) => String(v2) for (v1, v2) in v) for (k, v) in colmeta)

function _arrowtypemeta(::Nothing, n, m)
    return toidict(("ARROW:extension:name" => n, "ARROW:extension:metadata" => m))
end

function _arrowtypemeta(meta, n, m)
    dict = Dict(meta)
    dict["ARROW:extension:name"] = n
    dict["ARROW:extension:metadata"] = m
    return toidict(dict)
end

# now we check for ArrowType converions and dispatch on ArrowKind
function arrowvector(::Type{S}, x, i, nl, fi, de, ded, meta; kw...) where {S}
    meta = _normalizemeta(meta)
    return arrowvector(ArrowKind(S), x, i, nl, fi, de, ded, meta; kw...)
end

struct NullVector{T} <: ArrowVector{T}
    data::MissingVector
    metadata::Union{Nothing, Base.ImmutableDict{String, String}}
end
Base.size(v::NullVector) = (length(v.data),)
Base.getindex(v::NullVector{T}, i::Int) where {T} = ArrowTypes.fromarrow(T, getindex(v.data, i))

arrowvector(::NullKind, x, i, nl, fi, de, ded, meta; kw...) = NullVector{eltype(x)}(MissingVector(length(x)), isnothing(meta) ? nothing : toidict(meta))
compress(Z::Meta.CompressionType, comp, v::NullVector) =
    Compressed{Z, NullVector}(v, CompressedBuffer[], length(v), length(v), Compressed[])

function makenodesbuffers!(col::NullVector, fieldnodes, fieldbuffers, bufferoffset, alignment)
    push!(fieldnodes, FieldNode(length(col), length(col)))
    @debugv 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    return bufferoffset
end

function writebuffer(io, col::NullVector, alignment)
    return
end

"""
    Arrow.ValidityBitmap

A bit-packed array type where each bit corresponds to an element in an
[`ArrowVector`](@ref), indicating whether that element is "valid" (bit == 1),
or not (bit == 0). Used to indicate element missingness (whether it's null).

If the null count of an array is zero, the `ValidityBitmap` will be "empty"
and all elements are treated as "valid"/non-null.
"""
struct ValidityBitmap <: ArrowVector{Bool}
    bytes::Vector{UInt8} # arrow memory blob
    pos::Int # starting byte of validity bitmap
    ℓ::Int # # of _elements_ (not bytes!) in bitmap (because bitpacking)
    nc::Int # null count
end

Base.size(p::ValidityBitmap) = (p.ℓ,)
nullcount(x::ValidityBitmap) = x.nc

function ValidityBitmap(x)
    T = eltype(x)
    if !(T >: Missing)
        return ValidityBitmap(UInt8[], 1, length(x), 0)
    end
    len = length(x)
    blen = cld(len, 8)
    bytes = Vector{UInt8}(undef, blen)
    st = iterate(x)
    nc = 0
    b = 0xff
    j = k = 1
    for y in x
        if y === missing
            nc += 1
            b = setbit(b, false, j)
        end
        j += 1
        if j == 9
            @inbounds bytes[k] = b
            b = 0xff
            j = 1
            k += 1
        end
    end
    if j > 1
        bytes[k] = b
    end
    return ValidityBitmap(nc == 0 ? UInt8[] : bytes, 1, nc == 0 ? 0 : len, nc)
end

@propagate_inbounds function Base.getindex(p::ValidityBitmap, i::Integer)
    # no boundscheck because parent array should do it
    # if a validity bitmap is empty, it either means:
    #   1) the parent array null_count is 0, so all elements are valid
    #   2) parent array is also empty, so "all" elements are valid
    p.nc == 0 && return true
    # translate element index to bitpacked byte index
    a, b = divrem(i-1, 8) .+ (1,1)
    @inbounds byte = p.bytes[p.pos + a - 1]
    # check individual bit of byte
    return getbit(byte, b)
end

@propagate_inbounds function Base.setindex!(p::ValidityBitmap, v, i::Integer)
    x = convert(Bool, v)
    p.ℓ == 0 && !x && throw(BoundsError(p, i))
    a, b = fldmod1(i, 8)
    @inbounds byte = p.bytes[p.pos + a - 1]
    @inbounds p.bytes[p.pos + a - 1] = setbit(byte, x, b)
    return v
end

function writebitmap(io, col::ArrowVector, alignment)
    v = col.validity
    @debugv 1 "writing validity bitmap: nc = $(v.nc), n = $(cld(v.ℓ, 8))"
    v.nc == 0 && return 0
    n = Base.write(io, view(v.bytes, v.pos:(v.pos + cld(v.ℓ, 8) - 1)))
    return n + writezeros(io, paddinglength(n, alignment))
end

include("compressed.jl")
include("primitive.jl")
include("bool.jl")
include("list.jl")
include("fixedsizelist.jl")
include("map.jl")
include("struct.jl")
include("unions.jl")
include("dictencoding.jl")
