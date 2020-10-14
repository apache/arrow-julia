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

abstract type ArrowVector{T} <: AbstractVector{T} end

Base.IndexStyle(::Type{A}) where {A <: ArrowVector} = Base.IndexLinear()
Base.similar(::Type{A}, dims::Dims) where {T, A <: ArrowVector{T}} = Vector{T}(undef, dims)
validitybitmap(x::ArrowVector) = x.validity
nullcount(x::ArrowVector) = validitybitmap(x).nc
getmetadata(x::ArrowVector) = x.metadata

function toarrowvector(x, de=DictEncoding[], meta=getmetadata(x); compression::Union{Nothing, LZ4FrameCompressor, ZstdCompressor}=nothing, kw...)
    @debug 2 "converting top-level column to arrow format: col = $(typeof(x)), compression = $compression, kw = $(kw.data)"
    @debug 3 x
    A = arrowvector(x, de, meta; compression=compression, kw...)
    if compression isa LZ4FrameCompressor
        A = compress(Meta.CompressionType.LZ4_FRAME, compression, A)
    elseif compression isa ZstdCompressor
        A = compress(Meta.CompressionType.ZSTD, compression, A)
    end
    @debug 2 "converted top-level column to arrow format: $(typeof(A))"
    @debug 3 A
    return A
end

function arrowvector(x, de, meta; dictencoding::Bool=false, dictencode::Bool=false, kw...)
    if !(x isa DictEncode) && !dictencoding && (dictencode || (x isa AbstractArray && DataAPI.refarray(x) !== x))
        x = DictEncode(x)
    end
    T = eltype(x)
    S = maybemissing(T)
    return arrowvector(S, T, x, de, meta; kw...)
end

# conversions to arrow types
arrowvector(::Type{Dates.Date}, ::Type{S}, x, de, meta; kw...) where {S} =
    arrowvector(converter(DATE, x), de, meta; kw...)
arrowvector(::Type{Dates.Time}, ::Type{S}, x, de, meta; kw...) where {S} =
    arrowvector(converter(TIME, x), de, meta; kw...)
arrowvector(::Type{Dates.DateTime}, ::Type{S}, x, de, meta; kw...) where {S} =
    arrowvector(converter(DATETIME, x), de, meta; kw...)
arrowvector(::Type{P}, ::Type{S}, x, de, meta; kw...) where {P <: Dates.Period, S} =
    arrowvector(converter(Duration{arrowperiodtype(P)}, x), de, meta; kw...)

# fallback that calls ArrowType
function arrowvector(::Type{S}, ::Type{T}, x, de, meta; kw...) where {S, T}
    if ArrowTypes.istyperegistered(S)
        meta = meta === nothing ? Dict{String, String}() : meta
        arrowtype = ArrowTypes.getarrowtype!(meta, S)
        return arrowvector(converter(arrowtype, x), de, meta; kw...)
    end
    return arrowvector(ArrowType(S), x, de, meta; kw...)
end

arrowvector(::NullType, x, de, meta; kw...) = MissingVector(length(x))
compress(Z::Meta.CompressionType, comp, v::MissingVector) =
    Compressed{Z, MissingVector}(v, CompressedBuffer[], length(v), length(v), Compressed[])

function makenodesbuffers!(col::MissingVector, fieldnodes, fieldbuffers, bufferoffset, alignment)
    push!(fieldnodes, FieldNode(length(col), length(col)))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    return bufferoffset
end

function writebuffer(io, col::MissingVector, alignment)
    return
end

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
    i = 0
    nc = 0
    for k = 1:blen
        b = 0x00
        for j = 1:8
            if (i + j) <= len
                y, state = st
                if y === missing
                    nc += 1
                    b = setbit(b, false, j)
                else
                    b = setbit(b, true, j)
                end
                st = iterate(x, state)
            end
        end
        i += 8
        @inbounds bytes[k] = b
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
    a, b = fldmod1(i, 8)
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
    @debug 1 "writing validity bitmap: nc = $(v.nc), n = $(cld(v.ℓ, 8))"
    v.nc == 0 && return 0
    n = Base.write(io, view(v.bytes, v.pos:(v.pos + cld(v.ℓ, 8) - 1)))
    return n + writezeros(io, paddinglength(n, alignment))
end

include("compressed.jl")
include("primitive.jl")
include("list.jl")
include("fixedsizelist.jl")
include("map.jl")
include("struct.jl")
include("unions.jl")
include("dictencoding.jl")
