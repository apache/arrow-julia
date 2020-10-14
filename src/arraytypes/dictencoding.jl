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

mutable struct DictEncoding{T, A} <: ArrowVector{T}
    id::Int64
    data::A
    isOrdered::Bool
end

Base.size(d::DictEncoding) = size(d.data)

@propagate_inbounds function Base.getindex(d::DictEncoding{T}, i::Integer) where {T}
    @boundscheck checkbounds(d, i)
    return @inbounds ArrowTypes.arrowconvert(T, d.data[i])
end

# convenience wrapper to signal that an input column should be
# dict encoded when written to the arrow format
struct DictEncodeType{T} end
getT(::Type{DictEncodeType{T}}) where {T} = T

struct DictEncode{T, A} <: AbstractVector{DictEncodeType{T}}
    data::A
end

DictEncode(x::A) where {A} = DictEncode{eltype(A), A}(x)
Base.IndexStyle(::Type{<:DictEncode}) = Base.IndexLinear()
Base.size(x::DictEncode) = (length(x.data),)
Base.iterate(x::DictEncode, st...) = iterate(x.data, st...)
Base.getindex(x::DictEncode, i::Int) = getindex(x.data, i)
ArrowTypes.ArrowType(::Type{<:DictEncodeType}) = DictEncodedType()

struct DictEncoded{T, S, A} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    indices::Vector{S}
    encoding::DictEncoding{T, A}
    metadata::Union{Nothing, Dict{String, String}}
end

DictEncoded(b::Vector{UInt8}, v::ValidityBitmap, inds::Vector{S}, encoding::DictEncoding{T, A}, meta) where {S, T, A} =
    DictEncoded{T, S, A}(b, v, inds, encoding, meta)

Base.size(d::DictEncoded) = size(d.indices)

isdictencoded(d::DictEncoded) = true
isdictencoded(x) = false
isdictencoded(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = true

signedtype(::Type{UInt8}) = Int8
signedtype(::Type{UInt16}) = Int16
signedtype(::Type{UInt32}) = Int32
signedtype(::Type{UInt64}) = Int64

indtype(d::D) where {D <: DictEncoded} = indtype(D)
indtype(::Type{DictEncoded{T, S, A}}) where {T, S, A} = signedtype(S)
indtype(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = indtype(A)

getid(d::DictEncoded) = d.encoding.id
getid(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = c.data.encoding.id

function arrowvector(::DictEncodedType, x, de, meta; dictencode::Bool=false, dictencodenested::Bool=false, kw...)
    @assert x isa DictEncode
    x = x.data
    len = length(x)
    validity = ValidityBitmap(x)
    if x isa AbstractArray && DataAPI.refarray(x) !== x
        inds = copy(DataAPI.refarray(x))
        # adjust to "offset" instead of index
        for i = 1:length(inds)
            @inbounds inds[i] -= 1
        end
        pool = DataAPI.refpool(x)
        data = arrowvector(pool, de, nothing; dictencode=dictencodenested, dictencodenested=dictencodenested, dictencoding=true, kw...)
        encoding = DictEncoding{eltype(data), typeof(data)}(0, data, false)
    else
        # need to encode ourselves
        y = PooledArray(x)
        inds = DataAPI.refarray(y)
        # adjust to "offset" instead of index
        for i = 1:length(inds)
            @inbounds inds[i] = inds[i] - 1
        end
        data = arrowvector(DataAPI.refpool(y), de, nothing; dictencode=dictencodenested, dictencodenested=dictencodenested, dictencoding=true, kw...)
        encoding = DictEncoding{eltype(data), typeof(data)}(0, data, false)
    end
    push!(de, encoding)
    return DictEncoded(UInt8[], validity, inds, encoding, data.metadata)
end

@propagate_inbounds function Base.getindex(d::DictEncoded, i::Integer)
    @boundscheck checkbounds(d, i)
    @inbounds valid = d.validity[i]
    !valid && return missing
    @inbounds idx = d.indices[i]
    return @inbounds d.encoding[idx + 1]
end

@propagate_inbounds function Base.setindex!(d::DictEncoded{T}, v, i::Integer) where {T}
    @boundscheck checkbounds(d, i)
    if v === missing
        @inbounds d.validity[i] = false
    else
        ix = findfirst(d.encoding.data, v)
        if ix === nothing
            push!(d.encoding.data, v)
            @inbounds d.indices[i] = length(d.encoding.data) - 1
        else
            @inbounds d.indices[i] = ix - 1
        end
    end
    return v
end

function Base.copy(x::DictEncoded{T, S}) where {T, S}
    pool = copy(x.encoding.data)
    valid = x.validity
    inds = x.indices
    if T >: Missing
        refs = Vector{S}(undef, length(inds))
        @inbounds for i = 1:length(inds)
            refs[i] = ifelse(valid[i], inds[i] + one(S), missing)
        end
    else
        refs = copy(inds)
        @inbounds for i = 1:length(inds)
            refs[i] = refs[i] + one(S)
        end
    end
    return PooledArray(PooledArrays.RefArray(refs), Dict{T, S}(val => i for (i, val) in enumerate(pool)), pool)
end

function compress(Z::Meta.CompressionType, comp, x::A) where {A <: DictEncoded}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    inds = compress(Z, comp, x.indices)
    return Compressed{Z, A}(x, [validity, inds], len, nc, Compressed[])
end

function makenodesbuffers!(col::DictEncoded{T, S}, fieldnodes, fieldbuffers, bufferoffset, alignment) where {T, S}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += blen
    # indices
    blen = sizeof(S) * len
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += padding(blen, alignment)
    return bufferoffset
end

function writebuffer(io, col::DictEncoded, alignment)
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col, alignment)
    # write indices
    n = writearray(io, col.indices)
    @debug 1 "writing array: col = $(typeof(col.indices)), n = $n, padded = $(padding(n, alignment))"
    writezeros(io, paddinglength(n, alignment))
    return
end
