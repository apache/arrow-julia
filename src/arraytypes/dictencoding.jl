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
    Arrow.DictEncoding

Represents the "pool" of possible values for a [`DictEncoded`](@ref)
array type. Whether the order of values is significant can be checked
by looking at the `isOrdered` boolean field.

The `S` type parameter, while not tied directly to any field, is the
signed integer "index type" of the parent DictEncoded. We keep track
of this in the DictEncoding in order to validate the length of the pool
doesn't exceed the index type limit. The general workflow of writing arrow
data means the initial schema will typically be based off the data in the
first record batch, and subsequent record batches need to match the same
schema exactly. For example, if a non-first record batch dict encoded column
were to cause a DictEncoding pool to overflow on unique values, a fatal error
should be thrown.
"""
mutable struct DictEncoding{T, S, A} <: ArrowVector{T}
    id::Int64
    data::A
    isOrdered::Bool
    metadata::Union{Nothing, Base.ImmutableDict{String, String}}
end

indextype(::Type{DictEncoding{T, S, A}}) where {T, S, A} = S
indextype(::T) where {T <: DictEncoding} = indextype(T)

Base.size(d::DictEncoding) = size(d.data)

@propagate_inbounds function Base.getindex(d::DictEncoding{T}, i::Integer) where {T}
    @boundscheck checkbounds(d, i)
    return @inbounds ArrowTypes.fromarrow(T, d.data[i])
end

# convenience wrapper to signal that an input column should be
# dict encoded when written to the arrow format
struct DictEncodeType{T} end
getT(::Type{DictEncodeType{T}}) where {T} = T

"""
    Arrow.DictEncode(::AbstractVector, id::Integer=nothing)

Signals that a column/array should be dictionary encoded when serialized
to the arrow streaming/file format. An optional `id` number may be provided
to signal that multiple columns should use the same pool when being
dictionary encoded.
"""
struct DictEncode{T, A} <: AbstractVector{DictEncodeType{T}}
    id::Int64
    data::A
end

DictEncode(x::A, id=-1) where {A} = DictEncode{eltype(A), A}(id, x)
Base.IndexStyle(::Type{<:DictEncode}) = Base.IndexLinear()
Base.size(x::DictEncode) = (length(x.data),)
Base.iterate(x::DictEncode, st...) = iterate(x.data, st...)
Base.getindex(x::DictEncode, i::Int) = getindex(x.data, i)
ArrowTypes.ArrowKind(::Type{<:DictEncodeType}) = DictEncodedKind()
Base.copy(x::DictEncode) = DictEncode(x.data, x.id)

"""
    Arrow.DictEncoded

A dictionary encoded array type (similar to a `PooledArray`). Behaves just
like a normal array in most respects; internally, possible values are stored
in the `encoding::DictEncoding` field, while the `indices::Vector{<:Integer}`
field holds the "codes" of each element for indexing into the encoding pool.
Any column/array can be dict encoding when serializing to the arrow format
either by passing the `dictencode=true` keyword argument to [`Arrow.write`](@ref)
(which causes _all_ columns to be dict encoded), or wrapping individual columns/
arrays in [`Arrow.DictEncode(x)`](@ref).
"""
struct DictEncoded{T, S, A} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    indices::Vector{S}
    encoding::DictEncoding{T, S, A}
    metadata::Union{Nothing, Base.ImmutableDict{String, String}}
end

DictEncoded(b::Vector{UInt8}, v::ValidityBitmap, inds::Vector{S}, encoding::DictEncoding{T, S, A}, meta) where {S, T, A} =
    DictEncoded{T, S, A}(b, v, inds, encoding, meta)

Base.size(d::DictEncoded) = size(d.indices)

isdictencoded(d::DictEncoded) = true
isdictencoded(x) = false
isdictencoded(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = true

function signedtype(n::Integer)
    typs = (Int8, Int16, Int32, Int64)
    typs[something(findfirst(n .≤ typemax.(typs)), 4)]
end

signedtype(::Type{UInt8}) = Int8
signedtype(::Type{UInt16}) = Int16
signedtype(::Type{UInt32}) = Int32
signedtype(::Type{UInt64}) = Int64
signedtype(::Type{T}) where {T <: Signed} = T

indtype(d::DictEncoded{T, S, A}) where {T, S, A} = S
indtype(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = indtype(c.data)

dictencodeid(colidx, nestedlevel, fieldid) = (Int64(nestedlevel) << 48) | (Int64(fieldid) << 32) | Int64(colidx)

getid(d::DictEncoded) = d.encoding.id
getid(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = c.data.encoding.id

function arrowvector(::DictEncodedKind, x::DictEncoded, i, nl, fi, de, ded, meta; dictencode::Bool=false, dictencodenested::Bool=false, kw...)
    id = x.encoding.id
    if !haskey(de, id)
        de[id] = Lockable(x.encoding)
    else
        encodinglockable = de[id]
        Base.@lock encodinglockable begin
            encoding = encodinglockable.value
            # in this case, we just need to check if any values in our local pool need to be delta dicationary serialized
            deltas = setdiff(x.encoding, encoding)
            if !isempty(deltas)
                ET = indextype(encoding)
                if length(deltas) + length(encoding) > typemax(ET)
                    error("fatal error serializing dict encoded column with ref index type of $ET; subsequent record batch unique values resulted in $(length(deltas) + length(encoding)) unique values, which exceeds possible index values in $ET")
                end
                data = arrowvector(deltas, i, nl, fi, de, ded, nothing; dictencode=dictencodenested, dictencodenested=dictencodenested, dictencoding=true, kw...)
                push!(ded, DictEncoding{eltype(data), ET, typeof(data)}(id, data, false, getmetadata(data)))
                if typeof(encoding.data) <: ChainedVector
                    append!(encoding.data, data)
                else
                    data2 = ChainedVector([encoding.data, data])
                    encoding = DictEncoding{eltype(data2), ET, typeof(data2)}(id, data2, false, getmetadata(encoding))
                    de[id] = Lockable(encoding)
                end
            end
        end
    end
    return x
end

function arrowvector(::DictEncodedKind, x, i, nl, fi, de, ded, meta; dictencode::Bool=false, dictencodenested::Bool=false, kw...)
    @assert x isa DictEncode
    id = x.id == -1 ? dictencodeid(i, nl, fi) : x.id
    x = x.data
    len = length(x)
    validity = ValidityBitmap(x)
    if !haskey(de, id)
        # dict encoding doesn't exist yet, so create for 1st time
        if DataAPI.refarray(x) === x || DataAPI.refpool(x) === nothing
            # need to encode ourselves
            x = PooledArray(x; signed=true, compress=true)
            inds = DataAPI.refarray(x)
            pool = DataAPI.refpool(x)
        else
            pool = DataAPI.refpool(x)
            refa = DataAPI.refarray(x)
            inds = copyto!(similar(Vector{signedtype(length(pool))}, length(refa)), refa)
        end
        # horrible hack? yes. better than taking CategoricalArrays dependency? also yes.
        if typeof(pool).name.name == :CategoricalRefPool
            if eltype(x) >: Missing
                pool = vcat(missing, DataAPI.levels(x))
            else
                pool = DataAPI.levels(x)
                for i = 1:length(inds)
                    @inbounds inds[i] -= 1
                end
            end
        else
            # adjust to "offset" instead of index
            for i = 1:length(inds)
                @inbounds inds[i] -= 1
            end
        end
        data = arrowvector(pool, i, nl, fi, de, ded, nothing; dictencode=dictencodenested, dictencodenested=dictencodenested, dictencoding=true, kw...)
        encoding = DictEncoding{eltype(data), eltype(inds), typeof(data)}(id, data, false, getmetadata(data))
        de[id] = Lockable(encoding)
    else
        # encoding already exists
          # compute inds based on it
          # if value doesn't exist in encoding, push! it
          # also add to deltas updates
        encodinglockable = de[id]
        Base.@lock encodinglockable begin
            encoding = encodinglockable.value
            len = length(x)
            ET = indextype(encoding)
            pool = Dict{Union{eltype(encoding), eltype(x)}, ET}(a => (b - 1) for (b, a) in enumerate(encoding))
            deltas = eltype(x)[]
            inds = Vector{ET}(undef, len)
            categorical = typeof(x).name.name == :CategoricalArray
            for (j, val) in enumerate(x)
                if categorical
                    val = get(val)
                end
                @inbounds inds[j] = get!(pool, val) do
                    push!(deltas, val)
                    return length(pool)
                end
            end
            if !isempty(deltas)
                if length(deltas) + length(encoding) > typemax(ET)
                    error("fatal error serializing dict encoded column with ref index type of $ET; subsequent record batch unique values resulted in $(length(deltas) + length(encoding)) unique values, which exceeds possible index values in $ET")
                end
                data = arrowvector(deltas, i, nl, fi, de, ded, nothing; dictencode=dictencodenested, dictencodenested=dictencodenested, dictencoding=true, kw...)
                push!(ded, DictEncoding{eltype(data), ET, typeof(data)}(id, data, false, getmetadata(data)))
                if typeof(encoding.data) <: ChainedVector
                    append!(encoding.data, data)
                else
                    data2 = ChainedVector([encoding.data, data])
                    encoding = DictEncoding{eltype(data2), ET, typeof(data2)}(id, data2, false, getmetadata(encoding))
                    de[id] = Lockable(encoding)
                end
            end
        end
    end
    if meta !== nothing && getmetadata(encoding) !== nothing
        meta = toidict(merge!(Dict(meta), Dict(getmetadata(encoding))))
    elseif getmetadata(encoding) !== nothing
        meta = getmetadata(encoding)
    end
    return DictEncoded(UInt8[], validity, inds, encoding, meta)
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
    refs = copy(inds)
    @inbounds for i = 1:length(inds)
        refs[i] = refs[i] + one(S)
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

function DataAPI.levels(x::DictEncoded)
    rp = DataAPI.refpool(x)   # may contain missing values
    Missing <: eltype(rp) || return rp
    convert(AbstractArray{nonmissingtype(eltype(rp))}, deleteat!(rp, ismissing.(rp)))
end

function makenodesbuffers!(col::DictEncoded{T, S}, fieldnodes, fieldbuffers, bufferoffset, alignment) where {T, S}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debugv 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debugv 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += blen
    # indices
    blen = sizeof(S) * len
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debugv 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += padding(blen, alignment)
    return bufferoffset
end

DataAPI.refarray(x::DictEncoded{T, S}) where {T, S} = x.indices .+ one(S)

DataAPI.refpool(x::DictEncoded) = copy(x.encoding.data)

function writebuffer(io, col::DictEncoded, alignment)
    @debugv 1 "writebuffer: col = $(typeof(col))"
    @debugv 2 col
    writebitmap(io, col, alignment)
    # write indices
    n = writearray(io, col.indices)
    @debugv 1 "writing array: col = $(typeof(col.indices)), n = $n, padded = $(padding(n, alignment))"
    writezeros(io, paddinglength(n, alignment))
    return
end
