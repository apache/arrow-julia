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
    Arrow.Map

An `ArrowVector` where each element is a "map" of some kind, like a `Dict`.
"""
struct Map{T,O,A} <: ArrowVector{T}
    validity::ValidityBitmap
    offsets::Offsets{O}
    data::A
    ℓ::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

Base.size(l::Map) = (l.ℓ,)

@propagate_inbounds function Base.getindex(l::Map{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    @inbounds lo, hi = l.offsets[i]
    if Base.nonmissingtype(T) !== T
        return l.validity[i] ?
               ArrowTypes.fromarrow(
            T,
            Dict(x.key => x.value for x in view(l.data, lo:hi)),
        ) : missing
    else
        return ArrowTypes.fromarrow(T, Dict(x.key => x.value for x in view(l.data, lo:hi)))
    end
end

@inline function _promotemapoffsets(offsets::Vector{Int32}, len::Int, filled::Int)
    promoted = Vector{Int64}(undef, len + 1)
    copyto!(promoted, 1, offsets, 1, filled)
    return promoted
end

function _mapoffsetsandvaluesindexed(::Type{KT}, x; largelists::Bool=false) where {KT}
    len = length(x)
    O = largelists ? Int64 : Int32
    offsets = Vector{O}(undef, len + 1)
    offsets[1] = zero(O)
    total = 0
    off = firstindex(x) - 1
    @inbounds for i = 1:len
        y = x[i + off]
        if y !== missing
            total += length(y)
            if O === Int32 && total > typemax(Int32)
                O = Int64
                offsets = _promotemapoffsets(offsets, len, i)
            end
        end
        offsets[i + 1] = total
    end
    values = Vector{KT}(undef, total)
    pos = 1
    @inbounds for i = 1:len
        y = x[i + off]
        y === missing && continue
        for (k, v) in pairs(y)
            values[pos] = KT(k, v)
            pos += 1
        end
    end
    return offsets, values
end

function mapoffsetsandvalues(::Type{KT}, x; largelists::Bool=false) where {KT}
    Base.has_offset_axes(x) &&
        return _mapoffsetsandvaluesindexed(KT, x; largelists=largelists)
    len = length(x)
    O = largelists ? Int64 : Int32
    offsets = Vector{O}(undef, len + 1)
    offsets[1] = zero(O)
    total = 0
    i = 1
    for y in x
        if y !== missing
            total += length(y)
            if O === Int32 && total > typemax(Int32)
                O = Int64
                offsets = _promotemapoffsets(offsets, len, i)
            end
        end
        @inbounds offsets[i + 1] = total
        i += 1
    end
    values = Vector{KT}(undef, total)
    pos = 1
    for y in x
        y === missing && continue
        for (k, v) in pairs(y)
            @inbounds values[pos] = KT(k, v)
            pos += 1
        end
    end
    return offsets, values
end

function mapoffsetsandvalues(
    ::Type{KT},
    x::ArrowTypes.ToArrow;
    largelists::Bool=false,
) where {KT}
    len = length(x)
    O = largelists ? Int64 : Int32
    offsets = Vector{O}(undef, len + 1)
    offsets[1] = zero(O)
    total = 0
    @inbounds for i = 1:len
        y = x[i]
        if y !== missing
            total += length(y)
            if O === Int32 && total > typemax(Int32)
                O = Int64
                offsets = _promotemapoffsets(offsets, len, i)
            end
        end
        offsets[i + 1] = total
    end
    values = Vector{KT}(undef, total)
    pos = 1
    @inbounds for i = 1:len
        y = x[i]
        y === missing && continue
        for (k, v) in pairs(y)
            values[pos] = KT(k, v)
            pos += 1
        end
    end
    return offsets, values
end

keyvaluetypes(::Type{NamedTuple{(:key, :value),Tuple{K,V}}}) where {K,V} = (K, V)

arrowvector(::MapKind, x::Map, i, nl, fi, de, ded, meta; kw...) = x

function arrowvector(::MapKind, x, i, nl, fi, de, ded, meta; largelists::Bool=false, kw...)
    len = length(x)
    validity = ValidityBitmap(x)
    ET = eltype(x)
    DT = Base.nonmissingtype(ET)
    KDT, VDT = keytype(DT), valtype(DT)
    ArrowTypes.concrete_or_concreteunion(KDT) || throw(
        ArgumentError(
            "`keytype(d)` must be concrete to serialize map-like `d`, but `keytype(d) == $KDT`",
        ),
    )
    ArrowTypes.concrete_or_concreteunion(VDT) || throw(
        ArgumentError(
            "`valtype(d)` must be concrete to serialize map-like `d`, but `valtype(d) == $VDT`",
        ),
    )
    KT = KeyValue{KDT,VDT}
    offsetsdata, values = mapoffsetsandvalues(KT, x; largelists=largelists)
    offsets = Offsets(UInt8[], offsetsdata)
    data =
        arrowvector(values, i, nl + 1, fi, de, ded, nothing; largelists=largelists, kw...)
    K, V = keyvaluetypes(eltype(data))
    return Map{withmissing(ET, Dict{K,V}),eltype(offsetsdata),typeof(data)}(
        validity,
        offsets,
        data,
        len,
        meta,
    )
end

function compress(Z::Meta.CompressionType.T, comp, x::A) where {A<:Map}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    offsets = compress(Z, comp, x.offsets.offsets)
    buffers = [validity, offsets]
    children = Compressed[]
    push!(children, compress(Z, comp, x.data))
    return Compressed{Z,A}(x, buffers, len, nc, children)
end

function makenodesbuffers!(
    col::Union{Map{T,O,A},List{T,O,A}},
    fieldnodes,
    fieldbuffers,
    bufferoffset,
    alignment,
) where {T,O,A}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    # adjust buffer offset, make array buffer
    bufferoffset += blen
    blen = sizeof(O) * (len + 1)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += padding(blen, alignment)
    if liststringtype(col)
        blen = length(col.data)
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
        bufferoffset += padding(blen, alignment)
    else
        bufferoffset =
            makenodesbuffers!(col.data, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    return bufferoffset
end

function writebuffer(io, col::Union{Map{T,O,A},List{T,O,A}}, alignment) where {T,O,A}
    @debug "writebuffer: col = $(typeof(col))"
    @debug col
    writebitmap(io, col, alignment)
    # write offsets
    n = writearray(io, O, col.offsets.offsets)
    @debug "writing array: col = $(typeof(col.offsets.offsets)), n = $n, padded = $(padding(n, alignment))"
    writezeros(io, paddinglength(n, alignment))
    # write values array
    if liststringtype(col)
        n = writearray(io, UInt8, col.data)
        @debug "writing array: col = $(typeof(col.data)), n = $n, padded = $(padding(n, alignment))"
        writezeros(io, paddinglength(n, alignment))
    else
        writebuffer(io, col.data, alignment)
    end
    return
end
