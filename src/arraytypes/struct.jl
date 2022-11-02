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
    Arrow.Struct

An `ArrowVector` where each element is a "struct" of some kind with ordered, named fields, like a `NamedTuple{names, types}` or regular julia `struct`.
"""
struct Struct{T, S} <: ArrowVector{T}
    validity::ValidityBitmap
    data::S # Tuple of ArrowVector
    ℓ::Int
    metadata::Union{Nothing, Base.ImmutableDict{String,String}}
end

Base.size(s::Struct) = (s.ℓ,)

isnamedtuple(::Type{<:NamedTuple}) = true
isnamedtuple(T) = false
istuple(::Type{<:Tuple}) = true
istuple(T) = false

@propagate_inbounds function Base.getindex(s::Struct{T,S}, i::Integer) where {T,S}
    @boundscheck checkbounds(s, i)
    NT = Base.nonmissingtype(T)
    if isnamedtuple(NT) || istuple(NT)
        if NT !== T
            return s.validity[i] ? NT(ntuple(j->s.data[j][i], fieldcount(S))) : missing
        else
            return NT(ntuple(j->s.data[j][i], fieldcount(S)))
        end
    else
        if NT !== T
            return s.validity[i] ? ArrowTypes.fromarrow(NT, (s.data[j][i] for j = 1:fieldcount(S))...) : missing
        else
            return ArrowTypes.fromarrow(NT, (s.data[j][i] for j = 1:fieldcount(S))...)
        end
    end
end

# @propagate_inbounds function Base.setindex!(s::Struct{T}, v::T, i::Integer) where {T}
#     @boundscheck checkbounds(s, i)
#     if v === missing
#         @inbounds s.validity[i] = false
#     else
#         NT = Base.nonmissingtype(T)
#         N = fieldcount(NT)
#         foreach(1:N) do j
#             @inbounds s.data[j][i] = getfield(v, j)
#         end
#     end
#     return v
# end

struct ToStruct{T, i, A} <: AbstractVector{T}
    data::A # eltype is NamedTuple or some struct
end

ToStruct(x::A, j::Integer) where {A} = ToStruct{fieldtype(Base.nonmissingtype(eltype(A)), j), j, A}(x)

Base.IndexStyle(::Type{<:ToStruct}) = Base.IndexLinear()
Base.size(x::ToStruct) = (length(x.data),)

Base.@propagate_inbounds function Base.getindex(A::ToStruct{T, j}, i::Integer) where {T, j}
    @boundscheck checkbounds(A, i)
    @inbounds x = A.data[i]
    return x === missing ? ArrowTypes.default(T) : getfield(x, j)
end

arrowvector(::StructKind, x::Struct, i, nl, fi, de, ded, meta; kw...) = x

namedtupletype(::Type{NamedTuple{names, types}}, data) where {names, types} = NamedTuple{names, Tuple{(eltype(x) for x in data)...}}
namedtupletype(::Type{T}, data) where {T} = NamedTuple{fieldnames(T), Tuple{(eltype(x) for x in data)...}}
namedtupletype(::Type{T}, data) where {T <: Tuple} = NamedTuple{map(Symbol, fieldnames(T)), Tuple{(eltype(x) for x in data)...}}

function arrowvector(::StructKind, x, i, nl, fi, de, ded, meta; kw...)
    len = length(x)
    validity = ValidityBitmap(x)
    T = Base.nonmissingtype(eltype(x))
    data = Tuple(arrowvector(ToStruct(x, j), i, nl + 1, j, de, ded, nothing; kw...) for j = 1:fieldcount(T))
    return Struct{withmissing(eltype(x), namedtupletype(T, data)), typeof(data)}(validity, data, len, meta)
end

function compress(Z::Meta.CompressionType, comp, x::A) where {A <: Struct}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    buffers = [validity]
    children = Compressed[]
    for y in x.data
        push!(children, compress(Z, comp, y))
    end
    return Compressed{Z, A}(x, buffers, len, nc, children)
end

function makenodesbuffers!(col::Struct{T}, fieldnodes, fieldbuffers, bufferoffset, alignment) where {T}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debugv 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debugv 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += blen
    for child in col.data
        bufferoffset = makenodesbuffers!(child, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    return bufferoffset
end

function writebuffer(io, col::Struct, alignment)
    @debugv 1 "writebuffer: col = $(typeof(col))"
    @debugv 2 col
    writebitmap(io, col, alignment)
    # write values arrays
    for child in col.data
        writebuffer(io, child, alignment)
    end
    return
end
