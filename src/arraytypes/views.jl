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

struct ViewElement
    length::Int32
    prefix::Int32
    bufindex::Int32
    offset::Int32
end

"""
    Arrow.View

An `ArrowVector` where each element is a variable sized list of some kind, like an `AbstractVector` or `AbstractString`.
"""
struct View{T} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    data::Vector{ViewElement}
    inline::Vector{UInt8} # `data` field reinterpreted as a byte array
    buffers::Vector{Vector{UInt8}} # holds non-inlined data
    ℓ::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

Base.size(l::View) = (l.ℓ,)

@propagate_inbounds function Base.getindex(l::View{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    @inbounds v = l.data[i]
    S = Base.nonmissingtype(T)
    if S <: Base.CodeUnits
        # BinaryView
        return !l.validity[i] ? missing :
               v.length < 13 ?
               Base.CodeUnits(
            StringView(
                @view l.inline[(((i - 1) * 16) + 5):(((i - 1) * 16) + 5 + v.length - 1)]
            ),
        ) :
               Base.CodeUnits(
            StringView(
                @view l.buffers[v.bufindex + 1][(v.offset + 1):(v.offset + v.length)]
            ),
        )
    else
        # Utf8View
        return !l.validity[i] ? missing :
               v.length < 13 ?
               ArrowTypes.fromarrow(
            T,
            StringView(
                @view l.inline[(((i - 1) * 16) + 5):(((i - 1) * 16) + 5 + v.length - 1)]
            ),
        ) :
               ArrowTypes.fromarrow(
            T,
            StringView(
                @view l.buffers[v.bufindex + 1][(v.offset + 1):(v.offset + v.length)]
            ),
        )
    end
end

# @propagate_inbounds function Base.setindex!(l::List{T}, v, i::Integer) where {T}

# end
