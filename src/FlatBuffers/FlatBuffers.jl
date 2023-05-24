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

module FlatBuffers

import Base: ==

const UOffsetT = UInt32
const SOffsetT = Int32
const VOffsetT = UInt16
const VtableMetadataFields = 2

basetype(::Enum) = UInt8
basetype(::Type{T}) where {T <: Enum{S}} where {S} = S

function readbuffer(t::AbstractVector{UInt8}, pos::Integer, ::Type{Bool})
    @inbounds b = t[pos + 1]
    return b === 0x01
end

function readbuffer(t::AbstractVector{UInt8}, pos::Integer, ::Type{T}) where {T}
    GC.@preserve t begin
        ptr = convert(Ptr{T}, pointer(t, pos + 1))
        x = unsafe_load(ptr)
    end
end

include("builder.jl")
include("table.jl")

function Base.show(io::IO, x::TableOrStruct)
    print(io, "$(typeof(x))")
    if isempty(propertynames(x))
        print(io, "()")
    else
        show(io, NamedTuple{propertynames(x)}(Tuple(getproperty(x, y) for y in propertynames(x))))
    end
end

end # module
