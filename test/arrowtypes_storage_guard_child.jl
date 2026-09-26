# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

using Arrow
using ArrowTypes

struct ArrowTypesStorageGuardValue{N}
    value::Int8
end

ArrowTypes.ArrowType(::Type{ArrowTypesStorageGuardValue{N}}) where {N} = NTuple{N,Int8}
ArrowTypes.toarrow(x::ArrowTypesStorageGuardValue{N}) where {N} =
    ntuple(_ -> x.value, Val(N))

T = ArrowTypesStorageGuardValue{2048}
input = Union{Missing,T}[missing]
io = IOBuffer()
err = try
    Arrow.write(io, (value=input,); file=false)
    nothing
catch e
    e
end
err isa ArgumentError || error("oversized ArrowType result did not fail with ArgumentError")
message = sprint(showerror, err)
occursin("concrete Tuple with 2048 fields", message) || error(message)
occursin("supported limit is 1024", message) || error(message)
println("oversized ArrowType storage rejected")
