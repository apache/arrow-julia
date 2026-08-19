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

# CloudStore.jl objects as Arrow byte-range sources: `Arrow.Table(obj;
# scan=…)` reads only the selected columns' bytes from S3, Azure Blob
# Storage, or GCS through HTTP `Range` requests.
module ArrowCloudStoreExt

using Arrow
using CloudStore: CloudStore, Object

"""
    CloudObjectSource(obj::CloudStore.Object) <: Arrow.AbstractArrowSource

A `CloudStore.Object` as an [`Arrow.AbstractArrowSource`](@ref): the length
is the object's known size, one range is one HTTP `Range` GET, and the
planned ranges of a scan are fetched concurrently. `Arrow.Table(obj; …)` and
`Arrow.Stream(obj; …)` construct one implicitly.
"""
struct CloudObjectSource{O<:Object} <: Arrow.AbstractArrowSource
    obj::O
end

Arrow.sourcelength(s::CloudObjectSource) = Int64(s.obj.size)

function Arrow.readrange(s::CloudObjectSource, off, len)
    len == 0 && return UInt8[]
    last = off + len - 1
    obj = s.obj
    bytes = CloudStore.get(
        obj.store,
        obj.key;
        credentials=obj.credentials,
        headers=["Range" => "bytes=$(off)-$(last)"],
        allowMultipart=false,
        objectMaxSize=Int(len),
    )
    return bytes isa Vector{UInt8} ? bytes : Vector{UInt8}(bytes)
end

# One task per planned range: the requests are independent GETs, and the
# planner has already coalesced neighbours, so the remaining ranges are
# worth issuing at once.
function Arrow.readranges(s::CloudObjectSource, ranges::Vector{NTuple{2,Int64}})
    length(ranges) <= 1 &&
        return Vector{UInt8}[Arrow.readrange(s, off, len) for (off, len) in ranges]
    tasks = [Threads.@spawn Arrow.readrange(s, off, len) for (off, len) in ranges]
    return Vector{UInt8}[fetch(t)::Vector{UInt8} for t in tasks]
end

Arrow.Table(obj::Object; kw...) = Arrow.Table(CloudObjectSource(obj); kw...)
Arrow.Stream(obj::Object; kw...) = Arrow.Stream(CloudObjectSource(obj); kw...)

end # module
