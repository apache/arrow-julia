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
# Storage through HTTP `Range` requests.
module ArrowCloudStoreExt

using Arrow
using CloudStore: CloudStore, Object

"""
    CloudObjectSource(obj::CloudStore.Object) <: Arrow.AbstractArrowSource

A `CloudStore.Object` as an [`Arrow.AbstractArrowSource`](@ref): the length
is the object's known size, one range is one HTTP `Range` GET pinned to the
object's ETag with `If-Match` (an overwritten key fails the read instead of
mixing versions across requests), and Arrow issues up to
`CONCURRENT_RANGE_READS` of a round's ranges at once. `Arrow.Table(obj; …)`
and `Arrow.Stream(obj; …)` construct one implicitly.
"""
struct CloudObjectSource{O<:Object} <: Arrow.AbstractArrowSource
    obj::O
    etag::String

    function CloudObjectSource(obj::Object)
        if isempty(obj.eTag)
            snapshot = Object(obj.store, obj.key; credentials=obj.credentials)
            snapshot.size == obj.size ||
                throw(ArgumentError("object size changed before the Arrow read"))
            obj = snapshot
        end
        return new{typeof(obj)}(obj, _ifmatch(obj.eTag))
    end
end

# Independent HTTP range GETs are latency-bound, so a round's requests are
# worth overlapping; the bound keeps a fragmented object from becoming a
# request storm.
const CONCURRENT_RANGE_READS = 16

Arrow.sourcelength(s::CloudObjectSource) = Int64(s.obj.size)
Arrow.concurrentreads(::CloudObjectSource) = CONCURRENT_RANGE_READS

# The object's ETag as an `If-Match` value (the header wants it quoted).
function _ifmatch(etag::AbstractString)
    (isempty(etag) || startswith(etag, "W/")) &&
        throw(ArgumentError("Arrow range reads require a strong object ETag"))
    tag = etag
    if startswith(tag, '"')
        ncodeunits(tag) >= 2 && endswith(tag, '"') ||
            throw(ArgumentError("invalid object ETag"))
        tag = SubString(tag, 2, prevind(tag, lastindex(tag)))
    end
    all(b -> b == 0x21 || 0x23 <= b <= 0x7e || b >= 0x80, codeunits(tag)) ||
        throw(ArgumentError("invalid object ETag"))
    return string('"', tag, '"')
end

function Arrow.readrange(s::CloudObjectSource, off, len)
    len == 0 && return UInt8[]
    last = off + len - 1
    obj = s.obj
    headers = ["Range" => "bytes=$(off)-$(last)", "If-Match" => s.etag]
    bytes = CloudStore.get(
        obj.store,
        obj.key;
        credentials=obj.credentials,
        headers=headers,
        allowMultipart=false,
        objectMaxSize=Int(len),
    )
    return bytes isa Vector{UInt8} ? bytes : Vector{UInt8}(bytes)
end

Arrow.Table(obj::Object; kw...) = Arrow.Table(CloudObjectSource(obj); kw...)
Arrow.Stream(obj::Object; kw...) = Arrow.Stream(CloudObjectSource(obj); kw...)

end # module
