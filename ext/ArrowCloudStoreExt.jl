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

const HTTP = CloudStore.API.HTTP

"""
    CloudObjectSource(obj::CloudStore.Object) <: Arrow.AbstractArrowSource

A `CloudStore.Object` as an [`Arrow.AbstractArrowSource`](@ref): the length
is the object's known size, one range is one HTTP `Range` GET pinned to the
object's strong ETag with `If-Match` (an overwritten key fails the read instead of
mixing versions across requests), and Arrow issues up to
`CONCURRENT_RANGE_READS` of a round's ranges at once. `Arrow.Table(obj; …)`
and `Arrow.Stream(obj; …)` construct one implicitly.

If the ETag is missing, construction refreshes metadata once. A changed size or
missing, weak, or malformed ETag throws before any ranges are read.

Every nonempty read, including a whole-object read, requires a 206 response
whose range, total size, ETag, and body length match the request. HTTP content
encodings such as gzip are rejected; Arrow's IPC compression remains supported.
An empty read returns an empty vector without a request.
"""
struct CloudObjectSource{O<:Object} <: Arrow.AbstractArrowSource
    obj::O

    function CloudObjectSource(obj::Object)
        obj.size >= 0 || throw(ArgumentError("object size must be nonnegative"))
        snapshot =
            isempty(obj.eTag) ? Object(obj.store, obj.key; credentials=obj.credentials) :
            obj
        snapshot.size == obj.size ||
            throw(ArgumentError("object size changed before reading"))
        tag = _ifmatch(snapshot.eTag)
        tag !== nothing && occursin(r"^\"[^\x00-\x20\"\x7f]*\"\z", tag) ||
            throw(ArgumentError("cloud source requires a strong ETag"))
        return new{typeof(snapshot)}(snapshot)
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
    isempty(etag) && return nothing
    return startswith(etag, '"') ? String(etag) : string('"', etag, '"')
end

function Arrow.readrange(s::CloudObjectSource, off, len)
    len == 0 && return UInt8[]
    last = off + len - 1
    obj = s.obj
    headers = ["Range" => "bytes=$(off)-$(last)", "Accept-Encoding" => "identity"]
    etag = _ifmatch(obj.eTag)
    etag === nothing || push!(headers, "If-Match" => etag)
    response = CloudStore.API.getObject(
        obj.store,
        CloudStore.API.makeURL(obj.store, obj.key),
        headers;
        credentials=obj.credentials,
        decompress=false,
    )
    response.status == 206 ||
        throw(Arrow.ValidationError("cloud range request did not return a 206 response"))
    range =
        match(r"^bytes (\d+)-(\d+)/(\d+)\z"i, HTTP.header(response, "Content-Range", ""))
    range !== nothing &&
    (tryparse(Int64, range[1]), tryparse(Int64, range[2]), tryparse(Int64, range[3])) ==
    (off, last, obj.size) || throw(
        Arrow.ValidationError("cloud response does not match the requested byte range"),
    )
    HTTP.header(response, "ETag", "") == etag ||
        throw(Arrow.ValidationError("cloud response does not match the source ETag"))
    lowercase(strip(HTTP.header(response, "Content-Encoding", ""))) in ("", "identity") ||
        throw(Arrow.ValidationError("cloud range response has an HTTP content encoding"))
    bytes = response.body
    length(bytes) == len ||
        throw(Arrow.ValidationError("cloud range response has an unexpected body length"))
    return bytes isa Vector{UInt8} ? bytes : Vector{UInt8}(bytes)
end

Arrow.Table(obj::Object; kw...) = Arrow.Table(CloudObjectSource(obj); kw...)
Arrow.Stream(obj::Object; kw...) = Arrow.Stream(CloudObjectSource(obj); kw...)

end # module
