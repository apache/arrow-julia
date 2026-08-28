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

# =============================================================================
# The byte-range source interface: what a remote (or any byte-addressable)
# object must provide for `Arrow.Table` to read it with exact range requests.
# =============================================================================

"""
    Arrow.AbstractArrowSource

A byte-addressable object of known length — an object in cloud storage, an
HTTP resource, an in-memory buffer — that [`Arrow.Table`](@ref) reads with
exact byte-range requests instead of downloading whole. With a
`Tables.Scan`, Arrow reads the file-format footer from the tail. It prunes
batches by the footer's statistics and — without a filter — by the scan's
window. It then requests only the surviving batches' buffers for the
selected and filter-referenced columns, coalesced into a few ranges issued
in one round. A filtered limit stops decoding, not fetching.

An implementation defines two methods:

    Arrow.sourcelength(src)::Integer                  # total length in bytes
    Arrow.readrange(src, offset, len)::Vector{UInt8}  # `len` bytes from 0-based `offset`

and may override

    Arrow.concurrentreads(src)::Int                   # default 1

to let Arrow issue planned ranges through `readrange` concurrently. One
read handle samples that preference once, clamps it by
`Limits.max_concurrent_reads`, and uses one semaphore across every operation
on that handle (Arrow places each result by its request, so completion order
never matters). Separate read handles are independent; a transport that
needs one process-wide cap must enforce it inside `readrange`.

Every returned vector must have exactly the requested length. Arrow
validates lengths and offsets and refuses violations with a
`ValidationError`. Arrow cannot authenticate the bytes themselves: the
source is trusted to return what lives at the requested range.

```julia
struct BytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::BytesSource) = length(s.data)
Arrow.readrange(s::BytesSource, offset, len) = s.data[(offset + 1):(offset + len)]

tbl = Arrow.Table(BytesSource(bytes); scan=Tables.Scan(select=(:a, :b)))
```

The `CloudStore.jl` extension makes a `CloudStore.Object` a source, so
`Arrow.Table(CloudStore.Object(bucket, key); scan=…)` reads just the needed
column bytes from S3 or Azure Blob Storage. Stream-format objects have
no footer and are read whole.
"""
abstract type AbstractArrowSource end

"""
    Arrow.sourcelength(src::AbstractArrowSource) -> Integer

The source's total length in bytes. Required of every implementation.
"""
function sourcelength end

"""
    Arrow.readrange(src::AbstractArrowSource, offset, len) -> Vector{UInt8}

`len` bytes starting at 0-based `offset`; the result must have exactly `len`
elements. Required of every implementation.
"""
function readrange end

"""
    Arrow.concurrentreads(src::AbstractArrowSource) -> Int

How many [`Arrow.readrange`](@ref) calls Arrow may have in flight at once
through one `SourceFile` handle. The default, `1`, reads them one at a time;
a transport whose requests are independent (HTTP range GETs) returns a bound
suited to it. The handle samples the value once, clamps it by
`Limits.max_concurrent_reads`, and stores every result by request index.
Separate handles are independent.
"""
concurrentreads(::AbstractArrowSource) = 1
