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

Supertype of sources that supply bytes by range, such as objects in cloud storage, for
[`Arrow.Table`](@ref) and [`Arrow.Stream`](@ref). An implementation defines

    Arrow.sourcelength(src)::Integer                  # total length in bytes
    Arrow.readrange(src, offset, len)::Vector{UInt8}  # `len` bytes from 0-based `offset`

and may define [`Arrow.concurrentreads`](@ref) to let several ranges be read at once.

`Arrow.Table` with `select`, `drop`, `filter`, `limit`, or `scan` reads the file-format
footer from the end of the source, then fetches only the record batches and columns it
needs, coalesced into a few ranges; without them, it fetches the whole source in 8 MiB
ranges. `Arrow.Stream` fetches record batches as the iteration reaches them, small ones
several to a request. A stream-format source has no footer and is read in order.

`readrange` must return a new vector of exactly `len` bytes each time, and never reuse or
change it: Arrow keeps it without copying. Arrow validates what it reads, but the source
is trusted to return the bytes stored at the range.

```julia
struct BytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::BytesSource) = length(s.data)
Arrow.readrange(s::BytesSource, offset, len) = s.data[(offset + 1):(offset + len)]

table = Arrow.Table(BytesSource(bytes); select=[:a, :b])
```

With CloudStore.jl loaded, a `CloudStore.Object` (S3 or Azure Blob Storage) is a source.
"""
abstract type AbstractArrowSource end

"""
    Arrow.sourcelength(src::Arrow.AbstractArrowSource) -> Integer

The source's length in bytes. Every source defines it.
"""
function sourcelength end

"""
    Arrow.readrange(src::Arrow.AbstractArrowSource, offset, len) -> Vector{UInt8}

The `len` bytes starting at 0-based `offset`, as a new vector of exactly `len` elements
that the source does not touch again: Arrow keeps it without copying. Every source
defines it.
"""
function readrange end

"""
    Arrow.concurrentreads(src::Arrow.AbstractArrowSource) -> Int

How many [`Arrow.readrange`](@ref) calls one read may have in flight at once, up to
`Limits.max_concurrent_reads`. The default, 1, reads ranges one at a time; a source whose
requests are independent, such as HTTP range requests, returns a larger number. The
limit applies to each read separately: a source that needs one cap for the whole
process enforces it in `readrange`.
"""
concurrentreads(::AbstractArrowSource) = 1
