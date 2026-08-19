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
`Tables.Scan`, the file-format footer is fetched from the tail, the record
batches are pruned by the footer's statistics and the scan's window, and only
the buffers of the selected (and filter-referenced) columns are requested,
coalesced into a few range reads.

An implementation defines two methods:

    Arrow.sourcelength(src)::Integer                  # total length in bytes
    Arrow.readrange(src, offset, len)::Vector{UInt8}  # `len` bytes from 0-based `offset`

and may override

    Arrow.readranges(src, ranges::Vector{NTuple{2,Int64}}) -> Vector{Vector{UInt8}}

whose default reads the planned `(offset, len)` ranges one at a time through
`readrange`; a transport that can issue them concurrently should. Every
returned vector must have exactly the requested length.

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
column bytes from S3, Azure Blob Storage, or GCS. Stream-format objects have
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
    Arrow.readranges(src::AbstractArrowSource, ranges::Vector{NTuple{2,Int64}}) -> Vector{Vector{UInt8}}

One result per requested `(offset, len)`, in order. The default reads them
serially through [`Arrow.readrange`](@ref); a transport overrides this to
issue the planned ranges concurrently.
"""
readranges(src::AbstractArrowSource, ranges::Vector{NTuple{2,Int64}}) =
    Vector{UInt8}[readrange(src, off, len) for (off, len) in ranges]
