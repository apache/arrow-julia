<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# ArrowStrings.jl

An inline-else-view string representation designed for Arrow.jl and compatible
parsers. It is a separate package, released and registered independently from
this subdirectory like `ArrowTypes.jl`, so producers and consumers can use the
representation without depending on each other. Until its first release,
Arrow.jl resolves it through a `[sources]` path entry.

## Installation

ArrowStrings is not yet registered in General. Until then, run this from the
repository root:

```julia
import Pkg
Pkg.develop(path="src/ArrowStrings")
```

After registration, install a released version from the Julia REPL:

```julia
import Pkg
Pkg.add("ArrowStrings")
```

## API

* `ArrowString <: AbstractString` — a string value whose 16-byte payload **is** an
  Arrow StringView entry: strings of up to 12 bytes are stored inline;
  longer strings are a 4-byte prefix plus `(Int32 buffer index, Int32
  offset)` into a byte buffer. Byte access, `==`, `cmp`/`isless`, `hash`,
  and iteration never allocate and agree with `String`; `String(s)` copies
  out.
* `StringVector{ELT}` — a column of them: a payload vector plus the
  byte buffers the views point into. That is an Arrow Utf8View array's
  memory (views buffer + variadic data buffers), so Arrow.jl can write the
  column without repacking its payloads or data buffers.
  `ELT` is `ArrowString` or `Union{Missing, ArrowString}`; `getindex`
  allocates nothing; `materialize` copies out to `Vector{String}` (or
  `Vector{Union{String,Missing}}` for the nullable `ELT`).
* `ArrowBytes <: AbstractVector{UInt8}` and `BytesVector{ELT}` — the bytes
  counterparts, over the same 16-byte payload: Arrow's BinaryView entry is
  byte for byte the StringView layout, so one payload type serves both.
  `ArrowBytes` gives allocation-free byte access with `==` and `hash`
  agreeing with `Vector{UInt8}` (they work as `Dict` keys next to byte
  vectors); `Vector{UInt8}(b)` copies one value out, and
  `materialize(::BytesVector)` copies a column out to
  `Vector{Vector{UInt8}}`. `ELT` is `ArrowBytes` or
  `Union{Missing, ArrowBytes}`.

Everything depends only on Base and uses concrete types. CI compiles and runs
representative construction, access, comparison, and materialization under
JuliaC `--trim=safe`. Buffers must stay under 2 GiB (Arrow's `Int32` view
words). Construction validates payload geometry and prefixes; a builder that
produced every payload itself from bounds it already checked — a parser whose
offsets were validated on read — can pass `Val(:trusted)` as a trailing
constructor argument to skip that re-validation, vouching for the invariants
itself. Do not resize or mutate the payload vector or any referenced buffer
while a column is in use.

```julia
using ArrowStrings
buf = Vector{UInt8}(codeunits("id,name\n1,abcd\n2,a much longer value\n"))
payloads = [ArrowStrings.inline_payload(buf, 11, 4),
            ArrowStrings.view_payload(buf, 18, 19, 0, 17)]
col = StringVector{ArrowString}(payloads, buf, UInt8[])
col[2] == "a much longer value"     # true, no allocation
```

The Arrow 3.0 rewrite and this package used Anthropic Claude Code and OpenAI
Codex for code generation, test generation, and review. Apache Arrow
maintainers remain responsible for understanding, reviewing, testing, and
approving the code and each release.
