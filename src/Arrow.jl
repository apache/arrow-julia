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

"""
    Arrow.jl — a pure Julia implementation of the Apache Arrow columnar format.

Public surface: `Arrow.Table` reads the IPC stream and file formats (paths,
`IO`, byte vectors, or a `RangedSource`/`RangedFile` over a byte-range
fetcher) as Tables.jl tables, with `Tables.Scan` pushdown; `Arrow.Stream`
iterates a path, `IO`, or byte-vector source one record batch at a time;
`Arrow.write` writes any Tables.jl source; `close!` releases mapped or
foreign storage deterministically.

Layering ([docs/dev/core-README.md](https://github.com/apache/arrow-julia/blob/main/docs/dev/core-README.md)
documents each layer in depth):

- `ArrowCore` (private): ownership regions, layout registry, `ArrayData`,
  staged validation, accessors — the trim-friendly, dependency-free core.
- `Meta`: FlatBuffers metadata bindings AND the shape verifier, both
  GENERATED from the vendored spec schemas (`src/metadata/fbs/`, generator
  `tools/fbsgen.jl`) over the vendored `FlatBuffers` runtime.
- IPC adapters (`ipc_read.jl`, `ipc_write.jl`): stream and file formats,
  framing, resource limits, compression, dictionary lifecycles.
- `cdata.jl`: the C data and C stream interfaces, import and export, with
  lifecycle accounting.
- `scan.jl`: `Tables.Scan` pushdown over byte ranges plus footer-carried
  statistics pruning.
- `table.jl`, `write.jl`: the facade over the adapters.

The adapter entry points (`readstream`, `writestream`, `readfile`,
`writefile`, `to_c_data`, `from_c_data`, `export_stream!`, `from_c_stream`)
are exercised directly by the test batteries, the arrow-testing conformance
corpus, and the pyarrow/nanoarrow oracle suites under `conformance/`.
"""
module Arrow

using Tables
import Base64
import DataAPI
import ArrowStrings
import Dates
import Mmap
import CodecLz4
import CodecZstd
import TranscodingStreams
using CodecLz4: LZ4FrameCompressor
using CodecZstd: ZstdCompressor

const CLZ4 = CodecLz4
const CZSTD = CodecZstd
const ZSTD = CZSTD.LibZstd
const TS = TranscodingStreams

isdefined(Tables, :Scan) || error(
    "Arrow 3.0's scan support needs Tables.jl's `Tables.Scan` " *
    "interface; upgrade Tables.jl (or dev the `jq/scan` branch)",
)

include(joinpath("FlatBuffers", "FlatBuffers.jl"))
const FB = FlatBuffers

# Generated metadata bindings + shape verifier (regenerate with
# `julia tools/fbsgen.jl src/metadata/fbs src/metadata`).
module Meta
using EnumX
using ..FlatBuffers
include(joinpath("metadata", "Schema.jl"))
include(joinpath("metadata", "File.jl"))
include(joinpath("metadata", "Message.jl"))
include(joinpath("metadata", "VerifierRuntime.jl"))
include(joinpath("metadata", "Verifier.jl"))
end

include("ArrowCore.jl")
using .ArrowCore
const AC = ArrowCore

include("ipc_read.jl")
include("ipc_write.jl")
include("cdata.jl")
include("scan.jl")

# The public facade.
include("table.jl")
include("write.jl")

"""
    Arrow.nextbatch!(source) -> Union{Nothing, RecordBatch}

Pull the next record batch from a record-batch source — an IPC stream
(`readstream`), an imported C stream (`from_c_stream`) — or `nothing` at end
of stream. Sources are single-owner cursors: overlapping calls on one source
are an error.
"""
AC.nextbatch!

"""
    Arrow.ValidationError

Thrown by every validation tier — structural, semantic, and the opt-in
`validate_full` — when a descriptor or array violates the Arrow format.
"""
AC.ValidationError

export close!

end # module Arrow
