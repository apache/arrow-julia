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

# Layering (docs/dev/core-README.md documents each layer in depth):
#
# - `ArrowCore` (private): ownership regions, layout registry, `ArrayData`,
#   staged validation, accessors; the trim-friendly, dependency-free core.
# - `Meta`: FlatBuffers metadata bindings and the shape verifier, both
#   generated from the vendored spec schemas (`src/metadata/fbs/`, generator
#   `tools/fbsgen.jl`) over the vendored `FlatBuffers` runtime.
# - IPC adapters (`ipc_read.jl`, `ipc_write.jl`): stream and file formats,
#   framing, resource limits, compression, dictionary lifecycles.
# - `cdata.jl`: the C data and C stream interfaces, import and export, with
#   lifecycle accounting.
# - `scan.jl`: `Tables.Scan` pushdown over byte ranges plus footer-carried
#   statistics pruning.
# - `table.jl`, `write.jl`: the read facade and write orchestration.
# - `columnconstruction.jl`: column-wide construction policy, including
#   retained schemas, dictionaries, and recursive ArrowTypes.jl lowering.
"""
    Arrow

Read and write the Apache Arrow IPC file and stream formats. [`Arrow.Table`](@ref) reads
a path, an `IO`, a byte vector, or an [`Arrow.AbstractArrowSource`](@ref) as Tables.jl
columns; [`Arrow.Stream`](@ref) reads one record batch at a time; [`Arrow.write`](@ref)
and [`Arrow.Writer`](@ref) write any Tables.jl table. The names are public but not
exported, apart from `ArrowTypes`: call them as `Arrow.Table` and so on.
"""
module Arrow

using Tables
import Base64
import DataAPI
import DataStrings, DataDecimals, Durations
import PooledArrays
using SentinelArrays: ChainedVector
import ArrowTypes
import Dates
import Mmap
import UUIDs
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
    "interface; upgrade Tables.jl to 1.14 or later",
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
import .ArrowCore: release!
const AC = ArrowCore

include("ipc_read.jl")
include("ipc_dictionary.jl")
include("ipc_write.jl")
include("cdata.jl")
include("source.jl")
include("arrowtypes.jl")
include("columns.jl")

# The public facade. `table.jl` includes the private scan-plan module after
# its public/storage type seam is defined; `write.jl` includes the private
# column-construction module after the `DictEncode` marker is defined.
include("table.jl")
include("write.jl")
include("sharedvalues.jl")

@doc """
    Arrow.Field

A column's name, Arrow type, nullability, children, and metadata, as the C data
interface uses it. [`Arrow.fromjulia`](@ref) and [`Arrow.from_c_data`](@ref) return one.
""" Field

@doc """
    Arrow.Schema

The [`Arrow.Field`](@ref)s of a record batch, in order, with optional key-value
metadata. The C stream interface describes its batches with one.
""" Schema

@doc """
    Arrow.ArrayData

The values of one column in Arrow's memory layout: its buffers, children, optional
dictionary, and length. [`Arrow.fromjulia`](@ref) and [`Arrow.from_c_data`](@ref) return
one; [`Arrow.materialize`](@ref) converts it to a Julia vector, and `copy` copies it into
Julia-owned memory.
""" ArrayData

@doc """
    Arrow.RecordBatch

A record batch: a [`Arrow.Schema`](@ref) (`batch.schema`), one
[`Arrow.ArrayData`](@ref) per field (`batch.columns`), and the row count
(`batch.nrows`). [`Arrow.batch`](@ref) builds one from Julia vectors.
""" RecordBatch

@doc """
    Arrow.fromjulia(name, values) -> (Arrow.Field, Arrow.ArrayData)

The field and data of a column named `name` holding `values`, for the C data
interface. The data may share `values`' memory: do not modify or resize `values` while
the result is in use.
""" fromjulia

@doc """
    Arrow.batch(columns::NamedTuple) -> Arrow.RecordBatch

A record batch of the named vectors in `columns`, as [`Arrow.fromjulia`](@ref) builds
each one.
""" batch

@doc """
    Arrow.materialize(field, data) -> Vector
    Arrow.materialize(T, field, data) -> Vector{T}

The values of `data`, a column described by `field`, as a new Julia vector. The typed
form returns a `Vector{T}` and first checks that `T` fits the field's Arrow type.
""" materialize

"""
    Arrow.nextbatch!(stream::Arrow.ImportedStream) -> Union{Arrow.RecordBatch,Nothing}

The next record batch of a stream imported with [`Arrow.from_c_stream`](@ref), or
`nothing` at its end. Overlapping calls on one stream are an error.
"""
AC.nextbatch!

"""
    Arrow.ValidationError

Thrown when input is not valid Arrow data, or uses something this package does not
read: corrupt, truncated, or inconsistent metadata or buffers, bytes that are not Arrow
at all, or big-endian data. The message names the problem.
"""
AC.ValidationError

# Julia 1.11 added `public`. Keep the source parseable on the supported
# Julia 1.10 floor while giving tooling an exact, non-exporting API boundary
# on newer Julia versions.
@static if VERSION >= v"1.11"
    Core.eval(
        @__MODULE__,
        Expr(
            :public,
            :Table,
            :Stream,
            :read,
            :release!,
            :col,
            :colcmp,
            :colin,
            :isnull,
            :ArrowVector,
            :Primitive,
            :BoolVector,
            :ListVector,
            :ListValue,
            :MapVector,
            :MapValue,
            :StructVector,
            :DictEncoded,
            :RunEndEncoded,
            :write,
            :Writer,
            :append,
            :tobuffer,
            :getmetadata,
            :DictEncode,
            :AbstractArrowSource,
            :sourcelength,
            :readrange,
            :concurrentreads,
            :Field,
            :Schema,
            :ArrayData,
            :RecordBatch,
            :fromjulia,
            :batch,
            :materialize,
            :CArrowSchema,
            :CArrowArray,
            :CArrowArrayStream,
            :to_c_data,
            :from_c_data,
            :export_stream!,
            :from_c_stream,
            :ForeignOwner,
            :ImportedStream,
            :nextbatch!,
            :reap!,
            :Limits,
            :AllocationLimitError,
            :ValidationError,
        ),
    )
end

# The one name Arrow 2.x exported. Packages that define custom-type mappings
# reach the interface as `Arrow.ArrowTypes` or through `using Arrow`.
export ArrowTypes

# Precompile the common write/read/scan shapes last, once every layer above
# is defined.
include("precompile.jl")

end # module Arrow
