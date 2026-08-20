```@meta
CurrentModule = Arrow
```
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

# Migrating from Arrow.jl 2.x

Arrow.jl 3.0 is a breaking rewrite. The common names remain the same:
[`Arrow.Table`](@ref), [`Arrow.Stream`](@ref), [`Arrow.write`](@ref), and
[`Arrow.DictEncode`](@ref). The storage model and some advanced write
features changed.

## Requirements

Arrow 3.0 requires Julia 1.10 or later. It also requires the first Tables.jl
release that provides `Tables.Scan` and ArrowStrings.jl 1.0. The development
branch uses temporary source overrides until those releases are registered.

## Materialized columns

Arrow 2.x returned lazy `ArrowVector` objects that viewed the source buffers.
Arrow 3.0 returns plain Julia vectors:

```julia
table = Arrow.Table("data.arrow")
table.id isa Vector
```

There is no `convert=false` mode. Code that checks for an `ArrowVector` type
must use the Tables.jl interface or the column's normal `AbstractVector`
interface instead.

Concrete Arrow 2.x array types such as `Arrow.Primitive`, `Arrow.List`, and
`Arrow.DictEncoded` are no longer part of the read result. Use
`Tables.schema(table)`, `eltype(column)`, and the standard array interface.
Use `Tables.getcolumn(table, name_or_index)` in place of Arrow 2.x table
indexing.

The old positional byte-window arguments and multi-input constructors were
removed. Pass one complete path, `IO`, byte vector, or byte-range source to
`Arrow.Table` or `Arrow.Stream`. Slice an in-memory byte vector before the call
when needed. Read independent IPC sources separately and combine their tables
with a Tables.jl-aware consumer.

The source can still be memory-mapped while it is read. The returned columns
do not borrow the map. Call [`Arrow.close!`](@ref) when you need to release a
mapped file at a known time:

```julia
table = Arrow.Table("data.arrow")
Arrow.close!(table)
rm("data.arrow")

# The materialized columns are still valid.
sum(table.id)
```

## Writing

`Arrow.write` is eager. It materializes and validates all input partitions,
builds the complete IPC output, and then writes it to the sink. Arrow 3.0 does
not provide these Arrow 2.x features:

| Arrow 2.x feature | Arrow 3.0 action |
|---|---|
| `Arrow.Writer` | Collect the source as Tables.jl partitions and call `Arrow.write`. |
| `Arrow.append` | Rewrite the complete file. There is no append-to-file path. |
| `table |> Arrow.write(sink)` | Call `Arrow.write(sink, table)`. The curried write form was removed. |
| `Arrow.tobuffer(table)` | Write to an `IOBuffer`, then call `take!`. See below. |
| `ntasks` | Remove the keyword. Encoding is not task-parallel in 3.0. |
| `Arrow.ToArrow` | Convert the column to a supported Julia element type before writing. |
| ArrowTypes.jl custom types | Convert the values before writing. Arrow 3.0 does not consume the ArrowTypes interface. |

The output default for an `IO` changed. Arrow 2.x wrote the stream format to
an `IO` by default. Arrow 3.0 uses `file=true` for both paths and `IO` sinks.
Pass `file=false` when you need the IPC stream format:

```julia
io = IOBuffer()
Arrow.write(io, table; file=false)
stream_bytes = take!(io)
```

That pattern replaces `Arrow.tobuffer(table)`. Use `file=true` when you need
an IPC file with footer-based random access.

Arrow 3.0 keeps `file`, `compress`, `metadata`, and `colmetadata`. Compression
is selected with `:lz4` or `:zstd`; passing an initialized compressor object
is no longer supported. The Arrow 2.x `alignment`, `dictencode`,
`dictencodenested`, `denseunions`, `largelists`, `maxdepth`, and `ntasks`
writer keywords were removed. Wrap only the columns that need dictionary
encoding in [`Arrow.DictEncode`](@ref).

For example, convert a custom type to a supported storage column explicitly:

```julia
struct AccountID
    value::Int64
end

ids = AccountID.(1:3)
Arrow.write("accounts.arrow", (id = getfield.(ids, :value),))
```

[`Arrow.DictEncode`](@ref) remains the opt-in wrapper for a newly written
dictionary-encoded column.

## Supported write types

The writer accepts fixed-width integers and floats, `Bool`, strings, supported
`Dates` values, lists of supported core values, and top-level `NamedTuple`
struct columns. See [Type mapping when writing](@ref) for the complete table.

Arrow 3.0 does not write an arbitrary Julia struct by discovering its fields.
Convert it to a `NamedTuple` or to separate columns first.

ArrowTypes.jl mappings, including `ArrowType`, `toarrow`, `arrowname`, and
`fromarrow`, are not consulted. `Arrow.ToTimestamp` was also removed. Convert
zoned or custom values to one of the documented write types before calling
`Arrow.write`.

## Schema retention

When the source is an `Arrow.Table` or `Arrow.Stream`, the writer keeps
compatible details from the source schema. These details include temporal
units, byte and list widths, composite descriptors, nullability, field
metadata, schema metadata, and top-level dictionary index types and category
order. A column that was replaced with an incompatible Julia type is rejected
instead of being silently written under the old schema.

Materialization discards Union routing and nested dictionary pools. A retained
Union or nested Dictionary therefore fails clearly when rewritten. Top-level
dictionaries of scalar or composite values are supported after a full read. A
scan result can lack the hidden source pool, so an ordered dictionary from such
a result also fails clearly. Exact buffer sharing, overlapping ListView ranges,
and Run-End Encoding segmentation are not retained; the writer emits a
canonical layout with the same logical values and schema type.

## Names and imports

Arrow 3.0 uses a small export surface. Qualify package functions:

```julia
using Arrow

table = Arrow.Table("data.arrow")
Arrow.write("copy.arrow", table)
```

`ArrowTypes` is no longer re-exported. Import ArrowTypes.jl directly if other
code still uses that package.

`Arrow.getmetadata` was removed. Arrow 3.0 uses DataAPI.jl metadata methods:

```julia
using Arrow, DataAPI

collect(DataAPI.metadatakeys(table))
DataAPI.metadata(table, "key")
DataAPI.colmetadata(table, :column, "key")
```

## New features

Arrow 3.0 adds:

- `Tables.Scan` pushdown for projection, filters, limits, and offsets.
- Sparse byte-range reads through [`Arrow.AbstractArrowSource`](@ref).
- A CloudStore.jl extension for object storage.
- Arrow C data and C stream import and export.
- Arrow StringView and BinaryView support.
- Stronger validation and resource limits for untrusted IPC input.

See the [User Manual](@ref) for examples and the [API Reference](@ref) for the
supported entry points.
