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

Arrow 3.0 requires Julia 1.10 or later and ArrowTypes.jl 2.x. It also requires
Tables.jl 1.14 (the first release that provides `Tables.Scan`) and
ArrowStrings.jl 1.0. The development branch uses source overrides for the
in-repository subpackages until their releases are registered.

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
do not borrow the map. Call [`Arrow.release!`](@ref) when you need to release a
mapped file at a known time:

```julia
table = Arrow.Table("data.arrow")
Arrow.release!(table)
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
| `Arrow.ToArrow` | Import `ArrowTypes.ToArrow` directly when an explicit lazy conversion view is needed. Normal writes apply the mapping automatically. |
| `Arrow.ArrowTypes` or an exported `ArrowTypes` name | Use `import ArrowTypes` for new code. The qualified `Arrow.ArrowTypes` binding remains for compatibility, but it is not exported. |

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

Arrow 3.0 again consumes the ArrowTypes.jl mapping interface. Package authors
should depend on and import ArrowTypes.jl directly. Define `ArrowType` and
`toarrow` to lower a custom value to supported storage. Add an extension name
and the read hooks when the logical type must round-trip:

```julia
import ArrowTypes

struct AccountID
    value::Int64
end

const ACCOUNT_ID = Symbol("JuliaLang.Example.AccountID")

ArrowTypes.ArrowType(::Type{AccountID}) = Int64
ArrowTypes.toarrow(id::AccountID) = id.value
ArrowTypes.arrowname(::Type{AccountID}) = ACCOUNT_ID
ArrowTypes.JuliaType(::Val{ACCOUNT_ID}, ::Type{Int64}, metadata) = AccountID
ArrowTypes.fromarrow(::Type{AccountID}, value::Int64) = AccountID(value)

Arrow.write("accounts.arrow", (id = AccountID.(1:3),))
table = Arrow.Table("accounts.arrow")
getfield.(table.id, :value) == [1, 2, 3] # true
```

The lowering and restoration apply recursively to top-level values and values
nested in lists, tuples and fixed-size lists, structs, maps,
dictionary-encoded values, and freshly synthesized heterogeneous Unions.
Arrow writes `arrowname` and `arrowmetadata` as standard extension metadata.
On read, it uses `JuliaType` and then `fromarrow` or `fromarrowstruct`. If the
current process has no mapping for an extension name, Arrow warns and returns
the ordinary storage values instead.
Defining `ArrowKind` alone is not a supported way to select an Arrow 3.0
physical layout. Use the `ArrowType` and `toarrow` lowering interface.

[`Arrow.DictEncode`](@ref) remains the opt-in wrapper for a newly written
dictionary-encoded column. Its pool values use the same recursive ArrowTypes.jl
mapping. A fully read top-level dictionary also retains its pool for rewrite.

## Supported write types

The writer accepts fixed-width integers and floats, `Bool`, strings, supported
`Dates` values, lists of supported core values, top-level `NamedTuple` struct
columns, and fresh heterogeneous Julia Union columns whose members are
writable at that nesting depth. Fresh heterogeneous Unions use the canonical
dense Arrow Union layout. See [Type mapping when writing](@ref) for the
complete table.

A top-level `NamedTuple` column may use `Union{Missing, T}`. Arrow writes the
outer missing state in the Struct validity bitmap and keeps each child's
declared nullability unchanged.

Declared Unions may contain up to 32 members. Runtime writer or storage
inference is limited to 8 distinct types across all partitions. This includes
abstract ArrowTypes.jl storage, abstract or `Any` dictionary values, and
abstract retained ArrowTypes.jl targets. Declare the intended Union when a
column needs more runtime types.

A fresh unresolved abstract declaration keeps concrete subtype writer evidence
across all partitions. A concrete subtype's extension metadata is not discarded.
Heterogeneous subtype evidence uses an explicit Union and the same 8-type
inference limit.

A plain concrete struct whose fields are supported can use ArrowTypes.jl's
default `StructKind` mapping. Without extension hooks, it reads back as
ordinary Struct storage rather than the original Julia type. To select a
different stable representation, map the struct to a supported storage type
with `ArrowTypes.ArrowType` and `ArrowTypes.toarrow`, or convert it to a
`NamedTuple` or separate columns. An `ArrowKind` override alone does not select
an arbitrary Arrow 3.0 physical layout. `Arrow.ToTimestamp` was removed; define
an ArrowTypes.jl lowering or convert zoned values before writing.

## Schema retention

When the source is an `Arrow.Table` or `Arrow.Stream`, the writer keeps
compatible details from the source schema. These details include temporal
units, byte and list widths, composite descriptors, nullability, field
metadata, schema metadata, and top-level dictionary index types and category
order. A column that was replaced with an incompatible Julia type is rejected
instead of being silently written under the old schema.

A fresh Julia column with a heterogeneous declared `Union` element type is
synthesized as a canonical dense Arrow Union. Materialization of an existing
unregistered Arrow Union discards its child type IDs and offsets. Such a
retained Union still fails clearly when rewritten from an `Arrow.Table`; the
writer does not invent new routing under the retained schema. A registered
ArrowTypes.jl logical type whose storage is a Union retains enough writer-side
type evidence to reconstruct the original child routing, including dense or
sparse mode and type IDs. Sparse children use canonical hidden placeholder
values outside their active rows. When `JuliaType` returns an abstract target,
a concrete writer subtype may omit an extension identity or use the retained
parent's identity. A different explicit identity is rejected instead of being
silently relabeled. A retained nested Dictionary also fails because its pool is
lost. Top-level dictionaries of scalar or composite values are supported after
a full read. A scan result can lack the hidden source pool, so an ordered
dictionary from such a result also fails clearly. A nullable `Dictionary<Null>`
with an unknown extension fails
closed because materialization cannot retain valid-index versus null-index
provenance. Exact buffer sharing, overlapping ListView ranges, and Run-End
Encoding segmentation are not retained; the writer emits a canonical layout
with the same public-domain values and schema type.

Registered public-domain values can rebuild compatible retained binary, list,
date-like, duration, wide-decimal, and interval descriptors. The retained
descriptor controls widths, sizes, units, and child fields. Values that do not
meet those exact constraints fail with `ArgumentError`. A retained Map with
`keysSorted=true` also rejects a replacement row whose keys are not sorted.

The removed `maxdepth` keyword is not replaced by an unbounded writer.
Recursive ArrowTypes.jl storage schemas and recursive value containers throw
`ArgumentError`, as does custom mapping nesting beyond the fixed depth of 64.
To keep composite descriptors from allocating in proportion to untrusted
schema width, `JuliaType` receives exact `NTuple{N,T}` fixed-list storage when
`N` is at most 1024 and the compact `Tuple{Vararg{T}}` family above that limit.
Exact-arity registrations above the limit remain unregistered. Extension
Structs receive an exact `NamedTuple` storage signature through 1024 children
only when child names are unique, contain no embedded NUL, already exist as
Julia `Symbol`s, are at most 4096 UTF-8 bytes each, and use at most 64 KiB in
total. Otherwise the labelled Struct remains unknown and reads as ordered
`Pair` storage. To preserve ArrowTypes.jl Tuple storage, one bounded exception
may intern positional child names only when the complete sequence is exactly
`"1"`, `"2"`, …, `string(N)` for `N ≤ 1024`. Unknown extension labels return
before this check. Arbitrary or partly positional Struct names are not interned.
Any writer-side `ArrowType` result that is a concrete tuple above the same limit
is rejected before writer specialization. This includes ArrowTypes.jl's default
mapping for a tuple value. A custom hook itself remains trusted Julia code and
must return normally for Arrow to validate its result.

Retained null-parent child slots now use direct length-based construction.
Null-only fixed-size lists and inactive sparse-Union children no longer expand
into one Julia placeholder per hidden element.

## Names and imports

Arrow 3.0 uses a small export surface. Qualify package functions:

```julia
using Arrow

table = Arrow.Table("data.arrow")
Arrow.write("copy.arrow", table)
```

`ArrowTypes` is no longer exported. Import ArrowTypes.jl directly when
defining mappings. `Arrow.ArrowTypes` remains available as a qualified
compatibility binding.

Core schema names now stay as `String` values. `Arrow.Table` converts only
top-level Tables.jl column names to `Symbol`, after it preflights the complete
schema against a 4096-byte per-name limit, a 65,536-novel-name limit, and a
1-MiB novel-name byte budget. Unknown ArrowTypes extension labels are checked
without interning them. The built-in `JuliaLang.Symbol` extension rejects a
novel IPC payload with `ValidationError` instead of interning input-controlled
process-global state. This is an intentional behavior change: an input that a
prior Arrow.jl release read by interning its payload can now fail.

`Arrow.getmetadata` was removed. Arrow 3.0 uses DataAPI.jl metadata methods.
Add DataAPI.jl as a direct dependency of code that imports it:

```julia
import Pkg
Pkg.add("DataAPI")
```

```julia
using Arrow, DataAPI

collect(DataAPI.metadatakeys(table))
DataAPI.metadata(table, "key")
DataAPI.colmetadata(table, :column, "key")
```

## New features

Arrow 3.0 adds:

- `Tables.Scan` pushdown for projection, filters, limits, and offsets.
- Recursive ArrowTypes.jl custom and extension-type mappings. Filters over a
  field that contains a registered logical type at any depth evaluate over the
  public materialized values because the interface does not require its
  storage lowering to preserve Julia comparison semantics.
- Sparse byte-range reads through [`Arrow.AbstractArrowSource`](@ref).
- A CloudStore.jl extension for object storage.
- Arrow C data and C stream import and export.
- Arrow StringView and BinaryView support.
- Stronger validation and resource limits for untrusted IPC input.

See the [User Manual](@ref) for examples and the [API Reference](@ref) for the
supported entry points.
