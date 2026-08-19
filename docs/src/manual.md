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

# User Manual

[Apache Arrow](https://arrow.apache.org) specifies a columnar memory layout
and an interprocess (IPC) serialization format for it, so that table data
can be shared across languages and processes without conversion. Arrow.jl
is a pure Julia implementation of that
[specification](https://arrow.apache.org/docs/format/Columnar.html): it
reads and writes the IPC stream and file formats, exchanges in-memory data
with other implementations through the C data and C stream interfaces, and
presents everything to Julia through the [Tables.jl](https://tables.juliadata.org)
interface.

```julia
using Arrow, Tables

Arrow.write("data.arrow", (a = [1, 2, 3], b = ["x", "y", missing]))
tbl = Arrow.Table("data.arrow")
tbl.a            # Vector{Int64}
tbl.b            # Vector{Union{Missing, String}}
```

## Reading

### `Arrow.Table`

[`Arrow.Table`](@ref) reads an IPC source into columns. The source may be a
file path, an `IO`, a `Vector{UInt8}` of IPC bytes, or a byte-range fetcher
([Reading remote and partial files](@ref)). Both IPC formats are accepted
and detected automatically: the *file* format (`ARROW1` magic, random
access, optional footer statistics) and the *stream* format.

```julia
tbl = Arrow.Table("data.arrow")           # a path: memory-mapped file
tbl = Arrow.Table(io)                      # an IO: read to the end
tbl = Arrow.Table(bytes)                   # IPC bytes already in memory
```

`Arrow.Table` satisfies the Tables.jl columns interface, so it works with
every Tables.jl-aware sink and consumer:

```julia
Tables.columnnames(tbl)                    # (:a, :b)
Tables.schema(tbl)
tbl.a                                      # property access = a column
Tables.getcolumn(tbl, :b)
length(tbl)                                # number of rows
DataFrame(tbl)                             # any Tables.jl sink
```

Columns are **materialized**: each column is a plain Julia `Vector` with a
concrete element type determined by the Arrow schema (see [Type
mapping when reading](@ref)). A `Table` therefore does not borrow the source bytes after
it is constructed, and its columns behave like any other Julia vectors.

### Memory mapping and `close!`

A file path is memory-mapped by default (`mmap=true`), so reading a large
file does not copy it into RAM up front; pass `mmap=false` to read the file
into memory instead. A memory map is released when the last reference to it
is garbage collected. To release it deterministically — required on Windows
before a still-mapped file can be deleted, and useful anywhere for prompt
resource release — call [`Arrow.close!`](@ref):

```julia
tbl = Arrow.Table("data.arrow")
# ... use tbl ...
Arrow.close!(tbl)                          # unmaps NOW; tbl's columns remain usable
rm("data.arrow")
```

`close!` is idempotent. Because a `Table`'s columns are copies, a closed
`Table` remains fully usable; a closed [`Arrow.Stream`](@ref) refuses
further iteration cleanly.

### `Arrow.Stream`

[`Arrow.Stream`](@ref) iterates a source one record batch at a time; each
iteration yields an `Arrow.Table` for that batch — the tool for pipelines
that process batches independently:

```julia
for batch in Arrow.Stream("big.arrow")
    process(batch)                         # batch isa Arrow.Table
end
```

A `Stream` satisfies `Tables.partitions` (each batch is one partition), so
partition-aware sinks see the source's batch structure —
`Arrow.write(sink, Arrow.Stream(...))` writes one record batch per input
batch.

Memory: only the memory-mapped **file-format** path (the default when you
pass a path to a file-format source) avoids holding the whole source —
batches are decoded from the mapping one at a time, so a consumer's loop
over such a `Stream` holds one batch of columns at a time (plus the file's
dictionaries), and that is the way to process a file larger than RAM. A
**stream-format** source is read to the end and every batch is decoded when
the `Stream` is constructed. A file-format `IO` or byte-vector input is
also read to the end (the whole source is held in memory), but its record
batches are still decoded lazily, one per iteration. `Arrow.write`
materializes every partition before writing (see [Writing](@ref)), so it
does not bound memory either.

### Metadata

Schema-level and per-column key/value metadata carried in the IPC schema is
readable through the [DataAPI.jl](https://github.com/JuliaData/DataAPI.jl)
metadata interface:

```julia
DataAPI.metadatakeys(tbl)
DataAPI.metadata(tbl, "key")
DataAPI.colmetadatakeys(tbl, :a)
DataAPI.colmetadata(tbl, :a, "key")
```

### Type mapping when reading

Scalar Arrow types map to Julia element types by a closed rule over the
schema — never by inspecting values, so an all-`missing` or zero-row scalar
column has the same element type as a populated one. A nullable Arrow field
maps to `Union{Missing, T}`.

| Arrow type | Julia element type |
|---|---|
| Int8…Int64, UInt8…UInt64 | the same-width `Integer` |
| Float16/32/64 | `Float16`/`Float32`/`Float64` |
| Bool | `Bool` |
| Utf8, LargeUtf8, Utf8View | `String` |
| Binary, LargeBinary, BinaryView, FixedSizeBinary | `Vector{UInt8}` |
| Date32 | `Dates.Date` |
| Date64 | `Dates.DateTime` |
| Timestamp (second, millisecond) | `Dates.DateTime` |
| Timestamp (microsecond, nanosecond) | `Int64` (raw storage — `DateTime` cannot represent it) |
| Time32/Time64 | `Dates.Time` |
| Duration | `Dates.Second`/`Millisecond`/`Microsecond`/`Nanosecond` by unit |
| Decimal32/64 | `Int32`/`Int64` (unscaled integer storage) |
| Decimal128/256 | `Vector{UInt8}` (raw little-endian storage) |
| Interval | `Int32` (year-month) or a `NamedTuple` (day-time, month-day-nano) |
| Dictionary-encoded scalar | the mapping of the *value* type (indices are resolved) |

Composite and wrapper layouts are read on the dynamic path (each row is
built as a Julia value), and their column element type is likewise derived
from the schema — the declared row container — so it too is the same for a
zero-row, an all-`missing`, and a populated column:

| Arrow type | Element type |
|---|---|
| List, LargeList, FixedSizeList, ListView | `Vector{Any}` (rows are vectors of the child's values) |
| Struct | `Vector{Pair{String,Any}}` (rows are ordered name => value pairs) |
| Map | `Vector{Pair{Any,Any}}` |
| Union | the join of the children's element types when it is concrete (a homogeneous union reads as that type); otherwise `Any`, narrowed from the rows |
| Run-end encoded | the *values* child's element type (runs are expanded) |
| Null | `Missing` |

Two refinements. The `Dates` conversions above apply at the top level and
through dictionary encoding; a temporal type nested under a run-end-encoded
or union wrapper stays in its raw integer storage. And field nullability is
*advisory* in Arrow (the reference implementation and the conformance
corpus accept a null under a `nullable=false` field), so a column that
holds a null its field did not declare reads as `Union{Missing, T}` rather
than failing; a conforming column keeps its declared, `Missing`-free type.
Sub-millisecond timestamps stay as raw integers everywhere rather than
silently truncating into `DateTime`; the same rule applies when writing.

### Scan pushdown

`Arrow.Table` accepts a `Tables.Scan` — a plain-data description of which
columns to keep, which rows qualify, and how many — and pushes it down into
the reader:

```julia
using Tables: Scan, col, coleq, in_, isnull

scan = Scan(select = (:id, :amount),
            filter = (col(:amount) > 100) & !isnull(col(:id)),
            limit = 1_000)
tbl = Arrow.Table("orders.arrow"; scan = scan)
```

* `select`: a reference or tuple of select items (`ref`, `ref => name`,
  `ref => Type`, `ref => Type => name`; refs are `Symbol`, `String`, `Int`,
  `Regex`, `Tables.Not`, `Tables.All`). Only the selected columns (and the
  columns the filter references) are decoded; everything else is skipped
  without being sliced, decompressed, or validated.
* `filter`: an expression over `Tables.col` — comparisons against literals
  (`>`, `>=`, `<`, `<=`, `coleq`, `colne`), `in_`, `isnull`, string
  predicates, combined with `&`, `|`, `!`. A row is kept iff the predicate
  is exactly `true` (`missing` excludes, SQL-style).
* `limit`/`offset`: applied to qualifying rows.

On the file format, batches whose footer statistics prove no row can match
the filter are never fetched or decoded, and exact `limit`/`offset` windows
skip whole batches when there is no filter. On the stream format the scan is
applied after decode with identical results. A scan whose filter literal has
no exact storage representation (a cross-domain or out-of-range value), or
whose projection is empty (`select = ()`), falls back to reading the whole
source and evaluating over the converted public values.

Batch pruning uses per-batch statistics (row count, null count, min, max
per column) carried in the file's footer schema metadata under the key
`JuliaArrow:batch_statistics.v1`, in the value layout of the Arrow
project's statistics schema — other readers see ordinary metadata.
[`Arrow.write`](@ref) does not embed them; files that carry them prune,
files that do not are simply scanned batch by batch.

### Reading remote and partial files

The file format is random-access: the footer says where every batch and
buffer lives, so a reader that can fetch byte ranges — from object storage,
over HTTP, or from a local file it prefers not to map whole — needs only
the ranges its scan touches. [`Arrow.RangedSource`](@ref) is that fetcher
contract: a function `fetch(offset, len) -> Vector{UInt8}` over an object
of known total length. [`Arrow.RangedFile`](@ref) wraps one with the fetch
protocol (the eight-byte head magic, then the footer from the tail, batch
windowing from the footer's block metadata, dictionary bodies only for the
columns in play, coalesced body ranges for exactly the decoded columns):

```julia
src = Arrow.RangedSource(Int64(objectsize)) do offset, len
    fetchbytes(url, offset, len)           # your transport: S3, HTTP, ...
end
tbl = Arrow.Table(src; scan = Scan(select = (:id,), filter = col(:day) > 20))
```

Overriding `Arrow.fetchranges(::RangedSource, ranges)` lets a transport
issue the planned ranges concurrently; the default fetches them serially.
`RangedFile(src; tailbytes, coalesce_gap, limits)` tunes the initial tail
read, how close two ranges must be to merge into one request, and the
resource limits. Arrow.jl has no HTTP or cloud dependency of its own — a
transport package only has to construct a `RangedSource`.

## Writing

### `Arrow.write`

[`Arrow.write`](@ref) writes any Tables.jl-compatible source to a path or
an `IO`:

```julia
Arrow.write("out.arrow", tbl)              # file format (ARROW1 + footer)
Arrow.write(io, tbl; file = false)          # stream format
Arrow.write("out.arrow", tbl; compress = :zstd)     # or :lz4
Arrow.write("out.arrow", tbl;
    metadata = ["source" => "sensor-7"],
    colmetadata = Dict(:temp => ["unit" => "C"]))
```

Each `Tables.partitions` partition of the source becomes one record batch,
so `Arrow.write(sink, Arrow.Stream(path))` and
`Arrow.write(sink, Tables.partitioner(...))` preserve batch structure. The
writer is eager and whole-buffer: every partition is materialized, encoded
and validated in memory, then the complete IPC bytes are written to the
sink once — it holds the whole table, so it is not a bounded-memory path
for sources larger than RAM (there is no incremental writer in 3.0).

`compress` applies per-buffer LZ4 frame or Zstandard compression as
defined by the IPC specification (buffers that do not shrink are stored
raw). Compressed files are readable by every implementation that supports
IPC compression.

### Dictionary encoding

Wrap a column in [`Arrow.DictEncode`](@ref) to write it dictionary-encoded
(a pool of unique values plus integer indices), which is what a
categorical or low-cardinality string column wants:

```julia
Arrow.write("out.arrow", (region = Arrow.DictEncode(regions), sales = sales))
```

Reading a dictionary-encoded column resolves the indices: the column comes
back as its value type. When a `Table` read from Arrow is written again,
its dictionary encoding is preserved.

### Type mapping when writing

Writing maps Julia element types to Arrow types. The *core* domain — what
can appear at any nesting depth — is:

| Julia element type | Arrow type |
|---|---|
| `Int8`…`Int64`, `UInt8`…`UInt64` | the same-width integer |
| `Float16/32/64` | the same-width float |
| `Bool` | Bool |
| `String` | Utf8 |
| `Vector{T}` for core `T` (including `Vector{UInt8}`) | List of the mapping of `T` |
| `Union{Missing, T}` for core `T` | the mapping of `T`, nullable |

At the *top level* of a column the facade adds:

| Julia element type | Arrow type |
|---|---|
| any other `AbstractString` (e.g. `SubString`) | Utf8 |
| `Dates.Date` | Date32 |
| `Dates.DateTime` | Timestamp (millisecond) |
| `Dates.Time` | Time64 (nanosecond) |
| `Dates.Second/Millisecond/Microsecond/Nanosecond` | Duration of that unit |
| `NamedTuple` whose fields are core columns | Struct (no top-level nulls — wrap fields as nullable children instead) |
| `Arrow.DictEncode` over a core column | Dictionary of the wrapped mapping |
| `ArrowStrings.ArrowStringVector` | Utf8View, **zero-copy** — the column's memory is the Arrow array (see below) |

The top-level conversions do not recurse: a `Vector{Date}` inside a list, a
`Date` or `SubString` field of a `NamedTuple`, or `DictEncode` over dates
are refused with an `ArgumentError` naming the element type. A column with
element type `Any` is narrowed once (recovering list columns of a common
element type) and refused if it cannot be narrowed to a writable type.
When the source is an `Arrow.Table` or `Arrow.Stream`, the writer *retains*
the Arrow schema it was read with — temporal units, dictionary encoding,
nested list descriptors, nullability, and metadata all survive a read/write
round trip.

### ArrowStrings columns

[ArrowStrings.jl](https://github.com/apache/arrow-julia/tree/main/src/ArrowStrings)
(a separate package that lives in this repository) defines
`ArrowString`, a 16-byte string value that *is* an Arrow StringView entry
(inline up to 12 bytes, otherwise a prefix plus buffer index and offset),
and `ArrowStringVector`, a column of them over a set of byte buffers —
which *is* an Arrow Utf8View array's memory. CSV.jl parses string columns
into this representation, so `Arrow.write` on such a column wraps its
payload vector and buffers as the Arrow column without copying or
materializing a single `String`.

## Validation

Every batch is validated before it is exposed by a read or emitted by a
write: buffer arity and byte lengths against the schema (structural), and
offset monotonicity, dictionary index domains, union type ids and the other
data-intrinsic invariants (semantic). Metadata is verified by a generated
FlatBuffers shape verifier before any of it is used, and resource limits
(metadata size, body size, allocation budget, nesting depth) are enforced
before any metadata-directed allocation, so a corrupt or hostile file
produces a clean `ValidationError` rather than a crash or an unbounded
allocation.

Content-policy checks that the reference implementation treats as
advisory — UTF-8 well-formedness of string bytes, the `nullable=false`
declaration on a field, canonical zero padding of bitmaps — are not
enforced by default, matching the behavior of the other Arrow
implementations on the ecosystem's own conformance files.

## The C data interface

Arrow's [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
and [C stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
move columns and record batches between implementations in the same
process without copying: a pair of C structs (`ArrowSchema`, `ArrowArray`)
or a stream struct (`ArrowArrayStream`) is filled by a producer and read by
a consumer, and ownership is transferred with a release callback.

Arrow.jl exposes both interfaces at the level of the engine's column
representation (a `Field` describing the type and an `ArrayData` holding
the buffers), which every Julia column read by `Arrow.Table` is built from:

* `Arrow.to_c_data(field, data) -> (schemaptr, arrayptr)` exports one
  column; the structs stay valid until the consumer calls their `release`
  callbacks, and `Arrow.reap!()` reclaims the export bookkeeping afterward.
* `Arrow.from_c_data(schemaptr, arrayptr) -> (field, data)` imports one
  column, *moving* the array (its source `release` is nulled, as the spec
  requires). The imported buffers stay valid as long as the returned data is
  reachable; `Arrow.close!` on any imported buffer, or `Arrow.release!` on
  the import's owner, runs the producer's release callback exactly once.
* `Arrow.export_stream!(streamptr, schema, batches)` fills a caller-owned
  `ArrowArrayStream`; `Arrow.from_c_stream(streamptr)` imports one and
  yields record batches through `Arrow.nextbatch!`.

For example, handing a column to PyArrow in-process through PythonCall:

```julia
using Arrow, PythonCall
pa = pyimport("pyarrow")

f, d = Arrow.ArrowCore.fromjulia("x", [1, 2, missing, 4])
sp, ap = Arrow.to_c_data(f, d)
pyarr = pa.Array._import_from_c(UInt(ap), UInt(sp))     # PyArrow now owns the structs
pyarr.to_pylist()                                        # [1, 2, None, 4]
```

The conformance suite under `conformance/` round-trips every layout the
format defines through PyArrow over exactly this path, in both directions.

## Compiling with JuliaC `--trim`

Arrow.jl's engine is designed to compile under JuliaC's `--trim=safe`:
type descriptors are runtime values, layout dispatch goes through closed
`isa` ladders, and the value-domain entry points (reading, C data
import/export, and the typed accessors `Arrow.ArrowCore.materialize(::Type{T},
field, data)`) are statically resolvable. The repository's
`test/trim_compile_tests.jl` gate holds that at zero verifier errors and
warnings. The dynamic facade conveniences (property access on `Arrow.Table`,
`NamedTuple` rows) are not part of that guarantee.

## Differences from Arrow.jl 2.x

Arrow.jl 3.0 is a new implementation. The everyday surface — `Arrow.Table`,
`Arrow.Stream`, `Arrow.write`, `Arrow.DictEncode`, Tables.jl integration,
compression, metadata — is the same in spirit, with these differences:

* **Columns are plain `Vector`s.** 2.x returned lazy `ArrowVector` views
  over the mapped bytes; 3.0 materializes columns with concrete element
  types (the mapping tables above), and the source may be released with
  `Arrow.close!` at any time afterward.
* **Scan pushdown and byte-range reads** (`Tables.Scan`, `RangedSource`,
  `RangedFile`) are new.
* **Not present in 3.0**: `Arrow.Writer`/`Arrow.append` (incremental and
  append-to-file writing), multithreaded encoding (`ntasks`), the
  `convert=false` lazy read mode, `Arrow.ToArrow`, and ArrowTypes.jl
  custom-type serialization: a `NamedTuple` column is written as a Struct
  column of its fields, and other Julia structs are not writable. Big-endian
  and delta-dictionary IPC streams are refused.
* **The C data and C stream interfaces** are new.
