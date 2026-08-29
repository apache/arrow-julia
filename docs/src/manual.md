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
interface (Tables.jl 1.14 or later, for `Tables.Scan`).

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
tbl = Arrow.Table("data.arrow")           # a file-format path: memory-mapped
tbl = Arrow.Table(io)                      # an IO: read to the end
tbl = Arrow.Table(bytes)                   # IPC bytes already in memory
```

`Arrow.Table` satisfies the Tables.jl columns interface, so it works with
every Tables.jl-aware sink and consumer:

```julia
Tables.columnnames(tbl)                    # [:a, :b]
Tables.schema(tbl)
tbl.a                                      # property access = a column
Tables.getcolumn(tbl, :b)
length(tbl)                                # number of rows
```

Pass `tbl` directly to any Tables.jl-compatible sink.

Columns are **materialized**: each column is a plain Julia `Vector` with a
concrete element type determined by the Arrow schema (see [Type
mapping when reading](@ref)). A `Table` therefore does not borrow the source bytes after
it is constructed, and its columns behave like any other Julia vectors.

### Memory mapping and `release!`

A path to a file-format source is memory-mapped by default (`mmap=true`), so
reading a large file does not copy it into RAM up front; pass `mmap=false`
to read it into memory instead (a stream-format path is always read into
memory). A memory map is released when the last reference to it
is garbage collected. To release it deterministically — required on Windows
before a still-mapped file can be deleted, and useful anywhere for prompt
resource release — call [`Arrow.release!`](@ref):

```julia
tbl = Arrow.Table("data.arrow")
# ... use tbl ...
Arrow.release!(tbl)                          # unmaps NOW; tbl's columns remain usable
rm("data.arrow")
```

`release!` is idempotent. Because a `Table`'s columns are copies, a released
`Table` remains fully usable; a released [`Arrow.Stream`](@ref) refuses
further iteration cleanly. Every yielded `Table` shares the source lifetime:
releasing a batch closes its parent `Stream`, while the batch's materialized
columns remain usable.

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

Memory: only the memory-mapped **file-format** path avoids holding the whole
source. Batches decode from the map one at a time, so a loop over such a
`Stream` holds one batch of columns plus the file's dictionaries. That is the
way to process a file larger than RAM. A
`Stream` security budget (the `max_total_allocated_bytes` field of
[`Arrow.Limits`](@ref)) stays cumulative across every batch it yields, even
after the consumer drops that batch. Raise the limit explicitly for a trusted
large file whose total decoded allocation exceeds the default; the same
`limits` keyword applies to [`Arrow.Table`](@ref):

```julia
trusted_limits = Arrow.Limits(max_total_allocated_bytes = 2 * 1024^3)
stream = Arrow.Stream("big.arrow"; limits = trusted_limits)
```

A **stream-format** source is read to the end and every batch is decoded when
the `Stream` is constructed. A file-format `IO` or byte-vector input is
also read to the end (the whole source is held in memory), but its record
batches are still decoded lazily, one per iteration. `Arrow.write`
materializes every partition before writing (see
[Writing](@ref "manual-writing")), so it does not bound memory either.

### Metadata

Schema-level and per-column key/value metadata carried in the IPC schema is
readable through the [DataAPI.jl](https://github.com/JuliaData/DataAPI.jl)
metadata interface. Code that imports DataAPI must add it as a direct
dependency:

```julia
import Pkg
Pkg.add("DataAPI")
```

```@example metadata_api
using Arrow, DataAPI

io = IOBuffer()
Arrow.write(io, (a = [1, 2],);
    metadata = ["source" => "docs"],
    colmetadata = Dict(:a => ["unit" => "count"]))
metadata_table = Arrow.Table(take!(io))

(
    schema_keys = collect(DataAPI.metadatakeys(metadata_table)),
    source = DataAPI.metadata(metadata_table, "source"),
    column_keys = collect(DataAPI.colmetadatakeys(metadata_table, :a)),
    unit = DataAPI.colmetadata(metadata_table, :a, "unit"),
)
```

Arrow IPC permits duplicate field names. Use integer column positions for
such a table. Name-based property access, `Tables.getcolumn`, and DataAPI
column metadata access are ambiguous and throw an `ArgumentError`. Scan
pushdown also refuses duplicate names. Positional access preserves every
column and its metadata:

```julia
Tables.getcolumn(tbl, 1)
DataAPI.colmetadata(tbl, 1, "key")
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
| Timestamp (second, millisecond) | `Dates.DateTime` (UTC instant; a declared timezone stays in the retained schema — loading TimeZones.jl reads these as `ZonedDateTime` instead) |
| Timestamp (microsecond, nanosecond) | `Int64` (raw storage — `DateTime` cannot represent it) |
| Time32/Time64 | `Dates.Time` |
| Duration | `Dates.Second`/`Millisecond`/`Microsecond`/`Nanosecond` by unit |
| Decimal32/64 | `Int32`/`Int64` (unscaled integer storage) |
| Decimal128/256 | `Vector{UInt8}` (raw native-endian storage) |
| Interval | `Int32` (year-month) or a `NamedTuple` (day-time, month-day-nano) |
| Dictionary-encoded scalar | the mapping of the *value* type (indices are resolved) |

Composite layouts (list, struct, map, union) are read on the dynamic path
(each row is built as a Julia value), and their column element type is
derived from the schema — the declared row container — so it too is the
same for a zero-row, an all-`missing`, and a populated column, with two
exceptions: the heterogeneous-union case in the Union row, and nulls the
field did not declare (below). The two wrapper layouts are transparent:
a dictionary-encoded or run-end-encoded column takes the route and the
element type of its value child (a closed scalar child keeps the typed
path):

| Arrow type | Element type |
|---|---|
| List, LargeList, FixedSizeList, ListView, LargeListView | `Vector{Any}` (rows are vectors of the child's values) |
| Struct | `Vector{Pair{String,Any}}` (rows are ordered name => value pairs) |
| Map | `Vector{Pair{Any,Any}}` |
| Union | the join of the children's element types when it is concrete (a homogeneous union reads as that type); otherwise `Any`, narrowed from the rows |
| Run-end encoded | the *values* child's element type (runs are expanded) |
| Null | `Missing` |

Three refinements. A list-family field with a registered ArrowTypes.jl child
uses a row vector with that restored child element type; this is the same for
a zero-row and a populated column. The `Dates` conversions above apply at the top level and
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
using Tables: Scan, col, colcmp, colin, isnull

scan = Scan(select = (:id, :amount),
            filter = (col(:amount) > 100) & !isnull(col(:id)),
            limit = 1_000)
tbl = Arrow.Table("orders.arrow"; scan = scan)
```

* `select`: a reference or tuple of select items (`ref`, `ref => name`,
  `ref => Type`, `ref => Type => name`; refs are `Symbol`, `String`, `Int`,
  `Regex`, `Tables.Not`, `Tables.All`). Only the selected columns and the
  columns the filter references are decoded. For every decoded batch, complete
  node and buffer metadata is validated first. Other columns' body buffers are
  not sliced, decompressed, content-validated, or materialized.
* `filter`: an expression over `Tables.col` — comparisons against literals
  (`>`, `>=`, `<`, `<=`, `colcmp`), `colin`, `isnull`, string
  predicates, combined with `&`, `|`, `!`. A row is kept iff the predicate
  is exactly `true` (`missing` excludes, SQL-style).
* `limit`/`offset`: applied to qualifying rows.

On the file format, batches whose footer statistics prove no row can match
receive no dedicated metadata or body request and are not decoded; the filter
is evaluated batch by batch and `limit`/`offset` compose exactly over the
qualifying rows. Without a filter, whole batches outside the window are never
decoded, and with one decoding stops as soon as the window is full. On the stream format
the scan is applied after decode with identical results.

A scan whose filter has no semantics-preserving storage representation falls
back to reading the whole source and evaluating over converted public values.
This includes an inexact literal and a temporal conversion that aliases values
or wraps ordering. The detailed rules:

* Temporal lowering: Date64 and millisecond Timestamp equality can lower, but
  their ordered comparisons stay public; Timestamp-second and Time predicates
  also stay public. Duration lowering accepts a literal in the column unit or
  a coarser fixed unit, but a finer unit stays public because Julia can
  overflow while promoting stored values.
* Set membership: temporal Tuple and Array membership follows the same
  equality rules. Set members must also have the column's canonical public
  type, which preserves `isequal` and hashing. A custom membership object
  stays public because Arrow cannot transform it without changing its `in`
  semantics.
* Empty projection: `select = ()` stays on the ranged path. It preserves the
  selected row count without fetching output column bodies.

Filters over a field that contains a registered ArrowTypes.jl extension value
at any depth are evaluated over the restored public values. The ArrowTypes
interface does not require `toarrow` to preserve Julia comparison semantics,
so lowering an arbitrary custom literal for physical pushdown would be
incorrect. Projection and the other scan operations still apply normally.

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
the ranges its scan touches. [`Arrow.AbstractArrowSource`](@ref) is that
contract: a byte-addressable object of known length, read through
[`Arrow.sourcelength`](@ref) and [`Arrow.readrange`](@ref). Given one,
`Arrow.Table` with a scan first fetches and caches a tail window. That window
normally contains the complete Footer. If it does not, one exact cached
follow-up fetch retrieves the Footer. The reader then keeps only the batches
that the Footer's statistics and the scan's window allow, fetches those
batches' metadata, and requests ranges that cover the selected and
filter-referenced buffers. The common path has three sequential request rounds:
tail/Footer, metadata, and body. An oversized Footer adds a fourth round. Each
metadata or body round can contain multiple coalesced requests, which a
concurrent source can issue in parallel. Tail reads and coalescing may
physically over-read unrequested bytes.

With [CloudStore.jl](https://github.com/JuliaServices/CloudStore.jl)
loaded, a `CloudStore.Object` (S3 or Azure Blob Storage) is such a source
directly, and its planned ranges are requested concurrently:

```julia
using Arrow, Tables, CloudStore
obj = CloudStore.Object(bucket, "events/2024-05.arrow"; credentials)
tbl = Arrow.Table(obj; scan = Scan(select = (:id,), filter = col(:day) > 20))
```

Any other transport is two methods away:

```julia
struct HTTPSource <: Arrow.AbstractArrowSource
    url::String
    length::Int64
end
Arrow.sourcelength(s::HTTPSource) = s.length
Arrow.readrange(s::HTTPSource, offset, len) = fetchbytes(s.url, offset, len)  # a Range GET

tbl = Arrow.Table(HTTPSource(url, objectsize); scan = Scan(select = (:id,)))
```

Overriding [`Arrow.concurrentreads`](@ref) lets Arrow issue planned ranges
concurrently through `readrange`. One reader samples that value once, clamps
it by the `max_concurrent_reads` field of [`Arrow.Limits`](@ref), and shares
the cap across all operations on that read. Separate reads are
independent; enforce a transport-wide cap inside `readrange` when required.
Results are placed by request, and the default reads one at a time.
Without a scan the whole object is read, as is a stream-format object (no
footer) and a scan that cannot be pushed down. Arrow.jl has no HTTP or
cloud dependency of its own.

## [Writing](@id manual-writing)

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
for sources larger than RAM. Use [`Arrow.Writer`](@ref) when tables must be
published incrementally.

`compress` applies per-buffer LZ4 frame or Zstandard compression as
defined by the IPC specification (buffers that do not shrink are stored
raw). Compressed files are readable by every implementation that supports
IPC compression.

### Incremental writing and stream append

[`Arrow.Writer`](@ref) publishes each table's batches before the next table
is supplied, so it holds only the current table in memory:

```julia
Arrow.Writer("out.arrow") do writer
    for table in tables
        Arrow.write(writer, table)
    end
end
```

The first table fixes the schema. Later tables must have the same columns in
the same order and compatible types. Closing the writer finishes the output;
an abandoned writer can leave a torn stream or a file without its footer.

[`Arrow.append`](@ref) extends an existing IPC stream in place. It refuses the
IPC file format, whose footer must be written when the file is closed. Changed
dictionary pools require a stream created with
`Arrow.Writer(...; file=false, dictreplacement=true)`.

### Dictionary encoding

Wrap a column in [`Arrow.DictEncode`](@ref) to write it dictionary-encoded
(a category pool plus integer indices), which is what a categorical or
low-cardinality string column wants:

```julia
Arrow.write("out.arrow", (region = Arrow.DictEncode(regions), sales = sales))
```

Reading a dictionary-encoded column resolves the indices: the column comes
back as its value type. When a `Table` read from Arrow is written again,
its dictionary encoding is preserved. Fresh `DictEncode` output coalesces
categories by exact Arrow storage identity. A retained rewrite preserves the
source pool prefix, including unused or duplicate physical categories, because
pool order and index meaning are part of the encoded data.

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
| `Union{T1, T2, ...}` with two or more non-`Missing` writable member types | canonical dense Union with one child per declared member type |
| `Missing` | Null |

The dense Union mapping applies to freshly supplied Julia data. Each member
uses the same recursive core or ArrowTypes.jl mapping that it would use at
that nesting depth. A `Missing` member is represented by a Null child. A
Union may have at most 32 declared members.

An abstract or `Any` element type does not declare those member types. When
Arrow.jl must infer writer or storage types from runtime values, it accepts at
most 8 distinct types across the complete column and all of its partitions.
This rule includes abstract ArrowTypes.jl storage, dictionary pools, and
retained registered ArrowTypes.jl columns. Use an explicit declared `Union`
when a column intentionally has more types. This separate limit bounds
schema-planning and compiler work for runtime-generated parametric types; it
does not reduce the 32-member declared Union limit.

If an abstract declaration has no `ArrowType` mapping or extension identity of
its own, Arrow uses its observed concrete subtypes as writer evidence. This
keeps a concrete subtype's extension metadata. Multiple observed subtypes form
an explicit Union under the same 8-type inference limit. An empty abstract
column still needs declared schema evidence because it has no runtime subtype.

At the *top level* of a column the facade adds:

| Julia element type | Arrow type |
|---|---|
| any other `AbstractString` (e.g. `SubString`) | Utf8 |
| `Dates.Date` | Date32 |
| `Dates.DateTime` | Timestamp (millisecond) |
| `Dates.Time` | Time64 (nanosecond) |
| `Dates.Second/Millisecond/Microsecond/Nanosecond` | Duration of that unit |
| `NamedTuple` whose fields are core columns | Struct; `Union{Missing, T}` adds parent validity while child nullability stays declared |
| `Arrow.DictEncode` over a writable column | Dictionary of the recursive mapping of its values |
| `ArrowStrings.StringVector` | Utf8View, **zero-copy** — the column's memory is the Arrow array (see below) |

These native facade conversions do not recurse through list or struct shapes:
a `Vector{Date}` inside a list, or a `Date` or `SubString` field of a
`NamedTuple`, is refused with an `ArgumentError` naming the element type. A
top-level `DictEncode` pool uses the same native column mapping as an ordinary
top-level column. The ArrowTypes.jl mappings described below do recurse. A
column with element type `Any` is narrowed once (recovering list columns of a
common element type) and refused if it cannot be narrowed to a writable type.
When the source is an `Arrow.Table` or `Arrow.Stream`, the writer retains the
compatible Arrow descriptor tree. Temporal units, byte and list widths,
Struct, Map, Run-End Encoding, nullability, field metadata, schema metadata,
and top-level dictionary index types and category order survive a read/write
round trip. Buffer sharing, overlapping ListView ranges, and exact run
segmentation are rebuilt into a canonical form without changing logical
values. A retained Map that declares sorted keys is rewritten only when each
row remains sorted.

A fresh Julia column with a heterogeneous declared `Union` element type is
synthesized as a canonical dense Arrow Union. This is distinct from rewriting
a retained Union. Once an unregistered Arrow Union is materialized, its
original child type IDs and offsets are no longer present in the Julia values.
Writing that retained Union from an `Arrow.Table` fails with a clear
`ArgumentError` instead of inventing new routing under the old schema.

A registered ArrowTypes.jl target is different. Its public-domain values keep
writer-side type evidence. When that type lowers to Union storage, Arrow.jl can
route each value back through the retained children and preserve external child
order, labels, type IDs, dense or sparse mode, nullability, and metadata. Sparse
children use canonical hidden placeholder values outside their active rows.
This also works when `JuliaType` returns an abstract read target and concrete
writer subtypes provide the storage mapping. A concrete subtype may omit an
extension identity or use the retained parent's identity. A different explicit
extension name or metadata is rejected instead of being silently relabeled. An
outer missing value uses a separate unmarked Null child.
If the logical storage Union already contains `Missing`, adding an outer missing
state is rejected because Arrow cannot distinguish the two states.

Registered values can also rebuild compatible retained storage descriptors.
This includes binary and binary-view widths, fixed-size binary, list and
fixed-size-list layouts, date and duration units, wide decimals, and interval
layouts. The retained descriptor remains authoritative. Byte widths, list
sizes, temporal exactness, child fields, and interval storage shapes are
checked before output is written. Concrete declared element types provide the
same schema evidence for empty and typed all-missing columns, so those columns
cannot bypass retained-schema checks.

A nested dictionary has the same fail-closed rule as an unregistered Union
because its pool is not retained. Top-level dictionaries, including
dictionaries of composite values, are retained on a full read. A scan result
may not carry the hidden source pool; an ordered dictionary then fails instead
of inventing category order. A nullable `Dictionary<Null>` with an unknown
extension also fails closed: after materialization, `missing` cannot say whether
the source row was a valid index into the Null pool or a null dictionary index.

### Custom and extension types

Arrow 3.0 applies [ArrowTypes.jl](https://github.com/apache/arrow-julia/tree/main/src/ArrowTypes)
mappings automatically. Import ArrowTypes.jl directly and declare it as a
dependency of the package that owns the custom type. `Arrow.ArrowTypes`
remains available as a qualified compatibility binding, and `ArrowTypes`
stays exported as an Arrow 2.x compatibility exception.

Define `ArrowType` and `toarrow` to lower a custom value to a supported storage
type. Define an extension name and the read hooks when the logical type must
round-trip:

```@example arrowtypes_account_id
using Arrow
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

io = IOBuffer()
Arrow.write(io, (id = AccountID.(1:3),); file = false)
table = Arrow.Table(take!(io))
values = getfield.(table.id, :value)
@assert values == [1, 2, 3]
values
```

Arrow applies `ArrowType` and `toarrow` recursively to top-level values and to
values nested in lists, tuples and fixed-size lists, structs, maps,
dictionary-encoded values, and freshly synthesized heterogeneous Unions. It
writes `arrowname` and `arrowmetadata` as standard extension metadata. On read,
it resolves the logical type with `JuliaType` and restores each value with
`fromarrow` or `fromarrowstruct`.
When an extension name has no registered mapping in the current process,
Arrow warns and returns its ordinary storage values. A plain concrete struct
whose fields are supported can use ArrowTypes.jl's default `StructKind`
mapping; without extension hooks, it reads back as ordinary Struct storage.
Use the lowering interface to select a different stable storage
representation. An `ArrowKind` override alone does not select an arbitrary
Arrow 3.0 physical layout.

Arrow does not intern an unknown on-wire extension name merely to probe
`JuliaType(Val(...))`. It dispatches only when that name is already a Julia
`Symbol`. Unsupported-name warnings are deduplicated by the complete extension
label. Each table materialization emits at most one warning for each of 16
distinct labels, then one suppression notice for further distinct labels. A
warning shows at most 128 UTF-8 bytes of its label. The built-in
`JuliaLang.Symbol` mapping also lifts only a payload that is already interned. A
novel payload produces `ValidationError` instead of adding permanent
process-global symbol state.

Recursive custom storage schemas and recursive value containers are rejected
with `ArgumentError`. Custom mappings may nest to 64 levels. Deeper mappings
are rejected before the writer can recurse without a bound.

Arrow also bounds exact Julia type construction for composite descriptors.
It passes the exact `NTuple{N,T}` storage type to `JuliaType` when `N` is at
most 1024. For a larger fixed-size-list descriptor, it passes the compact
`Tuple{Vararg{T}}` family instead. Generic registrations still resolve. A
registration that requires the exact oversized arity remains unknown, so Arrow
returns ordinary storage values. An extension-labelled Struct receives its
exact `NamedTuple` storage signature through 1024 children only when its child
names are unique, contain no embedded NUL, already exist as Julia `Symbol`s,
are at most 4096 UTF-8 bytes each, and use at most 64 KiB in total. Otherwise
the labelled Struct remains an unknown extension and reads as ordered `Pair`
storage. One bounded compatibility exception preserves ArrowTypes.jl Tuple
storage: when the complete child-name sequence is exactly `"1"`, `"2"`, …,
`string(N)` for `N ≤ 1024`, Arrow may intern only that fixed finite set of
positional names. The exception does not apply to an unknown extension label or
to arbitrary or partly positional Struct names.
If writer-side `ArrowType` resolution returns a concrete tuple with more than
1024 fields, the writer rejects that storage type before specializing on it.
This also applies to ArrowTypes.jl's default mapping for a tuple value. Custom
trait code still runs as ordinary trusted Julia code; Arrow cannot recover if
the hook crashes while it constructs its return value.

Retained composite rows hidden by a nullable ancestor are constructed from the
retained Field and the required logical length. Arrow does not manufacture a
Julia object for every hidden child slot. This keeps Null-only fixed-size-list
and inactive sparse-Union storage bounded by the bytes the output requires.

### ArrowStrings columns

[ArrowStrings.jl](https://github.com/apache/arrow-julia/tree/main/src/ArrowStrings)
(a separate package that lives in this repository) defines
`ArrowString`, a 16-byte string value that *is* an Arrow StringView entry
(inline up to 12 bytes, otherwise a prefix plus buffer index and offset),
and `StringVector`, a column of them over a set of byte buffers —
which *is* an Arrow Utf8View array's memory. A parser or other producer can
build this representation directly. `Arrow.write` then wraps its payload
vector and buffers as the Arrow column without repacking them or
materializing a `String`.

## Validation

Every batch decoded by a read, and every batch emitted by a write, is validated:
buffer arity and byte lengths against the schema (structural), and offset
monotonicity, dictionary index domains, union type ids, and the other
data-intrinsic invariants (semantic). Metadata is verified by a generated
FlatBuffers shape verifier before any of it is used. Resource limits (metadata
size, body size, allocation budget, nesting depth) are enforced before any
metadata-directed or package-controlled read-facade allocation. The one
cumulative budget covers byte-range fetch and assembly, IPC decode, Core and
ArrowTypes containers, scan slices and joins, and final table columns. Custom
user hook allocations remain the hook author's responsibility. Vector
reserves include a conservative backing-store capacity, not only the logical
element payload, because supported Julia versions can round that capacity to
the next allocation class.

Core schema field and child names remain `String` values and are not interned.
The Tables.jl facade is the explicit `Symbol` boundary because Tables column
names are Symbols. Before it interns any novel top-level field name, Arrow
preflights the complete schema: each name is at most 4096 UTF-8 bytes, one table
materialization may add at most 65,536 novel names, and their combined UTF-8
size is at most 1 MiB. A schema outside those limits produces
`ValidationError` without partially interning its novel names.

A ranged scan validates the Footer's complete Block index up front and the
complete node and buffer metadata for every statistics-surviving record before
it requests body ranges. It does not request or decode statistics-pruned record
metadata or skipped body content. Corrupt or hostile input that reaches these
validation stages produces a clean `ValidationError` rather than a crash or an
unbounded allocation.

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

Arrow.jl exposes a low-level interface over `Arrow.Field`, `Arrow.ArrayData`,
`Arrow.Schema`, and `Arrow.RecordBatch`. These names and the exact ABI structs
`Arrow.CArrowSchema`, `Arrow.CArrowArray`, and `Arrow.CArrowArrayStream` are
public but not exported. Qualify them with `Arrow.`. `Arrow.fromjulia` and
`Arrow.batch` build engine values from Julia columns, and `Arrow.materialize`
converts imported column data back to a Julia vector.

* `Arrow.to_c_data(field, data) -> (schemaptr, arrayptr)` exports one
  column; the structs stay valid until the consumer calls their `release`
  callbacks, and `Arrow.reap!()` reclaims the export bookkeeping afterward.
* `Arrow.from_c_data(schemaptr, arrayptr) -> (field, data)` imports one
  column, *moving* the array (its source `release` is nulled, as the spec
  requires). The imported buffers stay valid as long as the returned data is
  reachable; `Arrow.release!` on the import's `ForeignOwner` (or on the
  owner region behind any imported buffer, `buffer.region`) revokes every
  imported buffer and runs the producer's release callback exactly once.
* `Arrow.export_stream!(streamptr, schema, batches)` fills a caller-owned
  `ArrowArrayStream`; `Arrow.from_c_stream(streamptr)` imports one and
  yields record batches through `Arrow.nextbatch!`.

For example, handing a column to PyArrow in-process through PythonCall:

```julia
using Arrow, PythonCall
pa = pyimport("pyarrow")

f, d = Arrow.fromjulia("x", [1, 2, missing, 4])
sp, ap = Arrow.to_c_data(f, d)
pyarr = pa.Array._import_from_c(UInt(ap), UInt(sp))     # PyArrow now owns the structs
pyarr.to_pylist()                                        # [1, 2, None, 4]
```

The conformance suite under `conformance/` round-trips the supported layout
families through PyArrow over this path in both directions. It records
explicit skips for invalid fixtures, layouts an external oracle cannot
construct or import, and features that Arrow.jl intentionally rejects.

## Compiling with JuliaC `--trim`

Arrow.jl's engine is designed to compile under JuliaC's `--trim=safe`:
type descriptors are runtime values, layout dispatch goes through closed
`isa` ladders, and the value-domain entry points (reading, C data
import/export, and the typed accessors `Arrow.ArrowCore.materialize(::Type{T},
field, data)`) are statically resolvable. The repository's
`test/trim_compile_tests.jl` gate holds that at zero verifier errors and
warnings. The same gate exercises ArrowStrings construction, inline and view
access, missing values, comparison, and materialization. Arrow.jl itself
supports Julia 1.10 and later; JuliaC's `--trim` needs Julia 1.12, so the gate
runs only there. The dynamic facade conveniences (property access on
`Arrow.Table`, `NamedTuple` rows) are not part of that guarantee.

## Updating from Arrow.jl 2.x

Arrow.jl 3.0 changes the storage model and removes some advanced Arrow 2.x
write features. Read [Migrating from Arrow.jl 2.x](@ref) before you update.
