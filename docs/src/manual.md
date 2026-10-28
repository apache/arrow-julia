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
using Arrow, Tables, DataFrames

Arrow.write("data.arrow", (a = [1, 2, 3], b = ["x", "y", missing]))
tbl = Arrow.Table("data.arrow")
tbl.a            # a read-only view of the file's Int64 values
tbl.b            # a DataStrings.StringVector of Union{Missing, DataString}
df = Arrow.read("data.arrow", DataFrame)    # owned, mutable columns
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
Tables.rowcount(tbl)                       # number of rows
```

Pass `tbl` directly to any Tables.jl-compatible sink.

Column element types follow the Arrow schema (see [Type mapping when
reading](@ref)). Flat columns read the Arrow buffers in place: numbers,
booleans, temporal values, decimals, and dictionary-encoded values are
read-only [`Arrow.ArrowVector`](@ref) views, so building a table costs time
proportional to its columns and record batches, not its size. A column read
from several record batches is a `ChainedVector` of per-batch views. String
and binary columns are `DataStrings.StringVector` and `BytesVector`, built
with one copy of their bytes; pass `stringtype=String` for `Vector{String}`
columns. List, map, struct, and union columns read as typed rows (see
[Type mapping when reading](@ref)).

Views are read-only. `copy(column)` returns an owned `Vector` (a
`PooledArray` for a dictionary-encoded column), `copy(table)` copies every
column, and `DataFrame(table)` copies once by default. [`Arrow.read`](@ref)
hands a sink owned columns with exactly one copy:

```julia
df = Arrow.read("data.arrow", DataFrame)
```

A column keeps the memory it reads alive while it is reachable, even after
its table is gone. Reductions such as `sum`, `count`, and `sum(skipmissing(col))`
run directly on the buffers.

### Memory mapping and `release!`

A path to a file-format source is memory-mapped by default (`mmap=true`), so
opening a large file does not copy it into RAM; pass `mmap=false` to read it
into memory instead (a stream-format path is always read into memory). A
mapping is unmapped once the table and all of its columns are garbage
collected. Windows cannot replace or delete a mapped file, so there the
default is `mmap=false`. A byte vector is read in place: do not modify or
resize it while the table's columns are in use.

[`Arrow.release!`](@ref) ends a read: the table refuses further column
access, and a released [`Arrow.Stream`](@ref) refuses further iteration
(every yielded `Table` shares its parent's source, so releasing either one
releases both). Columns taken before the release stay usable:

```julia
tbl = Arrow.Table("data.arrow")
ids = tbl.id
Arrow.release!(tbl)          # tbl.id now throws; `ids` still reads the file
```

`Serialization.serialize` saves the table's Julia values, schema, and retained
dictionary pools. Deserialization returns a table with owned columns.

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
way to process a file larger than RAM. Each of those batches decodes under its
own allocation allowance ([`Arrow.Limits`](@ref)), sized from its own bytes,
so a file of any length streams with the default limits.

A **stream-format** source is read to the end and every batch is decoded when
the `Stream` is constructed. A file-format `IO` or byte-vector input is
also read to the end (the whole source is held in memory), but its record
batches are still decoded lazily, one per iteration.

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
| Utf8, LargeUtf8, Utf8View | `DataStrings.DataString` (`String` with `stringtype=String`) |
| Binary, LargeBinary, BinaryView | `DataStrings.DataBytes` |
| FixedSizeBinary(N) | `NTuple{N,UInt8}` for `N ≤ 256`; `DataStrings.DataBytes` for wider values |
| Date32 | `Dates.Date` |
| Date64 | `Dates.DateTime` |
| Timestamp (no timezone) | `Durations.Timestamp{P}` at the column unit (the stored UTC instant, exact at every unit) |
| Timestamp with a declared timezone | `Durations.ZonedTimestamp{P,Z}` at every unit (the stored UTC instant; `Z` is the declared zone — see below) |
| Time32/Time64 | `Dates.Time` |
| Duration | `Dates.Second`/`Millisecond`/`Microsecond`/`Nanosecond` by unit |
| Decimal (scale from 0 to the precision) | `DataDecimals.Decimal{P,S,T}` with `T` matching the descriptor width |
| Decimal (negative scale, or scale above the precision) | raw storage: `Int32`/`Int64` (32/64-bit) or `Vector{UInt8}` (128/256-bit) |
| Interval | `Durations.Duration` (month/day/nanosecond components) |
| Dictionary-encoded scalar | the mapping of the *value* type, in an `Arrow.DictEncoded` column whose codes `DataAPI.refarray`, `refpool`, and `levels` expose |
| Run-end encoded scalar | the mapping of the *values* type, in an `Arrow.RunEndEncoded` column (runs are not expanded) |

Composite layouts read as typed rows built from their children's element
types, so a composite column's element type is also the same for a zero-row,
an all-`missing`, and a populated column (except for nulls the field did not
declare, below). The two wrapper layouts are transparent: a
dictionary-encoded or run-end-encoded column takes the element type of its
value child:

| Arrow type | Element type |
|---|---|
| List, LargeList, ListView, LargeListView | [`Arrow.ListRow{T}`](@ref Arrow.ListRow): a read-only view of the row's values |
| FixedSizeList(N) | `NTuple{N,T}` for `N ≤ 64` and non-null values; otherwise `Arrow.ListRow{T}` (a tuple with `Missing` in its element type is abstract, so every row would be boxed) |
| Struct | a `NamedTuple` of the children's names and element types, built when the row is read |
| Map | [`Arrow.MapRow{K,V}`](@ref Arrow.MapRow), an `AbstractVector{Pair{K,V}}`: the row's entries in stored order, duplicate keys kept (`Dict(row)` builds a dictionary) |
| Union | the `Union` of the children's element types |
| Run-end encoded (composite values) | the *values* child's element type (runs are expanded) |
| Null | `Missing` |

A composite column stores its children by their element types alone:
strings in a `DataStrings.StringVector`, binary in a `BytesVector`, lists in an
[`Arrow.ListVector`](@ref), maps in an [`Arrow.MapVector`](@ref), structs in an
[`Arrow.StructVector`](@ref), other values in a `Vector`. No child value is
boxed, and a struct row is built only when it is read. The children are the
column's own copies, so a row stays valid after its table is gone;
`copy(column)` gives `Vector{T}` rows (`Vector{Pair{K,V}}` for maps,
`NamedTuple`s for structs). Three kinds of field keep the dynamic path,
which builds each row as a Julia value (list rows as `Vector{Any}`, struct
rows as `Vector{Pair{String,Any}}` ordered name => value pairs): a struct
with more than 64 fields or duplicate field names, a ListView or
LargeListView of composite values, and a composite nested more than 8 levels
deep (each level adds to the row type, and Julia's compile time grows with
it).

Three refinements. A list-family field with a registered ArrowTypes.jl child
uses a row vector with that restored child element type; this is the same for
a zero-row and a populated column. The public value domain is uniform at
every depth: the temporal, decimal, and interval conversions above apply to
a leaf whether it is a top-level column, a dictionary pool, a run-end-encoded
values child, a union branch, or a field inside a struct, list, or map row.
And field nullability is
*advisory* in Arrow (the reference implementation and the conformance
corpus accept a null under a `nullable=false` field), so a column that
holds a null its field did not declare reads as `Union{Missing, T}` rather
than failing; a conforming column keeps its declared, `Missing`-free type.
Timestamps never truncate or wrap into `DateTime`: they read as
`Durations.Timestamp` and `Durations.ZonedTimestamp` (both compare equal to
`DateTime` values at the same instant) at every depth. A decimal coefficient
outside the field's declared precision reads as stored, as in Arrow C++. A
time of day outside the format's [0, 24 h) range has no `Dates.Time` value
(it would wrap modulo 24 hours), so reading one throws
`Arrow.ValidationError` naming the column.

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

* Temporal lowering: Date64 equality can lower, but its ordered
  comparisons stay public, and Time predicates stay public. Timestamp
  columns read as `Durations.Timestamp` and `ZonedTimestamp` — total
  bijections with their storage — so every operator lowers when the
  literal converts exactly (`Timestamp`, `DateTime`, and `Date` literals
  on zone-naive columns; `ZonedTimestamp` in any zone, and `ZonedDateTime`
  with TimeZones.jl loaded, on zone-declared columns). A zone-naive
  literal never lowers against a zone-declared column, or vice versa: the
  public domain defines those comparisons as unequal (`==` is `false`;
  ordered comparisons error). Duration lowering accepts a literal in the
  column unit or a coarser fixed unit, but a finer unit stays public
  because Julia can overflow while promoting stored values.
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
[`Arrow.write`](@ref) does not compute them; files that carry them prune,
files that do not are simply scanned batch by batch. `Arrow.write` and
[`Arrow.Writer`](@ref) drop automatically inherited batch statistics when
rewriting an `Arrow.Table` or `Arrow.Stream`. The source statistics may no
longer describe the output values, column order, or batch boundaries.
An explicit `metadata` override is used as supplied, including any statistics.

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

## Writing

[`Arrow.write`](@ref) writes any Tables.jl source to a path or an `IO`, and
[`Arrow.Writer`](@ref) and [`Arrow.append`](@ref) write tables as they arrive:

```julia
Arrow.write("out.arrow", df; compress = :zstd)
```

The [Writing](@ref manual-writing) page covers the keywords, batches and
partitions, nullability, compression, threads, strings, dictionary encoding,
incremental writing, metadata, and the type each Julia type writes as.

## Custom and extension types

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

Arrow reads the canonical extension types `arrow.uuid` as `UUIDs.UUID` and
`arrow.bool8` as `Bool` (any nonzero byte is `true`), and `arrow.json` and
`arrow.opaque` as their storage values (`String`, bytes) without a warning. The
`JuliaLang.UUID` label older Arrow.jl versions wrote (little-endian bytes) still
reads as `UUID`, and rewriting such a column keeps that layout.

Arrow does not intern an unknown on-wire extension name merely to probe
`JuliaType(Val(...))`. It dispatches only when that name is already a Julia
`Symbol`. An unsupported extension name warns once per process; after 16
distinct names, one notice says further warnings are suppressed. A warning
shows at most 128 UTF-8 bytes of its label. The built-in `JuliaLang.Symbol`
mapping interns new `Symbol` values, at most 65,536 of them (1 MiB in total) per
read; a column beyond that, or a payload containing NUL, produces
`ValidationError`.

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

## DataStrings columns

[DataStrings.jl](https://github.com/JuliaData/DataStrings.jl)
(registered in General) defines
`DataString`, a 16-byte string value that *is* an Arrow StringView entry
(inline up to 12 bytes, otherwise a prefix plus buffer index and offset),
and `StringVector`, a column of them over a set of byte buffers —
which *is* an Arrow Utf8View array's memory. A parser or other producer can
build this representation directly. By default the writer packs its bytes
into a Utf8 column in one pass over the payloads; with `stringview=true` it
wraps the payload vector and buffers as an in-memory Utf8View column without a
copy.

When writing IPC, Arrow compacts Utf8View and BinaryView arrays. Only byte
ranges referenced by valid long entries are copied to the output buffers.
Unused backing buffers and gaps are omitted. Null entries and unused inline
bytes are zeroed. Repeated and overlapping ranges within a backing buffer
continue to share output bytes. Source payloads and buffers are not changed.
The same rule applies to view arrays inside nested columns and dictionary
pools. Dictionary values, order, and indices are retained, including pool
values that no row uses. Compaction does not remove those dictionary values.

## Validation

Every batch decoded by a read, and every batch emitted by a write, is validated:
buffer arity and byte lengths against the schema (structural), and offset
monotonicity, dictionary index domains, union type ids, and the other
data-intrinsic invariants (semantic). Metadata is verified by a generated
FlatBuffers shape verifier before any of it is used. Resource limits (metadata
size, body size, allocation budget, nesting depth) are enforced before any
metadata-directed or package-controlled read-facade allocation. A read's
budget (one per `Arrow.Table`, one per batch of a file-format `Arrow.Stream`)
covers byte-range fetch and assembly, IPC decode, Core and ArrowTypes
containers, scan slices and joins, and final table columns. It scales with the
input (`max_allocation_ratio` bytes per input byte, at least 256 MiB, at most
`max_total_allocated_bytes`, the machine's physical memory by default). A
compressed buffer counts at its decompressed size, as far as its column reads
it, so highly compressible files read with the default limits. Lengths a small
file declares without backing bytes fail before allocation, and a compressed
buffer that claims more than its column reads decodes only the column's bytes.
Custom user hook allocations remain the hook author's responsibility. Small
vector reserves include a conservative backing-store capacity, not only the
logical element payload, because supported Julia versions can round that
capacity to the next allocation class.

Core schema field and child names remain `String` values and are not interned.
The Tables.jl facade is the explicit `Symbol` boundary because Tables column
names are Symbols. Before it interns any novel top-level field name, Arrow
preflights the complete schema: each name is at most 4096 UTF-8 bytes, one table
materialization may add at most 65,536 novel names, and their combined UTF-8
size is at most 1 MiB. A schema outside those limits produces
`ValidationError` without partially interning its novel names. A declared
timestamp timezone becomes the `Z` parameter of `ZonedTimestamp{P,Z}`; one
longer than 4096 UTF-8 bytes or containing a NUL byte produces
`ValidationError`.

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
  If `Arrow.to_c_data` or `Arrow.export_stream!` lent those buffers to
  another consumer, the producer release waits until that consumer releases
  them and `Arrow.reap!()` reclaims the export.
* `Arrow.export_stream!(streamptr, schema, batches)` fills a caller-owned
  `ArrowArrayStream`; `Arrow.from_c_stream(streamptr)` imports one and
  yields record batches through `Arrow.nextbatch!`. A producer batch whose
  struct array has null rows is refused with `Arrow.ValidationError`: a
  record batch has no row validity.

`copy(data::Arrow.ArrayData)` copies all declared buffer slices, children,
and dictionary values into Julia-owned byte arrays. It preserves the physical
layout and element offset. The result has no foreign owner or release callback.
Keep the associated `Field` to get named values with
`Arrow.materialize(field, copied)`. `copy(batch::Arrow.RecordBatch)` detaches
all its columns and preserves its immutable schema and row count.

`Serialization.serialize` uses the same ownership boundary. It saves buffer
bytes and reconstructs fresh owners on deserialization. It does not save raw
addresses, foreign callbacks, or cached semantic validation. This works for
`ArrayData` and record batches, including nested and dictionary arrays. Load
`Serialization` to enable this support. The source must remain valid and
unchanged until copying or serialization finishes.

!!! warning "Do not use deepcopy to detach Arrow storage"
    `deepcopy` is not specialized for these objects. It copies raw pointer
    fields and release bookkeeping without rebuilding ownership. The copy
    can still point to the source storage after that storage is released.
    Access or release through the copy can then use freed memory. Use `copy`
    or `Serialization.serialize` for a detached value instead.

Custom metadata keys and values are opaque byte strings. Import and export
preserve invalid UTF-8 and embedded NUL bytes in metadata. C format strings
and field names still follow the C Data interface's UTF-8 requirements.

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
warnings. The same gate exercises DataStrings construction, inline and view
access, missing values, comparison, and materialization. Arrow.jl itself
supports Julia 1.10 and later; JuliaC's `--trim` needs Julia 1.12, so the gate
runs only there. The dynamic facade conveniences (property access on
`Arrow.Table`, `NamedTuple` rows) are not part of that guarantee.

## Updating from Arrow.jl 2.x

Arrow.jl 3.0 changes the storage model and removes some advanced Arrow 2.x
write features. Read [Migrating from Arrow.jl 2.x](@ref) before you update.

### Shared decimal, interval, and timestamp values

Arrow uses DataDecimals 1 and Durations 1.4 from General. Decimal columns
whose scale is between 0 and the precision decode to `DataDecimals.Decimal{P,S,T}`
at every depth. The integer width matches the Arrow descriptor. Other
decimals (a negative scale, or a scale above the precision) keep the raw
representation.
Calendar interval columns decode to `Durations.Duration`; elapsed-time duration
columns still use `Dates.Second`, `Dates.Millisecond`, `Dates.Microsecond`, or
`Dates.Nanosecond`.

Zone-naive timestamp columns decode to `Durations.Timestamp{P}` at the
column unit, and every timezone-declared timestamp column decodes to
`Durations.ZonedTimestamp{P,Z}` with the declared zone in `Z` — both are
8-byte values whose bits are exactly the column storage, holding the UTC
instant. No zone rules are consulted to read or write them; local-time
operations on a `ZonedTimestamp` know `"UTC"` and fixed offsets, and loading
TimeZones.jl teaches them named zones (and conversion to and from
`ZonedDateTime`).

The writer accepts fixed-scale DataDecimals columns, Durations columns, and
`Timestamp`/`ZonedTimestamp` columns (one zone and one resolution per
column; `Durations.astimezone` normalizes a mixed column). It writes
Durations as MONTH_DAY_NANO intervals. Rewriting a retained year-month
or day-time interval checks that the value fits that original representation.
