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

# [Writing](@id manual-writing)

[`Arrow.write`](@ref) writes any Tables.jl source (a `DataFrame`, a
`NamedTuple` of vectors, a `CSV.File`, an [`Arrow.Table`](@ref)) to a file path
or an `IO`, and returns the sink:

```julia
Arrow.write("out.arrow", df)                      # IPC file format
Arrow.write(io, df; file = false)                 # IPC stream format
Arrow.write("out.arrow", df; compress = :zstd)    # or :lz4
df |> Arrow.write("out.arrow")                    # curried
bytes = take!(Arrow.tobuffer(df))                 # stream-format bytes in memory
```

The file format (`file = true`, the default for paths and `IO`s) starts with
the `ARROW1` magic and ends with a footer that indexes the record batches, so
readers can open any batch directly. The stream format has no footer and can
be read as it arrives; it is what [`Arrow.append`](@ref) extends.

## Keywords

| keyword | default | effect |
|---|---|---|
| `file` | `true` | file format; `false` writes the stream format |
| `compress` | `nothing` | `:lz4` (LZ4 frame) or `:zstd` buffer compression |
| `compresslevel` | `nothing` | the codec's level: LZ4 up to 12 (default 0), Zstandard up to 22 (default 3) |
| `batchsize` | `1_048_576` | most rows in one record batch |
| `dictencode` | `false` | dictionary-encode every column (`true`) or the named ones |
| `dictdeltas` | `false` | write the categories a dictionary gains as delta batches |
| `largelists` | `false` | declare 64-bit offsets: LargeUtf8, LargeBinary, LargeList |
| `stringview` | `false` | write strings and bytes as Utf8View and BinaryView |
| `metadata` | the table's | schema key-value pairs |
| `colmetadata` | the table's | per-column key-value pairs, keyed by column name |
| `ntasks` | `Threads.nthreads()` | tasks that build columns and compress buffers |

[`Arrow.Writer`](@ref) takes the same keywords, plus `nullable` and
`dictreplacement` (see [Writing incrementally](@ref)). The Arrow 2.x keywords
`alignment`, `dictencodenested`, `denseunions`, and `maxdepth` are accepted
with a one-time warning and have no effect.

## Batches and partitions

Each `Tables.partitions` partition of the source becomes one or more record
batches of at most `batchsize` rows. `Arrow.write(sink, Arrow.Stream(path))`
and `Arrow.write(sink, Tables.partitioner(tables))` keep the source's batch
boundaries. Partitions are read, encoded, and written one at a time, so
`Arrow.write("big.arrow", CSV.Chunks("big.csv"))` holds about one partition in
memory.

The schema comes from the first partition. Later partitions must conform to
it: the same columns in the same order, numbers that widen losslessly (`Int32`
into an `Int64` column), and missing values only where the field is nullable.
Anything else is refused with an error naming the column.

## Nullability

A field is nullable when its column can hold `missing`: when its element type
is a `Union{Missing,T}`. A source with several partitions declares every field
nullable, since a later partition may hold missing values the first did not;
pyarrow and Polars write partitioned data the same way. So such a source reads
back with `Union{Missing,T}` element types even when no value is missing. An
[`Arrow.Writer`](@ref) declares every field nullable too, unless it is created
with `nullable = false`. An `Arrow.Table` or `Arrow.Stream` source keeps the
nullability its schema declares.

## Compression

`compress = :lz4` or `:zstd` compresses each buffer of a record batch on its
own, as the IPC format defines (an LZ4 or Zstandard frame after the buffer's
uncompressed length); a buffer that does not shrink is stored raw. Dictionary
batches are compressed the same way. pyarrow and Polars read both codecs;
DuckDB 1.5.5's `read_arrow` reads Zstandard but not LZ4. Zstandard usually
gives smaller files; LZ4 is faster to write and read.

## Threads and memory

`ntasks` tasks build the columns of a record batch and compress its buffers
in parallel; batches are written in order, one at a time. A batch with fewer
than 65,536 values (rows times columns) builds on one task, and a body of
less than 1 MiB compresses on one task, since smaller work does not pay for
the tasks. The output is the same bytes for every `ntasks`. Start Julia with
threads (`julia -t 8`) for the tasks to run at once.

The writer holds the partition being written, the record batch being built,
and that batch's encoded buffers, and in the file format each dictionary that
grows until it is written at the end. Columns that are already Arrow layouts
(fixed-width values and validity from an `Arrow.Table`, say) are written from
their own memory, without a copy.

## Strings and bytes

Strings (`String`, `SubString`, `DataStrings.DataString`, InlineStrings, any
`AbstractString`) write as Utf8, and byte vectors (`Vector{UInt8}`,
`DataStrings.DataBytes`, any `AbstractVector{UInt8}`) as Binary, the types
every reader supports. With `stringview = true` they write as Utf8View and
BinaryView instead, which DuckDB's `read_arrow` and older readers refuse.

Strings must be valid UTF-8, as the Arrow format requires, at any depth: a
string that is not throws an `ArgumentError` naming the column and row. Write
data that is not text as `Vector{UInt8}` values, which write as Binary.

Utf8, Binary, and List use 32-bit offsets, which address 2 GiB of data in one
record batch. A batch whose strings, bytes, or list values would pass that is
cut into smaller batches, so the declared types do not change;
`largelists = true` declares 64-bit offsets (LargeUtf8, LargeBinary,
LargeList) instead. A single value or dictionary larger than 2 GiB needs
`largelists = true`.

A `DataStrings.StringVector` column (what [`Arrow.Table`](@ref) reads strings
as) writes in one pass over its 16-byte values; when the values' bytes already
sit in one of its buffers in row order (as after reading a Utf8 column), the
output points at that buffer instead of copying it. With `stringview = true`
the values and buffers are the Utf8View array; only the bytes that non-null
values use are written.

## Dictionary encoding

A dictionary-encoded column stores each distinct value once, in the
dictionary, and an integer index per row. Low-cardinality strings (a region, a
status) are the usual case. Three kinds of column write dictionary-encoded:

- [`Arrow.DictEncode`](@ref)`(v)` wraps any writable column;
- a column with a `DataAPI.refpool`, such as a `PooledArray` or
  `CategoricalArray`: its pool, in pool order and with unused entries, starts
  the dictionary, `CategoricalValue`s are written as the values they wrap, and
  an ordered `CategoricalArray` writes an ordered dictionary;
- the columns that the `dictencode` keyword names (or every column, with
  `dictencode = true`).

```julia
Arrow.write("sales.arrow", (region = Arrow.DictEncode(regions), sales = sales))
```

The indices use the narrowest signed integer type that holds the categories
(`Int8` up to 128 categories) when the table has one partition. A source with
several partitions, and an [`Arrow.Writer`](@ref), use `Int32`, since later
partitions may add categories. Reading a dictionary-encoded column gives its
values; writing an `Arrow.Table` again keeps the encoding, the dictionary's
order, and categories no row uses.

### Dictionaries that grow

When a later partition, or a later table given to an `Arrow.Writer`, holds new
categories, the column keeps one dictionary: the new categories follow the
earlier ones and the indices refer to the combined dictionary. How the growth
is written:

| output | default | `dictdeltas = true` |
|---|---|---|
| file | each dictionary once, complete, after the record batches | a delta batch with the new categories |
| stream | the whole dictionary again, before the batch that needs it | a delta batch with the new categories |

The IPC file format allows a dictionary anywhere before the footer. Readers
of the outputs (pyarrow 25, Polars 1.43, DuckDB 1.5.5 `read_arrow`):

| | pyarrow | Polars | DuckDB |
|---|---|---|---|
| default (file or stream) | reads | reads | refuses dictionaries |
| `dictdeltas = true` | reads | refuses | refuses dictionaries |

A stream `Arrow.Writer` created with `dictreplacement = true` instead
replaces each dictionary with the next table's own categories. The schema
then declares the DictionaryReplacement feature, which some readers (nanoarrow)
refuse. A file cannot replace a dictionary: writing a stream whose
dictionary is replaced (through an `Arrow.Stream` of it) to a file gives the
file one dictionary that holds the categories of both.

## Writing incrementally

[`Arrow.Writer`](@ref) writes tables as they arrive, and writes each table's
batches before the next table is given, so it holds about one partition of the
current table:

```julia
Arrow.Writer("out.arrow") do writer
    for table in tables
        Arrow.write(writer, table)
    end
end
```

The first table fixes the schema, with every field nullable (`nullable =
false` keeps the first table's nullability). Later tables conform to it as
later partitions do. Closing the writer, which the function form always does,
finishes the output; a writer that is never closed leaves a stream without
its end marker or a file without its footer.

[`Arrow.append`](@ref) adds a table to the end of an existing stream-format
file or `IO`, with the same rules for the new table; a missing or empty sink
starts a new stream. It refuses the file format, whose footer would have to be
rewritten; produce such output with an `Arrow.Writer` or rewrite it with
`Arrow.write`. `table |> Arrow.append(path)` is the curried form.

## Metadata

`metadata` sets the schema's key-value pairs and `colmetadata` each column's,
as a `Dict` or pairs (`colmetadata` maps column names to them). Both default
to the table's DataAPI metadata, so a `DataFrame`'s `metadata` and
`colmetadata` are written; keys under the reserved `ARROW:` prefix are not.
An `Arrow.Table` or `Arrow.Stream` source keeps its schema and field
metadata. [`Arrow.getmetadata`](@ref) and DataAPI's `metadata`/`colmetadata`
read them back.

## Type mapping when writing

Every Julia element type below writes as the Arrow type shown, at any depth: a
`Vector{Date}` inside a list, or a `Timestamp` field of a `NamedTuple`, writes
the type it writes as a column.

| Julia element type | Arrow type |
|---|---|
| `Int8`…`Int64`, `UInt8`…`UInt64` | the same-width integer |
| `Float16`, `Float32`, `Float64` | the same-width float |
| `Bool` | Bool |
| `AbstractString` (`String`, `SubString`, `DataString`, InlineStrings) | Utf8 |
| `AbstractVector{UInt8}` (`Vector{UInt8}`, `DataBytes`) | Binary |
| `Symbol` | Utf8, labelled so it reads back as `Symbol` |
| `Char` | UInt32, labelled so it reads back as `Char` |
| `Missing` | Null |
| `Union{Missing,T}` | the type of `T`, nullable |
| `Union{T1,T2,...}` of up to 32 types | dense Union with one child per type |
| any other `AbstractVector{T}` | List of the type of `T` |
| `NTuple{N,T}` of one type (not `UInt8`) | FixedSizeList(N) of the type of `T` |
| `NTuple{N,UInt8}` | FixedSizeBinary(N) |
| other `Tuple`s | Struct with fields `1`, `2`, …, labelled `JuliaLang.Tuple` |
| `NamedTuple` | Struct with one child per field |
| `AbstractDict{K,V}`, or a vector of `Pair{K,V}` | Map of the types of `K` and `V` (keys may not be missing) |
| `Dates.Date` | Date32 |
| `Dates.DateTime` | Timestamp (millisecond) |
| `Dates.Time` | Time64 (nanosecond) |
| `Dates.Second`, `Millisecond`, `Microsecond`, `Nanosecond` | Duration of that unit |
| `Dates.Week`, `Day`, `Hour`, `Minute` | Duration (second), labelled so they read back as the same type |
| `Dates.Month`, `Year` | Interval (year-month), labelled |
| `Durations.Timestamp{P}` | Timestamp of that unit, no timezone |
| `Durations.ZonedTimestamp{P,Z}` | Timestamp of that unit with timezone `String(Z)` |
| `Durations.Duration` | Interval (month-day-nanosecond) |
| `DataDecimals.Decimal{P,S,T}` | Decimal(P, S) as wide as `T` (32 to 256 bits) |
| `UUIDs.UUID` | `arrow.uuid` extension (FixedSizeBinary(16)) |
| `Int128`, `UInt128` | FixedSizeBinary(16), labelled |
| a type with an ArrowTypes.jl mapping | its storage type, labelled with its extension name |

A vector of `Pair`s that is one of a `Union`'s types, or that is stored through
an ArrowTypes.jl mapping (or in a struct column with missing rows and a `Union`
field), writes as a list of `first`/`second` structs instead of a map, as
Arrow 2.x wrote every vector of `Pair`s.

A column whose element type is abstract (`Any`, `Real`, `NamedTuple`) is
narrowed from its values: values of one type write as that type, and values of
up to 8 types as a dense Union of them. A column of `Timestamp`s, `ZonedTimestamp`s,
or `Decimal`s of different units, zones, or precisions is refused, since one
Arrow column has one of each (`Durations.astimezone` gives a column one zone).
A type with no Arrow type, such as `BigInt` or a `Matrix`, is refused with an
error naming the column. [Custom and extension types](@ref) describes ArrowTypes.jl
mappings for your own types.

## Rewriting Arrow data

Writing an [`Arrow.Table`](@ref) or [`Arrow.Stream`](@ref) keeps its schema:
the Arrow types (temporal units, offset widths, unions' modes and type ids,
run-end encoding, map key sorting), child names, nullability, field and
schema metadata, and top-level dictionaries with their index types and
category order. Batch statistics are dropped, since values or batch
boundaries may have changed. Buffer sharing, list views, and run boundaries
are written in a canonical form with the same values.

A union column's values go back to the children they came from: each value to
the one child whose type it has. A union with two children of one type (two
Int32 children, say) cannot be rewritten this way, because the values do not
say which child each came from; writing it throws an error that suggests
writing the values as a new column (`Arrow.write(path, DataFrame(table))`).
A dictionary-encoded column inside another column (a list of
dictionary-encoded strings, say) is refused too: its dictionary is not kept
when it is read, so its values cannot be encoded as they were.

A column of an ArrowTypes.jl type lowers its values again under the retained
type, a Union's children included. A value that does not fit the retained
type (another width, unit, or list size) is refused with an error naming the
column, as is a column replaced with values of another type.
