```@raw html
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
```

# Writing

## Write a table

[`Arrow.write(sink, table)`](@ref Arrow.write) writes any Tables.jl source (a
`DataFrame`, a `NamedTuple` of vectors, a `CSV.File`, an `Arrow.Table`) to a path or an
`IO` and returns the sink; any other object that `open(sink, "w")` accepts, such as a
FilePathsBase.jl path, is written as a path is, in place. `Arrow.write(sink)` returns a
function, so a table can be piped into it, as in
`CSV.File("trips.csv") |> Arrow.write("trips.arrow")`.

```@example writing
using Arrow, Tables

dir = mktempdir()
path = joinpath(dir, "trips.arrow")
table = (id=[1, 2, 3], fare=[7.5, 12.0, missing])
Arrow.write(path, table)
table |> Arrow.write(path)
Arrow.write(path, table; compress=:zstd)
Arrow.Table(path).fare
```

A path gets the IPC **file** format: the `ARROW1` magic, the record batches, and a footer
that indexes them, so readers can open any batch directly. An `IO` gets the **stream**
format, which has no footer and can be read as it arrives ([Streams and IPC](@ref)).
`file=true` or `file=false` overrides the choice. [`Arrow.tobuffer(table)`](@ref
Arrow.tobuffer) writes the stream format to a new `IOBuffer`.

```@example writing
(String(read(path, 6)), String(take!(Arrow.write(IOBuffer(), table; file=true))[1:6]))
```

Writing to an existing path writes a temporary file in the same directory and renames it
over the old one at the end. A failed write leaves the old file unchanged, a table that
maps the old file keeps reading it, the file keeps its permissions, a symlink is written
through, and a read-only file is refused. An `IO` is left open.

The keywords:

| keyword | default | |
|:--|:--|:--|
| `file` | `true` for a path, `false` for an `IO` | the file format, or the stream format |
| `compress` | `nothing` | `:lz4` (LZ4 frame) or `:zstd` |
| `compresslevel` | `nothing` | the codec's level: LZ4 up to 12 (default 0), Zstandard up to 22 (default 3) |
| `batchsize` | `1_048_576` | rows per record batch |
| `dictencode` | `nothing` | `true`, or a column name or names: dictionary-encode those columns too; `false`: write pooled columns as values |
| `dictdeltas` | `false` | write the categories a dictionary gains as delta dictionaries |
| `largelists` | `false` | 64-bit offsets: LargeUtf8, LargeBinary, LargeList |
| `stringview` | `false` | write strings and bytes as Utf8View and BinaryView |
| `metadata`, `colmetadata` | the table's | schema and column key-value metadata |
| `ntasks` | `Threads.nthreads()` | tasks that build columns and compress buffers |

[`Arrow.Writer`](@ref) also takes `nullable` and `dictreplacement`. A misspelled keyword is
an `ArgumentError` that lists the valid ones; the Arrow 2.x keywords `alignment`,
`dictencodenested`, `denseunions`, and `maxdepth` are accepted with a warning and ignored.

## Column types

Each column's element type decides its Arrow type, at any depth: a `Vector{Date}` inside a
list writes the type a `Date` column writes. [Type mapping](@ref) lists every type in both
directions.

Strings must be valid UTF-8, as the format requires: a string that is not fails the write
with an error naming its column and row. Write bytes that are not text as `Vector{UInt8}`
values, which write as Binary.

Utf8, Binary, and List store 32-bit offsets, which address 2 GiB of data in one record
batch. A batch whose strings, bytes, or list values would pass that is cut into smaller
batches, so the declared types never change. `largelists=true` declares 64-bit offsets
instead; a single value larger than 2 GiB needs it.

## Batches and partitions

Each `Tables.partitions` partition of the source becomes one or more record batches of at
most `batchsize` rows; a partition with no rows adds none, so a table with no rows writes
its schema alone. `Arrow.write(path, Arrow.Stream(src))` keeps the source's record
batches. Partitions are read, encoded, and written one at a time, so
`Arrow.write("big.arrow", CSV.Chunks("big.csv"))` holds about one chunk in memory.

The first partition fixes the schema. With several partitions every field is declared
nullable, since a later partition may hold missing values the first did not (pyarrow and
Polars write partitioned data the same way), so such a table reads back with
`Union{Missing,T}` columns. Later partitions must have the same columns in the same
order, with values that convert exactly (an `Int32` partition fits an `Int64` column);
anything else is an error naming the column.

## Compression

`compress=:lz4` or `:zstd` compresses each buffer of each record batch and dictionary on
its own, as the format defines; a buffer that does not shrink is stored uncompressed.
pyarrow and Polars read both codecs; DuckDB 1.5's `read_arrow` reads Zstandard but not
LZ4. Zstandard usually writes smaller files; LZ4 is faster to write and read.

## Dictionary encoding

A dictionary-encoded column stores each distinct value once and an integer index per row.
Low-cardinality strings, such as a region or a status, are the usual case. A column is
dictionary encoded when

- it is wrapped in [`Arrow.DictEncode`](@ref);
- it has a `DataAPI.refpool`, as a `PooledArray`, a `CategoricalArray`, and a dictionary
  column read from Arrow do: its pool becomes the dictionary, in pool order and with
  unused entries, and an ordered `CategoricalArray` writes an ordered dictionary; or
- `dictencode` names it, or is `true`.

`dictencode=false` writes pooled and categorical columns, and dictionary columns read
from Arrow, as plain values, for readers that refuse dictionaries such as DuckDB 1.5's
`read_arrow`; an `Arrow.DictEncode` column stays encoded.

```@example writing
using PooledArrays

Arrow.write(path, (region=Arrow.DictEncode(["east", "west", "east"]),
    status=PooledArray(["open", "closed", "open"]), id=[1, 2, 3]))
t = Arrow.Table(path)
t.region
```

The indices are the narrowest signed integers that hold the dictionary (`Int8` up to 128
values) for a one-partition table, and `Int32` for several partitions and for an
`Arrow.Writer`, since later tables may add values. Reading gives a
[`Arrow.DictEncoded`](@ref) column: `DataAPI.refarray`, `DataAPI.refpool`, and
`DataAPI.levels` expose the encoding, and `copy` gives a `PooledArray`.

### Dictionaries that grow

When a later partition, or a later table given to an `Arrow.Writer`, has new values, the
column keeps one dictionary: the new values follow the earlier ones. How the growth is
written, and who reads it:

| output | default | `dictdeltas=true` |
|:--|:--|:--|
| file | the whole dictionary once, after the record batches | a delta dictionary with the new values |
| stream | the whole dictionary again, before the batch that needs it | a delta dictionary with the new values |
| read by | pyarrow, Polars | pyarrow; not Polars 1.43 |

Arrow 2.x reads all of these, but with several threads it can fail on the default file
output with a `KeyError`, decoding a record batch before the dictionary written after it;
write such files with `dictdeltas=true` when Arrow 2.x readers need them. DuckDB 1.5's
`read_arrow` refuses dictionaries in every form.

A stream [`Arrow.Writer`](@ref) created with `dictreplacement=true` instead replaces each
dictionary with the next table's own values. The schema then declares the
DictionaryReplacement feature, which some readers (nanoarrow) refuse.

## Write in parts

[`Arrow.Writer`](@ref) writes one output from several tables, such as batches that do
not fit in memory together. Each `Arrow.write(writer, table)` writes that table's record
batches before it returns; `close` finishes the output, and the function form always
closes it.

```@example writing
Arrow.Writer(path) do writer
    Arrow.write(writer, (id=[1, 2], name=["a", "b"]))
    Arrow.write(writer, (id=Int32[3], name=[missing]))
end
Arrow.Table(path).name
```

The first table fixes the column names, order, and types. Every column is declared
nullable unless the writer was created with `nullable=false`, so later tables may hold
missing values. Later tables convert to the first table's types exactly: an `Int32`
column fits an `Int64` one, but a `Float64` column does not. A table that does not conform is
an error naming the column and both types; the batches written before it stay in the
output and the writer stays open. A writer closed before it receives a table writes an
output with no columns; a writer that is never closed leaves a file without its footer or
a stream without its end marker.

[`Arrow.append(path, table)`](@ref Arrow.append) adds a table's record batches to an
existing stream-format file (or a seekable `IO`), under the same rules; a missing or empty
file starts a new stream. The file format cannot be appended to, since its footer would
have to be rewritten: produce it with an `Arrow.Writer`, or rewrite it with
`Arrow.write`.

## Metadata

`metadata` sets the schema's key-value pairs and `colmetadata` each column's, as a `Dict`
or a vector of pairs; `colmetadata` maps column names, as `Symbol`s or strings, to them.
Values that are not strings are written as they print. Both default to the
table's DataAPI metadata, so a `DataFrame`'s `metadata` and `colmetadata` are written;
keys under the reserved `ARROW:` prefix are not. Writing an `Arrow.Table` or
`Arrow.Stream` keeps its schema and column metadata.

## Memory and threads

A write holds the partition it is writing, the record batch it is building and that
batch's encoded buffers, and, in the file format, each dictionary that grows until it is
written at the end. Columns that are already Arrow layouts, such as the views of an
`Arrow.Table`, are written from their own memory without a copy.

The columns of a record batch are built and its buffers compressed on up to `ntasks`
tasks (default `Threads.nthreads()`); with compression, a batch compresses while the next
one builds. `ntasks=1` writes on the calling task. The bytes written are the same for
every `ntasks`.
