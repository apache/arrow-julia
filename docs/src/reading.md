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

# Reading

## Read a table

[`Arrow.Table(source)`](@ref Arrow.Table) reads Arrow IPC data, in the file or the stream
format, into columns. A table is a `Tables.AbstractColumns`: `table.fare`,
`table[:fare]`, `table["fare"]`, `table[2]`, and `Tables.getcolumn(table, :fare)` return a
column, `names(table)` lists them, and any Tables.jl sink accepts the table.

```@example reading
using Arrow, DataFrames, Dates, Tables

dir = mktempdir()
path = joinpath(dir, "trips.arrow")
Arrow.write(path, (id=1:6, fare=[7.5, 12.0, 3.25, 40.0, 9.5, 21.0],
    zone=["A", "B", "A", "C", "B", "A"], day=Date(2025, 1, 1):Day(1):Date(2025, 1, 6)))

table = Arrow.Table(path)
```

A source is a path, an `IO`, a byte vector, or an [`Arrow.AbstractArrowSource`](@ref)
([Sources](@ref)); any other object that `open(source, "r")` accepts, such as a
FilePathsBase.jl path, reads as the file or `IO` it opens. Column types are listed under
[Type mapping](@ref). Columns that share a name read as `a`, `a_1`, `a_2`, and so on, as
CSV.jl names them.

## Columns are read-only views

Numbers, booleans, dates, times, timestamps, decimals, and the indices of dictionary
columns are read in place: such a column is an [`Arrow.ArrowVector`](@ref) that views the
mapped file, the byte vector, or the decompressed buffer it was read from. Opening a
table costs time per column and record batch, not per byte. A column of a file with
several record batches is a `SentinelArrays.ChainedVector` of one view per batch.

```@example reading
table.fare
```

Strings and binary values read as a `DataStrings.StringVector` of `DataString`s and a
`BytesVector` of `DataBytes`, built with one copy of their bytes the first time the table
hands the column out. `stringtype=String` reads strings as `String`s, nested ones
included, at the cost of one allocation per value. Lists, maps, and structs read as
typed rows ([Nested values](@ref)).

Columns are read-only:

```@example reading
try
    table.fare[1] = 0.0
catch err
    err
end
```

A column keeps the memory it reads alive for as long as the column is reachable, even
after its table is gone.

## Copy into Julia vectors

`copy(column)` and `copy(table)` copy into owned, mutable columns: a `Vector` per column,
a `PooledArray` for a dictionary column, and a `StringVector` for strings, which is
mutable too. `DataFrame(table)` copies once, as `DataFrame` always does;
`DataFrame(table; copycols=false)` keeps the views. [`Arrow.read(source,
sink)`](@ref Arrow.read) reads and hands the copies to a sink in one step:

```@example reading
df = Arrow.read(path, DataFrame)
```

`copy(table)` and `Arrow.read` also detach nested Arrow views. Lists become vectors,
maps become vectors of key-value pairs (keeping duplicate keys), and structs and tuples
keep their shape with owned child values. A copied table therefore keeps no dependency
on the input's buffers. Nested values that were already ordinary Julia arrays follow
Julia's usual shallow-copy behavior.

## Memory mapping and lifetime

A path is memory mapped by default, so opening a file larger than memory is cheap and
only the pages a read touches are loaded. `mmap=false` reads the file into memory
instead. The mapping lasts until the table and every column taken from it are garbage
collected.

`Arrow.write` to a mapped path writes a new file and renames it over the old one, so
columns of the old file keep reading the old contents. On Linux and macOS, deleting or
renaming a mapped file is safe too.

!!! warning "Files that other programs rewrite in place"
    If another program truncates and rewrites a file in place (`open(path, "w")`,
    `CSV.write`, pyarrow, Polars, a shell redirect) while a column of a mapped table of
    that file is alive, the next read of that column crashes Julia with a bus error
    (SIGBUS). No check can prevent it: the operating system removes the pages under the
    mapping. Read such files with `mmap=false`.

!!! note "Windows"
    Windows cannot delete a file while it is mapped, so there a path is read into memory
    unless `mmap=true` is passed. With `mmap=true`, deleting the file fails until the
    table and its columns are garbage collected; `Arrow.write` can still replace it.

[`Arrow.release!(table)`](@ref Arrow.release!(::Arrow.Table)) ends a read: the table
refuses further column access. A column taken before the release keeps working, and its
memory is freed when it is garbage collected.

```@example reading
t = Arrow.Table(path)
fares = t.fare
Arrow.release!(t)
sum(fares)
```

## Choose columns and rows

`select` and `drop` choose columns (names, indices, a regex, a vector of them, or a
function `(i, name) -> Bool` as in CSV.jl), `filter` keeps the rows where a predicate is
`true`, and `limit` caps the row count. Only the selected and filter columns are read, and a `limit` stops reading
once it has its rows.

```@example reading
DataFrame(Arrow.Table(path; select=[:id, :fare], filter=Arrow.col(:fare) > 10, limit=2))
```

Predicates are the `Tables.col` family, which Arrow provides under its own names, so a
filter needs no other package:

- [`Arrow.col`](@ref) with `<`, `<=`, `>`, and `>=` against numbers, strings, and
  characters;
- [`Arrow.colcmp`](@ref) for equality and for other values, such as dates and
  timestamps;
- [`Arrow.colin`](@ref) for membership and [`Arrow.isnull`](@ref) for missing values;
- `&`, `|`, and `!` to combine them.

A row whose predicate is `missing` is dropped.

```@example reading
keep = Arrow.colcmp(==, Arrow.col(:zone), "A") &
       Arrow.colcmp(>=, Arrow.col(:day), Date(2025, 1, 2))
DataFrame(Arrow.Table(path; filter=keep))
```

A literal must be of its column's kind: comparing a string column with a number is an
`ArgumentError` that names the column and both kinds of value. A `Date`, `DateTime`, or `Durations.Timestamp`
compared with a UTC timestamp column (pandas' time-zone-aware data, DuckDB's
TIMESTAMPTZ) is that time in UTC. A column in another zone needs a
`Durations.ZonedTimestamp` literal, and the error says how to write one.

`select` also takes `name => newname` to rename a column and `name => Type` to read it
as another type, such as `:day => DateTime`. For an offset, pass a `Tables.Scan` as
`scan` instead of the keywords:

```@example reading
DataFrame(Arrow.Table(path; scan=Tables.Scan(select=(:id, :fare => :amount), offset=1, limit=3)))
```

## Threads

A read uses up to `ntasks` tasks (default `Threads.nthreads()`): record batches decode
and decompress in parallel, columns build in parallel, and a long string column builds in
chunks of rows. `ntasks=1` reads on the calling task. The result is the same for every
`ntasks`. Start Julia with threads (`julia -t 8`) for the tasks to run at once.

## Metadata

`DataAPI.metadata(table)` gives the schema's key-value metadata and
`DataAPI.colmetadata(table, :fare)` a column's. `DataFrame(table)` and `Arrow.read(path,
DataFrame)` keep both, and writing the table again keeps them too.
[`Arrow.getmetadata(table)`](@ref Arrow.getmetadata) is the Arrow 2.x form.

```@example reading
using DataAPI
meta = joinpath(dir, "meta.arrow")
Arrow.write(meta, (fare=[7.5],); metadata=["source" => "meter"],
    colmetadata=Dict(:fare => ["unit" => "USD"]))
t = Arrow.Table(meta)
(DataAPI.metadata(t), DataAPI.colmetadata(t, :fare))
```

## Sources

- A **path** is memory mapped ([Memory mapping and lifetime](@ref)).
- A **byte vector** is read in place: do not modify or resize it while its table's
  columns are in use.
- An **`IO`** is read to its end into memory by `Arrow.Table`; [`Arrow.Stream`](@ref)
  reads a stream-format `IO` one record batch at a time ([Streams and IPC](@ref)). The
  `IO` is not closed.
- An [`Arrow.AbstractArrowSource`](@ref) supplies bytes by range, for storage the others
  do not cover. It implements [`Arrow.sourcelength`](@ref) and [`Arrow.readrange`](@ref),
  and [`Arrow.concurrentreads`](@ref) when ranges may be read on several tasks at once.
  A read with `select`, `drop`, `filter`, `limit`, or `scan` reads the file-format
  footer from the end of the source, then fetches only the record batches and columns
  it needs, in a few coalesced ranges. A read without them fetches the whole source in
  8 MiB ranges.

```@example reading
struct CountingSource <: Arrow.AbstractArrowSource
    bytes::Vector{UInt8}
    fetched::Base.RefValue{Int}
end
Arrow.sourcelength(src::CountingSource) = length(src.bytes)
function Arrow.readrange(src::CountingSource, offset, count)
    src.fetched[] += count
    return src.bytes[(offset + 1):(offset + count)]
end

big = joinpath(dir, "big.arrow")
Arrow.write(big, (id=1:100_000, fare=rand(100_000), note=fill("no comment", 100_000)))
src = CountingSource(read(big), Ref(0))
fares = Arrow.Table(src; select=[:fare]).fare
(bytes_fetched=src.fetched[], file_bytes=filesize(big))
```

With [CloudStore.jl](https://github.com/JuliaServices/CloudStore.jl) loaded, a
`CloudStore.Object` (S3 or Azure Blob Storage) reads by ranged requests the same way:

```julia
using Arrow, CloudStore

object = CloudStore.Object(bucket, "events/2025-01.arrow"; credentials)
table = Arrow.Table(object; select=[:id, :amount])
```

`Serialization.serialize` of a table saves its values and schema; the deserialized table
owns its columns.
