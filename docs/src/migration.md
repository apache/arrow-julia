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

# Migrating from Arrow.jl 2.x

Arrow.jl 3.0 is a new implementation with the same main names: `Arrow.Table`,
`Arrow.Stream`, `Arrow.write`, `Arrow.Writer`, `Arrow.append`, `Arrow.tobuffer`,
`Arrow.DictEncode`, and `Arrow.getmetadata`. It reads every file Arrow 2.x writes, with
the same values. Code that reads a table into a `DataFrame` and writes it back usually
runs unchanged. What changes is the Julia type of some values, a few removed arguments,
and, for teams that run both versions, a few column types that Arrow 2.x cannot read.

## Before you upgrade

Arrow 3.0 needs Julia 1.10 or later and uses ArrowTypes.jl 2, Tables.jl 1.14,
DataStrings.jl, DataDecimals.jl, and Durations.jl. It no longer depends on TimeZones.jl
or BitIntegers.jl.

Some registered packages still declare compatibility with Arrow 2 only, so adding Arrow 3
can make the package manager hold them, or downgrade them, to older versions; until its
next release this includes CategoricalArrays, through its Arrow extension. After
upgrading, check `Pkg.status()` for packages that went backwards.

## Calls

| Arrow 2.x | Arrow 3.0 |
|:--|:--|
| `Arrow.Table(path; convert=false)` | removed: values always convert to Julia types ([Values](@ref)) |
| `Arrow.Table(bytes, pos, len)` | `Arrow.Table(bytes[pos:(pos + len - 1)])` |
| `Arrow.Table(path)` with a path object, such as a FilePathsBase.jl or AWSS3.jl `S3Path` | `Arrow.Table(string(path))` for a local path, or `Arrow.Table(read(path))`; to write, `write(path, take!(Arrow.write(IOBuffer(), table; file=true)))` |
| `Arrow.Table([path1, path2])`, `Arrow.Stream([...])` | read each source: `reduce(vcat, DataFrame.(Arrow.Table.(paths)))`, or `for path in paths, batch in Arrow.Stream(path)` |
| `table.col[i] = x` | columns are read-only: `df = DataFrame(table); df.col[i] = x` |
| `table.col isa Arrow.List` (or `Map`, `Struct`, `DenseUnion`, ...) | the column types are now `Arrow.ListVector`, `MapVector`, `StructVector`, and plain `Vector`s for unions; better, test the element type |
| `Arrow.getmetadata(table)` | unchanged; `DataAPI.metadata(table)` also works |
| `Arrow.getmetadata(table.col)` | `DataAPI.colmetadata(table, :col)` |
| `Arrow.write(...; compress=LZ4FrameCompressor())` | `compress=:lz4` (or `:zstd`), with `compresslevel` |
| `Arrow.write(...; alignment, dictencodenested, denseunions, maxdepth)` | accepted with a warning and ignored |
| `Arrow.write(...; ntasks=0)` | `ntasks=1` |
| `Arrow.ToTimestamp(x)` | removed: write `Durations.ZonedTimestamp` values, or `ZonedDateTime`s with TimeZones loaded |
| `open(Arrow.Writer, path) do writer ... end` | unchanged, or `Arrow.Writer(path) do writer ... end` |

A keyword Arrow 3.0 does not know is an `ArgumentError` that lists the valid ones and
suggests the name to use for pyarrow's, Polars', and Arrow 2.x's names. `release!` is new
and public, and like every name but `ArrowTypes` it is called with the `Arrow.` prefix.

## Values

Columns are still views of the file, and most element types are the same as in 2.x.
These changed:

| Arrow type | Arrow 2.x | Arrow 3.0 | To get the 2.x value |
|:--|:--|:--|:--|
| Utf8 | `String` | `DataStrings.DataString` | `String(x)`; or read with `stringtype=String` |
| Binary | `Base.CodeUnits` | `DataStrings.DataBytes` | `Vector{UInt8}(x)` |
| Timestamp | `DateTime`, truncated to milliseconds | `Durations.Timestamp{unit}`, exact | `DateTime(x)`; or select the column as `:col => DateTime` |
| Timestamp with a zone | `TimeZones.ZonedDateTime` | `Durations.ZonedTimestamp{unit,zone}` | `ZonedDateTime(x)`, with TimeZones loaded |
| Decimal | `Arrow.Decimal{P,S,Int128}` | `DataDecimals.Decimal{P,S,T}` | |
| Interval | `Arrow.Interval` | `Durations.Duration` | |
| List | a `SubArray` view | `Arrow.ListValue{T}` view | `copy(x)` |
| FixedSizeList with nullable values | a `Tuple` | `Arrow.ListValue{T}` | `Tuple(x)` |
| Map | `Dict{K,V}` | `Arrow.MapValue{K,V}`, an `AbstractDict` in stored order | `Dict(x)`; `Dict{String,V}(x)` for `String` keys |
| Struct | `NamedTuple` | `NamedTuple`, with `DataString` fields for strings | |

A `DataString` compares and hashes equal to the `String` with the same characters, and a
`Timestamp` to the `DateTime` at the same time, so lookups, joins, and comparisons
against `String` and `DateTime` values keep working. Code that dispatches on, or asserts,
`String`, `DateTime`, or `Dict` element types needs the conversions above:

```@example migration
using Arrow, Dates

path = joinpath(mktempdir(), "values.arrow")
Arrow.write(path, (name=["Ada"], at=[DateTime(2025, 1, 1, 12)], attrs=[Dict("k" => 1)]))
table = Arrow.Table(path)
(table.name[1] == "Ada", String(table.name[1]), DateTime(table.at[1]),
    Dict{String,Int}(table.attrs[1]))
```

[Type mapping](@ref) lists every type.

Ordered dictionary columns keep their order in `DataAPI.levels`, and with
CategoricalArrays loaded `isordered(column)` reports it; see [Dictionary
columns](@ref) for rebuilding an ordered `CategoricalArray`.

`Symbol` columns intern at most `Arrow.Limits().max_new_symbols` new symbols per read
([Resource limits](@ref)).

## Writing changes

- A path still gets the file format and an `IO` the stream format; `file=` overrides.
- `Arrow.Writer` takes its schema from the first table, with every column nullable
  unless `nullable=false`. Later tables, and tables given to `Arrow.append`, must have
  the same columns with values that convert exactly (an `Int32` column fits an `Int64`
  one), or the write is an error naming the column. Arrow 2.x did not check them.
- `dictencode=false` writes `PooledArray` and `CategoricalArray` columns as plain values;
  2.x dictionary-encoded them regardless. `dictencode` also takes column names.
- Strings must be valid UTF-8; write other bytes as `Vector{UInt8}`.
- `UUID` columns write as the canonical `arrow.uuid` extension, which pyarrow and Polars
  read as UUIDs. Files from 2.x, which used a `JuliaLang.UUID` label, still read as
  `UUID`.
- `Int128` and `UInt128` write as 16-byte fixed-size binary labelled `JuliaLang.Int128`
  and `JuliaLang.UInt128`.
- A vector of `Pair`s writes as an Arrow map, where 2.x wrote a list of structs.
- `metadata` and `colmetadata` default to the table's DataAPI metadata, so a
  `DataFrame`'s metadata is written.
- New keywords: `compresslevel`, `batchsize`, `dictdeltas`, `stringview`, and, for
  `Arrow.Writer`, `nullable` and `dictreplacement` ([Writing](@ref)).

## Packages that define mappings

ArrowTypes.jl 2 keeps the 2.x interface. Define `ArrowTypes.ArrowType` and
`ArrowTypes.toarrow` to write a type as a supported one, and `ArrowTypes.arrowname`,
`ArrowTypes.JuliaType`, and `ArrowTypes.fromarrow` to read it back ([Custom
types](@ref)). Files written with 2.x mappings read back through the same definitions.
Overriding `ArrowTypes.ArrowKind` alone no longer chooses how a type is stored. A
package that defines mappings should depend on ArrowTypes.jl directly rather than reach
it through Arrow.

## Reading and writing alongside Arrow.jl 2.x

Arrow 3.0 reads every file Arrow 2.x writes, with the values 2.x reads, as the types
listed under [Values](@ref). The other direction has gaps: Arrow 2.x reads most files
Arrow 3.0 writes, but not all. While some readers still run Arrow 2.x, avoid these
columns or write them in the form the last column suggests:

| Written by Arrow 3.0 | Arrow 2.x reads | For 2.x readers, write |
|:--|:--|:--|
| `UUID` | `NTuple{16,UInt8}`, with a warning | `string.(uuids)` |
| `Int128`, `UInt128` | `NTuple{16,UInt8}`, with a warning | `string.(x)` |
| `Minute`, `Hour`, `Day`, `Week` | an `Arrow.Duration` of seconds, with a warning | `Dates.Second.(x)` |
| `Month`, `Year` | an `Arrow.Interval` of months, with a warning | `Dates.value.(x)` |
| `VersionNumber` | fails when the column is read | `string.(versions)` |
| `Durations.Duration` | cannot open the file | separate columns, or `string.(x)` |
| A dictionary column that gained values across partitions or `Arrow.Writer` tables, in the file format | with several threads, can fail with a `KeyError` | `dictdeltas=true`, or the stream format |
| Decimal32 or Decimal64, kept from a file another tool wrote | wrong values, without an error | `DataFrame(table)` first, which writes Decimal128 |

Arrow 2.x reads the other columns as its own types, as listed under [Values](@ref):
timestamps as `DateTime`, strings as `String`, maps as `Dict`s.

Arrow.jl releases before 2.4.2 stored `ZonedDateTime` columns as local times, labelled
`JuliaLang.ZonedDateTime`. Arrow 3.0 warns about that label and reads the stored values
as UTC instants, so rewrite such files with Arrow 2.x before reading them with 3.0.
