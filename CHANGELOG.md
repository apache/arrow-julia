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

# Changelog

## 3.0.0 - Unreleased

Arrow.jl 3.0 is a new implementation of the reader, the writer, and the C data
interface. The main names stay: `Arrow.Table`, `Arrow.Stream`, `Arrow.write`,
`Arrow.Writer`, `Arrow.append`, `Arrow.tobuffer`, `Arrow.DictEncode`, and
`Arrow.getmetadata`. The [migration guide](docs/src/migration.md) shows how to update
code, and what Arrow 2.x can read of files Arrow 3.0 writes.

### Breaking

- Requires Julia 1.10, ArrowTypes.jl 2, Tables.jl 1.14, DataStrings.jl 1,
  DataDecimals.jl 1, and Durations.jl 1.4. TimeZones.jl and BitIntegers.jl are no
  longer dependencies.
- Value types: strings read as `DataStrings.DataString` and binary as `DataBytes`
  (`stringtype=String` reads `String`s); timestamps as `Durations.Timestamp{P}` at the
  column's unit, no longer truncated to `DateTime`; zoned timestamps as
  `Durations.ZonedTimestamp{P,Z}` instead of `ZonedDateTime`; decimals as
  `DataDecimals.Decimal`; intervals as `Durations.Duration`.
- Nested values: list rows are `Arrow.ListValue` views; map rows are `Arrow.MapValue`s,
  an `AbstractDict` in stored order, instead of `Dict`s; fixed-size lists read as tuples
  only when they have at most 64 values and no nulls.
- Columns are read-only; assigning into one is an error. Copy with `copy(table)`,
  `DataFrame(table)`, or `Arrow.read(source, DataFrame)`.
- Removed: the `convert` keyword, `Arrow.Table(bytes, pos, len)`, reading a vector of
  sources as one table or stream, `Arrow.ToTimestamp`, and the `Arrow.ToArrow`
  binding. Compressor objects are no longer accepted by `compress`.
- Unknown keywords are an `ArgumentError`. The 2.x writer keywords `alignment`,
  `dictencodenested`, `denseunions`, and `maxdepth` are ignored with a warning.
- `Arrow.Writer` takes its schema from the first table, with every column nullable
  unless `nullable=false`; later tables, and tables given to `Arrow.append`, must
  convert to the schema exactly.
- `dictencode=false` writes `PooledArray` and `CategoricalArray` columns as plain values.
- Writing a string that is not valid UTF-8 is an error.
- New column encodings that Arrow 2.x reads differently or not at all: `UUID` as the
  canonical `arrow.uuid` extension; `Int128` and `UInt128` as 16-byte fixed-size binary
  labelled `JuliaLang.Int128`/`UInt128`; `Minute`, `Hour`, `Day`, and `Week` as
  durations in seconds and `Month` and `Year` as year-month intervals, labelled;
  `VersionNumber` as labelled strings; `Durations.Duration` as month-day-nano intervals.
- A `Symbol` column interns at most `Limits.max_new_symbols` new symbols per read.
- Columns that share a name read as `a`, `a_1`, `a_2`, as in CSV.jl.
- Rejected input: big-endian IPC, metadata versions before V4, bytes after a stream's
  end marker in `Arrow.Table` (read several streams with one `Arrow.Stream` each), and
  a time of day outside [0, 24 h), which 2.x wrapped.

### Added

- `select`, `drop`, `filter`, `limit`, and `scan` (a `Tables.Scan`) on `Arrow.Table`,
  `Arrow.Stream`, and `Arrow.read`, with the filter functions `Arrow.col`,
  `Arrow.colcmp`, `Arrow.colin`, and `Arrow.isnull`.
- `Arrow.read(source, sink)` reads into a sink with owned columns.
- `Arrow.AbstractArrowSource` (`Arrow.sourcelength`, `Arrow.readrange`,
  `Arrow.concurrentreads`) reads only the byte ranges a read needs, and a CloudStore.jl
  extension reads S3 and Azure objects that way.
- `Arrow.Stream` reads a stream-format `IO` one record batch at a time as it arrives,
  and gives file-format batches by index (`stream[i]`), with `length`, `DataAPI.nrow`,
  `Tables.schema`, and DataAPI metadata without reading data.
- `ntasks` on reads. Reads and writes give the same values and bytes for every `ntasks`.
- `Arrow.release!`, `copy(table)`, `Arrow.Limits`, `Arrow.ValidationError`, and
  `Arrow.AllocationLimitError`.
- Writer keywords `compresslevel`, `batchsize`, `dictdeltas` (delta dictionaries),
  `stringview` (string and binary views), and, on `Arrow.Writer`, `nullable` and
  `dictreplacement` (replacement dictionaries); `dictencode` takes column names.
- Reading list views, run-end encoding, month-day-nano intervals, and the canonical
  `arrow.uuid`, `arrow.bool8`, and `arrow.fixed_shape_tensor` extensions. Rewriting a
  table keeps these types.
- The C data and C stream interfaces (`Arrow.to_c_data`, `Arrow.from_c_data`,
  `Arrow.export_stream!`, `Arrow.from_c_stream`).
- A TimeZones.jl extension that writes `ZonedDateTime` columns, and a CategoricalArrays
  extension that writes ordered `CategoricalArray`s as ordered dictionaries and reports
  `isordered` for ordered dictionary columns.

### Changed

- On Windows, a path is read into memory unless `mmap=true`, since Windows cannot
  delete a mapped file.
- String columns are `StringVector`s, built once with one copy of their bytes; 2.x
  allocated a new `String` on every access.
- `Arrow.write` writes partition by partition, so a partitioned source such as
  `CSV.Chunks` writes in about one partition's memory, and validates each record batch
  before writing it.
- Writing to an existing path writes a new file and renames it over the old one: a
  failed write leaves the old file, and tables that map the old file keep working.
- Decimals write as decimal128, or decimal256 above 38 digits, whatever the storage of
  the Julia value.
- `metadata` and `colmetadata` default to the table's DataAPI metadata.
- A table with several partitions writes every column as nullable.
- A buffer that compression would not shrink is written uncompressed.

### Fixed

- Polars' IPC files read; 2.x read them as an empty table.
- Decimal32 and Decimal64 columns read their values; 2.x read them as 128-bit values.
- Timestamps finer than a millisecond read exactly.
- Corrupt or hostile input fails with a `ValidationError` or `AllocationLimitError`
  instead of crashing, hanging, or allocating without bound.
