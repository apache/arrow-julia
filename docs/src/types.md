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

# Type mapping

## Arrow to Julia

A column's element type comes from its Arrow type alone, never from its values, so an
empty or all-missing column has the same type as a full one. A nullable field's elements
are `Union{Missing,T}`.

| Arrow type | Julia element type |
|:--|:--|
| Int8-Int64, UInt8-UInt64, Float16-Float64, Bool | the same type |
| Utf8, LargeUtf8, Utf8View | `DataStrings.DataString` (the column is a `StringVector`); `String` with `stringtype=String` |
| Binary, LargeBinary, BinaryView | `DataStrings.DataBytes` (the column is a `BytesVector`) |
| FixedSizeBinary(N) | `NTuple{N,UInt8}`; `DataBytes` when N > 256 |
| Date32, Date64 | `Dates.Date`, `Dates.DateTime` |
| Time32, Time64 | `Dates.Time` |
| Timestamp(unit) | `Durations.Timestamp{unit}` |
| Timestamp(unit, zone) | `Durations.ZonedTimestamp{unit,Symbol(zone)}` |
| Duration(unit) | `Dates.Second`, `Millisecond`, `Microsecond`, or `Nanosecond` |
| Interval | `Durations.Duration` |
| Decimal32/64/128/256(P, S) | `DataDecimals.Decimal{P,S,T}`, `T` the 32- to 256-bit integer of the type's width |
| List, LargeList, ListView, LargeListView | [`Arrow.ListValue{T}`](@ref Arrow.ListValue) |
| FixedSizeList(N) | `NTuple{N,T}`, or `ListValue{T}` when N > 64 or values may be null |
| Struct | a `NamedTuple` |
| Map | [`Arrow.MapValue{K,V}`](@ref Arrow.MapValue) |
| Dense and sparse Union | a `Union` of the children's types |
| Dictionary, run-end encoded | the values' type, in an [`Arrow.DictEncoded`](@ref) or [`Arrow.RunEndEncoded`](@ref) column |
| Null | `Missing` |

Extension types read as their Julia type: canonical `arrow.uuid` as `UUIDs.UUID`,
`arrow.bool8` as `Bool`, `arrow.fixed_shape_tensor` as `Array{T,N}` rows, and the labels
Arrow.jl writes ([Julia to Arrow](@ref)) as the Julia type that wrote them. `arrow.json`
and `arrow.opaque` read as their storage, strings or bytes. Another extension name reads
as its storage too, with one warning per name per process, unless a package defines a
mapping for it ([Custom types](@ref)).

A decimal with a negative scale, or a scale above its precision, reads as it is stored:
integers for Decimal32 and Decimal64, little-endian bytes for Decimal128 and Decimal256.
A decimal value with more digits than its precision reads as stored, as pyarrow reads
it. A time of day outside [0, 24 h) has no `Dates.Time` value and fails the read
with an error naming the column.

## Julia to Arrow

Each column's element type decides its Arrow type, at any depth. Values read back as the
same type unless the table says otherwise.

| Julia element type | Arrow type | Reads back as |
|:--|:--|:--|
| `Int8`-`Int64`, `UInt8`-`UInt64`, `Float16`-`Float64`, `Bool` | the same type | the same type |
| `AbstractString` (`String`, `SubString`, `DataString`) | Utf8 | `DataString` |
| InlineStrings' `String1`-`String255` | Utf8, labelled by InlineStrings | the same type |
| `Symbol`, `Char` | Utf8, UInt32; labelled | `Symbol`, `Char` |
| `Vector{UInt8}`, `DataBytes` | Binary | `DataBytes` |
| `NTuple{N,UInt8}` | FixedSizeBinary(N) | `NTuple{N,UInt8}` |
| `Int128`, `UInt128` | FixedSizeBinary(16), labelled `JuliaLang.Int128`/`UInt128` | the same type |
| `UUID` | `arrow.uuid` | `UUID` |
| `Date`, `Time` | Date32, Time64(nanosecond) | `Date`, `Time` |
| `DateTime` | Timestamp(millisecond) | `Durations.Timestamp{Millisecond}` |
| `Durations.Timestamp{P}` | Timestamp(P) | `Durations.Timestamp{P}` |
| `Durations.ZonedTimestamp{P,Z}` | Timestamp(P, zone `Z`) | `Durations.ZonedTimestamp{P,Z}` |
| `TimeZones.ZonedDateTime` (with TimeZones loaded), one zone per column | Timestamp(millisecond, zone) | `ZonedTimestamp{Millisecond,Z}` |
| `Nanosecond`-`Second` | Duration of that unit | the same type |
| `Minute`, `Hour`, `Day`, `Week` | Duration(second), labelled | the same type |
| `Month`, `Year` | Interval(year-month), labelled | the same type |
| `Durations.Duration` | Interval(month-day-nano) | `Durations.Duration` |
| `DataDecimals.Decimal{P,S}` | Decimal128(P, S), or Decimal256 when P > 38 | `Decimal{P,S}`, stored in `Int128` or `Int256` |
| `PooledArray`, `CategoricalArray` | Dictionary of the pool's values; ordered if the array is | an `Arrow.DictEncoded` column |
| `AbstractVector{T}` | List | `ListValue{T}` |
| `NTuple{N,T}` | FixedSizeList(N) | `NTuple{N,T}` |
| other `Tuple`s | Struct with fields `1`, `2`, ..., labelled | the same type |
| `NamedTuple` | Struct | `NamedTuple` |
| `AbstractDict{K,V}`, `AbstractVector{<:Pair}` | Map | `MapValue{K,V}` |
| `Union{Missing,T}` | the type of `T`, nullable | `Union{Missing,T}` |
| `Union{T1,T2,...}` | dense Union | a `Union` of the members' types as read |
| `VersionNumber`, `Nothing` | Utf8, Null; labelled | the same type |
| `Missing` only | Null | `Missing` |

"Labelled" means the column carries an `ARROW:extension:name` such as
`JuliaLang.Symbol`, so Arrow.jl reads the Julia type back; other readers see the
storage type. SentinelArrays' `ChainedVector` and CSV.jl's columns write without being
collected first.

A column whose element type is abstract (`Any`, `Real`, a `NamedTuple` with open field
types) is written as the types its values have: one type writes as that type, and up to
8 types as a dense Union of them. A column of `Timestamp`s, `ZonedTimestamp`s, or
`Decimal`s that mixes units, zones, or precisions is an error, since one Arrow column has
one of each (`Durations.astimezone` gives a column one zone). A type with no Arrow type,
such as `BigInt`, an `@enum`, or a `Matrix`, is an error that names the column; map it
with [Custom types](@ref), or convert it first.

## Timestamps

Timestamps read as `Durations.Timestamp{P}` and `Durations.ZonedTimestamp{P,Z}` at the
column's unit: 8-byte values that hold the stored instant exactly, at every unit, with no
time-zone database. A `Timestamp` compares and hashes equal to the `DateTime` at the
same time, so a `DateTime` column reads back equal to what was written, and
`DateTime(ts)` converts (truncating below a millisecond).

A `ZonedTimestamp{P,Z}` holds a UTC instant and its zone `Z`. Instants compare with each
other across zones, but never equal a `DateTime`, which has no zone; `DateTime(zt)` gives
the local time in `Z`. UTC and fixed offsets such as `+05:30` need nothing else; named
zones such as `America/New_York` need TimeZones.jl loaded for local times and for
`ZonedDateTime(zt)`.

```@example types
using Arrow, Dates, Durations

io = IOBuffer()
Arrow.write(io, (at=[DateTime(2025, 1, 1, 12)],
    utc=[Durations.ZonedTimestamp{Millisecond,:UTC}(2025, 1, 1, 12)]))
table = Arrow.Table(take!(io))
table.at
```

```@example types
table.at[1] == DateTime(2025, 1, 1, 12)
```

```@example types
table.utc
```

To read a timestamp column as `DateTime`s, give the type in `select`:
`Arrow.Table(path; select=[:id, :at => DateTime])`.

## Decimals

Decimals read as `DataDecimals.Decimal{P,S,T}` whose storage integer `T` matches the
Arrow type's width: `Int32` for Decimal32, `Int64` for Decimal64, `Int128` for
Decimal128, and `DataDecimals.Int256` for Decimal256. They write as Decimal128 up to 38
digits and Decimal256 above, as pyarrow, Polars, and DuckDB write them, whatever the
storage of the Julia value. So `dec"1.25"`, a `Decimal{3,2,Int32}`, reads back as the
equal `Decimal{3,2,Int128}`. Rewriting a column read from a Decimal32 or Decimal64 field
keeps that width.

## Strings and bytes

A `DataStrings.DataString` is a 16-byte value: strings of up to 12 bytes are stored in
the value, longer ones refer to the column's bytes. It is an `AbstractString` that
compares and hashes equal to the `String` with the same characters; `String(s)` makes
one. A `StringVector` writes as Utf8 in one pass over its values, or as Utf8View with
`stringview=true`. DuckDB 1.5's `read_arrow` and other older readers refuse the view
types, so they are written only on request.

## Nested values

Lists, maps, and structs read as typed rows, in [`Arrow.ListVector`](@ref),
[`Arrow.MapVector`](@ref), and [`Arrow.StructVector`](@ref) columns:

- A list row is an [`Arrow.ListValue{T}`](@ref Arrow.ListValue), a read-only
  `AbstractVector{T}` view of the column's values; `copy(row)` gives a `Vector{T}`.
- A struct row is a `NamedTuple` of its fields, built when it is read. A struct with
  more than 64 fields, or with repeated field names, reads as a `Vector` of
  `name => value` pairs instead.
- A map row is an [`Arrow.MapValue{K,V}`](@ref Arrow.MapValue), an `AbstractDict{K,V}`
  over the map's entries in stored order: `row[key]`, `get`, and `haskey` look keys up,
  and iteration gives `key => value` pairs, duplicate keys included (a lookup finds the
  last). `Dict(row)` builds a `Dict`.
- A union value is a value of one of the children's types.

`T` includes `Missing` only when the schema allows null elements, and a null list,
struct, or map reads as `missing`. Rows stay valid after their table is gone. Copies of a
nested column (`copy`, `DataFrame(table)`) are `Vector`s of plain rows, and the writer
takes plain rows back: vectors, named tuples, dictionaries, and vectors of pairs.

```@example types
io = IOBuffer()
Arrow.write(io, (tags=[["a", "b"], String[]], point=[(x=1.0, y=2.0), (x=3.0, y=4.0)],
    attrs=[Dict("k" => 1), Dict("j" => 2, "k" => 3)]))
nested = Arrow.Table(take!(io))
nested.attrs
```

```@example types
(nested.point[2].y, nested.attrs[2]["k"], copy(nested.tags[1]))
```

Each level of a typed row nests one more type, and Julia compiles deep ones slowly. A
column nested more than 8 levels deep therefore reads with untyped rows: lists as
`Vector{Any}` and structs as `name => value` pairs.

## Dictionary columns

A dictionary column reads as an [`Arrow.DictEncoded`](@ref) of its values' type.
`DataAPI.refarray`, `DataAPI.refpool`, and `DataAPI.levels` expose the encoding without
decoding it; `levels` follows the dictionary's order. `copy(column)` gives a
`PooledArray`. With CategoricalArrays.jl loaded, `isordered(column)` tells whether the
dictionary is ordered, and `categorical` rebuilds a `CategoricalArray` with its order:

```@example types
using CategoricalArrays, DataAPI

sizes = categorical(["small", "large", "medium"]; levels=["small", "medium", "large"],
    ordered=true)
io = IOBuffer()
Arrow.write(io, (size=sizes,))
column = Arrow.Table(take!(io)).size
cat = categorical(column; levels=DataAPI.levels(column), ordered=isordered(column))
(levels(cat), cat[1] < cat[2])
```

## Custom types

[ArrowTypes.jl](https://github.com/apache/arrow-julia/tree/main/src/ArrowTypes) maps
Julia types to Arrow types. A package that owns a type depends on ArrowTypes and defines
`ArrowTypes.ArrowType` and `ArrowTypes.toarrow` to write it as a supported type; to read
it back, it also names an extension with `ArrowTypes.arrowname` and defines
`ArrowTypes.JuliaType` and `ArrowTypes.fromarrow`:

```@example types
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
Arrow.write(io, (id=AccountID.(1:3),))
Arrow.Table(take!(io)).id
```

The mapping applies at any depth: in lists, structs, maps, unions, and dictionaries.
`ArrowTypes.arrowmetadata` adds a string the reader passes to `JuliaType`. Without a
mapping, a struct with supported field types writes as an Arrow struct and reads back as
a `NamedTuple`. In a process that has not loaded the mapping, the column reads as its
storage, with one warning.

## Rewriting tables

Writing an `Arrow.Table` or `Arrow.Stream` keeps its schema: each column's Arrow type,
with its units, offset widths, union mode, dictionary index type and order, run-end
encoding, and list views, along with nullability and metadata. Copying it into Julia
vectors first (`DataFrame(table)`, `copy(table)`) keeps only what the Julia types say,
so some types change on the way back while the values stay equal:

| Read from | Written from Julia vectors as |
|:--|:--|
| LargeUtf8, Utf8View; LargeBinary, BinaryView | Utf8; Binary |
| LargeList, ListView, FixedSizeList with nullable values | List |
| Date64 | Timestamp(millisecond) |
| Time32, Time64(microsecond) | Time64(nanosecond) |
| Decimal32, Decimal64 | Decimal128 |
| Dictionary with Int16-Int64 indices | Dictionary with the narrowest indices |
| Ordered Dictionary | Dictionary, not ordered: a `PooledArray` has no order ([Dictionary columns](@ref) shows how to keep it) |
| Run-end encoded | plain values |
| Sparse or dense Union | dense Union with a Null child |
| `arrow.bool8`, `arrow.json` | Bool, Utf8 |
| Interval(year-month or day-time) | Interval(month-day-nano) |

A union with two children of the same type cannot be rewritten as it was, because its
values do not say which child they came from; writing it is an error that suggests
`Arrow.write(path, DataFrame(table))`. A dictionary column nested inside another column
is refused the same way.
