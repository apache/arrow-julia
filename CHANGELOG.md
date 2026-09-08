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

This file records user-visible changes to Arrow.jl. See the
[3.0 migration guide](docs/src/migration.md) for update instructions.

## 3.0.0 - Unreleased

Arrow.jl 3.0 is a breaking major release. It replaces the internal reader,
writer, validation, scan, and C interface engines.

### Breaking changes

- Julia 1.10 is now the minimum supported Julia version.
- `Arrow.Table` materializes plain Julia vectors. It no longer returns lazy
  `ArrowVector` views into the source.
- `Arrow.Table(...; convert=false)` and the related lazy conversion path were
  removed.
- Positional byte-window arguments and the multi-input `Table` and `Stream`
  constructors were removed. Each read accepts one complete source.
- `Arrow.write` is an eager, whole-buffer writer. `Arrow.Writer` (incremental
  writes, both formats) and `Arrow.append` (IPC streams) are reimplemented on
  the 3.0 core with one semantic change: the first table written fixes the
  schema, and later tables must conform — no inference crosses tables.
- Writing to an `IO` now emits the IPC file format by default. Pass
  `file=false` for the stream format. `Arrow.tobuffer` still emits the stream
  format, matching its 2.x bytes.
- The `alignment`, `dictencode`, `dictencodenested`, `denseunions`,
  `largelists`, `maxdepth`, and `ntasks` writer keywords no longer have any
  effect. `Arrow.write` accepts them with a one-time warning each and ignores
  them; wrap columns in `Arrow.DictEncode` to dictionary-encode.
- The `compress` keyword accepts `nothing`, `:lz4`, or `:zstd`; initialized
  compressor objects are no longer accepted.
- The `Arrow.ToArrow` compatibility binding was removed. Import
  `ArrowTypes.ToArrow` directly when an explicit lazy conversion view is
  needed. Normal writes apply `ArrowTypes.ArrowType` and `ArrowTypes.toarrow`
  automatically.
- The DataAPI.jl metadata interface replaces the Arrow 2.x metadata
  accessors. `Arrow.getmetadata(table)` remains as a compatibility method
  over it; the per-column `getmetadata(column)` form is gone because columns
  are plain vectors — use `DataAPI.colmetadata` instead.
- The package now has a narrow export surface. Use names such as
  `Arrow.Table`, `Arrow.Stream`, `Arrow.write`, and `Arrow.DictEncode` through
  the `Arrow` namespace. Only `release!` and `ArrowTypes` are exported.
  Packages that define custom mappings should still depend on and import
  ArrowTypes.jl directly.
- Big-endian IPC and delta-dictionary messages are rejected.
- Arrow 3.0 requires ArrowTypes.jl 2.x, Tables.jl 1.14 (the first release
  with `Tables.Scan`), DataStrings.jl 1.0, DataDecimals.jl 1.0, and Durations.jl 1.0.
- Supported top-level decimals and calendar intervals use shared DataDecimals
  and Durations values. Negative-scale decimals retain raw coefficients.

### Added

- `Tables.Scan` projection, filter, limit, and offset pushdown.
- Typed `Tables.Scan` select overrides for composite rows: a
  `:column => NamedTuple{...}` select item reads a Struct column as typed
  rows (recursively, including `Vector{...}` targets for list columns).
- A TimeZones.jl extension. When TimeZones.jl is loaded, second- and
  millisecond-unit timestamps that declare a timezone read as
  `ZonedDateTime` (the Arrow 2.x behavior) and round-trip through rewrite,
  and a fresh single-zone `ZonedDateTime` column writes as a
  timezone-declared millisecond timestamp; without the extension those
  columns read as naive UTC `DateTime` values.
- The Arrow 2.x compatibility surface: exported `ArrowTypes`,
  `Arrow.getmetadata(table)`, `Arrow.tobuffer`, and the curried
  `table |> Arrow.write(sink)` form.
- An incremental writer: `Arrow.Writer(sink; file=true)` publishes each
  written table's batches immediately, holding only the current table in
  memory, for both IPC formats. `Arrow.append(sink, table)` extends an
  existing IPC stream in place. `Arrow.Writer(sink; dictreplacement=true)`
  declares the DictionaryReplacement feature up front so a stream's pools
  can be replaced by later writes or appends; declared features are demands
  on readers, so nothing is declared speculatively.
- The IPC reader accepts vtables that understate a wider field's extent,
  as the reference implementation does. Arrow 2.x's FlatBuffers builder
  produced them for every dictionary-encoded field carrying metadata, so
  Arrow 3.0 now reads every such 2.x file (including all CategoricalArrays
  columns) that earlier 3.0 development builds refused.
- `Arrow.AbstractArrowSource` for sparse byte-range reads.
- A CloudStore.jl extension for remote object reads.
- Footer statistics that can prune record batches before their data is read.
- Arrow C data and C stream import and export.
- Arrow StringView and BinaryView support. DataStrings.jl provides a reusable
  zero-copy StringView representation for Arrow.jl and compatible producers.
- Fresh Julia columns with a heterogeneous declared `Union` element type are
  synthesized as canonical dense Arrow Union arrays. Each child uses the
  recursive core or ArrowTypes.jl mapping supported at that nesting depth.
- Recursive ArrowTypes.jl custom-type lowering and extension-type restoration
  for top-level values and values nested in lists, tuples and fixed-size lists,
  structs, maps, dictionary-encoded values, and freshly synthesized
  heterogeneous Unions.
- Structural, semantic, and optional full-content validation tiers.
- Resource limits for untrusted IPC metadata and buffers.
- Apache Arrow gold-corpus tests, external IPC oracle tests, C interface oracle
  tests, deterministic differential and bounded mutation fuzzing, and a JuliaC
  `--trim=safe` compile gate.

### Changed

- `Arrow.Stream` yields materialized `Arrow.Table` batches.
- A file-format path is memory-mapped by default. Call `Arrow.release!` to release
  the map at a known time. Materialized table columns remain usable afterward.
- A table read from Arrow retains compatible schema details when it is written
  again, including temporal units, dictionary encoding and category order,
  list widths, composite descriptors, nullability, and ordered duplicate
  metadata. Fresh heterogeneous Julia Union columns can be synthesized.
  Registered ArrowTypes.jl public-domain values can also reconstruct a retained Union
  from their writer-side type evidence, including dense or sparse mode and type
  IDs. An unregistered retained Union still fails clearly after facade
  materialization discards its original routing.
  Nested Dictionaries fail clearly after their pool data is lost.
- Writing is validated before bytes are published to the output sink.
- Fresh `Union{Missing, NamedTuple}` columns use Struct parent validity while
  preserving each child's declared nullability and type.
- Custom values are lowered recursively through `ArrowTypes.ArrowType` and
  `ArrowTypes.toarrow`. Extension names and metadata are written, and reads use
  `ArrowTypes.JuliaType`, `ArrowTypes.fromarrow`, and
  `ArrowTypes.fromarrowstruct` to restore registered logical types. An unknown
  extension name warns and returns its storage value.
- A fresh abstract column with no mapping of its own uses concrete subtype
  evidence across the complete column. Concrete subtype extensions are kept,
  and heterogeneous subtype evidence forms an explicit bounded Union instead
  of silently erasing subtype metadata.
- Registered logical types whose storage is a Union preserve external child
  order, child labels, type IDs, dense or sparse mode, nested descriptor details,
  and outer-null routing on rewrite. Sparse children use canonical hidden
  placeholders outside their active rows. An abstract registered target accepts
  an extensionless concrete subtype or one with the retained parent identity; a
  different explicit identity fails closed. A logical storage Union that already
  uses `Missing` cannot also add an outer missing state because the two states
  have no distinguishable Arrow representation.
- Hidden retained composite slots are built directly from their Field and
  logical length. Null-only fixed-size-list descendants do not allocate one
  Julia placeholder per hidden element, including partly missing fresh or
  retained columns and inactive sparse-Union rows. Sparse Union children also
  distinguish inactive physical slots from selected values, so inactive null
  storage does not weaken selected-value validation.
- Recursive ArrowTypes.jl storage schemas, recursive value containers, and
  custom mapping nesting beyond 64 levels fail with `ArgumentError`. The
  removed `maxdepth` keyword does not make writer recursion unbounded.
- Declared writer Unions support up to 32 branches. Runtime writer or storage
  inference accepts at most 8 distinct types across the complete column. This
  covers abstract ArrowTypes storage, abstract or `Any` dictionary values, and
  abstract retained ArrowTypes targets, and bounds per-type schema planning and
  compiler work.
- ArrowTypes logical resolution uses exact fixed-size-list tuple signatures
  through arity 1024 and a compact tuple-family signature above that limit,
  preventing fixed-size-list sizes from causing proportional type allocation.
- Extension Struct signatures are exact through 1024 children only when child
  names are unique, contain no embedded NUL, already exist as Julia `Symbol`s,
  and pass the 4096-byte per-name and 64-KiB total limits. Otherwise labelled
  Structs remain unknown extensions and return ordered `Pair` storage.
- One bounded ArrowTypes.jl Tuple compatibility exception may intern only a
  complete canonical positional sequence `"1"`, `"2"`, …, `string(N)` through
  `N = 1024`. Unknown extension labels return before that check; arbitrary or
  partly positional Struct names are not interned.
- Any writer-side `ArrowType` result that is a concrete tuple with more than
  1024 fields is rejected before the writer specializes on the oversized
  storage shape. This includes ArrowTypes.jl's default mapping for a tuple
  value.
- Core keeps schema names as strings. The Tables.jl facade preflights per-name,
  novel-name-count, and novel-name-byte limits before it interns top-level
  column names. Unknown ArrowTypes extension labels do not create Julia
  symbols. Unsupported-label warnings are deduplicated by label and capped at
  16 distinct labels plus one suppression notice per table materialization. A
  novel `JuliaLang.Symbol` IPC payload now raises `ValidationError` instead of
  being interned, so an input that an earlier Arrow.jl release read successfully
  can now fail.
- Registered public-domain values can rebuild compatible retained binary, list,
  date-like, duration, wide-decimal, and interval descriptors. Retained widths,
  sizes, units, child fields, and sorted Map claims are checked before output.
- Empty and typed all-missing registered columns use their concrete declared
  element types as schema evidence instead of bypassing retained-field checks.
- Scan filters over fields that contain registered ArrowTypes.jl logical types
  at any depth evaluate over the public materialized values. The mapping
  interface does not require storage lowering to preserve Julia comparison
  semantics.
- Removed the JSON3/StructTypes-based arrow-JSON test integration and the
  associated support claims. Arrow 2.x used those packages only in tests; no
  public API existed.
