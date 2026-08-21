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
- `Arrow.Writer`, `Arrow.append`, and incremental or append-to-file writes were
  removed. `Arrow.write` is an eager, whole-buffer writer.
- Writing to an `IO` now emits the IPC file format by default. Pass
  `file=false` for the stream format.
- The curried `Arrow.write(sink)` form and `Arrow.tobuffer` were removed.
- The `ntasks` multithreaded encoding option was removed.
- The `alignment`, `dictencode`, `dictencodenested`, `denseunions`,
  `largelists`, and `maxdepth` writer keywords were removed.
- The `compress` keyword accepts `nothing`, `:lz4`, or `:zstd`; initialized
  compressor objects are no longer accepted.
- The `Arrow.ToArrow` compatibility binding was removed. Import
  `ArrowTypes.ToArrow` directly when an explicit lazy conversion view is
  needed. Normal writes apply `ArrowTypes.ArrowType` and `ArrowTypes.toarrow`
  automatically.
- `ArrowTypes` is no longer exported. Import ArrowTypes.jl directly when
  defining a custom mapping. `Arrow.ArrowTypes` remains available as a
  qualified compatibility binding.
- `Arrow.getmetadata` was replaced by the DataAPI.jl metadata interface.
- The package now has a narrow export surface. Use names such as
  `Arrow.Table`, `Arrow.Stream`, `Arrow.write`, and `Arrow.DictEncode` through
  the `Arrow` namespace. Only `close!` is exported.
- Big-endian IPC and delta-dictionary messages are rejected.
- Arrow 3.0 requires ArrowTypes.jl 2.x, a Tables.jl release that provides
  `Tables.Scan`, and the ArrowStrings.jl 1.0 release. The final Tables.jl lower
  compat bound will be set after that Tables.jl release is registered.

### Added

- `Tables.Scan` projection, filter, limit, and offset pushdown.
- `Arrow.AbstractArrowSource` for sparse byte-range reads.
- A CloudStore.jl extension for remote object reads.
- Footer statistics that can prune record batches before their data is read.
- Arrow C data and C stream import and export.
- Arrow StringView and BinaryView support. ArrowStrings.jl provides a reusable
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
  tests, and a JuliaC `--trim=safe` compile gate.

### Changed

- `Arrow.Stream` yields materialized `Arrow.Table` batches.
- A file-format path is memory-mapped by default. Call `Arrow.close!` to release
  the map at a known time. Materialized table columns remain usable afterward.
- A table read from Arrow retains compatible schema details when it is written
  again, including temporal units, dictionary encoding and category order,
  list widths, composite descriptors, nullability, and ordered duplicate
  metadata. Fresh heterogeneous Julia Union columns can be synthesized, but a
  retained Union still fails clearly after facade materialization discards its
  original routing. Nested Dictionaries fail clearly after their pool data is
  lost.
- Writing is validated before bytes are published to the output sink.
- Custom values are lowered recursively through `ArrowTypes.ArrowType` and
  `ArrowTypes.toarrow`. Extension names and metadata are written, and reads use
  `ArrowTypes.JuliaType`, `ArrowTypes.fromarrow`, and
  `ArrowTypes.fromarrowstruct` to restore registered logical types. An unknown
  extension name warns and returns its storage value.
- Scan filters over fields that contain registered ArrowTypes.jl logical types
  at any depth evaluate over the public materialized values. The mapping
  interface does not require storage lowering to preserve Julia comparison
  semantics.
