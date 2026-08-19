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

> **This is the Arrow.jl 3.0 development branch.** The last 2.x release
> lives on its release tags. `Arrow.Table`, `Arrow.Stream`, `Arrow.write`,
> `Arrow.close!`, the byte-range readers and the C data / C stream entry
> points are the public surface (see `docs/src/reference.md`); the engine
> beneath them is exercised by the test batteries, the apache/arrow-testing
> conformance corpus, and the pyarrow/nanoarrow oracle suites. Until
> `Tables.Scan` ships in a Tables.jl release, this branch needs Tables.jl's
> `jq/scan` branch: `Pkg.add(url="https://github.com/JuliaData/Tables.jl", rev="jq/scan")`.

This is a pure Julia implementation of the
[Apache Arrow](https://arrow.apache.org) data standard.

## Layout

- `src/ArrowCore.jl` — the private core: ownership regions, layout
  registry, `ArrayData`, staged validation, accessors. Dependency-free and
  trim-friendly.
- `src/metadata/` — FlatBuffers metadata bindings and shape verifier,
  both GENERATED from the vendored spec schemas (`src/metadata/fbs/`) by
  `tools/fbsgen.jl`.
- `src/ipc_read.jl`, `src/ipc_write.jl` — the IPC stream and file
  formats: framing, resource limits, compression, dictionary lifecycles.
- `src/cdata.jl` — the C data and C stream interfaces, import and export.
- `src/source.jl` — the `AbstractArrowSource` interface: what a
  byte-range-addressable object (cloud storage, HTTP, …) provides so
  `Arrow.Table` can fetch only the bytes a scan touches.
- `src/scan.jl` — `Tables.Scan` pushdown over byte ranges plus
  footer-carried statistics pruning.
- `src/table.jl`, `src/write.jl` — the public facade: `Arrow.Table`,
  `Arrow.Stream`, `Arrow.write`, `close!`.
- `ext/ArrowCloudStoreExt.jl` — CloudStore.jl objects as sources (S3,
  Azure Blob Storage, GCS) with concurrent range reads.
- `src/FlatBuffers/` — the vendored FlatBuffers runtime the generated
  bindings run over.
- `src/ArrowStrings/` — ArrowStrings.jl, a separate package (to be
  registered on its own, like `src/ArrowTypes/`; until its first release
  Arrow depends on it through the `[sources]` path entry in `Project.toml`):
  the inline-else-view string representation shared with CSV.jl, whose
  column memory is an Arrow Utf8View array.
- `bench/` — the serialize/deserialize benchmark harness (this package,
  Arrow.jl 2.x, PyArrow) over identical workloads.
- `test/` — core unit tests, the facade tests, the four adapter acceptance
  batteries, the frozen 2.x-written compatibility fixtures
  (`test/fixtures2x/`), and the `--trim=safe` compile gate.
- `conformance/` — the arrow-testing gold-corpus runner, the integration
  JSON implementation, the pyarrow/nanoarrow IPC oracle round-trip suite,
  and the in-process pyarrow C Data / C Stream oracle — all run inside one
  docker image by `conformance/run.jl` (Harbor.jl); docker is the only
  host requirement (plus network on the first run, to fetch Harbor.jl and
  build the image).
- `docs/src/` — the published user manual and API reference (`docs/make.jl`).
- `docs/dev/` — the engine design document, the scan/ranged-fetch design
  notes, the FlatBuffers/C-data research notes, and the review record.

The design rationale for every layer is `docs/dev/core-README.md`.

## Status

Conformance: 275/275 gold-corpus checks pass (36 declared skips);
170/170 IPC oracle round-trips against pyarrow and nanoarrow (43 skips are
oracle capability gaps); 143/143 C Data and C Stream interface round-trips
through an in-process pyarrow over the whole gold matrix (both directions,
pyarrow-native memory, sliced exports; 9 declared skips). Run them all with
`julia conformance/run.jl` (docker is the only host requirement, plus
network on the first run).
