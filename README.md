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

> **This is the Arrow.jl 3.0 development branch.** The 2.x implementation
> has been replaced by a ground-up rewrite; the last 2.x release lives on
> its release tags. The user-facing API (`Arrow.Table`, `Arrow.Stream`,
> writers and builders) is the rewrite's next arc — until it lands, this
> branch is engine + adapters, exercised by the test batteries, the
> apache/arrow-testing conformance corpus, and a pyarrow/nanoarrow oracle
> suite.

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
- `src/cdata.jl` — the C data interface, import and export.
- `src/scan.jl` — `Tables.Scan` pushdown over byte ranges plus
  footer-carried statistics pruning.
- `test/` — core unit tests, the four adapter acceptance batteries, the
  frozen 2.x-written compatibility fixtures (`test/fixtures2x/`), and the
  `--trim=safe` compile gate.
- `conformance/` — the arrow-testing gold-corpus runner, the integration
  JSON implementation, the pyarrow/nanoarrow IPC oracle round-trip suite,
  and the in-process pyarrow C Data / C Stream oracle.
- `docs/dev/` — the engine design document and the codex review record of
  the rewrite (rounds 1–28 so far).

The design rationale for every layer is `docs/dev/core-README.md`.

## Status

Conformance: 275/275 gold-corpus checks pass (36 declared skips);
170/170 IPC oracle round-trips against pyarrow and nanoarrow (43 skips are
oracle capability gaps); 141/141 C Data and C Stream interface round-trips
through an in-process pyarrow over the whole gold matrix (both directions,
pyarrow-native memory, sliced exports; 9 declared skips). See
`conformance/` to run any of them.
