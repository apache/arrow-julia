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

# Arrow.jl 3.0 replacement review — round 29

Date: 2026-08-16

Scope: commits `d012e7c6e592f175fc9da208da4d82be8da1eec0` and
`4701bb8c020a1b14cf69975c701671620c6576bb`, with `4701bb8` as the reviewed
head and `d012e7c` as the parent example state for the extraction audit.

## Result

Two dependency findings remain. One declared dependency combination cannot
load Arrow. The other uses a transitive package through a private binding.
The adapter split, precompile state, fixture replay, 2.x deletion, Batteries
aliases, stress-child project handoff, ArrowTypes preservation, and required
validation are otherwise clean.

## Findings

1. **HIGH — the declared CodecZstd 0.7 compatibility cannot load the
   package.** `Project.toml:32` permits `CodecZstd = "0.7, 0.8"`, but package
   loading immediately evaluates `const ZSTD = CZSTD.LibZstd`
   (`src/Arrow.jl:51-54`). `CodecZstd` 0.7.0 has no `LibZstd` binding.

   I resolved a clean temporary environment with Arrow 3.0, the required
   development Tables checkout, CodecLz4 0.4.0, CodecZstd 0.7.0, and
   TranscodingStreams 0.9.13. Arrow precompilation failed at
   `src/Arrow.jl:53`:

   ```text
   UndefVarError: `LibZstd` not defined in `CodecZstd`
   ```

   The normal test environment resolves CodecZstd 0.8.7, so the keep-green
   suite cannot expose this supported-resolution failure. A second clean
   environment with CodecLz4 0.4.0, CodecZstd 0.8.0, and EnumX 1.0.0 loaded
   Arrow and passed both LZ4 and Zstd write/read round trips. The narrow fix
   is to change the CodecZstd compat entry to `"0.8"`.

2. **LOW — Arrow calls TranscodingStreams through a private transitive
   binding.** `src/Arrow.jl:54` binds
   `TS = CodecLz4.TranscodingStreams`, and `src/ipc_write.jl:74,83,93,97`
   calls `TS.initialize` and `TS.finalize`. TranscodingStreams is not a direct
   dependency after `4701bb8`. The binding is neither exported nor public
   from CodecLz4 (`Base.ispublic(CodecLz4, :TranscodingStreams) == false`).

   This works today. CodecLz4 0.4.0 and 0.4.6 both create that binding, both
   codecs refer to the same TranscodingStreams module, and the exact-minimum
   codec probe passed. It is still not a robust package boundary. A compatible
   CodecLz4 patch may change its private import form without changing its
   public API, which would make Arrow fail during load. Restore
   TranscodingStreams as a direct dependency with compat, import it directly,
   and bind or call that direct module instead.

## Split fidelity

A rename-aware comparison of the four product files against the `d012e7c`
examples found only 13 inserted lines. They are the six documented path-comment
updates and the C-data callback correction at `src/cdata.jl:952-959,1895`.
Every other retained executable line is byte-for-byte parent code.

The removed standalone imports, metadata construction, and aliases are present
in `src/Arrow.jl:42-54,60-77`. The four product includes are at
`src/Arrow.jl:79-82`. The shared fixture and mutation support moved from the
IPC reader example to `test/battery_helpers.jl:17-514`. The acceptance mains
moved to the four battery files. The only non-path ArrowCore changes are the
documented `Field` and `LayoutSpec` constructor narrowings at
`src/ArrowCore.jl:482-486,527-530`. The four `_of` dispatch ladders are
unchanged.

`src/cdata.jl:1927-1932` still contains `_viewentry` and `_viewlong`, which the
C-data battery uses. Those helpers were outside the shared block moved to
`battery_helpers.jl` and are unchanged from the parent. I did not classify
their retention as a split delta.

## Precompile safety

The module-global inventory found no raw pointer, malloc result, native codec
context, Task, standalone Condition, RNG, mapping, IO handle, or dynamic
library handle. The mutable globals are empty Julia registries, one unlocked
`ReentrantLock`, one integer `Ref`, and atomic test counters
(`src/cdata.jl:319-321,938-939,1291`). They hold no process-local resource at
cache creation.

Every remaining `@cfunction` is evaluated inside a runtime function. This
includes export callbacks at `src/cdata.jl:702-703`, the corrected accessors at
`956-959`, result callbacks at `1382,1421`, and stream callbacks at
`1502-1508`. All mallocs are inside export/import constructors or functions.
Decoder contexts are created per `DecodeState` and closed
(`src/ipc_read.jl:501-535`); encoder objects are created per `EncodeState` and
closed (`src/ipc_write.jl:65-99`).

A fresh-cache precompile followed by a second process successfully used the
registry lock, invoked both runtime callback accessors, and ran a real callback
that nulled its release field. The package also loaded from source with
`--compiled-modules=no --warn-overwrite=yes` without an overwrite warning.
No sibling of the two removed pointer constants remains.

## Fixture integrity and replay

The expanded call-site set is exactly the 37 filenames under
`test/fixtures2x/`. All 37 files decoded to their recorded schema, batch count,
and values. The audit also checked compression metadata, dictionary sharing,
union mode, signed zero and NaN values, wide and large sequences, and the exact
`hash.(1:4096)` byte fixture. The fixture directory is byte-identical across
the move: both tree objects are
`7a91563c3a5d33a8ecfef36d22abc70adfda4a4f`.

The plain dictionary expectations are correct. `expected.dict` remains plain
at `test/ipc_read_battery.jl:45-53`; only the provenance closure wraps it with
2.x `DictEncode` at `55-60`; and the decoded plain values are asserted at
`81-99`. The write and scan batteries use the same pattern at
`test/ipc_write_battery.jl:60-74` and `test/scan_battery.jl:146-160`.
`nullvalues` is correctly hoisted outside its closure and compared with the
reordered decoded stream at `test/ipc_read_battery.jl:550-569`.

Only the exact record branch calls `write2x()`
(`test/battery_helpers.jl:26-36`). A sentinel-closure probe returned the frozen
bytes in replay mode without running the closure. An AST ancestry sweep found
no `Arrow.write`, `Arrow.DictEncode`, or `Arrow.Decimal` outside `_fixture2x`
provenance closures. Arrow 3.0 lacks the 2.x constructor and facade methods,
so record mode cannot succeed without a 2.x checkout; the complete replay
suite proves those bodies are not reached.

## Deletion and ArrowTypes

No live old concrete type, old dependency, or deleted 2.x API reference remains
in `src/`, `test/`, `conformance/`, or `tools/` outside provenance closures.
All executable include targets resolve to present files. Old paths that remain
in prose do not participate in loading.

The `src/ArrowTypes` tree is unchanged in both scoped commits and has tree
object `f2690de3eb209263e3a8fe363f38801ba865cb87` in every relevant revision.
The local General registry entry still names UUID
`31f734f8-188a-4ce0-8406-c8a06bd891cd` and
`subdir = "src/ArrowTypes"`; that matches the present subpackage and its
`Project.toml`.

## Batteries aliases and child process

There are 329 effective aliases after the generated-name and module-name
filters. The only Base export collision is `Meta`, which deliberately becomes
`Arrow.Meta`; every battery `Meta.` use is a generated Arrow metadata binding.
There is no Test, Tables-export, or PooledArrays collision. `Schema` collides
only with Tables' private namespace and deliberately becomes ArrowCore's
`Schema`; battery calls to the Tables API remain qualified. The Test macros
remain intact.

Under `Pkg.test`, `Base.active_project()` was the temporary combined test
project containing Arrow and all test dependencies. The exact child command at
`test/cdata_battery.jl:1271-1272` inherited that project and passed with four
threads. The normal full test also passed the child stress.

## Project and compatibility audit

Mmap is used by ArrowCore, Base64 by scan statistics, EnumX by the generated
metadata layer, Tables by IPC and scan, and both codecs by the adapters.
`test/Project.toml` contains every package imported directly by the batteries:
Test, Tables, PooledArrays, and Base64. Julia 1.12 is consistent with the
source. I treated the development Tables checkout and its unreleased `Scan`
surface as an explicit constraint, not a finding. The CodecZstd floor and
private TranscodingStreams access are the two exceptions described above.

## Assumptions and decisions

- I used `d012e7c` as the direct parent example state for split fidelity.
- I treated comments and docstrings as prose, not live deleted-name references.
- I used the current local General registry entry for the ArrowTypes subdir
  check.
- I kept the development Tables dependency and constrained GC-reachability
  model unchanged, as required.
- I classified the private TranscodingStreams binding as a finding even though
  all dependency versions tested today pass.
- This was a review-only task. I added this report and made no product fix or
  commit. I did not touch the five pre-existing untracked files.

## Validation

- `julia --project=. -e 'using Pkg; Pkg.test()'` — exit 0; 325/325 core,
  4/4 threaded, and all four acceptance batteries, including the four-thread
  C-data child.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` — exit
  0; 275 pass / 0 fail / 36 skip.
- `julia --startup-file=no tools/fbsgen.jl src/metadata/fbs src/metadata`
  followed by `git diff --exit-code src/metadata` — exit 0; no diff.
- Docker daemon/local-image check — exit 0.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` — exit
  0; 170 pass / 0 fail / 43 skip.
- Exact CodecZstd 0.7.0 compatibility probe — exit 0 as an expected-failure
  probe; Arrow load reproduced the `LibZstd` error.
- Exact minimum CodecZstd 0.8.0 / CodecLz4 0.4.0 / EnumX 1.0.0 probe — exit 0;
  LZ4 and Zstd round trips passed.
- Fresh-cache precompile/reload callback probe, alias intersection probe,
  37-fixture exact-content audit, replay sentinel, include graph, deleted-name
  AST sweep, and ArrowTypes registry/tree checks — all passed.

VERDICT: FINDINGS
