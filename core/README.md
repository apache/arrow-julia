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

# ArrowCore prove-out

A working, tested implementation of the **runtime-tagged, C-data-shaped
core** proposed in the Arrow.jl redesign report (`Arrow-redesign-report.md`,
§9), plus two adapter prove-outs showing how the IPC and C-data layers sit
on top. Standalone: nothing in `src/` is touched; the module depends only on
Base.

This is deliberately more than a sketch and less than a package: enough real
code, tests, and adapters to judge the approach and its simplification
claims concretely.

## Files

| File | LoC | What it is |
|---|---|---|
| `ArrowCore.jl` | ~1,270 | The Core module: `OwnerRegion`/`BufferSlice` ownership + access guards, runtime `ArrowType` descriptors, `Field`/`Schema` (with endianness), `ArrayData`, the structural layout registry, staged validation, per-layout element accessors, minimal builders, `RecordBatch` + `RecordBatchSource` |
| `test/runtests.jl` | ~380 | 125 assertions: lifecycle races (guard vs forceclose, timeout-restores-open, invalidation), slice bounds, unaligned loads, registry coverage for every format-1.5 layout, value round-trips across 12 layouts, corrupt-metadata rejection at each validation stage, cache behavior |
| `examples/ipc_read.jl` | ~420 | The IPC adapter prove-out: stage-1 framing with resource limits, metadata→Core mapping, and ONE generic registry-driven decoder — reading a real multi-batch stream **written by today's Arrow.jl 2.x** (nullable ints/floats/bools/strings, lists, structs, dictionary-encoded), then proving the limits and truncation semantics |
| `examples/cdata.jl` | ~590 | The C-data adapter prove-out: spec-exact ABI structs, export (control block + export registry + reap queue + exactly-once release), import (one `ForeignOwner` per moved tree, declared extents), full round-trip + lifecycle tests |

## Run it

```bash
julia --startup-file=no core/test/runtests.jl
julia --project=. --startup-file=no core/examples/ipc_read.jl   # needs the repo project (uses 2.x to write test bytes)
julia --startup-file=no core/examples/cdata.jl
```

## What each report claim looks like in code

| Report claim (§) | Where proven |
|---|---|
| Ownership as an object; corrupt metadata → error, never segfault (§8.2) | `BufferSlice` checked construction; `loadat` last-line bounds; tests "staged validation rejects corrupt metadata" |
| Deterministic close: guards vs reachability, timeout restores open, generation invalidation (§9 Core) | `withguard`/`forceclose!`; tests "forceclose! waits for guards; timeout restores open", mmap close test (real munmap via ccall — no stdlib finalizer dependence) |
| Logical params as values, never type params (§8.1) | `TimestampType(unit, tz)` etc.; test asserts two timezones share one Julia type |
| One structural registry + per-layout methods (§8.4) | `layoutspec` (28 lines of table) + `_value` methods; dense-union `ELEMENT_OFFSETS` vs range `OFFSETS` distinction lives in the registry, not in validator special cases |
| Staged validation + resource limits before allocation (§8.5) | `validate_structural`/`validate_semantic` (cached)/`validate_full`; `Limits` + `framemessages` in the IPC example — a hostile body length is rejected before any decode allocation |
| Message body as decoding authority (§9 IPC) | every batch buffer is a checked `subslice` of its message's body slice |
| Generic node/buffer walk replaces ten `build` methods (§9 IPC) | `decodefield` + `DecodeCursor` (~45 lines); end-of-batch leftover-nodes/buffers check turns accounting bugs into errors instead of the #540 corruption class |
| IPC ids are adapter bookkeeping, not Core state (§9) | `corefield` records ids in the adapter's side table; Core `Field` never sees one |
| C-data is struct filling over ArrayData (§9 C-data) | `to_c_data`/`from_c_data`; one release per moved tree; control block + export registry + reap queue; exactly-once + failure-path + post-release invalidation all demonstrated |
| Declared (unverifiable) foreign extents (§8.5) | `_import_array` computes required sizes from the registry; comment marks the trust boundary |
| Boundary truncation tolerated, mid-body truncation is an error (§9 append rules) | the IPC example's final two checks |
| Function-barrier bulk access (§8.9) | `materialize` → `_materialize_loop` barrier; scalar `getvalue` documents its per-call dispatch cost honestly |

## The simplification ledger (measured, current tree)

| Concern | 2.x today | This prove-out |
|---|---|---|
| Read-path decode | 10 `build` methods hand-threading `(nodeidx, bufferidx, varbufferidx)`, ~420 lines (src/table.jl:754-1174), duplicated again in `Stream` | 1 generic `decodefield` + cursor, ~45 lines, shared by record and dictionary batches |
| Type mapping | 22 `juliaeltype` + 21 `arrowtype` methods entangled with value conversion (src/eltypes.jl, 578 lines) | `coretype` — one value-level function (~55 lines); Julia value conversion stays out of Core entirely |
| Buffer bookkeeping | every wrapper type carries a `bytes` GC-root field by convention; `unsafe_wrap` + manual alignment copy | `OwnerRegion`/`BufferSlice`: rooting, bounds, and alignment handled once |
| Untrusted input | length prefix → `Vector{UInt8}(undef, n)` (src/table.jl:804-816); truncation → silent empty stream | limits before allocation; truncation → `ValidationError` |
| C-data interface | five stalled attempts against the 2.x internals | ~590-line worked example incl. lifecycle tests |
| New layout cost | new arraytype file + new `build` method + counter threading through all others + eltypes methods + serialize triplet | registry row + one accessor method group (`ELEMENT_OFFSETS` for dense unions was added mid-prove-out in exactly this shape) |

Total prove-out: ~2,660 lines including tests and both adapters — against a
2.x read path + type mapping alone of ~1,700 lines that covers no C-data, no
staged validation, and no deterministic close.

## Honest status

Implemented and tested here: primitives (all widths), bool, decimal
32/64/128/256 (raw bytes for ≥128), date/time/timestamp/duration, interval
(including MONTH_DAY_NANO, which 2.x cannot parse), utf8/binary (+large),
fixed-size binary, list (+large), fixed-size list, struct, map, sparse +
dense unions, dictionary-encoded (non-delta), null; logical `offset`
(sliced) data; lifecycle; staged validation; IPC stream read; C-data
export/import.

Registry + structural validation only (accessors intentionally error, per
report roadmap slices 2f/2h): Utf8View/BinaryView, ListView, run-end
encoding. Not attempted here (roadmap): IPC file footer/index, delta
dictionaries and the writer's dictionary coordinator, compression,
endianness normalization, builders beyond the test-support minimum, the
facade (`ViewPlan`, typed views, Tables.jl, ArrowTypes integration), and the
C stream interface.

Known prove-out shortcuts a production version replaces: `materialize`
returns runtime-narrowed vectors (the facade's typed views make this
precise); the C-data reap queue is drained explicitly instead of by a
background reaper task; `mmapregion` is POSIX-only; the export registry
locks a plain Dict (fine at adapter-call frequency).
