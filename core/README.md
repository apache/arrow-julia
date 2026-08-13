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

A working implementation of the runtime-tagged, C-data-shaped core proposed
in the Arrow.jl redesign report (`Arrow-redesign-report.md`, §9). Two examples
show how IPC and C Data adapters sit above that core. Nothing outside `core/`
is changed. `ArrowCore.jl` depends only on Base; the IPC example uses the
repository project to write fixtures and reuse its generated metadata bindings.

This is more than a sketch and less than a package. It contains enough code,
tests, and adversarial fixtures to test the architecture. The exact limits are
listed under Honest status.

## Files

| File | Purpose |
|---|---|
| `ArrowCore.jl` | Ownership and access guards, runtime descriptors, `Field`/`Schema`, `ArrayData`, the layout registry, staged validation, accessors, minimal builders, `RecordBatch`, and `RecordBatchSource` |
| `test/runtests.jl` | Core layout, validation, cache, bounds, lifecycle, mmap, and concurrency tests; it also starts a four-thread stress subprocess |
| `examples/ipc_read.jl` | Checked IPC stream framing, a bounded metadata verifier, metadata-to-Core mapping, dictionary state, and one registry-driven decoder over real 2.x-written streams |
| `examples/cdata.jl` | C ABI definitions, zero-copy export and import, shared-tree ownership, C move semantics, exactly-once release, and lifecycle tests |
| `REVIEW-codex-r1.md` | Round-1 findings and the disposition of each item |

## Run it

```bash
julia --startup-file=no core/test/runtests.jl
julia --project=. --startup-file=no core/examples/ipc_read.jl   # needs the repo project (uses 2.x to write test bytes)
julia --startup-file=no core/examples/cdata.jl
```

## What each report claim looks like in code

| Report claim (§) | Where proven |
|---|---|
| Ownership as an object; bad owned/verified spans fail before access (§8.2) | `OwnerRegion`, checked `BufferSlice` construction, guarded `loadat`, and staged-validation tests. Foreign C extents remain a trusted declaration. |
| Deterministic close (§9 Core) | `withguard` and `forceclose!` use one lifecycle word. A sole closer blocks new guards, waits for active guards, restores open state on timeout, and publishes a new closed generation after release. Finalization uses the same protocol. |
| Logical parameters are values (§8.1) | `TimestampType(unit, timezone)`, `DecimalType(precision, scale, bitwidth)`, and the other descriptors keep schema data out of Julia type parameters. |
| One structural registry plus bounded per-layout methods (§8.4) | `layoutspec` defines buffer roles, child arity, offset width, and variadic status. Access and semantic rules remain grouped methods. |
| Staged validation and bounded IPC metadata work (§8.5) | Structural checks are separate from semantic and full checks. Data-intrinsic semantic results are cached; Field contracts run every time. The IPC verifier applies object, depth, byte, message, buffer, and array limits before metadata-directed decode work. |
| Message body is the decode authority (§9 IPC) | Every declared batch buffer becomes a checked `subslice` of its own message body. Cursor completion and non-overlap checks reject skewed buffer tables. |
| IPC ids remain adapter state (§9) | `corefield` records ids in identity-keyed adapter tables. `DictionaryType` holds the value type and `ArrayData.dictionary` holds the value array; neither stores an IPC id. |
| C Data is a direct mapping over `ArrayData` (§9 C Data) | `to_c_data` and `from_c_data` use per-structure callbacks and controls, separate schema/array aggregate roots, source-region pins, transitive release, and explicit reaping. Tests cover child moves, nested moves, siblings, dictionaries, failures, and post-release access. |
| Function-barrier bulk access (§8.9) | `materialize` enters `_materialize_loop`; scalar `getvalue` keeps runtime dispatch explicit. |

## Simplification shown by the prove-out

- Buffer rooting, bounds, alignment, and deterministic invalidation live in
  `OwnerRegion` and `BufferSlice`, not in every array wrapper.
- One cursor and recursive decoder account for nodes and fixed buffers for the
  mapped IPC subset. Record and dictionary batches use the same path.
- Runtime type mapping is separate from Julia value conversion.
- C Data export fills ABI structures from the same buffer and child tree that
  Core accessors use.
- Adding a layout requires one registry entry and a bounded set of semantic,
  adapter, and accessor methods. The registry does not claim to remove those
  layout-specific rules.

## Honest status

Core accessors and validation cover integer, floating point, Boolean,
decimal, date, time, timestamp, duration, all interval variants, UTF-8 and
binary with 32-bit or 64-bit offsets, fixed-size binary, list, fixed-size
list, struct, map, sparse and dense union, dictionary, and null arrays.
Logical parent offsets and nested slices are tested. Struct scalars use a
`NamedTuple` only when names are unique, nonempty, and valid Julia Symbol
names; otherwise they use an ordered vector of `Pair{String,Any}` so valid
duplicate, omitted, or non-Symbol-compatible names do not fail. Utf8View,
BinaryView, ListView, and run-end encoding have registry
entries and structural validation but no accessors. This is a declared scope
boundary. `validate_full` adds UTF-8 well-formedness only; canonical padding
and unused-bit checks remain production work.

The IPC example has a narrower mapping. It reads streams containing integer,
floating point, Boolean, decimal, date, time, timestamp, duration, UTF-8,
binary, fixed-size binary, list, fixed-size list, struct, map, null, and
dictionary overlays. It rejects interval, union, variadic view, and run-end
metadata because the reused bindings and adapter do not map them. Nested
dictionary encodings inside a dictionary value are also rejected. It accepts
V4 and V5 metadata on little-endian hosts, supports feature-gated full
dictionary replacement, preserves old dictionary snapshots, and rejects
delta dictionaries. It requires the current eight-byte continuation-marker
framing and does not accept the pre-0.15 four-byte legacy prefix. Compression
and endian normalization are excluded.

The IPC example reads one borrowed `Vector{UInt8}` and eagerly decodes all
batches before it exposes the `RecordBatchSource` pull interface. The caller
must not mutate or resize that vector while the stream or its batches live.
It is not the report's incremental `IO` framer or file-footer reader. Its
byte-wise verifier is a local bridge around the repository's older generated
bindings. Production work must regenerate the bindings from the pinned
schema and use a generated verifier; the report explicitly rejects a custom
parser as the final design. `max_total_allocated_bytes` is a conservative
budget for metadata copies and metadata-directed Julia containers. It is not
an exact measurement of every Julia runtime allocation. Message bodies stay
zero-copy and have separate body and buffer limits. Schema and Field metadata
are copied into dictionaries, so duplicate keys and original ordering are not
lossless.

The C Data example maps Boolean, integer, floating point, UTF-8, binary, list,
struct, map, and dictionary formats. Other Core layouts are not mapped. Field
metadata is omitted on export and ignored on import; dictionary value-schema
names, nullability, and metadata are not a lossless round trip. Foreign
allocation extents cannot be verified by the ABI and remain trusted
declarations. Import checks the pointer tables, counts, descriptor shape, and
checked geometry that the ABI does expose.

The C release callbacks implement transitive release and consumer move
semantics only under this prove-out execution contract: callbacks for
one exported tree are serialized and run on Julia-attached threads. They call
Julia and use a `ReentrantLock`. The production native CAS and lock-free
foreign-thread trampoline from §9 is not implemented. `reap!` performs an
explicit registry scan; there is no background reaper. Schema and array trees
have independent aggregate lifetimes and per-node control blocks.

Other exclusions are unchanged: no IPC file footer/index, compression,
writer coordinator, facade, `ViewPlan`, typed views, ArrowTypes integration,
C stream interface, or builders beyond test support. `mmapregion` is
POSIX-only. Concurrent external truncation of a mapped file is unsupported.
The ABI layout checks include 32-bit expectations, but this review executed
them only on the available 64-bit host.
