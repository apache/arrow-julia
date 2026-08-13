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
| `REVIEW-codex-r1.md` through `REVIEW-codex-r11.md` | Adversarial review findings and the disposition of each item |

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
| Staged validation and bounded IPC metadata work (§8.5) | Structural checks are separate from semantic and full checks, and each later public stage composes the earlier stages. Data-intrinsic semantic results are cached; Field contracts run every time. The IPC framer enforces metadata, body, message, and allocation limits; the byte verifier enforces object, depth, and copy-reserve limits; and the decode cursor enforces array and buffer limits before the related work. |
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

## Interruption safety

This prove-out makes its committed ownership handoffs interruption-atomic. An
acquired access guard transfers to `withguard` cleanup or rolls back. A close
claim is restored until release-callback entry; callback entry commits an
at-most-once generic release. Successful mmap acquisition, C export allocation
and pin registration, C import moves, C export release commits, and IPC cursor
claims and advances are recorded in owner, registry, or caller-owned rollback
state before interruptible work resumes. Failure at one of these handoffs
either restores the prior state or leaves the resource under a committed
cleanup owner. A successful public return commits returned C pointers or an
IPC batch to the caller.

This is not instruction-level async-exception atomicity. Julia can deliver
`InterruptException` and task cancellation at safepoints, and any allocation
can throw. Julia has no operation that atomically combines a native effect such
as `mmap`, `munmap`, `malloc`, `free`, or a foreign callback with publication of
Julia state. The code defers SIGINT only across bounded handoffs. It re-enables
SIGINT during waits and user work.

Cleanup outside those committed handoffs is best effort. `OwnerRegion` and
imported-owner finalizers backstop resources that have a Julia owner. Mmap and C
producer cleanup retry interruption only when an explicit state marker or a
`release == NULL` marker makes retry safe. A generic release callback runs at
most once after entry because it may have partly freed its resource before it
fails. Successful C exports have no Julia finalizer. They remain registry-rooted
until the consumer calls their release callbacks and `reap!` performs cleanup.
Abrupt process termination, arbitrary instruction-level exception injection,
and a foreign callback that does not return or fails after partial cleanup are
outside this guarantee.

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
entries and structural validation but no semantic validation or accessors.
`validate_semantic` and `validate_full` reject those layouts instead of
certifying unchecked content. This is a declared scope boundary.
`validate_full` adds UTF-8 well-formedness only for supported layouts;
canonical padding and unused-bit checks remain production work.
Map validation checks physical layout and reachable Field nullability. It does
not check key uniqueness, hashability, or ordering; `keysSorted` remains a
producer declaration.
Core `RecordBatch` buffers must use host-native endianness. An adapter must
normalize non-native input before it constructs a batch.
Timestamp validation checks the Arrow unit domain and timezone-string UTF-8.
It does not resolve names against a timezone database.

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

Compatible fields that share one IPC dictionary id also share one immutable
pool object. Eager stream decoding fully validates each immutable pool
snapshot once, then reuses that identity certificate for structural,
intrinsic, and Field-contract validation. It still checks each field's index
array independently. This keeps validation work linear in the encoded indices
plus distinct pool data.

The IPC adapter runs structural and semantic Core validation before it exposes
a batch. It does not opt into `validate_full`, so UTF-8 body content is not
checked. The byte-wise metadata verifier does validate FlatBuffer strings.
The framer rejects a non-little-endian host before it calls the older generated
FlatBuffers getters, which use native-endian scalar loads.

The IPC example reads one borrowed `Vector{UInt8}` and eagerly decodes all
batches before it exposes the `RecordBatchSource` pull interface. The caller
must not mutate or resize that vector while the stream or its batches live.
The same immutable-borrow rule applies to Julia vectors wrapped directly by
Core builders or `heapregion` while their `ArrayData` or cached validation
results remain in use.
It is not the report's incremental `IO` framer or file-footer reader. Its
byte-wise verifier is a local bridge around the repository's older generated
bindings. Production work must regenerate the bindings from the pinned
schema and use a generated verifier; the report explicitly rejects a custom
parser as the final design. `max_total_allocated_bytes` is a conservative
budget for metadata copies and metadata-directed Julia containers. It is not
an exact measurement of every Julia runtime allocation. Message bodies stay
zero-copy and have separate body and buffer limits. Schema and Field metadata
are copied into dictionaries, so duplicate keys and original ordering are not
lossless. `IPCStream` is a single-owner pull cursor. Overlapping `nextbatch!`
calls throw `ConcurrencyViolationError`.

The C Data example maps Boolean, integer, floating point, UTF-8, binary, list,
struct, map, and dictionary formats. Other Core layouts are not mapped. Field
metadata is omitted on export and ignored on import; dictionary value-schema
names, nullability, and metadata are not a lossless round trip. Foreign
allocation extents cannot be verified by the ABI and remain trusted
declarations. The producer must keep declared storage alive and unchanged
until Core releases it. Import checks the pointer tables, counts, descriptor
shape, and checked geometry that the ABI does expose. Import and export run
full UTF-8 validation. Field names that contain an embedded NUL are rejected
because the C interface uses NUL-terminated strings.

The C release callbacks use producer-owned canonical child and dictionary
topology, so cleanup does not depend on caller-mutated public counts or pointer
tables. They still inspect canonical descendants' public release fields to
honor consumer moves. The callbacks implement transitive release and consumer
move semantics only under this prove-out execution contract: callbacks for
one exported tree are serialized and run on Julia-attached threads. They call
Julia and use a `ReentrantLock`. The production native CAS and lock-free
foreign-thread trampoline from §9 is not implemented. `reap!` performs an
explicit registry scan; there is no background reaper. Schema and array trees
have independent aggregate lifetimes and per-node control blocks.

Other exclusions are unchanged: no IPC file footer/index, compression,
writer coordinator, facade, `ViewPlan`, typed views, ArrowTypes integration,
C stream interface, or builders beyond test support. `mmapregion` is
POSIX-only. External writes or truncation of a mapped file while the mapping
or cached validation results remain in use are unsupported.
The ABI layout checks include 32-bit expectations, but this review executed
them only on the available 64-bit host.
