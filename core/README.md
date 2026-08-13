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
is changed. `ArrowCore.jl` depends on Base and the Mmap standard library; the
IPC example uses the repository project to write fixtures and reuse its
generated metadata bindings.

This is more than a sketch and less than a package. It contains enough code,
tests, and adversarial fixtures to test the architecture. The exact limits are
listed under Honest status.

## Files

| File | Purpose |
|---|---|
| `ArrowCore.jl` | Reachability-rooted ownership regions, runtime descriptors, `Field`/`Schema`, `ArrayData`, the layout registry, staged validation, accessors, minimal builders, `RecordBatch`, and `RecordBatchSource` |
| `test/runtests.jl` | Core layout, validation, cache, bounds, region, mmap, and concurrency tests; it also starts a four-thread stress subprocess |
| `examples/ipc_read.jl` | Checked IPC stream framing, a bounded metadata verifier, metadata-to-Core mapping, dictionary state, and one registry-driven decoder over real 2.x-written streams |
| `examples/ipc_write.jl` | The write half over the same registry: Core-to-metadata mapping, one generic registry-driven encoder, replacement-on-change dictionary batches, per-buffer compression, and the file format (Block index + Footer) with a lazy random-access `ArrowFile` reader |
| `examples/cdata.jl` | C ABI definitions, zero-copy export and import, shared-tree ownership, C move semantics, and exactly-once release tests |
| `REVIEW-codex-r1.md` through `REVIEW-codex-r12.md` | Adversarial review findings and the disposition of each item |

## Run it

```bash
julia --startup-file=no core/test/runtests.jl
julia --project=. --startup-file=no core/examples/ipc_read.jl   # needs the repo project (uses 2.x to write test bytes)
julia --project=. --startup-file=no core/examples/ipc_write.jl  # needs the repo project (2.x reads this writer's bytes back)
julia --startup-file=no core/examples/cdata.jl
julia --startup-file=no core/test/trim_compile_tests.jl         # JuliaC --trim=safe gate (installs JuliaC on first run)
```

## What each report claim looks like in code

| Report claim (§) | Where proven |
|---|---|
| Ownership as an object; bad owned/verified spans fail before access (§8.2) | `OwnerRegion`, checked `BufferSlice` construction, bounds-checked `loadat`, and staged-validation tests. Foreign C extents remain a trusted declaration. |
| Core memory ownership (§9 Core) | See "Memory model" below. Regions use GC reachability as their sole validity contract. |
| Logical parameters are values (§8.1) | `TimestampType(unit, timezone)`, `DecimalType(precision, scale, bitwidth)`, and the other descriptors keep schema data out of Julia type parameters. |
| One structural registry plus bounded per-layout methods (§8.4) | `layoutspec` defines buffer roles, child arity, offset width, and variadic status. Access and semantic rules remain grouped methods. |
| Staged validation and bounded IPC metadata work (§8.5) | Structural checks are separate from semantic and full checks, and each later public stage composes the earlier stages. Data-intrinsic semantic results are cached; Field contracts run every time. The IPC framer enforces metadata, body, message, and allocation limits; the byte verifier enforces object, depth, and copy-reserve limits; and the decode cursor enforces array and buffer limits before the related work. |
| Message body is the decode authority (§9 IPC) | Every declared batch buffer becomes a checked `subslice` of its own message body. Cursor completion and non-overlap checks reject skewed buffer tables. |
| IPC ids remain adapter state (§9) | `corefield` records ids in identity-keyed adapter tables. `DictionaryType` holds the value type and `ArrayData.dictionary` holds the value array; neither stores an IPC id. |
| C Data is a direct mapping over `ArrayData` (§9 C Data) | `to_c_data` and `from_c_data` use per-structure callbacks and controls, separate schema/array aggregate roots that keep sources reachable, transitive release, and explicit reaping. Tests cover child moves, nested moves, siblings, dictionaries, failures, and exactly-once release. |
| Function-barrier bulk access (§8.9) | `materialize` enters `_materialize_loop`; scalar `getvalue` keeps runtime dispatch explicit. |

## Memory model (constrained by design)

Buffer validity is GC reachability, nothing more. An `OwnerRegion` is an
immutable `(ptr, len, alignment, root)` record: `root` is an opaque GC
anchor (the wrapped `Vector`, the Mmap-stdlib array, or an adapter's owner
object), and holding any slice of a region keeps the backing memory alive by
construction. Loads are a bounds check plus a raw load — no lock, no guard,
no atomic, no state machine on the hot path.

This is a deliberate revision of the report's §9 Layer 0 (maintainer
decision, Aug 2026, during this prove-out). The earlier lifecycle machinery
— per-load guards, `withguard`/`forceclose!`, `InvalidatedError`,
`MemoryKind`, concrete release actions — existed to make *optional eager
release* safe, and eager release was the only feature it protected. The
guards could never protect against external file truncation (no userspace
scheme can), so cutting eager release collapses the whole apparatus.
What the constraint gives up, knowingly:

- **No eager unmap.** A mapped file's unmap happens when the last region
  becomes unreachable and the GC runs the stdlib finalizer. On platforms that
  prohibit deleting a live mapping, collection must complete before the path
  can be deleted.
- **No revocation.** Nothing can invalidate outstanding slices; there is no
  `InvalidatedError`. A C-data consumer that touches an imported tree after
  explicitly releasing it gets undefined behavior — exactly the C Data
  spec's own post-release rule, now stated instead of policed.
- **External truncation of a mapped file remains unsupported** — as it was
  under the guard design, which could not prevent it either.

Exactly-once release survives where it belongs: in the C-data adapter's
`ForeignOwner` (one `@atomic` flag, a finalizer, and an explicit `release!`)
and in the export registry, which roots exported columns until the consumer
releases them and a reap drops the root.

## Simplification shown by the prove-out

- Buffer rooting, bounds, and alignment live in `OwnerRegion` and
  `BufferSlice`, not in every array wrapper.
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
Logical parent offsets and nested slices are tested. Struct scalars always use
an ordered `Vector{Pair{String,Any}}`, so names stay in the value domain and
valid duplicate, empty, or non-Symbol-compatible names do not fail. Utf8View,
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

The IPC examples map integer, floating point, Boolean, decimal, date, time,
timestamp, duration, all three interval units (MONTH_DAY_NANO through a raw
unit-slot bridge — the vendored enum predates it, and 2.x cannot parse it),
UTF-8, binary (32- and 64-bit offsets), fixed-size binary, list, large list,
fixed-size list, struct, map, sparse and dense union, null, and dictionary
overlays — the same set Core's accessors cover. Variadic view and run-end
metadata are rejected (the Core scope boundary). Nested
dictionary encodings inside a dictionary value are also rejected. It accepts
V4 and V5 metadata on little-endian hosts, supports feature-gated full
dictionary replacement, preserves old dictionary snapshots, and rejects
delta dictionaries. It requires the current eight-byte continuation-marker
framing and does not accept the pre-0.15 four-byte legacy prefix. Compression
uses the V5 `BodyCompression` field for LZ4_FRAME and ZSTD. It accepts the
standard `COMPRESSED_BODY` schema feature. It also accepts V5 compressed
streams from Arrow.jl 2.x that omit that feature for compatibility. It rejects
`BodyCompression` under V4. Endian normalization is excluded.

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

The write half (`ipc_write.jl`) covers the same mapped subset with one
registry-driven encoder — the declared inverse of `decodefield`. It writes
V5 stream bytes (schema, dictionary batches, record batches, end-of-stream)
and the file format (leading/trailing magic, Block indexes, Footer), with
per-buffer LZ4_FRAME/ZSTD compression behind the spec's Int64 prefix and the
`-1` stored-raw fallback. Dictionary handling is replacement-on-change:
one batch per pool snapshot, a replacement batch only when a later batch's
pool identity differs, `Feature.DICTIONARY_REPLACEMENT` declared in that
case (and `COMPRESSED_BODY` when compressing). Every column is semantically
validated before its bytes are emitted. The writer is eager and sequential —
it assembles byte vectors and copies buffer contents into message bodies;
the report's parallel encode pipeline with byte-credit accounting, its
incremental `IO` sink tiers, and append-as-resume remain production work.
Arrays with a nonzero element offset are refused (materialize first), each
field gets its own dictionary id (identity-shared pools re-encode per
field), and the file format refuses pools that change identity across
batches (one dictionary batch per id). `readfile` verifies both magics, the
footer, and every Block's extents before use; `ArrowFile` decodes record
batches lazily by footer index — each `getindex` runs with a fresh
allocation budget and codec contexts over the shared, eagerly-decoded
dictionary set, so concurrent reads need no coordination. An `mmapregion`
input exercises the same path over a mapped file.

The IPC read example reads one borrowed `Vector{UInt8}` and eagerly decodes
all batches before it exposes the `RecordBatchSource` pull interface. The
caller must not mutate or resize that vector while the stream or its batches
live. The same immutable-borrow rule applies to Julia vectors wrapped
directly by Core builders or `heapregion` while their `ArrayData` or cached
validation results remain in use.
It is not the report's incremental `IO` framer. Its
byte-wise verifier is a local bridge around the repository's older generated
bindings. Production work must regenerate the bindings from the pinned
schema and use a generated verifier; the report explicitly rejects a custom
parser as the final design. `max_total_allocated_bytes` is one reader-wide,
conservative budget for metadata copies, metadata-directed Julia containers,
and exact-sized decompressed outputs across all eager dictionary and record
batches. It is not an exact measurement of every Julia runtime allocation.
Wire message bodies stay zero-copy and have separate body and buffer limits;
positively compressed buffers become owned copies. Schema and Field metadata
are copied into dictionaries, so duplicate keys and original ordering are not
lossless. `IPCStream` is a single-owner pull cursor. Overlapping `nextbatch!`
calls throw `ConcurrencyViolationError`.

The C Data example maps the same descriptor set Core's accessors cover:
Boolean, integer, floating point, null, decimal (32/64/128/256 widths in the
`d:` form), date, time, timestamp (with and without timezone), duration, all
three interval units, UTF-8 and binary (both offset widths), fixed-size
binary, list, large list, fixed-size list, struct, map, sparse and dense
union (type ids carried in the format string), and dictionary. View and REE
formats are refused (the Core scope boundary). Field
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
honor consumer moves. A callback transaction that fails before commit restores
its node to LIVE and returns at the void C boundary; a later explicit call can
resume it without repeating completed children. It does not retry forever
inside the callback. The callbacks implement transitive release and consumer
move semantics only under this prove-out execution contract: callbacks for
one exported tree are serialized and run on Julia-attached threads. They call
Julia and use a `ReentrantLock`. The production native CAS and lock-free
foreign-thread trampoline from §9 is not implemented. `reap!` performs an
explicit registry scan; there is no background reaper. Schema and array trees
have independent aggregate lifetimes and per-node control blocks.

The C stream interface (`ArrowArrayStream`) is mapped in both directions.
`export_stream!` fills a caller-owned struct that streams batches as
struct-typed arrays (children = the schema's columns); each
`get_schema`/`get_next` result is an ordinary export root with the standard
release/reap lifecycle, producer-side failures are reported through
`get_last_error` (EINVAL + a NUL-terminated message owned by the stream
until replaced or released), and the stream's own registry root drops at its
release callback. `from_c_stream` moves a producer's stream (struct copy +
source release null), reads the schema once, pulls batches whose trees each
own one ForeignOwner, and surfaces producer errors as exceptions carrying
the producer's message. Execution contract (report §9, v1, stated loudly):
stream callbacks call into Julia, so they are legal only from Julia-attached
threads, and calls on one stream must not overlap — the C stream spec itself
declares the structure not thread-safe. The marshaling worker that would
make any-thread callers legal is production work.

Other exclusions are unchanged: no parallel writer coordinator or byte-credit
pipeline, append-as-resume, facade, `ViewPlan`, typed views, ArrowTypes
integration, or builders beyond test support. `mmapregion` maps via
the Mmap STDLIB (cross-platform) and keeps the mapped array as the region's
`root`; the stdlib finalizer unmaps when that root becomes unreachable (see
"Memory model"). The mapped array is an internal anchor: resizing it through
`region.root` falls under the same immutable-borrow rule as any wrapped
vector. External writes or truncation of a mapped file while the mapping or
cached validation results remain in use are unsupported. On systems that
prohibit deleting active mapped files, collection must complete before the
path can be deleted.
The ABI layout checks include 32-bit expectations, but this review executed
them only on the available 64-bit host.

## Trim-compile support (JuliaC `--trim=safe`)

`core/test/trim_compile_tests.jl` compiles `core/test/trim_entrypoint.jl`
with JuliaC's `--trim=safe` and holds the same bar as the JSON/HTTP/Reseau/
StructUtils harnesses: **zero verifier errors, zero verifier warnings, and
the produced binary runs to exit 0** (binary ≈ 2.2 MB). The design rules
that get a runtime-tagged core there — worth carrying into the real
implementation:

- **Closed-set dispatch ladders.** Dispatch on an abstract-typed field is
  dynamic; the descriptor set is closed (it IS the layout registry), so
  `@inline` `isa` ladders (`layoutspec_of`, `_value_of`, `_materialize_of`,
  `typeequal`, `descriptorname`, `_validate_descriptor_of`) devirtualize
  every generic entry point. Multiple dispatch remains the per-layout
  extension surface underneath.
- **Literal load widths.** `loadat(b, T, off)` with a runtime `T::DataType`
  leaves the raw-load path unresolved; accessors branch to literal widths
  instead. This is also faster.
- **CAS for atomic counters.** JuliaC's verifier has not implemented
  `Core.modifyfield!` (each `@atomic x.f += 1` is a verifier warning), while
  `@atomicreplace` verifies clean, so `ReleaseCounter` uses a CAS loop. The
  constrained memory model needs no other synchronization in core at all.
- **`Ptr{Cvoid}` finalizers** (adapter guidance — core itself registers no
  finalizer since regions are plain immutable records). Base's generic
  `finalizer(f, o)` is `@nospecialize`d and unresolvable; the typed pointer
  form (`finalizer(@cfunction(...), o)`) is an ordinary ccall. The C entry
  must swallow errors so nothing unwinds into the GC's finalizer runner.
- **Concrete containers at the boundary.** Struct scalars are
  `Vector{Pair{String,Any}}` (a NamedTuple carries names in the TYPE domain
  — intrinsically dynamic from runtime schemas, and unable to represent
  Arrow's duplicate/empty names); lists materialize as `Vector{Any}` without
  the runtime-narrowing comprehension. Typed element containers and the
  NamedTuple surface are the facade's ViewPlan work (report §14.2).
- **Beware splatting Base conveniences.** `write(filename, x)` and
  `open(...) do` route through vararg-splatting internals; `mktempdir`'s
  cleanup registry parks the trimmed runtime's scheduler. The workload uses
  the primitive forms.
- Heterogeneous NamedTuple ingestion (`batch(nt)`, `fromjulia_struct`) is
  runtime-schema builder work and stays outside the trim-safe surface.

## Interruption contract

Asynchronous interruption (SIGINT / `InterruptException`, task cancellation)
is explicitly **out of contract**, matching ecosystem practice — Base itself
does not make arbitrary code async-exception-atomic, and the earlier
`disable_sigint`/retry scaffolding bought a property that cannot be fully
delivered. Ordinary exception safety (error paths clean up; adapter release
is exactly-once) **is** in contract and tested. A formal revisit is planned
when Julia 1.14's structured cancellation gives Base a real system to build
on. Relatedly, `Threads.Atomic` boxes appear nowhere in `core/`. The
`ArrowCore` module uses atomics only for the two `ArrayData` validation caches
and the `ReleaseCounter` test utility; its constrained memory model has no
region lifecycle to synchronize. The adapters add one pull-claim flag on
`IPCStream` and one exactly-once flag on `ForeignOwner`.

## Compression

The IPC examples implement spec buffer compression for **LZ4_FRAME and
ZSTD**, both directions. Each reader lazily creates raw native codec contexts
and closes them on every `readstream` exit path; each writer owns one lazily
initialized compressor object per codec and finalizes it on every writer exit
path; there are no global pools. The write side emits the Int64
uncompressed-length prefix per buffer and stores incompressible payloads raw
behind the `-1` sentinel. The adapter checks
the per-buffer Int64 uncompressed-length prefix and the `-1` stored-raw
sentinel. A zero-byte wire buffer may omit the prefix. A nonzero compressed
buffer, including declared length zero, must contain a valid frame.

Declared sizes are bounded and charged to the shared reader budget before one
exact-sized output vector is allocated. The codecs decode directly from the
wire slice (its region rooted across the native call with `GC.@preserve`),
with no payload copy and no growable output. The LZ4 loop
requires one complete frame, exact input consumption, and exact output size.
The ZSTD one-shot decode uses the same exact destination. Acceptance covers
V5 feature handling, 2.x-written record and dictionary batches, empty and raw
buffers, hostile prefixes, compressed bombs, aggregate batch budgets,
truncation, concatenated LZ4 frames, and corrupt-context cleanup. In the
production package the codecs are package extensions; the example's closed
two-codec switch is the trim-friendly shape of the same idea.
