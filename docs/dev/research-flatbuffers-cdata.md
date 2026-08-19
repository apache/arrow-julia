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

# Research: vendored FlatBuffers runtime + recent C-data PRs

Date: 2026-08-16. Branch: `core-rewrite`. Scope: (1) assess `src/FlatBuffers/`
and an upstreaming path; (2) survey the C-data PRs on apache/arrow-julia and
compare against `src/cdata.jl`. Research only — no code changed.

---

## 1. Vendored FlatBuffers runtime

### 1.1 What we have

Three files, 682 lines: `src/FlatBuffers/FlatBuffers.jl` (58),
`builder.jl` (448), `table.jl` (176). Provenance: introduced whole in the
2020 donation commit (`50e015f` "Pure Julia implementation of apache arrow
format") as a fresh, trimmed port of the Go runtime — it was **not** forked
from JuliaData/FlatBuffers.jl's code; the two share only the Go-port
ancestry. Around it the rewrite adds `src/metadata/VerifierRuntime.jl`
(261 lines, hand-maintained, schema-blind) and `tools/fbsgen.jl` (686 lines),
which regenerates `src/metadata/{Schema,File,Message,Verifier}.jl`.

The rewrite's usage surface is narrow. Read side: `getrootas`, `init`,
`offset`, `get`, `indirect`, `String`, `Array`, `union`, `vector`/`vectorlen`
(via generated getters). Write side: `Builder`, `startobject!`/`endobject!`,
`prependslot!`/`prependoffsetslot!`/`prependstructslot!`,
`startvector!`/`endvector!`, `createstring!`, `finish!`, `prep!`, `pad!`,
`place!`. Zero call sites outside the runtime for: `getslot`,
`getoffsetslot`, `bytevector`, `createsharedstring!`,
`finishwithfileidentifier`, `reset!`, `union!`, `getvalue`.

### 1.2 Correctness hazards (standalone)

Context first: in the rewrite **no generated getter runs on unverified
bytes**. `verify_ipc_metadata` stages `verifyrootstart_Message` /
`verifyrootrest_Message` before `FB.getrootas` (`src/ipc_read.jl:164-183`,
comment at `:256-258`), and `verify_footer` does the same for the file footer
(`src/ipc_write.jl:834-848`). The verifier runtime does checked, byte-assembled
loads with range/alignment/domain/budget proofs
(`src/metadata/VerifierRuntime.jl:112-234`). The list below is therefore what
the runtime lacks **on its own** — relevant only if it were ever reused
without the generated verifier in front:

- **Unchecked scalar loads.** `readbuffer` is a raw `unsafe_load` at
  `pointer(t, pos+1)` with no bounds check (`FlatBuffers.jl:34-39`); the
  `Bool` overload uses `@inbounds` indexing (`:29-32`) and tests `b === 0x01`,
  so a byte of `0x02` silently reads as `false` (the verifier's `_vbool`
  restricts the domain to `{0x00,0x01}` first).
- **Wire-controlled string length.** `Base.String(t::Table, off)` calls
  `unsafe_string(pointer(...), len)` with `len` read from the buffer
  (`table.jl:68-73`) — an arbitrary out-of-bounds read standalone.
- **Wire-controlled vector wrap.** `Array{T}` does
  `unsafe_wrap(Base.Array, ptr, vectorlen(t, off))` with no check that
  `len * sizeof(S)` fits the buffer, and no alignment check for `S`
  (`table.jl:109-115`). The verifier proves both (bounds and
  `start % min(elemsize, 8) == 0`, `VerifierRuntime.jl:228-230`) before any
  getter constructs the wrap. GC-rooting is correct: the wrap aliases
  `bytes(t)` and the `Array` struct keeps `_tab` (hence the bytes) reachable.
- **Unchecked offset math.** `offset()` subtracts a wire `SOffsetT` from
  `pos` with no range check (`table.jl:55-59`); `indirect` adds a wire
  `UOffsetT` (`:62`); `getrootas` trusts the root offset (`:41-42`). On
  64-bit hosts the promotions land in `Int64` so there is no silent wraparound,
  only OOB positions; on 32-bit hosts (`pos::Base.Int` = `Int32` in the
  generated tables) a hostile `UOffsetT ≥ 2^31` can overflow. The verifier
  runtime deliberately computes everything in explicit `Int64` with
  `checked_add`/`checked_sub`/`checked_mul` and subtraction-form range checks.
- **Builder-side widths.** `Builder.head::UOffsetT` (UInt32) caps builders at
  4 GiB; `createstring!`'s `b.head -= l` (`builder.jl:315`) would wrap if
  `place!`/copy ever ran without a preceding `prep!` (correct usage prevents
  it; nothing enforces it).
- **Host-endian reads.** The write path is explicitly little-endian
  (byte-shift loop, `builder.jl:72-77`) but reads are native-endian
  (`unsafe_load`; `read(IOBuffer, ...)` in `vtableEqual`). The runtime is
  correct only on LE hosts — which is every supported Julia platform, but
  worth stating.
- **Latent bugs in dead code.** `reset!` calls undefined `emtpy!`
  (`builder.jl:62`) and would error on first use; `union!` mutates
  `t2.pos`/`t2.bytes` (`table.jl:144-149`) but fbsgen-generated tables are
  immutable structs, so it would throw; `bytevector`'s view end is
  `start + len + 1` — one byte long (`table.jl:79`). All three are unused in
  the rewrite; delete or fix when next touching the runtime.

### 1.3 Performance issues

- **`vtableEqual` allocates per slot.** Each comparison reads a `VOffsetT`
  via `read(IOBuffer(view(...)), VOffsetT)` (`builder.jl:429-431`), inside a
  loop over candidate vtables (`:141-157`) — an allocation per slot per
  candidate, on every `endobject!`. This is the clearest write-path win:
  replace with `readbuffer`.
- **Buffer growth is exact-fit.** `prep!` grows by prepending exactly
  `totalsize` zero bytes via a temporary `zeros(UInt8, totalsize)`
  (`builder.jl:238-244`) — no exponential reserve; it leans entirely on
  `Vector`'s amortized beginning-growth and pays one temp allocation per
  growth.
- **Byte-at-a-time writes.** `pad!` runs a closure per zero byte
  (`builder.jl:219`); `Base.write(::Builder, off, x)` is a shift-and-store
  loop with a bounds-checked `setindex!` per byte (`:71-77`) despite
  `place!`'s "without checking for space" contract.
- **Per-access wrap allocation.** Every generated vector getter constructs a
  fresh `FlatBuffers.Array` → one `unsafe_wrap` array header per property
  access (`table.jl:109-115`). Frequency is per-batch (nodes/buffers/fields),
  not per-value, so it is visible but not dominant.
- **Always-allocated shared-string Dict.** Every `Builder()` allocates a
  `Dict{String,UOffsetT}` (`builder.jl:42,56`) that Arrow never uses.
- Non-issues worth recording: generated tables are concrete immutable structs
  (no abstract fields); `Builder` fields are concrete; getter returns are
  `Union{Nothing,T}` **by design** (absent optional field) and the adapters
  narrow immediately after verification.

### 1.4 Spec-feature gaps

Missing vs the FlatBuffers spec: any runtime verifier (Arrow supplies its
own generated one), size-prefixed roots (`finishsizeprefixed!` /
size-prefixed `getrootas`), read-side file-identifier check (write-side
`finishwithfileidentifier` exists, unused), public alignment forcing beyond
internal `prep!`, `key`/sorted-vector lookup, nested-flatbuffer helpers, any
object/reflection API. Present but unused by Arrow: shared strings
(`createsharedstring!`, `builder.jl:297-301`), vtable deduplication (used).
None of the gaps matter for Arrow's three schemas.

### 1.5 Upstream JuliaData/FlatBuffers.jl today

- Latest release **v0.6.2, 2025-03-24**; 47 stars; 21 open issues; 2 open PRs
  (a Jan-2026 dependabot bump and vtjnash's one-line codecov fix **#72, open
  since 2025-03-24**). The last non-bot commits are Mar 2025 drive-bys
  (mkitti docs, KristofferC version bump, vtjnash `eval`-globals hygiene).
  README advertises testing "against Julia 1.0, latest 1.X". Effectively
  dormant.
- Architecturally it is the **2016-era reflection design**, not our runtime:
  user-defined Julia types mapped via `@STRUCT`/`@DEFAULT`/`@ALIGN`/
  `@UNION`/`@with_kw` macros, `slot_offsets(T)`, `default(T)`,
  `deserialize(io, T)`; internals (`src/internals.jl`) still read **every
  scalar through `read(IOBuffer(view(...)), T)`** — an allocation per load.
  Our vendored copy replaced exactly this model in 2020. Divergence is total;
  there is no code to merge in either direction, only a wholesale
  replacement.
- Would upstream accept a modernization? It is a JuliaData package and the
  original author is this project's maintainer, so acceptance is not the
  obstacle. The obstacle is that "upstreaming" means shipping a breaking
  0.7/1.0 that abandons the reflection API its remaining dependents pin, plus
  owning a general-purpose IDL surface indefinitely.

### 1.6 Could fbsgen.jl generalize into a flatc-for-Julia?

What it already does (`tools/fbsgen.jl:33-40`): `table`/`struct`/`enum`
(explicit values)/`union`, scalars, vectors, string/table refs, scalar and
enum defaults, `(deprecated)`, comment stripping, cross-file type references
by leaf name (`:126-130`), Arrow's Base-name collisions via `RENAMES`
(`:169-170`), a hand-injected `REQUIRED` set (`:224-231`, because Arrow's
.fbs declares no `(required)`), and — the distinctive part — a **generated
shape verifier** per table with a root start/rest split for constant-time
policy gating (`:484-619`) over the schema-blind runtime.

To be a general tool it would additionally need:

- **Attributes**: `id` (field reordering — `slotmap` assumes declaration
  order, `:185-197`; the attrs capture group is parsed but only
  `deprecated` is consulted, `:124-132`), `required`, `key`, `force_align`,
  `bit_flags`, `nested_flatbuffer`.
- **Namespaces as modules** (currently stripped, `:97-98`; cross-namespace
  name collisions would break) and real `include` resolution (`generate()`
  hardcodes `("Schema", "File", "Message")`, `:646`).
- **More IDL**: optional scalars (`= null`), fixed-size struct arrays
  (`[T:N]`), struct-in-struct (`structsize` assumes scalar fields, `:199-209`),
  unions of strings/structs, vectors of unions, `file_identifier`/
  `file_extension` surfacing, `rpc_service` (skippable), a real tokenizer in
  place of the regex parser.
- **Runtime additions**: size-prefixed roots, identifier check, and
  parameterization of the verifier's Arrow-specific budget constants
  (`VerifierRuntime.jl:64-67`) and staging policy.
- **A conformance corpus** against flatc golden binaries (monster_test.fbs)
  — the only credible correctness story for a generator.

Estimate: roughly 4-6 engineer-weeks to a registered, conformance-tested
v0.1 (parser+IDL 1-2 wk, runtime completeness ~1 wk, verifier
generalization ~1 wk, corpus+CI 1-2 wk), plus indefinite maintenance of a
general-purpose surface Arrow does not need.

### 1.7 Recommendation

**Keep vendoring for 3.0 (option i), leave extraction as a post-3.0 option
(option iii); do not retrofit FlatBuffers.jl v0.6 (option ii).**

- The whole owned surface — runtime + verifier runtime + generator — is
  ~1,630 lines, regeneration is mechanical (`tools/fbsgen.jl` exists exactly
  because hand-drift was the bug class, `:24-31`), and the verifier budgets
  are security posture the project must control and version with itself.
- A dependency on an external FlatBuffers package re-couples the metadata hot
  path to another release cadence and supply chain — the opposite direction
  from the rounds 29-30 dependency-trimming work.
- Option ii is strictly worse than iii: same engineering as a new package
  plus a breaking transition imposed on the dormant package's dependents.
- If community demand materializes, extract as **FlatBuffersGen.jl** with the
  1.6 list as the roadmap; Arrow should keep vendoring its *generated output*
  regardless (no build-time codegen), so migration risk to Arrow is low and
  deferrable.
- Independent of that decision, three cheap in-repo cleanups: fix or delete
  `reset!`/`union!`/`bytevector` (§1.2), de-IOBuffer `vtableEqual`, and batch
  `pad!`/`write` (§1.3).

---

## 2. Recent C-data PRs on apache/arrow-julia

Tracking issue: **#184 "Support C data interface"** (open). Three
independent efforts, all against the 2.x internals — which is why each
re-derives lifecycle machinery the Core rewrite gets structurally
(`src/cdata.jl:22-27` states this as the prove-out's claim, and its header
already cites #178, #179, #561, #594, #603-607).

### 2.1 Inventory

| PR | Author | State | Size (gh) | Scope |
|---|---|---|---|---|
| #561 | ollemartensson (Olle Mårtensson) | OPEN, since 2025-08-31 | +3388/−1 | `export_to_c`/`import_from_c`, format strings for all types, GuardianObject + ImportedArrayHandle lifecycle, 37 tests + property tests, `examples/cdata_demo.jl`. Predecessor #560 closed. |
| #594 | robertbuessow (Robert Büssow) | OPEN, since 2026-05-25 | +2130/−1 | `from_c_data`/`to_c_data` claiming primitive, bool, list, FSL, map, struct, union, dict-encoded; `CDataHandle` with GC-finalizer safety net (`jl_safe_printf`, atomic counter); **C-compiler `offsetof()` probe ABI test**; leak-count testset. Follow-ups #595 (remove finalizer) and #596 closed within days — visible design churn on ownership. |
| #603 | samtalki (Samuel Talkington) | OPEN, since 2026-07-06 | +1952/−4 | Import foundation: null/primitive/top-level struct → Tables.jl column table; layout/count/offset/flag/dictionary validation; move semantics; misaligned-buffer copy; dict-encoding fix for non-1-based pools. |
| #604 | samtalki | OPEN draft | +2838/−4 | Import breadth: bool, string, binary, list, FSB, FSL, temporal, decimal; nested structs. |
| #605 | samtalki | OPEN draft | +3947/−4 | Export: `to_c_data(col; name)` / `to_c_data(tbl; names)`; independent schema/array owners; child-release on partial-build failure. |
| #606 | samtalki | OPEN draft | +4257/−4 | Hardening: deterministic malformed-import fuzz, compile-a-C-producer smoke test, GC stress, **optional PyArrow capsule smoke test**. |
| #607 | samtalki | OPEN, since 2026-07-12 | +1246/−1 | Null+primitive import only — extracted from #603 at kou's request; the reviewable head of the stack (`src/cdata.jl` 647 lines + `test/cdata.jl` 592). |

Engagement state: **kou (Sutou Kouhei) is actively reviewing** #603/#607;
samtalki is responsive (multiple restacks, −400 LOC on request, ownership
model reworked to match arrow-rs/nanoarrow after review), is transparent
about AI assistance (Generated-by: OpenAI Codex; later Copilot and Fable 5
passes), and **already credits robertbuessow and ollemartensson as
co-authors** on every PR in the stack. None of the three efforts includes
`ArrowArrayStream` in either direction.

### 2.2 API and lifecycle vs our `src/cdata.jl`

Ours (included in the facade at `src/Arrow.jl:82`): `parseformat`
(`cdata.jl:223`), `to_c_data(f::Field, d::ArrayData) -> (Ptr{CArrowSchema},
Ptr{CArrowArray})` with independent schema/array roots (`:684-720`),
`from_c_data(sp, ap) -> (Field, ArrayData)` (`:952`),
`export_stream!(sp, sch, batches)` (`:1457`) and `from_c_stream(sp) ->
ImportedStream <: AC.RecordBatchSource` (`:1625`). Export lifecycle: one
release callback per structure, malloc'd per-node control blocks, an
`EXPORT_REGISTRY` of `ExportedRoot`s, canonical-topology release traversal
that never trusts consumer-mutated counts (`:412-468`), and an explicit
`reap!()` (`:759`) with a tested in-progress-export/reaper race
(`test/cdata_battery.jl:41-60`). Import lifecycle: single `ForeignOwner` per
moved tree, atomic exactly-once release with producer-conformance check
(release must NULL the release field, `:928-930`), declared buffer extents
from the layout registry (`:1137-1231`), then the full three-stage Core
validation (`validate_structural`/`semantic`/`full`, `:975-977`). Tests
include per-ABI struct size/offset gates (64-bit, both 32-bit int64
alignments; `test/cdata_battery.jl:18-38`) and a four-thread re-exec stress
child (`test/cdata_stress_child.jl`).

Comparison against samtalki's #607 head (the code most likely to merge):

- **Ownership container.** Theirs: one `CDataOwner` holding *both* moved
  structs in Julia `Ref`s, releasing schema and array together at owner
  release, exactly-once via `released::Bool` under a `ReentrantLock`, with a
  trylock-retry GC finalizer. Ours: schema released **immediately after
  parsing** (`:979-983`) — the producer's schema obligation ends at import —
  and the array copy lives in malloc'd memory with an atomic-swap
  exactly-once (`:922-935`). Ours additionally verifies the producer nulled
  the release field; theirs does not.
- **Post-release semantics.** Theirs: every `getindex` runs inside
  `_with_live` — a ReentrantLock acquire per element — so reads after
  `release_c_data` throw. Ours: reachability-based validity with documented
  spec-UB after explicit `release!` (`:60-64`), zero per-read overhead. Their
  gate is a real safety-UX win and a real throughput cost; the right review
  feedback is to make it optional, and the right 3.0 stance is to consider a
  checked/debug import mode rather than an always-on lock.
- **Misaligned buffers.** Theirs copies misaligned fixed-width buffers into
  aligned storage (mirroring arrow-rs). Ours stays zero-copy for any
  alignment because `loadat` falls back to an unaligned load per element
  (`src/ArrowCore.jl:325-332`).
- **`null_count == -1`.** Equivalent policy (bitmap required when unknown);
  theirs resolves eagerly with a word-wise `_count_nulls`, ours defers to
  `ArrayData`'s on-demand atomic `nullcount` (`src/ArrowCore.jl:669`).
- **Bounded string imports.** Theirs caps C-string scans at 4096 bytes
  (`_unsafe_string_bounded`); our `_import_cstring` is an unbounded
  `unsafe_string` (`cdata.jl:1078-1082`). Within the trusted-ABI rule this is
  defensible, but the cap converts a missing NUL from a memory scan into a
  clean error — cheap to adopt.
- **Schema metadata.** Their import validates the metadata block's bounds.
  Ours neither imports (`_import_field` never reads `sch.metadata`) nor
  exports it (`metadata = C_NULL`, `cdata.jl:631`) — a genuine functional gap
  to close in the production adapter.
- **Scope.** Ours covers unions, views/list-views, REE, dictionaries, and
  both stream directions with exception-safe move seams enumerated at each
  boundary (`:963-991`, `:1633-1660`); their landed scope (#607) is
  null+primitive, with breadth and export still drafts and streams absent
  everywhere.

From the other two: #594's **C-compiler `offsetof()` probe** is stronger
than our Julia-side static asserts (it checks against an actual C compiler's
layout at test time) and its finalizer-discipline findings (no task switches
in finalizers, atomic counters) are hard-won Julia-runtime knowledge that
samtalki's stack absorbed; #561 contributed the first complete format-string
map and property-test framing.

### 2.3 What to incorporate, and how to credit

Worth porting (with `Co-authored-by` credit):

1. #606's deterministic malformed-import fuzz corpus and compile-a-C-producer
   smoke test; the optional **PyArrow capsule round-trip** — we currently
   have no external-implementation integration test for C-data.
2. #594's C `offsetof()` probe alongside our static ABI gates.
3. #607's bounded C-string reads and metadata-bounds validation; schema
   `metadata` import/export (our gap, §2.2).
4. Naming convergence is already free: `from_c_data`/`to_c_data` match; keep
   `release_c_data`-style user-facing verbs in the facade docs so their users
   land softly.

Engagement: these are three good-faith contributors who converged on the
same wall (2.x internals lack an `ArrayData`-shaped core; five stalled
attempts, `cdata.jl:24-27`). Concretely: (a) comment on #607/#603 with the
3.0 plan before it merges redundant machinery, raising the per-getindex lock
and deferred-schema-release points as review feedback; (b) invite samtalki
and kou to review 3.0's `src/cdata.jl` lifecycle design; (c) credit all
three (samtalki, robertbuessow, ollemartensson) in the facade's C-data docs
and in commit trailers when porting their tests; (d) offer the stream
interface and the not-yet-drafted types (unions, views, REE) as follow-up
work they could build on 3.0's Core rather than on 2.x.
