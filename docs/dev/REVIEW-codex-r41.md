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

# Arrow.jl 3.0 code review — round 41

Date: 2026-08-17

Scope: exact fix commit `621ea4581038c1ebe6c0fa80627c78d297be811e`
on `core-rewrite`. Its parent,
`4c8df01b40d20d8425e579a00c97e6bf8e2409a4`, records round 40 against
code commit `31922e991b3bd4a332cdde1c414e5c9fc5b18523`. I used the
manifest-selected Tables.jl checkout on `jq/scan` at
`d1fbb6eb577741688dba70039754166b51c1cdcc` as the authority.

## Result

Round 40 is not clean. One MEDIUM finding remains.

The orphan-dictionary fix closes finding 1 at the root. Every nonempty
dictionary-block index now rejects before dictionary header parsing or any
predicate/window early stop. The recursive list imposition also closes
finding 3. Exact nested schemas, large offsets, nullability, metadata,
validation-before-publish, and replacement errors all pass.

The six direct vector layouts in the requested parity check pass. The
original Binary, FixedSizeList, Struct, and Map empty-retention cases pass
12/12. FixedSizeBinary and BinaryView add 6/6. The declared-type rule is
still not exhaustive. It omits reachable ListView, Decimal, and Interval
layouts. It also loses field-only RunEndEncoded information through
Dictionary and treats every Union as undecidable. These cases reproduce the
same empty/nonempty split and empty rewrite failure as round-40 finding 2.

All five required gates pass. The full round-38/39/40 clean set also stays
clean.

## Findings

1. **MEDIUM — the declared-type rule still omits supported closed
   materializers.**

   `_boundschema` uses `_declaredeltype` for an empty pre-override column but
   the observed `eltype` for a nonempty column at `src/table.jl:563-571`.
   The new table at `src/table.jl:551-561` therefore must cover every closed
   facade row domain. Its own comment makes that contract explicit at
   `src/table.jl:546-550`.

   Three direct Core families are still absent:

   - `ListViewType` returns `Vector{Any}` at
     `src/ArrowCore.jl:2052-2062`.
   - `DecimalType` returns `Int32`, `Int64`, or `Vector{UInt8}` at
     `src/ArrowCore.jl:1870-1884`.
   - `IntervalType` returns `Int32` or one of two fixed `NamedTuple` row
     types at `src/ArrowCore.jl:1887-1901`.

   These layouts are reachable through both IPC adapters. Their read mapping
   is at `src/ipc_read.jl:320-332`; their write mapping is at
   `src/ipc_write.jl:186-220`.

   One composition also loses a declared type. Root REE recursion uses the
   values child Field at `src/table.jl:541-545`. Dictionary recursion instead
   calls `_declaredbasetype(t.valuetype)` at `src/table.jl:560`, where no
   child Field is available. A valid `Dictionary<REE<Binary>>` therefore
   falls back to `Any`.

   The blanket Union defense is also incomplete. `Any` is conservative for a
   genuinely heterogeneous Union whose observed winner can change the public
   element type. It is not correct for every Union. Union children live on
   the Field. A one-child Union has a closed domain, and an all-compatible
   child set has a decidable override result. Valid `Union<Int64> => Integer`
   and `Union<List<Int64>> => Vector` controls reproduce the split.

   The focused results are:

   | Layout group | Empty keep | Nonempty keep | Empty rewrite |
   |---|---:|---:|---:|
   | Six requested direct vector layouts | 18/18 | 18/18 | not applicable |
   | ListView, Decimal, Interval | 0/27 | 27/27 | 54/54 refused |
   | Dictionary of REE of Binary | 0/3 | 3/3 | 6/6 refused |
   | Decidable Union controls | 0/6 | 6/6 | 12/12 refused |

   Values and public element types still match `Tables.finish`. The failure
   is retained identity and function. Every omitted empty rewrite refuses
   with a clean `ArgumentError`; none emits drifted bytes.

   This is the same root and impact as round-40 finding 2, so I keep its
   MEDIUM severity. The declared decision needs Field-aware handling for all
   registered materializers. Union handling can decide compatibility across
   its declared children without assuming one universal runtime row type.

## Closed portions of round 40

- **Finding 1 is closed.** The zero-field ranged branch validates the complete
  block index at `src/scan.jl:967`, rejects any nonempty dictionary index at
  `src/scan.jl:971-972`, and only then evaluates the filter and record window.
  `_zerofieldblockcount` is record-only at `src/scan.jl:913-930`.
- Real unknown-id, delta, duplicate-id, and inner-RecordBatch-mismatch
  dictionary messages all reject through the full reader and direct ranged
  scan. The ranged path gives the same orphan error for all four. An
  `AlwaysFalse` filter cannot bypass it. Fetch logging confirms that no
  dictionary header is read.
- **Finding 3 is closed.** Retained list rewrites route through `_imposelist`
  at `src/write.jl:199-214`. The recursion at `src/write.jl:240-278`
  restores list and leaf names, nullability, metadata, and descriptor width.
  Offset widening copies all `offset + len + 1` entries at
  `src/write.jl:248-261`.
- Exact recursive schemas pass 24/24. Large-parent rewrites pass 12/12.
  Sliced offsets, empty slices, null padding, nested non-nullable refusals,
  and actionable replacement errors also pass.
- The rebuilt ArrayData is not published unchecked. The IPC writer validates
  every column at `src/ipc_write.jl:623-645` before encoding starts. The
  facade finishes `_writebytes` before it writes to the caller IO at
  `src/write.jl:409-412`. Malformed imposed offsets reject with no partial
  output.

## Clean regression sweep

- Frame integrity, cumulative file and ranged budgets, charge-before-fetch,
  `limit=1` laziness, later-block early stop, and every `OpNode` control pass
  40/40.
- The file header charge remains 4,688 bytes. The ranged footer charge is
  5,984 bytes and one ranged header costs 4,776 bytes. The exact 10,760-byte
  cap succeeds. The former split cap of 10,672 bytes rejects.
- The prior small-list matrix passes: Tables authority 6/6, empty and
  nonempty retention 3/3 each, and empty and nonempty rewrites 6/6 each.
- Expanded list shapes pass 42/42. Empty conversions match the authority
  30/30, retention passes 30/30, rewrites pass 60/60, and listed parity passes
  5/5. Replacement errors pass 24/24 and Bool preservation passes 6/6.
- The direct zero-field matrix passes 57/57. Reject windows pass 10/10.
  Overflow and residual controls pass 18/18. Pathological-name controls pass
  17/17.
- Allocation is flat between 10,000 and 1,000,000 declared rows: file 5,488
  bytes, stream 1,792 bytes, and ranged 18,880 bytes at both sizes.

## Assumptions and decisions

- I used `Tables.apply`, `Tables.finish`, and `Tables.bind` from the
  manifest-selected checkout as the semantic authority.
- I treated layouts accepted by Core validation and both IPC adapters as
  supported layouts.
- I treated retained identity as the exact recursive Arrow descriptor,
  names, nullability, field metadata, and schema metadata.
- I treated a zero-field schema as unable to declare a dictionary id. This
  matches the full reader and the normal ranged path.
- I accepted `Any` for a genuinely heterogeneous Union. I did not accept it
  as a blanket answer for one-child or all-compatible Union fields.
- I treated values below a null list slot as unreachable padding. Widening
  may canonicalize that hidden padding, but it must preserve every reachable
  value and valid offset.
- I rated the remaining declared-type failure MEDIUM, consistent with
  round 40. It loses retained identity and prevents empty rewrites, but it
  refuses cleanly.
- The host is 64-bit, so `Int` and `Int64` have the same range in the overflow
  probes.
- I made no product or test changes. All probes ran in scratch directories.
  One scratch probe was briefly created at the repository root because of a
  path-resolution error. It was moved to `/tmp` immediately. The final
  worktree preserves the six pre-existing untracked files and adds only this
  review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 246/246, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, and the compiled binary run passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 skip.
- Dictionary adversarial probe — exit 0; zero-field assertions 36/36 and
  normal-schema semantic controls 16/16. All four requested defects refused.
- Requested-layout closure probe — exit 0; the six direct layouts pass 36/36
  across empty/nonempty and file/stream/ranged inputs.
- Exhaustive declared-type diagnostic — exit 0; 144/144 covered checks,
  260/260 omitted-layout reproduction checks, 40/40 Union controls, and 3/3
  recursion controls. Its closure-contract assertion exits 1 as expected:
  omitted-layout empty retention is 0/30.
- Empty rewrite impact probe — exit 0; all 60 omitted-layout transitions and
  all 12 decidable-Union transitions refused with no emitted drift.
- Tables authority controls — exit 0; 12/12.
- Exact nested-descriptor probe — exit 0; values 36/36, exact recursive
  schemas 24/24, and large-parent rewrites 12/12.
- Nested imposition edge probe — exit 0; null/empty/padding transitions
  24/24, sliced widening 3/3, non-nullable refusals 6/6, actionable
  replacement errors 4/4, and pre-publish malformed-offset refusal 1/1.
- Round-40 scan closure probe — exit 0; 40/40.
- Direct zero-field matrix — exit 0; 57/57.
- Reject-window controls — exit 0; 10/10.
- Allocation-flatness probe — exit 0; zero growth on file, stream, and ranged
  paths.
- Overflow and residual probe — exit 0; 18/18.
- Pathological-name probe — exit 0; 17/17.
- Prior small-list matrix — exit 0; authority 6/6, retention 6/6, and rewrites
  12/12.
- Prior expanded facade probe — exit 0; shapes 42/42, conversions 30/30,
  retention 30/30, rewrites 60/60, parity 5/5, replacement errors 24/24, and
  Bool preservation 6/6.
- `git diff --check` — exit 0.

VERDICT: FINDINGS
