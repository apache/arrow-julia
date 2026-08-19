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

# ArrowCore prove-out review — round 18

Date: 2026-08-14

Scope: design commit `2636810`, Stage-A commit `9de423e`, ranged-read commit
`3c3c5bc`, statistics commit `b6f8dca`, and their round-18 amendments. The
Tables authority was the local `jq/scan` branch at
`5b4986c0e49260bbcc0965f8386f98b32b4821fd`.

## Findings and dispositions

1. **Positional filter references could return wrong values after Stage-A
   projection.** A bound `Tables.col(3)` stayed positional in the residual.
   Rebinding it against the reduced decode-set table could select another
   field or fail. Disposition: fixed in `6a05250`. The residual now rewrites
   matched positional references to source names. Whole-file and ranged
   differential tests cover positional filters, duplicate selections,
   renames, rename/source-name collisions, and filter-only columns.

2. **Unvalidated RecordBatch lengths could shift a consumed window, and
   zero-column results lost row counts.** A forged batch length could move an
   offset into the wrong later row without decoding the corrupt batch.
   Empty `NamedTuple`s also represented every zero-column file as zero rows.
   Disposition: fixed in `6a05250`. `_recordbatchmeta` validates the global
   length, every top-level FieldNode length, exact node/buffer counts, and
   buffer geometry before planning. `_ScanColumns` preserves explicit row
   counts. Corruption tests cover both apply paths and a `[3, 0, 2]`
   zero-column file.

3. **Extreme windows broke the literal apply/finish equation.** The current
   Tables authority overflows while forming `offset + 1` or `offset + limit`.
   Arrow had consumed the window and returned a mathematically sensible empty
   result, while the authority threw `BoundsError`. Disposition: fixed in
   `54b1cf8`. Such windows remain residual until Tables uses safe arithmetic,
   so both sides have the same observable behavior.

4. **The ranged planner accepted Footer and metadata states that the whole
   reader rejected.** Missing checks included overlapping Blocks, forbidden
   dictionary replacement, message/body/metadata limits, complete
   RecordBatch metadata, and an out-of-body zero-length buffer. Some selected
   buffers were fetched before their sizes were checked. Disposition: fixed
   in `743e1a8`. The ranged path now validates limits, features, the complete
   Block index, message kinds, node/buffer metadata, and body containment
   before body fetches. Fetch payload count/length overrides also fail closed.

5. **Range coalescing and the written request model were not sound at the
   edges.** `_coalesce` could overflow, and the design incorrectly put
   limit/offset pruning before the RecordBatch metadata pass even though
   Footer Blocks have no row counts. It also omitted the head and optional
   exact-footer requests. Disposition: fixed in `743e1a8` and the amended
   design. Coalescing uses checked ends and difference-based gap comparison.
   The design and test text now distinguish metadata requests from excluded
   body requests and state the configured over-read policy.

6. **Whole-file Scan could bypass the operation-wide allocation limit.** It
   created a fresh budget for each batch, unlike the ranged path. Two
   compressed batches could each fit alone while exceeding the aggregate
   limit. Disposition: fixed in `743e1a8`. One whole-file apply now shares one
   `AllocationBudget` and codec state across metadata and decompression. The
   two-path compressed aggregate test pins parity.

7. **Float statistics pruning had false negatives.** `isless`/`isequal`
   imposed total-order semantics that differ from Tables predicates. Signed
   zero equality/range/membership and `!(x == NaN)` could prune qualifying
   rows. Disposition: fixed in `8ee162e`. `_maypass` now uses the predicate's
   IEEE operators, treats incomparable results conservatively, and disables
   bounds when NaN occurs. The finite/zero/infinity/NaN property matrix and
   whole/ranged regressions have no false prune.

8. **Statistics metadata was not safely optional or budgeted.** A valid IPC
   stream with the wrong schema could throw an uncaught `BoundsError`.
   Base64 and embedded-stream decode used a fresh default budget. Swallowing
   partial budget exhaustion also made a larger caller limit fail where a
   smaller limit passed. Disposition: fixed in `8ee162e`. The embedded stream
   shares the scan budget. `AllocationLimitError` propagates, while malformed
   content within budget degrades to no pruning. The reader validates the
   canonical statistics schema skeleton before using values. Tests cover bad
   Base64, wrong field count, a two-field Bool look-alike, batch-count
   mismatch, and a compressed two-megabyte statistics value at two limits on
   both paths.

9. **The emitted official value layout used top-level ordinals instead of
   flattened RecordBatch FieldNode indexes.** A column after a nested field
   was therefore not interoperable. Dictionary folds also crashed or
   under-counted when a valid index resolved to a null pool value.
   Disposition: fixed in `8ee162e`. Writer and reader map flattened indexes;
   nested-field statistics are accepted but ignored by top-level pruning.
   Dictionary statistics use logical values and nullness. The nested
   `struct{a,b}, x` fixture pins column indexes `[null, 0, 3]`.

10. **The statistics trust tests did not pin every claimed path.** Wide and
    narrow lies were tested only on `ArrowFile`; malformed coverage was also
    narrow. Disposition: fixed in `0ed981e`. Whole-file and ranged tests now
    pin malformed degradation, conservative wide lies, and row-losing narrow
    lies. This matches the documented trusted-for-completeness boundary: the
    residual corrects false inclusions, but it cannot recover a pruned batch.

11. **The design and README overstated implementation and trim status.** The
    design still said “PROPOSAL”, described a future streaming writer as
    present, misstated the fetch order, and claimed the trim gate compiled a
    scan application. The README omitted the new example and its Tables
    development dependency. Disposition: fixed across `743e1a8`, `8ee162e`,
    and `9dce6fb`. P1–P3 are now labeled prove-out implementations. P4 and the
    missing project-dependent scan trim harness are explicit production work.

12. **Two defects remain in the external Tables authority, not in this core
    change.** `validate=false` omits unmatched filter references from
    `filtercols` but leaves the expression for `finish`, which then errors.
    Nested-list equality broadcasts the column against the literal and can
    throw `DimensionMismatch`. Arrow matches the current authority on both;
    a core-only workaround would violate the apply/finish comparison. These
    need upstream Tables decisions. The README now states that `jq/scan` is
    unreleased and locally developed.

13. **No defect found in the remaining skip/decode and may-pass surface.**
    Focused probes covered null, fixed-size list, sparse/dense union,
    compression, adjacent sparse spans, and nested dictionaries before a
    selected later field. `skipfield!`, `decodefield`, and `_bufferspan`
    consume the same registry traversal. A 20,000-case window probe covered
    exact boundaries, three-batch spans, empty batches, `limit=0`, and offsets
    beyond the total. Prefix-successor and heterogeneous-`In` probes stayed
    conservative; unsupported UInt64, interval, and struct bounds opt out.

## Assumptions and decisions

- The constrained GC-reachability memory model remains final. No lifecycle,
  revocation, interruption, guard, or `Threads.Atomic` mechanism was added.
- Footer-only schema authority is an intentional ranged divergence. It does
  not waive Block overlap, required-feature, limit, or RecordBatch metadata
  checks.
- Statistics under the local placement key are trusted for completeness.
  Malformed statistics are optional; caller resource-limit exhaustion is not.
- The active Tables source code is the protocol authority. Core residualizes
  its extreme arithmetic edge instead of changing the dependency or the
  untracked Manifest.
- Only `core/` tracked files changed. Existing unrelated untracked files were
  not modified.

## Validation

- `julia --startup-file=no core/test/runtests.jl` — 252/252 core and 4/4
  threaded-cache tests passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl` — passed.
- `julia --project=. --startup-file=no core/examples/ipc_write.jl` — passed.
- `julia --startup-file=no core/examples/cdata.jl` — passed.
- `julia --project=. --startup-file=no core/examples/scan_ranges.jl` — all
  Stage-A, byte-range, statistics, corruption, budget, and trust checks passed.
- `julia --startup-file=no core/test/trim_compile_tests.jl` — 6/6; compile and
  produced-binary run passed with the harness's zero-error/zero-warning gate.
- `git diff --check` — passed.

VERDICT: CLEAN
