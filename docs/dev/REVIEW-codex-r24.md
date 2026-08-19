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

# ArrowCore prove-out review — round 24

Date: 2026-08-15

Scope: `2148d76^..e3e097b` under `core/`. This includes the binding
regeneration commit because the round description and question 5 require it.

## Result

Findings remain. Two paths can change dictionary values. The conformance
comparison can also accept changed floating-point values.

## Findings

1. **HIGH — one record batch can silently change values when two fields share
   an id but carry different pools.** `writestream` accepts a caller id table
   that maps both fields to one id. `_streamfeatures` and the emission loop
   then process each `(field, pool)` in order
   (`core/examples/ipc_write.jl:637-650`, `681-690`). They treat an
   intra-batch pool change as a temporal dictionary replacement. Both
   dictionary messages precede the same record batch, so only the last pool is
   active when the record is decoded. A focused batch with id `7`, pool
   `["a"]` for field 1, and pool `["b"]` for field 2 wrote without error and
   read back as `["b"]`, `["b"]`. The writer must require one pool identity
   per id within each batch. It must also validate the value-schema
   compatibility of every caller-supplied shared id before it writes bytes.

2. **HIGH — a shared outer dictionary id can hide conflicting nested wire
   ids.** `validatedictionaryids` compares the logical value Fields for a
   repeated id, but it walks nested dictionaries only for the first occurrence
   (`core/examples/ipc_read.jl:721-749`). Two compatible
   `dict<list<dict<utf8>>>` fields can therefore share outer id `10` while
   their nested fields use ids `20` and `21`. The writer emitted dictionary
   batches in order `20, 10, 21`. Its own reader then threw a raw `KeyError`
   at `core/examples/ipc_read.jl:1329` because the id table contained only
   `10` and `20`. Removing batch `21` made the reader accept the stream and
   materialize both fields through pool `20`; it silently ignored field 2's
   declared nested id. `missingdicts` stops at a resolved outer dictionary
   (`core/examples/ipc_read.jl:1070-1087`), and the outer pool is decoded once
   through the first Field path (`1132-1138`). With the current one-pool-per-id
   model, every repeated id must have the same nested dictionary-id topology.

3. **MEDIUM — the corpus and oracle comparison accepts changed float values.**
   `_eq` applies `isapprox(rtol=1e-6, atol=1e-9)` to every float
   (`core/conformance/corpus.jl:83-86`). `docsequal` therefore reported no
   difference for all of these pairs:

   - `Float64(1.0)` and `nextfloat(1.0)`;
   - `Float32(1.0)` and `nextfloat(Float32(1.0))`;
   - `Float64(1000.0)` and `1000.0005`.

   The exact gold comparison also accepted a mutation of the real
   `cpp-21.0.0/generated_primitive` `float64_nonnullable` value from
   `471.617` to `471.6174`. Oracle results use the same comparator
   (`core/conformance/oracle.jl:280-285`). The Float32 JSON decimal quirk does
   not require a tolerance: every checked gold value matched exactly after
   both operands were converted to `Float32`, and Float64 values matched
   exactly as `Float64`. The comparison should use precision-aware exact
   equality, with explicit NaN handling.

4. **MEDIUM — ranged scans still enforce the nullability contract in the old
   semantic tier.** `_validateplannedfield!` rejects a fully covered
   non-nullable Field with a positive node null count
   (`core/examples/scan_ranges.jl:231-243`). `_validatebodyplan` starts every
   selected top-level Field with that mode (`310-316`), including ranged
   dictionary and record plans (`927-934`). A whole-file scan of a
   nonnullable `Int64` Field with values `[missing, 7]` returned those values.
   The equivalent ranged scan threw `ValidationError: fully covered
   non-nullable field declares a positive null count`. Tests at `2052-2053`
   and `2072-2073` still pin the obsolete rejection. This check does not
   protect body access. Buffer minima are checked at `245-257`, and decoded
   columns still receive semantic validation at `397-409`. Remove only the
   general Field-nullability check and its `allslots` propagation. Keep the
   fixed Null, Union, and REE layout rules.

5. **MEDIUM — fresh dictionary-id allocation can wrap into an existing id.**
   `assigndictids` starts at `maximum(values(given)) + 1` and increments with
   unchecked `Int64` arithmetic (`core/examples/ipc_write.jl:541-559`). With
   two given ids at `typemin(Int64)` and `typemax(Int64)`, a third dictionary
   Field received `typemin(Int64)` again. Arrow defines the wire id as a signed
   `long`; this code does not declare a smaller allowed domain. Allocation
   must use an occupied-id set and checked or wrap-aware search, or fail with
   `ValidationError` when it cannot allocate a fresh id. An ordinary partial
   table is otherwise correct: seeding only field 2 with `41` assigned
   `[42, 41, 43]`.

6. **MEDIUM — the oracle's text-wide capability classifier can turn a new
   interoperability failure into a skip.** `classify` skips every PyArrow or
   nanoarrow exception whose text contains `not yet supported` or
   `unsupported feature` (`core/conformance/oracle.jl:80-88`, `114-145`). It
   is not limited to the declared nanoarrow gaps or to known cases. For
   example, a writer regression that adds an unknown feature or spuriously
   declares `COMPRESSED_BODY` can make nanoarrow refuse our otherwise
   primitive stream as an unsupported feature. The runner records a skip and
   never reaches the value comparison. PyArrow accepting the stream does not
   prove nanoarrow compatibility. Whitelist expected skips by oracle, case,
   and feature. Treat every other error as a failure. The supplied `170 / 0 /
   43` result attributes its current skips to the expected gaps, but this
   classifier cannot preserve that invariant for later runs.

7. **LOW — the JSON renderer fabricates offsets for a malformed sliced empty
   array.** `_offsetlist` says its shortcut is for an unsliced array, but it
   tests only `b.len == 0 && n == 0`
   (`core/conformance/arrowjson.jl:348-359`). For Utf8, List, and Map data with
   `len=0`, `offset=1`, and an absent offsets buffer, `validate_structural` and
   `validate_semantic` correctly reject `offsets buffer too small: 0 < 8
   bytes`. `ArrowJSON.tojsoncolumn` instead returns `OFFSET = Int32[0]`.
   This does not permit an out-of-bounds read, but it lets the renderer hide
   malformed direct Core data. The shortcut must also require
   `d.offset == 0`.

8. **LOW — active documentation still states the pre-round contracts and
   binding state.** The module overview and `validate_semantic` docstring say
   nullability runs on every semantic call (`core/ArrowCore.jl:53-58`,
   `1230-1235`). `core/README.md:150-165` still describes raw binding bridges
   and says nested dictionary encoding is rejected. Lines `198-205` say the
   IPC writer checks every Field contract, assigns distinct ids, and
   re-encodes shared pools. Lines `245-247` say the bindings still need
   regeneration. These claims now contradict the scoped changes. The local
   byte-wise verifier is still present, so only the binding-regeneration half
   of the last statement is stale.

## Load-bearing audit

### Validation tiers

- `validate_semantic` still composes structural validation first. Structural
  validation checks buffer counts and fixed sizes, child extents, union
  descriptor ids, Map schema rules, and REE schema and extent rules
  (`core/ArrowCore.jl:952-1093`).
- Its intrinsic stage still checks offset monotonicity and final bounds,
  dictionary index bounds, union ids and dense offsets, view ranges,
  ListView ranges, REE ordering and coverage, and bitmap/null-count agreement
  (`core/ArrowCore.jl:1241-1327`, `1354-1441`).
- Date64 day divisibility, Time range, Decimal precision, and general
  `Field.nullable` enforcement are absent from semantic validation and are
  present under `validate_full` (`core/ArrowCore.jl:1118-1224`,
  `1599-1653`). `validate_full` also retains the pre-existing Utf8 and
  Utf8View content check. Body UTF-8 was already a full-tier check before this
  round; Field names, metadata, and timestamp timezone UTF-8 remain
  structural.
- No moved check supplies a memory bound. Date, Time, and Decimal access uses
  descriptor-fixed widths. Validity access uses the bitmap, not
  `Field.nullable`. Raw loads retain final bounds checks. C Data import and
  export call `validate_full` by policy. The ranged-scan finding is a contract
  rejection, not a memory-safety dependency.

### Dictionary fidelity

- `IPCStream` and `ArrowFile` retain the adapter id table, and their rewrite
  methods pass it back to the writer. The gold shared-dictionary case retained
  ids `[0, 0]`. The generated nested-dictionary case retained all five ids.
- Post-order `dictionarypools` traversal is correct for ordinary nested
  dependencies (`core/examples/ipc_write.jl:566-586`).
- Per-position dictionary normalization is necessary for the current gold
  corpus because its nested-dictionary JSON uses three pools while its own
  stream and file use five. It still compares each position's indices and
  resolved pool values. It cannot prove id topology, so focused topology tests
  must cover the two dictionary findings above.

### Empty offsets

- The IPC change itself is safe. `decodefield` creates offset-zero data,
  accepts an absent offsets buffer only for a zero-length node, and rejects a
  nonempty partial slot (`core/examples/ipc_read.jl:1106-1150`). Semantic
  validation runs before publication.
- Focused probes accepted buffer lengths `0` and `4` and rejected `3` for
  Utf8, List, and Map. LargeUtf8 accepted `0` and `8` and rejected `7`.
- Unsliced empty materialization executes no element or offset loop. A sliced
  empty array must carry `(offset + 1) * offsetwidth` bytes and is rejected by
  structural and semantic validation without that prefix
  (`core/ArrowCore.jl:988-995`, `1253-1274`). `loadat` and `subslice` keep
  their final checked bounds. Finding 7 is a renderer consistency defect, not
  an IPC or memory-safety defect.

### Corpus and oracle normalization

- Map child-name normalization is position-only. The same gold family uses
  `some_entries/some_key/some_value` in JSON and file data but
  `entries/key/value` in stream data. It does not reorder children or values.
- Decimal normalization removes `bitWidth` only when it is `128`, which is the
  FlatBuffers default (`core/metadata/Schema.jl:325-339`). A width of `64`
  remains a difference.
- The Python driver's default `RecordBatch.validate()` is consistent with the
  stated structural-oracle policy. It does not opt into PyArrow's more
  expensive full checks. Returned bytes are then read by Core and compared by
  value. The Core side does not call `validate_full`, whose advisory checks
  would reject the declared gold Date64, Time, Decimal, and nullability cases.
  Findings 3 and 6 are the unsound comparison and classification paths.

### Regenerated bindings and verifier

- Running `core/tools/fbsgen.jl` against the current Apache `Schema.fbs`,
  `Message.fbs`, and `File.fbs` produced a byte-for-byte match for all four
  files under `core/metadata/`, including `Flatbuf.jl`.
- The generated bindings have the current five-slot RecordBatch, 64-bit
  `variadicBufferCounts`, Decimal `bitWidth=128` default, type tags through
  `LargeListView`, Schema features, and five-slot Footer.
- `fbsgen.jl` generates getters and builders. It does not generate a
  verifier. Commit `2148d76` leaves the local byte-wise verifier implementation
  in front of the new getters. The current `ipc_read.jl` command rejected the
  oversized metadata vector, V3 metadata, mixed message versions, and the
  retargeted misaligned vector fixture, then printed its FlatBuffer
  verification sentinel. The IPC write command also passed the current
  wire-shape checks. No verifier acceptance regression was found.

## Assumptions and decisions

- The written range `2148d76..HEAD` normally excludes `2148d76`. I included
  that commit because the request explicitly includes the regenerated
  bindings and asks question 5 about them.
- A `dictids` keyword table is treated as caller input. It cannot be trusted
  only because reader round-trips are its primary use.
- This was a review request. I wrote this report only. I did not change product
  code or create a fix commit.
- I did not run the Docker oracle, as directed. I reviewed its source and used
  the supplied `170 pass / 0 fail / 43 skip` result only as prior execution
  evidence.
- The constrained GC-reachability model, four `_of` ladders, and Tables dev
  dependency remain unchanged. The five unrelated untracked files were not
  modified.

## Validation

- `julia --startup-file=no core/test/trim_compile_tests.jl` — 6/6 passed.
- `julia --startup-file=no core/test/runtests.jl` — 325/325 Core and 4/4
  threaded-cache tests passed.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_read.jl`
  — passed, including malformed metadata and empty-offset checks.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_write.jl`
  — passed.
- `julia --startup-file=no core/examples/cdata.jl` — passed, including the
  four-thread child.
- `julia --project=core/conformance --startup-file=no core/examples/scan_ranges.jl`
  — passed on an exact rerun. The first invocation stalled in Julia's
  multi-threaded compilation of `_stats_base_fixture`; a one-thread isolated
  call and the unchanged exact ten-thread rerun both completed. No code or
  environment change was needed for the passing exact run.
- `julia --project=core/conformance --startup-file=no core/conformance/corpus.jl`
  — 275 pass / 0 fail / 36 declared skips.
- Focused probes reproduced both dictionary failures, the id wrap collision,
  the ranged nullability split, the sliced-empty renderer split, and all three
  float false-equalities.
- `git diff --check 2148d76^..HEAD` reports only one new blank line at EOF in
  each generated `File.jl`, `Message.jl`, and `Schema.jl`.

VERDICT: FINDINGS
