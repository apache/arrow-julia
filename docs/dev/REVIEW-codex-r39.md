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

# Arrow.jl 3.0 code review — round 39

Date: 2026-08-17

Scope: exact fix commit `0d71f2b2938984d607063263df418943dc855fc9`
on `core-rewrite`. Its parent, `10fac82601582c6cf74d55e7615363f12855d778`,
records round 38 against code commit
`adf72aa0be73ae7607b3a29f3112cbe4832cd220`. I used the manifest-selected
Tables.jl checkout on `jq/scan` at
`d1fbb6eb577741688dba70039754166b51c1cdcc` as the authority.

## Result

Round 38 is not closed. Five findings remain: two HIGH and three MEDIUM.

The two zero-field semantic fixes work on valid inputs. The direct scan matrix
passes 57/57. The million-row facade allocation is flat in the declared row
count. The incremental window passes every requested overflow case and returns
an empty residual, so `Tables.finish` does not apply the window twice.

The list fix is partial. Empty and nonempty reads now retain their descriptors
and metadata, and all six nonempty rewrites pass. All six empty rewrites still
fail, so the required rewrite matrix passes only 6/12.

The fix also introduces two HIGH defects in the direct zero-field scan paths.
The ranged path accepts malformed block framing. Both file and ranged paths
bypass the cumulative allocation budget. Two new MEDIUM correctness defects
also exist: the empty-column schema rule retains stale descriptors after real
conversions, and the facade silently accepts unsupported extension predicates
when `validate=false`.

## Findings

1. **HIGH — zero-field `RangedFile` scans bypass block-frame validation.**

   `Tables.apply(::RangedFile, ...)` parses the footer at
   `src/scan.jl:933-953`, but its new zero-field branch returns at
   `src/scan.jl:954-961`. This happens before `_validateblockindex` at
   `src/scan.jl:969` and before the normal per-block parser at
   `src/scan.jl:1000-1019`.

   `_zerofieldbatchrows` fetches each payload from `off + 8` at
   `src/scan.jl:903-923`. It therefore skips the continuation prefix and the
   declared metadata length. It also omits the message body-length check, the
   footer/message version check, compression rejection, and the global block
   extent and overlap checks. `_parseblockmeta` performs the missing frame and
   body-length checks at `src/scan.jl:794-816`.

   I zeroed the continuation prefix of a valid zero-field record block. The
   full reader rejected the bytes with
   `ValidationError: footer block does not point at a message`. Direct ranged
   scans accepted the same bytes and returned three rows, or one row with
   `limit=1`.

   This is not a body-fetch requirement. The metadata-only path must still use
   the normal block-index and frame validation before it trusts the row count.

2. **HIGH — the new zero-field paths bypass the cumulative allocation
   budget.**

   The file path calls `_batchrows(f, i)` from the generator at
   `src/scan.jl:573-578`. That overload creates a new `AllocationBudget` for
   every batch at `src/scan.jl:345-357`. The column-bearing path instead
   creates one budget at `src/scan.jl:585` and passes it through every header
   read at `src/scan.jl:591`.

   A 2,000-batch probe used
   `Limits(max_total_allocated_bytes=2_105_336)`. One shared budget refused at
   batch 450. The zero-field scan accepted all 2,000 batches and returned
   2,000 rows. Each header charged 4,688 bytes, for 9,376,000 cumulative bytes.

   The ranged path has the same contract failure. Its caller creates a budget
   and parses the footer at `src/scan.jl:936-937`. The helper then creates a
   second budget and parses the footer again at `src/scan.jl:903-905`. A
   one-batch probe accepted 16,656 bytes of documented cumulative charges
   under a 10,672-byte limit. The helper also grows an uncharged `counts`
   vector at `src/scan.jl:906,920`, fetches a header before charging it at
   `src/scan.jl:912-913`, and materializes every count before a capped window
   can stop.

   This is separate from the fixed row-count mask allocation. The valid
   million-row facade probe is now flat. The new direct paths still discard
   the reader-wide cumulative budget that `Limits` documents at
   `src/ipc_read.jl:63-77`.

3. **MEDIUM — empty list facades still cannot be rewritten.**

   Empty and nonempty `ListType(false)` reads retain their field descriptor and
   column metadata on file, stream, and ranged inputs. Nonempty rewrites pass
   6/6. Empty rewrites fail 0/6, so the required 12-transition matrix passes
   only 6/12.

   `_writecolumn` sends an abstract list element type to `_narrowlists` at
   `src/write.jl:69-79`. `_narrowlists` derives its child type only from
   observed nonempty values at `src/write.jl:85-103`. A zero-row list has no
   observed child value, so the function leaves `Any` and the writer raises:

   ```text
   ArgumentError: column l has element type Any and cannot be narrowed to a
   writable Arrow column; give it a concrete element type
   ```

   The retained-field writer still calls this natural inference path at
   `src/write.jl:199-205` instead of using the retained list child descriptor.
   Row-bearing all-empty lists and nested lists also fail, which confirms that
   the new pass is observation-dependent and is not recursive.

   The regression at `test/facade_tests.jl:605-622` checks one nonempty
   stream-to-stream rewrite and one empty read-retention case. It does not
   exercise an empty rewrite or the required path/format matrix.

4. **MEDIUM — the empty-precolumn rule retains stale descriptors after real
   conversions.**

   `_boundschema` now skips its conversion test whenever the pre-conversion
   column is empty at `src/table.jl:539-542`. This keeps the source field for
   every zero-row override, not only for an override that subsumes the source
   facade type. That contradicts the keep/drop rule stated at
   `src/table.jl:526-531`.

   An empty `Int64 => Float64` scan produced the correct public `Float64[]` on
   file, stream, and ranged inputs, but retained the source Arrow `IntType`
   descriptor. Every rewrite then rejected the mismatch. An empty
   `List => String` conversion behaved the same way: the public values matched
   `Tables.finish`, but the retained descriptor remained `ListType`.

   Empty conversions need a descriptor-based compatibility decision. A blanket
   empty-column exception preserves metadata and types that no longer describe
   the output.

5. **MEDIUM — zero-field facades silently accept unsupported extension
   predicates when `validate=false`.**

   `_publicscan` calls `Tables.bind` only when `scan.validate` is true at
   `src/table.jl:458-466`. `_zerofieldpredicate` maps every unrecognized node
   to `missing` at `src/scan.jl:434`. The Tables.jl authority rejects `OpNode`
   during binding at `~/.julia/dev/Tables/src/scan.jl:377-379`, regardless of
   the reference-validation setting.

   With `Tables.OpNode(:custom, Any[])` and `validate=false`, file, stream, and
   ranged facades all returned zero rows. `Tables.finish` raised the required
   `ArgumentError`. `validate=false` permits unmatched column references; it
   does not make an unsupported operation executable.

## Closed portions of round 38

- The focused direct zero-field matrix passed 57/57. `AlwaysTrue`, windows,
  unmatched `isnull` under `validate=false`, `AlwaysFalse`, unknown
  comparisons, strict validation, and file/ranged behavior all match the
  authority.
- Reject-window controls passed 10/10.
- The 266-byte, 1,000,000-row allocation probe is flat between 10,000 and
  1,000,000 declared rows: file 5,488 bytes, stream 1,792 bytes, and ranged
  18,880 bytes at both counts.
- Overflow and residual checks passed 18/18 on file and ranged sources.
  `limit=0`, `limit=typemax(Int)`, and offset-then-cap succeed. Unbounded scans,
  including `AlwaysTrue`, refuse with `ValidationError`. Returned residuals are
  empty, and downstream `Tables.finish` preserves the consumed result.
- The former `#arrowcount#` pathological-name mismatch is gone; its focused
  controls passed 17/17.
- Empty and nonempty list reads retain their descriptor and metadata on all
  three input paths. Nonempty rewrites pass 6/6.
- Built-in zero-field predicate evaluation follows the Tables.jl three-valued
  rules. The integer window arithmetic itself is overflow-safe.

## Assumptions and decisions

- I used `Tables.finish` and `Tables.bind` from the manifest-selected checkout
  as the semantic authority.
- I used each explicit record-batch length as the row-count authority for a
  zero-field source.
- I treated `ValidationError` or `AllocationLimitError` as an acceptable
  unbounded overflow refusal.
- I treated the documented allocation limit as cumulative across one scan,
  matching the existing column-bearing implementation.
- I treated block framing, version, body-length, and extent checks as mandatory
  for a ranged metadata-only read.
- I counted the empty-list writer failure because 12/12 rewrites are an
  explicit round-39 condition. I kept its round-38 MEDIUM severity.
- I rated malformed-input acceptance and reader-budget bypass HIGH. I rated the
  stale empty schema and unsupported extension-filter behavior MEDIUM.
- The host is 64-bit, so `Int` and `Int64` have the same range in the overflow
  probes.
- I made no product or test changes. I preserved the six pre-existing untracked
  files and added only this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 164/164, and every acceptance
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, zero verifier warnings, and the trimmed binary passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 skip.
- Focused zero-field matrix — exit 0; 57/57.
- Reject-window controls — exit 0; 10/10.
- Allocation-flatness probe — exit 0 with assertions; file 5,488 bytes,
  stream 1,792 bytes, and ranged 18,880 bytes at both declared counts.
- Overflow and residual probe — exit 0; 18/18.
- Pathological-name regression probe — exit 0; 17/17.
- Focused list matrix — exit 1; authority values/types 6/6, retention 6/6,
  nonempty rewrites 6/6, and empty rewrites 0/6.
- Ranged frame-integrity probe — exit 1 by assertion; the malformed frame was
  rejected by the full reader and accepted by the direct ranged scan.
- Cumulative-budget probes — exit 0 with assertions; both file and ranged
  budget resets reproduced.
- Empty-conversion and unsupported-predicate probe — exit 0 with assertions;
  all stated mismatches reproduced on file, stream, and ranged facades.
- `git diff --check` — exit 0.

VERDICT: FINDINGS
