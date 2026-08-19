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

# Arrow.jl 3.0 code review — round 53

Date: 2026-08-17

Scope: exact commit `e0d2ce22daf879be19b85960c78169e75a4abb0c`
(`test: pin Float24 refusal and Dictionary-wrapped union routing`) on
`core-rewrite`. Its parent is
`3ec81f389e3fada9b16907d12d06dff65729058c`, which records the round-52
review of product commit
`f8c334e9692fd7ec6ffa429492ca1fc3eb36bdf1`. I reviewed the two-commit
`f8c334e..e0d2ce2` delta, ran both new pins and focused mutations, reran the
round-51/52 Core and facade matrices, ran the complete three-leg benchmark
harness, and ran every required gate at the exact HEAD.

The manifest-selected Tables.jl development dependency was the clean
`jq/scan` checkout at
`d1fbb6eb577741688dba70039754166b51c1cdcc`.

## Result

Round 53 is not clean. I found one LOW test-coverage issue.

The asserted suite counts are correct: ArrowCore reports 391/391 and the
facade reports 272/272. The Float24 fixture reaches both typed entry points,
and the Dictionary-wrapped union assertion is present beside the REE
assertion. Current product behavior is correct. The Dictionary pin catches a
Dictionary-routing regression, and the Float24 scalar pin catches removal of
the element-width guard.

The Float24 materialize assertion does not catch removal of the independent
bulk-width gate. Its six-byte fixture causes the regressed bulk path to throw
the expected exception type from buffer bounds before a wrong-width copy can
occur. A padded fixture reproduces the round-51 wrong copy while the complete
tracked Core suite remains green under that mutation. Thus, one of the two
round-52 coverage findings is not fully closed.

The two-commit delta contains only the round-52 review document and the two
test additions. I found no product, benchmark, conformance, project, or
manifest change and no other finding of any severity.

## Findings

1. **LOW — the Float24 materialize assertion can pass after the bulk-width
   gate regresses.**

   The new fixture creates two Float24 values with the descriptor's natural
   six-byte data buffer at `test/core_tests.jl:1436-1438`. It requires typed
   `materialize` and typed `getvalue` to throw `ArgumentError` at
   `test/core_tests.jl:1439-1440`.

   Typed materialization tries `_bulkmaterialize` before the element loop at
   `src/ArrowCore.jl:2529-2535`. The bulk gate correctly requires the Julia
   claim width to equal the descriptor width at
   `src/ArrowCore.jl:2551-2563`. If only that gate is removed, the Float64
   claim selects an eight-byte width and requests a 16-byte source slice at
   `src/ArrowCore.jl:2564-2568`. `subslice` then throws `ArgumentError`
   because the tracked buffer has only six bytes at
   `src/ArrowCore.jl:325-332`. The materialize assertion therefore passes
   for the wrong reason.

   A scratch mutation changed only
   `primwidth(t) == w || return nothing` to `true || return nothing`. The
   complete tracked Core file still passed 391/391. The tracked Float24
   fixture passed 2/2: materialize threw the buffer-bounds `ArgumentError`,
   while getvalue threw the intended claim-refusal `ArgumentError`.

   With the same invalid descriptor and a padded 16-byte buffer containing
   the bytes of `Float64[1.25, -3.5]`, the mutation reproduced the exact
   round-51 finding:

   ```text
   typed_materialize=[1.25, -3.5]
   dynamic_materialize=Any[Float16(0.0), Float16(0.0)]
   ```

   Typed getvalue still refused through the separate element-width guard at
   `src/ArrowCore.jl:2402-2416`. The tracked materialize fixture needs enough
   backing bytes for a wrong eight-byte copy to succeed; then removal of the
   bulk gate will fail the test instead of producing the expected exception
   from an unrelated bounds check.

## Closure of the round-52 findings

- **Float24 tracked refusal:** partially closed. The new construction and two
  assertions exist at `test/core_tests.jl:1436-1440`, run in ArrowCore, and
  account for the count increase from 389 to 391. Removing the scalar
  invalid-width guard made both new Float24 assertions fail with `TypeError`
  instead of `ArgumentError`. Removing only the bulk-width gate left
  391/391 green, so the materialize half does not durably pin the bulk-copy
  refusal.
- **Dictionary-wrapped union routing:** closed. The new assertion at
  `test/facade_tests.jl:930-933` complements the REE assertion at
  `test/facade_tests.jl:925-929` and accounts for the increase from 271 to
  272. `_typedroutable` unwraps Dictionary and REE fields at
  `src/table.jl:233-240`, and `_batchcolumn` consumes that result at
  `src/table.jl:250-253`. A scratch mutation that disabled only Dictionary
  recursion made the new assertion fail while the REE assertion stayed
  green: 271 pass / 1 fail / 272 total.

## Round-51/52 clean regression surface

- The unvalidated Core probe passed the 324/324 valid matrix over all 27
  fixed-width descriptors. It also passed cached-bitmap authority 4/4,
  hostile validity geometry 5/5, invalid Int/Float widths 22/22, invalid Time
  combinations 15/15, nonzero offsets 7/7, Bool exclusion 4/4, and
  Decimal32/64 null punching 10/10.
- The facade public-path matrix passed 346/346. NullType, direct homogeneous
  unions, Dictionary<Union>, REE<Union>, file, stream, IO, and scan paths
  returned the expected values. The nested Dictionary/REE union matrix
  passed 38/38 across both wrapper orders and deeper combinations.
- The complete benchmark driver exited 0 with rewrite, registered Arrow
  2.8.1, and PyArrow. Each JSONL leg contained all ten expected records. The
  runtime read-semantics warning and complete ten-row report printed.
- The three current dictpool outputs each declared a DictionaryType field,
  contained 2,000,000 rows and 32 logical values, and matched the workload
  formula: 12/12 physical and logical assertions. The 2.x provenance check
  reported version 2.8.1, registry tracking, and neither path nor repository
  tracking.
- The harness source is unchanged in the two-commit delta. Dictionary pool
  construction remains inside all write timings at
  `bench/workloads.jl:53-58` and `bench/bench_pyarrow.py:91-104`; the 2.8.1
  pin and setup remain at `bench/env2x/Project.toml:5-6` and
  `bench/run.jl:47-52`; missing-Docker handling remains at
  `bench/run.jl:65-80`; completeness checks remain at
  `bench/run.jl:84-102`; and the read warning remains at
  `bench/run.jl:104-109`.

## Assumptions and decisions

- I treated direct unvalidated `ArrayData`, including an oversized backing
  buffer, as in scope. Round 51 used that exact state to expose the wrong
  bulk copy, and the implementation states that the typed bulk path serves
  unvalidated data at `src/ArrowCore.jl:2565-2566`.
- I required a regression pin for typed materialize to fail if either the
  bulk-width guard or the scalar-width guard is removed. Both guards enforce
  the same public refusal through independent control-flow paths.
- I rated the finding LOW because the product has both guards today and
  staged validation rejects Float24. This is a durable-coverage gap, not a
  current wrong-value or unsafe-copy defect.
- I accepted the corpus and oracle's declared skips as the established
  baseline. The host was 64-bit arm64 macOS with Julia 1.12.6. The oracle
  used PyArrow 20.0.0 and nanoarrow 0.9.0.
- The live ignored env2x manifest printed Pkg's stale-project warning during
  setup. The harness still exited 0 and loaded registered Arrow 2.8.1. I did
  not treat local ignored environment state as a tracked finding.
- I did not modify product or test code. All mutations and generated
  benchmark data stayed in scratch locations. I moved the 861 MB benchmark
  scratch directory to the macOS Trash after verification, so it remains
  recoverable. The six protected untracked files remained present and
  untouched. This review document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  671 reported assertions: ArrowCore 391/391, threaded caches 4/4, facade
  272/272, and each IPC read, IPC write, C Data, and ranged-scan acceptance
  battery 1/1.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6,
  compile plus run passed, with zero verifier errors and zero verifier
  warnings.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with PyArrow 20.0.0 and
  nanoarrow 0.9.0.
- `git diff --check` — exit 0. `git diff --check HEAD^^ HEAD` also exits 0
  for the exact two-commit delta.
- Direct tracked files — both exit 0:
  `julia --project=. --startup-file=no test/core_tests.jl` reported
  ArrowCore 391/391 plus threaded caches 4/4, and
  `julia --project=. --startup-file=no test/facade_tests.jl` reported facade
  272/272.
- Bulk-gate mutation — exit 0 for both the tracked Float24 fixture (2/2) and
  the full mutated Core file (391/391). The padded-buffer diagnostic exited
  0 and reproduced the wrong typed copy shown in the finding.
- Scalar-gate mutation — exit 1 as required; both new Float24 assertions
  failed with `TypeError` instead of the expected `ArgumentError`.
- Dictionary-routing mutation — exit 1 as required; only the new Dictionary
  assertion failed, for 271 pass / 1 fail / 272 total.
- Core scratch probe — exit 0 with the exact matrix counts listed above:
  `julia --project=/Users/jacob.quinn/.julia/dev/Arrow --startup-file=no
  /tmp/arrow-r52-core.vKSRMv/core_probe.jl` after changing only its scratch
  HEAD pin to `e0d2ce2` and restoring it afterward.
- Facade scratch probes — both exit 0:
  `julia --project=. --startup-file=no
  /tmp/arrow-r52-facade.32gIvL/facade_matrix.jl` reported 346/346, and
  `julia --project=. --startup-file=no
  /tmp/arrow-r52-facade.32gIvL/deep_wrapper_matrix.jl` reported 38/38.
- Full three-leg benchmark — exit 0:
  `julia --project=. --startup-file=no bench/run.jl
  /tmp/arrow-r53-bench.jWsEoN/out`. Rewrite, Arrow 2.8.1, and PyArrow each
  emitted 10 records; the report warning and all ten result rows printed.
- Dictpool output probe — exit 0, 12/12. All three generated files had a
  dictionary field, 2,000,000 rows, a 32-value pool, and exact logical
  values.
- Final HEAD remained
  `e0d2ce22daf879be19b85960c78169e75a4abb0c`. Repository status contained
  only the six protected untracked files plus this review document.

VERDICT: FINDINGS
