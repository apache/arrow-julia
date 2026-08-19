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

# Arrow.jl 3.0 code review — round 52

Date: 2026-08-17

Scope: exact commit `f8c334e9692fd7ec6ffa429492ca1fc3eb36bdf1`
(`fix: resolve round 51 findings — bitmap authority, routing, fair legs`) on
`core-rewrite`. Its parent is
`728e0eda671aa646f3be12f529a30b81dd168310`, which records the round-51
review of final batch commit `05b8f75938ac17bd514c5bac1533ba4dc4606547`.
I reviewed only the one-commit round-51 fix and reran the full package, trim,
corpus, oracle, focused Core/facade, allocation, and benchmark surfaces at
the exact fix commit.

The manifest-selected Tables.jl development dependency was the clean
`jq/scan` checkout at
`d1fbb6eb577741688dba70039754166b51c1cdcc`.

## Result

Round 52 is not clean. I found two LOW test-coverage issues.

The implementation closes all nine round-51 behavior findings. Cached null
counts no longer override validity bitmaps. Hostile validity geometry stays
bounds-safe. Valid fixed-width values retain bulk extraction, while invalid
Int24 and Float24 values refuse on typed reads. NullType and every tested
direct or wrapped homogeneous union use the Core dynamic route. The complete
benchmark harness now uses dictionary encoding in all three legs, pins and
instantiates registered Arrow 2.8.1, prints the read warning, rejects
incomplete or duplicate output, and skips cleanly without Docker.

The committed regression counts do not match the claimed pins. The Core
suite reports 389, not 390, because it has no Float24 assertion. The facade
suite reports 271, not 272, because its wrapper assertion covers REE but not
Dictionary. Focused scratch probes confirm that both omitted product cases
work at this commit. These are missing durable guards, not current product
failures.

## Findings

1. **LOW — the Float24 managed-refusal fix has no tracked regression pin.**

   The fix is generic over `IntType` and `FloatType` at
   `src/ArrowCore.jl:2402-2416`, and the bulk gate separately requires the
   descriptor width to equal the Julia element width at
   `src/ArrowCore.jl:2551-2563`. The new test block describes invalid widths
   in the plural, but it constructs only `IntType(24, true)` at
   `test/core_tests.jl:1429-1435`. No tracked test constructs
   `FloatType(24)`.

   The real package run reports ArrowCore 389/389 instead of the claimed
   390. A scratch Float24 probe confirms that both typed `getvalue` and typed
   `materialize` throw managed `ArgumentError` for non-null values. Empty and
   all-null Float24 data retain dynamic/typed parity without a raw load.
   Thus, the code closes the round-51 failure, but a future Float-specific
   regression can pass the repository suite.

2. **LOW — Dictionary-wrapped homogeneous-union routing has no tracked
   regression pin.**

   `_typedroutable` correctly walks Dictionary and REE wrappers and rejects
   a union below either one at `src/table.jl:233-240`. The regression test
   comment says that Dictionary and REE wrappers route dynamically at
   `test/facade_tests.jl:925`, but the only assertion at
   `test/facade_tests.jl:926-929` constructs an REE wrapper. It does not
   construct a Dictionary wrapper.

   The real facade run reports 271/271 instead of the claimed 272. Scratch
   IPC tests confirm correct Dictionary<Union>, Dictionary<REE<Union>>, and
   REE<Dictionary<Union>> routing across Table, Stream, file, IO, and scan
   paths. The product behavior is correct, but one exact round-51 HIGH shape
   has no repository regression guard.

## Round-51 closure evidence

- **Findings 3 and 4, Core bulk extraction:** the validity loops at
  `src/ArrowCore.jl:2576-2593` use `isvalid_at`, never `nullcount`. The
  bitmap lookup applies the logical offset at `src/ArrowCore.jl:742-769`,
  while the data copy applies `d.offset * width` at
  `src/ArrowCore.jl:2565-2568`. Cached-zero nullable reads preserved exact
  missing placement. Missing-free reads refused the first null. The
  nine-row/one-byte validity case raised `BoundsError` on dynamic and typed
  paths. Int24 and Float24 non-null typed reads raised `ArgumentError` on
  both scalar and materialized routes.
- **Findings 1 and 2, facade routing:** `_closedclaim` rejects `Missing` and
  bottom at `src/table.jl:218-227`. `_batchcolumn` requires both a closed
  claim and a routable descriptor at `src/table.jl:250-253`. NullType,
  direct homogeneous Union, Dictionary<Union>, REE<Union>, and both nested
  wrapper orders reached `AC.materialize(f, d)` and produced the expected
  values on all tested public paths.
- **Finding 5, dictpool fairness:** both Julia legs build
  `Arrow.DictEncode` inputs at `bench/workloads.jl:53-58`, with pool
  construction occurring during the timed `Arrow.write`. PyArrow begins
  with plain strings and calls `dictionary_encode()` inside its write timer
  at `bench/bench_pyarrow.py:91-104`. The three full-size output files all
  declare dictionary-encoded String fields with 32-entry pools and equal
  logical values.
- **Finding 6, 2.x setup:** `bench/env2x/Project.toml:5-6` pins
  `Arrow = "=2.8.1"`, and `bench/run.jl:47-52` instantiates it before any
  leg. An empty-depot provenance check loaded registered Arrow 2.8.1 with
  registry tracking true and path/repository tracking false.
- **Finding 7, read semantics:** the runtime report prints the warning at
  `bench/run.jl:104-109`. The 2.x source description at
  `bench/bench_2x.jl:17-21` states that only the top-level column is copied
  and nested list values remain Arrow-backed views.
- **Finding 8, collector completeness:** `bench/run.jl:84-102` rejects a
  duplicate key before insertion and checks every expected implementation,
  workload, and operation key before printing. Exact collector probes
  refused both a missing final key and a duplicate first key.
- **Finding 9, missing Docker:** `bench/run.jl:65-80` first uses
  `Sys.which`, then catches inspection failures. A PATH-scrubbed production
  driver run printed the clean PyArrow skip and a complete two-leg report.
- **Adversarial and performance checks:** the valid fixed-width matrix stayed
  324/324. Nonzero offsets crossing a bitmap-byte boundary kept data and
  validity aligned. Bool stayed outside the bulk overload. Decimal32/64
  punched cached-zero nulls correctly. The fresh-process allocation pin
  remained 8,005,696 bytes for both 100,000-row tracked workloads, below the
  12,000,000-byte no-boxing bound. A closed facade Int64 route allocated
  819,312 bytes for 100,000 rows.

## Assumptions and decisions

- I treated direct unvalidated `ArrayData` as in scope. For empty and
  all-null invalid descriptors, I accepted dynamic/typed parity without a
  raw load, as the prompt permits parity or a managed refusal. For non-null
  Int24 and Float24 data, I required managed typed refusals.
- I rated the two absent regression pins LOW because focused tests prove the
  implementation correct today. I still treated them as findings because
  the stated 390 and 272 pins are false and two exact prior-finding shapes
  can regress without failing the repository suite.
- The full harness used each writer's native dictionary index width. Rewrite
  and PyArrow selected Int32; Arrow 2.8.1 selected Int8. I accepted this as
  like-for-like because all three inputs require pool construction inside
  the timer and all three output fields are dictionary encoded. An index
  width selected by the writer is part of the implementation result.
- A literal empty-depot run of the 3.x leg is not feasible on this branch.
  Registered Tables 1.13 does not yet provide `Tables.Scan`. This limitation
  predates the benchmark batch and the fix commit; `docs/dev/core-README.md:57`
  documents the required unreleased Tables branch. I copied the active clean
  development Manifest only into the archived scratch checkout and then ran
  the full harness from the empty scratch depot. I separately proved that
  the new env2x instantiate step works from that empty depot and resolves
  registered Arrow 2.8.1.
- I accepted the corpus and oracle's declared skips as the established
  baseline. The host was 64-bit arm64 macOS with Julia 1.12.6. The oracle
  used PyArrow 20.0.0 and nanoarrow 0.9.0.
- I did not modify product or test code. All focused probe sources, output
  files, and scratch depots are outside the repository. The six protected
  untracked files remained present and untouched. This review document is
  the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  668 reported assertions: ArrowCore 389/389, threaded caches 4/4, facade
  271/271, and each IPC read, IPC write, C Data, and ranged-scan acceptance
  battery 1/1. The two suite totals are the evidence for both findings.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6,
  compile plus run passed, with zero verifier errors and zero verifier
  warnings.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with PyArrow 20.0.0 and
  nanoarrow 0.9.0.
- `git diff --check` — exit 0. `git diff --check HEAD^ HEAD` also exits 0 for
  the exact fix commit.
- Core scratch probe — exit 0; 324/324 descriptor assertions, 4/4 cached
  bitmap cases, 5/5 hostile-validity cases, 22/22 invalid Int/Float cases,
  15/15 invalid-Time cases, 7/7 offset cases, 4/4 Bool cases, and 10/10
  Decimal cases. Command:
  `julia --project=/Users/jacob.quinn/.julia/dev/Arrow --startup-file=no /tmp/arrow-r52-core.vKSRMv/core_probe.jl`.
- Facade public-path matrix — exit 0, 346/346. Deep nested-wrapper matrix —
  exit 0, 38/38. Commands:
  `julia --project=. --startup-file=no /tmp/arrow-r52-facade.32gIvL/facade_matrix.jl`
  and
  `julia --project=. --startup-file=no /tmp/arrow-r52-facade.32gIvL/deep_wrapper_matrix.jl`.
- Fresh-process allocation commands — both exit 0. The tracked child reported
  8,005,696 bytes for each workload; the independent facade child reported
  819,312 bytes:
  `julia --project=. --startup-file=no test/typed_alloc_child.jl` and
  `julia --project=. --startup-file=no /tmp/arrow-r52-facade.32gIvL/facade_alloc_child.jl`.
- Full three-leg benchmark — exit 0 from the archived exact source with the
  active scan-enabled Manifest and empty scratch depot. Each JSONL file had
  10 records. The warning and complete ten-row report printed. Command:
  `JULIA_DEPOT_PATH=/tmp/arrow-r52-bench.g9TJPs/depot julia --project=. --startup-file=no bench/run.jl /tmp/arrow-r52-bench.g9TJPs/out`.
- Dictpool physical probe — exit 0; rewrite, Arrow 2.8.1, and PyArrow each
  produced a 2,000,000-row dictionary-encoded String field with a 32-value
  pool. All decoded logical values matched.
- Arrow 2.x provenance probe — exit 0; version 2.8.1, registry tracking true,
  path tracking false, repository tracking false, and source under the empty
  scratch depot.
- Collector probes — each exit 0 after matching the required refusal. The
  partial output raised `missing benchmark record for
  ("rewrite", "dictpool", "read")`; the duplicate raised `duplicate
  benchmark record for ("rewrite", "primitive", "write")`.
- PATH-scrubbed production-driver probe — exit 0 with
  `PATH=/usr/bin:/bin`; `Sys.which("docker")` was `nothing`, the PyArrow leg
  skipped, and the complete two-leg report printed.
- Final HEAD remained
  `f8c334e9692fd7ec6ffa429492ca1fc3eb36bdf1`. Repository status contained
  only the six protected untracked files plus this review document.

VERDICT: FINDINGS
