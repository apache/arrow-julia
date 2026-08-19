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

# Arrow.jl 3.0 code review — round 66

Date: 2026-08-19

Scope: exact commit `415d0c798d492b866d0c23a7a12dba8fe473a253`
(`fix: resolve round 65 findings — Integer length guard, policy-compliant
worker pool, no GCS claim`) on `core-rewrite`. Local `HEAD`, local
`core-rewrite`, local `origin/core-rewrite`, and the live `origin`
`refs/heads/core-rewrite` all resolved to that SHA. Round 65 reviewed
`d3ea7a7cf16001985a0237e18c9c873307acbc6c`. This round reviewed both
commits in `d3ea7a7..415d0c7`:

- `8ee33ba` — record the round-65 review;
- `415d0c7` — close its three LOW findings.

I read both commits and the complete two-commit delta before testing it.

## Result

All three round-65 findings are closed. I found no new finding in the
two-commit delta. The required package, Julia 1.10.11, Minio, trim,
documentation, formatter, and whitespace gates pass.

## Round-65 closure

### Source length type validation

`SourceFile` now checks `reported isa Integer` before either the sign check or
the `Int64` range check (`src/scan.jl:784-786`). A non-`Integer` therefore
cannot reach comparison or conversion. `_BadLengthSource.reported` is now
`Any` (`test/facade_tests.jl:95-99`). The facade suite tests `Float64`,
`String`, and `nothing` results through `Arrow.Table`
(`test/facade_tests.jl:285-295`).

The exact public-path probe produced:

```text
sourcelength = 3402.0  => ValidationError
sourcelength = 1.0     => ValidationError
sourcelength = 1.5     => ValidationError
sourcelength = "3402"  => ValidationError
sourcelength = nothing => ValidationError
```

Each value produced `Arrow.ArrowCore.ValidationError` with the invalid-length
message. No value produced `InexactError`, `MethodError`, or a decoded table.

### Policy-compliant range-read worker pool

The implementation at `src/scan.jl:909-955` follows the applicable repository
rules:

- `_SpanQueue` is a mutable struct with `@atomic next::Int`;
- no `Threads.Atomic` or `Atomic{...}` remains in `src/scan.jl`;
- every worker is
  `errormonitor(Threads.@spawn _readworker!(results, sf, spans, queue))`
  inside `@sync`;
- the worker loop is the separate `_readworker!` function and exits with an
  explicit `return nothing`;
- `_readspans` and `_firstcause` use explicit returns;
- the changed functions and type definitions have one blank line between
  them and no trailing whitespace.

A 600,922-byte public `Arrow.Table` scan used a source with
`concurrentreads(src) == 4`. Peak in-flight reads were two. The two concurrent
metadata and body waves each completed request 2 before request 1. The
decoded result still remained in file order as `x == [1, 2, 3, 4]`. The same
14/14 probe passed with four Julia threads and with one Julia thread. The
sleeping `readrange` methods yielded on the one-thread run, which is the
relevant model for remote I/O.

A worker-injected `ValidationError` surfaced from `Arrow.Table` as the
original `ValidationError`, not as `TaskFailedException` or
`CompositeException`. `_firstcause` therefore still restores the public
exception through `@sync`.

`errormonitor` also printed one `UNHANDLED TASK ERROR` diagnostic for that
injected failure. I accept this as the direct effect of the repository's
unconditional spawned-task monitoring rule. It does not change the exception
returned by `Arrow.Table`, and the public API does not promise silent stderr.
If silent handling becomes a requirement, the repository should record the
narrow enclosing-`@sync` exception that its guidelines allow and then omit
`errormonitor` at this site. The current code must not silently diverge from
the current rule.

### CloudStore provider claims

The README, manual, extension header, and `AbstractArrowSource` docstring now
claim only S3 and Azure Blob Storage support. The exact command

```text
grep -rn --exclude='REVIEW-codex-*.md' --exclude-dir=.git GCS .
```

exited 1 with empty output. Broader searches for `Google Cloud Storage`,
`Google Cloud`, and `GCP` were also empty outside historical review records.

## Delta audit

`8ee33ba` adds only `docs/dev/REVIEW-codex-r65.md`. `415d0c7` changes only the
six stated closure files: `README.md`, `docs/src/manual.md`,
`ext/ArrowCloudStoreExt.jl`, `src/scan.jl`, `src/source.jl`, and
`test/facade_tests.jl`.

The source-length guard short-circuits safely. The worker counter assigns one
index to each request. Workers write separate result slots. `@sync` waits for
all workers before the result vector is used. The original worker exception
is preserved. The provider edits remove the unsupported claim. I found no
new correctness, concurrency, compatibility, documentation, or formatting
defect in the two-commit delta.

## Assumptions and decisions

- I treated this as a closing review of the two-commit delta and the three
  round-65 findings. I did not reopen unchanged, previously reviewed code.
- I treated sleeping `readrange` methods as a valid model of yieldable remote
  I/O for the one-thread worker-pool probe.
- I accepted the duplicate `errormonitor` diagnostic under the current
  mandatory monitoring rule. A silent implementation needs a documented
  narrow policy exception first.
- I accepted Documenter's local deployment-skip warning after doctests,
  cross-references, document checks, and HTML rendering completed.
- I did not run the conformance container because this round does not touch
  the IPC or C-data paths and the review request excludes that gate.
- I did not modify product or test code. All probes and environment changes
  stayed in detached or temporary worktrees. The protected untracked files
  `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`, `ISSUE-540.md`,
  `ISSUE-580.md`, and `mytestdata.arrow` remained untouched. This review file
  is the only active-checkout change made by this round.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0
  on Julia 1.12.6 in `/private/tmp/arrow-r66-main` after developing
  `/Users/jacob.quinn/.julia/dev/Tables`: ArrowCore 421/421, facade 342/342,
  CloudStore/Minio 17/17, and every IPC, C-data, statistics, scan, and
  byte-range battery passed.
- The required Julia 1.10.11 binary, from fresh environment
  `/private/tmp/arrow-r66-j110-env.T0CBNG` that developed exact Arrow,
  `src/ArrowStrings`, and `/Users/jacob.quinn/.julia/dev/Tables`:
  `Pkg.test("Arrow")` — exit 0 with ArrowCore 421/421, facade 342/342,
  CloudStore/Minio 17/17, and all batteries passing.
- Both package gates printed one non-fatal deprecation warning for verbosity
  logging macros. Neither printed a test failure.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0, 6/6. Its
  assertions require zero verifier errors, zero verifier warnings, compiler
  exit 0, and trimmed-executable exit 0.
- Documentation setup developed exact Arrow and the required Tables checkout.
  `julia --project=docs --startup-file=no docs/make.jl` — exit 0. Doctests,
  cross-references, document checks, and HTML rendering passed. The only
  warning was the expected local deployment skip.
- JuliaFormatter 2.12.5 `format("."; verbose=true)` — exit 0 and no tracked
  change in `/private/tmp/arrow-r66-format`.
- The focused source probe — exit 0, 14/14 with four threads and exit 0,
  14/14 with one thread. It covers all five non-`Integer` values, bounded
  in-flight reads, out-of-order completion, indexed results, and original
  worker exception type.
- `git show --check` for both commits, `git diff --check d3ea7a7..HEAD`, the
  final active `git diff --check`, and detached-worktree checks passed. A
  no-index whitespace check of this untracked review file produced no
  diagnostic; its expected nonzero status only records that it differs from
  `/dev/null`.
- Final local and live remote refs remained
  `415d0c798d492b866d0c23a7a12dba8fe473a253`. The active checkout contained
  only the six protected untracked files and this review document.

VERDICT: CLEAN
