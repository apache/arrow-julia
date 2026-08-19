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

# Arrow.jl 3.0 code review — round 60

Date: 2026-08-18

Scope: exact commit `10158a883de2d09fed8316fed2542d3d9961c953`
(`fix: resolve round 59 findings — per-package Julia floor in CI, wrapper
route prose`) on `core-rewrite`. Round 59 reviewed through
`95d24806a3efd1124cbd2871e18171db4032916b` and recorded two LOW findings
in `docs/dev/REVIEW-codex-r59.md`. I reviewed the complete three-commit
`95d2480..10158a8` delta:

- `e800c6f` — rename the ArrowStrings type family and related helpers;
- `d19e3c8` — record the round-59 review;
- `10158a8` — implement the two claimed round-59 closures.

All Tables-dependent checks used the clean `jq/scan` checkout at
`ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`.

## Result

Round 60 is not clean. Both round-59 findings are closed, and the
ArrowString rename preserves runtime behavior after the intentional public
API rename. I found one new LOW CI finding in the closure commit.

Passing the package directory to `setup-julia@v3` correctly makes the
ArrowStrings `min` cells use Julia 1.10. It also makes ArrowTypes use its
actual declared floor, Julia 1.0, not Julia 1.12 as stated in the requested
claim. The current `macos-latest` runner is ARM64, but Julia 1.0.5 has no
macOS ARM64 binary. Both ArrowTypes / `min` / macOS thread cells therefore
fail in `setup-julia` before tests start.

The repaired manual matches the route probe sentence by sentence. The
rename has no executable change beyond renamed bindings and dispatch
signatures. Its two strictly non-name edits are diagnostic terminology and
a test comment.

## Findings

### 1. LOW — the package-aware `min` selector makes two ArrowTypes macOS cells unresolvable

The regular matrix includes ArrowTypes, `min`, `macos-latest`, and thread
counts 1 and 2 at `.github/workflows/ci.yml:83-101`. The closure commit now
passes `project: ${{ matrix.pkg.dir }}` to `setup-julia@v3` at lines
103-108. That is the right repair for ArrowStrings, but it applies to every
package in the matrix.

The [setup-julia v3 contract](https://github.com/julia-actions/setup-julia/tree/v3)
says `project` selects the project used to resolve `min`. In v3, `min`
selects the earliest compatible major/minor line and the latest patch on
that line. The exact projects and current official `versions.json` resolve
as follows:

```text
Arrow.jl        Project.toml                  julia = "1.12"  -> 1.12.7
ArrowTypes.jl   src/ArrowTypes/Project.toml   julia = "1.0"   -> 1.0.5
ArrowStrings.jl src/ArrowStrings/Project.toml julia = "1.10"  -> 1.10.12
```

Thus the requested claim that ArrowTypes also resolves Julia 1.12 is false.
Its `julia = "1.0"` compat at `src/ArrowTypes/Project.toml:27-28` is
unchanged by this delta.

The current
[GitHub runner-image table](https://github.com/actions/runner-images/blob/main/README.md)
maps `macos-latest` to an ARM64 image. `setup-julia` defaults to the runner
architecture. The official Julia 1.0.5 file list has macOS x86_64 artifacts
only. I built the current v3 action at
`fa02766e078afaaf09b14210362cee14137e6a32` and invoked its real resolver and
file selector. Resolution produced the three versions above. Its Darwin
`getFileInfo(..., "1.0.5", "aarch64")` probe exited 1 with:

```text
Could not find aarch64/1.0.5 binaries
```

The same ARM64 file probe exited 0 for Julia 1.10.12 and 1.12.7. This is a
deterministic failure in the two ArrowTypes / `min` / `macos-latest` cells,
not only an incorrect description in the requested claim.

Disposition: open. Keep the package-aware `project` input. Exclude the
unsupported ArrowTypes floor cells on ARM macOS, or run that floor on an
Intel macOS runner. Do not raise ArrowTypes' Julia floor unless dropping
Julia 1.0 support is intentional.

## Round-59 closure checks

### Package-specific Julia floor — ArrowStrings closed; finding 1 records the new ArrowTypes regression

The workflow now selects each package's own project. ArrowStrings' named
`min` cells therefore resolve the current Julia 1.10 patch, not the root
Arrow.jl 1.12 line. This closes round-59 finding 1 and remains durable when
the `lts` alias moves.

The exact current resolutions are Arrow 1.12.7, ArrowStrings 1.10.12, and
ArrowTypes 1.0.5. Finding 1 above records the unsupported ARM combination
introduced for ArrowTypes. I did not run GitHub Actions. I verified the
input contract and failure with the action's real resolver and current
official release metadata.

### Composite and wrapper route prose — closed

The revised lead-in at `docs/src/manual.md:164-172` now separates composite
layouts from transparent wrappers and names both type-rule exceptions. An
independent 44-check probe exited 0:

```text
REE<Int64>: closed=true, routable=true,
  dynamic eltype=Any, batch eltype=Int64, Table eltype=Int64

Dictionary<Utf8>: closed=true, routable=true,
  dynamic eltype=Any, batch eltype=String, Table eltype=String

heterogeneous Union:
  zero=Any, Int-only=Int64, mixed=Any, all-missing=Missing
```

The statements match in order:

- list, struct, map, and union use the dynamic route;
- their declared public row type comes from the schema;
- the heterogeneous Union row explicitly permits row-based narrowing;
- the following paragraph covers undeclared physical nulls;
- REE and dictionary inherit the value child's route and element type;
- a closed scalar child keeps the typed path.

Round-59 finding 2 is closed. I found no new route or manual defect.

## ArrowString rename review

### Diff fidelity

`git diff 95d2480 e800c6f` changes nine files with 179 insertions and 179
deletions. A zero-context diff has 134 hunks. No file is added, deleted, or
renamed.

After applying the requested substitutions, all runtime source is
byte-identical except for one diagnostic string. Adding the local test
helper renames makes eight of the nine files byte-identical. The only
remaining file difference is a comment-only rewrite.

The strictly non-name edits are:

- `src/ArrowCore.jl:2782` changes the error text from `compact view
  payloads` to `view-entry payloads`;
- `test/core_tests.jl:733-736` changes the local encoder comment to say it
  models an ArrowStrings payload while Core cannot depend on that package.

The executable part of the second hunk only renames the local test type
`CompactPayload` to `ViewEntry`. The additional test-only helper names are
`csfrombytes` to `asfrombytes`, `csscratchbytes` to `asscratchbytes`,
`foldcshash` to `foldashash`, and `foldcscmp` to `foldascmp`. I found no
method-body, condition, constant, layout, or data-flow change.

The mechanical substitution leaves three article errors: `a ArrowString`
at `src/ArrowStrings/src/ArrowStrings.jl:47,387` and `A ArrowStringVector`
in a test comment at `test/facade_tests.jl:1048`. These are editorial nits,
not behavior or release-readiness findings.

### Stale-name scan

I used `git grep ... HEAD` over the tracked `src`, `test`, `docs`,
`conformance`, `.github/workflows`, and `README.md` trees.

- `CompactString` remains only in historical round-57 through round-59
  review records: 10 matches.
- `fromcompactviews` remains only in historical round-55 through round-59
  review records: 10 matches.
- `_cs_` has no match.
- The literal unbounded `cs[a-z]+\(` expression matches unrelated suffixes
  in `docsequal`, `execstream`, `atomicswap`, `cstring`, and `publicscan`,
  plus archived `foldcshash`. An identifier-bound scan and exact scans for
  `cslen`, `csbufidx`, `csoffset`, and `cspos` have no active match.

I preserved the historical review records because they document the API at
the commits they reviewed. Excluding those records, every stale-name scan
exited 1 with no match.

### Export and facade behavior

`src/ArrowStrings/src/ArrowStrings.jl:53` has one export statement:

```julia
export ArrowString, ArrowStringVector, ArrowStringPayload
```

Runtime checks confirmed all three types are defined and exported, and they
are the only explicit exports. The three old type names and four old payload
accessors are undefined. The four new payload accessors are defined.
`ArrowCore.fromviewentries` is exported and has both methods at
`src/ArrowCore.jl:2775-2807`; `fromcompactviews` is undefined. I treated the
absence of compatibility aliases as the requested API rename, not a defect.

An independent facade probe selected the exact
`_writecolumn(::String, ::ArrowStrings.ArrowStringVector)` method at
`src/write.jl:109-112` and passed 21/21, exit 0. It verified:

- the result is Utf8View;
- the payload vector and both variadic buffers remain the exact region roots;
- a payload length of -1 writes and reads back as `missing`;
- `Arrow.Table` reads the mixed column correctly;
- a nullable all-inline column uses zero variadic data buffers;
- that zero-buffer file has only validity and views buffers;
- its payload vector also remains the exact views-buffer root.

The standalone ArrowStrings suite and full Arrow suite exercise the renamed
types and methods. The rename is behavior-preserving relative to the new
public names.

## Three-commit delta review

- `e800c6f` contains the rename, the two non-behavioral text edits above,
  and no runtime behavior change.
- `d19e3c8` adds only the 281-line round-59 review record.
- `10158a8` changes one setup-julia input and the manual lead-in. The prose
  fix is accurate. The workflow fix closes ArrowStrings but introduces
  finding 1.
- The current workflow parses as YAML.
- I found no other correctness, safety, portability, performance, test, or
  documentation issue in the delta.

## Assumptions and decisions

- I treated the exact committed tree and each package's committed Julia
  compat as authoritative. Mutable manifests and probes stayed in detached
  scratch worktrees.
- I treated setup-julia and runner labels as time-sensitive. I verified the
  current v3 action, official Julia metadata, and official runner table on
  the review date.
- I classified two guaranteed setup failures as a LOW repository finding,
  not as a harmless mistake in the requested parenthetical.
- I evaluated behavior preservation after the intentional public API rename.
  I did not require old-name compatibility aliases.
- I treated prior review files as immutable historical evidence, not active
  API documentation.
- I disclosed but did not elevate the three indefinite-article grammar nits.
- I accepted the declared conformance skips. I treated Documenter's local
  deployment-skip warning as environmental only after a content-only build
  exited 0 with no warning or error.
- A supplementary local ArrowTypes Julia 1.0.5 run was stopped while its old
  Pkg cloned General. It exited 130 before tests. This does not affect the
  independently reproduced setup-julia ARM failure.
- The host was 64-bit ARM macOS with Julia 1.12.6, Julia 1.10.11, and Docker
  29.6.2.
- I did not modify product or test code in the main checkout. The protected
  untracked files `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`,
  `ISSUE-540.md`, `ISSUE-580.md`, and `mytestdata.arrow` remained untouched.
  This review document is the only main-checkout change made by this review.

## Validation

- A fresh detached root environment first resolved registry Tables 1.13.0.
  The exact root test command then exited 1 at the known `Tables.Scan`
  prerequisite. I developed the clean Tables checkout named above into the
  scratch environment. The required exact command
  `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` then
  exited 0: ArrowCore 420/420, facade 321/321, threaded caches 4/4, and IPC
  read, IPC write, C data, and ranged-scan acceptance 1/1 each.
- `julia --project=src/ArrowStrings --startup-file=no -e 'using Pkg;
  Pkg.test()'` — exit 0, 2,495/2,495 on Julia 1.12.6.
- The same ArrowStrings command under Julia 1.10.11 — exit 0, 2,495/2,495,
  with zero warnings and errors in the clean rerun.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0, 6/6;
  compile and executable passed with zero verifier warnings and errors.
- `julia --startup-file=no conformance/run.jl` — exit 0: corpus 275 pass / 0
  fail / 36 declared skips; PyArrow 25.0.1 and nanoarrow 0.9.0 oracle 170 /
  0 / 43; C data and stream oracle 143 / 0 / 9. The driver reported corpus,
  oracle, and cdata `PASS`.
- The required docs environment command developed exact Arrow and the clean
  Tables checkout, instantiated, and exited 0. `julia --project=docs
  docs/make.jl` exited 0. Its document build had no content warning or error;
  `deploydocs` emitted the expected local-environment skip warning. A second
  content-only invocation exited 0 with zero warnings and errors.
- Independent route probe — exit 0, 44/44. Independent rename/facade probe —
  exit 0, 21/21.
- The current setup-julia resolver probe exited 0 with Arrow 1.12.7,
  ArrowStrings 1.10.12, and ArrowTypes 1.0.5. Its expected unsupported
  ArrowTypes macOS ARM file probe exited 1 with the error quoted in finding
  1; the two supported ARM probes exited 0.
- `.github/workflows/ci.yml` parsed as YAML, exit 0.
- `git diff --check` and `git diff --check 95d2480..10158a8` exited 0. A
  separate whitespace and final-verdict check covers this untracked report.
- Final branch HEAD remained
  `10158a883de2d09fed8316fed2542d3d9961c953`.

VERDICT: FINDINGS
