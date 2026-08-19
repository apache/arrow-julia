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

# Arrow.jl 3.0 code review — round 61

Date: 2026-08-18

Scope: exact commit `69ef8aa7fcf316288f3349e27ec4182ec5f15e5e`
(`build: Julia 1.10 floor for Arrow.jl and ArrowTypes.jl`) on
`core-rewrite`. Round 60 reviewed through
`10158a883de2d09fed8316fed2542d3d9961c953` and recorded one LOW CI
finding. I reviewed the complete three-commit `10158a8..69ef8aa` delta:

- `286fc76` — add the round-60 review record;
- `8b2103f` — temporarily exclude ArrowTypes from the `min` cells;
- `69ef8aa` — supersede that exclusion, set the repository Julia floor to
  1.10, repair the mmap carrier, share layout-role constants, reorder CI
  setup, and document the trim boundary.

All Tables-dependent local checks used the clean `jq/scan` checkout at
`ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`.

## Result

Round 61 is clean. The round-60 LOW finding is closed. I found no issue of
any severity in the three-commit delta or in the Julia 1.10 compatibility
surface reviewed here.

All three packages parse, precompile, load, and pass their suites on Julia
1.10.11. The full Arrow facade, scan, C-data, read, and write source loads
without an overwrite or deprecation warning. The version-gated mmap carrier
matches the Mmap stdlib on both sides of Julia 1.11. The shared layout-role
vectors have no mutation or identity-dependent consumer. The fresh-process
allocation pin passes on Julia 1.10.11 and 1.12.6. The Julia 1.12 trim gate
still compiles and runs with zero verifier errors and warnings.

The CI matrix is resolvable and buildable for `min`, `lts`, `1`, and `pre`.
The root Arrow test cells still cannot obtain the unreleased Tables
`jq/scan` branch. That condition predates this delta and is not worsened by
it.

## Findings

No findings of any severity.

## Round-60 closure

Arrow.jl now declares `julia = "1.10"` at `Project.toml:40-48`.
ArrowTypes declares the same floor at
`src/ArrowTypes/Project.toml:27-28`. ArrowStrings already declared it at
`src/ArrowStrings/Project.toml:23-24`.

The current `setup-julia@v3` tag is exact action commit
`fa02766e078afaaf09b14210362cee14137e6a32`. Its real resolver and the
current official Julia metadata select:

| Selector | Arrow.jl | ArrowTypes.jl | ArrowStrings.jl |
|---|---:|---:|---:|
| `min` | 1.10.12 | 1.10.12 | 1.10.12 |
| `lts` | 1.10.12 | 1.10.12 | 1.10.12 |
| `1` | 1.12.7 | 1.12.7 | 1.12.7 |
| `pre` | 1.13.0-rc3 | 1.13.0-rc3 | 1.13.0-rc3 |

The action reads the supplied project for `min` and selects the latest
patch of the earliest compatible minor line. The official metadata has
Linux x86_64 and macOS aarch64 artifacts for every selected release. See
the exact [setup input logic](https://github.com/julia-actions/setup-julia/blob/fa02766e078afaaf09b14210362cee14137e6a32/src/setup-julia.ts#L89-L99),
[selector logic](https://github.com/julia-actions/setup-julia/blob/fa02766e078afaaf09b14210362cee14137e6a32/src/installer.ts#L170-L203),
[Julia release metadata](https://julialang-s3.julialang.org/bin/versions.json),
and [runner image table](https://github.com/actions/runner-images/blob/main/README.md).

Thus, each ArrowTypes `min` cell now starts on Julia 1.10.12. That release
has a native macOS ARM64 binary, and its Pkg supports `test/Project.toml`.
This removes both causes behind round 60. Commit `8b2103f` is fully
superseded at HEAD, and the temporary broad exclusion is gone from the
matrix at `.github/workflows/ci.yml:83-101`.

## Julia 1.10 source and load audit

I scanned every Julia source under `src/`. This includes Arrow, ArrowCore,
the vendored FlatBuffers runtime, generated metadata, the facade, scan,
C data, read, write, ArrowTypes, and ArrowStrings.

- `Memory{UInt8}` and `.ref.mem` occur only in the static Julia 1.11+
  branch at `src/ArrowCore.jl:251-256`.
- `Base.hash_bytes` and `Base.HASH_SECRET` occur only behind the
  `isdefined` gate at `src/ArrowStrings/src/ArrowStrings.jl:323-325`.
  Julia 1.10 uses the `memhash` fallback at lines 326-330.
- There is no executable `public`, `ScopedValue`, `OncePerProcess`,
  `OncePerThread`, `Base.Lockable`, `Base.Fix`, `MemoryRef`,
  `GenericMemory`, `Iterators.cycle`, or `@main` use.
- The atomic forms are valid on Julia 1.10. The suites execute the release,
  pull-claim, cache, and C-data atomic paths.
- The `--trim` command occurs only in the explicit Julia 1.12 gate. The
  guard is `_TRIM_SUPPORTED = VERSION >= v"1.12.0-rc1"` at
  `test/trim_compile_tests.jl:31`.

A source parser and binding comparison found no post-1.10 syntax head. A
Base-qualified binding audit found only `hash_bytes` and `HASH_SECRET`
absent on 1.10, and both are guarded as stated above. A direct stdlib audit
confirmed the other used Base and Dates internals on 1.10.11.

A fresh isolated depot precompiled Arrow, ArrowTypes, and ArrowStrings with
`--warn-overwrite=yes --depwarn=yes`. A second fresh process loaded Arrow,
`Arrow.ArrowCore`, `Arrow.FlatBuffers`, `Arrow.Meta`, ArrowTypes, and
ArrowStrings. Both exited 0. Their logs contain no overwrite, deprecation,
or other warning. I did not use `--depwarn=error`.

A broad lowered-code audit reported one undefined name in the dormant
vendored helper `FlatBuffers.getvalue(::Type{<:Enum})` at
`src/FlatBuffers/table.jl:66`. No repository path calls that helper; the
generated metadata calls `FlatBuffers.get`, and the line is unchanged since
2023. It fails independently of Julia 1.10 and is absent from this delta. I
did not classify it as a finding in this compatibility and delta round.

## Mmap release and facade lifetime

The installed Julia stdlib sources confirm the carrier split. Julia 1.10.11
creates `A` at `stdlib/v1.10/Mmap/src/Mmap.jl:260` and registers
`finalizer(A)` at line 261. Julia 1.12.6 creates `A` at
`stdlib/v1.12/Mmap/src/Mmap.jl:254` and registers
`finalizer(A.ref.mem)` at line 255.

HEAD matches those implementations at `src/ArrowCore.jl:247-263`.
`_mmaproot` returns the backing `Memory` on Julia 1.11+ and the array on
Julia 1.10. `_release_mmap` asserts the same carrier type.
`mmapregion` passes that carrier into `ReleaseCell` at lines 277-289.

The tracked observer at `test/core_tests.jl:305-319` passed on both tested
Julia versions. A separate Julia 1.10 Table probe used the default mapped
file path. It proved all of these facts:

- `_mmaproot(region.root) === region.root::Vector{UInt8}`;
- `Arrow.close!(table)` ran an observer finalizer attached to that carrier;
- later raw region loads threw `InvalidStateException`;
- copied integer and nullable string columns, row values, and row count
  remained readable after close;
- a second close was inert.

The tracked facade checks at `test/facade_tests.jl:68-72` and `:146-154`
also verify that materialized columns remain usable after deterministic
release.

The docstring now sits directly before `mmapregion` at
`src/ArrowCore.jl:265-277`. A Docs binding probe on Julia 1.10 confirmed
that `mmapregion` owns the docstring and `_release_mmap` does not.

## Shared layout-role constants and allocation

`FrozenVector` exposes read operations at `src/ArrowCore.jl:421-436`.
`LayoutSpec` is immutable at lines 584-590. The nine shared role vectors are
at lines 607-619, and all layout rows use them at lines 621-652.

Repository-wide mutation and identity scans found no mutation of
`spec.buffers` and no `===`, `objectid`, or pointer-identity dependency for
a layout spec or its buffers. Production consumers only read the vector by
length, indexing, search, or iteration. Direct probes on Julia 1.10 and
1.12 confirmed that `setindex!` and `push!` fail. Equal leaf layouts safely
share one vector. Deliberate reflection through the private `_data` field
is outside the normal `FrozenVector` interface, and no repository code uses
that escape.

The fresh-process allocation child runs inside the tracked Arrow suite at
`test/core_tests.jl:1564-1568`. Its unchanged limit is 12,000,000 bytes per
100,000 rows. I measured:

| Julia | two `Int64` fields | `Int64` + interval field |
|---|---:|---:|
| 1.10.11 | 4,783,696 bytes | 4,783,696 bytes |
| 1.12.6 | 1,605,696 bytes | 1,605,696 bytes |

Both processes were fresh, performed only the three scripted warmups, and
stayed below the bound. The Julia 1.10 delta is about 32 bytes per row, as
the updated note at `test/typed_alloc_child.jl:17-22` states.

Sharing the role vectors does not change trim resolution. The explicit trim
gate compiled and ran on Julia 1.12.6. It passed 6/6 with zero verifier
errors and zero verifier warnings.

## CI workflow and package release note

The test matrix has 48 cells: three packages, four version selectors, two
operating systems, and two thread counts. Each package passes its own
project to setup, build, and test at `.github/workflows/ci.yml:83-107` and
`:127-134`.

Julia 1.10 does not read the root `[sources]` entry for unregistered
ArrowStrings. The current [Pkg documentation](https://pkgdocs.julialang.org/dev/toml-files/#The-%5Bsources%5D-section)
states that this section needs Julia 1.11. A fresh Julia 1.10.11 build
without the local develop step exited 1 with:

```text
expected package ArrowStrings [c38d8858] to be registered
```

After both local packages were developed, the manifest contained local
ArrowTypes and ArrowStrings paths and the action-equivalent build exited 0.
HEAD performs this develop at workflow lines 118-126, before
`julia-buildpkg`. The exact
[build action](https://github.com/julia-actions/julia-buildpkg/blob/90dd6f23eb49626e4e6612cb9d64d456f86e6a1c/action.yml#L61-L64)
and [test action](https://github.com/julia-actions/julia-runtest/blob/6e050c8013b833b1195105ff2fce9cd802f53271/action.yml#L81-L94)
honor their `project` inputs. Therefore, every package and selector cell can
resolve and complete its build step.

The registry Tables release still lacks `Tables.Scan`. Root Arrow CI tests
therefore cannot pass until CI can obtain the unreleased `jq/scan` source.
This is the requested pre-existing condition. The reviewed commits do not
change it.

ArrowTypes still says `version = "2.3.0"` at
`src/ArrowTypes/Project.toml:21`. Version 2.3.0 is already registered with
Julia compatibility `1`; see General's
[versions](https://github.com/JuliaRegistries/General/blob/master/A/ArrowTypes/Versions.toml)
and [compatibility](https://github.com/JuliaRegistries/General/blob/master/A/ArrowTypes/Compat.toml)
records. A release that contains the new floor must use a distinct version
newer than 2.3.0. The exact SemVer increment is a release-policy decision.
This is a release-time note, not a finding against an unreleased HEAD.

The manual accurately separates the two floors at
`docs/src/manual.md:410-421`: Arrow supports Julia 1.10+, while the JuliaC
trim gate needs Julia 1.12.

## Three-commit delta review

- `286fc76` adds only the 285-line round-60 review record.
- `8b2103f` adds only the temporary nine-line ArrowTypes `min` exclusion.
- `69ef8aa` removes that exclusion and contains the stated seven-file floor,
  mmap, layout, CI, test-note, and manual change.
- The mmap branch matches both stdlib implementations. The layout rows keep
  the prior role order and widths. The CI order is required on Julia 1.10.
  The manual matches the executable gates.
- The workflow parses as YAML. The exact delta and final tree pass
  `git diff --check`.

I found no new correctness, safety, portability, performance, CI, test, or
documentation issue in the delta.

## Assumptions and decisions

- I treated exact committed HEAD and the package projects as authoritative.
  All Pkg writes and probes stayed in detached worktrees, isolated depots,
  or scratch environments.
- I treated setup action tags, selector aliases, Julia artifacts, runner
  labels, and registry records as time-sensitive. I checked them on the
  review date. I did not run GitHub Actions.
- I treated successful dependency resolution plus the `julia-buildpkg`
  contract as “buildable.” I kept the later known Tables test failure
  separate.
- I treated normal `FrozenVector` operations as its supported interface. I
  did not require protection against deliberate reflection into `_data`.
- I treated the unchanged, uncalled FlatBuffers helper as outside this
  Julia-floor and three-commit delta review.
- I accepted the documented conformance skips. I treated Documenter's local
  deployment-skip warning as environmental after a content-only build
  exited 0 with no warning or error.
- The host was 64-bit ARM macOS with Julia 1.10.11, Julia 1.12.6, and Docker
  29.6.2.
- I did not modify product or test code in the main checkout. The protected
  untracked files `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`,
  `ISSUE-540.md`, `ISSUE-580.md`, and `mytestdata.arrow` remained untouched.
  This review document is the only main-checkout change made by this round.

## Validation

- Julia 1.10.11 isolated Arrow develop, build, and `Pkg.test("Arrow")` —
  exit 0: ArrowCore 420/420, facade 321/321, threaded caches 4/4, and IPC
  read, IPC write, C data, and ranged-scan acceptance 1/1 each. The two
  fresh allocation children printed 4,783,696 bytes.
- Julia 1.10.11 isolated `Pkg.test("ArrowTypes")` — exit 0, 133/133.
- Julia 1.10.11 isolated `Pkg.test("ArrowStrings")` — exit 0, 2,495/2,495.
- Julia 1.10.11 fresh precompile and module-load probes — exit 0; no method
  overwrite, deprecation, or other warning. The focused mmap Table probe
  also exited 0.
- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` on Julia
  1.12.6 — exit 0: ArrowCore 420/420, facade 321/321, threaded caches 4/4,
  and each acceptance suite 1/1. The allocation children printed 1,605,696
  bytes.
- `julia --project=src/ArrowStrings --startup-file=no -e 'using Pkg;
  Pkg.test()'` — exit 0, 2,495/2,495 on Julia 1.12.6.
- `julia --project=src/ArrowTypes --startup-file=no -e 'using Pkg;
  Pkg.test()'` — exit 0, 133/133 on Julia 1.12.6.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0, 6/6;
  compiler and executable passed with zero verifier errors and warnings.
- `julia --startup-file=no conformance/run.jl` — exit 0: corpus 275 pass / 0
  fail / 36 declared skips; PyArrow 25.0.1 and nanoarrow 0.9.0 oracle 170 /
  0 / 43; C data and stream oracle 143 / 0 / 9. The driver reported corpus,
  oracle, and cdata `PASS`.
- The required docs environment developed exact Arrow and the clean Tables
  checkout. `julia --project=docs docs/make.jl` exited 0. Its document build
  had no content warning or error; `deploydocs` emitted only the expected
  local-environment skip warning. A content-only build exited 0 with zero
  warnings and errors. The rewritten docs Project was restored.
- The current setup-julia resolver probe exited 0. The expected Julia 1.10
  no-develop build probe exited 1; the required develop-before-build probe
  exited 0. All selected release/runner artifact probes succeeded.
- YAML parsing, the layout/doc binding probe, `git diff --check`, and
  `git diff --check 10158a8..69ef8aa` exited 0.
- Final branch HEAD remained
  `69ef8aa7fcf316288f3349e27ec4182ec5f15e5e`. Repository status contained
  only the six protected untracked files plus this review document.

VERDICT: CLEAN
