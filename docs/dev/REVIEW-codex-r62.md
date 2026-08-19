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

# Arrow.jl 3.0 code review — round 62

Date: 2026-08-19

Scope: exact commit `30181d190b6daa7414e841fd9d8b507a89e15db2`
(`style: apply JuliaFormatter to the rewrite`) on `core-rewrite`. Local
`HEAD`, local `core-rewrite`, local `origin/core-rewrite`, and the live GitHub
`refs/heads/core-rewrite` all resolved to that SHA. Round 61 is recorded at
`ba06e9301a583ec0ff868b1372a47104b91056d5`. I reviewed the complete
three-commit `ba06e93..30181d1` delta:

- `e39265b` — add ASF headers and release ArrowTypes 2.4.0;
- `690e1cc` — repository-wide test, dead-code, documentation, and tooling
  cleanup;
- `30181d1` — apply JuliaFormatter to 39 Julia files.

All Tables-dependent local checks used the clean `jq/scan` checkout at
`ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`.

## Result

Round 62 has four LOW findings. The product behavior and all executable
package, trim, documentation, and conformance suites are clean. The findings
are one strict formatter-claim failure, an incomplete development-note
cleanup, two ordinary dead helpers missed by the sweep, and one conformance
image claim mismatch.

The restored statistics and byte-range suites run inside `Pkg.test` on both
Julia 1.10.11 and 1.12.6. Their required pass lines occur exactly once in
each log. The nullable-list behavior is correct and fixes schema stability
across partitions. The `DictEncode` hierarchy is coherent. No supported path
can run the semantic REE check on a node before structural validation has
established that node's invariants. Every named deletion had no live caller.
The four generated metadata files regenerate byte-identically after
JuliaFormatter.

`format(".")` is idempotent at HEAD. However, the required stripped
`Meta.parseall` comparison reports `bench/run.jl` as different. The
pre-expansion AST keeps command-literal indentation as string data. The
resulting command values are still identical.

## Findings

### LOW — the formatter commit is not expression-tree identical

The required comparison parsed every one of the 39 changed `.jl` files at
`690e1cc` and HEAD, recursively removed every `LineNumberNode`, including
nodes below `QuoteNode`, and compared the remaining trees with `isequal`.
It reported 38 identical files and one mismatch:

```text
MISMATCH bench/run.jl
FILES 39
MISMATCHES 1
```

An exhaustive tree diff found exactly three different leaves. They are the
raw strings held by the `@cmd` macro calls for the rewrite, Arrow 2.x, and
Docker benchmark legs at `bench/run.jl:59-95`. JuliaFormatter reduced the
continuation indentation in each string by four spaces. No expression head,
arity, symbol, literal other than those spaces, or other value changed.

This does not change the executed commands. On Julia 1.10.11 and 1.12.6, all
five command literals from both revisions were evaluated with fixture paths
that contained spaces. The three affected raw strings differed, but every
field of every resulting `Cmd` object, including each `exec` vector, was
identical. Julia's command parser treats the changed indentation as argument
separation.

Thus the claim “whitespace/wrapping only” is true at runtime, but the strict
AST-identity gate requested for this round fails. Protecting or restructuring
these multiline command literals would make a future formatter-only commit
pass that gate.

### LOW — the FlatBuffers/C-data research note does not describe HEAD

Commit `690e1cc` says the research note was corrected to the present tree.
The note still contains false claims, and that commit introduced two malformed
reference remnants:

- `docs/dev/research-flatbuffers-cdata.md:41-46` lists the deleted
  `prependstructslot!` as part of the live writer surface.
- Lines 108-109 say every `Builder` allocates a shared-string `Dict`. Lines
  117-123 say `finishwithfileidentifier` exists and shared strings through
  `createsharedstring!` are present. Commit `690e1cc` deleted all of them.
  The current `Builder` at `src/FlatBuffers/builder.jl:31-53` has no shared
  string field or allocation.
- Line 53 contains the malformed text `comment at )`. Line 289 contains
  `boundary ();`. Both remnants were introduced by `690e1cc` when it removed
  old line references.
- Lines 37-38 and 188-190 state 261 lines for `VerifierRuntime.jl`, about 660
  for `fbsgen.jl`, and about 1,500 in total. HEAD has 284, 960, and 1,817
  lines, respectively. JuliaFormatter made these counts stale in `30181d1`.
- Lines 248-249 say C-data import runs structural, semantic, and full
  validation. `_from_c_data` calls `validate_semantic` at
  `src/cdata.jl:1064-1103`; that composes structural and semantic checks.
  `validate_full` remains an explicit caller opt-in.
- Lines 264-270 describe only reachability-based import validity and zero
  per-read overhead. The documented public `close!(::ForeignOwner)` uses a
  shared revocation cell at `src/cdata.jl:1032-1041`, and each `sliceptr`
  checks that cell. The separate `release!` operation keeps its documented
  post-release undefined-behavior rule, but the note's broad zero-overhead
  comparison is still false.
- Lines 278-282 say `_import_cstring` is unbounded. It enforces a 1 MiB limit
  before every dereference at `src/cdata.jl:1219-1243`.
- Lines 283-286 say schema metadata is neither imported nor exported.
  `_import_cmetadata` and `_import_field` import it at
  `src/cdata.jl:1255-1287`; `_cmetadata!` exports it. The C-data battery tests
  both directions. Positive allocation extents remain a trusted-ABI limit,
  but metadata I/O is not missing.
- Lines 304-306 say there is no external C-data integration test.
  `conformance/cdata_oracle.jl` tests Arrow.jl with PyArrow in both
  directions, PyArrow-native memory, slices, and C streams. Lines 308-309
  also list bounded strings and metadata import/export as future work even
  though both exist.
- Line 311 recommends `release_c_data`-style facade names. The present public
  docs and API use `release!` and `close!`. This is an obsolete recommendation
  rather than a runtime claim.

Some statements predate this delta. They are findings here because the
repository-wide cleanup explicitly claimed that this note now described the
present tree, edited many of these lines, deleted the named APIs, and was the
requested sweep for missed stale narrative.

### LOW — two ordinary dead helpers remain

The cleanup pass missed two private helpers:

- `_u64` at `conformance/arrowjson.jl:225` has no caller. `_intdata` parses
  unsigned types directly at lines 246-252.
- `_ident` at `tools/fbsgen.jl:82` has no caller. `parsefbs` begins at line 89
  and does not use it.

Exact whole-repository symbol searches, excluding historical review records,
found only each definition. Both helpers predate this delta. They are in
scope because `690e1cc` claimed a repository-wide dead-code cleanup and this
round explicitly requested a second sweep.

### LOW — EnumX remains in the conformance image warm list

`conformance/Project.toml:18-23` has neither EnumX nor PooledArrays, as
claimed. `conformance/Dockerfile:64-71` still explicitly includes `EnumX` in
the `Pkg.add` warm list. The `690e1cc` diff removed only PooledArrays from
that list, although its full commit message and this round's contract both
say that EnumX and PooledArrays were removed from the environment and image.

Keeping EnumX in the image is functionally reasonable: root Arrow still
directly depends on it at `Project.toml:29`, and the inline `Meta` module uses
it. This is a tooling and claim mismatch, not a dependency or conformance
failure. The image list or the stated contract must be made consistent.

## Restored test reachability

`test/runtests.jl:19-28` includes the core tests, facade tests, and batteries.
`test/batteries.jl:28-50` includes all five battery files and calls the scan
entry points in the required order:

```julia
_stats_main()
filebytes, af, full = _scan_main()
_ranged_main(filebytes, af, full)
```

Both complete inside the real Arrow `Pkg.test` process on Julia 1.10.11 and
1.12.6. Each log contains exactly one `Statistics write/prune checks passed.`
line and exactly one `Byte-range scan checks passed.` line.

A repository-wide scan of test definitions and call sites found no other
orphan test helper. The typed-allocation, threaded, C-string-guard, and
C-data-stress child scripts all have checked launch paths. The `_fixture2x`
do-blocks are intentional 2.x fixture provenance in replay mode, not dormant
3.0 assertions. No `f(...) = begin ... end` form remains at HEAD, at
`690e1cc`, or at its parent.

The dead `_u64` in conformance and `_ident` in the generator are reported
above because they are ordinary implementation helpers, not test entry
points.

## Behavior changes

### Declared nullability for lists

`_build_list` now derives `Field.nullable` from `eltype(v) >: Missing` at
`src/ArrowCore.jl:3005-3027`, matching `_build_strings` and the primitive
builder rule. This is correct.

A missing-free `Vector{Union{Missing,Vector{Int64}}}` produced a nullable
field with null count zero. File and stream facade round trips returned
`Union{Missing,Vector{Any}}`, preserved the values, and preserved the
nullable schema through a second write/read cycle. A two-partition write
whose first partition had no observed missing value and whose second did
contain `missing` also passed. Under the old observed-count rule, the first
partition declared a non-nullable schema and the second hit the nullable
mismatch at `src/write.jl:698-706`.

`fromjulia_dict` deliberately still uses `nc > 0` at
`src/ArrowCore.jl:3051-3064`. A missing-free declared-union indices vector
therefore remained non-nullable. This is consistent with the stated
exception: its constructed indices vector does not retain the caller's
declared index type.

### `DictEncode` element type

`DictEncode{T,V<:AbstractVector{T}} <: AbstractVector{T}` at
`src/write.jl:27-37` now satisfies the `AbstractVector` element-type
contract directly. Construction, indexing, dispatch, and writer round trips
passed. No repository code names an explicit old parameterization.

External code that explicitly named the old one-parameter type shape must
change. That is an expected source break for the unreleased 3.0 major
version. The new hierarchy is coherent and caused no downstream facade or
conformance issue.

## REE validation order

The reduced `_validate_ree_values` is safe on every supported and
repository-reachable path:

- Public `validate_semantic` routes to `_validate_semantic` at
  `src/ArrowCore.jl:1472-1484`.
- `_validate_semantic` always calls `_validate_structural` before
  `_validate_semantic_intrinsic` on the same root.
- The structural walk checks `typeequal` for each node, recurses through all
  children at lines 1236-1238, and then establishes the REE descriptor,
  non-null run-end field, parent null-count rule, and equal child lengths at
  lines 1292-1307.
- Only after the complete structural walk returns can the intrinsic walk
  call `_validate_ree_values` at line 1562. Its child recursion is at lines
  1576-1586.
- `validate_full` composes `validate_semantic` first at lines 1899-1912.
  Adapters call the public semantic function or `_validate_semantic`; none
  calls the intrinsic helper directly.
- The `semachecked` cache can skip an already-completed intrinsic scan, but
  structural validation is not cached and still runs first. Dictionary
  memoization only skips pools already certified by the dictionary-batch or
  writer validation path.

Repository-wide call-site searches found no other caller of
`_validate_semantic_intrinsic` and no other caller of `_validate_ree_values`.
Calling an underscore-prefixed intrinsic helper directly from external code
is unsupported; no package path does so.

## Dead-code deletion and generation audit

At the parent of `690e1cc`, definition-and-caller searches over `src/`,
`test/`, `conformance/`, `bench/`, `tools/`, and `docs/` confirmed no live
caller for the removed names. This includes the generated metadata and
Verifier runtime. I checked:

- `_blockmessagebodylength`, `_containsdictionary`, the five-argument
  `IPCStream`, `_scanbatch(f, i, mask)`, the four-argument `_writecolumn`, the
  IPC/C-data `_vu16` alias, and dictionary `formatstring`;
- `_validate_temporal_values`, the generic `_validate_advisory_values`
  fallback, `arrowtype_for(::Type{String})`, `_juliatype_of`'s Struct branch,
  and `batch`'s duplicate validation loop;
- FlatBuffers `finishwithfileidentifier`, `createsharedstring!` and its
  `sharedstrings` field, `createbytevector`, `prependstructslot!`, the unused
  `sh` parameter, `getvalue`, `getoffsetslot`, `getslot`, `setindex!`, and the
  Builder-based table constructor.

The only current nonhistorical text references to removed FlatBuffers names
are the stale research-note claims reported above. The remaining `_vu16` in
`VerifierRuntime.jl` is a separate live verifier function, not the deleted
IPC/C-data alias.

`src/metadata/Flatbuf.jl` is absent and has no include. The inline `Meta`
module at `src/Arrow.jl:75-85` includes `Schema.jl`, `File.jl`, `Message.jl`,
`VerifierRuntime.jl`, and `Verifier.jl`. The generator emits the four
generated files. Running it and then formatting those four outputs produced
no byte difference from HEAD.

## Documentation and tooling audit

`Base.Docs.meta` probes attached every new or moved docstring to its intended
definition. The checked objects included `mmapregion`, `ForeignOwner`,
`_boundschema`, `_blockmessage`, the descriptor structs, `Schema`,
`BufferRole`, `layoutspec`, `close!(::ForeignOwner)`,
`release!(::StreamOwner)`, dynamic and typed `materialize`, `statsfile`,
`ArrowTypes.ToArrow`, `ValidationError`, and `ImportedStream`. Comparing the
same objects and signatures across Arrow and ArrowCore found no duplicate
docstring.

The docs build rendered `Arrow.ValidationError`, `Arrow.ImportedStream`, and
`Arrow.close!(::Arrow.ForeignOwner)` from `docs/src/reference.md:47-65`.
The corrected user documentation matches the implementation:

- `getvalue` uses a closed descriptor `isa` ladder;
- `fromjulia` cannot zero-copy Bool because its Arrow data is bit-packed;
- `mmap` is a path-and-file-format option, not a stream option;
- deterministic C-data `close!` targets the shared revocation cell behind a
  `ForeignOwner` or owner region.

The separate research-note errors are findings because they make additional
present-tree claims that contradict this implementation.

The CI matrix at `.github/workflows/ci.yml:83-101` is three packages by
`['min', '1.11', '1', 'pre']`, two operating systems, and two thread counts:
48 upload-producing test cells. The official setup, build, and test action
contracts honor the supplied project paths. Only the root Arrow cell develops
`src/ArrowStrings`, before the build action. Root Arrow has no ArrowTypes
dependency. `codecov.yaml:18-22` waits for 48 builds.

The conformance host declares Harbor 1.1 compatibility. The corpus refuses an
empty `ARROW_TESTING_DIR` before path use. The container creates a separate
runtime environment and develops `/work` and `/opt/Tables`, so Pkg does not
rewrite the checkout. The EnumX image-list exception is the finding above.

The release README covers ArrowTypes and ArrowStrings registration and orders
ArrowStrings before Arrow. `verify_rc.sh` tests ArrowTypes, tests ArrowStrings,
and develops the local ArrowStrings package before testing root Arrow. Every
release shell script passed `bash -n`.

## ASF and formatter audit

ArrowTypes is version 2.4.0 and declares Julia 1.10 at
`src/ArrowTypes/Project.toml:18-28`. The header commit and the new review-file
header were checked against the repository audit form.

In a detached scratch worktree, `dev/release/run_rat.sh .` reported one
unapproved path only:

```text
NOT APPROVED: .git (./.git): false
```

No project file was unapproved. RAT exits 1 for that expected linked-worktree
`.git` entry. Its XML, filtered report, and jar remained ignored in the
scratch worktree and did not touch the active checkout. A separate RAT run
over this new review file exited 0 with no unapproved license.

JuliaFormatter 2.12.5 ran `format(".")` in the detached worktree. It exited
0, and an immediate `git diff --exit-code`, `git diff --check`, and complete
status check were empty. The strict cross-revision AST exception is reported
as the first finding.

## Assumptions and decisions

- I treated exact committed HEAD and the package projects as authoritative.
  All Pkg writes, generation, formatting, RAT outputs, and probes stayed in
  detached worktrees, isolated depots, or scratch environments.
- I used named `Pkg.test` calls from scratch environments on Julia 1.10.11.
  This is the checkout-safe equivalent of the requested project gates. Each
  main-package environment developed Arrow, ArrowTypes, ArrowStrings, and the
  requested Tables checkout. The subpackage environments developed their
  target packages. Pkg never touched a checkout Project file.
- I treated generated binding methods and public interface methods without a
  textual repository caller as intentional surface, not dead code.
- I treated dated external-PR history in the research note as historical.
  I classified a statement when it asserted a current repository fact.
- I classified the command-literal AST mismatch as LOW because the explicit
  gate fails but both supported Julia versions construct identical commands.
- I classified the retained EnumX warm entry as a claim mismatch, not a
  dependency defect, because root Arrow still depends on EnumX.
- I accepted the declared conformance skips. I treated Documenter's local
  deployment-skip warning as environmental after all content checks and
  rendering completed successfully.
- Every Julia gate ran as the real process with its exit status captured
  directly. No Julia process was piped through a filter.
- The host was 64-bit ARM macOS with Julia 1.10.11, Julia 1.12.6, Docker
  29.6.2, and JuliaFormatter 2.12.5.
- I did not modify product or test code. The protected untracked files
  `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`, `ISSUE-540.md`,
  `ISSUE-580.md`, and `mytestdata.arrow` remained untouched. This review
  document is the only active-checkout change made by this round.

## Validation

- Julia 1.12.6 isolated `Pkg.test("Arrow")` — exit 0: ArrowCore 421/421,
  facade 321/321, and all IPC read, IPC write, C-data, statistics, scan, and
  ranged-scan acceptance checks completed. Both required scan markers occur
  exactly once.
- Julia 1.12.6 isolated `Pkg.test("ArrowStrings")` — exit 0, 2,495/2,495.
- Julia 1.12.6 isolated `Pkg.test("ArrowTypes")` — exit 0, 133/133 for
  ArrowTypes 2.4.0.
- Julia 1.10.11 repeated the same three isolated gates — all exit 0. Arrow
  passed 421/421 and 321/321 plus every acceptance battery; ArrowStrings
  passed 2,495/2,495; ArrowTypes passed 133/133. Both required scan markers
  occur exactly once. The logs contain no warning, error, or failure.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0, 6/6. Compile
  and execution passed with zero verifier errors and warnings.
- `julia --startup-file=no conformance/run.jl` — exit 0: corpus 275 pass / 0
  fail / 36 skip; PyArrow 25.0.1 and nanoarrow 0.9.0 oracle 170 / 0 / 43;
  C-data and stream oracle 143 / 0 / 9. The driver reported corpus, oracle,
  and cdata `PASS`.
- The docs environment developed exact Arrow and the clean Tables checkout.
  `julia --project=docs --startup-file=no docs/make.jl` exited 0. Doctests,
  expansion, cross-references, document checks, and HTML rendering passed.
  The sole warning was the expected local `deploydocs` environment-detection
  skip. The scratch `docs/Project.toml` was restored.
- JuliaFormatter no-op, generated-file identity, doc binding and duplicate
  probes, command-value equivalence, shell syntax checks, and `git diff
  --check` all exited 0.
- The stripped expression-tree comparison intentionally exited 1 after
  reporting only `bench/run.jl`; that failure is finding 1.
- RAT intentionally exited 1 after reporting only detached-worktree `.git`
  noise. No repository file failed the license audit.
- Final branch HEAD remained
  `30181d190b6daa7414e841fd9d8b507a89e15db2`. Status contained only the six
  protected untracked files plus this review document.

VERDICT: FINDINGS
