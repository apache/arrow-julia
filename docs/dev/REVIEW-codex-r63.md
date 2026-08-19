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

# Arrow.jl 3.0 code review — round 63

Date: 2026-08-19

Scope: exact commit `071f367ed8dcc0667aabf43a92de91056ea4e87a`
(`fix: resolve round 62 findings — bench command literals, research note, two
dead helpers`) on `core-rewrite`. Local `HEAD`, local `core-rewrite`, local
`origin/core-rewrite`, and the live GitHub `refs/heads/core-rewrite` all
resolved to that SHA. Round 62 reviewed
`30181d190b6daa7414e841fd9d8b507a89e15db2`. This round reviewed both commits
in `30181d1..071f367`:

- `740cf53` — record the round-62 review;
- `071f367` — change `bench/run.jl`, `conformance/arrowjson.jl`,
  `docs/dev/research-flatbuffers-cdata.md`, and `tools/fbsgen.jl`.

The delta changes no product code.

## Result

Round 63 has one LOW finding. Three round-62 findings are closed. The EnumX
disposition is accepted. The FlatBuffers/C-data research-note finding is not
closed because the note still contains false present-tree, implementation,
and provenance claims. One false claim, that `_import_cmetadata` is
bounds-checked, was added by the fix commit itself.

The formatter-safe command rewrite is correct. `format(".")` is a no-op, all
five `Cmd` values match `690e1cc` for the same inputs on Julia 1.10.11 and
1.12.6, and no current command literal contains a newline. `_u64` and
`_ident` are gone. A new whole-directory dead-symbol sweep found no other
dead helper in `conformance/`, `tools/`, or `bench/`. The four generated
metadata files regenerate byte-identically after JuliaFormatter. The corpus
suite passes 275/0/36. The package gate passes 421/421 ArrowCore tests and
321/321 facade tests.

## Finding

### LOW — the FlatBuffers/C-data research note still does not describe HEAD

The corrections in `071f367` removed the specific stale helper names, line
counts, malformed remnants, and obsolete C-data gaps reported in round 62.
The note still has these false statements:

- `docs/dev/research-flatbuffers-cdata.md:33-37` says the donated runtime was
  a fresh Go port, was not forked from JuliaData/FlatBuffers.jl, and shares
  only Go ancestry with it. A pre-donation JuliaData/FlatBuffers.jl snapshot,
  commit `898c221e9ee3bb3cb512cf37201b59a70ac267d0`, contains substantial
  Julia-specific builder code that is nearly verbatim in Arrow commit
  `50e015f`: `prep!`, `place!`, vector and string construction, offset
  insertion, assertions, and the vtable writer. Lines 129-136 therefore also
  overstate the architectural split as “total” with no code to merge. The
  high-level reflection and generated-table APIs differ, but the low-level
  implementation has material shared code.
- Lines 40-45 present the rewrite's FlatBuffers usage surface as complete but
  omit live names used outside `src/FlatBuffers/`: `Table`, `Struct`,
  `bytes`, `pos`, `structsizeof`, `UOffsetT`, `prepend!`, `prependoffset!`,
  and `finishedbytes`. The generated struct writers also call
  `offset(::Builder)`, although `offset` is listed only on the read side.
  Examples occur in `src/metadata/{Schema,File,Message}.jl` and
  `src/ipc_write.jl:166-317,483,847`.
- Lines 49-53 put `verifyrootrest_Message` before `FB.getrootas` and say the
  footer does the same. The actual order is root-start verification,
  `FB.getrootas`, the verified inline `version` getter and version gate, then
  root-rest verification at `src/ipc_read.jl:155-160` and
  `src/ipc_write.jl:883-888`. The security invariant remains valid, but the
  stated sequence is false.
- Lines 82-85 say `b.head -= l` would wrap without `prep!`. On the reviewed
  64-bit Julia versions, `UInt32 - Int` produces `Int64`, and assigning a
  negative result to `head::UInt32` throws `InexactError`; it does not wrap.
  `createstring!` also calls `prep!` before the subtraction at
  `src/FlatBuffers/builder.jl:285-296`. A raw `place!` call still has an
  unenforced space contract, and 32-bit mixed arithmetic can differ.
- Lines 145-149 say Arrow's Base-name collisions are handled through
  `RENAMES`. `tools/fbsgen.jl:193-195` uses `RENAMES` only for
  `Struct_ => Struct`. `Bool`, `Int`, and `Type` collisions are handled by
  scalar qualification and a module-local generic at lines 169-195 and
  274-299.
- Lines 203-206 say the current `src/cdata.jl` header states the lifecycle
  re-derivation claim and cites issues #178, #179, #561, #594, and #603-607.
  Its header at `src/cdata.jl:17-62` contains neither the claim nor those
  citations. Lines 306-308 repeat the unsupported source attribution as
  “five stalled attempts, `cdata.jl`.”
- Lines 251-258 say Arrow releases the schema immediately after parsing.
  `_from_c_data` parses the field, preflights and imports the array, and runs
  `validate_semantic` at `src/cdata.jl:1098-1103`. It releases the schema in
  the subsequent `finally` block at lines 1104-1107. The release occurs
  before the API returns, but not immediately after schema parsing.
- Lines 271-273 say Arrow defers an imported `null_count == -1` to on-demand
  `nullcount`. Import calls `validate_semantic` before it returns, and
  semantic validation always counts nulls and atomically stores the result at
  `src/ArrowCore.jl:1563-1574`. The cache is atomic, but the initial scan is
  eager.
- Lines 278-281 say live PR #607 validates schema-metadata bounds and call
  Arrow's `_import_cmetadata` bounds-checked. Exact PR #607 head
  `23de5c2353c34da5557844b30a200d48f78d12f4` does not parse the metadata
  block; it only carries the metadata pointer through the schema move.
  Arrow's `_import_cmetadata` at `src/cdata.jl:1255-1277` explicitly trusts
  producer-declared positive counts and lengths and uses `unsafe_load` and
  `unsafe_string` without an allocation extent or positive cap. Import,
  export, and oracle coverage are present, but neither bounds claim is true.
  The false `_import_cmetadata` qualifier was introduced by `071f367`.

The remainder of the requested note correction is accurate. The live writer
surface no longer names `prependstructslot!`. The gap list no longer calls
file-identifier finishing or shared strings present. The stale line counts
and malformed reference remnants are gone. Import runs structural and
semantic validation through `validate_semantic`; `validate_full` is an
explicit caller opt-in. Every raw imported-region access checks the shared
revocation cell used by `close!`. `_import_cstring` has a 1 MiB cap.
Metadata is imported and exported. The PyArrow oracle covers both C-data
directions, native memory, slices, and streams. The documented lifecycle
verbs are `release!` and `close!`.

## Round-62 closure

### Formatter AST gate — closed

At HEAD, all five `@cmd` macro inputs in `bench/run.jl` are single-line
strings. The four paths formerly held in multiline literals are bound to
locals and interpolated. A recursive `Meta.parseall` walk found zero newline
characters in every raw command string.

I evaluated the five command macrocalls from `690e1cc` and HEAD in the same
Julia process. The fixture used spaces and punctuation in `here` and
`workdir`. On Julia 1.10.11 and 1.12.6, all five pairs were `isequal`, and
their `exec`, `ignorestatus`, `flags`, `env`, `dir`, and `cpus` fields were
identical. The checks cover environment instantiation, the rewrite leg, the
Arrow 2.x leg, Docker inspection, and the Docker/PyArrow leg.

JuliaFormatter 2.12.5 ran `format(".")` at HEAD. It exited 0 and left the
detached worktree byte-clean.

### Research note — open

The reported round-62 examples are corrected, but the finding remains open
for the false statements listed above.

### Dead helpers — closed

`_u64` and `_ident` have no current nonhistorical occurrence. At `30181d1`,
each occurred only in its own definition. The fix deletes only those
definitions.

I regenerated `Schema.jl`, `File.jl`, `Message.jl`, and `Verifier.jl` with
both the pre-fix and HEAD generator, then formatted both outputs with
JuliaFormatter 2.12.5 and the repository `.JuliaFormatter.toml`. Both output
sets match each other and the four committed files byte for byte.

### EnumX — disposition accepted

`conformance/Dockerfile:64-71` says the warm list contains every registered
dependency resolved by the package and conformance environments. Root
`Project.toml:29` directly depends on EnumX, and `src/Arrow.jl:77-84` uses it
for generated metadata enums. The conformance project does not need to
duplicate that direct dependency. Keeping EnumX follows the stated rule.
The `690e1cc` commit message overstated its removal; no code change is needed.

## Dead-symbol and delta audit

I parsed all ten Julia files in `conformance/`, `tools/`, and `bench/` with
`Meta.parseall`, collected 105 unqualified method definitions representing
101 unique names, and checked repository-wide symbol references. Every
low-reference definition has a caller or is an intentional top-level/public
entry point. I separately checked the nine Python function definitions in
`bench/bench_pyarrow.py`, plus low-reference constants, types, and imports.
No additional dead symbol remains in the requested directories.

The complete `30181d1..071f367` diff is mechanically narrow. The benchmark
rewrite preserves command values. The two helper deletions are output-neutral.
`740cf53` only adds the round-62 record. No product-code defect was introduced.
The new false bounds qualifier and the other remaining false note statements
are the sole finding.

## Assumptions and decisions

- I treated the research note's “usage surface” lists as complete because the
  text calls them the surface and says the vendored runtime carries only what
  that surface needs.
- I refreshed the note's dated GitHub state on 2026-08-19. The PR states,
  sizes, authors, and the current JuliaData/FlatBuffers.jl release and issue
  counts still match the note. I inspected live PR #607 at its exact head for
  the metadata claim.
- I classified the remaining research-note defects as one LOW finding. They
  do not alter package behavior, but the user required every false claim to
  be a finding.
- I accepted the existing tagged conformance image. The corpus command did
  not require an image rebuild.
- I treated generated interface methods and intentional script entry points
  without an in-repository caller as live, not dead code.
- A fresh detached worktree initially resolved registered Tables 1.13.0,
  which does not provide the required `Tables.Scan`. The repository README
  requires the `jq/scan` checkout. I developed the clean checkout at
  `ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9` into the ignored scratch
  manifest and reran the exact package gate.
- I did not run the full Julia 1.10 package suite, trim tests, documentation
  build, or the oracle/C-data Docker suites because this round excluded them.
  I used Julia 1.10 only for command-value equivalence and the builder
  subtraction probe.
- I did not modify product or test code. The protected untracked files
  `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`, `ISSUE-540.md`,
  `ISSUE-580.md`, and `mytestdata.arrow` remained untouched. This review
  document is the only active-checkout change made by this round.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0
  after the documented Tables setup: ArrowCore 421/421 and facade 321/321.
  IPC read, IPC write, C-data, statistics, `Tables.Scan`, and byte-range
  checks all printed their pass markers.
- `julia --startup-file=no conformance/run.jl corpus` — exit 0: 275 pass,
  0 fail, 36 skip. Each of the five check families passed 55/55. The existing
  image used Julia 1.12.6, PyArrow 25.0.1, nanoarrow 0.9.0, and the required
  Tables checkout.
- JuliaFormatter 2.12.5 `format(".")` — exit 0 and no tracked change.
- Five cross-revision `Cmd` comparisons — all fields identical on Julia
  1.10.11 and 1.12.6.
- Both generator versions plus JuliaFormatter — all four generated metadata
  files byte-identical to HEAD.
- Whole-directory Julia and Python dead-symbol sweeps — no additional dead
  symbol.
- `dev/release/run_rat.sh .` in a detached worktree exited 1 only because RAT
  sees the administrative `.git` pointer as an unapproved file. The same
  exact committed tree, materialized from `git archive` without worktree
  metadata, exited 0 with no unapproved license. A separate RAT check of this
  review document also exited 0.
- `git diff --check` passed for the reviewed delta. The no-index whitespace
  check of this new review file produced no diagnostic; its expected exit 1
  only records that the file differs from `/dev/null`. Every detached
  worktree remained clean.
- Final branch HEAD remained
  `071f367ed8dcc0667aabf43a92de91056ea4e87a`. Active status contained only
  the six protected untracked files and this review document.

VERDICT: FINDINGS
