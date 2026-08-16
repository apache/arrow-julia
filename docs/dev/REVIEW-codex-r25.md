# ArrowCore prove-out review — round 25

Date: 2026-08-15

Scope: `923cdee^..923cdee` under `core/`, judged only against the round-24
findings.

## Result

One LOW documentation finding remains. All eight functional fixes work as
requested. No behavioral regression was found.

## Findings

1. **LOW — the corpus overview still states the old approximate-float
   contract.** `core/conformance/corpus.jl:38-40` says that floats are
   compared approximately. Commit `923cdee` replaced `isapprox` with exact
   equality at `core/conformance/corpus.jl:83-90` and added HALF/SINGLE
   physical-precision normalization at `136-179`. The overview was accurate
   in the parent commit, so the behavior change made it stale. It should say
   that floats are compared exactly after precision-aware normalization.

## Round-24 closure audit

1. **One pool per shared id: closed.** `_validatewriterbatches` builds a fresh
   id-to-pool identity table for every record batch and rejects a second pool
   for the same id (`core/examples/ipc_write.jl:626-649`). Both writers run
   schema/id and batch validation before their output vector is created
   (`694-704`, `751-768`). The committed skew/acceptance pair passes
   (`1654-1668`).

2. **One nested topology per repeated id: closed.** `compatible` compares the
   ids of dictionary-typed fields (`core/examples/ipc_read.jl:723-733`). A
   first occurrence still walks its value field and registers nested ids
   (`734-748`). The stream lookup now throws `ValidationError` when its
   validated value-field table has no id (`1334-1336`). The topology pair
   passes (`core/examples/ipc_write.jl:1669-1681`).

3. **Exact float comparison: functionally closed.** `_eq` uses exact `==`,
   with signed zero unified and NaNs equal (`core/conformance/corpus.jl:83-90`).
   `_normalize!` first rounds HALF and SINGLE batch columns and dictionary
   pools through their physical precision (`136-179`). The stale overview is
   the finding above.

4. **Ranged nullability tier: closed.** The general Field-nullability check
   and `allslots` parameter are gone. Null and Union rules remain at
   `core/examples/scan_ranges.jl:234-239`; FixedSizeList, Struct, sparse
   Union, and REE rules remain at `277-298`. The two acceptance pins pass at
   `2048-2049` and `2068-2069`.

5. **Fresh dictionary-id allocation: closed.** `assigndictids` uses an
   occupied set, assigns from zero upward, and checks `typemax(Int64)` before
   incrementing (`core/examples/ipc_write.jl:541-570`). The wrap pin passes
   (`1682-1687`).

6. **Oracle capability gaps: closed.** The Python driver returns unclassified
   exception text (`core/conformance/oracle.jl:80-84`). Julia applies the
   explicit check/case/error whitelist at `246-262` and classifies results at
   `281-294`. Wrong-check, wrong-case, wrong-substring, and unknown-feature
   probes all remained failures.

7. **Sliced empty offsets: closed.** `_offsetlist` takes the zero-buffer
   shortcut only when `d.offset == 0` (`core/conformance/arrowjson.jl:348-360`).
   An unsliced empty array produced its terminal zero. A sliced empty array
   with no offsets buffer reached the checked load and failed.

8. **Named documentation updates: closed.** The module overview and
   `validate_semantic` docstring state the advisory tier correctly
   (`core/ArrowCore.jl:53-64`, `1230-1240`). The README now describes the
   regenerated bindings, nested/shared dictionary contracts, writer
   validation tier, and local verifier accurately (`core/README.md:158-165`,
   `188-212`, `249-253`).

## Adversarial checks

- A single outer dictionary occurrence registered ids `[10, 20]`.
- `writestream` rejected outer ids `10/10` with nested ids `20/21` before its
  output vector existed.
- Two distinct nested pools carrying id `20` failed with
  `ValidationError`. The same nested pool passed, wrote 1264 bytes, and read
  back successfully.
- Allocation beside given `typemin(Int64)` and `typemax(Int64)` ids produced
  two fresh, distinct ids `0` and `1`. Full domain exhaustion is not practical
  to construct; the terminal guard was checked statically.
- `renumber!` and dictionary-pool float normalization both use field-before-
  children preorder. A reverse-ordered nested-pool probe normalized the
  correct pool. A document without a `dictionaries` key also normalized.
- Float64 and Float32 one-ULP changes failed comparison. The real
  `471.617 -> 471.6174` mutation failed. Signed zero, NaN, and equivalent
  HALF/SINGLE decimal representations behaved as intended.
- Planned and skipped scan walks still consume the same node, fixed-buffer,
  variadic-buffer, dictionary, and child counts. `finishcursor!` passed for
  mixed masks. A malformed REE plan still failed with `ValidationError`.

## Scope and constraints

- The commit changes eight files, all under `core/`.
- The four `_of` ladders are unchanged.
- No GC-reachability, ownership, pointer, or preservation code changed.
- Tracked dependency files are unchanged. The conformance environment still
  uses the local Tables development path.
- The five pre-existing untracked files were not modified.

## Assumptions and decisions

- `923cdee` is the current HEAD and the sole code-review scope.
- Pool identity, not value equality, is the shared-id contract.
- I treated the corpus-file overview as active contract documentation. The
  commit made its prior statement false, so I counted it as a LOW finding
  under the request to confirm that nothing else regressed.
- This was a review request. I wrote this report only. I did not change
  product code or create a fix commit.
- I did not run the network-bound Docker oracle, as directed. The supplied
  `170 pass / 0 fail / 43 skip` result is not independent evidence from this
  run.

## Validation

- `julia --startup-file=no core/test/trim_compile_tests.jl` — 6/6 passed.
- `julia --startup-file=no core/test/runtests.jl` — 325/325 Core and 4/4
  threaded-cache tests passed.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_read.jl`
  — passed.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_write.jl`
  — passed, including the shared-id pins.
- `julia --startup-file=no core/examples/cdata.jl` — passed, including the
  four-thread child.
- `julia --project=core/conformance --startup-file=no core/examples/scan_ranges.jl`
  — passed.
- `julia --project=core/conformance --startup-file=no core/conformance/corpus.jl`
  — 275 pass / 0 fail / 36 skip.
- `git diff --check 923cdee^ 923cdee` — clean.

VERDICT: FINDINGS
