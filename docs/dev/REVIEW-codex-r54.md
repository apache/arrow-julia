# Arrow.jl 3.0 code review — round 54

Date: 2026-08-17

Scope: exact commit `6333241025d0207498ff039c01026df18283d18d`
(`docs+test: record round 53 review; harden the Float24 bulk pin`) on
`core-rewrite`. Its parent is
`e0d2ce22daf879be19b85960c78169e75a4abb0c`, the exact round-53 review
target. I reviewed the one-commit `e0d2ce2..6333241` delta, repeated the
round-53 bulk-gate mutation in an isolated worktree, reran every required
gate, and replayed the focused Core, facade, allocation, and three-leg
benchmark surfaces from rounds 51-53.

All 3.0 package, conformance, and focused probe runs selected the clean
Tables.jl `jq/scan` checkout at
`d1fbb6eb577741688dba70039754166b51c1cdcc`.

## Result

Round 54 is clean. The one LOW finding from round 53 is closed.

The padded Float24 fixture contains 16 bytes. This is enough for the
regressed bulk path to copy two Float64 values. With only
`primwidth(t) == w || return nothing` neutralized, the complete tracked Core
suite failed at the Float24 materialize assertion: 390 pass, 1 fail, 391
total. The mutated call returned `[1.25, -3.5]`. It did not throw from buffer
bounds. The separate scalar getvalue assertion remained green.

With the guard restored, the package suite passed all 671 reported
assertions. The focused round-51/52 Core and facade matrices also passed.
The complete round-53 benchmark surface passed with all three
implementations and all expected records.

The one-commit delta adds the round-53 review document and changes only the
Float24 fixture in `test/core_tests.jl`. It does not change product,
benchmark, conformance, project, or manifest source. I found no new issue of
any severity.

## Closure of the round-53 finding

The fixture now creates `f64bytes` from two Float64 values at
`test/core_tests.jl:1436-1442`. It keeps the typed materialize and getvalue
refusal assertions at `test/core_tests.jl:1443-1445`.

The valid implementation checks the descriptor width at
`src/ArrowCore.jl:2551-2563`. A Float24 descriptor with a Float64 claim fails
that check. Materialization then uses the element path, where the independent
claim-width check at `src/ArrowCore.jl:2402-2416` raises the required managed
`ArgumentError`.

The scratch mutation changed only:

```diff
-    primwidth(t) == w || return nothing
+    true || return nothing
```

The bulk path then requested and copied the full 16-byte window at
`src/ArrowCore.jl:2564-2574`. The tracked assertion at
`test/core_tests.jl:1444` failed because no `ArgumentError` was thrown. A
separate direct call returned the two padded values. Thus, the pin now fails
for the exact wrong-width-copy regression from round 51. It no longer passes
because the backing buffer is too short.

## One-commit delta review

- `docs/dev/REVIEW-codex-r53.md` adds the 200-line round-53 review record.
- `test/core_tests.jl` has six additions and one deletion. It replaces the
  natural six-byte Float24 buffer with the 16-byte padded payload.
- No implementation, benchmark, conformance, dependency, or build file
  changed.
- The payload is deterministic on the host. The pin requires a managed
  refusal. Therefore, any successful bulk return fails the assertion.
- The existing scalar pin remains independent from the bulk pin.

I found no correctness, safety, portability, or test-quality concern in this
delta.

## Round-51/52/53 clean regression surface

- The Core probe passed 324/324 valid fixed-width descriptor assertions.
  It also passed cached-bitmap authority 4/4, hostile validity geometry 5/5,
  invalid Int/Float widths 22/22, invalid Time combinations 15/15, nonzero
  offsets 7/7, Bool exclusion 4/4, and Decimal32/64 null punching 10/10.
- The facade public-path matrix passed 346/346. The nested Dictionary/REE
  union matrix passed 38/38.
- The independent closed facade allocation check passed at 819,312 bytes.
- The complete benchmark driver exited 0. Rewrite, registered Arrow 2.8.1,
  and PyArrow each emitted all ten JSONL records. The runtime read-semantics
  warning and complete ten-row report printed.
- The three dictionary outputs passed 12/12 checks. Each used a
  `DictionaryType` field, contained 2,000,000 rows and 32 logical values,
  and matched the workload formula.
- The Arrow 2.x leg loaded version 2.8.1. Its dependency was neither path nor
  repository tracked.

## Assumptions and decisions

- I treated direct unvalidated `ArrayData` with an oversized backing buffer
  as in scope. Round 51 exposed the wrong copy through this same state. The
  typed bulk path also states that it serves unvalidated data.
- I required the tracked materialize assertion to fail when only the bulk
  guard was removed. The independent scalar assertion did not need to fail
  under that mutation.
- I accepted the established corpus and oracle skip lists. The host was
  64-bit arm64 macOS with Julia 1.12.6. The oracle used PyArrow 20.0.0 and
  nanoarrow 0.9.0.
- Detached worktrees do not contain the ignored development manifests. Cold
  setup attempts therefore stopped before valid tests because released
  Tables lacks `Tables.Scan`, or because the conformance dependencies were
  absent. I selected the same clean scan-enabled Tables checkout used in
  rounds 52-53, then repeated the exact gate commands. I kept setup-only
  exits separate from the reported gate exits.
- I did not modify product or test code in the main checkout. All mutation
  work stayed in a scratch worktree, which was removed after verification.
  I moved the 861 MB benchmark scratch directory to the macOS Trash, so it
  remains recoverable.
- The six protected untracked files remained present and untouched. This
  review document is the only repository change.

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
- `git diff --check` — exit 0. `git diff --check HEAD^ HEAD` also exited 0
  for the exact one-commit delta.
- Bulk-gate mutation — direct tracked Core command exit 1 as required:
  ArrowCore 390 pass / 1 fail / 391 total. The only failure was the Float24
  materialize assertion at `test/core_tests.jl:1444`, which reported that no
  exception was thrown. The direct value diagnostic exited 0 and returned
  `[1.25, -3.5]`.
- Core scratch probe — exit 0 with the exact matrix counts listed above.
- Facade scratch probes — exit 0 at 346/346 and 38/38. The facade allocation
  child also exited 0 at 819,312 bytes.
- Full three-leg benchmark — exit 0. Each implementation emitted 10 records.
  The dictionary output probe exited 0 at 12/12.
- Final HEAD remained
  `6333241025d0207498ff039c01026df18283d18d`. Repository status contained
  only the six protected untracked files plus this review document.

VERDICT: CLEAN
