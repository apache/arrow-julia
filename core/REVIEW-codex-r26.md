# ArrowCore prove-out review — round 26

Date: 2026-08-15

Scope: `70b7199^..56e8180` under `core/`, limited to the round-25 review
record and its one follow-up documentation fix.

## Result

No findings. Rounds 24–26 converge from eight findings, to one LOW, to zero.

## Closing checks

1. **`56e8180`: clean.** The overview now states the implemented contract:
   float values compare exactly after HALF and SINGLE columns are
   canonicalized through their physical precision. `_normalize!` applies the
   conversion to batch columns and dictionary pools before `_eq` uses exact
   equality. The commit changes only `core/conformance/corpus.jl`; all changed
   lines are full-line comments. It replaces three comment lines with four
   reflowed comment lines and touches no executable line.

2. **`70b7199`: clean.** The commit adds only
   `core/REVIEW-codex-r25.md`. Its record marks all eight round-24 items
   closed, reports the one stale overview comment as LOW, and ends
   `VERDICT: FINDINGS`. The commit message states the same result. The record
   is unchanged in `56e8180`.

## Assumptions and decisions

- “Exact” means the implemented numeric value equality, including equal NaNs
  and unified signed zero, after physical-precision canonicalization. It does
  not mean bitwise equality.
- I treated only `70b7199` and `56e8180` as review scope. The constrained
  GC-reachability model, four `_of` ladders, and Tables development dependency
  remain unchanged.
- No fix was necessary. This review record is the only file I added.
- I did not run the network-bound Docker oracle. The supplied host-side result
  remains 170 pass / 0 fail / 43 skip.

## Validation

- `julia --startup-file=no core/test/trim_compile_tests.jl` — 6/6 passed.
- `julia --startup-file=no core/test/runtests.jl` — 325/325 Core and 4/4
  threaded-cache tests passed.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_read.jl`
  — passed.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_write.jl`
  — passed.
- `julia --startup-file=no core/examples/cdata.jl` — passed, including the
  four-thread child.
- `julia --project=core/conformance --startup-file=no core/examples/scan_ranges.jl`
  — passed. Its first invocation stalled in Julia's idle scheduler; each phase
  passed in isolation, and the unchanged exact command passed on retry.
- `julia --project=core/conformance --startup-file=no core/conformance/corpus.jl`
  — 275 pass / 0 fail / 36 skip.
- `git diff --check` is clean for each scoped commit.

VERDICT: CLEAN
