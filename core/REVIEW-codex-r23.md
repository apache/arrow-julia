# ArrowCore prove-out review — round 23

Date: 2026-08-15

Scope: `a08dcb2` and `a907e5b` only.

## Result

No findings.

- `a08dcb2` only moves `_stats_main()` before `_scan_main()`. The exact scan
  command completed against Tables `jq/scan` at `3bfa6b6` on Julia 1.12.6
  with 10 default threads. It printed the statistics, Stage-A, and byte-range
  final sentinels in that order and exited successfully. All 124 prior
  `@assert` sites remain: 31 statistics, 30 Stage-A, and 63 ranged. Statistics
  build their own fixtures; `_scan_main()` still supplies `filebytes`, `af`,
  and `full` directly to `_ranged_main()`.
- `a907e5b` is a mechanical rename. Replacing `Tables.read` with
  `Tables.scan` in each parent file makes it byte-identical to the committed
  file. No active `Tables.read` use remains under `core/`; the sole hit is
  round-22 review history. Tables `77d82d1..3bfa6b6` contains only the rename
  commit. `bind`, `finish`, and fallback `apply` are unchanged, and the
  one-call wrapper still performs `apply` followed by `finish`. The focused
  Tables scan tests pass 89/89. No Arrow-side semantic fix is needed.

## Assumptions and decisions

- Review-history files mean `core/REVIEW*.md`.
- The constrained GC model, four `_of` ladders, and dev'ed Tables dependency
  remain unchanged and outside this round's scope.
- Unrelated untracked files were ignored. No fix commit was necessary.

## Validation

- Trim compile: 6/6 passed.
- Core: 297/297 plus 4/4 threaded-cache tests passed.
- IPC read, IPC write, and C Data commands passed.
- The exact scan command completed without a stall and all three groups passed.
- Both scoped commit diffs pass `git diff --check`.

VERDICT: CLEAN
