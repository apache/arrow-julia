# ArrowCore prove-out review — round 22

Date: 2026-08-15

Scope: only the ten round-21 commits `a51568e..45dc5c6` under `core/`, with
fresh adversarial checks of their stated fixes.

## Finding and disposition

1. **LOW — the exact scan keep-green command could still stall.** Commit
   `bd0bbaf` split the statistics acceptance and put `Arrow.Table` behind an
   `invokelatest` barrier, but the driver still compiled `_stats_main` after
   the large Stage-A and ranged drivers. On Julia 1.12 with 10 threads, the
   exact command completed both earlier groups and then remained blocked for
   46 minutes before the first statistics result. The statistics units passed
   alone; the failure was the order-dependent aggregate compilation after the
   large scan driver. Disposition: fixed in `a08dcb2`. The independent
   statistics group now runs first. This adds no machinery and preserves every
   assertion.

No other finding survived focused reproduction. The descriptor fallbacks,
ListView child contracts, sliced and nested REE paths, IPC REE schemas, ranged
variadic accounting, nested statistics, boundary regressions, and four `_of`
ladders all matched their round-21 claims.

## Assumptions and decisions

- The constrained GC-reachability model and four `_of` ladders remain final.
- A finding fixed during this round still makes this a findings round.
- The active Tables `jq/scan` checkout changed from `Tables.read` to
  `Tables.scan` during review. It was not modified or reverted. Final scan
  validation used an isolated clean `77d82d1` checkout.

## Validation

- Trim compile: 6/6 passed.
- Core: 297/297 plus 4/4 threaded-cache tests passed.
- IPC read, IPC write, and C Data acceptance commands passed.
- The exact scan script completed in 27.9 seconds with Julia 1.12, 10 threads,
  and isolated Tables `77d82d1`. Statistics, Stage-A, and byte-range sentinels
  all passed.
- Focused probes passed for REE-to-REE, REE-to-dictionary, dictionary-to-REE,
  View values, sliced boundaries, empty sliced REE, LargeListView, adjacent
  variadic overflows, schema-only stream/file checks, and all 22 descriptors.
- `git diff --check` passed.

VERDICT: FINDINGS
