# Arrow.jl 3.0 dependency review — round 30

Date: 2026-08-16

Scope: commit `e97d9e107500495c99bf35d63805b6077beacf03` only.

## Result

The direct TranscodingStreams dependency path is correct. The declared
minimum dependency graph is not resolvable, so the required minimum-version
load and compression round-trips cannot run. One adjacent source comment is
also stale.

## Findings

1. **HIGH — the codec minima cannot co-resolve with the declared
   TranscodingStreams floor.** `Project.toml:32-36` permits CodecLz4 0.4.0
   and CodecZstd 0.8.0, but requires TranscodingStreams 0.10 or 0.11. The
   General registry requires TranscodingStreams 0.9 for both CodecLz4 0.4.0
   and CodecZstd 0.8.0.

   A clean temporary environment fixed the requested versions exactly:
   CodecLz4 0.4.0, CodecZstd 0.8.0, EnumX 1.0.0, TranscodingStreams 0.10.0,
   Arrow at `e97d9e1`, and a clean Tables `jq/scan` checkout. `Pkg` failed
   with `Unsatisfiable requirements`: CodecZstd 0.8.0 restricted
   TranscodingStreams to 0.9.0–0.9.13 while the explicit requirement fixed
   it to 0.10.0. A separate probe reproduced the same conflict through
   CodecLz4 0.4.0. No Manifest exists for the requested graph. Therefore
   Arrow cannot load and neither round-trip can run.

   The narrow correction is to restore
   `TranscodingStreams = "0.9.12, 0.10, 0.11"`. That was the pre-rewrite
   bound. A disposable control with that bound resolved both codec minima,
   EnumX 1.0.0, and TranscodingStreams 0.9.12. It loaded Arrow and passed
   complete LZ4 and Zstd write/read value round-trips. If 0.10 is an
   intentional floor, the alternative is to raise the codec floors to
   CodecLz4 0.4.1 and CodecZstd 0.8.1.

2. **LOW — the writer comment now contradicts the dependency declaration.**
   `src/ipc_write.jl:58-59` says TranscodingStreams is not a direct
   dependency. Commit `e97d9e1` made it a direct dependency. Remove or update
   the comment.

## Direct dependency audit

`Project.toml:29` declares TranscodingStreams directly. `src/Arrow.jl:48`
imports it directly, and line 55 binds `TS = TranscodingStreams`.
`src/ipc_write.jl:74,83,93,97` uses that alias. No executable reference to
`CodecLz4.TranscodingStreams` or `CLZ4.TranscodingStreams` remains.

## Assumptions and decisions

- I treated the exact versions in the prompt as a required joint resolution.
- I used a clean Tables `jq/scan` checkout at `3bfa6b6` for the isolated
  minimum and correction probes.
- I classified the false dependency comment as a finding because this commit
  changed the fact that the comment describes.
- This was a review-only task. I changed only this report and did not touch
  the pre-existing untracked files.

## Validation

- Exact requested minimum resolution — failed with an unsatisfiable
  TranscodingStreams constraint, which reproduces finding 1.
- Corrected 0.9.12-floor control — loaded Arrow; LZ4 and Zstd round-trips
  passed with two batches and full materialized-value equality.
- CodecLz4 0.4.1 / CodecZstd 0.8.1 / TranscodingStreams 0.10.0 control —
  loaded Arrow; both codec round-trips passed.
- `Arrow.TS === TranscodingStreams` and direct-dependency metadata check —
  passed.
- `julia --project=. -e 'using Pkg; Pkg.test()'` — exit 0; 325/325 core,
  4/4 threaded, and all four acceptance batteries passed.
- `git diff --check e97d9e1^ e97d9e1` — exit 0.

VERDICT: FINDINGS
