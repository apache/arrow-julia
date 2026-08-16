# ArrowCore prove-out review — round 16

Scope: fresh review of `9aa74c3` and `e5486fc`, which added the bounded
Footer-Block preflight before optional-EOS classification. The constrained
GC-reachability memory model remains unchanged.

## Numbered findings and dispositions

1. **LOW — the integrated preflight allocated 48 bytes after warm-up.** The
   fixed-table and Buffer-vector walk itself allocated zero bytes, but
   `_verifyblockframes` constructed a second whole-file `BufferSlice` even
   though `readfile` already held the same slice. This made the exact
   zero-allocation claim false by a constant 48 bytes on Julia 1.12.6. Fixed
   in `949efe4`: `_verifyblockframes` now accepts and reuses `readfile`'s
   existing slice. For 0, 1, 2, 10, and 100 record Blocks, its post-warm-up
   allocation now exactly equals the pre-existing `_validateblockindex`
   allocation, with zero incremental bytes from the preflight.

## Checked without another finding

- Every Footer Block is bound to its aligned continuation prefix, exact framed
  metadata length, Message body length, expected header kind, nested
  `DictionaryBatch.data` RecordBatch, and complete declared Buffer vector
  before EOS selection. Buffer offsets, lengths, checked ends, body
  containment, and nonempty ordering match the lazy decode cursor.
- Variadic counts introduce no separate body extents; all variadic buffers are
  still entries in `RecordBatch.buffers`. Nonempty variadic layouts remain an
  intentional lazy refusal. Compressed Buffer lengths are wire lengths that
  include the eight-byte compression prefix. Missing or empty Buffer vectors,
  zero-length entries, empty schemas, and omitted default-zero body lengths
  remain accepted where full decode accepts them.
- The preflight's table, slot, reference, vector, and alignment rules match
  `verify_ipc_metadata`. Focused Arrow.jl 2.x files passed with dictionaries,
  nested lists, nullable integers, zero-row columns, and uncompressed, LZ4,
  and ZSTD bodies. No preflight-only rejection of a fully accepted decode was
  found.
- The walk has fixed table depth. A Buffer count must have `16n` real metadata
  bytes before iteration. Disjoint Block extents bound aggregate scanning by
  indexed file size, and checked arithmetic covers vector and Buffer ends.
  A 500,000-Buffer probe was linear and allocation-free in the frame walker;
  tiny metadata with a maximal count and overflowing Buffer ends failed
  cleanly.
- The two forged regressions pin the intended fixes without rebuilding
  history. On `c4d2487`, both `_rejects(readfile(...))` assertions would fail
  because only Footer tuple arithmetic preceded EOS selection. `9aa74c3`
  rejects the Footer-only lie through the Message body length. `e5486fc`
  rejects the coordinated Footer/Message lie through the final Buffer end.

## Assumptions and decisions

- Extra body alignment padding remains valid. Bytes excluded consistently by
  the Footer, Message, and all declared Buffer extents are not payload.
- “Accepted decode” means metadata verification, header-kind checks, and full
  record or dictionary decoding all succeed. Preflight acceptance followed by
  a later semantic refusal is intentionally allowed.
- I treated the zero-allocation requirement as applying to the integrated
  preflight, not only `_verifyblockframe`. I fixed the duplicate slice instead
  of weakening the claim or adding a version-sensitive allocation assertion.
- No Core type, lifecycle state, guard, revocation, atomic, interruption path,
  export, or dependency changed. The five pre-existing untracked files were
  not touched.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 252/252 Core checks and 4/4
  threaded-cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed.
- `julia --project=. --startup-file=no core/examples/ipc_write.jl`: passed
  after `949efe4`, including both forged-EOS regressions and 2.x interop.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including its
  four-thread child.
- `julia --startup-file=no core/test/trim_compile_tests.jl`: 6/6 passed; the
  trimmed binary compiled and exited successfully.
- Focused allocation probe before `949efe4`: 48 incremental bytes for 0, 1,
  2, 10, and 100 Blocks. After `949efe4`: 0 incremental bytes for every case.
- Both scoped fix commits and `949efe4` carry the required Codex co-author
  trailer. `git diff --check` passed.

VERDICT: FINDINGS
