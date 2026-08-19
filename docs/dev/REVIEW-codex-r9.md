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

# ArrowCore prove-out review — round 9

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-8 fixes recorded in `REVIEW-codex-r1.md` through
`REVIEW-codex-r8.md`. The design authority was `Arrow-redesign-report.md`
§9. This was a fresh adversarial pass over Core, the IPC and C Data examples,
their tests, and the README. Declared exclusions were kept excluded.
Unsupported and trusted boundaries were checked for honest documentation and
safe failure instead of being implemented.

1. **HIGH — C Data import armed the copied owner before ownership moved from
   the source array.** `ForeignOwner(arr)` registered its finalizer before
   `from_c_data` nulled the source `ArrowArray.release` field. An interrupt in
   that handoff left two live structs with the same producer callback. A
   deterministic producer probe finalized the copy once while the source
   remained live, then releasing the source invoked the callback a second
   time. A foreign producer need not make that duplicate call safe. Fixed in
   `4ece8bd` and completed in `b38df35`: owner construction is now unarmed,
   and only the moved copy is then armed. The source `release` field is the
   authoritative ownership marker, including if a task-delivered exception
   lands immediately after its NULL store. Failure after the move releases
   through the copied owner exactly once; failure before it leaves the source
   as the sole owner. The regression interrupts immediately after the move and
   proves exact root, callback, and source-pin cleanup.

2. **MEDIUM — interruption could permanently strand an export release
   callback in `RELEASING`.** Array and schema callbacks changed their control
   byte from `LIVE` to `RELEASING` before recursive work, but had no rollback
   boundary. A real SIGINT after the claim left the public release callback
   non-NULL, the root counter unchanged, and the control byte at `RELEASING`.
   A retry then treated that state as inert, so the registry root, native
   allocations, and source pins could never be reaped. This is also an ABI
   boundary: the [C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
   gives release callbacks a void signature, so an exception cannot be
   reported to the consumer. Fixed in `2427c5b` and completed in `4f3af27`:
   claims allocate and publish their rollback state before mutation; the
   exception boundary starts before the claim, including task-delivered
   cancellation; a failed recursive transaction rolls its node back to
   `LIVE`; completed descendants remain marked by NULL callbacks and are
   skipped on retry; and the C entrypoint defers SIGINT and retries internally
   until the one consumer call completes. Regressions cover failure directly
   after the claim, one-shot entrypoint retry, and failure after one of two
   children has completed. A real post-fix SIGINT probe reached `RELEASED`,
   set the callback to NULL, reaped both roots, and closed the source.

3. **MEDIUM — `reap!` removed roots before their cleanup was interruption-safe.**
   The reaper popped every zero-count `ExportedRoot` from the registry and
   only then freed its malloc ledger and released its source guards. A real
   SIGINT after the pop made the roots unreachable; a 5,000-root probe left
   4,902 guards stranded even though the registry was empty. The same
   pop-before-cleanup sequence existed in direct discard and published-build
   failure cleanup. Fixed in `a4055b4` and completed in `4f3af27`: a root now
   stays registry-visible with one cleanup claim until all resources are gone.
   Its rollback boundary and preallocated claim slot exist before the state
   mutation, including for task-delivered cancellation. Each native free and
   pin release removes its ledger entry first, so an unexpected failure can
   reset the claim and resume without a double free. Registry removal is the
   final step, with SIGINT deferred across the transaction. The same helper
   now owns reaping, discard, and published-build failure cleanup. Regressions
   inject failure after the claim and after the first native free. A real
   SIGINT was delivered only after cleanup completed, and eight concurrent
   reapers cleaned 200 roots and source guards exactly.

## Scope decisions and withdrawals

- No additional defect was found in Core layout validation, ownership gates,
  guard/close ordering, finalization, cache publication, or unsafe access.
- No additional IPC defect was found in byte verification, framing, body
  authority, dictionary state, or cursor serialization. A deterministic
  20,000-case mutation pass accepted and fully materialized 1,374 cases and
  rejected 18,626 cases through checked validation or overflow paths.
- No additional C Data defect was found in publish-after-build visibility,
  canonical recursive topology, source-pin deduplication, move semantics, ABI
  geometry, schema flags, or import validation after the three fixes above.
- A persistent internal invariant failure in a void release callback cannot be
  reported to C. The callback deliberately does not return false success; its
  retry loop is not presented as recovery from a permanently broken invariant.
- The README's implementation claims still match the current code. Foreign
  allocation extents and C strings remain trusted declarations at the stated
  in-process ABI boundary.
- View/ListView/REE semantic work, padding and unused-bit checks, current IPC
  compression and endian normalization, file footer/index support, facade
  work, native foreign-thread C callbacks, and the other README exclusions
  remain out of scope. The 32-bit ABI branch was source-inspected but not
  executed on the available 64-bit host.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 290/290 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including framing, metadata limits, body authority, dictionary snapshots,
  and serialized cursor checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including the new
  import-handoff, callback-transaction, partial-descendant, and retryable
  cleanup regressions.
- The C Data suite also passed with `--check-bounds=yes` and with
  `--threads=4`. Core and IPC passed with `--check-bounds=yes`.
- Focused post-fix probes used real SIGINT delivery and exact post-mutation
  task cancellation at import, release, and cleanup claims. A four-thread
  stress used eight reapers over 200 native roots and returned the shared
  source guard count to zero.
- All round-9 changes are confined to `core/`. Each logical fix is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>`
  trailer.

VERDICT: FINDINGS
