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

# ArrowCore prove-out review — round 10

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-9 fixes recorded in `REVIEW-codex-r1.md` through
`REVIEW-codex-r9.md`. The design authority was `Arrow-redesign-report.md`
§9. This was a fresh adversarial pass over Core, the IPC and C Data examples,
their tests, and the README. Declared exclusions were kept excluded.
Unsupported and trusted boundaries were checked for honest documentation and
safe failure instead of being implemented.

1. **HIGH — mmap ownership could be lost or released twice across constructor
   handoffs.** `_mmapregion` could arm an `OwnerRegion` finalizer and then
   directly unmap in its catch when the factory failed before returning. The
   later finalizer called `munmap` again. The successful `mmap` result also
   crossed a pre-handler gap before owner construction, and transient
   interruption during the first cleanup attempt could lose the only mapping
   owner. Fixed in `e6197f2`, `8aafa54`, `d5903a6`, and `02de76e`: catch and
   finalizer share a serialized, retryable mapping-release state; successful
   unmap is checked before publishing `RELEASED`; constructor and callback
   cleanup use a no-escape retry; and the mmap-to-finalizer handoff is one
   deferred transaction. Regressions cover factory failure after finalizer
   arming, interruption before and after unmap commit, failure immediately
   after mmap, and interrupted explicit close. `bad7d1b` also made rollback
   use a local completion token so it cannot steal a later owner's claim.

2. **HIGH — generic lifecycle ownership mutations preceded their cleanup
   handlers.** Guard increment, `withguard` handoff, close claim, close
   callback ownership, manual-finalizer rearm, and `OwnerRegion` finalizer
   registration each had an interruption window that could leak a guard,
   strand `CLOSING`, lose a release callback, or lose the finalizer backstop.
   Fixed in `2809a0a`, `bad7d1b`, `fa1d0fc`, and `47e1987`: each mutation now
   transfers into an installed handler with SIGINT deferred, every pre-release
   rollback is no-escape and retryable, local tokens prevent ABA claim theft,
   and callback entry remains the exactly-once commit point for arbitrary
   generic release callbacks. Focused regressions inject interruption after
   guard acquisition, close claim, before callback entry, during busy
   finalizer rearm, and after finalizer registration.

3. **HIGH — C Data export construction could lose native allocations, source
   pins, or published roots.** Pin acquisition completed before the pin was
   recorded. `malloc` completed before its cleanup handler. Schema publication
   preceded the API catch, and array publication was omitted from rollback.
   Cleanup itself could be interrupted after freeing a raw C struct, making a
   raw-pointer retry either leak the other root or dereference freed memory.
   Fixed in `72aa6f3` and `7df0af2`: pins are recorded directly in their root;
   allocation-to-ledger registration is one owned transaction; the exact
   public `to_c_data` frame owns caller-visible result and stable registry-key
   slots; and private or two-root rollback clears raw slots before no-escape
   cleanup by key. Regressions fail after allocation, after each pin acquire,
   after each publication, and after the array root has already been freed.

4. **HIGH — C Data node release published a reaper-eligible root before its
   public callback state was coherent.** `_finish_node!` separately decremented
   `remaining`, marked the control released, and nulled the C release pointer.
   Failure between stores could leave a live public callback pointing into a
   root the reaper could free. A later exception after coherent commit could
   also make the callback retry a raw pointer already freed by the reaper.
   Fixed in `7eb1b96` and `07707d5`: all three stores form one rollbackable
   transaction under the registry lock; a Julia-side committed token transfers
   before the native claim is cleared; claim rollback is no-escape; and the C
   entrypoint never retries or touches native memory after commit. Regressions
   fail after every store and reap the root before throwing after commit.

5. **MEDIUM — C Data import cleanup did not own all producer callbacks and
   failure paths.** `ForeignOwner` construction preceded the schema cleanup
   boundary. An exception from schema `finally` bypassed moved-array cleanup.
   Producer array and schema release callbacks were one-shot calls, so an
   interruption before the producer marked `release = NULL` could close the
   generic gate while leaving producer resources live. Fixed in `fd3d088`:
   schema cleanup is installed before owner construction; an outer catch owns
   failures from both the import body and schema `finally`; the moved array
   keeps one persistent writable C struct; and C-specific void callback
   cleanup retries interruption until that struct publishes NULL. Generic
   `OwnerRegion` callback semantics remain exactly-once. Regressions cover
   owner-construction failure, post-schema-finally interruption, moved-owner
   cleanup, and producer callback interruption after NULL publication.

6. **MEDIUM — the IPC cursor claim and advance were not interruption-atomic.**
   `nextbatch!` claimed `pulling` before its `try/finally`, so cancellation
   could wedge the stream busy. Cancellation after incrementing `nextindex`
   could also silently consume a batch that never reached the caller. Fixed in
   `4756b5f`: the exact public method owns caller-side claim, advance, and old
   index slots; the claim and speculative advance run with interruption
   deferred; failure restores the index before releasing the pull gate; and
   both cleanup steps are retryable without repeating a completed mutation.
   Regressions interrupt immediately after claim and immediately after advance,
   then prove that the same first batch is returned exactly once.

## Scope decisions and withdrawals

- No additional defect was found in Core layout descriptors, staged
  validation, cache publication, bounds-checked access, or semantic accessors.
- No additional IPC defect was found in metadata verification, framing, body
  authority, resource arithmetic, dictionary snapshots, or mapped layout
  conformance. A fresh 10,000-case mutation probe produced no unexpected
  exception class for accepted or rejected input.
- No additional C Data defect was found in ABI geometry, canonical topology,
  move semantics, mapped format strings, flags, or pointer-table validation
  after the fixes above.
- A candidate finding about invalid UTF-8 hidden below a null parent was
  withdrawn. Full recursive child validation matches the Apache C++ and Rust
  reference validators; it was not a defensible conformance defect.
- The stale C Data example description was corrected: the demo has a nullable
  floating-point column, not nullable integers.
- View/ListView/REE semantic work, padding and unused-bit checks, current IPC
  compression and endian normalization, file footer/index support, facade
  work, native foreign-thread callbacks, background reaping, and the other
  README exclusions remain out of scope. The 32-bit ABI branch was inspected
  but not executed on the available 64-bit host.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 334/334 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including framing, verifier limits, dictionary snapshots, concurrent cursor
  stress, and the new interruption rollback checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including export
  construction, release commit, import cleanup, producer callback, and reaper
  regressions.
- All three suites also passed with `--check-bounds=yes`. The C Data suite
  additionally passed with `--threads=4`.
- All round-10 changes are confined to `core/`. Every logical fix is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>`
  trailer.

VERDICT: FINDINGS
