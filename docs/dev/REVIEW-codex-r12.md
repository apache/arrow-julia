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

# ArrowCore prove-out review — round 12

Scope: the four commits after the round-11 clean verdict on branch
`core-rewrite`: IPC buffer compression, concrete `ReleaseAction`, removal of
the old interruption machinery, and JuliaC `--trim=safe` support. This review
also covered their interactions with the prior Core, IPC, and C Data code. The
current implementation was checked against the Arrow
[columnar format](https://arrow.apache.org/docs/format/Columnar.html),
[Schema.fbs](https://raw.githubusercontent.com/apache/arrow/main/format/Schema.fbs),
and
[Message.fbs](https://raw.githubusercontent.com/apache/arrow/main/format/Message.fbs).

## Numbered findings and dispositions

All findings below were fixed and verified. No material correctness, safety,
conformance, lifecycle, trim, or dead-scaffolding finding remains.

1. **HIGH — the compression feature/version gate was inverted.** The schema
   verifier rejected the standard `COMPRESSED_BODY` feature, while record and
   dictionary `BodyCompression` metadata could be accepted under V4 even
   though that field was introduced with V5. Fixed in `6bd5963`: feature 2 is
   accepted, compression is rejected under V4, and both record and dictionary
   batches use the same checked codec path. V5 streams from Arrow.jl 2.x that
   omit feature 2 remain an explicit, documented compatibility exception.

2. **HIGH — declared decompressed size did not bound actual allocation.** The
   old path copied the compressed payload and used a growable transcoding
   output. A forged small prefix could therefore inflate a large frame before
   the exact-size check failed. Fixed in `6bd5963`: decoding now reads directly
   from a guarded body slice into one exact-sized output vector. The declared
   size is checked and charged before allocation. `-1` remains a zero-copy raw
   subslice. A nonzero wire buffer with prefix zero must still contain a valid
   frame whose output is exactly empty.

3. **HIGH — the allocation budget reset for each batch cursor.** Multiple
   retained record or dictionary batches could each spend the full configured
   limit. Fixed in `6bd5963`: one reader-owned `AllocationBudget` now charges
   metadata work and all successful decompressed outputs across the complete
   eager read. A failed decode refunds only its unescaped output claim.

4. **MEDIUM — LZ4 output length did not prove one complete frame.** A frame
   missing its footer, or two concatenated LZ4 frames, could have the expected
   output length and pass. Fixed in `6bd5963`: a bounded `LZ4F_decompress` loop
   now requires the end-of-frame result, exact compressed input consumption,
   and exact output size. ZSTD uses an exact-destination one-shot DCtx decode.

5. **MEDIUM — corrupt LZ4 input could lose the native context pointer.** The
   prior wrapper could clear its pointer on a codec error before cleanup freed
   the context. Fixed in `6bd5963`: the reader owns raw lazy LZ4 and ZSTD
   context pointers, resets and reuses them, disarms them before native free,
   and closes them in `readstream`'s `finally` on every exit path.

6. **HIGH — `CcallRelease` leaked its owned argument when conformance
   verification failed.** The producer callback ran, `verify_null_at` threw,
   and the free step was skipped while `forceclose!` still committed `CLOSED`.
   Fixed in `213daf5`: callback execution and verification are covered by a
   `finally` that frees an owned argument. A non-nulling callback regression
   proves the verification error, exact deallocation, cleared action, terminal
   `CLOSED` state, and inert later close.

7. **HIGH — initial finalizer-registration failure lost ordinary exception
   cleanup.** Removing the interruption retry block also removed the catch that
   closed a newly transferred resource when finalizer registration failed.
   Fixed in `213daf5`: the initial registration wrapper force-closes the still
   unescaped, unguarded region before it rethrows. The busy-finalizer rearm path
   stays separate. A focused failure seam proves exactly-once cleanup.

8. **MEDIUM — the old C release entry could retry forever on an ordinary
   persistent error.** Its nested catch/retry machinery survived the explicit
   interruption-contract change and could turn an invariant failure into an
   infinite loop. Fixed in `054d3a6`: a C callback makes one rollback-safe
   attempt. A pre-commit failure restores `LIVE`; a later explicit callback can
   resume without repeating completed children. The obsolete `_entry`/`_impl`
   hook chains and all unused `after_*`, factory, allocator, pin, publish, and
   foreign-call injection paths were removed. Injection seams retained in the
   tree each support a surviving ordinary-error test.

9. **LOW — MapClaim ownership races lacked true multi-threaded coverage.** The
   original tests drove two-owner interleavings with cooperative tasks but did
   not contend on the CAS from multiple worker threads. Fixed in `213daf5` and
   `d23fbab`: tests cover a failed owner restoring `LIVE` for a waiter and 100
   four-thread rounds with 16 simultaneous contenders. Each round performs one
   unmap and publishes terminal `RELEASED`. There is no semantic ABA: only the
   CAS winner can change `RELEASING` back to `LIVE` after a failed native
   release, every waiter rereads before CAS, and `RELEASED` is terminal.

10. **LOW — trim and documentation rules were not fully reflected in source.**
    Temporal access used a runtime-selected load type, `descriptorname` was not
    annotated like the other closed ladders, Struct scalar docs still mentioned
    `NamedTuple`, and old README sections contradicted the new interruption and
    compression contracts. Fixed in `054d3a6`, `1bc466c`, `abf4aef`, and
    `e8f6bd8`: load widths are literal branches; all six ladders are `@inline`
    and cover all 22 current descriptor leaves; Struct scalars are consistently
    `Vector{Pair{String,Any}}`; the new release API exports only
    `ReleaseAction` and `CcallRelease`; and stale callback, compression, and
    interruption prose is removed.

## ReleaseAction and ordinary-exception judgment

- `forceclose!` has one CAS winner. Its `finally` always clears the release
  action and publishes a new `CLOSED` generation, including when the action
  throws. Other closers only wait for `CLOSED` or a restored `OPEN` state.
- The C Data importer first owns its copied ABI storage with a free-only
  action. It nulls the producer's source release pointer to commit the move,
  then upgrades the action to call, verify, and free the copied producer
  structure. Failure after the move arms that same full action before cleanup.
- `verify_null_at` is checked after the foreign callback. Its owned argument is
  freed independently of callback or verification success.
- The `Ptr{Cvoid}` finalizer entry intentionally drops errors because nothing
  may unwind through the GC's C finalizer runner. This is safe within the stated
  contract: `forceclose!` independently clears the action and commits `CLOSED`,
  and `CcallRelease` independently frees its owned argument.
- Asynchronous interruption remains out of contract. No retry or SIGINT
  deferral machinery remains. Ordinary exceptions retain rollback or a
  committed exactly-once cleanup owner. Atomic state uses `@atomic` struct
  fields and CAS loops; `Threads.Atomic` and atomic RMW are absent.

## Assumptions and decisions

- V5 compressed streams from Arrow.jl 2.x may omit schema feature 2. This
  compatibility exception is deliberate and documented. V4 compression is
  always rejected.
- The allocation limit is a conservative reader-wide budget for metadata and
  owned decompressed output. It is not an exact account of all Julia runtime
  allocations. Fixed per-reader native codec workspace is outside this budget.
- Foreign pointer extents remain trusted ABI declarations because the C Data
  interface does not expose allocation bounds. C callbacks for one exported
  tree remain serialized and run on Julia-attached threads, as documented.
- Process termination and process-fatal allocation failure are outside ordinary
  exception recovery. The 32-bit ABI branch was inspected but not executed on
  the available 64-bit host.
- Test-only release actions and counters were kept internal. Only the concrete
  boundary types needed by adapters were added to the public export surface.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 318/318 Core checks and
  704/704 four-thread lifecycle, cache, and MapClaim checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed all
  framing, schema/version, record and dictionary compression, prefix, empty,
  allocation-budget, decompression-bomb, frame-completion, context-cleanup, and
  cursor checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed all ABI, export,
  import, move, release, conformance, finalizer, failure-cleanup, and reaper
  checks.
- `julia --startup-file=no core/test/trim_compile_tests.jl`: 6/6 harness checks
  passed. JuliaC `--trim=safe` produced zero verifier errors, zero verifier
  warnings, and a binary that ran to exit 0.
- All round-12 changes are confined to `core/`. Each review commit ends with
  `Co-Authored-By: Codex <codex@openai.com>`.

VERDICT: CLEAN
