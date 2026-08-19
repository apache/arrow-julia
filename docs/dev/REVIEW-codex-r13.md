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

# ArrowCore prove-out review — round 13

Round 13 take 1 was lost to an architecture redirect; its two commits targeted
machinery that was later deleted. Take 2 reviewed this constrained model but
was lost to a network drop before it wrote a report. Its recovered GC
regression is `dcf8dfc`.

Scope: `dcd43a8` and `dcf8dfc` on branch `core-rewrite`, plus their interactions
with the existing Core, IPC, and C Data prove-outs. The maintainer's constrained
memory model is accepted as the design boundary: reachability is the only Core
validity mechanism, and post-release C Data access is caller-contract undefined
behavior.

## Numbered findings and dispositions

All findings below were fixed and verified. No material reachability, ownership,
registry, mmap, trim, concurrency, or stale-scaffolding finding remains.

1. **HIGH — non-empty regions could omit their only lifetime root.**
   `OwnerRegion(ptr, len)` still accepted `root=nothing`, a default inherited
   from the deleted release-action model. A live slice could then outlive the
   Julia allocation behind its pointer. Fixed in `028020c`: non-empty regions
   require a root. A construction regression rejects the rootless form. Every
   successful Core, mmap, IPC, and C Data construction path supplies its owner.

2. **HIGH — Core raw reads relied on compiler liveness instead of the Julia
   pointer contract.** The deleted `_guarded` helper invoked a closure but did
   not formally preserve the slice owner across `unsafe_load` or
   `unsafe_copyto!`. Fixed in `028020c`: `loadat` preserves its `BufferSlice`,
   and `slicebytes` preserves both source and destination through the complete
   raw-pointer window. The recovered `dcf8dfc` test forces collection during
   `loadat` and proves that the region root survives.

3. **MEDIUM — failed `ForeignOwner` finalizer registration leaked its native
   struct copy.** If registration threw after the inert copied `ArrowArray` was
   allocated, no returned owner guaranteed cleanup. Fixed in `642aecd`: the
   constructor claims and frees the inert copy before it rethrows. The injected
   registrar installs the finalizer and then fails; the test proves that later
   finalization is inert, the producer stays source-owned, and the source is
   released once.

4. **MEDIUM — owner allocation after `malloc` had an uncovered leak seam.**
   Copy initialization and `new` ran after native allocation but before a
   `ForeignOwner` or finalizer existed. Fixed in `e1317a5`: one catch owns every
   operation from copy initialization through Julia owner construction and
   frees the native block on failure. Process-fatal allocation failure remains
   outside the ordinary-exception contract; a catchable `OutOfMemoryError`
   follows this cleanup path.

5. **LOW — the remaining C Data concurrency and error claims lacked durable
   regressions.** The pop-first registry had no checked-in test with concurrent
   release callbacks and reapers. Explicit and finalizer release also lacked a
   true contention test, and the nonconforming producer path did not prove its
   one-free terminal state. Fixed in `e1317a5`: the standard C Data command now
   starts a four-thread child that races 2,000 registry roots across four
   releasers and three reapers, then races explicit release against finalization
   for 200 owners. Focused tests also verify that a producer which does not null
   its copied release field reports the error, frees once, and leaves later
   release attempts inert.

6. **LOW — region and trim documentation retained false or stale claims.**
   Source outside the two deliberate design-history passages still referred to
   guards and pins. The region shape omitted `alignment`; its alignment field
   was described as consulted but was dead; the README attributed a deletion
   rule to Mmap that its documentation does not state; and it described a
   deleted trim closure and an incomplete atomic set. Fixed in `c805a05`,
   `4722370`, and `f5f4ace`: current prose describes reachability, declared
   extents, mmap limits, the raw-load trim path, and all remaining atomics.
   `loadat` now uses the recorded base alignment plus the slice-relative offset
   when it selects an aligned load.

## Constrained-model judgment

- A `BufferSlice` owns its immutable `OwnerRegion`; the region owns its opaque
  root. Core preserves that chain over every raw dereference and copy. The IPC
  codec path preserves both the wire region and output across native decode.
- `mmapregion` roots the Mmap array itself. No in-tree code exposes or resizes
  that root. Empty and missing files fail through checked paths. The README
  states that resize, external mutation, and external truncation are outside
  the model.
- A moved C Data tree has one `ForeignOwner`. Its copied release field is NULL
  before the source-null commit and armed immediately after that commit. One
  atomic swap selects explicit release or finalization. Construction failures
  before the move leave the producer source-owned; failures after the move call
  the producer once and free the copy.
- Export publication roots each source before its pointer escapes. Release
  callbacks update `remaining` under `REGISTRY_LOCK`. Cleanup claims a finished
  root by popping it under the same lock, then performs only non-throwing frees.
  Concurrent reapers cannot claim the same root, and no callback can observe a
  popped live root.
- The remaining atomics have separate roles: `ArrayData` validation caches,
  `ReleaseCounter`, `IPCStream.pulling`, and `ForeignOwner.released`. The review
  found no lost update or lock-order defect in their current protocols.

## Assumptions and decisions

- The maintainer's constrained memory model is final for this round. I did not
  restore eager release, revocation, guards, region lifecycle state, or pins.
- Foreign C extents and producer callbacks are trusted ABI declarations. The
  caller does not race mutation or release during import. Post-release access
  is undefined behavior by contract.
- Ordinary exceptions are in scope. Asynchronous interruption, process exit,
  process-fatal allocation failure, external mmap truncation, and hostile
  producer behavior beyond the checked release-field rule are outside scope.
- The available host is 64-bit. I inspected but did not execute 32-bit ABI
  branches. I kept all new helpers and tests internal; the export surface did
  not grow.
- I added permanent multi-threaded C Data coverage because exclusivity is a
  concurrency claim. I did not add lifecycle machinery or synchronization to
  the Core load path.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 252/252 Core checks and 4/4
  four-thread cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed all
  framing, schema, compression, resource-limit, dictionary, pull-concurrency,
  truncation, and adversarial checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed all ABI, registry,
  export/import, move, exactly-once release, finalizer, failure-cleanup, and
  conformance checks. Its four-thread child passed the registry/release stress.
- The four-thread C Data child also passed five consecutive direct runs during
  stress stabilization.
- `julia --startup-file=no core/test/trim_compile_tests.jl`: 6/6 harness checks
  passed after the final Core change. JuliaC `--trim=safe` produced zero verifier
  errors, zero verifier warnings, and a binary that exited 0.
- A final stale-symbol scan found deleted lifecycle names only in the two
  allowed design-history passages and older review records. `git diff --check`
  passed. All round-13 edits are inside `core/`, and each review-fix commit has
  the required Codex co-author trailer.

VERDICT: CLEAN
