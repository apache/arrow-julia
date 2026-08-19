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

# ArrowCore prove-out review — round 11

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-10 fixes recorded in `REVIEW-codex-r1.md` through
`REVIEW-codex-r10.md`. The design authority was `Arrow-redesign-report.md`
§9. This was a fresh adversarial pass over Core, the IPC and C Data examples,
their tests, and the README. The README Honest status controlled the prove-out
scope. Declared exclusions were checked for honest failure and documentation,
not treated as missing production features.

## Numbered findings and dispositions

No material correctness, safety, conformance, or validation finding remained.

1. **LOW documentation — the interruption guarantee was implicit.** The code
   and fault-injection tests defined committed ownership handoffs, but the
   README did not state where the guarantee ends. This made the prove-out look
   either stronger or weaker than it is. Fixed in `eddd3c0`: the new
   Interruption safety section lists the atomic handoffs, defines their
   rollback-or-owned-cleanup guarantee, states the best-effort behavior outside
   those handoffs, and distinguishes finalizer-backed Julia owners from
   consumer-owned C exports. Per the round-11 instructions, this wording fix is
   not counted as a material finding.

## Interruption-class judgment

**The interruption class is closed for this prove-out.** No remaining material
window can be closed without requiring instruction-level exception atomicity
that Julia does not provide.

The current implementation makes the following committed handoffs
interruption-atomic:

- access-guard acquisition, transfer to `withguard`, and rollback;
- close claim, timeout or pre-callback rollback, callback-entry commit, and
  closed-state publication;
- successful mmap acquisition, finalizer ownership, and retryable munmap;
- C export allocation registration, source pins, root publication, release
  commit, and registered cleanup;
- C import source move, imported-owner finalizer setup, and producer cleanup;
- IPC single-puller claim, speculative cursor advance, and rollback.

At each handoff, an exception either restores the prior state or leaves the
resource under a committed cleanup owner. Generic release callback entry is the
at-most-once commit point because an arbitrary callback can partly free a
resource before it fails. A successful public return commits returned C
pointers or the returned IPC batch to the caller.

The remaining theoretical seams are the boundary between a native side effect
and a Julia state store, allocation failure while Julia builds bookkeeping, and
the final return-to-caller boundary. Julia has no primitive that atomically
combines `mmap`, `munmap`, `malloc`, `free`, or a foreign callback with Julia
state publication. SIGINT deferral protects bounded handoffs, not every machine
instruction. `OwnerRegion` and imported-owner finalizers backstop resources
after a Julia owner exists. Successful C exports instead remain registry-rooted
until the consumer calls their release callbacks and `reap!` runs. These limits
are now the declared contract, so individual instances are not findings.

## Scope decisions and clean areas

- No new defect was found in OwnerRegion state transitions, guard ordering,
  finalization, mmap ownership, BufferSlice geometry, runtime descriptors,
  layout validation, semantic caches, accessors, builders, or RecordBatch
  construction.
- No new IPC defect was found in FlatBuffer verification, resource charging,
  framing, message-body authority, exact node and buffer accounting,
  dictionary compatibility and snapshots, pending all-null dictionaries, or
  cursor ownership.
- No new C Data defect was found in ABI geometry, format mapping, canonical
  release topology, source pins, move semantics, import validation, producer
  cleanup, or registry reaping.
- The mapped behavior was checked against the current
  [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html)
  and
  [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
  Foreign allocation and pointer-table extents remain trusted because the ABI
  does not provide verifiable allocation bounds.
- View/ListView/REE semantics, padding and unused-bit checks, IPC compression
  and endian normalization, file footer/index support, the incremental IO
  framer, facade work, native foreign-thread C callbacks, background reaping,
  and the other README exclusions remain out of scope. Unsupported mapped
  stages still fail closed. The 32-bit ABI branch was inspected but not
  executed on the available 64-bit host.

## Assumptions and decisions

- The README contract for serialized C callbacks on Julia-attached threads
  remains in force.
- Foreign C producers follow the C Data release contract and keep their
  declared backing live and unchanged. Borrowed Julia vectors and mapped files
  remain unchanged as required by Honest status.
- Non-adversarial-thread use excludes abrupt process termination and arbitrary
  instruction-level exception injection.
- No code change was made because no material defect was found. The LOW
  contract correction was made directly. Unavoidable execution-model seams
  were not enumerated as separate findings.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 334/334 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including framing, verifier limits, dictionary snapshots, cursor stress, and
  interruption rollback checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including export,
  import, move, release, finalizer, failure cleanup, and reaper checks.
- All three suites also passed with `--check-bounds=yes`. The C Data suite used
  four Julia threads for that run.
- A fresh 30,000-case IPC metadata mutation pass accepted and materialized 546
  cases and rejected 29,454 with `ValidationError`. A separate 30,000-case body
  mutation pass accepted and materialized 10,142 cases and rejected 19,858
  cleanly. Neither pass produced an unexpected exception.
- Focused C Data checks round-tripped sliced primitive, Boolean, UTF-8, binary,
  list, struct, and dictionary arrays. A four-thread stress exported, released,
  and reaped 200 trees and left the registry empty.
- All round-11 changes are confined to `core/`. Each commit ends with the
  requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: CLEAN
