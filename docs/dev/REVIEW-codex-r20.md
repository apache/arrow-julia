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

# ArrowCore prove-out review — round 20

Date: 2026-08-14

Scope: the six round-19 fixes `de48462..3b96b5e`, plus the round-20
disposition `6f3afc4`. The Tables authority remained the local `jq/scan`
branch at `5b4986c0e49260bbcc0965f8386f98b32b4821fd`.

## Finding and disposition

1. **LOW — planned-request wording was not complete.** The main README,
   design contract, `skipfield!` comment, `RangedFile` docstring, and
   `ipc_read.jl` comment correctly allowed tail and coalescing over-read. One
   design summary and several counting-source comments and labels still said
   that skipped, window-excluded, dictionary, or statistics-pruned bytes were
   “never fetched” or fetched only for the decode set. Those statements
   generalized fixture-specific request-log observations into a physical I/O
   guarantee. Disposition: fixed in `6f3afc4`. General claims now use planned
   or dedicated ranges. Physical observations are limited to the exact test
   settings that assert them. No reader behavior changed.

Because this round found an issue, it does not meet the zero-finding
convergence bar even though the issue is now fixed.

## Closing checks

- **Row-count guards are clean.** `_addscanrows` accepts exactly
  `typemax(Int)` and rejects the next row before conversion or addition. Both
  apply paths and zero-column `_fulltable` use it. `_batchwindow` represents
  `limit=nothing` with an explicit Boolean, so an offset-only scan cannot omit
  a later batch.
- **The ranged preflight boundary is clean in both directions.** Complete
  candidate metadata is parsed first. Wanted dictionary plans and every
  final-window record plan validate schema-derived minima, FieldNodes, buffer
  geometry, codec metadata, fixed null counts, fully covered null contracts,
  and required dictionary ids before the first dedicated body request. Checks
  that need compressed prefixes or payloads, offsets, type ids, validity bits,
  or reachable child slots remain after body fetch. The IPC verifier rejects
  unsupported View/ListView/REE schema tags before block planning. No
  whole-file versus ranged acceptance drift was found.
- **Request claims are now consistent after `6f3afc4`.** README, design,
  source comments, and test labels distinguish planned requests from permitted
  physical over-read. The remaining “were not fetched” text is a runtime
  `SparseBody` containment error, not a fetch-policy claim.

## Assumptions and decisions

- The constrained GC-reachability memory model remains final. No lifecycle,
  concurrency, cache, or other machinery was added.
- “Before the first body request” means before the first dedicated planned
  body request. Head, tail, footer, or coalesced requests may physically
  over-read other bytes.
- Fixture request logs remain useful physical observations, but their labels
  must not state a stronger general API contract.
- A finding discovered and fixed in this round still makes the round a
  findings round under the explicit convergence rule.
- Only tracked files under `core/` changed. The five unrelated untracked files
  were not modified.

## Validation

- `julia --startup-file=no core/test/runtests.jl` — 252/252 core and 4/4
  threaded-cache tests passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl` — passed.
- `julia --project=. --startup-file=no core/examples/ipc_write.jl` — passed.
- `julia --startup-file=no core/examples/cdata.jl` — passed, including the
  four-thread child.
- `julia --project=. --startup-file=no core/examples/scan_ranges.jl` — all
  Stage-A, byte-range, statistics, corruption, budget, and trust checks passed
  after `6f3afc4`.
- `julia --startup-file=no core/test/trim_compile_tests.jl` — 6/6; compile and
  produced-binary run passed after `6f3afc4`.
- Focused row-count probes accepted `typemax(Int)` and rejected one more on
  whole-file, ranged, and `_fulltable` paths. The unlimited-window probe kept
  all three batches and both readers rejected the final unaddressable total.
- Focused preflight probes passed 8/8 valid lazy boundaries, rejected 2/2
  fully covered null-contract failures before a body request, and kept 4/4
  data-dependent failures after body access. An unsupported-schema probe made
  only head, tail, and footer requests before rejection.
- Active-source wording search found no remaining physical “never fetched”
  policy claim. `git diff --check` passed.

VERDICT: FINDINGS
