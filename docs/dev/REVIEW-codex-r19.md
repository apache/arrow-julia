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

# ArrowCore prove-out review — round 19

Date: 2026-08-14

Scope: round-18 fixes `6a05250..2c5b055`, interrupted take-1 commits
`cd786c6` and `aa9a801`, and round-19 dispositions through `3b96b5e`. The
Tables authority remained the local `jq/scan` branch at
`5b4986c0e49260bbcc0965f8386f98b32b4821fd`.

Take 1 ended after committing its two fixes and before writing a review. Both
commits were re-judged. Their direction is retained: `cd786c6` correctly moved
message, codec, and dictionary-plan failures before body requests, and
`aa9a801` correctly distinguished planned requests from permitted physical
over-read. The follow-up findings below complete those changes.

## Findings and dispositions

1. **Aggregate zero-column row counts could wrap or truncate.** Whole-file and
   ranged scans added wire `Int64` batch lengths into host `Int` without a
   guard. `_fulltable` had the same wrap, which could mask the differential
   failure. An offset-only window also used `typemax(Int64)` as an unlimited
   sentinel and could omit a later batch. Disposition: fixed in `de48462`.
   Aggregate results now accept exactly `typemax(Int)`, reject the next row,
   and represent `limit=nothing` explicitly.

2. **The ranged preflight still allowed metadata-deterministic failures after
   a planned body request.** The missing checks covered layout-derived buffer
   minima, compressed prefixes and required payloads, canonical empty IPC
   offsets, exact sparse-union children, fixed Null/Union counts, nested child
   extents, and non-nullable fields whose slots are provably fully covered.
   Disposition: fixed across `4c6f0fc`, `57b5762`, `ded6405`, and `3b96b5e`.
   Wanted dictionary plans and final-window record plans now finish before the
   first dedicated body request. Nullable masking, extra backing, List/Map and
   Union data-dependent coverage, skipped fields, excluded batches, unneeded
   dictionaries, and statistics-pruned records remain lazy.

3. **Fetch documentation still used physical “never fetched” claims.** A
   default tail read can cover an entire small file, and coalescing can cross
   unrequested bytes. Disposition: fixed in `4c6f0fc`, `57b5762`, and
   `25db0ca`. The README, design, and source comments now say that skipped or
   pruned data causes no dedicated/planned range and is not parsed or decoded;
   configured requests may still physically over-read it.

No defect remains in the other scoped changes. Positional filter references,
renames, duplicate selections, and `validate=false` match the active Tables
authority. Extreme residual windows preserve the literal apply/finish
contract. NaN, signed-zero, and mixed-precision statistics pruning remains
one-sided. The trust tests cover both whole-file and ranged paths. The official
statistics value-schema flexibility remains accepted.

## Assumptions and decisions

- The constrained GC-reachability model remains final. No lifecycle or
  concurrency machinery was added.
- “Before body request” covers every failure determined by the planned schema,
  FieldNodes, buffer table, and codec metadata. Checks that require compressed
  prefixes, payload bytes, offsets, type ids, or validity bits run after those
  bytes are fetched.
- A Stage-A intermediate result above `typemax(Int)` is unaddressable and fails
  closed even if a residual filter could later reduce it.
- Footer-only schema authority and trusted-for-completeness statistics remain
  intentional. Statistics-pruned record metadata is not separately requested,
  parsed, or validated; tail/coalescing may over-read it.
- Only tracked files under `core/` changed. Existing unrelated untracked files
  were not modified.

## Validation

- `julia --startup-file=no core/test/runtests.jl` — 252/252 core and 4/4
  threaded-cache tests passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl` — passed.
- `julia --project=. --startup-file=no core/examples/ipc_write.jl` — passed.
- `julia --startup-file=no core/examples/cdata.jl` — passed, including the
  four-thread child.
- `julia --project=. --startup-file=no core/examples/scan_ranges.jl` — all
  Stage-A, byte-range, statistics, corruption, budget, and trust checks passed.
- `julia --startup-file=no core/test/trim_compile_tests.jl` — 6/6; compile and
  produced-binary run passed.
- Focused probes covered 81,840 independent window cases, positional-filter
  differential matrices, Float32/Float64 and NaN pruning, exact request logs,
  every added preflight branch, and seven valid lazy masking/extra-backing
  boundaries.
- `git diff --check` — passed.

VERDICT: CLEAN
