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

# ArrowCore prove-out review — round 5

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-4 fixes recorded in `REVIEW-codex-r1.md` through
`REVIEW-codex-r4.md`. The design authority was
`Arrow-redesign-report.md` §9. This was a fresh adversarial pass over Core,
the IPC and C Data examples, their tests, and the README. Declared exclusions
were kept excluded. Unsupported and trusted boundaries were checked for
honest documentation and safe failure instead of being implemented.

1. **HIGH — IPC used native-endian generated getters before rejecting an
   unsupported big-endian host.** `framemessages` verified the FlatBuffer with
   byte-wise little-endian loads, but then called the repository's older
   generated FlatBuffers code before `readstream` checked `Base.ENDIAN_BOM`.
   Those generated getters use native-endian `unsafe_load`. On a big-endian
   host, a valid little-endian root offset or body length could therefore be
   byte-swapped into an out-of-bounds address before the documented
   unsupported-host error. Fixed in `59cd812`: the host gate now runs at the
   `framemessages` boundary, before prefix loads or any generated getter. A
   dependency-injected host-BOM regression proves the order on little-endian
   CI. The README now states this boundary.

## Scope decisions and withdrawals

- No additional defect was found in canonical lifecycle delegation,
  guard/close ordering, the publish-after-build C export registry, source
  pins, moved-node release, explicit reaping, or IPC cursor serialization.
- A possible unsigned-index Metadata V4 defect was withdrawn. The official
  schema permits unsigned dictionary index types in V4; the V4/V5 union
  layout difference is irrelevant because IPC union mapping is excluded.
- Empty sliced-array buffer rules differ between implementations. Core's
  strict `offset + length` geometry agrees with the C Data requirement and
  arrow-rs. The C++ validator relaxes fixed-width storage when logical length
  is zero. This is an interoperability strictness choice, not an accepted
  unsafe path or a contradiction of the stated prove-out contract, so it was
  not reported as a defect.
- Foreign C pointer extents and NUL termination remain trusted ABI
  declarations, as documented. View/ListView/REE semantic work, padding and
  unused-bit checks, IPC compression and endian normalization, file
  footer/index support, facade work, native foreign-thread C callbacks, and
  the other README exclusions remain out of scope and fail closed where
  stated.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 279/279 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including the new unsupported-host ordering gate, the four-thread cursor
  gate, bounded metadata expansion, checked body spans, and dictionary
  snapshot and certificate checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including ABI
  layout, publish/reap ordering, move semantics, malformed-topology cleanup,
  source pins, and registry-empty checks.
- Randomized Core probes ran about 10,000 nested-slice comparisons, 120,000
  validation candidates, and 100,000 Decimal cases. All 57,426 accepted
  validation candidates materialized safely, and every Decimal result matched
  a BigInt oracle.
- A separate 20,000-case mutation pass over a complex real IPC stream accepted
  1,203 valid mutations. Every accepted batch materialized without a later
  failure. Focused C Data round trips covered Boolean, binary, large binary,
  list, large list, and sliced primitive/list/struct arrays.
- All round-5 changes are confined to `core/`. Each logical change is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: FINDINGS
