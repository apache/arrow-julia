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

# ArrowCore prove-out review — round 6

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-5 fixes recorded in `REVIEW-codex-r1.md` through
`REVIEW-codex-r5.md`. The design authority was `Arrow-redesign-report.md`
§9. This was a fresh adversarial pass over Core, the IPC and C Data examples,
their tests, and the README. Declared exclusions were kept excluded.
Unsupported and trusted boundaries were checked for honest documentation and
safe failure instead of being implemented.

1. **MEDIUM — empty-vector alignment was both over-strict and incomplete.**
   `_vvector` required the nominal element area of every vector to be aligned,
   even when its length was zero and there was no element to access. The
   official C++ 21 integration stream
   [`generated_null_trivial.stream`](https://github.com/apache/arrow-testing/blob/master/data/arrow-ipc-stream/integration/cpp-21.0.0/generated_null_trivial.stream)
   has an empty vector of 16-byte `Buffer` structs whose nominal element area
   is only four-byte aligned, so the adapter rejected the valid zero-row Null
   batches. Fixed in `74e9ef7`: element alignment is required only for a
   nonempty vector. Re-review then found that the relaxed check also let a
   malformed empty table vector place its four-byte length word at an
   unaligned address. The byte verifier accepted it, after which the older
   generated `children` getter reached `unsafe_wrap` and threw `ArgumentError`.
   Fixed in `759092d`: every vector length word must be four-byte aligned,
   independently of element alignment. End-to-end regressions accept the
   official empty struct-vector form and reject the malformed empty table
   vector before any generated getter runs.

2. **MEDIUM — absent IPC metadata values were silently changed to empty
   strings.** `_vkeyvalue` required `KeyValue.key` but treated
   `KeyValue.value` as optional. `coremetadata` then mapped an absent value to
   `""`, so malformed absence and an explicit empty value became
   indistinguishable. The canonical Arrow C++ reader
   [requires both fields](https://github.com/apache/arrow/blob/main/cpp/src/arrow/ipc/metadata_internal.cc#L1283-L1299).
   Fixed in `1a7ac47`: the verifier now requires the value string. A regression
   rejects the absent field while preserving an explicit empty string.

3. **MEDIUM — an absent `Schema.fields` vector was accepted as an empty
   schema.** `_vschema` treated a missing vector as zero fields, although a
   valid empty schema uses a present zero-length vector and the canonical
   Arrow C++ reader
   [requires `Schema.fields`](https://github.com/apache/arrow/blob/main/cpp/src/arrow/ipc/metadata_internal.cc#L1441-L1456).
   Fixed in `f14ef19`: the vector reference is now required. Regressions keep
   the present empty form valid and reject the absent form.

4. **LOW — two Core docstrings described future adapters as implemented.**
   `RecordBatchSource` said the C-stream importer and facade already presented
   its pull shape. `RecordBatch` said C Data and partition iteration already
   crossed the batch boundary. The README correctly excludes the C stream
   interface, facade, and partition layer, and the C Data example maps one
   column rather than a batch stream. Fixed in `3386c6f` and `2b9c29a`: the
   docstrings distinguish the implemented IPC path from the target design.

## Scope decisions and withdrawals

- The remaining FlatBuffer nullability audit was clean. `RecordBatch.nodes`
  and `RecordBatch.buffers` may be absent for a zero-column batch; a nonempty
  schema forces both through checked cursor consumption. Empty
  `Field.children` is intentionally optional. A missing dictionary index type
  retains the format's signed-Int32 default. Other optional fields either have
  valid defaults or lead to a declared unsupported-feature rejection.
- No additional defect was found in canonical lifecycle delegation,
  guard/close ordering, the publish-after-build C export registry, source
  pins, moved-node release, explicit reaping, or IPC cursor serialization.
- No additional unsafe Core or C Data path was found. Supported dereferences
  remain behind guarded, checked regions. C export pins source regions before
  pointers escape, and import failure cleanup uses the canonical private
  topology.
- Foreign C allocation extents, pointer-table extents, NUL termination, and
  producer callback behavior remain trusted ABI declarations. Borrowed Julia
  vectors must not be mutated or resized, and mapped files must not be
  externally truncated. These are documented scope boundaries.
- View/ListView/REE semantic work, IPC compression and endian normalization,
  file footer/index support, facade work, native foreign-thread C callbacks,
  and the other README exclusions remain out of scope and fail closed where
  stated. The 32-bit ABI branch was source-inspected but not executed on the
  available 64-bit host.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 279/279 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including both empty-vector alignment regressions, required metadata values,
  required schema fields, the cursor gate, bounded metadata expansion, body
  spans, and dictionary snapshot checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including ABI
  layout, publish/reap ordering, move semantics, malformed-topology cleanup,
  source pins, and registry-empty checks.
- All 25 C++ 21 integration streams in the adapter's declared type subset
  decoded and fully materialized. The separate official shared-dictionary
  stream also decoded and materialized.
- A deterministic 20,000-case mutation pass over a nested, dictionary-bearing
  stream accepted 1,302 cases. Every accepted stream materialized. All 18,698
  rejected cases threw `ValidationError`; the first pre-fix pass exposed the
  empty-vector length-alignment gap described above.
- A four-thread probe overlapped 1,600 C exports, callbacks, and `reap!` calls.
  It had no failures and left the registry empty. Focused C Data round trips
  covered Boolean, binary, large binary, list, large list, and sliced struct
  arrays.
- All round-6 changes are confined to `core/`. Each logical change is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: FINDINGS
