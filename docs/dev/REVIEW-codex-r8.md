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

# ArrowCore prove-out review — round 8

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-7 fixes recorded in `REVIEW-codex-r1.md` through
`REVIEW-codex-r7.md`. The design authority was `Arrow-redesign-report.md`
§9. This was a fresh adversarial pass over Core, the IPC and C Data examples,
their tests, and the README. Declared exclusions were kept excluded.
Unsupported and trusted boundaries were checked for honest documentation and
safe failure instead of being implemented.

1. **HIGH — legacy V4 compression bypassed the declared IPC compression
   exclusion and produced silent data corruption.** Arrow 0.17 marked
   experimental compression in `Message.custom_metadata` with the key
   `ARROW:experimental_compression`. Its body buffers start with an eight-byte
   uncompressed-length prefix. The verifier checked the metadata shape, but
   the decoder ignored this key and checked only the later
   `RecordBatch.compression` field. A valid V4 LZ4 stream for `Int64[42]` was
   accepted and materialized as `[8]`, which was the length prefix rather than
   the value. Fixed in `1a0d1f6`: every V4 record or dictionary message is
   scanned for the exact legacy key before body decoding and fails closed.
   The regression builds a real length-prefixed LZ4 frame. This matches the
   [Arrow 0.17.1 writer](https://github.com/apache/arrow/blob/apache-arrow-0.17.1/cpp/src/arrow/ipc/writer.cc#L167-L189)
   and the current C++ reader's handling for
   [record batches](https://github.com/apache/arrow/blob/72c7ecf98d815e307f409cb8d00e1bd53b7b641c/cpp/src/arrow/ipc/reader.cc#L762-L807)
   and
   [dictionary batches](https://github.com/apache/arrow/blob/72c7ecf98d815e307f409cb8d00e1bd53b7b641c/cpp/src/arrow/ipc/reader.cc#L903-L914).

2. **MEDIUM — interruption while waiting for guards could permanently strand
   an `OwnerRegion` in `CLOSING`.** After `forceclose!` won the `OPEN` to
   `CLOSING` transition, its guard-wait loop called the interruptible `yield`
   without an exception cleanup boundary. Task cancellation or
   `InterruptException` left the gate closed to new guards, did not run the
   release callback, and made every later close wait or time out. Fixed in
   `c305742`: the winning closer now restores its exact prior `OPEN` state on
   every exception before release starts. Once release starts, the existing
   exactly-once callback and `CLOSED` publication remain unchanged. A
   deterministic injected-wait regression proves that access and a later
   close both succeed and that release runs once.

3. **MEDIUM — C Data import accepted unknown and type-invalid schema flags.**
   `_import_field` read the three known bits only where it used them, so an
   integer schema with an unknown bit, `DICTIONARY_ORDERED`, or
   `MAP_KEYS_SORTED` imported and materialized successfully. The resulting
   Core `Field` silently discarded schema information that this validating
   adapter cannot represent. Fixed in `47082e9`: import rejects bits outside
   the current flag mask, dictionary ordering without a dictionary schema,
   and sorted-map keys on a non-map format. Regressions cover every class and
   prove exact schema and moved-array cleanup. The strict type-relevance rule
   agrees with
   [nanoarrow schema validation](https://github.com/apache/arrow-nanoarrow/blob/b27fd93d0f519cf1504190420e87b001406f4855/src/nanoarrow/common/schema.c#L1336-L1352).

4. **LOW — `heapregion` documented resize risk but omitted the immutable-byte
   requirement behind cached validation.** Same-size mutation does not move a
   Julia vector, but it can invalidate a cached semantic certificate. A
   borrowed valid Date64 buffer was certified, mutated to an invalid value,
   and then accepted from the cache. Fixed in `ca5f02b`: the `heapregion`
   docstring and README now state that backing vectors must not be mutated or
   resized while their `ArrayData` or cached validation results remain in use.

5. **LOW — mapped and foreign backing had the same incomplete storage
   contract.** `mmapregion` documented external truncation but not same-size
   writes through another descriptor or process. Such a write reproduced the
   cached Date64 certificate failure. `foreignregion` documented declared
   extents but did not state that the producer must keep storage alive and
   unchanged. Fixed in `d120948`: both constructor docstrings and the README
   now state the full lifetime and immutability preconditions. These are
   explicit zero-copy trust boundaries; runtime mutation detection would
   require copying or a different ownership model.

## Scope decisions and withdrawals

- No additional defect was found in canonical lifecycle delegation,
  sole-closer ownership, publish-after-build C export registration, callback
  and reaper serialization, or IPC cursor serialization.
- No additional C Data defect was found after strict schema-flag validation.
  Fresh nanoarrow full validation accepted every unmodified mapped export and
  sliced layout, and a nanoarrow-produced Int64 array imported, materialized,
  and released. A 500-export concurrent release/reaper stress had no failure,
  leak, or stranded registry root.
- No additional IPC mapping or validation defect was found. A 20,000-case
  mutation pass accepted and materialized 795 cases; all 19,205 rejected cases
  failed cleanly. Real writer probes covered every mapped primitive, temporal,
  decimal, null, list, fixed-size, map, and dictionary family.
- A possible exact `bodyLength` equality check was withdrawn. Arrow body
  padding is not fully declared by buffer metadata, and the README explicitly
  excludes canonical padding checks. Requiring equality would reject valid
  streams with a larger alignment policy.
- The [C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
  permits consumers to ignore flags. This adapter does not retain opaque C
  schema state, so it now takes the strict current-version policy: it accepts
  the three known flags only where their meanings apply.
- View/ListView/REE semantic work, padding and unused-bit checks, current IPC
  compression and endian normalization, file footer/index support, facade
  work, native foreign-thread C callbacks, and the other README exclusions
  remain out of scope. The 32-bit ABI branch was source-inspected but not
  executed on the available 64-bit host.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 290/290 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including the real legacy V4 compression rejection, resource limits, body
  authority, dictionary snapshots, and serialized cursor checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including strict
  schema-flag rejection, exact failed-import cleanup, move semantics, source
  pins, malformed-topology cleanup, and empty-registry checks.
- The C Data suite also passed with `--check-bounds=yes` and with
  `--threads=4` on the final tree.
- A bounds-checked flag probe tried every mask from 0 through 15 plus both
  signed Int64 extremes on primitive, map, and dictionary schemas. It also
  placed unknown bits on nested list and dictionary-value fields. Every valid
  combination imported, and every invalid combination performed exact cleanup.
- All round-8 changes are confined to `core/`. Each logical change is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: FINDINGS
