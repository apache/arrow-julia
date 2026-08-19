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

# ArrowCore prove-out review — round 15

Scope: the ten round-14 commits `9ad953e` through `c4d2487`, with fresh eyes
on their fixes and tests. The constrained memory model remains final: Core
buffer validity is GC reachability only. This review added no guard,
revocation, lifecycle state, `Threads.Atomic`, or interruption machinery.

## Numbered findings and dispositions

1. **MEDIUM — optional-EOS classification trusted Footer Block arithmetic
   before the indexed Message proved which bytes were data.** `readfile`
   checked Block tuple extents against the Footer, then used their maximum end
   to classify the final marker-shaped eight bytes. A forged Footer could
   shorten the last record Block by eight bytes. A no-EOS file whose terminal
   `Int64` was `0x00000000ffffffff` then opened with `dataend` eight bytes
   short. Lazy batch access rejected the inconsistent Block, so no incorrect
   value escaped, but open-time classification was wrong. Arrow.jl 2.8.1 still
   read the row. Fixed in `9aa74c3` and completed in `e5486fc`: before EOS
   selection, a bounded, zero-copy preflight binds every Footer Block to its
   continuation prefix, Message body length, expected DictionaryBatch or
   RecordBatch header, nested dictionary RecordBatch, and every declared wire
   buffer extent. A coordinated forgery of both Footer and Message body
   lengths now fails because the buffer table still proves that the excluded
   bytes are data. The preflight allocates zero bytes after warm-up. It does
   not require exact body use, so valid extra alignment padding remains
   accepted. Full metadata verification, decoding, and configured per-batch
   resource limits remain lazy. Regressions cover Footer-only and coordinated
   forgeries plus a valid omitted-default, zero-body Block.

## Checked without another finding

- EOS, no-EOS, and marker-shaped-terminal-data files classify correctly in
  this reader and remain readable by Arrow.jl 2.8.1. Exact half-open contact
  at the schema end, adjacent Block boundary, data end, EOS start, and Footer
  start is accepted. Schema overlap, Block overlap, escape, and arithmetic
  overflow fail closed. Zero-length bodies and the largest aligned Int32
  metadata length preserve their intended boundary behavior.
- Arrow.jl 2.8.1 writes the required physical terminal offset for zero-row
  Utf8, Binary, List, Map, large-offset, and empty dictionary-value arrays.
  Normal offsets are four bytes and large offsets are eight bytes. All tested
  first/later partitions and uncompressed, LZ4, and ZSTD forms decode. C Data
  export still materializes a rooted terminal zero, and import still rejects a
  NULL offset pointer even for zero rows.
- Stream publication rollback is exact for key overflow, caller-struct store
  failure, schema-result store failure, batch-result store failure, and error
  buffer allocation failure. Control allocation/free counts, both registry
  counts, output slots, `nextindex`, and retry behavior remain consistent.
  `StreamOwner` starts inert, registration failure frees only the copy, the
  source-null move precedes rearming, and explicit/finalizer release races have
  one winner.
- Writer Schema validation complements Core validation. Core owns native
  endianness and schema metadata checks; the adapter recursively checks field
  UTF-8, metadata, mapped descriptors, and wire shape. Decoded 2.x schemas
  create a distinct Core `Field` per occurrence, including legal aliased
  FlatBuffer tables, so the repeated-identity refusal does not reject decoded
  interoperable schemas.
- The C format parser accepts valid boundary forms, including exactly 128
  distinct union ids and multibyte timestamp timezones. Multibyte bytes in
  grammar positions, invalid UTF-8, embedded NULs, duplicate/overlong union
  lists, and non-ASCII integer spellings fail as `ValidationError`s. No valid
  form in the focused matrix was over-rejected.
- Round-14 README claims and the mmap reachability test remain accurate. The
  mmap test now keeps only a slice through collection and drops all mapping
  references before path deletion. No round-14 assertion was weakened.

## Assumptions and decisions

- Foreign C allocation extents and producer callbacks remain trusted ABI
  declarations. Caller-owned C structs remain live during calls. Calls on one
  stream do not overlap and run on Julia-attached threads.
- Ordinary exceptions are in scope. Asynchronous interruption, process exit,
  external mmap mutation or truncation, and post-release C access remain out
  of contract.
- I preserved lazy record decoding and its per-batch metadata, body, and
  allocation limits. The new open-time preflight reads only the fixed tables
  needed to prove Block framing and body coverage. It does not copy metadata
  or decode arrays.
- Body length need not equal the greatest declared buffer end. Extra alignment
  padding is valid, so the preflight requires containment, alignment, and
  non-overlap rather than equality.
- The available host is 64-bit little-endian. The 32-bit ABI branch was
  inspected but not executed. No public export surface or dependency was
  added. All changes remain inside `core/`. No push, rebase, or amend was
  performed. Pre-existing untracked issue notes and `mytestdata.arrow` were
  not touched.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 252/252 Core checks and 4/4
  four-thread cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed all
  framing, verification, compression, dictionary, resource-limit, and
  pull-concurrency checks.
- `julia --project=. --startup-file=no core/examples/ipc_write.jl`: passed all
  Core and Arrow.jl 2.x stream/file round trips plus Block, schema, empty
  offset, zero-body, and both forged optional-EOS regressions.
- `julia --startup-file=no core/examples/cdata.jl`: passed all format,
  ownership, registry, publication, failure-path, and four-thread checks.
- `julia --startup-file=no core/test/trim_compile_tests.jl`: 6/6 checks passed.
  JuliaC `--trim=safe` compiled and the produced binary exited successfully.
- Focused Block probes covered twelve targeted malformed envelopes, 20,000
  random envelopes for the first fix, and 30,000 one-to-five-byte metadata
  mutations for the completed preflight. All rejected cases failed cleanly.
  Mixed nested/dictionary 2.x files passed with no compression, LZ4, and ZSTD.
- Focused C-stream injection covered every requested publication failure and
  500 explicit-release/finalizer races. Focused C-format checks covered 51
  valid boundary forms, 81 multibyte grammar placements, and invalid UTF-8 or
  NUL placements. `git diff --check` passed. Both fix commits carry the exact
  required Codex co-author trailer.

VERDICT: FINDINGS
