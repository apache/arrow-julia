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

# ArrowCore prove-out review — round 27

Date: 2026-08-15

Scope: commit `85dddea6113ca657669cb4821564a18dd154ad52` under
`core/`, compared with parent `ae5fe80b41874f097bf2c47b6f25786142123f0b`.

## Result

One resource-accounting and fail-fast finding remains. The generated shape
walkers, runtime primitives, generator layouts, wrapper getters, and
shadowing discipline are otherwise clean.

## Finding

1. **MEDIUM — unsupported metadata versions now traverse the full attacker-
   directed graph before the adapter rejects them.** In the parent Message
   wrapper, the root table was visited and charged, then the accepted-version
   check ran before header traversal
   (`85dddea^:core/examples/ipc_read.jl:475-490`). The parent Footer wrapper
   used the same order
   (`85dddea^:core/examples/ipc_write.jl:849-865`). The new wrappers call the
   complete generated walkers first (`core/examples/ipc_read.jl:192-199`,
   `core/examples/ipc_write.jl:838-846`). Those walkers recurse through the
   Message header or Footer schema before they return
   (`core/metadata/Verifier.jl:619-629`, `697-730`).

   This is observable on rejected input. I changed an otherwise valid Schema
   Message and Footer to version value `2` (V3) and set the verifier reserve
   limit to exactly one table charge, 2048 bytes. The current commit returned:

   ```text
   message_type=AllocationLimitError
   message_error=metadata-directed allocation budget exceeded while visiting Schema
   footer_type=AllocationLimitError
   footer_error=metadata-directed allocation budget exceeded while visiting Schema
   ```

   The same probe on the parent returned the immediate errors:

   ```text
   message_type=ValidationError
   message_error=ValidationError("invalid IPC FlatBuffer: unsupported metadata version 2 (only V4/V5 are accepted)")
   footer_type=ValidationError
   footer_error=ValidationError("invalid IPC FlatBuffer: unsupported footer version 2 (only V4/V5 are accepted)")
   ```

   Both probes exited 0. The regression changes charge order, work performed,
   exception class, and diagnostic text. A large unsupported-version graph can
   now consume the configured object or reserve budget before the constant-
   time version gate. The traversal stays bounded, and the input still
   rejects, but the old fail-fast DoS policy does not.

   The root cause is the new monolithic generated root walk. Adapter-specific
   version policy can run only after that walk returns. The durable fix is a
   generated staged root API. It must verify the root/table and version field,
   expose that verified value to the adapter gate, then resume generated
   reference traversal without visiting or charging the root twice. A
   hand-written wrapper precheck for vtable slot 0 would restore order but
   would put schema geometry back outside the generator.

   No repository or `arrow-testing` check greps the changed messages. This
   does not remove the behavioral and exception-priority regression.

## Rejection-parity audit

The old walker checks and their current equivalents are:

| Old surface | Current coverage |
| --- | --- |
| Byte ranges and byte-wise scalar loads | `VerifierRuntime.jl:112-131` |
| Table/vtable geometry, object bounds, slot width/alignment, and forward references | `VerifierRuntime.jl:141-185` |
| Boolean and enum domains | `VerifierRuntime.jl:187-199` |
| String bounds, NUL, reserve charge, and UTF-8 | `VerifierRuntime.jl:201-215` |
| Vector count, object accounting, element bounds/alignment, reserve charge, and table entries | `VerifierRuntime.jl:217-260` |
| Required `KeyValue.key/value` strings | `Verifier.jl:428-434` |
| Per-Type table shell plus scalar, boolean, enum, string, and vector fields for tags 1–26 | `Verifier.jl:20-426`, with dispatch at `465-584` |
| Dictionary id, optional Int index type, ordered flag, and dictionary-kind enum | `Verifier.jl:445-454` |
| Field name/nullability, required Type tag/value, dictionary, children, and metadata | `Verifier.jl:465-589` |
| Schema endianness, required fields, metadata, and Feature enum vector | `Verifier.jl:600-608` |
| RecordBatch length, 16-byte FieldNode/Buffer vectors, compression table, and 8-byte variadic counts | `Verifier.jl:640-667` |
| DictionaryBatch id, required RecordBatch data, and delta flag | `Verifier.jl:678-686` |
| Message root, version domain, required header union, body length, and metadata | `Verifier.jl:697-737` plus the adapter subset gate |
| Footer root, version domain, required schema, 24-byte Block vectors, and metadata | `Verifier.jl:619-637` plus the adapter subset gate |

The old `_vtype` made `Int.bitWidth` and `Decimal.precision` shape-required
(`85dddea^:core/examples/ipc_read.jl:356-394`). The generated shape walker
does not. Therefore, the generated root walker alone is not literally a
rejection superset. Omitted values become zero through the generated getters.
The complete input adapters still reject them:

- `coretype` maps all direct and nested type tables, including dictionary
  index types (`core/examples/ipc_read.jl:309-365`, `425-428`).
- `validateschemafield` validates every descriptor recursively
  (`core/examples/ipc_read.jl:472-503`).
- Integer width and Decimal precision/width domains reject zero
  (`core/ArrowCore.jl:863-876`).
- Stream, full-file, and ranged-file schema paths all run that validation
  before they expose or use a schema (`core/examples/ipc_read.jl:1005-1010`,
  `core/examples/ipc_write.jl:1297-1302`,
  `core/examples/scan_ranges.jl:814-819`).

Focused probes confirmed shape acceptance followed by full-reader rejection
for omitted direct `Int.bitWidth`, omitted `Decimal.precision`, an omitted
dictionary-index `Int.bitWidth`, and an omitted Int width in a Footer schema.
The resulting descriptor errors were the expected integer-width or
decimal-precision errors.

The old RecordBatch compression subtable checks remain. The generated
`verify_BodyCompression` validates table geometry, codec domain `{0,1}`, and
method domain `{0}` (`core/metadata/Verifier.jl:640-646`). Corrupt codec and
method probes both rejected. The generated walker is stricter because it also
visits, depth-checks, and charges this table.

Message and Footer now both enforce `root >= 4` explicitly
(`core/metadata/Verifier.jl:632-636`, `733-737`). The old Footer omitted the
explicit comparison, but its table checks still rejected roots 0–3: zero had
a zero vtable offset, and 1–3 were misaligned. The acceptance set is unchanged.

## Runtime-port fidelity

The primitive formulas and their internal order match the deleted code:

- `_vvisit!` increments and checks the object count before its 2048-byte
  charge.
- `_vstring` does reference, alignment, bounds, NUL, `128+n` charge, then
  UTF-8 validation.
- `_vvector` does reference, length bounds/alignment, count ceiling and count
  charge, data bounds/alignment, then the `256+1024n` reserve charge.
- Missing optional strings and vectors still return before a charge. Invalid
  vector geometry still counts entries before it fails and does not apply the
  final vector reserve.
- `_vtablevector` still charges the vector before it validates child entries.
  All 35 generated table walkers use `_vtable`, `_vvisit!`, then depth.

Three generated-walk changes are stricter or change rejected-input priority:

- A compressed one-column RecordBatch now counts six objects and reserves
  9728 bytes. The parent counted five and reserved 7680 bytes because it did
  not visit `BodyCompression`.
- Generated depth includes the Message/Footer root-to-header edge. A minimal
  one-Int schema needs `max_nesting_depth=3`, versus 2 in the parent.
- An unknown Field Type tag now fails in the generated union ladder before a
  target-table visit. The old `_vtype` visited and charged that unknown target
  before it failed. This changes an already-rejected path, not its safety.

I did not classify these three changes as findings. They follow the stated
uniform every-table policy and do not weaken a check. The unsupported-version
ordering in the finding is different: it removes the parent's constant-time
adapter gate and permits a complete otherwise-valid graph walk.

`_vvector`'s no-context default is not reachable from real input verification.
Every generated call passes the bounded `VerifyContext`. The no-context calls
are fixture or mutation locators in the examples. The new permissive default
therefore cannot bypass reader, writer, file, or ranged-scan limits.

## Generator and wrapper audit

- One `slotmap` drives getters/builders and verifiers
  (`core/tools/fbsgen.jl:183-197`, `350-366`, `420-431`, `496-516`). Field
  slots are 0, 1, Type tag 2, Type value 3, 4, 5, 6. Message slots are version
  0, header tag 1, header value 2, body length 3, metadata 4.
- Required unions require a present tag slot, a nonzero tag, and a present
  value reference (`core/tools/fbsgen.jl:509-536`). Type dispatch covers tags
  1–26. Message dispatch handles Schema, DictionaryBatch, and RecordBatch;
  Tensor, SparseTensor, and unknown tags fail closed
  (`core/metadata/Verifier.jl:478-584`, `709-726`).
- `enumdomain` masks signed values to the declared wire width
  (`core/tools/fbsgen.jl:211-217`). A synthetic negative-member probe passed
  for 1-, 2-, 4-, and 8-byte bases, including each signed minimum and `-1`.
- Computed layouts are Block 24, Buffer 16, and FieldNode 16. Bindings,
  builders, getters, and verifier vector widths agree
  (`core/metadata/File.jl:76-102`, `core/metadata/Schema.jl:662-684`,
  `core/metadata/Message.jl:20-42`,
  `core/metadata/Verifier.jl:626-627`, `662-663`).
- The version getter returns a verified `MetadataVersion.T`. Converting it
  through `Int64` and then `Int16` preserves every declared value. The wrappers
  still accept only values 3 and 4.
- Header dispatch is total after verification. `_schemafeatures` successfully
  read `FlatBuffers.Array{Feature.T}` as `[0,1,2]`; a wire value 3 failed before
  the getter ran. The V4/features coupling remains.
- `_blocktuples` uses the generated names `offset`, `metaDataLength`, and
  `bodyLength`, and widens all three values to `Int64`
  (`core/examples/ipc_write.jl:829-848`).

## Shadowing and constraints

An AST and source audit found no accidental bare use of `Union`, `Int`,
`Bool`, `Type`, `Date`, or `Time` in the two new metadata files. Runtime
annotations use `Base.Int` and `Base.Bool`. Emitted binding annotations use
`Base.Int`, `Base.Bool`, `Base.Type`, or `Core.Type`. Remaining bare names in
generated bindings are intentional metadata declarations or dispatch values.
The new files use comments, not docstrings that can lower a bare shadowed
name. No `Meta.eval`, `Core.eval`, or `include_string` path consumes generated
strings.

The commit changes only `core/`. It does not change the constrained
GC-reachability model, the four `_of` ladders, or dependency files. The Tables
development dependency remains as-is. The five pre-existing untracked files
were not modified.

## Assumptions and decisions

- I judged omission of Int width and Decimal precision at complete stream,
  full-file, and ranged-file input boundaries, as the request directs. I did
  not treat the internal shape-only entrypoints as public acceptance APIs.
- I treated stricter every-table object/depth accounting as intentional.
- I treated unsupported-version fail-fast and exception priority as part of
  the old DoS-accounting behavior. The current change is therefore a finding
  even though both revisions reject the input.
- Generator correctness is scoped to the three pinned schemas and their
  documented FlatBuffers subset. I did not treat hypothetical unsupported IDL
  shapes as findings.
- This was a review request. I added this report only. I did not make a
  product fix or commit.

## Validation

- `julia --startup-file=no core/test/trim_compile_tests.jl` — exit 0; 6/6.
- `julia --startup-file=no core/test/runtests.jl` — exit 0; 325/325 Core and
  4/4 threaded-cache tests.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_read.jl`
  — exit 0.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_write.jl`
  — exit 0.
- `julia --startup-file=no core/examples/cdata.jl` — exit 0, including the
  four-thread child.
- `julia --project=core/conformance --startup-file=no core/examples/scan_ranges.jl`
  — exit 0 on an unchanged retry. The first process entered the known idle
  scheduler stall and was interrupted after more than two minutes; it exited
  130.
- `julia --project=core/conformance --startup-file=no core/conformance/corpus.jl`
  — exit 0; 275 pass / 0 fail / 36 skip.
- `julia --project=core/conformance --startup-file=no core/conformance/oracle.jl`
  with local image `arrow-conformance-oracle:latest` — exit 0; 170 pass / 0
  fail / 43 skip.
- `julia --startup-file=no core/tools/fbsgen.jl core/metadata/fbs core/metadata`
  — exit 0. The following `git diff --exit-code core/metadata` — exit 0.
- Focused omission, enum-vector, union-tag, root-offset, negative-enum,
  struct-layout, runtime-accounting, and parent/current version-order probes
  all exited 0.

VERDICT: FINDINGS
