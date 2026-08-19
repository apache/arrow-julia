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

# Arrow.jl 3.0 code review — round 43

Date: 2026-08-17

Scope: exact fix commit `ca6c5c147d2cfb45130407a56b10e4f62fea6d8e`
on `core-rewrite`. Its parent,
`8f7a81fb82edfa07c6ccde4211044f07ba1fa7a3`, records round 42
against code commit `30066aa6bdc6a7a355b73beace1b84687d23869c`.
I used the manifest-selected Tables.jl checkout on `jq/scan` at
`d1fbb6eb577741688dba70039754166b51c1cdcc` as the authority.

## Result

Round 42 is closed. I found no issue of any severity.

The fix makes the empty-column declared domain match the container that the
facade can materialize for a valid mixed population. Temporal leaves below a
transparent REE or Union remain raw integers. A Dictionary preserves the
conversion state that reaches it. It does not restart conversion after a
wrapper. A plain top-level Dictionary still forwards temporal conversion.

The multi-child Union rule now matches Julia's `map(identity)` widening. The
empty and mixed `Union<Int64,Utf8>` populations both drop retained identity
under `=> Union{Integer,AbstractString}`. An Int-only population keeps it. I
accept this narrower-population result as inherent container widening.

All five required gates pass. The full round-38 through round-42 clean set
also passes.

## Findings

No findings of any severity.

## Closure of round 42

- `_postconvert` dispatches on the root descriptor at `src/table.jl:157-184`.
  Its only value conversions are Date, supported Timestamp units, Time,
  Duration, and root Dictionary delegation.
- `_declaredeltype` starts with conversion enabled at `src/table.jl:551`.
  REE and Union force it off at `src/table.jl:553-565`. Dictionary only passes
  the current state at `src/table.jl:556-558`. The flag is therefore monotone.
  A Dictionary below REE or Union cannot restore conversion.
- `_istemporalconv` and `_rawdeclaredbasetype` at `src/table.jl:573-578` cover
  the complete conversion set. Their `Int32` and `Int64` results match the
  Core storage methods at `src/ArrowCore.jl:1851-1867` and the descriptor
  widths at `src/ArrowCore.jl:592-598`.
- `_boundschema` uses the declared domain only for an empty pre-override
  column at `src/table.jl:602-610`. Nonempty columns use their actual
  `eltype`. The new declaration therefore controls exactly the prior split.
- The closing probe covered REE<Date32>, Union<Date32>, and
  Dictionary<REE<Date32>> on file, stream, and ranged paths. Under
  `=> Integer`, all 18 empty and nonempty cases kept exact retained identity.
  Under `=> Dates.Date`, all nine empty cases converted and dropped the field.
  All nine nonempty cases refused with `MethodError`, as did `Tables.finish`.
- The accepted REE<Dictionary<Date32>> and Union<Dictionary<Date32>> fixtures
  stayed raw `Int32` and retained under `=> Integer` on all six paths. A
  top-level Dictionary<Date32> declared a nonmissing Date domain, materialized
  `Vector{Date}`, and retained on all six empty and nonempty paths.
- The multi-child fold at `src/table.jl:560-565` uses pairwise
  `Base.promote_typejoin`. The actual Any-domain facade column uses
  `map(identity)` at `src/table.jl:636-642`. A closed 54-domain sweep checked
  157,464 three-domain combinations with no order or associativity mismatch.
  A second sweep checked 8,000 ordered value populations with no mismatch
  against the actual `map(identity)` container type.
- The heterogeneous Union probe covered empty, mixed, and Int-only populations
  on all three paths. Empty and mixed populations dropped 3/3 each. Int-only
  populations retained 3/3. Values and output element types matched the
  Tables authority.
- Tables makes its no-op decision from the actual container `eltype` at
  `~/.julia/dev/Tables/src/scan.jl:525-530`. It applies that rule from
  `Tables.finish` at `~/.julia/dev/Tables/src/scan.jl:545-561`. The dedicated
  authority probe passed 170/170 assertions.
- The new built-in pins at `test/facade_tests.jl:809-865` assert the raw REE
  declaration, the heterogeneous Union declaration, and the empty/nonempty
  end-to-end decisions. The independent probes add the full three-path and
  composition coverage.

## Clean regression sweep

- ListView, every Decimal width, and every Interval unit retained 27/27 in
  each state. Their natural rewrites refused 54/54 in each state with
  `ArgumentError` and no published bytes.
- Dictionary<REE<Binary>> retained 3/3 in each state. Its rewrites refused
  6/6 in each state. One-child and compatible Union controls also passed for
  empty and nonempty populations.
- Orphan Dictionary validation passed 36/36 zero-field assertions and 16/16
  semantic controls. Unknown ids, deltas, duplicate ids, and inner
  RecordBatch mismatches still reject before a filter or window can hide them.
- Frame integrity, file and ranged budgets, charge-before-fetch, `limit=1`
  laziness, later-block early stop, and every `OpNode` control passed 40/40.
  The file header charge stayed 4,688 bytes. The ranged footer charge stayed
  5,984 bytes. One ranged header stayed 4,776 bytes. The exact 10,760-byte cap
  succeeded, and the former 10,672-byte cap rejected.
- The small-list matrix passed Tables authority 6/6, retention 6/6, and
  rewrites 12/12. The expanded matrix passed shapes 42/42, conversions 30/30,
  retention 30/30, rewrites 60/60, parity 5/5, replacement errors 24/24, and
  Bool preservation 6/6.
- Direct Binary, FixedSizeList, Struct, and Map parity passed for empty and
  nonempty inputs. Nested values and exact recursive schemas passed. Nested
  imposition passed 24/24 complex transitions plus the sliced, empty,
  nonnullable, replacement, and pre-publication refusal controls.
- The direct zero-field matrix passed 57/57. Reject windows passed 10/10.
  Overflow and residual controls passed 18/18. Pathological-name controls
  passed 17/17.
- Allocation stayed flat from 10,000 to 1,000,000 declared rows: file 5,488
  bytes, stream 1,792 bytes, and ranged 18,880 bytes at both sizes.
- A 64-level declared-type walk completed. An 80-level IPC schema rejected at
  the accepted metadata depth boundary.

## Assumptions and decisions

- I used `Tables.apply`, `Tables.finish`, and `Tables.bind` from the live
  manifest path as the semantic authority. `Manifest.toml:105-109` records a
  path dependency, not a Git revision. I therefore recorded and checked the
  live Tables SHA before using it.
- I treated layouts accepted by Core validation and the IPC adapters as the
  supported input domain. Dictionary-encoded Dictionary values are not in
  that domain. The validator rejects them at `src/ArrowCore.jl:978-984`. I
  did not use that invalid double-Dictionary shape as end-to-end evidence.
- I accepted Int-only heterogeneous-Union retention. The empty declaration is
  conservative for a valid population that uses all declared child domains.
- I treated retained identity as the exact recursive Arrow descriptor, names,
  nullability, field metadata, and schema metadata.
- I treated rewrite refusal as clean only when it occurred before any output
  bytes were published.
- The host is 64-bit and used Julia 1.12.6. All probes ran from scratch
  directories. I made no product or test changes.
- Legacy probes were changed only in `include_string`: two SHA pins were
  updated, one obsolete private helper argument was removed, and the old
  heterogeneous empty expectation was changed from keep to conservative drop.
- The six pre-existing untracked files remain present and unmodified. This
  review document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 268/268, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, and the compiled binary run passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 skip.
- Round-43 closing composition probe — exit 0; all file, stream, ranged,
  authority, temporal-unit, Dictionary, and heterogeneous-Union counts above
  passed.
- Tables authority probe — exit 0; 170/170.
- Union join closure probe — exit 0; 157,464/157,464 with no order or
  associativity mismatch. Population widening probe — exit 0; 8,000/8,000.
- Adapted round-42 declared-layout and Union probe — exit 0; every exotic
  layout, rewrite refusal, Union control, and depth control above passed.
- Round-40 scan closure probe — exit 0; 40/40.
- Dictionary adversarial probe — exit 0; 36/36 zero-field assertions and
  16/16 semantic controls.
- Small-list, expanded-facade, composite-parity, nested-descriptor, and nested
  imposition probes — exit 0; every count above passed.
- Direct zero-field matrix — exit 0; 57/57.
- Reject-window controls — exit 0; 10/10.
- Allocation-flatness probe — exit 0; zero growth on file, stream, and ranged
  paths.
- Overflow and residual probe — exit 0; 18/18.
- Pathological-name probe — exit 0; 17/17.
- `git diff --check` — exit 0.

VERDICT: CLEAN
