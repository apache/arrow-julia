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

# Arrow.jl 3.0 code review — round 50

Date: 2026-08-17

Scope: exact fix commit `aa2ff7977f73f12f9418b1c3d2e929b18c0dfc9d`
(`fix: resolve round 49 finding — field metadata rides the wrapper node`) on
`core-rewrite`. Its parent,
`75594ac20924462bd3d9d150670f333048d12944`, records the round-49 review of
exact code commit `98ecbc56c9841efc964ca4b6da75e81381841420`.
I reviewed only the round-49 fix diff and reran the full C-data, metadata,
interop, trim, conformance, ownership, release, and threaded regression
surface at the exact fix commit.

## Result

The round-49 finding is closed at the root. I found no issue of any severity.

Dictionary field metadata now exports on the outer wrapper. The dependent
value-type node receives no ordinary field metadata. This matches the Apache
C++ bridge and PyArrow 20.0.0. A real in-process pointer handoff confirms that
PyArrow imports Arrow.jl's metadata, type, and values. The reverse handoff
confirms that Arrow.jl preserves PyArrow's native field metadata and values.

Foreign producers may still annotate both nodes. Arrow.jl keeps all pairs in
wrapper-first order, including duplicate keys. Arrow.jl's own wrapper-only
export imports each pair exactly once. Raw checks pass for top-level and
nested dictionaries, composite dictionary values, union children, and map
entries. All five required gates pass.

## Findings

No findings of any severity.

## Closure of the round-49 finding

- `_export_schema!` treats only the dictionary's dependent value node as
  special at `src/cdata.jl:627-646`. It builds that node with
  `AC.dictvaluefield` at `:643-646`. `dictvaluefield` preserves the value type
  and real value children but creates no metadata at
  `src/ArrowCore.jl:1169-1176`.
- The current schema node always receives `f.metadata` at
  `src/cdata.jl:653-662`. A dictionary wrapper therefore receives ordinary
  field metadata. Its dependent value node receives NULL. The direct
  repository pins check both pointers and own round-trip identity at
  `test/cdata_battery.jl:1287-1301`.
- This matches Apache C++. `ExportField` emits `field.metadata()` on the field
  wrapper in
  [bridge.cc:183-191](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L183-L191).
  `ExportType` starts without field metadata at
  [bridge.cc:194-202](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L194-L202),
  and dictionary export uses it for the dependent value type at
  [bridge.cc:262-273](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L262-L273).
- A PyArrow 20.0.0 in-process C-pointer field import returned Arrow.jl's
  metadata as `{b'ordinary': b'ours'}`. A second full array import returned
  dictionary values `['lo', 'hi']`. The raw Arrow.jl schemas had a populated
  wrapper and a NULL dependent metadata pointer. The probes verified the
  schema move and explicitly completed the paired array lifetime.
- The reverse pointer handoff used PyArrow's native field and dictionary-array
  exporters. Arrow.jl imported metadata `ordinary => pyarrow` and values
  `Any["lo", "hi"]`. PyArrow's raw export had the same wrapper-populated,
  dependent-NULL shape.
- The unchanged round-49 ctypes probe also exits 0 with PyArrow 20.0.0. It
  confirms that PyArrow imports wrapper-only metadata, ignores ordinary
  value-only metadata, and uses dependent metadata for value-type extension
  information.
- `_import_field` reads the wrapper at `src/cdata.jl:1220-1226`, imports the
  dependent node at `:1242-1243`, and concatenates wrapper pairs before
  dependent pairs without deduplication at `:1248-1256`. The resulting Core
  field is built once at `:1259-1263`. A doctored PyArrow producer with both
  nodes populated retained all four pairs, including the same key on both
  nodes. The repository pin for this fallback is at
  `test/cdata_battery.jl:1302-1319`.
- An own-export probe used three ordered pairs, including a duplicate key.
  The wrapper contained those three pairs, the dependent node was NULL, and
  re-import returned the same three pairs once and in the same order.

## Adversarial schema-node audit

- Every real child field recurses through `_export_schema!` at
  `src/cdata.jl:630-641` and receives its own `f.metadata` at `:653-662`.
  Raw probes confirm this for list and struct children, sparse-union children,
  map entries, map keys and values, both REE children, and dictionaries at
  those child positions.
- A struct-valued dictionary exports metadata on the dictionary wrapper,
  NULL on the dependent struct node, and each struct child's metadata on that
  child's wrapper. Its own import preserves the parent metadata, child
  metadata, and values.
- A dictionary nested through a composite dictionary value is representable.
  An outer dictionary to struct to inner dictionary probe exports and imports
  with the same wrapper rules at both dictionary positions. A direct
  dictionary-of-dictionary value is not representable. Core rejects it at
  `src/ArrowCore.jl:978-984`.
- C streams use the same schema exporter at `src/cdata.jl:1484-1497` and the
  same field importer at `:1787-1796`. There is no second placement path.
- Core has one field-metadata slot at `src/ArrowCore.jl:521-526`. Import of a
  foreign dependent-node annotation preserves every pair but flattens its
  original node attribution. A later Arrow.jl export places those pairs on
  the wrapper. This is the stated fallback and not an own-round-trip defect:
  Arrow.jl's exporter never creates dependent-node pairs.

## Round-48/49 clean regression surface

- The exact guard-page child and an independent boundary family both exit 0.
  Exactly 1,048,576 readable non-NUL bytes followed by `PROT_NONE` refuses
  without a signal. NUL at offset 1,048,575 succeeds. NUL at offset 1,048,576
  and a readable limit-plus-one non-NUL buffer refuse. Empty, ASCII, and
  multibyte UTF-8 strings are unchanged.
- Exact native-endian metadata encoding still produces 577,954 bytes for
  20,008 ordered pairs. The matrix includes empty keys and values, multibyte
  UTF-8, embedded NUL bytes in keys and values, and duplicate keys.
- NULL, empty-vector, and non-NULL zero-count cases retain their canonical
  behavior. Imported metadata strings remain independent copies after the
  producer allocation changes or is released.
- List, struct, leaf, dense-union, both REE child positions, stream fields,
  and nested stream fields retain metadata. Schema and array moves, double
  import refusal, double release, export-ledger cleanup, stream release edges,
  and registry cleanup pass.
- Negative pair counts, key lengths, and value lengths refuse with
  `ValidationError`. Fully allocated positive declarations of 250,000 empty
  pairs, a 2 MiB key, and a 2 MiB value remain accepted under the documented
  trusted-producer policy.
- The full C-data battery passes its ABI, format, topology, ownership,
  lifetime, negative-geometry, nested, stream, and release checks. Its
  four-thread stress child also passes.

## Assumptions and decisions

- I treated Core `Field.metadata` as ordinary field metadata. I therefore
  required it on the same wrapper used by C++ `ExportField` and PyArrow.
- I accepted wrapper-first flattening for foreign dependent-node metadata.
  Core cannot retain two-node attribution in its one slot. I required pair
  preservation, duplicate preservation, and exact identity for Arrow.jl's own
  wrapper-only output.
- I treated direct dictionary-of-dictionary values as out of the representable
  Core set because Core and the Arrow specification reject them. I tested a
  dictionary nested through a struct value as the valid adversarial shape.
- I accepted the corpus and oracle's declared skips as the existing baseline.
- The host is 64-bit arm64 macOS and used Julia 1.12.6. The cross-language
  probe used PyArrow 20.0.0 in a scratch Python environment. All probe sources
  and logs live outside the repository.
- One initial scratch adversarial assertion expected a later rejection
  message. Public validation rejected the invalid shape earlier. The corrected
  probe exits 0. One initial system-Python command had no PyArrow installed;
  the unchanged probe then ran in the pinned PyArrow 20.0.0 environment and
  exited 0. Neither setup issue was a product failure.
- I made no product or test change. The six protected untracked files remain
  present and unmodified. This review document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  660/660 reported assertions: threaded caches 4/4, ArrowCore 384/384, facade
  268/268, and each IPC read, IPC write, C Data, and ranged-scan acceptance
  battery 1/1. The C-data battery includes the guard child, ownership and
  release edges, and the four-thread stress child.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, zero verifier warnings, and compile plus run passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with PyArrow 20.0.0 and
  nanoarrow 0.9.0.
- `git diff --check` — exit 0. `git diff --check HEAD^ HEAD` also exits 0 for
  the exact fix commit.
- In-process Arrow.jl/PyArrow field-and-array pointer handoff — exit 0 in both
  directions; metadata, dictionary types, values, release moves, both-node
  concatenation, duplicate ordering, and own-import identity pass.
- Unchanged round-49 PyArrow ctypes probe — exit 0 with PyArrow 20.0.0;
  native wrapper placement, dependent extension metadata, and PyArrow import
  behavior match the C++ bridge.
- Raw nested-node matrix — exit 0; top-level, list-child, struct-child,
  struct-valued, union-child, and map-value dictionaries use populated field
  wrappers and NULL dependent nodes. Nested value children keep their own
  wrapper metadata.
- Consolidated metadata, ownership, release, and stream probe — exit 0; exact
  577,954-byte encoding, 20,008-pair identity, embedded-NUL and duplicate
  matrices, recursive positions, copied lifetimes, negative declarations,
  large positive declarations, stream edges, and all registry counts pass.
- C-string guard-page child and independent boundary family — both exit 0;
  all exact boundary outcomes pass without a signal.
- Adversarial union, map, and nested-dictionary matrix — corrected run exit 0.
  The first scratch run exited 1 only because its assertion expected the later
  nested-dictionary diagnostic; the product had already refused the shape.
- Final HEAD remains `aa2ff7977f73f12f9418b1c3d2e929b18c0dfc9d`.
  Repository status contains only the six protected untracked files plus this
  review document.

VERDICT: CLEAN
