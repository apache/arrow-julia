# Arrow.jl 3.0 code review — round 49

Date: 2026-08-17

Scope: exact fix commit `98ecbc56c9841efc964ca4b6da75e81381841420`
(`fix: resolve round 48 findings — scan window, dict value-node metadata`) on
`core-rewrite`. Its parent,
`f744c377236d1280311eda3056d49535282920ca`, records the round-48 review of
feature commit `5f36bda06a92dfc916d9980abd426851ddbdce84`.
I reviewed only the round-48 fix diff and reran the full C-data, metadata,
trim, conformance, ownership, release, and threaded regression surface at the
exact fix commit.

## Result

Round-48 finding 1 is closed at the root. The C-string scan now enforces its
limit before every dereference. The exact guard-page child exits 0, and the
full boundary family has the requested results.

Round-48 finding 2 is not closed at the root. The new importer keeps every
pair from wrapper and dependent value nodes. Its wrapper-first concatenation,
duplicate handling, nested value fields, type stability, and own round-trip
all pass. The exporter, however, puts ordinary dictionary `Field.metadata`
only on the dependent value node and sets the wrapper metadata to NULL. That
is not the Apache C++ bridge convention for field metadata. PyArrow 20.0.0
drops the metadata when it imports this placement.

I found one MEDIUM issue and no other issue. All five required gates pass.
The gates do not exercise this C-schema field-metadata interoperability case.

## Findings

1. **MEDIUM — dictionary field metadata is exported on the wrong C-schema
   node for C++/PyArrow consumers.**

   `_export_schema!` copies `f.metadata` onto a synthesized dependent value
   `Field` at `src/cdata.jl:648-653`. It then passes `nothing` to `_cmetadata!`
   for the outer dictionary wrapper at `src/cdata.jl:661-665`. A raw export
   therefore has `wrapper.metadata == NULL` and
   `wrapper.dictionary.metadata != NULL`.

   Apache C++ does the opposite for ordinary field metadata. `ExportField`
   exports `field.metadata()` on the outer schema in
   [bridge.cc:183-191](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L183-L191).
   Its dictionary path creates the dependent exporter with
   `ExportType(value_type)` at
   [bridge.cc:262-273](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L262-L273).
   `ExportType` passes no field metadata; it emits only additional type
   metadata such as extension metadata at
   [bridge.cc:194-202](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L194-L202).
   This distinction also matches Arrow.jl's IPC adapter: it writes
   `f.metadata` to the dictionary field's `custom_metadata` at
   `src/ipc_write.jl:250-285` and reads it back from that field at
   `src/ipc_read.jl:380-403`.

   The C++ importer reconstructs the dictionary from the dependent importer's
   type only at
   [bridge.cc:1053-1065](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L1053-L1065).
   `MakeField` attaches the outer node's decoded metadata at
   [bridge.cc:1006-1009](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L1006-L1009).
   Arbitrary metadata on the dependent type node is not promoted to field
   metadata.

   A pointer-level PyArrow 20.0.0 probe confirms the source behavior. Native
   PyArrow export put metadata on the wrapper and left the value node NULL.
   PyArrow import preserved wrapper-only metadata. Its `field.metadata` was
   `None` for the value-only placement emitted by this
   commit. With both nodes annotated, it returned only the wrapper pairs. A
   schema-level import also returned `None` for a dictionary child using the
   value-only placement. Every probe exited 0; the loss was a normal import,
   not a rejection.

   The result is one-way interoperability. Arrow.jl reads PyArrow's
   wrapper-only export and preserves it. PyArrow reads Arrow.jl's value-only
   export and silently loses ordinary field metadata. Physical dictionary
   types and values remain intact, so I rank this MEDIUM.

   Keep the single Core/IPC field-metadata slot on the outer wrapper when
   exporting. The new wrapper-first import concatenation can remain as a
   lossless fallback for foreign producers. If exact wrapper-versus-value
   attribution, including dependent extension-type metadata, is required
   across a later re-export, Core needs a separate dependent-node metadata
   representation; one flattened slot cannot preserve that distinction.

## Closure of round-48 finding 1

- `CSTRING_SCAN_LIMIT` remains 1,048,576 bytes at `src/cdata.jl:1163-1167`.
  `_import_cstring` checks `n >= CSTRING_SCAN_LIMIT` at `:1174-1178` before
  its only load at `:1179`. It can dereference offsets 0 through 1,048,575
  only.
- `test/cstring_guard_child.jl:30-42` maps exactly the scan limit of readable
  bytes and places `PROT_NONE` immediately after it. Lines `:44-50` require a
  `ValidationError`. Lines `:52-56` put NUL in the last readable byte and
  require a payload length of limit minus one. The C-data battery launches
  this as a real subprocess at `test/cdata_battery.jl:1336-1338`.
- The repository guard child exits 0 with `cstring guard page ok`. An
  independent scratch child with the same mapping also exits 0. There is no
  signal termination.
- NUL at zero-based offset limit minus one succeeds with length 1,048,575.
  NUL at offset limit refuses with `ValidationError`. A fully readable
  limit-plus-one non-NUL buffer also refuses. Empty, ordinary ASCII,
  multibyte UTF-8, format, and short name strings are unchanged.

## Sound portions of the dictionary fix

- `_import_field` reads both node blobs and forms `dmeta` at
  `src/cdata.jl:1246-1260`. It places wrapper pairs first, appends dependent
  value pairs, and does not deduplicate. `Field` freezes that ordered vector
  at `:1263-1268`.
- The scratch matrix passes value-node-only, wrapper-node-only, and both-node
  imports. The both-node case preserves two entries for the same key in
  wrapper-first order. Own export/import preserves the exact ordered metadata
  vector.
- `AC.dictvaluefield` creates a nullable dependent value field with the exact
  value type and original children at `src/ArrowCore.jl:1169-1176`.
  The export rebuild at `src/cdata.jl:648-652` keeps that name, type,
  nullability, and child vector while adding metadata. A struct-valued
  dictionary probe preserved the synthesized value node's type, nullable
  flag, child count, child nullability, child metadata, and values.
- The trim gate passes with zero verifier errors and warnings. The
  `dmeta === nothing` branch at `src/cdata.jl:1261-1268` therefore retains the
  intended concrete constructor paths under trim.
- Core rejects a dictionary whose value type is another `DictionaryType` at
  `src/ArrowCore.jl:978-984`. I treated the requested nested dictionary value
  case as a dictionary with a nested value type and independently annotated
  child fields, not the forbidden dictionary-of-dictionary shape.

## Round-48 clean regression surface

- Exact native-endian metadata encoding still matches the reference bytes.
  The 577,954-byte probe round-trips 20,008 ordered pairs. It includes empty
  keys and values, multibyte UTF-8, embedded NUL bytes in keys and values, and
  duplicate keys.
- NULL, empty-vector, and non-NULL zero-pair metadata cases retain their
  behavior. Imported metadata strings remain independent copies after the
  producer allocation is released.
- Metadata at list, struct, leaf, dense-union, and both REE child positions
  round-trips. Stream field and nested metadata survives the producer schema
  release and later batch pulls.
- Negative pair counts, key lengths, and value lengths refuse with
  `ValidationError`. Fully allocated positive declarations of 250,000 empty
  pairs, a 2 MiB key, and a 2 MiB value remain accepted under the documented
  trusted-producer policy.
- Schema and array moves, double import refusal, double release, copied
  metadata lifetime, export malloc-ledger cleanup, stream release edges, and
  registry cleanup pass. The full C-data battery also passes its four-thread
  stress child.

## Assumptions and decisions

- I treated `CSTRING_SCAN_LIMIT` as an exact readable scan window. Boundary
  offsets in the probes are zero-based. This makes the maximum payload the
  limit minus one, as the fix commit states.
- I treated Core `Field.metadata` as field metadata, not arbitrary metadata
  for the dependent dictionary value `DataType`. This follows Arrow.jl's IPC
  read/write mapping and the cited C++ `ExportField` behavior.
- I accepted wrapper-first concatenation as the least-lossy import mapping
  into Core's one metadata slot. I did not require exact two-node attribution
  after import because the stated design explicitly does not represent it.
- I did require another conforming consumer to see our ordinary field
  metadata. The prompt explicitly asks whether PyArrow can read our export,
  and wrapper NULL fails that check.
- I used PyArrow 20.0.0, the same version reported by the required oracle
  gate. The probe used PyArrow-owned C schemas and changed only their metadata
  pointers to the layouts under test. Release callbacks and topology remained
  valid.
- I rated the interop loss MEDIUM because it silently removes schema
  information while leaving physical data valid.
- The host is 64-bit arm64 macOS and used Julia 1.12.6. All probe sources,
  logs, mappings, and PyArrow runs live in scratch directories outside the
  repository.
- I made no product or test change. The six protected untracked files remain
  present and unmodified. This review document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  660/660 reported assertions: threaded caches 4/4, ArrowCore 384/384, facade
  268/268, and each IPC read, IPC write, C Data, and ranged-scan acceptance
  battery 1/1. The C-data battery includes the guard child, both stream
  directions, move and release edges, and the four-thread stress child.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, zero verifier warnings, compiler exit 0, and compiled
  binary exit 0.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with pyarrow 20.0.0 and
  nanoarrow 0.9.0.
- `git diff --check` — exit 0. `git diff --check HEAD^ HEAD` also exits 0 for
  the exact fix commit.
- C-string exact-boundary guard-page child — exit 0. Independent boundary
  family — exit 0, with the four requested edge outcomes and ordinary short
  strings.
- Consolidated round-48 metadata, ownership, release, and stream probe —
  exit 0; exact 577,954-byte encoding, 20,008-pair identity, recursive
  positions, negative declarations, large positive declarations, and all
  registry counts pass.
- Dictionary metadata matrix — exit 0; raw value-only placement, wrapper-only
  import, both-node wrapper-first concatenation with duplicate preservation,
  own round-trip identity, and nested value-field shape and metadata pass.
- PyArrow C-schema field and schema probe — exit 0; native wrapper-only
  placement preserves metadata, while this commit's value-only placement
  imports with no field metadata. Both-node import keeps only wrapper pairs.
- Official C++ source cross-check — ordinary field metadata stays on the
  wrapper; the dependent node uses `ExportType` and carries only type-level
  additions such as extension metadata.
- Final HEAD remains `98ecbc56c9841efc964ca4b6da75e81381841420`.
  Repository status contains only the six protected untracked files plus this
  review document.

VERDICT: FINDINGS
