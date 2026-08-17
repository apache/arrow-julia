# Arrow.jl 3.0 code review — round 48

Date: 2026-08-17

Scope: exact feature commit `5f36bda06a92dfc916d9980abd426851ddbdce84`
(`feat: C-data field metadata transport and bounded C-string reads`) on
`core-rewrite`. Its parent,
`9a69a7e51ccb9388380c6016ec682fa5ace1d588`, records the clean round-47
typed-access review. I reviewed only this feature diff and its full C-data,
trim, conformance, and lifecycle regression surface.

## Result

Round 48 is not clean. I found two issues: one HIGH and one MEDIUM.

The new C-string loop reads one byte beyond its stated 1 MiB scan window
before it checks the limit. A missing-NUL probe with exactly 1 MiB of readable
bytes and an unreadable guard page after them terminates Julia with `SIGBUS`.
It does not throw the promised `ValidationError`.

Dictionary value-schema metadata is also lost. Import parses metadata from the
dependent `ArrowSchema.dictionary` node. It then drops that metadata while it
reconstructs `DictionaryType`. Export has no Core field from which to restore
it, so the dependent node returns with `metadata == C_NULL`.

The normal metadata encoding is exact. Ordinary top-level, nested list/struct,
union, REE, and stream field metadata round-trips pass. Empty strings,
multibyte UTF-8, embedded NUL bytes, duplicate keys, and 20,008 ordered pairs
also pass. Export and import ownership are sound. All five required gates pass,
including zero trim verifier errors and warnings. The gates do not cover the
two findings.

## Findings

1. **HIGH — the bounded C-string reader dereferences byte 1 MiB + 1 before
   enforcing its 1 MiB scan limit.**

   `CSTRING_SCAN_LIMIT` is 1 MiB at `src/cdata.jl:1159`. The loop loads
   `p + n` in its condition at `src/cdata.jl:1163`. It increments `n` and only
   then checks `n > CSTRING_SCAN_LIMIT` at `:1164-1167`.

   After the loop reads 1,048,576 non-NUL bytes at offsets 0 through
   1,048,575, it evaluates the condition again at offset 1,048,576. The limit
   check has not run for this new address. The effective read window is
   therefore 1,048,577 bytes.

   I mapped exactly 1,048,576 readable non-NUL bytes and put a `PROT_NONE`
   guard page immediately after them. The scratch child exited 138 with
   `SIGBUS` at `src/cdata.jl:1163`. A payload of limit minus one plus NUL
   succeeds. A payload of exactly the limit plus NUL also succeeds, but only
   because the importer reads the extra terminator byte. A fully readable
   limit-plus-one non-NUL buffer throws `ValidationError`.

   The adopted [PR #607 loop](https://github.com/samtalki/arrow-julia/blob/23de5c2353c34da5557844b30a200d48f78d12f4/src/cdata.jl#L396-L406)
   reads at most `maxbytes` bytes and then refuses. Apply the limit before the
   next dereference. Under scan-window semantics, the NUL must be within the
   1 MiB window, so the maximum payload is limit minus one. Pin this with a
   guard-page subprocess test.

   I rank this HIGH because malformed ABI input causes deterministic native
   process termination at the exact boundary that this hardening says will
   produce a clean refusal.

2. **MEDIUM — metadata on the dependent dictionary value schema is parsed and
   then discarded.**

   `_import_field` recursively imports `sch.dictionary` into `vf` at
   `src/cdata.jl:1233-1234`. The dictionary branch uses `vf.type` and
   `vf.children` at `:1241-1245`. It does not retain `vf.metadata`.

   Export cannot recover the lost value-schema metadata. `_export_schema!`
   asks `AC.dictvaluefield` for the dependent field at `src/cdata.jl:643-646`.
   That helper creates a new value field with the value type and nested
   children, but no metadata, at `src/ArrowCore.jl:1173-1176`.
   `DictionaryType` stores only index type, value type, and ordering at
   `src/ArrowCore.jl:495-500`. `Field` has only one metadata slot at
   `:521-526`. The outer field and dependent value field therefore cannot
   both retain independent metadata.

   A scratch C-schema probe used `scope=wrapper` on the outer index schema and
   `scope=dictionary-value-field` on its dictionary schema. Direct import of
   the value schema preserved the second pair. Import through the outer
   dictionary preserved only the wrapper pair. The reconstructed value field
   had `metadata === nothing`, and re-export set its metadata pointer to NULL.

   The new battery says dictionary value fields are covered at
   `test/cdata_battery.jl:1271-1274`, but its dictionary case at `:1287-1293`
   sets and checks metadata only on the outer dictionary field. It does not
   construct an independently annotated dependent schema.

   The C interface permits metadata on every `ArrowSchema` node and gives the
   dictionary node no exception. Arrow C++ also uses dependent-schema metadata
   for dictionary value extension types in its
   [dictionary exporter](https://github.com/apache/arrow/blob/d048f71964fe2df5540be2256048eb15f830962b/cpp/src/arrow/c/bridge.cc#L241-L289).
   The root fix needs a Core representation for the dependent value field.
   Merging its metadata into the outer field would still lose the distinction.

   I rank this MEDIUM because it silently loses valid schema information and
   can lose logical extension semantics. The physical dictionary values stay
   intact.

## Sound portions and trust boundary

- `_cmetadata!` at `src/cdata.jl:581-603` matches the
  [official C Data metadata format](https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.metadata):
  native-endian signed int32 pair and byte counts, exact key/value bytes, and
  no NUL terminators. NULL represents absent or empty metadata.
- A raw-byte probe matched the little-endian reference encoding exactly. It
  round-tripped 20,008 ordered pairs in 577,954 bytes. The set included empty
  keys and values, multibyte UTF-8, embedded NUL bytes in values and keys, and
  duplicate keys.
- Top-level, list-to-struct-to-leaf depth, dense-union children, and both REE
  children retain their metadata. Dictionary wrapper metadata and nested
  fields inside a dictionary value type retain theirs. Only the dependent
  dictionary value schema itself fails.
- `metadata === nothing` and an empty metadata vector export as NULL. A
  non-NULL zero-pair blob imports as `nothing`. Negative pair counts, key
  lengths, and value lengths throw `ValidationError`.
- Positive metadata declarations remain intentionally uncapped. Fully
  allocated probes with 250,000 empty pairs, a 2 MiB key, and a 2 MiB value
  import successfully. I did not make this a finding. The stated contract
  trusts producer-declared pointer extents, the C ABI supplies no metadata
  allocation length, and Arrow C++ also rejects negative declarations without
  imposing a resource cap.
- The research note's #607 metadata-bounds attribution is stale. Current
  #607 has no metadata parser. The 4,096-pair, 1 MiB per-field, and 1 MiB
  aggregate caps are in [PR #603](https://github.com/samtalki/arrow-julia/blob/e238f137c9d2b664ef40e178b66769646af476e7/src/cdata.jl#L56-L63),
  with its bounded walk at
  [lines 608-646](https://github.com/samtalki/arrow-julia/blob/e238f137c9d2b664ef40e178b66769646af476e7/src/cdata.jl#L608-L646).
  Those caps limit work. They cannot prove a producer's actual allocation.
- Exported metadata is registered by `_malloc!` in the `ExportedRoot` malloc
  ledger at `src/cdata.jl:338-345` and `:548-578`. Reaping frees that ledger at
  `:774-803`. Imported key/value strings are copied by explicit-length
  `unsafe_string` at `:1198-1206` before the schema release at `:1056-1060`.
- The full battery retains stream directions, move refusal, double release,
  release edges, and four-thread stress. The focused stream probe also retains
  field and nested-child metadata after the schema producer releases it.
- Ordinary format and name strings retain their behavior. Invalid UTF-8 still
  throws `ValidationError`. The only C-string failure is the scan boundary.

## Assumptions and decisions

- I treated `CSTRING_SCAN_LIMIT` as the maximum number of readable bytes that
  the importer may inspect. This matches the constant name, the feature's
  clean-refusal claim, and #607. If the intended policy instead allows a
  1 MiB payload, it must state that the importer reads 1 MiB + 1 bytes and
  cannot promise refusal when only 1 MiB is readable.
- I treated the dependent dictionary `ArrowSchema` as an in-scope field
  position. The prompt explicitly requires dictionary value-field fidelity,
  and the C specification permits metadata on that node.
- I treated `Schema.metadata` on the synthetic ArrowArrayStream root as outside
  this field-only commit. The requested feature and API contract name
  `Field.metadata`. Stream child field metadata remains in scope and passes.
- I accepted uncapped positive metadata declarations under the documented
  trusted-producer policy. I reported the difference from #603 but did not
  turn an intentional policy into a separate finding.
- I rated the boundary crash HIGH and the dictionary schema loss MEDIUM for
  the impact stated in each finding.
- The host is 64-bit arm64 macOS and used Julia 1.12.6. All probe sources,
  logs, mappings, and binaries live in scratch directories outside the
  repository.
- I made no product or test change. The six protected untracked files remain
  present and unmodified. This review document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  660/660 reported assertions: threaded caches 4/4, ArrowCore 384/384, facade
  268/268, and each IPC read, IPC write, C Data, and ranged-scan acceptance
  battery 1/1. The C Data battery includes both stream directions, move and
  release edges, and the four-thread stress child.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, zero verifier warnings, compiler exit 0, and compiled
  binary exit 0.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with pyarrow 20.0.0 and
  nanoarrow 0.9.0.
- `git diff --check` — exit 0. `git diff --check HEAD^ HEAD` also exits 0 for
  the exact feature commit.
- Consolidated metadata and lifecycle probe — exit 0. It covers the exact raw
  encoding, 20,008-pair ordering, all safe string and metadata edge cases,
  ordinary recursive field positions, copy-after-release, the export malloc
  ledger, positive and negative declarations, streams, moves, and releases.
- C-string exact-boundary guard-page child — exit 138 (`SIGBUS`), reproducing
  finding 1 at `src/cdata.jl:1163`. Two independent scratch children produced
  the same result.
- Dictionary dependent-schema diagnostic — exit 0; it preserved outer and
  direct value metadata, then reported `metadata === nothing` after dictionary
  reconstruction, reproducing finding 2.
- Official specification and Arrow C++ source cross-check — the byte layout,
  native endianness, empty-to-NULL rule, C++ encoder, and dependent dictionary
  extension-metadata behavior match the analysis above.
- Final HEAD remains `5f36bda06a92dfc916d9980abd426851ddbdce84`.
  Repository status contains only the six protected untracked files plus this
  review document.

VERDICT: FINDINGS
