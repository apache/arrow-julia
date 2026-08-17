# Arrow.jl 3.0 code review — round 40

Date: 2026-08-17

Scope: exact fix commit `31922e991b3bd4a332cdde1c414e5c9fc5b18523`
on `core-rewrite`. Its parent, `dd4725b586019e879652a4b6e23785b7b43cd0fd`,
records round 39 against code commit
`0d71f2b2938984d607063263df418943dc855fc9`. I used the
manifest-selected Tables.jl checkout on `jq/scan` at
`d1fbb6eb577741688dba70039754166b51c1cdcc` as the authority.

## Result

Round 39 is not clean. Three findings remain: one HIGH and two MEDIUM.

The exact round-39 record-frame, cumulative-budget, small-list, listed
empty-conversion, and unsupported-predicate reproductions are closed. The
full required gates also pass. The facade suite reports 224/224 tests; the
223 count in the request is stale.

The adversarial pass found incomplete root closure in three adjacent cases.
The new zero-field ranged helper accepts semantically invalid dictionary
blocks. The new empty-column type rule covers variable lists but not four
other vector-valued descriptors. The new retained-list retype recovers Julia
value structure but does not retain nested Arrow descriptors.

## Findings

1. **HIGH — zero-field ranged scans accept invalid dictionary blocks.**

   `_zerofieldblockcount` validates the block extent, outer frame, header
   kind, metadata version, and legacy compression marker at
   `src/scan.jl:913-931`. It then returns zero for a dictionary block at
   `src/scan.jl:932`.

   That return skips the dictionary checks used by the normal ranged path at
   `src/scan.jl:1061-1076`: delta rejection, schema-id membership, duplicate-id
   rejection, inner `RecordBatch` validation, and `_batchcodec`. The full
   reader performs the equivalent checks at `src/ipc_write.jl:1308-1334`.

   I used verified frames with a zero-field schema, one valid one-row record
   batch, and malformed dictionary blocks. Direct zero-field ranged scans
   accepted one row in all four cases:

   ```text
   dictionary defect                    zero ranged   normal ranged/full
   unknown id                           accepted 1    rejected
   delta                                accepted 1    rejected
   duplicate id                         accepted 1    rejected
   inner RecordBatch length mismatch    accepted 1    rejected
   ```

   A zero-field schema cannot declare a dictionary id. Therefore any indexed
   dictionary block is orphaned. Rejecting nonempty `dictblocks` is sufficient
   for this branch. Reusing the normal dictionary metadata pass is also valid.

   The continuation-prefix fix closes the original record-block reproduction.
   It does not close the ranged trust boundary at the root because dictionary
   semantics still bypass validation.

2. **MEDIUM — the empty-column declared type rule is incomplete for
   vector-valued descriptors.**

   `_boundschema` promises the same keep/drop decision for empty and nonempty
   columns at `src/table.jl:526-537`. `_declaredbasetype` only maps
   `ListType` to a vector at `src/table.jl:540-543`. `BinaryType`,
   `FixedSizeListType`, `StructType`, and `MapType` fall through
   `_facadebasetype` to `Any` at `src/table.jl:190-209`.

   Those four descriptors materialize nonempty rows as vectors at
   `src/ArrowCore.jl:1935-2006`. A `=> Vector` override is therefore a no-op
   for each nonempty facade. It retains the source descriptor and field
   metadata. The same override over an empty facade uses declared `Any` at
   `src/table.jl:551-553`, drops the field and metadata, and then cannot be
   rewritten from the empty abstract vector.

   File, stream, and ranged probes reproduced this empty/nonempty split for
   all four descriptors. This is the same identity failure as round-39
   finding 4, outside the one `ListType` special case. The declared mapping
   must cover every supported vector-valued facade layout, or the keep/drop
   rule must use an equivalent descriptor predicate.

3. **MEDIUM — retained nested-list rewrites do not preserve child
   descriptors.**

   The new path uses a retained child only to derive a Julia element type at
   `src/write.jl:204-206`. It then builds the column naturally and installs
   `fn.children` at `src/write.jl:211-221`. `_retainedlisteltype` does not
   carry list width or metadata at `src/write.jl:232-244`.

   The natural builder always emits `ListType(false)`, derives nullability
   from observed nulls, and creates children without retained metadata at
   `src/ArrowCore.jl:2251-2268`. Valid hand-built nested-list streams showed
   the following drift for both zero-row and row-bearing inputs:

   - Child and grandchild metadata were removed.
   - A declared nullable child became nonnullable when no null was observed.
   - A large-list child under a small-list parent silently became a small
     list.
   - A large-list parent still failed clean rewrite because natural inference
     produced a small list.

   The listed 12-transition matrix uses naturally generated small-list
   descriptors, so it does not expose this loss. Round-39 finding 3 is not
   closed at the recursive retained-schema boundary. The writer must rebuild
   compatible data under the retained child fields, not replace those fields
   with naturally inferred children.

## Closed portions of round 39

- The corrupted continuation-prefix probe passes. The full reader and direct
  ranged scans, both unbounded and `limit=1`, reject with `ValidationError`.
- The file scan uses one cumulative budget. A 2,000-batch scan under
  2,105,336 bytes rejects at batch 450. Each successful header consumes 4,688
  bytes.
- The ranged scan parses one footer and uses one budget. The footer charge is
  5,984 bytes. One ranged header costs 4,776 bytes: the file-path 4,688-byte
  charge plus the 88-byte range fetch. The exact 10,760-byte cap succeeds.
  The old split-budget 10,672-byte cap rejects. A prefetch control rejects
  before the dedicated metadata request.
- Ranged counts stream lazily. A 2,000-batch `limit=1` request fetches only
  the first record metadata block. An unbounded request under the same cap
  rejects.
- The required small-list matrix passes 12/12 rewrites. Empty and nonempty
  retention pass 3/3 each. Row-bearing all-empty, nested, nullable, Bool, and
  string shapes pass 42/42 rewrites. Bool and Char replacements reject with
  clean identity errors 24/24. Valid Bool values remain Bool 6/6.
- The listed empty-conversion matrix matches `Tables.finish` 30/30, makes the
  intended retention decision 30/30, and rewrites 60/60. Empty/nonempty
  parity passes 5/5 for the listed list and scalar cases.
- `OpNode` rejects with `ArgumentError` under both validation settings on all
  three facade paths and all three direct controls.
- The direct zero-field matrix passes 57/57. Reject-window controls pass
  10/10. Allocation is flat between 10,000 and 1,000,000 declared rows.
  Overflow and residual controls pass 18/18. Pathological-name controls pass
  17/17.

## Assumptions and decisions

- I used `Tables.finish` and `Tables.bind` from the manifest-selected checkout
  as the semantic authority.
- I used each explicit record-batch length as the zero-field row-count
  authority.
- I treated dictionary ids absent from the schema as invalid. This matches the
  full reader and the normal ranged path.
- I treated retained identity as recursive. It includes descriptor parameters,
  nullability, field metadata, and schema metadata.
- For an empty composite facade, I used its descriptor-defined nonempty row
  type for the keep/drop decision. This is the rule documented by
  `_boundschema` and already applied to variable lists.
- I accepted early stop after an exact limit and after a row-invariant false
  predicate. The ranged statistics path already permits result-directed
  metadata pruning. Global block extents still validate first, and every
  consumed record block receives full frame validation.
- I rated malformed indexed-metadata acceptance HIGH, consistent with
  round-39 finding 1. I rated the two retained-schema failures MEDIUM because
  the tested values remain intact or writing fails cleanly.
- The host is 64-bit, so `Int` and `Int64` have the same range in the overflow
  probes.
- I made no product or test changes. I preserved the six pre-existing
  untracked files and added only this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 224/224, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, zero verifier warnings, and the trimmed binary passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 skip.
- Round-40 scan closure probe — exit 0; 40/40. It includes frame integrity,
  file and ranged budget accounting, charge-before-fetch ordering, laziness,
  early stop, and all `OpNode` controls.
- Dictionary adversarial probe — exit 0; 16/16 assertions reproduced finding
  1 against full-reader and normal-ranged controls.
- Required list matrix — exit 0; authority 6/6, retention 6/6, and rewrites
  12/12.
- Expanded facade edge probe — exit 0; shape rewrites 42/42, empty conversions
  30/30, retention decisions 30/30, rewrites 60/60, parity 5/5, replacement
  errors 24/24, and Bool preservation 6/6.
- Declared scalar/natural-list baseline — exit 0; 13/13 descriptor/materialized
  type pairs agree.
- Empty composite parity probe — exit 1 by contract assertion; authority values
  pass 24/24, empty retention passes 0/12, and nonempty retention passes 12/12.
- Nested descriptor identity probe — exit 1 by contract assertion; small and
  large-child values pass 24/24, exact recursive schemas pass 0/24, and
  large-parent rewrites pass 0/12.
- Composite and nested-descriptor adversarial probe — exit 0; 48/48 composite
  and 18/18 nested assertions reproduced findings 2 and 3.
- Tables typed-composite authority control — exit 0; 2/2. Empty and nonempty
  `Vector{Vector{UInt8}}` remain `Vector{UInt8}` under `=> Vector`.
- Direct zero-field matrix — exit 0; 57/57.
- Reject-window controls — exit 0; 10/10.
- Allocation-flatness probe — exit 0; file 5,488 bytes, stream 1,792 bytes,
  and ranged 18,880 bytes at both 10,000 and 1,000,000 declared rows.
- Overflow and residual probe — exit 0; 18/18.
- Pathological-name probe — exit 0; 17/17.
- `git diff --check` — exit 0.

VERDICT: FINDINGS
