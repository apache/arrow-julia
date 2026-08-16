# ArrowCore prove-out review — round 4

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-3 fixes recorded in `REVIEW-codex-r1.md`,
`REVIEW-codex-r2.md`, and `REVIEW-codex-r3.md`. The design authority was
`Arrow-redesign-report.md` §9. This was a fresh adversarial pass over Core,
the IPC and C Data examples, their tests, and the README. Declared exclusions
were kept excluded. Unsupported and trusted boundaries were checked for
honest documentation instead of being implemented.

1. **MEDIUM — valid maximum-width dictionary indices overflowed during
   access.** Semantic validation correctly accepted an Int8 index of 127 for
   a 128-value pool and a UInt8 index of 255 for a 256-value pool. The
   dictionary accessor then converted the Arrow zero-based index to a Julia
   one-based index with `checked_add(idx, one(idx))`. That addition ran in the
   narrow index type and threw `OverflowError` for both valid values. Fixed in
   `927904c`: the validated index is widened to Int64 before adding one.
   Regressions validate and materialize the signed and unsigned 8-bit maximum
   cases.

2. **HIGH — RecordBatch construction did not prove that columns matched their
   schema or had valid buffer geometry.** The constructor checked only column
   count and length. It accepted an Int64 Field paired with Float64 data and an
   Int64 array with no buffers. This let a false schema/data pair or an array
   that fails on later access cross Core's sole interchange boundary. Fixed in
   `aa622c7`: construction now runs structural validation for each Field and
   column pair. Semantic content checks remain lazy. Regressions reject both
   the type mismatch and the missing data buffer.

3. **MEDIUM — IPC repeatedly walked shared dictionary pools, so independent
   resource limits did not bound validation work.** Every dictionary column
   ran semantic validation. Multiple fields and record batches that shared
   one dictionary identity therefore re-ran pool Field contracts and
   recursively revisited the pool tree. A valid stream with many references
   to one large or wide pool could require work proportional to the product
   of the reference count and pool size, although its encoded size was
   proportional to their sum. `9e16099` first added a stream-wide identity
   memo for Field contracts. The required re-review showed that this was
   incomplete: structural validation, intrinsic recursion, and RecordBatch
   construction still walked a wide nested pool for every batch. Fixed fully
   in `c2fbf14`: each immutable pool snapshot receives an identity certificate
   only after ordinary semantic validation of its DictionaryBatch and before
   publication. Later record validation skips structural, intrinsic, and
   Field-contract recursion for that exact pool identity, while it still
   validates every index array. Dictionary replacements have new identities
   and must earn new certificates. A traversal-counter regression pins every
   skipped path, including RecordBatch construction. A 100-batch wide-Struct
   probe remained approximately constant when the pool grew from 10 to 10,000
   children.

## Scope decisions and withdrawals

- No additional defect was found in canonical lifecycle delegation,
  guard/close ordering, the publish-after-build C export registry, moved-node
  release, or IPC cursor serialization. Focused four-thread stress covered the
  cursor and callback/reaper schedules.
- A suspected native-address wrap in imported C pointer tables was withdrawn.
  The [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
  does not declare allocation extents for those foreign tables. This
  prove-out explicitly treats the producer's pointers and extents as trusted.
  The exposed counts, lengths, and derived buffer geometry remain checked.
- RecordBatch construction now enforces structural validity, not eager
  semantic validity. This preserves the staged-validation design while
  preventing malformed geometry and schema/type disagreement from crossing
  the interchange boundary.
- View/ListView/REE semantic work, padding and unused-bit checks, IPC
  compression and endian normalization, file footer/index support, facade
  work, native foreign-thread C callbacks, and the other README exclusions
  remain out of scope and fail closed where stated.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 279/279 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including the four-thread cursor gate, bounded metadata expansion, checked
  body spans, dictionary replacement snapshots, and shared-pool certificate
  regressions.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including ABI
  layout, publish/reap ordering, move semantics, malformed-topology cleanup,
  source pins, and registry-empty checks.
- Randomized validation exercised 300,000 primitive, binary, list,
  fixed-list, union, dictionary, and sliced-map arrays. All 55,293 cases
  accepted by `validate_full` materialized without failure.
- The shared-pool performance probe used 100 record validations. Median
  semantic time was about 0.000106 seconds for a 10-child pool and 0.000109
  seconds for a 10,000-child pool. RecordBatch construction was about 0.000071
  seconds at both widths. The committed regression uses deterministic visit
  counts, not timing thresholds.
- All round-4 changes are confined to `core/`. Each logical change is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: FINDINGS
