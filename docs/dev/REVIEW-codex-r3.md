# ArrowCore prove-out review — round 3

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1 and
round-2 fixes recorded in `REVIEW-codex-r1.md` and
`REVIEW-codex-r2.md`. The design authority was
`Arrow-redesign-report.md` §9. This was a fresh adversarial pass over Core,
the IPC and C Data examples, their tests, and the README. Declared exclusions
were kept excluded. Unsupported or trusted boundaries were checked for honest
documentation instead of being implemented.

1. **HIGH — failed C Data import cleanup trusted the malformed public topology
   that caused the failure.** `from_c_data` moved the input before validation,
   then failure cleanup called producer release callbacks. Those callbacks
   traversed the caller-visible `n_children`, `children`, and `dictionary`
   fields. Setting a one-child List table to `NULL` made cleanup dereference
   `NULL` and terminate a subprocess. Setting its count to zero skipped the
   hidden child and stranded an export root and source pin. Schema trees and
   dictionaries had the same defect. Fixed in `8dbf439`: the exporter keeps
   canonical child and dictionary topology in its private registry root.
   Release traversal uses that topology while still reading each canonical
   descendant's public `release` field to honor a valid consumer move. Tests
   cover null tables, false zero counts, missing dictionaries, moved children,
   moved dictionaries, exact reaping, source-pin release, and natural owner
   finalization. A four-thread follow-up repeatedly cleaned 120 malformed trees
   while a reaper ran and left the registry empty.

2. **HIGH — the public semantic and full validators did not enforce their
   structural prerequisite.** Calling `validate_semantic` or `validate_full`
   directly on an Int64 array with no data buffer returned success and set the
   semantic cache flag. A mismatched Field descriptor also passed. Later
   access could then fail after the data had been certified. Fixed in
   `73c0feb`: the public semantic stage first runs structural validation, and
   the public full stage composes semantic validation. Private recursive
   passes avoid repeating a full structural tree walk at every depth.
   Regressions call both later stages directly with wrong type, arity, and
   buffer geometry and verify that no cache certificate is published.

3. **HIGH — forged enum-backed descriptors could select inconsistent layout
   and accessor branches.** Invalid Date, Time, Timestamp, Duration, Interval,
   and Union enum values were accepted. An invalid Union mode was treated as
   dense by `layoutspec` but as sparse by semantic checks and access, which
   could end in `BoundsError`. Fixed in `a080f4d`: every enum-backed descriptor
   has an explicit allowed domain, and an unknown value is never interpreted
   through a default `else` branch. Regressions forge each invalid enum with
   `reinterpret` and require structural rejection.

4. **MEDIUM — recursive Field nullability checks rejected valid storage hidden
   by a parent null or union selection.** A nullable null Struct or
   FixedSizeList slot failed when its hidden child storage was null under a
   non-nullable child Field. Null List ranges and unselected Union positions
   had the same contextual problem. Fixed in `73c0feb`: cached intrinsic data
   checks are separate from an uncached reachability-aware Field-contract
   traversal. The traversal follows only valid Struct slots, valid fixed/list
   ranges, and the selected Union child. Root nullability remains enforced.
   Dictionary pools remain independent arrays. A final re-review found that
   nested dictionary pools needed their own contract walk; `91a0ee3` fixed
   that regression. Tests cover masked and visible Struct, FixedSizeList, List,
   Map, dense and sliced sparse Union, and nested dictionary-pool cases.

5. **MEDIUM — Decimal coefficients were not checked against declared
   precision.** A `DecimalType(1, 0, 32)` coefficient of `10` or `-10` passed
   every validation stage, even though precision one permits magnitudes below
   ten. IPC only runs through semantic validation before exposure, so corrupt
   IPC data also passed. Fixed in `fb03d23`: semantic validation compares every
   valid coefficient's two's-complement magnitude with `10^precision` using
   fixed four-limb arithmetic for Decimal32/64/128/256. `07e0ce4` corrected
   carry truncation at maximum precision after an independent BigInt oracle
   exposed the first implementation's checked-conversion failure. Tests cover
   positive and negative boundaries, null masking, Decimal128 precision 38,
   Decimal256 precision 76, and a malformed IPC stream. A 40,048-case
   randomized differential check matched BigInt for all four widths.

6. **MEDIUM — schema string and enum domains were incompletely validated.** A
   malformed UTF-8 Timestamp timezone, Field metadata key/value, or Schema
   metadata key/value could enter a certified Core tree. A forged invalid
   Schema endianness could enter a RecordBatch. Fixed in `766d987`: descriptor
   validation checks timezone UTF-8, Field structural validation checks every
   metadata string, and the RecordBatch schema boundary checks Schema metadata
   and the endianness enum. Tests cover each key/value position, dictionary
   value descriptors, valid Unicode, and an invalid endianness value.

7. **MEDIUM — ordered metadata input was incompatible with the stored metadata
   model.** `_freezemetadata` iterated `pairs(metadata)`, so a vector or tuple
   of `Pair` values produced index-to-Pair entries and failed with
   `String(::Int64)`. This prevented callers from preserving order and
   duplicate keys even though `Field` and `Schema` store an ordered frozen
   vector of pairs. Fixed in `766d987` and tightened in `bf2c0e6`: Pair
   sequences preserve order and duplicates through a defensive copy, while
   arbitrary sequence elements fail with `ArgumentError`. Dict and NamedTuple
   inputs retain their existing path.

8. **MEDIUM — empty and all-missing List builders changed the child schema to
   Int64.** `fromjulia("x", Vector{Vector{Float64}}())` and an all-missing
   `Vector{Union{Missing,Vector{UInt8}}}` both produced `List<Int64>`. The
   resulting arrays validated, so the declared Arrow schema depended on the
   presence of values instead of the Julia element type. Fixed in `f138f32`:
   `_build_list` derives an empty child vector from the statically declared
   non-missing vector element type. Tests cover empty, all-missing, unsigned,
   and nested lists; unsupported child types fail cleanly instead of guessing.

9. **MEDIUM — Core accepted non-native RecordBatch endianness but always used
   native loads.** On a little-endian host, a valid BigEndian Int32 buffer for
   value one passed construction and semantic validation but materialized as
   16,777,216. Fixed in `af64e39`: RecordBatch construction rejects a valid
   non-native Schema endianness and requires adapters to normalize before Core
   access. Native endianness is now the Schema default, and Decimal limb
   assembly is defined in native order. The README states this boundary and a
   regression rejects the opposite native enum.

## Scope decisions and withdrawals

- The producer-private C topology fix hardens this prove-out's own callbacks.
  Foreign allocation extents and callback behavior remain trusted declarations
  because the C Data ABI does not supply verifiable allocation bounds. This is
  consistent with the [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
  and remains stated in the README.
- A suspected `ForeignOwner` finalizer leak was withdrawn. An unreachable
  owner left a registry entry whose aggregate `remaining` count was already
  zero. That proves the finalizer ran; explicit `reap!` intentionally owns
  registry removal. A natural-GC regression now pins this distinction.
- No additional defect was found in canonical lifecycle delegation, the
  publish-after-build export registry, guard/close ordering, moved-node
  exactly-once release, semantic cache publication, or IPC cursor
  serialization. Focused threaded stress covered those schedules.
- UTF-8 array content remains an opt-in full check. The IPC prove-out runs
  structural and semantic validation, not `validate_full`; the README now says
  this directly. C Data import and export still run full validation.
- Map key uniqueness, hashability, ordering, and the truth of `keysSorted` are
  producer/application contracts under the
  [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html),
  not checks implemented by this validator. Timestamp timezone names are not
  resolved against a timezone database. Both limits are now explicit.
- View/ListView/REE semantic work, padding and unused-bit checks, IPC
  compression and endian normalization, file footer/index support, facade
  work, the native foreign-thread C callback trampoline, and the other README
  exclusions remain out of scope and fail closed where stated.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 270/270 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including Decimal precision rejection, the four-thread cursor gate,
  metadata limits, body-span corruption, and dictionary snapshot tests.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including malformed
  public topology cleanup, move semantics, natural finalization, concurrent
  construction/reaping, source-pin release, and registry-empty checks.
- The Decimal precision comparison matched a BigInt oracle for 40,048 boundary
  and random signed values across 32, 64, 128, and 256 bits.
- All round-3 changes are confined to `core/`. Each logical change is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: FINDINGS
