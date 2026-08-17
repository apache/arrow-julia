# Arrow.jl 3.0 code review — round 38

Date: 2026-08-17

Scope: exact commit `adf72aa0be73ae7607b3a29f3112cbe4832cd220`
on `core-rewrite`. I reviewed the round-37 fix against code head
`5f5715cdb0805a193d0b4c761c8ae41ae54cb395` and report commit
`f6b126a`. I used the manifest-selected Tables.jl checkout at
`d1fbb6eb577741688dba70039754166b51c1cdcc` as the authority.

## Result

Round 37 is not closed. Three findings remain: two HIGH and one MEDIUM.

The scalar override conversions now match `Tables.finish`. Facade zero-field
filters now return the correct counts. Duplicate-source `precols` alignment,
the ranged limits, the empty-projection text, and the full keep-green matrix
also pass.

Direct zero-field `Tables.scan` still loses every row kept by a residual
filter. The facade fix also allocates row-count-sized masks outside the reader
budget. The requested empty/nonempty list closure is incomplete: the zero-row
case loses its retained field and metadata, and every list rewrite still
fails.

## Findings

1. **HIGH — direct zero-field `Tables.scan` still loses kept rows.**

   `Tables.apply(::ArrowFile, ...)` preserves the decoded row count in
   `_scantable` at `src/scan.jl:525-539`. It then leaves the filter and window
   residual at `src/scan.jl:507-513` and `src/scan.jl:548-551`. The ranged
   path does the same at `src/scan.jl:1021-1045`.

   `Tables.scan` calls `finish(apply(...))`. Generic `Tables.finish` evaluates
   the residual correctly, but then returns an empty `NamedTuple`. That value
   cannot carry a positive row count. The count stored in `_ScanColumns` is
   lost.

   The focused probe produced these results:

   ```text
   source       filter                                  expected   got
   ArrowFile    AlwaysTrue()                            3          0
   ArrowFile    AlwaysTrue(), limit=1, offset=1          1          0
   ArrowFile    isnull(col(:gone)), validate=false       3          0
   ArrowFile    same filter, limit=1, offset=1            1          0
   RangedFile   same four cases                       3/1/3/1      0/0/0/0
   ```

   The file, stream, and ranged `Arrow.Table` facade paths all returned the
   expected counts. `AlwaysFalse`, an unknown comparison, strict validation,
   and reject-window controls also passed. This is a direct Tables.jl scan
   contract failure, not a predicate-evaluation failure.

   A root fix must preserve the zero-field count through the residual. One
   direct option is to evaluate the row-invariant zero-field predicate once,
   consume its filter and window in both apply paths, and return an empty
   residual with the final `_ScanColumns` count.

2. **HIGH — the facade fix allocates from an untrusted zero-field row count.**

   `_publicscan` creates a dummy column from `n0` and calls `Tables.finish` at
   `src/table.jl:464-473`. The zero-size `Vector{Missing}` storage is not the
   large allocation. `Tables.finish` creates a row-sized predicate mask and
   matching-row indexes before it applies `limit`.

   A 266-byte file with a declared count of 1,000,000 rows produced this
   post-warm-up result for `AlwaysTrue(), limit=1`:

   ```text
   source    allocated bytes   result rows
   file            9,339,648             1
   stream          9,178,608             1
   ranged          9,195,696             1
   ```

   Each reader used `Limits(max_total_allocated_bytes=100_000)`. The scan
   allocation is outside that budget. Allocation grew from 111,232 bytes at
   10,000 rows to 9,176,400 bytes at 1,000,000 rows. The default
   `max_array_length` is 1,000,000,000 at `src/ipc_read.jl:69-77`, so a few
   hundred input bytes can request several gigabytes. `limit=1` does not
   bound the work.

   Round 37 explicitly required that this path not allocate a mask from the
   untrusted count. A shared scalar evaluator for zero-field predicates can
   return true, false, or missing once. The implementation can then apply
   the row count and window with integer arithmetic. That also gives the
   direct apply paths a single root-cause fix.

3. **MEDIUM — the requested empty/nonempty list override closure is
   incomplete.**

   The scalar override matrix is fixed. The list matrix is not. A zero-row
   real `ListType(false)` facade has `eltype == Any` because the composite
   facade mapping at `src/table.jl:186-221` has no observed values from which
   to narrow the type. `_boundschema` therefore drops its retained field at
   `src/table.jl:543-560` for `=> Vector`. The values and resulting element
   type match `Tables.finish`, but the field descriptor and column metadata
   are lost on file, stream, and ranged reads. The nonempty case now retains
   its descriptor and metadata on all three paths.

   Both the empty and nonempty results still fail clean rewrite. All 12
   input/output transitions failed:

   ```text
   2 list shapes × 3 input paths × 2 output formats = 12 failures
   ArgumentError: fromjulia: unsupported element type Any (prove-out scope)
   emitted bytes: 0 for every failure
   ```

   The retained non-temporal writer falls back to natural inference at
   `src/write.jl:167-180`. The nested-list builder then sees an `Any` child
   type and rejects it at `src/ArrowCore.jl:2183-2199` and
   `src/ArrowCore.jl:2251-2263`. This writer gap also affects the unchanged
   list facade, so the latest commit did not introduce it. It is still an
   acceptance failure because round 38 explicitly requires clean rewrites.

   The new regression at `test/facade_tests.jl:565-576` checks only the
   nonempty read-retention case. It does not check the zero-row case or a
   rewrite. A root fix needs descriptor-derived composite facade typing or a
   retained-descriptor composite writer. It must cover both empty and
   nonempty columns.

## Accepted pathological dummy-name edge

The private dummy name can affect one deliberately constructed filter:

```julia
!Tables.in_(Tables.col(Symbol("#arrowcount#")), ())
```

With `validate=false`, a truly absent column returns zero rows. The facade
returns three rows, or one after the requested window, on file, stream, and
ranged paths. Strict validation still errors. Simple `isnull` and comparison
controls agree with the absent-column authority.

I did not count this as a separate finding under the prompt's explicit
pathological-name allowance. A zero-field schema cannot contain a real field
with this name, and the mismatch needs a deliberate reference to the private
sentinel plus a predicate that distinguishes absent from concrete missing.
The scalar root fix for finding 2 removes the sentinel and this edge.

## Clean portions of the closing sweep

- `Int64 => Union{Missing,Float64}` and
  `Union{Missing,Int64} => Float64` matched the authority on file, stream,
  and ranged reads. Six file/stream rewrites passed. Their rewritten fields
  were nullable and non-nullable, respectively.
- Direct `Any[]`, nonempty `Any`-list, and observed-missing helper controls
  matched `Tables.finish` 3/3.
- Selecting the same source column twice with different overrides passed
  12/12 schema-alignment checks and 24/24 rewrites. Both output orders,
  pushdown and public fallback, and all three input paths were covered.
- Facade zero-field semantics passed for `AlwaysTrue`, its window,
  `isnull(col(:gone))` under `validate=false`, `AlwaysFalse`, unknown
  comparisons, and strict validation. Ranged limits passed 3/3.
- The `Arrow.Table` docstring at `src/table.jl:43-49` now names
  `select=()` and the full ranged-read fallback.
- `git diff --check HEAD^ HEAD` exited 0.

## Assumptions and decisions

- I used `Tables.finish` over converted public values as the authority for
  override values and declared element types.
- I used the explicit record-batch row count as the authority for a
  zero-field source.
- I treated direct `Tables.scan` as part of the requested round-37 closure.
- I kept the zero-row retained-field requirement because the prompt states
  it explicitly, even though `Tables.finish` must convert the facade's
  current `Any[]` element type to `Vector`.
- I counted the pre-existing list writer gap because clean rewrite is an
  explicit round-38 acceptance condition. I did not attribute that gap to
  the latest commit.
- I rated the direct count loss and small-input memory amplification HIGH.
  I rated the limited list metadata and rewrite closure MEDIUM.
- I accepted the exact dummy-name mismatch as pathological under the prompt.
- I made no product or test changes. I preserved the six pre-existing
  untracked files and added only this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 150/150, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6,
  zero verifier errors, zero verifier warnings, and the trimmed binary passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 skip.
- Focused scalar override probe — exit 0; reads 3/3 and rewrites 6/6.
- Duplicate-source `precols` probe — exit 0; alignment 12/12 and rewrites
  24/24.
- Focused list probe — exit 1; authority values/types 6/6, nonempty retention
  3/3, empty retention 0/3, and rewrites 0/12.
- Focused zero-field matrix — exit 1; 49/57 passed, with the eight direct
  file/ranged kept-row failures in finding 1. Reject-window controls passed
  10/10 in a separate exit-0 probe.
- Allocation probes — exit 0 with assertions enabled; the measured growth
  and budget bypass are in finding 2.
- Pathological dummy-name probe — exit 0 with 11 asserted observations; the
  accepted mismatch is recorded above.

VERDICT: FINDINGS
