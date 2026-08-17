# Arrow.jl 3.0 code review — round 35

Date: 2026-08-17

Scope: exact commit `434e8de7e404320e679d12b3637e306b595bd5a4`
on `core-rewrite`. I reviewed it against round 34 at
`5c170df83a838ee76214350d5264d3087a0f5eaa` and re-ran the requested
file, stream, and ranged probes.

## Result

The round-34 fixes are not closed. The exact five-row temporal differential,
the main retained-schema transition table, renamed-field binding, empty
projections over nonempty schemas, reporting partitions, and DataAPI
missing-column errors now pass. However, public scan semantics still drift for
sub-millisecond timestamps, the required nullable `Int64 => Float64` override
still fails, retained output schemas can describe the wrong type, retained
rewrites still accept incompatible replacements, and zero-field scan windows
still lose row counts.

## Findings

1. **HIGH — temporal lowering still changes public predicate semantics and
   does not send every unrepresentable literal to the fallback.**

   The facade exposes microsecond and nanosecond Timestamp columns as raw
   `Int64` values at `src/table.jl:153-163` and `src/table.jl:182-188`.
   `_storagevalue` nevertheless converts `Date` and `DateTime` literals to
   microsecond or nanosecond storage at `src/table.jl:247-261`. That conversion
   is physically exact but is not equivalent to comparing the public values.

   A valid differential produced:

   ```text
   predicate                         Tables.finish   file   stream   ranged
   Timestamp[us] == DateTime(...)    []              [2]    [2]      [2]
   Timestamp[ns] == DateTime(...)    []              [2]    [2]      [2]
   ```

   `_lowerscan` marked both filters pushable. `Tables.finish` correctly
   compared an `Int64` column with a `DateTime` and found no match. All three
   facade paths compared scaled integers and returned a row.

   The tagged fallback is also incomplete:

   - Date32 narrowing at `src/table.jl:231-245` calls `Int32(...)` without
     catching range failure. A valid `Date(6_000_000, 1, 1)` literal produced
     `InexactError: trunc(Int32, 2190735472)` in file, stream, and ranged
     scans. The public authority returned no rows.
   - Duration lowering at `src/table.jl:271-281` catches only `InexactError`.
     Comparing a public `Second` column with `Month(1)` returned no rows under
     `Tables.finish`, but all three facade paths raised `MethodError` while
     trying to convert `Month` to `Second`.

   Checked microsecond and nanosecond overflow did take the fallback in 2/2
   controls. The remaining problem is the conversion contract: exact physical
   representation is not enough. Scan lowering must preserve the actual
   facade comparison domain, and every unsupported conversion must return the
   unpushable tag instead of throwing.

2. **HIGH — the required nullable `Int64 => Float64` override still fails,
   and successful overrides retain a stale Arrow schema.**

   `_wrapscanned` applies an override with `collect(T, converted)` at
   `src/table.jl:519-526`. That is not the missing-preserving conversion used
   by `Tables.finish`. For
   `Union{Missing,Int64}[1, missing]` with a plain `Float64` override, the
   authority returns `Union{Missing,Float64}[1.0, missing]`. File, stream, and
   ranged facade scans all raise:

   ```text
   MethodError: Cannot convert Missing to Float64
   ```

   The new test at `test/facade_tests.jl:384-394` requests
   `Union{Missing,Float64}` instead of the required plain `Float64`, so it does
   not exercise this case.

   `_boundschema` has a second defect at `src/table.jl:492-505`. It copies the
   source `f.type` and ignores `bc.type`. If an `Int64 => Float64` scan has no
   missing value, or explicitly requests `Union{Missing,Float64}`, the output
   column is Float64 but its retained field is still Int64. Rewriting each
   file, stream, and ranged result then fails:

   ```text
   ArgumentError: column x no longer matches its retained Arrow type
   Arrow.ArrowCore.IntType; it now maps to Arrow.ArrowCore.FloatType
   ```

   A bound output schema must describe the actual overridden output type, or
   the writer must re-infer an overridden field. `Date => Date` is a valid
   no-op and passed in all three paths.

3. **HIGH — retained rewrites still accept incompatible visible columns.**

   `_retainedstorage` now calls `_storagevalue` at `src/write.jl:122-138`, but
   scan-literal compatibility and retained-column identity are different
   contracts. The writer does not first prove that the visible column has the
   retained facade type.

   Replacing a retained Date32 `Vector{Date}` with a midnight
   `Vector{DateTime}` succeeded. The rewrite silently produced Date32 and read
   back as `Vector{Date}`. The input column type changed without an error.

   The retained dictionary path has a separate nullability hole.
   `_retaineddict` copies `nullable=false` at `src/write.jl:264-279`, while
   `_dictbatch` creates a null bitmap at `src/write.jl:249-260`. The path at
   `src/write.jl:327-353` bypasses the non-nullable check in `_writecolumn`.
   A retained `Dictionary(Int8, Utf8, ordered=true, nullable=false)` accepted
   `Union{Missing,String}["a", missing]`. `Arrow.write` succeeded, then
   rereading the result failed with `MethodError: Cannot convert Missing to
   String`.

   The added combined replacement test fails first on its Date32 column, so it
   does not reach the dictionary replacement. Separate controls confirmed that
   Int64 replacements into Date32 and Dictionary, and missing values in a
   non-nullable primitive Int64 column, now fail with clear `ArgumentError`s.

4. **MEDIUM — zero-field scan row counts are still incomplete.**

   `_lowerscan` rejects an empty bound output only when `fields` is nonempty at
   `src/table.jl:342-352`. A zero-field RangedFile therefore remains pushable.
   Its explicit empty push selection loses the count before `_wrapscanned` can
   preserve it:

   ```text
   three-row zero-field source, Scan()
   file=3   stream=3   ranged=0
   ```

   The default ranged `Arrow.Table(source)` path also returned 0.

   `_publicscan` loses a different zero-field count at
   `src/table.jl:443-457`. It sends filter and window work through
   `Tables.finish`, whose empty `NamedTuple` cannot carry the count. On a
   three-row zero-field source, `Scan(limit=1, offset=1)` returned 0 for file,
   stream, and ranged inputs; the requested count is 1.

   Empty projections over nonempty schemas passed 18/18, including combined
   filter, limit, offset, and select cases. Reporting zero-column partitions
   also passed 6/6.

5. **MEDIUM — an unpushable ranged scan fetches the whole object, but the
   public cost contract still states the opposite.**

   The fallback at `src/table.jl:409-411` first performs a full ranged scan and
   then calls `_publicscan`. On a 1,051,410-byte object while selecting only
   `id`, the measured cost was:

   ```text
   pushable scan    2,400 bytes in 8 calls
   fallback scan   1,051,492 bytes in 9 calls
   ```

   The fallback fetched the unselected 1,048,576-byte string body. This is the
   declared correctness-first design cost, but `Arrow.Table` still says that
   selected columns are the only columns decoded and that ranged pruning
   happens before bytes are fetched at `src/table.jl:37-41`. The public docs
   need an explicit fallback exception so a remote range user can plan for a
   full-object read.

6. **LOW — integer-width drift errors still omit width and signedness.**

   `src/write.jl:363-368` still formats descriptors with `summary(type)`.
   Int64 to Int32, Int32 to Int64, and Int32 to UInt32 drift all report only
   `Arrow.ArrowCore.IntType`. The partition and column are present, but the two
   mismatching descriptor values remain hidden.

7. **LOW — the target range still fails the whitespace check.**

   `src/scan.jl:788` still contains trailing whitespace. The latest commit did
   not touch that line.

## Clean portions of the closing sweep

- The exact five-row round-34 temporal differential passed 15/15 across file,
  stream, and ranged paths: Date32 against midnight DateTime, Date64 against
  DateTime, Timestamp seconds and milliseconds against Date, and Date against
  an integer.
- The named non-midnight DateTime fallback, finer-than-unit Time fallback,
  nested `Cmp`/`In`/boolean cases, and combined nonempty window/projection
  cases passed 35/35. Checked scaling passed 2/2 overflow controls.
- The retained transition table passed all four file/stream read-write format
  combinations. Date32, Date64, Timestamp s/ms/us/ns, Time s/ms/us/ns, and
  Duration s kept descriptor, nullability, and metadata. The requested
  `Dictionary(Int8, Utf8, ordered=true)` also kept its descriptor and flags.
- Normal renames and collisions bound the correct Timestamp-second field,
  timezone, nullability, field metadata, and schema metadata. Rewrites stayed
  identity across file, stream, and ranged inputs. A direct output-width skew
  raised the requested `AssertionError`.
- `select=()` over a nonempty schema retained the filtered/windowed count in
  all three paths. Zero-column reporting partitions passed.
- DataAPI missing, selected-away, and renamed-column checks passed 21/21.

## Assumptions and decisions

- I used `Tables.finish` over fully converted public values as the predicate
  and projection authority. For a zero-field result, I used the explicit
  source row count and the requested window arithmetic because an empty
  `NamedTuple` cannot report that count.
- I treated retained identity as including the visible facade type, logical
  descriptor parameters, nullability, schema metadata, and field metadata.
- I treated “missing into non-nullable” as applying to dictionary indices as
  well as primitive and temporal columns.
- I treated the full ranged fallback read as an accepted design choice only
  when its cost is stated in the public API documentation.
- I reopened the two round-34 low findings because round 35 sets a zero-finding
  convergence bar.
- I made no product or test changes. I preserved the six pre-existing
  untracked files and added only this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 121/121, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6 and zero
  trim-verifier errors.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` — exit
  0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` — exit
  0; 170 pass / 0 fail / 43 skip.
- Focused temporal closure controls — 15/15 exact differential, 35/35 named
  fallback/nested/window controls, and 2/2 checked-overflow controls passed.
  Separate probes captured six sub-millisecond path mismatches, three Date32
  boundary exceptions, and three Duration calendar-period exceptions.
- Focused retained and bound-schema controls — the full transition matrix,
  specified replacement refusals, renames, collisions, and width assertion
  passed. Separate probes captured the Date32 cross-type replacement,
  non-nullable dictionary replacement, plain nullable Float64 override, and
  stale overridden schema.
- Focused row-count and DataAPI controls — nonempty empty projections 18/18,
  reporting partitions 6/6, and DataAPI 21/21 passed. The zero-field suite had
  14 pass / 4 fail.
- `git diff --check 8359aab08a08adcf8e2363a640ed1519e40a338a..HEAD` — exit 2;
  trailing whitespace at `src/scan.jl:788`.

VERDICT: FINDINGS
