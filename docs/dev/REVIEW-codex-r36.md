# Arrow.jl 3.0 code review — round 36

Date: 2026-08-17

Scope: exact commit `35f8447b25d96f3959aacb48744eab80a2f03efe`
on `core-rewrite`. I reviewed the round-35 fix against code head
`434e8de7e404320e679d12b3637e306b595bd5a4`. I also re-ran the exact
round-35 focused probes over file, stream, and ranged inputs.

## Result

Round 35 is not closed. The temporal facade-domain fix is sound. The required
nullable `Int64 => Float64` control, the main retained transition table, the
basic zero-field count matrix, and the improved descriptor errors also pass.

Seven findings remain. The override helper still differs from
`Tables.finish`. The retained dictionary path can still emit an unreadable
file. Identity-first rejection still covers only temporal fields. The new
zero-field branch ignores filters and validation, and its ranged shortcut
discards caller limits. The public ranged-cost exception is incomplete. The
exact cumulative round-35 diff range still has trailing whitespace.

## Findings

1. **HIGH — `_applyoverride` does not match `Tables.finish`, including for a
   nullable no-op.**

   `Tables.finish` first returns the source column when its element type is
   already a subtype of `Union{T,Missing}`. `_applyoverride` at
   `src/table.jl:568-575` always rebuilds the column. It decides output
   nullability from observed values instead of the declared source element
   type.

   A nullable source with no observed missing values exposes the difference:

   ```text
   override                         Tables.finish                 facade, all paths
   Union{Missing,Int64} => Int64    Union{Missing,Int64}          Int64
   Union{Missing,Date}  => Date     Union{Missing,Date}           Date
   ```

   File, stream, and ranged inputs produced 6/6 element-type mismatches.
   Values stayed equal, but the explicit `Date => Date` no-op was not a no-op.

   Abstract supertypes expose a second form of the same defect. `Real`,
   `Integer`, `Number`, and `Any` should leave a nullable Int64 column
   unchanged. All 12 file, stream, and ranged outputs instead used abstract
   element types. All 12 rewrites then failed with errors such as:

   ```text
   ArgumentError: fromjulia: unsupported element type Union{Missing, Real}
   ```

   `AbstractString` similarly changed `Union{Missing,String}` into
   `Union{Missing,AbstractString}` on all three paths. Its rewrites succeeded,
   but all three lost the retained field metadata.

   `_boundschema` compounds the problem at `src/table.jl:517-523`. It treats
   every non-identical override base type as a changed type and removes the
   retained field. The authority treats a supertype that already accepts the
   source element type as a no-op.

   The requested plain `Float64` override with an observed missing value
   passed on all paths. `String` no-op and the failing `Symbol`/`String`
   conversion controls also matched the authority. The missing case does not
   cover the declared-nullable, no-observed-null branch.

2. **HIGH — a non-nullable retained dictionary still accepts missing and
   emits unreadable output.**

   `_dictbatch` creates a validity bitmap and nonzero null count at
   `src/write.jl:252-261`. `_retaineddict` then copies `nullable=false` at
   `src/write.jl:264-280`. The retained dictionary branch at
   `src/write.jl:327-354` has no missing-value rejection.

   I replaced a retained
   `Dictionary(Int8, Utf8, ordered=true, nullable=false)` column with
   `Union{Missing,String}["a", missing]`. All six file/stream/ranged input to
   file/stream output transitions reported write success. Every reread failed:

   ```text
   MethodError: Cannot convert Missing to String
   ```

   The combined replacement regression at `test/facade_tests.jl:373-381`
   still throws first on its Date column. It never reaches its dictionary
   column. The separate round-35 dictionary defect therefore remains
   untested and open.

3. **MEDIUM — identity-first rejection still applies only to temporal
   retained fields.**

   The new declared-type gate at `src/write.jl:141-160` runs before temporal
   value conversion. Primitive and UTF-8 retained fields instead build a
   natural column before comparing descriptors at `src/write.jl:162-176`.
   Retained dictionaries enumerate `unique(skipmissing(vals))` before their
   value descriptor is checked at `src/write.jl:327-345`.

   I used wrong-typed vectors whose `getindex` throws
   `ErrorException("POISON VALUE INTERPRETATION")`. These three replacements
   all indexed the vector before rejecting its declared type:

   ```text
   retained Int64             <- PoisonVec{String}
   retained Utf8              <- PoisonVec{Int64}
   retained Dictionary{Utf8}  <- PoisonVec{Int64}
   ```

   The Date32 control rejected all wrong-typed file, stream, and ranged
   replacements with `ArgumentError` without indexing the vector. The same
   identity-first contract must apply to other retained fields whose facade
   type is already known.

4. **HIGH — zero-field scans ignore filters and scan validation.**

   The no-column branch in `_publicscan` at `src/table.jl:453-463` applies
   only offset and limit arithmetic. It does not bind the scan. It does not
   evaluate `scan.filter`. The comment that a filter cannot reference anything
   also misses constant scan expressions and `validate=false` missing-column
   semantics.

   On a three-row, zero-field source, all file, stream, and ranged paths
   produced these results:

   ```text
   scan                                      expected       facade
   filter=AlwaysFalse()                      0 rows         3 rows
   AlwaysFalse(), limit=1, offset=1          0 rows         1 row
   unknown comparison, validate=false       0 rows         3 rows
   unknown comparison, validate=true        ArgumentError  3 rows
   invalid select, validate=true             ArgumentError  3 rows
   ```

   This is silent predicate and validation loss. The requested `Scan()` and
   limit/offset-only cases pass, but they do not exercise the rest of the scan
   contract.

5. **HIGH — the ranged zero-field shortcut discards the caller's limits.**

   The exact whole-object fetch at `src/table.jl:406-411` is correctly guarded
   by `isempty(rfields)`. After the fetch, however, it calls `readfile(bytes)`
   without `limits=rf.limits`. That selects the default limits at
   `src/ipc_write.jl:1234-1235` and bypasses the ranged checks, including the
   record limit at `src/scan.jl:119-123`.

   I used one zero-field record batch with three rows and
   `Limits(max_array_length=2)`:

   ```text
   Tables.scan(rf, Scan())        ValidationError: record batch length 3 exceeds limit
   Arrow.Table(rf; scan=Scan())   success, 3 rows
   Arrow.Table(rf)                success, 3 rows
   ```

   The shortcut must keep the complete `RangedFile` limit and allocation
   contract. A schema shape must not switch validation policy.

6. **MEDIUM — the public ranged fallback exception is still incomplete.**

   The new paragraph at `src/table.jl:43-47` now states the whole-object cost
   for an unrepresentable filter literal. The main text still says that only
   selected columns are decoded. `_lowerscan` at `src/table.jl:344-354` also
   makes every nonempty-schema `select=()` request unpushable, so the facade
   reads all columns to recover the row count.

   On an 80,466-byte ranged file with one 10,000-row Int64 column:

   ```text
   direct Tables.scan, select=()    352 bytes in 4 calls
   Arrow.Table, select=()        80,560 bytes in 8 calls
   ```

   The facade result had the correct zero-column row count, but it fetched the
   selected-away 80,000-byte body. The public exception must also state this
   empty-output fallback, or the implementation must preserve the count
   without the full public-table fallback.

7. **LOW — the exact cumulative round-35 range still fails the whitespace
   check.**

   Re-running the command recorded in round 35 gives:

   ```text
   git diff --check 8359aab08a08adcf8e2363a640ed1519e40a338a..35f8447b25d96f3959aacb48744eab80a2f03efe
   src/scan.jl:788: trailing whitespace.
   exit 2
   ```

   `git diff --check 06b9e1d..35f8447` exits 0 only because the latest commit
   did not touch `src/scan.jl:788`. The prior LOW finding remains in the same
   requested cumulative range.

## Clean portions of the closing sweep

- Temporal closure passed 35/35. Timestamp microsecond and nanosecond columns
  compared with physically matching `DateTime` literals returned no rows on
  file, stream, and ranged inputs, equal to `Tables.finish`. Both scans were
  tagged unpushable.
- `Date(6_000_000, 1, 1)` against Date32 and `Month(1)` against a Second column
  took the public fallback on all three paths. They returned no rows without
  an exception.
- The exact five-row round-34 differential passed 15/15 across all three
  paths. The supported facade-domain branch matrix passed 58/58. The code uses
  one `_facadebasetype` lookup at `src/table.jl:243`, and its F-driven branches
  match `_postconvert` for every supported temporal descriptor.
- The round-35 concrete override, rename, metadata, rewrite, and standard Date
  no-op controls passed 102/102. The full retained transition matrix passed
  654/654 across file, stream, and ranged inputs and both output formats.
  Date32/64, Timestamp s/ms/us/ns, Time s/ms/us/ns, Duration s/ms/us/ns, and
  ordered `Dictionary(Int8, Utf8)` kept values, descriptor parameters,
  nullability, field metadata, and schema metadata.
- Temporal identity-before-value controls passed 54/54. Normal primitive
  nullability and dictionary wrong-type refusal controls passed 12/12.
- The requested zero-field count matrix passed 24/24. `Scan()`, default reads,
  limit-only, offset-only, combined windows, zero limits, and past-end windows
  kept the expected count on file, stream, and ranged inputs.
- The exact ranged `(0, object_length)` shortcut occurred only when
  `rfields` was empty. A zero-field log included the whole-object request. A
  nonempty control did not.
- Int64 to Int32, Int32 to Int64, and Int32 to UInt32 errors now show
  `IntType(bits, signed)` for both descriptors. Width and signedness are clear.

## Assumptions and decisions

- I used `Tables.finish` over fully converted public values as the authority
  for values and output element types.
- For zero-field outputs, I used the explicit source count plus filter and
  window semantics. An empty `NamedTuple` cannot carry a positive row count.
- I treated retained identity as including the visible facade type, descriptor
  parameters, nullability, field metadata, and schema metadata.
- I interpreted “before value interpretation” to mean that a known-incompatible
  declared element type must be rejected before the vector is indexed.
- I treated the exact round-35 cumulative diff command as the requested range.
  I also checked the latest single-commit range separately.
- I used file-format bytes for ranged controls. I used a small tail and no
  coalescing when I needed to distinguish an exact whole-object request.
- I added no product or test changes. I preserved the six pre-existing
  untracked files and added only this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 132/132, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6 and no
  trim verifier errors.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` — exit
  0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` — exit
  0; 170 pass / 0 fail / 43 skip.
- Focused temporal controls — 35/35 closure checks, 15/15 exact five-row path
  differentials, and 58/58 facade-domain branch checks passed.
- Focused override and identity controls — 102/102 requested standard cases,
  654/654 retained transitions, 54/54 temporal identity checks, and 12/12
  normal refusal controls passed. Separate probes captured the override,
  dictionary, and non-temporal identity failures above.
- Focused zero-field controls — 24/24 count and window cases passed. Separate
  probes captured filter, validation, and ranged-limit failures.
- Descriptor diagnostic controls — all three width/signedness transitions
  passed.
- `git diff --check 06b9e1d..35f8447` — exit 0.
- `git diff --check 8359aab08a08adcf8e2363a640ed1519e40a338a..35f8447` —
  exit 2; trailing whitespace at `src/scan.jl:788`.

VERDICT: FINDINGS
