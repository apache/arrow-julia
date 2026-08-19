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

# Arrow.jl 3.0 code review — round 37

Date: 2026-08-17

Scope: exact commit `5f5715cdb0805a193d0b4c761c8ae41ae54cb395`
on `core-rewrite`. I reviewed the round-36 fix against code head
`35f8447b25d96f3959aacb48744eab80a2f03efe` and the report commit
`fcccf2e856eb98176d29f000f40a63828fd72241`. I re-ran the requested
file, stream, and ranged probes. I used the manifest-selected Tables.jl
checkout at `d1fbb6eb577741688dba70039754166b51c1cdcc` as the authority.

## Result

Round 36 is not closed. Four findings remain: two HIGH and two MEDIUM.

The requested nullable no-op matrix, observed-missing control, retained
dictionary rejection, poison-vector identity checks, false-valued zero-field
matrix, ranged limits, and whitespace check pass. The `Date => Int64`
override also runs after facade conversion and matches `Tables.finish`.

The override conversion branch still does not match `Tables.finish` when it
must build a new vector. The zero-field fix treats every filter as false.
A runtime list no-op loses its retained field and column metadata. The public
ranged-cost text still omits the nonempty-source `select=()` fallback from
round 36.

## Findings

1. **HIGH — `_applyoverride` still differs from `Tables.finish` after the
   no-op check.**

   The early subtype return at `src/table.jl:577-580` is correct. The
   conversion branch at `src/table.jl:581-587` is not. It removes `Missing`
   from the requested target. It then selects output nullability from the
   declared source element type:

   ```julia
   TN = Base.nonmissingtype(T)
   if eltype(col) >: Missing
       return Union{Missing,TN}[...]
   end
   ```

   The authority in Tables.jl first applies the same no-op check. For a real
   conversion, it preserves the requested `T` and uses observed missing
   values. The current code therefore fails in both directions:

   ```text
   source / override                         Tables.finish                 facade
   Int64[1,2] => Union{Missing,Float64}       Union{Missing,Float64}        Float64
   Union{Missing,Int64}[1,2] => Float64       Float64                       Union{Missing,Float64}
   empty ListType facade, Any => Vector       Vector                        Union{Missing,Vector}
   ```

   File, stream, and ranged inputs produced every mismatch above. Values
   were equal, but declared element types were not. Rewriting the first two
   scalar results also recorded the opposite Arrow field nullability from the
   authority:

   ```text
   override                                  authority rewrite   facade rewrite
   Int64 => Union{Missing,Float64}            nullable=true       nullable=false
   Union{Missing,Int64} => Float64            nullable=false      nullable=true
   ```

   The empty-list control used a real `ListType(false)` descriptor and zero
   rows. Its facade column had `eltype == Any`. Direct helper controls with
   both `Any[]` and nonempty `Any[Vector{Any}([1])]` showed the same added
   `Missing`. An observed-missing `Any` control matched the authority.

   The requested observed-missing
   `Union{Missing,Int64}[1,missing] => Float64` control also matched on all
   three paths. That control reaches the one branch where declared and
   observed nullability agree. The implementation must keep the early no-op,
   then follow the authority's observed-missing rule while preserving the
   requested target type.

2. **HIGH — zero-field scans still treat every filter as false.**

   `_publicscan` binds a validated scan at `src/table.jl:463`, but lines
   `465-466` then set the row count to zero for every non-`nothing` filter.
   This fixes `AlwaysFalse` and an unknown comparison. It does not implement
   the filter contract.

   The following results occurred on file, stream, and ranged facade paths:

   ```text
   scan                                             expected   facade
   filter=AlwaysTrue()                              3 rows     0 rows
   AlwaysTrue(), limit=1, offset=1                  1 row      0 rows
   isnull(col(:gone)), validate=false               3 rows     0 rows
   same missing-column filter, limit=1, offset=1    1 row      0 rows
   ```

   Tables.jl defines an unmatched filter reference under `validate=false` as
   an all-missing column. `isnull(col(:gone))` must therefore keep every row.
   A constant `AlwaysTrue` filter must also keep every row. Direct
   `Tables.scan` over `ArrowFile` and `RangedFile` has the same true-filter
   count loss.

   The added regressions at `test/facade_tests.jl:496-504` cover only
   predicates whose correct result is empty. A complete fix must evaluate the
   zero-column predicate semantics, preserve the explicit source count when
   it is true, and then apply the window. It should not allocate a mask whose
   size comes directly from an untrusted row count.

3. **MEDIUM — a runtime list-to-`Vector` no-op loses its retained field and
   column metadata.**

   A nonempty list facade materializes with element type `Vector{Any}`.
   `Vector` therefore accepts it under the same subtype rule as
   `Tables.finish`. `_applyoverride` correctly returns it unchanged on file,
   stream, and ranged inputs.

   `_boundschema` makes a different decision at `src/table.jl:528-533`. It
   tests `_facadeeltype(f)`, which is statically `Any` for `ListType`, instead
   of the actual narrowed column element type. It classifies the override as
   a conversion and removes the retained field.

   All three paths had equal values and the authority element type
   `Vector{Any}`. All three also had these identity failures:

   ```text
   retained fields before override    1
   retained fields after override     0
   schema metadata                    retained
   field metadata                     lost
   ```

   Both file and stream rewrite attempts then raised
   `ArgumentError: fromjulia: unsupported element type Any` on every input
   path. The unchanged list facade currently has the same nested writer
   limitation, so I do not attribute that full writer gap to this commit.
   The round-37 defect is the separate decision to discard a retained field
   and its metadata for an actual no-op. Schema retention must use the same
   actual-column subtype decision as `_applyoverride`.

4. **MEDIUM — the round-36 empty-projection ranged-cost disclosure is still
   missing.**

   The public text at `src/table.jl:37-49` says that only selected columns are
   decoded. Its exceptions now name unrepresentable filter literals and a
   zero-field source. The round-36 finding concerned a different case:
   `select=()` on a source that has columns.

   `_lowerscan` still marks that request unpushable at `src/table.jl:346-350`.
   The facade reads all selected-away data to recover the row count. The
   round-36 cost probe remains reproducible:

   ```text
   10,000-row Int64 object               80,466 bytes
   low-level ranged plan, select=()         352 bytes in 4 calls
   Arrow.Table facade, select=()          80,560 bytes in 8 calls
   facade row count                       10,000
   ```

   The implementation can keep this fallback, but the public exception must
   name empty output projections over nonempty schemas. Adding the separate
   zero-field-source case did not close the reported documentation finding.

## Clean portions of the closing sweep

- The prompt labels the nullable no-op matrix as 6/6 but spells seven
  override pairs. I tested all seven on file, stream, and ranged inputs.
  Reads passed 21/21. Both file and stream rewrites passed 42/42. Declared
  element types, values, descriptors, nullability, field metadata, and schema
  metadata were retained for `Int64`, `Real`, `Integer`, `Number`, `Any`,
  `Date`, and `AbstractString` targets.
- The observed-missing `Float64` control passed 3/3 reads and 6/6 rewrites.
- A conflicting `Date => Int64` override passed 3/3 authority comparisons.
  `Tables.finish` and every facade path raised the same `MethodError` naming
  `Date` and `Int64`. No path cast the raw Date32 storage integer.
- A non-nullable retained ordered `Dictionary(Int8, Utf8)` replacement that
  could hold missing failed cleanly for all six input/output transitions.
  Every failure was an `ArgumentError`. Every output buffer stayed at zero
  bytes.
- Retained Int64, UTF-8, and dictionary poison replacements passed 18/18.
  All three input paths and both output formats rejected on declared type.
  Every poison-vector index counter stayed at zero.
- The requested zero-field facade matrix passed 69/69 across file, stream,
  and ranged inputs. This included defaults, windows, `AlwaysFalse` under both
  validation settings, unknown comparison under both settings, and invalid
  selection under both settings. The true-filter finding above is the missing
  complementary branch.
- Ranged zero-field limits passed 3/3. Direct `Tables.scan`, explicit facade
  `Scan()`, and the default facade read all rejected a three-row batch under
  `Limits(max_array_length=2)` with `ValidationError`.
- `git diff --check 8359aab..5f5715c` exited 0. The cumulative
  round-35-through-now range is clean.

## Assumptions and decisions

- I used `Tables.finish` over fully converted public facade values as the
  authority for values and declared output element types.
- For zero-field outputs, I used the explicit source row count and Tables.jl
  predicate semantics. An empty `NamedTuple` cannot carry a positive count.
- I treated a no-op as retained identity for its descriptor, nullability,
  field metadata, and schema metadata.
- I treated the actual post-facade column element type as authoritative when
  deciding whether an override is a no-op. The descriptor mapping to `Any`
  is too coarse for that decision.
- I accepted a full ranged fallback as a design choice only when its cost is
  stated in the public API text.
- I added true-valued zero-field filters, both mirror-image nullability
  conversions, and empty and nonempty list controls. The changed branches
  claim exact or universal behavior, so these controls were needed to test
  the root cause.
- I made no product or test changes. I preserved the six pre-existing
  untracked files and added only this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 142/142, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6 and no
  trim verifier errors.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` — exit
  0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` — exit
  0; 170 pass / 0 fail / 43 skip.
- Focused override probe — the required reads, rewrites, dictionary, poison,
  and Date-conflict groups passed with the counts above. The combined command
  exited 1 only because it also asserted the eight reported Any/list failures:
  three empty-list type mismatches, three list-retention failures, and two
  direct-Any helper mismatches.
- Focused zero-field probe — requested matrix 69/69 and ranged limits 3/3
  passed. Separate true-filter controls reproduced the finding on all three
  facade paths.
- Ranged empty-projection probe — reproduced 352 bytes in 4 calls for the
  low-level plan and 80,560 bytes in 8 calls for the facade.
- Cumulative whitespace check — exit 0.

VERDICT: FINDINGS
