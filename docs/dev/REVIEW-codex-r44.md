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

# Arrow.jl 3.0 code review — round 44

Date: 2026-08-17

Scope: exact feature commit `d74c909c70759d803ba9b36033965a38b985dd96`
(`feat: typed element access for static schemas (review R5)`) on
`core-rewrite`. Its parent,
`4c0781bcab2914ef08665d279953a4e3d06daccf`, records the clean round-43
facade closure. I reviewed only the R5 typed-element-access commit.

## Result

Round 44 is not clean. I found two HIGH and two MEDIUM issues.

The scalar, list, dictionary, REE, Map, View, Decimal, Interval, and
fixed-size paths return the same valid values and null placement as dynamic
access in the requested matrix. Exact type identity is the right R5 rule.
The overloads add no dispatch ambiguity. The official trim gate also passes.

The official gate does not cover a heterogeneous Struct with four fields.
That ordinary static schema fails trim verification. Claim checking is also
driven by visited values instead of the Arrow descriptor, so empty, all-null,
and empty-child data can certify an incompatible claim. Typed composite
recursion bypasses the child `ArrayData.len` guard that dynamic recursion
keeps. Finally, NamedTuple names are converted and checked for every field of
every row.

All five required repository gates pass. They do not cover these four cases.

## Findings

1. **HIGH — heterogeneous NamedTuple schemas with four or more fields fail
   trim verification.**

   The R5 comment promises that a concrete static schema makes every load
   statically resolvable at `src/ArrowCore.jl:2155-2161`. Struct extraction
   instead builds its row through an `ntuple(Val(fieldcount(E)))` closure at
   `src/ArrowCore.jl:2287-2295`. For a heterogeneous four-field row,
   `fieldtype(E, j)` does not stay field-specific inside that closure under
   JuliaC. The verifier sees `NTuple{4,Any}`.

   A standalone claim
   `NamedTuple{(:a,:b,:c,:d),Tuple{Int64,Int32,String,Float64}}` works under
   ordinary Julia. The same `materialize` workload under `--trim=safe` exits
   1 with four verifier errors and zero warnings. The unresolved sites are
   the `ntuple` call at `src/ArrowCore.jl:2290` and construction of `E(vals)`
   at `src/ArrowCore.jl:2295`. A `getvalue`-only workload fails at the same
   two sites.

   The boundary is not a generic width limit. A heterogeneous arity-0 through
   arity-6 compile matrix reports errors only for arities 4, 5, and 6.
   Heterogeneous arity 3 compiles and its binary exits 0. Homogeneous arities
   4 and 6 also compile and run. The official workload uses only two Struct
   fields at `test/trim_entrypoint.jl:181-187`, so its zero-error result does
   not establish the stated Struct claim.

   The Struct walk needs a compile-time-unrolled field construction that
   preserves each `fieldtype(E, j)` for ordinary wider heterogeneous schemas.
   The trim regression must include at least four heterogeneous fields for
   both public entry points.

2. **HIGH — static claim validation depends on observed rows instead of the
   declared element domain.**

   The API says that `T` asserts the element domain and that mismatches refuse
   at `src/ArrowCore.jl:2158-2174`. Scalar leaves say the claim must match
   `juliatype(t)` exactly at `src/ArrowCore.jl:2227-2238`. The implementation
   performs the validity check first. List child claims are checked only while
   a row's child window is visited at `src/ArrowCore.jl:2241-2261`, and the
   other composite checks are likewise inside value extraction at
   `src/ArrowCore.jl:2265-2308`. Bulk access performs no claim preflight before
   its row loop at `src/ArrowCore.jl:2352-2358`.

   Valid focused cases therefore false-accept incompatible schemas:

   - `materialize(String, empty_Int64)` returns `String[]`.
   - `materialize(Int64, empty_Union)` returns `Int64[]`, although
     `src/ArrowCore.jl:2319-2323` says every static Union claim refuses.
   - An all-null Int64 column accepts `Union{Missing,String}`.
   - Empty List<Int32> windows accept `Vector{Int64}`.
   - A null Struct accepts a NamedTuple with the wrong name and field type.

   These are valid empty or null data, not malformed buffers. They defeat the
   feature's schema-assertion purpose and make acceptance depend on whether a
   batch happens to contain a visible value. No conversion occurs, but the
   asserted domain is still false. Claim compatibility must be checked
   recursively from `T`, `Field`, and `ArrayData.type` before null handling,
   child-window iteration, or the materialize loop.

3. **MEDIUM — typed composite recursion bypasses child logical bounds and can
   return hidden backing values.**

   Public dynamic access checks `1 <= i <= d.len` at
   `src/ArrowCore.jl:1825-1827`. Dynamic List, FixedSizeList, Struct,
   Dictionary, ListView, and REE recursion returns through public `getvalue`
   at `src/ArrowCore.jl:1967`, `:1978`, `:1996`, `:2036-2037`, `:2068`, and
   `:2073-2074`.

   The typed variants call `_typedvalue_of` directly at
   `src/ArrowCore.jl:2259-2260`, `:2275-2276`, `:2292-2293`, `:2307-2308`,
   and `:2311-2314`. That helper's ladder at `src/ArrowCore.jl:2200-2224`
   has no logical bounds check.

   I gave each composite a child with logical length one and a backing buffer
   that still held a second physical value. Dynamic List, FixedSizeList,
   ListView, Struct, REE, and Dictionary reads all threw `BoundsError` at the
   child boundary. Typed reads returned the hidden second value: `[11, 22]`,
   `(x = 22,)`, `22`, or `"b"`.

   Final `BufferSlice` checks still prevent a raw region escape, and the
   normal validators reject this geometry. I therefore rank this MEDIUM.
   It is still a public typed/dynamic parity break on the explicitly requested
   unvalidated-access boundary. Recursive typed loads need the same logical
   child-bound guard as recursive dynamic loads.

4. **MEDIUM — NamedTuple schema-name validation allocates in every row.**

   `String(names[j])` is inside the row's `ntuple` closure at
   `src/ArrowCore.jl:2287-2293`. `_typedmaterialize_loop` repeats that closure
   for every row at `src/ArrowCore.jl:2352-2357`, even though the claimed names
   and `Field.children` do not change during the materialization.

   A warmed Julia 1.12.6 allocation split over a two-Int64-field Struct found
   64 bytes per row from the two name conversions/checks. The existing Struct
   validity lookup accounts for another 64 bytes per row. At 100,000 rows the
   current typed materialization allocated 14,405,696 bytes. A scratch control
   that retained the parent validity check but moved only the fixed name check
   before the loop allocated 8,005,760 bytes, with equal output. The
   R5-specific repeated-name cost was 6,399,936 bytes, about 64 bytes per row.

   This is pathological loop work for a static schema. The same recursive
   claim preflight required by finding 2 can check field count, names, and
   field claims once and remove this allocation from the element loop.

## Correct portions of R5

- Exact identity is the right leaf contract. Allowing an Int32 claim to read
  Int64 storage would either violate the `::T` result or introduce the
  conversion that R5 excludes. `Missing` should change null admissibility,
  not the nonmissing storage type.
- The requested valid-layout matrix passes FixedSizeList with child and parent
  offsets, sliced List offsets with a child offset, overlapping and sliced
  ListView windows, Decimal32/64/128, all three Interval units, inline and
  spilled string View entries, binary View, FixedSizeBinary with nulls, Map
  with an entries offset, REE with null value runs and a parent slice,
  Dictionary pool nulls and null indices, and List<Dictionary>.
- Typed values, dynamic values, and missing placement agree in every valid
  matrix case. NamedTuple Struct rows agree after normalizing the dynamic
  ordered `Vector{Pair{String,Any}}` representation by field name.
- Int32 on Int64, Integer on Int64, Float64 on Float32, Bool on Int8, String
  on Binary, Vector{Int64} on List<Int32>, and a deep NamedTuple field mismatch
  all refuse with `ArgumentError` and the relevant field name. No probe found
  a silent conversion.
- Top-level indices 0 and `len + 1` throw `BoundsError`. Short scalar backing
  buffers also throw a managed `BoundsError` in both paths.
- Nonempty Union static claims refuse. A dictionary without a pool throws
  `ValidationError`. A malformed REE child count throws a managed exception.
- `getvalue(Any, ...)` and `materialize(Any, ...)` delegate to the unchanged
  dynamic methods. `Test.detect_ambiguities(ArrowCore; recursive=true)` finds
  zero ambiguity across the two overload pairs.
- Inference returns the exact claimed type for scalar, nullable scalar, List,
  NamedTuple, and Map entry points. A focused typed Map materialization also
  compiles and runs under trim when the consumer uses only its statically
  known container shape.

## Assumptions and decisions

- I treated the type as a descriptor-level element-domain assertion, not a
  claim about only the non-null values observed in one batch. This follows the
  R5 static-schema goal and the new API documentation.
- I treated public typed recursion as required to preserve the dynamic path's
  child logical bounds even when a caller skipped semantic validation. This
  is the adversarial boundary requested for this round. I ranked the defect
  MEDIUM because validated adapter data rejects the malformed geometry and
  the final buffer bounds still prevent an address escape.
- I accepted `E === juliatype(t)` rather than subtype widening. Exact identity
  implements the requested assertion without conversion.
- I treated `Any` as the documented dynamic sentinel at the public root. The
  code does not define whether nested `Any` is a wildcard. I did not turn
  `Vector{Any}` or a NamedTuple `Any` field into a separate finding.
- I treated Map's exact `Vector{Pair{Any,Any}}` domain as the deliberately
  advertised dynamic Map representation. Materialization itself is
  trim-resolvable. Reading a Pair's `Any` field without a later type assertion
  is not statically resolvable, and `Vector{Pair{String,Int64}}` is refused.
  This limits Map's usefulness for R5 but does not contradict the narrower
  contract implemented in this commit.
- The host is 64-bit arm64 and used Julia 1.12.6. All probes and compiled
  binaries live in scratch directories under `/tmp`.
- I made no product or test change. The six pre-existing untracked files
  remain present and unmodified. This review document is the only repository
  change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  647/647 total: threaded caches 4/4, ArrowCore 371/371, facade 268/268, and
  each IPC read, IPC write, C Data, and ranged-scan acceptance battery 1/1.
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
- Requested typed-layout, refusal, slice, and hostile matrix — exit 1 by its
  assertions; 229/235 passed. The six failures are the List, FixedSizeList,
  ListView, Struct, REE, and Dictionary reproductions in finding 3. Every
  requested valid-layout and refusal assertion passed.
- Claim-preflight probe — exit 0; printed all five valid false-accept cases in
  finding 2.
- Four-field heterogeneous NamedTuple under ordinary Julia — exit 0. The same
  `materialize` trim compile — exit 1; four verifier errors, zero warnings.
  A `getvalue`-only compile has the same result.
- Heterogeneous NamedTuple arity-0 through arity-6 trim matrix — exit 1;
  twelve verifier errors, all at arities 4, 5, and 6. Heterogeneous arity 3
  and homogeneous arities 4 and 6 each compile with exit 0 and run with exit
  0.
- Focused typed Map trim materialization — compile exit 0 and binary exit 0.
- NamedTuple allocation split — exit 0; the two fixed name checks account for
  64 bytes per row, and the 100,000-row materialization and hoisted-name
  control produce equal values.
- API and inference probe — exit 0; zero method ambiguities and exact inferred
  public return types for every concrete claim checked.

VERDICT: FINDINGS
