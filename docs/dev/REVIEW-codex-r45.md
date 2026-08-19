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

# Arrow.jl 3.0 code review — round 45

Date: 2026-08-17

Scope: exact fix commit `66588198fdd78091ac7cfd56d0cf5d0d13b7971e`
(`fix: resolve round 44 findings — descriptor preflight, generated structs`)
on `core-rewrite`. Its parent,
`d5873a9c1c029150f9059654c46bba5f83109eb1`, records the round-44 review
of feature commit `d74c909c70759d803ba9b36033965a38b985dd96`. I reviewed the
fix diff and reran the full round-44 regression surface at the exact fix
commit.

## Result

Round 45 is not clean. The four round-44 defects close in their requested
correctness probes. I found two new MEDIUM issues.

The new generated Struct path compiles at every requested arity. Descriptor
preflight rejects the five false-accept classes. Recursive typed access now
preserves child logical bounds. Struct field names no longer convert in the
row loop. However, the compiled `_typedchild` recursion boundary allocates on
every Struct field read in a normal fresh process. This makes the fixed path
allocate 66% more than the round-44 implementation. A non-concrete
NamedTuple Union claim also reaches the generated preflight and throws an
internal `MethodError` instead of the documented `ArgumentError`.

All five repository gates pass. The targeted runtime and trim correctness
matrices also pass. Those gates do not cover the two issues below.

## Findings

1. **MEDIUM — the `_typedchild` recursion boundary causes a larger per-row
   allocation regression.**

   `_typedchild` is the new compiled recursion boundary at
   `src/ArrowCore.jl:2328-2331`. The generated Struct row calls it once per
   field at `src/ArrowCore.jl:2426-2430`. The materialize loop repeats that
   path for every row at `src/ArrowCore.jl:2485-2491`. List,
   FixedSizeList, Dictionary, and REE use the same boundary at
   `src/ArrowCore.jl:2393`, `:2408`, and `:2441-2447`.

   I reran the exact round-44 allocation workload in separate fresh Julia
   processes. It materializes 100,000 rows of
   `NamedTuple{(:a,:b),Tuple{Int64,Int64}}`. I warmed each exact source three
   times and repeated each measurement five times.

   - The round-44 parent allocates 14,405,696 bytes.
   - This fix allocates 23,972,992 bytes.
   - The regression is 9,567,296 bytes, or 95.67 bytes per row and 66.41%.
   - An equal-output control with the same child logical-bounds check but an
     inline `_typedvalue_of` edge allocates 8,005,696 bytes.
   - `_checkclaim` itself allocates zero in the warmed 100,000-call control.

   Allocation profiles confirm that the old per-row `String` conversions are
   gone. The new boxes root at `_typedchild` line 2330. An isolated fresh
   process allocates 48 bytes per `_typedchild(Int64, ...)` call, while the
   direct `_typedvalue_of` control allocates zero. A manual
   `Core.Compiler.return_type` query before the workload removes this cost.
   Normal application code must not need compiler-introspection priming to
   reach the intended allocation behavior.

   The finding-4 name work is closed narrowly, but its replacement is worse
   in the same hot loop. Keep the child bounds guard and trim-resolvable edge,
   but remove the compilation-order-dependent boxing. Add a fresh-process
   allocation regression so inference inspection cannot prime the result.

2. **MEDIUM — a NamedTuple Union claim fails inside the generated preflight
   instead of refusing with `ArgumentError`.**

   Struct preflight accepts any `E <: NamedTuple` at
   `src/ArrowCore.jl:2256-2262`. A Union of NamedTuple types satisfies that
   subtype test, and `fieldcount(E)` can match the Struct. The generated
   `_checkstructclaim` then calls `fieldnames(E)` at
   `src/ArrowCore.jl:2288-2295`. Base has no such method for a Union.

   Against a valid one-field Int64 Struct, this incompatible claim throws
   `MethodError` from line 2292 through both public entry points:

   `Union{NamedTuple{(:a,),Tuple{Int64}},NamedTuple{(:a,),Tuple{String}}}`

   The public contract at `src/ArrowCore.jl:2176-2184` says mismatches refuse
   with `ArgumentError`. Public preflight reaches this generator from
   `src/ArrowCore.jl:2188` and `:2195`. The call fails closed, so I do not rank
   it HIGH. It still leaks an internal generation error for a public type
   claim. Refuse non-exact Struct claim shapes before calling the generator.

## Round-44 closure

- Finding 1 is closed. `_structrow` is a flat generated tuple expression with
  literal field types. Heterogeneous arities 0 through 6 and homogeneous
  arities 4 and 6 compile separately with `--trim=safe`. Every compiler and
  binary exits 0, with zero verifier errors and zero verifier warnings.
- Finding 2 is closed for the requested valid descriptors. Empty Int64 under
  String, empty Arrow Union under Int64, all-null Int64 under
  `Union{Missing,String}`, empty List<Int32> under `Vector{Int64}`, and a null
  Struct under a wrong NamedTuple all throw `ArgumentError` from both public
  entry points. The recursive preflight also trim-compiles through
  `Dictionary<REE<Dictionary<Int64>>>`.
- Finding 3 is closed. List, FixedSizeList, ListView, Struct, REE, and
  Dictionary typed reads now throw the same `BoundsError` as dynamic reads
  when a child has one logical value over a two-value backing buffer.
  Negative dictionary indices and index overflow keep dynamic/typed error
  parity.
- Finding 4 is closed narrowly. Field-name strings bake into
  `_checkstructclaim`; `_structrow` contains no name conversion, and the
  warmed preflight allocation control is zero. Finding 1 above is a distinct
  regression caused by the new recursion boundary.
- The prior valid-layout matrix stays clean for FixedSizeList, ListView,
  Decimal, Interval, View, FixedSizeBinary, Map, REE, Dictionary, sliced and
  offset variants, and the requested compositions. Typed and dynamic values
  and missing placement agree. Refusals do not convert. The overloads remain
  ambiguity-free, and public inference returns the exact claimed types.

## Assumptions and decisions

- I treated `T` as a descriptor-level element-domain assertion, as in round
  44. Empty and null data do not weaken the required base claim.
- I treated the NamedTuple Union as an incompatible, non-exact Struct claim.
  It may fail closed, but the documented managed refusal is still
  `ArgumentError`. I ranked the leak MEDIUM because it affects public error
  discipline but does not accept or convert a value.
- I treated a 66% allocation increase in the same typed materialization hot
  loop as a regression even though the literal name conversions are gone.
  I ranked it MEDIUM because values remain correct.
- Public `getvalue` preflight is proportional to schema width and wrapper
  depth. It allocates zero after warm-up in the checked Struct and Dictionary
  cases, and row construction has the same width/depth order. I accepted this
  explicit preflight cost.
- Generated rows through 256 fields worked under ordinary Julia. A 512-field
  stress compile was still running after 60 seconds and used about 1.1 GB, so
  I stopped only that scratch process. I did not make this a separate finding:
  the caller must supply the large static NamedTuple type, and this review has
  no bounded compile-time contract.
- The host is 64-bit arm64 macOS and used Julia 1.12.6. All probes and copied
  parent sources live in scratch directories under `/tmp`.
- I made no product or test change. The six pre-existing untracked files
  remain present and unmodified. This review document is the only repository
  change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  657/657 total: threaded caches 4/4, ArrowCore 381/381, facade 268/268, and
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
  the exact fix commit.
- Round-44 runtime matrix — exit 0; 197/197: claim preflight 20/20, hidden
  child bounds 32/32, valid layout/value/missing parity 109/109,
  refusal/ambiguity/inference 33/33, and allocation assertions 3/3.
- Arity trim matrix — all nine compile exits 0, every binary exits 0, and all
  nine logs contain zero verifier errors and zero verifier warnings.
- `Dictionary<REE<Dictionary<Int64>>>` — ordinary exit 0; trim compile exit 0;
  zero verifier errors and warnings; binary exit 0; both public typed entry
  points return the expected values.
- Allocation comparison — all parent and fix probe processes exit 0. Five
  repeated measurements are stable at 14,405,696 and 23,972,992 bytes. The
  bounds-safe control exits 0 with equal output at 8,005,696 bytes.
- Hostile NamedTuple Union probe — exit 0 after catching the two public-call
  failures; both captured exceptions are `MethodError`, not `ArgumentError`.
- Final repository status retains the six protected untracked files plus this
  review file. No protected file was inspected or modified.

VERDICT: FINDINGS
