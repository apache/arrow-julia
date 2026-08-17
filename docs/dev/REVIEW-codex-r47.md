# Arrow.jl 3.0 code review — round 47

Date: 2026-08-17

Scope: exact fix commit `acb5479e73bc157751bda854f8b857512253a4fb`
(`fix: resolve round 46 finding — complete the scalar fast ladder`) on
`core-rewrite`. Its parent,
`96aa44b37ca1a56942b5a1f92a0a23e4d334e91e`, records the round-46
review of code commit `8f0d9a1f919227bf42e275de79a13ab1dca9fac4`.
I reviewed the round-46 fix diff and reran the full round-45/46 regression
surface at the exact fix commit.

## Result

Round 46 is closed at the root. I found no issue of any severity.

Interval, View, and Null children now use the inline scalar ladder. Their
nested-read allocation matches bounds-preserving direct-leaf controls exactly.
The prior compiled-shell delta is zero for every Interval unit, Null, and
inline and spilled UTF-8 and Binary Views.

The ladder now contains exactly the 14 nonrecursive layouts in the 22-layout
registry. The fallback contains only the eight layouts that recurse through
physical children or an external dictionary value reference. No child-bearing
layout is in the inline ladder.

The two-`Int64` and `Int64` plus Interval repository workloads both allocate
exactly 8,005,696 bytes in fresh processes. The two-`Int64` workload also stays
at 8,005,696 bytes under every round-46 first-touch order.

All five required gates pass. The full correctness, refusal, bounds, parity,
ambiguity, inference, allocation, wrapper, and arity-trim matrices also pass.

## Findings

No findings of any severity.

## Closure of round 46

- The registry has 22 descriptors at `src/ArrowCore.jl:440-510`. Its 14
  nonrecursive leaves are Null, Bool, Int, Float, Decimal, FixedSizeBinary,
  Binary, Utf8, Date, Time, Timestamp, Duration, Interval, and View. Their
  registry rows are at `src/ArrowCore.jl:605-619` and `:630`.
- `_typedchild` contains exactly those 14 leaves at
  `src/ArrowCore.jl:2345-2358`. Interval, View, and Null are the final three
  branches at `:2356-2358`. The composite fallback is at `:2359`.
- List, FixedSizeList, Struct, Map, Union, Dictionary, ListView, and REE remain
  outside the inline ladder. Their registry rows are at
  `src/ArrowCore.jl:620-636`. List and ListView recurse at `:2413-2432`,
  FixedSizeList at `:2436-2447`, Struct at `:2451-2468`, Dictionary at
  `:2471-2480`, and REE at `:2483-2485`. Map reads its entry children at
  `:2001-2013`. Union has declared children but typed access refuses at
  `:2490-2494`.
- Dictionary is the only layout in that set with registry `childcount == 0`.
  Its values live in `ArrayData.dictionary`, not `ArrayData.children`, and its
  typed read recurses into that value array at `src/ArrowCore.jl:2476-2480`.
  It therefore belongs in the composite shell.
- The shared typed leaf method is at `src/ArrowCore.jl:2399-2410`. Null has
  its dedicated typed leaf at `:2487-2488`. Static child reads for List,
  ListView, FixedSizeList, Struct, Dictionary, and REE route through the
  guarded `_typedchild` edge at `:2341-2344`. Map retains its dynamic
  Any-valued entry reads.
- Every retained round-46 Interval probe now has zero actual/control delta.
  YEAR_MONTH Struct and child loops allocate 6,826,048 and 426,048 bytes.
  DAY_TIME allocates 8,819,264 and 2,419,264 bytes. MONTH_DAY_NANO allocates
  11,205,696 and 4,805,696 bytes. Each number repeated five times.
- Null Struct materialization allocates 6,400,064 bytes in both production and
  direct controls. Inline UTF-8 and Binary View Structs allocate 36,019,264
  and 32,819,264 bytes in both controls. Bounds-preserving extended controls
  also give zero delta for spilled UTF-8 and Binary Views in both production-
  first and control-first fresh processes.
- The built-in allocation pin at `test/typed_alloc_child.jl:26-58` prints
  `typed alloc ok: 8005696` and `typed interval alloc ok: 8005696`.

## Round-45/46 regression surface

- The exact two-`Int64` Struct workload allocates 8,005,696 bytes in all five
  repetitions for baseline, `getvalue`-first, Dictionary/REE-wrapper-first,
  and interleaved-claim first-touch orders. The interleaved order covers
  Int64, String, Bool, Float64, and `Vector{Int64}` claims.
- The hostile NamedTuple matrix passes 78/78. Fourteen Union, UnionAll,
  abstract-field, `Any`-field, wrong-arity, bottom, and Missing claims refuse
  through both public entry points with `ArgumentError`. None leaks
  `MethodError`. Missing-admitting valid claims retain exact values and
  inferred result types. Ambiguity count is zero.
- The round-44 runtime matrix passes 197/197: descriptor false-accept checks
  20/20, hidden child bounds parity 32/32, layout value and missing parity
  109/109, refusal/ambiguity/inference 33/33, and allocation checks 3/3.
- Heterogeneous NamedTuple arities 0 through 6 and homogeneous arities 4 and
  6 compile and run separately under `--trim=safe`. All nine compiler exits
  and all nine binary exits are 0. Every log contains zero verifier errors and
  zero verifier warnings.
- `Dictionary<REE<Dictionary<Int64>>>` exits 0 under ordinary Julia. Its trim
  compiler and binary also exit 0 with zero verifier errors and warnings.
  Both public typed entry points return the expected values.

## Assumptions and decisions

- I kept the round-44 through round-46 contract that `T` is an exact
  descriptor-level element-domain claim. Empty and null data do not weaken
  the required base claim.
- I classified layouts by value recursion, not only by registry
  `childcount`. This keeps Dictionary in the composite set because it follows
  `ArrayData.dictionary`.
- I kept Union outside the leaf ladder. It has declared children, even though
  typed access refuses during preflight.
- I used equal-output controls that retain the child logical-bounds guard and
  the parent Struct validity lookup. Allocation processes used three warmups,
  five measured repetitions, and no compiler introspection.
- I treated `Verifier error` and `Verifier warning` records as the trim
  diagnostics, as the repository harness does at
  `test/trim_compile_tests.jl:88-91`. The macOS linker emitted its known
  libunwind message. It is not a verifier diagnostic.
- The host is 64-bit arm64 macOS and used Julia 1.12.6. All probes, logs, trim
  projects, and compiled binaries live in scratch directories under `/tmp`.
- I made no product or test change. The six protected untracked files remain
  present and unmodified. This review document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  660/660 reported assertions: threaded caches 4/4, ArrowCore 384/384, facade
  268/268, and each IPC read, IPC write, C Data, and ranged-scan acceptance
  battery 1/1. Both fresh allocation workloads report 8,005,696 bytes.
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
- Exact retained round-46 Interval and omitted-leaf probes — exit 0; all
  actual/direct allocation pairs repeat five times with zero delta.
- Extended leaf allocation matrix — all 12 fresh processes exit 0; Interval,
  Null, and inline and spilled UTF-8 and Binary Views match bounds-preserving
  controls in both first-touch orders.
- Exact round-45 allocation matrix — all four fresh processes exit 0; every
  first-touch order reports 8,005,696 bytes in each of five repetitions.
- Union/UnionAll, Missing, inference, and ambiguity matrix — exit 0; 78/78,
  zero method ambiguities, and no leaked `MethodError`.
- Round-44 runtime matrix — exit 0; 197/197 with the category totals above.
- Arity trim matrix — all nine compile exits 0, all nine binary exits 0, and
  all logs contain zero verifier errors and zero verifier warnings.
- Wrapper-chain probe — ordinary exit 0; trim compiler exit 0; zero verifier
  errors and warnings; binary exit 0.
- Registry audit — exit 0; all 22 `ArrowType` subtypes are accounted for, the
  14-type leaf set equals the inline ladder, and the eight recursive layouts
  equal the fallback set.
- Final HEAD remains `acb5479e73bc157751bda854f8b857512253a4fb`.
  Repository status contains only the six protected untracked files plus this
  review document.

VERDICT: CLEAN
