# Arrow.jl 3.0 code review — round 46

Date: 2026-08-17

Scope: exact fix commit `8f0d9a1f919227bf42e275de79a13ab1dca9fac4`
(`fix: resolve round 45 findings — split recursion edge, exact NT claims`)
on `core-rewrite`. Its parent,
`a12c6df8963d4066355a8ad381ec0d7e69816c82`, records the round-45 review
of fix commit `66588198fdd78091ac7cfd56d0cf5d0d13b7971e`. I reviewed the
round-45 fix diff and reran the full round-44/45 regression surface at the
exact fix commit.

## Result

Round 46 is not clean. I found one MEDIUM issue.

The exact round-45 two-`Int64` Struct workload is fixed. It allocates
8,005,696 bytes in fresh processes under every requested first-touch order.
The NamedTuple Union and UnionAll matrix also refuses through both public
entry points with `ArgumentError`. No case leaks `MethodError`.

The allocation fix is not complete at the root. The new inline child ladder
omits three no-child leaf layouts: Interval, View, and Null. Each falls through
the compiled composite shell. Nested typed reads therefore retain about 64
bytes of avoidable allocation per row. The new allocation pin uses only
`Int64` children, so it does not cover this remainder.

All five repository gates pass. The full correctness, inference, ambiguity,
and trim matrices also pass. They do not detect this per-row cost.

## Findings

1. **MEDIUM — the scalar fast ladder omits three leaf layouts and leaves the
   recursion-boundary allocation in place for them.**

   The registry declares `NullType`, `IntervalType`, and `ViewType` with zero
   children at `src/ArrowCore.jl:605`, `:614`, and `:630`. Interval extraction
   is fixed-width at `src/ArrowCore.jl:1895-1909`. View extraction reads one
   scalar String or byte vector at `src/ArrowCore.jl:2040-2057`. Null returns
   one `missing` value at `src/ArrowCore.jl:1917-1919`.

   `_typedchild` handles selected leaves in its inline ladder at
   `src/ArrowCore.jl:2341-2355`. It omits all three types. Its default at
   `src/ArrowCore.jl:2356` sends them to `_typedchildbox`, whose compiled
   boundary is at `src/ArrowCore.jl:2359-2361`. This is not required for these
   layouts because none recurses into a child. The shared scalar `_typedvalue`
   method itself includes Interval and View at `src/ArrowCore.jl:2399-2407`.
   Null has its typed leaf at `src/ArrowCore.jl:2484-2485`. Generated Struct
   rows call `_typedchild` for every field at `src/ArrowCore.jl:2461-2465`.

   I measured each workload in fresh Julia processes. I warmed the exact
   measured function three times, ran five repetitions, and did no compiler
   introspection. Every repetition for each case was identical.

   - A 100,000-row one-field Struct with `IntervalType(YEAR_MONTH)` allocates
     13,217,872 bytes. An equal-output, bounds-safe direct-leaf control
     allocates 6,826,048 bytes. The shell adds 6,391,824 bytes, or 63.92 bytes
     per row and 93.64% over the control.
   - A 100,000-row one-field Struct with an inline UTF-8 View allocates
     42,411,088 bytes. A production-equivalent ladder with one added
     `ViewType` leaf branch allocates 36,019,264 bytes with equal output. The
     same shell adds 6,391,824 bytes, or 17.75% over the control.
   - A 100,000-row one-field Struct with a Null child allocates 12,791,888
     bytes. The equal-output direct control allocates 6,400,064 bytes. The
     delta is again 6,391,824 bytes.

   The View result is also worse than the erased dynamic leaf path. A
   100,000-read production `_typedchild` loop allocates 35,191,824 bytes. The
   direct typed leaf allocates 28,800,000 bytes, and dynamic `getvalue`
   allocates 28,791,824 bytes. A scratch `ViewType` fast branch removes the
   full residual cost. Direct-first and production-first compiled functions
   produce the same five measurements.

   The repository pin at `test/typed_alloc_child.jl:26-35` builds only two
   `Int64` children. It proves the reported workload, but not the stated
   closed scalar set. Extend the inline ladder to every profitable no-child
   leaf and pin Interval, View, and Null in fresh processes. Keep the child
   bounds check and rerun the trim matrix.

   I rank this MEDIUM because values, errors, and inferred result types remain
   correct. The cost is still deterministic per-row allocation in the typed
   materialization hot loop, and it nearly doubles the small Interval and
   Null controls.

## Round-45 status and regression surface

- Finding 1 closes for the exact reported workload, but not at the root due
  to the finding above. The 100,000-row
  `NamedTuple{(:a,:b),Tuple{Int64,Int64}}` workload allocates exactly
  8,005,696 bytes in all five repetitions for the baseline, `getvalue`-first,
  Dictionary/REE-wrapper-first, and interleaved-claim orders. The interleaved
  process touches Int64, String, Bool, Float64, and `Vector{Int64}` claims.
  The repository child pin also prints `typed alloc ok: 8005696`.
- Finding 2 is closed. The Struct preflight requires
  `E isa DataType && E <: NamedTuple` before field count or reflection at
  `src/ArrowCore.jl:2256-2266`. The generated reflection remains behind that
  gate at `src/ArrowCore.jl:2292-2300`.
- The hostile claim matrix passes 78/78 assertions. It covers same-shape and
  different-shape row unions, unions with Missing, `NamedTuple{(:a,)}`, bare
  `NamedTuple`, UnionAlls with unknown names or field types, abstract and
  `Any` fields, wrong arity, `Union{}`, and `Missing`. Both entry points throw
  `ArgumentError` for every incompatible shape. None throws `MethodError`.
- The `::T` assertions preserve correct values and exact inference when the
  admitted domain contains Missing. This holds for scalar fields, nullable
  Structs, List of nullable Struct, and a Struct containing nullable
  Dictionary and REE fields.
- `_typedvalue_of` has only two callers: public `getvalue` at
  `src/ArrowCore.jl:2190` and `_typedchildbox` at
  `src/ArrowCore.jl:2361`. All internal recursive reads route through
  `_typedchild` at `src/ArrowCore.jl:2428`, `:2443`, `:2463`, `:2476`, and
  `:2482`. I found no other hoisted recursive ladder caller.
- The Dictionary and REE shell cost is acceptable. Their top-level typed
  100,000-read loops allocate zero. Direct `_typedchild` loops allocate
  6,391,824 bytes. Dynamic loops allocate 12,791,280 bytes for Dictionary and
  12,790,656 bytes for REE. Typed-first and dynamic-first processes agree.
  These wrappers need the concrete recursive boundary, and the typed path
  remains cheaper than the dynamic path.
- The round-44 runtime matrix passes 197/197: descriptor false-accept checks
  20/20, hidden child bounds parity 32/32, valid layout/value/missing parity
  109/109, refusal/ambiguity/inference 33/33, and allocation assertions 3/3.
- Heterogeneous NamedTuple arities 0 through 6 and homogeneous arities 4 and
  6 compile separately under `--trim=safe`. All nine compilers and binaries
  exit 0. All nine logs contain zero verifier errors and zero warnings.
- `Dictionary<REE<Dictionary<Int64>>>` exits 0 under ordinary Julia and trim.
  Its trim compiler and binary exit 0 with zero verifier errors and warnings.
  Both public typed entry points return the expected values.

## Assumptions and decisions

- I kept the round-44/45 rule that `T` is a descriptor-level element-domain
  assertion. Empty and null data do not weaken the required base claim.
- I classified a registered layout with `childcount == 0` and direct value
  extraction as a leaf. This follows the fix's stated scalar/composite split.
- I used equal-output controls that keep the child logical-bounds check and
  the existing Struct validity lookup. I attributed only the remaining delta
  to the compiled shell.
- I treated the wrapper shell as intentional and acceptable. Dictionary and
  REE preserve the same claim across real recursive edges, compile under
  trim, and cost less than the dynamic path.
- I ranked the incomplete leaf split MEDIUM. It affects performance, not
  correctness, but it has the same per-row root cause and scale as the
  round-45 MEDIUM finding.
- The host is 64-bit arm64 macOS and used Julia 1.12.6. All probes, logs, and
  compiled binaries live in scratch directories under `/tmp`.
- I made no product or test change. The six protected untracked files remain
  present and unmodified. This review document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  660/660 reported assertions: threaded caches 4/4, ArrowCore 384/384, facade
  268/268, and each IPC read, IPC write, C Data, and ranged-scan acceptance
  battery 1/1. The fresh allocation child reports 8,005,696 bytes.
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
- Round-44 runtime matrix — exit 0; 197/197 with the category totals above.
- Arity trim matrix — all nine compile exits 0, all nine binary exits 0, and
  all logs contain zero verifier errors and zero verifier warnings.
- Wrapper-chain probe — ordinary exit 0; trim compiler exit 0; zero verifier
  errors and warnings; binary exit 0.
- Exact round-45 allocation matrix — every fresh process exits 0. All four
  first-touch orders produce 8,005,696 bytes in each of five repetitions.
  No allocation process runs compiler introspection.
- Wrapper allocation matrix — typed-first and dynamic-first processes exit 0
  with the stable costs above.
- Union/UnionAll, Missing, inference, and ambiguity matrix — exit 0; 78/78;
  zero method ambiguities and no leaked `MethodError`.
- Omitted-leaf allocation probes — every fresh process exits 0. Interval,
  inline and spilled UTF-8 View, inline and spilled Binary View, and Null each
  repeat their reported allocation five times without variation.
- Final repository status retains the six protected untracked files plus this
  review file. No protected file was inspected or modified.

VERDICT: FINDINGS
