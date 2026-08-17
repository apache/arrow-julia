# Arrow.jl 3.0 code review — round 51

Date: 2026-08-17

Scope: exact commits `abc8831634e248edd205f342a4aa746503437365`
(`feat: serialize/deserialize benchmark harness (review R12)`),
`a45ac45ab7c4d240e872bc904eb6f7e0b323a960`
(`perf: facade reads ride the typed element path where the claim closes`),
and `05b8f75938ac17bd514c5bac1533ba4dc4606547`
(`perf: bulk fixed-width extraction for closed typed claims`) on
`core-rewrite`. Their parent is
`5ecc7954b851bf202f7f5732b7d1adf367536d59`, which records the clean
round-50 close of the C-data arc. I reviewed only this three-commit batch and
reran the full package, trim, corpus, and oracle regression surface at the
exact final commit.

## Result

Round 51 is not clean. I found two HIGH, five MEDIUM, and two LOW issues.

The valid fixed-width bulk-copy surface is sound. A 27-descriptor matrix
matched typed and dynamic values, including exact missing placement, sliced
offsets, all valid integer, float, temporal, and Decimal32/64 layouts, empty
windows, oversized backing buffers, bitmap boundaries, hostile short data
buffers, integer overflow, mmap storage, and imported C-data storage.

Two unvalidated Core states still break the typed contract. A caller-supplied
cached zero null count suppresses real bitmap nulls. Invalid 24-bit integer
and float descriptors also pass the new exact-type gate even though the
claimed Julia size disagrees with the descriptor width.

The facade routing change breaks two valid public inputs. Every `NullType`
column now throws while classifying `Missing`. A homogeneous union derives a
concrete Julia claim and is routed to the typed path, although Core explicitly
rejects all typed union claims. Direct unions and unions under Dictionary or
REE wrappers fail.

The benchmark's ordinary primitive, nullable, string, and list workload
formulas match. Both timing helpers use one warmup and the median of three
measured runs. The dictionary write comparison is not like-for-like, the 2.x
environment does not run from a clean checkout, and the printed report omits
the required read-semantics warning. The result collector also accepts
incomplete output, and a missing Docker executable does not skip cleanly.

All five required gates pass. They do not exercise these cases.

## Findings

1. **HIGH — valid `NullType` columns fail on public facade reads.**

   `_declaredbasetype` maps `NullType` to `Missing` at
   `src/table.jl:608-617`, and `_declaredeltype` returns that claim at
   `src/table.jl:592-593`. `_closedclaim` then computes
   `Base.nonmissingtype(Missing) == Union{}` at `src/table.jl:218-220`.
   Bottom is a subtype of `Vector`, so the branch at `:221` calls
   `eltype(Union{})` and throws `ArgumentError: Union{} does not have
   elements`.

   This happens before `_batchcolumn` can select either materializer at
   `src/table.jl:233-236`. Both Core routes can represent the data: dynamic
   `NullType` extraction returns `missing` at `src/ArrowCore.jl:1917-1919`,
   and the typed preflight and element method accept a `Missing` claim at
   `src/ArrowCore.jl:2280-2282` and `:2487-2488`.

   A valid two-row Null array passed full validation and IPC serialization.
   Full `Table` reads from stream and file, both `Stream` iteration paths,
   and a stream scan all failed with the same exception. A file no-op scan
   used the dynamic route and returned `[missing, missing]`. The route needs
   an explicit bottom/`Missing` decision before recursive vector inspection.

2. **HIGH — homogeneous unions are routed to a typed path that rejects every
   union.**

   `_declaredeltype` joins union child domains at `src/table.jl:585-590`.
   A union whose children both hold `Int64` therefore declares `Int64`.
   `_closedclaim(Int64)` returns true at `src/table.jl:218-223`, and
   `_batchcolumn` calls typed materialization at `:233-236`.

   Core deliberately has no typed union domain. `_checkclaim` refuses every
   `UnionType` at `src/ArrowCore.jl:2279`, and the typed value method states
   the same rule at `src/ArrowCore.jl:2490-2494`. A valid two-child sparse
   union passed structural and semantic validation and IPC serialization.
   Dynamic materialization returned `[10, 40]`; public facade reads threw
   `ArgumentError: field union materializes UnionType-layout values; the
   claimed static element type does not match`.

   Dictionary-wrapped and REE-wrapped homogeneous unions fail the same way
   because `_declaredeltype` is transparent through those wrappers at
   `src/table.jl:578-583`. A type-only predicate cannot distinguish a direct
   `Int64` leaf from a union whose winning values happen to share that Julia
   type. The route must inspect the Arrow descriptor through transparent
   wrappers and keep every union dynamic.

3. **MEDIUM — cached `nullcount=0` lets bulk extraction erase bitmap nulls.**

   `ArrayData` accepts a caller-supplied null-count cache at
   `src/ArrowCore.jl:708-729`. `nullcount` trusts every nonnegative cached
   value without reading the bitmap at `src/ArrowCore.jl:779-784`. The new
   bulk path uses that cache as its bitmap authority at
   `src/ArrowCore.jl:2565-2575`. It returns a Missing-free vector when the
   cache says zero at `:2566-2568`, and it returns a widened Missing-admitting
   vector without a bitmap pass at `:2570-2572`.

   The dynamic primitive path checks the bitmap for each element at
   `src/ArrowCore.jl:1835-1837`. The focused unvalidated probe produced:

   ```text
   dynamic=Any[10, missing, 30]
   typed nullable=Union{Missing, Int64}[10, 20, 30]
   typed Missing-free=[10, 20, 30]
   ```

   A nine-row data buffer with a one-byte validity buffer and cached zero also
   returned all nine typed values. Dynamic access refused the ninth validity
   read with `BoundsError`. Thus, the optimization can both lose missing
   placement and skip hostile validity geometry.

   `validate_semantic` detects a declared-versus-actual null-count mismatch at
   `src/ArrowCore.jl:1383-1389`. Normal validated IPC, C-data, and facade
   inputs cannot reach this state. Direct unvalidated `ArrayData` can, and
   `src/ArrowCore.jl:2554-2555` explicitly says this typed path serves
   unvalidated data. A cached count can be a fast-path authority only after a
   validation certificate; otherwise the bitmap must decide validity.

4. **MEDIUM — the exact-type gate does not prove that the claim size matches
   the layout width.**

   `primwidth` derives an integer or float layout width from the descriptor at
   `src/ArrowCore.jl:592-610`. The fallback branches in `juliatype` map an
   invalid integer width to `Int64` or `UInt64` and an invalid float width to
   `Float64` at `src/ArrowCore.jl:1779-1783`. `_checkclaim` accepts that result
   at `src/ArrowCore.jl:2284-2286`.

   `_bulkmaterialize` checks only `E === juliatype(t)` at
   `src/ArrowCore.jl:2549-2551`. It then chooses its copy width from
   `sizeof(E)` at `:2552-2557`, not from the descriptor layout. Invalid
   24-bit descriptors therefore reach an eight-byte bulk copy although their
   layout width is three bytes. The probe produced:

   ```text
   Int24: juliatype=Int64 sizeof=8 layout_width=3
   typed=[72623859790382856, 17]
   dynamic=Any[8, 7]

   Float24: juliatype=Float64 sizeof=8 layout_width=3
   typed=[1.25, -3.5]
   dynamic=Any[Float16(0.0), Float16(0.0)]
   ```

   Descriptor validation rejects both states at `src/ArrowCore.jl:933-941`,
   so validated adapters are safe. The unvalidated typed API is not. The bulk
   gate must require a recognized descriptor and exact agreement between
   `sizeof(E)` and the fixed layout width before it copies.

5. **MEDIUM — the `dictpool` write comparison uses different physical inputs
   and different timed work.**

   The shared Julia workload returns a plain `Vector{String}` at
   `bench/workloads.jl:53-55`. Both Julia legs pass that table directly to
   timed `Arrow.write` calls at `bench/bench_rewrite.jl:25-30` and
   `bench/bench_2x.jl:26-30`. The rewrite only builds dictionary storage for
   an explicit `DictEncode` input or retained dictionary field at
   `src/write.jl:106-112` and `:490-500`, so this workload writes ordinary
   Utf8 storage.

   PyArrow calls `dictionary_encode()` while building its table at
   `bench/bench_pyarrow.py:63-66`. That happens before the write timer at
   `:89-98`. PyArrow therefore writes a pre-encoded dictionary and excludes
   pool construction from its timed work.

   A reduced three-leg probe confirmed the mismatch. Rewrite wrote a
   `string` field in 31,466 bytes. Arrow 2.8.1 wrote a `string` field in
   31,450 bytes. PyArrow wrote
   `dictionary<values=string, indices=int32>` in 13,826 bytes. All logical
   value sequences matched, but the physical write work did not.

   The reported dictionary-pool ratio cannot support a dictionary-writer
   performance conclusion. All legs must receive the same physical encoding,
   and pool construction must be either inside or outside every write timer.

6. **MEDIUM — the 2.x leg does not run from a clean checkout and does not pin
   the stated 2.8.1 baseline.**

   The driver starts `bench/bench_2x.jl` directly at `bench/run.jl:52-56`.
   That script immediately loads `Arrow` at `bench/bench_2x.jl:22`. The commit
   has no `bench/env2x/Manifest.toml`; `.gitignore:18-19` ignores all manifest
   files. The only version constraint is `Arrow = "2"` at
   `bench/env2x/Project.toml:5-6`.

   A copy of that project in an empty depot failed at `using Arrow` with
   `Run Pkg.instantiate()` and exit 1. The documented one-command invocation
   at `bench/run.jl:20` performs no setup step. After a manual scratch
   instantiate, the environment resolved registered Arrow 2.8.1. The live
   package had `is_tracking_registry=true`, `is_tracking_path=false`, and
   `is_tracking_repo=false`; it did not load this repository.

   The provenance is correct only after the missing setup step. The broad
   `"2"` compatibility range also permits a later 2.x release, so it does not
   reproduce the stated 2.8.1 comparison. The driver must create or
   instantiate a pinned registered environment before it launches the leg.

7. **MEDIUM — the printed benchmark report omits the read warning, and the
   2.x list leg is not fully materialized.**

   The source comment accurately says that read rows are not like-for-like at
   `bench/run.jl:27-32`. Runtime output at `bench/run.jl:81-95` prints only
   the table. It emits no warning before the result that a user may copy or
   publish. The Python file also says that the report states the asymmetry at
   `bench/bench_pyarrow.py:18-20`, but the report does not.

   The 2.x leg further claims that `copy()` makes both implementations pay
   full materialization at `bench/bench_2x.jl:17-19`. The timed code makes
   one top-level copy per column at `:31-36`. A list probe showed that the
   rewrite returned `Vector{Vector{Any}}`, while Arrow 2.8.1 returned
   `Vector{SubArray{Int64,...}}` after `copy`. The outer vector was copied,
   but each inner list still referenced Arrow storage.

   The differing read contracts are acceptable by design. The report must
   print that warning next to every result, and the 2.x description must not
   call a shallow nested copy full materialization.

8. **LOW — the result collector silently accepts missing and duplicate JSONL
   records.**

   The parser stores results in one dictionary at `bench/run.jl:73-79`.
   A duplicate `(impl, workload, op)` key overwrites the earlier record. The
   table then uses default `(NaN, 0)` values for absent keys at `:86-95` and
   prints `-`. A leg that exits 0 with partial or duplicate output therefore
   produces a plausible table instead of refusing the result.

   The flat regular expressions parse every current emitter successfully,
   and malformed required fields fail rather than fabricate numbers. The
   driver still needs an exact expected-key set and duplicate rejection
   before it prints a benchmark table.

9. **LOW — a missing Docker executable does not take the documented clean
   skip.**

   `bench/run.jl:22-24` says PyArrow skips cleanly when Docker is unavailable.
   The availability check at `bench/run.jl:59-61` wraps the image inspection
   in `success`, which handles a nonzero process exit but not process-spawn
   failure. With Docker removed from `PATH`, that expression raised
   `IOError: could not spawn docker ... ENOENT` and exited 1. A missing image
   and a stopped daemon return false as intended. The driver must also handle
   a missing executable as an unavailable Docker case.

## Sound portions of the batch

- The bulk positive probe passed 324/324 assertions over 27 valid
  descriptors: signed and unsigned integers at 8/16/32/64 bits,
  Float16/32/64, both date units, every timestamp, time, and duration unit,
  and Decimal32/64. Decimal values matched the dynamic raw `Int32`/`Int64`
  reinterpretation.
- The same probe passed 6/6 bitmap cases. It covered nulls at 64-bit word
  boundaries, leading and trailing nulls, nonzero physical bit offsets, and
  all-null columns. `copyto!` widened values correctly, and the punch loop
  reproduced exact dynamic missing placement for valid data.
- Nonzero logical offsets, offset-plus-null combinations, misaligned slice
  starts, zero-length columns, oversized buffers, and data outside the
  logical window all passed. Three hostile short-data cases refused through
  `subslice`. Offset multiplication, length multiplication, and subslice-end
  addition overflow all refused.
- Exact valid claim mismatches, including Float16 against Float32 storage and
  Bool exclusion, stayed on the element/refusal path. Decimal128/256,
  strings, composites, and bitmaps did not enter the bulk copy.
- The raw copy's ownership is sound. `GC.@preserve vals d` covers the copy at
  `src/ArrowCore.jl:2560-2563`, and `d` retains its buffer regions and owner at
  `src/ArrowCore.jl:696-703`. Real mmap and imported C-data buffers survived
  forced collection during focused reads.
- The facade routing matrix passed closed nullable primitives,
  Dictionary<String>, and REE<nullable Int64>. It kept Dictionary<List>,
  nullable lists, structs, REE<Struct>, and heterogeneous unions dynamic.
  Typed and dynamic values, public values, and public element types matched.
  File and stream Tables, both Stream partition paths, stream scans, mmap
  close, file removal, and later GC all passed for these cases.
- The non-dictionary benchmark workloads use the same row counts, values,
  and storage types across Julia and Python. `bench_time` at
  `bench/workloads.jl:66-75` and `bench` at
  `bench/bench_pyarrow.py:78-86` each perform one warmup and choose the median
  of three measured runs. All current emitters produce parseable flat JSONL.

## Assumptions and decisions

- I treated valid public `Table` and `Stream` failures as HIGH. They reject
  supported Arrow layouts after full validation and affect ordinary facade
  entry points.
- I treated direct unvalidated Core access as in scope. The prompt requires
  hostile geometry probes, and the bulk-path comment explicitly promises a
  bounds-safe unvalidated route. I rated those two issues MEDIUM because
  staged validation blocks them from normal adapters and the facade.
- I treated a dictionary serialization comparison as like-for-like only when
  every leg starts with the same physical encoding and includes the same pool
  construction work. Equal logical strings are not enough for a writer
  benchmark that claims dictionary-pool performance.
- I required the read-semantics warning in runtime output. A source comment
  does not travel with the printed Markdown table.
- I treated the usage line as a clean-checkout command because the harness
  documents no separate environment setup. I required a registered 2.8.1
  source and a reproducible pin. The current manually instantiated live
  environment did resolve the registered package, never this repository.
- I did not run the full machine-relative benchmark. I used reduced protocol,
  schema, materialization, parsing, and environment probes. I did not judge
  absolute timings.
- I accepted the corpus and oracle's declared skips as the existing baseline.
  The host is 64-bit arm64 macOS and used Julia 1.12.6. The oracle used
  PyArrow 20.0.0 and nanoarrow 0.9.0. All focused probe sources and outputs
  live outside the repository.
- One initial scratch assertion classified the `NullType` exception as a
  `MethodError`. Julia raises `ArgumentError` for `eltype(Union{})`. The
  corrected assertion exits 0 and confirms the same product failure.
- I made no product or test change. The six protected untracked files remain
  present and unmodified. This review document is the only repository
  change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  660/660 reported assertions: ArrowCore 384/384, threaded caches 4/4,
  facade 268/268, and each IPC read, IPC write, C Data, and ranged-scan
  acceptance battery 1/1.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, zero verifier warnings, and compile plus run passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with PyArrow 20.0.0 and
  nanoarrow 0.9.0.
- `git diff --check` — exit 0. `git diff --check 5ecc795..HEAD` also exits 0
  for the exact three-commit batch.
- Valid bulk matrix — exit 0; 324/324 descriptor assertions, 6/6 bitmap
  cases, 3/3 hostile geometry cases, 6/6 valid mismatch gates, 3/3 checked
  overflow cases, and 2/2 mmap/foreign-lifetime cases.
- Bulk adversarial diagnostic — exit 0; 18 assertions reproduce the cached
  null-count and invalid-width findings. Both malformed states are rejected
  by validation.
- Facade routing matrix — exit 0; all listed closed and open routes, values,
  element types, facade entry points, mmap closure, and ownership checks
  pass.
- Null and homogeneous-union direct public reproducers — each exits 1 with
  the stated product exception. A consolidated entry-point diagnostic exits
  0 and confirms both failures across stream/file Table, both Stream paths,
  and stream scan. Its dynamic controls return the expected values.
- Wrapped-union diagnostic — exit 0; direct Union, Dictionary<Union>, and
  REE<Union> all derive `Int64`, report closed, and refuse the typed route.
- Reduced dictionary protocol probe — all three legs exit 0; logical values
  match, while the two Julia schemas are Utf8 and PyArrow's schema is
  dictionary encoded with the reported file sizes.
- Current 2.x provenance probe — exit 0; Arrow 2.8.1 loads from
  `~/.julia/packages`, reports registry tracking, and reports no path or
  repository tracking. The clean copied environment probe exits 1 before a
  manual instantiate, as documented.
- Missing-Docker probe — exit 1 with spawn `ENOENT`, confirming that this
  unavailable case does not reach the skip branch.
- Final HEAD remains `05b8f75938ac17bd514c5bac1533ba4dc4606547`.
  Repository status contains only the six protected untracked files plus this
  review document.

VERDICT: FINDINGS
