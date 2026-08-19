# Arrow.jl 3.0 code review — round 56

Date: 2026-08-18

Scope: exact commit `9d89d55b472b5fcb6ce17633a9482a48de902dd1`
(`docs: rewrite the user manual and API reference for 3.0`) on
`core-rewrite`. Round 55 reviewed through
`90f11af6edfa889ac01408fa10b12d4bed96eb33` and recorded one MEDIUM plus
four LOW findings in `docs/dev/REVIEW-codex-r55.md`. I reviewed the complete
four-commit `90f11af..9d89d55` delta:

- `35b41d6` — Tables scan-protocol migration;
- `50e5934` — round-55 review record;
- `fc0de92` — round-55 fixes plus non-nullable plain Bool input;
- `9d89d55` — 3.0 user manual and API reference.

All Tables-dependent checks used the clean `jq/scan` checkout at
`ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`.

## Result

Round 56 is not clean. The round-55 stream-validation, Python-fallback,
metadata-sentinel, and compact-position findings are closed. The specified
documentation cleanup is only partly closed: most named statements were
fixed, but one replacement statement is still false and another historical
claim remains.

I found two MEDIUM and three LOW issues on the required review surfaces:

1. the required Documenter build fails because `Arrow.export_stream!` has
   no attached docstring, and the explicit reference remains incomplete;
2. the new manual falsely promises bounded-memory batch-by-batch writing;
3. both type-mapping tables and the 2.x-differences section overstate actual
   read and write behavior;
4. direct empty `Tables.scan` results from `ArrowFile` and `RangedFile` lose
   their schema element types;
5. current scan, range-fetch, and round-55 cleanup prose still describes
   removed or false behavior.

The package, trim, corpus, IPC-oracle, C-data-oracle, and diff gates all
exit 0. The separate required documentation build exits 1.

## Findings

### 1. MEDIUM — the API-reference build fails on an unattached `export_stream!` docstring

`docs/src/reference.md:48-53` includes `Arrow.export_stream!` in an `@docs`
block. The intended doc block is at `src/cdata.jl:1576-1585`, but commit
`fc0de92` inserted `_validate_stream_schema` and `_validate_stream_field` at
`src/cdata.jl:1590-1607` between that block and `export_stream!` at
`src/cdata.jl:1609-1611`. Julia therefore does not attach the block to the
public function.

The requested exact-HEAD build exits 1:

```text
Error: no docs found for 'Arrow.export_stream!' in `@docs` block in
docs/src/reference.md:48-53
ERROR: `makedocs` encountered an error [:docs_block]
```

A direct documentation lookup also returns `nothing` for
`Arrow.export_stream!`. The adjacent `to_c_data` repair is correct: its doc
block now attaches to `to_c_data`, not `_build_c_data!`.

In a detached scratch worktree, removing only the failing `@docs` entry made
the build exit 0. The only later warning was the accepted local deployment
notice. This isolates the failure to the missing binding documentation; it
does not make removal the right fix. The doc block must sit immediately
before `export_stream!`.

The explicit reference also omits user-facing operations that the manual
asks users or transport implementations to call: `fetchranges`, `reap!`,
`nextbatch!`, and `release!`. `checkdocs=:exports` cannot detect those
omissions because Arrow exports only `close!`; the package intentionally
keeps the other public names namespace-qualified.

Disposition: open. Reattach the docstring, keep the `@docs` entry, and make
the explicit namespace-qualified reference match the intended public
surface.

### 2. MEDIUM — `Arrow.write(..., Arrow.Stream(...))` is not a bounded-memory streaming writer

`docs/src/manual.md:110-112` says an `Arrow.Stream` can be handed to
`Arrow.write` to stream input batch by batch “without ever holding the whole
table.” The `Arrow.Stream` docstring repeats “streams batch-per-batch” at
`src/table.jl:714-720`.

The writer does the opposite. `write(io, tbl)` first calls `_writebytes` at
`src/write.jl:409-412`. `_writebytes` exhausts every partition and stores all
of their column vectors at `src/write.jl:452-479`, builds column storage and
record batches for every partition at `src/write.jl:490-574`, and creates one
complete IPC byte vector at `src/write.jl:576-577`. Only then does
`write(io, bytes)` emit to the sink once.

A three-partition source plus a logging sink produced:

```text
[:partition_1, :partition_2, :partition_3, (:sink_write, 850)]
sink_writes=1
```

The same manual contradicts itself at `docs/src/manual.md:244-248`: it first
says partitions write batch by batch, then accurately says the writer is
whole-buffer and writes the sink once. The false guarantee is operationally
important because the preceding section recommends `Arrow.Stream` for data
larger than memory.

Disposition: open. Either remove the bounded-memory and batch-emission
promise from the manual and `Stream` docstring, or implement an incremental
writer before making that promise.

### 3. LOW — the manual's type-mapping tables are not closed schema rules

The read table is introduced at `docs/src/manual.md:129-132` as a closed
schema rule which never inspects values, with zero-row and all-missing
columns promised the same element type as populated columns. That is true
for closed scalar layouts, but not for dynamic composites and wrappers.

`_facadebasetype` returns `Any` for those layouts at
`src/table.jl:190-209`. `_facadecolumn` then uses `map(identity, converted)`
at `src/table.jl:256-263`, whose output narrows from observed values. Focused
exact-HEAD probes produced:

```text
zero List                  eltype=Any
all-missing nullable List  eltype=Missing
zero Struct                eltype=Any
zero Null                  eltype=Any
zero homogeneous Union     eltype=Any
```

The documented results are respectively `Vector{Any}`,
`Union{Missing,Vector{Any}}`, `Vector{Pair{String,Any}}`, `Missing`, and the
homogeneous child type. A heterogeneous `Union<Int64,String>` schema with
only the integer arm active returned `Vector{Int64}` instead of the
documented pairwise schema join. A run-end-encoded Date32 column returned
raw `Int32`, not `Date`; `src/table.jl:611-618` deliberately leaves temporal
leaves under transparent wrappers in their storage domain.

The write table also overstates recursive composition. Its `Vector{T}` row
at `docs/src/manual.md:283` says List of the mapping of `T`, but List<Date>,
List<Time>, and List<NamedTuple> all refuse. NamedTuple fields containing
Date or `SubString` also refuse. `DictEncode(Date)` refuses even though the
DictEncode row promises a dictionary of the wrapped mapping. The top-level
conversion ladder is at `src/write.jl:41-81`; nested and dictionary builders
route to Core's narrower `fromjulia` domain at
`src/ArrowCore.jl:2628-2748`.

Finally, `docs/src/manual.md:378-380` says an arbitrary Julia struct is
written as an Arrow Struct column. `_writecolumn` handles only NamedTuple at
`src/write.jl:60-66`; a `Vector{AuditStruct}` cleanly raises
`ArgumentError: fromjulia: unsupported element type AuditStruct`.

The specifically requested positive probes do pass:

- a `Vector{UInt8}` row maps to List<UInt8>;
- microsecond timestamps materialize as `Vector{Int64}`;
- a plain `Vector{Bool}` writes a non-nullable Bool field;
- Decimal128 materializes as `Vector{Vector{UInt8}}` with 16-byte values;
- string `DictEncode` survives read/write/read as a Dictionary descriptor and
  preserves values.

Disposition: open. Narrow the tables and Differences section to the actual
top-level and recursive domains, or extend the implementation and add
zero/all-missing/schema-stability tests before promising closed mappings.

### 4. LOW — direct empty file scans lose declared column types

`Tables.scan(af, scan)` and `Tables.scan(rf, scan)` now explicitly compose
Arrow pushdown with the generic residual executor at
`src/scan.jl:1175-1187`. For a scan that produces no rows, both `_applyscan`
implementations construct `Any[]` for an empty decoded part at
`src/scan.jl:614-616` and `src/scan.jl:1160-1162`.

For `Scan(limit=0)` and an offset beyond the input, the direct comparison is:

```text
generic Tables.scan: i::Int64, s::String
ArrowFile scan:      i::Any,   s::Any
RangedFile scan:     i::Any,   s::Any
```

`Tables.schema` exposes the mismatch. A filter whose statistics prune every
batch has the same result. `test/scan_battery.jl:37-45` compares row count,
names, and values, but not `eltype` or `Tables.schema`, so the differential
battery cannot detect it.

`Arrow.Table(...; scan=...)` restores the declared facade element types in
`_wrapscanned`; its focused empty-result schema comparison passes 3/3. No
row value is wrong. The direct mismatch also predates `35b41d6`, but this
round records it because the prompt explicitly requires the direct
`ArrowFile`/`RangedFile` scan surface and the new method documents agreement
with the generic executor.

Disposition: open. Build empty decoded columns from their declared storage
types and extend the direct differential to compare schema and element types.

### 5. LOW — current cleanup and migration prose still states removed or false behavior

The exact runtime grep for `Tables.apply` or `Tables.finish` in Julia source
has no hits. Current developer documentation still has six exact removed-API
references:

- `docs/dev/DESIGN-scan-ranges-trim.md:37,82,88,305`;
- `docs/dev/core-README.md:228,233`.

The design document also retains the old apply/finish protocol in present
tense at lines 17-18, 33-35, 40, 51-53, 78, 85, 90-94, and 326. Current
source comments retain “apply/finish” or “finish” at
`src/scan.jl:555-568`, `src/scan.jl:956`, and `src/table.jl:697`.
`src/scan.jl:555-557` now says the generic `Tables.scan` authority forms
overflowing additions; the exact Tables executor uses saturating `min`
arithmetic instead.

The new manual and the `RangedFile` docstring also say the fetch protocol is
“tail-first” at `docs/src/manual.md:209-212` and `src/scan.jl:818-821`.
`_rangedfooter` fetches the eight-byte head first at `src/scan.jl:850-855`,
then the tail at lines 856-857. A counting probe logged:

```text
[(0, 8), (0, 498)]
```

The specific round-55 head-plus-tail corrections at `src/scan.jl:926` and
`docs/dev/DESIGN-scan-ranges-trim.md:130-156` are accurate. The new manual
reintroduces the wrong ordering elsewhere.

Two round-55 cleanup statements also remain false:

- `test/core_tests.jl:17` now says “ArrowCore in isolation (Base + Mmap
  only),” but the file loads `Arrow` at line 28 and exercises the IPC and C
  data adapters at lines 803-810. This is another false replacement for the
  removed “Stdlib only” claim.
- `src/ArrowCore.jl:1900` says MONTH_DAY_NANO is a unit “today's Arrow.jl
  cannot even parse,” while `src/ipc_read.jl:304-306` parses it and the
  batteries exercise it.

The other named round-55 statements are corrected: explicit `reap!`, typed
materialization, `rangedschema`, the primary DESIGN fetch list, endian and
32-bit behavior, corpus facade prose, the trim-runner dependency wording,
and the `review follow-up R5` text.

Disposition: open. Update current design/source prose to the scan executor
that exists, state head-then-tail, and replace the two remaining false
history/isolation statements.

## Round-55 closure checks

### Zero-batch stream schema validation — closed

`_validate_stream_schema` runs Core schema validation plus a recursive Field
name, metadata, and descriptor walk at `src/cdata.jl:1590-1607`. Export calls
it before allocation or registry publication at `src/cdata.jl:1613-1617`.
Import calls it before returning `ImportedStream` at
`src/cdata.jl:1810-1813`.

Three zero-batch export schemas with an invalid schema key, schema value, or
nested-field value each raised `ValidationError` containing `UTF-8`.
Stream/export registry counts stayed at their baselines.

PyArrow 25.0.1 empty `RecordBatchReader` streams with an invalid key and an
invalid value each raised the corresponding `ValidationError`. A wrapped
producer release callback ran exactly once in each case, the source release
field was NULL after the move, and stream/export registries returned to
0/0. The ours-to-ours post-export corruption probe also refused, nulled the
source release, and returned both registries to zero after `reap!`.

### Python fallback — closed

`_oracle_python` selects `python3`, then `python`, and evaluates the explicit
error only if both are absent at `conformance/cdata_oracle.jl:72-90`. A
no-`uv` temporary-venv probe selected CPython 3.12, created the venv, and
loaded PyArrow 25.0.1; exit 0.

### Ordered metadata sentinel — closed

`_metadata_sentinel!` compares exact ordered pair sequences at schema, leaf,
nested-child, and dictionary-field levels at
`conformance/cdata_oracle.jl:347-401`. The ordinary oracle reports both
sentinel lines PASS and ends with drained registries.

In a detached exact-HEAD worktree, adding `reverse!(out)` before
`_import_cmetadata` returns made the sentinel command exit 1 as required:
both C data and C stream sentinel lines failed, with registries still 0/0.
The scratch worktree was removed afterward. The corpus comparison continues
to sort metadata and remains intentionally blind to pair order; it is not
used as the exact sequence proof. A focused corpus probe reversed the two
schema metadata pairs and all nine `lots_of_meta` pairs in
`cpp-21.0.0/generated_custom_metadata.json.gz`. `docsequal` still returned
zero differences, as designed.

### Compact-view position refusal — closed

`fromcompactviews` rejects zero, extreme endpoints, and every position whose
magnitude cannot fit the Int32 view offset before calling `abs` or doing
extent arithmetic at `src/ArrowCore.jl:2810-2825` in the exact committed
source. After that guard, `pos0 <= typemax(Int32)` and
`len <= typemax(Int32)`, so `pos0 + len <= 4_294_967_294`; no Int64 overflow
is reachable.

A boundary-partition probe covered lengths 13 and `typemax(Int32)`, all
relevant endpoints, the five pinned extremes, and 10,000 random Int64
positions:

```text
positions exercised=10048 ok=7 ArgumentError=10041 OverflowError=0
```

### Documentation sweep — partly closed

The requested corrections are present except for the false replacement in
`test/core_tests.jl:17`; the remaining historical MONTH_DAY_NANO comment is
also current source prose. Finding 5 gives the exact disposition.

### Plain Bool declaration — clean

A plain `Vector{Bool}` now builds and writes a non-nullable Bool Field with
zero nulls. `Union{Missing,Bool}` remains nullable. The facade round-trip
returns `Vector{Bool}`.

## Tables migration review

- No executable or test Julia call to `Tables.apply` or `Tables.finish`
  remains. The stale current-document references are finding 5.
- `Tables.scan(::Union{ArrowFile,RangedFile}, ::Tables.Scan)` dispatches to
  Arrow's method, calls `_applyscan`, and hands the result and residual to
  the generic Tables executor. NamedTuple inputs dispatch to Tables itself.
- The complete scan, ranged, and statistics battery exits 0: 17 direct
  differentials, 9 ranged differentials, column/window/dictionary-body skip
  proofs, corruption and allocation refusals, 8 statistics predicates, and
  7 IEEE/signed-zero/NaN predicates on both whole and ranged paths.
- A custom facade differential compared 10 scan shapes over file, stream,
  and ranged inputs against `Tables.scan(Tables.columns(full), scan)`. It
  passed 30/30 including row counts, names, values, and element types.
- Empty facade results matched generic schemas 3/3. Direct handles have the
  lower-level mismatch in finding 4.

The external Tables `Scan(scan; ...)` copy constructor at Tables
`src/scan.jl:303-306` bypasses the nonnegative checks used by its keyword
constructor. `Tables.Scan(s; limit=-1)` and `offset=-1` therefore construct
live invalid scans. Arrow's own two copy-constructor calls replace only safe
axes (`select=nothing`, or `filter=nothing, limit=nothing, offset=0`), so I
did not rate this as an Arrow finding. It should be corrected upstream in
Tables.

I also did not rate the `typemax(Int)` window body-fetch gap as a new runtime
finding. The guard and residualization predate `35b41d6`; old Tables
`finish` was already saturating. The mechanically renamed false explanation
is included in finding 5.

## Assumptions and decisions

- I treated the exact committed tree, not later uncommitted edits, as the
  review authority. All package/conformance gates and exact-source probes ran
  before a separate concurrent process changed `src/ArrowCore.jl` and
  `test/core_tests.jl` in the shared main checkout. Detached worktrees were
  pinned to the target commit.
- I treated direct `Tables.scan(ArrowFile/RangedFile, scan)` as in scope
  because the prompt names those calls. I recorded its preexisting empty
  schema mismatch while marking its ancestry.
- I treated actual facade `eltype` as the manual's “Julia element type,”
  including its explicit zero-row and all-missing promise. I treated “mapping
  of T” as recursive composition because that is what the writing table says.
- I treated “without ever holding the whole table” as bounded partition
  consumption plus progressive sink emission, not merely one logical record
  batch per input partition.
- Integration-JSON metadata comparison may remain order-insensitive by
  design. The synthetic sentinel is the exact ordered-sequence authority.
- The host was 64-bit arm64 macOS with Julia 1.12.6. The IPC oracle used
  PyArrow 20.0.0 and nanoarrow 0.9.0; the C-data oracle used PyArrow 25.0.1.
- I accepted only the local Documenter deployment-environment notice. The
  actual exact-HEAD build stops earlier on a docs-block error.
- I did not modify product or test code. All deliberate mutations stayed in
  detached scratch worktrees and were removed. I did not revert or alter the
  later concurrent changes in the shared checkout. The protected untracked
  files remained present.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0
  at the exact target: ArrowCore 430/430, threaded caches 4/4, facade
  272/272, and IPC read, IPC write, C Data, and ranged-scan acceptance 1/1
  each.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6,
  compile plus run passed, with zero verifier errors and zero verifier
  warnings.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with PyArrow 20.0.0 and
  nanoarrow 0.9.0.
- `julia --project=conformance --startup-file=no conformance/cdata_oracle.jl`
  — exit 0; PyArrow 25.0.1 over 38 families, 143 pass / 0 fail / 9 skips:
  C data 37/37, native import 37/37, slices 29/29, C stream 37/37, exact
  metadata sentinels 2/2, and registry drain 1/1.
- Requested docs environment setup with `Pkg.develop` for exact Arrow and
  Tables, then `Pkg.instantiate()` — exit 0.
- `julia --project=docs --startup-file=no docs/make.jl` — exit 1 at exact
  HEAD on the missing `Arrow.export_stream!` docs block. A scratch diagnostic
  without that one reference exited 0 with only the local deploy notice.
- Complete manual scan/ranged/statistics battery — exit 0 with the counts and
  skip proofs listed above. Custom facade differential passed 30/30; empty
  facade schema comparison passed 3/3.
- Zero-batch invalid-metadata export and PyArrow import probes — exit 0 with
  the required refusals, exactly-once producer releases, and drained
  registries.
- Ordered-metadata scratch mutation — sentinel command exit 1 as required;
  both exact-sequence verdicts failed and registries drained.
- Corpus metadata-order probe — exit 0; reversing the two schema pairs and
  nine `lots_of_meta` pairs produced zero `docsequal` differences, confirming
  the intentional order-insensitive corpus verdict.
- Compact-position proof/probe — exit 0; no `OverflowError` among 10,048
  exercised positions and no overflow is reachable after the source guard.
- Required positive manual mapping probe — exit 0 for List<UInt8>,
  microsecond Timestamp, non-nullable Bool, Decimal128, and retained string
  dictionary encoding. Negative documentation probes produced the managed
  results in findings 2 and 3.
- `git diff --check` and
  `git diff --check 90f11af6edfa889ac01408fa10b12d4bed96eb33..9d89d55b472b5fcb6ce17633a9482a48de902dd1`
  — exit 0 for the exact review target and four-commit delta before this
  report.
- Final branch HEAD remained
  `9d89d55b472b5fcb6ce17633a9482a48de902dd1`. The shared checkout acquired
  concurrent uncommitted changes to `src/ArrowCore.jl` and
  `test/core_tests.jl` after exact-HEAD validation; I preserved them. This
  review document is the only repository change made by this review.

VERDICT: FINDINGS
