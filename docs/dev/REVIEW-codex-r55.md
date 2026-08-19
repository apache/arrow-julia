# Arrow.jl 3.0 code review — round 55

Date: 2026-08-18

Scope: exact commit `90f11af6edfa889ac01408fa10b12d4bed96eb33`
(`docs: comment and documentation history sweep — present tense only (review
R4)`) on `core-rewrite`. Commit
`7cb967594ea7561a23b8caf138523c169663de85` records the clean round-54
review. I reviewed the five-commit `7cb9675..90f11af` delta: the seven
short-form definition rewrites, the C-data validation/metadata/REE fixes,
the C Data and C Stream oracle, `fromcompactviews`, and the documentation
sweep. I also ran every required gate and the requested adversarial probes at
the exact target source.

## Result

Round 55 is not clean. I found one MEDIUM issue and four LOW issues.

The semantic-tier C boundary is memory-safe and now agrees with the IPC
reader and writer. The checks removed from the default boundary are content
or advisory checks, not buffer geometry checks. Invalid UTF-8 imports as a
bounded Julia `String`, remains safe to inspect, and is refused by the
opt-in `validate_full` tier. REE parent `null_count == -1` also resolves to
zero through access, validation, JSON, C export, and IPC write.

Schema metadata is placed on the correct struct schema node in both stream
directions. Ordered duplicate and nested metadata survive the current
product path. However, a zero-batch stream never reaches batch semantic
validation. It can therefore export or import invalid UTF-8 schema metadata
even though Core classifies that check as structural.

The new oracle passes its full 141/0/9 matrix, and its slice, ownership,
dictionary, and registry logic is sound. Two harness defects remain. Its
document comparison cannot detect metadata-pair order corruption, and its
documented no-`uv` Python fallback always throws before it can create the
venv. `fromcompactviews` is safe and correct over the requested layout and
bounds cases, except that extreme signed positions violate its documented
`ArgumentError` contract. The documentation sweep also leaves several false
or historical statements.

## Findings

1. **MEDIUM — zero-batch C streams bypass structural validation of schema
   metadata.**

   Core requires schema metadata keys and values to be valid UTF-8 at
   `src/ArrowCore.jl:981-997`. The recursive Field check enforces the same
   rule at `src/ArrowCore.jl:1018-1023`. The IPC writer applies the schema
   walk eagerly at `src/ipc_write.jl:595-606`.

   The C Stream export path does not. `export_stream!` copies
   `sch.metadata` to the struct-typed `batchfield` and publishes the stream
   at `src/cdata.jl:1590-1627`. `get_schema` exports that field directly at
   `src/cdata.jl:1475-1488`. The first validation call is in `get_next`,
   after it has selected an actual batch, at `src/cdata.jl:1500-1520`.

   Import has the same gap. `from_c_stream` preflights and imports the C
   schema, then constructs and returns the Core `Schema` at
   `src/cdata.jl:1756-1788`. It does not run `_validate_schema` or the
   recursive Field metadata walk. `nextbatch!` first calls
   `validate_semantic` at `src/cdata.jl:1826-1835`, but end-of-stream returns
   before that work at `src/cdata.jl:1811-1813`.

   A focused ours-to-ours probe built an empty stream whose schema metadata
   key contained byte `0xff`. Export succeeded, import succeeded, the
   returned key had `isvalid == false`, and `nextbatch!` returned `nothing`.
   A separate PyArrow 25.0.1 empty `RecordBatchReader` probe repeated the
   import result for an invalid key and for an invalid value:

   ```text
   bad key:   keyvalid=false valuevalid=true  next=nothing
   bad value: keyvalid=true  valuevalid=false next=nothing
   export/stream registries after release: 0/0
   ```

   A nonempty stream eventually refuses the same schema when it validates
   its first batch. An empty stream never does. This is not a memory-safety
   issue: metadata bytes are copied into bounded Julia strings. It is a
   public boundary-contract failure that returns a Core schema which violates
   Core's structural invariant. I rank the silent schema acceptance MEDIUM.

   Disposition: open. Validate the full schema tree before publishing an
   exported stream and immediately after importing a stream schema. Do not
   make schema validity depend on the presence of a data batch.

2. **LOW — the oracle's automatic no-`uv` Python setup always throws.**

   The fallback at `conformance/cdata_oracle.jl:82-84` calls:

   ```julia
   something(Sys.which("python3"), Sys.which("python"), error(...))
   ```

   Julia evaluates all call arguments before entering `something`, so the
   `error` executes even when `python3` or `python` exists. The documented
   `python3 -m venv` plus pip path at
   `conformance/cdata_oracle.jl:57-62` cannot run on a host without `uv`.

   A direct probe on a host with both Python names present exited 1 with
   `ERROR: sentinel fallback evaluated`. The `uv` path and
   `ARROW_CDATA_ORACLE_PYTHON` path work; the required gate passed through
   the latter.

   Disposition: open. Select the first non-`nothing` executable before the
   explicit error branch, so the error remains lazy.

3. **LOW — the C-data oracle can report PASS after metadata-pair order is
   corrupted.**

   `_compare` delegates all equality to `docsequal` at
   `conformance/cdata_oracle.jl:225-227`. Corpus normalization sorts every
   metadata list by key and value at `conformance/corpus.jl:227-237`.
   Reversing both schema metadata and the nine metadata pairs on the
   `lots_of_meta` field in `generated_custom_metadata` therefore returned:

   ```text
   reordered_metadata_diffs=0
   ```

   The corpus-derived input also cannot cover duplicate metadata keys.
   ArrowJSON converts the sequence to a `Dict` at
   `conformance/arrowjson.jl:134-147` and
   `conformance/arrowjson.jl:494-498`, which collapses duplicates before any
   C pointer crosses the boundary. Core deliberately treats sequential
   metadata as lossless and preserves order and duplicate keys at
   `test/core_tests.jl:201-210`. The product C Stream battery also pins an
   ordered duplicate sequence at `test/cdata_battery.jl:1360-1376`.

   The value-level corpus comparison remains correct for values and for IPC
   dictionary-id canonicalization. A dictionary-pool content mutation
   produced a precise diff. This finding is narrower: the oracle header says
   metadata is covered at `conformance/cdata_oracle.jl:31-38`, but exact
   metadata-sequence fidelity is not covered. The current product path
   preserved a three-level nested duplicate sequence exactly through
   ours-to-PyArrow-to-ours.

   Disposition: open. Keep the integration-JSON normalization for corpus
   equality, but add a strict synthetic metadata sentinel that compares the
   ordered pair sequence without a `Dict` or sorting step.

4. **LOW — extreme compact-view positions violate the documented exception
   contract.**

   `fromcompactviews` promises that escaping positions/extents and offsets
   outside `Int32` are refused with `ArgumentError` at
   `src/ArrowCore.jl:2770-2772`. The implementation calculates the signed
   zero-based position and runs `checked_add(pos0, len)` before the `Int32`
   check at `src/ArrowCore.jl:2813-2818`.

   With a 13-byte long entry, `pos == typemax(Int64)` raised
   `OverflowError: 9223372036854775806 + 13`. With nonempty `extra`,
   `pos == typemin(Int64)` raised
   `OverflowError: 9223372036854775807 + 13`. Both inputs fail before any
   buffer access, so there is no unsafe read or write. The error type alone
   violates the public docstring.

   Disposition: open. Convert checked-arithmetic overflow on this refusal
   path to the documented `ArgumentError`, or narrow the documented contract
   if `OverflowError` is intentional.

5. **LOW — the present-tense sweep leaves false and historical statements in
   current source and documentation.**

   I grouped these as one finding because they share commit `90f11af`'s
   stated documentation-sweep objective and do not change runtime behavior.

   - `src/cdata.jl:802-807` says this file is an example and that the real
     adapter has a background reaper. This file is the real adapter.
     `reap!` is the explicit scan at `src/cdata.jl:809-817`; no background
     task exists. `docs/dev/core-README.md:283-285` states the current fact.
   - `src/ArrowCore.jl:65-72`, `src/ArrowCore.jl:2071-2078`,
     `src/ArrowCore.jl:2142-2145`, and `src/table.jl:261-263` still assign
     result typing to a future `ViewPlan` or typed-view facade. The facade
     already routes closed claims through typed `materialize` at
     `src/table.jl:243-253`, and its file header states that no lazy
     typed-view layer exists.
   - `src/scan.jl:926-930` describes `rangedschema` as a one-tail-fetch read.
     `_rangedfooter` always fetches the eight-byte head and then the tail at
     `src/scan.jl:850-857`; a focused call logged
     `[(0, 8), (0, 482)]`. The fetch-protocol list also starts with only the
     tail at `docs/dev/DESIGN-scan-ranges-trim.md:128-133`, while its accurate
     request model includes one head plus one tail at
     `docs/dev/DESIGN-scan-ranges-trim.md:153-155`.
   - `docs/dev/core-README.md:153-155` says an adapter normalizes non-native
     endian data before constructing a batch. The IPC reader and writer
     explicitly refuse it at `src/ipc_read.jl:954` and
     `src/ipc_write.jl:1258`; no current adapter normalizes it.
   - `docs/dev/core-README.md:300-301` says the 32-bit ABI branch is
     "exercised only on 64-bit hosts." The branch is inspected but not
     exercised on the available 64-bit hosts.
   - `src/ArrowCore.jl:2149-2152` retains a `review follow-up R5` reference.
     `conformance/corpus.jl:47-50` still waits for the facade to formalize a
     public surface, although the facade exists.
   - `test/core_tests.jl:17-29` calls the complete `test/runtests.jl` command
     "Stdlib only", and `test/trim_compile_tests.jl:25-26` repeats that
     claim. The runner includes facade and battery files which load Tables,
     PooledArrays, DataAPI, Arrow, and other package dependencies.

   A literal scan found no remaining `prove-out`, `report §`, or
   `codex-round` spelling in the requested current-document scope. The
   multiline `review follow-up R5` text escaped that literal pattern. The
   only pinned substring touched by an error rewording was
   `little-endian host`, and the new messages retain it. No other changed
   error message intersects a pinned substring.

   Disposition: open. Correct the current facts and remove the remaining
   review/future-surface prose. The fix round can treat these as one bounded
   documentation cleanup.

## Clean dispositions

### C-data validation tier

- `validate_full` composes semantic validation before its additional work at
  `src/ArrowCore.jl:1690-1703`. Its extra checks are Field nullability
  (`src/ArrowCore.jl:1569-1677`), Date64 whole-day divisibility
  (`src/ArrowCore.jl:1177-1196`), time-of-day range
  (`src/ArrowCore.jl:1272-1292`), decimal precision
  (`src/ArrowCore.jl:1199-1269`), canonical unused/trailing bitmap bits and
  padding (`src/ArrowCore.jl:1706-1732`), and Utf8/Utf8View well-formedness
  (`src/ArrowCore.jl:1745-1755`). These are content, canonical-form, or
  advisory schema checks. None establishes allocation extent, buffer span,
  offset geometry, child extent, union routing, dictionary index geometry,
  view range, or REE run geometry.
- Structural and semantic validation establish those properties with checked
  arithmetic at `src/ArrowCore.jl:1001-1161` and
  `src/ArrowCore.jl:1295-1510`. Every raw load retains a final slice bounds
  check at `src/ArrowCore.jl:330-364`.
- C Data export/import and both stream directions use semantic validation at
  `src/cdata.jl:747-750`, `src/cdata.jl:1031-1052`,
  `src/cdata.jl:1500-1524`, and `src/cdata.jl:1815-1840`. The IPC reader uses
  the semantic tier before exposure at `src/ipc_read.jl:822-850` and
  `src/ipc_read.jl:1007-1016`. The IPC stream/file writers use it at
  `src/ipc_write.jl:609-633`, `src/ipc_write.jl:669-684`, and
  `src/ipc_write.jl:726-743`. No default adapter calls `validate_full`.
- A focused foreign-producer probe changed a bounded Utf8 data byte to
  `0xff` before C import. Import and `validate_semantic` succeeded;
  materialization returned a Julia `String` with code units `UInt8[0xff]`;
  `isvalid` was false; representation and iteration were safe; and
  `validate_full` raised `ValidationError` containing `invalid UTF-8`.
  Release and reaping restored the export registry. Dropping the full tier
  is therefore safe under the stated C ABI ownership contract.

### Stream metadata placement

- `export_stream!` places schema metadata on the struct-typed root at
  `src/cdata.jl:1597-1602`; `from_c_stream` rebuilds `Schema.metadata` from
  that same node at `src/cdata.jl:1784-1788`. This matches the Apache C++
  bridge's [root schema export](https://github.com/apache/arrow/blob/59bea6ec485e7fe351d1aa6753f964f6a6bc353a/cpp/src/arrow/c/bridge.cc#L191-L197).
- A three-level nested metadata probe and an ordered duplicate-key probe
  survived ours-to-PyArrow-to-ours exactly in both directions.
- `nothing` and an explicit empty pair vector both canonicalize to `nothing`
  through the C Stream boundary. PyArrow 25.0.1 exports a NULL metadata
  pointer for both `None` and `{}`. Apache C++ likewise encodes metadata only
  when nonempty and maps NULL or a zero pair count to no metadata in
  [bridge.cc](https://github.com/apache/arrow/blob/59bea6ec485e7fe351d1aa6753f964f6a6bc353a/cpp/src/arrow/c/bridge.cc#L267-L276).
  I accepted this C-boundary canonicalization. Finding 1 concerns invalid
  content, not placement or empty representation.

### REE unknown null count

- `ArrayData` permits `nullcount == -1` at `src/ArrowCore.jl:701-722`.
  Structural REE validation rejects only a positive parent count at
  `src/ArrowCore.jl:1135-1160`, and `_validate_ree_values` does the same at
  `src/ArrowCore.jl:1484-1510`.
- Bitmap-less `_count_nulls` returns and caches zero at
  `src/ArrowCore.jl:780-792`. REE scalar and bulk access use the run search
  and values child, not the parent cache. C export calls `nullcount(d)` at
  `src/cdata.jl:700-718`; IPC FieldNode encoding calls it at
  `src/ipc_write.jl:390-396` and `src/ipc_write.jl:439-443`. Both emit zero,
  never `-1`.
- A focused probe passed structural access with the raw cache still `-1`,
  then passed materialization, `_count_nulls`, `nullcount`, semantic/full
  validation, ArrowJSON equality against explicit zero, C export/import, and
  IPC stream round-trip. Every outward representation resolved the parent
  count to zero.

### Oracle mechanics

- Dictionary-id canonicalization at `conformance/corpus.jl:109-139` is
  sufficient for the PyArrow-rebuilt path. A pool-content mutation produced
  a comparison failure; only adapter-local id and sharing choices are
  normalized.
- `_sliceours` at `conformance/cdata_oracle.jl:207-214` passed semantic
  validation for 51/51 references and exact logical comparison for
  4,020/4,020 slots across REE, sparse/dense unions, structs, dictionaries,
  views, list views, maps, and the other implemented layouts. Parent offsets
  select the correct child slots; dense unions and offset layouts retain
  their explicit child offsets. Forcing returned offsets to zero produced a
  failure rather than a false pass.
- Successful loop paths call `pydel!` for the retained `pyb`, `native`,
  `sliced`, and reader handles. Each imported batch tree is released through
  its shared `ForeignOwner`, and each imported stream is released.
  `_releasebatch!` can use the first column because all children of the
  imported struct share one owner. Short-lived Python rebuild objects and
  error paths can defer cleanup to finalizers; the final Python/Julia GC and
  `Arrow.reap!()` cover them.
- The registry verdict is not vacuous. A live exported batch changed the
  registry count from 0 to 2; deletion and reaping returned it to 0. The full
  oracle ended with `export registries drained` at 1/1.
- Parent/child relaunch with the explicit Python interpreter bound PythonCall
  correctly. The no-`uv` fallback alone is finding 2.

### `fromcompactviews`

- Length 12 copied both words byte-for-byte. Length 13 rewrote the second
  word to buffer index 0 and the correct zero-based offset. Negative
  positions used buffer index 1 only with nonempty `extra`. `buf` and
  `extra` retained zero-copy owner identity.
- Null payloads became zero view entries with the validity bit clear and the
  exact null count. `nullable=false` with a null passed semantic validation
  and failed opt-in full validation, as designed.
- Position zero, a negative position with empty `extra`, escaping content,
  and `pos0 == typemax(Int32) + 1` raised `ArgumentError`. A backed
  `pos0 == typemax(Int32)` entry succeeded. Prefix mismatch was accepted by
  the builder and structural tier, then refused by semantic validation.
- Both `NTuple{16,UInt8}` and `UInt128` payload vectors passed full
  validation. The two-word load loop was fully inferred for concrete `P`
  with no unresolved `Any` call site. The function is not reachable from the
  current trim workload, and the trim gate stayed clean.
- Every negative length is treated as null. This matches the CSV kernel's
  `len < 0` rule. The little-endian word/value claim also matches the real
  kernel's two-`UInt64` payload layout.

### Short-form definitions and remaining sweep checks

- Commit `552e657` converts exactly seven `= begin` definitions to
  `function ... end`: `_bitmap`, `_bitmapbytes`, local `lookup`, `_exactdiv`,
  `_rawcolumn`, `Stream`, and local `retainedfield`. Explicit returns preserve
  the old final expressions and all early returns. The full gates found no
  behavior change.
- I did not report the inline-view padding wording at
  `src/ArrowCore.jl:1415-1421`. It classifies canonical unused inline bytes
  as full-tier concern; it does not clearly claim that the check is already
  implemented, and earlier review records disclose it as remaining work.
- Changed user-visible error messages retain every substring pinned by the
  current tests. The package and conformance gates pass.

## Assumptions and decisions

- I treated schema and Field metadata UTF-8 as a mandatory Core structural
  invariant at every adapter boundary. A C producer may carry arbitrary
  bytes, but `from_c_stream` must either reject them or normalize them before
  it returns a Core `Schema`.
- I treated metadata as an ordered pair sequence because Core explicitly
  preserves order and duplicate keys. Integration-JSON value comparison may
  remain unordered, but it is not enough to prove exact C metadata fidelity.
- I accepted C Stream empty-metadata canonicalization because Arrow.jl,
  PyArrow, and the Apache C++ bridge agree. I did not require `Pair[]` to
  survive as distinct from `nothing` at this boundary.
- I rated the zero-batch schema issue MEDIUM because it silently returns an
  invalid Core schema. It is not a geometry or memory-safety failure. I rated
  the oracle, compact-view error type, and documentation issues LOW because
  current valid data values remain correct and safe.
- The Arrow C ABI does not declare allocation extents for most buffers. As in
  prior rounds, I assumed producer-declared pointers remain live and stable
  through the ownership move. The invalid-UTF8 probe mutated producer-owned
  storage before import, while the producer still owned it.
- The host was 64-bit arm64 macOS with Julia 1.12.6. The IPC oracle used
  PyArrow 20.0.0 and nanoarrow 0.9.0. The C-data oracle used PyArrow 25.0.1.
- The active Tables.jl development checkout moved during this review from
  the scan-capable `d1fbb6eb577741688dba70039754166b51c1cdcc` to
  `ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`, which removed
  `Tables.apply`. The first live package-gate attempt therefore exited 1
  when a C-data battery child tried to precompile Arrow. This is external
  development-dependency drift, not an Arrow HEAD regression. I did not
  alter that checkout. I repeated every required gate in an isolated Arrow
  worktree at the exact target commit with Tables pinned to `d1fbb6e`.
- I did not modify product or test code. All probes stayed in scratch
  locations. I removed the isolated worktrees after verification. The six
  protected untracked files remained present and untouched. This review
  document is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0
  in the isolated exact-HEAD worktree; 702 reported assertions: ArrowCore
  422/422, threaded caches 4/4, facade 272/272, and IPC read, IPC write,
  C Data, and ranged-scan acceptance 1/1 each.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6,
  compile plus run passed, with zero verifier errors and zero verifier
  warnings.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 documented skips.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 documented skips with PyArrow 20.0.0 and
  nanoarrow 0.9.0.
- `ARROW_CDATA_ORACLE_PYTHON=/Users/jacob.quinn/.cache/arrow-julia/cdata-oracle-venv/bin/python julia --project=conformance --startup-file=no conformance/cdata_oracle.jl`
  — exit 0; PyArrow 25.0.1 over 38 families, 141 pass / 0 fail / 9 skips:
  ours-to-PyArrow C 37/37, PyArrow-native 37/37, sliced export 29/29,
  C Stream 37/37, and registry drain 1/1.
- `git diff --check` — exit 0 before and after the report. `git diff --check
  7cb9675..90f11af` also exited 0 for the exact five-commit delta.
- Invalid-Utf8 C Data probe — exit 0; import and semantic validation passed,
  materialization remained safe, full validation refused, and registries
  drained.
- REE unknown-null-count probe — exit 0 across structural/full validation,
  access, JSON, C Data, and IPC.
- Stream metadata placement/empty/nested/duplicate probes — exit 0. Invalid
  zero-batch metadata probes reproduced finding 1 and drained registries.
- Oracle corruption probes — metadata reordering falsely compared equal as
  described in finding 3; dictionary-content and forced-offset corruptions
  were detected. Slice references passed 51/51 arrays and 4,020/4,020 slots.
  The live-registry probe rose from 0 to 2 and returned to 0.
- Compact-view edge and inference probes — exit 0 for every requested valid
  and refusal case; the two extreme positions reproduced finding 4 with
  managed `OverflowError`.
- Final HEAD remained
  `90f11af6edfa889ac01408fa10b12d4bed96eb33`. Repository status contained
  only the six protected untracked files plus this review document.

VERDICT: FINDINGS
