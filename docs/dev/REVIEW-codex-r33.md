# Arrow.jl 3.0 code review — round 33

Date: 2026-08-16

Scope: commits `f30fb5334446f8a47c2dbd2c01a5742f5ff2338b` and
`3e0b7c71335e6418f8fcb2a3da5d0e26e875e504` only, with the facade request in
`Arrow_Review.md` item 17 as the public-surface authority.

## Result

Five of the six round-32 findings are closed. The mmap, shared C-data
revocation, `OP_NE`, borrowed-root, and owner-only-empty-import fixes all
passed focused probes. The runtime `Ptr{Cvoid}` release trampolines also
survived an isolated precompile/reload and a concurrent close/finalizer race
without a double release. The documentation finding is not closed.

The facade is not ready. It has five high-severity correctness findings:
partition name/order drift silently relabels values; field types and
nullability depend on observed values; temporal scans disagree across file,
stream, and ranged inputs; the ranged path discards its schema; and a
`Table`/`Stream` read-write cycle silently loses Arrow logical types and
metadata. Empty and zero-column inputs, DataAPI, closed streams, and
multi-batch scan performance expose additional findings.

## Findings

1. **HIGH — later partition names and order are ignored, so data can be
   silently assigned to the wrong fields.**

   `_writebytes` builds `pairs` for every partition, but uses the fields only
   for the first schema. For every later partition it discards `p[1]` and
   passes only the positionally ordered `p[2]` arrays to the first schema
   (`src/write.jl:149-163`). Structural validation can detect a different
   physical type. It cannot detect a different name when the types match.

   A focused probe wrote these partitions:

   ```julia
   (left = Int64[1], right = Int64[10])
   (right = Int64[20], left = Int64[2])
   ```

   The result was:

   ```text
   left  = [1, 20]
   right = [10, 2]
   ```

   The expected result by name is `left = [1, 2]` and
   `right = [10, 20]`. A one-column rename from `x` to `y` was also accepted
   as one `x` column. This is silent data corruption at the facade boundary.

   Int64-to-Int32 and Int32-to-Int64 drift did reject, but both errors were
   only:

   ```text
   ValidationError("field/type mismatch: IntType vs IntType")
   ```

   The error does not identify the partition, column, signedness, or width.
   Every partition must be checked against the first partition's field names,
   order, logical descriptors, and compatible nullability before its arrays
   are attached to that schema.

2. **HIGH — facade column types and field nullability are inferred from
   observed values instead of the Arrow field.**

   Core materialization returns `Vector{Any}`. `_facadecolumn` narrows that
   vector with `map(identity, col)` (`src/table.jl:158-164`), and
   `Tables.schema(::Table)` then reports the value-derived `eltype`
   (`src/table.jl:69-70`). The result depends on the rows in the current table
   or batch rather than `Field.type` and `Field.nullable`.

   Focused results were:

   ```text
   declared Union{Missing,String}, values ["a", "b"]       -> Vector{String}
   declared Union{Missing,String}, values [missing,missing] -> Vector{Missing}
   declared Int64, zero rows                                -> Vector{Any}
   declared Union{Missing,String}, zero rows                -> Vector{Any}
   ```

   A nullable input column with no current nulls also writes a non-nullable
   field because the builders set nullability from `null_count > 0`, not from
   the declared Julia element type (`src/ArrowCore.jl:2220-2221,2242-2246`).
   Sibling `Stream` partitions can therefore expose different Julia eltypes
   for one Arrow field.

   The all-missing case has a worse read-write failure. A nullable UTF-8 field
   materializes as `Vector{Missing}`. On rewrite,
   `Base.nonmissingtype(Missing)` is `Union{}`. Bottom is a subtype of
   `Dates.Date`, so `_writecolumn` selects its first temporal branch
   (`src/write.jl:41-49`). The exact schema transition was:

   ```text
   Utf8Type nullable=true
     -> Arrow.Table column eltype Missing
     -> Arrow.write(Table)
     -> DateType(DAY) nullable=true
   ```

   Typed empty facade columns cannot be rewritten at all because their
   `Vector{Any}` reaches Core as an unsupported element type. A valid
   schema-only IPC stream with one Int64 field and zero record batches fails
   even earlier: `_facadecolumn` calls `reduce(vcat, parts)` with no parts and
   throws `ArgumentError: reducing over an empty collection`.

   Narrowing must be driven by a closed mapping from `Field` to the intended
   Julia element type, including the declared nullable union. It cannot use
   the observed values as the schema authority.

3. **HIGH — temporal `Tables.Scan` behavior is not differentially equal
   across facade source formats.**

   The file path applies the scan to the low-level `ArrowFile` before facade
   conversion (`src/table.jl:216-219`). Residual filtering therefore compares
   raw Date/Timestamp storage integers with `Date` or `DateTime` literals. The
   stream path first builds a converted `Table`, applies `Tables.finish`, and
   then `_wrapscanned` applies the same temporal conversion a second time
   (`src/table.jl:220-222,247-259`). Renames make `_wrapscanned` miss the source
   field because it looks up the output name in the input schema.

   Differential probes produced:

   ```text
   timestamp equality authority: x=[3], stamp=[1970-01-01T00:00:02]
   file facade result:           x=Any[], stamp=Union{Missing,DateTime}[]
   stream facade result:         MethodError: Int64(::DateTime)

   date equality authority:      x=[3], date=[2024-01-03]
   file facade result:           x=Any[], date=Union{Missing,Date}[]
   stream facade result:         MethodError: Int64(::Date)
   ```

   Even a stream scan that only selects a Date column throws the double-
   conversion error. A file scan that renames that Date column returns the
   raw epoch integer because the renamed output no longer matches a schema
   field. Integer and string scan values passed. The temporal paths did not.

   Scan planning needs one defined value domain. Facade predicates must either
   be translated to physical values before pushdown, or temporal filtering
   must remain residual until values are in their public Julia form. Facade
   post-conversion must then run exactly once, including after a rename.

4. **HIGH — `Arrow.Table(::RangedFile)` discards the schema and returns a
   different public table from the same full file.**

   The ranged branch unconditionally calls `_wrapscanned(got, nothing)`
   (`src/table.jl:209-212`). `_wrapscanned` needs the schema for temporal
   conversion and stores it for DataAPI (`src/table.jl:247-261`). Passing
   `nothing` loses field types, nullability, schema metadata, field metadata,
   and dictionary/temporal interpretation at the facade.

   The same Timestamp-millisecond file returned `DateTime` through the normal
   file path but returned:

   ```text
   values = Any[1000, 2000]
   eltype = Any
   stored schema = nothing
   ```

   `DataAPI.metadata` and `DataAPI.colmetadata` on that table then threw a
   `FieldError` while trying to access `nothing.metadata`, even though
   `DataAPI.metadatasupport(Table).read` reports true.

   Ranged fetch planning itself passed: the focused selection returned the
   correct rows and fetched 16,185 of 47,914 bytes. The defect is the facade
   result, not the sparse-fetch plan. The schema parsed from the verified
   footer must be carried through the scan result and into `Table`.

5. **HIGH — `Arrow.write` ignores the Arrow schema retained by a facade
   `Table` or `Stream`, causing silent schema and metadata drift.**

   `Table` stores the decoded `AC.Schema` (`src/table.jl:48-54`), but
   `_writebytes` always infers new fields from the materialized Julia columns
   and uses only explicit metadata keyword arguments (`src/write.jl:147-169`).
   It does not consume the retained schema or DataAPI metadata.

   A read-then-write probe confirmed these silent transitions:

   ```text
   Date64                 -> Timestamp millisecond
   Timestamp second       -> Timestamp millisecond
   Timestamp microsecond  -> plain Int64
   Timestamp nanosecond   -> plain Int64
   Time second            -> Time nanosecond
   Time millisecond       -> Time nanosecond
   Time microsecond       -> Time nanosecond
   DictionaryType         -> Utf8Type
   schema metadata        -> absent
   field metadata         -> absent
   ```

   Date32 and Duration units stayed stable. Values also stayed exact, including
   microsecond/nanosecond timestamp values `1` and `1001`; there was no silent
   sub-millisecond numeric truncation. The logical Arrow schema still changed.

   Direct `DictEncode` input does initially build a `DictionaryType`, but the
   facade materializes it as a plain vector and provides no `DataAPI.refpool`;
   rewriting loses the encoding. Two file-format partitions with identical
   `DictEncode` values also failed because the facade built two distinct pool
   objects and the file writer treated them as a replacement. The same input
   passed in stream format.

   The public `Table`/`Stream` writer path needs a schema-aware fast path. It
   must preserve field descriptors, nullability, metadata, and dictionary
   intent when the values still satisfy that schema.

6. **MEDIUM — zero-column row counts are lost in both write and read
   facades.**

   `_writebytes` sets `n = 0` whenever a partition has no columns
   (`src/write.jl:161`), even when `Tables.rowcount(part)` is nonzero. `Table`
   stores no row count of its own (`src/table.jl:48-54`), so it cannot expose a
   nonzero count when there is no column from which to infer it.

   A valid zero-column Tables source with row count 3 wrote a zero-row batch.
   A low-level valid zero-column, three-row Arrow batch also read as row count
   0 through both `Table` and `Stream`. This matters for `select=()` scans and
   for valid Arrow record batches with no fields.

   Zero-row partitions that did have columns retained their batch mapping:
   `Stream` reported three batches with lengths `[0, 2, 0]`. Their empty
   columns still had the type-loss defect in finding 2.

7. **MEDIUM — the declared DataAPI read support is incomplete and has
   incorrect missing-schema behavior.**

   The methods at `src/table.jl:74-107` implement the simplest string-key and
   Symbol-column lookups only. Focused conformance calls found:

   - no `metadata(t, key, default)` method;
   - no `colmetadata(t, col, key, default)` method;
   - no Int column selector, although DataAPI requires Symbol and Int;
   - no zero-argument `colmetadatakeys(t)` iterator;
   - therefore no working aggregate `DataAPI.colmetadata(t)`;
   - a ranged/missing schema throws `FieldError`, not `KeyError`.

   The last failure comes from the boolean guards at
   `src/table.jl:81-87,100-107`: when `sch === nothing` or `f === nothing`,
   `||` short-circuits before the intended throw, and the following loop
   dereferences `nothing`. Standard unscanned file metadata and field metadata
   passed, including the `style=true` form.

8. **MEDIUM — `close!(::Stream)` has no stream state and does not always
   stop iteration.**

   `Stream` stores only a source and its regions. `close!` closes those regions,
   and `iterate` infers closure only if materialization happens to touch a
   revoked buffer (`src/table.jl:274-304`). A normal nonempty next batch throws
   `InvalidStateException`, and repeated close is idempotent. A zero-row next
   batch touches no buffer and is returned after close.

   This contradicts the public close docstring, which says a closed stream
   refuses further iteration (`src/table.jl:110-116`). `Stream` needs its own
   monotonic closed state checked before every iteration, independent of
   batch shape.

9. **MEDIUM — the facade exposes quadratic aggregation paths, while
   `_facadecolumn` itself is linear but type-unstable.**

   `_facadecolumn` receives `Vector{Vector{Any}}`. Base selects its optimized
   `_typed_vcat` method, so concatenation plus `map(identity)` is O(rows), not
   quadratic. Allocation scaled linearly to about 16 MB for one million rows
   across four parts. This meets the stated v1 complexity allowance.

   It is not type-stable. `@code_warntype` reports `Body::Any` because the
   `map(identity)` result and `_postconvert` result depend on runtime values.
   `Tables.getcolumn(::Table, ...)` also infers only `AbstractVector` because
   columns are stored in `Vector{AbstractVector}`. This instability is the
   mechanism behind finding 2, not just a compiler-display concern.

   Two separate facade paths are genuinely quadratic:

   - Both scan implementations store parts in `Vector{Any}` and call generic
     pairwise `reduce(vcat, ...)` (`src/scan.jl:525-538,989-1005`). Allocation
     for 1,000-row batches rose from 532,032 bytes at 10 batches to 10,954,816
     at 50 and 41,956,544 at 100. The actual 100-batch facade scan allocated
     47,615,680 bytes versus 4,740,320 for the no-scan facade read.
   - `_withcolmeta` rebuilds `Dict(colmetadata)` for every field
     (`src/write.jl:174-179`). A warmed 1,000-field call allocated about
     35.7 MB; 2,000 fields allocated about 141 MB.

   The scan part container must keep a vector-of-vectors element type so Base's
   linear concatenation specialization applies. Column metadata must be
   normalized once before the field loop.

10. **LOW — a generator used as a column fails through an internal
    `MethodError`.**

    `_writecolumn` accepts only `AbstractVector` (`src/write.jl:41`). Ranges
    passed, and a generator of NamedTuple rows passed after Tables collected
    it. A custom column-access Tables source whose column was a generator
    failed with:

    ```text
    MethodError: no method matching
    _writecolumn(::String, ::Base.Generator...)
    ```

    This was an explicit facade edge in the review request. Either collect
    iterable columns behind a bounded materialization path, or reject them at
    the public boundary with a useful `ArgumentError` that states the accepted
    Tables column contract.

11. **LOW — round-32 documentation is still stale, and the public manual
    still describes the removed 2.x facade.**

    Commit `3e0b7c7` changes no Markdown file. Current contradictions include:

    - `README.md:20-26` and `src/Arrow.jl:34-38` say `Table`, `Stream`, and the
      writers have not landed.
    - `src/cdata.jl:60-64` says imported C data has no revocation machinery.
    - `docs/dev/core-README.md:76-101` says regions have no atomic state, eager
      unmap, or revocation.
    - `docs/dev/core-README.md:322-332,370-375` excludes the facade and says
      mmap release waits for GC.
    - `test/core_tests.jl:83-84` and `test/trim_entrypoint.jl:43-45` say there
      is no lifecycle state and nothing to close.
    - `docs/src/manual.md:53-96,195-201` promises zero-copy `ArrowVector`
      columns, `convert=false`, old indexing/dictionary methods, and
      `getmetadata`. The new facade materializes plain vectors; those options
      and APIs are absent. Later manual sections also document the old
      `Writer`/`ntasks` surface.

    Thus round-32 finding 6 is not closed. The facade is also publicly
    documented as both absent and as its incompatible 2.x predecessor.

## Round-32 closure details

### 1. Mmap `close!` — closed

`mmapregion` now gives its cell the backing `Memory{UInt8}`, which is the
object on which Mmap installs its finalizer (`src/ArrowCore.jl:263-283`). The
original observer probe reported the observer finalizer as true immediately
after `close!`. A separate, deliberately unsafe child dereference after close
exited 139/SIGSEGV, confirming that the mapping was gone. The main probe did
not dereference after unmap.

### 2. Shared C-data revocation — closed

`ForeignOwner` constructs one `ReleaseCell` (`src/cdata.jl:883-913`), and all
fixed and variadic imported regions receive that cell
(`src/cdata.jl:1189-1281`). An imported UTF-8 column had separate offset and
data regions. The probe confirmed the same cell, closed one region, observed
the cell as closed inside the producer callback before release, and got
`InvalidStateException` from both sibling `sliceptr` paths. The callback ran
once and the export reaped once.

### 3. `OP_NE` pruning — closed

The explicit branch at `src/scan.jl:1402-1413` prunes only when
`min == max == literal`; unknown operators now conservatively fetch. Direct
results were:

```text
constant equal -> false (prune)
mixed bounds   -> true  (fetch)
NaN bounds     -> true  (fetch)
```

Mutating one batch's maximum from `0.0` to `1.0` flipped `_maypass` from
false to true. Row-level `colne(x, 0.0)` retained `[1.0, NaN]`. The complete
statistics differential, including whole-file and ranged scans for equal,
mixed, signed-zero, and NaN batches, exited 0.

### 4. Borrowed heap roots — closed

`ReleaseCell()` has a null action and `heapregion` uses it
(`src/ArrowCore.jl:161-162,241-249`). A caller finalizer ran zero times during
`close!` and ran once only when the caller later finalized its own vector.
Close still revoked all slice access.

### 5. Owner-only empty imports — closed

`ArrayData.owner` retains the `ForeignOwner`, and `close!(ForeignOwner)` closes
its cell (`src/cdata.jl:967-975,1280-1281`). An empty imported Int64 array had
zero regions. Closing through the owner set the cell closed, ran the producer
release once, and reaped once.

### 6. Documentation — not closed

See finding 11.

### Ptr-ABI precompile and exactly-once race

The two new release-action `@cfunction` sites are evaluated inside runtime
functions at `src/ArrowCore.jl:280` and `src/cdata.jl:900-901`. No
module-level `const` cfunction trampoline exists.

An isolated new depot instantiated and precompiled Arrow, then a separate
compiled-module process ran the full release probe. It exited 0, so the result
did not reuse this checkout's old package image.

For the release race, 300 concurrent pairs ran `close!(owner)` against the
owner's registered `finalize(owner)` path. Both orderings occurred. Every cell
ended closed, every owner ended released, and the producer callback count was
exactly 300. A second run with `MallocErrorAbort=1` and `MallocScribble=1` also
exited 0. Both paths route to `release!`; its atomic swap at
`src/cdata.jl:974-986` selected the only callback/free winner. Natural GC
cannot finalize an owner that the close task still strongly references, so
the explicit `finalize` call is the registered-finalizer race harness.

A nonconforming producer that failed to null its release field propagated the
intended `C Data producer release did not mark the structure released` error;
there was no fatal cfunction unwind or double free.

## Alias-shadow sweep

No remaining alias-shadow finding was found.

- Arrow exports only `close!`, consistent with a narrow package surface.
- The real Base collision is `Arrow.write`; every wholesale battery/corpus
  alias loop now skips it.
- `Table` and `Stream` remain undefined in those alias modules.
- Bare `write` resolves to `Base.write` where the test and corpus code use it.
- `Meta`, `Schema`, and `materialize` resolve to the intended Arrow internals.
- PooledArrays adds no conflicting alias candidate.
- The battery suite, corpus, and oracle all ran through these environments.

## Coverage gaps

The repository now has a direct mmap Memory-finalizer assertion, a borrowed
heap-finalizer assertion, and a generic shared-cell test. It does not have
durable regression tests for three safety-specific paths exercised by the
external probes:

- a real multi-buffer `ForeignOwner` import where closing one UTF-8 region
  revokes its sibling before producer release;
- an owner-only empty import closed through `ForeignOwner.cell` (the current
  battery still calls `release!` directly);
- `ReleaseCell.close!` racing the registered ForeignOwner finalizer (the
  existing stress helper races `release!` with `finalize`).

These are test gaps, not observed runtime failures. They should be added with
the fixes because they pin the load-bearing wiring that a generic cell test
cannot cover.

The 61 facade tests are also value-focused. They do not assert retained
logical descriptors, declared nullability, post-scan eltypes, read-write
metadata, same-typed partition name drift, schema-only streams, zero-column
row counts, or temporal scan parity. The focused failures above demonstrate
that equal values alone are not a sufficient facade round-trip oracle.

## Checks that passed

- Direct facade conversion decoded Date32, Date64, timestamp seconds and
  milliseconds, all four Time units, and all four Duration units to the
  intended Julia values.
- Microsecond and nanosecond timestamps stayed as exact raw Int64 values on
  file, stream, scan, and rewrite paths. No sub-millisecond truncation was
  observed.
- Direct facade writing round-tripped Date, DateTime at millisecond precision,
  Time at nanosecond precision, and Second/Millisecond/Microsecond/Nanosecond
  duration columns.
- Direct `DictEncode` produced dictionary storage and correct values for a
  single partition. Stream-format pool replacement across partitions worked.
- Partitions mapped one-to-one to record batches, including zero-row batches
  with columns.
- Integer/string facade scans matched `Tables.finish` by value for file and
  stream input.
- Ranged selection returned the correct rows and fetched fewer bytes than the
  full object.
- Standard file DataAPI metadata returned schema- and field-level values.
- Facade mmap `close!` closed the region, retained the copied column values,
  and allowed file deletion.
- Ranges and generators of rows were accepted by the writer.

## Assumptions and decisions

- I reviewed exact HEAD `3e0b7c71335e6418f8fcb2a3da5d0e26e875e504`
  against the recorded round-32 state at
  `f30fb5334446f8a47c2dbd2c01a5742f5ff2338b`.
- I kept the development Tables dependency unchanged.
- I treated schema fidelity as preserving Arrow logical descriptors, field
  nullability, schema metadata, field metadata, and dictionary intent through
  public facade round trips, not only preserving current values.
- I treated a closed stream's documented refusal as independent of whether a
  later batch happens to touch a physical buffer.
- I included the existing scan aggregation paths because `Arrow.Table(...;
  scan=...)` makes them part of the requested public facade and the review
  explicitly asked for embarrassing quadratic behavior.
- The release probes ran on Julia 1.12.6 on macOS. Windows deletion was not
  executed, but the probe observed the Mmap owner finalizer and a child proved
  the mapping was actually inaccessible after close.
- This was a review task. I added only this report. I did not apply product
  fixes, change dependencies, or touch the six files that were already
  untracked at review start.

## Validation

- `julia --project=. -e 'using Pkg; Pkg.test()'` — exit 0; Core 342/342,
  threaded Core 4/4, facade 61/61, and all four adapter batteries passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6 and zero
  trim-verifier errors.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` — exit
  0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` — exit
  0; 170 pass / 0 fail / 43 skip with the existing daemon and cached image.
- Direct complete statistics battery (`_stats_main`) — exit 0; whole-file and
  ranged differential, signed-zero, NaN, constant-equal, mixed-batch,
  malformed-statistics, and pruning/decode checks passed.
- Direct `OP_NE` mutation probe — exit 0; false/true/true for constant-equal,
  mixed, and NaN bounds, then false-to-true after the bound mutation.
- Focused lifecycle probe with four Julia threads — exit 0; mmap, shared
  ForeignOwner siblings, heap borrow, empty owner, and 300 release races
  passed.
- The same lifecycle probe under allocator abort/scribble — exit 0.
- Isolated-depot instantiate/precompile plus a separate compiled-module
  lifecycle process — exit 0.
- Focused facade and temporal probes — exit 0 after asserting the documented
  passes and capturing the findings above.
- `git diff --check f30fb53..3e0b7c7` — exit 0.
- Final repository status retained only the six pre-existing untracked files.

VERDICT: FINDINGS
