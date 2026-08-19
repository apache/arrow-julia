# Design: Tables.Scan pushdown, cloud byte-range reads, and the trim contract

Implemented in `src/scan.jl` and exposed through the facade
(`Arrow.Table(source; scan=…)`, `RangedFile`); §5 lists what is and is not
built. The three pieces share one mechanism: **a bound column set drives
both what gets decoded and what gets fetched, and every request value is
plain data intended to remain visible to the trim verifier.** Section 4
separates that design intent from what the trim harness actually compiles.

---

## 1. Tables.Scan support

`Tables.Scan` (Tables.jl `jq/scan` branch) is a plain-data scan request —
select/rename/type items, a closed predicate algebra (`Cmp`/`In`/`IsNull`/
`StrPred`/`And`/`Or`/`Not`, with `OpNode` as the growth channel), `limit`/
`offset` — consumed via `apply(source, scan) -> (table, residual)` +
`finish(table, residual)`. Key contract points this design leans on:

- pushed and residual work may **overlap** (inexact pruning keeps the filter
  in the residual);
- `limit`/`offset` may be consumed **only** when every applied filter was
  exact;
- no `Function` fields anywhere — the algebra is closed and value-only.

### What Arrow can push, by axis

| Axis | Mechanism | Exactness |
|---|---|---|
| `select` | decode only (selected ∪ filter-referenced) columns: a registry-driven `skipfield!` advances the node/buffer cursor past unselected fields without body slicing, content validation, or materialization. Complete node/buffer metadata is still validated first. Nested subtrees skip with their parent; no body range is requested for an unselected dictionary column. | exact as IO/decode reduction (see below for who projects) |
| `limit`/`offset` | `RecordBatch.length` is wire metadata: whole batches before `offset` and after `offset+limit` are never decoded. Ranged reads still fetch candidate RecordBatch metadata because Footer Blocks have no row counts, but request no body range for excluded batches. Tail reads and configured coalescing may physically over-read otherwise unrequested bytes. | exact when no filter; poisoned by any filter per the contract |
| `filter` | two tiers: (a) **statistics pruning** — per-batch min/max/null-count, when present (§3 of this doc), prune batches that cannot satisfy the predicate; (b) **mask at materialization** — evaluate the predicate over decoded columns through Core accessors and apply the mask when building output columns. | (a) inexact — filter stays in residual; (b) exact — enables limit pushdown with filters |
| `types` (`ref => T`) | left in the residual for `finish`'s elementwise convert. Arrow's schema is source-fixed; an override is a conversion request, not a parse seed (unlike CSV). Exception: see §4 — in trim mode the overrides double as the known-schema pin. | residual |

### The `apply` shape — two stages

**Stage A (adapter-level; what is implemented).** `Tables.apply` on the file/stream
handles does **IO-and-decode reduction with a full residual**:

    apply(f, scan) =
      bind against schema names →
      decode set = selected ∪ filtercols (source order, source names) →
      resolve positional filter refs to source names →
      batch set = limit/offset window (when filter === nothing),
                  ∩ stats-surviving batches (when stats present) →
      return (table over decode set, residual)

where the residual is the original scan minus `limit`/`offset` when those
were consumed. Critically, when the filter references unselected columns the
returned table **keeps them under source names and leaves `select` in the
residual** — `finish` then filters, projects, renames, and converts. This is
the only correct composition: if the adapter consumed `select` while leaving
`filter` in the residual, `finish` could not evaluate predicates over
already-dropped columns. Simple, correct, and captures the dominant win:
unselected columns cost zero decode and add zero planned body bytes. Tail reads
and coalescing may still over-read them under §2's explicit policy.

Two refinements the differential tests forced (both implemented in
`src/scan.jl`):

- **The residual selection must be RESOLVED, not passed through.** `Not` and
  `Regex` select items re-bound against the reduced output table are wrong
  (`Not(:x)`'s excluded name no longer exists; a regex can over-match a
  filter-only column). The residual carries the bound columns as concrete
  source-name items with their renames and type overrides attached.
- **The residual filter must resolve positional references too.** A bound
  `col(3)` means source column 3. Re-binding that integer against the reduced
  decode-set table can select a different column or fail. Matched integer
  references therefore become source-name references in the residual.
- **Wire row counts are trusted only after metadata validation.** Before a
  `RecordBatch.length` drives a window, it is range-checked and matched to
  every top-level FieldNode length. Exact node/buffer counts and buffer
  geometry are also checked from metadata alone. Aggregate scan row counts
  must fit Tables' `Int` row-count API: Stage A accepts a planned result through
  `typemax(Int)` and rejects a larger one. An offset-only window represents
  `limit=nothing` explicitly; it does not use a finite sentinel that can omit
  later batches.
- **One apply call has one allocation budget.** Standalone lazy `file[i]`
  calls retain their documented per-call budgets. A scan that visits many
  batches shares one budget and codec state across all of its metadata and
  decompression work, matching the ranged operation.
- **Authority-overflow windows stay residual.** The current `Tables.finish`
  implementation forms `offset + 1` and `offset + limit` with unchecked
  `Int` arithmetic. Stage A does not consume a request when either expression
  would overflow, so the apply/finish equation remains exact until Tables
  adopts saturating window arithmetic.
- **Stage A needs no row-level predicate evaluator.** The filter always
  stays in the residual, so `Tables.finish`/`filtermask` do row evaluation;
  Arrow-side predicate logic first appears as the *interval* ladder for
  statistics pruning (§3). Stream handles keep the default no-push `apply`
  — the eager stream reader has already decoded by the time `apply`
  runs; stream pushdown would need an incremental framer.

**Stage B (facade-level; not implemented).** A facade `apply` that consumes
everything exactly: per-column masks evaluated through Core accessors (no
materialization of excluded rows), projection/renames applied at column
construction, `limit`/`offset` composed with exact masks. Residual: empty,
CSV-kernel style. Stage B subsumes Stage A.

A Stage B row evaluator would be a **closed `isa` ladder over the closed
`ScanExpr` set**, walking Core accessors (`isvalid_at` + `_value`)
column-at-a-time. Stage A implements only `_maypass`, a separate closed ladder
over statistics values. `Tables.bind` rejects `OpNode` because this adapter
recognizes none. No closures or `Function` fields are needed.

The statistics fold resolves dictionary indices through the pool before it
computes logical null/min/max values. A Stage B row evaluator could test
equality/membership against each stable pool snapshot once and then compare
indices; that pool-index optimization is not part of Stage A.

---

## 2. Cloud byte-range reads

### Why the format already supports this

The IPC **file** format is: magic · messages · Footer(schema, dictionary
Blocks, record Blocks) · footer-length · magic. Every Block carries
`(offset, metaDataLength, bodyLength)`; every RecordBatch header carries a
per-buffer `(offset, length)` table within its body; the registry walk maps
buffer indices → fields deterministically (the exact mechanism
`decodefield`/`encodefield!`/`skipfield!` share). So the fetch plan for
"columns X, Y of batches 3..7" is pure arithmetic over two small metadata
reads. Compressed buffers are self-contained (per-buffer prefix + frame), so
they range-fetch identically. The **stream** format has no footer and stays
sequential — cloud-native access is a file-format feature, stated plainly.

### The fetch protocol

1. **Head + tail fetch** (two range requests): the eight-byte head (magic
   + padding check) and the last `tailbytes` (default 64 KiB). The tail
   covers footer-length + magic + the whole Footer in almost every real
   file; if `footerlen + 10 > tailbytes`, one exact follow-up fetch.
   → schema, Block indexes, (§3) statistics — everything pruning needs.
2. **Statistics prune** from the Footer metadata — zero additional fetches.
3. **Block metadata fetches**: dictionary metadata plus
   `(offset, metaDataLength)` for each statistics-surviving record batch,
   coalesced across nearby spans. RecordBatch row counts are here, not in the
   Footer, so `limit`/`offset` windowing happens after this pass.
4. **Window and buffer-range plan**: row counts choose the exact batch/body
   window when there is no filter; the bound column set then maps to a buffer
   index set (subtree-
   inclusive; dictionary Blocks for selected dictionary columns) → byte
   ranges → **coalesce** ranges with gaps below `coalesce_gap` (default
   ~256 KiB — a gap fetch is usually cheaper than a request round-trip;
   both knobs are options, not constants).
5. **Body fetches**: each coalesced range lands in its own owned heap
   region. Decode resolves each declared buffer `(offset, len)` to its
   containing fetched range and subslices — the message-body-authority
   invariant becomes *"every buffer must fall inside a fetched range that
   was itself derived from the verified buffer table"*: same trust story,
   sparse backing.

Request-count model (what actually matters against cloud latency): `1` head
+ `1` tail (plus one exact Footer follow-up when the tail is too small)
+ `⌈candidate metadata spans after coalescing⌉` + `⌈coalesced body ranges⌉`.
For a 40-column file reading 3 columns of every batch, this moves roughly
`3/40` of the body bytes plus metadata. Statistics-pruned batches contribute
no requested metadata/body range. Coalescing is an explicit over-read policy,
so a requested span may cross otherwise unneeded bytes when the configured
gap permits it.

### The interface (no HTTP/CloudStore deps in Arrow)

Arrow defines a minimal fetcher contract and owns the planner; transports
live in extensions:

    struct RangedSource{F}          # name bikesheddable
        fetch::F                    # fetch(offset::Int64, len::Int64) -> Vector{UInt8}
        len::Int64                  # total object length, known up front
    end
    fetchranges(s::RangedSource, ranges) -> Vector{Vector{UInt8}}
        # default: serial map over s.fetch; transports override for
        # concurrent range GETs (CloudStore does this well) — concurrency
        # stays in the extension, never in Arrow.

- The entry points are `Tables.scan(RangedFile(source), scan)` and
  `Arrow.Table(RangedFile(source); scan=…)`. A
  production `readfile(::RangedSource; scan=...)` can make the existing
  whole-buffer and `mmapregion` paths trivial `RangedSource`s
  (fetch = copy/subslice), so ONE reader serves local and remote and the
  differential test is free: sparse fetch ≡ whole-file read, plus
  fetch-count/byte-count assertions on a counting test source.
- **Why a parametric functor field and not an abstract type**: dispatch cost
  is irrelevant (IO-bound), but trim is not — an open abstract type makes
  `fetch` a dynamic call the verifier cannot resolve; a concrete `F` in a
  trimmed app is statically known. This is runtime plumbing, not a `Scan`
  value, so the no-`Function`-fields rule for plain-data requests does not
  apply to it. (Decision point for Jacob: if extension ergonomics ever
  demand an abstract type, a closed core ladder + open-only-in-extensions
  split is the fallback; the functor is simpler and trim-cleaner.)
- Extension: `ArrowCloudStoreExt` (loaded with CloudStore.jl) provides
  constructors from S3/Azure objects → `RangedSource` with concurrent
  `fetchranges` and object-length discovery (HEAD). An `ArrowHTTPExt` shape
  is identical if ever wanted. Zero new hard deps.
- Explicitly out of scope v1, documented: caching/prefetch policy beyond
  coalescing, retries (the fetcher's job), writers over ranges, stream
  format, mutation detection (ETag pinning is the extension's concern —
  the fetcher closure can bake in `If-Match`).

The ranged reader deliberately uses the Footer schema as its sole schema
authority. It does not parse or cross-check the leading schema message or
optional EOS marker, although a head, tail, or coalesced request can physically
over-read those or other unrequested bytes. The complete Footer Block index is
bounded and checked for overlap. Required features and message limits are
global. Per-record metadata/body/buffer limits stay lazy like `ArrowFile`:
dictionary blocks and statistics-surviving record candidates are checked, while
statistics-pruned record metadata causes no dedicated range request and is not
validated (a tail or coalesced request may still over-read it). Message kind,
version, legacy-compression state, complete node/buffer metadata,
layout-derived buffer minima, child extents, and fixed or fully-covered
null-count contracts for planned subtrees, planned codec, and required
dictionary presence are validated before any planned body range is requested.
Skipped buffer contents remain unvalidated by design.

---

## 3. Per-batch statistics (the pruning fuel)

Arrow's format has no per-batch statistics on the wire; the ecosystem's
"statistics schema" standardizes the **value layout** for exchanging
statistics as Arrow data, but placement in IPC files is not (yet)
standardized upstream. This convention is deliberately conservative:

- **Placement (our convention, upgradeable)**: one schema-level custom
  metadata key, e.g. `JuliaArrow:batch_statistics.v1`, carried in the
  **Footer's** schema copy so the tail fetch alone powers pruning.
- **Value layout**: follow the official statistics-schema array layout,
  serialized as one embedded IPC stream (statistics ARE Arrow data); per
  record batch × flattened RecordBatch FieldNode index: min, max,
  null_count, distinct_count-if-known. Top-level fields after nested fields
  therefore do not use their top-level ordinal as the `column` value.
  Using the official layout keeps us convention-compatible if upstream
  standardizes placement later — we then emit both keys for a deprecation
  cycle and read either.
- Writer: `withstatistics` / `statsfile` eagerly compute the embedded
  stream for already-encoded batches. An opt-in `statistics=true` keyword
  computing the same fold state during encode is not implemented; file
  format only. An append path would have to recompute or drop
  — dropping with a warning is the honest v1.
- Reader: prune under `Cmp`/`In`/`IsNull` (and `StrPred` prefix ranges for
  `startswith`) with one-sided may-contain logic — a batch survives unless
  the predicate is provably false for ALL rows; the filter always stays in
  the residual (pruning is inexact by design). Missing or MALFORMED
  statistics degrade to "no pruning", never to an error. The embedded
  stream and Base64 output share the enclosing scan allocation budget;
  exhausting that cumulative caller limit remains a scan error instead of
  being mistaken for malformed optional metadata.
  Float comparisons use the predicate's IEEE operators; any NaN disables
  bounds, and signed zero is not ordered with `isless`. Dictionary folds
  count null pool results as logical nulls.
- **Trust model, stated plainly**: statistics are
  trusted-for-completeness, exactly like Parquet row-group stats. The
  residual re-filter protects one direction only — batches kept by lying
  stats still filter row-exactly. The other direction has no net: stats
  that under-report a range cause false EXCLUSION. Excluded batches are not
  decoded and cause no dedicated metadata/body range request, so their
  qualifying rows are silently lost. Tail/coalescing over-read does not restore
  them. Wide (conservative) lies cost pruning, never correctness; narrow lies
  lose rows. The acceptance battery pins all three behaviors.

---

## 4. The trim contract (staying on the radar, explicitly)

**Trimmability is a standing production gate, not an aspiration.** The
`--trim=safe` harness (0 errors / 0 warnings / binary exit 0) compiles
`ArrowCore` plus its value-domain and typed-value workloads and the C-data
seams. It does **not** yet compile a scan-and-materialize app, so §1–§3 are
designed for trim but not yet gated by it. The rules in `core-README.md`
("Trim-compile support") constrain their form:

- `Tables.Scan` is trim-aligned by its own charter (no `Function` fields;
  closed algebra). The evaluator uses the same closed-set `isa` ladder
  pattern as `layoutspec_of`; `OpNode` rejection keeps the set closed.
  `bind` is plain data → plain data.
- The range planner is arithmetic over `Int64`s; `RangedSource{F}` is
  concrete in any trimmed app. No dynamic registry, no abstract-typed
  fields on the hot path.
- **Two-tier public API (mirroring the CSV rewrite)**: the runtime-tagged
  core is inherently trim-safe — descriptors are values, accessors use
  literal load widths, struct scalars are `Vector{Pair{String,Any}}`. So:
  - **Tier 1 (trim target)**: the value-domain entry points —
    open/scan/materialize returning value-domain data, plus C-data/stream
    interop and the typed `getvalue(::Type{T}, …)`/`materialize(::Type{T}, …)`
    path. A harness compiling a scan-and-materialize app at 0/0/exit-0,
    kept permanently in CI, is what would make the scan half guaranteed.
  - **Tier 2 (dynamic, ergonomic)**: the typed facade (`Arrow.Table`
    property access, NamedTuple rows) — explicitly NOT trim-guaranteed,
    the same split the CSV rewrite made.
  - **The known-schema bridge**: `Scan`'s `ref => Type` overrides ARE the
    known-schema declaration. In a trimmed app, a scan with concrete type
    pins can drive the typed-column path whose element types are statically
    known (`Vector{Int64}`, `Vector{Union{Missing,Float64}}`, …) through
    closed-width branches — "provide a known schema and get typed columns,
    trimmed" falls out of the same plain-data request, no second schema
    surface needed.

---

## 5. Status

Implemented (`src/scan.jl`, `src/table.jl`):

- **Scan pushdown**: `skipfield!`, `Tables.apply(::ArrowFile, scan)` with
  Stage-A semantics, exact limit/offset batch skipping, resolved residual
  selections, zero-field scans, and the differential battery with
  corruption-backed never-decoded proofs. `Arrow.Table(source; scan=…)`
  routes through it on file-format and ranged inputs; stream-format inputs
  scan post-decode with identical results.
- **RangedSource**: the `RangedSource{F}` contract, the `RangedFile` fetch
  protocol, the coalescing planner, `SparseBody` decode, and
  counting-source proofs (zero planned body ranges for skipped columns,
  window-excluded batches, and unneeded dictionary bodies, with exact
  request-log checks under the fixtures' tail/coalescing settings).
- **Statistics**: `withstatistics`/`statsfile` fold the official statistics
  value layout into `JuliaArrow:batch_statistics.v1` (footer schema
  metadata, base64-wrapped IPC stream, one statistics batch per data batch,
  serialized through this writer); `_maypass` may-contain pruning is wired
  into both applies (ranged pruning happens before the block-metadata pass,
  so pruned batches cause no dedicated metadata/body request; configured
  tail/coalescing may over-read them); acceptance pins exactness,
  degradation, and both lie directions.

Not implemented: a CloudStore/HTTP transport extension (the fetcher
contract is the extension point), Stage B's exact facade `apply`, an
encode-time `statistics=true` writer keyword, upstream-placement tracking
for statistics, and the scan-and-materialize trim harness. Scan pushdown
depends on Tables.jl's `jq/scan` branch until that API is released.
