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

# Design: Tables.Scan pushdown, cloud byte-range reads, and the trim contract

Implemented in `src/scan.jl` and `src/source.jl` and exposed through the
facade (`Arrow.Table(source; scan=…)` over an `AbstractArrowSource`); §5
lists what is and is not built. The three pieces share one mechanism: **a bound column set drives
both what gets decoded and what gets fetched, and every request value is
plain data intended to remain visible to the trim verifier.** Section 4
separates that design intent from what the trim harness actually compiles.

---

## 1. Tables.Scan support

`Tables.Scan` (from the Tables.jl revision pinned in `Project.toml` until its
first release) is a plain-data scan request —
select/rename/type items, a closed predicate algebra (`Cmp`/`In`/`IsNull`/
`StrPred`/`And`/`Or`/`Not`, with `OpNode` as the growth channel), `limit`/
`offset`. A source accepts a `Scan` as a keyword and pushes down what it can
while materializing. Arrow resolves each request once against the source
schema. Direct handles pass the bound request to the storage-only
`_applyscan(handle, bound)`. The public facade passes the complete plan to
`_applyfacadescan`, which owns route-aware materialization, public conversion,
and result wrapping as one operation. Key contract points this design leans
on:

- a pushed result must equal the executor's over the same scan, row for
  row (the differential battery pins this);
- `limit`/`offset` may be consumed **only** over exactly filtered rows;
- no `Function` values in `Scan` or predicate data — the request algebra is
  closed and value-only.

### What Arrow can push, by axis

| Axis | Mechanism | Exactness |
|---|---|---|
| `select` | decode only (selected ∪ filter-referenced) columns: a registry-driven `skipfield!` advances the node/buffer cursor past unselected fields without body slicing, content validation, or materialization. Complete node/buffer metadata is still validated first. Nested subtrees skip with their parent; no body range is requested for an unselected dictionary column. | exact as IO/decode reduction (see below for who projects) |
| `limit`/`offset` | Without a filter `RecordBatch.length` is wire metadata: whole batches before `offset` and after `offset+limit` are never decoded (ranged reads still fetch candidate RecordBatch metadata because Footer Blocks have no row counts, but request no body range for excluded batches). With a filter the window composes over the qualifying rows of each decoded batch and decoding stops once it is full. Tail reads and configured coalescing may physically over-read otherwise unrequested bytes. | exact |
| `filter` | two tiers: (a) **statistics pruning** — per-batch min/max/null-count, when present (§3 of this doc), prune batches that cannot satisfy the predicate; (b) **mask at materialization** — evaluate the predicate over each batch's decoded columns with the generic evaluator (`Tables.filtermask`) and keep only the qualifying rows when building output columns. | (a) inexact — a pruned batch is provably empty; (b) exact — enables limit/offset pushdown with filters |
| `types` (`ref => T`) | An elementwise conversion request, not a parse seed. Direct handle scans apply it to storage-domain columns. The facade applies it after ArrowTypes and native public conversion. See §4 for the known-schema role in trim mode. | exact |

### The pushdown shape

`_ScanPlan` resolves a facade request once. It keeps the public-domain bound
plan and, when every filter literal has an exact storage representation, one
lowered storage-domain bound plan. `_runboundscan` is the private batch kernel.
`_applyscan` closes it over ordinary storage materialization for direct
handles. File and ranged `_applyfacadescan` methods close it over facade
routing. The stream method routes decoded columns and uses `_executeplan`.
Every facade method consumes its private route markers before returning:

    plan(scan, fields) =
      resolve once against schema names →
      lower filter literals exactly for storage, or mark public fallback →
      strip public type overrides from the storage plan

    Temporal `In` values lower for Tuple and Array containers. Set members
    lower only from the field's canonical public type, preserving `isequal`
    and hashing. Custom membership objects stay intact and force
    public-domain fallback.

Tables.jl does not yet expose execution for an existing `BoundScan`.
`_executeplan` is the narrow local adapter for public and stream fallback. It
uses Tables.jl's predicate and allocation seams and is pinned to the generic
executor by the differential battery; it avoids reconstructing and resolving a
second `Scan`.

    _runboundscan(f, bound, materializecolumn) =
      decode set = selected ∪ filtercols (source order, source names) →
      batch set = limit/offset window from wire row counts (no filter),
                  ∩ stats-surviving batches (filter present, stats present) →
      per decoded batch (`_ScanSink`):
        materialize the decode set through the operation's fixed policy →
        rows = filter ? qualifying rows after this batch's share of
                        offset/limit (saturating, the executor's rule)
                      : the metadata window's rows →
        keep each selected column's rows, in selection order, under its
        output name; stop decoding once the window is full →
      return table over the selection

    _applyscan(handle, bound) =
      _runboundscan(handle, bound, storage materializer)

    _applyfacadescan(file-or-ranged-source, plan) =
      assert plan.storage has no public type overrides →
      _runboundscan(source, plan.storage, route-aware materializer) →
      lift ArrowTypes routes and native public values →
      apply plan.public type overrides and wrap the public Table

    _applyfacadescan(stream, plan) =
      assert plan.storage has no public type overrides →
      materialize route-aware columns after stream decode →
      _executeplan(columns, plan.storage) →
      lift routes, apply plan.public overrides, and wrap the public Table

The filter is evaluated by the generic evaluator (`Tables.filtermask`) over
the batch's decoded columns, so Arrow's pushdown and the executor share one
three-valued semantics by construction (a `missing` predicate excludes the
row). Unselected columns cost zero decode and add zero planned body bytes.
Tail reads and coalescing may still over-read them under §2's explicit
policy.

Properties the composition holds (all implemented in `src/scan.jl`):

- **Positional filter references resolve to source names** before
  evaluation (`Tables.resolve` normalizes them): a bound `col(3)` means
  source column 3, and the decode-set table the evaluator sees carries
  source names.
- **Wire row counts are trusted only after metadata validation.** Before a
  `RecordBatch.length` drives a window, it is range-checked and matched to
  every top-level FieldNode length. Exact node/buffer counts and buffer
  geometry are also checked from metadata alone. Aggregate scan row counts
  must fit Tables' `Int` row-count API: a planned result through
  `typemax(Int)` is accepted and a larger one rejected. An offset-only
  window represents `limit=nothing` explicitly; it does not use a finite
  sentinel that can omit later batches.
- **One scan has one allocation budget.** Standalone lazy `file[i]` calls
  retain their documented per-call budgets. A scan that visits many batches
  shares one budget and codec state across footer work, metadata, fetched
  range payloads, decompression, and any full-object public-domain fallback.
- **Windows saturate.** `offset`/`limit` compose with `min` arithmetic over
  row counts, as the executor does, so `offset + limit` never overflows.
- **Type overrides run once in the correct value domain.** Direct handle
  scans apply them to storage-domain columns. The facade keeps them out of
  its storage plan and applies them after public conversion.
- **Private Union routes stay local.** Direct `_applyscan` cannot select the
  ArrowTypes route-aware materializer. `_applyfacadescan` creates and consumes
  identical-storage Union child markers inside one operation, before a result
  crosses back into `table.jl`.
- **Zero-field sources** consume filter and window from row counts alone
  (`_zerofieldwindow`): a row-invariant predicate evaluates once, and no
  per-row mask is allocated from an untrusted row count.
- **Advisory nullability does not weaken the skip boundary.** The reader
  admits nulls under `nullable=false`; `validate_full` rejects them. If such
  a null exists only in an unread batch, it does not widen the selected
  result's concrete Julia vector type. Row values and conforming schemas
  still match the generic executor exactly.

The statistics fold resolves dictionary indices through the pool before it
computes logical null/min/max values; the row evaluator compares decoded
values. Stream-format sources are scanned by the facade after decode — the
eager stream reader has already decoded by then; stream pushdown would need
an incremental framer.

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

1. **Tail fetch** (one range request, cached on the handle): the last
   `tailbytes` (default 64 KiB). Its trailing magic decides file vs stream
   format (a stream object is read whole instead), and it covers
   footer-length + magic + the whole Footer in almost every real file; if
   `footerlen + 10 > tailbytes`, one exact follow-up fetch (also cached).
   → schema, Block indexes, (§3) statistics — everything pruning needs.
   The leading magic is not fetched: the Footer is the sole authority.
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
5. **Body fetches** — the selected dictionary bodies and the selected record
   buffers in ONE round: each coalesced range lands in its own owned heap
   region; the dictionaries decode first from the shared spans. Decode
   resolves each declared buffer `(offset, len)` to its containing fetched
   range and subslices — the message-body-authority
   invariant becomes *"every buffer must fall inside a fetched range that
   was itself derived from the verified buffer table"*: same trust story,
   sparse backing.

Request-count model (what actually matters against cloud latency): `1` tail
(plus one exact Footer follow-up when the tail is too small)
+ `⌈candidate metadata spans after coalescing⌉` + `⌈coalesced body ranges⌉`
— three rounds, each one wall-clock round trip when the source issues a
round's ranges concurrently.
For a 40-column file reading 3 columns of every batch, this moves roughly
`3/40` of the body bytes plus metadata. Statistics-pruned batches contribute
no requested metadata/body range. Coalescing is an explicit over-read policy,
so a requested span may cross otherwise unneeded bytes when the configured
gap permits it.

### The interface (no HTTP/CloudStore deps in Arrow)

Arrow defines a minimal source contract (`src/source.jl`) and owns the
planner; transports live in extensions:

    abstract type AbstractArrowSource end
    sourcelength(src)::Integer                  # total object length, known up front
    readrange(src, offset, len)::Vector{UInt8}  # one exact range, 0-based offset
    concurrentreads(src)::Int                   # default 1
        # Arrow reads a round's planned ranges through readrange with a
        # worker pool of that size, storing every result by request index —
        # a source's completion order can never permute payloads, and the
        # reads in flight are bounded whatever the span count.

- The entry point is `Arrow.Table(src; scan=…)`, which builds the internal
  `SourceFile(src; limits, tailbytes, coalesce_gap)` handle and runs
  `Tables.scan(sf, scan)`; the source's length is read once, at handle
  construction, and the tail once per handle. `Arrow.Table(src)` without a
  scan, with a scan that cannot be pushed down, over a zero-field file, or
  over a stream-format object reads the object whole (one request beyond
  the cached tail), as does `Arrow.Stream(src)`.
- The abstract type is a dynamic call under `--trim` for a source type the
  trimmed app never mentions; an app that names its concrete source type
  resolves statically. The planner itself is arithmetic over `Int64`s.
- Extension: `ext/ArrowCloudStoreExt.jl` (loaded with CloudStore.jl) makes
  a `CloudStore.Object` a source — its known `size` is the length, one
  range is one HTTP `Range` GET pinned to the object's ETag with
  `If-Match`, and `concurrentreads` is 16 — and adds `Arrow.Table(::CloudStore.Object; …)` and
  `Arrow.Stream(::CloudStore.Object; …)`. An HTTP transport is the same two
  methods. Zero new hard deps.
- The differential test: sparse fetch ≡ whole-file read, plus
  request-count/byte-count/range assertions on a counting test source
  (`test/scan_battery.jl`), and the CloudStore extension end to end against
  a local S3-compatible server (`test/cloudstore_tests.jl`).
- Explicitly out of scope v1, documented: caching/prefetch policy beyond
  coalescing, retries (the source's job), writers over ranges, stream
  format (read whole), mutation detection (ETag pinning is the extension's
  concern).

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
  record batch × flattened RecordBatch FieldNode index: row_count,
  null_count, min, max (the `ARROW:*:exact` keys). Top-level fields after
  nested fields
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
  the predicate is provably false for ALL rows; the exact row filter always
  runs after pruning (pruning is inexact by design). Missing or MALFORMED
  statistics degrade to "no pruning", never to an error. The embedded
  stream and Base64 output share the enclosing scan allocation budget;
  exhausting that cumulative caller limit remains a scan error instead of
  being mistaken for malformed optional metadata.
  Float comparisons use the predicate's IEEE operators; any NaN disables
  bounds, and signed zero is not ordered with `isless`. Dictionary folds
  count null pool results as logical nulls.
- **Trust model, stated plainly**: statistics are
  trusted-for-completeness, exactly like Parquet row-group stats. The exact
  row filter protects one direction only — batches kept by lying stats still
  filter row-exactly. The other direction has no net: stats
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
  `resolve` is plain data → plain data.
- The range planner is arithmetic over `Int64`s; a trimmed app that names
  its concrete `AbstractArrowSource` type resolves the source calls
  statically. No dynamic registry on the hot path.
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

- **Scan pushdown**: `skipfield!`, one `_ScanPlan` compilation per facade
  request, storage-only `_applyscan(::ArrowFile, bound)`, and the closed
  file/ranged `_applyfacadescan(source, plan)` route consuming the bound scan
  exactly through `_ScanSink` (per-batch filter evaluation, composed
  limit/offset with early stop, selection, renames, direct-handle type
  overrides at column construction, and facade conversion before return),
  zero-field scans, and the
  differential battery with corruption-backed never-decoded proofs. `Arrow.Table(source; scan=…)`
  routes through it on file-format and ranged inputs; stream-format inputs
  route inside their facade operation and scan post-decode with identical
  results.
- **Byte-range sources**: the `AbstractArrowSource` contract, the
  `SourceFile` fetch protocol, the coalescing planner, `SparseBody` decode,
  the CloudStore.jl extension, and counting-source proofs (zero planned body ranges for skipped columns,
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

Not implemented: an HTTP transport extension (the source contract is the
extension point; the CloudStore extension is the model), an encode-time
`statistics=true` writer keyword, upstream-placement tracking
for statistics, and the scan-and-materialize trim harness. Scan pushdown
depends on the pinned Tables.jl development revision until that API is
released.
