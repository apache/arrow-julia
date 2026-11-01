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
facade (`Arrow.Table(source; scan=…)`, the `select`/`drop`/`filter`/`limit`/
`offset` keywords, and `Arrow.Stream`); §4 lists what is and is not built.
The pieces share one mechanism: **a bound column set drives both what gets
decoded and what gets fetched, and every request value is plain data intended
to remain visible to the trim verifier.** Section 3 separates that design
intent from what the trim harness actually compiles.

---

## 1. Tables.Scan support

`Tables.Scan` (Tables.jl 1.14) is a plain-data scan request: select, rename,
and type items, a closed predicate algebra (`Cmp`/`In`/`IsNull`/`StrPred`/
`And`/`Or`/`Not`, with `OpNode` as the growth channel), and `limit`/`offset`.
Arrow resolves each request once against the source schema with
`Tables.resolve` (`_ScanPlan`) and executes the bound request itself. Key
contract points this design leans on:

- a pushed result must equal the generic executor's over the same scan, row
  for row (the differential battery pins this for every source kind);
- `limit`/`offset` may be consumed **only** over exactly filtered rows;
- no `Function` values in `Scan` or predicate data: the request algebra is
  closed and value-only.

### One planner, one executor

Every read goes through one planner and one executor, whatever the source:
a memory-mapped file, a byte vector, an `IO` read whole, a byte-range source
such as a cloud object, or stream-format input. A whole read is the scan that
selects every column.

    plan(scan, fields) = resolve once against the schema names →
      check literal kinds against their columns (`_checkliterals`) →
      give zone-free time literals a UTC column's zone (`_zonedbound`)

    _scanfile(src, footer, plan) =
      one pass over every Block's metadata (a region serves it in place; a
      byte-range source fetches it in coalesced ranges that also cover the
      start of each body) →
      row counts → the batch window when there is no filter →
      the decode set's dictionaries, in footer order →
      a decode plan per batch in the window → _runscan

    _scanstream(stream) =
      frame the messages, apply dictionaries in stream order, and plan each
      record batch against the dictionaries in effect for it → _runscan

    _runscan(records, plan) =
      no filter and no window: decode the decode set of every batch on up to
      `ntasks` tasks and build the columns a full read builds;
      otherwise, per batch: decode the filter columns, evaluate the filter on
      their public values (`Tables.filtermask`) in chunks of rows, then decode
      the other selected columns only if a row qualifies, and keep copies of
      the qualifying rows; without `limit` or `offset` batches run on up to
      `ntasks` tasks, with a window they run in order and stop once it is full

`Arrow.Stream` runs the same executor over one batch at a time.

A region (a mapped file or a byte vector) is its own span provider: one span
over the whole region serves every range the planner names, without a copy.
A byte-range source fetches each round's ranges through `readrange`,
coalesced, and a fetched range is referenced in place by the columns that
read it. The planner is the same for both; only the fetch differs.

### What Arrow pushes, by axis

| Axis | Mechanism |
|---|---|
| `select` | only the decode set (selected ∪ filter columns) is decoded; `skipfield!` advances the node and buffer cursor past every other field with each buffer-table invariant checked, without slicing, decompressing, or validating its body; over a byte-range source no body range is planned for it |
| `limit`/`offset` | without a filter, `RecordBatch.length` is wire metadata: batches outside the window are never decoded or fetched; with a filter, the window composes over the qualifying rows, in order, and the scan stops once it is full |
| `filter` | evaluated per batch over the public values of the filter columns, the values a full read returns; the other selected columns are decoded (and over a byte-range source, fetched) only for batches with a qualifying row |
| `types` (`ref => T`) | an elementwise `convert` after the scan, as the generic executor applies it. Two overrides read stored values instead: a time of day as a `Dates.Period`, which also reads times outside a day, and a `JuliaLang.Symbol` column as strings, which creates no `Symbol` |

The filter is evaluated by the generic evaluator over the batch's public
values, so pushdown and the executor share one three-valued semantics by
construction (a `missing` predicate excludes the row), and every predicate a
user can write pushes down: no literal needs a stored-value form.

Column types do not depend on the source kind or on a filter. A read that
keeps every row of the batches it reads builds the columns a full read
builds: views of the source, `ChainedVector`s across batches, and string
columns over the source's bytes. A read with a filter or a window keeps
copies of its rows: `Vector`s for flat and nested values, string and binary
columns that hold just their values' bytes, and dictionary columns that keep
their pool, so a filtered result does not keep a batch's decompressed bytes
or the mapping alive for a few of its rows. An empty result column has the
type a read of no batches gives it.

Properties the composition holds:

- **Positional filter references resolve to source names** before
  evaluation (`Tables.resolve` normalizes them).
- **Wire row counts are trusted only after metadata validation.** Before a
  `RecordBatch.length` drives a window, it is range-checked and matched to
  every top-level FieldNode length. The rows of all batches together must fit
  Tables' `Int` row count, whatever the window keeps.
- **One scan has one allocation budget,** shared by footer work, metadata,
  fetched ranges, decompression, and column building.
- **Windows saturate.** `offset`/`limit` compose with `min` arithmetic over
  row counts, so `offset + limit` never overflows.
- **Zero-field sources** read their row counts from metadata; a filter that
  reads no column evaluates once per batch, and no per-row mask is allocated
  from an untrusted row count.
- **A scan checks only what it reads.** Every Block's metadata is verified,
  but the body of a dictionary outside the decode set, or of a record batch
  outside the window, is never read, so damage there does not fail the scan.

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
   → schema and Block indexes. The leading magic is not fetched: the Footer
   is the sole authority.
2. **Block metadata fetches**: the metadata of every dictionary and record
   batch Block, each with the start of its body (all of a body up to four
   `gapbytes`, else its first `gapbytes`), coalesced across nearby spans. A
   scan with a row window reads metadata alone. RecordBatch row counts are
   here, not in the Footer, so the window is chosen after this pass.
3. **Window and buffer-range plan**: row counts choose the batch window when
   there is no filter; the decode set then maps to a buffer index set
   (subtree-inclusive; dictionary Blocks for the decode set's dictionaries,
   including those a dictionary's values use) → byte ranges → **coalesce**
   ranges with gaps below `gapbytes` (default 256 KiB — a gap fetch is usually
   cheaper than a request round-trip).
4. **Body fetches**: the decode set's dictionary bodies and the record bodies
   the executor reads first (every decode-set column without a filter; the
   filter columns with one) in one round. A filter then fetches the other
   selected columns of the batches that keep rows in one more round; a
   filtered `limit` fetches batch by batch and stops when it is full, freeing
   each batch's bytes once it is read. Each coalesced range lands in its own
   owned heap region; decode resolves each declared buffer `(offset, len)` to
   its containing fetched range and subslices it: *every buffer must fall
   inside a fetched range that was itself derived from the verified buffer
   table*, the message-body authority invariant with sparse backing.

Request-count model (what matters against cloud latency): `1` tail (plus one
exact Footer follow-up when the tail is too small) + `⌈metadata spans after
coalescing⌉` + `⌈coalesced body ranges⌉`. A read without a filter makes
three sequential rounds, a filtered read four. The source can issue a round's
ranges concurrently. For a 40-column file reading 3 columns of every batch,
this moves roughly `3/40` of the body bytes plus metadata. Coalescing is an
explicit over-read policy: a requested span may cross unneeded bytes when the
configured gap permits it.

`Arrow.Stream` over a byte-range file source reads the footer and the
dictionaries when it opens. A batch that decodes every column reads its whole
block, and the blocks after it while they fit 8 MiB, with one window fetched
ahead. A batch that decodes some columns reads its metadata with the start of
its body, then the ranges of its decode set it lacks.

### The interface (no HTTP/CloudStore deps in Arrow)

Arrow defines a minimal source contract (`src/source.jl`) and owns the
planner; transports live in extensions:

    abstract type AbstractArrowSource end
    sourcelength(src)::Integer                  # total object length, known up front
    readrange(src, offset, len)::Vector{UInt8}  # one exact range, 0-based offset
    concurrentreads(src)::Int                   # default 1
        # Arrow reads a round's planned ranges through readrange with a
        # handle-wide worker pool, capped by Limits.max_concurrent_reads,
        # storing every result by request index —
        # a source's completion order can never permute payloads, and the
        # reads in flight are bounded whatever the span count.

- `Arrow.Table(src; …)` builds the internal `SourceFile(src; limits,
  tailbytes, gapbytes)` handle and reads through the planner; the source's
  length is read once, at handle construction, and the tail once per handle.
  `Arrow.Table(src)` without a scan, or over a stream-format object, reads
  the object whole in 8 MiB ranges (one request beyond the cached tail for a
  source that reads one range at a time).
- One `SourceFile` samples `concurrentreads` once and shares one semaphore
  across its operations. Separate handles are independent; a transport that
  requires one wider cap enforces it in `readrange`.
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
  (`test/scan_battery.jl`, `test/ranged_scan_tests.jl`), and the CloudStore
  extension end to end against a local S3-compatible server
  (`test/cloudstore_tests.jl`).
- Explicitly out of scope, documented: caching/prefetch policy beyond
  coalescing and `Arrow.Stream`'s read-ahead, retries (the source's job),
  writers over ranges, stream format (read whole), mutation detection (ETag
  pinning is the extension's concern).

The ranged reader deliberately uses the Footer schema as its sole schema
authority. It does not parse or cross-check the leading schema message or
optional EOS marker, although a head, tail, or coalesced request can physically
over-read those or other unrequested bytes. The complete Footer Block index is
bounded and checked for overlap. Required features and message limits are
global. Per-record metadata/body/buffer limits stay lazy like `ArrowFile`:
every dictionary and record block's metadata is checked. Message kind,
version, legacy-compression state, complete node/buffer metadata,
layout-derived buffer minima, child extents, and fixed or fully-covered
null-count contracts for planned subtrees, planned codec, and required
dictionary presence are validated before any planned body range is requested.
Skipped buffer contents remain unvalidated by design.

---

## 3. The trim contract (staying on the radar, explicitly)

**Trimmability is a standing production gate, not an aspiration.** The
`--trim=safe` harness (0 errors / 0 warnings / binary exit 0) compiles
`ArrowCore` plus its value-domain and typed-value workloads and the C-data
seams, plus a representative DataStrings workload. It does **not** yet compile
a scan-and-materialize app, so §1–§2 are designed for trim but not yet gated by
it. The rules in `core-README.md` ("Trim-compile support") constrain their form:

- `Tables.Scan` is trim-aligned by its own charter (no `Function` fields;
  closed algebra). The evaluator uses the same closed-set `isa` ladder
  pattern as `layoutspec_of`; `OpNode` rejection keeps the set closed.
  `resolve` is plain data → plain data.
- The range planner is arithmetic over `Int64`s; a trimmed app that names
  its concrete `AbstractArrowSource` type resolves the source calls
  statically. No dynamic registry on the hot path.
- **Two-tier public API**: the runtime-tagged
  core is inherently trim-safe — descriptors are values, accessors use
  literal load widths, struct scalars are `Vector{Pair{String,Any}}`. So:
  - **Tier 1 (trim target)**: the value-domain entry points —
    open/scan/materialize returning value-domain data, plus C-data/stream
    interop and the typed `getvalue(::Type{T}, …)`/`materialize(::Type{T}, …)`
    path. A harness compiling a scan-and-materialize app at 0/0/exit-0,
    kept permanently in CI, is what would make the scan half guaranteed.
  - **Tier 2 (dynamic, ergonomic)**: the typed facade (`Arrow.Table`
    property access, NamedTuple rows) — explicitly NOT trim-guaranteed.
  - **The known-schema bridge**: `Scan`'s `ref => Type` overrides ARE the
    known-schema declaration. In a trimmed app, a scan with concrete type
    pins can drive the typed-column path whose element types are statically
    known (`Vector{Int64}`, `Vector{Union{Missing,Float64}}`, …) through
    closed-width branches — "provide a known schema and get typed columns,
    trimmed" falls out of the same plain-data request, no second schema
    surface needed.

---

## 4. Status

Implemented (`src/scan.jl`, `src/table.jl`, `src/source.jl`):

- **One planner and one executor** for every source, as decided on Aug 19
  2026: memory-mapped files, byte vectors, an `IO` read whole, byte-range
  sources, and stream-format input. A region serves the planner's ranges in
  place, a byte-range source fetches them in coalesced rounds, and
  `Arrow.Stream` runs the executor over one batch at a time. `skipfield!`
  skips what the scan does not decode; `_ScanPlan` resolves a request once;
  `_runscan` reads filter columns first, evaluates filters on public values in
  chunks of rows on up to `ntasks` tasks, and decodes the other selected
  columns only for batches that keep rows.
- **Byte-range sources**: the `AbstractArrowSource` contract, the
  `SourceFile` fetch protocol, the coalescing planner, `SparseBody` decode,
  the CloudStore.jl extension, and counting-source proofs (zero planned body
  ranges for skipped columns, window-excluded batches, and unneeded
  dictionary bodies, with exact request-log checks under the fixtures'
  tail/coalescing settings).

Not implemented: an HTTP transport extension (the source contract is the
extension point; the CloudStore extension is the model) and the
scan-and-materialize trim harness. `Arrow.Stream` decodes every dictionary
batch of a file when it opens, including those of columns it does not
select.

Possible future work: per-batch statistics (row counts, null counts, minimum
and maximum values) in IPC footers, to prune batches a filter cannot match
before their metadata is fetched. The Arrow statistics schema standardizes
the value layout but not a placement in IPC files; without a placement other
implementations read, a private key would help only Julia readers of files
Julia wrote, and wrong statistics silently drop rows.
