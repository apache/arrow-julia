# Design: Tables.Scan pushdown, cloud byte-range reads, and the trim contract

Status: PROPOSAL (Aug 14, 2026) — extends the redesign report's §9 IPC adapter
and §14 decision rules. Nothing here is implemented yet except where noted as
already existing in the prove-out. The three pieces are designed together
because they share one mechanism: **a bound column set drives both what gets
decoded and what gets fetched, and every value involved is plain data the
trim verifier can see through.**

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
| `select` | decode only (selected ∪ filter-referenced) columns: a registry-driven `skipfield!` advances the node/buffer cursor past unselected fields without slicing, validating, or materializing them. Nested subtrees skip with their parent; unselected dictionary columns skip their dictionary batches (file format: never even framed). | exact as IO/decode reduction (see below for who projects) |
| `limit`/`offset` | `RecordBatch.length` is wire metadata: whole batches before `offset` and after `offset+limit` are never decoded (file format: never fetched). Row counts are known without touching a single body byte. | exact when no filter; poisoned by any filter per the contract |
| `filter` | two tiers: (a) **statistics pruning** — per-batch min/max/null-count, when present (§3 of this doc), prune batches that cannot satisfy the predicate; (b) **mask at materialization** — evaluate the predicate over decoded columns through Core accessors and apply the mask when building output columns. | (a) inexact — filter stays in residual; (b) exact — enables limit pushdown with filters |
| `types` (`ref => T`) | left in the residual for `finish`'s elementwise convert. Arrow's schema is source-fixed; an override is a conversion request, not a parse seed (unlike CSV). Exception: see §4 — in trim mode the overrides double as the known-schema pin. | residual |

### The `apply` shape — two stages

**Stage A (adapter-level, near-term).** `Tables.apply` on the file/stream
handles does **IO-and-decode reduction with a full residual**:

    apply(f, scan) =
      bind against schema names →
      decode set = selected ∪ filtercols (source order, source names) →
      batch set = limit/offset window (when filter === nothing),
                  ∩ stats-surviving batches (when stats present) →
      return (table over decode set, residual)

where the residual is the original scan minus `limit`/`offset` when those
were consumed. Critically, when the filter references unselected columns the
returned table **keeps them under source names and leaves `select` in the
residual** — `finish` then filters, projects, renames, and converts. This is
the only correct composition: if the adapter consumed `select` while leaving
`filter` in the residual, `finish` could not evaluate predicates over
already-dropped columns. Simple, correct, and captures the dominant win
(unselected columns cost zero decode — and with §2, zero bytes).

Two refinements the P1 prove-out's differential tests forced (both now
implemented in `examples/scan_ranges.jl`):

- **The residual selection must be RESOLVED, not passed through.** `Not` and
  `Regex` select items re-bound against the reduced output table are wrong
  (`Not(:x)`'s excluded name no longer exists; a regex can over-match a
  filter-only column). The residual carries the bound columns as concrete
  source-name items with their renames and type overrides attached.
- **Stage A needs no row-level predicate evaluator.** The filter always
  stays in the residual, so `Tables.finish`/`filtermask` do row evaluation;
  Arrow-side predicate logic first appears as the *interval* ladder for
  statistics pruning (§3). Stream handles keep the default no-push `apply`
  — the eager prove-out stream has already decoded by the time `apply`
  runs; stream pushdown belongs to the production incremental framer.

**Stage B (facade-level, ViewPlan era).** The facade's `apply` consumes
everything exactly: per-column masks evaluated through Core accessors (no
materialization of excluded rows), projection/renames applied at ViewPlan
construction, `limit`/`offset` composed with exact masks. Residual: empty,
CSV-kernel style. Stage B subsumes Stage A; Stage A ships first because it
needs no facade.

Predicate evaluation in both stages is a **closed `isa` ladder over the
closed `ScanExpr` set**, walking Core accessors (`isvalid_at` + `_value`)
column-at-a-time. `OpNode` is rejected (the algebra's own documented rule:
only sources that recognize a node may consume it; ours recognizes none).
No closures, no `Function` fields — the evaluator is trim-clean by the same
construction as the layout registry (§4).

Dictionary columns prune cheaply under equality/membership predicates: test
the predicate against the **pool** once, then compare index sets — worth
noting in the design since the snapshot model makes pool identity stable per
batch run.

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

1. **Tail fetch** (one range request): last `tailbytes` (default 64 KiB).
   Covers footer-length + magic + the whole Footer in almost every real
   file; if `footerlen + 10 > tailbytes`, one exact follow-up fetch.
   → schema, Block indexes, (§3) statistics — everything pruning needs.
2. **Prune** batches by scan (`limit`/`offset` windows, statistics) —
   zero additional fetches.
3. **Block metadata fetches**: `(offset, metaDataLength)` per surviving
   batch, coalesced across adjacent batches. → per-buffer tables.
4. **Buffer-range plan**: bound column set → buffer index set (subtree-
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

Request-count model (what actually matters against cloud latency): `1` tail
+ `⌈surviving-batch metadata spans after coalescing⌉` + `⌈coalesced body
ranges⌉` — for a 40-column file reading 3 columns of every batch, typically
2 + one body request per batch group, moving ~`3/40` of the body bytes plus
metadata. With statistics pruning, batches drop out entirely at step 2.

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

- `readfile(::RangedSource; scan=...)` is the entry point; the existing
  whole-buffer and `mmapregion` paths become trivial `RangedSource`s
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

---

## 3. Per-batch statistics (the pruning fuel)

Arrow's format has no per-batch statistics on the wire; the ecosystem's
"statistics schema" standardizes the **value layout** for exchanging
statistics as Arrow data, but placement in IPC files is not (yet)
standardized upstream. Proposal, kept deliberately conservative:

- **Placement (our convention, upgradeable)**: one schema-level custom
  metadata key, e.g. `JuliaArrow:batch_statistics.v1`, carried in the
  **Footer's** schema copy so the tail fetch alone powers pruning.
- **Value layout**: follow the official statistics-schema array layout,
  serialized as one embedded IPC stream (statistics ARE Arrow data); per
  record batch × per column: min, max, null_count, distinct_count-if-known.
  Using the official layout keeps us convention-compatible if upstream
  standardizes placement later — we then emit both keys for a deprecation
  cycle and read either.
- Writer: opt-in kwarg (`statistics=true`), computed streaming during
  encode (min/max/nullcount are cheap fold state per column); file format
  only. Append (§ report) must recompute or drop — dropping with a warning
  is the honest v1.
- Reader: prune under `Cmp`/`In`/`IsNull` (and `StrPred` prefix ranges for
  `startswith`) with three-valued logic — a batch survives unless the
  predicate is provably false for ALL rows; the filter always stays in the
  residual (pruning is inexact by design). Missing/foreign/stale statistics
  degrade to "no pruning", never to wrong answers; `validate_semantic`
  still guards decoded data, so lying statistics can suppress rows only if
  they lie in the conservative direction — worth one adversarial test:
  stats that contradict decoded content must not corrupt exactness of the
  residual pipeline (they cannot, because the residual filter re-runs).

---

## 4. The trim contract (staying on the radar, explicitly)

Reaffirmed: **trimmability is a standing gate, not an aspiration.** The
prove-out's `--trim=safe` gate (0 errors / 0 warnings / binary exit 0) has
stayed green through every round; the rules that keep it green are in the
README ("Trim-compile support") and they bind this design too:

- `Tables.Scan` is already trim-aligned by its own charter (no `Function`
  fields; closed algebra). Our evaluator adds the same closed-set `isa`
  ladder pattern as `layoutspec_of`; `OpNode` rejection keeps the set
  closed. `bind` is plain data → plain data.
- The range planner is arithmetic over `Int64`s; `RangedSource{F}` is
  concrete in any trimmed app. No dynamic registry, no abstract-typed
  fields on the hot path.
- **Two-tier public API (mirroring the CSV rewrite)**: the runtime-tagged
  core is inherently trim-safe — descriptors are values, accessors use
  literal load widths, struct scalars are `Vector{Pair{String,Any}}`. So:
  - **Tier 1 (trimmable, guaranteed)**: the value-domain entry points —
    open/scan/materialize returning value-domain data, plus C-data/stream
    interop. Gate: a trim harness compiles a scan-and-materialize app at
    0/0/exit-0, permanently in CI.
  - **Tier 2 (dynamic, ergonomic)**: the typed facade (`Arrow.Table`
    property access, NamedTuple rows, ViewPlan specialization) — explicitly
    NOT trim-guaranteed, same split the CSV rewrite made.
  - **The known-schema bridge**: `Scan`'s `ref => Type` overrides ARE the
    known-schema declaration. In a trimmed app, a scan with concrete type
    pins can drive a typed-column path whose element types are statically
    known (`Vector{Int64}`, `Vector{Union{Missing,Float64}}`, …) through
    closed-width branches — "provide a known schema and get typed columns,
    trimmed" falls out of the same plain-data request, no second schema
    surface needed.

---

## 5. Phasing (each phase codex-reviewed per the standing protocol)

- **P1 — Scan on the prove-out** (small): `skipfield!`, `Tables.apply` for
  `ArrowFile`/`readstream` (Stage A semantics), limit/offset batch
  skipping, closed-ladder filter evaluator + differential tests against
  `Tables.finish`-only execution.
- **P2 — RangedSource** (medium): the fetcher contract, planner, sparse
  region assembly in `readfile`, counting-source differential tests
  (bytes/requests), mmap/vector adapters. Proves the fetch-count model.
- **P3 — statistics** (small-medium): writer fold + footer metadata key,
  reader pruning, adversarial stats tests. Unlocks tail-fetch-only pruning.
- **P4 (production)**: `ArrowCloudStoreExt`, Stage B facade `apply`,
  upstream-placement tracking for statistics.

Open decisions before P1 starts: (a) Stage-A residual shape as specified
(full residual, source names) — sign-off; (b) `RangedSource` functor vs
abstract type; (c) statistics placement key + whether P3 lands in the
prove-out or waits for the real package; (d) whether `Tables.jl#jq/scan`
is API-stable enough to build against now, or P1 should pin a commit.
