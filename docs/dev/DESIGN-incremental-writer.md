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
--->

# Design: the Arrow 3.x incremental writer

Status: PROPOSAL (not yet reviewed). Motivating issues: #237, #244, #247,
#413 (write memory), #485/#501 (Writer/append removal fallout), Pioneer.jl
and SpineOpt.jl migration blockers.

## Problem

Arrow 3.0's writer is eager: `Arrow.write` materializes every partition,
constructs every column across all partitions at once, validates, and then
publishes the complete IPC output. That buys whole-table schema inference
(cross-partition pool unification, Union routes, abstract narrowing) and
validation-before-publish, but it removes the two 2.x capabilities people
actually used: writing batches as they are produced (`Arrow.Writer`), and
adding batches to an existing sink (`Arrow.append`). It also makes every
write hold the whole table plus the whole output in memory.

## What the internals already give us

Both IPC writers are batch-sequential loops over a byte accumulator:

- `writestream`: `_schemamessage!` once → per batch `_dictionarymessage!`
  (replacement supported: a pool identity change re-emits under the same id)
  + `_recordmessage!` → EOS marker.
- `writefile`: leading magic → the same message loop with per-message Block
  bookkeeping (offsets measured against the accumulator length) → EOS →
  Footer (schema + dictionary/record Block vectors) + length + magic.

Nothing in the loop needs the batch *vector*; only three things are
whole-table today: the `out::Vector{UInt8}` accumulator, the upfront
`_validatewriterbatches(sch, batches, ids)` sweep, and the facade's
materialize-all-partitions phase in `_writebytes`.

The second ingredient already exists too: **retained-schema column
construction**. `_constructcolumn(name, parts; retained=field)` builds a
column against a FIXED `Field` and fails closed with clear errors when the
data does not conform — the engine a fixed-schema incremental writer needs,
already tested by every facade rewrite.

## Proposal

### Layer 1 (internal): `IPCWriteState`

Extract the shared loop into a state struct in `ipc_write.jl`:

```julia
mutable struct IPCWriteState
    io::IO                      # the sink; batch bytes publish per batch
    file::Bool                  # footer bookkeeping on close?
    schema::Schema
    fielddictids::IdDict{Field,Int64}
    ids                         # assigndictids result
    codec::UInt8
    state::Union{Nothing,EncodeState}
    current::Dict{Int64,ArrayData}      # stream: last pool per id
    emitted::Set{Int64}                 # file: ids already written
    dictblocks::Vector{NTuple{3,Int64}} # file only
    recordblocks::Vector{NTuple{3,Int64}}
    written::Int64              # bytes published (replaces length(out))
    closed::Bool
end

beginwrite!(io, sch; file, compress, dictids) -> IPCWriteState
writebatch!(st, batch::AC.RecordBatch)   # validate THIS batch, stage its
                                         # bytes, publish once
finishwrite!(st)                         # EOS; file: footer + magic
```

Each `writebatch!` stages one batch's messages into a scratch buffer,
validates before touching `io`, then publishes — the eager writer's
validate-before-publish invariant holds per batch instead of per file.
`writestream`/`writefile` become `beginwrite!` + a loop + `finishwrite!`
over an in-memory sink, so the eager paths are byte-identical and the whole
existing battery/conformance/oracle surface proves the refactor.

Dictionary rules fall out of the existing checks:

- Stream format: a later batch with a different pool identity re-emits a
  replacement dictionary message (already the loop's behavior).
- File format: one dictionary batch per id; `writebatch!` refuses a pool
  identity change with the same wording `writefile` uses today. (A future
  option is accumulating pools and emitting dictionary blocks just before
  the footer — the Footer's Block index makes position irrelevant to
  conforming readers — but that changes pool-growth semantics and is out of
  scope for v1.)

### Layer 2 (facade): `Arrow.Writer`

The current `Arrow.Writer` tombstone becomes the real thing again:

```julia
w = Arrow.Writer(sink; file=true, compress=nothing,
                 metadata=nothing, colmetadata=nothing)
Arrow.write(w, table)   # one record batch per Tables.partitions partition
close(w)                # idempotent; EOS or footer; then close(io) iff we opened it
Arrow.Writer(sink; kw...) do w ... end
```

- The FIRST `Arrow.write(w, table)` runs today's full column construction on
  that table alone, fixing the `Schema`, and emits the schema message plus
  the first batches.
- Every LATER write constructs each column with `retained=` that fixed
  schema — exactly the rewrite path — so conformance failures reuse the
  existing, well-worded errors.
- Nullability is declared, not data-driven, matching the 3.0 write rule:
  a field is nullable iff the first table's column eltype admits `Missing`.
  A later batch with missing values under a non-nullable field is refused
  with a fix-forward message ("make the first table's column eltype
  missing-capable, or write an empty typed table first"). Writing a
  zero-row table with a fully typed `Tables.Schema` as the first write is
  the supported way to pin a schema explicitly — no new kwarg needed.
- `sink` is a path (opened at construction; the handle owns and closes it)
  or an `IO` (borrowed; `close(w)` finishes the IPC output but leaves the
  `IO` open). A mid-stream error leaves a torn sink — documented, like any
  incremental format writer; `Arrow.write` remains the atomic option.
- Single-owner cursor, like the read side: overlapping `write`/`close`
  calls on one handle throw; an `@atomic closed` flag backs idempotent
  close and use-after-close errors.
- Memory: O(current table) plus retained pool references. This is the
  documented answer to #237/#244/#247/#413.

### Layer 3 (facade): `Arrow.append`

```julia
Arrow.append(path_or_io, table; compress=nothing)
```

v1 supports the STREAM format only — the format where append is natural and
the format 2.x users actually appended to (SpineOpt, Agents.jl):

1. Read the existing schema: open the sink, `readstream` the schema message
   (and dictionary ids) — validation of the existing prefix is the reader's
   normal framing pass over what it consumes; append does not re-verify
   every historical batch body.
2. Construct the new table's columns with `retained=` the existing schema.
3. Seek to the trailing EOS marker (last 8 bytes), truncate/overwrite it,
   `writebatch!` the new batches (dictionary replacement messages allowed),
   re-emit EOS.

A file-format sink gets a clean refusal pointing at `Arrow.Writer` or a
whole-file rewrite. (File-format append — footer read, truncate, extend the
Block lists, rewrite the footer — is mechanically possible with
`verify_footer` + the Block machinery, but it rewrites trailing bytes in
place and v1 should not normalize that risk. Revisit on demand.)

## What this deliberately does not do

- No cross-batch schema inference: the first write (or an explicit typed
  empty first write) is the schema authority. This is the semantic price of
  incrementality, stated loudly in the docstrings: the eager
  `Arrow.write` can infer a Union across partitions; the Writer cannot.
- No `ntasks`-style pipelining: `writebatch!` is serial. The 2.x
  concurrency was a large share of its bug surface; add measured
  parallelism later if benchmarks demand it.
- No delta emission (the reader accepts deltas from other producers) and no
  file-format pool growth.
- No append-to-file-format in v1.

## Testing

- Refactor equivalence: eager `writestream`/`writefile` outputs are
  byte-identical before/after Layer 1 (golden comparison in the battery).
- Writer suites: multi-write stream and file outputs are byte-identical to
  the equivalent eager `Tables.partitioner` write for conforming inputs;
  schema-conformance refusals (type drift, missing under non-nullable,
  file-format pool change) assert the error wording; torn-sink behavior on
  a mid-write error; do-block; close idempotence; use-after-close.
- Append suites: append to a 3.0-written stream, a 2.x-written stream
  fixture (fixtures2x), and an appended-to-appended stream; read back with
  `Arrow.Stream` and pyarrow (conformance oracle case); dictionary
  replacement across appends; refusal on schema drift and on file-format
  sinks.
- Fuzz: extend the deterministic fuzzer with a Writer route (random batch
  splits of each corpus table must read back equal to the eager write).

## Sizing and sequencing

Layer 1 is a contained refactor of two functions that already have the
right loop shape. Layer 2's hard half (fixed-schema construction) already
exists as the retained path; the new code is the handle, first-write
bootstrapping, and errors. Layer 3 reuses both. Ship order: Layer 1+2
(Writer, both formats) before the 3.0 release — it converts the release's
one structural regression into a headline feature and unblocks Pioneer;
Layer 3 (stream append) with it if review goes smoothly, else in 3.0.x
(SpineOpt/Agents migrate to `Arrow.Writer` either way).
