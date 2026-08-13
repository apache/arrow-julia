# ArrowCore prove-out review — round 14

Scope: primary commits `eb093e0`, `ee1268d`, `b626138`, and `4252342`,
plus their interactions with Core and the read adapter. The secondary pass
covered `028020c`, `642aecd`, `4722370`, `e1317a5`, `f5f4ace`, `c805a05`,
and `dcf8dfc` with fresh eyes.

The review used the current Arrow
[Columnar format](https://arrow.apache.org/docs/format/Columnar.html),
[C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html),
and [C Stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
as the wire and ABI references. The constrained memory model remains final:
GC reachability is the only Core validity mechanism. No guard, revocation,
lifecycle state, `Threads.Atomic`, or interruption machinery was added.

## Numbered findings and dispositions

All findings below were fixed and verified.

1. **HIGH — one repeated `Field` identity could silently merge two dictionary
   columns.** `assigndictids` used an `IdDict`, so two schema positions that
   shared one immutable `Field` object received one id. Two different pools
   were then emitted under that id, and the first column decoded through the
   second pool. Fixed in `8618d02` and completed in `74b8a6f`: the complete
   schema walk rejects a repeated `Field` identity. The regression covers
   top-level dictionary aliases and aliases inside dictionary value subtrees.

2. **HIGH — a shared dictionary pool skipped later Field contracts.** The
   writer certified a pool by identity under the first value Field. A later
   dictionary Field with stricter child nullability could reuse that
   certificate and emit data that this reader then rejected. Fixed in
   `8618d02`: each `(value Field, pool)` pair runs Field-contract validation
   before the pool enters the identity cache. The incompatible shared-pool
   regression now fails at write time.

3. **MEDIUM — schema features were not exact.** Compressed files omitted
   `COMPRESSED_BODY` from both schema copies. A compressed zero-batch stream
   could declare it without emitting a compressed body. The file reader also
   accepted `DICTIONARY_REPLACEMENT`, which files forbid. Fixed in `8618d02`:
   both file schemas use the four-slot raw Schema bridge, compression is
   declared only when a batch is emitted, and files reject replacement.

4. **MEDIUM — empty offset arrays had the same non-conforming shape in both
   adapters.** IPC emitted a zero-byte offsets buffer, and C Data export used
   `NULL`. Both paths therefore omitted the required `length + 1` terminal
   offset while their own readers still accepted the data. Fixed in `8618d02`
   and `6764cd1`: each writer materializes one zero offset for a canonical
   empty array. IPC and C Data import require that physical offset. Tests
   inspect the wire buffer length and the exported C pointer, not only the
   logical round trip.

5. **MEDIUM — sparse-union and type-id checks were late or incomplete.** IPC
   accepted and emitted sparse children longer than the parent, although IPC
   sparse children must have the parent length. An omitted `typeIds` vector
   with 129 children threw `InexactError` during `Int8` conversion. Fixed in
   `8618d02`: IPC encode and decode require exact sparse child lengths, and
   union id count, domain, and uniqueness are checked before conversion.
   Core keeps its wider child-extent rule for valid sliced C Data arrays.

6. **MEDIUM — schema-only writes bypassed the full Schema envelope checks.**
   Invalid UTF-8 names or metadata could be written, and a big-endian Core
   schema was silently described as little-endian when no batch forced later
   validation. Fixed in `8618d02`: stream and file writers validate Schema
   endianness, metadata, and every recursive Field before emitting bytes.

7. **MEDIUM — `readfile` trusted only `Footer.schema`.** It did not parse or
   compare the leading Schema Message. The same file bytes could therefore
   describe different field names, features, dictionary ids, metadata, or
   metadata versions depending on whether a stream or file reader opened
   them. Fixed in `8618d02` and `0662b6a`: the leading Message is verified
   under the shared budget and compared semantically with the Footer,
   including raw Field metadata, dictionary ids, schema features, versions,
   and Message/Footer custom metadata.

8. **MEDIUM — Footer Blocks could overlap each other or escape into the
   Footer.** `_blockmessage` bounded a Block against the whole region. A
   forged body length could consume Footer bytes while the declared buffers
   still used only the original body. Fixed in `8618d02`: all Block extents
   are checked against the data boundary, schema overlap is rejected, and
   dictionary and record Blocks must not overlap.

9. **MEDIUM — optional EOS detection collided with valid data.** The first
   fix classified the final eight bytes as EOS from their value alone. A
   no-EOS file whose final `Int64` was `0x00000000ffffffff` then lost eight
   bytes of its indexed body and failed. Fixed in `74b8a6f`: Blocks are first
   verified against the Footer boundary. The final marker-shaped bytes count
   as EOS only when no indexed Block occupies them. This reader and Arrow.jl
   2.x both accept the regression file.

10. **MEDIUM — file verification did not apply all reader limits
    cumulatively.** Footer graph verification reset the allocation budget,
    and Block reads omitted metadata, body, and message-count limits. Fixed in
    `8618d02`: the Footer copy and verified graph share one budget, opening a
    file counts its Schema and indexed messages, and eager dictionary plus
    lazy record access apply the same metadata and body limits as streams.

11. **MEDIUM — file read and write lacked an early host-endian gate.** On a
    big-endian host, byte-wise verification could succeed before old generated
    getters or native `reinterpret` operations used incompatible scalar
    order. Fixed in `8618d02`: stream/file write and file read reject the host
    before those operations. The injected opposite-BOM regression is clean.

12. **MEDIUM — the C format parser did not fail cleanly or early.** A
    multibyte timestamp prefix threw `StringIndexError`. Decimal descriptors
    with invalid precision or bit width reached array geometry and could throw
    `OverflowError`. Numeric fields accepted whitespace, `+`, and hexadecimal
    spellings. Duplicate or overlong union-id lists allocated heavily before
    later validation. Fixed in `6764cd1`: parsing is byte-safe, accepts strict
    ASCII decimal grammar, validates descriptors immediately, caps unions at
    128 ids without `split`, and rejects duplicate ids, invalid UTF-8, and
    embedded NULs with `ValidationError`.

13. **MEDIUM — a negative terminal C offset escaped as an internal error.**
    Import used the final variable-length offset as an `OwnerRegion` length
    before semantic validation. A value of `-1` threw `ArgumentError` instead
    of an adapter validation error. Fixed in `6764cd1`: the final offset is
    checked before region construction. Cleanup still releases the moved tree
    exactly once.

14. **HIGH — failed `StreamOwner` finalizer registration could release one
    producer stream twice.** The constructor copied an armed stream and called
    `release!` on registration failure before the source move was committed.
    The source remained armed and could call the producer again. Fixed in
    `9ad953e`: `StreamOwner` starts inert, registration failure frees only the
    inert copy, and the copy is armed immediately after the source-null move.

15. **HIGH — a pulled C stream batch was not moved and could leak on owner
    construction failure.** `get_next` returned a live `ArrowArray`, but the
    caller copied it into `ForeignOwner` without nulling the source output.
    A failed finalizer registration stranded the result export root. Fixed in
    `9ad953e`: failure before the move releases the live output slot; success
    nulls that slot and then arms the destination owner. Focused tests prove
    one callback on both paths.

16. **HIGH — exported C stream callbacks had incomplete exception barriers.**
    State lookup ran outside `try` in `get_schema` and `get_next`; error and
    release callbacks also had paths that could unwind Julia through C.
    Fixed in `9ad953e`: each callback contains its complete body in a
    non-throwing barrier. Status callbacks return `Base.Libc.EINVAL`,
    `get_last_error` returns `NULL` on failure, and `release` swallows failures
    at its void boundary. Import now requires the mandatory error callback,
    and failed error-message allocation clears stale state.

17. **HIGH — C stream publication transactions could strand native or Julia
    roots.** Key overflow or caller-struct publication failure leaked the
    stream control block and registry entry. A later failure while storing a
    `get_schema` or `get_next` result leaked that result's export root and
    advanced the batch cursor incorrectly. Fixed in `9ad953e` and `d231092`:
    stream publication rolls back its control and exact registry entry;
    result publication records and removes an unpublished root; `nextindex`
    advances only after the caller-owned struct receives the result.

18. **HIGH — native owner windows relied on incidental Julia liveness.**
    Stream callback calls, the producer-owned error pointer copy, and several
    inherited `ForeignOwner` raw block operations lacked formal preservation.
    Fixed in `9ad953e`: the owning Julia object and caller result storage stay
    inside `GC.@preserve` for each complete raw-pointer or callback window.
    This also closes the secondary `e1317a5` preserve gap.

19. **LOW — the mmap reachability regression did not test what it claimed and
    could fail on Windows.** The test kept the region and mapped bytes alive
    while claiming that only the slice remained. It then deleted the path
    before the mapping became unreachable. Fixed in `462c3db`: a helper
    returns only the slice through the GC probe, then all mapping references
    leave scope and collection completes before path deletion.

20. **LOW — capability and contract prose lagged the adapters.** The file
    table omitted `ArrowArrayStream`; ownership and atomic lists omitted
    `StreamOwner`; Core docstrings still called the C stream future work; IPC
    file, compression, dictionary, union-id, and empty-offset claims were
    incomplete; and 64-bit review prose was round-specific. Fixed in
    `fc77f5b` and `891e38a`. The README also states the Arrow.jl 2.x custom
    union-id limitation and the unavoidable C timestamp empty-timezone
    canonicalization.

## Checked without a finding

- The registry walk now has matching node, buffer, and child order in
  `encodefield!` and `decodefield`. Dictionary fields use index buffers in the
  record and value buffers in dictionary batches. Null counts are recomputed
  before write and checked against validity bitmaps after read.
- The V5 Message and Footer builders use correct reversed vectors, raw Schema
  feature and interval-unit slots, eight-byte padding, and Message finishing.
  Block structs have the required 24-byte layout. `metaDataLength` includes
  the eight-byte prefix and padded metadata, but not the body.
- LZ4_FRAME and ZSTD use one codec object per writer/reader state. The native
  calls preserve input owners and output arrays. Empty buffers, compressed
  frames, and `-1` stored-raw buffers have distinct checked paths.
- Dictionary replacement remains snapshot-based per id. Files emit one
  dictionary batch per id and reject pool changes. Lazy file access uses one
  fresh allocation budget and decode state per `getindex`; the file region
  roots heap or mmap bytes for the handle lifetime.
- C Data format mappings match the supported Core descriptors. Sparse union
  type ids and dense Int32 offsets use per-slot geometry with no `+1` entry.
  Core checks sparse coverage, dense id/offset bounds, and monotonic offsets
  per child.
- `MONTH_DAY_NANO` uses an `Int32` month, `Int32` day, and `Int64` nanosecond
  value at byte offsets 0, 4, and 8. The raw IPC interval unit is Int16 in both
  directions.
- The remaining round-13 changes are clean after the two secondary fixes.
  Region bounds/alignment, inert owner rollback, validation caches, dictionary
  certificates, and trim mmap cleanup remain consistent with reachability.

## Assumptions and decisions

- Foreign C allocation extents and producer callbacks remain trusted ABI
  declarations. Caller-owned C structs stay alive for each call. Calls on one
  stream do not overlap and run on Julia-attached threads, as documented.
- Ordinary exceptions are in scope. Asynchronous interruption, process exit,
  external mmap mutation/truncation, and post-release C access remain out of
  contract.
- I rejected repeated writer `Field` identities instead of replacing every
  field-keyed table with occurrence paths. This is the smallest safe prove-out
  rule and prevents silent dictionary corruption.
- The file reader accepts both optional-EOS and no-EOS files for current 2.x
  interoperability. Verified Block extents decide whether marker-shaped final
  bytes belong to data.
- The reader keeps compatibility with V5 compressed Arrow.jl 2.x streams that
  omit `COMPRESSED_BODY`. This writer declares the feature exactly when it
  emits compressed batches.
- Core supports custom union ids. The writer does not reduce that valid
  domain to match Arrow.jl 2.x's positional-id limitation.
- The available host is 64-bit little-endian. The 32-bit ABI branch was
  inspected but not executed. No public export surface or dependency was
  added. No push, rebase, or amend was performed.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 252/252 Core checks and 4/4
  four-thread cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed all
  framing, verification, compression, dictionary, resource-limit, union,
  metadata, and pull-concurrency checks.
- `julia --project=. --startup-file=no core/examples/ipc_write.jl`: passed all
  Core and Arrow.jl 2.x stream/file round trips plus the new dictionary,
  feature, offset, union, schema, budget, Block, and optional-EOS regressions.
- `julia --startup-file=no core/examples/cdata.jl`: passed all format,
  geometry, ownership, registry, move, callback, publication, and failure-path
  checks. Its four-thread C Data child passed.
- `julia --startup-file=no core/test/trim_compile_tests.jl`: 6/6 checks passed.
  JuliaC `--trim=safe` compiled and the produced binary exited successfully.
- `git diff --check` passed. Every round-14 fix is inside `core/`. Every fix
  commit has the exact required Codex co-author trailer.

VERDICT: CLEAN
