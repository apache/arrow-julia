# ArrowCore prove-out review — round 1

Scope: `core/` on branch `core-rewrite`. The architecture in
`Arrow-redesign-report.md` §9 was the design authority. Deliberate exclusions
were not implemented. Claims were narrowed when the example did less than the
report's production design.

1. **CRITICAL — two concurrent closers could both own release.**
   `forceclose!` accepted an existing `closing` phase and could perform a
   `closing => closing` CAS. A second closer could publish `closed` while the
   first release callback still ran. A timed-out first closer could then write
   the old `open` state back. Fixed in `b9afada`: only `open => closing` wins;
   later closers wait for `closed` or a precise `closing => open` timeout
   restore. Release remains exactly once.

2. **HIGH — finalization bypassed the guard protocol.** A manual
   `finalize(region)` could release memory while an access guard was active,
   and it raced explicit close. Fixed in `04d0789`: finalization uses the same
   CAS and guard handshake and reinstalls its backstop when a manual finalizer
   finds an active guard. Release exceptions now publish `closed` in `finally`
   instead of stranding the region in `closing`. Timeout inputs and arithmetic
   are checked.

3. **HIGH — pointer geometry could escape checked slices.** Negative relative
   subslices could move before a parent message body. `loadat` used an
   overflowing `byteoff + sizeof(T)` check. Ownership geometry and roots were
   mutable after slice construction. The unaligned path used a risky
   reinterpret pattern. Fixed in `04d0789` and `9deb269`: relative offsets are
   nonnegative, last-line bounds use subtraction, geometry/root fields are
   constant, nonempty NULL regions are rejected, and unaligned values are
   assembled from a byte tuple. IPC tests include a negative body-relative
   buffer offset.

4. **MEDIUM — mmap geometry had a path-to-file race.** The old path was sized
   before the file descriptor was opened. A replacement could pair one inode's
   size with another inode's mapping. Fixed in `04d0789`: open first, size that
   descriptor, then map it. Concurrent external truncation of the mapped inode
   cannot be prevented and is now an explicit unsupported condition.

5. **HIGH — C import release did not invalidate the imported tree.**
   `ForeignOwner.release!` called the producer while descendant regions stayed
   open. A later accessor could use freed storage. Fixed in `49899c8`: every
   region in one moved ArrowArray tree shares one lifecycle gate. Explicit
   release and finalization wait for active guards, close the tree once, and
   make all later access throw `InvalidatedError`.

6. **HIGH — C export did not pin zero-copy source memory.** The registry kept
   Julia objects reachable, but a caller could still force-close mmap or
   foreign regions while a C consumer held raw pointers. Fixed in `49899c8`:
   array export takes one long-lived guard for each unique lifecycle and drops
   those pins only when that array tree is reaped. Partial pin acquisition and
   export failure release all acquired pins.

7. **HIGH — exported C release was not transitive or safe for moved children.**
   A root callback did not call live child and dictionary callbacks, contrary
   to the C Data producer contract. A later shared-root design then reclaimed
   a child that a conforming consumer had moved. Fixed across `49899c8`,
   `c5f218c`, and `2951444`: every C structure has an exactly-once per-node
   control; schema and array trees have separate aggregate roots; root release
   recursively releases non-moved nodes; source-NULL moved nodes remain live;
   and the last outstanding moved node enables one aggregate reap. Tests cover
   list children, dictionaries, nested map-entry subtrees, and multiple moved
   siblings. This follows the [Arrow C Data release and move rules](https://arrow.apache.org/docs/format/CDataInterface.html#moving-child-arrays).

8. **HIGH — C export/import failure paths leaked or dereferenced invalid
   structure tables.** Export published a schema before an array failure and
   did not validate the Field/Data pair first. Import used mandatory pointers,
   child entries, counts, and `offset + length` before complete preflight.
   Empty arrays with only NULL buffers could also lose their owner root. Fixed
   in `49899c8`: export validates before publication and rolls both roots back;
   import checks root and descendant release pointers, mandatory tables and
   entries, nonnegative counts/geometry, exact layout shape, checked addition,
   and a depth cap before recursive use. Empty arrays carry an explicit owner
   anchor. Failure still releases the moved input exactly once.

9. **MEDIUM — C flags and schema claims were incomplete.** Ordered dictionary
   and sorted-map flags were dropped. The README implied that all Core layouts
   and Field metadata round-tripped through C Data. Fixed in `49899c8` and the
   claim cleanup: required flags round-trip; the README lists the actual mapped
   formats and states that Field metadata and dictionary value-schema details
   are not lossless.

10. **HIGH — runtime descriptor and tree-shape validation was incomplete.**
    Validation compared descriptor Julia types instead of descriptor values,
    truncated child recursion through `zip`, accepted invalid primitive widths
    and union ids, and left mutable vectors able to invalidate cached results.
    Fixed in `04d0789`: value-level descriptor equality and domain checks,
    exact child arity, checked metadata-derived sizes, union id count/range and
    uniqueness, forbidden nested REE values, and defensive frozen copies for
    Fields, schemas, layouts, buffers, and children.

11. **HIGH — accepted arrays could still fail or read the wrong slot.** ListView
    was treated as range offsets instead of per-row offsets plus sizes. Map
    ignored the entries child offset. Struct and sparse union indexing did not
    apply the parent offset. Dense union ordering and selected-child bounds
    were incomplete. Dictionary full validation did not recurse into values.
    Fixed in `04d0789` and `11ba1a5`: buffer roles and per-layout checks match
    the columnar layout; nested indexing applies each physical offset once;
    dense offsets are nondecreasing per child; dictionary indices and values
    are both validated; and adversarial offset/nesting tests cover these paths.

12. **HIGH — null metadata and the semantic cache were unsound.** Invalid
    `null_count` ranges, absent validity buffers with declared nulls, and a
    mismatch between the bitmap and a cached count could pass. One ArrayData
    validated through a nullable Field could then bypass a non-nullable Field
    contract through `semachecked`. Fixed in `04d0789` and `11ba1a5`:
    null-count range and bitmap consistency are checked; the atomic cache only
    covers data-intrinsic scans; Field nullability and recursive Field contracts
    run on every call. Threaded stress tests cover null-count and semantic-cache
    races.

13. **MEDIUM — valid edge layouts were rejected or failed in accessors.** Empty
    offset-based arrays with a NULL offsets buffer were rejected. Empty List or
    Map slots at the largest signed offset overflowed when forming `lo + 1`.
    Dictionary value nullability incorrectly inherited index nullability.
    Struct access threw for valid duplicate or omitted names. Fixed across
    `04d0789`, `9516ec5`, and `ce92cfe`: canonical empty offsets are accepted;
    empty ranges return before addition; dictionary values are independently
    nullable; and duplicate-name Struct scalars use an ordered Pair vector.
    Map child names remain unrestricted because the Arrow schema says the
    conventional `entries`/`key`/`value` names are not enforced.

14. **HIGH — IPC metadata reached generated getters without verification.**
    Corrupt FlatBuffers counts and references could drive generated
    `unsafe_wrap` and string/table getters outside the intended metadata graph.
    Fixed in `bacb6dd`: a byte-wise verifier checks every table, vtable, scalar,
    union, vector, string, and reference used by the mapped adapter before the
    first generated getter runs. It validates metadata versions, schema
    endianness, required features, nesting, descriptors, and message header
    types. Fuzzed mutations and targeted corrupt tables fail cleanly.

15. **HIGH — IPC limits did not bound metadata-directed expansion.** Aliased
    Field tables expanded exponentially in `corefield`; shared large strings
    were copied once per getter occurrence; dictionary replacement rebuilt the
    value Field repeatedly. Fixed in `bacb6dd`: the verifier counts logical
    traversal occurrences, charges every logical string use and conservative
    container reserves, limits vector entries/depth/objects, and builds one
    Core dictionary value Field per id. Tests include an aliased schema DAG and
    repeated shared-name strings.

16. **HIGH — IPC framing and body authority were incomplete.** Checked message
    arithmetic, metadata/body alignment, exact EOS handling, trailing-byte
    rejection, version consistency, Big-endian rejection, and metadata/body
    length checks were missing. A negative buffer offset could subslice before
    the body. Fixed in `bacb6dd`: continuation framing uses checked spans and
    eight-byte rules; missing final EOS after a complete message remains the
    declared tolerated boundary case; partial prefixes, body truncation, and
    bytes after explicit EOS fail. Every buffer is a nonnegative checked slice
    of its own body.

17. **HIGH — record and dictionary buffer accounting accepted corrupt data.**
    Dictionary batches skipped cursor exhaustion and validation. RecordBatch
    header lengths were ignored. Zero-length scalar defaults became `nothing`.
    Zero-column batches bypassed the array-length limit. Overlapping body
    buffers could alias another column and return wrong values. Fixed in
    `bacb6dd`: record and dictionary paths share exact cursor completion;
    normalized lengths are checked against nodes and limits; omitted zero
    defaults are accepted; nonempty buffers must be aligned and nonoverlapping;
    and zero-column row counts remain explicit.

18. **HIGH — IPC dictionary state was not spec-safe.** Dictionary pools
    inherited encoded-index nullability; equal nested schemas sharing an id
    compared by object identity; replacement did not require the declared
    feature; batches could observe mutable later state; and a valid all-null
    reference before its dictionary was rejected. Fixed in `bacb6dd`: pool
    Fields are independently nullable; recursive schema compatibility governs
    shared ids; full replacement is feature-gated; delta remains excluded;
    every decoded batch captures its dictionary snapshot; and completely-null
    pending records resolve when the dictionary arrives.

19. **MEDIUM — dictionary replacement caused quadratic pending work.** Every
    replacement scanned and reallocated the full pending-record list, although
    a pending record can wait only for an undefined id. A valid stream could
    amplify `M` pending batches and `R` replacements into `O(M*R)` work outside
    the verifier reserve. Fixed in `bacb6dd`: pending resolution runs only for
    the first definition of an id. A fresh independent audit exercised
    replacement while another dictionary remained pending.

20. **MEDIUM — concurrency coverage could pass on one Julia thread.** Task
    interleaving did not prove the two-location memory-order handshake on
    separate OS threads. Fixed in `04d0789`: the normal Core test command starts
    a `--threads=4` child and runs guard/close, cache, and lifecycle stress.

21. **MEDIUM — implementation claims exceeded the prove-out.** The module and
    README had stale line/assertion counts, called all semantic validation a
    cached one-time operation, described a removed reap queue and singular
    control, implied broad IPC and C Data format coverage, and described an
    eager byte-vector IPC decoder as a general stream reader. Fixed in the
    claim update `af9b496`: measured counts were removed; adapter subsets, metadata
    loss, borrow lifetime, eager decode, current continuation framing, and all
    declared exclusions are explicit.

22. **HIGH design gap, disclosed — C callbacks are not arbitrary-thread
    production callbacks.** The example's callbacks enter Julia and take a
    `ReentrantLock`; they do not implement §9's native CAS and lock-free
    foreign-thread trampoline. This was not expanded into production adapter
    work. The source and README now require serialized callbacks for one tree
    on Julia-attached threads. Transitive release and move semantics are tested
    within that contract.

23. **MEDIUM design gap, disclosed — the IPC verifier is a local bridge.** The
    report requires pinned regenerated bindings and a generated verifier, with
    no custom parser. Updating the repository bindings would touch `src/`,
    which this review forbids. The bounded byte-wise verifier therefore remains
    only in the `core/` example. The README states this difference and does not
    present it as the production implementation.

24. **LOW verification gap, disclosed — 32-bit ABI expectations were not
    executed.** The example contains explicit 32-bit size and field-offset
    gates, but the available host is 64-bit. The 64-bit ABI gate passed. A
    32-bit CI job is still required to execute the other branch.

25. **MEDIUM — temporal semantic domains were not enforced.** Date64 values
    that were not whole days in milliseconds and Time values outside one day
    passed even full validation. Fixed in `7f4913a`:
    Date64 requires divisibility by 86,400,000, and every Time unit requires a
    value in its unit-adjusted half-open day range. Tests cover negative,
    upper-bound, and valid boundary values for 32-bit and 64-bit storage.

Final review basis: the current [Arrow columnar and IPC specification](https://arrow.apache.org/docs/format/Columnar.html)
and [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).

VERDICT: FINDINGS
