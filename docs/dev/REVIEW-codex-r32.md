# Arrow.jl 3.0 code review — round 32

Date: 2026-08-16

Scope: commits `0625d0ff1b6ac3a6743719890c0eb11fef4753cf` and
`a94392cd87cee61cec761cde70b337a30169bc96` only.

## Result

Six findings remain. Three can cause a failed deterministic release, unsafe
post-release access, or wrong scan results. The canonical-bit implementation,
trim devirtualization, and FlatBuffers edits are otherwise clean.

## Findings

1. **HIGH — `close!` does not unmap an mmap region.**

   `close!` calls `finalize(r.root)` (`src/ArrowCore.jl:212-215`), and
   `mmapregion` stores the mapped `Vector` as that root
   (`src/ArrowCore.jl:240-249`). Julia 1.12.6 does not register the unmap
   finalizer on that vector. `Mmap.mmap` registers it on `A.ref.mem`
   (`stdlib/v1.12/Mmap/src/Mmap.jl:253-263`). `finalize(A)` therefore does not
   run the unmap action.

   A focused probe attached an observation finalizer to `A.ref.mem`. After
   `close!(r)`, that finalizer had not run and a direct diagnostic read still
   returned the mapped byte. It ran only after `finalize(A.ref.mem)`:

   ```text
   after_close root_mem_finalizer_ran=false mapping_still_readable=true
   after_finalize_mem root_mem_finalizer_ran=true
   ```

   The probe exited 0. It did not access memory after the real unmap.

   The `rm(path)` assertion at `test/core_tests.jl:273-281` is not a portable
   unmap test. POSIX permits unlinking a mapped file, and a separate probe
   confirmed that the old mapping remained readable after `rm`. Thus, the
   test can pass without an unmap and does not prove the Windows use case that
   motivated this API.

   The root cause is that the GC anchor and the object that owns the release
   finalizer are not necessarily the same object. The close action must target
   an explicit owned release object, not assume `finalize(root)` releases the
   storage.

2. **HIGH — closing one imported C-data region releases the shared owner but
   leaves sibling regions open.**

   `_import_array` creates a distinct `OwnerRegion` for every non-null fixed
   or variadic buffer (`src/cdata.jl:1165-1243`). All those regions use the
   same `ForeignOwner` as their root. `close!` marks only its receiver closed
   and then finalizes that shared owner. A producer release callback may free
   every buffer in the imported tree, while the other regions still report
   `closed == false`.

   A UTF-8 C-data round trip has separate offset and data regions. Closing the
   offset region produced this state:

   ```text
   same_owner=true owner_released=true first_closed=true
   sibling_closed=false sibling_sliceptr_nonnull=true
   ```

   The probe exited 0 and then closed the sibling and reaped the export. It
   deliberately did not dereference a pointer after reap. The released owner
   plus a successful sibling `sliceptr` is sufficient to prove the unsafe
   state. With a foreign producer that frees during its callback, the sibling
   path can dereference freed storage.

   The raw-pointer audit itself passed: every region-data pointer in `src/`
   flows through `sliceptr`. The defect is that the closed flag is per extent
   while the release lifetime is per aggregate owner. All regions under one
   release action need one shared revocation state, or a higher-level close
   must revoke every extent before it releases the owner.

3. **HIGH — the `colne` rewrite can make statistics pruning lose valid
   rows.**

   Commit `0625d0f` changed the NaN battery predicate from a negated equality
   node to `Tables.colne` (`test/scan_battery.jl:881-890`). This preserves
   row-level meaning, but it changes the expression from
   `NotExpr(Cmp(OP_EQ))` to `Cmp(OP_NE)`.

   `_maypass` handles `OP_EQ`, `OP_LT`, `OP_LE`, and `OP_GT`, then treats every
   remaining comparison as `OP_GE` (`src/scan.jl:1396-1407`). It therefore
   handles `OP_NE` as `OP_GE`. For a zero-valued batch and `x != NaN`, it tests
   `0.0 >= NaN`, gets `false`, and wrongly prunes the batch. The old negated
   equality used the conservative logic at `src/scan.jl:1432-1440`.

   The exact direct-battery result was:

   ```text
   expected = Any[0.0, 0.0, -0.0, -0.0, NaN, NaN]
   actual   = Any[NaN, NaN]
   ```

   A focused probe confirmed that the row masks are equal but the batch
   decision differs:

   ```text
   row_masks_old_new=Bool[1, 1, 1]/Bool[1, 1, 1]
   stats_maypass_old_new=true/false
   ```

   That probe exited 0. A direct `_stats_main()` run exited 1 at
   `test/scan_battery.jl:889`.

   `Pkg.test()` stays green because `test/batteries.jl:57-59` calls only
   `_scan_main()`. It never calls `_stats_main()` or `_ranged_main(...)`.
   Repository search finds those functions only at their definitions. The
   changed assertion is therefore outside the keep-green suite.

   `OP_NE` may prune only when known `min == max == rhs`. Otherwise it must
   keep the batch. The existing negated-equality branch has the needed rule.
   A focused `OP_NE` regression must also run from `Pkg.test()`.

4. **MEDIUM — generic `finalize(root)` runs unrelated finalizers on borrowed
   heap vectors.**

   `heapregion` is a zero-copy borrow and stores the caller's vector as the
   opaque GC root (`src/ArrowCore.jl:218-226`). `close!` nevertheless runs all
   finalizers registered directly on that vector. A probe closed a heap
   region while the caller still held the vector:

   ```text
   heap_user_finalizer_calls=1 vector_still_live=1
   after_second_close_and_finalize=1
   ```

   The probe exited 0. `close!` must not interpret an opaque borrowed GC
   anchor as an owned release action. Heap roots need no eager action.

5. **MEDIUM — an empty imported C-data array may have no region through which
   to close its owner.**

   Empty fixed-width imports keep the live `ForeignOwner` in
   `ArrayData.owner`, while all physical buffers can be canonical absent
   `BufferSlice()` values. A probe reported:

   ```text
   region_count=0 owner_released=false
   ```

   It then released and reaped the owner without a leak. A future
   `close!(::Table)` cannot implement C-data release only by walking
   `OwnerRegion` objects. It must also close owner-only empty arrays. This is
   the same aggregate-lifetime mismatch as finding 2.

6. **LOW — close-related documentation still states the removed design.**

   The module overview says regions are immutable, loads have no per-access
   synchronization, and eager release is out of scope
   (`src/ArrowCore.jl:35-43`). The memory-model block repeats those claims and
   says there is no revocation (`src/ArrowCore.jl:102-129`). The OwnerRegion
   docstring says the type is immutable and has no lifecycle
   (`src/ArrowCore.jl:151-160`). `src/cdata.jl:52-64`,
   `docs/dev/core-README.md:76-89`, and `test/core_tests.jl:67` also describe
   the old no-close model. These statements now contradict the type, hot
   path, and public `close!` API introduced by `a94392c`.

## Checks that passed

### Remaining `close!` questions

- The source raw-pointer audit found only one direct `r.ptr` read, inside
  `sliceptr` (`src/ArrowCore.jl:281-286`). `loadat`, `slicebytes`, C-data
  export, and IPC decompression all obtain region pointers through it. Other
  `unsafe_*` calls operate on C ABI structures, owned malloc blocks,
  FlatBuffers vectors, or destination vectors.
- The immutable data fields remain `const`. `BufferSlice` remains 24 bytes.
  `OwnerRegion` grows from 32 to 40 bytes. Optimized `loadat` IR contains one
  monotonic atomic byte load for the close check. A 1,000-load loop inferred
  `Int64` and allocated zero bytes.
- `ForeignOwner` registers one finalizer and claims release with one atomic
  swap (`src/cdata.jl:876-910,948-962`). A sequence of `close!`, explicit
  `release!`, later `finalize`, and repeated `close!` left `released=true`,
  reaped the two export roots once, and returned zero on a second reap. No
  double-free path was found. This exactly-once result does not repair the
  sibling-region revocation defect.

### Canonical bits

No canonical-bit defect was found.

- The check runs only when `offset == 0 && len > 0`
  (`src/ArrowCore.jl:1681-1683`). Recursive child and dictionary calls apply
  that rule independently to every node (`src/ArrowCore.jl:1727-1731`).
- `cld(len, 8)` computes the occupied bytes. Arrow bitmaps use LSB numbering,
  so `UInt8(0xff) << (len % 8)` selects exactly the unused high bits. The
  final used byte is loaded at zero-based offset `nbytes - 1`. Padding starts
  at `nbytes`. This matches the
  [Apache Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html).
- When `len % 8 == 0`, the mask step is skipped. A full final Boolean DATA
  byte is not mistaken for padding. Empty arrays return before any load.
- Null and REE layouts have no buffers. V5 unions have type IDs and optional
  element offsets but no validity bitmap. These layouts cannot enter the
  bitmap branch. Dictionary and nested child arrays are checked according to
  their own physical layouts.
- The focused probe covered lengths 1 through 40, every tail size, exact-byte
  Boolean lengths, zero and nonzero padding, empty arrays, sliced children,
  Null, sparse Union, and REE.
- C-data export, import, stream export, and stream import still call
  `validate_full` (`src/cdata.jl:723-728,983-1006,1396-1418,1693-1732`). The
  C-data battery, full package suite, corpus, and oracle results are recorded
  below.

### Trim devirtualization

No trim-devirtualization defect was found.

- The closed set contains 22 concrete `ArrowType` descriptors and 22 layout
  methods (`src/ArrowCore.jl:405-475,570-601`). `formatstring_of` enumerates
  all 22 (`src/cdata.jl:150-173`). A runtime probe matched every ladder branch
  to direct dispatch.
- Only Date, Decimal, and Time have non-default advisory-value methods
  (`src/ArrowCore.jl:1156-1263`). `_validate_advisory_values_of` enumerates
  exactly those three (`src/ArrowCore.jl:1705-1713`).
- `_newroot` creates only the key and GC-managed containers before the `try`.
  Native allocations first occur inside `build(root)`. Build and publication
  failures clear handoff slots and select exactly one private or registered
  cleanup path (`src/cdata.jl:749-855`). Focused build and publication-failure
  injections left no registry entry and no native allocation.
- The array claim slot exactly matches
  `Tuple{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowArray}},Ptr{CArrowArray}}}`. The
  schema slot uses the corresponding schema types
  (`src/cdata.jl:350-390,498-535`). Runtime probes confirmed both producer and
  consumer tuple types.
- `_arraydata` retains the original checks, conversions, frozen containers,
  owner, null count, and initial cache state. `@nospecialize` changes code
  generation only, not construction semantics
  (`src/ArrowCore.jl:673-695`; `src/cdata.jl:1256-1257`).

### FlatBuffers

No FlatBuffers defect was found.

- `VOffsetT` is `UInt16`. The new two-byte expression reconstructs
  `b[1] | b[2] << 8`, which is the required little-endian value. FlatBuffers
  stores all scalars in little-endian form and shares equal vtables, as the
  [FlatBuffers internals specification](https://flatbuffers.dev/internals/)
  states. On this little-endian host, an exhaustive 65,536-value probe matched
  the removed `read(IOBuffer(...), UInt16)` expression.
- Two identical generated Int tables retained one vtable entry. The dedup
  probe exited 0.
- A mixed stream read from the frozen 2.x fixture, written at parent
  `0625d0f`, and written at `a94392c` produced 2,672 bytes at both revisions.
  Both files had SHA-256
  `13035541212c37041b1af7300649a4f2b4068069f6f1597838b3dae3a07b7bf9`.
  `cmp` passed, and the current bytes round-tripped as seven fields and two
  batches.
- Repository-wide reference search found no caller of the deleted `reset!`,
  `bytevector`, or `union!` methods. They were private vendored-runtime
  carryovers and are dead in this tree.

### Remaining Tables DSL checks

- Every `coleq` rewrite creates the same `Cmp(OP_EQ, ...)` node as the removed
  equality overload. Each changed row-level assertion retains its meaning.
- The extreme offset/limit test (`test/scan_battery.jl:198-208`) compares to
  the current saturating `Tables.finish` authority and checks zero rows for
  whole-file and ranged sources. It asserts the correct result. The exact
  request is residualized by `_canconsumewindow` (`src/scan.jl:490-494`), so
  it does not exercise a source-consumed overflowing window.

## Assumptions and decisions

- I reviewed HEAD `a94392c` against parent `313cb2f` and kept the development
  Tables checkout at `d1fbb6eb577741688dba70039754166b51c1cdcc` unchanged.
- I accepted the constrained reachability model and the stated contract that
  callers quiesce concurrent readers before `close!`.
- I assumed a conforming C-data producer may free its full array tree during
  the release callback. This is the safety boundary that exposes finding 2.
- I treated deterministic mmap close as requiring a real unmap. Merely making
  Julia `BufferSlice` access throw is not sufficient for the stated Windows
  file-release use case.
- I treated the official Arrow format as the authority for bitmap layout and
  the official FlatBuffers format as the authority for vtable byte order.
- This was a review-only task. I added only this report. I did not apply a
  product fix, alter the Tables development dependency, or touch the six
  files that were already untracked at review start. Unrelated facade files
  and tracked edits appeared in the shared checkout after the validation
  runs; I left them untouched and outside this two-commit review.

## Validation

- `julia --project=. -e 'using Pkg; Pkg.test()'` — exit 0; 336/336 Core,
  4/4 threaded Core, and the IPC read, IPC write, C-data, and `_scan_main`
  acceptance testsets passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, including
  the C-data workload and zero trim-verifier errors.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` — exit
  0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` — exit
  0; 170 pass / 0 fail / 43 skip with the cached Docker image.
- Direct `_stats_main()` battery — exit 1 at
  `test/scan_battery.jl:889`, reproducing finding 3.
- Mmap finalizer-target, C-data sibling-region, heap-finalizer, empty-import,
  ForeignOwner exactly-once, closed-set ladder, `_newroot` failure, typed
  claim-slot, `_arraydata`, canonical-bit, vtable-dedup, and mixed-writer
  byte-identity probes — all exited 0 with the stated assertions.
- `git diff --check 0625d0f^..a94392c` — exit 0.

VERDICT: FINDINGS
