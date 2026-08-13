# ArrowCore prove-out review — round 2

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
fixes recorded in `REVIEW-codex-r1.md`. The design authority was
`Arrow-redesign-report.md` §9. This was a fresh adversarial pass over Core,
the IPC and C Data examples, their tests, and the README. Declared exclusions
were kept excluded; a boundary that claimed validation without implementing it
was changed to fail closed.

1. **HIGH — nested lifecycle delegation could release storage under an active
   guard.** `OwnerRegion(lifecycle=child)` was accepted when `child` already
   delegated to another region, but `_lifecycle` followed only one hop. A
   grandchild access incremented the intermediate guard count while closing
   the root saw zero guards and released the shared storage. Fixed in
   `80c6eef`: construction canonicalizes every delegate to the root lifecycle
   gate. A deterministic regression holds a grandchild guard while attempting
   to close the root.

2. **CRITICAL — the C export reaper could free allocations while the exporter
   still built them.** `_newroot` inserted an `ExportedRoot` with
   `remaining == 0` into `EXPORT_REGISTRY` before its build closure ran. A
   concurrent `reap!` could remove the root, free a just-created malloc, and
   let the builder continue through the freed pointer. Fixed in `35b9e62`:
   roots remain private until construction completes, and publication happens
   atomically before return. The regression blocks a builder after `_malloc!`,
   runs the reaper, and proves that neither the malloc nor the root is exposed
   early.

3. **HIGH — generic raw loads accepted Julia values that contain managed
   references.** `loadat(slice, T, offset)` sent arbitrary `T` to
   `unsafe_load`. Loading attacker-controlled bytes as a struct with an `Any`
   field caused a Julia subprocess to exit with signal 11. Fixed in `59b5ae1`:
   `loadat` rejects every non-isbits type before size calculation or pointer
   access. The former crashing type now throws `ArgumentError` in process.

4. **HIGH — a declared region extent could wrap the native address space.** A
   region starting at `Ptr{UInt8}(typemax(UInt))` with length two passed the
   signed geometry checks. A one-byte slice then wrapped pointer arithmetic
   and `loadat` segfaulted. Fixed in `ffc5023`: `OwnerRegion` proves that the
   address of its final byte is representable before any slice can be built.
   The near-`typemax(UInt)` case now fails at construction.

5. **MEDIUM — structural-only layouts were falsely marked semantically
   validated.** `validate_semantic` cached success for Utf8View, BinaryView,
   ListView, and run-end encoding even though the prove-out deliberately has
   no semantic content checks or accessors for them. Invalid ListView
   offset/size pairs and invalid REE run ends could therefore receive a false
   semantic certificate. Fixed in `d55bda5`: semantic and full validation now
   reject these declared exclusions with `ValidationError`; structural
   validation remains available and documented.

6. **MEDIUM — concurrent IPC pulls raced and duplicated batches.**
   `IPCStream.nextindex` was a plain mutable integer. An eight-thread repro
   over 200,000 unique batches returned 1,187,355 results, of which only
   200,000 were unique. This violated §9's single-owner reader contract
   without reporting the usage error. Fixed in `1913d7a`: an atomic ownership
   gate rejects overlapping `nextbatch!` calls with
   `ConcurrencyViolationError` and releases the gate in `finally`. The normal
   IPC command now runs a four-thread, 200,000-batch child regression and
   proves exact-once consumption.

7. **MEDIUM — the IPC verifier accepted malformed UTF-8 FlatBuffer strings.**
   `_vstring` checked bounds, length, and the trailing NUL but not the string
   payload. A schema with a field-name byte changed to `0xff` passed the
   verifier and reached generated metadata access. Fixed in `b692594`: the
   byte-wise verifier checks UTF-8 before the first generated getter runs. A
   corrupt-schema regression confirms a clean `ValidationError`.

8. **MEDIUM — a valid Core Struct name could fail after successful
   validation.** Struct scalar access converted every unique, nonempty child
   name to `Symbol`. An embedded-NUL name passed structural and semantic
   validation but `Symbol(name)` threw. Fixed in `24384da`: `NamedTuple` is
   used only for Symbol-compatible names; the ordered `Pair{String,Any}`
   fallback preserves every other spelling. The README and regression now
   cover this boundary.

9. **HIGH — C Data import and export skipped full Utf8 validation.** Both
   directions ran structural and semantic checks only. A Utf8 array containing
   `0xff` was exported and imported as invalid Arrow Utf8 data, contrary to the
   §9 requirement that C import run the staged checks over its declared
   geometry. Fixed in `f8373cb`: both directions run `validate_full` before
   success. Regressions cover pre-publication export rejection and post-move
   import rejection with exact schema/array cleanup.

10. **MEDIUM — schema names crossed incompatible string boundaries without
    validation.** Core accepted malformed UTF-8 field names. C export copied
    an embedded NUL into a NUL-terminated name, so import silently changed
    `"a\0b"` to `"a"`; C import also accepted malformed UTF-8 names. Fixed in
    `f8373cb` and `be10935`: Core structural validation requires UTF-8 field
    names, C string creation rejects malformed UTF-8 and embedded NULs, and C
    import validates strings before building Fields. Failure-path regressions
    prove that no export root leaks and moved imports release both lifetimes.

11. **MEDIUM — runtime descriptors exceeded their Arrow wire domains.** Empty
    arrays using a decimal scale outside `Int32`, a fixed-size-binary width
    above `typemax(Int32)`, or a fixed-size-list size above
    `typemax(Int32)` passed validation even though the corresponding
    [`Schema.fbs`](https://github.com/apache/arrow/blob/main/format/Schema.fbs)
    fields are signed 32-bit integers. Fixed in `3b0d61c`: descriptor
    validation enforces those wire ranges. Zero-length regressions isolate the
    descriptor checks from buffer-size arithmetic.

12. **MEDIUM — REE structural validation accepted impossible geometry.** A
    nonempty parent with no physical runs passed. An Int16-run-end parent with
    logical extent 32,768 also passed even though no Int16 run end can cover
    it. The parent null count was not required to be zero. Fixed in `ad38281`:
    structural validation requires a physical run for a nonempty extent,
    bounds the logical extent by the run-end integer type, and enforces the
    [REE parent null-count rule](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout).
    Ordering, positivity, final coverage, and access remain excluded semantic
    work and therefore fail closed under finding 5.

13. **MEDIUM — the dictionary replacement and snapshot claim lacked a
    positive test.** The IPC example only duplicated a dictionary without the
    required feature and asserted rejection. It did not prove the README claim
    that a feature-enabled full replacement works or that an older batch keeps
    its prior dictionary. Fixed in `a331758`: a hand-built feature-bearing
    stream now has a record before and after a full replacement, and asserts
    old-pool/new-pool materialization plus distinct immutable snapshots.

14. **LOW — the access-guard comment overstated the current implementation.**
    It said bulk kernels take one guard per call, but `materialize` currently
    reuses scalar accessors and may take several guards per element. Fixed in
    `de0d505`: the comment states the implemented granularity and identifies
    one-guard bulk amortization as future facade work.

## Scope decisions and withdrawals

- The declared view-layout and REE accessors/semantic scans were not
  implemented. Their validation boundary now rejects unsupported stages
  instead of claiming success.
- Foreign C allocation sizes remain trusted declarations because the C Data
  ABI supplies pointers, not allocation extents. This is still stated in the
  README.
- Padding, unused-bit checks, compression, file footer/index support, facade
  work, and the native foreign-thread C callback trampoline remain declared
  exclusions.
- A possible `@generated` style concern was withdrawn. Its explicit ban is in
  report §8, not the user-designated §9 authority, and this pass found no
  concrete correctness or compile-shape defect from the small ABI setter.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 212/212 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including the four-thread cursor gate, corrupt UTF-8 metadata, and positive
  dictionary-replacement snapshot regressions.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including
  concurrent construction/reaping, invalid UTF-8/name rollback, moved-import
  cleanup, and registry-empty checks.
- All round-2 changes are confined to `core/`. Each logical change is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: FINDINGS
