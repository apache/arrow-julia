# ArrowCore prove-out review — round 7

Scope: the current `core/` tree on branch `core-rewrite`, after the round-1
through round-6 fixes recorded in `REVIEW-codex-r1.md` through
`REVIEW-codex-r6.md`. The design authority was `Arrow-redesign-report.md`
§9. This was a fresh adversarial pass over Core, the IPC and C Data examples,
their tests, and the README. Declared exclusions were kept excluded.
Unsupported and trusted boundaries were checked for honest documentation and
safe failure instead of being implemented.

1. **MEDIUM — `mmapregion` leaked a successful mapping when ownership
   construction failed.** `mmapregion` called `mmap` and then constructed its
   release closure and `OwnerRegion` with no cleanup boundary. An allocation
   or constructor exception in that handoff left the mapping live with no
   owner. Fixed in `c87c038`: the post-`mmap` handoff is protected, and every
   failure before `OwnerRegion` takes ownership calls `munmap` directly. An
   injected constructor-failure regression performs one real unmap. The
   normal deterministic-close path still performs one real unmap.

2. **MEDIUM — C export construction could leak native allocations and source
   guards on bookkeeping failures.** `_malloc!` acquired native memory before
   its ledger insertion. `_pin_regions` acquired each lifecycle guard before
   appending it to a rollback vector. `_newroot` constructed its root and
   backing dictionaries outside the cleanup boundary, after the caller could
   already hold pins. Allocation failure at any seam lost the resource or
   left a source region permanently busy. Fixed in `9f9f325`: malloc ledger
   capacity is reserved first, post-malloc registration has an explicit
   ownership handoff, guard rollback uses a prebuilt vector and acquired
   prefix, and `_newroot` constructs all bookkeeping before it acquires pins.
   Every later build or publication failure now unwinds through one root
   cleanup path. Injected regressions cover failure before and after malloc
   registration, during guard acquisition, during root construction, and
   during export building.

3. **LOW — the export registry's source-rooting guarantee lacked a permanent
   drop-and-GC regression.** The example said Julia source references were
   dropped before import, but its main batch stayed live. The registry logic
   was sound in an isolated probe, so this was a coverage gap rather than an
   implementation defect. Fixed in `737e7eb`: a no-inline helper returns only
   C pointers and weak references, forced GC proves that the exported
   `ArrayData` and source region remain live, and import, release, and reaping
   complete with an empty registry.

4. **LOW — comments and README text misstated implemented boundaries.** A
   Core comment said REE child arity was Field-declared even though the layout
   registry fixes it at two. The `getvalue` docstring named a nonexistent
   `foreachvalue` bulk path. The IPC framer docstring said every truncated
   stream failed even though boundary EOF after a complete message is
   intentionally accepted. The README assigned every IPC limit to the byte
   verifier although framing and cursor stages enforce distinct limits.
   Fixed in `dd53157`: each statement now matches the code and the committed
   regressions.

## Scope decisions and withdrawals

- No additional defect was found in canonical lifecycle delegation,
  guard/close ordering, the publish-after-build C export registry, callback
  and reaper serialization, or IPC cursor serialization. A fresh four-thread
  post-fix stress released and reaped 800 independent C export trees without
  a race, leak, or stranded source gate.
- No additional FlatBuffer, framing, dictionary-state, body-authority, or Core
  validation defect was found. A deterministic 30,000-case mutation pass over
  a nested dictionary-bearing stream accepted 1,953 cases. Every accepted
  stream materialized; all 28,047 rejected cases threw `ValidationError`. An
  independent 100,000-case pass accepted and materialized 8,053 cases with no
  unexpected failure type.
- Logical parent offsets and child offsets were rechecked against the
  [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html)
  and the canonical C++ slice model. Children correctly keep their own base
  offsets while a sliced parent selects the logical child positions. The
  suspected double-offset defect was withdrawn.
- Foreign C allocation extents, pointer-table extents, NUL termination, and
  producer callback behavior remain trusted ABI declarations under the
  [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
  Borrowed Julia vectors must not be mutated or resized, and mapped files must
  not be externally truncated. These remain documented boundaries.
- View/ListView/REE semantic work, padding and unused-bit checks, IPC
  compression and endian normalization, file footer/index support, facade
  work, native foreign-thread C callbacks, and the other README exclusions
  remain out of scope and fail closed where stated. The 32-bit ABI branch was
  source-inspected but not executed on the available 64-bit host.

## Validation

- `julia --startup-file=no core/test/runtests.jl`: 284/284 Core checks and
  404/404 four-thread lifecycle/cache checks passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl`: passed,
  including FlatBuffer verification, resource limits, body spans, cursor
  serialization, truncation boundaries, and dictionary snapshot checks.
- `julia --startup-file=no core/examples/cdata.jl`: passed, including the new
  ownership-handoff fault injections, forced-GC registry rooting, ABI layout,
  move semantics, source pins, malformed-topology cleanup, and empty-registry
  checks.
- A fresh four-thread C export/release/reap stress covered 800 trees, 1,600
  roots, and 1,600 callbacks. All 800 live-pin close attempts were rejected;
  all 1,600 roots were reaped; all 800 source gates then closed; the registry
  ended empty; and no operation failed.
- All round-7 changes are confined to `core/`. Each logical change is a small
  commit with the requested `Co-Authored-By: Codex <codex@openai.com>` trailer.

VERDICT: FINDINGS
