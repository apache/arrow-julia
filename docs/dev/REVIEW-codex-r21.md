# ArrowCore prove-out review — round 21

Date: 2026-08-15

Scope: the round-21 commits `c08f9d2`, `ce8a09a`, `026bb15`, `779f9bc`,
`578c402`, and `75acaab`, plus dispositions through `bd0bbaf`. The Arrow
Columnar Format, Schema FlatBuffer schema, Message FlatBuffer schema, and C
Data Interface are the layout authorities.

## Findings and dispositions

1. **MEDIUM — ListView skipped reachable child Field contracts.** Intrinsic
   validation checked every offset/size pair, including null slots, but
   `_validate_field_contract_at` walked only List, Map, fixed-size list, and
   struct ranges. A valid ListView slot could therefore expose `missing`
   through a child Field declared `nullable=false`. Disposition: fixed in
   `5b28e44`. The walker now validates the selected `_listview_range`, keeps
   null-parent masking, and accepts a zero-size list at the child-length
   offset. All three cases have regressions.

2. **MEDIUM — valid nested run-end encoding was rejected.** Core structural
   validation and IPC schema validation both rejected REE whose values child
   was REE. This contradicted the [Columnar Format](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout),
   which permits any values array type, and the corresponding
   [Schema.fbs declaration](https://github.com/apache/arrow/blob/main/format/Schema.fbs).
   The generic access, null, validation, IPC, and C Data recursion already
   supported the shape behind those guards. `_statfold` also unwrapped only
   one dictionary/REE layer, so legal nested values lost usable bounds.
   Disposition: fixed in `ab96f18`, `38a1aad`, and `0cfa693`. Core, both IPC
   formats with plain and zstd bodies, C Data, and nested REE/View statistics
   now have positive coverage.

3. **MEDIUM — ranged dictionary decoding dropped variadic counts.** The
   dictionary-batch `DecodeCursor` in `scan_ranges.jl` omitted
   `variadics=variadiccounts(rb)`. Eager decode was correct, but a selected
   dictionary with View values failed even when its legal count was zero.
   A following plain field confirmed that the failure was cursor skew, not
   value semantics. Disposition: fixed in `7c6ca5f`; ranged dictionary View
   decoding and the later field now round-trip together.

4. **MEDIUM — ranged buffer-span totals could wrap.** `_recordbatchmeta`
   used an unchecked `sum` over per-field spans. Three View counts
   `[typemax(Int64)-2, typemax(Int64)-2, 6]` produced wrapped total `6`, equal
   to the six fixed buffers. A `limit=0` scan could then accept the corrupt
   batch because its body was outside the window. Disposition: fixed in
   `7c6ca5f`. `_bufferspan` remains Int64 throughout, every recursive and
   top-level addition uses `_planadd`, and host-Int conversion is guarded.
   The exact wrap now rejects before window exclusion.

5. **LOW — the raw descriptor fallback was shadowed.** The new
   `_validate_descriptor(::Any)` fallback could not reject an unregistered
   `ArrowType`, because `_validate_descriptor(::ArrowType)=nothing` was more
   specific. This made the recorded raw-method-table protection incomplete.
   Disposition: fixed in `a51568e`. Built-in descriptor no-op methods are now
   explicit, and an unregistered subtype reaches the throwing fallback.
   `layoutspec` and `_value` fallbacks were already correct. A separate
   `_materialize_loop(::Any, ...)` method is unnecessary: the public/raw
   `_materialize_of` ladder rejects unknown descriptors before the constrained
   loop.

6. **LOW — an empty sliced REE could require a physical run.** Structural
   validation used `offset + length == 0` to identify an empty array. Thus
   `length=0, offset>0` with empty physical children was called nonempty.
   Disposition: fixed in `ab96f18`. Physical runs are required only when
   logical length is nonzero; representability of `offset + length` remains
   checked.

7. **LOW — IPC schema-only paths omitted prescribed REE child names.** They
   checked child count, run-end type, width, signedness, and nullability, but
   accepted names other than `run_ends` and `values`. Core caught this only
   once an array arrived, so a schema-only stream or file could carry the
   invalid schema. Disposition: fixed in `38a1aad`; schema-only stream and
   file refusals are pinned.

8. **LOW — active status text lagged the implementation.** Module and type
   docs still described View, ListView, and REE as structural-only; an IPC
   comment kept them outside accessor coverage; the README stopped its review
   index at round 18 and could imply that all six dispatch ladders were
   collapsed. Disposition: fixed in `38a1aad` and `2bb7337`. The experiment
   text now says that the four `_of` ladders were collapsed.

9. **LOW — the exact scan keep-green command stalled during test
   compilation.** With Julia 1.12 and the normal 10-thread environment, the
   process completed Stage A and byte-range checks, then remained idle before
   entering the monolithic statistics acceptance function. Replaying its
   operations at top level and running with one thread both passed. The
   compile trigger was specialization of the legacy Arrow 2.x `Arrow.Table`
   constructor inside the large function. Round-21 disposition: `bd0bbaf`
   split the acceptance into bounded, non-inlined compile units and put the
   legacy constructor behind one narrow `invokelatest` barrier. Round 22 found
   that the exact command still stalled when the large Stage-A driver compiled
   before the aggregate statistics driver. Commit `a08dcb2` completed the fix
   by running the independent statistics group first.

Because this round found issues, it does not meet the zero-finding convergence
bar even though every finding above is fixed.

## Closing checks

- **The ladder experiment evidence is accurate.** Replacing only
  `layoutspec_of` with the recorded plain forward reproduced the unresolved
  `layoutspec(d.type::ArrowType)::Any` verifier site and a 2/6 gate. Replacing
  all four `_of` ladders also produced a 2/6 gate, with downstream `Any`
  cascades. The verifier did not enumerate the closed method table. No part of
  the collapsed forwards was worth retaining beyond the corrected throwing
  fallbacks. The ladders remain unchanged.
- **View semantics match the format within the documented validation tier.**
  Valid slots reject negative lengths. Length alone selects the representation:
  12 is inline and 13 is out-of-line, so there is no independent
  “out-of-line with length <=12” tag to reject. Prefix comparison runs only
  for long entries. Signed buffer indexes and offsets, checked half-open
  containment, parent offsets, and unrestricted null-entry bytes are correct.
  The new tests pin 12/13 and a sliced physical view entry. Canonical unused
  inline-byte padding remains explicitly disclosed production work, not a
  hidden support claim.
- **ListView and REE geometry are clean after disposition.** ListView uses
  Int32 or Int64 by `large`, checks every slot, accepts overlap and unordered
  ranges, applies `d.offset`, and permits `(child_length, 0)`. REE accepts only
  signed Int16/32/64 run ends, rejects run nulls, requires positive strict
  ascent and final coverage of `offset + length`, routes logical nulls through
  arbitrarily nested values, and finds exact sliced run boundaries correctly.
- **The IPC walks are aligned.** Focused mixed nesting covered two View
  fields, View below struct/list/union/REE, a later plain field, dictionary
  View values, and nested REE/View values. The depth-first probe consumed 13
  nodes, 29 buffers, and counts `[1,2,1,1,1,1]` with no leftover. Stream,
  file, and selected ranged results matched. The permanent exotic fixture
  carries counts `[2,0]` for a top-level View and a View below nested REE.
- **The `[long]` bridge and slot arithmetic are correct.** `variadiccounts`
  uses vtable offset 12 for RecordBatch slot 4 and `FB.Array{Int64}`, whose
  pointer stride is eight bytes. Present, present-empty, and absent vectors
  return the correct Int64 results. The writer builds five slots and writes
  slot index 4. No `rb.variadicBufferCounts` access remains under `core/`.
- **C Data is clean.** Formats `vu`, `vz`, `+vl`, `+vL`, and `+r` match the
  [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html#binary-view-arrays).
  Export order is fixed buffers, N variadic buffers, then the Int64 sizes
  buffer; `n_buffers=fixed+N+1`. The sizes allocation is in the export ledger.
  Import permits a null sizes pointer only for N=0, rejects negative sizes and
  null nonempty buffers, and keeps declared nonnegative extents inside the
  documented trusted-ABI boundary. Non-View layouts still require exact
  arity, so the View lower-bound rule cannot admit their extra buffers.
- **Statistics are clean.** Logical REE null counts, sliced REE values,
  Utf8View String bounds, one-sided/NaN behavior, nested wrapper unwrapping,
  and `_maypass` pruning all passed focused and end-to-end checks.
- **Trim remains clean.** The REE width typeassert remains necessary. No new
  width-dependent call consumes a value narrowed only inside an `isa`/`||`
  condition. The final gate is 6/6 with zero verifier errors or warnings and
  a successful produced-binary run.

## Assumptions and decisions

- The constrained GC-reachability memory model and the dispatch ladders remain
  final. No lifecycle, cache, or facade machinery was added.
- Official Arrow format text overrides behavior implied by the local reader.
- C Data buffer allocations are a trusted ABI boundary. The importer rejects
  negative lengths and uses checked geometry, but does not invent an arbitrary
  maximum for a producer-declared nonnegative foreign extent.
- The documented canonical-padding gap remains out of this round. It is not
  needed for safe access, and the README does not claim that check.
- A finding discovered and fixed in this round still makes the verdict a
  findings verdict.
- Only tracked files under `core/` changed. The five unrelated untracked files
  were not modified.

## Validation

- `julia --startup-file=no core/test/runtests.jl` — 297/297 Core and 4/4
  threaded-cache tests passed.
- `julia --project=. --startup-file=no core/examples/ipc_read.jl` — passed.
- `julia --project=. --startup-file=no core/examples/ipc_write.jl` — passed,
  including nested REE/View stream and file round-trips, plain and zstd.
- `julia --startup-file=no core/examples/cdata.jl` — passed, including 26
  descriptor shapes and the four-thread child.
- `julia --project=. --startup-file=no core/examples/scan_ranges.jl` — the
  round-21 completion claim did not reproduce in round 22. Commit `a08dcb2`
  corrected the remaining order-dependent Julia 1.12 compile stall; see the
  round-22 report for the clean isolated-dependency validation.
- `julia --startup-file=no core/test/trim_compile_tests.jl` — 6/6; compile and
  produced-binary run passed after the final Core edit.
- Focused C Data probes pinned N=0/N=1 trailing-size shapes, ledger ownership,
  null-pointer rules, negative sizes, trusted `typemax(Int64)` extents, and
  exact non-View arity. Focused View/ListView/REE slice and boundary probes
  matched the permanent regressions.
- `git diff --check` passed. Both disposable ladder worktrees were removed.

VERDICT: FINDINGS
