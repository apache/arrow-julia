# ArrowCore prove-out review — round 28

Date: 2026-08-15

Scope: commit `6b03ddbbd108facaa4df89d3d71347cd7fe742f5` under
`core/`, compared with parent `592620ab99c1cc5b7791f54fda75b8d57b4cbcc6`,
and judged only against the round-27 finding.

## Result

No findings. The staged generated API restores the pre-round-27 adapter's
constant-time metadata-version gate without dropping a generated check or
visiting the root twice.

## Round-27 probe and root accounting

I rebuilt the otherwise-valid one-Int Schema Message and empty-Schema file
Footer used by the round-27 probe, changed each root version to wire value
`2`, and set the verifier reserve limit to exactly one table charge, 2048
bytes. The focused probe exited 0 and returned:

```text
message_type=ValidationError
message_error=ValidationError("invalid IPC FlatBuffer: unsupported metadata version 2 (only V4/V5 are accepted)")
footer_type=ValidationError
footer_error=ValidationError("invalid IPC FlatBuffer: unsupported footer version 2 (only V4/V5 are accepted)")
```

These match the pre-round-27 adapter baseline's exception type and exact
diagnostics. The Message wrapper runs `verifyrootstart_Message`, reads the
verified version, applies the adapter gate, and only then runs
`verifyrootrest_Message`
(`core/examples/ipc_read.jl:192-205`). The Footer wrapper has the same order
(`core/examples/ipc_write.jl:838-848`). No reference-directed traversal can
run before either unsupported-version rejection.

For a valid V5 Schema Message, the inline stage ended at one object and 2048
reserved bytes. The reference stage ended at five objects and 9857 bytes.
The composed `verifyroot_Message` ended at the same `(5, 9857)`. The adapter
also passed with the exact limits `max_metadata_objects=5` and
`reserve_limit=9857`; a second root visit or charge would have exceeded those
limits. The complete header graph therefore runs once, while the root itself
is neither revisited nor recharged.

## Complete stage partition

I compared the generated walkers with the parent at normalized Julia-AST
statement level. The audit covered all 35 table walkers and all 53 active
schema fields. The parent's 66 field-check statements partition into 29
inline statements and 37 reference statements. For every table:

- the combined-stage statement multiset exactly equals the parent walker;
- neither stage overlaps the other;
- each stage is an order-preserving subsequence of the parent walker; and
- each field's internal check order is unchanged.

The schema-field inventory also balances exactly:

- inline: 5 booleans, 13 enums, and 11 scalars;
- refs: 4 strings, 1 enum vector, 2 scalar vectors, 4 struct vectors, 5 table
  references, 6 table vectors, and 2 unions.

Both union fields keep their tag lookup, tag requirement/domain checks, value
reference, and complete dispatch together in refs
(`core/tools/fbsgen.jl:511-538`). Enums, booleans, scalars, and direct inline
structs go only to inline (`core/tools/fbsgen.jl:539-546,568-569`). Strings,
vectors, and table references go only to refs
(`core/tools/fbsgen.jl:547-573`). The pinned schemas have no direct
struct-valued field; their four vectors of inline structs correctly remain in
refs because the vector itself is a reference.

The deliberate cross-field order is now all inline fields followed by all
reference fields. Within each stage, schema order is preserved. Within each
field, the exact parent checks and their order are preserved.

## Accounting and nested parity

All 35 `verifyinline_T` functions contain exactly one `_vtable`, one
`_vvisit!`, and one depth check. All 35 `verifyrefs_T` functions bind
`bytes = t.bytes` and contain no table reconstruction, visit, or depth check.
All 35 composed `verify_T` functions call their inline and reference stages
once. Each root start performs the parent's root-prefix checks and calls only
inline; each root rest calls only refs at depth zero; each complete root calls
start and rest once (`core/tools/fbsgen.jl:578-617`).

Nested table references and table vectors still call the composed
`verify_T`, not either partial stage (`core/tools/fbsgen.jl:526-529,562-564,
570-573`). A focused current-versus-direct-parent verifier probe confirmed
identical valid accounting for Message `(5, 9857)`, nested Schema `(4, 7809)`,
and a separate one-Int Footer `(5, 10369)`. With
`max_nesting_depth=0`, both revisions rejected after visiting the nested
Schema, at `(2, 4096)`, with `metadata nesting exceeds limit`. Nested
traversal and valid-input accounting are unchanged apart from the deliberate
cross-field stage order described above.

The returned `VTable` retains a strong reference to the original byte vector
through its `bytes` field, and refs recovers that same vector from `t.bytes`.
The stage boundary does not weaken the constrained GC-reachability model.

## Regeneration and constraints

`julia --startup-file=no core/tools/fbsgen.jl core/metadata/fbs core/metadata`
exited 0. The required `git diff --exit-code core/metadata` then exited 0.

The target commit changes only these four `core/` files:

- `core/tools/fbsgen.jl`
- `core/metadata/Verifier.jl`
- `core/examples/ipc_read.jl`
- `core/examples/ipc_write.jl`

It does not change `core/ArrowCore.jl`, the four `_of` ladders, dependency
files, or the Tables development dependency. The five pre-existing untracked
files were not modified.

## Assumptions and decisions

- I treated the three pinned FlatBuffers schemas as the generator scope, as
  in round 27.
- I used the one-Int Schema Message and empty-Schema file Footer from round 27
  as the otherwise-valid version-gate fixtures.
- I treated the explicit all-inline-then-all-refs cross-field order as part of
  the requested design. I required every field's own checks and order to
  remain identical.
- I used exact object and reserve limits to make any duplicate root visit or
  charge fail visibly.
- This was a review-only task. I added this report and made no product fix or
  commit because the convergence bar was met.

## Validation

- Focused Message/Footer version-gate and exact-accounting probe — exit 0.
- Current-versus-direct-parent nested/accounting parity probe — exit 0.
- All-table normalized-AST partition audit — exit 0; 35/35 tables, zero
  omissions, overlaps, or order failures.
- `julia --startup-file=no core/test/trim_compile_tests.jl` — exit 0; 6/6.
- `julia --startup-file=no core/test/runtests.jl` — exit 0; 325/325 Core and
  4/4 threaded-cache tests.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_read.jl`
  — exit 0.
- `julia --project=core/conformance --startup-file=no core/examples/ipc_write.jl`
  — exit 0.
- `julia --startup-file=no core/examples/cdata.jl` — exit 0, including the
  four-thread child.
- `julia --project=core/conformance --startup-file=no core/examples/scan_ranges.jl`
  — exit 0 on the unchanged third process, with all three scan sections green.
  The first two processes reproduced the known idle scheduler stall and were
  interrupted after more than two minutes; each real exit code was 130.
- `julia --project=core/conformance --startup-file=no core/conformance/corpus.jl`
  — exit 0; 275 pass / 0 fail / 36 skip.
- Docker daemon and local-image checks — exit 0.
- `julia --project=core/conformance --startup-file=no core/conformance/oracle.jl`
  with local `arrow-conformance-oracle:latest` — exit 0; 170 pass / 0 fail /
  43 skip.
- Required regeneration — exit 0; required metadata diff — exit 0.

VERDICT: CLEAN
