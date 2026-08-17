# Arrow.jl 3.0 code review — round 42

Date: 2026-08-17

Scope: exact fix commit `30066aa6bdc6a7a355b73beace1b84687d23869c`
on `core-rewrite`. Its parent,
`4882625427a6131113a4c5774fc55c4ee4add34b`, records round 41 against
code commit `621ea4581038c1ebe6c0fa80627c78d297be811e`. I used the
manifest-selected Tables.jl checkout on `jq/scan` at
`d1fbb6eb577741688dba70039754166b51c1cdcc` as the authority.

## Result

Round 41 is not clean. One MEDIUM finding remains.

The direct declared-type additions work. ListView, every Decimal width, and
every Interval unit make the same empty and nonempty keep decision on file,
stream, and ranged paths. Dictionary of REE of Binary now reaches the values
child. One-child, all-integer, missing-child, and zero-child Union controls
also behave as intended. Kept exotic layouts refuse natural rewrites cleanly
and publish no bytes.

The Field-aware composition rule is still not the type authority used by the
facade. It composes child facade domains. The actual pre-override column uses
conversion selected by the root descriptor and Julia's observed container
type join. Temporal children under REE or Union therefore remain raw storage
integers, while the declared rule reports `Dates.Date`. A valid mixed
heterogeneous Union can also materialize as `Vector{Any}` even when the union
of its child domains is a subtype of the requested target. Both cases restore
the empty/nonempty retained-schema split that round 41 required the fix to
remove.

All five required gates pass. The full round-38/39/40/41 clean set also stays
clean.

## Findings

1. **MEDIUM — composed declared domains do not match the actual facade
   container type.**

   `_boundschema` uses `_declaredeltype(f)` for an empty pre-override column
   but `eltype(precols[i])` for a nonempty column at `src/table.jl:584-592`.
   The declared path recurses through REE, Dictionary, and Union children at
   `src/table.jl:546-556`; temporal leaves then fall through to the facade
   mapping at `src/table.jl:575-582` and `src/table.jl:190-202`.

   The materialized path is different. `_postconvert` dispatches on the root
   descriptor at `src/table.jl:157-184` and has no REE or Union method.
   `_wrapscanned` invokes that root conversion and builds the pre-override
   column at `src/table.jl:616-627`. Core Date32 returns `Int32` at
   `src/ArrowCore.jl:1856-1860`; Union and REE forward the winning child value
   at `src/ArrowCore.jl:2009-2019` and `src/ArrowCore.jl:2065-2066`.
   Consequently, REE<Date32>, Union<Date32>, and
   Dictionary<REE<Date32>> declare `Dates.Date` but materialize nonempty
   facade columns with `eltype == Int32`.

   Under `=> Integer`, all nine empty file/stream/ranged cases dropped their
   retained field and all nine nonempty cases kept it. The focused direct REE
   and Union control also tested `=> Dates.Date`: all six empty cases
   succeeded and retained the exotic field, while all six nonempty cases
   refused with `MethodError`. Values, output element types, and refusals
   matched `Tables.finish`; retained identity and empty/nonempty acceptance
   did not.

   The same root appears with genuinely heterogeneous winners.
   `Union<Int64,Utf8> => Union{Integer,AbstractString}` has declared domain
   `Union{Int64,String}`, so all three empty paths retain the field. An
   Int-only population also retains it. A valid mixed Int/String population
   widens through `map(identity, converted)` at `src/table.jl:214-221` to an
   `Any` pre-column, so Tables performs a real container conversion and all
   three paths drop the field. The manifest Tables authority makes its no-op
   decision from the actual container `eltype` at
   `~/.julia/dev/Tables/src/scan.jl:525-530`.

   The regression pins at `test/facade_tests.jl:782-823` cover only
   compositions whose Core and facade domains agree. They do not cover a
   temporal child under a transparent wrapper or mixed heterogeneous Union
   winners.

   This is the same retained-identity and empty/nonempty class as round 41, so
   I keep its MEDIUM severity. No wrong values were emitted. A root fix needs
   one shared authority for the actual pre-override container type. It must
   either convert temporal wrapper children or declare their raw domains, and
   it must keep multi-child Unions conservative when a valid mixed population
   widens beyond the mathematical union of child types.

## Closed portions of round 41

- The declared-base table now covers every registered closed Core `_value`
  materializer. Its ListView, Decimal, and Interval entries match the methods
  at `src/ArrowCore.jl:1870-1901` and `src/ArrowCore.jl:2052-2062`.
- ListView, Decimal, and Interval retention passes 27/27 for empty inputs and
  27/27 for nonempty inputs. Their natural rewrites refuse 54/54 in each
  state with `ArgumentError` and zero published bytes.
- Dictionary<REE<Binary>> retention passes 3/3 in each state. Its rewrites
  refuse 6/6 in each state.
- The one-child Union controls pass 6/6 in each state. Their rewrites refuse
  12/12 in each state. `Union<Int64,Int32> => Integer` passes 3/3 in each
  state.
- `Union<Missing,Int64> => Integer` passes 3/3 in each state. A zero-child
  Union declares `Any` and converts/drops 3/3. A one-Null-child Union declares
  `Missing`.
- A 64-level declared-type walk completes. An 80-level IPC schema rejects in
  the metadata verifier with `metadata nesting exceeds limit`. IPC input is
  bounded by `Limits.max_nesting_depth = 64` at `src/ipc_read.jl:69-77` and
  `src/ipc_read.jl:136-138`. C Data preflight has the same bound at
  `src/cdata.jl:1046-1068`.
- Ordinary `Field` construction defensively copies children through
  `FrozenVector` at `src/ArrowCore.jl:416-427` and
  `src/ArrowCore.jl:539-541`. A self-cycle requires private backing mutation;
  it is not reachable through accepted IPC or C Data input.

## Clean regression sweep

- Frame integrity, cumulative file and ranged budgets, charge-before-fetch,
  `limit=1` laziness, later-block early stop, and every `OpNode` control pass
  40/40.
- The file header charge remains 4,688 bytes. The ranged footer charge is
  5,984 bytes and one ranged header costs 4,776 bytes. The exact 10,760-byte
  cap succeeds. The former 10,672-byte cap rejects.
- Orphan-dictionary rejection passes 36/36 zero-field assertions and 16/16
  semantic controls. Unknown id, delta, duplicate id, and inner RecordBatch
  mismatch all reject before a filter or window can hide them.
- The prior small-list matrix passes: Tables authority 6/6, retention 6/6,
  and rewrites 12/12. Expanded list and conversion controls pass shapes
  42/42, conversions 30/30, retention 30/30, rewrites 60/60, parity 5/5,
  replacement errors 24/24, and Bool preservation 6/6.
- Direct composite authority and retention pass for Binary, FixedSizeList,
  Struct, and Map on empty and nonempty inputs. Nested values pass 36/36,
  exact recursive schemas pass 24/24, and large-parent rewrites pass 12/12.
  Focused list imposition edges pass 18/18.
- The direct zero-field matrix passes 57/57. Reject windows pass 10/10.
  Overflow and residual controls pass 18/18. Pathological-name controls pass
  17/17.
- Allocation is flat between 10,000 and 1,000,000 declared rows: file 5,488
  bytes, stream 1,792 bytes, and ranged 18,880 bytes at both sizes.

## Assumptions and decisions

- I used `Tables.apply`, `Tables.finish`, and `Tables.bind` from the
  manifest-selected checkout as the semantic authority. In particular, I
  treated its actual-container `eltype` check as the no-op/conversion rule.
- I treated layouts accepted by Core validation and both IPC adapters as
  supported layouts.
- I treated retained identity as the exact recursive Arrow descriptor, names,
  nullability, field metadata, and schema metadata.
- I grouped the temporal-wrapper and heterogeneous-Union symptoms as one
  finding. Both come from `_declaredeltype` predicting a domain different
  from the pre-override facade container used by the keep/drop authority.
- I accepted data-dependent retention for a genuinely heterogeneous Union
  when the observed nonempty container has a narrower type. I did not accept
  retaining an empty field when a valid all-target-compatible mixed
  population performs a real Tables container conversion.
- I treated the IPC and C Data depth gates as the accepted-input recursion
  boundary. I did not treat a cycle made by mutating private `FrozenVector`
  storage as a public-input defect.
- The host is 64-bit. I made no product or test changes. All probes ran from
  scratch directories. Two legacy probes pinned the round-40 SHA; I changed
  only that expected SHA in `include_string` scratch runs before rerunning
  them.
- The six pre-existing untracked files remain present and unmodified. This
  review is the only repository change.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0;
  ArrowCore 342/342, threaded caches 4/4, facade 259/259, and every adapter
  battery passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6, zero
  verifier errors, and the compiled binary run passed.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` —
  exit 0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` —
  exit 0; 170 pass / 0 fail / 43 skip.
- Round-42 declared-layout, Union, rewrite, and depth probe — exit 0; all
  requested closure counts and edge controls above passed.
- Temporal-composition reproduction probe — exit 0; the asserted mismatch
  reproduced on all file, stream, and ranged cases with the counts above.
- Tables authority controls — exit 0; 12/12.
- Round-40 scan closure probe — exit 0; 40/40.
- Dictionary adversarial probe — exit 0; 36/36 zero-field assertions and
  16/16 semantic controls.
- Prior small-list and expanded-facade probes, with only their scratch SHA pin
  updated — exit 0; every count above passed.
- Composite parity, nested-descriptor, and list-imposition probes — exit 0;
  every count above passed.
- Direct zero-field matrix — exit 0; 57/57.
- Reject-window controls — exit 0; 10/10.
- Allocation-flatness probe — exit 0; zero growth on file, stream, and ranged
  paths.
- Overflow and residual probe — exit 0; 18/18.
- Pathological-name probe — exit 0; 17/17.
- `git diff --check` — exit 0.

VERDICT: FINDINGS
