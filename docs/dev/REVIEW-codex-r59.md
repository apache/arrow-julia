# Arrow.jl 3.0 code review — round 59

Date: 2026-08-18

Scope: exact commit `95d24806a3efd1124cbd2871e18171db4032916b`
(`fix: resolve round 58 findings — zero-buffer views, length refusal,
ArrowStrings CI, prose`) on `core-rewrite`. Round 58 reviewed through
`8322039f3db605af9044764f2469304d0168d69f` and recorded one MEDIUM plus
four LOW findings in `docs/dev/REVIEW-codex-r58.md`. I reviewed the complete
two-commit `8322039..95d2480` delta:

- `d00e4c9` — record the round-58 review;
- `95d2480` — implement the five claimed round-58 closures.

All Tables-dependent checks used the clean `jq/scan` checkout at
`ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`.

## Result

Round 59 is not clean. The zero-buffer write, payload-range validation, local
ArrowStrings compatibility, public type behavior, and Stream behavior all
pass. I found no new product-code correctness or safety issue in the delta.

Two LOW closure gaps remain:

1. the ArrowStrings `min` CI cells resolve the root Arrow.jl Julia 1.12
   floor, not ArrowStrings' Julia 1.10 floor;
2. the repaired manual still says every composite and wrapper uses dynamic
   materialization and has a data-independent element type, although closed
   REE/dictionary wrappers use the typed route and heterogeneous unions
   intentionally narrow from observed rows.

Thus round-58 findings 3 and 4 are only partly closed. Findings 1, 2, and 5
are closed.

## Findings

### 1. LOW — ArrowStrings has no durable package-minimum CI lane

The regular package matrix now includes ArrowStrings at
`.github/workflows/ci.yml:84-95`, and the later build and test actions receive
`matrix.pkg.dir` at lines 117-130. The `setup-julia@v3` step at lines 103-106,
however, receives only `version`.

The [setup-julia v3 input contract](https://github.com/julia-actions/setup-julia#inputs)
says that its `project` input selects the project used to resolve versions
such as `min`, and defaults to `JULIA_PROJECT` or `.`. The
[action definition](https://github.com/julia-actions/setup-julia/blob/v3/action.yml)
states the same default. This workflow sets neither input nor environment
value. Its `min` cells therefore read root `Project.toml:40-48`, whose Julia
compat is `1.12`, instead of `src/ArrowStrings/Project.toml:23-24`, whose
compat is `1.10`.

Consequently, the job named `ArrowStrings.jl - Julia min` installs a Julia
1.12 release. The current `lts` cells do install Julia 1.10, so current CI
does exercise the supported minimum today. Both exact local suites also
pass 2,495/2,495. That does not make the named minimum lane durable: `lts` is
a moving alias, and the Julia 1.10 coverage disappears when the LTS advances.

The nightly matrix and both monorepo develop sets correctly include
ArrowStrings at `.github/workflows/ci_nightly.yml:30-39,102-110` and
`.github/workflows/ci.yml:164-172`.

Disposition: open. Pass `project: ${{ matrix.pkg.dir }}` to setup-julia, or
add an explicit ArrowStrings Julia 1.10 matrix entry that remains after the
LTS alias changes.

### 2. LOW — the manual still overstates the wrapper execution and type rule

The revised table at `docs/src/manual.md:169-176` gives the requested public
types. The advisory-null paragraph at lines 178-184 is also correct. The
lead-in at lines 164-167 still makes two blanket claims that the
implementation and its own table contradict.

First, it says every composite and wrapper layout is read on the dynamic
path. `_typedroutable` deliberately unwraps closed dictionary and
run-end-encoded value fields at `src/table.jl:249-256`.
`_batchcolumn` selects typed materialization when that route and the declared
claim are closed at lines 266-274. A direct route probe produced:

```text
REE<Int64>:       closed=true, routable=true, dynamic eltype=Any, batch eltype=Int64
Dictionary<Utf8>: closed=true, routable=true, dynamic eltype=Any, batch eltype=String
```

REE<Int64> alone disproves the blanket wrapper statement in the paragraph's
own table domain.

Second, the lead-in says the element type is the same for zero-row,
all-`missing`, and populated wrapper columns. The Union row at line 174
correctly states the exception: a heterogeneous union whose declared join is
`Any` narrows from its rows. The direct probe showed:

```text
heterogeneous Union: zero=Any, Int-only=Int64, mixed=Any, all-missing=Missing
```

The later advisory-null paragraph correctly documents the other exception,
so I do not count either qualification as a separate finding. The problem is
the unqualified summary sentence immediately before them.

Disposition: open. Limit the dynamic-path statement to unresolved composite
domains. State that closed dictionary and REE wrappers retain the child
route. Qualify type stability for heterogeneous unions and for undeclared
physical nulls.

## Round-58 closure checks

### Zero-buffer Utf8View write — closed

`fromcompactviews` now permits an empty data-buffer vector at
`src/ArrowCore.jl:2779-2807`. The facade regression at
`test/facade_tests.jl:1083-1096` pins both the builder and serialized file
shape.

An independent 9-check probe built a non-nullable, all-inline
`CompactStringVector` with zero source buffers. It exited 0 and reported:

```text
source variadic buffers = 0
builder buffers         = 2
file batch buffers      = 2
readback                = ["", "abcdefghijkl"]
```

The two serialized buffers are the fixed validity and views pair. There is
no variadic data buffer.

### ArrowStrings payload ranges — closed

`inline_payload` enforces `0:12`, and `view_payload` enforces
`13:typemax(Int32)` before packing at
`src/ArrowStrings/src/ArrowStrings.jl:89-135`. The committed pins are at
`src/ArrowStrings/test/runtests.jl:120-142`.

The independent 20-check edge probe exited 0:

- inline lengths 0 and 12 were accepted; -1 and 13 were refused;
- view lengths 13 and 2,147,483,647 were accepted; -1, 12, and
  2,147,483,648 were refused;
- buffer index and offset accepted exact `typemax(Int32)` and refused
  negative values and `typemax(Int32) + 1`;
- rebasing accepted result offsets 0 and `typemax(Int32)` and refused
  negative and overflowing results;
- the oversized length never became a negative/null payload.

### ArrowStrings compatibility and allocation probes — behavior closed; finding 1 is CI durability

The allocation measurements are now top-level typed functions at
`src/ArrowStrings/test/runtests.jl:63-68`. Their hash, comparison, and access
assertions pass at lines 209-212 and 275-276.

The standalone package suite passed 2,495/2,495 under both Julia 1.12.6 and
the requested exact Julia 1.10.11 binary. ArrowStrings is present in both
package matrices and monorepo develop sets. Finding 1 records only the
incorrect project used by the regular matrix's named `min` cells.

### Facade type mapping — behavior closed; finding 2 is prose accuracy

The requested facade matrix passed in all four empty-source forms:
file/schema-only, file/one-zero-row-batch, stream/schema-only, and
stream/one-zero-row-batch.

```text
REE<Int64>         Int64
REE<Date32>        Int32
REE<Binary>        Vector{UInt8}
List               Vector{Any}
Struct             Vector{Pair{String,Any}}
Null               Missing
homogeneous Union  Int64
heterogeneous      Any
```

Advisory physical nulls widened Int64 and REE<Int64> to
`Union{Missing,Int64}` and List to `Union{Missing,Vector{Any}}`. A conforming
non-nullable Int64 column stayed `Int64`. Thus the table and advisory-null
paragraph are correct. Finding 2 records the separate false route statement
and unqualified type-stability lead-in.

### Stream buffering and decoding prose — closed

The manual at `docs/src/manual.md:114-124` and the `Stream` docstring at
`src/table.jl:752-769` now distinguish source buffering from record-batch
decoding correctly.

The independent three-way probe reported:

```text
stream IO: consumed=784/784; IPCStream; decoded=3; later byte damage ignored
file IO:   consumed=1042/1042; ArrowFile; lazy blocks=3; batch-2 damage observed at iteration
file bytes: retained by identity; ArrowFile; lazy blocks=3
file path:  ArrowFile; mmap release present; lazy blocks=3
```

Stream-format input was fully consumed and decoded at construction.
File-format IO was fully consumed but its record blocks decoded on access.
The byte-vector source stayed retained by identity with lazy record blocks.
The path source used the mmap-backed file route. The larger-than-RAM advice
is therefore accurate.

## Two-commit delta review

- `d00e4c9` adds only the 442-line round-58 review record.
- `95d2480` changes eight implementation, test, workflow, and manual files:
  91 additions and 42 deletions.
- The zero-buffer change removes only the artificial nonempty-vector guard;
  structural and semantic validation remain in their existing layers.
- The payload guards run before packing, cover both sides of the inline/view
  boundary, and retain the exact Int32 word limits.
- The type-stable allocation wrappers measure the intended kernels and pass
  on both supported Julia versions tested.
- Both CI YAML files parse successfully. Finding 1 is semantic, not syntax.
- The Stream wording matches the construction and iteration paths.
- I found no other correctness, safety, portability, performance, test, or
  documentation issue in the delta.

## Assumptions and decisions

- I treated the exact committed tree as the authority. Mutable environment
  setup and probe files stayed in detached scratch worktrees or temporary
  environments.
- I treated the decoded file batch's two-buffer array as the serialized wire
  layout. It is reconstructed from the file record-batch metadata and body,
  not reused from the pre-write builder.
- I treated Julia 1.10 as a supported floor that needs a durable CI selector.
  I did not treat today's separate `lts` alias as a substitute for the
  package-specific `min` lane.
- I treated the user manual as the public facade contract. A false statement
  about whether a wrapper uses typed or dynamic materialization is a LOW
  documentation defect even when the returned values and types are correct.
- I treated the Union table row and advisory-null paragraph as explicit
  exceptions, not additional findings. Their unqualified lead-in belongs to
  finding 2.
- I accepted the declared conformance skips. I required the final direct
  Documenter build to have no warning or error.
- The host was 64-bit arm64 macOS with Julia 1.12.6 and Docker 29.6.2.
- I did not modify product or test code in the main checkout. The protected
  untracked files `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`,
  `ISSUE-540.md`, `ISSUE-580.md`, and `mytestdata.arrow` remained untouched.
  This review document is the only main-checkout change made by this review.

## Validation

- Detached worktrees do not contain the ignored development manifest. The
  first root-package attempt therefore exited 1 at the known `Tables.Scan`
  prerequisite with released Tables. Developing the clean Tables checkout
  above into the scratch environment exited 0. The exact required command
  `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` then exited
  0: ArrowCore 420/420, threaded caches 4/4, facade 321/321, and IPC read,
  IPC write, C data, and ranged-scan acceptance 1/1 each.
- `julia --project=src/ArrowStrings --startup-file=no -e 'using Pkg;
  Pkg.test()'` — exit 0, 2,495/2,495 on Julia 1.12.6.
- The same ArrowStrings command under the requested exact Julia 1.10.11
  binary — exit 0, 2,495/2,495.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0, 6/6;
  compile plus executable passed with zero verifier warnings and zero
  verifier errors.
- `julia --startup-file=no conformance/run.jl` — exit 0: corpus 275 pass / 0
  fail / 36 declared skips; PyArrow 25.0.1 and nanoarrow 0.9.0 IPC oracle
  170 / 0 / 43; C data and stream oracle 143 / 0 / 9. The driver reported
  corpus, oracle, and cdata `PASS`.
- A clean temporary docs environment developed exact Arrow and the clean
  Tables checkout, then instantiated successfully. Direct `makedocs` with
  `remotes=nothing`, the repository remote, and an explicit HTML repository
  link exited 0 with zero warnings and zero errors.
- Independent zero-buffer probe — exit 0, 9/9. Payload-edge probe — exit 0,
  20/20.
- Independent facade/Stream/manual probe — exit 0, 315/315. The requested
  closure subset passed 307/307: facade 256, advisory nulls 26, Stream 22,
  and three aggregate assertions. The remaining eight checks exposed and
  pinned finding 2.
- `git diff --check` and
  `git diff --check
  8322039f3db605af9044764f2469304d0168d69f..95d24806a3efd1124cbd2871e18171db4032916b`
  — exit 0 before this report. A separate whitespace and final-verdict check
  covers this untracked report after writing.
- Final branch HEAD remained
  `95d24806a3efd1124cbd2871e18171db4032916b`.

VERDICT: FINDINGS
