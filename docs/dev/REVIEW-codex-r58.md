# Arrow.jl 3.0 code review — round 58

Date: 2026-08-18

Scope: exact commit `8322039f3db605af9044764f2469304d0168d69f`
(`fix: resolve round 57 findings — advisory nulls read, one public type
rule, self-bootstrapping driver`) on `core-rewrite`. Round 57 reviewed
through `1284eccda1114786b5e4dc07ae2b3b198fac3d14` and recorded three
MEDIUM plus three LOW findings in `docs/dev/REVIEW-codex-r57.md`. I
reviewed the complete three-commit `1284ecc..8322039` delta:

- `dd84277` — add the separately registered in-repository
  ArrowStrings.jl package and its zero-copy Arrow write route;
- `ae3dce9` — record round 57;
- `8322039` — resolve the six round-57 findings.

All Tables-dependent checks used the clean `jq/scan` checkout at
`ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`.

## Result

Round 58 is not clean. I found one MEDIUM and four LOW issues.

The round-57 behavior fixes work. Advisory nulls read on every requested
surface. Schema-only and zero-row-batch public types agree. The ordinary
conforming Int64 path does not run the new missing-value scan. The
conformance driver starts from a fresh checkout without `--project`. The
three one-suite commands and the current `fromcompactviews` design passage
are correct.

ArrowStrings also passes its 2,487-test current-Julia suite and broad
independent hostile-input checks. Hashing agrees with `String` on both the
`memhash` and `hash_bytes` Base paths. Typed inline/view access and iteration
allocate zero bytes. N-buffer selection, garbage null entries, root
identity, IPC round-trip, PyArrow `string_view`, and a separate JuliaC trim
probe all work.

The remaining issues are:

1. the new write route rejects a valid all-inline view column with zero
   variadic data buffers;
2. `view_payload` silently truncates an oversized Int32 length word into a
   null instead of refusing it;
3. the separate ArrowStrings package has no CI lane, and its declared Julia
   1.10 test suite is already red;
4. the manual still documents the pre-fix row-narrowed empty wrapper and
   composite types;
5. the corrected Stream warning now says non-mmap file inputs decode all
   batches eagerly, although they only buffer all source bytes eagerly.

All required round-58 gates exit 0. The extra Julia 1.10 ArrowStrings check
in finding 3 is not one of those current-Julia gates.

## Findings

### 1. MEDIUM — Arrow.write rejects legal all-inline CompactStringVectors with zero data buffers

An Arrow `Utf8View` array may have zero variadic data buffers when every
present value is inline. The existing IPC writer battery states and proves
that rule at `test/ipc_write_battery.jl:715-725`. Core structural validation
also accepts the fixed validity and views buffers with no variadic tail at
`src/ArrowCore.jl:1027-1031`.

The new vector overload of `fromcompactviews` instead rejects an empty
`buffers` vector unconditionally at `src/ArrowCore.jl:2779-2784`.
`Arrow._writecolumn` reaches that check directly for every
`CompactStringVector` at `src/write.jl:109-112`.

A valid all-inline column worked as an ArrowStrings vector and failed only
when it entered the advertised Arrow route:

```text
collect(String, column) = ["hi"]
Arrow._writecolumn("s", column) ->
ArgumentError: a view column needs at least one data buffer
```

The reproducer used
`CompactStringVector{CompactString}(payloads,
Vector{Vector{UInt8}}())` and exited 42 after confirming that exact
`ArgumentError`. The normal CSV-shaped two-buffer constructor is not
affected because it always supplies `buf` and `extra`, even when both are
empty. The new general N-buffer constructor and the Arrow format both admit
N = 0, so this remains a valid-input failure in the newly added API.

Disposition: open. Remove the artificial nonempty-buffer requirement. Add
an `Arrow.write` regression for an all-inline zero-buffer column and pin the
explicit zero variadic-buffer count on the wire.

### 2. LOW — view_payload truncates an out-of-range length into a null

The ArrowStrings module describes the content length as an Int32 word and
states the under-2-GiB limit at
`src/ArrowStrings/src/ArrowStrings.jl:28-42`. `view_payload` says it refuses
view words that do not fit Int32 at lines 113-125. Its implementation checks
the buffer index and offset, but it does not check `len`; line 129 stores
`len % UInt32`.

The exact Int32 edge probe produced:

```text
buffer index = typemax(Int32)       accepted
offset       = typemax(Int32)       accepted
buffer index = typemax(Int32) + 1   ArgumentError
offset       = typemax(Int32) + 1   ArgumentError
length       = typemax(Int32)       accepted
length       = typemax(Int32) + 1   accepted; decoded length = -2147483648
```

`rebase_payload` correctly accepted exact offset 0 and
`typemax(Int32)`, and refused negative and overflowing results. The missing
length check is not only a malformed intermediate value. The Arrow write
path treats every negative payload length as null when it builds validity.
An end-to-end probe requested length 2,147,483,648, passed full validation,
wrote the column, and read back `missing`:

```text
(requested = 2147483648, encoded = -2147483648,
 nullcount = 1, value = missing, eltype = Union{Missing,String})
```

This input is outside the documented buffer-size contract, so I rate the
missing refusal LOW. It must still fail rather than silently change data.

Disposition: open. Require a view length in
`INLINE_MAX + 1:typemax(Int32)` before packing the payload. Pin both Int32
boundaries and the overflow refusal.

### 3. LOW — ArrowStrings has no CI lane and its supported Julia 1.10 suite fails

ArrowStrings declares `julia = "1.10"` at
`src/ArrowStrings/Project.toml:23-24`. The regular test matrix lists only
Arrow.jl and ArrowTypes.jl at `.github/workflows/ci.yml:83-93`. The nightly
matrix does the same at `.github/workflows/ci_nightly.yml:30-37`. The
monorepo setup also develops only Arrow and ArrowTypes at
`.github/workflows/ci.yml:162-170` and
`.github/workflows/ci_nightly.yml:100-108`.

Thus the separately registered package's own 2,487 tests do not run in
minimum, LTS, current, prerelease, or nightly CI. Root Arrow cannot cover its
minimum because root Arrow itself requires Julia 1.12.

This is an observed compatibility gap, not only missing future coverage. In
a detached exact-HEAD worktree, the exact standalone gate under Julia
1.10.11 exited 1 with 2,485 pass and two failures:

```text
test/runtests.jl:193  @allocated(foldcshash(...)) == 0
                      observed 16 bytes
test/runtests.jl:259  @allocated(sumncodeunits(...)) == 0
                      observed 16 bytes
```

Independent type-stable functions over the same inline, view, hash, and
iteration paths allocated zero bytes on Julia 1.10.11. The two failures are
therefore test-harness portability failures, not evidence that the public
typed kernels allocate. They still make `Pkg.test()` red on a declared
supported release. Julia 1.12.6 passed 2,487/2,487, and Julia 1.13.0-rc1
also passed while exercising the `Base.hash_bytes` branch.

Disposition: open. Add ArrowStrings to the regular and nightly package
matrices, including its Julia 1.10 minimum. Put the allocation assertions in
fully type-stable measurement functions so that a supported-version test
run measures the kernel rather than local-scope boxing. The separate
ArrowStrings trim workload passed in this review, but the repository trim
gate still covers ArrowCore only.

### 4. LOW — the manual still states the removed row-narrowed empty-type rule

Round 57 finding 4 asked for one complete public type rule across
schema-only and zero-row-batch sources. HEAD implements that rule through
`_publictype`/`_publiccolumn` at `src/table.jl:211-228`, `_facadecolumn` at
lines 292-299, and `_declaredeltype` at lines 626-681. The requested matrix
confirms that the behavior is now stable.

The manual still says every composite and wrapper column narrows its element
type from observed rows, that a zero-row column has element type `Any`, and
that an all-missing column has element type `Missing` at
`docs/src/manual.md:161-164`. That is the contract the fix replaced.

The public facade now reports schema-derived types for both a schema-only
source and one zero-row batch:

```text
REE<Int64>                    Int64
REE<Date32>                   Int32
REE<Binary>                   Vector{UInt8}
List<Int64>                   Vector{Any}
Struct                        Vector{Pair{String,Any}}
Null                          Missing
homogeneous Union<Int64,...>  Int64
heterogeneous Union           Any
```

A populated nullable List containing only nulls reads with element type
`Union{Missing,Vector{Any}}`, not `Missing`. Direct ArrowFile/RangedFile
scans intentionally remain in the raw storage-domain executor and may use
`Any` for dynamic composites; the manual describes the public facade.

Disposition: open. Replace the row-narrowing paragraph with the actual
schema-derived public rule. State the declared row containers for composite
layouts, the union child join, transparent REE behavior, and when observed
advisory nulls widen a non-nullable declaration.

### 5. LOW — Stream prose confuses eager source buffering with eager batch decoding

The important round-57 memory correction is safe. Only a default
memory-mapped file path avoids holding the complete IPC source in memory,
and the manual now recommends only that path for a file larger than RAM.

The replacement wording goes further. The manual says every stream-format,
IO, and byte-vector input is "read to the end and fully decoded" at
`docs/src/manual.md:114-121`. The Stream docstring repeats the claim at
`src/table.jl:760-767`.

That is true for stream-format input. It is false for file-format IO and
byte inputs. `_openbytes` sends file-format bytes to `readfile` at
`src/table.jl:443-445`; the IO overload reads all bytes and then calls that
function at lines 454-455. `readfile` buffers the complete source but returns
an `ArrowFile` whose record blocks decode through `getindex`, as documented
and implemented at `src/ipc_write.jl:1209-1216` and 1334-1354.

A three-batch probe produced:

```text
stream-format IO: consumed all bytes; IPCStream; 3 batches decoded
file-format IO:   consumed all bytes; ArrowFile; 3 lazy record blocks
file-format bytes:                   ArrowFile; 3 lazy record blocks
```

Disposition: open. Say that stream-format inputs are fully decoded at
construction. Say that non-mmap file-format inputs are fully buffered but
retain lazy record-batch decoding. Keep the current larger-than-RAM warning.

## Round-57 closure checks

### Advisory Field nullability — behavior closed

An independent 40-check matrix covered five fixtures on all four entries:
`Arrow.Table`, facade scan, direct `ArrowFile` scan, and direct `RangedFile`
scan.

- A non-nullable Int64 field with physical values `[missing, 7]` read as
  `Vector{Union{Missing,Int64}}` everywhere.
- REE<Int64> with `[missing, 7]` in its non-nullable values child did the
  same.
- A non-nullable dictionary with a null-free pool stayed
  `Vector{String}`.
- A non-nullable dictionary whose pool held a null widened to
  `Vector{Union{Missing,String}}`.
- A conforming non-nullable Int64 batch stayed `Vector{Int64}`.

`_batchcolumn` widens its raw typed claim only after `_hasnulls`, which
checks physical nulls, REE's values child, and dictionary pools at
`src/table.jl:266-289`.

### Stream bounded-memory guidance — safety issue closed; finding 5 is wording accuracy

Stream-format IO consumed and decoded all three batches at construction.
The memory-mapped file path retained an `ArrowFile` and decoded a record
batch only when indexed. The manual now limits the larger-than-RAM advice to
that mapped path. Finding 5 records the separate buffering-versus-decoding
overstatement for non-mmap file inputs.

### Fresh conformance driver — closed

In a tracked-only archive with an empty depot,
`julia --startup-file=no conformance/run.jl __bootstrap_probe__` activated
and instantiated `conformance/host`, precompiled Harbor 1.1.0, loaded it,
and reached the deliberate unknown-suite rejection. It did not fail at
`using Harbor` and needed no `--project`.

The full required driver then passed from a separate detached exact-HEAD
worktree: corpus 275/0/36, IPC oracle 170/0/43, and C-data oracle 143/0/9.

### Empty facade types — behavior closed; finding 4 is the stale manual contract

For each of the eleven requested scalar layouts, I compared schema-only and
one-zero-row-batch sources in file and stream format. I checked the facade,
facade scan, direct ArrowFile, and direct RangedFile surfaces as applicable.
All matched:

- Binary, LargeBinary, FixedSizeBinary, BinaryView -> `Vector{UInt8}`;
- Decimal32 -> `Int32`; Decimal64 -> `Int64`;
- Decimal128 and Decimal256 -> `Vector{UInt8}`;
- year-month Interval -> `Int32`;
- day-time and month-day-nano Interval -> their declared `NamedTuple`s.

REE<Int64>, REE<Date32>, REE<Binary>, heterogeneous Union, List, Struct,
Null, and homogeneous Union also kept the same type between schema-only and
zero-batch sources. Finding 4 records the manual text that still describes
the old behavior.

### One-suite commands and fromcompactviews design passage — closed

`docs/dev/core-README.md:55-58` contains three executable one-suite
commands, one each for `corpus`, `oracle`, and `cdata`.

The current design passage at `docs/dev/core-README.md:172-179` says the
payload vector is the views buffer, every data buffer is retained by
identity, only validity is built, and long-entry geometry is checked during
semantic/full validation. That matches the implementation.

## `_publictype` scan guard

`_publictype` returns before `any(...)` when its declared type is `Any` or
already admits `Missing`. It also short-circuits before `any(...)` when the
input column's element type cannot admit `Missing` at
`src/table.jl:219-223`.

- A ten-million-row `AbstractVector{Int64}` whose `getindex` throws returned
  `Int64` from `_publictype` with zero element reads.
- A nullable declared field over a throwing
  `AbstractVector{Union{Missing,Int64}}` also had zero reads.
- A counting missing-capable vector with no null read all three slots and
  stayed `Int64`; one with a null at slot two stopped after two reads and
  widened.
- A warmed five-million-element `Vector{Int64}` call allocated zero bytes.
  One hundred thousand `_publictype` calls completed in 0.0221 seconds.

Null, union, composite, dictionary, and REE public types matched the
schema-derived rules recorded above. The ordinary conforming typed path is
therefore O(1); it does not add a data scan.

## ArrowStrings adversarial verification

### String semantics and allocation

An independent hostile-input probe ran under Julia 1.10.11, 1.12.6, and
1.13.0-rc1. It made 284,331 checks per run over 194 hostile byte strings,
all 37,636 ordered pairs, and every 65,536 two-byte input. It compared raw
bytes, code units, `==`/`isequal`, both `cmp` directions, `isless`, five hash
seeds, iteration, length, `eachindex`, `isvalid`, character indexing,
`thisind`, `nextind`, and `prevind` against `String`. Every semantic check
passed. Julia 1.10/1.12 used the `memhash` branch. Julia 1.13 used
`Base.hash_bytes`.

Typed probes reported zero bytes for vector `getindex`, direct string
`getindex`, checked code-unit access, and iteration on both inline and view
values. The supported-version suite failure in finding 3 is limited to the
test measurement context.

### Buffer and Int32 edges

A three-buffer column selected each long value from the named buffer and
read correctly. Ordinary access to both a negative and an overlarge buffer
index raised `BoundsError`. `view_payload` and `rebase_payload` handled
buffer-index and offset Int32 edges correctly. Finding 2 records the missing
length-word check.

### Zero-copy write, null bytes, and PyArrow

A four-slot column covered inline bytes with NUL, buffer 0, a hostile null,
and buffer 2. `_writecolumn` retained the payload vector and all three data
buffers by identity. The null validity bit was clear. Its other twelve entry
bytes stayed unchanged in the `ArrayData` and serialized IPC views buffer.

`Arrow.Table` read back
`["a\0bZ", "first-buffer-value", missing, "third-buffer-value"]` as
`Union{Missing,String}` under a retained Utf8View field. In the conformance
image, PyArrow 25.0.1 reported `string_view` and the same values, including
the null. Finding 1 is the separate legal zero-buffer case.

### Trim claim — checked, but not repository-gated

A separate scratch JuliaC `--trim=safe` workload covered inline/view access,
comparison, hashing, iteration, missing values, and materialization. Compile
exited 0 with zero verifier errors and zero verifier warnings; the produced
binary exited 0. The current required `test/trim_compile_tests.jl` gate
includes ArrowCore directly and does not cover ArrowStrings. The standalone
result supports the trim claim for the tested surface, but no committed gate
protects it.

## Assumptions and decisions

- I treated the exact committed tree as the authority. I ran mutable setup
  only in detached worktrees, tracked-only archives, or temporary
  environments.
- I treated N = 0 as part of the advertised N-buffer representation because
  Core and its IPC writer battery explicitly support zero variadic buffers.
- I treated `view_payload` input outside the 2-GiB contract as an error path
  that must refuse, not as supported data. That is why finding 2 is LOW
  despite the silent null conversion.
- I treated `julia = "1.10"` as a supported ArrowStrings test target. I
  separated its test-harness allocation failure from the independently
  verified zero-allocation typed kernels.
- I treated the user manual as the public facade contract. Raw direct scan
  handles may retain `Any` for dynamic composite storage columns.
- I distinguished source buffering from record-batch decoding. The
  round-57 larger-than-RAM safety issue is closed even though finding 5's
  decode-timing statement is false.
- I rated the valid zero-buffer write rejection MEDIUM. I rated the
  out-of-contract length refusal, missing CI/minimum test coverage, and two
  public prose defects LOW.
- I accepted the declared conformance skips. I accepted Documenter's local
  deployment-skip warning only after a second `remotes=nothing` build exited
  0 without warnings.
- The host was 64-bit arm64 macOS with Julia 1.12.6 and Docker 29.6.2.
  Additional ArrowStrings probes used Julia 1.10.11 and 1.13.0-rc1.
- I did not modify product or test code. The protected untracked files
  `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`, `ISSUE-540.md`,
  `ISSUE-580.md`, and `mytestdata.arrow` remained untouched. This review
  document is the only main-checkout change made by this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit
  0: ArrowCore 420/420, threaded caches 4/4, facade 317/317, and IPC read,
  IPC write, C data, and ranged-scan acceptance 1/1 each.
- `julia --project=src/ArrowStrings --startup-file=no -e 'using Pkg;
  Pkg.test()'` — exit 0: 2,487/2,487 on Julia 1.12.6.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0: 6/6,
  compile plus executable passed, with zero verifier errors and zero
  verifier warnings.
- `julia --startup-file=no conformance/run.jl` — exit 0 from a detached
  exact-HEAD worktree: corpus 275 pass / 0 fail / 36 skip; IPC oracle 170 / 0
  / 43; C data and C stream 143 / 0 / 9.
- Requested docs environment `Pkg.develop` for exact Arrow and the clean
  Tables checkout plus `Pkg.instantiate()` — exit 0 in a detached worktree.
  `docs/make.jl` — exit 0 with only the expected local deployment warning.
  A direct equivalent `makedocs(..., remotes=nothing)` — exit 0 with no
  warning or error.
- Independent round-57 closure probe — exit 0. Nullability 40/40; eleven
  scalar and wrapper empty-type matrices passed; `_publictype` no-scan
  controls passed.
- ArrowStrings hostile semantics/allocation probe — exit 0 under Julia
  1.10.11, 1.12.6, and 1.13.0-rc1. Zero-copy/null/root probe — exit 0,
  14/14 plus serialized round-trip 5/5. PyArrow 25.0.1 probe — exit 0.
- Extra supported-minimum `Pkg.test()` for ArrowStrings under Julia 1.10.11
  — exit 1, 2,485/2,487, as finding 3 records.
- Separate ArrowStrings JuliaC trim probe — compile exit 0, zero verifier
  errors/warnings; produced binary exit 0. This was not the committed trim
  gate.
- `git diff --check
  1284eccda1114786b5e4dc07ae2b3b198fac3d14..8322039f3db605af9044764f2469304d0168d69f`
  and the required main-checkout `git diff --check` — exit 0 before this
  report. A separate whitespace and final-verdict check covers this
  untracked report after writing.
- Final branch HEAD remained
  `8322039f3db605af9044764f2469304d0168d69f`.

VERDICT: FINDINGS
