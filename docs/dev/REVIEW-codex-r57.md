# Arrow.jl 3.0 code review — round 57

Date: 2026-08-18

Scope: exact commit `1284eccda1114786b5e4dc07ae2b3b198fac3d14`
(`fix: resolve round 56 findings — docs build, honest write/type prose,
typed empty scans`) on `core-rewrite`. Round 56 reviewed through
`9d89d55b472b5fcb6ce17633a9482a48de902dd1` and recorded two MEDIUM plus
three LOW findings in `docs/dev/REVIEW-codex-r56.md`. I reviewed the complete
four-commit `9d89d55..1284ecc` delta:

- `49f6595` — one Docker image and Harbor driver for all conformance suites;
- `beefc68` — the round-56 review record;
- `79bd4ee` — zero-copy `fromcompactviews` over CSV CompactString payloads;
- `1284ecc` — the round-56 fixes.

All Tables-dependent checks used the clean `jq/scan` checkout at
`ee9df1ef2a7bc9ed346b034cc3fcf52a855cc0d9`. The actual CSV integration
probe used a detached checkout at
`a1528fcee8ac62b96b0b2183c836934d67f8ef35`.

## Result

Round 57 is not clean. I found three MEDIUM and three LOW issues.

The API-reference repair, whole-buffer writer wording, named scan/design
cleanup, zero-copy CompactString implementation, ordinary empty direct-scan
typing, and all functional Docker suites pass. The remaining and new issues
are:

1. typed direct scans now reject nulls that the normal IPC validation tier
   intentionally accepts under a non-nullable Field;
2. the replacement `Arrow.Stream` prose still promises bounded-memory
   consumption for IPC stream-format sources that are fully read and decoded
   before iteration;
3. the advertised conformance command cannot start from a fresh tracked
   checkout because host Harbor is not instantiated;
4. the revised empty-column type rules still depend on whether the source
   has zero batches or one zero-row batch;
5. the documented one-suite conformance command is a shell pipeline, not a
   choice of arguments;
6. the current core design document still describes the removed
   CompactString copy/rewrite implementation.

Every required gate exits 0 after its stated environment preparation. The
fresh-checkout failure of the exact conformance command is finding 3, not a
suite failure inside Docker.

## Findings

### 1. MEDIUM — typed direct scans enforce an opt-in nullability contract

Commit `1284ecc` changed decoded direct-scan columns from dynamic
`materialize` to `_scancolumn`/`_batchcolumn` at `src/scan.jl:495-507`, with
the ArrowFile and RangedFile call sites at `src/scan.jl:625` and
`src/scan.jl:1177`.

`_batchcolumn` derives its static claim from the Field at
`src/table.jl:250-253`. `_declaredeltype` excludes `Missing` when
`nullable=false` at `src/table.jl:591-608`. If the stored data nevertheless
contains a null, typed materialization reaches `_typednullrefuse` at
`src/ArrowCore.jl:2227-2231` and throws:

```text
ArgumentError: field x holds a null but the static element type does not admit missing
```

That is not the reader's normal validation contract. Field nullability is
advisory for ecosystem compatibility: the reference implementation and the
arrow-testing corpus accept such data. Core therefore checks it only in
opt-in `validate_full`, as stated and implemented at
`src/ArrowCore.jl:1655-1703`. IPC exposes a batch after structural and
semantic validation, not full validation
(`docs/dev/core-README.md:186-193`). The existing Core regression at
`test/core_tests.jl:1172-1192` pins the same distinction. Round 24 rated the
equivalent ranged rejection MEDIUM, and round 25 recorded its removal.

A one-row non-nullable Int64 Field with a cleared validity bit produced:

```text
Core materialize: Any[missing]

HEAD 1284ecc:
ArrowFile  direct scan -> ArgumentError: field x holds a null ...
RangedFile direct scan -> ArgumentError: field x holds a null ...

parent 9d89d55:
ArrowFile  direct scan -> Any[missing] :: Any
RangedFile direct scan -> Any[missing] :: Any
```

The same parent-versus-HEAD probe over `REE<Int64>` with a non-nullable
values child containing `[missing, 7]` reproduced the regression on both
handles. Union roots and dictionary/REE-wrapped unions remain safe because
`_typedroutable` keeps them dynamic. Primitive, binary, interval, and
REE-transparent closed leaves are affected.

The new empty-schema battery does not cover this accepted mismatch. Its
ordinary Int/String fixture has data consistent with Field nullability.

Disposition: open. The scan routing cannot treat advisory Field nullability
as proof that the decoded data contains no null. Preserve the typed fast path
for conforming batches, but use a missing-capable result when the data
contradicts that advisory declaration. Add primitive and REE regressions for
both direct handles. The implementation must also define how that fallback
interacts with the new empty-result schema rule.

### 2. MEDIUM — `Arrow.Stream` is not bounded-memory for IPC stream-format sources

The writer-side correction is true: `Arrow.write` materializes every
partition, constructs the complete IPC buffer, and writes the sink once.
However, its replacement guidance remains too broad.

`docs/src/manual.md:99-115` calls `Arrow.Stream` the tool for sources larger
than memory and says the consumer loop is the bounded-memory path. The
`Stream` docstring at `src/table.jl:717-724` says a consumer that processes
and drops batches holds one batch of columns at a time.

That is true for a default-mmap IPC **file-format** path. It is false for the
IPC **stream format** and for every IO input:

- `_opensource` reads every IO and every non-mmap path to the end at
  `src/table.jl:412-420`;
- `_readstream` frames the complete byte vector and decodes every record into
  `batchslots` at `src/ipc_read.jl:941-1057`;
- the resulting `IPCStream.batches` vector is retained by `Stream` at
  `src/table.jl:726-733`;
- iteration only indexes those already-decoded batches at
  `src/table.jl:738-756`.

A three-batch compressed stream-format probe produced:

```text
stream-format IO bytes=4464 position before=0 after constructor=4464
source=Arrow.IPCStream decoded before iteration=3 decoded after first=3
stream-format path source=Arrow.IPCStream decoded at construction=3
file-format path source=Arrow.ArrowFile batches=3
```

The file-format control stayed lazy. The stream-format constructor consumed
the complete IO and retained all three decoded batches before the first
iteration.

Disposition: open. Qualify the bounded-memory guidance to a lazy file-format
path with default mmap behavior, including the retained shared dictionaries,
or implement an incremental stream-format reader before recommending it for
inputs larger than RAM.

### 3. MEDIUM — the documented conformance command cannot start from a fresh checkout

The new workflow presents this as the complete all-suite command in
`README.md:65-66`, `docs/dev/core-README.md:53`, and
`conformance/run.jl:20`:

```bash
julia --project=conformance --startup-file=no conformance/run.jl
```

In a detached exact-HEAD worktree with only tracked files, that command
exited 1 after 1.28 seconds, before any Docker call:

```text
ERROR: LoadError: ArgumentError: Package Harbor [...] is required but does not seem to be installed:
 - Run `Pkg.instantiate()` to install all recorded dependencies.
...
in expression starting at .../conformance/run.jl:34
```

`conformance/run.jl:34` executes `using Harbor` before any setup.
`Manifest.toml` is ignored repository-wide by `.gitignore:18`, so a fresh
checkout has no `conformance/Manifest.toml`. The `Pkg.instantiate()` at
`conformance/run.jl:81-84` is inside the future container and is unreachable
until host Harbor loads. No current setup instruction instantiates the host
conformance project. The active main checkout had a private ignored
Manifest, which masked this failure.

The undisclosed host command fixed the environment:

```bash
julia --project=conformance --startup-file=no -e 'using Pkg; Pkg.instantiate()'
```

It exited 0 in 13.17 seconds and created only the ignored scratch Manifest.
The exact conformance gate then passed twice. Thus the Docker image and
suites work, but the committed one-command / “Docker is the only host
requirement” workflow does not.

Disposition: open. Make the driver bootstrap its host Harbor dependency
before loading it, or document a required one-time host instantiate and
qualify the Docker-only statement. Keep the in-container environment setup
separate from this host prerequisite.

### 4. LOW — empty facade types depend on physical record-batch structure

The revised read table says every scalar is mapped by a closed schema rule,
so every zero-row scalar retains the populated element type
(`docs/src/manual.md:132-153`). It then says composite and wrapper columns
narrow from rows, so every zero-row wrapper is `Any`
(`docs/src/manual.md:155-167`). The earlier overview still says every column
has a concrete type determined by the schema at
`docs/src/manual.md:72-75`, which also contradicts the documented dynamic
path.

The actual empty type depends on whether the IPC source has no record batches
or one zero-row batch. Schema-only stream and file inputs returned `Any` for
all eleven tested closed scalar layouts:

- Binary, LargeBinary, FixedSizeBinary, and BinaryView;
- Decimal32, Decimal64, Decimal128, and Decimal256;
- year-month, day-time, and month-day-nano Interval.

Adding one zero-row RecordBatch under the identical schema changed every
column to the documented concrete type: `Vector{UInt8}`, `Int32`, `Int64`, or
the relevant Interval `NamedTuple`.

The inverse wrapper rule also failed:

```text
REE<Int64>  schema-only => Any; zero-row batch => Int64
REE<Date32> schema-only => Any; zero-row batch => Int32
REE<Binary> schema-only => Any; zero-row batch => Vector{UInt8}
```

File and stream formats agreed. Direct ArrowFile/RangedFile scans of the
schema-only file were correctly typed by the new `_joinscanparts` path, and
the ranged facade inherited those types. The ordinary Table facade did not.

Root cause: `_facadebasetype` omits binary, decimal, and interval layouts at
`src/table.jl:190-209`, and `_facadecolumn` uses that incomplete mapping when
`parts` is empty at `src/table.jl:256-263`. With one empty batch,
`_batchcolumn` supplies a typed empty part. `_declaredbasetype` includes the
omitted layouts at `src/table.jl:623-638`, and REE transparently inherits its
values child's type at `src/table.jl:591-595`.

Disposition: open. Use one complete Field-aware type rule for both zero-part
and one-empty-part facade construction, or narrow the manual to the actual
batch-dependent contract. Pin schema-only and zero-row-batch cases for every
closed scalar and transparent wrapper.

### 5. LOW — the documented one-suite command is a shell pipeline

`docs/dev/core-README.md:54-55` prints this literal Bash command:

```bash
julia --project=conformance conformance/run.jl corpus|oracle|cdata
```

In zsh or Bash, `|` creates two pipelines. It does not express alternatives.
The shell tries to run commands named `oracle` and `cdata`; a literal probe
exited 127. The driver never receives the advertised choice.

Disposition: open. Use a `<suite>` placeholder outside a literal command, or
show the three executable commands separately.

### 6. LOW — the core design document describes the removed CompactString rewrite

The zero-copy implementation and its API doc are correct, but the current
design authority is stale. `docs/dev/core-README.md:166-170` still says:

- inline entries are copied;
- long entries have their second word rewritten;
- only the two data buffers wrap zero-copy.

Commit `79bd4ee` removed that work. `src/ArrowCore.jl:2752-2797` now builds
only the validity bitmap. The payload vector itself is the views buffer, and
`payloads`, `buf`, and `extra` are all retained by identity. Valid long-entry
words already contain the Int32 buffer index and zero-based Int32 offset.

Disposition: open. Update the current design document to the actual
zero-copy three-root contract and say that geometry is checked during
semantic/full validation rather than construction.

## Round-56 closure checks

### API reference and Documenter — closed

The requested docs environment developed exact Arrow plus the clean Tables
checkout and instantiated successfully. The exact build exits 0. Its only
warning is Documenter's expected local deployment notice:

```text
could not auto-detect the building environment; skipping deployment
```

There are no docs-block, cross-reference, or checkdocs failures. The built
reference contains one resolved entry each for `export_stream!`,
`fetchranges`, `nextbatch!`, `release!(::ForeignOwner)`,
`release!(::ImportedStream)`, and `reap!`. The `export_stream!` block is
attached at `src/cdata.jl:1599-1608`; `nextbatch!` resolves through module
Arrow as intended.

### Whole-buffer writer wording — writer side closed; replacement guidance has finding 2

The manual and `Stream` docstring no longer say that `Arrow.write` emits
partitions incrementally. A three-partition source produced:

```text
[:partition_1, :partition_2, :partition_3, (:sink_write, 826)]
```

All partitions were consumed before the single sink write. The remaining
issue is the distinct read-side memory claim in finding 2.

### Read/write type tables — partly closed

The main manual probe passed 37/37:

- zero-row List, Struct, Null, and Union columns used `Any` in the tested
  one-batch shapes;
- an all-missing List narrowed to `Missing`;
- Date32 under REE and Union stayed raw `Int32`;
- Date32 through Dictionary converted to `Date`;
- nested core lists and the listed top-level facade conversions succeeded;
- List<Date>, Date/SubString NamedTuple fields, DictEncode(Date), and an
  arbitrary Julia struct each raised an `ArgumentError` naming the
  unsupported type.

The write-domain and NamedTuple corrections are accurate. The empty-source
rule remains open as finding 4.

### Direct empty scan schema — ordinary cases closed; typed route has finding 1

The complete scan battery exits 0. Its limit-zero, offset-past-input, and
statistics-pruned empty results match the full direct schema on ArrowFile and
RangedFile. A broader focused matrix also passed for dictionary strings,
heterogeneous and wrapped unions, REE<Date32>, wrapped REE/dictionary
combinations, List, Struct, Null, Interval, Decimal128, projections, renames,
overrides, filter-only columns, and zero-field files.

`_typedroutable` correctly keeps direct and wrapped unions dynamic. The new
route is not safe for every semantically accepted decoded field because it
assumes advisory nullability, as finding 1 shows.

### Scan/design cleanup — closed

The named current design and source prose now describes the generic
`Tables.scan` executor, saturating overflow behavior, head-then-tail ranged
reads, the real core test scope, and MONTH_DAY_NANO parsing. Historical
review documents retain old terminology as history only.

## CompactString zero-copy verification

The implementation claim itself is clean.

- The host probe exited 0, 24/24. All three roots satisfied identity:
  `d.buffers[2].region.root === payloads`, buffer 3 was `buf`, and buffer 4
  was `extra`. Long entries kept `(buffer index, offset)` values `(0, 2)` and
  `(1, 0)`. Bad geometry constructed and then failed
  `validate_semantic`. An empty `extra` buffer crossed C data.
- A temporary environment developed the actual CSV checkout at exact
  `a1528fc`, Arrow at exact HEAD, and local Tables. A real
  `CSV.CSVKernel.CompactStringVector` containing inline, input-buffer,
  escaped-extra-buffer, and missing slots passed 6/6. Its three roots were
  identical; long words were `(0, 7)` and `(1, 0)`.
- The in-container PyArrow 25.0.1 probe used the actual CSV payload type. A
  null entry had length `-1`, a garbage prefix, buffer index `0x7fffffff`,
  and offset `0xdeadbeef`; its exact 16 bytes were:

```text
ffffffffadfbcadeffffff7fefbeadde
```

Arrow `validate_full` passed. The unchanged views buffer crossed C data at
the same address. PyArrow saw `string_view`, preserved the null, and
`validate(full=True)` passed. All payload, input-buffer, and extra-buffer
roots remained identical.

## Docker/conformance verification

The containerized implementation works after the undisclosed host setup in
finding 3.

- A no-cache build from the `conformance/` directory alone exited 0 in
  108.59 seconds. BuildKit transferred a 2-byte context and the resulting
  image had an empty `/work`; no repository source was copied into it.
- The image contained Julia 1.12.6, PyArrow 25.0.1, nanoarrow 0.9.0,
  Tables `ee9df1e`, arrow-testing `9ff285c`, and
  `/lib/aarch64-linux-gnu/libpython3.11.so.1.0`.
- After host instantiation, the required driver exited 0 in 46.86 seconds:
  corpus 275 pass / 0 fail / 36 skip; IPC oracle 170 / 0 / 43; C data and C
  stream 143 / 0 / 9.
- A second complete run exited 0 in 46.45 seconds with the same counts.
  Direct environment preparation took 3.87 seconds on the persistent depot
  and did no precompile work. A fresh comparison volume took 6.06 seconds
  and precompiled Arrow/Mmap. The named depot therefore retained the
  worktree-specific cache.
- The C-data suite used `/opt/pyarrow/bin/python` in-process and passed all
  37 Julia-to-PyArrow, 37 native import, 29 slice, 37 stream, two metadata,
  and one registry-drain checks.
- The `uv`, `venv`, `pip`, and `python3 -m` sweep found those setup commands
  only in `conformance/Dockerfile`, where they build the in-image
  `/opt/pyarrow` environment. No host Python/venv/pip setup remains in the
  Julia conformance drivers.

## Assumptions and decisions

- I treated the exact committed tree as the authority and ran mutable setup
  only in detached worktrees or temporary environments.
- I treated normal semantic-tier acceptance, the explicit Core test, and the
  round-24/25 decision as authoritative for non-nullable Fields containing
  nulls. `validate_full` remains the opt-in strict tier.
- I treated a schema-only source as a zero-row table because `Arrow.Table`
  exposes each declared field and `Tables.schema` reports its column type.
- I treated shell commands in executable code blocks as literal copyable
  commands. I did not treat an unmentioned host `Pkg.instantiate()` as part
  of a stated one-command, Docker-only workflow.
- I rated the direct-scan regression MEDIUM because it restores a previously
  fixed compatibility rejection on both handles. I rated the Stream claim
  MEDIUM because following the larger-than-RAM guidance can cause an OOM. I
  rated the clean-checkout conformance failure MEDIUM because the exact new
  entry command cannot reach any suite. The remaining behavior and prose
  defects are LOW.
- I accepted the established conformance skips and only Documenter's local
  deployment warning.
- The host was 64-bit arm64 macOS with Julia 1.12.6 and Docker client/server
  29.6.2.
- I did not modify product or test code. Probe files and dependency changes
  stayed under `/tmp` detached worktrees. After the exact target was pinned,
  the shared main checkout acquired concurrent product/test edits and a new
  untracked `src/ArrowStrings/` tree; I preserved them. The protected
  untracked files `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`,
  `ISSUE-540.md`, `ISSUE-580.md`, and `mytestdata.arrow` remained untouched.
  This review document is the only main-checkout change made by this review.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0
  after developing the clean Tables `jq/scan` checkout: ArrowCore 420/420,
  threaded caches 4/4, facade 272/272, and IPC read, IPC write, C data, and
  ranged-scan acceptance 1/1 each. A fresh scratch project first resolved
  registered Tables 1.13.0 and failed before tests because it lacks the
  unreleased `Tables.Scan` interface; the established branch pin corrected
  that dependency precondition.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6,
  compile plus run passed, with zero verifier errors and zero verifier
  warnings.
- Fresh tracked `julia --project=conformance --startup-file=no
  conformance/run.jl` — exit 1 before Docker on missing host Harbor, as
  finding 3 records. Host `Pkg.instantiate()` — exit 0. The exact required
  command then exited 0 twice with all three suites and the counts above.
- Requested docs environment `Pkg.develop` for exact Arrow and Tables plus
  `Pkg.instantiate()` — exit 0. `julia --project=docs --startup-file=no
  docs/make.jl` — exit 0 with only the accepted local deployment warning.
- Round-56 manual/writer probe — exit 0, 37/37, with one final sink write.
- Direct scan battery and rich wrapper/schema probe — exit 0 on both
  ArrowFile and RangedFile. The HEAD must-succeed advisory-nullability probe
  exited 1; the identical parent `9d89d55` probe exited 0. The diagnostic
  variant exited 0 while capturing the two HEAD exceptions.
- Compact-view host probe — exit 0, 24/24. Actual CSV `a1528fc` integration
  — exit 0, 6/6. In-container garbage-null C-data/PyArrow probe — exit 0,
  with both Arrow and PyArrow full validation passing.
- Context-only cached Docker build — exit 0 in 0.46 seconds. Context-only
  no-cache build — exit 0 in 108.59 seconds.
- `git diff --check` and
  `git diff --check 9d89d55b472b5fcb6ce17633a9482a48de902dd1..1284eccda1114786b5e4dc07ae2b3b198fac3d14`
  — exit 0 before this report; the main-checkout check also exited 0 after
  the report was written. A separate whitespace and final-verdict check over
  this untracked report exited 0.
- Final branch HEAD remained
  `1284eccda1114786b5e4dc07ae2b3b198fac3d14`.

VERDICT: FINDINGS
