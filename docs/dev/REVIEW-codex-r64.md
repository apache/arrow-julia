<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Arrow.jl 3.0 code review — round 64

Date: 2026-08-19

Scope: exact commit `e446f9765f74a21c0b5f1d82b8d1d863dfa8cbdc`
(`feat!: byte-range reads through AbstractArrowSource; CloudStore.jl
extension`) on `core-rewrite`. Local `HEAD`, local `core-rewrite`, local
`origin/core-rewrite`, and the live GitHub `refs/heads/core-rewrite` all
resolved to that SHA. Round 63 reviewed
`071f367ed8dcc0667aabf43a92de91056ea4e87a`. This round reviewed all three
commits in `071f367..e446f97`:

- `01678e4` — record the round-63 review;
- `00d2d72` — correct the FlatBuffers/C-data research note;
- `e446f97` — replace the 3.0-only ranged-source surface with
  `AbstractArrowSource` and add the CloudStore extension.

I read the full `e446f97` commit message before reviewing the code.

## Result

The round-63 LOW finding is closed. Round 64 has one HIGH, three MEDIUM, and
three LOW findings:

1. equal-length `readranges` payloads can be returned out of order and
   silently exchange rows or columns;
2. CloudStore range requests are not pinned to the `CloudStore.Object` ETag,
   so a key overwrite can mix object versions across request rounds;
3. CloudStore starts one unbounded task and request per planned range;
4. a selected dictionary adds a fourth sequential request round, contrary to
   the three-round design and manual;
5. a Footer larger than the tail window is fetched twice through the public
   `Arrow.Table` path;
6. two range-geometry cases escape as `InexactError` or `ArgumentError`
   instead of the required `ValidationError`;
7. the manual and `Arrow.Table` docstring falsely say that no-scan or
   unpushable ranged reads fetch the whole object.

All requested package, Julia 1.10, Minio, trim, conformance, documentation,
formatter, RAT, and whitespace gates pass at the reviewed SHA. Green gates do
not cover the hostile ordering, mutation, concurrency, or request-count cases
below.

## Findings

### 1. HIGH — equal-length `readranges` payloads can silently change data order

`src/source.jl:79-84` requires `readranges` to return one result per requested
range, in order. `_fetchspans` at `src/scan.jl:860-883` validates the result
count and each payload length. It does not receive or validate a range identity
for each payload. Two results with the same length can therefore trade places
without any range-layer check noticing.

The public-path probe used `Arrow.Table(src; scan=Tables.Scan(select=(:x,)))`
with the default 64 KiB tail and 256 KiB coalescing gap. The 600,922-byte file
had two record batches. Each batch held a two-element `Int64` column plus an
unselected 300,000-byte string buffer. The selected metadata and body spans had
equal lengths and stayed separate:

```text
tail:     (535386, 65536)
metadata: [(184, 208), (300424, 208)]
bodies:   [(392, 16), (300632, 16)]
```

An `AbstractArrowSource` override that returned `reverse(payloads)` from each
`readranges` call completed without an exception:

```text
expected x = [1, 2, 3, 4]
actual   x = [3, 4, 1, 2]
```

A second probe selected equal-size `x` and `z` buffers in one batch. Reversing
the body results changed `x=[1,2], z=[3,4]` to `x=[3,4], z=[1,2]`, again with
no exception. This is silent data corruption, not only a missed diagnostic.
A concurrent adapter that collects task results in completion order is a
plausible way to make this mistake.

The current return type cannot prove payload provenance. To catch accidental
reordering, make each batched result carry its requested range or request
index and validate those identities before constructing `FetchedSpans`.
Another option is to let Arrow own indexed result placement and expose only a
bounded concurrency policy to transports. No interface can authenticate a
source that deliberately returns arbitrary same-length bytes for a claimed
range; if that remains a trusted-source boundary, state it explicitly instead
of promising that wrong order fails closed. Add the pure-public regression
above.

### 2. MEDIUM — CloudStore reads are not pinned to one object version

`CloudStore.Object` records both the object size and its ETag. The extension
uses the size at `ext/ArrowCloudStoreExt.jl:37`, but the GET at lines 39-51
sends only the `Range` header. A runtime capture for an object with
`eTag="etag"` contained:

```text
headers = ["Range" => "bytes=3-6"]
```

There was no conditional header. If the key is overwritten between the tail,
metadata, dictionary-body, and record-body rounds, Arrow can combine bytes
from different object versions. Equal-length replacement ranges pass all
payload-length checks. Old Footer statistics can also prune record batches
from the replacement object. This can return incorrect data without a
validation error.

The design assigns mutation detection and ETag pinning to the extension at
`docs/dev/DESIGN-scan-ranges-trim.md:220-223`, so the current extension does
not implement its stated responsibility. Send a correctly quoted `If-Match`
precondition on every nonempty GET when `obj.eTag` is present. Add a Minio
test that constructs an object handle, overwrites its key, and requires the
old handle to reject the next range. If one portable precondition cannot cover
all supported stores, use provider-specific version preconditions or fall back
to one whole-object GET for a store that cannot condition every range.

### 3. MEDIUM — CloudStore range concurrency is unbounded

`ext/ArrowCloudStoreExt.jl:54-61` constructs one `Threads.@spawn` task for
every coalesced span before it fetches any result. Coalescing limits nearby
ranges; it does not limit the span count. The default reader permits up to
1,000,000 messages at `src/ipc_read.jl:51-59`, and valid large files can keep
one separated metadata or body span per batch at `src/scan.jl:1167-1193` and
`src/scan.jl:1312-1359`.

A large fragmented object can therefore allocate a very large task vector and
start a request storm. This can exhaust task memory, HTTP connections, file
descriptors, provider quotas, or the remote service before Arrow's byte budget
is exhausted.

Use a bounded worker pool and store results by input index. A configurable cap
is best; a conservative default such as `4 * Threads.nthreads()` is sufficient
for an initial implementation. A semaphore around one task per range does not
fix task allocation because it still creates all tasks. Add a fake transport
test that measures peak concurrent requests and result ordering.

### 4. MEDIUM — dictionary scans use four request rounds, not three

The design and manual promise three transport rounds: tail, surviving-batch
metadata, and selected buffers. The planner instead fetches selected
dictionary bodies at `src/scan.jl:1271-1279`, decodes them, and only then calls
`_fetchspans` again for record bodies at line 1359. These are two sequential
body rounds.

The exact 3,402-byte battery fixture produced these public `Arrow.Table`
traces. Each line was a separate request or batched-transport call:

```text
Arrow.Table(src) and Arrow.Table(src; scan=Tables.Scan()):
  (0, 3402)       tail
  (632, 1720)     batch metadata
  (816, 32)       dictionary body
  (1440, 1229)    record bodies
  => 4 rounds

Arrow.Table(src; scan=Tables.Scan(select=(:ints,), limit=3)):
  (0, 3402)
  (632, 1720)
  (1440, 40)
  => 3 rounds

Arrow.Table(src; scan=Tables.Scan(select=(:dict,))):
  (0, 3402)
  (632, 1720)
  (816, 32)
  (1744, 925)
  => 4 rounds
```

The source length and tail were each requested exactly once per call. The
extra call is a real wall-clock round for the CloudStore override, not a count
artifact from the default serial implementation. It contradicts
`docs/dev/DESIGN-scan-ranges-trim.md:178-182`, `docs/src/manual.md:245-249`,
and the main commit message.

The metadata pass already determines dictionary and record buffer ranges.
Fetch both sets in one `readranges` call, then decode dictionaries before
records from the combined returned spans. If the implementation intentionally
keeps the dependency split, change every three-round claim to four rounds when
a selected dictionary is present.

### 5. LOW — an out-of-tail Footer is fetched twice

Only `SourceFile.tail` is cached at `src/scan.jl:763-800`. The facade calls
`_sourceschema(sf)` at `src/table.jl:509-512`; that calls `_rangedfooter` at
`src/scan.jl:1081-1085`. The later `Tables.scan(sf, ...)` calls `_rangedfooter`
again through `_applyscan` at lines 1088-1092. If the Footer is not wholly in
the cached tail, both passes execute the exact follow-up fetch at lines
991-995.

A direct public `Arrow.Table(src; scan=...)` probe used the default tail and a
70,224-byte Footer:

```text
object length 148578, footer length 70224
(83042, 65536)       tail
(78344, 70224)       Footer follow-up
(78344, 70224)       same Footer follow-up again
[(70192, 144)]       batch metadata
[(70336, 8000)]      body
```

The 1,000 decoded rows were correct, but the call added one sequential network
round and downloaded the same 70,224 bytes twice. The singular follow-up claim
at `docs/dev/DESIGN-scan-ranges-trim.md:152-156` is false. Cache the exact
Footer payload so the scan pass can reverify it without another GET. Passing
the parsed Footer from schema lowering is also possible only if its verification
result and remaining allocation-budget accounting carry into scan planning.

### 6. LOW — two range-geometry errors are not `ValidationError`

The source contract permits any `Integer` result from `sourcelength`.
`SourceFile` converts it before checking it at `src/scan.jl:779-782`:

```julia
len = Int64(sourcelength(src))
len >= 0 || throw(ValidationError("source reports a negative length"))
```

An ordinary `Int64(-1)` and an exactly representable `Int128(typemin(Int64))`
produce `ValidationError`. Values outside `Int64` produce `InexactError`
instead:

```text
Int128(-9223372036854775809) => InexactError
Int128( 9223372036854775808) => InexactError
```

Read the `Integer`, validate its sign and `Int64` range, then convert it.

Separately, `_fetchexact` returns `ValidationError` for negative offsets,
negative lengths, and past-end ranges, and `_fetchspans` returns
`ValidationError` for past-end ranges. Negative `_fetchspans` offsets or
lengths reach `_coalesce` first and produce `ArgumentError` at
`src/scan.jl:833-837`. Change that internal guard to `ValidationError` or
validate the spans before `_coalesce`. Current verified metadata cannot reach
this internal case, which limits its severity, but it does not meet the
round's requested uniform error contract.

### 7. LOW — no-scan and fallback fetch documentation describes the wrong path

`docs/src/manual.md:274-277` says, “Without a scan the whole object is read.”
The rendered `Arrow.Table` docstring at `src/table.jl:48-54` also says an
unpushable ranged scan “fetches the entire object.” Actual code creates
`Tables.Scan()` and runs the all-column range planner at
`src/table.jl:512-531`. Only stream-format objects and zero-field files call
`_wholeobject` at lines 506-523. The design describes this correctly at
`docs/dev/DESIGN-scan-ranges-trim.md:202-206`.

A 2,269,762-byte public `Arrow.Table(src)` probe requested:

```text
(2204226, 65536)
[(184, 208), (1129296, 208)]
[(392, 2269120)]
```

It did not request bytes `0:183` and did not issue a whole-object GET. Change
the manual and docstring to say that no-scan and unpushable file reads plan all
columns and all candidate batches. Keep the whole-object statement only for
stream-format and zero-field sources.

## Round-63 closure

The LOW finding in `docs/dev/REVIEW-codex-r63.md` is closed. I checked every
corrected statement in `docs/dev/research-flatbuffers-cdata.md` against the
current tree:

- Lines 33-38 now identify the shared low-level Julia builder code and the
  distinct high-level APIs. Arrow donation commit `50e015f` and the
  pre-donation JuliaData/FlatBuffers.jl commit
  `898c221e9ee3bb3cb512cf37201b59a70ac267d0` materially share `prep!`,
  `place!`, vector and string construction, offset insertion, and vtable
  writing.
- Lines 42-49 now include the live runtime names used outside
  `src/FlatBuffers/`: `Table`, `Struct`, `bytes`, `pos`, `structsizeof`,
  `UOffsetT`, `prepend!`, `prependoffset!`, `finishedbytes`, and the Builder
  `offset` call. A fresh qualified-name sweep found no omitted live name.
- Lines 53-58 now give the actual root-start, `FB.getrootas`, inline version
  gate, root-rest order at `src/ipc_read.jl:155-160` and
  `src/ipc_write.jl:883-888`.
- Lines 87-91 now state that `createstring!` calls `prep!` and that a 64-bit
  head underflow throws `InexactError`; direct Julia 1.10.11 and 1.12.6
  probes agree.
- Lines 135-142 limit “nothing to merge” to the different high-level designs
  and preserve the shared low-level builder qualification.
- Lines 151-158 correctly distinguish `RENAMES` for `Struct_ => Struct` from
  the scalar qualification and module-local generic used for `Bool`, `Int`,
  and `Type` at `tools/fbsgen.jl:169-195,274-299`.
- The nonexistent `cdata.jl` header citation and the “five stalled attempts,
  `cdata.jl`” attribution are gone. Lines 208-212 state the three independent
  efforts, and lines 315-322 describe them as good-faith contributors.
- Lines 257-264 now say schema release occurs before import returns, after
  import and validation, matching `src/cdata.jl:1098-1107`.
- Lines 277-280 now say both imports count unknown nulls eagerly and cache the
  result, matching `src/ArrowCore.jl:1563-1574`.
- Lines 285-290 now say the landed PR only carries the metadata pointer and
  that Arrow's `_import_cmetadata` trusts producer-declared counts and lengths,
  matching `src/cdata.jl:1255-1278`. It makes no bounds claim.

No round-63 research-note statement remains open.

## Interface and format probes

The source length and tail were each read exactly once per public
`Arrow.Table(src)` call. The request counts for the battery fixture are in
finding 4. Other hostile and format probes produced these results:

- A short or long `readrange` payload produced `ValidationError` with the
  returned and expected lengths.
- A short or long `readranges` payload and a wrong payload count produced
  `ValidationError`.
- Reordered unequal-length payloads produced `ValidationError`; finding 1 is
  the equal-length hole.
- A source that reported one byte less than the object produced
  `ValidationError("missing trailing ARROW1 magic")`. A reported length of
  five produced a clean truncated-prefix `ValidationError`.
- Objects of lengths zero through five all produced clean `ValidationError`s.
- An object ending in `ARROW1` but too short to be a file produced
  `ValidationError("file is too short to be an IPC file")`.
- A 482-byte valid file shorter than `tailbytes` decoded `[7, 8]`. It fetched
  the whole object once as the tail, then made the documented metadata and
  body requests.
- A 160,280-byte stream-format object with a scan returned rows
  `[19998, 19999, 20000]`. It fetched `(94744, 65536)` once as the tail and
  `(0, 94744)` once as the prefix. `_wholeobject` reused the cached tail.
- A zero-field file with batches of three and two rows returned row count five
  and used one whole-object tail request.
- A valid file whose leading magic was replaced by `BROKEN` decoded through
  `AbstractArrowSource`; `Arrow.readfile` rejected the same bytes with
  `ValidationError("missing leading ARROW1 magic")`.

Dropping the leading-magic fetch therefore changes accepted input. The ranged
path now accepts a footer-valid object with missing or corrupt leading magic,
and it does not cross-check the leading schema. The local whole-file reader
still enforces both file magics. I accept this deliberate difference because
`SourceFile` and the design state that the Footer is the sole authority. It
saves the head request and does not weaken the bounds and metadata checks on
the bytes the ranged path uses. The behavior must remain explicit because it
is not strict IPC file-format validation.

## CloudStore extension audit

The basic integration is correct apart from findings 2 and 3:

- CloudStore 1.8.0 defines the used `Object` fields. `obj.size` is the known
  length, and `obj.credentials`, `obj.store`, `obj.key`, and `obj.eTag` are
  present.
- `headers=["Range" => "bytes=a-b"]` matches CloudStore's own range helper.
- `allowMultipart=false` selects the one-GET path. That path returns
  `resp.body`, and the extension normalizes it to `Vector{UInt8}`.
  `objectMaxSize=len` only helps CloudStore select that path; with
  `allowMultipart=false` it does not cap or validate the response length.
  Arrow's own bounds and exact-length checks provide those validations.
- Passing `credentials=obj.credentials` explicitly is correct and necessary.
  The top-level `CloudStore.get(::Object)` in both 1.6.x and 1.8.0 dispatches
  through the store and key without forwarding the stored credentials.
- `len == 0` returns `UInt8[]` without a GET, including for a direct offset
  beyond the object. `SourceFile` rejects an out-of-object planned range
  before it reaches this method.
- Against Minio, `(9, 1)` on a 10-byte object returned one byte; `(9, 2)` also
  returned one byte because S3 truncates a range that starts inside the
  object; `(10, 1)` threw `HTTP.StatusError` with status 416. `_fetchexact`
  rejects `(9, 2)` against the known object size before a planned GET. Its
  exact-length check also rejects a short response if a stale reported extent
  permits such a GET. If stale `obj.size` permits a range that begins beyond
  the replacement object's end, CloudStore throws raw `HTTP.StatusError(416)`;
  `_fetchexact` does not normalize a thrown transport exception. Direct
  `readrange` exposes the same transport behavior.

The `[weakdeps]`, `[extensions]`, and `CloudStore = "1.6"` entries are wired
correctly. Isolated CloudStore 1.6.0 and 1.6.4 environments contain the used
fields and keywords, load the extension, and pass the range return-type probe.
The local development checkout contains the same APIs. Its current branch is
six commits past the v1.6.4 tag and its own environment hits an unrelated
expired Reseau precompile certificate; the isolated tag-level probes establish
the declared floor without changing that dirty checkout. Registered
CloudStore 1.8.0 loaded under Julia 1.12.6 and Julia 1.10.11.

## Documentation, API surface, trim, and delta audit

`src/source.jl:23-87` has docstrings for `AbstractArrowSource`,
`sourcelength`, `readrange`, and `readranges`. `docs/src/reference.md:39-46`
renders all four. The HTTPSource sketch implements the two required methods.
The CloudStore example matches the constructor and the `Tables` names imported
earlier in the manual.

No `RangedSource`, `RangedFile`, `fetchranges`, or `rangedschema` occurrence
remains outside historical review records. Those names are undefined at HEAD.
`SourceFile` is the internal planner handle. The only exports remain the
module and `close!`; the four new interface names are qualified APIs, which
keeps the package's export surface minimal.

The trim entrypoint includes `ArrowCore.jl` and `cdata.jl` only. It does not
include `source.jl`, `scan.jl`, or the facade, so the new abstract type is not
on its reachable path. The trim compile and executable each exited zero with
zero verifier errors and zero verifier warnings.

`01678e4` adds only the round-63 record. `00d2d72` closes that record's LOW
finding. I found no additional defect in those two commits. The seven findings
above are in the `e446f97` redesign and its documentation.

## Assumptions and decisions

- I treated the user-required fail-closed behavior as authoritative. The
  source contract says results are in order, but the optional concurrent
  override makes accidental completion-order results plausible. Arbitrary
  same-length source contents cannot be authenticated without a stronger
  transport or checksum contract.
- I counted a transport round as one sequential `readrange` call or one
  `readranges` invocation. Multiple ranges inside one `readranges` call are
  one round because a concurrent transport can issue them together.
- I classified silent row or column exchange as HIGH. I classified possible
  mixed-version reads, unbounded cloud fan-out, and a guaranteed extra cloud
  round for selected dictionaries as MEDIUM. I classified the rare large-
  Footer duplicate, inconsistent error types, and false prose as LOW.
- I accepted footer-only authority and the leading-magic acceptance change as
  an explicit performance tradeoff, not a finding.
- I accepted Documenter's local deployment-skip notice after all content,
  cross-reference, docstring, and rendering checks completed successfully.
- I recommend a bounded worker pool rather than a semaphore because a
  semaphore would not bound the number of allocated tasks.
- I did not modify product or test code. All probes and dependency manifests
  stayed in detached or temporary worktrees. The protected untracked files
  `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`, `ISSUE-540.md`,
  `ISSUE-580.md`, and `mytestdata.arrow` remained untouched. This review
  document is the only active-checkout change made by this round.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — exit 0
  in the detached gate worktree after developing the required Tables checkout:
  ArrowCore 421/421, facade 321/321, CloudStore/Minio 12/12, and all IPC,
  C-data, statistics, scan, and byte-range batteries passed.
- Julia 1.10.11, from the required binary and an isolated environment that
  developed exact Arrow, `src/ArrowStrings`, and
  `/Users/jacob.quinn/.julia/dev/Tables`: `Pkg.test("Arrow")` — exit 0 with
  the same 421/421, 321/321, 12/12, and battery results. The CloudStore
  extension precompiled and loaded.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0, 6/6. The
  compile and produced binary exited 0; verifier errors 0, warnings 0.
- `julia --startup-file=no conformance/run.jl` — exit 0: corpus 275 pass,
  0 fail, 36 declared skips; IPC oracle 170 pass, 0 fail, 43 declared skips;
  C-data/C-stream oracle 143 pass, 0 fail, 9 declared skips.
- Documentation setup developed exact `.` and the required Tables checkout
  into `docs/`. `julia --project=docs --startup-file=no docs/make.jl` — exit
  0. The four new reference entries rendered. The only warning was the
  expected local deployment skip. `docs/Project.toml` was restored
  byte-identically afterward.
- JuliaFormatter 2.12.5 `format(".")` — exit 0 and no tracked change.
- `dev/release/run_rat.sh .` — final exit 0, “No unapproved licenses,” after
  temporarily moving the detached worktree's administrative `.git` pointer
  and ignored generated docs/conformance artifacts out of the scan. The first
  run, before removing those generated artifacts, exited 1 only for those 12
  ignored files; no committed file was unapproved. All artifacts were restored.
- `/private/tmp/arrow-r64-source-probe.jl` — exit 0 with
  `ASSERTIONS|all_expected_observations=true`; it covers the public request
  traces, hostile return sizes/counts/order, format probes, zero fields, and
  leading-magic difference.
- Focused CloudStore 1.8.0 Minio suite — exit 0, 12/12. Minio range-edge,
  CloudStore 1.6.0, CloudStore 1.6.4, Julia 1.10 extension-load, ETag-header,
  and return-type probes all completed as described above. The concurrency
  finding is from direct extension and planner-limit inspection.
- `git diff --check 071f367..HEAD` and final `git diff --check` passed. A
  no-index whitespace check of this untracked review document produced no
  diagnostic; its expected exit 1 only records that the file differs from
  `/dev/null`. The detached worktrees remained tracked-clean.
- Final branch HEAD and live `origin/core-rewrite` remained
  `e446f9765f74a21c0b5f1d82b8d1d863dfa8cbdc`. Active status contained only
  the six protected untracked files and this review document.

VERDICT: FINDINGS
