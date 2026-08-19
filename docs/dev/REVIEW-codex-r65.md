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

# Arrow.jl 3.0 code review — round 65

Date: 2026-08-19

Scope: exact commit `d3ea7a7cf16001985a0237e18c9c873307acbc6c`
(`fix: resolve round 64 findings — Arrow-owned range reads, ETag pinning,
one body round`) on `core-rewrite`. Local `HEAD`, local `core-rewrite`, local
`origin/core-rewrite`, and the live `origin` `refs/heads/core-rewrite` all
resolved to that SHA. Round 64 reviewed
`e446f9765f74a21c0b5f1d82b8d1d863dfa8cbdc`. This round reviewed both
commits in `e446f97..d3ea7a7`:

- `16cfb8b` — record the round-64 review;
- `d3ea7a7` — close its seven findings.

I read the full `d3ea7a7` commit message before reviewing the code.

## Result

The seven round-64 findings are closed for their stated cases. Round 65 has
three LOW findings:

1. `sourcelength` results are not checked to be `Integer`s before comparison
   and conversion;
2. the new worker pool violates the repository rules for atomics and spawned
   task monitoring;
3. Arrow claims that the released CloudStore integration supports GCS, but
   the current registered CloudStore package supports only S3 and Azure.

All required package, Julia 1.10, Minio, trim, conformance, documentation,
formatter, RAT, and whitespace gates pass after their documented environment
setup. The ranged-source probes pass for result placement, bounded
concurrency, original exception types, request rounds, Footer reuse,
whole-object fallbacks, and limits.

## Findings

### 1. LOW — `sourcelength` is not checked to be an `Integer`

`src/scan.jl:784-789` checks the reported value against zero and
`typemax(Int64)`, then converts it with `Int64(reported)`. It does not first
check `reported isa Integer`.

The exact public-path probe produced:

```text
sourcelength = 3402.0  => accepted; Arrow.Table decoded the fixture
sourcelength = 1.0     => accepted as SourceFile.len == 1
sourcelength = 1.5     => InexactError
sourcelength = "3402"  => MethodError
sourcelength = nothing => MethodError
```

The commit message and this round's requested closure both say that the
result is validated as an `Integer` before conversion. The new
`_BadLengthSource` test cannot cover this case because its `reported` field
has type `Integer`. This is also inconsistent with the explicit
`ValidationError` checks for a `readrange` result's element type and length.

Add an `isa Integer` guard before either comparison. Add non-`Integer` source
results to the public-path contract tests. This is LOW because a conforming
source already returns an `Integer`; it does not affect correct adapters.

### 2. LOW — the worker pool violates the repository concurrency rules

The applicable repository instructions say to avoid `Atomic{T}` and use an
`@atomic` field on a mutable struct (`../AGENTS.md:12`). They also require
every `Threads.@spawn` task to be wrapped with `errormonitor` unless an
exception is explicit (`../AGENTS.md:16`). `_readspans` instead introduces
`Threads.Atomic{Int}` at `src/scan.jl:919` and a bare `Threads.@spawn` at line
922. No exception to either rule is recorded.

The implementation is functionally sound in the exercised cases. The atomic
counter gives every request one index. `@sync` waits for every worker before
the result vector is used. `_firstcause` restores the original worker
exception. This is therefore repository-policy noncompliance, not a race or
data-corruption finding.

Use a mutable worker-state type with an `@atomic` index field and apply the
required spawned-task monitoring. If `@sync` is intended to replace
`errormonitor`, record that narrow exception in the repository instructions
instead of silently diverging from them.

### 3. LOW — the released CloudStore integration does not support GCS

Arrow names S3, Azure Blob Storage, and GCS as working `CloudStore.Object`
sources in `README.md:50-51`, `ext/ArrowCloudStoreExt.jl:17-19`,
`docs/src/manual.md:251-253`, and `src/source.jl:61-64`. A fresh registry
resolution selected CloudStore 1.8.0, the current registered version. That
release documents `Object` for S3 and Azure. Its `Object` credential field
accepts only AWS or Azure credentials. Its public dispatch contains only
`AWS.Bucket` and `Azure.Container`. Its GCP URL dispatch is commented out.
The declared CloudStore 1.6 floor has the same limitation.

GCS itself supports `Range` plus `If-Match` and returns HTTP 412 on an ETag
mismatch. The HTTP mechanism is portable. The missing part is a released
CloudStore `Object` and GET path that Arrow can call.

Remove GCS from the current support claims. Alternatively, require a released
CloudStore version with GCS support and add a provider-path test before making
the claim. This is a documentation and availability defect; it does not
affect the verified S3 or Azure paths.

## Round-64 closure

### Ordered and bounded range reads

The HIGH ordering finding is closed. `Arrow.readranges` no longer exists.
`_readspans` issues only indexed `readrange` calls and stores each payload in
`results[i]`. A direct nine-span probe with `concurrentreads(src) == 4`
completed in this order:

```text
[4, 3, 2, 1, 6, 8, 5, 7, 9]
```

All nine indexed byte vectors were correct after `_readspans` returned. Peak
in-flight reads were four. A public 600,922-byte two-batch probe completed
both two-request planner waves later-request-first, but returned
`x == [1, 2, 3, 4]` in file order. Its peak was two, below the source limit
of four. Zero and negative limits clamp to one serial reader.

The same closure probe ran with `--threads=1`. Its sleeping `readrange`
methods yielded, so `Threads.@spawn` still overlapped the I/O-shaped work.
The repository facade test also passed 338/338 with one Julia thread.

A short result inside a two-request concurrent metadata wave surfaced from
`Arrow.Table` as the original `Arrow.ArrowCore.ValidationError`, not a
`TaskFailedException` or `CompositeException`. Public sources that returned a
short vector, a long vector, a `String`, or `nothing` also produced exact
`ValidationError`s.

### ETag pinning

The CloudStore mutation finding is closed for the supported providers. The
extension adds a quoted `If-Match` beside `Range` on each nonempty GET. A
strong Minio probe inspected both the direct stale `readrange` and the stale
public `Arrow.Table` failure. Each was the original `HTTP.StatusError` with
status 412, an XML `PreconditionFailed` code, the old quoted ETag, and a
nonempty range. A fresh `CloudStore.Object` had a new ETag and read all 2,000
replacement rows. The same probe passed at CloudStore 1.6.0.

The provider contracts agree: mismatched `If-Match` on a read returns 412 for
[Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html),
[Azure Blob Storage](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations),
and [Google Cloud Storage](https://docs.cloud.google.com/storage/docs/xml-api/get-object-download).
Range reads do not bypass the condition.

An empty stored ETag skips the header. I accept this. There is no valid entity
tag to send, and normal provider metadata supplies one. A manually built or
nonconforming empty-ETag object therefore relies on the documented trusted
source stability boundary and is not version-pinned.

The committed Minio assertions use broad `@test_throws Exception`. They do
not themselves pin status 412. The focused runtime inspection above proves
that both assertions currently fail for the intended precondition and not for
a range, credential, or service error. Earlier reads and the fresh-object read
also prove that the service and credentials remain usable.

### One body round and request counts

The dictionary-round finding is closed. A selected dictionary over the
3,402-byte battery fixture with `tailbytes=1024` produced exactly three
planner waves:

```text
tail:     [(2378, 1024)]
metadata: [(632, 184), (848, 592), (1760, 592)]
bodies:   [(816, 32), (1744, 13)]
```

The body wave contains both the dictionary body and the selected record
buffer. Its requests completed `[2, 1]`; dictionaries decoded first from the
shared `FetchedSpans`, and the public result matched the in-memory table. The
dead `blockwants` name is absent.

With the default 64 KiB tail, the entire 3,402-byte fixture is cached by one
request `(0, 3402)`. Its metadata and dictionary/body spans are served from
that cache, so the public selected-dictionary path costs one request, not
three.

The public 600,922-byte probe followed the three-round design with five
requests:

```text
tail:     (535386, 65536)
metadata: (184, 208), (300424, 208)
bodies:   (392, 16), (300632, 16)
```

The two metadata requests and the two body requests each belong to one
concurrent planner round.

### Tail, Footer, and source geometry

The tail and Footer cache findings are closed. The 148,578-byte probe had a
70,224-byte Footer outside its 64 KiB tail. Its public trace was:

```text
(83042, 65536)  tail
(78344, 70224)  exact Footer follow-up
(70192, 144)    record metadata
(70336, 8000)   selected body
```

The exact Footer follow-up occurred once. The schema pass and scan pass reused
it. The table decoded rows `1:1000` correctly.

The integer range cases from round 64 now return `ValidationError`: negative
reported lengths, `Int128(typemax(Int64)) + 1`, and
`Int128(typemin(Int64)) - 1`. Negative `_coalesce` range geometry also returns
`ValidationError`. A source that reported the 3,402-byte object one byte
smaller failed cleanly on the missing trailing magic. One that reported it
one byte larger and returned the available 3,402 bytes failed with
`ValidationError("range fetch returned 3402 bytes, expected 3403")`. No
process crashed. Finding 1 is the separate non-`Integer` return gap.

### Whole-object fallbacks and limits

The fallback finding is closed. Each object below was larger than its tail
window. Each public result matched the corresponding in-memory read. Each
path made two requests whose byte counts summed exactly to the object length:

- a 160,280-byte stream-format object with a scan;
- the 600,922-byte file-format object without a scan;
- that file with `select=()`;
- a 216,626-byte Date32 file with an unrepresentable midday `DateTime`
  filter literal;
- a 140,378-byte zero-field file.

The stream trace was `(94744, 65536)` for the tail and `(0, 94744)` for the
prefix. The other paths had the same tail-plus-prefix shape. A zero-field
`SourceFile` with `Limits(max_array_length=2)` still made the complete
two-request read, then returned the exact `ValidationError` that record-batch
length three exceeds the limit. `_openbytes(...; limits=sf.limits)` therefore
preserves the handle's limits through the whole-object path.

The `Arrow.Table` docstring, manual, and design now describe these fallbacks.
The design records tail, candidate metadata, and one combined body round for a
pushable scan.

## Interface, documentation, and delta audit

No `readranges` occurrence remains outside historical round-64 review text.
The live interface and reference page contain `sourcelength`, `readrange`,
and optional `concurrentreads`. The manual's `HTTPSource` sketch defines the
two required methods and remains valid. The `AbstractArrowSource` docstring
states that Arrow validates geometry and payload shape but cannot authenticate
same-length bytes claimed for a range.

`16cfb8b` adds only the round-64 record. The functional changes in `d3ea7a7`
close the seven stated defects. I found no ordering race, unassigned-result
use, cache-key error, dictionary/body dependency error, fallback limit loss,
or new data error in the delta. Findings 1 and 2 are in `d3ea7a7`. Finding 3
is a provider claim introduced by the round-64 feature and detected during
this round's required GCS contract check.

## Assumptions and decisions

- I treated a request round as one planner wave. A concurrent source can send
  all requests in that wave together. The request count can be larger than the
  round count.
- I treated arbitrary same-length bytes from a source as trusted, as the new
  interface docstring states. I treated payload type, payload length, source
  length type, and range geometry as structural contract values that must fail
  closed.
- I accepted an empty ETag as an explicit loss of version pinning because no
  usable conditional value exists. I did not treat it as equivalent to a
  verified stable object.
- I classified the non-`Integer` result and repository-rule violations as LOW
  because neither affects a conforming source's data. I classified the GCS
  claim as LOW because it overstates provider availability but does not break
  the working S3 and Azure paths.
- I accepted Documenter's local deployment-skip warning after doctests,
  cross-references, document checks, and HTML rendering completed.
- I did not modify product or test code. All probes and environment changes
  stayed in detached or temporary worktrees. The protected untracked files
  `Arrow_Review.md`, `ISSUE-420.md`, `ISSUE-474.md`, `ISSUE-540.md`,
  `ISSUE-580.md`, and `mytestdata.arrow` remained untouched. This review file
  is the only active-checkout change made by this round.

## Validation

- `julia --project=. --startup-file=no -e 'using Pkg; Pkg.test()'` — final
  exit 0 on Julia 1.12.6 in `/private/tmp/arrow-r65-gates` after developing
  `/Users/jacob.quinn/.julia/dev/Tables`: ArrowCore 421/421, facade 338/338,
  CloudStore/Minio 17/17, and every IPC, C-data, statistics, scan, and
  byte-range battery passed. An initial fresh resolution selected released
  Tables 1.13.0 and exited 1 before tests because that release has no
  `Tables.Scan`; the repository requires the development Tables checkout.
  The exact gate was rerun after that environment setup.
- The required Julia 1.10.11 binary, from a fresh environment that developed
  exact Arrow, `src/ArrowStrings`, and
  `/Users/jacob.quinn/.julia/dev/Tables`: `Pkg.test("Arrow")` — exit 0 with
  ArrowCore 421/421, facade 338/338, CloudStore/Minio 17/17, and all batteries
  passing.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0, 6/6. Its
  assertions report zero verifier errors, zero verifier warnings, and a
  successful trimmed executable.
- `julia --startup-file=no conformance/run.jl` — exit 0: corpus 275 pass,
  0 fail, 36 declared skips; IPC oracle 170 pass, 0 fail, 43 declared skips;
  C-data/C-stream oracle 143 pass, 0 fail, 9 declared skips.
- Documentation setup developed exact `.` and the required Tables checkout
  into `docs/`. `julia --project=docs docs/make.jl` — exit 0. Doctests,
  cross-references, document checks, and HTML rendering passed. The only
  warning was the expected local deployment skip. `docs/Project.toml` was
  restored byte-identically afterward.
- JuliaFormatter 2.12.5 `format(".")` — exit 0 and no tracked change.
- `dev/release/run_rat.sh .` — final exit 0, `No unapproved licenses`, in the
  clean RAT worktree after temporarily moving only its administrative `.git`
  pointer outside the scan. The initial invocation exited 1 only for that
  pointer.
- `/private/tmp/arrow-r65-source-probes.jl`, with `--threads=1` — exit 0,
  58/58 closure assertions plus the non-`Integer` evidence assertions. It
  covers indexed placement, peak concurrency, original exception types,
  hostile payloads and lengths, request traces, dictionary rounds, Footer
  reuse, whole-object fallbacks, equality, and limits.
- `test/cloudstore_tests.jl` in the isolated CloudStore 1.8.0 environment —
  exit 0, 17/17. `/private/tmp/arrow-r65-etag-probe-a8f31.jl` — exit 0 in
  both CloudStore 1.8.0 and 1.6.0 environments with exact 412, header, body,
  exception-type, and fresh-object assertions.
- `julia --threads=1 --project=. --startup-file=no test/facade_tests.jl` —
  exit 0, 338/338.
- `git diff --check e446f97..HEAD`, the final active `git diff --check`, and
  the detached probe worktree checks passed. A no-index whitespace check of
  this untracked review file produced no diagnostic; its expected nonzero
  status only records that it differs from `/dev/null`.
- Final local and live remote refs remained
  `d3ea7a7cf16001985a0237e18c9c873307acbc6c`. The active checkout contained
  only the six protected untracked files and this review document.

VERDICT: FINDINGS
