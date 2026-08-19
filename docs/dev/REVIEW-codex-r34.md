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

# Arrow.jl 3.0 code review — round 34

Date: 2026-08-17

Scope: exact commit `5c170df83a838ee76214350d5264d3087a0f5eaa`,
judged against the seven round-33 closure probes and the additional machinery
listed in the round-34 request. Its parent is
`8359aab08a08adcf8e2363a640ed1519e40a338a`.

## Result

The round-33 fixes are not closed. Partition name/order validation, the core
schema-authority cases, ranged schema carriage, the standard shared dictionary
pool, and the requested DataAPI defaults/selectors/aggregates passed. The new
paths still contain four high-severity correctness defects: temporal scan
lowering changes predicate semantics; retained rewrites are not schema
identity; incompatible replacement columns can be silently reinterpreted; and
scan type overrides run in the physical storage domain. Zero-column row counts
and DataAPI missing-column behavior also remain incomplete.

## Findings

1. **HIGH — temporal literal lowering is not semantics-preserving.**

   `_storagevalue` has three independent correctness problems at
   `src/table.jl:219-249`.

   - `DateType` uses the `DateUnit` value `MILLISECOND_DATE`, but line 227
     compares it with `AC.MILLISECOND`, which is a `TimeUnit`. A Date64
     `DateTime` literal is therefore never lowered.
   - The closed branches handle `Date` for `DateType` and `DateTime` for
     `TimestampType`, but they omit Julia's valid Date/DateTime cross-type
     equality. Date32 compared with an equivalent midnight `DateTime`, and a
     second/millisecond Timestamp compared with an equivalent `Date`, disagree
     with the public-value authority.
   - Line 249 returns any unrecognized literal unchanged. An integer can then
     compare against temporal physical storage even though it is unequal to
     the public Date value. The microsecond/nanosecond multiplication at lines
     231-232 is also unchecked, so an unrepresentable DateTime can wrap into a
     matching Int64 instead of producing an exactness error.

   A valid file/stream/ranged differential produced:

   ```text
   predicate                      Tables.finish   file   stream   ranged
   Date32 == midnight DateTime    [2]             []     []       []
   Date64 == DateTime             [2]             []     []       []
   Timestamp[s] == Date           [2]             []     []       []
   Timestamp[ms] == Date          [2]             []     []       []
   Date(1970-01-02) == 1          []              [2]    [2]      [2]
   ```

   Renamed temporal outputs were included. `Cmp`, `In`, and nested boolean
   lowering were exercised. Date32-with-Date, Timestamp-with-DateTime, and
   Time-with-Time controls passed; Date64-with-DateTime is the exception above.
   Twelve finer-than-storage-unit controls produced the intended clean
   `ArgumentError`. The fix needs a tagged converted/incompatible result, not a
   pass-through fallback, plus checked scaling and explicit Date/DateTime
   compatibility.

2. **HIGH — the retained read→write schema transition is not identity.**

   Date64 cannot be rewritten at all: the same unit typo reaches
   `_retainedstorage` at `src/write.jl:121-125` and ends in
   `MethodError: Int64(::DateTime)`. Every other tested non-nullable temporal
   descriptor becomes nullable. `_retainedstorage` always creates a
   `Union{Missing,Int64}` vector, then `_rebuildtemporal` derives nullability
   from that temporary vector at `src/write.jl:156-173`.

   The exact transition probe reported:

   ```text
   Date32        type_equal=true   nullable=false -> true
   Date64        rewrite error
   Timestamp s   type_equal=true   nullable=false -> true
   Timestamp ms  type_equal=true   nullable=false -> true
   Timestamp us  type_equal=true   nullable=false -> true
   Timestamp ns  type_equal=true   nullable=false -> true
   Time s        type_equal=true   nullable=false -> true
   Time ms       type_equal=true   nullable=false -> true
   Time us       type_equal=true   nullable=false -> true
   Time ns       type_equal=true   nullable=false -> true
   Duration s    type_equal=true   nullable=false -> true
   ```

   Dictionary descriptors also drift. An unchanged
   `Dictionary(Int8, Utf8, ordered=true)` rewrote as
   `Dictionary(Int32, Utf8, ordered=false)`. The main dictionary branch infers
   `f1` and installs `f1.type` at `src/write.jl:277-295`; it does not call the
   retained-type-checking overload at lines 176-188.

   Standard schema and top-level field metadata survived an unscanned default
   dictionary rewrite, and the requested default DictEncode file case used one
   shared pool across both batches. Those controls do not cover complete
   descriptor identity. Retained nullability must come from the retained Field,
   subject to validating the values, and dictionary rebuilding must use and
   validate the retained index type, value type, and ordered flag.

3. **HIGH — a replaced facade column can be silently interpreted under a
   stale retained schema.**

   The temporal retained path treats every integer vector as physical storage
   at `src/write.jl:129-141`. Replacing a Date32 column with `Int64[100, 101]`
   succeeded and reread as `Date(1970-04-11)` and `Date(1970-04-12)` instead of
   rejecting an incompatible public column. A replaced Timestamp-millisecond
   column was similarly interpreted as epoch milliseconds.

   The dictionary path is more permissive. Replacing a retained
   `Dictionary<Utf8>` column with `Int64[7, 8]` succeeded and silently changed
   it to `Dictionary<Int64>` because the path at `src/write.jl:277-295` never
   compares the inferred descriptor with the retained descriptor. A
   non-nullable retained primitive replaced with a vector containing `missing`
   also silently widens because line 149 uses `f.nullable || fn.nullable`.

   The fast path must first prove that the visible column matches the retained
   facade type. Raw integer carriage is valid only for descriptors that the
   facade itself exposes as raw integers, such as microsecond/nanosecond
   timestamps. An incompatible replacement must take a clearly defined
   re-inference path or fail with a descriptive `ArgumentError`; it must not be
   reinterpreted as retained physical storage.

4. **HIGH — scan type overrides execute in the physical domain and the scan
   result retains the wrong schema.**

   The file and ranged paths pass the lowered scan, including public output
   type overrides, into `Tables.scan` before facade conversion
   (`src/table.jl:336-361`). The stream path does the same with raw columns.
   `_wrapscanned` then converts again according to the original bound selection
   at lines 395-416.

   Two valid controls failed in file, stream, and ranged paths:

   ```text
   nullable Int64 => Float64
   authority: Union{Missing,Float64}[1.0, missing]
   facade:    MethodError converting missing to Float64

   Date => Date
   authority: valid no-op
   facade:    MethodError converting raw Int32 to Date
   ```

   `_wrapscanned` also stores the original full source schema at line 416, not
   a schema derived from `Tables.bind`. A Timestamp-second field renamed by a
   scan reads correctly, but a later facade rewrite infers Timestamp-millisecond
   and drops its field metadata. A rename that collides with another original
   field name can bind the output to that unrelated retained field.

   Ordinary binding cardinalities aligned in tested nonempty cases, including
   `Scan()`, `validate=false`, regex, `Not`, and duplicate-source selections
   with unique output names. The `break` at line 407 should still be an asserted
   invariant, not silent partial conversion. Public type overrides must remain
   residual until after public conversion, and `_wrapscanned` must construct a
   bound output schema.

5. **MEDIUM — zero-column row counts are still lost for reporting sources and
   empty scan projections.**

   `_writebytes` iterates each partition but asks
   `Tables.rowcount(Tables.columns(part))` at `src/write.jl:244-257`. A custom
   source whose own `Tables.rowcount(source)` returned 3, but whose separate
   zero-column `NamedTuple()` could only report 0, wrote zero rows in both IPC
   formats. This is a reporting source under the round-34 rule; the writer is
   querying the wrong object.

   `select=()` over a three-row, one-column input also returned zero columns
   and row count 0 for file, stream, and ranged inputs. The file/ranged apply
   stage creates a row-count-carrying `_scantable` at `src/scan.jl:525-551`,
   but its residual empty projection passes through `Tables.finish` and loses
   the count. `_wrapscanned` then trusts that zero at `src/table.jl:415-419`.

   A separate valid zero-column, three-row IPC stream also became zero rows
   when read with `scan=Tables.Scan()`, while the equivalent file and ranged
   inputs retained three. The stream branch first constructs an empty raw
   `NamedTuple` at `src/table.jl:357-360`, which has already lost the batch row
   count before `_wrapscanned` can preserve it.

   Direct read→write of a low-level zero-column three-row batch passed, as did
   sources whose columns object itself reports the row count. The remaining
   paths need to retain the partition's reported count and carry the scan
   stage's authoritative row count across an empty projection.

6. **MEDIUM — DataAPI methods do not reject missing visible columns.**

   Defaults, style tuples, Symbol/Int selectors, aggregate keys, aggregate
   dictionaries, and `KeyError` with a missing schema passed. Missing-column
   conformance did not:

   ```text
   DataAPI.colmetadatakeys(t, :z)        -> ()
   DataAPI.colmetadata(t, :z, "u", 7)   -> 7
   ```

   DataAPI permits a default only for a missing key on an existing column; a
   missing column must error. `_schemafield` searches the retained source schema
   without first checking the visible names at `src/table.jl:98-108`, and
   `colmetadata` treats a missing field like a missing key at lines 114-123.
   After `select=(:y,)`, metadata for unselected source column `:x` was still
   readable even though `:x` was not a column of the table. The output-schema
   fix from finding 4 should make the visible column set authoritative here.

7. **LOW — integer-width drift errors still hide the mismatching descriptor
   values.**

   Swapped names and a one-column rename now reject before binding. Int64→Int32
   and Int32→Int64 also reject with the correct partition and column. Both
   directions, however, produce the same message:

   ```text
   ArgumentError: partition 2 column x maps to Arrow type
   Arrow.ArrowCore.IntType, but the first partition declared
   Arrow.ArrowCore.IntType; make the column types agree across partitions
   ```

   `summary` at `src/write.jl:312-317` omits `bits` and `signed`. Round 33 asked
   for width and signedness as well as partition and column, so that diagnostic
   part remains incomplete.

8. **LOW — the target diff fails the whitespace check.**

   `git diff --check 8359aab..5c170df` exits 2 because `src/scan.jl:788` is a
   whitespace-only line with trailing spaces.

## Clean portions of the requested sweep

- Schema authority passed: all-missing nullable UTF-8 materialized as
  `Union{Missing,String}`, rewrote as nullable UTF-8, and did not become a
  Date; nullable primitive/string columns with no null values stayed nullable;
  typed zero-row and zero-record-batch file/stream sources stayed typed.
- Ranged facade reads carried Timestamp conversion, schema metadata, and field
  metadata. A selective probe fetched 10,480 of 110,914 bytes in seven calls.
  This includes the declared extra schema/footer fetch and remains sparse.
- Multi-partition default `DictEncode` file output emitted one dictionary and
  two record batches with the same pool object and correct values.
- `_lowerexpr` covers the current ScanExpr node set. `Cmp` and `In` lower their
  literals; `AndExpr`, `OrExpr`, and `NotExpr` recurse. `StrPred` needs no
  lowering because materialization supplies decoded strings. `IsNull` needs no
  lowering because it has no literal and validity becomes `missing`.
  `AlwaysTrue`/`AlwaysFalse` need no conversion; bare `Col` and generic
  `OpNode` filters are rejected by Tables.jl. Focused StrPred, IsNull, negated
  IsNull, and empty And/Or probes matched authority.
- Retained temporal writing calls `_storagevalue`, so scan and retained rewrite
  share that conversion path. The natural writer still duplicates canonical
  Date/DateTime/Time/Period closures at `src/write.jl:41-59`; those canonical
  conversions agreed in focused controls. The shared `_storagevalue` bugs are
  why scan and retained rewrite fail together.

## Assumptions and decisions

- I treated the seven enumerated round-34 probes as the scope authority. I did
  not reopen unrelated round-33 findings 8-11.
- I used `Tables.finish` over facade/public Julia values as the scan semantic
  authority, as required by the Tables.Scan contract.
- I treated schema identity as including logical descriptor parameters,
  nullability, schema metadata, and field metadata. Dictionary index type and
  ordered state are descriptor parameters.
- I treated a source as reporting a zero-column row count when
  `Tables.rowcount(source)` has an explicit result, even if a separate columns
  object cannot infer it.
- I treated an incompatible user replacement as requiring a clean rejection or
  an explicit re-inference path. Silent physical reinterpretation is not a
  valid retained-schema rewrite.
- I made no product changes. I added only this review. I preserved the six
  pre-existing untracked files.

## Validation

- `julia --project=. -e 'using Pkg; Pkg.test()'` — exit 0; Core 342/342,
  threaded Core 4/4, facade 95/95, and all adapter batteries passed.
- `julia --startup-file=no test/trim_compile_tests.jl` — exit 0; 6/6 and zero
  trim-verifier errors.
- `julia --project=conformance --startup-file=no conformance/corpus.jl` — exit
  0; 275 pass / 0 fail / 36 skip.
- `julia --project=conformance --startup-file=no conformance/oracle.jl` — exit
  0; 170 pass / 0 fail / 43 skip with the existing daemon and cached image.
- Focused round-33 pass controls — exit 0; 100/100 assertions across schema
  authority, matching temporal literals, exactness errors, ranged schema and
  metadata, rewrite controls, zero-column direct round trips, and requested
  DataAPI calls.
- Focused finding capture — exit 0; 44/44 assertions pinned the incorrect
  temporal results, retained-schema transitions, replacement behavior, empty
  projection counts, reporting-source count loss, and missing-column DataAPI
  behavior.
- `git diff --check 8359aab..5c170df` — exit 2; trailing whitespace at
  `src/scan.jl:788`.
- Final status retained all six pre-existing untracked files and added only
  this report.

VERDICT: FINDINGS
