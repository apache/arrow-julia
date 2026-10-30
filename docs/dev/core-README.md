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

# The Arrow.jl engine

Arrow.jl separates the Arrow memory layout from its transports and Julia table
interface. `ArrowCore` owns descriptors, buffers, and validation. IPC and C data
adapters consume those values. The table facade presents Julia columns and
Tables.jl operations; the writer converts those columns back into Arrow buffers.
`ArrowCore` is an internal module, not a separately versioned package.

The user guides describe the supported [types](../src/types.md),
[read operations](../src/reading.md), [writes](../src/writing.md),
[streams](../src/streams.md), and [limits](../src/limits.md). This document explains
where those contracts are implemented and the constraints on changing them.

## Code map

| File | Responsibility |
|---|---|
| `src/ArrowCore.jl` | Ownership regions, checked buffer slices, type descriptors, fields, schemas, array layouts, validation, and low-level builders/accessors |
| `src/FlatBuffers/`, `src/metadata/` | Vendored FlatBuffers runtime and generated metadata bindings/verifier; `tools/fbsgen.jl` regenerates the latter from the vendored schemas |
| `src/ipc_read.jl` | Framing, limits, metadata decoding, layout decoding, and decompression |
| `src/ipc_dictionary.jl` | Dictionary snapshots, replacement and delta updates, shared IDs, and dictionary dependencies |
| `src/ipc_write.jl` | File indexes, IPC encoding, batch emission, compression, and incremental writer state |
| `src/cdata.jl` | C data and C stream import/export, callbacks, and foreign ownership |
| `src/source.jl` | Byte-range sources and read scheduling; `ext/ArrowCloudStoreExt.jl` supplies the CloudStore integration |
| `src/columns.jl` | Read-only buffer views, typed nested rows, dictionary/run-end columns, and deferred string construction |
| `src/table.jl` | `Table`/`Stream`, source opening, Tables.jl and DataAPI interfaces, copying, and public value conversion |
| `src/scan.jl` | Scan binding, projection, filters, row windows, statistics, and sparse ranged reads |
| `src/arrowtypes.jl`, `src/sharedvalues.jl` | ArrowTypes mappings and the shared string, decimal, and temporal value domains |
| `src/columnconstruction.jl` | Native and custom column construction, retained schemas, nested layouts, and dictionary pooling |
| `src/write.jl` | Public write options, partition iteration, `Writer`, append, and destination ownership |
| `src/precompile.jl` | Representative public reads and writes used to prepare the package image |
| `src/ArrowTypes/` | The separately registered ArrowTypes interface package; it is developed with Arrow in this repository |

The only exported compatibility binding is `ArrowTypes`. Public entry points are
listed in `src/Arrow.jl`; implementation helpers are not part of that API.

## Buffers and lifetime

An `ArrayData` describes a column through checked `BufferSlice`s, child arrays,
an optional dictionary, and a logical offset and length. Each slice retains an
`OwnerRegion`: a pointer, extent, alignment, GC root, and shared release cell.
The layout registry describes buffer roles and child shapes. IPC dictionary IDs
stay in the adapter; Core dictionaries describe their values and index layout.

Low-level access checks bounds and the region's release cell. Releasing a foreign
owner revokes its regions and invokes the producer's callback exactly once.
Exported objects retain their roots until their C release callbacks run and
`reap!` removes the registry entries. Re-exporting imported data pins the foreign
owner until the consumer has released it.

The public table facade has a different lifetime contract. Its numeric and nested
columns keep their backing storage alive and read without a per-element release
check. `release!(table)` prevents later column retrieval; `release!(stream)` also
prevents iteration. Columns already handed out remain valid. Facade memory maps
are therefore unmapped by GC after the table and its columns become unreachable,
not eagerly by `release!`. A low-level region can use eager release where its
caller controls every access. Keep these two contracts separate.

Input byte vectors and borrowed Julia vectors must not be resized or changed
while a read or low-level view uses them. External truncation of a mapped file
can cause a process-level bus error; the facade cannot guard against it. The
reader's `mmap=false` option avoids that dependency. Replacing an existing string
path through `Arrow.write` uses a temporary file and rename, so existing mappings
continue to see the old file.

C pointers carry declared extents, not verifiable allocation sizes. The C data
producer must keep those allocations alive and unchanged until release. Exported
callbacks run Julia code: call them from Julia-attached threads, and serialize
calls on one C stream. The interfaces do not supply a foreign-thread trampoline.

## Reading and validation

IPC framing and the generated FlatBuffers verifier check sizes and metadata
geometry before metadata-directed allocations. Every batch buffer belongs to a
checked slice of that message's body. Compressed buffers decode into bounded,
exact-sized outputs; declared lengths and codec consumption are checked. Both
LZ4_FRAME and ZSTD are supported, including the stored-raw buffer sentinel.

Structural validation checks layout geometry. Semantic validation checks the
conditions needed to read those layouts safely. `validate_full` adds stricter
content checks such as UTF-8, decimal precision, time-of-day range, advisory
nullability, and canonical bitmap padding. These are distinct contracts: do not
turn an optional content check into an unconditional reader refusal without
checking interoperability and the public type conversion that follows it.

File reads use the footer's schema and batch index, including files whose data
section has no leading schema message. Stream reads consume messages in order.
Dictionary replacements and deltas publish new pools; earlier batches retain
their own snapshots. File indexes and ranged reads must preserve the same
values when dictionary updates and record batches occur in different orders.

`Arrow.Table` exposes read-only views for primitive, Boolean, dictionary,
run-end, list, map, and struct layouts. Multi-batch columns use
`SentinelArrays.ChainedVector`. Native scalar conversions preserve the shared
public domains at every depth: Durations timestamps, DataDecimals decimals,
and the supported date/time/interval values. Custom ArrowTypes mappings may
require materialization.

String and binary columns use DataStrings. The reader reserves their construction
budget up front and builds their payloads on first column access. Owned
decompression buffers can be retained; other input bytes are copied into
DataStrings buffers. Opening a table alone does not pay all of that work.
`stringtype=String` instead constructs Julia strings, including nested
strings. A benchmark that claims to read every column must retrieve every column
inside its timing. Neither retrieval nor `length(column)` visits every value in
a numeric or nested view.

`Arrow.Limits` bounds metadata, array sizes, concurrent reads, new Symbol values,
and allocated output. The allocation allowance depends on input bytes, with a
minimum of 256 MiB and the caller's `max_total_allocated_bytes` ceiling. Compressed
input receives allowance for its supported decompressed data. Each stream batch
has its own budget, so a long stream is not charged against one cumulative cap.
The accounting bounds package-controlled work; it does not measure arbitrary
user-defined conversion hooks or the entire process heap.

## Scans and ranged sources

`Tables.Scan` is available from registered Tables.jl 1.14 or later; no source
revision is required. The facade accepts it directly and translates its simpler
read keywords into the same plan. Binding, public-to-storage literal conversion,
and column selection happen once per request.

A file scan decodes the selected columns and those needed by its filter. Without
a filter, offsets and limits select batches by their row counts. With a filter,
qualifying rows are counted before applying the result window. Output columns
are constructed only for the rows kept. Type overrides apply in the facade's
public value domain. Repeated top-level names receive unique suffixes before
selection and metadata lookup.

`SourceFile` uses a byte-range source's footer and index to request needed metadata
and buffers. Adjacent ranges may be coalesced under `gapbytes`; concurrency is
bounded by the source and reader limits. File and stream sources use different
read strategies, including stream windows and prefetching. Do not infer a bound
on bytes fetched from a bound on rows returned: filtering can still require
reading buffers for rows that are later rejected.

Embedded per-batch statistics use `JuliaArrow:batch_statistics.v1` metadata. They
can prove batches irrelevant to a predicate, but their correctness is a producer
contract. Narrow, incorrect statistics can hide rows. Rewriting a table drops
inherited batch statistics because its values or batch boundaries may have
changed. Schema and field metadata remain ordered pairs so duplicate metadata
keys and their ordering can survive a retained-schema rewrite.

## Writing and concurrency

`write` and `Writer` share the incremental IPC machinery. They consume one
partition at a time and divide it into record batches. `batchsize` caps rows;
overflow of a 32-bit string/list offset splits a batch further. The source
partition itself remains live while its batches are written. Reused construction
buffers and dictionary pools also contribute to peak memory, so a row-count cap
is not a byte quota on arbitrary input iterators.

`ntasks` bounds work on independent columns and compression buffers. Publication
and dictionary state remain ordered. Output must be independent of the task
count, and a single caller owns a `Writer`; overlapping writes are unsupported.
Readers similarly parallelize suitable column construction while preserving
batch order and ownership.

The first input establishes an incremental writer's schema. Later inputs use
compatible conversions and the declared nullability policy. Dictionary pools can
grow: file output normally emits the complete dictionary at close, while stream
output emits an updated dictionary before the next dependent batch. Delta and
replacement modes have explicit options and consumer compatibility constraints
in the [stream guide](../src/streams.md). `append` resumes an existing IPC stream's
schema and final dictionary state; it does not append to the random-access file
format.

Fresh Julia columns and retained-schema rewrites use the same recursive column
constructor. Retained descriptors remain authoritative where values and views
preserve enough information to reproduce them. Unsupported reconstruction must
fail instead of silently relabeling values. `dictencode=false` deliberately writes
pooled columns as plain values, while `DictEncode` remains an explicit encoding
request. Automatic dictionaries use DataAPI ref pools; optional CategoricalArrays
methods preserve ordered categories. These hooks avoid making DataFrames or CSV
runtime dependencies.

## Verification and compilation

On Julia 1.10, develop ArrowTypes explicitly because that Julia version does not
read `[sources]` entries:

```sh
julia --project=. -e 'using Pkg; Pkg.develop(path="src/ArrowTypes"); Pkg.instantiate()'
julia --project=. -e 'using Pkg; Pkg.test()'
julia --project=. test/fuzz.jl --cases 16 --mutations 64
julia --startup-file=no test/trim_compile_tests.jl
julia --startup-file=no conformance/run.jl
julia --project=. bench/run.jl
julia tools/fbsgen.jl src/metadata/fbs src/metadata
```

The package suite includes Core, public interfaces, optional integrations,
frozen Arrow 2.x fixtures, corruption cases, and four adapter acceptance suites.
The deterministic fuzzer records seeds and replay artifacts. Conformance compares
IPC with the pinned Apache corpus and PyArrow/nanoarrow, and exchanges actual
C data/C stream pointers with PyArrow in one process. `conformance/run.jl` prepares
a Docker image and its Julia/Python environments; direct tool and corpus versions
are pinned in the conformance files, while transitive dependencies and the base
image are not fully locked.

The trim gate exercises a specific low-level Core/C data workload and requires
zero verifier warnings or errors plus a runnable binary. It does not establish
that arbitrary table schemas, custom mappings, or the complete facade compile
under `--trim=safe`. Closed descriptor dispatch, concrete containers, and runtime
C-function pointers keep that workload compilable. Never store raw `@cfunction`
pointers in module-level constants: precompilation cannot preserve their runtime
addresses.

The public precompile workload and invalidation regression test cover a separate
concern: loading other data packages should not force Arrow's common operations
to compile again. Abstract extension calls use bounded specialization and
inference barriers where required. Preserve these boundaries when adding public
methods, and measure first use separately from warmed throughput.
