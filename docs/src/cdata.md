```@raw html
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
```

# C data interface

The Arrow [C data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
and [C stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
hand tables to another Arrow implementation in the same process, such as pyarrow, Polars,
or DuckDB through PythonCall, as pointers to Arrow buffers instead of IPC bytes. The
producer fills C structs (`ArrowSchema`, `ArrowArray`, `ArrowArrayStream`); the consumer
moves them out and calls their `release` callbacks when it is done.

[`Arrow.export_stream!`](@ref) exports any Tables.jl table as a stream of record batches
and [`Arrow.export_c_data!`](@ref) as one record batch. [`Arrow.Table`](@ref) and
[`Arrow.Stream`](@ref) import from pointers to the structs. An exported table is built
as [`Arrow.write`](@ref) builds it, so the other implementation sees what it would read
from the IPC bytes, and an imported table has the column types an IPC read gives.

With PythonCall, a `DataFrame` goes to pyarrow and a pyarrow table comes back like this:

```julia
using Arrow, PythonCall, DataFrames
pa = pyimport("pyarrow")

# Julia -> pyarrow: any Tables.jl table, as a stream of record batches
df = DataFrame(id=1:3, name=["a", "b", missing])
stream = Ref{Arrow.CArrowArrayStream}()
pytable = GC.@preserve stream begin
    p = Base.unsafe_convert(Ptr{Arrow.CArrowArrayStream}, stream)
    Arrow.export_stream!(p, df)
    pa.RecordBatchReader._import_from_c(UInt(p)).read_all()
end

# pyarrow -> Julia: pyarrow fills the struct and Arrow.Table moves it out
stream = Ref{Arrow.CArrowArrayStream}()
t = GC.@preserve stream begin
    p = Base.unsafe_convert(Ptr{Arrow.CArrowArrayStream}, stream)
    pytable.to_reader()._export_to_c(UInt(p))
    Arrow.Table(p)
end
t.name   # the strings and the missing value, as Arrow.Table(bytes) reads them
```

`read_all()` pulls every batch on the calling Julia thread. Polars takes the resulting
table through `polars.from_arrow(pytable)`. DuckDB's Python adapter can hand even a
table back to native workers through C Stream. For DuckDB, first put the batches in
PyArrow-owned memory and then register the table. See [Threads](@ref cdata-threads).
The `Ref` and `GC.@preserve` keep the caller's base struct alive while the consumer
moves it.

One record batch crosses as a pair of caller-owned base structs in either direction.

```julia
aref = Ref{Arrow.CArrowArray}()
sref = Ref{Arrow.CArrowSchema}()
batch = GC.@preserve aref sref begin
    ap = Base.unsafe_convert(Ptr{Arrow.CArrowArray}, aref)
    sp = Base.unsafe_convert(Ptr{Arrow.CArrowSchema}, sref)
    Arrow.export_c_data!(sp, ap, df)
    pa.RecordBatch._import_from_c(UInt(ap), UInt(sp))
end

aref = Ref{Arrow.CArrowArray}()
sref = Ref{Arrow.CArrowSchema}()
t = GC.@preserve aref sref begin
    ap = Base.unsafe_convert(Ptr{Arrow.CArrowArray}, aref)
    sp = Base.unsafe_convert(Ptr{Arrow.CArrowSchema}, sref)
    batch._export_to_c(UInt(ap), UInt(sp))
    Arrow.Table(sp, ap)   # a struct array is a table; any other array is one column
end
```

[`Arrow.Stream(streamptr)`](@ref Arrow.Stream) pulls one record batch per iteration
instead of reading the whole stream, for a DuckDB or Polars result larger than memory.

## What crosses, and what is copied

- Exports use [`Arrow.write`](@ref)'s column construction and keywords: `dictencode`,
  `stringview`, `largelists`, `metadata`, and `colmetadata` mean what they mean there,
  and the consumer sees the same schema and values as from the IPC bytes. A `Vector` of
  `Int8` to `Int64`, `UInt8` to `UInt64`, or `Float16` to `Float64` crosses without
  copying, and so do an [`Arrow.Table`](@ref) column's integer, float, and boolean
  values and validity bitmaps, in struct fields and list values too. Everything else is
  built into new Arrow buffers, as a write builds it: `Bool` and `missing` values in
  Julia vectors (a `Vector{Union{Missing,T}}` is copied even when it holds no
  `missing`), other numbers (`Int128`, `Complex`, `Rational`, and decimals, except a
  `Vector` of 128-bit decimals), dates, times, timestamps, durations, fixed-size binary
  and UUID values, string and list offsets, a list's own validity bitmap, fixed-size
  list values, and dictionary-encoded columns. String bytes may be copied.
  `export_stream!` sends one record batch per `Tables.partitions` partition (an
  `Arrow.Stream` is a partition per batch) and splits a batch only when its string data
  outgrows 32-bit offsets or `batchsize` requires it. Multi-partition sources may reuse
  their column buffers, so each batch is copied before requesting another partition.
  Single-batch export refuses a table that needs more than one batch.
- Imports have the column types an IPC read gives: strings as `DataString`, timestamps
  as `Timestamp` or `ZonedTimestamp`, decimals as `Decimal`, nested values as the same
  views, with `stringtype`, `ntasks`, and `limits` as for `Arrow.Table`. Numbers, dates,
  and other fixed-width values read the producer's memory in place; string and binary
  values are copied into Julia-owned buffers. A C stream read whole chains its batches
  into one table. `copy(table)` copies all of it into Julia-owned memory.
- C imports apply `limits` to schema size and depth, array length, and declared buffer
  extents. The producer must supply valid pointers for those extents. The C interface
  can carry dictionary-encoded dictionary values. The IPC format has only one
  dictionary encoding per field.

## Memory

The array export keeps its buffers alive until the consumer releases the array. A cached
schema has its own lifetime. The caller keeps each base struct alive until the consumer
has moved it or finished reading it after release. [`Arrow.to_c_data`](@ref) allocates
the bases for callers that need raw pointers. Free them with [`Arrow.free_c_data!`](@ref)
after use. Never modify or resize a vector that an active consumer reads in place.

An imported table's columns read the producer's memory, and the producer's `release`
callback runs once the table and its columns are garbage collected, as a mapped file is
unmapped. [`Arrow.release!`](@ref)`(table)` only refuses further column access. On an
[`Arrow.Stream`](@ref) over a C stream, `Arrow.release!` of the stream or of a table it
yielded releases the producer's stream at once and refuses further column access, as
for any stream; columns already taken stay valid. `deepcopy` of imported data shares
its memory and its release with the original; use `copy`.

## [Threads](@id cdata-threads)

Arrow.jl supports exported callbacks on Julia-managed threads, including array and
schema release. Serialize calls on each stream. Pull a PyArrow reader with `read_all()`
on the calling Julia thread and keep the resulting table alive there until its Arrow.jl
buffers are released. To hand data to a native worker pool, use a table with its own
buffers. An IPC write and read is one way to transfer ownership.

```julia
ipc = pyimport("pyarrow.ipc")
sink = pa.BufferOutputStream()
writer = ipc.new_stream(sink, pytable.schema)
writer.write_table(pytable)
writer.close()
owned = ipc.open_stream(sink.getvalue()).read_all()
```

This also avoids Julia adopting a library's persistent native threads. Thread adoption
can deadlock with garbage collection or during normal process shutdown on Julia 1.12
and 1.13.

A producer's `release` callback for imported data runs on the thread that imported
it, or on thread 1 when another library's thread imported it inside a callback: when a
garbage collection on another thread finds the data unreachable, that thread runs the
callback at its next yield. pyarrow's release of memory it shares with NumPy takes
Python's GIL, which PythonCall's main thread holds, so data imported on the main thread
is released there without waiting on the GIL.

## Low-level columns

[`Arrow.export_c_data!`](@ref)`(schemaptr, arrayptr, field, data)` exports one column given as an `Arrow.Field`
and an `Arrow.ArrayData`, and [`Arrow.from_c_data`](@ref) imports one, returning that
pair. These are the forms the conformance suite and a custom producer or consumer use;
tables need neither.
