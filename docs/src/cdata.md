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
and [`Arrow.to_c_data`](@ref) as one record batch; [`Arrow.Table`](@ref) and
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

`read_all()` reads every batch on the calling Julia thread. Polars and DuckDB take the
pyarrow table it returns: `polars.from_arrow(pytable)`, or DuckDB's
`connection.register("name", pytable)`. Give DuckDB the table, not the reader: DuckDB
reads a reader on its own threads, which can deadlock with Julia's garbage collector
(see [Threads](@ref cdata-threads)). The `Ref`, `GC.@preserve`, and `unsafe_convert`
lines are the C ABI: the caller owns the struct's memory while the other side fills or
empties it.

One record batch crosses as two structs that Arrow.jl allocates, and comes back from
structs the caller allocates:

```julia
schemaptr, arrayptr = Arrow.to_c_data(df)
batch = pa.RecordBatch._import_from_c(UInt(arrayptr), UInt(schemaptr))

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
  and the consumer sees the same schema and values as from the IPC bytes. Numbers,
  booleans, and validity bitmaps cross without copying, from `Vector`s of numbers and
  from an [`Arrow.Table`](@ref)'s columns, inside lists and structs too. String and list
  offsets are rebuilt and string bytes may be copied; dates, times, dictionary-encoded
  columns, and other Julia values are built into new Arrow buffers, as a write builds
  them. `export_stream!` sends one record batch per `Tables.partitions` partition
  (an `Arrow.Stream` is a partition per batch) and splits a batch only when its string
  data outgrows 32-bit offsets; `to_c_data` refuses a table that needs more than one
  batch.
- Imports have the column types an IPC read gives: strings as `DataString`, timestamps
  as `Timestamp` or `ZonedTimestamp`, decimals as `Decimal`, nested values as the same
  views, with `stringtype`, `ntasks`, and `limits` as for `Arrow.Table`. Numbers, dates,
  and other fixed-width values read the producer's memory in place; string and binary
  values are copied into Julia-owned buffers. A C stream read whole chains its batches
  into one table. `copy(table)` copies all of it into Julia-owned memory.
- Exported and imported data is validated as an IPC read is.

## Memory

An export keeps its source reachable until the consumer has released it: the table's
columns, or the vectors it was built from. The structs Arrow.jl allocated are then freed
by a later export or a garbage collection, with nothing to call. The consumer reads
exported vectors in place, so modifying one changes what it reads; resizing one is safe
from Julia 1.11 on, where the exported storage stays alive, and frees the consumer's
bytes on Julia 1.10.

An imported table's columns read the producer's memory, and the producer's `release`
callback runs once the table and its columns are garbage collected, as a mapped file is
unmapped. [`Arrow.release!`](@ref)`(table)` only refuses further column access. On an
[`Arrow.Stream`](@ref) over a C stream, `Arrow.release!` of the stream or of a table it
yielded releases the producer's stream at once and refuses further column access, as
for any stream; columns already taken stay valid. `deepcopy` of imported data shares
its memory and its release with the original; use `copy`.

## [Threads](@id cdata-threads)

The callbacks Arrow.jl exports are Julia functions. A consumer that calls them from its
own threads, as DuckDB's and Arrow C++'s thread pools do, makes Julia adopt those
threads. Such a consumer can deadlock with Julia's garbage collector while the Julia
thread that called it waits for it: a collection waits for that Julia thread, which
waits for the consumer, whose threads wait inside the callbacks for the collection.
DuckDB's scan of a `RecordBatchReader` works this way, so read the stream with
`read_all()` on the calling thread first and give DuckDB the pyarrow table.

On Julia 1.12 and 1.13 a process in which such a consumer thread ran a callback hangs
at exit, when the consumer's library joins its threads (a Julia bug; 1.10 and 1.11 exit
normally). Closing the consumer, as with DuckDB's `connection.close()`, does not avoid
it, because the threads belong to the library's thread pool. Until Julia fixes it,
`ccall(:_exit, Cvoid, (Cint,), 0)` ends such a process at once; it skips exit hooks, so
flush output and close files first.

A producer's `release` callback for imported data runs on the thread that imported
it: when a garbage collection on another thread finds the data unreachable, the
importing thread runs the callback at its next yield. pyarrow's release of memory it
shares with NumPy takes Python's GIL, which PythonCall's main thread holds, so data
imported on the main thread is released there without waiting on the GIL.

## Low-level columns

[`Arrow.to_c_data`](@ref)`(field, data)` exports one column given as an `Arrow.Field`
and an `Arrow.ArrayData`, and [`Arrow.from_c_data`](@ref) imports one, returning that
pair. These are the forms the conformance suite and a custom producer or consumer use;
tables need neither.
