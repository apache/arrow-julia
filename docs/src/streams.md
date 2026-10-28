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

# Streams and IPC

Arrow has two IPC formats. The **file** format starts with the `ARROW1` magic and ends
with a footer that indexes the record batches, so a reader can open any batch directly;
it suits files on disk. The **stream** format is a schema message, then dictionaries and
record batches, then an end marker; a reader can process it as it arrives, so it suits
pipes, sockets, and HTTP bodies. `Arrow.Table` and `Arrow.Stream` read both and detect
which one they are given. `Arrow.write` writes the file format to a path and the stream
format to an `IO`.

## Read record batches

[`Arrow.Stream(source)`](@ref Arrow.Stream) reads a source one record batch at a time;
each iteration gives an `Arrow.Table` of one batch. It takes the keywords of
`Arrow.Table`: `select`, `drop`, and `filter` apply to every batch, and `limit` counts
rows across batches. A loop that processes and drops each batch holds one batch at a
time, plus the dictionaries, so a source larger than memory reads in bounded memory.

```@example streams
using Arrow, DataAPI, DataFrames, Tables

path = joinpath(mktempdir(), "batches.arrow")
Arrow.write(path, Tables.partitioner([(id=[1, 2], x=[0.5, 1.5]), (id=[3], x=[2.5])]))

stream = Arrow.Stream(path)
[sum(batch.x) for batch in stream]
```

A stream is its own `Tables.partitions`, so `Arrow.write(out, Arrow.Stream(path))` writes
one record batch per batch read. `Tables.schema(stream)`, `DataAPI.metadata(stream)`, and
`DataAPI.colmetadata(stream, :x)` read no data. For the file format, `length(stream)` is
the number of batches, `stream[i]` reads batch `i`, and `DataAPI.nrow(stream)` counts rows
from the batches' metadata:

```@example streams
(length(stream), DataAPI.nrow(stream), DataFrame(stream[2]))
```

A stream-format source has no index: it is read in order, its batch count is known only
once it has been read, and an `Arrow.Stream` over an `IO` can be iterated once.

## Read a stream as it arrives

Over a stream-format `IO`, `Arrow.Stream` reads each record batch when the iteration
reaches it, so a consumer processes batches while the producer is still writing.
[`Arrow.Writer(io)`](@ref Arrow.Writer) is the producer's side: each
`Arrow.write(writer, table)` writes that table's batches to the `IO` before it returns,
and `close(writer)` writes the end marker and leaves the `IO` open. Here the two ends of
a TCP connection are in one process:

```@example streams
using Sockets

port, server = listenany(ip"127.0.0.1", 40000)
producer = @async begin
    socket = accept(server)
    Arrow.Writer(socket) do writer
        for i in 1:3
            Arrow.write(writer, (batch=[i, i], value=[10i, 10i + 1]))
        end
    end
    close(socket)
end

client = connect(ip"127.0.0.1", port)
received = [collect(batch.value) for batch in Arrow.Stream(client)]
wait(producer); close(client); close(server)
received
```

An `IO` that buffers its writes, such as a file's `IOStream`, may hold a batch until
`flush(io)`. [`Arrow.append`](@ref) adds batches to the end of an existing stream in a
file or a seekable `IO`.

`Arrow.Table(io)` reads an `IO` to its end and refuses bytes after a stream's end marker.
To read several streams written one after another to one `IO`, call `Arrow.Stream(io)`
once per stream: each call reads one stream and stops after its end marker.

```@example streams
io = IOBuffer()
Arrow.write(io, (a=[1],))
Arrow.write(io, (a=[2, 3],))
seekstart(io)
[[collect(batch.a) for batch in Arrow.Stream(io)] for _ in 1:2]
```

## Bytes in memory

[`Arrow.tobuffer(table)`](@ref Arrow.tobuffer) writes a table in the stream format to a
new `IOBuffer`, positioned at its start, and `Arrow.Table(bytes)` reads a byte vector in
place:

```@example streams
bytes = take!(Arrow.tobuffer((a=[1, 2],)))
Arrow.Table(bytes).a
```

## Other Arrow implementations

Other Arrow implementations read both formats: pyarrow reads streams with
`pyarrow.ipc.open_stream` and files with `pyarrow.ipc.open_file`, Polars with
`polars.read_ipc_stream` and `polars.read_ipc`. Arrow.jl reads what they write:

```python
import pyarrow as pa, pyarrow.ipc as ipc, sys

table = pa.table({"id": [1, 2, 3]})
with ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
    writer.write_table(table)       # julia: for batch in Arrow.Stream(stdin) ... end
```

To exchange columns with another implementation in the same process without copying,
use the [C data interface](@ref "C data interface").
