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

# Arrow.jl

Arrow.jl reads and writes the [Apache Arrow](https://arrow.apache.org) IPC file and
stream formats in pure Julia. A file reads as Tables.jl columns that view its memory, and
any Tables.jl table writes. The API is namespaced: `Arrow.Table`, `Arrow.write`, and the
rest are called with the `Arrow.` prefix, and only `ArrowTypes` is exported.

## Quick start

```@example quickstart
using Arrow, DataFrames

path = joinpath(mktempdir(), "example.arrow")
Arrow.write(path, (id=[1, 2, 3], name=["Ada", "Ben", missing]))

table = Arrow.Table(path)
table.name
```

```@example quickstart
df = DataFrame(table)
```

[Reading](@ref) covers tables, columns, filters, threads, and sources;
[Writing](@ref) covers the writer's options, dictionaries, and incremental writes;
[Streams and IPC](@ref) covers record batches, pipes, and sockets; [Type mapping](@ref)
lists the column types in both directions; [Limits and errors](@ref) covers resource
limits, errors, and what is not supported. Upgrading from Arrow.jl 2.x? Read
[Migrating from Arrow.jl 2.x](@ref) first. The [API reference](@ref "API reference")
lists the public types and functions.

## Format

Arrow.jl implements version 1.5 of the Arrow columnar format: the IPC file and stream
formats, LZ4 frame and Zstandard compression, dictionary encoding with delta and
replacement dictionaries, extension types, and the [C data interface](@ref "C data
interface"). It reads every data type of that version, and writes all of them except
dictionary encoding inside a nested column. A few types have no Julia type of their own,
such as run-end encoding, list views, sparse unions, and date64; they are written when a
table read from Arrow is written again ([Rewriting tables](@ref)). It needs no Arrow C++
library.
