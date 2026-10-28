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
hand columns and record batches to another Arrow implementation in the same process,
such as pyarrow through PythonCall, without copying them. The producer fills C structs
(`ArrowSchema`, `ArrowArray`, `ArrowArrayStream`) and the consumer calls their `release`
callbacks when it is done.

The functions work on low-level values rather than tables: an [`Arrow.Field`](@ref)
describes a column, an [`Arrow.ArrayData`](@ref) holds its buffers, and an
[`Arrow.RecordBatch`](@ref) is a [`Arrow.Schema`](@ref) with one `ArrayData` per field.
[`Arrow.fromjulia`](@ref) and [`Arrow.batch`](@ref) build them from Julia vectors, and
[`Arrow.materialize`](@ref) turns imported data back into a Julia vector.

```@example cdata
using Arrow

field, data = Arrow.fromjulia("x", [1, 2, missing, 4])
schemaptr, arrayptr = Arrow.to_c_data(field, data)    # export
field2, data2 = Arrow.from_c_data(schemaptr, arrayptr) # import
Arrow.materialize(Union{Missing,Int}, field2, data2)
```

- [`Arrow.to_c_data`](@ref) exports one column and returns pointers to its structs. They
  stay valid until the consumer calls their `release` callbacks; [`Arrow.reap!`](@ref)
  then frees what Arrow.jl kept for them.
- [`Arrow.from_c_data`](@ref) imports one column. It takes over the array, as the
  interface requires, and its buffers stay valid while the returned data is reachable.
  `Arrow.release!(data.owner)` gives them back to the producer at once.
- [`Arrow.export_stream!`](@ref) fills a caller-allocated `ArrowArrayStream` from a
  schema and record batches; [`Arrow.from_c_stream`](@ref) imports one, and
  [`Arrow.nextbatch!`](@ref) reads its batches until it returns `nothing`.

Exported and imported data is validated as an IPC read is. `copy(data)` copies an
imported column into Julia-owned memory, and `Serialization.serialize` saves the values
of `ArrayData` and record batches. Do not use `deepcopy` to detach them: the copy still
points at the producer's memory, which the producer may free.

With PythonCall, a column goes to pyarrow like this:

```julia
using Arrow, PythonCall
pa = pyimport("pyarrow")

field, data = Arrow.fromjulia("x", [1, 2, missing, 4])
schemaptr, arrayptr = Arrow.to_c_data(field, data)
array = pa.Array._import_from_c(UInt(arrayptr), UInt(schemaptr))
array.to_pylist()  # [1, 2, None, 4]
```
