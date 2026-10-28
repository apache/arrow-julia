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

# Limits and errors

## Errors

Reads and writes throw one of two package errors, or one of these Base errors:

- [`Arrow.ValidationError`](@ref): the input is not valid Arrow data, such as a truncated
  file, inconsistent metadata, an offset or dictionary index out of range, or bytes that
  are not Arrow at all; it uses something this package does not read ([Not
  supported](@ref)); or it declares a size past one of the [Resource limits](@ref). The
  message names the problem, and for a limit the field to raise.
- [`Arrow.AllocationLimitError`](@ref): a read needed more memory than its allocation
  budget allows ([Resource limits](@ref)); the message names the `Arrow.Limits` fields
  to raise.
- `ArgumentError`: an argument that cannot work, such as an unknown keyword, a `select`
  that matches no column, a filter literal of another kind than its column, or, when
  writing, a value with no Arrow type, invalid UTF-8, or a table that does not fit an
  `Arrow.Writer`'s schema. The message names the column.
- `InvalidStateException`: a table or stream used after [`Arrow.release!`](@ref
  Arrow.release!(::Arrow.Table)).
- `SystemError` and `Base.IOError`: the file system, such as a missing file or a
  read-only one.

```@example limits
using Arrow

bytes = take!(Arrow.tobuffer((id=collect(1:10_000),)))
try
    Arrow.Table(bytes[1:(end - 100)])
catch err
    showerror(stdout, err)
end
```

## Resource limits

Pass an [`Arrow.Limits`](@ref) as the `limits` keyword of `Arrow.Table` or `Arrow.Stream`:

| field | default | caps |
|:--|:--|:--|
| `max_metadata_bytes` | 16 MiB | the metadata of one message |
| `max_body_bytes` | none | the body of one message |
| `max_buffer_bytes` | none | one buffer, compressed or not |
| `max_total_allocated_bytes` | physical memory | the memory one read, or one batch of a stream, allocates |
| `max_allocation_ratio` | 64 | that memory per byte of input |
| `max_messages` | 1,000,000 | messages in one file or table |
| `max_metadata_objects` | 1,000,000 | FlatBuffers objects in one message |
| `max_nesting_depth` | 64 | nesting of types and metadata |
| `max_array_length` | 1,000,000,000 | the length of one array |
| `max_concurrent_reads` | 64 | ranges an `Arrow.AbstractArrowSource` reads at once |
| `max_new_symbols` | 1,000,000 | new `Symbol` values one read interns |

A read may allocate `max_allocation_ratio` bytes per byte of its input, at least 256 MiB
and at most `max_total_allocated_bytes`; each batch of an `Arrow.Stream` gets its own
allowance from its own bytes. A compressed buffer counts at its decompressed size, so
ordinary files of any size and compression ratio read with the defaults. Lengths that a
file declares without the bytes to back them fail before memory is reserved for them.
Data that expands much further than its bytes, such as long runs or a heavily repeated
dictionary value, can need a larger `max_allocation_ratio`.

```@example limits
try
    Arrow.Table(bytes; limits=Arrow.Limits(max_array_length=1_000))
catch err
    showerror(stdout, err)
end
```

Symbols last for the rest of the process, so a read interns at most `max_new_symbols`
new `Symbol` values from `Symbol` columns, and refuses a column that needs more before
interning any. Such a column can be read as strings: `Arrow.Table(path; select=[:id =>
String])`.

Column names and time-zone names become `Symbol`s too. A read refuses, with a
`ValidationError`, a name longer than 4096 bytes, a schema that would intern more than
65,536 new column names or 1 MiB of them, and more than 1,024 new time zones.

## Validation

Every record batch is checked before it is used: the metadata by a FlatBuffers
verifier, then each buffer's size against the schema, offsets, dictionary indices, union
type ids, and run ends. Corrupt or hostile input therefore fails with a
`ValidationError` or an `AllocationLimitError` rather than crashing, hanging, or
allocating without bound; the package's fuzz tests check this. Three things the format
leaves to the writer are not checked, as other implementations do not check them: that
strings read are valid UTF-8, that a field declared non-nullable has no nulls (such a
column reads as `Union{Missing,T}`), and the padding bytes.

Writes validate every record batch before its bytes reach the sink.

## Not supported

These fail with a `ValidationError`, or an `ArgumentError` when writing:

- Big-endian IPC data.
- Metadata versions before V4, and the pre-1.0 experimental compression that predates
  the format's body compression.
- Appending to a file-format file; append to the stream format or use an `Arrow.Writer`.
- Writing a `BigInt`, an `@enum`, a multidimensional array, or another type with no Arrow
  type: convert it, or map it with ArrowTypes ([Custom types](@ref)).
- Writing dictionary encoding inside a nested column (it reads), and rewriting, as it
  was read, a union with two children of the same type ([Rewriting tables](@ref)).
