
<a id='API-Reference'></a>

<a id='API-Reference-1'></a>

# API Reference

<a id='Arrow.ArrowVector' href='#Arrow.ArrowVector'>#</a>
**`Arrow.ArrowVector`** &mdash; *Type*.



```julia
Arrow.ArrowVector
```

An abstract type that subtypes `AbstractVector`. Each specific arrow array type subtypes `ArrowVector`. See [`BoolVector`](reference.md#Arrow.BoolVector), [`Primitive`](reference.md#Arrow.Primitive), [`List`](reference.md#Arrow.List), [`Map`](reference.md#Arrow.Map), [`FixedSizeList`](reference.md#Arrow.FixedSizeList), [`Struct`](reference.md#Arrow.Struct), [`DenseUnion`](reference.md#Arrow.DenseUnion), [`SparseUnion`](reference.md#Arrow.SparseUnion), and [`DictEncoded`](reference.md#Arrow.DictEncoded) for more details.

<a id='Arrow.BoolVector' href='#Arrow.BoolVector'>#</a>
**`Arrow.BoolVector`** &mdash; *Type*.



```julia
Arrow.BoolVector
```

A bit-packed array type, similar to [`ValidityBitmap`](reference.md#Arrow.ValidityBitmap), but which holds boolean values, `true` or `false`.

<a id='Arrow.Compressed' href='#Arrow.Compressed'>#</a>
**`Arrow.Compressed`** &mdash; *Type*.



```julia
Arrow.Compressed
```

Represents the compressed version of an [`ArrowVector`](reference.md#Arrow.ArrowVector). Holds a reference to the original column. May have `Compressed` children for nested array types.

<a id='Arrow.DenseUnion' href='#Arrow.DenseUnion'>#</a>
**`Arrow.DenseUnion`** &mdash; *Type*.



```julia
Arrow.DenseUnion
```

An `ArrowVector` where the type of each element is one of a fixed set of types, meaning its eltype is like a julia `Union{type1, type2, ...}`. An `Arrow.DenseUnion`, in comparison to `Arrow.SparseUnion`, stores elements in a set of arrays, one array per possible type, and an "offsets" array, where each offset element is the index into one of the typed arrays. This allows a sort of "compression", where no extra space is used/allocated to store all the elements.

<a id='Arrow.DictEncode' href='#Arrow.DictEncode'>#</a>
**`Arrow.DictEncode`** &mdash; *Type*.



```julia
Arrow.DictEncode(::AbstractVector, id::Integer=nothing)
```

Signals that a column/array should be dictionary encoded when serialized to the arrow streaming/file format. An optional `id` number may be provided to signal that multiple columns should use the same pool when being dictionary encoded.

<a id='Arrow.DictEncoded' href='#Arrow.DictEncoded'>#</a>
**`Arrow.DictEncoded`** &mdash; *Type*.



```julia
Arrow.DictEncoded
```

A dictionary encoded array type (similar to a `PooledArray`). Behaves just like a normal array in most respects; internally, possible values are stored in the `encoding::DictEncoding` field, while the `indices::Vector{<:Integer}` field holds the "codes" of each element for indexing into the encoding pool. Any column/array can be dict encoding when serializing to the arrow format either by passing the `dictencode=true` keyword argument to [`Arrow.write`](reference.md#Arrow.write) (which causes *all* columns to be dict encoded), or wrapping individual columns/ arrays in [`Arrow.DictEncode(x)`](reference.md#Arrow.DictEncode).

<a id='Arrow.DictEncoding' href='#Arrow.DictEncoding'>#</a>
**`Arrow.DictEncoding`** &mdash; *Type*.



```julia
Arrow.DictEncoding
```

Represents the "pool" of possible values for a [`DictEncoded`](reference.md#Arrow.DictEncoded) array type. Whether the order of values is significant can be checked by looking at the `isOrdered` boolean field.

<a id='Arrow.FixedSizeList' href='#Arrow.FixedSizeList'>#</a>
**`Arrow.FixedSizeList`** &mdash; *Type*.



```julia
Arrow.FixedSizeList
```

An `ArrowVector` where each element is a "fixed size" list of some kind, like a `NTuple{N, T}`.

<a id='Arrow.List' href='#Arrow.List'>#</a>
**`Arrow.List`** &mdash; *Type*.



```julia
Arrow.List
```

An `ArrowVector` where each element is a variable sized list of some kind, like an `AbstractVector` or `AbstractString`.

<a id='Arrow.Map' href='#Arrow.Map'>#</a>
**`Arrow.Map`** &mdash; *Type*.



```julia
Arrow.Map
```

An `ArrowVector` where each element is a "map" of some kind, like a `Dict`.

<a id='Arrow.Primitive' href='#Arrow.Primitive'>#</a>
**`Arrow.Primitive`** &mdash; *Type*.



```julia
Arrow.Primitive
```

An `ArrowVector` where each element is a "fixed size" scalar of some kind, like an integer, float, decimal, or time type.

<a id='Arrow.SparseUnion' href='#Arrow.SparseUnion'>#</a>
**`Arrow.SparseUnion`** &mdash; *Type*.



```julia
Arrow.SparseUnion
```

An `ArrowVector` where the type of each element is one of a fixed set of types, meaning its eltype is like a julia `Union{type1, type2, ...}`. An `Arrow.SparseUnion`, in comparison to `Arrow.DenseUnion`, stores elements in a set of arrays, one array per possible type, and each typed array has the same length as the full array. This ends up with "wasted" space, since only one slot among the typed arrays is valid per full array element, but can allow for certain optimizations when each typed array has the same length.

<a id='Arrow.Stream' href='#Arrow.Stream'>#</a>
**`Arrow.Stream`** &mdash; *Type*.



```julia
Arrow.Stream(io::IO; convert::Bool=true)
Arrow.Stream(file::String; convert::Bool=true)
Arrow.Stream(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)
```

Start reading an arrow formatted table, from:

  * `io`, bytes will be read all at once via `read(io)`
  * `file`, bytes will be read via `Mmap.mmap(file)`
  * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`

Reads the initial schema message from the arrow stream/file, then returns an `Arrow.Stream` object which will iterate over record batch messages, producing an [`Arrow.Table`](reference.md#Arrow.Table) on each iteration.

By iterating [`Arrow.Table`](reference.md#Arrow.Table), `Arrow.Stream` satisfies the `Tables.partitions` interface, and as such can be passed to Tables.jl-compatible sink functions.

This allows iterating over extremely large "arrow tables" in chunks represented as record batches.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be lazily converted to more friendly Julia defaults; by default, `convert=true`.

<a id='Arrow.Struct' href='#Arrow.Struct'>#</a>
**`Arrow.Struct`** &mdash; *Type*.



```julia
Arrow.Struct
```

An `ArrowVector` where each element is a "struct" of some kind with ordered, named fields, like a `NamedTuple{names, types}` or regular julia `struct`.

<a id='Arrow.Table' href='#Arrow.Table'>#</a>
**`Arrow.Table`** &mdash; *Type*.



```julia
Arrow.Table(io::IO; convert::Bool=true)
Arrow.Table(file::String; convert::Bool=true)
Arrow.Table(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)
```

Read an arrow formatted table, from:

  * `io`, bytes will be read all at once via `read(io)`
  * `file`, bytes will be read via `Mmap.mmap(file)`
  * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`

Returns a `Arrow.Table` object that allows column access via `table.col1`, `table[:col1]`, or `table[1]`.

NOTE: the columns in an `Arrow.Table` are views into the original arrow memory, and hence are not easily modifiable (with e.g. `push!`, `append!`, etc.). To mutate arrow columns, call `copy(x)` to materialize the arrow data as a normal Julia array.

`Arrow.Table` also satisfies the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface, and so can easily be materialied via any supporting sink function: e.g. `DataFrame(Arrow.Table(file))`, `SQLite.load!(db, "table", Arrow.Table(file))`, etc.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be lazily converted to more friendly Julia defaults; by default, `convert=true`.

<a id='Arrow.ValidityBitmap' href='#Arrow.ValidityBitmap'>#</a>
**`Arrow.ValidityBitmap`** &mdash; *Type*.



```julia
Arrow.ValidityBitmap
```

A bit-packed array type where each bit corresponds to an element in an [`ArrowVector`](reference.md#Arrow.ArrowVector), indicating whether that element is "valid" (bit == 1), or not (bit == 0). Used to indicate element missingness (whether it's null).

If the null count of an array is zero, the `ValidityBitmap` will be "emtpy" and all elements are treated as "valid"/non-null.

<a id='Arrow.arrowtype' href='#Arrow.arrowtype'>#</a>
**`Arrow.arrowtype`** &mdash; *Function*.



Given a FlatBuffers.Builder and a Julia column or column eltype, Write the field.type flatbuffer definition of the eltype

<a id='Arrow.getmetadata' href='#Arrow.getmetadata'>#</a>
**`Arrow.getmetadata`** &mdash; *Function*.



```julia
Arrow.getmetadata(x) => Dict{String, String}
```

Retrieve any metadata (as a `Dict{String, String}`) attached to an object.

Metadata may be attached to any object via [`Arrow.setmetadata!`](reference.md#Arrow.setmetadata!-Tuple{Any,Dict{String,String}}), or deserialized via the arrow format directly (the format allows attaching metadata to table, column, and other objects).

<a id='Arrow.juliaeltype' href='#Arrow.juliaeltype'>#</a>
**`Arrow.juliaeltype`** &mdash; *Function*.



Given a flatbuffers metadata type definition (a Field instance from Schema.fbs), translate to the appropriate Julia storage eltype

<a id='Arrow.setmetadata!-Tuple{Any,Dict{String,String}}' href='#Arrow.setmetadata!-Tuple{Any,Dict{String,String}}'>#</a>
**`Arrow.setmetadata!`** &mdash; *Method*.



```julia
Arrow.setmetadata!(x, metadata::Dict{String, String})
```

Set the metadata for any object, provided as a `Dict{String, String}`. Metadata attached to a table or column will be serialized when written as a stream or file.

<a id='Arrow.write' href='#Arrow.write'>#</a>
**`Arrow.write`** &mdash; *Function*.



```julia
Arrow.write(io::IO, tbl)
Arrow.write(file::String, tbl)
tbl |> Arrow.write(io_or_file)
```

Write any [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible `tbl` out as arrow formatted data. Providing an `io::IO` argument will cause the data to be written to it in the ["streaming" format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format), unless `file=true` keyword argument is passed. Providing a `file::String` argument will result in the ["file" format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) being written.

Multiple record batches will be written based on the number of `Tables.partitions(tbl)` that are provided; by default, this is just one for a given table, but some table sources support automatic partitioning. Note you can turn multiple table objects into partitions by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that each table must have the exact same `Tables.Schema`.

By default, `Arrow.write` will use multiple threads to write multiple record batches simultaneously (e.g. if julia is started with `julia -t 8` or the `JULIA_NUM_THREADS` environment variable is set).

Supported keyword arguments to `Arrow.write` include:

  * `compress`: possible values include `:lz4`, `:zstd`, or your own initialized `LZ4FrameCompressor` or `ZstdCompressor` objects; will cause all buffers in each record batch to use the respective compression encoding
  * `alignment::Int=8`: specify the number of bytes to align buffers to when written in messages; strongly recommended to only use alignment values of 8 or 64 for modern memory cache line optimization
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written; to dict encode specific columns, wrap the column/array in `Arrow.DictEncode(col)`
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; other language implementations [may not support this](https://arrow.apache.org/docs/status.html)
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `file::Bool=false`: if a an `io` argument is being written to, passing `file=true` will cause the arrow file format to be written instead of just IPC streaming

