
<a id='API-Reference'></a>

<a id='API-Reference-1'></a>

# API Reference

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

Reads the initial schema message from the arrow stream/file, then returns an `Arrow.Stream` object which will iterate over record batch messages, producing an `Arrow.Table` on each iteration.

By iterating `Arrow.Table`, `Arrow.Stream` satisfies the `Tables.partitions` interface, and as such can be passed to Tables.jl-compatible sink functions.

This allows iterating over extremely large "arrow tables" in chunks represented as record batches.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be lazily converted to more friendly Julia defaults; by default, `convert=true`.

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

`Arrow.Table` also satisfies the Tables.jl interface, and so can easily be materialied via any supporting sink function: e.g. `DataFrame(Arrow.Table(file))`, `SQLite.load!(db, "table", Arrow.Table(file))`, etc.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be lazily converted to more friendly Julia defaults; by default, `convert=true`.

<a id='Arrow.arrowtype' href='#Arrow.arrowtype'>#</a>
**`Arrow.arrowtype`** &mdash; *Function*.



Given a FlatBuffers.Builder and a Julia column eltype, Write the field.type flatbuffer definition

<a id='Arrow.bitpackedbytes-Tuple{Integer}' href='#Arrow.bitpackedbytes-Tuple{Integer}'>#</a>
**`Arrow.bitpackedbytes`** &mdash; *Method*.



```julia
bitpackedbytes(n[, pad=true])
```

Determines the number of bytes used by `n` bits, optionally with padding.

<a id='Arrow.getbit-Tuple{UInt8,Integer}' href='#Arrow.getbit-Tuple{UInt8,Integer}'>#</a>
**`Arrow.getbit`** &mdash; *Method*.



```julia
getbit
```

This deliberately elides bounds checking.

<a id='Arrow.juliaeltype' href='#Arrow.juliaeltype'>#</a>
**`Arrow.juliaeltype`** &mdash; *Function*.



Given a flatbuffers metadata type definition (a Field instance from Schema.fbs), translate to the appropriate Julia storage eltype

<a id='Arrow.padding-Tuple{Integer}' href='#Arrow.padding-Tuple{Integer}'>#</a>
**`Arrow.padding`** &mdash; *Method*.



```julia
padding(n::Integer)
```

Determines the total number of bytes needed to store `n` bytes with padding. Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.

<a id='Arrow.setbit-Tuple{UInt8,Bool,Integer}' href='#Arrow.setbit-Tuple{UInt8,Bool,Integer}'>#</a>
**`Arrow.setbit`** &mdash; *Method*.



```julia
setbit
```

This also deliberately elides bounds checking.

<a id='Arrow.write' href='#Arrow.write'>#</a>
**`Arrow.write`** &mdash; *Function*.



```julia
Arrow.write(io::IO, tbl)
Arrow.write(file::String, tbl)
```

Write any Tables.jl-compatible `tbl` out as arrow formatted data. Providing an `io::IO` argument will cause the data to be written to it in the "streaming" format, unless `file=true` keyword argument is passed. Providing a `file::String` argument will result in the "file" format being written.

Multiple record batches will be written based on the number of `Tables.partitions(tbl)` that are provided; by default, this is just one for a given table, but some table sources support automatic partitioning. Note you can turn multiple table objects into partitions by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that each table must have the exact same `Tables.Schema`.

By default, `Arrow.write` will use multiple threads to write multiple record batches simultaneously (e.g. if julia is started with `julia -t 8`).

Supported keyword arguments to `Arrow.write` include:

  * `compress::Symbol`: possible values include `:lz4` or `:zstd`; will cause all buffers in each record batch to use the respective compression encoding
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; many other implementations don't support this
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `file::Bool=false`: if a an `io` argument is being written to, passing `file=true` will cause the arrow file format to be written instead of just IPC streaming

