
<a id='Arrow.jl'></a>

<a id='Arrow.jl-1'></a>

# Arrow.jl

- [User Manual](manual.md#User-Manual)
    - [Reading arrow data](manual.md#Reading-arrow-data)
        - [`Arrow.Table`](manual.md#Arrow.Table)
        - [Arrow types](manual.md#Arrow-types)
        - [`Arrow.Stream`](manual.md#Arrow.Stream)
        - [Table and column metadata](manual.md#Table-and-column-metadata)
    - [Writing arrow data](manual.md#Writing-arrow-data)
        - [`Arrow.write`](manual.md#Arrow.write)
        - [Multithreaded writing](manual.md#Multithreaded-writing)
        - [Compression](manual.md#Compression)
- [API Reference](reference.md#API-Reference)

<a id='Arrow' href='#Arrow'>#</a>
**`Arrow`** &mdash; *Module*.



```julia
Arrow.jl
```

A pure Julia implementation of the [apache arrow](https://arrow.apache.org/) memory format specification.

This implementation supports the 1.0 version of the specification, including support for:

  * All primitive data types
  * All nested data types
  * Dictionary encodings and messages
  * Extension types
  * Streaming, file, record batch, and replacement and isdelta dictionary messages

It currently doesn't include support for:

  * Tensors or sparse tensors
  * Flight RPC
  * C data interface

Third-party data formats:

  * csv and parquet support via the existing CSV.jl and Parquet.jl packages
  * Other Tables.jl-compatible packages automatically supported (DataFrames.jl, JSONTables.jl, JuliaDB.jl, SQLite.jl, MySQL.jl, JDBC.jl, ODBC.jl, XLSX.jl, etc.)
  * No current Julia packages support ORC or Avro data formats

See docs for official Arrow.jl API with `Arrow.Table`, `Arrow.write`, and `Arrow.Stream`.

