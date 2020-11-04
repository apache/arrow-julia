
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
  * Dictionary encodings, nested dictionary encodings, and messages
  * Extension types
  * Streaming, file, record batch, and replacement and isdelta dictionary messages
  * Buffer compression/decompression via the standard LZ4 frame and Zstd formats

It currently doesn't include support for:

  * Tensors or sparse tensors
  * Flight RPC
  * C data interface

Third-party data formats:

  * csv and parquet support via the existing [CSV.jl](https://github.com/JuliaData/CSV.jl) and [Parquet.jl](https://github.com/JuliaIO/Parquet.jl) packages
  * Other [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible packages automatically supported ([DataFrames.jl](https://github.com/JuliaData/DataFrames.jl), [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl), [JuliaDB.jl](https://github.com/JuliaData/JuliaDB.jl), [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl), [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl), [JDBC.jl](https://github.com/JuliaDatabases/JDBC.jl), [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl), [XLSX.jl](https://github.com/felipenoris/XLSX.jl), etc.)
  * No current Julia packages support ORC or Avro data formats

See docs for official Arrow.jl API with the [User Manual](manual.md#User-Manual) and reference docs for [`Arrow.Table`](reference.md#Arrow.Table), [`Arrow.write`](reference.md#Arrow.write), and [`Arrow.Stream`](reference.md#Arrow.Stream).

