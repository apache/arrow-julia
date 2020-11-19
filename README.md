# Arrow

[![docs](https://img.shields.io/badge/docs-latest-blue&logo=julia)](https://arrow.juliadata.org/dev/)
[![Build Status](https://travis-ci.com/JuliaData/Arrow.jl.svg?branch=master)](https://travis-ci.com/JuliaData/Arrow.jl.svg?branch=master)
[![codecov](https://codecov.io/gh/JuliaData/Arrow.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/Arrow.jl)

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.

Please see this [document](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout) for a description of the Arrow memory layout.

## Format Support

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

See the [full documentation](https://arrow.juliadata.org/dev/) for details on reading and writing arrow data.
