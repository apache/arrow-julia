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

# Arrow

[![docs](https://img.shields.io/badge/docs-latest-blue&logo=julia)](https://arrow.juliadata.org/dev/)
[![CI](https://github.com/JuliaData/Arrow.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/Arrow.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/JuliaData/Arrow.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/Arrow.jl)

[![deps](https://juliahub.com/docs/Arrow/deps.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w?t=2)
[![version](https://juliahub.com/docs/Arrow/version.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w)
[![pkgeval](https://juliahub.com/docs/Arrow/pkgeval.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w)

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.

Please see this [document](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout) for a description of the Arrow memory layout.

## Installation

The package can be installed by typing in the following in a Julia REPL:

```julia
julia> using Pkg; Pkg.add("Arrow")
```

or to use the official-apache code that follows the official apache release process, you can do:

```julia
julia> using Pkg; Pkg.add(url="https://github.com/apache/arrow", subdir="julia/Arrow.jl")
```

## Local Development

When developing on Arrow.jl it is recommended that you run the following to ensure that any
changes to ArrowTypes.jl are immediately available to Arrow.jl without requiring a release:

```sh
julia --project -e 'using Pkg; Pkg.develop(path="src/ArrowTypes")'
```

## Difference between this code and the apache/arrow/julia/Arrow repository

The code in the apache/arrow repository is officially part of the apache/arrow project and as such follows the regulated release cadence of the entire project, following standard community
voting protocols. The JuliaData/Arrow.jl repository can be viewed as a sort of "dev" or "latest" branch of this code that may release more frequently, but without following
official apache release guidelines. The two repositories are synced, however, so any bugfix patches in JuliaData will be upstreamed to apache/arrow for each release.


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
  * CSV, parquet and avro support via the existing [CSV.jl](https://github.com/JuliaData/CSV.jl), [Parquet.jl](https://github.com/JuliaIO/Parquet.jl) and [Avro.jl](https://github.com/JuliaData/Avro.jl) packages
  * Other Tables.jl-compatible packages automatically supported ([DataFrames.jl](https://github.com/JuliaData/DataFrames.jl), [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl), [JuliaDB.jl](https://github.com/JuliaData/JuliaDB.jl), [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl), [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl), [JDBC.jl](https://github.com/JuliaDatabases/JDBC.jl), [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl), [XLSX.jl](https://github.com/felipenoris/XLSX.jl), etc.)
  * No current Julia packages support ORC

See the [full documentation](https://arrow.juliadata.org/dev/) for details on reading and writing arrow data.
