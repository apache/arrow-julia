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

[![docs](https://img.shields.io/badge/docs-latest-blue&logo=julia)](https://arrow.apache.org/julia/)
[![CI](https://github.com/apache/arrow-julia/workflows/CI/badge.svg)](https://github.com/apache/arrow-julia/actions?query=workflow%3ACI)
[![codecov](https://app.codecov.io/gh/apache/arrow-julia/branch/main/graph/badge.svg)](https://app.codecov.io/gh/apache/arrow-julia)

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

Arrow.jl currently requires Julia `1.12+`.

## Local Development

When developing on Arrow.jl it is recommended that you run the following to ensure that any
changes to ArrowTypes.jl are immediately available to Arrow.jl without requiring a release:

```sh
julia --project -e 'using Pkg; Pkg.develop(path="src/ArrowTypes")'
```

Current write-path notes:
  * `Arrow.tobuffer` includes a direct single-partition fast path for eligible inputs
  * `Arrow.tobuffer(Tables.partitioner(...))` also includes a targeted direct multi-record-batch path for single-column top-level strings and single-column non-missing binary/code-units columns
  * `Arrow.write(io, Tables.partitioner(...))` now reuses that same targeted direct multi-record-batch path instead of always going through the legacy `Writer` orchestration
  * multi-column partitions, dictionary-encoded top-level columns, map-heavy inputs, and missing-binary partitions retain the existing writer path

## Format Support

This implementation supports the 1.0 version of the specification, including support for:
  * All primitive data types
  * All nested data types
  * Dictionary encodings and messages
  * Dictionary-encoded `CategoricalArray` interop, including missing-value roundtrips through `Arrow.Table`, `copy`, and `DataFrame(...; copycols=true)`
  * Extension types
  * Base Julia `Enum` logical types via the `JuliaLang.Enum` extension label, with native Julia roundtrips back to the original enum type while `convert=false` and non-Julia consumers still see the primitive storage type
  * View-backed Utf8/Binary columns, including recovery from under-reported variadic buffer counts by inferring the required external buffers from valid view elements
  * Streaming, file, record batch, and replacement and isdelta dictionary messages

It currently doesn't include support for:
  * Tensors or sparse tensors
  * C data interface

Flight RPC status:
  * Experimental `Arrow.Flight` support is available in-tree
  * Requires Julia `1.12+`
  * Includes generated protocol bindings and complete client constructors for the `FlightService` RPC surface
  * Keeps the top-level Flight module shell thin, with exports and generated-protocol setup split out of `src/flight/Flight.jl`
  * Includes high-level `FlightData <-> Arrow IPC` helpers for `Arrow.Table`, `Arrow.Stream`, and DoPut payload generation
  * Keeps the Flight IPC conversion layer modular under `src/flight/convert/`, with `src/flight/convert.jl` retained as a thin entrypoint
  * Includes client helpers for request headers, binary metadata, handshake token reuse, and TLS configuration via `withheaders`, `withtoken`, and `authenticate`
  * Keeps the Flight client implementation modular under `src/flight/client/`, with thin entrypoints at `src/flight/client.jl` and `src/flight/client/rpc_methods.jl`
  * Includes a transport-agnostic server core (`Service`, `ServerCallContext`, `ServiceDescriptor`, `MethodDescriptor`) for local Flight method dispatch, path lookup, and handler testing
  * Keeps the transport-agnostic server core modular under `src/flight/server/`, with `src/flight/server.jl` retained as a thin entrypoint
  * Includes an optional `gRPCServer.jl` package extension that maps `Arrow.Flight.Service` into `gRPCServer.ServiceDescriptor` and registers Flight proto types with the external server package when it is present
  * Keeps the optional `gRPCServer.jl` bridge modular under `ext/arrowgrpcserverext/`, with `ext/ArrowgRPCServerExt.jl` retained as a thin entrypoint
  * Includes optional live interoperability coverage for `Handshake`, authenticated token propagation, `PollFlightInfo`, and TLS via dedicated Python reference servers
  * Includes optional live `pyarrow.flight` interoperability coverage for `ListFlights`, `GetFlightInfo`, `GetSchema`, `DoGet`, `DoPut`, `DoExchange`, `ListActions`, and `DoAction`
  * Keeps targeted Flight verification modular under `test/flight/`, with `test/flight.jl` retained as a thin entrypoint for local and CI invocation stability, the client-constructor/protocol-wrapper checks decomposed under `test/flight/client_surface/`, the optional `gRPCServer` extension scenarios decomposed under `test/flight/grpcserver_extension/`, the `pyarrow.flight` interop scenarios decomposed under `test/flight/pyarrow_interop/`, and the transport-agnostic server-core checks decomposed under `test/flight/server_core/`
  * Includes `test/flight_grpcserver.jl` as a temporary-environment runner for optional native `gRPCServer` coverage without mutating `test/Project.toml`
  * Dedicated CI jobs now exercise the Flight interop suite on stable and nightly Linux; native Julia server transport remains optional/experimental and is not part of the default Flight suite

Third-party data formats:
  * CSV, parquet and avro support via the existing [CSV.jl](https://github.com/JuliaData/CSV.jl), [Parquet.jl](https://github.com/JuliaIO/Parquet.jl) and [Avro.jl](https://github.com/JuliaData/Avro.jl) packages
  * Other Tables.jl-compatible packages automatically supported ([DataFrames.jl](https://github.com/JuliaData/DataFrames.jl), [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl), [JuliaDB.jl](https://github.com/JuliaData/JuliaDB.jl), [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl), [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl), [JDBC.jl](https://github.com/JuliaDatabases/JDBC.jl), [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl), [XLSX.jl](https://github.com/felipenoris/XLSX.jl), etc.)
  * No current Julia packages support ORC

See the [full documentation](https://arrow.apache.org/julia/) for details on reading and writing arrow data.
