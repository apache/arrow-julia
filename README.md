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

# Arrow.jl

[![Documentation](https://img.shields.io/badge/docs-latest-blue?logo=julia)](https://arrow.apache.org/julia/)
[![CI](https://github.com/apache/arrow-julia/actions/workflows/ci.yml/badge.svg)](https://github.com/apache/arrow-julia/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/apache/arrow-julia/branch/main/graph/badge.svg)](https://codecov.io/gh/apache/arrow-julia)

Arrow.jl is a pure Julia implementation of the
[Apache Arrow](https://arrow.apache.org) columnar data standard. It reads and
writes Arrow IPC files and streams. It also supports the Arrow C data and C
stream interfaces, Tables.jl, compressed buffers, and selective byte-range
reads.

> [!IMPORTANT]
> This is the Arrow.jl 3.0 development branch. Arrow 3.0 is not registered
> yet. It requires a Tables.jl release that contains `Tables.Scan` and the
> first registered ArrowStrings.jl release. Until then, a checkout must use
> the local `src/ArrowStrings` and `src/ArrowTypes` packages and the pinned
> Tables.jl development commit shown below.

## Installation

Install the latest registered release from the Julia REPL:

```julia
import Pkg
Pkg.add("Arrow")
```

## Quick start

```julia
using Arrow, Tables

data = (id = [1, 2, 3], name = ["Ada", "Babbage", missing])
Arrow.write("data.arrow", data)

table = Arrow.Table("data.arrow")
Tables.columnnames(table) # [:id, :name]
collect(table.name) == ["Ada", "Babbage", missing] # true
```

`Arrow.Table` accepts a path, an `IO`, IPC bytes, or an
`Arrow.AbstractArrowSource`. `Arrow.Stream` reads one record batch at a time.
`Arrow.write` accepts any Tables.jl source.

Arrow 3.0 includes:

- IPC file and stream reads and writes.
- LZ4 frame and Zstandard buffer compression.
- Dictionary encoding.
- `Tables.Scan` projection, filter, limit, and offset pushdown.
- Sparse byte-range reads, including a CloudStore.jl extension.
- Arrow C data and C stream import and export.
- Recursive ArrowTypes.jl mappings for custom and extension types.
- Structural, semantic, and optional full-content validation.

Arrow 3.0 is a breaking rewrite. Read the
[migration guide](docs/src/migration.md) before you update from Arrow 2.x.
See the [changelog](CHANGELOG.md) for the full release summary. The
[user manual](https://arrow.apache.org/julia/) and
[API reference](docs/src/reference.md) describe the supported public API.

## Development

In a checkout of this branch, prepare the local subpackages and the temporary
Tables.jl dependency, then run the tests:

```julia
import Pkg
Pkg.activate(".")
Pkg.develop(path="src/ArrowStrings")
Pkg.develop(path="src/ArrowTypes")
Pkg.add(url="https://github.com/JuliaData/Tables.jl",
        rev="64268c6a316e380cc3da26965f440a5433ebc1f7")
Pkg.test()
```

The repository also has Apache Arrow gold-corpus checks, PyArrow and
Nanoarrow IPC oracle checks, and PyArrow C interface checks. Run all of them
with `julia conformance/run.jl`. Docker and network access for the first image
build are required.

The Arrow 3.0 rewrite used Anthropic Claude Code and OpenAI Codex for code
generation, test generation, and review. Apache Arrow maintainers remain
responsible for understanding, reviewing, testing, and approving the code and
each release.

See [the engine design](docs/dev/core-README.md) for the source layout and
internal contracts.
