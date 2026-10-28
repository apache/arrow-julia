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

Arrow.jl reads and writes the [Apache Arrow](https://arrow.apache.org) IPC file and
stream formats in pure Julia. A file reads as Tables.jl columns that view its memory, and
any Tables.jl table writes. It also implements the Arrow C data and C stream interfaces.

> [!IMPORTANT]
> This is the Arrow.jl 3.0 development branch; 3.0 is not registered yet. Arrow 3.0 is a
> breaking release: read the [migration guide](docs/src/migration.md) before you upgrade
> from 2.x. A checkout uses the in-repository `src/ArrowTypes` package: Julia 1.11+
> resolves it through `[sources]`; on Julia 1.10 run the `Pkg.develop` commands below.

## Installation

```julia
import Pkg
Pkg.add("Arrow")
```

## Quick start

```julia
using Arrow, DataFrames

Arrow.write("data.arrow", (id=[1, 2, 3], name=["Ada", "Ben", missing]))

table = Arrow.Table("data.arrow")        # columns view the file
table.name                               # ["Ada", "Ben", missing]
df = Arrow.read("data.arrow", DataFrame) # a DataFrame with its own columns

subset = Arrow.Table("data.arrow"; select=[:id], filter=Arrow.col(:id) > 1)

for batch in Arrow.Stream("data.arrow")  # one record batch at a time
    # ...
end
```

The [documentation](https://arrow.apache.org/julia/) covers reading, writing, streams,
the type mapping, limits and errors, the C data interface, and migrating from 2.x. The
[changelog](CHANGELOG.md) summarizes what changed in 3.0.

## Development

In a checkout of this branch, prepare the local subpackages, then run the
tests:

```julia
import Pkg
Pkg.activate(".")
Pkg.develop(path="src/ArrowTypes")
Pkg.test()
```

The repository also has Apache Arrow gold-corpus checks, PyArrow and
Nanoarrow IPC oracle checks, and PyArrow C interface checks. Run all of them
with `julia conformance/run.jl`. Docker and network access for the first image
build are required.

Run `julia --project=. test/fuzz.jl --cases 16 --mutations 64` for the
deterministic PR-sized fuzz suite. The scheduled workflow runs the extended
512-case and 20,000-mutation suite with a new reproducible master seed for each
scheduled run. It repeats the first full route sweep and every 256th mutation
to detect unstable outcomes. If the runner records or times out on a case, the
workflow uploads its replay coordinates, mutated bytes when available, the
resolved package environment, and a location-independent `replay.sh` wrapper.

The Arrow 3.0 rewrite used Anthropic Claude Code and OpenAI Codex for code
generation, test generation, and review. Apache Arrow maintainers remain
responsible for understanding, reviewing, testing, and approving the code and
each release.

See [the engine design](docs/dev/core-README.md) for the source layout and
internal contracts.
