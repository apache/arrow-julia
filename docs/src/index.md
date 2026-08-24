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

# Arrow.jl

A pure Julia implementation of the [Apache Arrow](https://arrow.apache.org)
columnar format: the IPC stream and file formats (read and write, with
memory-mapped and byte-range reads, scan pushdown, and compression), the C
data and C stream interfaces for in-process exchange with other
implementations, recursive ArrowTypes.jl mappings for custom values, and
Tables.jl integration throughout.

```@example quick_start
using Arrow
path = joinpath(mktempdir(), "data.arrow")
Arrow.write(path, (a = [1, 2, 3], b = ["x", "y", missing]))
tbl = Arrow.Table(path)
collect(tbl.b)
```

```@contents
Pages = ["manual.md", "migration.md", "reference.md"]
Depth = 3
```

```@docs
Arrow
```
