# Arrow

[![Build Status](https://travis-ci.org/JuliaData/Arrow.jl.svg?branch=master)](https://travis-ci.org/JuliaData/Arrow.jl)
[![codecov.io](http://codecov.io/github/JuliaData/Arrow.jl/coverage.svg?branch=master)](http://codecov.io/github/JuliaData/Arrow.jl?branch=master)

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.

Please see this [document](https://arrow.apache.org/docs/memory_layout.html) for a description of the Arrow memory layout.

### Basic usage:

#### Installation

```julia
] add Tables#master
] add https://github.com/JuliaData/Arrow.jl#master
```

#### Reading

Read from `IO`, file, or byte vector directly. Arrow data can be in file or streaming format, `Arrow.Table` will detect automatically.

```julia
using Arrow

# read arrow table from file format
tbl = Arrow.Table(file)

# read arrow table from IO
tbl = Arrow.Table(io)

# read arrow table directly from bytes, like from an HTTP request
resp = HTTP.get(url)
tbl = Arrow.Table(resp.body)
```

#### Writing

Write any Tables.jl source as arrow formatted data. Can write directly to `IO` or to a provided file name.

```julia
# write directly to any IO in streaming format
Arrow.write(io, tbl)

# write to a file in file format
Arrow.write("data.arrow", tbl)
```
