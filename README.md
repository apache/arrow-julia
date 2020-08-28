# Arrow

[![Build Status](https://travis-ci.org/JuliaData/Arrow.jl.svg?branch=master)](https://travis-ci.org/JuliaData/Arrow.jl)
[![codecov.io](http://codecov.io/github/JuliaData/Arrow.jl/coverage.svg?branch=master)](http://codecov.io/github/JuliaData/Arrow.jl?branch=master)

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.

Please see this [document](https://arrow.apache.org/docs/memory_layout.html) for a description of the Arrow memory layout.
