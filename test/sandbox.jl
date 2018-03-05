using BenchmarkTools
using Arrow
using CategoricalArrays


data = convert(Vector{UInt8}, reinterpret(UInt8, [2,3,5,7]))
data = vcat(0x0f, data)

# v = NullablePrimitive([2,missing,5,7])

v = NullablePrimitive{Int64}(data, 1, 2, 4)

