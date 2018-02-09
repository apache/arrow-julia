using BenchmarkTools
using Arrow
using Missings
using WeakRefStrings
using CategoricalArrays


data = reinterpret(UInt8, [2,3,5,7])
data = vcat(0x0f, data)

# v = NullablePrimitive([2,missing,5,7])

v = NullablePrimitive([2,missing,5,7])

l = NullableList(["a", "ab", missing, "abc"])

