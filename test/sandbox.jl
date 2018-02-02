using Arrow
using Missings
using WeakRefStrings


data = reinterpret(UInt8, [2,3,5,7])
# data = vcat(0x0f, data)

v = Primitive([2,3,5,7])



