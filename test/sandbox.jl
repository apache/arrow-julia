using Arrow
using Missings


mask = bitpack([true, true, true, false, true])
vals = reinterpret(UInt8, [2, 3, 5, 7, 11])


b = Buffer(vcat(mask, vals))

A = NullablePrimitive{Int64}(b, 1, length(mask)+1, 5, 1)

