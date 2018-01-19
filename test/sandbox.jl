using Arrow
using Missings


# mask = bitpack([true, true, true, false, true])
# vals = reinterpret(UInt8, [2, 3, 5, 7, 11])


data = convert(Vector{UInt8}, "thisissomenewtext")
mask = bitpack([true, false, true, true, true])
offsets = reinterpret(UInt8, Int32[0, 4, 6, 10, 13, 17])


b = Buffer(vcat(data, mask, offsets))

A = Primitive{UInt8}(b, 1, length(data))
l = NullableList{Primitive{UInt8},String}(b, length(data)+1, length(data)+length(mask)+1,
                                          5, 1, A)

