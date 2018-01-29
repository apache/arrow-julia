using Arrow
using Missings
using WeakRefStrings


# data = convert(Vector{UInt8}, "thisissomenewtext")
# mask = bitpack([true, false, true, true, true])
# offsets = reinterpret(UInt8, Int32[0, 4, 6, 10, 13, 17])
# 
# 
# b = Buffer(vcat(data, mask, offsets))
# 
# A = Primitive{UInt8}(b, 1, length(data))
# l = NullableList{Primitive{UInt8},WeakRefString{UInt8}}(b, length(data)+1, length(data)+length(mask)+1,
#                                           5, 1, A)

b = Buffer(zeros(UInt8, 128))


A = ["a", "ab", missing, "abc"]
# l = List(b, 1, offsetsbytes(A)+1, UInt8, A)

l = NullableList(b, 1, minbitmaskbytes(A)+1, minbitmaskbytes(A)+offsetsbytes(A)+1,
                 UInt8, A)



