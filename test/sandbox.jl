using Arrow


v = zeros(UInt8, 64)
v[1] = UInt8(1*1 + 0*2 + 1*4 + 1*8)

m = reinterpret(UInt8, [2, 3, 5, 7])
m = vcat(v, m)

b = Buffer(m)

# A = PrimitiveArray{Int64}(b, 1, 4)
A = NullablePrimitiveArray{Int64}(b, 1, 65, 4, 1)

