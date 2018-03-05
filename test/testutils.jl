

const PRIMITIVE_ELTYPES = [Float32, Float64, Int32, Int64, UInt16]
const MAX_RAND_PAD_LENGTH = 16


randpad() = rand(UInt8, rand(1:MAX_RAND_PAD_LENGTH))


function rand_primitive_buffer(;lpad=randpad(), rpad=randpad())
    dtype = rand(PRIMITIVE_ELTYPES)
    len = rand(1:MAX_VECTOR_LENGTH)
    v = rand(dtype, len)
    b = convert(Vector{UInt8}, reinterpret(UInt8, v))
    b = vcat(lpad, b, rpad)
    len, dtype, length(lpad), b, v
end

function rand_nullableprimitive_buffer(;lpad=randpad(), rpad=randpad())
    dtype = rand(PRIMITIVE_ELTYPES)
    len = rand(1:MAX_VECTOR_LENGTH)
    pres = rand(Bool, len)
    mask = bitpack(pres)
    vraw = rand(dtype, len)
    b = vcat(lpad, mask, convert(Vector{UInt8}, reinterpret(UInt8, vraw)), rpad)
    v = Union{dtype,Missing}[pres[i] ? vraw[i] : missing for i ∈ 1:len]
    len, dtype, length(mask), length(lpad), b, v
end

