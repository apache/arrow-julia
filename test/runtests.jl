using Arrow
using Compat.Test, Compat.Random

if VERSION < v"0.7.0-"
    using Missings
end

const SEED = 999
const PRIMITIVE_ELTYPES = [Float32, Float64, Int32, Int64, UInt16]

srand(SEED)


@testset "Primitive" begin
    for i ∈ 1:3
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(32:256)
        v = rand(dtype, len)
        b = convert(Vector{UInt8}, reinterpret(UInt8, v))
        A = Primitive{dtype}(b, 1, len)
        for j ∈ 1:8
            k = rand(1:len)
            @test A[k] == v[k]
        end
    end
end


# TODO finish setting up this test!
@testset "NullablePrimitives" begin
    for i ∈ 1:3
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(32:256)
        pres = rand(Bool, len)
        mask = bitpack(pres)
        vraw = rand(dtype, len)
        data = vcat(mask, reinterpret(UInt8, vraw))
        b = convert(Vector{UInt8}, data)
        A = NullablePrimitive{dtype}(b, 1, length(mask)+1, len)
        for j ∈ 1:8
            k = rand(1:len)
            # strict equality should work even for floats in this case
            @test pres[k] ? (A[k] == vraw[k]) : ismissing(A[k])
        end
    end
end

