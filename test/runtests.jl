using Arrow
using Compat.Test, Compat.Random

if VERSION < v"0.7.0-"
    using Missings
end

const SEED = 999

# number of tests
const N_OUTER = 4
const N_IDX_CHECK = 32
const MAX_IDX_LEN = 32
const MAX_VECTOR_LENGTH = 256

srand(SEED)

include("testutils.jl")


@testset "PrimitiveAccess" begin
    for i ∈ 1:N_OUTER
        len, dtype, lpad, b, v = rand_primitive_buffer()
        p = Primitive{dtype}(b, lpad+1, len)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] == v[k]
        end
        # AbstractVector{<:Integer} indices
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] == v[idx]
        end
    end
end


@testset "PrimitiveConstruct" begin
    for i ∈ 1:N_OUTER
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(1:MAX_VECTOR_LENGTH)
        v = rand(dtype, len)
        p = Primitive(v)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] == v[k]
        end
        # AbstractVector{<:Integer} indices
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] == v[idx]
        end
    end
end


@testset "NullablePrimitiveAccess" begin
    for i ∈ 1:N_OUTER
        len, dtype, bmask, lpad, b, v = rand_nullableprimitive_buffer()
        p = NullablePrimitive{dtype}(b, 1+lpad, 1+bmask+lpad, len)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test (ismissing(p[k]) && ismissing(v[k])) || (p[k] == v[k])
        end
        # AbstractVector{<:Integer} indices
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            sp = p[idx]
            sv = v[idx]
            ssp = collect(skipmissing(sp))
            ssv = collect(skipmissing(sv))
            @test length(sp) == length(sv) && ssp == ssv
        end
    end
end


@testset "NullablePrimitiveConstruct" begin
    for i ∈ 1:N_OUTER
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(1:MAX_VECTOR_LENGTH)
        v = Union{dtype,Missing}[rand(Bool) ? missing : rand(dtype) for i ∈ 1:len]
        p = NullablePrimitive(v)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test (ismissing(p[k]) && ismissing(v[k])) || (p[k] == v[k])
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            sp = p[idx]
            sv = v[idx]
            ssp = collect(skipmissing(sp))
            ssv = collect(skipmissing(sv))
            @test length(sp) == length(sv) && ssp == ssv
        end
    end
end


# @testset "ListAccess" begin
#     len  = 5
#     offs = Int32[0,4,7,8,12,14]
#     vals = convert(Vector{UInt8}, codeunits("firewalkwithme"))
#     lpad = randpad()
#     rpad = randpad()
#     b = vcat(lpad, convert(Vector{UInt8}, reinterpret(UInt8, offs)), vals, rpad)
#     l = List{String}(b, 1+length(lpad), 1+length(lpad)+sizeof(Int32)*length(offs),
#                      len, UInt8, length(vals))
#     @test offsets(l)[:] == offs
#     @test values(l)[:] == vals
#     @test l[1] == "fire"
#     @test l[2] == "wal"
#     @test l[3] == "k"
#     @test l[4] == "with"
#     @test l[5] == "me"
#     @test l[[1,3,5]] == ["fire", "k", "me"]
#     @test l[[false,true,false,true,false]] = ["wal", "with"]
# end
