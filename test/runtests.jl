using Arrow
using Compat, Compat.Test, Compat.Random, Compat.Dates

if VERSION < v"0.7.0-"
    using Missings
end

const ≅ = isequal

const SEED = 999

const N_OUTER = 4
const N_IDX_CHECK = 32
const MAX_IDX_LEN = 32
const MAX_VECTOR_LENGTH = 256
const MAX_STRING_LENGTH = 32

const PRIMITIVE_ELTYPES = [Float32, Float64, Int32, Int64, UInt16]

srand(SEED)

include("testutils.jl")


@testset "Primitive_access" begin
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
        @test p[:] == v
    end
end


@testset "Primitive_construct" begin
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
        @test p[:] == v
    end
end


@testset "NullablePrimitive_access" begin
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
        @test p[:] ≅ v
    end
end


@testset "NullablePrimitive_construct" begin
    for i ∈ 1:N_OUTER
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(1:MAX_VECTOR_LENGTH)
        v = Union{dtype,Missing}[rand(Bool) ? missing : rand(dtype) for i ∈ 1:len]
        p = NullablePrimitive(v)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] ≅ v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] ≅ v[idx]
        end
        @test p[:] ≅ v
    end
end


@testset "List_access" begin
    len  = 5
    offs = Int32[0,4,7,8,12,14]
    vals = convert(Vector{UInt8}, codeunits("firewalkwithme"))
    valspad = zeros(UInt8, Arrow.paddinglength(length(vals)))
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, convert(Vector{UInt8}, reinterpret(UInt8, offs)), vals, valspad, rpad)
    l = List{String}(b, 1+length(lpad), 1+length(lpad)+sizeof(Int32)*length(offs),
                     len, UInt8, length(vals))
    @test offsets(l)[:] == offs
    @test values(l)[:] == vals
    @test l[1] == "fire"
    @test l[2] == "wal"
    @test l[3] == "k"
    @test l[4] == "with"
    @test l[5] == "me"
    @test l[[1,3,5]] == ["fire", "k", "me"]
    @test l[[false,true,false,true,false]] == ["wal", "with"]
    @test l[:] == ["fire", "wal", "k", "with", "me"]
end


@testset "List_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        v = String[randstring(rand(0:MAX_STRING_LENGTH)) for i ∈ 1:len]
        l = List(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test l[k] == v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test l[idx] == v[idx]
        end
        @test l[:] == v
    end
end


@testset "NullableList_access" begin
    len = 7
    offs = Int32[0,4,9,9,14,14,17,21]
    vals = convert(Vector{UInt8}, codeunits("kirkspockbonesncc1701"))
    valspad = zeros(UInt8, Arrow.paddinglength(length(vals)))
    pres = Bool[true,true,false,true,false,true,true]
    mask = Arrow.bitpackpadded(pres)
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, mask, convert(Vector{UInt8}, reinterpret(UInt8, offs)), vals, valspad, rpad)
    l = NullableList{String}(b, 1+length(lpad), 1+length(lpad)+length(mask),
                             1+length(lpad)+length(mask)+sizeof(Int32)*length(offs),
                             len, UInt8, length(vals))
    @test offsets(l)[:] == offs
    @test values(l)[:] == vals
    @test l[1] == "kirk"
    @test l[2] == "spock"
    @test ismissing(l[3])
    @test l[4] == "bones"
    @test ismissing(l[5])
    @test l[6] == "ncc"
    @test l[7] == "1701"
    @test l[[2,5,4]] ≅ ["spock", missing, "bones"]
    @test l[[true,false,true,false,false,false,false]] ≅ ["kirk", missing]
    @test l[:] ≅ ["kirk", "spock", missing, "bones", missing, "ncc", "1701"]
end


@testset "NullableList_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        v = Union{String,Missing}[rand(Bool) ? missing : randstring(rand(0:MAX_STRING_LENGTH))
                                  for i ∈ 1:len]
        l = NullableList(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test l[k] ≅ v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test l[idx] ≅ v[idx]
        end
        @test l[:] ≅ v
    end
end


@testset "BitPrimitive_access" begin
    len = 10
    bits = vcat(UInt8[0xfd,0x02], zeros(UInt8, 6))
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, bits, rpad)
    v = Bool[true,false,true,true,true,true,true,true,false,true]
    p = BitPrimitive(b, 1+length(lpad), 10)
    for j ∈ 1:N_IDX_CHECK
        k = rand(1:len)
        @test p[k] == v[k]
    end
    for j ∈ 1:N_IDX_CHECK
        idx = rand(1:len, rand(1:MAX_IDX_LEN))
        @test p[idx] == v[idx]
    end
    @test p[:] == v
end


@testset "BitPrimitive_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        v = rand(Bool, len)
        p = BitPrimitive(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] == v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] == v[idx]
        end
        @test p[:] == v
    end
end


@testset "NullableBitPrimitive_access" begin
    len = 7
    bits = vcat(UInt8[0x4f], zeros(UInt8, 7))
    bmask = vcat(UInt8[0x79], zeros(UInt8, 7))
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, bmask, bits, rpad)
    v = [true,missing,missing,true,false,false,true]
    p = NullableBitPrimitive(b, 1+length(lpad), 1+length(lpad)+length(bmask), length(v))
    for j ∈ 1:N_IDX_CHECK
        k = rand(1:len)
        @test p[k] ≅ v[k]
    end
    for j ∈ 1:N_IDX_CHECK
        idx = rand(1:len, rand(1:MAX_IDX_LEN))
        @test p[idx] ≅ v[idx]
    end
    @test p[:] ≅ v
end


@testset "NullableBitPrimitive_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        pres = rand(Bool, len)
        vals = rand(Bool, len)
        v = Union{Bool,Missing}[pres[i] ? missing : vals[i] for i ∈ 1:len]
        p = NullableBitPrimitive(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] ≅ v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] ≅ v[idx]
        end
        @test p[:] ≅ v
    end
end


@testset "arrowformat_construct" begin
    len = rand(1:MAX_VECTOR_LENGTH)
    randstring_() = randstring(rand(1:MAX_STRING_LENGTH))


    p = arrowformat(rand(Float64, len))
    @test typeof(p) == Primitive{Float64}

    mask = rand(Bool, len)

    v = Union{Float32,Missing}[mask[i] ? rand(Float32) : missing for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Float32}

    v = String[randstring_() for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == List{String,Primitive{UInt8}}

    v = Union{String,Missing}[mask[i] ? randstring_() : missing for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == NullableList{String,Primitive{UInt8}}

    v = rand(Bool, len)
    p = arrowformat(v)
    @test typeof(p) == BitPrimitive

    v = Union{Bool,Missing}[mask[i] ? rand(Bool) : missing for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == NullableBitPrimitive

    v = Date[Date(1), Date(2)]
    p = arrowformat(v)
    @test typeof(p) == Primitive{Arrow.Datestamp}

    v = DateTime[DateTime(0), DateTime(1)]
    p = arrowformat(v)
    @test typeof(p) == Primitive{Arrow.Timestamp{Millisecond}}

    v = Time[Time(0), Time(1)]
    p = arrowformat(v)
    @test typeof(p) == Primitive{Arrow.TimeOfDay{Nanosecond,Int64}}

    v = Union{Date,Missing}[Date(1), missing]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Arrow.Datestamp}

    v = Union{DateTime,Missing}[DateTime(0), missing]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Arrow.Timestamp{Millisecond}}

    v = Union{Time,Missing}[Time(0), missing]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Arrow.TimeOfDay{Nanosecond,Int64}}
end
