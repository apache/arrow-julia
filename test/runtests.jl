using Arrow
using Compat, Compat.Test, Compat.Random, Compat.Dates
using CategoricalArrays

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
const OFFSET_ELTYPES = [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64]

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
            @test sp ≅ sv
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
    offstype = rand(OFFSET_ELTYPES)
    offs = convert(Vector{offstype}, [0,4,7,8,12,14])
    vals = convert(Vector{UInt8}, codeunits("firewalkwithme"))
    valspad = zeros(UInt8, Arrow.paddinglength(length(vals)))
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, convert(Vector{UInt8}, reinterpret(UInt8, offs)), vals, valspad, rpad)
    l = List{String,offstype}(b, 1+length(lpad), 1+length(lpad)+sizeof(offstype)*length(offs),
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
    offstype = rand(OFFSET_ELTYPES)
    offs = convert(Vector{offstype}, [0,4,9,9,14,14,17,21])
    vals = convert(Vector{UInt8}, codeunits("kirkspockbonesncc1701"))
    valspad = zeros(UInt8, Arrow.paddinglength(length(vals)))
    pres = Bool[true,true,false,true,false,true,true]
    mask = Arrow.bitpackpadded(pres)
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, mask, convert(Vector{UInt8}, reinterpret(UInt8, offs)), vals, valspad, rpad)
    l = NullableList{String,offstype}(b, 1+length(lpad), 1+length(lpad)+length(mask),
                                      1+length(lpad)+length(mask)+sizeof(offstype)*length(offs),
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


@testset "DictEncoding_access" begin
    len = 7
    refs = Primitive(Int32[0,1,2,1,0,3,2])
    data = List(["fire", "walk", "with", "me"])
    d = DictEncoding(refs, data)
    @test references(d) == refs
    @test levels(d) == data
    @test d[1] == "fire"
    @test d[2] == "walk"
    @test d[3] == "with"
    @test d[4] == "walk"
    @test d[5] == "fire"
    @test d[6] == "me"
    @test d[7] == "with"
    @test d[[1,4,3,6]] == ["fire", "walk", "with", "me"]
    @test d[[true,false,false,false,false,false,true]] == ["fire", "with"]
    @test d[:] == ["fire", "walk", "with", "walk", "fire", "me", "with"]
end


@testset "DictEncoding_construct" begin
    v = [-999, missing, 55, -999, 42]
    d = DictEncoding(categorical(v))
    pool = CategoricalPool{Int,Int32}([-999, 55, 42])
    ref = CategoricalArray{Union{Int,Missing},1}(Int32[1,0,2,1,3], pool)
    @test typeof(d.refs) == NullablePrimitive{Int32}
    @test typeof(d.pool) == Primitive{Int64}
    @test d[1] == -999
    @test ismissing(d[2])
    @test d[3] == 55
    @test d[4] == -999
    @test d[5] == 42
    @test d[[1,3,5]] ≅ v[[1,3,5]]
    @test d[[false,true,false,true,false]] ≅ v[[false,true,false,true,false]]
    @test d[1:end] ≅ v
    @test categorical(d) ≅ ref
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
    @test typeof(p) == List{String,Int64,Primitive{UInt8}}

    v = Union{String,Missing}[mask[i] ? randstring_() : missing for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == NullableList{String,Int64,Primitive{UInt8}}

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

    v = categorical(["a", "b", "c"])
    p = arrowformat(v)
    @test typeof(p) == DictEncoding{String,Primitive{Int32},List{String,Int64,Primitive{UInt8}}}
end
