# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

using Test, Arrow, Tables, Dates, PooledArrays, TimeZones, UUIDs, CategoricalArrays, DataAPI

include(joinpath(dirname(pathof(Arrow)), "ArrowTypes/test/tests.jl"))
include(joinpath(dirname(pathof(Arrow)), "../test/testtables.jl"))
include(joinpath(dirname(pathof(Arrow)), "../test/integrationtest.jl"))
include(joinpath(dirname(pathof(Arrow)), "../test/dates.jl"))

struct CustomStruct
    x::Int
    y::Float64
    z::String
end

struct CustomStruct2{sym}
    x::Int
end

@testset "Arrow" begin

@testset "table roundtrips" begin

for case in testtables
    testtable(case...)
end

end # @testset "table roundtrips"

@testset "arrow json integration tests" begin

for file in readdir(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"))
    jsonfile = joinpath(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"), file)
    println("integration test for $jsonfile")
    df = ArrowJSON.parsefile(jsonfile);
    io = Arrow.tobuffer(df)
    tbl = Arrow.Table(io; convert=false);
    @test isequal(df, tbl)
end

end # @testset "arrow json integration tests"

@testset "misc" begin

# multiple record batches
t = Tables.partitioner(((col1=Union{Int64, Missing}[1,2,3,4,5,6,7,8,9,missing],), (col1=Union{Int64, Missing}[missing,11],)))
io = Arrow.tobuffer(t)
tt = Arrow.Table(io)
@test length(tt) == 1
@test isequal(tt.col1, vcat([1,2,3,4,5,6,7,8,9,missing], [missing,11]))
@test eltype(tt.col1) === Union{Int64, Missing}

# Arrow.Stream
seekstart(io)
str = Arrow.Stream(io)
state = iterate(str)
@test state !== nothing
tt, st = state
@test length(tt) == 1
@test isequal(tt.col1, [1,2,3,4,5,6,7,8,9,missing])

state = iterate(str, st)
@test state !== nothing
tt, st = state
@test length(tt) == 1
@test isequal(tt.col1, [missing,11])

@test iterate(str, st) === nothing

@test isequal(collect(str)[1].col1, [1,2,3,4,5,6,7,8,9,missing])
@test isequal(collect(str)[2].col1, [missing,11])

# dictionary batch isDelta
t = (
    col1=Int64[1,2,3,4],
    col2=Union{String, Missing}["hey", "there", "sailor", missing],
    col3=NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b=(c="sailor",)), (a=Int64(4), b=(c="jo-bob",))]
)
t2 = (
    col1=Int64[1,2,5,6],
    col2=Union{String, Missing}["hey", "there", "sailor2", missing],
    col3=NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(5), b=(c="sailor2",)), (a=Int64(4), b=(c="jo-bob",))]
)
tt = Tables.partitioner((t, t2))
tt = Arrow.Table(Arrow.tobuffer(tt; dictencode=true, dictencodenested=true))
@test tt.col1 == [1,2,3,4,1,2,5,6]
@test isequal(tt.col2, ["hey", "there", "sailor", missing, "hey", "there", "sailor2", missing])
@test isequal(tt.col3, vcat(NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b=(c="sailor",)), (a=Int64(4), b=(c="jo-bob",))], NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(5), b=(c="sailor2",)), (a=Int64(4), b=(c="jo-bob",))]))

t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
meta = Dict("key1" => "value1", "key2" => "value2")
Arrow.setmetadata!(t, meta)
meta2 = Dict("colkey1" => "colvalue1", "colkey2" => "colvalue2")
Arrow.setmetadata!(t.col1, meta2)
tt = Arrow.Table(Arrow.tobuffer(t))
@test length(tt) == length(t)
@test tt.col1 == t.col1
@test eltype(tt.col1) === Int64
@test Arrow.getmetadata(tt) == meta
@test Arrow.getmetadata(tt.col1) == meta2

# custom compressors
lz4 = Arrow.CodecLz4.LZ4FrameCompressor(; compressionlevel=8)
Arrow.CodecLz4.TranscodingStreams.initialize(lz4)
t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
tt = Arrow.Table(Arrow.tobuffer(t; compress=lz4))
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

zstd = Arrow.CodecZstd.ZstdCompressor(; level=8)
Arrow.CodecZstd.TranscodingStreams.initialize(zstd)
t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
tt = Arrow.Table(Arrow.tobuffer(t; compress=zstd))
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# custom alignment
t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
tt = Arrow.Table(Arrow.tobuffer(t; alignment=64))
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# 53
s = "a" ^ 100
t = (a=[SubString(s, 1:10), SubString(s, 11:20)],)
tt = Arrow.Table(Arrow.tobuffer(t))
@test tt.a == ["aaaaaaaaaa", "aaaaaaaaaa"]

# 49
@test_throws ArgumentError Arrow.Table("file_that_doesnt_exist")

# 52
t = (a=Arrow.DictEncode(string.(1:129)),)
tt = Arrow.Table(Arrow.tobuffer(t))

# 60: unequal column lengths
io = IOBuffer()
@test_throws ArgumentError Arrow.write(io, (a = Int[], b = ["asd"], c=collect(1:100)))

# nullability of custom extension types
t = (a=['a', missing],)
tt = Arrow.Table(Arrow.tobuffer(t))
@test isequal(tt.a, ['a', missing])

# automatic custom struct serialization/deserialization
t = (col1=[CustomStruct(1, 2.3, "hey"), CustomStruct(4, 5.6, "there")],)

Arrow.ArrowTypes.arrowname(::Type{CustomStruct}) = Symbol("JuliaLang.CustomStruct")
Arrow.ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.CustomStruct")}, S) = CustomStruct
tt = Arrow.Table(Arrow.tobuffer(t))
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# 76
t = (col1=NamedTuple{(:a,),Tuple{Union{Int,String}}}[(a=1,), (a="x",)],)
tt = Arrow.Table(Arrow.tobuffer(t))
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# 89 etc. - test deprecation paths for old UUID autoconversion + UUID FixedSizeListKind overloads
u = 0x6036fcbd20664bd8a65cdfa25434513f
@test Arrow.ArrowTypes.arrowconvert(UUID, (value=u,)) === UUID(u)
@test Arrow.ArrowTypes.arrowconvert(UUID, u) === UUID(u)
@test Arrow.ArrowTypes.gettype(Arrow.ArrowTypes.ArrowKind(UUID)) == UInt8
@test Arrow.ArrowTypes.getsize(Arrow.ArrowTypes.ArrowKind(UUID)) == 16

# 98
t = (a = [Nanosecond(0), Nanosecond(1)], b = [uuid4(), uuid4()], c = [missing, Nanosecond(1)])
tt = Arrow.Table(Arrow.tobuffer(t))
@test copy(tt.a) isa Vector{Nanosecond}
@test copy(tt.b) isa Vector{UUID}
@test copy(tt.c) isa Vector{Union{Missing,Nanosecond}}

# copy on DictEncoding w/ missing values
x = PooledArray(["hey", missing])
x2 = Arrow.toarrowvector(x)
@test isequal(copy(x2), x)

# some dict encoding coverage

# signed indices for DictEncodedKind #112 #113 #114
av = Arrow.toarrowvector(PooledArray(repeat(["a", "b"], inner = 5)))
@test isa(first(av.indices), Signed)

av = Arrow.toarrowvector(CategoricalArray(repeat(["a", "b"], inner = 5)))
@test isa(first(av.indices), Signed)

av = Arrow.toarrowvector(CategoricalArray(["a", "bb", missing]))
@test isa(first(av.indices), Signed)
@test length(av) == 3
@test eltype(av) == Union{String, Missing}

av = Arrow.toarrowvector(CategoricalArray(["a", "bb", "ccc"]))
@test isa(first(av.indices), Signed)
@test length(av) == 3
@test eltype(av) == String

# 120
x = PooledArray(["hey", missing])
x2 = Arrow.toarrowvector(x)
@test eltype(DataAPI.refpool(x2)) == Union{Missing, String}
@test eltype(DataAPI.levels(x2)) == String
@test DataAPI.refarray(x2) == [1, 2]

# 121
a = PooledArray(repeat(string.('S', 1:130), inner=5), compress=true)
@test eltype(a.refs) == UInt8
av = Arrow.toarrowvector(a)
@test eltype(av.indices) == Int16

# 123
t = (x = collect(zip(rand(10), rand(10))),)
tt = Arrow.Table(Arrow.tobuffer(t))
@test tt.x == t.x

# 144
t = Tables.partitioner(((a=Arrow.DictEncode([1,2,3]),), (a=Arrow.DictEncode(fill(1, 129)),)))
tt = Arrow.Table(Arrow.tobuffer(t))
@test length(tt.a) == 132

# 126
t = Tables.partitioner(
    (
        (a=Arrow.toarrowvector(PooledArray([1,2,3  ])),),
        (a=Arrow.toarrowvector(PooledArray([1,2,3,4])),),
        (a=Arrow.toarrowvector(PooledArray([1,2,3,4,5])),),
    )
)
tt = Arrow.Table(Arrow.tobuffer(t))
@test length(tt.a) == 12
@test tt.a == [1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5]

t = Tables.partitioner(
    (
        (a=Arrow.toarrowvector(PooledArray([1,2,3  ], signed=true, compress=true)),),
        (a=Arrow.toarrowvector(PooledArray(collect(1:129))),),
    )
)
io = IOBuffer()
@test_throws ErrorException Arrow.write(io, t)

# 75
tbl = Arrow.Table(Arrow.tobuffer((sets = [Set([1,2,3]), Set([1,2,3])],)))
@test eltype(tbl.sets) <: Set

# 85
tbl = Arrow.Table(Arrow.tobuffer((tups = [(1, 3.14, "hey"), (1, 3.14, "hey")],)))
@test eltype(tbl.tups) <: Tuple

# Nothing
tbl = Arrow.Table(Arrow.tobuffer((nothings=[nothing, nothing, nothing],)))
@test tbl.nothings == [nothing, nothing, nothing]

# arrowmetadata
t = (col1=[CustomStruct2{:hey}(1), CustomStruct2{:hey}(2)],)
ArrowTypes.arrowname(::Type{<:CustomStruct2}) = Symbol("CustomStruct2")
tbl = Arrow.Table(Arrow.tobuffer(t))
# test we get the warning about deserializing
@test eltype(tbl.col1) <: NamedTuple
ArrowTypes.arrowmetadata(::Type{CustomStruct2{sym}}) where {sym} = sym
ArrowTypes.JuliaType(::Val{:CustomStruct2}, S, meta) = CustomStruct2{Symbol(meta)}
tbl = Arrow.Table(Arrow.tobuffer(t))
@test eltype(tbl.col1) == CustomStruct2{:hey}

end # @testset "misc"

end
