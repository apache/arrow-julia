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

using Test
using Arrow
using ArrowTypes
using Tables
using Dates
using PooledArrays
using TimeZones
using UUIDs
using Sockets
using CategoricalArrays
using DataAPI
using FilePathsBase
using DataFrames
import Random: randstring

include(joinpath(dirname(pathof(ArrowTypes)), "../test/tests.jl"))
include(joinpath(dirname(pathof(Arrow)), "../test/testtables.jl"))
include(joinpath(dirname(pathof(Arrow)), "../test/testappend.jl"))
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

    @testset "table append" begin
        # skip windows since file locking prevents mktemp cleanup
        if !Sys.iswindows()
            for case in testtables
                testappend(case...)
            end

            testappend_partitions()

            for compression_option in (:lz4, :zstd)
                testappend_compression(compression_option)
            end
        end
    end # @testset "table append"

    @testset "arrow json integration tests" begin
        for file in readdir(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"))
            jsonfile = joinpath(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"), file)
            @testset "integration test for $jsonfile" begin
                df = ArrowJSON.parsefile(jsonfile)
                io = Arrow.tobuffer(df)
                tbl = Arrow.Table(io; convert=false)
                @test isequal(df, tbl)
            end
        end
    end # @testset "arrow json integration tests"

    @testset "abstract path" begin
        # Make a custom path type that simulates how AWSS3.jl's S3Path works
        struct CustomPath <: AbstractPath
            path::PosixPath
        end

        Base.read(p::CustomPath) = read(p.path)

        io = Arrow.tobuffer((col=[0],))
        tt = Arrow.Table(io)

        mktempdir() do dir
            p = Path(joinpath(dir, "test.arrow"))
            Arrow.write(p, tt)
            @test isfile(p)

            # skip windows since file locking prevents mktemp cleanup
            if !Sys.iswindows()
                tt2 = Arrow.Table(p)
                @test values(tt) == values(tt2)

                tt3 = Arrow.Table(CustomPath(p))
                @test values(tt) == values(tt3)
            end
        end
    end # @testset "abstract path"

    @testset "misc" begin
        @testset "# multiple record batches" begin
            t = Tables.partitioner((
                (col1=Union{Int64,Missing}[1, 2, 3, 4, 5, 6, 7, 8, 9, missing],),
                (col1=Union{Int64,Missing}[missing, 11],),
            ))
            io = Arrow.tobuffer(t)
            tt = Arrow.Table(io)
            @test length(tt) == 1
            @test isequal(
                tt.col1,
                vcat([1, 2, 3, 4, 5, 6, 7, 8, 9, missing], [missing, 11]),
            )
            @test eltype(tt.col1) === Union{Int64,Missing}

            # Arrow.Stream
            seekstart(io)
            str = Arrow.Stream(io)
            @test eltype(str) == Arrow.Table
            @test !Base.isdone(str)
            state = iterate(str)
            @test state !== nothing
            tt, st = state
            @test length(tt) == 1
            @test isequal(tt.col1, [1, 2, 3, 4, 5, 6, 7, 8, 9, missing])

            state = iterate(str, st)
            @test state !== nothing
            tt, st = state
            @test length(tt) == 1
            @test isequal(tt.col1, [missing, 11])

            @test iterate(str, st) === nothing

            @test isequal(collect(str)[1].col1, [1, 2, 3, 4, 5, 6, 7, 8, 9, missing])
            @test isequal(collect(str)[2].col1, [missing, 11])
        end

        @testset "# dictionary batch isDelta" begin
            t = (
                col1=Int64[1, 2, 3, 4],
                col2=Union{String,Missing}["hey", "there", "sailor", missing],
                col3=NamedTuple{
                    (:a, :b),
                    Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}},
                }[
                    (a=Int64(1), b=missing),
                    (a=Int64(1), b=missing),
                    (a=Int64(3), b=(c="sailor",)),
                    (a=Int64(4), b=(c="jo-bob",)),
                ],
            )
            t2 = (
                col1=Int64[1, 2, 5, 6],
                col2=Union{String,Missing}["hey", "there", "sailor2", missing],
                col3=NamedTuple{
                    (:a, :b),
                    Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}},
                }[
                    (a=Int64(1), b=missing),
                    (a=Int64(1), b=missing),
                    (a=Int64(5), b=(c="sailor2",)),
                    (a=Int64(4), b=(c="jo-bob",)),
                ],
            )
            tt = Tables.partitioner((t, t2))
            tt = Arrow.Table(Arrow.tobuffer(tt; dictencode=true, dictencodenested=true))
            @test tt.col1 == [1, 2, 3, 4, 1, 2, 5, 6]
            @test isequal(
                tt.col2,
                ["hey", "there", "sailor", missing, "hey", "there", "sailor2", missing],
            )
            @test isequal(
                tt.col3,
                vcat(
                    NamedTuple{
                        (:a, :b),
                        Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}},
                    }[
                        (a=Int64(1), b=missing),
                        (a=Int64(1), b=missing),
                        (a=Int64(3), b=(c="sailor",)),
                        (a=Int64(4), b=(c="jo-bob",)),
                    ],
                    NamedTuple{
                        (:a, :b),
                        Tuple{Int64,Union{Missing,NamedTuple{(:c,),Tuple{String}}}},
                    }[
                        (a=Int64(1), b=missing),
                        (a=Int64(1), b=missing),
                        (a=Int64(5), b=(c="sailor2",)),
                        (a=Int64(4), b=(c="jo-bob",)),
                    ],
                ),
            )
        end

        @testset "metadata" begin
            t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
            meta = Dict("key1" => "value1", "key2" => "value2")
            meta2 = Dict("colkey1" => "colvalue1", "colkey2" => "colvalue2")
            tt = Arrow.Table(
                Arrow.tobuffer(t; colmetadata=Dict(:col1 => meta2), metadata=meta),
            )
            @test length(tt) == length(t)
            @test tt.col1 == t.col1
            @test eltype(tt.col1) === Int64
            @test Arrow.getmetadata(tt) == Arrow.toidict(meta)
            @test Arrow.getmetadata(tt.col1) == Arrow.toidict(meta2)

            t = (col1=collect(1:10), col2=collect('a':'j'), col3=collect(1:10))
            meta = ("key1" => :value1, :key2 => "value2")
            meta2 = ("colkey1" => :colvalue1, :colkey2 => "colvalue2")
            meta3 = ("colkey3" => :colvalue3,)
            tt = Arrow.Table(
                Arrow.tobuffer(
                    t;
                    colmetadata=Dict(:col2 => meta2, :col3 => meta3),
                    metadata=meta,
                ),
            )
            @test Arrow.getmetadata(tt) ==
                  Arrow.toidict(String(k) => String(v) for (k, v) in meta)
            @test Arrow.getmetadata(tt.col1) === nothing
            @test Arrow.getmetadata(tt.col2)["colkey1"] == "colvalue1"
            @test Arrow.getmetadata(tt.col2)["colkey2"] == "colvalue2"
            @test Arrow.getmetadata(tt.col3)["colkey3"] == "colvalue3"
        end

        @testset "# custom compressors" begin
            lz4 = Arrow.CodecLz4.LZ4FrameCompressor(; compressionlevel=8)
            Arrow.CodecLz4.TranscodingStreams.initialize(lz4)
            t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
            tt = Arrow.Table(Arrow.tobuffer(t; compress=lz4))
            @test length(tt) == length(t)
            @test all(isequal.(values(t), values(tt)))

            zstd = Arrow.CodecZstd.ZstdCompressor(; level=8)
            Arrow.CodecZstd.TranscodingStreams.initialize(zstd)
            t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
            tt = Arrow.Table(Arrow.tobuffer(t; compress=zstd))
            @test length(tt) == length(t)
            @test all(isequal.(values(t), values(tt)))
        end

        @testset "# custom alignment" begin
            t = (col1=Int64[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],)
            tt = Arrow.Table(Arrow.tobuffer(t; alignment=64))
            @test length(tt) == length(t)
            @test all(isequal.(values(t), values(tt)))
        end

        @testset "# 53" begin
            s = "a"^100
            t = (a=[SubString(s, 1:10), SubString(s, 11:20)],)
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test tt.a == ["aaaaaaaaaa", "aaaaaaaaaa"]
        end

        @testset "# 49" begin
            @test_throws SystemError Arrow.Table("file_that_doesnt_exist")
            @test_throws SystemError Arrow.Table(p"file_that_doesnt_exist")
        end

        @testset "# 52" begin
            t = (a=Arrow.DictEncode(string.(1:129)),)
            tt = Arrow.Table(Arrow.tobuffer(t))
        end

        @testset "# 60: unequal column lengths" begin
            io = IOBuffer()
            @test_throws ArgumentError Arrow.write(
                io,
                (a=Int[], b=["asd"], c=collect(1:100)),
            )
        end

        @testset "# nullability of custom extension types" begin
            t = (a=['a', missing],)
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test isequal(tt.a, ['a', missing])
        end

        @testset "# automatic custom struct serialization/deserialization" begin
            t = (col1=[CustomStruct(1, 2.3, "hey"), CustomStruct(4, 5.6, "there")],)

            Arrow.ArrowTypes.arrowname(::Type{CustomStruct}) =
                Symbol("JuliaLang.CustomStruct")
            Arrow.ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.CustomStruct")}, S) =
                CustomStruct
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test length(tt) == length(t)
            @test all(isequal.(values(t), values(tt)))
        end

        @testset "# 76" begin
            t = (col1=NamedTuple{(:a,),Tuple{Union{Int,String}}}[(a=1,), (a="x",)],)
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test length(tt) == length(t)
            @test all(isequal.(values(t), values(tt)))
        end

        @testset "# 89 etc. - UUID FixedSizeListKind overloads" begin
            @test Arrow.ArrowTypes.gettype(Arrow.ArrowTypes.ArrowKind(UUID)) == UInt8
            @test Arrow.ArrowTypes.getsize(Arrow.ArrowTypes.ArrowKind(UUID)) == 16
        end

        @testset "# 98" begin
            t = (
                a=[Nanosecond(0), Nanosecond(1)],
                b=[uuid4(), uuid4()],
                c=[missing, Nanosecond(1)],
            )
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test copy(tt.a) isa Vector{Nanosecond}
            @test copy(tt.b) isa Vector{UUID}
            @test copy(tt.c) isa Vector{Union{Missing,Nanosecond}}
        end

        @testset "# copy on DictEncoding w/ missing values" begin
            x = PooledArray(["hey", missing])
            x2 = Arrow.toarrowvector(x)
            @test isequal(copy(x2), x)
        end

        @testset "# some dict encoding coverage" begin
            # signed indices for DictEncodedKind #112 #113 #114
            av = Arrow.toarrowvector(PooledArray(repeat(["a", "b"], inner=5)))
            @test isa(first(av.indices), Signed)

            av = Arrow.toarrowvector(CategoricalArray(repeat(["a", "b"], inner=5)))
            @test isa(first(av.indices), Signed)

            av = Arrow.toarrowvector(CategoricalArray(["a", "bb", missing]))
            @test isa(first(av.indices), Signed)
            @test length(av) == 3
            @test eltype(av) == Union{String,Missing}

            av = Arrow.toarrowvector(CategoricalArray(["a", "bb", "ccc"]))
            @test isa(first(av.indices), Signed)
            @test length(av) == 3
            @test eltype(av) == String
        end

        @testset "# 120" begin
            x = PooledArray(["hey", missing])
            x2 = Arrow.toarrowvector(x)
            @test eltype(DataAPI.refpool(x2)) == Union{Missing,String}
            @test eltype(DataAPI.levels(x2)) == String
            @test DataAPI.refarray(x2) == [1, 2]
        end

        @testset "# 121" begin
            a = PooledArray(repeat(string.('S', 1:130), inner=5), compress=true)
            @test eltype(a.refs) == UInt8
            av = Arrow.toarrowvector(a)
            @test eltype(av.indices) == Int16
        end

        @testset "# 123" begin
            t = (x=collect(zip(rand(10), rand(10))),)
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test tt.x == t.x
        end

        @testset "# 144" begin
            t = Tables.partitioner((
                (a=Arrow.DictEncode([1, 2, 3]),),
                (a=Arrow.DictEncode(fill(1, 129)),),
            ))
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test length(tt.a) == 132
        end

        @testset "# 126" begin
            t = Tables.partitioner((
                (a=Arrow.toarrowvector(PooledArray([1, 2, 3])),),
                (a=Arrow.toarrowvector(PooledArray([1, 2, 3, 4])),),
                (a=Arrow.toarrowvector(PooledArray([1, 2, 3, 4, 5])),),
            ))
            tt = Arrow.Table(Arrow.tobuffer(t))
            @test length(tt.a) == 12
            @test tt.a == [1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5]

            t = Tables.partitioner((
                (
                    a=Arrow.toarrowvector(
                        PooledArray([1, 2, 3], signed=true, compress=true),
                    ),
                ),
                (a=Arrow.toarrowvector(PooledArray(collect(1:129))),),
            ))
            io = IOBuffer()
            @test_logs (:error, "error writing arrow data on partition = 2") begin
                @test_throws ErrorException Arrow.write(io, t)
            end
        end

        @testset "# 75" begin
            tbl = Arrow.Table(Arrow.tobuffer((sets=[Set([1, 2, 3]), Set([1, 2, 3])],)))
            @test eltype(tbl.sets) <: Set
        end

        @testset "# 85" begin
            tbl = Arrow.Table(Arrow.tobuffer((tups=[(1, 3.14, "hey"), (1, 3.14, "hey")],)))
            @test eltype(tbl.tups) <: Tuple
        end

        @testset "Nothing" begin
            tbl = Arrow.Table(Arrow.tobuffer((nothings=[nothing, nothing, nothing],)))
            @test tbl.nothings == [nothing, nothing, nothing]
        end

        @testset "arrowmetadata" begin
            # arrowmetadata
            t = (col1=[CustomStruct2{:hey}(1), CustomStruct2{:hey}(2)],)
            ArrowTypes.arrowname(::Type{<:CustomStruct2}) = Symbol("CustomStruct2")
            @test_logs (:warn, r"unsupported ARROW:extension:name type: \"CustomStruct2\"") begin
                tbl = Arrow.Table(Arrow.tobuffer(t))
            end
            @test eltype(tbl.col1) <: NamedTuple
            ArrowTypes.arrowmetadata(::Type{CustomStruct2{sym}}) where {sym} = sym
            ArrowTypes.JuliaType(::Val{:CustomStruct2}, S, meta) =
                CustomStruct2{Symbol(meta)}
            tbl = Arrow.Table(Arrow.tobuffer(t))
            @test eltype(tbl.col1) == CustomStruct2{:hey}
        end

        @testset "# 166" begin
            t = (col1=[zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.NANOSECOND,nothing})],)
            tbl = Arrow.Table(Arrow.tobuffer(t))
            @test_logs (
                :warn,
                r"automatically converting Arrow.Timestamp with precision = NANOSECOND",
            ) begin
                @test tbl.col1[1] == Dates.DateTime(1970)
            end
        end

        @testset "# 95; Arrow.ToTimestamp" begin
            x = [ZonedDateTime(Dates.DateTime(2020), tz"Europe/Paris")]
            c = Arrow.ToTimestamp(x)
            @test eltype(c) ==
                  Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MILLISECOND,Symbol("Europe/Paris")}
            @test c[1] ==
                  Arrow.Timestamp{Arrow.Flatbuf.TimeUnit.MILLISECOND,Symbol("Europe/Paris")}(
                1577833200000,
            )
        end

        @testset "# 158" begin
            # arrow ipc stream generated from pyarrow with no record batches
            bytes = UInt8[
                0xff,
                0xff,
                0xff,
                0xff,
                0x78,
                0x00,
                0x00,
                0x00,
                0x10,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x0a,
                0x00,
                0x0c,
                0x00,
                0x06,
                0x00,
                0x05,
                0x00,
                0x08,
                0x00,
                0x0a,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
                0x04,
                0x00,
                0x0c,
                0x00,
                0x00,
                0x00,
                0x08,
                0x00,
                0x08,
                0x00,
                0x00,
                0x00,
                0x04,
                0x00,
                0x08,
                0x00,
                0x00,
                0x00,
                0x04,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0x00,
                0x00,
                0x14,
                0x00,
                0x00,
                0x00,
                0x10,
                0x00,
                0x14,
                0x00,
                0x08,
                0x00,
                0x06,
                0x00,
                0x07,
                0x00,
                0x0c,
                0x00,
                0x00,
                0x00,
                0x10,
                0x00,
                0x10,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
                0x02,
                0x10,
                0x00,
                0x00,
                0x00,
                0x1c,
                0x00,
                0x00,
                0x00,
                0x04,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0x00,
                0x00,
                0x61,
                0x00,
                0x00,
                0x00,
                0x08,
                0x00,
                0x0c,
                0x00,
                0x08,
                0x00,
                0x07,
                0x00,
                0x08,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x01,
                0x40,
                0x00,
                0x00,
                0x00,
                0xff,
                0xff,
                0xff,
                0xff,
                0x00,
                0x00,
                0x00,
                0x00,
            ]
            tbl = Arrow.Table(bytes)
            @test length(tbl.a) == 0
            @test eltype(tbl.a) == Union{Int64,Missing}
        end

        @testset "# 181" begin
            d = Dict{Int,Int}()
            for i = 1:9
                d = Dict(i => d)
            end
            tbl = (x=[d],)
            msg = "reached nested serialization level (20) deeper than provided max depth argument (19); to increase allowed nesting level, pass `maxdepth=X`"
            @test_throws ErrorException(msg) Arrow.tobuffer(tbl; maxdepth=19)
            @test Arrow.Table(Arrow.tobuffer(tbl; maxdepth=20)).x == tbl.x
        end

        @testset "# 167" begin
            t = (col1=[["boop", "she"], ["boop", "she"], ["boo"]],)
            tbl = Arrow.Table(Arrow.tobuffer(t))
            @test eltype(tbl.col1) <: AbstractVector{String}
        end

        @testset "# 200 VersionNumber" begin
            t = (col1=[v"1"],)
            tbl = Arrow.Table(Arrow.tobuffer(t))
            @test eltype(tbl.col1) == VersionNumber
        end

        @testset "`show`" begin
            str = nothing
            table = (; a=1:5, b=fill(1.0, 5))
            arrow_table = Arrow.Table(Arrow.tobuffer(table))
            # 2 and 3-arg show with no metadata
            for outer str in
                (sprint(show, arrow_table), sprint(show, MIME"text/plain"(), arrow_table))
                @test length(str) < 100
                @test occursin("5 rows", str)
                @test occursin("2 columns", str)
                @test occursin("Int", str)
                @test occursin("Float64", str)
                @test !occursin("metadata entries", str)
            end

            # 2-arg show with metadata
            big_dict = Dict((randstring(rand(5:10)) => randstring(rand(1:3)) for _ = 1:100))
            arrow_table = Arrow.Table(Arrow.tobuffer(table; metadata=big_dict))
            str2 = sprint(show, arrow_table)
            @test length(str2) > length(str)
            @test length(str2) < 200
            @test occursin("metadata entries", str2)

            # 3-arg show with metadata
            str3 = sprint(
                show,
                MIME"text/plain"(),
                arrow_table;
                context=IOContext(IOBuffer(), :displaysize => (24, 100), :limit => true),
            )
            @test length(str3) < 1000
            # some but not too many `=>`'s for printing the metadata
            @test 5 < length(collect(eachmatch(r"=>", str3))) < 20
        end

        @testset "# 194" begin
            @test isempty(Arrow.Table(Arrow.tobuffer(Dict{Symbol,Vector}())))
        end

        @testset "# 229" begin
            struct Foo229{x}
                y::String
                z::Int
            end
            Arrow.ArrowTypes.arrowname(::Type{<:Foo229}) = Symbol("JuliaLang.Foo229")
            Arrow.ArrowTypes.ArrowType(::Type{Foo229{x}}) where {x} =
                Tuple{String,String,Int}
            Arrow.ArrowTypes.toarrow(row::Foo229{x}) where {x} = (String(x), row.y, row.z)
            Arrow.ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.Foo229")}, ::Any) = Foo229
            Arrow.ArrowTypes.fromarrow(::Type{<:Foo229}, x, y, z) = Foo229{Symbol(x)}(y, z)
            cols = (
                k1=[Foo229{:a}("a", 1), Foo229{:b}("b", 2)],
                k2=[Foo229{:c}("c", 3), Foo229{:d}("d", 4)],
            )
            tbl = Arrow.Table(Arrow.tobuffer(cols))
            @test tbl.k1 == cols.k1
            @test tbl.k2 == cols.k2
        end

        @testset "# PR 234" begin
            # bugfix parsing primitive arrays
            buf = [
                0x14,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x0e,
                0x00,
                0x14,
                0x00,
                0x00,
                0x00,
                0x10,
                0x00,
                0x0c,
                0x00,
                0x08,
                0x00,
                0x04,
                0x00,
                0x0e,
                0x00,
                0x00,
                0x00,
                0x2c,
                0x00,
                0x00,
                0x00,
                0x38,
                0x00,
                0x00,
                0x00,
                0x38,
                0x00,
                0x00,
                0x00,
                0x38,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x03,
                0x00,
                0x00,
                0x00,
                0x01,
                0x00,
                0x00,
                0x00,
                0x02,
                0x00,
                0x00,
                0x00,
                0x03,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
            ]

            struct TestData <: Arrow.FlatBuffers.Table
                bytes::Vector{UInt8}
                pos::Base.Int
            end

            function Base.getproperty(x::TestData, field::Symbol)
                if field === :DataInt32
                    o = Arrow.FlatBuffers.offset(x, 12)
                    o != 0 && return Arrow.FlatBuffers.Array{Int32}(x, o)
                else
                    @warn "field $field not supported"
                end
            end

            d = Arrow.FlatBuffers.getrootas(TestData, buf, 0)
            @test d.DataInt32 == UInt32[1, 2, 3]
        end

        @testset "# test multiple inputs treated as one table" begin
            t = (col1=[1, 2, 3, 4, 5], col2=[1.2, 2.3, 3.4, 4.5, 5.6])
            tbl = Arrow.Table([Arrow.tobuffer(t), Arrow.tobuffer(t)])
            @test tbl.col1 == [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
            @test tbl.col2 == [1.2, 2.3, 3.4, 4.5, 5.6, 1.2, 2.3, 3.4, 4.5, 5.6]

            # schemas must match between multiple inputs
            t2 = (col1=[1.2, 2.3, 3.4, 4.5, 5.6],)
            @test_throws ArgumentError Arrow.Table([Arrow.tobuffer(t), Arrow.tobuffer(t2)])

            # test multiple inputs treated as one table
            tbls = collect(Arrow.Stream([Arrow.tobuffer(t), Arrow.tobuffer(t)]))
            @test tbls[1].col1 == tbls[2].col1
            @test tbls[1].col2 == tbls[2].col2

            # schemas must match between multiple inputs
            t2 = (col1=[1.2, 2.3, 3.4, 4.5, 5.6],)
            @test_throws ArgumentError collect(
                Arrow.Stream([Arrow.tobuffer(t), Arrow.tobuffer(t2)]),
            )
        end

        @testset "# 253" begin
            # https://github.com/apache/arrow-julia/issues/253
            @test Arrow.toidict(Pair{String,String}[]) ==
                  Base.ImmutableDict{String,String}()
        end

        @testset "# 232" begin
            # https://github.com/apache/arrow-julia/issues/232
            t = (; x=[Dict(true => 1.32, 1.2 => 0.53495216)])
            @test_throws ArgumentError(
                "`keytype(d)` must be concrete to serialize map-like `d`, but `keytype(d) == Real`",
            ) Arrow.tobuffer(t)
            t = (; x=[Dict(32.0 => true, 1.2 => 0.53495216)])
            @test_throws ArgumentError(
                "`valtype(d)` must be concrete to serialize map-like `d`, but `valtype(d) == Real`",
            ) Arrow.tobuffer(t)
            t = (; x=[Dict(true => 1.32, 1.2 => true)])
            @test_throws ArgumentError(
                "`keytype(d)` must be concrete to serialize map-like `d`, but `keytype(d) == Real`",
            ) Arrow.tobuffer(t)
        end

        @testset "# 214" begin
            # https://github.com/apache/arrow-julia/issues/214
            t1 = (; x=[(Nanosecond(42),)])
            t2 = Arrow.Table(Arrow.tobuffer(t1))
            t3 = Arrow.Table(Arrow.tobuffer(t2))
            @test t3.x == t1.x

            t1 = (; x=[(; a=Nanosecond(i), b=Nanosecond(i + 1)) for i = 1:5])
            t2 = Arrow.Table(Arrow.tobuffer(t1))
            t3 = Arrow.Table(Arrow.tobuffer(t2))
            @test t3.x == t1.x
        end

        @testset "Writer" begin
            io = IOBuffer()
            writer = open(Arrow.Writer, io)
            a = 1:26
            b = 'A':'Z'
            partitionsize = 10
            iter_a = Iterators.partition(a, partitionsize)
            iter_b = Iterators.partition(b, partitionsize)
            for (part_a, part_b) in zip(iter_a, iter_b)
                Arrow.write(writer, (a=part_a, b=part_b))
            end
            close(writer)
            seekstart(io)
            table = Arrow.Table(io)
            @test table.a == collect(a)
            @test table.b == collect(b)
        end

        @testset "# Empty input" begin
            @test Arrow.Table(UInt8[]) isa Arrow.Table
            @test isempty(Tables.rows(Arrow.Table(UInt8[])))
            @test Arrow.Stream(UInt8[]) isa Arrow.Stream
            @test isempty(Tables.partitions(Arrow.Stream(UInt8[])))
        end

        @testset "# 324" begin
            # https://github.com/apache/arrow-julia/issues/324
            @test_throws ArgumentError filter!(x -> x > 1, Arrow.toarrowvector([1, 2, 3]))
        end

        @testset "# 327" begin
            # https://github.com/apache/arrow-julia/issues/327
            zdt =
                ZonedDateTime(DateTime(2020, 11, 1, 6), tz"America/New_York"; from_utc=true)
            arrow_zdt = ArrowTypes.toarrow(zdt)
            zdt_again = ArrowTypes.fromarrow(ZonedDateTime, arrow_zdt)
            @test zdt == zdt_again

            # Check that we still correctly read in old TimeZones
            original_table =
                (; col=[ZonedDateTime(DateTime(1, 2, 3, 4, 5, 6), tz"UTC+3") for _ = 1:5])
            table = Arrow.Table(joinpath(@__DIR__, "old_zdt.arrow"))
            @test original_table.col == table.col
        end

        @testset "# 243" begin
            table = (; col=[(; v=v"1"), (; v=v"2"), missing])
            @test isequal(Arrow.Table(Arrow.tobuffer(table)).col, table.col)
        end

        @testset "# 367" begin
            t = (; x=Union{ZonedDateTime,Missing}[missing])
            a = Arrow.Table(Arrow.tobuffer(t))
            @test Tables.schema(a) == Tables.schema(t)
            @test isequal(a.x, t.x)
        end

        # https://github.com/apache/arrow-julia/issues/414
        df = DataFrame(("$i" => rand(1000) for i = 1:65536)...)
        df_load = Arrow.Table(Arrow.tobuffer(df))
        @test Tables.schema(df) == Tables.schema(df_load)
        for (col1, col2) in zip(Tables.columns(df), Tables.columns(df_load))
            @test col1 == col2
        end

        @testset "# 411" begin
            # Vector{UInt8} are written as List{UInt8} in Arrow
            # Base.CodeUnits are written as Binary
            t = (
                a=[[0x00, 0x01], UInt8[], [0x03]],
                am=[[0x00, 0x01], [0x03], missing],
                b=[b"01", b"", b"3"],
                bm=[b"01", b"3", missing],
                c=["a", "b", "c"],
                cm=["a", "c", missing],
            )
            buf = Arrow.tobuffer(t)
            tt = Arrow.Table(buf)
            @test t.a == tt.a
            @test isequal(t.am, tt.am)
            @test t.b == tt.b
            @test isequal(t.bm, tt.bm)
            @test t.c == tt.c
            @test isequal(t.cm, tt.cm)
            @test Arrow.schema(tt)[].fields[1].type isa Arrow.Flatbuf.List
            @test Arrow.schema(tt)[].fields[3].type isa Arrow.Flatbuf.Binary
            pos = position(buf)
            Arrow.append(buf, tt)
            seekstart(buf)
            buf1 = read(buf, pos)
            buf2 = read(buf)
            t1 = Arrow.Table(buf1)
            t2 = Arrow.Table(buf2)
            @test isequal(t1.a, t2.a)
            @test isequal(t1.am, t2.am)
            @test isequal(t1.b, t2.b)
            @test isequal(t1.bm, t2.bm)
            @test isequal(t1.c, t2.c)
            @test isequal(t1.cm, t2.cm)
        end

        @testset "# 435" begin
            t = Arrow.Table(
                joinpath(dirname(pathof(Arrow)), "../test/java_compress_len_neg_one.arrow"),
            )
            @test length(t) == 15
            @test length(t.isA) == 102
        end

        @testset "# 293" begin
            t = (a=[1, 2, 3], b=[1.0, 2.0, 3.0])
            buf = Arrow.tobuffer(t)
            tbl = Arrow.Table(buf)
            parts = Tables.partitioner((t, t))
            buf2 = Arrow.tobuffer(parts)
            tbl2 = Arrow.Table(buf2)
            for t in Tables.partitions(tbl2)
                @test t.a == tbl.a
                @test t.b == tbl.b
            end
        end

        @testset "# 437" begin
            t = Arrow.Table(
                joinpath(
                    dirname(pathof(Arrow)),
                    "../test/java_compressed_zero_length.arrow",
                ),
            )
            @test length(t) == 2
            @test length(t.name) == 0
        end

        @testset "# 458" begin
            x = (; a=[[[[1]]]])
            buf = Arrow.tobuffer(x)
            t = Arrow.Table(buf)
            @test t.a[1][1][1][1] == 1
        end

        @testset "# 456" begin
            NT = @NamedTuple{x::Int, y::Union{Missing,Int}}
            data = NT[(x=1, y=2), (x=2, y=missing), (x=3, y=4), (x=4, y=5)]
            t = [(a=1, b=view(data, 1:2)), (a=2, b=view(data, 3:4)), missing]
            @test Arrow.toarrowvector(t) isa Arrow.Struct
        end

        # @testset "# 461" begin

        # table = (; v=[v"1", v"2", missing])
        # buf = Arrow.tobuffer(table)
        # table2 = Arrow.Table(buf)
        # @test isequal(table.v, table2.v)

        # end
        if isdefined(ArrowTypes, :StructElement)
            @testset "# 493" begin
                # This test stresses the existence of the mechanism
                # implemented in https://github.com/apache/arrow-julia/pull/493,
                # but doesn't stress the actual use case that motivates
                # that mechanism, simply because it'd be more annoying to
                # write that test; see the PR for details.
                struct Foo493
                    x::Int
                    y::Int
                end
                ArrowTypes.arrowname(::Type{Foo493}) = Symbol("JuliaLang.Foo493")
                ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.Foo493")}, T) = Foo493
                function ArrowTypes.fromarrowstruct(
                    ::Type{Foo493},
                    ::Val{fnames},
                    x...,
                ) where {fnames}
                    nt = NamedTuple{fnames}(x)
                    return Foo493(nt.x + 1, nt.y + 1)
                end
                t = (; f=[Foo493(1, 2), Foo493(3, 4)])
                buf = Arrow.tobuffer(t)
                tbl = Arrow.Table(buf)
                @test tbl.f[1] === Foo493(2, 3)
                @test tbl.f[2] === Foo493(4, 5)
            end
        end

        @testset "# 504" begin
            struct Foo504
                x::Int
            end

            struct Bar504
                a::Foo504
            end

            v = [Bar504(Foo504(i)) for i = 1:3]
            io = IOBuffer()
            Arrow.write(io, v; file=false)
            seekstart(io)
            Arrow.append(io, v) # testing the compatility between the schema of the arrow Table, and the "schema" of v (using the fallback mechanism of Tables.jl)
            seekstart(io)
            t = Arrow.Table(io)
            @test Arrow.Tables.rowcount(t) == 6
        end

        @testset "# 526: Arrow.Time" begin
            tt = testtables[4]
            # just to make sure we're grabbing the correct table
            @test first(tt) == "arrow date/time types"
            tbl = Arrow.Table(Arrow.tobuffer(tt[2]))
            @test tbl.col16[1] == Dates.Time(0, 0, 0)
        end

        @testset "#511: Bug in reading Utf8View data" begin
            t = Arrow.Table(
                joinpath(
                    dirname(pathof(Arrow)),
                    "../test/reject_reason_trimmed.arrow",
                ),
            )
            @test t.reject_reason[end] == "POST_ONLY"
        end
    end # @testset "misc"

    @testset "DataAPI.metadata" begin
        df = DataFrame(a=1, b=2, c=3)
        for i = 1:2
            io = IOBuffer()
            if i == 1 # skip writing metadata in the first iteration
                Arrow.write(io, df)
            else
                Arrow.write(io, df, metadata=metadata(df), colmetadata=colmetadata(df))
            end
            seekstart(io)
            tbl = Arrow.Table(io)

            @test DataAPI.metadatasupport(typeof(tbl)) == (read=true, write=false)
            @test metadata(tbl) == metadata(df)
            @test metadata(tbl; style=true) == metadata(df; style=true)
            @test_throws Exception metadata(tbl, "xyz")
            @test metadata(tbl, "xyz", "something") == "something"
            @test metadata(tbl, "xyz", "something"; style=true) == ("something", :default)
            @test metadatakeys(tbl) == metadatakeys(df)

            @test DataAPI.colmetadatasupport(typeof(tbl)) == (read=true, write=false)
            @test colmetadata(tbl) == colmetadata(df)
            @test colmetadata(tbl; style=true) == colmetadata(df; style=true)
            @test_throws MethodError colmetadata(tbl, "xyz")
            @test_throws KeyError colmetadata(tbl, :xyz)
            @test colmetadata(tbl, :b) == colmetadata(df, :b)
            @test_throws MethodError colmetadata(tbl, :b, "xyz")
            @test colmetadata(tbl, :b, "xyz", "something") == "something"
            @test colmetadata(tbl, :b, "xyz", "something"; style=true) ==
                  ("something", :default)
            @test Set(colmetadatakeys(tbl)) == Set(colmetadatakeys(df))

            # add metadata for the second iteration
            metadata!(df, "tkey", "tvalue")
            metadata!(df, "tkey2", "tvalue2")
            colmetadata!(df, :a, "ackey", "acvalue")
            colmetadata!(df, :a, "ackey2", "acvalue2")
            colmetadata!(df, :c, "cckey", "ccvalue")
        end
    end # @testset "DataAPI.metadata"
end
