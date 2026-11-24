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

module DataFramesTests

using Test, Arrow, DataFrames, DataAPI, Dates
using PooledArrays: PooledArray

@testset "DataFrame workflows" begin
    @test Base.get_extension(Arrow, :ArrowDataFramesExt) !== nothing
    df = DataFrame(
        id=[1, 2, 3],
        x=[1.5, missing, 3.5],
        name=["a", "a longer string than twelve bytes", missing],
        day=[Date(2025, 1, 1), Date(2025, 1, 2), Date(2025, 1, 3)],
        region=PooledArray(["east", "west", "east"]),
        tags=[["a", "b"], String[], ["c"]],
        point=[(x=1, y=2.0), (x=3, y=4.0), (x=5, y=6.0)],
        attrs=[Dict("k" => 1), Dict("j" => 2, "k" => 3), Dict{String,Int}()],
    )
    metadata!(df, "source", "test"; style=:default)
    colmetadata!(df, :id, "unit", "count"; style=:default)
    path = joinpath(mktempdir(), "df.arrow")
    Arrow.write(path, df)

    @testset "a copy equals what was written and owns its columns" begin
        t = Arrow.Table(path)
        copied = DataFrame(t)
        @test isequal(copied, df)
        @test isequal(Arrow.read(path, DataFrame), df)
        @test copied.attrs[2]["k"] == 3 && copied.tags[1] == ["a", "b"]
        copied.id[1] = 10
        @test t.id[1] == 1
        @test metadata(copied, "source") == "test"
        @test colmetadata(copied, :id, "unit") == "count"
        push!(copied.tags[2], "new")
        @test isempty(t.tags[2])
    end

    @testset "views read through DataFrames operations" begin
        views = DataFrame(Arrow.Table(path); copycols=false)
        @test isequal(views, df)
        @test_throws Exception (views.id[1] = 10)
        @test sort(views, :id; rev=true).id == [3, 2, 1]
        @test filter(:id => >(1), views).id == [2, 3]
        grouped = combine(groupby(views, :region), :id => sum => :total)
        @test sort(grouped, :region).total == [4, 2]
        joined = innerjoin(views, DataFrame(id=[2], y=[20]); on=:id)
        @test joined.y == [20] && joined.day == [Date(2025, 1, 2)]
    end

    @testset "a table read back writes back" begin
        t = Arrow.Table(path)
        again = Arrow.Table(take!(Arrow.write(IOBuffer(), DataFrame(t); file=true)))
        @test isequal(DataFrame(again), df)
        rewritten = Arrow.Table(take!(Arrow.write(IOBuffer(), t; file=true)))
        @test isequal(DataFrame(rewritten), df)
        @test DataAPI.metadata(rewritten, "source") == "test"
    end
end

end # module DataFramesTests
