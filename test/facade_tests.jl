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

# The public facade: Arrow.Table / Arrow.Stream / Arrow.write. Interface
# tests only — layout/validation depth lives in the core tests, batteries,
# and the conformance suites.

module FacadeTests

using Test
using Tables
using Dates
import DataAPI
using Arrow

const MIXED = (
    ints = Int64[1, 2, 3, 4],
    floats = [1.5, missing, 3.5, 4.5],
    strs = ["a", "bb", missing, "dddd"],
    dates = [Date(2024, 1, 1), Date(2025, 6, 15), missing, Date(1969, 12, 31)],
    stamps = [DateTime(2024, 1, 1, 12, 30), missing, DateTime(2000, 1, 1),
        DateTime(1970, 1, 1)],
    clocks = [Time(12, 30, 15), Time(0), missing, Time(23, 59, 59)],
    spans = [Millisecond(250), missing, Millisecond(0), Millisecond(-10)],
    lists = [[1, 2], Int64[], [3], missing],
    pooled = Arrow.DictEncode(["lo", "hi", "lo", missing]),
)

function assert_mixed(t)
    @test t.ints == MIXED.ints
    @test isequal(t.floats, MIXED.floats)
    @test isequal(t.strs, MIXED.strs)
    @test isequal(t.dates, MIXED.dates)
    @test isequal(t.stamps, MIXED.stamps)
    @test isequal(t.clocks, MIXED.clocks)
    @test isequal(t.spans, MIXED.spans)
    @test isequal(t.lists, [Any[1, 2], Any[], Any[3], missing])
    @test isequal(t.pooled, ["lo", "hi", "lo", missing])
end

@testset "Arrow facade" begin
    @testset "file round-trip with metadata" begin
        path = tempname()
        Arrow.write(path, MIXED; metadata=Dict("who" => "facade"),
            colmetadata=Dict(:ints => Dict("unit" => "count")))
        t = Arrow.Table(path)
        assert_mixed(t)
        @test Tables.istable(typeof(t))
        @test Tables.columnnames(t) == collect(keys(MIXED))
        @test DataAPI.metadata(t, "who") == "facade"
        @test collect(DataAPI.metadatakeys(t)) == ["who"]
        @test DataAPI.colmetadata(t, :ints, "unit") == "count"
        @test_throws KeyError DataAPI.metadata(t, "absent")
        # Materialized columns survive deterministic release; the mapping
        # is gone, so the file is deletable everywhere (the Windows case).
        Arrow.close!(t)
        @test t.ints == MIXED.ints
        rm(path)
    end

    @testset "stream format, IO sinks and sources" begin
        io = IOBuffer()
        Arrow.write(io, MIXED; file=false)
        t = Arrow.Table(seekstart(io))
        assert_mixed(t)
    end

    @testset "compression kwargs" begin
        for compress in (:lz4, :zstd)
            io = IOBuffer()
            Arrow.write(io, MIXED; compress=compress)
            assert_mixed(Arrow.Table(take!(io)))
        end
    end

    @testset "partitions become record batches; Stream iterates them" begin
        io = IOBuffer()
        Arrow.write(io, Tables.partitioner([(x=Int64[1, 2],),
            (x=Int64[3, 4],)]); file=false)
        bytes = take!(io)
        s = Arrow.Stream(bytes)
        @test length(s) == 2
        parts = collect(s)
        @test parts[1].x == [1, 2] && parts[2].x == [3, 4]
        # Stream is a Tables.partitions source: writing it re-partitions.
        io2 = IOBuffer()
        Arrow.write(io2, Arrow.Stream(bytes); file=false)
        @test length(Arrow.Stream(take!(io2))) == 2
        # Whole-table read concatenates.
        @test Arrow.Table(bytes).x == [1, 2, 3, 4]
    end

    @testset "scan pushdown through Arrow.Table" begin
        io = IOBuffer()
        Arrow.write(io, (x=collect(Int64, 1:100), y=string.(1:100)))
        fb = take!(io)
        t = Arrow.Table(fb; scan=Tables.Scan(select=(:y,),
            filter=Tables.coleq(Tables.col(:x), 42)))
        @test Tables.columnnames(t) == [:y]
        @test t.y == ["42"]
        # Stream-format input takes the post-decode path, same result.
        io2 = IOBuffer()
        Arrow.write(io2, (x=collect(Int64, 1:100), y=string.(1:100));
            file=false)
        t2 = Arrow.Table(take!(io2); scan=Tables.Scan(select=(:y,),
            filter=Tables.coleq(Tables.col(:x), 42)))
        @test t2.y == ["42"]
        # Renames land as output names.
        t3 = Arrow.Table(fb; scan=Tables.Scan(select=(:x => :renamed,),
            limit=2))
        @test Tables.columnnames(t3) == [:renamed]
        @test t3.renamed == [1, 2]
    end

    @testset "ranged source fetches only what the scan needs" begin
        io = IOBuffer()
        Arrow.write(io, Tables.partitioner([
            (a=collect(Int64, 1:1000), b=[string("v", i) for i = 1:1000]),
            (a=collect(Int64, 1001:2000), b=[string("v", i) for i = 1001:2000])]))
        fb = take!(io)
        fetched = Ref(Int64(0))
        src = Arrow.RangedSource(
            (off, len) -> (fetched[] += len; fb[(off + 1):(off + len)]),
            Int64(length(fb)))
        rf = Arrow.RangedFile(src; tailbytes=1024, coalesce_gap=0)
        t = Arrow.Table(rf; scan=Tables.Scan(select=(:b,), limit=3, offset=1500))
        @test t.b == ["v1501", "v1502", "v1503"]
        # The first batch is skipped entirely and column :a is never fetched.
        @test fetched[] < length(fb) ÷ 2
    end

    @testset "mmap path and close!" begin
        path = tempname()
        Arrow.write(path, (x=collect(Int64, 1:10),))
        t = Arrow.Table(path)   # mmap by default for ARROW1 files
        @test t.x == 1:10
        Arrow.close!(t)
        rm(path)                # deletable post-close on every platform
        @test t.x == 1:10
    end

    @testset "substrings and generic vectors convert" begin
        io = IOBuffer()
        subs = split("alpha,beta,gamma", ",")
        Arrow.write(io, (s=subs, r=1:3); file=false)
        t = Arrow.Table(take!(io))
        @test t.s == ["alpha", "beta", "gamma"]
        @test t.r == [1, 2, 3]
    end

    @testset "structs of named tuples" begin
        io = IOBuffer()
        Arrow.write(io, (st=[(a=1, b="x"), (a=2, b="y")],); file=false)
        t = Arrow.Table(take!(io))
        @test t.st == [["a" => 1, "b" => "x"], ["a" => 2, "b" => "y"]]
    end

    @testset "errors are clean" begin
        @test_throws ArgumentError Arrow.write(IOBuffer(),
            Tables.partitioner(NamedTuple[]))
        @test_throws ArgumentError Arrow.write(IOBuffer(),
            (st=[(a=1,), missing],))
    end
end

end # module FacadeTests
