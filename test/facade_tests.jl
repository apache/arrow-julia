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

    @testset "partition drift is refused, not misbound" begin
        io = IOBuffer()
        @test_throws ArgumentError Arrow.write(io, Tables.partitioner([
            (left=Int64[1], right=Int64[10]),
            (right=Int64[20], left=Int64[2])]); file=false)
        @test_throws ArgumentError Arrow.write(io, Tables.partitioner([
            (x=Int64[1],), (y=Int64[2],)]); file=false)
        @test_throws ArgumentError Arrow.write(io, Tables.partitioner([
            (x=Int64[1],), (x=Int32[2],)]); file=false)
    end

    @testset "schema is the authority for facade eltypes" begin
        io = IOBuffer()
        Arrow.write(io, (s=Union{Missing,String}["a", "b"],
            m=Union{Missing,String}[missing, missing],); file=false)
        t = Arrow.Table(take!(io))
        @test eltype(t.s) == Union{Missing,String}
        @test eltype(t.m) == Union{Missing,String}
        # all-missing columns round-trip as their DECLARED type
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)
        t2 = Arrow.Table(take!(io2))
        @test eltype(t2.m) == Union{Missing,String}
        @test isequal(t2.m, [missing, missing])
        # zero-row typed columns, including via a scan with no matches
        io3 = IOBuffer()
        Arrow.write(io3, (x=Int64[1],); file=false)
        b3 = take!(io3)
        t3 = Arrow.Table(b3; scan=Tables.Scan(filter=Tables.coleq(
            Tables.col(:x), 99)))
        @test eltype(t3.x) == Int64 && isempty(t3.x)
    end

    @testset "temporal scans agree across formats and renames" begin
        data = (x=Int64[1, 2, 3],
            date=[Date(2024, 1, 1), Date(2024, 1, 2), Date(2024, 1, 3)],
            stamp=[DateTime(1970, 1, 1), DateTime(1970, 1, 1, 0, 0, 2),
                DateTime(2001, 9, 9)])
        fio = IOBuffer(); Arrow.write(fio, data)
        sio = IOBuffer(); Arrow.write(sio, data; file=false)
        scan = Tables.Scan(filter=Tables.coleq(Tables.col(:date),
            Date(2024, 1, 3)))
        want = Tables.finish(data, scan)
        for bytes in (take!(fio), take!(sio))
            got = Arrow.Table(bytes; scan=scan)
            @test got.x == want.x
            @test got.date == want.date && eltype(got.date) <: Union{Missing,Date}
            @test got.stamp == want.stamp
        end
        # renamed temporal output still converts
        rio = IOBuffer(); Arrow.write(rio, data)
        tr = Arrow.Table(take!(rio); scan=Tables.Scan(
            select=(:date => :d,), limit=1))
        @test tr.d == [Date(2024, 1, 1)]
    end

    @testset "ranged reads carry the schema" begin
        io = IOBuffer()
        Arrow.write(io, (stamp=[DateTime(2020, 5, 5)],);
            metadata=Dict("origin" => "ranged"))
        fb = take!(io)
        src = Arrow.RangedSource(fb)
        t = Arrow.Table(src)
        @test t.stamp == [DateTime(2020, 5, 5)]
        @test eltype(t.stamp) == Union{Missing,DateTime} || eltype(t.stamp) == DateTime
        @test DataAPI.metadata(t, "origin") == "ranged"
    end

    @testset "facade rewrite preserves the retained schema" begin
        # Build exotic units through the core writer, then facade-read and
        # facade-rewrite; the logical schema must not drift.
        micros = Union{Missing,Int64}[1, 1001]
        f, d = Arrow.AC.fromjulia("us", micros)
        t_us = Arrow.AC.TimestampType(Arrow.AC.MICROSECOND, nothing)
        d_us = Arrow.AC._arraydata(t_us, d.len, d.buffers, 0,
            Arrow.AC.ArrayData[], nothing, d.owner, Arrow.AC.nullcount(d))
        f_us = Arrow.AC.Field("us", t_us; nullable=true)
        sch = Arrow.AC.Schema([f_us]; metadata=["k" => "v"])
        bytes = Arrow.writestream(sch,
            [Arrow.AC.RecordBatch(sch, [d_us], 2)])
        t = Arrow.Table(bytes)
        @test t.us == [1, 1001]          # sub-ms stays raw, exact
        io = IOBuffer()
        Arrow.write(io, t; file=false)
        rt = Arrow.Table(take!(io))
        rsch = getfield(rt, :schema)
        @test rsch.fields[1].type isa Arrow.AC.TimestampType
        @test rsch.fields[1].type.unit == Arrow.AC.MICROSECOND
        @test DataAPI.metadata(rt, "k") == "v"   # schema metadata carried
        @test rt.us == [1, 1001]
        # dictionary columns round-trip as dictionaries, multi-partition,
        # file format (one shared pool, no replacement refusal)
        io2 = IOBuffer()
        Arrow.write(io2, Tables.partitioner([
            (d=Arrow.DictEncode(["a", "b"]),),
            (d=Arrow.DictEncode(["b", "c"]),)]); file=true)
        t2 = Arrow.Table(take!(io2))
        @test t2.d == ["a", "b", "b", "c"]
        sch2 = getfield(t2, :schema)
        @test sch2.fields[1].type isa Arrow.AC.DictionaryType
        io3 = IOBuffer()
        Arrow.write(io3, t2; file=true)
        t3 = Arrow.Table(take!(io3))
        @test getfield(t3, :schema).fields[1].type isa Arrow.AC.DictionaryType
        @test t3.d == ["a", "b", "b", "c"]
    end

    @testset "zero-column row counts survive" begin
        # A zero-column three-row batch built at the core level: the facade
        # read must preserve the count, and a facade round-trip must carry
        # it back out (Table knows its row count even with no columns).
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        bytes = Arrow.writestream(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        t = Arrow.Table(bytes)
        @test Tables.rowcount(t) == 3
        @test isempty(Tables.columnnames(t))
        io = IOBuffer()
        Arrow.write(io, t; file=false)
        t2 = Arrow.Table(take!(io))
        @test Tables.rowcount(t2) == 3
    end

    @testset "DataAPI defaults and selectors" begin
        io = IOBuffer()
        Arrow.write(io, (x=Int64[1],); file=false,
            colmetadata=Dict(:x => Dict("u" => "1")))
        t = Arrow.Table(take!(io))
        @test DataAPI.metadata(t, "absent", "fallback") == "fallback"
        @test DataAPI.colmetadata(t, 1, "u") == "1"
        @test DataAPI.colmetadata(t, :x, "nope", :d) == :d
        @test collect(first.(DataAPI.colmetadatakeys(t))) == [:x]
    end

    @testset "temporal scans preserve cross-type predicate semantics" begin
        data = (x=Int64[1, 2, 3],
            d32=[Date(1970, 1, 1), Date(1970, 1, 2), Date(1970, 1, 3)],
            ts=[DateTime(2020, 1, 1), DateTime(2020, 1, 2), DateTime(2020, 1, 3)])
        fio = IOBuffer(); Arrow.write(fio, data); fb = take!(fio)
        sio = IOBuffer(); Arrow.write(sio, data; file=false); sb = take!(sio)
        cases = [
            # Date32 vs midnight DateTime: cross-type equality holds
            Tables.Scan(select=(:x,), filter=Tables.coleq(Tables.col(:d32),
                DateTime(1970, 1, 2))),
            # Timestamp vs Date
            Tables.Scan(select=(:x,), filter=Tables.coleq(Tables.col(:ts),
                Date(2020, 1, 2))),
            # raw integer vs a temporal column: never equal in public domain
            Tables.Scan(select=(:x,), filter=Tables.coleq(Tables.col(:d32), 1)),
            # non-midnight DateTime vs Date32: no exact representation
            Tables.Scan(select=(:x,), filter=Tables.coleq(Tables.col(:d32),
                DateTime(1970, 1, 2, 12))),
        ]
        for scan in cases
            want = Tables.finish(data, scan)
            for bytes in (fb, sb)
                got = Arrow.Table(bytes; scan=scan)
                @test isequal(got.x, want.x)
            end
        end
    end

    @testset "retained rewrite is schema identity" begin
        # Non-nullable temporal descriptors stay non-nullable; Date64 works.
        vals = Int64[0, 86_400_000]
        f64, _ = Arrow.AC.fromjulia("d", vals)
        t64 = Arrow.AC.DateType(Arrow.AC.MILLISECOND_DATE)
        d64 = Arrow.AC._arraydata(t64, 2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)], 0,
            Arrow.AC.ArrayData[], nothing, nothing, 0)
        fld = Arrow.AC.Field("d", t64; nullable=false)
        sch = Arrow.AC.Schema([fld])
        bytes = Arrow.writestream(sch,
            [Arrow.AC.RecordBatch(sch, [d64], 2)])
        t = Arrow.Table(bytes)
        @test t.d == [DateTime(1970, 1, 1), DateTime(1970, 1, 2)]
        io = IOBuffer(); Arrow.write(io, t; file=false)
        rt = getfield(Arrow.Table(take!(io)), :schema)
        @test rt.fields[1].type isa Arrow.AC.DateType
        @test rt.fields[1].type.unit == Arrow.AC.MILLISECOND_DATE
        @test rt.fields[1].nullable == false
        # Retained dictionary identity: index width and ordered survive.
        pool = ["a", "b"]
        pf, pd = Arrow.AC.fromjulia("d", pool)
        dt = Arrow.AC.DictionaryType(Arrow.AC.IntType(8, true), pf.type, true)
        idx = Int8[0, 1, 0]
        dd = Arrow.AC.ArrayData(dt, 3,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(idx)];
            dictionary=pd, nullcount=0)
        df = Arrow.AC.Field("d", dt; nullable=false)
        dsch = Arrow.AC.Schema([df])
        dbytes = Arrow.writestream(dsch,
            [Arrow.AC.RecordBatch(dsch, [dd], 3)])
        dt2 = Arrow.Table(dbytes)
        io2 = IOBuffer(); Arrow.write(io2, dt2; file=false)
        rsch = getfield(Arrow.Table(take!(io2)), :schema)
        rdt = rsch.fields[1].type
        @test rdt isa Arrow.AC.DictionaryType
        @test rdt.indextype.bits == 8 && rdt.ordered == true
        @test rsch.fields[1].nullable == false
    end

    @testset "replaced facade columns are refused" begin
        io = IOBuffer()
        Arrow.write(io, (d=[Date(2024, 1, 1)], p=Arrow.DictEncode(["x"])))
        t = Arrow.Table(take!(io))
        broken = Arrow.Table(getfield(t, :names),
            AbstractVector[Int64[100], Int64[7]], getfield(t, :lookup),
            getfield(t, :schema), Arrow.AC.OwnerRegion[], 1)
        io2 = IOBuffer()
        @test_throws ArgumentError Arrow.write(io2, broken; file=false)
    end

    @testset "type overrides and renamed schemas" begin
        io = IOBuffer()
        Arrow.write(io, (x=Union{Missing,Int64}[1, missing],
            d=[Date(2024, 1, 1), Date(2024, 1, 2)]))
        fb = take!(io)
        t = Arrow.Table(fb; scan=Tables.Scan(
            select=(:x => Union{Missing,Float64}, :d => Date)))
        @test isequal(t.x, Union{Missing,Float64}[1.0, missing])
        @test t.d == [Date(2024, 1, 1), Date(2024, 1, 2)]
        # a renamed output binds ITS OWN field in the stored schema
        tr = Arrow.Table(fb; scan=Tables.Scan(select=(:d => :when,)))
        rsch = getfield(tr, :schema)
        @test length(rsch.fields) == 1
        @test rsch.fields[1].name == "when"
        @test rsch.fields[1].type isa Arrow.AC.DateType
        io3 = IOBuffer()
        Arrow.write(io3, tr; file=false)
        back = getfield(Arrow.Table(take!(io3)), :schema)
        @test back.fields[1].type isa Arrow.AC.DateType
    end

    @testset "empty projections keep row counts" begin
        io = IOBuffer()
        Arrow.write(io, (x=collect(Int64, 1:5),); file=false)
        t = Arrow.Table(take!(io); scan=Tables.Scan(select=(),
            filter=Tables.col(:x) > 2))
        @test isempty(Tables.columnnames(t))
        @test Tables.rowcount(t) == 3
    end

    @testset "DataAPI missing columns are errors" begin
        io = IOBuffer()
        Arrow.write(io, (x=Int64[1],); file=false)
        t = Arrow.Table(take!(io))
        @test_throws ArgumentError DataAPI.colmetadatakeys(t, :nope)
        @test_throws ArgumentError DataAPI.colmetadata(t, :nope, "k")
    end

    @testset "errors are clean" begin
        @test_throws ArgumentError Arrow.write(IOBuffer(),
            Tables.partitioner(NamedTuple[]))
        @test_throws ArgumentError Arrow.write(IOBuffer(),
            (st=[(a=1,), missing],))
    end
end

end # module FacadeTests
