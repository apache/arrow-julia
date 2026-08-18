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

    @testset "lowering honors the facade comparison domain" begin
        # Sub-ms timestamps materialize as raw Int64: a DateTime literal is
        # never equal in public, and integers compare directly.
        us = Union{Missing,Int64}[1_000_000, 2_000_000]
        f, d = Arrow.AC.fromjulia("us", us)
        t_us = Arrow.AC.TimestampType(Arrow.AC.MICROSECOND, nothing)
        d_us = Arrow.AC._arraydata(t_us, 2, d.buffers, 0,
            Arrow.AC.ArrayData[], nothing, nothing, 2 - 2)
        sch = Arrow.AC.Schema([Arrow.AC.Field("us", t_us; nullable=true)])
        bytes = Arrow.writestream(sch,
            [Arrow.AC.RecordBatch(sch, [d_us], 2)])
        data = (us=us,)
        for scan in (
            Tables.Scan(filter=Tables.coleq(Tables.col(:us),
                DateTime(1970, 1, 1, 0, 0, 1))),
            Tables.Scan(filter=Tables.coleq(Tables.col(:us), 2_000_000)))
            want = Tables.finish(data, scan)
            got = Arrow.Table(bytes; scan=scan)
            @test isequal(got.us, want.us)
        end
        # Out-of-range and cross-Period literals fall back, matching the
        # authority instead of throwing.
        pdata = (d=[Date(2024, 1, 1)], s=[Second(30)])
        io = IOBuffer(); Arrow.write(io, pdata); pb = take!(io)
        for scan in (
            Tables.Scan(filter=Tables.coleq(Tables.col(:d),
                Date(6_000_000, 1, 1))),
            Tables.Scan(filter=Tables.coleq(Tables.col(:s), Month(1))))
            want = Tables.finish(pdata, scan)
            got = Arrow.Table(pb; scan=scan)
            @test Tables.rowcount(got) == Tables.rowcount(Tables.columns(want))
        end
    end

    @testset "overrides preserve missing and re-infer on rewrite" begin
        io = IOBuffer()
        Arrow.write(io, (x=Union{Missing,Int64}[1, missing],))
        fb = take!(io)
        t = Arrow.Table(fb; scan=Tables.Scan(select=(:x => Float64,)))
        @test isequal(t.x, Union{Missing,Float64}[1.0, missing])
        # rewrite after an override re-infers the column cleanly
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)
        t2 = Arrow.Table(take!(io2))
        @test isequal(t2.x, Union{Missing,Float64}[1.0, missing])
        @test getfield(t2, :schema).fields[1].type isa Arrow.AC.FloatType
    end

    @testset "zero-field counts across all paths" begin
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        bytes = Arrow.writefile(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        for source in (bytes, Arrow.RangedSource(bytes))
            t = Arrow.Table(source; scan=Tables.Scan())
            @test Tables.rowcount(t) == 3
            tw = Arrow.Table(source; scan=Tables.Scan(limit=1, offset=1))
            @test Tables.rowcount(tw) == 1
        end
    end

    @testset "override subsumption is a no-op; zero-field filters count" begin
        io = IOBuffer()
        Arrow.write(io, (x=Union{Missing,Int64}[1, 2],
            s=Union{Missing,String}["a", "b"]))
        fb = take!(io)
        # nullable source, no observed missing: supertype/no-op overrides
        # keep the DECLARED element type, exactly like Tables.finish.
        t = Arrow.Table(fb; scan=Tables.Scan(select=(
            :x => Int64, :s => AbstractString)))
        @test eltype(t.x) == Union{Missing,Int64}
        @test eltype(t.s) == Union{Missing,String}
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)   # rewrites cleanly, metadata intact
        @test isequal(Arrow.Table(take!(io2)).x, [1, 2])
        # zero-field: filters and validation apply
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        tz = Arrow.Table(zb; scan=Tables.Scan(
            filter=Tables.coleq(Tables.col(:nope), 1), validate=false))
        @test Tables.rowcount(tz) == 0
        @test_throws ArgumentError Arrow.Table(zb; scan=Tables.Scan(
            filter=Tables.coleq(Tables.col(:nope), 1)))
        # ranged zero-field honors RangedFile limits
        rfz = Arrow.RangedFile(Arrow.RangedSource(zb);
            limits=Arrow.Limits(max_array_length=2))
        @test_throws Arrow.AC.ValidationError Arrow.Table(rfz)
    end

    @testset "replaced columns are refused before value access" begin
        io = IOBuffer()
        Arrow.write(io, (n=Int64[1], s=["x"], p=Arrow.DictEncode(["x"])))
        t = Arrow.Table(take!(io))
        for (col, bad) in ((:n, ["oops"]), (:s, Int64[1]), (:p, Int64[7]))
            cols = AbstractVector[c for c in getfield(t, :columns)]
            cols[getfield(t, :lookup)[col]] = bad
            broken = Arrow.Table(getfield(t, :names), cols,
                getfield(t, :lookup), getfield(t, :schema),
                Arrow.AC.OwnerRegion[], 1)
            io2 = IOBuffer()
            @test_throws ArgumentError Arrow.write(io2, broken; file=false)
        end
        # missing into a non-nullable retained dictionary is refused
        pool = ["a"]
        pf, pd = Arrow.AC.fromjulia("d", pool)
        dt = Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true), pf.type, false)
        dd = Arrow.AC.ArrayData(dt, 1,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0])];
            dictionary=pd, nullcount=0)
        df = Arrow.AC.Field("d", dt; nullable=false)
        dsch = Arrow.AC.Schema([df])
        dbytes = Arrow.writestream(dsch,
            [Arrow.AC.RecordBatch(dsch, [dd], 1)])
        td = Arrow.Table(dbytes)
        cols = AbstractVector[Union{Missing,String}["a", missing][1:1]]
        cols[1] = Union{Missing,String}[missing]
        brokend = Arrow.Table(getfield(td, :names), cols,
            getfield(td, :lookup), getfield(td, :schema),
            Arrow.AC.OwnerRegion[], 1)
        io3 = IOBuffer()
        @test_throws ArgumentError Arrow.write(io3, brokend; file=false)
    end

    @testset "override conversions follow the authority exactly" begin
        io = IOBuffer()
        Arrow.write(io, (a=Int64[1, 2], b=Union{Missing,Int64}[1, 2]))
        fb = take!(io)
        # Real conversions: requested type exact, missing only when observed.
        t = Arrow.Table(fb; scan=Tables.Scan(select=(
            :a => Union{Missing,Float64}, :b => Float64)))
        @test eltype(t.a) == Union{Missing,Float64}
        @test eltype(t.b) == Float64
        # Zero-field: true-valued and unmatched-reference filters keep rows.
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        t2 = Arrow.Table(zb; scan=Tables.Scan(
            filter=Tables.isnull(Tables.col(:gone)), validate=false))
        @test Tables.rowcount(t2) == 3
        t3 = Arrow.Table(zb; scan=Tables.Scan(
            filter=Tables.isnull(Tables.col(:gone)), validate=false,
            limit=1, offset=1))
        @test Tables.rowcount(t3) == 1
        # List => Vector is a no-op: values, retained field, and metadata
        # all survive.
        io4 = IOBuffer()
        Arrow.write(io4, (l=[[1, 2], [3]],); file=false,
            colmetadata=Dict(:l => Dict("k" => "v")))
        lb = take!(io4)
        t4 = Arrow.Table(lb; scan=Tables.Scan(select=(:l => Vector,)))
        @test isequal(t4.l, [Any[1, 2], Any[3]])
        rsch = getfield(t4, :schema)
        @test length(rsch.fields) == 1
        @test rsch.fields[1].type isa Arrow.AC.ListType
        @test DataAPI.colmetadata(t4, :l, "k") == "v"
    end

    @testset "zero-field scans hold at the Tables.scan layer too" begin
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        af = Arrow.readfile(zb)
        for (scan, want) in (
            (Tables.Scan(filter=Tables.AlwaysTrue()), 3),
            (Tables.Scan(filter=Tables.AlwaysTrue(), limit=1, offset=1), 1),
            (Tables.Scan(filter=Tables.isnull(Tables.col(:gone)),
                validate=false), 3),
            (Tables.Scan(filter=Tables.AlwaysFalse()), 0))
            got = Tables.scan(af, scan)
            @test Tables.rowcount(Tables.columns(got)) == want
            rgot = Tables.scan(Arrow.RangedFile(Arrow.RangedSource(zb)), scan)
            @test Tables.rowcount(Tables.columns(rgot)) == want
        end
        # The facade path allocates nothing proportional to a hostile count:
        # a tiny file claiming a million rows answers limit=1 instantly.
        big = Arrow.writefile(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 1_000_000)])
        stats = @timed Arrow.Table(big; scan=Tables.Scan(
            filter=Tables.AlwaysTrue(), limit=1))
        @test Tables.rowcount(stats.value) == 1
        @test stats.bytes < 1_000_000
        # The metadata-only ranged read keeps the reader trust boundary: a
        # corrupted continuation prefix rejects exactly as the full reader
        # rejects it.
        bad = copy(zb)
        bad[65:68] .= 0x00
        @test_throws Arrow.AC.ValidationError Arrow.readfile(copy(bad))
        @test_throws Arrow.AC.ValidationError Tables.scan(
            Arrow.RangedFile(Arrow.RangedSource(copy(bad))), Tables.Scan())
        # Header reads share ONE cumulative budget, as Limits documents:
        # many tiny batches refuse under a bound one batch fits.
        many = Arrow.writefile(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 1) for _ = 1:200])
        tight = Arrow.Limits(max_total_allocated_bytes=6000)
        @test_throws Arrow.AllocationLimitError Tables.scan(
            Arrow.readfile(copy(many); limits=tight), Tables.Scan())
        @test_throws Arrow.AllocationLimitError Tables.scan(
            Arrow.RangedFile(Arrow.RangedSource(copy(many)); limits=tight),
            Tables.Scan())
        # A zero-field schema declares no dictionary ids: a footer listing a
        # well-framed dictionary block is orphaned, and the metadata-only
        # ranged read rejects it exactly as the full reader does.
        f0 = Arrow.readfile(copy(zb))
        (roff, rmetalen, rbodylen) = f0.recordblocks[1]
        recbytes = zb[Int(roff)+1:Int(roff + rmetalen + rbodylen)]
        dataend = Int(roff + rmetalen + rbodylen)
        doctored = copy(zb[1:dataend])
        append!(doctored, recbytes)
        append!(doctored,
            reinterpret(UInt8, UInt32[Arrow.CONTINUATION, UInt32(0)]))
        fbb = Arrow.FB.Builder(1024)
        schoff = Arrow._metaschema!(fbb, sch,
            Base.IdDict{Arrow.AC.Field,Int64}(), Int64[])
        Arrow.Meta.footerStartDictionariesVector(fbb, 1)
        Arrow.Meta.createBlock(fbb, Int64(dataend), Int32(rmetalen), rbodylen)
        dictvec = Arrow.FB.endvector!(fbb, 1)
        Arrow.Meta.footerStartRecordBatchesVector(fbb, 1)
        Arrow.Meta.createBlock(fbb, roff, Int32(rmetalen), rbodylen)
        recordvec = Arrow.FB.endvector!(fbb, 1)
        Arrow.Meta.footerStart(fbb)
        Arrow.Meta.footerAddVersion(fbb, Arrow.Meta.MetadataVersion.V5)
        Arrow.Meta.footerAddSchema(fbb, schoff)
        Arrow.Meta.footerAddDictionaries(fbb, dictvec)
        Arrow.Meta.footerAddRecordBatches(fbb, recordvec)
        Arrow.FB.finish!(fbb, Arrow.Meta.footerEnd(fbb))
        ftr = collect(Arrow.FB.finishedbytes(fbb))
        append!(doctored, ftr)
        append!(doctored, reinterpret(UInt8, Int32[Int32(length(ftr))]))
        append!(doctored, Arrow.FILE_MAGIC)
        @test_throws Arrow.AC.ValidationError Arrow.readfile(copy(doctored))
        @test_throws Arrow.AC.ValidationError Tables.scan(
            Arrow.RangedFile(Arrow.RangedSource(copy(doctored))),
            Tables.Scan())
        # Structural binding is unconditional: an unsupported predicate node
        # rejects even with validate=false, on every facade path.
        zs = Arrow.writestream(sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        for source in (zb, zs, Arrow.RangedSource(zb))
            @test_throws ArgumentError Arrow.Table(source;
                scan=Tables.Scan(filter=Tables.OpNode(:custom, Any[]),
                    validate=false))
        end
    end

    @testset "list columns rewrite after materialization" begin
        io = IOBuffer()
        Arrow.write(io, (l=[[1, 2], Int64[], [3]],); file=false)
        t = Arrow.Table(take!(io))
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)
        t2 = Arrow.Table(take!(io2))
        @test isequal(t2.l, [Any[1, 2], Any[], Any[3]])
        # empty list column with a retained field keeps descriptor+metadata
        io3 = IOBuffer()
        Arrow.write(io3, (l=Vector{Int64}[],); file=false,
            colmetadata=Dict(:l => Dict("k" => "v")))
        lb = take!(io3)
        t3 = Arrow.Table(lb; scan=Tables.Scan(select=(:l => Vector,)))
        rsch = getfield(t3, :schema)
        @test length(rsch.fields) == 1
        @test rsch.fields[1].type isa Arrow.AC.ListType
        @test DataAPI.colmetadata(t3, :l, "k") == "v"
        # The full transition matrix: empty and nonempty list facades from
        # every input path rewrite cleanly to both output formats — the
        # retained child descriptor supplies the element type observation
        # cannot (zero-row, all-empty-rows, and nested shapes included).
        for rows in (Vector{Int64}[], [[1, 2], Int64[], [3]],
            [Int64[], Int64[]], [[Int64[1, 2]], [Int64[]]])
            iof = IOBuffer(); Arrow.write(iof, (l=rows,); file=true)
            fbb = take!(iof)
            ios = IOBuffer(); Arrow.write(ios, (l=rows,); file=false)
            sbb = take!(ios)
            for src in (Arrow.Table(fbb), Arrow.Table(sbb),
                Arrow.Table(Arrow.RangedSource(fbb)))
                for file in (true, false)
                    out = IOBuffer()
                    Arrow.write(out, src; file=file)
                    back = Arrow.Table(take!(out))
                    @test isequal(collect(Any, back.l), collect(Any, rows))
                    bsch = getfield(back, :schema)
                    @test bsch.fields[1].type isa Arrow.AC.ListType
                end
            end
        end
        # An EMPTY real conversion drops the stale source descriptor (the
        # declared facade type decides, exactly as a nonempty column would)
        # and the rewrite re-infers from the converted values.
        io5 = IOBuffer()
        Arrow.write(io5, (a=Int64[],); file=false)
        eb = take!(io5)
        tec = Arrow.Table(eb; scan=Tables.Scan(select=(:a => Float64,)))
        @test eltype(Tables.getcolumn(tec, :a)) === Float64
        @test isempty(getfield(tec, :schema).fields)
        out5 = IOBuffer()
        Arrow.write(out5, tec; file=true)
        @test eltype(Arrow.Table(take!(out5)).a) === Float64
        tes = Arrow.Table(lb; scan=Tables.Scan(select=(:l => String,)))
        @test isempty(getfield(tes, :schema).fields)
        # Retained identity is RECURSIVE: names, nullability, metadata, and
        # list WIDTH survive a rewrite at every level (the natural builder
        # only emits small lists — imposition rebuilds retained large
        # offsets), for row-bearing and zero-row columns alike.
        leafd = Arrow.AC.ArrayData(Arrow.AC.IntType(64, true), 3,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[10, 20, 30])])
        innerd = Arrow.AC.ArrayData(Arrow.AC.ListType(false), 2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0, 2, 3])];
            children=[leafd])
        outerd = Arrow.AC.ArrayData(Arrow.AC.ListType(true), 2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[0, 1, 2])];
            children=[innerd])
        leaff = Arrow.AC.Field("item", Arrow.AC.IntType(64, true);
            nullable=false)
        innerf = Arrow.AC.Field("inner", Arrow.AC.ListType(false);
            nullable=true, metadata=["ik" => "iv"], children=[leaff])
        outerf = Arrow.AC.Field("l", Arrow.AC.ListType(true); nullable=false,
            metadata=["ok" => "ov"], children=[innerf])
        nsch = Arrow.AC.Schema([outerf])
        for nrows in (2, 0)
            data = nrows == 0 ? Arrow.AC.ArrayData(Arrow.AC.ListType(true),
                0, [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[0])];
                children=[Arrow.AC.ArrayData(Arrow.AC.ListType(false), 0,
                    [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0])];
                    children=[Arrow.AC.ArrayData(Arrow.AC.IntType(64, true),
                        0, [Arrow.AC.BufferSlice(),
                            Arrow.AC._databuffer(Int64[])])])]) : outerd
            nb = Arrow.writestream(nsch,
                [Arrow.AC.RecordBatch(nsch, Arrow.AC.ArrayData[data], nrows)])
            tsrc = Arrow.Table(nb)
            outn = IOBuffer()
            Arrow.write(outn, tsrc; file=false)
            tback = Arrow.Table(take!(outn))
            @test isequal(tback.l, tsrc.l)
            fb1 = getfield(tback, :schema).fields[1]
            @test fb1.type == Arrow.AC.ListType(true)
            @test fb1.metadata !== nothing && ("ok" => "ov") in fb1.metadata
            fi = fb1.children[1]
            @test fi.name == "inner" && fi.type == Arrow.AC.ListType(false)
            @test fi.nullable
            @test fi.metadata !== nothing && ("ik" => "iv") in fi.metadata
            @test fi.children[1].name == "item"
        end
        # Every vector-materializing descriptor decides keep/drop the same
        # for empty and nonempty columns: Binary rows are Vector{UInt8}, so
        # => Vector subsumes and keeps the field either way.
        for (n, offs, bytes) in ((2, Int32[0, 2, 3], UInt8[1, 2, 3]),
            (0, Int32[0], UInt8[]))
            bd = Arrow.AC.ArrayData(Arrow.AC.BinaryType(false), n,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(offs),
                 Arrow.AC._databuffer(bytes)])
            bf = Arrow.AC.Field("b", Arrow.AC.BinaryType(false);
                nullable=false, metadata=["bk" => "bv"])
            bsch = Arrow.AC.Schema([bf])
            bb = Arrow.writestream(bsch,
                [Arrow.AC.RecordBatch(bsch, Arrow.AC.ArrayData[bd], n)])
            tb = Arrow.Table(bb; scan=Tables.Scan(select=(:b => Vector,)))
            bsch2 = getfield(tb, :schema)
            @test length(bsch2.fields) == 1
            @test bsch2.fields[1].type isa Arrow.AC.BinaryType
            @test DataAPI.colmetadata(tb, :b, "bk") == "bv"
        end
        # The declared row domain covers EVERY closed materializer (the
        # Core _value methods are the authority), including Field-aware
        # compositions; empty and nonempty columns decide keep/drop alike.
        @test Arrow._declaredbasetype(Arrow.AC.ListViewType(false)) ===
              Vector{Any}
        @test Arrow._declaredbasetype(Arrow.AC.DecimalType(10, 2, 32)) ===
              Int32
        @test Arrow._declaredbasetype(Arrow.AC.DecimalType(38, 2, 128)) ===
              Vector{UInt8}
        @test Arrow._declaredbasetype(
            Arrow.AC.IntervalType(Arrow.AC.YEAR_MONTH)) === Int32
        @test Arrow._declaredbasetype(
            Arrow.AC.IntervalType(Arrow.AC.DAY_TIME)) ===
              NamedTuple{(:days, :millis),Tuple{Int32,Int32}}
        dleaf = Arrow.AC.Field("v", Arrow.AC.BinaryType(false); nullable=true)
        druns = Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true);
            nullable=false)
        dref = Arrow.AC.Field("d",
            Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true),
                Arrow.AC.RunEndEncodedType(), false);
            nullable=true, children=[druns, dleaf])
        @test Arrow._declaredeltype(dref) === Union{Missing,Vector{UInt8}}
        u1 = Arrow.AC.Field("u", Arrow.AC.UnionType(Arrow.AC.DenseMode,
            Int8[0]); nullable=false,
            children=[Arrow.AC.Field("a", Arrow.AC.IntType(64, true);
                nullable=false)])
        @test Arrow._declaredeltype(u1) === Int64
        # The declared domain equals the ACTUAL container type: temporal
        # leaves under a transparent wrapper stay raw storage, and a
        # multi-child union declares the mixed-population join.
        dtf = Arrow.AC.Field("values", Arrow.AC.DateType(Arrow.AC.DAY);
            nullable=true)
        rnf = Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true);
            nullable=false)
        reef0 = Arrow.AC.Field("r", Arrow.AC.RunEndEncodedType();
            nullable=false, children=[rnf, dtf])
        @test Arrow._declaredeltype(reef0) === Union{Missing,Int32}
        huf = Arrow.AC.Field("u", Arrow.AC.UnionType(Arrow.AC.SparseMode,
            Int8[0, 1]); nullable=false,
            children=[Arrow.AC.Field("a", Arrow.AC.IntType(64, true);
                nullable=false),
                Arrow.AC.Field("b", Arrow.AC.Utf8Type(false);
                    nullable=false)])
        @test Arrow._declaredeltype(huf) === Any
        # REE<Date32> end-to-end: raw Int32 rows, => Integer keeps the
        # retained field for empty and nonempty columns alike.
        for (n, runs, vals) in ((3, Int32[2, 3], Int32[19000, 19001]),
            (0, Int32[], Int32[]))
            vd = Arrow.AC.ArrayData(Arrow.AC.DateType(Arrow.AC.DAY),
                length(vals), [Arrow.AC.BufferSlice(),
                    Arrow.AC._databuffer(vals)])
            rd = Arrow.AC.ArrayData(Arrow.AC.IntType(32, true),
                length(runs), [Arrow.AC.BufferSlice(),
                    Arrow.AC._databuffer(runs)])
            reed = Arrow.AC.ArrayData(Arrow.AC.RunEndEncodedType(), n,
                Arrow.AC.BufferSlice[]; children=[rd, vd], nullcount=0)
            rsch0 = Arrow.AC.Schema([reef0])
            rb = Arrow.writestream(rsch0,
                [Arrow.AC.RecordBatch(rsch0, Arrow.AC.ArrayData[reed], n)])
            n > 0 && @test Arrow.Table(rb).r == Int32[19000, 19000, 19001]
            tre = Arrow.Table(rb; scan=Tables.Scan(select=(:r => Integer,)))
            rsch2 = getfield(tre, :schema)
            @test length(rsch2.fields) == 1
            @test rsch2.fields[1].type isa Arrow.AC.RunEndEncodedType
        end
        # Mixed heterogeneous union: a valid mixed population widens to Any,
        # so empty and nonempty drop the field alike under a union target.
        for n in (2, 0)
            tid = Arrow.AC._databuffer(Int8[0, 1][1:n])
            uad = Arrow.AC.ArrayData(Arrow.AC.IntType(64, true), n,
                [Arrow.AC.BufferSlice(),
                 Arrow.AC._databuffer(Int64[5, 6][1:n])])
            ubd = Arrow.AC.ArrayData(Arrow.AC.Utf8Type(false), n,
                [Arrow.AC.BufferSlice(),
                 Arrow.AC._databuffer(Int32[0, 1, 2][1:(n + 1)]),
                 Arrow.AC._databuffer(UInt8[0x61, 0x62][1:n])])
            uud = Arrow.AC.ArrayData(Arrow.AC.UnionType(Arrow.AC.SparseMode,
                Int8[0, 1]), n, [tid]; children=[uad, ubd], nullcount=0)
            usch0 = Arrow.AC.Schema([huf])
            ub0 = Arrow.writestream(usch0,
                [Arrow.AC.RecordBatch(usch0, Arrow.AC.ArrayData[uud], n)])
            tuo = Arrow.Table(ub0; scan=Tables.Scan(
                select=(:u => Union{Integer,AbstractString},)))
            @test isempty(getfield(tuo, :schema).fields)
        end
        # End-to-end: Decimal64 rows are raw Int64 — an => Integer override
        # keeps the retained field for empty and nonempty columns alike.
        for (n, vals) in ((2, Int64[1234, 5678]), (0, Int64[]))
            dd = Arrow.AC.ArrayData(Arrow.AC.DecimalType(10, 2, 64), n,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)])
            df = Arrow.AC.Field("dec", Arrow.AC.DecimalType(10, 2, 64);
                nullable=false, metadata=["dk" => "dv"])
            dsch = Arrow.AC.Schema([df])
            db = Arrow.writestream(dsch,
                [Arrow.AC.RecordBatch(dsch, Arrow.AC.ArrayData[dd], n)])
            td = Arrow.Table(db; scan=Tables.Scan(select=(:dec => Integer,)))
            dsch2 = getfield(td, :schema)
            @test length(dsch2.fields) == 1
            @test dsch2.fields[1].type isa Arrow.AC.DecimalType
            @test DataAPI.colmetadata(td, :dec, "dk") == "dv"
        end
        # Identity-strict at every depth: a replaced list column refuses,
        # never coerces (convert would turn true into Int64(1)).
        io6 = IOBuffer()
        Arrow.write(io6, (l=[[1, 2]],); file=false)
        trl = Arrow.Table(take!(io6))
        cols = AbstractVector[c for c in getfield(trl, :columns)]
        cols[1] = Any[Any[true, false]]
        swapped = Arrow.Table(getfield(trl, :names), cols,
            getfield(trl, :lookup), getfield(trl, :schema),
            Arrow.AC.OwnerRegion[], 1)
        @test_throws ArgumentError Arrow.write(IOBuffer(), swapped;
            file=false)
    end

    @testset "typed read routing serves every valid layout" begin
        # NullType columns (claim = Missing) and homogeneous unions (claim
        # joins to a concrete type Core refuses) must ride the dynamic
        # path.
        nd = Arrow.AC.ArrayData(Arrow.AC.NullType(), 2,
            Arrow.AC.BufferSlice[]; nullcount=2)
        nf = Arrow.AC.Field("n", Arrow.AC.NullType(); nullable=true)
        nsch = Arrow.AC.Schema([nf])
        nb = Arrow.writestream(nsch,
            [Arrow.AC.RecordBatch(nsch, Arrow.AC.ArrayData[nd], 2)])
        @test isequal(Arrow.Table(nb).n, [missing, missing])
        tid = Arrow.AC._databuffer(Int8[0, 1])
        ua = Arrow.AC.ArrayData(Arrow.AC.IntType(64, true), 2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[10, 20])])
        ub_ = Arrow.AC.ArrayData(Arrow.AC.IntType(64, true), 2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[30, 40])])
        uud = Arrow.AC.ArrayData(Arrow.AC.UnionType(Arrow.AC.SparseMode,
            Int8[0, 1]), 2, [tid]; children=[ua, ub_], nullcount=0)
        uf = Arrow.AC.Field("u", Arrow.AC.UnionType(Arrow.AC.SparseMode,
            Int8[0, 1]); nullable=false,
            children=[Arrow.AC.Field("a", Arrow.AC.IntType(64, true);
                nullable=false),
                Arrow.AC.Field("b", Arrow.AC.IntType(64, true);
                    nullable=false)])
        usch = Arrow.AC.Schema([uf])
        ubz = Arrow.writestream(usch,
            [Arrow.AC.RecordBatch(usch, Arrow.AC.ArrayData[uud], 2)])
        @test Arrow.Table(ubz).u == [10, 40]
        # Dictionary- and REE-wrapped unions route dynamic too.
        @test !Arrow._typedroutable(Arrow.AC.Field("r",
            Arrow.AC.RunEndEncodedType(); nullable=false,
            children=[Arrow.AC.Field("run_ends",
                Arrow.AC.IntType(32, true); nullable=false), uf]))
        @test !Arrow._typedroutable(Arrow.AC.Field("d",
            Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true),
                uf.type, false); nullable=false,
            children=collect(Arrow.AC.Field, uf.children)))
    end

    @testset "errors are clean" begin
        @test_throws ArgumentError Arrow.write(IOBuffer(),
            Tables.partitioner(NamedTuple[]))
        @test_throws ArgumentError Arrow.write(IOBuffer(),
            (st=[(a=1,), missing],))
    end
end

end # module FacadeTests
