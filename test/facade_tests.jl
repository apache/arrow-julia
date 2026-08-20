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
using ArrowStrings

# In-memory byte-range sources: the plain one, and one that meters bytes.
struct _BytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_BytesSource) = length(s.data)
Arrow.readrange(s::_BytesSource, off, len) = s.data[(off + 1):(off + len)]
struct _MeteredSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
    fetched::Base.RefValue{Int64}
end
Arrow.sourcelength(s::_MeteredSource) = length(s.data)
function Arrow.readrange(s::_MeteredSource, off, len)
    s.fetched[] += len
    return s.data[(off + 1):(off + len)]
end
# A source that logs every request, and a concurrent one whose reads finish
# in reverse request order (the last-issued read of a round completes first)
# while a counter records the most reads ever in flight.
struct _LoggingSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
    requests::Vector{NTuple{2,Int64}}
end
Arrow.sourcelength(s::_LoggingSource) = length(s.data)
function Arrow.readrange(s::_LoggingSource, off, len)
    push!(s.requests, (Int64(off), Int64(len)))
    return s.data[(off + 1):(off + len)]
end
mutable struct _ConcurrentSource <: Arrow.AbstractArrowSource
    const data::Vector{UInt8}
    const limit::Int
    @atomic inflight::Int
    @atomic peak::Int
    @atomic issued::Int
end
_ConcurrentSource(data, limit) = _ConcurrentSource(data, limit, 0, 0, 0)
Arrow.sourcelength(s::_ConcurrentSource) = length(s.data)
Arrow.concurrentreads(s::_ConcurrentSource) = s.limit
function Arrow.readrange(s::_ConcurrentSource, off, len)
    n = @atomic s.inflight += 1
    while true
        p = @atomic s.peak
        (n <= p || (@atomicreplace s.peak p => n).success) && break
    end
    order = @atomic s.issued += 1
    # Later requests of a round return sooner: results must land by request.
    sleep(0.002 * max(0, s.limit - (order % s.limit)))
    @atomic s.inflight -= 1
    return s.data[(off + 1):(off + len)]
end
# Sources that violate the contract in each way the reader must refuse.
struct _ShortSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_ShortSource) = length(s.data)
Arrow.readrange(s::_ShortSource, off, len) = s.data[(off + 1):(off + max(0, len - 1))]
struct _LongSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_LongSource) = length(s.data)
Arrow.readrange(s::_LongSource, off, len) = vcat(s.data[(off + 1):(off + len)], 0x00)
struct _WrongTypeSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_WrongTypeSource) = length(s.data)
Arrow.readrange(s::_WrongTypeSource, off, len) = String(s.data[(off + 1):(off + len)])
struct _BadLengthSource <: Arrow.AbstractArrowSource
    reported::Any
end
Arrow.sourcelength(s::_BadLengthSource) = s.reported
Arrow.readrange(s::_BadLengthSource, off, len) = zeros(UInt8, len)

const MIXED = (
    ints=Int64[1, 2, 3, 4],
    floats=[1.5, missing, 3.5, 4.5],
    strs=["a", "bb", missing, "dddd"],
    dates=[Date(2024, 1, 1), Date(2025, 6, 15), missing, Date(1969, 12, 31)],
    stamps=[
        DateTime(2024, 1, 1, 12, 30),
        missing,
        DateTime(2000, 1, 1),
        DateTime(1970, 1, 1),
    ],
    clocks=[Time(12, 30, 15), Time(0), missing, Time(23, 59, 59)],
    spans=[Millisecond(250), missing, Millisecond(0), Millisecond(-10)],
    lists=[[1, 2], Int64[], [3], missing],
    pooled=Arrow.DictEncode(["lo", "hi", "lo", missing]),
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
        Arrow.write(
            path,
            MIXED;
            metadata=Dict("who" => "facade"),
            colmetadata=Dict(:ints => Dict("unit" => "count")),
        )
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
        Arrow.write(
            io,
            Tables.partitioner([(x=Int64[1, 2],), (x=Int64[3, 4],)]);
            file=false,
        )
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
        t = Arrow.Table(
            fb;
            scan=Tables.Scan(select=(:y,), filter=Tables.colcmp(==, Tables.col(:x), 42)),
        )
        @test Tables.columnnames(t) == [:y]
        @test t.y == ["42"]
        # Stream-format input takes the post-decode path, same result.
        io2 = IOBuffer()
        Arrow.write(io2, (x=collect(Int64, 1:100), y=string.(1:100)); file=false)
        t2 = Arrow.Table(
            take!(io2);
            scan=Tables.Scan(select=(:y,), filter=Tables.colcmp(==, Tables.col(:x), 42)),
        )
        @test t2.y == ["42"]
        # Renames land as output names.
        t3 = Arrow.Table(fb; scan=Tables.Scan(select=(:x => :renamed,), limit=2))
        @test Tables.columnnames(t3) == [:renamed]
        @test t3.renamed == [1, 2]
    end

    @testset "ranged source fetches only what the scan needs" begin
        io = IOBuffer()
        Arrow.write(
            io,
            Tables.partitioner([
                (a=collect(Int64, 1:1000), b=[string("v", i) for i = 1:1000]),
                (a=collect(Int64, 1001:2000), b=[string("v", i) for i = 1001:2000]),
            ]),
        )
        fb = take!(io)
        fetched = Ref(Int64(0))
        src = _MeteredSource(fb, fetched)
        rf = Arrow.SourceFile(src; tailbytes=1024, coalesce_gap=0)
        t = Arrow.Table(rf; scan=Tables.Scan(select=(:b,), limit=3, offset=1500))
        @test t.b == ["v1501", "v1502", "v1503"]
        # The first batch is skipped entirely and column :a is never fetched.
        @test fetched[] < length(fb) ÷ 2

        # Request rounds through the public path over an object larger than
        # the tail window: a pushable scan is the tail, the surviving batch's
        # metadata, then the selected buffers (three rounds; here one request
        # each with coalescing), the tail read exactly once; ranges inside the
        # cached tail window are served from it without a request.
        wide = [string("v", i, "-", repeat("x", 60)) for i = 1:2000]
        wio = IOBuffer()
        Arrow.write(
            wio,
            Tables.partitioner([
                (a=collect(Int64, 1:1000), b=wide[1:1000]),
                (a=collect(Int64, 1001:2000), b=wide[1001:2000]),
            ]),
        )
        wb = take!(wio)
        @test length(wb) > 65536
        log = _LoggingSource(wb, NTuple{2,Int64}[])
        t2 = Arrow.Table(log; scan=Tables.Scan(select=(:a,), filter=Tables.col(:a) < 10))
        @test t2.a == collect(1:9)
        tailreq = (Int64(length(wb) - 65536), Int64(65536))
        @test count(==(tailreq), log.requests) == 1
        @test length(log.requests) == 3
        # An object no larger than the tail window is entirely in hand after
        # the tail read: every planned range is served from it.
        small = _LoggingSource(fb, NTuple{2,Int64}[])
        t3 = Arrow.Table(small; scan=Tables.Scan(select=(:a,), limit=3, offset=1500))
        @test t3.a == [1501, 1502, 1503]
        @test small.requests == [(Int64(0), Int64(length(fb)))]
        # No scan (and any unpushable scan): the object is read whole — the
        # cached tail plus the prefix, two requests, no planning.
        empty!(log.requests)
        tw = Arrow.Table(log)
        @test tw.a == 1:2000 && length(log.requests) == 2
        @test sum(last, log.requests) == length(wb)
        empty!(log.requests)
        tz = Arrow.Table(log; scan=Tables.Scan(select=()))
        @test length(tz) == 2000 && length(log.requests) == 2

        # Concurrent reads: bounded by the source's limit, results placed by
        # request even though later requests complete first.
        cs = _ConcurrentSource(fb, 3)
        cf = Arrow.SourceFile(cs; tailbytes=1024, coalesce_gap=0)
        tc = Arrow.Table(cf; scan=Tables.Scan(select=(:a, :b)))
        @test tc.a == 1:2000 && tc.b == [string("v", i) for i = 1:2000]
        @test 1 < (@atomic cs.peak) <= 3

        # Contract violations fail closed with ValidationError, never a
        # wrong table or a stray error type.
        for bad in (_ShortSource(fb), _LongSource(fb), _WrongTypeSource(fb))
            @test_throws Arrow.AC.ValidationError Arrow.Table(
                bad;
                scan=Tables.Scan(select=(:a,)),
            )
        end
        for reported in (
            -1,
            Int128(typemax(Int64)) + 1,
            Int128(typemin(Int64)) - 1,
            3402.0,
            1.5,
            "3402",
            nothing,
        )
            @test_throws Arrow.AC.ValidationError Arrow.Table(_BadLengthSource(reported))
        end
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
        @test_throws ArgumentError Arrow.write(
            io,
            Tables.partitioner([
                (left=Int64[1], right=Int64[10]),
                (right=Int64[20], left=Int64[2]),
            ]);
            file=false,
        )
        @test_throws ArgumentError Arrow.write(
            io,
            Tables.partitioner([(x=Int64[1],), (y=Int64[2],)]);
            file=false,
        )
        @test_throws ArgumentError Arrow.write(
            io,
            Tables.partitioner([(x=Int64[1],), (x=Int32[2],)]);
            file=false,
        )
    end

    @testset "schema is the authority for facade eltypes" begin
        io = IOBuffer()
        Arrow.write(
            io,
            (s=Union{Missing,String}["a", "b"], m=Union{Missing,String}[missing, missing]);
            file=false,
        )
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
        t3 = Arrow.Table(b3; scan=Tables.Scan(filter=Tables.colcmp(==, Tables.col(:x), 99)))
        @test eltype(t3.x) == Int64 && isempty(t3.x)
    end

    @testset "temporal scans agree across formats and renames" begin
        data = (
            x=Int64[1, 2, 3],
            date=[Date(2024, 1, 1), Date(2024, 1, 2), Date(2024, 1, 3)],
            stamp=[
                DateTime(1970, 1, 1),
                DateTime(1970, 1, 1, 0, 0, 2),
                DateTime(2001, 9, 9),
            ],
        )
        fio = IOBuffer()
        Arrow.write(fio, data)
        sio = IOBuffer()
        Arrow.write(sio, data; file=false)
        scan = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:date), Date(2024, 1, 3)))
        want = Tables.scan(data, scan)
        for bytes in (take!(fio), take!(sio))
            got = Arrow.Table(bytes; scan=scan)
            @test got.x == want.x
            @test got.date == want.date && eltype(got.date) <: Union{Missing,Date}
            @test got.stamp == want.stamp
        end
        # renamed temporal output still converts
        rio = IOBuffer()
        Arrow.write(rio, data)
        tr = Arrow.Table(take!(rio); scan=Tables.Scan(select=(:date => :d,), limit=1))
        @test tr.d == [Date(2024, 1, 1)]
    end

    @testset "ranged reads carry the schema" begin
        io = IOBuffer()
        Arrow.write(
            io,
            (stamp=[DateTime(2020, 5, 5)],);
            metadata=Dict("origin" => "ranged"),
        )
        fb = take!(io)
        src = _BytesSource(fb)
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
        d_us = Arrow.AC._arraydata(
            t_us,
            d.len,
            d.buffers,
            0,
            Arrow.AC.ArrayData[],
            nothing,
            d.owner,
            Arrow.AC.nullcount(d),
        )
        f_us = Arrow.AC.Field("us", t_us; nullable=true)
        sch = Arrow.AC.Schema([f_us]; metadata=["k" => "v"])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d_us], 2)])
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
        Arrow.write(
            io2,
            Tables.partitioner([
                (d=Arrow.DictEncode(["a", "b"]),),
                (d=Arrow.DictEncode(["b", "c"]),),
            ]);
            file=true,
        )
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
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
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
        Arrow.write(io, (x=Int64[1],); file=false, colmetadata=Dict(:x => Dict("u" => "1")))
        t = Arrow.Table(take!(io))
        @test DataAPI.metadata(t, "absent", "fallback") == "fallback"
        @test DataAPI.colmetadata(t, 1, "u") == "1"
        @test DataAPI.colmetadata(t, :x, "nope", :d) == :d
        @test collect(first.(DataAPI.colmetadatakeys(t))) == [:x]
    end

    @testset "temporal scans preserve cross-type predicate semantics" begin
        data = (
            x=Int64[1, 2, 3],
            d32=[Date(1970, 1, 1), Date(1970, 1, 2), Date(1970, 1, 3)],
            ts=[DateTime(2020, 1, 1), DateTime(2020, 1, 2), DateTime(2020, 1, 3)],
        )
        fio = IOBuffer()
        Arrow.write(fio, data)
        fb = take!(fio)
        sio = IOBuffer()
        Arrow.write(sio, data; file=false)
        sb = take!(sio)
        cases = [
            # Date32 vs midnight DateTime: cross-type equality holds
            Tables.Scan(
                select=(:x,),
                filter=Tables.colcmp(==, Tables.col(:d32), DateTime(1970, 1, 2)),
            ),
            # Timestamp vs Date
            Tables.Scan(
                select=(:x,),
                filter=Tables.colcmp(==, Tables.col(:ts), Date(2020, 1, 2)),
            ),
            # raw integer vs a temporal column: never equal in public domain
            Tables.Scan(select=(:x,), filter=Tables.colcmp(==, Tables.col(:d32), 1)),
            # non-midnight DateTime vs Date32: no exact representation
            Tables.Scan(
                select=(:x,),
                filter=Tables.colcmp(==, Tables.col(:d32), DateTime(1970, 1, 2, 12)),
            ),
        ]
        for scan in cases
            want = Tables.scan(data, scan)
            for bytes in (fb, sb)
                got = Arrow.Table(bytes; scan=scan)
                @test isequal(got.x, want.x)
            end
        end
    end

    @testset "retained rewrite is schema identity" begin
        # Non-nullable temporal descriptors stay non-nullable; Date64 works.
        vals = Int64[0, 86_400_000]
        t64 = Arrow.AC.DateType(Arrow.AC.MILLISECOND_DATE)
        d64 = Arrow.AC._arraydata(
            t64,
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)],
            0,
            Arrow.AC.ArrayData[],
            nothing,
            nothing,
            0,
        )
        fld = Arrow.AC.Field("d", t64; nullable=false)
        sch = Arrow.AC.Schema([fld])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d64], 2)])
        t = Arrow.Table(bytes)
        @test t.d == [DateTime(1970, 1, 1), DateTime(1970, 1, 2)]
        io = IOBuffer()
        Arrow.write(io, t; file=false)
        rt = getfield(Arrow.Table(take!(io)), :schema)
        @test rt.fields[1].type isa Arrow.AC.DateType
        @test rt.fields[1].type.unit == Arrow.AC.MILLISECOND_DATE
        @test rt.fields[1].nullable == false
        # Retained dictionary identity: index width and ordered survive.
        pool = ["a", "b"]
        pf, pd = Arrow.AC.fromjulia("d", pool)
        dt = Arrow.AC.DictionaryType(Arrow.AC.IntType(8, true), pf.type, true)
        idx = Int8[0, 1, 0]
        dd = Arrow.AC.ArrayData(
            dt,
            3,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(idx)];
            dictionary=pd,
            nullcount=0,
        )
        df = Arrow.AC.Field("d", dt; nullable=false)
        dsch = Arrow.AC.Schema([df])
        dbytes = Arrow.writestream(dsch, [Arrow.AC.RecordBatch(dsch, [dd], 3)])
        dt2 = Arrow.Table(dbytes)
        io2 = IOBuffer()
        Arrow.write(io2, dt2; file=false)
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
        broken = Arrow.Table(
            getfield(t, :names),
            AbstractVector[Int64[100], Int64[7]],
            getfield(t, :lookup),
            getfield(t, :schema),
            Arrow.AC.OwnerRegion[],
            1,
        )
        io2 = IOBuffer()
        @test_throws ArgumentError Arrow.write(io2, broken; file=false)
    end

    @testset "type overrides and renamed schemas" begin
        io = IOBuffer()
        Arrow.write(
            io,
            (x=Union{Missing,Int64}[1, missing], d=[Date(2024, 1, 1), Date(2024, 1, 2)]),
        )
        fb = take!(io)
        t = Arrow.Table(
            fb;
            scan=Tables.Scan(select=(:x => Union{Missing,Float64}, :d => Date)),
        )
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
        t = Arrow.Table(take!(io); scan=Tables.Scan(select=(), filter=Tables.col(:x) > 2))
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
        d_us = Arrow.AC._arraydata(
            t_us,
            2,
            d.buffers,
            0,
            Arrow.AC.ArrayData[],
            nothing,
            nothing,
            0,
        )
        sch = Arrow.AC.Schema([Arrow.AC.Field("us", t_us; nullable=true)])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d_us], 2)])
        data = (us=us,)
        for scan in (
            Tables.Scan(
                filter=Tables.colcmp(==, Tables.col(:us), DateTime(1970, 1, 1, 0, 0, 1)),
            ),
            Tables.Scan(filter=Tables.colcmp(==, Tables.col(:us), 2_000_000)),
        )
            want = Tables.scan(data, scan)
            got = Arrow.Table(bytes; scan=scan)
            @test isequal(got.us, want.us)
        end
        # Out-of-range and cross-Period literals fall back, matching the
        # authority instead of throwing.
        pdata = (d=[Date(2024, 1, 1)], s=[Second(30)])
        io = IOBuffer()
        Arrow.write(io, pdata)
        pb = take!(io)
        for scan in (
            Tables.Scan(filter=Tables.colcmp(==, Tables.col(:d), Date(6_000_000, 1, 1))),
            Tables.Scan(filter=Tables.colcmp(==, Tables.col(:s), Month(1))),
        )
            want = Tables.scan(pdata, scan)
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
        bytes = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        for source in (bytes, _BytesSource(bytes))
            t = Arrow.Table(source; scan=Tables.Scan())
            @test Tables.rowcount(t) == 3
            tw = Arrow.Table(source; scan=Tables.Scan(limit=1, offset=1))
            @test Tables.rowcount(tw) == 1
        end
    end

    @testset "override subsumption is a no-op; zero-field filters count" begin
        io = IOBuffer()
        Arrow.write(io, (x=Union{Missing,Int64}[1, 2], s=Union{Missing,String}["a", "b"]))
        fb = take!(io)
        # nullable source, no observed missing: supertype/no-op overrides
        # keep the DECLARED element type, exactly like Tables.scan.
        t = Arrow.Table(fb; scan=Tables.Scan(select=(:x => Int64, :s => AbstractString)))
        @test eltype(t.x) == Union{Missing,Int64}
        @test eltype(t.s) == Union{Missing,String}
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)   # rewrites cleanly, metadata intact
        @test isequal(Arrow.Table(take!(io2)).x, [1, 2])
        # zero-field: filters and validation apply
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        tz = Arrow.Table(
            zb;
            scan=Tables.Scan(
                filter=Tables.colcmp(==, Tables.col(:nope), 1),
                validate=false,
            ),
        )
        @test Tables.rowcount(tz) == 0
        @test_throws ArgumentError Arrow.Table(
            zb;
            scan=Tables.Scan(filter=Tables.colcmp(==, Tables.col(:nope), 1)),
        )
        # ranged zero-field honors SourceFile limits
        rfz = Arrow.SourceFile(_BytesSource(zb); limits=Arrow.Limits(max_array_length=2))
        @test_throws Arrow.AC.ValidationError Arrow.Table(rfz)
    end

    @testset "replaced columns are refused before value access" begin
        io = IOBuffer()
        Arrow.write(io, (n=Int64[1], s=["x"], p=Arrow.DictEncode(["x"])))
        t = Arrow.Table(take!(io))
        for (col, bad) in ((:n, ["oops"]), (:s, Int64[1]), (:p, Int64[7]))
            cols = AbstractVector[c for c in getfield(t, :columns)]
            cols[getfield(t, :lookup)[col]] = bad
            broken = Arrow.Table(
                getfield(t, :names),
                cols,
                getfield(t, :lookup),
                getfield(t, :schema),
                Arrow.AC.OwnerRegion[],
                1,
            )
            io2 = IOBuffer()
            @test_throws ArgumentError Arrow.write(io2, broken; file=false)
        end
        # missing into a non-nullable retained dictionary is refused
        pool = ["a"]
        pf, pd = Arrow.AC.fromjulia("d", pool)
        dt = Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true), pf.type, false)
        dd = Arrow.AC.ArrayData(
            dt,
            1,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0])];
            dictionary=pd,
            nullcount=0,
        )
        df = Arrow.AC.Field("d", dt; nullable=false)
        dsch = Arrow.AC.Schema([df])
        dbytes = Arrow.writestream(dsch, [Arrow.AC.RecordBatch(dsch, [dd], 1)])
        td = Arrow.Table(dbytes)
        cols = AbstractVector[Union{Missing,String}[missing]]
        brokend = Arrow.Table(
            getfield(td, :names),
            cols,
            getfield(td, :lookup),
            getfield(td, :schema),
            Arrow.AC.OwnerRegion[],
            1,
        )
        io3 = IOBuffer()
        @test_throws ArgumentError Arrow.write(io3, brokend; file=false)
    end

    @testset "override conversions follow the authority exactly" begin
        io = IOBuffer()
        Arrow.write(io, (a=Int64[1, 2], b=Union{Missing,Int64}[1, 2]))
        fb = take!(io)
        # Real conversions: requested type exact, missing only when observed.
        t = Arrow.Table(
            fb;
            scan=Tables.Scan(select=(:a => Union{Missing,Float64}, :b => Float64)),
        )
        @test eltype(t.a) == Union{Missing,Float64}
        @test eltype(t.b) == Float64
        # Zero-field: true-valued and unmatched-reference filters keep rows.
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        t2 = Arrow.Table(
            zb;
            scan=Tables.Scan(filter=Tables.isnull(Tables.col(:gone)), validate=false),
        )
        @test Tables.rowcount(t2) == 3
        t3 = Arrow.Table(
            zb;
            scan=Tables.Scan(
                filter=Tables.isnull(Tables.col(:gone)),
                validate=false,
                limit=1,
                offset=1,
            ),
        )
        @test Tables.rowcount(t3) == 1
        # List => Vector is a no-op: values, retained field, and metadata
        # all survive.
        io4 = IOBuffer()
        Arrow.write(
            io4,
            (l=[[1, 2], [3]],);
            file=false,
            colmetadata=Dict(:l => Dict("k" => "v")),
        )
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
        zb = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        af = Arrow.readfile(zb)
        for (scan, want) in (
            (Tables.Scan(filter=Tables.AlwaysTrue()), 3),
            (Tables.Scan(filter=Tables.AlwaysTrue(), limit=1, offset=1), 1),
            (Tables.Scan(filter=Tables.isnull(Tables.col(:gone)), validate=false), 3),
            (Tables.Scan(filter=Tables.AlwaysFalse()), 0),
        )
            got = Tables.scan(af, scan)
            @test Tables.rowcount(Tables.columns(got)) == want
            rgot = Tables.scan(Arrow.SourceFile(_BytesSource(zb)), scan)
            @test Tables.rowcount(Tables.columns(rgot)) == want
        end
        # The facade path allocates nothing proportional to a hostile count:
        # a tiny file claiming a million rows answers limit=1 instantly.
        big = Arrow.writefile(
            sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 1_000_000)],
        )
        stats =
            @timed Arrow.Table(big; scan=Tables.Scan(filter=Tables.AlwaysTrue(), limit=1))
        @test Tables.rowcount(stats.value) == 1
        @test stats.bytes < 1_000_000
        # The metadata-only ranged read keeps the reader trust boundary: a
        # corrupted continuation prefix rejects exactly as the full reader
        # rejects it.
        bad = copy(zb)
        bad[65:68] .= 0x00
        @test_throws Arrow.AC.ValidationError Arrow.readfile(copy(bad))
        @test_throws Arrow.AC.ValidationError Tables.scan(
            Arrow.SourceFile(_BytesSource(copy(bad))),
            Tables.Scan(),
        )
        # Header reads share ONE cumulative budget, as Limits documents:
        # many tiny batches refuse under a bound one batch fits.
        many = Arrow.writefile(
            sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 1) for _ = 1:200],
        )
        tight = Arrow.Limits(max_total_allocated_bytes=6000)
        @test_throws Arrow.AllocationLimitError Tables.scan(
            Arrow.readfile(copy(many); limits=tight),
            Tables.Scan(),
        )
        @test_throws Arrow.AllocationLimitError Tables.scan(
            Arrow.SourceFile(_BytesSource(copy(many)); limits=tight),
            Tables.Scan(),
        )
        # A zero-field schema declares no dictionary ids: a footer listing a
        # well-framed dictionary block is orphaned, and the metadata-only
        # ranged read rejects it exactly as the full reader does.
        f0 = Arrow.readfile(copy(zb))
        (roff, rmetalen, rbodylen) = f0.recordblocks[1]
        recbytes = zb[(Int(roff) + 1):Int(roff + rmetalen + rbodylen)]
        dataend = Int(roff + rmetalen + rbodylen)
        doctored = copy(zb[1:dataend])
        append!(doctored, recbytes)
        append!(doctored, reinterpret(UInt8, UInt32[Arrow.CONTINUATION, UInt32(0)]))
        fbb = Arrow.FB.Builder(1024)
        schoff = Arrow._metaschema!(fbb, sch, Base.IdDict{Arrow.AC.Field,Int64}(), Int64[])
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
            Arrow.SourceFile(_BytesSource(copy(doctored))),
            Tables.Scan(),
        )
        # Structural binding is unconditional: an unsupported predicate node
        # rejects even with validate=false, on every facade path.
        zs = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        for source in (zb, zs, _BytesSource(zb))
            @test_throws ArgumentError Arrow.Table(
                source;
                scan=Tables.Scan(filter=Tables.OpNode(:custom, Any[]), validate=false),
            )
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
        Arrow.write(
            io3,
            (l=Vector{Int64}[],);
            file=false,
            colmetadata=Dict(:l => Dict("k" => "v")),
        )
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
        for rows in (
            Vector{Int64}[],
            [[1, 2], Int64[], [3]],
            [Int64[], Int64[]],
            [[Int64[1, 2]], [Int64[]]],
        )
            iof = IOBuffer()
            Arrow.write(iof, (l=rows,); file=true)
            fbb = take!(iof)
            ios = IOBuffer()
            Arrow.write(ios, (l=rows,); file=false)
            sbb = take!(ios)
            for src in (Arrow.Table(fbb), Arrow.Table(sbb), Arrow.Table(_BytesSource(fbb)))
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
        leafd = Arrow.AC.ArrayData(
            Arrow.AC.IntType(64, true),
            3,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[10, 20, 30])],
        )
        innerd = Arrow.AC.ArrayData(
            Arrow.AC.ListType(false),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0, 2, 3])];
            children=[leafd],
        )
        outerd = Arrow.AC.ArrayData(
            Arrow.AC.ListType(true),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[0, 1, 2])];
            children=[innerd],
        )
        leaff = Arrow.AC.Field("item", Arrow.AC.IntType(64, true); nullable=false)
        innerf = Arrow.AC.Field(
            "inner",
            Arrow.AC.ListType(false);
            nullable=true,
            metadata=["ik" => "iv"],
            children=[leaff],
        )
        outerf = Arrow.AC.Field(
            "l",
            Arrow.AC.ListType(true);
            nullable=false,
            metadata=["ok" => "ov"],
            children=[innerf],
        )
        nsch = Arrow.AC.Schema([outerf])
        for nrows in (2, 0)
            data =
                nrows == 0 ?
                Arrow.AC.ArrayData(
                    Arrow.AC.ListType(true),
                    0,
                    [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[0])];
                    children=[
                        Arrow.AC.ArrayData(
                            Arrow.AC.ListType(false),
                            0,
                            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0])];
                            children=[
                                Arrow.AC.ArrayData(
                                    Arrow.AC.IntType(64, true),
                                    0,
                                    [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[])],
                                ),
                            ],
                        ),
                    ],
                ) : outerd
            nb = Arrow.writestream(
                nsch,
                [Arrow.AC.RecordBatch(nsch, Arrow.AC.ArrayData[data], nrows)],
            )
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
        for (n, offs, bytes) in
            ((2, Int32[0, 2, 3], UInt8[1, 2, 3]), (0, Int32[0], UInt8[]))
            bd = Arrow.AC.ArrayData(
                Arrow.AC.BinaryType(false),
                n,
                [
                    Arrow.AC.BufferSlice(),
                    Arrow.AC._databuffer(offs),
                    Arrow.AC._databuffer(bytes),
                ],
            )
            bf = Arrow.AC.Field(
                "b",
                Arrow.AC.BinaryType(false);
                nullable=false,
                metadata=["bk" => "bv"],
            )
            bsch = Arrow.AC.Schema([bf])
            bb = Arrow.writestream(
                bsch,
                [Arrow.AC.RecordBatch(bsch, Arrow.AC.ArrayData[bd], n)],
            )
            tb = Arrow.Table(bb; scan=Tables.Scan(select=(:b => Vector,)))
            bsch2 = getfield(tb, :schema)
            @test length(bsch2.fields) == 1
            @test bsch2.fields[1].type isa Arrow.AC.BinaryType
            @test DataAPI.colmetadata(tb, :b, "bk") == "bv"
        end
        # The declared row domain covers EVERY closed materializer (the
        # Core _value methods are the authority), including Field-aware
        # compositions; empty and nonempty columns decide keep/drop alike.
        @test Arrow._declaredbasetype(Arrow.AC.ListViewType(false)) === Vector{Any}
        @test Arrow._declaredbasetype(Arrow.AC.DecimalType(10, 2, 32)) === Int32
        @test Arrow._declaredbasetype(Arrow.AC.DecimalType(38, 2, 128)) === Vector{UInt8}
        @test Arrow._declaredbasetype(Arrow.AC.IntervalType(Arrow.AC.YEAR_MONTH)) === Int32
        @test Arrow._declaredbasetype(Arrow.AC.IntervalType(Arrow.AC.DAY_TIME)) ===
              NamedTuple{(:days, :millis),Tuple{Int32,Int32}}
        dleaf = Arrow.AC.Field("v", Arrow.AC.BinaryType(false); nullable=true)
        druns = Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true); nullable=false)
        dref = Arrow.AC.Field(
            "d",
            Arrow.AC.DictionaryType(
                Arrow.AC.IntType(32, true),
                Arrow.AC.RunEndEncodedType(),
                false,
            );
            nullable=true,
            children=[druns, dleaf],
        )
        @test Arrow._declaredeltype(dref) === Union{Missing,Vector{UInt8}}
        u1 = Arrow.AC.Field(
            "u",
            Arrow.AC.UnionType(Arrow.AC.DenseMode, Int8[0]);
            nullable=false,
            children=[Arrow.AC.Field("a", Arrow.AC.IntType(64, true); nullable=false)],
        )
        @test Arrow._declaredeltype(u1) === Int64
        # The declared domain equals the ACTUAL container type: temporal
        # leaves under a transparent wrapper stay raw storage, and a
        # multi-child union declares the mixed-population join.
        dtf = Arrow.AC.Field("values", Arrow.AC.DateType(Arrow.AC.DAY); nullable=true)
        rnf = Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true); nullable=false)
        reef0 = Arrow.AC.Field(
            "r",
            Arrow.AC.RunEndEncodedType();
            nullable=false,
            children=[rnf, dtf],
        )
        @test Arrow._declaredeltype(reef0) === Union{Missing,Int32}
        huf = Arrow.AC.Field(
            "u",
            Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]);
            nullable=false,
            children=[
                Arrow.AC.Field("a", Arrow.AC.IntType(64, true); nullable=false),
                Arrow.AC.Field("b", Arrow.AC.Utf8Type(false); nullable=false),
            ],
        )
        @test Arrow._declaredeltype(huf) === Any
        # REE<Date32> end-to-end: raw Int32 rows, => Integer keeps the
        # retained field for empty and nonempty columns alike.
        for (n, runs, vals) in
            ((3, Int32[2, 3], Int32[19000, 19001]), (0, Int32[], Int32[]))
            vd = Arrow.AC.ArrayData(
                Arrow.AC.DateType(Arrow.AC.DAY),
                length(vals),
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)],
            )
            rd = Arrow.AC.ArrayData(
                Arrow.AC.IntType(32, true),
                length(runs),
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(runs)],
            )
            reed = Arrow.AC.ArrayData(
                Arrow.AC.RunEndEncodedType(),
                n,
                Arrow.AC.BufferSlice[];
                children=[rd, vd],
                nullcount=0,
            )
            rsch0 = Arrow.AC.Schema([reef0])
            rb = Arrow.writestream(
                rsch0,
                [Arrow.AC.RecordBatch(rsch0, Arrow.AC.ArrayData[reed], n)],
            )
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
            uad = Arrow.AC.ArrayData(
                Arrow.AC.IntType(64, true),
                n,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[5, 6][1:n])],
            )
            ubd = Arrow.AC.ArrayData(
                Arrow.AC.Utf8Type(false),
                n,
                [
                    Arrow.AC.BufferSlice(),
                    Arrow.AC._databuffer(Int32[0, 1, 2][1:(n + 1)]),
                    Arrow.AC._databuffer(UInt8[0x61, 0x62][1:n]),
                ],
            )
            uud = Arrow.AC.ArrayData(
                Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]),
                n,
                [tid];
                children=[uad, ubd],
                nullcount=0,
            )
            usch0 = Arrow.AC.Schema([huf])
            ub0 = Arrow.writestream(
                usch0,
                [Arrow.AC.RecordBatch(usch0, Arrow.AC.ArrayData[uud], n)],
            )
            tuo = Arrow.Table(
                ub0;
                scan=Tables.Scan(select=(:u => Union{Integer,AbstractString},)),
            )
            @test isempty(getfield(tuo, :schema).fields)
        end
        # End-to-end: Decimal64 rows are raw Int64 — an => Integer override
        # keeps the retained field for empty and nonempty columns alike.
        for (n, vals) in ((2, Int64[1234, 5678]), (0, Int64[]))
            dd = Arrow.AC.ArrayData(
                Arrow.AC.DecimalType(10, 2, 64),
                n,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)],
            )
            df = Arrow.AC.Field(
                "dec",
                Arrow.AC.DecimalType(10, 2, 64);
                nullable=false,
                metadata=["dk" => "dv"],
            )
            dsch = Arrow.AC.Schema([df])
            db = Arrow.writestream(
                dsch,
                [Arrow.AC.RecordBatch(dsch, Arrow.AC.ArrayData[dd], n)],
            )
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
        swapped = Arrow.Table(
            getfield(trl, :names),
            cols,
            getfield(trl, :lookup),
            getfield(trl, :schema),
            Arrow.AC.OwnerRegion[],
            1,
        )
        @test_throws ArgumentError Arrow.write(IOBuffer(), swapped; file=false)
    end

    @testset "typed read routing serves every valid layout" begin
        # NullType columns (claim = Missing) and homogeneous unions (claim
        # joins to a concrete type Core refuses) must ride the dynamic
        # path.
        nd = Arrow.AC.ArrayData(Arrow.AC.NullType(), 2, Arrow.AC.BufferSlice[]; nullcount=2)
        nf = Arrow.AC.Field("n", Arrow.AC.NullType(); nullable=true)
        nsch = Arrow.AC.Schema([nf])
        nb =
            Arrow.writestream(nsch, [Arrow.AC.RecordBatch(nsch, Arrow.AC.ArrayData[nd], 2)])
        @test isequal(Arrow.Table(nb).n, [missing, missing])
        tid = Arrow.AC._databuffer(Int8[0, 1])
        ua = Arrow.AC.ArrayData(
            Arrow.AC.IntType(64, true),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[10, 20])],
        )
        ub_ = Arrow.AC.ArrayData(
            Arrow.AC.IntType(64, true),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[30, 40])],
        )
        uud = Arrow.AC.ArrayData(
            Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]),
            2,
            [tid];
            children=[ua, ub_],
            nullcount=0,
        )
        uf = Arrow.AC.Field(
            "u",
            Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]);
            nullable=false,
            children=[
                Arrow.AC.Field("a", Arrow.AC.IntType(64, true); nullable=false),
                Arrow.AC.Field("b", Arrow.AC.IntType(64, true); nullable=false),
            ],
        )
        usch = Arrow.AC.Schema([uf])
        ubz = Arrow.writestream(
            usch,
            [Arrow.AC.RecordBatch(usch, Arrow.AC.ArrayData[uud], 2)],
        )
        @test Arrow.Table(ubz).u == [10, 40]
        # Dictionary- and REE-wrapped unions route dynamic too.
        @test !Arrow._typedroutable(
            Arrow.AC.Field(
                "r",
                Arrow.AC.RunEndEncodedType();
                nullable=false,
                children=[
                    Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true); nullable=false),
                    uf,
                ],
            ),
        )
        @test !Arrow._typedroutable(
            Arrow.AC.Field(
                "d",
                Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true), uf.type, false);
                nullable=false,
                children=collect(Arrow.AC.Field, uf.children),
            ),
        )
    end

    @testset "errors are clean" begin
        @test_throws ArgumentError Arrow.write(IOBuffer(), Tables.partitioner(NamedTuple[]))
        @test_throws ArgumentError Arrow.write(IOBuffer(), (st=[(a=1,), missing],))
    end

    @testset "advisory nullability: nulls under a non-nullable field read" begin
        # Field nullability is advisory at the reader tier (the semantic
        # validation IPC applies accepts such data, as the reference
        # implementation does). A non-nullable field whose batch holds a
        # null therefore READS, as a missing-capable column, on every
        # entry: the facade, the facade's scan path, and the direct handle
        # scan — for primitives and through a run-end-encoded wrapper.
        AC = Arrow.AC
        f = AC.Field("x", AC.IntType(64, true); nullable=false)
        d = AC.ArrayData(
            AC.IntType(64, true),
            2,
            [AC._databuffer(UInt8[0b10]), AC._databuffer(Int64[0, 7])];
            nullcount=1,
        )
        sch = AC.Schema([f])
        bytes = Arrow.writefile(sch, AC.RecordBatch[AC.RecordBatch(sch, [d], 2)])
        @test isequal(Arrow.Table(bytes).x, [missing, 7])
        @test eltype(Arrow.Table(bytes).x) === Union{Missing,Int64}
        @test isequal(Arrow.Table(bytes; scan=Tables.Scan()).x, [missing, 7])
        for handle in (Arrow.readfile(bytes), Arrow.SourceFile(_BytesSource(bytes)))
            got = Tables.scan(handle, Tables.Scan())
            @test isequal(got.x, [missing, 7])
            @test eltype(got.x) === Union{Missing,Int64}
        end
        rf, rd = AC.fromjulia("run_ends", Int32[1, 2])
        vf0, vd = AC.fromjulia("values", Union{Missing,Int64}[missing, 7])
        vf = AC.Field("values", vf0.type; nullable=false)
        t = AC.RunEndEncodedType()
        ref = AC.Field("r", t; children=[rf, vf])
        red = AC.ArrayData(t, 2, AC.BufferSlice[]; children=[rd, vd], nullcount=0)
        sch2 = AC.Schema([ref])
        bytes2 = Arrow.writefile(sch2, AC.RecordBatch[AC.RecordBatch(sch2, [red], 2)])
        @test isequal(Arrow.Table(bytes2).r, [missing, 7])
        for handle in (Arrow.readfile(bytes2), Arrow.SourceFile(_BytesSource(bytes2)))
            @test isequal(Tables.scan(handle, Tables.Scan()).r, [missing, 7])
        end
        # a dictionary column: a null-free pool under a non-nullable field is
        # Missing-free; a pool holding a null (the schema's one flag cannot
        # declare it) reads missing-capable
        df0, dd = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, 0])
        df = AC.Field(
            "d",
            df0.type;
            nullable=false,
            children=collect(AC.Field, df0.children),
        )
        schd = AC.Schema([df])
        bytesd = Arrow.writefile(schd, AC.RecordBatch[AC.RecordBatch(schd, [dd], 3)])
        @test Arrow.Table(bytesd).d == ["lo", "hi", "lo"]
        @test eltype(Arrow.Table(bytesd).d) === String
        pf0, pd = AC.fromjulia_dict("d", Union{Missing,String}["lo", missing], [0, 1, 0])
        pf = AC.Field(
            "d",
            pf0.type;
            nullable=false,
            children=collect(AC.Field, pf0.children),
        )
        schp = AC.Schema([pf])
        bytesp = Arrow.writefile(schp, AC.RecordBatch[AC.RecordBatch(schp, [pd], 3)])
        @test isequal(Arrow.Table(bytesp).d, ["lo", missing, "lo"])
        @test eltype(Arrow.Table(bytesp).d) === Union{Missing,String}
        # a conforming batch keeps the declared, Missing-free type
        cf, cd = AC.fromjulia("c", Int64[1, 2])
        schc = AC.Schema([cf])
        bytesc = Arrow.writefile(schc, AC.RecordBatch[AC.RecordBatch(schc, [cd], 2)])
        @test eltype(Arrow.Table(bytesc).c) === Int64
    end

    @testset "empty column types do not depend on batch structure" begin
        # A schema-only source and a source with one zero-row batch give
        # every column the same element type, for the closed scalars and
        # through the transparent wrappers.
        AC = Arrow.AC
        cases = [
            (
                "bin",
                AC.BinaryType(false),
                () -> AC.ArrayData(
                    AC.BinaryType(false),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int32[0]), AC._databuffer(UInt8[])];
                    nullcount=0,
                ),
                Vector{UInt8},
            ),
            (
                "fsb",
                AC.FixedSizeBinaryType(2),
                () -> AC.ArrayData(
                    AC.FixedSizeBinaryType(2),
                    0,
                    [AC.BufferSlice(), AC._databuffer(UInt8[])];
                    nullcount=0,
                ),
                Vector{UInt8},
            ),
            (
                "d128",
                AC.DecimalType(10, 2, 128),
                () -> AC.ArrayData(
                    AC.DecimalType(10, 2, 128),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int128[])];
                    nullcount=0,
                ),
                Vector{UInt8},
            ),
            (
                "d64",
                AC.DecimalType(10, 2, 64),
                () -> AC.ArrayData(
                    AC.DecimalType(10, 2, 64),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int64[])];
                    nullcount=0,
                ),
                Int64,
            ),
            (
                "iym",
                AC.IntervalType(AC.YEAR_MONTH),
                () -> AC.ArrayData(
                    AC.IntervalType(AC.YEAR_MONTH),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int32[])];
                    nullcount=0,
                ),
                Int32,
            ),
            (
                "imdn",
                AC.IntervalType(AC.MONTH_DAY_NANO),
                () -> AC.ArrayData(
                    AC.IntervalType(AC.MONTH_DAY_NANO),
                    0,
                    [AC.BufferSlice(), AC._databuffer(UInt8[])];
                    nullcount=0,
                ),
                NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}},
            ),
        ]
        for (name, t, mk, want) in cases
            f = AC.Field(name, t; nullable=false)
            sch = AC.Schema([f])
            so = Arrow.Table(Arrow.writefile(sch, AC.RecordBatch[]))
            zr = Arrow.Table(
                Arrow.writefile(sch, AC.RecordBatch[AC.RecordBatch(sch, [mk()], 0)]),
            )
            @test eltype(Tables.getcolumn(so, 1)) === want
            @test eltype(Tables.getcolumn(zr, 1)) === want
            @test length(Tables.getcolumn(so, 1)) == 0 == length(Tables.getcolumn(zr, 1))
        end
        # transparent REE over Int64: the values child's type, both ways
        rf, rd = AC.fromjulia("run_ends", Int32[])
        vf, vd = AC.fromjulia("values", Int64[])
        t = AC.RunEndEncodedType()
        ref = AC.Field("r", t; children=[rf, vf])
        sch = AC.Schema([ref])
        so = Arrow.Table(Arrow.writefile(sch, AC.RecordBatch[]))
        red = AC.ArrayData(t, 0, AC.BufferSlice[]; children=[rd, vd], nullcount=0)
        zr =
            Arrow.Table(Arrow.writefile(sch, AC.RecordBatch[AC.RecordBatch(sch, [red], 0)]))
        @test eltype(so.r) === Int64 === eltype(zr.r)
        # composites: the declared row container, both ways
        lf, ld = AC.fromjulia("l", Vector{Int64}[])
        schl = AC.Schema([lf])
        @test eltype(Arrow.Table(Arrow.writefile(schl, AC.RecordBatch[])).l) === Vector{Any}
        @test eltype(
            Arrow.Table(
                Arrow.writefile(schl, AC.RecordBatch[AC.RecordBatch(schl, [ld], 0)]),
            ).l,
        ) === Vector{Any}
    end

    @testset "ArrowStrings columns write as Utf8View, zero-copy" begin
        # A ArrowStringVector's memory IS a Utf8View array: payloads are the
        # views buffer, its byte buffers the variadic data buffers. Build one
        # the way the CSV kernel does (inline ≤12, else a view into buffer 0
        # or the `extra` buffer 1) and check the writer wraps rather than
        # materializes, the file carries Utf8View, and the facade reads it
        # back as ordinary Strings.
        buf = Vector{UInt8}(codeunits("id,s\n1,abcd\n2,thirteen-byte\n3,\n"))
        extra = Vector{UInt8}(codeunits("she said \"hi\" and left"))
        long1 = first(findfirst(codeunits("thirteen-byte"), buf))
        abcd = first(findfirst(codeunits("abcd"), buf))
        payloads = ArrowStringPayload[
            ArrowStrings.inline_payload(buf, abcd, 4),
            ArrowStrings.view_payload(buf, long1, 13, 0, long1 - 1),
            ArrowStrings.PAYLOAD_MISSING,
            ArrowStrings.view_payload(extra, 1, length(extra), 1, 0),
        ]
        col = ArrowStringVector{Union{Missing,ArrowString}}(payloads, buf, extra)
        f, d = Arrow._writecolumn("s", col)
        @test f.type == Arrow.AC.ViewType(true) && f.nullable
        @test d.buffers[2].region.root === payloads       # views: the payload vector itself
        @test d.buffers[3].region.root === buf
        @test d.buffers[4].region.root === extra
        io = IOBuffer()
        Arrow.write(io, (id=[1, 2, 3, 4], s=col))
        bytes = take!(io)
        @test Arrow.readfile(bytes).schema.fields[2].type == Arrow.AC.ViewType(true)
        t = Arrow.Table(bytes)
        @test eltype(t.s) === Union{Missing,String}
        @test isequal(t.s, ["abcd", "thirteen-byte", missing, "she said \"hi\" and left"])
        # a non-nullable column declares non-nullable
        col0 = ArrowStringVector{ArrowString}(payloads[[1, 2]], buf, extra)
        f0, _ = Arrow._writecolumn("s", col0)
        @test !f0.nullable
        Arrow.write(io, (s=col0,))
        @test Arrow.Table(take!(io)).s == ["abcd", "thirteen-byte"]
        # an all-inline column may have ZERO data buffers — the format allows
        # a Utf8View with no variadic buffers, and the wire carries exactly
        # the fixed validity + views pair
        inl = ArrowStringVector{ArrowString}(
            [
                ArrowStrings.inline_payload(buf, abcd, 4),
                ArrowStrings.inline_payload(buf, abcd, 2),
            ],
            Vector{Vector{UInt8}}(),
        )
        fi, di = Arrow._writecolumn("s", inl)
        @test length(di.buffers) == 2
        Arrow.write(io, (s=inl,))
        bytes0 = take!(io)
        @test Arrow.Table(bytes0).s == ["abcd", "ab"]
        af = Arrow.readfile(bytes0)
        @test af.schema.fields[1].type == Arrow.AC.ViewType(true)
        @test length(af[1].columns[1].buffers) == 2         # no variadic buffers on the wire
    end
end

end # module FacadeTests
