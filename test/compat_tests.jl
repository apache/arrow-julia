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
using Dates
using Tables
using Arrow
using ArrowTypes
import Durations

const AC = Arrow.ArrowCore

# The Arrow 2.x compatibility surface: the exported ArrowTypes binding, the
# getmetadata accessor, tobuffer, the curried write, the removed-keyword
# warnings, and the typed scan overrides the migration guide recommends. The
# ArrowTimeZonesExt tests run in a child process so this process never loads
# TimeZones and keeps testing naive reads.

@testset "Arrow 2.x compat surface" begin
    @testset "ArrowTypes is exported" begin
        @test :ArrowTypes in names(Arrow)
        @test Arrow.ArrowTypes === ArrowTypes
    end

    @testset "getmetadata" begin
        tbl = (; a=[1, 2])
        io = IOBuffer()
        Arrow.write(io, tbl; metadata=("k" => "v", "k2" => "v2"))
        t = Arrow.Table(take!(io))
        @test Arrow.getmetadata(t) == Dict("k" => "v", "k2" => "v2")
        io = IOBuffer()
        Arrow.write(io, tbl)
        bare = Arrow.Table(take!(io))
        @test Arrow.getmetadata(bare) === nothing
        @test_throws ArgumentError Arrow.getmetadata(bare.a)
    end

    @testset "tobuffer emits the stream format" begin
        buf = Arrow.tobuffer((; a=[1, 2, 3]))
        @test position(buf) == 0
        bytes = take!(buf)
        @test bytes[1:6] != b"ARROW1"
        t = Arrow.Table(bytes)
        @test t.a == [1, 2, 3]
        parts = collect(Arrow.Stream(bytes))
        @test length(parts) == 1
    end

    @testset "curried write" begin
        path = joinpath(mktempdir(), "curried.arrow")
        @test ((; a=[1, 2]) |> Arrow.write(path)) == path
        @test Arrow.Table(path).a == [1, 2]
        io = IOBuffer()
        @test ((; a=[3]) |> Arrow.write(io; file=false)) === io
        @test Arrow.Table(take!(io)).a == [3]
    end

    @testset "removed write keywords warn and are ignored" begin
        io = IOBuffer()
        @test_logs (:warn, r"`dictencode` was removed") match_mode = :any Arrow.write(
            io,
            (; a=[1]);
            dictencode=true,
        )
        @test Arrow.Table(take!(io)).a == [1]
        # Ignored keywords still produce a working write.
        io = IOBuffer()
        Arrow.write(io, (; a=[1]); ntasks=2, alignment=64, maxdepth=6)
        @test Arrow.Table(take!(io)).a == [1]
    end
end

@testset "typed scan overrides for composite rows" begin
    NT = NamedTuple{(:a, :b),Tuple{Float64,String}}
    tbl = (; x=[1, 2], c=[(a=1.0, b="x"), (a=2.0, b="y")])
    io = IOBuffer()
    Arrow.write(io, tbl)
    bytes = take!(io)

    @testset "struct rows to NamedTuple" begin
        t = Arrow.Table(bytes; scan=Tables.Scan(select=(:x, :c => NT)))
        @test eltype(t.c) == NT
        @test t.c == tbl.c
    end

    @testset "missing rows stay missing" begin
        io = IOBuffer()
        Arrow.write(io, (; c=[(a=1.0, b="x"), missing]))
        t = Arrow.Table(take!(io); scan=Tables.Scan(select=(:c => NT,)))
        @test eltype(t.c) == Union{Missing,NT}
        @test t.c[1] == (a=1.0, b="x")
        @test t.c[2] === missing
    end

    @testset "nested vectors of structs" begin
        # Fresh list-of-struct writes are unsupported, so build the fixture
        # at the core level: a list column over one struct child.
        fs, ds = AC.fromjulia_struct("item", (a=[1.0, 2.0, 3.0], b=["x", "y", "z"]))
        lt = AC.ListType(false)
        f = AC.Field("c", lt; nullable=false, children=[fs])
        d = AC.ArrayData(
            lt,
            2,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 1, 3])];
            children=[ds],
            nullcount=0,
        )
        sch = AC.Schema([f])
        nested = Arrow.writefile(sch, [AC.RecordBatch(sch, [d])])
        t = Arrow.Table(nested; scan=Tables.Scan(select=(:c => Vector{NT},)))
        @test eltype(t.c) == Vector{NT}
        @test t.c == [[(a=1.0, b="x")], [(a=2.0, b="y"), (a=3.0, b="z")]]
    end

    @testset "field-name mismatch errors" begin
        Bad = NamedTuple{(:a, :wrong),Tuple{Float64,String}}
        @test_throws ArgumentError Arrow.Table(bytes; scan=Tables.Scan(select=(:c => Bad,)))
    end

    @testset "public temporal leaves in typed rows" begin
        # Rows hold public values at every depth, so a claim naming the
        # public leaf types is an identity check per leaf.
        ns = AC.TimestampType(AC.NANOSECOND, nothing)
        day = AC.DateType(AC.DAY)
        tsf = AC.Field("when", ns; nullable=false)
        dayf = AC.Field("day", day; nullable=false)
        stf = AC.Field("c", AC.StructType(); nullable=false, children=[tsf, dayf])
        tsd = AC.ArrayData(
            ns,
            2,
            [AC.BufferSlice(), AC._databuffer(Int64[10, 20])];
            nullcount=0,
        )
        dayd = AC.ArrayData(
            day,
            2,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 1])];
            nullcount=0,
        )
        std = AC.ArrayData(
            AC.StructType(),
            2,
            [AC.BufferSlice()];
            children=[tsd, dayd],
            nullcount=0,
        )
        sch = AC.Schema([stf])
        tsbytes = Arrow.writefile(sch, [AC.RecordBatch(sch, [std])])
        Ts = Durations.Timestamp{Dates.Nanosecond}
        TNT = NamedTuple{(:when, :day),Tuple{Ts,Dates.Date}}
        t = Arrow.Table(tsbytes; scan=Tables.Scan(select=(:c => TNT,)))
        @test eltype(t.c) == TNT
        @test t.c == [
            (when=reinterpret(Ts, Int64(10)), day=Dates.Date(1970, 1, 1)),
            (when=reinterpret(Ts, Int64(20)), day=Dates.Date(1970, 1, 2)),
        ]
        # a claim naming raw storage integers no longer matches public rows
        RawNT = NamedTuple{(:when, :day),Tuple{Int64,Int32}}
        @test_throws Exception Arrow.Table(tsbytes; scan=Tables.Scan(select=(:c => RawNT,)))
    end
end

@testset "timezone-aware timestamps without TimeZones" begin
    # This process must not have loaded TimeZones: zoned reads are native
    # ZonedTimestamp values and need no extension.
    @test Base.get_extension(Arrow, :ArrowTimeZonesExt) === nothing
    t = AC.TimestampType(AC.MILLISECOND, "America/Denver")
    d = AC.ArrayData(t, 1, [AC.BufferSlice(), AC._databuffer(Int64[0])]; nullcount=0)
    sch = AC.Schema([AC.Field("ts", t; nullable=false)])
    tbl = Arrow.Table(Arrow.writefile(sch, [AC.RecordBatch(sch, [d])]))
    Z = Durations.ZonedTimestamp{Dates.Millisecond,Symbol("America/Denver")}
    @test eltype(tbl.ts) == Z
    @test tbl.ts[1] == Z(Durations.Timestamp{Dates.Millisecond}(1970), Dates.UTC)
    # instant operations need no zone rules; local-time ones name TimeZones
    @test tbl.ts[1] < tbl.ts[1] + Dates.Second(1)
    err = try
        Dates.hour(tbl.ts[1])
    catch e
        e
    end
    @test err isa ArgumentError && occursin("TimeZones.jl", err.msg)
end

@testset "ArrowTimeZonesExt child" begin
    @test success(
        `$(Base.julia_cmd()) --startup-file=no --project=$(Base.active_project()) $(joinpath(@__DIR__, "timezones_child.jl"))`,
    )
end
