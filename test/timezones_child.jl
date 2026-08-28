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

# ArrowTimeZonesExt coverage. Loading TimeZones.jl activates the extension
# for the whole process, so compat_tests.jl re-execs this file as a child:
# the parent process stays extension-free and keeps testing the naive reads.
module TimeZonesExtChild

using Test, Dates, Tables, TimeZones
import Arrow
const AC = Arrow.ArrowCore

# One single-column file-format buffer with a timestamp descriptor built
# directly, since the facade writer has no fresh ZonedDateTime route yet.
function tzfile(unit, tz, values::Vector{Int64}; validity=nothing)
    t = AC.TimestampType(unit, tz)
    bufs =
        validity === nothing ? [AC.BufferSlice(), AC._databuffer(values)] :
        [AC._databuffer(validity), AC._databuffer(values)]
    d = AC.ArrayData(t, length(values), bufs; nullcount=validity === nothing ? 0 : -1)
    f = AC.Field("ts", t; nullable=validity !== nothing)
    sch = AC.Schema([f])
    return Arrow.writefile(sch, [AC.RecordBatch(sch, [d])])
end

@testset "ArrowTimeZonesExt" begin
    @test Base.get_extension(Arrow, :ArrowTimeZonesExt) !== nothing
    denver = tz"America/Denver"

    @testset "millisecond reads as ZonedDateTime" begin
        t = Arrow.Table(tzfile(AC.MILLISECOND, "America/Denver", Int64[0, 1_000]))
        @test eltype(t.ts) == ZonedDateTime
        @test t.ts[1] == ZonedDateTime(DateTime(1970, 1, 1), denver; from_utc=true)
        @test t.ts[2] == ZonedDateTime(DateTime(1970, 1, 1, 0, 0, 1), denver; from_utc=true)
    end

    @testset "second unit scales" begin
        t = Arrow.Table(tzfile(AC.SECOND, "UTC", Int64[42]))
        @test eltype(t.ts) == ZonedDateTime
        @test t.ts[1] ==
              ZonedDateTime(DateTime(1970, 1, 1, 0, 0, 42), tz"UTC"; from_utc=true)
    end

    @testset "fixed-offset zone" begin
        t = Arrow.Table(tzfile(AC.MILLISECOND, "+07:00", Int64[0]))
        @test eltype(t.ts) == ZonedDateTime
        @test DateTime(t.ts[1], TimeZones.UTC) == DateTime(1970, 1, 1)
    end

    @testset "unparseable zone warns and reads naive" begin
        bytes = tzfile(AC.MILLISECOND, "Bogus/Nowhere", Int64[0])
        t = @test_logs (:warn, r"cannot parse") match_mode = :any Arrow.Table(bytes)
        @test eltype(t.ts) == DateTime
        @test t.ts[1] == DateTime(1970, 1, 1)
    end

    @testset "finer units keep raw storage" begin
        t = Arrow.Table(tzfile(AC.MICROSECOND, "America/Denver", Int64[7]))
        @test eltype(t.ts) == Int64
        @test t.ts[1] == 7
    end

    @testset "nulls widen with Missing" begin
        t = Arrow.Table(tzfile(AC.MILLISECOND, "UTC", Int64[0, 0]; validity=UInt8[0x01]))
        @test eltype(t.ts) == Union{Missing,ZonedDateTime}
        @test t.ts[2] === missing
    end

    @testset "retained rewrite round-trips" begin
        t = Arrow.Table(tzfile(AC.MILLISECOND, "America/Denver", Int64[0, 1_000]))
        io = IOBuffer()
        Arrow.write(io, t)
        t2 = Arrow.Table(take!(io))
        @test eltype(t2.ts) == ZonedDateTime
        @test t2.ts == t.ts
    end

    @testset "second-unit rewrite stays exact" begin
        t = Arrow.Table(tzfile(AC.SECOND, "UTC", Int64[42]))
        io = IOBuffer()
        Arrow.write(io, t)
        t2 = Arrow.Table(take!(io))
        @test t2.ts == t.ts
    end

    @testset "fresh ZonedDateTime columns write as timestamps" begin
        zs = [
            ZonedDateTime(DateTime(2020, 1, 1, 12), denver),
            ZonedDateTime(DateTime(2020, 1, 2, 12), denver),
        ]
        io = IOBuffer()
        Arrow.write(io, (; ts=zs))
        t = Arrow.Table(take!(io))
        @test eltype(t.ts) == ZonedDateTime
        @test t.ts == zs
        mixed =
            [ZonedDateTime(DateTime(2020), denver), ZonedDateTime(DateTime(2020), tz"UTC")]
        @test_throws ArgumentError Arrow.write(IOBuffer(), (; ts=mixed))
        io = IOBuffer()
        Arrow.write(io, (; ts=[zs[1], missing]))
        t2 = Arrow.Table(take!(io))
        @test eltype(t2.ts) == Union{Missing,ZonedDateTime}
        @test t2.ts[1] == zs[1]
        @test t2.ts[2] === missing
    end

    @testset "scan filter lowers a ZonedDateTime literal" begin
        bytes = tzfile(AC.MILLISECOND, "America/Denver", Int64[0, 1_000, 2_000])
        want = ZonedDateTime(DateTime(1970, 1, 1, 0, 0, 1), denver; from_utc=true)
        t = Arrow.Table(
            bytes;
            scan=Tables.Scan(filter=Tables.colcmp(==, Tables.col(:ts), want)),
        )
        @test length(t.ts) == 1
        @test t.ts[1] == want
    end
end

end # module TimeZonesExtChild
