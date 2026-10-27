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

# Files written by Polars (frozen bytes; regenerate with
# `python test/support/generate_polars_fixtures.py`).

using Test, Arrow, Tables, Dates, DataDecimals, Durations

const POLARS_FIXTURES = joinpath(@__DIR__, "fixtures-polars")

struct PolarsBytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::PolarsBytesSource) = length(s.data)
Arrow.readrange(s::PolarsBytesSource, off, len) = s.data[(off + 1):(off + len)]

const POLARS_EXPECTED = (
    i=[1, 2, missing],
    f=[1.5, missing, 3.0],
    s=["a", missing, "a much longer string value"],
    b=[true, false, missing],
    d=[Date(2024, 1, 1), missing, Date(2024, 1, 3)],
    ts=[
        Durations.Timestamp{Microsecond}(DateTime(2024, 1, 1, 12)),
        missing,
        Durations.Timestamp{Microsecond}(DateTime(2024, 1, 3)),
    ],
    dur=[Microsecond(5_000_000), missing, Microsecond(86_400_000_000)],
    t=[Time(1, 2, 3), missing, Time(23, 59)],
    dec=[
        reinterpret(Decimal{10,2,Int128}, Int128(125)),
        missing,
        reinterpret(Decimal{10,2,Int128}, Int128(-350)),
    ],
    cat=["x", "y", "x"],
    l=[[1, 2], missing, Int[]],
    bin=[UInt8[0x00, 0x01], missing, Vector{UInt8}("xyz")],
)

function polars_matches(t)
    Tables.columnnames(t) == collect(keys(POLARS_EXPECTED)) || return false
    for (nm, want) in pairs(POLARS_EXPECTED)
        col = Tables.getcolumn(t, nm)
        length(col) == length(want) || return false
        all(isequal(col[k], want[k]) for k in eachindex(want)) || return false
    end
    return true
end

@testset "Polars IPC files" begin
    # Polars writes the file format's leading Schema message as a bare
    # FlatBuffer; the footer carries the schema every reader uses.
    for name in ("default", "lz4", "zstd")
        path = joinpath(POLARS_FIXTURES, "$name.arrowbytes")
        bytes = read(path)
        @test bytes[9:12] != fill(0xff, 4)
        @test polars_matches(Arrow.Table(path))
        @test polars_matches(Arrow.Table(path; mmap=false))
        @test polars_matches(Arrow.Table(bytes))
        @test polars_matches(open(io -> Arrow.Table(io), path))
        @test polars_matches(only(collect(Arrow.Stream(path))))
        scanned =
            Arrow.Table(PolarsBytesSource(bytes); scan=Tables.Scan(select=(:s, :dec, :l)))
        @test isequal(collect(scanned.s), POLARS_EXPECTED.s)
        @test isequal(collect(scanned.dec), POLARS_EXPECTED.dec)
    end
    @test polars_matches(Arrow.Table(joinpath(POLARS_FIXTURES, "stream.arrowbytes")))

    # Polars' 128-bit integers (non-standard Int bitWidth 128).
    t = Arrow.Table(joinpath(POLARS_FIXTURES, "int128.arrowbytes"))
    @test eltype(t.i128) == Union{Missing,Int128}
    @test isequal(collect(t.i128), [1, -Int128(2)^100, missing])
    @test eltype(t.u128) == Union{Missing,UInt128}
    @test isequal(collect(t.u128), [UInt128(2)^127 + 5, UInt128(0), missing])
end
