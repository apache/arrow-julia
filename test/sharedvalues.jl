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

using Test, Arrow, DataDecimals, Durations, DataStrings, Tables
@testset "Registered shared values" begin
    for D in (
        DataDecimals.Decimal32{2},
        DataDecimals.Decimal64{2},
        DataDecimals.Decimal128{2},
        DataDecimals.Decimal256{2},
    )
        values = Union{Missing,D}[D("12.34"), missing, D("-0.01")]
        io = IOBuffer()
        Arrow.write(io, (; x=values))
        bytes = take!(io)
        table = Arrow.Table(bytes)
        @test isequal(table.x, values)
        @test eltype(table.x) == eltype(values)
        selected = Arrow.Table(
            bytes;
            scan=Tables.Scan(filter=Tables.colcmp(>, Tables.col(:x), 20)),
        )
        @test isempty(selected.x)
        for sparse in (D[], Union{Missing,D}[missing, missing])
            buffer = IOBuffer()
            Arrow.write(buffer, (; x=sparse))
            @test isequal(Arrow.Table(take!(buffer)).x, sparse)
        end
        io = IOBuffer()
        Arrow.write(io, table)
        @test isequal(Arrow.Table(take!(io)).x, values)
    end
    values = Union{Missing,Durations.Duration}[
        Durations.Duration(2, -3, 4),
        missing,
        Durations.Duration(0, 0, -1),
    ]
    io = IOBuffer()
    Arrow.write(io, (; x=values))
    table = Arrow.Table(take!(io))
    @test isequal(table.x, values)
    io = IOBuffer()
    Arrow.write(io, table)
    @test isequal(Arrow.Table(take!(io)).x, values)
end

@test_throws ArgumentError Arrow.write(
    IOBuffer(),
    (; x=[reinterpret(DataDecimals.Decimal32{2}, Int32(1_000_000_000))]),
)
