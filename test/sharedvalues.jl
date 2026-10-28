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
        # Written as decimal128 (precision <= 38) or decimal256, whatever the
        # storage type, as pyarrow, Polars, and DuckDB write decimals.
        P = D.parameters[1]
        W = P <= 38 ? Int128 : DataDecimals.Int256
        @test getfield(table, :schema).fields[1].type.bits == 8sizeof(W)
        @test eltype(table.x) == Union{Missing,DataDecimals.Decimal{P,2,W}}
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

@testset "decimal columns write their storage directly" begin
    AC = Arrow.ArrowCore
    D = DataDecimals.Decimal128{2}
    n = 100_000
    plain = [reinterpret(D, Int128(i)) for i = 1:n]
    nullable = Union{Missing,D}[isodd(i) ? plain[i] : missing for i = 1:n]
    for x in (plain, nullable, view(plain, 2:n), Tables.partitioner([plain, plain]))
        tbl = x isa AbstractVector ? (; x=x) : Tables.partitioner([(; x=p) for p in x.x])
        io = IOBuffer()
        Arrow.write(io, tbl)
        expected = x isa AbstractVector ? x : vcat(plain, plain)
        @test isequal(Arrow.Table(take!(io)).x, expected)
    end
    # One allocation per row before: a coefficient byte vector per value.
    Arrow.write(IOBuffer(), (; x=nullable))
    @test @allocations(Arrow.write(IOBuffer(), (; x=nullable))) < 1_000
    @test @allocations(Arrow.write(IOBuffer(), (; x=plain))) < 1_000
    # Narrower storage widens to decimal128 in one pass, not per value.
    small = [reinterpret(DataDecimals.Decimal32{2}, Int32(i)) for i = 1:n]
    Arrow.write(IOBuffer(), (; x=small))
    @test @allocations(Arrow.write(IOBuffer(), (; x=small))) < 1_000
    io = IOBuffer()
    Arrow.write(io, (; x=small))
    @test Arrow.Table(take!(io)).x == small
    # Precision is advisory, as in Arrow C++: an out-of-precision coefficient
    # (only reachable through `reinterpret`) writes as stored.
    io = IOBuffer()
    Arrow.write(
        io,
        (; x=[reinterpret(DataDecimals.Decimal32{2}, Int32(1_000_000_000))]);
        file=true,
    )
    data = AC.rolebuffer(Arrow.readfile(take!(io))[1].columns[1], AC.DATA)
    @test AC.loadat(data, Int128, 0) == 1_000_000_000
end

@testset "decimals past their precision rewrite as stored" begin
    # Arrow C++ (pyarrow) writes such coefficients as stored, and so does a
    # read-then-rewrite, from the table or from plain values.
    AC = Arrow.ArrowCore
    for (bits, T) in ((32, Int32), (128, Int128))
        t = AC.DecimalType(3, 2, bits)
        stored = T[100_000, -7]   # 1000.00 has more digits than precision 3
        d = AC.ArrayData(t, 2, [AC.BufferSlice(), AC._databuffer(stored)])
        sch = AC.Schema([AC.Field("x", t; nullable=false)])
        src = Arrow.Table(Arrow.writefile(sch, [AC.RecordBatch(sch, [d], 2)]))
        @test DataDecimals.unscaled.(src.x) == stored
        for back in (src, (; x=collect(src.x)))
            again = Arrow.Table(Arrow.tobuffer(back))
            @test DataDecimals.unscaled.(again.x) == stored
        end
    end
end

@testset "small-precision decimals write as decimal128" begin
    # Arrow 2.x reads only decimal128/256 (it read decimal32/64 data as
    # garbage); pyarrow, Polars, and DuckDB write decimal128 here too.
    io = IOBuffer()
    Arrow.write(io, (price=[dec"1.25", dec"-3.10"], qty=Decimal{10,2}.(["1.50", "2.00"])))
    t = Arrow.Table(take!(io))
    @test [f.type.bits for f in getfield(t, :schema).fields] == [128, 128]
    @test t.price == [dec"1.25", dec"-3.10"]
    @test t.qty == Decimal{10,2}.(["1.50", "2.00"])
    # A rewrite keeps a retained decimal32 column's width.
    AC = Arrow.ArrowCore
    t32 = AC.DecimalType(9, 2, 32)
    d = AC.ArrayData(t32, 2, [AC.BufferSlice(), AC._databuffer(Int32[125, -310])])
    sch = AC.Schema([AC.Field("x", t32; nullable=false)])
    src = Arrow.Table(Arrow.writestream(sch, [AC.RecordBatch(sch, [d], 2)]))
    io = IOBuffer()
    Arrow.write(io, src)
    back = Arrow.Table(take!(io))
    @test getfield(back, :schema).fields[1].type.bits == 32
    @test back.x == [dec"1.25", dec"-3.10"]
end
