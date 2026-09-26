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

# Writer symmetry for nested public values: facade rows carry public leaf
# values (Dates, Durations, DataDecimals) at every depth, and both retained
# rewrites and fresh composite writes lower them exactly or refuse.

module NestedValuesTests

using Test
using Dates
using Logging
using Arrow
import ArrowTypes, DataDecimals, Durations

const AC = Arrow.AC
const Ts = Durations.Timestamp
const Zts = Durations.ZonedTimestamp
const Denver = Zts{Microsecond,Symbol("America/Denver")}
const Dec64 = DataDecimals.Decimal{10,2,Int64}
const MonthDayNano = NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}}

# A registered logical Union. Its ArrowTypes-domain storage keeps a
# nanosecond timestamp branch as raw Int64 and a millisecond one as DateTime.
struct NestedStampOrLabel
    value::Union{Int64,DateTime,String}
end
Base.:(==)(a::NestedStampOrLabel, b::NestedStampOrLabel) = a.value == b.value
const NESTED_STAMP_OR_LABEL = Symbol("JuliaLang.ArrowTests.NestedStampOrLabel")
ArrowTypes.arrowname(::Type{NestedStampOrLabel}) = NESTED_STAMP_OR_LABEL
ArrowTypes.JuliaType(::Val{NESTED_STAMP_OR_LABEL}, S, metadata) = NestedStampOrLabel

# An identity extension over its storage: nested registrations of it build
# deeply nested isbits tuples whose restoration copies inline payload.
const NESTED_IDENTITY = Symbol("JuliaLang.ArrowTests.NestedIdentity")
ArrowTypes.JuliaType(::Val{NESTED_IDENTITY}, S, metadata) = S
ArrowTypes.ArrowType(::Type{NestedStampOrLabel}) = Union{Int64,DateTime,String}
ArrowTypes.toarrow(x::NestedStampOrLabel) = x.value
ArrowTypes.fromarrow(::Type{NestedStampOrLabel}, x) = NestedStampOrLabel(x)

validity(present) = AC._bitmapbuffer(present)

"One primitive-layout leaf over `storage`; `present` makes it nullable."
function leaf(name, t, storage::Vector; present=nothing)
    buffers = [present === nothing ? AC.BufferSlice() : validity(present)]
    push!(buffers, AC._databuffer(storage))
    nulls = present === nothing ? 0 : count(!, present)
    return AC.Field(name, t; nullable=present !== nothing),
    AC.ArrayData(t, length(storage), buffers; nullcount=nulls)
end

samefields(a, b) = length(a) == length(b) && all(Arrow._fieldcontractequal.(a, b))

"""
Core-built file -> Arrow.Table -> Arrow.tobuffer -> Arrow.Table. Checks that
both reads materialize `expected` and that the rewrite keeps every
descriptor; returns the first table.
"""
function checkrewrite(field, data, expected)
    schema = AC.Schema([field])
    table = Arrow.Table(Arrow.writefile(schema, [AC.RecordBatch(schema, [data])]))
    back = Arrow.Table(take!(Arrow.tobuffer(table)))
    name = Symbol(field.name)
    @test isequal(collect(getproperty(table, name)), expected)
    @test isequal(collect(getproperty(back, name)), expected)
    @test eltype(getproperty(back, name)) == eltype(getproperty(table, name))
    @test samefields(getfield(back, :schema).fields, getfield(table, :schema).fields)
    @test samefields(getfield(back, :schema).fields, [field])
    return table
end

replaced(table, column) = Arrow.Table(
    getfield(table, :names),
    AbstractVector[column],
    getfield(table, :lookup),
    getfield(table, :schema),
    AC.OwnerRegion[],
    length(column),
)

function refusal(f)
    try
        f()
    catch err
        return err
    end
    return nothing
end

ns(x) = reinterpret(Ts{Nanosecond}, Int64(x))
row(pairs...) = Pair{String,Any}[pairs...]
const StructRows = Union{Missing,Vector{Pair{String,Any}}}

const NS = AC.TimestampType(AC.NANOSECOND, nothing)
const ST = AC.StructType()

function stampedstruct()
    whenfield, whendata =
        leaf("when", NS, Int64[1, 0, 0, 4]; present=[true, false, false, true])
    field = AC.Field("s", ST; nullable=true, children=[whenfield])
    data = AC.ArrayData(
        ST,
        4,
        [validity([true, false, true, true])];
        children=[whendata],
        nullcount=1,
    )
    expected = StructRows[
        row("when" => ns(1)),
        missing,
        row("when" => missing),
        row("when" => ns(4)),
    ]
    return field, data, expected
end

@testset "nested public values: writer symmetry" begin
    @testset "retained struct{timestamp[ns]} with a null parent and null child" begin
        field, data, expected = stampedstruct()
        checkrewrite(field, data, expected)
    end

    @testset "retained struct{zoned timestamp} with a hidden non-null child" begin
        zoned = AC.TimestampType(AC.MICROSECOND, "America/Denver")
        atfield, atdata = leaf("at", zoned, Int64[10, 0, 30])
        field = AC.Field("z", ST; nullable=true, children=[atfield])
        data = AC.ArrayData(
            ST,
            3,
            [validity([true, false, true])];
            children=[atdata],
            nullcount=1,
        )
        expected = StructRows[
            row("at" => reinterpret(Denver, Int64(10))),
            missing,
            row("at" => reinterpret(Denver, Int64(30))),
        ]
        table = checkrewrite(field, data, expected)
        @test last(only(table.z[1])) isa Denver
    end

    @testset "retained list{date32} with null rows and null items" begin
        itemfield, itemdata = leaf(
            "item",
            AC.DateType(AC.DAY),
            Int32[0, 1, 0, 19000];
            present=[true, true, false, true],
        )
        lt = AC.ListType(false)
        field = AC.Field("days", lt; nullable=true, children=[itemfield])
        data = AC.ArrayData(
            lt,
            3,
            [validity([true, false, true]), AC._databuffer(Int32[0, 2, 2, 4])];
            children=[itemdata],
            nullcount=1,
        )
        epoch = Date(1970, 1, 1)
        expected = Union{Missing,Vector{Any}}[
            Any[epoch, epoch + Day(1)],
            missing,
            Any[missing, epoch + Day(19000)],
        ]
        checkrewrite(field, data, expected)
    end

    @testset "retained map{utf8 => duration[ms]} with a null row and value" begin
        keyfield, keydata = AC.fromjulia("key", ["a", "b", "c"])
        valuefield, valuedata = leaf(
            "value",
            AC.DurationType(AC.MILLISECOND),
            Int64[5, 0, 7];
            present=[true, false, true],
        )
        entries = AC.Field("entries", ST; nullable=false, children=[keyfield, valuefield])
        entriesdata = AC.ArrayData(
            ST,
            3,
            [AC.BufferSlice()];
            children=[keydata, valuedata],
            nullcount=0,
        )
        mt = AC.MapType(false)
        field = AC.Field("m", mt; nullable=true, children=[entries])
        data = AC.ArrayData(
            mt,
            3,
            [validity([true, false, true]), AC._databuffer(Int32[0, 2, 2, 3])];
            children=[entriesdata],
            nullcount=1,
        )
        expected = Union{Missing,Vector{Pair{Any,Any}}}[
            Pair{Any,Any}["a" => Millisecond(5), "b" => missing],
            missing,
            Pair{Any,Any}["c" => Millisecond(7)],
        ]
        checkrewrite(field, data, expected)
    end

    @testset "retained fixed-size-list{decimal64} with a hidden row" begin
        itemfield, itemdata =
            leaf("item", AC.DecimalType(10, 2, 64), Int64[125, -5, 0, 0, 999, 1])
        ft = AC.FixedSizeListType(2)
        field = AC.Field("amounts", ft; nullable=true, children=[itemfield])
        data = AC.ArrayData(
            ft,
            3,
            [validity([true, false, true])];
            children=[itemdata],
            nullcount=1,
        )
        dec(x) = reinterpret(Dec64, Int64(x))
        expected = Union{Missing,Vector{Any}}[
            Any[dec(125), dec(-5)],
            missing,
            Any[dec(999), dec(1)],
        ]
        checkrewrite(field, data, expected)
    end

    @testset "retained registered dense union{timestamp[ns], timestamp[ms, UTC], utf8}" begin
        # Plain retained Unions refuse (their type ids are not recoverable
        # from public values); a registered logical Union keeps its routes.
        nsfield, nsdata = leaf("0", NS, Int64[11, 22])
        msfield, msdata =
            leaf("1", AC.TimestampType(AC.MILLISECOND, "UTC"), Int64[86_400_000])
        strfield, strdata = AC.fromjulia("2", ["x"])
        ut = AC.UnionType(AC.DenseMode, Int8[0, 1, 2])
        field = AC.Field(
            "u",
            ut;
            nullable=false,
            metadata=[
                "ARROW:extension:name" => String(NESTED_STAMP_OR_LABEL),
                "ARROW:extension:metadata" => "",
            ],
            children=[nsfield, msfield, strfield],
        )
        data = AC.ArrayData(
            ut,
            4,
            [AC._databuffer(Int8[0, 2, 1, 0]), AC._databuffer(Int32[0, 0, 0, 1])];
            children=[nsdata, msdata, strdata],
            nullcount=0,
        )
        expected = NestedStampOrLabel.(Any[Int64(11), "x", DateTime(1970, 1, 2), Int64(22)])
        checkrewrite(field, data, expected)
    end

    @testset "retained run-end encoded interval with a null run" begin
        runfield, rundata = AC.fromjulia("run_ends", Int32[2, 3])
        valuefield, valuedata = leaf(
            "values",
            AC.IntervalType(AC.MONTH_DAY_NANO),
            MonthDayNano[MonthDayNano((1, 2, 3)), MonthDayNano((0, 0, 0))];
            present=[true, false],
        )
        rt = AC.RunEndEncodedType()
        field = AC.Field("r", rt; nullable=true, children=[runfield, valuefield])
        data = AC.ArrayData(
            rt,
            3,
            AC.BufferSlice[];
            children=[rundata, valuedata],
            nullcount=0,
        )
        span = Durations.Duration(1, 2, 3)
        checkrewrite(field, data, Union{Missing,Durations.Duration}[span, span, missing])
    end

    @testset "retained dictionary of struct{timestamp[ns]}" begin
        whenfield, whendata = leaf("when", NS, Int64[100, 200])
        pool = AC.ArrayData(ST, 2, [AC.BufferSlice()]; children=[whendata], nullcount=0)
        dt = AC.DictionaryType(AC.IntType(32, true), ST, false)
        field = AC.Field("d", dt; nullable=true, children=[whenfield])
        data = AC.ArrayData(
            dt,
            4,
            [validity([true, true, false, true]), AC._databuffer(Int32[1, 0, 0, 1])];
            dictionary=pool,
            nullcount=1,
        )
        expected = StructRows[
            row("when" => ns(200)),
            row("when" => ns(100)),
            missing,
            row("when" => ns(200)),
        ]
        checkrewrite(field, data, expected)
    end

    @testset "retained hidden slots on the non-masked storage path" begin
        # decimal128 storage is not isbits, and run-end values are a plain
        # vector, so both lower visible values one at a time.
        dt = AC.DecimalType(20, 3, 128)
        bytes = zeros(UInt8, 48)
        bytes[1] = 0x07
        bytes[33] = 0x09
        decfield = AC.Field("x", dt; nullable=false)
        decdata =
            AC.ArrayData(dt, 3, [AC.BufferSlice(), AC._databuffer(bytes)]; nullcount=0)
        runfield, rundata = AC.fromjulia("run_ends", Int32[1, 3])
        valuefield, valuedata = leaf("values", NS, Int64[5, 9])
        rt = AC.RunEndEncodedType()
        reefield = AC.Field("r", rt; nullable=false, children=[runfield, valuefield])
        reedata = AC.ArrayData(
            rt,
            3,
            AC.BufferSlice[];
            children=[rundata, valuedata],
            nullcount=0,
        )
        field = AC.Field("s", ST; nullable=true, children=[decfield, reefield])
        data = AC.ArrayData(
            ST,
            3,
            [validity([true, false, true])];
            children=[decdata, reedata],
            nullcount=1,
        )
        D = DataDecimals.Decimal{20,3,Int128}
        expected = StructRows[
            row("x" => reinterpret(D, Int128(7)), "r" => ns(5)),
            missing,
            row("x" => reinterpret(D, Int128(9)), "r" => ns(9)),
        ]
        checkrewrite(field, data, expected)
    end

    @testset "retained nested columns refuse replaced values" begin
        field, data, expected = stampedstruct()
        table = checkrewrite(field, data, expected)
        for bad in (
            # typed child path: a String is not a timestamp value
            StructRows[row("when" => "oops")],
            # a raw storage count is not a public timestamp value
            StructRows[row("when" => Int64(1))],
            # masked child path (a null parent hides one slot)
            StructRows[missing, row("when" => Int64(1))],
        )
            err = refusal(() -> Arrow.write(IOBuffer(), replaced(table, bad); file=false))
            @test err isa ArgumentError
            @test occursin("retained Arrow type", sprint(showerror, err))
        end
    end

    @testset "incremental writer and append keep nested public values" begin
        field, data, expected = stampedstruct()
        table = checkrewrite(field, data, expected)
        io = IOBuffer()
        w = Arrow.Writer(io; file=false)
        Arrow.write(w, table)
        Arrow.write(w, table)
        close(w)
        written = Arrow.Table(take!(io))
        @test isequal(collect(written.s), vcat(expected, expected))
        @test samefields(getfield(written, :schema).fields, [field])

        io = IOBuffer()
        Arrow.write(io, table; file=false)
        Arrow.append(io, table)
        appended = Arrow.Table(take!(io))
        @test isequal(collect(appended.s), vcat(expected, expected))
        @test samefields(getfield(appended, :schema).fields, [field])
    end

    @testset "fresh NamedTuple rows lower public leaf fields" begin
        rows = [
            (
                dt=DateTime(2026, 1, 2, 3, 4, 5),
                n=Ts{Nanosecond}(2026, 1, 1) + Nanosecond(7),
                d=Date(2026, 3, 4),
            ),
            (dt=DateTime(1969, 12, 31), n=Ts{Nanosecond}(1900, 1, 1), d=Date(1, 1, 1)),
        ]
        table = Arrow.Table(take!(Arrow.tobuffer((s=rows,))))
        field = only(getfield(table, :schema).fields)
        @test [child.type for child in field.children] == [
            AC.TimestampType(AC.MILLISECOND, nothing),
            AC.TimestampType(AC.NANOSECOND, nothing),
            AC.DateType(AC.DAY),
        ]
        @test [first.(r) for r in table.s] == [["dt", "n", "d"], ["dt", "n", "d"]]
        @test [last.(r) for r in table.s] == [collect(Any, r) for r in rows]
        @test last(table.s[1][1]) isa Ts{Millisecond}
        back = Arrow.Table(take!(Arrow.tobuffer(table)))
        @test isequal(collect(back.s), collect(table.s))
        @test samefields(getfield(back, :schema).fields, [field])
    end

    @testset "fresh nullable NamedTuple rows lower through the masked path" begin
        Row = @NamedTuple{at::DateTime, amount::Dec64, span::Durations.Duration}
        rows = Union{Missing,Row}[
            (at=DateTime(2026, 1, 1), amount=Dec64(1.25), span=Durations.Duration(1, 2, 3)),
            missing,
        ]
        table = Arrow.Table(take!(Arrow.tobuffer((s=rows,))))
        field = only(getfield(table, :schema).fields)
        @test [child.type for child in field.children] == [
            AC.TimestampType(AC.MILLISECOND, nothing),
            AC.DecimalType(10, 2, 64),
            AC.IntervalType(AC.MONTH_DAY_NANO),
        ]
        @test ismissing(table.s[2])
        @test last.(table.s[1]) == collect(Any, rows[1])
        back = Arrow.Table(take!(Arrow.tobuffer(table)))
        @test isequal(collect(back.s), collect(table.s))
    end

    @testset "fresh list of Timestamp round-trips" begin
        lists = [
            [Ts{Nanosecond}(2026, 1, 1), Ts{Nanosecond}(2026, 1, 2)],
            Ts{Nanosecond}[],
            missing,
        ]
        table = Arrow.Table(take!(Arrow.tobuffer((l=lists,))))
        field = only(getfield(table, :schema).fields)
        @test field.type == AC.ListType(false)
        @test only(field.children).type == AC.TimestampType(AC.NANOSECOND, nothing)
        @test isequal(collect(table.l), lists)
        back = Arrow.Table(take!(Arrow.tobuffer(table)))
        @test isequal(collect(back.l), lists)
        @test samefields(getfield(back, :schema).fields, [field])
    end

    @testset "fresh union and map children lower public values" begin
        values = Union{Ts{Nanosecond},String}[Ts{Nanosecond}(2026, 1, 1), "x"]
        table = Arrow.Table(take!(Arrow.tobuffer((u=values,))))
        field = only(getfield(table, :schema).fields)
        @test field.type isa AC.UnionType
        @test AC.TimestampType(AC.NANOSECOND, nothing) in [c.type for c in field.children]
        @test collect(table.u) == values

        maps = [Dict("a" => Millisecond(5))]
        mtable = Arrow.Table(take!(Arrow.tobuffer((m=maps,))))
        @test collect(mtable.m) == [Pair{Any,Any}["a" => Millisecond(5)]]
    end

    @testset "fresh nested temporal columns refuse like the top level" begin
        z1 = Denver(Ts{Microsecond}(2026, 1, 1), Dates.UTC)
        z2 = Durations.astimezone(z1, :UTC)
        ZRow = NamedTuple{(:z,),Tuple{Zts{Microsecond}}}
        for column in ([[z1, z2]], ZRow[(z=z1,), (z=z2,)])
            err = refusal(() -> Arrow.tobuffer((c=column,)))
            @test err isa ArgumentError
            @test occursin("astimezone", sprint(showerror, err))
        end
        err = refusal(() -> Arrow.tobuffer((c=[Ts[Ts{Nanosecond}(2026, 1, 1)]],)))
        @test err isa ArgumentError
        @test occursin("abstract Timestamp", sprint(showerror, err))
    end

    @testset "nested conversion consumes the reader allocation budget" begin
        n = 20_000
        function streamread(bytes, cap)
            stream = Arrow.Stream(
                copy(bytes);
                limits=Arrow.Limits(max_total_allocated_bytes=cap),
            )
            table = only(collect(stream))
            return table, stream.budget.limit - Arrow._remaining(stream.budget)
        end
        function corebytes(field, data)
            schema = AC.Schema([field])
            return Arrow.writestream(schema, [AC.RecordBatch(schema, [data])])
        end
        decfield, decdata = leaf("x", AC.DecimalType(10, 2, 64), fill(Int64(70000), n))
        decbytes = corebytes(AC.Field("x", decfield.type), decdata)

        tsfield, tsdata = leaf("0", NS, collect(Int64, 1:n))
        ut = AC.UnionType(AC.DenseMode, Int8[1])
        unionbytes = corebytes(
            AC.Field("x", ut; nullable=false, children=[tsfield]),
            AC.ArrayData(
                ut,
                n,
                [AC._databuffer(ones(Int8, n)), AC._databuffer(Int32.(0:(n - 1)))];
                children=[tsdata],
                nullcount=0,
            ),
        )

        runsfield, runsdata = AC.fromjulia("run_ends", Int32.(1:n))
        reetype = AC.RunEndEncodedType()
        valfield, valdata = leaf("values", NS, collect(Int64, 1:n))
        unknown = [
            "ARROW:extension:name" => "JuliaLang.ArrowTests.NestedUnknown",
            "ARROW:extension:metadata" => "",
        ]
        reebytes = corebytes(
            AC.Field(
                "x",
                reetype;
                nullable=false,
                metadata=unknown,
                children=[runsfield, valfield],
            ),
            AC.ArrayData(
                reetype,
                n,
                AC.BufferSlice[];
                children=[runsdata, valdata],
                nullcount=0,
            ),
        )

        lifted = [NestedStampOrLabel(isodd(i) ? Int64(i) : "s$(i)") for i = 1:n]
        liftedbytes = take!(Arrow.tobuffer((x=lifted,)))

        # Fifteen singleton fixed-size-list identity registrations around a
        # 128-float leaf: every level's restored tuple copies the full
        # inline payload, so charges must scale with the row's byte size.
        fslrows, fslwidth, fsldepth = 500, 128, 15
        identity_meta = [
            "ARROW:extension:name" => String(NESTED_IDENTITY),
            "ARROW:extension:metadata" => "",
        ]
        fslfield, fsldata = AC.fromjulia("item", fill(1.5, fslrows * fslwidth))
        fslexpected = ntuple(_ -> 1.5, fslwidth)
        for level = 1:fsldepth
            width = level == 1 ? fslwidth : 1
            t = AC.FixedSizeListType(width)
            fslfield = AC.Field(
                "x",
                t;
                nullable=false,
                metadata=identity_meta,
                children=[
                    AC.Field(
                        "item",
                        fslfield.type;
                        nullable=fslfield.nullable,
                        metadata=fslfield.metadata,
                        children=fslfield.children,
                    ),
                ],
            )
            fsldata = AC.ArrayData(
                t,
                fslrows,
                [AC.BufferSlice()];
                children=[fsldata],
                nullcount=0,
            )
            level == 1 || (fslexpected = (fslexpected,))
        end
        fslschema = AC.Schema([fslfield])
        fslbytes = Arrow.writestream(fslschema, [AC.RecordBatch(fslschema, [fsldata])])
        # Two payload copies per level per row is the reserve model.
        fslfloor = 2 * fslrows * fsldepth * 8 * fslwidth

        # Each dynamic-conversion seam reserves a fixed per-value overhead
        # (`_publicvalue`'s converted leaf, the shared-decimal `_postconvert`,
        # and the ArrowTypes raw-domain and fromarrow tails), and composite
        # restorations reserve the row's inline payload besides. The floors
        # below fail if a reserve is dropped.
        with_logger(NullLogger()) do
            for (name, bytes, floor, first_expected) in (
                ("flat decimal", decbytes, 64n, reinterpret(Dec64, Int64(70000))),
                ("union timestamp", unionbytes, 112n, ns(1)),
                ("unknown-label run-end timestamp", reebytes, 160n, ns(1)),
                ("registered fromarrow union", liftedbytes, 224n, lifted[1]),
                ("nested fixed-size-list identity", fslbytes, fslfloor, fslexpected),
            )
                table, charged = streamread(bytes, typemax(Int64))
                @test isequal(first(table.x), first_expected)
                @test charged > floor
                # The charge is the security bound: a cap below it refuses,
                # and the exact charge admits the read.
                @test_throws Arrow.AllocationLimitError streamread(bytes, charged ÷ 2)
                streamread(bytes, charged)
                @static if VERSION >= v"1.12"
                    # Warmed, the read's true allocation stays within its
                    # charge (boxing differs on older runtimes, so the
                    # allocation pin runs on the tested-current one).
                    streamread(bytes, charged)
                    GC.gc()
                    allocated = @allocated streamread(bytes, charged)
                    @test allocated <= charged + 200_000
                end
            end
        end
    end
end

end # module
