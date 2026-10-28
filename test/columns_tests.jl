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

# Read-side column construction for flat layouts.

using Test, Arrow, Tables, DataAPI, Dates, DataDecimals, Durations, PooledArrays
using DataStrings: DataString, DataBytes, StringVector, BytesVector
isdefined(Main, :rmmapped) || include(joinpath(@__DIR__, "support", "mapped.jl"))

struct ColumnsBytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::ColumnsBytesSource) = length(s.data)
Arrow.readrange(s::ColumnsBytesSource, off, len) = s.data[(off + 1):(off + len)]

# Bytes `f(x)` allocates once compiled, measured inside a function so the
# call is not a dynamic one.
allocationsof(f, x) = (f(x); @allocated f(x))
# The fewest bytes `f()` allocates over three calls: a collection that runs
# finalizers or other tasks during one measurement adds to that one only.
fewestbytes(f) = minimum(_ -> (@allocated f()), 1:3)

function arrowbytes(table; kw...)
    io = IOBuffer()
    Arrow.write(io, table; kw...)
    return take!(io)
end

# Reads of a mapped file run in their own frame, so no temporary outlives
# them and garbage collection can unmap the file before it is deleted.
function mappedviews(path, n)
    # Columns outlive their table: drop it and collect garbage.
    x = Arrow.Table(path; mmap=true).x
    s = Arrow.Table(path; mmap=true).s
    for _ = 1:3
        GC.gc(true)
    end
    @test sum(x) == n * (n + 1) ÷ 2 && s[n] == string(n)
    # Release ends the table, not its taken columns.
    t = Arrow.Table(path; mmap=true)
    y = t.x
    Arrow.release!(t)
    @test_throws InvalidStateException t.x
    @test_throws InvalidStateException copy(t)
    GC.gc(true)
    @test y[end] == n
    # Replacing a file that live columns map keeps the old contents (Windows
    # too: the writer renames a new file over the mapped one).
    Arrow.write(path, Arrow.Table(path; mmap=true))
    Arrow.write(path, (x=collect((-n):-1),); file=true)
    GC.gc(true)
    @test x[1] == 1 && y[1] == 1 && Arrow.Table(path).x[1] == -n
    return nothing
end

function mappedentrypoints(tbl, bytes, path)
    for t in (
        Arrow.Table(bytes),
        Arrow.Table(IOBuffer(bytes)),
        Arrow.Table(path; mmap=true),
        Arrow.Table(path; mmap=false),
        only(collect(Arrow.Stream(bytes))),
        only(collect(Arrow.Stream(path; mmap=true))),
    )
        @test t.x isa Arrow.Primitive && isequal(collect(t.x), tbl.x)
        @test t.s isa StringVector && t.s == tbl.s
        @test t.d isa Arrow.Primitive && t.d == tbl.d
    end
    return nothing
end

@testset "column reads" begin
    @testset "decimals read in bulk, as stored" begin
        n = 100_000
        for (P, S, T) in
            ((9, 2, Int32), (18, 3, Int64), (38, 4, Int128), (76, 5, DataDecimals.Int256))
            D = Decimal{P,S,T}
            vals = [reinterpret(D, T(i) * T(7) - T(3)) for i = 1:n]
            nullable = Union{Missing,D}[isodd(i) ? vals[i] : missing for i = 1:n]
            for col in (vals, nullable)
                io = IOBuffer()
                Arrow.write(io, (d=col,))
                bytes = take!(io)
                t = Arrow.Table(bytes)
                @test eltype(t.d) == eltype(col)
                @test isequal(collect(t.d), col)
                # One bulk copy per column: no allocation per value.
                @test (@allocations Arrow.Table(bytes)) < 10_000
            end
        end
        # A decimal no `Decimal` type holds (a negative scale, or a scale above
        # the precision) is a view of its storage: integers, or the stored bytes
        # at 128 and 256 bits. Nested in a list, it reads as typed rows.
        AC = Arrow.AC
        bytesof(x) = collect(reinterpret(UInt8, [x]))
        lt = AC.ListType(false)
        for (t, raw) in (
            (AC.DecimalType(5, -1, 32), Int32[99_999, -7]),
            (AC.DecimalType(12, -3, 64), Int64[-999_999_999_999, 0]),
            (AC.DecimalType(10, -2, 128), Int128[123, -5]),
            (AC.DecimalType(2, 4, 256), DataDecimals.Int256[1, -1]),
        )
            want = eltype(raw) <: Union{Int32,Int64} ? raw : bytesof.(raw)
            df = AC.Field("d", t; nullable=false)
            dd = AC.ArrayData(t, 2, [AC.BufferSlice(), AC._databuffer(raw)]; nullcount=0)
            lf = AC.Field(
                "l",
                lt;
                nullable=false,
                children=[AC.Field("item", t; nullable=false)],
            )
            offsets = AC._databuffer(Int32[0, 2, 2])
            ld =
                AC.ArrayData(lt, 2, [AC.BufferSlice(), offsets]; children=[dd], nullcount=0)
            sch = AC.Schema([df, lf])
            tbl = Arrow.Table(Arrow.writefile(sch, [AC.RecordBatch(sch, [dd, ld])]))
            @test tbl.d isa Arrow.Primitive && tbl.d == want
            @test tbl.l[1] isa Arrow.ListValue && tbl.l[1] == want && isempty(tbl.l[2])
            back = Arrow.readfile(take!(Arrow.write(IOBuffer(), tbl)))
            @test [f.type for f in back.schema.fields] == [t, lt]
            @test only(back.schema.fields[2].children).type == t
            @test Arrow.Table(back).d == want
        end
    end

    @testset "flat columns are read-only views" begin
        cols = (
            i=Int32[1, -2, 3],
            f=Union{Missing,Float64}[1.5, missing, -0.0],
            b=Union{Missing,Bool}[true, missing, false],
            d=[Date(2024, 1, 1), Date(1969, 12, 31), Date(2000, 2, 29)],
            t=[Time(0), Time(23, 59, 59, 999), Time(12)],
            ts=[DateTime(2024, 1, 1, 12), DateTime(1970), DateTime(1900, 3, 4)],
            dur=Union{Missing,Millisecond}[Millisecond(5), missing, Millisecond(-7)],
            dec=[reinterpret(Decimal{10,2,Int64}, Int64(k)) for k in (125, -350, 0)],
            iv=[
                Durations.Duration(1, 2, 3),
                Durations.Duration(0, 0, 0),
                Durations.Duration(-1, 5, 7),
            ],
            u=UInt8[0x00, 0xff, 0x7f],
        )
        bytes = arrowbytes(cols)
        t = Arrow.Table(bytes)
        for (nm, want) in pairs(cols)
            col = Tables.getcolumn(t, nm)
            @test col isa Arrow.ArrowVector
            @test isequal(collect(col), collect(want))
            @test_throws ErrorException col[1] = col[2]
            owned = copy(col)
            @test owned isa Vector && isequal(owned, collect(col))
        end
        @test t.ts isa Arrow.Primitive{Durations.Timestamp{Millisecond}}
        @test t.b isa Arrow.BoolVector{Union{Missing,Bool}}
        # Strings and binary are DataStrings columns; `stringtype=String` copies.
        sb = arrowbytes((s=Union{Missing,String}["a", missing, "a much longer string"],))
        st = Arrow.Table(sb)
        @test st.s isa StringVector{Union{Missing,DataString}}
        @test isequal(st.s, ["a", missing, "a much longer string"])
        binvalues = [UInt8[1, 2], UInt8[], collect(0x00:0x20)]
        bf = Arrow.AC.Field("bin", Arrow.AC.BinaryType(false); nullable=false)
        bd = Arrow.AC.ArrayData(
            bf.type,
            3,
            [
                Arrow.AC.BufferSlice(),
                Arrow.AC._databuffer(Int32[0, 2, 2, 35]),
                Arrow.AC._databuffer(reduce(vcat, binvalues)),
            ];
            nullcount=0,
        )
        bsch = Arrow.AC.Schema([bf])
        bin = Arrow.Table(Arrow.writestream(bsch, [Arrow.AC.RecordBatch(bsch, [bd])])).bin
        @test bin isa BytesVector{DataBytes} && bin == binvalues
        ss = Arrow.Table(sb; stringtype=String)
        @test ss.s isa Vector{Union{Missing,String}}
        @test isequal(ss.s, ["a", missing, "a much longer string"])
        @test_throws ArgumentError Arrow.Table(sb; stringtype=Symbol)
        # Null columns.
        @test Arrow.Table(arrowbytes((m=[missing, missing],))).m isa Vector{Missing}
    end

    @testset "multi-batch columns chain per-batch views" begin
        parts = Tables.partitioner([
            (x=Int64[1, 2], s=["a", "bb"], c=PooledArray(["u", "v"])),
            (x=Int64[3], s=["a fairly long string value"], c=PooledArray(["v"])),
        ])
        for file in (true, false)
            t = Arrow.Table(arrowbytes(parts; file=file))
            # Several partitions declare every field nullable.
            @test t.x isa Arrow.ChainedVector{Union{Missing,Int64},<:Arrow.Primitive}
            @test t.x == [1, 2, 3] &&
                  sum(t.x) == 6 &&
                  copy(t.x) isa Vector{Union{Missing,Int64}}
            # One StringVector spans every batch.
            @test t.s isa StringVector && t.s == ["a", "bb", "a fairly long string value"]
            @test collect(t.c) == ["u", "v", "v"]
        end
        # Empty batches chain to a vector with no chunks; scans still slice it.
        empty = arrowbytes(
            Tables.partitioner([(x=Int8[], b=Bool[]), (x=Int8[], b=Bool[])]);
            file=false,
        )
        @test length(collect(Arrow.Stream(empty))) == 2
        for source in (empty, ColumnsBytesSource(empty))
            s = Arrow.Table(source; scan=Tables.Scan())
            @test isempty(s.x) && eltype(s.x) == Union{Missing,Int8} && isempty(s.b)
        end
        t = Arrow.Table(empty)
        @test t.x[Int[]] == Int8[] && similar(t.x, 2) isa Vector{Union{Missing,Int8}}
    end

    @testset "dictionary columns expose their encoding" begin
        pool = PooledArray(Union{Missing,String}["lo", "hi", missing, "lo", "hi"])
        t = Arrow.Table(arrowbytes((c=pool,)))
        c = t.c
        @test c isa Arrow.DictEncoded
        @test isequal(collect(c), collect(pool))
        refs, rp = DataAPI.refarray(c), DataAPI.refpool(c)
        @test all(isequal(rp[refs[i]], c[i]) for i in eachindex(c))
        @test firstindex(rp) == 0 && rp[0] === missing
        @test DataAPI.levels(c) == ["lo", "hi"]
        owned = copy(c)
        @test owned isa PooledArray && isequal(owned, collect(c))
        owned[1] = "new"
        @test owned[1] == "new" && c[1] == "lo"
    end

    @testset "run-end encoded columns read lazily" begin
        rf, rd = Arrow.AC.fromjulia("run_ends", Int32[2, 5])
        vf, vd = Arrow.AC.fromjulia("values", Int64[7, 9])
        ree = Arrow.AC.RunEndEncodedType()
        f = Arrow.AC.Field("r", ree; nullable=false, children=[rf, vf])
        d = Arrow.AC.ArrayData(
            ree,
            5,
            Arrow.AC.BufferSlice[];
            children=[rd, vd],
            nullcount=0,
        )
        sch = Arrow.AC.Schema([f])
        t = Arrow.Table(Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d])]))
        @test t.r isa Arrow.RunEndEncoded{Int64}
        @test t.r == [7, 7, 9, 9, 9] && copy(t.r) == [7, 7, 9, 9, 9]
    end

    @testset "time-of-day values outside a day name the column" begin
        f = Arrow.AC.Field("late", Arrow.AC.TimeType(Arrow.AC.SECOND, 32); nullable=false)
        d = Arrow.AC.ArrayData(
            f.type,
            1,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[86_400])];
            nullcount=0,
        )
        sch = Arrow.AC.Schema([f])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d])])
        err = try
            Arrow.Table(bytes)
        catch e
            e
        end
        @test err isa Arrow.ValidationError && occursin("\"late\"", err.msg)
        # The error names a scan that reads the stored values as periods.
        @test occursin("Tables.Scan(select=(:late => Dates.Second,))", err.msg)
        ids = Arrow.AC.Field("id", Arrow.AC.IntType(64, true); nullable=false)
        idd = Arrow.AC.ArrayData(
            ids.type,
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[1, 2])];
            nullcount=0,
        )
        d2 = Arrow.AC.ArrayData(
            f.type,
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[86_400, 5])];
            nullcount=0,
        )
        sch2 = Arrow.AC.Schema([f, ids])
        stream = Arrow.writestream(sch2, [Arrow.AC.RecordBatch(sch2, [d2, idd])])
        file = Arrow.writefile(sch2, [Arrow.AC.RecordBatch(sch2, [d2, idd])])
        for src in (stream, file, ColumnsBytesSource(file))
            t = Arrow.Table(src; scan=Tables.Scan(select=(:late => Second,)))
            @test t.late == [Second(86_400), Second(5)]
            t = Arrow.Table(src; scan=Tables.Scan(select=(:late => Millisecond, :id)))
            @test t.late == [Millisecond(86_400_000), Millisecond(5000)] && t.id == [1, 2]
            t = Arrow.Table(
                src;
                scan=Tables.Scan(select=(:late => Second,), filter=Tables.col(:id) > 1),
            )
            @test t.late == [Second(5)]
        end
    end

    @testset "fixed-shape tensors read as arrays" begin
        AC = Arrow.AC
        meta(json) = [
            "ARROW:extension:name" => "arrow.fixed_shape_tensor",
            "ARROW:extension:metadata" => json,
        ]
        cf, cd = AC.fromjulia("item", Int32.(0:11))
        cf2, cd2 = AC.fromjulia("item", Int32.(0:11))
        tf = AC.Field(
            "t",
            AC.FixedSizeListType(6);
            nullable=true,
            metadata=meta("{\"shape\":[2,3]}"),
            children=[cf],
        )
        pf = AC.Field(
            "p",
            AC.FixedSizeListType(6);
            nullable=true,
            metadata=meta("{\"shape\":[2,3],\"permutation\":[1,0]}"),
            children=[cf2],
        )
        td = AC.ArrayData(AC.FixedSizeListType(6), 2, [AC.BufferSlice()]; children=[cd])
        pd = AC.ArrayData(AC.FixedSizeListType(6), 2, [AC.BufferSlice()]; children=[cd2])
        sch = AC.Schema([tf, pf])
        bytes = Arrow.writestream(sch, [AC.RecordBatch(sch, [td, pd], 2)])
        t = Arrow.Table(bytes)
        # Row-major storage over the physical shape [2, 3].
        @test eltype(t.t) == Union{Missing,Matrix{Int32}}
        @test t.t[2] == Int32[6 7 8; 9 10 11]
        # Logical dimension 1 is physical dimension 2.
        @test t.p[1] == Int32[0 3; 1 4; 2 5]
        # A rewrite stores the rows back in row-major order.
        again = Arrow.Table(arrowbytes(t))
        @test isequal(collect(again.t), collect(t.t)) &&
              isequal(collect(again.p), collect(t.p))
    end

    @testset "views keep their memory alive" begin
        n = 1_000_000
        path = tempname()
        try
            Arrow.write(path, (x=collect(1:n), s=string.(1:n)); file=true)
            mappedviews(path, n)
        finally
            rmmapped(path)
        end
        @test Arrow._MMAP_DEFAULT == !Sys.iswindows()
        # Tasks read columns whose table is gone while collections run.
        bytes = arrowbytes(Tables.partitioner([(v=collect(1:1000) .+ k,) for k = 0:3]))
        cols = [Arrow.Table(copy(bytes)).v for _ = 1:4]
        tasks = [errormonitor(Threads.@spawn begin
            total = 0
            for _ = 1:200
                total += sum(col)
                yield()
            end
            total
        end) for col in cols]
        for _ = 1:20
            GC.gc(false)
            yield()
        end
        @test all(fetch(t) == 200 * sum(cols[1]) for t in tasks)
    end

    @testset "every read entry point gives the same views" begin
        tbl = (
            x=Union{Missing,Int64}[1, missing, 3],
            s=["a", "b", "c"],
            d=[Date(2024), Date(2025), Date(2026)],
        )
        for file in (true, false)
            bytes = arrowbytes(tbl; file=file)
            path = tempname()
            write(path, bytes)
            try
                mappedentrypoints(tbl, bytes, path)
                # Scans give the element types of a full read.
                full = Arrow.Table(bytes)
                for source in (bytes, ColumnsBytesSource(bytes))
                    for scan in (
                        Tables.Scan(select=(:s, :x)),
                        Tables.Scan(filter=Tables.colcmp(>, Tables.col(:x), 1)),
                        Tables.Scan(filter=Tables.colcmp(==, Tables.col(:x), 99)),
                    )
                        st = Arrow.Table(source; scan=scan)
                        for nm in Tables.columnnames(st)
                            @test eltype(Tables.getcolumn(st, nm)) ==
                                  eltype(Tables.getcolumn(full, nm))
                        end
                    end
                end
            finally
                rmmapped(path)
            end
        end
    end

    @testset "a scan that only selects reads as a full read does" begin
        part = (x=collect(1:3), s=["a", "b", "c"], d=Date.(2020:2022))
        bytes = arrowbytes(Tables.partitioner([part, part]); file=true)
        full = Arrow.Table(bytes)
        st = Arrow.Table(bytes; scan=Tables.Scan(select=(:d, :s, :x)))
        for nm in (:x, :s, :d)
            @test typeof(Tables.getcolumn(st, nm)) == typeof(Tables.getcolumn(full, nm))
            @test Tables.getcolumn(st, nm) == Tables.getcolumn(full, nm)
        end
        # A temporal filter runs on the stored integers and returns dates.
        filter = Tables.colcmp(>=, Tables.col(:d), Date(2021))
        ft = Arrow.Table(bytes; scan=Tables.Scan(filter=filter))
        @test ft.d == Date.([2021, 2022, 2021, 2022]) && ft.x == [2, 3, 2, 3]
        # Selecting views allocates independently of the file's size.
        large = arrowbytes((a=collect(1:2_000_000), b=rand(2_000_000)); file=true)
        sel = Tables.Scan(select=(:a,))
        Arrow.Table(large; scan=sel)
        @test (@allocated Arrow.Table(large; scan=sel)) < 200_000
    end

    @testset "fixed-size binary wider than 256 bytes reads as DataBytes" begin
        AC = Arrow.AC
        for (w, T) in ((256, NTuple{256,UInt8}), (300, Arrow.DataStrings.DataBytes))
            data = UInt8[i % 251 for i = 1:(3w)]
            t = AC.FixedSizeBinaryType(w)
            f = AC.Field("b", t; nullable=true)
            valid = AC._bitmapbuffer(Bool[true, false, true])
            d = AC.ArrayData(t, 3, [valid, AC._databuffer(data)]; nullcount=1)
            s = AC.Schema([f])
            tbl = Arrow.Table(Arrow.writestream(s, [AC.RecordBatch(s, [d], 3)]))
            @test eltype(tbl.b) == Union{Missing,T}
            @test tbl.b[2] === missing && collect(tbl.b[3]) == data[(2w + 1):(3w)]
            # A rewrite keeps the fixed-size layout.
            again = Arrow.Table(arrowbytes(tbl))
            @test isequal(again.b, tbl.b) && getfield(again, :schema).fields[1].type == t
        end
    end

    @testset "nested columns read as typed rows" begin
        Row = Arrow.ListValue
        tbl = (
            l=Union{Missing,Vector{Int64}}[[1, 2], missing, Int64[]],
            ls=[["a"], String[], ["b", "c"]],
            st=[(a=1, b="x"), (a=2, b="y"), (a=3, b="z")],
            m=[Dict("k" => 1), Dict{String,Int64}(), Dict("p" => 2)],
            t=[(Int16(1), Int16(2)), (Int16(3), Int16(4)), (Int16(5), Int16(6))],
            ll=[[[1], [2, 3]], Vector{Int64}[], [Int64[]]],
        )
        t = Arrow.Table(arrowbytes(tbl))
        @test eltype(t.l) == Union{Missing,Row{Int64}}
        @test isequal(collect(t.l), tbl.l)
        @test eltype(t.ls) == Row{DataString} && t.ls == tbl.ls
        @test eltype(t.st) == @NamedTuple{a::Int64, b::DataString} && t.st == tbl.st
        @test eltype(t.m) == Arrow.MapValue{DataString,Int64}
        @test t.m == [["k" => 1], Pair{String,Int64}[], ["p" => 2]]
        # The element type alone fixes how children are stored: strings in a
        # StringVector, fixed-width values in a Primitive view, structs lazily
        # in a StructVector, maps as key and value columns, so no value is boxed.
        @test t.ls isa Arrow.ListVector && t.ls.values isa StringVector{DataString}
        @test t.st isa Arrow.StructVector &&
              t.st.children[1] isa Arrow.Primitive{Int64,Int64,Nothing} &&
              t.st.children[2] isa StringVector{DataString}
        @test t.m isa Arrow.MapVector && t.m.keys isa StringVector{DataString}
        @test t.ll.values isa Arrow.ListVector{Row{Int64}}
        @test eltype(t.t) == NTuple{2,Int16} && t.t == tbl.t
        @test eltype(t.ll) == Row{Row{Int64}} && t.ll == tbl.ll
        # Rows outlive their table; copies hold rows as their own vectors.
        row = Arrow.Table(arrowbytes(tbl)).l[1]
        GC.gc(true)
        @test row == [1, 2]
        @test copy(t.l) isa Vector{Union{Missing,Vector{Int64}}} &&
              isequal(copy(t.l), tbl.l)
        @test copy(t.m) isa Vector{Vector{Pair{DataString,Int64}}} &&
              copy(t.st) isa Vector{@NamedTuple{a::Int64,b::DataString}}
        # Several batches chain, and copy the same way.
        two = Arrow.Table(arrowbytes(Tables.partitioner([tbl, tbl]); file=true))
        @test isequal(collect(two.l), [tbl.l; tbl.l]) && two.st == [tbl.st; tbl.st]
        @test copy(two.l) isa Vector && all(r -> r === missing || r isa Vector, copy(two.l))
        # A struct row's fixed-size binary field is an `NTuple`, and the
        # struct rewrites from it.
        ids = Arrow.Table(arrowbytes((s=[(id=ntuple(UInt8, 16), n=1), missing],)))
        @test eltype(ids.s) == Union{Missing,@NamedTuple{id::NTuple{16,UInt8},n::Int64}}
        @test isequal(Arrow.Table(arrowbytes(ids)).s, ids.s)
        # So does a read fixed-size binary column on its own.
        fsb = Arrow.Table(arrowbytes((id=[ntuple(UInt8, 16)],))).id
        @test Arrow.Table(arrowbytes((id=fsb,))).id == [ntuple(UInt8, 16)]
        # Scans give the full read's rows.
        st = Arrow.Table(arrowbytes(tbl); scan=Tables.Scan(select=(:st, :l)))
        @test typeof(st.st) == typeof(t.st) && typeof(st.l) == typeof(t.l)
        # Unions are a small Union of their children's types.
        U = Union{Int64,String}
        u = Arrow.Table(arrowbytes((u=U[1, "two"],))).u
        @test eltype(u) == Union{Int64,DataString} && u == [1, "two"]
        # A map's entries start at the entries struct's own offset.
        AC = Arrow.AC
        kf, kd = AC.fromjulia("key", ["a", "b", "c"])
        vf, vd = AC.fromjulia("value", Int64[1, 2, 3])
        ef = AC.Field("entries", AC.StructType(); nullable=false, children=[kf, vf])
        ed = AC.ArrayData(
            AC.StructType(),
            2,
            [AC.BufferSlice()];
            offset=1,
            children=[kd, vd],
        )
        mf = AC.Field("m", AC.MapType(false); nullable=false, children=[ef])
        md = AC.ArrayData(
            AC.MapType(false),
            1,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 2])];
            children=[ed],
        )
        ctx = Arrow._ViewContext(DataString, nothing)
        @test Arrow._viewcolumn(mf, AC.ArrayData[md], ctx) == [["b" => 2, "c" => 3]]
        # Long fixed-size lists read as views; wide or duplicate-named
        # structs keep `Pair` rows.
        long = Arrow.Table(arrowbytes((x=[ntuple(Int64, 65)],))).x
        @test eltype(long) == Row{Int64} && long[1] == 1:65
        wide = NamedTuple{Tuple(Symbol(:f, k) for k = 1:65)}(Tuple(1:65))
        @test Arrow.Table(arrowbytes((w=[wide],))).w[1] isa AbstractVector{<:Pair}
        # Rows of 32 or more fields build without boxing each value.
        nt = NamedTuple{Tuple(Symbol(:f, k) for k = 1:40)}(Tuple(1:40))
        bytes = arrowbytes((s=fill(nt, 10_000),))
        @test Arrow.Table(bytes).s[end] === nt
        @test (@allocated Arrow.Table(bytes)) < 2 * 10_000 * sizeof(nt)
        # Fixed-size lists of nullable values read as `ListValue`s: a tuple type
        # with `Missing` in it is abstract, so its rows would be boxed and a
        # rewrite would see up to 2^N runtime types.
        vals = Union{Missing,Int16}[
            (r >> k) & 1 == 1 ? missing : Int16(4r + k) for r = 0:15 for k = 0:3
        ]
        cf, cd = AC.fromjulia("item", vals)
        ff = AC.Field("f", AC.FixedSizeListType(4); nullable=false, children=[cf])
        fd = AC.ArrayData(AC.FixedSizeListType(4), 16, [AC.BufferSlice()]; children=[cd])
        fs = AC.Schema([ff])
        f4 = Arrow.Table(Arrow.writestream(fs, [AC.RecordBatch(fs, [fd], 16)]))
        @test eltype(f4.f) == Row{Union{Missing,Int16}}
        @test isequal(reduce(vcat, f4.f), vals)
        @test isequal(Arrow.Table(arrowbytes(f4)).f, f4.f)
        # List views gather their rows' values, so rows may overlap or come
        # in any order.
        cf, cd = AC.fromjulia("item", ["a", "b", "c", "a longer value"])
        vf = AC.Field("v", AC.ListViewType(false); nullable=true, children=[cf])
        vd = AC.ArrayData(
            AC.ListViewType(false),
            3,
            [
                AC._databuffer(UInt8[0b101]),
                AC._databuffer(Int32[2, 0, 0]),
                AC._databuffer(Int32[2, 1, 3]),
            ];
            children=[cd],
            nullcount=1,
        )
        vs = AC.Schema([vf])
        lv = Arrow.Table(Arrow.writestream(vs, [AC.RecordBatch(vs, [vd], 3)])).v
        @test eltype(lv) == Union{Missing,Row{DataString}}
        @test isequal(collect(lv), [["c", "a longer value"], missing, ["a", "b", "c"]])
        # Fixed-size lists of no values read as empty tuples.
        cf, cd = AC.fromjulia("item", Int64[])
        zf = AC.Field("z", AC.FixedSizeListType(0); nullable=false, children=[cf])
        zd = AC.ArrayData(AC.FixedSizeListType(0), 2, [AC.BufferSlice()]; children=[cd])
        zs = AC.Schema([zf])
        z = Arrow.Table(Arrow.writestream(zs, [AC.RecordBatch(zs, [zd], 2)])).z
        @test eltype(z) == Tuple{} && z == [(), ()]
        # Nesting deeper than 8 levels keeps the materializing path: a row
        # type grows with every level, and so does its compile time.
        nest(x, k) = k == 0 ? x : nest([x], k - 1)
        deep8 = Arrow.Table(arrowbytes((x=[nest([1], 7)],))).x
        deep9 = Arrow.Table(arrowbytes((x=[nest([1], 8)],))).x
        @test eltype(deep8) <: Row && deep8 == [nest([1], 7)]
        @test !(eltype(deep9) <: Row) && deep9 == [nest([1], 8)]
    end

    @testset "nested children of union or run-end encoded values" begin
        # A union of row types is a union column's values: stored as a Vector.
        U = Union{Vector{Int64},Vector{Bool}}
        t = Arrow.Table(arrowbytes((c=[U[[1], [true]], U[]],)))
        @test isequal(t.c, [[[1], [true]], []])
        NT = Union{@NamedTuple{a::Int64},@NamedTuple{b::String}}
        t = Arrow.Table(arrowbytes((c=[NT[(a=1,), (b="x",)]],)))
        @test t.c[1][1] == (a=1,) && t.c[1][2] == (b="x",)
        t = Arrow.Table(arrowbytes((m=[Dict{String,U}("a" => [1], "b" => [true])],)))
        @test Dict(t.m[1]) == Dict("a" => [1], "b" => [true])
        # Run-end encoded list, struct, and map values inside a list.
        ree = Arrow.AC.RunEndEncodedType()
        lt = Arrow.AC.ListType(false)
        offsets = Arrow.AC._databuffer(Int32[0, 3, 3, 6])
        same(a, b) = isequal(a isa Arrow.MapValue ? Dict(a) : a, b)
        for values in (
            Union{Missing,Vector{Int64}}[[1, 2], missing, [3]],
            Union{Missing,@NamedTuple{p::Int64,q::String}}[
                (p=1, q="a"),
                missing,
                (p=3, q="c"),
            ],
            [Dict("k" => 1), Dict{String,Int64}(), Dict("j" => 2)],
        )
            rf, rd = Arrow.AC.fromjulia("run_ends", Int32[2, 3, 6])
            src = Arrow.readstream(arrowbytes((values=values,); file=false))
            vf, vd =
                only(Arrow.AC.schema(src).fields), only(Arrow.AC.nextbatch!(src).columns)
            ef = Arrow.AC.Field("item", ree; nullable=true, children=[rf, vf])
            ed = Arrow.AC.ArrayData(ree, 6, Arrow.AC.BufferSlice[]; children=[rd, vd])
            f = Arrow.AC.Field("l", lt; nullable=false, children=[ef])
            d = Arrow.AC.ArrayData(lt, 3, [Arrow.AC.BufferSlice(), offsets]; children=[ed])
            sch = Arrow.AC.Schema([f])
            t = Arrow.Table(Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d])]))
            runs = values[[1, 1, 2, 3, 3, 3]]
            @test all(same.(t.l[1], runs[1:3])) && isempty(t.l[2])
            @test all(same.(t.l[3], runs[4:6]))
        end
    end

    @testset "nested rows read without boxing their values" begin
        n = 100_000
        tbl = (
            st=[(a=i, b=string("s", i)) for i = 1:n],
            ls=[[string(i), "x"] for i = 1:n],
            m=[Dict(string(i) => i) for i = 1:n],
        )
        bytes = arrowbytes(tbl)
        Arrow.Table(bytes)
        # A boxed child value or row would cost an allocation each.
        @test (@allocations Arrow.Table(bytes)) < 5_000
        t = Arrow.Table(bytes)
        bsum(st) = sum(r -> ncodeunits(r.b), st)
        lsum(ls) = sum(r -> sum(ncodeunits, r), ls)
        # Rows build without allocating once compiled (older Julia and coverage
        # builds do not always inline far enough for that).
        if VERSION >= v"1.12" && Base.JLOptions().code_coverage == 0
            @test allocationsof(bsum, t.st) == 0 && allocationsof(lsum, t.ls) == 0
        end
        @test t.st[end] == tbl.st[end] && t.ls[2] == ["2", "x"] && t.m[3] == ["3" => 3]
    end

    @testset "fixed-width nested children and list offsets view the data read" begin
        n = 200_000
        tbl = (
            l=[[i, i + 1, i + 2, i + 3] for i = 1:n],
            st=[(a=Int32(i), c=i / 2) for i = 1:n],
            m=[Dict(i => float(i)) for i = 1:n],
        )
        path = tempname()
        Arrow.write(path, tbl)
        Arrow.Table(path; mmap=true)
        # Copies of the 4n list values, the struct fields, the map entries, or
        # the list and map offsets would allocate 4 bytes or more each.
        @test fewestbytes(() -> Arrow.Table(path; mmap=true)) < 2n
        t = Arrow.Table(path; mmap=true)
        @test t.l.offsets isa Vector{Int32} && t.m.offsets isa Vector{Int32}
        @test t.l == tbl.l && t.st == tbl.st && t.m[5] == [5 => 5.0]
        # A rewrite reads the stored offsets.
        @test Arrow.Table(arrowbytes(t)).l == tbl.l
        # Rows keep the mapping they view alive.
        row, st, entry = t.l[7], t.st, t.m[9]
        Arrow.release!(t)
        t = nothing
        GC.gc(true)
        GC.gc(true)
        @test row == [7, 8, 9, 10] && st[n] == tbl.st[n] && entry == [9 => 9.0]
        row = st = entry = nothing
        rmmapped(path)
    end

    @testset "an uncompressed open allocates independently of its size" begin
        small = arrowbytes((a=collect(1:1_000), b=rand(1_000)))
        large = arrowbytes((a=collect(1:2_000_000), b=rand(2_000_000)))
        Arrow.Table(small)
        Arrow.Table(large)
        @test (@allocated Arrow.Table(large)) < (@allocated Arrow.Table(small)) + 100_000
    end

    @testset "Arrow.read copies once into owned columns" begin
        bytes = arrowbytes((
            x=[1, 2, 3],
            c=PooledArray(["a", "b", "a"]),
            l=[[1], [2, 3], Int[]],
        ))
        nt = Arrow.read(bytes, Tables.columntable)
        @test nt.x isa Vector{Int64} && nt.x == [1, 2, 3]
        @test nt.c isa PooledArray && nt.c == ["a", "b", "a"]
        @test nt.l == [[1], [2, 3], Int[]]
        @test_throws ArgumentError Arrow.read(bytes)
    end

    @testset "reads give the same columns for any ntasks" begin
        n = 150_000   # string columns span several chunks of rows
        word(i) = i % 7 == 0 ? "" : repeat(string(i, base=36), 1 + i % 5)
        part(r) = (
            x=collect(Int64, r),
            f=[isodd(i) ? missing : i / 2 for i in r],
            s=[i % 11 == 0 ? missing : word(i) for i in r],
            v=[word(i) for i in r],
            b=[Vector{UInt8}(word(i)) for i in r],
            c=PooledArray([isodd(i) ? "odd" : "even" for i in r]),
            d=[reinterpret(Decimal{20,2,Int128}, Int128(i)) for i in r],
            t=[Date(2000) + Day(i % 1000) for i in r],
            q=[isodd(i) for i in r],
        )
        parts = Tables.partitioner([part(1:n), part((n + 1):(2n)), part(1:0)])
        same(a, b) = typeof(a) == typeof(b) && isequal(collect(a), collect(b))
        for (file, compress) in
            ((true, nothing), (true, :zstd), (true, :lz4), (false, :zstd))
            bytes = arrowbytes(parts; file=file, compress=compress, stringview=false)
            views = arrowbytes(parts; file=file, compress=compress, stringview=true)
            for src in (bytes, views), stringtype in (DataString, String)
                one = Arrow.Table(src; ntasks=1, stringtype=stringtype)
                for ntasks in (2, 5)
                    t = Arrow.Table(src; ntasks=ntasks, stringtype=stringtype)
                    @test all(
                        same(Tables.getcolumn(t, nm), Tables.getcolumn(one, nm)) for
                        nm in Tables.columnnames(one)
                    )
                end
                for (b1, b5) in
                    zip(Arrow.Stream(src; ntasks=1), Arrow.Stream(src; ntasks=5))
                    @test all(
                        same(Tables.getcolumn(b5, nm), Tables.getcolumn(b1, nm)) for
                        nm in Tables.columnnames(b1)
                    )
                end
                scan = Tables.Scan(select=(:s, :x, :c))
                @test all(
                    same(
                        Tables.getcolumn(Arrow.Table(src; scan=scan, ntasks=5), nm),
                        Tables.getcolumn(Arrow.Table(src; scan=scan, ntasks=1), nm),
                    ) for nm in (:s, :x, :c)
                )
            end
        end
        @test_throws ArgumentError Arrow.Table(arrowbytes(part(1:3)); ntasks=0)
        @test_throws ArgumentError Arrow.Stream(arrowbytes(part(1:3)); ntasks=-1)
    end

    @testset "a stream-format scan builds only the columns it needs" begin
        tbl = (x=collect(1:6), s=["a", "b", "c", "d", "e", "f"], y=collect(6:-1:1))
        bytes = arrowbytes(tbl; file=false)
        t = Arrow.Table(bytes; scan=Tables.Scan(select=(:s,), filter=Tables.col(:y) > 3))
        @test Tables.columnnames(t) == [:s] && t.s == ["a", "b", "c"]
        t = Arrow.Table(bytes; scan=Tables.Scan(select=(:y, :s), limit=2))
        @test t.y == [6, 5] && t.s == ["a", "b"]
        # A scan that only selects gives the full read's columns.
        full = Arrow.Table(bytes)
        t = Arrow.Table(bytes; scan=Tables.Scan(select=(:s, :x)))
        @test typeof(t.s) == typeof(full.s) && typeof(t.x) == typeof(full.x)
        @test t.s == tbl.s && t.x == tbl.x
    end

    @testset "string columns build when first taken" begin
        n = 100_000
        strs = [string("value number ", i) for i = 1:n]
        bytes = arrowbytes((s=strs, x=collect(1:n)))
        Arrow.Table(bytes)
        # Opening the table builds no string column (its payloads are 16n bytes).
        @test fewestbytes(() -> Arrow.Table(bytes)) < 16n ÷ 4
        t = Arrow.Table(bytes)
        @test Tables.schema(t).types == (DataString, Int64)
        # Tasks that ask at once get the one built column.
        cols = fetch.([Threads.@spawn(t.s) for _ = 1:8])
        @test all(c -> c === cols[1], cols) && cols[1] isa StringVector
        @test cols[1] == strs && t.s === cols[1] && t[1] === cols[1]
        @test copy(Arrow.Table(bytes)).s == strs
        @test Arrow.read(bytes, Tables.columntable).s == strs
        # A scan that only selects builds them the same way.
        Arrow.Table(bytes; select=[:x, :s])
        @test fewestbytes(() -> Arrow.Table(bytes; select=[:x, :s])) < 16n ÷ 4
        @test Arrow.Table(bytes; select=[:s]).s == strs
        @test Arrow.Table(bytes; stringtype=String).s == strs
        # The read's allowance is charged when the table opens.
        @test_throws Arrow.AllocationLimitError Arrow.Table(
            bytes;
            limits=Arrow.Limits(max_total_allocated_bytes=16n ÷ 2),
        )
        # A released table builds nothing more.
        released = Arrow.Table(bytes)
        Arrow.release!(released)
        @test_throws InvalidStateException released.s
    end

    @testset "string columns keep decompressed data without a copy" begin
        strs = [string(repeat("x", 100), i) for i = 1:100_000]
        data = sum(ncodeunits, strs)
        for compress in (:zstd, :lz4), stringview in (false, true)
            bytes = arrowbytes((s=strs,); compress=compress, stringview=stringview)
            @test Arrow.Table(bytes).s == strs
            # The decompressed data plus 16-byte payloads once the column is
            # built; a copy adds `data` more.
            @test fewestbytes(() -> Arrow.Table(bytes).s) < data + 4_000_000
        end
    end

    @testset "columns of several batches copy and broadcast by batch" begin
        part(r) = (
            x=collect(Int64, r),
            n=[iseven(i) ? missing : i for i in r],
            c=PooledArray([string("k", i % 3) for i in r]),
            s=[string(i) for i in r],
        )
        parts = Tables.partitioner([part(1:5), part(6:6), part(7:12)])
        for file in (true, false)
            t = Arrow.Table(arrowbytes(parts; file=file))
            @test t.x isa Arrow.ChainedVector
            owned = copy(t)
            @test owned.x isa Vector{Union{Missing,Int64}} && owned.x == 1:12
            @test owned.n isa Vector{Union{Missing,Int64}} && isequal(owned.n, t.n)
            # Dictionary batches (shared, delta, or replaced pools) give one PooledArray.
            @test owned.c isa PooledArray && owned.c == [string("k", i % 3) for i = 1:12]
            @test owned.s == string.(1:12)
            nt = Arrow.read(arrowbytes(parts; file=file), Tables.columntable)
            @test nt.x isa Vector && nt.c isa PooledArray && !(nt.s isa Arrow.ChainedVector)
            # Broadcasts give what they give over a Vector of the same values.
            v = collect(t.x)
            n = collect(t.n)
            for (got, want) in (
                (t.x .+ 1, v .+ 1),
                (t.x .> 6, v .> 6),
                (t.n .* 2, n .* 2),
                (coalesce.(t.n, 0), coalesce.(n, 0)),
                (Float64.(t.x), Float64.(v)),
            )
                @test typeof(got) == typeof(want) && isequal(got, want)
            end
            @test t.x .+ t.x .* collect(1:12) == v .+ v .* (1:12)
            @test isequal(t.x .+ t.n, v .+ collect(t.n))
            @test t.x .+ [10] == v .+ 10 && t.x .+ Ref(1) == v .+ 1
            @test string.(t.s, "!") == string.(1:12, "!")
            dest = zeros(Int, 12)
            dest .= t.x .* 2
            @test dest == 2 .* (1:12)
            @test t.x .+ Tuple(1:12) == 2 .* (1:12)
            @test (t.x .+ (1:12)') == v .+ (1:12)'
        end
    end

    @testset "parallel work keeps nothing alive once it returns" begin
        # A finished task can stay reachable (a thread keeps the last task it
        # ran), so the work function, and a read's data it captures, must not
        # be reachable from it. `run!` must not inline: a caller's stack slot
        # would keep `data` alive on its own.
        @noinline function run!(ref)
            data = zeros(UInt8, 10)
            ref[] = WeakRef(data)
            Arrow._foreach((i, _) -> (data[i]=0x01; nothing), 10, 4)
            return nothing
        end
        retained = map(1:10) do _
            ref = Ref{WeakRef}()
            run!(ref)
            for _ = 1:3
                GC.gc(true)
            end
            ref[].value !== nothing
        end
        @test !any(retained)
    end

    @testset "parallel work reports the first failing item" begin
        for ntasks in (1, 3, 8)
            ran = zeros(Int, 20)
            err = try
                Arrow._foreach(20, ntasks) do i, w
                    @test 1 <= w <= min(20, ntasks)
                    ran[i] += 1
                    i in (6, 9, 14) && error("item $i")
                end
                nothing
            catch e
                e
            end
            @test err isa ErrorException && err.msg == "item 6"
            # Every item before the first failure ran exactly once.
            @test all(==(1), ran[1:6]) && all(<=(1), ran)
            ran .= 0
            Arrow._foreach((i, w) -> (ran[i] += 1), 20, ntasks)
            @test all(==(1), ran)
        end
    end
end
