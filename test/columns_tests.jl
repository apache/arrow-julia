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

struct ColumnsBytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::ColumnsBytesSource) = length(s.data)
Arrow.readrange(s::ColumnsBytesSource, off, len) = s.data[(off + 1):(off + len)]

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
    # Replacing a file that live columns map keeps the old contents.
    if !Sys.iswindows()
        Arrow.write(path, Arrow.Table(path; mmap=true))
        Arrow.write(path, (x=collect((-n):-1),); file=true)
        GC.gc(true)
        @test x[1] == 1 && y[1] == 1 && Arrow.Table(path).x[1] == -n
    end
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
    end

    @testset "views keep their memory alive" begin
        n = 1_000_000
        path = tempname()
        try
            Arrow.write(path, (x=collect(1:n), s=string.(1:n)); file=true)
            mappedviews(path, n)
        finally
            GC.gc(true)  # Windows deletes the file only once nothing maps it.
            rm(path; force=true)
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
                GC.gc(true)
                rm(path; force=true)
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

    @testset "nested columns read as typed rows" begin
        Row = Arrow.ListRow
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
        @test eltype(t.m) == Row{Pair{DataString,Int64}}
        @test t.m == [["k" => 1], Pair{String,Int64}[], ["p" => 2]]
        @test eltype(t.t) == NTuple{2,Int16} && t.t == tbl.t
        @test eltype(t.ll) == Row{Row{Int64}} && t.ll == tbl.ll
        # Rows outlive their table; copies hold rows as their own vectors.
        row = Arrow.Table(arrowbytes(tbl)).l[1]
        GC.gc(true)
        @test row == [1, 2]
        @test copy(t.l) isa Vector{Union{Missing,Vector{Int64}}} &&
              isequal(copy(t.l), tbl.l)
        # Several batches chain, and copy the same way.
        two = Arrow.Table(arrowbytes(Tables.partitioner([tbl, tbl]); file=true))
        @test isequal(collect(two.l), [tbl.l; tbl.l]) && two.st == [tbl.st; tbl.st]
        @test copy(two.l) isa Vector && all(r -> r === missing || r isa Vector, copy(two.l))
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
        # Fixed-size lists of nullable values read as `ListRow`s: a tuple type
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
end
