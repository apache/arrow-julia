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
import DataAPI
import Durations
import UUIDs
using PooledArrays: PooledArray

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
        # Arrow 2.x returned `nothing` for any other object; callers pass
        # `metadata=Arrow.getmetadata(table)` for arbitrary write inputs.
        @test Arrow.getmetadata(bare.a) === nothing
        @test Arrow.getmetadata([(a=1,), (a=2,)]) === nothing
        @test Arrow.getmetadata(tbl) === nothing
    end

    @testset "unsupported sources name the supported kinds" begin
        bytes = take!(Arrow.tobuffer((; a=[1])))
        err = @test_throws ArgumentError Arrow.Table([bytes, bytes])
        @test occursin("one IPC source", err.value.msg)
        @test_throws ArgumentError Arrow.Stream(view(bytes, 1:length(bytes)))
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
        @test_logs (:warn, r"`alignment` was removed") match_mode = :any Arrow.write(
            io,
            (; a=[1]);
            alignment=64,
        )
        @test Arrow.Table(take!(io)).a == [1]
        # Ignored keywords still produce a working write.
        io = IOBuffer()
        Arrow.write(io, (; a=[1]); ntasks=2, alignment=64, maxdepth=6)
        @test Arrow.Table(take!(io)).a == [1]
    end
end

@testset "unknown keywords and mismatched filter literals are ArgumentErrors" begin
    bytes = take!(Arrow.tobuffer((a=[1, 2], s=["x", "y"])))
    msg(f) =
        try
            f()
            ""
        catch e
            e isa ArgumentError ? e.msg : "not an ArgumentError: $(typeof(e))"
        end
    m = msg(() -> Arrow.write(IOBuffer(), (a=[1],); compression=:zstd))
    @test occursin("unknown write keyword `compression` (use `compress`).", m)
    @test occursin("The keywords are file, compress, compresslevel, batchsize", m)
    m = msg(() -> Arrow.Writer(IOBuffer(); compression_level=3, nope=1))
    @test occursin("keywords `compression_level` (use `compresslevel`), `nope`", m)
    m = msg(() -> Arrow.append(Arrow.tobuffer((a=[1],)), (a=[2],); compression=:lz4))
    @test occursin("unknown append keyword `compression` (use `compress`)", m)
    m = msg(() -> Arrow.Table(bytes; columns=[:a]))
    @test occursin("unknown read keyword `columns` (use `select`).", m)
    @test occursin("The keywords are select, drop, filter, limit, scan", m)
    @test occursin(
        "`convert` (removed in Arrow 3.0",
        msg(() -> Arrow.Table(bytes; convert=false)),
    )
    @test occursin(
        "`use_threads` (use `ntasks`)",
        msg(() -> Arrow.Stream(bytes; use_threads=true)),
    )
    @test occursin(
        "`memory_map` (use `mmap`)",
        msg(() -> Arrow.read(bytes, identity; memory_map=true)),
    )
    # Arrow 2.x's byte windows.
    @test occursin(
        "Arrow.Table(bytes[pos:pos+len-1])",
        msg(() -> Arrow.Table(bytes, 1, 10)),
    )
    @test occursin("Arrow.Stream(bytes[pos:pos+len-1])", msg(() -> Arrow.Stream(bytes, 1)))
    # A literal of another kind than its column, when the read binds the filter.
    m = msg(() -> Arrow.Table(bytes; filter=Arrow.col(:s) > 1))
    @test m == "the filter compares column s, which holds strings, with 1::Int64"
    m = msg(() -> Arrow.Table(bytes; filter=Arrow.colin(Arrow.col(:a), ["1"])))
    @test m == "the filter compares column a, which holds Int64 values, with \"1\"::String"
    m = msg(() -> Arrow.Stream(bytes; filter=startswith(Arrow.col(:a), "1")))
    @test m == "the filter matches text in column a, which holds Int64 values"
    # Literals of the column's kind still read.
    @test Arrow.Table(bytes; filter=Arrow.col(:a) > 1.5).a == [2]
    @test Arrow.Table(bytes; filter=startswith(Arrow.col(:s), "y")).s == ["y"]
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
        bytes = take!(io)
        for T in (NT, Union{Missing,NT})
            t = Arrow.Table(bytes; scan=Tables.Scan(select=(:c => T,)))
            @test eltype(t.c) == Union{Missing,NT}
            @test t.c[1] == (a=1.0, b="x")
            @test t.c[2] === missing
        end
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

    @testset "abstract NamedTuple columns widen field by field" begin
        # Rows whose fields are missing in some rows have different concrete
        # types; Arrow 2.x wrote them as one Struct, and so does 3.0.
        v = [(a=isodd(i) ? missing : i, b=isodd(i) ? "x$i" : missing) for i = 1:4]
        @test !isconcretetype(eltype(v))
        W = @NamedTuple{a::Union{Missing,Int64}, b::Union{Missing,String}}
        t = Arrow.Table(Arrow.tobuffer((; v)); scan=Tables.Scan(select=(:v => W,)))
        @test eltype(t.v) == W
        @test isequal(t.v, v)
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

# A value wrapper with a DataAPI ref pool, shaped like CategoricalArrays'
# CategoricalValue/CategoricalArray, which has no ArrowTypes.jl mapping.
struct CompatWrapped
    x::String
end
DataAPI.unwrap(w::CompatWrapped) = w.x
struct CompatWrappedVector <: AbstractVector{CompatWrapped}
    refs::Vector{UInt32}
    levels::Vector{String}
end
Base.size(v::CompatWrappedVector) = size(v.refs)
Base.getindex(v::CompatWrappedVector, i::Int) = CompatWrapped(v.levels[v.refs[i]])
DataAPI.refarray(v::CompatWrappedVector) = v.refs
DataAPI.refpool(v::CompatWrappedVector) = CompatWrapped.(v.levels)

struct CompatHasPtr
    p::Ptr{Nothing}
end

@testset "DataAPI ref-pool columns write dictionary-encoded" begin
    # Arrow 2.x dictionary-encoded PooledArray and CategoricalArray columns;
    # the pool order and unused entries carry over.
    function pool(col)
        s = Arrow.readstream(take!(Arrow.tobuffer((; col))))
        f = only(s.schema.fields)
        @test f.type isa AC.DictionaryType
        d = only(s.batches).columns[1]
        return Arrow.materialize(AC.dictvaluefield(f, f.type), d.dictionary)
    end
    p = PooledArray(["b", "a", "b"])
    p[1] = "z"
    @test pool(p) == ["b", "a", "z"]
    @test Arrow.Table(Arrow.tobuffer((; p))).p == ["z", "a", "b"]
    m = PooledArray(["a", missing, "a"])
    @test isequal(Arrow.Table(Arrow.tobuffer((; m))).m, ["a", missing, "a"])
    # Wrapped values without an ArrowTypes.jl mapping unwrap through DataAPI.
    w = CompatWrappedVector(UInt32[2, 3, 2], ["c", "b", "a"])
    @test pool(w) == ["c", "b", "a"]
    t = Arrow.Table(Arrow.tobuffer((; w)))
    @test eltype(t.w) == Arrow.DataStrings.DataString
    @test t.w == ["b", "a", "b"]
    # Arrow.Writer encodes them too; later tables' new categories extend them.
    io = IOBuffer()
    Arrow.Writer(io) do writer
        Arrow.write(writer, (; p=PooledArray(["a", "b", "a"]), w))
        return Arrow.write(writer, (; p=PooledArray(["c", "c", "d"]), w))
    end
    t = Arrow.Table(take!(io))
    @test all(f -> f.type isa AC.DictionaryType, getfield(t, :schema).fields)
    @test t.p == ["a", "b", "a", "c", "c", "d"]
    @test t.w == ["b", "a", "b", "b", "a", "b"]
    # A struct whose fields cannot be written names the column and type.
    err = @test_throws ArgumentError Arrow.tobuffer((; x=[CompatHasPtr(C_NULL)]))
    @test occursin("column x holds", err.value.msg)
    @test occursin("CompatHasPtr", err.value.msg)
end

@testset "dictionary writes do not allocate per row" begin
    # Ref-pool and DictEncode columns share one encoder; 200,000 cells here.
    vals = [string("c", i % 50) for i = 1:100_000]
    tbl = (d=Arrow.DictEncode(vals), p=PooledArray(vals))
    Arrow.tobuffer(tbl)
    @test (@allocations Arrow.tobuffer(tbl)) < 100_000
end

@testset "Arrow 2.x FixedSizeBinary extension columns" begin
    # Arrow 2.x wrote every NTuple{N,UInt8} storage type, including UUID, as
    # FixedSizeBinary(N); ArrowTypes.jl restores UUID from NTuple{16,UInt8}.
    u = [UUIDs.UUID(1), UUIDs.UUID(typemax(UInt128) - 1)]
    t = AC.FixedSizeBinaryType(16)
    meta = ["ARROW:extension:name" => "JuliaLang.UUID", "ARROW:extension:metadata" => ""]
    bytes = collect(reinterpret(UInt8, [x.value for x in u]))
    d = AC.ArrayData(t, 2, [AC.BufferSlice(), AC._databuffer(bytes)]; nullcount=0)
    sch = AC.Schema([AC.Field("u", t; nullable=false, metadata=meta)])
    tbl = Arrow.Table(Arrow.writefile(sch, [AC.RecordBatch(sch, [d])]))
    @test eltype(tbl.u) == UUIDs.UUID
    @test tbl.u == u
    # A rewrite keeps the 2.x label and byte order.
    re = Arrow.readfile(take!(Arrow.write(IOBuffer(), tbl; file=true)))
    @test Arrow._fieldmetadata(only(re.schema.fields)) == meta
    @test AC.slicebytes(AC.rolebuffer(re[1].columns[1], AC.DATA)) == bytes
end

@testset "files from Arrow.jl before 2.6" begin
    # Those releases wrote V5 messages under a V4 file footer, which the
    # reference implementation reads.
    bytes = take!(Arrow.write(IOBuffer(), (; a=[1, 2]); file=true))
    footerlen = Int(only(reinterpret(Int32, bytes[(end - 9):(end - 6)])))
    footerstart = length(bytes) - 10 - footerlen
    footer = Arrow.FB.getrootas(
        Arrow.Meta.Footer,
        bytes[(footerstart + 1):(footerstart + footerlen)],
        0,
    )
    @test footer.version == Arrow.Meta.MetadataVersion.V5
    at = footerstart + Arrow.FB.offset(footer, 4) + Arrow.FB.pos(footer)
    bytes[(at + 1):(at + 2)] = reinterpret(UInt8, [Int16(Arrow.Meta.MetadataVersion.V4)])
    @test Arrow.Table(bytes).a == [1, 2]
    @test [b.a for b in Arrow.Stream(bytes)] == [[1, 2]]
end

@testset "Arrow 2.x temporal extension labels" begin
    # Arrow 2.x labelled its temporal columns; Arrow 3.0 reads the storage
    # natively and does not warn about these labels.
    label(name) = ["ARROW:extension:name" => name, "ARROW:extension:metadata" => ""]
    ms = AC.TimestampType(AC.MILLISECOND, nothing)
    zms = AC.TimestampType(AC.MILLISECOND, "UTC")
    cols = [
        ("d", AC.DateType(AC.DAY), "JuliaLang.Date", Int32[1]),
        ("dt", ms, "JuliaLang.DateTime", Int64[1]),
        ("t", AC.TimeType(AC.NANOSECOND, 64), "JuliaLang.Time", Int64[1]),
        ("p", AC.DurationType(AC.SECOND), "JuliaLang.Dates.Period", Int64[1]),
        ("z", zms, "JuliaLang.ZonedDateTime-UTC", Int64[1]),
        ("ts", ms, "JuliaLang.Durations.Timestamp", Int64[1]),
    ]
    fields = [AC.Field(n, t; nullable=false, metadata=label(l)) for (n, t, l, _) in cols]
    data = [
        AC.ArrayData(t, 1, [AC.BufferSlice(), AC._databuffer(v)]; nullcount=0) for
        (_, t, _, v) in cols
    ]
    sch = AC.Schema(fields)
    bytes = Arrow.writestream(sch, [AC.RecordBatch(sch, data)])
    tbl = @test_logs min_level = Base.CoreLogging.Warn Arrow.Table(bytes)
    @test tbl.d == [Date(1970, 1, 2)]
    @test tbl.dt == [DateTime(1970, 1, 1, 0, 0, 0, 1)]
    @test tbl.t == [Time(0, 0, 0, 0, 0, 1)]
    @test tbl.p == [Second(1)]
    @test tbl.ts == tbl.dt
    # Before Arrow 2.4.2 the zoned label stored local wall time, which a UTC
    # instant read does not reproduce, so it still warns.
    old = AC.Field("z", zms; nullable=false, metadata=label("JuliaLang.ZonedDateTime"))
    osch = AC.Schema([old])
    obytes = Arrow.writestream(osch, [AC.RecordBatch(osch, [data[5]])])
    @test_logs (:warn, r"JuliaLang.ZonedDateTime") Arrow.Table(obytes)
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
