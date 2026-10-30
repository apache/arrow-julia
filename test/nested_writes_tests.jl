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

# Writing plain nested Julia values (vectors, NamedTuples, NTuples, Dicts,
# Pair vectors) and the typed nested rows Arrow.Table reads.

module NestedWritesTests

using Test
using Arrow
using Dates
using Tables

const AC = Arrow.AC

# A row whose length grows on every look.
mutable struct Grows <: AbstractVector{Int}
    n::Int
end
Base.size(g::Grows) = (g.n += 1,)
Base.getindex(::Grows, i::Int) = i

# A map row type of its own, as Parquet's MapValue is.
struct PairRow <: AbstractVector{Pair{String,Int}}
    pairs::Vector{Pair{String,Int}}
end
Base.size(r::PairRow) = size(r.pairs)
Base.getindex(r::PairRow, i::Int) = r.pairs[i]

roundtrip(cols; kw...) = Arrow.Table(take!(Arrow.tobuffer(cols; kw...)))
field(t, j=1) = getfield(t, :schema).fields[j]
# A map row as the Dict it describes, for comparing with Dict inputs.
asdict(x) = x === missing ? missing : Dict(x)

@testset "plain nested writes" begin
    @testset "lists, with missing rows and values" begin
        cols = (
            a=[[1, 2], Int[], [3]],
            b=Union{Missing,Vector{Union{Missing,Int32}}}[[1, missing], missing, Int32[]],
            c=[["x", "yy"], String[], ["a longer string than twelve bytes"]],
            d=[[[1], Int[]], [[2, 3]], Vector{Int}[]],
            e=[1:2, 3:3, 4:3],
        )
        t = roundtrip(cols)
        for (j, name) in enumerate(keys(cols))
            @test isequal(collect(t[name]), collect(cols[name]))
            @test field(t, j).type == AC.ListType(false)
        end
        @test field(t, 2).nullable && only(field(t, 2).children).nullable
        @test !field(t, 1).nullable && !only(field(t, 1).children).nullable
        @test field(roundtrip((a=cols.a,); largelists=true)).type == AC.ListType(true)
    end

    @testset "structs, and children hidden under missing rows" begin
        NT = @NamedTuple{a::Int64, s::String, l::Vector{Int8}, n::@NamedTuple{x::Float64}}
        rows = Union{Missing,NT}[
            (a=1, s="one", l=Int8[1, 2], n=(x=1.5,)),
            missing,
            (a=3, s="", l=Int8[], n=(x=-0.0,)),
        ]
        t = roundtrip((r=rows,))
        @test isequal(collect(t.r), rows)
        f = field(t)
        @test f.type isa AC.StructType && f.nullable
        # Non-nullable children stay non-nullable: their slots under the
        # missing row hold placeholders.
        @test [c.nullable for c in f.children] == [false, false, false, false]
        fields = [(a=missing, b="x"), (a=2, b=missing)]
        @test isequal(collect(roundtrip((r=fields,)).r), fields)
    end

    @testset "maps from Dict and Pair-vector rows" begin
        dicts = Union{Missing,Dict{String,Union{Missing,Int64}}}[
            Dict("a" => 1, "b" => missing),
            missing,
            Dict{String,Union{Missing,Int64}}(),
        ]
        t = roundtrip((m=dicts,))
        @test field(t).type isa AC.MapType
        entries = only(field(t).children)
        @test entries.name == "entries" && !entries.nullable
        @test [c.name for c in entries.children] == ["key", "value"]
        @test !entries.children[1].nullable && entries.children[2].nullable
        @test isequal(asdict.(collect(t.m)), dicts)
        # Pair rows keep their order and repeated keys.
        pairs = [["k" => 1, "k" => 2, "a" => 3], Pair{String,Int}[]]
        tp = roundtrip((m=pairs,))
        @test field(tp).type isa AC.MapType
        @test collect.(tp.m) == pairs
        @test eltype(tp.m) <: Arrow.MapValue
        # Any vector type of Pairs is a map row (Parquet's MapValue, say).
        own = [PairRow(["a" => 1]), PairRow(["b" => 2, "c" => 3])]
        to = roundtrip((m=own, l=[[own[1]], [own[2], own[1]]]))
        @test field(to, 1).type isa AC.MapType
        @test only(field(to, 2).children).type isa AC.MapType
        @test collect.(to.m) == [r.pairs for r in own]
        err = try
            Arrow.tobuffer((m=[Dict{Union{Missing,String},Int}(missing => 1)],))
        catch e
            e
        end
        @test err isa ArgumentError && occursin("column m has a missing map key", err.msg)
    end

    @testset "NTuples as fixed-size lists" begin
        rows = Union{Missing,NTuple{3,Float64}}[(1.0, 2.0, 3.0), missing]
        t = roundtrip((p=rows,))
        @test field(t).type == AC.FixedSizeListType(3)
        @test !only(field(t).children).nullable
        @test isequal(collect(t.p), rows)
        strs = [("a", "b"), ("c", "")]
        @test collect(roundtrip((p=strs,)).p) == strs
    end

    @testset "deep combinations with missing at every level" begin
        Inner = @NamedTuple{k::Int64, v::Union{Missing,String}}
        rows = [
            [Dict("x" => Union{Missing,Inner}[(k=1, v="a"), missing, (k=2, v=missing)])],
            Dict{String,Vector{Union{Missing,Inner}}}[],
            [Dict{String,Vector{Union{Missing,Inner}}}()],
        ]
        t = roundtrip((d=rows,))
        @test field(t).type == AC.ListType(false)
        @test only(field(t).children).type isa AC.MapType
        @test isequal([asdict.(r) for r in t.d], rows)
    end

    @testset "Vector{Any} columns narrow from their values" begin
        @test collect(roundtrip((x=Any[[1, 2], [3]],)).x) == [[1, 2], [3]]
        @test collect(roundtrip((x=Any[(a=1,), (a=2,)],)).x) == [(a=1,), (a=2,)]
        t = roundtrip((x=Any[missing, missing],))
        @test field(t).type isa AC.NullType
        # Only empty rows: their declared element type.
        t = roundtrip((x=Any[Int16[], Int16[]],))
        @test only(field(t).children).type == AC.IntType(16, true)
        err = try
            Arrow.tobuffer((x=Any[Any[], Any[]],))
        catch e
            e
        end
        @test err isa ArgumentError
        @test startswith(err.msg, "column x (child item) has")
    end

    @testset "a row that changes between passes is refused" begin
        err = try
            Arrow.tobuffer((x=[Grows(0)],))
        catch e
            e
        end
        @test err isa ArgumentError &&
              occursin("changed while it was being written", err.msg)
    end
end

@testset "typed nested rows write back" begin
    rows = Union{Missing,Vector{Union{Missing,Int64}}}[[1, missing], missing, [], [4, 5, 6]]
    structs = Union{Missing,@NamedTuple{a::Int32,b::Union{Missing,String}}}[
        (a=1, b="x"),
        missing,
        (a=3, b=missing),
        (a=4, b="y"),
    ]
    maps = [Dict("a" => 1), Dict{String,Int}(), Dict("b" => 2, "c" => 3), Dict("d" => 4)]
    t = roundtrip((l=rows, s=structs, m=maps))
    # A fresh write of the read views, of owned copies, and a retained rewrite
    # all keep the schema and values.
    for src in (Tables.columntable(t), map(copy, Tables.columntable(t)), t)
        back = roundtrip(src)
        @test all(j -> Arrow._fieldcontractequal(field(back, j), field(t, j)), 1:3)
        @test isequal(collect(back.l), collect(t.l))
        @test isequal(collect(back.s), collect(t.s))
        @test isequal(collect(back.m), collect(t.m))
    end
    # Batches of a multi-batch column write as they were read.
    chunked = roundtrip(Tables.partitioner([(l=rows[1:2],), (l=rows[3:4],)]))
    @test isequal(collect(roundtrip(Tables.columntable(chunked)).l), rows)
end

@testset "a read list whose null rows span values" begin
    # Arrow lets a null list slot cover child values: offsets [0, 2, 3, 3]
    # with row 2 null over value 3.
    child =
        AC.ArrayData(AC.IntType(64, true), 3, [AC.BufferSlice(), AC._databuffer([1, 2, 3])])
    lt = AC.ListType(false)
    d = AC.ArrayData(
        lt,
        3,
        [AC._bitmapbuffer([true, false, true]), AC._databuffer(Int32[0, 2, 3, 3])];
        children=[child],
        nullcount=1,
    )
    f = AC.Field("l", lt; nullable=true, children=[AC.Field("item", AC.IntType(64, true))])
    sch = AC.Schema([f])
    t = Arrow.Table(Arrow.writestream(sch, [AC.RecordBatch(sch, [d], 3)]))
    @test isequal(collect(t.l), [[1, 2], missing, Int[]])
    back = roundtrip(Tables.columntable(t))
    @test isequal(collect(back.l), [[1, 2], missing, Int[]])
end

@testset "a retained rewrite keeps the source's child names" begin
    child =
        AC.ArrayData(AC.IntType(64, true), 3, [AC.BufferSlice(), AC._databuffer([1, 2, 3])])
    lt = AC.ListType(false)
    d = AC.ArrayData(
        lt,
        2,
        [AC.BufferSlice(), AC._databuffer(Int32[0, 2, 3])];
        children=[child],
    )
    f = AC.Field(
        "l",
        lt;
        nullable=false,
        children=[AC.Field("element", AC.IntType(64, true))],
    )
    sch = AC.Schema([f])
    t = Arrow.Table(Arrow.writestream(sch, [AC.RecordBatch(sch, [d], 2)]))
    back = roundtrip(t)
    @test only(field(back).children).name == "element"
    @test collect(back.l) == [[1, 2], [3]]
end

@testset "a Union of tuple types writes unlabelled, as a map value too" begin
    v = Union{Tuple{Int32,Int32},Tuple{Date,Date,Date}}[
        (Int32(1), Int32(2)),
        (Date(2020), Date(2020), Date(2021)),
    ]
    for tbl in ((c=v,), (c=[Dict("a" => x) for x in v],))
        t = roundtrip(tbl)
        @test field(t).metadata === nothing
        @test field(t).type isa AC.UnionType || field(t).children[1].type isa AC.StructType
        back = collect(t.c)
        tbl.c isa Vector{<:Dict} ? @test(Dict.(back) == tbl.c) : @test(back == v)
    end
end

@testset "a retained rewrite routes typed union rows to their children" begin
    # A dense union, as a fresh write makes one.
    t = roundtrip((u=Union{Int64,String}[1, "two", 3],))
    back = roundtrip(t)
    @test Arrow._fieldcontractequal(field(back), field(t))
    @test collect(back.u) == [1, "two", 3]
    # A sparse union with a null, built as pyarrow would.
    int = AC.ArrayData(
        AC.IntType(64, true),
        3,
        [AC._bitmapbuffer([true, true, false]), AC._databuffer([1, 0, 0])];
        nullcount=1,
    )
    str = last(AC.fromjulia("s", ["", "two", ""]))
    ut = AC.UnionType(AC.SparseMode, Int8[0, 1])
    d = AC.ArrayData(ut, 3, [AC._databuffer(Int8[0, 1, 0])]; children=[int, str])
    children = [
        AC.Field("i", AC.IntType(64, true)),
        AC.Field("s", AC.Utf8Type(false); nullable=false),
    ]
    f = AC.Field("u", ut; nullable=false, children)
    sch = AC.Schema([f])
    sparse = Arrow.Table(Arrow.writestream(sch, [AC.RecordBatch(sch, [d], 3)]))
    @test isequal(collect(sparse.u), [1, "two", missing])
    back = roundtrip(sparse)
    @test field(back).type.mode == AC.SparseMode
    @test isequal(collect(back.u), [1, "two", missing])
    # Two children of one type cannot be told apart by value.
    twin = AC.ArrayData(
        ut,
        2,
        [AC._databuffer(Int8[0, 1])];
        children=[
            last(AC.fromjulia("a", Int32[1, 2])),
            last(AC.fromjulia("b", Int32[3, 4])),
        ],
    )
    tf = AC.Field(
        "u",
        ut;
        nullable=false,
        children=[AC.Field("a", AC.IntType(32, true)), AC.Field("b", AC.IntType(32, true))],
    )
    tsch = AC.Schema([tf])
    twins = Arrow.Table(Arrow.writestream(tsch, [AC.RecordBatch(tsch, [twin], 2)]))
    err = try
        roundtrip(twins)
        nothing
    catch e
        e
    end
    @test err isa ArgumentError && occursin("fit none or several", err.msg)
end

@testset "union children lower their values, nested and inside unions" begin
    ut = AC.UnionType(AC.SparseMode, Int8[0, 1])
    rewrite(f, d, n) = begin
        sch = AC.Schema([f])
        src = Arrow.Table(Arrow.writestream(sch, [AC.RecordBatch(sch, [d], n)]))
        back = roundtrip(src)
        @test Arrow._fieldcontractequal(field(back), field(src))
        @test isequal(collect(Tables.getcolumn(back, 1)), collect(Tables.getcolumn(src, 1)))
        return back
    end
    sparse(children, ids, fields) = (
        AC.Field("u", ut; children=fields),
        AC.ArrayData(ut, length(ids), [AC._databuffer(ids)]; children),
    )
    # A date child: its values lower to days, not stored as they read.
    datef, dated = AC.fromjulia("d", Int32[19000, 0, 19002])
    datef = AC.Field("d", AC.DateType(AC.DAY))
    dated = AC.ArrayData(AC.DateType(AC.DAY), 3, dated.buffers)
    strf, strd = AC.fromjulia("s", ["", "two", ""])
    uf, ud = sparse([dated, strd], Int8[0, 1, 0], [datef, strf])
    back = rewrite(uf, ud, 3)
    @test collect(back.u) == [Dates.Date(2022, 1, 8), "two", Dates.Date(2022, 1, 10)]
    # The same union as a struct field, and as list values.
    sf = AC.Field("st", AC.StructType(); children=[uf])
    sd = AC.ArrayData(AC.StructType(), 3, [AC.BufferSlice()]; children=[ud])
    rewrite(sf, sd, 3)
    lt = AC.ListType(false)
    lf = AC.Field("l", lt; children=[uf])
    ld = AC.ArrayData(
        lt,
        2,
        [AC.BufferSlice(), AC._databuffer(Int32[0, 1, 3])];
        children=[ud],
    )
    rewrite(lf, ld, 2)
    # A union whose first child is itself a union.
    intf, intd = AC.fromjulia("i", Int32[7, 8, 9])
    of, od = sparse([ud, intd], Int8[0, 1, 0], [uf, intf])
    back = rewrite(of, od, 3)
    @test collect(Tables.getcolumn(back, 1)) ==
          [Dates.Date(2022, 1, 8), 8, Dates.Date(2022, 1, 10)]
end

end # module NestedWritesTests
