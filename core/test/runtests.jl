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

# Standalone: `julia --startup-file=no core/test/runtests.jl`. Stdlib only.

using Test

include(joinpath(@__DIR__, "..", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore

@testset "ArrowCore" begin

@testset "OwnerRegion lifecycle" begin
    @testset "heap wrap is zero-copy and rooted" begin
        v = Int64[1, 2, 3, 4]
        r = heapregion(v)
        @test r.len == 32
        @test r.kind == AC.Heap
        b = BufferSlice(r, 0, 32)
        @test AC.loadat(b, Int64, Int64(0)) == 1
        @test AC.loadat(b, Int64, Int64(24)) == 4
        @test_throws ErrorException setproperty!(r, :ptr, Ptr{UInt8}(0))
        @test_throws ErrorException setproperty!(r, :root, nothing)
    end

    @testset "mmap region: read, deterministic close, invalidation" begin
        path = tempname()
        write(path, UInt8[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88])
        r = mmapregion(path)
        b = BufferSlice(r, 0, 8)
        @test AC.loadat(b, UInt8, Int64(0)) == 0x11
        @test AC.loadat(b, UInt32, Int64(4)) == 0x88776655
        @test forceclose!(r)
        # closed: every subsequent access through the region fails cleanly
        @test_throws InvalidatedError AC.loadat(b, UInt8, Int64(0))
        # idempotent
        @test forceclose!(r)
        rm(path)
    end

    @testset "forceclose! waits for guards; timeout restores open" begin
        v = zeros(UInt8, 64)
        r = heapregion(v)
        entered = Base.Event()
        release = Base.Event()
        t = Threads.@spawn withguard(r) do
            notify(entered)
            wait(release)
            42
        end
        wait(entered)
        # a guard is held: a short-timeout close must fail AND restore open
        @test forceclose!(r; timeout_ms=50) == false
        @test AC.phase(@atomic r.state) == AC.PHASE_OPEN
        # region still fully usable after the busy close
        @test withguard(() -> 1, r) == 1
        notify(release)
        @test fetch(t) == 42
        @test forceclose!(r)
        @test_throws InvalidatedError withguard(() -> 1, r)
    end

    @testset "guard acquired after close fails" begin
        r = heapregion(zeros(UInt8, 8))
        @test forceclose!(r)
        @test_throws InvalidatedError withguard(() -> 1, r)
        @test (@atomic r.guards) == 0   # failed acquire backed out its count
    end

    @testset "invalid construction and release errors stay closed" begin
        @test_throws ArgumentError AC.OwnerRegion(Ptr{UInt8}(0), 1, AC.Foreign)
        @test_throws ArgumentError forceclose!(heapregion(UInt8[0]); timeout_ms=-1)
        calls = Ref(0)
        bytes = UInt8[0]
        r = GC.@preserve bytes AC.OwnerRegion(Ptr{UInt8}(pointer(bytes)), 1, AC.Foreign;
            root=bytes, releasefn=_ -> (calls[] += 1; error("release failed")))
        @test_throws ErrorException forceclose!(r)
        @test calls[] == 1
        @test AC.phase(@atomic r.state) == AC.PHASE_CLOSED
        @test forceclose!(r)
        @test calls[] == 1
    end


    @testset "one closer owns the release callback" begin
        bytes = UInt8[0]
        entered = Base.Event()
        finish = Base.Event()
        calls = Threads.Atomic{Int}(0)
        r = GC.@preserve bytes AC.OwnerRegion(Ptr{UInt8}(pointer(bytes)), 1, AC.Foreign;
            root=bytes, releasefn=_ -> begin
                Threads.atomic_add!(calls, 1)
                notify(entered)
                wait(finish)
            end)
        first = Threads.@spawn forceclose!(r)
        wait(entered)
        @test forceclose!(r; timeout_ms=0) == false
        @test AC.phase(@atomic r.state) == AC.PHASE_CLOSING
        waiter = Threads.@spawn forceclose!(r)
        notify(finish)
        @test fetch(first)
        @test fetch(waiter)
        @test calls[] == 1
        @test AC.phase(@atomic r.state) == AC.PHASE_CLOSED
    end

    @testset "manual finalization honors an active guard" begin
        bytes = UInt8[0]
        calls = Ref(0)
        r = GC.@preserve bytes AC.OwnerRegion(Ptr{UInt8}(pointer(bytes)), 1, AC.Foreign;
            root=bytes, releasefn=_ -> (calls[] += 1))
        withguard(r) do
            finalize(r)
            @test calls[] == 0
            @test AC.phase(@atomic r.state) == AC.PHASE_OPEN
        end
        finalize(r)
        @test calls[] == 1
        @test AC.phase(@atomic r.state) == AC.PHASE_CLOSED
    end
end

@testset "BufferSlice bounds" begin
    r = heapregion(zeros(UInt8, 16))
    @test_throws ArgumentError BufferSlice(r, 0, 17)
    @test_throws ArgumentError BufferSlice(r, 16, 1)
    @test_throws ArgumentError BufferSlice(r, -1, 4)
    b = BufferSlice(r, 8, 8)
    @test length(b) == 8
    @test_throws ArgumentError AC.subslice(b, 4, 8)   # 4+8 > 8
    @test_throws ArgumentError AC.subslice(b, -1, 1)  # cannot escape parent span
    @test_throws ArgumentError AC.subslice(b, 0, -1)
    sub = AC.subslice(b, 4, 4)
    @test length(sub) == 4
    # loadat re-checks: last line of defense before the pointer
    @test_throws BoundsError AC.loadat(b, UInt64, Int64(1))
    @test_throws BoundsError AC.loadat(b, UInt8, Int64(8))
    @test_throws BoundsError AC.loadat(b, UInt8, typemax(Int64))
    # empty buffer
    e = BufferSlice()
    @test length(e) == 0
    @test AC.isempty_buffer(e)
end

@testset "unaligned loads" begin
    bytes = UInt8[0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]
    r = heapregion(bytes)
    b = BufferSlice(r, 0, 9)
    # offset 1 is misaligned for Int64; must still read correctly
    v = AC.loadat(b, Int64, Int64(1))
    @test v == Int64(1) | (Int64(2) << 56)
end

@testset "layout registry covers all format-1.5 layouts" begin
    types = AC.ArrowType[
        NullType(), BoolType(), IntType(32, true), IntType(64, false),
        FloatType(64), DecimalType(10, 2, 128), DecimalType(9, 2, 32),
        FixedSizeBinaryType(16), BinaryType(false), BinaryType(true),
        Utf8Type(false), Utf8Type(true), DateType(AC.DAY),
        TimeType(AC.NANOSECOND, 64), TimestampType(AC.MICROSECOND, "UTC"),
        DurationType(AC.MILLISECOND), IntervalType(AC.MONTH_DAY_NANO),
        ListType(false), ListType(true), FixedSizeListType(3), StructType(),
        MapType(false), UnionType(AC.DenseMode, Int8[0, 1]),
        UnionType(AC.SparseMode, Int8[0, 1]),
        DictionaryType(IntType(32, true), Utf8Type(false), false),
        ViewType(true), ListViewType(false), RunEndEncodedType(),
    ]
    for t in types
        spec = layoutspec(t)
        @test spec isa LayoutSpec
        # offsets width only ever 0/4/8
        @test spec.offsetwidth in (0, 4, 8)
    end
    # two timestamps with different timezones: same Julia type (the #503 fix)
    @test typeof(TimestampType(AC.SECOND, "America/Denver")) ==
          typeof(TimestampType(AC.NANOSECOND, nothing))

    # Schema/data containers defensively copy mutable caller input and expose
    # no normal mutation API. This keeps semantic-cache results stable.
    ids = Int8[0, 1]
    ut = UnionType(AC.SparseMode, ids)
    ids[1] = 7
    @test collect(ut.typeids) == Int8[0, 1]
    @test_throws Exception setindex!(ut.typeids, Int8(7), 1)
    children = Field[fromjulia("x", Int64[])[1]]
    frozen = Field("l", ListType(false); children=children)
    empty!(children)
    @test length(frozen.children) == 1

    # ListView offsets are per-slot and may be unordered; view data buffers
    # are variadic after the fixed validity/views pair.
    cf, cd = fromjulia("item", Int64[1, 2, 3])
    lvt = ListViewType(false)
    lvf = Field("lv", lvt; children=[cf])
    lvd = AC.ArrayData(lvt, 2,
        [BufferSlice(), AC._databuffer(Int32[2, 0]), AC._databuffer(Int32[1, 2])];
        children=[cd], nullcount=0)
    @test validate_structural(lvf, lvd) === lvd
    @test validate_semantic(lvf, lvd) === lvd
    vt = ViewType(true)
    vf = Field("v", vt)
    vd = AC.ArrayData(vt, 1,
        [BufferSlice(), AC._databuffer(zeros(UInt8, 16)), AC._databuffer(UInt8[0x61])];
        nullcount=0)
    @test validate_structural(vf, vd) === vd
end

@testset "fromjulia round-trips" begin
    @testset "zero-copy primitive" begin
        v = Int64[10, 20, 30]
        f, d = fromjulia("x", v)
        @test f.type == IntType(64, true)
        @test nullcount(d) == 0
        @test [getvalue(f, d, i) for i = 1:3] == v
        @test materialize(f, d) == v
    end

    @testset "nullable primitive" begin
        f, d = fromjulia("x", [1.5, missing, 3.5])
        validate_structural(f, d)
        @test nullcount(d) == 1
        @test isequal(materialize(f, d), [1.5, missing, 3.5])
    end

    @testset "bool with missings (bit-packed values)" begin
        f, d = fromjulia("b", [true, missing, false, true])
        @test isequal(materialize(f, d), [true, missing, false, true])
    end

    @testset "strings incl. empty and missing" begin
        vals = ["hey", "", missing, "αβ∀"]
        f, d = fromjulia("s", collect(vals))
        validate_structural(f, d)
        validate_semantic(f, d)
        validate_full(f, d)
        @test isequal(materialize(f, d), vals)
    end

    @testset "list of ints with missing" begin
        vals = [[1, 2], Int[], missing, [3]]
        f, d = fromjulia("l", collect(vals))
        validate_structural(f, d)
        validate_semantic(f, d)
        out = materialize(f, d)
        @test isequal(out, [[1, 2], Int[], missing, [3]])
    end

    @testset "struct" begin
        f, d = AC.fromjulia_struct("st", (a=Int64[1, 2], b=["x", "y"]))
        validate_structural(f, d)
        @test materialize(f, d) == [(a=1, b="x"), (a=2, b="y")]
    end

    @testset "dictionary-encoded" begin
        f, d = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, missing, 0])
        validate_structural(f, d)
        validate_semantic(f, d)
        @test isequal(materialize(f, d), ["lo", "hi", missing, "lo"])
    end

    @testset "canonical empty offset arrays" begin
        st = Utf8Type(false)
        sf = Field("s", st)
        sd = AC.ArrayData(st, 0,
            [BufferSlice(), BufferSlice(), BufferSlice()]; nullcount=0)
        @test validate_structural(sf, sd) === sd
        @test validate_semantic(sf, sd) === sd
        @test isempty(materialize(sf, sd))

        cf, cd = fromjulia("item", Int64[])
        lt = ListType(false)
        lf = Field("l", lt; children=[cf])
        ld = AC.ArrayData(lt, 0, [BufferSlice(), BufferSlice()];
            children=[cd], nullcount=0)
        @test validate_structural(lf, ld) === ld
        @test validate_semantic(lf, ld) === ld
        @test isempty(materialize(lf, ld))
    end

    @testset "dictionary pool nullability is independent" begin
        vf, vd = fromjulia("pool", Union{Missing,String}[missing, "x"])
        t = DictionaryType(IntType(32, true), vf.type, false)
        f = Field("d", t; nullable=false, children=vf.children)
        d = AC.ArrayData(t, 2,
            [BufferSlice(), AC._databuffer(Int32[0, 1])];
            dictionary=vd, nullcount=0)
        validate_structural(f, d)
        validate_semantic(f, d)
        @test isequal(materialize(f, d), [missing, "x"])
    end
end

# Layouts fromjulia doesn't build: construct by hand to prove the accessors.
@testset "hand-built layouts" begin
    @testset "fixed-size list" begin
        t = FixedSizeListType(2)
        cf, cd = fromjulia("item", Int64[1, 2, 3, 4, 5, 6])
        f = Field("fsl", t; children=[cf])
        d = AC.ArrayData(t, 3, [BufferSlice()]; children=[cd], nullcount=0)
        validate_structural(f, d)
        @test materialize(f, d) == [[1, 2], [3, 4], [5, 6]]
    end

    @testset "map" begin
        # map<utf8, int64>: entries struct("key","value"), offsets [0,2,3]
        kf, kd = fromjulia("key", ["a", "b", "c"])
        vf, vd = fromjulia("value", Int64[1, 2, 3])
        ef = Field("entries", StructType(); nullable=false, children=[kf, vf])
        ed = AC.ArrayData(StructType(), 3, [BufferSlice()]; children=[kd, vd], nullcount=0)
        offs = Int32[0, 2, 3]
        t = MapType(false)
        f = Field("m", t; children=[ef])
        d = AC.ArrayData(t, 2, [BufferSlice(), AC._databuffer(offs)];
            children=[ed], nullcount=0)
        validate_structural(f, d)
        validate_semantic(f, d)
        @test materialize(f, d) == [["a" => 1, "b" => 2], ["c" => 3]]
    end

    @testset "map applies the entries struct offset" begin
        kf, kd = fromjulia("key", ["skip", "a", "b"])
        vf, vd = fromjulia("value", Int64[0, 1, 2])
        ef = Field("entries", StructType(); nullable=false, children=[kf, vf])
        ed = AC.ArrayData(StructType(), 2, [BufferSlice()]; offset=1,
            children=[kd, vd], nullcount=0)
        t = MapType(false)
        f = Field("m", t; children=[ef])
        d = AC.ArrayData(t, 1,
            [BufferSlice(), AC._databuffer(Int32[0, 2])]; children=[ed], nullcount=0)
        validate_structural(f, d)
        validate_semantic(f, d)
        @test materialize(f, d) == [["a" => 1, "b" => 2]]
    end

    @testset "empty large-list range does not wrap" begin
        cf = Field("item", NullType())
        cd = AC.ArrayData(NullType(), typemax(Int64), BufferSlice[])
        t = ListType(true)
        f = Field("list", t; children=[cf])
        d = AC.ArrayData(t, 1,
            [BufferSlice(), AC._databuffer(Int64[typemax(Int64), typemax(Int64)])];
            children=[cd], nullcount=0)
        validate_structural(f, d)
        validate_semantic(f, d)
        @test getvalue(f, d, 1) == Any[]
    end

    @testset "dense union" begin
        t = UnionType(AC.DenseMode, Int8[0, 1])
        af, ad = fromjulia("i", Int64[10, 20])
        bf, bd = fromjulia("s", ["x"])
        f = Field("u", t; children=[af, bf])
        typeids = Int8[0, 1, 0]
        offsets = Int32[0, 0, 1]
        d = AC.ArrayData(t, 3,
            [AC._databuffer(typeids), AC._databuffer(offsets)];
            children=[ad, bd])
        validate_structural(f, d)
        validate_semantic(f, d)
        @test materialize(f, d) == [10, "x", 20]
    end

    @testset "sparse union" begin
        t = UnionType(AC.SparseMode, Int8[0, 1])
        af, ad = fromjulia("i", Int64[10, 20, 30])
        bf, bd = fromjulia("s", ["x", "y", "z"])
        f = Field("u", t; children=[af, bf])
        d = AC.ArrayData(t, 3, [AC._databuffer(Int8[0, 1, 0])];
            children=[ad, bd])
        validate_structural(f, d)
        @test materialize(f, d) == [10, "y", 30]
    end

    @testset "interval MONTH_DAY_NANO (the unit 2.x cannot parse)" begin
        t = IntervalType(AC.MONTH_DAY_NANO)
        raw = vcat(reinterpret(UInt8, Int32[1, 2]), reinterpret(UInt8, Int64[3]))
        f = Field("iv", t; nullable=false)
        d = AC.ArrayData(t, 1, [BufferSlice(), AC._databuffer(collect(raw))]; nullcount=0)
        validate_structural(f, d)
        @test getvalue(f, d, 1) == (months=1, days=2, nanos=3)
    end

    @testset "decimal32/64 read at the right width (the 2.x misread)" begin
        for (bits, T) in ((32, Int32), (64, Int64))
            t = DecimalType(9, 2, bits)
            vals = T[12345, -678]
            f = Field("dec", t; nullable=false)
            d = AC.ArrayData(t, 2, [BufferSlice(), AC._databuffer(vals)]; nullcount=0)
            validate_structural(f, d)
            @test [getvalue(f, d, i) for i = 1:2] == vals
        end
    end

    @testset "logical offset (sliced data)" begin
        v = Int64[1, 2, 3, 4, 5]
        t = IntType(64, true)
        f = Field("x", t; nullable=false)
        d = AC.ArrayData(t, 3, [BufferSlice(), AC._databuffer(v)]; offset=2, nullcount=0)
        validate_structural(f, d)
        @test materialize(f, d) == [3, 4, 5]
    end

    @testset "logical offset in struct and sparse union" begin
        af, ad = fromjulia("a", Int64[10, 20, 30])
        sf = Field("st", StructType(); children=[af])
        sd = AC.ArrayData(StructType(), 2, [BufferSlice()]; offset=1,
            children=[ad], nullcount=0)
        validate_structural(sf, sd)
        @test materialize(sf, sd) == [(a=20,), (a=30,)]

        uf = Field("u", UnionType(AC.SparseMode, Int8[0, 1]);
            children=[af, fromjulia("b", ["x", "y", "z"])[1]])
        bd = fromjulia("b", ["x", "y", "z"])[2]
        ud = AC.ArrayData(uf.type, 2, [AC._databuffer(Int8[0, 1, 0])];
            offset=1, children=[ad, bd], nullcount=0)
        validate_structural(uf, ud)
        validate_semantic(uf, ud)
        @test materialize(uf, ud) == ["y", 30]
    end

    @testset "view/REE layouts: registry-known, access explicitly unsupported" begin
        t = RunEndEncodedType()
        ref, red = fromjulia("run_ends", Int32[2, 3])
        vf, vd = fromjulia("values", Int64[7, 9])
        f = Field("ree", t; children=[ref, vf])
        d = AC.ArrayData(t, 3, BufferSlice[]; children=[red, vd])
        validate_structural(f, d)  # structure IS validated
        @test_throws ErrorException getvalue(f, d, 1)
    end
end

@testset "staged validation rejects corrupt metadata" begin
    @testset "structural: descriptor values and field shape must match" begin
        f = Field("x", IntType(32, true))
        d = AC.ArrayData(IntType(64, true), 1,
            [BufferSlice(), AC._databuffer(Int64[1])]; nullcount=0)
        @test_throws ValidationError validate_structural(f, d)
        @test_throws ValidationError validate_structural(
            Field("l", ListType(false)),
            AC.ArrayData(ListType(false), 1,
                [BufferSlice(), AC._databuffer(Int32[0, 0])];
                children=[fromjulia("item", Int64[])[2]], nullcount=0))
        badt = IntType(24, true)
        @test_throws ValidationError validate_structural(Field("bad", badt),
            AC.ArrayData(badt, 1,
                [BufferSlice(), AC._databuffer(UInt8[0, 0, 0])]; nullcount=0))
    end

    @testset "structural: wrong buffer arity" begin
        t = IntType(64, true)
        f = Field("x", t)
        d = AC.ArrayData(t, 3, [BufferSlice()])   # missing DATA buffer
        @test_throws ValidationError validate_structural(f, d)
    end

    @testset "structural: short data buffer (checked arithmetic)" begin
        t = IntType(64, true)
        f = Field("x", t)
        short = AC._databuffer(Int64[1])          # 8 bytes for len=3
        d = AC.ArrayData(t, 3, [BufferSlice(), short])
        @test_throws ValidationError validate_structural(f, d)
    end

    @testset "structural: short offsets buffer" begin
        t = Utf8Type(false)
        f = Field("s", t)
        offs = AC._databuffer(Int32[0, 1])        # need len+1 = 4 entries
        d = AC.ArrayData(t, 3, [BufferSlice(), offs, BufferSlice()])
        @test_throws ValidationError validate_structural(f, d)
    end

    @testset "semantic: non-monotonic offsets" begin
        t = Utf8Type(false)
        f = Field("s", t)
        offs = AC._databuffer(Int32[0, 2, 1, 3])
        data = AC._databuffer(UInt8[0x61, 0x62, 0x63])
        d = AC.ArrayData(t, 3, [BufferSlice(), offs, data])
        validate_structural(f, d)
        @test_throws ValidationError validate_semantic(f, d)
    end

    @testset "semantic: final offset beyond data extent" begin
        t = Utf8Type(false)
        f = Field("s", t)
        offs = AC._databuffer(Int32[0, 1, 2, 99])
        data = AC._databuffer(UInt8[0x61, 0x62, 0x63])
        d = AC.ArrayData(t, 3, [BufferSlice(), offs, data])
        validate_structural(f, d)
        @test_throws ValidationError validate_semantic(f, d)
    end

    @testset "semantic: dictionary index out of bounds" begin
        f, d = AC.fromjulia_dict("d", ["a", "b"], [0, 1])
        # corrupt: poke an index past the pool through a rebuilt ArrayData
        bad = AC.ArrayData(d.type, d.len, [d.buffers[1], AC._databuffer(Int32[0, 7])];
            dictionary=d.dictionary)
        @test_throws ValidationError validate_semantic(f, bad)
    end

    @testset "semantic: union type id outside declared domain" begin
        t = UnionType(AC.SparseMode, Int8[0, 1])
        af, ad = fromjulia("i", Int64[1, 2])
        bf, bd = fromjulia("s", ["x", "y"])
        f = Field("u", t; children=[af, bf])
        d = AC.ArrayData(t, 2, [AC._databuffer(Int8[0, 5])]; children=[ad, bd])
        validate_structural(f, d)
        @test_throws ValidationError validate_semantic(f, d)
    end

    @testset "union ids and dense offsets obey the format" begin
        af, ad = fromjulia("i", Int64[1, 2])
        bf, bd = fromjulia("j", Int64[3, 4])
        dupt = UnionType(AC.SparseMode, Int8[0, 0])
        @test_throws ValidationError validate_structural(
            Field("u", dupt; children=[af, bf]),
            AC.ArrayData(dupt, 2, [AC._databuffer(Int8[0, 0])];
                children=[ad, bd], nullcount=0))

        negt = UnionType(AC.SparseMode, Int8[-1, 0])
        @test_throws ValidationError validate_structural(
            Field("u", negt; children=[af, bf]),
            AC.ArrayData(negt, 2, [AC._databuffer(Int8[-1, 0])];
                children=[ad, bd], nullcount=0))

        shortt = UnionType(AC.SparseMode, Int8[0])
        @test_throws ValidationError validate_structural(
            Field("u", shortt; children=[af, bf]),
            AC.ArrayData(shortt, 2, [AC._databuffer(Int8[0, 0])];
                children=[ad, bd], nullcount=0))

        t = UnionType(AC.DenseMode, Int8[0])
        f = Field("u", t; children=[af])
        d = AC.ArrayData(t, 2,
            [AC._databuffer(Int8[0, 0]), AC._databuffer(Int32[1, 0])];
            children=[ad], nullcount=0)
        validate_structural(f, d)
        @test_throws ValidationError validate_semantic(f, d)

        # Equal dense offsets are valid; only decreases are forbidden.
        repeated = AC.ArrayData(t, 2,
            [AC._databuffer(Int8[0, 0]), AC._databuffer(Int32[0, 0])];
            children=[ad], nullcount=0)
        validate_structural(f, repeated)
        @test validate_semantic(f, repeated) === repeated
    end

    @testset "structural: nested REE is forbidden" begin
        rf, rd = fromjulia("run_ends", Int32[1])
        vf, vd = fromjulia("values", Int64[1])
        innerf = Field("values", RunEndEncodedType(); children=[rf, vf])
        innerd = AC.ArrayData(RunEndEncodedType(), 1, BufferSlice[];
            children=[rd, vd], nullcount=0)
        outerf = Field("ree", RunEndEncodedType(); children=[rf, innerf])
        outerd = AC.ArrayData(RunEndEncodedType(), 1, BufferSlice[];
            children=[rd, innerd], nullcount=0)
        @test_throws ValidationError validate_structural(outerf, outerd)
    end

    @testset "structural: map uses canonical child names" begin
        kf, kd = fromjulia("key", ["a"])
        vf, vd = fromjulia("value", Int64[1])
        entries = Field("entries", StructType(); nullable=false,
            children=[kf, vf])
        ed = AC.ArrayData(StructType(), 1, [BufferSlice()];
            children=[kd, vd], nullcount=0)
        t = MapType(false)
        d = AC.ArrayData(t, 1,
            [BufferSlice(), AC._databuffer(Int32[0, 1])];
            children=[ed], nullcount=0)
        @test validate_structural(Field("m", t; children=[entries]), d) === d
        wrongentries = Field("not_entries", StructType(); nullable=false,
            children=[kf, vf])
        wrongkey = Field("entries", StructType(); nullable=false,
            children=[Field("not_key", kf.type; nullable=kf.nullable), vf])
        wrongvalue = Field("entries", StructType(); nullable=false,
            children=[kf, Field("not_value", vf.type; nullable=vf.nullable)])
        for badentries in (wrongentries, wrongkey, wrongvalue)
            @test_throws ValidationError validate_structural(
                Field("m", t; children=[badentries]), d)
        end
    end

    @testset "semantic: declared null count matches bitmap" begin
        f, d = fromjulia("x", [1, missing])
        bad = AC.ArrayData(d.type, d.len, d.buffers; nullcount=0)
        validate_structural(f, bad)
        @test_throws ValidationError validate_semantic(f, bad)
        absent = AC.ArrayData(d.type, d.len,
            [BufferSlice(), d.buffers[2]]; nullcount=1)
        @test_throws ValidationError validate_structural(f, absent)
        @test_throws ArgumentError AC.ArrayData(d.type, d.len, d.buffers; nullcount=3)
    end

    @testset "field nullability is checked outside the data cache" begin
        nullable, d = fromjulia("x", [1, missing])
        validate_semantic(nullable, d)
        @test (@atomic d.semachecked)
        nonnullable = Field("x", d.type; nullable=false)
        @test_throws ValidationError validate_semantic(nonnullable, d)

        af, ad = fromjulia("a", Union{Missing,Int64}[missing])
        uf = Field("u", UnionType(AC.DenseMode, Int8[0]); nullable=false,
            children=[af])
        ud = AC.ArrayData(uf.type, 1,
            [AC._databuffer(Int8[0]), AC._databuffer(Int32[0])];
            children=[ad], nullcount=0)
        validate_structural(uf, ud)
        @test_throws ValidationError validate_semantic(uf, ud)
    end

    @testset "full: invalid UTF-8" begin
        t = Utf8Type(false)
        f = Field("s", t)
        offs = AC._databuffer(Int32[0, 2])
        data = AC._databuffer(UInt8[0xff, 0xfe])
        d = AC.ArrayData(t, 1, [BufferSlice(), offs, data])
        validate_structural(f, d)
        validate_semantic(f, d)
        @test_throws ValidationError validate_full(f, d)
    end

    @testset "full: invalid UTF-8 in dictionary values" begin
        vt = Utf8Type(false)
        vd = AC.ArrayData(vt, 1,
            [BufferSlice(), AC._databuffer(Int32[0, 1]), AC._databuffer(UInt8[0xff])];
            nullcount=0)
        t = DictionaryType(IntType(32, true), vt, false)
        f = Field("d", t; nullable=false)
        d = AC.ArrayData(t, 1,
            [BufferSlice(), AC._databuffer(Int32[0])]; dictionary=vd, nullcount=0)
        validate_structural(f, d)
        validate_semantic(f, d)
        @test_throws ValidationError validate_full(f, d)
    end

    @testset "semantic result is cached" begin
        f, d = fromjulia("s", ["a", "b"])
        @test !(@atomic d.semachecked)
        validate_semantic(f, d)
        @test (@atomic d.semachecked)
        validate_semantic(f, d)   # second call is the cached no-op path
        @test (@atomic d.semachecked)
    end
end

@testset "nullcount is lazy and cached" begin
    f, d = fromjulia("x", [1, missing, missing, 4])
    @test (@atomic d.nullcount) == 2   # builder knew it
    d2 = AC.ArrayData(d.type, d.len, d.buffers)  # unknown (-1)
    @test (@atomic d2.nullcount) == -1
    @test nullcount(d2) == 2
    @test (@atomic d2.nullcount) == 2
end

@testset "RecordBatch" begin
    b = batch((a=Int64[1, 2, 3], b=["x", "y", "z"]))
    @test b.nrows == 3
    @test length(b.schema.fields) == 2
    @test materialize(b.schema.fields[1], b.columns[1]) == [1, 2, 3]
    @test_throws ArgumentError RecordBatch(b.schema,
        [b.columns[1], AC.fromjulia("b", ["only-one"])[2]])
    empty_schema = Schema(Field[])
    @test RecordBatch(empty_schema, ArrayData[], 7).nrows == 7
end

end # ArrowCore testset

# The required standalone command commonly starts Julia with one thread.
# Run the memory-order stress in a small four-thread child so this gate tests
# real OS-thread interleavings on every invocation.
run(`$(Base.julia_cmd()) --startup-file=no --threads=4 $(joinpath(@__DIR__, "threaded_stress.jl"))`)
