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

# Core unit tests: ArrowCore's regions, descriptors, layouts, validation,
# accessors, and builders — plus the two adapter seams the builders feed
# (IPC write/read and C data export/import) where a Core column must cross.

using Test

# Top-level release-action trampoline for the ReleaseCell test (the closure
# form of @cfunction is unsupported on some platforms).
function _cell_bump(p::Ptr{Cvoid})::Cvoid
    (unsafe_pointer_to_objref(p)::Base.RefValue{Int})[] += 1
    return nothing
end

using Arrow
using Arrow.ArrowCore
const AC = ArrowCore

struct ManagedLoad
    value::Any
end

struct GcTriggeredLoad
    value::UInt8
end

struct UnregisteredArrowType <: ArrowType end

mutable struct RegionRootProbe
    bytes::Vector{UInt8}
    finalized::Base.RefValue{Bool}
end

function AC.datatype_alignment(::Type{GcTriggeredLoad})
    GC.gc(true)
    return 1
end

@noinline function load_while_collecting(finalized)
    root = RegionRootProbe(UInt8[0x2a], finalized)
    finalizer(root) do probe
        probe.finalized[] = true
    end
    region = AC.OwnerRegion(Ptr{UInt8}(pointer(root.bytes)), 1; root=root)
    slice = BufferSlice(region, 0, 1)
    return AC.loadat(slice, GcTriggeredLoad, Int64(0)), finalized[]
end

@noinline function read_mapped_slice_while_collecting(path)
    # Keep only the slice local. Its region must be enough to retain the Mmap
    # array through collection and the raw loads below.
    b = BufferSlice(mmapregion(path), 0, 8)
    rooted = b.region.root isa Vector{UInt8}
    GC.gc(true)
    return rooted,
    AC.loadat(b, UInt8, Int64(0)),
    AC.loadat(b, UInt32, Int64(4)),
    AC.loadat(b, UInt8, Int64(7))
end

@testset "ArrowCore" begin
    @testset "OwnerRegion: reachability-based validity" begin
        @testset "heap wrap is zero-copy and rooted" begin
            v = Int64[1, 2, 3, 4]
            r = heapregion(v)
            @test r.len == 32
            @test r.root === v
            b = BufferSlice(r, 0, 32)
            @test AC.loadat(b, Int64, Int64(0)) == 1
            @test AC.loadat(b, Int64, Int64(24)) == 4
            # Regions are immutable values: nothing to close, nothing to race.
            @test_throws ErrorException setproperty!(r, :root, nothing)
        end

        @testset "raw load preserves the region root" begin
            finalized = Ref(false)
            value, finalized_during_load = load_while_collecting(finalized)
            @test value == GcTriggeredLoad(0x2a)
            @test !finalized_during_load
        end

        @testset "construction validation" begin
            @test_throws ArgumentError AC.OwnerRegion(Ptr{UInt8}(0), 1)
            @test_throws ArgumentError AC.OwnerRegion(Ptr{UInt8}(8), -1)
            @test_throws ArgumentError AC.OwnerRegion(Ptr{UInt8}(8), 1)
            # extents that would wrap native pointer arithmetic are rejected
            @test_throws ArgumentError AC.OwnerRegion(
                Ptr{UInt8}(typemax(UInt) - 8),
                64;
                root=UInt8[],
            )
            @test_throws ArgumentError heapregion(["not", "isbits"])
        end

        @testset "mapped region: stdlib-backed, reachability-valid" begin
            path = tempname()
            write(path, UInt8[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88])
            rooted, firstbyte, lastword, lastbyte = read_mapped_slice_while_collecting(path)
            @test rooted
            @test firstbyte == 0x11
            @test lastword == 0x88776655
            @test lastbyte == 0x88
            emptypath = tempname()
            touch(emptypath)
            @test_throws ArgumentError mmapregion(emptypath)
            rm(emptypath)
            @test_throws SystemError mmapregion(tempname())
            # The helper returned no region or slice. Collect the stdlib mapping
            # before deleting the path on platforms that lock active mappings.
            GC.gc(true)
            GC.gc(true)
            rm(path)
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
        @test_throws ArgumentError OwnerRegion(Ptr{UInt8}(typemax(UInt)), 2)
        # empty buffer
        e = BufferSlice()
        @test length(e) == 0
        @test AC.isempty_buffer(e)
        # Arbitrary bytes must never become managed Julia references.
        managed = BufferSlice(r, 0, sizeof(ManagedLoad))
        @test_throws ArgumentError AC.loadat(managed, ManagedLoad, Int64(0))
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
            NullType(),
            BoolType(),
            IntType(32, true),
            IntType(64, false),
            FloatType(64),
            DecimalType(10, 2, 128),
            DecimalType(9, 2, 32),
            FixedSizeBinaryType(16),
            BinaryType(false),
            BinaryType(true),
            Utf8Type(false),
            Utf8Type(true),
            DateType(AC.DAY),
            TimeType(AC.NANOSECOND, 64),
            TimestampType(AC.MICROSECOND, "UTC"),
            DurationType(AC.MILLISECOND),
            IntervalType(AC.MONTH_DAY_NANO),
            ListType(false),
            ListType(true),
            FixedSizeListType(3),
            StructType(),
            MapType(false),
            UnionType(AC.DenseMode, Int8[0, 1]),
            UnionType(AC.SparseMode, Int8[0, 1]),
            DictionaryType(IntType(32, true), Utf8Type(false), false),
            ViewType(true),
            ListViewType(false),
            RunEndEncodedType(),
        ]
        for t in types
            spec = layoutspec(t)
            @test spec isa LayoutSpec
            # offsets width only ever 0/4/8
            @test spec.offsetwidth in (0, 4, 8)
        end
        @test_throws ArgumentError layoutspec(UnregisteredArrowType())
        @test_throws ArgumentError AC._validate_descriptor(UnregisteredArrowType())
        # two timestamps with different timezones: same Julia type
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

        # Sequential metadata is the lossless representation: preserve order and
        # duplicate keys while defensively copying the caller's container.
        ordered_metadata = ["k" => "first", "k" => "second", "z" => "last"]
        metadata_field = Field("m", IntType(8, true); metadata=ordered_metadata)
        pop!(ordered_metadata)
        @test collect(metadata_field.metadata) ==
              ["k" => "first", "k" => "second", "z" => "last"]
        @test collect(
            Schema([metadata_field]; metadata=("a" => "1", "a" => "2")).metadata,
        ) == ["a" => "1", "a" => "2"]
        @test_throws ArgumentError Field("m", IntType(8, true); metadata=["not a pair"])
        @test_throws ArgumentError Schema([metadata_field]; metadata=("not a pair",))

        # ListView offsets are per-slot and may be unordered; view data buffers
        # are variadic after the fixed validity/views pair. Both validate and
        # read end-to-end.
        cf, cd = fromjulia("item", Int64[1, 2, 3])
        lvt = ListViewType(false)
        lvf = Field("lv", lvt; children=[cf])
        lvd = AC.ArrayData(
            lvt,
            2,
            [BufferSlice(), AC._databuffer(Int32[2, 0]), AC._databuffer(Int32[1, 2])];
            children=[cd],
            nullcount=0,
        )
        @test validate_structural(lvf, lvd) === lvd
        @test validate_semantic(lvf, lvd) === lvd
        @test getvalue(lvf, lvd, 1) == [3]          # unordered offsets: slot 1 reads the tail
        @test getvalue(lvf, lvd, 2) == [1, 2]
        vt = ViewType(true)
        vf = Field("v", vt)
        vd = AC.ArrayData(
            vt,
            1,
            [BufferSlice(), AC._databuffer(zeros(UInt8, 16)), AC._databuffer(UInt8[0x61])];
            nullcount=0,
        )
        @test validate_structural(vf, vd) === vd
        @test validate_semantic(vf, vd) === vd
        @test getvalue(vf, vd, 1) == ""             # zeroed entry: inline empty string
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

        @testset "release! releases deterministically" begin
            f, d = fromjulia("x", Int64[1, 2, 3])
            buf = d.buffers[2]
            @test AC.loadat(buf, Int64, Int64(0)) == 1
            r = buf.region::OwnerRegion
            # A heap region is a BORROW: release! revokes but must not run the
            # caller's own finalizers on the borrowed vector.
            borrowed = r.root::Vector{Int64}
            callerfin = Ref(false)
            finalizer(_ -> callerfin[] = true, borrowed)
            release!(r)
            @test !callerfin[]
            @test_throws InvalidStateException AC.loadat(buf, Int64, Int64(0))
            @test_throws InvalidStateException AC.slicebytes(buf)
            @test_throws InvalidStateException materialize(f, d)
            release!(r)   # idempotent
            GC.@preserve borrowed nothing

            # Regions sharing one ReleaseCell are revoked together and the
            # release action runs exactly once.
            released = Ref(0)
            cell = ReleaseCell(@cfunction(_cell_bump, Cvoid, (Ptr{Cvoid},)), released)
            v1, v2 = UInt8[1, 2], UInt8[3, 4]
            ra = GC.@preserve v1 OwnerRegion(pointer(v1), 2; root=v1, cell=cell)
            rb = GC.@preserve v2 OwnerRegion(pointer(v2), 2; root=v2, cell=cell)
            sa, sb = BufferSlice(ra, 0, 2), BufferSlice(rb, 0, 2)
            @test AC.loadat(sb, UInt8, Int64(0)) == 0x03
            release!(ra)
            @test_throws InvalidStateException AC.loadat(sa, UInt8, Int64(0))
            @test_throws InvalidStateException AC.loadat(sb, UInt8, Int64(0))
            release!(rb)
            @test released[] == 1

            # An mmap-backed region actually unmaps NOW: the release targets the
            # object Mmap registered the unmap finalizer on (`_mmaproot`), and
            # the observer proves it ran — rm() alone would not, since POSIX
            # happily unlinks mapped files.
            path, io = mktemp()
            write(io, zeros(UInt8, 64))
            close(io)
            mr = mmapregion(path)
            mslice = BufferSlice(mr, 0, mr.len)
            @test AC.loadat(mslice, UInt8, Int64(0)) == 0x00
            unmapped = Ref(false)
            finalizer(_ -> unmapped[] = true, AC._mmaproot(mr.root::Vector{UInt8}))
            release!(mr)
            @test unmapped[]
            @test_throws InvalidStateException AC.loadat(mslice, UInt8, Int64(0))
            rm(path)
        end

        @testset "canonical bit-packed form is full-tier only" begin
            # A junk trailing bit in the final validity byte: semantic accepts
            # (readers must not rely on unused bits), validate_full rejects.
            vals = Union{Missing,Int64}[1, missing, 3]
            f, d = fromjulia("x", vals)
            vbytes = AC.slicebytes(AC.validitybuffer(d))
            junk = copy(vbytes)
            junk[end] |= 0x80                     # bit 8 of a 3-element bitmap
            jd = AC.ArrayData(
                d.type,
                d.len,
                [AC._databuffer(junk), d.buffers[2]];
                nullcount=1,
            )
            @test validate_semantic(f, jd) === jd
            @test_throws ValidationError AC.validate_full(f, jd)
            @test AC.validate_full(f, d) === d    # canonical original passes
            # Sliced windows are exempt: trailing bits may belong to a sibling.
            sliced = AC.ArrayData(
                d.type,
                2,
                [AC._databuffer(junk), d.buffers[2]];
                offset=1,
                nullcount=1,
            )
            @test validate_semantic(f, sliced) === sliced
            @test AC.validate_full(f, sliced) === sliced
        end

        @testset "list of ints with missing" begin
            vals = [[1, 2], Int[], missing, [3]]
            f, d = fromjulia("l", collect(vals))
            validate_structural(f, d)
            validate_semantic(f, d)
            out = materialize(f, d)
            @test isequal(out, [[1, 2], Int[], missing, [3]])
        end

        @testset "empty lists preserve their declared child type" begin
            ff, fd = fromjulia("empty-float-list", Vector{Vector{Float64}}())
            @test ff.children[1].type == FloatType(64)
            @test isempty(materialize(ff, fd))

            uf, ud = fromjulia(
                "missing-uint-list",
                Union{Missing,Vector{UInt8}}[missing, missing],
            )
            @test uf.children[1].type == IntType(8, false)
            @test isequal(materialize(uf, ud), [missing, missing])

            nf, nd = fromjulia("null-list", [Missing[missing, missing], Missing[]])
            @test nf.children[1].type isa NullType
            @test nf.children[1].nullable
            @test isequal(materialize(nf, nd), [Missing[missing, missing], Missing[]])
            @test_throws ArgumentError fromjulia("bottom", Union{}[])
        end

        @testset "struct" begin
            f, d = AC.fromjulia_struct("st", (a=Int64[1, 2], b=["x", "y"]))
            validate_structural(f, d)
            # Core struct scalars are ordered name=>value pairs; the NamedTuple
            # surface is the facade's (or a static claim through getvalue(::Type{T}, ...)).
            @test materialize(f, d) == [["a" => 1, "b" => "x"], ["a" => 2, "b" => "y"]]
        end

        @testset "dictionary-encoded" begin
            f, d = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, missing, 0])
            validate_structural(f, d)
            validate_semantic(f, d)
            @test isequal(materialize(f, d), ["lo", "hi", missing, "lo"])
        end

        @testset "maximum narrow dictionary indices" begin
            for (T, signed) in ((Int8, true), (UInt8, false))
                n = Int(typemax(T)) + 1
                vf, vd = fromjulia("pool", collect(Int64(1):Int64(n)))
                t = DictionaryType(IntType(8, signed), vf.type, false)
                f = Field("d", t; nullable=false)
                d = AC.ArrayData(
                    t,
                    1,
                    [BufferSlice(), AC._databuffer(T[typemax(T)])];
                    dictionary=vd,
                    nullcount=0,
                )
                validate_structural(f, d)
                validate_semantic(f, d)
                @test materialize(f, d) == Int64[n]
            end
        end

        @testset "canonical empty offset arrays" begin
            st = Utf8Type(false)
            sf = Field("s", st)
            sd = AC.ArrayData(
                st,
                0,
                [BufferSlice(), BufferSlice(), BufferSlice()];
                nullcount=0,
            )
            @test validate_structural(sf, sd) === sd
            @test validate_semantic(sf, sd) === sd
            @test isempty(materialize(sf, sd))

            cf, cd = fromjulia("item", Int64[])
            lt = ListType(false)
            lf = Field("l", lt; children=[cf])
            ld = AC.ArrayData(
                lt,
                0,
                [BufferSlice(), BufferSlice()];
                children=[cd],
                nullcount=0,
            )
            @test validate_structural(lf, ld) === ld
            @test validate_semantic(lf, ld) === ld
            @test isempty(materialize(lf, ld))
        end

        @testset "dictionary pool nullability is independent" begin
            vf, vd = fromjulia("pool", Union{Missing,String}[missing, "x"])
            t = DictionaryType(IntType(32, true), vf.type, false)
            f = Field("d", t; nullable=false, children=vf.children)
            d = AC.ArrayData(
                t,
                2,
                [BufferSlice(), AC._databuffer(Int32[0, 1])];
                dictionary=vd,
                nullcount=0,
            )
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
            ed = AC.ArrayData(
                StructType(),
                3,
                [BufferSlice()];
                children=[kd, vd],
                nullcount=0,
            )
            offs = Int32[0, 2, 3]
            t = MapType(false)
            f = Field("m", t; children=[ef])
            d = AC.ArrayData(
                t,
                2,
                [BufferSlice(), AC._databuffer(offs)];
                children=[ed],
                nullcount=0,
            )
            validate_structural(f, d)
            validate_semantic(f, d)
            @test materialize(f, d) == [["a" => 1, "b" => 2], ["c" => 3]]
        end

        @testset "map applies the entries struct offset" begin
            kf, kd = fromjulia("key", ["skip", "a", "b"])
            vf, vd = fromjulia("value", Int64[0, 1, 2])
            ef = Field("entries", StructType(); nullable=false, children=[kf, vf])
            ed = AC.ArrayData(
                StructType(),
                2,
                [BufferSlice()];
                offset=1,
                children=[kd, vd],
                nullcount=0,
            )
            t = MapType(false)
            f = Field("m", t; children=[ef])
            d = AC.ArrayData(
                t,
                1,
                [BufferSlice(), AC._databuffer(Int32[0, 2])];
                children=[ed],
                nullcount=0,
            )
            validate_structural(f, d)
            validate_semantic(f, d)
            @test materialize(f, d) == [["a" => 1, "b" => 2]]
        end

        @testset "empty large-list range does not wrap" begin
            cf = Field("item", NullType())
            cd = AC.ArrayData(NullType(), typemax(Int64), BufferSlice[])
            t = ListType(true)
            f = Field("list", t; children=[cf])
            d = AC.ArrayData(
                t,
                1,
                [BufferSlice(), AC._databuffer(Int64[typemax(Int64), typemax(Int64)])];
                children=[cd],
                nullcount=0,
            )
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
            d = AC.ArrayData(
                t,
                3,
                [AC._databuffer(typeids), AC._databuffer(offsets)];
                children=[ad, bd],
            )
            validate_structural(f, d)
            validate_semantic(f, d)
            @test materialize(f, d) == [10, "x", 20]
        end

        @testset "sparse union" begin
            t = UnionType(AC.SparseMode, Int8[0, 1])
            af, ad = fromjulia("i", Int64[10, 20, 30])
            bf, bd = fromjulia("s", ["x", "y", "z"])
            f = Field("u", t; children=[af, bf])
            d = AC.ArrayData(t, 3, [AC._databuffer(Int8[0, 1, 0])]; children=[ad, bd])
            validate_structural(f, d)
            @test materialize(f, d) == [10, "y", 30]
        end

        @testset "interval MONTH_DAY_NANO" begin
            t = IntervalType(AC.MONTH_DAY_NANO)
            raw = vcat(reinterpret(UInt8, Int32[1, 2]), reinterpret(UInt8, Int64[3]))
            f = Field("iv", t; nullable=false)
            d = AC.ArrayData(
                t,
                1,
                [BufferSlice(), AC._databuffer(collect(raw))];
                nullcount=0,
            )
            validate_structural(f, d)
            @test getvalue(f, d, 1) == (months=1, days=2, nanos=3)
        end

        @testset "decimal32/64 read at the right width" begin
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
            d = AC.ArrayData(
                t,
                3,
                [BufferSlice(), AC._databuffer(v)];
                offset=2,
                nullcount=0,
            )
            validate_structural(f, d)
            @test materialize(f, d) == [3, 4, 5]
        end

        @testset "logical offset in struct and sparse union" begin
            af, ad = fromjulia("a", Int64[10, 20, 30])
            sf = Field("st", StructType(); children=[af])
            sd = AC.ArrayData(
                StructType(),
                2,
                [BufferSlice()];
                offset=1,
                children=[ad],
                nullcount=0,
            )
            validate_structural(sf, sd)
            @test materialize(sf, sd) == [["a" => 20], ["a" => 30]]

            uf = Field(
                "u",
                UnionType(AC.SparseMode, Int8[0, 1]);
                children=[af, fromjulia("b", ["x", "y", "z"])[1]],
            )
            bd = fromjulia("b", ["x", "y", "z"])[2]
            ud = AC.ArrayData(
                uf.type,
                2,
                [AC._databuffer(Int8[0, 1, 0])];
                offset=1,
                children=[ad, bd],
                nullcount=0,
            )
            validate_structural(uf, ud)
            validate_semantic(uf, ud)
            @test materialize(uf, ud) == ["y", 30]
        end

        @testset "struct access preserves names that NamedTuple cannot represent" begin
            af, ad = fromjulia("dup", Int64[1])
            bf, bd = fromjulia("dup", Int64[2])
            f = Field("s", StructType(); children=[af, bf])
            d = AC.ArrayData(
                StructType(),
                1,
                [BufferSlice()];
                children=[ad, bd],
                nullcount=0,
            )
            validate_structural(f, d)
            validate_semantic(f, d)
            @test getvalue(f, d, 1) == ["dup" => 1, "dup" => 2]

            unnamed = Field(
                "s",
                StructType();
                children=[
                    Field("", af.type; nullable=false),
                    Field("", bf.type; nullable=false),
                ],
            )
            validate_structural(unnamed, d)
            validate_semantic(unnamed, d)
            @test getvalue(unnamed, d, 1) == ["" => 1, "" => 2]

            nulname = "embedded\0nul"
            nulnamed =
                Field("s", StructType(); children=[Field(nulname, af.type; nullable=false)])
            nuld =
                AC.ArrayData(StructType(), 1, [BufferSlice()]; children=[ad], nullcount=0)
            validate_structural(nulnamed, nuld)
            validate_semantic(nulnamed, nuld)
            @test getvalue(nulnamed, nuld, 1) == [nulname => 1]
        end

        @testset "run-end encoding: validation, access, logical nulls, slicing" begin
            t = RunEndEncodedType()
            ref, red = fromjulia("run_ends", Int32[2, 3])
            vf, vd = fromjulia("values", Int64[7, 9])
            f = Field("ree", t; children=[ref, vf])
            d = AC.ArrayData(t, 3, BufferSlice[]; children=[red, vd], nullcount=0)
            validate_structural(f, d)
            @test validate_semantic(f, d) === d
            @test [getvalue(f, d, i) for i = 1:3] == [7, 7, 9]
            @test materialize(f, d) == [7, 7, 9]
            @test nullcount(d) == 0

            # nulls are runs whose VALUE is null; the parent has no bitmap
            nvf, nvd = fromjulia("values", Union{Missing,Int64}[missing, 4])
            nf = Field("ree", t; children=[ref, nvf])
            nd = AC.ArrayData(t, 3, BufferSlice[]; children=[red, nvd], nullcount=0)
            @test validate_semantic(nf, nd) === nd
            @test isequal(materialize(nf, nd), [missing, missing, 4])

            # slicing shifts logical positions through the run search
            sliced =
                AC.ArrayData(t, 2, BufferSlice[]; offset=1, children=[red, vd], nullcount=0)
            @test validate_semantic(f, sliced) === sliced
            @test materialize(f, sliced) == [7, 9]
            boundary =
                AC.ArrayData(t, 1, BufferSlice[]; offset=2, children=[red, vd], nullcount=0)
            @test validate_semantic(f, boundary) === boundary
            @test materialize(f, boundary) == [9]

            # adversarial: non-ascending, zero/negative, short coverage,
            # unequal children, declared parent nulls
            badruns(v) = AC.ArrayData(
                t,
                3,
                BufferSlice[];
                children=[fromjulia("run_ends", v)[2], vd],
                nullcount=0,
            )
            @test_throws ValidationError validate_semantic(f, badruns(Int32[3, 2]))
            @test_throws ValidationError validate_semantic(f, badruns(Int32[0, 3]))
            @test_throws ValidationError validate_semantic(f, badruns(Int32[2, 2]))
            @test_throws ValidationError validate_semantic(f, badruns(Int32[1, 2]))
            shortchild = AC.ArrayData(
                t,
                3,
                BufferSlice[];
                children=[red, fromjulia("values", Int64[7])[2]],
                nullcount=0,
            )
            @test_throws ValidationError validate_semantic(f, shortchild)
            declared = AC.ArrayData(t, 3, BufferSlice[]; children=[red, nvd], nullcount=2)
            @test_throws ValidationError validate_semantic(nf, declared)
            # An UNKNOWN parent null count (-1, spec-legal for every layout, and
            # what a C producer may hand us) is not a declared positive count:
            # it validates and resolves to the bitmap-less physical zero.
            unknown = AC.ArrayData(t, 3, BufferSlice[]; children=[red, nvd])
            @test validate_semantic(nf, unknown) === unknown
            @test nullcount(unknown) == 0
            @test isequal(materialize(nf, unknown), [missing, missing, 4])
        end

        @testset "view layouts: entries, prefixes, variadic buffers" begin
            # helper: build one 16-byte view entry (mirrors the batteries'
            # `_viewentry`/`_viewlong`; this suite does not load those helpers)
            entry(len::Int, rest::Vector{UInt8}) = vcat(
                reinterpret(UInt8, Int32[Int32(len)]),
                rest,
                zeros(UInt8, 12 - length(rest)),
            )
            long(len, prefix, bufidx, off) = vcat(
                reinterpret(UInt8, Int32[Int32(len)]),
                prefix,
                reinterpret(UInt8, Int32[Int32(bufidx), Int32(off)]),
            )
            vt = ViewType(true)
            vf = Field("v", vt; nullable=true)
            payload = collect(codeunits("hello-world-beyond-inline"))
            views = vcat(
                entry(5, collect(codeunits("hello"))),                # inline short
                long(25, payload[1:4], 0, 0),                          # out-of-line
                entry(12, collect(codeunits("exactly-12bb"))),
            )         # inline max
            vd = AC.ArrayData(
                vt,
                3,
                [BufferSlice(), AC._databuffer(views), AC._databuffer(payload)];
                nullcount=0,
            )
            @test validate_semantic(vf, vd) === vd
            @test AC.validate_full(vf, vd) === vd
            @test materialize(vf, vd) ==
                  ["hello", "hello-world-beyond-inline", "exactly-12bb"]

            # Length 12 is inline; length 13 is out-of-line. The parent offset
            # selects the second physical 16-byte view entry.
            payload13 = collect(codeunits("exactly-13-by"))
            boundaryviews = vcat(
                entry(12, collect(codeunits("exactly-12bb"))),
                long(13, payload13[1:4], 0, 0),
            )
            slicedview = AC.ArrayData(
                vt,
                1,
                [BufferSlice(), AC._databuffer(boundaryviews), AC._databuffer(payload13)];
                offset=1,
                nullcount=0,
            )
            @test validate_full(vf, slicedview) === slicedview
            @test materialize(vf, slicedview) == ["exactly-13-by"]

            # binary views return bytes
            bt = ViewType(false)
            bf = Field("b", bt)
            bd = AC.ArrayData(
                bt,
                1,
                [BufferSlice(), AC._databuffer(entry(2, UInt8[0xff, 0x00]))];
                nullcount=0,
            )
            @test validate_semantic(bf, bd) === bd
            @test getvalue(bf, bd, 1) == UInt8[0xff, 0x00]

            # null slots' entry bytes are unrestricted by the spec
            nulld = AC.ArrayData(
                vt,
                1,
                [
                    AC._databuffer(UInt8[0x00]),
                    AC._databuffer(long(99, UInt8[1, 2, 3, 4], 7, -5)),
                ];
                nullcount=1,
            )
            @test validate_semantic(vf, nulld) === nulld
            @test getvalue(vf, nulld, 1) === missing

            # adversarial: bad prefix, escaping range, bad buffer index,
            # negative length/offset
            badprefix = AC.ArrayData(
                vt,
                1,
                [
                    BufferSlice(),
                    AC._databuffer(long(25, UInt8[1, 2, 3, 4], 0, 0)),
                    AC._databuffer(payload),
                ];
                nullcount=0,
            )
            @test_throws ValidationError validate_semantic(vf, badprefix)
            escaping = AC.ArrayData(
                vt,
                1,
                [
                    BufferSlice(),
                    AC._databuffer(long(26, payload[1:4], 0, 4)),
                    AC._databuffer(payload),
                ];
                nullcount=0,
            )
            @test_throws ValidationError validate_semantic(vf, escaping)
            badbuf = AC.ArrayData(
                vt,
                1,
                [
                    BufferSlice(),
                    AC._databuffer(long(25, payload[1:4], 3, 0)),
                    AC._databuffer(payload),
                ];
                nullcount=0,
            )
            @test_throws ValidationError validate_semantic(vf, badbuf)
            neglen = AC.ArrayData(
                vt,
                1,
                [BufferSlice(), AC._databuffer(entry(-1, UInt8[]))];
                nullcount=0,
            )
            @test_throws ValidationError validate_semantic(vf, neglen)

            # ListView invariants bind NULL slots too (spec rule)
            cf, cd = fromjulia("item", Int64[1, 2, 3])
            lvt = ListViewType(false)
            lvf = Field("lv", lvt; nullable=true, children=[cf])
            nullbad = AC.ArrayData(
                lvt,
                1,
                [
                    AC._databuffer(UInt8[0x00]),
                    AC._databuffer(Int32[9]),
                    AC._databuffer(Int32[9]),
                ];
                children=[cd],
                nullcount=1,
            )
            @test_throws ValidationError validate_semantic(lvf, nullbad)
            # overlapping, shared child ranges are legal
            overlap = AC.ArrayData(
                lvt,
                2,
                [BufferSlice(), AC._databuffer(Int32[0, 0]), AC._databuffer(Int32[3, 2])];
                children=[cd],
                nullcount=0,
            )
            @test validate_semantic(lvf, overlap) === overlap
            @test materialize(lvf, overlap) == [[1, 2, 3], [1, 2]]
            slicedlistview = AC.ArrayData(
                lvt,
                1,
                [BufferSlice(), AC._databuffer(Int32[2, 0]), AC._databuffer(Int32[1, 2])];
                offset=1,
                children=[cd],
                nullcount=0,
            )
            @test validate_semantic(lvf, slicedlistview) === slicedlistview
            @test materialize(lvf, slicedlistview) == [[1, 2]]
            # large list-view uses 64-bit offsets and sizes
            llvt = ListViewType(true)
            llvf = Field("llv", llvt; children=[cf])
            llvd = AC.ArrayData(
                llvt,
                1,
                [BufferSlice(), AC._databuffer(Int64[1]), AC._databuffer(Int64[2])];
                children=[cd],
                nullcount=0,
            )
            @test validate_semantic(llvf, llvd) === llvd
            @test getvalue(llvf, llvd, 1) == [2, 3]
        end

        @testset "fromviewentries: ArrowString payloads → Utf8View, zero-copy" begin
            # A local encoder of the ArrowStrings payload (Core cannot depend on
            # the package), which IS an Arrow view entry: length | first 4 bytes,
            # then bytes 5..12 (≤12) or (Int32 buffer index, Int32 0-based
            # offset). Any 16-byte isbits type is accepted.
            struct ViewEntry
                a::UInt64
                b::UInt64
            end
            function inlineentry(bytes::Vector{UInt8})
                len = length(bytes)
                a = UInt64(len % UInt32)
                b = zero(UInt64)
                for i = 1:min(len, 4)
                    a |= UInt64(bytes[i]) << (32 + 8 * (i - 1))
                end
                for i = 5:len
                    b |= UInt64(bytes[i]) << (8 * (i - 5))
                end
                return ViewEntry(a, b)
            end
            function viewentry(data::Vector{UInt8}, pos1::Int, len::Int, bufidx::Int)
                a = UInt64(len % UInt32)
                for i = 1:4
                    a |= UInt64(data[pos1 + i - 1]) << (32 + 8 * (i - 1))
                end
                return ViewEntry(
                    a,
                    UInt64(bufidx % UInt32) | (UInt64((pos1 - 1) % UInt32) << 32),
                )
            end
            nullentry() = ViewEntry(UInt64(0xffffffff), zero(UInt64))

            # buf: a "CSV input" with fields at known positions; extra: one
            # unescaped-at-parse-time long value.
            buf = collect(
                codeunits(
                    "id,name\n1,\"\"\n2,abcd\n3,twelve-bytes\n4,thirteen-byte\n5,a much longer value here\n",
                ),
            )
            long1 = findfirst(codeunits("thirteen-byte"), buf)
            long2 = findfirst(codeunits("a much longer value here"), buf)
            extra = collect(codeunits("she said \"hi\" and left"))
            payloads = ViewEntry[
                inlineentry(UInt8[]),                                    # ""  (len 0)
                inlineentry(collect(codeunits("abcd"))),                 # len 4 (a only)
                inlineentry(collect(codeunits("twelve-bytes"))),         # len 12 (inline max)
                viewentry(buf, first(long1), 13, 0),                     # first long: buf
                nullentry(),                                             # missing
                viewentry(buf, first(long2), 24, 0),                     # long: buf
                viewentry(extra, 1, length(extra), 1),                   # long: extra
            ]
            f, d = fromviewentries("s", payloads, buf, extra)
            @test f.type == ViewType(true)
            @test f.nullable
            @test length(d) == 7
            @test nullcount(d) == 1
            @test validate_full(f, d) === d          # geometry, prefixes, UTF-8
            @test isequal(
                materialize(f, d),
                [
                    "",
                    "abcd",
                    "twelve-bytes",
                    "thirteen-byte",
                    missing,
                    "a much longer value here",
                    "she said \"hi\" and left",
                ],
            )
            # ZERO-COPY: the views buffer IS the payload vector, and the data
            # buffers are the caller's vectors — none of the three is copied
            @test d.buffers[2].region.root === payloads
            @test d.buffers[3].region.root === buf
            @test d.buffers[4].region.root === extra
            # the null slot's entry bytes are left as they are (spec: unspecified)
            @test AC.loadat(d.buffers[2], Int32, Int64(16 * 4)) == Int32(-1)

            # the column crosses both adapters as an ordinary Utf8View
            sch = Schema(Field[f])
            b = AC.RecordBatch(sch, ArrayData[d], 7)
            s = Arrow.readstream(Arrow.writestream(sch, AC.RecordBatch[b]))
            @test isequal(
                materialize(s.schema.fields[1], s.batches[1].columns[1]),
                materialize(f, d),
            )
            sp, ap = Arrow.to_c_data(f, d)
            f2, d2 = Arrow.from_c_data(sp, ap)
            @test isequal(materialize(f2, d2), materialize(f, d))
            Arrow.release!(d2.owner::Arrow.ForeignOwner)
            Arrow.reap!()

            # no nulls, empty extra: the empty bitmap and an empty second data
            # buffer (buffer index 1 always means `extra`)
            f0, d0 = fromviewentries("t", payloads[[2, 3]], buf, UInt8[]; nullable=false)
            @test !f0.nullable
            @test length(d0.buffers) == 4
            @test AC.isempty_buffer(d0.buffers[1])
            @test validate_full(f0, d0) === d0
            @test materialize(f0, d0) == ["abcd", "twelve-bytes"]
            # ... and it too crosses the C boundary (an empty variadic buffer)
            sp0, ap0 = Arrow.to_c_data(f0, d0)
            f0b, d0b = Arrow.from_c_data(sp0, ap0)
            @test materialize(f0b, d0b) == ["abcd", "twelve-bytes"]
            Arrow.release!(d0b.owner::Arrow.ForeignOwner)
            Arrow.reap!()

            # malformed long entries construct (a wrap makes no promises about
            # content) and refuse where every builder's output does: validation
            for bad in (
                viewentry(extra, 1, length(extra), 1) => UInt8[],  # extra referenced but empty
                ViewEntry(payloads[6].a, UInt64(0) | (UInt64(length(buf) - 3) << 32)) =>
                    extra,   # escapes buf
                ViewEntry(payloads[6].a, UInt64(7)) => extra,
            )            # buffer index 7
                fb, db = fromviewentries("t", [bad.first], buf, bad.second)
                @test_throws ValidationError validate_semantic(fb, db)
            end
            # wrong payload width and non-isbits payloads are constructor errors
            @test_throws ArgumentError fromviewentries("t", UInt64[1, 2], buf, extra)
            @test_throws ArgumentError fromviewentries("t", Any[1], buf, extra)
        end
    end

    @testset "staged validation rejects corrupt metadata" begin
        @testset "structural: descriptor values and field shape must match" begin
            f = Field("x", IntType(32, true))
            d = AC.ArrayData(
                IntType(64, true),
                1,
                [BufferSlice(), AC._databuffer(Int64[1])];
                nullcount=0,
            )
            @test_throws ValidationError validate_structural(f, d)
            @test_throws ValidationError validate_structural(
                Field("l", ListType(false)),
                AC.ArrayData(
                    ListType(false),
                    1,
                    [BufferSlice(), AC._databuffer(Int32[0, 0])];
                    children=[fromjulia("item", Int64[])[2]],
                    nullcount=0,
                ),
            )
            badt = IntType(24, true)
            @test_throws ValidationError validate_structural(
                Field("bad", badt),
                AC.ArrayData(
                    badt,
                    1,
                    [BufferSlice(), AC._databuffer(UInt8[0, 0, 0])];
                    nullcount=0,
                ),
            )

            invalidname = String(UInt8[0xff])
            namef, named = fromjulia(invalidname, Int64[1])
            @test_throws ValidationError validate_structural(namef, named)

            badutf8 = String(UInt8[0xff])
            for metadata in (Dict(badutf8 => "v"), Dict("k" => badutf8))
                badfield = Field("metadata", IntType(8, true); metadata=metadata)
                baddata = AC.ArrayData(
                    badfield.type,
                    0,
                    [BufferSlice(), BufferSlice()];
                    nullcount=0,
                )
                @test_throws ValidationError validate_structural(badfield, baddata)
            end
            badtimezone = TimestampType(AC.SECOND, badutf8)
            @test_throws ValidationError validate_structural(
                Field("timestamp", badtimezone),
                AC.ArrayData(badtimezone, 0, [BufferSlice(), BufferSlice()]; nullcount=0),
            )

            badtimeunit = reinterpret(AC.TimeUnit, UInt8(0xff))
            baddateunit = reinterpret(AC.DateUnit, UInt8(0xff))
            badintervalunit = reinterpret(AC.IntervalUnit, UInt8(0xff))
            badunionmode = reinterpret(AC.UnionMode, UInt8(0xff))
            for t in (
                DateType(baddateunit),
                TimeType(badtimeunit, 64),
                TimestampType(badtimeunit, nothing),
                DurationType(badtimeunit),
                IntervalType(badintervalunit),
            )
                spec = layoutspec(t)
                d = AC.ArrayData(t, 0, [BufferSlice() for _ in spec.buffers]; nullcount=0)
                @test_throws ValidationError validate_structural(Field("bad", t), d)
            end

            cf, cd = fromjulia("item", Int64[])
            badunion = UnionType(badunionmode, Int8[0])
            baduniondata = AC.ArrayData(
                badunion,
                0,
                [BufferSlice(), BufferSlice()];
                children=[cd],
                nullcount=0,
            )
            @test_throws ValidationError validate_structural(
                Field("bad-union", badunion; children=[cf]),
                baduniondata,
            )

            if Sys.WORD_SIZE > 32
                for scale in (Int(typemin(Int32)) - 1, Int(typemax(Int32)) + 1)
                    badscale = DecimalType(1, scale, 32)
                    @test_throws ValidationError validate_structural(
                        Field("decimal", badscale),
                        AC.ArrayData(
                            badscale,
                            0,
                            [BufferSlice(), BufferSlice()];
                            nullcount=0,
                        ),
                    )
                end

                badwidth = FixedSizeBinaryType(Int(typemax(Int32)) + 1)
                @test_throws ValidationError validate_structural(
                    Field("fixed", badwidth),
                    AC.ArrayData(badwidth, 0, [BufferSlice(), BufferSlice()]; nullcount=0),
                )

                cf, cd = fromjulia("item", Int64[])
                badsize = FixedSizeListType(Int(typemax(Int32)) + 1)
                @test_throws ValidationError validate_structural(
                    Field("list", badsize; children=[cf]),
                    AC.ArrayData(badsize, 0, [BufferSlice()]; children=[cd], nullcount=0),
                )
            end
        end

        @testset "structural: wrong buffer arity" begin
            t = IntType(64, true)
            f = Field("x", t)
            d = AC.ArrayData(t, 3, [BufferSlice()])   # missing DATA buffer
            @test_throws ValidationError validate_structural(f, d)
            @test_throws ValidationError validate_semantic(f, d)
            @test_throws ValidationError validate_full(f, d)
            @test !(@atomic d.semachecked)

            wrongf = Field("x", IntType(32, true))
            good64 =
                AC.ArrayData(t, 1, [BufferSlice(), AC._databuffer(Int64[1])]; nullcount=0)
            @test_throws ValidationError validate_semantic(wrongf, good64)
            @test_throws ValidationError validate_full(wrongf, good64)
            @test !(@atomic good64.semachecked)
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

        @testset "semantic: Date64 and Time values obey their domains" begin
            # Temporal value domains (Date64 whole days, Time-of-day range) are
            # ADVISORY: the semantic stage accepts anything the layout admits and
            # the opt-in validate_full tier enforces the domain.
            function checkvalues(t, values; valid=true)
                f = Field("temporal", t; nullable=false)
                d = AC.ArrayData(
                    t,
                    length(values),
                    [BufferSlice(), AC._databuffer(values)];
                    nullcount=0,
                )
                validate_structural(f, d)
                @test validate_semantic(f, d) === d
                if valid
                    @test AC.validate_full(f, d) === d
                else
                    @test_throws ValidationError AC.validate_full(f, d)
                end
            end

            checkvalues(DateType(AC.MILLISECOND_DATE), Int64[-86_400_000, 0, 86_400_000])
            # Date64 whole-day divisibility is ADVISORY (the arrow-testing gold
            # corpus itself violates it): semantic accepts, validate_full rejects.
            let t = DateType(AC.MILLISECOND_DATE),
                f = Field("temporal", t; nullable=false),
                d = AC.ArrayData(
                    t,
                    1,
                    [BufferSlice(), AC._databuffer(Int64[1])];
                    nullcount=0,
                )

                @test validate_semantic(f, d) === d
                @test_throws ValidationError AC.validate_full(f, d)
            end
            checkvalues(TimeType(AC.SECOND, 32), Int32[0, 86_399])
            checkvalues(TimeType(AC.SECOND, 32), Int32[-1]; valid=false)
            checkvalues(TimeType(AC.SECOND, 32), Int32[86_400]; valid=false)
            checkvalues(TimeType(AC.MILLISECOND, 32), Int32[86_399_999])
            checkvalues(TimeType(AC.MICROSECOND, 64), Int64[86_399_999_999])
            checkvalues(TimeType(AC.NANOSECOND, 64), Int64[86_399_999_999_999])
            checkvalues(TimeType(AC.NANOSECOND, 64), Int64[86_400_000_000_000]; valid=false)
        end

        @testset "semantic: decimal values fit declared precision" begin
            # Decimal precision is ADVISORY (the arrow-testing gold corpus carries
            # decimal(3,2) values with five digits): validate_semantic accepts any
            # coefficient; validate_full enforces the declared digit count.
            function checkdecimal(t, bytes; valid=true, bitmap=BufferSlice(), nullcount=0)
                f = Field("decimal", t)
                d = AC.ArrayData(
                    t,
                    length(bytes) ÷ (t.bits ÷ 8),
                    [bitmap, AC._databuffer(bytes)];
                    nullcount=nullcount,
                )
                @test validate_semantic(f, d) === d
                if valid
                    @test AC.validate_full(f, d) === d
                else
                    @test_throws ValidationError AC.validate_full(f, d)
                end
            end

            for (bits, T) in ((32, Int32), (64, Int64), (128, Int128))
                t = DecimalType(1, 0, bits)
                checkdecimal(t, collect(reinterpret(UInt8, T[9, -9])))
                checkdecimal(t, collect(reinterpret(UInt8, T[10])); valid=false)
                checkdecimal(t, collect(reinterpret(UInt8, T[-10])); valid=false)
            end

            limit128 = Int128(10)^38
            t128max = DecimalType(38, 0, 128)
            checkdecimal(
                t128max,
                collect(reinterpret(UInt8, Int128[limit128 - 1, -limit128 + 1])),
            )
            checkdecimal(
                t128max,
                collect(reinterpret(UInt8, Int128[limit128]));
                valid=false,
            )
            checkdecimal(
                t128max,
                collect(reinterpret(UInt8, Int128[-limit128]));
                valid=false,
            )

            # Decimal256 values are represented here as four little-endian UInt64
            # limbs. Cover positive/negative precision edges without a BigInt
            # dependency in either Core or its tests.
            t256 = DecimalType(1, 0, 256)
            pos9 = UInt64[9, 0, 0, 0]
            pos10 = UInt64[10, 0, 0, 0]
            neg9 = UInt64[
                typemax(UInt64) - 8,
                typemax(UInt64),
                typemax(UInt64),
                typemax(UInt64),
            ]
            checkdecimal(t256, collect(reinterpret(UInt8, vcat(pos9, neg9))))
            checkdecimal(t256, collect(reinterpret(UInt8, pos10)); valid=false)

            # 10^76 spans all four limbs. These exact boundaries exercise carry
            # propagation when the precision limit is built.
            limit76 = UInt64[
                0x0000000000000000,
                0x7775a5f171951000,
                0x0764b4abe8652979,
                0x161bcca7119915b5,
            ]
            below76 = UInt64[
                0xffffffffffffffff,
                0x7775a5f171950fff,
                0x0764b4abe8652979,
                0x161bcca7119915b5,
            ]
            t256max = DecimalType(76, 0, 256)
            checkdecimal(t256max, collect(reinterpret(UInt8, below76)))
            checkdecimal(t256max, collect(reinterpret(UInt8, limit76)); valid=false)

            # Invalid bytes in a null slot are masked and do not violate the
            # precision contract.
            checkdecimal(
                DecimalType(1, 0, 32),
                collect(reinterpret(UInt8, Int32[10]));
                bitmap=AC._databuffer(UInt8[0x00]),
                nullcount=1,
            )
        end

        @testset "semantic: dictionary index out of bounds" begin
            f, d = AC.fromjulia_dict("d", ["a", "b"], [0, 1])
            # corrupt: poke an index past the pool through a rebuilt ArrayData
            bad = AC.ArrayData(
                d.type,
                d.len,
                [d.buffers[1], AC._databuffer(Int32[0, 7])];
                dictionary=d.dictionary,
            )
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
                AC.ArrayData(
                    dupt,
                    2,
                    [AC._databuffer(Int8[0, 0])];
                    children=[ad, bd],
                    nullcount=0,
                ),
            )

            negt = UnionType(AC.SparseMode, Int8[-1, 0])
            @test_throws ValidationError validate_structural(
                Field("u", negt; children=[af, bf]),
                AC.ArrayData(
                    negt,
                    2,
                    [AC._databuffer(Int8[-1, 0])];
                    children=[ad, bd],
                    nullcount=0,
                ),
            )

            shortt = UnionType(AC.SparseMode, Int8[0])
            @test_throws ValidationError validate_structural(
                Field("u", shortt; children=[af, bf]),
                AC.ArrayData(
                    shortt,
                    2,
                    [AC._databuffer(Int8[0, 0])];
                    children=[ad, bd],
                    nullcount=0,
                ),
            )

            t = UnionType(AC.DenseMode, Int8[0])
            f = Field("u", t; children=[af])
            d = AC.ArrayData(
                t,
                2,
                [AC._databuffer(Int8[0, 0]), AC._databuffer(Int32[1, 0])];
                children=[ad],
                nullcount=0,
            )
            validate_structural(f, d)
            @test_throws ValidationError validate_semantic(f, d)

            # Equal dense offsets are valid; only decreases are forbidden.
            repeated = AC.ArrayData(
                t,
                2,
                [AC._databuffer(Int8[0, 0]), AC._databuffer(Int32[0, 0])];
                children=[ad],
                nullcount=0,
            )
            validate_structural(f, repeated)
            @test validate_semantic(f, repeated) === repeated
        end

        @testset "structural: nested REE values are supported" begin
            irf, ird = fromjulia("run_ends", Int32[1, 2])
            ivf, ivd = fromjulia("values", Int64[10, 20])
            innerf = Field("values", RunEndEncodedType(); children=[irf, ivf])
            innerd = AC.ArrayData(
                RunEndEncodedType(),
                2,
                BufferSlice[];
                children=[ird, ivd],
                nullcount=0,
            )
            orf, ord = fromjulia("run_ends", Int32[2, 4])
            outerf = Field("ree", RunEndEncodedType(); children=[orf, innerf])
            outerd = AC.ArrayData(
                RunEndEncodedType(),
                4,
                BufferSlice[];
                children=[ord, innerd],
                nullcount=0,
            )
            @test validate_full(outerf, outerd) === outerd
            @test getvalue(outerf, outerd, 3) == 20
            @test materialize(outerf, outerd) == [10, 10, 20, 20]
        end

        @testset "structural: REE geometry must be representable" begin
            erf, erd = fromjulia("run_ends", Int16[])
            evf, evd = fromjulia("values", Int64[])
            t = RunEndEncodedType()
            f = Field("ree", t; children=[erf, evf])
            emptyslice = AC.ArrayData(
                t,
                0,
                BufferSlice[];
                offset=5,
                children=[erd, evd],
                nullcount=0,
            )
            @test validate_full(f, emptyslice) === emptyslice
            emptyphysical =
                AC.ArrayData(t, 1, BufferSlice[]; children=[erd, evd], nullcount=0)
            @test_throws ValidationError validate_structural(f, emptyphysical)

            rf, rd = fromjulia("run_ends", Int16[typemax(Int16)])
            vf, vd = fromjulia("values", Int64[1])
            f = Field("ree", t; children=[rf, vf])
            overflow = AC.ArrayData(
                t,
                Int64(typemax(Int16)) + 1,
                BufferSlice[];
                children=[rd, vd],
                nullcount=0,
            )
            @test_throws ValidationError validate_structural(f, overflow)

            badnulls = AC.ArrayData(t, 1, BufferSlice[]; children=[rd, vd], nullcount=1)
            @test_throws ValidationError validate_structural(f, badnulls)
        end

        @testset "semantic: declared null count matches bitmap" begin
            f, d = fromjulia("x", [1, missing])
            bad = AC.ArrayData(d.type, d.len, d.buffers; nullcount=0)
            validate_structural(f, bad)
            @test_throws ValidationError validate_semantic(f, bad)
            absent = AC.ArrayData(d.type, d.len, [BufferSlice(), d.buffers[2]]; nullcount=1)
            @test_throws ValidationError validate_structural(f, absent)
            @test_throws ArgumentError AC.ArrayData(d.type, d.len, d.buffers; nullcount=3)
        end

        @testset "field nullability is checked outside the data cache" begin
            nullable, d = fromjulia("x", [1, missing])
            validate_semantic(nullable, d)
            @test (@atomic d.semachecked)
            # `nullable` is ADVISORY (the arrow-testing gold corpus has
            # non-nullable fields holding nulls, and C++ reads them): the semantic
            # stage accepts and the cached certificate is not poisoned by the
            # Field; the opt-in validate_full tier enforces the declaration.
            nonnullable = Field("x", d.type; nullable=false)
            @test validate_semantic(nonnullable, d) === d
            @test_throws ValidationError AC.validate_full(nonnullable, d)

            af, ad = fromjulia("a", Union{Missing,Int64}[missing])
            uf = Field("u", UnionType(AC.DenseMode, Int8[0]); nullable=false, children=[af])
            ud = AC.ArrayData(
                uf.type,
                1,
                [AC._databuffer(Int8[0]), AC._databuffer(Int32[0])];
                children=[ad],
                nullcount=0,
            )
            validate_structural(uf, ud)
            @test validate_semantic(uf, ud) === ud
            @test_throws ValidationError AC.validate_full(uf, ud)
        end

        # These pin the MASKING rules of the nullability walk (a null parent hides
        # non-nullable child slots), which runs in the validate_full tier.
        @testset "parent nulls mask hidden non-nullable child slots" begin
            cf = Field("x", IntType(64, true); nullable=false)
            cd = AC.ArrayData(
                cf.type,
                2,
                [AC._databuffer(UInt8[0x00]), AC._databuffer(Int64[0, 0])];
                nullcount=2,
            )

            sf = Field("s", StructType(); children=[cf])
            masked_struct = AC.ArrayData(
                StructType(),
                1,
                [AC._databuffer(UInt8[0x00])];
                children=[cd],
                nullcount=1,
            )
            @test AC.validate_full(sf, masked_struct) === masked_struct
            visible_struct =
                AC.ArrayData(StructType(), 1, [BufferSlice()]; children=[cd], nullcount=0)
            @test_throws ValidationError AC.validate_full(sf, visible_struct)

            flt = FixedSizeListType(2)
            flf = Field("fixed", flt; children=[cf])
            masked_fixed = AC.ArrayData(
                flt,
                1,
                [AC._databuffer(UInt8[0x00])];
                children=[cd],
                nullcount=1,
            )
            @test AC.validate_full(flf, masked_fixed) === masked_fixed
            visible_fixed =
                AC.ArrayData(flt, 1, [BufferSlice()]; children=[cd], nullcount=0)
            @test_throws ValidationError AC.validate_full(flf, visible_fixed)

            lt = ListType(false)
            lf = Field("list", lt; children=[cf])
            offsets = AC._databuffer(Int32[0, 1])
            masked_list = AC.ArrayData(
                lt,
                1,
                [AC._databuffer(UInt8[0x00]), offsets];
                children=[cd],
                nullcount=1,
            )
            @test AC.validate_full(lf, masked_list) === masked_list
            visible_list =
                AC.ArrayData(lt, 1, [BufferSlice(), offsets]; children=[cd], nullcount=0)
            @test_throws ValidationError AC.validate_full(lf, visible_list)

            lvt = ListViewType(false)
            lvf = Field("listview", lvt; children=[cf])
            lvoffsets = AC._databuffer(Int32[0])
            lvsizes = AC._databuffer(Int32[1])
            masked_listview = AC.ArrayData(
                lvt,
                1,
                [AC._databuffer(UInt8[0x00]), lvoffsets, lvsizes];
                children=[cd],
                nullcount=1,
            )
            @test AC.validate_full(lvf, masked_listview) === masked_listview
            visible_listview = AC.ArrayData(
                lvt,
                1,
                [BufferSlice(), lvoffsets, lvsizes];
                children=[cd],
                nullcount=0,
            )
            @test_throws ValidationError AC.validate_full(lvf, visible_listview)
            empty_at_end = AC.ArrayData(
                lvt,
                1,
                [BufferSlice(), AC._databuffer(Int32[2]), AC._databuffer(Int32[0])];
                children=[cd],
                nullcount=0,
            )
            @test AC.validate_full(lvf, empty_at_end) === empty_at_end

            keyfield = Field("key", IntType(64, true); nullable=false)
            keydata = AC.ArrayData(
                keyfield.type,
                1,
                [AC._databuffer(UInt8[0x00]), AC._databuffer(Int64[0])];
                nullcount=1,
            )
            valuefield, valuedata = fromjulia("value", Int64[1])
            entriesfield = Field(
                "entries",
                StructType();
                nullable=false,
                children=[keyfield, valuefield],
            )
            entriesdata = AC.ArrayData(
                StructType(),
                1,
                [BufferSlice()];
                children=[keydata, valuedata],
                nullcount=0,
            )
            mt = MapType(false)
            mf = Field("map", mt; children=[entriesfield])
            masked_map = AC.ArrayData(
                mt,
                1,
                [AC._databuffer(UInt8[0x00]), offsets];
                children=[entriesdata],
                nullcount=1,
            )
            @test AC.validate_full(mf, masked_map) === masked_map
            visible_map = AC.ArrayData(
                mt,
                1,
                [BufferSlice(), offsets];
                children=[entriesdata],
                nullcount=0,
            )
            @test_throws ValidationError AC.validate_full(mf, visible_map)
        end

        # Nullability lives in the validate_full tier; these pin that only the
        # SELECTED union child slot is inspected.
        @testset "union contracts inspect only selected child slots" begin
            af, ad = fromjulia("a", Int64[1])
            bf = Field("b", IntType(64, true); nullable=false)
            bd = AC.ArrayData(
                bf.type,
                1,
                [AC._databuffer(UInt8[0x00]), AC._databuffer(Int64[0])];
                nullcount=1,
            )
            t = UnionType(AC.DenseMode, Int8[0, 1])
            f = Field("u", t; children=[af, bf])
            selected_valid = AC.ArrayData(
                t,
                1,
                [AC._databuffer(Int8[0]), AC._databuffer(Int32[0])];
                children=[ad, bd],
                nullcount=0,
            )
            @test AC.validate_full(f, selected_valid) === selected_valid

            selected_null = AC.ArrayData(
                t,
                1,
                [AC._databuffer(Int8[1]), AC._databuffer(Int32[0])];
                children=[ad, bd],
                nullcount=0,
            )
            @test_throws ValidationError AC.validate_full(f, selected_null)

            # Sparse selection applies the parent offset, but still ignores every
            # unselected child's storage at that logical position.
            saf, sad = fromjulia("a", Int64[1, 2])
            sbf = Field("b", IntType(64, true); nullable=false)
            sbd = AC.ArrayData(
                sbf.type,
                2,
                [AC._databuffer(UInt8[0x00]), AC._databuffer(Int64[0, 0])];
                nullcount=2,
            )
            st = UnionType(AC.SparseMode, Int8[0, 1])
            sf = Field("u", st; children=[saf, sbf])
            sparse_valid = AC.ArrayData(
                st,
                1,
                [AC._databuffer(Int8[1, 0])];
                offset=1,
                children=[sad, sbd],
                nullcount=0,
            )
            @test AC.validate_full(sf, sparse_valid) === sparse_valid
            sparse_null = AC.ArrayData(
                st,
                1,
                [AC._databuffer(Int8[0, 1])];
                offset=1,
                children=[sad, sbd],
                nullcount=0,
            )
            @test_throws ValidationError AC.validate_full(sf, sparse_null)
        end

        @testset "nested dictionary pools retain field contracts" begin
            valuefield = Field("x", IntType(64, true); nullable=false)
            nullvalue = AC.ArrayData(
                valuefield.type,
                1,
                [AC._databuffer(UInt8[0x00]), AC._databuffer(Int64[0])];
                nullcount=1,
            )
            valuetype = StructType()
            pool = AC.ArrayData(
                valuetype,
                1,
                [BufferSlice()];
                children=[nullvalue],
                nullcount=0,
            )
            dicttype = DictionaryType(IntType(32, true), valuetype, false)
            dictfield = Field("dict", dicttype; children=[valuefield])
            dictdata = AC.ArrayData(
                dicttype,
                1,
                [BufferSlice(), AC._databuffer(Int32[0])];
                dictionary=pool,
                nullcount=0,
            )
            outerfield = Field("outer", StructType(); children=[dictfield])
            outerdata = AC.ArrayData(
                StructType(),
                1,
                [BufferSlice()];
                children=[dictdata],
                nullcount=0,
            )
            @test_throws ValidationError AC.validate_full(outerfield, outerdata)

            maskedpool = AC.ArrayData(
                valuetype,
                1,
                [AC._databuffer(UInt8[0x00])];
                children=[nullvalue],
                nullcount=1,
            )
            maskeddict = AC.ArrayData(
                dicttype,
                1,
                [BufferSlice(), AC._databuffer(Int32[0])];
                dictionary=maskedpool,
                nullcount=0,
            )
            maskedouter = AC.ArrayData(
                StructType(),
                1,
                [BufferSlice()];
                children=[maskeddict],
                nullcount=0,
            )
            @test AC.validate_full(outerfield, maskedouter) === maskedouter
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
            vd = AC.ArrayData(
                vt,
                1,
                [BufferSlice(), AC._databuffer(Int32[0, 1]), AC._databuffer(UInt8[0xff])];
                nullcount=0,
            )
            t = DictionaryType(IntType(32, true), vt, false)
            f = Field("d", t; nullable=false)
            d = AC.ArrayData(
                t,
                1,
                [BufferSlice(), AC._databuffer(Int32[0])];
                dictionary=vd,
                nullcount=0,
            )
            validate_structural(f, d)
            validate_semantic(f, d)
            @test_throws ValidationError validate_full(f, d)
        end

        @testset "semantic result is cached" begin
            f, d = fromjulia("s", ["a", "b"])
            @test !(@atomic d.semachecked)
            validate_semantic(f, d)
            @test (@atomic d.semachecked)
            # The data-intrinsic scan is cached; Field contracts still run.
            validate_semantic(f, d)
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
        @test_throws ArgumentError RecordBatch(
            b.schema,
            [b.columns[1], AC.fromjulia("b", ["only-one"])[2]],
        )
        wrongtype = AC.fromjulia("a", Float64[1, 2, 3])[2]
        @test_throws ValidationError RecordBatch(Schema([b.schema.fields[1]]), [wrongtype])
        missingdata = AC.ArrayData(IntType(64, true), 3, BufferSlice[])
        @test_throws ValidationError RecordBatch(
            Schema([b.schema.fields[1]]),
            [missingdata],
        )
        empty_schema = Schema(Field[])
        @test RecordBatch(empty_schema, ArrayData[], 7).nrows == 7
        badutf8 = String(UInt8[0xff])
        @test_throws ValidationError RecordBatch(
            Schema(Field[]; metadata=Dict(badutf8 => "v")),
            ArrayData[],
            0,
        )
        @test_throws ValidationError RecordBatch(
            Schema(Field[]; metadata=Dict("k" => badutf8)),
            ArrayData[],
            0,
        )
        badendian = reinterpret(AC.Endianness, UInt8(0xff))
        @test_throws ValidationError RecordBatch(
            Schema(Field[]; endianness=badendian),
            ArrayData[],
            0,
        )
        nonnative =
            AC._native_endianness() == AC.LittleEndian ? AC.BigEndian : AC.LittleEndian
        @test_throws ValidationError RecordBatch(
            Schema(Field[]; endianness=nonnative),
            ArrayData[],
            0,
        )

        @testset "certified dictionary pools are not revisited" begin
            # The descriptor set is closed (the layout-registry ladder), so the
            # revisit observable is the pool's own semantic-cache bit: clear it
            # after certification — a revisit would run the semantic stage and
            # set it back; a honored certificate leaves it untouched.
            valuefield, pool = fromjulia("pool", ["x"])
            validate_semantic(valuefield, pool)
            validated = AC._ValidatedDictionaries(pool => nothing)
            @atomic :monotonic pool.semachecked = false

            dicttype = DictionaryType(IntType(8, false), valuefield.type, false)
            dictfield = Field("d", dicttype; nullable=false)
            dictdata = AC.ArrayData(
                dicttype,
                1,
                [BufferSlice(), AC._databuffer(UInt8[0])];
                dictionary=pool,
                nullcount=0,
            )
            @test AC._validate_semantic(dictfield, dictdata, validated) === dictdata
            @test !(@atomic :monotonic pool.semachecked)

            batch = AC.RecordBatch(Schema([dictfield]), [dictdata], 1, validated)
            @test batch.columns[1] === dictdata
            @test !(@atomic :monotonic pool.semachecked)
        end
    end

    @testset "typed element access (static schemas)" begin
        f, d = fromjulia("x", Int64[1, 2, 3])
        @test getvalue(Int64, f, d, 2) === Int64(2)
        @test materialize(Int64, f, d) == Int64[1, 2, 3]
        @test materialize(Int64, f, d) isa Vector{Int64}
        @test getvalue(Any, f, d, 1) === Int64(1)   # dynamic delegation
        @test_throws BoundsError getvalue(Int64, f, d, 4)
        # exact-match discipline: no conversion, no widening
        @test_throws ArgumentError getvalue(Int32, f, d, 1)
        @test_throws ArgumentError getvalue(Integer, f, d, 1)
        fm, dm = fromjulia("y", [1.5, missing])
        @test getvalue(Union{Missing,Float64}, fm, dm, 2) === missing
        @test isequal(materialize(Union{Missing,Float64}, fm, dm), [1.5, missing])
        @test_throws ArgumentError getvalue(Float64, fm, dm, 2)
        fs, ds = fromjulia("s", ["a", missing])
        @test getvalue(Union{Missing,String}, fs, ds, 1) == "a"
        fb, db = fromjulia("b", [true, false])
        @test materialize(Bool, fb, db) == [true, false]
        # A plain Vector{Bool} is a NON-nullable column (bit-packed through
        # the nullable builder, but the declaration is the input's).
        @test !fb.nullable
        @test nullcount(db) == 0
        @test fromjulia("bm", [true, missing])[1].nullable
        # lists recurse the claim
        fl, dl = fromjulia("l", [Int64[1, 2], Int64[]])
        @test getvalue(Vector{Int64}, fl, dl, 1) == [1, 2]
        @test materialize(Vector{Int64}, fl, dl) isa Vector{Vector{Int64}}
        @test_throws ArgumentError getvalue(Vector{Float64}, fl, dl, 1)
        fn, dn = fromjulia("ln", [[1.5, missing], missing])
        @test isequal(
            getvalue(Union{Missing,Vector{Union{Missing,Float64}}}, fn, dn, 1),
            [1.5, missing],
        )
        @test getvalue(Union{Missing,Vector{Union{Missing,Float64}}}, fn, dn, 2) === missing
        # structs: Vector{Pair} row or a NamedTuple claim (names must match)
        saf, sad = fromjulia("a", Int64[7, 8])
        sbf, sbd = fromjulia("b", ["x", "y"])
        sf = Field("st", StructType(); nullable=false, children=[saf, sbf])
        sd =
            AC.ArrayData(StructType(), 2, [BufferSlice()]; children=[sad, sbd], nullcount=0)
        NT = NamedTuple{(:a, :b),Tuple{Int64,String}}
        @test getvalue(NT, sf, sd, 2) === (a=Int64(8), b="y")
        @test materialize(NT, sf, sd) isa Vector{NT}
        @test getvalue(Vector{Pair{String,Any}}, sf, sd, 1) == ["a" => 7, "b" => "x"]
        WRONGNAME = NamedTuple{(:a, :c),Tuple{Int64,String}}
        @test_throws ArgumentError getvalue(WRONGNAME, sf, sd, 1)
        WRONGTYPE = NamedTuple{(:a, :b),Tuple{Int32,String}}
        @test_throws ArgumentError getvalue(WRONGTYPE, sf, sd, 1)
        # dictionary reads recurse into the pool
        df, dd = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, missing])
        @test getvalue(Union{Missing,String}, df, dd, 2) == "hi"
        @test getvalue(Union{Missing,String}, df, dd, 3) === missing
        # typed == dynamic on every covered layout
        for (ff, cc, T) in (
            (f, d, Int64),
            (fm, dm, Union{Missing,Float64}),
            (fs, ds, Union{Missing,String}),
            (fl, dl, Vector{Int64}),
            (df, dd, Union{Missing,String}),
        )
            @test isequal(materialize(T, ff, cc), materialize(ff, cc))
        end
        # The claim checks against the DESCRIPTOR: empty or all-null data
        # certifies nothing.
        fe, de = fromjulia("e", Int64[])
        @test_throws ArgumentError materialize(String, fe, de)
        @test materialize(Int64, fe, de) == Int64[]
        fan, dan = fromjulia("an", Union{Missing,Int64}[missing, missing])
        @test_throws ArgumentError materialize(Union{Missing,String}, fan, dan)
        fel, del = fromjulia("el", Vector{Int32}[])
        @test_throws ArgumentError materialize(Vector{Int64}, fel, del)
        @test materialize(Vector{Int32}, fel, del) == Vector{Int32}[]
        # Four heterogeneous NamedTuple fields (the compile-time-unrolled
        # struct row; ntuple closures lose per-field types at this arity).
        h1 = fromjulia("a", Int64[1, 2])
        h2 = fromjulia("b", [1.5, 2.5])
        h3 = fromjulia("c", ["x", "y"])
        h4 = fromjulia("d", [true, false])
        hf =
            Field("st", StructType(); nullable=false, children=[h1[1], h2[1], h3[1], h4[1]])
        hd = AC.ArrayData(
            StructType(),
            2,
            [BufferSlice()];
            children=[h1[2], h2[2], h3[2], h4[2]],
            nullcount=0,
        )
        NT4 = NamedTuple{(:a, :b, :c, :d),Tuple{Int64,Float64,String,Bool}}
        @test getvalue(NT4, hf, hd, 2) === (a=Int64(2), b=2.5, c="y", d=false)
        @test materialize(NT4, hf, hd) isa Vector{NT4}
        # A null struct never certifies a wrong claim either.
        WRONG4 = NamedTuple{(:a, :b, :c, :z),Tuple{Int64,Float64,String,Bool}}
        @test_throws ArgumentError getvalue(WRONG4, hf, hd, 1)
        # Typed recursion keeps the dynamic path's child LOGICAL bounds: a
        # hidden physical value past a child's logical length is unreadable.
        leafd = AC.ArrayData(
            IntType(64, true),
            1,
            [BufferSlice(), AC._databuffer(Int64[11, 22])],
        )
        listd = AC.ArrayData(
            ListType(false),
            1,
            [BufferSlice(), AC._databuffer(Int32[0, 2])];
            children=[leafd],
        )
        lfb = Field(
            "l",
            ListType(false);
            nullable=false,
            children=[Field("item", IntType(64, true); nullable=false)],
        )
        @test_throws BoundsError getvalue(Vector{Int64}, lfb, listd, 1)
        @test_throws BoundsError getvalue(lfb, listd, 1)   # dynamic parity
        # Non-exact NamedTuple shapes (Unions, UnionAlls) refuse with the
        # documented ArgumentError, never a generation-time MethodError.
        UNT = Union{
            NamedTuple{(:a, :b),Tuple{Int64,Int64}},
            NamedTuple{(:a, :b),Tuple{Int64,String}},
        }
        @test_throws ArgumentError getvalue(UNT, hf, hd, 1)
        @test_throws ArgumentError materialize(UNT, hf, hd)
        # Bulk extraction trusts the BITMAP, never a caller-supplied
        # null-count cache (the typed path serves unvalidated data).
        bmp = AC._bitmapbuffer([true, false, true])
        hd = AC.ArrayData(
            IntType(64, true),
            3,
            [bmp, AC._databuffer(Int64[10, 20, 30])];
            nullcount=0,
        )
        hfb = Field("h", IntType(64, true); nullable=true)
        @test isequal(materialize(Union{Missing,Int64}, hfb, hd), [10, missing, 30])
        @test_throws ArgumentError materialize(Int64, hfb, hd)
        # Hostile validity geometry stays a BoundsError, exactly like the
        # per-element path.
        short = AC.ArrayData(
            IntType(8, true),
            9,
            [BufferSlice(AC.heapregion(UInt8[0xff]), 0, 1), AC._databuffer(Int8.(1:9))];
            nullcount=0,
        )
        sf9 = Field("s", IntType(8, true); nullable=true)
        @test_throws BoundsError materialize(Union{Missing,Int8}, sf9, short)
        # Invalid widths (juliatype 64-bit fallback) refuse instead of
        # copying at the claim's width or asserting a mistyped load.
        i24 = AC.ArrayData(
            IntType(24, true),
            2,
            [BufferSlice(), AC._databuffer(UInt8[8, 0, 0, 7, 0, 0])],
        )
        f24 = Field("x", IntType(24, true); nullable=false)
        @test_throws ArgumentError materialize(Int64, f24, i24)
        @test_throws ArgumentError getvalue(Int64, f24, i24, 1)
        # The buffer is PADDED to hold two Float64 bit patterns: a removed
        # bulk-width gate would then copy wrong-width values successfully
        # instead of tripping buffer bounds — this pin must fail on the
        # wrong VALUES, not pass on an incidental bounds error.
        f64bytes = collect(reinterpret(UInt8, Float64[1.25, -3.5]))
        fl24 = AC.ArrayData(FloatType(24), 2, [BufferSlice(), AC._databuffer(f64bytes)])
        ff24 = Field("y", FloatType(24); nullable=false)
        @test_throws ArgumentError materialize(Float64, ff24, fl24)
        @test_throws ArgumentError getvalue(Float64, ff24, fl24, 1)
        # Fresh-process allocation: the typed hot loop must reach steady
        # state without compiler-introspection priming (a separate process
        # so this suite's own inference cannot mask a regression).
        @test success(
            `$(Base.julia_cmd()) --startup-file=no --project=$(Base.active_project()) $(joinpath(@__DIR__, "typed_alloc_child.jl"))`,
        )
    end

    # The suite commonly starts Julia with one thread. Run the memory-order
    # stress in a small four-thread child so this gate tests real OS-thread
    # interleavings on every invocation.
    @testset "threaded stress child" begin
        @test success(
            `$(Base.julia_cmd()) --startup-file=no --threads=4 --project=$(Base.active_project()) $(joinpath(@__DIR__, "threaded_stress.jl"))`,
        )
    end
end # ArrowCore testset
