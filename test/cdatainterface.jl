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

@testset "Arrow C Data Interface" begin
    @testset "struct sizes" begin
        @test sizeof(Arrow.ArrowSchema) == 9 * 8
        @test sizeof(Arrow.ArrowArray) == 10 * 8
    end

    # Helper: convert a Julia array to ArrowVector for export
    function to_arrow(x)
        return Arrow.toarrowvector(x)
    end

    @testset "export: format strings" begin
        for (input, expected) in [
            (Int8[1], "c"),
            (UInt8[1], "C"),
            (Int16[1], "s"),
            (UInt16[1], "S"),
            (Int32[1], "i"),
            (UInt32[1], "I"),
            (Int64[1], "l"),
            (UInt64[1], "L"),
            (Float32[1.0], "f"),
            (Float64[1.0], "g"),
            (Bool[true], "b"),
            (["hello"], "u"),
            ([missing], "n"),
        ]
            s_ref, a_ref = Arrow.to_c_data(to_arrow(input))
            GC.@preserve s_ref a_ref begin
                @test unsafe_string(s_ref[].format) == expected
            end
        end
    end

    @testset "export: nullable flag" begin
        s_ref, _ = Arrow.to_c_data(to_arrow(Union{Int32,Missing}[1, missing]))
        @test (s_ref[].flags & Arrow.CDATA_FLAG_NULLABLE) != 0

        s_ref2, _ = Arrow.to_c_data(to_arrow(Int32[1, 2]))
        @test (s_ref2[].flags & Arrow.CDATA_FLAG_NULLABLE) == 0
    end

    @testset "export: Int32 buffer contents" begin
        data = Int32[10, 20, 30]
        s_ref, a_ref = Arrow.to_c_data(to_arrow(data))
        arr = a_ref[]
        @test arr.length == 3
        @test arr.null_count == 0
        @test arr.offset == 0
        @test arr.n_buffers == 2
        @test arr.n_children == 0
        # validity buffer should be C_NULL (no nulls)
        validity_ptr = unsafe_load(arr.buffers)
        @test validity_ptr == C_NULL
        # data buffer
        data_ptr = unsafe_load(arr.buffers + sizeof(Ptr{Cvoid}))
        @test data_ptr != C_NULL
        result = unsafe_wrap(Array, Ptr{Int32}(data_ptr), 3; own=false)
        @test result == Int32[10, 20, 30]
    end

    @testset "export: validity bitmap" begin
        data = Union{Int32,Missing}[1, missing, 3]
        s_ref, a_ref = Arrow.to_c_data(to_arrow(data))
        arr = a_ref[]
        @test arr.null_count == 1
        validity_ptr = Ptr{UInt8}(unsafe_load(arr.buffers))
        @test validity_ptr != C_NULL
        byte = unsafe_load(validity_ptr)
        # bits 0,2 set; bit 1 clear (element 2 is missing)
        @test (byte & 0x01) != 0   # element 1: valid
        @test (byte & 0x02) == 0   # element 2: null
        @test (byte & 0x04) != 0   # element 3: valid
    end

    @testset "export: String list" begin
        data = ["hello", "world"]
        s_ref, a_ref = Arrow.to_c_data(to_arrow(data))
        arr = a_ref[]
        sch = s_ref[]
        @test unsafe_string(sch.format) == "u"
        @test arr.n_buffers == 3
        # offsets buffer
        off_ptr = Ptr{Int32}(unsafe_load(arr.buffers + sizeof(Ptr{Cvoid})))
        offsets = unsafe_wrap(Array, off_ptr, 3; own=false)
        @test offsets == Int32[0, 5, 10]
        # data buffer
        data_ptr = Ptr{UInt8}(unsafe_load(arr.buffers + 2*sizeof(Ptr{Cvoid})))
        str_bytes = unsafe_wrap(Array, data_ptr, 10; own=false)
        @test String(str_bytes) == "helloworld"
    end

    @testset "export: struct" begin
        data = [(x=Int32(1), y="a"), (x=Int32(2), y="b")]
        s_ref, a_ref = Arrow.to_c_data(to_arrow(data))
        sch = s_ref[]
        arr = a_ref[]
        @test unsafe_string(sch.format) == "+s"
        @test sch.n_children == 2
        @test arr.n_children == 2
        @test arr.n_buffers == 1
        # child 0: x field
        c0_sch = unsafe_load(unsafe_load(sch.children))
        @test unsafe_string(c0_sch.format) == "i"
    end

    @testset "export: release semantics" begin
        data = Int32[1, 2, 3]
        s_ref, a_ref = Arrow.to_c_data(to_arrow(data))
        arr = a_ref[]
        token = UInt64(UInt(arr.private_data))
        @test haskey(Arrow._EXPORT_ROOTS, token)

        # Simulate C calling release on the array
        ccall(
            arr.release,
            Cvoid,
            (Ptr{Arrow.ArrowArray},),
            Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref),
        )

        # Token should be removed
        @test !haskey(Arrow._EXPORT_ROOTS, token)
        # release pointer should be nulled out
        @test a_ref[].release == C_NULL
    end

    @testset "round-trip: Int32" begin
        data = Int32[1, 2, 3, 4, 5]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test collect(imported) == data
    end

    @testset "round-trip: Float64 with missing" begin
        data = Union{Float64,Missing}[1.0, missing, 3.14]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test isequal(collect(imported), data)
    end

    @testset "round-trip: Bool" begin
        data = [true, false, true, false]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test collect(imported) == data
    end

    @testset "round-trip: Bool with missing" begin
        data = Union{Bool,Missing}[true, missing, false]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test isequal(collect(imported), data)
    end

    @testset "round-trip: String" begin
        data = ["hello", "world", "foo"]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test collect(imported) == data
    end

    @testset "round-trip: String with missing" begin
        data = Union{String,Missing}["hello", missing, "world"]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test isequal(collect(imported), data)
    end

    @testset "round-trip: Date" begin
        data = [Dates.Date(2020, 1, 1), Dates.Date(2021, 6, 15)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref));
            convert=false,
        )
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: Timestamp" begin
        data = [Dates.DateTime(2020, 1, 1), Dates.DateTime(2021, 6, 15)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref));
            convert=false,
        )
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: struct" begin
        data = [(x=Int32(1), y="a"), (x=Int32(2), y="b")]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        result = collect(imported)
        @test length(result) == 2
        @test result[1].x == Int32(1)
        @test result[1].y == "a"
        @test result[2].x == Int32(2)
        @test result[2].y == "b"
    end

    @testset "round-trip: dict encoded" begin
        data = Arrow.DictEncode(["a", "b", "a", "c", "b"])
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test collect(imported) == ["a", "b", "a", "c", "b"]
    end

    @testset "round-trip: null array" begin
        data = fill(missing, 5)
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "n"
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref)),
        )
        @test length(imported) == 5
        @test all(ismissing, imported)
    end

    @testset "import: non-zero offset" begin
        # Manually construct an ArrowArray with offset=2
        data = Int32[99, 99, 1, 2, 3]   # logical elements start at index 3
        buf_ptrs = Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(data))]

        arr_ref = Ref(
            Arrow.ArrowArray(
                Int64(3),   # length = 3
                Int64(0),   # null_count = 0
                Int64(2),   # offset = 2
                Int64(2),   # n_buffers = 2
                Int64(0),   # n_children = 0
                Ptr{Ptr{Cvoid}}(pointer(buf_ptrs)),
                Ptr{Ptr{Arrow.ArrowArray}}(C_NULL),
                Ptr{Arrow.ArrowArray}(C_NULL),
                Ptr{Cvoid}(C_NULL),  # no release needed (Julia-owned data)
                Ptr{Cvoid}(C_NULL),
            ),
        )
        fmt_bytes = Vector{UInt8}("i\0")
        sch_ref = Ref(
            Arrow.ArrowSchema(
                Cstring(pointer(fmt_bytes)),
                Cstring(C_NULL),
                Cstring(C_NULL),
                Int64(0),
                Int64(0),
                Ptr{Ptr{Arrow.ArrowSchema}}(C_NULL),
                Ptr{Arrow.ArrowSchema}(C_NULL),
                Ptr{Cvoid}(C_NULL),
                Ptr{Cvoid}(C_NULL),
            ),
        )
        GC.@preserve data buf_ptrs fmt_bytes arr_ref sch_ref begin
            imported = Arrow.from_c_data(
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, sch_ref)),
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, arr_ref)),
            )
            @test collect(imported) == Int32[1, 2, 3]
        end
    end

    @testset "import: C_NULL validity with null_count=0" begin
        data = Int32[10, 20, 30]
        buf_ptrs = Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(data))]
        arr_ref = Ref(
            Arrow.ArrowArray(
                Int64(3),
                Int64(0),
                Int64(0),
                Int64(2),
                Int64(0),
                Ptr{Ptr{Cvoid}}(pointer(buf_ptrs)),
                Ptr{Ptr{Arrow.ArrowArray}}(C_NULL),
                Ptr{Arrow.ArrowArray}(C_NULL),
                Ptr{Cvoid}(C_NULL),
                Ptr{Cvoid}(C_NULL),
            ),
        )
        fmt_bytes = Vector{UInt8}("i\0")
        sch_ref = Ref(
            Arrow.ArrowSchema(
                Cstring(pointer(fmt_bytes)),
                Cstring(C_NULL),
                Cstring(C_NULL),
                Int64(0),
                Int64(0),
                Ptr{Ptr{Arrow.ArrowSchema}}(C_NULL),
                Ptr{Arrow.ArrowSchema}(C_NULL),
                Ptr{Cvoid}(C_NULL),
                Ptr{Cvoid}(C_NULL),
            ),
        )
        GC.@preserve data buf_ptrs fmt_bytes arr_ref sch_ref begin
            imported = Arrow.from_c_data(
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, sch_ref)),
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, arr_ref)),
            )
            @test collect(imported) == Int32[10, 20, 30]
        end
    end

    @testset "metadata serialization round-trip" begin
        data = Int32[1, 2, 3]
        av = to_arrow(data)
        # Manually create a Primitive with metadata
        meta = Base.ImmutableDict("key1" => "val1", "key2" => "val2")
        av_meta =
            Arrow.Primitive(eltype(av), av.arrow, av.validity, av.data, length(av), meta)
        s_ref, a_ref = Arrow.to_c_data(av_meta)
        sch = s_ref[]
        @test sch.metadata != C_NULL
        parsed = Arrow._parse_c_metadata(sch.metadata)
        @test parsed isa Base.ImmutableDict
        @test parsed["key1"] == "val1"
        @test parsed["key2"] == "val2"
    end

    @testset "from_c_data table" begin
        col1 = to_arrow(Int32[1, 2, 3])
        col2 = to_arrow(["a", "b", "c"])
        s1, a1 = Arrow.to_c_data(col1; name="x")
        s2, a2 = Arrow.to_c_data(col2; name="y")
        tbl = Arrow.from_c_data(
            [
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s1)),
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s2)),
            ],
            [
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a1)),
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a2)),
            ],
        )
        @test Tables.columnnames(tbl) == [:x, :y]
        @test collect(Tables.getcolumn(tbl, :x)) == Int32[1, 2, 3]
        @test collect(Tables.getcolumn(tbl, :y)) == ["a", "b", "c"]
    end

    # Helper: convert a Ref to a Ptr{Cvoid} for from_c_data
    _cptr(r::Ref{Arrow.ArrowSchema}) =
        Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, r))
    _cptr(r::Ref{Arrow.ArrowArray}) =
        Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, r))

    # ── Lists ────────────────────────────────────────────────────────────────

    @testset "round-trip: list of Int32 (+l)" begin
        data = [[Int32(1), Int32(2), Int32(3)], [Int32(4), Int32(5)], [Int32(6)]]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "+l"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref))
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: list of Int32 with missing (+l)" begin
        data = Union{Vector{Int32},Missing}[[Int32(1), Int32(2)], missing, [Int32(3)]]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "+l"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref))
        @test isequal(collect(imported), collect(av))
    end

    @testset "round-trip: list of String (+l)" begin
        data = [["a", "bb"], ["ccc"], ["d", "ee", "fff"]]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "+l"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref))
        @test collect(imported) == collect(av)
    end

    # ── Fixed-size list ──────────────────────────────────────────────────────

    @testset "round-trip: fixed-size list NTuple{2,Float32} (+w:2)" begin
        data = [(1.0f0, 2.0f0), (3.0f0, 4.0f0), (5.0f0, 6.0f0)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "+w:2"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: fixed-size list NTuple{3,Int64} (+w:3)" begin
        data = [(Int64(1), Int64(2), Int64(3)), (Int64(4), Int64(5), Int64(6))]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "+w:3"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    # ── Map ──────────────────────────────────────────────────────────────────

    @testset "round-trip: map Dict{String,Int32} (+m)" begin
        data = [Dict("a" => Int32(1), "b" => Int32(2)), Dict("c" => Int32(3))]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "+m"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref))
        @test collect(imported) == collect(av)
    end

    # ── Unions ───────────────────────────────────────────────────────────────

    @testset "round-trip: dense union Union{Int32,String} (+ud:)" begin
        data = Union{Int32,String}[Int32(1), "hello", Int32(3), "world"]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test startswith(unsafe_string(s_ref[].format), "+ud:")
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref))
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: sparse union Union{Int32,Float64} (+us:)" begin
        data = Union{Int32,Float64}[Int32(1), 2.0, Int32(3), 4.5]
        av = Arrow.toarrowvector(data; denseunions=false)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test startswith(unsafe_string(s_ref[].format), "+us:")
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref))
        @test collect(imported) == collect(av)
    end

    # ── Time-of-day ──────────────────────────────────────────────────────────

    @testset "round-trip: Time nanoseconds (ttn)" begin
        data = [Dates.Time(12, 30, 0), Dates.Time(0, 0, 1, 0, 0, 42)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "ttn"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    # ── Duration ─────────────────────────────────────────────────────────────

    @testset "round-trip: Duration seconds (tDs)" begin
        data = [Dates.Second(5), Dates.Second(10), Dates.Second(-1)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "tDs"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: Duration milliseconds (tDm)" begin
        data = [Dates.Millisecond(100), Dates.Millisecond(-50)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "tDm"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: Duration microseconds (tDu)" begin
        data = [Dates.Microsecond(1000), Dates.Microsecond(2000)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "tDu"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: Duration nanoseconds (tDn)" begin
        data = [Dates.Nanosecond(999), Dates.Nanosecond(0)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "tDn"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    # ── Timestamp with timezone ───────────────────────────────────────────────

    @testset "round-trip: Timestamp with UTC timezone (tsm:UTC)" begin
        data = [
            TimeZones.ZonedDateTime(2023, 1, 1, TimeZones.tz"UTC"),
            TimeZones.ZonedDateTime(2023, 6, 1, 12, 0, 0, TimeZones.tz"UTC"),
        ]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        fmt = unsafe_string(s_ref[].format)
        @test startswith(fmt, "ts") && endswith(fmt, ":UTC")
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    # ── Interval ─────────────────────────────────────────────────────────────

    @testset "round-trip: Interval year-month (tiM)" begin
        IU = Arrow.Meta.Flatbuf.IntervalUnit
        YM = Arrow.Interval{IU.YEAR_MONTH,Int32}
        data = YM[YM(Int32(3)), YM(Int32(-1)), YM(Int32(0))]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "tiM"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    @testset "round-trip: Interval day-time (tiD)" begin
        IU = Arrow.Meta.Flatbuf.IntervalUnit
        DT = Arrow.Interval{IU.DAY_TIME,Int64}
        data = DT[DT(Int64(86400)), DT(Int64(0)), DT(Int64(-3600))]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "tiD"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    # ── Decimal ───────────────────────────────────────────────────────────────

    @testset "round-trip: Decimal{10,2,Int128} (d:10,2,128)" begin
        D = Arrow.Decimal{10,2,Int128}
        data = D[D(Int128(314)), D(Int128(-100)), D(Int128(0))]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        @test unsafe_string(s_ref[].format) == "d:10,2,128"
        imported = Arrow.from_c_data(_cptr(s_ref), _cptr(a_ref); convert=false)
        @test collect(imported) == collect(av)
    end

    # ── Table export ──────────────────────────────────────────────────────────

    @testset "to_c_data: Arrow.Table round-trip" begin
        io = IOBuffer()
        Arrow.write(io, (x=Int32[1, 2, 3], y=["a", "b", "c"]))
        seekstart(io)
        tbl = Arrow.Table(io)
        srefs, arefs = Arrow.to_c_data(tbl)
        @test length(srefs) == 2
        sptrs = [_cptr(r) for r in srefs]
        aptrs = [_cptr(r) for r in arefs]
        GC.@preserve srefs arefs begin
            tbl2 = Arrow.from_c_data(sptrs, aptrs; names=[:x, :y])
            @test Tables.columnnames(tbl2) == [:x, :y]
            @test collect(Tables.getcolumn(tbl2, :x)) == Int32[1, 2, 3]
            @test collect(Tables.getcolumn(tbl2, :y)) == ["a", "b", "c"]
        end
    end

    # ── Bool with non-byte-aligned offset ────────────────────────────────────

    @testset "import: Bool with non-byte-aligned offset=3" begin
        # Packed byte: 0b10110110 (LSB first)
        # bits 3,4,5,6,7 → 0,1,1,0,1  (reading from bit-position 3)
        bools = UInt8[0b10110110]
        validity = UInt8[0xff]
        buf_ptrs = Ptr{Cvoid}[Ptr{Cvoid}(pointer(validity)), Ptr{Cvoid}(pointer(bools))]
        arr_ref = Ref(
            Arrow.ArrowArray(
                Int64(5),
                Int64(0),
                Int64(3),
                Int64(2),
                Int64(0),
                Ptr{Ptr{Cvoid}}(pointer(buf_ptrs)),
                Ptr{Ptr{Arrow.ArrowArray}}(C_NULL),
                Ptr{Arrow.ArrowArray}(C_NULL),
                Ptr{Cvoid}(C_NULL),
                Ptr{Cvoid}(C_NULL),
            ),
        )
        fmt_bytes = Vector{UInt8}("b\0")
        sch_ref = Ref(
            Arrow.ArrowSchema(
                Cstring(pointer(fmt_bytes)),
                Cstring(C_NULL),
                Cstring(C_NULL),
                Int64(0),
                Int64(0),
                Ptr{Ptr{Arrow.ArrowSchema}}(C_NULL),
                Ptr{Arrow.ArrowSchema}(C_NULL),
                Ptr{Cvoid}(C_NULL),
                Ptr{Cvoid}(C_NULL),
            ),
        )
        GC.@preserve bools validity buf_ptrs arr_ref sch_ref begin
            imported = Arrow.from_c_data(_cptr(sch_ref), _cptr(arr_ref))
            @test collect(imported) == [false, true, true, false, true]
        end
    end

    # ── release_c_data idempotency ────────────────────────────────────────────

    @testset "release_c_data: double-release is a no-op" begin
        col1 = to_arrow(Int32[1, 2])
        col2 = to_arrow(["a", "b"])
        s1, a1 = Arrow.to_c_data(col1; name="x")
        s2, a2 = Arrow.to_c_data(col2; name="y")
        sptrs = [_cptr(s1), _cptr(s2)]
        aptrs = [_cptr(a1), _cptr(a2)]
        GC.@preserve s1 a1 s2 a2 begin
            tbl = Arrow.from_c_data(sptrs, aptrs)
            Arrow.release_c_data(tbl)
            @test_nowarn Arrow.release_c_data(tbl)  # second call must not throw
        end
    end
end # @testset "Arrow C Data Interface"
