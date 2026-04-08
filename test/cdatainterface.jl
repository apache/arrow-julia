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
        @test sizeof(Arrow.ArrowArray)  == 10 * 8
    end

    # Helper: convert a Julia array to ArrowVector for export
    function to_arrow(x)
        return Arrow.toarrowvector(x)
    end

    @testset "export: format strings" begin
        for (input, expected) in [
            (Int8[1],      "c"),
            (UInt8[1],     "C"),
            (Int16[1],     "s"),
            (UInt16[1],    "S"),
            (Int32[1],     "i"),
            (UInt32[1],    "I"),
            (Int64[1],     "l"),
            (UInt64[1],    "L"),
            (Float32[1.0], "f"),
            (Float64[1.0], "g"),
            (Bool[true],   "b"),
            (["hello"],    "u"),
            ([missing],    "n"),
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
        @test arr.length    == 3
        @test arr.null_count == 0
        @test arr.offset    == 0
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
        @test arr.n_buffers  == 1
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
        ccall(arr.release, Cvoid, (Ptr{Arrow.ArrowArray},),
              Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a_ref))

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
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
        )
        @test collect(imported) == data
    end

    @testset "round-trip: Float64 with missing" begin
        data = Union{Float64,Missing}[1.0, missing, 3.14]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
        )
        @test isequal(collect(imported), data)
    end

    @testset "round-trip: Bool" begin
        data = [true, false, true, false]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
        )
        @test collect(imported) == data
    end

    @testset "round-trip: Bool with missing" begin
        data = Union{Bool,Missing}[true, missing, false]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
        )
        @test isequal(collect(imported), data)
    end

    @testset "round-trip: String" begin
        data = ["hello", "world", "foo"]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
        )
        @test collect(imported) == data
    end

    @testset "round-trip: String with missing" begin
        data = Union{String,Missing}["hello", missing, "world"]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
        )
        @test isequal(collect(imported), data)
    end

    @testset "round-trip: Date" begin
        data = [Dates.Date(2020, 1, 1), Dates.Date(2021, 6, 15)]
        av = to_arrow(data)
        s_ref, a_ref = Arrow.to_c_data(av)
        imported = Arrow.from_c_data(
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s_ref)),
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref));
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
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref));
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
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
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
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
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
            Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a_ref)),
        )
        @test length(imported) == 5
        @test all(ismissing, imported)
    end

    @testset "import: non-zero offset" begin
        # Manually construct an ArrowArray with offset=2
        data = Int32[99, 99, 1, 2, 3]   # logical elements start at index 3
        buf_ptrs = Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(data))]

        arr_ref = Ref(Arrow.ArrowArray(
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
        ))
        fmt_bytes = Vector{UInt8}("i\0")
        sch_ref = Ref(Arrow.ArrowSchema(
            Cstring(pointer(fmt_bytes)),
            Cstring(C_NULL),
            Cstring(C_NULL),
            Int64(0),
            Int64(0),
            Ptr{Ptr{Arrow.ArrowSchema}}(C_NULL),
            Ptr{Arrow.ArrowSchema}(C_NULL),
            Ptr{Cvoid}(C_NULL),
            Ptr{Cvoid}(C_NULL),
        ))
        GC.@preserve data buf_ptrs fmt_bytes arr_ref sch_ref begin
            imported = Arrow.from_c_data(
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, sch_ref)),
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  arr_ref)),
            )
            @test collect(imported) == Int32[1, 2, 3]
        end
    end

    @testset "import: C_NULL validity with null_count=0" begin
        data = Int32[10, 20, 30]
        buf_ptrs = Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(data))]
        arr_ref = Ref(Arrow.ArrowArray(
            Int64(3), Int64(0), Int64(0), Int64(2), Int64(0),
            Ptr{Ptr{Cvoid}}(pointer(buf_ptrs)),
            Ptr{Ptr{Arrow.ArrowArray}}(C_NULL), Ptr{Arrow.ArrowArray}(C_NULL),
            Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL),
        ))
        fmt_bytes = Vector{UInt8}("i\0")
        sch_ref = Ref(Arrow.ArrowSchema(
            Cstring(pointer(fmt_bytes)), Cstring(C_NULL), Cstring(C_NULL),
            Int64(0), Int64(0),
            Ptr{Ptr{Arrow.ArrowSchema}}(C_NULL), Ptr{Arrow.ArrowSchema}(C_NULL),
            Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL),
        ))
        GC.@preserve data buf_ptrs fmt_bytes arr_ref sch_ref begin
            imported = Arrow.from_c_data(
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, sch_ref)),
                Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray},  arr_ref)),
            )
            @test collect(imported) == Int32[10, 20, 30]
        end
    end

    @testset "metadata serialization round-trip" begin
        data = Int32[1, 2, 3]
        av = to_arrow(data)
        # Manually create a Primitive with metadata
        meta = Base.ImmutableDict("key1" => "val1", "key2" => "val2")
        av_meta = Arrow.Primitive(eltype(av), av.arrow, av.validity, av.data, length(av), meta)
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
            [Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s1)),
             Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s2))],
            [Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a1)),
             Ptr{Cvoid}(Base.unsafe_convert(Ptr{Arrow.ArrowArray}, a2))],
        )
        @test Tables.columnnames(tbl) == [:x, :y]
        @test collect(Tables.getcolumn(tbl, :x)) == Int32[1, 2, 3]
        @test collect(Tables.getcolumn(tbl, :y)) == ["a", "b", "c"]
    end

end # @testset "Arrow C Data Interface"
