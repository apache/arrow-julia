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

using Serialization

function _cdata_release_schema(ptr::Ptr{Arrow.ArrowSchema})
    schema = unsafe_load(ptr)
    unsafe_store!(
        ptr,
        Arrow.ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            schema.n_children,
            schema.children,
            schema.dictionary,
            C_NULL,
            schema.private_data,
        ),
    )
    return
end

function _cdata_release_array(ptr::Ptr{Arrow.ArrowArray})
    array = unsafe_load(ptr)
    unsafe_store!(
        ptr,
        Arrow.ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            array.n_buffers,
            array.n_children,
            array.buffers,
            array.children,
            array.dictionary,
            C_NULL,
            array.private_data,
        ),
    )
    return
end

using Dates

const _CDATA_RELEASE_SCHEMA =
    @cfunction(_cdata_release_schema, Cvoid, (Ptr{Arrow.ArrowSchema},))
const _CDATA_RELEASE_ARRAY =
    @cfunction(_cdata_release_array, Cvoid, (Ptr{Arrow.ArrowArray},))
const _CDATA_REENTRANT_OBJECT = Ref{Any}(nothing)
const _CDATA_REENTRANT_RELEASES = Ref(0)

function _cdata_release_array_reentrant(ptr::Ptr{Arrow.ArrowArray})
    _CDATA_REENTRANT_RELEASES[] += 1
    x = _CDATA_REENTRANT_OBJECT[]
    x === nothing || Arrow.release_c_data(x)
    _cdata_release_array(ptr)
    return
end

const _CDATA_RELEASE_ARRAY_REENTRANT =
    @cfunction(_cdata_release_array_reentrant, Cvoid, (Ptr{Arrow.ArrowArray},))

mutable struct CDataFixture
    schema::Ref{Arrow.ArrowSchema}
    array::Ref{Arrow.ArrowArray}
    roots::Vector{Any}
end

const _CDATA_FIXTURE_ROOTS = Any[]

_schema_ptr(x::CDataFixture) = Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, x.schema)
_array_ptr(x::CDataFixture) = Base.unsafe_convert(Ptr{Arrow.ArrowArray}, x.array)

function _cstring_root(s::Union{Nothing,String}, roots)
    s === nothing && return Cstring(C_NULL)
    bytes = Vector{UInt8}(s * "\0")
    push!(roots, bytes)
    return Cstring(pointer(bytes))
end

function _metadata_root(meta::Union{Nothing,AbstractDict}, roots)
    meta === nothing && return Cstring(C_NULL)
    io = IOBuffer()
    write(io, Int32(length(meta)))
    for (k, v) in meta
        kb = codeunits(String(k))
        vb = codeunits(String(v))
        write(io, Int32(length(kb)))
        write(io, kb)
        write(io, Int32(length(vb)))
        write(io, vb)
    end
    bytes = take!(io)
    push!(roots, bytes)
    return Cstring(pointer(bytes))
end

function _cdata_fixture(
    fmt::String,
    len::Integer,
    buffers::Vector{Ptr{Cvoid}};
    name=nothing,
    metadata=nothing,
    flags::Int64=0,
    null_count::Int64=0,
    offset::Int64=0,
    children::Vector{CDataFixture}=CDataFixture[],
)
    roots = Any[]
    append!(roots, children)
    schema_ptrs =
        [Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, child.schema) for child in children]
    array_ptrs =
        [Base.unsafe_convert(Ptr{Arrow.ArrowArray}, child.array) for child in children]
    if !isempty(children)
        push!(roots, schema_ptrs)
        push!(roots, array_ptrs)
    end
    buffer_ptrs = copy(buffers)
    if !isempty(buffer_ptrs)
        push!(roots, buffer_ptrs)
    end
    schema = Ref(
        Arrow.ArrowSchema(
            _cstring_root(fmt, roots),
            _cstring_root(name, roots),
            _metadata_root(metadata, roots),
            flags,
            Int64(length(children)),
            isempty(children) ? Ptr{Ptr{Arrow.ArrowSchema}}(C_NULL) :
            Ptr{Ptr{Arrow.ArrowSchema}}(pointer(schema_ptrs)),
            Ptr{Arrow.ArrowSchema}(C_NULL),
            _CDATA_RELEASE_SCHEMA,
            Ptr{Cvoid}(C_NULL),
        ),
    )
    array = Ref(
        Arrow.ArrowArray(
            Int64(len),
            null_count,
            offset,
            Int64(length(buffer_ptrs)),
            Int64(length(children)),
            isempty(buffer_ptrs) ? Ptr{Ptr{Cvoid}}(C_NULL) :
            Ptr{Ptr{Cvoid}}(pointer(buffer_ptrs)),
            isempty(children) ? Ptr{Ptr{Arrow.ArrowArray}}(C_NULL) :
            Ptr{Ptr{Arrow.ArrowArray}}(pointer(array_ptrs)),
            Ptr{Arrow.ArrowArray}(C_NULL),
            _CDATA_RELEASE_ARRAY,
            Ptr{Cvoid}(C_NULL),
        ),
    )
    push!(roots, schema)
    push!(roots, array)
    fixture = CDataFixture(schema, array, roots)
    push!(_CDATA_FIXTURE_ROOTS, fixture)
    return fixture
end

function _primitive_fixture(
    fmt,
    data::Vector{T};
    validity=nothing,
    null_count::Int64=0,
    flags::Int64=0,
    offset::Int64=0,
    len::Int=length(data) - Int(offset),
    name=nothing,
    metadata=nothing,
) where {T}
    roots = Any[data]
    buffers = Ptr{Cvoid}[
        validity === nothing ? Ptr{Cvoid}(C_NULL) : Ptr{Cvoid}(pointer(validity)),
        isempty(data) ? Ptr{Cvoid}(C_NULL) : Ptr{Cvoid}(pointer(data)),
    ]
    validity !== nothing && push!(roots, validity)
    fixture = _cdata_fixture(
        fmt,
        len,
        buffers;
        name=name,
        metadata=metadata,
        flags=flags,
        null_count=null_count,
        offset=offset,
    )
    append!(fixture.roots, roots)
    return fixture
end

function _unaligned_primitive_fixture(
    fmt,
    data::Vector{T};
    offset::Int64=Int64(0),
    len::Int64=Int64(length(data)) - offset,
) where {T}
    nbytes = length(data) * sizeof(T)
    bytes = Vector{UInt8}(undef, nbytes + 1)
    unsafe_copyto!(pointer(bytes, 2), Ptr{UInt8}(pointer(data)), nbytes)
    buffers = Ptr{Cvoid}[Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(pointer(bytes, 2))]
    fixture = _cdata_fixture(fmt, len, buffers; offset=offset)
    append!(fixture.roots, Any[data, bytes])
    return fixture
end

function _cdata_import_collect(f::CDataFixture)
    x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
    return collect(x)
end

function _set_schema!(
    f::CDataFixture;
    format=f.schema[].format,
    name=f.schema[].name,
    metadata=f.schema[].metadata,
    flags=f.schema[].flags,
    n_children=f.schema[].n_children,
    children=f.schema[].children,
    dictionary=f.schema[].dictionary,
    release=f.schema[].release,
    private_data=f.schema[].private_data,
)
    f.schema[] = Arrow.ArrowSchema(
        format,
        name,
        metadata,
        flags,
        n_children,
        children,
        dictionary,
        release,
        private_data,
    )
    return f
end

function _set_array!(
    f::CDataFixture;
    length=f.array[].length,
    null_count=f.array[].null_count,
    offset=f.array[].offset,
    n_buffers=f.array[].n_buffers,
    buffers=f.array[].buffers,
    n_children=f.array[].n_children,
    children=f.array[].children,
    dictionary=f.array[].dictionary,
    release=f.array[].release,
    private_data=f.array[].private_data,
)
    f.array[] = Arrow.ArrowArray(
        length,
        null_count,
        offset,
        n_buffers,
        n_children,
        buffers,
        children,
        dictionary,
        release,
        private_data,
    )
    return f
end

@testset "Arrow C Data Interface import" begin
    @testset "ABI layout" begin
        @test isbitstype(Arrow.ArrowSchema)
        @test isbitstype(Arrow.ArrowArray)
        if Sys.WORD_SIZE == 64
            ptr = sizeof(Ptr{Cvoid})
            @test sizeof(Arrow.ArrowSchema) == 7 * ptr + 2 * sizeof(Int64)
            @test sizeof(Arrow.ArrowArray) == 5 * ptr + 5 * sizeof(Int64)
        end
        @test fieldcount(Arrow.ArrowSchema) == 9
        @test fieldcount(Arrow.ArrowArray) == 10
    end

    @testset "primitive arrays" begin
        data = Int32[1, 2, 3]
        f = _primitive_fixture("i", data)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test x isa Arrow.CDataVector
        @test collect(x) == Int32[1, 2, 3]
        @test copy(x) == Int32[1, 2, 3]
        data[1] = 99
        @test collect(x) == Int32[99, 2, 3]

        f = _unaligned_primitive_fixture("i", Int32[7, 8, 9])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == Int32[7, 8, 9]
        fill!(f.roots[end], 0x00)
        @test collect(x) == Int32[7, 8, 9]
        f = _unaligned_primitive_fixture("i", Int32[6, 7, 8, 9]; offset=Int64(1), len=2)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == Int32[7, 8]
        fill!(f.roots[end], 0x00)
        @test collect(x) == Int32[7, 8]

        validity = UInt8[0b00000101]
        f = _primitive_fixture(
            "i",
            Int32[10, 20, 30];
            validity=validity,
            null_count=Int64(1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
        )
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test isequal(collect(x), Union{Int32,Missing}[10, missing, 30])

        f = _primitive_fixture(
            "i",
            Int32[10, 20, 30];
            validity=validity,
            null_count=Int64(-1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
        )
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test Arrow.nullcount(x) == 1
        @test isequal(collect(x), Union{Int32,Missing}[10, missing, 30])
    end

    @testset "null arrays" begin
        f = _cdata_fixture("n", 3, Ptr{Cvoid}[]; null_count=Int64(3))
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test x isa Arrow.CDataVector{Missing}
        @test size(x) == (3,)
        @test Arrow.validitybitmap(x) === nothing
        @test Arrow.nullcount(x) == 3
        @test x[2] === missing
        @test isequal(collect(x), [missing, missing, missing])
        f = _cdata_fixture("n", 3, Ptr{Cvoid}[]; null_count=Int64(3))
        node = Arrow._validate_node(_schema_ptr(f), _array_ptr(f); top_level=true)
        validity = Arrow._make_validity(node)
        @test validity.bytes == UInt8[]
        @test validity.null_count == 0
    end

    @testset "bool bit offsets" begin
        data = UInt8[0b10110110]
        validity = UInt8[0xff]
        f = _cdata_fixture(
            "b",
            5,
            Ptr{Cvoid}[Ptr{Cvoid}(pointer(validity)), Ptr{Cvoid}(pointer(data))];
            offset=Int64(3),
        )
        append!(f.roots, Any[data, validity])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == [false, true, true, false, true]

        validity = UInt8[0b00001101]
        f = _cdata_fixture(
            "b",
            4,
            Ptr{Cvoid}[Ptr{Cvoid}(pointer(validity)), Ptr{Cvoid}(pointer(data))];
            flags=Arrow.ARROW_FLAG_NULLABLE,
            null_count=Int64(-1),
        )
        append!(f.roots, Any[data, validity])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test Arrow.nullcount(x) == 1
        @test isequal(collect(x), Union{Bool,Missing}[false, missing, true, false])
    end

    @testset "string and binary arrays" begin
        offsets = Int32[0, 3, 3, 6]
        bytes = Vector{UInt8}(codeunits("abcdef"))
        f = _cdata_fixture(
            "u",
            3,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), Ptr{Cvoid}(pointer(bytes))],
        )
        append!(f.roots, Any[offsets, bytes])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == ["abc", "", "def"]
        offsets[2] = 2
        @test collect(x) == ["ab", "c", "def"]

        offset_values = Int32[0, 1, 3]
        offset_nbytes = length(offset_values) * sizeof(Int32)
        offset_bytes = Vector{UInt8}(undef, offset_nbytes + 1)
        unsafe_copyto!(
            pointer(offset_bytes, 2),
            Ptr{UInt8}(pointer(offset_values)),
            offset_nbytes,
        )
        bytes = Vector{UInt8}(codeunits("abc"))
        f = _cdata_fixture(
            "u",
            2,
            Ptr{Cvoid}[
                C_NULL,
                Ptr{Cvoid}(pointer(offset_bytes, 2)),
                Ptr{Cvoid}(pointer(bytes)),
            ],
        )
        append!(f.roots, Any[offset_bytes, bytes])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        fill!(offset_bytes, 0x00)
        @test collect(x) == ["a", "bc"]

        offsets64 = Int64[0, 2, 5]
        bytes2 = Vector{UInt8}(codeunits("hello"))
        f = _cdata_fixture(
            "U",
            2,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets64)), Ptr{Cvoid}(pointer(bytes2))],
        )
        append!(f.roots, Any[offsets64, bytes2])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == ["he", "llo"]

        bad_utf8 = UInt8[0xff]
        offsets = Int32[0, 1]
        f = _cdata_fixture(
            "u",
            1,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), Ptr{Cvoid}(pointer(bad_utf8))],
        )
        append!(f.roots, Any[offsets, bad_utf8])
        @test_throws ArgumentError Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))

        offsets = Int32[0, 2, 3]
        bytes = UInt8[0x01, 0x02, 0xff]
        f = _cdata_fixture(
            "z",
            2,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), Ptr{Cvoid}(pointer(bytes))],
        )
        append!(f.roots, Any[offsets, bytes])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == [b"\x01\x02", b"\xff"]

        offsets64 = Int64[0, 1, 3]
        f = _cdata_fixture(
            "Z",
            2,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets64)), Ptr{Cvoid}(pointer(bytes))],
        )
        append!(f.roots, Any[offsets64, bytes])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == [b"\x01", b"\x02\xff"]

        offsets = Int32[123]
        f = _cdata_fixture("u", 0, Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), C_NULL])
        push!(f.roots, offsets)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == String[]

        offsets = Int32[123, 123, 123]
        f = _cdata_fixture("u", 2, Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), C_NULL])
        push!(f.roots, offsets)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == ["", ""]

        offsets = Int32[0, 1, 2, 3]
        bytes = Vector{UInt8}(codeunits("abc"))
        validity = UInt8[0b00000101]
        f = _cdata_fixture(
            "u",
            3,
            Ptr{Cvoid}[
                Ptr{Cvoid}(pointer(validity)),
                Ptr{Cvoid}(pointer(offsets)),
                Ptr{Cvoid}(pointer(bytes)),
            ];
            flags=Arrow.ARROW_FLAG_NULLABLE,
            null_count=Int64(-1),
        )
        append!(f.roots, Any[offsets, bytes, validity])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        y = copy(x)
        Arrow.release_c_data(x)
        @test isequal(y, Union{String,Missing}["a", missing, "c"])
        @test_throws ArgumentError x[1]
    end

    @testset "fixed size binary" begin
        data = UInt8[0x01, 0x02, 0x03, 0x04, 0x05, 0x06]
        f = _cdata_fixture("w:3", 2, Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(data))])
        push!(f.roots, data)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == [(0x01, 0x02, 0x03), (0x04, 0x05, 0x06)]
    end

    @testset "list of primitives" begin
        child = _primitive_fixture("i", Int32[1, 2, 3, 4, 5])
        offsets = Int32[0, 2, 5]
        f = _cdata_fixture(
            "+l",
            2,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets))];
            children=[child],
        )
        push!(f.roots, offsets)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == [Int32[1, 2], Int32[3, 4, 5]]

        child = _primitive_fixture("i", Int32[10, 20, 30, 40, 50, 60])
        offsets = Int64[0, 1, 3, 6]
        f = _cdata_fixture(
            "+L",
            2,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets))];
            offset=Int64(1),
            children=[child],
        )
        push!(f.roots, offsets)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test collect(x) == [Int32[20, 30], Int32[40, 50, 60]]

        child = _primitive_fixture("i", Int32[10, 20, 30])
        offset_values = Int64[0, 1, 3]
        offset_nbytes = length(offset_values) * sizeof(Int64)
        offset_bytes = Vector{UInt8}(undef, offset_nbytes + 1)
        unsafe_copyto!(
            pointer(offset_bytes, 2),
            Ptr{UInt8}(pointer(offset_values)),
            offset_nbytes,
        )
        f = _cdata_fixture(
            "+L",
            2,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offset_bytes, 2))];
            children=[child],
        )
        push!(f.roots, offset_bytes)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        fill!(offset_bytes, 0x00)
        @test collect(x) == [Int32[10], Int32[20, 30]]

        child = _primitive_fixture("i", Int32[1, 2, 3, 4, 5])
        offsets = Int32[0, 2, 2, 5]
        validity = UInt8[0b00000101]
        f = _cdata_fixture(
            "+l",
            3,
            Ptr{Cvoid}[Ptr{Cvoid}(pointer(validity)), Ptr{Cvoid}(pointer(offsets))];
            flags=Arrow.ARROW_FLAG_NULLABLE,
            null_count=Int64(-1),
            children=[child],
        )
        append!(f.roots, Any[offsets, validity])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test isequal(
            collect(x),
            Union{Vector{Int32},Missing}[Int32[1, 2], missing, Int32[3, 4, 5]],
        )
    end

    @testset "fixed size list" begin
        child = _primitive_fixture("f", Float32[1, 2, 3, 4, 5, 6])
        f = _cdata_fixture("+w:3", 2, Ptr{Cvoid}[C_NULL]; children=[child])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=false)
        @test collect(x) == [(1.0f0, 2.0f0, 3.0f0), (4.0f0, 5.0f0, 6.0f0)]

        child = _primitive_fixture("i", Int32[1, 2, 3, 4, 5, 6, 7, 8, 9])
        f = _cdata_fixture("+w:3", 2, Ptr{Cvoid}[C_NULL]; offset=Int64(1), children=[child])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=false)
        @test collect(x) == [(Int32(4), Int32(5), Int32(6)), (Int32(7), Int32(8), Int32(9))]
    end

    @testset "struct root table with names and metadata" begin
        xchild =
            _primitive_fixture("i", Int32[1, 2, 3]; name="x", metadata=Dict("unit" => "id"))
        yoffsets = Int32[0, 1, 2, 3]
        ybytes = Vector{UInt8}(codeunits("abc"))
        ychild = _cdata_fixture(
            "u",
            3,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(yoffsets)), Ptr{Cvoid}(pointer(ybytes))];
            name="y",
        )
        append!(ychild.roots, Any[yoffsets, ybytes])
        root = _cdata_fixture(
            "+s",
            3,
            Ptr{Cvoid}[C_NULL];
            children=[xchild, ychild],
            metadata=Dict("source" => "cdata"),
        )
        tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        @test Tables.columnnames(tbl) == [:x, :y]
        @test Tables.schema(tbl).types == (Int32, String)
        @test length(tbl) == 2
        @test Tables.rowcount(tbl) == 3
        @test Tables.istable(typeof(tbl))
        @test Tables.columnaccess(typeof(tbl))
        @test Tables.columns(tbl) === tbl
        @test propertynames(tbl) == (:x, :y)
        @test :rowcount in propertynames(tbl, true)
        @test tbl.rowcount == 3
        @test collect(Tables.getcolumn(tbl, :x)) == Int32[1, 2, 3]
        @test collect(Tables.getcolumn(tbl, 1)) == Int32[1, 2, 3]
        @test collect(tbl[1]) == Int32[1, 2, 3]
        @test collect(tbl.y) == ["a", "b", "c"]
        @test copy(tbl) == (x=Int32[1, 2, 3], y=["a", "b", "c"])
        @test DataAPI.metadatasupport(typeof(tbl)) == (read=true, write=false)
        @test DataAPI.colmetadatasupport(typeof(tbl)) == (read=true, write=false)
        @test Dict(Arrow.getmetadata(tbl)) == Dict("source" => "cdata")
        @test DataAPI.metadata(tbl) == Dict("source" => "cdata")
        @test DataAPI.metadata(tbl, "source") == "cdata"
        @test DataAPI.metadata(tbl, "source", "fallback"; style=true) == ("cdata", :default)
        @test DataAPI.metadata(tbl, "missing", "fallback") == "fallback"
        @test Set(DataAPI.metadatakeys(tbl)) == Set(["source"])
        @test DataAPI.colmetadata(tbl, :x, "unit") == "id"
        @test DataAPI.colmetadata(tbl, :x, "unit", "fallback"; style=true) ==
              ("id", :default)
        @test DataAPI.colmetadata(tbl, :x, "missing", "fallback") == "fallback"
        @test Set(DataAPI.colmetadatakeys(tbl, :x)) == Set(["unit"])
        colkeys = collect(DataAPI.colmetadatakeys(tbl))
        @test length(colkeys) == 1
        @test first(colkeys[1]) == :x
        @test Set(last(colkeys[1])) == Set(["unit"])
        @test DataAPI.colmetadata(tbl) == Dict(:x => Dict("unit" => "id"))

        a = _primitive_fixture("i", Int32[1, 2, 3]; name="a")
        b = _primitive_fixture("i", Int32[4, 5, 6]; name="b")
        pair = _cdata_fixture("+s", 3, Ptr{Cvoid}[C_NULL]; name="pair", children=[a, b])
        root = _cdata_fixture("+s", 3, Ptr{Cvoid}[C_NULL]; children=[pair])
        pair_tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        @test collect(pair_tbl.pair) == [(a=1, b=4), (a=2, b=5), (a=3, b=6)]

        shortmeta_child =
            _primitive_fixture("i", Int32[1]; name="x", metadata=Dict("k" => "v"))
        shortmeta_root = _cdata_fixture(
            "+s",
            1,
            Ptr{Cvoid}[C_NULL];
            children=[shortmeta_child],
            metadata=Dict("x" => "yz"),
        )
        shortmeta_tbl =
            Arrow.from_c_data(_schema_ptr(shortmeta_root), _array_ptr(shortmeta_root))
        @test DataAPI.metadata(shortmeta_tbl, "x") == "yz"
        @test DataAPI.colmetadata(shortmeta_tbl, :x, "k") == "v"

        col = Tables.getcolumn(tbl, :x)
        GC.gc(true)
        @test collect(col) == Int32[1, 2, 3]
        # deepcopy detaches the table and its columns from the producer.
        @test deepcopy(tbl).x == Int32[1, 2, 3]
    end

    @testset "nested struct columns" begin
        child_validity = UInt8[0b00011101]
        xchild = _primitive_fixture(
            "i",
            Int32[10, 20, 30, 40, 50];
            validity=child_validity,
            null_count=Int64(1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
            name="x",
        )
        struct_validity = UInt8[0b00011011]
        nested = _cdata_fixture(
            "+s",
            5,
            Ptr{Cvoid}[Ptr{Cvoid}(pointer(struct_validity))];
            name="point",
            children=[xchild],
            metadata=Dict("shape" => "point"),
            null_count=Int64(1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
        )
        push!(nested.roots, struct_validity)
        T = NamedTuple{(:x,),Tuple{Union{Int32,Missing}}}

        parent_validity = UInt8[0b00001111]
        root = _cdata_fixture(
            "+s",
            5,
            Ptr{Cvoid}[Ptr{Cvoid}(pointer(parent_validity))];
            children=[nested],
            null_count=Int64(1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
        )
        push!(root.roots, parent_validity)
        tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        col = Tables.getcolumn(tbl, :point)
        @test eltype(col) == Union{T,Missing}
        @test Arrow.nullcount(col) == 2
        @test Dict(Arrow.getmetadata(col)) == Dict("shape" => "point")
        @test isequal(
            collect(col),
            Union{T,Missing}[(x=10,), (x=missing,), missing, (x=40,), missing],
        )

        root =
            _cdata_fixture("+s", 3, Ptr{Cvoid}[C_NULL]; offset=Int64(1), children=[nested])
        tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        col = Tables.getcolumn(tbl, :point)
        @test Tables.schema(tbl).types == (Union{T,Missing},)
        @test isequal(collect(col), Union{T,Missing}[(x=missing,), missing, (x=40,)])

        collected = collect(col)
        copied = copy(col)
        detached = deepcopy(col)
        Arrow.release_c_data(col)
        @test root.array[].release == C_NULL
        @test root.schema[].release == C_NULL
        @test nested.array[].release != C_NULL
        @test nested.schema[].release != C_NULL
        @test_throws ArgumentError col[1]
        @test isequal(collected, Union{T,Missing}[(x=missing,), missing, (x=40,)])
        @test isequal(copied, Union{T,Missing}[(x=missing,), missing, (x=40,)])
        # The deepcopied column is detached and outlives the release.
        @test isequal(collect(detached), collected)
    end

    @testset "struct table offsets and owner roots" begin
        child = _primitive_fixture("i", Int32[10, 20, 30]; name="x")
        root =
            _cdata_fixture("+s", 2, Ptr{Cvoid}[C_NULL]; offset=Int64(1), children=[child])
        tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        @test collect(Tables.getcolumn(tbl, :x)) == Int32[20, 30]

        col = let
            child2 = _primitive_fixture("i", Int32[10, 20, 30]; name="x")
            root2 = _cdata_fixture(
                "+s",
                2,
                Ptr{Cvoid}[C_NULL];
                offset=Int64(1),
                children=[child2],
            )
            tbl2 = Arrow.from_c_data(_schema_ptr(root2), _array_ptr(root2))
            Tables.getcolumn(tbl2, :x)
        end
        GC.gc(true)
        GC.gc(true)
        @test collect(col) == Int32[20, 30]
        Arrow.release_c_data(col)

        child = _primitive_fixture("i", Int32[10, 20, 30]; offset=Int64(1), len=2, name="x")
        root =
            _cdata_fixture("+s", 2, Ptr{Cvoid}[C_NULL]; offset=Int64(1), children=[child])
        @test_throws ArgumentError Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
    end

    @testset "struct parent validity masks children" begin
        child_validity = UInt8[0b00001011]
        parent_validity = UInt8[0b00001101]
        child = _primitive_fixture(
            "i",
            Int32[1, 2, 3, 4];
            validity=child_validity,
            null_count=Int64(1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
            name="x",
        )
        root = _cdata_fixture(
            "+s",
            4,
            Ptr{Cvoid}[Ptr{Cvoid}(pointer(parent_validity))];
            children=[child],
            null_count=Int64(1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
        )
        push!(root.roots, parent_validity)
        tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        col = Tables.getcolumn(tbl, :x)
        @test Tables.schema(tbl).types == (Union{Int32,Missing},)
        @test Arrow.nullcount(col) == 2
        @test isequal(collect(col), Union{Int32,Missing}[1, missing, missing, 4])
        @test isequal(copy(tbl), (x=Union{Int32,Missing}[1, missing, missing, 4],))

        parent_validity = UInt8[0b00000010]
        child = _primitive_fixture("i", Int32[10, 20, 30]; name="x")
        root = _cdata_fixture(
            "+s",
            2,
            Ptr{Cvoid}[Ptr{Cvoid}(pointer(parent_validity))];
            offset=Int64(1),
            children=[child],
            null_count=Int64(1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
        )
        push!(root.roots, parent_validity)
        tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        @test isequal(collect(Tables.getcolumn(tbl, :x)), Union{Int32,Missing}[20, missing])
    end

    @testset "temporal and decimal formats" begin
        dates = Arrow.DATE[Arrow.DATE(1), Arrow.DATE(2)]
        f = _primitive_fixture("tdD", dates)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=false)
        @test collect(x) == dates
        f = _primitive_fixture("tdD", dates)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=true)
        @test collect(x) == convert.(Dates.Date, dates)

        D = Arrow.Decimal{10,2,Int128}
        decimals = D[D(123), D(-45)]
        f = _primitive_fixture("d:10,2", decimals)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=false)
        @test collect(x) == decimals

        timestamps = Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,nothing}[
            Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,nothing}(0),
            Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,nothing}(1),
        ]
        f = _primitive_fixture("tsm:", timestamps)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=true)
        @test collect(x) ==
              Dates.DateTime[Dates.DateTime(1970), Dates.DateTime(1970, 1, 1, 0, 0, 0, 1)]

        durations = Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}[
            Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}(1),
            Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}(2),
        ]
        f = _primitive_fixture("tDs", durations)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=true)
        @test collect(x) == Dates.Second[Dates.Second(1), Dates.Second(2)]

        intervals = Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH,Int32}[
            Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH}(12),
            Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH}(18),
        ]
        f = _primitive_fixture("tiM", intervals)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f); convert=false)
        @test collect(x) == intervals
    end

    @testset "release behavior" begin
        f = _primitive_fixture("i", Int32[1, 2, 3])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        owner = Arrow._owner(x)
        @test x[1] == 1
        Arrow.release_c_data(x)
        @test owner.array[].release == C_NULL
        @test owner.schema[].release == C_NULL
        @test_nowarn Arrow.release_c_data(x)
        @test_throws ArgumentError x[1]

        f = _primitive_fixture("i", Int32[1, 2, 3])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        owner = Arrow._owner(x)
        owner_lock = getfield(owner, :lock)
        started = Channel{Nothing}(1)
        locked = false
        lock(owner_lock)
        locked = true
        try
            task = Threads.@spawn begin
                put!(started, nothing)
                Arrow.release_c_data(x)
            end
            take!(started)
            sleep(0.05)
            @test owner.array[].release != C_NULL
            unlock(owner_lock)
            locked = false
            wait(task)
        finally
            locked && unlock(owner_lock)
        end
        @test owner.array[].release == C_NULL
        @test owner.schema[].release == C_NULL

        f = _primitive_fixture("i", Int32[1, 2, 3])
        _set_array!(f; release=_CDATA_RELEASE_ARRAY_REENTRANT)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        owner = Arrow._owner(x)
        _CDATA_REENTRANT_OBJECT[] = x
        _CDATA_REENTRANT_RELEASES[] = 0
        try
            @test_nowarn Arrow.release_c_data(x)
            @test _CDATA_REENTRANT_RELEASES[] == 1
            @test owner.array[].release == C_NULL
            @test owner.schema[].release == C_NULL
            @test_throws ArgumentError x[1]
        finally
            _CDATA_REENTRANT_OBJECT[] = nothing
        end
    end

    @testset "import moves the base structures" begin
        f = _primitive_fixture("i", Int32[1, 2, 3])
        _set_array!(f; release=_CDATA_RELEASE_ARRAY_REENTRANT)
        _CDATA_REENTRANT_OBJECT[] = nothing
        _CDATA_REENTRANT_RELEASES[] = 0
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        # The sources are marked released without calling their callbacks.
        @test f.schema[].release == C_NULL
        @test f.array[].release == C_NULL
        @test _CDATA_REENTRANT_RELEASES[] == 0
        # The caller may reuse the source structures immediately after import.
        f.schema[] = Arrow.ArrowSchema(
            Cstring(C_NULL),
            Cstring(C_NULL),
            Cstring(C_NULL),
            -1,
            -1,
            C_NULL,
            C_NULL,
            C_NULL,
            C_NULL,
        )
        f.array[] =
            Arrow.ArrowArray(-1, -1, -1, -1, -1, C_NULL, C_NULL, C_NULL, C_NULL, C_NULL)
        @test x == Int32[1, 2, 3]
        Arrow.release_c_data(x)
        @test _CDATA_REENTRANT_RELEASES[] == 1
        @test_nowarn Arrow.release_c_data(x)
        @test _CDATA_REENTRANT_RELEASES[] == 1
        @test_throws ArgumentError x[1]

        # A moved-from source cannot be imported again.
        f = _primitive_fixture("i", Int32[1, 2])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test_throws ArgumentError Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        Arrow.release_c_data(x)

        @test_throws ArgumentError Arrow.from_c_data(
            Ptr{Arrow.ArrowSchema}(C_NULL),
            Ptr{Arrow.ArrowArray}(C_NULL),
        )
    end

    @testset "table release behavior" begin
        child = _primitive_fixture("i", Int32[1, 2, 3]; name="x")
        root = _cdata_fixture("+s", 3, Ptr{Cvoid}[C_NULL]; children=[child])
        tbl = Arrow.from_c_data(_schema_ptr(root), _array_ptr(root))
        col = Tables.getcolumn(tbl, :x)
        @test col[1] == 1
        Arrow.release_c_data(tbl)
        @test_nowarn Arrow.release_c_data(tbl)
        @test_throws ArgumentError Tables.getcolumn(tbl, :x)
        @test_throws ArgumentError col[1]
    end

    @testset "copy and collect own the result" begin
        f = _primitive_fixture("i", Int32[1, 2, 3])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        a = collect(x)
        b = copy(x)
        Arrow.release_c_data(x)
        @test a == Int32[1, 2, 3]
        @test b == Int32[1, 2, 3]
    end

    @testset "null count, flag, and owned copy semantics" begin
        # A declared null count is trusted without scanning the bitmap.
        f = _primitive_fixture(
            "i",
            Int32[1, 2, 3];
            validity=UInt8[0b00000101],
            null_count=Int64(0),
            flags=Arrow.ARROW_FLAG_NULLABLE,
        )
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test Arrow.nullcount(x) == 0
        @test isequal(collect(x), Union{Int32,Missing}[1, 2, 3])
        # An unknown null count with an offset bitmap resolves by popcount
        # across byte boundaries: physical bits 5, 8, and 11 are clear, so
        # logical elements 3, 6, and 9 are null.
        f = _primitive_fixture(
            "i",
            Int32.(1:13);
            validity=UInt8[0b11011111, 0b11110110],
            null_count=Int64(-1),
            flags=Arrow.ARROW_FLAG_NULLABLE,
            offset=Int64(3),
        )
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test Arrow.nullcount(x) == 3
        @test isequal(
            collect(x),
            Union{Int32,Missing}[4, 5, missing, 7, 8, missing, 10, 11, missing, 13],
        )
        # Reserved flag bits are ignored for forward compatibility.
        f = _primitive_fixture("i", Int32[1, 2]; flags=Int64(8) | Arrow.ARROW_FLAG_NULLABLE)
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        @test eltype(x) == Union{Int32,Missing}
        @test isequal(collect(x), [1, 2])
        # deepcopy and serialize materialize owned copies through the liveness gate.
        f = _primitive_fixture("i", Int32[1, 2, 3])
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        dc = deepcopy(x)
        io = IOBuffer()
        serialize(io, x)
        seekstart(io)
        sc = deserialize(io)
        Arrow.release_c_data(x)
        @test dc == Int32[1, 2, 3]
        @test sc == Int32[1, 2, 3]
        @test_throws ArgumentError deepcopy(x)
        @test_throws ArgumentError serialize(IOBuffer(), x)
        # Finalization releases the import exactly once.
        f = _primitive_fixture("i", Int32[1, 2, 3])
        _set_array!(f; release=_CDATA_RELEASE_ARRAY_REENTRANT)
        _CDATA_REENTRANT_OBJECT[] = nothing
        _CDATA_REENTRANT_RELEASES[] = 0
        @test _cdata_import_collect(f) == Int32[1, 2, 3]
        GC.gc()
        GC.gc()
        @test _CDATA_REENTRANT_RELEASES[] == 1
    end

    @testset "malformed inputs" begin
        bad(f) = @test_throws ArgumentError Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        bad_primitive(mutator) = begin
            f = _primitive_fixture("i", Int32[1])
            mutator(f)
            bad(f)
        end

        f = _primitive_fixture("i", Int32[1])
        @test_throws ArgumentError Arrow.from_c_data(
            Ptr{Arrow.ArrowSchema}(C_NULL),
            _array_ptr(f),
        )
        @test_throws ArgumentError Arrow.from_c_data(
            _schema_ptr(f),
            Ptr{Arrow.ArrowArray}(C_NULL),
        )

        f = _cdata_fixture("?", 0, Ptr{Cvoid}[])
        bad(f)
        @test f.array[].release == C_NULL
        @test f.schema[].release == C_NULL

        for mutator in (
            f -> _set_schema!(f; release=C_NULL),
            f -> _set_array!(f; release=C_NULL),
            f -> _set_array!(f; length=Int64(-1)),
            f -> _set_array!(f; offset=Int64(-1)),
            f -> _set_array!(f; length=typemax(Int64), offset=Int64(1)),
            f -> _set_array!(f; n_buffers=Int64(-1)),
            f -> _set_array!(f; n_children=Int64(-1)),
            f -> _set_schema!(f; n_children=Int64(-1)),
            f -> _set_array!(f; n_children=Int64(1)),
            f -> _set_schema!(f; n_children=Int64(1)),
        )
            bad_primitive(mutator)
        end

        for null_count in (Int64(2), Int64(-1), Int64(1))
            bad(
                _primitive_fixture(
                    "i",
                    Int32[1];
                    null_count=null_count,
                    flags=Arrow.ARROW_FLAG_NULLABLE,
                ),
            )
        end

        f = _primitive_fixture("i", Int32[1])
        _set_array!(f; buffers=Ptr{Ptr{Cvoid}}(C_NULL))
        bad(f)

        data = Int32[1]
        f = _cdata_fixture("i", 1, Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(data)), C_NULL])
        push!(f.roots, data)
        bad(f)

        for f in (
            _cdata_fixture("n", 0, Ptr{Cvoid}[C_NULL]),
            _cdata_fixture("i", 1, Ptr{Cvoid}[C_NULL, C_NULL]),
            _cdata_fixture("i", 1, Ptr{Cvoid}[C_NULL]),
        )
            bad(f)
        end

        child = _primitive_fixture("i", Int32[1])
        f = _cdata_fixture("+l", 1, Ptr{Cvoid}[C_NULL, C_NULL]; children=[child])
        bad(f)

        child = _primitive_fixture("i", Int32[1, 2])
        offsets = Int32[0, 3]
        f = _cdata_fixture(
            "+l",
            1,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets))];
            children=[child],
        )
        push!(f.roots, offsets)
        bad(f)

        child = _primitive_fixture("i", Int32[1, 2, 3, 4, 5])
        f = _cdata_fixture("+w:3", 2, Ptr{Cvoid}[C_NULL]; children=[child])
        bad(f)

        offsets = Int32[0, 3, 2]
        bytes = UInt8[1, 2, 3]
        f = _cdata_fixture(
            "z",
            2,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), Ptr{Cvoid}(pointer(bytes))],
        )
        append!(f.roots, Any[offsets, bytes])
        bad(f)

        offsets = Int32[0, -1]
        bytes = UInt8[]
        f = _cdata_fixture(
            "z",
            1,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), Ptr{Cvoid}(C_NULL)],
        )
        append!(f.roots, Any[offsets, bytes])
        bad(f)

        offsets = Int32[0, 1]
        bytes = UInt8[0x01]
        f = _cdata_fixture(
            "u",
            1,
            Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets)), Ptr{Cvoid}(pointer(bytes))];
            flags=Arrow.ARROW_FLAG_NULLABLE,
            null_count=Int64(-1),
        )
        append!(f.roots, Any[offsets, bytes])
        bad(f)

        badmeta = reinterpret(UInt8, Int32[1, -1])
        f = _primitive_fixture("i", Int32[1])
        f.schema[] = Arrow.ArrowSchema(
            f.schema[].format,
            f.schema[].name,
            Cstring(pointer(badmeta)),
            f.schema[].flags,
            f.schema[].n_children,
            f.schema[].children,
            f.schema[].dictionary,
            f.schema[].release,
            f.schema[].private_data,
        )
        push!(f.roots, badmeta)
        bad(f)

        for flags in (Arrow.ARROW_FLAG_DICTIONARY_ORDERED, Arrow.ARROW_FLAG_MAP_KEYS_SORTED)
            bad_primitive(f -> _set_schema!(f; flags=flags))
        end

        # A zero length array with a positive offset still describes
        # offset * sizeof(T) data bytes, like arrow-rs and nanoarrow.
        bad(_cdata_fixture("i", 0, Ptr{Cvoid}[C_NULL, C_NULL]; offset=Int64(1)))

        for words in (Int32[1, -1], Int32[1, 0, -1])
            bytes = reinterpret(UInt8, words)
            f = _primitive_fixture("i", Int32[1])
            _set_schema!(f; metadata=Cstring(pointer(bytes)))
            append!(f.roots, Any[words, bytes])
            bad(f)
        end

        function bad_child(schema_ptr, array_ptr, child)
            schema_children = Ptr{Arrow.ArrowSchema}[schema_ptr]
            array_children = Ptr{Arrow.ArrowArray}[array_ptr]
            root = _cdata_fixture("+s", 1, Ptr{Cvoid}[C_NULL])
            _set_schema!(
                root;
                n_children=1,
                children=Ptr{Ptr{Arrow.ArrowSchema}}(pointer(schema_children)),
            )
            _set_array!(
                root;
                n_children=1,
                children=Ptr{Ptr{Arrow.ArrowArray}}(pointer(array_children)),
            )
            append!(root.roots, Any[child, schema_children, array_children])
            bad(root)
        end

        child = _primitive_fixture("i", Int32[1])
        bad_child(Ptr{Arrow.ArrowSchema}(C_NULL), _array_ptr(child), child)
        bad_child(_schema_ptr(child), Ptr{Arrow.ArrowArray}(C_NULL), child)

        child = _primitive_fixture("i", Int32[1])
        _set_schema!(child; release=C_NULL)
        bad_child(_schema_ptr(child), _array_ptr(child), child)

        child = _primitive_fixture("i", Int32[1])
        _set_array!(child; release=C_NULL)
        bad_child(_schema_ptr(child), _array_ptr(child), child)

        child = _primitive_fixture("i", Int32[1])
        f = _primitive_fixture("i", Int32[1])
        schema_children = Ptr{Arrow.ArrowSchema}[_schema_ptr(child)]
        array_children = Ptr{Arrow.ArrowArray}[_array_ptr(child)]
        _set_schema!(
            f;
            n_children=1,
            children=Ptr{Ptr{Arrow.ArrowSchema}}(pointer(schema_children)),
        )
        _set_array!(
            f;
            n_children=1,
            children=Ptr{Ptr{Arrow.ArrowArray}}(pointer(array_children)),
        )
        append!(f.roots, Any[child, schema_children, array_children])
        bad(f)

        # Duplicate struct field names cannot build a table or NamedTuple rows.
        a = _primitive_fixture("i", Int32[1]; name="dup")
        b = _primitive_fixture("i", Int32[1]; name="dup")
        bad(_cdata_fixture("+s", 1, Ptr{Cvoid}[C_NULL]; children=[a, b]))

        # Aliased child pointers cannot make validation explode combinatorially.
        c = _cdata_fixture("+s", 1, Ptr{Cvoid}[C_NULL])
        selfs = Ptr{Arrow.ArrowSchema}[_schema_ptr(c), _schema_ptr(c)]
        selfa = Ptr{Arrow.ArrowArray}[_array_ptr(c), _array_ptr(c)]
        _set_schema!(c; n_children=2, children=Ptr{Ptr{Arrow.ArrowSchema}}(pointer(selfs)))
        _set_array!(c; n_children=2, children=Ptr{Ptr{Arrow.ArrowArray}}(pointer(selfa)))
        append!(c.roots, Any[selfs, selfa])
        bad(_cdata_fixture("+s", 1, Ptr{Cvoid}[C_NULL]; children=[c]))

        f = _primitive_fixture("i", Int32[1])
        dict_schema = Ref(f.schema[])
        _set_schema!(f; dictionary=Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, dict_schema))
        push!(f.roots, dict_schema)
        bad(f)

        f = _primitive_fixture("i", Int32[1])
        dict_array = Ref(f.array[])
        _set_array!(f; dictionary=Base.unsafe_convert(Ptr{Arrow.ArrowArray}, dict_array))
        push!(f.roots, dict_array)
        bad(f)

        f = _primitive_fixture("i", Int32[1])
        dict_schema = Ref(f.schema[])
        dict_array = Ref(f.array[])
        _set_schema!(f; dictionary=Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, dict_schema))
        _set_array!(f; dictionary=Base.unsafe_convert(Ptr{Arrow.ArrowArray}, dict_array))
        append!(f.roots, Any[dict_schema, dict_array])
        bad(f)

        f = _primitive_fixture("i", Int32[1])
        fmt = fill(UInt8('i'), Arrow._CDATA_MAX_FORMAT_BYTES + 1)
        _set_schema!(f; format=Cstring(pointer(fmt)))
        push!(f.roots, fmt)
        bad(f)

        for fmt in ("tsq:", "d:x,2", "d:10,2,64", "w:0", "+w:x")
            f = _cdata_fixture(fmt, 0, Ptr{Cvoid}[C_NULL, C_NULL])
            @test_throws ArgumentError Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        end

        f = _cdata_fixture(
            "w:$(Arrow._CDATA_MAX_FIXED_SIZE + 1)",
            0,
            Ptr{Cvoid}[C_NULL, C_NULL],
        )
        @test_throws ArgumentError Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))

        child = _primitive_fixture("i", Int32[])
        for _ = 1:Arrow._CDATA_MAX_DEPTH
            child = _cdata_fixture("+s", 0, Ptr{Cvoid}[C_NULL]; children=[child])
        end
        bad(child)

        child = _primitive_fixture("i", Int32[])
        for _ = 1:Arrow._CDATA_MAX_DEPTH
            offsets = Int32[0]
            child = _cdata_fixture(
                "+l",
                0,
                Ptr{Cvoid}[C_NULL, Ptr{Cvoid}(pointer(offsets))];
                children=[child],
            )
            push!(child.roots, offsets)
        end
        bad(child)
    end
end
