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

function _cdata_fixture(
    fmt::String,
    len::Integer,
    buffers::Vector{Ptr{Cvoid}};
    flags::Int64=0,
    null_count::Int64=0,
    offset::Int64=0,
)
    roots = Any[]
    buffer_ptrs = copy(buffers)
    if !isempty(buffer_ptrs)
        push!(roots, buffer_ptrs)
    end
    schema = Ref(
        Arrow.ArrowSchema(
            _cstring_root(fmt, roots),
            Cstring(C_NULL),
            Cstring(C_NULL),
            flags,
            Int64(0),
            Ptr{Ptr{Arrow.ArrowSchema}}(C_NULL),
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
            Int64(0),
            isempty(buffer_ptrs) ? Ptr{Ptr{Cvoid}}(C_NULL) :
            Ptr{Ptr{Cvoid}}(pointer(buffer_ptrs)),
            Ptr{Ptr{Arrow.ArrowArray}}(C_NULL),
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
) where {T}
    roots = Any[data]
    buffers = Ptr{Cvoid}[
        validity === nothing ? Ptr{Cvoid}(C_NULL) : Ptr{Cvoid}(pointer(validity)),
        isempty(data) ? Ptr{Cvoid}(C_NULL) : Ptr{Cvoid}(pointer(data)),
    ]
    validity !== nothing && push!(roots, validity)
    fixture =
        _cdata_fixture(fmt, len, buffers; flags=flags, null_count=null_count, offset=offset)
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

        for flags in (Arrow.ARROW_FLAG_DICTIONARY_ORDERED, Arrow.ARROW_FLAG_MAP_KEYS_SORTED)
            bad_primitive(f -> _set_schema!(f; flags=flags))
        end

        # A zero length array with a positive offset still describes
        # offset * sizeof(T) data bytes, like arrow-rs and nanoarrow.
        bad(_cdata_fixture("i", 0, Ptr{Cvoid}[C_NULL, C_NULL]; offset=Int64(1)))

        # A primitive format must reject children.
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
    end
end
