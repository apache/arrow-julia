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

@testset "C Data review regressions" begin
    @testset "host C ABI" begin
        cc = Sys.which("cc")
        if cc === nothing || Sys.iswindows()
            @test_skip "host C compiler unavailable"
        else
            mktempdir() do dir
                exe = joinpath(dir, "cdata_abi")
                src = joinpath(@__DIR__, "cdata_abi.c")
                flags = Sys.ARCH === :i686 ? ["-m32"] : String[]
                run(`$cc $flags -std=c11 -o $exe $src`)
                layout = Dict{String,Int}()
                for line in split(chomp(read(`$exe`, String)), '\n')
                    key, value = split(line, '=')
                    layout[key] = parse(Int, value)
                end
                @test layout["pointer.size"] == sizeof(Ptr{Cvoid})
                @test layout["int64.alignment"] == Base.datatype_alignment(Int64)
                for T in (Arrow.ArrowSchema, Arrow.ArrowArray)
                    name = string(nameof(T))
                    @test layout["$name.size"] == sizeof(T)
                    @test layout["$name.alignment"] == Base.datatype_alignment(T)
                    for (i, field) in enumerate(fieldnames(T))
                        @test layout["$name.$field"] == fieldoffset(T, i)
                    end
                end
            end
        end
    end

    @testset "format scan boundary" begin
        limit = Arrow._CDATA_MAX_FORMAT_BYTES
        bytes = fill(UInt8('i'), limit + 1)
        bytes[limit] = 0x00
        GC.@preserve bytes begin
            ptr = Cstring(pointer(bytes))
            @test ncodeunits(Arrow._unsafe_string_bounded(ptr, limit, "format")) == limit - 1
            bytes[limit] = UInt8('i')
            bytes[limit + 1] = 0x00
            @test_throws "no NUL terminator within the $limit byte import limit" Arrow._unsafe_string_bounded(
                ptr,
                limit,
                "format",
            )
        end
    end

    @testset "unknown null counts at every bit alignment" begin
        bytes = UInt8[0x00, 0xff, 0xa5, 0x3c, 0x81, 0x7e]
        for offset = 0:15, len = 0:32
            # Count individual bits as an independent oracle for the byte masks.
            expected = count(offset:(offset + len - 1)) do bit
                (bytes[div(bit, 8) + 1] >> rem(bit, 8)) & 0x01 == 0
            end
            @test Arrow._count_nulls(bytes, offset, len) == expected
        end
    end

    @testset "release clearing preserves other fields" begin
        f = _primitive_fixture("i", Int32[1, 2, 3]; offset=Int64(1))
        for (ref, ptr, clear!) in (
            (f.schema, _schema_ptr(f), Arrow._clear_schema_release!),
            (f.array, _array_ptr(f), Arrow._clear_array_release!),
        )
            before = ref[]
            clear!(ptr)
            @test ref[].release == C_NULL
            for field in fieldnames(typeof(before))
                field === :release && continue
                @test getfield(ref[], field) == getfield(before, field)
            end
            @test_nowarn clear!(ptr)
        end
    end

    @testset "finalizer retry preserves ownership" begin
        f = _primitive_fixture("i", Int32[1, 2, 3])
        _set_array!(f; release=_CDATA_RELEASE_ARRAY_REENTRANT)
        _CDATA_REENTRANT_OBJECT[] = nothing
        _CDATA_REENTRANT_RELEASES[] = 0
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        owner = Arrow._owner(x)
        started = Channel{Nothing}(1)
        resume = Channel{Nothing}(1)
        holder = @async begin
            lock(owner.lock)
            try
                put!(started, nothing)
                take!(resume)
            finally
                unlock(owner.lock)
            end
        end
        take!(started)
        try
            # ReentrantLock is task-owned, so even one thread exercises contention.
            finalize(owner)
            @test !owner.released
            @test _CDATA_REENTRANT_RELEASES[] == 0
        finally
            put!(resume, nothing)
            wait(holder)
        end
        @test x[1] == 1
        finalize(owner)
        @test owner.released
        @test owner.schema[].release == C_NULL
        @test _CDATA_REENTRANT_RELEASES[] == 1
        finalize(owner)
        @test _CDATA_REENTRANT_RELEASES[] == 1
        @test_throws ArgumentError x[1]
    end

    @testset "deepcopy detaches buffers and release callbacks" begin
        data = Int32[1, 2, 3]
        validity = UInt8[0x05]
        f = _primitive_fixture("i", data; validity=validity, null_count=Int64(1))
        x = Arrow.from_c_data(_schema_ptr(f), _array_ptr(f))
        detached = deepcopy(x)
        owner = Arrow._owner(detached)
        @test owner !== Arrow._owner(x)
        @test owner.schema[].release == C_NULL
        @test owner.array[].release == C_NULL
        data[1] = 99
        validity[1] = 0x06
        @test x[1] === missing
        Arrow.release_c_data(x)
        @test isequal(collect(detached), [1, missing, 3])
        @test_throws ArgumentError copy(x)
        @test_throws ArgumentError collect(x)
        @test_throws ArgumentError deepcopy(x)
        @test_throws ArgumentError serialize(IOBuffer(), x)
        @test_nowarn Arrow.release_c_data(detached)
    end

    @testset "validation diagnostics" begin
        f = _primitive_fixture("i", Int32[1])
        _set_array!(f; n_buffers=Int64(1))
        @test_throws "ArrowArray.n_buffers is 1; expected 2 for the format" Arrow.from_c_data(
            _schema_ptr(f),
            _array_ptr(f),
        )
        f = _primitive_fixture("i", Int32[1])
        _set_schema!(f; n_children=Int64(2))
        @test_throws "ArrowArray.n_children is 0; expected 2 from ArrowSchema.n_children" Arrow.from_c_data(
            _schema_ptr(f),
            _array_ptr(f),
        )
        for schema_dictionary in (false, true)
            f = _primitive_fixture("i", Int32[1])
            dictionary = _primitive_fixture("i", Int32[1])
            if schema_dictionary
                _set_schema!(f; dictionary=_schema_ptr(dictionary))
            else
                _set_array!(f; dictionary=_array_ptr(dictionary))
            end
            @test_throws "must both be NULL or both be non-NULL" Arrow.from_c_data(
                _schema_ptr(f),
                _array_ptr(f),
            )
        end
    end
end
