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

# The incremental writer (Arrow.Writer) and stream append (Arrow.append):
# byte parity with the eager writer, fixed-schema conformance, dictionary
# rules per format, close semantics, and appends to 2.x-written streams.

module IncrementalWriterTests

using Test
using Dates
using Tables
using Arrow

const AC = Arrow.ArrowCore

@testset "incremental writer and append" begin
    t1 = (; a=Int64[1, 2], b=["x", "y"])
    t2 = (; a=Int64[3, 4], b=["z", "w"])

    @testset "multi-write output is byte-identical to the eager writer" begin
        for file in (true, false), compress in (nothing, :zstd)
            io = IOBuffer()
            w = Arrow.Writer(io; file=file, compress=compress)
            Arrow.write(w, t1)
            Arrow.write(w, t2)
            close(w)
            incremental = take!(io)
            eager = Arrow._writebytes(
                Tables.partitioner([t1, t2]);
                file=file,
                compress=compress,
            )
            @test incremental == eager
            t = Arrow.Table(incremental)
            @test t.a == [1, 2, 3, 4]
            @test t.b == ["x", "y", "z", "w"]
        end
    end

    @testset "path sink, function form, and open" begin
        path = joinpath(mktempdir(), "w.arrow")
        Arrow.Writer(path; compress=:lz4) do w
            Arrow.write(w, t1)
            Arrow.write(w, t2)
        end
        @test Arrow.Table(path).a == [1, 2, 3, 4]
        w = open(Arrow.Writer, joinpath(mktempdir(), "w2.arrow"); file=false)
        @test w isa Arrow.Writer
        Arrow.write(w, t1)
        close(w)
        @test isopen(w) == false
    end

    @testset "Stream sees one partition per write" begin
        io = IOBuffer()
        w = Arrow.Writer(io; file=false)
        Arrow.write(w, t1)
        Arrow.write(w, t2)
        close(w)
        parts = collect(Arrow.Stream(take!(io)))
        @test length(parts) == 2
        @test parts[2].a == [3, 4]
    end

    @testset "writer metadata and colmetadata" begin
        io = IOBuffer()
        w = Arrow.Writer(io; metadata=("k" => "v",), colmetadata=Dict(:a => ("c" => "d",)))
        Arrow.write(w, t1)
        close(w)
        t = Arrow.Table(take!(io))
        @test Arrow.getmetadata(t) == Dict("k" => "v")
        @test collect(Arrow.DataAPI.colmetadatakeys(t, :a)) == ["c"]
    end

    @testset "the first write fixes the schema" begin
        io = IOBuffer()
        w = Arrow.Writer(io)
        Arrow.write(w, t1)
        err = @test_throws ArgumentError Arrow.write(w, (; a=[1], c=["q"]))
        @test occursin("do not match", err.value.msg)
        # Type drift and missing under a non-nullable field are refused.
        @test_throws Exception Arrow.write(w, (; a=[1.5], b=["q"]))
        @test_throws Exception Arrow.write(w, (; a=[missing, 1], b=["q", "r"]))
        # The writer stays usable after a refused write.
        Arrow.write(w, t2)
        close(w)
        @test Arrow.Table(take!(io)).a == [1, 2, 3, 4]
    end

    @testset "a zero-row typed first write pins the schema" begin
        io = IOBuffer()
        w = Arrow.Writer(io; file=false)
        Arrow.write(w, (; a=Union{Missing,Int64}[], b=String[]))
        Arrow.write(w, (; a=[missing, 2], b=["x", "y"]))
        close(w)
        t = Arrow.Table(take!(io))
        @test eltype(t.a) == Union{Missing,Int64}
        @test isequal(t.a, [missing, 2])
    end

    @testset "stream format replaces dictionary pools across writes" begin
        # Replacement must be opted into: the declaration is a demand on
        # readers, so the default writer refuses a changed pool instead.
        io = IOBuffer()
        w = Arrow.Writer(io; file=false)
        Arrow.write(w, (; d=Arrow.DictEncode(["a", "b", "a"])))
        err = @test_throws Arrow.ValidationError Arrow.write(
            w,
            (; d=Arrow.DictEncode(["c", "c", "b"])),
        )
        @test occursin("DictionaryReplacement", err.value.msg)
        Arrow.write(w, (; d=Arrow.DictEncode(["a", "b", "b"])))   # same pool reuses
        close(w)
        @test Arrow.Table(take!(io)).d == ["a", "b", "a", "a", "b", "b"]

        io = IOBuffer()
        w = Arrow.Writer(io; file=false, dictreplacement=true)
        Arrow.write(w, (; d=Arrow.DictEncode(["a", "b", "a"])))
        Arrow.write(w, (; d=Arrow.DictEncode(["c", "c", "b"])))
        close(w)
        t = Arrow.Table(take!(io))
        @test t.d == ["a", "b", "a", "c", "c", "b"]
        # The file format has no replacement to declare.
        @test_throws ArgumentError Arrow.Writer(IOBuffer(); dictreplacement=true)
    end

    @testset "file format reuses an identical pool and refuses a change" begin
        io = IOBuffer()
        w = Arrow.Writer(io; file=true)
        Arrow.write(w, (; d=Arrow.DictEncode(["a", "b", "a"])))
        Arrow.write(w, (; d=Arrow.DictEncode(["a", "b", "b"])))
        err = @test_throws Arrow.ValidationError Arrow.write(
            w,
            (; d=Arrow.DictEncode(["b", "a"])),
        )
        @test occursin("one dictionary batch per", err.value.msg)
        close(w)
        t = Arrow.Table(take!(io))
        @test t.d == ["a", "b", "a", "a", "b", "b"]
    end

    @testset "close is idempotent; a closed writer refuses writes" begin
        io = IOBuffer()
        w = Arrow.Writer(io)
        Arrow.write(w, t1)
        close(w)
        close(w)
        err = @test_throws ArgumentError Arrow.write(w, t2)
        @test occursin("closed", err.value.msg)
        # Closing a writer that never wrote produces no IPC output but does
        # not throw (finally-friendly).
        io2 = IOBuffer()
        close(Arrow.Writer(io2))
        @test isempty(take!(io2))
    end

    @testset "close finalizes what was published" begin
        # A refused write publishes nothing; close still emits a valid
        # file containing every accepted batch.
        io = IOBuffer()
        w = Arrow.Writer(io; file=true)
        Arrow.write(w, t1)
        @test_throws Exception Arrow.write(w, (; a=[1.5], b=["q"]))
        close(w)
        t = Arrow.Table(take!(io))
        @test t.a == [1, 2]
    end

    @testset "append to a stream path and io" begin
        path = joinpath(mktempdir(), "s.arrow")
        Arrow.write(path, t1; file=false)
        @test Arrow.append(path, t2) == path
        t = Arrow.Table(path)
        @test t.a == [1, 2, 3, 4]
        @test t.b == ["x", "y", "z", "w"]
        Arrow.append(path, (; a=Int64[5], b=["v"]))
        @test Arrow.Table(path).a == [1, 2, 3, 4, 5]

        io = IOBuffer()
        Arrow.write(io, t1; file=false)
        Arrow.append(io, t2)
        @test Arrow.Table(take!(io)).a == [1, 2, 3, 4]
    end

    @testset "append with dictionary columns" begin
        # An eager-written stream declares no replacement: a content-equal
        # pool appends by reuse, and a changed pool is refused.
        path = joinpath(mktempdir(), "d.arrow")
        Arrow.write(path, (; d=Arrow.DictEncode(["a", "b", "a"])); file=false)
        Arrow.append(path, (; d=Arrow.DictEncode(["a", "b", "b"])))
        @test Arrow.Table(path).d == ["a", "b", "a", "a", "b", "b"]
        err = @test_throws Arrow.ValidationError Arrow.append(
            path,
            (; d=Arrow.DictEncode(["z", "z", "q"])),
        )
        @test occursin("DictionaryReplacement", err.value.msg)

        # A Writer stream that opted into replacement accepts replacing
        # appends.
        rpath = joinpath(mktempdir(), "r.arrow")
        Arrow.Writer(rpath; file=false, dictreplacement=true) do w
            Arrow.write(w, (; d=Arrow.DictEncode(["a", "b", "a"])))
        end
        Arrow.append(rpath, (; d=Arrow.DictEncode(["b", "a"])))
        Arrow.append(rpath, (; d=Arrow.DictEncode(["z", "z", "q"])))
        @test Arrow.Table(rpath).d == ["a", "b", "a", "b", "a", "z", "z", "q"]
    end

    @testset "append refusals" begin
        path = joinpath(mktempdir(), "f.arrow")
        Arrow.write(path, t1; file=true)
        err = @test_throws ArgumentError Arrow.append(path, t2)
        @test occursin("file format", err.value.msg)

        spath = joinpath(mktempdir(), "s.arrow")
        Arrow.write(spath, t1; file=false)
        err = @test_throws ArgumentError Arrow.append(spath, (; wrong=[1]))
        @test occursin("do not match", err.value.msg)
        # A corrupt prefix refuses instead of gaining valid-looking bytes.
        bytes = read(spath)
        bytes[9] ⊻= 0x40
        write(spath, bytes)
        @test_throws Exception Arrow.append(spath, t2)
    end

    @testset "append to an Arrow 2.x stream" begin
        fixture = read(joinpath(@__DIR__, "fixtures2x", "mixed-two-partitions.arrowbytes"))
        path = joinpath(mktempdir(), "mixed.arrow")
        write(path, fixture)
        before = Arrow.Table(read(path))
        tail = (;
            ints=Int64[6, 7],
            floats=[6.5, missing],
            bools=[false, true],
            strs=["tail", missing],
            lists=[[7, 8], missing],
            structs=[
                Pair{String,Any}["a" => 6, "b" => "u"],
                Pair{String,Any}["a" => 7, "b" => "t"],
            ],
            dict=Arrow.DictEncode(["lo", "hi"]),
        )
        # The 2.x pool carries a null slot (2.x encoded missing as a pool
        # entry), which a 3.0 pool never reproduces — so the append needs
        # replacement, which the 2.x schema message does not declare. The
        # refusal must leave the file untouched.
        err = @test_throws Arrow.ValidationError Arrow.append(path, tail)
        @test occursin("DictionaryReplacement", err.value.msg)
        @test read(path) == fixture
        # The migration recipe: rewrite once through a replacement-declaring
        # writer (retention preserves the 2.x pool, null slot included, so
        # appends with fresh pools need replacement).
        migrated = Arrow.Table(read(path))
        Arrow.Writer(path; file=false, dictreplacement=true) do w
            Arrow.write(w, migrated)
        end
        Arrow.append(path, tail)
        t = Arrow.Table(path)
        # The 2.x fixture holds the five-row table twice (two partitions).
        @test t.ints == [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7]
        @test t.strs[11] == "tail"
        @test isequal(t.dict, [before.dict..., "lo", "hi"])
        @test t.structs[end] == [Pair{String,Any}("a", 7), Pair{String,Any}("b", "t")]
    end
end

end # module IncrementalWriterTests
