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
using PooledArrays
import CategoricalArrays
import DataAPI

const AC = Arrow.ArrowCore

# A producer may refill its buffers once the consumer asks for another partition.
struct ReusedPartitions{Known,F}
    values::Vector{Int}
    count::Int
    before_next::F
end
Tables.partitions(p::ReusedPartitions) = p
Base.IteratorSize(::Type{<:ReusedPartitions{true}}) = Base.HasLength()
Base.IteratorSize(::Type{<:ReusedPartitions{false}}) = Base.SizeUnknown()
Base.length(p::ReusedPartitions) = p.count
function Base.iterate(p::ReusedPartitions, i=1)
    p.before_next(i)
    i > p.count && return nothing
    fill!(p.values, i)
    return (; a=p.values), i + 1
end

# The IPC messages of a stream, or of a file's message section.
function framedmessages(bytes)
    if bytes[1:6] == b"ARROW1"
        footerlen = only(reinterpret(Int32, bytes[(end - 9):(end - 6)]))
        bytes = bytes[9:(end - 10 - footerlen)]
    end
    return Arrow.framemessages(AC.heapregion(bytes))
end

# (id, isDelta, length) of each dictionary batch, in order.
dictbatches(bytes) = [
    (m.msg.header.id, m.msg.header.isDelta, m.msg.header.data.length) for
    m in framedmessages(bytes) if m.msg.header isa Arrow.Meta.DictionaryBatch
]

# :dictionary or :record for each batch message, in order.
batchkinds(bytes) = [
    m.msg.header isa Arrow.Meta.DictionaryBatch ? :dictionary : :record for
    m in framedmessages(bytes) if !(m.msg.header isa Arrow.Meta.Schema)
]

@testset "incremental writer and append" begin
    t1 = (; a=Int64[1, 2], b=["x", "y"])
    t2 = (; a=Int64[3, 4], b=["z", "w"])

    @testset "partitions publish before their buffers are reused" begin
        for known in (false, true),
            file in (false, true),
            compress in (nothing, :lz4, :zstd)

            io = IOBuffer()
            part = ReusedPartitions{known,typeof(identity)}(zeros(Int, 32_768), 3, identity)
            Arrow.write(io, part; file, compress, ntasks=4)
            @test collect(Arrow.Table(take!(io)).a) == repeat(1:3; inner=32_768)
        end

        # The producer can depend on the previous batch already being available.
        io = IOBuffer()
        before_next = function (i)
            i == 1 && return
            pos = position(io)
            seekstart(io)
            bytes = read(io)
            seek(io, pos)
            @test collect(Arrow.Table(bytes).a) == repeat(1:(i - 1); inner=2)
        end
        part = ReusedPartitions{false,typeof(before_next)}([0, 0], 3, before_next)
        Arrow.write(io, part)
        @test collect(Arrow.Table(take!(io)).a) == [1, 1, 2, 2, 3, 3]

        # Unknown-length inputs declare nullable fields without reading ahead,
        # even when this particular iteration happens to yield just one table.
        for known in (false, true)
            part = ReusedPartitions{known,typeof(identity)}([0, 0], 1, identity)
            table = Arrow.Table(take!(Arrow.tobuffer(part)))
            @test eltype(table.a) === (known ? Int : Union{Missing,Int})
        end

        for compress in (nothing, :lz4, :zstd)
            io = Arrow.tobuffer((; a=[0, 0]))
            part = ReusedPartitions{false,typeof(identity)}([0, 0], 3, identity)
            Arrow.append(io, part; compress)
            @test collect(Arrow.Table(take!(io)).a) == [0, 0, 1, 1, 2, 2, 3, 3]
        end
    end

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

    @testset "a zero-batch Stream keeps its schema when rewritten" begin
        io = IOBuffer()
        Arrow.Writer(io; file=false) do w
            Arrow.write(w, (; a=Int64[], b=Union{Missing,String}[]))
        end
        bytes = take!(io)
        # Drop the one empty record batch: schema message, then end-of-stream.
        msgs = Arrow.framemessages(Arrow.ArrowCore.heapregion(bytes))
        schemaend = msgs[1].body.offset + msgs[1].body.len
        nobatches = vcat(bytes[1:schemaend], bytes[(end - 7):end])
        @test isempty(collect(Arrow.Stream(nobatches)))

        for file in (true, false)
            out = IOBuffer()
            Arrow.write(
                out,
                Arrow.Stream(nobatches);
                file=file,
                colmetadata=Dict(:a => ["k" => "v"]),
            )
            t = Arrow.Table(take!(out))
            @test Tables.columnnames(t) == [:a, :b]
            @test eltype(t.b) == Union{Missing,Arrow.DataStrings.DataString}
            @test DataAPI.colmetadata(t, :a, "k") == "v"
        end

        out = IOBuffer()
        Arrow.Writer(out; file=false) do w
            Arrow.write(w, Arrow.Stream(nobatches))
            Arrow.write(w, t1)
            Arrow.write(w, Arrow.Stream(nobatches))
        end
        @test Arrow.Table(take!(out)).a == [1, 2]

        path = joinpath(mktempdir(), "s.arrow")
        Arrow.write(path, t1; file=false)
        Arrow.append(path, Arrow.Stream(nobatches))
        @test Arrow.Table(path).a == [1, 2]

        @test_throws ArgumentError Arrow.write(IOBuffer(), Tables.partitioner(NamedTuple[]))

        # The Writer interns the schema's names under the facade's bounds.
        longsch = Arrow.ArrowCore.Schema([
            Arrow.ArrowCore.Field("x"^5000, Arrow.ArrowCore.IntType(64, true)),
        ])
        longbytes = Arrow.writestream(longsch, Arrow.ArrowCore.RecordBatch[])
        @test_throws Arrow.ValidationError Arrow.Writer(IOBuffer(); file=false) do w
            Arrow.write(w, Arrow.Stream(longbytes))
        end
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
        # Lossy type drift is refused, naming the column and both types.
        err = @test_throws ArgumentError Arrow.write(w, (; a=[1.5], b=["q"]))
        @test occursin("column a", err.value.msg)
        @test occursin("double", err.value.msg) && occursin("int64", err.value.msg)
        # Every field is nullable, so later missing values conform; the
        # writer stays usable after a refused write.
        Arrow.write(w, (; a=[missing, 1], b=["q", missing]))
        Arrow.write(w, t2)
        close(w)
        t = Arrow.Table(take!(io))
        @test isequal(t.a, [1, 2, missing, 1, 3, 4])
        @test isequal(t.b, ["x", "y", "q", missing, "z", "w"])
        # nullable=false declares the first table's nullability instead.
        w = Arrow.Writer(IOBuffer(); nullable=false)
        Arrow.write(w, t1)
        err = @test_throws ArgumentError Arrow.write(w, (; a=[missing, 1], b=["q", "r"]))
        @test occursin("not nullable", err.value.msg)
        close(w)
    end

    @testset "later tables widen losslessly" begin
        for file in (true, false)
            io = IOBuffer()
            Arrow.Writer(io; file) do w
                Arrow.write(w, (; i=Int64[1], f=[1.5], g=[0.5], s=["a"], u=[true]))
                Arrow.write(
                    w,
                    (; i=Int32[2], f=Float32[2.5], g=Int16[3], s=[missing], u=[false]),
                )
                Arrow.write(
                    w,
                    (; i=[missing], f=[missing], g=[missing], s=[missing], u=[missing]),
                )
            end
            t = Arrow.Table(take!(io))
            @test isequal(collect(t.i), [1, 2, missing])
            @test isequal(collect(t.f), [1.5, 2.5, missing])
            @test isequal(collect(t.g), [0.5, 3.0, missing])
            @test isequal(collect(t.s), ["a", missing, missing])
            @test isequal(collect(t.u), [true, false, missing])
            @test eltype(t.i) == Union{Missing,Int64}
        end
        # Narrowing (Int64 into Int32) is refused.
        w = Arrow.Writer(IOBuffer())
        Arrow.write(w, (; i=Int32[1]))
        err = @test_throws ArgumentError Arrow.write(w, (; i=Int64[2]))
        @test occursin("column i", err.value.msg)
        close(w)
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

    @testset "later tables take the first table's Julia value types" begin
        # Later tables are built like later partitions of one eager write, so
        # writer values the retained public domain does not use still conform.
        cols = (;
            dt=[DateTime(2026), DateTime(2027)],
            s=[SubString("ab", 1, 1), SubString("ab", 2, 2)],
            nt=[(a=1, b="x"), (a=2, b="y")],
            tup=[(1, 2), (3, 4)],
            d=[Dict("k" => 1), Dict("k" => 2)],
        )
        for file in (true, false)
            io = IOBuffer()
            Arrow.Writer(io; file=file) do w
                Arrow.write(w, cols)
                return Arrow.write(w, cols)
            end
            @test take!(io) ==
                  Arrow._writebytes(Tables.partitioner([cols, cols]); file=file)
        end
        path = joinpath(mktempdir(), "values.arrow")
        Arrow.write(path, cols; file=false)
        Arrow.append(path, cols)
        t = Arrow.Table(path)
        @test t.dt == repeat(cols.dt, 2)
        @test t.s == ["a", "b", "a", "b"]
        # A non-conforming later table names the difference.
        io = IOBuffer()
        w = Arrow.Writer(io)
        Arrow.write(w, (; nt=[(a=1, b="x")]))
        err = @test_throws ArgumentError Arrow.write(w, (; nt=[(b="x", a=1)]))
        @test occursin("does not conform", err.value.msg)
        close(w)
    end

    @testset "append DateTime values to an Arrow 2.x labelled timestamp" begin
        # Arrow 2.x labelled every DateTime column JuliaLang.DateTime; fresh
        # DateTime values lower exactly into the retained descriptor.
        ms = AC.TimestampType(AC.MILLISECOND, nothing)
        meta = [
            "ARROW:extension:name" => "JuliaLang.DateTime",
            "ARROW:extension:metadata" => "",
        ]
        f = AC.Field("dt", ms; nullable=true, metadata=meta)
        d = AC.ArrayData(ms, 1, [AC.BufferSlice(), AC._databuffer(Int64[0])]; nullcount=0)
        sch = AC.Schema([f])
        path = joinpath(mktempdir(), "dt.arrow")
        write(path, Arrow.writestream(sch, [AC.RecordBatch(sch, [d])]))
        Arrow.append(path, (; dt=[DateTime(2026, 1, 2, 3)]))
        @test Arrow.Table(path).dt == [DateTime(1970), DateTime(2026, 1, 2, 3)]
    end

    @testset "dictionaries grow across tables in both formats" begin
        for file in (true, false),
            dictdeltas in (false, true),
            mk in (Arrow.DictEncode, PooledArray)

            io = IOBuffer()
            Arrow.Writer(io; file, dictdeltas) do w
                Arrow.write(w, (; d=mk(["a", "b", "a"])))
                Arrow.write(w, (; d=mk(["c", "c", "b"])))   # adds "c"
                Arrow.write(w, (; d=mk(["b", "a"])))        # nothing new
            end
            bytes = take!(io)
            @test Arrow.Table(bytes).d == ["a", "b", "a", "c", "c", "b", "b", "a"]
            batches = [(isdelta, n) for (_, isdelta, n) in dictbatches(bytes)]
            if dictdeltas
                # A base dictionary, then a delta holding only the new category.
                @test batches == [(false, 2), (true, 1)]
            elseif file
                # One complete dictionary, after the record batches using it.
                @test batches == [(false, 3)]
                @test batchkinds(bytes) == [:record, :record, :record, :dictionary]
            else
                # A stream writes the grown dictionary again, whole.
                @test batches == [(false, 2), (false, 3)]
            end
            # A growing pool is indexed with Int32, as pyarrow writes.
            f = only(
                Arrow.readfile(
                    take!(Arrow.write(IOBuffer(), Arrow.Table(bytes); file=true)),
                ).schema.fields,
            )
            @test (f.type::AC.DictionaryType).indextype == AC.IntType(32, true)
        end
        # dictreplacement=true replaces each table's pool instead (stream only).
        io = IOBuffer()
        w = Arrow.Writer(io; file=false, dictreplacement=true)
        Arrow.write(w, (; d=Arrow.DictEncode(["a", "b", "a"])))
        Arrow.write(w, (; d=Arrow.DictEncode(["c", "c", "b"])))
        close(w)
        bytes = take!(io)
        @test Arrow.Table(bytes).d == ["a", "b", "a", "c", "c", "b"]
        @test [(isdelta, n) for (_, isdelta, n) in dictbatches(bytes)] == [(false, 2), (false, 2)]
        # The file format has no replacement to declare.
        @test_throws ArgumentError Arrow.Writer(IOBuffer(); file=true, dictreplacement=true)
    end

    @testset "one-shot dictionaries use the narrowest index type" begin
        indextype(t) = (
            only(
                Arrow.readfile(take!(Arrow.write(IOBuffer(), t; file=true))).schema.fields,
            ).type::AC.DictionaryType
        ).indextype
        @test indextype((; d=Arrow.DictEncode(["a", "b"]))) == AC.IntType(8, true)
        @test indextype((; d=PooledArray(string.(1:200)))) == AC.IntType(16, true)
        @test indextype((; d=PooledArray(string.(1:40_000)))) == AC.IntType(32, true)
        # An ordered CategoricalArray writes an ordered dictionary.
        for ordered in (true, false)
            c = CategoricalArrays.categorical(
                ["lo", "hi", "lo"];
                ordered,
                levels=["lo", "hi"],
            )
            bytes = take!(Arrow.write(IOBuffer(), (; c=c); file=true))
            t = only(Arrow.readfile(bytes).schema.fields).type::AC.DictionaryType
            @test t.ordered == ordered
            @test Arrow.Table(bytes).c == ["lo", "hi", "lo"]
        end
    end

    @testset "close is idempotent; a closed writer refuses writes" begin
        io = IOBuffer()
        w = Arrow.Writer(io)
        Arrow.write(w, t1)
        close(w)
        close(w)
        err = @test_throws ArgumentError Arrow.write(w, t2)
        @test occursin("closed", err.value.msg)
        # Closing a writer that never wrote does not throw (finally-friendly)
        # and writes a valid output with no columns.
        io2 = IOBuffer()
        close(Arrow.Writer(io2))
        none = Arrow.Table(take!(io2))
        @test isempty(Tables.columnnames(none)) && Tables.rowcount(none) == 0
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

    @testset "compressed batches on several tasks publish before each write returns" begin
        big = (; a=collect(1:50_000), b=[string(k) for k = 1:50_000])
        # The second partition is refused; the first's batch still publishes.
        bad = Tables.partitioner([(; a=[1, 2], b=["p", "q"]), (; a=[1.5], b=["x"])])
        for compress in (:lz4, :zstd), file in (true, false)
            sizes = map((1, 4)) do ntasks
                io = IOBuffer()
                w = Arrow.Writer(io; file, compress, ntasks, batchsize=7000)
                Arrow.write(w, big)
                afterbig = io.size
                @test_throws ArgumentError Arrow.write(w, bad)
                afterbad = io.size
                close(w)
                bytes = take!(io)
                t = Arrow.Table(bytes)
                @test t.a == [big.a; 1; 2] && t.b == [big.b; "p"; "q"]
                (afterbig, afterbad, bytes)
            end
            @test sizes[1] == sizes[2]
        end
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

        # Appending to nothing starts a stream; the one-argument form curries.
        fresh = joinpath(mktempdir(), "new.arrows")
        @test (t1 |> Arrow.append(fresh)) == fresh
        @test Arrow.append(fresh, t2) == fresh
        @test Arrow.Table(fresh).a == [1, 2, 3, 4]
        @test read(fresh, 6) != b"ARROW1"
        io = IOBuffer()
        Arrow.append(io, t1)
        Arrow.append(io, t2)
        @test Arrow.Table(take!(io)).b == ["x", "y", "z", "w"]
    end

    @testset "append to a stream without an end-of-stream marker" begin
        # The reader accepts a stream that ends right after its last message;
        # appending must keep that message intact.
        io = IOBuffer()
        Arrow.write(io, t1; file=false)
        noeos = take!(io)[1:(end - 8)]
        @test Arrow.Table(noeos).a == [1, 2]
        path = joinpath(mktempdir(), "noeos.arrow")
        write(path, noeos)
        Arrow.append(path, t2)
        @test Arrow.Table(path).a == [1, 2, 3, 4]
        Arrow.append(path, t2)
        @test Arrow.Table(path).b == ["x", "y", "z", "w", "z", "w"]

        io = IOBuffer()
        write(io, noeos)
        Arrow.append(io, t2)
        @test Arrow.Table(take!(io)).b == ["x", "y", "z", "w"]
    end

    @testset "append with dictionary columns" begin
        # New categories extend the stream's dictionary, written again whole
        # (or as a delta batch with dictdeltas=true); existing ones reuse it.
        for dictdeltas in (false, true)
            path = joinpath(mktempdir(), "d.arrow")
            Arrow.write(path, (; d=Arrow.DictEncode(["a", "b", "a"])); file=false)
            Arrow.append(path, (; d=Arrow.DictEncode(["a", "b", "b"])); dictdeltas)
            @test Arrow.Table(path).d == ["a", "b", "a", "a", "b", "b"]
            Arrow.append(path, (; d=Arrow.DictEncode(["z", "z", "q"])); dictdeltas)
            @test Arrow.Table(path).d == ["a", "b", "a", "a", "b", "b", "z", "z", "q"]
            @test [(isdelta, n) for (_, isdelta, n) in dictbatches(read(path))] == (dictdeltas ? [(false, 2), (true, 2)] : [(false, 2), (false, 4)])
        end

        # A Writer stream that opted into replacement replaces on append.
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
            dict=Arrow.DictEncode(["lo", "mid"]),
        )
        # The 2.x pool carries a null slot (2.x encoded missing as a pool
        # entry); the new category extends it with a delta dictionary batch.
        Arrow.append(path, tail)
        t = Arrow.Table(path)
        # The 2.x fixture holds the five-row table twice (two partitions).
        @test t.ints == [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7]
        @test t.strs[11] == "tail"
        @test isequal(t.dict, [before.dict..., "lo", "mid"])
        @test t.structs[end] == (a=7, b="t")
    end
end

end # module IncrementalWriterTests
