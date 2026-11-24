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

module ReviewRegressions

using Arrow, CategoricalArrays, DataAPI, Serialization, Tables, Test
import ArrowTypes

const AC = Arrow.AC
bytes(tbl; kw...) = take!(Arrow.tobuffer(tbl; kw...))
frames(data) = Arrow.framemessages(AC.heapregion(data))

struct CountingSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
    largest::Threads.Atomic{Int}
end
CountingSource(data) = CountingSource(data, Threads.Atomic{Int}(0))
Arrow.sourcelength(s::CountingSource) = length(s.data)
function Arrow.readrange(s::CountingSource, off, n)
    Threads.atomic_max!(s.largest, Int(n))
    return s.data[(off + 1):(off + n)]
end

@testset "Historical IPC files and union layouts" begin
    empty = Arrow.Table(joinpath(@__DIR__, "java_compressed_zero_length.arrow"))
    @test Tables.columnnames(empty) == [:name, :age]
    @test Tables.rowcount(empty) == 0
    viewfile = Arrow.Table(joinpath(@__DIR__, "reject_reason_trimmed.arrow"))
    @test Tables.rowcount(viewfile) == 27
    @test viewfile.reject_reason[end] == "POST_ONLY"

    # Arrow IPC stores variadicBufferCounts as signed 64-bit integers.
    fb = Arrow.FlatBuffers
    b = fb.Builder(64)
    fb.startvector!(b, sizeof(Int64), 3, sizeof(Int64))
    foreach(cnt -> fb.prepend!(b, cnt), Iterators.reverse(Int64[0, 7, 0]))
    counts = fb.endvector!(b, 3)
    fb.startobject!(b, 5)
    fb.prependoffsetslot!(b, 4, counts, 0)
    fb.finish!(b, Arrow.Meta.recordBatchEnd(b))
    rb = fb.getrootas(Arrow.Meta.RecordBatch, Vector(fb.finishedbytes(b)), 0)
    @test eltype(rb.variadicBufferCounts) === Int64
    @test collect(rb.variadicBufferCounts) == Int64[0, 7, 0]

    for version in ("v4", "v5"), format in ("file", "stream")
        path = joinpath(@__DIR__, "fixtures2x", "union-$version-$format.arrowbytes")
        data = read(path)
        for src in (data, Arrow.SourceFile(CountingSource(data); tailbytes=32, gapbytes=0))
            t = Arrow.Table(src; select=[:sparse, :sibling, :dense])
            @test t.sibling == [31, 32]
            for col in (t.sparse, t.dense)
                @test col == [11, 2.5]
                @test col[1] isa Int64 && col[2] isa Float64
            end
            @test only(Arrow.Stream(src; select=:sibling)).sibling == [31, 32]
            @test only(Arrow.Stream(src; select=:dense)).dense == [11, 2.5]
        end
    end
end

@testset "Append negotiates the existing stream's version and features" begin
    old = read(joinpath(@__DIR__, "fixtures-dictionaries", "delta-V4-stream.arrowbytes"))
    tbl = (x=Int64[1],)
    for (original, options) in ((old, (;)), (bytes(tbl), (; compress=:zstd)))
        io = IOBuffer()
        write(io, original)
        @test_throws ArgumentError Arrow.append(io, tbl; options...)
        @test take!(io) == original
        mktemp() do path, file
            write(file, original)
            close(file)
            @test_throws ArgumentError Arrow.append(path, tbl; options...)
            @test read(path) == original
        end
    end
    io = IOBuffer()
    Arrow.write(io, tbl; compress=:lz4)
    Arrow.append(io, (x=[2],); compress=:zstd)
    compressed = take!(io)
    @test Arrow.Table(compressed).x == [1, 2]
    @test Arrow.FEATURE_COMPRESSED_BODY in Int64.(first(frames(compressed)).features)
    @test frames(compressed)[3].msg.header.compression !== nothing

    f, d = AC.fromjulia_dict("a", ["old"], [0])
    g = AC.Field("b", f.type; nullable=f.nullable, children=f.children)
    sch = AC.Schema([f, g])
    # Both columns share the very same dictionary on the wire.
    ids = IdDict(f => Int64(7), g => Int64(7))
    original = Arrow.writestream(sch, [AC.RecordBatch(sch, [d, d])]; dictids=ids)
    for right in ("old", "right")
        io = IOBuffer()
        write(io, original)
        Arrow.append(io, (a=["left"], b=[right]))
        Arrow.append(io, (a=[right], b=["left"]))
        appended = take!(io)
        result = Arrow.Table(appended)
        @test result.a == ["old", "left", right]
        @test result.b == ["old", right, "left"]
        @test !(
            Arrow.FEATURE_DICTIONARY_REPLACEMENT in
            Int64.(first(frames(appended)).features)
        )
        dicts = [
            m.msg.header for
            m in frames(appended) if m.msg.header isa Arrow.Meta.DictionaryBatch
        ]
        @test [(d.id, d.isDelta) for d in dicts] == [(7, false), (7, true)]
    end
end

@testset "Writing streams with no batches preserves the projected schema" begin
    for (data, limit) in (
        (bytes((x=[1], y=[2]); file=true), 0),
        (bytes((x=Int[], y=Int[]); file=true), nothing),
    )
        for (select, types) in (
            ([:x => Float64, :y], (Float64, Int64)),
            ([:x => Float64, :y => Float32], (Float64, Float32)),
        )
            stream = Arrow.Stream(data; select, limit)
            result = Arrow.Table(bytes(stream))
            @test Tables.columnnames(result) == [:x, :y]
            @test Tables.schema(result).types == types
            @test Tables.rowcount(result) == 0
        end
    end
end

@testset "Range limits, ownership, and empty dictionary scans" begin
    large = bytes((x=collect(Int64, 1:1_000_000),); file=true)
    for (limits, error) in (
        (Arrow.Limits(max_total_allocated_bytes=200_000), Arrow.AllocationLimitError),
        (Arrow.Limits(max_body_bytes=200_000), Arrow.ValidationError),
    )
        counter = CountingSource(large)
        @test_throws error first(Arrow.Stream(counter; limits))
        @test counter.largest[] <= 200_000
    end
    # A small first batch cannot trigger a prefetch of an oversized successor.
    parts = Tables.partitioner([(x=[1],), (x=collect(Int64, 1:1_000_000),)])
    counter = CountingSource(bytes(parts; file=true))
    s = Arrow.Stream(counter; limits=Arrow.Limits(max_total_allocated_bytes=200_000))
    firstbatch, state = iterate(s)
    @test firstbatch.x == [1]
    @test_throws Arrow.AllocationLimitError iterate(s, state)
    @test counter.largest[] <= 200_000

    data = bytes((x=[1, 2], y=["one", "two"]); file=true)
    remote = CountingSource(data)
    for t in (Arrow.Table(remote; select=:x), copy(Arrow.Table(data)))
        x = t.x
        Arrow.release!(t)
        Arrow.release!(t)
        @test_throws InvalidStateException t.x
        @test x == [1, 2]
    end
    s = Arrow.Stream(remote; select=:x)
    batch = only(s)
    x = batch.x
    Arrow.release!(batch)
    @test_throws InvalidStateException iterate(s)
    @test x == [1, 2]

    for file in (true, false), stringtype in (String, Arrow.DataStrings.DataString)
        c = categorical(["b", "c"]; levels=["c", "b", "unused"], ordered=true)
        data = bytes((x=[1, 2], c=c); file)
        t = Arrow.Table(data; select=:c, filter=Arrow.col(:x) > 3, stringtype)
        @test isempty(t.c)
        @test DataAPI.levels(t.c) == ["c", "b", "unused"]
        @test DataAPI.refpool(t.c) !== nothing
        @test isordered(t.c)
        @test Base.nonmissingtype(eltype(t.c)) === stringtype
    end
end

@noinline function retainedscan()
    data = bytes((x=collect(Int64, 1:100_000),); file=true)
    return Arrow.Table(data; limit=1), WeakRef(AC._storageroot(data))
end

@testset "A short owned scan can release its input" begin
    t, backing = retainedscan()
    GC.@preserve t begin
        GC.gc(true)
        GC.gc(true)
        @test backing.value === nothing
        @test t.x == [1]
    end
end

@testset "Dictionary identity preserves nested tags and float bits" begin
    U = Union{Int64,Float64}
    for values in (
        [U[1], U[1.0]],
        [
            [reinterpret(Float64, 0x7ff8000000000001)],
            [reinterpret(Float64, 0x7ff8000000000002)],
        ],
    )
        t = Arrow.Table(bytes((x=Arrow.DictEncode(values),)))
        for i in eachindex(values)
            @test typeof(only(t.x[i])) === typeof(only(values[i]))
            @test only(t.x[i]) === only(values[i])
        end
        @test collect(DataAPI.refarray(t.x)) == [1, 2]
    end
    data = bytes((x=Arrow.DictEncode(U[1, 1.0]),))
    t = Arrow.Table(data)
    io = IOBuffer()
    serialize(io, t)
    for v in (
        copy(t.x),
        copy(t).x,
        Arrow.read(data, Tables.columntable).x,
        deserialize(seekstart(io)).x,
    )
        @test v[1] === Int64(1)
        @test v[2] === Float64(1)
    end
end

struct MappedVector <: AbstractVector{Int}
    text::String
end
Base.size(::MappedVector) = (1,)
Base.getindex(::MappedVector, i::Int) = i == 1 ? 42 : throw(BoundsError())
ArrowTypes.ArrowType(::Type{MappedVector}) = String
ArrowTypes.arrowname(::Type{MappedVector}) = :ArrowReviewMappedVector
ArrowTypes.toarrow(x::MappedVector) = x.text
ArrowTypes.JuliaType(::Val{:ArrowReviewMappedVector}, ::Any, ::Any) = MappedVector
ArrowTypes.fromarrow(::Type{MappedVector}, x) = MappedVector(String(x))

@testset "Narrowing an Any column preserves registered vector mappings" begin
    t = Arrow.Table(bytes((mapped=Any[MappedVector("value")],)))
    @test only(t.mapped) isa MappedVector
    @test only(t.mapped).text == "value"
end

@testset "Zero-column table equality and duplicate metadata" begin
    data = bytes((x=[1, 2, 3, 4, 5],))
    t = Arrow.Table(data; scan=Tables.Scan(select=()))
    shorter = Arrow.Table(data; scan=Tables.Scan(select=(), limit=2))
    equal = Arrow.Table(data; scan=Tables.Scan(select=(), limit=5))
    @test t != shorter && !isequal(t, shorter)
    @test t == equal && isequal(t, equal) && hash(t) == hash(equal)
    @test length(Dict(t => 1, shorter => 2)) == 2
    meta = ["k" => "first", "k" => "last"]
    t = Arrow.Table(bytes((x=[1],); metadata=meta, colmetadata=(x=meta,)))
    @test Arrow.getmetadata(t)["k"] == DataAPI.metadata(t, "k") == "last"
    @test DataAPI.metadata(t)["k"] == DataAPI.colmetadata(t, :x)["k"] == "last"
    @test collect(getfield(t, :schema).metadata) == meta
end

@noinline function export_backing(sp, ap)
    x = collect(Int64, 1:1024)
    Arrow.export_c_data!(sp, ap, (; x))
    return WeakRef(AC._storageroot(x))
end

@noinline function release_array_backing(ap, storage)
    GC.gc(true)
    was_live = storage.value !== nothing
    Arrow._release_c!(ap)
    Arrow._reap!()
    return was_live
end

@testset "C schema and array own independent lifetimes" begin
    sr, ar = Ref{Arrow.CArrowSchema}(), Ref{Arrow.CArrowArray}()
    GC.@preserve sr ar begin
        sp = Base.unsafe_convert(Ptr{Arrow.CArrowSchema}, sr)
        ap = Base.unsafe_convert(Ptr{Arrow.CArrowArray}, ar)
        storage = export_backing(sp, ap)
        try
            @test release_array_backing(ap, storage)
            GC.gc(true)
            GC.gc(true)
            @test storage.value === nothing
            @test ar[].release == C_NULL && sr[].release != C_NULL
            @test unsafe_string(sr[].format) == "+s"
        finally
            Arrow._release_c!(ap)
            Arrow._release_c!(sp)
            Arrow._reap!()
        end
    end
end

@testset "C import limits apply to arrays and streams" begin
    for limits in (
        Arrow.Limits(max_array_length=1),
        Arrow.Limits(max_buffer_bytes=1),
        Arrow.Limits(max_body_bytes=1),
        Arrow.Limits(max_metadata_bytes=1),
        Arrow.Limits(max_metadata_objects=1),
        Arrow.Limits(max_nesting_depth=0),
    )
        tbl = (x=Int32[1, 2],)
        sp, ap = Arrow.to_c_data(tbl)
        try
            @test_throws Arrow.ValidationError Arrow.Table(sp, ap; limits)
            @test unsafe_load(sp).release == unsafe_load(ap).release == C_NULL
        finally
            Arrow.free_c_data!(sp, ap)
        end
        str = Ref{Arrow.CArrowArrayStream}()
        GC.@preserve str begin
            ptr = Base.unsafe_convert(Ptr{Arrow.CArrowArrayStream}, str)
            Arrow.export_stream!(ptr, tbl)
            @test_throws Arrow.ValidationError Arrow.Table(ptr; limits)
            @test str[].release == C_NULL
        end
    end
end

end # module
