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

module RangedScanTests

using Test, Arrow, Tables, Dates

struct MeteredSource <: Arrow.AbstractArrowSource
    bytes::Vector{UInt8}
    fetched::Base.RefValue{Int}
    requests::Base.RefValue{Int}
end
MeteredSource(bytes, fetched) = MeteredSource(bytes, fetched, Ref(0))
Arrow.sourcelength(s::MeteredSource) = length(s.bytes)
function Arrow.readrange(s::MeteredSource, offset, len)
    s.fetched[] += len
    s.requests[] += 1
    return s.bytes[(offset + 1):(offset + len)]
end

@testset "filtered range scans stop fetching at their limit" begin
    for compress in (nothing, :lz4, :zstd)
        io = IOBuffer()
        Arrow.Writer(io; file=true, compress) do writer
            for batch = 1:32
                Arrow.write(
                    writer,
                    (
                        id=collect(((batch - 1) * 2048 + 1):(batch * 2048)),
                        category=Arrow.DictEncode(fill("batch-$batch", 2048)),
                    ),
                )
            end
        end
        bytes = take!(io)
        for limit in (0, 1)
            fetched = Ref(0)
            source = Arrow.SourceFile(MeteredSource(bytes, fetched); tailbytes=1024)
            table = Arrow.Table(source; filter=Arrow.col(:id) > 0, limit)
            @test collect(table.id) == collect(1:limit)
            @test collect(table.category) == fill("batch-1", limit)
            @test fetched[] < length(bytes) ÷ 2
        end
        source = Arrow.SourceFile(MeteredSource(bytes, Ref(0)); tailbytes=1024)
        table = Arrow.Table(
            source;
            scan=Tables.Scan(filter=Arrow.col(:id) > 2048, offset=2050, limit=5),
        )
        @test collect(table.id) == collect(4099:4103)
        @test collect(table.category) == fill("batch-3", 5)
    end
end

@testset "filters on converted columns fetch only their columns" begin
    n = 20_000
    io = IOBuffer()
    Arrow.Writer(io; file=true) do writer
        for batch = 1:4
            Arrow.write(
                writer,
                (
                    x=collect(((batch - 1) * n + 1):(batch * n)),
                    t=[Time(0) + Second(i % 86_400) for i = 1:n],
                    k=[Symbol(:key, i % 7) for i = 1:n],
                    pad=fill(repeat("p", 64), n),
                ),
            )
        end
    end
    bytes = take!(io)
    full = Arrow.Table(copy(bytes))
    for filter in (
        Arrow.colcmp(>=, Arrow.col(:t), Time(5, 30)),
        Arrow.colcmp(==, Arrow.col(:t), Time(0, 0, 7)),
        Arrow.colin(Arrow.col(:t), (Time(0, 0, 7), Time(0, 0, 9))),
        Arrow.colcmp(==, Arrow.col(:k), :key3),
    )
        fetched = Ref(0)
        source = Arrow.SourceFile(MeteredSource(bytes, fetched); tailbytes=1024, gapbytes=0)
        got = Arrow.Table(source; select=[:x], filter)
        want = Tables.scan(full, Tables.Scan(select=(:x,), filter=filter))
        @test collect(got.x) == collect(want.x)
        @test fetched[] < length(bytes) ÷ 4
    end
end

@testset "a select through a source builds what a mapped read builds" begin
    n = 20_000
    io = IOBuffer()
    Arrow.Writer(io; file=true) do writer
        for batch = 1:4
            Arrow.write(
                writer,
                (
                    x=collect(((batch - 1) * n + 1):(batch * n)),
                    s=[string("value number ", i) for i = 1:n],
                    pad=fill(repeat("p", 64), n),
                ),
            )
        end
    end
    bytes = take!(io)
    path = tempname()
    write(path, bytes)
    fetched = Ref(0)
    mapped() = Arrow.Table(path; select=[:x, :s])
    ranged() = Arrow.Table(Arrow.SourceFile(MeteredSource(bytes, fetched)); select=[:x, :s])
    m, r = mapped(), ranged()
    @test typeof(m.x) == typeof(r.x) && typeof(m.s) == typeof(r.s)
    @test m.x == r.x && m.s == r.s
    # Beyond the bytes the source hands over, a ranged read allocates what a
    # mapped read does, plus a few KiB per batch of range planning.
    mappedbytes = @allocated mapped()
    fetched[] = 0
    rangedbytes = @allocated ranged()
    @test rangedbytes - fetched[] <= mappedbytes + 256 * 1024
    rm(path)
end

@testset "a select through a source keeps only the bytes its columns use" begin
    io = IOBuffer()
    Arrow.Writer(io; file=true) do writer
        for batch = 1:200
            Arrow.write(
                writer,
                (
                    i1=collect((1000batch - 999):(1000batch)),
                    s1=fill(repeat("s", 40), 1000),
                    pad=fill(repeat("p", 60), 1000),
                ),
            )
        end
    end
    bytes = take!(io)
    mapped = Arrow.Table(copy(bytes))
    # One small column of the object keeps about its own bytes.
    t = Arrow.Table(MeteredSource(bytes, Ref(0)); select=[:i1])
    @test t.i1 == mapped.i1
    @test Base.summarysize(t.i1) < 1.5 * 8 * 200_000 < length(bytes) / 8
    # Columns that fill most of what was fetched view it in place.
    fetched = Ref(0)
    ranged() = Arrow.Table(MeteredSource(bytes, fetched); select=[:s1, :pad])
    t = ranged()
    @test t.s1 == mapped.s1 && t.pad == mapped.pad
    fetched[] = 0
    copied = @allocated(ranged()) - fetched[]
    @test copied < length(bytes) / 4
end

@testset "a Stream over a source fetches only the columns it reads" begin
    n = 20_000
    io = IOBuffer()
    Arrow.Writer(io; file=true) do writer
        for batch = 1:4
            Arrow.write(
                writer,
                (
                    x=collect(((batch - 1) * n + 1):(batch * n)),
                    pad=fill(repeat("p", 64), n),
                ),
            )
        end
    end
    bytes = take!(io)
    for (kw, want) in (
        ((select=[:x],), collect(1:(4n))),
        ((select=[:x], filter=Arrow.col(:x) > 3n), collect((3n + 1):(4n))),
    )
        fetched = Ref(0)
        stream = Arrow.Stream(Arrow.SourceFile(MeteredSource(bytes, fetched)); kw...)
        @test reduce(vcat, [collect(t.x) for t in stream]) == want
        @test fetched[] < length(bytes) ÷ 3
    end
    # A stream that reads every column reads whole blocks.
    fetched = Ref(0)
    stream = Arrow.Stream(Arrow.SourceFile(MeteredSource(bytes, fetched)))
    @test sum(t -> length(t.x), stream) == 4n
    @test fetched[] >= length(bytes) - 1024
end

@testset "a read that fetches many ranges at once finds each of them" begin
    io = IOBuffer()
    Arrow.Writer(io; file=true) do writer
        for batch = 1:64
            Arrow.write(writer, (x=collect((100batch - 99):(100batch)), y=fill(batch, 100)))
        end
    end
    bytes = take!(io)
    want = Arrow.Table(bytes; select=[:x], filter=Arrow.col(:y) > 8)
    for gapbytes in (0, 64)
        source = Arrow.SourceFile(MeteredSource(bytes, Ref(0)); gapbytes)
        got = Arrow.Table(source; select=[:x], filter=Arrow.col(:y) > 8)
        @test got.x == want.x == 801:6400
    end
end

@testset "small record batches cost few requests" begin
    io = IOBuffer()
    Arrow.Writer(io; file=true) do writer
        for batch = 1:2000
            Arrow.write(writer, (x=collect((10batch - 9):(10batch)), y=fill(batch, 10)))
        end
    end
    bytes = take!(io)
    # A Stream reads small batches whole, many to a request.
    source = MeteredSource(bytes, Ref(0))
    @test sum(t -> sum(t.x), Arrow.Stream(source; select=[:x])) == sum(1:20_000)
    @test source.requests[] <= 2000 ÷ 100
    # A filtered window fetches a small batch's columns in one request.
    source = MeteredSource(bytes, Ref(0))
    t = Arrow.Table(source; select=[:x], filter=Arrow.col(:y) > 0, limit=10^9)
    @test t.x == 1:20_000
    metadata = 2000
    @test source.requests[] <= metadata + 2000 + 2
end

end
