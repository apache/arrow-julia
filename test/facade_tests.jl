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

# The public facade: Arrow.Table / Arrow.Stream / Arrow.write. Interface
# tests only — layout/validation depth lives in the core tests, batteries,
# and the conformance suites.

module FacadeTests

using Test
using Tables
using Dates
import DataAPI
import Profile
using Arrow
using DataStrings
using Logging
using UUIDs
import DataDecimals, Durations
using DataStrings: StringVector, StringPayload, BytesVector, DataBytes
include(joinpath(@__DIR__, "support", "mapped.jl"))

struct _InterruptingPeriod <: Dates.Period end
struct _OutOfMemoryPeriod <: Dates.Period end
struct _InterruptingInteger <: Integer end
struct _ScanHookInteger <: Integer end
Base.Int64(::_InterruptingInteger) = throw(InterruptException())
const _SCAN_HOOK_CALLS = Ref(0)
Base.convert(::Type{Second}, ::_InterruptingPeriod) = throw(InterruptException())
Base.convert(::Type{Second}, ::_OutOfMemoryPeriod) = throw(OutOfMemoryError())
Base.Int64(::_ScanHookInteger) = (_SCAN_HOOK_CALLS[] += 1; Int64(7))
Base.:(==)(::Int64, ::_ScanHookInteger) = false
Base.:(==)(::_ScanHookInteger, ::Int64) = false

# In-memory byte-range sources: the plain one, and one that meters bytes.
struct _BytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_BytesSource) = length(s.data)
Arrow.readrange(s::_BytesSource, off, len) = s.data[(off + 1):(off + len)]
struct _MeteredSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
    fetched::Base.RefValue{Int64}
end
Arrow.sourcelength(s::_MeteredSource) = length(s.data)
function Arrow.readrange(s::_MeteredSource, off, len)
    s.fetched[] += len
    return s.data[(off + 1):(off + len)]
end
# A source that logs every request, and a concurrent one whose reads finish
# in reverse request order (the last-issued read of a round completes first)
# while a counter records the most reads ever in flight.
struct _LoggingSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
    requests::Vector{NTuple{2,Int64}}
end
Arrow.sourcelength(s::_LoggingSource) = length(s.data)
# A Stream reads ahead on another task: log and inspect under one lock.
const _LOG_LOCK = ReentrantLock()
function Arrow.readrange(s::_LoggingSource, off, len)
    @lock _LOG_LOCK push!(s.requests, (Int64(off), Int64(len)))
    return s.data[(off + 1):(off + len)]
end
mutable struct _ConcurrentSource <: Arrow.AbstractArrowSource
    const data::Vector{UInt8}
    const limit::Int
    @atomic inflight::Int
    @atomic peak::Int
    @atomic issued::Int
    @atomic preferencequeries::Int
end
_ConcurrentSource(data, limit) = _ConcurrentSource(data, limit, 0, 0, 0, 0)
Arrow.sourcelength(s::_ConcurrentSource) = length(s.data)
function Arrow.concurrentreads(s::_ConcurrentSource)
    @atomic s.preferencequeries += 1
    return s.limit
end
function Arrow.readrange(s::_ConcurrentSource, off, len)
    n = @atomic s.inflight += 1
    while true
        p = @atomic s.peak
        (n <= p || (@atomicreplace s.peak p => n).success) && break
    end
    order = @atomic s.issued += 1
    # Later requests of a round return sooner: results must land by request.
    delaywidth = min(s.limit, 4)
    sleep(0.002 * max(0, delaywidth - (order % delaywidth)))
    @atomic s.inflight -= 1
    return s.data[(off + 1):(off + len)]
end
# Sources that violate the contract in each way the reader must refuse.
# A table with DataAPI metadata, as a DataFrame carries it.
struct _MetaTable
    columns::NamedTuple
    metadata::Dict{String,String}
    colmetadata::Dict{Symbol,Dict{String,String}}
end
Tables.istable(::Type{_MetaTable}) = true
Tables.columnaccess(::Type{_MetaTable}) = true
Tables.columns(t::_MetaTable) = t.columns
DataAPI.metadatasupport(::Type{_MetaTable}) = (read=true, write=false)
DataAPI.metadatakeys(t::_MetaTable) = keys(t.metadata)
DataAPI.metadata(t::_MetaTable, k::AbstractString) = t.metadata[k]
DataAPI.colmetadatasupport(::Type{_MetaTable}) = (read=true, write=false)
DataAPI.colmetadatakeys(t::_MetaTable, c::Symbol) = keys(get(t.colmetadata, c, Dict()))
DataAPI.colmetadata(t::_MetaTable, c::Symbol, k::AbstractString) = t.colmetadata[c][k]

struct _ShortSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_ShortSource) = length(s.data)
Arrow.readrange(s::_ShortSource, off, len) = s.data[(off + 1):(off + max(0, len - 1))]
struct _LongSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_LongSource) = length(s.data)
Arrow.readrange(s::_LongSource, off, len) = vcat(s.data[(off + 1):(off + len)], 0x00)
struct _WrongTypeSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_WrongTypeSource) = length(s.data)
Arrow.readrange(s::_WrongTypeSource, off, len) = String(s.data[(off + 1):(off + len)])
struct _BadLengthSource <: Arrow.AbstractArrowSource
    reported::Any
end
Arrow.sourcelength(s::_BadLengthSource) = s.reported
Arrow.readrange(s::_BadLengthSource, off, len) = zeros(UInt8, len)
struct _BadConcurrencySource <: Arrow.AbstractArrowSource
    preference::Any
end
Arrow.sourcelength(::_BadConcurrencySource) = 0
Arrow.concurrentreads(s::_BadConcurrencySource) = s.preference
Arrow.readrange(::_BadConcurrencySource, off, len) = zeros(UInt8, len)

const MIXED = (
    ints=Int64[1, 2, 3, 4],
    floats=[1.5, missing, 3.5, 4.5],
    strs=["a", "bb", missing, "dddd"],
    dates=[Date(2024, 1, 1), Date(2025, 6, 15), missing, Date(1969, 12, 31)],
    stamps=[
        DateTime(2024, 1, 1, 12, 30),
        missing,
        DateTime(2000, 1, 1),
        DateTime(1970, 1, 1),
    ],
    clocks=[Time(12, 30, 15), Time(0), missing, Time(23, 59, 59)],
    spans=[Millisecond(250), missing, Millisecond(0), Millisecond(-10)],
    lists=[[1, 2], Int64[], [3], missing],
    pooled=Arrow.DictEncode(["lo", "hi", "lo", missing]),
)

function assert_mixed(t)
    @test t.ints == MIXED.ints
    @test isequal(t.floats, MIXED.floats)
    @test isequal(t.strs, MIXED.strs)
    @test isequal(t.dates, MIXED.dates)
    @test isequal(t.stamps, MIXED.stamps)
    @test isequal(t.clocks, MIXED.clocks)
    @test isequal(t.spans, MIXED.spans)
    @test isequal(t.lists, [Any[1, 2], Any[], Any[3], missing])
    @test isequal(t.pooled, ["lo", "hi", "lo", missing])
end

# Bytes one warmed call of `f` requests from the allocator. `@allocated`
# counts the usable size of each big block instead, and an allocator that
# hands back a larger cached block (macOS malloc does) can nearly double
# that. Top-level functions, so the probe measures `f`, not testset closure
# boxing.
function _requestedbytes(f)
    f()
    f()
    Profile.Allocs.clear()
    Profile.Allocs.@profile sample_rate = 1 f()
    bytes = sum(a.size for a in Profile.Allocs.fetch().allocs; init=0)
    Profile.Allocs.clear()
    return bytes
end

_tableallocs(bytes) = _requestedbytes(() -> Arrow.Table(bytes))
_writeallocs(path, tbl) = _requestedbytes(() -> Arrow.write(path, tbl))

@testset "Arrow facade" begin
    @testset "file round-trip with metadata" begin
        path = tempname()
        Arrow.write(
            path,
            MIXED;
            metadata=Dict("who" => "facade"),
            colmetadata=Dict(:ints => Dict("unit" => "count")),
        )
        t = Arrow.Table(path)
        assert_mixed(t)
        @test Tables.istable(typeof(t))
        @test Tables.columnnames(t) == collect(keys(MIXED))
        @test DataAPI.metadata(t, "who") == "facade"
        @test collect(DataAPI.metadatakeys(t)) == ["who"]
        @test DataAPI.colmetadata(t, :ints, "unit") == "count"
        @test_throws KeyError DataAPI.metadata(t, "absent")
        # A released table refuses column access; a column taken before the
        # release keeps the mapping alive and stays readable.
        ints = t.ints
        Arrow.release!(t)
        @test_throws InvalidStateException t.ints
        @test ints == MIXED.ints
        rm(path)
    end

    @testset "stream format, IO sinks and sources" begin
        io = IOBuffer()
        Arrow.write(io, MIXED; file=false)
        t = Arrow.Table(seekstart(io))
        assert_mixed(t)
    end

    @testset "compression kwargs" begin
        for compress in (:lz4, :zstd)
            io = IOBuffer()
            Arrow.write(io, MIXED; compress=compress)
            assert_mixed(Arrow.Table(take!(io)))
        end
    end

    @testset "partitions become record batches; Stream iterates them" begin
        io = IOBuffer()
        Arrow.write(
            io,
            Tables.partitioner([(x=Int64[1, 2],), (x=Int64[3, 4],)]);
            file=false,
        )
        bytes = take!(io)
        s = Arrow.Stream(bytes)
        @test length(s) == 2
        parts = collect(s)
        @test parts[1].x == [1, 2] && parts[2].x == [3, 4]
        # Stream is a Tables.partitions source: writing it re-partitions.
        io2 = IOBuffer()
        Arrow.write(io2, Arrow.Stream(bytes); file=false)
        @test length(Arrow.Stream(take!(io2))) == 2
        # Release closes the public iterator for raw and compressed streams.
        # Decompressed heap buffers must not bypass the same lifecycle gate.
        for file in (false, true), compress in (:none, :lz4, :zstd)
            closedio = IOBuffer()
            Arrow.write(closedio, (x=Int64[1],); file=file, compress=compress)
            closed = Arrow.Stream(take!(closedio))
            Arrow.release!(closed)
            Arrow.release!(closed)
            @test_throws InvalidStateException first(closed)

            partitionio = IOBuffer()
            Arrow.write(
                partitionio,
                Tables.partitioner([(x=Int64[1],), (x=Int64[2],)]);
                file=file,
                compress=compress,
            )
            partitionstream = Arrow.Stream(take!(partitionio))
            partition, state = iterate(partitionstream)
            x = partition.x
            @test x == [1]
            Arrow.release!(partition)
            @test_throws InvalidStateException partition.x
            @test x == [1]
            @test_throws InvalidStateException iterate(partitionstream, state)
        end
        # A path-backed file's mapping is never unmapped eagerly (columns may
        # still read it): releasing a yielded batch closes the stream, and
        # garbage collection unmaps once nothing reaches the mapping. The
        # checks run in their own frame, so no temporary outlives them.
        function releasemapped(path)
            mappedstream = Arrow.Stream(path; mmap=true)
            mappedregion = only(getfield(mappedstream, :regions))
            @test getfield(getfield(mappedregion, :cell), :action) == C_NULL
            mappedpartition, state = iterate(mappedstream)
            x = mappedpartition.x
            Arrow.release!(mappedpartition)
            @test_throws InvalidStateException mappedpartition.x
            @test x == [1]
            @test_throws InvalidStateException iterate(mappedstream, state)
            return nothing
        end
        path = tempname()
        try
            Arrow.write(path, Tables.partitioner([(x=Int64[1],), (x=Int64[2],)]); file=true)
            releasemapped(path)
        finally
            rmmapped(path)
        end
        # Whole-table read concatenates.
        @test Arrow.Table(bytes).x == [1, 2, 3, 4]
        # A Stream is a sequence of tables, not one: whole-table Tables.jl
        # access refuses instead of reading each batch as one row of vectors.
        @test_throws ArgumentError Tables.columns(Arrow.Stream(bytes))
        @test_throws ArgumentError Tables.rows(Arrow.Stream(bytes))
        @test_throws ArgumentError Tables.columntable(Arrow.Stream(bytes))
        @test_throws ArgumentError Tables.rowtable(Arrow.Stream(bytes))
    end

    @testset "a stream-format Stream reads one batch at a time" begin
        tables = [(x=Int64[i, i + 1], s=["a$i", "a much longer value $i"]) for i = 1:3]
        # Over a pipe, each batch is yielded while the writer still holds
        # the next one: the writer waits for the reader before writing it.
        pipe = Base.BufferStream()
        seen = Channel{Int}(3)
        writer = errormonitor(Threads.@spawn begin
            Arrow.Writer(pipe; file=false) do w
                for (k, tbl) in enumerate(tables)
                    Arrow.write(w, tbl)
                    k < length(tables) && take!(seen)
                end
            end
            close(pipe)
        end)
        reader = Threads.@spawn begin
            got = Int64[]
            for (k, b) in enumerate(Arrow.Stream(pipe))
                append!(got, b.x)
                put!(seen, k)
            end
            got
        end
        done = timedwait(() -> istaskdone(reader), 120) == :ok
        @test done && fetch(reader) == [1, 2, 2, 3, 3, 4]
        # A failed or stuck reader must not leave the writer waiting.
        close(seen)
        done || close(pipe)
        @test timedwait(() -> istaskdone(writer), 60) == :ok
        io = IOBuffer()
        Arrow.write(io, Tables.partitioner(tables); file=false)
        bytes = take!(io)
        # Over bytes the batches are framed in place: a length, and a second
        # iteration starts over.
        s = Arrow.Stream(bytes)
        @test length(s) == 3
        @test [b.x for b in s] == [b.x for b in s] == [[1, 2], [2, 3], [3, 4]]
        @test_throws ArgumentError s[1]
        # An IO is read once, and its batch count is not known up front.
        once = Arrow.Stream(IOBuffer(bytes))
        @test_throws ArgumentError length(once)
        @test [b.s[2] for b in once] == ["a much longer value $i" for i = 1:3]
        @test_throws ArgumentError iterate(once)
        # A file-format Stream has random access.
        fio = IOBuffer()
        Arrow.write(fio, Tables.partitioner(tables); file=true)
        f = Arrow.Stream(take!(fio))
        @test length(f) == 3 && f[2].x == [2, 3] && f[3].s[1] == "a3"
        @test_throws BoundsError f[4]
        # A truncated stream yields the batches before the damage, then fails;
        # the Stream stays stopped at the error.
        for src in (bytes[1:(end - 24)], IOBuffer(bytes[1:(end - 24)]))
            cut = Arrow.Stream(src)
            b, state = iterate(cut)
            @test b.x == [1, 2]
            b, state = iterate(cut, state)
            @test b.x == [2, 3]
            @test_throws Arrow.ValidationError iterate(cut, state)
            @test_throws ArgumentError iterate(cut, state)
        end
    end

    @testset "facade reader limits" begin
        fileio = IOBuffer()
        Arrow.write(fileio, (x=Int64[1, 2],))
        filebytes = take!(fileio)
        streamio = IOBuffer()
        Arrow.write(streamio, (x=Int64[1, 2],); file=false)
        streambytes = take!(streamio)

        configured = Arrow.Limits(
            max_metadata_bytes=1024 * 1024,
            max_messages=16,
            max_concurrent_reads=3,
        )
        incompatible = Arrow.Limits(max_concurrent_reads=2)
        too_tight = Arrow.Limits(max_metadata_bytes=0)

        path = tempname()
        open(path, "w") do io
            Base.write(io, filebytes)
        end
        try
            raw_sources = (
                ("path mmap", () -> path, NamedTuple()),
                ("path IO", () -> path, (mmap=false,)),
                ("IO", () -> IOBuffer(copy(filebytes)), NamedTuple()),
                ("stream IO", () -> IOBuffer(copy(streambytes)), NamedTuple()),
                ("byte vector", () -> copy(filebytes), NamedTuple()),
                ("AbstractArrowSource", () -> _BytesSource(copy(filebytes)), NamedTuple()),
            )
            for (name, makesource, kwargs) in raw_sources
                @testset "$name" begin
                    table = Arrow.Table(makesource(); limits=configured, kwargs...)
                    @test table.x == [1, 2]
                    stream = Arrow.Stream(makesource(); limits=configured, kwargs...)
                    @test first(stream).x == [1, 2]
                    @test getfield(stream, :src).limits == configured
                    Arrow.release!(table)
                    Arrow.release!(stream)

                    @test_throws Arrow.AC.ValidationError Arrow.Table(
                        makesource();
                        limits=too_tight,
                        kwargs...,
                    )
                    @test_throws Arrow.AC.ValidationError Arrow.Stream(
                        makesource();
                        limits=too_tight,
                        kwargs...,
                    )
                end
            end
        finally
            ispath(path) && rm(path)
        end

        opened_handles = (
            (
                "SourceFile",
                Arrow.SourceFile(_BytesSource(copy(filebytes)); limits=configured),
            ),
            ("ArrowFile", Arrow.readfile(copy(filebytes); limits=configured)),
            ("IPCStream", Arrow.readstream(copy(streambytes); limits=configured)),
        )
        for (name, handle) in opened_handles
            @testset "$name" begin
                @test Arrow.Table(handle).x == [1, 2]
                @test Arrow.Table(handle; limits=configured).x == [1, 2]
                stream = Arrow.Stream(handle)
                @test first(stream).x == [1, 2]
                @test getfield(stream, :src).limits == configured
                @test first(Arrow.Stream(handle; limits=configured)).x == [1, 2]
                @test_throws ArgumentError Arrow.Table(handle; limits=incompatible)
                @test_throws ArgumentError Arrow.Stream(handle; limits=incompatible)
            end
        end

        deferred = Arrow.Limits(max_array_length=1)
        @test_throws Arrow.AC.ValidationError Arrow.Table(
            Arrow.SourceFile(_BytesSource(copy(filebytes)); limits=deferred),
        )
        @test_throws Arrow.AC.ValidationError Arrow.Table(
            Arrow.readfile(copy(filebytes); limits=deferred),
        )

        @static if VERSION >= v"1.11"
            @test Base.ispublic(Arrow, :Limits)
            @test Base.ispublic(Arrow, :AllocationLimitError)
            @test Base.ispublic(Arrow, :release!)
        end
        @test !Base.isexported(Arrow, :Limits)
        @test !Base.isexported(Arrow, :AllocationLimitError)
        @test !Base.isexported(Arrow, :release!)
    end

    @testset "default limits scale with the input" begin
        # Ordinary files read with the default limits whatever their size.
        path = tempname()
        try
            Arrow.write(path, (a=collect(Int64, 1:20_000_000),))
            t = Arrow.Table(path)
            @test length(t.a) == 20_000_000 && t.a[end] == 20_000_000
            Arrow.release!(t)
            strings = [lpad(string(i), 64, 'x') for i = 1:1_000_000]
            Arrow.write(path, (s=strings,))
            t = Arrow.Table(path)
            @test t.s == strings
            Arrow.release!(t)
            # An explicit ceiling still applies (string columns copy their
            # bytes: about 80 MB here).
            @test_throws Arrow.AllocationLimitError Arrow.Table(
                path;
                limits=Arrow.Limits(max_total_allocated_bytes=16 * 1024^2),
            )
        finally
            rm(path; force=true)
        end
        # A 464-byte file whose run-end encoded column claims ~1e9 rows reads
        # as a run-end view: nothing proportional to the claimed length is
        # allocated.
        rf, rd = Arrow.AC.fromjulia("run_ends", Int32[999_999_999])
        vf, vd = Arrow.AC.fromjulia("values", Int64[7])
        ree = Arrow.AC.RunEndEncodedType()
        f = Arrow.AC.Field("r", ree; nullable=false, children=[rf, vf])
        d = Arrow.AC.ArrayData(
            ree,
            999_999_999,
            Arrow.AC.BufferSlice[];
            children=[rd, vd],
            nullcount=0,
        )
        sch = Arrow.AC.Schema([f])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d])])
        fbytes = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, [d])])
        @test length(bytes) < 1024
        # Every source kind, including handles opened beforehand.
        for source in (
            bytes,
            fbytes,
            IOBuffer(bytes),
            Arrow.readstream(copy(bytes)),
            Arrow.readfile(copy(fbytes)),
        )
            r = Arrow.Table(source).r
            @test length(r) == 999_999_999 && r[1] == 7 && r[end] == 7
        end
        stats = @timed Arrow.Table(bytes)
        @test stats.value isa Arrow.Table
        @test stats.bytes < 64_000_000
    end

    @testset "scan pushdown through Arrow.Table" begin
        io = IOBuffer()
        Arrow.write(io, (x=collect(Int64, 1:100), y=string.(1:100)))
        fb = take!(io)
        t = Arrow.Table(
            fb;
            scan=Tables.Scan(select=(:y,), filter=Tables.colcmp(==, Tables.col(:x), 42)),
        )
        @test Tables.columnnames(t) == [:y]
        @test t.y == ["42"]
        # Stream-format input takes the post-decode path, same result.
        io2 = IOBuffer()
        Arrow.write(io2, (x=collect(Int64, 1:100), y=string.(1:100)); file=false)
        t2 = Arrow.Table(
            take!(io2);
            scan=Tables.Scan(select=(:y,), filter=Tables.colcmp(==, Tables.col(:x), 42)),
        )
        @test t2.y == ["42"]
        # Renames land as output names.
        t3 = Arrow.Table(fb; scan=Tables.Scan(select=(:x => :renamed,), limit=2))
        @test Tables.columnnames(t3) == [:renamed]
        @test t3.renamed == [1, 2]
    end

    @testset "read keywords build one scan" begin
        tbl = (a=collect(1:6), b=["x", "y", "z", "w", "v", "u"], c=collect(6.0:-1:1))
        for file in (true, false)
            io = IOBuffer()
            Arrow.write(io, tbl; file=file)
            bytes = take!(io)
            @test Tables.columnnames(Arrow.Table(bytes; select=[:c, :a])) == [:c, :a]
            @test Tables.columnnames(Arrow.Table(bytes; drop=[:b])) == [:a, :c]
            @test Arrow.Table(bytes; filter=Arrow.col(:a) > 4).b == ["v", "u"]
            @test Arrow.Table(
                bytes;
                filter=Arrow.colcmp(<=, Arrow.col(:c), 2.0),
                limit=1,
            ).a == [5]
            @test Arrow.Table(bytes; filter=Arrow.colin(Arrow.col(:b), ("y", "w"))).a ==
                  [2, 4]
            @test isempty(Arrow.Table(bytes; filter=Arrow.isnull(Arrow.col(:a))).a)
            @test Arrow.Table(bytes; limit=2).a == [1, 2]
            @test Arrow.read(bytes, Tables.columntable; select=[:b]).b == tbl.b
            @test_throws ArgumentError Arrow.Table(bytes; select=[:a], drop=[:b])
            @test_throws ArgumentError Arrow.Table(
                bytes;
                limit=1,
                scan=Tables.Scan(select=(:a,)),
            )
        end
        @test Arrow.col === Tables.col && Arrow.colcmp === Tables.colcmp
        @test Arrow.colin === Tables.colin && Arrow.isnull === Tables.isnull
    end

    @testset "a Stream describes its source without reading a batch" begin
        tbl = (a=collect(1:3), b=["x", "y", "z"])
        io = IOBuffer()
        Arrow.write(
            io,
            Tables.partitioner([tbl, tbl]);
            file=true,
            metadata=Dict("k" => "v"),
            colmetadata=Dict(:a => Dict("ck" => "cv")),
        )
        s = Arrow.Stream(take!(io))
        shown = sprint(show, MIME"text/plain"(), s)
        @test startswith(
            shown,
            "Arrow.Stream with 2 record batches, 2 columns, and schema:",
        )
        @test occursin(":b", shown) && length(shown) < 300
        @test Tables.columnnames(s) == [:a, :b]
        @test Tables.schema(s).types == (Union{Missing,Int64}, Union{Missing,DataString})
        @test DataAPI.nrow(s) == 6 && DataAPI.ncol(s) == 2
        @test DataAPI.metadata(s, "k") == "v" && DataAPI.metadata(s) == Dict("k" => "v")
        @test DataAPI.colmetadata(s, :a, "ck") == "cv"
        @test DataAPI.colmetadata(s, :a) == Dict("ck" => "cv")
        io = IOBuffer()
        Arrow.write(io, tbl; file=false)
        seekstart(io)
        s = Arrow.Stream(io)
        @test startswith(sprint(show, s), "Arrow.Stream with 2 columns and schema:")
        @test Tables.columnnames(s) == [:a, :b]
        @test_throws ArgumentError DataAPI.nrow(s)
        @test first(s).b == tbl.b
    end

    @testset "Stream batches take the read keywords" begin
        parts = [(x=collect(1:4) .+ 4k, s=string.(collect(1:4) .+ 4k)) for k = 0:2]
        xs(stream) = reduce(vcat, [collect(t.x) for t in stream]; init=Int[])
        for file in (true, false)
            io = IOBuffer()
            Arrow.write(io, Tables.partitioner(parts); file=file)
            bytes = take!(io)
            @test [Tables.columnnames(t) for t in Arrow.Stream(bytes; select=[:s])] == fill([:s], 3)
            @test [Tables.columnnames(t) for t in Arrow.Stream(bytes; drop=[:s])] == fill([:x], 3)
            @test xs(Arrow.Stream(bytes; filter=Arrow.col(:x) > 5)) == 6:12
            # A window counts rows across batches and stops the iteration.
            @test xs(Arrow.Stream(bytes; limit=6)) == 1:6
            @test length(collect(Arrow.Stream(bytes; limit=6))) == 2
            @test xs(Arrow.Stream(bytes; scan=Tables.Scan(offset=3, limit=6))) == 4:9
            scan =
                Tables.Scan(select=(:x => :y,), filter=Arrow.col(:x) > 1, offset=2, limit=5)
            @test reduce(vcat, [collect(t.y) for t in Arrow.Stream(bytes; scan=scan)]) ==
                  Arrow.Table(bytes; scan=scan).y ==
                  4:8
            s = Arrow.Stream(bytes; select=[:s], filter=Arrow.col(:x) > 5)
            @test Tables.columnnames(s) == [:s]
            @test Tables.schema(s).types == (Union{Missing,DataString},)
            @test_throws ArgumentError DataAPI.nrow(s)
            @test_throws ArgumentError Arrow.Stream(bytes; select=[:nope])
            if file
                @test s[2].s == ["6", "7", "8"]
                @test DataAPI.nrow(Arrow.Stream(bytes; select=[:s])) == 12
                @test_throws ArgumentError Arrow.Stream(bytes; limit=2)[1]
            end
        end
    end

    @testset "input that is not Arrow IPC data says so" begin
        for bytes in
            (collect(UInt8, 1:16), UInt8[0x10, 0, 0, 0, 1, 2, 3, 4], UInt8[1, 2, 3])
            err = try
                Arrow.Table(bytes)
            catch e
                e
            end
            @test err isa Arrow.ValidationError
            @test startswith(
                sprint(showerror, err),
                "Arrow.ValidationError: not Arrow IPC data",
            )
        end
    end

    @testset "a source streams batches in windows and downloads whole in concurrent ranges" begin
        # Four batches of 160 KB: a file pass reads them in one window after
        # the tail, and serves a second pass and random access from it.
        parts = [(a=collect(Int64, ((k - 1) * 20_000 + 1):(k * 20_000)),) for k = 1:4]
        for file in (true, false)
            bytes = take!(Arrow.write(IOBuffer(), Tables.partitioner(parts); file=file))
            log = _LoggingSource(bytes, NTuple{2,Int64}[])
            st = Arrow.Stream(log)
            @test [b.a[1] for b in st] == [1, 20_001, 40_001, 60_001]
            # A stream object reads forward, its readahead doubling.
            @test length(log.requests) <= (file ? 2 : 2 + length(parts))
            n = length(log.requests)
            @test [b.a[end] for b in st] == [20_000, 40_000, 60_000, 80_000]
            if file
                @test length(log.requests) == n
                @test st[3].a[1] == 40_001 && length(log.requests) == n
                # The row count reads each batch's metadata, not its body.
                @test DataAPI.nrow(st) == 80_000
                @test length(log.requests) == n + length(parts)
                @test maximum(last, log.requests[(n + 1):end]) < 1_000
            else
                # A second pass starts over.
                @test length(log.requests) > n
            end
            kept = Arrow.Stream(log; filter=Arrow.col(:a) > 70_000, select=[:a])
            @test sum(b -> length(b.a), kept) == 10_000
        end
        # Batches larger than a window are fetched one at a time, and at most
        # one ahead of the batch in use: the third is not fetched while the
        # first is in use.
        large = [(a=collect(Int64, ((k - 1) * 1_200_000 + 1):(k * 1_200_000)),) for k = 1:3]
        bytes = take!(Arrow.write(IOBuffer(), Tables.partitioner(large)))
        third = Arrow.readfile(bytes).recordblocks[3][1]
        log = _LoggingSource(bytes, NTuple{2,Int64}[])
        st = Arrow.Stream(log)
        b1, _ = iterate(st)
        @test b1.a[1] == 1
        tailstart = length(bytes) - 65536
        @test @lock _LOG_LOCK all(r -> sum(r) <= third || r[1] >= tailstart, log.requests)
        @test sum(b -> length(b.a), st) == 3_600_000
        # With concurrent reads, a whole-object read fetches the bytes before
        # the tail in 8 MiB ranges at once, into one buffer.
        big = take!(Arrow.write(IOBuffer(), (a=collect(Int64, 1:2_200_000),)))
        cs = _ConcurrentSource(big, 4)
        @test Arrow.Table(cs).a == 1:2_200_000
        @test (@atomic cs.issued) == 1 + cld(length(big) - 65536, 8 * 1024 * 1024)
        @test (@atomic cs.peak) > 1
    end

    @testset "ranged source fetches only what the scan needs" begin
        io = IOBuffer()
        Arrow.write(
            io,
            Tables.partitioner([
                (a=collect(Int64, 1:1000), b=[string("v", i) for i = 1:1000]),
                (a=collect(Int64, 1001:2000), b=[string("v", i) for i = 1001:2000]),
            ]),
        )
        fb = take!(io)
        fetched = Ref(Int64(0))
        src = _MeteredSource(fb, fetched)
        rf = Arrow.SourceFile(src; tailbytes=1024, coalesce_gap=0)
        t = Arrow.Table(rf; scan=Tables.Scan(select=(:b,), limit=3, offset=1500))
        @test t.b == ["v1501", "v1502", "v1503"]
        # The first batch is skipped entirely and column :a is never fetched.
        @test fetched[] < length(fb) ÷ 2

        # Request rounds through the public path over an object larger than
        # the tail window: a pushable scan reads the tail once, then the
        # surviving batches' metadata with their bodies (small bodies come
        # whole), so the selected buffers need no third request; ranges
        # inside the cached tail window are served from it without a request.
        wide = [string("v", i, "-", repeat("x", 60)) for i = 1:2000]
        wio = IOBuffer()
        Arrow.write(
            wio,
            Tables.partitioner([
                (a=collect(Int64, 1:1000), b=wide[1:1000]),
                (a=collect(Int64, 1001:2000), b=wide[1001:2000]),
            ]),
        )
        wb = take!(wio)
        @test length(wb) > 65536
        log = _LoggingSource(wb, NTuple{2,Int64}[])
        t2 = Arrow.Table(log; scan=Tables.Scan(select=(:a,), filter=Tables.col(:a) < 10))
        @test t2.a == collect(1:9)
        tailreq = (Int64(length(wb) - 65536), Int64(65536))
        @test count(==(tailreq), log.requests) == 1
        @test length(log.requests) == 2
        # An object no larger than the tail window is entirely in hand after
        # the tail read: every planned range is served from it.
        small = _LoggingSource(fb, NTuple{2,Int64}[])
        t3 = Arrow.Table(small; scan=Tables.Scan(select=(:a,), limit=3, offset=1500))
        @test t3.a == [1501, 1502, 1503]
        @test small.requests == [(Int64(0), Int64(length(fb)))]
        # No scan reads the object whole: the cached tail plus the prefix,
        # two requests and no range planning.
        empty!(log.requests)
        tw = Arrow.Table(log)
        @test tw.a == 1:2000 && length(log.requests) == 2
        @test sum(last, log.requests) == length(wb)

        # An empty projection still uses the ranged path. The result keeps its
        # row count and transfers much less than the whole object. The scan
        # battery checks the exact no-body request plan.
        emptyfetched = Ref(Int64(0))
        emptyfile = Arrow.SourceFile(
            _MeteredSource(wb, emptyfetched);
            tailbytes=1024,
            coalesce_gap=0,
        )
        tz = Arrow.Table(emptyfile; scan=Tables.Scan(select=()))
        @test isempty(Tables.columnnames(tz))
        @test Tables.rowcount(tz) == 2000
        @test emptyfetched[] < length(wb) ÷ 2

        # A filtered empty projection remains range-efficient. The scan
        # battery checks that only the filter buffers are requested.
        filterfetched = Ref(Int64(0))
        filterfile = Arrow.SourceFile(
            _MeteredSource(wb, filterfetched);
            tailbytes=1024,
            coalesce_gap=0,
        )
        tf =
            Arrow.Table(filterfile; scan=Tables.Scan(select=(), filter=Tables.col(:a) < 10))
        @test isempty(Tables.columnnames(tf))
        @test Tables.rowcount(tf) == 9
        @test filterfetched[] < length(wb) ÷ 2

        # Concurrent reads: bounded by the source's limit, results placed by
        # request even though later requests complete first.
        cs = _ConcurrentSource(fb, 3)
        cf = Arrow.SourceFile(cs; tailbytes=1024, coalesce_gap=0)
        tc = Arrow.Table(cf; scan=Tables.Scan(select=(:a, :b)))
        @test tc.a == 1:2000 && tc.b == [string("v", i) for i = 1:2000]
        @test 1 < (@atomic cs.peak) <= 3
        @test (@atomic cs.preferencequeries) == 1

        # Limits cap both the worker count and active reads. The source
        # preference is sampled once when the handle is constructed.
        cappedsource = _ConcurrentSource(fb, 10_000)
        capped = Arrow.SourceFile(
            cappedsource;
            limits=Arrow.Limits(max_concurrent_reads=2),
            tailbytes=1024,
            coalesce_gap=0,
        )
        cappedtable = Arrow.Table(capped; scan=Tables.Scan(select=(:a, :b)))
        @test cappedtable.a == 1:2000
        @test 1 < (@atomic cappedsource.peak) <= 2
        @test (@atomic cappedsource.preferencequeries) == 1

        # One SourceFile owns one handle-wide semaphore across overlapping
        # scans, not one semaphore per scan operation.
        serialsource = _ConcurrentSource(fb, 1)
        serial = Arrow.SourceFile(serialsource; tailbytes=1024, coalesce_gap=0)
        results = Vector{Any}(undef, 2)
        @sync for i = 1:2
            Threads.@spawn results[i] =
                Arrow.Table(serial; scan=Tables.Scan(select=(:a, :b)))
        end
        @test all(t -> t.a == 1:2000, results)
        @test (@atomic serialsource.peak) == 1
        @test (@atomic serialsource.preferencequeries) == 1

        # Contract violations fail closed with ValidationError, never a
        # wrong table or a stray error type.
        for bad in (_ShortSource(fb), _LongSource(fb), _WrongTypeSource(fb))
            @test_throws Arrow.AC.ValidationError Arrow.Table(
                bad;
                scan=Tables.Scan(select=(:a,)),
            )
        end
        for reported in (
            -1,
            Int128(typemax(Int64)) + 1,
            Int128(typemin(Int64)) - 1,
            3402.0,
            1.5,
            "3402",
            nothing,
        )
            @test_throws Arrow.AC.ValidationError Arrow.Table(_BadLengthSource(reported))
        end
        @test_throws ArgumentError Arrow.SourceFile(
            _BytesSource(fb);
            limits=Arrow.Limits(max_concurrent_reads=0),
        )
        for preference in (0, -1, Int128(typemax(Int)) + 1, 1.0, 1.5, "2", nothing)
            @test_throws Arrow.AC.ValidationError Arrow.SourceFile(
                _BadConcurrencySource(preference),
            )
        end
    end

    @testset "exact generic scan budgets" begin
        x = Union{Missing,Int64}[missing, 2, 3, 4, 5, 6, 7, 8]
        input = (x=x, y=Int64[9, 8, 7, 6, 5, 4, 3, 2])
        scan = Tables.Scan(
            select=(:x,),
            filter=(Tables.col(:x) > 2) & (Tables.col(:y) < 7),
            offset=1,
            limit=2,
        )
        bound = Tables.resolve(scan, [:x, :y])
        V(T, n) = Arrow.AC._materializedvectorbytes(T, n)
        # Two leaf masks plus the conjunction, the final Bool mask, five
        # qualifying indexes, then a two-row projected nullable column.
        needed =
            V(Symbol, 2) +
            3 * V(Union{Missing,Bool}, 8) +
            V(Bool, 8) +
            V(Int, 5) +
            V(Symbol, 1) +
            V(AbstractVector, 1) +
            V(eltype(x), 2)
        exact = Arrow.AllocationBudget(needed)
        got = Arrow._executeplan(input, bound, exact)
        @test got.x == Union{Missing,Int64}[5, 6]
        @test Arrow._remaining(exact) == 0
        @test_throws Arrow.AllocationLimitError Arrow._executeplan(
            input,
            bound,
            Arrow.AllocationBudget(needed - 1),
        )
    end

    @testset "scan compilation bounds request amplification" begin
        # One source column can expand to an arbitrary number of requested
        # aliases. The resolver must reject that request before Tables builds
        # its output vector.
        aliases = Tables.Scan(select=fill(:only, 100_000))
        @test_throws Arrow.AllocationLimitError Arrow._resolvescan(
            aliases,
            [:only],
            Arrow.AllocationBudget(1_000_000),
        )

        # Regex resolution converts schema Symbols to Strings twice: once in
        # the bounded preflight and once in Tables.resolve.
        longname = Symbol(repeat("field", 20_000))
        regexscan = Tables.Scan(select=(r"absent",), validate=false)
        @test_throws Arrow.AllocationLimitError Arrow._resolvescan(
            regexscan,
            [longname],
            Arrow.AllocationBudget(100_000),
        )

        # Filter resolution reconstructs the expression tree. Its width is a
        # request-controlled resource even over a one-column file.
        leaf = Tables.col(:only) > 0
        widefilter = Tables.Scan(filter=Tables.AndExpr(fill(leaf, 100_000)))
        @test_throws Arrow.AllocationLimitError Arrow._resolvescan(
            widefilter,
            [:only],
            Arrow.AllocationBudget(1_000_000),
        )

        # Scalar conversion has fourteen closed descriptor/unit tokens.
        # Membership uses them only when the complete public conversion
        # preserves equality. Abstract inputs must not box each member.
        nvalues = 100_000
        temporal_cases = (
            (
                Arrow.AC.Field("d", Arrow.AC.DateType(Arrow.AC.DAY); nullable=false),
                Any[Date(2024, 1, 1) for _ = 1:nvalues],
                Int32(Dates.value(Date(2024, 1, 1)) - Dates.value(Date(1970, 1, 1))),
            ),
            (
                Arrow.AC.Field(
                    "d64",
                    Arrow.AC.DateType(Arrow.AC.MILLISECOND_DATE);
                    nullable=false,
                ),
                Any[Date(2024, 1, 1) for _ = 1:nvalues],
                Int64(1_704_067_200_000),
            ),
            (
                Arrow.AC.Field(
                    "timestamp",
                    Arrow.AC.TimestampType(Arrow.AC.SECOND, nothing);
                    nullable=false,
                ),
                Any[DateTime(1970, 1, 1, 0, 0, 1) for _ = 1:nvalues],
                Int64(1),
            ),
            (
                Arrow.AC.Field(
                    "time",
                    Arrow.AC.TimeType(Arrow.AC.SECOND, 32);
                    nullable=false,
                ),
                Any[Time(1) for _ = 1:nvalues],
                Int64(3_600),
            ),
            (
                Arrow.AC.Field(
                    "duration",
                    Arrow.AC.DurationType(Arrow.AC.NANOSECOND);
                    nullable=false,
                ),
                Any[isodd(i) ? Second(1) : Millisecond(1_000) for i = 1:nvalues],
                Int64(1_000_000_000),
            ),
        )
        membership_cases = (temporal_cases[1], temporal_cases[2], temporal_cases[5])
        needed = Arrow.AC._materializedvectorbytes(Union{Int32,Int64}, nvalues)
        for (field, values, expected) in membership_cases
            Arrow._lowermembership(field, values, nothing) # warm the closed kernel
            @test (@allocated Arrow._lowermembership(field, values, nothing)) <= needed
            exact = Arrow.AllocationBudget(needed)
            good, lowered = Arrow._lowermembership(field, values, exact)
            @test good && eltype(lowered) == Union{Int32,Int64}
            @test all(==(expected), lowered)
            @test Arrow._remaining(exact) == 0
            @test_throws Arrow.AllocationLimitError Arrow._lowermembership(
                field,
                values,
                Arrow.AllocationBudget(needed - 1),
            )
        end

        # Retained-schema column construction hoists the same token through a
        # function barrier. Vector{Any} inputs allocate only the final column,
        # not one dynamically dispatched scalar box per row.
        retainedneeded = Arrow.AC._materializedvectorbytes(Union{Missing,Int64}, nvalues)
        for (field, values, expected) in temporal_cases
            Arrow._retainedstorage(field.type, values, "temporal")
            @test (@allocated Arrow._retainedstorage(field.type, values, "temporal")) <=
                  retainedneeded
            @test all(
                ==(Int64(expected)),
                Arrow._retainedstorage(field.type, values, "temporal"),
            )
        end

        # Calendar periods cannot enter the membership kernel, and an integer
        # has no timestamp public representation: neither lowers, and the
        # membership path never inspects or rebuilds the caller's container.
        timestampfield = Arrow.AC.Field(
            "timestamp",
            Arrow.AC.TimestampType(Arrow.AC.MICROSECOND, nothing);
            nullable=false,
        )
        @test Arrow._exactfacadevalue(
            Arrow._facadetoken(timestampfield.type),
            _InterruptingInteger(),
        ) === nothing
        for (field, values) in ((temporal_cases[5][1], Any[Month(1)]),)
            good, original = Arrow._lowermembership(field, values, nothing)
            @test !good
            @test original === values
        end
        secondsfield = Arrow.AC.Field(
            "seconds",
            Arrow.AC.DurationType(Arrow.AC.SECOND);
            nullable=false,
        )
        finervalues = Any[Millisecond(1_000)]
        untouched = Arrow.AllocationBudget(0)
        good, original = Arrow._lowermembership(secondsfield, finervalues, untouched)
        @test !good
        @test original === finervalues
        @test Arrow._remaining(untouched) == 0
        rawvalues = Any[_InterruptingInteger()]
        good, original = Arrow._lowermembership(timestampfield, rawvalues, nothing)
        @test !good
        @test original === rawvalues

        # Set has a separate hashing path. Its conservative Dict-style charge
        # must accept N, reject N-1, and cover the full unique-key allocation.
        setvalues = Set(Date(2024, 1, 1) + Day(i) for i = 0:(nvalues - 1))
        setneeded =
            512 +
            4 *
            nvalues *
            (Base.elsize(Vector{Union{Int32,Int64}}) + Base.elsize(Vector{Nothing}) + 16)
        Arrow._lowermembership(temporal_cases[1][1], setvalues, nothing)
        @test (@allocated Arrow._lowermembership(
            temporal_cases[1][1],
            setvalues,
            nothing,
        )) <= setneeded
        exact = Arrow.AllocationBudget(setneeded)
        good, lowered = Arrow._lowermembership(temporal_cases[1][1], setvalues, exact)
        @test good
        firstday = Int32(Dates.value(Date(2024, 1, 1) - Date(1970, 1, 1)))
        @test lowered == Set{Union{Int32,Int64}}(firstday .+ Int32.(0:(nvalues - 1)))
        @test Arrow._remaining(exact) == 0
        @test_throws Arrow.AllocationLimitError Arrow._lowermembership(
            temporal_cases[1][1],
            setvalues,
            Arrow.AllocationBudget(setneeded - 1),
        )
    end

    @testset "exact range-fetch worker budgets" begin
        ranges = NTuple{2,Int64}[(0, 1), (2, 1), (4, 1), (6, 1)]
        V(T, n) = Arrow.AC._materializedvectorbytes(T, n)
        O(n) = Arrow.AC._materializedobjectbytes(n)
        needed =
            V(NTuple{2,Int64}, 4) +
            4 * V(UInt8, 1) +
            V(Vector{UInt8}, 4) +
            2 * 4096 +
            O(sizeof(Arrow._SpanQueue)) +
            V(Arrow.AC.BufferSlice, 4) +
            O(sizeof(Arrow.AC.ReleaseCell)) +
            4 * O(sizeof(Arrow.AC.OwnerRegion)) +
            2 * V(Int64, 4)
        function fetch(cap)
            source = _ConcurrentSource(collect(UInt8, 0:15), 8)
            handle = Arrow.SourceFile(
                source;
                limits=Arrow.Limits(max_concurrent_reads=2),
                coalesce_gap=0,
            )
            return Arrow._fetchspans(handle, ranges, 0; budget=Arrow.AllocationBudget(cap)),
            source
        end
        spans, source = fetch(needed)
        @test [Arrow.AC.slicebytes(slice) for slice in spans.slices] == [[0x00], [0x02], [0x04], [0x06]]
        @test (@atomic source.issued) == 4
        @test_throws Arrow.AllocationLimitError fetch(needed - 1)

        prefix = V(NTuple{2,Int64}, 4) + 4 * V(UInt8, 1) + V(Vector{UInt8}, 4)
        source = _ConcurrentSource(collect(UInt8, 0:15), 8)
        handle = Arrow.SourceFile(
            source;
            limits=Arrow.Limits(max_concurrent_reads=2),
            coalesce_gap=0,
        )
        err = try
            Arrow._fetchspans(
                handle,
                ranges,
                0;
                budget=Arrow.AllocationBudget(prefix + 2 * 4096 - 1),
            )
            nothing
        catch e
            e
        end
        @test err isa Arrow.AllocationLimitError
        @test occursin("range-fetch workers", sprint(showerror, err))
        @test (@atomic source.issued) == 0
    end

    @testset "ranged scan uses one cumulative exact budget" begin
        io = IOBuffer()
        Arrow.write(
            io,
            Tables.partitioner([
                (x=Int64[1, 2, 3, 4], y=["a", "b", "c", "d"]),
                (x=Int64[5, 6, 7, 8], y=["e", "f", "g", "h"]),
            ]),
        )
        bytes = take!(io)
        scan = Tables.Scan(select=(:x,), filter=Tables.col(:x) > 2, offset=1, limit=2)
        function ranged(cap)
            handle = Arrow.SourceFile(_BytesSource(bytes); tailbytes=64, coalesce_gap=0)
            budget = Arrow.AllocationBudget(cap)
            footer = Arrow._rangedfooter(handle, budget)
            bound = Arrow._compilehandlescan(scan, footer.fields, budget)
            return Arrow._applyscan(handle, bound, footer, budget), budget
        end
        probe_limit = Int64(1_000_000_000)
        got, probe = ranged(probe_limit)
        @test got.x == [4, 5]
        needed = probe_limit - Arrow._remaining(probe)
        exactgot, exact = ranged(needed)
        @test exactgot.x == [4, 5]
        @test Arrow._remaining(exact) == 0
        @test_throws Arrow.AllocationLimitError ranged(needed - 1)
    end

    @testset "the allocation limit error" begin
        io = IOBuffer()
        Arrow.write(
            io,
            Tables.partitioner([(x=Int64[1, 2],), (x=Int64[3, 4],)]);
            file=false,
        )
        bytes = take!(io)
        # The displayed error names the limit and how to raise it.
        err = try
            Arrow.Table(bytes; limits=Arrow.Limits(max_total_allocated_bytes=64))
        catch e
            e
        end
        @test err isa Arrow.AllocationLimitError
        shown = sprint(showerror, err)
        @test occursin(err.msg, shown)
        @test occursin("limits=Arrow.Limits(max_allocation_ratio=", shown)
        @test occursin("max_total_allocated_bytes=", shown)
    end

    @testset "mmap path and release!" begin
        path = tempname()
        try
            Arrow.write(path, (x=collect(Int64, 1:10),))

            # Before parsing succeeds, the path reader owns the map. A parse
            # error must revoke it instead of leaving the source file locked.
            region = Arrow.AC.mmapregion(path)
            @test_throws Arrow.AC.ValidationError Arrow._readmappedfile(
                region,
                Arrow.Limits(max_metadata_bytes=0),
                nothing,
            )
            @test_throws InvalidStateException Arrow.AC.sliceptr(
                Arrow.AC.BufferSlice(region, 0, 1),
            )

            t = Arrow.Table(path)   # mmap by default for ARROW1 files (not on Windows)
            x = t.x
            @test x == 1:10
            Arrow.release!(t)
            @test_throws InvalidStateException t.x
            @test x == 1:10
        finally
            ispath(path) && rm(path)
        end
    end

    @testset "substrings and generic vectors convert" begin
        io = IOBuffer()
        subs = split("alpha,beta,gamma", ",")
        Arrow.write(io, (s=subs, r=1:3); file=false)
        t = Arrow.Table(take!(io))
        @test t.s == ["alpha", "beta", "gamma"]
        @test t.r == [1, 2, 3]
    end

    @testset "structs of named tuples" begin
        io = IOBuffer()
        Arrow.write(io, (st=[(a=1, b="x"), (a=2, b="y")],); file=false)
        t = Arrow.Table(take!(io))
        @test t.st == [(a=1, b="x"), (a=2, b="y")]
        @test eltype(t.st) == @NamedTuple{a::Int64, b::Arrow.DataStrings.DataString}

        Row = @NamedTuple{a::Int64, b::Union{Missing,String}}
        nullable = Union{Missing,Row}[(a=1, b="x"), missing, (a=3, b=missing)]
        expected = Any[(a=1, b="x"), missing, (a=3, b=missing)]
        for file in (false, true)
            io = IOBuffer()
            Arrow.write(io, (st=nullable,); file)
            bytes = take!(io)
            field =
                only((file ? Arrow.readfile(bytes) : Arrow.readstream(bytes)).schema.fields)
            @test field.type isa Arrow.AC.StructType
            @test field.nullable
            @test !field.children[1].nullable
            @test field.children[2].nullable
            @test isequal(Arrow.Table(bytes).st, expected)
        end

        parts = Tables.partitioner((
            (st=Union{Missing,Row}[missing],),
            (st=Union{Missing,Row}[(a=2, b="visible")],),
        ))
        io = IOBuffer()
        Arrow.write(io, parts; file=false)
        @test isequal(Arrow.Table(take!(io)).st, Any[missing, (a=2, b="visible")])
    end

    @testset "partition drift is refused, not misbound" begin
        io = IOBuffer()
        @test_throws ArgumentError Arrow.write(
            io,
            Tables.partitioner([
                (left=Int64[1], right=Int64[10]),
                (right=Int64[20], left=Int64[2]),
            ]);
            file=false,
        )
        @test_throws ArgumentError Arrow.write(
            io,
            Tables.partitioner([(x=Int64[1],), (y=Int64[2],)]);
            file=false,
        )
        # The schema comes from the first partition: a later Int32 column
        # widens into its int64 field; a Float64 column cannot.
        @test_throws ArgumentError Arrow.write(
            io,
            Tables.partitioner([(x=Int64[1],), (x=Float64[2.5],)]);
            file=false,
        )
        bytes = take!(
            Arrow.write(
                IOBuffer(),
                Tables.partitioner([(x=Int64[1],), (x=Int32[2],)]);
                file=false,
            ),
        )
        @test isequal(Arrow.Table(bytes).x, [1, 2])
        @test eltype(Arrow.Table(bytes).x) === Union{Missing,Int64}
    end

    @testset "schema is the authority for facade eltypes" begin
        io = IOBuffer()
        Arrow.write(
            io,
            (s=Union{Missing,String}["a", "b"], m=Union{Missing,String}[missing, missing]);
            file=false,
        )
        t = Arrow.Table(take!(io))
        @test eltype(t.s) == Union{Missing,DataString}
        @test eltype(t.m) == Union{Missing,DataString}
        # all-missing columns round-trip as their DECLARED type
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)
        t2 = Arrow.Table(take!(io2))
        @test eltype(t2.m) == Union{Missing,DataString}
        @test isequal(t2.m, [missing, missing])
        # zero-row typed columns, including via a scan with no matches
        io3 = IOBuffer()
        Arrow.write(io3, (x=Int64[1],); file=false)
        b3 = take!(io3)
        t3 = Arrow.Table(b3; scan=Tables.Scan(filter=Tables.colcmp(==, Tables.col(:x), 99)))
        @test eltype(t3.x) == Int64 && isempty(t3.x)
    end

    @testset "typed schemas keep rowtable working; wide schemas stay stored" begin
        # At or below _MAX_TYPED_SCHEMA_FIELDS the schema is typed, so
        # NamedTuple materializers accept it.
        io = IOBuffer()
        Arrow.write(
            io,
            (a=Int64[1, 2, 3], b=Union{Missing,String}["x", missing, "z"]);
            file=false,
        )
        t = Arrow.Table(take!(io))
        sch = Tables.schema(t)
        @test !(sch isa Tables.Schema{nothing,nothing})
        @test sch.names == (:a, :b)
        @test sch.types == (Int64, Union{Missing,DataString})
        rows = Tables.rowtable(t)
        @test rows isa Vector{NamedTuple{(:a, :b),Tuple{Int64,Union{Missing,DataString}}}}
        @test [r.a for r in rows] == [1, 2, 3]
        @test isequal([r.b for r in rows], ["x", missing, "z"])
        # Above the threshold the schema keeps names and types in values so
        # compiler work cannot scale with input names; Tables.jl then refuses
        # NamedTuple rows, pinned here deliberately.
        ncols = Arrow._MAX_TYPED_SCHEMA_FIELDS + 1
        wide = NamedTuple(Symbol("w", i) => Int64[i] for i = 1:ncols)
        wio = IOBuffer()
        Arrow.write(wio, wide; file=false)
        tw = Arrow.Table(take!(wio))
        wsch = Tables.schema(tw)
        @test wsch isa Tables.Schema{nothing,nothing}
        @test length(wsch.names) == ncols
        @test wsch.types == fill(Int64, ncols)
        @test_throws ArgumentError Tables.rowtable(tw)
        widerr = try
            Tables.rowtable(tw)
            nothing
        catch exception
            exception
        end
        @test occursin("input table too wide", sprint(showerror, widerr))
    end

    @testset "temporal scans agree across formats and renames" begin
        data = (
            x=Int64[1, 2, 3],
            date=[Date(2024, 1, 1), Date(2024, 1, 2), Date(2024, 1, 3)],
            stamp=[
                DateTime(1970, 1, 1),
                DateTime(1970, 1, 1, 0, 0, 2),
                DateTime(2001, 9, 9),
            ],
        )
        fio = IOBuffer()
        Arrow.write(fio, data)
        sio = IOBuffer()
        Arrow.write(sio, data; file=false)
        scan = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:date), Date(2024, 1, 3)))
        want = Tables.scan(data, scan)
        for bytes in (take!(fio), take!(sio))
            got = Arrow.Table(bytes; scan=scan)
            @test got.x == want.x
            @test got.date == want.date && eltype(got.date) <: Union{Missing,Date}
            @test got.stamp == want.stamp
        end
        # renamed temporal output still converts
        rio = IOBuffer()
        Arrow.write(rio, data)
        tr = Arrow.Table(take!(rio); scan=Tables.Scan(select=(:date => :d,), limit=1))
        @test tr.d == [Date(2024, 1, 1)]
    end

    @testset "ranged reads carry the schema" begin
        io = IOBuffer()
        Arrow.write(
            io,
            (stamp=[DateTime(2020, 5, 5)],);
            metadata=Dict("origin" => "ranged"),
        )
        fb = take!(io)
        src = _BytesSource(fb)
        t = Arrow.Table(src)
        @test t.stamp == [DateTime(2020, 5, 5)]
        Tms = Durations.Timestamp{Millisecond}
        @test eltype(t.stamp) == Union{Missing,Tms} || eltype(t.stamp) == Tms
        @test DataAPI.metadata(t, "origin") == "ranged"
    end

    @testset "facade rewrite preserves the retained schema" begin
        # Build exotic units through the core writer, then facade-read and
        # facade-rewrite; the logical schema must not drift.
        micros = Union{Missing,Int64}[1, 1001]
        f, d = Arrow.AC.fromjulia("us", micros)
        t_us = Arrow.AC.TimestampType(Arrow.AC.MICROSECOND, nothing)
        d_us = Arrow.AC._arraydata(
            t_us,
            d.len,
            d.buffers,
            0,
            Arrow.AC.ArrayData[],
            nothing,
            d.owner,
            Arrow.AC.nullcount(d),
        )
        f_us = Arrow.AC.Field("us", t_us; nullable=true)
        sch = Arrow.AC.Schema([f_us]; metadata=["k" => "v"])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d_us], 2)])
        t = Arrow.Table(bytes)
        # sub-ms is exact: the counts read back as Timestamp{Microsecond}
        us = Durations.Timestamp{Dates.Microsecond}
        @test isequal(t.us, reinterpret(us, Int64[1, 1001]))
        io = IOBuffer()
        Arrow.write(io, t; file=false)
        rt = Arrow.Table(take!(io))
        rsch = getfield(rt, :schema)
        @test rsch.fields[1].type isa Arrow.AC.TimestampType
        @test rsch.fields[1].type.unit == Arrow.AC.MICROSECOND
        @test DataAPI.metadata(rt, "k") == "v"   # schema metadata carried
        @test isequal(rt.us, reinterpret(us, Int64[1, 1001]))
        # dictionary columns round-trip as dictionaries, multi-partition,
        # file format (one shared pool, no replacement refusal)
        io2 = IOBuffer()
        Arrow.write(
            io2,
            Tables.partitioner([
                (d=Arrow.DictEncode(["a", "b"]),),
                (d=Arrow.DictEncode(["b", "c"]),),
            ]);
            file=true,
        )
        t2 = Arrow.Table(take!(io2))
        @test t2.d == ["a", "b", "b", "c"]
        sch2 = getfield(t2, :schema)
        @test sch2.fields[1].type isa Arrow.AC.DictionaryType
        io3 = IOBuffer()
        Arrow.write(io3, t2; file=true)
        t3 = Arrow.Table(take!(io3))
        @test getfield(t3, :schema).fields[1].type isa Arrow.AC.DictionaryType
        @test t3.d == ["a", "b", "b", "c"]
    end

    @testset "aliased extension columns rewrite" begin
        # Selecting one column twice shares its retained child fields; the
        # rewrite still needs one Field object per schema position.
        io = IOBuffer()
        Arrow.write(io, (u=[Base.UUID(UInt128(i)) for i = 1:3],))
        t = Arrow.Table(take!(io); scan=Tables.Scan(select=(:u, :u => :v)))
        out = IOBuffer()
        Arrow.write(out, t)
        back = Arrow.Table(take!(out))
        @test back.u == back.v == t.u
    end

    @testset "zero-column row counts survive" begin
        # A zero-column three-row batch built at the core level: the facade
        # read must preserve the count, and a facade round-trip must carry
        # it back out (Table knows its row count even with no columns).
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        t = Arrow.Table(bytes)
        @test Tables.rowcount(t) == 3
        @test isempty(Tables.columnnames(t))
        io = IOBuffer()
        Arrow.write(io, t; file=false)
        t2 = Arrow.Table(take!(io))
        @test Tables.rowcount(t2) == 3
        @test length(t2) == 0
        @test isempty(collect(t2))
    end

    @testset "a single-batch primitive column is materialized once" begin
        # One 8 MB output vector, not a second public copy of it.
        bytes = Arrow.tobuffer((a=collect(Int64, 1:1_000_000),)) |> take!
        @test _tableallocs(bytes) < 12_000_000
        nb =
            Arrow.tobuffer((
                a=Union{Missing,Float64}[isodd(i) ? missing : 1.0 for i = 1:1_000_000],
            )) |> take!
        # Julia 1.10 boxes in the per-element null punch, a separate cost.
        VERSION >= v"1.12" && @test _tableallocs(nb) < 14_000_000
        # An uncompressed write stages the 8 MB column in the message body,
        # the batch message, and the output buffer, and no more often.
        path = tempname()
        try
            @test _writeallocs(path, (a=collect(Int64, 1:1_000_000),)) < 30_000_000
        finally
            rm(path; force=true)
        end
    end

    @testset "Table iterates its columns like other Tables.AbstractColumns" begin
        # length counts columns, so iteration, values, and end indexing see
        # every column exactly once whatever the row count.
        t = Arrow.Table(Arrow.tobuffer((a=Int64[1, 2, 3, 4], b=["w", "x", "y", "z"])))
        @test length(t) == 2
        @test Tables.rowcount(t) == 4
        @test collect(t) == [[1, 2, 3, 4], ["w", "x", "y", "z"]]
        @test values(t) == collect(t)
        @test t[end] == ["w", "x", "y", "z"]
        wide = Arrow.Table(Arrow.tobuffer((a=Int64[1], b=["x"], c=[1.5])))
        @test length(collect(wide)) == 3
        @test [c[1] for c in wide] == Any[1, "x", 1.5]
        empty = Arrow.Table(Arrow.tobuffer((a=Int64[],)))
        @test length(empty) == 1
        @test !isempty(empty)
        @test Tables.rowcount(empty) == 0
    end

    @testset "DataAPI defaults and selectors" begin
        io = IOBuffer()
        Arrow.write(io, (x=Int64[1],); file=false, colmetadata=Dict(:x => Dict("u" => "1")))
        t = Arrow.Table(take!(io))
        @test DataAPI.metadata(t, "absent", "fallback") == "fallback"
        @test DataAPI.colmetadata(t, 1, "u") == "1"
        @test DataAPI.colmetadata(t, :x, "nope", :d) == :d
        @test collect(first.(DataAPI.colmetadatakeys(t))) == [:x]

        # Listing every column's metadata (what DataFrame(table) does) is
        # linear in the column count, not quadratic.
        n = 3000
        wide = NamedTuple(Symbol("w", i) => Int64[i] for i = 1:n)
        meta = Dict(Symbol("w", i) => ["k" => string(i)] for i = 1:10:n)
        wio = IOBuffer()
        Arrow.write(wio, wide; file=false, colmetadata=meta)
        tw = Arrow.Table(take!(wio))
        listed = collect(DataAPI.colmetadatakeys(tw))
        @test length(listed) == length(meta)
        @test DataAPI.colmetadata(tw, :w2991, "k") == "2991"
        @test minimum(@elapsed(collect(DataAPI.colmetadatakeys(tw))) for _ = 1:3) < 0.2
    end

    @testset "temporal scans preserve cross-type predicate semantics" begin
        data = (
            x=Int64[1, 2, 3],
            d32=[Date(1970, 1, 1), Date(1970, 1, 2), Date(1970, 1, 3)],
            ts=[DateTime(2020, 1, 1), DateTime(2020, 1, 2), DateTime(2020, 1, 3)],
        )
        fio = IOBuffer()
        Arrow.write(fio, data)
        fb = take!(fio)
        sio = IOBuffer()
        Arrow.write(sio, data; file=false)
        sb = take!(sio)
        cases = [
            # Date32 vs midnight DateTime: cross-type equality holds
            Tables.Scan(
                select=(:x,),
                filter=Tables.colcmp(==, Tables.col(:d32), DateTime(1970, 1, 2)),
            ),
            # Timestamp vs Date
            Tables.Scan(
                select=(:x,),
                filter=Tables.colcmp(==, Tables.col(:ts), Date(2020, 1, 2)),
            ),
            # raw integer vs a temporal column: never equal in public domain
            Tables.Scan(select=(:x,), filter=Tables.colcmp(==, Tables.col(:d32), 1)),
            # non-midnight DateTime vs Date32: no exact representation
            Tables.Scan(
                select=(:x,),
                filter=Tables.colcmp(==, Tables.col(:d32), DateTime(1970, 1, 2, 12)),
            ),
        ]
        for scan in cases
            want = Tables.scan(data, scan)
            for bytes in (fb, sb)
                got = Arrow.Table(bytes; scan=scan)
                @test isequal(got.x, want.x)
            end
        end
    end

    @testset "temporal pushdown preserves the complete public domain" begin
        function rawtemporal(t, values)
            data = Arrow.AC.ArrayData(
                t,
                length(values),
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(values)];
                nullcount=0,
            )
            field = Arrow.AC.Field("v", t; nullable=false)
            schema = Arrow.AC.Schema([field])
            batch = Arrow.AC.RecordBatch(schema, [data], length(values))
            return field, Arrow.writefile(schema, [batch])
        end
        function assertmatches(bytes, scan)
            full = Arrow.Table(copy(bytes))
            want = Tables.scan(full, scan)
            got = Arrow.Table(copy(bytes); scan=scan)
            @test isequal(got.v, Tables.getcolumn(Tables.columns(want), :v))
            @test Tables.rowcount(got) == Tables.rowcount(Tables.columns(want))
        end

        # Time predicates stay public. A stored time of day outside [0, 24h)
        # has no exact Dates.Time (which wraps modulo 24 hours), so reads
        # refuse it instead of aliasing it onto another time.
        let (type, values, literal) =
                (Arrow.AC.TimeType(Arrow.AC.SECOND, 32), Int32[0, 86_399], Time(0))
            field, bytes = rawtemporal(type, values)
            for scan in (
                Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), literal)),
                Tables.Scan(filter=Tables.colin(Tables.col(:v), (literal,))),
            )
                @test Arrow._ScanPlan(scan, [field]).storage === nothing
                assertmatches(bytes, scan)
                @test Tables.rowcount(Arrow.Table(copy(bytes); scan=scan)) == 1
            end
        end
        for (type, values) in (
            (Arrow.AC.TimeType(Arrow.AC.SECOND, 32), Int32[0, 86_400]),
            (Arrow.AC.TimeType(Arrow.AC.MILLISECOND, 32), Int32[-1, 0]),
            (Arrow.AC.TimeType(Arrow.AC.MICROSECOND, 64), Int64[0, typemax(Int64)]),
            (Arrow.AC.TimeType(Arrow.AC.NANOSECOND, 64), Int64[86_400 * 10 ^ 9, 0]),
        )
            field, bytes = rawtemporal(type, values)
            @test_throws Arrow.ValidationError Arrow.Table(copy(bytes))
            scan = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), Time(0)))
            @test_throws Arrow.ValidationError Arrow.Table(copy(bytes); scan=scan)
        end

        # Second-unit timestamps used to wrap-alias extreme counts into one
        # DateTime; as Timestamp{Second} every count is a distinct public
        # value, so equality lowers and matches exactly one row.
        let field_bytes = rawtemporal(
                Arrow.AC.TimestampType(Arrow.AC.SECOND, nothing),
                Int64[0, Int64(1) << 61],
            )
            field, bytes = field_bytes
            for scan in (
                Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), DateTime(1970, 1, 1))),
                Tables.Scan(filter=Tables.colin(Tables.col(:v), (DateTime(1970, 1, 1),))),
            )
                @test Arrow._ScanPlan(scan, [field]).storage !== nothing
                assertmatches(bytes, scan)
                @test Tables.rowcount(Arrow.Table(copy(bytes); scan=scan)) == 1
            end
        end

        # Date64's millisecond epoch addition is one-to-one but wraps Int64
        # ordering: equality may lower; ordered comparisons stay public. A
        # millisecond TIMESTAMP reads as Timestamp{Millisecond} — a total
        # bijection — so both of its comparisons lower.
        let field_bytes = rawtemporal(
                Arrow.AC.DateType(Arrow.AC.MILLISECOND_DATE),
                Int64[typemax(Int64), 0],
            )
            field, bytes = field_bytes
            eqscan =
                Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), DateTime(1970, 1, 1)))
            @test Arrow._ScanPlan(eqscan, [field]).storage !== nothing
            assertmatches(bytes, eqscan)
            ordered =
                Tables.Scan(filter=Tables.colcmp(<, Tables.col(:v), DateTime(1970, 1, 1)))
            @test Arrow._ScanPlan(ordered, [field]).storage === nothing
            assertmatches(bytes, ordered)
        end
        let field_bytes = rawtemporal(
                Arrow.AC.TimestampType(Arrow.AC.MILLISECOND, nothing),
                Int64[typemax(Int64), 0],
            )
            field, bytes = field_bytes
            for op in (==, <)
                scan = Tables.Scan(
                    filter=Tables.colcmp(op, Tables.col(:v), DateTime(1970, 1, 1)),
                )
                @test Arrow._ScanPlan(scan, [field]).storage !== nothing
                assertmatches(bytes, scan)
            end
        end

        # A coarser Duration literal converts into the column's unit without
        # touching stored values. A finer literal can overflow while Julia
        # promotes a stored public value, so it must stay on the public plan.
        duration = Arrow.AC.DurationType(Arrow.AC.SECOND)
        field, bytes = rawtemporal(duration, Int64[60])
        coarse = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), Minute(1)))
        @test Arrow._ScanPlan(coarse, [field]).storage !== nothing
        assertmatches(bytes, coarse)
        coarsein = Tables.Scan(filter=Tables.colin(Tables.col(:v), (Minute(1),)))
        @test Arrow._ScanPlan(coarsein, [field]).storage !== nothing
        assertmatches(bytes, coarsein)
        finer = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), Millisecond(60_000)))
        @test Arrow._ScanPlan(finer, [field]).storage === nothing
        assertmatches(bytes, finer)
        finerin = Tables.Scan(filter=Tables.colin(Tables.col(:v), (Millisecond(60_000),)))
        @test Arrow._ScanPlan(finerin, [field]).storage === nothing
        assertmatches(bytes, finerin)

        field, bytes = rawtemporal(duration, Int64[typemin(Int64)])
        overflow =
            Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), Millisecond(-1_000_000)))
        @test Arrow._ScanPlan(overflow, [field]).storage === nothing
        @test_throws InexactError Tables.scan(Arrow.Table(copy(bytes)), overflow)
        @test_throws InexactError Arrow.Table(copy(bytes); scan=overflow)

        # Scan planning must never execute a permissive conversion hook. An
        # integer literal has no timestamp public representation, so the plan
        # stays public and the hook is never consulted on either plan.
        field, bytes =
            rawtemporal(Arrow.AC.TimestampType(Arrow.AC.MICROSECOND, nothing), Int64[7])
        literal = _ScanHookInteger()
        hookscan = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:v), literal))
        _SCAN_HOOK_CALLS[] = 0
        @test Arrow._ScanPlan(hookscan, [field]).storage === nothing
        @test _SCAN_HOOK_CALLS[] == 0
        @test isempty(Arrow.Table(copy(bytes); scan=hookscan).v)
        @test _SCAN_HOOK_CALLS[] == 0
    end

    @testset "retained rewrite is schema identity" begin
        # Non-nullable temporal descriptors stay non-nullable; Date64 works.
        vals = Int64[0, 86_400_000]
        t64 = Arrow.AC.DateType(Arrow.AC.MILLISECOND_DATE)
        d64 = Arrow.AC._arraydata(
            t64,
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)],
            0,
            Arrow.AC.ArrayData[],
            nothing,
            nothing,
            0,
        )
        fld = Arrow.AC.Field("d", t64; nullable=false)
        sch = Arrow.AC.Schema([fld])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d64], 2)])
        t = Arrow.Table(bytes)
        @test t.d == [DateTime(1970, 1, 1), DateTime(1970, 1, 2)]
        io = IOBuffer()
        Arrow.write(io, t; file=false)
        rt = getfield(Arrow.Table(take!(io)), :schema)
        @test rt.fields[1].type isa Arrow.AC.DateType
        @test rt.fields[1].type.unit == Arrow.AC.MILLISECOND_DATE
        @test rt.fields[1].nullable == false
        # Retained dictionary identity: index width and ordered survive.
        pool = ["a", "b"]
        pf, pd = Arrow.AC.fromjulia("d", pool)
        dt = Arrow.AC.DictionaryType(Arrow.AC.IntType(8, true), pf.type, true)
        idx = Int8[0, 1, 0]
        dd = Arrow.AC.ArrayData(
            dt,
            3,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(idx)];
            dictionary=pd,
            nullcount=0,
        )
        df = Arrow.AC.Field("d", dt; nullable=false)
        dsch = Arrow.AC.Schema([df])
        dbytes = Arrow.writestream(dsch, [Arrow.AC.RecordBatch(dsch, [dd], 3)])
        dt2 = Arrow.Table(dbytes)
        io2 = IOBuffer()
        Arrow.write(io2, dt2; file=false)
        rsch = getfield(Arrow.Table(take!(io2)), :schema)
        rdt = rsch.fields[1].type
        @test rdt isa Arrow.AC.DictionaryType
        @test rdt.indextype.bits == 8 && rdt.ordered == true
        @test rsch.fields[1].nullable == false
    end

    @testset "replaced facade columns are refused" begin
        io = IOBuffer()
        Arrow.write(io, (d=[Date(2024, 1, 1)], p=Arrow.DictEncode(["x"])))
        t = Arrow.Table(take!(io))
        broken = Arrow.Table(
            getfield(t, :names),
            AbstractVector[Int64[100], Int64[7]],
            getfield(t, :lookup),
            getfield(t, :schema),
            Arrow.AC.OwnerRegion[],
            1,
        )
        io2 = IOBuffer()
        @test_throws ArgumentError Arrow.write(io2, broken; file=false)
    end

    @testset "type overrides and renamed schemas" begin
        io = IOBuffer()
        Arrow.write(
            io,
            (x=Union{Missing,Int64}[1, missing], d=[Date(2024, 1, 1), Date(2024, 1, 2)]),
        )
        fb = take!(io)
        t = Arrow.Table(
            fb;
            scan=Tables.Scan(select=(:x => Union{Missing,Float64}, :d => Date)),
        )
        @test isequal(t.x, Union{Missing,Float64}[1.0, missing])
        @test t.d == [Date(2024, 1, 1), Date(2024, 1, 2)]
        # a renamed output binds ITS OWN field in the stored schema
        tr = Arrow.Table(fb; scan=Tables.Scan(select=(:d => :when,)))
        rsch = getfield(tr, :schema)
        @test length(rsch.fields) == 1
        @test rsch.fields[1].name == "when"
        @test rsch.fields[1].type isa Arrow.AC.DateType
        io3 = IOBuffer()
        Arrow.write(io3, tr; file=false)
        back = getfield(Arrow.Table(take!(io3)), :schema)
        @test back.fields[1].type isa Arrow.AC.DateType
    end

    @testset "empty projections keep row counts" begin
        io = IOBuffer()
        Arrow.write(io, (x=collect(Int64, 1:5),); file=false)
        t = Arrow.Table(take!(io); scan=Tables.Scan(select=(), filter=Tables.col(:x) > 2))
        @test isempty(Tables.columnnames(t))
        @test Tables.rowcount(t) == 3
    end

    @testset "DataAPI missing columns are errors" begin
        io = IOBuffer()
        Arrow.write(io, (x=Int64[1],); file=false)
        t = Arrow.Table(take!(io))
        @test_throws ArgumentError DataAPI.colmetadatakeys(t, :nope)
        @test_throws ArgumentError DataAPI.colmetadata(t, :nope, "k")
    end

    @testset "lowering honors the facade comparison domain" begin
        # Sub-ms timestamps materialize as Timestamp{Microsecond}: a DateTime
        # literal compares by instant, and raw integers are never equal.
        us = Union{Missing,Int64}[1_000_000, 2_000_000]
        f, d = Arrow.AC.fromjulia("us", us)
        t_us = Arrow.AC.TimestampType(Arrow.AC.MICROSECOND, nothing)
        d_us = Arrow.AC._arraydata(
            t_us,
            2,
            d.buffers,
            0,
            Arrow.AC.ArrayData[],
            nothing,
            nothing,
            0,
        )
        sch = Arrow.AC.Schema([Arrow.AC.Field("us", t_us; nullable=true)])
        bytes = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d_us], 2)])
        data = (
            us=Union{Missing,Durations.Timestamp{Microsecond}}[
                reinterpret(Durations.Timestamp{Microsecond}, x) for
                x in Int64[1_000_000, 2_000_000]
            ],
        )
        for (scan, matched) in (
            (
                Tables.Scan(
                    filter=Tables.colcmp(
                        ==,
                        Tables.col(:us),
                        DateTime(1970, 1, 1, 0, 0, 1),
                    ),
                ),
                1,
            ),
            (Tables.Scan(filter=Tables.colcmp(==, Tables.col(:us), 2_000_000)), 0),
        )
            want = Tables.scan(data, scan)
            got = Arrow.Table(bytes; scan=scan)
            @test isequal(got.us, want.us)
            @test length(got.us) == matched
        end
        # Out-of-range and cross-Period literals fall back, matching the
        # authority instead of throwing.
        pdata = (d=[Date(2024, 1, 1)], s=[Second(30)])
        io = IOBuffer()
        Arrow.write(io, pdata)
        pb = take!(io)
        for scan in (
            Tables.Scan(filter=Tables.colcmp(==, Tables.col(:d), Date(6_000_000, 1, 1))),
            Tables.Scan(filter=Tables.colcmp(==, Tables.col(:s), Month(1))),
        )
            want = Tables.scan(pdata, scan)
            got = Arrow.Table(pb; scan=scan)
            @test Tables.rowcount(got) == Tables.rowcount(Tables.columns(want))
        end

        # Exact-conversion failures select the public-domain fallback. Process
        # control and resource failures must still escape immediately.
        ok, original = Arrow._facadetostorage(t_us, big(typemax(Int64)) + 1)
        @test !ok
        @test original == big(typemax(Int64)) + 1
        # Epoch subtraction must reject, not wrap, a DateTime whose exact
        # storage delta falls below Int64.
        extreme = DateTime(Dates.UTM(typemin(Int64)))
        for t in (
            Arrow.AC.DateType(Arrow.AC.MILLISECOND_DATE),
            Arrow.AC.TimestampType(Arrow.AC.MILLISECOND, nothing),
        )
            ok, original = Arrow._facadetostorage(t, extreme)
            @test !ok
            @test original === extreme
        end
        dur_s = Arrow.AC.DurationType(Arrow.AC.SECOND)
        @test_throws InterruptException Arrow._facadetostorage(dur_s, _InterruptingPeriod())
        @test_throws OutOfMemoryError Arrow._facadetostorage(dur_s, _OutOfMemoryPeriod())
    end

    @testset "native Timestamp and ZonedTimestamp columns" begin
        Ts = Durations.Timestamp
        Zts = Durations.ZonedTimestamp
        Denver = Zts{Microsecond,Symbol("America/Denver")}

        # Every zone-naive unit reads as Timestamp{P}; a written DateTime
        # column reads back as Timestamp{Millisecond} with equal instants.
        for (P, unit) in (
            (Second, Arrow.AC.SECOND),
            (Millisecond, Arrow.AC.MILLISECOND),
            (Microsecond, Arrow.AC.MICROSECOND),
            (Nanosecond, Arrow.AC.NANOSECOND),
        )
            col = [Ts{P}(2026, 1, 1), Ts{P}(2026, 1, 2)]
            tp = Arrow.Table(take!(Arrow.tobuffer((c=col,))))
            @test getfield(tp, :schema).fields[1].type ==
                  Arrow.AC.TimestampType(unit, nothing)
            @test eltype(tp.c) == Ts{P} && tp.c == col
        end
        dts = [DateTime(2026, 1, 1), DateTime(2026, 1, 2)]
        tdt = Arrow.Table(take!(Arrow.tobuffer((c=dts,))))
        @test eltype(tdt.c) == Ts{Millisecond}
        @test tdt.c == dts && tdt.c == Ts{Millisecond}.(dts)

        # Fresh writes round-trip zero-conversion at each resolution, with
        # and without missing; the descriptor carries the unit and zone.
        ncol = [Ts{Nanosecond}(2026, 1, 1), Ts{Nanosecond}(2026, 1, 2)]
        zcol = [Denver(Ts{Microsecond}(2026, 1, i), Dates.UTC) for i = 1:4]
        bytes = take!(Arrow.tobuffer((n=ncol, z=zcol[1:2], mz=[zcol[3], missing])))
        t = Arrow.Table(copy(bytes))
        sch = getfield(t, :schema)
        @test sch.fields[1].type == Arrow.AC.TimestampType(Arrow.AC.NANOSECOND, nothing)
        @test sch.fields[2].type ==
              Arrow.AC.TimestampType(Arrow.AC.MICROSECOND, "America/Denver")
        @test eltype(t.n) == Ts{Nanosecond} && t.n == ncol
        @test eltype(t.z) == Denver && t.z == zcol[1:2]
        @test eltype(t.mz) == Union{Missing,Denver}
        @test isequal(collect(t.mz), [zcol[3], missing])
        # retained rewrite keeps unit, zone, and values
        rt = Arrow.Table(take!(Arrow.tobuffer(Arrow.Table(copy(bytes)))))
        @test eltype(rt.z) == Denver && rt.z == zcol[1:2] && rt.n == ncol

        # One column, one zone and one resolution: abstract eltypes refuse.
        mixed = [zcol[1], Durations.astimezone(zcol[2], :UTC)]
        @test_throws ArgumentError Arrow.tobuffer((m=mixed,))
        # mixed naive resolutions PROMOTE to the finer unit and write cleanly
        units = [Ts{Nanosecond}(2026, 1, 1), Ts{Microsecond}(2026, 1, 2)]
        @test eltype(units) == Ts{Nanosecond}
        tu = Arrow.Table(take!(Arrow.tobuffer((u=units,))))
        @test eltype(tu.u) == Ts{Nanosecond} && tu.u == units
        # a declared-abstract column writes as its values' one resolution; a
        # non-Arrow resolution refuses
        ta = Arrow.Table(take!(Arrow.tobuffer((u=Ts[Ts{Nanosecond}(2026)],))))
        @test eltype(ta.u) == Ts{Nanosecond}
        @test_throws ArgumentError Arrow._instantunit(Ts{Dates.Minute})

        # Zoned scans push down every operator; literals convert exactly
        # across zones and units. Zone-free literals refuse on this zone
        # (only UTC takes them), and a raw count matches nothing.
        zbytes = take!(Arrow.tobuffer((z=zcol,)))
        zfield = getfield(Arrow.Table(copy(zbytes)), :schema).fields[1]
        for (filter, want) in (
            (Tables.colcmp(>, Tables.col(:z), zcol[2]), zcol[3:4]),
            (
                Tables.colcmp(==, Tables.col(:z), Durations.astimezone(zcol[2], :UTC)),
                zcol[2:2],
            ),
            (
                Tables.colcmp(
                    <=,
                    Tables.col(:z),
                    Zts{Nanosecond,:UTC}(Ts{Nanosecond}(zcol[2].utc), Dates.UTC),
                ),
                zcol[1:2],
            ),
        )
            scan = Tables.Scan(; filter)
            @test Arrow._ScanPlan(scan, [zfield]).storage !== nothing
            @test collect(Arrow.Table(copy(zbytes); scan).z) == want
        end
        for naive in (DateTime(2026, 1, 2), Ts{Microsecond}(2026, 1, 2))
            scan = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:z), naive))
            @test_throws ArgumentError Arrow.Table(copy(zbytes); scan)
        end
        rawscan =
            Tables.Scan(filter=Tables.colcmp(==, Tables.col(:z), Dates.value(zcol[2])))
        @test Arrow._ScanPlan(rawscan, [zfield]).storage === nothing
        @test isempty(Arrow.Table(copy(zbytes); scan=rawscan).z)
        # a zoned literal against a zone-naive column matches nothing
        nbytes = take!(Arrow.tobuffer((n=ncol,)))
        nscan = Tables.Scan(filter=Tables.colcmp(==, Tables.col(:n), zcol[1]))
        @test isempty(Arrow.Table(copy(nbytes); scan=nscan).n)
        # a nanosecond zoned literal that truncates on a microsecond column
        # has no exact storage form; public evaluation still answers exactly
        sub = Zts{Nanosecond,:UTC}(Ts{Nanosecond}(zcol[2].utc) + Nanosecond(1), Dates.UTC)
        subscan = Tables.Scan(filter=Tables.colcmp(<, Tables.col(:z), sub))
        @test Arrow._ScanPlan(subscan, [zfield]).storage === nothing
        @test collect(Arrow.Table(copy(zbytes); scan=subscan).z) == zcol[1:2]

        # The Arrow spec reads an EMPTY timezone string as zone-naive.
        et = Arrow.AC.TimestampType(Arrow.AC.NANOSECOND, "")
        ed = Arrow.AC.ArrayData(
            et,
            1,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[7])];
            nullcount=0,
        )
        esch = Arrow.AC.Schema([Arrow.AC.Field("e", et; nullable=false)])
        etbl = Arrow.Table(Arrow.writefile(esch, [Arrow.AC.RecordBatch(esch, [ed])]))
        @test eltype(etbl.e) == Ts{Nanosecond}
        @test Dates.value(etbl.e[1]) == 7

        # Empty columns declare the same eltype as populated ones.
        ebytes = take!(Arrow.tobuffer((n=Ts{Nanosecond}[], z=Denver[])))
        et2 = Arrow.Table(ebytes)
        @test eltype(et2.n) == Ts{Nanosecond} && isempty(et2.n)
        @test eltype(et2.z) == Denver && isempty(et2.z)
    end

    @testset "leaf conversion authorities agree" begin
        # `_publicleafscalar` (dynamic rows) and `_postconvert` (whole
        # columns) must convert identically for every convertible leaf.
        AC2 = Arrow.AC
        cases = Any[
            (AC2.DateType(AC2.DAY), Int32[0, 19000]),
            (AC2.DateType(AC2.MILLISECOND_DATE), Int64[0, 86_400_000]),
            (AC2.TimeType(AC2.SECOND, 32), Int32[0, 86_399]),
            (AC2.TimeType(AC2.NANOSECOND, 64), Int64[0, 1]),
            (AC2.DurationType(AC2.MICROSECOND), Int64[-5, 7]),
            (AC2.IntervalType(AC2.YEAR_MONTH), Int32[3, -2]),
            (
                AC2.IntervalType(AC2.MONTH_DAY_NANO),
                [(months=Int32(1), days=Int32(2), nanos=Int64(3))],
            ),
            (AC2.DecimalType(10, 2, 64), Int64[12345, -7]),
            (AC2.TimestampType(AC2.SECOND, nothing), Int64[0, 42]),
            (AC2.TimestampType(AC2.NANOSECOND, nothing), Int64[typemin(Int64), 9]),
            (AC2.TimestampType(AC2.MICROSECOND, "America/Denver"), Int64[0, -1]),
            (AC2.TimestampType(AC2.MILLISECOND, "+07:00"), Int64[5]),
        ]
        for (t, xs) in cases
            converted = Arrow._postconvert(t, collect(xs))
            for (x, c) in zip(xs, converted)
                s = Arrow._publicleafscalar(t, x)
                @test s === c || isequal(s, c) && typeof(s) === typeof(c)
            end
            @test Arrow._isconvertibleleaf(t)
        end
        @test !Arrow._isconvertibleleaf(Arrow.AC.IntType(64, true))
        @test !Arrow._isconvertibleleaf(Arrow.AC.Utf8Type(false))
    end

    @testset "overrides preserve missing and re-infer on rewrite" begin
        io = IOBuffer()
        Arrow.write(io, (x=Union{Missing,Int64}[1, missing],))
        fb = take!(io)
        t = Arrow.Table(fb; scan=Tables.Scan(select=(:x => Float64,)))
        @test isequal(t.x, Union{Missing,Float64}[1.0, missing])
        # rewrite after an override re-infers the column cleanly
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)
        t2 = Arrow.Table(take!(io2))
        @test isequal(t2.x, Union{Missing,Float64}[1.0, missing])
        @test getfield(t2, :schema).fields[1].type isa Arrow.AC.FloatType
    end

    @testset "zero-field counts across all paths" begin
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        bytes = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        for source in (bytes, _BytesSource(bytes))
            t = Arrow.Table(source; scan=Tables.Scan())
            @test Tables.rowcount(t) == 3
            tw = Arrow.Table(source; scan=Tables.Scan(limit=1, offset=1))
            @test Tables.rowcount(tw) == 1
        end
    end

    @testset "override subsumption is a no-op; zero-field filters count" begin
        io = IOBuffer()
        Arrow.write(io, (x=Union{Missing,Int64}[1, 2], s=Union{Missing,String}["a", "b"]))
        fb = take!(io)
        # nullable source, no observed missing: supertype/no-op overrides
        # keep the DECLARED element type, exactly like Tables.scan.
        t = Arrow.Table(fb; scan=Tables.Scan(select=(:x => Int64, :s => AbstractString)))
        @test eltype(t.x) == Union{Missing,Int64}
        @test eltype(t.s) == Union{Missing,DataString}
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)   # rewrites cleanly, metadata intact
        @test isequal(Arrow.Table(take!(io2)).x, [1, 2])
        # zero-field: filters and validation apply
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        tz = Arrow.Table(
            zb;
            scan=Tables.Scan(
                filter=Tables.colcmp(==, Tables.col(:nope), 1),
                validate=false,
            ),
        )
        @test Tables.rowcount(tz) == 0
        @test_throws ArgumentError Arrow.Table(
            zb;
            scan=Tables.Scan(filter=Tables.colcmp(==, Tables.col(:nope), 1)),
        )
        # ranged zero-field honors SourceFile limits
        rfz = Arrow.SourceFile(_BytesSource(zb); limits=Arrow.Limits(max_array_length=2))
        @test_throws Arrow.AC.ValidationError Arrow.Table(rfz)
    end

    @testset "replaced columns are refused before value access" begin
        io = IOBuffer()
        Arrow.write(io, (n=Int64[1], s=["x"], p=Arrow.DictEncode(["x"])))
        t = Arrow.Table(take!(io))
        for (col, bad) in ((:n, ["oops"]), (:s, Int64[1]), (:p, Int64[7]))
            cols = AbstractVector[c for c in getfield(t, :columns)]
            cols[getfield(t, :lookup)[col]] = bad
            broken = Arrow.Table(
                getfield(t, :names),
                cols,
                getfield(t, :lookup),
                getfield(t, :schema),
                Arrow.AC.OwnerRegion[],
                1,
            )
            io2 = IOBuffer()
            @test_throws ArgumentError Arrow.write(io2, broken; file=false)
        end
        # missing into a non-nullable retained dictionary is refused
        pool = ["a"]
        pf, pd = Arrow.AC.fromjulia("d", pool)
        dt = Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true), pf.type, false)
        dd = Arrow.AC.ArrayData(
            dt,
            1,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0])];
            dictionary=pd,
            nullcount=0,
        )
        df = Arrow.AC.Field("d", dt; nullable=false)
        dsch = Arrow.AC.Schema([df])
        dbytes = Arrow.writestream(dsch, [Arrow.AC.RecordBatch(dsch, [dd], 1)])
        td = Arrow.Table(dbytes)
        cols = AbstractVector[Union{Missing,String}[missing]]
        brokend = Arrow.Table(
            getfield(td, :names),
            cols,
            getfield(td, :lookup),
            getfield(td, :schema),
            Arrow.AC.OwnerRegion[],
            1,
        )
        io3 = IOBuffer()
        @test_throws ArgumentError Arrow.write(io3, brokend; file=false)
    end

    @testset "override conversions follow the authority exactly" begin
        io = IOBuffer()
        Arrow.write(io, (a=Int64[1, 2], b=Union{Missing,Int64}[1, 2]))
        fb = take!(io)
        # Real conversions: requested type exact, missing only when observed.
        t = Arrow.Table(
            fb;
            scan=Tables.Scan(select=(:a => Union{Missing,Float64}, :b => Float64)),
        )
        @test eltype(t.a) == Union{Missing,Float64}
        @test eltype(t.b) == Float64
        # Zero-field: true-valued and unmatched-reference filters keep rows.
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        t2 = Arrow.Table(
            zb;
            scan=Tables.Scan(filter=Tables.isnull(Tables.col(:gone)), validate=false),
        )
        @test Tables.rowcount(t2) == 3
        t3 = Arrow.Table(
            zb;
            scan=Tables.Scan(
                filter=Tables.isnull(Tables.col(:gone)),
                validate=false,
                limit=1,
                offset=1,
            ),
        )
        @test Tables.rowcount(t3) == 1
        # List => AbstractVector is a no-op: values, retained field, and
        # metadata all survive. List => Vector copies each row view.
        io4 = IOBuffer()
        Arrow.write(
            io4,
            (l=[[1, 2], [3]],);
            file=false,
            colmetadata=Dict(:l => Dict("k" => "v")),
        )
        lb = take!(io4)
        t4 = Arrow.Table(lb; scan=Tables.Scan(select=(:l => AbstractVector,)))
        @test isequal(t4.l, [[1, 2], [3]])
        rsch = getfield(t4, :schema)
        @test length(rsch.fields) == 1
        @test rsch.fields[1].type isa Arrow.AC.ListType
        @test DataAPI.colmetadata(t4, :l, "k") == "v"
        t5 = Arrow.Table(lb; scan=Tables.Scan(select=(:l => Vector,)))
        @test t5.l == [[1, 2], [3]] && eltype(t5.l) <: Vector
    end

    @testset "zero-field scans hold at the Tables.scan layer too" begin
        sch = Arrow.AC.Schema(Arrow.AC.Field[])
        zb = Arrow.writefile(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        af = Arrow.readfile(zb)
        for (scan, want) in (
            (Tables.Scan(filter=Tables.AlwaysTrue()), 3),
            (Tables.Scan(filter=Tables.AlwaysTrue(), limit=1, offset=1), 1),
            (Tables.Scan(filter=Tables.isnull(Tables.col(:gone)), validate=false), 3),
            (Tables.Scan(filter=Tables.AlwaysFalse()), 0),
        )
            got = Tables.scan(af, scan)
            @test Tables.rowcount(Tables.columns(got)) == want
            rgot = Tables.scan(Arrow.SourceFile(_BytesSource(zb)), scan)
            @test Tables.rowcount(Tables.columns(rgot)) == want
        end
        # The facade path allocates nothing proportional to a hostile count:
        # a tiny file claiming a million rows answers limit=1 instantly.
        big = Arrow.writefile(
            sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 1_000_000)],
        )
        # Warm first: compiling the scan is not part of the per-row cost.
        Arrow.Table(big; scan=Tables.Scan(filter=Tables.AlwaysTrue(), limit=1))
        stats =
            @timed Arrow.Table(big; scan=Tables.Scan(filter=Tables.AlwaysTrue(), limit=1))
        @test Tables.rowcount(stats.value) == 1
        @test stats.bytes < 1_000_000
        # The metadata-only ranged read keeps the reader trust boundary: a
        # corrupted continuation prefix rejects exactly as the full reader
        # rejects it.
        bad = copy(zb)
        bad[65:68] .= 0x00
        @test_throws Arrow.AC.ValidationError Arrow.readfile(copy(bad))
        @test_throws Arrow.AC.ValidationError Tables.scan(
            Arrow.SourceFile(_BytesSource(copy(bad))),
            Tables.Scan(),
        )
        # Header reads share ONE cumulative budget, as Limits documents:
        # many tiny batches refuse under a bound one batch fits.
        many = Arrow.writefile(
            sch,
            [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 1) for _ = 1:200],
        )
        tight = Arrow.Limits(max_total_allocated_bytes=6000)
        @test_throws Arrow.AllocationLimitError Tables.scan(
            Arrow.readfile(copy(many); limits=tight),
            Tables.Scan(),
        )
        @test_throws Arrow.AllocationLimitError Tables.scan(
            Arrow.SourceFile(_BytesSource(copy(many)); limits=tight),
            Tables.Scan(),
        )
        # A zero-field schema declares no dictionary ids: a footer listing a
        # well-framed dictionary block is orphaned, and the metadata-only
        # ranged read rejects it exactly as the full reader does.
        f0 = Arrow.readfile(copy(zb))
        (roff, rmetalen, rbodylen) = f0.recordblocks[1]
        recbytes = zb[(Int(roff) + 1):Int(roff + rmetalen + rbodylen)]
        dataend = Int(roff + rmetalen + rbodylen)
        doctored = copy(zb[1:dataend])
        append!(doctored, recbytes)
        append!(doctored, reinterpret(UInt8, UInt32[Arrow.CONTINUATION, UInt32(0)]))
        fbb = Arrow.FB.Builder(1024)
        schoff = Arrow._metaschema!(fbb, sch, Base.IdDict{Arrow.AC.Field,Int64}(), Int64[])
        Arrow.Meta.footerStartDictionariesVector(fbb, 1)
        Arrow.Meta.createBlock(fbb, Int64(dataend), Int32(rmetalen), rbodylen)
        dictvec = Arrow.FB.endvector!(fbb, 1)
        Arrow.Meta.footerStartRecordBatchesVector(fbb, 1)
        Arrow.Meta.createBlock(fbb, roff, Int32(rmetalen), rbodylen)
        recordvec = Arrow.FB.endvector!(fbb, 1)
        Arrow.Meta.footerStart(fbb)
        Arrow.Meta.footerAddVersion(fbb, Arrow.Meta.MetadataVersion.V5)
        Arrow.Meta.footerAddSchema(fbb, schoff)
        Arrow.Meta.footerAddDictionaries(fbb, dictvec)
        Arrow.Meta.footerAddRecordBatches(fbb, recordvec)
        Arrow.FB.finish!(fbb, Arrow.Meta.footerEnd(fbb))
        ftr = collect(Arrow.FB.finishedbytes(fbb))
        append!(doctored, ftr)
        append!(doctored, reinterpret(UInt8, Int32[Int32(length(ftr))]))
        append!(doctored, Arrow.FILE_MAGIC)
        @test_throws Arrow.AC.ValidationError Arrow.readfile(copy(doctored))
        @test_throws Arrow.AC.ValidationError Tables.scan(
            Arrow.SourceFile(_BytesSource(copy(doctored))),
            Tables.Scan(),
        )
        # Structural binding is unconditional: an unsupported predicate node
        # rejects even with validate=false, on every facade path.
        zs = Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, Arrow.AC.ArrayData[], 3)])
        for source in (zb, zs, _BytesSource(zb))
            @test_throws ArgumentError Arrow.Table(
                source;
                scan=Tables.Scan(filter=Tables.OpNode(:custom, Any[]), validate=false),
            )
        end
    end

    @testset "list columns rewrite after materialization" begin
        io = IOBuffer()
        Arrow.write(io, (l=[[1, 2], Int64[], [3]],); file=false)
        t = Arrow.Table(take!(io))
        io2 = IOBuffer()
        Arrow.write(io2, t; file=false)
        t2 = Arrow.Table(take!(io2))
        @test isequal(t2.l, [Any[1, 2], Any[], Any[3]])
        # empty list column with a retained field keeps descriptor+metadata
        io3 = IOBuffer()
        Arrow.write(
            io3,
            (l=Vector{Int64}[],);
            file=false,
            colmetadata=Dict(:l => Dict("k" => "v")),
        )
        lb = take!(io3)
        t3 = Arrow.Table(lb; scan=Tables.Scan(select=(:l => AbstractVector,)))
        rsch = getfield(t3, :schema)
        @test length(rsch.fields) == 1
        @test rsch.fields[1].type isa Arrow.AC.ListType
        @test DataAPI.colmetadata(t3, :l, "k") == "v"
        # The full transition matrix: empty and nonempty list facades from
        # every input path rewrite cleanly to both output formats — the
        # retained child descriptor supplies the element type observation
        # cannot (zero-row, all-empty-rows, and nested shapes included).
        for rows in (
            Vector{Int64}[],
            [[1, 2], Int64[], [3]],
            [Int64[], Int64[]],
            [[Int64[1, 2]], [Int64[]]],
        )
            iof = IOBuffer()
            Arrow.write(iof, (l=rows,); file=true)
            fbb = take!(iof)
            ios = IOBuffer()
            Arrow.write(ios, (l=rows,); file=false)
            sbb = take!(ios)
            for src in (Arrow.Table(fbb), Arrow.Table(sbb), Arrow.Table(_BytesSource(fbb)))
                for file in (true, false)
                    out = IOBuffer()
                    Arrow.write(out, src; file=file)
                    back = Arrow.Table(take!(out))
                    @test isequal(collect(Any, back.l), collect(Any, rows))
                    bsch = getfield(back, :schema)
                    @test bsch.fields[1].type isa Arrow.AC.ListType
                end
            end
        end
        # An EMPTY real conversion drops the stale source descriptor (the
        # declared facade type decides, exactly as a nonempty column would)
        # and the rewrite re-infers from the public-domain values.
        io5 = IOBuffer()
        Arrow.write(io5, (a=Int64[],); file=false)
        eb = take!(io5)
        tec = Arrow.Table(eb; scan=Tables.Scan(select=(:a => Float64,)))
        @test eltype(Tables.getcolumn(tec, :a)) === Float64
        @test isempty(getfield(tec, :schema).fields)
        out5 = IOBuffer()
        Arrow.write(out5, tec; file=true)
        @test eltype(Arrow.Table(take!(out5)).a) === Float64
        tes = Arrow.Table(lb; scan=Tables.Scan(select=(:l => String,)))
        @test isempty(getfield(tes, :schema).fields)
        # Retained identity is RECURSIVE: names, nullability, metadata, and
        # list WIDTH survive a rewrite at every level (the natural builder
        # only emits small lists — imposition rebuilds retained large
        # offsets), for row-bearing and zero-row columns alike.
        leafd = Arrow.AC.ArrayData(
            Arrow.AC.IntType(64, true),
            3,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[10, 20, 30])],
        )
        innerd = Arrow.AC.ArrayData(
            Arrow.AC.ListType(false),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0, 2, 3])];
            children=[leafd],
        )
        outerd = Arrow.AC.ArrayData(
            Arrow.AC.ListType(true),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[0, 1, 2])];
            children=[innerd],
        )
        leaff = Arrow.AC.Field("item", Arrow.AC.IntType(64, true); nullable=false)
        innerf = Arrow.AC.Field(
            "inner",
            Arrow.AC.ListType(false);
            nullable=true,
            metadata=["ik" => "iv"],
            children=[leaff],
        )
        outerf = Arrow.AC.Field(
            "l",
            Arrow.AC.ListType(true);
            nullable=false,
            metadata=["ok" => "ov"],
            children=[innerf],
        )
        nsch = Arrow.AC.Schema([outerf])
        for nrows in (2, 0)
            data =
                nrows == 0 ?
                Arrow.AC.ArrayData(
                    Arrow.AC.ListType(true),
                    0,
                    [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[0])];
                    children=[
                        Arrow.AC.ArrayData(
                            Arrow.AC.ListType(false),
                            0,
                            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0])];
                            children=[
                                Arrow.AC.ArrayData(
                                    Arrow.AC.IntType(64, true),
                                    0,
                                    [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[])],
                                ),
                            ],
                        ),
                    ],
                ) : outerd
            nb = Arrow.writestream(
                nsch,
                [Arrow.AC.RecordBatch(nsch, Arrow.AC.ArrayData[data], nrows)],
            )
            tsrc = Arrow.Table(nb)
            outn = IOBuffer()
            Arrow.write(outn, tsrc; file=false)
            tback = Arrow.Table(take!(outn))
            @test isequal(tback.l, tsrc.l)
            fb1 = getfield(tback, :schema).fields[1]
            @test fb1.type == Arrow.AC.ListType(true)
            @test fb1.metadata !== nothing && ("ok" => "ov") in fb1.metadata
            fi = fb1.children[1]
            @test fi.name == "inner" && fi.type == Arrow.AC.ListType(false)
            @test fi.nullable
            @test fi.metadata !== nothing && ("ik" => "iv") in fi.metadata
            @test fi.children[1].name == "item"
        end
        # Every vector-materializing descriptor decides keep/drop the same
        # for empty and nonempty columns: Binary rows are Vector{UInt8}, so
        # => Vector subsumes and keeps the field either way.
        for (n, offs, bytes) in
            ((2, Int32[0, 2, 3], UInt8[1, 2, 3]), (0, Int32[0], UInt8[]))
            bd = Arrow.AC.ArrayData(
                Arrow.AC.BinaryType(false),
                n,
                [
                    Arrow.AC.BufferSlice(),
                    Arrow.AC._databuffer(offs),
                    Arrow.AC._databuffer(bytes),
                ],
            )
            bf = Arrow.AC.Field(
                "b",
                Arrow.AC.BinaryType(false);
                nullable=false,
                metadata=["bk" => "bv"],
            )
            bsch = Arrow.AC.Schema([bf])
            bb = Arrow.writestream(
                bsch,
                [Arrow.AC.RecordBatch(bsch, Arrow.AC.ArrayData[bd], n)],
            )
            tb = Arrow.Table(bb; scan=Tables.Scan(select=(:b => AbstractVector,)))
            bsch2 = getfield(tb, :schema)
            @test length(bsch2.fields) == 1
            @test bsch2.fields[1].type isa Arrow.AC.BinaryType
            @test DataAPI.colmetadata(tb, :b, "bk") == "bv"
        end
        # The declared row domain covers EVERY closed materializer (the
        # Core _value methods are the authority), including Field-aware
        # compositions; empty and nonempty columns decide keep/drop alike.
        @test Arrow._declaredbasetype(Arrow.AC.ListViewType(false)) === Vector{Any}
        @test Arrow._declaredbasetype(Arrow.AC.DecimalType(9, 2, 32)) ===
              DataDecimals.Decimal32{2}
        @test Arrow._declaredbasetype(Arrow.AC.DecimalType(38, 2, 128)) ===
              DataDecimals.Decimal128{2}
        @test Arrow._declaredbasetype(Arrow.AC.IntervalType(Arrow.AC.YEAR_MONTH)) ===
              Durations.Duration
        @test Arrow._declaredbasetype(Arrow.AC.IntervalType(Arrow.AC.DAY_TIME)) ===
              Durations.Duration
        dleaf = Arrow.AC.Field("v", Arrow.AC.BinaryType(false); nullable=true)
        druns = Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true); nullable=false)
        dref = Arrow.AC.Field(
            "d",
            Arrow.AC.DictionaryType(
                Arrow.AC.IntType(32, true),
                Arrow.AC.RunEndEncodedType(),
                false,
            );
            nullable=true,
            children=[druns, dleaf],
        )
        @test Arrow._declaredeltype(dref) === Union{Missing,Vector{UInt8}}
        u1 = Arrow.AC.Field(
            "u",
            Arrow.AC.UnionType(Arrow.AC.DenseMode, Int8[0]);
            nullable=false,
            children=[Arrow.AC.Field("a", Arrow.AC.IntType(64, true); nullable=false)],
        )
        @test Arrow._declaredeltype(u1) === Int64
        # The declared domain equals the ACTUAL container type: convertible
        # leaves declare their PUBLIC type at every depth, and a
        # multi-child union declares the mixed-population join.
        dtf = Arrow.AC.Field("values", Arrow.AC.DateType(Arrow.AC.DAY); nullable=true)
        rnf = Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true); nullable=false)
        reef0 = Arrow.AC.Field(
            "r",
            Arrow.AC.RunEndEncodedType();
            nullable=false,
            children=[rnf, dtf],
        )
        @test Arrow._declaredeltype(reef0) === Union{Missing,Date}
        @test Arrow._declaredeltype(reef0, false) === Union{Missing,Int32}
        huf = Arrow.AC.Field(
            "u",
            Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]);
            nullable=false,
            children=[
                Arrow.AC.Field("a", Arrow.AC.IntType(64, true); nullable=false),
                Arrow.AC.Field("b", Arrow.AC.Utf8Type(false); nullable=false),
            ],
        )
        @test Arrow._declaredeltype(huf) === Any
        # REE<Date32> end-to-end: public Date rows; a `=> Date` override is
        # a subsuming no-op that keeps the retained field for empty and
        # nonempty columns alike.
        for (n, runs, vals) in
            ((3, Int32[2, 3], Int32[19000, 19001]), (0, Int32[], Int32[]))
            vd = Arrow.AC.ArrayData(
                Arrow.AC.DateType(Arrow.AC.DAY),
                length(vals),
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)],
            )
            rd = Arrow.AC.ArrayData(
                Arrow.AC.IntType(32, true),
                length(runs),
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(runs)],
            )
            reed = Arrow.AC.ArrayData(
                Arrow.AC.RunEndEncodedType(),
                n,
                Arrow.AC.BufferSlice[];
                children=[rd, vd],
                nullcount=0,
            )
            rsch0 = Arrow.AC.Schema([reef0])
            rb = Arrow.writestream(
                rsch0,
                [Arrow.AC.RecordBatch(rsch0, Arrow.AC.ArrayData[reed], n)],
            )
            if n > 0
                got = Arrow.Table(rb).r
                @test eltype(got) == Union{Missing,Date}
                @test got == Date.(
                    Dates.UTD.(Int64[19000, 19000, 19001] .+ Dates.value(Date(1970, 1, 1))),
                )
            end
            tre = Arrow.Table(rb; scan=Tables.Scan(select=(:r => Date,)))
            rsch2 = getfield(tre, :schema)
            @test length(rsch2.fields) == 1
            @test rsch2.fields[1].type isa Arrow.AC.RunEndEncodedType
        end
        # Mixed heterogeneous union: rows are a small Union of the children's
        # types, so a union target covering them keeps the field, empty or not.
        for n in (2, 0)
            tid = Arrow.AC._databuffer(Int8[0, 1][1:n])
            uad = Arrow.AC.ArrayData(
                Arrow.AC.IntType(64, true),
                n,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[5, 6][1:n])],
            )
            ubd = Arrow.AC.ArrayData(
                Arrow.AC.Utf8Type(false),
                n,
                [
                    Arrow.AC.BufferSlice(),
                    Arrow.AC._databuffer(Int32[0, 1, 2][1:(n + 1)]),
                    Arrow.AC._databuffer(UInt8[0x61, 0x62][1:n]),
                ],
            )
            uud = Arrow.AC.ArrayData(
                Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]),
                n,
                [tid];
                children=[uad, ubd],
                nullcount=0,
            )
            usch0 = Arrow.AC.Schema([huf])
            ub0 = Arrow.writestream(
                usch0,
                [Arrow.AC.RecordBatch(usch0, Arrow.AC.ArrayData[uud], n)],
            )
            tuo = Arrow.Table(
                ub0;
                scan=Tables.Scan(select=(:u => Union{Integer,AbstractString},)),
            )
            @test length(getfield(tuo, :schema).fields) == 1
        end
        # End-to-end: Decimal64 rows are shared decimals — an => Real override
        # keeps the retained field for empty and nonempty columns alike.
        for (n, vals) in ((2, Int64[1234, 5678]), (0, Int64[]))
            dd = Arrow.AC.ArrayData(
                Arrow.AC.DecimalType(10, 2, 64),
                n,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(vals)],
            )
            df = Arrow.AC.Field(
                "dec",
                Arrow.AC.DecimalType(10, 2, 64);
                nullable=false,
                metadata=["dk" => "dv"],
            )
            dsch = Arrow.AC.Schema([df])
            db = Arrow.writestream(
                dsch,
                [Arrow.AC.RecordBatch(dsch, Arrow.AC.ArrayData[dd], n)],
            )
            td = Arrow.Table(db; scan=Tables.Scan(select=(:dec => Real,)))
            dsch2 = getfield(td, :schema)
            @test length(dsch2.fields) == 1
            @test dsch2.fields[1].type isa Arrow.AC.DecimalType
            @test DataAPI.colmetadata(td, :dec, "dk") == "dv"
        end
        # A coefficient outside the declared precision reads as stored, as in
        # Arrow C++, at the top level and inside a list.
        let t = Arrow.AC.DecimalType(3, 2, 64)
            D = DataDecimals.Decimal{3,2,Int64}
            dd = Arrow.AC.ArrayData(
                t,
                2,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[100, 1000])],
            )
            dsch = Arrow.AC.Schema([Arrow.AC.Field("dec", t; nullable=false)])
            db = Arrow.writestream(dsch, [Arrow.AC.RecordBatch(dsch, [dd], 2)])
            @test Arrow.Table(db).dec == reinterpret(D, Int64[100, 1000])
            item = Arrow.AC.Field("item", t; nullable=false)
            lt = Arrow.AC.ListType(false)
            ld = Arrow.AC.ArrayData(
                lt,
                1,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32[0, 2])];
                children=[dd],
            )
            lsch =
                Arrow.AC.Schema([Arrow.AC.Field("l", lt; nullable=false, children=[item])])
            lb = Arrow.writestream(lsch, [Arrow.AC.RecordBatch(lsch, [ld], 1)])
            @test only(Arrow.Table(lb).l) == reinterpret(D, Int64[100, 1000])
        end
        # Identity-strict at every depth: a replaced list column refuses,
        # never coerces (convert would turn true into Int64(1)).
        io6 = IOBuffer()
        Arrow.write(io6, (l=[[1, 2]],); file=false)
        trl = Arrow.Table(take!(io6))
        cols = AbstractVector[c for c in getfield(trl, :columns)]
        cols[1] = Any[Any[true, false]]
        swapped = Arrow.Table(
            getfield(trl, :names),
            cols,
            getfield(trl, :lookup),
            getfield(trl, :schema),
            Arrow.AC.OwnerRegion[],
            1,
        )
        @test_throws ArgumentError Arrow.write(IOBuffer(), swapped; file=false)
    end

    @testset "typed read routing serves every valid layout" begin
        # NullType columns (claim = Missing) and homogeneous unions (claim
        # joins to a concrete type Core refuses) must ride the dynamic
        # path.
        nd = Arrow.AC.ArrayData(Arrow.AC.NullType(), 2, Arrow.AC.BufferSlice[]; nullcount=2)
        nf = Arrow.AC.Field("n", Arrow.AC.NullType(); nullable=true)
        nsch = Arrow.AC.Schema([nf])
        nb =
            Arrow.writestream(nsch, [Arrow.AC.RecordBatch(nsch, Arrow.AC.ArrayData[nd], 2)])
        @test isequal(Arrow.Table(nb).n, [missing, missing])
        tid = Arrow.AC._databuffer(Int8[0, 1])
        ua = Arrow.AC.ArrayData(
            Arrow.AC.IntType(64, true),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[10, 20])],
        )
        ub_ = Arrow.AC.ArrayData(
            Arrow.AC.IntType(64, true),
            2,
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[30, 40])],
        )
        uud = Arrow.AC.ArrayData(
            Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]),
            2,
            [tid];
            children=[ua, ub_],
            nullcount=0,
        )
        uf = Arrow.AC.Field(
            "u",
            Arrow.AC.UnionType(Arrow.AC.SparseMode, Int8[0, 1]);
            nullable=false,
            children=[
                Arrow.AC.Field("a", Arrow.AC.IntType(64, true); nullable=false),
                Arrow.AC.Field("b", Arrow.AC.IntType(64, true); nullable=false),
            ],
        )
        usch = Arrow.AC.Schema([uf])
        ubz = Arrow.writestream(
            usch,
            [Arrow.AC.RecordBatch(usch, Arrow.AC.ArrayData[uud], 2)],
        )
        @test Arrow.Table(ubz).u == [10, 40]
        # Dictionary- and REE-wrapped unions route dynamic too.
        @test !Arrow._typedroutable(
            Arrow.AC.Field(
                "r",
                Arrow.AC.RunEndEncodedType();
                nullable=false,
                children=[
                    Arrow.AC.Field("run_ends", Arrow.AC.IntType(32, true); nullable=false),
                    uf,
                ],
            ),
        )
        @test !Arrow._typedroutable(
            Arrow.AC.Field(
                "d",
                Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true), uf.type, false);
                nullable=false,
                children=collect(Arrow.AC.Field, uf.children),
            ),
        )
    end

    @testset "errors are clean" begin
        @test_throws ArgumentError Arrow.write(IOBuffer(), Tables.partitioner(NamedTuple[]))

        # Nested facade conversions recurse: SubString and Date fields of a
        # NamedTuple write the same descriptors they write at the top level.
        TextRow = @NamedTuple{text::SubString{String}}
        text = SubString("nested", 1, 4)
        for values in (TextRow[(text=text,)], Union{Missing,TextRow}[(text=text,), missing])
            t = Arrow.Table(take!(Arrow.tobuffer((st=values,))))
            @test only(skipmissing(t.st)).text == "nest"
        end

        DateRow = @NamedTuple{day::Date}
        for values in (
            DateRow[(day=Date(2024, 1, 1),)],
            Union{Missing,DateRow}[(day=Date(2024, 1, 1),), missing],
        )
            t = Arrow.Table(take!(Arrow.tobuffer((st=values,))))
            @test only(skipmissing(t.st)).day == Date(2024, 1, 1)
        end

        # A declared timezone becomes a Symbol type parameter; one that no
        # Symbol can hold, or past the per-name bound, is invalid input.
        for zone in ("UT\0C", "Z"^5000)
            zt = Arrow.AC.TimestampType(Arrow.AC.MILLISECOND, zone)
            zd = Arrow.AC.ArrayData(
                zt,
                1,
                [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int64[0])],
            )
            zs = Arrow.AC.Schema([Arrow.AC.Field("t", zt; nullable=false)])
            zb = Arrow.writestream(zs, [Arrow.AC.RecordBatch(zs, [zd], 1)])
            @test_throws Arrow.ValidationError Arrow.Table(zb)
        end

        # Footer Block extents near typemax(Int64) are invalid input, not an
        # arithmetic overflow, on every read path.
        io = IOBuffer()
        Arrow.write(io, (x=Int64[1, 2],); file=true)
        good = take!(io)
        (offset, metalen, bodylen) = Arrow.readfile(copy(good)).recordblocks[1]
        block = reinterpret(UInt8, [offset, Int64(metalen), bodylen])
        at = findlast(block, good)
        @test at !== nothing
        for (slot, forged) in ((0, typemax(Int64) - 7), (16, typemax(Int64) - 7))
            bad = copy(good)
            bad[first(at) .+ (slot:(slot + 7))] .= reinterpret(UInt8, [forged])
            @test_throws Arrow.ValidationError Arrow.Table(bad)
            @test_throws Arrow.ValidationError Arrow.Table(
                _BytesSource(bad);
                scan=Tables.Scan(select=(:x,)),
            )
        end
    end

    @testset "advisory nullability: nulls under a non-nullable field read" begin
        # Field nullability is advisory at the reader tier (the semantic
        # validation IPC applies accepts such data, as the reference
        # implementation does). A non-nullable field whose batch holds a
        # null therefore READS, as a missing-capable column, on every
        # entry: the facade, the facade's scan path, and the direct handle
        # scan — for primitives and through a run-end-encoded wrapper.
        AC = Arrow.AC
        f = AC.Field("x", AC.IntType(64, true); nullable=false)
        d = AC.ArrayData(
            AC.IntType(64, true),
            2,
            [AC._databuffer(UInt8[0b10]), AC._databuffer(Int64[0, 7])];
            nullcount=1,
        )
        sch = AC.Schema([f])
        bytes = Arrow.writefile(sch, AC.RecordBatch[AC.RecordBatch(sch, [d], 2)])
        @test isequal(Arrow.Table(bytes).x, [missing, 7])
        @test eltype(Arrow.Table(bytes).x) === Union{Missing,Int64}
        @test isequal(Arrow.Table(bytes; scan=Tables.Scan()).x, [missing, 7])
        for handle in (Arrow.readfile(bytes), Arrow.SourceFile(_BytesSource(bytes)))
            got = Tables.scan(handle, Tables.Scan())
            @test isequal(got.x, [missing, 7])
            @test eltype(got.x) === Union{Missing,Int64}
        end
        rf, rd = AC.fromjulia("run_ends", Int32[1, 2])
        vf0, vd = AC.fromjulia("values", Union{Missing,Int64}[missing, 7])
        vf = AC.Field("values", vf0.type; nullable=false)
        t = AC.RunEndEncodedType()
        ref = AC.Field("r", t; children=[rf, vf])
        red = AC.ArrayData(t, 2, AC.BufferSlice[]; children=[rd, vd], nullcount=0)
        sch2 = AC.Schema([ref])
        bytes2 = Arrow.writefile(sch2, AC.RecordBatch[AC.RecordBatch(sch2, [red], 2)])
        @test isequal(Arrow.Table(bytes2).r, [missing, 7])
        for handle in (Arrow.readfile(bytes2), Arrow.SourceFile(_BytesSource(bytes2)))
            @test isequal(Tables.scan(handle, Tables.Scan()).r, [missing, 7])
        end
        # a dictionary column: a null-free pool under a non-nullable field is
        # Missing-free; a pool holding a null (the schema's one flag cannot
        # declare it) reads missing-capable
        df0, dd = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, 0])
        df = AC.Field(
            "d",
            df0.type;
            nullable=false,
            children=collect(AC.Field, df0.children),
        )
        schd = AC.Schema([df])
        bytesd = Arrow.writefile(schd, AC.RecordBatch[AC.RecordBatch(schd, [dd], 3)])
        @test Arrow.Table(bytesd).d == ["lo", "hi", "lo"]
        @test eltype(Arrow.Table(bytesd).d) === DataString
        pf0, pd = AC.fromjulia_dict("d", Union{Missing,String}["lo", missing], [0, 1, 0])
        pf = AC.Field(
            "d",
            pf0.type;
            nullable=false,
            children=collect(AC.Field, pf0.children),
        )
        schp = AC.Schema([pf])
        bytesp = Arrow.writefile(schp, AC.RecordBatch[AC.RecordBatch(schp, [pd], 3)])
        @test isequal(Arrow.Table(bytesp).d, ["lo", missing, "lo"])
        @test eltype(Arrow.Table(bytesp).d) === Union{Missing,DataString}
        # a conforming batch keeps the declared, Missing-free type
        cf, cd = AC.fromjulia("c", Int64[1, 2])
        schc = AC.Schema([cf])
        bytesc = Arrow.writefile(schc, AC.RecordBatch[AC.RecordBatch(schc, [cd], 2)])
        @test eltype(Arrow.Table(bytesc).c) === Int64
    end

    @testset "empty column types do not depend on batch structure" begin
        # A schema-only source and a source with one zero-row batch give
        # every column the same element type, for the closed scalars and
        # through the transparent wrappers.
        AC = Arrow.AC
        cases = [
            (
                "bin",
                AC.BinaryType(false),
                () -> AC.ArrayData(
                    AC.BinaryType(false),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int32[0]), AC._databuffer(UInt8[])];
                    nullcount=0,
                ),
                DataStrings.DataBytes,
            ),
            (
                "fsb",
                AC.FixedSizeBinaryType(2),
                () -> AC.ArrayData(
                    AC.FixedSizeBinaryType(2),
                    0,
                    [AC.BufferSlice(), AC._databuffer(UInt8[])];
                    nullcount=0,
                ),
                NTuple{2,UInt8},
            ),
            (
                "d128",
                AC.DecimalType(10, 2, 128),
                () -> AC.ArrayData(
                    AC.DecimalType(10, 2, 128),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int128[])];
                    nullcount=0,
                ),
                DataDecimals.Decimal{10,2,Int128},
            ),
            (
                "d64",
                AC.DecimalType(10, 2, 64),
                () -> AC.ArrayData(
                    AC.DecimalType(10, 2, 64),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int64[])];
                    nullcount=0,
                ),
                DataDecimals.Decimal{10,2,Int64},
            ),
            (
                "iym",
                AC.IntervalType(AC.YEAR_MONTH),
                () -> AC.ArrayData(
                    AC.IntervalType(AC.YEAR_MONTH),
                    0,
                    [AC.BufferSlice(), AC._databuffer(Int32[])];
                    nullcount=0,
                ),
                Durations.Duration,
            ),
            (
                "imdn",
                AC.IntervalType(AC.MONTH_DAY_NANO),
                () -> AC.ArrayData(
                    AC.IntervalType(AC.MONTH_DAY_NANO),
                    0,
                    [AC.BufferSlice(), AC._databuffer(UInt8[])];
                    nullcount=0,
                ),
                Durations.Duration,
            ),
        ]
        for (name, t, mk, want) in cases
            f = AC.Field(name, t; nullable=false)
            sch = AC.Schema([f])
            so = Arrow.Table(Arrow.writefile(sch, AC.RecordBatch[]))
            zr = Arrow.Table(
                Arrow.writefile(sch, AC.RecordBatch[AC.RecordBatch(sch, [mk()], 0)]),
            )
            @test eltype(Tables.getcolumn(so, 1)) === want
            @test eltype(Tables.getcolumn(zr, 1)) === want
            @test length(Tables.getcolumn(so, 1)) == 0 == length(Tables.getcolumn(zr, 1))
        end
        # transparent REE over Int64: the values child's type, both ways
        rf, rd = AC.fromjulia("run_ends", Int32[])
        vf, vd = AC.fromjulia("values", Int64[])
        t = AC.RunEndEncodedType()
        ref = AC.Field("r", t; children=[rf, vf])
        sch = AC.Schema([ref])
        so = Arrow.Table(Arrow.writefile(sch, AC.RecordBatch[]))
        red = AC.ArrayData(t, 0, AC.BufferSlice[]; children=[rd, vd], nullcount=0)
        zr =
            Arrow.Table(Arrow.writefile(sch, AC.RecordBatch[AC.RecordBatch(sch, [red], 0)]))
        @test eltype(so.r) === Int64 === eltype(zr.r)
        # composites: the typed row, both ways
        lf, ld = AC.fromjulia("l", Vector{Int64}[])
        schl = AC.Schema([lf])
        Row = Arrow.ListValue{Int64}
        @test eltype(Arrow.Table(Arrow.writefile(schl, AC.RecordBatch[])).l) === Row
        @test eltype(
            Arrow.Table(
                Arrow.writefile(schl, AC.RecordBatch[AC.RecordBatch(schl, [ld], 0)]),
            ).l,
        ) === Row
    end

    @testset "type mappings: bytes, 128-bit ints, UUIDs, periods, abstract columns" begin
        rt(t; kw...) = Arrow.Table(take!(Arrow.write(IOBuffer(), t; kw...)))
        fieldof(t) = getfield(t, :schema).fields[1]
        label(f) = Arrow._fieldmetavalue(f, "ARROW:extension:name")
        # Byte vectors are Binary; byte tuples FixedSizeBinary.
        for col in ([UInt8[1, 2], UInt8[]], [codeunits("ab"), codeunits("")])
            t = rt((; x=col))
            @test fieldof(t).type == Arrow.AC.BinaryType(false)
            @test t.x == collect.(col)
        end
        long = collect(0x01:0x20)
        bv = DataStrings.BytesVector{DataBytes}(
            [
                DataStrings.inline_payload(UInt8[1, 2], 1, 2),
                DataStrings.inline_payload(UInt8[], 1, 0),
                DataStrings.view_payload(long, 1, 32, 0, 0),
            ],
            [long],
        )
        for (sv, type) in
            ((false, Arrow.AC.BinaryType(false)), (true, Arrow.AC.ViewType(false)))
            t = rt((; x=bv); stringview=sv)
            @test fieldof(t).type == type
            @test t.x == [UInt8[1, 2], UInt8[], collect(0x01:0x20)]
        end
        t = rt((; x=[(0x01, 0x02, 0x03), (0x04, 0x05, 0x06)]))
        @test fieldof(t).type == Arrow.AC.FixedSizeBinaryType(3)
        @test t.x == [(0x01, 0x02, 0x03), (0x04, 0x05, 0x06)]
        # UUID: canonical arrow.uuid, big-endian bytes; fresh and retained.
        u = Union{Missing,UUID}[UUID("00112233-4455-6677-8899-aabbccddeeff"), missing]
        t = rt((; x=u))
        f = fieldof(t)
        @test f.type == Arrow.AC.FixedSizeBinaryType(16) && label(f) == "arrow.uuid"
        bytes = Arrow.readfile(take!(Arrow.write(IOBuffer(), (; x=u))))[1].columns[1]
        @test Arrow.AC.loadat(Arrow.AC.rolebuffer(bytes, Arrow.AC.DATA), UInt8, 0) == 0x00
        @test Arrow.AC.loadat(Arrow.AC.rolebuffer(bytes, Arrow.AC.DATA), UInt8, 15) == 0xff
        @test isequal(collect(t.x), u) && isequal(collect(rt(t).x), u)
        # 128-bit integers: FixedSizeBinary(16) with a Julia label.
        for (T, name) in ((Int128, "JuliaLang.Int128"), (UInt128, "JuliaLang.UInt128"))
            col = T[typemin(T), one(T), typemax(T)]
            t = rt((; x=col))
            @test label(fieldof(t)) == name
            @test eltype(t.x) == T && t.x == col && rt(t).x == col
        end
        # Larger periods: exact durations and year-month intervals that read back
        # as the same period type.
        for (col, type) in (
            ([Day(2), Day(-1)], Arrow.AC.DurationType(Arrow.AC.SECOND)),
            ([Week(1)], Arrow.AC.DurationType(Arrow.AC.SECOND)),
            ([Hour(3)], Arrow.AC.DurationType(Arrow.AC.SECOND)),
            ([Minute(-5)], Arrow.AC.DurationType(Arrow.AC.SECOND)),
            ([Month(14)], Arrow.AC.IntervalType(Arrow.AC.YEAR_MONTH)),
            ([Year(-2)], Arrow.AC.IntervalType(Arrow.AC.YEAR_MONTH)),
        )
            t = rt((; x=col))
            @test fieldof(t).type == type
            @test eltype(t.x) == eltype(col) && t.x == col && rt(t).x == col
        end
        @test_throws OverflowError Arrow.tobuffer((; x=[Week(typemax(Int64))]))
        # Abstract columns narrow to their values' types.
        t = rt((; x=Real[1, 2]))
        @test eltype(t.x) == Int64 && t.x == [1, 2]
        for col in (Any[1, "a", 2.5, missing], Real[1, 2.5])
            t = rt((; x=col))
            @test fieldof(t).type isa Arrow.AC.UnionType
            @test isequal(collect(t.x), col)
        end
        t = rt((; x=Any[[1], ["a", "b"]]))
        @test isequal(collect(t.x), [[1], ["a", "b"]])
        @test_throws ArgumentError Arrow.tobuffer((;
            x=Any[
                Int8(1),
                Int16(2),
                Int32(3),
                Int64(4),
                UInt8(5),
                UInt16(6),
                UInt32(7),
                UInt64(8),
                9.0,
            ]
        ))
    end

    @testset "canonical extension reads" begin
        AC = Arrow.AC
        ext(name) = ["ARROW:extension:name" => name, "ARROW:extension:metadata" => ""]
        function onecolumn(f, d; kw...)
            sch = AC.Schema([f])
            return Arrow.Table(Arrow.writefile(sch, [AC.RecordBatch(sch, [d])]); kw...)
        end
        _, d8 = AC.fromjulia("x", Int8[0, 1, 7])
        t = onecolumn(
            AC.Field("x", AC.IntType(8, true); nullable=false, metadata=ext("arrow.bool8")),
            d8,
        )
        @test eltype(t.x) == Bool && t.x == [false, true, true]
        # A rewrite keeps the label and stores the values as 0 and 1.
        bytes = take!(Arrow.write(IOBuffer(), t))
        f = only(Arrow.readfile(bytes).schema.fields)
        @test f.type == AC.IntType(8, true)
        @test Dict(f.metadata)["ARROW:extension:name"] == "arrow.bool8"
        @test Arrow.Table(bytes).x == [false, true, true]
        # On uint8 storage the label is not arrow.bool8's: the column reads,
        # and rewrites, as its stored bytes.
        _, du8 = AC.fromjulia("x", UInt8[0, 254])
        f = AC.Field("x", AC.IntType(8, false); nullable=false, metadata=ext("arrow.bool8"))
        empty!(Arrow._WARNED_EXTENSIONS)
        t = @test_logs (:warn, r"unsupported ARROW:extension:name") onecolumn(f, du8)
        @test t.x == [0x00, 0xfe]
        @test Arrow.Table(take!(Arrow.write(IOBuffer(), t))).x == [0x00, 0xfe]
        _, ds = AC.fromjulia("x", ["{\"a\": 1}", "[]"])
        for name in ("arrow.json", "arrow.opaque")
            f = AC.Field("x", AC.Utf8Type(false); nullable=false, metadata=ext(name))
            t = @test_logs min_level = Logging.Warn onecolumn(f, ds)
            @test t.x == ["{\"a\": 1}", "[]"]
        end
        # An unknown extension warns once per process.
        name = "example.unknown.$(time_ns())"
        f = AC.Field("x", AC.Utf8Type(false); nullable=false, metadata=ext(name))
        @test_logs (:warn, r"unsupported ARROW:extension:name") onecolumn(f, ds)
        @test_logs min_level = Logging.Warn onecolumn(f, ds)
        # Labels that read as their storage read as its columns: strings as
        # a StringVector (or `stringtype`), integers as a view; a rewrite
        # keeps the label.
        for label in ("arrow.json", "arrow.opaque", name)
            f = AC.Field("x", AC.Utf8Type(false); nullable=false, metadata=ext(label))
            t = onecolumn(f, ds)
            @test t.x isa StringVector && eltype(t.x) == DataStrings.DataString
            @test eltype(onecolumn(f, ds; stringtype=String).x) == String
            back = Arrow.readfile(take!(Arrow.write(IOBuffer(), t)))
            @test Dict(only(back.schema.fields).metadata)["ARROW:extension:name"] == label
        end
        # So does a name some code already made a Symbol, when no
        # ArrowTypes.jl `JuliaType` method takes it.
        Symbol(name)
        f = AC.Field("x", AC.Utf8Type(false); nullable=false, metadata=ext(name))
        @test onecolumn(f, ds).x isa StringVector
        _, d32 = AC.fromjulia("x", Int32[1, 2, 3])
        f = AC.Field("x", AC.IntType(32, true); nullable=false, metadata=ext(name))
        t = onecolumn(f, d32)
        @test t.x isa Arrow.Primitive && t.x == [1, 2, 3]
    end

    @testset "time zones: filter literals and the per-read cap" begin
        AC = Arrow.AC
        utc(x...) = Durations.ZonedTimestamp{Dates.Microsecond,:UTC}(x...)
        denver = Durations.ZonedTimestamp{Dates.Microsecond,Symbol("America/Denver")}
        ts = [utc(2019, 12, 31), utc(2020, 1, 1, 12)]
        bytes = take!(Arrow.write(IOBuffer(), (u=ts, d=denver.(ts), n=[1, 2])))
        rows(f) = Arrow.Table(bytes; scan=Tables.Scan(filter=f)).n
        # A zone-free literal on a UTC column is that UTC wall time.
        for lit in (
            Dates.DateTime(2020),
            Dates.Date(2020),
            Durations.Timestamp{Dates.Millisecond}(2020),
            utc(2020),
        )
            @test rows(Tables.colcmp(>=, Tables.col(:u), lit)) == [2]
        end
        @test rows(Tables.colin(Tables.col(:u), [Dates.DateTime(2020, 1, 1, 12)])) == [2]
        @test rows(Tables.colin(Tables.col(:u), Set([Dates.DateTime(2020, 1, 1, 12)]))) ==
              [2]
        # An inexact literal for the column's unit compares exactly.
        later = Durations.Timestamp{Dates.Nanosecond}(2020, 1, 1, 12) + Dates.Nanosecond(1)
        @test isempty(rows(Tables.colcmp(>=, Tables.col(:u), later)))
        # Another zone needs a zoned literal, and the error shows one.
        err = try
            rows(Tables.colcmp(>=, Tables.col(:d), Dates.DateTime(2020)))
        catch e
            e
        end
        @test err isa ArgumentError && occursin(
            "Durations.ZonedTimestamp{Dates.Microsecond,:UTC}(2020, 1, 1)",
            err.msg,
        )
        @test rows(Tables.colcmp(>=, Tables.col(:d), utc(2020))) == [2]
        # One read may bring at most 1,024 zones the process has not seen.
        tag = string(time_ns())
        zoned(n) = AC.Schema([
            AC.Field("t$i", AC.TimestampType(AC.SECOND, "Test/$tag-$n-$i"); nullable=true) for i = 1:n
        ])
        @test_throws Arrow.ValidationError Arrow.Table(
            Arrow.writefile(zoned(1025), AC.RecordBatch[]),
        )
        @test length(
            Tables.columnnames(Arrow.Table(Arrow.writefile(zoned(3), AC.RecordBatch[]))),
        ) == 3
    end

    @testset "read views write back from their own buffers" begin
        AC = Arrow.AC
        n = 3000
        src = (;
            i=[isodd(k) ? missing : k for k = 1:n],
            d=[k % 5 == 0 ? missing : Dates.Date(2020) + Dates.Day(k) for k = 1:n],
            b=[k % 7 == 0 ? missing : iseven(k) for k = 1:n],
            s=[k % 3 == 0 ? missing : repeat("s", k % 40) for k = 1:n],
        )
        t = Arrow.Table(take!(Arrow.write(IOBuffer(), src; batchsize=1000)))
        cols = (; i=t.i, d=t.d, b=t.b, s=t.s)
        # A batch that is one read batch reuses its buffers.
        chunk = first(t.i.arrays)
        _, d = Arrow._constructpart("i", chunk)
        @test AC.sliceptr(d.buffers[2]) == AC.sliceptr(AC.rolebuffer(chunk.owner, AC.DATA))
        for batchsize in (1000, 777, 5000)
            back = Arrow.Table(take!(Arrow.write(IOBuffer(), cols; batchsize)))
            @test all(
                c -> isequal(collect(getproperty(back, c)), getproperty(src, c)),
                keys(src),
            )
        end
        # A string batch read in order is a slice of the read buffer; strings
        # appended one by one (short ones only inline) are copied.
        _, sd = Arrow._constructpart("s", view(t.s, 1:1000))
        @test any(b -> b === AC.rolebuffer(sd, AC.DATA).region.root, t.s.buffers)
        appended = DataStrings.StringVector(src.s)
        for batchsize in (3, 1000)
            back = Arrow.Table(take!(Arrow.write(IOBuffer(), (; s=appended); batchsize)))
            @test isequal(collect(back.s), src.s)
        end
        # An edited value never writes the read buffer's stale bytes: a short
        # value lives in its payload (the buffer keeps the old bytes), a long
        # one moves to an edit buffer.
        rows = view(t.s, 1:1000)
        for (row, value) in ((2, "sx"), (5, "a value longer than twelve bytes"))
            t.s[row] = value
            back = Arrow.Table(take!(Arrow.write(IOBuffer(), (; s=rows))))
            @test back.s[row] == value
            @test isequal(collect(back.s), collect(rows))
        end
    end

    @testset "ntasks: the same bytes on any number of tasks" begin
        n = 40_000
        src = (;
            i=collect(1:n),
            s=[isodd(k) ? "short$k" : "a longer string value $k" for k = 1:n],
            l=[collect(1:(k % 4)) for k = 1:n],
            r=[(a=k, b=string(k)) for k = 1:n],
            d=Arrow.DictEncode([string(k % 7) for k = 1:n]),
        )
        for compress in (nothing, :lz4, :zstd), file in (true, false)
            one = take!(
                Arrow.write(IOBuffer(), src; compress, file, ntasks=1, batchsize=9000),
            )
            many = take!(
                Arrow.write(IOBuffer(), src; compress, file, ntasks=4, batchsize=9000),
            )
            @test one == many
        end
        @test_throws ArgumentError Arrow.write(IOBuffer(), src; ntasks=0)
        # With several failing columns, the error is the first column's.
        for ntasks in (1, 4)
            err = try
                Arrow.write(
                    IOBuffer(),
                    (; a=fill(Any[], 100_000), c=fill(Any[], 100_000));
                    ntasks,
                )
                nothing
            catch e
                e
            end
            @test err isa ArgumentError && startswith(err.msg, "column a (child item)")
        end
    end

    @testset "metadata defaults to the table's DataAPI metadata" begin
        t = _MetaTable(
            (; a=[1, 2], u=[UUIDs.UUID(1), UUIDs.UUID(2)]),
            Dict("source" => "test", "ARROW:schema" => "dropped"),
            Dict(:a => Dict("unit" => "m"), :u => Dict("ARROW:extension:name" => "x.y")),
        )
        back = Arrow.Table(take!(Arrow.write(IOBuffer(), t)))
        @test DataAPI.metadata(back) == Dict("source" => "test")
        @test DataAPI.colmetadata(back, :a, "unit") == "m"
        # The reserved ARROW: keys stay the writer's: u keeps its arrow.uuid label.
        @test DataAPI.colmetadata(back, :u, "ARROW:extension:name") == "arrow.uuid"
        @test back.u == t.columns.u
        # Keywords win over the table's metadata.
        kw = Arrow.Table(take!(Arrow.write(IOBuffer(), t; metadata=["k" => "v"])))
        @test DataAPI.metadata(kw) == Dict("k" => "v")
    end

    @testset "strings that are not UTF-8 are refused, naming the column and row" begin
        bad = String(UInt8[0x61, 0xff])
        msg(t; kw...) =
            try
                Arrow.write(IOBuffer(), t; kw...)
                ""
            catch e
                e isa ArgumentError ? e.msg : sprint(showerror, e)
            end
        utf8msg(at) = "column $at holds a string that is not valid UTF-8"
        @test startswith(msg((; s=["ok", bad])), utf8msg("s row 2"))
        @test occursin("Vector{UInt8}", msg((; s=["ok", bad])))
        @test startswith(msg((; s=["ok", bad]); stringview=true), utf8msg("s row 2"))
        @test startswith(msg((; s=[fill("ok", 5); bad]); batchsize=2), utf8msg("s row 6"))
        parts = Tables.partitioner([(; s=["a"]), (; s=["b", bad])])
        @test startswith(msg(parts), utf8msg("s row 2 of partition 2"))
        @test startswith(msg((; s=[["ok"], ["a", bad]])), utf8msg("s (child item) row 2"))
        @test startswith(
            msg((; s=[(a=1, b="ok"), (a=2, b=bad)])),
            utf8msg("s (child b) row 2"),
        )
        @test startswith(
            msg((; s=[Dict("ok" => 1), Dict(bad => 2)])),
            utf8msg("s (child entries.key) row 2"),
        )
        @test startswith(msg((; s=Arrow.DictEncode(["ok", "ok", bad]))), utf8msg("s row 3"))
        @test startswith(msg((; s=[:ok, Symbol(bad)])), utf8msg("s row 2"))
        # Rows split inside one character: the bytes are UTF-8 together, not apart.
        halves = [String(UInt8[0xc3]), String(UInt8[0xa9])]
        @test startswith(msg((; s=halves)), utf8msg("s row 1"))
        @test msg((; s=["é", "日本", "a", ""])) == ""
        io = IOBuffer()
        @test_throws ArgumentError Arrow.Writer(io) do w
            Arrow.write(w, (; s=["ok"]))
            Arrow.write(w, (; s=[bad]))
        end
        stream = take!(Arrow.write(IOBuffer(), (; s=["ok"]); file=false))
        err = try
            Arrow.append(IOBuffer(stream; read=true, write=true), (; s=["a", bad]))
            nothing
        catch e
            e
        end
        @test err isa ArgumentError && startswith(err.msg, utf8msg("s row 2"))
        # A table read from a file that holds such a string rewrites only as binary.
        f, d = Arrow.AC.fromjulia("s", ["ok", bad])
        sch = Arrow.AC.Schema([f])
        src = Arrow.Table(Arrow.writestream(sch, [Arrow.AC.RecordBatch(sch, [d], 2)]))
        @test startswith(msg(src), utf8msg("s row 2"))
    end

    @testset "errors name the column and say what to do" begin
        msg(t) =
            try
                Arrow.write(IOBuffer(), t)
                ""
            catch e
                sprint(showerror, e)
            end
        @test startswith(
            msg((; mixed=Any[1, 1.0, "x", 'c', :s, 0x01, Int16(2), Int32(3), true])),
            "ArgumentError: column mixed holds values of more than 8 types",
        )
        @test occursin("column m holds Matrix{Int64} values", msg((; m=[[1 2; 3 4]])))
        @test occursin("column a has no values to infer an Arrow type", msg((; a=Any[])))
        @test occursin(
            "column b holds BigInt values, which have no Arrow type",
            msg((; b=[big(1)])),
        )
    end

    @testset "write knobs: batchsize, compresslevel, dictencode, largelists" begin
        AC = Arrow.AC
        nbatches(bytes) = length(Arrow.readfile(bytes).recordblocks)
        types(bytes) = [f.type for f in Arrow.readfile(bytes).schema.fields]
        t = (; a=collect(1:10), s=[isodd(i) ? "x$i" : missing for i = 1:10])
        for (batchsize, n) in ((1, 10), (3, 4), (10, 1), (11, 1))
            bytes = take!(Arrow.write(IOBuffer(), t; batchsize))
            @test nbatches(bytes) == n
            back = Arrow.Table(bytes)
            @test back.a == t.a && isequal(collect(back.s), t.s)
        end
        @test nbatches(take!(Arrow.write(IOBuffer(), (; a=zeros(Int8, (1 << 20) + 1))))) ==
              2
        parts = Tables.partitioner([(; a=collect(1:5)), (; a=Int[]), (; a=[6, 7])])
        bytes = take!(Arrow.write(IOBuffer(), parts; batchsize=2))
        f = Arrow.readfile(bytes)
        @test [f[i].nrows for i = 1:length(f)] == [2, 2, 1, 0, 2]
        @test Arrow.Table(bytes).a == 1:7
        @test_throws ArgumentError Arrow.write(IOBuffer(), t; batchsize=0)
        io = IOBuffer()
        Arrow.Writer(io; batchsize=4) do w
            Arrow.write(w, t)
            Arrow.write(w, t)
        end
        @test nbatches(take!(io)) == 6
        # compression levels change the bytes, not the values
        big = (; a=repeat(collect(1:1000), 50))
        for compress in (:lz4, :zstd)
            sizes = map((1, 9)) do compresslevel
                bytes = take!(Arrow.write(IOBuffer(), big; compress, compresslevel))
                @test Arrow.Table(bytes).a == big.a
                length(bytes)
            end
            @test sizes[1] != sizes[2]
        end
        @test_throws ArgumentError Arrow.write(IOBuffer(), t; compresslevel=3)
        # dictencode: every column, or the named ones
        dt = (; a=[1, 2, 1], s=["x", "y", "x"])
        isdict(bytes) = [ty isa AC.DictionaryType for ty in types(bytes)]
        @test isdict(take!(Arrow.write(IOBuffer(), dt; dictencode=true))) == [true, true]
        @test isdict(take!(Arrow.write(IOBuffer(), dt; dictencode=[:s]))) == [false, true]
        @test isdict(take!(Arrow.write(IOBuffer(), dt; dictencode=["a"]))) == [true, false]
        @test_throws ArgumentError Arrow.write(IOBuffer(), dt; dictencode=[:nope])
        plain = Arrow.Table(take!(Arrow.write(IOBuffer(), dt)))
        bytes = take!(Arrow.write(IOBuffer(), plain; dictencode=[:s]))
        @test isdict(bytes) == [false, true] && Arrow.Table(bytes).s == dt.s
        # 64-bit offsets only on request
        lt = (; s=["a", "bb"], b=[UInt8[1], UInt8[]], l=[[1, 2], [3]])
        @test types(take!(Arrow.write(IOBuffer(), lt))) ==
              [AC.Utf8Type(false), AC.BinaryType(false), AC.ListType(false)]
        bytes = take!(Arrow.write(IOBuffer(), lt; largelists=true))
        @test types(bytes) == [AC.Utf8Type(true), AC.BinaryType(true), AC.ListType(true)]
        back = Arrow.Table(bytes)
        @test back.s == lt.s && back.b == lt.b && back.l == lt.l
    end

    @testset "batches whose offsets would pass Int32 are cut smaller" begin
        AC = Arrow.AC
        # A builder reports the batch and row where 32-bit offsets overflow;
        # the writer cuts every batch at least that long and rebuilds.
        calls = Ref(0)
        parts = [AbstractVector[collect(1:10)]]
        fields, data, rows =
            Arrow._constructbatches(parts, Any[nothing], [10], 4) do cols, _
                calls[] += 1
                calls[] == 1 && throw(Arrow._OffsetOverflow("s", 2, 3))
                return [AC.Field("a", AC.IntType(64, true); nullable=false)],
                [[last(AC.fromjulia("a", collect(c[1]))) for c in cols]]
            end
        @test calls[] == 2
        @test rows == [2, 2, 2, 2, 2]
        # the cut applies from each partition's start, leaving no slivers
        sizes = [4]
        Arrow._shrinkbatches!(
            sizes,
            Arrow._batchplan([10], sizes),
            Arrow._OffsetOverflow("s", 1, 4),
        )
        @test Arrow._batchplan([10], sizes) == [(1, 1:3), (1, 4:6), (1, 7:9), (1, 10:10)]
        # a single value too large for 32-bit offsets says how to write it
        err = try
            Arrow._constructbatches(parts, Any[nothing], [10], 4) do cols, _
                throw(Arrow._OffsetOverflow("s", 1, 1))
            end
        catch e
            e
        end
        @test err isa ArgumentError && occursin("largelists=true", err.msg)
    end

    @testset "DataStrings columns write as utf8, or as Utf8View zero-copy" begin
        # A StringVector's memory IS a Utf8View array: payloads are the
        # views buffer, its byte buffers the variadic data buffers. Build one
        # the way the CSV kernel does (inline ≤12, else a view into buffer 0
        # or the `extra` buffer 1) and check the writer wraps rather than
        # materializes, the file carries Utf8View, and the facade reads it
        # back as a StringVector.
        buf = Vector{UInt8}(codeunits("id,s\n1,abcd\n2,thirteen-byte\n3,\n"))
        extra = Vector{UInt8}(codeunits("she said \"hi\" and left"))
        long1 = first(findfirst(codeunits("thirteen-byte"), buf))
        abcd = first(findfirst(codeunits("abcd"), buf))
        payloads = StringPayload[
            DataStrings.inline_payload(buf, abcd, 4),
            DataStrings.view_payload(buf, long1, 13, 0, long1 - 1),
            DataStrings.PAYLOAD_MISSING,
            DataStrings.view_payload(extra, 1, length(extra), 1, 0),
        ]
        col = StringVector{Union{Missing,DataString}}(payloads, buf, extra)
        expected = ["abcd", "thirteen-byte", missing, "she said \"hi\" and left"]
        # By default the bytes are packed into one utf8 column.
        f, columnparts = Arrow._constructcolumn(:s, AbstractVector[col])
        @test f.type == Arrow.AC.Utf8Type(false) && f.nullable
        io = IOBuffer()
        Arrow.write(io, (id=[1, 2, 3, 4], s=col))
        bytes = take!(io)
        @test Arrow.readfile(bytes).schema.fields[2].type == Arrow.AC.Utf8Type(false)
        @test isequal(Arrow.Table(bytes).s, expected)
        # stringview=true writes the column's own memory as Utf8View.
        f, columnparts = Arrow._constructcolumn(:s, AbstractVector[col]; stringview=true)
        d = only(columnparts)
        @test f.type == Arrow.AC.ViewType(true) && f.nullable
        @test d.buffers[2].region.root === payloads       # views: the payload vector itself
        @test d.buffers[3].region.root === buf
        @test d.buffers[4].region.root === extra
        Arrow.write(io, (id=[1, 2, 3, 4], s=col); stringview=true)
        bytes = take!(io)
        @test Arrow.readfile(bytes).schema.fields[2].type == Arrow.AC.ViewType(true)
        t = Arrow.Table(bytes)
        @test eltype(t.s) === Union{Missing,DataString}
        @test isequal(t.s, expected)
        # a non-nullable column declares non-nullable
        col0 = StringVector{DataString}(payloads[[1, 2]], buf, extra)
        f0, _ = Arrow._constructcolumn(:s, AbstractVector[col0])
        @test !f0.nullable
        Arrow.write(io, (s=col0,))
        @test Arrow.Table(take!(io)).s == ["abcd", "thirteen-byte"]
        # an all-inline column may have ZERO data buffers — the format allows
        # a Utf8View with no variadic buffers, and the wire carries exactly
        # the fixed validity + views pair
        inl = StringVector{DataString}(
            [
                DataStrings.inline_payload(buf, abcd, 4),
                DataStrings.inline_payload(buf, abcd, 2),
            ],
            Vector{Vector{UInt8}}(),
        )
        fi, inlineparts = Arrow._constructcolumn(:s, AbstractVector[inl]; stringview=true)
        di = only(inlineparts)
        @test length(di.buffers) == 2
        Arrow.write(io, (s=inl,); stringview=true)
        bytes0 = take!(io)
        @test Arrow.Table(bytes0).s == ["abcd", "ab"]
        af = Arrow.readfile(bytes0)
        @test af.schema.fields[1].type == Arrow.AC.ViewType(true)
        @test length(af[1].columns[1].buffers) == 2         # no variadic buffers on the wire
    end
end

end # module FacadeTests
