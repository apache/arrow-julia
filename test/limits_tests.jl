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

# Reader allocation budgets: per-batch streams, compressed input, contention.

using Test, Arrow, Tables
const ZstdCompressor = Arrow.ZstdCompressor

# Decode one compressed buffer whose column reads `needed` bytes (-1: unknown).
function decodebuffer(payload, declared, needed; codec=Arrow.CODEC_ZSTD, budget)
    bytes = vcat(collect(reinterpret(UInt8, [Int64(declared)])), payload)
    wire = Arrow.BufferSlice(Arrow.heapregion(bytes), 0, length(bytes))
    state = Arrow.DecodeState(budget)
    cursor = Arrow.DecodeCursor(
        nothing,
        nothing,
        Arrow.BufferSlice(),
        Arrow.Limits();
        codec=codec,
        state=state,
    )
    try
        return Arrow.AC.slicebytes(Arrow._decompressbuffer!(cursor, wire, Int64(needed)))
    finally
        close(state)
    end
end

smallbudget() = Arrow.AllocationBudget(64 * 1024; ratio=64, cap=1 << 30)

@testset "reader allocation limits" begin
    @testset "decompressed bytes a column reads fund themselves" begin
        n = 1 << 20
        frame = transcode(ZstdCompressor, zeros(UInt8, n))
        # 1 MiB from a few hundred bytes, under a 64 KiB budget.
        @test length(decodebuffer(frame, n, n; budget=smallbudget())) == n
        # The same claim with no column known to read it is not funded...
        @test_throws Arrow.AllocationLimitError decodebuffer(
            frame,
            n,
            -1;
            budget=smallbudget(),
        )
        # ...and a column that reads 100 bytes decodes only those.
        @test length(decodebuffer(frame, n, 100; budget=smallbudget())) == 128
        @test_throws Arrow.ValidationError decodebuffer(
            frame[1:8],
            n,
            100;
            budget=smallbudget(),
        )
    end

    @testset "the bytes a column reads come from its layout" begin
        AC = Arrow.AC
        slice(bytes) = Arrow.BufferSlice(Arrow.heapregion(bytes), 0, length(bytes))
        validity = slice(UInt8[0xff])
        offsets = slice(collect(reinterpret(UInt8, Int32[0, 3, 7])))
        needed(t, role, n, previous) =
            Arrow._neededbytes(role, AC.layoutspec_of(t), Int64(n), previous)
        @test needed(AC.Utf8Type(false), AC.DATA, 2, [validity, offsets]) == 7
        @test needed(AC.Utf8Type(false), AC.OFFSETS, 2, [validity]) == 12
        @test needed(AC.IntType(32, true), AC.DATA, 5, [validity]) == 20
        @test needed(AC.BoolType(), AC.DATA, 9, [validity]) == 2
        # Zero-width values have no offsets to read an end from.
        @test needed(AC.FixedSizeBinaryType(0), AC.DATA, 5, [validity]) == -1
    end

    @testset "a claim the frame header contradicts fails before allocation" begin
        src = rand(UInt8, 1000)
        frame = Vector{UInt8}(undef, 2000)
        len = GC.@preserve frame src Arrow.ZSTD.ZSTD_compress(
            frame,
            length(frame),
            src,
            length(src),
            1,
        )
        resize!(frame, len)
        budget = Arrow.AllocationBudget(1 << 30)
        err = try
            decodebuffer(frame, 1 << 29, -1; budget=budget)
        catch e
            e
        end
        @test err isa Arrow.ValidationError && occursin("records", err.msg)
        @test Arrow._remaining(budget) == 1 << 30
        @test decodebuffer(frame, 1000, 1000; budget=smallbudget()) == src
    end

    @testset "a Stream gives each batch its own allowance" begin
        parts = Tables.partitioner([(x=rand(400_000),) for _ = 1:4])
        io = IOBuffer()
        Arrow.write(io, parts; file=true, compress=:zstd)
        bytes = take!(io)
        # A cap that holds one batch but not the whole file.
        limits = Arrow.Limits(max_total_allocated_bytes=5_000_000)
        @test_throws Arrow.AllocationLimitError Arrow.Table(copy(bytes); limits=limits)
        @test sum(length(b.x) for b in Arrow.Stream(copy(bytes); limits=limits)) ==
              1_600_000
    end

    @testset "concurrent charges are exact" begin
        budget = Arrow.AllocationBudget(64 * 1000)
        tasks = [errormonitor(Threads.@spawn begin
            for _ = 1:1000
                Arrow._charge!(budget, Int64(1), "test")
            end
        end) for _ = 1:64]
        foreach(wait, tasks)
        @test Arrow._remaining(budget) == 0
        @test_throws Arrow.AllocationLimitError Arrow._charge!(budget, Int64(1), "test")
    end
end
