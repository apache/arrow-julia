# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright ownership.

using Test

include(joinpath(@__DIR__, "..", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore

@testset "ArrowCore threaded lifecycle and caches" begin
    @test Threads.nthreads() >= 4

    @testset "one concurrent closer releases" begin
        for _ = 1:100
            bytes = UInt8[0]
            calls = Threads.Atomic{Int}(0)
            r = GC.@preserve bytes AC.OwnerRegion(
                Ptr{UInt8}(pointer(bytes)), 1, AC.Foreign;
                root=bytes,
                releasefn=NotifyRelease(calls))
            go = Threads.Atomic{Bool}(false)
            tasks = [Threads.@spawn begin
                while !go[]
                    yield()
                end
                forceclose!(r; timeout_ms=1000)
            end for _ = 1:16]
            go[] = true
            results = fetch.(tasks)
            @test any(results)
            @test calls[] == 1
            @test AC.phase(@atomic r.state) == AC.PHASE_CLOSED
        end
    end

    @testset "guard and release handshake" begin
        for _ = 1:100
            bytes = UInt8[0x5a]
            released = Threads.Atomic{Int}(0)
            overlap = Threads.Atomic{Bool}(false)
            r = GC.@preserve bytes AC.OwnerRegion(
                Ptr{UInt8}(pointer(bytes)), 1, AC.Foreign;
                root=bytes, releasefn=NotifyRelease(released))
            go = Threads.Atomic{Bool}(false)
            workers = [Threads.@spawn begin
                while !go[]
                    yield()
                end
                for _ = 1:100
                    try
                        withguard(r) do
                            released[] > 0 && (overlap[] = true)
                            unsafe_load(r.ptr) == 0x5a || (overlap[] = true)
                            yield()
                            released[] > 0 && (overlap[] = true)
                        end
                    catch e
                        e isa InvalidatedError || rethrow()
                    end
                end
            end for _ = 1:8]
            closer = Threads.@spawn begin
                while !go[]
                    yield()
                end
                while !forceclose!(r; timeout_ms=1000)
                    yield()
                end
            end
            go[] = true
            fetch.(workers)
            fetch(closer)
            @test !overlap[]
        end
    end

    @testset "concurrent validation caches" begin
        f, built = fromjulia("x", [i % 7 == 0 ? missing : i for i = 1:10_000])
        d = AC.ArrayData(built.type, built.len, built.buffers)
        expected = count(i -> i % 7 == 0, 1:10_000)
        failures = Threads.Atomic{Int}(0)
        Threads.@threads for _ = 1:1000
            try
                nullcount(d) == expected || Threads.atomic_add!(failures, 1)
                validate_semantic(f, d) === d || Threads.atomic_add!(failures, 1)
            catch
                Threads.atomic_add!(failures, 1)
            end
        end
        @test failures[] == 0
        @test (@atomic d.nullcount) == expected
        @test (@atomic d.semachecked)
    end
end
