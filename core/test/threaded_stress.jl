# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright ownership.

using Test

include(joinpath(@__DIR__, "..", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore

# Local coordination flag with an atomic field (no Threads.Atomic boxes —
# they are effectively deprecated in favor of `@atomic` struct fields).
mutable struct Gate
    @atomic open::Bool
end
Gate() = Gate(false)
open!(g::Gate) = (@atomic g.open = true)
isopen_gate(g::Gate) = @atomic g.open

@testset "ArrowCore threaded lifecycle and caches" begin
    @test Threads.nthreads() >= 4

    @testset "one concurrent closer releases" begin
        for _ = 1:100
            bytes = UInt8[0]
            calls = AC.ReleaseCounter()
            r = GC.@preserve bytes AC.OwnerRegion(
                Ptr{UInt8}(pointer(bytes)), 1, AC.Foreign;
                root=bytes,
                releasefn=AC.NotifyRelease(calls))
            go = Gate()
            tasks = [Threads.@spawn begin
                while !isopen_gate(go)
                    yield()
                end
                forceclose!(r; timeout_ms=1000)
            end for _ = 1:16]
            open!(go)
            results = fetch.(tasks)
            @test any(results)
            @test calls[] == 1
            @test AC.phase(@atomic r.state) == AC.PHASE_CLOSED
        end
    end

    @testset "one concurrent MapClaim owner releases" begin
        for _ = 1:100
            claim = AC.MapClaim()
            calls = AC.ReleaseCounter()
            ready = AC.ReleaseCounter()
            go = Gate()
            unmapper = function (_p, _len)
                AC.increment!(calls)
                # Keep RELEASING visible while contenders execute their CAS
                # loops on other worker threads.
                for _ = 1:8
                    yield()
                end
                nothing
            end
            tasks = [Threads.@spawn begin
                AC.increment!(ready)
                while !isopen_gate(go)
                    yield()
                end
                AC._release_mapping_once!(claim, Ptr{Cvoid}(1), 1, unmapper)
            end for _ = 1:16]
            while ready[] != length(tasks)
                yield()
            end
            open!(go)
            @test all(x -> x === nothing, fetch.(tasks))
            @test calls[] == 1
            @test (@atomic claim.s) == 0x02
        end
    end

    @testset "guard and release handshake" begin
        for _ = 1:100
            bytes = UInt8[0x5a]
            released = AC.ReleaseCounter()
            overlap = Gate()
            r = GC.@preserve bytes AC.OwnerRegion(
                Ptr{UInt8}(pointer(bytes)), 1, AC.Foreign;
                root=bytes, releasefn=AC.NotifyRelease(released))
            go = Gate()
            workers = [Threads.@spawn begin
                while !isopen_gate(go)
                    yield()
                end
                for _ = 1:100
                    try
                        withguard(r) do
                            released[] > 0 && open!(overlap)
                            unsafe_load(r.ptr) == 0x5a || open!(overlap)
                            yield()
                            released[] > 0 && open!(overlap)
                        end
                    catch e
                        e isa InvalidatedError || rethrow()
                    end
                end
            end for _ = 1:8]
            closer = Threads.@spawn begin
                while !isopen_gate(go)
                    yield()
                end
                while !forceclose!(r; timeout_ms=1000)
                    yield()
                end
            end
            open!(go)
            fetch.(workers)
            fetch(closer)
            @test !isopen_gate(overlap)
        end
    end

    @testset "concurrent validation caches" begin
        f, built = fromjulia("x", [i % 7 == 0 ? missing : i for i = 1:10_000])
        d = AC.ArrayData(built.type, built.len, built.buffers)
        expected = count(i -> i % 7 == 0, 1:10_000)
        failures = AC.ReleaseCounter()
        Threads.@threads for _ = 1:1000
            try
                nullcount(d) == expected || AC.increment!(failures)
                validate_semantic(f, d) === d || AC.increment!(failures)
            catch
                AC.increment!(failures)
            end
        end
        @test failures[] == 0
        @test (@atomic d.nullcount) == expected
        @test (@atomic d.semachecked)
    end
end
