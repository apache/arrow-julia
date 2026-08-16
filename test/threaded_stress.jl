# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright ownership.

using Test

using Arrow
using Arrow.ArrowCore
const AC = ArrowCore

@testset "ArrowCore threaded caches" begin
    @test Threads.nthreads() >= 4

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
