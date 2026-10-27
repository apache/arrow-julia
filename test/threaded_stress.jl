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

# Stress the concurrent validation caches and the shared allocation budget.
# Run as a child process with `-t 4` (core_tests.jl launches it with
# `--threads=4`); it requires Threads.nthreads() >= 4.

using Test

using Arrow
using Arrow.ArrowCore
using Tables
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

    @testset "concurrent allocation budget" begin
        limit = 100_000
        budget = Arrow.AllocationBudget(limit)
        successes = Threads.Atomic{Int}(0)
        @sync for _ = 1:Threads.nthreads()
            Threads.@spawn for _ = 1:limit
                try
                    Arrow._charge!(budget, Int64(1), "threaded budget probe")
                    Threads.atomic_add!(successes, 1)
                catch e
                    e isa Arrow.AllocationLimitError || rethrow()
                end
            end
        end
        @test successes[] == limit
        @test Arrow._remaining(budget) == 0
    end

    @testset "parallel reads from concurrent tasks" begin
        n = 100_000
        parts = Tables.partitioner([
            (x=collect(1:n), s=[i % 9 == 0 ? missing : string(i, base=7) for i = 1:n]),
            (x=collect(1:n), s=[repeat("long value ", i % 4) for i = 1:n]),
        ])
        io = IOBuffer()
        Arrow.write(io, parts; compress=:zstd)
        bytes = take!(io)
        expected = Arrow.Table(bytes; ntasks=1)
        failures = AC.ReleaseCounter()
        tasks = map(1:4) do _
            errormonitor(
                Threads.@spawn begin
                    for _ = 1:5
                        t = Arrow.Table(bytes; ntasks=4)
                        isequal(t.x, expected.x) && isequal(t.s, expected.s) ||
                            AC.increment!(failures)
                    end
                end
            )
        end
        foreach(wait, tasks)
        @test failures[] == 0
    end
end
