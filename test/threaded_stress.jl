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
end
