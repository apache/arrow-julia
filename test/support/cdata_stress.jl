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

# The fresh-process C Data stress probe and its exact dependencies. This file
# is included into both acceptance support and the narrow child support module.

const TEST_CONFORMING_RELEASES = ReleaseCounter()

function _test_conforming_release(p::Ptr{CArrowArray})::Cvoid
    increment!(TEST_CONFORMING_RELEASES)
    _store_field!(p, :release, Ptr{Cvoid}(C_NULL))
    return nothing
end

# Runtime accessor, not a module-level pointer constant: a raw @cfunction
# pointer stored in a const is invalid after a precompile-cache reload.
test_conforming_release() = @cfunction(_test_conforming_release, Cvoid, (Ptr{CArrowArray},))

function _test_c_array(release::Ptr{Cvoid})
    return CArrowArray(
        0,
        0,
        0,
        0,
        0,
        Ptr{Ptr{Cvoid}}(C_NULL),
        Ptr{Ptr{CArrowArray}}(C_NULL),
        Ptr{CArrowArray}(C_NULL),
        release,
        Ptr{Cvoid}(C_NULL),
    )
end

_registry_count() = lock(REGISTRY_LOCK) do
    length(EXPORT_REGISTRY)
end

function _call_release(p::Ptr{CArrowSchema})
    release = lock(REGISTRY_LOCK) do
        unsafe_load(p).release
    end
    release == C_NULL || ccall(release, Cvoid, (Ptr{CArrowSchema},), p)
    return nothing
end

function _call_release(p::Ptr{CArrowArray})
    release = lock(REGISTRY_LOCK) do
        unsafe_load(p).release
    end
    release == C_NULL || ccall(release, Cvoid, (Ptr{CArrowArray},), p)
    return nothing
end

function _stress_reaper(ready, start, done, workers)
    increment!(ready)
    wait(start)
    reaped = 0
    for _ = 1:10_000
        reaped += reap!()
        done[] == workers && _registry_count() == 0 && break
        yield()
    end
    return reaped
end

function _threaded_cdata_stress()
    Threads.nthreads() >= 4 ||
        error("threaded C Data stress requires at least four threads")

    # Different exported trees may release concurrently. Reapers scan and
    # claim those roots at the same time; each root must be popped once.
    n = 1_000
    workers = 4
    f, d = fromjulia("registry-race", Int64[1])
    roots = [to_c_data(f, d) for _ = 1:n]
    ready = ReleaseCounter()
    done = ReleaseCounter()
    start = Base.Event()
    releasers = [errormonitor(Threads.@spawn begin
        increment!(ready)
        wait(start)
        try
            for i = worker:workers:n
                sp, ap = roots[i]
                _call_release(sp)
                _call_release(ap)
                i % 16 == 0 && yield()
            end
        finally
            increment!(done)
        end
    end) for worker = 1:workers]
    reapers = [
        errormonitor(Threads.@spawn _stress_reaper(ready, start, done, workers)) for _ = 1:3
    ]
    while ready[] != length(releasers) + length(reapers)
        yield()
    end
    notify(start)
    foreach(fetch, releasers)
    reaped_by_task = fetch.(reapers)
    reaped = sum(reaped_by_task) + reap!()
    @assert reaped == 2n (reaped, reaped_by_task, _registry_count())
    @assert _registry_count() == 0

    # One atomic swap must choose between explicit release and the registered
    # finalizer before either path reads or frees the native struct copy.
    rounds = 200
    before = TEST_CONFORMING_RELEASES[]
    owners = ForeignOwner[]
    for _ = 1:rounds
        owner = ForeignOwner(_test_c_array(test_conforming_release()))
        _arm_foreign_owner!(owner)
        push!(owners, owner)
    end
    ready = ReleaseCounter()
    start = Base.Event()
    contenders = Task[]
    for owner in owners
        push!(contenders, errormonitor(Threads.@spawn begin
            increment!(ready)
            wait(start)
            release!(owner)
        end))
        push!(contenders, errormonitor(Threads.@spawn begin
            increment!(ready)
            wait(start)
            finalize(owner)
        end))
    end
    while ready[] != length(contenders)
        yield()
    end
    notify(start)
    foreach(fetch, contenders)
    @assert TEST_CONFORMING_RELEASES[] - before == rounds
    for owner in owners
        @assert (@atomic owner.released)
    end
    return nothing
end
