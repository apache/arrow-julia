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

# The thread a test producer's release last ran on.
mutable struct ReleaseThread
    @atomic id::Int
end
const TEST_RELEASE_THREAD = ReleaseThread(0)

function _test_thread_release(p::Ptr{CArrowArray})::Cvoid
    @atomic TEST_RELEASE_THREAD.id = Threads.threadid()
    _store_field!(p, :release, Ptr{Cvoid}(C_NULL))
    return nothing
end

test_thread_release() = @cfunction(_test_thread_release, Cvoid, (Ptr{CArrowArray},))

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

# A moved test array whose owner is unreachable once this returns.
@noinline function _drop_import()
    arr = Ref(_test_c_array(test_thread_release()))
    GC.@preserve arr _moveowner(Base.unsafe_convert(Ptr{CArrowArray}, arr))
    return nothing
end

# The thread pool of the thread `_drop_import_on_foreign_thread` started.
const FOREIGN_POOL = Ref(:none)

function _foreign_drop_import(::Ptr{Cvoid})
    FOREIGN_POOL[] = Threads.threadpool()
    _drop_import()
    return nothing
end

# Runs `_drop_import` on a thread Julia did not start, as a C library's worker
# thread would call back into Julia, and waits for that thread to end.
function _drop_import_on_foreign_thread()
    entry = @cfunction(_foreign_drop_import, Cvoid, (Ptr{Cvoid},))
    tid = Ref{UInt}(0)
    err = ccall(
        :uv_thread_create,
        Cint,
        (Ptr{UInt}, Ptr{Cvoid}, Ptr{Cvoid}),
        tid,
        entry,
        C_NULL,
    )
    err == 0 || error("uv_thread_create failed: $err")
    # Joined GC-safe, so a collection the adopted thread waits for can run.
    state = ccall(:jl_gc_safe_enter, Int8, ())
    err = ccall(:uv_thread_join, Cint, (Ptr{UInt},), tid)
    ccall(:jl_gc_safe_leave, Cvoid, (Int8,), state)
    err == 0 || error("uv_thread_join failed: $err")
    return nothing
end

_registry_count() = lock(REGISTRY_LOCK) do
    length(EXPORTS)
end
"Free every export released before the call."
_reapreleased!() = _reap!()
"Reap, then count what is still registered."
_drained() = (_reapreleased!(); _registry_count())
"The registry key of the export whose struct is at `p`."
_exportkey(p::Ptr) = Ptr{Int64}(unsafe_load(p).private_data)
_registered(key::Ptr{Int64}) = lock(REGISTRY_LOCK) do
    haskey(EXPORTS, key)
end

# Wait up to `seconds` for `done()`, sleeping so tasks on this thread and on
# other threads run meanwhile.
function _waitfor(done; seconds=30.0)
    t = time()
    while !done() && time() - t < seconds
        sleep(0.001)
    end
    return done()
end

# Collect and reap until `done()`: a collection hands an import's release to the
# thread that made the import, which runs it once that thread wakes.
_collectuntil(done) = _waitfor(() -> (GC.gc(true); _reap!(); done()))

function _stress_reaper(ready, start, done, workers)
    increment!(ready)
    wait(start)
    reaped = 0
    for _ = 1:10_000
        reaped += _reap!()
        done[] == workers && _registry_count() == 0 && break
        yield()
    end
    return reaped
end

function _threaded_cdata_stress()
    Threads.nthreads() >= 4 ||
        error("threaded C Data stress requires at least four threads")

    # Different exports may be released concurrently, from any thread, while
    # reapers scan and free the released ones; each export is freed once.
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
                _release_c!(sp)
                _release_c!(ap)
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
    reaped = sum(reaped_by_task) + _reapreleased!()
    @assert reaped == n (reaped, reaped_by_task, _registry_count())
    @assert _registry_count() == 0

    # One atomic swap must choose between explicit release and the registered
    # finalizer before either path reads or frees the native struct copy.
    rounds = 200
    before = TEST_CONFORMING_RELEASES[]
    owners = ForeignOwner[]
    for _ = 1:rounds
        arr = Ref(_test_c_array(test_conforming_release()))
        push!(
            owners,
            GC.@preserve arr _moveowner(Base.unsafe_convert(Ptr{CArrowArray}, arr))
        )
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
        @assert (@atomic owner.cell.closed)
    end

    # Tables exported and imported back on several threads at once, with the
    # imports released by garbage collection: every export is freed, and
    # every import reads what was exported.
    tasks = [
        errormonitor(Threads.@spawn begin
                for i = 1:250
                    xs = collect(Int64, i:(i + 99))
                    sp, ap = to_c_data((x=xs, s=["a", "bb"][mod1.(1:100, 2)]))
                    t = Table(sp, ap)
                    @assert collect(t.x) == xs
                    i % 50 == 0 && GC.gc(false)
                end
            end) for _ = 1:4
    ]
    foreach(fetch, tasks)
    @assert _collectuntil(() -> _registry_count() == 0) _registry_count()

    # A garbage collection on another thread hands an import's release to the
    # thread that imported it, so a release that waits on a lock the importing
    # thread holds (Python's GIL) cannot stall the collection.
    importer = Threads.threadid()
    crossed = false
    for _ = 1:20
        @atomic TEST_RELEASE_THREAD.id = 0
        _drop_import()
        collector = fetch(errormonitor(Threads.@spawn begin
            GC.gc(true)
            Threads.threadid()
        end))
        _waitfor(() -> (@atomic TEST_RELEASE_THREAD.id) != 0)
        @assert (@atomic TEST_RELEASE_THREAD.id) == importer
        crossed = collector != importer
        crossed && break
    end
    @assert crossed

    # A thread Julia adopted from a foreign library runs no Julia tasks, so an
    # import made there is released on thread 1 once collected.
    @atomic TEST_RELEASE_THREAD.id = 0
    _drop_import_on_foreign_thread()
    @assert FOREIGN_POOL[] === :foreign
    _collectuntil(() -> (@atomic TEST_RELEASE_THREAD.id) != 0)
    released = @atomic TEST_RELEASE_THREAD.id
    @assert released == 1 released
    return nothing
end
