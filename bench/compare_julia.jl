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

# Run both Arrow releases in fresh processes against the same fixture files.
# Prepare fixtures with bench/run.jl, then see bench/README.md.
if isempty(ARGS) || first(ARGS) != "--leg"
    length(ARGS) == 2 || error("usage: compare_julia.jl BENCH_DIR ARROW_2_8_1_PROJECT")
    workdir, baseline = abspath.(ARGS)
    current = dirname(Base.active_project())
    threads = Threads.nthreads()
    for (project, label) in ((current, "candidate"), (baseline, "2.8.1"))
        run(
            `$(Base.julia_cmd()) --startup-file=no --threads=$threads --project=$project $(@__FILE__) --leg $workdir $label`,
        )
    end
    exit()
end

using Libdl
const LOAD_STARTED = time_ns()
using Arrow, Tables
const LOAD_SECONDS = (time_ns() - LOAD_STARTED) / 1e9
include(joinpath(@__DIR__, "workloads.jl"))

function measure(f, verify, label)
    first = @timed f()
    verify(first.value)
    times, allocations = Float64[], Int[]
    for _ = 1:5
        GC.gc()
        sample = @timed f()
        verify(sample.value)
        push!(times, sample.time)
        push!(allocations, sample.bytes)
    end
    sort!(times)
    sort!(allocations)
    println(
        label,
        " first_s=",
        first.time,
        " median_s=",
        times[3],
        " range_s=",
        (times[1], times[end]),
        " median_bytes=",
        allocations[3],
    )
    return nothing
end

logicalsum(v) = sum(
    x -> x isa AbstractString ? ncodeunits(x) : x isa AbstractVector ? sum(x) : x,
    v;
    init=0,
)

function checkread(path, input, label)
    openfirst() = first(Tables.getcolumn(Arrow.Table(path), 1))
    expectedfirst = first(Tables.getcolumn(input, 1))
    measure(
        openfirst,
        x -> isequal(x, expectedfirst) || error("first value changed"),
        label * " first",
    )
    col = Tables.getcolumn(Arrow.Table(path), 1)
    original = Tables.getcolumn(input, 1)
    isequal(col, original) || error("input and file values differ")
    expected = logicalsum(original)
    measure(
        () -> logicalsum(col),
        x -> x == expected || error("walk checksum changed"),
        label * " walk",
    )
    return nothing
end

if isdefined(Arrow, :AbstractArrowSource)
    mutable struct CountedSource <: Arrow.AbstractArrowSource
        bytes::Vector{UInt8}
        requests::Int
        fetched::Int
    end
    Arrow.sourcelength(s::CountedSource) = length(s.bytes)
    function Arrow.readrange(s::CountedSource, offset, len)
        s.requests += 1
        s.fetched += len
        return s.bytes[(offset + 1):(offset + len)]
    end

    @noinline function owned_row()
        io = Arrow.tobuffer((x=collect(1:100_000),); file=true)
        bytes = take!(io)
        return Arrow.Table(bytes; limit=1),
        WeakRef(Arrow.AC._storageroot(bytes)),
        length(bytes)
    end

    function ranges()
        for n in (4000, 8000, 16000)
            parts = Tables.partitioner((x=[i],) for i = 1:n)
            bytes = take!(Arrow.tobuffer(parts; file=true))
            function runscan()
                source = CountedSource(bytes, 0, 0)
                t = Arrow.Table(source; filter=Arrow.colcmp(==, Arrow.col(:x), n), limit=1)
                return only(t.x), source.requests, source.fetched
            end
            measure(
                runscan,
                r -> r[1] == n || error("late match changed"),
                "ranges $n batches",
            )
            _, requests, fetched = runscan()
            println("ranges $n requests=", requests, " fetched_bytes=", fetched)
        end
        result, backing, nbytes = owned_row()
        GC.@preserve result begin
            GC.gc(true)
            result.x == [1] || error("owned row changed")
            println(
                "owned row input_bytes=",
                nbytes,
                " retained_input_bytes=",
                backing.value === nothing ? 0 : nbytes,
            )
        end
        return nothing
    end
end

function main(workdir, label)
    version = Base.pkgversion(Arrow)
    label == "2.8.1" && version != v"2.8.1" && error("baseline is Arrow $version")
    println(
        "Arrow ",
        version,
        " Julia ",
        VERSION,
        " threads=",
        Threads.nthreads(),
        " load_s=",
        LOAD_SECONDS,
    )
    images = filter(
        path -> occursin("/compiled/", path) && basename(dirname(path)) == "Arrow",
        Libdl.dllist(),
    )
    println("loaded_arrow_image_bytes=", sum(filesize, images; init=0))
    for (name, make) in BENCH_WORKLOADS
        name in ("primitive", "strings", "lists") || continue
        input = make()
        path = joinpath(workdir, "rewrite-$name.arrow")
        checkread(path, input, name)
        function encode()
            io = IOBuffer()
            Arrow.write(io, input; file=true, ntasks=1)
            return take!(io)
        end
        function checkwrite(bytes)
            got = Arrow.Table(bytes)
            isequal(Tables.columntable(got), input) || error("write round-trip changed")
        end
        measure(encode, checkwrite, name * " write")
    end
    # Equal total data spread over sixteen columns stresses first access.
    widepath = joinpath(workdir, "compare-wide.arrow")
    n = max(1, BENCH_ROWS_STRINGS ÷ 16)
    s = [string("wide-", i) for i = 1:n]
    wide = NamedTuple{ntuple(i -> Symbol("s", i), 16)}(ntuple(_ -> s, 16))
    label == "candidate" && Arrow.write(widepath, wide; file=true, ntasks=1)
    checkread(widepath, wide, "wide strings")
    println("process_peak_rss_bytes=", Sys.maxrss())
    label == "candidate" && ranges()
    return nothing
end

main(ARGS[2], ARGS[3])
