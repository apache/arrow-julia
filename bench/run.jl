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

# Serialize/deserialize benchmark driver: Arrow.jl vs PyArrow over
# identical logical workloads.
#
#   julia --project=. bench/run.jl [workdir]
#
# Legs run in separate processes on the host when PyArrow is installed.
# The conformance image is a fallback. Results print seconds and throughput.
#
# Both readers open the Julia-written files and return native columns.
# Arrow.jl also validates buffers and builds DataStrings payloads; PyArrow
# generally wraps buffers. Neither times scalar iteration or mutable copies.

function _runleg(cmd::Cmd, out::String)
    open(out, "w") do io
        run(pipeline(cmd; stdout=io))
    end
    return nothing
end

# The workload names each leg must report (bench/workloads.jl owns their
# definitions; the driver cannot include that file — it references Arrow).
const WORKLOAD_NAMES = ("primitive", "nullable", "strings", "lists", "dictpool")

function main(workdir::String)
    mkpath(workdir)
    here = @__DIR__
    repo = dirname(here)
    legs = Tuple{String,String}[]
    threads = Threads.nthreads()
    scale = get(ENV, "ARROW_BENCH_SCALE", "1")
    println("Julia $(VERSION), $threads threads; ARROW_BENCH_SCALE=$scale")

    rewriteout = joinpath(workdir, "rewrite.jsonl")
    rewritescript = joinpath(here, "bench_rewrite.jl")
    _runleg(
        `$(Base.julia_cmd()) --startup-file=no --threads=$threads --project=$repo $rewritescript $workdir`,
        rewriteout,
    )
    push!(legs, ("rewrite", rewriteout))

    pyout = joinpath(workdir, "pyarrow.jsonl")
    python = get(ENV, "ARROW_BENCH_PYTHON", something(Sys.which("python3"), "python3"))
    havepython = try
        success(pipeline(`$python -c "import pyarrow"`; stdout=devnull, stderr=devnull))
    catch
        false
    end
    !havepython &&
        haskey(ENV, "ARROW_BENCH_PYTHON") &&
        error("ARROW_BENCH_PYTHON must name a Python executable with pyarrow installed")
    # A container is useful for smoke checks, but its IO and CPU scheduling
    # differ from the host Julia process. Prefer a local Python for timings.
    havedocker =
        !havepython &&
        Sys.which("docker") !== nothing &&
        try
            success(
                pipeline(
                    `docker image inspect arrow-julia-conformance:latest`;
                    stdout=devnull,
                    stderr=devnull,
                ),
            )
        catch
            false
        end
    if havepython
        version = readchomp(`$python -c "import pyarrow; print(pyarrow.__version__)"`)
        println("PyArrow $version, $threads threads, host Python: $python")
        _runleg(`$python $(joinpath(here, "bench_pyarrow.py")) $workdir $threads`, pyout)
        push!(legs, ("pyarrow", pyout))
    elseif havedocker
        image = "arrow-julia-conformance:latest"
        python = "/opt/pyarrow/bin/python"
        println(
            "PyArrow runs in Docker; host/container timing ratios are not directly comparable.",
        )
        _runleg(
            `docker run --rm -e ARROW_BENCH_SCALE=$scale -v $workdir:/bench -v $here:/src $image $python /src/bench_pyarrow.py /bench $threads`,
            pyout,
        )
        push!(legs, ("pyarrow", pyout))
    else
        println(
            "(pyarrow leg skipped: neither host PyArrow nor the conformance Docker image is available)",
        )
    end

    # Minimal JSONL field extraction; the emitters write flat one-line
    # objects with known keys.
    results = Dict{Tuple{String,String,String},Tuple{Float64,Int64}}()
    for (_, file) in legs, line in eachline(file)
        isempty(strip(line)) && continue
        g(k) = match(Regex("\"$k\":\"?([^\",}]+)"), line).captures[1]
        key = (g("impl"), g("workload"), g("op"))
        haskey(results, key) && error("duplicate benchmark record for $key")
        results[key] = (parse(Float64, g("seconds")), parse(Int64, g("bytes")))
    end
    # A leg that exits 0 with partial output must refuse, not print a
    # plausible table.
    impls = [name for (name, _) in legs]
    for impl in impls, wl in WORKLOAD_NAMES, op in ("write", "read")
        haskey(results, (impl, wl, op)) ||
            error("missing benchmark record for $((impl, wl, op))")
    end

    println()
    println("Reads use the same memory-mapped files and request every column.")
    println(
        "Arrow.jl validates buffers and builds string payloads; PyArrow generally wraps buffers.",
    )
    println(
        "Neither read times scalar iteration or mutable copies. Writes use fresh destinations.",
    )
    println()
    println(
        "| workload | op | " *
        join(impls, " | ") *
        " | MB/s (" *
        join(impls, " / ") *
        ") |",
    )
    println("|---|---|" * repeat("---|", length(impls) + 1))
    for wl in WORKLOAD_NAMES, op in ("write", "read")
        secs = [get(results, (impl, wl, op), (NaN, 0))[1] for impl in impls]
        mbs = [
            begin
                s, b = get(results, (impl, wl, op), (NaN, 0))
                isnan(s) ? "-" : string(round(b / s / 1e6; digits=0))
            end for impl in impls
        ]
        println(
            "| $wl | $op | " *
            join([isnan(s) ? "-" : string(round(s; digits=4)) for s in secs], " | ") *
            " | " *
            join(mbs, " / ") *
            " |",
        )
    end
end

main(isempty(ARGS) ? mktempdir() : abspath(ARGS[1]))
