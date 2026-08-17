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

# Serialize/deserialize benchmark driver (review R12): Arrow.jl 3.0 vs
# Arrow.jl 2.x vs PyArrow over identical logical workloads.
#
#   julia --project=. bench/run.jl [workdir]
#
# Legs run in their own processes (2.x under bench/env2x; PyArrow inside
# the conformance oracle image when docker is available — skipped
# cleanly otherwise). Results print as a markdown table of seconds and
# throughput.
#
# READ SEMANTICS DIFFER BY DESIGN, so read rows are not like-for-like:
#   rewrite  = full structural+semantic validation + materialized Julia
#              Vectors (the facade contract)
#   arrow2x  = lazy zero-copy wrap + one copy() per column, no validation
#   pyarrow  = memory-mapped wrap only; per-element work is deferred
# Write rows ARE like-for-like: table in memory -> IPC file on disk.

function _runleg(cmd::Cmd, out::String)
    open(out, "w") do io
        run(pipeline(cmd; stdout=io))
    end
    return nothing
end

function main(workdir::String)
    mkpath(workdir)
    here = @__DIR__
    repo = dirname(here)
    legs = Tuple{String,String}[]

    rewriteout = joinpath(workdir, "rewrite.jsonl")
    _runleg(`$(Base.julia_cmd()) --startup-file=no --project=$repo
             $(joinpath(here, "bench_rewrite.jl")) $workdir`, rewriteout)
    push!(legs, ("rewrite", rewriteout))

    out2x = joinpath(workdir, "arrow2x.jsonl")
    _runleg(`$(Base.julia_cmd()) --startup-file=no
             --project=$(joinpath(here, "env2x"))
             $(joinpath(here, "bench_2x.jl")) $workdir`, out2x)
    push!(legs, ("arrow2x", out2x))

    pyout = joinpath(workdir, "pyarrow.jsonl")
    havedocker = success(pipeline(
        `docker image inspect arrow-conformance-oracle:latest`;
        stdout=devnull, stderr=devnull))
    if havedocker
        _runleg(`docker run --rm -v $workdir:/bench -v $here:/src
                 arrow-conformance-oracle:latest
                 python3 /src/bench_pyarrow.py /bench`, pyout)
        push!(legs, ("pyarrow", pyout))
    else
        println("(pyarrow leg skipped: oracle docker image not available)")
    end

    # Minimal JSONL field extraction; the emitters write flat one-line
    # objects with known keys.
    results = Dict{Tuple{String,String,String},Tuple{Float64,Int64}}()
    for (_, file) in legs, line in eachline(file)
        isempty(strip(line)) && continue
        g(k) = match(Regex("\"$k\":\"?([^\",}]+)"), line).captures[1]
        results[(g("impl"), g("workload"), g("op"))] =
            (parse(Float64, g("seconds")), parse(Int64, g("bytes")))
    end

    impls = [name for (name, _) in legs]
    println()
    println("| workload | op | " * join(impls, " | ") * " | MB/s (" *
            join(impls, " / ") * ") |")
    println("|---|---|" * repeat("---|", length(impls) + 1))
    for (wl, _) in (("primitive", 0), ("nullable", 0), ("strings", 0),
        ("lists", 0), ("dictpool", 0)), op in ("write", "read")
        secs = [get(results, (impl, wl, op), (NaN, 0))[1] for impl in impls]
        mbs = [begin
            s, b = get(results, (impl, wl, op), (NaN, 0))
            isnan(s) ? "-" : string(round(b / s / 1e6; digits=0))
        end for impl in impls]
        println("| $wl | $op | " *
                join([isnan(s) ? "-" : string(round(s; digits=4)) for s in secs], " | ") *
                " | " * join(mbs, " / ") * " |")
    end
end

main(isempty(ARGS) ? mktempdir() : ARGS[1])
