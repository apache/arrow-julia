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

# =============================================================================
# The conformance driver: run the suites inside the conformance image.
#
#     julia conformance/run.jl [suite ...] [--rebuild]
#
# Suites: `corpus` (arrow-testing gold files), `oracle` (our IPC bytes
# through pyarrow and nanoarrow), `cdata` (C Data / C Stream through an
# in-process pyarrow); no argument runs all three. Docker is the only host
# requirement (plus network the first time, to fetch Harbor.jl and build the
# image): the image (conformance/Dockerfile) carries Julia, the oracle
# Python, the gold corpus, and Tables.jl's scan branch, and the repository is
# bind-mounted at /work. The image is built once (`--rebuild` forces it) and
# a named volume keeps the Julia depot — precompilation caches — between
# runs. Harbor.jl manages the container; suite output streams live.
#
# This script needs no `--project`: it activates and instantiates its own
# host environment (conformance/host/, Harbor only). conformance/Project.toml
# is the IN-CONTAINER suite environment and is prepared inside the image.
#
# Exit code: 0 iff every requested suite passed.
# =============================================================================

import Pkg
Pkg.activate(joinpath(@__DIR__, "host"); io=devnull)
Pkg.instantiate(; io=devnull)
using Harbor

const IMAGE = "arrow-julia-conformance:latest"
const DEPOT_VOLUME = "arrow-julia-conformance-depot"
const REPO = normpath(joinpath(@__DIR__, ".."))
const SUITES = Dict(
    "corpus" => "conformance/corpus.jl",
    "oracle" => "conformance/oracle.jl",
    "cdata" => "conformance/cdata_oracle.jl",
)
const SUITE_ORDER = ["corpus", "oracle", "cdata"]

_haveimage() = success(pipeline(`docker image inspect $IMAGE`;
    stdout=devnull, stderr=devnull))

function buildimage()
    println("conformance: building $IMAGE (once; --rebuild forces)")
    Base.run(`docker build -t $IMAGE -f $(joinpath(REPO, "conformance", "Dockerfile"))
        $(joinpath(REPO, "conformance"))`)
    return nothing
end

# `docker exec` directly (not Harbor.exec, which captures output): the suites
# run for minutes and their progress belongs on the terminal.
function execstream(container, cmd::Vector{String})
    proc = Base.run(ignorestatus(`docker exec $(container.id) $cmd`))
    return proc.exitcode
end

function main(args)
    rebuild = "--rebuild" in args
    requested = filter(a -> a != "--rebuild", args)
    isempty(requested) && (requested = SUITE_ORDER)
    for s in requested
        haskey(SUITES, s) || error("unknown suite $(repr(s)); choose from $(join(SUITE_ORDER, ", "))")
    end
    (rebuild || !_haveimage()) && buildimage()

    container = Harbor.run!(IMAGE; command=["sleep", "infinity"], detach=true,
        volumes=Dict("/work" => REPO, "/opt/julia-depot" => DEPOT_VOLUME))
    results = Dict{String,Int}()
    try
        # The suite environment: the repository's conformance project with
        # the mounted checkout and the image's Tables branch developed in.
        # Cheap when the depot volume is warm; fetches only what changed.
        println("conformance: preparing the suite environment")
        rc = execstream(container, ["julia", "--project=/opt/env", "-e",
            """using Pkg
               cp("/work/conformance/Project.toml", "/opt/env/Project.toml"; force=true)
               Pkg.develop(path="/work"); Pkg.develop(path="/opt/Tables")
               Pkg.instantiate(); Pkg.precompile()"""])
        rc == 0 || error("suite environment preparation failed (exit $rc)")
        for s in requested
            println()
            println("conformance: ===== $s =====")
            results[s] = execstream(container,
                ["julia", "--project=/opt/env", "--startup-file=no", SUITES[s]])
        end
    finally
        Harbor.cleanup!(container)
    end
    println()
    for s in requested
        println(rpad(s, 8), results[s] == 0 ? "PASS" : "FAIL (exit $(results[s]))")
    end
    exit(all(==(0), values(results)) ? 0 : 1)
end

main(ARGS)
