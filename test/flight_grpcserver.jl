# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

using Pkg

const TEST_ROOT = @__DIR__
const ARROW_ROOT = normpath(joinpath(TEST_ROOT, ".."))
const ARROWTYPES_ROOT = joinpath(ARROW_ROOT, "src", "ArrowTypes")

function maybe_git_root(path::AbstractString)
    try
        return readchomp(pipeline(`git -C $path rev-parse --show-toplevel`; stderr=devnull))
    catch
        return nothing
    end
end

function flight_grpcserver_roots(path::AbstractString)
    roots = String[]
    current = abspath(path)
    while true
        root = maybe_git_root(current)
        if !isnothing(root) && root ∉ roots
            push!(roots, root)
        end
        parent = dirname(current)
        parent == current && break
        current = parent
    end
    return roots
end

function locate_grpcserver()
    if haskey(ENV, "ARROW_FLIGHT_GRPCSERVER_PATH")
        candidate = abspath(ENV["ARROW_FLIGHT_GRPCSERVER_PATH"])
        isdir(candidate) || error("ARROW_FLIGHT_GRPCSERVER_PATH does not exist: $candidate")
        return candidate
    end
    for root in flight_grpcserver_roots(TEST_ROOT)
        candidate = joinpath(root, ".cache", "vendor", "gRPCServer.jl")
        isdir(candidate) && return candidate
    end
    error(
        "Could not locate vendored gRPCServer.jl. " *
        "Set ARROW_FLIGHT_GRPCSERVER_PATH to an explicit checkout path.",
    )
end

const TEMP_ENV = mktempdir()
cp(joinpath(TEST_ROOT, "Project.toml"), joinpath(TEMP_ENV, "Project.toml"))

Pkg.activate(TEMP_ENV)
Pkg.develop(PackageSpec(path=ARROW_ROOT))
Pkg.develop(PackageSpec(path=ARROWTYPES_ROOT))
Pkg.develop(PackageSpec(path=locate_grpcserver()))
Pkg.instantiate()

using Test
using Arrow

include(joinpath(TEST_ROOT, "flight.jl"))
