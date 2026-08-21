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

using Test
using Aqua
using Arrow
using Pkg

# Core unit tests (ArrowCore in isolation).
include("core_tests.jl")

# The public facade (Arrow.Table / Arrow.Stream / Arrow.write).
include("facade_tests.jl")

# ArrowTypes logical-type lowering, extension metadata, and lifting.
include("arrowtypes_compat_tests.jl")

# Seeded end-to-end properties over public IPC paths.
include("property_tests.jl")

# Release-blocking regressions found during the 3.0 rewrite audit.
include("rewrite_regressions.jl")

# The CloudStore.jl extension against a local S3-compatible server.
include("cloudstore_tests.jl")

# The adapter acceptance batteries: assertion-dense scripts over the
# package's internals, sharing one module that aliases the package
# namespace wholesale.
include("batteries.jl")

# Package hygiene: compat bounds, stale dependencies, ambiguities, exports,
# and unbound type parameters.
const ROOT_PROJECT = Pkg.TOML.parsefile(joinpath(pkgdir(Arrow), "Project.toml"))
const HAS_TABLES_SOURCE_OVERRIDE =
    haskey(get(ROOT_PROJECT, "sources", Dict{String,Any}()), "Tables")
# TODO: Removing the temporary Tables source override after Tables.Scan is
# released automatically re-enables this check on Julia 1.10.
Aqua.test_all(Arrow; persistent_tasks=(!(VERSION < v"1.11" && HAS_TABLES_SOURCE_OVERRIDE)))
