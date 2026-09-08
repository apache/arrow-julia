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

# Core unit tests (ArrowCore in isolation).
include("core_tests.jl")

# The public facade (Arrow.Table / Arrow.Stream / Arrow.write).
include("facade_tests.jl")
include("sharedvalues.jl")

# ArrowTypes logical-type lowering, extension metadata, and lifting.
include("arrowtypes_compat_tests.jl")

# Seeded end-to-end properties over public IPC paths.
include("property_tests.jl")

# Persist only referenced content from shared Utf8View/BinaryView buffers.
include("ipc_view_output_tests.jl")

# Read-then-rewrite fidelity through the facade, plus reader-budget accounting.
include("rewrite_regressions.jl")

# The Arrow 2.x compatibility surface (getmetadata, tobuffer, curried write,
# removed-keyword warnings, typed scan overrides) and the ArrowTimeZonesExt
# child suite.
include("compat_tests.jl")

# The incremental writer (Arrow.Writer) and stream append (Arrow.append).
include("incremental_writer_tests.jl")
include("writer_sink_tests.jl")

# Shared acceptance/conformance support contracts and adapter composition.
include("conformance_support_tests.jl")

# The CloudStore.jl extension against a local S3-compatible server.
include("cloudstore_tests.jl")

# The adapter acceptance batteries: assertion-dense scripts over the
# package's internals, sharing one explicit private support module.
include("batteries.jl")

# Package hygiene: compat bounds, stale dependencies, ambiguities, exports,
# and unbound type parameters.
Aqua.test_all(Arrow)
