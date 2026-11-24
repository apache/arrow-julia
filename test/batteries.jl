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

# The Test adapter owns only presentation. AcceptanceSupport owns the private
# package dependencies, fixtures, runner order, and implementation.
module Batteries

using Test
include(joinpath(@__DIR__, "support", "AcceptanceSupport.jl"))

# The batteries signal failure by throwing; a testset that completes is the
# pass.
const ACCEPTANCE_SUITES = AcceptanceSupport.acceptance_suites()
@assert map(first, ACCEPTANCE_SUITES) == (
    "IPC read acceptance",
    "IPC write acceptance",
    "C data acceptance",
    "Scan acceptance",
)
for (name, runner) in ACCEPTANCE_SUITES
    @testset "$name" begin
        runner()
    end
end

end # module Batteries
