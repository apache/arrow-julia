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

# The acceptance batteries exercise the package's internals wholesale (they
# were the prove-out's standalone example mains). Rather than maintain a
# hundred-name import list, this module aliases every binding the package
# defines; the facade arc will replace battery-style access with the public
# API and per-name imports.
module Batteries

using Test
using Tables
using PooledArrays
import Base64
using Arrow

# names(all=true) covers Arrow's own bindings; ArrowCore's exported names
# reach Arrow through `using` and need listing explicitly.
for n in union(names(Arrow; all=true), names(Arrow.ArrowCore))
    sn = String(n)
    (startswith(sn, "#") || n in (:eval, :include, :Arrow)) && continue
    isdefined(Arrow, n) || continue
    @eval const $n = Arrow.$n
end

include("battery_helpers.jl")
include("ipc_read_battery.jl")
include("ipc_write_battery.jl")
include("cdata_battery.jl")
include("scan_battery.jl")

@testset "IPC read acceptance" begin
    ipc_read_battery()
    @test true
end
@testset "IPC write acceptance" begin
    ipc_write_battery()
    @test true
end
@testset "C data acceptance" begin
    cdata_battery()
    @test true
end
@testset "Ranged scan acceptance" begin
    _scan_main()
    @test true
end

end # module Batteries
