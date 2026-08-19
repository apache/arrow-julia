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

# The acceptance batteries exercise the package's internals wholesale.
# Rather than maintain a hundred-name import list, this module aliases every
# binding the package defines.
module Batteries

using Test
using Tables
using PooledArrays
import Base64
using Arrow

include("battery_prelude.jl")

include("battery_helpers.jl")
include("ipc_read_battery.jl")
include("ipc_write_battery.jl")
include("cdata_battery.jl")
include("scan_battery.jl")

# The batteries signal failure by throwing; a testset that completes is the
# pass.
@testset "IPC read acceptance" begin
    ipc_read_battery()
end
@testset "IPC write acceptance" begin
    ipc_write_battery()
end
@testset "C data acceptance" begin
    cdata_battery()
end
@testset "Scan acceptance" begin
    _stats_main()
    filebytes, af, full = _scan_main()
    _ranged_main(filebytes, af, full)
end

end # module Batteries
