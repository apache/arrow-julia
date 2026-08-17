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

# Fresh-process typed-materialization allocation pin (codex round 45): the
# recursion architecture must not box per row in a process that never ran
# compiler introspection — warm-up alone must reach steady state. The
# bound sits between the intended cost (~80 B/row: the output vector plus
# the pre-existing struct validity lookup) and the boxed regression
# (~240 B/row).
using Arrow
const AC = Arrow.ArrowCore

c1 = AC.fromjulia("a", collect(Int64, 1:100_000))
c2 = AC.fromjulia("b", collect(Int64, 1:100_000))
sf = AC.Field("st", AC.StructType(); nullable=false, children=[c1[1], c2[1]])
sd = AC.ArrayData(AC.StructType(), 100_000, [AC.BufferSlice()];
    children=[c1[2], c2[2]], nullcount=0)
NT = NamedTuple{(:a, :b),Tuple{Int64,Int64}}
for _ = 1:3
    AC.materialize(NT, sf, sd)
end
bytes = @allocated AC.materialize(NT, sf, sd)
bytes < 12_000_000 || error("typed struct materialization allocates " *
    "$bytes bytes per 100k rows; the recursion edge is boxing again")
println("typed alloc ok: $bytes")
