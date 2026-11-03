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

# Fresh-process typed-materialization allocation pin: the
# recursion architecture must not box per row in a process that never ran
# compiler introspection — warm-up alone must reach steady state. The
# bound sits between the intended cost (the output vector, 16 B/row; Julia
# 1.10's inference adds ~32 B/row of leaf-load boxing) and the boxed
# regression (~200 B/row).
using Arrow
const AC = Arrow.ArrowCore

c1 = AC.fromjulia("a", collect(Int64, 1:100_000))
c2 = AC.fromjulia("b", collect(Int64, 1:100_000))
sf = AC.Field("st", AC.StructType(); nullable=false, children=[c1[1], c2[1]])
sd = AC.ArrayData(
    AC.StructType(),
    100_000,
    [AC.BufferSlice()];
    children=[c1[2], c2[2]],
    nullcount=0,
)
NT = NamedTuple{(:a, :b),Tuple{Int64,Int64}}
for _ = 1:3
    AC.materialize(NT, sf, sd)
end
bytes = @allocated AC.materialize(NT, sf, sd)
bytes < 12_000_000 || error(
    "typed struct materialization allocates " *
    "$bytes bytes per 100k rows; the recursion edge is boxing again",
)
println("typed alloc ok: $bytes")

# Every NO-CHILD leaf layout must ride the inline fast ladder, not the
# compiled composite shell: an Interval child pins the non-juliatype-
# uniform remainder.
iv = AC.ArrayData(
    AC.IntervalType(AC.YEAR_MONTH),
    100_000,
    [AC.BufferSlice(), AC._databuffer(collect(Int32, 1:100_000))],
)
ivf = AC.Field("iv", AC.IntervalType(AC.YEAR_MONTH); nullable=false)
sf2 = AC.Field("st", AC.StructType(); nullable=false, children=[c1[1], ivf])
sd2 = AC.ArrayData(
    AC.StructType(),
    100_000,
    [AC.BufferSlice()];
    children=[c1[2], iv],
    nullcount=0,
)
NT2 = NamedTuple{(:a, :iv),Tuple{Int64,Int32}}
for _ = 1:3
    AC.materialize(NT2, sf2, sd2)
end
bytes2 = @allocated AC.materialize(NT2, sf2, sd2)
bytes2 < 12_000_000 || error(
    "typed interval-child materialization " *
    "allocates $bytes2 bytes per 100k rows; a no-child leaf layout is " *
    "routing through the compiled shell",
)
println("typed interval alloc ok: $bytes2")
