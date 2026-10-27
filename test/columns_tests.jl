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

# Read-side column construction for flat layouts.

using Test, Arrow, DataDecimals

@testset "column reads" begin
    @testset "decimals read in bulk, as stored" begin
        n = 100_000
        for (P, S, T) in
            ((9, 2, Int32), (18, 3, Int64), (38, 4, Int128), (76, 5, DataDecimals.Int256))
            D = Decimal{P,S,T}
            vals = [reinterpret(D, T(i) * T(7) - T(3)) for i = 1:n]
            nullable = Union{Missing,D}[isodd(i) ? vals[i] : missing for i = 1:n]
            for col in (vals, nullable)
                io = IOBuffer()
                Arrow.write(io, (d=col,))
                bytes = take!(io)
                t = Arrow.Table(bytes)
                @test eltype(t.d) == eltype(col)
                @test isequal(collect(t.d), col)
                # One bulk copy per column: no allocation per value.
                @test (@allocations Arrow.Table(bytes)) < 10_000
            end
        end
    end
end
