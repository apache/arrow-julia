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

module RangedScanBudgetTests

using Test, Arrow

struct BytesSource <: Arrow.AbstractArrowSource
    bytes::Vector{UInt8}
end
Arrow.sourcelength(s::BytesSource) = length(s.bytes)
Arrow.readrange(s::BytesSource, offset, len) = s.bytes[(offset + 1):(offset + len)]

@testset "filtered range scans budget many batches linearly" begin
    for compress in (nothing, :lz4, :zstd)
        io = IOBuffer()
        Arrow.Writer(io; file=true, compress) do writer
            for i = 1:3000
                Arrow.write(writer, (x=Int32[i],))
            end
        end
        source = Arrow.SourceFile(BytesSource(take!(io)))
        table = Arrow.Table(source; filter=Arrow.col(:x) < 0, limit=1)
        @test isempty(table.x)
    end
end

end
