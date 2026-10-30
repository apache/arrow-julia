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

module RangedScanTests

using Test, Arrow, Tables

struct MeteredSource <: Arrow.AbstractArrowSource
    bytes::Vector{UInt8}
    fetched::Base.RefValue{Int}
end
Arrow.sourcelength(s::MeteredSource) = length(s.bytes)
function Arrow.readrange(s::MeteredSource, offset, len)
    s.fetched[] += len
    return s.bytes[(offset + 1):(offset + len)]
end

@testset "filtered range scans stop fetching at their limit" begin
    for compress in (nothing, :lz4, :zstd)
        io = IOBuffer()
        Arrow.Writer(io; file=true, compress) do writer
            for batch = 1:32
                Arrow.write(
                    writer,
                    (
                        id=collect(((batch - 1) * 2048 + 1):(batch * 2048)),
                        category=Arrow.DictEncode(fill("batch-$batch", 2048)),
                    ),
                )
            end
        end
        bytes = take!(io)
        for limit in (0, 1)
            fetched = Ref(0)
            source = Arrow.SourceFile(MeteredSource(bytes, fetched); tailbytes=1024)
            table = Arrow.Table(source; filter=Arrow.col(:id) > 0, limit)
            @test collect(table.id) == collect(1:limit)
            @test collect(table.category) == fill("batch-1", limit)
            @test fetched[] < length(bytes) ÷ 2
        end
        source = Arrow.SourceFile(MeteredSource(bytes, Ref(0)); tailbytes=1024)
        table = Arrow.Table(
            source;
            scan=Tables.Scan(filter=Arrow.col(:id) > 2048, offset=2050, limit=5),
        )
        @test collect(table.id) == collect(4099:4103)
        @test collect(table.category) == fill("batch-3", 5)
    end
end

end
