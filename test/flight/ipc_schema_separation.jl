# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@testset "Flight IPC schema separation" begin
    source = Tables.partitioner(((word=["red", "blue"],), (word=["red", "green"],)))
    messages = Arrow.Flight.flightdata(source; dictencode=true)
    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    info = Arrow.Flight.Protocol.FlightInfo(
        schema_bytes[5:end],
        nothing,
        Arrow.Flight.Protocol.FlightEndpoint[],
        Int64(-1),
        Int64(-1),
        false,
        UInt8[],
    )
    payload = messages[2:end]

    @test length(messages) >= 4
    @test Arrow.Flight.schemaipc(info) == schema_bytes

    batches = collect(Arrow.Flight.stream(payload; schema=info))
    @test length(batches) == 2
    @test isequal(batches[1].word, ["red", "blue"])
    @test isequal(batches[2].word, ["red", "green"])

    tbl = Arrow.Flight.table(payload; schema=info)
    @test isequal(tbl.word, ["red", "blue", "red", "green"])
end
