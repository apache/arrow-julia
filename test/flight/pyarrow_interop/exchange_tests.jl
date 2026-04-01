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

function pyarrow_interop_test_exchange(client, exchange_descriptor)
    exchange_source = Tables.partitioner((
        (id=Int64[21, 22], name=["twenty-one", "twenty-two"]),
        (id=Int64[23], name=["twenty-three"]),
    ))
    exchange_metadata = Dict("dataset" => "interop-exchange")
    exchange_colmetadata = Dict(:name => Dict("lang" => "en"))
    exchange_app_metadata = ["client:0", "client:1"]
    exchange_source =
        Arrow.Flight.withappmetadata(exchange_source; app_metadata=exchange_app_metadata)
    exchange_req, exchange_response = Arrow.Flight.doexchange(
        client,
        exchange_source;
        descriptor=exchange_descriptor,
        metadata=exchange_metadata,
        colmetadata=exchange_colmetadata,
    )
    exchanged_messages = Arrow.Flight.Protocol.FlightData[]
    exchange_batches = collect(
        Arrow.Flight.stream((
            (push!(exchanged_messages, message); message) for message in exchange_response
        ),),
    )
    gRPCClient.grpc_async_await(exchange_req)

    @test length(exchange_batches) == 2
    @test exchange_batches[1].id == [21, 22]
    @test exchange_batches[1].name == ["twenty-one", "twenty-two"]
    @test DataAPI.metadata(exchange_batches[1], "dataset") == "interop-exchange"
    @test DataAPI.colmetadata(exchange_batches[1], :name, "lang") == "en"
    @test exchange_batches[2].id == [23]
    @test exchange_batches[2].name == ["twenty-three"]
    @test DataAPI.metadata(exchange_batches[2], "dataset") == "interop-exchange"
    @test DataAPI.colmetadata(exchange_batches[2], :name, "lang") == "en"
    exchange_table = Arrow.Flight.table(exchanged_messages)
    @test exchange_table.id == [21, 22, 23]
    @test exchange_table.name == ["twenty-one", "twenty-two", "twenty-three"]
    @test DataAPI.metadata(exchange_table, "dataset") == "interop-exchange"
    @test DataAPI.colmetadata(exchange_table, :name, "lang") == "en"
    @test filter(!isempty, getfield.(exchanged_messages, :app_metadata)) ==
          [b"client:0", b"client:1"]

    exchange_batches_with_app =
        collect(Arrow.Flight.stream(exchanged_messages; include_app_metadata=true))
    @test exchange_batches_with_app[1].table.id == [21, 22]
    @test exchange_batches_with_app[2].table.id == [23]
    @test String.(getproperty.(exchange_batches_with_app, :app_metadata)) ==
          exchange_app_metadata

    exchange_table_with_app =
        Arrow.Flight.table(exchanged_messages; include_app_metadata=true)
    @test exchange_table_with_app.table.id == [21, 22, 23]
    @test exchange_table_with_app.table.name == ["twenty-one", "twenty-two", "twenty-three"]
    @test String.(exchange_table_with_app.app_metadata) == exchange_app_metadata
end
