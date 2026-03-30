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

@testset "Flight header interop" begin
    server = FlightTestSupport.start_headers_flight_server()
    if isnothing(server)
        @test true
    else
        protocol = Arrow.Flight.Protocol

        try
            FlightTestSupport.with_test_grpc_handle() do grpc
                base_client =
                    Arrow.Flight.Client("grpc://127.0.0.1:$(server.port)"; grpc=grpc)
                client = Arrow.Flight.withheaders(
                    base_client,
                    "authorization" => "Bearer token1234",
                )

                actions_req, actions_channel = Arrow.Flight.listactions(client)
                actions = collect(actions_channel)
                gRPCClient.grpc_async_await(actions_req)
                @test actions == [
                    protocol.ActionType(
                        "echo-authorization",
                        "Return the Authorization header",
                    ),
                ]

                action_req, action_channel = Arrow.Flight.doaction(
                    client,
                    protocol.Action("echo-authorization", UInt8[]),
                )
                action_results = collect(action_channel)
                gRPCClient.grpc_async_await(action_req)
                @test length(action_results) == 1
                @test String(action_results[1].body) == "Bearer token1234"

                call_req, call_channel = Arrow.Flight.doaction(
                    base_client,
                    protocol.Action("echo-authorization", UInt8[]);
                    headers=["authorization" => "Bearer call-level"],
                )
                call_results = collect(call_channel)
                gRPCClient.grpc_async_await(call_req)
                @test length(call_results) == 1
                @test String(call_results[1].body) == "Bearer call-level"
            end
        finally
            FlightTestSupport.stop_pyarrow_flight_server(server)
        end
    end
end
