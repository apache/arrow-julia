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

@testset "Flight handshake interop" begin
    server = FlightTestSupport.start_handshake_flight_server()
    if isnothing(server)
        @test true
    else
        protocol = Arrow.Flight.Protocol

        try
            FlightTestSupport.with_test_grpc_handle() do grpc
                client = Arrow.Flight.Client("grpc://127.0.0.1:$(server.port)"; grpc=grpc)

                handshake_req, handshake_request, handshake_response =
                    Arrow.Flight.handshake(client)
                put!(handshake_request, protocol.HandshakeRequest(UInt64(0), b"test"))
                put!(handshake_request, protocol.HandshakeRequest(UInt64(0), b"p4ssw0rd"))
                close(handshake_request)

                handshake_messages = collect(handshake_response)
                gRPCClient.grpc_async_await(handshake_req)

                @test length(handshake_messages) == 1
                @test handshake_messages[1].protocol_version == 0
                @test handshake_messages[1].payload == b"secret:test"

                token_client = Arrow.Flight.withtoken(client, handshake_messages[1].payload)
                actions_req, actions_channel = Arrow.Flight.listactions(token_client)
                actions = collect(actions_channel)
                gRPCClient.grpc_async_await(actions_req)
                @test actions ==
                      [protocol.ActionType("authenticated", "Requires a valid auth token")]

                auth_client, auth_messages =
                    Arrow.Flight.authenticate(client, "test", "p4ssw0rd")
                @test length(auth_messages) == 1
                @test auth_messages[1].protocol_version ==
                      handshake_messages[1].protocol_version
                @test auth_messages[1].payload == handshake_messages[1].payload
                @test auth_client.headers == ["auth-token-bin" => b"secret:test"]

                bad_req, bad_request, bad_response = Arrow.Flight.handshake(client)
                put!(bad_request, protocol.HandshakeRequest(UInt64(0), b"test"))
                put!(bad_request, protocol.HandshakeRequest(UInt64(0), b"wrong"))
                close(bad_request)

                @test isempty(collect(bad_response))
                @test_throws gRPCClient.gRPCServiceCallException gRPCClient.grpc_async_await(
                    bad_req,
                )
            end
        finally
            FlightTestSupport.stop_pyarrow_flight_server(server)
        end
    end
end
