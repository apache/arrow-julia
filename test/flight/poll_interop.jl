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

@testset "Flight poll interop" begin
    server = FlightTestSupport.start_poll_flight_server()
    if isnothing(server)
        @test true
    else
        protocol = Arrow.Flight.Protocol
        descriptor_type = protocol.var"FlightDescriptor.DescriptorType"

        try
            FlightTestSupport.with_test_grpc_handle() do grpc
                client = Arrow.Flight.Client("grpc://127.0.0.1:$(server.port)"; grpc=grpc)
                initial_descriptor = protocol.FlightDescriptor(
                    descriptor_type.PATH,
                    UInt8[],
                    ["interop", "poll"],
                )

                first_poll = Arrow.Flight.pollflightinfo(client, initial_descriptor)
                @test !isnothing(first_poll.info)
                @test !isnothing(first_poll.flight_descriptor)
                @test first_poll.flight_descriptor.path == ["interop", "poll", "retry"]
                @test first_poll.info.total_records == 1
                @test first_poll.info.ordered
                @test first_poll.progress ≈ 0.5
                @test Arrow.Flight.schemaipc(first_poll.info) == Arrow.Flight.schemaipc(
                    protocol.SchemaResult(first_poll.info.schema[5:end]),
                )

                second_poll =
                    Arrow.Flight.pollflightinfo(client, first_poll.flight_descriptor)
                @test !isnothing(second_poll.info)
                @test isnothing(second_poll.flight_descriptor)
                @test second_poll.info.flight_descriptor.path == ["interop", "poll"]
                @test second_poll.progress ≈ 1.0
                @test length(second_poll.info.endpoint) == 1
                @test second_poll.info.endpoint[1].ticket.ticket == b"poll-ticket"
            end
        finally
            FlightTestSupport.stop_pyarrow_flight_server(server)
        end
    end
end
