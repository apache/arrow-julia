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

@testset "Flight TLS interop" begin
    mktempdir() do dir
        tls_material = FlightTestSupport.generate_test_tls_certificate(dir)
        if isnothing(tls_material)
            @test true
            return
        end
        cert_path, key_path = tls_material
        server = FlightTestSupport.start_tls_flight_server(cert_path, key_path)
        if isnothing(server)
            @test true
        else
            protocol = Arrow.Flight.Protocol
            descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
            descriptor = protocol.FlightDescriptor(
                descriptor_type.PATH,
                UInt8[],
                ["interop", "tls", "download"],
            )

            try
                FlightTestSupport.with_test_grpc_handle() do grpc
                    client = Arrow.Flight.Client(
                        "grpc+tls://localhost:$(server.port)";
                        grpc=grpc,
                        tls_root_certs=cert_path,
                    )
                    info = Arrow.Flight.getflightinfo(client, descriptor)
                    @test info.total_records == 3
                    @test length(info.endpoint) == 1

                    schema = Arrow.Flight.getschema(client, descriptor)
                    @test Arrow.Flight.schemaipc(schema) == Arrow.Flight.schemaipc(info)

                    req, channel = Arrow.Flight.doget(client, info.endpoint[1].ticket)
                    messages = collect(channel)
                    gRPCClient.grpc_async_await(req)

                    table = Arrow.Flight.table(messages; schema=info)
                    @test table.id == [31, 32, 33]
                    @test table.name == ["thirty-one", "thirty-two", "thirty-three"]
                end

                FlightTestSupport.with_test_grpc_handle() do grpc
                    insecure_client = Arrow.Flight.Client(
                        "grpc+tls://localhost:$(server.port)";
                        grpc=grpc,
                        disable_server_verification=true,
                    )
                    info = Arrow.Flight.getflightinfo(insecure_client, descriptor)
                    @test info.total_records == 3
                end
            finally
                FlightTestSupport.stop_pyarrow_flight_server(server)
            end
        end
    end
end
