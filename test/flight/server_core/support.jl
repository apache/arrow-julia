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

function flight_server_core_fixture()
    protocol = Arrow.Flight.Protocol
    context = Arrow.Flight.ServerCallContext(
        headers=["authorization" => "Bearer test", "auth-token-bin" => UInt8[0x01, 0x02]],
        peer="127.0.0.1:4000",
        secure=true,
    )
    descriptor_info = Arrow.Flight.servicedescriptor(Arrow.Flight.Service())
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["server", "dataset"])
    service = Arrow.Flight.Service()
    implemented = Arrow.Flight.Service(
        getflightinfo=(ctx, req) -> begin
            @test ctx === context
            @test req.path == descriptor.path
            return protocol.FlightInfo(
                UInt8[],
                req,
                protocol.FlightEndpoint[],
                7,
                42,
                false,
                UInt8[],
            )
        end,
        doget=(ctx, ticket, response) -> begin
            @test ctx === context
            @test ticket.ticket == b"ticket-1"
            put!(response, protocol.FlightData(nothing, UInt8[], UInt8[], UInt8[]))
            close(response)
            return :doget_ok
        end,
        listactions=(ctx, response) -> begin
            @test ctx === context
            put!(response, protocol.ActionType("ping", "Ping action"))
            close(response)
            return :listactions_ok
        end,
    )
    return (; protocol, context, descriptor_info, descriptor, service, implemented)
end
