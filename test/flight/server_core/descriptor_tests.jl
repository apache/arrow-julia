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

function flight_server_core_test_descriptors(fixture)
    handshake_descriptor = Arrow.Flight.lookupmethod(fixture.descriptor_info, "Handshake")
    @test !isnothing(handshake_descriptor)
    @test handshake_descriptor.path == "/arrow.flight.protocol.FlightService/Handshake"
    @test handshake_descriptor.request_streaming
    @test handshake_descriptor.response_streaming
    @test handshake_descriptor.request_type === fixture.protocol.HandshakeRequest
    @test handshake_descriptor.response_type === fixture.protocol.HandshakeResponse

    doget_descriptor = Arrow.Flight.lookupmethod(
        fixture.descriptor_info,
        "/arrow.flight.protocol.FlightService/DoGet",
    )
    @test !isnothing(doget_descriptor)
    @test !doget_descriptor.request_streaming
    @test doget_descriptor.response_streaming
    @test doget_descriptor.request_type === fixture.protocol.Ticket
    @test doget_descriptor.response_type === fixture.protocol.FlightData
    @test isnothing(Arrow.Flight.lookupmethod(fixture.descriptor_info, "MissingMethod"))
end
