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

function grpcserver_extension_test_bidi_streaming(grpcserver, service, fixture, metadata)
    grpc_descriptor = grpcserver.service_descriptor(service)
    protocol = Arrow.Flight.Protocol

    handshake_messages, handshake_closed, handshake_stream = grpcserver_capture_bidi_stream(
        grpcserver,
        protocol.HandshakeRequest,
        protocol.HandshakeResponse,
        fixture.handshake_requests,
    )
    grpc_descriptor.methods["Handshake"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/Handshake";
            metadata=metadata,
        ),
        handshake_stream,
    )
    @test handshake_closed[]
    @test length(handshake_messages) == 1
    @test handshake_messages[1].payload == b"native-token"

    doput_messages, doput_closed, doput_stream = grpcserver_capture_bidi_stream(
        grpcserver,
        protocol.FlightData,
        protocol.PutResult,
        fixture.messages,
    )
    grpc_descriptor.methods["DoPut"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoPut";
            metadata=metadata,
        ),
        doput_stream,
    )
    @test doput_closed[]
    @test length(doput_messages) == 1
    @test String(doput_messages[1].app_metadata) == "stored"

    doexchange_messages, doexchange_closed, doexchange_stream =
        grpcserver_capture_bidi_stream(
            grpcserver,
            protocol.FlightData,
            protocol.FlightData,
            fixture.exchange_messages,
        )
    grpc_descriptor.methods["DoExchange"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoExchange";
            metadata=metadata,
        ),
        doexchange_stream,
    )
    @test doexchange_closed[]
    @test length(doexchange_messages) == length(fixture.exchange_messages)

    failing_service = Arrow.Flight.Service(
        doexchange=(ctx, request, response) ->
            throw(ArgumentError("bidi streaming failed before first response")),
    )
    failing_descriptor = grpcserver.service_descriptor(failing_service)
    failing_messages, failing_closed, failing_stream = grpcserver_capture_bidi_stream(
        grpcserver,
        protocol.FlightData,
        protocol.FlightData,
        fixture.exchange_messages,
    )
    failure = try
        failing_descriptor.methods["DoExchange"].handler(
            grpcserver_extension_context(
                grpcserver,
                "/arrow.flight.protocol.FlightService/DoExchange";
                metadata=metadata,
            ),
            failing_stream,
        )
        nothing
    catch err
        err
    end
    @test failure isa ArgumentError
    @test occursin(
        "bidi streaming failed before first response",
        sprint(showerror, failure),
    )
    @test !failing_closed[]
    @test isempty(failing_messages)
end
