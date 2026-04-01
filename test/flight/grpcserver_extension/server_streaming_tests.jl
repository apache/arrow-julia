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

function grpcserver_extension_test_server_streaming(grpcserver, service, fixture, metadata)
    grpc_descriptor = grpcserver.service_descriptor(service)
    protocol = Arrow.Flight.Protocol

    doget_messages, doget_closed, doget_stream =
        grpcserver_capture_server_stream(grpcserver, protocol.FlightData)
    grpc_descriptor.methods["DoGet"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoGet";
            metadata=metadata,
        ),
        fixture.ticket,
        doget_stream,
    )
    @test doget_closed[]
    @test length(doget_messages) == length(fixture.messages)
    doget_table = Arrow.Flight.table(doget_messages; schema=fixture.info)
    @test doget_table.name == ["one", "two", "three"]
    @test Arrow.getmetadata(doget_table)["dataset"] == "native"
    @test Arrow.getmetadata(doget_table.name)["lang"] == "en"

    doget_any_messages = Any[]
    doget_any_closed = Ref(false)
    doget_any_stream = grpcserver.ServerStream{Any}(
        (message, compress) -> begin
            @test compress
            push!(doget_any_messages, message)
        end,
        () -> (doget_any_closed[] = true),
    )
    grpc_descriptor.methods["DoGet"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoGet";
            metadata=metadata,
        ),
        fixture.ticket,
        doget_any_stream,
    )
    @test doget_any_closed[]
    @test length(doget_any_messages) == length(fixture.messages)
    @test all(message -> message isa protocol.FlightData, doget_any_messages)

    actions_messages, actions_closed, actions_stream =
        grpcserver_capture_server_stream(grpcserver, protocol.ActionType)
    grpc_descriptor.methods["ListActions"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/ListActions";
            metadata=metadata,
        ),
        protocol.Empty(),
        actions_stream,
    )
    @test actions_closed[]
    @test length(actions_messages) == 1
    @test actions_messages[1].var"#type" == "ping"

    action_messages, action_closed, action_stream =
        grpcserver_capture_server_stream(grpcserver, protocol.Result)
    grpc_descriptor.methods["DoAction"].handler(
        grpcserver_extension_context(
            grpcserver,
            "/arrow.flight.protocol.FlightService/DoAction";
            metadata=metadata,
        ),
        protocol.Action("ping", UInt8[]),
        action_stream,
    )
    @test action_closed[]
    @test length(action_messages) == 1
    @test String(action_messages[1].body) == "pong"

    failing_service = Arrow.Flight.Service(
        doget=(ctx, req, response) ->
            throw(ArgumentError("server streaming failed before first response")),
    )
    failing_descriptor = grpcserver.service_descriptor(failing_service)
    failing_messages, failing_closed, failing_stream =
        grpcserver_capture_server_stream(grpcserver, protocol.FlightData)
    failure = try
        failing_descriptor.methods["DoGet"].handler(
            grpcserver_extension_context(
                grpcserver,
                "/arrow.flight.protocol.FlightService/DoGet";
                metadata=metadata,
            ),
            fixture.ticket,
            failing_stream,
        )
        nothing
    catch err
        err
    end
    @test failure isa ArgumentError
    @test occursin(
        "server streaming failed before first response",
        sprint(showerror, failure),
    )
    @test !failing_closed[]
    @test isempty(failing_messages)
end
