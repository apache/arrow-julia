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

function flight_server_core_test_direct_handlers(fixture)
    @test_throws gRPCClient.gRPCServiceCallException Arrow.Flight.getflightinfo(
        fixture.service,
        fixture.context,
        fixture.descriptor,
    )

    info =
        Arrow.Flight.getflightinfo(fixture.implemented, fixture.context, fixture.descriptor)
    @test info.total_records == 7
    @test info.total_bytes == 42
    @test info.flight_descriptor.path == ["server", "dataset"]

    get_response = Channel{fixture.protocol.FlightData}(1)
    @test Arrow.Flight.doget(
        fixture.implemented,
        fixture.context,
        fixture.protocol.Ticket(b"ticket-1"),
        get_response,
    ) == :doget_ok
    @test length(collect(get_response)) == 1

    actions_response = Channel{fixture.protocol.ActionType}(1)
    @test Arrow.Flight.listactions(
        fixture.implemented,
        fixture.context,
        actions_response,
    ) == :listactions_ok
    actions = collect(actions_response)
    @test length(actions) == 1
    @test getfield(actions[1], Symbol("#type")) == "ping"
    @test actions[1].description == "Ping action"
end
