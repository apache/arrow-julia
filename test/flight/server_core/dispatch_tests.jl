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

function flight_server_core_test_dispatch(fixture)
    dispatch_info = Arrow.Flight.dispatch(
        fixture.implemented,
        fixture.context,
        "/arrow.flight.protocol.FlightService/GetFlightInfo",
        fixture.descriptor,
    )
    @test dispatch_info.total_records == 7
    @test dispatch_info.flight_descriptor.path == ["server", "dataset"]

    doget_descriptor = Arrow.Flight.lookupmethod(
        fixture.descriptor_info,
        "/arrow.flight.protocol.FlightService/DoGet",
    )
    get_response = Channel{fixture.protocol.FlightData}(1)
    @test Arrow.Flight.dispatch(
        fixture.implemented,
        fixture.context,
        doget_descriptor,
        fixture.protocol.Ticket(b"ticket-1"),
        get_response,
    ) == :doget_ok
    @test length(collect(get_response)) == 1

    actions_response = Channel{fixture.protocol.ActionType}(1)
    @test Arrow.Flight.dispatch(
        fixture.implemented,
        fixture.context,
        "ListActions",
        actions_response,
    ) == :listactions_ok
    @test length(collect(actions_response)) == 1
    @test_throws ArgumentError Arrow.Flight.dispatch(
        fixture.implemented,
        fixture.context,
        "/arrow.flight.protocol.FlightService/MissingMethod",
        fixture.descriptor,
    )
end
