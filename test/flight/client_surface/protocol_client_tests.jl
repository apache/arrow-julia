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

function flight_client_surface_test_protocol_clients(fixture)
    client = fixture.client

    @test isdefined(fixture.protocol, :FlightService_Handshake_Client)
    @test isdefined(fixture.protocol, :FlightService_ListFlights_Client)
    @test isdefined(fixture.protocol, :FlightService_GetFlightInfo_Client)
    @test isdefined(fixture.protocol, :FlightService_PollFlightInfo_Client)
    @test isdefined(fixture.protocol, :FlightService_GetSchema_Client)
    @test isdefined(fixture.protocol, :FlightService_DoGet_Client)
    @test isdefined(fixture.protocol, :FlightService_DoPut_Client)
    @test isdefined(fixture.protocol, :FlightService_DoExchange_Client)
    @test isdefined(fixture.protocol, :FlightService_DoAction_Client)
    @test isdefined(fixture.protocol, :FlightService_ListActions_Client)

    @test Arrow.Flight._handshake_client(client).path ==
          "/arrow.flight.protocol.FlightService/Handshake"
    @test Arrow.Flight._listflights_client(client).path ==
          "/arrow.flight.protocol.FlightService/ListFlights"
    @test Arrow.Flight._getflightinfo_client(client).path ==
          "/arrow.flight.protocol.FlightService/GetFlightInfo"
    @test Arrow.Flight._pollflightinfo_client(client).path ==
          "/arrow.flight.protocol.FlightService/PollFlightInfo"
    @test Arrow.Flight._getschema_client(client).path ==
          "/arrow.flight.protocol.FlightService/GetSchema"
    @test Arrow.Flight._doget_client(client).path ==
          "/arrow.flight.protocol.FlightService/DoGet"
    @test Arrow.Flight._doput_client(client).path ==
          "/arrow.flight.protocol.FlightService/DoPut"
    @test Arrow.Flight._doexchange_client(client).path ==
          "/arrow.flight.protocol.FlightService/DoExchange"
    @test Arrow.Flight._doaction_client(client).path ==
          "/arrow.flight.protocol.FlightService/DoAction"
    @test Arrow.Flight._listactions_client(client).path ==
          "/arrow.flight.protocol.FlightService/ListActions"
end
