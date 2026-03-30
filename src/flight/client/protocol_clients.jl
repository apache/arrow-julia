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

_handshake_client(client::Client; kwargs...) = Protocol.FlightService_Handshake_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

_listflights_client(client::Client; kwargs...) = Protocol.FlightService_ListFlights_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

_getflightinfo_client(client::Client; kwargs...) =
    Protocol.FlightService_GetFlightInfo_Client(
        client.host,
        client.port;
        _rpc_options(client; kwargs...)...,
    )

_pollflightinfo_client(client::Client; kwargs...) =
    Protocol.FlightService_PollFlightInfo_Client(
        client.host,
        client.port;
        _rpc_options(client; kwargs...)...,
    )

_getschema_client(client::Client; kwargs...) = Protocol.FlightService_GetSchema_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

_doget_client(client::Client; kwargs...) = Protocol.FlightService_DoGet_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

_doput_client(client::Client; kwargs...) = Protocol.FlightService_DoPut_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

_doexchange_client(client::Client; kwargs...) = Protocol.FlightService_DoExchange_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

_doaction_client(client::Client; kwargs...) = Protocol.FlightService_DoAction_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)

_listactions_client(client::Client; kwargs...) = Protocol.FlightService_ListActions_Client(
    client.host,
    client.port;
    _rpc_options(client; kwargs...)...,
)
