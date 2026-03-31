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

function _unimplemented(service_method::String)
    throw(
        gRPCClient.gRPCServiceCallException(
            gRPCClient.GRPC_UNIMPLEMENTED,
            "Arrow Flight server method $(service_method) is not implemented",
        ),
    )
end

function _invoke_handler(handler::Union{Nothing,Function}, service_method::String, args...)
    isnothing(handler) && _unimplemented(service_method)
    return handler(args...)
end

handshake(
    service::Service,
    context::ServerCallContext,
    request::Channel{Protocol.HandshakeRequest},
    response::Channel{Protocol.HandshakeResponse},
) = _invoke_handler(service.handshake, "Handshake", context, request, response)

listflights(
    service::Service,
    context::ServerCallContext,
    criteria::Protocol.Criteria,
    response::Channel{Protocol.FlightInfo},
) = _invoke_handler(service.listflights, "ListFlights", context, criteria, response)

getflightinfo(
    service::Service,
    context::ServerCallContext,
    descriptor::Protocol.FlightDescriptor,
) = _invoke_handler(service.getflightinfo, "GetFlightInfo", context, descriptor)

pollflightinfo(
    service::Service,
    context::ServerCallContext,
    descriptor::Protocol.FlightDescriptor,
) = _invoke_handler(service.pollflightinfo, "PollFlightInfo", context, descriptor)

getschema(
    service::Service,
    context::ServerCallContext,
    descriptor::Protocol.FlightDescriptor,
) = _invoke_handler(service.getschema, "GetSchema", context, descriptor)

doget(
    service::Service,
    context::ServerCallContext,
    ticket::Protocol.Ticket,
    response::Channel{Protocol.FlightData},
) = _invoke_handler(service.doget, "DoGet", context, ticket, response)

doput(
    service::Service,
    context::ServerCallContext,
    request::Channel{Protocol.FlightData},
    response::Channel{Protocol.PutResult},
) = _invoke_handler(service.doput, "DoPut", context, request, response)

doexchange(
    service::Service,
    context::ServerCallContext,
    request::Channel{Protocol.FlightData},
    response::Channel{Protocol.FlightData},
) = _invoke_handler(service.doexchange, "DoExchange", context, request, response)

doaction(
    service::Service,
    context::ServerCallContext,
    action::Protocol.Action,
    response::Channel{Protocol.Result},
) = _invoke_handler(service.doaction, "DoAction", context, action, response)

listactions(
    service::Service,
    context::ServerCallContext,
    response::Channel{Protocol.ActionType},
) = _invoke_handler(service.listactions, "ListActions", context, response)
