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

const FLIGHT_SERVICE_NAME = "arrow.flight.protocol.FlightService"

struct MethodDescriptor
    name::String
    path::String
    handler_field::Symbol
    request_streaming::Bool
    response_streaming::Bool
    request_type::Type
    response_type::Type
end

struct ServiceDescriptor
    name::String
    methods::Vector{MethodDescriptor}
    method_lookup::Dict{String,MethodDescriptor}
end

function MethodDescriptor(
    name::AbstractString,
    handler_field::Symbol,
    request_streaming::Bool,
    response_streaming::Bool,
    request_type::Type,
    response_type::Type,
)
    normalized_name = String(name)
    MethodDescriptor(
        normalized_name,
        "/$(FLIGHT_SERVICE_NAME)/$(normalized_name)",
        handler_field,
        request_streaming,
        response_streaming,
        request_type,
        response_type,
    )
end

function ServiceDescriptor(name::AbstractString, methods::Vector{MethodDescriptor})
    lookup = Dict{String,MethodDescriptor}()
    for method in methods
        lookup[method.name] = method
        lookup[method.path] = method
    end
    return ServiceDescriptor(String(name), methods, lookup)
end

const FLIGHT_METHODS = [
    MethodDescriptor(
        "Handshake",
        :handshake,
        true,
        true,
        Protocol.HandshakeRequest,
        Protocol.HandshakeResponse,
    ),
    MethodDescriptor(
        "ListFlights",
        :listflights,
        false,
        true,
        Protocol.Criteria,
        Protocol.FlightInfo,
    ),
    MethodDescriptor(
        "GetFlightInfo",
        :getflightinfo,
        false,
        false,
        Protocol.FlightDescriptor,
        Protocol.FlightInfo,
    ),
    MethodDescriptor(
        "PollFlightInfo",
        :pollflightinfo,
        false,
        false,
        Protocol.FlightDescriptor,
        Protocol.PollInfo,
    ),
    MethodDescriptor(
        "GetSchema",
        :getschema,
        false,
        false,
        Protocol.FlightDescriptor,
        Protocol.SchemaResult,
    ),
    MethodDescriptor("DoGet", :doget, false, true, Protocol.Ticket, Protocol.FlightData),
    MethodDescriptor("DoPut", :doput, true, true, Protocol.FlightData, Protocol.PutResult),
    MethodDescriptor(
        "DoExchange",
        :doexchange,
        true,
        true,
        Protocol.FlightData,
        Protocol.FlightData,
    ),
    MethodDescriptor("DoAction", :doaction, false, true, Protocol.Action, Protocol.Result),
    MethodDescriptor(
        "ListActions",
        :listactions,
        false,
        true,
        Protocol.Empty,
        Protocol.ActionType,
    ),
]

const FLIGHT_SERVICE_DESCRIPTOR = ServiceDescriptor(FLIGHT_SERVICE_NAME, FLIGHT_METHODS)

servicedescriptor(::Service) = FLIGHT_SERVICE_DESCRIPTOR

function lookupmethod(descriptor::ServiceDescriptor, key::AbstractString)
    return get(descriptor.method_lookup, String(key), nothing)
end

lookupmethod(service::Service, key::AbstractString) =
    lookupmethod(servicedescriptor(service), key)
