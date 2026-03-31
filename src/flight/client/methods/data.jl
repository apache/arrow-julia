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

doget(
    client::Client,
    ticket::Protocol.Ticket,
    response::Channel{Protocol.FlightData};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    _doget_client(client; kwargs...),
    ticket,
    response;
    headers=_merge_headers(client, headers),
)

function doget(
    client::Client,
    ticket::Protocol.Ticket;
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    response = Channel{Protocol.FlightData}(response_capacity)
    req = doget(client, ticket, response; headers=headers, kwargs...)
    return req, response
end

doput(
    client::Client,
    request::Channel{Protocol.FlightData},
    response::Channel{Protocol.PutResult};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    _doput_client(client; kwargs...),
    request,
    response;
    headers=_merge_headers(client, headers),
)

function doput(
    client::Client;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    response = Channel{Protocol.PutResult}(response_capacity)
    req = doput(client, request, response; headers=headers, kwargs...)
    return req, request, response
end

doexchange(
    client::Client,
    request::Channel{Protocol.FlightData},
    response::Channel{Protocol.FlightData};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    _doexchange_client(client; kwargs...),
    request,
    response,
    headers=_merge_headers(client, headers),
)

function doexchange(
    client::Client;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    response = Channel{Protocol.FlightData}(response_capacity)
    req = doexchange(client, request, response; headers=headers, kwargs...)
    return req, request, response
end
