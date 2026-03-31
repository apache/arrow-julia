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

handshake(
    client::Client,
    request::Channel{Protocol.HandshakeRequest},
    response::Channel{Protocol.HandshakeResponse};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    _handshake_client(client; kwargs...),
    request,
    response,
    headers=_merge_headers(client, headers),
)

function handshake(
    client::Client;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    request = Channel{Protocol.HandshakeRequest}(request_capacity)
    response = Channel{Protocol.HandshakeResponse}(response_capacity)
    req = handshake(client, request, response; headers=headers, kwargs...)
    return req, request, response
end

function authenticate(
    client::Client,
    requests::AbstractVector{<:Protocol.HandshakeRequest};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    req, request_channel, response_channel = handshake(client; headers=headers, kwargs...)
    for request in requests
        put!(request_channel, request)
    end
    close(request_channel)

    responses = collect(response_channel)
    gRPCClient.grpc_async_await(req)

    isempty(responses) &&
        throw(ArgumentError("Arrow Flight handshake returned no response messages"))

    return withtoken(client, responses[end].payload), responses
end

function authenticate(
    client::Client,
    payloads::AbstractVector{<:AbstractVector{UInt8}};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    requests = [
        Protocol.HandshakeRequest(UInt64(0), Vector{UInt8}(payload)) for payload in payloads
    ]
    return authenticate(client, requests; headers=headers, kwargs...)
end

function authenticate(
    client::Client,
    username::AbstractString,
    password::AbstractString;
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    return authenticate(
        client,
        [Vector{UInt8}(codeunits(username)), Vector{UInt8}(codeunits(password))];
        headers=headers,
        kwargs...,
    )
end
