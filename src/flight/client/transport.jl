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

function _append_headers_unlocked!(
    req::gRPCClient.gRPCRequest,
    headers::AbstractVector{HeaderPair},
)
    isempty(headers) && return req
    for header_line in _header_lines(headers)
        req.headers = gRPCClient.curl_slist_append(req.headers, header_line)
    end
    gRPCClient.curl_easy_setopt(req.easy, gRPCClient.CURLOPT_HTTPHEADER, req.headers)
    return req
end

function _apply_tls_options_unlocked!(client::Client, req::gRPCClient.gRPCRequest)
    if !client.secure
        return req
    end

    if client.disable_server_verification
        gRPCClient.curl_easy_setopt(req.easy, gRPCClient.CURLOPT_SSL_VERIFYPEER, Clong(0))
        gRPCClient.curl_easy_setopt(req.easy, gRPCClient.CURLOPT_SSL_VERIFYHOST, Clong(0))
    else
        gRPCClient.curl_easy_setopt(req.easy, gRPCClient.CURLOPT_SSL_VERIFYPEER, Clong(1))
        gRPCClient.curl_easy_setopt(req.easy, gRPCClient.CURLOPT_SSL_VERIFYHOST, Clong(2))
    end

    !isnothing(client.tls_root_certs) && gRPCClient.curl_easy_setopt(
        req.easy,
        gRPCClient.CURLOPT_CAINFO,
        client.tls_root_certs,
    )
    !isnothing(client.cert_chain) &&
        gRPCClient.curl_easy_setopt(req.easy, gRPCClient.CURLOPT_SSLCERT, client.cert_chain)
    !isnothing(client.private_key) &&
        gRPCClient.curl_easy_setopt(req.easy, gRPCClient.CURLOPT_SSLKEY, client.private_key)
    !isnothing(client.key_password) && gRPCClient.curl_easy_setopt(
        req.easy,
        gRPCClient.CURLOPT_KEYPASSWD,
        client.key_password,
    )

    return req
end

function _apply_client_options_unlocked!(
    client::Client,
    req::gRPCClient.gRPCRequest,
    headers::AbstractVector{HeaderPair},
)
    _append_headers_unlocked!(req, headers)
    return _apply_tls_options_unlocked!(client, req)
end

function _grpc_sync_request(
    client::Client,
    rpc_client::gRPCClient.gRPCServiceClient{TRequest,false,TResponse,false},
    request::TRequest;
    headers::AbstractVector{HeaderPair}=HeaderPair[],
) where {TRequest<:Any,TResponse<:Any}
    req = lock(rpc_client.grpc.lock) do
        req = gRPCClient.grpc_async_request(rpc_client, request)
        _apply_client_options_unlocked!(client, req, headers)
    end
    return gRPCClient.grpc_async_await(rpc_client, req)
end

function _grpc_async_request(
    client::Client,
    rpc_client::gRPCClient.gRPCServiceClient{TRequest,false,TResponse,true},
    request::TRequest,
    response::Channel{TResponse};
    headers::AbstractVector{HeaderPair}=HeaderPair[],
) where {TRequest<:Any,TResponse<:Any}
    return lock(rpc_client.grpc.lock) do
        req = gRPCClient.grpc_async_request(rpc_client, request, response)
        _apply_client_options_unlocked!(client, req, headers)
    end
end

function _grpc_async_request(
    client::Client,
    rpc_client::gRPCClient.gRPCServiceClient{TRequest,true,TResponse,false},
    request::Channel{TRequest},
    response::Channel{TResponse};
    headers::AbstractVector{HeaderPair}=HeaderPair[],
) where {TRequest<:Any,TResponse<:Any}
    return lock(rpc_client.grpc.lock) do
        req = gRPCClient.grpc_async_request(rpc_client, request, response)
        _apply_client_options_unlocked!(client, req, headers)
    end
end

function _grpc_async_request(
    client::Client,
    rpc_client::gRPCClient.gRPCServiceClient{TRequest,true,TResponse,true},
    request::Channel{TRequest},
    response::Channel{TResponse};
    headers::AbstractVector{HeaderPair}=HeaderPair[],
) where {TRequest<:Any,TResponse<:Any}
    return lock(rpc_client.grpc.lock) do
        req = gRPCClient.grpc_async_request(rpc_client, request, response)
        _apply_client_options_unlocked!(client, req, headers)
    end
end

struct FlightAsyncRequest{R}
    request::R
    producer::Union{Nothing,Task}
end

function Base.wait(req::FlightAsyncRequest)
    producer = getfield(req, :producer)
    isnothing(producer) || wait(producer)
    return wait(getfield(req, :request))
end

function gRPCClient.grpc_async_await(req::FlightAsyncRequest)
    producer = getfield(req, :producer)
    isnothing(producer) || wait(producer)
    return gRPCClient.grpc_async_await(getfield(req, :request))
end

function gRPCClient.grpc_async_await(
    client::gRPCClient.gRPCServiceClient{TRequest,true,TResponse,false},
    req::FlightAsyncRequest,
) where {TRequest<:Any,TResponse<:Any}
    producer = getfield(req, :producer)
    isnothing(producer) || wait(producer)
    return gRPCClient.grpc_async_await(client, getfield(req, :request))
end

_default_rpc_options(client::Client) = (
    secure=client.secure,
    grpc=client.grpc,
    deadline=client.deadline,
    keepalive=client.keepalive,
    max_send_message_length=client.max_send_message_length,
    max_recieve_message_length=client.max_recieve_message_length,
)

_rpc_options(client::Client; kwargs...) =
    merge(_default_rpc_options(client), NamedTuple(kwargs))
