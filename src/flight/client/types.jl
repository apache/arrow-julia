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

struct Client
    host::String
    port::Int64
    secure::Bool
    grpc::gRPCClient.gRPCCURL
    deadline::Float64
    keepalive::Float64
    max_send_message_length::Int64
    max_recieve_message_length::Int64
    headers::Vector{HeaderPair}
    tls_root_certs::Union{Nothing,String}
    cert_chain::Union{Nothing,String}
    private_key::Union{Nothing,String}
    key_password::Union{Nothing,String}
    disable_server_verification::Bool
end

function Client(
    host,
    port;
    secure::Bool=false,
    grpc::gRPCClient.gRPCCURL=gRPCClient.grpc_global_handle(),
    deadline::Real=10,
    keepalive::Real=60,
    max_send_message_length::Integer=DEFAULT_MAX_MESSAGE_LENGTH,
    max_recieve_message_length::Integer=DEFAULT_MAX_MESSAGE_LENGTH,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    tls_root_certs::Union{Nothing,AbstractString}=nothing,
    cert_chain::Union{Nothing,AbstractString}=nothing,
    private_key::Union{Nothing,AbstractString}=nothing,
    key_password::Union{Nothing,AbstractString}=nothing,
    disable_server_verification::Bool=false,
)
    Client(
        String(host),
        Int64(port),
        secure,
        grpc,
        Float64(deadline),
        Float64(keepalive),
        Int64(max_send_message_length),
        Int64(max_recieve_message_length),
        _normalize_headers(headers),
        isnothing(tls_root_certs) ? nothing : String(tls_root_certs),
        isnothing(cert_chain) ? nothing : String(cert_chain),
        isnothing(private_key) ? nothing : String(private_key),
        isnothing(key_password) ? nothing : String(key_password),
        disable_server_verification,
    )
end

Client(location::Protocol.Location; kwargs...) = Client(location.uri; kwargs...)

function Client(uri::AbstractString; kwargs...)
    secure, host, port = _parse_location(String(uri))
    Client(host, port; secure=secure, kwargs...)
end

function _rebuild_client(client::Client; headers::AbstractVector{<:Pair}=client.headers)
    return Client(
        client.host,
        client.port;
        secure=client.secure,
        grpc=client.grpc,
        deadline=client.deadline,
        keepalive=client.keepalive,
        max_send_message_length=client.max_send_message_length,
        max_recieve_message_length=client.max_recieve_message_length,
        headers=headers,
        tls_root_certs=client.tls_root_certs,
        cert_chain=client.cert_chain,
        private_key=client.private_key,
        key_password=client.key_password,
        disable_server_verification=client.disable_server_verification,
    )
end
