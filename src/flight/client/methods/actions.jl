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

doaction(
    client::Client,
    action::Protocol.Action,
    response::Channel{Protocol.Result};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
) = _grpc_async_request(
    client,
    _doaction_client(client; kwargs...),
    action,
    response;
    headers=_merge_headers(client, headers),
)

function doaction(
    client::Client,
    action::Protocol.Action;
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    response = Channel{Protocol.Result}(response_capacity)
    req = doaction(client, action, response; headers=headers, kwargs...)
    return req, response
end

function listactions(
    client::Client,
    response::Channel{Protocol.ActionType};
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    return _grpc_async_request(
        client,
        _listactions_client(client; kwargs...),
        Protocol.Empty(),
        response,
        headers=_merge_headers(client, headers),
    )
end

function listactions(
    client::Client;
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    kwargs...,
)
    response = Channel{Protocol.ActionType}(response_capacity)
    req = listactions(client, response; headers=headers, kwargs...)
    return req, response
end
