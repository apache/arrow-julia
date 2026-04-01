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

function doput(
    client::Client,
    source,
    response::Channel{Protocol.PutResult};
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    kwargs...,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    grpc_request = doput(client, request, response; headers=headers, kwargs...)
    producer = errormonitor(
        Threads.@spawn putflightdata!(
            request,
            source;
            close=true,
            descriptor=descriptor,
            compress=compress,
            largelists=largelists,
            denseunions=denseunions,
            dictencode=dictencode,
            dictencodenested=dictencodenested,
            alignment=alignment,
            maxdepth=maxdepth,
            metadata=metadata,
            colmetadata=colmetadata,
        )
    )
    return FlightAsyncRequest(grpc_request, producer)
end

function doput(
    client::Client,
    source;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    kwargs...,
)
    response = Channel{Protocol.PutResult}(response_capacity)
    req = doput(
        client,
        source,
        response;
        request_capacity=request_capacity,
        headers=headers,
        descriptor=descriptor,
        compress=compress,
        largelists=largelists,
        denseunions=denseunions,
        dictencode=dictencode,
        dictencodenested=dictencodenested,
        alignment=alignment,
        maxdepth=maxdepth,
        metadata=metadata,
        colmetadata=colmetadata,
        kwargs...,
    )
    return req, response
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

function doexchange(
    client::Client,
    source,
    response::Channel{Protocol.FlightData};
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    kwargs...,
)
    request = Channel{Protocol.FlightData}(request_capacity)
    grpc_request = doexchange(client, request, response; headers=headers, kwargs...)
    producer = errormonitor(
        Threads.@spawn putflightdata!(
            request,
            source;
            close=true,
            descriptor=descriptor,
            compress=compress,
            largelists=largelists,
            denseunions=denseunions,
            dictencode=dictencode,
            dictencodenested=dictencodenested,
            alignment=alignment,
            maxdepth=maxdepth,
            metadata=metadata,
            colmetadata=colmetadata,
        )
    )
    return FlightAsyncRequest(grpc_request, producer)
end

function doexchange(
    client::Client,
    source;
    request_capacity::Integer=DEFAULT_STREAM_BUFFER,
    response_capacity::Integer=DEFAULT_STREAM_BUFFER,
    headers::AbstractVector{<:Pair}=HeaderPair[],
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    kwargs...,
)
    response = Channel{Protocol.FlightData}(response_capacity)
    req = doexchange(
        client,
        source,
        response;
        request_capacity=request_capacity,
        headers=headers,
        descriptor=descriptor,
        compress=compress,
        largelists=largelists,
        denseunions=denseunions,
        dictencode=dictencode,
        dictencodenested=dictencodenested,
        alignment=alignment,
        maxdepth=maxdepth,
        metadata=metadata,
        colmetadata=colmetadata,
        kwargs...,
    )
    return req, response
end
