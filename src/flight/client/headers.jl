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

_normalize_header_value(value::AbstractString) = String(value)
_normalize_header_value(value::AbstractVector{UInt8}) = Vector{UInt8}(value)
function _normalize_header_value(value)
    throw(
        ArgumentError(
            "Arrow Flight header values must be strings or byte vectors, got $(typeof(value))",
        ),
    )
end

function _normalize_headers(headers::AbstractVector{<:Pair})
    normalized = HeaderPair[]
    for header in headers
        push!(normalized, String(first(header)) => _normalize_header_value(last(header)))
    end
    return normalized
end

withheaders(client::Client, headers::Pair...) = withheaders(client, collect(headers))

function withheaders(client::Client, headers::AbstractVector{<:Pair})
    merged_headers = copy(client.headers)
    append!(merged_headers, _normalize_headers(headers))
    return _rebuild_client(client; headers=merged_headers)
end

withtoken(client::Client, token::AbstractString) =
    withtoken(client, Vector{UInt8}(codeunits(token)))
withtoken(client::Client, token::AbstractVector{UInt8}) =
    _withreplacedheader(client, AUTH_TOKEN_HEADER => Vector{UInt8}(token))

function _withreplacedheader(client::Client, header::Pair)
    normalized_header = String(first(header)) => _normalize_header_value(last(header))
    name = lowercase(first(normalized_header))
    filtered_headers = HeaderPair[
        existing for existing in client.headers if lowercase(first(existing)) != name
    ]
    push!(filtered_headers, normalized_header)
    return _rebuild_client(client; headers=filtered_headers)
end

function _header_lines(headers::AbstractVector{HeaderPair})
    lines = String[]
    for (name, value) in headers
        isempty(name) && throw(ArgumentError("Arrow Flight header names must not be empty"))
        any(ch -> ch == '\r' || ch == '\n', name) &&
            throw(ArgumentError("Arrow Flight header names must not contain newlines"))
        rendered_value = _render_header_value(name, value)
        any(ch -> ch == '\r' || ch == '\n', rendered_value) &&
            throw(ArgumentError("Arrow Flight header values must not contain newlines"))
        push!(lines, string(name, ": ", rendered_value))
    end
    return lines
end

function _render_header_value(name::String, value::String)
    if endswith(lowercase(name), "-bin")
        return Base64.base64encode(codeunits(value))
    end
    return value
end

function _render_header_value(name::String, value::Vector{UInt8})
    endswith(lowercase(name), "-bin") ||
        throw(ArgumentError("Arrow Flight binary header values require a '-bin' suffix"))
    return Base64.base64encode(value)
end

function _merge_headers(client::Client, headers::AbstractVector{<:Pair}=HeaderPair[])
    merged_headers = copy(client.headers)
    append!(merged_headers, _normalize_headers(headers))
    return merged_headers
end
