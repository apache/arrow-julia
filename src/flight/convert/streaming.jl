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

streambytes(message::Protocol.FlightData; kwargs...) =
    streambytes(Protocol.FlightData[message]; kwargs...)

function _missing_schema_message()
    return join(
        [
            "cannot derive Arrow Flight schema from a response stream without a schema message",
            "the server may have terminated the stream before emitting the first schema-bearing FlightData message",
            "or the underlying transport did not surface the corresponding gRPC status",
        ],
        "; ",
    )
end

function _require_schema_messages(messages::AbstractVector{<:Protocol.FlightData}, schema)
    schema === nothing || return messages
    any(message -> !isempty(message.data_header), messages) && return messages
    throw(ArgumentError(_missing_schema_message()))
end

function streambytes(
    messages;
    schema=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    collected = _require_schema_messages(_collect_messages(messages), schema)
    io = IOBuffer()
    schema === nothing || Base.write(io, schemaipc(schema; alignment=alignment))
    for message in collected
        if isempty(message.data_header)
            isempty(message.data_body) || throw(
                ArgumentError("FlightData message has a body but no Arrow IPC header"),
            )
            continue
        end
        _write_framed_message(io, message.data_header, message.data_body, alignment)
    end
    end_marker && _write_end_marker(io)
    return take!(io)
end

function stream(
    messages;
    schema=nothing,
    convert::Bool=true,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    bytes = streambytes(messages; schema=schema, alignment=alignment, end_marker=end_marker)
    return ArrowParent.Stream(bytes; convert=convert)
end

function table(
    messages;
    schema=nothing,
    convert::Bool=true,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    bytes = streambytes(messages; schema=schema, alignment=alignment, end_marker=end_marker)
    return ArrowParent.Table(bytes; convert=convert)
end
