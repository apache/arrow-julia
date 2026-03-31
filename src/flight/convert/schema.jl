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

function _normalize_schemaipc(
    schema::AbstractVector{UInt8};
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
)
    bytes = Vector{UInt8}(schema)
    isempty(bytes) && throw(ArgumentError("schema bytes cannot be empty"))
    if length(bytes) >= 8 &&
       ArrowParent.readbuffer(bytes, 1, UInt32) == ArrowParent.CONTINUATION_INDICATOR_BYTES
        return bytes
    end
    if length(bytes) >= 4
        metalen = ArrowParent.readbuffer(bytes, 1, Int32)
        if metalen >= 0 && metalen == length(bytes) - 4
            io = IOBuffer()
            Base.write(io, ArrowParent.CONTINUATION_INDICATOR_BYTES)
            Base.write(io, bytes)
            return take!(io)
        end
    end
    io = IOBuffer()
    _write_framed_message(io, bytes, UInt8[], alignment)
    return take!(io)
end

schemaipc(result::Protocol.SchemaResult; alignment::Integer=DEFAULT_IPC_ALIGNMENT) =
    _normalize_schemaipc(result.schema; alignment=alignment)

schemaipc(info::Protocol.FlightInfo; alignment::Integer=DEFAULT_IPC_ALIGNMENT) =
    _normalize_schemaipc(info.schema; alignment=alignment)

schemaipc(schema::AbstractVector{UInt8}; alignment::Integer=DEFAULT_IPC_ALIGNMENT) =
    _normalize_schemaipc(schema; alignment=alignment)

function schemaipc(message::Protocol.FlightData; alignment::Integer=DEFAULT_IPC_ALIGNMENT)
    isempty(message.data_header) &&
        throw(ArgumentError("FlightData message is missing the Arrow IPC header"))
    io = IOBuffer()
    _write_framed_message(io, message.data_header, message.data_body, alignment)
    return take!(io)
end

function schemaipc(source; kwargs...)
    alignment = get(kwargs, :alignment, DEFAULT_IPC_ALIGNMENT)
    messages = flightdata(source; kwargs...)
    isempty(messages) &&
        throw(ArgumentError("cannot derive schema bytes from an empty Flight source"))
    return schemaipc(first(messages); alignment=alignment)
end
