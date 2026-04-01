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

function _message_body(msg::ArrowParent.Message, alignment::Integer)
    msg.columns === nothing && return UInt8[]
    io = IOBuffer()
    for col in Tables.Columns(msg.columns)
        ArrowParent.writebuffer(io, col, alignment)
    end
    return take!(io)
end

function _flightdata_message(
    msg::ArrowParent.Message;
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    app_metadata::AbstractVector{UInt8}=UInt8[],
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
)
    body = _message_body(msg, alignment)
    length(body) == msg.bodylen ||
        throw(ArgumentError("FlightData body length mismatch while encoding Arrow IPC"))
    return Protocol.FlightData(
        descriptor,
        Vector{UInt8}(msg.msgflatbuf),
        Vector{UInt8}(app_metadata),
        body,
    )
end

function _write_framed_message(
    io::IO,
    data_header::AbstractVector{UInt8},
    data_body::AbstractVector{UInt8},
    alignment::Integer,
)
    metalen = ArrowParent.padding(length(data_header), alignment)
    Base.write(io, ArrowParent.CONTINUATION_INDICATOR_BYTES)
    Base.write(io, Int32(metalen))
    Base.write(io, data_header)
    ArrowParent.writezeros(io, ArrowParent.paddinglength(length(data_header), alignment))
    Base.write(io, data_body)
    return
end

function _write_end_marker(io::IO)
    Base.write(io, ArrowParent.CONTINUATION_INDICATOR_BYTES)
    Base.write(io, Int32(0))
    return
end
