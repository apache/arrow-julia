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

function grpcserver_capture_server_stream(grpcserver, ::Type{T}) where {T}
    messages = T[]
    closed = Ref(false)
    stream = grpcserver.ServerStream{T}(
        (message, compress) -> begin
            @test compress
            push!(messages, message)
        end,
        () -> (closed[] = true),
    )
    return messages, closed, stream
end

function grpcserver_capture_bidi_stream(
    grpcserver,
    ::Type{Request},
    ::Type{Response},
    requests,
) where {Request,Response}
    messages = Response[]
    closed = Ref(false)
    stream = grpcserver.BidiStream{Request,Response}(
        FlightTestSupport.next_message_factory(requests),
        (message, compress) -> begin
            @test compress
            push!(messages, message)
        end,
        () -> (closed[] = true),
        () -> false,
    )
    return messages, closed, stream
end
