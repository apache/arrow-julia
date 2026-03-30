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

function _method_type(method::Flight.MethodDescriptor)
    if method.request_streaming
        return method.response_streaming ? gRPCServer.MethodType.BIDI_STREAMING :
               gRPCServer.MethodType.CLIENT_STREAMING
    end
    return method.response_streaming ? gRPCServer.MethodType.SERVER_STREAMING :
           gRPCServer.MethodType.UNARY
end

function _call_context(context::gRPCServer.ServerContext)
    headers = Flight.HeaderPair[
        String(name) => (value isa String ? value : Vector{UInt8}(value)) for
        (name, value) in pairs(context.metadata)
    ]
    peer = string(context.peer.address, ":", context.peer.port)
    return Flight.ServerCallContext(
        headers=headers,
        peer=peer,
        secure=(context.peer.certificate !== nothing),
    )
end

function _proto_type_name(T::Type)
    type_name = string(T)
    if startswith(type_name, GENERATED_TYPE_PREFIX)
        return type_name[(ncodeunits(GENERATED_TYPE_PREFIX) + 1):end]
    end
    return type_name
end
