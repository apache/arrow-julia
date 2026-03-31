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

function pyarrow_interop_descriptors(protocol)
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    return (
        download=protocol.FlightDescriptor(
            descriptor_type.PATH,
            UInt8[],
            ["interop", "download"],
        ),
        upload=protocol.FlightDescriptor(
            descriptor_type.PATH,
            UInt8[],
            ["interop", "upload"],
        ),
        exchange=protocol.FlightDescriptor(
            descriptor_type.PATH,
            UInt8[],
            ["interop", "exchange"],
        ),
    )
end

function pyarrow_interop_collect(req, channel)
    messages = collect(channel)
    gRPCClient.grpc_async_await(req)
    return messages
end

function pyarrow_interop_send_messages(req, request, response, messages)
    for message in messages
        put!(request, message)
    end
    close(request)
    responses = collect(response)
    gRPCClient.grpc_async_await(req)
    return responses
end
