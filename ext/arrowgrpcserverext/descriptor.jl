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

function _register_proto_types!(method::Flight.MethodDescriptor)
    registry = gRPCServer.get_type_registry()
    registry[_proto_type_name(method.request_type)] = method.request_type
    registry[_proto_type_name(method.response_type)] = method.response_type
    return nothing
end

function gRPCServer.service_descriptor(service::Flight.Service)
    descriptor = Flight.servicedescriptor(service)
    methods = Dict{String,gRPCServer.MethodDescriptor}()
    for method in descriptor.methods
        _register_proto_types!(method)
        methods[method.name] = gRPCServer.MethodDescriptor(
            method.name,
            _method_type(method),
            _proto_type_name(method.request_type),
            _proto_type_name(method.response_type),
            _handler(service, method),
        )
    end
    return gRPCServer.ServiceDescriptor(descriptor.name, methods, nothing)
end
