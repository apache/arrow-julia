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

function grpcserver_extension_test_descriptor(grpcserver, service)
    grpc_descriptor = grpcserver.service_descriptor(service)
    @test Base.get_extension(Arrow, :ArrowgRPCServerExt) !== nothing
    @test grpc_descriptor.name == "arrow.flight.protocol.FlightService"
    @test haskey(grpc_descriptor.methods, "GetFlightInfo")
    @test haskey(grpc_descriptor.methods, "DoGet")
    @test haskey(grpc_descriptor.methods, "DoExchange")
    @test grpc_descriptor.methods["GetFlightInfo"].method_type ==
          grpcserver.MethodType.UNARY
    @test grpc_descriptor.methods["DoGet"].method_type ==
          grpcserver.MethodType.SERVER_STREAMING
    @test grpc_descriptor.methods["DoExchange"].method_type ==
          grpcserver.MethodType.BIDI_STREAMING
    @test grpc_descriptor.methods["DoGet"].input_type == "arrow.flight.protocol.Ticket"
    @test grpc_descriptor.methods["DoGet"].output_type == "arrow.flight.protocol.FlightData"
    return grpc_descriptor
end
