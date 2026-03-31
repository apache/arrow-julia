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

include("grpcserver_extension/support.jl")
include("grpcserver_extension/descriptor_tests.jl")
include("grpcserver_extension/unary_tests.jl")
include("grpcserver_extension/streaming_tests.jl")

@testset "Flight gRPCServer extension" begin
    grpcserver = FlightTestSupport.load_grpcserver()
    if isnothing(grpcserver)
        @test true
    else
        protocol = Arrow.Flight.Protocol
        fixture = grpcserver_extension_fixture(protocol)
        service = grpcserver_extension_service(protocol, fixture)
        metadata = grpcserver_extension_metadata()

        grpcserver_extension_test_descriptor(grpcserver, service)
        grpcserver_extension_test_unary(grpcserver, service, fixture, metadata)
        grpcserver_extension_test_streaming(grpcserver, service, fixture, metadata)
    end
end
