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

include("pyarrow_interop/support.jl")
include("pyarrow_interop/discovery_tests.jl")
include("pyarrow_interop/download_tests.jl")
include("pyarrow_interop/upload_tests.jl")
include("pyarrow_interop/exchange_tests.jl")

@testset "Flight pyarrow interop" begin
    server = FlightTestSupport.start_pyarrow_flight_server()
    if isnothing(server)
        @test true
    else
        protocol = Arrow.Flight.Protocol
        descriptors = pyarrow_interop_descriptors(protocol)

        try
            FlightTestSupport.with_test_grpc_handle() do grpc
                client = Arrow.Flight.Client("grpc://127.0.0.1:$(server.port)"; grpc=grpc)
                pyarrow_interop_test_discovery(client, protocol, descriptors.download)
                pyarrow_interop_test_download(client, descriptors.download)
                pyarrow_interop_test_upload(client, descriptors.upload)
                pyarrow_interop_test_exchange(client, descriptors.exchange)
            end
        finally
            FlightTestSupport.stop_pyarrow_flight_server(server)
        end
    end
end
