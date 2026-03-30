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

module FlightTestSupport

using gRPCClient

export PyArrowFlightServer,
    flight_test_roots,
    pyarrow_flight_python,
    start_pyarrow_flight_server,
    start_headers_flight_server,
    start_handshake_flight_server,
    start_poll_flight_server,
    start_tls_flight_server,
    stop_pyarrow_flight_server,
    with_test_grpc_handle,
    load_grpcserver,
    generate_test_tls_certificate,
    next_message_factory

include("support/types.jl")
include("support/paths.jl")
include("support/python_servers.jl")
include("support/grpc.jl")
include("support/tls.jl")
include("support/streams.jl")

end
