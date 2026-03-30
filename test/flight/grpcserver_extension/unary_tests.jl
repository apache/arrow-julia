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

function grpcserver_extension_test_unary(grpcserver, service, fixture, metadata)
    grpc_descriptor = grpcserver.service_descriptor(service)

    unary_context = grpcserver_extension_context(
        grpcserver,
        "/arrow.flight.protocol.FlightService/GetFlightInfo";
        metadata=metadata,
    )
    schema_context = grpcserver_extension_context(
        grpcserver,
        "/arrow.flight.protocol.FlightService/GetSchema";
        metadata=metadata,
    )

    direct_info =
        grpc_descriptor.methods["GetFlightInfo"].handler(unary_context, fixture.descriptor)
    @test direct_info.total_records == 3
    @test direct_info.endpoint[1].ticket.ticket == fixture.ticket.ticket

    direct_schema =
        grpc_descriptor.methods["GetSchema"].handler(schema_context, fixture.descriptor)
    @test Arrow.Flight.schemaipc(direct_schema) == fixture.schema_bytes
end
