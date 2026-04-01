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

function grpcserver_extension_fixture(protocol)
    descriptor_type = protocol.var"FlightDescriptor.DescriptorType"
    descriptor =
        protocol.FlightDescriptor(descriptor_type.PATH, UInt8[], ["native", "dataset"])
    ticket = protocol.Ticket(b"native-ticket")
    dataset_metadata = Dict("dataset" => "native")
    dataset_colmetadata = Dict(:name => Dict("lang" => "en"))
    messages = Arrow.Flight.flightdata(
        Tables.partitioner((
            (id=Int64[1, 2], name=["one", "two"]),
            (id=Int64[3], name=["three"]),
        ));
        descriptor=descriptor,
        metadata=dataset_metadata,
        colmetadata=dataset_colmetadata,
    )
    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    info = protocol.FlightInfo(
        schema_bytes[5:end],
        descriptor,
        [protocol.FlightEndpoint(ticket, protocol.Location[], nothing, UInt8[])],
        Int64(3),
        Int64(-1),
        false,
        UInt8[],
    )
    handshake_requests = [protocol.HandshakeRequest(UInt64(0), b"native-token")]
    exchange_metadata = Dict("dataset" => "exchange")
    exchange_colmetadata = Dict(:name => Dict("lang" => "exchange"))
    exchange_messages = Arrow.Flight.flightdata(
        Tables.partitioner(((id=Int64[10], name=["ten"]),));
        descriptor=descriptor,
        metadata=exchange_metadata,
        colmetadata=exchange_colmetadata,
    )
    return (
        descriptor=descriptor,
        ticket=ticket,
        messages=messages,
        schema_bytes=schema_bytes,
        info=info,
        handshake_requests=handshake_requests,
        dataset_metadata=dataset_metadata,
        dataset_colmetadata=dataset_colmetadata,
        exchange_messages=exchange_messages,
        exchange_metadata=exchange_metadata,
        exchange_colmetadata=exchange_colmetadata,
    )
end
