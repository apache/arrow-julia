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

using Tables
using UUIDs

@testset "Flight IPC conversion helpers" begin
    missing_schema_fragment = "the server may have terminated the stream before emitting the first schema-bearing FlightData message"
    descriptor = Arrow.Flight.Protocol.FlightDescriptor(
        Arrow.Flight.Protocol.var"FlightDescriptor.DescriptorType".PATH,
        UInt8[],
        ["datasets", "roundtrip"],
    )
    source = Tables.partitioner((
        (id=Int64[1, 2], label=["one", "two"]),
        (id=Int64[3], label=["three"]),
    ))
    messages = Arrow.Flight.flightdata(source; descriptor=descriptor)

    @test !isempty(messages)
    @test messages[1].flight_descriptor == descriptor
    @test all(isnothing(msg.flight_descriptor) for msg in messages[2:end])
    @test !isempty(messages[1].data_header)
    @test isempty(messages[1].data_body)

    bytes = Arrow.Flight.streambytes(messages)
    @test Arrow.readbuffer(bytes, 1, UInt32) == Arrow.CONTINUATION_INDICATOR_BYTES
    @test Arrow.readbuffer(bytes, length(bytes) - 3, Int32) == 0

    batches = collect(Arrow.Flight.stream(messages))
    @test length(batches) == 2
    @test batches[1].id == [1, 2]
    @test batches[2].label == ["three"]

    tbl = Arrow.Flight.table(messages)
    @test tbl.id == [1, 2, 3]
    @test tbl.label == ["one", "two", "three"]

    schema_bytes = Arrow.Flight.schemaipc(first(messages))
    @test Arrow.Flight.schemaipc(Arrow.Flight.Protocol.SchemaResult(schema_bytes[5:end])) ==
          schema_bytes

    stream_error = try
        Arrow.Flight.stream(Arrow.Flight.Protocol.FlightData[])
        nothing
    catch err
        err
    end
    @test stream_error isa ArgumentError
    @test occursin(missing_schema_fragment, sprint(showerror, stream_error))

    table_error = try
        Arrow.Flight.table(Arrow.Flight.Protocol.FlightData[])
        nothing
    catch err
        err
    end
    @test table_error isa ArgumentError
    @test occursin(missing_schema_fragment, sprint(showerror, table_error))

    empty_tbl = Arrow.Flight.table(
        Arrow.Flight.Protocol.FlightData[];
        schema=Arrow.Flight.Protocol.SchemaResult(schema_bytes[5:end]),
    )
    @test isempty(empty_tbl.id)
    @test isempty(empty_tbl.label)

    extension_source = (
        uuid=[UUID(UInt128(1)), UUID(UInt128(2))],
        flag=[Arrow.Bool8(true), Arrow.Bool8(false)],
        json=Union{Missing,Arrow.JSONText{String}}[Arrow.JSONText("{\"a\":1}"), missing],
        ts=Union{Missing,Arrow.TimestampWithOffset{Arrow.Meta.TimeUnit.MILLISECOND}}[
            Arrow.TimestampWithOffset(
                Arrow.Timestamp{Arrow.Meta.TimeUnit.MILLISECOND,:UTC}(123),
                Int16(-480),
            ),
            missing,
        ],
    )
    extension_messages = Arrow.Flight.flightdata(extension_source)
    extension_batches = collect(Arrow.Flight.stream(extension_messages))
    extension_tbl = Arrow.Flight.table(extension_messages)

    @test Arrow.getmetadata(extension_batches[1].uuid)[Arrow.EXTENSION_NAME_KEY] ==
          "arrow.uuid"
    @test Arrow.getmetadata(extension_batches[1].flag)[Arrow.EXTENSION_NAME_KEY] ==
          "arrow.bool8"
    @test Arrow.getmetadata(extension_batches[1].json)[Arrow.EXTENSION_NAME_KEY] ==
          "arrow.json"
    @test Arrow.getmetadata(extension_batches[1].ts)[Arrow.EXTENSION_NAME_KEY] ==
          "arrow.timestamp_with_offset"
    @test Arrow.getmetadata(extension_tbl.uuid)[Arrow.EXTENSION_NAME_KEY] == "arrow.uuid"
    @test Arrow.getmetadata(extension_tbl.flag)[Arrow.EXTENSION_NAME_KEY] == "arrow.bool8"
    @test Arrow.getmetadata(extension_tbl.json)[Arrow.EXTENSION_NAME_KEY] == "arrow.json"
    @test Arrow.getmetadata(extension_tbl.ts)[Arrow.EXTENSION_NAME_KEY] ==
          "arrow.timestamp_with_offset"
    @test copy(extension_tbl.uuid) == extension_source.uuid
    @test Bool.(copy(extension_tbl.flag)) == Bool.(extension_source.flag)
    @test isequal(copy(extension_tbl.json), extension_source.json)
    @test isequal(copy(extension_tbl.ts), extension_source.ts)
end
