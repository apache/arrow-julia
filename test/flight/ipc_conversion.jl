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

using DataAPI
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

    metadata_source = Tables.partitioner(((title=["red", "blue"],), (title=["green"],)))
    metadata_messages = Arrow.Flight.flightdata(
        metadata_source;
        metadata=Dict("dataset" => "flight"),
        colmetadata=Dict(:title => Dict("lang" => "en")),
    )
    metadata_schema_bytes = Arrow.Flight.schemaipc(first(metadata_messages))
    metadata_info = Arrow.Flight.Protocol.FlightInfo(
        metadata_schema_bytes[5:end],
        nothing,
        Arrow.Flight.Protocol.FlightEndpoint[],
        Int64(-1),
        Int64(-1),
        false,
        UInt8[],
    )
    metadata_batches =
        collect(Arrow.Flight.stream(metadata_messages[2:end]; schema=metadata_info))
    metadata_table = Arrow.Flight.table(metadata_messages[2:end]; schema=metadata_info)

    @test length(metadata_batches) == 2
    @test DataAPI.metadata(metadata_batches[1], "dataset") == "flight"
    @test DataAPI.colmetadata(metadata_batches[1], :title, "lang") == "en"
    @test DataAPI.metadata(metadata_batches[2], "dataset") == "flight"
    @test DataAPI.colmetadata(metadata_batches[2], :title, "lang") == "en"
    @test metadata_table.title == ["red", "blue", "green"]
    @test DataAPI.metadata(metadata_table, "dataset") == "flight"
    @test DataAPI.colmetadata(metadata_table, :title, "lang") == "en"
    metadata_parts = collect(Tables.partitions(metadata_table))
    @test length(metadata_parts) == 2
    @test metadata_parts[1].title == ["red", "blue"]
    @test metadata_parts[2].title == ["green"]
    @test DataAPI.metadata(metadata_parts[1], "dataset") == "flight"
    @test DataAPI.colmetadata(metadata_parts[1], :title, "lang") == "en"
    @test DataAPI.metadata(metadata_parts[2], "dataset") == "flight"
    @test DataAPI.colmetadata(metadata_parts[2], :title, "lang") == "en"

    app_metadata_messages = [
        index == 1 ? message :
        Arrow.Flight.Protocol.FlightData(
            message.flight_descriptor,
            message.data_header,
            Vector{UInt8}(codeunits("batch:$(index - 2)")),
            message.data_body,
        ) for (index, message) in enumerate(metadata_messages)
    ]
    metadata_batches_with_app =
        collect(Arrow.Flight.stream(app_metadata_messages; include_app_metadata=true))
    metadata_table_with_app =
        Arrow.Flight.table(app_metadata_messages; include_app_metadata=true)
    @test length(metadata_batches_with_app) == 2
    @test metadata_batches_with_app[1].table.title == ["red", "blue"]
    @test metadata_batches_with_app[2].table.title == ["green"]
    @test String(metadata_batches_with_app[1].app_metadata) == "batch:0"
    @test String(metadata_batches_with_app[2].app_metadata) == "batch:1"
    @test metadata_table_with_app.table.title == ["red", "blue", "green"]
    @test String.(metadata_table_with_app.app_metadata) == ["batch:0", "batch:1"]

    reemitted_channel = Channel{Arrow.Flight.Protocol.FlightData}(8)
    reemit_task =
        @async Arrow.Flight.putflightdata!(reemitted_channel, metadata_table; close=true)
    reemitted_messages = collect(reemitted_channel)
    wait(reemit_task)
    reemitted_table = Arrow.Flight.table(reemitted_messages)
    @test reemitted_table.title == metadata_table.title
    @test DataAPI.metadata(reemitted_table, "dataset") == "flight"
    @test DataAPI.colmetadata(reemitted_table, :title, "lang") == "en"

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
