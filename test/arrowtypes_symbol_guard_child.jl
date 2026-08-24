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

# Run these checks in a fresh Julia process. Symbol interning is permanent for
# the process, so an in-process test could pass or fail because of an earlier
# test's global state instead of the Arrow read under test.

using Arrow
using ArrowTypes
using Logging
using Test

const AC = Arrow.ArrowCore
const REGISTERED_STRUCT_NAME = Symbol("JuliaLang.ArrowTests.SymbolGuardStruct")

struct SymbolGuardStruct end

ArrowTypes.JuliaType(::Val{REGISTERED_STRUCT_NAME}, storage, metadata) = SymbolGuardStruct

const UNIQUE_SUFFIX = string(getpid(), "_", time_ns())

freshname(prefix::AbstractString) = string(prefix, "_", UNIQUE_SUFFIX)

function isinterned(name::String)
    occursin('\0', name) && error("the Symbol guard only accepts NUL-free names")
    pointer = GC.@preserve name begin
        ccall(:jl_symbol_lookup, Ptr{Cvoid}, (Cstring,), name)
    end
    return pointer != C_NULL
end

function ipcone(field::AC.Field, data::AC.ArrayData; file::Bool)
    schema = AC.Schema([field])
    batch = AC.RecordBatch(schema, [data], data.len)
    return file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
end

extensionmetadata(name::AbstractString) = Pair{String,String}[
    "ARROW:extension:name" => String(name),
    "ARROW:extension:metadata" => "",
]

@testset "ArrowTypes untrusted Symbol guard" begin
    unknown = freshname("JuliaLang.ArrowTests.UnknownSymbolGuard")
    @test !isinterned(unknown)
    _, unknowndata = AC.fromjulia("value", Int64[7])
    unknownfield = AC.Field(
        "value",
        AC.IntType(64, true);
        nullable=false,
        metadata=extensionmetadata(unknown),
    )
    for file in (false, true)
        table = @test_logs (:warn, r"unsupported ARROW:extension:name") Arrow.Table(
            ipcone(unknownfield, unknowndata; file),
        )
        @test table.value == Int64[7]
        @test !isinterned(unknown)
    end

    childname = freshname("untrusted_struct_child")
    @test !isinterned(childname)
    childfield, childdata = AC.fromjulia(childname, Int64[8])
    structtype = AC.StructType()
    structfield = AC.Field(
        "value",
        structtype;
        nullable=false,
        metadata=extensionmetadata(String(REGISTERED_STRUCT_NAME)),
        children=[childfield],
    )
    structdata =
        AC.ArrayData(structtype, 1, [AC.BufferSlice()]; children=[childdata], nullcount=0)
    for file in (false, true)
        table = @test_logs (:warn, r"unsupported ARROW:extension:name") Arrow.Table(
            ipcone(structfield, structdata; file),
        )
        @test table.value == [Pair{String,Any}[childname => Int64(8)]]
        @test !isinterned(childname)
    end

    payload = freshname("untrusted_symbol_payload")
    @test !isinterned(payload)
    payloadfield, payloaddata = AC.fromjulia("value", String[payload])
    symbolfield = AC.Field(
        payloadfield.name,
        payloadfield.type;
        nullable=false,
        metadata=extensionmetadata("JuliaLang.Symbol"),
    )
    for file in (false, true)
        err = try
            Arrow.Table(ipcone(symbolfield, payloaddata; file))
            nothing
        catch exception
            exception
        end
        @test err isa Arrow.ValidationError
        @test occursin("is not already interned", sprint(showerror, err))
        @test !isinterned(payload)
    end

    streamname = freshname("untrusted_stream_field")
    @test !isinterned(streamname)
    streamfield, streamdata = AC.fromjulia(streamname, Int64[9])
    streambytes = ipcone(streamfield, streamdata; file=false)
    Arrow.readstream(streambytes)
    @test !isinterned(streamname)
    Arrow.Table(streambytes)
    @test isinterned(streamname)

    filename = freshname("untrusted_file_field")
    @test !isinterned(filename)
    filefield, filedata = AC.fromjulia(filename, Int64[10])
    filebytes = ipcone(filefield, filedata; file=true)
    Arrow.readfile(filebytes)
    @test !isinterned(filename)
    Arrow.Table(filebytes)
    @test isinterned(filename)

    # Four ASCII letters provide enough distinct four-byte names to reach the
    # count gate while staying below the independent 1 MiB byte budget. Skip
    # the small set that Julia or a loaded dependency already interned.
    countnames = String[]
    candidate = 0
    while length(countnames) <= Arrow._MAX_TABLES_NEW_FIELD_NAMES
        value = candidate
        bytes = Vector{UInt8}(undef, 4)
        for index = 1:4
            bytes[index] = UInt8('a') + UInt8(value % 26)
            value ÷= 26
        end
        name = String(bytes)
        isinterned(name) || push!(countnames, name)
        candidate += 1
    end
    @test all(!isinterned, countnames)
    countfields =
        AC.Field[AC.Field(name, AC.NullType(); nullable=true) for name in countnames]
    counterr = try
        Arrow._fieldnamesymbols(countfields)
        nothing
    catch exception
        exception
    end
    @test counterr isa Arrow.ValidationError
    @test occursin("novel-name limit", sprint(showerror, counterr))
    @test all(!isinterned, countnames)

    bytenames = String[]
    for index =
            1:(Arrow._MAX_TABLES_NEW_FIELD_NAME_BYTES ÷ Arrow._MAX_ARROWTYPE_SCHEMA_NAME_BYTES + 1)

        base = freshname("untrusted_byte_field_$index")
        push!(
            bytenames,
            base * repeat("x", Arrow._MAX_ARROWTYPE_SCHEMA_NAME_BYTES - ncodeunits(base)),
        )
    end
    @test all(!isinterned, bytenames)
    bytefields =
        AC.Field[AC.Field(name, AC.NullType(); nullable=true) for name in bytenames]
    byteerr = try
        Arrow._fieldnamesymbols(bytefields)
        nothing
    catch exception
        exception
    end
    @test byteerr isa Arrow.ValidationError
    @test occursin("novel-name byte budget", sprint(showerror, byteerr))
    @test all(!isinterned, bytenames)

    warningnames = [freshname("unknown_warning_$index") for index = 1:20]
    @test all(!isinterned, warningnames)
    warningfields = AC.Field[]
    warningcolumns = AC.ArrayData[]
    for (index, name) in enumerate(warningnames)
        _, data = AC.fromjulia("value", Int64[index])
        push!(
            warningfields,
            AC.Field(
                "value",
                AC.IntType(64, true);
                nullable=false,
                metadata=extensionmetadata(name),
            ),
        )
        push!(warningcolumns, data)
    end
    warningschema = AC.Schema(warningfields)
    warningbatch = AC.RecordBatch(warningschema, warningcolumns, 1)
    warningbytes = Arrow.writestream(warningschema, [warningbatch])
    warningio = IOBuffer()
    logger = SimpleLogger(warningio, Logging.Warn)
    with_logger(logger) do
        Arrow.Table(warningbytes)
    end
    warningtext = String(take!(warningio))
    @test length(findall("unsupported ARROW:extension:name type", warningtext)) == 16
    @test length(
        findall(
            "additional unsupported Arrow extension warnings were suppressed",
            warningtext,
        ),
    ) == 1
    @test all(!isinterned, warningnames)
end

println("ArrowTypes symbol guard passed")
