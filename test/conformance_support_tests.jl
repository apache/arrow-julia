# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module ConformanceSupportTests

using Test

# All three adapters must share one support-module identity when a caller
# composes them in one process.
module CombinedAdapters
const ROOT = normpath(joinpath(@__DIR__, ".."))
include(joinpath(ROOT, "conformance", "corpus.jl"))
const FIRST_VERDICT = Verdict
include(joinpath(ROOT, "conformance", "oracle.jl"))
include(joinpath(ROOT, "conformance", "cdata_oracle.jl"))
end

module NarrowCDataStress
include(joinpath(@__DIR__, "support", "CDataStressSupport.jl"))
end

# A composed include must ignore a child marker that belongs to another entry
# point. The test environment intentionally does not depend on PythonCall.
module PoisonedCDataInclude
const ROOT = normpath(joinpath(@__DIR__, ".."))
const SAVED_ARGS = copy(ARGS)
empty!(ARGS)
push!(ARGS, "--child")
try
    include(joinpath(ROOT, "conformance", "cdata_oracle.jl"))
finally
    empty!(ARGS)
    append!(ARGS, SAVED_ARGS)
end
end

const CS = CombinedAdapters.ConformanceSupport

@test !isdefined(PoisonedCDataInclude, :PythonCall)

function _field(
    name,
    type;
    nullable=true,
    children=Any[],
    metadata=Any[],
    dictionary=nothing,
)
    field = Dict{String,Any}(
        "name" => name,
        "nullable" => nullable,
        "type" => type,
        "children" => children,
        "metadata" => metadata,
    )
    dictionary === nothing || (field["dictionary"] = dictionary)
    return field
end

_onecolumndoc(field, column) = Dict{String,Any}(
    "schema" => Dict{String,Any}("fields" => Any[field]),
    "batches" =>
        Any[Dict{String,Any}("count" => column["count"], "columns" => Any[column])],
)

_emptydoc(field) = Dict{String,Any}(
    "schema" => Dict{String,Any}("fields" => Any[field]),
    "batches" => Any[],
)

function _coreview(data, offset, len)
    AC = CS.ArrowCore
    return AC.ArrayData(
        data.type,
        len,
        collect(AC.BufferSlice, data.buffers);
        offset=data.offset + offset,
        children=collect(AC.ArrayData, data.children),
        dictionary=data.dictionary,
    )
end

function _denseunionpair(left, right)
    AC = CS.ArrowCore
    leftfield, leftdata = AC.fromjulia("left", left)
    rightfield, rightdata = AC.fromjulia("right", right)
    uniontype = AC.UnionType(AC.DenseMode, Int8[0, 1])
    unionfield =
        AC.Field("union", uniontype; nullable=false, children=[leftfield, rightfield])
    function uniondata(typeid)
        return AC.ArrayData(
            uniontype,
            1,
            [AC._databuffer(Int8[typeid]), AC._databuffer(Int32[0])];
            children=[leftdata, rightdata],
            nullcount=0,
        )
    end
    return unionfield, uniondata(0), uniondata(1)
end

function _normalizationdoc(
    dictid;
    poolname,
    poolvalue,
    entryname,
    keyname,
    valuename,
    metadata,
    decimalwidth,
    nullvalue,
)
    dictionary = Dict{String,Any}(
        "id" => dictid,
        "indexType" =>
            Dict{String,Any}("name" => "int", "bitWidth" => 32, "isSigned" => true),
        "isOrdered" => false,
    )
    dictfield = _field(
        "d",
        Dict{String,Any}("name" => "floatingpoint", "precision" => "SINGLE");
        dictionary,
        metadata,
    )
    entryfield = _field(
        entryname,
        Dict{String,Any}("name" => "struct");
        nullable=false,
        children=Any[
            _field(
                keyname,
                Dict{String,Any}("name" => "int", "bitWidth" => 64, "isSigned" => false),
                nullable=false,
            ),
            _field(valuename, Dict{String,Any}("name" => "utf8")),
        ],
    )
    mapfield = _field(
        "m",
        Dict{String,Any}("name" => "map", "keysSorted" => false);
        children=Any[entryfield],
    )
    decimaltype = Dict{String,Any}("name" => "decimal", "precision" => 10, "scale" => 2)
    decimalwidth === nothing || (decimaltype["bitWidth"] = decimalwidth)
    fields = Any[
        dictfield,
        mapfield,
        _field("x", decimaltype),
        _field(
            "nulls",
            Dict{String,Any}("name" => "int", "bitWidth" => 64, "isSigned" => true),
        ),
    ]
    mapcolumn = Dict{String,Any}(
        "name" => "m",
        "count" => 1,
        "VALIDITY" => [1],
        "OFFSET" => [0, 1],
        "children" => Any[Dict{String,Any}(
            "name" => entryname,
            "count" => 1,
            "children" => Any[
                Dict{String,Any}("name" => keyname, "count" => 1, "DATA" => [1]),
                Dict{String,Any}(
                    "name" => valuename,
                    "count" => 1,
                    "OFFSET" => [0, 1],
                    "DATA" => ["v"],
                ),
            ],
        )],
    )
    return Dict{String,Any}(
        "schema" => Dict{String,Any}("fields" => fields, "metadata" => metadata),
        "dictionaries" => Any[Dict{String,Any}(
            "id" => dictid,
            "data" => Dict{String,Any}(
                "count" => 1,
                "columns" => Any[Dict{String,Any}(
                    "name" => poolname,
                    "count" => 1,
                    "VALIDITY" => [1],
                    "DATA" => Any[poolvalue],
                )],
            ),
        )],
        "batches" => Any[Dict{String,Any}(
            "count" => 1,
            "columns" => Any[
                Dict{String,Any}(
                    "name" => "d",
                    "count" => 1,
                    "VALIDITY" => [1],
                    "DATA" => [0],
                ),
                mapcolumn,
                Dict{String,Any}("name" => "x", "count" => 1, "DATA" => ["1"]),
                Dict{String,Any}(
                    "name" => "nulls",
                    "count" => 1,
                    "VALIDITY" => [0],
                    "DATA" => [nullvalue],
                ),
            ],
        )],
    )
end

@testset "acceptance and conformance support contracts" begin
    @test CombinedAdapters.Verdict === CS.Verdict === CombinedAdapters.FIRST_VERDICT
    @test isdefined(CombinedAdapters, :runcorpus)
    @test isdefined(CombinedAdapters, :runoracle)
    @test isdefined(CombinedAdapters, :_oracle_parent)
    @test CombinedAdapters.COMPRESSED_ORACLE_FAMILIES ==
          ("generated_primitive", "generated_nested_dictionary", "generated_binary_view")
    @test CombinedAdapters.STATISTICS_ORACLE_FAMILY == "generated_primitive"
    targeted = [
        CombinedAdapters.OracleCase("v__$(family)+$(variant)", "", "") for
        (family, variant) in CombinedAdapters.TARGETED_ORACLE_VARIANTS
    ]
    @test CombinedAdapters._requiretargetedoracles(targeted) === nothing
    @test_throws ErrorException CombinedAdapters._requiretargetedoracles(targeted[2:end])
    @test isdefined(NarrowCDataStress.CDataStressSupport, :run)
    @test !isdefined(NarrowCDataStress.CDataStressSupport, :AcceptanceSupport)
    @test !isdefined(NarrowCDataStress.CDataStressSupport, :PooledArrays)
    @test !isdefined(NarrowCDataStress.CDataStressSupport, :ipc_read_battery)
    @test !isdefined(NarrowCDataStress.CDataStressSupport, :scan_battery)
    @test isempty(CS.familyskipreason("generated_extension"))
    @test isempty(CS.familyskipreason("generated_decimal256"))
    @test !isempty(CS.familyskipreason("generated_primitive", "0.17.1"))
    @test isempty(CS.familyskipreason("generated_primitive", "0.14.1"))
    @test isempty(CS.familyskipreason("generated_primitive", "1.0.0-bigendian"))
    @test !isempty(CS.goldipcskipreason("generated_primitive", "0.14.1"))
    @test !isempty(CS.goldipcskipreason("generated_primitive", "1.0.0-bigendian"))
    @test isempty(CS.goldipcskipreason("generated_primitive", "0.17.1"))

    @testset "empty conformance inputs fail closed" begin
        mktempdir() do corpus
            root = joinpath(corpus, "data", "arrow-ipc-stream", "integration")
            mkpath(root)
            @test_throws ErrorException CombinedAdapters.runcorpus(corpus)
            @test_throws ErrorException CombinedAdapters.preparecases(
                corpus,
                joinpath(corpus, "oracle-work"),
            )

            skipped = joinpath(root, "0.17.1")
            mkpath(skipped)
            write(joinpath(skipped, "only_skipped.json.gz"), UInt8[])
            @test_throws ErrorException CombinedAdapters.runcorpus(corpus)
        end
    end

    @testset "route-specific legacy gold skips" begin
        AC = CS.ArrowCore
        AJ = CS.ArrowJSON
        field, data = AC.fromjulia("x", Union{Missing,Int64}[1, missing, 3])
        schema = AC.Schema([field])
        batches = [AC.RecordBatch(schema, [data], 3)]
        document = AJ.tojson(schema, batches)

        mktempdir() do corpus
            root = joinpath(corpus, "data", "arrow-ipc-stream", "integration")
            for (version, family) in (
                ("0.14.1", "legacy_case"),
                ("1.0.0-bigendian", "bigendian_case"),
                ("0.17.1", "v4_case"),
            )
                dir = joinpath(root, version)
                mkpath(dir)
                jsonbytes = Vector{UInt8}(codeunits(CS.JSON.json(document)))
                write(
                    joinpath(dir, family * ".json.gz"),
                    transcode(CS.CodecZlib.GzipCompressor, jsonbytes),
                )
                # These sentinels would fail if a legacy gold route ran.
                write(joinpath(dir, family * ".stream"), UInt8[0])
                write(joinpath(dir, family * ".arrow_file"), UInt8[0])
            end

            for (version, family) in
                (("0.14.1", "legacy_case"), ("1.0.0-bigendian", "bigendian_case"))
                verdicts = CS.Verdict[]
                CombinedAdapters.runfamily(joinpath(root, version), family, verdicts)
                @test length(verdicts) == 5
                @test count(v -> v.status == :pass, verdicts) == 3
                @test count(v -> v.status == :skip, verdicts) == 2
                @test count(v -> v.status == :fail, verdicts) == 0
                @test Set(v.check for v in verdicts if v.status == :skip) ==
                      Set(("gold stream→json", "gold file→json"))
            end

            workdir = joinpath(corpus, "oracle-work")
            cases, skips = CombinedAdapters.preparecases(corpus, workdir)
            @test Set(case.label for case in cases) ==
                  Set(("0.14.1/legacy_case", "1.0.0-bigendian/bigendian_case"))
            @test skips == [("0.17.1/v4_case", CS.FAMILY_SKIP["0.17.1"])]
            @test length(readdir(joinpath(workdir, "cases"))) == 4
        end
    end

    @testset "decimal256 integration JSON" begin
        AJ = CS.ArrowJSON
        AC = CS.ArrowCore
        minimum = -(BigInt(1) << 255)
        maximum = (BigInt(1) << 255) - 1
        values = string.([minimum, -BigInt(1), BigInt(0), BigInt(1), maximum])

        @test AJ._decimal256bytes("0") == zeros(UInt8, 32)
        @test AJ._decimal256bytes("1") == [UInt8(1); zeros(UInt8, 31)]
        @test AJ._decimal256bytes("-1") == fill(typemax(UInt8), 32)
        @test AJ._decimal256bytes(string(minimum)) == [zeros(UInt8, 31); UInt8(0x80)]
        @test AJ._decimal256bytes(string(maximum)) ==
              [fill(typemax(UInt8), 31); UInt8(0x7f)]
        for value in values
            @test string(AJ._decimal256value(AJ._decimal256bytes(value))) == value
        end
        @test_throws ArgumentError AJ._decimal256bytes(string(minimum - 1))
        @test_throws ArgumentError AJ._decimal256bytes(string(maximum + 1))
        @test_throws ArgumentError AJ._decimal256value(zeros(UInt8, 31))

        field = AC.Field("d", AC.DecimalType(76, 0, 256); nullable=true)
        column = Dict{String,Any}(
            "name" => "d",
            "count" => length(values),
            "VALIDITY" => [1, 1, 0, 1, 1],
            "DATA" => values,
        )
        data = AJ.fromjsoncolumn(
            field,
            column,
            Dict{Int64,AC.ArrayData}(),
            IdDict{AC.Field,Int64}(),
        )
        @test AJ.tojsoncolumn(field, data) == column
    end

    expected_checks = (
        ("ours→pyarrow stream", "pyarrow_stream", ".pyarrow.stream"),
        ("ours→pyarrow file", "pyarrow_file", ".pyarrow.arrow"),
        ("ours→nanoarrow stream", "nanoarrow_stream", ".nanoarrow.stream"),
    )
    @test map(check -> check[1:3], CombinedAdapters.ORACLE_CHECKS) == expected_checks
    @test !occursin("skip: ", CombinedAdapters.PYDRIVER)

    goldstats = Dict{String,Any}(
        "schema" => Dict{String,Any}("fields" => Any[]),
        "batches" => Any[Dict{String,Any}("count" => 1)],
    )
    inputstats = deepcopy(goldstats)
    inputstats["schema"]["metadata"] = Any[Dict{String,Any}(
        "key" => CombinedAdapters.Arrow.STATS_KEY,
        "value" => "opaque",
    ),]
    @test CombinedAdapters._withoutstatsmetadata(inputstats) == goldstats
    @test haskey(inputstats["schema"], "metadata")
    corruptstats = deepcopy(inputstats)
    corruptstats["batches"][1]["count"] = 2
    @test CombinedAdapters._withoutstatsmetadata(corruptstats) != goldstats
    @test CS.documentcheck("stats", "ours stats file→gold", goldstats) do
        CombinedAdapters._withoutstatsmetadata(corruptstats)
    end.status == :fail

    mktempdir() do dir
        goldpath = joinpath(dir, "gold.json")
        write(goldpath, "{}")
        case = CombinedAdapters.OracleCase("required", "required", goldpath)
        statuses = Dict{String,Any}(
            "pyarrow_stream" => "required oracle unavailable",
            "pyarrow_file" => "required oracle unavailable",
            "nanoarrow_stream" => "required oracle unavailable",
        )
        verdicts = CombinedAdapters.compareresults(
            [case],
            Tuple{String,String}[],
            Dict{String,Any}("cases" => Dict{String,Any}("required" => statuses)),
            dir,
        )
        @test length(verdicts) == length(expected_checks)
        @test all(verdict -> verdict.status == :fail, verdicts)
    end

    @test_throws ArgumentError CombinedAdapters.oraclereport(
        CS.Verdict[],
        Dict{String,Any}("pyarrow" => "p", "nanoarrow" => "n");
        io=IOBuffer(),
    )

    unicode = repeat("é", 201)
    verdict = CS.documentcheck("family", "check", Dict{String,Any}()) do
        throw(ErrorException(unicode))
    end
    @test verdict.status == :fail
    @test verdict.detail == repeat("é", 200)
    @test length(verdict.detail) == 200
    @test CS.errorverdict("family", "check", ErrorException(repeat("x", 201))).detail ==
          repeat("x", 200)
    @test_throws InterruptException CS.documentcheck(
        () -> throw(InterruptException()),
        "family",
        "check",
        Dict{String,Any}(),
    )
    @test_throws OutOfMemoryError CS.documentcheck(
        () -> throw(OutOfMemoryError()),
        "family",
        "check",
        Dict{String,Any}(),
    )

    metadata_a = Any[
        Dict{String,Any}("key" => "z", "value" => "1"),
        Dict{String,Any}("key" => "a", "value" => "2"),
    ]
    metadata_b = reverse(metadata_a)
    actual = _normalizationdoc(
        41;
        poolname="field-name",
        poolvalue=0.1,
        entryname="some_entries",
        keyname="some_key",
        valuename="some_value",
        metadata=metadata_a,
        decimalwidth=128,
        nullvalue=111,
    )
    expected = _normalizationdoc(
        7;
        poolname="DICT0",
        poolvalue=Float64(Float32(0.1)),
        entryname="entries",
        keyname="key",
        valuename="value",
        metadata=metadata_b,
        decimalwidth=nothing,
        nullvalue=-999,
    )
    actual_before = deepcopy(actual)
    expected_before = deepcopy(expected)
    @test isempty(CS.documentdiffs(actual, expected))
    @test actual == actual_before
    @test expected == expected_before
    changedpool = deepcopy(expected)
    changedpool["dictionaries"][1]["data"]["columns"][1]["DATA"][1] = 0.2
    @test !isempty(CS.documentdiffs(actual, changedpool))

    # Materialization, not generic string parsing, owns representation
    # normalization. Text stays exact; Int64 JSON number/string spellings and
    # decimal string spellings become the same declared storage value.
    AC = CS.ArrowCore
    AJ = CS.ArrowJSON
    textfield, textdata = AC.fromjulia("text", ["01"])
    textschema = AC.Schema([textfield])
    textdoc = AJ.tojson(textschema, [AC.RecordBatch(textschema, [textdata], 1)])
    changedtext = deepcopy(textdoc)
    changedtext["batches"][1]["columns"][1]["DATA"][1] = "1"
    changedtext["batches"][1]["columns"][1]["OFFSET"] = [0, 1]
    @test !isempty(CS.documentdiffs(textdoc, changedtext))

    intfield, intdata = AC.fromjulia("value", Int64[1])
    intschema = AC.Schema([intfield])
    intdoc = AJ.tojson(intschema, [AC.RecordBatch(intschema, [intdata], 1)])
    intnumber = deepcopy(intdoc)
    intnumber["batches"][1]["columns"][1]["DATA"] = Any[1]
    @test isempty(CS.documentdiffs(intdoc, intnumber))

    decimalspelling = deepcopy(expected)
    decimalspelling["batches"][1]["columns"][3]["DATA"][1] = "01"
    @test isempty(CS.documentdiffs(expected, decimalspelling))

    # Integration JSON makes these false-valued schema properties required.
    # A missing property must not compare equal to its explicit false value.
    required_false_paths = (
        ("field nullability", (doc) -> delete!(doc["schema"]["fields"][1], "nullable")),
        (
            "integer signedness",
            (doc) -> delete!(
                doc["schema"]["fields"][2]["children"][1]["children"][1]["type"],
                "isSigned",
            ),
        ),
        (
            "map key ordering",
            (doc) -> delete!(doc["schema"]["fields"][2]["type"], "keysSorted"),
        ),
        (
            "dictionary ordering",
            (doc) -> delete!(doc["schema"]["fields"][1]["dictionary"], "isOrdered"),
        ),
    )
    for (label, remove!) in required_false_paths
        incomplete = deepcopy(expected)
        remove!(incomplete)
        @testset "$label is required" begin
            @test !isempty(CS.documentdiffs(incomplete, expected))
            @test !isempty(CS.documentdiffs(expected, incomplete))
        end
    end

    @testset "integration JSON structure is fail-closed" begin
        structural_mutations = (
            (
                "extra root column",
                doc -> push!(
                    doc["batches"][1]["columns"],
                    deepcopy(doc["batches"][1]["columns"][end]),
                ),
            ),
            ("missing root column", doc -> pop!(doc["batches"][1]["columns"])),
            ("reordered root columns", doc -> reverse!(doc["batches"][1]["columns"])),
            (
                "wrong root column name",
                doc -> begin
                    doc["batches"][1]["columns"][1]["name"] = "wrong"
                end,
            ),
            (
                "extra Struct child",
                doc -> push!(
                    doc["batches"][1]["columns"][2]["children"][1]["children"],
                    deepcopy(
                        doc["batches"][1]["columns"][2]["children"][1]["children"][end],
                    ),
                ),
            ),
            (
                "missing Struct child",
                doc -> pop!(doc["batches"][1]["columns"][2]["children"][1]["children"]),
            ),
            (
                "reordered Struct children",
                doc -> reverse!(doc["batches"][1]["columns"][2]["children"][1]["children"]),
            ),
            (
                "wrong Struct child name",
                doc -> begin
                    doc["batches"][1]["columns"][2]["children"][1]["children"][1]["name"] = "wrong"
                end,
            ),
            (
                "dictionary batch count mismatch",
                doc -> begin
                    doc["dictionaries"][1]["data"]["count"] += 1
                end,
            ),
        )
        for (label, mutate!) in structural_mutations
            malformed = deepcopy(expected)
            mutate!(malformed)
            @testset "$label" begin
                @test !isempty(CS.documentdiffs(malformed, expected))
                @test !isempty(CS.documentdiffs(expected, malformed))
                @test !isempty(CS.documentdiffs(malformed, malformed))
            end
        end
    end

    @testset "integration JSON array lengths are exact" begin
        inttype = Dict{String,Any}("name" => "int", "bitWidth" => 32, "isSigned" => true)
        unionfield = _field(
            "u",
            Dict{String,Any}("name" => "union", "mode" => "SPARSE", "typeIds" => [0, 1]);
            children=Any[_field("a", inttype), _field("b", inttype)],
        )
        uniondoc = _onecolumndoc(
            unionfield,
            Dict{String,Any}(
                "name" => "u",
                "count" => 1,
                "TYPE_ID" => [0],
                "children" => Any[
                    Dict{String,Any}(
                        "name" => "a",
                        "count" => 1,
                        "VALIDITY" => [1],
                        "DATA" => [10],
                    ),
                    Dict{String,Any}(
                        "name" => "b",
                        "count" => 1,
                        "VALIDITY" => [1],
                        "DATA" => [20],
                    ),
                ],
            ),
        )
        array_mutations = (
            (
                "extra DATA",
                expected,
                doc -> push!(doc["batches"][1]["columns"][4]["DATA"], 0),
            ),
            (
                "truncated DATA",
                expected,
                doc -> pop!(doc["batches"][1]["columns"][4]["DATA"]),
            ),
            (
                "extra VALIDITY",
                expected,
                doc -> push!(doc["batches"][1]["columns"][4]["VALIDITY"], 1),
            ),
            (
                "truncated VALIDITY",
                expected,
                doc -> pop!(doc["batches"][1]["columns"][4]["VALIDITY"]),
            ),
            (
                "extra OFFSET",
                expected,
                doc -> push!(doc["batches"][1]["columns"][2]["OFFSET"], 1),
            ),
            (
                "truncated OFFSET",
                expected,
                doc -> pop!(doc["batches"][1]["columns"][2]["OFFSET"]),
            ),
            (
                "extra child DATA",
                expected,
                doc -> push!(
                    doc["batches"][1]["columns"][2]["children"][1]["children"][1]["DATA"],
                    0,
                ),
            ),
            (
                "truncated child DATA",
                expected,
                doc -> pop!(
                    doc["batches"][1]["columns"][2]["children"][1]["children"][1]["DATA"],
                ),
            ),
            (
                "extra TYPE_ID",
                uniondoc,
                doc -> push!(doc["batches"][1]["columns"][1]["TYPE_ID"], 0),
            ),
            (
                "truncated TYPE_ID",
                uniondoc,
                doc -> pop!(doc["batches"][1]["columns"][1]["TYPE_ID"]),
            ),
        )
        for (label, valid, mutate!) in array_mutations
            malformed = deepcopy(valid)
            mutate!(malformed)
            @testset "$label" begin
                @test !isempty(CS.documentdiffs(malformed, valid))
                @test !isempty(CS.documentdiffs(valid, malformed))
                @test !isempty(CS.documentdiffs(malformed, malformed))
            end
        end
    end

    @testset "schema child cardinality is fail-closed" begin
        child = _field(
            "child",
            Dict{String,Any}("name" => "int", "bitWidth" => 32, "isSigned" => true),
        )
        no_child_types = Any[
            Dict{String,Any}("name" => "null"),
            Dict{String,Any}("name" => "bool"),
            Dict{String,Any}("name" => "int", "bitWidth" => 32, "isSigned" => true),
            Dict{String,Any}("name" => "floatingpoint", "precision" => "DOUBLE"),
            Dict{String,Any}("name" => "utf8"),
            Dict{String,Any}("name" => "largeutf8"),
            Dict{String,Any}("name" => "binary"),
            Dict{String,Any}("name" => "largebinary"),
            Dict{String,Any}("name" => "utf8view"),
            Dict{String,Any}("name" => "binaryview"),
            Dict{String,Any}("name" => "fixedsizebinary", "byteWidth" => 4),
            Dict{String,Any}("name" => "decimal", "precision" => 10, "scale" => 2),
            Dict{String,Any}("name" => "date", "unit" => "DAY"),
            Dict{String,Any}("name" => "time", "unit" => "SECOND", "bitWidth" => 32),
            Dict{String,Any}("name" => "timestamp", "unit" => "SECOND"),
            Dict{String,Any}("name" => "duration", "unit" => "SECOND"),
            Dict{String,Any}("name" => "interval", "unit" => "YEAR_MONTH"),
        ]
        for type in no_child_types
            malformed = _emptydoc(_field("x", type; children=Any[child]))
            @testset "$(type["name"]) rejects children" begin
                @test !isempty(CS.documentdiffs(malformed, malformed))
            end
        end

        one_child_types = Any[
            Dict{String,Any}("name" => "list"),
            Dict{String,Any}("name" => "largelist"),
            Dict{String,Any}("name" => "listview"),
            Dict{String,Any}("name" => "largelistview"),
            Dict{String,Any}("name" => "fixedsizelist", "listSize" => 2),
            Dict{String,Any}("name" => "map", "keysSorted" => false),
        ]
        for type in one_child_types
            missing = _emptydoc(_field("x", type))
            extra = _emptydoc(_field("x", type; children=Any[child, deepcopy(child)]))
            @testset "$(type["name"]) requires one child" begin
                @test !isempty(CS.documentdiffs(missing, missing))
                @test !isempty(CS.documentdiffs(extra, extra))
            end
        end

        union = _emptydoc(
            _field(
                "x",
                Dict{String,Any}(
                    "name" => "union",
                    "mode" => "SPARSE",
                    "typeIds" => [0, 1],
                );
                children=Any[child],
            ),
        )
        encoded = _emptydoc(
            _field("x", Dict{String,Any}("name" => "runendencoded"); children=Any[child]),
        )
        @test !isempty(CS.documentdiffs(union, union))
        @test !isempty(CS.documentdiffs(encoded, encoded))

        emptystruct = _emptydoc(_field("x", Dict{String,Any}("name" => "struct")))
        @test isempty(CS.documentdiffs(emptystruct, emptystruct))
    end

    @testset "schema JSON scalar types are strict" begin
        scalar_mutations = (
            (
                "field nullability Bool as integer",
                doc -> (doc["schema"]["fields"][2]["children"][1]["nullable"] = 0),
            ),
            (
                "signedness Bool as integer",
                doc -> (
                    doc["schema"]["fields"][2]["children"][1]["children"][1]["type"]["isSigned"] =
                        0
                ),
            ),
            (
                "integer bit width as float",
                doc -> (
                    doc["schema"]["fields"][2]["children"][1]["children"][1]["type"]["bitWidth"] =
                        64.0
                ),
            ),
            (
                "integer bit width as string",
                doc -> (
                    doc["schema"]["fields"][2]["children"][1]["children"][1]["type"]["bitWidth"] = "64"
                ),
            ),
        )
        for (label, mutate!) in scalar_mutations
            malformed = deepcopy(expected)
            mutate!(malformed)
            @testset "$label" begin
                @test !isempty(CS.documentdiffs(malformed, expected))
                @test !isempty(CS.documentdiffs(expected, malformed))
                @test !isempty(CS.documentdiffs(malformed, malformed))
            end
        end
    end

    # Metadata is optional. Missing, null, and empty metadata all mean no
    # key/value metadata, but this equivalence is specific to `metadata`.
    no_metadata = deepcopy(expected)
    empty_metadata = deepcopy(expected)
    null_metadata = deepcopy(expected)
    delete!(no_metadata["schema"], "metadata")
    empty_metadata["schema"]["metadata"] = Any[]
    null_metadata["schema"]["metadata"] = nothing
    @test isempty(CS.documentdiffs(no_metadata, empty_metadata))
    @test isempty(CS.documentdiffs(no_metadata, null_metadata))

    @testset "nested nulls compare by logical value" begin
        inttype = Dict{String,Any}("name" => "int", "bitWidth" => 32, "isSigned" => true)

        structfield = _field(
            "parent",
            Dict{String,Any}("name" => "struct");
            children=Any[_field("item", inttype)],
        )
        structactual = _onecolumndoc(
            structfield,
            Dict{String,Any}(
                "name" => "parent",
                "count" => 3,
                "VALIDITY" => [1, 0, 1],
                "children" => Any[Dict{String,Any}(
                    "name" => "item",
                    "count" => 3,
                    "VALIDITY" => [1, 1, 1],
                    "DATA" => [10, 111, 30],
                )],
            ),
        )
        structexpected = deepcopy(structactual)
        structexpected["batches"][1]["columns"][1]["children"][1]["DATA"][2] = -111
        structbad = deepcopy(structexpected)
        structbad["batches"][1]["columns"][1]["children"][1]["DATA"][3] = 999

        fixedfield = _field(
            "parent",
            Dict{String,Any}("name" => "fixedsizelist", "listSize" => 2);
            children=Any[_field("item", inttype)],
        )
        fixedactual = _onecolumndoc(
            fixedfield,
            Dict{String,Any}(
                "name" => "parent",
                "count" => 3,
                "VALIDITY" => [1, 0, 1],
                "children" => Any[Dict{String,Any}(
                    "name" => "item",
                    "count" => 6,
                    "VALIDITY" => fill(1, 6),
                    "DATA" => [10, 11, 111, 112, 30, 31],
                )],
            ),
        )
        fixedexpected = deepcopy(fixedactual)
        fixedexpected["batches"][1]["columns"][1]["children"][1]["DATA"][3:4] = [-111, -112]
        fixedbad = deepcopy(fixedexpected)
        fixedbad["batches"][1]["columns"][1]["children"][1]["DATA"][6] = 999

        listfield = _field(
            "parent",
            Dict{String,Any}("name" => "list");
            children=Any[_field("item", inttype)],
        )
        listactual = _onecolumndoc(
            listfield,
            Dict{String,Any}(
                "name" => "parent",
                "count" => 3,
                "VALIDITY" => [1, 0, 1],
                "OFFSET" => [0, 1, 3, 4],
                "children" => Any[Dict{String,Any}(
                    "name" => "item",
                    "count" => 4,
                    "VALIDITY" => fill(1, 4),
                    "DATA" => [10, 111, 112, 30],
                )],
            ),
        )
        listexpected = _onecolumndoc(
            listfield,
            Dict{String,Any}(
                "name" => "parent",
                "count" => 3,
                "VALIDITY" => [1, 0, 1],
                "OFFSET" => [0, 1, 1, 2],
                "children" => Any[Dict{String,Any}(
                    "name" => "item",
                    "count" => 2,
                    "VALIDITY" => fill(1, 2),
                    "DATA" => [10, 30],
                )],
            ),
        )
        listbad = deepcopy(listexpected)
        listbad["batches"][1]["columns"][1]["children"][1]["DATA"][2] = 999

        entriesfield = _field(
            "entries",
            Dict{String,Any}("name" => "struct");
            nullable=false,
            children=Any[_field("key", inttype; nullable=false), _field("value", inttype)],
        )
        mapfield = _field(
            "parent",
            Dict{String,Any}("name" => "map", "keysSorted" => false);
            children=Any[entriesfield],
        )
        mapactual = _onecolumndoc(
            mapfield,
            Dict{String,Any}(
                "name" => "parent",
                "count" => 3,
                "VALIDITY" => [1, 0, 1],
                "OFFSET" => [0, 1, 3, 4],
                "children" => Any[Dict{String,Any}(
                    "name" => "entries",
                    "count" => 4,
                    "VALIDITY" => fill(1, 4),
                    "children" => Any[
                        Dict{String,Any}(
                            "name" => "key",
                            "count" => 4,
                            "VALIDITY" => fill(1, 4),
                            "DATA" => [1, 111, 112, 3],
                        ),
                        Dict{String,Any}(
                            "name" => "value",
                            "count" => 4,
                            "VALIDITY" => fill(1, 4),
                            "DATA" => [10, 111, 112, 30],
                        ),
                    ],
                )],
            ),
        )
        mapexpected = _onecolumndoc(
            mapfield,
            Dict{String,Any}(
                "name" => "parent",
                "count" => 3,
                "VALIDITY" => [1, 0, 1],
                "OFFSET" => [0, 1, 1, 2],
                "children" => Any[Dict{String,Any}(
                    "name" => "entries",
                    "count" => 2,
                    "VALIDITY" => fill(1, 2),
                    "children" => Any[
                        Dict{String,Any}(
                            "name" => "key",
                            "count" => 2,
                            "VALIDITY" => fill(1, 2),
                            "DATA" => [1, 3],
                        ),
                        Dict{String,Any}(
                            "name" => "value",
                            "count" => 2,
                            "VALIDITY" => fill(1, 2),
                            "DATA" => [10, 30],
                        ),
                    ],
                )],
            ),
        )
        mapbad = deepcopy(mapexpected)
        mapbad["batches"][1]["columns"][1]["children"][1]["children"][2]["DATA"][2] = 999

        for (label, actualdoc, expecteddoc, baddoc) in (
            ("struct", structactual, structexpected, structbad),
            ("fixed-size list", fixedactual, fixedexpected, fixedbad),
            ("list with a non-empty null segment", listactual, listexpected, listbad),
            ("map with a non-empty null segment", mapactual, mapexpected, mapbad),
        )
            @testset "$label" begin
                actualbefore = deepcopy(actualdoc)
                expectedbefore = deepcopy(expecteddoc)
                @test isempty(CS.documentdiffs(actualdoc, expecteddoc))
                @test !isempty(CS.documentdiffs(actualdoc, baddoc))
                @test actualdoc == actualbefore
                @test expecteddoc == expectedbefore
            end
        end
    end

    @testset "nonzero-offset Core batch windows" begin
        AC = CS.ArrowCore
        stringfield, stringdata = AC.fromjulia("text", ["zero", "one", "two", "three"])

        binarytype = AC.BinaryType(true)
        binaryfield = AC.Field("large_binary", binarytype; nullable=false)
        binaryvalues = [UInt8[0x00], UInt8[0x01, 0x02], UInt8[], UInt8[0x03]]
        binarydata = AC.ArrayData(
            binarytype,
            4,
            [
                AC.BufferSlice(),
                AC._databuffer(Int64[0, 1, 3, 3, 4]),
                AC._databuffer(vcat(binaryvalues...)),
            ];
            nullcount=0,
        )

        listfield, listdata =
            AC.fromjulia("list", [Int32[0], Int32[1, 2], Int32[3], Int32[4, 5]])
        structfield, structdata = AC.fromjulia_struct(
            "struct",
            (number=Int32[0, 1, 2, 3], text=["a", "b", "c", "d"]),
        )
        schema = AC.Schema([stringfield, binaryfield, listfield, structfield])
        source = AC.RecordBatch(schema, [stringdata, binarydata, listdata, structdata], 4)
        sliced = AC.RecordBatch(
            schema,
            [
                _coreview(stringdata, 1, 2),
                _coreview(binarydata, 1, 2),
                _coreview(listdata, 1, 2),
                _coreview(structdata, 1, 2),
            ],
            2,
        )

        @test isempty(CS.corebatchdiffs(sliced, source; expectedoffset=1, rows=2))
        @test !isempty(CS.corebatchdiffs(sliced, source; expectedoffset=0, rows=2))
        @test_throws BoundsError CS.corebatchdiffs(sliced, source; expectedoffset=3, rows=2)
    end

    @testset "Union comparison preserves selected type ids" begin
        AC = CS.ArrowCore
        AJ = CS.ArrowJSON

        function rejectdifferentroute(field, expecteddata, actualdata)
            schema = AC.Schema([field])
            expected = AC.RecordBatch(schema, [expecteddata], 1)
            actual = AC.RecordBatch(schema, [actualdata], 1)
            @test !isempty(CS.corebatchdiffs(actual, expected))
            @test !isempty(
                CS.documentdiffs(
                    AJ.tojson(schema, [actual]),
                    AJ.tojson(schema, [expected]),
                ),
            )
            return nothing
        end

        for (label, left, right) in (
            ("Int64 versus Utf8", Int64[1], String["1"]),
            ("Bool versus Int64", Bool[true], Int64[1]),
        )
            @testset "$label" begin
                field, expecteddata, actualdata = _denseunionpair(left, right)
                rejectdifferentroute(field, expecteddata, actualdata)
            end
        end

        # The selected Arrow child is semantic even when both children expose
        # the same Julia value and type. Nest the Union so a scalar-type check
        # or a top-level-only route check cannot satisfy this regression.
        unionfield, unionexpected, unionactual = _denseunionpair(Int64[1], Int64[1])
        structtype = AC.StructType()
        structfield = AC.Field("parent", structtype; nullable=true, children=[unionfield])
        visibleexpected = AC.ArrayData(
            structtype,
            1,
            [AC.BufferSlice()];
            children=[unionexpected],
            nullcount=0,
        )
        visibleactual = AC.ArrayData(
            structtype,
            1,
            [AC.BufferSlice()];
            children=[unionactual],
            nullcount=0,
        )
        @test AC.materialize(structfield, visibleactual) ==
              AC.materialize(structfield, visibleexpected)
        rejectdifferentroute(structfield, visibleexpected, visibleactual)

        # Map values and dictionary pools are two places where a generic
        # materializer can erase a nested Union route before comparison.
        keyfield, keydata = AC.fromjulia("key", Int32[7])
        entriesfield =
            AC.Field("entries", structtype; nullable=false, children=[keyfield, unionfield])
        expectedentries = AC.ArrayData(
            structtype,
            1,
            [AC.BufferSlice()];
            children=[keydata, unionexpected],
            nullcount=0,
        )
        actualentries = AC.ArrayData(
            structtype,
            1,
            [AC.BufferSlice()];
            children=[keydata, unionactual],
            nullcount=0,
        )
        maptype = AC.MapType(false)
        mapfield = AC.Field("map", maptype; nullable=false, children=[entriesfield])
        expectedmap = AC.ArrayData(
            maptype,
            1,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 1])];
            children=[expectedentries],
            nullcount=0,
        )
        actualmap = AC.ArrayData(
            maptype,
            1,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 1])];
            children=[actualentries],
            nullcount=0,
        )
        rejectdifferentroute(mapfield, expectedmap, actualmap)

        dictionarytype = AC.DictionaryType(AC.IntType(8, false), unionfield.type, false)
        dictionaryfield = AC.Field(
            "dictionary",
            dictionarytype;
            nullable=false,
            children=unionfield.children,
        )
        expecteddictionary = AC.ArrayData(
            dictionarytype,
            1,
            [AC.BufferSlice(), AC._databuffer(UInt8[0])];
            dictionary=unionexpected,
            nullcount=0,
        )
        actualdictionary = AC.ArrayData(
            dictionarytype,
            1,
            [AC.BufferSlice(), AC._databuffer(UInt8[0])];
            dictionary=unionactual,
            nullcount=0,
        )
        rejectdifferentroute(dictionaryfield, expecteddictionary, actualdictionary)

        # Physical child slots below a null parent remain unspecified. A route
        # tag must therefore follow the same masking rule as its logical value.
        hiddenvalidity = AC._databuffer(UInt8[0x00])
        hiddenexpected = AC.ArrayData(
            structtype,
            1,
            [hiddenvalidity];
            children=[unionexpected],
            nullcount=1,
        )
        hiddenactual = AC.ArrayData(
            structtype,
            1,
            [hiddenvalidity];
            children=[unionactual],
            nullcount=1,
        )
        hiddenschema = AC.Schema([structfield])
        expectedbatch = AC.RecordBatch(hiddenschema, [hiddenexpected], 1)
        actualbatch = AC.RecordBatch(hiddenschema, [hiddenactual], 1)
        @test isempty(CS.corebatchdiffs(actualbatch, expectedbatch))
        @test isempty(
            CS.documentdiffs(
                AJ.tojson(hiddenschema, [actualbatch]),
                AJ.tojson(hiddenschema, [expectedbatch]),
            ),
        )
    end
end

end # module ConformanceSupportTests
