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

using ArrowTypes
using DataAPI
using Dates
using Tables
using UUIDs

struct ArrowTypesTestID
    value::Int64
end
Base.:(==)(a::ArrowTypesTestID, b::ArrowTypesTestID) = a.value == b.value
Base.isequal(a::ArrowTypesTestID, b::ArrowTypesTestID) = isequal(a.value, b.value)

const ARROWTYPES_TEST_ID_NAME = Symbol("JuliaLang.ArrowTests.ID")
const ARROWTYPES_TEST_ID_JULIATYPE_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestID}) = Int64
ArrowTypes.toarrow(x::ArrowTypesTestID) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestID}) = ARROWTYPES_TEST_ID_NAME
function ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ID_NAME}, S, metadata)
    ARROWTYPES_TEST_ID_JULIATYPE_CALLS[] += 1
    return ArrowTypesTestID
end
ArrowTypes.fromarrow(::Type{ArrowTypesTestID}, x::Int64) = ArrowTypesTestID(x)

struct ArrowTypesTestParityID
    value::Int64
end
Base.:(==)(a::ArrowTypesTestParityID, b::ArrowTypesTestParityID) =
    iseven(a.value) == iseven(b.value)
Base.isequal(a::ArrowTypesTestParityID, b::ArrowTypesTestParityID) = a == b
Base.hash(x::ArrowTypesTestParityID, h::UInt) = hash(iseven(x.value), h)

const ARROWTYPES_TEST_PARITY_ID_NAME = Symbol("JuliaLang.ArrowTests.ParityID")
ArrowTypes.ArrowType(::Type{ArrowTypesTestParityID}) = Int64
ArrowTypes.toarrow(x::ArrowTypesTestParityID) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestParityID}) = ARROWTYPES_TEST_PARITY_ID_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_PARITY_ID_NAME}, S, metadata) =
    ArrowTypesTestParityID
ArrowTypes.fromarrow(::Type{ArrowTypesTestParityID}, x::Int64) = ArrowTypesTestParityID(x)

struct ArrowTypesTestAlternateID
    value::Int64
end
Base.:(==)(a::ArrowTypesTestAlternateID, b::ArrowTypesTestAlternateID) = a.value == b.value
Base.isequal(a::ArrowTypesTestAlternateID, b::ArrowTypesTestAlternateID) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_ALTERNATE_ID_NAME = Symbol("JuliaLang.ArrowTests.AlternateID")
ArrowTypes.ArrowType(::Type{ArrowTypesTestAlternateID}) = Int64
ArrowTypes.toarrow(x::ArrowTypesTestAlternateID) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestAlternateID}) = ARROWTYPES_TEST_ALTERNATE_ID_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ALTERNATE_ID_NAME}, S, metadata) =
    ArrowTypesTestAlternateID
ArrowTypes.fromarrow(::Type{ArrowTypesTestAlternateID}, x::Int64) =
    ArrowTypesTestAlternateID(x)

struct ArrowTypesTestLabel
    value::String
end
Base.:(==)(a::ArrowTypesTestLabel, b::ArrowTypesTestLabel) = a.value == b.value
Base.isequal(a::ArrowTypesTestLabel, b::ArrowTypesTestLabel) = isequal(a.value, b.value)

const ARROWTYPES_TEST_LABEL_NAME = Symbol("JuliaLang.ArrowTests.Label")
ArrowTypes.ArrowType(::Type{ArrowTypesTestLabel}) = String
ArrowTypes.toarrow(x::ArrowTypesTestLabel) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestLabel}) = ARROWTYPES_TEST_LABEL_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_LABEL_NAME}, S, metadata) = ArrowTypesTestLabel
ArrowTypes.fromarrow(::Type{ArrowTypesTestLabel}, x::AbstractString) =
    ArrowTypesTestLabel(String(x))

struct ArrowTypesTestPointerString <: AbstractString
    value::String
    ArrowTypesTestPointerString(value::String, ::Val{:decoded}) = new(value)
end

arrowtypes_test_pointer_string(value::String) =
    ArrowTypesTestPointerString(value, Val(:decoded))
Base.ncodeunits(x::ArrowTypesTestPointerString) = ncodeunits(x.value)
Base.codeunit(::Type{ArrowTypesTestPointerString}) = UInt8
Base.codeunit(x::ArrowTypesTestPointerString, i::Integer) = codeunit(x.value, i)
Base.isvalid(x::ArrowTypesTestPointerString, i::Integer) = isvalid(x.value, i)
Base.iterate(x::ArrowTypesTestPointerString) = iterate(x.value)
Base.iterate(x::ArrowTypesTestPointerString, state::Integer) = iterate(x.value, state)
Base.length(x::ArrowTypesTestPointerString) = length(x.value)
Base.getindex(x::ArrowTypesTestPointerString, i::Int) = x.value[i]
Base.:(==)(a::ArrowTypesTestPointerString, b::ArrowTypesTestPointerString) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestPointerString, b::ArrowTypesTestPointerString) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_POINTER_STRING_NAME = Symbol("JuliaLang.ArrowTests.PointerString")
const ARROWTYPES_TEST_POINTER_STRING_CALLS = Ref(0)
const ARROWTYPES_TEST_POINTER_STRING_PREFIX = "physical:"
ArrowTypes.ArrowType(::Type{ArrowTypesTestPointerString}) = String
ArrowTypes.toarrow(x::ArrowTypesTestPointerString) =
    ARROWTYPES_TEST_POINTER_STRING_PREFIX * x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestPointerString}) =
    ARROWTYPES_TEST_POINTER_STRING_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_POINTER_STRING_NAME}, S, metadata) =
    ArrowTypesTestPointerString
function ArrowTypes.fromarrow(
    ::Type{ArrowTypesTestPointerString},
    ptr::Ptr{UInt8},
    len::Int,
)
    ARROWTYPES_TEST_POINTER_STRING_CALLS[] += 1
    storage = unsafe_string(ptr, len)
    startswith(storage, ARROWTYPES_TEST_POINTER_STRING_PREFIX) ||
        throw(ArgumentError("PointerString storage does not have its physical prefix"))
    start = ncodeunits(ARROWTYPES_TEST_POINTER_STRING_PREFIX) + 1
    return ArrowTypesTestPointerString(String(SubString(storage, start)), Val(:decoded))
end

struct ArrowTypesTestDay
    value::Date
end
Base.:(==)(a::ArrowTypesTestDay, b::ArrowTypesTestDay) = a.value == b.value
Base.isequal(a::ArrowTypesTestDay, b::ArrowTypesTestDay) = isequal(a.value, b.value)

const ARROWTYPES_TEST_DAY_NAME = Symbol("JuliaLang.ArrowTests.Day")
ArrowTypes.ArrowType(::Type{ArrowTypesTestDay}) = Date
ArrowTypes.toarrow(x::ArrowTypesTestDay) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestDay}) = ARROWTYPES_TEST_DAY_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_DAY_NAME}, S, metadata) = ArrowTypesTestDay
ArrowTypes.fromarrow(::Type{ArrowTypesTestDay}, x::Date) = ArrowTypesTestDay(x)

struct ArrowTypesTestDated
    day::Date
    count::Int32
end
Base.:(==)(a::ArrowTypesTestDated, b::ArrowTypesTestDated) =
    a.day == b.day && a.count == b.count
Base.isequal(a::ArrowTypesTestDated, b::ArrowTypesTestDated) =
    isequal(a.day, b.day) && isequal(a.count, b.count)

const ARROWTYPES_TEST_DATED_NAME = Symbol("JuliaLang.ArrowTests.Dated")
const ArrowTypesTestDatedStorage = @NamedTuple{day::Date, count::Int32}
ArrowTypes.ArrowType(::Type{ArrowTypesTestDated}) = ArrowTypesTestDatedStorage
ArrowTypes.toarrow(x::ArrowTypesTestDated) = (day=x.day, count=x.count)
ArrowTypes.arrowname(::Type{ArrowTypesTestDated}) = ARROWTYPES_TEST_DATED_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_DATED_NAME}, S, metadata) = ArrowTypesTestDated
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestDated},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestDated(row.day, row.count)
end

struct ArrowTypesTestExactStruct
    value::Int32
end
Base.:(==)(a::ArrowTypesTestExactStruct, b::ArrowTypesTestExactStruct) = a.value == b.value
Base.isequal(a::ArrowTypesTestExactStruct, b::ArrowTypesTestExactStruct) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_EXACT_STRUCT_NAME = Symbol("JuliaLang.ArrowTests.ExactStruct")
const ArrowTypesTestExactStructStorage = @NamedTuple{value::Int32}
ArrowTypes.ArrowType(::Type{ArrowTypesTestExactStruct}) = ArrowTypesTestExactStructStorage
ArrowTypes.toarrow(x::ArrowTypesTestExactStruct) = (value=x.value,)
ArrowTypes.arrowname(::Type{ArrowTypesTestExactStruct}) = ARROWTYPES_TEST_EXACT_STRUCT_NAME
ArrowTypes.JuliaType(
    ::Val{ARROWTYPES_TEST_EXACT_STRUCT_NAME},
    ::Type{ArrowTypesTestExactStructStorage},
    metadata,
) = ArrowTypesTestExactStruct
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestExactStruct},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestExactStruct(row.value)
end

struct ArrowTypesTestNullLogical end
Base.:(==)(::ArrowTypesTestNullLogical, ::ArrowTypesTestNullLogical) = true
Base.isequal(::ArrowTypesTestNullLogical, ::ArrowTypesTestNullLogical) = true

const ARROWTYPES_TEST_NULL_LOGICAL_NAME = Symbol("JuliaLang.ArrowTests.NullLogical")
ArrowTypes.ArrowType(::Type{ArrowTypesTestNullLogical}) = Nothing
ArrowTypes.toarrow(::ArrowTypesTestNullLogical) = nothing
ArrowTypes.arrowname(::Type{ArrowTypesTestNullLogical}) = ARROWTYPES_TEST_NULL_LOGICAL_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NULL_LOGICAL_NAME}, ::Type{Missing}, metadata) =
    ArrowTypesTestNullLogical
ArrowTypes.fromarrow(::Type{ArrowTypesTestNullLogical}, ::Missing) =
    ArrowTypesTestNullLogical()

struct ArrowTypesTestAllNull
    none::Missing
end
Base.:(==)(::ArrowTypesTestAllNull, ::ArrowTypesTestAllNull) = true
Base.isequal(::ArrowTypesTestAllNull, ::ArrowTypesTestAllNull) = true

const ARROWTYPES_TEST_ALL_NULL_NAME = Symbol("JuliaLang.ArrowTests.AllNull")
const ArrowTypesTestAllNullStorage = @NamedTuple{none::Missing}
ArrowTypes.ArrowType(::Type{ArrowTypesTestAllNull}) = ArrowTypesTestAllNullStorage
ArrowTypes.toarrow(x::ArrowTypesTestAllNull) = (none=x.none,)
ArrowTypes.arrowname(::Type{ArrowTypesTestAllNull}) = ARROWTYPES_TEST_ALL_NULL_NAME
ArrowTypes.JuliaType(
    ::Val{ARROWTYPES_TEST_ALL_NULL_NAME},
    ::Type{ArrowTypesTestAllNullStorage},
    metadata,
) = ArrowTypesTestAllNull
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestAllNull},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestAllNull(row.none)
end

const ArrowTypesTestMember = @NamedTuple{id::ArrowTypesTestID, plain::Int32}

struct ArrowTypesTestEnvelope
    member::ArrowTypesTestMember
end
Base.:(==)(a::ArrowTypesTestEnvelope, b::ArrowTypesTestEnvelope) = a.member == b.member
Base.isequal(a::ArrowTypesTestEnvelope, b::ArrowTypesTestEnvelope) =
    isequal(a.member, b.member)

const ARROWTYPES_TEST_ENVELOPE_NAME = Symbol("JuliaLang.ArrowTests.Envelope")
const ArrowTypesTestEnvelopeStorage = @NamedTuple{member::ArrowTypesTestMember}
ArrowTypes.ArrowType(::Type{ArrowTypesTestEnvelope}) = ArrowTypesTestEnvelopeStorage
ArrowTypes.toarrow(x::ArrowTypesTestEnvelope) = (member=x.member,)
ArrowTypes.arrowname(::Type{ArrowTypesTestEnvelope}) = ARROWTYPES_TEST_ENVELOPE_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ENVELOPE_NAME}, S, metadata) =
    ArrowTypesTestEnvelope
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestEnvelope},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestEnvelope(row.member)
end

struct ArrowTypesTestGroup
    members::Vector{ArrowTypesTestMember}
end
Base.:(==)(a::ArrowTypesTestGroup, b::ArrowTypesTestGroup) = a.members == b.members
Base.isequal(a::ArrowTypesTestGroup, b::ArrowTypesTestGroup) = isequal(a.members, b.members)

const ARROWTYPES_TEST_GROUP_NAME = Symbol("JuliaLang.ArrowTests.Group")
ArrowTypes.ArrowType(::Type{ArrowTypesTestGroup}) = Vector{ArrowTypesTestMember}
ArrowTypes.toarrow(x::ArrowTypesTestGroup) = x.members
ArrowTypes.arrowname(::Type{ArrowTypesTestGroup}) = ARROWTYPES_TEST_GROUP_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_GROUP_NAME}, S, metadata) = ArrowTypesTestGroup
ArrowTypes.fromarrow(::Type{ArrowTypesTestGroup}, members::Vector{ArrowTypesTestMember}) =
    ArrowTypesTestGroup(members)

struct ArrowTypesTestStampedID
    id::ArrowTypesTestID
    day::Date
end
Base.:(==)(a::ArrowTypesTestStampedID, b::ArrowTypesTestStampedID) =
    a.id == b.id && a.day == b.day
Base.isequal(a::ArrowTypesTestStampedID, b::ArrowTypesTestStampedID) =
    isequal(a.id, b.id) && isequal(a.day, b.day)

const ARROWTYPES_TEST_STAMPED_ID_NAME = Symbol("JuliaLang.ArrowTests.StampedID")
const ArrowTypesTestStampedIDStorage = @NamedTuple{id::ArrowTypesTestID, day::Date}
ArrowTypes.ArrowType(::Type{ArrowTypesTestStampedID}) = ArrowTypesTestStampedIDStorage
ArrowTypes.toarrow(x::ArrowTypesTestStampedID) = (id=x.id, day=x.day)
ArrowTypes.arrowname(::Type{ArrowTypesTestStampedID}) = ARROWTYPES_TEST_STAMPED_ID_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_STAMPED_ID_NAME}, S, metadata) =
    ArrowTypesTestStampedID
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestStampedID},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestStampedID(row.id, row.day)
end

struct ArrowTypesTestPoint{Tag}
    x::Int32
    y::Int32
end
Base.:(==)(a::ArrowTypesTestPoint, b::ArrowTypesTestPoint) = a.x == b.x && a.y == b.y
Base.isequal(a::ArrowTypesTestPoint, b::ArrowTypesTestPoint) =
    isequal(a.x, b.x) && isequal(a.y, b.y)

const ARROWTYPES_TEST_POINT_NAME = Symbol("JuliaLang.ArrowTests.Point")
const ArrowTypesTestPointStorage = @NamedTuple{y::Int32, x::Int32}
ArrowTypes.ArrowType(::Type{<:ArrowTypesTestPoint}) = ArrowTypesTestPointStorage
ArrowTypes.toarrow(x::ArrowTypesTestPoint) = (y=x.y, x=x.x)
ArrowTypes.arrowname(::Type{<:ArrowTypesTestPoint}) = ARROWTYPES_TEST_POINT_NAME
ArrowTypes.arrowmetadata(::Type{ArrowTypesTestPoint{Tag}}) where {Tag} = String(Tag)
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_POINT_NAME}, S, metadata) =
    ArrowTypesTestPoint{Symbol(metadata)}
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestPoint{Tag}},
    ::Val{names},
    values...,
) where {Tag,names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestPoint{Tag}(row.x, row.y)
end

function arrowtypes_test_bytes(table; file::Bool, kwargs...)
    io = IOBuffer()
    Arrow.write(io, table; file=file, kwargs...)
    return take!(io)
end

arrowtypes_test_table(table; file::Bool, kwargs...) =
    Arrow.Table(arrowtypes_test_bytes(table; file=file, kwargs...))

function arrowtypes_test_rowdict(row)
    return Dict(String(first(pair)) => last(pair) for pair in row)
end
function arrowtypes_test_rowdict(row::NamedTuple)
    return Dict(String(name) => value for (name, value) in pairs(row))
end

const ARROWTYPES_TEST_AC = Arrow.ArrowCore

function arrowtypes_test_marked_id_field(name; nullable::Bool=false)
    return ARROWTYPES_TEST_AC.Field(
        name,
        ARROWTYPES_TEST_AC.IntType(64, true);
        nullable=nullable,
        metadata=[
            "ARROW:extension:name" => String(ARROWTYPES_TEST_ID_NAME),
            "ARROW:extension:metadata" => "",
        ],
    )
end

function arrowtypes_test_extension_name(field)
    field.metadata === nothing && return nothing
    for kv in field.metadata
        first(kv) == "ARROW:extension:name" && return last(kv)
    end
    return nothing
end

@testset "ArrowTypes compatibility" begin
    @testset "module boundary" begin
        @test Arrow.ArrowTypes === ArrowTypes
        @test !Base.isexported(Arrow, :ArrowTypes)
    end

    @testset "automatic scalar and struct mappings" begin
        ids = ArrowTypesTestID.(Int64[1, 2, 3])
        nullable_ids = Union{Missing,ArrowTypesTestID}[ids[1], missing, ids[3]]
        labels = ArrowTypesTestLabel.(["one", "two", "three"])
        days = ArrowTypesTestDay.([Date(2024, 1, 1), Date(2024, 2, 29), Date(2025, 1, 1)])
        dated = [
            ArrowTypesTestDated(Date(2024, 1, 1), Int32(1)),
            ArrowTypesTestDated(Date(2024, 2, 29), Int32(2)),
        ]
        points = ArrowTypesTestPoint{:north}.(Int32[1, 3, 5], Int32[2, 4, 6])

        for file in (false, true)
            format = file ? "file" : "stream"
            @testset "Int64 storage ($format)" begin
                table = arrowtypes_test_table((id=ids,); file=file)
                @test table.id == ids
                @test eltype(table.id) === ArrowTypesTestID
                @test DataAPI.colmetadata(table, :id, "ARROW:extension:name") ==
                      String(ARROWTYPES_TEST_ID_NAME)
            end
            @testset "nullable storage ($format)" begin
                table = arrowtypes_test_table((id=nullable_ids,); file=file)
                @test isequal(table.id, nullable_ids)
                @test eltype(table.id) === Union{Missing,ArrowTypesTestID}
            end
            @testset "String storage ($format)" begin
                table = arrowtypes_test_table((label=labels,); file=file)
                @test table.label == labels
                @test eltype(table.label) === ArrowTypesTestLabel
            end
            @testset "Date storage ($format)" begin
                table = arrowtypes_test_table((day=days,); file=file)
                @test table.day == days
                @test eltype(table.day) === ArrowTypesTestDay
            end
            @testset "NamedTuple storage with Date child ($format)" begin
                table = arrowtypes_test_table((dated=dated,); file=file)
                @test table.dated == dated
                @test eltype(table.dated) === ArrowTypesTestDated
            end
            @testset "NamedTuple storage ($format)" begin
                table = arrowtypes_test_table((point=points,); file=file)
                @test table.point == points
                @test eltype(table.point) === ArrowTypesTestPoint{:north}
                @test DataAPI.colmetadata(table, :point, "ARROW:extension:name") ==
                      String(ARROWTYPES_TEST_POINT_NAME)
                @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") ==
                      "north"
            end
        end
    end

    @testset "AbstractString pointer lifting" begin
        strings = arrowtypes_test_pointer_string.(["plain", "naïve", "箭头"])
        for file in (false, true)
            ARROWTYPES_TEST_POINTER_STRING_CALLS[] = 0
            table = arrowtypes_test_table((string=strings,); file=file)
            @test table.string == strings
            @test eltype(table.string) === ArrowTypesTestPointerString
            @test ARROWTYPES_TEST_POINTER_STRING_CALLS[] == length(strings)
        end
    end

    @testset "JuliaType resolves once per field" begin
        ids = ArrowTypesTestID.(Int64.(1:1_000))
        for file in (false, true)
            bytes = arrowtypes_test_bytes((id=ids,); file=file)
            ARROWTYPES_TEST_ID_JULIATYPE_CALLS[] = 0
            table = Arrow.Table(bytes)
            @test table.id == ids
            @test ARROWTYPES_TEST_ID_JULIATYPE_CALLS[] == 1
        end
    end

    @testset "nullable marked composite" begin
        points = Union{Missing,ArrowTypesTestPoint{:nullable}}[
            ArrowTypesTestPoint{:nullable}(Int32(1), Int32(2)),
            missing,
            ArrowTypesTestPoint{:nullable}(Int32(3), Int32(4)),
        ]
        for file in (false, true)
            table = arrowtypes_test_table((point=points,); file=file)
            @test isequal(table.point, points)
            @test eltype(table.point) === Union{Missing,ArrowTypesTestPoint{:nullable}}
            @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") ==
                  "nullable"
        end
    end

    @testset "nullable marked composite keeps exact storage type" begin
        values = Union{Missing,ArrowTypesTestExactStruct}[
            ArrowTypesTestExactStruct(Int32(1)),
            missing,
            ArrowTypesTestExactStruct(Int32(2)),
        ]
        for file in (false, true)
            table = arrowtypes_test_table((value=values,); file=file)
            @test isequal(table.value, values)
            @test eltype(table.value) === Union{Missing,ArrowTypesTestExactStruct}
            field = getfield(table, :schema).fields[1]
            @test field.nullable
            @test !field.children[1].nullable
        end
    end

    @testset "custom Nothing storage uses NullType" begin
        values = [ArrowTypesTestNullLogical(), ArrowTypesTestNullLogical()]
        for file in (false, true)
            table = arrowtypes_test_table((value=values,); file=file)
            @test table.value == values
            @test eltype(table.value) === ArrowTypesTestNullLogical
            field = getfield(table, :schema).fields[1]
            @test field.type isa ARROWTYPES_TEST_AC.NullType
            @test !field.nullable
        end
    end

    @testset "marked Struct keeps an all-null Missing child" begin
        values = [ArrowTypesTestAllNull(missing), ArrowTypesTestAllNull(missing)]
        for file in (false, true)
            table = arrowtypes_test_table((value=values,); file=file)
            @test table.value == values
            @test eltype(table.value) === ArrowTypesTestAllNull
            field = getfield(table, :schema).fields[1]
            @test field.type isa ARROWTYPES_TEST_AC.StructType
            @test field.children[1].type isa ARROWTYPES_TEST_AC.NullType
        end
    end

    @testset "all-missing unmarked List keeps its marked child type" begin
        T = Union{Missing,Vector{ArrowTypesTestID}}
        values = T[missing, missing]
        for file in (false, true)
            table = arrowtypes_test_table((value=values,); file=file)
            @test isequal(table.value, values)
            @test eltype(table.value) === T
        end
    end

    @testset "dense Union schema and logical child routing" begin
        SameStorage = Union{ArrowTypesTestID,ArrowTypesTestAlternateID}
        Mixed = Union{Missing,ArrowTypesTestID,String}
        same = SameStorage[
            ArrowTypesTestID(1),
            ArrowTypesTestAlternateID(2),
            ArrowTypesTestID(3),
        ]
        mixed = Mixed[missing, ArrowTypesTestID(4), "five"]
        nothingmixed = Union{Nothing,String}[nothing, "six", nothing]
        nested = [
            Mixed[missing, ArrowTypesTestID(7), "eight"],
            Mixed[ArrowTypesTestID(9)],
            Mixed[],
        ]

        for file in (false, true)
            table = arrowtypes_test_table((; same, mixed, nothingmixed, nested); file=file)
            @test table.same == same
            @test typeof.(table.same) == typeof.(same)
            @test isequal(table.mixed, mixed)
            @test typeof.(table.mixed) == typeof.(mixed)
            @test isequal(table.nothingmixed, nothingmixed)
            @test typeof.(table.nothingmixed) == typeof.(nothingmixed)
            @test isequal(table.nested, nested)
            @test [typeof.(row) for row in table.nested] == [typeof.(row) for row in nested]

            fields = getfield(table, :schema).fields
            for field in fields[1:3]
                @test field.type isa ARROWTYPES_TEST_AC.UnionType
                @test field.type.mode == ARROWTYPES_TEST_AC.DenseMode
                @test field.type.typeids == Int8.(0:(length(field.children) - 1))
                @test getfield.(field.children, :name) ==
                      string.(0:(length(field.children) - 1))
            end
            @test !fields[1].nullable
            @test fields[2].nullable
            @test !fields[3].nullable
            @test Set(arrowtypes_test_extension_name.(fields[1].children)) == Set([
                String(ARROWTYPES_TEST_ID_NAME),
                String(ARROWTYPES_TEST_ALTERNATE_ID_NAME),
            ])
            @test String(ARROWTYPES_TEST_ID_NAME) in
                  arrowtypes_test_extension_name.(fields[2].children)
            @test "JuliaLang.Nothing" in arrowtypes_test_extension_name.(fields[3].children)

            nestedfield = fields[4]
            @test nestedfield.type isa ARROWTYPES_TEST_AC.ListType
            unionchild = only(nestedfield.children)
            @test unionchild.type isa ARROWTYPES_TEST_AC.UnionType
            @test unionchild.type.mode == ARROWTYPES_TEST_AC.DenseMode
            @test getfield.(unionchild.children, :name) ==
                  string.(0:(length(unionchild.children) - 1))
            @test String(ARROWTYPES_TEST_ID_NAME) in
                  arrowtypes_test_extension_name.(unionchild.children)
        end
    end

    @testset "Arrow 2 dense Union schema routes identical storage children" begin
        uniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[0, 1])
        idfield = arrowtypes_test_marked_id_field("")
        alternatefield = ARROWTYPES_TEST_AC.Field(
            "",
            ARROWTYPES_TEST_AC.IntType(64, true);
            nullable=false,
            metadata=[
                "ARROW:extension:name" => String(ARROWTYPES_TEST_ALTERNATE_ID_NAME),
                "ARROW:extension:metadata" => "",
            ],
        )
        _, iddata = ARROWTYPES_TEST_AC.fromjulia("", Int64[1, 3])
        _, alternatedata = ARROWTYPES_TEST_AC.fromjulia("", Int64[2])
        # Arrow 2 wrote empty child names and marked the Union nullable. Value
        # routing is still defined by the type id, not by either choice.
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            uniontype;
            nullable=true,
            children=[idfield, alternatefield],
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            uniontype,
            3,
            [
                ARROWTYPES_TEST_AC._databuffer(Int8[0, 1, 0]),
                ARROWTYPES_TEST_AC._databuffer(Int32[0, 0, 1]),
            ];
            children=[iddata, alternatedata],
            nullcount=0,
        )
        schema = ARROWTYPES_TEST_AC.Schema([field])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 3)
        expected = Union{ArrowTypesTestID,ArrowTypesTestAlternateID}[
            ArrowTypesTestID(1),
            ArrowTypesTestAlternateID(2),
            ArrowTypesTestID(3),
        ]

        for file in (false, true)
            bytes =
                file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
            table = Arrow.Table(bytes)
            @test table.value == expected
            @test typeof.(table.value) == typeof.(expected)
            readfield = getfield(table, :schema).fields[1]
            @test getfield.(readfield.children, :name) == ["", ""]
            @test readfield.type.typeids == Int8[0, 1]
            @test readfield.nullable
            @test Set(arrowtypes_test_extension_name.(readfield.children)) == Set([
                String(ARROWTYPES_TEST_ID_NAME),
                String(ARROWTYPES_TEST_ALTERNATE_ID_NAME),
            ])
        end
    end

    @testset "colmetadata cannot replace a generated extension label" begin
        points = [ArrowTypesTestPoint{:protected}(Int32(1), Int32(2))]
        metadata = Dict(
            :point => [
                "ARROW:extension:name" => "JuliaLang.ArrowTests.Wrong",
                "ARROW:extension:metadata" => "wrong",
                "application:key" => "kept",
            ],
        )
        for file in (false, true)
            table = arrowtypes_test_table((point=points,); file=file, colmetadata=metadata)
            @test table.point == points
            @test DataAPI.colmetadata(table, :point, "ARROW:extension:name") ==
                  String(ARROWTYPES_TEST_POINT_NAME)
            @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") ==
                  "protected"
            @test DataAPI.colmetadata(table, :point, "application:key") == "kept"
        end
    end

    @testset "nested custom values" begin
        lists = [
            ArrowTypesTestID[ArrowTypesTestID(1), ArrowTypesTestID(2)],
            ArrowTypesTestID[],
            ArrowTypesTestID[ArrowTypesTestID(3)],
        ]
        rows = [
            (id=ArrowTypesTestID(1), label=ArrowTypesTestLabel("one")),
            (id=ArrowTypesTestID(2), label=ArrowTypesTestLabel("two")),
            (id=ArrowTypesTestID(3), label=ArrowTypesTestLabel("three")),
        ]

        for file in (false, true)
            listtable = arrowtypes_test_table((list=lists,); file=file)
            @test listtable.list == lists
            @test all(x -> eltype(x) === ArrowTypesTestID, listtable.list)

            # An unmarked Arrow Struct keeps the facade's ordered Pair-vector
            # row representation. Its marked children still lift recursively.
            rowtable = arrowtypes_test_table((row=rows,); file=file)
            gotrows = arrowtypes_test_rowdict.(rowtable.row)
            @test getindex.(gotrows, "id") == getproperty.(rows, :id)
            @test getindex.(gotrows, "label") == getproperty.(rows, :label)
            @test all(x -> x isa ArrowTypesTestID, getindex.(gotrows, "id"))
            @test all(x -> x isa ArrowTypesTestLabel, getindex.(gotrows, "label"))
        end
    end

    @testset "two-level nested custom values" begin
        members = [
            (id=ArrowTypesTestID(1), plain=Int32(10)),
            (id=ArrowTypesTestID(2), plain=Int32(20)),
            (id=ArrowTypesTestID(3), plain=Int32(30)),
        ]
        lists = [members[1:2], members[3:3]]
        envelopes = ArrowTypesTestEnvelope.(members)
        groups = [ArrowTypesTestGroup(members[1:2]), ArrowTypesTestGroup(members[3:3])]

        for file in (false, true)
            listtable = arrowtypes_test_table((list=lists,); file=file)
            gotlists = [arrowtypes_test_rowdict.(row) for row in listtable.list]
            wantlists = [arrowtypes_test_rowdict.(row) for row in lists]
            @test gotlists == wantlists
            @test eltype(listtable.list) === Vector{Vector{Pair{String,Any}}}
            @test all(row -> eltype(row) === Vector{Pair{String,Any}}, listtable.list)
            @test all(
                member -> arrowtypes_test_rowdict(member)["id"] isa ArrowTypesTestID,
                Iterators.flatten(listtable.list),
            )

            envelopetable = arrowtypes_test_table((envelope=envelopes,); file=file)
            @test envelopetable.envelope == envelopes
            @test eltype(envelopetable.envelope) === ArrowTypesTestEnvelope

            grouptable = arrowtypes_test_table((group=groups,); file=file)
            @test grouptable.group == groups
            @test eltype(grouptable.group) === ArrowTypesTestGroup
        end
    end

    @testset "nested temporal conversion follows the parent shape" begin
        idfield = arrowtypes_test_marked_id_field("id")
        _, iddata = ARROWTYPES_TEST_AC.fromjulia("id", Int64[1, 2])
        daytype = ARROWTYPES_TEST_AC.DateType(ARROWTYPES_TEST_AC.DAY)
        dayfield = ARROWTYPES_TEST_AC.Field("day", daytype; nullable=false)
        daydata = ARROWTYPES_TEST_AC.ArrayData(
            daytype,
            2,
            [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 1])];
            nullcount=0,
        )
        structtype = ARROWTYPES_TEST_AC.StructType()
        plainfield = ARROWTYPES_TEST_AC.Field(
            "plain",
            structtype;
            nullable=false,
            children=[idfield, dayfield],
        )
        markedidfield = arrowtypes_test_marked_id_field("id")
        _, markediddata = ARROWTYPES_TEST_AC.fromjulia("id", Int64[1, 2])
        markeddayfield = ARROWTYPES_TEST_AC.Field("day", daytype; nullable=false)
        markeddaydata = ARROWTYPES_TEST_AC.ArrayData(
            daytype,
            2,
            [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 1])];
            nullcount=0,
        )
        markedfield = ARROWTYPES_TEST_AC.Field(
            "marked",
            structtype;
            nullable=false,
            metadata=[
                "ARROW:extension:name" => String(ARROWTYPES_TEST_STAMPED_ID_NAME),
                "ARROW:extension:metadata" => "",
            ],
            children=[markedidfield, markeddayfield],
        )
        structdata = ARROWTYPES_TEST_AC.ArrayData(
            structtype,
            2,
            [ARROWTYPES_TEST_AC.BufferSlice()];
            children=[iddata, daydata],
            nullcount=0,
        )
        markedstructdata = ARROWTYPES_TEST_AC.ArrayData(
            structtype,
            2,
            [ARROWTYPES_TEST_AC.BufferSlice()];
            children=[markediddata, markeddaydata],
            nullcount=0,
        )
        schema = ARROWTYPES_TEST_AC.Schema([plainfield, markedfield])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [structdata, markedstructdata], 2)
        markedvalues = [
            ArrowTypesTestStampedID(ArrowTypesTestID(1), Date(1970, 1, 1)),
            ArrowTypesTestStampedID(ArrowTypesTestID(2), Date(1970, 1, 2)),
        ]

        for file in (false, true)
            bytes =
                file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
            table = Arrow.Table(bytes)
            plainrows = arrowtypes_test_rowdict.(table.plain)
            @test getindex.(plainrows, "id") == ArrowTypesTestID.(Int64[1, 2])
            @test getindex.(plainrows, "day") == Int32[0, 1]
            @test all(x -> x isa Int32, getindex.(plainrows, "day"))
            @test table.marked == markedvalues
            @test eltype(table.marked) === ArrowTypesTestStampedID
        end

        rows = [(id=ArrowTypesTestID(1), day=Date(1970, 1, 1))]
        for file in (false, true)
            @test_throws ArgumentError arrowtypes_test_bytes((row=rows,); file=file)
        end
    end

    @testset "built-in ArrowTypes mappings" begin
        symbols = [:alpha, :beta]
        uuids = [
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
            UUID("ffeeddcc-bbaa-9988-7766-554433221100"),
        ]
        versions = [v"1.2.3", v"2.0.0-rc1+build"]
        sets = [Set([1, 2]), Set([3])]
        nothings = Nothing[nothing, nothing]

        for file in (false, true)
            format = file ? "file" : "stream"
            for (name, values, expected) in (
                (:symbol, symbols, Symbol),
                (:uuid, uuids, UUID),
                (:version, versions, VersionNumber),
                (:set, sets, Set),
                (:nothing, nothings, Nothing),
            )
                @testset "$name ($format)" begin
                    table = arrowtypes_test_table(NamedTuple{(name,)}((values,)); file=file)
                    column = Tables.getcolumn(table, name)
                    @test column == values
                    if expected === Nothing
                        @test eltype(column) === Nothing
                    else
                        @test eltype(column) <: expected
                    end
                end
            end
        end
    end

    @testset "Arrow.Stream and partitions" begin
        parts = Tables.partitioner((
            (id=Union{Missing,ArrowTypesTestID}[ArrowTypesTestID(1), missing],),
            (id=Union{Missing,ArrowTypesTestID}[ArrowTypesTestID(2), ArrowTypesTestID(3)],),
        ))
        bytes = arrowtypes_test_bytes(parts; file=false)
        batches = collect(Arrow.Stream(bytes))
        @test length(batches) == 2
        @test isequal(
            collect(batches[1].id),
            Union{Missing,ArrowTypesTestID}[ArrowTypesTestID(1), missing],
        )
        @test collect(batches[2].id) == ArrowTypesTestID.(Int64[2, 3])
        @test all(batch -> eltype(batch.id) === Union{Missing,ArrowTypesTestID}, batches)
    end

    @testset "all-missing then populated partitions" begin
        T = Union{Missing,ArrowTypesTestID}
        parts =
            Tables.partitioner(((id=T[missing, missing],), (id=T[ArrowTypesTestID(7)],)))
        expected = T[missing, missing, ArrowTypesTestID(7)]
        for file in (false, true)
            bytes = arrowtypes_test_bytes(parts; file=file)
            table = Arrow.Table(bytes)
            @test isequal(table.id, expected)
            @test eltype(table.id) === T
            @test DataAPI.colmetadata(table, :id, "ARROW:extension:name") ==
                  String(ARROWTYPES_TEST_ID_NAME)
            if !file
                batches = collect(Arrow.Stream(bytes))
                @test length(batches) == 2
                @test isequal(collect(batches[1].id), T[missing, missing])
                @test collect(batches[2].id) == T[ArrowTypesTestID(7)]
                @test all(batch -> eltype(batch.id) === T, batches)
            end
        end
    end

    @testset "partitioned DictEncode uses declared nullability" begin
        T = Union{Missing,Int64}
        parts = Tables.partitioner((
            (value=Arrow.DictEncode(T[1, 2]),),
            (value=Arrow.DictEncode(T[missing, 3]),),
        ))
        expected = T[1, 2, missing, 3]
        for file in (false, true)
            table = Arrow.Table(arrowtypes_test_bytes(parts; file=file))
            @test isequal(table.value, expected)
            field = getfield(table, :schema).fields[1]
            @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
            @test field.nullable
        end
    end

    @testset "partition extension schema mismatch" begin
        for file in (false, true)
            different_metadata = Tables.partitioner((
                (point=ArrowTypesTestPoint{:a}[ArrowTypesTestPoint{:a}(1, 2)],),
                (point=ArrowTypesTestPoint{:b}[ArrowTypesTestPoint{:b}(3, 4)],),
            ))
            @test_throws ArgumentError arrowtypes_test_bytes(different_metadata; file=file)

            different_child_labels = Tables.partitioner((
                (row=[(id=ArrowTypesTestID(1),)],),
                (row=[(id=ArrowTypesTestAlternateID(2),)],),
            ))
            @test_throws ArgumentError arrowtypes_test_bytes(
                different_child_labels;
                file=file,
            )
        end
    end

    @testset "retained schema rewrite" begin
        ids = ArrowTypesTestID.(Int64[4, 5])
        points = ArrowTypesTestPoint{:retained}.(Int32[1, 2], Int32[3, 4])
        firstids = arrowtypes_test_table((id=ids,); file=true)
        firstpoints = arrowtypes_test_table((point=points,); file=true)

        for file in (false, true)
            format = file ? "file" : "stream"
            @testset "scalar ($format)" begin
                rewritten = Arrow.Table(arrowtypes_test_bytes(firstids; file=file))
                @test rewritten.id == ids
                @test DataAPI.colmetadata(rewritten, :id, "ARROW:extension:name") ==
                      String(ARROWTYPES_TEST_ID_NAME)
            end
            @testset "struct ($format)" begin
                rewritten = Arrow.Table(arrowtypes_test_bytes(firstpoints; file=file))
                @test rewritten.point == points
                @test DataAPI.colmetadata(rewritten, :point, "ARROW:extension:metadata") ==
                      "retained"
            end
        end
    end

    @testset "retained unmarked composites with marked children" begin
        rows = [
            (id=ArrowTypesTestID(1), plain=Int32(10)),
            (id=ArrowTypesTestID(2), plain=Int32(20)),
        ]
        lists = [
            ArrowTypesTestID[ArrowTypesTestID(1), ArrowTypesTestID(2)],
            ArrowTypesTestID[ArrowTypesTestID(3)],
        ]
        structsource = arrowtypes_test_table((row=rows,); file=true)
        listsource = arrowtypes_test_table((list=lists,); file=true)

        runfield, rundata = ARROWTYPES_TEST_AC.fromjulia("run_ends", Int32[2, 4])
        _, valuedata = ARROWTYPES_TEST_AC.fromjulia("values", Int64[1, 2])
        valuefield = arrowtypes_test_marked_id_field("values")
        reetype = ARROWTYPES_TEST_AC.RunEndEncodedType()
        reefield = ARROWTYPES_TEST_AC.Field(
            "ree",
            reetype;
            nullable=false,
            children=[runfield, valuefield],
        )
        reedata = ARROWTYPES_TEST_AC.ArrayData(
            reetype,
            4,
            ARROWTYPES_TEST_AC.BufferSlice[];
            children=[rundata, valuedata],
            nullcount=0,
        )
        reeschema = ARROWTYPES_TEST_AC.Schema([reefield])
        reesource = Arrow.Table(
            Arrow.writestream(
                reeschema,
                [ARROWTYPES_TEST_AC.RecordBatch(reeschema, [reedata], 4)],
            ),
        )
        reevalues = ArrowTypesTestID.(Int64[1, 1, 2, 2])
        @test reesource.ree == reevalues

        for file in (false, true)
            structtable = arrowtypes_test_table(structsource; file=file)
            structfield = getfield(structtable, :schema).fields[1]
            @test structfield.type isa ARROWTYPES_TEST_AC.StructType
            @test arrowtypes_test_rowdict.(structtable.row) ==
                  arrowtypes_test_rowdict.(structsource.row)
            @test all(
                value -> value isa ArrowTypesTestID,
                getindex.(arrowtypes_test_rowdict.(structtable.row), "id"),
            )

            listtable = arrowtypes_test_table(listsource; file=file)
            listfield = getfield(listtable, :schema).fields[1]
            @test listfield.type isa ARROWTYPES_TEST_AC.ListType
            @test listtable.list == lists
            @test all(list -> eltype(list) === ArrowTypesTestID, listtable.list)

            reetable = arrowtypes_test_table(reesource; file=file)
            rewrittenreefield = getfield(reetable, :schema).fields[1]
            @test rewrittenreefield.type isa ARROWTYPES_TEST_AC.RunEndEncodedType
            @test reetable.ree == reevalues
            @test eltype(reetable.ree) === ArrowTypesTestID
            @test any(
                kv ->
                    first(kv) == "ARROW:extension:name" &&
                    last(kv) == String(ARROWTYPES_TEST_ID_NAME),
                rewrittenreefield.children[2].metadata,
            )
        end
    end

    @testset "schema-only public composite types with marked children" begin
        structfield = ARROWTYPES_TEST_AC.Field(
            "struct",
            ARROWTYPES_TEST_AC.StructType();
            nullable=false,
            children=[arrowtypes_test_marked_id_field("id")],
        )
        keyfield = ARROWTYPES_TEST_AC.Field(
            "key",
            ARROWTYPES_TEST_AC.IntType(64, true);
            nullable=false,
        )
        entriesfield = ARROWTYPES_TEST_AC.Field(
            "entries",
            ARROWTYPES_TEST_AC.StructType();
            nullable=false,
            children=[keyfield, arrowtypes_test_marked_id_field("value")],
        )
        mapfield = ARROWTYPES_TEST_AC.Field(
            "map",
            ARROWTYPES_TEST_AC.MapType(false);
            nullable=false,
            children=[entriesfield],
        )
        fixedfield = ARROWTYPES_TEST_AC.Field(
            "fixed",
            ARROWTYPES_TEST_AC.FixedSizeListType(2);
            nullable=false,
            children=[arrowtypes_test_marked_id_field("item")],
        )
        unknownfield = ARROWTYPES_TEST_AC.Field(
            "unknown",
            ARROWTYPES_TEST_AC.StructType();
            nullable=false,
            metadata=[
                "ARROW:extension:name" => "JuliaLang.ArrowTests.UnknownStruct",
                "ARROW:extension:metadata" => "opaque",
            ],
            children=[
                ARROWTYPES_TEST_AC.Field(
                    "plain",
                    ARROWTYPES_TEST_AC.IntType(64, true);
                    nullable=false,
                ),
            ],
        )
        schema =
            ARROWTYPES_TEST_AC.Schema([structfield, mapfield, fixedfield, unknownfield])
        expected = (
            Vector{Pair{String,Any}},
            Vector{Pair{Any,Any}},
            Vector{ArrowTypesTestID},
            Vector{Pair{String,Any}},
        )

        _, fixedchilddata = ARROWTYPES_TEST_AC.fromjulia("item", Int64[1, 2, 3, 4])
        fixeddata = ARROWTYPES_TEST_AC.ArrayData(
            fixedfield.type,
            2,
            [ARROWTYPES_TEST_AC.BufferSlice()];
            children=[fixedchilddata],
            nullcount=0,
        )
        fixedschema = ARROWTYPES_TEST_AC.Schema([fixedfield])
        fixedbatch = ARROWTYPES_TEST_AC.RecordBatch(fixedschema, [fixeddata], 2)
        fixedvalues = [
            ArrowTypesTestID[ArrowTypesTestID(1), ArrowTypesTestID(2)],
            ArrowTypesTestID[ArrowTypesTestID(3), ArrowTypesTestID(4)],
        ]

        for file in (false, true)
            bytes =
                file ? Arrow.writefile(schema, ARROWTYPES_TEST_AC.RecordBatch[]) :
                Arrow.writestream(schema, ARROWTYPES_TEST_AC.RecordBatch[])
            table =
                @test_logs (:warn, r"unsupported .*extension.*UnknownStruct") Arrow.Table(
                    bytes,
                )
            @test Tuple(
                eltype(Tables.getcolumn(table, i)) for i in eachindex(schema.fields)
            ) === expected
            @test all(i -> isempty(Tables.getcolumn(table, i)), eachindex(schema.fields))

            fixedbytes =
                file ? Arrow.writefile(fixedschema, [fixedbatch]) :
                Arrow.writestream(fixedschema, [fixedbatch])
            fixedtable = Arrow.Table(fixedbytes)
            @test fixedtable.fixed == fixedvalues
            @test eltype(fixedtable.fixed) === Vector{ArrowTypesTestID}
            @test all(row -> eltype(row) === ArrowTypesTestID, fixedtable.fixed)
        end
    end

    @testset "unknown extension fallback" begin
        metadata = Dict(
            :x => Dict(
                "ARROW:extension:name" => "JuliaLang.ArrowTests.Unknown",
                "ARROW:extension:metadata" => "opaque",
            ),
        )
        bytes = arrowtypes_test_bytes((x=Int64[7, 8],); file=false, colmetadata=metadata)
        table = @test_logs (:warn, r"unsupported .*extension.*Unknown") Arrow.Table(bytes)
        @test table.x == Int64[7, 8]
        @test eltype(table.x) === Int64
        @test DataAPI.colmetadata(table, :x, "ARROW:extension:name") ==
              "JuliaLang.ArrowTests.Unknown"
    end

    @testset "unknown duplicate-name Struct extension fallback" begin
        child1, childdata1 = ARROWTYPES_TEST_AC.fromjulia("x", Int64[1, 2])
        child2, childdata2 = ARROWTYPES_TEST_AC.fromjulia("x", Int64[10, 20])
        structtype = ARROWTYPES_TEST_AC.StructType()
        field = ARROWTYPES_TEST_AC.Field(
            "duplicate",
            structtype;
            nullable=false,
            metadata=[
                "ARROW:extension:name" => "JuliaLang.ArrowTests.UnknownDuplicateStruct",
                "ARROW:extension:metadata" => "opaque",
            ],
            children=[child1, child2],
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            structtype,
            2,
            [ARROWTYPES_TEST_AC.BufferSlice()];
            children=[childdata1, childdata2],
            nullcount=0,
        )
        schema = ARROWTYPES_TEST_AC.Schema([field])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 2)
        expected =
            [Pair{String,Any}["x" => 1, "x" => 10], Pair{String,Any}["x" => 2, "x" => 20]]

        for file in (false, true)
            bytes =
                file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
            table =
                @test_logs (:warn, r"unsupported .*extension.*UnknownDuplicateStruct") Arrow.Table(
                    bytes,
                )
            @test table.duplicate == expected
            @test eltype(table.duplicate) === Vector{Pair{String,Any}}
        end
    end

    @testset "Tables.Scan logical values" begin
        ids = ArrowTypesTestID.(Int64[1, 2, 3, 2])
        labels = ["one", "two-a", "three", "two-b"]
        bytes = arrowtypes_test_bytes((id=ids, label=labels); file=true)

        selected = Arrow.Table(bytes; scan=Tables.Scan(select=(:id,)))
        @test selected.id == ids
        @test eltype(selected.id) === ArrowTypesTestID

        filtered = Arrow.Table(
            bytes;
            scan=Tables.Scan(
                select=(:id, :label),
                filter=Tables.colcmp(==, Tables.col(:id), ArrowTypesTestID(2)),
            ),
        )
        @test filtered.id == ArrowTypesTestID.(Int64[2, 2])
        @test filtered.label == ["two-a", "two-b"]
        @test eltype(filtered.id) === ArrowTypesTestID

        parityids = ArrowTypesTestParityID.(Int64[1, 2, 3, 4, 5])
        paritybytes =
            arrowtypes_test_bytes((id=parityids, row=Int32[1, 2, 3, 4, 5]); file=true)
        parityfiltered = Arrow.Table(
            paritybytes;
            scan=Tables.Scan(
                select=(:id, :row),
                filter=Tables.colcmp(==, Tables.col(:id), ArrowTypesTestParityID(2)),
            ),
        )
        @test getfield.(parityfiltered.id, :value) == Int64[2, 4]
        @test eltype(parityfiltered.id) === ArrowTypesTestParityID
        @test parityfiltered.row == Int32[2, 4]

        nestedparity = ArrowTypesTestParityID.(Int64[1, 2, 4])
        nestedbytes = arrowtypes_test_bytes(
            (
                structrow=[(id=value,) for value in nestedparity],
                listrow=[[value] for value in nestedparity],
                row=Int32[1, 2, 3],
            );
            file=true,
        )
        nestedfull = Arrow.Table(nestedbytes)
        structfiltered = Arrow.Table(
            nestedbytes;
            scan=Tables.Scan(
                select=(:structrow, :row),
                filter=Tables.colcmp(==, Tables.col(:structrow), nestedfull.structrow[2]),
            ),
        )
        @test structfiltered.row == Int32[2, 3]
        @test [
            arrowtypes_test_rowdict(value)["id"].value for value in structfiltered.structrow
        ] == Int64[2, 4]

        listfiltered = Arrow.Table(
            nestedbytes;
            scan=Tables.Scan(
                select=(:listrow, :row),
                filter=Tables.colcmp(==, Tables.col(:listrow), nestedfull.listrow[2]),
            ),
        )
        @test listfiltered.row == Int32[2, 3]
        @test [only(value).value for value in listfiltered.listrow] == Int64[2, 4]

        stringvalues = arrowtypes_test_pointer_string.(["alpha", "beta", "alpine"])
        stringbytes =
            arrowtypes_test_bytes((value=stringvalues, row=Int32[1, 2, 3]); file=true)
        ARROWTYPES_TEST_POINTER_STRING_CALLS[] = 0
        stringfiltered = Arrow.Table(
            stringbytes;
            scan=Tables.Scan(
                select=(:value, :row),
                filter=Tables.startswith(Tables.col(:value), "al"),
            ),
        )
        @test stringfiltered.value == stringvalues[[1, 3]]
        @test eltype(stringfiltered.value) === ArrowTypesTestPointerString
        @test stringfiltered.row == Int32[1, 3]
        @test ARROWTYPES_TEST_POINTER_STRING_CALLS[] == length(stringvalues)

        nothingvalues = Nothing[nothing, nothing, nothing]
        nothingbytes =
            arrowtypes_test_bytes((value=nothingvalues, row=Int32[1, 2, 3]); file=true)
        nothingfiltered = Arrow.Table(
            nothingbytes;
            scan=Tables.Scan(
                select=(:value, :row),
                filter=Tables.colcmp(==, Tables.col(:value), nothing),
            ),
        )
        @test nothingfiltered.value == nothingvalues
        @test eltype(nothingfiltered.value) === Nothing
        @test nothingfiltered.row == Int32[1, 2, 3]

        nullfiltered = Arrow.Table(
            nothingbytes;
            scan=Tables.Scan(
                select=(:value, :row),
                filter=Tables.isnull(Tables.col(:value)),
            ),
        )
        @test Tables.rowcount(nullfiltered) == 0
        @test isempty(nullfiltered.value)
        @test eltype(nullfiltered.value) === Nothing

        notnullfiltered = Arrow.Table(
            nothingbytes;
            scan=Tables.Scan(
                select=(:value, :row),
                filter=(!Tables.isnull(Tables.col(:value))),
            ),
        )
        @test notnullfiltered.value == nothingvalues
        @test eltype(notnullfiltered.value) === Nothing
        @test notnullfiltered.row == Int32[1, 2, 3]
    end
end
