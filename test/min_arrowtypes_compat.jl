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

# The main test matrix develops the monorepo ArrowTypes package. This focused
# check proves that Arrow also works with the declared ArrowTypes 2.0 minimum.

using Arrow
using ArrowTypes
using Test

struct MinArrowTypesID
    value::Int64
end

Base.:(==)(a::MinArrowTypesID, b::MinArrowTypesID) = a.value == b.value

const MIN_ARROW_TYPES_NAME = Symbol("JuliaLang.ArrowTests.MinArrowTypesID")

ArrowTypes.ArrowType(::Type{MinArrowTypesID}) = Int64
ArrowTypes.toarrow(value::MinArrowTypesID) = value.value
ArrowTypes.arrowname(::Type{MinArrowTypesID}) = MIN_ARROW_TYPES_NAME
ArrowTypes.JuliaType(::Val{MIN_ARROW_TYPES_NAME}, storage, metadata) = MinArrowTypesID
ArrowTypes.fromarrow(::Type{MinArrowTypesID}, value::Int64) = MinArrowTypesID(value)

struct MinArrowTypesPoint
    x::Int32
    y::Int32
end

const MIN_ARROW_TYPES_POINT_NAME = Symbol("JuliaLang.ArrowTests.MinArrowTypesPoint")
const MinArrowTypesPointStorage = @NamedTuple{x::Int32, y::Int32}

ArrowTypes.ArrowType(::Type{MinArrowTypesPoint}) = MinArrowTypesPointStorage
ArrowTypes.toarrow(value::MinArrowTypesPoint) = (x=value.x, y=value.y)
ArrowTypes.arrowname(::Type{MinArrowTypesPoint}) = MIN_ARROW_TYPES_POINT_NAME
ArrowTypes.JuliaType(::Val{MIN_ARROW_TYPES_POINT_NAME}, storage, metadata) =
    MinArrowTypesPoint
# ArrowTypes 2.0 predates `fromarrowstruct`. Arrow.jl must retain this
# positional Struct-lifting fallback for the whole declared 2.x range.
ArrowTypes.fromarrow(::Type{MinArrowTypesPoint}, x::Int32, y::Int32) =
    MinArrowTypesPoint(x, y)

function roundtrip(name::Symbol, values; file::Bool)
    io = IOBuffer()
    Arrow.write(io, NamedTuple{(name,)}((values,)); file)
    return getproperty(Arrow.Table(take!(io)), name)
end

@testset "minimum ArrowTypes compatibility" begin
    @test Base.pkgversion(ArrowTypes) == v"2.0.0"

    values = MinArrowTypesID.(Int64[typemin(Int64), -1, 0, 1, typemax(Int64)])
    nullable = Union{Missing,MinArrowTypesID}[values[1], missing, values[end]]
    points = MinArrowTypesPoint[MinArrowTypesPoint(1, 2), MinArrowTypesPoint(3, 4)]
    for file in (false, true)
        @test roundtrip(:id, values; file) == values
        @test isequal(roundtrip(:id, nullable; file), nullable)
        @test roundtrip(:point, points; file) == points
    end
end
