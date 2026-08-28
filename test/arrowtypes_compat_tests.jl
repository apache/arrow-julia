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

# ArrowTypes logical-type lowering, extension metadata, and lifting through
# the public read/write facade; included from runtests.jl.

using ArrowTypes
using DataAPI
using Dates
using Tables
using UUIDs

struct ArrowTypesTestBytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::ArrowTypesTestBytesSource) = length(s.data)
Arrow.readrange(s::ArrowTypesTestBytesSource, off, len) = s.data[(off + 1):(off + len)]

struct ArrowTypesTestRetainedPartitions{S,P}
    schema::S
    parts::P
end
Tables.partitions(source::ArrowTypesTestRetainedPartitions) = source.parts
Arrow._retainedschema(source::ArrowTypesTestRetainedPartitions) = source.schema

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

struct ArrowTypesTestCountedID
    value::Int32
end
Base.:(==)(a::ArrowTypesTestCountedID, b::ArrowTypesTestCountedID) = a.value == b.value
Base.isequal(a::ArrowTypesTestCountedID, b::ArrowTypesTestCountedID) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_COUNTED_ID_NAME = Symbol("JuliaLang.ArrowTests.CountedID")
const ARROWTYPES_TEST_COUNTED_ID_TYPE_CALLS = Ref(0)
const ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS = Ref(0)
function ArrowTypes.ArrowType(::Type{ArrowTypesTestCountedID})
    ARROWTYPES_TEST_COUNTED_ID_TYPE_CALLS[] += 1
    return Int32
end
function ArrowTypes.toarrow(x::ArrowTypesTestCountedID)
    ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] += 1
    return x.value
end
ArrowTypes.arrowname(::Type{ArrowTypesTestCountedID}) = ARROWTYPES_TEST_COUNTED_ID_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_COUNTED_ID_NAME}, S, metadata) =
    ArrowTypesTestCountedID
ArrowTypes.fromarrow(::Type{ArrowTypesTestCountedID}, x::Int32) = ArrowTypesTestCountedID(x)

struct ArrowTypesTestWriteOnly
    value::Int32
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestWriteOnly}) = Int32
ArrowTypes.toarrow(x::ArrowTypesTestWriteOnly) = x.value

struct ArrowTypesTestAlternateWriteOnly
    value::Int32
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestAlternateWriteOnly}) = Int32
ArrowTypes.toarrow(x::ArrowTypesTestAlternateWriteOnly) = x.value

abstract type ArrowTypesTestAbstractWriteOnly end
struct ArrowTypesTestAbstractWriteA <: ArrowTypesTestAbstractWriteOnly
    value::Int32
end
struct ArrowTypesTestAbstractWriteB <: ArrowTypesTestAbstractWriteOnly
    value::Int32
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestAbstractWriteA}) = Int32
ArrowTypes.ArrowType(::Type{ArrowTypesTestAbstractWriteB}) = Int32
ArrowTypes.toarrow(x::ArrowTypesTestAbstractWriteA) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestAbstractWriteB) = x.value

abstract type ArrowTypesTestAbstractMixedWrite end
struct ArrowTypesTestAbstractMixedInt <: ArrowTypesTestAbstractMixedWrite
    value::Int32
end
struct ArrowTypesTestAbstractMixedString <: ArrowTypesTestAbstractMixedWrite
    value::String
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestAbstractMixedInt}) = Int32
ArrowTypes.ArrowType(::Type{ArrowTypesTestAbstractMixedString}) = String
ArrowTypes.toarrow(x::ArrowTypesTestAbstractMixedInt) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestAbstractMixedString) = x.value

abstract type ArrowTypesTestDeclaredAbstract end
struct ArrowTypesTestDeclaredConcrete <: ArrowTypesTestDeclaredAbstract
    value::Int32
end
ArrowTypes.ArrowType(::Type{<:ArrowTypesTestDeclaredAbstract}) = Int32
ArrowTypes.toarrow(x::ArrowTypesTestDeclaredAbstract) = x.value

struct ArrowTypesTestLogicalUnion
    value::Union{Int64,String}
end
Base.:(==)(a::ArrowTypesTestLogicalUnion, b::ArrowTypesTestLogicalUnion) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestLogicalUnion, b::ArrowTypesTestLogicalUnion) =
    isequal(a.value, b.value)
Base.hash(x::ArrowTypesTestLogicalUnion, h::UInt) = hash(x.value, h)

const ARROWTYPES_TEST_LOGICAL_UNION_NAME = Symbol("JuliaLang.ArrowTests.LogicalUnion")
const ARROWTYPES_TEST_LOGICAL_UNION_TYPE_CALLS = Ref(0)
const ARROWTYPES_TEST_LOGICAL_UNION_CALLS = Ref(0)
function ArrowTypes.ArrowType(::Type{ArrowTypesTestLogicalUnion})
    ARROWTYPES_TEST_LOGICAL_UNION_TYPE_CALLS[] += 1
    return Union{Int64,String}
end
function ArrowTypes.toarrow(x::ArrowTypesTestLogicalUnion)
    ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] += 1
    return x.value
end
ArrowTypes.arrowname(::Type{ArrowTypesTestLogicalUnion}) =
    ARROWTYPES_TEST_LOGICAL_UNION_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_LOGICAL_UNION_NAME}, S, metadata) =
    ArrowTypesTestLogicalUnion
ArrowTypes.fromarrow(::Type{ArrowTypesTestLogicalUnion}, x::Int64) =
    ArrowTypesTestLogicalUnion(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestLogicalUnion}, x::AbstractString) =
    ArrowTypesTestLogicalUnion(String(x))

struct ArrowTypesTestLogicalListUnion
    value::Union{Vector{Int64},String}
end
Base.:(==)(a::ArrowTypesTestLogicalListUnion, b::ArrowTypesTestLogicalListUnion) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestLogicalListUnion, b::ArrowTypesTestLogicalListUnion) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_LOGICAL_LIST_UNION_NAME =
    Symbol("JuliaLang.ArrowTests.LogicalListUnion")
ArrowTypes.ArrowType(::Type{ArrowTypesTestLogicalListUnion}) = Union{Vector{Int64},String}
ArrowTypes.toarrow(x::ArrowTypesTestLogicalListUnion) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestLogicalListUnion}) =
    ARROWTYPES_TEST_LOGICAL_LIST_UNION_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_LOGICAL_LIST_UNION_NAME}, S, metadata) =
    ArrowTypesTestLogicalListUnion
ArrowTypes.fromarrow(::Type{ArrowTypesTestLogicalListUnion}, x::AbstractVector) =
    ArrowTypesTestLogicalListUnion(Int64[x...])
ArrowTypes.fromarrow(::Type{ArrowTypesTestLogicalListUnion}, x::AbstractString) =
    ArrowTypesTestLogicalListUnion(String(x))

struct ArrowTypesTestWriteOnlyLogicalUnion
    value::Union{Int64,String}
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestWriteOnlyLogicalUnion}) = Union{Int64,String}
ArrowTypes.toarrow(x::ArrowTypesTestWriteOnlyLogicalUnion) = x.value

struct ArrowTypesTestLogicalNullUnion
    value::Union{Nothing,String}
end
Base.:(==)(a::ArrowTypesTestLogicalNullUnion, b::ArrowTypesTestLogicalNullUnion) =
    isequal(a.value, b.value)
Base.isequal(a::ArrowTypesTestLogicalNullUnion, b::ArrowTypesTestLogicalNullUnion) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_LOGICAL_NULL_UNION_NAME =
    Symbol("JuliaLang.ArrowTests.LogicalNullUnion")
ArrowTypes.ArrowType(::Type{ArrowTypesTestLogicalNullUnion}) = Union{Nothing,String}
ArrowTypes.toarrow(x::ArrowTypesTestLogicalNullUnion) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestLogicalNullUnion}) =
    ARROWTYPES_TEST_LOGICAL_NULL_UNION_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_LOGICAL_NULL_UNION_NAME}, S, metadata) =
    ArrowTypesTestLogicalNullUnion
ArrowTypes.fromarrow(::Type{ArrowTypesTestLogicalNullUnion}, ::Nothing) =
    ArrowTypesTestLogicalNullUnion(nothing)
ArrowTypes.fromarrow(::Type{ArrowTypesTestLogicalNullUnion}, x::AbstractString) =
    ArrowTypesTestLogicalNullUnion(String(x))

struct ArrowTypesTestAmbiguousMissingUnion
    value::Union{Missing,String}
end
Base.:(==)(a::ArrowTypesTestAmbiguousMissingUnion, b::ArrowTypesTestAmbiguousMissingUnion) =
    isequal(a.value, b.value)
Base.isequal(
    a::ArrowTypesTestAmbiguousMissingUnion,
    b::ArrowTypesTestAmbiguousMissingUnion,
) = isequal(a.value, b.value)

const ARROWTYPES_TEST_AMBIGUOUS_MISSING_UNION_NAME =
    Symbol("JuliaLang.ArrowTests.AmbiguousMissingUnion")
ArrowTypes.ArrowType(::Type{ArrowTypesTestAmbiguousMissingUnion}) = Union{Missing,String}
ArrowTypes.toarrow(x::ArrowTypesTestAmbiguousMissingUnion) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestAmbiguousMissingUnion}) =
    ARROWTYPES_TEST_AMBIGUOUS_MISSING_UNION_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_AMBIGUOUS_MISSING_UNION_NAME}, S, metadata) =
    ArrowTypesTestAmbiguousMissingUnion
ArrowTypes.fromarrow(::Type{ArrowTypesTestAmbiguousMissingUnion}, ::Missing) =
    ArrowTypesTestAmbiguousMissingUnion(missing)
ArrowTypes.fromarrow(::Type{ArrowTypesTestAmbiguousMissingUnion}, x::AbstractString) =
    ArrowTypesTestAmbiguousMissingUnion(String(x))

struct ArrowTypesTestNamedWriteOnly
    value::Int32
end
const ARROWTYPES_TEST_NAMED_WRITE_ONLY = Symbol("JuliaLang.ArrowTests.NamedWriteOnly")
ArrowTypes.ArrowType(::Type{ArrowTypesTestNamedWriteOnly}) = Int32
ArrowTypes.toarrow(x::ArrowTypesTestNamedWriteOnly) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestNamedWriteOnly}) =
    ARROWTYPES_TEST_NAMED_WRITE_ONLY
ArrowTypes.arrowmetadata(::Type{ArrowTypesTestNamedWriteOnly}) = "write-only"

abstract type ArrowTypesTestAbstractReadID end
struct ArrowTypesTestConcreteWriteID <: ArrowTypesTestAbstractReadID
    value::Int32
end
Base.:(==)(a::ArrowTypesTestConcreteWriteID, b::ArrowTypesTestConcreteWriteID) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestConcreteWriteID, b::ArrowTypesTestConcreteWriteID) =
    isequal(a.value, b.value)
const ARROWTYPES_TEST_ABSTRACT_READ_ID = Symbol("JuliaLang.ArrowTests.AbstractReadID")
const ARROWTYPES_TEST_CONCRETE_WRITE_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestConcreteWriteID}) = Int32
function ArrowTypes.toarrow(x::ArrowTypesTestConcreteWriteID)
    ARROWTYPES_TEST_CONCRETE_WRITE_CALLS[] += 1
    return x.value
end
ArrowTypes.arrowname(::Type{ArrowTypesTestConcreteWriteID}) =
    ARROWTYPES_TEST_ABSTRACT_READ_ID
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ABSTRACT_READ_ID}, S, metadata) =
    ArrowTypesTestAbstractReadID
ArrowTypes.fromarrow(::Type{ArrowTypesTestAbstractReadID}, x::Int32) =
    ArrowTypesTestConcreteWriteID(x)

abstract type ArrowTypesTestManyDictRuntime end
struct ArrowTypesTestManyDictValue{N} <: ArrowTypesTestManyDictRuntime
    value::Int16
end
const ARROWTYPES_TEST_MANY_DICT_TYPE_CALLS = Ref(0)
function ArrowTypes.ArrowType(::Type{<:ArrowTypesTestManyDictValue})
    ARROWTYPES_TEST_MANY_DICT_TYPE_CALLS[] += 1
    return Int16
end
ArrowTypes.toarrow(x::ArrowTypesTestManyDictValue) = x.value

function arrowtypes_test_many_dict_values(n::Int)
    values = ArrowTypesTestManyDictRuntime[]
    for i = 1:n
        T = Core.apply_type(ArrowTypesTestManyDictValue, i)
        push!(values, T(Int16(i)))
    end
    return values
end

abstract type ArrowTypesTestManyRegisteredRuntime end
struct ArrowTypesTestManyRegisteredValue{N} <: ArrowTypesTestManyRegisteredRuntime
    value::Int16
end

const ARROWTYPES_TEST_MANY_REGISTERED_NAME =
    Symbol("JuliaLang.ArrowTests.ManyRegisteredRuntime")
ArrowTypes.ArrowType(::Type{ArrowTypesTestManyRegisteredRuntime}) = Int16
ArrowTypes.ArrowType(::Type{<:ArrowTypesTestManyRegisteredValue}) = Int16
ArrowTypes.toarrow(x::ArrowTypesTestManyRegisteredValue) = x.value
ArrowTypes.arrowname(::Type{<:ArrowTypesTestManyRegisteredRuntime}) =
    ARROWTYPES_TEST_MANY_REGISTERED_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_MANY_REGISTERED_NAME}, S, metadata) =
    ArrowTypesTestManyRegisteredRuntime
ArrowTypes.fromarrow(::Type{ArrowTypesTestManyRegisteredRuntime}, x::Int16) =
    ArrowTypesTestManyRegisteredValue{0}(x)

function arrowtypes_test_many_registered_values(n::Int)
    values = ArrowTypesTestManyRegisteredRuntime[]
    for i = 1:n
        T = Core.apply_type(ArrowTypesTestManyRegisteredValue, i)
        push!(values, T(Int16(i)))
    end
    return values
end

abstract type ArrowTypesTestAbstractTupleStorage end
struct ArrowTypesTestTupleStorageValue{N} <: ArrowTypesTestAbstractTupleStorage
    value::Int16
end

const ARROWTYPES_TEST_TUPLE_STORAGE_LOWER_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestAbstractTupleStorage}) = Tuple
function ArrowTypes.toarrow(x::ArrowTypesTestTupleStorageValue{N}) where {N}
    ARROWTYPES_TEST_TUPLE_STORAGE_LOWER_CALLS[] += 1
    return ntuple(_ -> x.value, Val(N))
end

function arrowtypes_test_tuple_storage_value(n::Int, value::Integer=n)
    T = Core.apply_type(ArrowTypesTestTupleStorageValue, n)
    return T(Int16(value))
end

struct ArrowTypesTestFlexTuple
    values::Vector{Int16}
end
Base.:(==)(a::ArrowTypesTestFlexTuple, b::ArrowTypesTestFlexTuple) = a.values == b.values
Base.isequal(a::ArrowTypesTestFlexTuple, b::ArrowTypesTestFlexTuple) =
    isequal(a.values, b.values)
Base.hash(x::ArrowTypesTestFlexTuple, h::UInt) = hash(x.values, h)

const ARROWTYPES_TEST_FLEX_TUPLE_NAME = Symbol("JuliaLang.ArrowTests.FlexTuple")
const ARROWTYPES_TEST_FLEX_TUPLE_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestFlexTuple}) = Tuple
function ArrowTypes.toarrow(x::ArrowTypesTestFlexTuple)
    ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] += 1
    return Tuple(x.values)
end
ArrowTypes.arrowname(::Type{ArrowTypesTestFlexTuple}) = ARROWTYPES_TEST_FLEX_TUPLE_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_FLEX_TUPLE_NAME}, S, metadata) =
    ArrowTypesTestFlexTuple
ArrowTypes.fromarrow(::Type{ArrowTypesTestFlexTuple}, x) =
    ArrowTypesTestFlexTuple(Int16[x...])

abstract type ArrowTypesTestSameFieldStorage end
struct ArrowTypesTestSameFieldStorageValue{N} <: ArrowTypesTestSameFieldStorage
    value::Int16
end
abstract type ArrowTypesTestSameFieldLogical end
struct ArrowTypesTestSameFieldLogicalValue{N} <: ArrowTypesTestSameFieldLogical
    value::Int16
end

const ARROWTYPES_TEST_SAME_FIELD_NAME = Symbol("JuliaLang.ArrowTests.SameFieldStorage")
const ARROWTYPES_TEST_SAME_FIELD_LOWER_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestSameFieldStorageValue{N}}) where {N} = Int16
ArrowTypes.toarrow(x::ArrowTypesTestSameFieldStorageValue) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestSameFieldStorageValue{N}}) where {N} =
    ARROWTYPES_TEST_SAME_FIELD_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_SAME_FIELD_NAME}, S, metadata) =
    ArrowTypesTestSameFieldStorage
ArrowTypes.fromarrow(::Type{ArrowTypesTestSameFieldStorage}, x::Int16) =
    ArrowTypesTestSameFieldStorageValue{0}(x)

ArrowTypes.ArrowType(::Type{ArrowTypesTestSameFieldLogical}) =
    ArrowTypesTestSameFieldStorage
function ArrowTypes.toarrow(x::ArrowTypesTestSameFieldLogicalValue{N}) where {N}
    ARROWTYPES_TEST_SAME_FIELD_LOWER_CALLS[] += 1
    return ArrowTypesTestSameFieldStorageValue{N}(x.value)
end

function arrowtypes_test_same_field_values(range)
    values = ArrowTypesTestSameFieldLogical[]
    for n in range
        T = Core.apply_type(ArrowTypesTestSameFieldLogicalValue, n)
        push!(values, T(Int16(n)))
    end
    return values
end

struct ArrowTypesTestDeclaredRegisteredUnionValue{N}
    value::Int16
end

Base.:(==)(
    a::ArrowTypesTestDeclaredRegisteredUnionValue,
    b::ArrowTypesTestDeclaredRegisteredUnionValue,
) = typeof(a) === typeof(b) && a.value == b.value
ArrowTypes.ArrowType(::Type{<:ArrowTypesTestDeclaredRegisteredUnionValue}) = Int16
ArrowTypes.toarrow(x::ArrowTypesTestDeclaredRegisteredUnionValue) = x.value
ArrowTypes.fromarrow(
    ::Type{ArrowTypesTestDeclaredRegisteredUnionValue{N}},
    x::Int16,
) where {N} = ArrowTypesTestDeclaredRegisteredUnionValue{N}(x)

const ARROWTYPES_TEST_DECLARED_REGISTERED_UNION_NAMES =
    ntuple(n -> Symbol("JuliaLang.ArrowTests.DeclaredRegisteredUnionValue.$n"), 9)
ArrowTypes.arrowname(::Type{ArrowTypesTestDeclaredRegisteredUnionValue{N}}) where {N} =
    ARROWTYPES_TEST_DECLARED_REGISTERED_UNION_NAMES[N]
for n = 1:9
    T = Core.apply_type(ArrowTypesTestDeclaredRegisteredUnionValue, n)
    name = ARROWTYPES_TEST_DECLARED_REGISTERED_UNION_NAMES[n]
    @eval ArrowTypes.JuliaType(::Val{$(QuoteNode(name))}, S, metadata) = $T
end

const ArrowTypesTestDeclaredRegisteredUnion = Union{
    ArrowTypesTestDeclaredRegisteredUnionValue{1},
    ArrowTypesTestDeclaredRegisteredUnionValue{2},
    ArrowTypesTestDeclaredRegisteredUnionValue{3},
    ArrowTypesTestDeclaredRegisteredUnionValue{4},
    ArrowTypesTestDeclaredRegisteredUnionValue{5},
    ArrowTypesTestDeclaredRegisteredUnionValue{6},
    ArrowTypesTestDeclaredRegisteredUnionValue{7},
    ArrowTypesTestDeclaredRegisteredUnionValue{8},
    ArrowTypesTestDeclaredRegisteredUnionValue{9},
}
ArrowTypes.ArrowType(::Type{ArrowTypesTestDeclaredRegisteredUnion}) =
    ArrowTypesTestDeclaredRegisteredUnion

struct ArrowTypesTestNestedLeaf
    value::Int16
end
struct ArrowTypesTestNestedFixed
    value::NTuple{2,ArrowTypesTestNestedLeaf}
end
struct ArrowTypesTestNestedList
    value::Vector{ArrowTypesTestNestedLeaf}
end
struct ArrowTypesTestNestedUnion
    value::Union{ArrowTypesTestNestedFixed,ArrowTypesTestNestedList}
end
struct ArrowTypesTestNestedOuter
    value::Vector{ArrowTypesTestNestedUnion}
end

Base.:(==)(a::ArrowTypesTestNestedLeaf, b::ArrowTypesTestNestedLeaf) = a.value == b.value
Base.:(==)(a::ArrowTypesTestNestedFixed, b::ArrowTypesTestNestedFixed) = a.value == b.value
Base.:(==)(a::ArrowTypesTestNestedList, b::ArrowTypesTestNestedList) = a.value == b.value
Base.:(==)(a::ArrowTypesTestNestedUnion, b::ArrowTypesTestNestedUnion) = a.value == b.value
Base.:(==)(a::ArrowTypesTestNestedOuter, b::ArrowTypesTestNestedOuter) = a.value == b.value

const ARROWTYPES_TEST_NESTED_LEAF_NAME = Symbol("JuliaLang.ArrowTests.NestedLeaf")
const ARROWTYPES_TEST_NESTED_FIXED_NAME = Symbol("JuliaLang.ArrowTests.NestedFixed")
const ARROWTYPES_TEST_NESTED_LIST_NAME = Symbol("JuliaLang.ArrowTests.NestedList")
const ARROWTYPES_TEST_NESTED_UNION_NAME = Symbol("JuliaLang.ArrowTests.NestedUnion")
const ARROWTYPES_TEST_NESTED_OUTER_NAME = Symbol("JuliaLang.ArrowTests.NestedOuter")

ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedLeaf}) = Int16
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedFixed}) = NTuple{2,ArrowTypesTestNestedLeaf}
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedList}) = Vector{ArrowTypesTestNestedLeaf}
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedUnion}) =
    Union{ArrowTypesTestNestedFixed,ArrowTypesTestNestedList}
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedOuter}) = Vector{ArrowTypesTestNestedUnion}

ArrowTypes.toarrow(x::ArrowTypesTestNestedLeaf) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedFixed) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedList) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedUnion) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedOuter) = x.value

ArrowTypes.arrowname(::Type{ArrowTypesTestNestedLeaf}) = ARROWTYPES_TEST_NESTED_LEAF_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedFixed}) = ARROWTYPES_TEST_NESTED_FIXED_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedList}) = ARROWTYPES_TEST_NESTED_LIST_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedUnion}) = ARROWTYPES_TEST_NESTED_UNION_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedOuter}) = ARROWTYPES_TEST_NESTED_OUTER_NAME

ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_LEAF_NAME}, S, metadata) =
    ArrowTypesTestNestedLeaf
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_FIXED_NAME}, S, metadata) =
    ArrowTypesTestNestedFixed
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_LIST_NAME}, S, metadata) =
    ArrowTypesTestNestedList
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_UNION_NAME}, S, metadata) =
    ArrowTypesTestNestedUnion
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_OUTER_NAME}, S, metadata) =
    ArrowTypesTestNestedOuter

ArrowTypes.fromarrow(::Type{ArrowTypesTestNestedLeaf}, x::Int16) =
    ArrowTypesTestNestedLeaf(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestNestedFixed}, x) =
    ArrowTypesTestNestedFixed(Tuple(ArrowTypesTestNestedLeaf[y for y in x]))
ArrowTypes.fromarrow(::Type{ArrowTypesTestNestedList}, x) =
    ArrowTypesTestNestedList(collect(ArrowTypesTestNestedLeaf, x))
ArrowTypes.fromarrow(::Type{ArrowTypesTestNestedUnion}, x::ArrowTypesTestNestedFixed) =
    ArrowTypesTestNestedUnion(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestNestedUnion}, x::ArrowTypesTestNestedList) =
    ArrowTypesTestNestedUnion(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestNestedOuter}, x) =
    ArrowTypesTestNestedOuter(collect(ArrowTypesTestNestedUnion, x))

abstract type ArrowTypesTestNestedAbstractLeaf end
struct ArrowTypesTestNestedAbstractLeafValue <: ArrowTypesTestNestedAbstractLeaf
    value::Int32
end
struct ArrowTypesTestNestedAbstractFixed
    value::NTuple{2,ArrowTypesTestNestedAbstractLeaf}
end
struct ArrowTypesTestNestedAbstractList
    value::Vector{ArrowTypesTestNestedAbstractLeaf}
end
struct ArrowTypesTestNestedAbstractUnion
    value::Union{ArrowTypesTestNestedAbstractFixed,ArrowTypesTestNestedAbstractList}
end
struct ArrowTypesTestNestedAbstractOuter
    value::Vector{ArrowTypesTestNestedAbstractUnion}
end

const ARROWTYPES_TEST_NESTED_ABSTRACT_LEAF_NAME =
    Symbol("JuliaLang.ArrowTests.NestedAbstractLeaf")
const ARROWTYPES_TEST_NESTED_ABSTRACT_FIXED_NAME =
    Symbol("JuliaLang.ArrowTests.NestedAbstractFixed")
const ARROWTYPES_TEST_NESTED_ABSTRACT_LIST_NAME =
    Symbol("JuliaLang.ArrowTests.NestedAbstractList")
const ARROWTYPES_TEST_NESTED_ABSTRACT_UNION_NAME =
    Symbol("JuliaLang.ArrowTests.NestedAbstractUnion")
const ARROWTYPES_TEST_NESTED_ABSTRACT_OUTER_NAME =
    Symbol("JuliaLang.ArrowTests.NestedAbstractOuter")

ArrowTypes.ArrowType(::Type{<:ArrowTypesTestNestedAbstractLeaf}) = Int32
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedAbstractFixed}) =
    NTuple{2,ArrowTypesTestNestedAbstractLeaf}
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedAbstractList}) =
    Vector{ArrowTypesTestNestedAbstractLeaf}
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedAbstractUnion}) =
    Union{ArrowTypesTestNestedAbstractFixed,ArrowTypesTestNestedAbstractList}
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedAbstractOuter}) =
    Vector{ArrowTypesTestNestedAbstractUnion}

ArrowTypes.toarrow(x::ArrowTypesTestNestedAbstractLeafValue) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedAbstractFixed) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedAbstractList) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedAbstractUnion) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestNestedAbstractOuter) = x.value

ArrowTypes.arrowname(::Type{<:ArrowTypesTestNestedAbstractLeaf}) =
    ARROWTYPES_TEST_NESTED_ABSTRACT_LEAF_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedAbstractFixed}) =
    ARROWTYPES_TEST_NESTED_ABSTRACT_FIXED_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedAbstractList}) =
    ARROWTYPES_TEST_NESTED_ABSTRACT_LIST_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedAbstractUnion}) =
    ARROWTYPES_TEST_NESTED_ABSTRACT_UNION_NAME
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedAbstractOuter}) =
    ARROWTYPES_TEST_NESTED_ABSTRACT_OUTER_NAME

ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_ABSTRACT_LEAF_NAME}, S, metadata) =
    ArrowTypesTestNestedAbstractLeaf
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_ABSTRACT_FIXED_NAME}, S, metadata) =
    ArrowTypesTestNestedAbstractFixed
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_ABSTRACT_LIST_NAME}, S, metadata) =
    ArrowTypesTestNestedAbstractList
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_ABSTRACT_UNION_NAME}, S, metadata) =
    ArrowTypesTestNestedAbstractUnion
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_ABSTRACT_OUTER_NAME}, S, metadata) =
    ArrowTypesTestNestedAbstractOuter

ArrowTypes.fromarrow(::Type{ArrowTypesTestNestedAbstractLeaf}, x::Int32) =
    ArrowTypesTestNestedAbstractLeafValue(x)

abstract type ArrowTypesTestAbstractUnionRead end
struct ArrowTypesTestConcreteUnionWrite <: ArrowTypesTestAbstractUnionRead
    value::Union{Int64,String}
end
Base.:(==)(a::ArrowTypesTestConcreteUnionWrite, b::ArrowTypesTestConcreteUnionWrite) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestConcreteUnionWrite, b::ArrowTypesTestConcreteUnionWrite) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_ABSTRACT_UNION_READ_NAME =
    Symbol("JuliaLang.ArrowTests.AbstractUnionRead")
const ARROWTYPES_TEST_CONCRETE_UNION_WRITE_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestAbstractUnionRead}) = Union{Int64,String}
ArrowTypes.ArrowType(::Type{ArrowTypesTestConcreteUnionWrite}) = Union{Int64,String}
function ArrowTypes.toarrow(x::ArrowTypesTestConcreteUnionWrite)
    ARROWTYPES_TEST_CONCRETE_UNION_WRITE_CALLS[] += 1
    return x.value
end
ArrowTypes.arrowname(::Type{<:ArrowTypesTestAbstractUnionRead}) =
    ARROWTYPES_TEST_ABSTRACT_UNION_READ_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ABSTRACT_UNION_READ_NAME}, S, metadata) =
    ArrowTypesTestAbstractUnionRead
ArrowTypes.fromarrow(::Type{ArrowTypesTestAbstractUnionRead}, x::Int64) =
    ArrowTypesTestConcreteUnionWrite(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestAbstractUnionRead}, x::AbstractString) =
    ArrowTypesTestConcreteUnionWrite(String(x))

struct ArrowTypesTestNestedUnionParent
    child::Union{Missing,ArrowTypesTestAbstractUnionRead}
end
Base.:(==)(a::ArrowTypesTestNestedUnionParent, b::ArrowTypesTestNestedUnionParent) =
    a.child == b.child
Base.isequal(a::ArrowTypesTestNestedUnionParent, b::ArrowTypesTestNestedUnionParent) =
    isequal(a.child, b.child)

const ARROWTYPES_TEST_NESTED_UNION_PARENT_NAME =
    Symbol("JuliaLang.ArrowTests.NestedUnionParent")
const ArrowTypesTestNestedUnionParentStorage =
    @NamedTuple{child::Union{Missing,ArrowTypesTestAbstractUnionRead}}
const ARROWTYPES_TEST_NESTED_UNION_PARENT_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedUnionParent}) =
    ArrowTypesTestNestedUnionParentStorage
function ArrowTypes.toarrow(x::ArrowTypesTestNestedUnionParent)
    ARROWTYPES_TEST_NESTED_UNION_PARENT_CALLS[] += 1
    return (child=x.child,)
end
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedUnionParent}) =
    ARROWTYPES_TEST_NESTED_UNION_PARENT_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_UNION_PARENT_NAME}, S, metadata) =
    ArrowTypesTestNestedUnionParent
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestNestedUnionParent},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestNestedUnionParent(row.child)
end

abstract type ArrowTypesTestRoutedRead end
struct ArrowTypesTestRoutedInt <: ArrowTypesTestRoutedRead
    value::Int32
end
struct ArrowTypesTestRoutedString <: ArrowTypesTestRoutedRead
    value::String
end
Base.:(==)(a::ArrowTypesTestRoutedInt, b::ArrowTypesTestRoutedInt) = a.value == b.value
Base.:(==)(a::ArrowTypesTestRoutedString, b::ArrowTypesTestRoutedString) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestRoutedInt, b::ArrowTypesTestRoutedInt) =
    isequal(a.value, b.value)
Base.isequal(a::ArrowTypesTestRoutedString, b::ArrowTypesTestRoutedString) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_ROUTED_READ_NAME = Symbol("JuliaLang.ArrowTests.RoutedRead")
ArrowTypes.ArrowType(::Type{ArrowTypesTestRoutedRead}) = Union{Int32,String}
ArrowTypes.ArrowType(::Type{ArrowTypesTestRoutedInt}) = Int32
ArrowTypes.ArrowType(::Type{ArrowTypesTestRoutedString}) = String
ArrowTypes.toarrow(x::ArrowTypesTestRoutedInt) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestRoutedString) = x.value
ArrowTypes.arrowname(::Type{<:ArrowTypesTestRoutedRead}) = ARROWTYPES_TEST_ROUTED_READ_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ROUTED_READ_NAME}, S, metadata) =
    ArrowTypesTestRoutedRead
ArrowTypes.fromarrow(::Type{ArrowTypesTestRoutedRead}, x::Int32) =
    ArrowTypesTestRoutedInt(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestRoutedRead}, x::AbstractString) =
    ArrowTypesTestRoutedString(String(x))

struct ArrowTypesTestRecursiveSchema
    value::Int32
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestRecursiveSchema}) =
    Vector{ArrowTypesTestRecursiveSchema}
ArrowTypes.toarrow(::ArrowTypesTestRecursiveSchema) = ArrowTypesTestRecursiveSchema[]

struct ArrowTypesTestRelabeledWriteID <: ArrowTypesTestAbstractReadID
    value::Int32
end
Base.:(==)(a::ArrowTypesTestRelabeledWriteID, b::ArrowTypesTestRelabeledWriteID) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestRelabeledWriteID, b::ArrowTypesTestRelabeledWriteID) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_RELABELED_WRITE_ID_NAME =
    Symbol("JuliaLang.ArrowTests.RelabeledWriteID")
ArrowTypes.ArrowType(::Type{ArrowTypesTestRelabeledWriteID}) = Int32
ArrowTypes.toarrow(x::ArrowTypesTestRelabeledWriteID) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestRelabeledWriteID}) =
    ARROWTYPES_TEST_RELABELED_WRITE_ID_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_RELABELED_WRITE_ID_NAME}, S, metadata) =
    ArrowTypesTestRelabeledWriteID
ArrowTypes.fromarrow(::Type{ArrowTypesTestRelabeledWriteID}, x::Int32) =
    ArrowTypesTestRelabeledWriteID(x)

abstract type ArrowTypesTestAmbiguousRouteRead end
struct ArrowTypesTestAmbiguousRouteLeft <: ArrowTypesTestAmbiguousRouteRead
    value::Int32
end
struct ArrowTypesTestAmbiguousRouteRight <: ArrowTypesTestAmbiguousRouteRead
    value::Int32
end
Base.:(==)(a::ArrowTypesTestAmbiguousRouteLeft, b::ArrowTypesTestAmbiguousRouteLeft) =
    a.value == b.value
Base.:(==)(a::ArrowTypesTestAmbiguousRouteRight, b::ArrowTypesTestAmbiguousRouteRight) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestAmbiguousRouteLeft, b::ArrowTypesTestAmbiguousRouteLeft) =
    isequal(a.value, b.value)
Base.isequal(a::ArrowTypesTestAmbiguousRouteRight, b::ArrowTypesTestAmbiguousRouteRight) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_AMBIGUOUS_ROUTE_NAME = Symbol("JuliaLang.ArrowTests.AmbiguousRoute")
ArrowTypes.ArrowType(::Type{ArrowTypesTestAmbiguousRouteRead}) =
    Union{ArrowTypesTestAmbiguousRouteLeft,ArrowTypesTestAmbiguousRouteRight}
ArrowTypes.ArrowType(::Type{ArrowTypesTestAmbiguousRouteLeft}) = Int32
ArrowTypes.ArrowType(::Type{ArrowTypesTestAmbiguousRouteRight}) = Int32
ArrowTypes.toarrow(x::ArrowTypesTestAmbiguousRouteLeft) = x.value
ArrowTypes.toarrow(x::ArrowTypesTestAmbiguousRouteRight) = -x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestAmbiguousRouteRead}) =
    ARROWTYPES_TEST_AMBIGUOUS_ROUTE_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_AMBIGUOUS_ROUTE_NAME}, S, metadata) =
    ArrowTypesTestAmbiguousRouteRead
ArrowTypes.fromarrow(::Type{ArrowTypesTestAmbiguousRouteRead}, x::Int32) =
    x < 0 ? ArrowTypesTestAmbiguousRouteRight(-x) : ArrowTypesTestAmbiguousRouteLeft(x)

struct ArrowTypesTestSecondTimestamp
    value::DateTime
end
Base.:(==)(a::ArrowTypesTestSecondTimestamp, b::ArrowTypesTestSecondTimestamp) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestSecondTimestamp, b::ArrowTypesTestSecondTimestamp) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_SECOND_TIMESTAMP_NAME = Symbol("JuliaLang.ArrowTests.SecondTimestamp")
ArrowTypes.ArrowType(::Type{ArrowTypesTestSecondTimestamp}) = DateTime
ArrowTypes.toarrow(x::ArrowTypesTestSecondTimestamp) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestSecondTimestamp}) =
    ARROWTYPES_TEST_SECOND_TIMESTAMP_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_SECOND_TIMESTAMP_NAME}, S, metadata) =
    ArrowTypesTestSecondTimestamp
ArrowTypes.fromarrow(::Type{ArrowTypesTestSecondTimestamp}, x::DateTime) =
    ArrowTypesTestSecondTimestamp(x)

struct ArrowTypesTestNestedLogicalUnion
    child::ArrowTypesTestLogicalUnion
end
Base.:(==)(a::ArrowTypesTestNestedLogicalUnion, b::ArrowTypesTestNestedLogicalUnion) =
    a.child == b.child
Base.isequal(a::ArrowTypesTestNestedLogicalUnion, b::ArrowTypesTestNestedLogicalUnion) =
    isequal(a.child, b.child)

const ARROWTYPES_TEST_NESTED_LOGICAL_UNION_NAME =
    Symbol("JuliaLang.ArrowTests.NestedLogicalUnion")
const ArrowTypesTestNestedLogicalUnionStorage =
    @NamedTuple{child::ArrowTypesTestLogicalUnion}
ArrowTypes.ArrowType(::Type{ArrowTypesTestNestedLogicalUnion}) =
    ArrowTypesTestNestedLogicalUnionStorage
ArrowTypes.toarrow(x::ArrowTypesTestNestedLogicalUnion) = (child=x.child,)
ArrowTypes.arrowname(::Type{ArrowTypesTestNestedLogicalUnion}) =
    ARROWTYPES_TEST_NESTED_LOGICAL_UNION_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NESTED_LOGICAL_UNION_NAME}, S, metadata) =
    ArrowTypesTestNestedLogicalUnion
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestNestedLogicalUnion},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestNestedLogicalUnion(row.child)
end

struct ArrowTypesTestKindCountedUnion
    value::Union{Int64,String}
end
Base.:(==)(a::ArrowTypesTestKindCountedUnion, b::ArrowTypesTestKindCountedUnion) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestKindCountedUnion, b::ArrowTypesTestKindCountedUnion) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_KIND_COUNTED_UNION_NAME =
    Symbol("JuliaLang.ArrowTests.KindCountedUnion")
const ARROWTYPES_TEST_KIND_COUNTED_UNION_CALLS = Ref(0)
function ArrowTypes.ArrowKind(::Type{ArrowTypesTestKindCountedUnion})
    ARROWTYPES_TEST_KIND_COUNTED_UNION_CALLS[] += 1
    return ArrowTypes.StructKind()
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestKindCountedUnion}) = Union{Int64,String}
ArrowTypes.toarrow(x::ArrowTypesTestKindCountedUnion) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestKindCountedUnion}) =
    ARROWTYPES_TEST_KIND_COUNTED_UNION_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_KIND_COUNTED_UNION_NAME}, S, metadata) =
    ArrowTypesTestKindCountedUnion
ArrowTypes.fromarrow(::Type{ArrowTypesTestKindCountedUnion}, x::Int64) =
    ArrowTypesTestKindCountedUnion(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestKindCountedUnion}, x::AbstractString) =
    ArrowTypesTestKindCountedUnion(String(x))

struct ArrowTypesTestRelabeledUnionWrite <: ArrowTypesTestAbstractUnionRead
    value::Union{Int64,String}
end
Base.:(==)(a::ArrowTypesTestRelabeledUnionWrite, b::ArrowTypesTestRelabeledUnionWrite) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestRelabeledUnionWrite, b::ArrowTypesTestRelabeledUnionWrite) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_RELABELED_UNION_WRITE_NAME =
    Symbol("JuliaLang.ArrowTests.RelabeledUnionWrite")
ArrowTypes.ArrowType(::Type{ArrowTypesTestRelabeledUnionWrite}) = Union{Int64,String}
ArrowTypes.toarrow(x::ArrowTypesTestRelabeledUnionWrite) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestRelabeledUnionWrite}) =
    ARROWTYPES_TEST_RELABELED_UNION_WRITE_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_RELABELED_UNION_WRITE_NAME}, S, metadata) =
    ArrowTypesTestRelabeledUnionWrite
ArrowTypes.fromarrow(::Type{ArrowTypesTestRelabeledUnionWrite}, x::Int64) =
    ArrowTypesTestRelabeledUnionWrite(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestRelabeledUnionWrite}, x::AbstractString) =
    ArrowTypesTestRelabeledUnionWrite(String(x))

struct ArrowTypesTestMillisecondDuration
    value::Dates.Millisecond
end
Base.:(==)(a::ArrowTypesTestMillisecondDuration, b::ArrowTypesTestMillisecondDuration) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestMillisecondDuration, b::ArrowTypesTestMillisecondDuration) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_MILLISECOND_DURATION_NAME =
    Symbol("JuliaLang.ArrowTests.MillisecondDuration")
ArrowTypes.ArrowType(::Type{ArrowTypesTestMillisecondDuration}) = Dates.Millisecond
ArrowTypes.toarrow(x::ArrowTypesTestMillisecondDuration) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestMillisecondDuration}) =
    ARROWTYPES_TEST_MILLISECOND_DURATION_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_MILLISECOND_DURATION_NAME}, S, metadata) =
    ArrowTypesTestMillisecondDuration
ArrowTypes.fromarrow(::Type{ArrowTypesTestMillisecondDuration}, x::Dates.Period) =
    ArrowTypesTestMillisecondDuration(convert(Dates.Millisecond, x))

struct ArrowTypesTestDateLikeAlias
    value::DateTime
end
Base.:(==)(a::ArrowTypesTestDateLikeAlias, b::ArrowTypesTestDateLikeAlias) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestDateLikeAlias, b::ArrowTypesTestDateLikeAlias) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_DATE_LIKE_ALIAS_NAME = Symbol("JuliaLang.ArrowTests.DateLikeAlias")
ArrowTypes.ArrowType(::Type{ArrowTypesTestDateLikeAlias}) = DateTime
ArrowTypes.toarrow(x::ArrowTypesTestDateLikeAlias) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestDateLikeAlias}) =
    ARROWTYPES_TEST_DATE_LIKE_ALIAS_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_DATE_LIKE_ALIAS_NAME}, S, metadata) =
    ArrowTypesTestDateLikeAlias
ArrowTypes.fromarrow(::Type{ArrowTypesTestDateLikeAlias}, x::Date) =
    ArrowTypesTestDateLikeAlias(DateTime(x))
ArrowTypes.fromarrow(::Type{ArrowTypesTestDateLikeAlias}, x::DateTime) =
    ArrowTypesTestDateLikeAlias(x)

struct ArrowTypesTestSortedMap
    value::Dict{Int32,Int32}
end
Base.:(==)(a::ArrowTypesTestSortedMap, b::ArrowTypesTestSortedMap) = a.value == b.value
Base.isequal(a::ArrowTypesTestSortedMap, b::ArrowTypesTestSortedMap) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_SORTED_MAP_NAME = Symbol("JuliaLang.ArrowTests.SortedMap")
ArrowTypes.ArrowType(::Type{ArrowTypesTestSortedMap}) = Dict{Int32,Int32}
ArrowTypes.toarrow(x::ArrowTypesTestSortedMap) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestSortedMap}) = ARROWTYPES_TEST_SORTED_MAP_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_SORTED_MAP_NAME}, S, metadata) =
    ArrowTypesTestSortedMap
ArrowTypes.fromarrow(::Type{ArrowTypesTestSortedMap}, x::AbstractDict) =
    ArrowTypesTestSortedMap(Dict{Int32,Int32}(x))

struct ArrowTypesTestBytesAlias
    value::Vector{UInt8}
end
Base.:(==)(a::ArrowTypesTestBytesAlias, b::ArrowTypesTestBytesAlias) = a.value == b.value
Base.isequal(a::ArrowTypesTestBytesAlias, b::ArrowTypesTestBytesAlias) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_BYTES_ALIAS_NAME = Symbol("JuliaLang.ArrowTests.BytesAlias")
ArrowTypes.ArrowType(::Type{ArrowTypesTestBytesAlias}) = Vector{UInt8}
ArrowTypes.toarrow(x::ArrowTypesTestBytesAlias) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestBytesAlias}) = ARROWTYPES_TEST_BYTES_ALIAS_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_BYTES_ALIAS_NAME}, S, metadata) =
    ArrowTypesTestBytesAlias
ArrowTypes.fromarrow(::Type{ArrowTypesTestBytesAlias}, x::AbstractVector{UInt8}) =
    ArrowTypesTestBytesAlias(collect(UInt8, x))

struct ArrowTypesTestIntListAlias
    value::Vector{Int32}
end
Base.:(==)(a::ArrowTypesTestIntListAlias, b::ArrowTypesTestIntListAlias) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestIntListAlias, b::ArrowTypesTestIntListAlias) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_INT_LIST_ALIAS_NAME = Symbol("JuliaLang.ArrowTests.IntListAlias")
ArrowTypes.ArrowType(::Type{ArrowTypesTestIntListAlias}) = Vector{Int32}
ArrowTypes.toarrow(x::ArrowTypesTestIntListAlias) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestIntListAlias}) =
    ARROWTYPES_TEST_INT_LIST_ALIAS_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_INT_LIST_ALIAS_NAME}, S, metadata) =
    ArrowTypesTestIntListAlias
ArrowTypes.fromarrow(::Type{ArrowTypesTestIntListAlias}, x) =
    ArrowTypesTestIntListAlias(collect(Int32, x))

struct ArrowTypesTestFixedList3
    value::NTuple{3,Int32}
end

const ARROWTYPES_TEST_FIXED_LIST3_NAME = Symbol("JuliaLang.ArrowTests.FixedList3")
ArrowTypes.ArrowType(::Type{ArrowTypesTestFixedList3}) = NTuple{3,Int32}
ArrowTypes.toarrow(x::ArrowTypesTestFixedList3) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestFixedList3}) = ARROWTYPES_TEST_FIXED_LIST3_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_FIXED_LIST3_NAME}, S, metadata) =
    ArrowTypesTestFixedList3
ArrowTypes.fromarrow(::Type{ArrowTypesTestFixedList3}, x) =
    ArrowTypesTestFixedList3(Tuple(Int32[y for y in x]))

struct ArrowTypesTestWrongStorage
    value::String
end

const ARROWTYPES_TEST_WRONG_STORAGE_NAME = Symbol("JuliaLang.ArrowTests.WrongStorage")
ArrowTypes.ArrowType(::Type{ArrowTypesTestWrongStorage}) = String
ArrowTypes.toarrow(x::ArrowTypesTestWrongStorage) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestWrongStorage}) =
    ARROWTYPES_TEST_WRONG_STORAGE_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_WRONG_STORAGE_NAME}, S, metadata) =
    ArrowTypesTestWrongStorage
ArrowTypes.fromarrow(::Type{ArrowTypesTestWrongStorage}, x::AbstractString) =
    ArrowTypesTestWrongStorage(String(x))

struct ArrowTypesTestPlanCounted
    value::Int32
end

const ARROWTYPES_TEST_PLAN_COUNTED_NAME = Symbol("JuliaLang.ArrowTests.PlanCounted")
const ARROWTYPES_TEST_PLAN_COUNTED_KIND_CALLS = Ref(0)
const ARROWTYPES_TEST_PLAN_COUNTED_NAME_CALLS = Ref(0)
const ARROWTYPES_TEST_PLAN_COUNTED_LOWER_CALLS = Ref(0)
function ArrowTypes.ArrowKind(::Type{ArrowTypesTestPlanCounted})
    ARROWTYPES_TEST_PLAN_COUNTED_KIND_CALLS[] += 1
    return ArrowTypes.PrimitiveKind()
end
ArrowTypes.ArrowType(::Type{ArrowTypesTestPlanCounted}) = Int32
function ArrowTypes.toarrow(x::ArrowTypesTestPlanCounted)
    ARROWTYPES_TEST_PLAN_COUNTED_LOWER_CALLS[] += 1
    return x.value
end
function ArrowTypes.hasarrowname(::Type{ArrowTypesTestPlanCounted})
    ARROWTYPES_TEST_PLAN_COUNTED_NAME_CALLS[] += 1
    return true
end
ArrowTypes.arrowname(::Type{ArrowTypesTestPlanCounted}) = ARROWTYPES_TEST_PLAN_COUNTED_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_PLAN_COUNTED_NAME}, S, metadata) =
    ArrowTypesTestPlanCounted
ArrowTypes.fromarrow(::Type{ArrowTypesTestPlanCounted}, x::Int32) =
    ArrowTypesTestPlanCounted(x)

abstract type ArrowTypesTestAbstractStringStorage end
const ARROWTYPES_TEST_ABSTRACT_STRING_STORAGE_NAME =
    Symbol("JuliaLang.ArrowTests.AbstractStringStorage")
ArrowTypes.ArrowType(::Type{ArrowTypesTestAbstractStringStorage}) = String
ArrowTypes.arrowname(::Type{ArrowTypesTestAbstractStringStorage}) =
    ARROWTYPES_TEST_ABSTRACT_STRING_STORAGE_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ABSTRACT_STRING_STORAGE_NAME}, S, metadata) =
    ArrowTypesTestAbstractStringStorage

abstract type ArrowTypesTestAbstractIdentityStorage end
const ARROWTYPES_TEST_ABSTRACT_IDENTITY_STORAGE_NAME =
    Symbol("JuliaLang.ArrowTests.AbstractIdentityStorage")
ArrowTypes.arrowname(::Type{ArrowTypesTestAbstractIdentityStorage}) =
    ARROWTYPES_TEST_ABSTRACT_IDENTITY_STORAGE_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_ABSTRACT_IDENTITY_STORAGE_NAME}, S, metadata) =
    ArrowTypesTestAbstractIdentityStorage

const ARROWTYPES_TEST_MALFORMED_JULIATYPE_NAME =
    Symbol("JuliaLang.ArrowTests.MalformedJuliaType")
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_MALFORMED_JULIATYPE_NAME}, S, metadata) = 42

struct ArrowTypesTestBadLower
    value::Int8
end

const ARROWTYPES_TEST_BAD_LOWER_NAME = Symbol("JuliaLang.ArrowTests.BadLower")
ArrowTypes.ArrowType(::Type{ArrowTypesTestBadLower}) = Int8
ArrowTypes.toarrow(::ArrowTypesTestBadLower) = Int16(1_000)
ArrowTypes.arrowname(::Type{ArrowTypesTestBadLower}) = ARROWTYPES_TEST_BAD_LOWER_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_BAD_LOWER_NAME}, S, metadata) =
    ArrowTypesTestBadLower
ArrowTypes.fromarrow(::Type{ArrowTypesTestBadLower}, x::Int8) = ArrowTypesTestBadLower(x)

struct ArrowTypesTestBadUnionLower
    value::Union{Int8,String}
end

const ARROWTYPES_TEST_BAD_UNION_LOWER_NAME = Symbol("JuliaLang.ArrowTests.BadUnionLower")
ArrowTypes.ArrowType(::Type{ArrowTypesTestBadUnionLower}) = Union{Int8,String}
ArrowTypes.toarrow(::ArrowTypesTestBadUnionLower) = Float64(1.5)
ArrowTypes.arrowname(::Type{ArrowTypesTestBadUnionLower}) =
    ARROWTYPES_TEST_BAD_UNION_LOWER_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_BAD_UNION_LOWER_NAME}, S, metadata) =
    ArrowTypesTestBadUnionLower
ArrowTypes.fromarrow(::Type{ArrowTypesTestBadUnionLower}, x::Int8) =
    ArrowTypesTestBadUnionLower(x)
ArrowTypes.fromarrow(::Type{ArrowTypesTestBadUnionLower}, x::AbstractString) =
    ArrowTypesTestBadUnionLower(String(x))

struct ArrowTypesTestBadStructLower
    value::Int8
end

const ArrowTypesTestBadStructStorage = @NamedTuple{a::Int8, b::Int8}
const ARROWTYPES_TEST_BAD_STRUCT_LOWER_NAME = Symbol("JuliaLang.ArrowTests.BadStructLower")
ArrowTypes.ArrowType(::Type{ArrowTypesTestBadStructLower}) = ArrowTypesTestBadStructStorage
ArrowTypes.toarrow(x::ArrowTypesTestBadStructLower) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestBadStructLower}) =
    ARROWTYPES_TEST_BAD_STRUCT_LOWER_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_BAD_STRUCT_LOWER_NAME}, S, metadata) =
    ArrowTypesTestBadStructLower
ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestBadStructLower},
    ::Val{names},
    values...,
) where {names} = ArrowTypesTestBadStructLower(first(values))

struct ArrowTypesTestNullableBytesAlias
    value::Vector{Union{Missing,UInt8}}
end

const ARROWTYPES_TEST_NULLABLE_BYTES_ALIAS_NAME =
    Symbol("JuliaLang.ArrowTests.NullableBytesAlias")
ArrowTypes.ArrowType(::Type{ArrowTypesTestNullableBytesAlias}) =
    Vector{Union{Missing,UInt8}}
ArrowTypes.toarrow(x::ArrowTypesTestNullableBytesAlias) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestNullableBytesAlias}) =
    ARROWTYPES_TEST_NULLABLE_BYTES_ALIAS_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_NULLABLE_BYTES_ALIAS_NAME}, S, metadata) =
    ArrowTypesTestNullableBytesAlias
ArrowTypes.fromarrow(::Type{ArrowTypesTestNullableBytesAlias}, x) =
    ArrowTypesTestNullableBytesAlias(collect(Union{Missing,UInt8}, x))

const ArrowTypesTestDayTimeStorage = NamedTuple{(:days, :millis),Tuple{Int32,Int32}}
struct ArrowTypesTestDayTimeInterval
    value::ArrowTypesTestDayTimeStorage
end
Base.:(==)(a::ArrowTypesTestDayTimeInterval, b::ArrowTypesTestDayTimeInterval) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestDayTimeInterval, b::ArrowTypesTestDayTimeInterval) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_DAY_TIME_INTERVAL_NAME =
    Symbol("JuliaLang.ArrowTests.DayTimeInterval")
ArrowTypes.ArrowType(::Type{ArrowTypesTestDayTimeInterval}) = ArrowTypesTestDayTimeStorage
ArrowTypes.toarrow(x::ArrowTypesTestDayTimeInterval) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestDayTimeInterval}) =
    ARROWTYPES_TEST_DAY_TIME_INTERVAL_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_DAY_TIME_INTERVAL_NAME}, S, metadata) =
    ArrowTypesTestDayTimeInterval
ArrowTypes.fromarrow(
    ::Type{ArrowTypesTestDayTimeInterval},
    x::ArrowTypesTestDayTimeStorage,
) = ArrowTypesTestDayTimeInterval(ArrowTypesTestDayTimeStorage(x))

const ArrowTypesTestMonthDayNanoStorage =
    NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}}
struct ArrowTypesTestMonthDayNanoInterval
    value::ArrowTypesTestMonthDayNanoStorage
end
Base.:(==)(a::ArrowTypesTestMonthDayNanoInterval, b::ArrowTypesTestMonthDayNanoInterval) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestMonthDayNanoInterval, b::ArrowTypesTestMonthDayNanoInterval) =
    isequal(a.value, b.value)

const ARROWTYPES_TEST_MONTH_DAY_NANO_INTERVAL_NAME =
    Symbol("JuliaLang.ArrowTests.MonthDayNanoInterval")
ArrowTypes.ArrowType(::Type{ArrowTypesTestMonthDayNanoInterval}) =
    ArrowTypesTestMonthDayNanoStorage
ArrowTypes.toarrow(x::ArrowTypesTestMonthDayNanoInterval) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestMonthDayNanoInterval}) =
    ARROWTYPES_TEST_MONTH_DAY_NANO_INTERVAL_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_MONTH_DAY_NANO_INTERVAL_NAME}, S, metadata) =
    ArrowTypesTestMonthDayNanoInterval
ArrowTypes.fromarrow(
    ::Type{ArrowTypesTestMonthDayNanoInterval},
    x::ArrowTypesTestMonthDayNanoStorage,
) = ArrowTypesTestMonthDayNanoInterval(ArrowTypesTestMonthDayNanoStorage(x))

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

mutable struct ArrowTypesTestMutableID
    value::Int64
end
Base.:(==)(a::ArrowTypesTestMutableID, b::ArrowTypesTestMutableID) = a.value == b.value
Base.isequal(a::ArrowTypesTestMutableID, b::ArrowTypesTestMutableID) =
    isequal(a.value, b.value)
Base.hash(x::ArrowTypesTestMutableID, h::UInt) = hash(x.value, h)

const ARROWTYPES_TEST_MUTABLE_ID_NAME = Symbol("JuliaLang.ArrowTests.MutableID")
ArrowTypes.ArrowType(::Type{ArrowTypesTestMutableID}) = Int64
ArrowTypes.toarrow(x::ArrowTypesTestMutableID) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestMutableID}) = ARROWTYPES_TEST_MUTABLE_ID_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_MUTABLE_ID_NAME}, S, metadata) =
    ArrowTypesTestMutableID
ArrowTypes.fromarrow(::Type{ArrowTypesTestMutableID}, x::Int64) = ArrowTypesTestMutableID(x)

struct ArrowTypesTestReverseID
    value::Int64
end
Base.:(==)(a::ArrowTypesTestReverseID, b::ArrowTypesTestReverseID) = a.value == b.value
Base.isequal(a::ArrowTypesTestReverseID, b::ArrowTypesTestReverseID) =
    isequal(a.value, b.value)
Base.hash(x::ArrowTypesTestReverseID, h::UInt) = hash(x.value, h)
Base.isless(a::ArrowTypesTestReverseID, b::ArrowTypesTestReverseID) =
    isless(b.value, a.value)

const ARROWTYPES_TEST_REVERSE_ID_NAME = Symbol("JuliaLang.ArrowTests.ReverseID")
ArrowTypes.ArrowType(::Type{ArrowTypesTestReverseID}) = Int64
ArrowTypes.toarrow(x::ArrowTypesTestReverseID) = x.value
ArrowTypes.arrowname(::Type{ArrowTypesTestReverseID}) = ARROWTYPES_TEST_REVERSE_ID_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_REVERSE_ID_NAME}, S, metadata) =
    ArrowTypesTestReverseID
ArrowTypes.fromarrow(::Type{ArrowTypesTestReverseID}, x::Int64) = ArrowTypesTestReverseID(x)

struct ArrowTypesTestCollidingMapKey
    value::Int64
end
Base.:(==)(a::ArrowTypesTestCollidingMapKey, b::ArrowTypesTestCollidingMapKey) =
    a.value == b.value
Base.isequal(a::ArrowTypesTestCollidingMapKey, b::ArrowTypesTestCollidingMapKey) =
    isequal(a.value, b.value)
Base.hash(x::ArrowTypesTestCollidingMapKey, h::UInt) = hash(x.value, h)

const ARROWTYPES_TEST_COLLIDING_MAP_KEY_NAME =
    Symbol("JuliaLang.ArrowTests.CollidingMapKey")
ArrowTypes.ArrowType(::Type{ArrowTypesTestCollidingMapKey}) = Int64
ArrowTypes.toarrow(::ArrowTypesTestCollidingMapKey) = Int64(0)
ArrowTypes.arrowname(::Type{ArrowTypesTestCollidingMapKey}) =
    ARROWTYPES_TEST_COLLIDING_MAP_KEY_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_COLLIDING_MAP_KEY_NAME}, S, metadata) =
    ArrowTypesTestCollidingMapKey
ArrowTypes.fromarrow(::Type{ArrowTypesTestCollidingMapKey}, x::Int64) =
    ArrowTypesTestCollidingMapKey(x)

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

struct ArrowTypesTestLabelRow
    text::ArrowTypesTestLabel
end
Base.:(==)(a::ArrowTypesTestLabelRow, b::ArrowTypesTestLabelRow) = a.text == b.text
Base.isequal(a::ArrowTypesTestLabelRow, b::ArrowTypesTestLabelRow) = isequal(a.text, b.text)

const ARROWTYPES_TEST_LABEL_ROW_NAME = Symbol("JuliaLang.ArrowTests.LabelRow")
const ArrowTypesTestLabelRowStorage = @NamedTuple{text::ArrowTypesTestLabel}
const ARROWTYPES_TEST_LABEL_ROW_CALLS = Ref(0)
ArrowTypes.ArrowType(::Type{ArrowTypesTestLabelRow}) = ArrowTypesTestLabelRowStorage
function ArrowTypes.toarrow(x::ArrowTypesTestLabelRow)
    ARROWTYPES_TEST_LABEL_ROW_CALLS[] += 1
    return (text=x.text,)
end
ArrowTypes.arrowname(::Type{ArrowTypesTestLabelRow}) = ARROWTYPES_TEST_LABEL_ROW_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_LABEL_ROW_NAME}, S, metadata) =
    ArrowTypesTestLabelRow
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestLabelRow},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestLabelRow(row.text)
end

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

struct ArrowTypesTestTemporalPack
    pair::NTuple{2,ArrowTypesTestDay}
    stamp::DateTime
end
Base.:(==)(a::ArrowTypesTestTemporalPack, b::ArrowTypesTestTemporalPack) =
    a.pair == b.pair && a.stamp == b.stamp
Base.isequal(a::ArrowTypesTestTemporalPack, b::ArrowTypesTestTemporalPack) =
    isequal(a.pair, b.pair) && isequal(a.stamp, b.stamp)

const ARROWTYPES_TEST_TEMPORAL_PACK_NAME = Symbol("JuliaLang.ArrowTests.TemporalPack")
const ArrowTypesTestTemporalPackStorage =
    @NamedTuple{pair::NTuple{2,ArrowTypesTestDay}, stamp::DateTime}
ArrowTypes.ArrowType(::Type{ArrowTypesTestTemporalPack}) = ArrowTypesTestTemporalPackStorage
ArrowTypes.toarrow(x::ArrowTypesTestTemporalPack) = (pair=x.pair, stamp=x.stamp)
ArrowTypes.arrowname(::Type{ArrowTypesTestTemporalPack}) =
    ARROWTYPES_TEST_TEMPORAL_PACK_NAME
ArrowTypes.JuliaType(::Val{ARROWTYPES_TEST_TEMPORAL_PACK_NAME}, S, metadata) =
    ArrowTypesTestTemporalPack
function ArrowTypes.fromarrowstruct(
    ::Type{ArrowTypesTestTemporalPack},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return ArrowTypesTestTemporalPack(row.pair, row.stamp)
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

arrowtypes_test_extension_metadata(name) = Pair{String,String}[
    "ARROW:extension:name" => String(name),
    "ARROW:extension:metadata" => "",
]

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

function arrowtypes_test_field_contract_equal(a, b)
    a.name == b.name || return false
    ARROWTYPES_TEST_AC.typeequal(a.type, b.type) || return false
    a.nullable == b.nullable || return false
    something(a.metadata, Pair{String,String}[]) ==
    something(b.metadata, Pair{String,String}[]) || return false
    length(a.children) == length(b.children) || return false
    return all(
        arrowtypes_test_field_contract_equal(a.children[i], b.children[i]) for
        i in eachindex(a.children)
    )
end

function arrowtypes_test_retained_rewrites(field, data, expected)
    schema = ARROWTYPES_TEST_AC.Schema([field])
    batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], length(expected))
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(schema, [batch]) :
            Arrow.writestream(schema, [batch])
        @test Arrow.Table(inputbytes).value == expected
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            @test Arrow.Table(outputbytes).value == expected
            outputfield, _ = arrowtypes_test_core_parts(outputbytes)
            @test arrowtypes_test_field_contract_equal(outputfield, field)
        end
    end
    return nothing
end

function arrowtypes_test_retained_replacement_error(field, data, replacement, messages)
    schema = ARROWTYPES_TEST_AC.Schema([field])
    batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], length(replacement))
    for inputfile in (false, true),
        sourcekind in (:table, :stream),
        outputfile in (false, true)

        inputbytes =
            inputfile ? Arrow.writefile(schema, [batch]) :
            Arrow.writestream(schema, [batch])
        source = if sourcekind === :table
            table = Arrow.Table(inputbytes)
            getfield(table, :columns)[1] = replacement
            table
        else
            parts = collect(Arrow.Stream(inputbytes))
            getfield(only(parts), :columns)[1] = replacement
            ArrowTypesTestRetainedPartitions(schema, Tuple(parts))
        end
        err = try
            arrowtypes_test_bytes(source; file=outputfile)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        rendered = sprint(showerror, err)
        for message in (messages isa AbstractString ? (messages,) : messages)
            @test occursin(message, rendered)
        end
    end
    return nothing
end

function arrowtypes_test_core_parts(bytes)
    stream = Arrow.Stream(bytes)
    source = getfield(stream, :src)
    budget = getfield(stream, :budget)
    field = Arrow._batchfields(source)[1]
    batches = [Arrow._batch(source, i, budget) for i = 1:Arrow._nbatches(source)]
    return field, batches
end

@testset "module boundary" begin
    @test Arrow.ArrowTypes === ArrowTypes
    # The one Arrow 2.x export kept for compatibility: `using Arrow` provides
    # the bare `ArrowTypes` binding.
    @test Base.isexported(Arrow, :ArrowTypes)
end

@testset "automatic scalar and struct mappings" begin
    ids = ArrowTypesTestID.(Int64[1, 2, 3])
    nullable_ids = Union{Missing,ArrowTypesTestID}[ids[1], missing, ids[3]]
    any_ids = Any[ids...]
    nullable_any_ids = Any[ids[1], missing, ids[3]]
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
        @testset "homogeneous Any storage ($format)" begin
            table = arrowtypes_test_table((id=any_ids,); file=file)
            @test table.id == ids
            @test eltype(table.id) === ArrowTypesTestID
            @test DataAPI.colmetadata(table, :id, "ARROW:extension:name") ==
                  String(ARROWTYPES_TEST_ID_NAME)

            nullable = arrowtypes_test_table((id=nullable_any_ids,); file=file)
            @test isequal(nullable.id, nullable_ids)
            @test eltype(nullable.id) === Union{Missing,ArrowTypesTestID}
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
            @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") == "north"
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

    partitioned = Tables.partitioner(((id=ids[1:1],), (id=ids[2:2],), (id=ids[3:3],)))
    for file in (false, true)
        bytes = arrowtypes_test_bytes(partitioned; file)
        ARROWTYPES_TEST_ID_JULIATYPE_CALLS[] = 0
        batches = collect(Arrow.Stream(bytes))
        @test only.(getproperty.(batches, :id)) == ids[1:3]
        @test ARROWTYPES_TEST_ID_JULIATYPE_CALLS[] == 1
    end

    retainedbytes = arrowtypes_test_bytes((id=ids[1:2],); file=false)
    retainedfield, _ = arrowtypes_test_core_parts(retainedbytes)
    for partitioncount in (1, 4, 16)
        parts = AbstractVector[
            ArrowTypesTestID[ArrowTypesTestID(partition)] for partition = 1:partitioncount
        ]
        ARROWTYPES_TEST_ID_JULIATYPE_CALLS[] = 0
        rebuiltfield, rebuiltdata =
            Arrow._constructcolumn(:id, parts; retained=retainedfield)
        @test arrowtypes_test_field_contract_equal(retainedfield, rebuiltfield)
        @test length(rebuiltdata) == partitioncount
        @test ARROWTYPES_TEST_ID_JULIATYPE_CALLS[] == 1
    end

    countedbytes = arrowtypes_test_bytes(
        (id=ArrowTypesTestCountedID[ArrowTypesTestCountedID(Int32(1))],);
        file=false,
    )
    countedfield, _ = arrowtypes_test_core_parts(countedbytes)
    for partitioncount in (1, 4, 16)
        parts = AbstractVector[
            ArrowTypesTestCountedID[
                ArrowTypesTestCountedID(Int32(3 * partition - 2)),
                ArrowTypesTestCountedID(Int32(3 * partition - 1)),
                ArrowTypesTestCountedID(Int32(3 * partition)),
            ] for partition = 1:partitioncount
        ]
        ARROWTYPES_TEST_COUNTED_ID_TYPE_CALLS[] = 0
        ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] = 0
        _, rebuiltdata = Arrow._constructcolumn(:id, parts; retained=countedfield)
        @test length(rebuiltdata) == partitioncount
        @test ARROWTYPES_TEST_COUNTED_ID_TYPE_CALLS[] == 1
        @test ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] == 3 * partitioncount
    end

    unionvalues = ArrowTypesTestLogicalUnion[
        ArrowTypesTestLogicalUnion(Int64(1)),
        ArrowTypesTestLogicalUnion("two"),
    ]
    unionbytes = arrowtypes_test_bytes((value=unionvalues,); file=false)
    unionfield, _ = arrowtypes_test_core_parts(unionbytes)
    for partitioncount in (1, 4, 16)
        parts = AbstractVector[copy(unionvalues) for _ = 1:partitioncount]
        ARROWTYPES_TEST_LOGICAL_UNION_TYPE_CALLS[] = 0
        ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] = 0
        _, rebuiltdata = Arrow._constructcolumn(:value, parts; retained=unionfield)
        @test length(rebuiltdata) == partitioncount
        @test ARROWTYPES_TEST_LOGICAL_UNION_TYPE_CALLS[] == 1
        @test ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] == 2 * partitioncount
    end
end

@testset "fresh ArrowTypes traits are column-scoped" begin
    for n in (1, 4, 16), encoded in (false, true)
        values = ArrowTypesTestCountedID.(Int32.(1:n))
        input = encoded ? Arrow.DictEncode(values) : values
        ARROWTYPES_TEST_COUNTED_ID_TYPE_CALLS[] = 0
        ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] = 0
        bytes = arrowtypes_test_bytes((value=input,); file=false)
        @test ARROWTYPES_TEST_COUNTED_ID_TYPE_CALLS[] == 1
        @test ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] == n
        @test Arrow.Table(bytes).value == values
    end
end

@testset "fresh partition traits are column-scoped" begin
    parts = Tables.partitioner(
        Tuple(
            (value=ArrowTypesTestPlanCounted[ArrowTypesTestPlanCounted(Int32(i))],) for
            i = 1:16
        ),
    )
    ARROWTYPES_TEST_PLAN_COUNTED_KIND_CALLS[] = 0
    ARROWTYPES_TEST_PLAN_COUNTED_NAME_CALLS[] = 0
    ARROWTYPES_TEST_PLAN_COUNTED_LOWER_CALLS[] = 0
    bytes = arrowtypes_test_bytes(parts; file=false)
    @test ARROWTYPES_TEST_PLAN_COUNTED_KIND_CALLS[] == 1
    @test ARROWTYPES_TEST_PLAN_COUNTED_NAME_CALLS[] == 1
    @test ARROWTYPES_TEST_PLAN_COUNTED_LOWER_CALLS[] == 16
    @test Arrow.Table(bytes).value == ArrowTypesTestPlanCounted.(Int32.(1:16))
end

@testset "retained Union ArrowKind is column-scoped" begin
    values = ArrowTypesTestKindCountedUnion[
        isodd(i) ? ArrowTypesTestKindCountedUnion(Int64(i)) :
        ArrowTypesTestKindCountedUnion("value-$i") for i = 1:100
    ]
    inputbytes = arrowtypes_test_bytes((value=values,); file=false)
    inputfield, _ = arrowtypes_test_core_parts(inputbytes)
    source = Arrow.Table(inputbytes)
    ARROWTYPES_TEST_KIND_COUNTED_UNION_CALLS[] = 0
    outputbytes = arrowtypes_test_bytes(source; file=false)
    @test ARROWTYPES_TEST_KIND_COUNTED_UNION_CALLS[] == 1
    @test Arrow.Table(outputbytes).value == values
    outputfield, _ = arrowtypes_test_core_parts(outputbytes)
    @test arrowtypes_test_field_contract_equal(outputfield, inputfield)
end

@testset "nullable writer Union planning is column-scoped" begin
    values = Union{Missing,ArrowTypesTestLogicalUnion}[
        i % 3 == 0 ? missing :
        isodd(i) ? ArrowTypesTestLogicalUnion(Int64(i)) :
        ArrowTypesTestLogicalUnion("value-$i") for i = 1:100
    ]
    inputbytes = arrowtypes_test_bytes((value=values,); file=false)
    retainedfield, _ = arrowtypes_test_core_parts(inputbytes)
    context = Arrow._WriterContext()
    rebuiltfield, rebuiltdata = Arrow._constructpart(retainedfield, values; context)
    @test arrowtypes_test_field_contract_equal(rebuiltfield, retainedfield)
    @test length(rebuiltdata) == length(values)

    storageunion = Union{Missing,Int64,String}
    @test haskey(context.unionvariants, storageunion)
    variants = context.unionvariants[storageunion]
    @test Set(variants) == Set(Type[Missing, Int64, String])
    @test Arrow._writerunionvariants!(context, storageunion) === variants
    branchkeys = Set(
        runtime for
        ((uniontype, runtime), _) in context.unionbranches if uniontype === storageunion
    )
    @test branchkeys == Set(Type[Int64, String])
    @test count(key -> first(key) === storageunion, keys(context.unionbranches)) == 2

    for _ = 1:100
        @test Arrow._writerunionvariants!(context, storageunion) === variants
        for runtime in (Int64, String)
            Arrow._writerunionbranch!(context, storageunion, runtime, variants)
        end
    end
    @test count(key -> first(key) === storageunion, keys(context.unionbranches)) == 2
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
        @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") == "nullable"
    end
end

@testset "nullable fresh composites use bounded hidden child values" begin
    IDRow = NTuple{2,ArrowTypesTestID}
    ID = Union{Missing,IDRow}
    idcases = (
        ID[missing, (ArrowTypesTestID(1), ArrowTypesTestID(2))],
        ID[(ArrowTypesTestID(1), ArrowTypesTestID(2)), missing],
        ID[missing, missing],
    )
    for values in idcases, file in (false, true)
        expected =
            Any[row === missing ? missing : ArrowTypesTestID[row...] for row in values]
        table = arrowtypes_test_table((value=values,); file=file)
        @test isequal(table.value, expected)
        field = getfield(table, :schema).fields[1]
        @test field.type isa ARROWTYPES_TEST_AC.FixedSizeListType
        @test field.nullable
        @test arrowtypes_test_extension_name(only(field.children)) ==
              String(ARROWTYPES_TEST_ID_NAME)
    end

    payload = repeat("x", 64 * 1024)
    LabelRow = NTuple{2,ArrowTypesTestLabel}
    labels = Union{Missing,LabelRow}[
        fill(missing, 16)...,
        (ArrowTypesTestLabel(payload), ArrowTypesTestLabel("tail")),
    ]
    expectedlabels = Any[
        fill(missing, 16)...,
        ArrowTypesTestLabel[ArrowTypesTestLabel(payload), ArrowTypesTestLabel("tail")],
    ]
    for file in (false, true)
        bytes = arrowtypes_test_bytes((value=labels,); file=file)
        @test length(bytes) < 3 * length(payload)
        @test isequal(Arrow.Table(bytes).value, expectedlabels)
    end
    alllabels = Union{Missing,LabelRow}[missing, missing]
    for file in (false, true)
        @test isequal(
            arrowtypes_test_table((value=alllabels,); file=file).value,
            Missing[missing, missing],
        )
    end

    structs = Union{Missing,ArrowTypesTestLabelRow}[
        fill(missing, 16)...,
        ArrowTypesTestLabelRow(ArrowTypesTestLabel(payload)),
    ]
    for file in (false, true)
        ARROWTYPES_TEST_LABEL_ROW_CALLS[] = 0
        bytes = arrowtypes_test_bytes((value=structs,); file=file)
        @test ARROWTYPES_TEST_LABEL_ROW_CALLS[] == 1
        @test length(bytes) < 3 * length(payload)
        @test isequal(Arrow.Table(bytes).value, structs)
    end

    Inner = NTuple{2,ArrowTypesTestLabel}
    Outer = @NamedTuple{inner::Inner}
    nested = Union{Missing,Outer}[
        missing,
        (inner=(ArrowTypesTestLabel(payload), ArrowTypesTestLabel("tail")),),
    ]
    expectednested = Any[
        missing,
        Pair{String,Any}["inner" => ArrowTypesTestLabel[
            ArrowTypesTestLabel(payload),
            ArrowTypesTestLabel("tail"),
        ],],
    ]
    for file in (false, true)
        bytes = arrowtypes_test_bytes((value=nested,); file=file)
        @test length(bytes) < 3 * length(payload)
        @test isequal(Arrow.Table(bytes).value, expectednested)
    end
end

@testset "nullable fresh Struct preserves declared Union routes" begin
    U = Union{Int64,String}
    Row = @NamedTuple{u::U}
    cases = (
        Union{Missing,Row}[missing, (u="text",), (u=Int64(7),)],
        Union{Missing,Row}[(u=Int64(7),), missing, (u="text",)],
        Union{Missing,Row}[missing, missing],
    )
    for values in cases, file in (false, true)
        expected = Any[
            row === missing ? missing : Pair{String,Any}["u" => row.u] for row in values
        ]
        bytes = arrowtypes_test_bytes((value=values,); file=file)
        table = Arrow.Table(bytes)
        @test isequal(table.value, expected)
        field, batches = arrowtypes_test_core_parts(bytes)
        unionfield = only(field.children)
        @test unionfield.type isa ARROWTYPES_TEST_AC.UnionType
        @test unionfield.type.typeids == Int8[0, 1]
        @test length(unionfield.children) == 2
        @test all(
            batch -> length(only(batch.columns[1].children)) == length(values),
            batches,
        )
    end

    Inner = @NamedTuple{u::U}
    Outer = @NamedTuple{inner::Inner}
    nestedcases = (
        Union{Missing,Outer}[missing, (inner=(u=Int64(7),),), (inner=(u="text",),)],
        Union{Missing,Outer}[missing, missing],
    )
    for values in nestedcases, file in (false, true)
        expected = Any[
            row === missing ? missing :
            Pair{String,Any}["inner" => Pair{String,Any}["u" => row.inner.u],] for
            row in values
        ]
        bytes = arrowtypes_test_bytes((value=values,); file=file)
        @test isequal(Arrow.Table(bytes).value, expected)
        field, _ = arrowtypes_test_core_parts(bytes)
        unionfield = only(only(field.children).children)
        @test unionfield.type isa ARROWTYPES_TEST_AC.UnionType
        @test length(unionfield.children) == 2
    end

    NullableChild = Union{Missing,Inner}
    NullableOuter = @NamedTuple{child::NullableChild}
    nullablechildren = Union{Missing,NullableOuter}[
        missing,
        (child=missing,),
        (child=(u="text",),),
        (child=(u=Int64(7),),),
    ]
    expectednullablechildren = Any[
        missing,
        Pair{String,Any}["child" => missing],
        Pair{String,Any}["child" => Pair{String,Any}["u" => "text"],],
        Pair{String,Any}["child" => Pair{String,Any}["u" => Int64(7)],],
    ]
    for file in (false, true)
        bytes = arrowtypes_test_bytes((value=nullablechildren,); file=file)
        @test isequal(Arrow.Table(bytes).value, expectednullablechildren)
        field, _ = arrowtypes_test_core_parts(bytes)
        unionfield = only(only(field.children).children)
        @test unionfield.type isa ARROWTYPES_TEST_AC.UnionType
        @test length(unionfield.children) == 2
    end
    allmissingchildren = Union{Missing,NullableOuter}[(child=missing,), (child=missing,)]
    for file in (false, true)
        table = arrowtypes_test_table((value=allmissingchildren,); file=file)
        @test isequal(
            table.value,
            Any[Pair{String,Any}["child" => missing], Pair{String,Any}["child" => missing]],
        )
        field = getfield(table, :schema).fields[1]
        @test only(only(field.children).children).type isa ARROWTYPES_TEST_AC.UnionType
    end

    FixedInner = NTuple{2,U}
    FixedOuter = @NamedTuple{inner::FixedInner}
    fixedcases = (
        Union{Missing,FixedOuter}[missing, (inner=("text", Int64(7)),)],
        Union{Missing,FixedOuter}[missing, missing],
    )
    for values in fixedcases, file in (false, true)
        expected = Any[
            row === missing ? missing : Pair{String,Any}["inner" => Any[row.inner...],]
            for row in values
        ]
        bytes = arrowtypes_test_bytes((value=values,); file=file)
        @test isequal(Arrow.Table(bytes).value, expected)
        field, _ = arrowtypes_test_core_parts(bytes)
        unionfield = only(only(field.children).children)
        @test unionfield.type isa ARROWTYPES_TEST_AC.UnionType
        @test length(unionfield.children) == 2
    end

    Wide = NTuple{256,UInt8}
    WideUnion = Union{Wide,String}
    WideRow = @NamedTuple{u::WideUnion}
    WideNullable = Union{Missing,WideRow}
    visible = WideNullable[(u="x",)]
    hidden = WideNullable[(u="x",), fill(missing, 32)...]
    for file in (false, true)
        visiblebytes = arrowtypes_test_bytes((value=visible,); file=file)
        hiddenbytes = arrowtypes_test_bytes((value=hidden,); file=file)
        @test length(hiddenbytes) - length(visiblebytes) < 2 * 1024
        @test isequal(
            Arrow.Table(hiddenbytes).value,
            Any[Pair{String,Any}["u" => "x"], fill(missing, 32)...],
        )
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
    same =
        SameStorage[ArrowTypesTestID(1), ArrowTypesTestAlternateID(2), ArrowTypesTestID(3)]
    mixed = Mixed[missing, ArrowTypesTestID(4), "five"]
    nothingmixed = Union{Nothing,String}[nothing, "six", nothing]
    nested =
        [Mixed[missing, ArrowTypesTestID(7), "eight"], Mixed[ArrowTypesTestID(9)], Mixed[]]

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

@testset "one logical type owns its whole Union storage Field" begin
    logical = ArrowTypesTestLogicalUnion[
        ArrowTypesTestLogicalUnion(Int64(1)),
        ArrowTypesTestLogicalUnion("two"),
    ]
    nullable = Union{Missing,ArrowTypesTestLogicalUnion}[
        missing,
        ArrowTypesTestLogicalUnion("three"),
        ArrowTypesTestLogicalUnion(Int64(4)),
    ]
    for values in (logical, nullable), inputfile in (false, true)
        ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] = 0
        bytes = arrowtypes_test_bytes((value=values,); file=inputfile)
        @test ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] == count(!ismissing, values)
        @test isequal(Arrow.Table(bytes).value, values)
        field, _ = arrowtypes_test_core_parts(bytes)
        @test field.type isa ARROWTYPES_TEST_AC.UnionType
        @test arrowtypes_test_extension_name(field) ==
              String(ARROWTYPES_TEST_LOGICAL_UNION_NAME)

        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] = 0
            rewritten = arrowtypes_test_bytes(source; file=outputfile)
            @test ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] == count(!ismissing, values)
            @test isequal(Arrow.Table(rewritten).value, values)
            rewrittenfield, _ = arrowtypes_test_core_parts(rewritten)
            @test arrowtypes_test_field_contract_equal(field, rewrittenfield)
        end
    end

    Row = @NamedTuple{u::ArrowTypesTestLogicalUnion}
    structcases = (
        Union{Missing,Row}[
            missing,
            (u=ArrowTypesTestLogicalUnion("five"),),
            (u=ArrowTypesTestLogicalUnion(Int64(6)),),
        ],
        Union{Missing,Row}[missing, missing],
    )
    for values in structcases, inputfile in (false, true)
        bytes = arrowtypes_test_bytes((value=values,); file=inputfile)
        expected = Any[
            row === missing ? missing : Pair{String,Any}["u" => row.u] for row in values
        ]
        @test isequal(Arrow.Table(bytes).value, expected)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            @test isequal(
                Arrow.Table(arrowtypes_test_bytes(source; file=outputfile)).value,
                expected,
            )
        end
    end

    fixed = Union{Missing,NTuple{2,ArrowTypesTestLogicalUnion}}[
        missing,
        (ArrowTypesTestLogicalUnion("seven"), ArrowTypesTestLogicalUnion(Int64(8))),
    ]
    expectedfixed = Any[missing, collect(only(skipmissing(fixed)))]
    for inputfile in (false, true)
        bytes = arrowtypes_test_bytes((value=fixed,); file=inputfile)
        @test isequal(Arrow.Table(bytes).value, expectedfixed)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            @test isequal(
                Arrow.Table(arrowtypes_test_bytes(source; file=outputfile)).value,
                expectedfixed,
            )
        end
    end

    categories = ArrowTypesTestLogicalUnion[
        ArrowTypesTestLogicalUnion(Int64(9)),
        ArrowTypesTestLogicalUnion("ten"),
        ArrowTypesTestLogicalUnion(Int64(9)),
    ]
    for inputfile in (false, true)
        ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] = 0
        bytes = arrowtypes_test_bytes((value=Arrow.DictEncode(categories),); file=inputfile)
        @test ARROWTYPES_TEST_LOGICAL_UNION_CALLS[] == 2
        @test Arrow.Table(bytes).value == categories
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            @test Arrow.Table(arrowtypes_test_bytes(source; file=outputfile)).value ==
                  categories
        end
    end

    uniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[42, 17])
    stringfield, stringdata = ARROWTYPES_TEST_AC.fromjulia("text", ["legacy"])
    intfield, intdata = ARROWTYPES_TEST_AC.fromjulia("integer", Int64[17])
    stringfield = ARROWTYPES_TEST_AC.Field(
        stringfield.name,
        stringfield.type;
        nullable=true,
        metadata=["note" => "retained"],
        children=collect(ARROWTYPES_TEST_AC.Field, stringfield.children),
    )
    intfield = ARROWTYPES_TEST_AC.Field(
        intfield.name,
        intfield.type;
        nullable=intfield.nullable,
        metadata=["note" => "also retained"],
        children=collect(ARROWTYPES_TEST_AC.Field, intfield.children),
    )
    legacyfield = ARROWTYPES_TEST_AC.Field(
        "value",
        uniontype;
        nullable=false,
        metadata=[
            "ARROW:extension:name" => String(ARROWTYPES_TEST_LOGICAL_UNION_NAME),
            "ARROW:extension:metadata" => "",
        ],
        children=[stringfield, intfield],
    )
    legacydata = ARROWTYPES_TEST_AC.ArrayData(
        uniontype,
        2,
        [
            ARROWTYPES_TEST_AC._databuffer(Int8[42, 17]),
            ARROWTYPES_TEST_AC._databuffer(Int32[0, 0]),
        ];
        children=[stringdata, intdata],
        nullcount=0,
    )
    legacyschema = ARROWTYPES_TEST_AC.Schema([legacyfield])
    legacybatch = ARROWTYPES_TEST_AC.RecordBatch(legacyschema, [legacydata], 2)
    legacyexpected = ArrowTypesTestLogicalUnion[
        ArrowTypesTestLogicalUnion("legacy"),
        ArrowTypesTestLogicalUnion(Int64(17)),
    ]
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(legacyschema, [legacybatch]) :
            Arrow.writestream(legacyschema, [legacybatch])
        @test Arrow.Table(inputbytes).value == legacyexpected
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            @test Arrow.Table(outputbytes).value == legacyexpected
            rewrittenfield, _ = arrowtypes_test_core_parts(outputbytes)
            @test arrowtypes_test_field_contract_equal(legacyfield, rewrittenfield)
        end
    end

    stringfield, stringdata = ARROWTYPES_TEST_AC.fromjulia("text", ["external"])
    elementfield, elementdata = ARROWTYPES_TEST_AC.fromjulia("element", Int64[1, 2])
    listtype = ARROWTYPES_TEST_AC.ListType(false)
    listfield = ARROWTYPES_TEST_AC.Field(
        "numbers",
        listtype;
        nullable=false,
        children=[elementfield],
    )
    listdata = ARROWTYPES_TEST_AC.ArrayData(
        listtype,
        1,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 2])];
        children=[elementdata],
        nullcount=0,
    )
    externaltype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[42, 17])
    externalfield = ARROWTYPES_TEST_AC.Field(
        "value",
        externaltype;
        nullable=false,
        metadata=[
            "ARROW:extension:name" => String(ARROWTYPES_TEST_LOGICAL_LIST_UNION_NAME),
            "ARROW:extension:metadata" => "",
        ],
        children=[stringfield, listfield],
    )
    externaldata = ARROWTYPES_TEST_AC.ArrayData(
        externaltype,
        2,
        [
            ARROWTYPES_TEST_AC._databuffer(Int8[17, 42]),
            ARROWTYPES_TEST_AC._databuffer(Int32[0, 0]),
        ];
        children=[stringdata, listdata],
        nullcount=0,
    )
    externalschema = ARROWTYPES_TEST_AC.Schema([externalfield])
    externalbatch = ARROWTYPES_TEST_AC.RecordBatch(externalschema, [externaldata], 2)
    externalexpected = ArrowTypesTestLogicalListUnion[
        ArrowTypesTestLogicalListUnion(Int64[1, 2]),
        ArrowTypesTestLogicalListUnion("external"),
    ]
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(externalschema, [externalbatch]) :
            Arrow.writestream(externalschema, [externalbatch])
        @test Arrow.Table(inputbytes).value == externalexpected
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            @test Arrow.Table(outputbytes).value == externalexpected
            rewrittenfield, _ = arrowtypes_test_core_parts(outputbytes)
            @test arrowtypes_test_field_contract_equal(externalfield, rewrittenfield)
        end
    end
end

@testset "declared abstract Union members keep schema authority" begin
    Declared = Union{ArrowTypesTestDeclaredAbstract,Float64}
    Textual = Union{Int64,AbstractString}
    cases = (
        (
            Declared[ArrowTypesTestDeclaredConcrete(Int32(1)), Float64(2.5)],
            Any[Int32(1), Float64(2.5)],
        ),
        (Declared[], Any[]),
        (Textual[Int64(3), "four"], Any[Int64(3), "four"]),
        (Textual[], Any[]),
    )
    for (values, expected) in cases, encoded in (false, true), file in (false, true)
        input = encoded ? Arrow.DictEncode(values) : values
        bytes = arrowtypes_test_bytes((value=input,); file)
        table = Arrow.Table(bytes)
        @test table.value == expected
        @test typeof.(table.value) == typeof.(expected)
        field, _ = arrowtypes_test_core_parts(bytes)
        valuefield = encoded ? ARROWTYPES_TEST_AC.dictvaluefield(field, field.type) : field
        @test valuefield.type isa ARROWTYPES_TEST_AC.UnionType
        @test length(valuefield.children) == 2
    end
end

@testset "abstract read target keeps concrete Union writer evidence" begin
    Concrete = ArrowTypesTestConcreteUnionWrite
    Abstract = ArrowTypesTestAbstractUnionRead
    cases = (
        Concrete[Concrete(Int64(1)), Concrete("two")],
        Union{Missing,Concrete}[missing, Concrete("three"), Concrete(Int64(4))],
        Union{Missing,Concrete}[missing, missing],
        Concrete[],
    )
    for values in cases, encoded in (false, true), inputfile in (false, true)
        input = encoded ? Arrow.DictEncode(values) : values
        inputbytes = arrowtypes_test_bytes((value=input,); file=inputfile)
        inputtable = Arrow.Table(inputbytes)
        @test isequal(inputtable.value, values)
        expectedtype = Missing <: eltype(values) ? Union{Missing,Abstract} : Abstract
        @test eltype(inputtable.value) === expectedtype
        inputfield, _ = arrowtypes_test_core_parts(inputbytes)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            outputtable = Arrow.Table(outputbytes)
            @test isequal(outputtable.value, values)
            @test eltype(outputtable.value) === expectedtype
            outputfield, _ = arrowtypes_test_core_parts(outputbytes)
            @test arrowtypes_test_field_contract_equal(inputfield, outputfield)
        end
    end
end

@testset "registered abstract target routes concrete storage children" begin
    Abstract = ArrowTypesTestRoutedRead
    values = Abstract[ArrowTypesTestRoutedInt(Int32(1)), ArrowTypesTestRoutedString("two")]
    for encoded in (false, true), inputfile in (false, true)
        input = encoded ? Arrow.DictEncode(values) : values
        inputbytes = arrowtypes_test_bytes((value=input,); file=inputfile)
        inputfield, _ = arrowtypes_test_core_parts(inputbytes)
        inputtable = Arrow.Table(inputbytes)
        @test inputtable.value == values
        @test typeof.(inputtable.value) == typeof.(values)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            outputtable = Arrow.Table(outputbytes)
            @test outputtable.value == values
            @test typeof.(outputtable.value) == typeof.(values)
            outputfield, _ = arrowtypes_test_core_parts(outputbytes)
            @test arrowtypes_test_field_contract_equal(inputfield, outputfield)
        end
    end
end

@testset "unresolved abstract declarations keep concrete writer evidence" begin
    Abstract = ArrowTypesTestAbstractReadID
    Concrete = ArrowTypesTestConcreteWriteID
    layouts = (
        ((value=Abstract[Concrete(Int32(1)), Concrete(Int32(2))],), 2),
        ((value=Union{Missing,Abstract}[missing, Concrete(Int32(3))],), 1),
        (
            Tables.partitioner((
                (value=Abstract[Concrete(Int32(4))],),
                (value=Abstract[Concrete(Int32(5))],),
            )),
            2,
        ),
    )
    for (layout, expectedcalls) in layouts, file in (false, true)
        ARROWTYPES_TEST_CONCRETE_WRITE_CALLS[] = 0
        bytes = arrowtypes_test_bytes(layout; file)
        @test ARROWTYPES_TEST_CONCRETE_WRITE_CALLS[] == expectedcalls
        table = Arrow.Table(bytes)
        @test all(x -> x === missing || x isa Concrete, table.value)
        @test DataAPI.colmetadata(table, :value, "ARROW:extension:name") ==
              String(ARROWTYPES_TEST_ABSTRACT_READ_ID)
    end

    mixed = ArrowTypesTestAbstractWriteOnly[
        ArrowTypesTestAbstractWriteA(Int32(6)),
        ArrowTypesTestAbstractWriteB(Int32(7)),
    ]
    mixedbytes = arrowtypes_test_bytes((value=mixed,); file=false)
    mixedfield, _ = arrowtypes_test_core_parts(mixedbytes)
    @test mixedfield.type isa ARROWTYPES_TEST_AC.UnionType
    @test length(mixedfield.children) == 2
    @test Arrow.Table(mixedbytes).value == Int32[6, 7]
end

@testset "recursive custom writer schemas fail closed" begin
    values = ArrowTypesTestRecursiveSchema[ArrowTypesTestRecursiveSchema(Int32(1))]
    for file in (false, true)
        err = try
            arrowtypes_test_bytes((value=values,); file)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("recursive ArrowTypes storage schema", sprint(showerror, err))
    end
end

@testset "recursive value containers fail closed" begin
    selfcycle = Any[]
    push!(selfcycle, selfcycle)
    left = Any[]
    right = Any[left]
    push!(left, right)
    for (label, values) in
        (("self cycle", Any[selfcycle]), ("two-container cycle", Any[left]))
        @testset "$label ($(file ? "file" : "stream"))" for file in (false, true)
            err = try
                arrowtypes_test_bytes((value=values,); file)
                nothing
            catch e
                e
            end
            @test err isa ArgumentError
            @test occursin("recursive ArrowTypes value container", sprint(showerror, err))
        end
    end

    deep = Int32(1)
    for _ = 1:63
        deep = Any[deep]
    end
    @test Arrow._preflightwritercontainers(Any[deep]) === nothing
    toodeep = Any[deep]
    err = try
        Arrow._preflightwritercontainers(Any[toodeep])
        nothing
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin("value nesting exceeds the supported depth 64", sprint(showerror, err))
end

@testset "abstract retained target rejects subtype relabeling" begin
    source = arrowtypes_test_table(
        (id=ArrowTypesTestConcreteWriteID[ArrowTypesTestConcreteWriteID(Int32(1))],);
        file=true,
    )
    @test eltype(source.id) === ArrowTypesTestAbstractReadID
    getfield(source, :columns)[1] =
        ArrowTypesTestRelabeledWriteID[ArrowTypesTestRelabeledWriteID(Int32(1))]
    for file in (false, true)
        err = try
            arrowtypes_test_bytes(source; file)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("cannot replace abstract retained target", sprint(showerror, err))
    end
end

@testset "abstract retained Union rejects subtype relabeling" begin
    values = ArrowTypesTestConcreteUnionWrite[
        ArrowTypesTestConcreteUnionWrite(Int64(1)),
        ArrowTypesTestConcreteUnionWrite("two"),
    ]
    source = arrowtypes_test_table((value=values,); file=true)
    @test eltype(source.value) === ArrowTypesTestAbstractUnionRead
    getfield(source, :columns)[1] = ArrowTypesTestRelabeledUnionWrite[
        ArrowTypesTestRelabeledUnionWrite(Int64(1)),
        ArrowTypesTestRelabeledUnionWrite("two"),
    ]
    for file in (false, true)
        err = try
            arrowtypes_test_bytes(source; file)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("cannot replace abstract retained target", sprint(showerror, err))
    end
end

@testset "retained Union rejects ambiguous storage-only routes" begin
    leftfield, leftdata = ARROWTYPES_TEST_AC.fromjulia("left", Int32[1])
    rightfield, rightdata = ARROWTYPES_TEST_AC.fromjulia("right", Int32[-2])
    uniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[42, 17])
    field = ARROWTYPES_TEST_AC.Field(
        "value",
        uniontype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_AMBIGUOUS_ROUTE_NAME),
        children=[leftfield, rightfield],
    )
    data = ARROWTYPES_TEST_AC.ArrayData(
        uniontype,
        2,
        [
            ARROWTYPES_TEST_AC._databuffer(Int8[42, 17]),
            ARROWTYPES_TEST_AC._databuffer(Int32[0, 0]),
        ];
        children=[leftdata, rightdata],
        nullcount=0,
    )
    schema = ARROWTYPES_TEST_AC.Schema([field])
    batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 2)
    expected = ArrowTypesTestAmbiguousRouteRead[
        ArrowTypesTestAmbiguousRouteLeft(Int32(1)),
        ArrowTypesTestAmbiguousRouteRight(Int32(2)),
    ]
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(schema, [batch]) :
            Arrow.writestream(schema, [batch])
        @test Arrow.Table(inputbytes).value == expected
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            err = try
                arrowtypes_test_bytes(source; file=outputfile)
                nothing
            catch e
                e
            end
            @test err isa ArgumentError
            @test occursin("multiple equally exact children", sprint(showerror, err))
        end
    end
end

@testset "writer planning has bounded type surfaces" begin
    @test Arrow._boundedfixedliststoragetype(1024, Int16) === NTuple{1024,Int16}
    @test Arrow._boundedfixedliststoragetype(1025, Int16) === Tuple{Vararg{Int16}}
    guardchild = joinpath(@__DIR__, "arrowtypes_storage_guard_child.jl")
    guardoutput = read(
        `$(Base.julia_cmd()) --startup-file=no --history-file=no --project=$(Base.active_project()) $guardchild`,
        String,
    )
    @test occursin("oversized ArrowType storage rejected", guardoutput)
    symbolguardchild = joinpath(@__DIR__, "arrowtypes_symbol_guard_child.jl")
    symbolguardoutput = read(
        `$(Base.julia_cmd()) --startup-file=no --history-file=no --project=$(Base.active_project()) $symbolguardchild`,
        String,
    )
    @test occursin("ArrowTypes symbol guard passed", symbolguardoutput)

    tuplevalues = ArrowTypesTestAbstractTupleStorage[
        arrowtypes_test_tuple_storage_value(1, 11),
        arrowtypes_test_tuple_storage_value(2, 22),
    ]
    tuplebytes = arrowtypes_test_bytes((value=tuplevalues,); file=false)
    tuplefield, tuplebatches = arrowtypes_test_core_parts(tuplebytes)
    @test tuplefield.type isa ARROWTYPES_TEST_AC.UnionType
    @test tuplefield.type.mode == ARROWTYPES_TEST_AC.DenseMode
    @test all(
        child -> child.type isa ARROWTYPES_TEST_AC.FixedSizeListType,
        tuplefield.children,
    )
    @test sort([child.type.listsize for child in tuplefield.children]) == [1, 2]
    @test only(tuplebatches).columns[1].type == tuplefield.type
    @test collect.(Arrow.Table(tuplebytes).value) == [[Int16(11)], [Int16(22), Int16(22)]]

    excessstorage = ArrowTypesTestAbstractTupleStorage[
        arrowtypes_test_tuple_storage_value(n) for n = 1:9
    ]
    push!(excessstorage, arrowtypes_test_tuple_storage_value(1, 10))
    ARROWTYPES_TEST_TUPLE_STORAGE_LOWER_CALLS[] = 0
    err = try
        arrowtypes_test_bytes((value=excessstorage,); file=false)
        nothing
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin(
        "column value has more than 8 inferred ArrowTypes storage types",
        sprint(showerror, err),
    )
    @test ARROWTYPES_TEST_TUPLE_STORAGE_LOWER_CALLS[] == 9

    nullabletuplevalues = Union{Missing,ArrowTypesTestAbstractTupleStorage}[missing]
    for n = 1:8
        push!(nullabletuplevalues, arrowtypes_test_tuple_storage_value(n))
    end
    ARROWTYPES_TEST_TUPLE_STORAGE_LOWER_CALLS[] = 0
    nullabletuplebytes = arrowtypes_test_bytes((value=nullabletuplevalues,); file=false)
    @test ARROWTYPES_TEST_TUPLE_STORAGE_LOWER_CALLS[] == 8
    nullabletuplefield, _ = arrowtypes_test_core_parts(nullabletuplebytes)
    @test nullabletuplefield.type isa ARROWTYPES_TEST_AC.UnionType
    @test length(nullabletuplefield.children) == 9
    @test count(
        child -> child.type isa ARROWTYPES_TEST_AC.NullType,
        nullabletuplefield.children,
    ) == 1
    nullabletupleresult = Arrow.Table(nullabletuplebytes).value
    @test first(nullabletupleresult) === missing
    @test collect.(nullabletupleresult[2:end]) == [fill(Int16(n), n) for n = 1:8]

    emptyouter =
        ArrowTypesTestNestedOuter[ArrowTypesTestNestedOuter(ArrowTypesTestNestedUnion[])]
    emptyouterbytes = arrowtypes_test_bytes((value=emptyouter,); file=false)
    @test Arrow.Table(emptyouterbytes).value == emptyouter
    emptyouterfield, _ = arrowtypes_test_core_parts(emptyouterbytes)
    @test emptyouterfield.type isa ARROWTYPES_TEST_AC.ListType
    nestedunionfield = only(emptyouterfield.children)
    @test nestedunionfield.type isa ARROWTYPES_TEST_AC.UnionType
    nestedchildnames = arrowtypes_test_extension_name.(nestedunionfield.children)
    fixedchild = findfirst(==(String(ARROWTYPES_TEST_NESTED_FIXED_NAME)), nestedchildnames)
    @test fixedchild !== nothing
    fixedfield = nestedunionfield.children[fixedchild]
    @test fixedfield.type isa ARROWTYPES_TEST_AC.FixedSizeListType
    @test fixedfield.type.listsize == 2

    visibleouter = ArrowTypesTestNestedOuter[ArrowTypesTestNestedOuter(
        ArrowTypesTestNestedUnion[
            ArrowTypesTestNestedUnion(
                ArrowTypesTestNestedFixed((
                    ArrowTypesTestNestedLeaf(Int16(1)),
                    ArrowTypesTestNestedLeaf(Int16(2)),
                )),
            ),
            ArrowTypesTestNestedUnion(
                ArrowTypesTestNestedList(
                    ArrowTypesTestNestedLeaf[
                        ArrowTypesTestNestedLeaf(Int16(3)),
                        ArrowTypesTestNestedLeaf(Int16(4)),
                        ArrowTypesTestNestedLeaf(Int16(5)),
                    ],
                ),
            ),
        ],
    )]
    emptyouterschema = ARROWTYPES_TEST_AC.Schema([emptyouterfield])
    visibleoutersource =
        ArrowTypesTestRetainedPartitions(emptyouterschema, ((value=visibleouter,),))
    visibleouterbytes = arrowtypes_test_bytes(visibleoutersource; file=false)
    @test Arrow.Table(visibleouterbytes).value == visibleouter
    visibleouterfield, _ = arrowtypes_test_core_parts(visibleouterbytes)
    @test arrowtypes_test_field_contract_equal(emptyouterfield, visibleouterfield)

    emptyabstractouter =
        ArrowTypesTestNestedAbstractOuter[ArrowTypesTestNestedAbstractOuter(
            ArrowTypesTestNestedAbstractUnion[],
        ),]
    emptyabstractbytes = arrowtypes_test_bytes((value=emptyabstractouter,); file=false)
    emptyabstractfield, _ = arrowtypes_test_core_parts(emptyabstractbytes)
    @test emptyabstractfield.type isa ARROWTYPES_TEST_AC.ListType
    abstractunionfield = only(emptyabstractfield.children)
    @test abstractunionfield.type isa ARROWTYPES_TEST_AC.UnionType
    abstractchildnames = arrowtypes_test_extension_name.(abstractunionfield.children)
    abstractfixedindex = findfirst(
        ==(String(ARROWTYPES_TEST_NESTED_ABSTRACT_FIXED_NAME)),
        abstractchildnames,
    )
    abstractlistindex =
        findfirst(==(String(ARROWTYPES_TEST_NESTED_ABSTRACT_LIST_NAME)), abstractchildnames)
    @test abstractfixedindex !== nothing
    @test abstractlistindex !== nothing
    abstractfixedfield = abstractunionfield.children[abstractfixedindex]
    abstractlistfield = abstractunionfield.children[abstractlistindex]
    @test abstractfixedfield.type isa ARROWTYPES_TEST_AC.FixedSizeListType
    @test abstractfixedfield.type.listsize == 2
    @test only(abstractfixedfield.children).type == ARROWTYPES_TEST_AC.IntType(32, true)
    @test abstractlistfield.type isa ARROWTYPES_TEST_AC.ListType
    @test only(abstractlistfield.children).type == ARROWTYPES_TEST_AC.IntType(32, true)

    values128 = arrowtypes_test_many_dict_values(128)
    Arrow._observeddictionarytypes("value", values128[1:2])
    directerror = Ref{Any}(nothing)
    directallocation = @allocated try
        Arrow._observeddictionarytypes("value", values128)
    catch e
        directerror[] = e
    end
    @test directallocation < 1_048_576
    @test directerror[] isa ArgumentError
    @test occursin(
        "more than 8 inferred runtime value types",
        sprint(showerror, directerror[]),
    )

    declaredtypes = Type[typeof(value) for value in values128[1:32]]
    declared32 = Arrow._writeruniontype(declaredtypes)
    @test length(Arrow._checkedwritervariants("declared writer", declared32)) == 32
    @test length(
        Arrow._mergewritertypes("declared writer", declaredtypes, Type[], 32, "types"),
    ) == 32
    @test length(Arrow._directunionroutes(declared32)) == 32

    declaredtypes33 = copy(declaredtypes)
    push!(declaredtypes33, Core.apply_type(ArrowTypesTestManyDictValue, 33))
    declared33 = Arrow._writeruniontype(declaredtypes33)
    err = try
        Arrow._checkedwritervariants("declared writer", declared33)
        nothing
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin(
        "declared writer has more than 32 Union branches",
        sprint(showerror, err),
    )

    values8 = values128[1:8]
    input = (value=Arrow.DictEncode(values8),)
    warmbytes = arrowtypes_test_bytes(input; file=false)
    @test Arrow.Table(warmbytes).value == Int16.(1:8)
    GC.gc()
    @test (@allocated arrowtypes_test_bytes(input; file=false)) < 5_000_000

    err = try
        arrowtypes_test_bytes((value=Arrow.DictEncode(values128[1:9]),); file=false)
        nothing
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin(
        "dictionary column value has more than 8 inferred runtime value types",
        sprint(showerror, err),
    )

    split8 = Tables.partitioner((
        (value=Arrow.DictEncode(values128[1:4]),),
        (value=Arrow.DictEncode(values128[5:8]),),
    ))
    @test Arrow.Table(arrowtypes_test_bytes(split8; file=false)).value == Int16.(1:8)

    split9 = Tables.partitioner((
        (value=Arrow.DictEncode(values128[1:4]),),
        (value=Arrow.DictEncode(values128[5:9]),),
    ))
    spliterror = function ()
        try
            arrowtypes_test_bytes(split9; file=false)
            return nothing
        catch e
            return e
        end
    end
    ARROWTYPES_TEST_MANY_DICT_TYPE_CALLS[] = 0
    err = spliterror()
    @test err isa ArgumentError
    @test ARROWTYPES_TEST_MANY_DICT_TYPE_CALLS[] == 0
    @test occursin(
        "dictionary column value has more than 8 inferred runtime value types",
        sprint(showerror, err),
    )
    GC.gc()
    @test (@allocated spliterror()) < 5_000_000

    seedbytes = arrowtypes_test_bytes(
        (
            value=ArrowTypesTestManyRegisteredRuntime[ArrowTypesTestManyRegisteredValue{0}(
                Int16(0),
            ),],
        );
        file=false,
    )
    seedfield, _ = arrowtypes_test_core_parts(seedbytes)
    seedschema = ARROWTYPES_TEST_AC.Schema([seedfield])
    registered8 = arrowtypes_test_many_registered_values(8)
    source8 = ArrowTypesTestRetainedPartitions(seedschema, ((value=registered8,),))
    registeredbytes = arrowtypes_test_bytes(source8; file=false)
    @test map(x -> x.value, Arrow.Table(registeredbytes).value) == Int16.(1:8)

    source9 = ArrowTypesTestRetainedPartitions(
        seedschema,
        ((value=arrowtypes_test_many_registered_values(9),),),
    )
    err = try
        arrowtypes_test_bytes(source9; file=false)
        nothing
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin(
        "column value has more than 8 inferred registered writer runtime types",
        sprint(showerror, err),
    )

    SeedRow = @NamedTuple{child::ArrowTypesTestManyRegisteredRuntime}
    StorageRow = Vector{Pair{String,Any}}
    torows = values -> StorageRow[Pair{String,Any}["child" => value] for value in values]
    seedrows = SeedRow[(child=ArrowTypesTestManyRegisteredValue{0}(Int16(0)),),]
    nestedseedbytes = arrowtypes_test_bytes((value=seedrows,); file=false)
    nestedfield, _ = arrowtypes_test_core_parts(nestedseedbytes)
    nestedschema = ARROWTYPES_TEST_AC.Schema([nestedfield])
    registered9 = arrowtypes_test_many_registered_values(9)

    nested8 =
        ArrowTypesTestRetainedPartitions(nestedschema, ((value=torows(registered9[1:8]),),))
    nestedbytes = arrowtypes_test_bytes(nested8; file=false)
    @test [
        arrowtypes_test_rowdict(row)["child"].value for
        row in Arrow.Table(nestedbytes).value
    ] == Int16.(1:8)

    for parts in (
        ((value=torows(registered9),),),
        ((value=torows(registered9[1:4]),), (value=torows(registered9[5:9]),)),
    )
        source = ArrowTypesTestRetainedPartitions(nestedschema, parts)
        err = try
            arrowtypes_test_bytes(source; file=false)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin(
            "column child has more than 8 inferred registered writer runtime types",
            sprint(showerror, err),
        )
    end

    @testset "retained registered inference is column-wide across children" begin
        WideSeedRow = @NamedTuple{
            left::ArrowTypesTestManyRegisteredRuntime,
            right::ArrowTypesTestManyRegisteredRuntime,
        }
        seedvalue = ArrowTypesTestManyRegisteredValue{0}(Int16(0))
        wideseed = WideSeedRow[(left=seedvalue, right=seedvalue)]
        wideseedbytes = arrowtypes_test_bytes((value=wideseed,); file=false)
        widefield, _ = arrowtypes_test_core_parts(wideseedbytes)
        wideschema = ARROWTYPES_TEST_AC.Schema([widefield])

        function widerange(range)
            values = ArrowTypesTestManyRegisteredRuntime[]
            for i in range
                T = Core.apply_type(ArrowTypesTestManyRegisteredValue, i)
                push!(values, T(Int16(i)))
            end
            return values
        end
        function widerows(left, right)
            return Vector{Pair{String,Any}}[
                Pair{String,Any}["left" => l, "right" => r] for (l, r) in zip(left, right)
            ]
        end

        acceptedrows = widerows(widerange(1:4), widerange(5:8))
        acceptedsource =
            ArrowTypesTestRetainedPartitions(wideschema, ((value=acceptedrows,),))
        acceptedbytes = arrowtypes_test_bytes(acceptedsource; file=false)
        @test [
            (
                arrowtypes_test_rowdict(row)["left"].value,
                arrowtypes_test_rowdict(row)["right"].value,
            ) for row in Arrow.Table(acceptedbytes).value
        ] == collect(zip(Int16.(1:4), Int16.(5:8)))

        rejectedrows = widerows(widerange(1:5), widerange(6:10))
        rejectedsource =
            ArrowTypesTestRetainedPartitions(wideschema, ((value=rejectedrows,),))
        err = try
            arrowtypes_test_bytes(rejectedsource; file=false)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin(
            "more than 8 inferred registered writer runtime types",
            sprint(showerror, err),
        )
    end

    @testset "fresh abstract storage is column-scoped across partitions" begin
        first4 = arrowtypes_test_same_field_values(1:4)
        second4 = arrowtypes_test_same_field_values(5:8)
        parts8 = Tables.partitioner(((value=first4,), (value=second4,)))
        ARROWTYPES_TEST_SAME_FIELD_LOWER_CALLS[] = 0
        accepted8 = try
            arrowtypes_test_bytes(parts8; file=false)
        catch e
            e
        end
        @test accepted8 isa Vector{UInt8}
        if accepted8 isa Vector{UInt8}
            @test ARROWTYPES_TEST_SAME_FIELD_LOWER_CALLS[] == 8
            field8, batches8 = arrowtypes_test_core_parts(accepted8)
            @test field8.type isa ARROWTYPES_TEST_AC.UnionType
            @test length(field8.children) == 4
            @test all(
                batch -> ARROWTYPES_TEST_AC.typeequal(batch.columns[1].type, field8.type),
                batches8,
            )
            @test map(x -> x.value, Arrow.Table(accepted8).value) == Int16.(1:8)
        end

        fifthroughnine = arrowtypes_test_same_field_values(5:9)
        parts9 = Tables.partitioner(((value=first4,), (value=fifthroughnine,)))
        ARROWTYPES_TEST_SAME_FIELD_LOWER_CALLS[] = 0
        err = try
            arrowtypes_test_bytes(parts9; file=false)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin(
            "column value has more than 8 inferred ArrowTypes storage types",
            sprint(showerror, err),
        )
        @test ARROWTYPES_TEST_SAME_FIELD_LOWER_CALLS[] == 9
    end

    @testset "declared registered Union bypasses the inference cap" begin
        values = ArrowTypesTestDeclaredRegisteredUnion[
            ArrowTypesTestDeclaredRegisteredUnionValue{n}(Int16(n)) for n = 1:9
        ]

        directbytes = arrowtypes_test_bytes((value=values,); file=false)
        directfield, _ = arrowtypes_test_core_parts(directbytes)
        @test directfield.type isa ARROWTYPES_TEST_AC.UnionType
        @test length(directfield.children) == 9
        @test Arrow.Table(directbytes).value == values

        nestedvalues = Vector{ArrowTypesTestDeclaredRegisteredUnion}[values]
        nestedbytes = arrowtypes_test_bytes((value=nestedvalues,); file=false)
        nestedfield, _ = arrowtypes_test_core_parts(nestedbytes)
        @test nestedfield.type isa ARROWTYPES_TEST_AC.ListType
        nestedunionfield = only(nestedfield.children)
        @test nestedunionfield.type isa ARROWTYPES_TEST_AC.UnionType
        @test length(nestedunionfield.children) == 9
        @test Arrow.Table(nestedbytes).value == nestedvalues
    end

    @testset "inference-only storage keeps an honest Null fallback" begin
        allmissing = Union{Missing,ArrowTypesTestAbstractTupleStorage}[missing, missing]
        allmissingbytes = arrowtypes_test_bytes((value=allmissing,); file=false)
        allmissingfield, allmissingbatches = arrowtypes_test_core_parts(allmissingbytes)
        @test allmissingfield.type isa ARROWTYPES_TEST_AC.NullType
        @test allmissingfield.nullable
        @test only(allmissingbatches).nrows == length(allmissing)
        @test Tables.rowcount(Arrow.Table(allmissingbytes)) == length(allmissing)

        empty = ArrowTypesTestAbstractTupleStorage[]
        emptybytes = arrowtypes_test_bytes((value=empty,); file=false)
        emptyfield, emptybatches = arrowtypes_test_core_parts(emptybytes)
        @test emptyfield.type isa ARROWTYPES_TEST_AC.NullType
        @test only(emptybatches).nrows == 0
        @test Tables.rowcount(Arrow.Table(emptybytes)) == 0
    end
end

@testset "abstract Tuple narrowing terminates at its canonical type" begin
    T = Tuple{Union{Missing,Int64}}
    firstpart = T[(Int64(1),)]
    secondpart = T[(missing,)]
    layouts = (
        ("one partition", (value=T[firstpart; secondpart],)),
        ("mixed partition", Tables.partitioner(((value=T[firstpart; secondpart],),))),
        ("split partitions", Tables.partitioner(((value=firstpart,), (value=secondpart,)))),
    )
    for (label, layout) in layouts, file in (false, true)
        @testset "$label ($(file ? "file" : "stream"))" begin
            bytes = arrowtypes_test_bytes(layout; file)
            table = Arrow.Table(bytes)
            @test isequal(collect.(table.value), Any[[Int64(1)], [missing]])
            field, batches = arrowtypes_test_core_parts(bytes)
            @test field.type isa ARROWTYPES_TEST_AC.FixedSizeListType
            @test field.type.listsize == 1
            foreach(
                batch -> ARROWTYPES_TEST_AC.validate_full(field, batch.columns[1]),
                batches,
            )
        end
    end

    StructT = Tuple{Union{Missing,Int64},String}
    structvalues = StructT[(Int64(1), "one"), (missing, "missing")]
    structlayouts = (
        ("one partition", (value=structvalues,)),
        ("mixed partition", Tables.partitioner(((value=structvalues,),))),
        (
            "split partitions",
            Tables.partitioner(((value=structvalues[1:1],), (value=structvalues[2:2],))),
        ),
    )
    for (label, layout) in structlayouts, file in (false, true)
        @testset "heterogeneous $label ($(file ? "file" : "stream"))" begin
            bytes = arrowtypes_test_bytes(layout; file)
            table = Arrow.Table(bytes)
            @test isequal(table.value, structvalues)
            @test eltype(table.value) === StructT
            field, batches = arrowtypes_test_core_parts(bytes)
            @test field.type isa ARROWTYPES_TEST_AC.StructType
            @test occursin("Tuple", something(arrowtypes_test_extension_name(field), ""))
            foreach(
                batch -> ARROWTYPES_TEST_AC.validate_full(field, batch.columns[1]),
                batches,
            )
        end
    end
end

@testset "inference-only tuple storage is planned once per complete column" begin
    Flex = ArrowTypesTestFlexTuple
    values = Flex[Flex(Int16[1]), Flex(Int16[2, 3])]
    layouts = (
        ("one partition", (value=values,)),
        ("empty first", Tables.partitioner(((value=Flex[],), (value=values,)))),
        ("split shapes", Tables.partitioner(((value=values[1:1],), (value=values[2:2],)))),
    )
    for (label, layout) in layouts, file in (false, true)
        @testset "$label ($(file ? "file" : "stream"))" begin
            ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] = 0
            bytes = arrowtypes_test_bytes(layout; file)
            @test ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] == length(values)
            @test Arrow.Table(bytes).value == values
            field, _ = arrowtypes_test_core_parts(bytes)
            @test field.type isa ARROWTYPES_TEST_AC.UnionType
            @test sort([child.type.listsize for child in field.children]) == [1, 2]
            @test arrowtypes_test_extension_name(field) ==
                  String(ARROWTYPES_TEST_FLEX_TUPLE_NAME)
        end
    end

    seedbytes = arrowtypes_test_bytes((value=values,); file=false)
    seedfield, _ = arrowtypes_test_core_parts(seedbytes)
    seedschema = ARROWTYPES_TEST_AC.Schema([seedfield])
    emptysource = ArrowTypesTestRetainedPartitions(seedschema, ((value=Flex[],),))
    for file in (false, true)
        ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] = 0
        emptybytes = arrowtypes_test_bytes(emptysource; file)
        @test ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] == 0
        @test Tables.rowcount(Arrow.Table(emptybytes)) == 0
        emptyfield, emptybatches = arrowtypes_test_core_parts(emptybytes)
        @test arrowtypes_test_field_contract_equal(seedfield, emptyfield)
        ARROWTYPES_TEST_AC.validate_full(emptyfield, only(emptybatches).columns[1])
    end

    Row = @NamedTuple{child::Flex}
    rows = Row[(child=values[1],), (child=values[2],)]
    nestedbytes = arrowtypes_test_bytes((value=rows,); file=false)
    nestedfield, _ = arrowtypes_test_core_parts(nestedbytes)
    nestedschema = ARROWTYPES_TEST_AC.Schema([nestedfield])
    emptyrows = Vector{Pair{String,Any}}[]
    nestedsource = ArrowTypesTestRetainedPartitions(nestedschema, ((value=emptyrows,),))
    for file in (false, true)
        ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] = 0
        rewritten = arrowtypes_test_bytes(nestedsource; file)
        @test ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] == 0
        rewrittenfield, _ = arrowtypes_test_core_parts(rewritten)
        @test arrowtypes_test_field_contract_equal(nestedfield, rewrittenfield)
        @test Tables.rowcount(Arrow.Table(rewritten)) == 0
    end

    categories = Flex[values[1], values[2], values[1]]
    for file in (false, true)
        ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] = 0
        dictbytes = arrowtypes_test_bytes((value=Arrow.DictEncode(categories),); file)
        @test ARROWTYPES_TEST_FLEX_TUPLE_CALLS[] == 2
        @test Arrow.Table(dictbytes).value == categories
        dictfield, dictbatches = arrowtypes_test_core_parts(dictbytes)
        @test dictfield.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test dictfield.type.valuetype isa ARROWTYPES_TEST_AC.UnionType
        @test length(only(dictbatches).columns[1].dictionary) == 2
    end
end

@testset "retained hidden fixed lists avoid placeholder amplification" begin
    emptyunion = function (listsize)
        itemfield, itemdata = ARROWTYPES_TEST_AC.fromjulia("item", Int64[])
        fixedtype = ARROWTYPES_TEST_AC.FixedSizeListType(listsize)
        fixedfield = ARROWTYPES_TEST_AC.Field(
            "huge",
            fixedtype;
            nullable=false,
            children=[itemfield],
        )
        fixeddata = ARROWTYPES_TEST_AC.ArrayData(
            fixedtype,
            0,
            [ARROWTYPES_TEST_AC.BufferSlice()];
            children=[itemdata],
            nullcount=0,
        )
        textfield, textdata = ARROWTYPES_TEST_AC.fromjulia("text", String[])
        uniontype =
            ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.SparseMode, Int8[42, 17])
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            uniontype;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(
                ARROWTYPES_TEST_LOGICAL_LIST_UNION_NAME,
            ),
            children=[fixedfield, textfield],
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            uniontype,
            0,
            [ARROWTYPES_TEST_AC._databuffer(Int8[])];
            children=[fixeddata, textdata],
            nullcount=0,
        )
        return field, data
    end
    readempty = function (listsize)
        field, data = emptyunion(listsize)
        schema = ARROWTYPES_TEST_AC.Schema([field])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 0)
        table = Arrow.Table(Arrow.writestream(schema, [batch]))
        isempty(table.value) || error("empty Union materialized a row")
        return nothing
    end

    readempty(2)
    readempty(1_000)
    readempty(100_000)

    field, data = emptyunion(typemax(Int32))
    arrowtypes_test_retained_rewrites(field, data, ArrowTypesTestLogicalListUnion[])

    directmissing = function (listsize)
        child = ARROWTYPES_TEST_AC.Field("item", ARROWTYPES_TEST_AC.NullType(); nullable=true)
        t = ARROWTYPES_TEST_AC.FixedSizeListType(listsize)
        field = ARROWTYPES_TEST_AC.Field("value", t; nullable=true, children=[child])
        values = Union{Missing,Vector{Missing}}[missing]
        rebuiltfield, rebuiltdata =
            Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        return nothing
    end
    directmissing(2)
    directmissing(1_000)
    directmissing(100_000)

    directmixed = function (listsize)
        child = ARROWTYPES_TEST_AC.Field("item", ARROWTYPES_TEST_AC.NullType(); nullable=true)
        t = ARROWTYPES_TEST_AC.FixedSizeListType(listsize)
        field = ARROWTYPES_TEST_AC.Field("value", t; nullable=true, children=[child])
        T = Union{Missing,Vector{Missing}}
        values = T[fill(missing, listsize)]
        append!(values, fill(missing, 49))
        rebuiltfield, rebuiltdata =
            Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        return nothing
    end
    directmixed(2)
    directmixed(1_000)
    directmixed(20_000)

    freshmixed = function (listsize)
        T = Union{Missing,Vector{Missing}}
        values = T[fill(missing, listsize)]
        append!(values, fill(missing, 49))
        kind = ArrowTypes.FixedSizeListKind{listsize,Missing}()
        rebuiltfield, rebuiltdata = Arrow._arrowtypesfixedlistcolumn(
            "value",
            values,
            kind;
            extension_shape=false,
            context=Arrow._WriterContext("value"),
        )
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        return nothing
    end
    freshmixed(2)
    freshmixed(1_000)
    freshmixed(20_000)

    nullsparsefield = function (listsize)
        child = ARROWTYPES_TEST_AC.Field("item", ARROWTYPES_TEST_AC.NullType(); nullable=true)
        fixed = ARROWTYPES_TEST_AC.Field(
            "fixed",
            ARROWTYPES_TEST_AC.FixedSizeListType(listsize);
            nullable=false,
            children=[child],
        )
        text = ARROWTYPES_TEST_AC.Field(
            "text",
            ARROWTYPES_TEST_AC.Utf8Type(false);
            nullable=false,
        )
        uniontype =
            ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.SparseMode, Int8[42, 17])
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            uniontype;
            nullable=false,
            children=[fixed, text],
        )
        return field
    end
    inactivesparse = function (listsize)
        field = nullsparsefield(listsize)
        routed = Any[Arrow._WriterRoutedUnion(2, "text")]
        rebuiltfield, rebuiltdata =
            Arrow._constructwriterunion(field, routed, Arrow._WriterContext("value"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        schema = ARROWTYPES_TEST_AC.Schema([rebuiltfield])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [rebuiltdata], 1)
        bytes = Arrow.writestream(schema, [batch])
        Arrow.Table(bytes).value == ["text"] ||
            error("inactive sparse fixed-list child changed the visible value")
        return length(bytes)
    end
    inactivebytes = inactivesparse(2)
    @test inactivesparse(1_000) == inactivebytes
    @test inactivesparse(100_000) == inactivebytes

    mixedsparse = function (listsize)
        field = nullsparsefield(listsize)
        routed = Any[Arrow._WriterRoutedUnion(1, fill(missing, listsize))]
        append!(routed, (Arrow._WriterRoutedUnion(2, "text") for _ = 1:49))
        rebuiltfield, rebuiltdata =
            Arrow._constructwriterunion(field, routed, Arrow._WriterContext("value"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        return nothing
    end
    mixedsparse(2)
    mixedsparse(1_000)
    mixedsparse(20_000)
    @test_throws ArgumentError Arrow._constructwriterunion(
        nullsparsefield(1),
        Any[Arrow._WriterRoutedUnion(1, Any[nothing])],
        Arrow._WriterContext("value"),
    )

    generalhidden = function (listsize)
        child = ARROWTYPES_TEST_AC.Field(
            "item",
            ARROWTYPES_TEST_AC.IntType(64, true);
            nullable=false,
        )
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            ARROWTYPES_TEST_AC.FixedSizeListType(listsize);
            nullable=true,
            children=[child],
        )
        values = Any[collect(Int64, 1:listsize)]
        append!(values, fill(missing, 49))
        rebuiltfield, rebuiltdata =
            Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        childdata = only(rebuiltdata.children)
        @test length(childdata) == 50 * listsize
        @test ARROWTYPES_TEST_AC.getvalue(child, childdata, Int64(1)) == 1
        @test !ARROWTYPES_TEST_AC.isvalid_at(rebuiltdata, Int64(2))
        return nothing
    end
    generalhidden(2)
    generalhidden(1_000)
    generalhidden(50_000)

    freshnative = function (listsize)
        T = Union{Missing,Vector{Int64}}
        values = T[collect(Int64, 1:listsize)]
        append!(values, fill(missing, 49))
        kind = ArrowTypes.FixedSizeListKind{listsize,Int64}()
        rebuiltfield, rebuiltdata = Arrow._arrowtypesfixedlistcolumn(
            "value",
            values,
            kind;
            extension_shape=false,
            context=Arrow._WriterContext("value"),
        )
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        @test length(only(rebuiltdata.children)) == 50 * listsize
        @test ARROWTYPES_TEST_AC.getvalue(
            only(rebuiltfield.children),
            only(rebuiltdata.children),
            Int64(1),
        ) == 1
        return nothing
    end
    freshnative(2)
    freshnative(1_000)
    freshnative(50_000)

    freshbool = function (listsize)
        T = Union{Missing,Vector{Bool}}
        values = T[Bool[isodd(index) for index = 1:listsize]]
        append!(values, fill(missing, 49))
        kind = ArrowTypes.FixedSizeListKind{listsize,Bool}()
        rebuiltfield, rebuiltdata = Arrow._arrowtypesfixedlistcolumn(
            "value",
            values,
            kind;
            extension_shape=false,
            context=Arrow._WriterContext("value"),
        )
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        childfield = only(rebuiltfield.children)
        childdata = only(rebuiltdata.children)
        @test length(childdata) == 50 * listsize
        @test ARROWTYPES_TEST_AC.getvalue(childfield, childdata, Int64(1))
        @test !ARROWTYPES_TEST_AC.getvalue(childfield, childdata, Int64(2))
        return nothing
    end
    freshbool(2)
    freshbool(1_000)
    freshbool(100_000)

    registeredhidden = function (listsize, missingrows=49)
        child = ARROWTYPES_TEST_AC.Field(
            "item",
            ARROWTYPES_TEST_AC.IntType(32, true);
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_COUNTED_ID_NAME),
        )
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            ARROWTYPES_TEST_AC.FixedSizeListType(listsize);
            nullable=true,
            children=[child],
        )
        T = Union{Missing,Vector{ArrowTypesTestCountedID}}
        values = T[ArrowTypesTestCountedID.(Int32.(1:listsize))]
        append!(values, fill(missing, missingrows))
        ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] = 0
        rebuiltfield, rebuiltdata =
            Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        @test ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] == listsize
        @test length(only(rebuiltdata.children)) == (missingrows + 1) * listsize
        return nothing
    end
    registeredhidden(2)
    registeredhidden(1_000)
    fixedlistallocchild = joinpath(@__DIR__, "arrowtypes_fixedlist_alloc_child.jl")
    fixedlistallocoutput = read(
        `$(Base.julia_cmd()) --startup-file=no --history-file=no --project=$(Base.active_project()) $fixedlistallocchild`,
        String,
    )
    @test occursin("ArrowTypes fixed-list allocation guard passed", fixedlistallocoutput)

    structallocchild = joinpath(@__DIR__, "arrowtypes_struct_alloc_child.jl")
    structallocoutput = read(
        `$(Base.julia_cmd()) --startup-file=no --history-file=no --project=$(Base.active_project()) $structallocchild`,
        String,
    )
    @test occursin("ArrowTypes Struct allocation guard passed", structallocoutput)

    countedchild = ARROWTYPES_TEST_AC.Field(
        "item",
        ARROWTYPES_TEST_AC.IntType(32, true);
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_COUNTED_ID_NAME),
    )
    allhiddenfield = ARROWTYPES_TEST_AC.Field(
        "value",
        ARROWTYPES_TEST_AC.FixedSizeListType(1_024);
        nullable=true,
        children=[countedchild],
    )
    ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] = 0
    allhiddenvalues = Union{Missing,Vector{ArrowTypesTestCountedID}}[missing]
    allhiddenrebuiltfield, allhiddenrebuiltdata = Arrow._constructpart(
        allhiddenfield,
        allhiddenvalues;
        context=Arrow._WriterContext("value"),
    )
    ARROWTYPES_TEST_AC.validate_full(allhiddenrebuiltfield, allhiddenrebuiltdata)
    @test ARROWTYPES_TEST_COUNTED_ID_LOWER_CALLS[] == 0
    @test length(only(allhiddenrebuiltdata.children)) == 1_024

    runfield = ARROWTYPES_TEST_AC.Field(
        "run_ends",
        ARROWTYPES_TEST_AC.IntType(32, true);
        nullable=false,
    )
    valuefield = ARROWTYPES_TEST_AC.Field(
        "values",
        ARROWTYPES_TEST_AC.IntType(16, true);
        nullable=true,
    )
    reetype = ARROWTYPES_TEST_AC.RunEndEncodedType()
    reefield = ARROWTYPES_TEST_AC.Field(
        "ree",
        reetype;
        nullable=false,
        children=[runfield, valuefield],
    )
    rebuiltreefield, rebuiltreedata =
        Arrow._constructhiddenpart(reefield, 3, Arrow._WriterContext("ree"))
    ARROWTYPES_TEST_AC.validate_full(rebuiltreefield, rebuiltreedata)
    @test ARROWTYPES_TEST_AC.materialize(rebuiltreefield, rebuiltreedata) == Int16[0, 0, 0]

    nullablechildren = [
        ARROWTYPES_TEST_AC.Field("text", ARROWTYPES_TEST_AC.Utf8Type(false); nullable=true),
        ARROWTYPES_TEST_AC.Field(
            "integer",
            ARROWTYPES_TEST_AC.IntType(32, true);
            nullable=true,
        ),
    ]
    for mode in (ARROWTYPES_TEST_AC.DenseMode, ARROWTYPES_TEST_AC.SparseMode)
        uniontype = ARROWTYPES_TEST_AC.UnionType(mode, Int8[3, 8])
        unionfield = ARROWTYPES_TEST_AC.Field(
            "union",
            uniontype;
            nullable=false,
            children=nullablechildren,
        )
        for n in (0, 3)
            rebuiltfield, rebuiltdata =
                Arrow._constructhiddenpart(unionfield, n, Arrow._WriterContext("union"))
            ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
            @test !any(ismissing, ARROWTYPES_TEST_AC.materialize(rebuiltfield, rebuiltdata))
        end
    end

    nullreefield = ARROWTYPES_TEST_AC.Field(
        "null_ree",
        reetype;
        nullable=false,
        children=[
            runfield,
            ARROWTYPES_TEST_AC.Field(
                "values",
                ARROWTYPES_TEST_AC.NullType();
                nullable=true,
            ),
        ],
    )
    integerfield = ARROWTYPES_TEST_AC.Field(
        "integer",
        ARROWTYPES_TEST_AC.IntType(32, true);
        nullable=true,
    )
    for mode in (ARROWTYPES_TEST_AC.DenseMode, ARROWTYPES_TEST_AC.SparseMode)
        uniontype = ARROWTYPES_TEST_AC.UnionType(mode, Int8[3, 8])
        unionfield = ARROWTYPES_TEST_AC.Field(
            "ree_union",
            uniontype;
            nullable=false,
            children=[nullreefield, integerfield],
        )
        @test Arrow._writerhiddenunionchild(unionfield) == 2
        rebuiltfield, rebuiltdata =
            Arrow._constructhiddenpart(unionfield, 3, Arrow._WriterContext("ree_union"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        @test ARROWTYPES_TEST_AC.materialize(rebuiltfield, rebuiltdata) == Int32[0, 0, 0]

        routedfield = ARROWTYPES_TEST_AC.Field(
            "routed_ree_union",
            uniontype;
            nullable=false,
            children=[nullreefield, integerfield],
        )
        routed = Any[Arrow._WriterRoutedUnion(2, Int32(7))]
        routedfield, routeddata = Arrow._constructwriterunion(
            routedfield,
            routed,
            Arrow._WriterContext("routed_ree_union"),
        )
        ARROWTYPES_TEST_AC.validate_full(routedfield, routeddata)
        @test ARROWTYPES_TEST_AC.materialize(routedfield, routeddata) == Int32[7]

        selectednull = Any[Arrow._WriterRoutedUnion(1, missing)]
        @test_throws ARROWTYPES_TEST_AC.ValidationError begin
            selectedfield, selecteddata = Arrow._constructwriterunion(
                ARROWTYPES_TEST_AC.Field(
                    "selected_ree_union",
                    uniontype;
                    nullable=false,
                    children=[nullreefield, integerfield],
                ),
                selectednull,
                Arrow._WriterContext("selected_ree_union"),
            )
            ARROWTYPES_TEST_AC.validate_full(selectedfield, selecteddata)
        end
    end

    expensivefixed = ARROWTYPES_TEST_AC.Field(
        "fixed",
        ARROWTYPES_TEST_AC.FixedSizeListType(100_000);
        nullable=false,
        children=[
            ARROWTYPES_TEST_AC.Field(
                "item",
                ARROWTYPES_TEST_AC.IntType(8, true);
                nullable=false,
            ),
        ],
    )
    cheapinteger = ARROWTYPES_TEST_AC.Field(
        "integer",
        ARROWTYPES_TEST_AC.IntType(8, true);
        nullable=false,
    )
    nullfield =
        ARROWTYPES_TEST_AC.Field("null", ARROWTYPES_TEST_AC.NullType(); nullable=true)
    innerunion = ARROWTYPES_TEST_AC.Field(
        "nested",
        ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[5, 9]);
        nullable=true,
        children=[nullfield, expensivefixed],
    )
    @test Arrow._writerplaceholdercost(innerunion; forcevalid=true) == 100_000
    for mode in (ARROWTYPES_TEST_AC.DenseMode, ARROWTYPES_TEST_AC.SparseMode)
        outerunion = ARROWTYPES_TEST_AC.Field(
            "outer",
            ARROWTYPES_TEST_AC.UnionType(mode, Int8[11, 13]);
            nullable=false,
            children=[innerunion, cheapinteger],
        )
        @test Arrow._writerhiddenunionchild(outerunion) == 2
        rebuiltfield, rebuiltdata =
            Arrow._constructhiddenpart(outerunion, 1, Arrow._WriterContext("outer"))
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
        @test ARROWTYPES_TEST_AC.materialize(rebuiltfield, rebuiltdata) == Int8[0]
    end

    nulluniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[1])
    nullunionfield = ARROWTYPES_TEST_AC.Field(
        "null_union",
        nulluniontype;
        nullable=false,
        children=[
            ARROWTYPES_TEST_AC.Field("null", ARROWTYPES_TEST_AC.NullType(); nullable=true),
        ],
    )
    err = try
        Arrow._constructhiddenpart(nullunionfield, 1, Arrow._WriterContext("null_union"))
        nothing
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin("no child that can synthesize", sprint(showerror, err))

    dictionarytype = ARROWTYPES_TEST_AC.DictionaryType(
        ARROWTYPES_TEST_AC.IntType(32, true),
        ARROWTYPES_TEST_AC.NullType(),
        false,
    )
    for nullable in (false, true)
        dictionaryfield =
            ARROWTYPES_TEST_AC.Field("dictionary_null", dictionarytype; nullable)
        rebuiltfield, rebuiltdata = Arrow._constructhiddenpart(
            dictionaryfield,
            2,
            Arrow._WriterContext("dictionary_null");
            forcevalid=(!nullable),
        )
        ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
    end
end

@testset "nested nullable fixed lists preserve Null-only validity" begin
    nullfield =
        ARROWTYPES_TEST_AC.Field("null", ARROWTYPES_TEST_AC.NullType(); nullable=true)
    innertype = ARROWTYPES_TEST_AC.FixedSizeListType(2)
    innerfield =
        ARROWTYPES_TEST_AC.Field("inner", innertype; nullable=true, children=[nullfield])
    outertype = ARROWTYPES_TEST_AC.FixedSizeListType(2)
    outerfield =
        ARROWTYPES_TEST_AC.Field("value", outertype; nullable=true, children=[innerfield])

    for innervalid in (Bool[true, true], Bool[true, false])
        nulldata = ARROWTYPES_TEST_AC.ArrayData(
            nullfield.type,
            4,
            ARROWTYPES_TEST_AC.BufferSlice[];
            nullcount=4,
        )
        innerdata = ARROWTYPES_TEST_AC.ArrayData(
            innertype,
            2,
            [ARROWTYPES_TEST_AC._bitmapbuffer(innervalid)];
            children=[nulldata],
            nullcount=count(!, innervalid),
        )
        outerdata = ARROWTYPES_TEST_AC.ArrayData(
            outertype,
            1,
            [ARROWTYPES_TEST_AC.BufferSlice()];
            children=[innerdata],
            nullcount=0,
        )
        expectedrow =
            Any[innervalid[i] ? fill(missing, 2) : missing for i in eachindex(innervalid)]
        expected = Any[expectedrow]
        schema = ARROWTYPES_TEST_AC.Schema([outerfield])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [outerdata], 1)
        for inputfile in (false, true)
            inputbytes =
                inputfile ? Arrow.writefile(schema, [batch]) :
                Arrow.writestream(schema, [batch])
            @test isequal(Arrow.Table(inputbytes).value, expected)
            for sourcekind in (:table, :stream), outputfile in (false, true)
                source =
                    sourcekind === :table ? Arrow.Table(inputbytes) :
                    Arrow.Stream(inputbytes)
                outputbytes = arrowtypes_test_bytes(source; file=outputfile)
                @test isequal(Arrow.Table(outputbytes).value, expected)
                outputfield, outputbatches = arrowtypes_test_core_parts(outputbytes)
                @test arrowtypes_test_field_contract_equal(outputfield, outerfield)
                outputinner = only(only(outputbatches).columns[1].children)
                @test [
                    ARROWTYPES_TEST_AC.isvalid_at(outputinner, Int64(i)) for
                    i in eachindex(innervalid)
                ] == innervalid
            end
        end
    end

    values = Any[Any[fill(missing, 2), missing], missing, Any[missing, fill(missing, 2)]]
    rebuiltfield, rebuiltdata =
        Arrow._constructpart(outerfield, values; context=Arrow._WriterContext("value"))
    ARROWTYPES_TEST_AC.validate_full(rebuiltfield, rebuiltdata)
    @test isequal(ARROWTYPES_TEST_AC.materialize(rebuiltfield, rebuiltdata), values)
    @test [
        ARROWTYPES_TEST_AC.isvalid_at(rebuiltdata, Int64(i)) for i in eachindex(values)
    ] == Bool[true, false, true]
    rebuiltinner = only(rebuiltdata.children)
    @test [ARROWTYPES_TEST_AC.isvalid_at(rebuiltinner, Int64(i)) for i = 1:6] == Bool[true, false, false, false, false, true]

    nonnullableinner =
        ARROWTYPES_TEST_AC.Field("inner", innertype; nullable=false, children=[nullfield])
    invalidfield = ARROWTYPES_TEST_AC.Field(
        "value",
        outertype;
        nullable=true,
        children=[nonnullableinner],
    )
    @test_throws ArgumentError Arrow._constructpart(
        invalidfield,
        Any[Any[fill(missing, 2), missing]];
        context=Arrow._WriterContext("value"),
    )

    advisorynull =
        ARROWTYPES_TEST_AC.Field("null", ARROWTYPES_TEST_AC.NullType(); nullable=false)
    advisoryinner =
        ARROWTYPES_TEST_AC.Field("inner", innertype; nullable=true, children=[advisorynull])
    advisoryouter = ARROWTYPES_TEST_AC.Field(
        "value",
        outertype;
        nullable=true,
        children=[advisoryinner],
    )
    advisoryvalues = Any[Any[fill(missing, 2), missing]]
    advisoryfield, advisorydata = Arrow._constructpart(
        advisoryouter,
        advisoryvalues;
        context=Arrow._WriterContext("value"),
    )
    @test isequal(
        ARROWTYPES_TEST_AC.materialize(advisoryfield, advisorydata),
        advisoryvalues,
    )
end

@testset "retained registered descriptor aliases" begin
    @testset "LargeUtf8" begin
        values = ["one", "arrows"]
        bytes = collect(codeunits(join(values)))
        t = ARROWTYPES_TEST_AC.Utf8Type(true)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_LABEL_NAME),
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            2,
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(Int64[0, 3, 9]),
                ARROWTYPES_TEST_AC._databuffer(bytes),
            ];
            nullcount=0,
        )
        arrowtypes_test_retained_rewrites(field, data, ArrowTypesTestLabel.(values))
    end

    @testset "Timestamp(SECOND)" begin
        t = ARROWTYPES_TEST_AC.TimestampType(ARROWTYPES_TEST_AC.SECOND, nothing)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(
                ARROWTYPES_TEST_SECOND_TIMESTAMP_NAME,
            ),
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            2,
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(Int64[1, 61]),
            ];
            nullcount=0,
        )
        expected = ArrowTypesTestSecondTimestamp[
            ArrowTypesTestSecondTimestamp(DateTime(1970, 1, 1, 0, 0, 1)),
            ArrowTypesTestSecondTimestamp(DateTime(1970, 1, 1, 0, 1, 1)),
        ]
        arrowtypes_test_retained_rewrites(field, data, expected)
    end
end

@testset "retained binary storage aliases" begin
    @testset "LargeBinary" begin
        t = ARROWTYPES_TEST_AC.BinaryType(true)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_BYTES_ALIAS_NAME),
        )
        values = [UInt8[0x01, 0x02, 0x03], UInt8[0xff]]
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            2,
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(Int64[0, 3, 4]),
                ARROWTYPES_TEST_AC._databuffer(vcat(values...)),
            ];
            nullcount=0,
        )
        arrowtypes_test_retained_rewrites(field, data, ArrowTypesTestBytesAlias.(values))
    end

    @testset "BinaryView" begin
        inlineentry = function (payload)
            entry = zeros(UInt8, 16)
            lengthbytes = collect(reinterpret(UInt8, Int32[Int32(length(payload))]))
            copyto!(entry, 1, lengthbytes, 1, 4)
            copyto!(entry, 5, payload, 1, length(payload))
            return entry
        end
        t = ARROWTYPES_TEST_AC.ViewType(false)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_BYTES_ALIAS_NAME),
        )
        values = [UInt8[0x00, 0xff], UInt8[0x01, 0x02, 0x03]]
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            2,
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(vcat(inlineentry.(values)...)),
            ];
            nullcount=0,
        )
        arrowtypes_test_retained_rewrites(field, data, ArrowTypesTestBytesAlias.(values))
    end

    @testset "FixedSizeBinary" begin
        t = ARROWTYPES_TEST_AC.FixedSizeBinaryType(3)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_BYTES_ALIAS_NAME),
        )
        values = [UInt8[0x01, 0x02, 0x03], UInt8[0x04, 0x05, 0x06]]
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            2,
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(vcat(values...)),
            ];
            nullcount=0,
        )
        arrowtypes_test_retained_rewrites(field, data, ArrowTypesTestBytesAlias.(values))
        wrong = ArrowTypesTestBytesAlias[
            ArrowTypesTestBytesAlias(UInt8[0x01, 0x02]),
            ArrowTypesTestBytesAlias(UInt8[0x03, 0x04]),
        ]
        arrowtypes_test_retained_replacement_error(field, data, wrong, "expected 3")
    end

    @testset "opaque aliases reject nullable child storage" begin
        t = ARROWTYPES_TEST_AC.BinaryType(true)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(
                ARROWTYPES_TEST_NULLABLE_BYTES_ALIAS_NAME,
            ),
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            0,
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(Int64[0]),
                ARROWTYPES_TEST_AC._databuffer(UInt8[]),
            ];
            nullcount=0,
        )
        arrowtypes_test_retained_replacement_error(
            field,
            data,
            ArrowTypesTestNullableBytesAlias[],
            "does not match retained field",
        )
    end
end

@testset "retained wide Decimal storage aliases" begin
    for (bits, width) in ((128, 16), (256, 32))
        @testset "Decimal$bits" begin
            t = ARROWTYPES_TEST_AC.DecimalType(bits == 128 ? 30 : 60, 4, bits)
            field = ARROWTYPES_TEST_AC.Field(
                "value",
                t;
                nullable=false,
                metadata=arrowtypes_test_extension_metadata(
                    ARROWTYPES_TEST_BYTES_ALIAS_NAME,
                ),
            )
            values = [UInt8.(1:width), reverse(UInt8.(1:width))]
            data = ARROWTYPES_TEST_AC.ArrayData(
                t,
                2,
                [
                    ARROWTYPES_TEST_AC.BufferSlice(),
                    ARROWTYPES_TEST_AC._databuffer(vcat(values...)),
                ];
                nullcount=0,
            )
            expected = ArrowTypesTestBytesAlias.(values)
            arrowtypes_test_retained_rewrites(field, data, expected)
            wrong = ArrowTypesTestBytesAlias[
                ArrowTypesTestBytesAlias(zeros(UInt8, width - 1)),
                ArrowTypesTestBytesAlias(zeros(UInt8, width - 1)),
            ]
            arrowtypes_test_retained_replacement_error(
                field,
                data,
                wrong,
                "expected $width",
            )
        end
    end
end

@testset "retained sequence storage aliases" begin
    childfield, childdata = ARROWTYPES_TEST_AC.fromjulia("item", Int32[1, 2, 3, 4])
    metadata = arrowtypes_test_extension_metadata(ARROWTYPES_TEST_INT_LIST_ALIAS_NAME)
    cases = (
        (
            "LargeList",
            ARROWTYPES_TEST_AC.ListType(true),
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(Int64[0, 2, 4]),
            ],
            [Int32[1, 2], Int32[3, 4]],
        ),
        (
            "ListView",
            ARROWTYPES_TEST_AC.ListViewType(false),
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(Int32[1, 0]),
                ARROWTYPES_TEST_AC._databuffer(Int32[2, 1]),
            ],
            [Int32[2, 3], Int32[1]],
        ),
        (
            "FixedSizeList",
            ARROWTYPES_TEST_AC.FixedSizeListType(2),
            [ARROWTYPES_TEST_AC.BufferSlice()],
            [Int32[1, 2], Int32[3, 4]],
        ),
    )
    for (label, t, buffers, values) in cases
        @testset "$label" begin
            field = ARROWTYPES_TEST_AC.Field(
                "value",
                t;
                nullable=false,
                metadata,
                children=[childfield],
            )
            data = ARROWTYPES_TEST_AC.ArrayData(
                t,
                2,
                buffers;
                children=[childdata],
                nullcount=0,
            )
            arrowtypes_test_retained_rewrites(
                field,
                data,
                ArrowTypesTestIntListAlias.(values),
            )
            if t isa ARROWTYPES_TEST_AC.FixedSizeListType
                wrong = ArrowTypesTestIntListAlias[
                    ArrowTypesTestIntListAlias(Int32[1]),
                    ArrowTypesTestIntListAlias(Int32[2, 3, 4]),
                ]
                arrowtypes_test_retained_replacement_error(field, data, wrong, "expected 2")
            end
        end
    end

    emptychildfield, emptychilddata = ARROWTYPES_TEST_AC.fromjulia("item", Int32[])
    fixedtype = ARROWTYPES_TEST_AC.FixedSizeListType(2)
    fixedfield = ARROWTYPES_TEST_AC.Field(
        "value",
        fixedtype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_FIXED_LIST3_NAME),
        children=[emptychildfield],
    )
    fixeddata = ARROWTYPES_TEST_AC.ArrayData(
        fixedtype,
        0,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[emptychilddata],
        nullcount=0,
    )
    arrowtypes_test_retained_replacement_error(
        fixedfield,
        fixeddata,
        ArrowTypesTestFixedList3[],
        "does not match retained field",
    )
end

@testset "retained Interval storage aliases" begin
    cases = (
        (
            "DAY_TIME",
            ARROWTYPES_TEST_AC.DAY_TIME,
            ArrowTypesTestDayTimeStorage[
                (days=Int32(1), millis=Int32(2)),
                (days=Int32(-3), millis=Int32(4)),
            ],
            ArrowTypesTestDayTimeInterval,
            ARROWTYPES_TEST_DAY_TIME_INTERVAL_NAME,
        ),
        (
            "MONTH_DAY_NANO",
            ARROWTYPES_TEST_AC.MONTH_DAY_NANO,
            ArrowTypesTestMonthDayNanoStorage[
                (months=Int32(1), days=Int32(2), nanos=Int64(3)),
                (months=Int32(-4), days=Int32(5), nanos=Int64(-6)),
            ],
            ArrowTypesTestMonthDayNanoInterval,
            ARROWTYPES_TEST_MONTH_DAY_NANO_INTERVAL_NAME,
        ),
    )
    for (label, unit, values, Wrapper, name) in cases
        @testset "$label" begin
            t = ARROWTYPES_TEST_AC.IntervalType(unit)
            field = ARROWTYPES_TEST_AC.Field(
                "value",
                t;
                nullable=false,
                metadata=arrowtypes_test_extension_metadata(name),
            )
            data = ARROWTYPES_TEST_AC.ArrayData(
                t,
                2,
                [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(values)];
                nullcount=0,
            )
            arrowtypes_test_retained_rewrites(field, data, Wrapper.(values))
        end
    end
end

@testset "retained temporal aliases convert across descriptors exactly" begin
    @testset "Duration SECOND from Millisecond" begin
        t = ARROWTYPES_TEST_AC.DurationType(ARROWTYPES_TEST_AC.SECOND)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(
                ARROWTYPES_TEST_MILLISECOND_DURATION_NAME,
            ),
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            1,
            [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int64[2])];
            nullcount=0,
        )
        expected = [ArrowTypesTestMillisecondDuration(Dates.Millisecond(2_000))]
        arrowtypes_test_retained_rewrites(field, data, expected)

        schema = ARROWTYPES_TEST_AC.Schema([field])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 1)
        source = Arrow.Table(Arrow.writestream(schema, [batch]))
        getfield(source, :columns)[1] =
            [ArrowTypesTestMillisecondDuration(Dates.Millisecond(1_500))]
        for file in (false, true)
            err = try
                arrowtypes_test_bytes(source; file)
                nothing
            catch e
                e
            end
            @test err isa ArgumentError
            @test occursin("cannot be represented exactly", sprint(showerror, err))
        end
    end

    @testset "Date32 from Timestamp(MILLISECOND)" begin
        day = Date(2024, 1, 2)
        t = ARROWTYPES_TEST_AC.DateType(ARROWTYPES_TEST_AC.DAY)
        field = ARROWTYPES_TEST_AC.Field(
            "value",
            t;
            nullable=false,
            metadata=arrowtypes_test_extension_metadata(
                ARROWTYPES_TEST_DATE_LIKE_ALIAS_NAME,
            ),
        )
        data = ARROWTYPES_TEST_AC.ArrayData(
            t,
            1,
            [
                ARROWTYPES_TEST_AC.BufferSlice(),
                ARROWTYPES_TEST_AC._databuffer(Int32[Dates.value(day - Date(1970, 1, 1))]),
            ];
            nullcount=0,
        )
        expected = [ArrowTypesTestDateLikeAlias(DateTime(day))]
        arrowtypes_test_retained_rewrites(field, data, expected)

        schema = ARROWTYPES_TEST_AC.Schema([field])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 1)
        source = Arrow.Table(Arrow.writestream(schema, [batch]))
        getfield(source, :columns)[1] =
            [ArrowTypesTestDateLikeAlias(DateTime(2024, 1, 2, 12))]
        for file in (false, true)
            err = try
                arrowtypes_test_bytes(source; file)
                nothing
            catch e
                e
            end
            @test err isa ArgumentError
            @test occursin("cannot be represented exactly", sprint(showerror, err))
        end
    end
end

@testset "retained Map rejects keyssorted relabeling" begin
    keyfield, keydata = ARROWTYPES_TEST_AC.fromjulia("key", Int32[1, 3])
    valuefield, valuedata = ARROWTYPES_TEST_AC.fromjulia("value", Int32[2, 4])
    structtype = ARROWTYPES_TEST_AC.StructType()
    entriesfield = ARROWTYPES_TEST_AC.Field(
        "entries",
        structtype;
        nullable=false,
        children=[keyfield, valuefield],
    )
    entriesdata = ARROWTYPES_TEST_AC.ArrayData(
        structtype,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[keydata, valuedata],
        nullcount=0,
    )
    maptype = ARROWTYPES_TEST_AC.MapType(true)
    field = ARROWTYPES_TEST_AC.Field(
        "value",
        maptype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_SORTED_MAP_NAME),
        children=[entriesfield],
    )
    data = ARROWTYPES_TEST_AC.ArrayData(
        maptype,
        1,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 2])];
        children=[entriesdata],
        nullcount=0,
    )
    schema = ARROWTYPES_TEST_AC.Schema([field])
    batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 1)
    expected = [ArrowTypesTestSortedMap(Dict(Int32(1) => Int32(2), Int32(3) => Int32(4)))]
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(schema, [batch]) :
            Arrow.writestream(schema, [batch])
        @test Arrow.Table(inputbytes).value == expected
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            err = try
                arrowtypes_test_bytes(source; file=outputfile)
                nothing
            catch e
                e
            end
            @test err isa ArgumentError
            @test occursin("does not match retained field", sprint(showerror, err))
        end
    end

    ordinaryfield =
        ARROWTYPES_TEST_AC.Field("value", maptype; nullable=false, children=[entriesfield])
    unsorted = [Pair{Any,Any}[Int32(3) => Int32(4), Int32(1) => Int32(2)]]
    arrowtypes_test_retained_replacement_error(
        ordinaryfield,
        data,
        unsorted,
        "not sorted by its physical key storage",
    )

    emptykeyfield, emptykeydata = ARROWTYPES_TEST_AC.fromjulia("key", Int32[])
    emptyvaluefield, emptyvaluedata = ARROWTYPES_TEST_AC.fromjulia("value", Int32[])
    emptyentriesfield = ARROWTYPES_TEST_AC.Field(
        "entries",
        structtype;
        nullable=false,
        children=[emptykeyfield, emptyvaluefield],
    )
    emptyentriesdata = ARROWTYPES_TEST_AC.ArrayData(
        structtype,
        0,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[emptykeydata, emptyvaluedata],
        nullcount=0,
    )
    emptymapfield = ARROWTYPES_TEST_AC.Field(
        "value",
        maptype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_SORTED_MAP_NAME),
        children=[emptyentriesfield],
    )
    emptymapdata = ARROWTYPES_TEST_AC.ArrayData(
        maptype,
        0,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0])];
        children=[emptyentriesdata],
        nullcount=0,
    )
    arrowtypes_test_retained_replacement_error(
        emptymapfield,
        emptymapdata,
        ArrowTypesTestSortedMap[],
        "does not match retained field",
    )
end

@testset "encoding identity uses physical ArrowTypes values" begin
    parityvalues = ArrowTypesTestParityID.(Int64[1, 3, 2, 4])
    layouts = (
        ("one partition", (value=Arrow.DictEncode(parityvalues),)),
        (
            "split partitions",
            Tables.partitioner((
                (value=Arrow.DictEncode(parityvalues[1:2]),),
                (value=Arrow.DictEncode(parityvalues[3:4]),),
            )),
        ),
    )
    for (label, layout) in layouts, file in (false, true)
        @testset "dictionary $label ($(file ? "file" : "stream"))" begin
            bytes = arrowtypes_test_bytes(layout; file)
            table = Arrow.Table(bytes)
            @test getproperty.(table.value, :value) == getproperty.(parityvalues, :value)
            field, batches = arrowtypes_test_core_parts(bytes)
            dictionary = first(batches).columns[1].dictionary
            @test length(dictionary) == 4
            valuefield = ARROWTYPES_TEST_AC.dictvaluefield(field, field.type)
            @test ARROWTYPES_TEST_AC.materialize(valuefield, dictionary) ==
                  Int64[1, 3, 2, 4]
        end
    end

    mutablevalues = ArrowTypesTestMutableID.(Int64[1, 2, 1])
    mutablelayouts = (
        (value=Arrow.DictEncode(mutablevalues),),
        Tables.partitioner((
            (value=Arrow.DictEncode(mutablevalues[1:2]),),
            (value=Arrow.DictEncode(mutablevalues[3:3]),),
        )),
    )
    physicalpools = function (bytes)
        field, batches = arrowtypes_test_core_parts(bytes)
        valuefield = ARROWTYPES_TEST_AC.dictvaluefield(field, field.type)
        return [
            ARROWTYPES_TEST_AC.materialize(
                valuefield,
                batch.columns[1].dictionary::ARROWTYPES_TEST_AC.ArrayData,
            ) for batch in batches
        ]
    end
    for layout in mutablelayouts, file in (false, true)
        bytes = arrowtypes_test_bytes(layout; file)
        @test all(==([1, 2]), physicalpools(bytes))
        @test getproperty.(Arrow.Table(bytes).value, :value) == Int64[1, 2, 1]
    end
    splitbytes = arrowtypes_test_bytes(last(mutablelayouts); file=false)
    for sourcekind in (:table, :stream), outputfile in (false, true)
        rewritten = splitbytes
        for _ = 1:3
            source =
                sourcekind === :table ? Arrow.Table(rewritten) : Arrow.Stream(rewritten)
            rewritten = arrowtypes_test_bytes(source; file=outputfile)
            @test all(==([1, 2]), physicalpools(rewritten))
            @test getproperty.(Arrow.Table(rewritten).value, :value) == Int64[1, 2, 1]
        end
    end

    # The first retained pool is part of the encoding contract. Preserve its
    # positions even when two categories have identical physical storage.
    duplicatevaluefield, duplicatepool =
        ARROWTYPES_TEST_AC.fromjulia("value", Int64[1, 1, 2])
    duplicatedicttype = ARROWTYPES_TEST_AC.DictionaryType(
        ARROWTYPES_TEST_AC.IntType(32, true),
        duplicatevaluefield.type,
        false,
    )
    duplicatedictfield = ARROWTYPES_TEST_AC.Field(
        "value",
        duplicatedicttype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_MUTABLE_ID_NAME),
    )
    duplicatedictdata = ARROWTYPES_TEST_AC.ArrayData(
        duplicatedicttype,
        3,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 1, 2])];
        dictionary=duplicatepool,
        nullcount=0,
    )
    duplicateschema = ARROWTYPES_TEST_AC.Schema([duplicatedictfield])
    duplicatebatch = ARROWTYPES_TEST_AC.RecordBatch(duplicateschema, [duplicatedictdata], 3)
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(duplicateschema, [duplicatebatch]) :
            Arrow.writestream(duplicateschema, [duplicatebatch])
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            @test getproperty.(Arrow.Table(outputbytes).value, :value) == Int64[1, 1, 2]
            outputfield, outputbatches = arrowtypes_test_core_parts(outputbytes)
            outputpool = only(outputbatches).columns[1].dictionary
            @test outputpool !== nothing
            outputvaluefield =
                ARROWTYPES_TEST_AC.dictvaluefield(outputfield, outputfield.type)
            @test ARROWTYPES_TEST_AC.materialize(outputvaluefield, outputpool) ==
                  Int64[1, 1, 2]
        end
    end

    runfield, runenddata = ARROWTYPES_TEST_AC.fromjulia("run_ends", Int32[1, 2])
    _, runvaluedata = ARROWTYPES_TEST_AC.fromjulia("values", Int64[1, 3])
    runvaluefield = arrowtypes_test_marked_id_field("values")
    runvaluefield = ARROWTYPES_TEST_AC.Field(
        runvaluefield.name,
        runvaluefield.type;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_PARITY_ID_NAME),
    )
    reetype = ARROWTYPES_TEST_AC.RunEndEncodedType()
    reefield = ARROWTYPES_TEST_AC.Field(
        "value",
        reetype;
        nullable=false,
        children=[runfield, runvaluefield],
    )
    reedata = ARROWTYPES_TEST_AC.ArrayData(
        reetype,
        2,
        ARROWTYPES_TEST_AC.BufferSlice[];
        children=[runenddata, runvaluedata],
        nullcount=0,
    )
    reeschema = ARROWTYPES_TEST_AC.Schema([reefield])
    reebatch = ARROWTYPES_TEST_AC.RecordBatch(reeschema, [reedata], 2)
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(reeschema, [reebatch]) :
            Arrow.writestream(reeschema, [reebatch])
        @test getproperty.(Arrow.Table(inputbytes).value, :value) == Int64[1, 3]
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            @test getproperty.(Arrow.Table(outputbytes).value, :value) == Int64[1, 3]
            outputfield, outputbatches = arrowtypes_test_core_parts(outputbytes)
            outputvalues = only(outputbatches).columns[1].children[2]
            @test ARROWTYPES_TEST_AC.materialize(outputfield.children[2], outputvalues) ==
                  Int64[1, 3]
        end
    end

    mutablerunfield, mutablerunenddata = ARROWTYPES_TEST_AC.fromjulia("run_ends", Int32[4])
    _, mutablerunvaluedata = ARROWTYPES_TEST_AC.fromjulia("values", Int64[1])
    mutablerunvaluefield = ARROWTYPES_TEST_AC.Field(
        "values",
        ARROWTYPES_TEST_AC.IntType(64, true);
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_MUTABLE_ID_NAME),
    )
    mutablereefield = ARROWTYPES_TEST_AC.Field(
        "value",
        reetype;
        nullable=false,
        children=[mutablerunfield, mutablerunvaluefield],
    )
    mutablereedata = ARROWTYPES_TEST_AC.ArrayData(
        reetype,
        4,
        ARROWTYPES_TEST_AC.BufferSlice[];
        children=[mutablerunenddata, mutablerunvaluedata],
        nullcount=0,
    )
    mutablereeschema = ARROWTYPES_TEST_AC.Schema([mutablereefield])
    mutablereebatch = ARROWTYPES_TEST_AC.RecordBatch(mutablereeschema, [mutablereedata], 4)
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(mutablereeschema, [mutablereebatch]) :
            Arrow.writestream(mutablereeschema, [mutablereebatch])
        @test getproperty.(Arrow.Table(inputbytes).value, :value) == fill(Int64(1), 4)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            outputfield, outputbatches = arrowtypes_test_core_parts(outputbytes)
            outputdata = only(outputbatches).columns[1]
            @test outputdata.children[2].len == 1
            @test ARROWTYPES_TEST_AC.materialize(
                outputfield.children[1],
                outputdata.children[1],
            ) == Int32[4]
            @test ARROWTYPES_TEST_AC.materialize(
                outputfield.children[2],
                outputdata.children[2],
            ) == Int64[1]
        end
    end

    reversekeyfield = ARROWTYPES_TEST_AC.Field(
        "key",
        ARROWTYPES_TEST_AC.IntType(64, true);
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_REVERSE_ID_NAME),
    )
    _, reversekeydata = ARROWTYPES_TEST_AC.fromjulia("key", Int64[1, 2])
    reversevaluefield, reversevaluedata =
        ARROWTYPES_TEST_AC.fromjulia("value", Int32[10, 20])
    reverseentries = ARROWTYPES_TEST_AC.Field(
        "entries",
        ARROWTYPES_TEST_AC.StructType();
        nullable=false,
        children=[reversekeyfield, reversevaluefield],
    )
    reverseentriesdata = ARROWTYPES_TEST_AC.ArrayData(
        reverseentries.type,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[reversekeydata, reversevaluedata],
        nullcount=0,
    )
    reversemaptype = ARROWTYPES_TEST_AC.MapType(true)
    reversemapfield = ARROWTYPES_TEST_AC.Field(
        "value",
        reversemaptype;
        nullable=false,
        children=[reverseentries],
    )
    reversemapdata = ARROWTYPES_TEST_AC.ArrayData(
        reversemaptype,
        1,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 2])];
        children=[reverseentriesdata],
        nullcount=0,
    )
    reversemapschema = ARROWTYPES_TEST_AC.Schema([reversemapfield])
    reversemapbatch = ARROWTYPES_TEST_AC.RecordBatch(reversemapschema, [reversemapdata], 1)
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(reversemapschema, [reversemapbatch]) :
            Arrow.writestream(reversemapschema, [reversemapbatch])
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            outputtable = Arrow.Table(outputbytes)
            @test getproperty.(first.(only(outputtable.value)), :value) == Int64[1, 2]
        end

        replacement = Arrow.Table(inputbytes)
        getfield(replacement, :columns)[1] = Any[Pair{Any,Any}[
            ArrowTypesTestReverseID(2) => Int32(20),
            ArrowTypesTestReverseID(1) => Int32(10),
        ],]
        for outputfile in (false, true)
            err = try
                arrowtypes_test_bytes(replacement; file=outputfile)
                nothing
            catch exception
                exception
            end
            @test err isa ArgumentError
            @test occursin("physical key storage", sprint(showerror, err))
        end
    end

    collisionrow = Dict(
        ArrowTypesTestCollidingMapKey(1) => Int32(10),
        ArrowTypesTestCollidingMapKey(2) => Int32(20),
    )
    for outputfile in (false, true)
        err = try
            arrowtypes_test_bytes((value=[collisionrow],); file=outputfile)
            nothing
        catch exception
            exception
        end
        @test err isa ArgumentError
        @test occursin("duplicate physical Map key storage", sprint(showerror, err))
    end

    collisionkeyfield = ARROWTYPES_TEST_AC.Field(
        "key",
        ARROWTYPES_TEST_AC.IntType(64, true);
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_COLLIDING_MAP_KEY_NAME),
    )
    collisionentries = ARROWTYPES_TEST_AC.Field(
        "entries",
        ARROWTYPES_TEST_AC.StructType();
        nullable=false,
        children=[collisionkeyfield, reversevaluefield],
    )
    collisionentriesdata = ARROWTYPES_TEST_AC.ArrayData(
        collisionentries.type,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[reversekeydata, reversevaluedata],
        nullcount=0,
    )
    collisionmaptype = ARROWTYPES_TEST_AC.MapType(false)
    collisionmapfield = ARROWTYPES_TEST_AC.Field(
        "value",
        collisionmaptype;
        nullable=false,
        children=[collisionentries],
    )
    collisionmapdata = ARROWTYPES_TEST_AC.ArrayData(
        collisionmaptype,
        1,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 2])];
        children=[collisionentriesdata],
        nullcount=0,
    )
    collisionmapschema = ARROWTYPES_TEST_AC.Schema([collisionmapfield])
    collisionmapbatch =
        ARROWTYPES_TEST_AC.RecordBatch(collisionmapschema, [collisionmapdata], 1)
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(collisionmapschema, [collisionmapbatch]) :
            Arrow.writestream(collisionmapschema, [collisionmapbatch])
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            err = try
                arrowtypes_test_bytes(source; file=outputfile)
                nothing
            catch exception
                exception
            end
            @test err isa ArgumentError
            @test occursin("duplicate physical Map key storage", sprint(showerror, err))
        end
    end
end

@testset "empty registered columns keep schema evidence" begin
    t = ARROWTYPES_TEST_AC.IntType(32, true)
    emptyfield = ARROWTYPES_TEST_AC.Field(
        "value",
        t;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_WRONG_STORAGE_NAME),
    )
    emptydata = ARROWTYPES_TEST_AC.ArrayData(
        t,
        0,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[])];
        nullcount=0,
    )
    arrowtypes_test_retained_replacement_error(
        emptyfield,
        emptydata,
        ArrowTypesTestWrongStorage[],
        "does not match retained field",
    )

    nullablefield = ARROWTYPES_TEST_AC.Field(
        "value",
        t;
        nullable=true,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_WRONG_STORAGE_NAME),
    )
    nullabledata = ARROWTYPES_TEST_AC.ArrayData(
        t,
        1,
        [
            ARROWTYPES_TEST_AC._bitmapbuffer(Bool[false]),
            ARROWTYPES_TEST_AC._databuffer(Int32[0]),
        ];
        nullcount=1,
    )
    logicalmissing = Union{Missing,ArrowTypesTestWrongStorage}[missing]
    arrowtypes_test_retained_replacement_error(
        nullablefield,
        nullabledata,
        logicalmissing,
        "does not match retained field",
    )

    schema = ARROWTYPES_TEST_AC.Schema([nullablefield])
    batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [nullabledata], 1)
    for inputfile in (false, true),
        sourcekind in (:table, :stream),
        outputfile in (false, true)

        inputbytes =
            inputfile ? Arrow.writefile(schema, [batch]) :
            Arrow.writestream(schema, [batch])
        source = if sourcekind === :table
            table = Arrow.Table(inputbytes)
            getfield(table, :columns)[1] = Missing[missing]
            table
        else
            parts = collect(Arrow.Stream(inputbytes))
            getfield(only(parts), :columns)[1] = Missing[missing]
            ArrowTypesTestRetainedPartitions(schema, Tuple(parts))
        end
        outputbytes = arrowtypes_test_bytes(source; file=outputfile)
        @test isequal(Arrow.Table(outputbytes).value, Missing[missing])
        outputfield, _ = arrowtypes_test_core_parts(outputbytes)
        @test arrowtypes_test_field_contract_equal(outputfield, nullablefield)
    end
end

@testset "abstract registered declarations distinguish schema evidence" begin
    t = ARROWTYPES_TEST_AC.IntType(32, true)
    data = ARROWTYPES_TEST_AC.ArrayData(
        t,
        0,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[])];
        nullcount=0,
    )

    explicitfield = ARROWTYPES_TEST_AC.Field(
        "value",
        t;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(
            ARROWTYPES_TEST_ABSTRACT_STRING_STORAGE_NAME,
        ),
    )
    arrowtypes_test_retained_replacement_error(
        explicitfield,
        data,
        ArrowTypesTestAbstractStringStorage[],
        "does not match retained field",
    )

    identityfield = ARROWTYPES_TEST_AC.Field(
        "value",
        t;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(
            ARROWTYPES_TEST_ABSTRACT_IDENTITY_STORAGE_NAME,
        ),
    )
    arrowtypes_test_retained_rewrites(
        identityfield,
        data,
        ArrowTypesTestAbstractIdentityStorage[],
    )
end

@testset "malformed JuliaType results fail with field context" begin
    t = ARROWTYPES_TEST_AC.IntType(32, true)
    field = ARROWTYPES_TEST_AC.Field(
        "value",
        t;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(
            ARROWTYPES_TEST_MALFORMED_JULIATYPE_NAME,
        ),
    )
    data = ARROWTYPES_TEST_AC.ArrayData(
        t,
        1,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[1])];
        nullcount=0,
    )
    schema = ARROWTYPES_TEST_AC.Schema([field])
    batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 1)
    checkerror = function (err)
        @test err isa ArgumentError
        message = sprint(showerror, err)
        @test occursin("ArrowTypes.JuliaType", message)
        @test occursin("MalformedJuliaType", message)
        @test occursin("field value", message)
    end
    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(schema, [batch]) :
            Arrow.writestream(schema, [batch])
        err = try
            Arrow.Table(inputbytes)
            nothing
        catch e
            e
        end
        checkerror(err)
    end

    source = Arrow._table(
        Symbol[:value],
        AbstractVector[Int32[1]],
        schema,
        ARROWTYPES_TEST_AC.OwnerRegion[],
        1,
    )
    for outputfile in (false, true)
        err = try
            arrowtypes_test_bytes(source; file=outputfile)
            nothing
        catch e
            e
        end
        checkerror(err)
    end
end

@testset "invalid toarrow results fail with column context" begin
    checkerror = function (err, column)
        @test err isa ArgumentError
        message = sprint(showerror, err)
        @test occursin("ArrowTypes.toarrow", message)
        @test occursin("ArrowTypesTestBadLower", message)
        @test occursin(column, message)
    end
    for outputfile in (false, true)
        err = try
            arrowtypes_test_bytes(
                (broken=ArrowTypesTestBadLower[ArrowTypesTestBadLower(Int8(1))],);
                file=outputfile,
            )
            nothing
        catch e
            e
        end
        checkerror(err, "broken")
    end

    intfield, intdata = ARROWTYPES_TEST_AC.fromjulia("integer", Int8[1])
    textfield, textdata = ARROWTYPES_TEST_AC.fromjulia("text", String[])
    uniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[42, 17])
    field = ARROWTYPES_TEST_AC.Field(
        "value",
        uniontype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_BAD_LOWER_NAME),
        children=[intfield, textfield],
    )
    data = ARROWTYPES_TEST_AC.ArrayData(
        uniontype,
        1,
        [
            ARROWTYPES_TEST_AC._databuffer(Int8[42]),
            ARROWTYPES_TEST_AC._databuffer(Int32[0]),
        ];
        children=[intdata, textdata],
        nullcount=0,
    )
    arrowtypes_test_retained_replacement_error(
        field,
        data,
        ArrowTypesTestBadLower[ArrowTypesTestBadLower(Int8(1))],
        ("ArrowTypes.toarrow", "ArrowTypesTestBadLower", "value"),
    )

    badunionfield = ARROWTYPES_TEST_AC.Field(
        "value",
        uniontype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_BAD_UNION_LOWER_NAME),
        children=[intfield, textfield],
    )
    arrowtypes_test_retained_replacement_error(
        badunionfield,
        data,
        ArrowTypesTestBadUnionLower[ArrowTypesTestBadUnionLower(Int8(1))],
        ("ArrowTypes.toarrow", "ArrowTypesTestBadUnionLower", "value"),
    )

    afield, adata = ARROWTYPES_TEST_AC.fromjulia("a", Int8[1])
    bfield, bdata = ARROWTYPES_TEST_AC.fromjulia("b", Int8[2])
    structtype = ARROWTYPES_TEST_AC.StructType()
    structfield = ARROWTYPES_TEST_AC.Field(
        "value",
        structtype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_BAD_STRUCT_LOWER_NAME),
        children=[afield, bfield],
    )
    structdata = ARROWTYPES_TEST_AC.ArrayData(
        structtype,
        1,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[adata, bdata],
        nullcount=0,
    )
    arrowtypes_test_retained_replacement_error(
        structfield,
        structdata,
        ArrowTypesTestBadStructLower[ArrowTypesTestBadStructLower(Int8(7))],
        ("ArrowTypes.toarrow", "ArrowTypesTestBadStructLower", "value"),
    )
end

@testset "registered sparse Union preserves routing" begin
    intfield, intdata = ARROWTYPES_TEST_AC.fromjulia("integer", Int64[7, 0, 9])
    stringfield, stringdata =
        ARROWTYPES_TEST_AC.fromjulia("text", ["hidden", "two", "hidden"])
    uniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.SparseMode, Int8[42, 17])
    field = ARROWTYPES_TEST_AC.Field(
        "value",
        uniontype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_LOGICAL_UNION_NAME),
        children=[intfield, stringfield],
    )
    data = ARROWTYPES_TEST_AC.ArrayData(
        uniontype,
        3,
        [ARROWTYPES_TEST_AC._databuffer(Int8[42, 17, 42])];
        children=[intdata, stringdata],
        nullcount=0,
    )
    expected = ArrowTypesTestLogicalUnion[
        ArrowTypesTestLogicalUnion(Int64(7)),
        ArrowTypesTestLogicalUnion("two"),
        ArrowTypesTestLogicalUnion(Int64(9)),
    ]
    arrowtypes_test_retained_rewrites(field, data, expected)
end

@testset "nested retained Union preserves noncanonical routing" begin
    stringfield, stringdata = ARROWTYPES_TEST_AC.fromjulia("text", ["two"])
    intfield, intdata = ARROWTYPES_TEST_AC.fromjulia("integer", Int64[1])
    uniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[9, 5])
    unionfield = ARROWTYPES_TEST_AC.Field(
        "child",
        uniontype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_LOGICAL_UNION_NAME),
        children=[stringfield, intfield],
    )
    uniondata = ARROWTYPES_TEST_AC.ArrayData(
        uniontype,
        2,
        [
            ARROWTYPES_TEST_AC._databuffer(Int8[5, 9]),
            ARROWTYPES_TEST_AC._databuffer(Int32[0, 0]),
        ];
        children=[stringdata, intdata],
        nullcount=0,
    )
    structtype = ARROWTYPES_TEST_AC.StructType()
    field = ARROWTYPES_TEST_AC.Field(
        "value",
        structtype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(
            ARROWTYPES_TEST_NESTED_LOGICAL_UNION_NAME,
        ),
        children=[unionfield],
    )
    data = ARROWTYPES_TEST_AC.ArrayData(
        structtype,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[uniondata],
        nullcount=0,
    )
    expected = ArrowTypesTestNestedLogicalUnion[
        ArrowTypesTestNestedLogicalUnion(ArrowTypesTestLogicalUnion(Int64(1))),
        ArrowTypesTestNestedLogicalUnion(ArrowTypesTestLogicalUnion("two")),
    ]
    arrowtypes_test_retained_rewrites(field, data, expected)
end

@testset "transparent REE<Date32> preserves its registered alias" begin
    firstday = Date(2024, 1, 1)
    secondday = Date(2024, 1, 2)
    runfield, rundata = ARROWTYPES_TEST_AC.fromjulia("run_ends", Int32[2, 3])
    valuetype = ARROWTYPES_TEST_AC.DateType(ARROWTYPES_TEST_AC.DAY)
    valuefield = ARROWTYPES_TEST_AC.Field("values", valuetype; nullable=false)
    epoch = Date(1970, 1, 1)
    values = Int32[Dates.value(firstday - epoch), Dates.value(secondday - epoch)]
    valuedata = ARROWTYPES_TEST_AC.ArrayData(
        valuetype,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(values)];
        nullcount=0,
    )
    reetype = ARROWTYPES_TEST_AC.RunEndEncodedType()
    field = ARROWTYPES_TEST_AC.Field(
        "value",
        reetype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_DAY_NAME),
        children=[runfield, valuefield],
    )
    data = ARROWTYPES_TEST_AC.ArrayData(
        reetype,
        3,
        ARROWTYPES_TEST_AC.BufferSlice[];
        children=[rundata, valuedata],
        nullcount=0,
    )
    expected = ArrowTypesTestDay[
        ArrowTypesTestDay(firstday),
        ArrowTypesTestDay(firstday),
        ArrowTypesTestDay(secondday),
    ]
    arrowtypes_test_retained_rewrites(field, data, expected)
end

@testset "nested abstract Union rewrites hidden and visible partitions" begin
    Parent = ArrowTypesTestNestedUnionParent
    Concrete = ArrowTypesTestConcreteUnionWrite
    firstpart = Union{Missing,Parent}[missing, missing]
    secondpart =
        Parent[Parent(Concrete(Int64(1))), Parent(missing), Parent(Concrete("two"))]
    expected = Union{Missing,Parent}[firstpart; secondpart]
    for inputfile in (false, true)
        parts = Tables.partitioner(((value=firstpart,), (value=secondpart,)))
        inputbytes = arrowtypes_test_bytes(parts; file=inputfile)
        inputfield, inputbatches = arrowtypes_test_core_parts(inputbytes)
        @test length(inputbatches) == 2
        @test isequal(Arrow.Table(inputbytes).value, expected)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            ARROWTYPES_TEST_NESTED_UNION_PARENT_CALLS[] = 0
            ARROWTYPES_TEST_CONCRETE_UNION_WRITE_CALLS[] = 0
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            outputtable = Arrow.Table(outputbytes)
            @test isequal(outputtable.value, expected)
            @test ARROWTYPES_TEST_NESTED_UNION_PARENT_CALLS[] == 3
            @test ARROWTYPES_TEST_CONCRETE_UNION_WRITE_CALLS[] == 2
            outputfield, outputbatches = arrowtypes_test_core_parts(outputbytes)
            @test arrowtypes_test_field_contract_equal(inputfield, outputfield)
            @test length(outputbatches) == (sourcekind === :table ? 1 : 2)
        end
    end
end

@testset "logical Null Union distinguishes storage from outer missing" begin
    values = ArrowTypesTestLogicalNullUnion[
        ArrowTypesTestLogicalNullUnion(nothing),
        ArrowTypesTestLogicalNullUnion("value"),
    ]
    nullable = Union{Missing,ArrowTypesTestLogicalNullUnion}[
        missing,
        ArrowTypesTestLogicalNullUnion(nothing),
        ArrowTypesTestLogicalNullUnion("value"),
    ]
    for input in (values, nullable), inputfile in (false, true)
        bytes = arrowtypes_test_bytes((value=input,); file=inputfile)
        @test isequal(Arrow.Table(bytes).value, input)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            @test isequal(
                Arrow.Table(arrowtypes_test_bytes(source; file=outputfile)).value,
                input,
            )
        end
    end

    storage_missing = ArrowTypesTestAmbiguousMissingUnion[
        ArrowTypesTestAmbiguousMissingUnion(missing),
        ArrowTypesTestAmbiguousMissingUnion("stored"),
    ]
    for encoded in (false, true), inputfile in (false, true)
        input = encoded ? Arrow.DictEncode(storage_missing) : storage_missing
        bytes = arrowtypes_test_bytes((value=input,); file=inputfile)
        table = Arrow.Table(bytes)
        @test table.value == storage_missing
        @test eltype(table.value) === ArrowTypesTestAmbiguousMissingUnion
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            rewritten = Arrow.Table(arrowtypes_test_bytes(source; file=outputfile))
            @test rewritten.value == storage_missing
            @test eltype(rewritten.value) === ArrowTypesTestAmbiguousMissingUnion
        end
    end

    ambiguous = Union{Missing,ArrowTypesTestAmbiguousMissingUnion}[
        missing,
        ArrowTypesTestAmbiguousMissingUnion(missing),
    ]
    for file in (false, true)
        err = try
            arrowtypes_test_bytes((value=ambiguous,); file)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("cannot distinguish", sprint(showerror, err))
    end

    nulltype = ARROWTYPES_TEST_AC.NullType()
    storagenullfield = ARROWTYPES_TEST_AC.Field("stored-missing", nulltype; nullable=true)
    outernullfield = ARROWTYPES_TEST_AC.Field("outer-missing", nulltype; nullable=true)
    nulldata = ARROWTYPES_TEST_AC.ArrayData(
        nulltype,
        1,
        ARROWTYPES_TEST_AC.BufferSlice[];
        nullcount=1,
    )
    stringfield, stringdata = ARROWTYPES_TEST_AC.fromjulia("string", ["external"])
    ambiguousunion =
        ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[9, 5, 1])
    ambiguousfield = ARROWTYPES_TEST_AC.Field(
        "value",
        ambiguousunion;
        nullable=true,
        metadata=[
            "ARROW:extension:name" =>
                String(ARROWTYPES_TEST_AMBIGUOUS_MISSING_UNION_NAME),
            "ARROW:extension:metadata" => "",
        ],
        children=[storagenullfield, stringfield, outernullfield],
    )
    ambiguousdata = ARROWTYPES_TEST_AC.ArrayData(
        ambiguousunion,
        3,
        [
            ARROWTYPES_TEST_AC._databuffer(Int8[9, 5, 1]),
            ARROWTYPES_TEST_AC._databuffer(Int32[0, 0, 0]),
        ];
        children=[nulldata, stringdata, nulldata],
        nullcount=0,
    )
    ambiguousschema = ARROWTYPES_TEST_AC.Schema([ambiguousfield])
    ambiguousbatch = ARROWTYPES_TEST_AC.RecordBatch(ambiguousschema, [ambiguousdata], 3)
    for file in (false, true)
        bytes =
            file ? Arrow.writefile(ambiguousschema, [ambiguousbatch]) :
            Arrow.writestream(ambiguousschema, [ambiguousbatch])
        err = try
            Arrow.Table(bytes)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("indistinguishable outer missing", sprint(showerror, err))
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
        bytes = file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
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

    # A facade scan must retain the dense-Union child id until ArrowTypes
    # lifting consumes it. Both children use Int64 storage, so materializing
    # an ordinary storage column here would erase their logical identity.
    filebytes = Arrow.writefile(schema, [batch])
    streambytes = Arrow.writestream(schema, [batch])
    sources = (
        () -> copy(filebytes),
        () ->
            Arrow.SourceFile(ArrowTypesTestBytesSource(copy(filebytes)); tailbytes=32),
        () -> ArrowTypesTestBytesSource(copy(streambytes)),
    )
    pushed = Tables.Scan(select=(:value => :routed,), offset=1, limit=2)
    for makesource in sources
        table = Arrow.Table(makesource(); scan=pushed)
        wanted = expected[2:3]
        @test collect(Tables.columnnames(table)) == [:routed]
        @test table.routed == wanted
        @test typeof.(table.routed) == typeof.(wanted)
        outfield = only(getfield(table, :schema).fields)
        @test outfield.name == "routed"
        @test outfield.type.typeids == Int8[0, 1]
        @test getfield.(outfield.children, :name) == ["", ""]
    end

    # A predicate over a logical child cannot lower to the shared Int64
    # storage domain. It must execute after the same facade conversion.
    publicfilter = Tables.Scan(
        select=(:value,),
        filter=Tables.colcmp(==, Tables.col(:value), ArrowTypesTestID(3)),
    )
    for makesource in sources
        table = Arrow.Table(makesource(); scan=publicfilter)
        @test table.value == [ArrowTypesTestID(3)]
        @test typeof.(table.value) == [ArrowTypesTestID]
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
        @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") == "protected"
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
        bytes = file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
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
    parts = Tables.partitioner(((id=T[missing, missing],), (id=T[ArrowTypesTestID(7)],)))
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

@testset "fresh DictEncode preserves declared Union value fields" begin
    T = Union{Int64,String}
    N = Union{Missing,Int64,String}
    cases = (
        ("integer branch", T[1, 2], (2, 0)),
        ("string branch", T["one", "two"], (0, 2)),
        ("both branches", T[1, "two"], (1, 1)),
        ("nullable indices", N[missing, 1, "two"], (1, 1)),
        ("all-null indices", N[missing, missing], (0, 0)),
    )
    for file in (false, true), (label, values, childlengths) in cases
        @testset "$label ($(file ? "file" : "stream"))" begin
            bytes = arrowtypes_test_bytes((value=Arrow.DictEncode(values),); file=file)
            table = Arrow.Table(bytes)
            @test isequal(table.value, values)
            @test typeof.(table.value) == typeof.(values)

            field, batches = arrowtypes_test_core_parts(bytes)
            @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
            @test field.nullable == (Missing <: eltype(values))
            valuefield = ARROWTYPES_TEST_AC.dictvaluefield(field, field.type)
            @test valuefield.type isa ARROWTYPES_TEST_AC.UnionType
            @test length(valuefield.children) == 2
            @test all(
                child -> !(child.type isa ARROWTYPES_TEST_AC.NullType),
                valuefield.children,
            )
            intchild = findfirst(
                child -> child.type isa ARROWTYPES_TEST_AC.IntType,
                valuefield.children,
            )
            stringchild = findfirst(
                child -> child.type isa ARROWTYPES_TEST_AC.Utf8Type,
                valuefield.children,
            )
            @test intchild !== nothing
            @test stringchild !== nothing
            pool = only(batches).columns[1].dictionary
            @test pool !== nothing
            @test length(pool.children[intchild]) == childlengths[1]
            @test length(pool.children[stringchild]) == childlengths[2]
        end
    end

    expected = T[1, "two", 3, "four"]
    parts = Tables.partitioner((
        (value=Arrow.DictEncode(T[1, "two"]),),
        (value=Arrow.DictEncode(T[3, "four"]),),
    ))
    for file in (false, true)
        bytes = arrowtypes_test_bytes(parts; file=file)
        table = Arrow.Table(bytes)
        @test table.value == expected
        @test typeof.(table.value) == typeof.(expected)
        field, batches = arrowtypes_test_core_parts(bytes)
        @test (field.type::ARROWTYPES_TEST_AC.DictionaryType).valuetype isa
              ARROWTYPES_TEST_AC.UnionType
        @test length(batches) == 2
        @test batches[1].columns[1].dictionary === batches[2].columns[1].dictionary
    end

    later = Tables.partitioner((
        (value=Arrow.DictEncode(Missing[missing]),),
        (value=Arrow.DictEncode(N[1, "two", missing]),),
    ))
    laterexpected = N[missing, 1, "two", missing]
    for file in (false, true)
        bytes = arrowtypes_test_bytes(later; file=file)
        table = Arrow.Table(bytes)
        @test isequal(table.value, laterexpected)
        @test typeof.(table.value) == typeof.(laterexpected)
        field, batches = arrowtypes_test_core_parts(bytes)
        @test field.nullable
        @test (field.type::ARROWTYPES_TEST_AC.DictionaryType).valuetype isa
              ARROWTYPES_TEST_AC.UnionType
        @test batches[1].columns[1].dictionary === batches[2].columns[1].dictionary
    end
end

@testset "DictEncode Union category identity includes runtime type" begin
    values = Union{Int64,Float64}[1, 1.0]
    snapshots = Any[Int64[1], Float64[1.0]]
    typed = Arrow._mergecategorypools(snapshots; widen=true)
    @test typed == Real[1, 1.0]
    @test typeof.(typed) == typeof.(values)
    @test Arrow._mergecategorypools([[1, 1], [1, 2]]) == [1, 1, 2]

    for file in (false, true)
        bytes = arrowtypes_test_bytes((value=Arrow.DictEncode(values),); file=file)
        table = Arrow.Table(bytes)
        @test table.value == values
        @test typeof.(table.value) == typeof.(values)
        field, batches = arrowtypes_test_core_parts(bytes)
        @test (field.type::ARROWTYPES_TEST_AC.DictionaryType).valuetype isa
              ARROWTYPES_TEST_AC.UnionType
        @test length(only(batches).columns[1].dictionary) == 2
    end
end

@testset "DictEncode preserves registered same-storage Union branches" begin
    T = Union{ArrowTypesTestID,ArrowTypesTestAlternateID}
    cases = (
        ("ID branch", T[ArrowTypesTestID(1)], (1, 0)),
        ("alternate branch", T[ArrowTypesTestAlternateID(1)], (0, 1)),
        ("both branches", T[ArrowTypesTestID(1), ArrowTypesTestAlternateID(1)], (1, 1)),
    )
    for file in (false, true), (label, values, childlengths) in cases
        @testset "$label ($(file ? "file" : "stream"))" begin
            bytes = arrowtypes_test_bytes((value=Arrow.DictEncode(values),); file=file)
            table = Arrow.Table(bytes)
            @test table.value == values
            @test typeof.(table.value) == typeof.(values)
            field, batches = arrowtypes_test_core_parts(bytes)
            valuefield = ARROWTYPES_TEST_AC.dictvaluefield(field, field.type)
            @test valuefield.type isa ARROWTYPES_TEST_AC.UnionType
            names = arrowtypes_test_extension_name.(valuefield.children)
            idchild = findfirst(==(String(ARROWTYPES_TEST_ID_NAME)), names)
            alternatechild = findfirst(==(String(ARROWTYPES_TEST_ALTERNATE_ID_NAME)), names)
            @test idchild !== nothing
            @test alternatechild !== nothing
            pool = only(batches).columns[1].dictionary
            @test length(pool.children[idchild]) == childlengths[1]
            @test length(pool.children[alternatechild]) == childlengths[2]
        end
    end

    parts = Tables.partitioner((
        (value=Arrow.DictEncode(T[ArrowTypesTestID(1)]),),
        (value=Arrow.DictEncode(T[ArrowTypesTestAlternateID(1)]),),
    ))
    expected = T[ArrowTypesTestID(1), ArrowTypesTestAlternateID(1)]
    for file in (false, true)
        bytes = arrowtypes_test_bytes(parts; file=file)
        table = Arrow.Table(bytes)
        @test table.value == expected
        @test typeof.(table.value) == typeof.(expected)
        field, batches = arrowtypes_test_core_parts(bytes)
        @test (field.type::ARROWTYPES_TEST_AC.DictionaryType).valuetype isa
              ARROWTYPES_TEST_AC.UnionType
        @test batches[1].columns[1].dictionary === batches[2].columns[1].dictionary
    end
end

@testset "fresh DictEncode uses the ordinary recursive value builder" begin
    rows = [(id=Int32(1), label="one"), (id=Int32(2), label="two")]
    maps = [Dict("one" => Int32(1)), Dict("two" => Int32(2))]
    tuples = [(Int16(1), Int16(2)), (Int16(3), Int16(4))]
    temporals = (
        Date[Date(2024, 1, 1), Date(2024, 2, 29)],
        DateTime[DateTime(2024, 1, 1, 1, 2, 3), DateTime(2024, 2, 29, 4, 5, 6)],
        Time[Time(1, 2, 3), Time(4, 5, 6)],
        Millisecond[Millisecond(7), Millisecond(11)],
    )
    ids = ArrowTypesTestID.(Int64[1, 2])
    writeonly = ArrowTypesTestWriteOnly.(Int32[3, 4])
    namedwriteonly = ArrowTypesTestNamedWriteOnly.(Int32[5, 6])

    for file in (false, true)
        narrowed = Arrow.Table(
            arrowtypes_test_bytes((value=Arrow.DictEncode(Any[1, 2]),); file=file),
        )
        @test narrowed.value == Int64[1, 2]

        structtable =
            Arrow.Table(arrowtypes_test_bytes((value=Arrow.DictEncode(rows),); file=file))
        @test arrowtypes_test_rowdict.(structtable.value) == arrowtypes_test_rowdict.(rows)

        maptable =
            Arrow.Table(arrowtypes_test_bytes((value=Arrow.DictEncode(maps),); file=file))
        @test Dict.(maptable.value) == maps

        tupletable =
            Arrow.Table(arrowtypes_test_bytes((value=Arrow.DictEncode(tuples),); file=file))
        @test collect.(tupletable.value) == collect.(tuples)
        @test all(row -> all(x -> x isa Int16, row), tupletable.value)

        for temporal in temporals
            temporaltable = Arrow.Table(
                arrowtypes_test_bytes((value=Arrow.DictEncode(temporal),); file=file),
            )
            @test temporaltable.value == temporal
            @test eltype(temporaltable.value) === eltype(temporal)
        end

        idtable =
            Arrow.Table(arrowtypes_test_bytes((value=Arrow.DictEncode(ids),); file=file))
        @test idtable.value == ids
        @test eltype(idtable.value) === ArrowTypesTestID

        writeonlytable = Arrow.Table(
            arrowtypes_test_bytes((value=Arrow.DictEncode(writeonly),); file=file),
        )
        @test writeonlytable.value == Int32[3, 4]
        @test eltype(writeonlytable.value) === Int32

        namedbytes =
            arrowtypes_test_bytes((value=Arrow.DictEncode(namedwriteonly),); file=file)
        field, _ = arrowtypes_test_core_parts(namedbytes)
        @test arrowtypes_test_extension_name(field) ==
              String(ARROWTYPES_TEST_NAMED_WRITE_ONLY)
        namedtable =
            @test_logs (:warn, r"unsupported .*extension.*NamedWriteOnly") Arrow.Table(
                namedbytes,
            )
        @test DataAPI.colmetadata(namedtable, :value, "ARROW:extension:metadata") ==
              "write-only"
        @test namedtable.value == Int32[5, 6]
        @test eltype(namedtable.value) === Int32
    end
end

@testset "fresh DictEncode uses writer evidence once per category" begin
    values = ArrowTypesTestConcreteWriteID.(Int32[1, 2, 1])
    for file in (false, true)
        ARROWTYPES_TEST_CONCRETE_WRITE_CALLS[] = 0
        plainbytes = arrowtypes_test_bytes((value=values,); file=file)
        @test ARROWTYPES_TEST_CONCRETE_WRITE_CALLS[] == length(values)
        plaintable = Arrow.Table(plainbytes)
        @test plaintable.value == values
        @test eltype(plaintable.value) === ArrowTypesTestAbstractReadID

        ARROWTYPES_TEST_CONCRETE_WRITE_CALLS[] = 0
        bytes = arrowtypes_test_bytes((value=Arrow.DictEncode(values),); file=file)
        @test ARROWTYPES_TEST_CONCRETE_WRITE_CALLS[] == 2
        table = Arrow.Table(bytes)
        @test table.value == values
        @test eltype(table.value) === ArrowTypesTestAbstractReadID
        @test DataAPI.colmetadata(table, :value, "ARROW:extension:name") ==
              String(ARROWTYPES_TEST_ABSTRACT_READ_ID)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            rewritten = Arrow.Table(arrowtypes_test_bytes(source; file=outputfile))
            @test rewritten.value == values
            @test eltype(rewritten.value) === ArrowTypesTestAbstractReadID
        end
    end

    Wide = @NamedTuple{a::Union{Missing,Int64}}
    Narrow = @NamedTuple{a::Int64}
    parts = Tables.partitioner((
        (value=Arrow.DictEncode(Wide[(a=missing,), (a=1,)]),),
        (value=Arrow.DictEncode(Narrow[(a=2,)]),),
    ))
    for file in (false, true)
        table = Arrow.Table(arrowtypes_test_bytes(parts; file=file))
        @test isequal(
            arrowtypes_test_rowdict.(table.value),
            [Dict("a" => missing), Dict("a" => 1), Dict("a" => 2)],
        )
        field = getfield(table, :schema).fields[1]
        @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test only(field.children).nullable
    end

    mixedparts = Tables.partitioner((
        (
            value=Arrow.DictEncode(
                ArrowTypesTestWriteOnly[
                    ArrowTypesTestWriteOnly(1),
                    ArrowTypesTestWriteOnly(2),
                ],
            ),
        ),
        (
            value=Arrow.DictEncode(
                ArrowTypesTestAlternateWriteOnly[
                    ArrowTypesTestAlternateWriteOnly(3),
                    ArrowTypesTestAlternateWriteOnly(4),
                ],
            ),
        ),
    ))
    for file in (false, true)
        bytes = arrowtypes_test_bytes(mixedparts; file=file)
        table = Arrow.Table(bytes)
        @test table.value == Int32[1, 2, 3, 4]
        @test eltype(table.value) === Int32
        field, batches = arrowtypes_test_core_parts(bytes)
        @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test field.type.valuetype isa ARROWTYPES_TEST_AC.IntType
        @test length(batches) == 2
        @test batches[1].columns[1].dictionary === batches[2].columns[1].dictionary
    end

    abstractparts = Tables.partitioner((
        (
            value=Arrow.DictEncode(
                ArrowTypesTestAbstractWriteOnly[
                    ArrowTypesTestAbstractWriteA(5),
                    ArrowTypesTestAbstractWriteB(6),
                ],
            ),
        ),
        (
            value=Arrow.DictEncode(
                ArrowTypesTestAbstractWriteOnly[ArrowTypesTestAbstractWriteA(7),],
            ),
        ),
    ))
    for file in (false, true)
        bytes = arrowtypes_test_bytes(abstractparts; file=file)
        table = Arrow.Table(bytes)
        @test table.value == Int32[5, 6, 7]
        @test eltype(table.value) === Int32
        field, batches = arrowtypes_test_core_parts(bytes)
        @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test field.type.valuetype isa ARROWTYPES_TEST_AC.IntType
        @test length(batches) == 2
        @test batches[1].columns[1].dictionary === batches[2].columns[1].dictionary
    end

    Mixed = ArrowTypesTestAbstractMixedWrite
    mixedvalues =
        Mixed[ArrowTypesTestAbstractMixedInt(8), ArrowTypesTestAbstractMixedString("nine")]
    layouts = (
        (
            "one partition",
            (value=Arrow.DictEncode(mixedvalues),),
            Union{Int32,String}[Int32(8), "nine"],
        ),
        (
            "split",
            Tables.partitioner((
                (value=Arrow.DictEncode(Mixed[ArrowTypesTestAbstractMixedInt(8)]),),
                (value=Arrow.DictEncode(Mixed[ArrowTypesTestAbstractMixedString("nine")]),),
            )),
            Union{Int32,String}[Int32(8), "nine"],
        ),
        (
            "reverse",
            Tables.partitioner((
                (value=Arrow.DictEncode(Mixed[ArrowTypesTestAbstractMixedString("nine")]),),
                (value=Arrow.DictEncode(Mixed[ArrowTypesTestAbstractMixedInt(8)]),),
            )),
            Union{Int32,String}["nine", Int32(8)],
        ),
        (
            "all-missing first",
            Tables.partitioner((
                (value=Arrow.DictEncode(Union{Missing,Mixed}[missing]),),
                (value=Arrow.DictEncode(mixedvalues),),
            )),
            Union{Missing,Int32,String}[missing, Int32(8), "nine"],
        ),
        (
            "mixed then homogeneous",
            Tables.partitioner((
                (value=Arrow.DictEncode(mixedvalues),),
                (value=Arrow.DictEncode(Mixed[ArrowTypesTestAbstractMixedInt(10)]),),
            )),
            Union{Int32,String}[Int32(8), "nine", Int32(10)],
        ),
    )
    for file in (false, true)
        controlbytes = arrowtypes_test_bytes(layouts[1][2]; file)
        controlfield, _ = arrowtypes_test_core_parts(controlbytes)
        controlvaluefield =
            ARROWTYPES_TEST_AC.dictvaluefield(controlfield, controlfield.type)
        for (label, layout, expected) in layouts
            @testset "$label ($(file ? "file" : "stream"))" begin
                bytes = arrowtypes_test_bytes(layout; file)
                table = Arrow.Table(bytes)
                @test isequal(table.value, expected)
                field, batches = arrowtypes_test_core_parts(bytes)
                valuefield = ARROWTYPES_TEST_AC.dictvaluefield(field, field.type)
                @test arrowtypes_test_field_contract_equal(controlvaluefield, valuefield)
                @test all(
                    batch ->
                        batch.columns[1].dictionary === batches[1].columns[1].dictionary,
                    batches,
                )
            end
        end
    end

    indistinguishable = Union{ArrowTypesTestWriteOnly,ArrowTypesTestAlternateWriteOnly}[
        ArrowTypesTestWriteOnly(11),
        ArrowTypesTestAlternateWriteOnly(12),
    ]
    mixedunionparts = Tables.partitioner((
        (value=Arrow.DictEncode(Union{Int64,String}[Int64(13), "fourteen"]),),
        (
            value=Arrow.DictEncode(
                ArrowTypesTestWriteOnlyLogicalUnion[
                    ArrowTypesTestWriteOnlyLogicalUnion(Int64(15)),
                    ArrowTypesTestWriteOnlyLogicalUnion("sixteen"),
                ],
            ),
        ),
    ))
    for file in (false, true)
        @test Arrow.Table(
            arrowtypes_test_bytes((value=Arrow.DictEncode(indistinguishable),); file),
        ).value == Int32[11, 12]
        @test Arrow.Table(arrowtypes_test_bytes(mixedunionparts; file)).value ==
              Union{Int64,String}[Int64(13), "fourteen", Int64(15), "sixteen"]
    end
end

@testset "retained dictionary Union reconstruction refuses lost routing" begin
    for T in (Union{Int64,String}, Union{Missing,Int64,String}),
        inputfile in (false, true),
        outputfile in (false, true),
        sourcekind in (:table, :stream)

        bytes =
            arrowtypes_test_bytes((value=Arrow.DictEncode(T[1, "two"]),); file=inputfile)
        source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
        err = try
            arrowtypes_test_bytes(source; file=outputfile)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("retained UnionType", sprint(showerror, err))
    end
end

@testset "partitioned DictEncode uses later type evidence" begin
    expected = Union{Missing,Int64}[missing, missing, 3, 4]
    for file in (false, true)
        parts = Tables.partitioner((
            (value=Arrow.DictEncode(Missing[missing, missing]),),
            (value=Arrow.DictEncode(Int64[3, 4]),),
        ))
        bytes = arrowtypes_test_bytes(parts; file=file)
        table = Arrow.Table(bytes)
        @test isequal(table.value, expected)
        @test eltype(table.value) === Union{Missing,Int64}
        field = getfield(table, :schema).fields[1]
        @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test field.type.valuetype isa ARROWTYPES_TEST_AC.IntType
        @test field.nullable

        batches = collect(Arrow.Stream(bytes))
        @test length(batches) == 2
        @test isequal(batches[1].value, Union{Missing,Int64}[missing, missing])
        @test batches[2].value == Int64[3, 4]

        allnull = Tables.partitioner((
            (value=Arrow.DictEncode(Missing[missing]),),
            (value=Arrow.DictEncode(Missing[]),),
        ))
        err = try
            arrowtypes_test_bytes(allnull; file=file)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("cannot infer a dictionary value type", sprint(showerror, err))
    end
end

@testset "partitioned DictEncode enforces logical value agreement" begin
    Point = ArrowTypesTestPoint{:dictionary}
    expected = Point[Point(Int32(1), Int32(2)), Point(Int32(3), Int32(4))]
    abstractfirst = Tables.partitioner((
        (point=Arrow.DictEncode(ArrowTypesTestPoint[]),),
        (point=Arrow.DictEncode(expected),),
    ))
    for file in (false, true)
        table = Arrow.Table(arrowtypes_test_bytes(abstractfirst; file=file))
        @test table.point == expected
        @test eltype(table.point) === Point
        @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") == "dictionary"
    end

    Left = ArrowTypesTestPoint{:left}
    Right = ArrowTypesTestPoint{:right}
    mismatch = Tables.partitioner((
        (point=Arrow.DictEncode(Left[Left(Int32(1), Int32(2))]),),
        (point=Arrow.DictEncode(Right[Right(Int32(3), Int32(4))]),),
    ))
    for file in (false, true)
        err = try
            arrowtypes_test_bytes(mismatch; file=file)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("different ordered metadata", sprint(showerror, err))
    end
end

@testset "partitioned DictEncode preserves registered Struct values" begin
    Point = ArrowTypesTestPoint{:dictionary}
    T = Union{Missing,Point}
    parts = Tables.partitioner((
        (point=Arrow.DictEncode(T[missing, missing]),),
        (
            point=Arrow.DictEncode(
                T[Point(Int32(1), Int32(2)), missing, Point(Int32(3), Int32(4))],
            ),
        ),
    ))
    expected =
        T[missing, missing, Point(Int32(1), Int32(2)), missing, Point(Int32(3), Int32(4))]
    colmetadata = Dict(:point => ["application:key" => "kept"])

    for file in (false, true)
        bytes = arrowtypes_test_bytes(parts; file=file, colmetadata=colmetadata)
        table = Arrow.Table(bytes)
        @test isequal(table.point, expected)
        @test eltype(table.point) === T

        field = getfield(table, :schema).fields[1]
        @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test field.nullable
        # The physical Struct order is deliberately different from the
        # logical Point constructor order. Dictionary lifting must honor
        # these child names through `fromarrowstruct`.
        @test getfield.(field.children, :name) == ["y", "x"]
        @test DataAPI.colmetadata(table, :point, "ARROW:extension:name") ==
              String(ARROWTYPES_TEST_POINT_NAME)
        @test DataAPI.colmetadata(table, :point, "ARROW:extension:metadata") == "dictionary"
        @test DataAPI.colmetadata(table, :point, "application:key") == "kept"

        stream = Arrow.Stream(bytes)
        source = getfield(stream, :src)
        budget = getfield(stream, :budget)
        batches = [Arrow._batch(source, i, budget) for i = 1:2]
        @test batches[1].columns[1].dictionary === batches[2].columns[1].dictionary
        valuefield = ARROWTYPES_TEST_AC.dictvaluefield(field, field.type)
        @test ARROWTYPES_TEST_AC.materialize(
            valuefield,
            batches[1].columns[1].dictionary,
        ) == Any[
            Pair{String,Any}["y" => Int32(2), "x" => Int32(1)],
            Pair{String,Any}["y" => Int32(4), "x" => Int32(3)],
        ]

        rewritten = arrowtypes_test_bytes(Arrow.Stream(bytes); file=file)
        rewritten_table = Arrow.Table(rewritten)
        @test isequal(rewritten_table.point, expected)
        @test eltype(rewritten_table.point) === T
        rewritten_field = getfield(rewritten_table, :schema).fields[1]
        @test rewritten_field.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test rewritten_field.nullable
        @test getfield.(rewritten_field.children, :name) == ["y", "x"]
        @test DataAPI.colmetadata(rewritten_table, :point, "ARROW:extension:metadata") ==
              "dictionary"
        @test DataAPI.colmetadata(rewritten_table, :point, "application:key") == "kept"
        rewritten_stream = Arrow.Stream(rewritten)
        rewritten_source = getfield(rewritten_stream, :src)
        rewritten_budget = getfield(rewritten_stream, :budget)
        rewritten_batches =
            [Arrow._batch(rewritten_source, i, rewritten_budget) for i = 1:2]
        @test rewritten_batches[1].columns[1].dictionary ===
              rewritten_batches[2].columns[1].dictionary
    end
end

@testset "DictEncode NullType keeps valid values distinct from outer nulls" begin
    function checknullencoding(bytes, expectedvalid; nullable::Bool)
        stream = Arrow.Stream(bytes)
        source = getfield(stream, :src)
        budget = getfield(stream, :budget)
        field = Arrow._batchfields(source)[1]
        @test field.type isa ARROWTYPES_TEST_AC.DictionaryType
        @test field.type.valuetype isa ARROWTYPES_TEST_AC.NullType
        @test field.nullable == nullable
        batches = [Arrow._batch(source, i, budget) for i = 1:Arrow._nbatches(source)]
        @test length(batches) == length(expectedvalid)
        for (batch, valid) in zip(batches, expectedvalid)
            data = batch.columns[1]
            pool = data.dictionary
            @test pool !== nothing
            @test length(pool) == 1
            @test ARROWTYPES_TEST_AC.nullcount(pool) == 1
            @test [ARROWTYPES_TEST_AC.isvalid_at(data, i) for i in eachindex(valid)] == valid
            @test ARROWTYPES_TEST_AC.nullcount(data) == count(!, valid)
        end
        if length(batches) > 1
            @test batches[1].columns[1].dictionary === batches[2].columns[1].dictionary
        end
        return nothing
    end

    cases = (
        ("Nothing", Nothing, nothing),
        ("custom", ArrowTypesTestNullLogical, ArrowTypesTestNullLogical()),
    )
    for (label, T, logical) in cases
        @testset "$label fresh" begin
            nonnullable = T[logical, logical]
            E = Union{Missing,T}
            nullable = E[logical, missing, logical]
            for file in (false, true)
                bytes =
                    arrowtypes_test_bytes((value=Arrow.DictEncode(nonnullable),); file=file)
                table = Arrow.Table(bytes)
                @test table.value == nonnullable
                @test eltype(table.value) === T
                checknullencoding(bytes, [Bool[true, true]]; nullable=false)

                nullablebytes =
                    arrowtypes_test_bytes((value=Arrow.DictEncode(nullable),); file=file)
                nullabletable = Arrow.Table(nullablebytes)
                @test isequal(nullabletable.value, nullable)
                @test eltype(nullabletable.value) === E
                checknullencoding(nullablebytes, [Bool[true, false, true]]; nullable=true)

                selected =
                    Arrow.Table(nullablebytes; scan=Tables.Scan(select=(:value,), limit=2))
                @test isequal(selected.value, nullable[1:2])
                filtered = Arrow.Table(
                    nullablebytes;
                    scan=Tables.Scan(
                        select=(:value,),
                        filter=Tables.colcmp(==, Tables.col(:value), logical),
                    ),
                )
                @test filtered.value == T[logical, logical]
            end
        end

        @testset "$label partitioned retained rewrite" begin
            E = Union{Missing,T}
            parts = Tables.partitioner((
                (value=Arrow.DictEncode(E[logical, missing]),),
                (value=Arrow.DictEncode(E[logical]),),
            ))
            expected = E[logical, missing, logical]
            for sourcefile in (false, true)
                bytes = arrowtypes_test_bytes(parts; file=sourcefile)
                table = Arrow.Table(bytes)
                @test isequal(table.value, expected)
                @test eltype(table.value) === E
                checknullencoding(bytes, [Bool[true, false], Bool[true]]; nullable=true)

                for outfile in (false, true)
                    tablerewrite = arrowtypes_test_bytes(table; file=outfile)
                    rewritten = Arrow.Table(tablerewrite)
                    @test isequal(rewritten.value, expected)
                    @test eltype(rewritten.value) === E
                    checknullencoding(
                        tablerewrite,
                        [Bool[true, false, true]];
                        nullable=true,
                    )

                    streamrewrite = arrowtypes_test_bytes(Arrow.Stream(bytes); file=outfile)
                    rewrittenstream = Arrow.Table(streamrewrite)
                    @test isequal(rewrittenstream.value, expected)
                    @test eltype(rewrittenstream.value) === E
                    checknullencoding(
                        streamrewrite,
                        [Bool[true, false], Bool[true]];
                        nullable=true,
                    )
                end
            end
        end
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
        @test_throws ArgumentError arrowtypes_test_bytes(different_child_labels; file=file)
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

    incompatible =
        arrowtypes_test_table((id=ArrowTypesTestID[ArrowTypesTestID(1)],); file=true)
    getfield(incompatible, :columns)[1] = Int64[99]
    incompatiblemissing =
        arrowtypes_test_table((id=Union{Missing,ArrowTypesTestID}[missing],); file=true)
    getfield(incompatiblemissing, :columns)[1] = Union{Missing,Int64}[missing]
    for file in (false, true)
        @test_throws ArgumentError arrowtypes_test_bytes(incompatible; file)
        @test_throws ArgumentError arrowtypes_test_bytes(incompatiblemissing; file)
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

@testset "retained temporal composites stay in one writer domain" begin
    presentdated = ArrowTypesTestDated[
        ArrowTypesTestDated(Date(2024, 1, 2), Int32(1)),
        ArrowTypesTestDated(Date(2024, 2, 3), Int32(2)),
    ]
    dated = Union{Missing,ArrowTypesTestDated}[
        ArrowTypesTestDated(Date(2024, 1, 2), Int32(1)),
        missing,
        ArrowTypesTestDated(Date(2024, 2, 3), Int32(2)),
    ]
    presentpack = ArrowTypesTestTemporalPack[
        ArrowTypesTestTemporalPack(
            (ArrowTypesTestDay(Date(2024, 3, 4)), ArrowTypesTestDay(Date(2024, 3, 5))),
            DateTime(2024, 3, 4, 5, 6, 7),
        ),
        ArrowTypesTestTemporalPack(
            (ArrowTypesTestDay(Date(2024, 4, 6)), ArrowTypesTestDay(Date(2024, 4, 7))),
            DateTime(2024, 4, 6, 7, 8, 9),
        ),
    ]
    pack = Union{Missing,ArrowTypesTestTemporalPack}[
        ArrowTypesTestTemporalPack(
            (ArrowTypesTestDay(Date(2024, 3, 4)), ArrowTypesTestDay(Date(2024, 3, 5))),
            DateTime(2024, 3, 4, 5, 6, 7),
        ),
        missing,
        ArrowTypesTestTemporalPack(
            (ArrowTypesTestDay(Date(2024, 4, 6)), ArrowTypesTestDay(Date(2024, 4, 7))),
            DateTime(2024, 4, 6, 7, 8, 9),
        ),
    ]
    emptyvisible = Union{Missing,ArrowTypesTestTemporalPack}[missing, missing]
    for values in (presentdated, dated, presentpack, pack, emptyvisible),
        encoded in (false, true),
        inputfile in (false, true)

        input = encoded ? Arrow.DictEncode(values) : values
        bytes = arrowtypes_test_bytes((value=input,); file=inputfile)
        @test isequal(Arrow.Table(bytes).value, values)
        field, _ = arrowtypes_test_core_parts(bytes)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source = sourcekind === :table ? Arrow.Table(bytes) : Arrow.Stream(bytes)
            rewrittenbytes = arrowtypes_test_bytes(source; file=outputfile)
            @test isequal(Arrow.Table(rewrittenbytes).value, values)
            rewrittenfield, _ = arrowtypes_test_core_parts(rewrittenbytes)
            @test arrowtypes_test_field_contract_equal(field, rewrittenfield)
        end
    end
end

@testset "nullable retained composites keep logical child placeholders" begin
    cases = (
        ("present first", Bool[true, false], true, false),
        ("present second", Bool[false, true], true, false),
        ("all parents null", Bool[false, false], true, false),
        ("nullable marked child", Bool[true, false], true, true),
        ("native child", Bool[false, true], false, false),
    )
    for parentkind in (:struct, :fixed), (label, present, marked, childnullable) in cases
        childname = parentkind === :struct ? "id" : "item"
        physical = if parentkind === :struct
            childnullable ? Union{Missing,Int64}[missing, 22] : Int64[11, 22]
        else
            childnullable ? Union{Missing,Int64}[missing, 12, 21, 22] : Int64[11, 12, 21, 22]
        end
        nativefield, childdata = ARROWTYPES_TEST_AC.fromjulia(childname, physical)
        childfield =
            marked ? arrowtypes_test_marked_id_field(childname; nullable=childnullable) :
            nativefield
        parenttype =
            parentkind === :struct ? ARROWTYPES_TEST_AC.StructType() :
            ARROWTYPES_TEST_AC.FixedSizeListType(2)
        parentfield = ARROWTYPES_TEST_AC.Field(
            "value",
            parenttype;
            nullable=true,
            children=[childfield],
        )
        parentdata = ARROWTYPES_TEST_AC.ArrayData(
            parenttype,
            2,
            [ARROWTYPES_TEST_AC._bitmapbuffer(present)];
            children=[childdata],
            nullcount=count(!, present),
        )
        schema = ARROWTYPES_TEST_AC.Schema([parentfield])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [parentdata], 2)
        logical = x -> x === missing || !marked ? x : ArrowTypesTestID(x)
        expected = Any[]
        for i = 1:2
            if !present[i]
                push!(expected, missing)
            elseif parentkind === :struct
                push!(expected, Pair{String,Any}[childname => logical(physical[i])])
            else
                lo = 2 * (i - 1) + 1
                push!(expected, [logical(physical[j]) for j = lo:(lo + 1)])
            end
        end

        @testset "$parentkind $label" begin
            for inputfile in (false, true)
                inputbytes =
                    inputfile ? Arrow.writefile(schema, [batch]) :
                    Arrow.writestream(schema, [batch])
                @test isequal(Arrow.Table(inputbytes).value, expected)
                for sourcekind in (:table, :stream), outputfile in (false, true)
                    source =
                        sourcekind === :table ? Arrow.Table(inputbytes) :
                        Arrow.Stream(inputbytes)
                    outputbytes = arrowtypes_test_bytes(source; file=outputfile)
                    rewritten = Arrow.Table(outputbytes)
                    @test isequal(rewritten.value, expected)
                    rewrittenfield = getfield(rewritten, :schema).fields[1]
                    @test arrowtypes_test_field_contract_equal(parentfield, rewrittenfield)
                    if marked
                        logicalchildren = Any[]
                        for row in rewritten.value
                            row === missing && continue
                            if parentkind === :struct
                                push!(logicalchildren, last(only(row)))
                            else
                                append!(logicalchildren, row)
                            end
                        end
                        @test all(
                            x -> x === missing || x isa ArrowTypesTestID,
                            logicalchildren,
                        )
                    end
                end
            end
        end
    end
end

@testset "retained composite placeholders preserve batch topology" begin
    for parentkind in (:struct, :fixed)
        childname = parentkind === :struct ? "id" : "item"
        childfield = arrowtypes_test_marked_id_field(childname)
        parenttype =
            parentkind === :struct ? ARROWTYPES_TEST_AC.StructType() :
            ARROWTYPES_TEST_AC.FixedSizeListType(2)
        parentfield = ARROWTYPES_TEST_AC.Field(
            "value",
            parenttype;
            nullable=true,
            children=[childfield],
        )
        schema = ARROWTYPES_TEST_AC.Schema([parentfield])
        presentparts = (Bool[false, false], Bool[true, false])
        physicalparts =
            parentkind === :struct ? (Int64[0, 0], Int64[7, 0]) :
            (Int64[0, 0, 0, 0], Int64[7, 8, 0, 0])
        batches = ARROWTYPES_TEST_AC.RecordBatch[]
        for (present, physical) in zip(presentparts, physicalparts)
            _, childdata = ARROWTYPES_TEST_AC.fromjulia(childname, physical)
            parentdata = ARROWTYPES_TEST_AC.ArrayData(
                parenttype,
                2,
                [ARROWTYPES_TEST_AC._bitmapbuffer(present)];
                children=[childdata],
                nullcount=count(!, present),
            )
            push!(batches, ARROWTYPES_TEST_AC.RecordBatch(schema, [parentdata], 2))
        end
        expected = if parentkind === :struct
            Any[missing, missing, Pair{String,Any}["id" => ArrowTypesTestID(7)], missing]
        else
            Any[
                missing,
                missing,
                ArrowTypesTestID[ArrowTypesTestID(7), ArrowTypesTestID(8)],
                missing,
            ]
        end

        for inputfile in (false, true)
            inputbytes =
                inputfile ? Arrow.writefile(schema, batches) :
                Arrow.writestream(schema, batches)
            for sourcekind in (:table, :stream), outputfile in (false, true)
                source =
                    sourcekind === :table ? Arrow.Table(inputbytes) :
                    Arrow.Stream(inputbytes)
                outputbytes = arrowtypes_test_bytes(source; file=outputfile)
                rewritten = Arrow.Table(outputbytes)
                @test isequal(rewritten.value, expected)
                rewrittenfield, rewrittenbatches = arrowtypes_test_core_parts(outputbytes)
                @test arrowtypes_test_field_contract_equal(parentfield, rewrittenfield)
                @test length(rewrittenbatches) == (sourcekind === :table ? 1 : 2)
                expectedvalidity =
                    sourcekind === :table ? [Bool[false, false, true, false]] :
                    collect(presentparts)
                @test [
                    [
                        ARROWTYPES_TEST_AC.isvalid_at(batch.columns[1], i) for
                        i = 1:(batch.nrows)
                    ] for batch in rewrittenbatches
                ] == expectedvalidity
                width = parentkind === :struct ? 1 : 2
                @test all(
                    batch -> length(only(batch.columns[1].children)) == width * batch.nrows,
                    rewrittenbatches,
                )
            end
        end
    end
end

@testset "retained nested Struct and fixed list preserve marked leaves" begin
    idfield = arrowtypes_test_marked_id_field("item")
    fixedtype = ARROWTYPES_TEST_AC.FixedSizeListType(2)
    fixedfield =
        ARROWTYPES_TEST_AC.Field("inner", fixedtype; nullable=false, children=[idfield])
    structtype = ARROWTYPES_TEST_AC.StructType()
    parentfield =
        ARROWTYPES_TEST_AC.Field("value", structtype; nullable=true, children=[fixedfield])
    schema = ARROWTYPES_TEST_AC.Schema([parentfield])
    presentparts = (Bool[true, true], Bool[true, false])
    physicalparts = (Int64[1, 2, 3, 4], Int64[5, 6, 70, 80])
    batches = ARROWTYPES_TEST_AC.RecordBatch[]
    for (present, physical) in zip(presentparts, physicalparts)
        _, iddata = ARROWTYPES_TEST_AC.fromjulia("item", physical)
        fixeddata = ARROWTYPES_TEST_AC.ArrayData(
            fixedtype,
            2,
            [ARROWTYPES_TEST_AC.BufferSlice()];
            children=[iddata],
            nullcount=0,
        )
        parentdata = ARROWTYPES_TEST_AC.ArrayData(
            structtype,
            2,
            [ARROWTYPES_TEST_AC._bitmapbuffer(present)];
            children=[fixeddata],
            nullcount=count(!, present),
        )
        push!(batches, ARROWTYPES_TEST_AC.RecordBatch(schema, [parentdata], 2))
    end
    expected = Any[
        Pair{String,Any}["inner" => ArrowTypesTestID[
            ArrowTypesTestID(1),
            ArrowTypesTestID(2),
        ],],
        Pair{String,Any}["inner" => ArrowTypesTestID[
            ArrowTypesTestID(3),
            ArrowTypesTestID(4),
        ],],
        Pair{String,Any}["inner" => ArrowTypesTestID[
            ArrowTypesTestID(5),
            ArrowTypesTestID(6),
        ],],
        missing,
    ]

    for inputfile in (false, true)
        inputbytes =
            inputfile ? Arrow.writefile(schema, batches) :
            Arrow.writestream(schema, batches)
        for sourcekind in (:table, :stream), outputfile in (false, true)
            source =
                sourcekind === :table ? Arrow.Table(inputbytes) : Arrow.Stream(inputbytes)
            outputbytes = arrowtypes_test_bytes(source; file=outputfile)
            rewritten = Arrow.Table(outputbytes)
            @test isequal(rewritten.value, expected)
            rewrittenfield, rewrittenbatches = arrowtypes_test_core_parts(outputbytes)
            @test arrowtypes_test_field_contract_equal(parentfield, rewrittenfield)
            @test length(rewrittenbatches) == (sourcekind === :table ? 1 : 2)
            @test all(
                batch -> begin
                    fixed = only(batch.columns[1].children)
                    ids = only(fixed.children)
                    length(fixed) == batch.nrows && length(ids) == 2 * batch.nrows
                end,
                rewrittenbatches,
            )
        end
    end
end

@testset "retained null parents do not duplicate variable payloads" begin
    payload = repeat("x", 64 * 1024)
    present = Bool[true, falses(16)...]
    for parentkind in (:struct, :fixed)
        childname = parentkind === :struct ? "text" : "item"
        physical =
            parentkind === :struct ? String[payload, fill("", 16)...] :
            String[payload, "tail", fill("", 32)...]
        childfield, childdata = ARROWTYPES_TEST_AC.fromjulia(childname, physical)
        parenttype =
            parentkind === :struct ? ARROWTYPES_TEST_AC.StructType() :
            ARROWTYPES_TEST_AC.FixedSizeListType(2)
        parentfield = ARROWTYPES_TEST_AC.Field(
            "value",
            parenttype;
            nullable=true,
            children=[childfield],
        )
        parentdata = ARROWTYPES_TEST_AC.ArrayData(
            parenttype,
            length(present),
            [ARROWTYPES_TEST_AC._bitmapbuffer(present)];
            children=[childdata],
            nullcount=count(!, present),
        )
        schema = ARROWTYPES_TEST_AC.Schema([parentfield])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [parentdata], length(present))
        expected = if parentkind === :struct
            Any[Pair{String,Any}["text" => payload], fill(missing, 16)...]
        else
            Any[String[payload, "tail"], fill(missing, 16)...]
        end

        for file in (false, true)
            inputbytes =
                file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
            for sourcekind in (:table, :stream)
                source =
                    sourcekind === :table ? Arrow.Table(inputbytes) :
                    Arrow.Stream(inputbytes)
                outputbytes = arrowtypes_test_bytes(source; file=file)
                @test length(outputbytes) < 3 * length(inputbytes)
                rewritten = Arrow.Table(outputbytes)
                @test isequal(rewritten.value, expected)
                @test arrowtypes_test_field_contract_equal(
                    parentfield,
                    getfield(rewritten, :schema).fields[1],
                )
            end
        end
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
    schema = ARROWTYPES_TEST_AC.Schema([structfield, mapfield, fixedfield, unknownfield])
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
            @test_logs (:warn, r"unsupported .*extension.*UnknownStruct") Arrow.Table(bytes)
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

@testset "wide extension Struct signatures are bounded" begin
    widefield = function (n; marked::Bool=true)
        children = ARROWTYPES_TEST_AC.Field[
            ARROWTYPES_TEST_AC.Field(
                "child_$i",
                ARROWTYPES_TEST_AC.NullType();
                nullable=true,
            ) for i = 1:n
        ]
        metadata =
            marked ?
            arrowtypes_test_extension_metadata("JuliaLang.ArrowTests.UnknownWideStruct") : nothing
        return ARROWTYPES_TEST_AC.Field(
            "wide",
            ARROWTYPES_TEST_AC.StructType();
            nullable=false,
            metadata,
            children,
        )
    end

    exact = widefield(1_024)
    # Exact NamedTuple lifting is available only for extension and child names
    # that trusted Julia code already registered in the process.
    Symbol("JuliaLang.ArrowTests.UnknownWideStruct")
    foreach(child -> Symbol(child.name), exact.children)
    haslabel, target, storage =
        Arrow._arrowtypestarget(Arrow._ArrowTypesContext(warn=false), exact)
    @test haslabel
    @test target === nothing
    @test storage <: NamedTuple
    @test fieldcount(storage) == 1_024

    bounded = widefield(1_025)
    @test Arrow._arrowtypestarget(Arrow._ArrowTypesContext(warn=false), bounded) ==
          (true, nothing, Vector{Pair{String,Any}})

    nested = ARROWTYPES_TEST_AC.Field(
        "outer",
        ARROWTYPES_TEST_AC.StructType();
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(
            "JuliaLang.ArrowTests.UnknownNestedWideStruct",
        ),
        children=[widefield(1_025; marked=false)],
    )
    Symbol("JuliaLang.ArrowTests.UnknownNestedWideStruct")
    _, nestedtarget, nestedstorage =
        Arrow._arrowtypestarget(Arrow._ArrowTypesContext(warn=false), nested)
    @test nestedtarget === nothing
    @test nestedstorage === Vector{Pair{String,Any}}

    larger = widefield(8_192)
    Arrow._arrowtypestarget(Arrow._ArrowTypesContext(warn=false), bounded)
    Arrow._arrowtypestarget(Arrow._ArrowTypesContext(warn=false), larger)
    GC.gc()
    smallallocation =
        @allocated Arrow._arrowtypestarget(Arrow._ArrowTypesContext(warn=false), bounded)
    GC.gc()
    largeallocation =
        @allocated Arrow._arrowtypestarget(Arrow._ArrowTypesContext(warn=false), larger)
    @test largeallocation <= smallallocation + 250_000

    schema = ARROWTYPES_TEST_AC.Schema([bounded])
    bytes = Arrow.writestream(schema, ARROWTYPES_TEST_AC.RecordBatch[])
    table = @test_logs (:warn, r"unsupported .*UnknownWideStruct") Arrow.Table(bytes)
    @test isempty(table.wide)
    @test eltype(table.wide) === Vector{Pair{String,Any}}

    unmarked = widefield(1_025; marked=false)
    unmarkedschema = ARROWTYPES_TEST_AC.Schema([unmarked])
    unmarkedbytes = Arrow.writestream(unmarkedschema, ARROWTYPES_TEST_AC.RecordBatch[])
    unmarkedtable = @test_logs min_level = Base.CoreLogging.Error Arrow.Table(unmarkedbytes)
    @test isempty(unmarkedtable.wide)
    @test eltype(unmarkedtable.wide) === Vector{Pair{String,Any}}

    longname = repeat("wide-extension-", 1_000)
    displayname = Arrow._extensionwarningname(longname)
    @test endswith(displayname, "…")
    @test ncodeunits(displayname) == Arrow._MAX_EXTENSION_WARNING_BYTES + ncodeunits("…")
end

@testset "ArrowTypes schema caches consume the reader allocation budget" begin
    leaf = ARROWTYPES_TEST_AC.Field(
        "leaf",
        ARROWTYPES_TEST_AC.IntType(64, true);
        nullable=false,
    )

    entryprobe = Arrow.AllocationBudget(1_000_000)
    entrybefore = Arrow._remaining(entryprobe)
    Arrow._chargedictentry!(entryprobe, ARROWTYPES_TEST_AC.Field, Bool, "test route cache")
    routeentrybytes = entrybefore - Arrow._remaining(entryprobe)
    @test routeentrybytes > 0

    calls = Ref(0)
    cache = Dict{ARROWTYPES_TEST_AC.Field,Bool}()
    compute = function ()
        calls[] += 1
        return false
    end
    @test_throws Arrow.AllocationLimitError Arrow._memoized!(
        compute,
        cache,
        leaf,
        Arrow.AllocationBudget(0),
        "test route cache",
    )
    @test isempty(cache)
    @test calls[] == 0

    exact = Arrow.AllocationBudget(routeentrybytes)
    @test !Arrow._memoized!(compute, cache, leaf, exact, "test route cache")
    @test Arrow._remaining(exact) == 0
    @test calls[] == 1

    @test_throws Arrow.AllocationLimitError Arrow._ArrowTypesRoutePlan(
        Arrow.AllocationBudget(0),
    )
    @test_throws Arrow.AllocationLimitError Arrow._ArrowTypesContext(
        warn=false,
        budget=Arrow.AllocationBudget(0),
    )

    plancontainerprobe = Arrow.AllocationBudget(1_000_000)
    plancontainerbefore = Arrow._remaining(plancontainerprobe)
    Arrow._ArrowTypesRoutePlan(plancontainerprobe)
    plancontainerbytes = plancontainerbefore - Arrow._remaining(plancontainerprobe)
    @test plancontainerbytes > 0
    Arrow._ArrowTypesRoutePlan()
    GC.gc()
    plancontainerallocation = @allocated Arrow._ArrowTypesRoutePlan()
    @test plancontainerbytes >= plancontainerallocation

    contextcontainerprobe = Arrow.AllocationBudget(1_000_000)
    contextcontainerbefore = Arrow._remaining(contextcontainerprobe)
    Arrow._ArrowTypesContext(warn=false, budget=contextcontainerprobe)
    contextcontainerbytes = contextcontainerbefore - Arrow._remaining(contextcontainerprobe)
    @test contextcontainerbytes > plancontainerbytes
    Arrow._ArrowTypesContext(warn=false)
    GC.gc()
    contextcontainerallocation = @allocated Arrow._ArrowTypesContext(warn=false)
    @test contextcontainerbytes >= contextcontainerallocation
    @test !Arrow._memoized!(compute, cache, leaf, exact, "test route cache")
    @test Arrow._remaining(exact) == 0
    @test calls[] == 1

    routeexact = Arrow.AllocationBudget(plancontainerbytes + routeentrybytes)
    exactplan = Arrow._ArrowTypesRoutePlan(routeexact)
    @test !Arrow._needsarrowtypesroute(leaf, exactplan)
    @test length(exactplan.routes) == 1
    @test Arrow._remaining(routeexact) == 0
    @test !Arrow._needsarrowtypesroute(leaf, exactplan)
    @test Arrow._remaining(routeexact) == 0

    intfield, intdata = ARROWTYPES_TEST_AC.fromjulia("integer", Int64[])
    textfield, textdata = ARROWTYPES_TEST_AC.fromjulia("text", String[])
    uniontype = ARROWTYPES_TEST_AC.UnionType(ARROWTYPES_TEST_AC.DenseMode, Int8[42, 17])
    unionfield = ARROWTYPES_TEST_AC.Field(
        "union",
        uniontype;
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(
            "JuliaLang.ArrowTests.RouteBudgetProbe",
        ),
        children=[intfield, textfield],
    )
    uniondata = ARROWTYPES_TEST_AC.ArrayData(
        uniontype,
        0,
        [ARROWTYPES_TEST_AC._databuffer(Int8[]), ARROWTYPES_TEST_AC._databuffer(Int32[])];
        children=[intdata, textdata],
        nullcount=0,
    )
    emptycolumnbytes = ARROWTYPES_TEST_AC._materializedvectorbytes(Any, 0)
    multibatchbudget = Arrow.AllocationBudget(
        plancontainerbytes + 2 * routeentrybytes + 2 * emptycolumnbytes,
    )
    multibatchplan = Arrow._ArrowTypesRoutePlan(multibatchbudget)
    # The same schema Field crosses both batch materializations. The exact
    # budget admits one extension-presence entry, one route entry, and two
    # empty result vectors only.
    @test isempty(Arrow._batchcolumn(unionfield, uniondata, multibatchplan))
    @test isempty(Arrow._batchcolumn(unionfield, uniondata, multibatchplan))
    @test length(multibatchplan.routes) == 1
    @test Arrow._remaining(multibatchbudget) == 0

    targetprobe = Arrow.AllocationBudget(1_000_000)
    targetbefore = Arrow._remaining(targetprobe)
    Arrow._chargedictentry!(targetprobe, ARROWTYPES_TEST_AC.Field, Any, "test target cache")
    targetentrybytes = targetbefore - Arrow._remaining(targetprobe)
    targetexact = Arrow.AllocationBudget(contextcontainerbytes + targetentrybytes)
    targetctx = Arrow._ArrowTypesContext(warn=false, budget=targetexact)
    @test Arrow._arrowtypestarget(targetctx, leaf) == (false, nothing, nothing)
    @test Arrow._remaining(targetexact) == 0
    @test Arrow._arrowtypestarget(targetctx, leaf) == (false, nothing, nothing)
    @test Arrow._remaining(targetexact) == 0

    targetzero = Arrow._ArrowTypesContext(warn=false)
    targetzero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypestarget(targetzero, leaf)
    @test isempty(targetzero.targets)

    logicalzero = Arrow._ArrowTypesContext(warn=false)
    logicalzero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypeslogicaleltype(
        logicalzero,
        leaf,
    )
    @test isempty(logicalzero.logicaltypes)

    publiczero = Arrow._ArrowTypesContext(warn=false)
    publiczero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypespubliceltype(publiczero, leaf)
    @test isempty(publiczero.publictypes)

    storagezero = Arrow._ArrowTypesContext(warn=false)
    storagezero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypesstoragetype!(
        storagezero,
        ArrowTypesTestID,
    )
    @test isempty(storagezero.storage)

    dictchildren = ARROWTYPES_TEST_AC.Field[
        ARROWTYPES_TEST_AC.Field("left", ARROWTYPES_TEST_AC.IntType(64, true)),
        ARROWTYPES_TEST_AC.Field("right", ARROWTYPES_TEST_AC.Utf8Type(false)),
    ]
    dictfield = ARROWTYPES_TEST_AC.Field(
        "dict",
        ARROWTYPES_TEST_AC.DictionaryType(
            ARROWTYPES_TEST_AC.IntType(32, true),
            ARROWTYPES_TEST_AC.StructType(),
            false,
        );
        nullable=false,
        metadata=arrowtypes_test_extension_metadata(ARROWTYPES_TEST_POINT_NAME),
        children=dictchildren,
    )
    dicttype = dictfield.type
    plainvaluefield = Arrow._arrowtypesdictvaluefield(dictfield, dicttype)
    metadatavaluefield =
        Arrow._arrowtypesdictvaluefield(dictfield, dicttype; retainmetadata=true)
    publicvaluefield = Arrow._arrowtypesdictvaluefield(dictfield, dicttype; nullable=false)
    @test plainvaluefield.children === dictfield.children
    @test metadatavaluefield.children === dictfield.children
    @test publicvaluefield.children === dictfield.children
    @test plainvaluefield.metadata === nothing
    @test metadatavaluefield.metadata === dictfield.metadata
    @test plainvaluefield.nullable
    @test !publicvaluefield.nullable

    fieldprobe = Arrow.AllocationBudget(1_000_000)
    fieldbefore = Arrow._remaining(fieldprobe)
    Arrow._arrowtypesdictvaluefield(dictfield, dicttype; budget=fieldprobe)
    fieldobjectbytes = fieldbefore - Arrow._remaining(fieldprobe)
    @test fieldobjectbytes > 0
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypesdictvaluefield(
        dictfield,
        dicttype;
        budget=Arrow.AllocationBudget(fieldobjectbytes - 1),
    )
    dictentryprobe = Arrow.AllocationBudget(1_000_000)
    dictentrybefore = Arrow._remaining(dictentryprobe)
    Arrow._chargedictentry!(
        dictentryprobe,
        ARROWTYPES_TEST_AC.Field,
        ARROWTYPES_TEST_AC.Field,
        "test dictionary-field cache",
    )
    dictentrybytes = dictentrybefore - Arrow._remaining(dictentryprobe)
    cachedfieldshort = Arrow._ArrowTypesContext(warn=false)
    cachedfieldshort.budget = Arrow.AllocationBudget(dictentrybytes + fieldobjectbytes - 1)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypesdictvaluefield(
        cachedfieldshort,
        dictfield,
        dicttype,
    )
    @test isempty(cachedfieldshort.dictionaryfields)
    cachedfieldexact = Arrow._ArrowTypesContext(warn=false)
    cachedfieldexact.budget = Arrow.AllocationBudget(dictentrybytes + fieldobjectbytes)
    cachedvaluefield =
        Arrow._arrowtypesdictvaluefield(cachedfieldexact, dictfield, dicttype)
    @test cachedvaluefield.children === dictfield.children
    @test Arrow._remaining(cachedfieldexact.budget) == 0
    @test Arrow._arrowtypesdictvaluefield(cachedfieldexact, dictfield, dicttype) ===
          cachedvaluefield
    @test Arrow._remaining(cachedfieldexact.budget) == 0

    dictzero = Arrow._ArrowTypesContext(warn=false)
    dictzero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypesdictvaluefield(
        dictzero,
        dictfield,
        dicttype,
    )
    @test isempty(dictzero.dictionaryfields)

    metadatazero = Arrow._ArrowTypesContext(warn=false)
    metadatazero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypesdictvaluefield(
        metadatazero,
        dictfield,
        dicttype;
        retainmetadata=true,
    )
    @test isempty(metadatazero.metadata_dictionaryfields)

    publicdictzero = Arrow._ArrowTypesContext(warn=false)
    publicdictzero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypespublicdictvaluefield(
        publicdictzero,
        dictfield,
        dicttype,
    )
    @test isempty(publicdictzero.public_dictionaryfields)

    routedzero = Arrow._ArrowTypesRoutePlan()
    routedzero.budget = Arrow.AllocationBudget(0)
    @test_throws Arrow.AllocationLimitError Arrow._routedictionaryfield!(
        routedzero,
        dictfield,
        dicttype,
    )
    @test isempty(routedzero.dictionaryfields)

    positionalchildren = ARROWTYPES_TEST_AC.Field[
        ARROWTYPES_TEST_AC.Field(
            string(i),
            ARROWTYPES_TEST_AC.IntType(64, true);
            nullable=false,
        ) for i = 1:1_024
    ]
    positional = ARROWTYPES_TEST_AC.Field(
        "positional",
        ARROWTYPES_TEST_AC.StructType();
        nullable=false,
        children=positionalchildren,
    )
    namesprobe = Arrow.AllocationBudget(1_000_000)
    namesbefore = Arrow._remaining(namesprobe)
    expectednames = Tuple(Symbol.(string.(1:1_024)))
    @test Arrow._arrowtypesstructnames(positional, namesprobe) == expectednames
    namesbytes = namesbefore - Arrow._remaining(namesprobe)
    @test namesbytes > 0
    @test_throws Arrow.AllocationLimitError Arrow._arrowtypesstructnames(
        positional,
        Arrow.AllocationBudget(namesbytes - 1),
    )
    namescontext = Arrow._ArrowTypesContext(warn=false)
    namescontext.budget = Arrow.AllocationBudget(namesbytes + 10 * routeentrybytes)
    @test Arrow._arrowtypesstructnames(namescontext, positional) == expectednames
    namesremaining = Arrow._remaining(namescontext.budget)
    @test Arrow._arrowtypesstructnames(namescontext, positional) == expectednames
    @test Arrow._remaining(namescontext.budget) == namesremaining
    @test length(namescontext.structnames) == 1

    children = ARROWTYPES_TEST_AC.Field[
        ARROWTYPES_TEST_AC.Field(
            "child_$i",
            ARROWTYPES_TEST_AC.IntType(64, true);
            nullable=false,
        ) for i = 1:8_191
    ]
    push!(children, arrowtypes_test_marked_id_field("last"))
    wide = ARROWTYPES_TEST_AC.Field(
        "wide",
        ARROWTYPES_TEST_AC.StructType();
        nullable=false,
        children,
    )
    widebudget = Arrow.AllocationBudget(32 * routeentrybytes - 1)
    wideplan = Arrow._ArrowTypesRoutePlan()
    wideplan.budget = widebudget
    @test_throws Arrow.AllocationLimitError Arrow._needsarrowtypesroute(wide, wideplan)
    @test length(wideplan.routes) == 30
    @test !haskey(wideplan.routes, wide)
    @test Arrow._remaining(widebudget) == routeentrybytes - 1

    presenceentries = length(children) + 1
    presenceprobe =
        Arrow.AllocationBudget(plancontainerbytes + presenceentries * routeentrybytes)
    presenceplan = Arrow._ArrowTypesRoutePlan(presenceprobe)
    presencebefore = Arrow._remaining(presenceprobe)
    @test Arrow._hasarrowtypesextension(wide, presenceplan)
    presenceafter = Arrow._remaining(presenceprobe)
    @test presenceafter < presencebefore
    @test Arrow._hasarrowtypesextension(wide, presenceplan)
    @test Arrow._remaining(presenceprobe) == presenceafter
    @test length(presenceplan.extensions) == presenceentries
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

    nullvaluefield, nullpool = ARROWTYPES_TEST_AC.fromjulia("x", Missing[missing])
    nulltype = ARROWTYPES_TEST_AC.DictionaryType(
        ARROWTYPES_TEST_AC.IntType(32, true),
        nullvaluefield.type,
        false,
    )
    nullfield = ARROWTYPES_TEST_AC.Field(
        "x",
        nulltype;
        nullable=true,
        metadata=[
            "ARROW:extension:name" => "JuliaLang.ArrowTests.UnknownNull",
            "ARROW:extension:metadata" => "opaque",
        ],
    )
    nulldata = Arrow._dictbatch(nullfield, Union{Missing,Int64}[0, missing], nullpool)
    nullschema = ARROWTYPES_TEST_AC.Schema([nullfield])
    nullbatch = ARROWTYPES_TEST_AC.RecordBatch(nullschema, [nulldata], 2)
    nullbytes = Arrow.writestream(nullschema, [nullbatch])
    nulltable =
        @test_logs (:warn, r"unsupported .*extension.*UnknownNull") Arrow.Table(nullbytes)
    @test isequal(nulltable.x, Missing[missing, missing])
    @test eltype(nulltable.x) === Missing
    for source in (nulltable, Arrow.Stream(nullbytes)), file in (false, true)
        err = try
            arrowtypes_test_bytes(source; file)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("unregistered", sprint(showerror, err))
        @test occursin("Dictionary<Null>", sprint(showerror, err))
    end

    nonnullfield = ARROWTYPES_TEST_AC.Field(
        "x",
        nulltype;
        nullable=false,
        metadata=collect(Pair{String,String}, nullfield.metadata),
    )
    nonnulldata = Arrow._dictbatch(nonnullfield, Int64[0, 0], nullpool)
    nonnullschema = ARROWTYPES_TEST_AC.Schema([nonnullfield])
    nonnullbatch = ARROWTYPES_TEST_AC.RecordBatch(nonnullschema, [nonnulldata], 2)
    nonnullbytes = Arrow.writestream(nonnullschema, [nonnullbatch])
    for source in (Arrow.Table(nonnullbytes), Arrow.Stream(nonnullbytes)),
        file in (false, true)

        output = arrowtypes_test_bytes(source; file)
        table = Arrow.Table(output)
        @test isequal(table.x, Missing[missing, missing])
        _, batches = arrowtypes_test_core_parts(output)
        @test all(ARROWTYPES_TEST_AC.isvalid_at(only(batches).columns[1], i) for i = 1:2)
    end

    function unknownnulldictionary(name, extension)
        valuefield, pool = ARROWTYPES_TEST_AC.fromjulia(name, Missing[missing])
        t = ARROWTYPES_TEST_AC.DictionaryType(
            ARROWTYPES_TEST_AC.IntType(32, true),
            valuefield.type,
            false,
        )
        field = ARROWTYPES_TEST_AC.Field(
            name,
            t;
            nullable=true,
            metadata=[
                "ARROW:extension:name" => extension,
                "ARROW:extension:metadata" => "opaque",
            ],
        )
        data = Arrow._dictbatch(field, Union{Missing,Int64}[0, missing], pool)
        return field, data
    end

    nested = Any[]

    structchild, structchilddata =
        unknownnulldictionary("d", "JuliaLang.ArrowTests.UnknownNullStruct")
    structtype = ARROWTYPES_TEST_AC.StructType()
    structfield = ARROWTYPES_TEST_AC.Field(
        "value",
        structtype;
        nullable=false,
        children=[structchild],
    )
    structdata = ARROWTYPES_TEST_AC.ArrayData(
        structtype,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[structchilddata],
        nullcount=0,
    )
    push!(
        nested,
        (
            "Struct",
            structfield,
            structdata,
            Any[Pair{String,Any}["d" => missing], Pair{String,Any}["d" => missing]],
        ),
    )

    listchild, listchilddata =
        unknownnulldictionary("item", "JuliaLang.ArrowTests.UnknownNullList")
    listtype = ARROWTYPES_TEST_AC.ListType(false)
    listfield =
        ARROWTYPES_TEST_AC.Field("value", listtype; nullable=false, children=[listchild])
    listdata = ARROWTYPES_TEST_AC.ArrayData(
        listtype,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 1, 2])];
        children=[listchilddata],
        nullcount=0,
    )
    push!(nested, ("List", listfield, listdata, Any[Missing[missing], Missing[missing]]))

    mapvalue, mapvaluedata =
        unknownnulldictionary("value", "JuliaLang.ArrowTests.UnknownNullMap")
    mapkey, mapkeydata = ARROWTYPES_TEST_AC.fromjulia("key", Int32[1, 2])
    entriesfield = ARROWTYPES_TEST_AC.Field(
        "entries",
        ARROWTYPES_TEST_AC.StructType();
        nullable=false,
        children=[mapkey, mapvalue],
    )
    entriesdata = ARROWTYPES_TEST_AC.ArrayData(
        ARROWTYPES_TEST_AC.StructType(),
        2,
        [ARROWTYPES_TEST_AC.BufferSlice()];
        children=[mapkeydata, mapvaluedata],
        nullcount=0,
    )
    maptype = ARROWTYPES_TEST_AC.MapType(false)
    mapfield =
        ARROWTYPES_TEST_AC.Field("value", maptype; nullable=false, children=[entriesfield])
    mapdata = ARROWTYPES_TEST_AC.ArrayData(
        maptype,
        2,
        [ARROWTYPES_TEST_AC.BufferSlice(), ARROWTYPES_TEST_AC._databuffer(Int32[0, 1, 2])];
        children=[entriesdata],
        nullcount=0,
    )
    push!(
        nested,
        (
            "Map",
            mapfield,
            mapdata,
            Any[Pair{Any,Any}[Int32(1) => missing], Pair{Any,Any}[Int32(2) => missing]],
        ),
    )

    reevalue, reevaludata =
        unknownnulldictionary("values", "JuliaLang.ArrowTests.UnknownNullREE")
    runfield, rundata = ARROWTYPES_TEST_AC.fromjulia("run_ends", Int32[1, 2])
    reetype = ARROWTYPES_TEST_AC.RunEndEncodedType()
    reefield = ARROWTYPES_TEST_AC.Field(
        "value",
        reetype;
        nullable=false,
        children=[runfield, reevalue],
    )
    reedata = ARROWTYPES_TEST_AC.ArrayData(
        reetype,
        2,
        ARROWTYPES_TEST_AC.BufferSlice[];
        children=[rundata, reevaludata],
        nullcount=0,
    )
    push!(nested, ("RunEndEncoded", reefield, reedata, Missing[missing, missing]))

    for (label, field, data, expected) in nested
        schema = ARROWTYPES_TEST_AC.Schema([field])
        batch = ARROWTYPES_TEST_AC.RecordBatch(schema, [data], 2)
        for file in (false, true)
            bytes =
                file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
            # The top-level fallback above verifies the warning contract.
            # These cases isolate recursive marker consumption.
            table = @test_logs min_level = Base.CoreLogging.Error Arrow.Table(bytes)
            @testset "$label ($(file ? "file" : "stream"))" begin
                @test isequal(table.value, expected)
                @test !occursin("_ArrowTypesRoutedNull", sprint(show, table.value))
            end
        end
    end
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
        bytes = file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
        table =
            @test_logs (:warn, r"unsupported .*extension.*UnknownDuplicateStruct") Arrow.Table(
                bytes,
            )
        @test table.duplicate == expected
        @test eltype(table.duplicate) === Vector{Pair{String,Any}}
    end
end

@testset "Tables.Scan public-domain values" begin
    ids = ArrowTypesTestID.(Int64[1, 2, 3, 2])
    labels = ["one", "two-a", "three", "two-b"]
    bytes = arrowtypes_test_bytes((id=ids, label=labels); file=true)

    selected = Arrow.Table(bytes; scan=Tables.Scan(select=(:id,)))
    @test selected.id == ids
    @test eltype(selected.id) === ArrowTypesTestID

    for emptyscan in (
        Tables.Scan(select=(:id => ArrowTypesTestID,), limit=0),
        Tables.Scan(select=(:id => ArrowTypesTestID,), offset=99),
    )
        emptyselected = Arrow.Table(bytes; scan=emptyscan)
        @test isempty(emptyselected.id)
        @test eltype(emptyselected.id) === ArrowTypesTestID
        @test collect(Tables.columnnames(emptyselected)) == [:id]
        fields = getfield(emptyselected, :schema).fields
        @test length(fields) == 1
        if length(fields) == 1
            @test fields[1].metadata !== nothing
            @test any(
                kv ->
                    first(kv) == "ARROW:extension:name" &&
                    last(kv) == String(ARROWTYPES_TEST_ID_NAME),
                fields[1].metadata,
            )
        end
    end

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
    paritybytes = arrowtypes_test_bytes((id=parityids, row=Int32[1, 2, 3, 4, 5]); file=true)
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
    nestedempty =
        Arrow.Table(nestedbytes; scan=Tables.Scan(select=(:structrow, :listrow), limit=0))
    @test isempty(nestedempty.structrow) && isempty(nestedempty.listrow)
    @test eltype(nestedempty.structrow) === eltype(nestedfull.structrow)
    @test eltype(nestedempty.listrow) === eltype(nestedfull.listrow)
    nestedemptyfields = getfield(nestedempty, :schema).fields
    nestedfullfields = getfield(nestedfull, :schema).fields
    @test length(nestedemptyfields) == 2
    @test length(nestedfullfields) == 3
    @test all(
        arrowtypes_test_field_contract_equal(emptyfield, fullfield) for
        (emptyfield, fullfield) in zip(nestedemptyfields, nestedfullfields[1:2])
    )
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

    unionvalues = ArrowTypesTestLogicalUnion[
        ArrowTypesTestLogicalUnion(Int64(1)),
        ArrowTypesTestLogicalUnion("two"),
    ]
    unionbytes = arrowtypes_test_bytes((value=unionvalues,); file=true)
    unionfull = Arrow.Table(unionbytes)
    for emptyscan in (Tables.Scan(limit=0), Tables.Scan(offset=99))
        unionempty = Arrow.Table(unionbytes; scan=emptyscan)
        @test isempty(unionempty.value)
        @test eltype(unionempty.value) === ArrowTypesTestLogicalUnion
        @test arrowtypes_test_field_contract_equal(
            only(getfield(unionempty, :schema).fields),
            only(getfield(unionfull, :schema).fields),
        )
    end

    stringvalues = arrowtypes_test_pointer_string.(["alpha", "beta", "alpine"])
    stringbytes = arrowtypes_test_bytes((value=stringvalues, row=Int32[1, 2, 3]); file=true)
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
        scan=Tables.Scan(select=(:value, :row), filter=Tables.isnull(Tables.col(:value))),
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
