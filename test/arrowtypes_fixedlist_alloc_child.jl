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

# Keep the allocation assertions in a small process. The full ArrowTypes suite
# deliberately creates many heap-backed regions; finalizer work from unrelated
# cases would make a process-global allocation counter measure the wrong seam.

using Arrow
using ArrowTypes
using Test

const AC = Arrow.ArrowCore

struct FixedListAllocID
    value::Int32
end

const FIXED_LIST_ALLOC_NAME = Symbol("JuliaLang.ArrowTests.FixedListAllocID")
const FIXED_LIST_ALLOC_CALLS = Ref(0)

ArrowTypes.ArrowType(::Type{FixedListAllocID}) = Int32
function ArrowTypes.toarrow(value::FixedListAllocID)
    FIXED_LIST_ALLOC_CALLS[] += 1
    return value.value
end
ArrowTypes.arrowname(::Type{FixedListAllocID}) = FIXED_LIST_ALLOC_NAME
ArrowTypes.JuliaType(::Val{FIXED_LIST_ALLOC_NAME}, storage, metadata) = FixedListAllocID
ArrowTypes.fromarrow(::Type{FixedListAllocID}, value::Int32) = FixedListAllocID(value)

struct FixedListAllocValue{T}
    value::T
end

const FIXED_LIST_ALLOC_BOOL_NAME = Symbol("JuliaLang.ArrowTests.FixedListAllocBool")
const FIXED_LIST_ALLOC_INT_NAME = Symbol("JuliaLang.ArrowTests.FixedListAllocInt")
const FIXED_LIST_ALLOC_STRING_NAME = Symbol("JuliaLang.ArrowTests.FixedListAllocString")
const FIXED_LIST_ALLOC_VALUE_CALLS = Ref(0)

ArrowTypes.ArrowType(::Type{FixedListAllocValue{T}}) where {T} = T
function ArrowTypes.toarrow(value::FixedListAllocValue)
    FIXED_LIST_ALLOC_VALUE_CALLS[] += 1
    return value.value
end
ArrowTypes.arrowname(::Type{FixedListAllocValue{Bool}}) = FIXED_LIST_ALLOC_BOOL_NAME
ArrowTypes.arrowname(::Type{FixedListAllocValue{Int64}}) = FIXED_LIST_ALLOC_INT_NAME
ArrowTypes.arrowname(::Type{FixedListAllocValue{String}}) = FIXED_LIST_ALLOC_STRING_NAME
ArrowTypes.JuliaType(::Val{FIXED_LIST_ALLOC_BOOL_NAME}, storage, metadata) =
    FixedListAllocValue{Bool}
ArrowTypes.JuliaType(::Val{FIXED_LIST_ALLOC_INT_NAME}, storage, metadata) =
    FixedListAllocValue{Int64}
ArrowTypes.JuliaType(::Val{FIXED_LIST_ALLOC_STRING_NAME}, storage, metadata) =
    FixedListAllocValue{String}
ArrowTypes.fromarrow(::Type{FixedListAllocValue{T}}, value::T) where {T} =
    FixedListAllocValue{T}(value)

struct FixedListAllocUnion
    value::Union{Vector{Int64},String}
end

const FIXED_LIST_ALLOC_UNION_NAME = Symbol("JuliaLang.ArrowTests.FixedListAllocUnion")

ArrowTypes.ArrowType(::Type{FixedListAllocUnion}) = Union{Vector{Int64},String}
ArrowTypes.toarrow(value::FixedListAllocUnion) = value.value
ArrowTypes.arrowname(::Type{FixedListAllocUnion}) = FIXED_LIST_ALLOC_UNION_NAME
ArrowTypes.JuliaType(::Val{FIXED_LIST_ALLOC_UNION_NAME}, storage, metadata) =
    FixedListAllocUnion
ArrowTypes.fromarrow(::Type{FixedListAllocUnion}, value::AbstractVector) =
    FixedListAllocUnion(Int64[value...])
ArrowTypes.fromarrow(::Type{FixedListAllocUnion}, value::AbstractString) =
    FixedListAllocUnion(String(value))

extensionmetadata() = Pair{String,String}[
    "ARROW:extension:name" => String(FIXED_LIST_ALLOC_NAME),
    "ARROW:extension:metadata" => "",
]

function registeredhidden(listsize::Int, missingrows::Int)
    child =
        AC.Field("item", AC.IntType(32, true); nullable=false, metadata=extensionmetadata())
    field =
        AC.Field("value", AC.FixedSizeListType(listsize); nullable=true, children=[child])
    T = Union{Missing,Vector{FixedListAllocID}}
    values = T[FixedListAllocID.(Int32.(1:listsize))]
    append!(values, fill(missing, missingrows))

    FIXED_LIST_ALLOC_CALLS[] = 0
    rebuiltfield, rebuiltdata =
        Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, rebuiltdata)
    FIXED_LIST_ALLOC_CALLS[] == listsize || error("hidden slots called toarrow")
    length(only(rebuiltdata.children)) == (missingrows + 1) * listsize ||
        error("fixed-list child length changed")
    return nothing
end

allocsample(::Type{Bool}) = true
allocsample(::Type{Int64}) = Int64(7)
allocsample(::Type{String}) = "x"

function fixedlistallocfield(::Type{S}, nullable::Bool; registered::Bool) where {S}
    sample = allocsample(S)
    probe = nullable ? Union{Missing,S}[sample] : S[sample]
    field, _ = AC.fromjulia("item", probe)
    registered || return field
    name = ArrowTypes.arrowname(FixedListAllocValue{S})
    return AC.Field(
        field.name,
        field.type;
        nullable=field.nullable,
        metadata=Pair{String,String}[
            "ARROW:extension:name" => String(name),
            "ARROW:extension:metadata" => "",
        ],
        children=collect(AC.Field, field.children),
    )
end

function maskedleafallocation(
    ::Type{S},
    listsize::Int,
    missingrows::Int;
    nullable::Bool,
    registered::Bool,
) where {S}
    child = fixedlistallocfield(S, nullable; registered)
    field =
        AC.Field("value", AC.FixedSizeListType(listsize); nullable=true, children=[child])
    sample = allocsample(S)
    if registered
        L = FixedListAllocValue{S}
        values = Union{Missing,Vector{L}}[fill(L(sample), listsize)]
    else
        E = nullable ? Union{Missing,S} : S
        values = Union{Missing,Vector{E}}[fill(convert(E, sample), listsize)]
    end
    append!(values, fill(missing, missingrows))

    FIXED_LIST_ALLOC_VALUE_CALLS[] = 0
    rebuiltfield, rebuiltdata =
        Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, rebuiltdata)
    childfield = only(rebuiltfield.children)
    childdata = only(rebuiltdata.children)
    registered &&
        FIXED_LIST_ALLOC_VALUE_CALLS[] != listsize &&
        error("hidden slots called toarrow")
    length(childdata) == (missingrows + 1) * listsize ||
        error("fixed-list child length changed")
    firstvalue = AC.getvalue(childfield, childdata, Int64(1))
    firstvalue == sample || error("visible payload changed")
    if nullable && missingrows > 0
        AC.getvalue(childfield, childdata, Int64(listsize + 1)) === missing ||
            error("hidden nullable child slot became valid")
    end
    return nothing
end

function allocationguard(
    ::Type{S};
    nullable::Bool,
    registered::Bool,
    listsize::Int=50_000,
) where {S}
    maskedleafallocation(S, 2, 1; nullable, registered)
    maskedleafallocation(S, listsize, 1; nullable, registered)
    maskedleafallocation(S, listsize, 49; nullable, registered)
    GC.gc()
    few = @allocated maskedleafallocation(S, listsize, 1; nullable, registered)
    GC.gc()
    many = @allocated maskedleafallocation(S, listsize, 49; nullable, registered)
    slots = 48 * listsize
    physicaldelta = if S === Bool
        (nullable ? 2 : 1) * cld(slots, 8)
    elseif S === String
        4 * slots + (nullable ? cld(slots, 8) : 0)
    else
        sizeof(S) * slots + (nullable ? cld(slots, 8) : 0)
    end
    @test many <= few + (7 * physicaldelta) ÷ 4 + 1_000_000
end

function allocationdelta(f, small::Int, large::Int, allowance::Int)
    f(2)
    f(small)
    f(large)
    GC.gc()
    smallallocation = @allocated f(small)
    GC.gc()
    largeallocation = @allocated f(large)
    @test largeallocation <= smallallocation + allowance
    return nothing
end

function emptyunion(listsize::Int)
    itemfield, itemdata = AC.fromjulia("item", Int64[])
    fixedtype = AC.FixedSizeListType(listsize)
    fixedfield = AC.Field("huge", fixedtype; nullable=false, children=[itemfield])
    fixeddata =
        AC.ArrayData(fixedtype, 0, [AC.BufferSlice()]; children=[itemdata], nullcount=0)
    textfield, textdata = AC.fromjulia("text", String[])
    uniontype = AC.UnionType(AC.SparseMode, Int8[42, 17])
    field = AC.Field(
        "value",
        uniontype;
        nullable=false,
        metadata=Pair{String,String}[
            "ARROW:extension:name" => String(FIXED_LIST_ALLOC_UNION_NAME),
            "ARROW:extension:metadata" => "",
        ],
        children=[fixedfield, textfield],
    )
    data = AC.ArrayData(
        uniontype,
        0,
        [AC._databuffer(Int8[])];
        children=[fixeddata, textdata],
        nullcount=0,
    )
    return field, data
end

function readempty(listsize::Int)
    field, data = emptyunion(listsize)
    schema = AC.Schema([field])
    batch = AC.RecordBatch(schema, [data], 0)
    isempty(Arrow.Table(Arrow.writestream(schema, [batch])).value) ||
        error("empty Union materialized a row")
    return nothing
end

function directmissing(listsize::Int)
    child = AC.Field("item", AC.NullType(); nullable=true)
    fixedtype = AC.FixedSizeListType(listsize)
    field = AC.Field("value", fixedtype; nullable=true, children=[child])
    values = Union{Missing,Vector{Missing}}[missing]
    rebuiltfield, rebuiltdata =
        Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, rebuiltdata)
    return nothing
end

function directmixed(listsize::Int)
    child = AC.Field("item", AC.NullType(); nullable=true)
    fixedtype = AC.FixedSizeListType(listsize)
    field = AC.Field("value", fixedtype; nullable=true, children=[child])
    values = Union{Missing,Vector{Missing}}[fill(missing, listsize)]
    append!(values, fill(missing, 49))
    rebuiltfield, rebuiltdata =
        Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, rebuiltdata)
    return nothing
end

function freshmixed(listsize::Int)
    values = Union{Missing,Vector{Missing}}[fill(missing, listsize)]
    append!(values, fill(missing, 49))
    kind = ArrowTypes.FixedSizeListKind{listsize,Missing}()
    rebuiltfield, rebuiltdata = Arrow._arrowtypesfixedlistcolumn(
        "value",
        values,
        kind;
        extension_shape=false,
        context=Arrow._WriterContext("value"),
    )
    AC.validate_full(rebuiltfield, rebuiltdata)
    return nothing
end

function nullsparsefield(listsize::Int)
    child = AC.Field("item", AC.NullType(); nullable=true)
    fixed =
        AC.Field("fixed", AC.FixedSizeListType(listsize); nullable=false, children=[child])
    text = AC.Field("text", AC.Utf8Type(false); nullable=false)
    uniontype = AC.UnionType(AC.SparseMode, Int8[42, 17])
    return AC.Field("value", uniontype; nullable=false, children=[fixed, text])
end

function inactivesparse(listsize::Int)
    field = nullsparsefield(listsize)
    routed = Any[Arrow._WriterRoutedUnion(2, "text")]
    rebuiltfield, rebuiltdata =
        Arrow._constructwriterunion(field, routed, Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, rebuiltdata)
    schema = AC.Schema([rebuiltfield])
    batch = AC.RecordBatch(schema, [rebuiltdata], 1)
    Arrow.Table(Arrow.writestream(schema, [batch])).value == ["text"] ||
        error("inactive sparse fixed-list child changed the visible value")
    return nothing
end

function mixedsparse(listsize::Int)
    field = nullsparsefield(listsize)
    routed = Any[Arrow._WriterRoutedUnion(1, fill(missing, listsize))]
    append!(routed, (Arrow._WriterRoutedUnion(2, "text") for _ = 1:49))
    rebuiltfield, rebuiltdata =
        Arrow._constructwriterunion(field, routed, Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, rebuiltdata)
    return nothing
end

function generalhidden(listsize::Int)
    child = AC.Field("item", AC.IntType(64, true); nullable=false)
    field =
        AC.Field("value", AC.FixedSizeListType(listsize); nullable=true, children=[child])
    values = Any[collect(Int64, 1:listsize)]
    append!(values, fill(missing, 49))
    rebuiltfield, rebuiltdata =
        Arrow._constructpart(field, values; context=Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, rebuiltdata)
    childdata = only(rebuiltdata.children)
    length(childdata) == 50 * listsize || error("fixed-list child length changed")
    AC.getvalue(child, childdata, Int64(1)) == 1 || error("visible payload changed")
    !AC.isvalid_at(rebuiltdata, Int64(2)) || error("missing parent became valid")
    return nothing
end

function freshnative(listsize::Int)
    values = Union{Missing,Vector{Int64}}[collect(Int64, 1:listsize)]
    append!(values, fill(missing, 49))
    kind = ArrowTypes.FixedSizeListKind{listsize,Int64}()
    rebuiltfield, rebuiltdata = Arrow._arrowtypesfixedlistcolumn(
        "value",
        values,
        kind;
        extension_shape=false,
        context=Arrow._WriterContext("value"),
    )
    AC.validate_full(rebuiltfield, rebuiltdata)
    childfield = only(rebuiltfield.children)
    childdata = only(rebuiltdata.children)
    length(childdata) == 50 * listsize || error("fixed-list child length changed")
    AC.getvalue(childfield, childdata, Int64(1)) == 1 || error("visible payload changed")
    return nothing
end

function freshbool(listsize::Int)
    values = Union{Missing,Vector{Bool}}[Bool[isodd(index) for index = 1:listsize],]
    append!(values, fill(missing, 49))
    kind = ArrowTypes.FixedSizeListKind{listsize,Bool}()
    rebuiltfield, rebuiltdata = Arrow._arrowtypesfixedlistcolumn(
        "value",
        values,
        kind;
        extension_shape=false,
        context=Arrow._WriterContext("value"),
    )
    AC.validate_full(rebuiltfield, rebuiltdata)
    childfield = only(rebuiltfield.children)
    childdata = only(rebuiltdata.children)
    length(childdata) == 50 * listsize || error("fixed-list child length changed")
    AC.getvalue(childfield, childdata, Int64(1)) || error("visible payload changed")
    !AC.getvalue(childfield, childdata, Int64(2)) || error("visible payload changed")
    return nothing
end

@testset "hidden fixed-list allocation" begin
    listsize = 50_000
    registeredhidden(2, 1)
    registeredhidden(listsize, 1)
    registeredhidden(listsize, 49)

    GC.gc()
    few = @allocated registeredhidden(listsize, 1)
    GC.gc()
    many = @allocated registeredhidden(listsize, 49)
    physicaldelta = 48 * sizeof(Int32) * listsize
    @test many <= few + (7 * physicaldelta) ÷ 4 + 1_000_000

    allocationguard(Int64; nullable=false, registered=false, listsize=20_000)
    for registered in (false, true)
        allocationguard(Bool; nullable=false, registered)
        allocationguard(Bool; nullable=true, registered)
        allocationguard(Int64; nullable=true, registered, listsize=20_000)
        allocationguard(String; nullable=true, registered, listsize=20_000)
    end

    allocationdelta(readempty, 1_000, 100_000, 250_000)
    allocationdelta(directmissing, 1_000, 100_000, 250_000)
    allocationdelta(directmixed, 1_000, 20_000, 250_000)
    allocationdelta(freshmixed, 1_000, 20_000, 750_000)
    allocationdelta(inactivesparse, 1_000, 100_000, 250_000)
    allocationdelta(mixedsparse, 1_000, 20_000, 250_000)

    physicaldelta = 50 * sizeof(Int64) * (50_000 - 1_000)
    listallowance = (7 * physicaldelta) ÷ 4 + 1_000_000
    allocationdelta(generalhidden, 1_000, 50_000, listallowance)
    allocationdelta(freshnative, 1_000, 50_000, listallowance)

    boolphysicaldelta = cld(50 * 100_000, 8) - cld(50 * 1_000, 8)
    boolallowance = (7 * boolphysicaldelta) ÷ 4 + 1_000_000
    allocationdelta(freshbool, 1_000, 100_000, boolallowance)
end

println("ArrowTypes fixed-list allocation guard passed")
