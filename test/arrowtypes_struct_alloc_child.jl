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
const STRUCT_ALLOC_WIDTH = 16
const STRUCT_ALLOC_NAMES = ntuple(i -> Symbol("child_$i"), STRUCT_ALLOC_WIDTH)

struct StructAllocID
    value::Int32
end

const STRUCT_ALLOC_ID_NAME = Symbol("JuliaLang.ArrowTests.StructAllocID")
const STRUCT_ALLOC_LOWER_CALLS = Ref(0)

ArrowTypes.ArrowType(::Type{StructAllocID}) = Int32
function ArrowTypes.toarrow(value::StructAllocID)
    STRUCT_ALLOC_LOWER_CALLS[] += 1
    return value.value
end
ArrowTypes.arrowname(::Type{StructAllocID}) = STRUCT_ALLOC_ID_NAME
ArrowTypes.JuliaType(::Val{STRUCT_ALLOC_ID_NAME}, storage, metadata) = StructAllocID
ArrowTypes.fromarrow(::Type{StructAllocID}, value::Int32) = StructAllocID(value)

const StructAllocFreshRow =
    NamedTuple{STRUCT_ALLOC_NAMES,NTuple{STRUCT_ALLOC_WIDTH,StructAllocID}}
const StructAllocRetainedRow = Vector{Pair{String,Any}}

function freshrows(n::Int)
    rows = Vector{Union{Missing,StructAllocFreshRow}}(undef, n)
    for i = 1:n
        rows[i] =
            i == 1 ?
            StructAllocFreshRow(
                ntuple(j -> StructAllocID(Int32(i + j)), STRUCT_ALLOC_WIDTH),
            ) : missing
    end
    return rows
end

function constructfresh(rows)
    STRUCT_ALLOC_LOWER_CALLS[] = 0
    field, data = Arrow._arrowtypesstructcolumn(
        "value",
        rows,
        StructAllocFreshRow;
        extension_shape=false,
        context=Arrow._WriterContext("value"),
    )
    AC.validate_full(field, data)
    visible = count(!ismissing, rows)
    STRUCT_ALLOC_LOWER_CALLS[] == STRUCT_ALLOC_WIDTH * visible ||
        error("fresh Struct child lowering did not run exactly once")
    length(data.children) == STRUCT_ALLOC_WIDTH || error("fresh Struct width changed")
    all(child -> length(child) == length(rows), data.children) ||
        error("fresh Struct child length changed")
    return nothing
end

function retainedfield(; marked::Bool=false)
    metadata =
        marked ?
        Pair{String,String}[
            "ARROW:extension:name" => String(STRUCT_ALLOC_ID_NAME),
            "ARROW:extension:metadata" => "",
        ] : nothing
    children = AC.Field[
        AC.Field(
            "child_$i",
            marked ? AC.IntType(32, true) : AC.IntType(64, true);
            nullable=false,
            metadata,
        ) for i = 1:STRUCT_ALLOC_WIDTH
    ]
    return AC.Field("value", AC.StructType(); nullable=true, children)
end

function retainedrows(n::Int; allnull::Bool)
    rows = Vector{Union{Missing,StructAllocRetainedRow}}(undef, n)
    for i = 1:n
        rows[i] =
            allnull || i % 4 != 0 ? missing :
            Pair{String,Any}["child_$j" => Int64(i + j) for j = 1:STRUCT_ALLOC_WIDTH]
    end
    return rows
end

function registeredretainedrows(n::Int)
    rows = Vector{Union{Missing,StructAllocRetainedRow}}(undef, n)
    for i = 1:n
        rows[i] =
            i % 4 == 0 ?
            Pair{String,Any}[
                "child_$j" => StructAllocID(Int32(i + j)) for j = 1:STRUCT_ALLOC_WIDTH
            ] : missing
    end
    return rows
end

function constructretained(field, rows)
    rebuiltfield, data =
        Arrow._constructpart(field, rows; context=Arrow._WriterContext("value"))
    AC.validate_full(rebuiltfield, data)
    length(data.children) == STRUCT_ALLOC_WIDTH || error("retained Struct width changed")
    all(child -> length(child) == length(rows), data.children) ||
        error("retained Struct child length changed")
    return nothing
end

function allocationdelta(f, small, large, physicaldelta::Int)
    f(small)
    f(large)
    GC.gc()
    smallallocation = @allocated f(small)
    GC.gc()
    largeallocation = @allocated f(large)
    @test largeallocation <= smallallocation + (7 * physicaldelta) ÷ 4 + 1_000_000
end

@testset "Struct child projection allocation" begin
    smalln = 2_000
    largen = 50_000
    freshsmall = freshrows(smalln)
    freshlarge = freshrows(largen)
    freshphysicaldelta =
        STRUCT_ALLOC_WIDTH * sizeof(Int32) * (largen - smalln) + cld(largen - smalln, 8)
    allocationdelta(constructfresh, freshsmall, freshlarge, freshphysicaldelta)

    field = retainedfield()
    retainedsmall = retainedrows(smalln; allnull=true)
    retainedlarge = retainedrows(largen; allnull=true)
    retainedphysicaldelta =
        STRUCT_ALLOC_WIDTH * sizeof(Int64) * (largen - smalln) + cld(largen - smalln, 8)
    allocationdelta(
        rows -> constructretained(field, rows),
        retainedsmall,
        retainedlarge,
        retainedphysicaldelta,
    )

    mixed = retainedrows(8; allnull=false)
    constructretained(field, mixed)

    registeredrows = registeredretainedrows(8)
    STRUCT_ALLOC_LOWER_CALLS[] = 0
    constructretained(retainedfield(; marked=true), registeredrows)
    @test STRUCT_ALLOC_LOWER_CALLS[] ==
          STRUCT_ALLOC_WIDTH * count(!ismissing, registeredrows)

    wrongname = copy(mixed)
    wrongname[4] = copy(wrongname[4])
    wrongname[4][7] = "wrong" => last(wrongname[4][7])
    err = try
        constructretained(field, wrongname)
        nothing
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin("retained struct child 7 is named", sprint(showerror, err))
end

println("ArrowTypes Struct allocation guard passed")
