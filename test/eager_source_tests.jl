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

module EagerSourceTests

using Test, Arrow, Tables
const AC = Arrow.AC
const ReleaseContext = Base.RefValue{Tuple{Vector{UInt8},Base.RefValue{Int}}}

function poison_source(p::Ptr{Cvoid})::Cvoid
    bytes, calls = (unsafe_pointer_to_objref(p)::ReleaseContext)[]
    calls[] += 1
    fill!(bytes, 0)
    return nothing
end

function eagerhandle(bytes; limits=Arrow.Limits())
    context = Ref((bytes, Ref(0)))
    cell = AC.ReleaseCell(@cfunction(poison_source, Cvoid, (Ptr{Cvoid},)), context)
    region = AC.OwnerRegion(pointer(bytes), length(bytes); root=bytes, cell)
    return Arrow.readfile(region; limits), region, context[][2]
end

@testset "facade columns outlive externally releasable sources" begin
    values = (x=Int64[11, 22], nested=[(n=Int64(1),), (n=Int64(2),)])
    for compress in (:none, :zstd), form in (:table, :scan, :iterate, :index)
        bytes = take!(Arrow.tobuffer(values; file=true, compress))
        handle, region, calls = eagerhandle(bytes)
        stream = form in (:iterate, :index) ? Arrow.Stream(handle) : nothing
        table = if form === :table
            Arrow.Table(handle)
        elseif form === :scan
            Arrow.Table(handle; select=[:x, :nested])
        elseif form === :iterate
            first(stream)
        else
            stream[1]
        end
        x, nested = table.x, table.nested
        @test x == values.x
        @test nested == values.nested
        Arrow.release!(table)
        @test calls[] == 0
        @test !(@atomic region.cell.closed)
        AC.release!(region)
        @test calls[] == 1
        @test x == values.x
        @test nested == values.nested
        @test_throws InvalidStateException table.x
        stream === nothing || @test_throws InvalidStateException first(stream)
    end
    # Detaching is part of the read's allocation allowance, before copying.
    bytes = take!(Arrow.tobuffer((x=collect(Int64, 1:10000),); file=true))
    limits = Arrow.Limits(max_total_allocated_bytes=length(bytes) - 1)
    handle, region, calls = eagerhandle(bytes; limits)
    @test_throws Arrow.AllocationLimitError Arrow.Table(handle)
    @test_throws Arrow.AllocationLimitError Arrow.Stream(handle)
    @test calls[] == 0
    @test !(@atomic region.cell.closed)
    AC.release!(region)
    @test calls[] == 1
end

end # module EagerSourceTests
