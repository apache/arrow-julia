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

@testset "loading common data packages keeps the precompiled code" begin
    # Packages that add methods to generic functions invalidate precompiled code
    # that was inferred with abstract types, and the next read or write compiles
    # again. Run in a fresh process, where the code is the package image's.
    # CategoricalArrays and TimeZones (which loads InlineStrings) add such methods;
    # so do DataFrames and its dependencies, whose kinds of methods the script
    # adds itself: `eltype` of a vector type (DataFrames' `RepeatedVector`), a
    # dictionary's `pairs`, `merge!`, and key iteration (DataStructures), and a
    # string's code units (LaTeXStrings).
    script = raw"""
        using Arrow
        entries = ccall(:jl_debug_method_invalidation, Any, (Cint,), 1)
        using CategoricalArrays, TimeZones
        struct RepeatedRows{T} <: AbstractVector{T} end
        Base.eltype(::Type{RepeatedRows{T}}) where {T} = T
        Base.size(::RepeatedRows) = (0,)
        struct SortedPairs{K,V} <: AbstractDict{K,V} end
        Base.pairs(d::SortedPairs) = d
        Base.merge!(d::SortedPairs, others...) = d
        Base.iterate(::Base.KeySet{<:Any,<:SortedPairs}, state) = nothing
        Base.keys(d::SortedPairs, prefix) = d
        struct Markup <: AbstractString end
        Base.codeunits(::Markup) = UInt8[]
        Base.eachindex(::Markup) = 1:0
        ccall(:jl_debug_method_invalidation, Any, (Cint,), 0)
        instance(x) =
            x isa Core.MethodInstance ? x :
            x isa Core.CodeInstance ? (x.def isa Core.MethodInstance ? x.def : x.def.def) :
            nothing
        invalidated = Set{Core.MethodInstance}()
        for x in entries
            mi = instance(x)
            mi isa Core.MethodInstance &&
                mi.def isa Method &&
                Base.moduleroot(mi.def.module) in (Arrow, Arrow.ArrowTypes) &&
                push!(invalidated, mi)
        end
        foreach(mi -> println(stderr, mi), Iterators.take(invalidated, 20))
        print(length(invalidated))
        """
    cmd = `$(Base.julia_cmd()) --startup-file=no --project=$(Base.active_project()) -e $script`
    invalidated = parse(Int, read(cmd, String))
    # Over a thousand were invalidated when the reader's and writer's kernels were
    # also inferred for abstract types; the bound leaves room for small changes in
    # those packages. A prerelease Julia adds its own Base code to what they
    # invalidate, so there the count is only reported.
    if isempty(VERSION.prerelease)
        @test invalidated < 200
    else
        @info "Arrow method instances invalidated on Julia $VERSION: $invalidated"
    end
end
