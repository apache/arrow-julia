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

# JuliaC `--trim=safe` workload for ArrowCore (compiled + executed by
# core/test/trim_compile_tests.jl, following the trim harness convention
# from JSON/HTTP/Reseau/StructUtils). Everything reachable from `main` must
# be free of dynamic dispatch: this file is the executable definition of
# ArrowCore's trim-safe surface.

include(joinpath(@__DIR__, "..", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore

function checked(cond::Bool, msg::String)::Nothing
    cond || error(msg)
    return nothing
end

function exercise_regions()::Nothing
    v = Int64[1, 2, 3, 4]
    r = heapregion(v)
    b = BufferSlice(r, 0, 32)
    checked(AC.loadat(b, Int64, Int64(0)) == 1, "heap load failed")
    checked(AC.loadat(b, Int64, Int64(24)) == 4, "heap tail load failed")
    sub = AC.subslice(b, 8, 16)
    checked(AC.loadat(sub, Int64, Int64(0)) == 2, "subslice load failed")
    notes = ReleaseCounter()
    bytes = UInt8[0x7f]
    fr = GC.@preserve bytes AC.OwnerRegion(Ptr{UInt8}(pointer(bytes)), 1,
        AC.Foreign; root=bytes, releasefn=NotifyRelease(notes))
    checked(withguard(() -> 1, fr) == 1, "guard failed")
    checked(forceclose!(fr), "forceclose failed")
    checked(notes[] == 1, "release action did not run exactly once")
    caught = false
    try
        withguard(() -> 1, fr)
    catch e
        caught = e isa InvalidatedError
    end
    checked(caught, "closed region accepted a guard")
    return nothing
end

function exercise_mmap(dir::String)::Nothing
    path = joinpath(dir, "trim.bin")
    write(path, UInt8[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88])
    r = mmapregion(path)
    b = BufferSlice(r, 0, 8)
    checked(AC.loadat(b, UInt32, Int64(4)) == 0x88776655, "mmap load failed")
    checked(forceclose!(r), "mmap close failed")
    caught = false
    try
        AC.loadat(b, UInt8, Int64(0))
    catch e
        caught = e isa InvalidatedError
    end
    checked(caught, "closed mapping still readable")
    return nothing
end

function exercise_values()::Nothing
    b = batch((
        xs=Int64[1, 2, 3],
        ys=[1.5, missing, 3.5],
        flags=[true, missing, false],
        strs=["a", "", missing],
        lists=[[1, 2], missing, Int64[]],
    ))
    checked(b.nrows == 3, "batch row count wrong")
    for (f, col) in zip(b.schema.fields, b.columns)
        validate_structural(f, col)
        validate_semantic(f, col)
    end
    f1, c1 = b.schema.fields[1], b.columns[1]
    checked(getvalue(f1, c1, 2) === Int64(2), "int getvalue failed")
    checked(nullcount(b.columns[2]) == 1, "nullcount failed")
    m1 = materialize(f1, c1)
    checked(length(m1) == 3, "materialize length wrong")
    f4, c4 = b.schema.fields[4], b.columns[4]
    checked(getvalue(f4, c4, 1) == "a", "string getvalue failed")
    checked(getvalue(f4, c4, 3) === missing, "missing string wrong")
    f5, c5 = b.schema.fields[5], b.columns[5]
    v5 = getvalue(f5, c5, 1)
    checked(v5 !== missing && length(v5) == 2, "list getvalue failed")
    sf, sd = AC.fromjulia_struct("st", (a=Int64[7, 8], b=["x", "y"]))
    validate_structural(sf, sd)
    sv = getvalue(sf, sd, 2)
    checked(sv !== missing, "struct getvalue missing")
    df, dd = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, missing, 0])
    validate_structural(df, dd)
    validate_semantic(df, dd)
    checked(getvalue(df, dd, 4) == "lo", "dictionary getvalue failed")
    checked(getvalue(df, dd, 3) === missing, "dictionary null failed")
    return nothing
end

function exercise_validation_errors()::Nothing
    t = IntType(64, true)
    f = Field("x", t)
    short = AC._databuffer(Int64[1])
    d = AC.ArrayData(t, 3, [BufferSlice(), short])
    caught = false
    try
        validate_structural(f, d)
    catch e
        caught = e isa ValidationError
    end
    checked(caught, "short data buffer accepted")
    ut = Utf8Type(false)
    uf = Field("s", ut)
    offs = AC._databuffer(Int32[0, 2, 1, 3])
    data = AC._databuffer(UInt8[0x61, 0x62, 0x63])
    ud = AC.ArrayData(ut, 3, [BufferSlice(), offs, data])
    validate_structural(uf, ud)
    caught = false
    try
        validate_semantic(uf, ud)
    catch e
        caught = e isa ValidationError
    end
    checked(caught, "non-monotonic offsets accepted")
    return nothing
end

function run_trim_workload()::Nothing
    exercise_regions()
    mktempdir() do dir
        exercise_mmap(dir)
    end
    exercise_values()
    exercise_validation_errors()
    return nothing
end

function @main(args::Vector{String})::Cint
    _ = args
    run_trim_workload()
    return 0
end

Base.Experimental.entrypoint(main, (Vector{String},))
