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
# test/trim_compile_tests.jl, following the trim harness convention
# from JSON/HTTP/Reseau/StructUtils). Everything reachable from `main` must
# be free of dynamic dispatch: this file is the executable definition of
# ArrowCore's trim-safe surface.

include(joinpath(@__DIR__, "..", "src", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore
# The C data interface is part of the trim-safe surface: a trimmed binary
# that moves columns across the C seams is the canonical embedding use.
include(joinpath(@__DIR__, "..", "src", "cdata.jl"))

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
    # Validity is reachability: the region's root IS the backing vector, and
    # holding the region is what keeps the memory alive. No lifecycle state
    # exists to exercise.
    checked(r.root === v, "heap region root identity failed")
    bytes = UInt8[0x7f]
    fr = GC.@preserve bytes AC.OwnerRegion(Ptr{UInt8}(pointer(bytes)), 1;
        root=bytes)
    fb = BufferSlice(fr, 0, 1)
    checked(AC.loadat(fb, UInt8, Int64(0)) == 0x7f, "rooted raw load failed")
    # Each raw load retains its final bounds check in the constrained model.
    caught = false
    try
        AC.loadat(b, Int64, Int64(32))
    catch e
        caught = e isa BoundsError
    end
    checked(caught, "out-of-bounds load accepted")
    return nothing
end

function exercise_mmap(dir::String)::Nothing
    path = joinpath(dir, "trim.bin")
    # Explicit open/write/close: Base's `write(filename, x)` convenience
    # routes through the vararg-splatting do-block `open`, which trim cannot
    # resolve.
    io = open(path, "w")
    write(io, UInt8[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88])
    close(io)
    r = mmapregion(path)
    b = BufferSlice(r, 0, 8)
    checked(AC.loadat(b, UInt32, Int64(4)) == 0x88776655, "mmap load failed")
    root = r.root
    checked(root isa Vector{UInt8} && length(root) == 8,
        "mmap region root is not the stdlib-mapped array")
    # Deterministic release: close! unmaps NOW (the Windows delete-a-mapped-
    # file case) and later access is a clean error, not a fault.
    close!(r)
    caught = false
    try
        AC.loadat(b, UInt32, Int64(4))
    catch e
        caught = e isa InvalidStateException
    end
    checked(caught, "use after close! accepted")
    return nothing
end

function exercise_cdata()::Nothing
    f0, d = fromjulia("xs", Int64[1, 2, 3])
    f = Field("xs", f0.type; nullable=f0.nullable,
        metadata=["mk" => "mv"], children=Field[])
    sp, ap = to_c_data(f, d)
    f2, d2 = from_c_data(sp, ap)
    validate_semantic(f2, d2)
    checked(getvalue(f2, d2, 3) === Int64(3), "cdata round-trip value failed")
    m2 = f2.metadata
    checked(m2 !== nothing && length(m2) == 1 && first(m2[1]) == "mk" &&
        last(m2[1]) == "mv", "cdata metadata round-trip failed")
    # A column imported over the C seam reads through a caller-supplied
    # static type, fully resolved.
    tm = materialize(Int64, f2, d2)
    checked(tm isa Vector{Int64} && tm == Int64[1, 2, 3],
        "cdata typed materialize failed")
    checked(nullcount(d2) == 0, "cdata round-trip nullcount failed")
    # close! on the imported region runs the foreign release callback now;
    # the export registry must be empty once the consumer releases.
    close!(d2.buffers[2].region::OwnerRegion)
    caught = false
    try
        getvalue(f2, d2, 1)
    catch e
        caught = e isa InvalidStateException
    end
    checked(caught, "use after cdata release accepted")
    # Consumer release marks the export roots; reap! collects them.
    reap!()
    checked(isempty(EXPORT_REGISTRY), "export registry not empty after reap")
    return nothing
end

function exercise_values()::Nothing
    # Columns are adapted one concrete vector at a time: heterogeneous
    # NamedTuple iteration (the `batch(nt)` convenience) is runtime-schema
    # work that belongs to the facade's builders, not a trim-safe core path.
    f1, c1 = fromjulia("xs", Int64[1, 2, 3])
    f2, c2 = fromjulia("ys", [1.5, missing, 3.5])
    f3, c3 = fromjulia("flags", [true, missing, false])
    f4, c4 = fromjulia("strs", ["a", "", missing])
    # A concrete Vector{Vector{Int64}} column; missing-list coverage lives
    # in the plain test suite (a Union-eltype column makes this call's
    # argument type imprecise for trim verification).
    f5, c5 = fromjulia("lists", [Int64[1, 2], Int64[3], Int64[]])
    for (f, col) in ((f1, c1), (f2, c2), (f3, c3), (f4, c4), (f5, c5))
        validate_structural(f, col)
        validate_semantic(f, col)
    end
    checked(getvalue(f1, c1, 2) === Int64(2), "int getvalue failed")
    checked(nullcount(c2) == 1, "nullcount failed")
    m1 = materialize(f1, c1)
    checked(length(m1) == 3, "materialize length wrong")
    checked(getvalue(f4, c4, 1) == "a", "string getvalue failed")
    checked(getvalue(f4, c4, 3) === missing, "missing string wrong")
    v5 = getvalue(f5, c5, 1)
    checked(v5 isa Vector{Any} && length(v5) == 2, "list getvalue failed")
    checked(getvalue(f5, c5, 3) isa Vector{Any}, "empty list getvalue failed")
    # Hand-built struct column: `fromjulia_struct` iterates a heterogeneous
    # NamedTuple (runtime-schema builder work, facade territory).
    saf, sad = fromjulia("a", Int64[7, 8])
    sbf, sbd = fromjulia("b", ["x", "y"])
    sf = Field("st", StructType(); nullable=false, children=[saf, sbf])
    sd = AC.ArrayData(StructType(), 2, [BufferSlice()];
        children=[sad, sbd], nullcount=0)
    validate_structural(sf, sd)
    sv = getvalue(sf, sd, 2)
    checked(sv isa Vector{Pair{String,Any}} && length(sv) == 2,
        "struct getvalue failed")
    df, dd = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, missing, 0])
    validate_structural(df, dd)
    validate_semantic(df, dd)
    checked(getvalue(df, dd, 4) == "lo", "dictionary getvalue failed")
    checked(getvalue(df, dd, 3) === missing, "dictionary null failed")
    return nothing
end

function exercise_typed_values()::Nothing
    # A caller-supplied static schema makes element access fully
    # resolvable — concrete claims at every call site below.
    f1, c1 = fromjulia("xs", Int64[1, 2, 3])
    checked(getvalue(Int64, f1, c1, 2) === Int64(2), "typed int failed")
    m1 = materialize(Int64, f1, c1)
    checked(m1 isa Vector{Int64} && m1[3] === Int64(3),
        "typed int materialize failed")
    f2, c2 = fromjulia("ys", [1.5, missing, 3.5])
    m2 = materialize(Union{Missing,Float64}, f2, c2)
    checked(m2 isa Vector{Union{Missing,Float64}} && m2[2] === missing,
        "typed float materialize failed")
    f4, c4 = fromjulia("strs", ["a", "", missing])
    checked(getvalue(Union{Missing,String}, f4, c4, 1) == "a",
        "typed string failed")
    f5, c5 = fromjulia("lists", [Int64[1, 2], Int64[3], Int64[]])
    m5 = materialize(Vector{Int64}, f5, c5)
    checked(m5 isa Vector{Vector{Int64}} && m5[1] == Int64[1, 2],
        "typed list materialize failed")
    saf, sad = fromjulia("a", Int64[7, 8])
    sbf, sbd = fromjulia("b", ["x", "y"])
    sf = Field("st", StructType(); nullable=false, children=[saf, sbf])
    sd = AC.ArrayData(StructType(), 2, [BufferSlice()];
        children=[sad, sbd], nullcount=0)
    sv = getvalue(NamedTuple{(:a, :b),Tuple{Int64,String}}, sf, sd, 2)
    checked(sv === (a=Int64(8), b="y"), "typed struct failed")
    df, dd = AC.fromjulia_dict("d", ["lo", "hi"], [0, 1, missing, 0])
    checked(getvalue(Union{Missing,String}, df, dd, 2) == "hi",
        "typed dictionary failed")
    # Four HETEROGENEOUS NamedTuple fields, both entry points: ntuple
    # closures erase per-field types at this arity — the unrolled struct
    # row must stay fully resolved.
    h1f, h1d = fromjulia("a", Int64[1, 2])
    h2f, h2d = fromjulia("b", [1.5, 2.5])
    h3f, h3d = fromjulia("c", ["x", "y"])
    h4f, h4d = fromjulia("flag", [true, false])
    hf = Field("st4", StructType(); nullable=false,
        children=[h1f, h2f, h3f, h4f])
    hd = AC.ArrayData(StructType(), 2, [BufferSlice()];
        children=[h1d, h2d, h3d, h4d], nullcount=0)
    NT4 = NamedTuple{(:a, :b, :c, :flag),Tuple{Int64,Float64,String,Bool}}
    hv = getvalue(NT4, hf, hd, 2)
    checked(hv === (a=Int64(2), b=2.5, c="y", flag=false),
        "typed 4-field struct getvalue failed")
    hm = materialize(NT4, hf, hd)
    checked(hm isa Vector{NT4} && hm[1].c == "x",
        "typed 4-field struct materialize failed")
    # The claim is exact: a mismatched static type refuses, never converts.
    caught = false
    try
        getvalue(Int32, f1, c1, 1)
    catch e
        caught = e isa ArgumentError
    end
    checked(caught, "typed mismatch accepted")
    caught = false
    try
        getvalue(Float64, f2, c2, 2)
    catch e
        caught = e isa ArgumentError
    end
    checked(caught, "typed null under non-missing claim accepted")
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
    # Plain mkdir/rm rather than `mktempdir() do`: Base's temp-path cleanup
    # registry (locks + atexit hooks) parks the scheduler under the trimmed
    # runtime; the primitive filesystem calls are all the workload needs.
    dir = joinpath(tempdir(), "arrowcore-trim-" * string(getpid()))
    mkdir(dir)
    try
        exercise_mmap(dir)
    finally
        rm(joinpath(dir, "trim.bin"); force=true)
        rm(dir)
    end
    exercise_values()
    exercise_typed_values()
    exercise_validation_errors()
    exercise_cdata()
    return nothing
end

function @main(args::Vector{String})::Cint
    _ = args
    run_trim_workload()
    return 0
end

Base.Experimental.entrypoint(main, (Vector{String},))
