# Micro-benchmark: schema-cached vs. schema-parsed-each-time batch import
#
# Run with: julia --project bench/schema_cache.jl

import Pkg
Pkg.activate(joinpath(@__DIR__, ".."))
if !haskey(Pkg.project().dependencies, "BenchmarkTools")
    Pkg.add("BenchmarkTools")
end

using BenchmarkTools
import Arrow
import Tables

const N_ROWS = 100

io = let
    buf = IOBuffer()
    Arrow.write(
        buf,
        (
            c1=rand(Int64, N_ROWS),
            c2=rand(Float64, N_ROWS),
            c3=[string(i) for i in 1:N_ROWS],
            c4=rand(Int32, N_ROWS),
            c5=rand(Float32, N_ROWS),
            c6=Union{Int64,Missing}[isodd(i) ? Int64(i) : missing for i in 1:N_ROWS],
            c7=[string('a' + mod(i, 26)) for i in 1:N_ROWS],
            c8=rand(Int64, N_ROWS),
            c9=rand(Float64, N_ROWS),
            c10=rand(Int32, N_ROWS),
        ),
    )
    seekstart(buf)
    buf
end

const TBL   = Arrow.Table(io)
const SREFS, AREFS = Arrow.to_c_data(TBL)

const SCHEMA_PTRS = [Ptr{Arrow.ArrowSchema}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s)) for s in SREFS]
const ARRAY_PTRS  = [Ptr{Arrow.ArrowArray}( Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a)) for a in AREFS]
const CVOID_SPTRS = Ptr{Cvoid}.(SCHEMA_PTRS)
const CVOID_APTRS = Ptr{Cvoid}.(ARRAY_PTRS)

# Parse schema once; reuse for every benchmark iteration
const SCHEMA = Arrow.parse_c_schema(SCHEMA_PTRS)

println("=== Baseline: from_c_data (parses schema every call) + access all columns ===")
b_baseline = @benchmark begin
    tbl = Arrow.from_c_data($CVOID_SPTRS, $CVOID_APTRS)
    for i in 1:length($SCHEMA.nodes)
        Tables.getcolumn(tbl, i)
    end
end evals=1 samples=500 seconds=10
show(stdout, MIME"text/plain"(), b_baseline)
println()

println("\n=== Cached: from_c_data with pre-parsed TableSchema + access all columns ===")
b_cached = @benchmark begin
    tbl = Arrow.from_c_data($SCHEMA, $CVOID_APTRS)
    for i in 1:length($SCHEMA.nodes)
        Tables.getcolumn(tbl, i)
    end
end evals=1 samples=500 seconds=10
show(stdout, MIME"text/plain"(), b_cached)
println()

speedup = median(b_baseline).time / median(b_cached).time
println("\nSpeedup (median): $(round(speedup; digits=2))x")

# ── Primitive-only table (Int64 + Float64, no strings, no nulls) ─────────────

io_prim = let
    buf = IOBuffer()
    Arrow.write(
        buf,
        (
            p1=rand(Int64, N_ROWS),
            p2=rand(Float64, N_ROWS),
            p3=rand(Int32, N_ROWS),
            p4=rand(Float32, N_ROWS),
            p5=rand(Int64, N_ROWS),
            p6=rand(Float64, N_ROWS),
            p7=rand(Int32, N_ROWS),
            p8=rand(Float64, N_ROWS),
            p9=rand(Int64, N_ROWS),
            p10=rand(Float32, N_ROWS),
        ),
    )
    seekstart(buf)
    buf
end

const TBL_P   = Arrow.Table(io_prim)
const SREFS_P, AREFS_P = Arrow.to_c_data(TBL_P)
const SPTRS_P  = [Ptr{Arrow.ArrowSchema}(Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, s)) for s in SREFS_P]
const APTRS_P  = [Ptr{Arrow.ArrowArray}( Base.unsafe_convert(Ptr{Arrow.ArrowArray},  a)) for a in AREFS_P]
const CSPTRS_P = Ptr{Cvoid}.(SPTRS_P)
const CAPTRS_P = Ptr{Cvoid}.(APTRS_P)
const SCHEMA_P = Arrow.parse_c_schema(SPTRS_P)

println("\n\n=== Primitives baseline: from_c_data (parses schema every call) ===")
b_prim_baseline = @benchmark begin
    tbl = Arrow.from_c_data($CSPTRS_P, $CAPTRS_P)
    for i in 1:length($SCHEMA_P.nodes)
        Tables.getcolumn(tbl, i)
    end
end evals=1 samples=500 seconds=10
show(stdout, MIME"text/plain"(), b_prim_baseline)
println()

println("\n=== Primitives cached: from_c_data with pre-parsed TableSchema ===")
b_prim_cached = @benchmark begin
    tbl = Arrow.from_c_data($SCHEMA_P, $CAPTRS_P)
    for i in 1:length($SCHEMA_P.nodes)
        Tables.getcolumn(tbl, i)
    end
end evals=1 samples=500 seconds=10
show(stdout, MIME"text/plain"(), b_prim_cached)
println()

speedup_prim = median(b_prim_baseline).time / median(b_prim_cached).time
println("\nSpeedup primitives (median): $(round(speedup_prim; digits=2))x")
