using Arrow, ArrowTypes, Random, BenchmarkTools

struct Foo
    a::Int
    b::String
    c::Vector{String}
    d::Float64
end

ArrowTypes.arrowname(::Type{Foo}) = Symbol("JuliaLang.Foo")
ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.Foo")}, T) = Foo

genfoo() = Foo(rand(1:10), randstring(10), [randstring(10) for _ in 1:rand(2:5)], rand())
t = (; f = [genfoo() for _ in 1:1000])
f = Arrow.Table(Arrow.tobuffer(t)).f
@benchmark sum(x -> x.d, $f)
