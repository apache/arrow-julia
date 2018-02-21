using Arrow
using BenchmarkTools

const L = 10^7


function wrap(::Type{T}, len::Integer, v::Vector{UInt8}) where T
    ptr = convert(Ptr{T}, pointer(v))
    unsafe_wrap(Array, ptr, len)
end


function benches1()
    A = reinterpret(UInt8, rand(Int64, L))
    
    info("performing wrap benchmark...")
    global b_wrap = @benchmark wrap(Int64, $L, $A)
    
    info("performing reinterpret benchmark...")
    global b_reint = @benchmark reinterpret(Int64, $A)
    

    p = Primitive{Int64}(A, 1, L)
    
    info("performing Arrow benchmark...")
    global b_arrow = @benchmark Arrow.getindex($p, 1:$L)
    global b_arrow_unsafe = @benchmark Arrow.unsafe_getvalue($p, 1:$L)

    global b_idx = @benchmark Arrow.rawvalueindex($p, 1:length($p))
end


benches1()

