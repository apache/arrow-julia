
bytes_for_bits(n::Integer) = div(((n + 7) & ~7), 8)
getbit(byte::UInt8, i::Integer) = (byte & BITMASK[i] > 0x00) ? true : false


# nbits must be ≤ 8
function _bitpack_byte(a::AbstractVector{Bool}, nbits::Integer)
    o = 0x00
    for i ∈ 1:nbits
        o += UInt8(a[i]) << (i-1)
    end
    o
end

"""
    bitpack(A::AbstractVector{Bool})

Returns a `Vector{UInt8}` the bits of which are the values of `A`.
"""
function bitpack(A::AbstractVector{Bool})
    a, b = divrem(length(A), 8)
    trailing = b > 0
    nbytes = a + Int(trailing)
    v = Vector{UInt8}(nbytes)
    for i ∈ 1:a
        k = (i-1)*8 + 1
        v[i] = _bitpack_byte(view(A, k:(k+7)), 8)
    end
    if trailing
        trail = (a*8+1):length(A)
        v[end] = _bitpack_byte(view(A, trail), length(trail))
    end
    v
end
export bitpack


"""
    unbitpack(A::AbstractVector{UInt8})

Returns a `Vector{Bool}` the values of which are the bits of `A`.
"""
function unbitpack(A::AbstractVector{UInt8})
    v = Vector{Bool}(length(A)*8)
    for i ∈ 1:length(A)
        for j ∈ 1:8
            v[(i-1)*8 + j] = getbit(A[i], j)
        end
    end
    v
end
export unbitpack
