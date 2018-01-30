
bytesforbits(n::Integer) = div(((n + 7) & ~7), 8)
export bytesforbits

getbit(byte::UInt8, i::Integer) = (byte & BITMASK[i] > 0x00) ? true : false
function setbit(byte::UInt8, x::Bool, i::Integer)
    if x
        byte | BITMASK[i]
    else
        byte & (~BITMASK[i])
    end
end


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


function checkinputsize(v::AbstractVector, idx::AbstractVector{<:Integer})
    if length(v) ≠ length(idx)
        throw(DimensionMismatch("tried to assign $(length(v)) elements to $(length(idx)) destinations"))
    end
end
function checkinputsize(v::AbstractVector, idx::AbstractVector{Bool})
    if length(v) ≠ sum(idx)
        throw(DimensionMismatch("tried to assign $(length(v)) elements to $(sum(idx)) destinations"))
    end
end
function checkinputsize(v::AbstractVector, A::ArrowVector)
    if length(v) ≠ length(A)
        throw(DimensionMismatch("tried to assign $(length(v)) elements to $(length(A)) destinations"))
    end
end


function check_buffer_overrun(b::Buffer, i::Integer, l::Integer, obj::Symbol)
    if length(b) - i + 1 < l
        throw(ErrorException("$obj of size $l bytes can't fit in buffer at location $i"))
    end
end
function check_buffer_overrun(b::Buffer, i::Integer, A::ArrowVector, obj::Symbol)
    check_buffer_overrun(b, i, minbytes(A), obj)
end


"""
    rawpadded(ptr::Ptr, len::Integer, padding::Function=identity)

Return a `Vector{UInt8}` padded to appropriate size specified by `padding`.
"""
function rawpadded(ptr::Ptr{UInt8}, len::Integer, padding::Function=identity)
    npad = padding(len) - len
    vcat(unsafe_wrap(Array, ptr, len), zeros(UInt8, npad))
end
