
#================================================================================================
    functions common to both lists and primitives
================================================================================================#
"""
    datapointer(A::ArrowVector)

Returns a pointer to the very start of the data buffer for `A` (i.e. does not depend on indices).
"""
datapointer(A::ArrowVector) = pointer(A.data)
export datapointer


"""
    valuespointer(A::ArrowVector)

Returns a pointer to the start of the values buffer for `A`.
"""
valuespointer(A::ArrowVector{J}) where J = datapointer(A) + A.values_idx - 1
export valuespointer


"""
    bitmaskpointer(A::ArrowVector)

Returns a pointer to the start of the bitmask buffer for `A`.
"""
function bitmaskpointer(A::ArrowVector{Union{J,Missing}}) where J
    datapointer(A) + A.bitmask_idx - 1
end
export bitmaskpointer


valuesindex(A::ArrowVector, i) = A.values_idx + i - 1
bitmaskindex(A::ArrowVector, i) = A.bitmask_idx + i - 1


# TODO this actually gets fucked up if bitmask has trailing ones (that's also why this is backwards)
"""
    nullcount(A::ArrowVector)

Return the number of nulls (`missing`s) in `A`.
"""
nullcount(A::ArrowVector) = 0
function nullcount(A::ArrowVector{Union{T,Missing}}) where T
    s = 0
    for i ∈ 0:(minbitmaskbytes(A)-1)
        @inbounds s += count_ones(A.data[A.bitmask_idx+i])
    end
    length(A) - s
end
export nullcount

checkbounds(A::ArrowVector, i::Integer) = (1 ≤ i ≤ length(A)) || throw(BoundsError(A, i))
# this is probably crazy in the general case, but should work well for unit ranges
function checkbounds(A::ArrowVector, idx::AbstractVector{<:Integer})
    a, b = extrema(idx)
    checkbounds(A, a) && checkbounds(A, b)
end
function checkbounds(A::ArrowVector, idx::AbstractVector{Bool})
    (length(A) ≠ length(idx)) && throw(ArgumentError("incorrect sized boolean indexer"))
end


"""
    unsafe_isnull(A::ArrowVector, i::Integer)

Check whether element `i` of `A` is null. This involves no bounds checking and a call to
`unsafe_load`.
"""
unsafe_isnull(A::ArrowVector, i::Integer) = false
function unsafe_isnull(A::ArrowVector{Union{T,Missing}}, i::Integer) where T
    a, b = divrem(i, 8)
    !getbit(unsafe_load(bitmaskpointer(A) + a), b)
end

"""
    isnull(A::ArrowVector, i)

Check whether element(s) `i` of `A` are null with bounds checking.

**WARNING** Bounds checking is not useful if pointers are misaligned!
"""
isnull(A::ArrowVector, i::Integer) = (checkbounds(A, i); unsafe_isnull(A, i))
isnull(A::ArrowVector, i::AbstractVector{<:Integer}) = (checkbounds(A,i); unsafe_isnull.(A,i))
export isnull


"""
    rawbitmask(p::ArrowVector{Union{J,Missing}}, padding::Function=identity)

Retrieve the raw value of the null bit mask for `p`.

The function `padding` should take as its sole argument the number of bytes of the raw bit mask
data and retrun the total number of bytes appropriate for the padding scheme.  Note that the argument
taken is the *minimum* number of bytes of the bitmask (i.e. `ceil(length(p)/8)`).
"""
function rawbitmask(p::ArrowVector{Union{J,Missing}}, padding::Function=identity) where J
    rawpadded(bitmaskpointer(A), minbitmaskbytes(p), padding)
end
export rawbitmask


"""
    unsafe_setnull!(A::ArrowVector{Union{J,Missing}}, x::Bool, i::Integer)

Set element `i` of `A` to be null. This involves no bounds checking and a call to `unsafe_store!`.
"""
function unsafe_setnull!(A::ArrowVector{Union{J,Missing}}, x::Bool, i::Integer) where J
    a, b = divrem(i, 8)
    ptr = bitmaskpointer(A) + a
    byte = setbit(unsafe_load(ptr), !x, b)
    unsafe_store!(ptr, byte)
end


"""
    unsafe_setnulls!(A::ArrowVector, nulls::AbstractVector{Bool})

Set *all* the nulls for the `ArrowVector`. This does not check bounds and contains a call to
`unsafe_copy!` (but does not copy directly from `nulls`).
"""
function unsafe_setnulls!(A::ArrowVector{Union{J,Missing}}, bytes::Vector{UInt8}) where J
    unsafe_copy!(bitmaskpointer(A), pointer(bytes), length(bytes))
end
function unsafe_setnulls!(A::ArrowVector{Union{J,Missing}}, nulls::AbstractVector{Bool}) where J
    unsafe_setnulls!(A, bitpack(.!nulls))
end


"""
    fillmissings!(v::AbstractVector, A::ArrowVector, idx::AbstractVector{<:Integer})

Sets whether the elements of `A` specified by `idx` are missing based on whether the elements
of `v` are missing.
"""
function fillmissings!(v::AbstractVector{Union{J,Missing}}, A::ArrowVector{Union{J,Missing}},
                       idx::AbstractVector{<:Integer}) where J
    for (i, j) ∈ enumerate(idx)
        unsafe_isnull(A, j) && (v[i] = missing)
    end
end
function fillmissings!(v::AbstractVector{Union{J,Missing}}, A::ArrowVector{Union{J,Missing}},
                       idx::AbstractVector{Bool}) where J
    j = 1
    for i ∈ 1:length(A)
        if idx[i]
            unsafe_isnull(A, i) && (v[j] = missing)
            j += 1
        end
    end
end
function fillmissings!(v::AbstractVector{Union{J,Missing}}, A::ArrowVector{Union{J,Missing}}) where J
    fillmissings!(v, A, 1:length(A))
end


# TODO this is really inefficient and also NullExceptions are uninformative
function nullexcept_inrange(A::ArrowVector{Union{T,Missing}}, i::Integer, j::Integer) where T
    for k ∈ i:j
        unsafe_isnull(A, i) && throw(NullException())
    end
end


length(A::ArrowVector) = Int(A.length)
size(A::ArrowVector) = (length(A),)
function size(A::ArrowVector, i::Integer)
    if i == 1
        return length(A)
    else
        return 1
    end
    throw(ArgumentError("arraysize: dimension $i out of range"))
end
endof(A::ArrowVector) = length(A)


eltype(A::ArrowVector{T}) where T = T


start(::ArrowVector) = 1
next(A::ArrowVector, i::Integer) = (A[i], i+1)
done(A::ArrowVector, i::Integer) = i > length(A)


convert(::Type{Array{T}}, A::ArrowVector{T}) where T = A[:]
convert(::Type{Vector{T}}, A::ArrowVector{T}) where T = A[:]


function view(l::ArrowVector{J}, i::Union{Integer,AbstractVector{<:Integer}}) where J
    @boundscheck checkbounds(l, i)
    SubArray(unsafe_getvalue(l, i), (i,))
end

function getindex(l::ArrowVector{J}, i::Integer) where J
    @boundscheck checkbounds(l, i)
    unsafe_getvalue(l, i)
end
function getindex(l::ArrowVector{J}, idx::AbstractVector{<:Integer}) where J
    @boundscheck checkbounds(l, i)
    copy(unsafe_getvalue(l, i))
end
function getindex(l::ArrowVector{J}, idx::AbstractVector{Bool}) where J
    @boundscheck checkbounds(l, i)
    unsafe_getvalue(l, i)
end

function getindex(l::ArrowVector{Union{J,Missing}}, i::Integer)::Union{J,Missing} where J
    @boundscheck checkbounds(l, i)
    unsafe_isnull(l, i) ? missing : unsafe_getvalue(l, i)
end
function getindex(l::ArrowVector{Union{J,Missing}}, idx::AbstractVector{<:Integer}) where J
    @boundscheck checkbounds(l, idx)
    v = Vector{Union{J,Missing}}(unsafe_getvalue(l, idx))
    fillmissings!(v, l, idx)
    v
end
function getindex(l::ArrowVector{Union{J,Missing}}, idx::AbstractVector{Bool}) where J
    @boundscheck checkbounds(l, idx)
    v = Union{J,Missing}[unsafe_getvalue(l, i) for i ∈ 1:length(l) if idx[i]]
    fillmissings!(v, l, idx)
    v
end
getindex(l::ArrowVector, ::Colon) = l[1:end]
