
#================================================================================================
    functions common to both lists and primitives
================================================================================================#
nullcount(A::ArrowVector) = 0
function nullcount(A::ArrowVector{Union{T,Missing}}) where T
    sum(count_ones(unsafe_load(A.validity, i)) for i ∈ 1:bytesforbits(length(A)))
end
export nullcount

checkbounds(A::ArrowVector, i::Integer) = (1 ≤ i ≤ A.length) || throw(BoundsError(A, i))
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
    !getbit(unsafe_load(A.validity + a), b)
end

"""
    isnull(A::ArrowVector, i)

Safely check whether element(s) `i` of `A` are null.
"""
isnull(A::ArrowVector, i::Integer) = (checkbounds(A, i); unsafe_isnull(A, i))
isnull(A::ArrowVector, i::AbstractVector{<:Integer}) = (checkbounds(A,i); unsafe_isnull.(A,i))
export isnull


"""
    unsafe_setnull!(A::ArrowVector{Union{J,Missing}}, x::Bool, i::Integer)

Set element `i` of `A` to be null. This involves no bounds checking and a call to `unsafe_store!`.
"""
function unsafe_setnull!(A::ArrowVector{Union{J,Missing}}, x::Bool, i::Integer) where J
    a, b = divrem(i, 8)
    ptr = A.validity + a
    byte = setbit(unsafe_load(ptr), !x, b)
    unsafe_store!(ptr, byte)
end


"""
    unsafe_setnulls!(A::ArrowVector, nulls::AbstractVector{Bool})

Set *all* the nulls for the `ArrowVector`. This does not check bounds and contains a call to
`unsafe_copy!` (but does not copy directly from `nulls`).
"""
function unsafe_setnulls!(A::ArrowVector{Union{J,Missing}}, bytes::Vector{UInt8}) where J
    unsafe_copy!(A.validity, pointer(bytes), length(bytes))
end
function unsafe_setnulls!(A::ArrowVector{Union{J,Missing}}, nulls::AbstractVector{Bool}) where J
    unsafe_setnulls!(A, bitpack(.!nulls))
end


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


convert(::Type{Array{T}}, A::ArrowVector{T}) where T = A[1:end]
convert(::Type{Vector{T}}, A::ArrowVector{T}) where T = A[1:end]


function getindex(l::ArrowVector{J}, i::Union{Integer,AbstractVector{<:Integer}}) where J
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
