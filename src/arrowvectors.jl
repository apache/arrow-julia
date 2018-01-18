
#================================================================================================
    functions common to both lists and primitives
================================================================================================#
nullcount(A::ArrowVector) = 0
nullcount(A::ArrowVector{Union{T,Missing}}) where T = Int(A.null_count)

checkbounds(A::ArrowVector, i::Integer) = (1 ≤ i ≤ A.length) || throw(BoundsError(A, i))
# this is probably crazy in the general case, but should work well for unit ranges
function checkbounds(A::ArrowVector, idx::AbstractVector{<:Integer})
    a, b = extrema(idx)
    checkbounds(A, a) && checkbounds(A, b)
end
function checkbounds(A::ArrowVector, idx::AbstractVector{Bool})
    (length(A) ≠ length(idx)) && throw(ArgumentError("incorrect sized boolean indexer"))
end


unsafe_isnull(A::ArrowVector, i::Integer) = false
function unsafe_isnull(A::ArrowVector{Union{T,Missing}}, i::Integer) where T
    a, b = divrem(i, 8)
    !getbit(unsafe_load(A.validity + a), b)
end

isnull(A::ArrowVector) = (checkbounds(A, i); unsafe_isnull(A, i))
export isnull


length(A::ArrowVector) = A.length
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
