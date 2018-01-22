
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
