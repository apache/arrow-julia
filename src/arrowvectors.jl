
#================================================================================================
    functions common to both lists and primitives
================================================================================================#

"""
    valuespointer(A::ArrowVector)

Returns a pointer to the start of the values buffer for `A`.
"""
valuespointer(A::ArrowVector) = valuespointer(A.values)
export valuespointer


"""
    bitmaskpointer(A::ArrowVector)

Returns a pointer to the start of the bitmask buffer for `A`.
"""
bitmaskpointer(A::ArrowVector{Union{J,Missing}}) where J = valuespointer(A.bitmask)
export bitmaskpointer


"""
    rawvalues(A::ArrowVector, idx)

Gets a `Vector{UInt8}` of the raw values associated with the indices `idx`.
"""
rawvalues(A::ArrowVector, i) = rawvalues(A.values, i)


# TODO this actually gets fucked up if bitmask has trailing ones (that's also why this is backwards)
"""
    nullcount(A::ArrowVector)

Return the number of nulls (`missing`s) in `A`.
"""
nullcount(A::ArrowVector) = 0
function nullcount(A::ArrowVector{Union{T,Missing}}) where T
    s = 0
    for i ∈ 1:minbitmaskbytes(A)
        s += count_ones(A.bitmask[i])
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
    unsafe_isnull(A::ArrowVector, idx)

Check whether element(s) `idx` of `A` are null.
"""
unsafe_isnull(A::ArrowVector, i::Integer) = false
function unsafe_isnull(A::ArrowVector{Union{T,Missing}}, i::Integer) where T
    a, b = divrem(i, 8)
    !getbit(unsafe_load(bitmaskpointer(A) + a), b)
end
unsafe_isnull(A::ArrowVector, idx::AbstractVector{<:Integer}) = Bool[unsafe_isnull(A, i) for i ∈ idx]


"""
    isnull(A::ArrowVector, idx)

Check whether element(s) `idx` of `A` are null.
"""
isnull(A::ArrowVector, i) = false
function isnull(A::ArrowVector{Union{J,Missing}}, i::Integer) where J
    a, b = divrem(i, 8)
    !getbit(A.bitmask[a+1], b)
end
isnull(A::ArrowVector, idx::AbstractVector{<:Integer}) = Bool[isnull(A, i) for i ∈ idx]
isnull(A::ArrowVector, idx::AbstractVector{Bool}) = Bool[isnull(A, i) for i ∈ 1:length(A) if idx[i]]
export isnull


"""
    unsafe_rawbitmask(p::ArrowVector{Union{J,Missing}}, padding::Function=identity)

Retrieve the raw value of the null bit mask for `p`.

The function `padding` should take as its sole argument the number of bytes of the raw bit mask
data and retrun the total number of bytes appropriate for the padding scheme.  Note that the argument
taken is the *minimum* number of bytes of the bitmask (i.e. `ceil(length(p)/8)`).
"""
function unsafe_rawbitmask(p::ArrowVector{Union{J,Missing}}, padding::Function=identity) where J
    unsafe_rawpadded(bitmaskpointer(A), minbitmaskbytes(p), padding)
end


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


macro _make_fillmissings_funcs(name::Symbol, func::Symbol)
esc(quote
    function $name(v::AbstractVector{Union{J,Missing}}, A::ArrowVector{Union{J,Missing}},
                   idx::AbstractVector{<:Integer}) where J
        for (i, j) ∈ enumerate(idx)
            $func(A, j) && (v[i] = missing)
        end
    end
    function $name(v::AbstractVector{Union{J,Missing}}, A::ArrowVector{Union{J,Missing}},
                   idx::AbstractVector{Bool}) where J
        j = 1
        for i ∈ 1:length(A)
            if idx[i]
                $func(A, i) && (v[j] = missing)
                j += 1
            end
        end
    end
    function $name(v::AbstractVector{Union{J,Missing}}, A::ArrowVector{Union{J,Missing}}) where J
        $name(v, A, 1:length(A))
    end
end)
end
@_make_fillmissings_funcs(unsafe_fillmissings!, unsafe_isnull)
@_make_fillmissings_funcs(fillmissings!, isnull)



# TODO this is really inefficient and also NullExceptions are uninformative
function nullexcept_inrange(A::ArrowVector{Union{T,Missing}}, i::Integer, j::Integer) where T
    for k ∈ i:j
        isnull(A, i) && throw(NullException())
    end
end


function setnonmissing!(A::ArrowVector{J}, v::AbstractVector{T}) where {J,T<:Union{J,Union{J,Missing}}}
    @boundscheck length(A) == length(v) || throw(ArgumentError("trying to set from wrong sized array"))
    for i ∈ 1:length(A)
        !ismissing(v[i]) && (A[i] = v[i])
    end
    A
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


# TODO in 0.6, views have to use unsafe methods
function unsafe_view(l::ArrowVector{J}, i::Union{Integer,AbstractVector{<:Integer}}) where J
    @boundscheck checkbounds(l, i)
    SubArray(unsafe_getvalue(l, i), (i,))
end

function getindex(l::ArrowVector{J}, i::Integer) where J
    @boundscheck checkbounds(l, i)
    getvalue(l, i)
end
function getindex(l::ArrowVector{Union{J,Missing}}, i::Integer)::Union{J,Missing} where J
    @boundscheck checkbounds(l, i)
    isnull(l, i) ? missing : getvalue(l, i)
end
function getindex(l::ArrowVector{Union{J,Missing}}, idx::AbstractVector{<:Integer}) where J
    @boundscheck checkbounds(l, idx)
    v = convert(Vector{Union{J,Missing}}, getvalue(l, idx))
    fillmissings!(v, l, idx)
    v
end
getindex(l::ArrowVector, ::Colon) = l[1:end]
