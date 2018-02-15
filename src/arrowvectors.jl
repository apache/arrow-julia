
#================================================================================================
    functions common to both lists and primitives
================================================================================================#
values(A::Primitive) = A
values(A::ArrowVector) = A.values
bitmask(A::ArrowVector) = A.bitmask
offsets(A::ArrowVector) = A.offsets
export values, bitmask, offsets

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
rawvalues(A::ArrowVector) = rawvalues(A.values)
export rawvalues


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
    for i ∈ idx
        checkbounds(A, i)
    end
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
    a, b = divrem(i-1, 8) .+ (0,1)
    !getbit(unsafe_load(bitmaskpointer(A) + a), b)
end
unsafe_isnull(A::ArrowVector, idx::AbstractVector{<:Integer}) = Bool[unsafe_isnull(A, i) for i ∈ idx]


"""
    isnull(A::ArrowVector, idx)

Check whether element(s) `idx` of `A` are null.
"""
isnull(A::ArrowVector, i) = false
function isnull(A::ArrowVector{Union{J,Missing}}, i::Integer) where J
    a, b = divrem(i-1, 8) .+ (1,1)
    !getbit(A.bitmask[a], b)
end
isnull(A::ArrowVector, idx::AbstractVector{<:Integer}) = Bool[isnull(A, i) for i ∈ idx]
isnull(A::ArrowVector, idx::AbstractVector{Bool}) = Bool[isnull(A, i) for i ∈ 1:length(A) if idx[i]]
export isnull


"""
    unsafe_rawbitmask(p::ArrowVector{Union{J,Missing}})

Retrieve the raw value of the null bit mask for `p`.
"""
function unsafe_rawbitmask(p::ArrowVector{Union{J,Missing}}) where J
    unsafe_rawpadded(bitmaskpointer(A), bitmaskbytes(p))
end


function setnull!(A::ArrowVector{Union{J,Missing}}, x::Bool, i::Integer) where J
    a, b = divrem(i-1, 8) .+ (1,1)
    byte = setbit(getvalue(A.bitmask, a), !x, b)
    setvalue!(A.bitmask, byte, a)
end

# TODO these are flipped relative to each other which is super confusing
function setnulls!(A::ArrowVector{Union{J,Missing}}, bytes::Vector{UInt8}) where J
    setvalue!(A.bitmask, bytes, 1:length(bytes))
end
function setnulls!(A::ArrowVector{Union{J,Missing}}, nulls::AbstractVector{Bool}) where J
    setnulls!(A, bitpack(.!nulls))
end


"""
    unsafe_setnull!(A::ArrowVector{Union{J,Missing}}, x::Bool, i::Integer)

Set element `i` of `A` to be null. This involves no bounds checking and a call to `unsafe_store!`.
"""
function unsafe_setnull!(A::ArrowVector{Union{J,Missing}}, x::Bool, i::Integer) where J
    a, b = divrem(i-1, 8) .+ (0,1)
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


# macro for creating arrowformat functions
macro _formats(constructor, argtype, w...)
esc(quote
    arrowformat(x::$argtype) where {$(w...)} = $constructor(x)
    function arrowformat(::Type{<:Array}, x::$argtype) where {$(w...)}
        $constructor(Array, x)
    end
    function arrowformat(data::Vector{UInt8}, i::Integer, x::$argtype) where {$(w...)}
        $constructor(data, i, x)
    end
end)
end

"""
    arrowformat(v::AbstractVector)
    arrowformat(Array, v::AbstractVector)

Convert a vector to the appropriate arrowformat.  If `Array` is passed, a contiguous array will
be used for the data buffer.
"""
function arrowformat end
@_formats Primitive AbstractVector{J} J
@_formats NullablePrimitive AbstractVector{Union{J,Missing}} J
@_formats List AbstractVector{J} J<:AbstractString
@_formats NullableList AbstractVector{Union{J,Missing}} J<:AbstractString
@_formats Primitive{Datestamp} AbstractVector{T} T<:Date
@_formats Primitive{Timestamp{Dates.Millisecond}} AbstractVector{T} T<:DateTime
@_formats Primitive{TimeOfDay{Dates.Nanosecond,Int64}} AbstractVector{T} T<:Dates.Time
@_formats NullablePrimitive{Datestamp} AbstractVector{Union{T,Missing}} T<:Date
@_formats NullablePrimitive{Timestamp{Dates.Millisecond}} AbstractVector{Union{T,Missing}} T<:DateTime
@_formats NullablePrimitive{TimeOfDay{Dates.Nanosecond,Int64}} AbstractVector{Union{T,Missing}} T<:Dates.Time
@_formats DictEncoding CategoricalArray{T,1,U} T<:Any U
export arrowformat


# TODO in 0.6, views have to use unsafe methods
# TODO bounds checking is a disaster right now, clean it up
function unsafe_view(l::ArrowVector{J}, i::Union{Integer,AbstractVector{<:Integer}}) where J
    @boundscheck checkbounds(l, i)
    SubArray(unsafe_getvalue(l, i), (i,))
end

# TODO clean up bounds checking macros in 0.7
function getindex(l::ArrowVector{J}, i::Integer) where J
    @boundscheck checkbounds(l, i)
    @inbounds o = getvalue(l, i)
    o
end
function getindex(l::ArrowVector{Union{J,Missing}}, i::Integer)::Union{J,Missing} where J
    @boundscheck checkbounds(l, i)
    @inbounds o = isnull(l, i) ? missing : getvalue(l, i)
    o
end
function getindex(l::ArrowVector{Union{J,Missing}}, idx::AbstractVector{<:Integer}) where J
    @boundscheck checkbounds(l, idx)
    @inbounds v = convert(Vector{Union{J,Missing}}, getvalue(l, idx))
    @inbounds fillmissings!(v, l, idx)
    v
end
getindex(l::ArrowVector, ::Colon) = l[1:end]


function writepadded(io::IO, A::Primitive, idx::Union{<:Integer,AbstractVector{<:Integer}})
    vals = rawvalues(A, idx)
    write(io, vals)
    pad = zeros(UInt8, padding(length(vals)) - length(vals))
    write(io, pad)
    padding(length(vals))
end
writepadded(io::IO, A::Primitive) = writepadded(io, A, 1:length(A))
function writepadded(io::IO, A::ArrowVector, subbuffs::Function...)
    s = 0
    for sb ∈ subbuffs
        s += writepadded(io, sb(A))
    end
    s
end
export writepadded
