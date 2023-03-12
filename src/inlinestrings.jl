# This code should be moved into InlineStrings extensions

### Type extensions
# Use InlineStrings to get data from pointers (for getindex and similar)
ArrowTypes.fromarrow(::Type{T}, ptr::Ptr{UInt8}, len::Int) where {T<:InlineString} =ArrowTypes.fromarrow(T, T(ptr, len))
ArrowTypes.fromarrow(::Type{Union{T,Missing}}, ptr::Ptr{UInt8}, len::Int) where {T<:InlineString} =ArrowTypes.fromarrow(T, T(ptr, len))

### Utilities for inlining strings
# determines the maximum string length necessary for the offsets 
# calculates difference between offsets as a proxy for string size
function _maximum_diff(v::AbstractVector{<:Integer})
    mx = first(v)
    prev = mx
    @inbounds if length(v) > 1
        mx = max(mx, v[2] - prev)
        prev = v[2]
        for i in firstindex(v)+2:lastindex(v)
            diff = v[i] - prev
            mx < diff && (mx = diff)
            prev = v[i]
        end
    end
    mx
end
# extract offsets from Arrow.List
_offsetsints(arrowlist::Arrow.List) = arrowlist.offsets.offsets

# convert strings to InlineStrings, does not check validity (hence unsafe!) - simply swaps the type
function _unsafe_convert(::Type{Arrow.List{S, O, A}}, vect::Arrow.List{T,O,A}) where {S<:Union{InlineString, Union{Missing, InlineString}},T<:Union{AbstractString, Union{Missing, AbstractString}},O,A}
    Arrow.List{S,O,A}(vect.arrow, vect.validity, vect.offsets, vect.data, vect.ℓ, vect.metadata)
end

# passthrough for non-strings
pick_string_type(::Type{T}, offsets) where {T} = T
pick_string_type(::Type{Union{Missing,T}}, offsets) where {T<:AbstractString} = Union{Missing,pick_string_type(T, offsets)}
function pick_string_type(::Type{T}, offsets::AbstractVector{<:Integer}) where {T<:AbstractString}
    max_size = _maximum_diff(offsets)
    # if the maximum string length is less than 255, we can use InlineStrings
    return max_size < 255 ? InlineStringType(max_size) : T
end
# find one joint string type for all chained arrays - vector of vectors
function pick_string_type(::Type{T}, vectoffsets::AbstractVector{<:AbstractVector{<:Integer}}) where {T<:AbstractString}
    max_size = _maximum_diff.(vectoffsets)|>maximum
    # if the maximum string length is less than 255, we can use InlineStrings
    return max_size < 255 ? InlineStringType(max_size) : T
end

# extend inlinestrings to pass through Arrow.Lists
_inlinestrings(vect::AbstractVector) = vect

## methods for SentinelArrays.ChainedVector (if we have many RecordBatches / partitions)
# if it's already an InlineString, we can pass it through
_inlinestrings(vect::SentinelArrays.ChainedVector{<:Union{T,Union{T,Missing}}}) where {T<:InlineString} = vect
# if we detect a String type, try to inline it -- we need to find one unified type across all chained arrays
function _inlinestrings(vectofvect::SentinelArrays.ChainedVector{T, Arrow.List{T,O,A}}) where {T<:Union{AbstractString,Union{Missing,AbstractString}},O,A}
    # find the smallest common denominator string type for all chained arrays
    S = pick_string_type(T, _offsetsints.(vectofvect.arrays))
    if S == T
        # if the type is the same, we can pass it through
        return vectofvect
    else 
        # otherwise, we need to reconstruct the ChainedVector with the new string type
        # TODO: look into in-place conversion
        return SentinelArrays.ChainedVector(_unsafe_convert.(Arrow.List{S,O,A},vectofvect.arrays))
    end
end

# TODO: check that we handle ChainedVector that contains something else than Arrow.List with Strings

# if we detect that the strings are small enough, we can inline them
function _inlinestrings(vect::Arrow.List{T,O,A}) where {T<:Union{AbstractString,Union{AbstractString,Missing}},O,A}
    S = pick_string_type(T, _offsetsints(vect))
    if S == T
        return vect
    else
        # reconstruct the Arrow.List with the new string type
        return _unsafe_convert(Arrow.List{S,O,A}, vect)
    end 
end

## methods for Arrow.List (if we have only 1 RecordBatch, ie, unpartitioned)
# if it's already an InlineString, we can pass it through
_inlinestrings(vect::Arrow.List{T,O,A}) where {T<:InlineString,O,A} = vect
# if we detect that the strings are small enough, we can inline them
function _inlinestrings(vect::Arrow.List{T,O,A}) where {T<:Union{AbstractString,Union{AbstractString,Missing}},O,A}
    S = pick_string_type(T, vect.offsets.offsets)
    # reconstruct the Arrow.List with the new string type
    Arrow.List{S,O,A}(vect.arrow, vect.validity, vect.offsets, vect.data, vect.ℓ, vect.metadata)
end
