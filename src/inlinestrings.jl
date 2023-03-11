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

# passthrough for non-strings
pick_string_type(::Type{T}, offsets) where {T} = T
pick_string_type(::Type{Union{Missing,T}}, offsets::AbstractVector{<:Integer}) where {T<:AbstractString} = Union{Missing,pick_string_type(T, offsets)}
function pick_string_type(::Type{T}, offsets::AbstractVector{<:Integer}) where {T<:AbstractString}
    max_size = _maximum_diff(offsets)
    # if the maximum string length is less than 255, we can use InlineStrings
    return max_size < 255 ? InlineStringType(max_size) : T
end
# extend inlinestrings to pass through Arrow.Lists
_inlinestrings(vect::AbstractVector) = vect
# if it's already an InlineString, we can pass it through
_inlinestrings(vect::Arrow.List{T,O,A}) where {T<:InlineString,O,A} = vect
# if we detect that the strings are small enough, we can inline them
function _inlinestrings(vect::Arrow.List{T,O,A}) where {T<:Union{AbstractString,Union{AbstractString,Missing}},O,A}
    S = pick_string_type(T, vect.offsets.offsets)
    # reconstruct the Arrow.List with the new string type
    Arrow.List{S,O,A}(vect.arrow, vect.validity, vect.offsets, vect.data, vect.ℓ, vect.metadata)
end
