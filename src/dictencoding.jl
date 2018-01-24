
struct DictEncoding{P<:ArrowVector,J} <: ArrowVector{J}
    refs::Primitive{Int32}
    pool::P
end
export DictEncoding

function DictEncoding{P,J}(ptr::Ptr, ref_loc::Integer, len::Integer, pool::P) where {J,P<:ArrowVector{J}}
    DictEncoding{P,J}(Primitive{Int32}(ptr, ref_loc, len), pool)
end
function DictEncoding{P,J}(b::Buffer, ref_loc::Integer, len::Integer,
                           pool::P) where {J,P<:ArrowVector{J}}
    DictEncoding{P,J}(pointer(b.data), ref_loc, len, pool)
end


length(d::DictEncoding) = length(d.refs)


# TODO mark bounds checking (also, these throw errors from d.refs)
# note that we define all of these methods to avoid method ambiguity
getindex(d::DictEncoding, i::Integer) = d.pool[d.refs[i]+1]
getindex(d::DictEncoding, i::AbstractVector{<:Integer}) = d.pool[d.refs[i].+1]
getindex(d::DictEncoding, i::AbstractVector{Bool}) = d.pool[d.refs[i].+1]

