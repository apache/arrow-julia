
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


# TODO mark bounds checking
getindex(d::DictEncoding, i) = d.pool[d.refs[i]]

