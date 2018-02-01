

struct VectorBuffer{P<:ArrowVector,J} <: ArrowVector{J}
    vec::P
    data::Vector{UInt8}
end
export VectorBuffer

# TODO WARNING!!! I still expect that in general this can break
function VectorBuffer(v::AbstractVector{J}) where J
    data = Vector{UInt8}(minbytes(v))
    p = Primitive(pointer(data), 1, v)
    VectorBuffer{typeof(p),J}(p, data)
end

function VectorBuffer(v::AbstractVector{Union{J,Missing}}) where J
    data = Vector{UInt8}(minbytes(v))
    p = NullablePrimitive(pointer(data), 1, minbitmaskbytes(v)+1, v)
    VectorBuffer{typeof(p),J}(p, data)
end

function VectorBuffer(::Type{U}, v::AbstractVector{J}) where {U,J<:AbstractString}
    
end


length(A::VectorBuffer) = length(A.vec)
size(A::VectorBuffer) = size(A.vec)
size(A::VectorBuffer, i::Integer) = size(A.vec, i)

valuesbytes(A::VectorBuffer) = valuesbytes(A.vec)
minbitmaskbytes(A::VectorBuffer) = minbitmaskbytes(A.vec)
offsetsbytes(A::VectorBuffer{<:AbstractList,J}) where J = offsetsbytes(A.vec)
minbytes(A::VectorBuffer) = minbytes(A.vec)

unsafe_getvalue(A::VectorBuffer, i) = unsafe_getvalue(A.vec, i)

# multiple to avoid method ambiguity
getindex(A::VectorBuffer, i::Integer) = getindex(A.vec, i)
getindex(A::VectorBuffer, idx::AbstractVector{<:Integer}) = getindex(A.vec, i)
