
# TODO add docs!!!

abstract type AbstractBitPrimitive{J} <: ArrowVector{J} end
export BitPrimitive


struct BitPrimitive <: AbstractBitPrimitive{Bool}
    length::Int32
    values::Primitive{UInt8}  # in principle this can be anything, but don't see the need
end
export BitPrimitive

function BitPrimitive(data::Vector{UInt8}, i::Integer, len::Integer)
    BitPrimitive(len, Primitive{UInt8}(data, i, bytesforbits(len)))
end
function BitPrimitive(data::Vector{UInt8}, i::Integer, x::AbstractVector{Bool})
    BitPrimitive(len, Primitive{UInt8}(data, i, bitpack(x)))
end
function BitPrimitive(data::Vector{UInt8}, i::Integer, x::AbstractVector{UInt8},
                      len::Integer=sizeof(UInt8)*length(x))
    BitPrimitive(len, Primitive{UInt8}(data, i, x))
end

function BitPrimitive(v::AbstractVector{UInt8}, len::Integer=sizeof(UInt8)*length(v))
    BitPrimitive(len, Primitive{UInt8}(v))
end
function BitPrimitive(v::AbstractVector{Bool})
    BitPrimitive(length(v), Primitive{UInt8}(bitpack(v)))
end

function BitPrimitive(::Type{<:Array}, v::AbstractVector{UInt8}, len::Integer=sizeof(UInt8)*length(v))
    BitPrimitive(length(v), Primitive{UInt8}(Array, bitpack(v)))
end



struct NullableBitPrimitive <: AbstractBitPrimitive{Union{Bool,Missing}}
    length::Int32
    bitmask::Primitive{UInt8}
    values::Primitive{UInt8}
end
export NullableBitPrimitive

function NullableBitPrimitive(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                              len::Integer)
    bmask = Primitive{UInt8}(data, bitmask_idx, bitmaskbytes(len))
    vals = Primitive{UInt8}(data, values_idx, bytesforbits(len))
    NullableBitPrimitive(len, bmask, vals)
end

function NullableBitPrimitive(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                              x::AbstractVector{T}) where T<:Union{Bool,Union{Bool,Missing}}
    bmask = Primitive{UInt8}(data, bitmask_idx, bitmask(x))
    vals = Primitive{UInt8}(data, values_idx, bytesforbits(x))
    setnonmissing!(vals, bitpack(x))
    NullableBitPrimitive(length(x), bmask, vals)
end
function NullableBitPrimitive(data::Vector{UInt8}, i::Integer, x::AbstractVector{T}
                             ) where T<:Union{Bool,Union{Bool,Missing}}
    NullableBitPrimitive(data, i, i+bitmaskbytes(x), x)
end

function NullableBitPrimitive(v::AbstractVector{T}) where T<:Union{Bool,Union{Bool,Missing}}
    bmask = Primitive{UInt8}(bitmask(v))
    vals = Primitive{UInt8}(bitpack(v))
    NullableBitPrimitive(length(v), bmask, vals)
end

function NullableBitPrimitive(::Type{<:Array}, v::AbstractVector{T}
                             ) where T<:Union{Bool,Union{Bool,Missing}}
    b = Vector{UInt8}(bitmaskbytes(v)+bytesforbits(v))
    NullableBitPrimitive(b, 1, v)
end



getvalue(A::AbstractBitPrimitive, idx) = getbit(values(A), idx)
setvalue!(A::AbstractBitPrimitive, x::Bool, i::Integer) = setbit!(values(A), x, i)
function setvalue!(A::AbstractBitPrimitive, x::AbstractVector{Bool}, idx::AbstractVector{<:Integer})
    setbit!(values(A), x, idx)
end


