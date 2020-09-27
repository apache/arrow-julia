module ArrowTypes

export ArrowType, NullType, PrimitiveType, ListType, FixedSizeListType, MapType, StructType, UnionType, DictEncodedType

abstract type ArrowType end

ArrowType(x::T) where {T} = ArrowType(T)
ArrowType(::Type{T}) where {T} = isprimitivetype(T) ? PrimitiveType() : StructType()

struct NullType <: ArrowType end

ArrowType(::Type{Missing}) = NullType()

struct PrimitiveType <: ArrowType end

ArrowType(::Type{<:Integer}) = PrimitiveType()
ArrowType(::Type{<:AbstractFloat}) = PrimitiveType()
ArrowType(::Type{Bool}) = PrimitiveType()

struct ListType <: ArrowType end

ArrowType(::Type{<:AbstractString}) = ListType()
ArrowType(::Type{<:AbstractArray}) = ListType()

struct FixedSizeListType <: ArrowType end

ArrowType(::Type{<:NTuple}) = FixedSizeListType()

struct MapType <: ArrowType end

ArrowType(::Type{<:Dict}) = MapType()

struct StructType <: ArrowType end

ArrowType(::Type{<:NamedTuple}) = StructType()

struct UnionType <: ArrowType end

ArrowType(::Union) = UnionType()

struct DictEncodedType <: ArrowType end

end # module ArrowTypes