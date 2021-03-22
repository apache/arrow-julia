# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The ArrowTypes module provides the [`ArrowTypes.Arrowtype`](@ref) interface trait that objects can define
in order to signal how they should be serialized in the arrow format.
"""
module ArrowTypes

using UUIDs

export ArrowKind, NullType, PrimitiveType, BoolType, ListType, FixedSizeListType, MapType, StructType, UnionType, DictEncodedType, toarrow, arrowname, fromarrow, ToArrow

abstract type ArrowKind end

ArrowKind(x::T) where {T} = ArrowKind(T)
ArrowKind(::Type{T}) where {T} = isprimitivetype(T) ? PrimitiveType() : StructType()

function ArrowType end
ArrowType(::Type{T}) where {T} = T

function toarrow end
toarrow(T, x) = x

function arrowname end
const EMPTY_SYMBOL = Symbol()
arrowname(T) = EMPTY_SYMBOL

function JuliaType end
JuliaType(x) = nothing

function fromarrow end
fromarrow(T, x) = x

struct NullType <: ArrowKind end

ArrowKind(::Type{Missing}) = NullType()

struct PrimitiveType <: ArrowKind end

ArrowKind(::Type{<:Integer}) = PrimitiveType()
ArrowKind(::Type{<:AbstractFloat}) = PrimitiveType()

ArrowKind(::Type{Char}) = PrimitiveType()
toarrow(x::Char) = convert(UInt32, x)
const CHAR = Symbol("JuliaLang.Char")
arrowname(::Type{Char}) = CHAR
fromarrow(::Val{CHAR}, x::UInt32) = Char(x)

struct BoolType <: ArrowKind end
ArrowKind(::Type{Bool}) = BoolType()

struct ListType{stringtype} <: ArrowKind end

ListType() = ListType{false}()

ArrowKind(::Type{<:AbstractString}) = ListType{true}()

fromarrow(V::Val{name}, ptr::Ptr{UInt8}, len::Int) where {name} = fromarrow(V, unsafe_string(ptr, len))

_symbol(ptr, len) = ccall(:jl_symbol_n, Ref{Symbol}, (Ptr{UInt8}, Int), ptr, len)
const SYMBOL = Symbol("JuliaLang.Symbol")
ArrowKind(::Type{Symbol}) = ListType{true}()
arrowname(::Type{Symbol}) = SYMBOL
toarrow(x::Symbol) = String(x)
fromarrow(::Val{SYMBOL}, ptr::Ptr{UInt8}, len::Int) = _symbol(ptr, len)

ArrowKind(::Type{<:AbstractArray}) = ListType()

struct FixedSizeListType{N, T} <: ArrowKind end
# gettype(::FixedSizeListType{N, T}) where {N, T} = T
# getsize(::FixedSizeListType{N, T}) where {N, T} = N

ArrowKind(::Type{NTuple{N, T}}) where {N, T} = FixedSizeListType{N, T}()

ArrowKind(::Type{UUID}) = FixedSizeListType{16, UINt8}()
const UUIDSYMBOL = Symbol("JuliaLang.UUID")
arrowname(::Type{UUID}) = UUIDSYMBOL
toarrow(x::UUID) = _cast(NTuple{16, UInt8}, u.value)
fromarrow(::Val{UUIDSYMBOL}, x::NTuple{16, UInt8}) = UUID(_cast(UInt128, x))

function _cast(::Type{Y}, x)::Y where {Y}
    y = Ref{Y}()
    _unsafe_cast!(y, Ref(x), 1)
    return y[]
end

function _unsafe_cast!(y::Ref{Y}, x::Ref, n::Integer) where {Y}
    X = eltype(x)
    GC.@preserve x y begin
        ptr_x = Base.unsafe_convert(Ptr{X}, x)
        ptr_y = Base.unsafe_convert(Ptr{Y}, y)
        unsafe_copyto!(Ptr{X}(ptr_y), ptr_x, n)
    end
    return y
end

struct StructType <: ArrowKind end

ArrowKind(::Type{<:NamedTuple}) = StructType()

fromarrow(V::Val{name}; kw...) where {name} = fromarrow(V, kw.data)
fromarrow(V::Val{name}, x::NamedTuple) where {name} = fromarrow(V, Tuple(x)...)

ArrowKind(::Type{<:Tuple}) = StructType()
const TUPLE = Symbol("JuliaLang.Tuple")
arrowname(::Type{NTuple{N, T}}) where {N, T} = EMPTY_SYMBOL
arrowname(::Type{T}) where {T <: Tuple} = TUPLE
fromarrow(::Val{TUPLE}, x::NamedTuple) = Tuple(x)

# must implement keytype, valtype
struct MapType <: ArrowKind end

ArrowKind(::Type{<:AbstractDict}) = MapType()

struct UnionType <: ArrowKind end

ArrowKind(::Union) = UnionType()

struct DictEncodedType <: ArrowKind end

"""
There are a couple places when writing arrow buffers where
we need to write a "dummy" value; it doesn't really matter
what we write, but we need to write something of a specific
type. So each supported writing type needs to define `default`.
"""
function default end

default(T) = zero(T)
default(::Type{Symbol}) = Symbol()
default(::Type{Char}) = '\0'
default(::Type{<:AbstractString}) = ""
default(::Type{Union{T, Missing}}) where {T} = default(T)

function default(::Type{A}) where {A <: AbstractVector{T}} where {T}
    a = similar(A, 1)
    a[1] = default(T)
    return a
end

default(::Type{NTuple{N, T}}) where {N, T} = ntuple(i -> default(T), N)
default(::Type{T}) where {T <: Tuple} = Tuple(default(fieldtype(T, i)) for i = 1:fieldcount(T))
default(::Type{T}) where {T <: AbstractDict} = T()
default(::Type{NamedTuple{names, types}}) where {names, types} = NamedTuple{names}(Tuple(default(fieldtype(types, i)) for i = 1:length(names)))

# lazily call toarrow(x) on getindex for each x in data
struct ToArrow{T, A} <: AbstractVector{T}
    data::A
end

function ToArrow(x::A) where {A}
    T = ArrowType(eltype(A))
    return ToArrow{T, A}(x)
end

Base.IndexStyle(::Type{<:ToArrow}) = Base.IndexLinear()
Base.size(x::ToArrow) = (length(x.data),)
Base.eltype(x::ToArrow{T, A}) where {T, A} = T
Base.getindex(x::ToArrow{T}, i::Int) where {T} = toarrow(getindex(x.data, i))::T

# const JULIA_TO_ARROW_TYPE_MAPPING = Dict{Type, Tuple{String, Type}}(
#     Char => ("JuliaLang.Char", UInt32),
#     Symbol => ("JuliaLang.Symbol", String),
#     UUID => ("JuliaLang.UUID", NTuple{16,UInt8}),
# )

# istyperegistered(::Type{T}) where {T} = haskey(JULIA_TO_ARROW_TYPE_MAPPING, T)

# function getarrowtype!(meta, ::Type{T}) where {T}
#     arrowname, arrowtype = JULIA_TO_ARROW_TYPE_MAPPING[T]
#     meta["ARROW:extension:name"] = arrowname
#     meta["ARROW:extension:metadata"] = ""
#     return arrowtype
# end

# const ARROW_TO_JULIA_TYPE_MAPPING = Dict{String, Tuple{Type, Type}}(
#     "JuliaLang.Char" => (Char, UInt32),
#     "JuliaLang.Symbol" => (Symbol, String),
#     "JuliaLang.UUID" => (UUID, NTuple{16,UInt8}),
# )

# function extensiontype(f, meta)
#     if haskey(meta, "ARROW:extension:name")
#         typename = meta["ARROW:extension:name"]
#         if haskey(ARROW_TO_JULIA_TYPE_MAPPING, typename)
#             T = ARROW_TO_JULIA_TYPE_MAPPING[typename][1]
#             return f.nullable ? Union{T, Missing} : T
#         else
#             @warn "unsupported ARROW:extension:name type: \"$typename\""
#         end
#     end
#     return nothing
# end

# function registertype!(juliatype::Type, arrowtype::Type, arrowname::String=string("JuliaLang.", string(juliatype)))
#     # TODO: validate that juliatype isn't already default arrow type
#     JULIA_TO_ARROW_TYPE_MAPPING[juliatype] = (arrowname, arrowtype)
#     ARROW_TO_JULIA_TYPE_MAPPING[arrowname] = (juliatype, arrowtype)
#     return
# end

end # module ArrowTypes