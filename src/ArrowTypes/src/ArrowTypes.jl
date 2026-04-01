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

using Sockets
using UUIDs

export ArrowKind,
    NullKind,
    PrimitiveKind,
    BoolKind,
    ListKind,
    FixedSizeListKind,
    MapKind,
    StructKind,
    UnionKind,
    DictEncodedKind,
    toarrow,
    arrowname,
    fromarrow,
    ToArrow

"""
    ArrowTypes.ArrowKind(T)

For a give type `T`, define it's "arrow type kind", or the general category of arrow types it should be treated as. Must be one of:
  * [`ArrowTypes.NullKind`](@ref): `Missing` is the only type defined as `NullKind`
  * [`ArrowTypes.PrimitiveKind`](@ref): `<:Integer`, `<:AbstractFloat`, along with `Arrow.Decimal`, and the various `Arrow.ArrowTimeType` subtypes
  * [`ArrowTypes.BoolKind`](@ref): only `Bool`
  * [`ArrowTypes.ListKind`](@ref): any `AbstractString` or `AbstractArray`
  * [`ArrowTypes.FixedSizeList`](@ref): `NTuple{N, T}`
  * [`ArrowTypes.MapKind`](@ref): any `AbstractDict`
  * [`ArrowTypes.StructKind`](@ref): any `NamedTuple` or plain struct (mutable or otherwise)
  * [`ArrowTypes.UnionKind`](@ref): any `Union`
  * [`ArrowTypes.DictEncodedKind`](@ref): array types that implement the `DataAPI.refpool` interface

The list of `ArrowKind`s listed above translate to different ways to physically store data as supported by the arrow data format.
See the docs for each for an idea of whether they might be an appropriate fit for a custom type.
Note that custom types need to satisfy any additional "interface methods" as required by the various `ArrowKind`
types. By default, if a type in julia is declared like `primitive type ...` it is considered a `PrimitiveKind`
and if `struct` or `mutable struct` it's considered a `StructKind`. Also note that types will rarely need to define `ArrowKind`;
much more common is to define `ArrowType(T)` and `toarrow(x::T)` to transform `T` to a natively supported arrow type, which will
already have its `ArrowKind` defined.
"""
abstract type ArrowKind end

ArrowKind(x::T) where {T} = ArrowKind(T)
ArrowKind(::Type{T}) where {T} = isprimitivetype(T) ? PrimitiveKind() : StructKind()

"""
    ArrowTypes.ArrowType(T) = S

Interface method to define the natively supported arrow type `S` that a given type `T` should be converted to before serializing.
Useful when a custom type wants a "serialization hook" or otherwise needs to be transformed/converted into a natively
supported arrow type for serialization. If a type defines `ArrowType`, it must also define a corresponding
[`ArrowTypes.toarrow(x::T)`](@ref) method which does the actual conversion from `T` to `S`.
Note that custom structs defined like `struct T` or `mutable struct T` are natively supported in serialization, so unless
_additional_ transformation/customization is desired, a custom type `T` can serialize with no `ArrowType` definition (by default,
each field of a struct is serialized, using the results of `fieldnames(T)` and `getfield(x, i)`).
Note that defining these methods only deal with custom _serialization_ to the arrow format; to be able to _deserialize_ custom
types at all, see the docs for [`ArrowTypes.arrowname`](@ref), [`ArrowTypes.arrowmetadata`](@ref), [`ArrowTypes.JuliaType`](@ref),
and [`ArrowTypes.fromarrow`](@ref).
"""
function ArrowType end
ArrowType(::Type{T}) where {T} = T
ArrowType(::Type{Any}) = Any
ArrowType(::Type{Union{Missing,T}}) where {T} = Union{Missing,ArrowType(T)}
ArrowType(::Type{Missing}) = Missing

"""
    ArrowTypes.toarrow(x::T) => S

Interface method to perform the actual conversion from an object `x` of type `T` to the type `S`. `T` and `S` must match the
types used when defining `ArrowTypes.ArrowType(::Type{T}) = S`. Hence, `S` is the natively supported arrow type that `T`
desires to convert to to enable serialization. See [`ArrowTypes.ArrowType`](@ref) docs for more details.
This enables custom objects to be serialized as a natively supported arrow data type.
"""
function toarrow end
toarrow(x) = x

"""
    ArrowTypes.arrowname(T) = Symbol(name)

Interface method to define the logical type "label" for a custom Julia type `T`. Names will be global for an entire arrow dataset,
and conventionally, custom types will just use their type name along with a Julia- and package-specific prefix; for example,
for a custom type `Foo`, I would define `ArrowTypes.arrowname(::Type{Foo}) = Symbol("JuliaLang.MyPackage.Foo")`.
This ensures other language implementations won't get confused and are safe to ignore the logical type label.
When arrow stores non-native data, it must still be _stored_ as a native data type, but can have type metadata tied to the data that
labels the original _logical_ type it originated from. This enables the conversion of native data back to the logical type when
deserializing, as long as the deserializer has the same definitions when the data was serialized. Namely, the current Julia
session will need the appropriate [`ArrowTypes.JuliaType`](@ref) and [`ArrowTypes.fromarrow`](@ref) definitions in order to know
how to convert the native data to the original logical type. See the docs for those interface methods in order to ensure a complete
implementation. Also see the accompanying [`ArrowTypes.arrowmetadata`](@ref) docs around providing additional metadata about a custom
logical type that may be necessary to fully re-create a Julia type (e.g. non-field-based type parameters).
"""
function arrowname end
const EMPTY_SYMBOL = Symbol()
arrowname(T) = EMPTY_SYMBOL
hasarrowname(T) = arrowname(T) !== EMPTY_SYMBOL
arrowname(::Type{Union{T,Missing}}) where {T} = arrowname(T)
arrowname(::Type{Union{T,Nothing}}) where {T} = arrowname(T)
arrowname(::Type{Missing}) = EMPTY_SYMBOL
arrowname(::Type{Any}) = EMPTY_SYMBOL

"""
    ArrowTypes.arrowmetadata(T) => String

Interface method to provide additional logical type metadata when serializing extension types. [`ArrowTypes.arrowname`](@ref)
provides the logical type _name_, which may be all that's needed to return a proper Julia type from [`ArrowTypes.JuliaType`](@ref),
but some custom types may, for example have type parameters that aren't inferred/based on fields. In order to fully recreate these
kinds of types when deserializing, these type parameters can be stored by defining `ArrowTypes.arrowmetadata(::Type{T}) = "type_param"`.
This will then be available to access by overloading `ArrowTypes.JuliaType(::Val{Symbol(name)}, S, arrowmetadata::String)`.
"""
function arrowmetadata end
const EMPTY_STRING = ""
arrowmetadata(T) = EMPTY_STRING
arrowmetadata(::Type{Union{T,Missing}}) where {T} = arrowmetadata(T)
arrowmetadata(::Type{Union{T,Nothing}}) where {T} = arrowmetadata(T)
arrowmetadata(::Type{Nothing}) = EMPTY_STRING
arrowmetadata(::Type{Missing}) = EMPTY_STRING
arrowmetadata(::Type{Any}) = EMPTY_STRING

"""
    ArrowTypes.JuliaType(::Val{Symbol(name)}, ::Type{S}, arrowmetadata::String) = T

Interface method to define the custom Julia logical type `T` that a serialized metadata label should be converted to when
deserializing. When reading arrow data, and a logical type label is encountered for a column, it will call
`ArrowTypes.JuliaType(Val(Symbol(name)), S, arrowmetadata)` to see if a Julia type has been "registered" for deserialization. The `name`
used when defining the method *must* correspond to the same `name` when defining `ArrowTypes.arrowname(::Type{T}) = Symbol(name)`.
The use of `Val(Symbol(...))` is to allow overloading a method on a specific logical type label. The `S` 2nd argument passed to
`JuliaType` is the native arrow serialized type. This can be useful for parametric Julia types that wish to correctly parameterize
their custom type based on what was serialized. The 3rd argument `arrowmetadata` is any metadata that was stored when the logical
type was serialized as the result of calling `ArrowTypes.arrowmetadata(T)`. Note the 2nd and 3rd arguments are optional when
overloading if unneeded.
When defining [`ArrowTypes.arrowname`](@ref) and `ArrowTypes.JuliaType`, you may also want to implement [`ArrowTypes.fromarrow`]
in order to customize how a custom type `T` should be constructed from the native arrow data type. See its docs for more details.
"""
function JuliaType end
JuliaType(val) = nothing
JuliaType(val, S) = JuliaType(val)
JuliaType(val, S, meta) = JuliaType(val, S)

"""
    ArrowTypes.fromarrow(::Type{T}, x::S) => T

Interface method that provides a "deserialization hook" for a custom type `T` to be constructed from the native arrow type `S`.
The `T` and `S` types must correspond to the definitions used in `ArrowTypes.ArrowType(::Type{T}) = S`. This is a paired method
with [`ArrowTypes.toarrow`](@ref).

The default definition is `ArrowTypes.fromarrow(::Type{T}, x) = T(x)`, so if that works for a custom type already, no additional
overload is necessary.
A few `ArrowKind`s have/allow slightly more custom overloads for their `fromarrow` methods:
  * `ListKind{true}`: for `String` types, they may overload `fromarrow(::Type{T}, ptr::Ptr{UInt8}, len::Int) = ...` to avoid
     materializing a `String`
  * `StructKind`:
     * May overload `fromarrow(::Type{T}, x...)` where individual fields are passed as separate
     positional arguments; so if my custom type `Interval` has two fields `first` and `last`, then I'd overload like
     `ArrowTypes.fromarrow(::Type{Interval}, first, last) = ...`. Note the default implementation is
     `ArrowTypes.fromarrow(::Type{T}, x...) = T(x...)`, so if your type already accepts all arguments in a constructor
     no additional `fromarrow` method should be necessary (default struct constructors have this behavior).
     * Alternatively, may overload `fromarrowstruct(::Type{T}, ::Val{fnames}, x...)`, where `fnames` is a tuple of the
     field names corresponding to the values in `x`. This approach is useful when you need to implement deserialization
     in a manner that is agnostic to the field order used by the serializer. When implemented, `fromarrowstruct` takes precedence over `fromarrow` in `StructKind` deserialization.
"""
function fromarrow end
fromarrow(::Type{T}, x::T) where {T} = x
fromarrow(::Type{T}, x...) where {T} = T(x...)
fromarrow(::Type{Union{Missing,T}}, ::Missing) where {T} = missing
fromarrow(::Type{Union{Missing,T}}, x::T) where {T} = x
fromarrow(::Type{Union{Missing,T}}, x::T) where {T<:NamedTuple} = x # ambiguity fix
fromarrow(::Type{Union{Missing,T}}, x) where {T} = fromarrow(T, x)

"NullKind data is actually not physically stored since the data is constant; just the length is needed"
struct NullKind <: ArrowKind end

ArrowKind(::Type{Missing}) = NullKind()
ArrowKind(::Type{Nothing}) = NullKind()
ArrowType(::Type{Nothing}) = Missing
toarrow(::Nothing) = missing
const NOTHING = Symbol("JuliaLang.Nothing")
arrowname(::Type{Nothing}) = NOTHING
JuliaType(::Val{NOTHING}) = Nothing
fromarrow(::Type{Nothing}, ::Missing) = nothing

"PrimitiveKind data is stored as plain bits in a single contiguous buffer"
struct PrimitiveKind <: ArrowKind end

ArrowKind(::Type{<:Integer}) = PrimitiveKind()
ArrowKind(::Type{<:AbstractFloat}) = PrimitiveKind()

ArrowType(::Type{Char}) = UInt32
toarrow(x::Char) = convert(UInt32, x)
const CHAR = Symbol("JuliaLang.Char")
arrowname(::Type{Char}) = CHAR
JuliaType(::Val{CHAR}) = Char
fromarrow(::Type{Char}, x::UInt32) = Char(x)

ArrowType(::Type{T}) where {T<:Enum} = Base.Enums.basetype(T)
toarrow(x::T) where {T<:Enum} = Base.Enums.basetype(T)(x)
const ENUM = Symbol("JuliaLang.Enum")
arrowname(::Type{T}) where {T<:Enum} = ENUM

function _qualifiedtypepath(::Type{T}) where {T}
    module_path = join(string.(Base.fullname(parentmodule(T))), ".")
    return string(module_path, ".", nameof(T))
end

function _enum_labels(::Type{T}) where {T<:Enum}
    B = Base.Enums.basetype(T)
    return join((string(instance, ":", B(instance)) for instance in instances(T)), ",")
end

function _parseenumlabels(labels::AbstractString, ::Type{B}) where {B<:Integer}
    pairs = Pair{String,B}[]
    isempty(labels) && return pairs
    for entry in split(labels, ',')
        isempty(entry) && return nothing
        delimiter = findfirst(==(':'), entry)
        delimiter === nothing && return nothing
        label = entry[1:prevind(entry, delimiter)]
        value = entry[nextind(entry, delimiter):end]
        isempty(label) && return nothing
        parsed = tryparse(B, value)
        parsed === nothing && return nothing
        push!(pairs, label => parsed)
    end
    return pairs
end

function _enumlabelsmatch(::Type{T}, labels::AbstractString) where {T<:Enum}
    B = Base.Enums.basetype(T)
    parsed = _parseenumlabels(labels, B)
    parsed === nothing && return false
    expected = [string(instance) => B(instance) for instance in instances(T)]
    length(parsed) == length(expected) || return false
    parsed_dict = Dict(parsed)
    length(parsed_dict) == length(parsed) || return false
    return parsed_dict == Dict(expected)
end

function arrowmetadata(::Type{T}) where {T<:Enum}
    return string("type=", _qualifiedtypepath(T), ";labels=", _enum_labels(T))
end

function _parsemetadata(metadata::AbstractString)
    parsed = Dict{String,String}()
    isempty(metadata) && return parsed
    for entry in split(metadata, ';')
        isempty(entry) && continue
        delimiter = findfirst(==('='), entry)
        delimiter === nothing && continue
        key = entry[1:prevind(entry, delimiter)]
        value = entry[nextind(entry, delimiter):end]
        parsed[key] = value
    end
    return parsed
end

function _rootmodule(name::Symbol)
    name === :Main && return Main
    if isdefined(Main, name)
        candidate = getfield(Main, name)
        candidate isa Module && return candidate
    end
    try
        return Base.root_module(Main, name)
    catch
        return nothing
    end
end

function _resolvequalifiedtype(path::AbstractString)
    parts = split(path, '.')
    length(parts) < 2 && return nothing
    current = _rootmodule(Symbol(first(parts)))
    current isa Module || return nothing
    for part in parts[2:(end - 1)]
        symbol = Symbol(part)
        isdefined(current, symbol) || return nothing
        current = getfield(current, symbol)
        current isa Module || return nothing
    end
    type_symbol = Symbol(last(parts))
    isdefined(current, type_symbol) || return nothing
    return getfield(current, type_symbol)
end

function JuliaType(::Val{ENUM}, S, metadata::String)
    parsed = _parsemetadata(metadata)
    haskey(parsed, "type") || return nothing
    haskey(parsed, "labels") || return nothing
    T = _resolvequalifiedtype(parsed["type"])
    T isa DataType || return nothing
    T <: Enum || return nothing
    storage_type = Base.nonmissingtype(S)
    Base.Enums.basetype(T) === storage_type || return nothing
    _enumlabelsmatch(T, parsed["labels"]) || return nothing
    return T
end

fromarrow(::Type{T}, x::Integer) where {T<:Enum} = T(x)

"BoolKind data is stored with values packed down to individual bits; so instead of a traditional Bool being 1 byte/8 bits, 8 Bool values would be packed into a single byte"
struct BoolKind <: ArrowKind end
ArrowKind(::Type{Bool}) = BoolKind()

"ListKind data are stored in two separate buffers; one buffer contains all the original data elements flattened into one long buffer; the 2nd buffer contains an offset into the 1st buffer for how many elements make up the original array element"
struct ListKind{stringtype} <: ArrowKind end

ListKind() = ListKind{false}()
isstringtype(::ListKind{stringtype}) where {stringtype} = stringtype
isstringtype(::Type{ListKind{stringtype}}) where {stringtype} = stringtype

ArrowKind(::Type{<:AbstractString}) = ListKind{true}()
# Treate Base.CodeUnits as Binary arrow type
ArrowKind(::Type{<:Base.CodeUnits}) = ListKind{true}()

fromarrow(::Type{T}, ptr::Ptr{UInt8}, len::Int) where {T} =
    fromarrow(T, unsafe_string(ptr, len))
fromarrow(::Type{T}, x) where {T<:Base.CodeUnits} = Base.CodeUnits(x)
fromarrow(::Type{Union{Missing,Base.CodeUnits}}, x) =
    x === missing ? missing : Base.CodeUnits(x)

ArrowType(::Type{Symbol}) = String
toarrow(x::Symbol) = String(x)
const SYMBOL = Symbol("JuliaLang.Symbol")
arrowname(::Type{Symbol}) = SYMBOL
JuliaType(::Val{SYMBOL}) = Symbol
_symbol(ptr, len) = ccall(:jl_symbol_n, Ref{Symbol}, (Ptr{UInt8}, Int), ptr, len)
fromarrow(::Type{Symbol}, ptr::Ptr{UInt8}, len::Int) = _symbol(ptr, len)

ArrowKind(::Type{<:AbstractArray}) = ListKind()
fromarrow(::Type{A}, x::A) where {A<:AbstractVector{T}} where {T} = x
fromarrow(::Type{A}, x::AbstractVector{T}) where {A<:AbstractVector{T}} where {T} =
    convert(A, x)
ArrowKind(::Type{<:AbstractSet}) = ListKind()
ArrowType(::Type{T}) where {T<:AbstractSet{S}} where {S} = Vector{S}
toarrow(x::AbstractSet) = collect(x)
const SET = Symbol("JuliaLang.Set")
arrowname(::Type{<:AbstractSet}) = SET
JuliaType(::Val{SET}, ::Type{T}) where {T<:AbstractVector{S}} where {S} = Set{S}
fromarrow(::Type{T}, x) where {T<:AbstractSet} = T(x)

"FixedSizeListKind data are stored in a single contiguous buffer; individual elements can be computed based on the fixed size of the lists"
struct FixedSizeListKind{N,T} <: ArrowKind end
gettype(::FixedSizeListKind{N,T}) where {N,T} = T
getsize(::FixedSizeListKind{N,T}) where {N,T} = N

ArrowKind(::Type{NTuple{N,T}}) where {N,T} = FixedSizeListKind{N,T}()

ArrowKind(::Type{UUID}) = FixedSizeListKind{16,UInt8}()
ArrowType(::Type{UUID}) = NTuple{16,UInt8}
toarrow(x::UUID) = _cast(NTuple{16,UInt8}, x.value)
const UUIDSYMBOL = Symbol("arrow.uuid")
const LEGACY_UUIDSYMBOL = Symbol("JuliaLang.UUID")
arrowname(::Type{UUID}) = UUIDSYMBOL
JuliaType(::Val{UUIDSYMBOL}) = UUID
JuliaType(::Val{LEGACY_UUIDSYMBOL}) = UUID
fromarrow(::Type{UUID}, x::NTuple{16,UInt8}) = UUID(_cast(UInt128, x))

ArrowKind(::Type{IPv4}) = PrimitiveKind()
ArrowType(::Type{IPv4}) = UInt32
toarrow(x::IPv4) = x.host
const IPV4_SYMBOL = Symbol("JuliaLang.IPv4")
arrowname(::Type{IPv4}) = IPV4_SYMBOL
JuliaType(::Val{IPV4_SYMBOL}) = IPv4
fromarrow(::Type{IPv4}, x::Integer) = IPv4(x)

ArrowKind(::Type{IPv6}) = FixedSizeListKind{16,UInt8}()
ArrowType(::Type{IPv6}) = NTuple{16,UInt8}
toarrow(x::IPv6) = _cast(NTuple{16,UInt8}, x.host)
const IPV6_SYMBOL = Symbol("JuliaLang.IPv6")
arrowname(::Type{IPv6}) = IPV6_SYMBOL
JuliaType(::Val{IPV6_SYMBOL}) = IPv6
fromarrow(::Type{IPv6}, x::NTuple{16,UInt8}) = IPv6(_cast(UInt128, x))

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

"StructKind data are stored in separate buffers for each field of the struct"
struct StructKind <: ArrowKind end

ArrowKind(::Type{<:NamedTuple}) = StructKind()

@inline fromarrowstruct(T::Type, ::Val, x...) = fromarrow(T, x...)

fromarrow(
    ::Type{NamedTuple{names,types}},
    x::NamedTuple{names,types},
) where {names,types<:Tuple} = x
fromarrow(::Type{T}, x::NamedTuple) where {T} = fromarrow(T, Tuple(x)...)

ArrowKind(::Type{<:Tuple}) = StructKind()
ArrowKind(::Type{Tuple{}}) = StructKind()
const TUPLE = Symbol("JuliaLang.Tuple")
# needed to disambiguate the FixedSizeList case for NTuple
arrowname(::Type{NTuple{N,T}}) where {N,T} = EMPTY_SYMBOL
arrowname(::Type{T}) where {T<:Tuple} = TUPLE
arrowname(::Type{Tuple{}}) = TUPLE
JuliaType(::Val{TUPLE}, ::Type{NamedTuple{names,types}}) where {names,types<:Tuple} = types
fromarrow(::Type{T}, x::NamedTuple) where {T<:Tuple} = Tuple(x)

# Complex
const COMPLEX = Symbol("JuliaLang.Complex")
arrowname(::Type{<:Complex}) = COMPLEX
JuliaType(::Val{COMPLEX}, ::Type{NamedTuple{names,Tuple{T,T}}}) where {names,T<:Real} =
    Complex{T}
fromarrowstruct(::Type{T}, ::Val{(:re, :im)}, re, im) where {T<:Complex} = T(re, im)
fromarrowstruct(::Type{T}, ::Val{(:im, :re)}, im, re) where {T<:Complex} = T(re, im)

# VersionNumber
const VERSION_NUMBER = Symbol("JuliaLang.VersionNumber")
ArrowKind(::Type{VersionNumber}) = StructKind()
arrowname(::Type{VersionNumber}) = VERSION_NUMBER
JuliaType(::Val{VERSION_NUMBER}) = VersionNumber
default(::Type{VersionNumber}) = v"0"

function fromarrow(::Type{VersionNumber}, v::NamedTuple)
    VersionNumber(v.major, v.minor, v.patch, v.prerelease, v.build)
end

"MapKind data are stored similarly to ListKind, where elements are flattened, and a 2nd offsets buffer contains the individual list element length data"
struct MapKind <: ArrowKind end

ArrowKind(::Type{<:AbstractDict}) = MapKind()

"UnionKind data are stored either in a separate, compacted buffer for each union type (dense), or in full-length buffers for each union type (sparse)"
struct UnionKind <: ArrowKind end

ArrowKind(::Union) = UnionKind()

"DictEncodedKind store a small pool of unique values in one buffer, with a full-length buffer of integer offsets into the small value pool"
struct DictEncodedKind <: ArrowKind end

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
default(::Type{T}) where {T<:Enum} = first(instances(T))
default(::Type{<:AbstractString}) = ""
default(::Type{Any}) = nothing
default(::Type{Missing}) = missing
default(::Type{Nothing}) = nothing
default(::Type{Union{T,Missing}}) where {T} = default(T)
default(::Type{Union{T,Nothing}}) where {T} = default(T)
default(::Type{Union{T,Missing,Nothing}}) where {T} = default(T)

function default(::Type{A}) where {A<:AbstractVector{T}} where {T}
    a = similar(A, 1)
    a[1] = default(T)
    return a
end

default(::Type{SubArray{T,N,P,I,L}}) where {T,N,P,I,L} = view(default(P), 0:-1)

default(::Type{NTuple{N,T}}) where {N,T} = ntuple(i -> default(T), N)
default(::Type{Tuple{}}) = ()
function default(::Type{T}) where {T<:Tuple}
    T === Tuple{} && return ()
    N = Base.isvarargtype(T.parameters[end]) ? length(T.parameters) - 1 : fieldcount(T)
    return Tuple(default(fieldtype(T, i)) for i = 1:N)
end

default(::Type{T}) where {T<:AbstractDict} = T()
default(::Type{NamedTuple{names,types}}) where {names,types} =
    NamedTuple{names}(Tuple(default(fieldtype(types, i)) for i = 1:length(names)))

function promoteunion(T, S)
    T === S && return T
    new = promote_type(T, S)
    return isabstracttype(new) ? Union{T,S} : new
end

function _toarroweltype(x)
    state = iterate(x)
    state === nothing && return Missing
    y, st = state
    srcT = Union{}
    stable = false
    T = Missing
    if y !== missing
        srcT = typeof(y)
        mapped = ArrowType(srcT)
        stable = isconcretetype(mapped)
        T = stable ? mapped : typeof(toarrow(y))
    end
    while true
        state = iterate(x, st)
        state === nothing && return T
        y, st = state
        if y === missing
            S = Missing
        elseif srcT === Union{}
            srcT = typeof(y)
            mapped = ArrowType(srcT)
            stable = isconcretetype(mapped)
            S = stable ? mapped : typeof(toarrow(y))
        elseif stable && typeof(y) === srcT
            continue
        else
            S = typeof(toarrow(y))
            if stable && typeof(y) !== srcT
                stable = false
            end
        end
        S === T && continue
        T = promoteunion(T, S)
    end
end

@inline _hasoffsetaxes(data) = Base.has_offset_axes(data)
@inline _offsetshift(data) = _hasoffsetaxes(data) ? firstindex(data) - 1 : 0
@inline _hasonebasedaxes(data) = !_hasoffsetaxes(data)

# lazily call toarrow(x) on getindex for each x in data
struct ToArrow{T,A} <: AbstractVector{T}
    data::A
    offset::Int
    needsconvert::Bool
end
@inline _sourcedata(x::ToArrow) = getfield(x, :data)
@inline _sourceoffset(x::ToArrow) = getfield(x, :offset)
@inline _needsconvert(x::ToArrow) = getfield(x, :needsconvert)
@inline _sourcevalue(x::ToArrow, i::Integer) =
    @inbounds getindex(_sourcedata(x), i + _sourceoffset(x))

function ToArrow{T,A}(data::A) where {T,A}
    needsconvert = !(eltype(A) === T && concrete_or_concreteunion(T))
    return ToArrow{T,A}(data, _offsetshift(data), needsconvert)
end

concrete_or_concreteunion(T) =
    isconcretetype(T) ||
    (T isa Union && concrete_or_concreteunion(T.a) && concrete_or_concreteunion(T.b))

function ToArrow(x::A) where {A}
    S = eltype(A)
    T = ArrowType(S)
    if S === T && concrete_or_concreteunion(S) && _hasonebasedaxes(x)
        return x
    elseif !concrete_or_concreteunion(T)
        # arrow needs concrete types, so try to find a concrete common type, preferring unions
        if isempty(x)
            return Missing[]
        end
        T = _toarroweltype(x)
        if T === Missing && concrete_or_concreteunion(S)
            T = promoteunion(T, typeof(toarrow(default(S))))
        end
    end
    return ToArrow{T,A}(x)
end

Base.IndexStyle(::Type{<:ToArrow}) = Base.IndexLinear()
Base.size(x::ToArrow) = (length(x.data),)
Base.eltype(::Type{TA}) where {T,A,TA<:ToArrow{T,A}} = T
function _convert(::Type{T}, x) where {T}
    if x isa T
        return x
    elseif T isa Union
        # T was a promoted Union and x is not already one of
        # the concrete Union types, so we need to just try
        # to convert, recursively, to one of the Union types
        # unfortunately not much we can do more efficiently here
        try
            return _convert(T.a, x)
        catch
            return _convert(T.b, x)
        end
    else
        return convert(T, x)
    end
end

@inline function _toarrowvalue(x::ToArrow{T}, value) where {T}
    _needsconvert(x) || return value
    return _convert(T, toarrow(value))
end

Base.@propagate_inbounds function Base.getindex(x::ToArrow{T}, i::Int) where {T}
    value = _sourcevalue(x, i)
    return _toarrowvalue(x, value)
end

function Base.iterate(x::ToArrow)
    state = iterate(x.data)
    state === nothing && return nothing
    value, st = state
    return _toarrowvalue(x, value), st
end

function Base.iterate(x::ToArrow, st)
    state = iterate(x.data, st)
    state === nothing && return nothing
    value, st = state
    return _toarrowvalue(x, value), st
end

end # module ArrowTypes
