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

_builtinarrowtype(::Type{ArrowTypes.UUID}) = NTuple{16,UInt8}
_builtintoarrow(x::ArrowTypes.UUID) = ArrowTypes._cast(NTuple{16,UInt8}, x.value)
_builtinarrowname(::Type{ArrowTypes.UUID}) = ArrowTypes.UUIDSYMBOL
_builtinextensionspec(::Type{ArrowTypes.UUID}) =
    ExtensionTypeSpec(_builtinarrowname(ArrowTypes.UUID), "")
_builtinextensionjuliatype(::Val{ArrowTypes.UUIDSYMBOL}, S, metadata::String) =
    ArrowTypes.UUID
_builtinextensionjuliatype(::Val{ArrowTypes.LEGACY_UUIDSYMBOL}, S, metadata::String) =
    ArrowTypes.UUID

_builtinextensionspec(::Type{Bool8}) = ExtensionTypeSpec(BOOL8_SYMBOL, "")
_builtinarrowtype(::Type{Bool8}) = Int8
_builtintoarrow(x::Bool8) = Int8(Bool(x))
_builtinarrowname(::Type{Bool8}) = BOOL8_SYMBOL
_builtinextensionjuliatype(::Val{BOOL8_SYMBOL}, ::Type{Int8}, metadata::String) = Bool8
_builtinfromarrow(::Type{Bool8}, x::Int8) = Bool8(x)
_builtindefault(::Type{Bool8}) = zero(Bool8)

_builtinextensionspec(::Type{JSONText{S}}) where {S<:AbstractString} =
    ExtensionTypeSpec(JSON_SYMBOL, "")
_builtinarrowtype(::Type{JSONText{S}}) where {S<:AbstractString} = S
_builtintoarrow(x::JSONText) = getfield(x, :value)
_builtinarrowname(::Type{JSONText{S}}) where {S<:AbstractString} = JSON_SYMBOL
_builtinextensionjuliatype(
    ::Val{JSON_SYMBOL},
    ::Type{S},
    metadata::String,
) where {S<:AbstractString} = JSONText{S}
_builtinfromarrow(::Type{JSONText{String}}, ptr::Ptr{UInt8}, len::Int) =
    JSONText(unsafe_string(ptr, len))
_builtinfromarrow(::Type{JSONText{S}}, x::S) where {S<:AbstractString} = JSONText{S}(x)
_builtindefault(::Type{JSONText{S}}) where {S<:AbstractString} =
    JSONText{S}(ArrowTypes.default(S))

_builtinextensionjuliatype(::Val{OPAQUE_SYMBOL}, S, metadata::String) = S
_builtinextensionjuliatype(::Val{PARQUET_VARIANT_SYMBOL}, S, metadata::String) = S
_builtinextensionjuliatype(::Val{FIXED_SHAPE_TENSOR_SYMBOL}, S, metadata::String) = S
_builtinextensionjuliatype(::Val{VARIABLE_SHAPE_TENSOR_SYMBOL}, S, metadata::String) = S
_builtinopaquemetadata(type_name::AbstractString, vendor_name::AbstractString) =
    "{\"type_name\":" *
    _jsonstringliteral(type_name) *
    ",\"vendor_name\":" *
    _jsonstringliteral(vendor_name) *
    "}"
_builtinvariantmetadata() = ""

function _builtinfixedshapetensormetadata(
    shape::AbstractVector{<:Integer};
    dim_names::Union{Nothing,AbstractVector{<:AbstractString}}=nothing,
    permutation::Union{Nothing,AbstractVector{<:Integer}}=nothing,
)
    parsed_shape = _parseintvector(FIXED_SHAPE_TENSOR_SYMBOL, collect(shape), "shape")
    parsed_dim_names = dim_names === nothing ? nothing : String.(dim_names)
    parsed_permutation =
        permutation === nothing ? nothing :
        _validatepermutation(
            FIXED_SHAPE_TENSOR_SYMBOL,
            Int.(permutation),
            length(parsed_shape),
        )
    parsed_dim_names !== nothing && length(parsed_dim_names) == length(parsed_shape) ||
        isnothing(parsed_dim_names) ||
        _canonicalextensionerror(
            FIXED_SHAPE_TENSOR_SYMBOL,
            "\"dim_names\" must have length $(length(parsed_shape))",
        )
    body = Dict{String,Any}("shape" => parsed_shape)
    parsed_dim_names !== nothing && (body["dim_names"] = parsed_dim_names)
    parsed_permutation !== nothing && (body["permutation"] = parsed_permutation)
    return JSON3.write(body)
end

function _builtinvariableshapetensormetadata(;
    uniform_shape::Union{Nothing,AbstractVector}=nothing,
    dim_names::Union{Nothing,AbstractVector{<:AbstractString}}=nothing,
    permutation::Union{Nothing,AbstractVector{<:Integer}}=nothing,
)
    uniform =
        uniform_shape === nothing ? nothing :
        _parseintvector(
            VARIABLE_SHAPE_TENSOR_SYMBOL,
            collect(uniform_shape),
            "uniform_shape";
            allow_null=true,
        )
    ndim = uniform === nothing ? nothing : length(uniform)
    parsed_dim_names = dim_names === nothing ? nothing : String.(dim_names)
    parsed_permutation = permutation === nothing ? nothing : Int.(permutation)
    ndim !== nothing && parsed_dim_names !== nothing && length(parsed_dim_names) == ndim ||
        ndim === nothing ||
        isnothing(parsed_dim_names) ||
        _canonicalextensionerror(
            VARIABLE_SHAPE_TENSOR_SYMBOL,
            "\"dim_names\" must have length $ndim",
        )
    ndim !== nothing &&
        parsed_permutation !== nothing &&
        _validatepermutation(VARIABLE_SHAPE_TENSOR_SYMBOL, parsed_permutation, ndim)
    body = Dict{String,Any}()
    uniform !== nothing && (body["uniform_shape"] = uniform)
    parsed_dim_names !== nothing && (body["dim_names"] = parsed_dim_names)
    parsed_permutation !== nothing && (body["permutation"] = parsed_permutation)
    return isempty(body) ? "" : JSON3.write(body)
end
_validatebuiltinextension(
    ::Val{PARQUET_VARIANT_SYMBOL},
    field::Meta.Field,
    metadata::String,
) = _validateparquetvariant(field, metadata)
_validatebuiltinextension(
    ::Val{FIXED_SHAPE_TENSOR_SYMBOL},
    field::Meta.Field,
    metadata::String,
) = _validatefixedshapetensor(field, metadata)
_validatebuiltinextension(
    ::Val{VARIABLE_SHAPE_TENSOR_SYMBOL},
    field::Meta.Field,
    metadata::String,
) = _validatevariableshapetensor(field, metadata)

_builtinextensionspec(::Type{ZonedDateTime}) = ExtensionTypeSpec(ZONEDDATETIME_SYMBOL, "")
_builtinarrowtype(::Type{ZonedDateTime}) = Timestamp
_builtintoarrow(x::ZonedDateTime) =
    convert(Timestamp{Meta.TimeUnit.MILLISECOND,Symbol(x.timezone)}, x)
_builtinarrowname(::Type{ZonedDateTime}) = ZONEDDATETIME_SYMBOL
_builtinextensionjuliatype(::Val{ZONEDDATETIME_SYMBOL}, S, metadata::String) = ZonedDateTime
_builtinfromarrow(::Type{ZonedDateTime}, x::Timestamp) = convert(ZonedDateTime, x)
_builtindefault(::Type{TimeZones.ZonedDateTime}) =
    TimeZones.ZonedDateTime(1, 1, 1, 1, 1, 1, TimeZones.tz"UTC")

_builtinextensionspec(::Type{TimestampWithOffset{U}}) where {U} =
    ExtensionTypeSpec(TIMESTAMP_WITH_OFFSET_SYMBOL, "")
_builtinarrowtype(::Type{TimestampWithOffset{U}}) where {U} =
    NamedTuple{(:timestamp, :offset_minutes),Tuple{Timestamp{U,:UTC},Int16}}
_builtintoarrow(x::TimestampWithOffset{U}) where {U} =
    (timestamp=getfield(x, :timestamp), offset_minutes=getfield(x, :offset_minutes))
_builtinarrowname(::Type{TimestampWithOffset{U}}) where {U} = TIMESTAMP_WITH_OFFSET_SYMBOL
_builtinextensionjuliatype(
    ::Val{TIMESTAMP_WITH_OFFSET_SYMBOL},
    ::Type{NamedTuple{(:timestamp, :offset_minutes),Tuple{Timestamp{U,:UTC},Int16}}},
    metadata::String,
) where {U} = TimestampWithOffset{U}
_builtindefault(::Type{TimestampWithOffset{U}}) where {U} = zero(TimestampWithOffset{U})
_builtinfromarrowstruct(
    ::Type{TimestampWithOffset{U}},
    ::Val{(:timestamp, :offset_minutes)},
    timestamp::Timestamp{U,:UTC},
    offset_minutes::Int16,
) where {U} = TimestampWithOffset{U}(timestamp, offset_minutes)
_builtinfromarrowstruct(
    ::Type{TimestampWithOffset{U}},
    ::Val{(:offset_minutes, :timestamp)},
    offset_minutes::Int16,
    timestamp::Timestamp{U,:UTC},
) where {U} = TimestampWithOffset{U}(timestamp, offset_minutes)

_builtinextensionjuliatype(::Val{OLD_ZONEDDATETIME_SYMBOL}, S, metadata::String) =
    LocalZonedDateTime
function _builtinfromarrow(::Type{LocalZonedDateTime}, x::Timestamp{U,TZ}) where {U,TZ}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, ZonedDateTime)
    return ZonedDateTime(
        Dates.DateTime(
            Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
        ),
        TimeZone(String(TZ)),
    )
end
