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

_builtinextensionspec(::Type{ArrowTypes.UUID}) =
    ExtensionTypeSpec(ArrowTypes.UUIDSYMBOL, "")
_builtinextensionjuliatype(::Val{ArrowTypes.UUIDSYMBOL}, S, metadata::String) =
    ArrowTypes.UUID
_builtinextensionjuliatype(::Val{ArrowTypes.LEGACY_UUIDSYMBOL}, S, metadata::String) =
    ArrowTypes.UUID

_builtinextensionspec(::Type{Bool8}) = ExtensionTypeSpec(BOOL8_SYMBOL, "")
_builtinextensionjuliatype(::Val{BOOL8_SYMBOL}, ::Type{Int8}, metadata::String) = Bool8

_builtinextensionspec(::Type{JSONText{S}}) where {S<:AbstractString} =
    ExtensionTypeSpec(JSON_SYMBOL, "")
_builtinextensionjuliatype(
    ::Val{JSON_SYMBOL},
    ::Type{S},
    metadata::String,
) where {S<:AbstractString} = JSONText{S}

_builtinextensionjuliatype(::Val{OPAQUE_SYMBOL}, S, metadata::String) = S
_builtinextensionjuliatype(::Val{PARQUET_VARIANT_SYMBOL}, S, metadata::String) = S
_builtinextensionjuliatype(::Val{FIXED_SHAPE_TENSOR_SYMBOL}, S, metadata::String) = S
_builtinextensionjuliatype(::Val{VARIABLE_SHAPE_TENSOR_SYMBOL}, S, metadata::String) = S

_builtinextensionspec(::Type{ZonedDateTime}) = ExtensionTypeSpec(ZONEDDATETIME_SYMBOL, "")
_builtinextensionjuliatype(::Val{ZONEDDATETIME_SYMBOL}, S, metadata::String) = ZonedDateTime

_builtinextensionspec(::Type{TimestampWithOffset{U}}) where {U} =
    ExtensionTypeSpec(TIMESTAMP_WITH_OFFSET_SYMBOL, "")
_builtinextensionjuliatype(
    ::Val{TIMESTAMP_WITH_OFFSET_SYMBOL},
    ::Type{NamedTuple{(:timestamp, :offset_minutes),Tuple{Timestamp{U,:UTC},Int16}}},
    metadata::String,
) where {U} = TimestampWithOffset{U}

_builtinextensionjuliatype(::Val{OLD_ZONEDDATETIME_SYMBOL}, S, metadata::String) =
    LocalZonedDateTime
