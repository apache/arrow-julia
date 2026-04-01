# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

const EXTENSION_NAME_KEY = "ARROW:extension:name"
const EXTENSION_METADATA_KEY = "ARROW:extension:metadata"

struct ExtensionTypeSpec
    name::Symbol
    metadata::String
end

@inline _extensiontypename(spec::ExtensionTypeSpec) = String(spec.name)
@inline _builtinextensionspec(::Type{T}) where {T} = nothing
@inline _builtinextensionjuliatype(::Val{name}, storageT) where {name} =
    _builtinextensionjuliatype(Val(name), storageT, "")
@inline _builtinextensionjuliatype(::Val{name}, storageT, metadata) where {name} = nothing
@inline _builtinarrowtype(::Type{T}) where {T} = nothing
@inline _builtintoarrow(x) = nothing
@inline _builtinarrowname(::Type{T}) where {T} = nothing
function _builtinfromarrow end
function _builtinfromarrowstruct end
function _builtindefault end
function _builtinopaquemetadata end
function _builtinvariantmetadata end
function _builtinfixedshapetensormetadata end
function _builtinvariableshapetensormetadata end
@inline _validatebuiltinextension(::Val{name}, field, metadata) where {name} = nothing

@inline function _extensionmetadatafor(::Type{T}, meta) where {T}
    spec = _extensionspec(T)
    spec === nothing && return meta
    return _mergeextensionmeta(meta, spec)
end

@inline function _extensionspec(::Type{T}) where {T}
    spec = _builtinextensionspec(T)
    spec !== nothing && return spec
    ArrowTypes.hasarrowname(T) || return nothing
    return ExtensionTypeSpec(ArrowTypes.arrowname(T), String(ArrowTypes.arrowmetadata(T)))
end

@inline function _extensionspec(meta::AbstractDict{String,String})
    haskey(meta, EXTENSION_NAME_KEY) || return nothing
    return ExtensionTypeSpec(
        Symbol(meta[EXTENSION_NAME_KEY]),
        get(meta, EXTENSION_METADATA_KEY, ""),
    )
end

function _mergeextensionmeta(::Nothing, spec::ExtensionTypeSpec)
    return toidict((
        EXTENSION_NAME_KEY => _extensiontypename(spec),
        EXTENSION_METADATA_KEY => spec.metadata,
    ),)
end

function _mergeextensionmeta(::Nothing, name::Symbol, metadata::String)
    return toidict((EXTENSION_NAME_KEY => String(name), EXTENSION_METADATA_KEY => metadata))
end

function _mergeextensionmeta(meta, spec::ExtensionTypeSpec)
    dict = Dict(meta)
    dict[EXTENSION_NAME_KEY] = _extensiontypename(spec)
    dict[EXTENSION_METADATA_KEY] = spec.metadata
    return toidict(dict)
end

function _mergeextensionmeta(meta, name::Symbol, metadata::String)
    dict = Dict(meta)
    dict[EXTENSION_NAME_KEY] = String(name)
    dict[EXTENSION_METADATA_KEY] = metadata
    return toidict(dict)
end

@inline function _builtinextensionjuliatype(spec::ExtensionTypeSpec, storageT)
    return _builtinextensionjuliatype(Val(spec.name), storageT, spec.metadata)
end

@inline function _resolveextensionjuliatype(spec::ExtensionTypeSpec, storageT)
    builtin = _builtinextensionjuliatype(spec, storageT)
    builtin !== nothing && return builtin
    return ArrowTypes.JuliaType(Val(spec.name), storageT, spec.metadata)
end

@inline function _validatebuiltinextension(spec::ExtensionTypeSpec, field::Meta.Field)
    return _validatebuiltinextension(Val(spec.name), field, spec.metadata)
end
