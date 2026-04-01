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

_metadata_entries(metadata) = metadata isa AbstractVector ? metadata : pairs(metadata)

function _normalize_metadata_overlay(metadata)
    metadata === nothing && return nothing
    return toidict(
        String(first(entry)) => String(last(entry)) for entry in _metadata_entries(metadata)
    )
end

function _merge_metadata_overlays(metadata_sources...)
    merged = Dict{String,String}()
    for metadata in metadata_sources
        metadata === nothing && continue
        for entry in _metadata_entries(metadata)
            merged[String(first(entry))] = String(last(entry))
        end
    end
    return isempty(merged) ? nothing : toidict(pairs(merged))
end

struct MetadataOverlayVector{T,V<:AbstractVector{T},M} <: AbstractVector{T}
    data::V
    metadata::M
end

Base.IndexStyle(::Type{<:MetadataOverlayVector{T,V}}) where {T,V} = Base.IndexStyle(V)
Base.size(x::MetadataOverlayVector) = size(x.data)
Base.axes(x::MetadataOverlayVector) = axes(x.data)
Base.length(x::MetadataOverlayVector) = length(x.data)
Base.getindex(x::MetadataOverlayVector, i::Int) = x.data[i]
Base.iterate(x::MetadataOverlayVector, state...) = iterate(x.data, state...)
getmetadata(x::MetadataOverlayVector) = x.metadata

struct MetadataOverlayTable{N,C,M}
    columns::NamedTuple{N,C}
    metadata::M
end

function Base.getproperty(x::MetadataOverlayTable, name::Symbol)
    if name === :columns || name === :metadata
        return getfield(x, name)
    end
    columns = getfield(x, :columns)
    if hasproperty(columns, name)
        return getproperty(columns, name)
    end
    return getfield(x, name)
end

function Base.propertynames(x::MetadataOverlayTable, private::Bool=false)
    column_names = propertynames(getfield(x, :columns))
    return private ? (:columns, :metadata, column_names...) : column_names
end

Tables.istable(::Type{<:MetadataOverlayTable}) = true
Tables.columnaccess(::Type{<:MetadataOverlayTable}) = true
Tables.columns(x::MetadataOverlayTable) = getfield(x, :columns)
Tables.schema(x::MetadataOverlayTable) = Tables.schema(getfield(x, :columns))
getmetadata(x::MetadataOverlayTable) = getfield(x, :metadata)

function _column_metadata_overlay(table_like)
    merged = Dict{Symbol,Any}()
    for name in Tables.schema(table_like).names
        metadata =
            _normalize_metadata_overlay(getmetadata(Tables.getcolumn(table_like, name)))
        metadata === nothing || (merged[name] = metadata)
    end
    return merged
end

function _merge_column_metadata_overlays(table_like, colmetadata)
    merged = _column_metadata_overlay(table_like)
    colmetadata === nothing && return merged
    for (name, metadata) in pairs(colmetadata)
        symbol_name = Symbol(name)
        merged_metadata =
            _merge_metadata_overlays(get(merged, symbol_name, nothing), metadata)
        merged_metadata === nothing || (merged[symbol_name] = merged_metadata)
    end
    return merged
end

function _metadata_overlay_table(columns::NamedTuple; metadata=nothing, colmetadata=nothing)
    wrapped_columns = Pair{Symbol,Any}[]
    for name in keys(columns)
        column_metadata = isnothing(colmetadata) ? nothing : get(colmetadata, name, nothing)
        push!(
            wrapped_columns,
            name => MetadataOverlayVector(columns[name], column_metadata),
        )
    end
    return MetadataOverlayTable((; wrapped_columns...), metadata)
end

"""
    Arrow.withmetadata(table_like; metadata=nothing, colmetadata=nothing)

Return a lightweight Tables.jl-compatible wrapper around `table_like` that
preserves any existing Arrow schema/field metadata and overlays additional
schema `metadata` and column `colmetadata` for subsequent Arrow serialization.

Both `metadata` and `colmetadata` follow the same shape accepted by
[`Arrow.write`](@ref): schema metadata must be an iterable of string-like pairs,
while `colmetadata` must map column names to iterables of string-like pairs.
When the source already carries metadata, overlay entries win on key conflicts.
"""
function withmetadata(columns::NamedTuple; metadata=nothing, colmetadata=nothing)
    normalized_metadata = _normalize_metadata_overlay(metadata)
    normalized_colmetadata = if isnothing(colmetadata)
        nothing
    else
        Dict(
            Symbol(name) => _normalize_metadata_overlay(column_metadata) for
            (name, column_metadata) in pairs(colmetadata)
        )
    end
    if normalized_metadata === nothing && isnothing(normalized_colmetadata)
        return columns
    end
    return _metadata_overlay_table(
        columns;
        metadata=normalized_metadata,
        colmetadata=normalized_colmetadata,
    )
end

function withmetadata(table_like; metadata=nothing, colmetadata=nothing)
    merged_metadata = _merge_metadata_overlays(getmetadata(table_like), metadata)
    merged_colmetadata = _merge_column_metadata_overlays(table_like, colmetadata)
    if merged_metadata === nothing && isempty(merged_colmetadata)
        return table_like
    end
    return _metadata_overlay_table(
        Tables.columntable(table_like);
        metadata=merged_metadata,
        colmetadata=isempty(merged_colmetadata) ? nothing : merged_colmetadata,
    )
end
