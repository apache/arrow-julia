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

function _sourcedefaultcolmetadata(cols)
    sch = Tables.schema(cols)
    isnothing(sch) && return nothing
    colmeta = Dict{Symbol,Any}()
    Tables.eachcolumn(sch, cols) do col, _, nm
        meta = ArrowParent.getmetadata(col)
        isnothing(meta) || (colmeta[nm] = meta)
    end
    isempty(colmeta) && return nothing
    return ArrowParent._normalizecolmeta(colmeta)
end

struct FlightAppMetadataSource{T,M}
    source::T
    app_metadata::M
end

ArrowParent.getmetadata(x::FlightAppMetadataSource) = ArrowParent.getmetadata(x.source)

"""
    Arrow.Flight.withappmetadata(source; app_metadata)

Return a lightweight wrapper around `source` that carries batch-wise Flight
`app_metadata` alongside the Arrow payload. The wrapper can be passed directly
to [`Arrow.Flight.flightdata`](@ref), [`Arrow.Flight.putflightdata!`](@ref),
or source-based [`Arrow.Flight.doexchange`](@ref) without manually threading
`app_metadata=...` through each call site.
"""
withappmetadata(source; app_metadata) =
    isnothing(app_metadata) ? source : FlightAppMetadataSource(source, app_metadata)

function _unwrap_app_metadata_source(source, app_metadata)
    source isa FlightAppMetadataSource || return source, app_metadata
    isnothing(app_metadata) || throw(
        ArgumentError(
            "app_metadata cannot be provided both via Arrow.Flight.withappmetadata(...) and the app_metadata keyword",
        ),
    )
    return source.source, source.app_metadata
end

_is_app_metadata_value(x) = x isa AbstractString || x isa AbstractVector{UInt8}

function _normalize_app_metadata_value(value)
    value === nothing && return UInt8[]
    value isa AbstractString && return Vector{UInt8}(codeunits(value))
    value isa AbstractVector{UInt8} && return Vector{UInt8}(value)
    throw(
        ArgumentError(
            "app_metadata entries must be AbstractString, AbstractVector{UInt8}, or nothing",
        ),
    )
end

function _normalize_app_metadata_source(app_metadata)
    isnothing(app_metadata) && return nothing
    return _is_app_metadata_value(app_metadata) ? (app_metadata,) : app_metadata
end

_app_metadata_cursor(app_metadata) =
    let metadata_iter = _normalize_app_metadata_source(app_metadata)
        isnothing(metadata_iter) ? nothing :
        (iter=metadata_iter, state=nothing, started=false)
    end

function _next_app_metadata(cursor)
    isnothing(cursor) && return UInt8[], cursor
    iter = cursor.iter
    next = cursor.started ? iterate(iter, cursor.state) : iterate(iter)
    isnothing(next) && throw(
        ArgumentError("app_metadata was exhausted before all record batches were emitted"),
    )
    value, state = next
    return _normalize_app_metadata_value(value), (iter=iter, state=state, started=true)
end

function _ensure_app_metadata_consumed(cursor)
    isnothing(cursor) && return nothing
    next = cursor.started ? iterate(cursor.iter, cursor.state) : iterate(cursor.iter)
    isnothing(next) && return nothing
    throw(ArgumentError("app_metadata contains more entries than source partitions"))
end

function _partition_with_app_metadata(tbl, cursor)
    app_metadata, cursor = _next_app_metadata(cursor)
    return tbl, app_metadata, cursor
end

function _emitflightdata!(
    emit,
    source;
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    app_metadata=nothing,
)
    source, app_metadata = _unwrap_app_metadata_source(source, app_metadata)
    dictencodings = Dict{Int64,Any}()
    schema = Ref{Tables.Schema}()
    normalized_colmetadata = ArrowParent._normalizecolmeta(colmetadata)
    source_meta = isnothing(metadata) ? ArrowParent.getmetadata(source) : metadata
    source_colmetadata = isnothing(colmetadata) ? nothing : normalized_colmetadata
    app_metadata_cursor = _app_metadata_cursor(app_metadata)

    for partition in Tables.partitions(source)
        tbl, record_app_metadata, app_metadata_cursor =
            _partition_with_app_metadata(partition, app_metadata_cursor)
        tblcols = Tables.columns(tbl)
        if isnothing(metadata)
            tblmeta = ArrowParent.getmetadata(tbl)
            isnothing(tblmeta) && (tblmeta = source_meta)
        else
            tblmeta = metadata
        end
        if isnothing(colmetadata)
            tblcolmetadata = _sourcedefaultcolmetadata(tblcols)
            isnothing(tblcolmetadata) && (tblcolmetadata = source_colmetadata)
        else
            tblcolmetadata = normalized_colmetadata
        end
        cols = ArrowParent.toarrowtable(
            tblcols,
            dictencodings,
            largelists,
            compress,
            denseunions,
            dictencode,
            dictencodenested,
            maxdepth,
            tblmeta,
            tblcolmetadata,
        )
        if !isassigned(schema)
            schema[] = Tables.schema(cols)
            emit(
                _flightdata_message(
                    ArrowParent.makeschemamsg(schema[], cols);
                    descriptor=descriptor,
                    alignment=alignment,
                ),
            )
            if !isempty(dictencodings)
                for (id, delock) in sort!(collect(dictencodings); by=x -> x.first, rev=true)
                    de = delock.value
                    dictsch = Tables.Schema((:col,), (eltype(de.data),))
                    emit(
                        _flightdata_message(
                            ArrowParent.makedictionarybatchmsg(
                                dictsch,
                                (col=de.data,),
                                id,
                                false,
                                alignment,
                            );
                            alignment=alignment,
                        ),
                    )
                end
            end
        elseif !isempty(cols.dictencodingdeltas)
            for de in cols.dictencodingdeltas
                dictsch = Tables.Schema((:col,), (eltype(de.data),))
                emit(
                    _flightdata_message(
                        ArrowParent.makedictionarybatchmsg(
                            dictsch,
                            (col=de.data,),
                            de.id,
                            true,
                            alignment,
                        );
                        alignment=alignment,
                    ),
                )
            end
        end
        emit(
            _flightdata_message(
                ArrowParent.makerecordbatchmsg(schema[], cols, alignment);
                app_metadata=record_app_metadata,
                alignment=alignment,
            ),
        )
        descriptor = nothing
    end
    _ensure_app_metadata_consumed(app_metadata_cursor)
    return nothing
end

function flightdata(
    source;
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    app_metadata=nothing,
)
    messages = Protocol.FlightData[]
    _emitflightdata!(
        message -> push!(messages, message),
        source;
        descriptor=descriptor,
        compress=compress,
        largelists=largelists,
        denseunions=denseunions,
        dictencode=dictencode,
        dictencodenested=dictencodenested,
        alignment=alignment,
        maxdepth=maxdepth,
        metadata=metadata,
        colmetadata=colmetadata,
        app_metadata=app_metadata,
    )
    return messages
end

function putflightdata!(
    sink,
    source;
    close::Bool=false,
    descriptor::Union{Nothing,Protocol.FlightDescriptor}=nothing,
    compress=nothing,
    largelists::Bool=false,
    denseunions::Bool=true,
    dictencode::Bool=false,
    dictencodenested::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    maxdepth::Integer=ArrowParent.DEFAULT_MAX_DEPTH,
    metadata::Union{Nothing,Any}=nothing,
    colmetadata::Union{Nothing,Any}=nothing,
    app_metadata=nothing,
)
    try
        _emitflightdata!(
            message -> put!(sink, message),
            source;
            descriptor=descriptor,
            compress=compress,
            largelists=largelists,
            denseunions=denseunions,
            dictencode=dictencode,
            dictencodenested=dictencodenested,
            alignment=alignment,
            maxdepth=maxdepth,
            metadata=metadata,
            colmetadata=colmetadata,
            app_metadata=app_metadata,
        )
    finally
        close && Base.close(sink)
    end
    return sink
end
