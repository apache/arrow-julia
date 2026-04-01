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
)
    dictencodings = Dict{Int64,Any}()
    schema = Ref{Tables.Schema}()
    normalized_colmetadata = ArrowParent._normalizecolmeta(colmetadata)
    source_meta = isnothing(metadata) ? ArrowParent.getmetadata(source) : metadata
    source_colmetadata = isnothing(colmetadata) ? nothing : normalized_colmetadata

    for tbl in Tables.partitions(source)
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
                alignment=alignment,
            ),
        )
        descriptor = nothing
    end
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
        )
    finally
        close && Base.close(sink)
    end
    return sink
end
