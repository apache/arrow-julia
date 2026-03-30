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
    dictencodings = Dict{Int64,Any}()
    messages = Protocol.FlightData[]
    schema = Ref{Tables.Schema}()
    normalized_colmetadata = ArrowParent._normalizecolmeta(colmetadata)
    meta = isnothing(metadata) ? ArrowParent.getmetadata(source) : metadata

    for tbl in Tables.partitions(source)
        tblcols = Tables.columns(tbl)
        cols = ArrowParent.toarrowtable(
            tblcols,
            dictencodings,
            largelists,
            compress,
            denseunions,
            dictencode,
            dictencodenested,
            maxdepth,
            meta,
            normalized_colmetadata,
        )
        if !isassigned(schema)
            schema[] = Tables.schema(cols)
            push!(
                messages,
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
                    push!(
                        messages,
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
                push!(
                    messages,
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
        push!(
            messages,
            _flightdata_message(
                ArrowParent.makerecordbatchmsg(schema[], cols, alignment);
                alignment=alignment,
            ),
        )
        descriptor = nothing
    end
    return messages
end
