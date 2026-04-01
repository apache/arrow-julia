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

streambytes(message::Protocol.FlightData; kwargs...) =
    streambytes(Protocol.FlightData[message]; kwargs...)

mutable struct FlightStream{M}
    messages::M
    state::Any
    started::Bool
    exhausted::Bool
    nextid::Int
    names::Vector{Symbol}
    types::Vector{Type}
    schema::Union{Nothing,ArrowParent.Meta.Schema}
    dictencodings::ArrowParent.Lockable{Dict{Int64,ArrowParent.DictEncoding}}
    dictencoded::Dict{Int64,ArrowParent.Meta.Field}
    convert::Bool
end

struct FlightStreamWithAppMetadata{S}
    stream::S
end

function FlightStream(messages; schema=nothing, convert::Bool=true)
    x = FlightStream(
        messages,
        nothing,
        false,
        false,
        0,
        Symbol[],
        Type[],
        nothing,
        ArrowParent.Lockable(Dict{Int64,ArrowParent.DictEncoding}()),
        Dict{Int64,ArrowParent.Meta.Field}(),
        convert,
    )
    schema === nothing || _register_schema!(x, _flight_schema(schema))
    return x
end

Base.IteratorSize(::Type{<:FlightStream}) = Base.SizeUnknown()
Base.eltype(::Type{<:FlightStream}) = ArrowParent.Table
Base.isdone(x::FlightStream) = x.exhausted

Base.IteratorSize(::Type{<:FlightStreamWithAppMetadata}) = Base.SizeUnknown()
Base.eltype(::Type{<:FlightStreamWithAppMetadata}) =
    NamedTuple{(:table, :app_metadata),Tuple{ArrowParent.Table,Vector{UInt8}}}

Tables.partitions(x::FlightStream) = x
Tables.partitions(x::FlightStreamWithAppMetadata) = x

function Tables.columnnames(x::FlightStream)
    _ensure_schema!(x)
    return getfield(x, :names)
end

function Tables.schema(x::FlightStream)
    _ensure_schema!(x)
    return Tables.Schema(Tables.columnnames(x), getfield(x, :types))
end

function Base.iterate(x::FlightStream)
    return _iterate_flight_stream!(x)
end

function Base.iterate(x::FlightStream, ::Nothing)
    return _iterate_flight_stream!(x)
end

function Base.iterate(x::FlightStreamWithAppMetadata)
    return _iterate_flight_stream!(x.stream; include_app_metadata=true)
end

function Base.iterate(x::FlightStreamWithAppMetadata, ::Nothing)
    return _iterate_flight_stream!(x.stream; include_app_metadata=true)
end

function _missing_schema_message()
    return join(
        [
            "cannot derive Arrow Flight schema from a response stream without a schema message",
            "the server may have terminated the stream before emitting the first schema-bearing FlightData message",
            "or the underlying transport did not surface the corresponding gRPC status",
        ],
        "; ",
    )
end

function _require_schema_messages(messages::AbstractVector{<:Protocol.FlightData}, schema)
    schema === nothing || return messages
    any(message -> !isempty(message.data_header), messages) && return messages
    throw(ArgumentError(_missing_schema_message()))
end

function _flight_schema(schema)
    schema isa ArrowParent.Meta.Schema && return schema
    bytes = schemaipc(schema)
    message = ArrowParent.FlatBuffers.getrootas(ArrowParent.Meta.Message, bytes, 8)
    header = message.header
    header isa ArrowParent.Meta.Schema ||
        throw(ArgumentError("Flight schema payload did not decode to an Arrow IPC schema"))
    return header
end

function _register_schema!(x::FlightStream, schema::ArrowParent.Meta.Schema)
    if isnothing(getfield(x, :schema))
        setfield!(x, :schema, schema)
        for field in schema.fields
            ArrowParent.rejectunsupported(field)
            push!(getfield(x, :names), Symbol(field.name))
            push!(
                getfield(x, :types),
                ArrowParent.juliaeltype(
                    field,
                    ArrowParent.buildmetadata(field.custom_metadata),
                    getfield(x, :convert),
                ),
            )
            ArrowParent.getdictionaries!(getfield(x, :dictencoded), field)
        end
        return x
    end
    schema == getfield(x, :schema) || throw(
        ArgumentError(
            "mismatched schemas between different arrow batches: $(getfield(x, :schema)) != $schema",
        ),
    )
    return x
end

function _next_flight_message!(x::FlightStream)
    getfield(x, :exhausted) && return nothing
    state =
        getfield(x, :started) ? iterate(getfield(x, :messages), getfield(x, :state)) :
        iterate(getfield(x, :messages))
    setfield!(x, :started, true)
    state === nothing && return (setfield!(x, :exhausted, true); nothing)
    message, next_state = state
    setfield!(x, :state, next_state)
    setfield!(x, :nextid, getfield(x, :nextid) + 1)
    return message
end

function _flight_batch(message::Protocol.FlightData, id::Integer)
    isempty(message.data_header) &&
        throw(ArgumentError("FlightData message is missing the Arrow IPC header"))
    msg =
        ArrowParent.FlatBuffers.getrootas(ArrowParent.Meta.Message, message.data_header, 0)
    return ArrowParent.Batch(msg, message.data_body, 1, Int(id))
end

function _ensure_schema!(x::FlightStream)
    isnothing(getfield(x, :schema)) || return x
    while true
        message = _next_flight_message!(x)
        message === nothing && throw(ArgumentError(_missing_schema_message()))
        if isempty(message.data_header)
            isempty(message.data_body) || throw(
                ArgumentError("FlightData message has a body but no Arrow IPC header"),
            )
            continue
        end
        batch = _flight_batch(message, getfield(x, :nextid))
        header = batch.msg.header
        if header isa ArrowParent.Meta.Schema
            _register_schema!(x, header)
            return x
        elseif header isa ArrowParent.Meta.Tensor
            throw(ArgumentError(ArrowParent.TENSOR_UNSUPPORTED))
        elseif header isa ArrowParent.Meta.SparseTensor
            throw(ArgumentError(ArrowParent.SPARSE_TENSOR_UNSUPPORTED))
        end
        throw(ArgumentError(_missing_schema_message()))
    end
end

function _store_dictionary_batch!(
    x::FlightStream,
    batch,
    header::ArrowParent.Meta.DictionaryBatch,
)
    id = header.id
    recordbatch = header.data
    @lock getfield(x, :dictencodings) begin
        dictencodings = getfield(x, :dictencodings)[]
        if haskey(dictencodings, id) && header.isDelta
            field = getfield(x, :dictencoded)[id]
            values, _, _, _ = ArrowParent.build(
                field,
                field.type,
                batch,
                recordbatch,
                getfield(x, :dictencodings),
                Int64(1),
                Int64(1),
                Int64(1),
                getfield(x, :convert),
            )
            dictencoding = dictencodings[id]
            append!(dictencoding.data, values)
            return
        end
        field = getfield(x, :dictencoded)[id]
        values, _, _, _ = ArrowParent.build(
            field,
            field.type,
            batch,
            recordbatch,
            getfield(x, :dictencodings),
            Int64(1),
            Int64(1),
            Int64(1),
            getfield(x, :convert),
        )
        A = ArrowParent.ChainedVector([values])
        S =
            field.dictionary.indexType === nothing ? Int32 :
            ArrowParent.juliaeltype(field, field.dictionary.indexType, false)
        dictencodings[id] = ArrowParent.DictEncoding{eltype(A),S,typeof(A)}(
            id,
            A,
            field.dictionary.isOrdered,
            values.metadata,
        )
    end
    return nothing
end

function _flight_table(x::FlightStream, columns)
    schema = getfield(x, :schema)
    schema === nothing && throw(ArgumentError(_missing_schema_message()))
    lookup = Dict{Symbol,AbstractVector}()
    types = Type[]
    for (nm, col) in zip(getfield(x, :names), columns)
        lookup[nm] = col
        push!(types, eltype(col))
    end
    return ArrowParent.Table(getfield(x, :names), types, columns, lookup, Ref(schema))
end

function _empty_flight_table(x::FlightStream)
    schema = getfield(x, :schema)
    schema === nothing && throw(ArgumentError(_missing_schema_message()))
    names = copy(getfield(x, :names))
    types = copy(getfield(x, :types))
    columns = AbstractVector[]
    for field in schema.fields
        T = ArrowParent.juliaeltype(
            field,
            ArrowParent.buildmetadata(field.custom_metadata),
            getfield(x, :convert),
        )
        push!(columns, T[])
    end
    lookup = Dict{Symbol,AbstractVector}(names[i] => columns[i] for i in eachindex(names))
    return ArrowParent.Table(names, types, columns, lookup, Ref(schema))
end

function _copy_flight_table(batch::ArrowParent.Table)
    names = copy(ArrowParent.names(batch))
    types = copy(ArrowParent.types(batch))
    columns = copy(ArrowParent.columns(batch))
    schema = ArrowParent.schema(batch)[]
    lookup = Dict{Symbol,AbstractVector}(names[i] => columns[i] for i in eachindex(names))
    return ArrowParent.Table(names, types, columns, lookup, Ref(schema))
end

_copy_app_metadata(message::Protocol.FlightData) = copy(message.app_metadata)

function _flight_batch_result(
    table::ArrowParent.Table,
    message::Protocol.FlightData;
    include_app_metadata::Bool,
)
    include_app_metadata || return table
    return (table=table, app_metadata=_copy_app_metadata(message))
end

_flightcolumndata(col) = ArrowParent._metadatavectordata(col)

function _chain_flight_column(col, batch_col)
    metadata = ArrowParent.getmetadata(col)
    chained =
        ArrowParent.ChainedVector([_flightcolumndata(col), _flightcolumndata(batch_col)])
    return ArrowParent._wrapmetadata(chained, metadata)
end

function _append_flight_column!(col, batch_col)
    append!(_flightcolumndata(col), _flightcolumndata(batch_col))
    return col
end

function _append_flight_batch!(
    table::ArrowParent.Table,
    batch::ArrowParent.Table,
    batchindex::Int,
)
    columns = ArrowParent.columns(table)
    batch_columns = ArrowParent.columns(batch)
    if batchindex == 2
        for i in eachindex(columns)
            columns[i] = _chain_flight_column(columns[i], batch_columns[i])
        end
    else
        for i in eachindex(columns)
            _append_flight_column!(columns[i], batch_columns[i])
        end
    end
    lookup = getfield(table, :lookup)
    for (nm, col) in zip(ArrowParent.names(table), columns)
        lookup[nm] = col
    end
    return table
end

function _materialize_flight_table(
    messages;
    schema=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
)
    stream_state = FlightStream(messages; schema=schema, convert=convert)
    state = _iterate_flight_stream!(stream_state; include_app_metadata=include_app_metadata)
    if state === nothing
        empty_table = _empty_flight_table(stream_state)
        return include_app_metadata ? (table=empty_table, app_metadata=Vector{UInt8}[]) :
               empty_table
    end
    first_value, _ = state
    first_table = include_app_metadata ? first_value.table : first_value
    out = _copy_flight_table(first_table)
    batch_app_metadata =
        include_app_metadata ? Vector{Vector{UInt8}}([first_value.app_metadata]) : nothing
    batchindex = 2
    while true
        next =
            _iterate_flight_stream!(stream_state; include_app_metadata=include_app_metadata)
        next === nothing && break
        batch_value, _ = next
        batch = include_app_metadata ? batch_value.table : batch_value
        _append_flight_batch!(out, batch, batchindex)
        include_app_metadata && push!(batch_app_metadata, batch_value.app_metadata)
        batchindex += 1
    end
    return include_app_metadata ? (table=out, app_metadata=batch_app_metadata) : out
end

function _iterate_flight_stream!(x::FlightStream; include_app_metadata::Bool=false)
    _ensure_schema!(x)
    while true
        message = _next_flight_message!(x)
        message === nothing && return nothing
        if isempty(message.data_header)
            isempty(message.data_body) || throw(
                ArgumentError("FlightData message has a body but no Arrow IPC header"),
            )
            continue
        end
        batch = _flight_batch(message, getfield(x, :nextid))
        header = batch.msg.header
        if header isa ArrowParent.Meta.Schema
            _register_schema!(x, header)
            continue
        elseif header isa ArrowParent.Meta.DictionaryBatch
            _store_dictionary_batch!(x, batch, header)
            continue
        elseif header isa ArrowParent.Meta.RecordBatch
            columns = collect(
                ArrowParent.VectorIterator(
                    getfield(x, :schema),
                    batch,
                    getfield(x, :dictencodings),
                    getfield(x, :convert),
                ),
            )
            return _flight_batch_result(
                _flight_table(x, columns),
                message;
                include_app_metadata=include_app_metadata,
            ),
            nothing
        elseif header isa ArrowParent.Meta.Tensor
            throw(ArgumentError(ArrowParent.TENSOR_UNSUPPORTED))
        elseif header isa ArrowParent.Meta.SparseTensor
            throw(ArgumentError(ArrowParent.SPARSE_TENSOR_UNSUPPORTED))
        end
        throw(ArgumentError("unsupported arrow message type: $(typeof(header))"))
    end
end

function streambytes(
    messages;
    schema=nothing,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    collected = _require_schema_messages(_collect_messages(messages), schema)
    io = IOBuffer()
    schema === nothing || Base.write(io, schemaipc(schema; alignment=alignment))
    for message in collected
        if isempty(message.data_header)
            isempty(message.data_body) || throw(
                ArgumentError("FlightData message has a body but no Arrow IPC header"),
            )
            continue
        end
        _write_framed_message(io, message.data_header, message.data_body, alignment)
    end
    end_marker && _write_end_marker(io)
    return take!(io)
end

function stream(
    messages;
    schema=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    messages isa AbstractVector{<:Protocol.FlightData} &&
        _require_schema_messages(messages, schema)
    flight_stream = FlightStream(messages; schema=schema, convert=convert)
    return include_app_metadata ? FlightStreamWithAppMetadata(flight_stream) : flight_stream
end

function table(
    messages;
    schema=nothing,
    convert::Bool=true,
    include_app_metadata::Bool=false,
    alignment::Integer=DEFAULT_IPC_ALIGNMENT,
    end_marker::Bool=true,
)
    return _materialize_flight_table(
        messages;
        schema=schema,
        convert=convert,
        include_app_metadata=include_app_metadata,
    )
end
