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

# =============================================================================
# The write facade: Tables.jl source -> Arrow IPC bytes.
#
# Column building sits on ArrowCore's builders (`fromjulia` and friends) plus
# the Dates conversions the facade owns (Core is deliberately
# conversion-free so the adapters share it unchanged). Batch emission is the
# IPC adapter's `writestream`/`writefile`; every column is validated before
# its bytes are published, exactly as the adapter promises.
# =============================================================================

"""
    Arrow.DictEncode(v)

Mark a column for dictionary encoding: the writer builds a pool of the
column's unique values and encodes slots as indices into it.
"""
struct DictEncode{V<:AbstractVector} <: AbstractVector{Any}
    data::V
end
Base.size(d::DictEncode) = size(d.data)
Base.getindex(d::DictEncode, i::Base.Int) = d.data[i]
Base.eltype(::Type{DictEncode{V}}) where {V} = eltype(V)

"One (Field, ArrayData) column from a Julia vector, facade conversions included."
function _writecolumn(name::String, v::AbstractVector)
    T = Base.nonmissingtype(eltype(v))
    if T <: Dates.Date
        return _temporalcolumn(name, v, AC.DateType(AC.DAY),
            x -> Int32(Dates.value(x) - _EPOCH_DAYS))
    elseif T <: Dates.DateTime
        return _temporalcolumn(name, v,
            AC.TimestampType(AC.MILLISECOND, nothing),
            x -> Int64(Dates.value(x) - Dates.UNIXEPOCH))
    elseif T <: Dates.Time
        return _temporalcolumn(name, v, AC.TimeType(AC.NANOSECOND, 64),
            x -> Int64(Dates.value(x)))
    elseif T <: Dates.Period && T <: Union{Dates.Second,Dates.Millisecond,
        Dates.Microsecond,Dates.Nanosecond}
        unit = T <: Dates.Second ? AC.SECOND :
            T <: Dates.Millisecond ? AC.MILLISECOND :
            T <: Dates.Microsecond ? AC.MICROSECOND : AC.NANOSECOND
        return _temporalcolumn(name, v, AC.DurationType(unit),
            x -> Int64(Dates.value(x)))
    elseif T <: NamedTuple
        any(ismissing, v) && throw(ArgumentError(
            "missing struct slots are not yet supported by the writer " *
            "(column $name); wrap fields as nullable children instead"))
        cols = NamedTuple{fieldnames(T)}(Tuple([getfield(x, k) for x in v]
            for k in fieldnames(T)))
        return AC.fromjulia_struct(name, cols)
    elseif T <: AbstractString && T != String
        return AC.fromjulia(name, _missings_to(String, v))
    else
        return AC.fromjulia(name, _plainvector(v))
    end
end

function _writecolumn(name::String, d::DictEncode)
    v = d.data
    pool = unique(skipmissing(v))
    lookup = Dict{Any,Int32}(x => Int32(i - 1) for (i, x) in enumerate(pool))
    indices = Union{Missing,Int32}[x === missing ? missing : lookup[x]
                                   for x in v]
    return AC.fromjulia_dict(name, collect(pool), indices)
end

# Days from Julia's Date epoch (0000-12-31) to the Arrow epoch (1970-01-01).
const _EPOCH_DAYS = Dates.value(Dates.Date(1970, 1, 1))

"Concrete Vector with an exact Union{Missing,T} or T eltype for fromjulia."
function _plainvector(v::AbstractVector)
    T = eltype(v)
    return v isa Vector{T} ? v : collect(T, v)
end

_missings_to(::Type{S}, v) where {S} =
    eltype(v) >: Missing ?
    Union{Missing,S}[x === missing ? missing : S(x) for x in v] :
    S[S(x) for x in v]

"Temporal column: convert values to storage integers, keep the validity."
function _temporalcolumn(name::String, v::AbstractVector, t::AC.ArrowType,
    tostorage::F) where {F}
    storage = Union{Missing,Int64}[x === missing ? missing :
                                   Int64(tostorage(x)) for x in v]
    f0, d0 = AC.fromjulia(name, storage)
    # Rebuild under the temporal descriptor with the storage width it
    # declares (Date32 narrows to Int32 storage).
    width = AC.primwidth(t)
    buffers = d0.buffers
    if width == 4
        narrow = Vector{Int32}(undef, length(v))
        for (i, x) in enumerate(storage)
            narrow[i] = x === missing ? Int32(0) : Int32(x)
        end
        buffers = [d0.buffers[1], AC._databuffer(narrow)]
    end
    d = AC._arraydata(t, d0.len, buffers, 0, AC.ArrayData[], nothing,
        d0.owner, AC.nullcount(d0))
    return AC.Field(name, t; nullable=eltype(v) >: Missing), d
end

"""
    Arrow.write(sink, table; file=true, compress=nothing,
                metadata=nothing, colmetadata=nothing)

Write any Tables.jl source as Arrow IPC. `sink` is a file path or an `IO`.
`file=true` emits the random-access file format (`ARROW1` magic + footer);
`file=false` the stream format. Each `Tables.partitions` partition becomes
one record batch. `compress` is `nothing`, `:lz4`, or `:zstd`.
`metadata`/`colmetadata` attach schema- and per-column key-value pairs
(a `Dict`, or pairs; `colmetadata` maps column name `Symbol`s to them).

The writer is eager and whole-buffer: batches are encoded and validated in
memory, then written to the sink once.
"""
function write(path::AbstractString, tbl; kwargs...)
    bytes = _writebytes(tbl; kwargs...)
    open(path, "w") do io
        Base.write(io, bytes)
    end
    return path
end

function write(io::IO, tbl; kwargs...)
    bytes = _writebytes(tbl; kwargs...)
    Base.write(io, bytes)
    return io
end

function _writebytes(tbl; file::Bool=true, compress::Union{Nothing,Symbol}=nothing,
    metadata=nothing, colmetadata=nothing)
    sch = nothing
    fields = AC.Field[]
    batches = AC.RecordBatch[]
    for part in Tables.partitions(tbl)
        cols = Tables.columns(part)
        names = Tables.columnnames(cols)
        pairs = [_writecolumn(String(nm), Tables.getcolumn(cols, nm))
                 for nm in names]
        if sch === nothing
            fields = AC.Field[_withcolmeta(p[1], colmetadata) for p in pairs]
            sch = AC.Schema(fields; metadata=_metapairs(metadata))
        end
        n = isempty(pairs) ? 0 : pairs[1][2].len
        push!(batches, AC.RecordBatch(sch, AC.ArrayData[p[2] for p in pairs], n))
    end
    sch === nothing &&
        throw(ArgumentError("table has no partitions; cannot infer a schema"))
    codec = compress === nothing ? :none : compress
    return file ? writefile(sch, batches; compress=codec) :
        writestream(sch, batches; compress=codec)
end

_metapairs(::Nothing) = nothing
_metapairs(m) = [String(k) => String(v) for (k, v) in Base.pairs(Dict(m))]

_withcolmeta(f::AC.Field, ::Nothing) = f
function _withcolmeta(f::AC.Field, colmetadata)
    cm = get(Dict(colmetadata), Symbol(f.name), nothing)
    cm === nothing && return f
    return AC.Field(f.name, f.type; nullable=f.nullable,
        metadata=_metapairs(cm), children=collect(AC.Field, f.children))
end
