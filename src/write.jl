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
# This file owns partition binding, retained-field alignment, schema and batch
# assembly, compression selection, and IPC emission. Column construction is a
# deep module behind `_constructcolumn`.
# =============================================================================

"""
    Arrow.DictEncode(v)

Mark a column for dictionary encoding: the writer builds a category pool and
encodes slots as integer indices into it. Fresh pools coalesce exact Arrow
storage values. A retained rewrite may preserve unused or duplicate physical
categories because pool order and index meaning are part of the encoded data.
"""
struct DictEncode{T,V<:AbstractVector{T}} <: AbstractVector{T}
    data::V
end
Base.size(d::DictEncode) = size(d.data)
Base.getindex(d::DictEncode, i::Int) = d.data[i]

include("columnconstruction.jl")

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

"Retained Arrow schema when the source is a facade read, else nothing."
_retainedschema(t::Table) = getfield(t, :schema)
_retainedschema(s::Stream) = _tableschema(getfield(s, :src))
_retainedschema(::Any) = nothing

"Dictionary pools retained by one facade partition, in column order."
_partitiondictpools(t::Table) = getfield(t, :retainedpools)
_partitiondictpools(::Any) = nothing

function _writebytes(
    tbl;
    file::Bool=true,
    compress::Union{Nothing,Symbol}=nothing,
    metadata=nothing,
    colmetadata=nothing,
)
    retained = _retainedschema(tbl)
    # Phase 1: materialize every partition's columns (this writer is eager),
    # validating name/order agreement — a drift here would silently bind
    # data to the wrong fields.
    names = Symbol[]
    partcols = Vector{AbstractVector}[]
    partpools = Any[]
    rowcounts = Int[]
    for part in Tables.partitions(tbl)
        cols = Tables.columns(part)
        pnames = collect(Symbol, Tables.columnnames(cols))
        if isempty(partcols)
            names = pnames
        else
            pnames == names || throw(
                ArgumentError(
                    "partition $(length(partcols) + 1) column names $(pnames) " *
                    "do not match the first partition's $(names) (same names, " *
                    "same order); reorder or rename the partition's columns",
                ),
            )
        end
        # Arrow permits duplicate field names. Tables.getcolumn(cols, name)
        # cannot distinguish them, so bind every partition by position.
        push!(
            partcols,
            AbstractVector[Tables.getcolumn(cols, j) for j in eachindex(pnames)],
        )
        pools = _partitiondictpools(part)
        pools !== nothing &&
            length(pools) != length(pnames) &&
            throw(
                ArgumentError(
                    "retained dictionary pool count does not match partition width",
                ),
            )
        push!(partpools, pools)
        n = Int(Tables.rowcount(cols))
        if n == 0 && isempty(pnames)
            n = max(n, Int(Tables.rowcount(part)))
        end
        push!(rowcounts, n)
    end
    isempty(partcols) &&
        throw(ArgumentError("table has no partitions; cannot infer a schema"))
    nparts = length(partcols)
    ncols = length(names)
    retainedaligned =
        retained !== nothing &&
        length(retained.fields) == ncols &&
        all(j -> retained.fields[j].name == String(names[j]), 1:ncols)
    function retainedfield(j)
        retained === nothing && return nothing
        retainedaligned && return retained.fields[j]
        count(==(names[j]), names) == 1 || return nothing
        matches = findall(f -> f.name == String(names[j]), collect(retained.fields))
        return length(matches) == 1 ? retained.fields[only(matches)] : nothing
    end
    # Phase 2: construct each complete logical column across all partitions.
    # The column module owns inference, retained reconstruction, ArrowTypes,
    # dictionary pooling, partition agreement, and field metadata.
    fields = Vector{AC.Field}(undef, ncols)
    coldata = Vector{Vector{AC.ArrayData}}(undef, ncols)
    colmetamap = colmetadata === nothing ? nothing : Dict(colmetadata)
    for j = 1:ncols
        parts = AbstractVector[partcols[k][j] for k = 1:nparts]
        poolhints = Any[pools === nothing ? nothing : pools[j] for pools in partpools]
        columnmeta = colmetamap === nothing ? nothing : get(colmetamap, names[j], nothing)
        fields[j], coldata[j] = _constructcolumn(
            names[j],
            parts;
            retained=retainedfield(j),
            poolhints=poolhints,
            metadata=_metapairs(columnmeta),
        )
    end
    outfields = fields
    schmeta =
        metadata !== nothing ? _metapairs(metadata) :
        (
            retained === nothing || retained.metadata === nothing ? nothing :
            collect(Pair{String,String}, retained.metadata)
        )
    sch = AC.Schema(outfields; metadata=schmeta)
    batches = AC.RecordBatch[
        AC.RecordBatch(sch, AC.ArrayData[coldata[j][k] for j = 1:ncols], rowcounts[k])
        for k = 1:nparts
    ]
    codec = compress === nothing ? :none : compress
    return file ? writefile(sch, batches; compress=codec) :
           writestream(sch, batches; compress=codec)
end

_metapairs(::Nothing) = nothing
function _metapairs(m)
    entries = m isa AbstractDict || m isa NamedTuple ? Base.pairs(m) : m
    out = Pair{String,String}[]
    for kv in entries
        kv isa Pair || throw(ArgumentError("metadata sequences must contain Pair values"))
        push!(out, String(first(kv)) => String(last(kv)))
    end
    return out
end
