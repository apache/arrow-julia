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
    elseif T === Any || (T <: AbstractVector && eltype(T) === Any)
        # Materialized facade columns are Any-eltype for composite layouts;
        # one narrowing pass recovers list columns (inner vectors narrow
        # element-wise, empties adopt the joined element type).
        w = _narrowlists(v)
        NW = Base.nonmissingtype(eltype(w))
        (NW === Any || (NW <: AbstractVector && eltype(NW) === Any)) &&
            throw(ArgumentError(
            "column $name has element type Any and cannot be narrowed to " *
            "a writable Arrow column; give it a concrete element type"))
        return _writecolumn(name, w)
    else
        return AC.fromjulia(name, _plainvector(v))
    end
end

"Narrow an Any-eltype column, recovering list-of-T structure when present."
function _narrowlists(v::AbstractVector)
    w = map(x -> x isa AbstractVector ? map(identity, x) : x, v)
    w = map(identity, w)
    NT = Base.nonmissingtype(eltype(w))
    NT <: AbstractVector || return w
    # Join the inner element types (empties narrow to Union{} and would
    # otherwise poison the join), then retype every inner vector.
    E = Union{}
    for x in w
        x === missing && continue
        isempty(x) && continue
        E = typejoin(E, eltype(x))
    end
    E === Union{} && (E = Any)
    E === Any && return w
    hasm = eltype(w) >: Missing
    S = hasm ? Union{Missing,Vector{E}} : Vector{E}
    return S[x === missing ? missing : convert(Vector{E}, x) for x in w]
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

# --- retained-schema rewrite (facade Table/Stream round-trips) --------------

"Storage integers for a public column under a RETAINED temporal descriptor."
function _retainedstorage(t::AC.ArrowType, v::AbstractVector, name::String)
    out = Union{Missing,Int64}[]
    sizehint!(out, length(v))
    for x in v
        if x === missing
            push!(out, missing)
        else
            ok, sv = _storagevalue(t, x)
            ok && sv isa Integer || throw(ArgumentError(
                "column $name holds $(typeof(x)) values that do not match " *
                "its retained Arrow type $(repr(t)); the column was " *
                "replaced with incompatible data"))
            push!(out, Int64(sv))
        end
    end
    return out
end

"Build one column under a retained Field: descriptor, nullability, metadata."
function _writecolumn(f::AC.Field, v::AbstractVector)
    t = f.type
    # Identity FIRST, for every retained field with a known facade type:
    # a replaced column is rejected on its declared element type before any
    # value is read.
    Fp = _facadebasetype(t)
    if Fp !== Any
        NT = Base.nonmissingtype(eltype(v))
        NT <: Fp || NT === Union{} || throw(ArgumentError(
            "column $(f.name) holds $(NT) values, but its retained Arrow " *
            "type $(repr(t)) materializes as $(Fp); the column was " *
            "replaced with incompatible data"))
        eltype(v) >: Missing && !f.nullable && throw(ArgumentError(
            "column $(f.name) may hold missing values but its retained " *
            "field is non-nullable"))
    end
    if t isa AC.DateType || t isa AC.TimestampType || t isa AC.TimeType ||
       t isa AC.DurationType
        if Fp === Int64
            storage = Union{Missing,Int64}[x === missing ? missing : Int64(x)
                                           for x in v]
        else
            storage = _retainedstorage(t, v, f.name)
        end
        return _rebuildtemporal(f, storage, length(v))
    end
    # List fields: the retained CHILD descriptor supplies the element type
    # observation cannot — zero-row, all-empty, and nested list columns have
    # no values to observe. Unresolvable children (temporal storage,
    # composites without a closed facade mapping) fall back to natural
    # inference unchanged.
    if t isa AC.ListType && length(f.children) == 1
        E = _retainedlisteltype(f.children[1])
        E === nothing || (v = _retypelist(f, v, E))
    end
    # Non-temporal: build naturally, then impose the retained descriptor —
    # types must agree and nullability comes from the RETAINED field (values
    # holding missing under a non-nullable field are a replacement error).
    fn, dn = _writecolumn(f.name, v)
    AC.typeequal(fn.type, t) || throw(ArgumentError(
        "column $(f.name) no longer matches its retained Arrow type " *
        "$(repr(t)); it now maps to $(repr(fn.type))"))
    fn.nullable && !f.nullable && AC.nullcount(dn) > 0 && throw(ArgumentError(
        "column $(f.name) holds missing values but its retained field is " *
        "non-nullable"))
    rebuilt = AC.Field(f.name, fn.type; nullable=f.nullable,
        metadata=f.metadata === nothing ? nothing :
            collect(Pair{String,String}, f.metadata),
        children=collect(AC.Field, fn.children))
    return rebuilt, dn
end

"""
The DECLARED Julia value type of a retained list child, when the facade
materializes it faithfully: primitives and strings resolve, nested lists
recurse, and everything else returns `nothing` (temporal children stay raw
storage integers at the facade; other composites have no closed mapping) —
the caller then keeps natural inference, the pre-retype behavior.
"""
function _retainedlisteltype(c::AC.Field)
    t = c.type
    if t isa AC.ListType
        length(c.children) == 1 || return nothing
        inner = _retainedlisteltype(c.children[1])
        inner === nothing && return nothing
        return c.nullable ? Union{Missing,Vector{inner}} : Vector{inner}
    end
    (t isa AC.DateType || t isa AC.TimestampType || t isa AC.TimeType ||
        t isa AC.DurationType || t isa AC.DictionaryType) && return nothing
    E0 = _facadebasetype(t)
    E0 === Any && return nothing
    return c.nullable ? Union{Missing,E0} : E0
end

"""
Retype list rows to the retained child element type — IDENTITY-strict at
every depth, like the scalar retained gate: values must already BE the
declared element type (a replaced column must refuse, never coerce — a
`convert` would silently turn a replacement `true` into `Int64(1)`).
Structure recovers (Any-eltyped rows retype), values never change.
"""
function _retypelist(f::AC.Field, v::AbstractVector, ::Type{E}) where {E}
    S = eltype(v) >: Missing ? Union{Missing,Vector{E}} : Vector{E}
    out = Vector{S}(undef, length(v))
    for (i, x) in enumerate(v)
        out[i] = x === missing ? missing : _retypevalue(Vector{E}, x, f)
    end
    return out
end

function _retypevalue(::Type{T}, x, f::AC.Field) where {T}
    if x === missing
        Missing <: T || throw(ArgumentError(
            "column $(f.name) holds missing elements but its retained " *
            "list child is non-nullable"))
        return missing
    end
    NT = Base.nonmissingtype(T)
    if NT <: AbstractVector
        x isa AbstractVector || throw(ArgumentError(
            "column $(f.name) holds $(typeof(x)) values, but its retained " *
            "Arrow type $(repr(f.type)) materializes as vectors; the " *
            "column was replaced with incompatible data"))
        E = eltype(NT)
        w = Vector{E}(undef, length(x))
        i = 0
        for elt in x
            i += 1
            w[i] = _retypevalue(E, elt, f)
        end
        return w
    end
    x isa NT || throw(ArgumentError(
        "column $(f.name) holds $(typeof(x)) elements that do not match " *
        "its retained list element type $(NT); the column was replaced " *
        "with incompatible data"))
    return x
end

function _rebuildtemporal(f::AC.Field, storage, n)
    t = f.type
    nmissing = count(x -> x === missing, storage)
    nmissing > 0 && !f.nullable && throw(ArgumentError(
        "column $(f.name) holds missing values but its retained field is " *
        "non-nullable"))
    f0, d0 = AC.fromjulia("x", storage)
    width = AC.primwidth(t)
    buffers = d0.buffers
    if width == 4
        narrow = Vector{Int32}(undef, n)
        for (i, x) in enumerate(storage)
            narrow[i] = x === missing ? Int32(0) : Int32(x)
        end
        buffers = [d0.buffers[1], AC._databuffer(narrow)]
    end
    d = AC._arraydata(t, d0.len, buffers, 0, AC.ArrayData[], nothing,
        d0.owner, AC.nullcount(d0))
    fld = AC.Field(f.name, t; nullable=f.nullable,
        metadata=f.metadata === nothing ? nothing :
            collect(Pair{String,String}, f.metadata))
    return fld, d
end

function _writecolumn(f::AC.Field, v::AbstractVector,
    pool::Vector, lookup::Dict)
    t = f.type::AC.DictionaryType
    indices = Union{Missing,Int32}[x === missing ? missing : lookup[x]
                                   for x in v]
    fn, dn = AC.fromjulia_dict(f.name, pool, indices)
    AC.typeequal(fn.type, t) || throw(ArgumentError(
        "column $(f.name) no longer matches its retained dictionary type"))
    fld = AC.Field(f.name, t; nullable=f.nullable,
        metadata=f.metadata === nothing ? nothing :
            collect(Pair{String,String}, f.metadata),
        children=collect(AC.Field, fn.children))
    return fld, dn
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

"Retained Arrow schema when the source is a facade read, else nothing."
_retainedschema(t::Table) = getfield(t, :schema)
_retainedschema(s::Stream) = _tableschema(getfield(s, :src))
_retainedschema(::Any) = nothing

"One shared-pool dictionary batch: identical pool OBJECT across batches."
function _dictbatch(fld::AC.Field, indices::Vector, pool_d::AC.ArrayData)
    t = fld.type::AC.DictionaryType
    IT = AC.juliatype(t.indextype)
    present = [x !== missing for x in indices]
    inds = IT[x === missing ? zero(IT) : IT(x) for x in indices]
    nc = count(!, present)
    d = AC.ArrayData(t, length(indices),
        [AC._bitmapbuffer(present), AC._databuffer(inds)];
        dictionary=pool_d, nullcount=nc)
    return d
end

"Field + first-batch data for a dictionary column under a RETAINED type."
function _retaineddict(rf::AC.Field, pool::Vector, firstidx::Vector,
    name::String)
    t = rf.type::AC.DictionaryType
    vf, vd = AC.fromjulia(name, pool)
    AC.typeequal(vf.type, t.valuetype) || throw(ArgumentError(
        "column $name pool maps to $(summary(vf.type)) but the retained " *
        "dictionary value type is $(summary(t.valuetype))"))
    IT = AC.juliatype(t.indextype)
    length(pool) - 1 <= typemax(IT) || throw(ArgumentError(
        "column $name pool of $(length(pool)) values exceeds the retained " *
        "$(summary(t.indextype)) index range"))
    fld = AC.Field(name, t; nullable=rf.nullable,
        metadata=rf.metadata === nothing ? nothing :
            collect(Pair{String,String}, rf.metadata),
        children=collect(AC.Field, vf.children))
    return fld, _dictbatch(fld, firstidx, vd)
end

function _writebytes(tbl; file::Bool=true, compress::Union{Nothing,Symbol}=nothing,
    metadata=nothing, colmetadata=nothing)
    retained = _retainedschema(tbl)
    # Phase 1: materialize every partition's columns (this writer is eager),
    # validating name/order agreement — a drift here would silently bind
    # data to the wrong fields.
    names = Symbol[]
    partcols = Vector{AbstractVector}[]
    rowcounts = Base.Int[]
    for part in Tables.partitions(tbl)
        cols = Tables.columns(part)
        pnames = collect(Symbol, Tables.columnnames(cols))
        if isempty(partcols)
            names = pnames
        else
            pnames == names || throw(ArgumentError(
                "partition $(length(partcols) + 1) column names $(pnames) " *
                "do not match the first partition's $(names) (same names, " *
                "same order); reorder or rename the partition's columns"))
        end
        push!(partcols, AbstractVector[Tables.getcolumn(cols, nm)
                                       for nm in pnames])
        n = Base.Int(Tables.rowcount(cols))
        if n == 0 && isempty(pnames)
            n = max(n, Base.Int(Tables.rowcount(part)))
        end
        push!(rowcounts, n)
    end
    isempty(partcols) &&
        throw(ArgumentError("table has no partitions; cannot infer a schema"))
    nparts = length(partcols)
    ncols = length(names)
    retainedfield(j) = begin
        retained === nothing && return nothing
        i = findfirst(f -> f.name == String(names[j]),
            collect(retained.fields))
        i === nothing ? nothing : retained.fields[i]
    end
    # Phase 2: build columns. Dictionary-intent columns (retained
    # DictionaryType or DictEncode input) share ONE pool object across all
    # partitions — the file format carries one dictionary batch per id, and
    # per-partition pools would read as replacement.
    fields = Vector{AC.Field}(undef, ncols)
    coldata = [Vector{AC.ArrayData}(undef, nparts) for _ = 1:ncols]
    for j = 1:ncols
        rf = retainedfield(j)
        dictintent = (rf !== nothing && rf.type isa AC.DictionaryType) ||
            any(partcols[k][j] isa DictEncode for k = 1:nparts)
        if dictintent
            vals = [partcols[k][j] isa DictEncode ?
                    (partcols[k][j]::DictEncode).data : partcols[k][j]
                    for k = 1:nparts]
            if rf !== nothing
                Fv = _facadebasetype(rf.type)
                for k = 1:nparts
                    NT = Base.nonmissingtype(eltype(vals[k]))
                    Fv !== Any && !(NT <: Fv) && NT !== Union{} &&
                        throw(ArgumentError(
                        "column $(names[j]) holds $(NT) values, but its " *
                        "retained dictionary materializes as $(Fv); the " *
                        "column was replaced with incompatible data"))
                    eltype(vals[k]) >: Missing && !rf.nullable &&
                        throw(ArgumentError(
                        "column $(names[j]) may hold missing values but " *
                        "its retained dictionary field is non-nullable"))
                end
            end
            pool = unique(x for k = 1:nparts for x in skipmissing(vals[k]))
            lookup = Dict{Any,Int32}(x => Int32(i - 1)
                                     for (i, x) in enumerate(pool))
            firstidx = Union{Missing,Int32}[x === missing ? missing :
                lookup[x] for x in vals[1]]
            if rf === nothing
                fld, d1 = AC.fromjulia_dict(String(names[j]), collect(pool),
                    firstidx)
            else
                fld, d1 = _retaineddict(rf, collect(pool), firstidx,
                    String(names[j]))
            end
            fields[j] = fld
            coldata[j][1] = d1
            pool_d = d1.dictionary::AC.ArrayData
            for k = 2:nparts
                idx = Union{Missing,Int32}[x === missing ? missing :
                    lookup[x] for x in vals[k]]
                coldata[j][k] = _dictbatch(fld, idx, pool_d)
            end
        else
            local firstfield::AC.Field
            for k = 1:nparts
                fk, dk = rf === nothing ?
                    _writecolumn(String(names[j]), partcols[k][j]) :
                    _writecolumn(rf, partcols[k][j])
                if k == 1
                    firstfield = fk
                else
                    AC.typeequal(fk.type, firstfield.type) ||
                        throw(ArgumentError(
                        "partition $k column $(names[j]) maps to Arrow " *
                        "type $(repr(fk.type)), but the first partition " *
                        "declared $(repr(firstfield.type)); make the " *
                        "column types agree across partitions"))
                    fk.nullable && !firstfield.nullable &&
                        throw(ArgumentError(
                        "partition $k column $(names[j]) is nullable but " *
                        "the first partition declared it non-nullable; " *
                        "make the first partition's column eltype " *
                        "Union{Missing,T} to widen the schema"))
                end
                coldata[j][k] = dk
            end
            fields[j] = firstfield
        end
    end
    outfields = AC.Field[_withcolmeta(fields[j], colmetadata) for j = 1:ncols]
    schmeta = metadata !== nothing ? _metapairs(metadata) :
        (retained === nothing || retained.metadata === nothing ? nothing :
         collect(Pair{String,String}, retained.metadata))
    sch = AC.Schema(outfields; metadata=schmeta)
    batches = AC.RecordBatch[
        AC.RecordBatch(sch,
            AC.ArrayData[coldata[j][k] for j = 1:ncols], rowcounts[k])
        for k = 1:nparts]
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
