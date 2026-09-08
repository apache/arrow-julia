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
# The read facade: Arrow IPC -> Tables.jl columns.
#
# `Arrow.Table` materializes the selected columns into plain Julia vectors
# (closed element-type claims through Core's bulk typed path, everything else
# through the dynamic accessors); there is no lazy typed-view layer.
# `Arrow.Stream` iterates record batches as one Table each. Scan pushdown
# routes through the ranged-scan adapter: on the file format, column
# pruning, statistics-based batch pruning, and window consumption all happen
# before decode.
# =============================================================================

"""
    Arrow.Table(source; scan=nothing, mmap=true, limits) -> Table

Read Arrow IPC data as Tables.jl columns. `source` is a file path, an `IO`,
raw bytes (`Vector{UInt8}`), or an [`Arrow.AbstractArrowSource`](@ref) — a
byte-range-addressable object, such as one in cloud storage, read with
exact range requests. Both IPC formats are accepted: the file format
(`ARROW1` magic, random access, footer statistics) and the stream format.
`mmap=true` memory-maps a file-format path instead of reading it into
memory; it has no effect on the other source kinds.

`limits` sets the [`Arrow.Limits`](@ref) policy for a raw source. Omit it to
use `Arrow.Limits()`. An internal reader handle that was opened under a
specific policy keeps that policy; a facade call cannot retroactively
re-verify it under different limits.

`scan` is a `Tables.Scan` pushdown request: only the selected and
filter-referenced columns are decoded, footer statistics prune batches no
row of which can match the filter, and the filter is evaluated batch by
batch. `limit`/`offset` compose exactly over the qualifying rows. Without a
filter, whole batches outside the window are never decoded. With a filter,
decoding stops as soon as the window fills. Over an `AbstractArrowSource`
the footer normally comes from one cached tail read; one exact cached
follow-up is used when it escapes that window. Statistics prune batches
before record metadata is requested. Surviving metadata is fetched and
validated before selected buffer ranges are requested in the next round. A
filtered limit then stops decoding, not fetching. On the stream format the
scan is applied after decode.

Some filters have no semantics-preserving storage-domain form: inexact
literals, temporal conversions that alias values or wrap ordering, unsafe
Duration unit promotion, and custom membership objects. Tuple and Array
membership lower only for equality-preserving conversions; Set members must
also have the column's canonical public type. Such filters fall back to
reading the whole source and evaluating over public values.

Independent of the filter, some sources are read whole: an
`AbstractArrowSource` without a scan, a zero-field file, and any
stream-format source. Empty projections use range planning and preserve
their row count without fetching output column bodies. When a filter must
prune remote fetches, write it in each column's public value domain.

Columns are materialized (plain `Vector`s): the returned table does not
borrow the source bytes, and [`Arrow.release!`](@ref) may be called at any
time afterward to release a memory-mapped file deterministically — do this
on Windows before deleting a mapped file.
"""
struct Table <: Tables.AbstractColumns
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol,Int}
    schema::Union{Nothing,AC.Schema}
    regions::Vector{AC.OwnerRegion}
    nrows::Int   # authoritative even with zero columns
    # Original dictionary pools, in column order, when the facade read had
    # enough information to retain them. This is intentionally separate from
    # `schema`: ordered dictionary semantics live in the pool order, not in
    # the DictionaryType descriptor alone.
    retainedpools::Vector{Any}
end

# Construct a Table with deliberately replaced materialized columns (the
# facade tests exercise refusal paths this way). The passed lookup is
# ignored and rebuilt from `names`, and no dictionary pools are retained.
Table(names, columns, lookup, schema, regions, nrows) = Table(
    names,
    columns,
    _namelookup(names),
    schema,
    regions,
    nrows,
    Any[nothing for _ in names],
)

function _namelookup(names, budget=nothing)
    _chargedict!(budget, Symbol, Int, length(names), "table column-name lookup")
    lookup = Dict{Symbol,Int}()
    sizehint!(lookup, length(names))
    for (i, nm) in enumerate(names)
        # Sentinel 0 marks a name that appears more than once; `_columnindex`
        # turns it into an "ambiguous, use positional access" error. An
        # absent key (-1 there) means no such column.
        lookup[nm] = haskey(lookup, nm) ? 0 : i
    end
    return lookup
end

function _columnindex(t::Table, nm::Symbol)
    i = get(getfield(t, :lookup), nm, -1)
    i == -1 && throw(ArgumentError("no column $(repr(nm)) in this table"))
    i == 0 && throw(
        ArgumentError("column name $(repr(nm)) is ambiguous; use positional column access"),
    )
    return i
end

function _validatefieldsymbolname(name::AbstractString)
    occursin('\0', name) && throw(
        AC.ValidationError(
            "field name contains an embedded NUL and cannot be represented " *
            "as a Tables.jl column Symbol",
        ),
    )
    ncodeunits(name) <= _MAX_ARROWTYPE_SCHEMA_NAME_BYTES || throw(
        AC.ValidationError(
            "field name exceeds the Tables.jl facade limit of " *
            "$_MAX_ARROWTYPE_SCHEMA_NAME_BYTES UTF-8 bytes",
        ),
    )
    return nothing
end

_fieldsymbol(name::AbstractString) = (_validatefieldsymbolname(name); Symbol(name))

const _MAX_TABLES_NEW_FIELD_NAMES = 65_536
const _MAX_TABLES_NEW_FIELD_NAME_BYTES = 1024 * 1024

function _fieldnamesymbols(fields, budget=nothing)
    _chargedict!(budget, String, Nothing, length(fields), "novel field-name set")
    novel = Set{String}()
    sizehint!(novel, length(fields))
    novelbytes = 0
    for field in fields
        name = field.name
        _validatefieldsymbolname(name)
        _existingjlsymbol(name) === nothing || continue
        name in novel && continue
        length(novel) < _MAX_TABLES_NEW_FIELD_NAMES || throw(
            AC.ValidationError(
                "Tables.jl facade field names exceed the per-read novel-name limit",
            ),
        )
        namebytes = ncodeunits(name)
        novelbytes <= _MAX_TABLES_NEW_FIELD_NAME_BYTES - namebytes || throw(
            AC.ValidationError(
                "Tables.jl facade field names exceed the per-read novel-name byte budget",
            ),
        )
        push!(novel, name)
        novelbytes += namebytes
    end
    budget === nothing || _charge!(budget, Int64(novelbytes), "novel field-name bytes")
    _chargevector!(budget, Symbol, length(fields), "table field names")
    return Symbol[_fieldsymbol(field.name) for field in fields]
end

function _table(
    names::Vector{Symbol},
    columns::Vector{AbstractVector},
    schema,
    regions,
    nrows::Integer,
    retainedpools=nothing,
    budget=nothing,
)
    retainedpools === nothing ||
        length(retainedpools) == length(names) ||
        throw(ArgumentError("retained dictionary pool count does not match column count"))
    _chargevector!(budget, Any, length(names), "table retained-pool slots")
    pools = retainedpools === nothing ? fill(nothing, length(names)) : Any[retainedpools...]
    lookup = _namelookup(names, budget)
    return Table(names, columns, lookup, schema, regions, Int(nrows), pools)
end

Tables.istable(::Type{Table}) = true
Tables.columnaccess(::Type{Table}) = true
Tables.columns(t::Table) = t
Tables.columnnames(t::Table) = getfield(t, :names)
Tables.getcolumn(t::Table, i::Int) = getfield(t, :columns)[i]
Tables.getcolumn(t::Table, nm::Symbol) = getfield(t, :columns)[_columnindex(t, nm)]
# Column count at or below which `Tables.schema` returns the fully typed
# `Tables.Schema{names, types}`. Typed schemas let materializers that refuse
# stored schemas (`Tables.rowtable`, `Tables.columntable`) work as usual and
# comfortably cover Tables.jl's own 100-column specialization threshold.
# Above it the names and types stay in `Vector` fields (`stored=true`) so
# compiler work cannot scale with untrusted input names; Tables.jl then
# refuses `NamedTuple` materialization with its "input table too wide" error,
# which is the intended behavior for very wide tables.
const _MAX_TYPED_SCHEMA_FIELDS = 256

function Tables.schema(t::Table)
    names = getfield(t, :names)
    types = Type[eltype(c) for c in getfield(t, :columns)]
    return Tables.Schema(names, types; stored=length(names) > _MAX_TYPED_SCHEMA_FIELDS)
end
Base.propertynames(t::Table) = getfield(t, :names)
Base.getproperty(t::Table, nm::Symbol) = Tables.getcolumn(t, nm)
Tables.rowcount(t::Table) = getfield(t, :nrows)
Base.length(t::Table) = getfield(t, :nrows)

DataAPI.metadatasupport(::Type{Table}) = (read=true, write=false)
DataAPI.colmetadatasupport(::Type{Table}) = (read=true, write=false)

const _NO_DEFAULT = gensym(:nodefault)

function DataAPI.metadatakeys(t::Table)
    sch = getfield(t, :schema)
    (sch === nothing || sch.metadata === nothing) && return ()
    return (String(first(kv)) for kv in sch.metadata)
end
function DataAPI.metadata(
    t::Table,
    key::AbstractString,
    default=_NO_DEFAULT;
    style::Bool=false,
)
    sch = getfield(t, :schema)
    if sch !== nothing && sch.metadata !== nothing
        for kv in sch.metadata
            first(kv) == key && return style ? (last(kv), :default) : last(kv)
        end
    end
    default === _NO_DEFAULT && throw(KeyError(key))
    return style ? (default, :default) : default
end
function _schemafield(t::Table, col::Int)
    names = getfield(t, :names)
    checkbounds(names, col)
    sch = getfield(t, :schema)
    sch === nothing && return nothing
    fields = sch.fields
    # An ordinary facade read retains one field per output column in the same
    # order. Position is the only unambiguous identity when Arrow legally
    # carries duplicate field names.
    if length(fields) == length(names) &&
       all(i -> Symbol(fields[i].name) == names[i], eachindex(names))
        return fields[col]
    end
    # A real Tables.Scan type conversion drops that column's retained field,
    # so its schema can be an ordered subset. Name lookup remains safe only
    # when the output name itself is unique.
    nm = names[col]
    count(==(nm), names) == 1 || return nothing
    text = String(nm)
    i = findfirst(f -> f.name == text, fields)
    return i === nothing ? nothing : sch.fields[i]
end
function _schemafield(t::Table, col::Symbol)
    return _schemafield(t, _columnindex(t, col))
end
function DataAPI.colmetadatakeys(t::Table, col::Union{Symbol,Int})
    c = col isa Symbol ? _columnindex(t, col) : col
    f = _schemafield(t, c)
    (f === nothing || f.metadata === nothing) && return ()
    return (String(first(kv)) for kv in f.metadata)
end
function DataAPI.colmetadatakeys(t::Table)
    return (
        nm => keys for (i, nm) in enumerate(getfield(t, :names)) for
        keys in (DataAPI.colmetadatakeys(t, i),) if !isempty(keys)
    )
end
function DataAPI.colmetadata(
    t::Table,
    col::Union{Symbol,Int},
    key::AbstractString,
    default=_NO_DEFAULT;
    style::Bool=false,
)
    c = col isa Symbol ? _columnindex(t, col) : col
    f = _schemafield(t, c)
    if f !== nothing && f.metadata !== nothing
        for kv in f.metadata
            first(kv) == key && return style ? (last(kv), :default) : last(kv)
        end
    end
    default === _NO_DEFAULT && throw(KeyError(key))
    return style ? (default, :default) : default
end

"""
    Arrow.getmetadata(t::Arrow.Table)

Compatibility form of the Arrow 2.x metadata accessor: the table-level
key-value metadata as a `Dict{String,String}`, or `nothing` when the table
has none. New code should prefer the DataAPI.jl interface
(`DataAPI.metadata`, `DataAPI.metadatakeys`), which `Arrow.Table` supports.

Arrow 2.x also accepted a column; Arrow 3.0 columns are plain Julia vectors
and carry no metadata, so use
`DataAPI.colmetadata(table, column, key)`/`DataAPI.colmetadatakeys(table)`
for per-column metadata instead.
"""
function getmetadata(t::Table)
    sch = getfield(t, :schema)
    (sch === nothing || sch.metadata === nothing || isempty(sch.metadata)) && return nothing
    return Dict{String,String}(String(first(kv)) => String(last(kv)) for kv in sch.metadata)
end
function getmetadata(::AbstractVector)
    throw(
        ArgumentError(
            "Arrow 3.0 columns are plain Julia vectors and carry no " *
            "per-column metadata; use DataAPI.colmetadata(table, column, key) " *
            "or DataAPI.colmetadatakeys(table) instead",
        ),
    )
end

"""
    Arrow.release!(t::Union{Table,Stream})

Deterministically release the source regions behind a read (a memory map
unmaps NOW; imported foreign buffers run their release callbacks). `Table`
columns are materialized copies, so a closed `Table` remains fully usable;
a closed `Stream` refuses further iteration cleanly. Every yielded `Table`
shares its parent `Stream` source lifetime, so releasing either one closes the
stream while already materialized columns remain usable. Idempotent.
"""
function AC.release!(t::Table)
    foreach(AC.release!, getfield(t, :regions))
    return nothing
end

# --- Dates conversion (the facade owns what Core deliberately does not) ----

# Days from Julia's Date epoch (0000-12-31) to the Arrow epoch (1970-01-01).
const _EPOCH_DAYS = Dates.value(Dates.Date(1970, 1, 1))
const _LoweredTemporal = Union{Int32,Int64}
const _MILLIS_PER_DAY = Int128(86_400_000)

# Map a temporal descriptor to the compile-time token the lowering loops
# dispatch on; fourteen values total (2 date + 4 timestamp + 4 time +
# 4 duration).
_facadetoken(t::AC.DateType) =
    t.unit == AC.DAY ? Val((:date, AC.DAY)) : Val((:date, AC.MILLISECOND_DATE))
_facadetoken(t::AC.TimestampType) =
    t.unit == AC.SECOND ? Val((:timestamp, AC.SECOND)) :
    t.unit == AC.MILLISECOND ? Val((:timestamp, AC.MILLISECOND)) :
    t.unit == AC.MICROSECOND ? Val((:timestamp, AC.MICROSECOND)) :
    Val((:timestamp, AC.NANOSECOND))
_facadetoken(t::AC.TimeType) =
    t.unit == AC.SECOND ? Val((:time, AC.SECOND)) :
    t.unit == AC.MILLISECOND ? Val((:time, AC.MILLISECOND)) :
    t.unit == AC.MICROSECOND ? Val((:time, AC.MICROSECOND)) : Val((:time, AC.NANOSECOND))
_facadetoken(t::AC.DurationType) =
    t.unit == AC.SECOND ? Val((:duration, AC.SECOND)) :
    t.unit == AC.MILLISECOND ? Val((:duration, AC.MILLISECOND)) :
    t.unit == AC.MICROSECOND ? Val((:duration, AC.MICROSECOND)) :
    Val((:duration, AC.NANOSECOND))

@inline function _closedint64(value)::Union{Nothing,Int64}
    value isa Union{Bool,Int8,Int16,Int32,Int64} && return Int64(value)
    if value isa Union{Int128,UInt64,UInt128}
        Int128(typemin(Int64)) <= value <= Int128(typemax(Int64)) || return nothing
        return Int64(value)
    end
    value isa Union{UInt8,UInt16,UInt32} && return Int64(value)
    # BigInt and user-defined Integer values can allocate or execute arbitrary
    # code. Scalar lowering handles them separately; membership stays public.
    return nothing
end

@inline function _boundedint64(value::Int128)::Union{Nothing,Int64}
    Int128(typemin(Int64)) <= value <= Int128(typemax(Int64)) || return nothing
    return Int64(value)
end

@inline function _boundedint32(value::Int128)::Union{Nothing,Int32}
    Int128(typemin(Int32)) <= value <= Int128(typemax(Int32)) || return nothing
    return Int32(value)
end

@inline function _datestorage(value)::Union{Nothing,Int32}
    days = if value isa Dates.Date
        Int128(Dates.value(value))
    elseif value isa Dates.DateTime
        q, r = divrem(Int128(Dates.value(value)), _MILLIS_PER_DAY)
        iszero(r) || return nothing
        q
    else
        return nothing
    end
    return _boundedint32(days - Int128(_EPOCH_DAYS))
end

@inline function _epochmillis(value)::Union{Nothing,Int64}
    absolute = if value isa Dates.Date
        # Match DateTime(Date): dates outside DateTime's Int64 millisecond
        # domain do not have a facade representation for these layouts.
        raw = Int128(Dates.value(value)) * _MILLIS_PER_DAY
        _boundedint64(raw) === nothing && return nothing
        raw
    elseif value isa Dates.DateTime
        Int128(Dates.value(value))
    else
        return nothing
    end
    return _boundedint64(absolute - Int128(Dates.UNIXEPOCH))
end

@inline function _timestorage(unit, value)::Union{Nothing,Int64}
    value isa Dates.Time || return nothing
    nanos = Int64(Dates.value(value))
    unit == AC.NANOSECOND && return nanos
    divisor =
        unit == AC.MICROSECOND ? Int64(1_000) :
        unit == AC.MILLISECOND ? Int64(1_000_000) : Int64(1_000_000_000)
    q, r = divrem(nanos, divisor)
    return iszero(r) ? q : nothing
end

@inline function _timestampstorage(unit, value)::Union{Nothing,Int64}
    if unit == AC.SECOND || unit == AC.MILLISECOND
        millis = _epochmillis(value)
        if millis === nothing
            # With TimeZones loaded, tz-declared columns read as
            # ZonedDateTime, so retained rewrites and filter literals must
            # lower those values exactly. The extension lowers only its own
            # type and never throws; everything else stays `nothing`.
            ext = Base.get_extension(@__MODULE__, :ArrowTimeZonesExt)
            ext === nothing && return nothing
            z = ext.zonedstorage(unit, value)
            return z === nothing ? nothing : z::Int64
        end
        unit == AC.MILLISECOND && return millis
        q, r = divrem(millis, Int64(1_000))
        return iszero(r) ? q : nothing
    end
    return _closedint64(value)
end

@inline function _periodscale(value)::Union{Nothing,Int128}
    value isa Dates.Week && return Int128(604_800_000_000_000)
    value isa Dates.Day && return Int128(86_400_000_000_000)
    value isa Dates.Hour && return Int128(3_600_000_000_000)
    value isa Dates.Minute && return Int128(60_000_000_000)
    value isa Dates.Second && return Int128(1_000_000_000)
    value isa Dates.Millisecond && return Int128(1_000_000)
    value isa Dates.Microsecond && return Int128(1_000)
    value isa Dates.Nanosecond && return Int128(1)
    # Calendar periods need a reference date. Compound and user-defined
    # periods can execute request-defined conversion code. Both stay public.
    return nothing
end

@inline function _periodnanos(value)::Union{Nothing,Int128}
    # Keep `Dates.value` inside each `isa` branch. Returning only the scale
    # loses the type refinement for Vector{Any} and boxes every converted row.
    value isa Dates.Week && return Int128(Dates.value(value)) * Int128(604_800_000_000_000)
    value isa Dates.Day && return Int128(Dates.value(value)) * Int128(86_400_000_000_000)
    value isa Dates.Hour && return Int128(Dates.value(value)) * Int128(3_600_000_000_000)
    value isa Dates.Minute && return Int128(Dates.value(value)) * Int128(60_000_000_000)
    value isa Dates.Second && return Int128(Dates.value(value)) * Int128(1_000_000_000)
    value isa Dates.Millisecond && return Int128(Dates.value(value)) * Int128(1_000_000)
    value isa Dates.Microsecond && return Int128(Dates.value(value)) * Int128(1_000)
    value isa Dates.Nanosecond && return Int128(Dates.value(value))
    return nothing
end

@inline _durationunitscale(unit) =
    unit == AC.SECOND ? Int128(1_000_000_000) :
    unit == AC.MILLISECOND ? Int128(1_000_000) :
    unit == AC.MICROSECOND ? Int128(1_000) : Int128(1)

@inline function _durationstorage(unit, value)::Union{Nothing,Int64}
    nanos = _periodnanos(value)
    nanos === nothing && return nothing
    divisor = _durationunitscale(unit)
    q, r = divrem(nanos, divisor)
    iszero(r) || return nothing
    return _boundedint64(q)
end

"""
Lower a built-in temporal value without exceptions or request-defined code.

`K` is one of the fourteen `_facadetoken` descriptor/unit values. A result is
exact. `nothing` selects the public-domain path or, for one scalar, the
guarded custom-conversion path below.
"""
@inline function _exactfacadevalue(::Val{K}, value)::Union{Nothing,Int32,Int64} where {K}
    kind, unit = K
    if kind === :date
        return unit == AC.DAY ? _datestorage(value) : _epochmillis(value)
    elseif kind === :timestamp
        return _timestampstorage(unit, value)
    elseif kind === :time
        return _timestorage(unit, value)
    elseif kind === :duration
        return _durationstorage(unit, value)
    end
    return nothing
end

function _facadeconversionfailure(err)
    err isa Union{InterruptException,OutOfMemoryError} && throw(err)
    err isa Union{InexactError,OverflowError,MethodError} || throw(err)
    # Range, inexactness, and missing conversion methods all mean that no
    # exact facade-to-storage representation exists.
    return nothing
end

@inline _exactfacadescalar(t::AC.DateType, value) =
    t.unit == AC.DAY ? _datestorage(value) : _epochmillis(value)
@inline _exactfacadescalar(t::AC.TimestampType, value) = _timestampstorage(t.unit, value)
@inline _exactfacadescalar(t::AC.TimeType, value) = _timestorage(t.unit, value)
@inline _exactfacadescalar(t::AC.DurationType, value) = _durationstorage(t.unit, value)

_facadescalarvalue(t::Union{AC.DateType,AC.TimeType}, value) = _exactfacadescalar(t, value)

@noinline function _customint64(value)::Union{Nothing,Int64}
    try
        return Int64(value)
    catch err
        return _facadeconversionfailure(err)
    end
end

function _facadescalarvalue(t::AC.TimestampType, value)::Union{Nothing,Int64}
    result = _exactfacadescalar(t, value)
    result === nothing || return result
    # Sub-millisecond timestamps remain raw integers in the public facade.
    value isa Integer && t.unit in (AC.MICROSECOND, AC.NANOSECOND) || return nothing
    return _customint64(value)
end

@noinline function _customdurationvalue(
    t::AC.DurationType,
    value::Dates.Period,
)::Union{Nothing,Int64}
    period =
        t.unit == AC.SECOND ? Dates.Second :
        t.unit == AC.MILLISECOND ? Dates.Millisecond :
        t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    try
        return Int64(Dates.value(convert(period, value)))
    catch err
        return _facadeconversionfailure(err)
    end
end

function _facadescalarvalue(t::AC.DurationType, value)::Union{Nothing,Int64}
    result = _exactfacadescalar(t, value)
    result === nothing || return result
    value isa Dates.Period || return nothing
    return _customdurationvalue(t, value)
end

"""
Lower one public-domain scalar to a descriptor's storage domain exactly.

The Boolean result is false when conversion would change facade comparison
semantics. Scan planning then evaluates in the public domain; retained column
construction reports incompatible replacement data.
"""
function _facadeliteral(t::AC.ArrowType, value)
    result = _facadescalarvalue(t, value)
    return result === nothing ? (false, value) : (true, result)
end

function _facadetostorage(t::AC.ArrowType, v)
    t isa AC.DictionaryType && return _facadetostorage(t.valuetype, v)
    if t isa Union{AC.DateType,AC.TimestampType,AC.TimeType,AC.DurationType}
        return _facadeliteral(t, v)
    end
    # Non-temporal fields compare in their storage (and public) domain, but
    # temporal public literals are incompatible with them.
    if v isa Dates.Date || v isa Dates.DateTime || v isa Dates.Time || v isa Dates.Period
        return false, v
    end
    return true, v
end

function _mapcol(::Type{T}, f::F, col, budget=nothing) where {T,F}
    Out = Missing <: eltype(col) ? Union{Missing,T} : T
    _chargevector!(budget, Out, length(col), "facade converted column")
    out = Vector{Out}(undef, length(col))
    for i in eachindex(col)
        value = col[i]
        out[i] = value === missing ? missing : f(value)::T
    end
    return out
end

# The ArrowTimeZonesExt extension (loaded when TimeZones.jl is) restores
# Arrow 2.x reads for second/millisecond timestamps that declare a timezone:
# they materialize as `ZonedDateTime` instead of a naive UTC `DateTime`.
# `nothing` (extension absent, no timezone, or a finer unit) keeps this
# file's naive behavior; the extension itself returns `nothing` when it
# cannot parse the declared zone, so the eltype decision and the column
# conversion below always agree. Finer units keep raw Int64 storage either
# way: neither DateTime nor ZonedDateTime can hold them exactly.
function _zonedext(t::AC.TimestampType)
    t.timezone === nothing && return nothing
    (t.unit == AC.SECOND || t.unit == AC.MILLISECOND) || return nothing
    return Base.get_extension(@__MODULE__, :ArrowTimeZonesExt)
end

_postconvert(::AC.ArrowType, col, budget=nothing) = col
_postconvert(t::AC.DateType, col, budget=nothing) =
    t.unit == AC.DAY ?
    _mapcol(Dates.Date, x -> Dates.Date(Dates.UTD(Int64(x) + _EPOCH_DAYS)), col, budget) :
    _mapcol(
        Dates.DateTime,
        x -> Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH)),
        col,
        budget,
    )
function _postconvert(t::AC.TimestampType, col, budget=nothing)
    ext = _zonedext(t)
    if ext !== nothing
        zoned = ext.zonedcolumn(t, col, budget)
        zoned === nothing || return zoned
    end
    # DateTime is millisecond-precision. Finer units stay as their raw
    # storage integers rather than silently truncating.
    t.unit == AC.SECOND && return _mapcol(
        Dates.DateTime,
        x -> Dates.DateTime(Dates.UTM(Int64(x) * 1000 + Dates.UNIXEPOCH)),
        col,
        budget,
    )
    t.unit == AC.MILLISECOND && return _mapcol(
        Dates.DateTime,
        x -> Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH)),
        col,
        budget,
    )
    return col
end
function _postconvert(t::AC.TimeType, col, budget=nothing)
    scale =
        t.unit == AC.SECOND ? Int64(1_000_000_000) :
        t.unit == AC.MILLISECOND ? Int64(1_000_000) :
        t.unit == AC.MICROSECOND ? Int64(1_000) : Int64(1)
    return _mapcol(
        Dates.Time,
        x -> Dates.Time(Dates.Nanosecond(Int64(x) * scale)),
        col,
        budget,
    )
end
function _postconvert(t::AC.DurationType, col, budget=nothing)
    P =
        t.unit == AC.SECOND ? Dates.Second :
        t.unit == AC.MILLISECOND ? Dates.Millisecond :
        t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    return _mapcol(P, x -> P(Int64(x)), col, budget)
end
_postconvert(t::AC.DictionaryType, col, budget=nothing) =
    _postconvert(t.valuetype, col, budget)

# The public element type of the SCALAR layouts the facade converts (Dates)
# or passes through; `_declaredbasetype` completes it for every layout and
# `_declaredeltype` is the Field-aware rule the facade materializes with.
function _facadebasetype(t::AC.ArrowType)
    t isa AC.DateType && return t.unit == AC.DAY ? Dates.Date : Dates.DateTime
    if t isa AC.TimestampType
        ext = _zonedext(t)
        if ext !== nothing
            Z = ext.zonedtype(t.timezone)
            Z === nothing || return Z
        end
        return t.unit == AC.SECOND || t.unit == AC.MILLISECOND ? Dates.DateTime : Int64
    end
    t isa AC.TimeType && return Dates.Time
    if t isa AC.DurationType
        return t.unit == AC.SECOND ? Dates.Second :
               t.unit == AC.MILLISECOND ? Dates.Millisecond :
               t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    end
    t isa AC.DictionaryType && return _facadebasetype(t.valuetype)
    t isa AC.IntType && return AC.juliatype(t)
    t isa AC.FloatType && return AC.juliatype(t)
    t isa AC.BoolType && return Bool
    t isa AC.Utf8Type && return String
    (t isa AC.ViewType && t.utf8) && return String
    return Any
end

# The public element type of a materialized column: the Field's declared
# domain (`_declaredeltype(f, true)` — closed for scalars, the row container
# for composites, transparent through dictionary/REE, the children's join
# for unions), widened with `Missing` only when the DATA holds nulls a
# non-nullable declaration did not admit. Field nullability is advisory at
# the reader tier (the semantic validation IPC applies accepts such data,
# as the reference implementation does), so those columns READ, as
# missing-capable, rather than throw. `Any` stays the narrowing path.
function _publictype(f::AC.Field, col)
    T = _declaredeltype(f, true)
    (T === Any || Missing <: T) && return T
    (eltype(col) >: Missing && any(x -> x === missing, col)) || return T
    return Union{Missing,T}
end

"The concrete element join a package-owned narrowing copy will allocate."
function _narrowedeltype(values)
    isempty(values) && return Any
    T = typeof(first(values))
    for i in Iterators.drop(eachindex(values), 1)
        T = Base.promote_typejoin(T, typeof(values[i]))
        T === Any && break
    end
    return T
end

"Copy one vector after preflighting and charging its final element type."
function _narrowcopy(values, budget, what::AbstractString)
    T = _narrowedeltype(values)
    _chargevector!(budget, T, length(values), what)
    out = Vector{T}(undef, length(values))
    copyto!(out, values)
    return out
end

function _publiccolumn(f::AC.Field, converted, budget=nothing)
    T = _publictype(f, converted)
    T === Any && return _narrowcopy(converted, budget, "facade public column")
    _chargevector!(budget, T, length(converted), "facade public column")
    return collect(T, converted)
end

# A claim the typed path resolves without boxing: concrete scalars,
# their Missing unions, and Vectors thereof. `Vector{Any}` (lists of
# unresolved children) and the `Vector{Pair}` composite rows gain nothing
# typed, so they stay on the dynamic path.
function _closedclaim(::Type{T}) where {T}
    T === Any && return false
    # NullType columns claim Missing (nonmissingtype gives BOTTOM, which
    # subtypes Vector and has no eltype): the dynamic path serves them.
    T === Missing && return false
    NT = Base.nonmissingtype(T)
    NT === Union{} && return false
    NT <: Vector && return _closedclaim(eltype(NT))
    NT <: Pair && return false
    return isconcretetype(NT)
end

# The claim alone cannot see a union: a homogeneous union JOINS to a
# concrete Julia type, but Core refuses every typed union read — the
# route must inspect the descriptor through the transparent wrappers.
function _typedroutable(f::AC.Field)
    t = f.type
    t isa AC.UnionType && return false
    t isa AC.DictionaryType && return _typedroutable(AC.dictvaluefield(f, t))
    (t isa AC.RunEndEncodedType && length(f.children) == 2) &&
        return _typedroutable(f.children[2])
    return true
end

"The storage element claim shared by decoded batches and zero-part scan joins."
function _storageelementclaim(f::AC.Field)
    T = _declaredeltype(f, false)
    return _closedclaim(T) && _typedroutable(f) ? T : Any
end

"""
Materialize one batch column in the storage domain: through the typed element
path when its raw domain is closed, else through Core's dynamic path. This
never emits the private ArrowTypes Union routing markers.
"""
function _storagebatchcolumn(f::AC.Field, d::AC.ArrayData, budget=nothing)
    T = _storageelementclaim(f)
    T === Any &&
        return budget === nothing ? AC.materialize(f, d) : AC.materialize(f, d, budget)
    # Field nullability is advisory: the batch may hold nulls under a
    # non-nullable declaration (the semantic tier accepts that, as the
    # reference implementation does). Admit them in the claim rather than
    # refuse the read; conforming batches keep the Missing-free fast path.
    (Missing <: T || !_hasnulls(f, d)) || (T = Union{Missing,T})
    return budget === nothing ? AC.materialize(T, f, d) : AC.materialize(T, f, d, budget)
end

"Materialize for the facade, retaining an ArrowTypes Union route when needed."
function _batchcolumn(f::AC.Field, d::AC.ArrayData, plan::_ArrowTypesRoutePlan)
    if _hasarrowtypesextension(f, plan)
        routed = _arrowtypesroutedcolumn(f, d, plan)
        routed === nothing || return routed
    end
    return _storagebatchcolumn(f, d, plan.budget)
end

function _batchcolumn(f::AC.Field, d::AC.ArrayData, arrowtypes::_ArrowTypesContext)
    if _hasarrowtypesextension(f, arrowtypes)
        routed = _arrowtypesroutedcolumn(f, d, _arrowtypesrouteplan!(arrowtypes))
        routed === nothing || return routed
    end
    return _storagebatchcolumn(f, d, arrowtypes.budget)
end

function _batchcolumn(f::AC.Field, d::AC.ArrayData, budget=nothing)
    _hasarrowtypesextension(f) || return _storagebatchcolumn(f, d, budget)
    return _batchcolumn(f, d, _ArrowTypesRoutePlan(budget))
end

# Whether the typed claim for `f` would meet a null in `d`: physical nulls
# at this level, plus — through the wrappers the claim is transparent to —
# the REE values child's and a dictionary pool's.
function _hasnulls(f::AC.Field, d::AC.ArrayData)
    t = d.type
    if t isa AC.RunEndEncodedType && length(d.children) == 2 && length(f.children) == 2
        return _hasnulls(f.children[2], d.children[2])
    end
    if t isa AC.DictionaryType && d.dictionary !== nothing
        return AC.nullcount(d) > 0 || _hasnulls(AC.dictvaluefield(f, t), d.dictionary)
    end
    return AC.nullcount(d) > 0
end

function _facadecolumn(
    f::AC.Field,
    parts::Vector,
    arrowtypes::_ArrowTypesContext=_ArrowTypesContext(),
)
    budget = arrowtypes.budget
    if isempty(parts)
        T =
            _hasarrowtypesextension(f, arrowtypes) ?
            _arrowtypespubliceltype(arrowtypes, f) : _declaredeltype(f, true)
        _chargevector!(budget, T === Any ? Any : T, 0, "empty facade column")
        return T === Any ? Any[] : Vector{T}()
    end
    if length(parts) == 1
        col = parts[1]
    else
        total = sum(length, parts; init=0)
        T = eltype(first(parts))
        for part in Iterators.drop(parts, 1)
            T = Base.promote_typejoin(T, eltype(part))
        end
        _chargevector!(budget, T, total, "joined facade column")
        col = Vector{T}(undef, total)
        offset = 0
        for part in parts
            copyto!(col, offset + 1, part, firstindex(part), length(part))
            offset += length(part)
        end
    end
    return _facadefromraw(f, col, arrowtypes)
end

function _facadesinglecolumn(f::AC.Field, d::AC.ArrayData, arrowtypes::_ArrowTypesContext)
    _chargevector!(arrowtypes.budget, AbstractVector, 1, "batch column parts")
    return _facadecolumn(f, AbstractVector[_batchcolumn(f, d, arrowtypes)], arrowtypes)
end

function _facadebatchcolumn(f::AC.Field, batches, i::Int, arrowtypes::_ArrowTypesContext)
    _chargevector!(arrowtypes.budget, AbstractVector, length(batches), "batch column parts")
    parts =
        AbstractVector[_batchcolumn(f, batch.columns[i], arrowtypes) for batch in batches]
    return _facadecolumn(f, parts, arrowtypes)
end

_facadefromraw(
    f::AC.Field,
    col::AbstractVector,
    arrowtypes::_ArrowTypesContext=_ArrowTypesContext(),
) =
    _hasarrowtypesextension(f, arrowtypes) ? _arrowtypescolumn(f, col, arrowtypes) :
    _publiccolumn(f, _postconvert(f.type, col, arrowtypes.budget), arrowtypes.budget)

"Wire owner regions that a facade result retains for source lifetime control."
function _sourceregions(s::IPCStream, budget=nothing)
    # The stream decoder owns one wire region. Positively decompressed heap
    # buffers retain their own vectors through the returned columns and need
    # no eager release action. Retain this explicit root instead of rebuilding
    # an input-width identity index over every buffer slot.
    _chargevector!(budget, AC.OwnerRegion, 1, "source owner regions")
    return AC.OwnerRegion[s.region]
end
function _sourceregions(f::ArrowFile, budget=nothing)
    _chargevector!(budget, AC.OwnerRegion, 1, "source owner regions")
    return AC.OwnerRegion[f.region]
end

# The private scan-plan module is included at the facade conversion seam.
# It owns binding, lowering, execution, projection, and output schema.
include("scan.jl")

# --- source opening ---------------------------------------------------------

const _FILE_MAGIC = b"ARROW1"

const _OpenedReader = Union{SourceFile,ArrowFile,IPCStream}

_configuredlimits(source::SourceFile) = source.limits
_configuredlimits(source::ArrowFile) = source.limits
_configuredlimits(source::IPCStream) = source.limits
_defaultreaderlimits(source) =
    source isa _OpenedReader ? _configuredlimits(source) : Limits()

"Resolve one facade read policy without weakening an already-opened handle."
function _readerlimits(source, requested::Limits)
    limits = if source isa _OpenedReader
        configured = _configuredlimits(source)
        requested == configured || throw(
            ArgumentError(
                "cannot change limits for an already-opened $(nameof(typeof(source))) " *
                "handle; omit `limits` or pass the handle's configured value",
            ),
        )
        configured
    else
        requested
    end
    _validatelimits(limits)
    return limits
end

_isfilebytes(bytes::Vector{UInt8}) = length(bytes) >= 6 && view(bytes, 1:6) == _FILE_MAGIC

# A successful ArrowFile adopts the mapped region. Before that handoff, this
# layer owns the region and must release it if validation or parsing fails.
# This is observable on Windows, where an unreleased mapping prevents the
# source file from being deleted.
function _readmappedfile(
    region::AC.OwnerRegion,
    limits::Limits,
    budget::Union{Nothing,AllocationBudget},
)
    try
        return budget === nothing ? readfile(region; limits=limits) :
               _readfile(region, limits, budget)
    catch
        AC.release!(region)
        rethrow()
    end
end

function _openbytes(
    bytes::Vector{UInt8};
    limits::Limits=Limits(),
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    if budget === nothing
        return _isfilebytes(bytes) ? readfile(bytes; limits=limits) :
               readstream(bytes; limits=limits)
    end
    return _isfilebytes(bytes) ? _readfile(heapregion(bytes), limits, budget) :
           _readstream(bytes, limits, budget)
end

function _opensource(
    path::AbstractString;
    mmap::Bool=true,
    limits::Limits=Limits(),
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    return open(path, "r") do io
        magic = Base.read(io, 6)
        seekstart(io)
        if magic == _FILE_MAGIC && mmap
            # Probe and map the same open file. A replacement of `path`
            # between those steps cannot redirect the read to another inode.
            region = AC._mmapregion(io, path)
            return _readmappedfile(region, limits, budget)
        end
        reported = stat(io).size
        (reported isa Integer && 0 <= reported <= typemax(Int)) ||
            throw(AC.ValidationError("source file length is not addressable"))
        size = Int(reported)
        _chargevector!(budget, UInt8, size, "source file read")
        bytes = Base.read(io, size)
        length(bytes) == size || throw(
            AC.ValidationError(
                "source file changed while reading: got $(length(bytes)) of $size bytes",
            ),
        )
        return _openbytes(bytes; limits=limits, budget=budget)
    end
end
function _opensource(io::IO; mmap::Bool=true, limits::Limits=Limits(), budget=nothing)
    if budget === nothing
        return _openbytes(Base.read(io); limits=limits)
    end
    # A generic IO has no reliable remaining-length query. Read bounded
    # chunks whose allocation is reserved first, then reserve the one final
    # assembly while all chunks are still live.
    _chargevector!(budget, Vector{UInt8}, 0, "source IO chunk references")
    chunks = Vector{UInt8}[]
    total = Int64(0)
    chunksize = Int64(64 * 1024)
    while !eof(io)
        _chargevector!(budget, UInt8, chunksize, "source IO chunk")
        chunk = Base.read(io, Int(chunksize))
        # Julia grows a pushed Vector geometrically, and recent runtimes add
        # another backing-store size-class round. Eight pointer slots per
        # logical chunk conservatively cover both layers; this is negligible
        # beside each 64 KiB chunk but keeps the accounting self-contained.
        _charge!(
            budget,
            Int64(8 * Base.elsize(Vector{Vector{UInt8}})),
            "source IO chunk reference",
        )
        push!(chunks, chunk)
        total = AC.checked_add(total, Int64(length(chunk)))
    end
    _chargevector!(budget, UInt8, total, "source IO assembly")
    bytes = isempty(chunks) ? UInt8[] : reduce(vcat, chunks)
    return _openbytes(bytes; limits=limits, budget=budget)
end
_opensource(
    bytes::Vector{UInt8};
    mmap::Bool=true,
    limits::Limits=Limits(),
    budget=nothing,
) = _openbytes(bytes; limits=limits, budget=budget)
_opensource(
    src::Union{IPCStream,ArrowFile};
    mmap::Bool=true,
    limits::Limits=_configuredlimits(src),
    budget=nothing,
) = src
# A byte-range source is read whole: iteration is sequential over every
# batch, so there is nothing for range planning to skip.
function _opensource(
    src::AbstractArrowSource;
    mmap::Bool=true,
    limits::Limits=Limits(),
    budget=nothing,
)
    return _opensource(
        SourceFile(src; limits=limits);
        mmap=mmap,
        limits=limits,
        budget=budget,
    )
end
function _opensource(
    sf::SourceFile;
    mmap::Bool=true,
    limits::Limits=sf.limits,
    budget=nothing,
)
    return _openbytes(_wholeobject(sf, budget); limits=limits, budget=budget)
end

# --- Table construction ------------------------------------------------------

function Table(
    source;
    scan::Union{Nothing,Tables.Scan}=nothing,
    mmap::Bool=true,
    limits::Limits=_defaultreaderlimits(source),
)
    readlimits = _readerlimits(source, limits)
    if source isa AbstractArrowSource || source isa SourceFile
        sf = source isa SourceFile ? source : SourceFile(source; limits=readlimits)
        _requirelittleendian()
        budget = AllocationBudget(readlimits.max_total_allocated_bytes)
        if scan === nothing
            opened = _openbytes(_wholeobject(sf, budget); limits=readlimits, budget=budget)
            return _tablefrom(opened, nothing, budget)
        end
        # Range planning pays off only for a pushable scan over a file-format
        # object with columns. With a scan the schema comes up front from
        # the cached tail: literal lowering, exactly-once output conversion,
        # and DataAPI metadata all need it.
        if _isfilesource(sf, budget)
            ft = _rangedfooter(sf, budget)
            plan = _ScanPlan(scan, ft.fields, budget)
            if !isempty(ft.fields) && plan.storage !== nothing
                return _applyfacadescan(sf, plan, ft, budget)
            end
            # A public-domain fallback still reuses the scan plan compiled
            # from the Footer schema after the object is read in full. Keep
            # the same budget through whole-object assembly, parsing, and
            # lazy batch materialization.
            opened = _openbytes(_wholeobject(sf, budget); limits=readlimits, budget=budget)
            return _tablefrom(opened, plan, budget)
        end
        # A stream-format object has no Footer to plan from. Read it whole,
        # without resetting the budget used by the format probe, then execute
        # the scan over its decoded columns.
        opened = _openbytes(_wholeobject(sf, budget); limits=readlimits, budget=budget)
        return _tablefrom(opened, scan, budget)
    end
    budget =
        source isa IPCStream ? source.budget :
        AllocationBudget(readlimits.max_total_allocated_bytes)
    opened = _opensource(source; mmap=mmap, limits=readlimits, budget=budget)
    scan === nothing && return _tablefrom(opened, nothing, budget)
    return _tablefrom(opened, scan, budget)
end

# A Table from an opened IPC source: the whole thing, or a scan pushed
# where the format can prove it and evaluated over the rest.
function _tablefrom(
    src::Union{IPCStream,ArrowFile},
    scan::Union{Nothing,Tables.Scan},
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    regions = _sourceregions(src, budget)
    fields = _corefields(src, budget)
    scan === nothing && return _materialize_table(src, regions, budget)
    return _tablefrom(src, _ScanPlan(scan, fields, budget), regions, fields, budget)
end

function _tablefrom(
    src::Union{IPCStream,ArrowFile},
    plan::_ScanPlan,
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    return _tablefrom(
        src,
        plan,
        _sourceregions(src, budget),
        _corefields(src, budget),
        budget,
    )
end

function _tablefrom(src, plan::_ScanPlan, regions, fields, budget)
    # Zero-field sources carry their row count on the Table itself; the raw
    # scan path would lose it inside an empty NamedTuple.
    isempty(fields) && return _publicscan(
        _materialize_table(src, regions, budget),
        _tableschema(src),
        fields,
        plan,
        regions,
        budget,
    )
    plan.storage !== nothing && return _applyfacadescan(src, plan, budget)
    # A scan with an unrepresentable storage-domain literal evaluates its
    # public-domain plan over the fully converted table.
    return _publicscan(
        _materialize_table(src, regions, budget),
        _tableschema(src),
        fields,
        plan,
        regions,
        budget,
    )
end

function _corefields(source::Union{IPCStream,ArrowFile}, budget=nothing)
    fields = source isa IPCStream ? source.corefields : source.fields
    _chargevector!(budget, AC.Field, length(fields), "source field list")
    return collect(AC.Field, fields)
end

_tableschema(s::IPCStream) = s.schema
_tableschema(f::ArrowFile) = f.schema

"Merge category snapshots while preserving every entry in the first snapshot."
function _mergecategorypools(pools; widen::Bool=false, budget=nothing)
    isempty(pools) && throw(ArgumentError("cannot merge an empty category-pool set"))
    length(pools) == 1 && return first(pools)
    # Public `hash`/`isequal` methods are logical API, not Arrow encoding
    # identity. IdDict uses Julia's non-overridable `===` relation. This may
    # retain harmless duplicate categories, which Arrow dictionaries permit,
    # but it can never merge two distinct physical values.
    capacity = Int64(0)
    for pool in pools
        capacity = AC.checked_add(capacity, Int64(length(pool)))
    end
    capacity <= typemax(Int) ||
        throw(AC.ValidationError("merged category pool is not addressable"))
    T = widen ? Any : eltype(first(pools))
    _chargevector!(budget, T, capacity, "merged category pool")
    _chargedict!(budget, Any, Nothing, capacity, "category identity index")
    out = Vector{T}()
    sizehint!(out, Int(capacity))
    append!(out, first(pools))
    seen = IdDict{Any,Nothing}()
    sizehint!(seen, Int(capacity))
    for value in out
        seen[value] = nothing
    end
    for pool in Iterators.drop(pools, 1), value in pool
        haskey(seen, value) && continue
        push!(out, value)
        seen[value] = nothing
    end
    return out
end

"Retain one top-level dictionary's category order across pool snapshots."
function _retaineddictpool(f::AC.Field, batches, i::Int, arrowtypes::_ArrowTypesContext)
    t = f.type
    t isa AC.DictionaryType || return nothing
    # DictionaryType stores the value descriptor on the index Field. Reapply
    # that Field's extension metadata while materializing pool snapshots so a
    # registered dictionary value and its nested children lift exactly once.
    vf = _arrowtypesdictvaluefield(arrowtypes, f, t; retainmetadata=true)
    isempty(batches) && return _facadecolumn(vf, Any[], arrowtypes)
    firstdictionary = batches[1].columns[i].dictionary::AC.ArrayData
    if all(batch -> batch.columns[i].dictionary === firstdictionary, batches)
        return _facadesinglecolumn(vf, firstdictionary, arrowtypes)
    end
    _chargevector!(
        arrowtypes.budget,
        AC.ArrayData,
        length(batches),
        "dictionary snapshot list",
    )
    _chargedict!(
        arrowtypes.budget,
        AC.ArrayData,
        Nothing,
        length(batches),
        "dictionary snapshot identity index",
    )
    dictionaries = AC.ArrayData[]
    sizehint!(dictionaries, length(batches))
    seen = Base.IdSet{AC.ArrayData}()
    sizehint!(seen, length(batches))
    for batch in batches
        dictionary = batch.columns[i].dictionary::AC.ArrayData
        dictionary in seen && continue
        push!(seen, dictionary)
        push!(dictionaries, dictionary)
    end
    _chargevector!(
        arrowtypes.budget,
        AbstractVector,
        length(dictionaries),
        "materialized dictionary pools",
    )
    pools = AbstractVector[
        _facadesinglecolumn(vf, dictionary, arrowtypes) for dictionary in dictionaries
    ]
    # Replacement pools can add categories, but the first pool's order and
    # duplicate entries remain authoritative. Encoding identity never calls a
    # public-domain value's equality or hashing methods.
    return _mergecategorypools(
        pools;
        widen=vf.type isa AC.UnionType,
        budget=arrowtypes.budget,
    )
end

function _retaineddictpools(fields, batches, arrowtypes::_ArrowTypesContext)
    _chargevector!(arrowtypes.budget, Any, length(fields), "retained dictionary pool slots")
    return Any[_retaineddictpool(f, batches, i, arrowtypes) for (i, f) in enumerate(fields)]
end

function _materialize_table(src::IPCStream, regions, budget=nothing)
    budget === nothing && (budget = src.budget)
    names = _fieldnamesymbols(src.schema.fields, budget)
    arrowtypes = _ArrowTypesContext(budget=budget)
    _chargevector!(budget, AbstractVector, length(src.corefields), "table columns")
    cols = AbstractVector[
        _facadebatchcolumn(f, src.batches, i, arrowtypes) for
        (i, f) in enumerate(src.corefields)
    ]
    nrows = sum(Int(b.nrows) for b in src.batches; init=0)
    pools = _retaineddictpools(src.corefields, src.batches, arrowtypes)
    return _table(names, cols, src.schema, regions, nrows, pools, budget)
end

function _materialize_table(src::ArrowFile, regions, budget=nothing)
    budget === nothing && (budget = AllocationBudget(src.limits.max_total_allocated_bytes))
    names = _fieldnamesymbols(src.schema.fields, budget)
    arrowtypes = _ArrowTypesContext(budget=budget)
    nb = length(src)
    _chargevector!(budget, AC.RecordBatch, nb, "decoded batch list")
    state = DecodeState(budget)
    batches = try
        [_filebatch(src, i, state) for i = 1:nb]
    finally
        close(state)
    end
    _chargevector!(budget, AbstractVector, length(src.fields), "table columns")
    cols = AbstractVector[
        _facadebatchcolumn(f, batches, i, arrowtypes) for (i, f) in enumerate(src.fields)
    ]
    nrows = sum(Int(b.nrows) for b in batches; init=0)
    pools = _retaineddictpools(src.fields, batches, arrowtypes)
    return _table(names, cols, src.schema, regions, nrows, pools, budget)
end

# The eltype the keep/drop decision uses for an EMPTY pre-override column:
# the descriptor's declared facade type. Composites materialize rows as
# vectors (their eltype accident is `Any[]` when no rows exist), so the
# declared domain — not the accident — must drive subsumption, keeping the
# empty decision identical to the nonempty one. Field-aware rules:
#   * The declared domain must equal the ACTUAL pre-override container type.
#     `_postconvert` dispatches on the ROOT descriptor only, so temporal
#     leaves under a transparent wrapper stay RAW storage integers (the
#     `converted` flag tracks that).
#   * Run-end encoding is transparent at the value layer: rows ARE the
#     values child's rows, with no REE-level validity.
#   * Dictionary rows are pool VALUES, and their composite children live on
#     the value FIELD (Dictionary<REE<...>>).
#   * A multi-child union declares what a valid MIXED population
#     materializes as: Julia's pairwise `promote_typejoin`, exactly the
#     widening `map(identity)` performs — not the mathematical union of the
#     child domains.
# Missing in the declared type never changes keep/drop (the rule tests
# `D <: Union{T,Missing}`), so nullability wraps are cosmetic.
function _declaredeltype(f::AC.Field, converted::Bool=true)
    t = f.type
    if t isa AC.RunEndEncodedType && length(f.children) == 2
        return _declaredeltype(f.children[2], false)
    end
    if t isa AC.DictionaryType
        # The schema carries ONE nullability flag for a dictionary column;
        # `dictvaluefield` marks the pool field nullable because the format
        # cannot say otherwise. That is not a declaration: strip it here and
        # let a pool that actually holds nulls widen the public type through
        # the data check (`_publictype`/`_hasnulls`), like any other null a
        # non-nullable declaration did not admit.
        D0 = Base.nonmissingtype(_declaredeltype(AC.dictvaluefield(f, t), converted))
        return f.nullable ? Union{Missing,D0} : D0
    end
    if t isa AC.UnionType && !isempty(f.children)
        D = _declaredeltype(f.children[1], false)
        for k = 2:length(f.children)
            D = Base.promote_typejoin(D, _declaredeltype(f.children[k], false))
        end
        return D
    end
    D = converted ? _declaredbasetype(t) : _rawdeclaredbasetype(t)
    return f.nullable ? Union{Missing,D} : D
end

# The units the facade converts at the TOP level; under a wrapper their
# columns keep Core storage integers, sized by the descriptor width.
_istemporalconv(t::AC.ArrowType) =
    t isa AC.DateType ||
    t isa AC.TimeType ||
    t isa AC.DurationType ||
    (t isa AC.TimestampType && (t.unit == AC.SECOND || t.unit == AC.MILLISECOND))
_rawdeclaredbasetype(t::AC.ArrowType) =
    _istemporalconv(t) ? (AC.primwidth(t) == 4 ? Int32 : Int64) : _declaredbasetype(t)
# One entry per Core layout whose _value materializes a CLOSED row type
# (the _value methods are the authority): every one must appear here, or
# empty and nonempty columns of that layout would decide keep/drop
# differently.
_declaredbasetype(t::AC.ArrowType) =
    t isa AC.ListType ? Vector{Any} :
    t isa AC.ListViewType ? Vector{Any} :
    t isa AC.FixedSizeListType ? Vector{Any} :
    t isa AC.BinaryType ? Vector{UInt8} :
    t isa AC.FixedSizeBinaryType ? Vector{UInt8} :
    (t isa AC.ViewType && !t.utf8) ? Vector{UInt8} :
    t isa AC.StructType ? Vector{Pair{String,Any}} :
    t isa AC.MapType ? Vector{Pair{Any,Any}} :
    t isa AC.NullType ? Missing :
    t isa AC.DecimalType ? (_shareddecimal(t) ? _decimalhost(t) : _rawdeclaredbasetype(t)) :
    t isa AC.IntervalType ? Durations.Duration :
    t isa AC.DictionaryType ? _declaredbasetype(t.valuetype) : _facadebasetype(t)

# --- Stream ------------------------------------------------------------------

"""
    Arrow.Stream(source; mmap=true, limits)

Iterate an IPC source (a file path, an `IO`, a `Vector{UInt8}`, or an
[`Arrow.AbstractArrowSource`](@ref), which is read whole) one record batch
at a time; each iteration yields an [`Arrow.Table`](@ref) for that batch.
`mmap=true` memory-maps a file-format path instead of reading it into
memory; it has no effect on the other source kinds. Satisfies `Tables.partitions` (each
batch is one partition), so partition-aware sinks — including `Arrow.write`,
which writes one record batch per partition — see the source batch structure.

`limits` has the same policy as [`Arrow.Table`](@ref). It configures a raw
source. An internal reader handle keeps the policy that verified it.

Memory: over a memory-mapped FILE-format path (the default for a path) the
batches are decoded lazily from the mapping, one per iteration, so a
consumer that processes and drops batches holds one batch of columns at a
time (plus the file's dictionaries) — the path for a file larger than RAM.
The `Limits.max_total_allocated_bytes` security budget is cumulative across
one `Stream` iterator, even after a batch is dropped; raise it explicitly for
a trusted large file whose total decoded allocation exceeds the default.
Every yielded `Table` shares the source lifetime: releasing a batch closes its
parent `Stream`, while the batch's materialized columns remain usable.
A STREAM-format source is read to the end and every batch is decoded when
the `Stream` is constructed. A file-format `IO` or byte-vector input is read
to the end too (the whole source stays in memory) but its record batches
still decode lazily, one per iteration. `Arrow.write` itself is whole-buffer
(it materializes every partition before writing), so it does not bound
memory either.
"""
struct Stream
    src::Union{IPCStream,ArrowFile}
    regions::Vector{AC.OwnerRegion}
    budget::AllocationBudget
    closed::AC.ReleaseCell
    arrowtypes::_ArrowTypesContext
    arrowtypeslock::ReentrantLock
end

function Stream(source; mmap::Bool=true, limits::Limits=_defaultreaderlimits(source))
    readlimits = _readerlimits(source, limits)
    budget =
        source isa IPCStream ? source.budget :
        AllocationBudget(readlimits.max_total_allocated_bytes)
    src = _opensource(source; mmap=mmap, limits=readlimits, budget=budget)
    regions = _sourceregions(src, budget)
    # The stream and every yielded partition share one lifetime authority.
    # Releasing either closes the same source region and makes later
    # iteration fail consistently for borrowed and decompressed columns.
    # `_sourceregions` returns exactly one region for both source kinds, so
    # `only` cannot throw; a second region would need to share this cell,
    # not add one — the Stream has a single close authority.
    return Stream(
        src,
        regions,
        budget,
        only(regions).cell,
        _ArrowTypesContext(budget=budget),
        ReentrantLock(),
    )
end

function AC.release!(s::Stream)
    AC.release!(getfield(s, :closed))
    foreach(AC.release!, getfield(s, :regions))
    return nothing
end

_nbatches(s::IPCStream) = length(s.batches)
_nbatches(f::ArrowFile) = length(f)
_batch(s::IPCStream, i, budget) = s.batches[i]
function _batch(f::ArrowFile, i, budget)
    state = DecodeState(budget)
    try
        return _filebatch(f, i, state)
    finally
        close(state)
    end
end
_batchfields(s::IPCStream) = s.corefields
_batchfields(f::ArrowFile) = f.fields

Base.length(s::Stream) = _nbatches(s.src)
Base.eltype(::Type{Stream}) = Table

function Base.iterate(s::Stream, i::Int=1)
    closed = getfield(s, :closed)
    (@atomic :acquire closed.closed) &&
        throw(InvalidStateException("the stream was released", :closed))
    i > _nbatches(s.src) && return nothing
    b = _batch(s.src, i, s.budget)
    fields = _batchfields(s.src)
    names = _fieldnamesymbols(fields, s.budget)
    arrowtypes = getfield(s, :arrowtypes)
    arrowtypeslock = getfield(s, :arrowtypeslock)
    lock(arrowtypeslock)
    try
        _chargevector!(s.budget, AbstractVector, length(fields), "table columns")
        cols = AbstractVector[
            _facadesinglecolumn(f, b.columns[j], arrowtypes) for (j, f) in enumerate(fields)
        ]
        pools = _retaineddictpools(fields, (b,), arrowtypes)
        return _table(
            names,
            cols,
            _tableschema(s.src),
            s.regions,
            Int(b.nrows),
            pools,
            s.budget,
        ),
        i + 1
    finally
        unlock(arrowtypeslock)
    end
end

Tables.partitions(s::Stream) = s
