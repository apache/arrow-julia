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
# `Arrow.Table` reads flat columns as zero-copy views (`columns.jl`) and
# materializes nested and union columns (closed element-type claims through
# Core's typed path, everything else through the dynamic accessors).
# `Arrow.Stream` iterates record batches as one Table each. Scan pushdown
# routes through the ranged-scan adapter: on the file format, column
# pruning, statistics-based batch pruning, and window consumption all happen
# before decode.
# =============================================================================

"""
    Arrow.Table(source; select=nothing, drop=nothing, filter=nothing, limit=nothing,
                scan=nothing, mmap, stringtype=DataString, limits, ntasks) -> Table

Read Arrow IPC data as Tables.jl columns. `source` is a file path, an `IO`,
raw bytes (`Vector{UInt8}`), or an [`Arrow.AbstractArrowSource`](@ref) — a
byte-range-addressable object, such as one in cloud storage, read with
exact range requests. Both IPC formats are accepted: the file format
(`ARROW1` magic, random access, footer statistics) and the stream format.

- `select` keeps the given columns, in that order (names, indices, a regex,
  or a vector of them); `drop` keeps all but the given columns.
- `filter` keeps the rows where a predicate built from [`Arrow.col`](@ref)
  is `true`, for example `filter=Arrow.col(:fare) > 10`.
- `limit` keeps at most that many (filtered) rows.
- `scan` takes a full `Tables.Scan` (renames, type overrides, `offset`,
  `validate`) instead of the keywords above.

Flat columns read their Arrow buffers in place: numbers, booleans, temporal
values, decimals, and dictionary-encoded values are read-only
[`Arrow.ArrowVector`](@ref) views, so building a table costs time
proportional to its columns and record batches, not its bytes. A column
with several record batches is a `ChainedVector` of per-batch views. String
and binary columns are `DataStrings.StringVector`/`BytesVector`, built with
one copy of their bytes the first time the table hands them out (the read's
allocation allowance is charged when it opens); `stringtype=String` reads
strings as `Vector{String}`. List, map, struct, and union columns read as typed rows:
[`Arrow.ListValue`](@ref) and [`Arrow.MapValue`](@ref) views, `NTuple`s,
`NamedTuple`s, and `Union`s. A
column keeps the memory it reads alive while it is reachable, independent of
its table. `copy(table)`, `DataFrame(table)`, and [`Arrow.read`](@ref) return
owned, mutable columns.

`mmap=true` memory-maps a path; a mapping is unmapped once the table and all
of its columns are garbage collected. It is the default except
on Windows, which cannot delete a file while it is mapped: there a path is
read into memory unless `mmap=true` is passed. `mmap` has no effect on the
other source kinds. A byte vector is read in place: do not modify or resize
it while the table's columns are in use.

`ntasks` (default `Threads.nthreads()`) is the number of tasks a read uses:
record batches decode and decompress field by field in parallel, columns
build in parallel, and a string column builds in chunks of rows. `ntasks=1`
reads on the calling task. The result does not depend on `ntasks`.

`limits` sets the [`Arrow.Limits`](@ref) policy for a raw source. Omit it to
use `Arrow.Limits()`. An internal reader handle that was opened under a
specific policy keeps that policy; a facade call cannot retroactively
re-verify it under different limits.

A read with these keywords decodes only the selected and filter-referenced
columns, footer statistics prune batches no row of which can match the
filter, and the filter is evaluated batch by batch. `limit`/`offset` compose exactly over the qualifying rows. Without a
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

"""
    copy(table::Arrow.Table) -> Arrow.Table

Copy every column into owned, mutable memory (`copy` of each column) and
retain the schema and dictionary category order. The new table reads nothing
from the source. As with `copy` on Julia vectors, nested mutable values can
still be shared.
"""
function Base.copy(t::Table)
    _checkopen(t)
    return _table(
        copy(getfield(t, :names)),
        AbstractVector[copy(_force(c)) for c in getfield(t, :columns)],
        getfield(t, :schema),
        AC.OwnerRegion[],
        getfield(t, :nrows),
        Any[p === nothing ? nothing : copy(p) for p in getfield(t, :retainedpools)],
    )
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
Tables.getcolumn(t::Table, i::Int) = (_checkopen(t); _force(getfield(t, :columns)[i]))
Tables.getcolumn(t::Table, nm::Symbol) =
    (_checkopen(t); _force(getfield(t, :columns)[_columnindex(t, nm)]))

# A released table refuses column access; columns taken before stay valid.
function _checkopen(t::Table)
    for r in getfield(t, :regions)
        (@atomic :acquire r.cell.closed) &&
            throw(InvalidStateException("the table was released", :closed))
    end
    return nothing
end
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
# `sym` names `s`, compared without interning `s` (Symbols hold no NUL).
function _symbolis(sym::Symbol, s::String)
    p = Base.unsafe_convert(Ptr{UInt8}, sym)
    n = ncodeunits(s)
    ccall(:strlen, Csize_t, (Ptr{UInt8},), p) == n || return false
    return GC.@preserve s ccall(
        :memcmp,
        Cint,
        (Ptr{UInt8}, Ptr{UInt8}, Csize_t),
        p,
        pointer(s),
        n,
    ) == 0
end

# An ordinary facade read retains one field per output column in the same
# order and under the same name.
function _schemaaligned(t::Table)
    names = getfield(t, :names)
    sch = getfield(t, :schema)
    sch === nothing && return false
    fields = sch.fields
    length(fields) == length(names) || return false
    for i in eachindex(names)
        _symbolis(names[i], fields[i].name) || return false
    end
    return true
end

function _schemafield(t::Table, col::Int, aligned::Bool=_schemaaligned(t))
    names = getfield(t, :names)
    checkbounds(names, col)
    sch = getfield(t, :schema)
    sch === nothing && return nothing
    fields = sch.fields
    # Position is the only unambiguous identity when Arrow legally carries
    # duplicate field names.
    aligned && return fields[col]
    # A real Tables.Scan type conversion drops that column's retained field,
    # so its schema can be an ordered subset. Name lookup remains safe only
    # when the output name itself is unique.
    nm = names[col]
    # The lookup holds 0 for a name that appears more than once.
    getfield(t, :lookup)[nm] == 0 && return nothing
    text = String(nm)
    i = findfirst(f -> f.name == text, fields)
    return i === nothing ? nothing : sch.fields[i]
end
function _schemafield(t::Table, col::Symbol)
    return _schemafield(t, _columnindex(t, col))
end
function DataAPI.colmetadatakeys(t::Table, col::Union{Symbol,Int})
    c = col isa Symbol ? _columnindex(t, col) : col
    return _colmetadatakeys(_schemafield(t, c))
end
function DataAPI.colmetadatakeys(t::Table)
    # One alignment check for the whole listing keeps it linear in the
    # column count.
    aligned = _schemaaligned(t)
    return (
        nm => keys for (i, nm) in enumerate(getfield(t, :names)) for
        keys in (_colmetadatakeys(_schemafield(t, i, aligned)),) if !isempty(keys)
    )
end
_colmetadatakeys(f) =
    (f === nothing || f.metadata === nothing) ? () :
    (String(first(kv)) for kv in f.metadata)
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
    Arrow.getmetadata(x)

Compatibility form of the Arrow 2.x metadata accessor. For an `Arrow.Table`,
returns the table-level key-value metadata as a `Dict{String,String}`, or
`nothing` when the table has none. Every other object returns `nothing`,
including other Tables.jl tables, so `metadata=Arrow.getmetadata(table)` works
for any `Arrow.write` input. New code should prefer the DataAPI.jl interface
(`DataAPI.metadata`, `DataAPI.metadatakeys`), which `Arrow.Table` supports.

A column also returns `nothing`; use
`DataAPI.colmetadata(table, column, key)`/`DataAPI.colmetadatakeys(table)` for
per-column metadata.
"""
function getmetadata(t::Table)
    sch = getfield(t, :schema)
    (sch === nothing || sch.metadata === nothing || isempty(sch.metadata)) && return nothing
    return Dict{String,String}(String(first(kv)) => String(last(kv)) for kv in sch.metadata)
end
# Arrow 2.x returned `nothing` for every non-Arrow object; a row table such as
# a `Vector` of records is indistinguishable from a column here.
getmetadata(::Any) = nothing

"""
    Arrow.release!(t::Union{Table,Stream})

End a read: the table refuses further column access, and a stream refuses
further iteration. Every `Table` a `Stream` yields shares its source, so
releasing either one releases both. Columns taken before the release remain
usable and keep the memory they read: a memory-mapped file is unmapped once
the table and all of its columns are garbage collected. Idempotent.
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
const _NANOS_PER_DAY = Int64(86_400_000_000_000)

"""
The public `Dates.Time` for one stored time-of-day value. `Dates.Time` wraps
modulo 24 hours, so a value outside the format's [0, 24h) range has no
exact public value and is refused instead of aliasing another time.
"""
function _publictime(unit, x)
    perday =
        unit == AC.SECOND ? Int64(86_400) :
        unit == AC.MILLISECOND ? Int64(86_400_000) :
        unit == AC.MICROSECOND ? Int64(86_400_000_000) : _NANOS_PER_DAY
    v = Int64(x)
    0 <= v < perday ||
        throw(ValidationError("time-of-day value $v is outside [0, $perday) in its unit"))
    return Dates.Time(Dates.Nanosecond(v * (_NANOS_PER_DAY ÷ perday)))
end

# Map a temporal descriptor to the compile-time token the lowering loops
# dispatch on; eighteen values total (2 date + 4 timestamp + 4 zoned
# timestamp + 4 time + 4 duration). Zone-declared timestamps carry their own
# kind: their literal domain (`ZonedTimestamp`/`ZonedDateTime`) is disjoint
# from the zone-naive one.
_facadetoken(t::AC.DateType) =
    t.unit == AC.DAY ? Val((:date, AC.DAY)) : Val((:date, AC.MILLISECOND_DATE))
_facadetoken(t::AC.TimestampType) =
    if _timestampzone(t) === nothing
        t.unit == AC.SECOND ? Val((:timestamp, AC.SECOND)) :
        t.unit == AC.MILLISECOND ? Val((:timestamp, AC.MILLISECOND)) :
        t.unit == AC.MICROSECOND ? Val((:timestamp, AC.MICROSECOND)) :
        Val((:timestamp, AC.NANOSECOND))
    else
        t.unit == AC.SECOND ? Val((:zonedtimestamp, AC.SECOND)) :
        t.unit == AC.MILLISECOND ? Val((:zonedtimestamp, AC.MILLISECOND)) :
        t.unit == AC.MICROSECOND ? Val((:zonedtimestamp, AC.MICROSECOND)) :
        Val((:zonedtimestamp, AC.NANOSECOND))
    end
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

# The Arrow spec treats an absent and an empty timezone string the same way:
# the column is zone-naive. One helper keeps every facade rule on that page.
@inline _timestampzone(t::AC.TimestampType) =
    t.timezone === nothing || isempty(t.timezone) ? nothing : t.timezone

@inline _unitticks(unit)::Int64 =
    unit == AC.SECOND ? Int64(1) :
    unit == AC.MILLISECOND ? Int64(1_000) :
    unit == AC.MICROSECOND ? Int64(1_000_000) : Int64(1_000_000_000)

# Ticks per second of a Timestamp/ZonedTimestamp period. Package-defined
# periods return `nothing`: their conversion is request-defined code, so
# their literals stay in the public domain.
@inline _periodticks(::Type{Dates.Second})::Union{Nothing,Int64} = Int64(1)
@inline _periodticks(::Type{Dates.Millisecond})::Union{Nothing,Int64} = Int64(1_000)
@inline _periodticks(::Type{Dates.Microsecond})::Union{Nothing,Int64} = Int64(1_000_000)
@inline _periodticks(::Type{Dates.Nanosecond})::Union{Nothing,Int64} = Int64(1_000_000_000)
@inline _periodticks(::Type)::Union{Nothing,Int64} = nothing

_instantperiod(::Durations.Timestamp{P}) where {P} = P
_instantperiod(::Durations.ZonedTimestamp{P}) where {P} = P

"Exactly rescale an instant count between tick rates, or `nothing`."
@inline function _rescalecount(count::Int64, from::Int64, to::Int64)::Union{Nothing,Int64}
    from == to && return count
    to > from && return _boundedint64(Int128(count) * Int128(to ÷ from))
    q, r = divrem(count, from ÷ to)
    return iszero(r) ? q : nothing
end

# Zone-NAIVE timestamp columns: `Durations.Timestamp`, `DateTime`, and `Date`
# literals lower by instant. Zoned values never lower here — the public
# domain defines them as unequal to every zone-naive value.
@inline function _timestampstorage(unit, value)::Union{Nothing,Int64}
    if value isa Durations.Timestamp
        from = _periodticks(_instantperiod(value))
        from === nothing && return nothing
        return _rescalecount(Int64(Dates.value(value)), from, _unitticks(unit))
    end
    value isa Durations.ZonedTimestamp && return nothing
    millis = _epochmillis(value)
    millis === nothing && return nothing
    return _rescalecount(millis, Int64(1_000), _unitticks(unit))
end

# Zone-DECLARED timestamp columns: a `ZonedTimestamp` literal in ANY zone
# lowers to its UTC instant — public `==`/`isless` between zoned values
# compare only the UTC time, so the zone label plays no role. With TimeZones
# loaded, `ZonedDateTime` literals lower the same way through the extension
# (it lowers only its own type and never throws). Zone-naive literals stay
# public: the domain defines them as unequal to every zoned value.
@inline function _zonedtimestampstorage(unit, value)::Union{Nothing,Int64}
    if value isa Durations.ZonedTimestamp
        from = _periodticks(_instantperiod(value))
        from === nothing && return nothing
        return _rescalecount(Int64(Dates.value(value)), from, _unitticks(unit))
    end
    ext = Base.get_extension(@__MODULE__, :ArrowTimeZonesExt)
    ext === nothing && return nothing
    z = ext.zonedstorage(unit, value)
    return z === nothing ? nothing : z::Int64
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

`K` is one of the eighteen `_facadetoken` descriptor/unit values. A result is
exact. `nothing` selects the public-domain path or, for a Duration scalar,
the guarded custom-conversion path below.
"""
@inline function _exactfacadevalue(::Val{K}, value)::Union{Nothing,Int32,Int64} where {K}
    kind, unit = K
    if kind === :date
        return unit == AC.DAY ? _datestorage(value) : _epochmillis(value)
    elseif kind === :timestamp
        return _timestampstorage(unit, value)
    elseif kind === :zonedtimestamp
        return _zonedtimestampstorage(unit, value)
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
@inline _exactfacadescalar(t::AC.TimestampType, value) =
    _timestampzone(t) === nothing ? _timestampstorage(t.unit, value) :
    _zonedtimestampstorage(t.unit, value)
@inline _exactfacadescalar(t::AC.TimeType, value) = _timestorage(t.unit, value)
@inline _exactfacadescalar(t::AC.DurationType, value) = _durationstorage(t.unit, value)

_facadescalarvalue(t::Union{AC.DateType,AC.TimeType}, value) = _exactfacadescalar(t, value)

_facadescalarvalue(t::AC.TimestampType, value) = _exactfacadescalar(t, value)

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
    # temporal public literals — including Timestamp and ZonedTimestamp,
    # which are AbstractDateTime — are incompatible with them.
    if v isa Dates.Date ||
       v isa Dates.AbstractDateTime ||
       v isa Dates.Time ||
       v isa Dates.Period
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

@inline _timestampperiod(unit) =
    unit == AC.SECOND ? Dates.Second :
    unit == AC.MILLISECOND ? Dates.Millisecond :
    unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond

# The `ZonedTimestamp{P,Z}` (and `Timestamp{P}`) element type of a
# zone-declared (naive micro/nanosecond) timestamp column. Both types have
# the 8-byte layout of the Arrow column itself — every Int64 is a valid
# value — so conversion is a per-element reinterpret.
_zonedtimestamptype(unit, zone::String) =
    Durations.ZonedTimestamp{_timestampperiod(unit),_zonesymbol(zone)}

# The declared zone becomes a type parameter. A Symbol cannot hold an
# embedded NUL, and the facade's per-name bound keeps input-controlled zone
# names from interning arbitrarily large process-global state.
function _zonesymbol(zone::String)
    occursin('\0', zone) &&
        throw(AC.ValidationError("timestamp timezone contains an embedded NUL"))
    ncodeunits(zone) <= _MAX_ARROWTYPE_SCHEMA_NAME_BYTES || throw(
        AC.ValidationError(
            "timestamp timezone exceeds $_MAX_ARROWTYPE_SCHEMA_NAME_BYTES UTF-8 bytes",
        ),
    )
    return Symbol(zone)
end

# Function barrier: `T` arrives as a runtime type (the zone name is a type
# parameter), one dynamic call per column, and the element loop compiles
# type-stable per `T`.
_instantcolumn(::Type{T}, col, budget) where {T} =
    _mapcol(T, x -> reinterpret(T, Int64(x)), col, budget)

_postconvert(::AC.ArrowType, col, budget=nothing) = col

# The convertible-leaf authority, per SCALAR: the same conversions the
# `_postconvert` column methods apply, for one value at a time. The dynamic
# row builder below uses it where a leaf lands inside an `Any`-shaped row.
# A differential test pins scalar/column agreement for every leaf kind.
_isconvertibleleaf(t::AC.ArrowType) =
    t isa AC.DateType ||
    t isa AC.TimeType ||
    t isa AC.TimestampType ||
    t isa AC.DurationType ||
    t isa AC.IntervalType ||
    (t isa AC.DecimalType && _shareddecimal(t))

function _publicleafscalar(t::AC.ArrowType, x)
    if t isa AC.DateType
        return t.unit == AC.DAY ? Dates.Date(Dates.UTD(Int64(x) + _EPOCH_DAYS)) :
               Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH))
    elseif t isa AC.TimestampType
        zone = _timestampzone(t)
        T =
            zone === nothing ? Durations.Timestamp{_timestampperiod(t.unit)} :
            _zonedtimestamptype(t.unit, zone)
        return reinterpret(T, Int64(x))
    elseif t isa AC.TimeType
        return _publictime(t.unit, x)
    elseif t isa AC.DurationType
        P =
            t.unit == AC.SECOND ? Dates.Second :
            t.unit == AC.MILLISECOND ? Dates.Millisecond :
            t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
        return P(Int64(x))
    elseif t isa AC.IntervalType
        return t.unit == AC.YEAR_MONTH ? Durations.Duration(x, 0, 0) :
               t.unit == AC.DAY_TIME ?
               Durations.Duration(0, x.days, Int64(x.millis) * 1_000_000) :
               Durations.Duration(x.months, x.days, x.nanos)
    elseif t isa AC.DecimalType && _shareddecimal(t)
        return _shareddecimalscalar(
            _decimalhost(t),
            _decimalstorage(t),
            Int(t.precision),
            x,
        )
    end
    return x
end

# Whole-column conversion, FIELD-aware: transparent wrappers (run-end
# encoding via the values child field, dictionary via the value field) walk
# to the leaf whose descriptor governs every element of the flat column the
# typed path produced. Composite roots fall to the no-op: their rows were
# already built in the public domain by `_publicvalue`.
function _postconvertfield(f::AC.Field, col, budget=nothing)
    t = f.type
    if t isa AC.RunEndEncodedType && length(f.children) == 2
        return _postconvertfield(f.children[2], col, budget)
    end
    t isa AC.DictionaryType &&
        return _postconvertfield(AC.dictvaluefield(f, t), col, budget)
    return _postconvert(t, col, budget)
end

# The facade's dynamic row builder: Core's `_value` shapes — struct rows as
# `Vector{Pair{String,Any}}`, list rows as `Vector{Any}`, map rows as
# `Vector{Pair{Any,Any}}` — with facade-converted leaves. `convertleaf` is
# false at a column root: a root scalar (or a transparent-wrapper chain to
# one) stays in storage for the one-pass whole-column conversion above and
# for storage-domain scan predicates. It turns true when the walk enters a
# composite container, where each leaf value lands inside an `Any`-shaped
# row and must convert AS it is read — a built union row cannot recover its
# branch afterward, so leaf conversion cannot run as a post-pass.
function _publicvalue(f::AC.Field, d::AC.ArrayData, i::Int64, budget, convertleaf::Bool)
    t = f.type
    if t isa AC.DictionaryType
        AC.isvalid_at(d, i) || return missing
        w = AC.primwidth(t.indextype)
        index =
            AC._load_int(AC.rolebuffer(d, AC.DATA), t.indextype, AC._slotbyteoff(d, i, w))
        pool = d.dictionary
        pool === nothing && throw(AC.ValidationError("dictionary array has no value pool"))
        return _publicvalue(
            AC.dictvaluefield(f, t),
            pool,
            AC.checked_add(Int64(index), Int64(1)),
            budget,
            convertleaf,
        )
    elseif t isa AC.RunEndEncodedType && length(f.children) == 2
        return _publicvalue(
            f.children[2],
            d.children[2],
            AC._ree_runindex(d, i),
            budget,
            convertleaf,
        )
    elseif t isa AC.UnionType
        childfield, childdata, childindex = AC._union_child(f, d, i)
        return _publicvalue(childfield, childdata, childindex, budget, true)
    elseif t isa AC.ListType
        AC.isvalid_at(d, i) || return missing
        lo, hi = AC._offsets_at(d, i, AC.layoutspec(t).offsetwidth == 8)
        childfield, childdata = f.children[1], d.children[1]
        _chargevector!(budget, Any, hi - lo, "facade public list row")
        out = Vector{Any}(undef, Int(hi - lo))
        for k = 1:length(out)
            out[k] = _publicvalue(
                childfield,
                childdata,
                AC.checked_add(lo, Int64(k)),
                budget,
                true,
            )
        end
        return out
    elseif t isa AC.ListViewType
        AC.isvalid_at(d, i) || return missing
        off, size = AC._listview_range(t, d, i)
        childfield, childdata = f.children[1], d.children[1]
        _chargevector!(budget, Any, size, "facade public list-view row")
        out = Vector{Any}(undef, Int(size))
        for k = 1:length(out)
            out[k] = _publicvalue(
                childfield,
                childdata,
                AC.checked_add(off, Int64(k)),
                budget,
                true,
            )
        end
        return out
    elseif t isa AC.FixedSizeListType
        AC.isvalid_at(d, i) || return missing
        childfield, childdata = f.children[1], d.children[1]
        base = AC.checked_mul(AC._slotindex0(d, i), Int64(t.listsize))
        _chargevector!(budget, Any, t.listsize, "facade public fixed-list row")
        out = Vector{Any}(undef, t.listsize)
        for k = 1:(t.listsize)
            out[k] = _publicvalue(
                childfield,
                childdata,
                AC.checked_add(base, Int64(k)),
                budget,
                true,
            )
        end
        return out
    elseif t isa AC.StructType
        AC.isvalid_at(d, i) || return missing
        childindex = AC.checked_add(d.offset, i)
        _chargevector!(
            budget,
            Pair{String,Any},
            length(f.children),
            "facade public struct row",
        )
        out = Vector{Pair{String,Any}}(undef, length(f.children))
        for k in eachindex(f.children)
            out[k] =
                f.children[k].name =>
                    _publicvalue(f.children[k], d.children[k], childindex, budget, true)
        end
        return out
    elseif t isa AC.MapType
        AC.isvalid_at(d, i) || return missing
        lo, hi = AC._offsets_at(d, i, false)
        entriesfield, entriesdata = f.children[1], d.children[1]
        keyfield, valuefield = entriesfield.children
        keydata, valuedata = entriesdata.children
        _chargevector!(budget, Pair{Any,Any}, hi - lo, "facade public map row")
        out = Vector{Pair{Any,Any}}(undef, Int(hi - lo))
        for k = 1:length(out)
            entryindex = AC.checked_add(entriesdata.offset, AC.checked_add(lo, Int64(k)))
            out[k] = Pair{Any,Any}(
                _publicvalue(keyfield, keydata, entryindex, budget, true),
                _publicvalue(valuefield, valuedata, entryindex, budget, true),
            )
        end
        return out
    end
    # A null slot must not run the budgeted extraction: its transient
    # charging machinery would allocate per missing value with nothing to
    # reserve it against. The Null layout has no validity buffer (asking
    # for it throws) and every slot is missing.
    t isa AC.NullType && return missing
    AC.isvalid_at(d, i) || return missing
    x = budget === nothing ? AC.getvalue(f, d, i) : AC.getvalue(f, d, i, budget)
    (convertleaf && x !== missing && _isconvertibleleaf(t)) || return x
    # The converted value is one more boxed object beside the storage value
    # the budgeted getvalue charged, plus transient conversion temporaries;
    # the measured worst case (shared Decimal inside a union row) leaves
    # about 96 bytes of boxes and charging machinery per converted leaf.
    _chargeobject!(budget, 96, "facade converted leaf")
    return _publicleafscalar(t, x)
end

"One dynamic-path column: public rows, storage roots (see `_publicvalue`)."
function _publicdynamiccolumn(f::AC.Field, d::AC.ArrayData, budget=nothing)
    _chargevector!(budget, Any, d.len, "facade dynamic column")
    return Any[_publicvalue(f, d, Int64(i), budget, false) for i = 1:(d.len)]
end
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
    zone = _timestampzone(t)
    # A zone-declared column materializes as `ZonedTimestamp` and a
    # zone-naive one as `Timestamp`, both at every unit: the stored value IS
    # the instant count, so conversion is a per-element reinterpret and no
    # value can wrap or truncate.
    zone === nothing ||
        return _instantcolumn(_zonedtimestamptype(t.unit, zone), col, budget)
    return _instantcolumn(Durations.Timestamp{_timestampperiod(t.unit)}, col, budget)
end
function _postconvert(t::AC.TimeType, col, budget=nothing)
    unit = t.unit
    return _mapcol(Dates.Time, x -> _publictime(unit, x), col, budget)
end
function _postconvert(t::AC.DurationType, col, budget=nothing)
    P =
        t.unit == AC.SECOND ? Dates.Second :
        t.unit == AC.MILLISECOND ? Dates.Millisecond :
        t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    return _mapcol(P, x -> P(Int64(x)), col, budget)
end

# The public element type of the SCALAR layouts the facade converts (Dates)
# or passes through; `_declaredbasetype` completes it for every layout and
# `_declaredeltype` is the Field-aware rule the facade materializes with.
function _facadebasetype(t::AC.ArrowType)
    t isa AC.DateType && return t.unit == AC.DAY ? Dates.Date : Dates.DateTime
    if t isa AC.TimestampType
        zone = _timestampzone(t)
        zone === nothing || return _zonedtimestamptype(t.unit, zone)
        return Durations.Timestamp{_timestampperiod(t.unit)}
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

"""
The public column for `converted`, a column already in the public domain.
`owned=true` means no other object references `converted` (the facade just
materialized or joined it), so a vector that already has the public element
type is returned as is instead of copied.
"""
function _publiccolumn(f::AC.Field, converted, budget=nothing; owned::Bool=false)
    T = _publictype(f, converted)
    T === Any && return _narrowcopy(converted, budget, "facade public column")
    owned && converted isa Vector{T} && return converted
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
    # A registered extension over a decimal restores from raw coefficients.
    (_isbulkdecimal(f) && !_hasarrowtypesextension(f)) &&
        return _decimalcolumn(f, d, budget)
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

# The public-domain sibling of `_storagebatchcolumn`: identical typed path
# (the flat storage column converts once, whole-column, in `_facadefromraw`),
# but the dynamic path builds PUBLIC rows — composite leaf conversion cannot
# run as a post-pass (see `_publicvalue`).
function _publicbatchcolumn(f::AC.Field, d::AC.ArrayData, budget=nothing)
    _isbulkdecimal(f) && return _decimalcolumn(f, d, budget)
    T = _storageelementclaim(f)
    T === Any && return _publicdynamiccolumn(f, d, budget)
    (Missing <: T || !_hasnulls(f, d)) || (T = Union{Missing,T})
    return budget === nothing ? AC.materialize(T, f, d) : AC.materialize(T, f, d, budget)
end

"Materialize for the facade, retaining an ArrowTypes Union route when needed."
function _batchcolumn(f::AC.Field, d::AC.ArrayData, plan::_ArrowTypesRoutePlan)
    if _hasarrowtypesextension(f, plan)
        # Ext-bearing columns WITH structure walk the routed builder: it
        # keeps union branch provenance and splits labeled (raw, for
        # restoration) from label-free (public) subtrees. A flat labeled
        # leaf column stays on the typed storage path; restoration converts
        # the whole column at once.
        routed = _arrowtypesroutedcolumn(f, d, plan; force=(!isempty(f.children)))
        routed === nothing || return routed
        return _storagebatchcolumn(f, d, plan.budget)
    end
    return _publicbatchcolumn(f, d, plan.budget)
end

function _batchcolumn(f::AC.Field, d::AC.ArrayData, arrowtypes::_ArrowTypesContext)
    if _hasarrowtypesextension(f, arrowtypes)
        routed = _arrowtypesroutedcolumn(
            f,
            d,
            _arrowtypesrouteplan!(arrowtypes);
            force=(!isempty(f.children)),
        )
        routed === nothing || return routed
        return _storagebatchcolumn(f, d, arrowtypes.budget)
    end
    return _publicbatchcolumn(f, d, arrowtypes.budget)
end

function _batchcolumn(f::AC.Field, d::AC.ArrayData, budget=nothing)
    _hasarrowtypesextension(f) || return _publicbatchcolumn(f, d, budget)
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
    # `col` is a fresh batch materialization or the join just built.
    return _facadefromraw(f, col, arrowtypes; owned=true)
end

function _facadesinglecolumn(f::AC.Field, d::AC.ArrayData, arrowtypes::_ArrowTypesContext)
    _chargevector!(arrowtypes.budget, AbstractVector, 1, "batch column parts")
    return _facadecolumn(f, AbstractVector[_batchcolumn(f, d, arrowtypes)], arrowtypes)
end

_columndatas(batches, i::Int) = AC.ArrayData[b.columns[i] for b in batches]

"""
Table columns `idxs` of `fields` over decoded `batches`, built on at most
`ntasks` tasks: a view column is one piece of work, a string or binary
column is cut into chunks of rows, and the materialized (nested, union,
extension) columns build on the calling task afterwards.
"""
# With `lazy`, string and binary columns are `_PendingStrings` that build when a
# table first hands them out.
function _buildcolumns(
    fields,
    batches,
    idxs,
    arrowtypes,
    views::_ViewContext,
    ntasks::Int;
    lazy::Bool=false,
)
    m = length(idxs)
    _chargevector!(views.budget, AbstractVector, m, "table columns")
    cols = Vector{AbstractVector}(undef, m)
    builds = Vector{Any}(nothing, m)
    jobs = Tuple{Int,Int}[]   # (output column, string chunk or 0 for the whole column)
    rows = ntasks > 1 ? _STRING_CHUNK_ROWS : typemax(Int)
    for (p, i) in enumerate(idxs)
        f = fields[i]
        _viewable(f) || continue
        if _isstringlayout(f.type)
            datas = _columndatas(batches, i)
            T = _vieweltype(f, datas, views)
            b = _stringbuild(T, datas, views.budget, rows)
            if lazy
                cols[p] = _PendingStrings{T}(b, ntasks)
                continue
            end
            builds[p] = _allocate!(b)
            append!(jobs, (p, k) for k in eachindex(b.chunks))
        else
            push!(jobs, (p, 0))
        end
    end
    _foreach(length(jobs), ntasks) do q, _
        p, k = jobs[q]
        if k == 0
            cols[p] = _viewcolumn(fields[idxs[p]], _columndatas(batches, idxs[p]), views)
        else
            _buildchunk!(builds[p], k)
        end
        return nothing
    end
    for (p, i) in enumerate(idxs)
        b = builds[p]
        if b !== nothing
            cols[p] = _finishbuild(b)
        elseif !isassigned(cols, p)
            cols[p] = _facadebatchcolumn(fields[i], batches, i, arrowtypes)
        end
    end
    return cols
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
    arrowtypes::_ArrowTypesContext=_ArrowTypesContext();
    owned::Bool=false,
) =
    _hasarrowtypesextension(f, arrowtypes) ? _arrowtypescolumn(f, col, arrowtypes) :
    _publiccolumn(
        f,
        _postconvertfield(f, col, arrowtypes.budget),
        arrowtypes.budget;
        owned=owned,
    )

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
_configuredlimits(source::Union{ArrowFile,_RangedFile}) = source.limits
_configuredlimits(source::IPCStream) = source.limits
_configuredlimits(source::_StreamReader) = source.limits
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
# layer owns the region and must unmap it if validation or parsing fails.
# This is observable on Windows, where an unreleased mapping prevents the
# source file from being deleted.
function _readmappedfile(
    region::AC.OwnerRegion,
    limits::Limits,
    budget::Union{Nothing,AllocationBudget},
)
    try
        budget === nothing && return readfile(region; limits=limits)
        return _readfile(region, limits, _boundbudget!(budget, limits, region.len))
    catch
        # No column exists yet, so the mapping can go now.
        AC.release!(region)
        AC._unmap!(region)
        rethrow()
    end
end

# A memory-mapped stream-format file decodes in place, like a byte vector.
function _readmappedstream(
    region::AC.OwnerRegion,
    limits::Limits,
    budget::Union{Nothing,AllocationBudget},
    ntasks::Int,
)
    try
        return _openbytes(
            region.root::Vector{UInt8};
            limits=limits,
            budget=budget,
            ntasks=ntasks,
        )
    catch
        # No column exists yet, so the mapping can go now.
        AC._unmap!(region)
        rethrow()
    end
end

function _openbytes(
    bytes::Vector{UInt8};
    limits::Limits=Limits(),
    budget::Union{Nothing,AllocationBudget}=nothing,
    ntasks::Int=1,
)
    if budget === nothing
        return _isfilebytes(bytes) ? readfile(bytes; limits=limits) :
               readstream(bytes; limits=limits)
    end
    # The read's allowance scales with the input it turned out to have.
    _boundbudget!(budget, limits, length(bytes))
    return _isfilebytes(bytes) ? _readfile(heapregion(bytes), limits, budget) :
           _readstream(bytes, limits, budget, ntasks)
end

function _opensource(
    path::AbstractString;
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=Limits(),
    budget::Union{Nothing,AllocationBudget}=nothing,
    ntasks::Int=1,
)
    return open(path, "r") do io
        magic = Base.read(io, 6)
        seekstart(io)
        reported = stat(io).size
        (reported isa Integer && 0 <= reported <= typemax(Int)) ||
            throw(AC.ValidationError("source file length is not addressable"))
        size = Int(reported)
        if mmap && size > 0
            # Probe and map the same open file. A replacement of `path`
            # between those steps cannot redirect the read to another inode.
            # Columns read the mapping in place, so `release!` must not unmap
            # it: the mapping lives until nothing reaches it.
            region = AC._mmapregion(io, path; eager=false)
            magic == _FILE_MAGIC && return _readmappedfile(region, limits, budget)
            return _readmappedstream(region, limits, budget, ntasks)
        end
        _chargevector!(budget, UInt8, size, "source file read")
        bytes = Base.read(io, size)
        length(bytes) == size || throw(
            AC.ValidationError(
                "source file changed while reading: got $(length(bytes)) of $size bytes",
            ),
        )
        return _openbytes(bytes; limits=limits, budget=budget, ntasks=ntasks)
    end
end
function _opensource(
    io::IO;
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=Limits(),
    budget=nothing,
    ntasks::Int=1,
)
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
    return _openbytes(bytes; limits=limits, budget=budget, ntasks=ntasks)
end
_opensource(
    bytes::Vector{UInt8};
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=Limits(),
    budget=nothing,
    ntasks::Int=1,
) = _openbytes(bytes; limits=limits, budget=budget, ntasks=ntasks)
_opensource(
    src::Union{IPCStream,ArrowFile};
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=_configuredlimits(src),
    budget=nothing,
    ntasks::Int=1,
) = src
# A byte-range source is read whole: iteration is sequential over every
# batch, so there is nothing for range planning to skip.
function _opensource(
    src::AbstractArrowSource;
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=Limits(),
    budget=nothing,
    ntasks::Int=1,
)
    return _opensource(
        SourceFile(src; limits=limits);
        mmap=mmap,
        limits=limits,
        budget=budget,
        ntasks=ntasks,
    )
end
function _opensource(
    sf::SourceFile;
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=sf.limits,
    budget=nothing,
    ntasks::Int=1,
)
    return _openbytes(_wholeobject(sf, budget); limits=limits, budget=budget, ntasks=ntasks)
end

# Arrow 2.x also accepted a vector of sources and positional byte windows;
# name the supported source kinds instead of failing with a MethodError.
function _opensource(source; kwargs...)
    throw(
        ArgumentError(
            "Arrow reads one IPC source: a file path, an IO, a Vector{UInt8}, or " *
            "an Arrow.AbstractArrowSource, not $(typeof(source)); read several " *
            "sources separately and combine their tables",
        ),
    )
end

# --- Table construction ------------------------------------------------------

# A fresh read budget. An opened file handle's input is its region; other
# sources are bounded by their input once it is read (`_openbytes`,
# `_readmappedfile`).
# A source whose size is not known yet starts at the cap; opening it bounds
# the budget to the input's allowance (`_boundbudget!`).
_sourcebudget(source, limits::Limits) =
    source isa ArrowFile ? _readbudget(limits, source.region.len) :
    AllocationBudget(
        limits.max_total_allocated_bytes;
        ratio=limits.max_allocation_ratio,
        cap=limits.max_total_allocated_bytes,
    )

# The scan the read keywords ask for, or `nothing` for a whole read.
function _scanrequest(select, drop, filter, limit, scan)
    sugar =
        select !== nothing || drop !== nothing || filter !== nothing || limit !== nothing
    if scan !== nothing
        sugar && throw(
            ArgumentError(
                "pass `scan` or the select/drop/filter/limit keywords, not both; " *
                "a Tables.Scan can express all of them",
            ),
        )
        return scan
    end
    sugar || return nothing
    (select === nothing || drop === nothing) ||
        throw(ArgumentError("pass `select` or `drop`, not both"))
    columns = drop === nothing ? something(select, Tables.All()) : Tables.Not(drop)
    return Tables.Scan(; select=columns, filter=filter, limit=limit)
end

function Table(
    source;
    select=nothing,
    drop=nothing,
    filter=nothing,
    limit=nothing,
    scan::Union{Nothing,Tables.Scan}=nothing,
    mmap::Bool=_MMAP_DEFAULT,
    stringtype::Type=DataStrings.DataString,
    limits::Limits=_defaultreaderlimits(source),
    ntasks::Integer=Threads.nthreads(),
)
    scan = _scanrequest(select, drop, filter, limit, scan)
    readlimits = _readerlimits(source, limits)
    _checkstringtype(stringtype)
    ntasks = _checkntasks(ntasks)
    if source isa AbstractArrowSource || source isa SourceFile
        sf = source isa SourceFile ? source : SourceFile(source; limits=readlimits)
        _requirelittleendian()
        budget = _readbudget(readlimits, sf.len)
        if scan === nothing
            opened = _openbytes(
                _wholeobject(sf, budget);
                limits=readlimits,
                budget=budget,
                ntasks=ntasks,
            )
            return _tablefrom(opened, nothing, budget; stringtype=stringtype, ntasks=ntasks)
        end
        # Range planning pays off only for a pushable scan over a file-format
        # object with columns. With a scan the schema comes up front from
        # the cached tail: literal lowering, exactly-once output conversion,
        # and DataAPI metadata all need it.
        if _isfilesource(sf, budget)
            ft = _rangedfooter(sf, budget)
            plan = _ScanPlan(scan, ft.fields, budget)
            if !isempty(ft.fields) && plan.storage !== nothing
                return _applyfacadescan(sf, plan, ft, budget; stringtype=stringtype)
            end
            # A public-domain fallback still reuses the scan plan compiled
            # from the Footer schema after the object is read in full. Keep
            # the same budget through whole-object assembly, parsing, and
            # lazy batch materialization.
            opened = _openbytes(
                _wholeobject(sf, budget);
                limits=readlimits,
                budget=budget,
                ntasks=ntasks,
            )
            return _tablefrom(opened, plan, budget; stringtype=stringtype, ntasks=ntasks)
        end
        # A stream-format object has no Footer to plan from. Read it whole,
        # without resetting the budget used by the format probe, then execute
        # the scan over its decoded columns.
        opened = _openbytes(
            _wholeobject(sf, budget);
            limits=readlimits,
            budget=budget,
            ntasks=ntasks,
        )
        return _tablefrom(opened, scan, budget; stringtype=stringtype, ntasks=ntasks)
    end
    budget = source isa IPCStream ? source.budget : _sourcebudget(source, readlimits)
    opened = _opensource(source; mmap=mmap, limits=readlimits, budget=budget, ntasks=ntasks)
    if Sys.isunix() &&
       mmap &&
       scan === nothing &&
       source isa AbstractString &&
       opened isa ArrowFile
        # A whole-table read touches every page of the mapping. One
        # madvise maps them all at once (as Arrow C++ does for mapped
        # reads), several times cheaper on macOS than a fault per page.
        try
            Mmap.madvise!(opened.region.root::Vector{UInt8}, Mmap.MADV_WILLNEED)
        catch e
            e isa SystemError || rethrow()
        end
    end
    return _tablefrom(opened, scan, budget; stringtype=stringtype, ntasks=ntasks)
end

# Windows cannot delete a file while it is mapped, and the facade unmaps
# only when the table and its columns are garbage collected, so paths there
# read into memory unless `mmap=true` is requested.
const _MMAP_DEFAULT = !Sys.iswindows()

"""
    Arrow.read(source, sink; kwargs...)

Read `source` as [`Arrow.Table`](@ref) does, with the same keywords, and pass
the result to `sink` with owned, mutable columns, copied exactly once:
`Arrow.read(path, DataFrame)` returns a `DataFrame` whose columns are
`Vector`s (`PooledArray`s for dictionary-encoded columns).
"""
function read(source, sink; kwargs...)
    t = Table(source; kwargs...)
    cols = AbstractVector[_ownedcolumn(_force(c)) for c in getfield(t, :columns)]
    owned = Table(
        getfield(t, :names),
        cols,
        getfield(t, :lookup),
        getfield(t, :schema),
        getfield(t, :regions),
        getfield(t, :nrows),
        getfield(t, :retainedpools),
    )
    return sink(Tables.CopiedColumns(owned))
end

function read(source; kwargs...)
    throw(
        ArgumentError(
            "Arrow.read needs a sink, as in Arrow.read(source, DataFrame); " *
            "Arrow.Table(source) reads without copying",
        ),
    )
end

"""
    Arrow.col(name)

A column reference for a read `filter`, the same as `Tables.col`:
`Arrow.Table(path; filter=Arrow.col(:x) > 3)`. Combine comparisons with `&`,
`|`, and `!`; see also [`Arrow.colcmp`](@ref), [`Arrow.colin`](@ref), and
[`Arrow.isnull`](@ref).
"""
const col = Tables.col

"""
    Arrow.colcmp(op, column, value)

A filter comparison for values the operator forms of [`Arrow.col`](@ref) do
not cover (dates, timestamps), the same as `Tables.colcmp`:
`Arrow.colcmp(>=, Arrow.col(:day), Date(2025, 1, 1))`.
"""
const colcmp = Tables.colcmp

"""
    Arrow.colin(column, values)

A filter that keeps rows whose value is in `values`, the same as `Tables.colin`.
"""
const colin = Tables.colin

"""
    Arrow.isnull(column)

A filter that keeps rows whose value is missing, the same as `Tables.isnull`.
"""
const isnull = Tables.isnull

# Views copy into owned memory, a column of several batches into one vector;
# columns this read materialized are already owned by nobody else.
_ownedcolumn(c::Union{ArrowVector,ChainedVector}) = copy(c)
_ownedcolumn(c) = c

# A Table from an opened IPC source: the whole thing, or a scan pushed
# where the format can prove it and evaluated over the rest.
function _tablefrom(
    src::Union{IPCStream,ArrowFile},
    scan::Union{Nothing,Tables.Scan},
    budget::Union{Nothing,AllocationBudget}=nothing;
    stringtype::Type=DataStrings.DataString,
    ntasks::Int=1,
)
    regions = _sourceregions(src, budget)
    fields = _corefields(src, budget)
    scan === nothing && return _materialize_table(
        src,
        regions,
        budget;
        stringtype=stringtype,
        ntasks=ntasks,
    )
    plan = _ScanPlan(scan, fields, budget)
    return _tablefrom(
        src,
        plan,
        regions,
        fields,
        budget;
        stringtype=stringtype,
        ntasks=ntasks,
    )
end

function _tablefrom(
    src::Union{IPCStream,ArrowFile},
    plan::_ScanPlan,
    budget::Union{Nothing,AllocationBudget}=nothing;
    stringtype::Type=DataStrings.DataString,
    ntasks::Int=1,
)
    return _tablefrom(
        src,
        plan,
        _sourceregions(src, budget),
        _corefields(src, budget),
        budget;
        stringtype=stringtype,
        ntasks=ntasks,
    )
end

function _tablefrom(
    src,
    plan::_ScanPlan,
    regions,
    fields,
    budget;
    stringtype::Type=DataStrings.DataString,
    ntasks::Int=1,
)
    # Zero-field sources carry their row count on the Table itself; the raw
    # scan path would lose it inside an empty NamedTuple.
    isempty(fields) && return _publicscan(
        _materialize_table(src, regions, budget; stringtype=stringtype, ntasks=ntasks),
        _tableschema(src),
        fields,
        plan,
        regions,
        budget,
    )
    plan.storage !== nothing &&
        return _applyfacadescan(src, plan, budget; stringtype=stringtype, ntasks=ntasks)
    # A scan with an unrepresentable storage-domain literal evaluates its
    # public-domain plan over the fully converted table.
    return _publicscan(
        _materialize_table(src, regions, budget; stringtype=stringtype, ntasks=ntasks),
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
_tableschema(f::Union{ArrowFile,_RangedFile}) = f.schema
_tableschema(r::_StreamReader) = r.st.schema

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

"""
The retained pool of a top-level dictionary column whose batches all share
`dictionary`; `nothing` (no batch) retains an empty pool.
"""
function _retainedsnapshotpool(
    f::AC.Field,
    dictionary::Union{Nothing,AC.ArrayData},
    arrowtypes::_ArrowTypesContext,
)
    # DictionaryType stores the value descriptor on the index Field. Reapply
    # that Field's extension metadata while materializing pool snapshots so a
    # registered dictionary value and its nested children lift exactly once.
    vf = _arrowtypesdictvaluefield(
        arrowtypes,
        f,
        f.type::AC.DictionaryType;
        retainmetadata=true,
    )
    dictionary === nothing && return _facadecolumn(vf, Any[], arrowtypes)
    return _facadesinglecolumn(vf, dictionary, arrowtypes)
end

"Retain one top-level dictionary's category order across pool snapshots."
function _retaineddictpool(f::AC.Field, batches, i::Int, arrowtypes::_ArrowTypesContext)
    t = f.type
    t isa AC.DictionaryType || return nothing
    isempty(batches) && return _retainedsnapshotpool(f, nothing, arrowtypes)
    firstdictionary = batches[1].columns[i].dictionary::AC.ArrayData
    if all(batch -> batch.columns[i].dictionary === firstdictionary, batches)
        return _retainedsnapshotpool(f, firstdictionary, arrowtypes)
    end
    vf = _arrowtypesdictvaluefield(arrowtypes, f, t; retainmetadata=true)
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

function _materialize_table(
    src::IPCStream,
    regions,
    budget=nothing;
    stringtype::Type=DataStrings.DataString,
    ntasks::Int=1,
)
    budget === nothing && (budget = src.budget)
    names = _fieldnamesymbols(src.schema.fields, budget)
    arrowtypes = _ArrowTypesContext(budget=budget)
    views = _ViewContext(stringtype, budget)
    fields = src.corefields
    cols = _buildcolumns(
        fields,
        src.batches,
        1:length(fields),
        arrowtypes,
        views,
        ntasks;
        lazy=true,
    )
    nrows = sum(Int(b.nrows) for b in src.batches; init=0)
    pools = _retaineddictpools(src.corefields, src.batches, arrowtypes)
    return _table(names, cols, src.schema, regions, nrows, pools, budget)
end

function _materialize_table(
    src::ArrowFile,
    regions,
    budget=nothing;
    stringtype::Type=DataStrings.DataString,
    ntasks::Int=1,
)
    budget === nothing && (budget = _readbudget(src.limits, src.region.len))
    names = _fieldnamesymbols(src.schema.fields, budget)
    arrowtypes = _ArrowTypesContext(budget=budget)
    views = _ViewContext(stringtype, budget)
    batches = _filerecordbatches(src, 1:length(src), budget, ntasks)
    cols = _buildcolumns(
        src.fields,
        batches,
        1:length(src.fields),
        arrowtypes,
        views,
        ntasks;
        lazy=true,
    )
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
#     With `converted=true` every convertible leaf declares its PUBLIC type
#     at every depth — wrappers and union children propagate the flag — and
#     the dynamic row builder (`_publicvalue`) delivers exactly that.
#     `converted=false` is the STORAGE-claim domain the typed Core path
#     materializes before the whole-column conversion.
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
        return _declaredeltype(f.children[2], converted)
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
        D = _declaredeltype(f.children[1], converted)
        for k = 2:length(f.children)
            D = Base.promote_typejoin(D, _declaredeltype(f.children[k], converted))
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
    t isa AC.TimestampType
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
    Arrow.Stream(source; select=nothing, drop=nothing, filter=nothing, limit=nothing,
                 scan=nothing, mmap, stringtype=DataString, limits, ntasks)

Iterate an IPC source (a file path, an `IO`, a `Vector{UInt8}`, or an
[`Arrow.AbstractArrowSource`](@ref)) one record batch at a time; each iteration yields an [`Arrow.Table`](@ref) for that batch,
with columns as in `Arrow.Table`. `mmap`, `stringtype`, and `ntasks` have the
same meaning and defaults as for [`Arrow.Table`](@ref); each batch decodes and
builds its columns on at most `ntasks` tasks. `select`, `drop`, `filter`, and
`scan` apply to every batch; `limit` and a scan's `offset` count rows across
the batches, and the iteration ends once `limit` rows have been yielded (such a
Stream can be iterated but not indexed). Satisfies `Tables.partitions` (each
batch is one partition), so partition-aware sinks — including `Arrow.write`,
which writes one record batch per partition — see the source batch structure.

`limits` has the same policy as [`Arrow.Table`](@ref). It configures a raw
source. An internal reader handle keeps the policy that verified it.

Batches decode as they are iterated. A stream-format source is read one
message at a time: from an `IO` (a file, a pipe, a socket) each batch's bytes
are read when the iteration reaches them, and over a byte vector or a
memory-mapped path the batches are framed in place. A file-format source
decodes each batch from the file (memory-mapped for a path; an `IO` is read
to its end first, since the format keeps its index at the end). A consumer
that processes and drops batches holds one batch of columns at a time, plus
the dictionaries, so a source larger than memory streams with the defaults;
each batch decodes under its own allocation allowance (see
[`Arrow.Limits`](@ref)), sized from its own bytes.

An `AbstractArrowSource` is fetched as it is iterated: a file-format object
reads its footer and dictionaries, then each record batch's bytes; a
stream-format object reads one message at a time.

A file-format source has `length` and `stream[i]`. A stream-format source
iterates in order; one over an `IO` can be iterated once (the bytes are
consumed), and its `length` is unknown until it has been read, as for a
stream-format `AbstractArrowSource`.

`Tables.schema(stream)` and `Tables.columnnames(stream)` give the columns of
each batch, `DataAPI.metadata` and `DataAPI.colmetadata` the schema's
key-value metadata, and `DataAPI.nrow(stream)` the row count of a
file-format source (from batch metadata alone), all without reading data.

Every yielded `Table` shares the source lifetime: releasing a batch releases its
parent `Stream` (see [`Arrow.release!`](@ref)).
"""
struct Stream
    src::Union{IPCStream,ArrowFile,_RangedFile,_StreamReader}
    regions::Vector{AC.OwnerRegion}
    budget::AllocationBudget
    closed::AC.ReleaseCell
    arrowtypes::_ArrowTypesContext
    views::_ViewContext
    arrowtypeslock::ReentrantLock
    ntasks::Int
    scan::Union{Nothing,Tables.Scan}   # applied to each batch; its window spans them
end

function Stream(
    source;
    select=nothing,
    drop=nothing,
    filter=nothing,
    limit=nothing,
    scan::Union{Nothing,Tables.Scan}=nothing,
    mmap::Bool=_MMAP_DEFAULT,
    stringtype::Type=DataStrings.DataString,
    limits::Limits=_defaultreaderlimits(source),
    ntasks::Integer=Threads.nthreads(),
)
    scan = _scanrequest(select, drop, filter, limit, scan)
    readlimits = _readerlimits(source, limits)
    _checkstringtype(stringtype)
    ntasks = _checkntasks(ntasks)
    budget = source isa IPCStream ? source.budget : _sourcebudget(source, readlimits)
    src = _openstream(source; mmap=mmap, limits=readlimits, budget=budget)
    regions = _sourceregions(src, budget)
    # The stream and every yielded partition share one lifetime authority.
    # Releasing either closes the same source region and makes later
    # iteration fail consistently for borrowed and decompressed columns.
    # `_sourceregions` returns exactly one region for both source kinds, so
    # `only` cannot throw; a second region would need to share this cell,
    # not add one — the Stream has a single close authority.
    # A scan that does not fit the schema fails here, not at the first batch.
    scan === nothing || _ScanPlan(scan, _batchfields(src), _readbudget(readlimits, 0))
    return Stream(
        src,
        regions,
        budget,
        only(regions).cell,
        _ArrowTypesContext(budget=budget),
        _ViewContext(stringtype, budget),
        ReentrantLock(),
        ntasks,
        scan,
    )
end

function AC.release!(s::Stream)
    AC.release!(getfield(s, :closed))
    foreach(AC.release!, getfield(s, :regions))
    return nothing
end

# A stream-format source reads incrementally: bytes and memory-mapped paths
# frame their messages in place, an `IO` is read one message at a time. A
# file-format source decodes its batches lazily from the whole file.
_openstream(source; kwargs...) = _opensource(source; kwargs...)
function _openstream(
    bytes::Vector{UInt8};
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=Limits(),
    budget=nothing,
)
    _isfilebytes(bytes) && return _openbytes(bytes; limits=limits, budget=budget)
    return _StreamReader(_RegionMessages(heapregion(bytes)), limits)
end
function _openstream(
    path::AbstractString;
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=Limits(),
    budget=nothing,
)
    magic = open(io -> Base.read(io, 6), path, "r")
    magic == _FILE_MAGIC &&
        return _opensource(path; mmap=mmap, limits=limits, budget=budget)
    mmap || return _openstream(Base.read(path); limits=limits)
    return open(path, "r") do io
        eof(io) && return _openstream(UInt8[]; limits=limits)
        _StreamReader(_RegionMessages(AC._mmapregion(io, path; eager=false)), limits)
    end
end
function _openstream(
    io::IO;
    mmap::Bool=_MMAP_DEFAULT,
    limits::Limits=Limits(),
    budget=nothing,
)
    head = Base.read(io, 8)
    if length(head) >= 6 && view(head, 1:6) == _FILE_MAGIC
        # The file format needs its footer: read the rest.
        return _openbytes(vcat(head, Base.read(io)); limits=limits, budget=budget)
    end
    return _StreamReader(_IOMessages(io, head, Int64(0)), limits)
end
# A byte-range source is fetched as it is iterated: a file-format object one
# record batch at a time (its footer and dictionaries first), a
# stream-format object one message at a time.
function _openstream(
    src::Union{AbstractArrowSource,SourceFile};
    limits::Limits=Limits(),
    budget=nothing,
    kwargs...,
)
    sf = src isa SourceFile ? src : SourceFile(src; limits=limits)
    _isfilesource(sf, budget) && return _RangedFile(sf, budget)
    return _StreamReader(_IOMessages(_SourceIO(sf), UInt8[], Int64(0)), sf.limits)
end

function _sourceregions(r::_RangedFile, budget=nothing)
    _chargevector!(budget, AC.OwnerRegion, 1, "source owner regions")
    return AC.OwnerRegion[r.region]
end

function _sourceregions(r::_StreamReader, budget=nothing)
    _chargevector!(budget, AC.OwnerRegion, 1, "source owner regions")
    return AC.OwnerRegion[r.region]
end

_nbatches(s::IPCStream) = length(s.batches)
_nbatches(f::Union{ArrowFile,_RangedFile}) = length(f)
_nbatches(r::_StreamReader{_RegionMessages}) = _countrecords(r)
_nbatches(::_StreamReader) = throw(
    ArgumentError(
        "the number of batches of a stream read from an IO is unknown until it is " *
        "read; iterate the Arrow.Stream, or collect it first",
    ),
)
_batch(s::IPCStream, i, budget, ntasks) = s.batches[i]
_batch(f::ArrowFile, i, budget, ntasks) = only(_filerecordbatches(f, (i,), budget, ntasks))
_batch(r::_RangedFile, i, budget, ntasks) = _rangedbatch(r, i, budget, ntasks)
_batchfields(s::IPCStream) = s.corefields
_batchfields(f::Union{ArrowFile,_RangedFile}) = f.fields
_batchfields(r::_StreamReader) = r.st.fields

Base.length(s::Stream) = _nbatches(s.src)
Base.eltype(::Type{Stream}) = Table
Base.IteratorSize(::Type{Stream}) = Base.SizeUnknown()

# Batch `i` of the stream and the allowance it decodes under, or `nothing`
# past the end. A stream-format source returns its batches in order (`i == 1`
# starts over).
function _streambatch(s::Stream, i::Int, ntasks::Int)
    src = s.src
    if src isa _StreamReader
        return @lock src.lock begin
            i == 1 && _restart!(src)
            _nextrecord!(src, ntasks)
        end
    end
    i > _nbatches(src) && return nothing
    budget = _batchbudget(src, i)
    return _batch(src, i, budget, ntasks), budget
end

# The state is the next batch and, for a scan's window, the qualifying rows
# skipped and yielded so far.
function Base.iterate(s::Stream, state::NTuple{3,Int}=(1, 0, 0))
    _checkstream(s)
    i, skipped, yielded = state
    scan = getfield(s, :scan)
    scan === nothing || scan.limit === nothing || yielded < scan.limit || return nothing
    got = _streambatch(s, i, getfield(s, :ntasks))
    got === nothing && return nothing
    b, budget = got
    scan === nothing && return _batchtable(s, b, budget), (i + 1, 0, 0)
    t, skipped, yielded = _scannedbatch(s, b, budget, scan, skipped, yielded)
    return t, (i + 1, skipped, yielded)
end

# Batch `b` under the stream's scan, after `skipped` qualifying rows of its
# offset and `yielded` rows of its limit: the batch's share of the window.
function _scannedbatch(s::Stream, b, budget, scan::Tables.Scan, skipped::Int, yielded::Int)
    src = s.src
    offset = scan.offset - skipped
    limit = scan.limit === nothing ? nothing : offset + scan.limit - yielded
    fields = collect(AC.Field, _batchfields(src))
    plan = _ScanPlan(Tables.Scan(scan; offset=0, limit=limit), fields, budget)
    views = _ViewContext(getfield(s, :views), budget)
    t = _scanbatches(
        _tableschema(src),
        fields,
        AC.RecordBatch[b],
        s.regions,
        plan,
        budget,
        views,
        getfield(s, :ntasks),
    )
    n = getfield(t, :nrows)
    skip = min(offset, n)
    take = scan.limit === nothing ? n - skip : min(scan.limit - yielded, n - skip)
    take == n || (t = _tablerows(t, (skip + 1):(skip + take)))
    return t, skipped + skip, yielded + take
end

# Rows `r` of table `t`.
function _tablerows(t::Table, r::UnitRange{Int})
    cols = AbstractVector[_force(c)[r] for c in getfield(t, :columns)]
    return Table(
        getfield(t, :names),
        cols,
        getfield(t, :lookup),
        getfield(t, :schema),
        getfield(t, :regions),
        length(r),
        getfield(t, :retainedpools),
    )
end

"""
    stream[i] -> Arrow.Table

Record batch `i` of a file-format `Arrow.Stream`, decoded on demand.
"""
function Base.getindex(s::Stream, i::Integer)
    _checkstream(s)
    src = s.src
    src isa _StreamReader && throw(
        ArgumentError(
            "a stream-format source has no random access to its batches; iterate it",
        ),
    )
    scan = getfield(s, :scan)
    scan === nothing ||
        (scan.limit === nothing && scan.offset == 0) ||
        throw(
            ArgumentError(
                "which batches hold a Stream's limited or offset rows is known only " *
                "while iterating; iterate the Stream",
            ),
        )
    1 <= i <= _nbatches(src) || throw(BoundsError(s, i))
    budget = _batchbudget(src, Int(i))
    b = _batch(src, Int(i), budget, getfield(s, :ntasks))
    scan === nothing && return _batchtable(s, b, budget)
    return first(_scannedbatch(s, b, budget, scan, 0, 0))
end

function _checkstream(s::Stream)
    closed = getfield(s, :closed)
    (@atomic :acquire closed.closed) &&
        throw(InvalidStateException("the stream was released", :closed))
    return nothing
end

# The table of one decoded batch of the stream.
function _batchtable(s::Stream, b::AC.RecordBatch, budget::AllocationBudget)
    fields = _batchfields(s.src)
    names = _fieldnamesymbols(fields, budget)
    arrowtypes = getfield(s, :arrowtypes)
    shared = getfield(s, :views)
    ntasks = getfield(s, :ntasks)
    arrowtypeslock = getfield(s, :arrowtypeslock)
    lock(arrowtypeslock)
    try
        # The caches outlive the batch; its allocations charge its own budget.
        arrowtypes.budget = budget
        plan = arrowtypes.routeplan
        plan === nothing || (plan.budget = budget)
        views = _ViewContext(shared, budget)
        cols = _buildcolumns(fields, (b,), 1:length(fields), arrowtypes, views, ntasks)
        pools = _retaineddictpools(fields, (b,), arrowtypes)
        return _table(
            names,
            cols,
            _tableschema(s.src),
            s.regions,
            Int(b.nrows),
            pools,
            budget,
        )
    finally
        unlock(arrowtypeslock)
    end
end

# A batch's allowance comes from its own bytes: the message for a file, the
# decoded buffers for a stream already read whole.
function _batchbudget(f::Union{ArrowFile,_RangedFile}, i::Int)
    _, metalen, bodylen = f.recordblocks[i]
    return _readbudget(_configuredlimits(f), metalen + bodylen)
end
_batchbudget(s::IPCStream, i::Int) = _readbudget(s.limits, _databytes(s.batches[i]))

_databytes(b::AC.RecordBatch) = sum(_databytes, b.columns; init=Int64(0))
_databytes(d::AC.ArrayData) =
    sum(x -> Int64(x.len), d.buffers; init=Int64(0)) +
    sum(_databytes, d.children; init=Int64(0))

Tables.partitions(s::Stream) = s

# A Stream is a sequence of record-batch tables, not one table. Without these
# methods Tables.jl's iterator fallback reads each batch as one row whose
# values are whole columns (so `DataFrame(stream)` would hold vectors).
function _streamisnotatable()
    throw(
        ArgumentError(
            "Arrow.Stream iterates record batches; read the whole table with " *
            "Arrow.Table(source), or iterate the Stream (or Tables.partitions) " *
            "for one batch at a time",
        ),
    )
end
Tables.columns(::Stream) = _streamisnotatable()
Tables.rows(::Stream) = _streamisnotatable()

# A zero-row table of the stream's schema: its column names, element types,
# and metadata, without reading a batch.
function _schematable(s::Stream)
    src = s.src
    fields = _batchfields(src)
    budget = _readbudget(_configuredlimits(src), 0)
    arrowtypes = getfield(s, :arrowtypes)
    arrowtypeslock = getfield(s, :arrowtypeslock)
    lock(arrowtypeslock)
    try
        arrowtypes.budget = budget
        plan = arrowtypes.routeplan
        plan === nothing || (plan.budget = budget)
        views = _ViewContext(getfield(s, :views), budget)
        scan = getfield(s, :scan)
        if scan !== nothing
            fieldvec = collect(AC.Field, fields)
            plan = _ScanPlan(Tables.Scan(scan; offset=0, limit=nothing), fieldvec, budget)
            return _scanbatches(
                _tableschema(src),
                fieldvec,
                AC.RecordBatch[],
                AC.OwnerRegion[],
                plan,
                budget,
                views,
                1,
            )
        end
        cols = _buildcolumns(fields, (), 1:length(fields), arrowtypes, views, 1)
        names = _fieldnamesymbols(fields, budget)
        return _table(names, cols, _tableschema(src), AC.OwnerRegion[], 0, nothing, budget)
    finally
        unlock(arrowtypeslock)
    end
end

Tables.schema(s::Stream) = Tables.schema(_schematable(s))
Tables.columnnames(s::Stream) =
    getfield(s, :scan) === nothing ? _fieldnamesymbols(_batchfields(s.src)) :
    Tables.columnnames(_schematable(s))

DataAPI.ncol(s::Stream) = length(Tables.columnnames(s))
function DataAPI.nrow(s::Stream)
    src = s.src
    scan = getfield(s, :scan)
    scan === nothing ||
        (scan.filter === nothing && scan.limit === nothing && scan.offset == 0) ||
        throw(
            ArgumentError(
                "the row count of a Stream with a filter, limit, or offset is " *
                "known once it is read; iterate it and add up its batches' rows",
            ),
        )
    src isa _StreamReader && throw(
        ArgumentError(
            "the row count of a stream-format source is known once it is read; " *
            "iterate the Arrow.Stream and add up its batches' rows",
        ),
    )
    src isa IPCStream && return sum(b -> Int(b.nrows), src.batches; init=0)
    budget = _readbudget(_configuredlimits(src), 0)
    return sum(i -> Int(_batchrows(src, i, budget)), 1:_nbatches(src); init=0)
end

DataAPI.metadatasupport(::Type{Stream}) = (read=true, write=false)
DataAPI.colmetadatasupport(::Type{Stream}) = (read=true, write=false)
DataAPI.metadatakeys(s::Stream) = DataAPI.metadatakeys(_schematable(s))
DataAPI.metadata(s::Stream, key::AbstractString, default=_NO_DEFAULT; style::Bool=false) =
    DataAPI.metadata(_schematable(s), key, default; style=style)
DataAPI.colmetadatakeys(s::Stream, col::Union{Symbol,Int}) =
    DataAPI.colmetadatakeys(_schematable(s), col)
DataAPI.colmetadatakeys(s::Stream) = DataAPI.colmetadatakeys(_schematable(s))
DataAPI.colmetadata(
    s::Stream,
    col::Union{Symbol,Int},
    key::AbstractString,
    default=_NO_DEFAULT;
    style::Bool=false,
) = DataAPI.colmetadata(_schematable(s), col, key, default; style=style)

function Base.show(io::IO, s::Stream)
    src = s.src
    m = DataAPI.ncol(s)
    cols = m == 1 ? "1 column" : "$m columns"
    if src isa _StreamReader
        print(io, "Arrow.Stream with ", cols, " and schema:\n")
    else
        n = _nbatches(src)
        print(io, "Arrow.Stream with ", n, n == 1 ? " record batch, " : " record batches, ")
        print(io, cols, ", and schema:\n")
    end
    show(IOContext(io, :print_schema_header => false), Tables.schema(s))
end
