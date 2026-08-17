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
# (the zero-copy typed-view layer, ViewPlan, is designed but deliberately
# deferred until the benchmark suite justifies its composite-eltype choice;
# it will slot in behind this same API). `Arrow.Stream` iterates record
# batches as one Table each. Scan pushdown routes through the ranged-scan
# adapter: on the file format, column pruning, statistics-based batch
# pruning, and window consumption all happen before decode.
# =============================================================================

"""
    Arrow.Table(source; scan=nothing, mmap=true) -> Table

Read Arrow IPC data as Tables.jl columns. `source` is a file path, an `IO`,
raw bytes (`Vector{UInt8}`), or an `Arrow.RangedSource` (byte-range reads —
see its docs). Both IPC formats are accepted: the file format (`ARROW1`
magic, random access, footer statistics) and the stream format.

`scan` is a `Tables.Scan` pushdown request: selected columns are the only
ones decoded, footer statistics prune batches no row of which can match the
filter, and exact limit/offset windows skip whole batches. On the file
format (and ranged sources) pruning happens before bytes are fetched or
decoded; on the stream format the scan is applied after decode.

One exception: a scan that cannot run in the storage domain — a filter
literal with no exact storage representation (a cross-domain or
out-of-range value), or an empty projection (`select=()`), whose row count
only the full read can carry — falls back to reading the whole source and
evaluating over the converted public values. Over a ranged source that
fallback fetches the entire object, as does reading a zero-field source;
plan remote filters in each column's public value domain.

Columns are materialized (plain `Vector`s): the returned table does not
borrow the source bytes, and [`Arrow.close!`](@ref) may be called at any
time afterward to release a memory-mapped file deterministically — do this
on Windows before deleting a mapped file.
"""
struct Table <: Tables.AbstractColumns
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol,Base.Int}
    schema::Union{Nothing,AC.Schema}
    regions::Vector{AC.OwnerRegion}
    nrows::Base.Int   # authoritative even with zero columns
end

function _table(names::Vector{Symbol}, columns::Vector{AbstractVector},
    schema, regions, nrows::Integer)
    lookup = Dict{Symbol,Base.Int}(nm => i for (i, nm) in enumerate(names))
    return Table(names, columns, lookup, schema, regions, Base.Int(nrows))
end

Tables.istable(::Type{Table}) = true
Tables.columnaccess(::Type{Table}) = true
Tables.columns(t::Table) = t
Tables.columnnames(t::Table) = getfield(t, :names)
Tables.getcolumn(t::Table, i::Base.Int) = getfield(t, :columns)[i]
Tables.getcolumn(t::Table, nm::Symbol) =
    getfield(t, :columns)[getfield(t, :lookup)[nm]]
Tables.schema(t::Table) = Tables.Schema(getfield(t, :names),
    [eltype(c) for c in getfield(t, :columns)])
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
function DataAPI.metadata(t::Table, key::AbstractString,
    default=_NO_DEFAULT; style::Bool=false)
    sch = getfield(t, :schema)
    if sch !== nothing && sch.metadata !== nothing
        for kv in sch.metadata
            first(kv) == key && return style ? (last(kv), :default) : last(kv)
        end
    end
    default === _NO_DEFAULT && throw(KeyError(key))
    return style ? (default, :default) : default
end
function _schemafield(t::Table, col::Symbol)
    sch = getfield(t, :schema)
    sch === nothing && return nothing
    i = findfirst(f -> f.name == String(col), collect(sch.fields))
    return i === nothing ? nothing : sch.fields[i]
end
function _colsymbol(t::Table, col::Symbol)
    haskey(getfield(t, :lookup), col) ||
        throw(ArgumentError("no column $(repr(col)) in this table"))
    return col
end
_colsymbol(t::Table, col::Base.Int) = getfield(t, :names)[col]
function DataAPI.colmetadatakeys(t::Table, col::Union{Symbol,Base.Int})
    f = _schemafield(t, _colsymbol(t, col))
    (f === nothing || f.metadata === nothing) && return ()
    return (String(first(kv)) for kv in f.metadata)
end
DataAPI.colmetadatakeys(t::Table) =
    (nm => DataAPI.colmetadatakeys(t, nm) for nm in getfield(t, :names)
     if !isempty(DataAPI.colmetadatakeys(t, nm)))
function DataAPI.colmetadata(t::Table, col::Union{Symbol,Base.Int},
    key::AbstractString, default=_NO_DEFAULT; style::Bool=false)
    f = _schemafield(t, _colsymbol(t, col))
    if f !== nothing && f.metadata !== nothing
        for kv in f.metadata
            first(kv) == key && return style ? (last(kv), :default) : last(kv)
        end
    end
    default === _NO_DEFAULT && throw(KeyError(key))
    return style ? (default, :default) : default
end

"""
    Arrow.close!(t::Union{Table,Stream})

Deterministically release the source regions behind a read (a memory map
unmaps NOW; imported foreign buffers run their release callbacks). `Table`
columns are materialized copies, so a closed `Table` remains fully usable;
a closed `Stream` refuses further iteration cleanly. Idempotent.
"""
function AC.close!(t::Table)
    foreach(AC.close!, getfield(t, :regions))
    return nothing
end

# --- Dates conversion (the facade owns what Core deliberately does not) ----

_mapcol(f::F, col) where {F} =
    eltype(col) >: Missing ?
    [x === missing ? missing : f(x) for x in col] : map(f, col)

_postconvert(::AC.ArrowType, col) = col
_postconvert(t::AC.DateType, col) = t.unit == AC.DAY ?
    _mapcol(x -> Dates.Date(Dates.UTD(Int64(x) + _EPOCH_DAYS)), col) :
    _mapcol(x -> Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH)), col)
function _postconvert(t::AC.TimestampType, col)
    # DateTime is millisecond-precision. Finer units stay as their raw
    # storage integers rather than silently truncating.
    t.unit == AC.SECOND &&
        return _mapcol(x -> Dates.DateTime(Dates.UTM(Int64(x) * 1000 +
            Dates.UNIXEPOCH)), col)
    t.unit == AC.MILLISECOND &&
        return _mapcol(x -> Dates.DateTime(Dates.UTM(Int64(x) +
            Dates.UNIXEPOCH)), col)
    return col
end
function _postconvert(t::AC.TimeType, col)
    scale = t.unit == AC.SECOND ? Int64(1_000_000_000) :
        t.unit == AC.MILLISECOND ? Int64(1_000_000) :
        t.unit == AC.MICROSECOND ? Int64(1_000) : Int64(1)
    return _mapcol(x -> Dates.Time(Dates.Nanosecond(Int64(x) * scale)), col)
end
function _postconvert(t::AC.DurationType, col)
    P = t.unit == AC.SECOND ? Dates.Second :
        t.unit == AC.MILLISECOND ? Dates.Millisecond :
        t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    return _mapcol(x -> P(Int64(x)), col)
end
_postconvert(t::AC.DictionaryType, col) = _postconvert(t.valuetype, col)

# The Julia element type a Field materializes as at the facade — a CLOSED
# mapping from the descriptor (the schema authority), never from observed
# values: an all-missing nullable Utf8 column is Vector{Union{Missing,
# String}}, a zero-row Int64 column is Vector{Int64}.
function _facadebasetype(t::AC.ArrowType)
    t isa AC.DateType &&
        return t.unit == AC.DAY ? Dates.Date : Dates.DateTime
    if t isa AC.TimestampType
        return t.unit == AC.SECOND || t.unit == AC.MILLISECOND ?
            Dates.DateTime : Int64
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
_facadeeltype(f::AC.Field) = f.nullable ?
    Union{Missing,_facadebasetype(f.type)} : _facadebasetype(f.type)

function _facadecolumn(f::AC.Field, parts::Vector)
    T = _facadeeltype(f)
    isempty(parts) && return T === Any ? Any[] : Vector{T}()
    col = length(parts) == 1 ? parts[1] : reduce(vcat, parts)
    converted = _postconvert(f.type, col)
    # materialize returns Vector{Any} (typed zero-copy views are ViewPlan's,
    # later); the FIELD decides the public eltype.
    return T === Any ? map(identity, converted) : collect(T, converted)
end

# --- scan value domain -------------------------------------------------------
# Pushdown and residual filtering run over PHYSICAL storage values; facade
# filter literals arrive in public Julia types. Lower every literal to the
# referenced field's storage domain BEFORE the scan, so file, ranged, and
# stream paths share one value domain; conversion back to public types then
# happens exactly once, on the scan OUTPUT (rename-aware via Tables.bind).

# Lowering returns (ok, value): ok=false means the literal has NO exact,
# semantics-preserving storage representation for this field (cross-type
# inexactness, wrong type entirely) — the caller must then evaluate the
# whole filter in the PUBLIC domain instead of pushing it down. There is no
# pass-through: an unlowered literal comparing "equal" to raw storage would
# change predicate semantics.
function _storagevalue(t::AC.ArrowType, v)
    t isa AC.DictionaryType && return _storagevalue(t.valuetype, v)
    istemporal = t isa AC.DateType || t isa AC.TimestampType ||
        t isa AC.TimeType || t isa AC.DurationType
    if istemporal
        # The contract is the FACADE comparison domain, not physical
        # representability: a literal lowers only when public-domain
        # comparison against this column's facade values could succeed.
        F = _facadebasetype(t)
        try
            if F === Int64
                # Raw-integer facade (sub-millisecond timestamps): only
                # integer literals compare in public; temporal literals are
                # never equal to Int64 values.
                v isa Integer && return true, Int64(v)
                return false, v
            elseif F === Dates.Date
                v isa Dates.Date &&
                    return true, Int32(Dates.value(v) - _EPOCH_DAYS)
                if v isa Dates.DateTime
                    v == Dates.DateTime(Dates.Date(v)) || return false, v
                    return true,
                        Int32(Dates.value(Dates.Date(v)) - _EPOCH_DAYS)
                end
                return false, v
            elseif F === Dates.DateTime
                dt = v isa Dates.DateTime ? v :
                    v isa Dates.Date ? Dates.DateTime(v) : nothing
                dt === nothing && return false, v
                ms = Int64(Dates.value(dt) - Dates.UNIXEPOCH)
                t isa AC.DateType && return true, ms       # Date64
                t.unit == AC.MILLISECOND && return true, ms
                return _exactdiv(ms, 1_000)                # SECOND
            elseif F === Dates.Time
                v isa Dates.Time || return false, v
                ns = Int64(Dates.value(v))
                t.unit == AC.NANOSECOND && return true, ns
                t.unit == AC.MICROSECOND && return _exactdiv(ns, 1_000)
                t.unit == AC.MILLISECOND && return _exactdiv(ns, 1_000_000)
                return _exactdiv(ns, 1_000_000_000)
            elseif F <: Dates.Period
                v isa Dates.Period || return false, v
                return true, Int64(Dates.value(convert(F, v)))
            end
        catch
            # Any conversion failure — range, inexactness, no method — means
            # the literal has no representation here; take the fallback.
            return false, v
        end
        return false, v
    end
    # Non-temporal fields compare in their storage (== public) domain, but a
    # temporal-typed public literal against them is incompatible.
    if v isa Dates.Date || v isa Dates.DateTime || v isa Dates.Time ||
       v isa Dates.Period
        return false, v
    end
    return true, v
end

_exactdiv(x::Int64, d::Integer) = begin
    q, r = divrem(x, Int64(d))
    r == 0 ? (true, q) : (false, x)
end

function _fieldfor(fields, ref, names)
    ref isa Base.Int && 1 <= ref <= length(fields) && return fields[ref]
    i = findfirst(==(Symbol(ref)), names)
    return i === nothing ? nothing : fields[i]
end

function _lowerexpr(e, fields, names, ok::Base.RefValue{Bool})
    e === nothing && return nothing
    if e isa Tables.Cmp
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return e
        good, v = _storagevalue(f.type, e.rhs)
        good || (ok[] = false)
        return Tables.Cmp(e.op, e.lhs, v)
    elseif e isa Tables.In
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return e
        vals = Any[]
        for x in e.values
            good, v = _storagevalue(f.type, x)
            good || (ok[] = false)
            push!(vals, v)
        end
        return Tables.In(e.lhs, Tuple(vals))
    elseif e isa Tables.AndExpr
        return Tables.AndExpr(
            Tables.ScanExpr[_lowerexpr(a, fields, names, ok) for a in e.args])
    elseif e isa Tables.OrExpr
        return Tables.OrExpr(
            Tables.ScanExpr[_lowerexpr(a, fields, names, ok) for a in e.args])
    elseif e isa Tables.NotExpr
        return Tables.NotExpr(_lowerexpr(e.arg, fields, names, ok))
    end
    return e
end

"""
Lower a scan for storage-domain pushdown. Returns `(pushscan, pushable)`:
when any filter literal has no exact storage representation, or the bound
output selects zero columns (the row count would be lost), pushable=false
and the caller evaluates the ORIGINAL scan over the converted public table.
Type overrides are ALWAYS stripped from the pushdown copy — they are public-
domain conversions and run after facade conversion.
"""
function _lowerscan(scan::Tables.Scan, fields)
    names = Symbol[Symbol(f.name) for f in fields]
    b = Tables.bind(scan, names)
    isempty(b.columns) && !isempty(fields) && return scan, false
    ok = Ref(true)
    lowered = _lowerexpr(scan.filter, fields, names, ok)
    ok[] || return scan, false
    pushselect = Tables.SelectItem[Tables.SelectItem(names[c.index], nothing,
        c.name == names[c.index] ? nothing : c.name) for c in b.columns]
    return Tables.Scan(pushselect, lowered, scan.limit, scan.offset,
        scan.validate), true
end

# --- source opening ---------------------------------------------------------

const _FILE_MAGIC = b"ARROW1"

_isfilebytes(bytes::Vector{UInt8}) =
    length(bytes) >= 6 && view(bytes, 1:6) == _FILE_MAGIC

function _openbytes(bytes::Vector{UInt8})
    return _isfilebytes(bytes) ? readfile(bytes) : readstream(bytes)
end

function _opensource(path::AbstractString; mmap::Bool=true)
    magic = open(io -> Base.read(io, 6), path)
    if magic == _FILE_MAGIC && mmap
        return readfile(mmapregion(path))
    end
    return _openbytes(Base.read(path))
end
_opensource(io::IO; mmap::Bool=true) = _openbytes(Base.read(io))
_opensource(bytes::Vector{UInt8}; mmap::Bool=true) = _openbytes(bytes)
_opensource(src::Union{IPCStream,ArrowFile}; mmap::Bool=true) = src

"Distinct owner regions reachable from a source's decoded batches."
function _sourceregions(s::IPCStream)
    seen = IdDict{AC.OwnerRegion,Nothing}()
    function walk(d::AC.ArrayData)
        for b in d.buffers
            b.region === nothing || (seen[b.region::AC.OwnerRegion] = nothing)
        end
        foreach(walk, d.children)
        d.dictionary === nothing || walk(d.dictionary::AC.ArrayData)
    end
    for batch in s.batches, col in batch.columns
        walk(col)
    end
    return collect(keys(seen))
end
_sourceregions(f::ArrowFile) = AC.OwnerRegion[f.region]

# --- Table construction ------------------------------------------------------

function Table(source; scan::Union{Nothing,Tables.Scan}=nothing,
    mmap::Bool=true)
    if source isa RangedSource || source isa RangedFile
        rf = source isa RangedSource ? RangedFile(source) : source
        # One extra tail fetch buys the schema up front: literal lowering,
        # exactly-once output conversion, and DataAPI metadata all need it.
        sch, rfields = rangedschema(rf)
        theScan = scan === nothing ? Tables.Scan() : scan
        if isempty(rfields)
            # A zero-field object is bytes-tiny; fetch it whole so the row
            # count survives the read.
            bytes = _fetchexact(rf.src, Int64(0), rf.src.len)
            return _publicscan(
                _materialize_table(readfile(bytes; limits=rf.limits),
                    AC.OwnerRegion[]), sch, rfields, theScan,
                AC.OwnerRegion[])
        end
        pushscan, pushable = _lowerscan(theScan, rfields)
        if pushable
            got = Tables.scan(rf, pushscan)
            return _wrapscanned(got, sch, rfields, theScan)
        end
        full = _wrapscanned(Tables.scan(rf, Tables.Scan()), sch, rfields,
            Tables.Scan())
        return _publicscan(full, sch, rfields, theScan, AC.OwnerRegion[])
    end
    src = _opensource(source; mmap=mmap)
    regions = _sourceregions(src)
    fields = _corefields(src)
    scan === nothing && return _materialize_table(src, regions)
    # Zero-field sources carry their row count on the Table itself; the raw
    # scan path would lose it inside an empty NamedTuple.
    isempty(fields) && return _publicscan(_materialize_table(src, regions),
        _tableschema(src), fields, scan, regions)
    pushscan, pushable = _lowerscan(scan, fields)
    if pushable
        if src isa ArrowFile
            got = Tables.scan(src, pushscan)
        else
            # Stream format: decode RAW columns and scan in the storage
            # domain — the same value domain as the pushdown paths.
            names = Symbol[Symbol(f.name) for f in fields]
            raw = NamedTuple{Tuple(names)}(Tuple(_rawcolumn(src, i)
                for i = 1:length(fields)))
            got = Tables.finish(raw, pushscan)
        end
        return _wrapscanned(got, _tableschema(src), fields, scan;
            regions=regions)
    end
    # Unpushable scans (unrepresentable literals, empty projections)
    # evaluate the ORIGINAL scan over the fully converted public table —
    # correctness first; these are rare shapes.
    return _publicscan(_materialize_table(src, regions), _tableschema(src),
        fields, scan, regions)
end

"Evaluate a scan in the PUBLIC value domain over a converted Table."
function _publicscan(full::Table, schema, sourcefields, scan, regions)
    if isempty(Tables.columnnames(full))
        # No columns can carry the count through Tables.finish. Binding is
        # STRUCTURAL and always runs — unsupported predicate nodes reject
        # regardless of `validate`, exactly as Tables.bind rules; validate
        # only opts out of unmatched column references.
        Tables.bind(scan, Symbol[])
        # Row-invariant predicate, evaluated ONCE — no mask or index vector
        # may be allocated from an untrusted row count.
        keep = _zerofieldpredicate(scan.filter)
        n1 = Base.Int(_zerofieldcount(Int64(Tables.rowcount(full)), keep,
            scan.limit, scan.offset))
        return _table(Symbol[], AbstractVector[], schema,
            AC.OwnerRegion[regions...], n1)
    end

    # Row count survives an empty projection: window+filter first over the
    # full column set, then project.
    counted = Tables.finish(full,
        Tables.Scan(nothing, scan.filter, scan.limit, scan.offset,
            scan.validate))
    n = Base.Int(Tables.rowcount(Tables.columns(counted)))
    got = Tables.finish(counted,
        Tables.Scan(scan.select, nothing, nothing, 0, scan.validate))
    cols = Tables.columns(got)
    names = collect(Symbol, Tables.columnnames(cols))
    columns = AbstractVector[Tables.getcolumn(cols, nm) for nm in names]
    precols = AbstractVector[]
    if !isempty(sourcefields)
        srcnames = Symbol[Symbol(f.name) for f in sourcefields]
        b = Tables.bind(scan, srcnames)
        precols = AbstractVector[Tables.getcolumn(full, srcnames[bc.index])
                                 for bc in b.columns]
    end
    bound = _boundschema(schema, sourcefields, scan, precols)
    return _table(names, columns, bound, AC.OwnerRegion[regions...], n)
end

_corefields(s::IPCStream) = collect(AC.Field, s.corefields)
_corefields(f::ArrowFile) = collect(AC.Field, f.fields)

_rawcolumn(s::IPCStream, i::Base.Int) = begin
    parts = [materialize(s.corefields[i], b.columns[i]) for b in s.batches]
    isempty(parts) ? Any[] : reduce(vcat, parts)
end

_tableschema(s::IPCStream) = s.schema
_tableschema(f::ArrowFile) = f.schema

function _materialize_table(src::IPCStream, regions)
    names = Symbol[Symbol(f.name) for f in src.schema.fields]
    cols = AbstractVector[
        _facadecolumn(f, [materialize(f, b.columns[i]) for b in src.batches])
        for (i, f) in enumerate(src.corefields)]
    nrows = sum(Base.Int(b.nrows) for b in src.batches; init=0)
    return _table(names, cols, src.schema, regions, nrows)
end

function _materialize_table(src::ArrowFile, regions)
    names = Symbol[Symbol(f.name) for f in src.schema.fields]
    nb = length(src)
    batches = [src[i] for i = 1:nb]
    cols = AbstractVector[
        _facadecolumn(f, [materialize(f, b.columns[i]) for b in batches])
        for (i, f) in enumerate(src.fields)]
    nrows = sum(Base.Int(b.nrows) for b in batches; init=0)
    return _table(names, cols, src.schema, regions, nrows)
end

"""
The OUTPUT schema of a scan: bound source fields under their output names.
`precols` supplies each output's pre-override (facade-narrowed) column, so
override keep/drop follows the SAME actual-subtype decision the conversion
made: a no-op override keeps its retained field; a real conversion drops it
(a later rewrite re-infers the column).
"""
# The eltype the keep/drop decision uses for an EMPTY pre-override column:
# the descriptor's declared facade type. Composites materialize rows as
# vectors (their eltype accident is `Any[]` when no rows exist), so the
# declared domain — not the accident — must drive subsumption, keeping the
# empty decision identical to the nonempty one.
# Field-aware cases: run-end encoding is transparent at the value layer
# (rows ARE the values child's rows, no REE-level validity); dictionary
# rows are pool VALUES whose composite children live on the value FIELD
# (Dictionary<REE<...>>); union rows take the WINNING child's type, so the
# declared domain is the union of the children's declared domains — closed
# for one-child and all-compatible unions, honest for heterogeneous ones.
# Missing in the declared type never changes keep/drop (the rule tests
# `D <: Union{T,Missing}`), so nullability wraps are cosmetic.
function _declaredeltype(f::AC.Field)
    t = f.type
    if t isa AC.RunEndEncodedType && length(f.children) == 2
        return _declaredeltype(f.children[2])
    end
    if t isa AC.DictionaryType
        D0 = _declaredeltype(AC.dictvaluefield(f, t))
        return f.nullable ? Union{Missing,D0} : D0
    end
    if t isa AC.UnionType && !isempty(f.children)
        return Union{Any[_declaredeltype(c) for c in f.children]...}
    end
    D = _declaredbasetype(t)
    return f.nullable ? Union{Missing,D} : D
end
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
    t isa AC.DecimalType ? (t.bits == 32 ? Int32 :
        t.bits == 64 ? Int64 : Vector{UInt8}) :
    t isa AC.IntervalType ? (t.unit == AC.YEAR_MONTH ? Int32 :
        t.unit == AC.DAY_TIME ?
        NamedTuple{(:days, :millis),Tuple{Int32,Int32}} :
        NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}}) :
    t isa AC.DictionaryType ? _declaredbasetype(t.valuetype) :
    _facadebasetype(t)

function _boundschema(schema, sourcefields, scan, precols)
    (schema === nothing || scan === nothing) && return schema
    b = Tables.bind(scan, Symbol[Symbol(f.name) for f in sourcefields])
    outfields = AC.Field[]
    for (i, bc) in enumerate(b.columns)
        f = sourcefields[bc.index]
        if bc.type !== nothing && i <= length(precols)
            D = isempty(precols[i]) ? _declaredeltype(f) : eltype(precols[i])
            D <: Union{bc.type,Missing} || continue
        end
        push!(outfields, AC.Field(String(bc.name), f.type;
            nullable=f.nullable,
            metadata=f.metadata === nothing ? nothing :
                collect(Pair{String,String}, f.metadata),
            children=collect(AC.Field, f.children)))
    end
    return AC.Schema(outfields; metadata=schema.metadata === nothing ?
        nothing : collect(Pair{String,String}, schema.metadata))
end

"Wrap a scan output (storage-domain columns) into a Table, converting once."
function _wrapscanned(got, schema, sourcefields, scan;
    regions=AC.OwnerRegion[])
    cols = Tables.columns(got)
    names = collect(Symbol, Tables.columnnames(cols))
    columns = AbstractVector[Tables.getcolumn(cols, nm) for nm in names]
    precols = AbstractVector[]
    if scan !== nothing && !isempty(sourcefields)
        b = Tables.bind(scan, Symbol[Symbol(f.name) for f in sourcefields])
        length(b.columns) == length(columns) || throw(AssertionError(
            "scan output width $(length(columns)) does not match its bound " *
            "selection $(length(b.columns))"))
        for (i, bc) in enumerate(b.columns)
            f = sourcefields[bc.index]
            converted = _postconvert(f.type, columns[i])
            # Public type overrides run HERE, after facade conversion —
            # they are public-domain requests, never storage casts, and
            # they preserve missing exactly as Tables.finish does.
            T = _facadeeltype(f)
            base = T === Any ? map(identity, converted) :
                collect(T, converted)
            push!(precols, base)
            columns[i] = bc.type === nothing ? base :
                _applyoverride(bc.type, base)
        end
    end
    nrows = isempty(columns) ? _scanrowcount(got) : length(columns[1])
    bound = _boundschema(schema, sourcefields, scan, precols)
    return _table(names, columns, bound, AC.OwnerRegion[regions...], nrows)
end

_scanrowcount(got) = Base.Int(Tables.rowcount(Tables.columns(got)))

"Convert a column to an override type with Tables.finish's exact rules."
function _applyoverride(T, col)
    # finish's no-op rule: a column already accepted by Union{T,Missing}
    # passes through untouched (supertype overrides included).
    eltype(col) <: Union{T,Missing} && return col
    # A REAL conversion preserves the requested target type exactly and
    # widens with Missing only for OBSERVED missing values — the
    # authority's rule, opposite of declared-nullability.
    TN = Base.nonmissingtype(T)
    hasmissing = any(x -> x === missing, col)
    if T >: Missing || hasmissing
        S = T >: Missing ? T : Union{Missing,TN}
        return S[x === missing ? missing : convert(TN, x) for x in col]
    end
    return TN[convert(TN, x) for x in col]
end

# --- Stream ------------------------------------------------------------------

"""
    Arrow.Stream(source; mmap=true)

Iterate an IPC source one record batch at a time; each iteration yields an
[`Arrow.Table`](@ref) for that batch. Satisfies `Tables.partitions`, so
`Arrow.write(sink, Arrow.Stream(...))` streams batch-per-batch, and works
directly with partition-aware sinks.
"""
struct Stream
    src::Union{IPCStream,ArrowFile}
    regions::Vector{AC.OwnerRegion}
end

Stream(source; mmap::Bool=true) = begin
    src = _opensource(source; mmap=mmap)
    Stream(src, _sourceregions(src))
end

AC.close!(s::Stream) = (foreach(AC.close!, getfield(s, :regions)); nothing)

_nbatches(s::IPCStream) = length(s.batches)
_nbatches(f::ArrowFile) = length(f)
_batch(s::IPCStream, i) = s.batches[i]
_batch(f::ArrowFile, i) = f[i]
_batchfields(s::IPCStream) = s.corefields
_batchfields(f::ArrowFile) = f.fields

Base.length(s::Stream) = _nbatches(s.src)
Base.eltype(::Type{Stream}) = Table

function Base.iterate(s::Stream, i::Base.Int=1)
    i > _nbatches(s.src) && return nothing
    b = _batch(s.src, i)
    fields = _batchfields(s.src)
    names = Symbol[Symbol(f.name) for f in fields]
    cols = AbstractVector[_facadecolumn(f, [materialize(f, b.columns[j])])
                          for (j, f) in enumerate(fields)]
    return _table(names, cols, _tableschema(s.src), s.regions,
        Base.Int(b.nrows)), i + 1
end

Tables.partitions(s::Stream) = s
