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
# Tables.Scan pushdown: one planner for IPC files, whose bytes are a region in
# memory or a byte-range source (`SourceFile` over an `AbstractArrowSource`),
# and one executor for every source. Design notes:
# docs/dev/DESIGN-scan-ranges-trim.md.
#
# The request is resolved once (`_ScanPlan`); the executor (`_runscan`) then
# consumes the bound scan exactly, batch by batch:
#
#   * the decode set is (selected ∪ filter-referenced) columns; everything
#     else is SKIPPED by `skipfield!`, a registry walk that consumes the
#     node/buffer accounting (all buffer-table invariants still checked)
#     without slicing, decompressing, validating, or materializing anything,
#     and over a byte-range source no body range is planned for it;
#   * without a filter `limit`/`offset` are metadata arithmetic:
#     `RecordBatch.length` is wire metadata, so batches outside the window
#     are never decoded; a filter is evaluated per batch on public values,
#     reads the other selected columns only for batches with a qualifying
#     row, and composes the window over the qualifying rows;
#   * the returned table holds the selected columns' kept rows under their
#     output names, in selection order, with type overrides converted.
# =============================================================================

# ---------------------------------------------------------------------------
# skipfield!: the decode walk minus the decode
# ---------------------------------------------------------------------------

"""
Advance the cursor past one field's node and buffers — the exact traversal
`decodefield` performs, with every buffer-table invariant still enforced
(`_buffermeta!`), but no body access: nothing is sliced, decompressed,
validated, or kept. Over a ranged source, no body range is planned for
the skipped bytes; tail reads and coalescing may still over-read them.
"""
function skipfield!(f::Field, c::DecodeCursor)
    t = f.type
    takenode!(c)
    spec = layoutspec(t)
    for _ in spec.buffers
        skipbuffer!(c)
    end
    if spec.variadic
        for _ = 1:takevariadic!(c)
            skipbuffer!(c)
        end
    end
    t isa DictionaryType && return nothing
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        skipfield!(f.children[i], c)
    end
    return nothing
end

"FieldNode entries consumed by one field subtree."
function _fieldnodespan(f::Field)
    f.type isa DictionaryType && return 1
    spec = layoutspec(f.type)
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    return 1 + sum(_fieldnodespan(f.children[i]) for i = 1:nchildren; init=0)
end

"""
Buffers consumed by one field subtree — the planner's registry arithmetic.
View fields consume their declared variadic count on top of the fixed
registry pair, so the walk carries the batch's variadic-count cursor in
depth-first order (the same order the decode cursor consumes it).
"""
function _bufferspan(f::Field, variadics::AbstractVector{Int64}, varidx::Base.RefValue{Int})
    spec = layoutspec(f.type)
    n = Int64(length(spec.buffers))
    if spec.variadic
        varidx[] <= length(variadics) || throw(
            ValidationError(
                "metadata declares fewer variadic buffer counts than the schema requires",
            ),
        )
        vc = variadics[varidx[]]
        varidx[] += 1
        vc >= 0 || throw(ValidationError("variadic buffer count $vc is invalid"))
        n = _checkedadd(n, vc, "buffer span")
    end
    f.type isa DictionaryType && return n
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        n = _checkedadd(n, _bufferspan(f.children[i], variadics, varidx), "buffer span")
    end
    return n
end

"""
Validate the metadata needed before a RecordBatch length may drive a scan
window or a buffer table may drive a range fetch. This is the metadata-only
half of the decode cursor: exact node/buffer counts, every node invariant,
top-level row-count agreement, and every buffer's geometry.
"""
function _recordbatchmeta(
    header::Meta.RecordBatch,
    fields,
    limits::Limits,
    bodylen::Int64,
    variadics::AbstractVector{Int64}=variadiccounts(header),
)
    rblen = something(header.length, Int64(0))
    rblen >= 0 || throw(ValidationError("record batch length $rblen is negative"))
    rblen <= limits.max_array_length || _limiterror(
        "record batch length $rblen",
        "max_array_length",
        limits.max_array_length,
    )

    nodes = something(header.nodes, Meta.FieldNode[])
    expectednodes = sum(_fieldnodespan(f) for f in fields; init=0)
    length(nodes) == expectednodes ||
        throw(ValidationError("field-node count does not match the schema"))
    nodeidx = 1
    for f in fields
        node = nodes[nodeidx]
        node.length == rblen || throw(
            ValidationError("RecordBatch length does not match top-level field nodes"),
        )
        nodeidx += _fieldnodespan(f)
    end
    for node in nodes
        node.length >= 0 ||
            throw(ValidationError("field-node length $(node.length) is negative"))
        node.length <= limits.max_array_length || _limiterror(
            "field-node length $(node.length)",
            "max_array_length",
            limits.max_array_length,
        )
        0 <= node.null_count <= node.length ||
            throw(ValidationError("invalid field-node null count $(node.null_count)"))
    end

    buffers = something(header.buffers, Meta.Buffer[])
    varidx = Ref(1)
    expectedbuffers = Int64(0)
    for f in fields
        expectedbuffers = _checkedadd(
            expectedbuffers,
            _bufferspan(f, variadics, varidx),
            "record-batch buffer span",
        )
    end
    varidx[] == length(variadics) + 1 ||
        throw(ValidationError("unconsumed variadic buffer counts: schema/batch mismatch"))
    length(buffers) == expectedbuffers ||
        throw(ValidationError("buffer count does not match the schema"))
    last_nonempty_end = Int64(0)
    for b in buffers
        offset = Int64(b.offset)
        len = Int64(b.length)
        offset >= 0 || throw(ValidationError("negative batch buffer offset $offset"))
        offset % 8 == 0 ||
            throw(ValidationError("batch buffer offset $offset is not 8-byte aligned"))
        len >= 0 || throw(ValidationError("batch buffer length $len is negative"))
        len <= limits.max_buffer_bytes || _limiterror(
            "batch buffer length $len",
            "max_buffer_bytes",
            limits.max_buffer_bytes,
        )
        bufferend = _checkedadd(offset, len, "batch buffer end")
        bufferend <= bodylen ||
            throw(ValidationError("batch buffer [$offset, $len] escapes its message body"))
        if len > 0
            offset >= last_nonempty_end ||
                throw(ValidationError("batch buffers overlap or move backwards"))
            last_nonempty_end = bufferend
        end
    end
    return rblen
end

function _planminbytes(role, spec, node, len::Int64)
    if role == AC.VALIDITY
        len == 0 && node.null_count == 0 && return Int64(0)
        return node.length ÷ 8 + (node.length % 8 == 0 ? 0 : 1)
    elseif role == AC.DATA
        spec.fixedwidth > 0 && return _checkedmul(
            node.length,
            Int64(spec.fixedwidth),
            "planned data-buffer size",
        )
        if spec.fixedwidth == -1
            return node.length ÷ 8 + (node.length % 8 == 0 ? 0 : 1)
        end
        return Int64(0)
    elseif role == AC.OFFSETS
        count = _checkedadd(node.length, Int64(1), "planned offset count")
        return _checkedmul(count, Int64(spec.offsetwidth), "planned offsets-buffer size")
    elseif role == AC.ELEMENT_OFFSETS || role == AC.SIZES
        return _checkedmul(
            node.length,
            Int64(spec.offsetwidth),
            "planned element-buffer size",
        )
    elseif role == AC.TYPE_IDS
        return node.length
    elseif role == AC.VIEWS
        return _checkedmul(node.length, Int64(16), "planned views-buffer size")
    end
    return Int64(0)
end

function _validateplannedfield!(f::Field, c::DecodeCursor, codec::Int8)
    node = takenode!(c)
    t = f.type
    if t isa NullType
        node.null_count == node.length ||
            throw(ValidationError("Null field-node null count must equal its length"))
    elseif t isa UnionType
        node.null_count == 0 ||
            throw(ValidationError("Union field-node null count must be zero"))
    end
    # Field.nullable is advisory (enforced only by the opt-in validate_full
    # tier), so a planned scan makes no nullability judgment here — the same
    # contract the whole-file path applies.
    spec = layoutspec(f.type)
    for role in spec.buffers
        _, len = _buffermeta!(c)
        if codec == CODEC_NONE || len == 0
            need = _planminbytes(role, spec, node, len)
            len >= need || throw(
                ValidationError(
                    "planned buffer length $len is smaller than required $need",
                ),
            )
        else
            len >= 8 || throw(
                ValidationError("compressed buffer of $len bytes lacks its length prefix"),
            )
            need = _planminbytes(role, spec, node, Int64(0))
            need > 0 &&
                len == 8 &&
                throw(
                    ValidationError(
                        "compressed planned buffer requires a nonempty payload",
                    ),
                )
        end
    end
    if spec.variadic
        # Variadic view-data buffers have no metadata-derivable minimum
        # (views reference them arbitrarily); geometry and, under
        # compression, the prefix rule are the plannable invariants.
        for _ = 1:takevariadic!(c)
            _, len = _buffermeta!(c)
            codec == CODEC_NONE ||
                len == 0 ||
                len >= 8 ||
                throw(
                    ValidationError(
                        "compressed buffer of $len bytes lacks its length prefix",
                    ),
                )
        end
    end
    f.type isa DictionaryType && return node.length
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    fslextent =
        t isa FixedSizeListType ?
        _checkedmul(node.length, Int64(t.listsize), "fixed-size-list child length") :
        Int64(0)
    firstchildlen = Int64(-1)
    for i = 1:nchildren
        childlen = _validateplannedfield!(f.children[i], c, codec)
        i == 1 && (firstchildlen = childlen)
        if t isa FixedSizeListType
            childlen >= fslextent || throw(
                ValidationError("fixed-size-list child is shorter than its parent extent"),
            )
        elseif t isa StructType
            childlen >= node.length ||
                throw(ValidationError("struct child is shorter than its parent extent"))
        elseif t isa UnionType && t.mode == AC.SparseMode
            childlen == node.length || throw(
                ValidationError(
                    "sparse-union child length does not equal its parent length",
                ),
            )
        elseif t isa RunEndEncodedType && i == 2
            childlen == firstchildlen ||
                throw(ValidationError("REE run-end and value child lengths must match"))
        end
    end
    if t isa RunEndEncodedType
        node.null_count == 0 || throw(ValidationError("REE parent null count must be zero"))
        node.length == 0 ||
            firstchildlen > 0 ||
            throw(
                ValidationError("a nonempty REE array requires at least one physical run"),
            )
        runtype = f.children[1].type::IntType
        maxrunend =
            runtype.bits == 16 ? Int64(typemax(Int16)) :
            runtype.bits == 32 ? Int64(typemax(Int32)) : typemax(Int64)
        node.length <= maxrunend ||
            throw(ValidationError("REE logical extent exceeds its run-end range"))
    end
    return node.length
end

"""
Validate every metadata-only invariant for the subtrees whose bodies are
planned, and return the number of nonempty masked-in buffers — exactly the
body ranges the caller will plan.
"""
function _validatebodyplan(
    header::Meta.RecordBatch,
    fields,
    limits::Limits,
    codec::Int8,
    mask,
    variadics::AbstractVector{Int64}=variadiccounts(header),
)
    cursor = DecodeCursor(
        header.nodes,
        header.buffers,
        BufferSlice(),
        limits;
        codec=codec,
        variadics=variadics,
    )
    selected = Int64(0)
    for (j, f) in enumerate(fields)
        firstbuffer = cursor.bufidx
        mask[j] ? _validateplannedfield!(f, cursor, codec) : skipfield!(f, cursor)
        if mask[j]
            for i = firstbuffer:(cursor.bufidx - 1)
                cursor.buffers[i].length > 0 &&
                    (selected = AC.checked_add(selected, Int64(1)))
            end
        end
    end
    finishcursor!(cursor)
    return selected
end

"""
Like `missingdicts`, but a missing dictionary only matters when its field is
in the decode set — a batch may legally reference an id its skipped columns
never resolve.
"""
function _scanmissingdicts(fields, nodes, dicts, fielddictids, mask::AbstractVector{Bool})
    ns = something(nodes, Meta.FieldNode[])
    idx = Ref(1)
    function walk(f::Field, decoded::Bool)
        idx[] <= length(ns) || throw(
            ValidationError("metadata declares fewer field nodes than the schema requires"),
        )
        node = ns[idx[]]
        idx[] += 1
        if f.type isa DictionaryType
            decoded || return
            id = fielddictids[f]
            if !haskey(dicts, id)
                node.length >= 0 && node.null_count == node.length || throw(
                    ValidationError(
                        "record batch uses undefined dictionary id $id for a non-null slot",
                    ),
                )
            end
            return
        end
        spec = layoutspec(f.type)
        nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
        for i = 1:nchildren
            walk(f.children[i], decoded)
        end
    end
    for (j, f) in enumerate(fields)
        walk(f, mask[j])
    end
    return nothing
end

# ---------------------------------------------------------------------------
# Masked batch decode over the file's Block index
# ---------------------------------------------------------------------------

"Row count of batch `i` from Block metadata alone — no body access."
function _batchrows(f::ArrowFile, i::Int, budget::AllocationBudget)
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, budget)
    _filemetadataversion(f.schemaversion, fm.version)
    rejectexperimentalcompression(fm)
    fm.msg.header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    return _recordbatchmeta(fm.msg.header, f.fields, f.limits, fm.body.len)
end

"""
Three-valued evaluation of a scan predicate over a ZERO-FIELD row: every
column reference is an all-missing column, constants evaluate, and the
result is `true`, `false`, or `missing` (SQL semantics; only `true` keeps
rows). Row-invariant by construction, so one evaluation covers every row —
no per-row mask may be allocated from an untrusted row count.
"""
function _zerofieldpredicate(e)
    e === nothing && return true
    e isa Tables.AlwaysTrue && return true
    e isa Tables.AlwaysFalse && return false
    e isa Tables.IsNull && return !e.negated
    if e isa Tables.AndExpr
        sawmissing = false
        for a in e.args
            r = _zerofieldpredicate(a)
            r === false && return false
            r === missing && (sawmissing = true)
        end
        return sawmissing ? missing : true
    end
    if e isa Tables.OrExpr
        sawmissing = false
        for a in e.args
            r = _zerofieldpredicate(a)
            r === true && return true
            r === missing && (sawmissing = true)
        end
        return sawmissing ? missing : false
    end
    if e isa Tables.NotExpr
        r = _zerofieldpredicate(e.arg)
        return r === missing ? missing : !r
    end
    return missing   # Cmp/In/StrPred against a missing column
end

# The field whose values a filter literal compares with: run-end and
# dictionary encodings are transparent wrappers around their values' field.
# (Dictionary children live on the type; run-end children on the Field.)
function _leaffield(f::AC.Field)
    t = f.type
    if t isa AC.RunEndEncodedType && length(f.children) == 2
        return _leaffield(f.children[2])
    end
    t isa AC.DictionaryType && return _leaffield(AC.dictvaluefield(f, t))
    return f
end

function _fieldfor(fields, ref, names)
    ref isa Int && 1 <= ref <= length(fields) && return fields[ref]
    i = findfirst(==(Symbol(ref)), names)
    return i === nothing ? nothing : fields[i]
end

# --- zoned timestamp literals --------------------------------------------------
# A UTC timestamp column (pandas tz-aware data, DuckDB TIMESTAMPTZ) reads as
# `ZonedTimestamp{P,:UTC}`, which does not compare with `DateTime`, `Date`, or
# `Timestamp`. A filter binding one of those to a UTC column reads it as the
# same UTC wall time. For a column in another zone a zone-free literal is
# ambiguous, and the filter fails naming a literal that works.

const _ZonelessTime = Union{Dates.DateTime,Dates.Date,Durations.Timestamp}

_utcinstant(x::Dates.DateTime) = Durations.ZonedTimestamp{Dates.Millisecond,:UTC}(
    Durations.Timestamp{Dates.Millisecond}(x),
)
_utcinstant(x::Dates.Date) = Durations.ZonedTimestamp{Dates.Second,:UTC}(x)
_utcinstant(x::Durations.Timestamp{P}) where {P} = Durations.ZonedTimestamp{P,:UTC}(x)

# `x` as a literal for field `f`: unchanged unless `f` holds zoned timestamps.
function _zonedliteral(f::AC.Field, x)
    x isa _ZonelessTime || return x
    t = _leaffield(f).type
    t isa AC.DictionaryType && (t = t.valuetype)
    t isa AC.TimestampType || return x
    zone = _timestampzone(t)
    zone === nothing && return x
    P = _timestampperiod(t.unit)
    if zone == "UTC"
        z = _uninferred(_utcinstant, x)
        # In the column's unit when exact, so `Set` membership hashes alike.
        try
            return Durations.ZonedTimestamp{P,:UTC}(z)
        catch err
            err isa Union{InexactError,OverflowError} || rethrow()
            return z
        end
    end
    unit = nameof(P)
    throw(
        ArgumentError(
            "column $(f.name) holds ZonedTimestamp{$unit, " *
            "$(_uninferred(repr, Symbol(zone))::String)} values, " *
            "which do not compare with a $(nameof(typeof(x))); compare with an instant " *
            "such as `Durations.ZonedTimestamp{Dates.$unit,:UTC}(2020, 1, 1)`",
        ),
    )
end

function _zonedliterals(e, fields, names)
    if e isa Tables.Cmp
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return e
        v = _zonedliteral(f, e.rhs)
        return v === e.rhs ? e : Tables.Cmp(e.op, e.lhs, v)
    elseif e isa Tables.In
        f = _fieldfor(fields, e.lhs.ref, names)
        values = e.values
        # Only Base containers whose membership semantics survive a rebuild.
        (f === nothing || !(values isa Union{Tuple,Array,Set})) && return e
        # `values`' type is known at run time; see `_uninferred`.
        return _uninferred(_zonedin, e, f, values)::Tables.In
    elseif e isa Tables.AndExpr
        return Tables.AndExpr(
            Tables.ScanExpr[_zonedliterals(a, fields, names) for a in e.args],
        )
    elseif e isa Tables.OrExpr
        return Tables.OrExpr(
            Tables.ScanExpr[_zonedliterals(a, fields, names) for a in e.args],
        )
    elseif e isa Tables.NotExpr
        return Tables.NotExpr(_zonedliterals(e.arg, fields, names))
    end
    return e
end

# `e` with its zone-free time values given `f`'s zone.
function _zonedin(e::Tables.In, f, values)
    any(x -> x isa _ZonelessTime, values) || return e
    lit(x) = _zonedliteral(f, x)
    rebuilt =
        values isa Tuple ? map(lit, values) :
        values isa Set ? Set(lit(x) for x in values) : map(lit, values)
    return Tables.In(e.lhs, rebuilt)
end

function _zonedbound(b::Tables.BoundScan, fields, names)
    b.filter === nothing && return b
    filter = _zonedliterals(b.filter, fields, names)
    filter === b.filter && return b
    return Tables.BoundScan(b.columns, filter, b.filtercols, b.limit, b.offset, b.validate)
end

# --- filter literal kinds --------------------------------------------------------
# A literal of another kind than its column's values (a number for a string
# column, say) is an ArgumentError naming the column when the scan binds, not a
# MethodError once values are compared. Kinds: numbers, strings, characters,
# local and zoned instants, times of day, periods; other values, and columns
# with an extension label, are not checked.

function _filterkind(f::AC.Field)
    _hasarrowtypesextension(f) && return nothing
    t = _leaffield(f).type
    t isa Union{AC.IntType,AC.FloatType,AC.BoolType,AC.DecimalType} && return :number
    (t isa AC.Utf8Type || (t isa AC.ViewType && t.utf8)) && return :string
    t isa AC.DateType && return :local
    t isa AC.TimestampType && return _timestampzone(t) === nothing ? :local : :zoned
    t isa AC.TimeType && return :time
    t isa AC.DurationType && return :period
    return nothing
end

_filterkind(::Type{T}) where {T} =
    T <: Real ? :number :
    T <: AbstractString ? :string :
    T <: AbstractChar ? :char :
    T <: Durations.ZonedTimestamp ? :zoned :
    T <: _ZonelessTime ? :local :
    T <: Dates.Time ? :time : T <: Dates.Period ? :period : nothing

function _checkliterals(e, fields, names)
    if e isa Union{Tables.Cmp,Tables.In,Tables.StrPred}
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return nothing
        kind = _filterkind(f)
        kind === nothing && return nothing
        r = e.lhs.ref
        ref = r isa Integer ? names[r] : r
        if e isa Tables.StrPred
            kind === :string || throw(
                ArgumentError(
                    "the filter matches text in column $ref, which holds " *
                    "$(_kindvalues(f, kind))",
                ),
            )
        elseif e isa Tables.Cmp
            _checkliteral(f, ref, kind, e.rhs)
        elseif e.values isa Union{Tuple,AbstractArray,AbstractSet}
            foreach(v -> _checkliteral(f, ref, kind, v), e.values)
        end
    elseif e isa Union{Tables.AndExpr,Tables.OrExpr}
        foreach(a -> _checkliterals(a, fields, names), e.args)
    elseif e isa Tables.NotExpr
        _checkliterals(e.arg, fields, names)
    end
    return nothing
end

function _checkliteral(f::AC.Field, ref, kind::Symbol, value)
    literal = _filterkind(typeof(value))
    (literal === nothing || literal === kind) && return nothing
    throw(
        ArgumentError(
            "the filter compares column $ref, which holds $(_kindvalues(f, kind)), " *
            "with $(repr(value))::$(typeof(value))",
        ),
    )
end

_kindvalues(f::AC.Field, kind::Symbol) =
    kind === :string ? "strings" : "$(Base.nonmissingtype(_declaredeltype(f))) values"

"""
One scan request resolved against one Arrow schema. `names` is the owned
source schema identity used by every executor; `bound` is the request, which
filters and converts public values.
"""
struct _ScanPlan
    names::Vector{Symbol}
    bound::Tables.BoundScan
end

function _deduplicatesorted!(values::Vector)
    isempty(values) && return values
    # Stability is irrelevant here. QuickSort avoids the input-sized scratch
    # vector used by Julia's stable default sort.
    sort!(values; alg=Base.Sort.QuickSort)
    writeidx = 1
    for readidx = 2:length(values)
        values[readidx] == values[writeidx] && continue
        writeidx += 1
        values[writeidx] = values[readidx]
    end
    resize!(values, writeidx)
    return values
end

# Not inferred: Tables resolves untyped selections (see `_uninferred`).
_resolvescan(scan::Tables.Scan, names) =
    _uninferred(Tables.resolve, scan, names)::Tables.BoundScan

function _ScanPlan(scan::Tables.Scan, fields, budget=nothing)
    # Unique names (`_fieldnamesymbols`), so every column has one reference.
    names = _fieldnamesymbols(fields, budget)
    b = _zonedbound(_resolvescan(scan, names), fields, names)
    _checkliterals(b.filter, fields, names)
    return _ScanPlan(names, b)
end

"Column table whose schema does not become part of its Julia type."
struct _ScanColumns
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    nrows::Int
end
Tables.istable(::Type{_ScanColumns}) = true
Tables.columnaccess(::Type{_ScanColumns}) = true
Tables.columns(t::_ScanColumns) = t
Tables.columnnames(t::_ScanColumns) = getfield(t, :names)
Tables.getcolumn(t::_ScanColumns, i::Int) = getfield(t, :columns)[i]
function Tables.getcolumn(t::_ScanColumns, name::Symbol)
    i = findfirst(==(name), getfield(t, :names))
    i === nothing && throw(
        ArgumentError("no column $(_uninferred(repr, name)::String) in this scan result"),
    )
    return getfield(t, :columns)[i]
end
Tables.rowcount(t::_ScanColumns) = getfield(t, :nrows)
function Tables.schema(t::_ScanColumns)
    names = getfield(t, :names)
    types = Type[eltype(c) for c in getfield(t, :columns)]
    # Preserve the compact typed schema for ordinary narrow results. Wide
    # schemas stay in values so compiler work cannot scale with input names.
    return Tables.Schema(names, types; stored=length(names) > _MAX_TYPED_SCHEMA_FIELDS)
end
Base.propertynames(t::_ScanColumns) = getfield(t, :names)
Base.getproperty(t::_ScanColumns, name::Symbol) = Tables.getcolumn(t, name)

# The pieces of one column, in order, as one column: the piece itself when there
# is one, string pieces as one string column over all their buffers, pieces of
# one dictionary-column type as a column of several record batches, or else a
# `Vector` of their element types' join.
function _joinparts(parts::Vector, budget=nothing)
    length(parts) == 1 && return parts[1]
    P = typeof(first(parts))
    # The pieces' types are known at run time; see `_uninferred`.
    P <: DictEncoded &&
        all(part -> part isa P, parts) &&
        return _uninferred(_chain, P, parts)
    T = _eltypeof(first(parts))
    for part in Iterators.drop(parts, 1)
        T = Base.promote_typejoin(T, _eltypeof(part))
    end
    total = sum(length, parts; init=0)
    if all(part -> part isa _StringColumn, parts)
        _chargevector!(budget, _StringPayload, total, "joined column")
        return _uninferred(_joinstrings, T, parts)
    end
    _chargevector!(budget, T, total, "joined column")
    return _uninferred(_copyparts!, Vector{T}(undef, total), parts)
end

_chain(::Type{P}, parts) where {P} = ChainedVector(P[parts...])

function _copyparts!(out::Vector, parts)
    offset = 0
    for part in parts
        copyto!(out, offset + 1, part, firstindex(part), length(part))
        offset += length(part)
    end
    return out
end

function _addscanrows(total::Int, rows::Int64)
    (0 <= total && 0 <= rows && rows <= typemax(Int) - total) ||
        throw(ValidationError("scan result row count is not addressable"))
    return total + Int(rows)
end

# ---------------------------------------------------------------------------
# Scan pushdown over a whole file
# ---------------------------------------------------------------------------

"""
Exact batch windowing for consumed `limit`/`offset`: per surviving batch,
how many leading rows to drop and how many to keep. Batches wholly outside
the window are absent — never decoded.
"""
function _batchwindow(
    rowcounts::Vector{Int64},
    offset::Int,
    limit::Union{Nothing,Int},
    budget=nothing,
)
    # Count first so a narrow limit does not reserve one entry for every
    # batch. The second pass allocates the exact logical window once.
    count = 0
    remaining_skip = Int64(offset)
    unlimited = limit === nothing
    remaining_take = unlimited ? Int64(0) : Int64(limit)
    for rows in rowcounts
        !unlimited && remaining_take <= 0 && break
        if remaining_skip >= rows
            remaining_skip -= rows
            continue
        end
        take =
            unlimited ? rows - remaining_skip : min(rows - remaining_skip, remaining_take)
        count += 1
        unlimited || (remaining_take -= take)
        remaining_skip = 0
    end
    Entry = Tuple{Int,Int64,Int64}
    _chargevector!(budget, Entry, count, "scan batch window")
    window = Entry[]   # (batch index, skip, take)
    sizehint!(window, count)
    remaining_skip = Int64(offset)
    unlimited = limit === nothing
    remaining_take = unlimited ? Int64(0) : Int64(limit)
    for (i, rows) in enumerate(rowcounts)
        !unlimited && remaining_take <= 0 && break
        if remaining_skip >= rows
            remaining_skip -= rows
            continue
        end
        take =
            unlimited ? rows - remaining_skip : min(rows - remaining_skip, remaining_take)
        push!(window, (i, remaining_skip, take))
        if !unlimited
            remaining_take -= take
        end
        remaining_skip = 0
    end
    return window
end

# ===========================================================================
# Byte-range reads — SourceFile over an AbstractArrowSource, the planner,
# and sparse decode
# ===========================================================================

"""
    SourceFile(src::AbstractArrowSource; limits=Limits(), tailbytes=65536, gapbytes=262144)

A handle that reads an Arrow IPC file from `src` by byte range, so a scan or
an `Arrow.Stream` fetches only the parts it needs. Internal, not part of the
public API: pass the source itself to `Arrow.Table` or `Arrow.Stream`.

- `limits`: the [`Arrow.Limits`](@ref) every read through the handle checks.
- `tailbytes`: the size of the first read, from the end of the object, which
  normally holds the footer (at least 32).
- `gapbytes`: how many unrequested bytes a read may include to save a request.
  Ranges at most this far apart are read together, and a scan reads a
  batch's body with its metadata when the body is at most four `gapbytes`
  (otherwise its first `gapbytes`). `gapbytes=0` reads only the bytes asked
  for, in more requests.
"""
# The footer is the only schema authority: a ranged read does not fetch the
# leading magic, cross-check the leading schema message, or look for the EOS
# marker, and the tail and coalesced reads may include bytes nobody asked for.
# The footer's Block index and the global feature and message limits are
# checked up front; per-record limits stay lazy, and every surviving
# candidate's metadata plan is validated before any planned body range is
# requested. Coalesced spans stop at 8 MiB. `limits.max_concurrent_reads`
# caps the source's preferred concurrency, and one semaphore is shared by
# every read on the handle. The source's length is read once, at construction.
struct SourceFile{S<:AbstractArrowSource}
    src::S
    len::Int64
    limits::Limits
    tailbytes::Int64
    gapbytes::Int64
    # The last `tailbytes` of the object, read once and reused by every
    # footer parse and format probe on this handle: `(bytes, tailstart)`.
    tail::Base.RefValue{Union{Nothing,Tuple{Vector{UInt8},Int64}}}
    # A Footer that did not fit in the tail window, read once as
    # `(footerstart, bytes)` and reused by every footer parse on this handle.
    footer::Base.RefValue{Union{Nothing,Tuple{Int64,Vector{UInt8}}}}
    concurrency::Int
    readsem::Base.Semaphore
    cachelock::ReentrantLock
end
function SourceFile(
    src::AbstractArrowSource;
    limits::Limits=Limits(),
    tailbytes::Integer=65536,
    gapbytes::Integer=262144,
)
    _validatelimits(limits)
    gap = Int64(gapbytes)
    gap >= 0 || throw(ArgumentError("gapbytes must not be negative, got $gapbytes"))
    reported = sourcelength(src)
    (reported isa Integer && reported >= 0 && reported <= typemax(Int64)) ||
        throw(ValidationError("source reports an invalid length $(repr(reported))"))
    preference = concurrentreads(src)
    (preference isa Integer && preference >= 1 && preference <= typemax(Int)) || throw(
        ValidationError(
            "source reports an invalid concurrent-read preference $(repr(preference))",
        ),
    )
    concurrency = min(Int(preference), limits.max_concurrent_reads)
    return SourceFile(
        src,
        Int64(reported),
        limits,
        Int64(max(tailbytes, 32)),
        gap,
        Base.RefValue{Union{Nothing,Tuple{Vector{UInt8},Int64}}}(nothing),
        Base.RefValue{Union{Nothing,Tuple{Int64,Vector{UInt8}}}}(nothing),
        concurrency,
        Base.Semaphore(concurrency),
        ReentrantLock(),
    )
end

# The object's tail window (at most `tailbytes`, the whole object when it is
# shorter), fetched on first use and cached on the handle.
function _fetchtail(sf::SourceFile, budget::Union{Nothing,AllocationBudget}=nothing)
    lock(sf.cachelock)
    try
        cached = sf.tail[]
        cached === nothing || return cached
        tailstart = max(Int64(0), sf.len - sf.tailbytes)
        _chargevector!(budget, UInt8, sf.len - tailstart, "tail range fetch")
        tail = _fetchexact(sf, tailstart, sf.len - tailstart)
        sf.tail[] = (tail, tailstart)
        return (tail, tailstart)
    finally
        unlock(sf.cachelock)
    end
end

# A Footer that escapes the tail window: one exact read, cached on the handle
# so the schema pass and the scan pass share it.
function _fetchfooter(
    sf::SourceFile,
    footerstart::Int64,
    footerlen::Int64,
    budget::AllocationBudget,
)
    lock(sf.cachelock)
    try
        cached = sf.footer[]
        if cached !== nothing && cached[1] == footerstart && length(cached[2]) == footerlen
            return cached[2]
        end
        _chargevector!(budget, UInt8, footerlen, "footer range fetch")
        bytes = _fetchexact(sf, footerstart, footerlen)
        sf.footer[] = (footerstart, bytes)
        return bytes
    finally
        unlock(sf.cachelock)
    end
end

# Whether the object is an IPC FILE (trailing `ARROW1` magic) — a stream
# object has no footer and is read whole instead of range-planned.
function _isfilesource(sf::SourceFile, budget::Union{Nothing,AllocationBudget}=nothing)
    tail, _ = _fetchtail(sf, budget)
    return length(tail) >= 6 && tail[(end - 5):end] == Vector{UInt8}(FILE_MAGIC)
end

# A long read (a whole object, a record batch, a stream message) fetches in
# ranges of this size, `sf.concurrency` at a time, so a source with concurrent
# reads downloads in parallel and holds at most that many ranges besides the
# result.
const _READ_RANGE = Int64(8 * 1024 * 1024)

# Bytes `[off, off + len)` of the source into `out` from index `at`: in
# `_READ_RANGE` ranges at once, or as one range from a source that reads one
# at a time.
function _fetchinto!(out::Vector{UInt8}, at::Int, sf::SourceFile, off::Int64, len::Int64)
    step = sf.concurrency == 1 ? max(len, Int64(1)) : _READ_RANGE
    _foreach(Int(cld(len, step)), sf.concurrency) do i, _
        o = (i - 1) * step
        n = min(step, len - o)
        copyto!(out, at + o, _fetchexact(sf, off + o, n), 1, n)
        return nothing
    end
    return out
end

# The whole object as one buffer, reusing the cached tail for its final window.
function _wholeobject(sf::SourceFile, budget::Union{Nothing,AllocationBudget}=nothing)
    tail, tailstart = _fetchtail(sf, budget)
    tailstart == 0 && return tail
    _chargevector!(budget, UInt8, sf.len, "whole-object buffer")
    _chargevector!(
        budget,
        UInt8,
        min(sf.concurrency * _READ_RANGE, tailstart),
        "range fetches",
    )
    out = Vector{UInt8}(undef, sf.len)
    copyto!(out, tailstart + 1, tail, 1, length(tail))
    return _fetchinto!(out, 1, sf, Int64(0), tailstart)
end

"""
Reads footer blocks for a pass in file order: a block read right after the
one before it comes with the blocks that follow it, up to `_READ_RANGE`
bytes in all, and those are then served from that window. While a window
(or a block larger than one) is in use, the next window is already being
fetched. A pass over small blocks makes one request per window instead of
one per block; a block read out of order is fetched alone. At most one
window is held and one more is in flight.
"""
mutable struct _BlockReader
    const sf::SourceFile
    const lock::ReentrantLock
    # Where the last block served ended, and the window's start and bytes.
    next::Int64
    start::Int64
    window::Vector{UInt8}
    # The window being fetched ahead: its start and end offsets, the index of
    # its last block, and the task fetching it.
    pending::Union{Nothing,Tuple{Int64,Int64,Int,Task}}
end
_BlockReader(sf::SourceFile) =
    _BlockReader(sf, ReentrantLock(), Int64(-1), Int64(0), UInt8[], nothing)

"""
A file-format source read one record batch at a time (`Arrow.Stream` over an
`AbstractArrowSource`): the footer and every dictionary batch when opened,
then each record batch's bytes when the batch is read.
"""
struct _RangedFile
    sf::SourceFile
    schema::Schema
    fields::AC.FrozenVector{Field}
    fielddictids::IdDict{Field,Int64}
    dictionaries::Dict{Int64,ArrayData}
    validated::AC._ValidatedDictionaries
    recordblocks::Vector{NTuple{3,Int64}}
    dataend::Int64
    limits::Limits
    footerversion::Int16
    # The stream's lifetime authority; each batch's bytes are their own region.
    region::OwnerRegion
    blocks::_BlockReader
end

function _RangedFile(sf::SourceFile, budget::AllocationBudget)
    ft = _readfooter(sf, sf.limits, budget)
    dictreader = _BlockReader(sf)
    framed(i, block) =
        let fm = first(_blockframe(dictreader, ft.dictblocks, i, ft.footerstart, budget))
            _filemetadataversion(ft.version, fm.version)
            fm
        end
    dicts, validated = _filedictionaries(
        framed,
        ft.dictblocks,
        ft.dictids,
        ft.dictvaluefields,
        ft.fielddictids,
        sf.limits,
        budget,
    )
    return _RangedFile(
        sf,
        ft.sch,
        AC.FrozenVector{Field}(ft.fields),
        ft.fielddictids,
        dicts,
        validated,
        ft.recordblocks,
        ft.footerstart,
        sf.limits,
        ft.version,
        heapregion(UInt8[]),
        _BlockReader(sf),
    )
end

# The bytes of `blocks[i]`, from the window, the window fetched ahead, or
# fetched now.
function _readblock(br::_BlockReader, blocks, i::Int, dataend::Int64)
    offset, frameend = _blockextent(blocks[i], dataend)
    ahead = nothing
    sequential = false
    @lock br.lock begin
        if br.start <= offset && frameend <= br.start + length(br.window)
            br.next = frameend
            return br.window[(offset - br.start + 1):(frameend - br.start)]
        end
        sequential = i == 1 || 0 <= offset - br.next <= br.sf.gapbytes
        p = br.pending
        if p !== nothing && p[1] == offset
            ahead = p
            br.pending = nothing
        end
    end
    if ahead === nothing
        stop, last = sequential ? _windowfrom(blocks, i, dataend) : (frameend, i)
        bytes = _fetchinto!(
            Vector{UInt8}(undef, stop - offset),
            1,
            br.sf,
            offset,
            stop - offset,
        )
    else
        _, stop, last, task = ahead
        bytes = try
            fetch(task)::Vector{UInt8}
        catch e
            rethrow(_firstcause(e))
        end
    end
    @lock br.lock begin
        br.next = frameend
        if sequential && last < length(blocks) && br.pending === nothing
            nstart, _ = _blockextent(blocks[last + 1], dataend)
            nstop, nlast = _windowfrom(blocks, last + 1, dataend)
            sf = br.sf
            task = errormonitor(
                Threads.@spawn _fetchinto!(
                    Vector{UInt8}(undef, nstop - nstart),
                    1,
                    sf,
                    nstart,
                    nstop - nstart,
                )
            )
            br.pending = (nstart, nstop, nlast, task)
        end
        stop == frameend && return bytes
        br.start, br.window = offset, bytes
    end
    return bytes[1:(frameend - offset)]
end

# The end offset and last block index of the window that starts at block
# `i`: the blocks after it while all of them fit in `_READ_RANGE` bytes.
function _windowfrom(blocks, i::Int, dataend::Int64)
    offset, stop = _blockextent(blocks[i], dataend)
    j = i
    while j < length(blocks)
        o, e = _blockextent(blocks[j + 1], dataend)
        (o >= stop && e - offset <= _READ_RANGE) || break
        stop = e
        j += 1
    end
    return stop, j
end

# A footer block fetched and framed as a message of its own: the message and
# the region holding the block's bytes.
function _blockframe(br::_BlockReader, blocks, i::Int, dataend::Int64, budget)
    region = heapregion(_readblock(br, blocks, i, dataend))
    _, metalen, bodylen = blocks[i]
    fm = _blockmessage(
        region,
        (Int64(0), metalen, bodylen),
        region.len,
        br.sf.limits,
        budget,
    )
    return fm, region
end

Base.length(r::_RangedFile) = length(r.recordblocks)

# Row count of batch `i` from one range request for its metadata.
_batchrows(r::_RangedFile, i::Int, budget::AllocationBudget) =
    _rangedbatchrows(r.sf, r.recordblocks[i], r.footerversion, r.fields, budget)

"""
A forward `IO` over a `SourceFile`, for reading a stream-format object one
message at a time: each fetch covers the bytes asked for plus a readahead
window that serves the next short reads.
"""
mutable struct _SourceIO <: IO
    const sf::SourceFile
    pos::Int64
    window::Vector{UInt8}
    windowstart::Int64
    # The readahead of the next short read: it doubles from
    # `_SOURCE_READAHEAD` to `_READ_RANGE`, so a long pass over small messages
    # makes few requests and a short one over-reads little.
    ahead::Int64
end
_SourceIO(sf::SourceFile) = _SourceIO(sf, Int64(0), UInt8[], Int64(0), _SOURCE_READAHEAD)

const _SOURCE_READAHEAD = Int64(64 * 1024)

Base.eof(io::_SourceIO) = io.pos >= io.sf.len
function Base.unsafe_read(io::_SourceIO, p::Ptr{UInt8}, n::UInt)
    want = Int64(n)
    want <= io.sf.len - io.pos || throw(EOFError())
    windowend = io.windowstart + length(io.window)
    buffered =
        io.windowstart <= io.pos < windowend ? min(windowend - io.pos, want) : Int64(0)
    if buffered > 0
        from = Int(io.pos - io.windowstart) + 1
        GC.@preserve io unsafe_copyto!(p, pointer(io.window, from), buffered)
        io.pos += buffered
    end
    rest = want - buffered
    rest == 0 && return nothing
    # One fetch covers the rest and the next window (a body and the next
    # message's prefix and metadata, for a stream).
    # A read shorter than one `_READ_RANGE` brings the readahead and doubles
    # it; a longer one (a large message) brings only the base readahead.
    short = rest < _READ_RANGE
    len = min(rest + (short ? io.ahead : _SOURCE_READAHEAD), io.sf.len - io.pos)
    short && (io.ahead = min(2 * io.ahead, _READ_RANGE))
    fetched = _fetchinto!(Vector{UInt8}(undef, len), 1, io.sf, io.pos, len)
    GC.@preserve fetched unsafe_copyto!(p + buffered, pointer(fetched), rest)
    io.pos += rest
    # Keep only the readahead, not a copy of the bytes just delivered.
    io.window = fetched[(rest + 1):end]
    io.windowstart = io.pos
    return nothing
end

# One exact range through the source, bounds-checked against the length
# read at construction and length-checked on return; a range the cached
# tail window already covers is served from it without a request.
function _fetchexact(sf::SourceFile, off::Int64, len::Int64)
    (off >= 0 && len >= 0 && off <= sf.len - len) ||
        throw(ValidationError("range fetch [$off, $len] escapes the object"))
    lock(sf.cachelock)
    try
        cached = sf.tail[]
        if cached !== nothing
            tail, tailstart = cached
            if off >= tailstart && off + len <= tailstart + length(tail)
                return tail[(off - tailstart + 1):(off - tailstart + len)]
            end
        end
    finally
        unlock(sf.cachelock)
    end
    Base.acquire(sf.readsem)
    got = try
        readrange(sf.src, off, len)
    finally
        Base.release(sf.readsem)
    end
    got isa Vector{UInt8} ||
        throw(ValidationError("readrange must return a Vector{UInt8}, got $(typeof(got))"))
    length(got) == len ||
        throw(ValidationError("range fetch returned $(length(got)) bytes, expected $len"))
    return got
end

"""
Merge sorted ranges whose gap is at most `gap`: a small over-read is usually
cheaper than another request round-trip. Ranges that do not overlap are not
merged past `maxspan` bytes, so a long run of them is fetched in several
requests at once. Returns file-coordinate spans.
"""
function _coalesce(
    ranges::Vector{NTuple{2,Int64}},
    gap::Int64,
    budget=nothing,
    maxspan::Int64=typemax(Int64),
)
    gap >= 0 || throw(ArgumentError("negative coalesce gap"))
    all(r -> r[1] >= 0 && r[2] >= 0, ranges) ||
        throw(ValidationError("negative range offset or length"))
    nonempty = count(r -> r[2] > 0, ranges)
    if nonempty == 0
        _chargevector!(budget, NTuple{2,Int64}, 0, "coalesced range plan")
        return NTuple{2,Int64}[]
    end
    _chargevector!(budget, NTuple{2,Int64}, nonempty, "coalesced range plan")
    # `sort` owns the one output vector. Compact it in place instead of
    # building a second range vector whose capacity also scales with input.
    sorted = Vector{NTuple{2,Int64}}(undef, nonempty)
    next = 0
    for range in ranges
        range[2] == 0 && continue
        next += 1
        sorted[next] = range
    end
    # Range order has no equal-key stability requirement. Keep the only
    # input-sized allocation the explicitly charged output vector above.
    sort!(sorted; alg=Base.Sort.QuickSort)
    outidx = 1
    for readidx = 2:length(sorted)
        off, len = sorted[readidx]
        loff, llen = sorted[outidx]
        loend = AC.checked_add(loff, llen)
        thisend = AC.checked_add(off, len)
        if off < loend || (off - loend <= gap && max(loend, thisend) - loff <= maxspan)
            sorted[outidx] = (loff, max(loend, thisend) - loff)
        else
            outidx += 1
            sorted[outidx] = (off, len)
        end
    end
    resize!(sorted, outidx)
    return sorted
end

"""
Fetched file-coordinate spans with their bytes, resolvable by containment.
`starts` is ascending. The spans of one fetch round are disjoint
(`_coalesce`); a scan's body round can overlap its metadata round
(`_unionspans`), so `_spanslice` also checks earlier spans.
"""
struct FetchedSpans
    starts::Vector{Int64}
    lens::Vector{Int64}
    slices::Vector{BufferSlice}
end

# How much of a body of `bodylen` bytes a block's metadata request also reads:
# all of it up to four `gapbytes`, otherwise one `gapbytes`' worth.
_speculativebody(bodylen::Int64, gap::Int64) =
    bodylen <= AC.checked_mul(gap, Int64(4)) ? bodylen : min(bodylen, gap)

function _fetchspans(
    sf::SourceFile,
    ranges::Vector{NTuple{2,Int64}},
    gap::Int64;
    budget::Union{Nothing,AllocationBudget}=nothing,
    what::AbstractString="range fetch",
)
    spans = _coalesce(ranges, gap, budget, _READ_RANGE)
    all(s -> s[1] >= 0 && s[2] >= 0 && s[1] <= sf.len - s[2], spans) ||
        throw(ValidationError("planned range escapes the object"))
    budget === nothing || foreach(s -> _chargevector!(budget, UInt8, s[2], what), spans)
    payloads = _readspans(sf, spans, budget)
    _chargevector!(budget, BufferSlice, length(payloads), "fetched range slices")
    if isempty(payloads)
        slices = BufferSlice[]
    else
        _chargeobject!(budget, sizeof(AC.ReleaseCell), "fetched range release state")
        ownerbytes = AC.checked_mul(
            Int64(length(payloads)),
            AC._materializedobjectbytes(sizeof(AC.OwnerRegion)),
        )
        budget === nothing || _charge!(budget, ownerbytes, "fetched range owners")
        cell = AC.ReleaseCell()
        slices = BufferSlice[
            BufferSlice(
                AC.OwnerRegion(Ptr{UInt8}(pointer(p)), sizeof(p); root=p, cell=cell),
                0,
                length(p),
            ) for p in payloads
        ]
    end
    _chargevector!(budget, Int64, length(spans), "fetched range starts")
    _chargevector!(budget, Int64, length(spans), "fetched range lengths")
    return FetchedSpans(Int64[s[1] for s in spans], Int64[s[2] for s in spans], slices)
end

# A region's bytes are all in memory: one span over the whole region serves
# every range a scan plans.
_fetchspans(r::OwnerRegion, ranges, gap::Int64; budget=nothing, what="") =
    FetchedSpans(Int64[0], Int64[r.len], BufferSlice[BufferSlice(r, 0, r.len)])

# Read one round's spans, each length-checked, into `results[i]` by request
# index. Small rounds read serially. Larger rounds use `sf.concurrency`
# workers that pull indexes off one atomic counter. Storing by request index
# means a source's completion order cannot permute payloads, and the worker
# count bounds the reads in flight whatever the span count.
function _readspans(sf::SourceFile, spans::Vector{NTuple{2,Int64}}, budget=nothing)
    n = length(spans)
    _chargevector!(budget, Vector{UInt8}, n, "range-fetch results")
    results = Vector{Vector{UInt8}}(undef, n)
    k = min(n, sf.concurrency)
    if k <= 1
        for i = 1:n
            results[i] = _fetchexact(sf, spans[i][1], spans[i][2])
        end
        return results
    end
    # Task and scheduler objects are fixed-size control allocations, but the
    # number of workers is input-directed. A conservative per-worker reserve
    # keeps that bounded by the same operation budget as fetched payloads.
    budget === nothing ||
        _charge!(budget, AC.checked_mul(Int64(k), Int64(4096)), "range-fetch workers")
    _chargeobject!(budget, sizeof(_SpanQueue), "range-fetch work queue")
    queue = _SpanQueue(0)
    try
        @sync for _ = 1:k
            errormonitor(Threads.@spawn _readworker!(results, sf, spans, queue))
        end
    catch e
        rethrow(_firstcause(e))
    end
    return results
end

# The shared request counter of one round's worker pool.
mutable struct _SpanQueue
    @atomic next::Int
end

# One worker: claim the next request index, read it into its slot, repeat.
function _readworker!(
    results::Vector{Vector{UInt8}},
    sf::SourceFile,
    spans::Vector{NTuple{2,Int64}},
    queue::_SpanQueue,
)
    n = length(spans)
    while true
        i = @atomic queue.next += 1
        i > n && return nothing
        results[i] = _fetchexact(sf, spans[i][1], spans[i][2])
    end
end

# The underlying exception of a failed worker task (`@sync` wraps it).
function _firstcause(e)
    e isa CompositeException && !isempty(e) && return _firstcause(first(e))
    e isa TaskFailedException && return _firstcause(e.task.result)
    return e
end

function _spanslice(fs::FetchedSpans, off::Int64, len::Int64)
    len == 0 && return BufferSlice()
    i = _spanholding(fs, off, len)
    i == 0 && throw(ValidationError("required bytes [$off, $len] were not fetched"))
    return AC.subslice(fs.slices[i], off - fs.starts[i], len)
end

# The index of a span that holds `[off, off + len)`, or 0.
function _spanholding(fs::FetchedSpans, off::Int64, len::Int64)
    stop = AC.checked_add(off, len)
    i = searchsortedlast(fs.starts, off)
    while i >= 1
        stop <= AC.checked_add(fs.starts[i], fs.lens[i]) && return i
        i -= 1
    end
    return 0
end

# Add the spans of a later fetch round to `fs`, keeping them ordered by start.
function _addspans!(fs::FetchedSpans, new::FetchedSpans, budget)
    n = length(new.starts)
    _chargevector!(budget, Int64, 2n, "fetched span index")
    _chargevector!(budget, BufferSlice, n, "fetched span slices")
    for k = 1:n
        at = searchsortedlast(fs.starts, new.starts[k]) + 1
        insert!(fs.starts, at, new.starts[k])
        insert!(fs.lens, at, new.lens[k])
        insert!(fs.slices, at, new.slices[k])
    end
    return fs
end

# Forget the spans inside `[from, to)`, so their bytes can be freed.
function _dropspans!(fs::FetchedSpans, from::Int64, to::Int64)
    keep = [!(from <= s && s + l <= to) for (s, l) in zip(fs.starts, fs.lens)]
    keepat!(fs.starts, keep)
    keepat!(fs.lens, keep)
    keepat!(fs.slices, keep)
    return fs
end

"""
    SparseBody

Stands in for a contiguous message body when only planned buffer windows
were fetched. Every declared buffer must resolve inside a fetched span that
was itself derived from the verified buffer table — the message-body
authority invariant, sparse.
"""
struct SparseBody
    bodylen::Int64
    bodystart::Int64          # file coordinate of the body's first byte
    spans::FetchedSpans
end

function _bodyslice(sb::SparseBody, offset::Int64, len::Int64)
    (offset >= 0 && len >= 0 && offset <= sb.bodylen - len) ||
        throw(ArgumentError("batch buffer escapes its message body"))
    len == 0 && return BufferSlice()
    return _spanslice(sb.spans, AC.checked_add(sb.bodystart, offset), len)
end

"""
Ids of every dictionary inside the masked top-level subtrees, including the
dictionaries a dictionary's values are encoded with.
"""
function _neededdictids(
    fields,
    fielddictids,
    dictvaluefields,
    mask::AbstractVector{Bool},
    budget=nothing,
)
    _chargedict!(budget, Int64, Nothing, length(fielddictids), "scan dictionary-id set")
    ids = Set{Int64}()
    function walk(f::Field)
        if f.type isa DictionaryType
            id = fielddictids[f]
            id in ids && return
            push!(ids, id)
            walk(dictvaluefields[id])
            return
        end
        foreach(walk, f.children)
    end
    for (j, f) in enumerate(fields)
        mask[j] && walk(f)
    end
    return ids
end

# The last `n` bytes of the object, from its cached tail window.
function _lastbytes(sf::SourceFile, n::Int, budget)
    tail, _ = _fetchtail(sf, budget)
    return tail[(end - n + 1):end]
end

# The Footer's bytes: from the cached tail window when it holds them.
function _footerbytes(sf::SourceFile, start::Int64, len::Int64, budget)
    tail, tailstart = _fetchtail(sf, budget)
    start >= tailstart || return _fetchfooter(sf, start, len, budget)
    _chargevector!(budget, UInt8, len, "footer allocation")
    return tail[(start - tailstart + 1):(start - tailstart + len)]
end

"""
One record block's row count from one range request for its metadata, with
the frame discipline of a scan's metadata pass: extent bounds before the
fetch, the fetch and parse charged to the caller's cumulative budget,
`_blockmeta` framing (continuation prefix, declared length, verified graph,
body-length cross-check), header kind, footer-version agreement,
and compression rejection.
"""
function _rangedbatchrows(
    sf::SourceFile,
    block::NTuple{3,Int64},
    version::Int16,
    fields::AbstractVector{Field},
    budget::AllocationBudget,
)
    _, metalen, bodylen = block
    declared = metalen - 8
    _checkblocklengths(declared, bodylen, sf.limits)
    _chargevector!(budget, UInt8, metalen, "metadata range fetch")
    payload = _fetchexact(sf, block[1], metalen)
    msg, v, header_type, _ =
        _blockmeta(BufferSlice(heapregion(payload), 0, metalen), block, sf.limits, budget)
    header_type == UInt8(3) ||
        throw(ValidationError("footer record block is not a record batch"))
    _filemetadataversion(version, v)
    rejectexperimentalcompression(msg, v, header_type)
    return _recordbatchmeta(msg.header::Meta.RecordBatch, fields, sf.limits, bodylen)
end

"Whether an output keeps its source field: no override, or a no-op one."
_keepsfield(bc::Tables.BoundColumn, precol) =
    bc.type === nothing || _eltypeof(precol) <: Union{bc.type,Missing}

"""
The OUTPUT schema of a scan: bound source fields under their output names.
`precols` supplies each output's pre-override (facade-narrowed) column, so
override keep/drop follows the SAME actual-subtype decision the conversion
made: a no-op override keeps its retained field; a real conversion drops it
(writing the result back to Arrow then re-infers the column).
"""
function _boundschema(schema, sourcefields, b::Tables.BoundScan, precols, budget=nothing)
    schema === nothing && return nothing
    _chargevector!(budget, AC.Field, length(b.columns), "scan output fields")
    outfields = AC.Field[]
    sizehint!(outfields, length(b.columns))
    for (i, bc) in enumerate(b.columns)
        f = sourcefields[bc.index]
        i <= length(precols) && !_keepsfield(bc, precols[i]) && continue
        # Metadata and children are immutable Core values. Share them instead
        # of copying the same schema subtrees once per aliased selection.
        _chargeobject!(budget, sizeof(bc.name), "scan field name")
        outname = String(bc.name)
        _chargeobject!(budget, Base.elsize(Vector{AC.Field}), "scan output field")
        push!(outfields, AC.Field(outname, f.type, f.nullable, f.metadata, f.children))
    end
    _chargeobject!(budget, Base.elsize(Vector{AC.Schema}), "scan output schema")
    return AC.Schema(
        AC.FrozenVector{AC.Field}(outfields, nothing),
        schema.metadata,
        schema.endianness,
    )
end

# A select override of a time-of-day column with a `Dates.Period` type reads
# its stored ticks as periods (`_timeperiods`), so its values are not checked
# against a day's length.
_periodoverride(bc::Tables.BoundColumn, f::AC.Field) =
    bc.type !== nothing &&
    Base.nonmissingtype(bc.type) <: Dates.Period &&
    f.type isa AC.TimeType
# A select override of a `JuliaLang.Symbol` column with a string type reads its
# stored strings, so the read creates no Symbol (`Limits.max_new_symbols`).
function _symboloverride(bc::Tables.BoundColumn, f::AC.Field)
    bc.type === nothing && return false
    Base.nonmissingtype(bc.type) <: AbstractString || return false
    ext = _arrowtypesextension(f)
    return ext !== nothing && first(ext) == "JuliaLang.Symbol"
end
# The fields whose overrides read their stored values (see above); a scan
# with any reads them from storage, not through the whole-read columns.
_rawoverrides(b::Tables.BoundScan, fields) = AC.Field[
    fields[bc.index] for bc in b.columns if
    _periodoverride(bc, fields[bc.index]) || _symboloverride(bc, fields[bc.index])
]

# A time-of-day column's stored ticks, not checked against a day's length.
function _storedticks(f::AC.Field, d::AC.ArrayData, budget)
    S = _rawdeclaredbasetype(f.type)
    _chargeobject!(budget, _VIEW_OBJECT_BYTES, "column view")
    nullable = Missing <: _declaredeltype(f, false) || _viewnulls(f, d)
    return _primitive(nullable ? Union{Missing,S} : S, S, Nothing, d, budget)
end

# ===========================================================================
# The scan executor: one planner for IPC files, one executor for every source
# ===========================================================================

# One record batch a scan reads: its decode plan, over a region, fetched
# ranges, or a stream message's body, and the dictionaries in effect for it.
struct _ScanRecord{B}
    plan::_RecordPlan{B}
    dictionaries::Dict{Int64,ArrayData}
end

# What decoding one source's record batches needs besides their plans.
struct _Decoder{F<:AbstractVector{Field}}
    fields::F
    fielddictids::IdDict{Field,Int64}
    validated::AC._ValidatedDictionaries
    limits::Limits
end

_decodefield(dec::_Decoder, r::_ScanRecord, j::Int, state::DecodeState) = _decodeplanned(
    r.plan,
    j,
    dec.fields[j],
    r.dictionaries,
    dec.fielddictids,
    dec.limits,
    dec.validated,
    state,
)

_rowcount(r::_ScanRecord) = Int(r.plan.rblen)

_columndictionary(dec::_Decoder, r::_ScanRecord, j::Int) =
    get(r.dictionaries, dec.fielddictids[dec.fields[j]], nothing)

# What building a scan's columns shares: view pools, ArrowTypes conversion
# (under `lock`: its caches are not thread-safe), and the fields whose
# overrides read stored values.
struct _ScanContext
    views::_ViewContext
    arrowtypes::_ArrowTypesContext
    lock::ReentrantLock
    rawoverrides::Vector{AC.Field}
end

_ScanContext(plan::_ScanPlan, fields, views::_ViewContext, arrowtypes) =
    _ScanContext(views, arrowtypes, ReentrantLock(), _rawoverrides(plan.bound, fields))

"""
One decoded batch column of a scan, in the public domain: the view a full
read gives, or the materialized column. A field in `ctx.rawoverrides` reads
its stored values instead (see `_periodoverride` and `_symboloverride`).
"""
function _scanbatchcolumn(f::AC.Field, d::AC.ArrayData, ctx::_ScanContext)
    if any(x -> x === f, ctx.rawoverrides)
        f.type isa AC.TimeType && return _storedticks(f, d, ctx.views.budget)
        return _storagebatchcolumn(f, d, ctx.views.budget)
    end
    _viewable(f) && return _viewcolumn(f, AC.ArrayData[d], ctx.views)
    return @lock ctx.lock _facadesinglecolumn(f, d, ctx.arrowtypes)
end

# Rows `rows` of batch column `d` of field `f`, in memory the result owns: a
# string or binary column copies just those values' bytes.
function _keptcolumn(f::AC.Field, d::AC.ArrayData, rows, ctx::_ScanContext)
    if _isstringlayout(f.type) && _viewable(f) && !any(x -> x === f, ctx.rawoverrides)
        return _rowsof(_vieweltype(f, AC.ArrayData[d], ctx.views), d, rows)
    end
    return _rowsof(_scanbatchcolumn(f, d, ctx), rows)
end

# The column a full read of no record batches gives field `f`.
_emptycolumn(f::AC.Field, ctx::_ScanContext) =
    _viewable(f) ? _viewcolumn(f, AC.ArrayData[], ctx.views) :
    @lock ctx.lock _facadecolumn(f, Any[], ctx.arrowtypes)

_filtermask(b::Tables.BoundScan, names, columns, n::Int) =
    _uninferred(Tables.filtermask, b, _ScanColumns(names, columns, n))::AbstractVector{Bool}

"""
The rows of a batch of `n` rows where `b`'s filter is `true`, evaluated over
its filter columns `columns` (named `names`) in chunks of rows on up to
`ntasks` tasks. A filter that reads no column is row-invariant: one
evaluation covers the batch.
"""
function _matchingrows(b::Tables.BoundScan, names, columns, n::Int, ntasks::Int)
    isempty(columns) && return _zerofieldpredicate(b.filter) === true ? (1:n) : (1:0)
    chunk = ntasks > 1 ? max(_STRING_CHUNK_ROWS, cld(n, ntasks)) : max(n, 1)
    nchunks = cld(n, chunk)
    nchunks <= 1 && return findall(_filtermask(b, names, columns, n))
    found = Vector{Vector{Int}}(undef, nchunks)
    _foreach(nchunks, ntasks) do c, _
        lo = (c - 1) * chunk + 1
        hi = min(c * chunk, n)
        sub = AbstractVector[view(col, lo:hi) for col in columns]
        found[c] = findall(_filtermask(b, names, sub, hi - lo + 1)) .+ (lo - 1)
        return nothing
    end
    return reduce(vcat, found)
end

# Decode fields `idxs` of record `r` into `cols` on up to `ntasks` tasks, task
# `w` with codec state `states[w]`.
function _decoderecord!(cols, dec::_Decoder, r, idxs, states, ntasks::Int)
    _foreach(length(idxs), ntasks) do q, w
        j = idxs[q]
        cols[j] = _decodefield(dec, r, j, states[w])
        return nothing
    end
    return cols
end

# Call `work(k, states, tasks)` for each record `k` in `ks` on up to `ntasks`
# tasks. With fewer records than tasks, each record gets several tasks (and
# that many codec states) for its own columns and rows.
function _eachrecord(work, ks, budget, ntasks::Int)
    isempty(ks) && return nothing
    outer = min(length(ks), ntasks)
    inner = max(1, ntasks ÷ outer)
    states = [DecodeState(budget) for _ = 1:(outer * inner)]
    try
        _foreach(length(ks), outer) do q, w
            work(ks[q], view(states, ((w - 1) * inner + 1):(w * inner)), inner)
            return nothing
        end
    finally
        foreach(close, states)
    end
    return nothing
end

_fieldmask(idxs, n::Int) = (m=falses(n); m[idxs].=true; m)

# Whether a scan keeps every row of every batch it reads.
_selectsonly(b::Tables.BoundScan) =
    b.filter === nothing && b.limit === nothing && b.offset == 0

"""
Read scan `plan` from `records`, the record batches it may keep, and return
its columns, row count, and the offset rows it skipped. Without a filter
every row of a batch is kept, or `windows[k]`, the rows of record `k` in the
scan's `offset`/`limit` window. A filter reads its filter columns first and
the other selected columns only for batches that keep rows; without a
`limit` or `offset`, batches run on up to `ntasks` tasks, and a batch with
tasks to spare evaluates the filter in chunks of rows. Kept rows are copied:
string and binary columns hold just their values' bytes. A scan that keeps
every row builds the columns a full read builds. `_fetch!(bodies, ks, mask)`
makes the bodies of the fields `mask` selects of records `ks` readable first.
"""
function _runscan(
    records::Vector,
    dec::_Decoder,
    plan::_ScanPlan,
    bodies,
    ctx::_ScanContext,
    budget::AllocationBudget;
    ntasks::Int,
    lazy::Bool,
    windows=nothing,
)
    b = plan.bound
    fields = dec.fields
    nf = length(fields)
    n = length(records)
    # The batches' rows together must fit an `Int`, whatever the scan keeps.
    total = 0
    for r in records
        total = _addscanrows(total, Int64(_rowcount(r)))
    end
    skipped = 0
    if b.filter === nothing && windows === nothing && !_selectsonly(b)
        windows = fill(1:0, n)
        rowcounts = Int64[_rowcount(r) for r in records]
        for (k, skip, take) in _batchwindow(rowcounts, b.offset, b.limit, budget)
            windows[k] = (Int(skip) + 1):(Int(skip) + Int(take))
        end
        skipped = min(b.offset, total)
    end
    outidx = Int[c.index for c in b.columns]
    filteridx = _deduplicatesorted!(copy(b.filtercols))
    decodeidx = _deduplicatesorted!(vcat(outidx, filteridx))
    if b.filter === nothing && windows === nothing && isempty(ctx.rawoverrides)
        _fetch!(bodies, 1:n, _fieldmask(decodeidx, nf))
        _chargevector!(budget, Union{Nothing,ArrayData}, n * nf, "batch columns")
        decoded = [Vector{Union{Nothing,ArrayData}}(nothing, nf) for _ = 1:n]
        m = length(decodeidx)
        states = [DecodeState(budget) for _ = 1:min(ntasks, max(1, n * m))]
        try
            _foreach(n * m, ntasks) do item, w
                k, q = divrem(item - 1, m) .+ (1, 1)
                j = decodeidx[q]
                decoded[k][j] = _decodefield(dec, records[k], j, states[w])
                return nothing
            end
        finally
            foreach(close, states)
        end
        batches = [(columns=c,) for c in decoded]
        columns = _buildcolumns(
            fields,
            batches,
            outidx,
            ctx.arrowtypes,
            ctx.views,
            ntasks;
            lazy=lazy,
        )
        return columns, total, 0
    end
    firstidx = b.filter === nothing ? decodeidx : filteridx
    restidx = setdiff(decodeidx, firstidx)
    firstmask, restmask = _fieldmask(firstidx, nf), _fieldmask(restidx, nf)
    filternames = Symbol[plan.names[i] for i in filteridx]
    parts = Matrix{Any}(nothing, length(outidx), n)
    kept = Vector{Any}(nothing, n)
    # A filter with a window composes it over the qualifying rows, in order.
    windowed = b.filter !== nothing && (b.limit !== nothing || b.offset > 0)
    skip = Ref(Int64(b.offset))
    take = Ref(b.limit === nothing ? Int64(-1) : Int64(b.limit))
    # Output columns of the fields `idxs` keep record `k`'s rows, from a filter
    # column already built (`built[i]`) or from the decoded batch.
    function gather!(k, cols, idxs, built=nothing)
        rows = kept[k]
        for (c, bc) in enumerate(b.columns)
            i = bc.index
            insorted(i, idxs) || continue
            parts[c, k] =
                built === nothing ? _keptcolumn(fields[i], cols[i]::ArrayData, rows, ctx) :
                _rowsof(built[i], rows)
        end
        return nothing
    end
    function first!(k, states, tasks)
        r = records[k]
        cols = _decoderecord!(Vector{Any}(nothing, nf), dec, r, firstidx, states, tasks)
        built = nothing
        rows = if b.filter === nothing
            windows === nothing ? (1:_rowcount(r)) : windows[k]
        else
            built = Vector{Any}(nothing, nf)
            for i in filteridx
                built[i] = _scanbatchcolumn(fields[i], cols[i], ctx)
            end
            filtercols = AbstractVector[built[i] for i in filteridx]
            _matchingrows(b, filternames, filtercols, _rowcount(r), tasks)
        end
        if windowed
            nskip = min(skip[], Int64(length(rows)))
            skip[] -= nskip
            ntake =
                take[] < 0 ? Int64(length(rows)) - nskip :
                min(take[], Int64(length(rows)) - nskip)
            take[] < 0 || (take[] -= ntake)
            rows = rows[(Int(nskip) + 1):(Int(nskip) + Int(ntake))]
        end
        isempty(rows) && return nothing
        kept[k] = rows
        gather!(k, cols, firstidx, built)
        return nothing
    end
    function rest!(k, states, tasks)
        cols = _decoderecord!(
            Vector{Any}(nothing, nf),
            dec,
            records[k],
            restidx,
            states,
            tasks,
        )
        gather!(k, cols, restidx)
        return nothing
    end
    if windowed
        states = [DecodeState(budget) for _ = 1:ntasks]
        try
            for k = 1:n
                take[] == 0 && break
                _fetch!(bodies, k:k, firstmask)
                first!(k, states, ntasks)
                if kept[k] !== nothing && !isempty(restidx)
                    _fetch!(bodies, k:k, restmask)
                    rest!(k, states, ntasks)
                end
                _release!(bodies, k)
            end
        finally
            foreach(close, states)
        end
    else
        ks = windows === nothing ? collect(1:n) : [k for k = 1:n if !isempty(windows[k])]
        _fetch!(bodies, ks, firstmask)
        _eachrecord(first!, ks, budget, ntasks)
        if !isempty(restidx)
            ks = [k for k in ks if kept[k] !== nothing]
            _fetch!(bodies, ks, restmask)
            _eachrecord(rest!, ks, budget, ntasks)
        end
    end
    columns = Vector{AbstractVector}(undef, length(outidx))
    for c in eachindex(outidx)
        ps = Any[p for p in view(parts, c, :) if p !== nothing]
        columns[c] =
            isempty(ps) ? _emptycolumn(fields[outidx[c]], ctx) : _joinparts(ps, budget)
    end
    nrows = 0
    for rows in kept
        rows === nothing || (nrows = _addscanrows(nrows, Int64(length(rows))))
    end
    windowed && (skipped = Int(b.offset - skip[]))
    return columns, nrows, skipped
end

"""
The Table of scan `plan` over `records`: `_runscan`'s columns under their
output names with the type overrides applied, the bound schema, and the
dictionary columns' category pools.
"""
function _scantable(
    records::Vector,
    dec::_Decoder,
    plan::_ScanPlan,
    bodies,
    ctx::_ScanContext,
    schema,
    regions,
    budget::AllocationBudget;
    ntasks::Int,
    lazy::Bool,
    windows=nothing,
)
    columns, nrows, skipped =
        _runscan(records, dec, plan, bodies, ctx, budget; ntasks, lazy, windows)
    b = plan.bound
    fields = dec.fields
    _chargevector!(budget, AbstractVector, length(columns), "pre-override scan columns")
    precols = AbstractVector[]
    sizehint!(precols, length(columns))
    for (i, bc) in enumerate(b.columns)
        f = fields[bc.index]
        col = columns[i]
        if _periodoverride(bc, f)
            columns[i] =
                _uninferred(_timeperiods, f.type, col, Base.nonmissingtype(bc.type))
            # A real conversion: the output does not keep the time field.
            push!(precols, Dates.Time[])
        elseif _symboloverride(bc, f)
            # The stored strings, without creating a Symbol per value.
            columns[i] = _uninferred(_applyoverride, bc.type, col, budget)
            push!(precols, Symbol[])
        else
            push!(precols, col)
            bc.type === nothing ||
                (columns[i] = _uninferred(_applyoverride, bc.type, col, budget))
        end
    end
    # A dictionary column keeps its category pool, as a full read does, so a
    # rewrite preserves category order (required when ordered).
    _chargevector!(budget, Any, length(b.columns), "scan retained dictionary pools")
    pools = Any[
        fields[bc.index].type isa DictionaryType && _keepsfield(bc, precols[i]) ?
        _retainedpool(fields[bc.index], _dictionariesof(dec, records, bc.index), ctx) :
        nothing for (i, bc) in enumerate(b.columns)
    ]
    names = Symbol[c.name for c in b.columns]
    bound = _boundschema(schema, fields, b, precols, budget)
    return _table(names, columns, bound, regions, nrows, pools, budget), skipped
end

# The distinct dictionaries of column `j` across `records`, in order.
function _dictionariesof(dec::_Decoder, records, j::Int)
    dictionaries = AC.ArrayData[]
    seen = Base.IdSet{AC.ArrayData}()
    for r in records
        d = _columndictionary(dec, r, j)
        (d === nothing || d in seen) && continue
        push!(seen, d)
        push!(dictionaries, d)
    end
    return dictionaries
end

_retainedpool(f::AC.Field, dictionaries, ctx::_ScanContext) =
    @lock ctx.lock _retainedpool(f, dictionaries, ctx.arrowtypes)

# The record bodies of a scan whose bytes are all in memory are readable.
_fetch!(::Nothing, ks, mask, extra=NTuple{2,Int64}[]) = nothing
_release!(::Nothing, k) = nothing

# What fetching a byte-range scan's record bodies needs: the bytes fetched so
# far, which fields' bodies of each record batch they hold, and each record
# batch's Block, header, variadic counts, and codec.
struct _RangedBodies
    sf::SourceFile
    spans::FetchedSpans
    fetched::BitMatrix   # [field, record]
    blocks::Vector{NTuple{3,Int64}}
    headers::Vector{Meta.RecordBatch}
    variadics::Vector{Vector{Int64}}
    codecs::Vector{Int8}
    fields::Vector{Field}
    budget::AllocationBudget
end

"""
Fetch the body ranges of the fields `mask` selects of record batches `ks`,
and the ranges `extra`, that `rb.spans` does not hold yet, in one coalesced
round. Every planned subtree's metadata is validated first, so a corrupt
buffer table fails before its ranges are requested.
"""
function _fetch!(rb::_RangedBodies, ks, mask, extra=NTuple{2,Int64}[])
    limits = rb.sf.limits
    count = Int64(length(extra))
    masks = BitVector[mask .& .!view(rb.fetched, :, k) for k in ks]
    for (k, m) in zip(ks, masks)
        any(m) || continue
        count = AC.checked_add(
            count,
            _validatebodyplan(
                rb.headers[k],
                rb.fields,
                limits,
                rb.codecs[k],
                m,
                rb.variadics[k],
            ),
        )
    end
    count <= typemax(Int) ||
        throw(ValidationError("body range count exceeds the host index range"))
    _chargevector!(rb.budget, NTuple{2,Int64}, count, "body range plan")
    ranges = Vector{NTuple{2,Int64}}(undef, Int(count))
    copyto!(ranges, extra)
    at = length(extra)
    for (k, mask) in zip(ks, masks)
        any(mask) || continue
        rb.fetched[:, k] .|= mask
        block = rb.blocks[k]
        buffers = something(rb.headers[k].buffers, Meta.Buffer[])
        varidx = Ref(1)
        bufidx = 1
        for (j, fld) in enumerate(rb.fields)
            span64 = _bufferspan(fld, rb.variadics[k], varidx)
            span64 <= typemax(Int) || throw(
                ValidationError("field buffer span $span64 exceeds the host index range"),
            )
            span = Int(span64)
            if mask[j]
                for q = bufidx:(bufidx + span - 1)
                    q <= length(buffers) || throw(
                        ValidationError(
                            "metadata declares fewer buffers than the schema requires",
                        ),
                    )
                    buf = buffers[q]
                    len = Int64(buf.length)
                    off = Int64(buf.offset)
                    (off >= 0 && len >= 0 && AC.checked_add(off, len) <= block[3]) || throw(
                        ValidationError(
                            "batch buffer [$off, $len] escapes its message body",
                        ),
                    )
                    len == 0 && continue
                    at += 1
                    ranges[at] = (block[1] + block[2] + off, len)
                end
            end
            bufidx += span
        end
    end
    at == length(ranges) ||
        throw(AssertionError("body range preflight count changed during planning"))
    _chargevector!(rb.budget, NTuple{2,Int64}, length(ranges), "unfetched body ranges")
    unfetched = filter(r -> _spanholding(rb.spans, r[1], r[2]) == 0, ranges)
    fetched = _fetchspans(
        rb.sf,
        unfetched,
        rb.sf.gapbytes;
        budget=rb.budget,
        what="body range fetch",
    )
    _addspans!(rb.spans, fetched, rb.budget)
    return nothing
end

# Record `k` is read: a scan that reads batch by batch frees its fetched bytes.
function _release!(rb::_RangedBodies, k)
    offset, metalen, bodylen = rb.blocks[k]
    _dropspans!(rb.spans, offset, offset + metalen + bodylen)
    return nothing
end

_gapbytes(sf::SourceFile) = sf.gapbytes
_gapbytes(::OwnerRegion) = Int64(0)

"""
Scan `plan` over the IPC file in `src`, a region (whose bytes are all in
memory) or a byte-range source, with verified footer `ft`. One pass reads
every Block's metadata: over a source, in coalesced ranges that also cover
the start of each body, or all of a small body. Row counts from it choose the
window; then the decode set's dictionaries decode in footer order, and
`_runscan` reads the record batches. Over a source, the dictionary bodies
and the first record bodies share one request round, and only the bodies of
the decode set's buffers are fetched.
"""
function _scanfile(
    src,
    ft,
    plan::_ScanPlan,
    limits::Limits,
    budget::AllocationBudget,
    views::_ViewContext,
    regions;
    ntasks::Int,
    lazy::Bool,
)
    b = plan.bound
    fields = ft.fields
    dictblocks, recordblocks = ft.dictblocks, ft.recordblocks
    ndict, nrec = length(dictblocks), length(recordblocks)
    nmeta = ndict + nrec
    _chargevector!(budget, NTuple{3,Int64}, nmeta, "scan metadata blocks")
    metablocks = vcat(dictblocks, recordblocks)
    for (_, metalen, bodylen) in metablocks
        _checkblocklengths(metalen - 8, bodylen, limits)
    end
    # Each block's metadata comes with the start of its body, or all of a small
    # body: the selected buffers often lie there, and an over-read of a few
    # hundred KiB costs less than a second request round. A scan with a row
    # window needs few of the bodies, so it reads metadata alone.
    gap = _gapbytes(src)
    limitedfilter = b.filter !== nothing && b.limit !== nothing
    bodygap = b.limit === nothing && b.offset == 0 ? gap : Int64(0)
    _chargevector!(budget, NTuple{2,Int64}, nmeta, "metadata range plan")
    metaranges = NTuple{2,Int64}[
        (bl[1], AC.checked_add(bl[2], _speculativebody(bl[3], bodygap))) for
        bl in metablocks
    ]
    # A filtered limit may end at its first batch. Coalescing across its
    # successors' bodies would download those rows before the filter runs.
    spans = _fetchspans(
        src,
        metaranges,
        limitedfilter ? Int64(0) : gap;
        budget=budget,
        what="metadata range fetch",
    )
    _chargevector!(budget, Tuple{Meta.Message,Int16}, nmeta, "parsed block metadata")
    msgs = Vector{Meta.Message}(undef, nmeta)
    versions = Vector{Int16}(undef, nmeta)
    headertypes = Vector{UInt8}(undef, nmeta)
    _foreach(nmeta, ntasks) do i, _
        block = metablocks[i]
        msg, v, ht, _ =
            _blockmeta(_spanslice(spans, block[1], block[2]), block, limits, budget)
        msgs[i], versions[i], headertypes[i] = msg, v, ht
        return nothing
    end
    for i = 1:nmeta
        isdict = i <= ndict
        headertypes[i] == (isdict ? UInt8(2) : UInt8(3)) || throw(
            ValidationError(
                isdict ? "footer dictionary block is not a dictionary batch" :
                "footer record block is not a record batch",
            ),
        )
        _filemetadataversion(ft.version, versions[i], versions[1])
        rejectexperimentalcompression(msgs[i], versions[i], headertypes[i])
    end
    # Row counts are in the metadata, not the Footer: a window needs them.
    _chargevector!(budget, Meta.RecordBatch, nrec, "record-batch headers")
    headers = Meta.RecordBatch[msgs[ndict + p].header::Meta.RecordBatch for p = 1:nrec]
    _chargevector!(budget, Vector{Int64}, nrec, "record variadic-count cache")
    variadics = Vector{Vector{Int64}}(undef, nrec)
    _chargevector!(budget, Int64, nrec, "scan batch row counts")
    rowcounts = Vector{Int64}(undef, nrec)
    _foreach(nrec, ntasks) do p, _
        variadics[p] = variadiccounts(headers[p])
        rowcounts[p] =
            _recordbatchmeta(headers[p], fields, limits, recordblocks[p][3], variadics[p])
        return nothing
    end
    # The file's rows together must fit an `Int`, whatever the scan keeps.
    total = 0
    for rows in rowcounts
        total = _addscanrows(total, rows)
    end
    window =
        b.filter === nothing && (b.limit !== nothing || b.offset > 0) ?
        _batchwindow(rowcounts, b.offset, b.limit, budget) :
        Tuple{Int,Int64,Int64}[(p, Int64(0), rowcounts[p]) for p = 1:nrec]
    decodemask = _fieldmask(
        _deduplicatesorted!(vcat(Int[c.index for c in b.columns], b.filtercols)),
        length(fields),
    )
    # The decode set's dictionaries decode whole; the others are checked from
    # their metadata only.
    needed =
        isempty(window) ? Set{Int64}() :
        _neededdictids(fields, ft.fielddictids, ft.dictvaluefields, decodemask, budget)
    _chargedict!(budget, Int64, Nothing, ndict, "seen dictionary ids")
    seenids = Set{Int64}()
    _chargevector!(budget, Int, ndict, "selected dictionary blocks")
    wanted = Int[]
    for (i, block) in enumerate(dictblocks)
        header = msgs[i].header::Meta.DictionaryBatch
        haskey(ft.dictids, header.id) ||
            throw(ValidationError("dictionary batch has unknown id $(header.id)"))
        _dictionarytransition(header.id, header.isDelta, header.id in seenids; file=true)
        push!(seenids, header.id)
        rb = header.data
        vf = ft.dictvaluefields[header.id]
        rbvariadics = variadiccounts(rb)
        _recordbatchmeta(rb, (vf,), limits, block[3], rbvariadics)
        if header.id in needed
            src isa SourceFile && _validatebodyplan(
                rb,
                (vf,),
                limits,
                _batchcodec(rb.compression, versions[i]),
                (true,),
                rbvariadics,
            )
            push!(wanted, i)
        end
    end
    for id in needed
        id in seenids || throw(
            ValidationError(
                "record batch references dictionary id $id before its dictionary batch",
            ),
        )
    end
    candidates = Int[p for (p, _, _) in window]
    bodies =
        src isa SourceFile ?
        _RangedBodies(
            src,
            spans,
            falses(length(fields), length(candidates)),
            recordblocks[candidates],
            headers[candidates],
            variadics[candidates],
            Int8[
                _batchcodec(headers[p].compression, versions[ndict + p]) for p in candidates
            ],
            fields,
            budget,
        ) : nothing
    # One round fetches the decode set's dictionary bodies and, unless a
    # filtered limit reads batch by batch, the bodies `_runscan` reads first.
    dictranges = NTuple{2,Int64}[
        (dictblocks[i][1] + dictblocks[i][2], dictblocks[i][3]) for
        i in wanted if dictblocks[i][3] > 0
    ]
    firstmask =
        b.filter === nothing ? decodemask :
        _fieldmask(_deduplicatesorted!(copy(b.filtercols)), length(fields))
    _fetch!(bodies, limitedfilter ? (1:0) : eachindex(candidates), firstmask, dictranges)
    _chargedict!(budget, Int64, ArrayData, length(wanted), "decoded dictionary table")
    dicts = Dict{Int64,ArrayData}()
    _chargedict!(
        budget,
        ArrayData,
        Nothing,
        length(wanted),
        "validated dictionary identities",
    )
    validated = AC._ValidatedDictionaries()
    state = DecodeState(budget)
    try
        for i in wanted
            block = dictblocks[i]
            header = msgs[i].header::Meta.DictionaryBatch
            vf = ft.dictvaluefields[header.id]
            decoded = _decodedictionary(
                header.data,
                _spanslice(spans, block[1] + block[2], block[3]),
                versions[i],
                vf,
                dicts,
                ft.fielddictids,
                limits,
                state,
            )
            _updatedictionary!(
                dicts,
                validated,
                header.id,
                header.isDelta,
                vf,
                decoded,
                limits,
                budget;
                file=true,
            )
        end
    finally
        close(state)
    end
    records = Vector{_ScanRecord{SparseBody}}(undef, length(candidates))
    _foreach(length(candidates), ntasks) do q, _
        p = candidates[q]
        block = recordblocks[p]
        header = headers[p]
        _scanmissingdicts(fields, header.nodes, dicts, ft.fielddictids, decodemask)
        body = SparseBody(block[3], block[1] + block[2], spans)
        records[q] = _ScanRecord(
            _recordplan(header, body, versions[ndict + p], fields, limits),
            dicts,
        )
        return nothing
    end
    windows =
        b.filter === nothing && !_selectsonly(b) ?
        UnitRange{Int}[
            (Int(skip) + 1):(Int(skip) + Int(take)) for (_, skip, take) in window
        ] : nothing
    dec = _Decoder(fields, ft.fielddictids, validated, limits)
    ctx = _ScanContext(plan, fields, views, _ArrowTypesContext(budget=budget))
    return _scantable(
        records,
        dec,
        plan,
        bodies,
        ctx,
        _uniqueschema(ft.sch),
        regions,
        budget;
        ntasks,
        lazy,
        windows,
    )
end

# Record batch `i` of a file-format stream source, planned to decode the
# fields `mask` selects, and what fetching its bodies needs (`nothing` when
# they are in memory).
_streamrecord(f::ArrowFile, i::Int, budget, mask) =
    _ScanRecord(_fileplan(f, i, budget, mask), f.dictionaries), nothing

# Over a byte-range source a batch that decodes every field reads its whole
# block, with read-ahead; one that decodes some reads its metadata with the
# start of its body, and `_runscan` fetches the planned ranges it lacks.
function _streamrecord(r::_RangedFile, i::Int, budget, mask)
    sf = r.sf
    block = r.recordblocks[i]
    spans = nothing
    if all(mask)
        fm = first(_blockframe(r.blocks, r.recordblocks, i, r.dataend, budget))
        msg, version, headertype, body = fm.msg, fm.version, fm.header_type, fm.body
    else
        range =
            (block[1], AC.checked_add(block[2], _speculativebody(block[3], sf.gapbytes)))
        spans = _fetchspans(
            sf,
            NTuple{2,Int64}[range],
            sf.gapbytes;
            budget=budget,
            what="metadata range fetch",
        )
        meta = _spanslice(spans, block[1], block[2])
        msg, version, headertype, _ = _blockmeta(meta, block, r.limits, budget)
        body = SparseBody(block[3], block[1] + block[2], spans)
    end
    _filemetadataversion(r.footerversion, version)
    headertype == UInt8(3) ||
        throw(ValidationError("footer record block is not a record batch"))
    rejectexperimentalcompression(msg, version, headertype)
    header = msg.header::Meta.RecordBatch
    variadics = variadiccounts(header)
    _recordbatchmeta(header, r.fields, r.limits, block[3], variadics)
    _scanmissingdicts(r.fields, header.nodes, r.dictionaries, r.fielddictids, mask)
    record =
        _ScanRecord(_recordplan(header, body, version, r.fields, r.limits), r.dictionaries)
    spans === nothing && return record, nothing
    bodies = _RangedBodies(
        sf,
        spans,
        falses(length(r.fields), 1),
        [block],
        Meta.RecordBatch[header],
        [variadics],
        Int8[_batchcodec(header.compression, version)],
        collect(Field, r.fields),
        budget,
    )
    return record, bodies
end

"""
Scan `plan` over the record batches of IPC stream `st` (`records`, planned as
`_streamrecords` frames them).
"""
function _scanstream(
    st::_StreamState,
    pending::Vector{PendingRecord},
    plan::_ScanPlan,
    budget::AllocationBudget,
    views::_ViewContext,
    regions;
    ntasks::Int,
    lazy::Bool,
)
    b = plan.bound
    records = _ScanRecord{BufferSlice}[_ScanRecord(p.plan, p.dictionaries) for p in pending]
    dec = _Decoder(st.fields, st.fielddictids, st.validated, st.limits)
    ctx = _ScanContext(plan, st.fields, views, _ArrowTypesContext(budget=budget))
    return _scantable(
        records,
        dec,
        plan,
        nothing,
        ctx,
        _uniqueschema(st.schema),
        regions,
        budget;
        ntasks,
        lazy,
    )
end

"Convert a column with the generic Tables.scan allocation contract."
function _applyoverride(::Type{T}, col, budget=nothing) where {T}
    eltype(col) <: Union{T,Missing} && return col
    anymissing = any(ismissing, col)
    E = anymissing ? Union{T,Missing} : T
    _chargevector!(budget, E, length(col), "scan type override")
    out = Tables.allocatecolumn(E, length(col))
    V = Base.nonmissingtype(T)
    @inbounds for i in eachindex(col)
        x = col[i]
        out[i] = ismissing(x) ? missing : convert(V, x)
    end
    return out
end
