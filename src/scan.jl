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
# Tables.Scan pushdown over the IPC file adapter, and — further down — the
# byte-range fetch protocol (`SourceFile` over an `AbstractArrowSource`) over
# the same bound column set. Design notes: docs/dev/DESIGN-scan-ranges-trim.md.
#
# Pushdown semantics: the request is resolved once, then the source consumes
# the bound scan exactly, batch by batch; type overrides apply after
# conversion to the public domain.
#
#   * the decode set is (selected ∪ filter-referenced) columns — everything
#     else is SKIPPED by `skipfield!`, a registry walk that consumes the
#     node/buffer accounting (all buffer-table invariants still checked)
#     without slicing, decompressing, validating, or materializing anything;
#   * without a filter `limit`/`offset` are metadata arithmetic —
#     `RecordBatch.length` is wire metadata, so batches outside the window
#     are never decoded; with a filter the `_ScanSink` evaluates it per
#     batch through the generic evaluator, composes the window over the
#     qualifying rows, and stops decoding once the window is full;
#   * the returned table holds the selected columns' surviving rows under
#     their output names, in selection order.
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
        spec.fixedwidth > 0 &&
            return _checkedmul(node.length, Int64(spec.fixedwidth), "planned data-buffer size")
        if spec.fixedwidth == -1
            return node.length ÷ 8 + (node.length % 8 == 0 ? 0 : 1)
        end
        return Int64(0)
    elseif role == AC.OFFSETS
        count = _checkedadd(node.length, Int64(1), "planned offset count")
        return _checkedmul(count, Int64(spec.offsetwidth), "planned offsets-buffer size")
    elseif role == AC.ELEMENT_OFFSETS || role == AC.SIZES
        return _checkedmul(node.length, Int64(spec.offsetwidth), "planned element-buffer size")
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
        _checkedmul(node.length, Int64(t.listsize), "fixed-size-list child length") : Int64(0)
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
The masked-decode core shared by the in-memory and ranged paths: masked-in
fields decode and validate exactly as `getindex`; masked-out fields advance
through `skipfield!`. The cursor must still finish clean — a skewed batch
fails identically either way. `body` is a `BufferSlice` or a `SparseBody`.
"""
function _maskedrecord(
    msg::Meta.Message,
    version::Int16,
    body,
    fields,
    dicts,
    fielddictids,
    validated,
    limits::Limits,
    schemaversion::Int16,
    mask::AbstractVector{Bool},
    state::DecodeState,
    suppliedvariadics::Union{Nothing,AbstractVector{Int64}}=nothing,
)
    version == schemaversion ||
        throw(ValidationError("IPC metadata version changes within the file"))
    rejectexperimentalcompression(msg, version, UInt8(3))
    header = msg.header
    header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    codec = _batchcodec(header.compression, version)
    variadics = suppliedvariadics === nothing ? variadiccounts(header) : suppliedvariadics
    rblen = _recordbatchmeta(
        header,
        fields,
        limits,
        body isa BufferSlice ? body.len : body.bodylen,
        variadics,
    )
    _scanmissingdicts(fields, header.nodes, dicts, fielddictids, mask)
    cursor = DecodeCursor(
        header.nodes,
        header.buffers,
        body,
        limits;
        codec=codec,
        state=state,
        variadics=variadics,
    )
    _chargevector!(
        state.budget,
        Union{Nothing,ArrayData},
        length(fields),
        "masked batch columns",
    )
    cols = Vector{Union{Nothing,ArrayData}}(nothing, length(fields))
    for (j, fld) in enumerate(fields)
        if mask[j]
            cols[j] = decodefield(fld, cursor, dicts, fielddictids)
        else
            skipfield!(fld, cursor)
        end
    end
    finishcursor!(cursor)
    for (j, fld) in enumerate(fields)
        col = cols[j]
        col === nothing && continue
        AC._validate_semantic(fld, col, validated)
        col.len == rblen || throw(
            ValidationError("RecordBatch length does not match top-level field nodes"),
        )
    end
    return rblen, cols
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

"Window arithmetic over a known row count (filter already evaluated)."
function _zerofieldcount(n0::Int64, keep, limit, offset)
    keep === true || return Int64(0)
    lo = min(Int64(offset), n0)
    n = n0 - lo
    limit === nothing ? n : min(n, Int64(limit))
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

# Join the pieces of one output column, one per batch, into one column of
# their element types' join; string pieces join into one string column.
function _joinscanparts(f::Field, parts::Vector, budget=nothing)
    if isempty(parts)
        # `_finishfacadescan` gives an empty column the type a full read
        # gives it.
        ET = _storageelementclaim(f)
        _chargevector!(budget, ET, 0, "empty scan column")
        return Vector{ET}()
    end
    length(parts) == 1 && return parts[1]
    total = sum(length, parts; init=0)
    T = _eltypeof(first(parts))
    for part in Iterators.drop(parts, 1)
        T = Base.promote_typejoin(T, _eltypeof(part))
    end
    if all(part -> part isa _StringColumn, parts)
        _chargevector!(budget, _StringPayload, total, "joined scan column")
        return _joinstrings(T, parts)
    end
    _chargevector!(budget, T, total, "joined scan column")
    out = Vector{T}(undef, total)
    offset = 0
    for part in parts
        copyto!(out, offset + 1, part, firstindex(part), length(part))
        offset += length(part)
    end
    return out
end

function _scantable(names, outcols, nrows::Int, budget=nothing)
    _chargevector!(budget, AbstractVector, length(names), "scan result columns")
    columns = AbstractVector[col for col in outcols]
    length(columns) == length(names) ||
        throw(AssertionError("scan result name/column count changed"))
    return _ScanColumns(names, columns, nrows)
end

function _addscanrows(total::Int, rows::Int64)
    (0 <= total && 0 <= rows && rows <= typemax(Int) - total) ||
        throw(ValidationError("scan result row count is not addressable"))
    return total + Int(rows)
end

# The fields `mask` selects of batch `i`, decoded on at most `ntasks` tasks.
function _scanbatch(f::ArrowFile, i::Int, mask::AbstractVector{Bool}, budget, ntasks::Int)
    plans, cols = _filebatches(f, (i,), budget, ntasks, mask)
    return only(plans).rblen, only(cols)
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

"""
The exact consumer of a bound scan, one decoded batch at a time. It
evaluates the filter over each batch's decoded columns through
`Tables.filtermask` (the executor's own three-valued semantics), composes
`offset`/`limit` over the qualifying rows with saturating arithmetic, and
stops the scan as soon as the window is full. It keeps only the selected
columns' surviving rows, in selection order, under their output names.
Without a filter, `_batchwindow` has already named the rows, so a batch
outside the window never decodes.
"""
mutable struct _ScanSink{M}
    const names::Vector{Symbol}
    const fields::Vector{Field}
    const columns::Vector{Tables.BoundColumn}
    const decodeidx::Vector{Int}
    const filteridx::Vector{Int}
    const filternames::Vector{Symbol}
    const bound::Tables.BoundScan
    const materializecolumn::M
    const budget::AllocationBudget
    const parts::Vector{Vector{Any}}
    const partcounts::Vector{Int}
    outrows::Int
    remaining_skip::Int64
    remaining_take::Int64   # -1 = unlimited
end

function _ScanSink(
    b::Tables.BoundScan,
    names::Vector{Symbol},
    fields,
    materializecolumn,
    budget::AllocationBudget,
)
    ndecode = length(b.columns) + length(b.filtercols)
    _chargevector!(budget, Int, ndecode, "scan decode-column index")
    decodeidx = Int[]
    sizehint!(decodeidx, ndecode)
    append!(decodeidx, (c.index for c in b.columns))
    append!(decodeidx, b.filtercols)
    _deduplicatesorted!(decodeidx)
    _chargevector!(budget, Int, length(b.filtercols), "scan filter-column index")
    filteridx = copy(b.filtercols)
    _deduplicatesorted!(filteridx)
    _chargevector!(budget, Symbol, length(filteridx), "scan filter names")
    filternames = Symbol[names[i] for i in filteridx]
    _chargevector!(budget, Field, length(fields), "scan source fields")
    copiedfields = collect(Field, fields)
    _chargevector!(budget, Vector{Any}, length(b.columns), "scan column-part lists")
    # Entries receive exact-capacity vectors once batch planning is complete.
    # One shared empty sentinel avoids one discarded empty Vector per column.
    _chargevector!(budget, Any, 0, "shared empty scan-part list")
    empty = Any[]
    parts = fill(empty, length(b.columns))
    _chargevector!(budget, Int, length(b.columns), "scan column-part counts")
    partcounts = zeros(Int, length(b.columns))
    # With a filter the window composes over qualifying rows here; without
    # one the metadata window has already applied it.
    filtered = b.filter !== nothing
    skip = filtered ? Int64(b.offset) : Int64(0)
    take = filtered && b.limit !== nothing ? Int64(b.limit) : Int64(-1)
    return _ScanSink{typeof(materializecolumn)}(
        names,
        copiedfields,
        b.columns,
        decodeidx,
        filteridx,
        filternames,
        b,
        materializecolumn,
        budget,
        parts,
        partcounts,
        0,
        skip,
        take,
    )
end

function _reservesinkparts!(sink::_ScanSink, capacity::Int)
    capacity >= 0 || throw(ArgumentError("negative scan part capacity"))
    capacity == 0 && return nothing
    for i in eachindex(sink.parts)
        _chargevector!(sink.budget, Any, capacity, "scan column parts")
        sink.parts[i] = Vector{Any}(undef, capacity)
    end
    return nothing
end

# Whether the sink still accepts rows: a filled limit ends the scan before
# the next batch is decoded.
function _sinkopen(sink::_ScanSink)
    return sink.remaining_take != 0
end

# Slice the kept rows without allocating: indexing a range yields a range,
# and a `findall` result is viewed in place.
_rowwindow(rows::AbstractRange, first::Int, last::Int) = rows[first:last]
_rowwindow(rows::AbstractVector, first::Int, last::Int) = view(rows, first:last)

_filterscratchvectors(
    ::Union{
        Tables.Cmp,
        Tables.In,
        Tables.IsNull,
        Tables.StrPred,
        Tables.AlwaysTrue,
        Tables.AlwaysFalse,
    },
) = Int64(1)
function _filterscratchvectors(e::Union{Tables.AndExpr,Tables.OrExpr})
    isempty(e.args) && return Int64(1)
    vectors = Int64(0)
    for arg in e.args
        vectors = AC.checked_add(vectors, _filterscratchvectors(arg))
    end
    return AC.checked_add(vectors, Int64(length(e.args) - 1))
end
_filterscratchvectors(e::Tables.NotExpr) =
    AC.checked_add(Int64(1), _filterscratchvectors(e.arg))
# A bound extension node is not expected to reach Tables' generic evaluator.
# Reserve one full intermediate before that evaluator reports its own error.
_filterscratchvectors(::Tables.ScanExpr) = Int64(1)

function _chargefilterscratch!(budget, filter, nrows::Integer)
    filter === nothing && return nothing
    one = AC._materializedvectorbytes(Union{Missing,Bool}, nrows)
    total = AC.checked_mul(_filterscratchvectors(filter), one)
    budget === nothing || _charge!(budget, total, "scan filter evaluation")
    return nothing
end

# The rows of one batch the scan keeps: the caller's metadata window when
# there is no filter, else this batch's qualifying rows after its share of
# `offset`/`limit`. A filter that references no decoded column is
# row-invariant, so one evaluation covers the batch.
function _sinkrows(sink::_ScanSink, decoded, rblen::Int64, skip::Int64, take::Int64)
    if sink.bound.filter === nothing
        take >= 0 || return 1:Int(rblen)
        return (Int(skip) + 1):(Int(skip) + Int(take))
    end
    if isempty(sink.filteridx)
        qualifying =
            _zerofieldpredicate(sink.bound.filter) === true ? (1:Int(rblen)) : (1:0)
    else
        _chargefilterscratch!(sink.budget, sink.bound.filter, rblen)
        _chargevector!(sink.budget, Bool, rblen, "scan filter mask")
        _chargevector!(
            sink.budget,
            AbstractVector,
            length(sink.filteridx),
            "scan filter columns",
        )
        filtercolumns = AbstractVector[decoded[idx] for idx in sink.filteridx]
        raw = _ScanColumns(sink.filternames, filtercolumns, Int(rblen))
        # Not inferred: Tables evaluates the filter on columns of any type.
        filtermask = _uninferred(Tables.filtermask, sink.bound, raw)::AbstractVector{Bool}
        matches = count(identity, filtermask)
        _chargevector!(sink.budget, Int, matches, "scan matching rows")
        qualifying = findall(filtermask)
    end
    skipped = min(sink.remaining_skip, Int64(length(qualifying)))
    sink.remaining_skip -= skipped
    available = Int64(length(qualifying)) - skipped
    taken = sink.remaining_take < 0 ? available : min(sink.remaining_take, available)
    sink.remaining_take < 0 || (sink.remaining_take -= taken)
    return _rowwindow(qualifying, Int(skipped) + 1, Int(skipped) + Int(taken))
end

# Consume one decoded batch (`cols` holds the decode set's ArrayData): each
# decode-set column materializes once through the operation's closed policy,
# the kept rows are chosen, and every output column keeps its slice of them.
function _consumebatch!(sink::_ScanSink, cols, rblen::Int64, skip::Int64, take::Int64)
    decoded = _decodedcolumns(sink)
    _materializecolumns!(decoded, sink, cols, sink.decodeidx)
    rows = _sinkrows(sink, decoded, rblen, skip, take)
    _keeprows!(sink, decoded, rows, rblen)
    return nothing
end

function _decodedcolumns(sink::_ScanSink)
    _chargedict!(
        sink.budget,
        Int,
        AbstractVector,
        length(sink.decodeidx),
        "scan decoded-column index",
    )
    decoded = Dict{Int,AbstractVector}()
    sizehint!(decoded, length(sink.decodeidx))
    return decoded
end

function _materializecolumns!(decoded, sink::_ScanSink, cols, idxs)
    for idx in idxs
        decoded[idx] =
            sink.materializecolumn(sink.fields[idx], cols[idx]::ArrayData, sink.budget)
    end
    return decoded
end

# Every output column keeps its slice of the kept rows: the column itself
# when the rows are all of it.
function _keeprows!(sink::_ScanSink, decoded, rows, rblen::Int64)
    sink.outrows = _addscanrows(sink.outrows, Int64(length(rows)))
    whole = length(rows) == rblen
    for (k, c) in enumerate(sink.columns)
        source = decoded[c.index]
        whole || _chargevector!(
            sink.budget,
            _eltypeof(source),
            length(rows),
            "scan column slice",
        )
        partidx = sink.partcounts[k] + 1
        partidx <= length(sink.parts[k]) ||
            throw(AssertionError("scan consumed more batches than its planned capacity"))
        sink.parts[k][partidx] = whole ? source : _rowsof(source, rows)
        sink.partcounts[k] = partidx
    end
    return nothing
end

# The consumed bound operation's result in selection order; type overrides
# apply after conversion, in `_finishfacadescan`.
function _sinkresult(sink::_ScanSink)
    _chargevector!(sink.budget, Symbol, length(sink.columns), "scan output names")
    outnames = Symbol[c.name for c in sink.columns]
    outcols = (
        begin
            parts = sink.parts[k]
            # Every column may still hold the shared empty sentinel here;
            # that only happens when no batch was consumed, so every count
            # is 0 and this resize cannot disturb a sibling.
            resize!(parts, sink.partcounts[k])
            _joinscanparts(sink.fields[c.index], parts, sink.budget)
        end for (k, c) in enumerate(sink.columns)
    )
    return _scantable(outnames, outcols, sink.outrows, sink.budget)
end

function _runboundscan(
    f::ArrowFile,
    b::Tables.BoundScan,
    names::Vector{Symbol},
    budget::AllocationBudget,
    materializecolumn,
    ntasks::Int=1,
)
    sink = _ScanSink(b, names, f.fields, materializecolumn, budget)
    _chargebitvector!(budget, length(names), "scan decode mask")
    mask = falses(length(names))
    mask[sink.decodeidx] .= true
    # Without a filter the window is metadata arithmetic: batches outside
    # it are never decoded. With one, the sink composes it over
    # qualifying rows as batches decode.
    windowed = b.filter === nothing && (b.limit !== nothing || b.offset > 0)
    window = if windowed
        _chargevector!(budget, Int64, length(f), "scan batch row counts")
        _batchwindow(
            Int64[_batchrows(f, i, budget) for i = 1:length(f)],
            b.offset,
            b.limit,
            budget,
        )
    else
        _chargevector!(budget, Tuple{Int,Int64,Int64}, length(f), "scan batch window")
        Tuple{Int,Int64,Int64}[(i, Int64(0), Int64(-1)) for i = 1:length(f)]
    end
    # A filtered scan with `limit == 0` is closed before the first batch
    # (`_sinkopen`), so it reserves no part slots at all.
    _reservesinkparts!(sink, b.filter !== nothing && b.limit == 0 ? 0 : length(window))
    # Late materialization: a filtered batch decodes its filter columns
    # first and the other selected columns only if some row qualifies.
    _chargevector!(budget, Int, length(sink.decodeidx), "scan late-column index")
    restidx = setdiff(sink.decodeidx, sink.filteridx)
    late = b.filter !== nothing && !isempty(sink.filteridx) && !isempty(restidx)
    if late
        _chargebitvector!(budget, length(names), "scan filter decode mask")
        _chargebitvector!(budget, length(names), "scan late decode mask")
        filtermask = falses(length(names))
        filtermask[sink.filteridx] .= true
        restmask = falses(length(names))
        restmask[restidx] .= true
    end
    for (i, skip, take) in window
        _sinkopen(sink) || break
        if late
            rblen, cols = _scanbatch(f, i, filtermask, budget, ntasks)
            decoded =
                _materializecolumns!(_decodedcolumns(sink), sink, cols, sink.filteridx)
            rows = _sinkrows(sink, decoded, rblen, skip, take)
            isempty(rows) && continue
            _, rest = _scanbatch(f, i, restmask, budget, ntasks)
            _materializecolumns!(decoded, sink, rest, restidx)
            _keeprows!(sink, decoded, rows, rblen)
        else
            rblen, cols = _scanbatch(f, i, mask, budget, ntasks)
            _consumebatch!(sink, cols, rblen, skip, take)
        end
    end
    return _sinkresult(sink)
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
mutable struct _RangedFile
    const sf::SourceFile
    const schema::Schema
    const fields::AC.FrozenVector{Field}
    const fielddictids::IdDict{Field,Int64}
    const dictionaries::Dict{Int64,ArrayData}
    const validated::AC._ValidatedDictionaries
    const recordblocks::Vector{NTuple{3,Int64}}
    const dataend::Int64
    const limits::Limits
    const footerversion::Int16
    # The message metadata version every block must share (0 until one is read).
    @atomic version::Int16
    # The stream's lifetime authority; each batch's bytes are their own region.
    const region::OwnerRegion
    const blocks::_BlockReader
end

function _RangedFile(sf::SourceFile, budget::AllocationBudget)
    ft = _readfooter(sf, sf.limits, budget)
    version = Ref(Int16(0))
    dictreader = _BlockReader(sf)
    framed(i, block) =
        let fm = first(_blockframe(dictreader, ft.dictblocks, i, ft.footerstart, budget))
            version[] = _filemetadataversion(
                ft.version,
                fm.version,
                version[] == 0 ? fm.version : version[],
            )
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
        version[],
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
function _batchrows(r::_RangedFile, i::Int, budget::AllocationBudget)
    seen = Ref(@atomic r.version)
    n = _rangedbatchrows(r.sf, r.recordblocks[i], r.footerversion, r.fields, budget, seen)
    @atomicreplace r.version Int16(0) => seen[]
    return n
end

# Record batch `i`, decoded through a one-block `ArrowFile` over its bytes.
function _rangedbatch(r::_RangedFile, i::Int, budget::AllocationBudget, ntasks::Int)
    1 <= i <= length(r.recordblocks) || throw(BoundsError(r, i))
    fm, region = _blockframe(r.blocks, r.recordblocks, i, r.dataend, budget)
    known = @atomic r.version
    if known == 0
        known, _ = @atomicreplace r.version Int16(0) => fm.version
        known == 0 && (known = fm.version)
    end
    version = _filemetadataversion(r.footerversion, fm.version, known)
    _, metalen, bodylen = r.recordblocks[i]
    f = ArrowFile(
        region,
        r.schema,
        r.fields,
        r.fielddictids,
        r.dictionaries,
        r.validated,
        [(Int64(0), metalen, bodylen)],
        region.len,
        r.limits,
        version,
    )
    return only(_filerecordbatches(f, (1,), budget, ntasks))
end

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

# Spans of two fetch rounds as one lookup, ordered by start.
function _unionspans(a::FetchedSpans, b::FetchedSpans, budget)
    isempty(b.starts) && return a
    n = length(a.starts) + length(b.starts)
    _chargevector!(budget, Int64, 2n, "fetched span index")
    _chargevector!(budget, BufferSlice, n, "fetched span slices")
    starts = vcat(a.starts, b.starts)
    order = sortperm(starts)
    return FetchedSpans(
        starts[order],
        vcat(a.lens, b.lens)[order],
        vcat(a.slices, b.slices)[order],
    )
end

function _fetchbodyspans(sf::SourceFile, ranges, metadata::FetchedSpans, budget)
    _chargevector!(budget, NTuple{2,Int64}, length(ranges), "unfetched body ranges")
    unfetched = filter(r -> _spanholding(metadata, r[1], r[2]) == 0, ranges)
    fetched =
        _fetchspans(sf, unfetched, sf.gapbytes; budget=budget, what="body range fetch")
    return _unionspans(metadata, fetched, budget)
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

"Ids of every dictionary field inside the masked top-level subtrees."
function _neededdictids(fields, fielddictids, mask::AbstractVector{Bool}, budget=nothing)
    capacity = Int64(0)
    function countfields(f::Field)
        if f.type isa DictionaryType
            capacity = AC.checked_add(capacity, Int64(1))
            return
        end
        foreach(countfields, f.children)
    end
    for (j, f) in enumerate(fields)
        mask[j] && countfields(f)
    end
    _chargedict!(budget, Int64, Nothing, capacity, "scan dictionary-id set")
    ids = Set{Int64}()
    capacity <= typemax(Int) && sizehint!(ids, Int(capacity))
    function walk(f::Field)
        if f.type isa DictionaryType
            push!(ids, fielddictids[f])
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
    messageversion::Base.RefValue{Int16},
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
    expected = messageversion[] == 0 ? v : messageversion[]
    messageversion[] = _filemetadataversion(version, v, expected)
    rejectexperimentalcompression(msg, v, header_type)
    return _recordbatchmeta(msg.header::Meta.RecordBatch, fields, sf.limits, bodylen)
end

function _runboundscan(
    sf::SourceFile,
    b::Tables.BoundScan,
    names::Vector{Symbol},
    ft,
    budget::AllocationBudget,
    materializecolumn,
)
    limits = sf.limits
    fields = ft.fields
    dictids = ft.dictids
    fielddictids = ft.fielddictids
    dictvaluefields = ft.dictvaluefields
    version = ft.version
    dictblocks = ft.dictblocks
    recordblocks = ft.recordblocks
    footerstart = ft.footerstart
    sink = _ScanSink(b, names, fields, materializecolumn, budget)
    decodeidx = sink.decodeidx
    _chargebitvector!(budget, length(names), "scan decode mask")
    mask = falses(length(names))
    mask[decodeidx] .= true

    nrec = length(recordblocks)
    # One coalesced metadata pass over the dictionary and record blocks;
    # bodies come later and only for what the scan needs. Every candidate
    # is bounded before its metadata fetch (ArrowFile's lazy record limits).
    nmeta = length(dictblocks) + nrec
    _chargevector!(budget, NTuple{3,Int64}, nmeta, "scan metadata blocks")
    metablocks = NTuple{3,Int64}[]
    sizehint!(metablocks, nmeta)
    append!(metablocks, dictblocks)
    append!(metablocks, recordblocks)
    for block in metablocks
        _, metalen, bodylen = block
        declared = metalen - 8
        _checkblocklengths(declared, bodylen, limits)
    end
    _chargevector!(budget, NTuple{2,Int64}, nmeta, "metadata range plan")
    # Each block's metadata comes with the start of its body, or all of a small
    # body: the selected buffers often lie there, and an over-read of a few
    # hundred KiB costs less than a second request round. The body round then
    # fetches only what this one did not. A scan with a row window needs few
    # of the bodies, so it reads metadata alone.
    limitedfilter = b.filter !== nothing && b.limit !== nothing
    bodygap = b.limit === nothing && b.offset == 0 ? sf.gapbytes : Int64(0)
    metaranges = NTuple{2,Int64}[
        (bl[1], AC.checked_add(bl[2], _speculativebody(bl[3], bodygap))) for
        bl in metablocks
    ]
    # A filtered limit may end at the first batch. Coalescing across its
    # successors' bodies would download those rows before the filter runs.
    metaspans = _fetchspans(
        sf,
        metaranges,
        limitedfilter ? Int64(0) : sf.gapbytes;
        budget=budget,
        what="metadata range fetch",
    )
    _chargevector!(
        budget,
        Tuple{Meta.Message,Int16},
        length(metablocks),
        "parsed block metadata",
    )
    blockmeta = Vector{Tuple{Meta.Message,Int16}}(undef, length(metablocks))
    for (i, block) in enumerate(metablocks)
        payload = _spanslice(metaspans, block[1], block[2])
        msg, v, header_type, _ = _blockmeta(payload, block, limits, budget)
        expected_dict = i <= length(dictblocks)
        (expected_dict ? header_type == UInt8(2) : header_type == UInt8(3)) || throw(
            ValidationError(
                expected_dict ? "footer dictionary block is not a dictionary batch" :
                "footer record block is not a record batch",
            ),
        )
        _filemetadataversion(version, v, i == 1 ? v : blockmeta[1][2])
        rejectexperimentalcompression(msg, v, header_type)
        blockmeta[i] = (msg, v)
    end

    isempty(blockmeta) || (version = blockmeta[1][2])

    # RecordBatch lengths live in block metadata, not the Footer. The metadata
    # pass above is required before limit/offset can choose body ranges.
    _chargevector!(budget, Meta.RecordBatch, nrec, "record-batch headers")
    headers = [blockmeta[length(dictblocks) + p][1].header::Meta.RecordBatch for p = 1:nrec]
    _chargevector!(budget, Vector{Int64}, nrec, "record variadic-count cache")
    recordvariadics = Vector{Vector{Int64}}(undef, nrec)
    _chargevector!(budget, Int64, nrec, "scan batch row counts")
    rowcounts = Vector{Int64}(undef, nrec)
    for (p, h) in enumerate(headers)
        variadics = variadiccounts(h)
        recordvariadics[p] = variadics
        rowcounts[p] = _recordbatchmeta(h, fields, limits, recordblocks[p][3], variadics)
    end
    windowed = b.filter === nothing && (b.limit !== nothing || b.offset > 0)
    window =
        windowed ? _batchwindow(rowcounts, b.offset, b.limit, budget) :
        begin
            _chargevector!(budget, Tuple{Int,Int64,Int64}, nrec, "scan batch window")
            Tuple{Int,Int64,Int64}[
                (p, Int64(0), Int64(-1)) for p = 1:nrec if _sinkopen(sink)
            ]
        end
    # A filtered scan with `limit == 0` is closed before the first batch
    # (`_sinkopen`), so it reserves no part slots at all.
    _reservesinkparts!(sink, b.filter !== nothing && b.limit == 0 ? 0 : length(window))

    # Decode-set dictionaries: whole bodies, coalesced; everything else is
    # metadata-only forever.
    needed =
        isempty(window) ? Set{Int64}() : _neededdictids(fields, fielddictids, mask, budget)
    _chargedict!(budget, Int64, Nothing, length(dictblocks), "seen dictionary ids")
    seenids = Set{Int64}()
    sizehint!(seenids, length(dictblocks))
    _chargevector!(budget, Int, length(dictblocks), "selected dictionary blocks")
    wanted_dict = Int[]
    sizehint!(wanted_dict, length(dictblocks))
    _chargevector!(
        budget,
        Vector{Int64},
        length(dictblocks),
        "dictionary variadic-count cache",
    )
    dictvariadics = Vector{Vector{Int64}}(undef, length(dictblocks))
    for (i, block) in enumerate(dictblocks)
        msg, _ = blockmeta[i]
        header = msg.header
        header isa Meta.DictionaryBatch ||
            throw(ValidationError("footer dictionary block is not a dictionary batch"))
        haskey(dictids, header.id) ||
            throw(ValidationError("dictionary batch has unknown id $(header.id)"))
        _dictionarytransition(header.id, header.isDelta, header.id in seenids; file=true)
        push!(seenids, header.id)
        rb = header.data
        vf = dictvaluefields[header.id]
        variadics = variadiccounts(rb)
        dictvariadics[i] = variadics
        _recordbatchmeta(rb, (vf,), limits, block[3], variadics)
        codec = _batchcodec(rb.compression, blockmeta[i][2])
        if header.id in needed
            _validatebodyplan(rb, (vf,), limits, codec, (true,), variadics)
            push!(wanted_dict, i)
        end
    end
    # `_validatebodyplan`'s return value is the exact number of nonempty
    # ranges the loop below will plan; the assertion after it re-checks that
    # the two passes agree.
    bodycount = Int64(count(i -> dictblocks[i][3] > 0, wanted_dict))
    for (p, _, _) in window
        _, v = blockmeta[length(dictblocks) + p]
        codec = _batchcodec(headers[p].compression, v)
        bodycount = AC.checked_add(
            bodycount,
            _validatebodyplan(headers[p], fields, limits, codec, mask, recordvariadics[p]),
        )
    end
    for id in needed
        id in seenids || throw(
            ValidationError(
                "record batch references dictionary id $id before its dictionary batch",
            ),
        )
    end
    _chargedict!(budget, Int64, ArrayData, length(wanted_dict), "decoded dictionary table")
    dicts = Dict{Int64,ArrayData}()
    sizehint!(dicts, length(wanted_dict))
    _chargedict!(
        budget,
        ArrayData,
        Nothing,
        length(wanted_dict),
        "validated dictionary identities",
    )
    validated = AC._ValidatedDictionaries()
    sizehint!(validated, length(wanted_dict))
    # An unrestricted scan coalesces the selected bodies into one round.
    # A filtered limit fetches them batch by batch, stopping when it is full.
    bodycount <= typemax(Int) ||
        throw(ValidationError("body range count exceeds the host index range"))
    _chargevector!(budget, NTuple{2,Int64}, bodycount, "body range plan")
    bodyranges = Vector{NTuple{2,Int64}}(undef, Int(bodycount))
    rangeidx = 0
    for i in wanted_dict
        dictblocks[i][3] == 0 && continue
        rangeidx += 1
        bodyranges[rangeidx] = (dictblocks[i][1] + dictblocks[i][2], dictblocks[i][3])
    end
    dictranges = 1:rangeidx
    _chargevector!(budget, UnitRange{Int}, length(window), "record body range indexes")
    recordranges = Vector{UnitRange{Int}}(undef, length(window))
    for (batchidx, (p, _, _)) in enumerate(window)
        firstindex = rangeidx + 1
        block = recordblocks[p]
        header = headers[p]
        buffers = something(header.buffers, Meta.Buffer[])
        variadics = recordvariadics[p]
        varidx = Ref(1)
        bufidx = 1
        for (j, fld) in enumerate(fields)
            span64 = _bufferspan(fld, variadics, varidx)
            span64 <= typemax(Int) || throw(
                ValidationError("field buffer span $span64 exceeds the host index range"),
            )
            span = Int(span64)
            if mask[j]
                for k = bufidx:(bufidx + span - 1)
                    k <= length(buffers) || throw(
                        ValidationError(
                            "metadata declares fewer buffers than the schema requires",
                        ),
                    )
                    buf = buffers[k]
                    len = Int64(buf.length)
                    off = Int64(buf.offset)
                    (off >= 0 && len >= 0 && AC.checked_add(off, len) <= block[3]) || throw(
                        ValidationError(
                            "batch buffer [$off, $len] escapes its message body",
                        ),
                    )
                    len == 0 && continue
                    rangeidx += 1
                    bodyranges[rangeidx] = (block[1] + block[2] + off, len)
                end
            end
            bufidx += span
        end
        recordranges[batchidx] = firstindex:rangeidx
    end
    rangeidx == length(bodyranges) ||
        throw(AssertionError("body range preflight count changed during planning"))
    bodyspans = _fetchbodyspans(
        sf,
        limitedfilter ? view(bodyranges, dictranges) : bodyranges,
        metaspans,
        budget,
    )

    state = DecodeState(budget)
    try
        if !isempty(wanted_dict)
            for i in wanted_dict
                block = dictblocks[i]
                msg, v = blockmeta[i]
                header = msg.header::Meta.DictionaryBatch
                rejectexperimentalcompression(msg, v, UInt8(2))
                rb = header.data
                codec = _batchcodec(rb.compression, v)
                vf = dictvaluefields[header.id]
                variadics = dictvariadics[i]
                rblen = _recordbatchmeta(rb, (vf,), limits, block[3], variadics)
                body = _spanslice(bodyspans, block[1] + block[2], block[3])
                cursor = DecodeCursor(
                    rb.nodes,
                    rb.buffers,
                    body,
                    limits;
                    codec=codec,
                    state=state,
                    variadics=variadics,
                )
                decoded = decodefield(vf, cursor, dicts, fielddictids)
                finishcursor!(cursor)
                decoded.len == rblen || throw(
                    ValidationError(
                        "dictionary RecordBatch length does not match its field node",
                    ),
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
        end

        for (batchidx, (p, skip, take)) in enumerate(window)
            _sinkopen(sink) || break
            if limitedfilter
                ranges = recordranges[batchidx]
                _chargevector!(
                    budget,
                    NTuple{2,Int64},
                    length(ranges),
                    "record body ranges",
                )
                # Limited-filter metadata reads contain no speculative body bytes.
                bodyspans = _fetchspans(
                    sf,
                    bodyranges[ranges],
                    sf.gapbytes;
                    budget=budget,
                    what="body range fetch",
                )
            end
            block = recordblocks[p]
            msg, v = blockmeta[length(dictblocks) + p]
            body = SparseBody(block[3], block[1] + block[2], bodyspans)
            rblen, cols = _maskedrecord(
                msg,
                v,
                body,
                fields,
                dicts,
                fielddictids,
                validated,
                limits,
                version,
                mask,
                state,
                recordvariadics[p],
            )
            _consumebatch!(sink, cols, rblen, skip, take)
        end
        return _sinkresult(sink)
    finally
        close(state)
    end
end

# Tables.jl exposes binding (`resolve`), predicate evaluation, and
# allocation, but no executor for an existing `BoundScan`. Keep this small
# local executor instead of reconstructing a Scan and resolving it a second
# time.
"Execute an already-compiled scan over a materialized Tables.jl source."
function _executeplan(table, b::Tables.BoundScan, budget=nothing)
    cols = Tables.columns(table)
    ncols = length(Tables.columnnames(cols))
    _chargevector!(budget, Symbol, ncols, "scan input names")
    names = collect(Symbol, Tables.columnnames(cols))
    return _executeplan(table, b, names, budget)
end

function _executeplan(table, b::Tables.BoundScan, names::Vector{Symbol}, budget=nothing)
    cols = Tables.columns(table)
    length(Tables.columnnames(cols)) == length(names) || throw(
        AssertionError(
            "scan source width $(length(Tables.columnnames(cols))) does not match " *
            "its compiled schema width $(length(names))",
        ),
    )
    nrows = Int(Tables.rowcount(cols))
    if isempty(names)
        n = _zerofieldcount(Int64(nrows), _zerofieldpredicate(b.filter), b.limit, b.offset)
        return _scantable(Symbol[], (), Int(n), budget)
    end

    idx = if b.filter === nothing
        1:nrows
    else
        _chargefilterscratch!(budget, b.filter, nrows)
        _chargevector!(budget, Bool, nrows, "scan filter mask")
        filtermask = _uninferred(Tables.filtermask, b, cols)::AbstractVector{Bool}
        matches = count(identity, filtermask)
        _chargevector!(budget, Int, matches, "scan matching rows")
        findall(filtermask)
    end
    skipped = min(b.offset, length(idx))
    available = length(idx) - skipped
    taken = b.limit === nothing ? available : min(b.limit, available)
    rows = _rowwindow(idx, skipped + 1, skipped + taken)
    _chargevector!(budget, Symbol, length(b.columns), "scan output names")
    outnames = Symbol[c.name for c in b.columns]
    outcols = (
        begin
            source = Tables.getcolumn(cols, c.index)
            _chargevector!(budget, _eltypeof(source), length(rows), "scan column slice")
            col = _rowsof(source, rows)
            c.type === nothing ? col : _uninferred(_applyoverride, c.type, col, budget)
        end for c in b.columns
    )
    return _scantable(outnames, outcols, taken, budget)
end

"Execute one scan plan in the public domain over a converted Table."
function _publicscan(
    full::Table,
    schema,
    sourcefields,
    plan::_ScanPlan,
    regions,
    budget=nothing,
)
    b = plan.bound
    got = _executeplan(full, b, plan.names, budget)
    cols = Tables.columns(got)
    ncols = length(Tables.columnnames(cols))
    _chargevector!(budget, Symbol, ncols, "public scan names")
    names = collect(Symbol, Tables.columnnames(cols))
    _chargevector!(budget, AbstractVector, ncols, "public scan columns")
    columns = AbstractVector[Tables.getcolumn(cols, nm) for nm in names]
    _chargevector!(budget, AbstractVector, length(b.columns), "pre-override scan columns")
    precols = AbstractVector[Tables.getcolumn(full, bc.index) for bc in b.columns]
    n = Int(Tables.rowcount(cols))
    bound = _boundschema(schema, sourcefields, b, precols, budget)
    fullpools = getfield(full, :retainedpools)
    _chargevector!(budget, Any, length(b.columns), "scan retained dictionary pools")
    pools = Any[
        _keepsfield(bc, precols[i]) ? fullpools[bc.index] : nothing for
        (i, bc) in enumerate(b.columns)
    ]
    _chargevector!(budget, AC.OwnerRegion, length(regions), "scan owner regions")
    return _table(names, columns, bound, AC.OwnerRegion[regions...], n, pools, budget)
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

"""
Finish a facade scan: apply its type overrides to the public columns it read,
give an empty column the type a full read gives, and wrap the Table.
"""
function _finishfacadescan(
    got,
    schema,
    sourcefields,
    plan::_ScanPlan,
    views::_ViewContext;
    regions=AC.OwnerRegion[],
    budget=nothing,
    poolof=nothing,
)
    cols = Tables.columns(got)
    ncols = length(Tables.columnnames(cols))
    _chargevector!(budget, Symbol, ncols, "facade scan names")
    names = collect(Symbol, Tables.columnnames(cols))
    _chargevector!(budget, AbstractVector, ncols, "facade scan columns")
    columns = AbstractVector[Tables.getcolumn(cols, nm) for nm in names]
    _chargevector!(budget, AbstractVector, length(columns), "pre-override facade columns")
    precols = AbstractVector[]
    sizehint!(precols, length(columns))
    b = plan.bound
    pools = nothing
    if !isempty(sourcefields)
        arrowtypes = _ArrowTypesContext(budget=budget)
        length(b.columns) == length(columns) || throw(
            AssertionError(
                "scan output width $(length(columns)) does not match its bound " *
                "selection $(length(b.columns))",
            ),
        )
        for (i, bc) in enumerate(b.columns)
            f = sourcefields[bc.index]
            col = isempty(columns[i]) ? _emptycolumn(f, views, arrowtypes) : columns[i]
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
                columns[i] =
                    bc.type === nothing ? col :
                    _uninferred(_applyoverride, bc.type, col, budget)
            end
        end
        # Dictionary columns keep their source category pool, as a full read
        # does, so a rewrite preserves category order (required when ordered).
        if poolof !== nothing
            _chargevector!(budget, Any, length(b.columns), "scan retained dictionary pools")
            pools = Any[
                sourcefields[bc.index].type isa DictionaryType &&
                _keepsfield(bc, precols[i]) ?
                poolof(bc, sourcefields[bc.index], arrowtypes) : nothing for
                (i, bc) in enumerate(b.columns)
            ]
        end
    end
    nrows = isempty(columns) ? _scanrowcount(got) : length(columns[1])
    bound = _boundschema(schema, sourcefields, b, precols, budget)
    _chargevector!(budget, AC.OwnerRegion, length(regions), "scan owner regions")
    return _table(names, columns, bound, AC.OwnerRegion[regions...], nrows, pools, budget)
end

# The column a full read of no record batches gives field `f`.
_emptycolumn(f::AC.Field, views::_ViewContext, arrowtypes) =
    _viewable(f) ? _viewcolumn(f, AC.ArrayData[], views) :
    _facadecolumn(f, Any[], arrowtypes)

# File record batches all reference each dictionary's final pool, so the first
# decoded snapshot of a selected dictionary column is the pool a full read
# retains. Slots start `nothing`; the facade materializer fills them.
function _dictsnapshots(fields, b::Tables.BoundScan, budget)
    n = count(bc -> fields[bc.index].type isa DictionaryType, b.columns)
    _chargedict!(budget, Field, Union{Nothing,ArrayData}, n, "scan dictionary snapshots")
    snapshots = IdDict{Field,Union{Nothing,ArrayData}}()
    for bc in b.columns
        f = fields[bc.index]
        f.type isa DictionaryType && (snapshots[f] = nothing)
    end
    return snapshots
end

function _snapshotdict!(snapshots, field::Field, data::ArrayData)
    get(snapshots, field, missing) === nothing && (snapshots[field] = data.dictionary)
    return nothing
end

function _applyfacadescan(
    f::ArrowFile,
    plan::_ScanPlan,
    budget::Union{Nothing,AllocationBudget}=nothing;
    stringtype::Type=DataStrings.DataString,
    ntasks::Int=1,
)
    b = plan.bound
    actual = budget === nothing ? _readbudget(f.limits, f.region.len) : budget
    arrowtypes = _ArrowTypesContext(budget=actual)
    views = _ViewContext(stringtype, actual)
    snapshots = _dictsnapshots(f.fields, b, actual)
    rawoverrides = _rawoverrides(b, f.fields)
    materialize =
        (field, data, _) -> (
            _snapshotdict!(snapshots, field, data);
            _scanbatchcolumn(field, data, arrowtypes, views, rawoverrides)
        )
    # A scan that only selects keeps every row: its columns are the full
    # read's columns (views chain, strings are one column), with no per-batch
    # slices or joins.
    selectonly = _selectsonly(b) && !isempty(plan.names) && isempty(rawoverrides)
    got =
        selectonly ? _selectscan(f, b, actual, snapshots, views, ntasks) :
        _runboundscan(f, b, plan.names, actual, materialize, ntasks)
    return _finishfacadescan(
        got,
        f.schema,
        f.fields,
        plan,
        views;
        regions=AC.OwnerRegion[f.region],
        budget=actual,
        poolof=(_, field, ctx) -> _retainedsnapshotpool(field, snapshots[field], ctx),
    )
end

_selectsonly(b::Tables.BoundScan) =
    b.filter === nothing && b.limit === nothing && b.offset == 0

# Decode the selected columns of every batch and build each as a full read
# does, in the public domain, on at most `ntasks` tasks.
function _selectscan(
    f::ArrowFile,
    b::Tables.BoundScan,
    budget,
    snapshots,
    views,
    ntasks::Int,
)
    _chargebitvector!(budget, length(f.fields), "scan decode mask")
    mask = falses(length(f.fields))
    for c in b.columns
        mask[c.index] = true
    end
    plans, cols = _filebatches(f, 1:length(f), budget, ntasks, mask)
    _chargevector!(budget, NamedTuple, length(f), "scan batches")
    batches = [(columns=c,) for c in cols]
    nrows = 0
    for p in plans
        nrows = _addscanrows(nrows, p.rblen)
    end
    for c in b.columns
        field = f.fields[c.index]
        foreach(batch -> _snapshotdict!(snapshots, field, batch.columns[c.index]), batches)
    end
    arrowtypes = _ArrowTypesContext(budget=budget)
    idxs = Int[c.index for c in b.columns]
    # As in a whole read, string columns build when the table hands them out.
    columns = _buildcolumns(f.fields, batches, idxs, arrowtypes, views, ntasks; lazy=true)
    return _scantable(Symbol[c.name for c in b.columns], columns, nrows, budget)
end

function _applyfacadescan(
    sf::SourceFile,
    plan::_ScanPlan,
    ft,
    budget::AllocationBudget;
    stringtype::Type=DataStrings.DataString,
)
    b = plan.bound
    arrowtypes = _ArrowTypesContext(budget=budget)
    views = _ViewContext(stringtype, budget)
    snapshots = _dictsnapshots(ft.fields, b, budget)
    rawoverrides = _rawoverrides(b, ft.fields)
    materialize =
        (field, data, _) -> (
            _snapshotdict!(snapshots, field, data);
            _scanbatchcolumn(field, data, arrowtypes, views, rawoverrides)
        )
    got = _runboundscan(sf, b, plan.names, ft, budget, materialize)
    return _finishfacadescan(
        got,
        ft.sch,
        ft.fields,
        plan,
        views;
        budget=budget,
        poolof=(_, field, ctx) -> _retainedsnapshotpool(field, snapshots[field], ctx),
    )
end

"""
One decoded batch column of a scan, in the public domain: the view a full
read gives, or the materialized column. A field in `rawoverrides` reads its
stored values instead (see `_periodoverride` and `_symboloverride`).
"""
function _scanbatchcolumn(
    f::AC.Field,
    d::AC.ArrayData,
    arrowtypes::_ArrowTypesContext,
    views::_ViewContext,
    rawoverrides,
)
    if any(x -> x === f, rawoverrides)
        f.type isa AC.TimeType && return _storedticks(f, d, views.budget)
        return _storagebatchcolumn(f, d, views.budget)
    end
    _viewable(f) && return _viewcolumn(f, AC.ArrayData[d], views)
    return _facadesinglecolumn(f, d, arrowtypes)
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

function _applyfacadescan(
    s::IPCStream,
    plan::_ScanPlan,
    supplied::Union{Nothing,AllocationBudget}=nothing;
    stringtype::Type=DataStrings.DataString,
    ntasks::Int=1,
)
    budget = supplied === nothing ? s.budget : supplied
    _chargevector!(budget, Field, length(s.corefields), "stream scan source fields")
    fields = collect(Field, s.corefields)
    regions = _sourceregions(s, budget)
    views = _ViewContext(stringtype, budget)
    return _scanbatches(s.schema, fields, s.batches, regions, plan, budget, views, ntasks)
end

# A scan over decoded record batches: every batch of a stream-format read, or
# one batch of an `Arrow.Stream`. The result keeps `regions`.
function _scanbatches(
    schema,
    fields::Vector{Field},
    batches,
    regions,
    plan::_ScanPlan,
    budget,
    views::_ViewContext,
    ntasks::Int,
)
    b = plan.bound
    _chargevector!(budget, AbstractVector, length(fields), "stream scan columns")
    arrowtypes = _ArrowTypesContext(budget=budget)
    nrows = sum(b -> Int(b.nrows), batches; init=0)
    poolof = (bc, field, ctx) -> _retaineddictpool(field, batches, bc.index, ctx)
    rawoverrides = _rawoverrides(b, fields)
    if _selectsonly(b) && !isempty(plan.names) && isempty(rawoverrides)
        # Every row is kept: the selected columns are the full read's.
        idxs = Int[c.index for c in b.columns]
        columns = _buildcolumns(fields, batches, idxs, arrowtypes, views, ntasks; lazy=true)
        got = _scantable(Symbol[c.name for c in b.columns], columns, nrows, budget)
        return _finishfacadescan(
            got,
            schema,
            fields,
            plan,
            views;
            regions=regions,
            budget=budget,
            poolof=poolof,
        )
    end
    # Only the selected and filter-referenced columns are built; the executor
    # reads no other column (an empty placeholder would fail loudly if it did).
    needed = falses(length(fields))
    for c in b.columns
        needed[c.index] = true
    end
    needed[b.filtercols] .= true
    b.validate && fill!(needed, true)
    rawcolumns = AbstractVector[
        needed[i] ?
        _joinscanparts(
            fields[i],
            AbstractVector[
                _scanbatchcolumn(fields[i], bt.columns[i], arrowtypes, views, rawoverrides)
                for bt in batches
            ],
            budget,
        ) : Missing[] for i = 1:length(fields)
    ]
    raw = _ScanColumns(plan.names, rawcolumns, nrows)
    # `_finishfacadescan` applies the type overrides.
    columns = Tables.BoundColumn[Tables.BoundColumn(c.index, c.name, nothing) for c in b.columns]
    unconverted =
        Tables.BoundScan(columns, b.filter, b.filtercols, b.limit, b.offset, b.validate)
    got = _executeplan(raw, unconverted, plan.names, budget)
    return _finishfacadescan(
        got,
        schema,
        fields,
        plan,
        views;
        regions=regions,
        budget=budget,
        poolof=poolof,
    )
end

_scanrowcount(got) = Int(Tables.rowcount(Tables.columns(got)))

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
        out[i] = ismissing(x) ? missing : _overridevalue(V, x)
    end
    return out
end

# One scan-override value. `convert` is the contract, but the composite rows
# the facade reads (struct rows as `NamedTuple`s or, for wide or duplicate
# names, `Vector{Pair}`s; list rows as views) have no `convert` methods to
# user types, so fully typed named-tuple and vector targets rebuild
# recursively. Struct fields match by position and the names must agree.
function _overridevalue(::Type{T}, x) where {T}
    x isa T && return x
    if T isa DataType && T <: NamedTuple && x isa NamedTuple
        names = fieldnames(T)
        keys(x) == names || throw(
            ArgumentError("struct row fields $(keys(x)) do not match $(T) fields $(names)"),
        )
        return T(
            ntuple(length(names)) do i
                v = x[i]
                v === missing ? missing :
                _overridevalue(Base.nonmissingtype(fieldtype(T, i)), v)
            end,
        )
    end
    if T isa DataType && T <: NamedTuple && x isa AbstractVector{<:Pair}
        names = fieldnames(T)
        length(x) == length(names) || throw(
            ArgumentError(
                "cannot convert a $(length(x))-field struct row to $(T) " *
                "with $(length(names)) fields",
            ),
        )
        vals = ntuple(length(names)) do i
            k, v = x[i]
            Symbol(k) === names[i] || throw(
                ArgumentError(
                    "struct row field $(repr(String(k))) does not match " *
                    "$(T) field $(repr(names[i])) at position $(i)",
                ),
            )
            F = fieldtype(T, i)
            v === missing ? missing : _overridevalue(Base.nonmissingtype(F), v)
        end
        return T(vals)
    end
    # A map row (a `MapValue`) converts entry by entry, as a vector of pairs.
    if T <: AbstractVector && x isa Union{AbstractVector,MapValue}
        E = eltype(T)
        N = Base.nonmissingtype(E)
        return convert(T, E[v === missing ? missing : _overridevalue(N, v) for v in x])
    end
    return convert(T, x)
end
