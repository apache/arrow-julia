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
# the bound scan exactly, batch by batch. Direct handle scans apply type
# overrides in the storage domain. The facade applies them after conversion
# to the public domain.
#
#   * the decode set is (selected ∪ filter-referenced) columns — everything
#     else is SKIPPED by `skipfield!`, a registry walk that consumes the
#     node/buffer accounting (all buffer-table invariants still checked)
#     without slicing, decompressing, validating, or materializing anything;
#   * whole batches are pruned by footer-carried statistics (may-contain);
#     without a filter `limit`/`offset` are metadata arithmetic —
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
        n = _planadd(n, vc, "buffer span")
    end
    f.type isa DictionaryType && return n
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        n = _planadd(n, _bufferspan(f.children[i], variadics, varidx), "buffer span")
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
    0 <= rblen <= limits.max_array_length ||
        throw(ValidationError("record batch length $rblen exceeds limit"))

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
        0 <= node.length <= limits.max_array_length ||
            throw(ValidationError("field-node length $(node.length) exceeds limit"))
        0 <= node.null_count <= node.length ||
            throw(ValidationError("invalid field-node null count $(node.null_count)"))
    end

    buffers = something(header.buffers, Meta.Buffer[])
    varidx = Ref(1)
    expectedbuffers = Int64(0)
    for f in fields
        expectedbuffers = _planadd(
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
        0 <= len <= limits.max_buffer_bytes ||
            throw(ValidationError("batch buffer length $len exceeds limit"))
        bufferend = try
            AC.checked_add(offset, len)
        catch e
            e isa OverflowError || rethrow()
            throw(ValidationError("batch buffer end overflows"))
        end
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

function _planadd(a::Int64, b::Int64, what::AbstractString)
    try
        return AC.checked_add(a, b)
    catch e
        e isa OverflowError || rethrow()
        throw(ValidationError("$what overflows"))
    end
end

function _planmul(a::Int64, b::Int64, what::AbstractString)
    try
        return AC.checked_mul(a, b)
    catch e
        e isa OverflowError || rethrow()
        throw(ValidationError("$what overflows"))
    end
end

function _planminbytes(role, spec, node, len::Int64)
    if role == AC.VALIDITY
        len == 0 && node.null_count == 0 && return Int64(0)
        return node.length ÷ 8 + (node.length % 8 == 0 ? 0 : 1)
    elseif role == AC.DATA
        spec.fixedwidth > 0 &&
            return _planmul(node.length, Int64(spec.fixedwidth), "planned data-buffer size")
        if spec.fixedwidth == -1
            return node.length ÷ 8 + (node.length % 8 == 0 ? 0 : 1)
        end
        return Int64(0)
    elseif role == AC.OFFSETS
        count = _planadd(node.length, Int64(1), "planned offset count")
        return _planmul(count, Int64(spec.offsetwidth), "planned offsets-buffer size")
    elseif role == AC.ELEMENT_OFFSETS || role == AC.SIZES
        return _planmul(node.length, Int64(spec.offsetwidth), "planned element-buffer size")
    elseif role == AC.TYPE_IDS
        return node.length
    elseif role == AC.VIEWS
        return _planmul(node.length, Int64(16), "planned views-buffer size")
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
        _planmul(node.length, Int64(t.listsize), "fixed-size-list child length") : Int64(0)
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

"Validate every metadata-only invariant for the subtrees whose bodies are planned."
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
    fm.version == f.schemaversion ||
        throw(ValidationError("IPC metadata version changes within the file"))
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

"""
Window a SEQUENCE of batch row counts without summing past the request: a
limit saturates (a hostile total never overflows a capped scan), while an
unbounded request keeps the checked-add contract — an overflowing total is
a refusal, exactly as the column-bearing paths refuse.
"""
function _zerofieldwindow(counts, keep, limit, offset)
    keep === true || return Int64(0)
    off = Int64(offset)
    lim = limit === nothing ? Int64(-1) : Int64(limit)
    n = Int64(0)
    for r0 in counts
        r = Int64(r0)
        skip = min(off, r)
        off -= skip
        r -= skip
        if lim >= 0
            take = min(r, lim - n)
            n += take
            n == lim && return n
        else
            n = _planadd(n, r, "zero-field scan row count")
        end
    end
    return n
end

# --- scan value domain -------------------------------------------------------
# Pushdown filtering runs over storage-domain values; facade
# filter literals arrive in public Julia types. Lower native literals to the
# referenced field's storage domain before the scan when that preserves
# semantics. Registered logical extensions fall back to the public domain.
# Conversion back to public types happens exactly once on the scan output
# under the output names already compiled into the scan plan.

# Lowering returns (ok, value): ok=false means the literal has NO exact,
# semantics-preserving storage representation for this field (cross-type
# inexactness, wrong type entirely) — the caller must then evaluate the
# whole filter in the PUBLIC domain instead of pushing it down. There is no
# pass-through: an unlowered literal comparing "equal" to raw storage would
# change predicate semantics.
_nativefacadeconversion(t::AC.ArrowType) =
    _istemporalconv(t) || (t isa AC.DictionaryType && _nativefacadeconversion(t.valuetype))
_nativefacadeconversion(f::AC.Field) =
    _nativefacadeconversion(f.type) || any(_nativefacadeconversion, f.children)

_equalityoperator(op) = op == Tables.OP_EQ || op == Tables.OP_NE

_scancomparisonpreserving(::AC.ArrowType, op) = true
_scancomparisonpreserving(t::AC.DictionaryType, op) =
    _scancomparisonpreserving(t.valuetype, op)
_scancomparisonpreserving(t::AC.DateType, op) = t.unit == AC.DAY || _equalityoperator(op)
_scancomparisonpreserving(t::AC.TimestampType, op) =
    t.unit in (AC.MICROSECOND, AC.NANOSECOND) ||
    (t.unit == AC.MILLISECOND && _equalityoperator(op))
_scancomparisonpreserving(::AC.TimeType, op) = false
_scancomparisonpreserving(::AC.DurationType, op) = true

_scanliteralcompatible(::AC.ArrowType, value) = true
_scanliteralcompatible(t::AC.DictionaryType, value) =
    _scanliteralcompatible(t.valuetype, value)
function _scanliteralcompatible(t::AC.DurationType, value)
    scale = _periodscale(value)
    return scale !== nothing && scale >= _durationunitscale(t.unit)
end

_scandescriptor(t::AC.DictionaryType) = _scandescriptor(t.valuetype)
_scandescriptor(t::AC.ArrowType) = t

function _scanfacadetostorage(t::AC.ArrowType, v)
    result = _exactfacadescalar(_scandescriptor(t), v)
    return result === nothing ? (false, v) : (true, result)
end

function _storagevalue(f::AC.Field, v, op)
    if !_hasarrowtypesextension(f)
        # A root temporal descriptor can lower a scalar literal. A temporal
        # conversion nested inside a container cannot: it would require a
        # semantics-aware recursive rewrite of the caller's composite value.
        _nativefacadeconversion(f) || return true, v
        _nativefacadeconversion(f.type) || return false, v
        _scancomparisonpreserving(f.type, op) || return false, v
        _scanliteralcompatible(f.type, v) || return false, v
        return _scanfacadetostorage(f.type, v)
    end
    # ArrowTypes does not require `toarrow` to preserve Julia comparison
    # semantics. A logical type may, for example, compare by an equivalence
    # class while storing one concrete identifier. Evaluate every registered
    # extension filter, including one nested below an unmarked container, over
    # the restored public values unless a future interface provides an
    # explicit comparison-preserving trait.
    return false, v
end

function _fieldfor(fields, ref, names)
    ref isa Int && 1 <= ref <= length(fields) && return fields[ref]
    i = findfirst(==(Symbol(ref)), names)
    return i === nothing ? nothing : fields[i]
end

_loweringdescriptor(t::AC.DictionaryType) = _loweringdescriptor(t.valuetype)
_loweringdescriptor(t::AC.ArrowType) = t

@noinline function _lowersequence(
    token::Val{K},
    descriptor::AC.ArrowType,
    values::V,
    budget,
) where {K,V<:Union{Tuple,Array}}
    # Tuple and Array membership both use `==`. A closed storage vector keeps
    # that contract without synthesizing a request-width Tuple type or boxing
    # every lowered temporal scalar.
    for x in values
        _scanliteralcompatible(descriptor, x) || return false, values
        _exactfacadevalue(token, x) === nothing && return false, values
    end
    _chargevector!(budget, _LoweredTemporal, length(values), "lowered membership values")
    out = Vector{_LoweredTemporal}(undef, length(values))
    for (i, x) in enumerate(values)
        out[i] = _exactfacadevalue(token, x)::_LoweredTemporal
    end
    return true, out
end

function _lowermembership(f::AC.Field, values::Union{Tuple,Array}, budget)
    # A temporal conversion nested inside a public container is not a scalar
    # membership conversion. Match `_storagevalue` and decline it before any
    # output is allocated.
    _nativefacadeconversion(f.type) || return false, values
    _scancomparisonpreserving(f.type, Tables.OP_EQ) || return false, values
    descriptor = _loweringdescriptor(f.type)
    return _lowersequence(_facadetoken(descriptor), descriptor, values, budget)
end

@noinline function _lowerset(token::Val{K}, publictype, values::Set, budget) where {K}
    for x in values
        x isa publictype || return false, values
        _exactfacadevalue(token, x) === nothing && return false, values
    end
    _chargedict!(
        budget,
        _LoweredTemporal,
        Nothing,
        length(values),
        "lowered membership set",
    )
    out = Set{_LoweredTemporal}()
    sizehint!(out, length(values))
    for x in values
        # Set membership uses `isequal` plus hashing. A cross-type public
        # value can compare `==` (Date and midnight DateTime) while remaining
        # a distinct Set key. Lower only canonical public-domain members.
        push!(out, _exactfacadevalue(token, x)::_LoweredTemporal)
    end
    return true, out
end

function _lowermembership(f::AC.Field, values::Set, budget)
    _nativefacadeconversion(f.type) || return false, values
    _scancomparisonpreserving(f.type, Tables.OP_EQ) || return false, values
    descriptor = _loweringdescriptor(f.type)
    return _lowerset(_facadetoken(descriptor), _facadebasetype(descriptor), values, budget)
end

_lowermembership(::AC.Field, values, budget) = (false, values)

@inline function _chargescannode!(budget, e, what)
    _chargeobject!(budget, max(sizeof(e), 64), what)
    return nothing
end

function _lowerexpr(e, fields, names, ok::Base.RefValue{Bool}, budget=nothing)
    e === nothing && return nothing
    if e isa Tables.Cmp
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return e
        good, v = _storagevalue(f, e.rhs, e.op)
        good || (ok[] = false)
        _chargescannode!(budget, e, "lowered comparison node")
        return Tables.Cmp(e.op, e.lhs, v)
    elseif e isa Tables.In
        f = _fieldfor(fields, e.lhs.ref, names)
        f === nothing && return e
        # Keep the caller's collection object. Rebuilding it as a Tuple would
        # change membership semantics: Set uses `isequal` (NaN and signed zero
        # matter), while Tuple and Array use `==`. Do not even iterate an
        # arbitrary membership object: `in(x, values)` is the Tables contract,
        # and `values` may be non-iterable or stateful. A field whose public
        # values require conversion can lower only whitelisted Base containers
        # whose membership semantics we can retain.
        if _hasarrowtypesextension(f)
            ok[] = false
            return e
        end
        _nativefacadeconversion(f) || return e
        good, values = _lowermembership(f, e.values, budget)
        if !good
            ok[] = false
            return e
        end
        _chargescannode!(budget, e, "lowered membership node")
        return Tables.In(e.lhs, values)
    elseif e isa Union{Tables.IsNull,Tables.StrPred}
        f = _fieldfor(fields, e.lhs.ref, names)
        if f !== nothing
            _hasarrowtypesextension(f) && (ok[] = false)
            e isa Tables.StrPred && _nativefacadeconversion(f) && (ok[] = false)
        end
        return e
    elseif e isa Tables.AndExpr
        _chargevector!(budget, Tables.ScanExpr, length(e.args), "lowered filter arguments")
        _chargescannode!(budget, e, "lowered conjunction node")
        return Tables.AndExpr(
            Tables.ScanExpr[_lowerexpr(a, fields, names, ok, budget) for a in e.args],
        )
    elseif e isa Tables.OrExpr
        _chargevector!(budget, Tables.ScanExpr, length(e.args), "lowered filter arguments")
        _chargescannode!(budget, e, "lowered disjunction node")
        return Tables.OrExpr(
            Tables.ScanExpr[_lowerexpr(a, fields, names, ok, budget) for a in e.args],
        )
    elseif e isa Tables.NotExpr
        _chargescannode!(budget, e, "lowered negation node")
        return Tables.NotExpr(_lowerexpr(e.arg, fields, names, ok, budget))
    end
    return e
end

"""
One scan plan resolved against one Arrow schema. `names` is the owned source
schema identity used by every executor. `public` is the request in the public
domain. `storage` is the same selection and window with an exactly lowered
storage-domain filter and no type overrides; `nothing` means the filter must
run after facade conversion.
"""
struct _ScanPlan
    names::Vector{Symbol}
    public::Tables.BoundScan
    storage::Union{Nothing,Tables.BoundScan}
end

"One storage-domain request and the schema names it was compiled against."
struct _BoundScanPlan
    names::Vector{Symbol}
    bound::Tables.BoundScan
end

function _alluniquenames(names, budget=nothing)
    _chargedict!(budget, Symbol, Nothing, length(names), "scan name-uniqueness index")
    return allunique(names)
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

function _resolverstringref(ref::String, budget)
    # Tables resolves a String reference through `Symbol(ref)`. Charge the
    # interned name before performing the same lookup during preflight; the
    # later upstream call then reuses that interned symbol.
    bytes = AC.checked_add(Int64(256), AC._materializedobjectbytes(sizeof(ref)))
    budget === nothing || _charge!(budget, bytes, "scan resolver String reference")
    return Symbol(ref)
end

function _resolverregexwork!(names, budget)
    # Both this allocation-free-counting pass and Tables._findcols convert
    # every Symbol to a String for Regex matching. Reserve both conversions
    # before the first one occurs, plus the upstream findall result at its
    # worst-case width.
    stringbytes = Int64(0)
    for name in names
        stringbytes = AC.checked_add(
            stringbytes,
            AC.checked_add(Int64(256), AC._materializedobjectbytes(sizeof(name))),
        )
    end
    budget === nothing ||
        _charge!(budget, AC.checked_mul(Int64(2), stringbytes), "scan resolver Regex names")
    capacity = AC.checked_mul(Int64(2), Int64(length(names)))
    _chargevector!(budget, Int, capacity, "scan resolver Regex matches")
    return nothing
end

"Count one selection reference while reserving Tables.resolve's temporary work."
function _resolverrefcount(ref, names, budget)
    if ref isa Regex
        _resolverregexwork!(names, budget)
        matches = Int64(0)
        for name in names
            occursin(ref, String(name)) && (matches = AC.checked_add(matches, Int64(1)))
        end
        return matches
    end
    _chargevector!(budget, Int, 1, "scan resolver reference match")
    target = ref isa String ? _resolverstringref(ref, budget) : ref
    if target isa Int
        return 1 <= target <= length(names) ? Int64(1) : Int64(0)
    end
    for name in names
        name == target && return Int64(1)
    end
    return Int64(0)
end

function _resolverselectionwork!(scan::Tables.Scan, names, budget)
    select = scan.select
    isempty(select) && return Int64(0)
    if first(select).ref isa Tables.Not
        _chargedict!(budget, Int, Nothing, length(names), "scan resolver exclusion index")
        for item in select
            rawrefs = (item.ref::Tables.Not).ref
            nrefs = rawrefs isa Union{Tuple,AbstractVector} ? length(rawrefs) : 1
            _chargevector!(budget, Any, nrefs, "scan resolver Not references")
            if rawrefs isa Union{Tuple,AbstractVector}
                for ref in rawrefs
                    _resolverrefcount(ref, names, budget)
                end
            else
                _resolverrefcount(rawrefs, names, budget)
            end
        end
        # Exclusion can only reduce the full-schema output. Reserving the
        # upper bound avoids building our own duplicate exclusion set.
        return Int64(length(names))
    end
    output = Int64(0)
    for item in select
        count =
            item.ref isa Tables.All ? Int64(length(names)) :
            _resolverrefcount(item.ref, names, budget)
        output = AC.checked_add(output, count)
    end
    return output
end

function _resolverfilterwork!(e, names, budget)
    e === nothing && return nothing
    if e isa Tables.Col
        e.ref isa String && _resolverstringref(e.ref, budget)
        _chargescannode!(budget, e, "scan resolver filter node")
    elseif e isa Union{Tables.Cmp,Tables.In,Tables.IsNull,Tables.StrPred}
        _resolverfilterwork!(e.lhs, names, budget)
        _chargescannode!(budget, e, "scan resolver filter node")
    elseif e isa Union{Tables.AndExpr,Tables.OrExpr}
        _chargevector!(
            budget,
            Tables.ScanExpr,
            length(e.args),
            "scan resolver filter arguments",
        )
        for arg in e.args
            _resolverfilterwork!(arg, names, budget)
        end
        _chargescannode!(budget, e, "scan resolver filter node")
    elseif e isa Tables.NotExpr
        _resolverfilterwork!(e.arg, names, budget)
        _chargescannode!(budget, e, "scan resolver filter node")
    end
    return nothing
end

function _resolvescan(scan::Tables.Scan, names, budget=nothing)
    # Preflight every request-directed allocation that Tables.resolve owns.
    # Selection may expand far beyond schema width through repeated aliases;
    # Regex/Not references own temporary vectors and String work; and filter
    # resolution reconstructs the full expression tree. No upstream compiler
    # work starts until the cumulative operation budget accepts all of it.
    n = length(names)
    _chargevector!(budget, Symbol, n, "scan resolver names")
    output = _resolverselectionwork!(scan, names, budget)
    # Tables grows BoundColumn[] through append!. Reserve the worst-case
    # geometric capacity separately from Julia's backing-store rounding.
    columncapacity = output == 0 ? Int64(0) : AC.checked_mul(Int64(2), output)
    _chargevector!(budget, Tables.BoundColumn, columncapacity, "scan resolver columns")
    _chargedict!(budget, Symbol, Nothing, output, "scan resolver output-name index")
    refcapacity = n == 0 ? Int64(0) : AC.checked_mul(Int64(2), Int64(n))
    _chargevector!(budget, Int, refcapacity, "scan resolver filter references")
    _resolverfilterwork!(scan.filter, names, budget)
    _chargeobject!(budget, 64, "resolved scan")
    return Tables.resolve(scan, names)
end

function _compileboundscan(scan::Tables.Scan, fields, budget=nothing)
    names = _fieldnamesymbols(fields, budget)
    _alluniquenames(names, budget) || throw(
        AC.ValidationError(
            "scan pushdown over duplicate column names is not supported; read the file without a scan",
        ),
    )
    return _BoundScanPlan(names, _resolvescan(scan, names, budget))
end

function _ScanPlan(scan::Tables.Scan, fields, budget=nothing)
    compiled = _compileboundscan(scan, fields, budget)
    names = compiled.names
    b = compiled.bound
    # The facade keeps zero-field sources on its metadata-count path. Avoid
    # compiling a storage form that no adapter will consume.
    isempty(fields) && return _ScanPlan(names, b, nothing)
    ok = Ref(true)
    lowered = _lowerexpr(b.filter, fields, names, ok, budget)
    ok[] || return _ScanPlan(names, b, nothing)
    _chargevector!(budget, Tables.BoundColumn, length(b.columns), "storage scan columns")
    columns =
        Tables.BoundColumn[Tables.BoundColumn(c.index, c.name, nothing) for c in b.columns]
    _chargevector!(budget, Int, length(b.filtercols), "storage scan filter columns")
    _chargeobject!(budget, 64, "storage scan")
    storage = Tables.BoundScan(
        columns,
        lowered,
        copy(b.filtercols),
        b.limit,
        b.offset,
        b.validate,
    )
    return _ScanPlan(names, b, storage)
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
    i === nothing && throw(ArgumentError("no column $(repr(name)) in this scan result"))
    return getfield(t, :columns)[i]
end
Tables.rowcount(t::_ScanColumns) = getfield(t, :nrows)
function Tables.schema(t::_ScanColumns)
    names = getfield(t, :names)
    types = Type[eltype(c) for c in getfield(t, :columns)]
    # Preserve the compact typed schema for ordinary narrow results. Wide
    # schemas stay in values so compiler work cannot scale with input names.
    return Tables.Schema(names, types; stored=length(names) > 256)
end
Base.propertynames(t::_ScanColumns) = getfield(t, :names)
Base.getproperty(t::_ScanColumns, name::Symbol) = Tables.getcolumn(t, name)

function _ScanColumns(columns::NamedTuple, nrows::Int)
    names = collect(Symbol, propertynames(columns))
    return _ScanColumns(
        names,
        AbstractVector[getfield(columns, i) for i in eachindex(names)],
        nrows,
    )
end

# Join the pieces of one decoded output column under its declared storage
# claim when that claim closes, else as `Vector{Any}`. A facade scan can put
# private ArrowTypes routing values in those pieces; they remain inside the
# facade scan operation and are lifted before it returns. Building the empty
# result from the same rule keeps conforming columns stable when a scan keeps
# no rows. Nulls under a non-nullable declaration are advisory at the reader
# tier. An unread violating batch does not widen a selected result: preserving
# the skip boundary takes priority over propagating invalid type evidence.

function _joinscanparts(f::Field, parts::Vector, budget=nothing)
    if isempty(parts)
        # Match `_storagebatchcolumn`: a zero-row result must not change its
        # public Tables.Schema based on whether a window avoided decoding or
        # a filter decoded an empty slice. Dynamic composite routes stay Any;
        # closed scalar routes retain their declared element type.
        ET = _storageelementclaim(f)
        _chargevector!(budget, ET, 0, "empty scan column")
        return Vector{ET}()
    end
    length(parts) == 1 && return parts[1]
    total = sum(length, parts; init=0)
    T = eltype(first(parts))
    for part in Iterators.drop(parts, 1)
        T = Base.promote_typejoin(T, eltype(part))
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

function _scanbatch(f::ArrowFile, i::Int, mask::AbstractVector{Bool}, state::DecodeState)
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, state.budget)
    return _maskedrecord(
        fm.msg,
        fm.version,
        fm.body,
        f.fields,
        f.dictionaries,
        f.fielddictids,
        f.validated,
        f.limits,
        f.schemaversion,
        mask,
        state,
    )
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
The exact consumer of a bound scan, one decoded batch at a time: the filter
is evaluated over the batch's decoded columns by the generic evaluator
(`Tables.filtermask` — the same three-valued semantics the executor has),
`offset`/`limit` are composed over qualifying rows with saturating
arithmetic (the executor's rule) and stop the scan the moment the window is
full, and only the selected columns' surviving rows are kept, in selection
order under their output names. Without a filter the caller's metadata
window (`_batchwindow`) already names the rows, so a batch outside it is
never decoded.
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

# The rows of one batch the scan keeps: the caller's metadata window when
# there is no filter, else the qualifying rows after this batch's share of
# the offset/limit. A filter that references no decoded column is
# row-invariant and evaluates once.
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
        filtermask = Tables.filtermask(sink.bound, raw)
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
    _chargedict!(
        sink.budget,
        Int,
        AbstractVector,
        length(sink.decodeidx),
        "scan decoded-column index",
    )
    decoded = Dict{Int,AbstractVector}()
    sizehint!(decoded, length(sink.decodeidx))
    for idx in sink.decodeidx
        f = sink.fields[idx]
        d = cols[idx]::ArrayData
        decoded[idx] = sink.materializecolumn(f, d, sink.budget)
    end
    rows = _sinkrows(sink, decoded, rblen, skip, take)
    sink.outrows = _addscanrows(sink.outrows, Int64(length(rows)))
    for (k, c) in enumerate(sink.columns)
        source = decoded[c.index]
        _chargevector!(sink.budget, eltype(source), length(rows), "scan column slice")
        partidx = sink.partcounts[k] + 1
        partidx <= length(sink.parts[k]) ||
            throw(AssertionError("scan consumed more batches than its planned capacity"))
        sink.parts[k][partidx] = source[rows]
        sink.partcounts[k] = partidx
    end
    return nothing
end

# The consumed bound operation's result in selection order. Direct-handle type
# overrides apply here. A facade storage plan has no overrides; its public
# plan applies them after conversion inside `_finishfacadescan`.
function _sinkresult(sink::_ScanSink)
    _chargevector!(sink.budget, Symbol, length(sink.columns), "scan output names")
    outnames = Symbol[c.name for c in sink.columns]
    outcols = (
        begin
            parts = sink.parts[k]
            resize!(parts, sink.partcounts[k])
            col = _joinscanparts(sink.fields[c.index], parts, sink.budget)
            c.type === nothing ? col : _applyoverride(c.type, col, sink.budget)
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
)
    if isempty(names)
        # Zero-field sources consume their filter and window here because an
        # empty NamedTuple cannot carry a row count. Header reads share one
        # cumulative allocation budget, like the column path.
        keep = _zerofieldpredicate(b.filter)
        n = _zerofieldwindow(
            (_batchrows(f, i, budget) for i = 1:length(f)),
            keep,
            b.limit,
            b.offset,
        )
        return _scantable(Symbol[], (), Int(n), budget)
    end
    sink = _ScanSink(b, names, f.fields, materializecolumn, budget)
    _chargebitvector!(budget, length(names), "scan decode mask")
    mask = falses(length(names))
    mask[sink.decodeidx] .= true
    state = DecodeState(budget)
    try
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
        # Statistics pruning: one-sided — a pruned batch is provably empty
        # under the filter; the rows of the rest are decided by the sink.
        _chargebitvector!(budget, length(f), "scan statistics decisions")
        keep = trues(length(f))
        if b.filter !== nothing
            stats = _readstats(
                f.schema.metadata,
                length(f),
                f.fields;
                limits=f.limits,
                budget=budget,
            )
            if stats !== nothing
                context = _statscontext(b.filter, names, budget)
                for i in eachindex(keep)
                    keep[i] =
                        _maypass(b.filter, stats[i].cols, names, stats[i].rows, context)
                end
            end
        end
        partcapacity =
            b.filter !== nothing && b.limit == 0 ? 0 :
            count(entry -> keep[first(entry)], window)
        _reservesinkparts!(sink, partcapacity)
        for (i, skip, take) in window
            _sinkopen(sink) || break
            keep[i] || continue
            rblen, cols = _scanbatch(f, i, mask, state)
            _consumebatch!(sink, cols, rblen, skip, take)
        end
        return _sinkresult(sink)
    finally
        close(state)
    end
end

# Direct-handle scans are storage-domain operations. Their interface cannot
# select the facade's private ArrowTypes routing path.
_applyscan(
    f::ArrowFile,
    plan::_BoundScanPlan,
    budget::AllocationBudget=AllocationBudget(f.limits.max_total_allocated_bytes),
) = _runboundscan(f, plan.bound, plan.names, budget, _storagebatchcolumn)

# ===========================================================================
# Byte-range reads — SourceFile over an AbstractArrowSource, the planner,
# and sparse decode
# ===========================================================================

"""
    SourceFile(src::AbstractArrowSource; limits, tailbytes=65536, coalesce_gap=262144)

The scan-driven, fetch-minimal file handle over a byte-range source:
`Tables.scan(sf, scan)` (and `Arrow.Table(src; scan=…)`, which builds one)
runs the fetch protocol. The footer normally comes from one cached tail read;
one exact cached follow-up is used when it escapes that window. Batch windowing
uses block metadata. Dictionary bodies are requested only for decode-set ids,
and per-buffer body ranges cover exactly the decode set before coalescing under
`coalesce_gap`. The source's length is read once, at construction.

Trust note, stated loudly: the ranged reader treats the FOOTER as the sole
schema authority — it does not fetch the leading magic, parse and
cross-check the leading schema message, or inspect the optional EOS marker.
The tail and coalesced requests may physically over-read unrequested bytes.
The full Footer Block index and global features/message limit are checked
up front. Per-record limits stay lazy; every surviving candidate's
metadata-only plan is validated before any planned body range is requested.
`limits.max_concurrent_reads` caps the source preference. One semaphore is
shared by every operation on this handle.
"""
struct SourceFile{S<:AbstractArrowSource}
    src::S
    len::Int64
    limits::Limits
    tailbytes::Int64
    coalesce_gap::Int64
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
    coalesce_gap::Integer=262144,
)
    _validatelimits(limits)
    gap = Int64(coalesce_gap)
    gap >= 0 || throw(ArgumentError("negative coalesce gap"))
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

# The whole object as bytes, reusing the cached tail for its final window.
function _wholeobject(sf::SourceFile, budget::Union{Nothing,AllocationBudget}=nothing)
    tail, tailstart = _fetchtail(sf, budget)
    tailstart == 0 && return tail
    _chargevector!(budget, UInt8, tailstart, "whole-object prefix fetch")
    prefix = _fetchexact(sf, Int64(0), tailstart)
    _chargevector!(budget, UInt8, sf.len, "whole-object assembly")
    return vcat(prefix, tail)
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
cheaper than another request round-trip. Returns file-coordinate spans.
"""
function _coalesce(ranges::Vector{NTuple{2,Int64}}, gap::Int64, budget=nothing)
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
        if off <= loend || off - loend <= gap
            sorted[outidx] = (loff, max(loend, thisend) - loff)
        else
            outidx += 1
            sorted[outidx] = (off, len)
        end
    end
    resize!(sorted, outidx)
    return sorted
end

"Fetched file-coordinate spans with their bytes, resolvable by containment."
struct FetchedSpans
    starts::Vector{Int64}
    lens::Vector{Int64}
    slices::Vector{BufferSlice}
end

function _fetchspans(
    sf::SourceFile,
    ranges::Vector{NTuple{2,Int64}},
    gap::Int64;
    budget::Union{Nothing,AllocationBudget}=nothing,
    what::AbstractString="range fetch",
)
    spans = _coalesce(ranges, gap, budget)
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

# Read one round's spans through the source, each length-checked, every
# result stored by its request index: serially, or through a worker pool of
# the handle's validated/capped worker count pulling requests off one counter — so a
# source's completion order can never permute payloads, and the number of
# reads in flight is bounded whatever the span count.
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
    i = searchsortedlast(fs.starts, off)
    (
        i >= 1 &&
        off >= fs.starts[i] &&
        AC.checked_add(off, len) <= AC.checked_add(fs.starts[i], fs.lens[i])
    ) || throw(ValidationError("required bytes [$off, $len] were not fetched"))
    return AC.subslice(fs.slices[i], off - fs.starts[i], len)
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

"""
Parse and verify one fetched block-metadata payload the way `_blockmessage`
does over a region: framing prefix, verified flatbuffer graph, declared
body length against the Block tuple.
"""
function _parseblockmeta(
    bytes::BufferSlice,
    block::NTuple{3,Int64},
    limits::Limits,
    budget::AllocationBudget,
)
    _, metalen, bodylen = block
    bytes.len == metalen ||
        throw(ValidationError("footer block metadata fetch length mismatch"))
    metalen >= 16 || throw(ValidationError("footer block has invalid extents"))
    AC.loadat(bytes, UInt32, Int64(0)) == CONTINUATION ||
        throw(ValidationError("footer block does not point at a message"))
    declared = Int64(AC.loadat(bytes, Int32, Int64(4)))
    declared == metalen - 8 ||
        throw(ValidationError("footer block metadata length does not match the message"))
    0 < declared <= limits.max_metadata_bytes || throw(
        ValidationError(
            "metadata length $declared outside (0, $(limits.max_metadata_bytes)]",
        ),
    )
    0 <= bodylen <= limits.max_body_bytes ||
        throw(ValidationError("body length $bodylen outside [0, $(limits.max_body_bytes)]"))
    _chargevector!(budget, UInt8, declared, "metadata allocation")
    metabytes = AC.slicebytes(AC.subslice(bytes, 8, declared))
    version, header_type, _, _ = _verify_ipc_metadata_budgeted(metabytes, limits, budget)
    msg = FB.getrootas(Meta.Message, metabytes, 0)
    Int64(msg.bodyLength) == bodylen ||
        throw(ValidationError("footer block body length does not match the message"))
    return msg, version, header_type
end

_parseblockmeta(bytes::Vector{UInt8}, block, limits, budget) =
    _parseblockmeta(BufferSlice(heapregion(bytes), 0, length(bytes)), block, limits, budget)

"Fetch and verify the ranged footer: schema, fields, blocks, id table."
function _rangedfooter(sf::SourceFile, budget::AllocationBudget)
    limits = sf.limits
    _requirelittleendian()
    _validatelimits(limits)
    L = sf.len
    L >= Int64(8 + 8 + 4 + 6) ||
        throw(ValidationError("file is too short to be an IPC file"))
    tail, tailstart = _fetchtail(sf, budget)
    tail[(end - 5):end] == Vector{UInt8}(FILE_MAGIC) ||
        throw(ValidationError("missing trailing ARROW1 magic"))
    footerlen = Int64(reinterpret(Int32, tail[(end - 9):(end - 6)])[1])
    0 < footerlen <= limits.max_metadata_bytes || throw(
        ValidationError(
            "footer length $footerlen outside (0, $(limits.max_metadata_bytes)]",
        ),
    )
    footerstart = L - 10 - footerlen
    footerstart >= 8 || throw(ValidationError("footer escapes the file"))

    footerbytes = if footerstart >= tailstart
        _chargevector!(budget, UInt8, footerlen, "footer allocation")
        tail[(footerstart - tailstart + 1):(footerstart - tailstart + footerlen)]
    else
        _fetchfooter(sf, footerstart, footerlen, budget)
    end
    version, features, dictblocks, recordblocks, _ =
        _verify_footer_budgeted(footerbytes, limits, budget)
    Int64(1) in features &&
        throw(ValidationError("dictionary replacement is forbidden in the IPC file format"))
    nmessages = AC.checked_add(
        Int64(1),
        AC.checked_add(Int64(length(dictblocks)), Int64(length(recordblocks))),
    )
    nmessages <= limits.max_messages ||
        throw(ValidationError("message count exceeds limit"))
    footer = FB.getrootas(Meta.Footer, footerbytes, 0)
    metaschema = footer.schema
    metaschema === nothing && throw(ValidationError("file footer carries no schema"))
    something(metaschema.endianness, Meta.Endianness.Little) == Meta.Endianness.Little ||
        throw(
            ValidationError(
                "big-endian IPC is not supported (no endianness normalization)",
            ),
        )
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()
    fields = Field[
        corefield(f, dictids, fielddictids) for
        f in something(metaschema.fields, Meta.Field[])
    ]
    foreach(validateschemafield, fields)
    dictvaluefields = validatedictionaryids(fields, fielddictids)
    sch = Schema(
        fields;
        metadata=coremetadata(metaschema.custom_metadata),
        endianness=AC.LittleEndian,
    )
    return (;
        sch,
        fields,
        dictids,
        fielddictids,
        dictvaluefields,
        version,
        dictblocks,
        recordblocks,
        footerstart,
        metaschema,
    )
end

"""
One record block's row count for the zero-field ranged path — the SAME
frame discipline as the column path's metadata pass: extent bounds before
the fetch, the fetch and parse charged to the caller's cumulative budget,
`_parseblockmeta` framing (continuation prefix, declared length, verified
graph, body-length cross-check), header kind, footer-version agreement,
and compression rejection.
"""
function _zerofieldblockcount(
    sf::SourceFile,
    block::NTuple{3,Int64},
    version::Int16,
    fields::Vector{Field},
    budget::AllocationBudget,
)
    _, metalen, bodylen = block
    declared = metalen - 8
    0 < declared <= sf.limits.max_metadata_bytes || throw(
        ValidationError(
            "metadata length $declared outside (0, $(sf.limits.max_metadata_bytes)]",
        ),
    )
    0 <= bodylen <= sf.limits.max_body_bytes || throw(
        ValidationError("body length $bodylen outside [0, $(sf.limits.max_body_bytes)]"),
    )
    _chargevector!(budget, UInt8, metalen, "metadata range fetch")
    payload = _fetchexact(sf, block[1], metalen)
    msg, v, header_type = _parseblockmeta(payload, block, sf.limits, budget)
    header_type == UInt8(3) ||
        throw(ValidationError("footer record block is not a record batch"))
    v == version || throw(ValidationError("IPC metadata version changes within the file"))
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
    if isempty(names)
        # Zero-field sources consume their filter and window here because an
        # empty NamedTuple cannot carry a row count. The metadata-only read
        # keeps the column path's trust boundary: the block index validates
        # first, every touched block passes the full frame checks, and every
        # fetch charges the one cumulative budget.
        _validateblockindex(dictblocks, recordblocks, footerstart; datastart=8)
        # A zero-field schema declares no dictionary ids, so every indexed
        # dictionary block is orphaned — the same rejection the id-membership
        # check produces on the column path and in the full reader.
        isempty(dictblocks) || throw(
            ValidationError(
                "dictionary batch has no declaring field in a zero-field schema",
            ),
        )
        keep = _zerofieldpredicate(b.filter)
        n = _zerofieldwindow(
            (
                _zerofieldblockcount(sf, block, version, fields, budget) for
                block in recordblocks
            ),
            keep,
            b.limit,
            b.offset,
        )
        return _scantable(Symbol[], (), Int(n), budget)
    end
    sink = _ScanSink(b, names, fields, materializecolumn, budget)
    decodeidx = sink.decodeidx
    _chargebitvector!(budget, length(names), "scan decode mask")
    mask = falses(length(names))
    mask[decodeidx] .= true

    # Footer Blocks remain mutually exclusive and bounded without parsing the
    # leading schema or optional EOS bytes. A tail request may over-read them.
    _validateblockindex(dictblocks, recordblocks, footerstart; datastart=8)

    # Statistics pruning happens FIRST: the stats live in the
    # footer schema's metadata, so pruned batches cause no block-metadata range
    # request. Tail reads may still over-read them. Pruning applies only under
    # a filter, and the window applies only without one, so they never interact.
    nrec = length(recordblocks)
    _chargebitvector!(budget, nrec, "scan statistics decisions")
    keep = trues(nrec)
    if b.filter !== nothing
        stats = _readstats(ft.sch.metadata, nrec, fields; limits=limits, budget=budget)
        if stats !== nothing
            context = _statscontext(b.filter, names, budget)
            for i in eachindex(keep)
                keep[i] = _maypass(b.filter, stats[i].cols, names, stats[i].rows, context)
            end
        end
    end
    _chargevector!(budget, Int, count(keep), "statistics-surviving batch indexes")
    recidxs = Int[i for i = 1:nrec if keep[i]]

    # One coalesced metadata pass over the dictionary blocks and the
    # SURVIVING record blocks; bodies come later and only for what the scan
    # needs.
    nmeta = length(dictblocks) + length(recidxs)
    _chargevector!(budget, NTuple{3,Int64}, nmeta, "scan metadata blocks")
    metablocks = NTuple{3,Int64}[]
    sizehint!(metablocks, nmeta)
    append!(metablocks, dictblocks)
    append!(metablocks, (recordblocks[i] for i in recidxs))
    # Match ArrowFile's lazy record limits: statistics-pruned records never
    # become candidates. Every candidate is bounded before its metadata fetch.
    for block in metablocks
        _, metalen, bodylen = block
        declared = metalen - 8
        0 < declared <= limits.max_metadata_bytes || throw(
            ValidationError(
                "metadata length $declared outside (0, $(limits.max_metadata_bytes)]",
            ),
        )
        0 <= bodylen <= limits.max_body_bytes || throw(
            ValidationError("body length $bodylen outside [0, $(limits.max_body_bytes)]"),
        )
    end
    _chargevector!(budget, NTuple{2,Int64}, nmeta, "metadata range plan")
    metaranges = NTuple{2,Int64}[(bl[1], bl[2]) for bl in metablocks]
    metaspans = _fetchspans(
        sf,
        metaranges,
        sf.coalesce_gap;
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
        msg, v, header_type = _parseblockmeta(payload, block, limits, budget)
        expected_dict = i <= length(dictblocks)
        (expected_dict ? header_type == UInt8(2) : header_type == UInt8(3)) || throw(
            ValidationError(
                expected_dict ? "footer dictionary block is not a dictionary batch" :
                "footer record block is not a record batch",
            ),
        )
        v == version ||
            throw(ValidationError("IPC metadata version changes within the file"))
        rejectexperimentalcompression(msg, v, header_type)
        blockmeta[i] = (msg, v)
    end

    # RecordBatch lengths live in block metadata, not the Footer. The metadata
    # pass above is required before limit/offset can choose body ranges.
    nsurv = length(recidxs)
    _chargevector!(budget, Meta.RecordBatch, nsurv, "record-batch headers")
    headers =
        [blockmeta[length(dictblocks) + p][1].header::Meta.RecordBatch for p = 1:nsurv]
    _chargevector!(budget, Vector{Int64}, nsurv, "record variadic-count cache")
    recordvariadics = Vector{Vector{Int64}}(undef, nsurv)
    _chargevector!(budget, Int64, nsurv, "scan batch row counts")
    rowcounts = Vector{Int64}(undef, nsurv)
    for (p, h) in enumerate(headers)
        variadics = variadiccounts(h)
        recordvariadics[p] = variadics
        rowcounts[p] =
            _recordbatchmeta(h, fields, limits, recordblocks[recidxs[p]][3], variadics)
    end
    windowed = b.filter === nothing && (b.limit !== nothing || b.offset > 0)
    window =
        windowed ? _batchwindow(rowcounts, b.offset, b.limit, budget) :
        begin
            _chargevector!(budget, Tuple{Int,Int64,Int64}, nsurv, "scan batch window")
            Tuple{Int,Int64,Int64}[(p, Int64(0), Int64(-1)) for p = 1:nsurv]
        end
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
        header.isDelta && throw(ValidationError("delta dictionaries are not supported"))
        haskey(dictids, header.id) ||
            throw(ValidationError("dictionary batch has unknown id $(header.id)"))
        header.id in seenids &&
            throw(ValidationError("the file format carries one dictionary batch per id"))
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
    # One body round: the selected dictionary bodies and the selected record
    # buffers are planned together and fetched in a single pass; the
    # dictionaries decode first from the shared spans.
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
    for (p, _, _) in window
        block = recordblocks[recidxs[p]]
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
    end
    rangeidx == length(bodyranges) ||
        throw(AssertionError("body range preflight count changed during planning"))
    bodyspans =
        _fetchspans(sf, bodyranges, sf.coalesce_gap; budget=budget, what="body range fetch")

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
                validate_semantic(vf, decoded)
                validated[decoded] = nothing
                dicts[header.id] = decoded
            end
        end

        for (p, skip, take) in window
            _sinkopen(sink) || break
            block = recordblocks[recidxs[p]]
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

# The ranged direct-handle operation is storage-only for the same reason as
# `_applyscan(::ArrowFile, ...)` above.
_applyscan(sf::SourceFile, plan::_BoundScanPlan, ft, budget::AllocationBudget) =
    _runboundscan(sf, plan.bound, plan.names, ft, budget, _storagebatchcolumn)

"Compile one direct handle request in the storage domain."
function _compilehandlescan(scan::Tables.Scan, fields, budget=nothing)
    return _compileboundscan(scan, fields, budget)
end

# Tables.jl currently exposes binding (`resolve`), predicate evaluation, and
# allocation, but no executor for an existing BoundScan. Keep this small local
# executor instead of reconstructing a Scan and resolving it a second time.
# The differential battery pins it to Tables.scan until the prerequisite API
# provides a bound-plan execution seam.
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
        filtermask = Tables.filtermask(b, cols)
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
            _chargevector!(budget, eltype(source), length(rows), "scan column slice")
            col = source[rows]
            c.type === nothing ? col : _applyoverride(c.type, col, budget)
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
    b = plan.public
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
    _chargevector!(budget, AC.OwnerRegion, length(regions), "scan owner regions")
    return _table(names, columns, bound, AC.OwnerRegion[regions...], n, nothing, budget)
end

"""
The OUTPUT schema of a scan: bound source fields under their output names.
`precols` supplies each output's pre-override (facade-narrowed) column, so
override keep/drop follows the SAME actual-subtype decision the conversion
made: a no-op override keeps its retained field; a real conversion drops it
(a later rewrite re-infers the column).
"""
function _boundschema(schema, sourcefields, b::Tables.BoundScan, precols, budget=nothing)
    schema === nothing && return nothing
    _chargevector!(budget, AC.Field, length(b.columns), "scan output fields")
    outfields = AC.Field[]
    sizehint!(outfields, length(b.columns))
    for (i, bc) in enumerate(b.columns)
        f = sourcefields[bc.index]
        if bc.type !== nothing && i <= length(precols)
            D = eltype(precols[i])
            D <: Union{bc.type,Missing} || continue
        end
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

"Finish a facade scan by converting its operation-local raw result once."
function _finishfacadescan(
    got,
    schema,
    sourcefields,
    plan::_ScanPlan;
    regions=AC.OwnerRegion[],
    budget=nothing,
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
    b = plan.public
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
            # Public type overrides run HERE, after facade conversion —
            # they are public-domain requests, never storage casts, and
            # they preserve missing exactly as Tables.scan does.
            base = _facadefromraw(f, columns[i], arrowtypes)
            push!(precols, base)
            columns[i] = bc.type === nothing ? base : _applyoverride(bc.type, base, budget)
        end
    end
    nrows = isempty(columns) ? _scanrowcount(got) : length(columns[1])
    bound = _boundschema(schema, sourcefields, b, precols, budget)
    _chargevector!(budget, AC.OwnerRegion, length(regions), "scan owner regions")
    return _table(names, columns, bound, AC.OwnerRegion[regions...], nrows, nothing, budget)
end

# The facade executor only accepts the override-free storage half of a plan.
# Keep that coupled invariant at the one operation that needs both halves.
function _facadestorage(plan::_ScanPlan)
    b = plan.storage
    b === nothing && throw(AssertionError("facade scan has no storage-domain plan"))
    p = plan.public
    length(b.columns) == length(p.columns) ||
        throw(AssertionError("facade storage selection width changed"))
    all(
        bc.type === nothing && bc.index == pc.index && bc.name == pc.name for
        (bc, pc) in zip(b.columns, p.columns)
    ) || throw(AssertionError("facade storage selection retained a public override"))
    b.filtercols == p.filtercols ||
        throw(AssertionError("facade storage filter references changed"))
    (b.limit == p.limit && b.offset == p.offset && b.validate == p.validate) ||
        throw(AssertionError("facade storage window or validation mode changed"))
    return b
end

# These facade operations own the full private route: create ArrowTypes Union
# markers while decoding, consume them during public conversion, and return a
# Table from the public domain. No marker-bearing intermediate crosses into
# `table.jl`, and direct `_applyscan` cannot select this materializer.
function _applyfacadescan(
    f::ArrowFile,
    plan::_ScanPlan,
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    b = _facadestorage(plan)
    actual =
        budget === nothing ? AllocationBudget(f.limits.max_total_allocated_bytes) : budget
    routes = _ArrowTypesRoutePlan(actual)
    materialize = (field, data, _) -> _batchcolumn(field, data, routes)
    got = _runboundscan(f, b, plan.names, actual, materialize)
    return _finishfacadescan(
        got,
        f.schema,
        f.fields,
        plan;
        regions=AC.OwnerRegion[f.region],
        budget=actual,
    )
end

function _applyfacadescan(sf::SourceFile, plan::_ScanPlan, ft, budget::AllocationBudget)
    routes = _ArrowTypesRoutePlan(budget)
    materialize = (field, data, _) -> _batchcolumn(field, data, routes)
    got = _runboundscan(sf, _facadestorage(plan), plan.names, ft, budget, materialize)
    return _finishfacadescan(got, ft.sch, ft.fields, plan; budget=budget)
end

function _facadescanrawcolumn(s::IPCStream, i::Int, budget, routes::_ArrowTypesRoutePlan)
    f = s.corefields[i]
    _chargevector!(budget, AbstractVector, length(s.batches), "stream scan column parts")
    parts = [_batchcolumn(f, b.columns[i], routes) for b in s.batches]
    return _joinscanparts(f, parts, budget)
end

function _applyfacadescan(
    s::IPCStream,
    plan::_ScanPlan,
    supplied::Union{Nothing,AllocationBudget}=nothing,
)
    budget = supplied === nothing ? s.budget : supplied
    b = _facadestorage(plan)
    _chargevector!(budget, Field, length(s.corefields), "stream scan source fields")
    fields = collect(Field, s.corefields)
    _chargevector!(budget, AbstractVector, length(fields), "stream scan columns")
    routes = _ArrowTypesRoutePlan(budget)
    rawcolumns =
        AbstractVector[_facadescanrawcolumn(s, i, budget, routes) for i = 1:length(fields)]
    nrows =
        isempty(rawcolumns) ? sum(Int(b.nrows) for b in s.batches; init=0) :
        length(first(rawcolumns))
    raw = _ScanColumns(plan.names, rawcolumns, nrows)
    got = _executeplan(raw, b, plan.names, budget)
    return _finishfacadescan(
        got,
        s.schema,
        fields,
        plan;
        regions=_sourceregions(s, budget),
        budget=budget,
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
    @inbounds for i in eachindex(col)
        x = col[i]
        out[i] = ismissing(x) ? missing : convert(T, x)
    end
    return out
end

"""
    Tables.scan(f::ArrowFile, scan)
    Tables.scan(sf::SourceFile, scan)

Scan an Arrow file handle: `_applyscan` decodes only the selected and
filter-referenced columns of the batches the footer statistics and the
window admit, evaluates the filter per batch, composes `offset`/`limit`
exactly (stopping as soon as the window is full), and builds the selected
columns under their output names. The request is compiled once against the
file schema; the scan plan then owns type overrides too. File and ranged
`Arrow.Table(source; scan=…)` use the same kernel through the closed
`_applyfacadescan` operation. Stream facade scans execute post-decode. Every
facade path converts private ArrowTypes routes before it returns.
"""
function Tables.scan(f::ArrowFile, scan::Tables.Scan)
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    return _applyscan(f, _compilehandlescan(scan, f.fields, budget), budget)
end

function Tables.scan(sf::SourceFile, scan::Tables.Scan)
    budget = AllocationBudget(sf.limits.max_total_allocated_bytes)
    ft = _rangedfooter(sf, budget)
    return _applyscan(sf, _compilehandlescan(scan, ft.fields, budget), ft, budget)
end

# ===========================================================================
# Per-batch statistics — the official value layout in a footer key
# ===========================================================================

# Placement is OUR convention (the statistics-schema spec's non-goals
# explicitly exclude placement); the VALUE layout is the official one:
#     struct<column: int32, statistics:
#            map<dictionary<utf8, int32>, dense_union<int64,float64,utf8,bool>>>
# serialized as one embedded IPC stream with one statistics record batch per
# data record batch, base64-wrapped into schema-level custom metadata so the
# tail fetch alone powers pruning.
const STATS_KEY = "JuliaArrow:batch_statistics.v1"
const STATS_ROW_COUNT = "ARROW:row_count:exact"
const STATS_NULL_COUNT = "ARROW:null_count:exact"
const STATS_MIN = "ARROW:min_value:exact"
const STATS_MAX = "ARROW:max_value:exact"
const STATS_KEYPOOL = [STATS_ROW_COUNT, STATS_NULL_COUNT, STATS_MIN, STATS_MAX]
const _ColumnStats =
    NamedTuple{(:nullcount, :min, :max),Tuple{Union{Missing,Int64},Any,Any}}
const _BatchStats =
    NamedTuple{(:rows, :cols),Tuple{Union{Missing,Int64},Dict{Int,_ColumnStats}}}

function _statsschema()
    key = Field(
        "key",
        DictionaryType(IntType(32, true), Utf8Type(false), false);
        nullable=false,
        children=Field[],
    )
    value = Field(
        "value",
        UnionType(AC.DenseMode, Int8[0, 1, 2, 3]);
        nullable=false,
        children=Field[
            Field("i64", IntType(64, true); nullable=false),
            Field("f64", FloatType(64); nullable=false),
            Field("str", Utf8Type(false); nullable=false),
            Field("bool", BoolType(); nullable=false),
        ],
    )
    entries = Field("entries", StructType(); nullable=false, children=Field[key, value])
    return Schema(
        Field[
            Field("column", IntType(32, true); nullable=true),
            Field("statistics", MapType(false); nullable=false, children=Field[entries]),
        ],
    )
end

function _bitmapbytes(bits::AbstractVector{Bool})
    bytes = zeros(UInt8, cld(length(bits), 8))
    for (i, b) in enumerate(bits)
        b && (bytes[1 + (i - 1) ÷ 8] |= UInt8(1) << ((i - 1) % 8))
    end
    return bytes
end

function _utf8data(strs::Vector{String})
    offsets = Int32[0]
    bytes = UInt8[]
    for s in strs
        append!(bytes, codeunits(s))
        push!(offsets, Int32(length(bytes)))
    end
    return ArrayData(
        Utf8Type(false),
        length(strs),
        [BufferSlice(), AC._databuffer(offsets), AC._databuffer(bytes)];
        nullcount=0,
    )
end

"""
Fold one column's statistics: (null count, min, max) with `nothing` bounds
for empty, all-null, or unsupported-type columns. Values normalize into the
union's members: Int64 for integral scalars (dates, times, timestamps, and
durations are integral in the value domain), Float64, String, Bool.
"""
function _statfold(f::Field, d::ArrayData)
    t = f.type
    # Statistics describe storage-domain values: dictionary columns fold through
    # their pools, and REE columns fold through their values child — the REE
    # parent's physical null count is always 0 (spec), so its logical null
    # count must be derived or `isnull` pruning would drop real nulls.
    statfield = f
    stat = t
    while stat isa DictionaryType || stat isa RunEndEncodedType
        if stat isa DictionaryType
            statfield = AC.dictvaluefield(statfield, stat)
            stat = stat.valuetype
        else
            statfield = statfield.children[2]
            stat = statfield.type
        end
    end
    nc = if t isa DictionaryType || t isa RunEndEncodedType
        count(i -> ismissing(AC.getvalue(f, d, i)), 1:(d.len))
    else
        AC.nullcount(d)
    end
    supported =
        stat isa IntType ? (stat.signed || stat.bits < 64) :
        stat isa FloatType ||
        stat isa BoolType ||
        stat isa Utf8Type ||
        (stat isa ViewType && stat.utf8) ||
        stat isa DateType ||
        stat isa TimeType ||
        stat isa TimestampType ||
        stat isa DurationType
    supported || return nc, nothing, nothing
    lo = hi = nothing
    hasnan = false
    for i = 1:(d.len)
        # getvalue's own first step is the validity check (or, for
        # bitmap-less layouts, the logical-null route), so `missing` here is
        # the one uniform null signal across every layout.
        v = AC.getvalue(f, d, i)
        ismissing(v) && continue
        v isa NamedTuple && return nc, nothing, nothing
        if v isa AbstractFloat && isnan(v)
            hasnan = true
            continue
        end
        if lo === nothing
            lo = v
            hi = v
        else
            isless(v, lo) && (lo = v)
            isless(hi, v) && (hi = v)
        end
    end
    _statnorm(v) =
        v isa Bool ? v :
        v isa AbstractString ? String(v) : v isa AbstractFloat ? Float64(v) : Int64(v)
    return nc,
    lo === nothing || hasnan ? nothing : _statnorm(lo),
    hi === nothing || hasnan ? nothing : _statnorm(hi)
end

"One statistics record batch (the official layout) for one data batch."
function _statsbatch(
    statssch::Schema,
    nrows::Int64,
    colstats::Vector{Tuple{Int,Int64,Any,Any}},
)
    rows = 1 + length(colstats)               # batch-level row + per-column rows
    colvalid = vcat(false, trues(length(colstats)))
    colvals = vcat(Int32(0), Int32[Int32(c[1] - 1) for c in colstats])
    columndata = ArrayData(
        IntType(32, true),
        rows,
        [AC._databuffer(_bitmapbytes(colvalid)), AC._databuffer(colvals)];
        nullcount=1,
    )
    keyidx = Int32[]
    typeids = Int8[]
    offsets = Int32[]
    i64s = Int64[]
    f64s = Float64[]
    strs = String[]
    bools = Bool[]
    mapoffsets = Int32[0]
    function pushstat!(key::String, v)
        push!(keyidx, Int32(findfirst(==(key), STATS_KEYPOOL) - 1))
        if v isa Bool
            push!(typeids, Int8(3))
            push!(offsets, Int32(length(bools)))
            push!(bools, v)
        elseif v isa String
            push!(typeids, Int8(2))
            push!(offsets, Int32(length(strs)))
            push!(strs, v)
        elseif v isa Float64
            push!(typeids, Int8(1))
            push!(offsets, Int32(length(f64s)))
            push!(f64s, v)
        else
            push!(typeids, Int8(0))
            push!(offsets, Int32(length(i64s)))
            push!(i64s, Int64(v))
        end
        return nothing
    end
    pushstat!(STATS_ROW_COUNT, nrows)
    push!(mapoffsets, Int32(length(keyidx)))
    for (_, nc, lo, hi) in colstats
        pushstat!(STATS_NULL_COUNT, nc)
        lo === nothing || pushstat!(STATS_MIN, lo)
        hi === nothing || pushstat!(STATS_MAX, hi)
        push!(mapoffsets, Int32(length(keyidx)))
    end
    nentries = length(keyidx)
    pool = _utf8data(String.(STATS_KEYPOOL))
    keydata = ArrayData(
        DictionaryType(IntType(32, true), Utf8Type(false), false),
        nentries,
        [BufferSlice(), AC._databuffer(keyidx)];
        dictionary=pool,
        nullcount=0,
    )
    booldata = ArrayData(
        BoolType(),
        length(bools),
        [BufferSlice(), AC._databuffer(_bitmapbytes(bools))];
        nullcount=0,
    )
    valuedata = ArrayData(
        UnionType(AC.DenseMode, Int8[0, 1, 2, 3]),
        nentries,
        [AC._databuffer(typeids), AC._databuffer(offsets)];
        children=[
            ArrayData(
                IntType(64, true),
                length(i64s),
                [BufferSlice(), AC._databuffer(i64s)];
                nullcount=0,
            ),
            ArrayData(
                FloatType(64),
                length(f64s),
                [BufferSlice(), AC._databuffer(f64s)];
                nullcount=0,
            ),
            _utf8data(strs),
            booldata,
        ],
        nullcount=0,
    )
    entriesdata = ArrayData(
        StructType(),
        nentries,
        [BufferSlice()];
        children=[keydata, valuedata],
        nullcount=0,
    )
    mapdata = ArrayData(
        MapType(false),
        rows,
        [BufferSlice(), AC._databuffer(mapoffsets)];
        children=[entriesdata],
        nullcount=0,
    )
    return AC.RecordBatch(statssch, ArrayData[columndata, mapdata], rows)
end

"""
    withstatistics(sch, batches) -> Schema

The writer half: fold per-batch column statistics, serialize them as one
IPC stream in the OFFICIAL statistics value layout (through this very
writer — statistics ARE Arrow data), and return a schema whose metadata
carries the base64 blob under `$STATS_KEY`. `writefile(withstatistics(sch,
batches), batches)` is the whole integration — statistics are pure schema
metadata; the writer itself is untouched.
"""
function withstatistics(sch::Schema, batches::AbstractVector{AC.RecordBatch})
    statssch = _statsschema()
    statsbatches = AC.RecordBatch[]
    for batch in batches
        colstats = Tuple{Int,Int64,Any,Any}[]
        fieldref = 1  # official zero-based FieldNode index, plus one for _statsbatch
        for (f, col) in zip(sch.fields, batch.columns)
            nc, lo, hi = _statfold(f, col)
            push!(colstats, (fieldref, nc, lo, hi))
            fieldref += _fieldnodespan(f)
        end
        push!(statsbatches, _statsbatch(statssch, batch.nrows, colstats))
    end
    blob = Base64.base64encode(writestream(statssch, statsbatches))
    # Replace only this private placement key. Keep every other metadata pair,
    # including duplicate keys and its original order.
    metadata = Pair{String,String}[]
    if sch.metadata !== nothing
        for kv in sch.metadata
            first(kv) == STATS_KEY || push!(metadata, String(first(kv)) => String(last(kv)))
        end
    end
    push!(metadata, STATS_KEY => blob)
    return Schema(collect(Field, sch.fields); metadata=metadata, endianness=sch.endianness)
end

"Validate the canonical outer statistics-schema shape before using values."
function _validatestatsschema(sch::Schema)
    length(sch.fields) == 2 ||
        throw(ArgumentError("statistics schema must have two fields"))
    column, statistics = sch.fields
    ct = column.type
    column.name == "column" &&
    column.nullable &&
    ct isa IntType &&
    ct.bits == 32 &&
    ct.signed &&
    isempty(column.children) ||
        throw(ArgumentError("statistics column field is not nullable int32"))
    statistics.name == "statistics" &&
    !statistics.nullable &&
    statistics.type isa MapType &&
    length(statistics.children) == 1 ||
        throw(ArgumentError("statistics field is not a non-null map"))
    entries = statistics.children[1]
    !entries.nullable && entries.type isa StructType && length(entries.children) == 2 ||
        throw(ArgumentError("statistics map entries are not a non-null key/value struct"))
    key, value = entries.children
    kt = key.type
    !key.nullable &&
    kt isa DictionaryType &&
    kt.indextype.bits == 32 &&
    kt.indextype.signed &&
    kt.valuetype isa Utf8Type &&
    !kt.valuetype.large &&
    isempty(key.children) ||
        throw(ArgumentError("statistics keys are not non-null dictionary<utf8, int32>"))
    !value.nullable && value.type isa UnionType && value.type.mode == AC.DenseMode ||
        throw(ArgumentError("statistics values are not a non-null dense union"))
    return nothing
end

"Write a statistics-carrying Arrow file: `writefile(withstatistics(sch, batches), batches)`."
statsfile(sch::Schema, batches::AbstractVector{AC.RecordBatch}; compress::Symbol=:none) =
    writefile(withstatistics(sch, batches), batches; compress=compress)

# ---- read + prune ---------------------------------------------------------

function _decodebase64budgeted(blob::String, budget::AllocationBudget)
    encodedbytes = Int64(ncodeunits(blob))
    maxdecoded = AC.checked_mul(cld(encodedbytes, Int64(4)), Int64(3))
    maxdecoded <= typemax(Int) ||
        throw(ValidationError("statistics base64 output is not addressable"))
    _chargevector!(budget, UInt8, maxdecoded, "statistics base64 output")
    # Decode into one pre-sized backing store. `Base64.base64decode` grows an
    # IOBuffer geometrically and can retain several times the final payload.
    # The upper bound stays charged after resize because Julia may retain it.
    decoded = Vector{UInt8}(undef, Int(maxdecoded))
    pipe = Base64.Base64DecodePipe(IOBuffer(blob))
    Base.readbytes!(pipe, decoded, Int(maxdecoded))
    return decoded
end

"""
Parse the statistics blob back through this reader. A missing key, corrupt
base64/stream, wrong schema, or wrong batch count degrades to `nothing` (no
pruning). Exhausting the caller's cumulative allocation budget still throws.
Returns per-batch `Dict{Int,...}` column stats (1-based top-level indices)
with `missing` bounds where absent.
"""
function _readstats(
    metadata,
    nbatches::Int,
    datafields=nothing;
    limits::Limits=Limits(),
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    metadata === nothing && return nothing
    blob = nothing
    for kv in metadata
        first(kv) == STATS_KEY && (blob = last(kv))
    end
    blob === nothing && return nothing
    localbudget =
        budget === nothing ? AllocationBudget(limits.max_total_allocated_bytes) : budget
    try
        blob isa String || return nothing
        decoded = _decodebase64budgeted(blob, localbudget)
        stream = _readstream(decoded, limits, localbudget)
        length(stream.batches) == nbatches || return nothing
        _validatestatsschema(stream.schema)
        colfield, mapfield = stream.schema.fields
        datafieldcount = datafields === nothing ? 0 : length(datafields)
        _chargedict!(localbudget, Int, Int, datafieldcount, "statistics field index")
        wiretotop = Dict{Int,Int}()
        sizehint!(wiretotop, datafieldcount)
        totalnodes = 0
        if datafields !== nothing
            for (j, f) in enumerate(datafields)
                wiretotop[totalnodes] = j
                totalnodes += _fieldnodespan(f)
            end
        end
        _chargevector!(localbudget, _BatchStats, nbatches, "statistics batches")
        out = _BatchStats[]
        sizehint!(out, nbatches)
        for sb in stream.batches
            cols = materialize(colfield, sb.columns[1], localbudget)
            maps = materialize(mapfield, sb.columns[2], localbudget)
            length(cols) == length(maps) ||
                throw(ArgumentError("statistics columns have different lengths"))
            rows::Union{Missing,Int64} = missing
            _chargedict!(
                localbudget,
                Int,
                _ColumnStats,
                length(cols),
                "statistics column map",
            )
            d = Dict{Int,_ColumnStats}()
            sizehint!(d, length(cols))
            for (colref, pairs) in zip(cols, maps)
                rowcount = missing
                nullcount = missing
                minvalue = missing
                maxvalue = missing
                for (key, value) in pairs
                    if key == STATS_ROW_COUNT
                        rowcount = value
                    elseif key == STATS_NULL_COUNT
                        nullcount = value
                    elseif key == STATS_MIN
                        minvalue = value
                    elseif key == STATS_MAX
                        maxvalue = value
                    end
                end
                if colref === missing
                    rc = rowcount
                    if rc !== missing
                        rc isa Int64 && rc >= 0 || throw(
                            ArgumentError(
                                "statistics row count must be a nonnegative Int64",
                            ),
                        )
                        rows = rc
                    end
                    continue
                end
                colref isa Integer ||
                    throw(ArgumentError("statistics column index must be integral"))
                wire = Int(colref)
                wire >= 0 || throw(ArgumentError("negative statistics column index"))
                top = if datafields === nothing
                    wire + 1
                else
                    wire < totalnodes || throw(
                        ArgumentError("statistics column index exceeds the schema"),
                    )
                    get(wiretotop, wire, nothing)
                end
                top === nothing && continue  # valid nested-field statistics
                nc = nullcount
                if nc !== missing
                    nc isa Int64 && nc >= 0 || throw(
                        ArgumentError("statistics null count must be a nonnegative Int64"),
                    )
                end
                d[top] = (nullcount=nc, min=minvalue, max=maxvalue)
            end
            if rows !== missing
                all(s -> s.nullcount === missing || s.nullcount <= rows, values(d)) ||
                    throw(ArgumentError("statistics null count exceeds row count"))
            end
            push!(out, (rows=rows, cols=d))
        end
        return out
    catch e
        e isa AllocationLimitError && rethrow()
        e isa InterruptException && rethrow()
        e isa OutOfMemoryError && rethrow()
        return nothing
    end
end

"Bytewise successor of a prefix, or `nothing` when none exists."
function _nextprefix(s::String, budget=nothing)
    _chargevector!(budget, UInt8, ncodeunits(s), "statistics prefix bytes")
    _chargeobject!(budget, ncodeunits(s) + 16, "statistics prefix String")
    bytes = collect(codeunits(s))
    while !isempty(bytes)
        if bytes[end] < 0xff
            bytes[end] += 0x01
            return String(bytes)
        end
        pop!(bytes)
    end
    return nothing
end

struct _StatsContext
    nameindex::Dict{Symbol,Int}
    prefixes::Dict{String,Union{Nothing,String}}
end

_statsstartswithcount(::Any) = Int64(0)
_statsstartswithcount(e::Tables.StrPred) =
    e.kind == Tables.STR_STARTSWITH ? Int64(1) : Int64(0)
function _statsstartswithcount(e::Union{Tables.AndExpr,Tables.OrExpr})
    count = Int64(0)
    for arg in e.args
        count = AC.checked_add(count, _statsstartswithcount(arg))
    end
    return count
end
_statsstartswithcount(e::Tables.NotExpr) = _statsstartswithcount(e.arg)

function _collectstatprefixes!(prefixes, e, budget)
    if e isa Tables.StrPred
        if e.kind == Tables.STR_STARTSWITH && !haskey(prefixes, e.s)
            prefixes[e.s] = _nextprefix(e.s, budget)
        end
    elseif e isa Union{Tables.AndExpr,Tables.OrExpr}
        for arg in e.args
            _collectstatprefixes!(prefixes, arg, budget)
        end
    elseif e isa Tables.NotExpr
        _collectstatprefixes!(prefixes, e.arg, budget)
    end
    return nothing
end

"Compile the schema and prefix work reused by every statistics batch."
function _statscontext(filter, names, budget)
    _chargedict!(budget, Symbol, Int, length(names), "statistics column-name index")
    nameindex = Dict{Symbol,Int}()
    sizehint!(nameindex, length(names))
    for (i, name) in enumerate(names)
        nameindex[name] = i
    end
    nprefixes = _statsstartswithcount(filter)
    _chargedict!(
        budget,
        String,
        Union{Nothing,String},
        nprefixes,
        "statistics prefix index",
    )
    prefixes = Dict{String,Union{Nothing,String}}()
    sizehint!(prefixes, nprefixes)
    _collectstatprefixes!(prefixes, filter, budget)
    return _StatsContext(nameindex, prefixes)
end

function _statcmp(f, a, b)
    return try
        f(a, b) === false ? false : true
    catch e
        e isa InterruptException && rethrow()
        e isa OutOfMemoryError && rethrow()
        true   # incomparable literal/stat types: never prune
    end
end

function _stateq(a, b)
    return try
        (a == b) === true
    catch e
        e isa InterruptException && rethrow()
        e isa OutOfMemoryError && rethrow()
        false
    end
end

const _TrustedStatNumber = Union{
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    BigInt,
    Float16,
    Float32,
    Float64,
    BigFloat,
}

# Statistics may prune only inside domains whose equality and order are the
# Base numeric/String contracts used to compute min/max. Tables permits custom
# literal comparison methods; a successful comparison is not proof that those
# methods agree with the stored bounds' order.
_trustedstatliteral(v) = v isa Union{_TrustedStatNumber,String}
_trustedstatliteral(v, s) =
    s.min isa Union{Bool,Int64,Float64} ? v isa _TrustedStatNumber :
    s.min isa String ? v isa String : false

# `Tables.In.values` need only support `in(x, values)`. Enumerate only Base
# containers whose membership is defined by their retained members; every
# other object disables pruning and goes to the exact row filter untouched.
_statsmembers(values::Union{Tuple,Array,BitArray,Set,BitSet}) = values
_statsmembers(values) = nothing

"""
One-sided may-contain evaluation of a scan predicate against one batch's
column statistics: `false` means PROVABLY no row qualifies (prune); `true`
means fetch and let the exact row filter decide. Comparisons follow SQL
missing semantics — null rows never satisfy a comparison, so an all-null
column proves comparison/`colin` predicates false.
"""
function _maypass(
    e::Tables.ScanExpr,
    stats,
    names,
    rowcount::Union{Missing,Int64},
    context::Union{Nothing,_StatsContext}=nothing,
)
    function lookup(col)
        ref = col.ref
        i = if ref isa Int
            1 <= ref <= length(names) ? ref : nothing
        elseif context !== nothing
            get(context.nameindex, ref isa String ? Symbol(ref) : ref, nothing)
        elseif ref isa String
            findfirst(==(Symbol(ref)), names)
        else
            findfirst(==(ref), names)
        end
        return i === nothing ? nothing : get(stats, i, nothing)
    end
    allnull(s) = s.nullcount !== missing && rowcount !== missing && s.nullcount >= rowcount
    unknownbounds(s) =
        s.min === missing ||
        s.max === missing ||
        (s.min isa AbstractFloat && isnan(s.min)) ||
        (s.max isa AbstractFloat && isnan(s.max))
    if e isa Tables.Cmp
        s = lookup(e.lhs)
        s === nothing && return true
        v = e.rhs
        _trustedstatliteral(v) || return true
        allnull(s) && return false
        unknownbounds(s) && return true
        _trustedstatliteral(v, s) || return true
        e.op == Tables.OP_EQ && return _statcmp(>=, v, s.min) && _statcmp(>=, s.max, v)
        # NE prunes only a provably constant batch equal to the literal:
        # min == max == v. Anything weaker (including any NaN, where the
        # equalities are false) must fetch.
        e.op == Tables.OP_NE && return !(_stateq(s.min, v) && _stateq(s.max, v))
        e.op == Tables.OP_LT && return _statcmp(<, s.min, v)
        e.op == Tables.OP_LE && return _statcmp(<=, s.min, v)
        e.op == Tables.OP_GT && return _statcmp(>, s.max, v)
        e.op == Tables.OP_GE && return _statcmp(>=, s.max, v)
        return true    # unknown comparison ops never prune
    elseif e isa Tables.In
        s = lookup(e.lhs)
        s === nothing && return true
        allnull(s) && return false
        unknownbounds(s) && return true
        members = _statsmembers(e.values)
        members === nothing && return true
        all(v -> _trustedstatliteral(v, s), members) || return true
        return any(_statcmp(>=, v, s.min) && _statcmp(>=, s.max, v) for v in members)
    elseif e isa Tables.IsNull
        s = lookup(e.lhs)
        s === nothing && return true
        s.nullcount === missing && return true
        return e.negated ? !allnull(s) : s.nullcount > 0
    elseif e isa Tables.StrPred
        e.kind == Tables.STR_STARTSWITH || return true
        s = lookup(e.lhs)
        s === nothing && return true
        unknownbounds(s) && return true
        _statcmp(>=, s.max, e.s) || return false
        next = context === nothing ? _nextprefix(e.s) : get(context.prefixes, e.s, nothing)
        return next === nothing || _statcmp(<, s.min, next)
    elseif e isa Tables.AndExpr
        return all(_maypass(a, stats, names, rowcount, context) for a in e.args)
    elseif e isa Tables.OrExpr
        return any(_maypass(a, stats, names, rowcount, context) for a in e.args)
    elseif e isa Tables.NotExpr
        inner = e.arg
        if inner isa Tables.Cmp && inner.op == Tables.OP_EQ
            s = lookup(inner.lhs)
            s === nothing && return true
            unknownbounds(s) && return true
            _trustedstatliteral(inner.rhs, s) || return true
            # everything equals v only when min == max == v
            return !(_stateq(s.min, inner.rhs) && _stateq(s.max, inner.rhs))
        end
        return true
    elseif e isa Tables.AlwaysFalse
        return false
    end
    return true    # AlwaysTrue, OpNode, unknown growth: never prune
end
