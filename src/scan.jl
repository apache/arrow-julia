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
# byte-range fetch protocol (`RangedFile`/`RangedSource`) over the same
# bound column set. Design notes: docs/dev/DESIGN-scan-ranges-trim.md.
#
# Pushdown semantics: the source consumes what it can PROVE and leaves exact
# row evaluation to `Tables.scan`.
#
#   * the decode set is (selected ∪ filter-referenced) columns — everything
#     else is SKIPPED by `skipfield!`, a registry walk that consumes the
#     node/buffer accounting (all buffer-table invariants still checked)
#     without slicing, decompressing, validating, or materializing anything;
#   * whole batches are pruned by footer-carried statistics (may-contain, so
#     the filter stays in the residual) and `limit`/`offset` are consumed
#     EXACTLY when no filter poisons the window: `RecordBatch.length` is
#     wire metadata, so batches outside the window are never decoded;
#   * the returned table keeps SOURCE names over the decode set and the
#     residual keeps `select` and `filter` — `Tables.scan` filters,
#     projects, renames, and converts. This is the only composition that
#     stays correct when the filter references unselected columns.
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
function _bufferspan(f::Field, variadics::AbstractVector{Int64},
    varidx::Base.RefValue{Int})
    spec = layoutspec(f.type)
    n = Int64(length(spec.buffers))
    if spec.variadic
        varidx[] <= length(variadics) || throw(ValidationError(
            "metadata declares fewer variadic buffer counts than the schema requires"))
        vc = variadics[varidx[]]
        varidx[] += 1
        vc >= 0 || throw(ValidationError(
            "variadic buffer count $vc is invalid"))
        n = _planadd(n, vc, "buffer span")
    end
    f.type isa DictionaryType && return n
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        n = _planadd(n, _bufferspan(f.children[i], variadics, varidx),
            "buffer span")
    end
    return n
end

"""
Validate the metadata needed before a RecordBatch length may drive a scan
window or a buffer table may drive a range fetch. This is the metadata-only
half of the decode cursor: exact node/buffer counts, every node invariant,
top-level row-count agreement, and every buffer's geometry.
"""
function _recordbatchmeta(header::Meta.RecordBatch, fields, limits::Limits,
    bodylen::Int64)
    rblen = something(header.length, Int64(0))
    0 <= rblen <= limits.max_array_length ||
        throw(ValidationError("record batch length $rblen exceeds limit"))

    nodes = something(header.nodes, Meta.FieldNode[])
    expectednodes = sum(_fieldnodespan(f) for f in fields; init=0)
    length(nodes) == expectednodes || throw(ValidationError(
        "field-node count does not match the schema"))
    nodeidx = 1
    for f in fields
        node = nodes[nodeidx]
        node.length == rblen || throw(ValidationError(
            "RecordBatch length does not match top-level field nodes"))
        nodeidx += _fieldnodespan(f)
    end
    for node in nodes
        0 <= node.length <= limits.max_array_length ||
            throw(ValidationError("field-node length $(node.length) exceeds limit"))
        0 <= node.null_count <= node.length ||
            throw(ValidationError("invalid field-node null count $(node.null_count)"))
    end

    buffers = something(header.buffers, Meta.Buffer[])
    variadics = variadiccounts(header)
    varidx = Ref(1)
    expectedbuffers = Int64(0)
    for f in fields
        expectedbuffers = _planadd(expectedbuffers,
            _bufferspan(f, variadics, varidx), "record-batch buffer span")
    end
    varidx[] == length(variadics) + 1 || throw(ValidationError(
        "unconsumed variadic buffer counts: schema/batch mismatch"))
    length(buffers) == expectedbuffers ||
        throw(ValidationError("buffer count does not match the schema"))
    last_nonempty_end = Int64(0)
    for b in buffers
        offset = Int64(b.offset)
        len = Int64(b.length)
        offset >= 0 || throw(ValidationError("negative batch buffer offset $offset"))
        offset % 8 == 0 || throw(ValidationError(
            "batch buffer offset $offset is not 8-byte aligned"))
        0 <= len <= limits.max_buffer_bytes ||
            throw(ValidationError("batch buffer length $len exceeds limit"))
        bufferend = try
            AC.checked_add(offset, len)
        catch e
            e isa OverflowError || rethrow()
            throw(ValidationError("batch buffer end overflows"))
        end
        bufferend <= bodylen || throw(ValidationError(
            "batch buffer [$offset, $len] escapes its message body"))
        if len > 0
            offset >= last_nonempty_end || throw(ValidationError(
                "batch buffers overlap or move backwards"))
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
        spec.fixedwidth > 0 && return _planmul(
            node.length, Int64(spec.fixedwidth), "planned data-buffer size")
        if spec.fixedwidth == -1
            return node.length ÷ 8 + (node.length % 8 == 0 ? 0 : 1)
        end
        return Int64(0)
    elseif role == AC.OFFSETS
        count = _planadd(node.length, Int64(1), "planned offset count")
        return _planmul(count, Int64(spec.offsetwidth), "planned offsets-buffer size")
    elseif role == AC.ELEMENT_OFFSETS || role == AC.SIZES
        return _planmul(node.length, Int64(spec.offsetwidth),
            "planned element-buffer size")
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
        node.null_count == node.length || throw(ValidationError(
            "Null field-node null count must equal its length"))
    elseif t isa UnionType
        node.null_count == 0 || throw(ValidationError(
            "Union field-node null count must be zero"))
    end
    # Field.nullable is advisory (enforced only by the opt-in validate_full
    # tier), so a planned scan makes no nullability judgment here — the same
    # contract the whole-file path applies.
    spec = layoutspec(f.type)
    for role in spec.buffers
        _, len = _buffermeta!(c)
        if codec == CODEC_NONE || len == 0
            need = _planminbytes(role, spec, node, len)
            len >= need || throw(ValidationError(
                "planned buffer length $len is smaller than required $need"))
        else
            len >= 8 || throw(ValidationError(
                "compressed buffer of $len bytes lacks its length prefix"))
            need = _planminbytes(role, spec, node, Int64(0))
            need > 0 && len == 8 && throw(ValidationError(
                "compressed planned buffer requires a nonempty payload"))
        end
    end
    if spec.variadic
        # Variadic view-data buffers have no metadata-derivable minimum
        # (views reference them arbitrarily); geometry and, under
        # compression, the prefix rule are the plannable invariants.
        for _ = 1:takevariadic!(c)
            _, len = _buffermeta!(c)
            codec == CODEC_NONE || len == 0 || len >= 8 || throw(ValidationError(
                "compressed buffer of $len bytes lacks its length prefix"))
        end
    end
    f.type isa DictionaryType && return node.length
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    childlens = Int64[]
    fslextent = t isa FixedSizeListType ? _planmul(node.length,
        Int64(t.listsize), "fixed-size-list child length") : Int64(0)
    for i = 1:nchildren
        push!(childlens, _validateplannedfield!(f.children[i], c, codec))
    end
    if t isa FixedSizeListType
        childlens[1] >= fslextent || throw(ValidationError(
            "fixed-size-list child is shorter than its parent extent"))
    elseif t isa StructType
        all(>=(node.length), childlens) || throw(ValidationError(
            "struct child is shorter than its parent extent"))
    elseif t isa UnionType && t.mode == AC.SparseMode
        all(==(node.length), childlens) || throw(ValidationError(
            "sparse-union child length does not equal its parent length"))
    elseif t isa RunEndEncodedType
        node.null_count == 0 || throw(ValidationError(
            "REE parent null count must be zero"))
        childlens[1] == childlens[2] || throw(ValidationError(
            "REE run-end and value child lengths must match"))
        node.length == 0 || childlens[1] > 0 || throw(ValidationError(
            "a nonempty REE array requires at least one physical run"))
        runtype = f.children[1].type::IntType
        maxrunend = runtype.bits == 16 ? Int64(typemax(Int16)) :
            runtype.bits == 32 ? Int64(typemax(Int32)) : typemax(Int64)
        node.length <= maxrunend || throw(ValidationError(
            "REE logical extent exceeds its run-end range"))
    end
    return node.length
end

"Validate every metadata-only invariant for the subtrees whose bodies are planned."
function _validatebodyplan(header::Meta.RecordBatch, fields, limits::Limits,
    codec::Int8, mask::AbstractVector{Bool})
    cursor = DecodeCursor(header.nodes, header.buffers, BufferSlice(), limits;
        codec=codec, variadics=variadiccounts(header))
    for (j, f) in enumerate(fields)
        mask[j] ? _validateplannedfield!(f, cursor, codec) : skipfield!(f, cursor)
    end
    finishcursor!(cursor)
    return nothing
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
        idx[] <= length(ns) ||
            throw(ValidationError("metadata declares fewer field nodes than the schema requires"))
        node = ns[idx[]]
        idx[] += 1
        if f.type isa DictionaryType
            decoded || return
            id = fielddictids[f]
            if !haskey(dicts, id)
                node.length >= 0 && node.null_count == node.length ||
                    throw(ValidationError("record batch uses undefined dictionary id $id for a non-null slot"))
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

_batchrows(f::ArrowFile, i::Int) = _batchrows(f, i,
    AllocationBudget(f.limits.max_total_allocated_bytes))

"""
The masked-decode core shared by the in-memory and ranged paths: masked-in
fields decode and validate exactly as `getindex`; masked-out fields advance
through `skipfield!`. The cursor must still finish clean — a skewed batch
fails identically either way. `body` is a `BufferSlice` or a `SparseBody`.
"""
function _maskedrecord(msg::Meta.Message, version::Int16, body,
    fields, dicts, fielddictids, validated, limits::Limits,
    schemaversion::Int16, mask::AbstractVector{Bool},
    state::DecodeState)
    version == schemaversion ||
        throw(ValidationError("IPC metadata version changes within the file"))
    rejectexperimentalcompression(msg, version, UInt8(3))
    header = msg.header
    header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    codec = _batchcodec(header.compression, version)
    rblen = _recordbatchmeta(header, fields, limits,
        body isa BufferSlice ? body.len : body.bodylen)
    _scanmissingdicts(fields, header.nodes, dicts, fielddictids, mask)
    cursor = DecodeCursor(header.nodes, header.buffers, body, limits;
        codec=codec, state=state, variadics=variadiccounts(header))
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
        col.len == rblen ||
            throw(ValidationError("RecordBatch length does not match top-level field nodes"))
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

"Resolve positional filter references once, against the source schema."
_resolvefilter(::Nothing, names) = nothing
function _resolvefilter(e::Tables.ScanExpr, names)
    col(c) = c.ref isa Int && 1 <= c.ref <= length(names) ?
        Tables.Col(names[c.ref]) : c
    e isa Tables.Cmp && return Tables.Cmp(e.op, col(e.lhs), e.rhs)
    e isa Tables.In && return Tables.In(col(e.lhs), e.values)
    e isa Tables.IsNull && return Tables.IsNull(col(e.lhs), e.negated)
    e isa Tables.StrPred && return Tables.StrPred(e.kind, col(e.lhs), e.s)
    e isa Tables.AndExpr && return Tables.AndExpr(
        Tables.ScanExpr[_resolvefilter(a, names) for a in e.args])
    e isa Tables.OrExpr && return Tables.OrExpr(
        Tables.ScanExpr[_resolvefilter(a, names) for a in e.args])
    e isa Tables.NotExpr && return Tables.NotExpr(_resolvefilter(e.arg, names))
    return e
end

"Column table that preserves a row count when there are no columns."
struct _ScanColumns{T}
    columns::T
    nrows::Int
end
Tables.istable(::Type{<:_ScanColumns}) = true
Tables.columnaccess(::Type{<:_ScanColumns}) = true
Tables.columns(t::_ScanColumns) = t
Tables.columnnames(t::_ScanColumns) = propertynames(t.columns)
Tables.getcolumn(t::_ScanColumns, i::Int) = getfield(t.columns, i)
Tables.getcolumn(t::_ScanColumns, name::Symbol) = getproperty(t.columns, name)
Tables.rowcount(t::_ScanColumns) = t.nrows

function _scantable(names, outcols, nrows::Int)
    table = NamedTuple{Tuple(names)}(outcols)
    return isempty(names) ? _ScanColumns(table, nrows) : table
end

function _addscanrows(total::Int, rows::Int64)
    (0 <= total && 0 <= rows && rows <= typemax(Int) - total) ||
        throw(ValidationError("scan result row count is not addressable"))
    return total + Int(rows)
end

function _scanbatch(f::ArrowFile, i::Int, mask::AbstractVector{Bool},
    budget::AllocationBudget, state::DecodeState)
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, budget)
    return _maskedrecord(fm.msg, fm.version, fm.body, f.fields,
        f.dictionaries, f.fielddictids, f.validated, f.limits,
        f.schemaversion, mask, state)
end

function _scanbatch(f::ArrowFile, i::Int, mask::AbstractVector{Bool})
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    state = DecodeState(budget)
    try
        return _scanbatch(f, i, mask, budget, state)
    finally
        close(state)
    end
end

# ---------------------------------------------------------------------------
# Scan pushdown over a whole file
# ---------------------------------------------------------------------------

"""
Exact batch windowing for consumed `limit`/`offset`: per surviving batch,
how many leading rows to drop and how many to keep. Batches wholly outside
the window are absent — never decoded.
"""
function _batchwindow(rowcounts::Vector{Int64}, offset::Int, limit::Union{Nothing,Int})
    window = Tuple{Int,Int64,Int64}[]   # (batch index, skip, take)
    remaining_skip = Int64(offset)
    unlimited = limit === nothing
    remaining_take = unlimited ? Int64(0) : Int64(limit)
    for (i, rows) in enumerate(rowcounts)
        !unlimited && remaining_take <= 0 && break
        if remaining_skip >= rows
            remaining_skip -= rows
            continue
        end
        take = unlimited ? rows - remaining_skip :
            min(rows - remaining_skip, remaining_take)
        push!(window, (i, remaining_skip, take))
        if !unlimited
            remaining_take -= take
        end
        remaining_skip = 0
    end
    return window
end

# The Tables.scan authority forms `offset + 1` and, with a limit,
# `offset + limit` in Int arithmetic. Keep an overflowing request residual so
# both sides of the apply/finish contract have the same observable result.
_canconsumewindow(scan::Tables.Scan) = scan.offset < typemax(Int) &&
    (scan.limit === nothing || scan.limit <= typemax(Int) - scan.offset)

function _applyscan(f::ArrowFile, scan::Tables.Scan)
    names = Symbol[Symbol(fld.name) for fld in f.fields]
    allunique(names) || throw(ValidationError(
        "scan pushdown over duplicate column names is not supported; read the file without a scan"))
    b = Tables.bind(scan, names)
    if isempty(names)
        # Zero-field sources: consume filter and window HERE — an empty
        # residual NamedTuple cannot carry a row count through finish. The
        # header reads share ONE budget: `Limits` documents a cumulative
        # allocation bound per read, exactly as the column path enforces.
        keep = _zerofieldpredicate(scan.filter)
        zfbudget = AllocationBudget(f.limits.max_total_allocated_bytes)
        n = _zerofieldwindow((_batchrows(f, i, zfbudget) for i = 1:length(f)),
            keep, scan.limit, scan.offset)
        return _scantable(Symbol[], (), Int(n)),
            Tables.Scan(nothing, nothing, nothing, 0, scan.validate)
    end
    decodeidx = sort!(unique!(vcat(Int[c.index for c in b.columns], copy(b.filtercols))))
    mask = falses(length(names))
    mask[decodeidx] .= true
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    state = DecodeState(budget)
    try
        consumed = scan.filter === nothing && _canconsumewindow(scan) &&
            (scan.limit !== nothing || scan.offset > 0)
        window = if consumed
            _batchwindow(Int64[_batchrows(f, i, budget) for i = 1:length(f)],
                scan.offset, scan.limit)
        else
            Tuple{Int,Int64,Int64}[(i, Int64(0), Int64(-1)) for i = 1:length(f)]
        end
        # Statistics pruning: one-sided — a pruned batch is provably
        # empty under the filter; the filter itself always stays in the residual.
        keep = trues(length(f))
        if scan.filter !== nothing
            stats = _readstats(f.schema.metadata, length(f), f.fields;
                limits=f.limits, budget=budget)
            stats === nothing ||
                (keep = Bool[_maypass(scan.filter, stats[i].cols, names, stats[i].rows)
                             for i = 1:length(f)])
        end
        parts = Dict{Int,Vector{Any}}(idx => Any[] for idx in decodeidx)
        outrows = 0
        for (i, skip, take) in window
            keep[i] || continue
            rblen, cols = _scanbatch(f, i, mask, budget, state)
            outrows = _addscanrows(outrows, take >= 0 ? take : rblen)
            for idx in decodeidx
                col = materialize(f.fields[idx], cols[idx]::ArrayData)
                take >= 0 && (col = col[(skip + 1):(skip + take)])
                push!(parts[idx], col)
            end
        end
        outcols = Tuple(isempty(parts[idx]) ? Any[] : reduce(vcat, parts[idx])
                        for idx in decodeidx)
        table = _scantable(names[decodeidx], outcols, outrows)
        # The residual's selection must be RESOLVED against the source schema:
        # the output table carries only the decode set, so re-binding `Not`
        # (whose excluded names are gone) or a `Regex` (which could over-match a
        # filter-only column) against it would be wrong. Bound columns become
        # concrete source-name items carrying their renames and type overrides.
        residualselect = scan.select === nothing ? nothing :
            Tables.SelectItem[Tables.SelectItem(names[c.index], c.type,
                c.name == names[c.index] ? nothing : c.name) for c in b.columns]
        limit = consumed ? nothing : scan.limit
        offset = consumed ? 0 : scan.offset
        residualfilter = _resolvefilter(scan.filter, names)
        return table, Tables.Scan(residualselect, residualfilter, limit, offset, scan.validate)
    finally
        close(state)
    end
end

# ===========================================================================
# Byte-range reads — RangedSource{F}, the planner, and sparse decode
# ===========================================================================

"""
    RangedSource{F}

The fetcher contract: `fetch(offset::Int64, len::Int64) ->
Vector{UInt8}` over a remote or local object of known total `len`, offsets
0-based. `F` is concrete per instantiation — in a trimmed app the fetch path
is statically resolvable, which is why this is a parametric functor and not
an abstract type. Transports (CloudStore, HTTP) live in extensions and only
need to construct one of these; `fetchranges` has a serial default they
override for concurrent range GETs.
"""
struct RangedSource{F}
    fetch::F
    len::Int64
end

RangedSource(bytes::Vector{UInt8}) =
    RangedSource((off, len) -> bytes[(off + 1):(off + len)], Int64(length(bytes)))

"One result vector per requested `(offset, len)`; override for concurrency."
fetchranges(s::RangedSource, ranges::Vector{NTuple{2,Int64}}) =
    Vector{UInt8}[_fetchexact(s, off, len) for (off, len) in ranges]

function _fetchexact(s::RangedSource, off::Int64, len::Int64)
    (off >= 0 && len >= 0 && off <= s.len - len) ||
        throw(ValidationError("range fetch [$off, $len] escapes the object"))
    bytes = s.fetch(off, len)
    length(bytes) == len ||
        throw(ValidationError("range fetch returned $(length(bytes)) bytes, expected $len"))
    return bytes
end

"Fetch accounting for the differential tests: every range, every byte."
mutable struct FetchLog
    requests::Int
    bytes::Int64
    ranges::Vector{NTuple{2,Int64}}
end
FetchLog() = FetchLog(0, 0, NTuple{2,Int64}[])

function countingsource(bytes::Vector{UInt8})
    log = FetchLog()
    fetch = (off, len) -> begin
        log.requests += 1
        log.bytes += len
        push!(log.ranges, (off, len))
        bytes[(off + 1):(off + len)]
    end
    return log, RangedSource(fetch, Int64(length(bytes)))
end

_fetched(log::FetchLog, pos::Int64) =
    any(off <= pos < off + len for (off, len) in log.ranges)

"""
Merge sorted ranges whose gap is at most `gap`: a small over-read is usually
cheaper than another request round-trip. Returns file-coordinate spans.
"""
function _coalesce(ranges::Vector{NTuple{2,Int64}}, gap::Int64)
    isempty(ranges) && return NTuple{2,Int64}[]
    gap >= 0 || throw(ArgumentError("negative coalesce gap"))
    all(r -> r[1] >= 0 && r[2] >= 0, ranges) ||
        throw(ArgumentError("negative range offset or length"))
    sorted = sort(ranges)
    out = NTuple{2,Int64}[sorted[1]]
    for (off, len) in Iterators.drop(sorted, 1)
        loff, llen = out[end]
        loend = AC.checked_add(loff, llen)
        thisend = AC.checked_add(off, len)
        if off <= loend || off - loend <= gap
            out[end] = (loff, max(loend, thisend) - loff)
        else
            push!(out, (off, len))
        end
    end
    return out
end

"Fetched file-coordinate spans with their bytes, resolvable by containment."
struct FetchedSpans
    starts::Vector{Int64}
    lens::Vector{Int64}
    slices::Vector{BufferSlice}
end

function _fetchspans(src::RangedSource, ranges::Vector{NTuple{2,Int64}}, gap::Int64;
    budget::Union{Nothing,AllocationBudget}=nothing,
    what::AbstractString="range fetch")
    spans = _coalesce(ranges, gap)
    budget === nothing || foreach(s -> _charge!(budget, s[2], what), spans)
    payloads = fetchranges(src, spans)
    length(payloads) == length(spans) || throw(ValidationError(
        "range fetch returned $(length(payloads)) payloads, expected $(length(spans))"))
    for (payload, (_, len)) in zip(payloads, spans)
        length(payload) == len || throw(ValidationError(
            "range fetch returned $(length(payload)) bytes, expected $len"))
    end
    slices = BufferSlice[BufferSlice(heapregion(p), 0, length(p)) for p in payloads]
    return FetchedSpans(Int64[s[1] for s in spans], Int64[s[2] for s in spans], slices)
end

function _spanslice(fs::FetchedSpans, off::Int64, len::Int64)
    len == 0 && return BufferSlice()
    i = searchsortedlast(fs.starts, off)
    (i >= 1 && off >= fs.starts[i] &&
     AC.checked_add(off, len) <= AC.checked_add(fs.starts[i], fs.lens[i])) ||
        throw(ValidationError("required bytes [$off, $len] were not fetched"))
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
function _neededdictids(fields, fielddictids, mask::AbstractVector{Bool})
    ids = Set{Int64}()
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
function _parseblockmeta(bytes::Vector{UInt8}, block::NTuple{3,Int64},
    limits::Limits, budget::AllocationBudget)
    offset, metalen, bodylen = block
    length(bytes) == metalen ||
        throw(ValidationError("footer block metadata fetch length mismatch"))
    metalen >= 16 || throw(ValidationError("footer block has invalid extents"))
    reinterpret(UInt32, bytes[1:4])[1] == CONTINUATION ||
        throw(ValidationError("footer block does not point at a message"))
    declared = Int64(reinterpret(Int32, bytes[5:8])[1])
    declared == metalen - 8 ||
        throw(ValidationError("footer block metadata length does not match the message"))
    0 < declared <= limits.max_metadata_bytes || throw(ValidationError(
        "metadata length $declared outside (0, $(limits.max_metadata_bytes)]"))
    0 <= bodylen <= limits.max_body_bytes || throw(ValidationError(
        "body length $bodylen outside [0, $(limits.max_body_bytes)]"))
    _charge!(budget, declared, "metadata allocation")
    metabytes = bytes[9:end]
    version, header_type, _, reserve = verify_ipc_metadata(metabytes, limits, budget.left)
    _charge!(budget, reserve, "verified metadata expansion")
    msg = FB.getrootas(Meta.Message, metabytes, 0)
    Int64(msg.bodyLength) == bodylen ||
        throw(ValidationError("footer block body length does not match the message"))
    return msg, version, header_type
end

"""
    RangedFile(src::RangedSource; limits, tailbytes=65536, coalesce_gap=262144)

The scan-driven, fetch-minimal file handle: `Tables.scan(rf, scan)` runs
the fetch protocol — tail-first footer, batch windowing from block
metadata, dictionary bodies only for decode-set ids, and per-buffer body
ranges for exactly the decode set, coalesced under `coalesce_gap`.

Trust note, stated loudly: the ranged reader treats the FOOTER as the sole
schema authority — it does not parse and cross-check the leading schema
message or inspect the optional EOS marker. Head, tail, and coalesced requests
may physically over-read unrequested bytes. The full Footer Block index and
global features/message limit are checked up front. Per-record limits stay
lazy; every surviving candidate's metadata-only plan is validated before any
planned body range is requested.
"""
struct RangedFile{F}
    src::RangedSource{F}
    limits::Limits
    tailbytes::Int64
    coalesce_gap::Int64
end
function RangedFile(src::RangedSource; limits::Limits=Limits(),
    tailbytes::Integer=65536, coalesce_gap::Integer=262144)
    gap = Int64(coalesce_gap)
    gap >= 0 || throw(ArgumentError("negative coalesce gap"))
    return RangedFile(src, limits, Int64(max(tailbytes, 32)), gap)
end

"Fetch and verify the ranged footer: schema, fields, blocks, id table."
function _rangedfooter(rf::RangedFile, budget::AllocationBudget)
    src = rf.src
    limits = rf.limits
    _requirelittleendian()
    _validatelimits(limits)
    L = src.len
    L >= Int64(8 + 8 + 4 + 6) ||
        throw(ValidationError("file is too short to be an IPC file"))
    head = _fetchexact(src, Int64(0), Int64(8))
    head[1:6] == Vector{UInt8}(FILE_MAGIC) ||
        throw(ValidationError("missing leading ARROW1 magic"))
    tailstart = max(Int64(0), L - rf.tailbytes)
    tail = _fetchexact(src, tailstart, L - tailstart)
    tail[(end - 5):end] == Vector{UInt8}(FILE_MAGIC) ||
        throw(ValidationError("missing trailing ARROW1 magic"))
    footerlen = Int64(reinterpret(Int32, tail[(end - 9):(end - 6)])[1])
    0 < footerlen <= limits.max_metadata_bytes ||
        throw(ValidationError("footer length $footerlen outside (0, $(limits.max_metadata_bytes)]"))
    footerstart = L - 10 - footerlen
    footerstart >= 8 || throw(ValidationError("footer escapes the file"))

    _charge!(budget, footerlen, "footer allocation")
    footerbytes = footerstart >= tailstart ?
        tail[(footerstart - tailstart + 1):(footerstart - tailstart + footerlen)] :
        _fetchexact(src, footerstart, footerlen)
    version, features, dictblocks, recordblocks, reserve =
        verify_footer(footerbytes, limits, budget.left)
    _charge!(budget, reserve, "verified footer expansion")
    Int64(1) in features && throw(ValidationError(
        "dictionary replacement is forbidden in the IPC file format"))
    nmessages = AC.checked_add(Int64(1),
        AC.checked_add(Int64(length(dictblocks)), Int64(length(recordblocks))))
    nmessages <= limits.max_messages ||
        throw(ValidationError("message count exceeds limit"))
    footer = FB.getrootas(Meta.Footer, footerbytes, 0)
    metaschema = footer.schema
    metaschema === nothing &&
        throw(ValidationError("file footer carries no schema"))
    something(metaschema.endianness, Meta.Endianness.Little) == Meta.Endianness.Little ||
        throw(ValidationError("big-endian IPC is not supported (no endianness normalization)"))
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()
    fields = Field[corefield(f, dictids, fielddictids)
                   for f in something(metaschema.fields, Meta.Field[])]
    foreach(validateschemafield, fields)
    dictvaluefields = validatedictionaryids(fields, fielddictids)
    sch = Schema(fields; metadata=coremetadata(metaschema.custom_metadata),
        endianness=AC.LittleEndian)
    return (; sch, fields, dictids, fielddictids, dictvaluefields, version,
        features, dictblocks, recordblocks, footerstart, tail, tailstart,
        metaschema)
end

"""
One record block's row count for the zero-field ranged path — the SAME
frame discipline as the column path's metadata pass: extent bounds before
the fetch, the fetch and parse charged to the caller's cumulative budget,
`_parseblockmeta` framing (continuation prefix, declared length, verified
graph, body-length cross-check), header kind, footer-version agreement,
and compression rejection.
"""
function _zerofieldblockcount(rf::RangedFile, block::NTuple{3,Int64},
    version::Int16, fields::Vector{Field}, budget::AllocationBudget)
    _, metalen, bodylen = block
    declared = metalen - 8
    0 < declared <= rf.limits.max_metadata_bytes || throw(ValidationError(
        "metadata length $declared outside (0, $(rf.limits.max_metadata_bytes)]"))
    0 <= bodylen <= rf.limits.max_body_bytes || throw(ValidationError(
        "body length $bodylen outside [0, $(rf.limits.max_body_bytes)]"))
    _charge!(budget, metalen, "metadata range fetch")
    payload = _fetchexact(rf.src, block[1], metalen)
    msg, v, header_type = _parseblockmeta(payload, block, rf.limits, budget)
    header_type == UInt8(3) ||
        throw(ValidationError("footer record block is not a record batch"))
    v == version ||
        throw(ValidationError("IPC metadata version changes within the file"))
    rejectexperimentalcompression(msg, v, header_type)
    return _recordbatchmeta(msg.header::Meta.RecordBatch, fields, rf.limits,
        bodylen)
end

"Schema-only ranged read for the facade (one tail fetch)."
function rangedschema(rf::RangedFile)
    budget = AllocationBudget(rf.limits.max_total_allocated_bytes)
    ft = _rangedfooter(rf, budget)
    return ft.sch, ft.fields
end

function _applyscan(rf::RangedFile, scan::Tables.Scan)
    src = rf.src
    limits = rf.limits
    budget = AllocationBudget(limits.max_total_allocated_bytes)
    ft = _rangedfooter(rf, budget)
    fields = ft.fields
    dictids = ft.dictids
    fielddictids = ft.fielddictids
    dictvaluefields = ft.dictvaluefields
    version = ft.version
    features = ft.features
    dictblocks = ft.dictblocks
    recordblocks = ft.recordblocks
    footerstart = ft.footerstart
    tail = ft.tail
    tailstart = ft.tailstart
    metaschema = ft.metaschema
    names = Symbol[Symbol(fld.name) for fld in fields]
    allunique(names) || throw(ValidationError(
        "scan pushdown over duplicate column names is not supported; read the file without a scan"))
    b = Tables.bind(scan, names)
    if isempty(names)
        # Zero-field sources: consume filter and window HERE — an empty
        # residual NamedTuple cannot carry a row count through finish. The
        # metadata-only read keeps the column path's trust boundary: the
        # block index validates first, every touched block passes the full
        # frame checks, and every fetch charges the one cumulative budget.
        _validateblockindex(dictblocks, recordblocks, footerstart; datastart=8)
        # A zero-field schema declares no dictionary ids, so every indexed
        # dictionary block is orphaned — the same rejection the id-membership
        # check produces on the column path and in the full reader.
        isempty(dictblocks) || throw(ValidationError(
            "dictionary batch has no declaring field in a zero-field schema"))
        keep = _zerofieldpredicate(scan.filter)
        n = _zerofieldwindow(
            (_zerofieldblockcount(rf, block, version, fields, budget)
             for block in recordblocks), keep, scan.limit, scan.offset)
        return _scantable(Symbol[], (), Int(n)),
            Tables.Scan(nothing, nothing, nothing, 0, scan.validate)
    end
    decodeidx = sort!(unique!(vcat(Int[c.index for c in b.columns], copy(b.filtercols))))
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
    keep = trues(nrec)
    if scan.filter !== nothing
        stats = _readstats(coremetadata(metaschema.custom_metadata), nrec, fields;
            limits=limits, budget=budget)
        stats === nothing ||
            (keep = Bool[_maypass(scan.filter, stats[i].cols, names, stats[i].rows)
                         for i = 1:nrec])
    end
    recidxs = Int[i for i = 1:nrec if keep[i]]

    # One coalesced metadata pass over the dictionary blocks and the
    # SURVIVING record blocks; bodies come later and only for what the scan
    # needs.
    metablocks = vcat(dictblocks, NTuple{3,Int64}[recordblocks[i] for i in recidxs])
    # Match ArrowFile's lazy record limits: statistics-pruned records never
    # become candidates. Every candidate is bounded before its metadata fetch.
    for block in metablocks
        _, metalen, bodylen = block
        declared = metalen - 8
        0 < declared <= limits.max_metadata_bytes || throw(ValidationError(
            "metadata length $declared outside (0, $(limits.max_metadata_bytes)]"))
        0 <= bodylen <= limits.max_body_bytes || throw(ValidationError(
            "body length $bodylen outside [0, $(limits.max_body_bytes)]"))
    end
    metaspans = _fetchspans(src,
        NTuple{2,Int64}[(bl[1], bl[2]) for bl in metablocks], rf.coalesce_gap;
        budget=budget, what="metadata range fetch")
    blockmeta = Vector{Tuple{Meta.Message,Int16}}(undef, length(metablocks))
    for (i, block) in enumerate(metablocks)
        payload = AC.slicebytes(_spanslice(metaspans, block[1], block[2]))
        msg, v, header_type = _parseblockmeta(payload, block, limits, budget)
        expected_dict = i <= length(dictblocks)
        (expected_dict ? header_type == UInt8(2) : header_type == UInt8(3)) ||
            throw(ValidationError(expected_dict ?
                "footer dictionary block is not a dictionary batch" :
                "footer record block is not a record batch"))
        v == version ||
            throw(ValidationError("IPC metadata version changes within the file"))
        rejectexperimentalcompression(msg, v, header_type)
        if !expected_dict
            _recordbatchmeta(msg.header::Meta.RecordBatch, fields, limits,
                block[3])
        end
        blockmeta[i] = (msg, v)
    end

    # RecordBatch lengths live in block metadata, not the Footer. The metadata
    # pass above is required before limit/offset can choose body ranges.
    nsurv = length(recidxs)
    headers = [blockmeta[length(dictblocks) + p][1].header::Meta.RecordBatch
               for p = 1:nsurv]
    rowcounts = Int64[_recordbatchmeta(h, fields, limits,
        recordblocks[recidxs[p]][3]) for (p, h) in enumerate(headers)]
    consumed = scan.filter === nothing && _canconsumewindow(scan) &&
        (scan.limit !== nothing || scan.offset > 0)
    window = consumed ? _batchwindow(rowcounts, scan.offset, scan.limit) :
        Tuple{Int,Int64,Int64}[(p, Int64(0), Int64(-1)) for p = 1:nsurv]

    # Decode-set dictionaries: whole bodies, coalesced; everything else is
    # metadata-only forever.
    needed = isempty(window) ? Set{Int64}() :
        _neededdictids(fields, fielddictids, mask)
    dicts = Dict{Int64,ArrayData}()
    validated = AC._ValidatedDictionaries()
    seenids = Set{Int64}()
    wanted_dict = Int[]
    for (i, block) in enumerate(dictblocks)
        msg, _ = blockmeta[i]
        header = msg.header
        header isa Meta.DictionaryBatch ||
            throw(ValidationError("footer dictionary block is not a dictionary batch"))
        header.isDelta &&
            throw(ValidationError("delta dictionaries are not supported"))
        haskey(dictids, header.id) ||
            throw(ValidationError("dictionary batch has unknown id $(header.id)"))
        header.id in seenids &&
            throw(ValidationError("the file format carries one dictionary batch per id"))
        push!(seenids, header.id)
        rb = header.data
        vf = dictvaluefields[header.id]
        _recordbatchmeta(rb, (vf,), limits, block[3])
        codec = _batchcodec(rb.compression, blockmeta[i][2])
        if header.id in needed
            _validatebodyplan(rb, (vf,), limits, codec, Bool[true])
            push!(wanted_dict, i)
        end
    end
    for (p, _, _) in window
        _, v = blockmeta[length(dictblocks) + p]
        codec = _batchcodec(headers[p].compression, v)
        _validatebodyplan(headers[p], fields, limits, codec, mask)
    end
    missingids = setdiff(needed, seenids)
    isempty(missingids) || throw(ValidationError(
        "record batch references dictionary id $(first(missingids)) before its dictionary batch"))
    state = DecodeState(budget)
    try
        if !isempty(wanted_dict)
            bodyspans = _fetchspans(src,
                NTuple{2,Int64}[(dictblocks[i][1] + dictblocks[i][2], dictblocks[i][3])
                                for i in wanted_dict], rf.coalesce_gap)
            for i in wanted_dict
                block = dictblocks[i]
                msg, v = blockmeta[i]
                header = msg.header::Meta.DictionaryBatch
                rejectexperimentalcompression(msg, v, UInt8(2))
                rb = header.data
                codec = _batchcodec(rb.compression, v)
                vf = dictvaluefields[header.id]
                rblen = _recordbatchmeta(rb, (vf,), limits, block[3])
                body = _spanslice(bodyspans, block[1] + block[2], block[3])
                cursor = DecodeCursor(rb.nodes, rb.buffers, body, limits;
                    codec=codec, state=state, variadics=variadiccounts(rb))
                decoded = decodefield(vf, cursor, dicts, fielddictids)
                finishcursor!(cursor)
                decoded.len == rblen ||
                    throw(ValidationError("dictionary RecordBatch length does not match its field node"))
                validate_semantic(vf, decoded)
                validated[decoded] = nothing
                dicts[header.id] = decoded
            end
        end

        bodyranges = NTuple{2,Int64}[]
        blockwants = Dict{Int,Vector{NTuple{2,Int64}}}()
        for (p, _, _) in window
            block = recordblocks[recidxs[p]]
            header = headers[p]
            buffers = something(header.buffers, Meta.Buffer[])
            variadics = variadiccounts(header)
            varidx = Ref(1)
            wants = NTuple{2,Int64}[]
            bufidx = 1
            for (j, fld) in enumerate(fields)
                span64 = _bufferspan(fld, variadics, varidx)
                span64 <= typemax(Int) || throw(ValidationError(
                    "field buffer span $span64 exceeds the host index range"))
                span = Int(span64)
                if mask[j]
                    for k = bufidx:(bufidx + span - 1)
                        k <= length(buffers) ||
                            throw(ValidationError("metadata declares fewer buffers than the schema requires"))
                        buf = buffers[k]
                        len = Int64(buf.length)
                        off = Int64(buf.offset)
                        (off >= 0 && len >= 0 && AC.checked_add(off, len) <= block[3]) ||
                            throw(ValidationError("batch buffer [$off, $len] escapes its message body"))
                        len == 0 && continue
                        push!(wants, (off, len))
                    end
                end
                bufidx += span
            end
            blockwants[p] = wants
            bodystart = block[1] + block[2]
            append!(bodyranges, NTuple{2,Int64}[(bodystart + off, len) for (off, len) in wants])
        end
        bodyspans = _fetchspans(src, bodyranges, rf.coalesce_gap)

        parts = Dict{Int,Vector{Any}}(idx => Any[] for idx in decodeidx)
        outrows = 0
        for (p, skip, take) in window
            block = recordblocks[recidxs[p]]
            msg, v = blockmeta[length(dictblocks) + p]
            body = SparseBody(block[3], block[1] + block[2], bodyspans)
            _, cols = _maskedrecord(msg, v, body, fields, dicts, fielddictids,
                validated, limits, version, mask, state)
            outrows = _addscanrows(outrows, take >= 0 ? take : rowcounts[p])
            for idx in decodeidx
                col = materialize(fields[idx], cols[idx]::ArrayData)
                take >= 0 && (col = col[(skip + 1):(skip + take)])
                push!(parts[idx], col)
            end
        end
        outcols = Tuple(isempty(parts[idx]) ? Any[] : reduce(vcat, parts[idx])
                        for idx in decodeidx)
        table = _scantable(names[decodeidx], outcols, outrows)
        residualselect = scan.select === nothing ? nothing :
            Tables.SelectItem[Tables.SelectItem(names[c.index], c.type,
                c.name == names[c.index] ? nothing : c.name) for c in b.columns]
        limit = consumed ? nothing : scan.limit
        offset = consumed ? 0 : scan.offset
        residualfilter = _resolvefilter(scan.filter, names)
        return table, Tables.Scan(residualselect, residualfilter, limit, offset, scan.validate)
    finally
        close(state)
    end
end

"""
    Tables.scan(f::ArrowFile, scan)
    Tables.scan(rf::RangedFile, scan)

Scan an Arrow file handle: push down what the file format can prove
(`_applyscan` — column pruning, statistics batch pruning, exact
limit/offset windows) and hand the residual to the generic `Tables.scan`
executor, whose semantics the pushdown must agree with. `Arrow.Table(source;
scan=…)` is the public entry over the same path.
"""
function Tables.scan(f::Union{ArrowFile,RangedFile}, scan::Tables.Scan)
    table, residual = _applyscan(f, scan)
    return Tables.scan(table, residual)
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
# tail fetch alone powers pruning. Upgradeable: if upstream ever
# standardizes placement, we emit both keys through a deprecation cycle.
const STATS_KEY = "JuliaArrow:batch_statistics.v1"
const STATS_ROW_COUNT = "ARROW:row_count:exact"
const STATS_NULL_COUNT = "ARROW:null_count:exact"
const STATS_MIN = "ARROW:min_value:exact"
const STATS_MAX = "ARROW:max_value:exact"
const STATS_KEYPOOL = [STATS_ROW_COUNT, STATS_NULL_COUNT, STATS_MIN, STATS_MAX]

function _statsschema()
    key = Field("key", DictionaryType(IntType(32, true), Utf8Type(false), false);
        nullable=false, children=Field[])
    value = Field("value", UnionType(AC.DenseMode, Int8[0, 1, 2, 3]);
        nullable=false, children=Field[
            Field("i64", IntType(64, true); nullable=false),
            Field("f64", FloatType(64); nullable=false),
            Field("str", Utf8Type(false); nullable=false),
            Field("bool", BoolType(); nullable=false)])
    entries = Field("entries", StructType(); nullable=false,
        children=Field[key, value])
    return Schema(Field[
        Field("column", IntType(32, true); nullable=true),
        Field("statistics", MapType(false); nullable=false,
            children=Field[entries])])
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
    return ArrayData(Utf8Type(false), length(strs),
        [BufferSlice(), AC._databuffer(offsets), AC._databuffer(bytes)];
        nullcount=0)
end

"""
Fold one column's statistics: (null count, min, max) with `nothing` bounds
for empty, all-null, or unsupported-type columns. Values normalize into the
union's members: Int64 for integral scalars (dates, times, timestamps, and
durations are integral in the value domain), Float64, String, Bool.
"""
function _statfold(f::Field, d::ArrayData)
    t = f.type
    # Statistics describe LOGICAL values: dictionary columns fold through
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
        count(i -> ismissing(AC.getvalue(f, d, i)), 1:d.len)
    else
        AC.nullcount(d)
    end
    supported = stat isa IntType ? (stat.signed || stat.bits < 64) :
        stat isa FloatType || stat isa BoolType || stat isa Utf8Type ||
        (stat isa ViewType && stat.utf8) ||
        stat isa DateType || stat isa TimeType || stat isa TimestampType ||
        stat isa DurationType
    supported || return nc, nothing, nothing
    lo = hi = nothing
    hasnan = false
    for i = 1:d.len
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
    _statnorm(v) = v isa Bool ? v : v isa AbstractString ? String(v) :
        v isa AbstractFloat ? Float64(v) : Int64(v)
    return nc, lo === nothing || hasnan ? nothing : _statnorm(lo),
        hi === nothing || hasnan ? nothing : _statnorm(hi)
end

"One statistics record batch (the official layout) for one data batch."
function _statsbatch(statssch::Schema, nrows::Int64,
    colstats::Vector{Tuple{Int,Int64,Any,Any}})
    rows = 1 + length(colstats)               # batch-level row + per-column rows
    colvalid = vcat(false, trues(length(colstats)))
    colvals = vcat(Int32(0), Int32[Int32(c[1] - 1) for c in colstats])
    columndata = ArrayData(IntType(32, true), rows,
        [AC._databuffer(_bitmapbytes(colvalid)), AC._databuffer(colvals)];
        nullcount=1)
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
            push!(typeids, Int8(3)); push!(offsets, Int32(length(bools))); push!(bools, v)
        elseif v isa String
            push!(typeids, Int8(2)); push!(offsets, Int32(length(strs))); push!(strs, v)
        elseif v isa Float64
            push!(typeids, Int8(1)); push!(offsets, Int32(length(f64s))); push!(f64s, v)
        else
            push!(typeids, Int8(0)); push!(offsets, Int32(length(i64s))); push!(i64s, Int64(v))
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
    keydata = ArrayData(DictionaryType(IntType(32, true), Utf8Type(false), false),
        nentries, [BufferSlice(), AC._databuffer(keyidx)];
        dictionary=pool, nullcount=0)
    booldata = ArrayData(BoolType(), length(bools),
        [BufferSlice(), AC._databuffer(_bitmapbytes(bools))]; nullcount=0)
    valuedata = ArrayData(UnionType(AC.DenseMode, Int8[0, 1, 2, 3]), nentries,
        [AC._databuffer(typeids), AC._databuffer(offsets)];
        children=[ArrayData(IntType(64, true), length(i64s),
                [BufferSlice(), AC._databuffer(i64s)]; nullcount=0),
            ArrayData(FloatType(64), length(f64s),
                [BufferSlice(), AC._databuffer(f64s)]; nullcount=0),
            _utf8data(strs), booldata],
        nullcount=0)
    entriesdata = ArrayData(StructType(), nentries, [BufferSlice()];
        children=[keydata, valuedata], nullcount=0)
    mapdata = ArrayData(MapType(false), rows,
        [BufferSlice(), AC._databuffer(mapoffsets)];
        children=[entriesdata], nullcount=0)
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
        for (j, (f, col)) in enumerate(zip(sch.fields, batch.columns))
            nc, lo, hi = _statfold(f, col)
            push!(colstats, (fieldref, nc, lo, hi))
            fieldref += _fieldnodespan(f)
        end
        push!(statsbatches, _statsbatch(statssch, batch.nrows, colstats))
    end
    blob = Base64.base64encode(writestream(statssch, statsbatches))
    metadata = Dict{String,String}(something(sch.metadata, Dict{String,String}()))
    metadata[STATS_KEY] = blob
    return Schema(collect(Field, sch.fields); metadata=metadata,
        endianness=sch.endianness)
end

"Validate the canonical outer statistics-schema shape before using values."
function _validatestatsschema(sch::Schema)
    length(sch.fields) == 2 ||
        throw(ArgumentError("statistics schema must have two fields"))
    column, statistics = sch.fields
    ct = column.type
    column.name == "column" && column.nullable && ct isa IntType &&
        ct.bits == 32 && ct.signed && isempty(column.children) ||
        throw(ArgumentError("statistics column field is not nullable int32"))
    statistics.name == "statistics" && !statistics.nullable &&
        statistics.type isa MapType && length(statistics.children) == 1 ||
        throw(ArgumentError("statistics field is not a non-null map"))
    entries = statistics.children[1]
    !entries.nullable && entries.type isa StructType &&
        length(entries.children) == 2 ||
        throw(ArgumentError("statistics map entries are not a non-null key/value struct"))
    key, value = entries.children
    kt = key.type
    !key.nullable && kt isa DictionaryType && kt.indextype.bits == 32 &&
        kt.indextype.signed && kt.valuetype isa Utf8Type &&
        !kt.valuetype.large && isempty(key.children) ||
        throw(ArgumentError("statistics keys are not non-null dictionary<utf8, int32>"))
    !value.nullable && value.type isa UnionType &&
        value.type.mode == AC.DenseMode ||
        throw(ArgumentError("statistics values are not a non-null dense union"))
    return nothing
end

statsfile(sch::Schema, batches::AbstractVector{AC.RecordBatch};
    compress::Symbol=:none) =
    writefile(withstatistics(sch, batches), batches; compress=compress)

# ---- read + prune ---------------------------------------------------------

"""
Parse the statistics blob back through this reader. A missing key, corrupt
base64/stream, wrong schema, or wrong batch count degrades to `nothing` (no
pruning). Exhausting the caller's cumulative allocation budget still throws.
Returns per-batch `Dict{Int,...}` column stats (1-based top-level indices)
with `missing` bounds where absent.
"""
function _readstats(metadata, nbatches::Int, datafields=nothing;
    limits::Limits=Limits(), budget::Union{Nothing,AllocationBudget}=nothing)
    metadata === nothing && return nothing
    blob = get(Dict(metadata), STATS_KEY, nothing)
    blob === nothing && return nothing
    localbudget = budget === nothing ?
        AllocationBudget(limits.max_total_allocated_bytes) : budget
    try
        encodedbytes = Int64(ncodeunits(blob))
        maxdecoded = AC.checked_mul(cld(encodedbytes, Int64(4)), Int64(3))
        _charge!(localbudget, maxdecoded, "statistics base64 allocation")
        decoded = Base64.base64decode(blob)
        localbudget.left += maxdecoded - Int64(length(decoded))
        stream = _readstream(decoded, limits, localbudget)
        length(stream.batches) == nbatches || return nothing
        _validatestatsschema(stream.schema)
        colfield, mapfield = stream.schema.fields
        wiretotop = Dict{Int,Int}()
        totalnodes = 0
        if datafields !== nothing
            for (j, f) in enumerate(datafields)
                wiretotop[totalnodes] = j
                totalnodes += _fieldnodespan(f)
            end
        end
        out = NamedTuple[]
        for sb in stream.batches
            cols = materialize(colfield, sb.columns[1])
            maps = materialize(mapfield, sb.columns[2])
            length(cols) == length(maps) ||
                throw(ArgumentError("statistics columns have different lengths"))
            rows = missing
            d = Dict{Int,NamedTuple{(:nullcount, :min, :max),
                Tuple{Union{Missing,Int64},Any,Any}}}()
            for (colref, pairs) in zip(cols, maps)
                stats = Dict{String,Any}(String(k) => v for (k, v) in pairs)
                if colref === missing
                    rc = get(stats, STATS_ROW_COUNT, missing)
                    if rc !== missing
                        rc isa Int64 && rc >= 0 || throw(ArgumentError(
                            "statistics row count must be a nonnegative Int64"))
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
                    wire < totalnodes ||
                        throw(ArgumentError("statistics column index exceeds the schema"))
                    get(wiretotop, wire, nothing)
                end
                top === nothing && continue  # valid nested-field statistics
                nc = get(stats, STATS_NULL_COUNT, missing)
                if nc !== missing
                    nc isa Int64 && nc >= 0 || throw(ArgumentError(
                        "statistics null count must be a nonnegative Int64"))
                end
                d[top] = (nullcount=nc,
                    min=get(stats, STATS_MIN, missing),
                    max=get(stats, STATS_MAX, missing))
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
function _nextprefix(s::String)
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

function _statcmp(f, a, b)
    return try
        f(a, b) === false ? false : true
    catch
        true   # incomparable literal/stat types: never prune
    end
end

function _stateq(a, b)
    return try
        (a == b) === true
    catch
        false
    end
end

"""
One-sided may-contain evaluation of a scan predicate against one batch's
column statistics: `false` means PROVABLY no row qualifies (prune); `true`
means fetch and let the residual filter decide. Comparisons follow SQL
missing semantics — null rows never satisfy a comparison, so an all-null
column proves compare/`in_` predicates false.
"""
function _maypass(e::Tables.ScanExpr, stats, names, rowcount::Union{Missing,Int64})
    function lookup(col)
        i = Tables._findcol(names, col.ref)
        return i === nothing ? nothing : get(stats, i, nothing)
    end
    allnull(s) = s.nullcount !== missing && rowcount !== missing &&
        s.nullcount >= rowcount
    unknownbounds(s) = s.min === missing || s.max === missing ||
        (s.min isa AbstractFloat && isnan(s.min)) ||
        (s.max isa AbstractFloat && isnan(s.max))
    if e isa Tables.Cmp
        s = lookup(e.lhs)
        s === nothing && return true
        allnull(s) && return false
        unknownbounds(s) && return true
        v = e.rhs
        e.op == Tables.OP_EQ &&
            return _statcmp(>=, v, s.min) && _statcmp(>=, s.max, v)
        # NE prunes only a provably constant batch equal to the literal:
        # min == max == v. Anything weaker (including any NaN, where the
        # equalities are false) must fetch.
        e.op == Tables.OP_NE &&
            return !(_stateq(s.min, v) && _stateq(s.max, v))
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
        return any(_statcmp(>=, v, s.min) && _statcmp(>=, s.max, v)
                   for v in e.values)
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
        next = _nextprefix(e.s)
        return next === nothing || _statcmp(<, s.min, next)
    elseif e isa Tables.AndExpr
        return all(_maypass(a, stats, names, rowcount) for a in e.args)
    elseif e isa Tables.OrExpr
        return any(_maypass(a, stats, names, rowcount) for a in e.args)
    elseif e isa Tables.NotExpr
        inner = e.arg
        if inner isa Tables.Cmp && inner.op == Tables.OP_EQ
            s = lookup(inner.lhs)
            s === nothing && return true
            unknownbounds(s) && return true
            # everything equals v only when min == max == v
            return !(_stateq(s.min, inner.rhs) && _stateq(s.max, inner.rhs))
        end
        return true
    elseif e isa Tables.AlwaysFalse
        return false
    end
    return true    # AlwaysTrue, OpNode, unknown growth: never prune
end

