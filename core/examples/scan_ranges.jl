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
# PROVE-OUT: Tables.Scan pushdown over the IPC file adapter
# (`DESIGN-scan-ranges-trim.md` §1, Stage A), and — further down — the
# byte-range fetch protocol over the same bound column set (§2).
#
# Run with the repo project, with Tables.jl's `jq/scan` branch dev'ed in:
#
#     julia --project=. core/examples/scan_ranges.jl
#
# Stage-A semantics, exactly as the design specifies:
#
#   * the decode set is (selected ∪ filter-referenced) columns — everything
#     else is SKIPPED by `skipfield!`, a registry walk that consumes the
#     node/buffer accounting (all buffer-table invariants still checked)
#     without slicing, decompressing, validating, or materializing anything;
#   * `limit`/`offset` are consumed EXACTLY when no filter is present:
#     `RecordBatch.length` is wire metadata, so whole batches outside the
#     window are never decoded;
#   * the returned table keeps SOURCE names over the decode set and the
#     residual keeps `select` and `filter` — `Tables.finish` filters,
#     projects, renames, and converts. This is the only composition that
#     stays correct when the filter references unselected columns.
#
# The acceptance battery is differential: for every scan,
# `Tables.read(file, scan)` must equal `Tables.finish(full_table, scan)`,
# and corruption probes prove skipped columns and skipped batches are
# genuinely never decoded.
# =============================================================================

include(joinpath(@__DIR__, "ipc_write.jl"))

using Tables
isdefined(Tables, :Scan) ||
    error("this prove-out needs Tables.jl's `jq/scan` branch (Tables.Scan); " *
          "dev it into the repo project: Pkg.develop(path=\"~/.julia/dev/Tables\")")

# ---------------------------------------------------------------------------
# skipfield!: the decode walk minus the decode
# ---------------------------------------------------------------------------

"""
Advance the cursor past one field's node and buffers — the exact traversal
`decodefield` performs, with every buffer-table invariant still enforced
(`_buffermeta!`), but no body access: nothing is sliced, decompressed,
validated, or kept. Over a ranged source (§2) the skipped bytes are never
even fetched.
"""
function skipfield!(f::Field, c::DecodeCursor)
    t = f.type
    takenode!(c)
    spec = layoutspec(t)
    for _ in spec.buffers
        skipbuffer!(c)
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

"Buffers consumed by one field subtree — the planner's registry arithmetic."
function _bufferspan(f::Field)
    spec = layoutspec(f.type)
    n = length(spec.buffers)
    f.type isa DictionaryType && return n
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        n += _bufferspan(f.children[i])
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
    isempty(something(header.variadicBufferCounts, Int64[])) ||
        throw(ValidationError("variadic-buffer layouts are outside this prove-out"))
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
    expectedbuffers = sum(_bufferspan(f) for f in fields; init=0)
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
        codec=codec, state=state)
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
# Tables.apply: Stage A
# ---------------------------------------------------------------------------

"""
Exact batch windowing for consumed `limit`/`offset`: per surviving batch,
how many leading rows to drop and how many to keep. Batches wholly outside
the window are absent — never decoded.
"""
function _batchwindow(rowcounts::Vector{Int64}, offset::Int, limit::Union{Nothing,Int})
    window = Tuple{Int,Int64,Int64}[]   # (batch index, skip, take)
    remaining_skip = Int64(offset)
    remaining_take = limit === nothing ? typemax(Int64) : Int64(limit)
    for (i, rows) in enumerate(rowcounts)
        remaining_take <= 0 && break
        if remaining_skip >= rows
            remaining_skip -= rows
            continue
        end
        take = min(rows - remaining_skip, remaining_take)
        push!(window, (i, remaining_skip, take))
        remaining_take -= take
        remaining_skip = 0
    end
    return window
end

function Tables.apply(f::ArrowFile, scan::Tables.Scan)
    names = Symbol[Symbol(fld.name) for fld in f.fields]
    allunique(names) || throw(ValidationError(
        "scan pushdown over duplicate column names is facade work; read the file without a scan"))
    b = Tables.bind(scan, names)
    decodeidx = sort!(unique!(vcat(Int[c.index for c in b.columns], copy(b.filtercols))))
    mask = falses(length(names))
    mask[decodeidx] .= true
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    state = DecodeState(budget)
    try
        consumed = scan.filter === nothing && (scan.limit !== nothing || scan.offset > 0)
        window = if consumed
            _batchwindow(Int64[_batchrows(f, i, budget) for i = 1:length(f)],
                scan.offset, scan.limit)
        else
            Tuple{Int,Int64,Int64}[(i, Int64(0), Int64(-1)) for i = 1:length(f)]
        end
        # Statistics pruning (design §3): one-sided — a pruned batch is provably
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
            outrows += Int(take >= 0 ? take : rblen)
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
# §2: byte-range reads — RangedSource{F}, the planner, and sparse decode
# ===========================================================================

"""
    RangedSource{F}

The fetcher contract (design §2): `fetch(offset::Int64, len::Int64) ->
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
authority invariant, sparse (design §2).
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

The scan-driven, fetch-minimal file handle: `Tables.apply(rf, scan)` runs
the design's fetch protocol — tail-first footer, batch windowing from block
metadata, dictionary bodies only for decode-set ids, and per-buffer body
ranges for exactly the decode set, coalesced under `coalesce_gap`.

Trust note, stated loudly: the ranged reader treats the FOOTER as the sole
schema authority — it does not fetch and cross-check the leading schema
message or inspect the optional EOS marker. Footer Block non-overlap,
resource limits, message kinds, and every RecordBatch node/buffer invariant
are still validated from fetched metadata before any body fetch.
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

function Tables.apply(rf::RangedFile, scan::Tables.Scan)
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
    budget = AllocationBudget(limits.max_total_allocated_bytes)
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
        throw(ValidationError("big-endian IPC requires normalization, which is outside this prove-out"))
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()
    fields = Field[corefield(f, dictids, fielddictids)
                   for f in something(metaschema.fields, Meta.Field[])]
    foreach(validateschemafield, fields)
    dictvaluefields = validatedictionaryids(fields, fielddictids)
    names = Symbol[Symbol(fld.name) for fld in fields]
    allunique(names) || throw(ValidationError(
        "scan pushdown over duplicate column names is facade work; read the file without a scan"))
    b = Tables.bind(scan, names)
    decodeidx = sort!(unique!(vcat(Int[c.index for c in b.columns], copy(b.filtercols))))
    mask = falses(length(names))
    mask[decodeidx] .= true

    # Footer Blocks remain mutually exclusive and bounded even though the
    # leading schema and optional EOS bytes are not fetched.
    _validateblockindex(dictblocks, recordblocks, footerstart; datastart=8)
    for block in vcat(dictblocks, recordblocks)
        _, metalen, bodylen = block
        declared = metalen - 8
        0 < declared <= limits.max_metadata_bytes || throw(ValidationError(
            "metadata length $declared outside (0, $(limits.max_metadata_bytes)]"))
        0 <= bodylen <= limits.max_body_bytes || throw(ValidationError(
            "body length $bodylen outside [0, $(limits.max_body_bytes)]"))
    end

    # Statistics pruning happens FIRST (design §3): the stats live in the
    # footer schema's metadata, so pruned batches never even get their
    # block metadata fetched. Pruning applies only under a filter, and the
    # window applies only without one, so the two never interact.
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
        if !expected_dict
            _recordbatchmeta(msg.header::Meta.RecordBatch, fields, limits, block[3])
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
    consumed = scan.filter === nothing && (scan.limit !== nothing || scan.offset > 0)
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
            throw(ValidationError("delta dictionaries are outside this prove-out"))
        haskey(dictids, header.id) ||
            throw(ValidationError("dictionary batch has unknown id $(header.id)"))
        header.id in seenids &&
            throw(ValidationError("the file format carries one dictionary batch per id"))
        push!(seenids, header.id)
        _recordbatchmeta(header.data, (dictvaluefields[header.id],), limits, block[3])
        header.id in needed && push!(wanted_dict, i)
    end
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
                    codec=codec, state=state)
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
            wants = NTuple{2,Int64}[]
            bufidx = 1
            for (j, fld) in enumerate(fields)
                span = _bufferspan(fld)
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
            outrows += Int(take >= 0 ? take : rowcounts[p])
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

# ===========================================================================
# §3: per-batch statistics — the official value layout in a footer key
# ===========================================================================

import Base64

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

_bitmapbytes(bits::AbstractVector{Bool}) = begin
    bytes = zeros(UInt8, cld(length(bits), 8))
    for (i, b) in enumerate(bits)
        b && (bytes[1 + (i - 1) ÷ 8] |= UInt8(1) << ((i - 1) % 8))
    end
    bytes
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
    stat = t isa DictionaryType ? t.valuetype : t
    nc = if t isa DictionaryType
        count(1:d.len) do i
            !AC.isvalid_at(d, i) || ismissing(AC.getvalue(f, d, i))
        end
    else
        AC.nullcount(d)
    end
    supported = stat isa IntType ? (stat.signed || stat.bits < 64) :
        stat isa FloatType || stat isa BoolType || stat isa Utf8Type ||
        stat isa DateType || stat isa TimeType || stat isa TimestampType ||
        stat isa DurationType
    supported || return nc, nothing, nothing
    lo = hi = nothing
    hasnan = false
    for i = 1:d.len
        AC.isvalid_at(d, i) || continue
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

_statcmp(f, a, b) = try
    f(a, b) === false ? false : true
catch
    true   # incomparable literal/stat types: never prune
end

_stateq(a, b) = try
    (a == b) === true
catch
    false
end

"""
One-sided may-contain evaluation of a scan predicate against one batch's
column statistics: `false` means PROVABLY no row qualifies (prune); `true`
means fetch and let the residual filter decide. Comparisons follow SQL
missing semantics — null rows never satisfy a comparison, so an all-null
column proves compare/`in_` predicates false.
"""
function _maypass(e::Tables.ScanExpr, stats, names, rowcount::Union{Missing,Int64})
    lookup(col) = begin
        i = Tables._findcol(names, col.ref)
        i === nothing ? nothing : get(stats, i, nothing)
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
        e.op == Tables.OP_LT && return _statcmp(<, s.min, v)
        e.op == Tables.OP_LE && return _statcmp(<=, s.min, v)
        e.op == Tables.OP_GT && return _statcmp(>, s.max, v)
        return _statcmp(>=, s.max, v)          # OP_GE
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

# ---------------------------------------------------------------------------
# Acceptance: differential against Tables.finish, plus skip proofs
# ---------------------------------------------------------------------------

function _fulltable(f::ArrowFile)
    names = Tuple(Symbol(fld.name) for fld in f.fields)
    if isempty(names)
        return _ScanColumns(NamedTuple(), Int(sum(_batchrows(f, i) for i = 1:length(f); init=0)))
    end
    cols = Tuple(begin
        parts = Any[materialize(fld, f[i].columns[j]) for i = 1:length(f)]
        isempty(parts) ? Any[] : reduce(vcat, parts)
    end for (j, fld) in enumerate(f.fields))
    return NamedTuple{names}(cols)
end

function _tables_equal(a, b)
    ca, cb = Tables.columns(a), Tables.columns(b)
    Tables.rowcount(ca) == Tables.rowcount(cb) || return false
    na, nb = Tables.columnnames(ca), Tables.columnnames(cb)
    collect(na) == collect(nb) || return false
    for n in na
        isequal(collect(Any, Tables.getcolumn(ca, n)),
                collect(Any, Tables.getcolumn(cb, n))) || return false
    end
    return true
end

"Body byte range of buffer number `bufindex` (1-based) of record batch `i`."
function _bufferposition(bytes::Vector{UInt8}, i::Int, bufindex::Int)
    file = readfile(copy(bytes))
    block = file.recordblocks[i]
    budget = AllocationBudget(file.limits.max_total_allocated_bytes)
    fm = _blockmessage(heapregion(copy(bytes)), block, file.dataend, file.limits, budget)
    header = fm.msg.header::Meta.RecordBatch
    buf = header.buffers[bufindex]
    bodystart = block[1] + block[2]
    return bodystart + Int64(buf.offset), Int64(buf.length)
end

function _scan_main()
    expected = (
        ints=Int64[1, 2, 3, 4, 5],
        floats=[1.5, missing, 3.5, missing, 5.5],
        bools=[true, false, true, missing, false],
        strs=["hey", "", missing, "αβ∀", "last"],
        lists=[[1, 2], Int64[], [3], missing, [4, 5, 6]],
        structs=[(a=1, b="x"), (a=2, b="y"), (a=3, b="z"), (a=4, b="w"), (a=5, b="v")],
        dict=Arrow.DictEncode(["lo", "hi", "lo", missing, "hi"]),
    )
    io = IOBuffer()
    Arrow.write(io, Tables.partitioner([expected, expected]); file=false)
    source = readstream(take!(io))
    filebytes = writefile(source)
    af = readfile(copy(filebytes))
    full = _fulltable(af)

    scans = Tables.Scan[
        Tables.Scan(),
        Tables.Scan(select=(:ints, :strs)),
        Tables.Scan(select=(:strs => :s2, :ints)),
        Tables.Scan(select=(r"s",)),
        Tables.Scan(select=(Tables.Not(:dict),)),
        Tables.Scan(filter=Tables.col(:ints) > 2),
        Tables.Scan(select=(:floats,), filter=Tables.col(:ints) > 2),
        Tables.Scan(select=(:dict,), filter=Tables.isnull(Tables.col(:floats))),
        Tables.Scan(limit=3),
        Tables.Scan(offset=7),
        Tables.Scan(offset=4, limit=3),
        Tables.Scan(offset=10),
        Tables.Scan(select=(:ints => Float64,)),
        Tables.Scan(select=(:ints,), filter=Tables.col(:ints) > 2, limit=2),
        Tables.Scan(filter=Tables.in_(Tables.col(:strs), ("hey", "last"))),
        Tables.Scan(select=(:strs, :lists), filter=Tables.col(3) == true),
        Tables.Scan(select=(:strs => :ints,), filter=Tables.col(4) == "hey"),
    ]
    for scan in scans
        got = Tables.read(af, scan)
        want = Tables.finish(full, scan)
        @assert _tables_equal(got, want) sprint(show, scan)
    end
    println("differential scans match Tables.finish over the full table ✓")

    # Residual semantics: window consumption vs filter poisoning.
    _, r1 = Tables.apply(af, Tables.Scan(select=(:ints,), offset=4, limit=3))
    @assert r1.limit === nothing && r1.offset == 0 && r1.select !== nothing
    _, r2 = Tables.apply(af, Tables.Scan(filter=Tables.col(:ints) > 2, limit=2))
    @assert r2.limit == 2 && r2.filter !== nothing
    println("limit/offset consume exactly; filters poison the window ✓")

    # Skip proof 1 (columns): corrupt the `strs` OFFSETS buffer of batch 2 so
    # semantic validation must reject any decode that touches it. Buffer
    # order: ints(v,d) floats(v,d) bools(v,d) strs(v,o,d) → offsets is #8.
    off, len = _bufferposition(filebytes, 2, 8)
    @assert len > 8
    corrupt = copy(filebytes)
    corrupt[(off + 5):(off + 8)] .= reinterpret(UInt8, Int32[Int32(2)^30])
    caf = readfile(copy(corrupt))
    @assert _rejects(() -> caf[2])                       # full decode sees it
    got = Tables.read(caf, Tables.Scan(select=(:ints, :floats)))
    @assert isequal(collect(Any, got.ints), collect(Any, full.ints))
    @assert _rejects(() -> Tables.read(caf, Tables.Scan(select=(:strs,))))
    println("skipped columns are never decoded (corruption stays invisible) ✓")

    # Skip proof 2 (batches): the same corruption sits in batch 2; a window
    # ending inside batch 1 never decodes batch 2 even when selecting strs.
    got = Tables.read(caf, Tables.Scan(select=(:strs,), limit=5))
    @assert isequal(collect(Any, got.strs), collect(Any, full.strs[1:5]))
    @assert _rejects(() -> Tables.read(caf, Tables.Scan(select=(:strs,), limit=6)))
    println("window-excluded batches are never decoded ✓")

    # Buffer-table invariants cannot be weakened by skipping: `skipbuffer!`
    # shares `_buffermeta!` with `takebuffer!` by construction, and for files
    # the round-15 open-time preflight enforces the same containment and
    # non-overlap rules before any cursor (selected or skipped) runs at all.
    overlap = copy(filebytes)
    block = readfile(copy(filebytes)).recordblocks[1]
    fmoff = block[1]
    # rewrite floats-data's declared offset backwards via the metadata:
    # locate buffer entry 4 inside the block metadata and zero its offset.
    meta = copy(overlap[(fmoff + 9):(fmoff + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    rb = _headertable(meta, msg)
    start, n = _vvector(rb, 2, 16; required=true)
    @assert n >= 4
    _write_i64!(meta, start + 3 * 16, Int64(0))
    copyto!(overlap, fmoff + 9, meta, 1, length(meta))
    @assert _rejects(() -> readfile(copy(overlap)))
    println("buffer-table invariants hold before any skip can run ✓")

    # Duplicate source names are a declared facade boundary.
    dupfields = Field[Field("x", IntType(64, true), true, nothing, Field[]),
                      Field("x", IntType(64, true), true, nothing, Field[])]
    dupsch = Schema(dupfields)
    dupcol() = ArrayData(IntType(64, true), 1,
        [BufferSlice(), AC._databuffer(Int64[7])]; nullcount=0)
    dupbytes = writefile(dupsch, [AC.RecordBatch(dupsch, ArrayData[dupcol(), dupcol()], 1)])
    dupaf = readfile(dupbytes)
    @assert _rejects(() -> Tables.apply(dupaf, Tables.Scan(select=(1,))))
    println("duplicate-name scans refuse cleanly (facade boundary) ✓")

    # Window row counts are metadata, but they are not trusted until the
    # RecordBatch length agrees with every top-level FieldNode. Otherwise a
    # corrupt skipped batch can shift the window and return valid but wrong
    # rows from a later batch.
    xio = IOBuffer()
    Arrow.write(xio, Tables.partitioner([(x=collect(Int64, 1:5),),
        (x=collect(Int64, 6:10),)]); file=false)
    xbytes = writefile(readstream(take!(xio)))
    badrows = copy(xbytes)
    xfile = readfile(copy(xbytes))
    block = xfile.recordblocks[1]
    meta = copy(badrows[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    rb = _headertable(meta, msg)
    _write_i64!(meta, _vfield(rb, 0, 8; required=true), Int64(4))
    copyto!(badrows, block[1] + 9, meta, 1, length(meta))
    shifted = Tables.Scan(select=(:x,), offset=5, limit=1)
    @assert _rejects(() -> Tables.read(readfile(copy(badrows)), shifted))
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(copy(badrows))), shifted))
    println("window row counts require top-level FieldNode agreement ✓")

    # A column table cannot infer row count when it has no columns. The scan
    # wrapper keeps the RecordBatch lengths so an empty scan remains identity.
    zerosch = Schema(Field[])
    zerobatches = AC.RecordBatch[
        AC.RecordBatch(zerosch, ArrayData[], 3),
        AC.RecordBatch(zerosch, ArrayData[], 0),
        AC.RecordBatch(zerosch, ArrayData[], 2)]
    zerobytes = writefile(zerosch, zerobatches)
    for source in (readfile(copy(zerobytes)), RangedFile(RangedSource(copy(zerobytes))))
        got = Tables.read(source, Tables.Scan())
        @assert isempty(Tables.columnnames(Tables.columns(got)))
        @assert Tables.rowcount(Tables.columns(got)) == 5
    end
    println("zero-column scans preserve their row count ✓")

    println()
    println("Tables.Scan Stage-A pushdown checks passed.")
    return filebytes, af, full
end

function _ranged_main(filebytes::Vector{UInt8}, af::ArrowFile, full)
    # Correctness: the ranged reader is differentially equal to the
    # whole-file reader across the scan battery.
    scans = Tables.Scan[
        Tables.Scan(),
        Tables.Scan(select=(:ints, :strs)),
        Tables.Scan(select=(:strs => :s2,)),
        Tables.Scan(select=(Tables.Not(:dict),)),
        Tables.Scan(select=(:dict,)),
        Tables.Scan(select=(:floats,), filter=Tables.col(:ints) > 2),
        Tables.Scan(offset=4, limit=3),
        Tables.Scan(select=(:ints,), filter=Tables.col(:ints) > 2, limit=2),
        Tables.Scan(select=(:strs, :lists), filter=Tables.col(3) == true),
    ]
    for scan in scans
        log, src = countingsource(filebytes)
        got = Tables.read(RangedFile(src), scan)
        want = Tables.finish(full, scan)
        @assert _tables_equal(got, want) sprint(show, scan)
    end
    println("ranged reads are differentially equal to whole-file reads ✓")

    # Byte accounting needs bodies that dwarf metadata: a two-column file
    # where the fat column is ~7× the narrow one. Selecting the narrow
    # column must fetch a small fraction of what the full scan fetches.
    n = 20_000
    fat(i) = string("padding-padding-padding-padding-padding-", i)
    bigio = IOBuffer()
    Arrow.write(bigio, Tables.partitioner([
        (a=collect(Int64, 1:n), b=[fat(i) for i = 1:n]),
        (a=collect(Int64, (n + 1):2n), b=[fat(i) for i = (n + 1):2n])]);
        file=false)
    bigbytes = writefile(readstream(take!(bigio)))
    logall, srcall = countingsource(bigbytes)
    Tables.read(RangedFile(srcall; tailbytes=256, coalesce_gap=64), Tables.Scan())
    logone, srcone = countingsource(bigbytes)
    Tables.read(RangedFile(srcone; tailbytes=256, coalesce_gap=64),
        Tables.Scan(select=(:a,)))
    @assert logone.bytes < logall.bytes ÷ 4 (logone.bytes, logall.bytes)
    println("narrow selections fetch a fraction of the bytes " *
            "($(logone.bytes) vs $(logall.bytes) of $(length(bigbytes))) ✓")

    # Unfetched-column proof: corrupt an unselected column's buffer ON THE
    # SOURCE — the scan succeeds AND the corrupted byte was never fetched.
    off, len = _bufferposition(filebytes, 2, 8)          # strs offsets, batch 2
    corrupt = copy(filebytes)
    corrupt[(off + 5):(off + 8)] .= reinterpret(UInt8, Int32[Int32(2)^30])
    logc, srcc = countingsource(corrupt)
    got = Tables.read(RangedFile(srcc; tailbytes=256, coalesce_gap=0), Tables.Scan(select=(:ints,)))
    @assert isequal(collect(Any, got.ints), collect(Any, full.ints))
    @assert !_fetched(logc, off + 5)
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(corrupt)),
        Tables.Scan(select=(:strs,))))
    println("skipped columns are never fetched (corruption stays untouched) ✓")

    # Window proof: limit inside batch 1 fetches no batch-2 body bytes.
    block2 = af.recordblocks[2]
    body2 = (block2[1] + block2[2], block2[3])
    logw, srcw = countingsource(filebytes)
    Tables.read(RangedFile(srcw; tailbytes=256, coalesce_gap=0), Tables.Scan(select=(:strs,), limit=5))
    @assert !any(_fetched(logw, body2[1] + k) for k = 0:8:(body2[2] - 1))
    println("window-excluded batch bodies are never fetched ✓")

    # Dictionary bodies are fetched only when a dictionary column is in the
    # decode set.
    dictblockbody = let
        # dict block extents via the footer: re-derive from the file bytes
        footerlen = Int64(reinterpret(Int32,
            filebytes[(end - 9):(end - 6)])[1])
        fb = filebytes[(end - 9 - footerlen):(end - 10)]
        _, _, dblocks, _, _ = verify_footer(fb, Limits())
        @assert length(dblocks) == 1
        (dblocks[1][1] + dblocks[1][2], dblocks[1][3])
    end
    lognod, srcnod = countingsource(filebytes)
    Tables.read(RangedFile(srcnod; tailbytes=256, coalesce_gap=0), Tables.Scan(select=(:ints,)))
    @assert !any(_fetched(lognod, dictblockbody[1] + k)
                 for k = 0:8:(dictblockbody[2] - 1))
    logd, srcd = countingsource(filebytes)
    Tables.read(RangedFile(srcd; tailbytes=256, coalesce_gap=0), Tables.Scan(select=(:dict,)))
    @assert any(_fetched(logd, dictblockbody[1] + k)
                for k = 0:8:(dictblockbody[2] - 1))
    logd0, srcd0 = countingsource(filebytes)
    Tables.read(RangedFile(srcd0; tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:dict,), limit=0))
    @assert !any(_fetched(logd0, dictblockbody[1] + k)
                 for k = 0:8:(dictblockbody[2] - 1))
    println("dictionary bodies are fetched only for decode-set ids ✓")

    # Coalescing: an infinite gap merges every body range into one request;
    # a zero gap issues more, smaller requests; both agree with the truth.
    logbig, srcbig = countingsource(filebytes)
    gotbig = Tables.read(RangedFile(srcbig; coalesce_gap=typemax(Int32)),
        Tables.Scan(select=(:ints, :strs)))
    logzero, srczero = countingsource(filebytes)
    gotzero = Tables.read(RangedFile(srczero; coalesce_gap=0),
        Tables.Scan(select=(:ints, :strs)))
    want = Tables.finish(full, Tables.Scan(select=(:ints, :strs)))
    @assert _tables_equal(gotbig, want) && _tables_equal(gotzero, want)
    @assert logbig.requests < logzero.requests
    @assert logzero.bytes <= logbig.bytes
    @assert _coalesce(NTuple{2,Int64}[(0, 8), (16, 8)], typemax(Int64)) ==
        NTuple{2,Int64}[(0, 24)]
    @assert try
        _coalesce(NTuple{2,Int64}[(0, 8)], Int64(-1))
        false
    catch e
        e isa ArgumentError
    end
    println("coalescing trades requests for bytes without changing results " *
            "($(logbig.requests) reqs/$(logbig.bytes)B vs $(logzero.requests) reqs/$(logzero.bytes)B) ✓")

    # A tail smaller than the footer forces the exact follow-up fetch.
    logt, srct = countingsource(filebytes)
    gott = Tables.read(RangedFile(srct; tailbytes=32), Tables.Scan(select=(:ints,)))
    @assert isequal(collect(Any, gott.ints), collect(Any, full.ints))
    println("undersized tails recover with one exact footer fetch ✓")

    # Compressed files range-read identically (per-buffer frames are
    # self-contained behind their prefixes).
    io = IOBuffer()
    Arrow.write(io, Tables.partitioner([
        (x=Int64[1, 2, 3], s=["a", "bb", "ccc"]),
        (x=Int64[4, 5, 6], s=["dd", "e", "ff"])]); file=false)
    zsource = readstream(take!(io))
    zbytes = writefile(zsource; compress=:zstd)
    zfull = _fulltable(readfile(copy(zbytes)))
    logz, srcz = countingsource(zbytes)
    gotz = Tables.read(RangedFile(srcz; tailbytes=256, coalesce_gap=64), Tables.Scan(select=(:x,)))
    @assert isequal(collect(Any, gotz.x), collect(Any, zfull.x))
    @assert logz.bytes < length(zbytes)
    println("compressed files range-read through self-contained buffers ✓")

    # Hostile inputs fail closed: forged footer length, overlapping Blocks,
    # out-of-body zero-length buffers, and truncated objects.
    badlen = copy(filebytes)
    lenpos = length(badlen) - 9
    badlen[lenpos:(lenpos + 3)] .= reinterpret(UInt8, Int32[Int32(2)^30])
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(badlen)), Tables.Scan()))
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(filebytes[1:20])), Tables.Scan()))

    overlap = copy(filebytes)
    footerlen = Int64(reinterpret(Int32, overlap[(end - 9):(end - 6)])[1])
    footerstart = Int64(length(overlap)) - 10 - footerlen
    footerbytes = copy(overlap[(footerstart + 1):(footerstart + footerlen)])
    footertable = _vtable(footerbytes, Int64(_vu32(footerbytes, 0)))
    recordstart, nrecords = _vvector(footertable, 3, 24; required=true)
    @assert nrecords >= 2
    firstblock = verify_footer(footerbytes, Limits())[4][1]
    _write_i64!(footerbytes, recordstart + 24, firstblock[1])
    _write_i32!(footerbytes, recordstart + 32, Int32(firstblock[2]))
    _write_i64!(footerbytes, recordstart + 40, firstblock[3])
    copyto!(overlap, footerstart + 1, footerbytes, 1, length(footerbytes))
    @assert _rejects(() -> readfile(copy(overlap)))
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(overlap)), Tables.Scan()))

    zerobuffer = copy(filebytes)
    block = af.recordblocks[1]
    meta = copy(zerobuffer[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    rb = _headertable(meta, msg)
    bufferstart, _ = _vvector(rb, 2, 16; required=true)
    _write_i64!(meta, bufferstart, block[3] + 8)
    copyto!(zerobuffer, block[1] + 9, meta, 1, length(meta))
    @assert _rejects(() -> readfile(copy(zerobuffer)))
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(zerobuffer)),
        Tables.Scan(select=(:ints,))))
    println("forged footers and truncated objects fail closed ✓")

    # Ranged limits are checked before body fetching. One whole-file Scan also
    # keeps one aggregate budget across every batch it decompresses.
    @assert _rejects(() -> Tables.read(
        RangedFile(RangedSource(filebytes); limits=Limits(max_body_bytes=32)),
        Tables.Scan(select=(:ints,))))
    @assert _rejects(() -> Tables.read(
        RangedFile(RangedSource(filebytes); limits=Limits(max_messages=1)), Tables.Scan()))
    loglimit, srclimit = countingsource(filebytes)
    intoff, _ = _bufferposition(filebytes, 1, 2)
    @assert _rejects(() -> Tables.read(RangedFile(srclimit;
        limits=Limits(max_buffer_bytes=8), tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:ints,))))
    @assert !_fetched(loglimit, intoff)

    large = (x=zeros(Int64, 10_000),)
    largeio = IOBuffer()
    Arrow.write(largeio, Tables.partitioner([large, large]); file=false)
    largebytes = writefile(readstream(take!(largeio)); compress=:zstd)
    tight = Limits(max_total_allocated_bytes=100_000)
    @assert _rejects(() -> Tables.read(readfile(copy(largebytes); limits=tight),
        Tables.Scan(select=(:x,))))
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(largebytes); limits=tight),
        Tables.Scan(select=(:x,))))
    println("range limits and scan-wide allocation budgets fail before overuse ✓")

    println()
    println("Byte-range scan checks passed.")
end

function _stats_main()
    # Two batches with DISJOINT ranges so predicates can discriminate:
    # batch 1: x ∈ 1:5, s ∈ "apple".."eagle";  batch 2: x ∈ 6:10, s ∈ "fig".."jam".
    t1 = (x=Int64[1, 2, 3, 4, 5], s=["apple", "berry", "cedar", "date", "eagle"])
    t2 = (x=Int64[6, 7, 8, 9, 10], s=["fig", "grape", "hazel", "iris", "jam"])
    io = IOBuffer()
    Arrow.write(io, Tables.partitioner([t1, t2]); file=false)
    source = readstream(take!(io))
    sbytes = statsfile(source.schema, source.batches)
    saf = readfile(copy(sbytes))
    sfull = _fulltable(saf)

    # The statistics blob is itself a valid stream this reader accepts, and
    # a file carrying it stays readable by this reader AND Arrow.jl 2.x.
    stats = _readstats(saf.schema.metadata, 2, saf.fields)
    @assert stats !== nothing
    @assert stats[1].rows == 5 && stats[2].rows == 5
    @assert stats[1].cols[1].min == 1 && stats[1].cols[1].max == 5
    @assert stats[2].cols[2].min == "fig" && stats[2].cols[2].max == "jam"
    filetbl = Arrow.Table(IOBuffer(copy(sbytes)))
    @assert length(Tables.getcolumn(Tables.columns(filetbl), 1)) == 10
    println("statistics round-trip the official value layout (Core + 2.x carry) ✓")

    # Official column references use the flattened RecordBatch FieldNode
    # order. A top-level field after a nested subtree is not its top-level
    # ordinal.
    nestedfields = Field[
        Field("st", StructType(); children=Field[
            Field("a", IntType(64, true)), Field("b", IntType(64, true))]),
        Field("x", IntType(64, true))]
    nestedsch = Schema(nestedfields)
    ints(v) = ArrayData(IntType(64, true), length(v),
        [BufferSlice(), AC._databuffer(Int64.(v))]; nullcount=0)
    structdata = ArrayData(StructType(), 2, [BufferSlice()];
        children=[ints([1, 2]), ints([3, 4])], nullcount=0)
    nestedbatch = AC.RecordBatch(nestedsch, [structdata, ints([5, 6])], 2)
    nestedstats = withstatistics(nestedsch, [nestedbatch])
    nestedstream = readstream(Base64.base64decode(Dict(nestedstats.metadata)[STATS_KEY]))
    refs = materialize(nestedstream.schema.fields[1], nestedstream.batches[1].columns[1])
    @assert isequal(collect(Any, refs), Any[missing, Int32(0), Int32(3)])
    println("statistics use official flattened FieldNode column indexes ✓")

    # Differential correctness with pruning active, whole-file and ranged.
    prunescans = Tables.Scan[
        Tables.Scan(filter=Tables.col(:x) > 7),
        Tables.Scan(select=(:s,), filter=Tables.col(:x) <= 3),
        Tables.Scan(filter=Tables.col(:x) > 100),
        Tables.Scan(filter=Tables.in_(Tables.col(:x), (2, 4))),
        Tables.Scan(filter=Tables.isnull(Tables.col(:x))),
        Tables.Scan(filter=Tables.startswith(Tables.col(:s), "i")),
        Tables.Scan(filter=(Tables.col(:x) > 2) & (Tables.col(:x) < 9)),
        Tables.Scan(filter=!(Tables.col(:x) == 3)),
    ]
    for scan in prunescans
        want = Tables.finish(sfull, scan)
        @assert _tables_equal(Tables.read(saf, scan), want) sprint(show, scan)
        @assert _tables_equal(
            Tables.read(RangedFile(RangedSource(copy(sbytes))), scan), want) sprint(show, scan)
    end
    println("pruned scans stay differentially exact (whole-file + ranged) ✓")

    # Float pruning must use the same IEEE operators as Tables.finish.
    fio = IOBuffer()
    Arrow.write(fio, Tables.partitioner([
        (x=Float64[0.0, 0.0],),
        (x=Float64[-0.0, -0.0],),
        (x=Float64[NaN, NaN],)]); file=false)
    fsource = readstream(take!(fio))
    fbytes = statsfile(fsource.schema, fsource.batches)
    faf = readfile(copy(fbytes))
    ffull = _fulltable(faf)
    floatscans = Tables.Scan[
        Tables.Scan(filter=Tables.col(:x) == -0.0),
        Tables.Scan(filter=Tables.col(:x) <= -0.0),
        Tables.Scan(filter=Tables.col(:x) >= 0.0),
        Tables.Scan(filter=Tables.in_(Tables.col(:x), (-0.0,))),
        Tables.Scan(filter=!(Tables.col(:x) == NaN))]
    for scan in floatscans
        want = Tables.finish(ffull, scan)
        @assert _tables_equal(Tables.read(faf, scan), want)
        @assert _tables_equal(Tables.read(RangedFile(RangedSource(fbytes)), scan), want)
    end
    println("float pruning preserves signed-zero and NaN predicate semantics ✓")

    # Dictionary nullness is logical: a valid outer index can resolve to a
    # null pool value and must count as null without entering min/max folds.
    pool = ArrayData(Utf8Type(false), 1,
        [AC._databuffer(UInt8[0x00]), AC._databuffer(Int32[0, 0]), BufferSlice()];
        nullcount=1)
    dtype = DictionaryType(IntType(32, true), Utf8Type(false), false)
    dfield = Field("d", dtype)
    ddata = ArrayData(dtype, 1,
        [BufferSlice(), AC._databuffer(Int32[0])]; dictionary=pool, nullcount=0)
    @assert _statfold(dfield, ddata) == (1, nothing, nothing)
    println("dictionary statistics count null pool values logically ✓")

    # Fetch proof: x > 7 prunes batch 1 — its block metadata AND body are
    # never fetched over a ranged source.
    block1 = saf.recordblocks[1]
    logp, srcp = countingsource(sbytes)
    got = Tables.read(RangedFile(srcp; tailbytes=256, coalesce_gap=0),
        Tables.Scan(filter=Tables.col(:x) > 7))
    @assert isequal(collect(Any, got.x), Any[8, 9, 10])
    @assert !any(_fetched(logp, block1[1] + k) for k = 0:8:(block1[2] + block1[3] - 1))
    println("stat-pruned batches are never fetched, metadata included ✓")

    # Decode proof (whole-file): semantic corruption inside a pruned batch
    # stays invisible with statistics, and is caught without them.
    soff, slen = _bufferposition(sbytes, 1, 4)          # batch 1 `s` offsets
    @assert slen > 8
    scorrupt = copy(sbytes)
    scorrupt[(soff + 5):(soff + 8)] .= reinterpret(UInt8, Int32[Int32(2)^30])
    scanx = Tables.Scan(select=(:s,), filter=Tables.col(:x) > 7)
    got = Tables.read(readfile(copy(scorrupt)), scanx)
    @assert isequal(collect(Any, got.s), Any["hazel", "iris", "jam"])
    plainbytes = writefile(source.schema, source.batches)
    pcorrupt = copy(plainbytes)
    poff, _ = _bufferposition(plainbytes, 1, 4)
    pcorrupt[(poff + 5):(poff + 8)] .= reinterpret(UInt8, Int32[Int32(2)^30])
    @assert _rejects(() -> Tables.read(readfile(copy(pcorrupt)), scanx))
    println("pruning skips decode; without statistics the same scan must decode ✓")

    # Malformed statistics degrade to no pruning, never to an error.
    badmeta = Dict{String,String}(STATS_KEY => "!!not-base64!!")
    badsch = Schema(collect(Field, source.schema.fields); metadata=badmeta,
        endianness=source.schema.endianness)
    badbytes = writefile(badsch, source.batches)
    got = Tables.read(readfile(copy(badbytes)), Tables.Scan(filter=Tables.col(:x) > 7))
    @assert isequal(collect(Any, got.x), Any[8, 9, 10])
    wrongio = IOBuffer()
    Arrow.write(wrongio, Tables.partitioner([(q=Int64[1],), (q=Int64[2],)]); file=false)
    wrongblob = Base64.base64encode(take!(wrongio))
    wrongsch = Schema(collect(Field, source.schema.fields);
        metadata=Dict{String,String}(STATS_KEY => wrongblob),
        endianness=source.schema.endianness)
    wrongbytes = writefile(wrongsch, source.batches)
    for sourcefile in (readfile(copy(wrongbytes)), RangedFile(RangedSource(wrongbytes)))
        got = Tables.read(sourcefile, Tables.Scan(filter=Tables.col(:x) > 7))
        @assert isequal(collect(Any, got.x), Any[8, 9, 10])
    end

    # A two-field stream is not enough: the canonical physical skeleton is
    # part of the official value-layout contract.
    rawstats = readstream(Base64.base64decode(Dict(saf.schema.metadata)[STATS_KEY]))
    boolsch = Schema(Field[
        Field("column", BoolType(); nullable=true), rawstats.schema.fields[2]])
    boolbatches = AC.RecordBatch[]
    for sb in rawstats.batches
        valid = trues(sb.nrows)
        valid[1] = false
        boolcol = ArrayData(BoolType(), sb.nrows,
            [AC._databuffer(_bitmapbytes(valid)),
             AC._databuffer(_bitmapbytes(trues(sb.nrows)))]; nullcount=1)
        push!(boolbatches, AC.RecordBatch(boolsch,
            ArrayData[boolcol, sb.columns[2]], sb.nrows))
    end
    boolblob = Base64.base64encode(writestream(boolsch, boolbatches))
    @assert _readstats(Dict(STATS_KEY => boolblob), 2, source.schema.fields) === nothing

    statssch = _statsschema()
    hugevalue = repeat("x", 2_000_000)
    hugebatches = AC.RecordBatch[_statsbatch(statssch, Int64(1),
        Tuple{Int64,Int64,Any,Any}[(1, Int64(0), hugevalue, hugevalue)])]
    hugeblob = Base64.base64encode(writestream(statssch, hugebatches; compress=:zstd))
    bombio = IOBuffer()
    Arrow.write(bombio, (s=["x"],); file=false)
    bombsource = readstream(take!(bombio))
    hugesch = Schema(collect(Field, bombsource.schema.fields);
        metadata=Dict{String,String}(STATS_KEY => hugeblob),
        endianness=bombsource.schema.endianness)
    hugebytes = writefile(hugesch, bombsource.batches)
    for cap in (Int64(50_000), Int64(100_000))
        tight = Limits(max_total_allocated_bytes=cap)
        for sourcefile in (readfile(copy(hugebytes); limits=tight),
            RangedFile(RangedSource(hugebytes); limits=tight))
            rejected = try
                Tables.read(sourcefile, Tables.Scan(filter=Tables.col(:s) == "x"))
                false
            catch e
                e isa AllocationLimitError
            end
            @assert rejected
        end
    end
    println("malformed statistics degrade; allocation exhaustion propagates ✓")

    # The trust model, pinned (design §3): wide lies only cost pruning;
    # narrow lies silently LOSE rows — statistics are trusted-for-
    # completeness, exactly like Parquet row-group stats.
    function liarfile(lo2, hi2)
        statssch = _statsschema()
        lie = AC.RecordBatch[
            _statsbatch(statssch, Int64(5),
                [(1, Int64(0), Int64(1), Int64(5)), (2, Int64(0), "apple", "eagle")]),
            _statsbatch(statssch, Int64(5),
                [(1, Int64(0), lo2, hi2), (2, Int64(0), "fig", "jam")])]
        blob = Base64.base64encode(writestream(statssch, lie))
        liesch = Schema(collect(Field, source.schema.fields);
            metadata=Dict{String,String}(STATS_KEY => blob),
            endianness=source.schema.endianness)
        return writefile(liesch, source.batches)
    end
    wide = Tables.read(readfile(liarfile(Int64(-1000), Int64(1000))),
        Tables.Scan(filter=Tables.col(:x) > 8))
    @assert isequal(collect(Any, wide.x), Any[9, 10])
    narrow = Tables.read(readfile(liarfile(Int64(6), Int64(7))),
        Tables.Scan(filter=Tables.col(:x) > 8))
    @assert isempty(narrow.x)          # rows 9, 10 silently lost: the trust boundary
    println("wide lies cost pruning only; narrow lies lose rows (trust model pinned) ✓")

    println()
    println("Statistics write/prune checks passed.")
end

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    filebytes, af, full = _scan_main()
    _ranged_main(filebytes, af, full)
    _stats_main()
end
