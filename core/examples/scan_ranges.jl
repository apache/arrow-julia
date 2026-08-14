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
function _batchrows(f::ArrowFile, i::Int)
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, budget)
    fm.msg.header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    return something(fm.msg.header.length, Int64(0))
end

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
    isempty(something(header.variadicBufferCounts, Int64[])) ||
        throw(ValidationError("variadic-buffer layouts are outside this prove-out"))
    _scanmissingdicts(fields, header.nodes, dicts, fielddictids, mask)
    rblen = something(header.length, Int64(0))
    0 <= rblen <= limits.max_array_length ||
        throw(ValidationError("record batch length $rblen exceeds limit"))
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

function _scanbatch(f::ArrowFile, i::Int, mask::AbstractVector{Bool})
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, budget)
    state = DecodeState(budget)
    try
        return _maskedrecord(fm.msg, fm.version, fm.body, f.fields,
            f.dictionaries, f.fielddictids, f.validated, f.limits,
            f.schemaversion, mask, state)
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
    consumed = scan.filter === nothing && (scan.limit !== nothing || scan.offset > 0)
    window = if consumed
        _batchwindow(Int64[_batchrows(f, i) for i = 1:length(f)],
            scan.offset, scan.limit)
    else
        Tuple{Int,Int64,Int64}[(i, Int64(0), Int64(-1)) for i = 1:length(f)]
    end
    parts = Dict{Int,Vector{Any}}(idx => Any[] for idx in decodeidx)
    for (i, skip, take) in window
        rblen, cols = _scanbatch(f, i, mask)
        for idx in decodeidx
            col = materialize(f.fields[idx], cols[idx]::ArrayData)
            take >= 0 && (col = col[(skip + 1):(skip + take)])
            push!(parts[idx], col)
        end
    end
    outcols = Tuple(isempty(parts[idx]) ? Any[] : reduce(vcat, parts[idx])
                    for idx in decodeidx)
    table = NamedTuple{Tuple(names[decodeidx])}(outcols)
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
    return table, Tables.Scan(residualselect, scan.filter, limit, offset, scan.validate)
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
    sorted = sort(ranges)
    out = NTuple{2,Int64}[sorted[1]]
    for (off, len) in Iterators.drop(sorted, 1)
        loff, llen = out[end]
        if off <= loff + llen + gap
            out[end] = (loff, max(llen, AC.checked_add(off, len) - loff))
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

function _fetchspans(src::RangedSource, ranges::Vector{NTuple{2,Int64}}, gap::Int64)
    spans = _coalesce(ranges, gap)
    payloads = fetchranges(src, spans)
    slices = BufferSlice[BufferSlice(heapregion(p), 0, length(p)) for p in payloads]
    return FetchedSpans(Int64[s[1] for s in spans], Int64[s[2] for s in spans], slices)
end

function _spanslice(fs::FetchedSpans, off::Int64, len::Int64)
    len == 0 && return BufferSlice()
    i = searchsortedlast(fs.starts, off)
    (i >= 1 && off >= fs.starts[i] && AC.checked_add(off, len) <= fs.starts[i] + fs.lens[i]) ||
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
    len == 0 && return BufferSlice()
    (offset >= 0 && len >= 0 && offset <= sb.bodylen - len) ||
        throw(ArgumentError("batch buffer escapes its message body"))
    return _spanslice(sb.spans, AC.checked_add(sb.bodystart, offset), len)
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
message, and it bounds blocks by the footer start rather than running the
whole-file optional-EOS preflight (both need bytes a range reader has no
other reason to fetch). A forged block overlapping unfetched territory
fails at decode validation, not at open.
"""
struct RangedFile{F}
    src::RangedSource{F}
    limits::Limits
    tailbytes::Int64
    coalesce_gap::Int64
end
RangedFile(src::RangedSource; limits::Limits=Limits(),
    tailbytes::Integer=65536, coalesce_gap::Integer=262144) =
    RangedFile(src, limits, Int64(max(tailbytes, 32)), Int64(coalesce_gap))

function Tables.apply(rf::RangedFile, scan::Tables.Scan)
    src = rf.src
    limits = rf.limits
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
    version, _, dictblocks, recordblocks, reserve =
        verify_footer(footerbytes, limits, budget.left)
    _charge!(budget, reserve, "verified footer expansion")
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

    # Block extents against the data boundary (footer start), pairwise
    # non-overlap by sortedness of the verified footer vectors.
    for block in vcat(dictblocks, recordblocks)
        off, metalen, bodylen = block
        (off >= 8 && metalen >= 16 && bodylen >= 0 &&
         off % 8 == 0 && metalen % 8 == 0 && bodylen % 8 == 0) ||
            throw(ValidationError("footer block has invalid extents"))
        AC.checked_add(AC.checked_add(off, metalen), bodylen) <= footerstart ||
            throw(ValidationError("footer block escapes the data section"))
    end

    # One coalesced metadata pass over every block (record AND dictionary —
    # ids and row counts both live there); bodies come later and only for
    # what the scan needs.
    allblocks = vcat(dictblocks, recordblocks)
    metaspans = _fetchspans(src, NTuple{2,Int64}[(bl[1], bl[2]) for bl in allblocks],
        rf.coalesce_gap)
    blockmeta = Vector{Tuple{Meta.Message,Int16}}(undef, length(allblocks))
    for (i, block) in enumerate(allblocks)
        payload = AC.slicebytes(_spanslice(metaspans, block[1], block[2]))
        msg, v, header_type = _parseblockmeta(payload, block, limits, budget)
        expected_dict = i <= length(dictblocks)
        (expected_dict ? header_type == UInt8(2) : header_type == UInt8(3)) ||
            throw(ValidationError(expected_dict ?
                "footer dictionary block is not a dictionary batch" :
                "footer record block is not a record batch"))
        v == version ||
            throw(ValidationError("IPC metadata version changes within the file"))
        blockmeta[i] = (msg, v)
    end

    # Decode-set dictionaries: whole bodies, coalesced; everything else is
    # metadata-only forever.
    needed = _neededdictids(fields, fielddictids, mask)
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
                isempty(something(rb.variadicBufferCounts, Int64[])) ||
                    throw(ValidationError("variadic-buffer layouts are outside this prove-out"))
                vf = dictvaluefields[header.id]
                rblen = something(rb.length, Int64(0))
                0 <= rblen <= limits.max_array_length ||
                    throw(ValidationError("dictionary batch length $rblen exceeds limit"))
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

        # Batch window from metadata row counts, then per-buffer body ranges
        # for exactly the decode set of exactly the surviving batches.
        nrec = length(recordblocks)
        headers = [blockmeta[length(dictblocks) + i][1].header::Meta.RecordBatch
                   for i = 1:nrec]
        rowcounts = Int64[something(h.length, Int64(0)) for h in headers]
        consumed = scan.filter === nothing && (scan.limit !== nothing || scan.offset > 0)
        window = consumed ? _batchwindow(rowcounts, scan.offset, scan.limit) :
            Tuple{Int,Int64,Int64}[(i, Int64(0), Int64(-1)) for i = 1:nrec]

        bodyranges = NTuple{2,Int64}[]
        blockwants = Dict{Int,Vector{NTuple{2,Int64}}}()
        for (i, _, _) in window
            block = recordblocks[i]
            header = headers[i]
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
                        len == 0 && continue
                        off = Int64(buf.offset)
                        (off >= 0 && len >= 0 && AC.checked_add(off, len) <= block[3]) ||
                            throw(ValidationError("batch buffer [$off, $len] escapes its message body"))
                        push!(wants, (off, len))
                    end
                end
                bufidx += span
            end
            blockwants[i] = wants
            bodystart = block[1] + block[2]
            append!(bodyranges, NTuple{2,Int64}[(bodystart + off, len) for (off, len) in wants])
        end
        bodyspans = _fetchspans(src, bodyranges, rf.coalesce_gap)

        parts = Dict{Int,Vector{Any}}(idx => Any[] for idx in decodeidx)
        for (i, skip, take) in window
            block = recordblocks[i]
            msg, v = blockmeta[length(dictblocks) + i]
            body = SparseBody(block[3], block[1] + block[2], bodyspans)
            _, cols = _maskedrecord(msg, v, body, fields, dicts, fielddictids,
                validated, limits, version, mask, state)
            for idx in decodeidx
                col = materialize(fields[idx], cols[idx]::ArrayData)
                take >= 0 && (col = col[(skip + 1):(skip + take)])
                push!(parts[idx], col)
            end
        end
        outcols = Tuple(isempty(parts[idx]) ? Any[] : reduce(vcat, parts[idx])
                        for idx in decodeidx)
        table = NamedTuple{Tuple(names[decodeidx])}(outcols)
        residualselect = scan.select === nothing ? nothing :
            Tables.SelectItem[Tables.SelectItem(names[c.index], c.type,
                c.name == names[c.index] ? nothing : c.name) for c in b.columns]
        limit = consumed ? nothing : scan.limit
        offset = consumed ? 0 : scan.offset
        return table, Tables.Scan(residualselect, scan.filter, limit, offset, scan.validate)
    finally
        close(state)
    end
end

# ---------------------------------------------------------------------------
# Acceptance: differential against Tables.finish, plus skip proofs
# ---------------------------------------------------------------------------

function _fulltable(f::ArrowFile)
    names = Tuple(Symbol(fld.name) for fld in f.fields)
    cols = Tuple(reduce(vcat, Any[materialize(fld, f[i].columns[j])
                                  for i = 1:length(f)])
                 for (j, fld) in enumerate(f.fields))
    return NamedTuple{names}(cols)
end

function _tables_equal(a, b)
    ca, cb = Tables.columns(a), Tables.columns(b)
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
    println("window-excluded batches are never fetched ✓")

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

    # Hostile inputs fail closed: forged footer length, block escaping the
    # data section, and truncated objects.
    badlen = copy(filebytes)
    lenpos = length(badlen) - 9
    badlen[lenpos:(lenpos + 3)] .= reinterpret(UInt8, Int32[Int32(2)^30])
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(badlen)), Tables.Scan()))
    @assert _rejects(() -> Tables.read(RangedFile(RangedSource(filebytes[1:20])), Tables.Scan()))
    println("forged footers and truncated objects fail closed ✓")

    println()
    println("Byte-range scan checks passed.")
end

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    filebytes, af, full = _scan_main()
    _ranged_main(filebytes, af, full)
end
