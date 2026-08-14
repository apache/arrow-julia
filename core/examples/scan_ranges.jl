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
Decode batch `i` under `mask`: masked-in fields decode and validate exactly
as `getindex`; masked-out fields advance through `skipfield!`. The cursor
must still finish clean — a skewed batch fails identically either way.
"""
function _scanbatch(f::ArrowFile, i::Int, mask::AbstractVector{Bool})
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, budget)
    fm.version == f.schemaversion ||
        throw(ValidationError("IPC metadata version changes within the file"))
    rejectexperimentalcompression(fm)
    header = fm.msg.header
    header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    codec = _batchcodec(header.compression, fm.version)
    isempty(something(header.variadicBufferCounts, Int64[])) ||
        throw(ValidationError("variadic-buffer layouts are outside this prove-out"))
    _scanmissingdicts(f.fields, header.nodes, f.dictionaries, f.fielddictids, mask)
    rblen = something(header.length, Int64(0))
    0 <= rblen <= f.limits.max_array_length ||
        throw(ValidationError("record batch length $rblen exceeds limit"))
    state = DecodeState(budget)
    try
        cursor = DecodeCursor(header.nodes, header.buffers, fm.body, f.limits;
            codec=codec, state=state)
        cols = Vector{Union{Nothing,ArrayData}}(nothing, length(f.fields))
        for (j, fld) in enumerate(f.fields)
            if mask[j]
                cols[j] = decodefield(fld, cursor, f.dictionaries, f.fielddictids)
            else
                skipfield!(fld, cursor)
            end
        end
        finishcursor!(cursor)
        for (j, fld) in enumerate(f.fields)
            col = cols[j]
            col === nothing && continue
            AC._validate_semantic(fld, col, f.validated)
            col.len == rblen ||
                throw(ValidationError("RecordBatch length does not match top-level field nodes"))
        end
        return rblen, cols
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

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    _scan_main()
end
