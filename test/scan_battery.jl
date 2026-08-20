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

# ---------------------------------------------------------------------------
# Acceptance: differential against the generic Tables.scan executor, plus skip proofs
# ---------------------------------------------------------------------------

function _fulltable(f::ArrowFile)
    names = Tuple(Symbol(fld.name) for fld in f.fields)
    if isempty(names)
        nrows = 0
        for i = 1:length(f)
            nrows = _addscanrows(nrows, _batchrows(f, i))
        end
        return _ScanColumns(NamedTuple(), nrows)
    end
    cols = Tuple(
        begin
            parts = Any[materialize(fld, f[i].columns[j]) for i = 1:length(f)]
            isempty(parts) ? Any[] : reduce(vcat, parts)
        end for (j, fld) in enumerate(f.fields)
    )
    return NamedTuple{names}(cols)
end

function _tables_equal(a, b)
    ca, cb = Tables.columns(a), Tables.columns(b)
    Tables.rowcount(ca) == Tables.rowcount(cb) || return false
    na, nb = Tables.columnnames(ca), Tables.columnnames(cb)
    collect(na) == collect(nb) || return false
    for n in na
        isequal(
            collect(Any, Tables.getcolumn(ca, n)),
            collect(Any, Tables.getcolumn(cb, n)),
        ) || return false
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

function _setbufferlength!(
    bytes::Vector{UInt8},
    block::NTuple{3,Int64},
    bufindex::Int,
    len::Int64,
)
    meta = copy(bytes[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    header = _headertable(meta, msg)
    kind = _vu8(meta, _vfield(msg, 1, 1; required=true))
    rb = kind == UInt8(2) ? _vtable(meta, _vref(header, 1; required=true)) : header
    start, n = _vvector(rb, 2, 16; required=true)
    1 <= bufindex <= n || throw(BoundsError(1:n, bufindex))
    _write_i64!(meta, start + (bufindex - 1) * 16 + 8, len)
    copyto!(bytes, block[1] + 9, meta, 1, length(meta))
    return bytes
end

function _setnodelength!(
    bytes::Vector{UInt8},
    block::NTuple{3,Int64},
    nodeindex::Int,
    len::Int64,
)
    meta = copy(bytes[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    header = _headertable(meta, msg)
    kind = _vu8(meta, _vfield(msg, 1, 1; required=true))
    rb = kind == UInt8(2) ? _vtable(meta, _vref(header, 1; required=true)) : header
    start, n = _vvector(rb, 1, 16; required=true)
    1 <= nodeindex <= n || throw(BoundsError(1:n, nodeindex))
    _write_i64!(meta, start + (nodeindex - 1) * 16, len)
    copyto!(bytes, block[1] + 9, meta, 1, length(meta))
    return bytes
end

function _setnodenullcount!(
    bytes::Vector{UInt8},
    block::NTuple{3,Int64},
    nodeindex::Int,
    count::Int64,
)
    meta = copy(bytes[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    header = _headertable(meta, msg)
    kind = _vu8(meta, _vfield(msg, 1, 1; required=true))
    rb = kind == UInt8(2) ? _vtable(meta, _vref(header, 1; required=true)) : header
    start, n = _vvector(rb, 1, 16; required=true)
    1 <= nodeindex <= n || throw(BoundsError(1:n, nodeindex))
    _write_i64!(meta, start + (nodeindex - 1) * 16 + 8, count)
    copyto!(bytes, block[1] + 9, meta, 1, length(meta))
    return bytes
end

"File fixture carrying Arrow 0.17's V4 message-level compression marker."
function _legacyv4file()
    stream = _experimental_v4_stream(Int64(42))
    frames = _frameinfo(stream)
    schemaframe = stream[frames[1].frame]
    recordframe = stream[frames[2].frame]
    metalen = Int64(8 + length(frames[2].metadata))
    bodylen = Int64(length(recordframe)) - metalen

    out = UInt8[]
    append!(out, FILE_MAGIC)
    append!(out, zeros(UInt8, 2))
    append!(out, schemaframe)
    recordoffset = Int64(length(out))
    append!(out, recordframe)
    append!(out, reinterpret(UInt8, UInt32[CONTINUATION, UInt32(0)]))

    sch = Schema(Field[Field("x", IntType(64, true); nullable=true)])
    fielddictids = assigndictids(sch.fields)
    b = FB.Builder(512)
    schoff = _metaschema!(b, sch, fielddictids, Int64[])
    Meta.footerStartDictionariesVector(b, 0)
    dictvec = FB.endvector!(b, 0)
    Meta.footerStartRecordBatchesVector(b, 1)
    Meta.createBlock(b, recordoffset, Int32(metalen), bodylen)
    recordvec = FB.endvector!(b, 1)
    Meta.footerStart(b)
    Meta.footerAddVersion(b, Meta.MetadataVersion.V4)
    Meta.footerAddSchema(b, schoff)
    Meta.footerAddDictionaries(b, dictvec)
    Meta.footerAddRecordBatches(b, recordvec)
    FB.finish!(b, Meta.footerEnd(b))
    footer = collect(FB.finishedbytes(b))
    append!(out, footer)
    append!(out, reinterpret(UInt8, Int32[Int32(length(footer))]))
    append!(out, FILE_MAGIC)
    return out, (recordoffset, metalen, bodylen)
end

function _scan_main()
    expected = MIXED_EXPECTED
    source = readstream(_mixed_two_partitions_bytes())
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
        Tables.Scan(filter=Tables.colin(Tables.col(:strs), ("hey", "last"))),
        Tables.Scan(filter=Tables.colin(Tables.col(:strs), (missing,))),
        Tables.Scan(select=(:strs, :lists), filter=Tables.colcmp(==, Tables.col(3), true)),
        Tables.Scan(
            select=(:strs => :ints,),
            filter=Tables.colcmp(==, Tables.col(4), "hey"),
        ),
    ]
    for scan in scans
        got = Tables.scan(af, scan)
        want = Tables.scan(full, scan)
        @assert _tables_equal(got, want) sprint(show, scan)
    end
    println("differential scans match Tables.scan over the full table ✓")

    # A column's element type is a property of the SCHEMA, not of how many
    # rows a scan kept: a scan that keeps no rows (limit 0, an offset past
    # the input, a filter statistics prune to nothing) has exactly the schema
    # of the full direct scan, on both the file and the ranged handle.
    fullschema = Tables.schema(Tables.scan(af, Tables.Scan()))
    for emptyscan in (
        Tables.Scan(limit=0),
        Tables.Scan(offset=10_000),
        Tables.Scan(filter=Tables.col(:ints) > 10_000),
    )
        for handle in (af, SourceFile(BytesSource(copy(filebytes))))
            got = Tables.scan(handle, emptyscan)
            @assert Tables.rowcount(Tables.columns(got)) == 0
            @assert Tables.schema(got) == fullschema sprint(show, emptyscan)
        end
    end
    println("empty scans keep the full scan's schema on both handles ✓")

    # Residual semantics: window consumption vs filter poisoning.
    _, r1 = _applyscan(af, Tables.Scan(select=(:ints,), offset=4, limit=3))
    @assert r1.limit === nothing && r1.offset == 0 && r1.select !== nothing
    _, r2 = _applyscan(af, Tables.Scan(filter=Tables.col(:ints) > 2, limit=2))
    @assert r2.limit == 2 && r2.filter !== nothing
    println("limit/offset consume exactly; filters poison the window ✓")

    # Extreme-but-valid windows: Tables.scan saturates, so the whole
    # pipeline agrees on the empty result whether the window is consumed at
    # the source or residualized.
    extreme = Tables.Scan(select=(:ints,), offset=typemax(Int), limit=typemax(Int))
    extremewant = Tables.scan(full, extreme)
    for sourcefile in (af, SourceFile(BytesSource(filebytes)))
        got = Tables.scan(sourcefile, extreme)
        @assert _tables_equal(got, extremewant)
        @assert length(Tables.getcolumn(Tables.columns(got), 1)) == 0
    end
    println("extreme scan windows saturate to the empty result ✓")

    # Skip proof 1 (columns): corrupt the `strs` OFFSETS buffer of batch 2 so
    # semantic validation must reject any decode that touches it. Buffer
    # order: ints(v,d) floats(v,d) bools(v,d) strs(v,o,d) → offsets is #8.
    off, len = _bufferposition(filebytes, 2, 8)
    @assert len > 8
    corrupt = copy(filebytes)
    corrupt[(off + 5):(off + 8)] .= reinterpret(UInt8, Int32[Int32(2) ^ 30])
    caf = readfile(copy(corrupt))
    @assert _rejects(() -> caf[2])                       # full decode sees it
    got = Tables.scan(caf, Tables.Scan(select=(:ints, :floats)))
    @assert isequal(collect(Any, got.ints), collect(Any, full.ints))
    @assert _rejects(() -> Tables.scan(caf, Tables.Scan(select=(:strs,))))
    println("skipped columns are never decoded (corruption stays invisible) ✓")

    # Skip proof 2 (batches): the same corruption sits in batch 2; a window
    # ending inside batch 1 never decodes batch 2 even when selecting strs.
    got = Tables.scan(caf, Tables.Scan(select=(:strs,), limit=5))
    @assert isequal(collect(Any, got.strs), collect(Any, full.strs[1:5]))
    @assert _rejects(() -> Tables.scan(caf, Tables.Scan(select=(:strs,), limit=6)))
    println("window-excluded batches are never decoded ✓")

    # Buffer-table invariants cannot be weakened by skipping: `skipbuffer!`
    # shares `_buffermeta!` with `takebuffer!` by construction, and for files
    # the open-time preflight enforces the same containment and
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
    dupfields = Field[
        Field("x", IntType(64, true), true, nothing, Field[]),
        Field("x", IntType(64, true), true, nothing, Field[]),
    ]
    dupsch = Schema(dupfields)
    dupcol() = ArrayData(
        IntType(64, true),
        1,
        [BufferSlice(), AC._databuffer(Int64[7])];
        nullcount=0,
    )
    dupbytes = writefile(dupsch, [AC.RecordBatch(dupsch, ArrayData[dupcol(), dupcol()], 1)])
    dupaf = readfile(dupbytes)
    @assert _rejects(() -> _applyscan(dupaf, Tables.Scan(select=(1,))))
    println("duplicate-name scans refuse cleanly (facade boundary) ✓")

    # Window row counts are metadata, but they are not trusted until the
    # RecordBatch length agrees with every top-level FieldNode. Otherwise a
    # corrupt skipped batch can shift the window and return valid but wrong
    # rows from a later batch.
    xbytes = writefile(
        readstream(
            _fixture2x("int64-two-batches") do
                xio = IOBuffer()
                Arrow.write(
                    xio,
                    Tables.partitioner([
                        (x=collect(Int64, 1:5),),
                        (x=collect(Int64, 6:10),),
                    ]);
                    file=false,
                )
                take!(xio)
            end,
        ),
    )
    badrows = copy(xbytes)
    xfile = readfile(copy(xbytes))
    block = xfile.recordblocks[1]
    meta = copy(badrows[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    rb = _headertable(meta, msg)
    _write_i64!(meta, _vfield(rb, 0, 8; required=true), Int64(4))
    copyto!(badrows, block[1] + 9, meta, 1, length(meta))
    shifted = Tables.Scan(select=(:x,), offset=5, limit=1)
    @assert _rejects(() -> Tables.scan(readfile(copy(badrows)), shifted))
    @assert _rejects(() -> Tables.scan(SourceFile(BytesSource(copy(badrows))), shifted))
    println("window row counts require top-level FieldNode agreement ✓")

    # Checked buffer-span addition is required before a zero-row window may
    # exclude the body. Without it, these three individually valid counts
    # wrap to the six fixed buffers and make corrupt metadata look exact.
    ovt = ViewType(true)
    ovfields = Field[Field("v$i", ovt) for i = 1:3]
    ovcols = ArrayData[
        ArrayData(ovt, 1, [BufferSlice(), AC._databuffer(zeros(UInt8, 16))]; nullcount=0) for _ = 1:3
    ]
    ovsch = Schema(ovfields)
    ovbytes = writefile(ovsch, [AC.RecordBatch(ovsch, ovcols, 1)])
    ovfile = readfile(copy(ovbytes))
    ovblock = only(ovfile.recordblocks)
    ovmeta = copy(ovbytes[(ovblock[1] + 9):(ovblock[1] + ovblock[2])])
    ovmsg = _vtable(ovmeta, Int64(_vu32(ovmeta, 0)))
    ovrb = _headertable(ovmeta, ovmsg)
    ovstart, ovn = _vvector(ovrb, 4, 8; required=true)
    @assert ovn == 3
    for (i, count) in enumerate(Int64[typemax(Int64) - 2, typemax(Int64) - 2, 6])
        _write_i64!(ovmeta, ovstart + (i - 1) * 8, count)
    end
    copyto!(ovbytes, ovblock[1] + 9, ovmeta, 1, length(ovmeta))
    overflowed = try
        badfile = readfile(copy(ovbytes))
        badfm = _blockmessage(
            badfile.region,
            only(badfile.recordblocks),
            badfile.dataend,
            badfile.limits,
            AllocationBudget(badfile.limits.max_total_allocated_bytes),
        )
        _recordbatchmeta(
            badfm.msg.header::Meta.RecordBatch,
            badfile.fields,
            badfile.limits,
            badfm.body.len,
        )
        false
    catch e
        e isa ValidationError &&
            occursin("record-batch buffer span overflows", sprint(showerror, e))
    end
    @assert overflowed
    @assert _rejects(
        () -> Tables.scan(SourceFile(BytesSource(copy(ovbytes))), Tables.Scan(limit=0)),
    )
    println("overflowing variadic buffer spans reject before window exclusion ✓")

    # A column table cannot infer row count when it has no columns. The scan
    # wrapper keeps the RecordBatch lengths so an empty scan remains identity.
    zerosch = Schema(Field[])
    zerobatches = AC.RecordBatch[
        AC.RecordBatch(zerosch, ArrayData[], 3),
        AC.RecordBatch(zerosch, ArrayData[], 0),
        AC.RecordBatch(zerosch, ArrayData[], 2),
    ]
    zerobytes = writefile(zerosch, zerobatches)
    for source in (readfile(copy(zerobytes)), SourceFile(BytesSource(copy(zerobytes))))
        got = Tables.scan(source, Tables.Scan())
        @assert isempty(Tables.columnnames(Tables.columns(got)))
        @assert Tables.rowcount(Tables.columns(got)) == 5
    end
    println("zero-column scans preserve their row count ✓")

    # A zero-column file can declare an addressable row count without body
    # bytes. The aggregate result must still fit Tables' Int row-count API.
    maxrows = Int64(typemax(Int))
    edgebatches = AC.RecordBatch[
        AC.RecordBatch(zerosch, ArrayData[], maxrows - 1),
        AC.RecordBatch(zerosch, ArrayData[], 1),
    ]
    overflowbatches = AC.RecordBatch[
        AC.RecordBatch(zerosch, ArrayData[], maxrows),
        AC.RecordBatch(zerosch, ArrayData[], 1),
    ]
    sentinelbatches =
        vcat(overflowbatches, AC.RecordBatch[AC.RecordBatch(zerosch, ArrayData[], 1)])
    edgebytes = writefile(zerosch, edgebatches)
    overflowbytes = writefile(zerosch, overflowbatches)
    sentinelbytes = writefile(zerosch, sentinelbatches)
    edgelimits = Limits(max_array_length=typemax(Int64))
    for source in (
        readfile(copy(edgebytes); limits=edgelimits),
        SourceFile(BytesSource(copy(edgebytes)); limits=edgelimits),
    )
        got = Tables.scan(source, Tables.Scan())
        @assert Tables.rowcount(Tables.columns(got)) == typemax(Int)
    end
    for source in (
        readfile(copy(overflowbytes); limits=edgelimits),
        SourceFile(BytesSource(copy(overflowbytes)); limits=edgelimits),
    )
        empty = Tables.scan(source, Tables.Scan(limit=0))
        @assert Tables.rowcount(Tables.columns(empty)) == 0
        capped = Tables.scan(source, Tables.Scan(limit=typemax(Int)))
        @assert Tables.rowcount(Tables.columns(capped)) == typemax(Int)
        shifted = Tables.scan(source, Tables.Scan(offset=1))
        @assert Tables.rowcount(Tables.columns(shifted)) == typemax(Int)
        @assert _rejects(() -> Tables.scan(source, Tables.Scan()))
        @assert _rejects(() -> Tables.scan(source, Tables.Scan(filter=Tables.AlwaysTrue())))
    end
    @assert _rejects(() -> _fulltable(readfile(copy(overflowbytes); limits=edgelimits)))
    for source in (
        readfile(copy(sentinelbytes); limits=edgelimits),
        SourceFile(BytesSource(copy(sentinelbytes)); limits=edgelimits),
    )
        @assert _rejects(() -> Tables.scan(source, Tables.Scan(offset=1)))
    end
    println("unaddressable cumulative row counts fail closed ✓")

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
        Tables.Scan(filter=Tables.colin(Tables.col(:strs), (missing,))),
        Tables.Scan(select=(:strs, :lists), filter=Tables.colcmp(==, Tables.col(3), true)),
    ]
    for scan in scans
        log, src = countingsource(filebytes)
        got = Tables.scan(SourceFile(src), scan)
        want = Tables.scan(full, scan)
        @assert _tables_equal(got, want) sprint(show, scan)
    end
    println("ranged reads are differentially equal to whole-file reads ✓")

    # Byte accounting needs bodies that dwarf metadata: a two-column file
    # where the fat column is ~7× the narrow one. Selecting the narrow
    # column must fetch a small fraction of what the full scan fetches.
    n = 20_000
    fat(i) = string("padding-padding-padding-padding-padding-", i)
    bigbytes = writefile(
        readstream(
            _fixture2x("wide-two-batches") do
                bigio = IOBuffer()
                Arrow.write(
                    bigio,
                    Tables.partitioner([
                        (a=collect(Int64, 1:n), b=[fat(i) for i = 1:n]),
                        (a=collect(Int64, (n + 1):2n), b=[fat(i) for i = (n + 1):2n]),
                    ]);
                    file=false,
                )
                take!(bigio)
            end,
        ),
    )
    logall, srcall = countingsource(bigbytes)
    Tables.scan(SourceFile(srcall; tailbytes=256, coalesce_gap=64), Tables.Scan())
    logone, srcone = countingsource(bigbytes)
    Tables.scan(
        SourceFile(srcone; tailbytes=256, coalesce_gap=64),
        Tables.Scan(select=(:a,)),
    )
    @assert logone.bytes < logall.bytes ÷ 4 (logone.bytes, logall.bytes)
    println(
        "narrow selections fetch a fraction of the bytes " *
        "($(logone.bytes) vs $(logall.bytes) of $(length(bigbytes))) ✓",
    )

    # Skipped-column range proof: corrupt an unselected column's buffer ON THE
    # SOURCE. The scan plans no body range for it; under this fixture's small
    # tail and zero coalescing gap, the request log also excludes that byte.
    off, len = _bufferposition(filebytes, 2, 8)          # strs offsets, batch 2
    corrupt = copy(filebytes)
    corrupt[(off + 5):(off + 8)] .= reinterpret(UInt8, Int32[Int32(2) ^ 30])
    logc, srcc = countingsource(corrupt)
    got = Tables.scan(
        SourceFile(srcc; tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:ints,)),
    )
    @assert isequal(collect(Any, got.ints), collect(Any, full.ints))
    @assert !_fetched(logc, off + 5)
    @assert _rejects(
        () -> Tables.scan(SourceFile(BytesSource(corrupt)), Tables.Scan(select=(:strs,))),
    )
    println(
        "skipped columns add no planned body range " *
        "(fixture request log excludes the corruption) ✓",
    )

    # Window proof: a limit inside batch 1 plans no batch-2 body range. This
    # fixture's request log also excludes sampled batch-2 body bytes.
    block2 = af.recordblocks[2]
    body2 = (block2[1] + block2[2], block2[3])
    logw, srcw = countingsource(filebytes)
    Tables.scan(
        SourceFile(srcw; tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:strs,), limit=5),
    )
    @assert !any(_fetched(logw, body2[1] + k) for k = 0:8:(body2[2] - 1))
    println("window-excluded batches add no planned body range ✓")

    # A dictionary body gets a planned range only when its column is in the
    # decode set. The zero-gap fixture also checks the observed request spans.
    dictblock = let
        # dict block extents via the footer: re-derive from the file bytes
        footerlen = Int64(reinterpret(Int32, filebytes[(end - 9):(end - 6)])[1])
        fb = filebytes[(end - 9 - footerlen):(end - 10)]
        _, _, dblocks, _, _ = verify_footer(fb, Limits())
        @assert length(dblocks) == 1
        dblocks[1]
    end
    dictblockbody = (dictblock[1] + dictblock[2], dictblock[3])
    lognod, srcnod = countingsource(filebytes)
    Tables.scan(
        SourceFile(srcnod; tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:ints,)),
    )
    @assert !any(_fetched(lognod, dictblockbody[1] + k) for k = 0:8:(dictblockbody[2] - 1))
    logd, srcd = countingsource(filebytes)
    Tables.scan(
        SourceFile(srcd; tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:dict,)),
    )
    @assert any(_fetched(logd, dictblockbody[1] + k) for k = 0:8:(dictblockbody[2] - 1))
    logd0, srcd0 = countingsource(filebytes)
    Tables.scan(
        SourceFile(srcd0; tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:dict,), limit=0),
    )
    @assert !any(_fetched(logd0, dictblockbody[1] + k) for k = 0:8:(dictblockbody[2] - 1))
    println("dictionary body ranges are planned only for decode-set ids ✓")

    # A dictionary batch has its own variadic-count cursor. Keep that cursor
    # when the dictionary values use a view layout, including the legal zero
    # count for an all-inline pool. A following plain field pins record-batch
    # alignment after the dictionary is installed.
    scanviewentry(s) = _viewentry(ncodeunits(s), collect(codeunits(s)))
    dvt = ViewType(true)
    dvpool = ArrayData(
        dvt,
        2,
        [BufferSlice(), AC._databuffer(vcat(scanviewentry("a"), scanviewentry("view")))];
        nullcount=0,
    )
    dvtpe = DictionaryType(IntType(32, true), dvt, false)
    dvf = Field("dictview", dvtpe; nullable=false)
    dvd = ArrayData(
        dvtpe,
        3,
        [BufferSlice(), AC._databuffer(Int32[0, 1, 0])];
        dictionary=dvpool,
        nullcount=0,
    )
    dvtailf, dvtaild = fromjulia("tail", Int64[7, 8, 9])
    dvsch = Schema(Field[dvf, dvtailf])
    dvbytes = writefile(dvsch, [AC.RecordBatch(dvsch, ArrayData[dvd, dvtaild], 3)])
    dvgot = Tables.scan(
        SourceFile(BytesSource(copy(dvbytes))),
        Tables.Scan(select=(:dictview, :tail)),
    )
    @assert collect(Any, dvgot.dictview) == Any["a", "view", "a"]
    @assert collect(Any, dvgot.tail) == Any[7, 8, 9]
    println("ranged dictionary views consume their own variadic counts ✓")

    # A selected dictionary id missing from the Footer is a metadata-only
    # refusal. It must fail before any dedicated record-body request.
    missingdict = copy(filebytes)
    footerlen = Int64(reinterpret(Int32, missingdict[(end - 9):(end - 6)])[1])
    footerstart = Int64(length(missingdict)) - 10 - footerlen
    footerbytes = copy(missingdict[(footerstart + 1):(footerstart + footerlen)])
    footertable = _vtable(footerbytes, Int64(_vu32(footerbytes, 0)))
    _write_u32!(footerbytes, _vref(footertable, 2; required=true), UInt32(0))
    copyto!(missingdict, footerstart + 1, footerbytes, 1, length(footerbytes))
    missingrecords = verify_footer(footerbytes, Limits())[4]
    missingscan = Tables.Scan(select=(:dict,))
    @assert _rejects(() -> Tables.scan(readfile(copy(missingdict)), missingscan))
    logmissing, srcmissing = countingsource(missingdict)
    @assert _rejects(
        () ->
            Tables.scan(SourceFile(srcmissing; tailbytes=32, coalesce_gap=0), missingscan),
    )
    @assert !any(_fetched(logmissing, block[1] + block[2]) for block in missingrecords)
    println("missing dictionary plans reject before dedicated record-body requests ✓")

    # Coalescing: an infinite gap merges every body range into one request;
    # a zero gap issues more, smaller requests; both agree with the truth.
    # (A small tail window, so the ranges are planned rather than served
    # from the cached tail.)
    logbig, srcbig = countingsource(filebytes)
    gotbig = Tables.scan(
        SourceFile(srcbig; tailbytes=256, coalesce_gap=typemax(Int32)),
        Tables.Scan(select=(:ints, :strs)),
    )
    logzero, srczero = countingsource(filebytes)
    gotzero = Tables.scan(
        SourceFile(srczero; tailbytes=256, coalesce_gap=0),
        Tables.Scan(select=(:ints, :strs)),
    )
    want = Tables.scan(full, Tables.Scan(select=(:ints, :strs)))
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
    println(
        "coalescing trades requests for bytes without changing results " *
        "($(logbig.requests) reqs/$(logbig.bytes)B vs $(logzero.requests) reqs/$(logzero.bytes)B) ✓",
    )

    # A tail smaller than the footer forces the exact follow-up fetch.
    logt, srct = countingsource(filebytes)
    gott = Tables.scan(SourceFile(srct; tailbytes=32), Tables.Scan(select=(:ints,)))
    @assert isequal(collect(Any, gott.ints), collect(Any, full.ints))
    println("undersized tails recover with one exact footer fetch ✓")

    # Compressed files range-read identically (per-buffer frames are
    # self-contained behind their prefixes).
    zsource = readstream(
        _fixture2x("int64-strings-two-batches") do
            io = IOBuffer()
            Arrow.write(
                io,
                Tables.partitioner([
                    (x=Int64[1, 2, 3], s=["a", "bb", "ccc"]),
                    (x=Int64[4, 5, 6], s=["dd", "e", "ff"]),
                ]);
                file=false,
            )
            take!(io)
        end,
    )
    zbytes = writefile(zsource; compress=:zstd)
    zfull = _fulltable(readfile(copy(zbytes)))
    logz, srcz = countingsource(zbytes)
    gotz = Tables.scan(
        SourceFile(srcz; tailbytes=256, coalesce_gap=64),
        Tables.Scan(select=(:x,)),
    )
    @assert isequal(collect(Any, gotz.x), collect(Any, zfull.x))
    @assert logz.bytes < length(zbytes)
    println("compressed files range-read through self-contained buffers ✓")

    # Every failure derivable from the selected metadata plan precedes its
    # first body request. Skipped columns and window-excluded batches keep
    # their intentional lazy boundary.
    block1 = af.recordblocks[1]
    badfixed = _setbufferlength!(copy(filebytes), block1, 2, Int64(1))
    fixedoff, _ = _bufferposition(filebytes, 1, 2)
    @assert _rejects(
        () -> Tables.scan(readfile(copy(badfixed)), Tables.Scan(select=(:ints,))),
    )
    logfixed, srcfixed = countingsource(badfixed)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(srcfixed; tailbytes=32, coalesce_gap=0),
            Tables.Scan(select=(:ints,)),
        ),
    )
    @assert !_fetched(logfixed, fixedoff)
    skipped = Tables.scan(
        SourceFile(BytesSource(copy(badfixed)); tailbytes=32, coalesce_gap=0),
        Tables.Scan(select=(:floats,)),
    )
    @assert isequal(collect(Any, skipped.floats), collect(Any, full.floats))

    validbytes = writefile(
        readstream(_fixture2x("nullable-int64-sixteen") do
            validio = IOBuffer()
            validdata = Union{Missing,Int64}[missing; collect(Int64, 2:16)]
            Arrow.write(validio, (x=validdata,); file=false)
            take!(validio)
        end),
    )
    validfile = readfile(copy(validbytes))
    badvalid = _setbufferlength!(copy(validbytes), validfile.recordblocks[1], 1, Int64(1))
    validpos, _ = _bufferposition(validbytes, 1, 1)
    logvalid, srcvalid = countingsource(badvalid)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(srcvalid; tailbytes=32, coalesce_gap=0),
            Tables.Scan(select=(:x,)),
        ),
    )
    @assert !_fetched(logvalid, validpos)
    validbudget = AllocationBudget(validfile.limits.max_total_allocated_bytes)
    validmsg = _blockmessage(
        validfile.region,
        validfile.recordblocks[1],
        validfile.dataend,
        validfile.limits,
        validbudget,
    )
    validfield = validfile.fields[1]
    strictfield = Field(
        validfield.name,
        validfield.type,
        false,
        validfield.metadata,
        validfield.children,
    )
    validheader = validmsg.msg.header::Meta.RecordBatch
    validcodec = _batchcodec(validheader.compression, validmsg.version)
    # Field.nullable is advisory: the planned path accepts the strict
    # declaration over data with nulls, exactly as the whole-file path does
    # (validate_full is where the declaration is enforced).
    @assert _validatebodyplan(
        validheader,
        (strictfield,),
        validfile.limits,
        validcodec,
        Bool[true],
    ) === nothing

    structbytes = writefile(
        readstream(
            _fixture2x("nullable-struct-child") do
                structio = IOBuffer()
                structdata = NamedTuple{(:n,),Tuple{Union{Missing,Int64}}}[
                    (n=missing,),
                    (n=Int64(2),),
                ]
                Arrow.write(structio, (x=structdata,); file=false)
                take!(structio)
            end,
        ),
    )
    structfile = readfile(copy(structbytes))
    structbudget = AllocationBudget(structfile.limits.max_total_allocated_bytes)
    structmsg = _blockmessage(
        structfile.region,
        structfile.recordblocks[1],
        structfile.dataend,
        structfile.limits,
        structbudget,
    )
    parentfield = structfile.fields[1]
    childfield = parentfield.children[1]
    strictchild = Field(
        childfield.name,
        childfield.type,
        false,
        childfield.metadata,
        childfield.children,
    )
    strictparent = Field(
        parentfield.name,
        parentfield.type,
        parentfield.nullable,
        parentfield.metadata,
        [strictchild],
    )
    structheader = structmsg.msg.header::Meta.RecordBatch
    structcodec = _batchcodec(structheader.compression, structmsg.version)
    @assert _validatebodyplan(
        structheader,
        (strictparent,),
        structfile.limits,
        structcodec,
        Bool[true],
    ) === nothing

    emptylistbytes = writefile(readstream(_fixture2x("empty-string-list") do
        emptylistio = IOBuffer()
        Arrow.write(emptylistio, (x=[String[]],); file=false)
        take!(emptylistio)
    end))
    emptylistfile = readfile(copy(emptylistbytes))
    emptylistblock = emptylistfile.recordblocks[1]
    bademptyoffset = _setbufferlength!(copy(emptylistbytes), emptylistblock, 4, Int64(0))
    parentoffsetpos, _ = _bufferposition(emptylistbytes, 1, 2)
    logemptyoffset, srcemptyoffset = countingsource(bademptyoffset)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(srcemptyoffset; tailbytes=32, coalesce_gap=0),
            Tables.Scan(select=(:x,)),
        ),
    )
    @assert !_fetched(logemptyoffset, parentoffsetpos)

    badoffsets = _setbufferlength!(copy(filebytes), block1, 8, Int64(4))
    offsetpos, _ = _bufferposition(filebytes, 1, 8)
    logoffsets, srcoffsets = countingsource(badoffsets)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(srcoffsets; tailbytes=32, coalesce_gap=0),
            Tables.Scan(select=(:strs,)),
        ),
    )
    @assert !_fetched(logoffsets, offsetpos)

    badstruct = _setnodelength!(copy(filebytes), block1, 8, Int64(4))
    structpos, _ = _bufferposition(filebytes, 1, 16)
    logstruct, srcstruct = countingsource(badstruct)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(srcstruct; tailbytes=32, coalesce_gap=0),
            Tables.Scan(select=(:structs,)),
        ),
    )
    @assert !_fetched(logstruct, structpos)

    nullfield = Field("n", NullType())
    sparsetype = UnionType(AC.SparseMode, Int8[0])
    sparsefield = Field("u", sparsetype; children=[nullfield])
    nulldata = ArrayData(NullType(), 1, BufferSlice[]; nullcount=1)
    sparsedata = ArrayData(
        sparsetype,
        1,
        [AC._databuffer(Int8[0])];
        children=[nulldata],
        nullcount=0,
    )
    sparseschema = Schema([sparsefield])
    sparsebytes = writefile(sparseschema, [AC.RecordBatch(sparseschema, [sparsedata], 1)])
    sparsefile = readfile(copy(sparsebytes))
    sparseblock = sparsefile.recordblocks[1]
    sparsepos, _ = _bufferposition(sparsebytes, 1, 1)
    sparsefailures = (
        _setnodenullcount!(
            _setnodelength!(copy(sparsebytes), sparseblock, 2, Int64(2)),
            sparseblock,
            2,
            Int64(2),
        ),
        _setnodenullcount!(copy(sparsebytes), sparseblock, 1, Int64(1)),
        _setnodenullcount!(copy(sparsebytes), sparseblock, 2, Int64(0)),
    )
    for broken in sparsefailures
        logsparse, srcsparse = countingsource(broken)
        @assert _rejects(
            () -> Tables.scan(
                SourceFile(srcsparse; tailbytes=32, coalesce_gap=0),
                Tables.Scan(select=(:u,)),
            ),
        )
        @assert !_fetched(logsparse, sparsepos)
    end

    zfile = readfile(copy(zbytes))
    zblock = zfile.recordblocks[1]
    compressedpos, _ = _bufferposition(zbytes, 1, 2)
    for badlen in (Int64(1), Int64(8))
        badcompressed = _setbufferlength!(copy(zbytes), zblock, 2, badlen)
        logcompressed, srccompressed = countingsource(badcompressed)
        @assert _rejects(
            () -> Tables.scan(
                SourceFile(srccompressed; tailbytes=32, coalesce_gap=0),
                Tables.Scan(select=(:x,)),
            ),
        )
        @assert !_fetched(logcompressed, compressedpos)
    end

    baddict = _setbufferlength!(copy(filebytes), dictblock, 2, Int64(1))
    logbaddict, srcbaddict = countingsource(baddict)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(srcbaddict; tailbytes=32, coalesce_gap=0),
            Tables.Scan(select=(:dict,)),
        ),
    )
    @assert !any(
        _fetched(logbaddict, dictblockbody[1] + k) for k = 0:8:(dictblockbody[2] - 1)
    )
    skippeddict = Tables.scan(
        SourceFile(BytesSource(copy(baddict)); tailbytes=32, coalesce_gap=0),
        Tables.Scan(select=(:ints,)),
    )
    @assert isequal(collect(Any, skippeddict.ints), collect(Any, full.ints))

    badwindow = _setbufferlength!(copy(filebytes), af.recordblocks[2], 2, Int64(1))
    windowpos, _ = _bufferposition(filebytes, 2, 2)
    logwindow, srcwindow = countingsource(badwindow)
    windowed = Tables.scan(
        SourceFile(srcwindow; tailbytes=32, coalesce_gap=0),
        Tables.Scan(select=(:ints,), limit=5),
    )
    @assert isequal(collect(Any, windowed.ints), collect(Any, full.ints[1:5]))
    @assert !_fetched(logwindow, windowpos)
    println("planned metadata failures reject before dedicated body requests ✓")

    # Legacy V4 message-level compression is rejected from metadata even when
    # limit=0 leaves no body to decode.
    legacyv4, legacyblock = _legacyv4file()
    legacyscan = Tables.Scan(select=(:x,), limit=0)
    @assert _rejects(() -> Tables.scan(readfile(copy(legacyv4)), legacyscan))
    loglegacy, srclegacy = countingsource(legacyv4)
    @assert _rejects(
        () -> Tables.scan(SourceFile(srclegacy; tailbytes=32, coalesce_gap=0), legacyscan),
    )
    @assert !_fetched(loglegacy, legacyblock[1] + legacyblock[2])
    println("legacy compression rejects before dedicated record-body requests ✓")

    # Hostile inputs fail closed: forged footer length, overlapping Blocks,
    # out-of-body zero-length buffers, and truncated objects.
    badlen = copy(filebytes)
    lenpos = length(badlen) - 9
    badlen[lenpos:(lenpos + 3)] .= reinterpret(UInt8, Int32[Int32(2) ^ 30])
    @assert _rejects(() -> Tables.scan(SourceFile(BytesSource(badlen)), Tables.Scan()))
    @assert _rejects(
        () -> Tables.scan(SourceFile(BytesSource(filebytes[1:20])), Tables.Scan()),
    )

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
    @assert _rejects(() -> Tables.scan(SourceFile(BytesSource(overlap)), Tables.Scan()))

    zerobuffer = copy(filebytes)
    block = af.recordblocks[1]
    meta = copy(zerobuffer[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    rb = _headertable(meta, msg)
    bufferstart, _ = _vvector(rb, 2, 16; required=true)
    _write_i64!(meta, bufferstart, block[3] + 8)
    copyto!(zerobuffer, block[1] + 9, meta, 1, length(meta))
    @assert _rejects(() -> readfile(copy(zerobuffer)))
    @assert _rejects(
        () ->
            Tables.scan(SourceFile(BytesSource(zerobuffer)), Tables.Scan(select=(:ints,))),
    )
    println("forged footers and truncated objects fail closed ✓")

    # Ranged limits are checked before dedicated body requests. One whole-file
    # Scan also keeps one aggregate budget across every batch it decompresses.
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(BytesSource(filebytes); limits=Limits(max_body_bytes=32)),
            Tables.Scan(select=(:ints,)),
        ),
    )
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(BytesSource(filebytes); limits=Limits(max_messages=1)),
            Tables.Scan(),
        ),
    )
    loglimit, srclimit = countingsource(filebytes)
    intoff, _ = _bufferposition(filebytes, 1, 2)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(
                srclimit;
                limits=Limits(max_buffer_bytes=8),
                tailbytes=256,
                coalesce_gap=0,
            ),
            Tables.Scan(select=(:ints,)),
        ),
    )
    @assert !_fetched(loglimit, intoff)

    largebytes = writefile(
        readstream(
            _fixture2x("large-zeros-two-partitions") do
                large = (x=zeros(Int64, 10_000),)
                largeio = IOBuffer()
                Arrow.write(largeio, Tables.partitioner([large, large]); file=false)
                take!(largeio)
            end,
        );
        compress=:zstd,
    )
    tight = Limits(max_total_allocated_bytes=100_000)
    @assert _rejects(
        () -> Tables.scan(
            readfile(copy(largebytes); limits=tight),
            Tables.Scan(select=(:x,)),
        ),
    )
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(BytesSource(largebytes); limits=tight),
            Tables.Scan(select=(:x,)),
        ),
    )
    println("range limits and scan-wide allocation budgets fail before overuse ✓")

    println()
    println("Byte-range scan checks passed.")
end

@noinline function _stats_base_fixture()
    # Two batches with DISJOINT ranges so predicates can discriminate:
    # batch 1: x ∈ 1:5, s ∈ "apple".."eagle";  batch 2: x ∈ 6:10, s ∈ "fig".."jam".
    t1 = (x=Int64[1, 2, 3, 4, 5], s=["apple", "berry", "cedar", "date", "eagle"])
    t2 = (x=Int64[6, 7, 8, 9, 10], s=["fig", "grape", "hazel", "iris", "jam"])
    source = readstream(_fixture2x("stats-two-batches") do
        io = IOBuffer()
        Arrow.write(io, Tables.partitioner([t1, t2]); file=false)
        take!(io)
    end)
    sbytes = statsfile(source.schema, source.batches)
    saf = readfile(copy(sbytes))
    sfull = _fulltable(saf)

    # The statistics blob is itself a valid stream this reader accepts.
    stats = _readstats(saf.schema.metadata, 2, saf.fields)
    @assert stats !== nothing
    @assert stats[1].rows == 5 && stats[2].rows == 5
    @assert stats[1].cols[1].min == 1 && stats[1].cols[1].max == 5
    @assert stats[2].cols[2].min == "fig" && stats[2].cols[2].max == "jam"
    # Statistics ride in schema metadata, so readers that do not know them
    # are unaffected — the oracle proves pyarrow reads stats-carrying files.
    println("statistics round-trip the official value layout ✓")

    return source, sbytes, saf, sfull
end

@noinline function _stats_fieldnode_check(source)
    # Official column references use the flattened RecordBatch FieldNode
    # order. A top-level field after a nested subtree is not its top-level
    # ordinal.
    nestedfields = Field[
        Field(
            "st",
            StructType();
            children=Field[Field("a", IntType(64, true)), Field("b", IntType(64, true))],
        ),
        Field("x", IntType(64, true)),
    ]
    nestedsch = Schema(nestedfields)
    ints(v) = ArrayData(
        IntType(64, true),
        length(v),
        [BufferSlice(), AC._databuffer(Int64.(v))];
        nullcount=0,
    )
    structdata = ArrayData(
        StructType(),
        2,
        [BufferSlice()];
        children=[ints([1, 2]), ints([3, 4])],
        nullcount=0,
    )
    nestedbatch = AC.RecordBatch(nestedsch, [structdata, ints([5, 6])], 2)
    nestedstats = withstatistics(nestedsch, [nestedbatch])
    nestedstream = readstream(Base64.base64decode(Dict(nestedstats.metadata)[STATS_KEY]))
    refs = materialize(nestedstream.schema.fields[1], nestedstream.batches[1].columns[1])
    @assert isequal(collect(Any, refs), Any[missing, Int32(0), Int32(3)])
    println("statistics use official flattened FieldNode column indexes ✓")

    return nestedstats
end

@noinline function _stats_predicate_checks(sbytes, saf, sfull)
    # Differential correctness with pruning active, whole-file and ranged.
    prunescans = Tables.Scan[
        Tables.Scan(filter=Tables.col(:x) > 7),
        Tables.Scan(select=(:s,), filter=Tables.col(:x) <= 3),
        Tables.Scan(filter=Tables.col(:x) > 100),
        Tables.Scan(filter=Tables.colin(Tables.col(:x), (2, 4))),
        Tables.Scan(filter=Tables.isnull(Tables.col(:x))),
        Tables.Scan(filter=Tables.startswith(Tables.col(:s), "i")),
        Tables.Scan(filter=(Tables.col(:x) > 2) & (Tables.col(:x) < 9)),
        Tables.Scan(filter=Tables.colcmp(!=, Tables.col(:x), 3)),
    ]
    for scan in prunescans
        want = Tables.scan(sfull, scan)
        @assert _tables_equal(Tables.scan(saf, scan), want) sprint(show, scan)
        @assert _tables_equal(
            Tables.scan(SourceFile(BytesSource(copy(sbytes))), scan),
            want,
        ) sprint(show, scan)
    end
    println("pruned scans stay differentially exact (whole-file + ranged) ✓")

    # Float pruning must use the same IEEE operators as Tables.scan.
    fsource = readstream(
        _fixture2x("float-zero-signs-nan") do
            fio = IOBuffer()
            Arrow.write(
                fio,
                Tables.partitioner([
                    (x=Float64[0.0, 0.0],),
                    (x=Float64[-0.0, -0.0],),
                    (x=Float64[NaN, NaN],),
                ]);
                file=false,
            )
            take!(fio)
        end,
    )
    fbytes = statsfile(fsource.schema, fsource.batches)
    faf = readfile(copy(fbytes))
    ffull = _fulltable(faf)
    floatscans = Tables.Scan[
        Tables.Scan(filter=Tables.colcmp(==, Tables.col(:x), -0.0)),
        Tables.Scan(filter=Tables.col(:x) <= -0.0),
        Tables.Scan(filter=Tables.col(:x) >= 0.0),
        Tables.Scan(filter=Tables.colin(Tables.col(:x), (-0.0,))),
        Tables.Scan(filter=Tables.colcmp(!=, Tables.col(:x), NaN)),
        # OP_NE pruning: a constant batch equal to the literal is the ONLY
        # provably prunable case; mixed batches and NaN stats must fetch.
        Tables.Scan(filter=Tables.colcmp(!=, Tables.col(:x), 0.0)),
        Tables.Scan(filter=Tables.colcmp(!=, Tables.col(:x), -0.0)),
    ]
    for scan in floatscans
        want = Tables.scan(ffull, scan)
        @assert _tables_equal(Tables.scan(faf, scan), want)
        @assert _tables_equal(Tables.scan(SourceFile(BytesSource(fbytes)), scan), want)
    end
    println("float pruning preserves signed-zero and NaN predicate semantics ✓")

    # Dictionary nullness is logical: a valid outer index can resolve to a
    # null pool value and must count as null without entering min/max folds.
    pool = ArrayData(
        Utf8Type(false),
        1,
        [AC._databuffer(UInt8[0x00]), AC._databuffer(Int32[0, 0]), BufferSlice()];
        nullcount=1,
    )
    dtype = DictionaryType(IntType(32, true), Utf8Type(false), false)
    dfield = Field("d", dtype)
    ddata = ArrayData(
        dtype,
        1,
        [BufferSlice(), AC._databuffer(Int32[0])];
        dictionary=pool,
        nullcount=0,
    )
    @assert _statfold(dfield, ddata) == (1, nothing, nothing)
    println("dictionary statistics count null pool values logically ✓")

    # Wrapper unwrapping is recursive: nested REE values may themselves use
    # a view layout. Logical null counts repeat the null value for every slot
    # in its outer run, while supported bounds keep their String domain.
    nvt = ViewType(true)
    nviews = vcat(
        reinterpret(UInt8, Int32[Int32(1)]),
        UInt8[0x70],
        zeros(UInt8, 11),
        zeros(UInt8, 16),
    )
    nvf = Field("values", nvt; nullable=true)
    nvd = ArrayData(
        nvt,
        2,
        [AC._databuffer(UInt8[0x01]), AC._databuffer(nviews)];
        nullcount=1,
    )
    nirf, nird = fromjulia("run_ends", Int32[1, 2])
    nif = Field("values", RunEndEncodedType(); children=[nirf, nvf])
    nid =
        ArrayData(RunEndEncodedType(), 2, BufferSlice[]; children=[nird, nvd], nullcount=0)
    norf, nord = fromjulia("run_ends", Int32[2, 4])
    nf = Field("nested", RunEndEncodedType(); children=[norf, nif])
    nd = ArrayData(RunEndEncodedType(), 4, BufferSlice[]; children=[nord, nid], nullcount=0)
    @assert _statfold(nf, nd) == (2, "p", "p")
    println("nested REE/view statistics fold logical nulls and String bounds ✓")

    # Request-plan proof: x > 7 prunes batch 1, so its block metadata and body
    # add no dedicated ranges. This fixture's request log also excludes its
    # indexed bytes.
    block1 = saf.recordblocks[1]
    logp, srcp = countingsource(sbytes)
    got = Tables.scan(
        SourceFile(srcp; tailbytes=256, coalesce_gap=0),
        Tables.Scan(filter=Tables.col(:x) > 7),
    )
    @assert isequal(collect(Any, got.x), Any[8, 9, 10])
    @assert !any(_fetched(logp, block1[1] + k) for k = 0:8:(block1[2] + block1[3] - 1))
    println("stat-pruned batches add no dedicated metadata/body range ✓")
    return nothing
end

@noinline function _stats_limit_and_decode_checks(source, sbytes)
    # Per-record limits stay lazy on both paths. A statistics-pruned large
    # record is accepted; a surviving one rejects before its ranged metadata
    # or body is fetched.
    limitsource = readstream(_fixture2x("int64-ten-thousand") do
        limitio = IOBuffer()
        Arrow.write(limitio, (x=collect(Int64, 1:10_000),); file=false)
        take!(limitio)
    end)
    limitbytes = statsfile(limitsource.schema, limitsource.batches)
    limitfooterlen = Int64(reinterpret(Int32, limitbytes[(end - 9):(end - 6)])[1])
    limitfooterstart = Int64(length(limitbytes)) - 10 - limitfooterlen
    limitfooter =
        copy(limitbytes[(limitfooterstart + 1):(limitfooterstart + limitfooterlen)])
    limitblock = only(verify_footer(limitfooter, Limits())[4])
    lazylimits = Limits(max_body_bytes=4096)
    @assert limitblock[3] > lazylimits.max_body_bytes
    prunedscan = Tables.Scan(filter=Tables.col(:x) < 0)
    @assert isempty(
        Tables.scan(readfile(copy(limitbytes); limits=lazylimits), prunedscan).x,
    )
    logpruned, srcpruned = countingsource(limitbytes)
    @assert isempty(
        Tables.scan(
            SourceFile(srcpruned; limits=lazylimits, tailbytes=32, coalesce_gap=0),
            prunedscan,
        ).x,
    )
    @assert !_fetched(logpruned, limitblock[1])
    keptscan = Tables.Scan(filter=Tables.col(:x) > 0)
    @assert _rejects(
        () -> Tables.scan(readfile(copy(limitbytes); limits=lazylimits), keptscan),
    )
    logkept, srckept = countingsource(limitbytes)
    @assert _rejects(
        () -> Tables.scan(
            SourceFile(srckept; limits=lazylimits, tailbytes=32, coalesce_gap=0),
            keptscan,
        ),
    )
    @assert !_fetched(logkept, limitblock[1])
    println("whole and ranged record limits have the same lazy boundary ✓")

    # Decode proof (whole-file): semantic corruption inside a pruned batch
    # stays invisible with statistics, and is caught without them.
    soff, slen = _bufferposition(sbytes, 1, 4)          # batch 1 `s` offsets
    @assert slen > 8
    scorrupt = copy(sbytes)
    scorrupt[(soff + 5):(soff + 8)] .= reinterpret(UInt8, Int32[Int32(2) ^ 30])
    scanx = Tables.Scan(select=(:s,), filter=Tables.col(:x) > 7)
    got = Tables.scan(readfile(copy(scorrupt)), scanx)
    @assert isequal(collect(Any, got.s), Any["hazel", "iris", "jam"])
    plainbytes = writefile(source.schema, source.batches)
    pcorrupt = copy(plainbytes)
    poff, _ = _bufferposition(plainbytes, 1, 4)
    pcorrupt[(poff + 5):(poff + 8)] .= reinterpret(UInt8, Int32[Int32(2) ^ 30])
    @assert _rejects(() -> Tables.scan(readfile(copy(pcorrupt)), scanx))
    println("pruning skips decode; without statistics the same scan must decode ✓")
    return nothing
end

@noinline function _stats_malformed_checks(source, saf, nestedstats)
    # Malformed statistics degrade to no pruning, never to an error.
    badmeta = Dict{String,String}(STATS_KEY => "!!not-base64!!")
    badsch = Schema(
        collect(Field, source.schema.fields);
        metadata=badmeta,
        endianness=source.schema.endianness,
    )
    badbytes = writefile(badsch, source.batches)
    for sourcefile in (readfile(copy(badbytes)), SourceFile(BytesSource(badbytes)))
        got = Tables.scan(sourcefile, Tables.Scan(filter=Tables.col(:x) > 7))
        @assert isequal(collect(Any, got.x), Any[8, 9, 10])
    end
    @assert _readstats(nestedstats.metadata, 2, source.schema.fields) === nothing
    wrongblob = Base64.base64encode(
        _fixture2x("stats-wrong-schema") do
            wrongio = IOBuffer()
            Arrow.write(
                wrongio,
                Tables.partitioner([(q=Int64[1],), (q=Int64[2],)]);
                file=false,
            )
            take!(wrongio)
        end,
    )
    wrongsch = Schema(
        collect(Field, source.schema.fields);
        metadata=Dict{String,String}(STATS_KEY => wrongblob),
        endianness=source.schema.endianness,
    )
    wrongbytes = writefile(wrongsch, source.batches)
    for sourcefile in (readfile(copy(wrongbytes)), SourceFile(BytesSource(wrongbytes)))
        got = Tables.scan(sourcefile, Tables.Scan(filter=Tables.col(:x) > 7))
        @assert isequal(collect(Any, got.x), Any[8, 9, 10])
    end

    # A two-field stream is not enough: the canonical physical skeleton is
    # part of the official value-layout contract.
    rawstats = readstream(Base64.base64decode(Dict(saf.schema.metadata)[STATS_KEY]))
    boolsch =
        Schema(Field[Field("column", BoolType(); nullable=true), rawstats.schema.fields[2]])
    boolbatches = AC.RecordBatch[]
    for sb in rawstats.batches
        valid = trues(sb.nrows)
        valid[1] = false
        boolcol = ArrayData(
            BoolType(),
            sb.nrows,
            [
                AC._databuffer(_bitmapbytes(valid)),
                AC._databuffer(_bitmapbytes(trues(sb.nrows))),
            ];
            nullcount=1,
        )
        push!(
            boolbatches,
            AC.RecordBatch(boolsch, ArrayData[boolcol, sb.columns[2]], sb.nrows),
        )
    end
    boolblob = Base64.base64encode(writestream(boolsch, boolbatches))
    @assert _readstats(Dict(STATS_KEY => boolblob), 2, source.schema.fields) === nothing

    statssch = _statsschema()
    hugevalue = repeat("x", 2_000_000)
    hugebatches = AC.RecordBatch[_statsbatch(
        statssch,
        Int64(1),
        Tuple{Int64,Int64,Any,Any}[(1, Int64(0), hugevalue, hugevalue)],
    )]
    hugeblob = Base64.base64encode(writestream(statssch, hugebatches; compress=:zstd))
    bombsource = readstream(_fixture2x("single-string") do
        bombio = IOBuffer()
        Arrow.write(bombio, (s=["x"],); file=false)
        take!(bombio)
    end)
    hugesch = Schema(
        collect(Field, bombsource.schema.fields);
        metadata=Dict{String,String}(STATS_KEY => hugeblob),
        endianness=bombsource.schema.endianness,
    )
    hugebytes = writefile(hugesch, bombsource.batches)
    for cap in (Int64(50_000), Int64(100_000))
        tight = Limits(max_total_allocated_bytes=cap)
        for sourcefile in (
            readfile(copy(hugebytes); limits=tight),
            SourceFile(BytesSource(hugebytes); limits=tight),
        )
            rejected = try
                Tables.scan(
                    sourcefile,
                    Tables.Scan(filter=Tables.colcmp(==, Tables.col(:s), "x")),
                )
                false
            catch e
                e isa AllocationLimitError
            end
            @assert rejected
        end
    end
    println("malformed statistics degrade; allocation exhaustion propagates ✓")
    return nothing
end

@noinline function _stats_trust_checks(source)
    # The trust model, pinned (design §3): wide lies only cost pruning;
    # narrow lies silently LOSE rows — statistics are trusted-for-
    # completeness, exactly like Parquet row-group stats.
    function liarfile(lo2, hi2)
        statssch = _statsschema()
        lie = AC.RecordBatch[
            _statsbatch(
                statssch,
                Int64(5),
                [(1, Int64(0), Int64(1), Int64(5)), (2, Int64(0), "apple", "eagle")],
            ),
            _statsbatch(
                statssch,
                Int64(5),
                [(1, Int64(0), lo2, hi2), (2, Int64(0), "fig", "jam")],
            ),
        ]
        blob = Base64.base64encode(writestream(statssch, lie))
        liesch = Schema(
            collect(Field, source.schema.fields);
            metadata=Dict{String,String}(STATS_KEY => blob),
            endianness=source.schema.endianness,
        )
        return writefile(liesch, source.batches)
    end
    wides = liarfile(Int64(-1000), Int64(1000))
    narrows = liarfile(Int64(6), Int64(7))
    trustscan = Tables.Scan(filter=Tables.col(:x) > 8)
    for sourcefile in (readfile(copy(wides)), SourceFile(BytesSource(wides)))
        wide = Tables.scan(sourcefile, trustscan)
        @assert isequal(collect(Any, wide.x), Any[9, 10])
    end
    for sourcefile in (readfile(copy(narrows)), SourceFile(BytesSource(narrows)))
        narrow = Tables.scan(sourcefile, trustscan)
        @assert isempty(narrow.x)      # rows 9, 10 silently lost: the trust boundary
    end
    println("wide lies cost pruning only; narrow lies lose rows (trust model pinned) ✓")

    return nothing
end

function _stats_main()
    source, sbytes, saf, sfull = _stats_base_fixture()
    nestedstats = _stats_fieldnode_check(source)
    _stats_predicate_checks(sbytes, saf, sfull)
    _stats_limit_and_decode_checks(source, sbytes)
    _stats_malformed_checks(source, saf, nestedstats)
    _stats_trust_checks(source)
    println()
    println("Statistics write/prune checks passed.")
    return nothing
end
