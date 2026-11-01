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

struct _ScanOverrideValue
    value::Int64
end
Base.convert(::Type{_ScanOverrideValue}, x::Integer) = _ScanOverrideValue(Int64(x))

struct _ScanDomain
    value::Int64
end
Base.in(x::Integer, d::_ScanDomain) = x == d.value

struct _DateScanDomain
    value::Arrow.Dates.Date
end
Base.in(x::Arrow.Dates.Date, d::_DateScanDomain) = x == d.value

mutable struct _ScanOverrideVector{T} <: AbstractVector{T}
    values::Vector{T}
end
_ScanOverrideVector{T}(::UndefInitializer, n::Integer) where {T} =
    _ScanOverrideVector{T}(Vector{T}(undef, n))
Base.size(v::_ScanOverrideVector) = size(v.values)
Base.getindex(v::_ScanOverrideVector, i::Int) = v.values[i]
Base.setindex!(v::_ScanOverrideVector, x, i::Int) = (v.values[i] = x)
Arrow.DataAPI.defaultarray(::Type{_ScanOverrideValue}, _) =
    _ScanOverrideVector{_ScanOverrideValue}

function _rangeintersects(a::NTuple{2,Int64}, b::NTuple{2,Int64})
    a[2] == 0 && return false
    b[2] == 0 && return false
    return a[1] < b[1] + b[2] && b[1] < a[1] + a[2]
end

_requestintersects(log::FetchLog, span::NTuple{2,Int64}) =
    any(request -> _rangeintersects(request, span), log.ranges)

function _footerblocks(bytes::Vector{UInt8})
    footerlen = Int64(reinterpret(Int32, bytes[(end - 9):(end - 6)])[1])
    footerbytes = bytes[(end - 9 - footerlen):(end - 10)]
    _, _, dictionaries, records, _ = verify_footer(footerbytes, Limits())
    return dictionaries, records
end

_bodyrange(block::NTuple{3,Int64}) = (block[1] + block[2], block[3])

function _recordbufferranges(bytes::Vector{UInt8}, i::Int)
    file = readfile(copy(bytes))
    block = file.recordblocks[i]
    budget = AllocationBudget(file.limits.max_total_allocated_bytes)
    fm = _blockmessage(heapregion(copy(bytes)), block, file.dataend, file.limits, budget)
    header = fm.msg.header::Meta.RecordBatch
    bodystart = block[1] + block[2]
    return NTuple{2,Int64}[
        (bodystart + Int64(buffer.offset), Int64(buffer.length)) for
        buffer in header.buffers
    ]
end

function _tables_values_equal(a, b)
    ca, cb = Tables.columns(a), Tables.columns(b)
    Tables.rowcount(ca) == Tables.rowcount(cb) || return false
    na, nb = Tables.columnnames(ca), Tables.columnnames(cb)
    collect(na) == collect(nb) || return false
    for n in na
        cola = Tables.getcolumn(ca, n)
        colb = Tables.getcolumn(cb, n)
        isequal(collect(Any, cola), collect(Any, colb)) || return false
    end
    return true
end

function _tables_equal(a, b)
    _tables_values_equal(a, b) || return false
    ca, cb = Tables.columns(a), Tables.columns(b)
    _schemas_equal(Tables.schema(ca), Tables.schema(cb)) || return false
    return all(
        eltype(Tables.getcolumn(ca, n)) == eltype(Tables.getcolumn(cb, n)) for
        n in Tables.columnnames(ca)
    )
end

# The generic executor reports no schema for a result with no columns.
_schemacolumns(s) =
    s === nothing ? (Symbol[], Type[]) : (collect(s.names), collect(s.types))
_schemas_equal(a, b) = _schemacolumns(a) == _schemacolumns(b)

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
    @assert !_tables_equal((x=Any[1],), (x=Int64[1],))
    source = readstream(_mixed_two_partitions_bytes())
    filebytes = writefile(source)
    full = Arrow.Table(copy(filebytes))
    sources(bytes) = (copy(bytes), SourceFile(BytesSource(copy(bytes)); tailbytes=32))

    scans = Tables.Scan[
        Tables.Scan(),
        Tables.Scan(select=(:ints, :strs)),
        Tables.Scan(select=(:strs => :s2, :ints)),
        Tables.Scan(select=()),
        Tables.Scan(select=(), filter=Tables.col(:ints) > 2),
        Tables.Scan(select=(r"s",)),
        Tables.Scan(select=(Tables.Not(:dict),)),
        Tables.Scan(select=(:dict,)),
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
        want = Tables.scan(full, scan)
        for src in sources(filebytes)
            got = Arrow.Table(src; scan=scan)
            @assert _tables_equal(got, want) sprint(show, scan)
        end
    end
    println("differential scans match Tables.scan over the full table ✓")

    # Membership keeps the caller's collection semantics through facade
    # planning. Sets use `isequal`, so they match NaN and distinguish signed
    # zero; rebuilding them as tuples would change both results.
    specialio = IOBuffer()
    Arrow.write(specialio, (x=[NaN, 0.0, -0.0], id=Int64[1, 2, 3]); file=true)
    specialbytes = take!(specialio)
    specialfull = Arrow.Table(copy(specialbytes))
    for (values, expectedids) in ((Set([NaN]), [1]), (Set([0.0]), [2]))
        scan = Tables.Scan(select=(:id,), filter=Tables.colin(Tables.col(:x), values))
        reference = Tables.scan(specialfull, scan)
        @assert reference.id == expectedids
        for src in sources(specialbytes)
            @assert Arrow.Table(src; scan=scan).id == expectedids
        end
    end
    println("facade membership preserves Set NaN and signed-zero semantics ✓")

    # Membership objects need not be iterable. Planning must pass them to
    # their custom `in` method unchanged.
    domainscan =
        Tables.Scan(select=(:id,), filter=Tables.colin(Tables.col(:id), _ScanDomain(2)))
    @assert Tables.scan(specialfull, domainscan).id == [2]
    for src in sources(specialbytes)
        @assert Arrow.Table(src; scan=domainscan).id == [2]
    end
    println("facade membership preserves custom non-iterable domains ✓")

    # Nullability is advisory at the reader tier. A window does not inspect an
    # unread violating batch merely to widen the selected vector. Conforming
    # batches still match the generic executor's value and schema exactly.
    nonnullable = Field("x", IntType(64, true); nullable=false)
    nn_schema = Schema([nonnullable])
    firstdata = ArrayData(
        IntType(64, true),
        2,
        [BufferSlice(), AC._databuffer(Int64[1, 2])];
        nullcount=0,
    )
    violatingdata = ArrayData(
        IntType(64, true),
        2,
        [AC._databuffer(UInt8[0b10]), AC._databuffer(Int64[0, 3])];
        nullcount=1,
    )
    conformingdata = ArrayData(
        IntType(64, true),
        2,
        [BufferSlice(), AC._databuffer(Int64[3, 4])];
        nullcount=0,
    )
    windowresults(bytes) =
        Any[Arrow.Table(src; scan=Tables.Scan(limit=2)) for src in sources(bytes)]
    violatingbytes = writefile(
        nn_schema,
        [
            AC.RecordBatch(nn_schema, [firstdata], 2),
            AC.RecordBatch(nn_schema, [violatingdata], 2),
        ],
    )
    violatingfull = Arrow.Table(copy(violatingbytes))
    violatingreference = Tables.scan(violatingfull, Tables.Scan(limit=2))
    @assert violatingreference.x == [1, 2]
    @assert eltype(violatingreference.x) === Union{Missing,Int64}
    for got in windowresults(violatingbytes)
        @assert got.x == [1, 2]
        @assert eltype(got.x) === Int64
    end

    conformingbytes = writefile(
        nn_schema,
        [
            AC.RecordBatch(nn_schema, [firstdata], 2),
            AC.RecordBatch(nn_schema, [conformingdata], 2),
        ],
    )
    conformingreference =
        Tables.scan(Arrow.Table(copy(conformingbytes)), Tables.Scan(limit=2))
    for got in windowresults(conformingbytes)
        @assert _tables_equal(got, conformingreference)
    end
    println("windowed nullability keeps the skip boundary and conforming schema parity ✓")

    # A column's element type is a property of the SCHEMA, not of how many
    # rows a scan kept: a zero limit, an offset past the input, and a decoded
    # filter that keeps nothing have exactly the schema of the full scan, on
    # both the mapped and the ranged path.
    fullschema = Tables.schema(Arrow.Table(copy(filebytes); scan=Tables.Scan()))
    for emptyscan in (
        Tables.Scan(limit=0),
        Tables.Scan(offset=10_000),
        Tables.Scan(filter=Tables.col(:ints) > 10_000),
    )
        for src in sources(filebytes)
            got = Arrow.Table(src; scan=emptyscan)
            @assert Tables.rowcount(got) == 0
            @assert _schemas_equal(Tables.schema(got), fullschema) sprint(show, emptyscan)
        end
    end
    println("empty scans keep the full scan's schema on both paths ✓")

    # The bound scan is consumed exactly: selection, renames, filter, window,
    # and type overrides all cross the facade once, with no reconstructed
    # residual.
    t1 = Arrow.Table(
        copy(filebytes);
        scan=Tables.Scan(select=(:ints => :i,), offset=4, limit=3),
    )
    @assert collect(Tables.columnnames(t1)) == [:i] && length(t1.i) == 3
    t2 = Arrow.Table(
        copy(filebytes);
        scan=Tables.Scan(select=(:ints,), filter=Tables.col(:ints) > 2, limit=2),
    )
    @assert t2.ints == [3, 4]
    t3 = Arrow.Table(copy(filebytes); scan=Tables.Scan(select=(:ints => Float64,)))
    @assert eltype(t3.ints) === Float64 && t3.ints == Float64.(full.ints)
    println("the bound scan is consumed exactly, including type overrides ✓")

    # Type overrides use Tables.jl's allocation seam. This is observable for
    # scalar types that choose a custom default column container.
    containerscan = Tables.Scan(select=(:ints => _ScanOverrideValue,))
    reference = Tables.scan(full, containerscan).ints
    @assert reference isa _ScanOverrideVector{_ScanOverrideValue}
    for src in sources(filebytes)
        overridden = Arrow.Table(src; scan=containerscan).ints
        @assert typeof(overridden) === typeof(reference)
        @assert getfield.(overridden, :value) == collect(Int64, full.ints)
    end
    println("type overrides honor Tables.allocatecolumn ✓")

    # Extreme-but-valid windows: the sink saturates like Tables.scan, so the
    # whole pipeline agrees on the empty result.
    extreme = Tables.Scan(select=(:ints,), offset=typemax(Int), limit=typemax(Int))
    extremewant = Tables.scan(full, extreme)
    for src in sources(filebytes)
        got = Arrow.Table(src; scan=extreme)
        @assert _tables_equal(got, extremewant)
        @assert length(got.ints) == 0
    end
    println("extreme scan windows saturate to the empty result ✓")

    # Skip proof 1 (columns): corrupt the `strs` OFFSETS buffer of batch 2 so
    # semantic validation must reject any decode that touches it. Buffer
    # order: ints(v,d) floats(v,d) bools(v,d) strs(v,o,d) → offsets is #8.
    off, len = _bufferposition(filebytes, 2, 8)
    @assert len > 8
    corrupt = copy(filebytes)
    corrupt[(off + 5):(off + 8)] .= reinterpret(UInt8, Int32[Int32(2) ^ 30])
    @assert _rejects(() -> readfile(copy(corrupt))[2])       # full decode sees it
    @assert _rejects(() -> Arrow.Table(copy(corrupt)))
    got = Arrow.Table(copy(corrupt); scan=Tables.Scan(select=(:ints, :floats)))
    @assert isequal(collect(Any, got.ints), collect(Any, full.ints))
    @assert _rejects(() -> Arrow.Table(copy(corrupt); scan=Tables.Scan(select=(:strs,))))
    println("skipped columns are never decoded (corruption stays invisible) ✓")

    # Skip proof 2 (batches): the same corruption sits in batch 2; a window
    # ending inside batch 1 never decodes batch 2 even when selecting strs.
    got = Arrow.Table(copy(corrupt); scan=Tables.Scan(select=(:strs,), limit=5))
    # A filter keeps the window in the sink: the window fills inside batch 1,
    # so batch 2 is never decoded either.
    gotf = Arrow.Table(
        copy(corrupt);
        scan=Tables.Scan(select=(:strs,), filter=Tables.col(:ints) > 0, limit=5),
    )
    @assert length(gotf.strs) == 5
    @assert isequal(collect(Any, got.strs), collect(Any, full.strs[1:5]))
    @assert _rejects(
        () -> Arrow.Table(copy(corrupt); scan=Tables.Scan(select=(:strs,), limit=6)),
    )
    println("window-excluded batches are never decoded ✓")

    # Buffer-table invariants cannot be weakened by skipping: `skipbuffer!`
    # shares `_buffermeta!` with `takebuffer!` by construction, so a batch
    # whose buffers move backwards is refused whichever columns are decoded.
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
    @assert _rejects(() -> readfile(copy(overlap))[1])
    for src in sources(overlap)
        @assert _rejects(() -> Arrow.Table(src; scan=Tables.Scan(select=(:ints,))))
    end
    println("buffer-table invariants hold before any skip can run ✓")

    # Duplicate source names.
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
    # They read as x, x_1, as CSV.jl names them, scans included.
    dupscan = Arrow.Table(dupbytes; scan=Tables.Scan(select=(2,)))
    @assert collect(Tables.columnnames(dupscan)) == [:x_1]
    println("duplicate source names scan as x, x_1 ✓")

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
    for src in sources(badrows)
        @assert _rejects(() -> Arrow.Table(src; scan=shifted))
    end
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
    for src in sources(ovbytes)
        @assert _rejects(() -> Arrow.Table(src; scan=Tables.Scan(limit=0)))
    end
    println("overflowing variadic buffer spans reject before window exclusion ✓")

    # A column table cannot infer row count when it has no columns. The table
    # keeps the RecordBatch lengths so an empty scan remains identity.
    zerosch = Schema(Field[])
    zerobatches = AC.RecordBatch[
        AC.RecordBatch(zerosch, ArrayData[], 3),
        AC.RecordBatch(zerosch, ArrayData[], 0),
        AC.RecordBatch(zerosch, ArrayData[], 2),
    ]
    zerobytes = writefile(zerosch, zerobatches)
    for src in sources(zerobytes)
        got = Arrow.Table(src; scan=Tables.Scan())
        @assert isempty(Tables.columnnames(got))
        @assert Tables.rowcount(got) == 5
    end
    println("zero-column scans preserve their row count ✓")

    # A zero-column file can declare an addressable row count without body
    # bytes. The aggregate result must still fit Tables' Int row-count API;
    # a larger total is refused, whatever the window asks for.
    maxrows = Int64(typemax(Int))
    edgebatches = AC.RecordBatch[
        AC.RecordBatch(zerosch, ArrayData[], maxrows - 1),
        AC.RecordBatch(zerosch, ArrayData[], 1),
    ]
    overflowbatches = AC.RecordBatch[
        AC.RecordBatch(zerosch, ArrayData[], maxrows),
        AC.RecordBatch(zerosch, ArrayData[], 1),
    ]
    edgebytes = writefile(zerosch, edgebatches)
    overflowbytes = writefile(zerosch, overflowbatches)
    edgelimits = Limits(max_array_length=typemax(Int64))
    limitedsources(bytes) =
        (copy(bytes), SourceFile(BytesSource(copy(bytes)); limits=edgelimits, tailbytes=32))
    for src in limitedsources(edgebytes)
        got = Arrow.Table(src; limits=edgelimits, scan=Tables.Scan())
        @assert Tables.rowcount(got) == typemax(Int)
    end
    for src in limitedsources(overflowbytes),
        scan in (
            nothing,
            Tables.Scan(),
            Tables.Scan(limit=0),
            Tables.Scan(offset=1),
            Tables.Scan(filter=Tables.AlwaysTrue()),
        )

        @assert _rejects(() -> Arrow.Table(src; limits=edgelimits, scan=scan))
    end
    println("unaddressable cumulative row counts fail closed ✓")

    println()
    println("Tables.Scan pushdown checks passed.")
    return filebytes, full
end

function _ranged_main(filebytes::Vector{UInt8}, full)
    ranged(src; tailbytes=65536, gapbytes=262144) =
        SourceFile(src; tailbytes=tailbytes, gapbytes=gapbytes)

    # Empty projections keep their row count without requesting a record or
    # dictionary body. A filtered empty projection requests only its filter
    # column's buffers. A 32-byte tail and zero gap make every body request
    # exact and observable in the log.
    dictionaryblocks, recordblocks = _footerblocks(filebytes)
    bodyranges = vcat(_bodyrange.(dictionaryblocks), _bodyrange.(recordblocks))
    emptylog, emptysource = countingsource(filebytes)
    emptygot = Arrow.Table(
        ranged(emptysource; tailbytes=32, gapbytes=0);
        scan=Tables.Scan(select=()),
    )
    @assert isempty(Tables.columnnames(emptygot))
    @assert Tables.rowcount(emptygot) == Tables.rowcount(full)
    @assert !any(span -> _requestintersects(emptylog, span), bodyranges)

    filteredlog, filteredsource = countingsource(filebytes)
    filteredscan = Tables.Scan(select=(), filter=Tables.col(:ints) > 2)
    filteredgot =
        Arrow.Table(ranged(filteredsource; tailbytes=32, gapbytes=0); scan=filteredscan)
    @assert _tables_equal(filteredgot, Tables.scan(full, filteredscan))
    for i = 1:length(recordblocks)
        buffers = _recordbufferranges(filebytes, i)
        @assert length(buffers) >= 2
        @assert _requestintersects(filteredlog, buffers[2])
        @assert !any(span -> _requestintersects(filteredlog, span), buffers[3:end])
    end
    @assert !any(
        span -> _requestintersects(filteredlog, span),
        _bodyrange.(dictionaryblocks),
    )
    println("empty projections request no output-column body ranges ✓")

    # A Date membership filter keeps `in`'s semantics over a byte-range
    # source and fetches less than half of the object.
    dateio = IOBuffer()
    dates = Arrow.Dates.Date(2024, 1, 1) .+ Arrow.Dates.Day.(0:9_999)
    Arrow.write(dateio, (d=dates, fat=fill(repeat("x", 128), length(dates))); file=true)
    datebytes = take!(dateio)
    targetdate = Arrow.Dates.Date(2024, 3, 1)
    datescan =
        Tables.Scan(select=(:d,), filter=Tables.colin(Tables.col(:d), Set([targetdate])))
    datefull = Arrow.Table(copy(datebytes))
    datefield = getfield(datefull, :schema).fields[1]
    datelog, datesource = countingsource(datebytes)
    dategot = Arrow.Table(ranged(datesource; tailbytes=32, gapbytes=0); scan=datescan)
    datewant = Tables.scan(datefull, datescan)
    @assert _tables_equal(dategot, datewant)
    @assert dategot.d == [targetdate]
    @assert sum(last, datelog.ranges) < length(datebytes) ÷ 2

    # Tuple membership uses `==`, while Set uses `isequal` plus hashing.
    # Date and midnight DateTime compare equal; whether they are the same Set
    # key depends on Julia's Dates hashing (distinct before Julia 1.14), so
    # the expectation follows Julia's own `in`.
    datetime = Arrow.Dates.DateTime(2024, 3, 1)
    for values in (Set([datetime]), (datetime,))
        expectedrows = targetdate in values ? 1 : 0
        scan = Tables.Scan(select=(:d,), filter=Tables.colin(Tables.col(:d), values))
        reference = Tables.scan(datefull, scan)
        got = Arrow.Table(
            SourceFile(BytesSource(copy(datebytes)); tailbytes=32, gapbytes=0);
            scan=scan,
        )
        @assert _tables_equal(got, reference)
        @assert got.d == (expectedrows == 0 ? Arrow.Dates.Date[] : [targetdate])
    end

    # A custom temporal membership object is not iterable. It must force the
    # complete filter to the public plan without changing its `in` method.
    domainscan = Tables.Scan(
        select=(:d,),
        filter=Tables.colin(Tables.col(:d), _DateScanDomain(targetdate)),
    )
    domainreference = Tables.scan(datefull, domainscan)
    for source in (copy(datebytes), SourceFile(BytesSource(copy(datebytes)); tailbytes=32))
        got = Arrow.Table(source; scan=domainscan)
        @assert _tables_equal(got, domainreference)
        @assert got.d == [targetdate]
    end

    # Missing inside a standard membership container also forces a public
    # plan. Exercise it below negation and composition so three-valued
    # membership cannot be simplified to plain false.
    nullableio = IOBuffer()
    nullable_dates = Union{Missing,Arrow.Dates.Date}[
        Arrow.Dates.Date(2024, 1, 1),
        missing,
        Arrow.Dates.Date(2024, 1, 3),
    ]
    Arrow.write(nullableio, (id=Int64[1, 2, 3], d=nullable_dates); file=true)
    nullablebytes = take!(nullableio)
    nullablefull = Arrow.Table(copy(nullablebytes))
    nullablefield = getfield(nullablefull, :schema).fields[2]
    membership = Tables.colin(Tables.col(:d), (Arrow.Dates.Date(2024, 1, 1), missing))
    arraymembership = Tables.colin(
        Tables.col(:d),
        Union{Missing,Arrow.Dates.Date}[Arrow.Dates.Date(2024, 1, 3), missing],
    )
    for (filter, expected) in (
        (!membership | Tables.isnull(Tables.col(:d)), Int64[2]),
        (arraymembership & !Tables.isnull(Tables.col(:d)), Int64[3]),
    )
        scan = Tables.Scan(select=(:id,), filter=filter)
        reference = Tables.scan(nullablefull, scan)
        @assert reference.id == expected
        for source in (
            copy(nullablebytes),
            SourceFile(BytesSource(copy(nullablebytes)); tailbytes=32),
        )
            @assert Arrow.Table(source; scan=scan).id == expected
        end
    end
    println("temporal membership preserves container semantics and safe range plans ✓")

    # Dictionary wrappers recurse into their temporal value descriptor during
    # planning. Exact Date and Timestamp membership lowers; fine-unit
    # Duration membership stays in the public domain.
    function temporaldictionary(t, pool, indices)
        pooldata = Arrow.AC.ArrayData(
            t,
            length(pool),
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(pool)];
            nullcount=0,
        )
        dicttype = Arrow.AC.DictionaryType(Arrow.AC.IntType(32, true), t, false)
        field = Arrow.AC.Field("v", dicttype; nullable=false)
        data = Arrow.AC.ArrayData(
            dicttype,
            length(indices),
            [Arrow.AC.BufferSlice(), Arrow.AC._databuffer(Int32.(indices))];
            dictionary=pooldata,
            nullcount=0,
        )
        schema = Arrow.AC.Schema([field])
        batch = Arrow.AC.RecordBatch(schema, [data], length(indices))
        return field, Arrow.writefile(schema, [batch])
    end
    dictionarycases = (
        (
            Arrow.AC.DateType(Arrow.AC.DAY),
            Int32[0, 1, 2],
            Int32[0, 1, 2, 1],
            (Arrow.Dates.Date(1970, 1, 2),),
            true,
            Arrow.Dates.Date[Arrow.Dates.Date(1970, 1, 2), Arrow.Dates.Date(1970, 1, 2)],
        ),
        (
            # Timestamp{Second} is a bijection with its counts: the extreme
            # pool entry does not alias the epoch, so the membership matches
            # exactly the epoch-valued row.
            Arrow.AC.TimestampType(Arrow.AC.SECOND, nothing),
            Int64[0, Int64(1) << 61],
            Int32[0, 1],
            (Arrow.Dates.DateTime(1970, 1, 1),),
            true,
            [
                Arrow.Durations.Timestamp{Arrow.Dates.Second}(
                    Arrow.Dates.UTInstant(Arrow.Dates.Second(0)),
                ),
            ],
        ),
        (
            Arrow.AC.DurationType(Arrow.AC.SECOND),
            Int64[60],
            Int32[0],
            (Arrow.Dates.Millisecond(60_000),),
            false,
            Arrow.Dates.Second[Arrow.Dates.Second(60)],
        ),
    )
    for (type, pool, indices, values, lowered, expected) in dictionarycases
        field, bytes = temporaldictionary(type, pool, indices)
        scan = Tables.Scan(select=(:v,), filter=Tables.colin(Tables.col(:v), values))
        dictionaryfull = Arrow.Table(copy(bytes))
        reference = Tables.scan(dictionaryfull, scan)
        @assert reference.v == expected
        for source in (copy(bytes), SourceFile(BytesSource(copy(bytes)); tailbytes=32))
            @assert Arrow.Table(source; scan=scan).v == expected
        end
    end
    println("temporal dictionary scans preserve pushdown and fallback semantics ✓")

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
                        (a=collect(Int64, (n + 1):(2n)), b=[fat(i) for i = (n + 1):(2n)]),
                    ]);
                    file=false,
                )
                take!(bigio)
            end,
        ),
    )
    logall, srcall = countingsource(bigbytes)
    Arrow.Table(ranged(srcall; tailbytes=256, gapbytes=64); scan=Tables.Scan())
    logone, srcone = countingsource(bigbytes)
    Arrow.Table(ranged(srcone; tailbytes=256, gapbytes=64); scan=Tables.Scan(select=(:a,)))
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
    got = Arrow.Table(
        ranged(srcc; tailbytes=256, gapbytes=0);
        scan=Tables.Scan(select=(:ints,)),
    )
    @assert isequal(collect(Any, got.ints), collect(Any, full.ints))
    @assert !_fetched(logc, off + 5)
    @assert _rejects(
        () -> Arrow.Table(ranged(BytesSource(corrupt)); scan=Tables.Scan(select=(:strs,))),
    )
    println(
        "skipped columns add no planned body range " *
        "(fixture request log excludes the corruption) ✓",
    )

    # Window proof: a limit inside batch 1 plans no batch-2 body range. This
    # fixture's request log also excludes sampled batch-2 body bytes.
    block2 = recordblocks[2]
    body2 = (block2[1] + block2[2], block2[3])
    logw, srcw = countingsource(filebytes)
    Arrow.Table(
        ranged(srcw; tailbytes=256, gapbytes=0);
        scan=Tables.Scan(select=(:strs,), limit=5),
    )
    @assert !any(_fetched(logw, body2[1] + k) for k = 0:8:(body2[2] - 1))
    println("window-excluded batches add no planned body range ✓")

    # A dictionary body gets a planned range only when its column is in the
    # decode set. The zero-gap fixture also checks the observed request spans.
    @assert length(dictionaryblocks) == 1
    dictblock = dictionaryblocks[1]
    dictblockbody = (dictblock[1] + dictblock[2], dictblock[3])
    lognod, srcnod = countingsource(filebytes)
    Arrow.Table(
        ranged(srcnod; tailbytes=256, gapbytes=0);
        scan=Tables.Scan(select=(:ints,)),
    )
    @assert !any(_fetched(lognod, dictblockbody[1] + k) for k = 0:8:(dictblockbody[2] - 1))
    logd, srcd = countingsource(filebytes)
    Arrow.Table(ranged(srcd; tailbytes=256, gapbytes=0); scan=Tables.Scan(select=(:dict,)))
    @assert any(_fetched(logd, dictblockbody[1] + k) for k = 0:8:(dictblockbody[2] - 1))
    logd0, srcd0 = countingsource(filebytes)
    Arrow.Table(
        ranged(srcd0; tailbytes=256, gapbytes=0);
        scan=Tables.Scan(select=(:dict,), limit=0),
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
    dvgot = Arrow.Table(
        ranged(BytesSource(copy(dvbytes)));
        scan=Tables.Scan(select=(:dictview, :tail)),
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
    @assert _rejects(() -> Arrow.Table(copy(missingdict); scan=missingscan))
    logmissing, srcmissing = countingsource(missingdict)
    @assert _rejects(
        () -> Arrow.Table(ranged(srcmissing; tailbytes=32, gapbytes=0); scan=missingscan),
    )
    @assert !any(_fetched(logmissing, block[1] + block[2]) for block in missingrecords)
    println("missing dictionary plans reject before dedicated record-body requests ✓")

    # Coalescing: an infinite gap merges every body range into one request;
    # a zero gap issues more, smaller requests; both agree with the truth.
    # (A small tail window, so the ranges are planned rather than served
    # from the cached tail.)
    twoscan = Tables.Scan(select=(:ints, :strs))
    logbig, srcbig = countingsource(filebytes)
    gotbig =
        Arrow.Table(ranged(srcbig; tailbytes=256, gapbytes=typemax(Int32)); scan=twoscan)
    logzero, srczero = countingsource(filebytes)
    gotzero = Arrow.Table(ranged(srczero; tailbytes=256, gapbytes=0); scan=twoscan)
    want = Tables.scan(full, twoscan)
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
    gott = Arrow.Table(ranged(srct; tailbytes=32); scan=Tables.Scan(select=(:ints,)))
    @assert isequal(collect(Any, gott.ints), collect(Any, full.ints))
    println("undersized tails recover with one exact footer fetch ✓")

    # Retained tail/Footer payloads cost no new allocation on a later scan.
    # The verifier's own expansion charges still apply on every parse.
    cachelog, cachesource = countingsource(filebytes)
    cachefile = SourceFile(cachesource; tailbytes=32)
    cachecap = Limits().max_total_allocated_bytes
    firstbudget = AllocationBudget(cachecap)
    _readfooter(cachefile, cachefile.limits, firstbudget)
    firstrequests = length(cachelog.ranges)
    secondbudget = AllocationBudget(cachecap)
    _readfooter(cachefile, cachefile.limits, secondbudget)
    footerlen = Int64(reinterpret(Int32, filebytes[(end - 9):(end - 6)])[1])
    footerstart = Int64(length(filebytes)) - 10 - footerlen
    tailstart = Int64(length(filebytes)) - 32
    cachedallocation = AC._materializedvectorbytes(UInt8, 32)
    footerstart < tailstart &&
        (cachedallocation += AC._materializedvectorbytes(UInt8, footerlen))
    firstused = cachecap - _remaining(firstbudget)
    secondused = cachecap - _remaining(secondbudget)
    @assert firstused - secondused == cachedallocation
    @assert length(cachelog.ranges) == firstrequests

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
    zfull = Arrow.Table(copy(zbytes))
    logz, srcz = countingsource(zbytes)
    gotz =
        Arrow.Table(ranged(srcz; tailbytes=256, gapbytes=0); scan=Tables.Scan(select=(:x,)))
    @assert isequal(collect(Any, gotz.x), collect(Any, zfull.x))
    @assert logz.bytes < length(zbytes)
    println("compressed files range-read through self-contained buffers ✓")

    # Every failure derivable from the selected metadata plan precedes its
    # first body request. Skipped columns and window-excluded batches keep
    # their intentional lazy boundary.
    block1 = recordblocks[1]
    badfixed = _setbufferlength!(copy(filebytes), block1, 2, Int64(1))
    fixedoff, _ = _bufferposition(filebytes, 1, 2)
    @assert _rejects(() -> Arrow.Table(copy(badfixed); scan=Tables.Scan(select=(:ints,))))
    logfixed, srcfixed = countingsource(badfixed)
    @assert _rejects(
        () -> Arrow.Table(
            ranged(srcfixed; tailbytes=32, gapbytes=0);
            scan=Tables.Scan(select=(:ints,)),
        ),
    )
    @assert !_fetched(logfixed, fixedoff)
    skipped = Arrow.Table(
        ranged(BytesSource(copy(badfixed)); tailbytes=32, gapbytes=0);
        scan=Tables.Scan(select=(:floats,)),
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
        () -> Arrow.Table(
            ranged(srcvalid; tailbytes=32, gapbytes=0);
            scan=Tables.Scan(select=(:x,)),
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
    ) == 2

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
    ) == 2

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
        () -> Arrow.Table(
            ranged(srcemptyoffset; tailbytes=32, gapbytes=0);
            scan=Tables.Scan(select=(:x,)),
        ),
    )
    @assert !_fetched(logemptyoffset, parentoffsetpos)

    badoffsets = _setbufferlength!(copy(filebytes), block1, 8, Int64(4))
    offsetpos, _ = _bufferposition(filebytes, 1, 8)
    logoffsets, srcoffsets = countingsource(badoffsets)
    @assert _rejects(
        () -> Arrow.Table(
            ranged(srcoffsets; tailbytes=32, gapbytes=0);
            scan=Tables.Scan(select=(:strs,)),
        ),
    )
    @assert !_fetched(logoffsets, offsetpos)

    badstruct = _setnodelength!(copy(filebytes), block1, 8, Int64(4))
    structpos, _ = _bufferposition(filebytes, 1, 16)
    logstruct, srcstruct = countingsource(badstruct)
    @assert _rejects(
        () -> Arrow.Table(
            ranged(srcstruct; tailbytes=32, gapbytes=0);
            scan=Tables.Scan(select=(:structs,)),
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
            () -> Arrow.Table(
                ranged(srcsparse; tailbytes=32, gapbytes=0);
                scan=Tables.Scan(select=(:u,)),
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
            () -> Arrow.Table(
                ranged(srccompressed; tailbytes=32, gapbytes=0);
                scan=Tables.Scan(select=(:x,)),
            ),
        )
        @assert !_fetched(logcompressed, compressedpos)
    end

    baddict = _setbufferlength!(copy(filebytes), dictblock, 2, Int64(1))
    logbaddict, srcbaddict = countingsource(baddict)
    @assert _rejects(
        () -> Arrow.Table(
            ranged(srcbaddict; tailbytes=32, gapbytes=0);
            scan=Tables.Scan(select=(:dict,)),
        ),
    )
    @assert !any(
        _fetched(logbaddict, dictblockbody[1] + k) for k = 0:8:(dictblockbody[2] - 1)
    )
    skippeddict = Arrow.Table(
        ranged(BytesSource(copy(baddict)); tailbytes=32, gapbytes=0);
        scan=Tables.Scan(select=(:ints,)),
    )
    @assert isequal(collect(Any, skippeddict.ints), collect(Any, full.ints))

    badwindow = _setbufferlength!(copy(filebytes), recordblocks[2], 2, Int64(1))
    windowpos, _ = _bufferposition(filebytes, 2, 2)
    logwindow, srcwindow = countingsource(badwindow)
    windowed = Arrow.Table(
        ranged(srcwindow; tailbytes=32, gapbytes=0);
        scan=Tables.Scan(select=(:ints,), limit=5),
    )
    @assert isequal(collect(Any, windowed.ints), collect(Any, full.ints[1:5]))
    @assert !_fetched(logwindow, windowpos)
    println("planned metadata failures reject before dedicated body requests ✓")

    # Legacy V4 message-level compression is rejected from metadata even when
    # limit=0 leaves no body to decode.
    legacyv4, legacyblock = _legacyv4file()
    legacyscan = Tables.Scan(select=(:x,), limit=0)
    @assert _rejects(() -> Arrow.Table(copy(legacyv4); scan=legacyscan))
    loglegacy, srclegacy = countingsource(legacyv4)
    @assert _rejects(
        () -> Arrow.Table(ranged(srclegacy; tailbytes=32, gapbytes=0); scan=legacyscan),
    )
    @assert !_fetched(loglegacy, legacyblock[1] + legacyblock[2])
    println("legacy compression rejects before dedicated record-body requests ✓")

    # Hostile inputs fail closed: forged footer length, overlapping Blocks,
    # out-of-body zero-length buffers, and truncated objects.
    badlen = copy(filebytes)
    lenpos = length(badlen) - 9
    badlen[lenpos:(lenpos + 3)] .= reinterpret(UInt8, Int32[Int32(2) ^ 30])
    @assert _rejects(() -> Arrow.Table(ranged(BytesSource(badlen)); scan=Tables.Scan()))
    @assert _rejects(
        () -> Arrow.Table(ranged(BytesSource(filebytes[1:20])); scan=Tables.Scan()),
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
    @assert _rejects(() -> Arrow.Table(ranged(BytesSource(overlap)); scan=Tables.Scan()))

    zerobuffer = copy(filebytes)
    block = recordblocks[1]
    meta = copy(zerobuffer[(block[1] + 9):(block[1] + block[2])])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    rb = _headertable(meta, msg)
    bufferstart, _ = _vvector(rb, 2, 16; required=true)
    _write_i64!(meta, bufferstart, block[3] + 8)
    copyto!(zerobuffer, block[1] + 9, meta, 1, length(meta))
    @assert _rejects(() -> readfile(copy(zerobuffer))[1])
    @assert _rejects(() -> Arrow.Table(copy(zerobuffer); scan=Tables.Scan(select=(:ints,))))
    @assert _rejects(
        () ->
            Arrow.Table(ranged(BytesSource(zerobuffer)); scan=Tables.Scan(select=(:ints,))),
    )
    println("forged footers and truncated objects fail closed ✓")

    # Ranged limits are checked before dedicated body requests. One whole-file
    # Scan also keeps one aggregate budget across every batch it decompresses.
    @assert _rejects(
        () -> Arrow.Table(
            SourceFile(BytesSource(filebytes); limits=Limits(max_body_bytes=32));
            scan=Tables.Scan(select=(:ints,)),
        ),
    )
    @assert _rejects(
        () -> Arrow.Table(
            SourceFile(BytesSource(filebytes); limits=Limits(max_messages=1));
            scan=Tables.Scan(),
        ),
    )
    loglimit, srclimit = countingsource(filebytes)
    intoff, _ = _bufferposition(filebytes, 1, 2)
    @assert _rejects(
        () -> Arrow.Table(
            SourceFile(
                srclimit;
                limits=Limits(max_buffer_bytes=8),
                tailbytes=256,
                gapbytes=0,
            );
            scan=Tables.Scan(select=(:ints,)),
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
        () -> Arrow.Table(copy(largebytes); limits=tight, scan=Tables.Scan(select=(:x,))),
    )
    @assert _rejects(
        () -> Arrow.Table(
            SourceFile(BytesSource(largebytes); limits=tight);
            scan=Tables.Scan(select=(:x,)),
        ),
    )

    # The first scan owns a newly fetched tail too. A large tail must not sit
    # outside the cumulative budget, even when an empty projection needs no
    # record body.
    tailio = IOBuffer()
    Arrow.write(tailio, (x=collect(Int64, 1:5_000),); file=true)
    tailfilebytes = take!(tailio)
    taillog, tailsource = countingsource(tailfilebytes)
    tailtight = Limits(max_total_allocated_bytes=32_000)
    tailerror = try
        Arrow.Table(
            SourceFile(tailsource; limits=tailtight, tailbytes=length(tailfilebytes));
            scan=Tables.Scan(select=()),
        )
        nothing
    catch e
        e
    end
    @assert tailerror isa AllocationLimitError
    @assert occursin("tail range fetch", sprint(showerror, tailerror))
    @assert isempty(taillog.ranges)

    println("range limits and scan-wide allocation budgets fail before overuse ✓")

    println()
    println("Byte-range scan checks passed.")
end
