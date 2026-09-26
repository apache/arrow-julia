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
# Acceptance: this writer's bytes read back by Core; 2.x-written fixtures
# read by Core.
# ---------------------------------------------------------------------------

"""
Hand-build a one-column batch from raw buffer bytes (the write-side mirror of
the read fixtures): interval layouts have no 2.x writer to lean on.
"""
function _handbatch(t::ArrowType, n::Int, buffers::Vector{Vector{UInt8}}; nullcount::Int=0)
    f = Field("x", t, true, nothing, Field[])
    slices = BufferSlice[
        isempty(bytes) ? BufferSlice() : BufferSlice(heapregion(bytes), 0, length(bytes)) for bytes in buffers
    ]
    d = ArrayData(t, n, slices; nullcount=nullcount)
    sch = Schema(Field[f])
    return sch, AC.RecordBatch(sch, ArrayData[d], n)
end

_le(xs...) = reduce(vcat, [collect(reinterpret(UInt8, [x])) for x in xs])

function _materialized(stream)
    return [
        [materialize(f, b.columns[i]) for (i, f) in enumerate(stream.schema.fields)] for
        b in stream.batches
    ]
end

function _assert_stream_equal(a, b)
    @assert length(a.batches) == length(b.batches)
    @assert length(a.schema.fields) == length(b.schema.fields)
    for (fa, fb) in zip(a.schema.fields, b.schema.fields)
        @assert fa.name == fb.name
        @assert AC.typeequal(fa.type, fb.type)
    end
    ma, mb = _materialized(a), _materialized(b)
    for (ba, bb) in zip(ma, mb), (ca, cb) in zip(ba, bb)
        @assert isequal(collect(Any, ca), collect(Any, cb))
    end
    return nothing
end

function ipc_write_battery()
    # The same fixture table the read acceptance uses: 2.x writes it, Core
    # decodes it, and from here on the WRITER is the system under test.
    expected = MIXED_EXPECTED
    source = readstream(_mixed_two_partitions_bytes())

    # Stream round-trip: our writer -> our reader.
    bytes = writestream(source)
    roundtrip = readstream(bytes)
    _assert_stream_equal(source, roundtrip)
    println("writer -> reader stream round-trip ✓")

    # The dictionary batch is emitted once: the second batch reuses the same
    # pool snapshot, so no replacement message and no feature declaration —
    # a declared feature is a demand on readers (nanoarrow refuses streams
    # declaring one it does not support), so eager streams declare only what
    # their batches actually use.
    kinds = [f.kind for f in _frameinfo(bytes)]
    @assert count(==(UInt8(2)), kinds) == 1
    @assert isempty(framemessages(heapregion(copy(bytes)))[1].features)
    println("unchanged pools write one dictionary batch (replacement-on-change) ✓")

    # Compressed round-trips, both codecs, both directions.
    for codec in (:lz4, :zstd)
        cbytes = writestream(source; compress=codec)
        cstream = readstream(cbytes)
        _assert_stream_equal(source, cstream)
        # The compression feature is declared (standards-conforming; 2.x
        # omits it and the reader accepts both).
        cframes = framemessages(heapregion(copy(cbytes)))
        @assert Int64(2) in cframes[1].features
        println("$(codec)-compressed writer stream round-trips ✓")
    end

    # Incompressible buffers fall back to the -1 stored-raw prefix.
    rawsource = readstream(_fixture2x("incompressible-bytes") do
        rng_bytes = Vector{UInt8}(reinterpret(UInt8, hash.(1:4096)))
        rawio = IOBuffer()
        Arrow.write(rawio, (x=rng_bytes,); file=false)
        take!(rawio)
    end)
    rawbytes = writestream(rawsource; compress=:lz4)
    rawstream = readstream(rawbytes)
    _assert_stream_equal(rawsource, rawstream)
    println("incompressible buffers store raw behind the -1 prefix ✓")

    # Replacement-on-change: a stream whose pool changes identity between
    # batches (built by the read example's replacement fixture) re-encodes to
    # a replacement stream — feature declared, two dictionary batches, and
    # both our reader and the frame shape agree.
    replaced = readstream(_dictionary_replacement_stream())
    rbytes = writestream(replaced)
    rframes = framemessages(heapregion(copy(rbytes)))
    @assert Int64(1) in rframes[1].features
    rkinds = [fm.header_type for fm in rframes]
    @assert count(==(UInt8(2)), rkinds) == 2
    rstream = readstream(rbytes)
    _assert_stream_equal(replaced, rstream)
    @assert rstream.batches[1].columns[1].dictionary !==
            rstream.batches[2].columns[1].dictionary
    println("pool-identity change emits a feature-gated replacement batch ✓")

    # Schema-only and zero-row streams.
    emptysch = Schema(Field[Field("x", IntType(64, true), true, nothing, Field[])])
    schemaonly = writestream(emptysch, AC.RecordBatch[])
    schemaonlystream = readstream(schemaonly)
    @assert isempty(schemaonlystream.batches)
    @assert isempty(
        framemessages(
            heapregion(copy(writestream(emptysch, AC.RecordBatch[]; compress=:zstd))),
        )[1].features,
    )
    zerorow = readstream(writestream(readstream(_fixture2x("int64-empty") do
        z = IOBuffer()
        Arrow.write(z, (x=Int64[],); file=false)
        take!(z)
    end)))
    @assert zerorow.batches[1].nrows == 0
    println("schema-only streams do not overdeclare compression; zero rows round-trip ✓")

    # Core may omit the physical offsets buffer for a canonical empty array.
    # IPC still carries length + 1 offsets, so the adapter materializes one
    # zero without changing Core's allocation-free representation.
    emptyutf8 = Utf8Type(false)
    emptyfield = Field("empty", emptyutf8)
    emptydata = ArrayData(emptyutf8, 0, [BufferSlice(), BufferSlice(), BufferSlice()])
    emptybatch = AC.RecordBatch(Schema([emptyfield]), [emptydata], 0)
    emptybytes = writestream(emptybatch.schema, [emptybatch])
    emptyframes = framemessages(heapregion(copy(emptybytes)))
    emptybuffers =
        something((emptyframes[2].msg.header::Meta.RecordBatch).buffers, Meta.Buffer[])
    @assert emptybuffers[2].length == 4
    # ... and the reader ACCEPTS the omitted-offsets form for zero-length
    # arrays (Core's canonical empty; nanoarrow and C++ write it), which the
    # same message with its offsets buffer length zeroed exercises.
    omittedempty = copy(emptybytes)
    _mutatemessage!(omittedempty, 2) do meta, msg
        rb = _headertable(meta, msg)
        bufferstart, nbufs = _vvector(rb, 2, 16; required=true)
        @assert nbufs == 3
        _write_i64!(meta, bufferstart + 16 + 8, Int64(0))
    end
    omittedstream = readstream(omittedempty)
    @assert isempty(
        materialize(omittedstream.schema.fields[1], omittedstream.batches[1].columns[1]),
    )
    println("empty IPC offset arrays: written with one terminal zero, read with none ✓")

    # Schema and field metadata round-trip through the writer.
    msource = readstream(
        _fixture2x("schema-field-metadata") do
            mio = IOBuffer()
            Arrow.write(
                mio,
                (x=Int64[1],);
                file=false,
                metadata=Dict("owner" => "jacob"),
                colmetadata=Dict(:x => Dict("unit" => "count")),
            )
            take!(mio)
        end,
    )
    mstream = readstream(writestream(msource))
    @assert Dict(mstream.schema.metadata) == Dict("owner" => "jacob")
    @assert Dict(mstream.schema.fields[1].metadata) == Dict("unit" => "count")
    println("schema and field metadata round-trip through the writer ✓")

    # Writer refusals: offset views, mismatched schemas, unknown codecs.
    off = ArrayData(IntType(64, true), 1, source.batches[1].columns[1].buffers; offset=1)
    offbatch = AC.RecordBatch(Schema(Field[source.schema.fields[1]]), ArrayData[off], 1)
    @assert _rejects(() -> writestream(offbatch.schema, [offbatch]))
    @assert _rejects(() -> writestream(Schema(Field[]), [source.batches[1]]))
    caught = try
        writestream(source; compress=:snappy)
        false
    catch e
        e isa ArgumentError
    end
    @assert caught
    @assert _rejects(() -> _requirelittleendian(UInt32(0x01020304)))
    println("offset views, schema mismatches, and unknown codecs are refused ✓")

    # Schema-only output still validates the full Schema/Field envelope.
    invalidname = String(UInt8[0xff])
    badnameschema = Schema(Field[Field(invalidname, IntType(64, true))])
    binaryschema = Schema(emptysch.fields; metadata=[invalidname => "value"])
    bigschema = Schema(emptysch.fields; endianness=AC.BigEndian)
    badreeschema = Schema(
        Field[Field(
            "ree",
            RunEndEncodedType();
            children=[
                Field("wrong", IntType(32, true); nullable=false),
                Field("also-wrong", IntType(64, true)),
            ],
        )],
    )
    @assert _rejects(() -> writestream(badnameschema, AC.RecordBatch[]))
    @assert _rejects(() -> writefile(badnameschema, AC.RecordBatch[]))
    binaryfile = readfile(writefile(binaryschema, AC.RecordBatch[]))
    @assert collect(binaryfile.schema.metadata) == [invalidname => "value"]
    @assert _rejects(() -> writestream(bigschema, AC.RecordBatch[]))
    @assert _rejects(() -> writestream(badreeschema, AC.RecordBatch[]))
    @assert _rejects(() -> writefile(badreeschema, AC.RecordBatch[]))
    println(
        "schema-only writers preserve binary metadata and validate names, endianness, and REE children ✓",
    )

    # A Field object is one writer-side dictionary-id key. Reusing that exact
    # object at two positions must not collapse two distinct pools onto one id.
    aliasfield, aliasdata1 = AC.fromjulia_dict("d", ["a", "b"], [0, 1])
    _, aliasdata2 = AC.fromjulia_dict("d", ["x", "y"], [0, 1])
    aliasschema = Schema(Field[aliasfield, aliasfield])
    aliasbatch = AC.RecordBatch(aliasschema, ArrayData[aliasdata1, aliasdata2], 2)
    @assert _rejects(() -> writestream(aliasschema, [aliasbatch]))
    sharedvaluechild = Field("value", IntType(64, true))
    aliaseddict = Field(
        "dict",
        DictionaryType(IntType(32, true), StructType(), false);
        children=[sharedvaluechild],
    )
    aliasedlist = Field("list", ListType(false); children=[sharedvaluechild])
    @assert _rejects(() -> assigndictids([aliaseddict, aliasedlist]))

    # One pool shared through two dictionary fields must satisfy both value
    # schemas. The batch's own schema permits the null; the requested writer
    # schema deliberately makes the second value child non-nullable.
    poolfield, pooldata = AC.fromjulia_struct("pool", (a=Union{Missing,Int64}[missing],))
    dtype = DictionaryType(IntType(32, true), poolfield.type, false)
    _, indexdata = fromjulia("index", Int32[0])
    dictdata = ArrayData(dtype, 1, indexdata.buffers; dictionary=pooldata, nullcount=0)
    nullablechild = poolfield.children[1]
    strictchild = Field(nullablechild.name, nullablechild.type; nullable=false)
    batchfields = Field[
        Field("left", dtype; children=[nullablechild]),
        Field("right", dtype; children=[nullablechild]),
    ]
    strictfields = Field[batchfields[1], Field("right", dtype; children=[strictchild])]
    sharedbatch = AC.RecordBatch(Schema(batchfields), ArrayData[dictdata, dictdata], 1)
    # Field.nullable is advisory at the semantic tier (the gold corpus itself
    # violates it), so the skewed write is accepted; the strict declaration
    # is enforced by the opt-in validate_full tier.
    @assert readstream(writestream(Schema(strictfields), [sharedbatch])) isa IPCStream
    @assert _rejects(() -> AC.validate_full(strictfields[2], dictdata))
    @assert AC.validate_full(batchfields[2], dictdata) === dictdata
    println("dictionary field aliases are refused; contract skew is validate_full's ✓")

    # One id names ONE pool within a record batch: a caller id table mapping
    # two fields to one id with DIFFERENT pools would decode both fields
    # through whichever pool was emitted last.
    skewf1, skewd1 = AC.fromjulia_dict("s1", ["a"], [0])
    skewf2, skewd2 = AC.fromjulia_dict("s2", ["b"], [0])
    skewids = IdDict{Field,Int64}(skewf1 => Int64(7), skewf2 => Int64(7))
    skewsch = Schema(Field[skewf1, skewf2])
    skewbatch = AC.RecordBatch(skewsch, ArrayData[skewd1, skewd2], 1)
    @assert _rejects(() -> writestream(skewsch, [skewbatch]; dictids=skewids))
    okd2 =
        ArrayData(skewf2.type, 1, skewd2.buffers; dictionary=skewd1.dictionary, nullcount=0)
    okbatch = AC.RecordBatch(skewsch, ArrayData[skewd1, okd2], 1)
    okstream = readstream(writestream(skewsch, [okbatch]; dictids=skewids))
    @assert okstream.fielddictids[okstream.schema.fields[1]] ==
            okstream.fielddictids[okstream.schema.fields[2]]
    # ... and a repeated id must carry ONE nested dictionary-id topology, or
    # the second field would decode through pools its schema never declared.
    innerty = DictionaryType(IntType(32, true), Utf8Type(false), false)
    inner1 = Field("inner", innerty)
    inner2 = Field("inner", innerty)
    outerty = DictionaryType(IntType(32, true), StructType(), false)
    topo1 = Field("o1", outerty; children=[inner1])
    topo2 = Field("o2", outerty; children=[inner2])
    topoids = IdDict{Field,Int64}(
        topo1 => Int64(10),
        topo2 => Int64(10),
        inner1 => Int64(20),
        inner2 => Int64(21),
    )
    @assert _rejects(() -> validatedictionaryids(Field[topo1, topo2], topoids))
    topoids[inner2] = Int64(20)
    @assert validatedictionaryids(Field[topo1, topo2], topoids) isa Dict
    # ... and fresh ids fill unoccupied values instead of wrapping past a
    # given id at the top of the signed-long domain.
    wrapfs = Field[Field("w$i", innerty) for i = 1:3]
    wrapids = assigndictids(
        wrapfs,
        IdDict{Field,Int64}(wrapfs[1] => typemin(Int64), wrapfs[2] => typemax(Int64)),
    )
    @assert length(Set(values(wrapids))) == 3
    println("shared dictionary ids: one pool per batch, one nested topology, no id wrap ✓")

    # Unions, both modes: 2.x writes them, Core reads and re-encodes them.
    # The mapped set matches Core's accessor coverage; the self-round-trips
    # below cover the view layouts and REE that Arrow.jl 2.x does not emit.
    sparsebytes = UInt8[]
    for (modename, dense) in (("dense", true), ("sparse", false))
        usource = readstream(
            _fixture2x("union-$(modename)") do
                uio = IOBuffer()
                Arrow.write(
                    uio,
                    (u=Union{Int64,String}[1, "x", 2, "y"],);
                    file=false,
                    denseunions=dense,
                )
                take!(uio)
            end,
        )
        ut = usource.schema.fields[1].type
        @assert ut isa UnionType
        @assert (ut.mode == AC.DenseMode) == dense
        ubytes = writestream(usource)
        dense || (sparsebytes = copy(ubytes))
        _assert_stream_equal(usource, readstream(ubytes))
        println("$(modename) unions round-trip ✓")
    end

    # IPC sparse-union children have exactly the parent length. Core allows a
    # longer backing child for sliced C Data, so this rule stays at the IPC
    # boundary. Omitted union ids also fail cleanly before Int8 conversion.
    onechild, longchild = fromjulia("i", Int64[10, 20])
    sparse = UnionType(AC.SparseMode, Int8[0])
    sparsefield = Field("u", sparse; children=[onechild])
    sparsedata = ArrayData(sparse, 1, [AC._databuffer(Int8[0])]; children=[longchild])
    sparsebatch = AC.RecordBatch(Schema([sparsefield]), [sparsedata], 1)
    @assert _rejects(() -> writestream(sparsebatch.schema, [sparsebatch]))
    ub = FB.Builder(64)
    Meta.unionStart(ub)
    Meta.unionAddMode(ub, Meta.UnionMode.Sparse)
    FB.finish!(ub, Meta.unionEnd(ub))
    umeta = FB.getrootas(Meta.Union, collect(FB.finishedbytes(ub)), 0)
    too_many_children = Field[Field("c$i", NullType()) for i = 1:129]
    @assert _rejects(() -> _coremetatype(umeta, too_many_children))
    _mutatemessage!(sparsebytes, 2) do meta, msg
        rb = _headertable(meta, msg)
        _write_i64!(meta, _vfield(rb, 0, 8; required=true), Int64(3))
        nodestart, nnodes = _vvector(rb, 1, 16; required=true)
        @assert nnodes >= 2
        _write_i64!(meta, nodestart, Int64(3))
    end
    @assert _rejects(() -> readstream(sparsebytes))
    customleft, customleftdata = fromjulia("left", Int64[10, 20])
    customright, customrightdata = fromjulia("right", ["x", "y"])
    customtype = UnionType(AC.SparseMode, Int8[7, 3])
    customfield = Field("u", customtype; children=[customleft, customright])
    customdata = ArrayData(
        customtype,
        2,
        [AC._databuffer(Int8[7, 3])];
        children=[customleftdata, customrightdata],
    )
    custombatch = AC.RecordBatch(Schema([customfield]), [customdata], 2)
    customstream = readstream(writestream(custombatch.schema, [custombatch]))
    @assert materialize(
        customstream.schema.fields[1],
        customstream.batches[1].columns[1],
    ) == Any[10, "y"]
    println("IPC sparse-union length and union-id domains are enforced ✓")

    # Intervals, all three units, hand-built (2.x has no interval writer).
    # MONTH_DAY_NANO exceeds 2.x entirely: its vendored enum predates the
    # unit, so 2.x must fail while this adapter round-trips it.
    ym = _handbatch(
        IntervalType(AC.YEAR_MONTH),
        3,
        [UInt8[0x05], _le(Int32(12), Int32(0), Int32(7))];
        nullcount=1,
    )
    dt = _handbatch(
        IntervalType(AC.DAY_TIME),
        3,
        [UInt8[], _le(Int32(1), Int32(2), Int32(3), Int32(4), Int32(5), Int32(6))],
    )
    mdn = _handbatch(
        IntervalType(AC.MONTH_DAY_NANO),
        2,
        [UInt8[], _le(Int32(1), Int32(2), Int64(3), Int32(4), Int32(5), Int64(6))],
    )
    intervalwant = (
        (ym, Any[12, missing, 7]),
        (dt, Any[(days=1, millis=2), (days=3, millis=4), (days=5, millis=6)]),
        (mdn, Any[(months=1, days=2, nanos=3), (months=4, days=5, nanos=6)]),
    )
    for ((sch, batch), want) in intervalwant
        ibytes = writestream(sch, [batch])
        istream = readstream(ibytes)
        @assert istream.schema.fields[1].type == sch.fields[1].type
        got = materialize(istream.schema.fields[1], istream.batches[1].columns[1])
        @assert isequal(collect(Any, got), want)
    end
    println("intervals round-trip, including MONTH_DAY_NANO ✓")

    # ---- File format ----------------------------------------------------

    filebytes = writefile(source)
    file = readfile(copy(filebytes))
    @assert length(file) == 2
    # Random access, last batch first — nothing but the footer index drives it.
    for i in (2, 1)
        batch = file[i]
        for (j, f) in enumerate(file.schema.fields)
            want = materialize(f, source.batches[i].columns[j])
            @assert isequal(
                collect(Any, materialize(f, batch.columns[j])),
                collect(Any, want),
            )
        end
    end
    println("writer -> readfile random-access round-trip ✓")

    # We read a 2.x-written file (the reverse direction — other
    # implementations reading OUR bytes — is the oracle suite's job).
    theirs = readfile(
        _fixture2x("mixed-two-partitions-file") do
            fio = IOBuffer()
            fwritetable = merge(expected, (dict=Arrow.DictEncode(expected.dict),))
            Arrow.write(fio, Tables.partitioner([fwritetable, fwritetable]); file=true)
            take!(fio)
        end,
    )
    @assert length(theirs) == 2
    for i = 1:2, (j, f) in enumerate(theirs.schema.fields)
        @assert isequal(
            collect(Any, materialize(f, theirs[i].columns[j])),
            collect(Any, materialize(f, source.batches[i].columns[j])),
        )
    end
    println("2.x-written files read back ✓")

    # Compressed file round-trip.
    zfilebytes = writefile(source; compress=:zstd)
    zfile = readfile(zfilebytes)
    for (j, f) in enumerate(zfile.schema.fields)
        @assert isequal(
            collect(Any, materialize(f, zfile[1].columns[j])),
            collect(Any, materialize(f, source.batches[1].columns[j])),
        )
    end
    zfooterlen = Int64(reinterpret(Int32, zfilebytes[(end - 9):(end - 6)])[1])
    zfooterstart = Int64(length(zfilebytes)) - 10 - zfooterlen
    zfooterbytes = copy(zfilebytes[(zfooterstart + 1):(zfooterstart + zfooterlen)])
    _, zfooterfeatures, _, _, _ = verify_footer(zfooterbytes, Limits())
    zstreamsection = copy(zfilebytes[9:zfooterstart])
    zschemafeatures = framemessages(heapregion(zstreamsection))[1].features
    @assert zschemafeatures == Int64[2] == zfooterfeatures
    emptyfilebytes = writefile(emptysch, AC.RecordBatch[]; compress=:zstd)
    emptyfooterlen = Int64(reinterpret(Int32, emptyfilebytes[(end - 9):(end - 6)])[1])
    emptyfooterstart = Int64(length(emptyfilebytes)) - 10 - emptyfooterlen
    emptyfooterbytes =
        copy(emptyfilebytes[(emptyfooterstart + 1):(emptyfooterstart + emptyfooterlen)])
    _, emptyfeatures, _, _, _ = verify_footer(emptyfooterbytes, Limits())
    @assert isempty(emptyfeatures)
    println("compressed file schemas declare feature 2 exactly when needed ✓")

    # Mmap path: the file region's root is the Mmap array; decode after GC.
    mmapdir = mktempdir()
    mmappath = joinpath(mmapdir, "roundtrip.arrow")
    write(mmappath, filebytes)
    mfile = readfile(mmapregion(mmappath))
    GC.gc(true)
    @assert length(mfile) == 2
    @assert isequal(
        collect(Any, materialize(mfile.schema.fields[1], mfile[2].columns[1])),
        collect(Any, materialize(source.schema.fields[1], source.batches[2].columns[1])),
    )
    println("mmap-backed files decode through the reachability-rooted region ✓")

    # File-format refusals: replacement pools, truncated/corrupt footers,
    # magic damage, block escapes.
    @assert _rejects(() -> writefile(replaced))
    nomagic = copy(filebytes)
    nomagic[end] ⊻= 0xff
    @assert _rejects(() -> readfile(nomagic))
    nohead = copy(filebytes)
    nohead[1] ⊻= 0xff
    @assert _rejects(() -> readfile(nohead))
    shortfile = filebytes[1:(end - 7)]
    @assert _rejects(() -> readfile(shortfile))
    lyinglen = copy(filebytes)
    lenpos = length(lyinglen) - 9
    lyinglen[lenpos:(lenpos + 3)] .= reinterpret(UInt8, Int32[Int32(2^30)])
    @assert _rejects(() -> readfile(lyinglen))

    # The leading Schema message is part of the file contract, not dead
    # padding. It must agree semantically with Footer.schema.
    differentschema = copy(filebytes)
    embeddedlen = Int64(reinterpret(Int32, differentschema[13:16])[1])
    embedded = copy(differentschema[17:(16 + embeddedlen)])
    embeddedmsg = _vtable(embedded, Int64(_vu32(embedded, 0)))
    embeddedschema = _vtable(embedded, _vref(embeddedmsg, 2; required=true))
    fieldvec, nembeddedfields = _vvector(embeddedschema, 1, 4; required=true)
    @assert nembeddedfields > 0
    embeddedfield =
        _vtable(embedded, AC.checked_add(fieldvec, Int64(_vu32(embedded, fieldvec))))
    namepos = _vref(embeddedfield, 0; required=true)
    differentschema[16 + namepos + 4 + 1] = UInt8('z')
    @assert _rejects(() -> readfile(differentschema))

    # Files cannot opt into stream dictionary replacement, even when their
    # block index happens to contain no duplicate dictionary id.
    replacementfeature = copy(zfilebytes)
    embeddedlen = Int64(reinterpret(Int32, replacementfeature[13:16])[1])
    embedded = copy(replacementfeature[17:(16 + embeddedlen)])
    embeddedmsg = _vtable(embedded, Int64(_vu32(embedded, 0)))
    embeddedschema = _vtable(embedded, _vref(embeddedmsg, 2; required=true))
    embeddedfeatures, nembeddedfeatures = _vvector(embeddedschema, 3, 8; required=true)
    @assert nembeddedfeatures == 1
    _write_i64!(replacementfeature, Int64(16) + embeddedfeatures, Int64(1))
    replacementfooterlen =
        Int64(reinterpret(Int32, replacementfeature[(end - 9):(end - 6)])[1])
    replacementfooterstart = Int64(length(replacementfeature)) - 10 - replacementfooterlen
    replacementfooter = copy(
        replacementfeature[(replacementfooterstart + 1):(replacementfooterstart + replacementfooterlen)],
    )
    replacementtable = _vtable(replacementfooter, Int64(_vu32(replacementfooter, 0)))
    replacementschema =
        _vtable(replacementfooter, _vref(replacementtable, 1; required=true))
    replacementfeatures, nreplacementfeatures =
        _vvector(replacementschema, 3, 8; required=true)
    @assert nreplacementfeatures == 1
    _write_i64!(replacementfeature, replacementfooterstart + replacementfeatures, Int64(1))
    @assert _rejects(() -> readfile(replacementfeature))
    @assert _rejects(
        () -> _validateblockindex(
            NTuple{3,Int64}[(Int64(304), Int64(16), Int64(0))],
            NTuple{3,Int64}[],
            Int64(312),
        ),
    )
    @assert _rejects(
        () -> _validateblockindex(
            NTuple{3,Int64}[(Int64(8), Int64(16), Int64(8))],
            NTuple{3,Int64}[(Int64(24), Int64(16), Int64(0))],
            Int64(64),
        ),
    )

    # A zero-body Block ends exactly after its metadata. The Message omits its
    # default-zero bodyLength slot, and the frame preflight must accept it.
    zerobodyschema = Schema(Field[])
    zerobodybatch = AC.RecordBatch(zerobodyschema, ArrayData[], 3)
    zerobodyfile = readfile(writefile(zerobodyschema, [zerobodybatch]))
    @assert only(zerobodyfile.recordblocks)[3] == 0
    @assert zerobodyfile[1].nrows == 3

    # The footer copy and verified graph share one allocation budget. File
    # message count and lazy bodies use the same limits as stream framing.
    simplefield, simpledata = fromjulia("x", Int64[1])
    simplebatch = AC.RecordBatch(Schema([simplefield]), [simpledata], 1)
    simplebytes = writefile(simplebatch.schema, [simplebatch])
    simplefooterlen = Int64(reinterpret(Int32, simplebytes[(end - 9):(end - 6)])[1])
    simplefooterstart = Int64(length(simplebytes)) - 10 - simplefooterlen
    simplefooter =
        copy(simplebytes[(simplefooterstart + 1):(simplefooterstart + simplefooterlen)])

    # Keep the message and Block internally consistent while extending the
    # indexed body into the footer. Open must reject the cross-boundary span.
    crossing = copy(simplebytes)
    crossingtable = _vtable(simplefooter, Int64(_vu32(simplefooter, 0)))
    crossingstart, crossingcount = _vvector(crossingtable, 3, 24)
    @assert crossingcount == 1
    crossingoffset = _vi64(simplefooter, crossingstart)
    crossingmeta = Int64(_vi32(simplefooter, crossingstart + 8))
    crossingbody = _vi64(simplefooter, crossingstart + 16)
    crossingmessage = copy(crossing[(crossingoffset + 9):(crossingoffset + crossingmeta)])
    crossingroot = _vtable(crossingmessage, Int64(_vu32(crossingmessage, 0)))
    bodypos = _vfield(crossingroot, 3, 8; required=true)
    newbodylen = crossingbody + 16
    _write_i64!(crossing, crossingoffset + 8 + bodypos, newbodylen)
    _write_i64!(crossing, simplefooterstart + crossingstart + 16, newbodylen)
    @assert _rejects(() -> readfile(crossing))

    # A no-EOS file may end its last data buffer with the eight-byte EOS byte
    # pattern. Indexed block extents, not that ambiguous pattern alone, decide
    # whether those bytes are data. Arrow.jl 2.x writes and accepts no-EOS
    # files, so retain that interoperable form.
    collisionfield, collisiondata = fromjulia("collision", Int64[Int64(0x00000000ffffffff)])
    collisionbatch = AC.RecordBatch(Schema([collisionfield]), [collisiondata], 1)
    collision = writefile(collisionbatch.schema, [collisionbatch])
    collisionfooterlen = Int64(reinterpret(Int32, collision[(end - 9):(end - 6)])[1])
    collisionfooterstart = Int64(length(collision)) - 10 - collisionfooterlen
    noeos = copy(collision)
    deleteat!(noeos, Int(collisionfooterstart - 7):Int(collisionfooterstart))
    noeosfile = readfile(noeos)
    @assert materialize(noeosfile.schema.fields[1], noeosfile[1].columns[1]) ==
            Int64[Int64(0x00000000ffffffff)]

    # Footer extents are not verified until they agree with the on-wire
    # Message envelope. Merely shortening the final Block must not make its
    # marker-shaped data look like an optional EOS marker at file-open time.
    forgedcollision = copy(noeos)
    forgedfooterlen = Int64(reinterpret(Int32, forgedcollision[(end - 9):(end - 6)])[1])
    forgedfooterstart = Int64(length(forgedcollision)) - 10 - forgedfooterlen
    forgedfooter =
        copy(forgedcollision[(forgedfooterstart + 1):(forgedfooterstart + forgedfooterlen)])
    forgedtable = _vtable(forgedfooter, Int64(_vu32(forgedfooter, 0)))
    forgedblocks, nforgedblocks = _vvector(forgedtable, 3, 24; required=true)
    @assert nforgedblocks == 1
    forgedbodylen = _vi64(forgedfooter, forgedblocks + 16)
    @assert forgedbodylen >= 8
    _write_i64!(forgedcollision, forgedfooterstart + forgedblocks + 16, forgedbodylen - 8)
    @assert _rejects(() -> readfile(forgedcollision))

    # Coordinating the same lie in Message.bodyLength is still insufficient:
    # the RecordBatch buffer table proves that the excluded bytes are data.
    coordinated = copy(forgedcollision)
    forgedoffset = _vi64(forgedfooter, forgedblocks)
    forgedmetalen = Int64(_vi32(forgedfooter, forgedblocks + 8))
    forgedmessage = copy(coordinated[(forgedoffset + 9):(forgedoffset + forgedmetalen)])
    forgedmessagetable = _vtable(forgedmessage, Int64(_vu32(forgedmessage, 0)))
    forgedmessagebody = _vfield(forgedmessagetable, 3, 8; required=true)
    _write_i64!(coordinated, forgedoffset + 8 + forgedmessagebody, forgedbodylen - 8)
    @assert _rejects(() -> readfile(coordinated))

    _, _, _, _, footreserve = verify_footer(simplefooter, Limits())
    tightbudget = max(simplefooterlen, footreserve)
    @assert _rejects(
        () -> readfile(
            copy(simplebytes);
            limits=Limits(max_total_allocated_bytes=tightbudget),
        ),
    )
    @assert _rejects(() -> readfile(copy(simplebytes); limits=Limits(max_messages=1)))
    bodylimited = readfile(copy(simplebytes); limits=Limits(max_body_bytes=0))
    @assert _rejects(() -> bodylimited[1])
    # A block offset pointing outside the file must fail cleanly.
    file2 = readfile(copy(filebytes))
    badblocks = [(Int64(2)^40, Int64(16), Int64(0))]
    badfile = ArrowFile(
        file2.region,
        file2.schema,
        file2.fields,
        file2.fielddictids,
        file2.dictionaries,
        file2.validated,
        badblocks,
        file2.dataend,
        file2.limits,
        file2.schemaversion,
    )
    @assert _rejects(() -> badfile[1])
    println("file magic, footer, and block extents are verified ✓")

    # ---- Format 1.3/1.4 layouts: views and run-end encoding ------------
    # No 2.x-written fixture exists for these layouts, so the acceptance is
    # self round-trip on both formats plus wire-shape checks:
    # the variadicBufferCounts vector, the late type tags, and the buffer
    # accounting that skewed nothing after them.
    payload1 = collect(codeunits("first-out-of-line-payload"))
    payload2 = collect(codeunits("second-buffer-payload-here"))
    views = vcat(
        _viewentry(3, collect(codeunits("abc"))),
        _viewlong(25, payload1[1:4], 0, 0),
        _viewlong(26, payload2[1:4], 1, 0),
        _viewentry(0, UInt8[]),
    )
    vt = ViewType(true)
    vf = Field("v", vt; nullable=true)
    vd = ArrayData(
        vt,
        4,
        [
            AC._databuffer(UInt8[0x0b]),
            AC._databuffer(views),
            AC._databuffer(payload1),
            AC._databuffer(payload2),
        ];
        nullcount=1,
    )
    lvt = ListViewType(false)
    lvcf, lvcd = fromjulia("item", Int64[10, 20, 30])
    lvf = Field("lv", lvt; children=[lvcf])
    lvd = ArrayData(
        lvt,
        3,
        [BufferSlice(), AC._databuffer(Int32[2, 0, 0]), AC._databuffer(Int32[1, 2, 3])];
        children=[lvcd],
        nullcount=0,
    )
    rt = RunEndEncodedType()
    ref, red = fromjulia("run_ends", Int32[2, 3, 4])
    rvf, rvd = fromjulia("values", Union{Missing,String}["x", missing, "z"])
    rf = Field("ree", rt; children=[ref, rvf])
    rd = ArrayData(rt, 4, BufferSlice[]; children=[red, rvd], nullcount=0)
    nvv = ArrayData(
        vt,
        2,
        [
            BufferSlice(),
            AC._databuffer(
                vcat(
                    _viewentry(1, collect(codeunits("p"))),
                    _viewentry(1, collect(codeunits("q"))),
                ),
            ),
        ];
        nullcount=0,
    )
    nvf = Field("values", vt; nullable=false)
    nirf, nird = fromjulia("run_ends", Int32[1, 2])
    nif = Field("values", rt; children=[nirf, nvf])
    nid = ArrayData(rt, 2, BufferSlice[]; children=[nird, nvv], nullcount=0)
    norf, nord = fromjulia("run_ends", Int32[2, 4])
    nf = Field("nested", rt; children=[norf, nif])
    nd = ArrayData(rt, 4, BufferSlice[]; children=[nord, nid], nullcount=0)
    # 64-bit-offset utf8/binary: the only IPC path exercising the LargeUtf8/
    # LargeBinary metadata tables — the only coverage of those generated
    # builders.
    luf = Field("lu", Utf8Type(true); nullable=false)
    lud = ArrayData(
        Utf8Type(true),
        4,
        [
            BufferSlice(),
            AC._databuffer(Int64[0, 1, 1, 3, 6]),
            AC._databuffer(collect(codeunits("abcdef"))),
        ];
        nullcount=0,
    )
    # a plain column AFTER the exotic ones proves no buffer skew
    tf, td = fromjulia("tail", Int64[1, 2, 3, 4])
    exsch = Schema(Field[vf, lvf, rf, nf, luf, tf])
    exlv = ArrayData(
        lvt,
        4,
        [
            BufferSlice(),
            AC._databuffer(Int32[2, 0, 0, 1]),
            AC._databuffer(Int32[1, 2, 3, 0]),
        ];
        children=[lvcd],
        nullcount=0,
    )
    exbatch = AC.RecordBatch(exsch, ArrayData[vd, exlv, rd, nd, lud, td], 4)
    exwant = Dict(
        "v" => Any["abc", "first-out-of-line-payload", missing, ""],
        "lv" => Any[[30], [10, 20], [10, 20, 30], Int64[]],
        "ree" => Any["x", "x", missing, "z"],
        "nested" => Any["p", "p", "q", "q"],
        "lu" => Any["a", "", "bc", "def"],
        "tail" => Any[1, 2, 3, 4],
    )
    for compress in (:none, :zstd)
        exbytes = writestream(exsch, [exbatch]; compress=compress)
        exstream = readstream(exbytes)
        for (i, f) in enumerate(exstream.schema.fields)
            @assert AC.typeequal(f.type, exsch.fields[i].type)
            got = collect(Any, materialize(f, exstream.batches[1].columns[i]))
            @assert isequal(got, exwant[f.name]) "$(f.name) ($compress): $got"
        end
        exfile = readfile(writefile(exsch, [exbatch]; compress=compress))
        for (i, f) in enumerate(exfile.schema.fields)
            got = collect(Any, materialize(f, exfile[1].columns[i]))
            @assert isequal(got, exwant[f.name]) "file $(f.name) ($compress): $got"
        end
    end
    println("views, list-views, and nested REE round-trip on both formats (plain + zstd) ✓")

    # Wire shape: variadic counts follow field preorder (1 buffer for the
    # top-level view: the second input buffer is referenced only by a null;
    # then 0 for the inline view below nested REE). The type tags are the
    # 1.3/1.4 ids.
    exframes = framemessages(heapregion(copy(writestream(exsch, [exbatch]))))
    exrb = exframes[2].msg.header::Meta.RecordBatch
    @assert variadiccounts(exrb) == Int64[1, 0]
    exmeta = exframes[1].msg.header::Meta.Schema
    @assert [typeof(f.type) for f in exmeta.fields] == [
        Meta.Utf8View,
        Meta.ListView,
        Meta.RunEndEncoded,
        Meta.RunEndEncoded,
        Meta.LargeUtf8,
        Meta.Int,
    ]
    println("variadic counts and 1.3/1.4 type tags are on the wire ✓")

    # A view column with ZERO variadic buffers (all inline) is legal and
    # round-trips with an explicit 0 count.
    inl = ArrayData(
        vt,
        2,
        [
            BufferSlice(),
            AC._databuffer(
                vcat(
                    _viewentry(2, collect(codeunits("hi"))),
                    _viewentry(1, collect(codeunits("!"))),
                ),
            ),
        ];
        nullcount=0,
    )
    inlsch = Schema(Field[Field("v", vt)])
    inlstream = readstream(writestream(inlsch, [AC.RecordBatch(inlsch, ArrayData[inl], 2)]))
    @assert materialize(inlstream.schema.fields[1], inlstream.batches[1].columns[1]) ==
            ["hi", "!"]
    println("all-inline views carry an explicit zero variadic count ✓")

    # Corrupt variadic counts fail closed: overstated (consumes into the
    # tail column's buffers → skew caught) and understated (leftover buffers).
    exraw = writestream(exsch, [exbatch])
    for lie in (Int64(2), Int64(0))
        lied = copy(exraw)
        _mutatemessage!(lied, 2) do meta, msg
            rb = _headertable(meta, msg)
            start, n = _vvector(rb, 4, 8; required=true)
            n == 2 || error("fixture declares $n variadic counts")
            _write_i64!(meta, start, lie)
        end
        @assert _rejects(() -> readstream(lied)) "variadic lie $lie accepted"
    end
    println("misdeclared variadic counts are rejected as skew ✓")

    println()
    println("IPC write, file-format, interop, and adversarial checks passed.")
end
