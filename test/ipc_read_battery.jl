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
# Acceptance: 2.x writes, Core reads
# ---------------------------------------------------------------------------

function ipc_read_battery()
    hostgate = try
        _framemessages(heapregion(UInt8[]), Limits(), UInt32(0x01020304))
        false
    catch e
        e isa ValidationError && occursin("little-endian host", e.msg)
    end
    @assert hostgate
    println("unsupported hosts fail before generated metadata getters ✓")

    emptybuffers = readstream(_misaligned_empty_buffers_stream())
    @assert emptybuffers.batches[1].nrows == 0
    @assert isempty(materialize(emptybuffers.schema.fields[1],
        emptybuffers.batches[1].columns[1]))
    println("empty struct vectors need no nominal element alignment ✓")

    @assert _rejects(() -> readstream(_misaligned_empty_children_stream()))
    println("vector length words are aligned before generated getters ✓")

    emptyvalue = readstream(_metadata_value_stream(true))
    @assert collect(emptyvalue.schema.metadata) == ["owner" => ""]
    @assert _rejects(() -> readstream(_metadata_value_stream(false)))
    println("metadata values are present, including explicit empty strings ✓")

    expected = MIXED_EXPECTED
    bytes = _mixed_two_partitions_bytes()

    stream = readstream(bytes)
    @assert length(stream.batches) == 2

    dictpos = findfirst(f -> f.type isa DictionaryType, stream.schema.fields)
    dictpos === nothing && error("acceptance stream has no dictionary field")
    dictfield = stream.schema.fields[dictpos]
    dictpool = stream.batches[1].columns[dictpos].dictionary
    @assert dictpool === stream.batches[2].columns[dictpos].dictionary
    validated = AC._ValidatedDictionaries()
    validate_semantic(AC.dictvaluefield(dictfield, dictfield.type), dictpool)
    validated[dictpool] = nothing
    for b in stream.batches
        validaterecordcolumns(stream.schema.fields, b.columns, validated)
    end
    @assert length(validated) == 1

    wanted = (
        ints=Any[1, 2, 3, 4, 5],
        floats=Any[1.5, missing, 3.5, missing, 5.5],
        bools=Any[true, false, true, missing, false],
        strs=Any["hey", "", missing, "αβ∀", "last"],
        lists=Any[[1, 2], Int64[], [3], missing, [4, 5, 6]],
        # Core struct scalars are ordered pairs; the 2.x-written fixture
        # above was fed NamedTuples.
        structs=Any[["a" => 1, "b" => "x"], ["a" => 2, "b" => "y"],
            ["a" => 3, "b" => "z"], ["a" => 4, "b" => "w"], ["a" => 5, "b" => "v"]],
        dict=Any["lo", "hi", "lo", missing, "hi"],
    )
    for b in stream.batches
        for (i, f) in enumerate(stream.schema.fields)
            got = materialize(f, b.columns[i])
            want = wanted[Symbol(f.name)]
            @assert isequal(collect(Any, got), want) "column $(f.name): got $got, want $want"
        end
    end
    println("all columns round-tripped through ArrowCore ✓")

    # Compressed acceptance: the same table, written by 2.x with each codec
    # (dictionary batches are compressed too), read back through Core. The
    # per-buffer Int64 prefix is bounded before allocation, the decompressed
    # size must match the declaration, and every decompressed buffer lives in
    # its own exact-sized owned region.
    for (codecname, kw) in (("lz4", :lz4), ("zstd", :zstd))
        cbytes = _fixture2x("mixed-two-partitions-$(codecname)") do
            cio = IOBuffer()
            cwritetable = merge(expected, (dict=Arrow.DictEncode(expected.dict),))
            Arrow.write(cio, Tables.partitioner([cwritetable, cwritetable]);
                file=false, compress=kw)
            take!(cio)
        end
        cstream = readstream(cbytes)
        @assert length(cstream.batches) == 2
        for b in cstream.batches
            for (i, f) in enumerate(cstream.schema.fields)
                got = materialize(f, b.columns[i])
                want = wanted[Symbol(f.name)]
                @assert isequal(collect(Any, got), want) "compressed $(codecname) column $(f.name): got $got"
            end
        end
        println("$(codecname)-compressed stream (incl. dictionary batches) decodes ✓")

        # Adversarial prefix manipulation, located via the framer itself:
        # find the first record batch's first nonempty buffer and rewrite its
        # Int64 uncompressed-length prefix in the raw bytes.
        prefixpos = let
            region = heapregion(copy(cbytes))
            msgs = framemessages(region, Limits())
            pos = Int64(-1)
            for fm in msgs
                fm.header_type == UInt8(3) || continue   # RecordBatch
                rb = fm.msg.header::Meta.RecordBatch
                for mb in rb.buffers
                    if mb.length > 0
                        pos = fm.body.offset + Int64(mb.offset)
                        break
                    end
                end
                pos >= 0 && break
            end
            @assert pos >= 0 "no nonempty compressed buffer found"
            pos
        end
        # (a) a hostile declared length is rejected BEFORE any allocation
        lying = copy(cbytes)
        lying[prefixpos+1:prefixpos+8] .= reinterpret(UInt8, [Int64(2)^61])
        @assert _rejects(() -> readstream(lying))
        println("$(codecname): hostile decompressed-length prefix rejected before allocation ✓")
        # (b) a prefix that understates the payload is a mismatch error, not
        # silent truncation
        short = copy(cbytes)
        short[prefixpos+1:prefixpos+8] .= reinterpret(UInt8, [Int64(1)])
        @assert _rejects(() -> readstream(short))
        println("$(codecname): declared/actual decompressed-size mismatch rejected ✓")
    end

    # Direct codec-boundary regressions. The destination is exactly the
    # declared size, so a compressed bomb cannot force a larger allocation.
    for (codecname, codec, compressor) in (
        ("lz4", CODEC_LZ4_FRAME, Arrow.LZ4FrameCompressor),
        ("zstd", CODEC_ZSTD, Arrow.ZstdCompressor),
    )
        emptyframe = transcode(compressor, UInt8[])
        @assert isempty(_decode_fixture(codec, emptyframe, 0))
        oneframe = transcode(compressor, UInt8[0x41])
        @assert _rejects(() -> _decode_fixture(codec, oneframe, 0))
        @assert _rejects(() -> _decode_fixture(codec, UInt8[], 0))
        @assert _decode_fixture(codec, UInt8[0x41, 0x42], -1) ==
            UInt8[0x41, 0x42]

        bomb = transcode(compressor, zeros(UInt8, 1024 * 1024))
        @assert _rejects(() -> _decode_fixture(codec, bomb, 1; budget=1))
        if codec == CODEC_LZ4_FRAME
            for n = 1:3
                @assert _rejects(() ->
                    _decode_fixture(codec, emptyframe[1:(end - n)], 0))
            end
            second = transcode(compressor, UInt8[0x42])
            @assert _rejects(() ->
                _decode_fixture(codec, vcat(oneframe, second), 2))
        else
            @assert _rejects(() ->
                _decode_fixture(codec, oneframe[1:(end - 1)], 1))
        end
        println("$(codecname): empty, truncated, and bounded-output frames are checked ✓")
    end

    # A corrupt LZ4 frame must not erase the native pointer before reader
    # cleanup. CodecLz4's streaming wrapper does erase it on this error, so
    # the adapter owns the raw context and frees it directly.
    badstate = DecodeState(AllocationBudget(0))
    badbytes = _compressed_wire(UInt8[0x01, 0x02, 0x03], 0)
    badwire = BufferSlice(heapregion(badbytes), 0, length(badbytes))
    badcursor = DecodeCursor(nothing, nothing, BufferSlice(), Limits();
        codec=CODEC_LZ4_FRAME, state=badstate)
    try
        @assert _rejects(() -> _decompressbuffer!(badcursor, badwire))
        @assert badstate.lz4 != C_NULL
    finally
        close(badstate)
    end
    @assert badstate.lz4 == C_NULL
    println("corrupt LZ4 frames retain their context until explicit cleanup ✓")

    # The schema feature is standard in V5. Arrow.jl 2.x omits it from its
    # compressed output, which this adapter accepts for compatibility. A
    # standards-conforming stream that declares it must also be accepted.
    simplebytes = _fixture2x("int64-three-zstd") do
        simpleio = IOBuffer()
        Arrow.write(simpleio, (x=Int64[1, 2, 3],); file=false, compress=:zstd)
        take!(simpleio)
    end
    simpleframes = _frameinfo(simplebytes)
    standardschema = _int64_schema_stream(Int64[2])
    resize!(standardschema, length(standardschema) - 8)
    standardbytes = vcat(standardschema,
        simplebytes[only(f.frame for f in simpleframes if f.kind == UInt8(3))],
        simplebytes[only(f.frame for f in simpleframes if f.kind == UInt8(0))])
    standardstream = readstream(standardbytes)
    @assert materialize(standardstream.schema.fields[1],
        standardstream.batches[1].columns[1]) == Any[1, 2, 3]

    v4compressed = copy(simplebytes)
    for (i, frame) in pairs(simpleframes)
        frame.kind == UInt8(0) && continue
        _mutatemessage!(v4compressed, i) do meta, msg
            _write_i16!(meta, _vfield(msg, 0, 2; required=true), Int16(3))
        end
    end
    @assert _rejects(() -> readstream(v4compressed))
    println("COMPRESSED_BODY is accepted in V5 and BodyCompression is rejected in V4 ✓")

    # The allocation limit is reader-wide. It does not reset for each eager
    # batch retained by IPCStream.
    large = (x=zeros(Int64, 10_000),)
    onebytes = _fixture2x("large-zeros-zstd") do
        oneio = IOBuffer()
        Arrow.write(oneio, large; file=false, compress=:zstd)
        take!(oneio)
    end
    aggregate_limit = Limits(max_total_allocated_bytes=100_000)
    @assert length(readstream(onebytes; limits=aggregate_limit).batches) == 1
    twobytes = _fixture2x("large-zeros-zstd-two-partitions") do
        twoio = IOBuffer()
        Arrow.write(twoio, Tables.partitioner([large, large]);
            file=false, compress=:zstd)
        take!(twoio)
    end
    @assert _rejects(() -> readstream(twobytes; limits=aggregate_limit))
    println("metadata and decompressed bytes share one reader-wide budget ✓")

    for kw in (:lz4, :zstd)
        emptycompressed = _fixture2x("int64-empty-$(kw)") do
            emptyio = IOBuffer()
            Arrow.write(emptyio, (x=Int64[],); file=false, compress=kw)
            take!(emptyio)
        end
        emptystream = readstream(emptycompressed)
        @assert isempty(materialize(emptystream.schema.fields[1],
            emptystream.batches[1].columns[1]))
    end
    println("zero-byte compressed buffers may omit the prefix ✓")

    # The 2.x writer permits a coefficient outside its declared decimal
    # precision. Precision is advisory at the semantic boundary (the gold
    # corpus itself carries five digits in a decimal(3,2)); the opt-in
    # validate_full tier enforces the declaration.
    baddecbytes = _fixture2x("decimal-over-precision") do
        baddecimalio = IOBuffer()
        D = Arrow.Decimal{Int32(1),Int32(0),Int128}
        Arrow.write(baddecimalio, (d=D[D(Int128(10))],); file=false)
        take!(baddecimalio)
    end
    baddec = readstream(baddecbytes)
    @assert _rejects(() -> AC.validate_full(baddec.schema.fields[1],
        baddec.batches[1].columns[1]))
    println("decimal coefficients outside declared precision are validate_full's ✓")

    pulled = readstream(bytes)
    @assert nextbatch!(pulled) isa RecordBatch
    @assert nextbatch!(pulled) isa RecordBatch
    @assert nextbatch!(pulled) === nothing
    println("RecordBatchSource pull protocol works ✓")

    # Framing limits actually bite: a 16-byte body cap rejects this stream
    # BEFORE any decode work happens.
    caught = try
        readstream(bytes; limits=Limits(max_body_bytes=16))
        false
    catch e
        e isa ValidationError
    end
    @assert caught
    println("stage-1 resource limits reject oversized bodies ✓")
    @assert _rejects(() -> readstream(bytes;
        limits=Limits(max_buffer_bytes=1)))
    @assert _rejects(() -> readstream(bytes;
        limits=Limits(max_total_allocated_bytes=1)))
    nmessages = length(framemessages(heapregion(bytes)))
    @assert length(readstream(bytes;
        limits=Limits(max_messages=nmessages)).batches) == 2
    println("buffer, allocation, and exact message-count limits work ✓")

    # Legal FlatBuffer aliasing must not amplify a small metadata message
    # into an unbounded Core schema or repeated large String copies.
    aliased = _aliased_field_stream(14)
    @assert _rejects(() -> readstream(aliased;
        limits=Limits(max_metadata_objects=100)))
    sharedname = _shared_name_stream(10, 50_000)
    @assert _rejects(() -> readstream(sharedname;
        limits=Limits(max_total_allocated_bytes=200_000,
            max_metadata_objects=1_000)))
    println("logical metadata expansion and repeated strings are budgeted ✓")

    # Truncation semantics, both halves of the append rule:
    # (a) losing only the 8-byte EOS block = boundary truncation, ACCEPTED
    #     (the stream ends after its last complete message);
    # (b) losing bytes of a message body = corruption, a clean framing error
    #     — never a silent empty/short stream and never
    #     an aliased read.
    boundary = readstream(bytes[1:(end - 8)])
    @assert length(boundary.batches) == 2
    println("boundary truncation (missing EOS) tolerated by design ✓")
    caught = try
        readstream(bytes[1:(end - 100)])
        false
    catch e
        e isa ValidationError
    end
    @assert caught
    println("mid-body truncation is a framing error, not a silent short read ✓")

    # A partial next prefix is corruption. An explicit EOS consumes the exact
    # stream, so any bytes after it are also rejected.
    for n = 1:7
        @assert _rejects(() -> readstream(bytes[1:(end - n)]))
    end
    @assert _rejects(() -> readstream(vcat(bytes, UInt8[0x01])))
    println("partial EOS and trailing junk are rejected ✓")

    # Mutate metadata in place to pin verifier and decoder boundaries.
    frames = _frameinfo(bytes)
    recordidx = findfirst(x -> x.kind == 3, frames)
    dictidx = findfirst(x -> x.kind == 2, frames)

    corrupt = copy(bytes)
    _mutatemessage!(corrupt, 1) do meta, msg
        schema = _headertable(meta, msg)
        vecp = _vref(schema, 1; required=true)
        _write_u32!(meta, vecp, UInt32(1_000_001))
    end
    @assert _rejects(() -> readstream(corrupt;
        limits=Limits(max_metadata_objects=1_000_000)))

    oldversion = copy(bytes)
    _mutatemessage!(oldversion, 1) do meta, msg
        _write_i16!(meta, _vfield(msg, 0, 2; required=true), Int16(2)) # V3
    end
    @assert _rejects(() -> readstream(oldversion))

    mixedversion = copy(bytes)
    _mutatemessage!(mixedversion, recordidx) do meta, msg
        _write_i16!(meta, _vfield(msg, 0, 2; required=true), Int16(3)) # V4
    end
    @assert _rejects(() -> readstream(mixedversion))
    println("FlatBuffer bounds and metadata versions are verified ✓")

    # Arrow 0.17 V4 used Message custom metadata for its experimental
    # compression marker. The body below is a real length-prefixed LZ4 frame;
    # it must fail closed instead of exposing that prefix as an Int64 value.
    @assert _rejects(() -> readstream(_experimental_v4_stream(Int64(42))))
    println("legacy V4 compression is rejected before body decoding ✓")

    bigendian = copy(bytes)
    _mutatemessage!(bigendian, 1) do meta, msg
        schema = _headertable(meta, msg)
        p = _vfield(schema, 0, 2)
        if p === nothing
            # The default Little value is omitted. The generated object has
            # two padding bytes after its fields reference; publish that slot.
            off = schema.olen - 2
            off >= 4 || error("schema table has no endian slot storage")
            _writele!(meta, schema.vpos + 4, UInt64(off), 2)
            p = schema.pos + off
        end
        _write_i16!(meta, p, Int16(1))
    end
    @assert _rejects(() -> readstream(bigendian))

    badschema = copy(bytes)
    _mutatemessage!(badschema, 1) do meta, msg
        schema = _headertable(meta, msg)
        fieldsvec = _vvector(schema, 1, 4; required=true)
        start, _ = fieldsvec
        firstfield = _vtable(meta, start + Int64(_vu32(meta, start)))
        inttype = _vtable(meta, _vref(firstfield, 3; required=true))
        _write_i32!(meta, _vfield(inttype, 0, 4; required=true), Int32(24))
    end
    @assert _rejects(() -> readstream(badschema))

    badutf8 = copy(bytes)
    _mutatemessage!(badutf8, 1) do meta, msg
        schema = _headertable(meta, msg)
        start, n = _vvector(schema, 1, 4; required=true)
        n > 0 || error("schema fixture has no fields")
        firstfield = _vtable(meta, start + Int64(_vu32(meta, start)))
        name = _vref(firstfield, 0; required=true)
        _vu32(meta, name) > 0 || error("schema fixture has an empty field name")
        meta[name + 5] = 0xff
    end
    @assert _rejects(() -> readstream(badutf8))
    println("endianness and schema descriptors are checked before batches ✓")

    # Zero is the FlatBuffers scalar default and may be omitted. Both widths
    # are valid Arrow descriptors, including schema-only streams.
    fsb = readstream(_zero_width_schema_stream(false))
    @assert fsb.schema.fields[1].type == FixedSizeBinaryType(0)
    fsl = readstream(_zero_width_schema_stream(true))
    @assert fsl.schema.fields[1].type == FixedSizeListType(0)
    println("omitted zero-width fixed-size defaults are accepted ✓")

    badbody = copy(bytes)
    _mutatemessage!(badbody, recordidx) do meta, msg
        _write_i64!(meta, _vfield(msg, 3, 8; required=true), Int64(17))
    end
    @assert _rejects(() -> readstream(badbody))

    badrowcount = copy(bytes)
    _mutatemessage!(badrowcount, recordidx) do meta, msg
        rb = _headertable(meta, msg)
        _write_i64!(meta, _vfield(rb, 0, 8; required=true), Int64(999))
    end
    @assert _rejects(() -> readstream(badrowcount))

    negativebuffer = copy(bytes)
    _mutatemessage!(negativebuffer, recordidx) do meta, msg
        rb = _headertable(meta, msg)
        start, _ = _vvector(rb, 2, 16; required=true)
        _write_i64!(meta, start, Int64(-16))
    end
    @assert _rejects(() -> readstream(negativebuffer))

    overlap = _fixture2x("two-int64-columns") do
        overlapio = IOBuffer()
        Arrow.write(overlapio, (x=Int64[1], y=Int64[2]); file=false)
        take!(overlapio)
    end
    overlaprecord = findfirst(x -> x.kind == 3, _frameinfo(overlap))
    _mutatemessage!(overlap, overlaprecord) do meta, msg
        rb = _headertable(meta, msg)
        start, n = _vvector(rb, 2, 16; required=true)
        n >= 4 || error("overlap fixture has fewer than four buffers")
        _write_i64!(meta, start + 3 * 16, Int64(0))
    end
    @assert _rejects(() -> readstream(overlap))
    println("body alignment, non-overlap, row counts, and body authority are pinned ✓")

    # A dictionary batch must consume its entire node/buffer declaration.
    wrongdict = copy(bytes)
    _mutatemessage!(wrongdict, 1) do meta, msg
        schema = _headertable(meta, msg)
        start, n = _vvector(schema, 1, 4; required=true)
        for i = 0:(n - 1)
            ep = start + 4i
            field = _vtable(meta, ep + Int64(_vu32(meta, ep)))
            _vref(field, 4) === nothing && continue
            tagp = _vfield(field, 2, 1; required=true)
            meta[tagp + 1] = UInt8(6) # Utf8 value type -> Bool
            break
        end
    end
    @assert _rejects(() -> readstream(wrongdict))

    # A repeated full dictionary is replacement. It is legal only when the
    # schema declares DICTIONARY_REPLACEMENT in its features vector.
    dictidx === nothing && error("acceptance stream has no dictionary batch")
    spans = _frameinfo(bytes)
    duplicate = vcat(bytes[1:last(spans[dictidx].frame)],
        bytes[spans[dictidx].frame],
        bytes[(last(spans[dictidx].frame) + 1):end])
    @assert _rejects(() -> readstream(duplicate))

    replaced = readstream(_dictionary_replacement_stream())
    @assert length(replaced.batches) == 2
    df = replaced.schema.fields[1]
    @assert materialize(df, replaced.batches[1].columns[1]) == ["aa", "bb", "aa"]
    @assert materialize(df, replaced.batches[2].columns[1]) == ["xx", "yy", "xx"]
    @assert replaced.batches[1].columns[1].dictionary !==
        replaced.batches[2].columns[1].dictionary
    println("dictionary replacement is feature-gated and snapshots stay immutable ✓")

    nestedvals = [[Int64(1), 2], [3]]
    sharedbytes = _fixture2x("shared-nested-dict") do
        sharedio = IOBuffer()
        Arrow.write(sharedio,
            (a=Arrow.DictEncode(nestedvals, 7), b=Arrow.DictEncode(nestedvals, 7));
            file=false)
        take!(sharedio)
    end
    sharedstream = readstream(sharedbytes)
    for i = 1:2
        @assert materialize(sharedstream.schema.fields[i],
            sharedstream.batches[1].columns[i]) == nestedvals
    end
    sharedcols = sharedstream.batches[1].columns
    @assert sharedcols[1].dictionary === sharedcols[2].dictionary
    sharedpool = sharedcols[1].dictionary
    sharedtype = sharedstream.schema.fields[1].type::DictionaryType
    validate_semantic(AC.dictvaluefield(sharedstream.schema.fields[1], sharedtype),
        sharedpool)
    sharedvalidated = AC._ValidatedDictionaries(sharedpool => nothing)
    validaterecordcolumns(sharedstream.schema.fields, sharedcols, sharedvalidated)
    @assert length(sharedvalidated) == 1
    println("shared dictionary ids reuse one full pool certificate ✓")

    poolbytes = _fixture2x("pooled-view-dict") do
        pool = PooledArray(Union{Missing,String}[missing, "x"])
        poolio = IOBuffer()
        Arrow.write(poolio, (d=Arrow.DictEncode(view(pool, 2:2)),); file=false)
        take!(poolio)
    end
    _mutatemessage!(poolbytes, 1) do meta, msg
        schema = _headertable(meta, msg)
        start, n = _vvector(schema, 1, 4; required=true)
        for i = 0:(n - 1)
            ep = start + 4i
            field = _vtable(meta, ep + Int64(_vu32(meta, ep)))
            _vref(field, 4) === nothing && continue
            nullable = _vfield(field, 1, 1; required=true)
            meta[nullable + 1] = 0x00
            return
        end
        error("dictionary fixture has no dictionary field")
    end
    poolstream = readstream(poolbytes)
    @assert materialize(poolstream.schema.fields[1],
        poolstream.batches[1].columns[1]) == ["x"]
    println("dictionary pool nullability is independent from index fields ✓")

    nullvalues = Union{Missing,String}[missing, missing]
    nullbytes = _fixture2x("all-null-dict") do
        nullio = IOBuffer()
        Arrow.write(nullio, (d=Arrow.DictEncode(nullvalues),); file=false)
        take!(nullio)
    end
    nullframes = _frameinfo(nullbytes)
    nschema = findfirst(x -> x.kind == 1, nullframes)
    ndict = findfirst(x -> x.kind == 2, nullframes)
    nrecord = findfirst(x -> x.kind == 3, nullframes)
    neos = findfirst(x -> x.kind == 0, nullframes)
    all(x -> x !== nothing, (nschema, ndict, nrecord, neos)) ||
        error("all-null dictionary fixture has unexpected framing")
    reordered = vcat(nullbytes[nullframes[nschema].frame],
        nullbytes[nullframes[nrecord].frame],
        nullbytes[nullframes[ndict].frame],
        nullbytes[nullframes[neos].frame])
    nullstream = readstream(reordered)
    @assert isequal(materialize(nullstream.schema.fields[1],
        nullstream.batches[1].columns[1]), nullvalues)
    println("all-null dictionary references may precede their dictionary ✓")

    # The 2.x writer omits Map.keysSorted when false. The generated getter
    # returns `nothing`; the adapter must apply the FlatBuffers default.
    mapbytes = _fixture2x("map-default-keyssorted") do
        mapio = IOBuffer()
        Arrow.write(mapio, (m=[Dict("a" => Int64(1))],); file=false)
        take!(mapio)
    end
    mapstream = readstream(mapbytes)
    mf = mapstream.schema.fields[1]
    @assert mf.type == MapType(false)
    @assert materialize(mf, mapstream.batches[1].columns[1]) == [["a" => 1]]
    println("valid 2.x Map streams decode with default keysSorted=false ✓")

    emptybytes = _fixture2x("int64-three") do
        emptyio = IOBuffer()
        Arrow.write(emptyio, (x=Int64[1, 2, 3],); file=false)
        take!(emptyio)
    end
    emptyframes = _frameinfo(emptybytes)
    emptyrecord = findfirst(x -> x.kind == 3, emptyframes)
    emptyrecord === nothing && error("empty-schema fixture has no record batch")
    _mutatemessage!(emptybytes, 1) do meta, msg
        schema = _headertable(meta, msg)
        fieldsref = _vref(schema, 1; required=true)
        _write_u32!(meta, fieldsref, UInt32(0))
    end
    _mutatemessage!(emptybytes, emptyrecord) do meta, msg
        rb = _headertable(meta, msg)
        nodesref = _vref(rb, 1; required=true)
        buffersref = _vref(rb, 2; required=true)
        _write_u32!(meta, nodesref, UInt32(0))
        _write_u32!(meta, buffersref, UInt32(0))
    end
    emptystream = readstream(emptybytes)
    @assert isempty(emptystream.schema.fields)
    @assert emptystream.batches[1].nrows == 3
    missingfields = copy(emptybytes)
    _mutatemessage!(missingfields, 1) do meta, msg
        schema = _headertable(meta, msg)
        _write_i16!(meta, schema.vpos + 6, Int16(0)) # omit fields vtable slot
    end
    @assert _rejects(() -> readstream(missingfields))
    toolong = copy(emptybytes)
    _mutatemessage!(toolong, emptyrecord) do meta, msg
        rb = _headertable(meta, msg)
        _write_i64!(meta, _vfield(rb, 0, 8; required=true), typemax(Int64))
    end
    @assert _rejects(() -> readstream(toolong;
        limits=Limits(max_array_length=1)))
    println("zero-column batches retain their explicit row count ✓")

    emptyrecordbytes = _fixture2x("int64-empty") do
        emptyrecordio = IOBuffer()
        Arrow.write(emptyrecordio, (x=Int64[],); file=false)
        take!(emptyrecordio)
    end
    emptyrecordstream = readstream(emptyrecordbytes)
    @assert emptyrecordstream.batches[1].nrows == 0
    @assert isempty(materialize(emptyrecordstream.schema.fields[1],
        emptyrecordstream.batches[1].columns[1]))

    emptydictbytes = _fixture2x("empty-dict") do
        emptydictio = IOBuffer()
        Arrow.write(emptydictio, (d=Arrow.DictEncode(String[]),); file=false)
        take!(emptydictio)
    end
    emptydictstream = readstream(emptydictbytes)
    @assert emptydictstream.batches[1].nrows == 0
    @assert isempty(materialize(emptydictstream.schema.fields[1],
        emptydictstream.batches[1].columns[1]))
    println("omitted zero-length record and dictionary lengths use defaults ✓")

    metabytes2x = _fixture2x("schema-field-metadata") do
        metaio = IOBuffer()
        Arrow.write(metaio, (x=Int64[1],); file=false,
            metadata=Dict("owner" => "jacob"),
            colmetadata=Dict(:x => Dict("unit" => "count")))
        take!(metaio)
    end
    metastream = readstream(metabytes2x)
    @assert Dict(metastream.schema.metadata) == Dict("owner" => "jacob")
    @assert Dict(metastream.schema.fields[1].metadata) == Dict("unit" => "count")
    println("schema and field metadata are preserved ✓")
    println()
    println("IPC framing, verification, decoding, and adversarial checks passed.")
end

