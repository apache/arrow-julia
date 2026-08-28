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
# The IPC writer, plus the file-format reader (`readfile`), over ArrowCore.
#
#   * Batch encode is the inverse of decode, one implementation: ONE generic
#     `encodefield!` walks the SAME `layoutspec` registry the decoder walks —
#     node, registry buffers in registry order, children in declared order.
#     There are no per-layout write methods to skew against the read side.
#
#   * Dictionary state machine, replacement-on-change: each batch's pools
#     are captured by identity. A dictionary batch is emitted before the
#     first record batch that references its id and again ONLY when a later
#     batch's pool for that id is a different snapshot. Replacement streams
#     declare Feature.DICTIONARY_REPLACEMENT in the schema.
#
#   * Compression at encode: per-buffer LZ4_FRAME/ZSTD with the Int64
#     uncompressed-length prefix, the `-1` stored-raw fallback when
#     compression does not help, codec objects owned per writer and
#     explicitly finalized. Compressed streams declare Feature.COMPRESSED_BODY;
#     the reader accepts compressed batches with or without the declaration.
#
#   * File format = stream framing + a Block index + a Footer: the writer
#     isolates footer bookkeeping from generic message writing; `readfile`
#     exposes the footer's record-batch index as a lazy random-access handle
#     (`length`/`getindex`) over one borrowed or mmapped region.
# =============================================================================

# ---------------------------------------------------------------------------
# Encode-side codec state: per-writer objects, explicitly finalized (both
# codecs share TranscodingStreams' one streams API)
# ---------------------------------------------------------------------------

mutable struct EncodeState
    lz4::Union{Nothing,LZ4FrameCompressor}
    zstd::Union{Nothing,ZstdCompressor}
end
EncodeState() = EncodeState(nothing, nothing)

function _lz4c!(s::EncodeState)
    if s.lz4 === nothing
        c = LZ4FrameCompressor()
        TS.initialize(c)
        s.lz4 = c
    end
    return s.lz4::LZ4FrameCompressor
end

function _zstdc!(s::EncodeState)
    if s.zstd === nothing
        c = ZstdCompressor()
        TS.initialize(c)
        s.zstd = c
    end
    return s.zstd::ZstdCompressor
end

function Base.close(s::EncodeState)
    lz4 = s.lz4
    s.lz4 = nothing
    try
        lz4 === nothing || TS.finalize(lz4)
    finally
        zstd = s.zstd
        s.zstd = nothing
        zstd === nothing || TS.finalize(zstd)
    end
    return nothing
end

# ---------------------------------------------------------------------------
# Metadata building: Core descriptors -> Meta tables (inverse of `coretype`)
# ---------------------------------------------------------------------------

_metatimeunit(u) =
    u == AC.SECOND ? Meta.TimeUnit.SECOND :
    u == AC.MILLISECOND ? Meta.TimeUnit.MILLISECOND :
    u == AC.MICROSECOND ? Meta.TimeUnit.MICROSECOND : Meta.TimeUnit.NANOSECOND

"""
Build the flatbuffer TYPE table for one Core descriptor. Returns
`(tag type, table offset)` for `fieldAddTypeType`/`fieldAddType`. The isa
ladder is the encode half of `coretype`; a descriptor outside the mapped set
is a clean writer refusal, mirroring the reader's refusal of unmapped tags.
"""
function metatype!(b::FB.Builder, t::ArrowType)
    if t isa IntType
        Meta.intStart(b)
        Meta.intAddBitWidth(b, Int32(t.bits))
        Meta.intAddIsSigned(b, t.signed)
        return Meta.Int, Meta.intEnd(b)
    elseif t isa FloatType
        Meta.floatingPointStart(b)
        Meta.floatingPointAddPrecision(
            b,
            t.bits == 16 ? Meta.Precision.HALF :
            t.bits == 32 ? Meta.Precision.SINGLE : Meta.Precision.DOUBLE,
        )
        return Meta.FloatingPoint, Meta.floatingPointEnd(b)
    elseif t isa BoolType
        Meta.boolStart(b)
        return Meta.Bool, Meta.boolEnd(b)
    elseif t isa Utf8Type
        if t.large
            Meta.largeUtf8Start(b)
            return Meta.LargeUtf8, Meta.largeUtf8End(b)
        end
        Meta.utf8Start(b)
        return Meta.Utf8, Meta.utf8End(b)
    elseif t isa BinaryType
        if t.large
            Meta.largeBinaryStart(b)
            return Meta.LargeBinary, Meta.largeBinaryEnd(b)
        end
        Meta.binaryStart(b)
        return Meta.Binary, Meta.binaryEnd(b)
    elseif t isa FixedSizeBinaryType
        Meta.fixedSizeBinaryStart(b)
        Meta.fixedSizeBinaryAddByteWidth(b, Int32(t.nbytes))
        return Meta.FixedSizeBinary, Meta.fixedSizeBinaryEnd(b)
    elseif t isa ListType
        if t.large
            Meta.largeListStart(b)
            return Meta.LargeList, Meta.largeListEnd(b)
        end
        Meta.listStart(b)
        return Meta.List, Meta.listEnd(b)
    elseif t isa FixedSizeListType
        Meta.fixedSizeListStart(b)
        Meta.fixedSizeListAddListSize(b, Int32(t.listsize))
        return Meta.FixedSizeList, Meta.fixedSizeListEnd(b)
    elseif t isa StructType
        Meta.structStart(b)
        return Meta.Struct, Meta.structEnd(b)
    elseif t isa MapType
        Meta.mapStart(b)
        t.keyssorted && Meta.mapAddKeysSorted(b, true)
        return Meta.Map, Meta.mapEnd(b)
    elseif t isa DateType
        Meta.dateStart(b)
        Meta.dateAddUnit(
            b,
            t.unit == AC.DAY ? Meta.DateUnit.DAY : Meta.DateUnit.MILLISECOND,
        )
        return Meta.Date, Meta.dateEnd(b)
    elseif t isa TimeType
        Meta.timeStart(b)
        Meta.timeAddUnit(b, _metatimeunit(t.unit))
        Meta.timeAddBitWidth(b, Int32(t.bits))
        return Meta.Time, Meta.timeEnd(b)
    elseif t isa TimestampType
        tz = t.timezone === nothing ? FB.UOffsetT(0) : FB.createstring!(b, t.timezone)
        Meta.timestampStart(b)
        Meta.timestampAddUnit(b, _metatimeunit(t.unit))
        tz == 0 || Meta.timestampAddTimezone(b, tz)
        return Meta.Timestamp, Meta.timestampEnd(b)
    elseif t isa DurationType
        Meta.durationStart(b)
        Meta.durationAddUnit(b, _metatimeunit(t.unit))
        return Meta.Duration, Meta.durationEnd(b)
    elseif t isa DecimalType
        Meta.decimalStart(b)
        Meta.decimalAddPrecision(b, Int32(t.precision))
        Meta.decimalAddScale(b, Int32(t.scale))
        Meta.decimalAddBitWidth(b, Int32(t.bits))
        return Meta.Decimal, Meta.decimalEnd(b)
    elseif t isa IntervalType
        Meta.intervalStart(b)
        Meta.intervalAddUnit(
            b,
            t.unit == AC.YEAR_MONTH ? Meta.IntervalUnit.YEAR_MONTH :
            t.unit == AC.DAY_TIME ? Meta.IntervalUnit.DAY_TIME :
            Meta.IntervalUnit.MONTH_DAY_NANO,
        )
        return Meta.Interval, Meta.intervalEnd(b)
    elseif t isa UnionType
        # The builder writes last-first, so every vector is prepended in
        # reverse. A zero UOffsetT means "absent": the slot is left out and
        # the getter returns the schema default.
        Meta.unionStartTypeIdsVector(b, length(t.typeids))
        foreach(x -> FB.prepend!(b, Int32(x)), Iterators.reverse(t.typeids))
        idvec = FB.endvector!(b, length(t.typeids))
        Meta.unionStart(b)
        Meta.unionAddMode(
            b,
            t.mode == AC.DenseMode ? Meta.UnionMode.Dense : Meta.UnionMode.Sparse,
        )
        Meta.unionAddTypeIds(b, idvec)
        return Meta.Union, Meta.unionEnd(b)
    elseif t isa ViewType
        if t.utf8
            Meta.utf8ViewStart(b)
            return Meta.Utf8View, Meta.utf8ViewEnd(b)
        end
        Meta.binaryViewStart(b)
        return Meta.BinaryView, Meta.binaryViewEnd(b)
    elseif t isa ListViewType
        if t.large
            Meta.largeListViewStart(b)
            return Meta.LargeListView, Meta.largeListViewEnd(b)
        end
        Meta.listViewStart(b)
        return Meta.ListView, Meta.listViewEnd(b)
    elseif t isa RunEndEncodedType
        Meta.runEndEncodedStart(b)
        return Meta.RunEndEncoded, Meta.runEndEncodedEnd(b)
    elseif t isa NullType
        Meta.nullStart(b)
        return Meta.Null, Meta.nullEnd(b)
    else
        throw(
            ValidationError(
                "IPC writer does not map descriptor " * "$(AC.descriptorname(t))",
            ),
        )
    end
end

function _metakeyvalues!(b::FB.Builder, metadata)
    metadata === nothing && return FB.UOffsetT(0)
    # Core metadata is an ordered Pair sequence. Preserve both that order and
    # duplicate keys; callers that want canonical ordering can supply it.
    pairs = collect(metadata)
    kvs = FB.UOffsetT[]
    for (k, v) in pairs
        key = FB.createstring!(b, k)
        val = FB.createstring!(b, v)
        Meta.keyValueStart(b)
        Meta.keyValueAddKey(b, key)
        Meta.keyValueAddValue(b, val)
        push!(kvs, Meta.keyValueEnd(b))
    end
    FB.startvector!(b, 4, length(kvs), 4)
    foreach(x -> FB.prependoffset!(b, x), Iterators.reverse(kvs))
    return FB.endvector!(b, length(kvs))
end

"""
Build the flatbuffer Field table for one Core Field (inverse of `corefield`).
A `DictionaryType` field writes its VALUE type into the type slots and its
index/id/ordering into a DictionaryEncoding table; the id comes from the
writer's side table — Core fields still never carry one.
"""
function metafield!(b::FB.Builder, f::Field, fielddictids::IdDict{Field,Int64})
    t = f.type
    valuetype = t
    dictoff = FB.UOffsetT(0)
    if t isa DictionaryType
        valuetype = t.valuetype
        idxtag, idxoff = metatype!(b, t.indextype)
        idxtag === Meta.Int ||
            throw(ValidationError("dictionary index type must be an integer"))
        Meta.dictionaryEncodingStart(b)
        Meta.dictionaryEncodingAddId(b, fielddictids[f])
        Meta.dictionaryEncodingAddIndexType(b, idxoff)
        t.ordered && Meta.dictionaryEncodingAddIsOrdered(b, true)
        dictoff = Meta.dictionaryEncodingEnd(b)
    end
    children = FB.UOffsetT[metafield!(b, c, fielddictids) for c in f.children]
    Meta.fieldStartChildrenVector(b, length(children))
    foreach(x -> FB.prependoffset!(b, x), Iterators.reverse(children))
    childvec = FB.endvector!(b, length(children))
    kvvec = _metakeyvalues!(b, f.metadata)
    name = FB.createstring!(b, f.name)
    tag, typeoff = metatype!(b, valuetype)
    Meta.fieldStart(b)
    Meta.fieldAddName(b, name)
    Meta.fieldAddNullable(b, f.nullable)
    Meta.fieldAddTypeType(b, tag)
    Meta.fieldAddType(b, typeoff)
    dictoff == 0 || Meta.fieldAddDictionary(b, dictoff)
    Meta.fieldAddChildren(b, childvec)
    kvvec == 0 || Meta.fieldAddCustomMetadata(b, kvvec)
    return Meta.fieldEnd(b)
end

_pad8!(bytes::Vector{UInt8}) = append!(bytes, zeros(UInt8, mod(-length(bytes), 8)))

"""
Finish the current builder content as one framed message: continuation
marker, padded metadata length, metadata, then the (already padded) body.
"""
function _finishmessage!(out::Vector{UInt8}, b::FB.Builder, msg, body::Vector{UInt8})
    FB.finish!(b, msg)
    meta = collect(FB.finishedbytes(b))
    _pad8!(meta)
    length(body) % 8 == 0 || throw(ArgumentError("message body must be padded"))
    append!(out, reinterpret(UInt8, UInt32[CONTINUATION, UInt32(length(meta))]))
    append!(out, meta)
    append!(out, body)
    return out
end

function _metaschema!(
    b::FB.Builder,
    sch::Schema,
    fielddictids::IdDict{Field,Int64},
    features::Vector{Int64},
)
    fields = FB.UOffsetT[metafield!(b, f, fielddictids) for f in sch.fields]
    Meta.schemaStartFieldsVector(b, length(fields))
    foreach(x -> FB.prependoffset!(b, x), Iterators.reverse(fields))
    fieldvec = FB.endvector!(b, length(fields))
    kvvec = _metakeyvalues!(b, sch.metadata)
    featurevec = FB.UOffsetT(0)
    if !isempty(features)
        FB.startvector!(b, 8, length(features), 8)
        foreach(x -> FB.prepend!(b, x), Iterators.reverse(features))
        featurevec = FB.endvector!(b, length(features))
    end
    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fieldvec)
    kvvec == 0 || Meta.schemaAddCustomMetadata(b, kvvec)
    featurevec == 0 || Meta.schemaAddFeatures(b, featurevec)
    return Meta.schemaEnd(b)
end

function _schemamessage!(
    out::Vector{UInt8},
    sch::Schema,
    fielddictids::IdDict{Field,Int64},
    features::Vector{Int64},
)
    b = FB.Builder(1024)
    schoff = _metaschema!(b, sch, fielddictids, features)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.Schema)
    Meta.messageAddHeader(b, schoff)
    return _finishmessage!(out, b, Meta.messageEnd(b), UInt8[])
end

# ---------------------------------------------------------------------------
# THE generic encoder: registry-driven node/buffer emission
# ---------------------------------------------------------------------------

mutable struct EncodeCursor
    nodes::Vector{NTuple{2,Int64}}       # (length, null_count), forward order
    buffers::Vector{NTuple{2,Int64}}     # (offset, length), forward order
    body::Vector{UInt8}
    codec::Int8
    state::Union{Nothing,EncodeState}
    variadics::Vector{Int64}             # per view field, depth-first order
end
EncodeCursor(codec::Int8, state::Union{Nothing,EncodeState}) =
    EncodeCursor(NTuple{2,Int64}[], NTuple{2,Int64}[], UInt8[], codec, state, Int64[])

function _compressbytes(state::EncodeState, codec::Int8, raw::Vector{UInt8})
    codec == CODEC_LZ4_FRAME && return transcode(_lz4c!(state), raw)
    return transcode(_zstdc!(state), raw)
end

"""
Append one buffer to the message body: raw bytes for uncompressed batches;
for compressed batches, the spec's Int64 uncompressed-length prefix plus the
frame, falling back to `-1` + raw whenever compression does not shrink the
payload. Zero-length buffers write no body bytes in either mode. The recorded
Buffer length is the wire length; 8-byte alignment padding sits between
buffers and belongs to neither.
"""
function encodebuffer!(c::EncodeCursor, bytes::Vector{UInt8})
    offset = Int64(length(c.body))
    offset % 8 == 0 || throw(ArgumentError("encoder lost body alignment"))
    if isempty(bytes)
        push!(c.buffers, (offset, Int64(0)))
        return nothing
    end
    if c.codec == CODEC_NONE
        append!(c.body, bytes)
        push!(c.buffers, (offset, Int64(length(bytes))))
        _pad8!(c.body)
        return nothing
    end
    compressed = _compressbytes(c.state::EncodeState, c.codec, bytes)
    if length(compressed) < length(bytes)
        append!(c.body, reinterpret(UInt8, Int64[Int64(length(bytes))]))
        append!(c.body, compressed)
        push!(c.buffers, (offset, Int64(8 + length(compressed))))
    else
        append!(c.body, reinterpret(UInt8, Int64[Int64(-1)]))
        append!(c.body, bytes)
        push!(c.buffers, (offset, Int64(8 + length(bytes))))
    end
    _pad8!(c.body)
    return nothing
end

"""
    encodefield!(cursor, f, d)

The write half of the registry walk — the exact mirror of `decodefield`:
one node, then the layout's buffers in registry order, then children in
declared order. Dictionary-encoded fields emit their INDEX buffers here;
their pool travels in a dictionary batch. Buffer content is emitted from the
`ArrayData` slices verbatim: the encoder adds no per-layout interpretation,
so read and write cannot skew.
"""
function encodefield!(c::EncodeCursor, f::Field, d::ArrayData)
    t = f.type
    AC.typeequal(t, d.type) ||
        throw(ValidationError("column data type does not match its schema field"))
    d.offset == 0 || throw(
        ValidationError(
            "IPC encode of sliced (nonzero-offset) arrays is not supported; materialize first",
        ),
    )
    push!(c.nodes, (d.len, AC.nullcount(d)))
    spec = layoutspec(t)
    if spec.variadic
        length(d.buffers) >= length(spec.buffers) ||
            throw(ValidationError("column buffer count does not match its layout"))
    else
        length(d.buffers) == length(spec.buffers) ||
            throw(ValidationError("column buffer count does not match its layout"))
    end
    for (role, b) in zip(spec.buffers, d.buffers)
        if role == AC.OFFSETS && d.len == 0 && b.len == 0
            # Core canonicalizes an empty offset array without allocating its
            # otherwise-unused physical buffer. IPC still requires the one
            # terminal zero offset (length + 1 entries).
            encodebuffer!(c, zeros(UInt8, spec.offsetwidth))
        else
            encodebuffer!(c, AC.slicebytes(b))
        end
    end
    if spec.variadic
        # View layouts append their variadic data buffers after the fixed
        # validity/views pair; the count travels in the header's
        # variadicBufferCounts vector, depth-first (format 1.4).
        push!(c.variadics, Int64(length(d.buffers) - length(spec.buffers)))
        for b in Iterators.drop(d.buffers, length(spec.buffers))
            encodebuffer!(c, AC.slicebytes(b))
        end
    end
    t isa DictionaryType && return nothing
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    length(d.children) == nchildren ||
        throw(ValidationError("column child count does not match its schema field"))
    if t isa UnionType && t.mode == AC.SparseMode
        all(child -> child.len == d.len, d.children) ||
            throw(ValidationError("IPC sparse-union children must equal the union length"))
    end
    for i = 1:nchildren
        encodefield!(c, f.children[i], d.children[i])
    end
    return nothing
end

function _batchheader!(b::FB.Builder, c::EncodeCursor, nrows::Int64)
    Meta.recordBatchStartNodesVector(b, length(c.nodes))
    for (len, nulls) in Iterators.reverse(c.nodes)
        Meta.createFieldNode(b, len, nulls)
    end
    nodes = FB.endvector!(b, length(c.nodes))
    Meta.recordBatchStartBuffersVector(b, length(c.buffers))
    for (off, len) in Iterators.reverse(c.buffers)
        Meta.createBuffer(b, off, len)
    end
    buffers = FB.endvector!(b, length(c.buffers))
    compression = FB.UOffsetT(0)
    if c.codec != CODEC_NONE
        Meta.bodyCompressionStart(b)
        Meta.bodyCompressionAddCodec(
            b,
            c.codec == CODEC_LZ4_FRAME ? Meta.CompressionType.LZ4_FRAME :
            Meta.CompressionType.ZSTD,
        )
        compression = Meta.bodyCompressionEnd(b)
    end
    varvec = FB.UOffsetT(0)
    if !isempty(c.variadics)
        Meta.recordBatchStartVariadicBufferCountsVector(b, length(c.variadics))
        foreach(x -> FB.prepend!(b, x), Iterators.reverse(c.variadics))
        varvec = FB.endvector!(b, length(c.variadics))
    end
    Meta.recordBatchStart(b)
    Meta.recordBatchAddLength(b, nrows)
    Meta.recordBatchAddNodes(b, nodes)
    Meta.recordBatchAddBuffers(b, buffers)
    compression == 0 || Meta.recordBatchAddCompression(b, compression)
    varvec == 0 || Meta.recordBatchAddVariadicBufferCounts(b, varvec)
    return Meta.recordBatchEnd(b)
end

function _recordmessage!(
    out::Vector{UInt8},
    batch::AC.RecordBatch,
    fields,
    codec::Int8,
    state::Union{Nothing,EncodeState},
)
    c = EncodeCursor(codec, state)
    for (f, col) in zip(fields, batch.columns)
        encodefield!(c, f, col)
    end
    b = FB.Builder(1024)
    rb = _batchheader!(b, c, batch.nrows)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.RecordBatch)
    Meta.messageAddHeader(b, rb)
    Meta.messageAddBodyLength(b, Int64(length(c.body)))
    return _finishmessage!(out, b, Meta.messageEnd(b), c.body)
end

function _dictionarymessage!(
    out::Vector{UInt8},
    id::Int64,
    vf::Field,
    pool::ArrayData,
    codec::Int8,
    state::Union{Nothing,EncodeState},
)
    c = EncodeCursor(codec, state)
    encodefield!(c, vf, pool)
    b = FB.Builder(1024)
    rb = _batchheader!(b, c, pool.len)
    Meta.dictionaryBatchStart(b)
    Meta.dictionaryBatchAddId(b, id)
    Meta.dictionaryBatchAddData(b, rb)
    dictbatch = Meta.dictionaryBatchEnd(b)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.DictionaryBatch)
    Meta.messageAddHeader(b, dictbatch)
    Meta.messageAddBodyLength(b, Int64(length(c.body)))
    return _finishmessage!(out, b, Meta.messageEnd(b), c.body)
end

# ---------------------------------------------------------------------------
# Stream writer driver
# ---------------------------------------------------------------------------

const CODEC_NAMES =
    Dict{Symbol,Int8}(:none => CODEC_NONE, :lz4 => CODEC_LZ4_FRAME, :zstd => CODEC_ZSTD)

function _requirelittleendian(host_endian_bom::UInt32=Base.ENDIAN_BOM)
    host_endian_bom == UInt32(0x04030201) ||
        throw(ValidationError("the IPC writer requires a little-endian host"))
    return nothing
end

"""
Assign one IPC dictionary id per dictionary-typed field, depth-first over the
schema — the writer-side half of the adapter id table (ids are adapter
bookkeeping; Core fields never carry them).
"""
function assigndictids(fields, given::IdDict{Field,Int64}=IdDict{Field,Int64}())
    # `given` lets a caller preserve ids from a source (a reader's table): two
    # fields sharing one id then share one dictionary batch, exactly as the
    # source did (the arrow-testing 4.0.0-shareddict integration case). Fresh
    # ids fill the lowest unoccupied values so they never collide with given
    # ones — including given ids at the top of the signed-long domain, where
    # `max + 1` would wrap.
    ids = IdDict{Field,Int64}(given)
    seen = IdDict{Field,Nothing}()
    used = Set{Int64}(values(ids))
    next = Ref(Int64(0))
    function freshid()
        while next[] in used
            next[] < typemax(Int64) ||
                throw(ValidationError("IPC dictionary id space is exhausted"))
            next[] += 1
        end
        push!(used, next[])
        return next[]
    end
    function walk(f::Field)
        haskey(seen, f) && throw(
            ValidationError(
                "IPC writer schema reuses one Field object in multiple positions",
            ),
        )
        seen[f] = nothing
        if f.type isa DictionaryType && !haskey(ids, f)
            ids[f] = freshid()
        end
        foreach(walk, f.children)
    end
    foreach(walk, fields)
    return ids
end

"""
Collect `(field, pool)` pairs for every dictionary-typed field in one batch,
paired with the schema walk (nested dictionaries included).
"""
function dictionarypools(fields, cols)
    pairs = Tuple{Field,ArrayData}[]
    function walk(f::Field, d::ArrayData)
        if f.type isa DictionaryType
            d.dictionary === nothing &&
                throw(ValidationError("dictionary column carries no pool"))
            # Post-order: pools nested INSIDE this pool's values are collected
            # (and therefore emitted) before it — the dependency order the
            # IPC spec requires for nested dictionary encoding.
            walk(AC.dictvaluefield(f, f.type), d.dictionary)
            push!(pairs, (f, d.dictionary))
            return
        end
        for (cf, cd) in zip(f.children, d.children)
            walk(cf, cd)
        end
    end
    for (f, d) in zip(fields, cols)
        walk(f, d)
    end
    return pairs
end

function _checkbatches(sch::Schema, batches)
    for batch in batches
        length(batch.columns) == length(sch.fields) ||
            throw(ValidationError("batch column count does not match the schema"))
        for (f, bf) in zip(sch.fields, batch.schema.fields)
            AC.typeequal(f.type, bf.type) && f.name == bf.name ||
                throw(ValidationError("batch schema does not match the stream schema"))
        end
    end
    return nothing
end

function _validatewriterschema(sch::Schema)
    AC._validate_schema(sch)
    function walk(f::Field)
        isvalid(f.name) || throw(ValidationError("field name is not valid UTF-8"))
        AC._validate_metadata(f.metadata, "field")
        foreach(walk, f.children)
        return nothing
    end
    foreach(walk, sch.fields)
    foreach(validateschemafield, sch.fields)
    return nothing
end

function _validatewriterbatch!(
    validated::AC._ValidatedDictionaries,
    sch::Schema,
    batch::AC.RecordBatch,
    ids::IdDict{Field,Int64},
)
    # A shared immutable pool must satisfy every value-field contract
    # through which the schema refers to it. Identity caching is safe only
    # after those field-specific checks have run.
    current = Dict{Int64,ArrayData}()
    for (f, pool) in dictionarypools(sch.fields, batch.columns)
        # One id names ONE pool within a record batch: every dictionary
        # message precedes the record message on the wire, so an
        # intra-batch pool change is not temporal replacement — it would
        # silently retarget the earlier field to the later pool.
        id = ids[f]
        haskey(current, id) &&
            current[id] !== pool &&
            throw(
                ValidationError(
                    "dictionary id $id carries two different pools in one record batch",
                ),
            )
        current[id] = pool
        validate_semantic(AC.dictvaluefield(f, f.type::DictionaryType), pool)
        validated[pool] = nothing
    end
    for (f, col) in zip(sch.fields, batch.columns)
        AC._validate_semantic(f, col, validated)
    end
    return nothing
end

function _validatewriterbatches(sch::Schema, batches, ids::IdDict{Field,Int64})
    validated = AC._ValidatedDictionaries()
    for batch in batches
        _validatewriterbatch!(validated, sch, batch, ids)
    end
    return nothing
end

const FEATURE_DICTIONARY_REPLACEMENT = Int64(1)
const FEATURE_COMPRESSED_BODY = Int64(2)

"""
Which features must the schema declare for these batches? Replacement is
detected by pool-identity change per id across the batch sequence
(replacement-on-change); compression declares COMPRESSED_BODY.
"""
function _streamfeatures(sch::Schema, batches, ids::IdDict{Field,Int64}, codec::Int8)
    features = Int64[]
    current = Dict{Int64,ArrayData}()
    replacement = false
    for batch in batches
        for (f, pool) in dictionarypools(sch.fields, batch.columns)
            id = ids[f]
            old = get(current, id, nothing)
            old === pool || old === nothing || (replacement = true)
            current[id] = pool
        end
    end
    replacement && push!(features, FEATURE_DICTIONARY_REPLACEMENT)
    isempty(batches) || codec == CODEC_NONE || push!(features, FEATURE_COMPRESSED_BODY)
    return features
end

# ---------------------------------------------------------------------------
# Incremental writer core: begin/writebatch/finish over one IO
# ---------------------------------------------------------------------------

"""
One in-progress IPC output. `beginwrite!` emits the schema message (and the
file-format preamble), `writebatch!` validates and publishes one record batch
(with any dictionary messages it needs), and `finishwrite!` emits the
end-of-stream marker (and the file-format Footer). Batch bytes are staged in
full and published to `io` once, so a validation or encoding failure never
publishes a partial message — but the SINK gains bytes batch by batch, and an
abandoned state leaves a torn stream or an unfooted file.

The eager `writestream`/`writefile` drive this same state over an in-memory
sink, validating every batch up front (`validate=false` per batch) and
passing the features their whole batch sequence implies, so their output is
byte-identical to the pre-incremental writers.
"""
mutable struct IPCWriteState
    const io::IO
    const file::Bool
    const schema::Schema
    const fielddictids::IdDict{Field,Int64}
    const ids::IdDict{Field,Int64}
    const features::Vector{Int64}
    const codec::Int8
    const state::Union{Nothing,EncodeState}
    const validated::AC._ValidatedDictionaries
    const current::Dict{Int64,ArrayData}  # last pool per id, for replacement
    const dictblocks::Vector{NTuple{3,Int64}}    # file format Footer Blocks
    const recordblocks::Vector{NTuple{3,Int64}}
    written::Int64                       # bytes published to `io`
    finished::Bool
end

# Batch sequences are unknown up front, so declare what MAY occur: any
# dictionary field may later replace its pool (stream format only — the file
# format forbids replacement), and a codec means compressed bodies.
function _incrementalfeatures(ids::IdDict{Field,Int64}, codec::Int8, file::Bool)
    features = Int64[]
    !file && !isempty(ids) && push!(features, FEATURE_DICTIONARY_REPLACEMENT)
    codec == CODEC_NONE || push!(features, FEATURE_COMPRESSED_BODY)
    return features
end

function _publish!(st::IPCWriteState, out::Vector{UInt8})
    Base.write(st.io, out)
    st.written += length(out)
    return nothing
end

function beginwrite!(
    io::IO,
    sch::Schema;
    file::Bool,
    compress::Symbol=:none,
    dictids::IdDict{Field,Int64}=IdDict{Field,Int64}(),
    features::Union{Nothing,Vector{Int64}}=nothing,
)
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _validatewriterschema(sch)
    ids = assigndictids(sch.fields, dictids)
    fielddictids = IdDict{Field,Int64}(ids)
    validatedictionaryids(sch.fields, fielddictids)
    feats = features === nothing ? _incrementalfeatures(ids, codec, file) : features
    st = IPCWriteState(
        io,
        file,
        sch,
        fielddictids,
        ids,
        feats,
        codec,
        codec == CODEC_NONE ? nothing : EncodeState(),
        AC._ValidatedDictionaries(),
        Dict{Int64,ArrayData}(),
        NTuple{3,Int64}[],
        NTuple{3,Int64}[],
        Int64(0),
        false,
    )
    out = UInt8[]
    if file
        append!(out, FILE_MAGIC)
        append!(out, zeros(UInt8, 2))    # pad to 8 before the first message
    end
    _schemamessage!(out, sch, fielddictids, feats)
    _publish!(st, out)
    return st
end

# Record one file-format Block around `emit!`: the Block offset is global
# (bytes already published plus this staging buffer's position), and
# metaDataLength spans prefix + metadata (up to the body start).
function _fileblock!(emit!::F, st::IPCWriteState, out::Vector{UInt8}, blocks) where {F}
    start = Int64(length(out))
    emit!()
    total = Int64(length(out)) - start
    metalen = Int64(8) + Int64(reinterpret(UInt32, out[(start + 5):(start + 8)])[1])
    push!(blocks, (st.written + start, metalen, total - metalen))
    return nothing
end

"Order-sensitive pool content equality, in each pool's public value domain."
function _poolsequal(vf::Field, a::ArrayData, b::ArrayData)
    a.len == b.len || return false
    return isequal(AC.materialize(vf, a), AC.materialize(vf, b))
end

function writebatch!(st::IPCWriteState, batch::AC.RecordBatch; validate::Bool=true)
    st.finished && throw(ArgumentError("this IPC writer is already finished"))
    if validate
        _checkbatches(st.schema, (batch,))
        _validatewriterbatch!(st.validated, st.schema, batch, st.ids)
    end
    out = UInt8[]
    for (f, pool) in dictionarypools(st.schema.fields, batch.columns)
        id = st.ids[f]
        old = get(st.current, id, nothing)
        old === pool && continue
        if old !== nothing
            vf = AC.dictvaluefield(f, f.type::DictionaryType)
            if st.file
                # The file format carries one dictionary batch per id. A new
                # pool object with identical content references the emitted
                # batch; anything else is replacement, which files cannot say.
                _poolsequal(vf, old, pool) || throw(
                    ValidationError(
                        "the IPC file format carries one dictionary batch per " *
                        "id; dictionary id $id changed pools across batches — " *
                        "use the stream format for replacement",
                    ),
                )
                st.current[id] = pool
                continue
            end
            if !(FEATURE_DICTIONARY_REPLACEMENT in st.features)
                # Emitting a replacement the schema message did not declare
                # would publish a stream this package's own reader refuses.
                _poolsequal(vf, old, pool) || throw(
                    ValidationError(
                        "dictionary id $id changed pools, but the stream's " *
                        "schema message does not declare the " *
                        "DictionaryReplacement feature",
                    ),
                )
                st.current[id] = pool
                continue
            end
        end
        vf = AC.dictvaluefield(f, f.type::DictionaryType)
        if st.file
            _fileblock!(st, out, st.dictblocks) do
                _dictionarymessage!(out, id, vf, pool, st.codec, st.state)
            end
        else
            _dictionarymessage!(out, id, vf, pool, st.codec, st.state)
        end
        st.current[id] = pool
    end
    if st.file
        _fileblock!(st, out, st.recordblocks) do
            _recordmessage!(out, batch, st.schema.fields, st.codec, st.state)
        end
    else
        _recordmessage!(out, batch, st.schema.fields, st.codec, st.state)
    end
    _publish!(st, out)
    return nothing
end

# The file-format trailer: Footer flatbuffer (schema again, then the two
# Block struct-vectors), the Int32 footer length, and the trailing magic.
function _filefooter!(
    out::Vector{UInt8},
    sch::Schema,
    fielddictids::IdDict{Field,Int64},
    features::Vector{Int64},
    dictblocks::Vector{NTuple{3,Int64}},
    recordblocks::Vector{NTuple{3,Int64}},
)
    b = FB.Builder(1024)
    schoff = _metaschema!(b, sch, fielddictids, features)
    Meta.footerStartDictionariesVector(b, length(dictblocks))
    for (off, metalen, bodylen) in Iterators.reverse(dictblocks)
        Meta.createBlock(b, off, Int32(metalen), bodylen)
    end
    dictvec = FB.endvector!(b, length(dictblocks))
    Meta.footerStartRecordBatchesVector(b, length(recordblocks))
    for (off, metalen, bodylen) in Iterators.reverse(recordblocks)
        Meta.createBlock(b, off, Int32(metalen), bodylen)
    end
    recordvec = FB.endvector!(b, length(recordblocks))
    Meta.footerStart(b)
    Meta.footerAddVersion(b, Meta.MetadataVersion.V5)
    Meta.footerAddSchema(b, schoff)
    Meta.footerAddDictionaries(b, dictvec)
    Meta.footerAddRecordBatches(b, recordvec)
    FB.finish!(b, Meta.footerEnd(b))
    footer = collect(FB.finishedbytes(b))
    append!(out, footer)
    append!(out, reinterpret(UInt8, Int32[Int32(length(footer))]))
    append!(out, FILE_MAGIC)
    return nothing
end

function finishwrite!(st::IPCWriteState)
    st.finished && return nothing
    out = UInt8[]
    append!(out, reinterpret(UInt8, UInt32[CONTINUATION, UInt32(0)]))
    st.file && _filefooter!(
        out,
        st.schema,
        st.fielddictids,
        st.features,
        st.dictblocks,
        st.recordblocks,
    )
    _publish!(st, out)
    st.finished = true
    st.state === nothing || close(st.state)
    return nothing
end

"Release the codec state without emitting anything; for error cleanup."
function abortwrite!(st::IPCWriteState)
    st.finished && return nothing
    st.finished = true
    st.state === nothing || close(st.state)
    return nothing
end

"""
Resume a STREAM whose schema message is already on the wire: a state that
emits no preamble. `features` must be the features that schema message
declared, and `current` the last pool each dictionary id has emitted —
`writebatch!` then skips content-identical pools and gates replacement on
the declared features exactly as it does mid-stream.
"""
function resumestream!(
    io::IO,
    sch::Schema,
    dictids::IdDict{Field,Int64},
    features::Vector{Int64};
    compress::Symbol=:none,
    current::Dict{Int64,ArrayData}=Dict{Int64,ArrayData}(),
)
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    ids = assigndictids(sch.fields, dictids)
    return IPCWriteState(
        io,
        false,
        sch,
        IdDict{Field,Int64}(ids),
        ids,
        features,
        codec,
        codec == CODEC_NONE ? nothing : EncodeState(),
        AC._ValidatedDictionaries(),
        current,
        NTuple{3,Int64}[],
        NTuple{3,Int64}[],
        Int64(0),
        false,
    )
end

"""
    writestream(sch, batches; compress=:none, dictids=IdDict{Field,Int64}()) -> Vector{UInt8}
    writestream(stream::IPCStream; compress=:none) -> Vector{UInt8}

Encode a complete IPC stream: schema message, dictionary batches emitted
before the first record batch that references them (and again on
pool-identity change), record batches, end-of-stream marker. Every column is
semantically validated before any of its bytes are emitted — the writer
refuses to publish data Core would refuse to read. `dictids` preserves ids
from a source table (the `IPCStream` form passes the reader's), so fields
sharing one id keep sharing one dictionary batch, exactly as the source did.
"""
function writestream(
    sch::Schema,
    batches::AbstractVector{AC.RecordBatch};
    compress::Symbol=:none,
    dictids::IdDict{Field,Int64}=IdDict{Field,Int64}(),
)
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    _checkbatches(sch, batches)
    _validatewriterschema(sch)
    # `beginwrite!` recomputes the same ids from `dictids` (assignment is
    # deterministic); this pass exists so the whole batch sequence is
    # validated before any bytes exist. Features use the incremental
    # default: a stream with dictionary fields declares DictionaryReplacement
    # whether or not these batches replace, so the output stays appendable.
    ids = assigndictids(sch.fields, dictids)
    validatedictionaryids(sch.fields, IdDict{Field,Int64}(ids))
    _validatewriterbatches(sch, batches, ids)
    io = IOBuffer()
    st = beginwrite!(io, sch; file=false, compress=compress, dictids=dictids)
    try
        for batch in batches
            writebatch!(st, batch; validate=false)
        end
        finishwrite!(st)
    finally
        abortwrite!(st)
    end
    return take!(io)
end

writestream(s::IPCStream; compress::Symbol=:none) =
    writestream(s.schema, s.batches; compress=compress, dictids=s.fielddictids)

# ---------------------------------------------------------------------------
# File format: magic + stream messages + Block index + Footer
# ---------------------------------------------------------------------------

const FILE_MAGIC = b"ARROW1"

"""
    writefile(sch, batches; compress=:none, dictids=IdDict{Field,Int64}()) -> Vector{UInt8}
    writefile(stream::IPCStream; compress=:none) -> Vector{UInt8}

The file variant: leading magic, the same stream messages, an end-of-stream
marker, then the Footer with its dictionary and record-batch Block indexes,
the Int32 footer length, and the trailing magic. Footer bookkeeping is
isolated here; message writing is the stream code above. The file format
carries exactly one dictionary batch per id, so batches whose pools change
identity are a clean refusal (the stream format handles replacement).
`dictids` preserves ids from a source table (the `IPCStream` form passes the
reader's), so fields sharing one id keep sharing one dictionary batch,
exactly as the source did.
"""
function writefile(
    sch::Schema,
    batches::AbstractVector{AC.RecordBatch};
    compress::Symbol=:none,
    dictids::IdDict{Field,Int64}=IdDict{Field,Int64}(),
)
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkbatches(sch, batches)
    _validatewriterschema(sch)
    ids = assigndictids(sch.fields, dictids)
    # CODEC_NONE so this refusal sees only DICTIONARY_REPLACEMENT.
    # COMPRESSED_BODY is legal in a file and is declared through the features
    # below.
    isempty(_streamfeatures(sch, batches, ids, CODEC_NONE)) || throw(
        ValidationError(
            "the IPC file format carries one dictionary batch per id; " *
            "a dictionary with changing pools requires the stream format",
        ),
    )
    # Same shared-id contract as the stream writer: compatible value schemas,
    # one nested id topology, one pool per id within each batch.
    validatedictionaryids(sch.fields, IdDict{Field,Int64}(ids))
    _validatewriterbatches(sch, batches, ids)
    io = IOBuffer()
    st = beginwrite!(
        io,
        sch;
        file=true,
        compress=compress,
        dictids=dictids,
        features=_streamfeatures(sch, batches, ids, codec),
    )
    try
        for batch in batches
            writebatch!(st, batch; validate=false)
        end
        finishwrite!(st)
    finally
        abortwrite!(st)
    end
    return take!(io)
end

writefile(s::IPCStream; compress::Symbol=:none) =
    writefile(s.schema, s.batches; compress=compress, dictids=s.fielddictids)

# ---------------------------------------------------------------------------
# File reader: footer verification + lazy random-access batch handle
# ---------------------------------------------------------------------------

_blocktuples(blocks) =
    blocks === nothing ? NTuple{3,Int64}[] :
    NTuple{3,Int64}[
        (Int64(b.offset), Int64(b.metaDataLength), Int64(b.bodyLength)) for b in blocks
    ]

"""
Footer verification, same wrapper role as `verify_ipc_metadata`: the
generated walker bounds the whole table graph, then the verified getters
supply the Block indexes.
"""
function verify_footer(
    bytes::Vector{UInt8},
    limits::Limits,
    reserve_limit::Int64=limits.max_total_allocated_bytes,
)
    ctx = _verifyctx(limits, reserve_limit)
    # Staged like verify_ipc_metadata: version gates between the inline and
    # reference stages, so an unsupported footer rejects in constant time.
    t = _verified(() -> Meta.verifyrootstart_Footer(bytes, ctx))
    footer = FB.getrootas(Meta.Footer, bytes, 0)
    version = Int16(Int64(footer.version))
    version in (Int16(3), Int16(4)) ||
        _vfail("unsupported footer version $version (only V4/V5 are accepted)")
    _verified(() -> Meta.verifyrootrest_Footer(t, ctx))
    features = _schemafeatures(footer.schema::Meta.Schema, version)
    return version,
    features,
    _blocktuples(footer.dictionaries),
    _blocktuples(footer.recordBatches),
    ctx.reserved
end

function _metadataequal(a, b)
    av = something(a, Meta.KeyValue[])
    bv = something(b, Meta.KeyValue[])
    length(av) == length(bv) || return false
    for (x, y) in zip(av, bv)
        x.key == y.key || return false
        something(x.value, "") == something(y.value, "") || return false
    end
    return true
end

function _fieldequal(a::Field, b::Field)
    a.name == b.name &&
    a.nullable == b.nullable &&
    AC.typeequal(a.type, b.type) &&
    a.metadata == b.metadata &&
    length(a.children) == length(b.children) || return false
    return all(_fieldequal(x, y) for (x, y) in zip(a.children, b.children))
end

function _schemaside(metaschema::Meta.Schema)
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()
    fields = Field[
        corefield(f, dictids, fielddictids) for
        f in something(metaschema.fields, Meta.Field[])
    ]
    foreach(validateschemafield, fields)
    valueschemas = validatedictionaryids(fields, fielddictids)
    length(valueschemas) == length(dictids) ||
        throw(ValidationError("duplicate dictionary id in file schema"))
    return fields
end

function _fieldwireequal(a::Meta.Field, b::Meta.Field)
    _metadataequal(a.custom_metadata, b.custom_metadata) || return false
    adict, bdict = a.dictionary, b.dictionary
    (adict === nothing) == (bdict === nothing) || return false
    if adict !== nothing
        adict.id == bdict.id || return false
    end
    achildren = something(a.children, Meta.Field[])
    bchildren = something(b.children, Meta.Field[])
    length(achildren) == length(bchildren) || return false
    return all(_fieldwireequal(x, y) for (x, y) in zip(achildren, bchildren))
end

function _schemaequal(a::Meta.Schema, b::Meta.Schema)
    something(a.endianness, Meta.Endianness.Little) ==
    something(b.endianness, Meta.Endianness.Little) || return false
    ametafields = something(a.fields, Meta.Field[])
    bmetafields = something(b.fields, Meta.Field[])
    length(ametafields) == length(bmetafields) || return false
    all(_fieldwireequal(x, y) for (x, y) in zip(ametafields, bmetafields)) || return false
    afields = _schemaside(a)
    bfields = _schemaside(b)
    length(afields) == length(bfields) || return false
    _metadataequal(a.custom_metadata, b.custom_metadata) || return false
    all(_fieldequal(x, y) for (x, y) in zip(afields, bfields)) || return false
    return true
end

function _fileschema(
    region::OwnerRegion,
    footerstart::Int64,
    limits::Limits,
    budget::AllocationBudget,
)
    blob = BufferSlice(region, 0, region.len)
    AC.loadat(blob, UInt32, Int64(8)) == CONTINUATION ||
        throw(ValidationError("file data section does not start with an IPC message"))
    declared = Int64(AC.loadat(blob, Int32, Int64(12)))
    0 < declared <= limits.max_metadata_bytes ||
        throw(ValidationError("file schema metadata length is outside the limit"))
    declared % 8 == 0 ||
        throw(ValidationError("file schema metadata is not 8-byte aligned"))
    metalen = AC.checked_add(Int64(8), declared)
    fm = _blockmessage(region, (Int64(8), metalen, Int64(0)), footerstart, limits, budget)
    fm.header_type == UInt8(1) && fm.msg.header isa Meta.Schema || # Schema
        throw(ValidationError("file data section does not start with a schema"))
    return fm, AC.checked_add(Int64(8), metalen)
end

"""
    ArrowFile

The footer's record-batch index as a random-access handle: `length(file)`
batches, `file[i]` decodes batch `i` on demand. Open decodes only the schema
and the dictionary batches every record shares. Each `getindex` decodes from
the mapped bytes with its own allocation budget and codec contexts, and the
handle is read-only after open, so concurrent `getindex` calls are safe. The
region root (heap vector or Mmap array) is the only lifetime anchor, as in
Core.
"""
struct ArrowFile
    region::OwnerRegion
    schema::Schema
    fields::AC.FrozenVector{Field}
    fielddictids::IdDict{Field,Int64}
    dictionaries::Dict{Int64,ArrayData}
    validated::AC._ValidatedDictionaries
    recordblocks::Vector{NTuple{3,Int64}}
    dataend::Int64
    limits::Limits
    schemaversion::Int16
end

Base.length(f::ArrowFile) = length(f.recordblocks)
AC.schema(f::ArrowFile) = f.schema

"""
Range-check one Footer Block's declared extents against the data section and
return its `(offset, frameend)` span.
"""
function _blockextent(block::NTuple{3,Int64}, dataend::Int64)
    offset, metalen, bodylen = block
    # 16 is the smallest legal prefix + metadata: an 8-byte prefix plus at
    # least one 8-byte-aligned metadata block.
    (offset >= 0 && metalen >= 16 && bodylen >= 0) ||
        throw(ValidationError("footer block has invalid extents"))
    offset % 8 == 0 || throw(ValidationError("footer block is not 8-byte aligned"))
    metalen % 8 == 0 ||
        throw(ValidationError("footer block metadata length is not 8-byte aligned"))
    bodylen % 8 == 0 ||
        throw(ValidationError("footer block body length is not 8-byte aligned"))
    frameend = AC.checked_add(AC.checked_add(offset, metalen), bodylen)
    frameend <= dataend || throw(ValidationError("footer block escapes the data section"))
    return offset, frameend
end

function _validateblockindex(dictblocks, recordblocks, dataend::Int64; datastart::Int64=0)
    extents = Tuple{Int64,Int64}[]
    indexedend = datastart
    for block in Iterators.flatten((dictblocks, recordblocks))
        extent = _blockextent(block, dataend)
        extent[1] >= datastart ||
            throw(ValidationError("footer block overlaps the file schema"))
        push!(extents, extent)
        indexedend = max(indexedend, extent[2])
    end
    sort!(extents; by=first)
    for i = 2:length(extents)
        extents[i - 1][2] <= extents[i][1] ||
            throw(ValidationError("footer blocks overlap"))
    end
    return indexedend
end

# The `_block*` traversal below mirrors the schema-blind vtable primitives of
# src/metadata/VerifierRuntime.jl (`_vtable`/`_vfield`/`_vref`/`_vvector`/
# `_vrange`) over a `BufferSlice` source with block-specific diagnostics; the
# two must change in lockstep.
function _blockrange(b::BufferSlice, pos::Int64, len::Int64, what::AbstractString)
    (pos >= 0 && len >= 0 && len <= b.len && pos <= b.len - len) ||
        throw(ValidationError("$what escapes block metadata"))
    return nothing
end

function _blockload(b::BufferSlice, ::Type{T}, pos::Int64, what::AbstractString) where {T}
    _blockrange(b, pos, Int64(sizeof(T)), what)
    return AC.loadat(b, T, pos)
end

function _blockadd(a::Int64, b::Int64, what::AbstractString)
    try
        return AC.checked_add(a, b)
    catch e
        e isa OverflowError || rethrow()
        throw(ValidationError("$what overflows"))
    end
end

function _blocksub(a::Int64, b::Int64, what::AbstractString)
    try
        return AC.checked_sub(a, b)
    catch e
        e isa OverflowError || rethrow()
        throw(ValidationError("$what overflows"))
    end
end

function _blockmul(a::Int64, b::Int64, what::AbstractString)
    try
        return AC.checked_mul(a, b)
    catch e
        e isa OverflowError || rethrow()
        throw(ValidationError("$what overflows"))
    end
end

struct _BlockTable
    metadata::BufferSlice
    pos::Int64
    vpos::Int64
    vlen::Int64
    olen::Int64
end

function _blocktable(metadata::BufferSlice, pos::Int64, what::AbstractString)
    pos % 4 == 0 || throw(ValidationError("$what is misaligned"))
    back = Int64(_blockload(metadata, Int32, pos, what))
    back != 0 || throw(ValidationError("$what has a zero vtable offset"))
    vpos = _blocksub(pos, back, what)
    vpos % 2 == 0 || throw(ValidationError("$what vtable is misaligned"))
    vlen = Int64(_blockload(metadata, UInt16, vpos, what))
    olen = Int64(_blockload(metadata, UInt16, _blockadd(vpos, Int64(2), what), what))
    vlen >= 4 && iseven(vlen) || throw(ValidationError("invalid $what vtable length $vlen"))
    olen >= 4 || throw(ValidationError("invalid $what object length $olen"))
    _blockrange(metadata, vpos, vlen, what)
    _blockrange(metadata, pos, olen, what)
    return _BlockTable(metadata, pos, vpos, vlen, olen)
end

function _blockfield(
    t::_BlockTable,
    slot::Int,
    width::Int,
    what::AbstractString;
    required::Bool=false,
)
    entryoff = Int64(4 + 2slot)
    if entryoff > t.vlen - 2
        required && throw(ValidationError("required $what is absent"))
        return nothing
    end
    entry = _blockadd(t.vpos, entryoff, what)
    off = Int64(_blockload(t.metadata, UInt16, entry, what))
    if off == 0
        required && throw(ValidationError("required $what is absent"))
        return nothing
    end
    (off >= 4 && width <= t.olen && off <= t.olen - width) ||
        throw(ValidationError("$what exceeds its table object"))
    pos = _blockadd(t.pos, off, what)
    width > 1 && pos % min(width, 8) != 0 && throw(ValidationError("$what is misaligned"))
    _blockrange(t.metadata, pos, Int64(width), what)
    return pos
end

function _blockref(t::_BlockTable, slot::Int, what::AbstractString; required::Bool=false)
    pos = _blockfield(t, slot, 4, what; required=required)
    pos === nothing && return nothing
    rel = Int64(_blockload(t.metadata, UInt32, pos, what))
    rel > 0 || throw(ValidationError("$what has a null or backward offset"))
    target = _blockadd(pos, rel, what)
    _blockrange(t.metadata, target, Int64(1), what)
    return target
end

function _blockvector(t::_BlockTable, slot::Int, elemsize::Int, what::AbstractString)
    pos = _blockref(t, slot, what)
    pos === nothing && return nothing
    pos % 4 == 0 || throw(ValidationError("$what length is misaligned"))
    n = Int64(_blockload(t.metadata, UInt32, pos, what))
    start = _blockadd(pos, Int64(4), what)
    bytes = _blockmul(n, Int64(elemsize), what)
    _blockrange(t.metadata, start, bytes, what)
    n > 0 &&
        elemsize > 1 &&
        start % min(elemsize, 8) != 0 &&
        throw(ValidationError("$what data is misaligned"))
    return start, n
end

"""
Read the fixed Message/RecordBatch envelope and wire-buffer structs that bind
one Footer Block to its on-wire frame. `_blockmessage` still verifies the
full metadata graph lazily. This preflight runs first so that EOS
classification never trusts a forged Footer extent.
"""
function _blockmessagebatch(metadata::BufferSlice)
    root = Int64(_blockload(metadata, UInt32, Int64(0), "message root"))
    root >= 4 || throw(ValidationError("invalid block message root offset"))
    msg = _blocktable(metadata, root, "block message table")
    bodypos = _blockfield(msg, 3, 8, "message body-length slot")
    bodylen =
        bodypos === nothing ? Int64(0) :
        _blockload(metadata, Int64, bodypos, "message body-length slot")
    headerpos = _blockfield(msg, 1, 1, "message header type"; required=true)
    headertype = _blockload(metadata, UInt8, headerpos, "message header type")
    headerref = _blockref(msg, 2, "message header"; required=true)
    header = _blocktable(metadata, headerref, "message header table")
    batch = if headertype == UInt8(2) # DictionaryBatch.data
        dataref = _blockref(header, 1, "dictionary batch data"; required=true)
        _blocktable(metadata, dataref, "dictionary record-batch table")
    elseif headertype == UInt8(3) # RecordBatch
        header
    else
        throw(ValidationError("footer block has unsupported message header type $headertype"))
    end
    return bodylen, headertype, batch
end

function _verifyblockbuffers(batch::_BlockTable, bodylen::Int64)
    buffers = _blockvector(batch, 2, 16, "record-batch buffer vector")
    buffers === nothing && return nothing
    start, n = buffers
    last_nonempty_end = Int64(0)
    for i = Int64(0):(n - 1)
        base = _blockadd(
            start,
            _blockmul(i, Int64(16), "record-batch buffer position"),
            "record-batch buffer position",
        )
        offset = _blockload(batch.metadata, Int64, base, "record-batch buffer offset")
        len = _blockload(
            batch.metadata,
            Int64,
            _blockadd(base, Int64(8), "record-batch buffer length"),
            "record-batch buffer length",
        )
        offset >= 0 || throw(ValidationError("negative batch buffer offset $offset"))
        len >= 0 || throw(ValidationError("negative batch buffer length $len"))
        offset % 8 == 0 ||
            throw(ValidationError("batch buffer offset $offset is not 8-byte aligned"))
        bufferend = _blockadd(offset, len, "batch buffer end")
        bufferend <= bodylen ||
            throw(ValidationError("batch buffer [$offset, $len] escapes its message body"))
        if len > 0
            offset >= last_nonempty_end ||
                throw(ValidationError("batch buffers overlap or move backwards"))
            last_nonempty_end = bufferend
        end
    end
    return nothing
end

function _verifyblockframe(
    blob::BufferSlice,
    block::NTuple{3,Int64},
    expectedheadertype::UInt8,
)
    offset, metalen, bodylen = block
    AC.loadat(blob, UInt32, offset) == CONTINUATION ||
        throw(ValidationError("footer block does not point at a message"))
    declared = Int64(AC.loadat(blob, Int32, offset + 4))
    declared == metalen - 8 ||
        throw(ValidationError("footer block metadata length does not match the message"))
    metadata = AC.subslice(blob, offset + 8, declared)
    messagebodylen, headertype, batch = _blockmessagebatch(metadata)
    messagebodylen == bodylen ||
        throw(ValidationError("footer block body length does not match the message"))
    headertype == expectedheadertype ||
        throw(ValidationError("footer block has the wrong message header type"))
    _verifyblockbuffers(batch, bodylen)
    return nothing
end

function _verifyblockframes(
    blob::BufferSlice,
    dictblocks,
    recordblocks,
    dataend::Int64;
    datastart::Int64=0,
)
    indexedend = _validateblockindex(dictblocks, recordblocks, dataend; datastart=datastart)
    foreach(block -> _verifyblockframe(blob, block, UInt8(2)), dictblocks)
    foreach(block -> _verifyblockframe(blob, block, UInt8(3)), recordblocks)
    return indexedend
end

"""
Frame and verify the single message a Block points at, against the block's
own declared extents and the enclosing region.
"""
function _blockmessage(
    region::OwnerRegion,
    block::NTuple{3,Int64},
    dataend::Int64,
    limits::Limits,
    budget::AllocationBudget,
)
    offset, _ = _blockextent(block, dataend)
    _, metalen, bodylen = block
    blob = BufferSlice(region, 0, region.len)
    AC.loadat(blob, UInt32, offset) == CONTINUATION ||
        throw(ValidationError("footer block does not point at a message"))
    declared = Int64(AC.loadat(blob, Int32, offset + 4))
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
    metabytes = AC.slicebytes(AC.subslice(blob, offset + 8, declared))
    version, header_type, features, _ =
        _verify_ipc_metadata_budgeted(metabytes, limits, budget)
    msg = FB.getrootas(Meta.Message, metabytes, 0)
    Int64(msg.bodyLength) == bodylen ||
        throw(ValidationError("footer block body length does not match the message"))
    return FramedMessage(
        msg,
        AC.subslice(blob, offset + metalen, bodylen),
        version,
        header_type,
        features,
    )
end

"""
    readfile(bytes::Vector{UInt8}; limits=Limits()) -> ArrowFile
    readfile(region::OwnerRegion; limits=Limits()) -> ArrowFile

Open an IPC-format file: verify both magics, the footer length, the footer
flatbuffer, and the schema; eagerly decode the dictionary blocks (shared by
every record batch); expose record batches lazily through the Block index.
Pass `mmapregion(path)` to read a file through Core's mmap path. Duplicate
dictionary ids and delta dictionaries are format errors here — the file
format carries exactly one dictionary batch per id.
"""
readfile(bytes::Vector{UInt8}; limits::Limits=Limits()) =
    _readfile(heapregion(bytes), limits, AllocationBudget(limits.max_total_allocated_bytes))

readfile(region::OwnerRegion; limits::Limits=Limits()) =
    _readfile(region, limits, AllocationBudget(limits.max_total_allocated_bytes))

function _readfile(region::OwnerRegion, limits::Limits, budget::AllocationBudget)
    _requirelittleendian()
    _validatelimits(limits)
    blob = BufferSlice(region, 0, region.len)
    # leading magic + pad (8), smallest footer flatbuffer (8), footer length
    # (4), trailing magic (6).
    minlen = Int64(8 + 8 + 4 + 6)
    region.len >= minlen || throw(ValidationError("file is too short to be an IPC file"))
    for (i, byte) in enumerate(FILE_MAGIC)
        AC.loadat(blob, UInt8, Int64(i - 1)) == byte ||
            throw(ValidationError("missing leading ARROW1 magic"))
        AC.loadat(blob, UInt8, region.len - 6 + (i - 1)) == byte ||
            throw(ValidationError("missing trailing ARROW1 magic"))
    end
    # The trailer is <footer><Int32 footer length><ARROW1>, so the length
    # word sits 10 bytes from the end.
    footerlen = Int64(AC.loadat(blob, Int32, region.len - 10))
    0 < footerlen <= limits.max_metadata_bytes || throw(
        ValidationError(
            "footer length $footerlen outside (0, $(limits.max_metadata_bytes)]",
        ),
    )
    footerstart = region.len - 10 - footerlen
    footerstart >= 8 || throw(ValidationError("footer escapes the file"))
    _chargevector!(budget, UInt8, footerlen, "footer allocation")
    footerbytes = AC.slicebytes(AC.subslice(blob, footerstart, footerlen))
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
    metaschema === nothing || (
        something(metaschema.endianness, Meta.Endianness.Little) ==
        Meta.Endianness.Little || throw(
            ValidationError(
                "big-endian IPC is not supported (no endianness normalization)",
            ),
        )
    )
    metaschema === nothing && throw(ValidationError("file footer carries no schema"))
    # Validate the leading schema and indexed messages against the Footer
    # boundary first. Only then can the final eight bytes be classified as an
    # optional EOS marker: a no-EOS file may end its last data buffer with the
    # same byte pattern.
    schemafm, schemaend = _fileschema(region, footerstart, limits, budget)
    schemafm.version == version ||
        throw(ValidationError("file schema and footer metadata versions differ"))
    schemafm.features == features ||
        throw(ValidationError("file schema and footer features differ"))
    _schemaequal(schemafm.msg.header::Meta.Schema, metaschema) ||
        throw(ValidationError("file schema and footer schema differ"))
    _metadataequal(schemafm.msg.custom_metadata, footer.custom_metadata) ||
        throw(ValidationError("file schema and footer custom metadata differ"))
    indexedend =
        _verifyblockframes(blob, dictblocks, recordblocks, footerstart; datastart=schemaend)
    haseos =
        footerstart - indexedend >= 8 &&
        AC.loadat(blob, UInt32, footerstart - 8) == CONTINUATION &&
        AC.loadat(blob, UInt32, footerstart - 4) == UInt32(0)
    dataend = haseos ? footerstart - 8 : footerstart
    _validateblockindex(dictblocks, recordblocks, dataend; datastart=schemaend)
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
    dicts = Dict{Int64,ArrayData}()
    validated = AC._ValidatedDictionaries()
    state = DecodeState(budget)
    try
        for block in dictblocks
            fm = _blockmessage(region, block, dataend, limits, budget)
            fm.version == version ||
                throw(ValidationError("IPC metadata version changes within the file"))
            rejectexperimentalcompression(fm)
            header = fm.msg.header
            header isa Meta.DictionaryBatch ||
                throw(ValidationError("footer dictionary block is not a dictionary batch"))
            header.isDelta && throw(ValidationError("delta dictionaries are not supported"))
            haskey(dictids, header.id) ||
                throw(ValidationError("dictionary batch has unknown id $(header.id)"))
            haskey(dicts, header.id) && throw(
                ValidationError("the file format carries one dictionary batch per id"),
            )
            rb = header.data
            codec = _batchcodec(rb.compression, fm.version)
            vf = dictvaluefields[header.id]
            rblen = something(rb.length, Int64(0))
            0 <= rblen <= limits.max_array_length ||
                throw(ValidationError("dictionary batch length $rblen exceeds limit"))
            cursor = DecodeCursor(
                rb.nodes,
                rb.buffers,
                fm.body,
                limits;
                codec=codec,
                state=state,
                variadics=variadiccounts(rb),
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
        # Every id a record batch may reference must be resolvable now unless
        # that batch proves all-null use — checked per batch at decode.
        return ArrowFile(
            region,
            sch,
            AC.FrozenVector{Field}(fields),
            fielddictids,
            dicts,
            validated,
            recordblocks,
            dataend,
            limits,
            version,
        )
    finally
        close(state)
    end
end

function _filebatch(f::ArrowFile, i::Integer, state::DecodeState)
    1 <= i <= length(f.recordblocks) || throw(BoundsError(f, i))
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, state.budget)
    fm.version == f.schemaversion ||
        throw(ValidationError("IPC metadata version changes within the file"))
    rejectexperimentalcompression(fm)
    fm.msg.header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    for id in missingdicts(f.fields, fm.msg.header.nodes, f.dictionaries, f.fielddictids)
        throw(
            ValidationError(
                "record batch references dictionary id $id " *
                "with no dictionary batch in the file",
            ),
        )
    end
    return decoderecord(
        fm,
        f.fields,
        f.schema,
        f.dictionaries,
        f.fielddictids,
        f.limits,
        f.validated,
        state,
    )
end

function Base.getindex(f::ArrowFile, i::Integer)
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    state = DecodeState(budget)
    try
        return _filebatch(f, i, state)
    finally
        close(state)
    end
end
