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
# PROVE-OUT: the IPC WRITE half of the adapter, over the same ArrowCore.
#
# Run with the repo project (the reader example supplies framing, the
# verifier, the metadata mapping, and 2.x for interop fixtures):
#
#     julia --project=. core/examples/ipc_write.jl
#
# What this demonstrates, mapped to the redesign report:
#
#   * §9 "batch encode is the inverse, one implementation": ONE generic
#     `encodefield!` walks the SAME `layoutspec` registry the decoder walks —
#     node, registry buffers in registry order, children in declared order.
#     There are no per-layout write methods to skew against the read side.
#
#   * §9 "dictionary state machine, replacement-on-change": each batch's
#     pools are captured by identity. A dictionary batch is emitted before
#     the first record batch that references its id and again ONLY when a
#     later batch's pool for that id is a different snapshot. Replacement
#     streams declare Feature.DICTIONARY_REPLACEMENT in the schema.
#
#   * §9 "compression at encode": per-buffer LZ4_FRAME/ZSTD with the Int64
#     uncompressed-length prefix, the `-1` stored-raw fallback when
#     compression does not help, codec objects owned per writer and
#     explicitly finalized. Compressed streams declare Feature.COMPRESSED_BODY
#     (2.x omits the declaration; the read side accepts both).
#
#   * File format = stream framing + a Block index + a Footer (§9): the
#     writer isolates footer bookkeeping from generic message writing;
#     `readfile` exposes the footer's record-batch index as a lazy
#     random-access handle (`length`/`getindex`) over one borrowed or mmapped
#     region — the report's `ArrowFile` shape (#353/#434).
#
# Acceptance at the bottom: bytes written here are read back by BOTH this
# adapter's reader and by today's Arrow.jl 2.x, element-for-element, plus
# adversarial writer-refusal and file-index cases. New core, real bytes,
# both directions.
# =============================================================================

include(joinpath(@__DIR__, "ipc_read.jl"))

# TranscodingStreams comes through the codec packages (it is not a direct
# repo dependency); both codecs share one streams API.
const TS = CLZ4.TranscodingStreams

# ---------------------------------------------------------------------------
# Encode-side codec state: per-writer objects, explicitly finalized
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

_metatimeunit(u) = u == AC.SECOND ? Meta.TimeUnit.SECOND :
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
        Meta.floatingPointAddPrecision(b, t.bits == 16 ? Meta.Precision.HALF :
            t.bits == 32 ? Meta.Precision.SINGLE : Meta.Precision.DOUBLE)
        return Meta.FloatingPoint, Meta.floatingPointEnd(b)
    elseif t isa BoolType
        Meta.boolStart(b)
        return Meta.Bool, Meta.boolEnd(b)
    elseif t isa Utf8Type
        if t.large
            # `largUtf8Start` is the vendored binding's own (typo) name.
            Meta.largUtf8Start(b)
            return Meta.LargeUtf8, Meta.largUtf8End(b)
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
        Meta.dateAddUnit(b, t.unit == AC.DAY ? Meta.DateUnit.DAY :
            Meta.DateUnit.MILLISECOND)
        return Meta.Date, Meta.dateEnd(b)
    elseif t isa TimeType
        Meta.timeStart(b)
        Meta.timeAddUnit(b, _metatimeunit(t.unit))
        Meta.timeAddBitWidth(b, Int32(t.bits))
        return Meta.Time, Meta.timeEnd(b)
    elseif t isa TimestampType
        tz = t.timezone === nothing ? FB.UOffsetT(0) :
            FB.createstring!(b, t.timezone)
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
    elseif t isa NullType
        Meta.nullStart(b)
        return Meta.Null, Meta.nullEnd(b)
    else
        throw(ValidationError("IPC writer does not map descriptor " *
            "$(AC.descriptorname(t)); union, interval, view, and REE IPC " *
            "mapping is outside this prove-out"))
    end
end

function _metakeyvalues!(b::FB.Builder, metadata)
    metadata === nothing && return FB.UOffsetT(0)
    pairs = sort!(collect(metadata); by=first)
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
        any(_containsdictionary, f.children) &&
            throw(ValidationError("children of an IPC dictionary field cannot be dictionary encoded"))
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

function _schemamessage!(out::Vector{UInt8}, sch::Schema,
    fielddictids::IdDict{Field,Int64}, features::Vector{Int64})
    b = FB.Builder(1024)
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
    # The vendored Schema binding predates `features`; build the four-slot
    # table directly (same bridge the reader fixtures use).
    FB.startobject!(b, 4)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fieldvec)
    kvvec == 0 || Meta.schemaAddCustomMetadata(b, kvvec)
    featurevec == 0 || FB.prependoffsetslot!(b, 3, featurevec, 0)
    schoff = FB.endobject!(b)
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
end
EncodeCursor(codec::Int8, state::Union{Nothing,EncodeState}) =
    EncodeCursor(NTuple{2,Int64}[], NTuple{2,Int64}[], UInt8[], codec, state)

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
    d.offset == 0 ||
        throw(ValidationError("IPC encode of offset array views is outside this prove-out; materialize first"))
    push!(c.nodes, (d.len, AC.nullcount(d)))
    spec = layoutspec(t)
    spec.variadic &&
        throw(ValidationError("IPC writer does not map variadic layouts"))
    length(d.buffers) == length(spec.buffers) ||
        throw(ValidationError("column buffer count does not match its layout"))
    for b in d.buffers
        encodebuffer!(c, AC.slicebytes(b))
    end
    t isa DictionaryType && return nothing
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    length(d.children) == nchildren ||
        throw(ValidationError("column child count does not match its schema field"))
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
        Meta.bodyCompressionAddCodec(b, c.codec == CODEC_LZ4_FRAME ?
            Meta.CompressionType.LZ4_FRAME : Meta.CompressionType.ZSTD)
        compression = Meta.bodyCompressionEnd(b)
    end
    Meta.recordBatchStart(b)
    Meta.recordBatchAddLength(b, nrows)
    Meta.recordBatchAddNodes(b, nodes)
    Meta.recordBatchAddBuffers(b, buffers)
    compression == 0 || Meta.recordBatchAddCompression(b, compression)
    return Meta.recordBatchEnd(b)
end

function _recordmessage!(out::Vector{UInt8}, batch::AC.RecordBatch,
    fields, codec::Int8, state::Union{Nothing,EncodeState})
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

function _dictionarymessage!(out::Vector{UInt8}, id::Int64, vf::Field,
    pool::ArrayData, codec::Int8, state::Union{Nothing,EncodeState})
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

const CODEC_NAMES = Dict{Symbol,Int8}(:none => CODEC_NONE,
    :lz4 => CODEC_LZ4_FRAME, :zstd => CODEC_ZSTD)

"""
Assign one IPC dictionary id per dictionary-typed field, depth-first over the
schema — the writer-side half of the adapter id table (report §9: ids are
adapter bookkeeping; Core fields never carry them).
"""
function assigndictids(fields)
    ids = IdDict{Field,Int64}()
    next = Ref(Int64(0))
    function walk(f::Field)
        if f.type isa DictionaryType
            ids[f] = next[]
            next[] += 1
            return
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

"""
Which features must the schema declare for these batches? Replacement is
detected by pool-identity change per id across the batch sequence
(replacement-on-change, report §9); compression declares COMPRESSED_BODY.
"""
function _streamfeatures(sch::Schema, batches, ids::IdDict{Field,Int64},
    codec::Int8)
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
    replacement && push!(features, Int64(1))   # Feature.DICTIONARY_REPLACEMENT
    codec == CODEC_NONE || push!(features, Int64(2))  # Feature.COMPRESSED_BODY
    return features
end

"""
    writestream(sch, batches; compress=:none) -> Vector{UInt8}

Encode a complete IPC stream: schema message, dictionary batches emitted
before the first record batch that references them (and again on
pool-identity change), record batches, end-of-stream marker. Every column is
semantically validated before any of its bytes are emitted — the writer
refuses to publish data Core would refuse to read.
"""
function writestream(sch::Schema, batches::AbstractVector{AC.RecordBatch};
    compress::Symbol=:none)
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkbatches(sch, batches)
    foreach(validateschemafield, sch.fields)
    ids = assigndictids(sch.fields)
    fielddictids = IdDict{Field,Int64}(ids)
    validated = AC._ValidatedDictionaries()
    for batch in batches
        # Certify each new pool snapshot once (identity-cached across
        # batches), then validate every column against its field contract —
        # the writer refuses to publish what the reader would refuse.
        for (f, pool) in dictionarypools(sch.fields, batch.columns)
            if !haskey(validated, pool)
                validate_semantic(AC.dictvaluefield(f, f.type::DictionaryType), pool)
                validated[pool] = nothing
            end
        end
        for (f, col) in zip(sch.fields, batch.columns)
            AC._validate_semantic(f, col, validated)
        end
    end
    out = UInt8[]
    state = codec == CODEC_NONE ? nothing : EncodeState()
    try
        _schemamessage!(out, sch, fielddictids,
            _streamfeatures(sch, batches, ids, codec))
        current = Dict{Int64,ArrayData}()
        for batch in batches
            for (f, pool) in dictionarypools(sch.fields, batch.columns)
                id = ids[f]
                get(current, id, nothing) === pool && continue
                vf = AC.dictvaluefield(f, f.type::DictionaryType)
                _dictionarymessage!(out, id, vf, pool, codec, state)
                current[id] = pool
            end
            _recordmessage!(out, batch, sch.fields, codec, state)
        end
        append!(out, reinterpret(UInt8, UInt32[CONTINUATION, UInt32(0)]))
        return out
    finally
        state === nothing || close(state)
    end
end

writestream(s::IPCStream; compress::Symbol=:none) =
    writestream(s.schema, s.batches; compress=compress)

# ---------------------------------------------------------------------------
# File format: magic + stream messages + Block index + Footer
# ---------------------------------------------------------------------------

const FILE_MAGIC = b"ARROW1"

"""
    writefile(sch, batches; compress=:none) -> Vector{UInt8}

The file variant: leading magic, the same stream messages, an end-of-stream
marker, then the Footer with its dictionary and record-batch Block indexes,
the Int32 footer length, and the trailing magic. Footer bookkeeping is
isolated here; message writing is the stream code above. The file format
carries exactly one dictionary batch per id, so batches whose pools change
identity are a clean refusal (the stream format handles replacement).
"""
function writefile(sch::Schema, batches::AbstractVector{AC.RecordBatch};
    compress::Symbol=:none)
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkbatches(sch, batches)
    foreach(validateschemafield, sch.fields)
    ids = assigndictids(sch.fields)
    isempty(_streamfeatures(sch, batches, ids, CODEC_NONE)) ||
        throw(ValidationError("the IPC file format carries one dictionary batch per id; " *
            "changing pools require the stream format"))
    fielddictids = IdDict{Field,Int64}(ids)
    validated = AC._ValidatedDictionaries()
    for batch in batches
        for (f, col) in zip(sch.fields, batch.columns)
            AC._validate_semantic(f, col, validated)
        end
        for (_, pool) in dictionarypools(sch.fields, batch.columns)
            validated[pool] = nothing
        end
    end
    out = UInt8[]
    append!(out, FILE_MAGIC)
    append!(out, zeros(UInt8, 2))            # pad to 8 before the first message
    state = codec == CODEC_NONE ? nothing : EncodeState()
    dictblocks = NTuple{3,Int64}[]           # (offset, metalen, bodylen)
    recordblocks = NTuple{3,Int64}[]
    try
        _schemamessage!(out, sch, fielddictids, Int64[])
        emitted = Set{Int64}()
        function block!(blocks, emit!)
            offset = Int64(length(out))
            emit!()
            # metaDataLength spans prefix + metadata (up to the body start).
            total = Int64(length(out)) - offset
            metalen = Int64(8) + Int64(reinterpret(UInt32,
                out[(offset + 5):(offset + 8)])[1])
            push!(blocks, (offset, metalen, total - metalen))
            return nothing
        end
        for batch in batches
            for (f, pool) in dictionarypools(sch.fields, batch.columns)
                id = ids[f]
                id in emitted && continue
                push!(emitted, id)
                vf = AC.dictvaluefield(f, f.type::DictionaryType)
                block!(dictblocks,
                    () -> _dictionarymessage!(out, id, vf, pool, codec, state))
            end
            block!(recordblocks,
                () -> _recordmessage!(out, batch, sch.fields, codec, state))
        end
        append!(out, reinterpret(UInt8, UInt32[CONTINUATION, UInt32(0)]))
        # Footer: schema again, then the two Block struct-vectors.
        b = FB.Builder(1024)
        fields = FB.UOffsetT[metafield!(b, f, fielddictids) for f in sch.fields]
        Meta.schemaStartFieldsVector(b, length(fields))
        foreach(x -> FB.prependoffset!(b, x), Iterators.reverse(fields))
        fieldvec = FB.endvector!(b, length(fields))
        kvvec = _metakeyvalues!(b, sch.metadata)
        Meta.schemaStart(b)
        Meta.schemaAddEndianness(b, Meta.Endianness.Little)
        Meta.schemaAddFields(b, fieldvec)
        kvvec == 0 || Meta.schemaAddCustomMetadata(b, kvvec)
        schoff = Meta.schemaEnd(b)
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
        return out
    finally
        state === nothing || close(state)
    end
end

writefile(s::IPCStream; compress::Symbol=:none) =
    writefile(s.schema, s.batches; compress=compress)

# ---------------------------------------------------------------------------
# File reader: footer verification + lazy random-access batch handle
# ---------------------------------------------------------------------------

function _vblockvector(t::_VTable, slot::Int, state::_VState)
    vec = _vvector(t, slot, 24; state=state)
    vec === nothing && return NTuple{3,Int64}[]
    start, n = vec
    blocks = Vector{NTuple{3,Int64}}(undef, n)
    for i = 0:(n - 1)
        base = start + 24i
        blocks[i + 1] = (_vi64(t.bytes, base),
            Int64(_vi32(t.bytes, base + 8)), _vi64(t.bytes, base + 16))
    end
    return blocks
end

"""
Byte-wise Footer verification (same bridge role as `verify_ipc_metadata`):
bound the whole table graph, then return the verified Block indexes. The
schema subgraph reuses the message verifier's `_vschema`.
"""
function verify_footer(bytes::Vector{UInt8}, limits::Limits)
    state = _VState(limits, limits.max_total_allocated_bytes)
    length(bytes) >= 4 || _vfail("missing footer root offset")
    root = Int64(_vu32(bytes, 0))
    t = _vtable(bytes, root)
    _vvisit!(state, :footer, t)
    vp = _vfield(t, 0, 2)
    version = vp === nothing ? Int16(0) : reinterpret(Int16, _vu16(bytes, vp))
    version in (Int16(3), Int16(4)) ||
        _vfail("unsupported footer version $version (only V4/V5 are accepted)")
    sp = _vref(t, 1; required=true)
    _vschema(_vtable(bytes, sp), state, 0)
    dictblocks = _vblockvector(t, 2, state)
    recordblocks = _vblockvector(t, 3, state)
    return version, dictblocks, recordblocks
end

"""
    ArrowFile

The footer's record-batch index as a random-access handle (report §9,
the #353/#434 shape): `length(file)` batches, `file[i]` decodes batch `i` on
demand — nothing is decoded at open beyond the schema and the dictionary
batches every record shares. Each `getindex` decodes fresh from the mapped
bytes with its own allocation budget and codec contexts; the handle itself
is immutable after open, so concurrent `getindex` calls are safe by
construction. The region root (heap vector or Mmap array) is the only
lifetime anchor, exactly as in Core.
"""
struct ArrowFile
    region::OwnerRegion
    schema::Schema
    fields::AC.FrozenVector{Field}
    fielddictids::IdDict{Field,Int64}
    dictionaries::Dict{Int64,ArrayData}
    validated::AC._ValidatedDictionaries
    recordblocks::Vector{NTuple{3,Int64}}
    limits::Limits
    schemaversion::Int16
end

Base.length(f::ArrowFile) = length(f.recordblocks)
AC.schema(f::ArrowFile) = f.schema

"""
Frame and verify the single message a Block points at, against the block's
own declared extents and the enclosing region.
"""
function _blockmessage(region::OwnerRegion, block::NTuple{3,Int64},
    limits::Limits, budget::AllocationBudget)
    offset, metalen, bodylen = block
    blob = BufferSlice(region, 0, region.len)
    (offset >= 0 && metalen >= 16 && bodylen >= 0) ||
        throw(ValidationError("footer block has invalid extents"))
    offset % 8 == 0 || throw(ValidationError("footer block is not 8-byte aligned"))
    metalen % 8 == 0 ||
        throw(ValidationError("footer block metadata length is not 8-byte aligned"))
    bodylen % 8 == 0 ||
        throw(ValidationError("footer block body length is not 8-byte aligned"))
    frameend = AC.checked_add(AC.checked_add(offset, metalen), bodylen)
    frameend <= region.len ||
        throw(ValidationError("footer block escapes the file"))
    AC.loadat(blob, UInt32, offset) == CONTINUATION ||
        throw(ValidationError("footer block does not point at a message"))
    declared = Int64(AC.loadat(blob, Int32, offset + 4))
    declared == metalen - 8 ||
        throw(ValidationError("footer block metadata length does not match the message"))
    _charge!(budget, declared, "metadata allocation")
    metabytes = AC.slicebytes(AC.subslice(blob, offset + 8, declared))
    version, header_type, features, reserve =
        verify_ipc_metadata(metabytes, limits, budget.left)
    _charge!(budget, reserve, "verified metadata expansion")
    msg = FB.getrootas(Meta.Message, metabytes, 0)
    Int64(msg.bodyLength) == bodylen ||
        throw(ValidationError("footer block body length does not match the message"))
    return FramedMessage(msg, AC.subslice(blob, offset + metalen, bodylen),
        version, header_type, features)
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
    readfile(heapregion(bytes); limits=limits)

function readfile(region::OwnerRegion; limits::Limits=Limits())
    blob = BufferSlice(region, 0, region.len)
    minlen = Int64(8 + 8 + 4 + 6)
    region.len >= minlen ||
        throw(ValidationError("file is too short to be an IPC file"))
    for (i, byte) in enumerate(FILE_MAGIC)
        AC.loadat(blob, UInt8, Int64(i - 1)) == byte ||
            throw(ValidationError("missing leading ARROW1 magic"))
        AC.loadat(blob, UInt8, region.len - 6 + (i - 1)) == byte ||
            throw(ValidationError("missing trailing ARROW1 magic"))
    end
    footerlen = Int64(AC.loadat(blob, Int32, region.len - 10))
    0 < footerlen <= limits.max_metadata_bytes ||
        throw(ValidationError("footer length $footerlen outside (0, $(limits.max_metadata_bytes)]"))
    footerstart = region.len - 10 - footerlen
    footerstart >= 8 ||
        throw(ValidationError("footer escapes the file"))
    budget = AllocationBudget(limits.max_total_allocated_bytes)
    _charge!(budget, footerlen, "footer allocation")
    footerbytes = AC.slicebytes(AC.subslice(blob, footerstart, footerlen))
    version, dictblocks, recordblocks = verify_footer(footerbytes, limits)
    footer = FB.getrootas(Meta.Footer, footerbytes, 0)
    metaschema = footer.schema
    metaschema === nothing ||
        (something(metaschema.endianness, Meta.Endianness.Little) == Meta.Endianness.Little ||
        throw(ValidationError("big-endian IPC requires normalization, which is outside this prove-out")))
    metaschema === nothing &&
        throw(ValidationError("file footer carries no schema"))
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()
    fields = Field[corefield(f, dictids, fielddictids)
                   for f in something(metaschema.fields, Meta.Field[])]
    foreach(validateschemafield, fields)
    dictvaluefields = validatedictionaryids(fields, fielddictids)
    sch = Schema(fields; metadata=coremetadata(metaschema.custom_metadata),
        endianness=AC.LittleEndian)
    dicts = Dict{Int64,ArrayData}()
    validated = AC._ValidatedDictionaries()
    state = DecodeState(budget)
    try
        for block in dictblocks
            fm = _blockmessage(region, block, limits, budget)
            fm.version == version ||
                throw(ValidationError("IPC metadata version changes within the file"))
            rejectexperimentalcompression(fm)
            header = fm.msg.header
            header isa Meta.DictionaryBatch ||
                throw(ValidationError("footer dictionary block is not a dictionary batch"))
            header.isDelta &&
                throw(ValidationError("delta dictionaries are outside this prove-out"))
            haskey(dictids, header.id) ||
                throw(ValidationError("dictionary batch has unknown id $(header.id)"))
            haskey(dicts, header.id) &&
                throw(ValidationError("the file format carries one dictionary batch per id"))
            rb = header.data
            codec = _batchcodec(rb.compression, fm.version)
            isempty(something(rb.variadicBufferCounts, Int64[])) ||
                throw(ValidationError("variadic-buffer layouts are outside this prove-out"))
            vf = dictvaluefields[header.id]
            rblen = something(rb.length, Int64(0))
            0 <= rblen <= limits.max_array_length ||
                throw(ValidationError("dictionary batch length $rblen exceeds limit"))
            cursor = DecodeCursor(rb.nodes, rb.buffers, fm.body, limits;
                codec=codec, state=state)
            decoded = decodefield(vf, cursor, dicts, fielddictids)
            finishcursor!(cursor)
            decoded.len == rblen ||
                throw(ValidationError("dictionary RecordBatch length does not match its field node"))
            validate_semantic(vf, decoded)
            validated[decoded] = nothing
            dicts[header.id] = decoded
        end
        # Every id a record batch may reference must be resolvable now unless
        # that batch proves all-null use — checked per batch at decode.
        return ArrowFile(region, sch, AC.FrozenVector{Field}(fields),
            fielddictids, dicts, validated, recordblocks, limits, version)
    finally
        close(state)
    end
end

function Base.getindex(f::ArrowFile, i::Integer)
    1 <= i <= length(f.recordblocks) || throw(BoundsError(f, i))
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    fm = _blockmessage(f.region, f.recordblocks[i], f.limits, budget)
    fm.version == f.schemaversion ||
        throw(ValidationError("IPC metadata version changes within the file"))
    rejectexperimentalcompression(fm)
    fm.msg.header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    for id in missingdicts(f.fields, fm.msg.header.nodes, f.dictionaries,
        f.fielddictids)
        throw(ValidationError("record batch references dictionary id $id " *
            "with no dictionary batch in the file"))
    end
    state = DecodeState(budget)
    try
        return decoderecord(fm, f.fields, f.schema, f.dictionaries,
            f.fielddictids, f.limits, f.validated, state)
    finally
        close(state)
    end
end

# ---------------------------------------------------------------------------
# Acceptance: this writer's bytes, read by Core AND by Arrow.jl 2.x
# ---------------------------------------------------------------------------

function _materialized(stream)
    return [[materialize(f, b.columns[i])
             for (i, f) in enumerate(stream.schema.fields)]
            for b in stream.batches]
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

function _assert_2x_reads(bytes::Vector{UInt8}, stream)
    tbl = Arrow.Table(IOBuffer(bytes))
    cols = Tables.columns(tbl)
    names = Tables.columnnames(cols)
    @assert length(names) == length(stream.schema.fields)
    total = [reduce(vcat, [collect(Any, materialize(f, b.columns[i]))
                           for b in stream.batches]; init=Any[])
             for (i, f) in enumerate(stream.schema.fields)]
    for (i, name) in enumerate(names)
        got = collect(Any, Tables.getcolumn(cols, name))
        want = total[i]
        # 2.x materializes structs as NamedTuples; Core scalars are ordered
        # pairs. Compare through one canonical form.
        canon(x) = x isa NamedTuple ? [String(k) => canon(v) for (k, v) in pairs(x)] :
            x isa AbstractVector{<:Pair} ? [k => canon(v) for (k, v) in x] :
            x isa AbstractVector ? Any[canon(v) for v in x] :
            x isa AbstractDict ? sort!([k => canon(v) for (k, v) in x]; by=first) :
            x
        @assert isequal(canon.(got), canon.(want)) "2.x column $name mismatch"
    end
    return nothing
end

function main()
    # The same fixture table the read acceptance uses: 2.x writes it, Core
    # decodes it, and from here on the WRITER is the system under test.
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

    # Stream round-trip: our writer -> our reader.
    bytes = writestream(source)
    roundtrip = readstream(bytes)
    _assert_stream_equal(source, roundtrip)
    println("writer -> reader stream round-trip ✓")

    # Stream interop: our writer -> Arrow.jl 2.x.
    _assert_2x_reads(bytes, source)
    println("2.x reads this writer's stream ✓")

    # The dictionary batch is emitted once: the second batch reuses the same
    # pool snapshot, so no replacement message and no feature declaration.
    kinds = [f.kind for f in _frameinfo(bytes)]
    @assert count(==(UInt8(2)), kinds) == 1
    @assert isempty(framemessages(heapregion(copy(bytes)))[1].features)
    println("unchanged pools write one dictionary batch (replacement-on-change) ✓")

    # Compressed round-trips, both codecs, both directions.
    for codec in (:lz4, :zstd)
        cbytes = writestream(source; compress=codec)
        cstream = readstream(cbytes)
        _assert_stream_equal(source, cstream)
        _assert_2x_reads(cbytes, source)
        # The compression feature is declared (standards-conforming; 2.x
        # omits it and the reader accepts both).
        cframes = framemessages(heapregion(copy(cbytes)))
        @assert Int64(2) in cframes[1].features
        println("$(codec)-compressed writer stream round-trips (Core + 2.x) ✓")
    end

    # Incompressible buffers fall back to the -1 stored-raw prefix.
    rng_bytes = Vector{UInt8}(reinterpret(UInt8, hash.(1:4096)))
    rawio = IOBuffer()
    Arrow.write(rawio, (x=rng_bytes,); file=false)
    rawsource = readstream(take!(rawio))
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
    zerorow = readstream(writestream(readstream(
        let z = IOBuffer(); Arrow.write(z, (x=Int64[],); file=false); take!(z) end)))
    @assert zerorow.batches[1].nrows == 0
    println("schema-only and zero-row streams round-trip ✓")

    # Schema and field metadata round-trip through the writer.
    mio = IOBuffer()
    Arrow.write(mio, (x=Int64[1],); file=false,
        metadata=Dict("owner" => "jacob"),
        colmetadata=Dict(:x => Dict("unit" => "count")))
    msource = readstream(take!(mio))
    mstream = readstream(writestream(msource))
    @assert Dict(mstream.schema.metadata) == Dict("owner" => "jacob")
    @assert Dict(mstream.schema.fields[1].metadata) == Dict("unit" => "count")
    println("schema and field metadata round-trip through the writer ✓")

    # Writer refusals: offset views, mismatched schemas, unknown codecs.
    off = ArrayData(IntType(64, true), 1,
        source.batches[1].columns[1].buffers; offset=1)
    offbatch = AC.RecordBatch(Schema(Field[source.schema.fields[1]]),
        ArrayData[off], 1)
    @assert _rejects(() -> writestream(offbatch.schema, [offbatch]))
    @assert _rejects(() -> writestream(Schema(Field[]), [source.batches[1]]))
    caught = try
        writestream(source; compress=:snappy)
        false
    catch e
        e isa ArgumentError
    end
    @assert caught
    println("offset views, schema mismatches, and unknown codecs are refused ✓")

    # ---- File format ----------------------------------------------------

    filebytes = writefile(source)
    file = readfile(copy(filebytes))
    @assert length(file) == 2
    # Random access, last batch first — nothing but the footer index drives it.
    for i in (2, 1)
        batch = file[i]
        for (j, f) in enumerate(file.schema.fields)
            want = materialize(f, source.batches[i].columns[j])
            @assert isequal(collect(Any, materialize(f, batch.columns[j])),
                collect(Any, want))
        end
    end
    println("writer -> readfile random-access round-trip ✓")

    # 2.x reads our file; we read a 2.x file.
    filetbl = Arrow.Table(IOBuffer(copy(filebytes)))
    @assert length(Tables.getcolumn(Tables.columns(filetbl), 1)) == 10
    fio = IOBuffer()
    Arrow.write(fio, Tables.partitioner([expected, expected]); file=true)
    theirs = readfile(take!(fio))
    @assert length(theirs) == 2
    for i = 1:2, (j, f) in enumerate(theirs.schema.fields)
        @assert isequal(collect(Any, materialize(f, theirs[i].columns[j])),
            collect(Any, materialize(f, source.batches[i].columns[j])))
    end
    println("file interop holds in both directions with 2.x ✓")

    # Compressed file round-trip.
    zfile = readfile(writefile(source; compress=:zstd))
    for (j, f) in enumerate(zfile.schema.fields)
        @assert isequal(collect(Any, materialize(f, zfile[1].columns[j])),
            collect(Any, materialize(f, source.batches[1].columns[j])))
    end
    println("compressed files round-trip ✓")

    # Mmap path: the file region's root is the Mmap array; decode after GC.
    mmapdir = mktempdir()
    mmappath = joinpath(mmapdir, "roundtrip.arrow")
    write(mmappath, filebytes)
    mfile = readfile(mmapregion(mmappath))
    GC.gc(true)
    @assert length(mfile) == 2
    @assert isequal(
        collect(Any, materialize(mfile.schema.fields[1], mfile[2].columns[1])),
        collect(Any, materialize(source.schema.fields[1], source.batches[2].columns[1])))
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
    # A block offset pointing outside the file must fail cleanly.
    file2 = readfile(copy(filebytes))
    badblocks = [(Int64(2)^40, Int64(16), Int64(0))]
    badfile = ArrowFile(file2.region, file2.schema, file2.fields,
        file2.fielddictids, file2.dictionaries, file2.validated, badblocks,
        file2.limits, file2.schemaversion)
    @assert _rejects(() -> badfile[1])
    println("file magic, footer, and block extents are verified ✓")

    println()
    println("IPC write, file-format, interop, and adversarial checks passed.")
end

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    main()
end
