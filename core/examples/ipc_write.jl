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
# Acceptance at the bottom: representative bytes written here are read back
# by BOTH this adapter's reader and by today's Arrow.jl 2.x,
# element-for-element. Custom union ids are verified through Core because
# 2.x indexes children by id instead of the schema's id-to-child mapping.
# Adversarial writer-refusal and file-index cases cover the boundaries.
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
    elseif t isa IntervalType
        Meta.intervalStart(b)
        Meta.intervalAddUnit(b, t.unit == AC.YEAR_MONTH ? Meta.IntervalUnit.YEAR_MONTH :
            t.unit == AC.DAY_TIME ? Meta.IntervalUnit.DAY_TIME :
            Meta.IntervalUnit.MONTH_DAY_NANO)
        return Meta.Interval, Meta.intervalEnd(b)
    elseif t isa UnionType
        Meta.unionStartTypeIdsVector(b, length(t.typeids))
        foreach(x -> FB.prepend!(b, Int32(x)), Iterators.reverse(t.typeids))
        idvec = FB.endvector!(b, length(t.typeids))
        Meta.unionStart(b)
        Meta.unionAddMode(b, t.mode == AC.DenseMode ? Meta.UnionMode.Dense :
            Meta.UnionMode.Sparse)
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
        throw(ValidationError("IPC writer does not map descriptor " *
            "$(AC.descriptorname(t))"))
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

function _metaschema!(b::FB.Builder, sch::Schema,
    fielddictids::IdDict{Field,Int64}, features::Vector{Int64})
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

function _schemamessage!(out::Vector{UInt8}, sch::Schema,
    fielddictids::IdDict{Field,Int64}, features::Vector{Int64})
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
    EncodeCursor(NTuple{2,Int64}[], NTuple{2,Int64}[], UInt8[], codec, state,
        Int64[])

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
            throw(ValidationError(
                "IPC sparse-union children must equal the union length"))
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
        Meta.bodyCompressionAddCodec(b, c.codec == CODEC_LZ4_FRAME ?
            Meta.CompressionType.LZ4_FRAME : Meta.CompressionType.ZSTD)
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

function _requirelittleendian(host_endian_bom::UInt32=Base.ENDIAN_BOM)
    host_endian_bom == UInt32(0x04030201) ||
        throw(ValidationError("this prove-out requires a little-endian host"))
    return nothing
end

"""
Assign one IPC dictionary id per dictionary-typed field, depth-first over the
schema — the writer-side half of the adapter id table (report §9: ids are
adapter bookkeeping; Core fields never carry them).
"""
function assigndictids(fields, given::IdDict{Field,Int64}=IdDict{Field,Int64}())
    # `given` lets a caller preserve ids from a source (a reader's table): two
    # fields sharing one id then share one dictionary batch, exactly as the
    # source did (4.0.0-shareddict). Fresh ids never collide with given ones.
    ids = IdDict{Field,Int64}(given)
    seen = IdDict{Field,Nothing}()
    next = Ref(isempty(given) ? Int64(0) : maximum(values(given)) + 1)
    function walk(f::Field)
        haskey(seen, f) && throw(ValidationError(
            "IPC writer schema reuses one Field object in multiple positions"))
        seen[f] = nothing
        if f.type isa DictionaryType && !haskey(ids, f)
            ids[f] = next[]
            next[] += 1
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
        isvalid(f.name) ||
            throw(ValidationError("field name is not valid UTF-8"))
        AC._validate_metadata(f.metadata, "field")
        foreach(walk, f.children)
        return nothing
    end
    foreach(walk, sch.fields)
    foreach(validateschemafield, sch.fields)
    return nothing
end

function _validatewriterbatches(sch::Schema, batches)
    validated = AC._ValidatedDictionaries()
    for batch in batches
        # A shared immutable pool must satisfy every value-field contract
        # through which the schema refers to it. Identity caching is safe only
        # after those field-specific checks have run.
        for (f, pool) in dictionarypools(sch.fields, batch.columns)
            validate_semantic(AC.dictvaluefield(f, f.type::DictionaryType), pool)
            validated[pool] = nothing
        end
        for (f, col) in zip(sch.fields, batch.columns)
            AC._validate_semantic(f, col, validated)
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
    isempty(batches) || codec == CODEC_NONE ||
        push!(features, Int64(2))  # Feature.COMPRESSED_BODY
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
    compress::Symbol=:none, dictids::IdDict{Field,Int64}=IdDict{Field,Int64}())
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkbatches(sch, batches)
    _validatewriterschema(sch)
    ids = assigndictids(sch.fields, dictids)
    fielddictids = IdDict{Field,Int64}(ids)
    _validatewriterbatches(sch, batches)
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
    writestream(s.schema, s.batches; compress=compress, dictids=s.fielddictids)

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
    compress::Symbol=:none, dictids::IdDict{Field,Int64}=IdDict{Field,Int64}())
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkbatches(sch, batches)
    _validatewriterschema(sch)
    ids = assigndictids(sch.fields, dictids)
    isempty(_streamfeatures(sch, batches, ids, CODEC_NONE)) ||
        throw(ValidationError("the IPC file format carries one dictionary batch per id; " *
            "changing pools require the stream format"))
    fielddictids = IdDict{Field,Int64}(ids)
    _validatewriterbatches(sch, batches)
    filefeatures = _streamfeatures(sch, batches, ids, codec)
    out = UInt8[]
    append!(out, FILE_MAGIC)
    append!(out, zeros(UInt8, 2))            # pad to 8 before the first message
    state = codec == CODEC_NONE ? nothing : EncodeState()
    dictblocks = NTuple{3,Int64}[]           # (offset, metalen, bodylen)
    recordblocks = NTuple{3,Int64}[]
    try
        _schemamessage!(out, sch, fielddictids, filefeatures)
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
        schoff = _metaschema!(b, sch, fielddictids, filefeatures)
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
    writefile(s.schema, s.batches; compress=compress, dictids=s.fielddictids)

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
function verify_footer(bytes::Vector{UInt8}, limits::Limits,
    reserve_limit::Int64=limits.max_total_allocated_bytes)
    state = _VState(limits, reserve_limit)
    length(bytes) >= 4 || _vfail("missing footer root offset")
    root = Int64(_vu32(bytes, 0))
    t = _vtable(bytes, root)
    _vvisit!(state, :footer, t)
    vp = _vfield(t, 0, 2)
    version = vp === nothing ? Int16(0) : reinterpret(Int16, _vu16(bytes, vp))
    version in (Int16(3), Int16(4)) ||
        _vfail("unsupported footer version $version (only V4/V5 are accepted)")
    sp = _vref(t, 1; required=true)
    features = _vschema(_vtable(bytes, sp), state, 0)
    version == Int16(3) && !isempty(features) &&
        _vfail("schema features require metadata V5")
    dictblocks = _vblockvector(t, 2, state)
    recordblocks = _vblockvector(t, 3, state)
    _vmetadata(t, 4, state, 0)
    return version, features, dictblocks, recordblocks, state.reserved
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
    a.name == b.name && a.nullable == b.nullable &&
        AC.typeequal(a.type, b.type) && a.metadata == b.metadata &&
        length(a.children) == length(b.children) || return false
    return all(_fieldequal(x, y) for (x, y) in zip(a.children, b.children))
end

function _schemaside(metaschema::Meta.Schema)
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()
    fields = Field[corefield(f, dictids, fielddictids)
        for f in something(metaschema.fields, Meta.Field[])]
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
    return all(_fieldwireequal(x, y)
        for (x, y) in zip(achildren, bchildren))
end

function _schemaequal(a::Meta.Schema, b::Meta.Schema)
    something(a.endianness, Meta.Endianness.Little) ==
        something(b.endianness, Meta.Endianness.Little) || return false
    ametafields = something(a.fields, Meta.Field[])
    bmetafields = something(b.fields, Meta.Field[])
    length(ametafields) == length(bmetafields) || return false
    all(_fieldwireequal(x, y)
        for (x, y) in zip(ametafields, bmetafields)) || return false
    afields = _schemaside(a)
    bfields = _schemaside(b)
    length(afields) == length(bfields) || return false
    _metadataequal(a.custom_metadata, b.custom_metadata) || return false
    all(_fieldequal(x, y) for (x, y) in zip(afields, bfields)) || return false
    return true
end

function _fileschema(region::OwnerRegion, footerstart::Int64, limits::Limits,
    budget::AllocationBudget)
    blob = BufferSlice(region, 0, region.len)
    AC.loadat(blob, UInt32, Int64(8)) == CONTINUATION ||
        throw(ValidationError("file data section does not start with an IPC message"))
    declared = Int64(AC.loadat(blob, Int32, Int64(12)))
    0 < declared <= limits.max_metadata_bytes ||
        throw(ValidationError("file schema metadata length is outside the limit"))
    declared % 8 == 0 ||
        throw(ValidationError("file schema metadata is not 8-byte aligned"))
    metalen = AC.checked_add(Int64(8), declared)
    fm = _blockmessage(region, (Int64(8), metalen, Int64(0)), footerstart,
        limits, budget)
    fm.header_type == UInt8(1) && fm.msg.header isa Meta.Schema ||
        throw(ValidationError("file data section does not start with a schema"))
    return fm, AC.checked_add(Int64(8), metalen)
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
    dataend::Int64
    limits::Limits
    schemaversion::Int16
end

Base.length(f::ArrowFile) = length(f.recordblocks)
AC.schema(f::ArrowFile) = f.schema

"""
Frame and verify the single message a Block points at, against the block's
own declared extents and the enclosing region.
"""
function _blockextent(block::NTuple{3,Int64}, dataend::Int64)
    offset, metalen, bodylen = block
    (offset >= 0 && metalen >= 16 && bodylen >= 0) ||
        throw(ValidationError("footer block has invalid extents"))
    offset % 8 == 0 || throw(ValidationError("footer block is not 8-byte aligned"))
    metalen % 8 == 0 ||
        throw(ValidationError("footer block metadata length is not 8-byte aligned"))
    bodylen % 8 == 0 ||
        throw(ValidationError("footer block body length is not 8-byte aligned"))
    frameend = AC.checked_add(AC.checked_add(offset, metalen), bodylen)
    frameend <= dataend ||
        throw(ValidationError("footer block escapes the data section"))
    return offset, frameend
end

function _validateblockindex(dictblocks, recordblocks, dataend::Int64;
    datastart::Int64=0)
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

function _blockrange(b::BufferSlice, pos::Int64, len::Int64,
    what::AbstractString)
    (pos >= 0 && len >= 0 && len <= b.len && pos <= b.len - len) ||
        throw(ValidationError("$what escapes block metadata"))
    return nothing
end

function _blockload(b::BufferSlice, ::Type{T}, pos::Int64,
    what::AbstractString) where {T}
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
    olen = Int64(_blockload(metadata, UInt16,
        _blockadd(vpos, Int64(2), what), what))
    vlen >= 4 && iseven(vlen) ||
        throw(ValidationError("invalid $what vtable length $vlen"))
    olen >= 4 || throw(ValidationError("invalid $what object length $olen"))
    _blockrange(metadata, vpos, vlen, what)
    _blockrange(metadata, pos, olen, what)
    return _BlockTable(metadata, pos, vpos, vlen, olen)
end

function _blockfield(t::_BlockTable, slot::Int, width::Int,
    what::AbstractString; required::Bool=false)
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
    width > 1 && pos % min(width, 8) != 0 &&
        throw(ValidationError("$what is misaligned"))
    _blockrange(t.metadata, pos, Int64(width), what)
    return pos
end

function _blockref(t::_BlockTable, slot::Int, what::AbstractString;
    required::Bool=false)
    pos = _blockfield(t, slot, 4, what; required=required)
    pos === nothing && return nothing
    rel = Int64(_blockload(t.metadata, UInt32, pos, what))
    rel > 0 || throw(ValidationError("$what has a null or backward offset"))
    target = _blockadd(pos, rel, what)
    _blockrange(t.metadata, target, Int64(1), what)
    return target
end

function _blockvector(t::_BlockTable, slot::Int, elemsize::Int,
    what::AbstractString)
    pos = _blockref(t, slot, what)
    pos === nothing && return nothing
    pos % 4 == 0 || throw(ValidationError("$what length is misaligned"))
    n = Int64(_blockload(t.metadata, UInt32, pos, what))
    start = _blockadd(pos, Int64(4), what)
    bytes = _blockmul(n, Int64(elemsize), what)
    _blockrange(t.metadata, start, bytes, what)
    n > 0 && elemsize > 1 && start % min(elemsize, 8) != 0 &&
        throw(ValidationError("$what data is misaligned"))
    return start, n
end

"""
Read the fixed Message/RecordBatch envelope and wire-buffer structs needed to
bind one Footer Block to its on-wire frame. The complete metadata graph remains
lazily verified by `_blockmessage`; this zero-allocation preflight prevents
optional-EOS classification from trusting forged Footer extents first.
"""
function _blockmessagebatch(metadata::BufferSlice)
    root = Int64(_blockload(metadata, UInt32, Int64(0), "message root"))
    root >= 4 || throw(ValidationError("invalid block message root offset"))
    msg = _blocktable(metadata, root, "block message table")
    bodypos = _blockfield(msg, 3, 8, "message body-length slot")
    bodylen = bodypos === nothing ? Int64(0) :
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
        throw(ValidationError(
            "footer block has unsupported message header type $headertype"))
    end
    return bodylen, headertype, batch
end

_blockmessagebodylength(metadata::BufferSlice) =
    first(_blockmessagebatch(metadata))

function _verifyblockbuffers(batch::_BlockTable, bodylen::Int64)
    buffers = _blockvector(batch, 2, 16, "record-batch buffer vector")
    buffers === nothing && return nothing
    start, n = buffers
    last_nonempty_end = Int64(0)
    for i = Int64(0):(n - 1)
        base = _blockadd(start, _blockmul(i, Int64(16),
            "record-batch buffer position"), "record-batch buffer position")
        offset = _blockload(batch.metadata, Int64, base,
            "record-batch buffer offset")
        len = _blockload(batch.metadata, Int64,
            _blockadd(base, Int64(8), "record-batch buffer length"),
            "record-batch buffer length")
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

function _verifyblockframe(blob::BufferSlice, block::NTuple{3,Int64},
    expectedheadertype::UInt8)
    offset, metalen, bodylen = block
    AC.loadat(blob, UInt32, offset) == CONTINUATION ||
        throw(ValidationError("footer block does not point at a message"))
    declared = Int64(AC.loadat(blob, Int32, offset + 4))
    declared == metalen - 8 ||
        throw(ValidationError(
            "footer block metadata length does not match the message"))
    metadata = AC.subslice(blob, offset + 8, declared)
    messagebodylen, headertype, batch = _blockmessagebatch(metadata)
    messagebodylen == bodylen ||
        throw(ValidationError(
            "footer block body length does not match the message"))
    headertype == expectedheadertype ||
        throw(ValidationError("footer block has the wrong message header type"))
    _verifyblockbuffers(batch, bodylen)
    return nothing
end

function _verifyblockframes(blob::BufferSlice, dictblocks, recordblocks,
    dataend::Int64; datastart::Int64=0)
    indexedend = _validateblockindex(dictblocks, recordblocks, dataend;
        datastart=datastart)
    foreach(block -> _verifyblockframe(blob, block, UInt8(2)), dictblocks)
    foreach(block -> _verifyblockframe(blob, block, UInt8(3)), recordblocks)
    return indexedend
end


function _blockmessage(region::OwnerRegion, block::NTuple{3,Int64},
    dataend::Int64, limits::Limits, budget::AllocationBudget)
    offset, _ = _blockextent(block, dataend)
    _, metalen, bodylen = block
    blob = BufferSlice(region, 0, region.len)
    AC.loadat(blob, UInt32, offset) == CONTINUATION ||
        throw(ValidationError("footer block does not point at a message"))
    declared = Int64(AC.loadat(blob, Int32, offset + 4))
    declared == metalen - 8 ||
        throw(ValidationError("footer block metadata length does not match the message"))
    0 < declared <= limits.max_metadata_bytes ||
        throw(ValidationError("metadata length $declared outside (0, $(limits.max_metadata_bytes)]"))
    0 <= bodylen <= limits.max_body_bytes ||
        throw(ValidationError("body length $bodylen outside [0, $(limits.max_body_bytes)]"))
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
    _requirelittleendian()
    _validatelimits(limits)
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
    metaschema === nothing ||
        (something(metaschema.endianness, Meta.Endianness.Little) == Meta.Endianness.Little ||
        throw(ValidationError("big-endian IPC requires normalization, which is outside this prove-out")))
    metaschema === nothing &&
        throw(ValidationError("file footer carries no schema"))
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
    indexedend = _verifyblockframes(blob, dictblocks, recordblocks, footerstart;
        datastart=schemaend)
    haseos = footerstart - indexedend >= 8 &&
        AC.loadat(blob, UInt32, footerstart - 8) == CONTINUATION &&
        AC.loadat(blob, UInt32, footerstart - 4) == UInt32(0)
    dataend = haseos ? footerstart - 8 : footerstart
    _validateblockindex(dictblocks, recordblocks, dataend;
        datastart=schemaend)
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
            fm = _blockmessage(region, block, dataend, limits, budget)
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
            vf = dictvaluefields[header.id]
            rblen = something(rb.length, Int64(0))
            0 <= rblen <= limits.max_array_length ||
                throw(ValidationError("dictionary batch length $rblen exceeds limit"))
            cursor = DecodeCursor(rb.nodes, rb.buffers, fm.body, limits;
                codec=codec, state=state, variadics=variadiccounts(rb))
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
            fielddictids, dicts, validated, recordblocks, dataend, limits,
            version)
    finally
        close(state)
    end
end

function Base.getindex(f::ArrowFile, i::Integer)
    1 <= i <= length(f.recordblocks) || throw(BoundsError(f, i))
    budget = AllocationBudget(f.limits.max_total_allocated_bytes)
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, budget)
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

"""
Hand-build a one-column batch from raw buffer bytes (the write-side mirror of
the read fixtures): interval layouts have no 2.x writer to lean on.
"""
function _handbatch(t::ArrowType, n::Int, buffers::Vector{Vector{UInt8}};
    nullcount::Int=0)
    f = Field("x", t, true, nothing, Field[])
    slices = BufferSlice[isempty(bytes) ? BufferSlice() :
        BufferSlice(heapregion(bytes), 0, length(bytes)) for bytes in buffers]
    d = ArrayData(t, n, slices; nullcount=nullcount)
    sch = Schema(Field[f])
    return sch, AC.RecordBatch(sch, ArrayData[d], n)
end

_le(xs...) = reduce(vcat, [collect(reinterpret(UInt8, [x])) for x in xs])

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
    @assert isempty(framemessages(heapregion(copy(writestream(emptysch,
        AC.RecordBatch[]; compress=:zstd))))[1].features)
    zerorow = readstream(writestream(readstream(
        let z = IOBuffer(); Arrow.write(z, (x=Int64[],); file=false); take!(z) end)))
    @assert zerorow.batches[1].nrows == 0
    println("schema-only streams do not overdeclare compression; zero rows round-trip ✓")

    # Core may omit the physical offsets buffer for a canonical empty array.
    # IPC still carries length + 1 offsets, so the adapter materializes one
    # zero without changing Core's allocation-free representation.
    emptyutf8 = Utf8Type(false)
    emptyfield = Field("empty", emptyutf8)
    emptydata = ArrayData(emptyutf8, 0,
        [BufferSlice(), BufferSlice(), BufferSlice()])
    emptybatch = AC.RecordBatch(Schema([emptyfield]), [emptydata], 0)
    emptybytes = writestream(emptybatch.schema, [emptybatch])
    emptyframes = framemessages(heapregion(copy(emptybytes)))
    emptybuffers = something((emptyframes[2].msg.header::Meta.RecordBatch).buffers,
        Meta.Buffer[])
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
    @assert isempty(materialize(omittedstream.schema.fields[1],
        omittedstream.batches[1].columns[1]))
    println("empty IPC offset arrays: written with one terminal zero, read with none ✓")

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
    @assert _rejects(() -> _requirelittleendian(UInt32(0x01020304)))
    println("offset views, schema mismatches, and unknown codecs are refused ✓")

    # Schema-only output still validates the full Schema/Field envelope.
    invalidname = String(UInt8[0xff])
    badnameschema = Schema(Field[Field(invalidname, IntType(64, true))])
    badmetaschema = Schema(emptysch.fields; metadata=[invalidname => "value"])
    bigschema = Schema(emptysch.fields; endianness=AC.BigEndian)
    badreeschema = Schema(Field[Field("ree", RunEndEncodedType(); children=[
        Field("wrong", IntType(32, true); nullable=false),
        Field("also-wrong", IntType(64, true))])])
    @assert _rejects(() -> writestream(badnameschema, AC.RecordBatch[]))
    @assert _rejects(() -> writefile(badnameschema, AC.RecordBatch[]))
    @assert _rejects(() -> writefile(badmetaschema, AC.RecordBatch[]))
    @assert _rejects(() -> writestream(bigschema, AC.RecordBatch[]))
    @assert _rejects(() -> writestream(badreeschema, AC.RecordBatch[]))
    @assert _rejects(() -> writefile(badreeschema, AC.RecordBatch[]))
    println("schema-only writers validate names, metadata, endianness, and REE children ✓")

    # A Field object is one writer-side dictionary-id key. Reusing that exact
    # object at two positions used to collapse two distinct pools onto one id.
    aliasfield, aliasdata1 = AC.fromjulia_dict("d", ["a", "b"], [0, 1])
    _, aliasdata2 = AC.fromjulia_dict("d", ["x", "y"], [0, 1])
    aliasschema = Schema(Field[aliasfield, aliasfield])
    aliasbatch = AC.RecordBatch(aliasschema,
        ArrayData[aliasdata1, aliasdata2], 2)
    @assert _rejects(() -> writestream(aliasschema, [aliasbatch]))
    sharedvaluechild = Field("value", IntType(64, true))
    aliaseddict = Field("dict",
        DictionaryType(IntType(32, true), StructType(), false);
        children=[sharedvaluechild])
    aliasedlist = Field("list", ListType(false);
        children=[sharedvaluechild])
    @assert _rejects(() -> assigndictids([aliaseddict, aliasedlist]))

    # One pool shared through two dictionary fields must satisfy both value
    # schemas. The batch's own schema permits the null; the requested writer
    # schema deliberately makes the second value child non-nullable.
    poolfield, pooldata = AC.fromjulia_struct("pool",
        (a=Union{Missing,Int64}[missing],))
    dtype = DictionaryType(IntType(32, true), poolfield.type, false)
    _, indexdata = fromjulia("index", Int32[0])
    dictdata = ArrayData(dtype, 1, indexdata.buffers;
        dictionary=pooldata, nullcount=0)
    nullablechild = poolfield.children[1]
    strictchild = Field(nullablechild.name, nullablechild.type;
        nullable=false)
    batchfields = Field[
        Field("left", dtype; children=[nullablechild]),
        Field("right", dtype; children=[nullablechild]),
    ]
    strictfields = Field[
        batchfields[1],
        Field("right", dtype; children=[strictchild]),
    ]
    sharedbatch = AC.RecordBatch(Schema(batchfields),
        ArrayData[dictdata, dictdata], 1)
    # Field.nullable is advisory at the semantic tier (the gold corpus itself
    # violates it), so the skewed write is accepted; the strict declaration
    # is enforced by the opt-in validate_full tier.
    @assert readstream(writestream(Schema(strictfields), [sharedbatch])) isa IPCStream
    @assert _rejects(() -> AC.validate_full(strictfields[2], dictdata))
    @assert AC.validate_full(batchfields[2], dictdata) === dictdata
    println("dictionary field aliases are refused; contract skew is validate_full's ✓")

    # Unions, both modes: 2.x writes them, Core reads and re-encodes them,
    # and 2.x reads this writer's bytes back. The mapped set now matches
    # Core's accessor coverage; the self-round-trips below cover the newer
    # view layouts and REE that Arrow.jl 2.x cannot yet emit.
    sparsebytes = UInt8[]
    for (modename, dense) in (("dense", true), ("sparse", false))
        uio = IOBuffer()
        Arrow.write(uio, (u=Union{Int64,String}[1, "x", 2, "y"],);
            file=false, denseunions=dense)
        usource = readstream(take!(uio))
        ut = usource.schema.fields[1].type
        @assert ut isa UnionType
        @assert (ut.mode == AC.DenseMode) == dense
        ubytes = writestream(usource)
        dense || (sparsebytes = copy(ubytes))
        _assert_stream_equal(usource, readstream(ubytes))
        _assert_2x_reads(ubytes, usource)
        println("$(modename) unions round-trip (Core + 2.x) ✓")
    end

    # IPC sparse-union children have exactly the parent length. Core allows a
    # longer backing child for sliced C Data, so this rule stays at the IPC
    # boundary. Omitted union ids also fail cleanly before Int8 conversion.
    onechild, longchild = fromjulia("i", Int64[10, 20])
    sparse = UnionType(AC.SparseMode, Int8[0])
    sparsefield = Field("u", sparse; children=[onechild])
    sparsedata = ArrayData(sparse, 1, [AC._databuffer(Int8[0])];
        children=[longchild])
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
    customfield = Field("u", customtype;
        children=[customleft, customright])
    customdata = ArrayData(customtype, 2, [AC._databuffer(Int8[7, 3])];
        children=[customleftdata, customrightdata])
    custombatch = AC.RecordBatch(Schema([customfield]), [customdata], 2)
    customstream = readstream(writestream(custombatch.schema, [custombatch]))
    @assert materialize(customstream.schema.fields[1],
        customstream.batches[1].columns[1]) == Any[10, "y"]
    println("IPC sparse-union length and union-id domains are enforced ✓")

    # Intervals, all three units, hand-built (2.x has no interval writer).
    # MONTH_DAY_NANO exceeds 2.x entirely: its vendored enum predates the
    # unit, so 2.x must fail while this adapter round-trips it.
    ym = _handbatch(IntervalType(AC.YEAR_MONTH), 3,
        [UInt8[0x05], _le(Int32(12), Int32(0), Int32(7))]; nullcount=1)
    dt = _handbatch(IntervalType(AC.DAY_TIME), 3,
        [UInt8[], _le(Int32(1), Int32(2), Int32(3), Int32(4), Int32(5), Int32(6))])
    mdn = _handbatch(IntervalType(AC.MONTH_DAY_NANO), 2,
        [UInt8[], _le(Int32(1), Int32(2), Int64(3), Int32(4), Int32(5), Int64(6))])
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
    mdnbytes = writestream(mdn[1], [mdn[2]])
    mdnfailed = try
        Arrow.Table(IOBuffer(mdnbytes))
        false
    catch
        true
    end
    @assert mdnfailed
    println("intervals round-trip, including MONTH_DAY_NANO beyond 2.x ✓")

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
    zfilebytes = writefile(source; compress=:zstd)
    zfile = readfile(zfilebytes)
    @assert length(Tables.getcolumn(Tables.columns(
        Arrow.Table(IOBuffer(copy(zfilebytes)))), 1)) == 10
    for (j, f) in enumerate(zfile.schema.fields)
        @assert isequal(collect(Any, materialize(f, zfile[1].columns[j])),
            collect(Any, materialize(f, source.batches[1].columns[j])))
    end
    zfooterlen = Int64(reinterpret(Int32, zfilebytes[(end - 9):(end - 6)])[1])
    zfooterstart = Int64(length(zfilebytes)) - 10 - zfooterlen
    zfooterbytes = copy(zfilebytes[(zfooterstart + 1):(zfooterstart + zfooterlen)])
    _, zfooterfeatures, _, _, _ = verify_footer(zfooterbytes, Limits())
    zstreamsection = copy(zfilebytes[9:zfooterstart])
    zschemafeatures = framemessages(heapregion(zstreamsection))[1].features
    @assert zschemafeatures == Int64[2] == zfooterfeatures
    emptyfilebytes = writefile(emptysch, AC.RecordBatch[]; compress=:zstd)
    emptyfooterlen = Int64(reinterpret(Int32,
        emptyfilebytes[(end - 9):(end - 6)])[1])
    emptyfooterstart = Int64(length(emptyfilebytes)) - 10 - emptyfooterlen
    emptyfooterbytes = copy(emptyfilebytes[
        (emptyfooterstart + 1):(emptyfooterstart + emptyfooterlen)])
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

    # The leading Schema message is part of the file contract, not dead
    # padding. It must agree semantically with Footer.schema.
    differentschema = copy(filebytes)
    embeddedlen = Int64(reinterpret(Int32, differentschema[13:16])[1])
    embedded = copy(differentschema[17:(16 + embeddedlen)])
    embeddedmsg = _vtable(embedded, Int64(_vu32(embedded, 0)))
    embeddedschema = _vtable(embedded,
        _vref(embeddedmsg, 2; required=true))
    fieldvec, nembeddedfields = _vvector(embeddedschema, 1, 4; required=true)
    @assert nembeddedfields > 0
    embeddedfield = _vtable(embedded,
        AC.checked_add(fieldvec, Int64(_vu32(embedded, fieldvec))))
    namepos = _vref(embeddedfield, 0; required=true)
    differentschema[16 + namepos + 4 + 1] = UInt8('z')
    @assert _rejects(() -> readfile(differentschema))

    # Files cannot opt into stream dictionary replacement, even when their
    # block index happens to contain no duplicate dictionary id.
    replacementfeature = copy(zfilebytes)
    embeddedlen = Int64(reinterpret(Int32, replacementfeature[13:16])[1])
    embedded = copy(replacementfeature[17:(16 + embeddedlen)])
    embeddedmsg = _vtable(embedded, Int64(_vu32(embedded, 0)))
    embeddedschema = _vtable(embedded,
        _vref(embeddedmsg, 2; required=true))
    embeddedfeatures, nembeddedfeatures =
        _vvector(embeddedschema, 3, 8; required=true)
    @assert nembeddedfeatures == 1
    _write_i64!(replacementfeature, Int64(16) + embeddedfeatures, Int64(1))
    replacementfooterlen = Int64(reinterpret(Int32,
        replacementfeature[(end - 9):(end - 6)])[1])
    replacementfooterstart = Int64(length(replacementfeature)) - 10 -
        replacementfooterlen
    replacementfooter = copy(replacementfeature[
        (replacementfooterstart + 1):(replacementfooterstart + replacementfooterlen)])
    replacementtable = _vtable(replacementfooter,
        Int64(_vu32(replacementfooter, 0)))
    replacementschema = _vtable(replacementfooter,
        _vref(replacementtable, 1; required=true))
    replacementfeatures, nreplacementfeatures =
        _vvector(replacementschema, 3, 8; required=true)
    @assert nreplacementfeatures == 1
    _write_i64!(replacementfeature,
        replacementfooterstart + replacementfeatures, Int64(1))
    @assert _rejects(() -> readfile(replacementfeature))
    @assert _rejects(() -> _validateblockindex(
        NTuple{3,Int64}[(Int64(304), Int64(16), Int64(0))],
        NTuple{3,Int64}[], Int64(312)))
    @assert _rejects(() -> _validateblockindex(
        NTuple{3,Int64}[(Int64(8), Int64(16), Int64(8))],
        NTuple{3,Int64}[(Int64(24), Int64(16), Int64(0))], Int64(64)))

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
    simplefooterlen = Int64(reinterpret(Int32,
        simplebytes[(end - 9):(end - 6)])[1])
    simplefooterstart = Int64(length(simplebytes)) - 10 - simplefooterlen
    simplefooter = copy(simplebytes[
        (simplefooterstart + 1):(simplefooterstart + simplefooterlen)])

    # Keep the message and Block internally consistent while extending the
    # indexed body into the footer. Open must reject the cross-boundary span.
    crossing = copy(simplebytes)
    crossingtable = _vtable(simplefooter, Int64(_vu32(simplefooter, 0)))
    crossingstart, crossingcount = _vvector(crossingtable, 3, 24)
    @assert crossingcount == 1
    crossingoffset = _vi64(simplefooter, crossingstart)
    crossingmeta = Int64(_vi32(simplefooter, crossingstart + 8))
    crossingbody = _vi64(simplefooter, crossingstart + 16)
    crossingmessage = copy(crossing[
        (crossingoffset + 9):(crossingoffset + crossingmeta)])
    crossingroot = _vtable(crossingmessage,
        Int64(_vu32(crossingmessage, 0)))
    bodypos = _vfield(crossingroot, 3, 8; required=true)
    newbodylen = crossingbody + 16
    _write_i64!(crossing, crossingoffset + 8 + bodypos, newbodylen)
    _write_i64!(crossing,
        simplefooterstart + crossingstart + 16, newbodylen)
    @assert _rejects(() -> readfile(crossing))

    # A no-EOS file may end its last data buffer with the eight-byte EOS byte
    # pattern. Indexed block extents, not that ambiguous pattern alone, decide
    # whether those bytes are data. Arrow.jl 2.x writes and accepts no-EOS
    # files, so retain that interoperable form.
    collisionfield, collisiondata =
        fromjulia("collision", Int64[Int64(0x00000000ffffffff)])
    collisionbatch = AC.RecordBatch(Schema([collisionfield]),
        [collisiondata], 1)
    collision = writefile(collisionbatch.schema, [collisionbatch])
    collisionfooterlen = Int64(reinterpret(Int32,
        collision[(end - 9):(end - 6)])[1])
    collisionfooterstart = Int64(length(collision)) - 10 - collisionfooterlen
    noeos = copy(collision)
    deleteat!(noeos,
        Int(collisionfooterstart - 7):Int(collisionfooterstart))
    noeosfile = readfile(noeos)
    @assert materialize(noeosfile.schema.fields[1],
        noeosfile[1].columns[1]) == Int64[Int64(0x00000000ffffffff)]
    @assert length(Tables.getcolumn(Tables.columns(
        Arrow.Table(IOBuffer(copy(noeos)))), 1)) == 1

    # Footer extents are not verified until they agree with the on-wire
    # Message envelope. Merely shortening the final Block must not make its
    # marker-shaped data look like an optional EOS marker at file-open time.
    forgedcollision = copy(noeos)
    forgedfooterlen = Int64(reinterpret(Int32,
        forgedcollision[(end - 9):(end - 6)])[1])
    forgedfooterstart = Int64(length(forgedcollision)) - 10 - forgedfooterlen
    forgedfooter = copy(forgedcollision[
        (forgedfooterstart + 1):(forgedfooterstart + forgedfooterlen)])
    forgedtable = _vtable(forgedfooter, Int64(_vu32(forgedfooter, 0)))
    forgedblocks, nforgedblocks = _vvector(forgedtable, 3, 24; required=true)
    @assert nforgedblocks == 1
    forgedbodylen = _vi64(forgedfooter, forgedblocks + 16)
    @assert forgedbodylen >= 8
    _write_i64!(forgedcollision,
        forgedfooterstart + forgedblocks + 16, forgedbodylen - 8)
    @assert _rejects(() -> readfile(forgedcollision))

    # Coordinating the same lie in Message.bodyLength is still insufficient:
    # the RecordBatch buffer table proves that the excluded bytes are data.
    coordinated = copy(forgedcollision)
    forgedoffset = _vi64(forgedfooter, forgedblocks)
    forgedmetalen = Int64(_vi32(forgedfooter, forgedblocks + 8))
    forgedmessage = copy(coordinated[
        (forgedoffset + 9):(forgedoffset + forgedmetalen)])
    forgedmessagetable = _vtable(forgedmessage,
        Int64(_vu32(forgedmessage, 0)))
    forgedmessagebody = _vfield(forgedmessagetable, 3, 8; required=true)
    _write_i64!(coordinated,
        forgedoffset + 8 + forgedmessagebody, forgedbodylen - 8)
    @assert _rejects(() -> readfile(coordinated))

    _, _, _, _, footreserve = verify_footer(simplefooter, Limits())
    tightbudget = max(simplefooterlen, footreserve)
    @assert _rejects(() -> readfile(copy(simplebytes);
        limits=Limits(max_total_allocated_bytes=tightbudget)))
    @assert _rejects(() -> readfile(copy(simplebytes);
        limits=Limits(max_messages=1)))
    bodylimited = readfile(copy(simplebytes); limits=Limits(max_body_bytes=0))
    @assert _rejects(() -> bodylimited[1])
    # A block offset pointing outside the file must fail cleanly.
    file2 = readfile(copy(filebytes))
    badblocks = [(Int64(2)^40, Int64(16), Int64(0))]
    badfile = ArrowFile(file2.region, file2.schema, file2.fields,
        file2.fielddictids, file2.dictionaries, file2.validated, badblocks,
        file2.dataend, file2.limits, file2.schemaversion)
    @assert _rejects(() -> badfile[1])
    println("file magic, footer, and block extents are verified ✓")

    # ---- Format 1.3/1.4 layouts: views and run-end encoding ------------
    # 2.x cannot write these (and misreads ListView per the report), so the
    # acceptance is self round-trip on both formats plus wire-shape checks:
    # the variadicBufferCounts vector, the late type tags, and the buffer
    # accounting that skewed nothing after them.
    viewentry(len, rest) = vcat(reinterpret(UInt8, Int32[Int32(len)]), rest,
        zeros(UInt8, 12 - length(rest)))
    viewlong(len, prefix, bufidx, off) =
        vcat(reinterpret(UInt8, Int32[Int32(len)]), prefix,
             reinterpret(UInt8, Int32[Int32(bufidx), Int32(off)]))
    payload1 = collect(codeunits("first-out-of-line-payload"))
    payload2 = collect(codeunits("second-buffer-payload-here"))
    views = vcat(
        viewentry(3, collect(codeunits("abc"))),
        viewlong(25, payload1[1:4], 0, 0),
        viewlong(26, payload2[1:4], 1, 0),
        viewentry(0, UInt8[]))
    vt = ViewType(true)
    vf = Field("v", vt; nullable=true)
    vd = ArrayData(vt, 4,
        [AC._databuffer(UInt8[0x0b]), AC._databuffer(views),
         AC._databuffer(payload1), AC._databuffer(payload2)]; nullcount=1)
    lvt = ListViewType(false)
    lvcf, lvcd = fromjulia("item", Int64[10, 20, 30])
    lvf = Field("lv", lvt; children=[lvcf])
    lvd = ArrayData(lvt, 3,
        [BufferSlice(), AC._databuffer(Int32[2, 0, 0]),
         AC._databuffer(Int32[1, 2, 3])]; children=[lvcd], nullcount=0)
    rt = RunEndEncodedType()
    ref, red = fromjulia("run_ends", Int32[2, 3, 4])
    rvf, rvd = fromjulia("values", Union{Missing,String}["x", missing, "z"])
    rf = Field("ree", rt; children=[ref, rvf])
    rd = ArrayData(rt, 4, BufferSlice[]; children=[red, rvd], nullcount=0)
    nvv = ArrayData(vt, 2,
        [BufferSlice(), AC._databuffer(vcat(
            viewentry(1, collect(codeunits("p"))),
            viewentry(1, collect(codeunits("q")))))]; nullcount=0)
    nvf = Field("values", vt; nullable=false)
    nirf, nird = fromjulia("run_ends", Int32[1, 2])
    nif = Field("values", rt; children=[nirf, nvf])
    nid = ArrayData(rt, 2, BufferSlice[]; children=[nird, nvv], nullcount=0)
    norf, nord = fromjulia("run_ends", Int32[2, 4])
    nf = Field("nested", rt; children=[norf, nif])
    nd = ArrayData(rt, 4, BufferSlice[]; children=[nord, nid], nullcount=0)
    # 64-bit-offset utf8/binary: the only IPC path exercising the LargeUtf8/
    # LargeBinary metadata tables (the vendored typo `largUtf8Start` hid
    # here undetected until regeneration).
    luf = Field("lu", Utf8Type(true); nullable=false)
    lud = ArrayData(Utf8Type(true), 4,
        [BufferSlice(), AC._databuffer(Int64[0, 1, 1, 3, 6]),
         AC._databuffer(collect(codeunits("abcdef")))]; nullcount=0)
    # a plain column AFTER the exotic ones proves no buffer skew
    tf, td = fromjulia("tail", Int64[1, 2, 3, 4])
    exsch = Schema(Field[vf, lvf, rf, nf, luf, tf])
    exlv = ArrayData(lvt, 4,
        [BufferSlice(), AC._databuffer(Int32[2, 0, 0, 1]),
         AC._databuffer(Int32[1, 2, 3, 0])]; children=[lvcd], nullcount=0)
    exbatch = AC.RecordBatch(exsch, ArrayData[vd, exlv, rd, nd, lud, td], 4)
    exwant = Dict(
        "v" => Any["abc", "first-out-of-line-payload", missing, ""],
        "lv" => Any[[30], [10, 20], [10, 20, 30], Int64[]],
        "ree" => Any["x", "x", missing, "z"],
        "nested" => Any["p", "p", "q", "q"],
        "lu" => Any["a", "", "bc", "def"],
        "tail" => Any[1, 2, 3, 4])
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

    # Wire shape: variadic counts follow field preorder (2 buffers for the
    # top-level view, then 0 for the inline view below nested REE); the type
    # tags are the 1.3/1.4 ids.
    exframes = framemessages(heapregion(copy(writestream(exsch, [exbatch]))))
    exrb = exframes[2].msg.header::Meta.RecordBatch
    @assert variadiccounts(exrb) == Int64[2, 0]
    exmeta = exframes[1].msg.header::Meta.Schema
    @assert [typeof(f.type) for f in exmeta.fields] ==
        [Meta.Utf8View, Meta.ListView, Meta.RunEndEncoded,
         Meta.RunEndEncoded, Meta.LargeUtf8, Meta.Int]
    println("variadic counts and 1.3/1.4 type tags are on the wire ✓")

    # A view column with ZERO variadic buffers (all inline) is legal and
    # round-trips with an explicit 0 count.
    inl = ArrayData(vt, 2,
        [BufferSlice(), AC._databuffer(vcat(viewentry(2, collect(codeunits("hi"))),
                                            viewentry(1, collect(codeunits("!")))))];
        nullcount=0)
    inlsch = Schema(Field[Field("v", vt)])
    inlstream = readstream(writestream(inlsch, [AC.RecordBatch(inlsch, ArrayData[inl], 2)]))
    @assert materialize(inlstream.schema.fields[1], inlstream.batches[1].columns[1]) ==
        ["hi", "!"]
    println("all-inline views carry an explicit zero variadic count ✓")

    # Corrupt variadic counts fail closed: overstated (consumes into the
    # tail column's buffers → skew caught) and understated (leftover buffers).
    exraw = writestream(exsch, [exbatch])
    for lie in (Int64(3), Int64(1))
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

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    main()
end
