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
    zstd::Union{Nothing,ZstdCompressor}
    const level::Union{Nothing,Int}   # codec compression level; nothing = codec default
    # Compressed frames this state wrote for messages not yet published, and
    # the published ones' vectors, reused for later frames: fresh memory
    # faults its pages in, which several threads cannot do at once.
    const inuse::Vector{Vector{UInt8}}
    const free::Vector{Vector{UInt8}}
end
EncodeState(level::Union{Nothing,Integer}=nothing) = EncodeState(
    nothing,
    level === nothing ? nothing : Int(level),
    Vector{UInt8}[],
    Vector{UInt8}[],
)

"""
Vectors the writer's columns rebuild for each record batch (string offsets and
bytes, validity bitmaps), taken here while the batch builds and returned once
it is published, so a write holds about one batch of them instead of every
batch until the GC runs; reused memory also skips the page faults that fresh
memory takes, which several threads cannot take at once.
"""
struct _BufferPool
    lock::ReentrantLock
    bytes::Vector{Vector{UInt8}}
    i32::Vector{Vector{Int32}}
    i64::Vector{Vector{Int64}}
    # Handed out since the last `_handoff!`: the batch being built.
    taken::Vector{Any}
end
_BufferPool() =
    _BufferPool(ReentrantLock(), Vector{UInt8}[], Vector{Int32}[], Vector{Int64}[], Any[])

_freelist(p::_BufferPool, ::Type{UInt8}) = p.bytes
_freelist(p::_BufferPool, ::Type{Int32}) = p.i32
_freelist(p::_BufferPool, ::Type{Int64}) = p.i64

"""
A `Vector{T}` of length `n`, from `pool` (or new without one): the shortest
free vector at least that long, else the longest one, grown.
"""
function _takevector(pool, ::Type{T}, n::Int) where {T}
    pool === nothing && return Vector{T}(undef, n)
    p = pool::_BufferPool
    @lock p.lock begin
        free = _freelist(p, T)
        k = 0
        for (i, v) in enumerate(free)
            m, best = length(v), k == 0 ? 0 : length(free[k])
            if k == 0 || (m >= n ? (best < n || m < best) : (best < n && m > best))
                k = i
            end
        end
        v = k == 0 ? Vector{T}(undef, n) : resize!(popat!(free, k), n)
        push!(p.taken, v)
        v
    end
end

"The vectors taken for the batch just built, to return once it is published."
function _handoff!(pool)
    pool === nothing && return nothing
    p = pool::_BufferPool
    @lock p.lock begin
        out = copy(p.taken)
        empty!(p.taken)
        (p, out)
    end
end

"Return a published batch's vectors, except those a kept dictionary still uses."
function _recycle!(st, recycle)
    recycle === nothing && return nothing
    p, vectors = recycle
    kept = Base.IdSet{Any}()
    for d in values(st.current)
        _bufferroots!(kept, d)
    end
    @lock p.lock for v in vectors
        v in kept || push!(_freelist(p, eltype(v)), v)
    end
    return nothing
end

function _bufferroots!(roots, d::ArrayData)
    for b in d.buffers
        b.region === nothing || push!(roots, b.region.root)
    end
    foreach(c -> _bufferroots!(roots, c), d.children)
    d.dictionary === nothing || _bufferroots!(roots, d.dictionary)
    return roots
end

"Return the frames of published messages to their states for reuse."
function _recycleframes!(states::Vector{EncodeState})
    for s in states
        append!(s.free, s.inuse)
        empty!(s.inuse)
    end
    return nothing
end

# One LZ4 frame of `raw` from a fresh context (about half a microsecond). A
# reused context keeps its previous input's hash table (LZ4's fast reset),
# which changes the output, so which task compressed which buffer would show
# in the bytes.
function _lz4frame(s::EncodeState, raw::TS.Buffer, out::Vector{UInt8})
    c =
        s.level === nothing ? LZ4FrameCompressor() :
        LZ4FrameCompressor(; compressionlevel=s.level)
    TS.initialize(c)
    try
        return transcode(c, raw, out)
    finally
        TS.finalize(c)
    end
end

function _zstdc!(s::EncodeState)
    if s.zstd === nothing
        c = s.level === nothing ? ZstdCompressor() : ZstdCompressor(; level=s.level)
        TS.initialize(c)
        s.zstd = c
    end
    return s.zstd::ZstdCompressor
end

function Base.close(s::EncodeState)
    zstd = s.zstd
    s.zstd = nothing
    zstd === nothing || TS.finalize(zstd)
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
The framing prefix of a message: continuation marker, padded metadata length,
then the padded metadata. Its body (`bodylen` bytes, already padded) follows.
"""
function _messageheader(b::FB.Builder, msg, bodylen::Int64)
    FB.finish!(b, msg)
    meta = collect(FB.finishedbytes(b))
    _pad8!(meta)
    bodylen % 8 == 0 || throw(ArgumentError("message body must be padded"))
    header = UInt8[]
    sizehint!(header, 8 + length(meta))
    append!(header, reinterpret(UInt8, UInt32[CONTINUATION, UInt32(length(meta))]))
    append!(header, meta)
    return header
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
    return append!(out, _messageheader(b, Meta.messageEnd(b), Int64(0)))
end

# ---------------------------------------------------------------------------
# THE generic encoder: registry-driven node/buffer emission
# ---------------------------------------------------------------------------

# A message body is the list of its pieces, each followed by zero padding to
# a multiple of 8 bytes. Uncompressed pieces are the column buffers
# themselves, so `_writebody` streams them to the sink without a body copy.
mutable struct EncodeCursor
    nodes::Vector{NTuple{2,Int64}}       # (length, null_count), forward order
    buffers::Vector{NTuple{2,Int64}}     # (offset, length), forward order
    body::Vector{AC.BufferSlice}         # body pieces, in order
    bodylen::Int64                       # padded body length
    codec::Int8
    # A compressed body's buffers in order, compressed together by
    # `_compressbody!` once the walk is done.
    raw::Vector{AC.BufferSlice}
    variadics::Vector{Int64}             # per view field, depth-first order
end
EncodeCursor(codec::Int8) = EncodeCursor(
    NTuple{2,Int64}[],
    NTuple{2,Int64}[],
    AC.BufferSlice[],
    Int64(0),
    codec,
    AC.BufferSlice[],
    Int64[],
)

# One frame of the `len` bytes at `ptr`: LZ4 from a fresh context
# (`_lz4frame`), Zstandard with `state`'s.
function _compressbytes(state::EncodeState, codec::Int8, ptr::Ptr{UInt8}, len::Int)
    raw = TS.Buffer(unsafe_wrap(Array, ptr, len))
    out = isempty(state.free) ? UInt8[] : pop!(state.free)
    # Room for the codec's worst case (LZ4 and Zstandard frames stay under
    # this), so a reused vector is never too small for one compression call.
    need = len + (len >> 7) + 1024
    length(out) < need && resize!(out, need)
    frame =
        codec == CODEC_LZ4_FRAME ? _lz4frame(state, raw, out) :
        transcode(_zstdc!(state), raw, out)
    push!(state.inuse, frame)
    return frame
end

function _addpiece!(c::EncodeCursor, b::AC.BufferSlice)
    push!(c.body, b)
    c.bodylen = AC.checked_add(c.bodylen, (b.len + 7) & ~Int64(7))
    return nothing
end

"""
Append one buffer to the message body: raw bytes for uncompressed batches;
for compressed batches, the spec's Int64 uncompressed-length prefix plus the
frame, falling back to `-1` + raw whenever compression does not shrink the
payload (see `_compressbody!`). Zero-length buffers write no body bytes in
either mode. The recorded Buffer length is the wire length; 8-byte alignment
padding sits between buffers and belongs to neither.
"""
encodebuffer!(c::EncodeCursor, bytes::Vector{UInt8}) =
    encodebuffer!(c, AC.BufferSlice(AC.heapregion(bytes), 0, length(bytes)))

function encodebuffer!(c::EncodeCursor, b::AC.BufferSlice)
    b.len == 0 || b.region !== nothing || throw(ArgumentError("empty buffer has no data"))
    c.codec == CODEC_NONE || (push!(c.raw, b); return nothing)
    offset = c.bodylen
    push!(c.buffers, (offset, Int64(b.len)))
    b.len == 0 || _addpiece!(c, b)
    return nothing
end

# Bodies under this many raw bytes compress on one task.
const _PARALLEL_BODY_BYTES = 1 << 20

"""
Compress a compressed message's buffers (`c.raw`), on up to `ntasks` tasks
with one codec context each, and lay out the body in buffer order. Each
buffer is its own frame, so the bytes do not depend on the task count.
"""
function _compressbody!(c::EncodeCursor, states::Vector{EncodeState}, ntasks::Int)
    c.codec == CODEC_NONE && return nothing
    raw = c.raw
    total = sum(b -> Int64(b.len), raw; init=Int64(0))
    k = total < _PARALLEL_BODY_BYTES ? 1 : ntasks
    # Largest buffers first, so the last task to finish has a small one.
    order = k == 1 ? (1:length(raw)) : sortperm(raw; by=b -> b.len, rev=true)
    done = _tasksmap(length(raw), k) do i, w
        b = raw[order[i]]
        b.len == 0 && return nothing
        frame = GC.@preserve b _compressbytes(
            states[w],
            c.codec,
            AC.sliceptr(b),
            Int(b.len),
        )
        return length(frame) < b.len ? frame : nothing
    end
    frames = Vector{Union{Nothing,Vector{UInt8}}}(undef, length(raw))
    for (i, j) in enumerate(order)
        frames[j] = done[i]
    end
    for (j, b) in enumerate(raw)
        offset = c.bodylen
        if b.len == 0
            push!(c.buffers, (offset, Int64(0)))
            continue
        end
        frame = frames[j]
        prefix = Int64[frame === nothing ? -1 : b.len]
        payload =
            frame === nothing ? b : AC.BufferSlice(AC.heapregion(frame), 0, length(frame))
        _addpiece!(c, AC.BufferSlice(AC.heapregion(prefix), 0, 8))
        _addpiece!(c, payload)
        push!(c.buffers, (offset, Int64(8 + payload.len)))
    end
    empty!(raw)
    return nothing
end

"""
`f(j, w)` for `j` in `1:n` on up to `ntasks` tasks (`_foreach`), results in
index order; `w` numbers the task, for per-task state. A failure rethrows the
lowest failing index's error, so it does not depend on scheduling.
"""
function _tasksmap(f::F, n::Int, ntasks::Int) where {F}
    results = Vector{Any}(undef, n)
    _foreach(n, ntasks) do j, w
        results[j] = f(j, w)
        return nothing
    end
    return results
end

const _PADDING = ntuple(k -> zeros(UInt8, k), 7)

"Write a message body: each piece, then its zero padding to 8 bytes."
function _writebody(io::IO, c::EncodeCursor)
    for b in c.body
        # An uncopied array over the piece; preserving the piece keeps its
        # owner alive across the write.
        GC.@preserve b Base.write(io, unsafe_wrap(Array, AC.sliceptr(b), Int(b.len)))
        pad = Int((-b.len) & 7)
        pad > 0 && Base.write(io, _PADDING[pad])
    end
    return nothing
end

"Encode view entries and the union of their referenced byte ranges."
function _encodeviewbuffers!(c::EncodeCursor, d::ArrayData)
    # In-memory views may borrow a whole parser input or a larger column.
    # IPC must not persist those unrelated bytes, null entries, or padding.
    views = AC.rolebuffer(d, AC.VIEWS)
    inputvalidity = AC.validitybuffer(d)
    entries = zeros(UInt8, AC.checked_mul(d.len, Int64(16)))
    payloads = reinterpret(UInt128, entries)
    words = reinterpret(Int32, entries)
    validity = AC.nullcount(d) == 0 ? UInt8[] : zeros(UInt8, cld(d.len, 8))
    # Prove that valid long entries cover a prefix of every source buffer.
    # Such columns need neither row sorting nor per-entry data copies.
    coverage = zeros(Int64, length(d.buffers) - 2)
    compact = true
    for i = 1:(d.len)
        (AC.isempty_buffer(inputvalidity) || AC.getbit(inputvalidity, i - 1)) || continue
        isempty(validity) || (validity[(i - 1) ÷ 8 + 1] |= UInt8(1) << ((i - 1) % 8))
        base = 16 * (i - 1)
        payload = AC.loadat(views, UInt128, base)
        len = reinterpret(Int32, payload % UInt32)
        # The writer requires little-endian storage. Mask inline padding;
        # long entries keep their source coordinates until remapped below.
        payloads[i] =
            len <= AC.VIEW_INLINE_MAX ? payload & (typemax(UInt128) >> (8 * (12 - len))) :
            payload
        if compact && len > AC.VIEW_INLINE_MAX
            bufidx, off = words[4 * i - 1], words[4 * i]
            extent = coverage[bufidx + 1]
            if off > extent
                compact = false
            else
                coverage[bufidx + 1] = max(extent, Int64(off) + len)
            end
        end
    end
    if compact && all(>(0), coverage)
        encodebuffer!(c, validity)
        encodebuffer!(c, entries)
        push!(c.variadics, Int64(length(coverage)))
        for (i, extent) in enumerate(coverage)
            # Only the proven prefix is copied; trailing bytes are unused.
            encodebuffer!(c, AC.subslice(d.buffers[i + 2], 0, extent))
        end
        return nothing
    end
    refs = Int64[i for i = 1:(d.len) if words[4 * i - 3] > AC.VIEW_INLINE_MAX]
    # Sort by source position so duplicate and overlapping views share the
    # same output bytes. Already ordered producer columns need no sort.
    sourcepos(i) = (words[4 * i - 1], words[4 * i])
    issorted(refs; by=sourcepos) || sort!(refs; by=sourcepos)
    buffers = Vector{UInt8}[]
    firstref = 1
    while firstref <= length(refs)
        bufidx = words[4 * refs[firstref] - 1]
        lastref = firstref
        extent = Int64(0)
        nbytes = Int64(0)
        while lastref <= length(refs) && words[4 * refs[lastref] - 1] == bufidx
            row = refs[lastref]
            off, len = words[4 * row], words[4 * row - 3]
            stop = Int64(off) + len
            nbytes += max(0, stop - max(Int64(off), extent))
            extent = max(extent, stop)
            lastref += 1
        end
        # One compact buffer per referenced source buffer keeps rewritten
        # offsets <= their original Int32 offsets, including large buffers.
        packed = Vector{UInt8}(undef, nbytes)
        source = AC._viewdatabuffer(d, bufidx)
        outputidx = Int32(length(buffers))
        pos = Int64(0)
        extent = Int64(0)
        GC.@preserve source packed begin
            for j = firstref:(lastref - 1)
                row = refs[j]
                off, len = words[4 * row], words[4 * row - 3]
                words[4 * row - 1] = outputidx
                words[4 * row] = Int32(pos - max(0, extent - off))
                stop = Int64(off) + len
                start = max(Int64(off), extent)
                count = max(0, stop - start)
                if count > 0
                    unsafe_copyto!(
                        pointer(packed, pos + 1),
                        AC.sliceptr(source) + start,
                        count,
                    )
                    pos += count
                end
                extent = max(extent, stop)
            end
        end
        push!(buffers, packed)
        firstref = lastref
    end
    encodebuffer!(c, validity)
    encodebuffer!(c, entries)
    push!(c.variadics, Int64(length(buffers)))
    for bytes in buffers
        encodebuffer!(c, bytes)
    end
    return nothing
end

"""
    encodefield!(cursor, f, d)

The write half of the registry walk — the exact mirror of `decodefield`:
one node, then the layout's buffers in registry order, then children in
declared order. Dictionary-encoded fields emit their INDEX buffers here;
their pool travels in a dictionary batch. View layouts compact referenced
content at this persistence boundary; other layouts emit their `ArrayData`
slices verbatim, apart from the empty terminal offset below.
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
    if t isa AC.ViewType
        _encodeviewbuffers!(c, d)
        return nothing
    end
    for (role, b) in zip(spec.buffers, d.buffers)
        if role == AC.OFFSETS && d.len == 0 && b.len == 0
            # Core canonicalizes an empty offset array without allocating its
            # otherwise-unused physical buffer. IPC still requires the one
            # terminal zero offset (length + 1 entries).
            encodebuffer!(c, zeros(UInt8, spec.offsetwidth))
        else
            encodebuffer!(c, b)
        end
    end
    if spec.variadic
        # View layouts append their variadic data buffers after the fixed
        # validity/views pair; the count travels in the header's
        # variadicBufferCounts vector, depth-first (format 1.4).
        push!(c.variadics, Int64(length(d.buffers) - length(spec.buffers)))
        for b in Iterators.drop(d.buffers, length(spec.buffers))
            encodebuffer!(c, b)
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

# Padded byte size of a column's buffers, children included and dictionary
# pools excluded (they travel in their own messages): the uncompressed body
# size.
function _bodybytes(d::ArrayData)
    n = Int64(0)
    for b in d.buffers
        n = AC.checked_add(n, (Int64(b.len) + 7) & ~Int64(7))
    end
    for child in d.children
        n = AC.checked_add(n, _bodybytes(child))
    end
    return n
end

# Capacity for an eager writer's output: the uncompressed record bodies plus
# slack for metadata and dictionaries, so the staging buffer rarely regrows.
function _outputhint(batches, codec::Int8)
    codec == CODEC_NONE || return 0
    n = Int64(4096)
    for batch in batches
        n = AC.checked_add(n, sum(_bodybytes, batch.columns; init=Int64(1024)))
    end
    return Int(n)
end

"A record batch message as its header and its body's `EncodeCursor`."
function _recordmessage(
    batch::AC.RecordBatch,
    fields,
    codec::Int8,
    states::Vector{EncodeState},
    ntasks::Int=1,
)
    c = EncodeCursor(codec)
    for (f, col) in zip(fields, batch.columns)
        encodefield!(c, f, col)
    end
    _compressbody!(c, states, ntasks)
    b = FB.Builder(1024)
    rb = _batchheader!(b, c, batch.nrows)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.RecordBatch)
    Meta.messageAddHeader(b, rb)
    Meta.messageAddBodyLength(b, c.bodylen)
    return _messageheader(b, Meta.messageEnd(b), c.bodylen), c
end

"A dictionary batch message as its header and its body's `EncodeCursor`."
function _dictionarymessage(
    id::Int64,
    vf::Field,
    pool::ArrayData,
    codec::Int8,
    states::Vector{EncodeState},
    ntasks::Int=1;
    isdelta::Bool=false,
)
    c = EncodeCursor(codec)
    encodefield!(c, vf, pool)
    _compressbody!(c, states, ntasks)
    b = FB.Builder(1024)
    rb = _batchheader!(b, c, pool.len)
    Meta.dictionaryBatchStart(b)
    Meta.dictionaryBatchAddId(b, id)
    Meta.dictionaryBatchAddData(b, rb)
    isdelta && Meta.dictionaryBatchAddIsDelta(b, true)
    dictbatch = Meta.dictionaryBatchEnd(b)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.DictionaryBatch)
    Meta.messageAddHeader(b, dictbatch)
    Meta.messageAddBodyLength(b, c.bodylen)
    return _messageheader(b, Meta.messageEnd(b), c.bodylen), c
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
function _streamfeatures(
    sch::Schema,
    batches,
    ids::IdDict{Field,Int64},
    codec::Int8,
    deltas=nothing,
)
    features = Int64[]
    current = Dict{Int64,ArrayData}()
    replacement = false
    for (k, batch) in enumerate(batches)
        for (f, pool) in dictionarypools(sch.fields, batch.columns)
            id = ids[f]
            old = get(current, id, nothing)
            old === pool ||
                old === nothing ||
                (deltas !== nothing && haskey(deltas[k], id)) ||
                (replacement = true)
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
end-of-stream marker (and the file-format Footer). A batch's messages are
encoded before any of them is published, so a validation or encoding failure
publishes nothing; each message is then written as its header followed by its
body, uncompressed buffers straight from the columns. The SINK gains bytes
batch by batch, and an abandoned state leaves a torn stream or an unfooted
file.

The eager `writestream`/`writefile` drive this same state over an in-memory
sink, validating every batch up front (`validate=false` per batch) and
declaring only the features their whole batch sequence uses.
"""
mutable struct IPCWriteState
    const io::IO
    const file::Bool
    const schema::Schema
    const fielddictids::IdDict{Field,Int64}
    const ids::IdDict{Field,Int64}
    const features::Vector{Int64}
    const codec::Int8
    # One codec context per compression task (none when uncompressed).
    const states::Vector{EncodeState}
    const ntasks::Int
    # How a pool that extends the one written before is written: `:delta`
    # (a delta dictionary batch), `:replace` (stream: the whole pool again),
    # or `:defer` (file: each id's final pool, once, by `finishwrite!`).
    const growth::Symbol
    # `:defer`: the dictionary fields to write at the end, in the order
    # nested pools need (inner before outer), each id once.
    const deferred::Vector{Tuple{Field,Int64}}
    const validated::AC._ValidatedDictionaries
    const current::Dict{Int64,ArrayData}  # last pool per id, for replacement
    const dictblocks::Vector{NTuple{3,Int64}}    # file format Footer Blocks
    const recordblocks::Vector{NTuple{3,Int64}}
    written::Int64                       # bytes published to `io`
    finished::Bool
    # A compressed batch encodes on its own task while the next one builds;
    # batches publish in order. Each pending entry is the encoding task and
    # the codec states it uses; `statesets` holds idle sets for reuse.
    const pending::Vector{Tuple{Task,Vector{EncodeState},Any}}
    const statesets::Vector{Vector{EncodeState}}
end

# Compressed batches encoding at once: two keep the tasks busy while one
# batch waits for its largest buffer, and two sets of frames are reused.
const _INFLIGHT = 2

function _checkgrowth(growth::Symbol, file::Bool)
    growth in (:delta, :replace, :defer) ||
        throw(ArgumentError("dictionary growth must be :delta, :replace, or :defer"))
    growth === (file ? :replace : :defer) && throw(
        ArgumentError(
            "dictionary growth $(repr(growth)) needs the " *
            (file ? "stream" : "file") *
            " format",
        ),
    )
    return growth
end

# Batch sequences are unknown up front. A declared feature is a demand on
# readers (nanoarrow refuses any stream declaring one it does not support),
# so nothing is declared speculatively: pool replacement needs the caller's
# explicit opt-in, and a codec declares compressed bodies because every
# batch this state writes will use it.
function _incrementalfeatures(
    ids::IdDict{Field,Int64},
    codec::Int8,
    file::Bool,
    dictreplacement::Bool,
)
    features = Int64[]
    dictreplacement &&
        !file &&
        !isempty(ids) &&
        push!(features, FEATURE_DICTIONARY_REPLACEMENT)
    codec == CODEC_NONE || push!(features, FEATURE_COMPRESSED_BODY)
    return features
end

# Codec contexts are created on first use, so unused ones cost nothing.
_encodestates(codec::Int8, level, ntasks::Integer) =
    codec == CODEC_NONE ? EncodeState[] : EncodeState[EncodeState(level) for _ = 1:ntasks]

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
    dictreplacement::Bool=false,
    compresslevel::Union{Nothing,Integer}=nothing,
    ntasks::Integer=1,
    growth::Symbol=:delta,
)
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkgrowth(growth, file)
    _validatewriterschema(sch)
    ids = assigndictids(sch.fields, dictids)
    fielddictids = IdDict{Field,Int64}(ids)
    validatedictionaryids(sch.fields, fielddictids)
    feats =
        features === nothing ? _incrementalfeatures(ids, codec, file, dictreplacement) :
        features
    st = IPCWriteState(
        io,
        file,
        sch,
        fielddictids,
        ids,
        feats,
        codec,
        _encodestates(codec, compresslevel, ntasks),
        Int(ntasks),
        growth,
        Tuple{Field,Int64}[],
        AC._ValidatedDictionaries(),
        Dict{Int64,ArrayData}(),
        NTuple{3,Int64}[],
        NTuple{3,Int64}[],
        Int64(0),
        false,
        Tuple{Task,Vector{EncodeState},Any}[],
        Vector{EncodeState}[],
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

# Publish staged messages in order: each header, then its body straight
# from the column buffers. A file-format Block records the global offset,
# the header length (prefix + metadata), and the body length.
function _publishmessages!(st::IPCWriteState, messages, states=st.states)
    for (header, body, blocks) in messages
        blocks === nothing ||
            push!(blocks, (st.written, Int64(length(header)), body.bodylen))
        Base.write(st.io, header)
        _writebody(st.io, body)
        st.written += length(header) + body.bodylen
    end
    # The sink holds the bytes now, so the frames can hold the next ones.
    _recycleframes!(states)
    return nothing
end

# Encode staged messages: each builder makes one (header, body) with `states`.
_buildmessages(builders, states, ntasks) = Tuple{Vector{UInt8},EncodeCursor,Any}[
    (build(states, ntasks)..., blocks) for (build, blocks) in builders
]

"Publish the oldest pending batch, after its encoding task finishes."
function _publishnext!(st::IPCWriteState)
    task, states, recycle = popfirst!(st.pending)
    messages = try
        fetch(task)
    catch err
        _recycleframes!(states)
        push!(st.statesets, states)
        err isa TaskFailedException && throw(err.task.exception)
        rethrow()
    end
    _publishmessages!(st, messages, states)
    push!(st.statesets, states)
    _recycle!(st, recycle)
    return nothing
end

"Publish every pending batch, in order."
function drainwrite!(st::IPCWriteState)
    while !isempty(st.pending)
        _publishnext!(st)
    end
    return nothing
end

"Order-sensitive pool content equality, in each pool's STORAGE value domain."
function _poolsequal(vf::Field, a::ArrayData, b::ArrayData)
    a.len == b.len || return false
    return isequal(AC.materialize(vf, a), AC.materialize(vf, b))
end

"""
    writebatch!(st, batch; validate=true, deltas=nothing)

Publish one record batch, preceded by any dictionary batches it needs. A pool
whose id has `deltas[id]` extends the pool written before by those values:
it is written as `st.growth` says (a delta dictionary batch, the whole pool
again in a stream, or nothing until `finishwrite!` writes each file
dictionary once). Any other changed pool is a replacement (stream format,
declared) or refused.
"""
function writebatch!(
    st::IPCWriteState,
    batch::AC.RecordBatch;
    validate::Bool=true,
    deltas::Union{Nothing,Dict{Int64,ArrayData}}=nothing,
    recycle=nothing,
)
    st.finished && throw(ArgumentError("this IPC writer is already finished"))
    if validate
        _checkbatches(st.schema, (batch,))
        _validatewriterbatch!(st.validated, st.schema, batch, st.ids)
    end
    # Stage every message (dictionaries first, then the record batch) and the
    # pool updates, so a failure publishes nothing and changes no state. A
    # shared or nested dictionary id occurs more than once in a batch, always
    # with one pool: its first occurrence stages the pool, the rest see it.
    builders = Tuple{Any,Any}[]
    staged = Dict{Int64,ArrayData}()
    deferred = Tuple{Field,Int64}[]
    dictblocks = st.file ? st.dictblocks : nothing
    for (f, pool) in dictionarypools(st.schema.fields, batch.columns)
        id = st.ids[f]
        old = haskey(staged, id) ? staged[id] : get(st.current, id, nothing)
        old === pool && continue
        vf = AC.dictvaluefield(f, f.type::DictionaryType)
        delta = deltas === nothing || old === nothing ? nothing : get(deltas, id, nothing)
        if st.growth === :defer
            # Batches index into the pool as far as it has grown; the final
            # pool covers them all.
            old === nothing ||
                delta !== nothing ||
                _poolsequal(vf, old, pool) ||
                throw(
                    ValidationError(
                        "column $(f.name) changed its existing dictionary " *
                        "categories; the IPC file format carries one dictionary " *
                        "per column, which can only gain categories — use the " *
                        "stream format for replacement",
                    ),
                )
            old === nothing && push!(deferred, (f, id))
            staged[id] = pool
            continue
        end
        if delta !== nothing && st.growth === :replace
            # A stream re-sends a grown pool whole, which every reader accepts.
            push!(builders, (_dictbuilder(st, id, vf, pool, false), dictblocks))
            staged[id] = pool
            continue
        end
        if delta !== nothing
            old.len + delta.len == pool.len || throw(
                ArgumentError("delta dictionary does not complete the pool for id $id"),
            )
            validate_semantic(vf, delta)
            push!(builders, (_dictbuilder(st, id, vf, delta, true), dictblocks))
            staged[id] = pool
            continue
        end
        if old !== nothing
            if st.file
                # The file format carries one dictionary per id, extended
                # only by deltas. A new pool object with identical content
                # references the emitted batches; anything else is
                # replacement, which files cannot say.
                _poolsequal(vf, old, pool) || throw(
                    ValidationError(
                        "column $(f.name) changed its existing dictionary " *
                        "categories; the IPC file format carries one dictionary " *
                        "per column (new categories are appended as deltas) — use " *
                        "the stream format for replacement",
                    ),
                )
                staged[id] = pool
                continue
            end
            if !(FEATURE_DICTIONARY_REPLACEMENT in st.features)
                # Require an explicit replacement declaration on output;
                # the reader also accepts undeclared replacements from peers.
                _poolsequal(vf, old, pool) || throw(
                    ValidationError(
                        "column $(f.name) changed its existing dictionary " *
                        "categories, but the stream's schema message does not " *
                        "declare the DictionaryReplacement feature (create the " *
                        "stream with Arrow.Writer(...; dictreplacement=true))",
                    ),
                )
                staged[id] = pool
                continue
            end
        end
        push!(builders, (_dictbuilder(st, id, vf, pool, false), dictblocks))
        staged[id] = pool
    end
    fields, codec = st.schema.fields, st.codec
    record = (states, k) -> _recordmessage(batch, fields, codec, states, k)
    push!(builders, (record, st.file ? st.recordblocks : nothing))
    if st.ntasks > 1 && codec != CODEC_NONE
        states =
            isempty(st.statesets) ? _encodestates(codec, st.states[1].level, st.ntasks) :
            pop!(st.statesets)
        task = Threads.@spawn _buildmessages(builders, states, st.ntasks)
        push!(st.pending, (task, states, recycle))
        merge!(st.current, staged)
        length(st.pending) > _INFLIGHT - 1 && _publishnext!(st)
    else
        _publishmessages!(st, _buildmessages(builders, st.states, st.ntasks))
        merge!(st.current, staged)
        _recycle!(st, recycle)
    end
    append!(st.deferred, deferred)
    return nothing
end

_dictbuilder(st::IPCWriteState, id, vf, pool, isdelta::Bool) =
    (states, k) -> _dictionarymessage(id, vf, pool, st.codec, states, k; isdelta)

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
    drainwrite!(st)
    if st.growth === :defer && !isempty(st.current)
        # Each id's final pool once, after the record batches that use it (the
        # file format allows a dictionary anywhere before the footer).
        messages = Tuple{Vector{UInt8},EncodeCursor,Any}[]
        for (f, id) in st.deferred
            vf = AC.dictvaluefield(f, f.type::DictionaryType)
            header, body =
                _dictionarymessage(id, vf, st.current[id], st.codec, st.states, st.ntasks)
            push!(messages, (header, body, st.dictblocks))
        end
        _publishmessages!(st, messages)
    end
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
    _closestates!(st)
    return nothing
end

"Release the codec state without emitting anything; for error cleanup."
function abortwrite!(st::IPCWriteState)
    st.finished && return nothing
    st.finished = true
    for (task, states, _) in st.pending
        # A failure here follows from the caller's error.
        try
            wait(task)
        catch
        end
        push!(st.statesets, states)
    end
    empty!(st.pending)
    _closestates!(st)
    return nothing
end

function _closestates!(st::IPCWriteState)
    foreach(close, st.states)
    foreach(states -> foreach(close, states), st.statesets)
    empty!(st.statesets)
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
    compresslevel::Union{Nothing,Integer}=nothing,
    current::Dict{Int64,ArrayData}=Dict{Int64,ArrayData}(),
    ntasks::Integer=1,
    growth::Symbol=:delta,
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
        _encodestates(codec, compresslevel, ntasks),
        Int(ntasks),
        _checkgrowth(growth, false),
        Tuple{Field,Int64}[],
        AC._ValidatedDictionaries(),
        current,
        NTuple{3,Int64}[],
        NTuple{3,Int64}[],
        Int64(0),
        false,
        Tuple{Task,Vector{EncodeState},Any}[],
        Vector{EncodeState}[],
    )
end

"""
    writestream(sch, batches; compress=:none, compresslevel=nothing, dictids=IdDict{Field,Int64}()) -> Vector{UInt8}
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
    compresslevel::Union{Nothing,Integer}=nothing,
    dictids::IdDict{Field,Int64}=IdDict{Field,Int64}(),
)
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkbatches(sch, batches)
    _validatewriterschema(sch)
    # `beginwrite!` recomputes the same ids from `dictids` (assignment is
    # deterministic); this pass exists so the whole batch sequence is
    # validated — and its EXACT features computed — before any bytes exist.
    # A declared feature is a demand on readers, so an eager stream declares
    # only what these batches actually use.
    ids = assigndictids(sch.fields, dictids)
    validatedictionaryids(sch.fields, IdDict{Field,Int64}(ids))
    _validatewriterbatches(sch, batches, ids)
    io = IOBuffer(; sizehint=_outputhint(batches, codec))
    st = beginwrite!(
        io,
        sch;
        file=false,
        compress=compress,
        compresslevel=compresslevel,
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

writestream(s::IPCStream; compress::Symbol=:none) =
    writestream(s.schema, s.batches; compress=compress, dictids=s.fielddictids)

# ---------------------------------------------------------------------------
# File format: magic + stream messages + Block index + Footer
# ---------------------------------------------------------------------------

const FILE_MAGIC = b"ARROW1"

"""
    writefile(sch, batches; compress=:none, compresslevel=nothing, dictids=IdDict{Field,Int64}()) -> Vector{UInt8}
    writefile(stream::IPCStream; compress=:none) -> Vector{UInt8}

The file variant: leading magic, the same stream messages, an end-of-stream
marker, then the Footer with its dictionary and record-batch Block indexes,
the Int32 footer length, and the trailing magic. Footer bookkeeping is
isolated here; message writing is the stream code above. The file format
carries one dictionary per id: a later pool that extends the previous one
value by value is written as a delta dictionary batch, and any other pool
change is a clean refusal (the stream format handles replacement).
`dictids` preserves ids from a source table (the `IPCStream` form passes the
reader's), so fields sharing one id keep sharing one dictionary batch,
exactly as the source did.
"""
function writefile(
    sch::Schema,
    batches::AbstractVector{AC.RecordBatch};
    compress::Symbol=:none,
    compresslevel::Union{Nothing,Integer}=nothing,
    dictids::IdDict{Field,Int64}=IdDict{Field,Int64}(),
)
    _requirelittleendian()
    haskey(CODEC_NAMES, compress) ||
        throw(ArgumentError("compress must be :none, :lz4, or :zstd"))
    codec = CODEC_NAMES[compress]
    _checkbatches(sch, batches)
    _validatewriterschema(sch)
    ids = assigndictids(sch.fields, dictids)
    # Same shared-id contract as the stream writer: compatible value schemas,
    # one nested id topology, one pool per id within each batch.
    validatedictionaryids(sch.fields, IdDict{Field,Int64}(ids))
    _validatewriterbatches(sch, batches, ids)
    deltas = _batchdeltas(sch, batches, ids)
    # CODEC_NONE so this refusal sees only DICTIONARY_REPLACEMENT.
    # COMPRESSED_BODY is legal in a file and is declared through the features
    # below.
    isempty(_streamfeatures(sch, batches, ids, CODEC_NONE, deltas)) || throw(
        ValidationError(
            "the IPC file format carries one dictionary per id, extended only " *
            "by delta batches; a dictionary whose existing categories change " *
            "requires the stream format",
        ),
    )
    io = IOBuffer(; sizehint=_outputhint(batches, codec))
    st = beginwrite!(
        io,
        sch;
        file=true,
        compress=compress,
        compresslevel=compresslevel,
        dictids=dictids,
        features=_streamfeatures(sch, batches, ids, codec, deltas),
    )
    try
        for (batch, delta) in zip(batches, deltas)
            writebatch!(st, batch; validate=false, deltas=delta)
        end
        finishwrite!(st)
    finally
        abortwrite!(st)
    end
    return take!(io)
end

writefile(s::IPCStream; compress::Symbol=:none) =
    writefile(s.schema, s.batches; compress=compress, dictids=s.fielddictids)

# Each batch's delta dictionaries against the pools the batches before it use.
function _batchdeltas(sch::Schema, batches, ids)
    current = Dict{Int64,ArrayData}()
    return map(batches) do batch
        deltas = _dictionarydeltas(current, sch.fields, ids, batch)
        for (f, pool) in dictionarypools(sch.fields, batch.columns)
            current[ids[f]] = pool
        end
        deltas
    end
end

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
    # Every extent is nonnegative, so these comparisons cannot overflow.
    (
        offset <= dataend &&
        metalen <= dataend - offset &&
        bodylen <= dataend - offset - metalen
    ) || throw(ValidationError("footer block escapes the data section"))
    frameend = offset + metalen + bodylen
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
    # An absent version slot is the schema default, V1 (0).
    versionpos = _blockfield(msg, 0, 2, "message version slot")
    version =
        versionpos === nothing ? Int16(0) :
        _blockload(metadata, Int16, versionpos, "message version slot")
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
    return bodylen, headertype, batch, version
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
    messagebodylen, headertype, batch, version = _blockmessagebatch(metadata)
    messagebodylen == bodylen ||
        throw(ValidationError("footer block body length does not match the message"))
    headertype == expectedheadertype ||
        throw(ValidationError("footer block has the wrong message header type"))
    _verifyblockbuffers(batch, bodylen)
    return version
end

"""
Preflight every Footer Block's frame. Returns the end of the indexed data and
the metadata version the indexed messages share (`footerversion` when the
file indexes no message).
"""
function _verifyblockframes(
    blob::BufferSlice,
    dictblocks,
    recordblocks,
    dataend::Int64,
    footerversion::Int16;
    datastart::Int64=0,
)
    indexedend = _validateblockindex(dictblocks, recordblocks, dataend; datastart=datastart)
    version = Int16(0)
    for (blocks, headertype) in ((dictblocks, UInt8(2)), (recordblocks, UInt8(3)))
        for block in blocks
            v = _verifyblockframe(blob, block, headertype)
            version = _filemetadataversion(footerversion, v, version == 0 ? v : version)
        end
    end
    return indexedend, version == 0 ? footerversion : version
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
flatbuffer and its schema, and the frame of every message the footer
indexes; eagerly decode the dictionary blocks (shared by every record batch);
expose record batches lazily through the Block index. The schema comes from
the footer; the leading Schema message is not read.
Pass `mmapregion(path)` to read a file through Core's mmap path. The file
format permits one base dictionary per id, followed by deltas.
Full dictionary replacement remains a format error.
"""
readfile(bytes::Vector{UInt8}; limits::Limits=Limits()) =
    _readfile(heapregion(bytes), limits, _readbudget(limits, length(bytes)))

readfile(region::OwnerRegion; limits::Limits=Limits()) =
    _readfile(region, limits, _readbudget(limits, region.len))

# C++ WriteFileFooter uses its current metadata version even when the
# writer emits V4 messages, and Arrow.jl before 2.6 wrote a V4 footer over V5
# messages. The footer must not select the message layout.
function _filemetadataversion(footer::Int16, message::Int16, expected::Int16=message)
    message == expected ||
        throw(ValidationError("IPC metadata version changes within the file"))
    (
        footer == message ||
        (footer in (Int16(3), Int16(4)) && message in (Int16(3), Int16(4)))
    ) || throw(ValidationError("IPC message and footer metadata versions differ"))
    return message
end

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
    # The Footer is authoritative, as in Arrow C++: the schema comes from it
    # and every message is reached through its Blocks, so the leading Schema
    # message is never read. (Polars writes that message without its stream
    # framing.) Data starts after the padded magic (`ARROW1` plus two bytes).
    # Validate the indexed messages against the Footer boundary first. Only
    # then can the final eight bytes be classified as an optional EOS marker:
    # a no-EOS file may end its last data buffer with the same byte pattern.
    datastart = Int64(8)
    indexedend, version = _verifyblockframes(
        blob,
        dictblocks,
        recordblocks,
        footerstart,
        version;
        datastart=datastart,
    )
    haseos =
        footerstart - indexedend >= 8 &&
        AC.loadat(blob, UInt32, footerstart - 8) == CONTINUATION &&
        AC.loadat(blob, UInt32, footerstart - 4) == UInt32(0)
    dataend = haseos ? footerstart - 8 : footerstart
    _validateblockindex(dictblocks, recordblocks, dataend; datastart=datastart)
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
    framed(_, block) =
        let fm = _blockmessage(region, block, dataend, limits, budget)
            fm.version == version ||
                throw(ValidationError("IPC metadata version changes within the file"))
            fm
        end
    dicts, validated = _filedictionaries(
        framed,
        dictblocks,
        dictids,
        dictvaluefields,
        fielddictids,
        limits,
        budget,
    )
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
end

"""
Decode a file's dictionary batches in footer order; `framed(i, block)` returns
block `i`'s framed message with its metadata version checked.
"""
function _filedictionaries(
    framed,
    dictblocks,
    dictids,
    dictvaluefields,
    fielddictids,
    limits::Limits,
    budget::AllocationBudget,
)
    dicts = Dict{Int64,ArrayData}()
    validated = AC._ValidatedDictionaries()
    state = DecodeState(budget)
    try
        for (i, block) in enumerate(dictblocks)
            fm = framed(i, block)
            rejectexperimentalcompression(fm)
            header = fm.msg.header
            header isa Meta.DictionaryBatch ||
                throw(ValidationError("footer dictionary block is not a dictionary batch"))
            haskey(dictids, header.id) ||
                throw(ValidationError("dictionary batch has unknown id $(header.id)"))
            _dictionarytransition(
                header.id,
                header.isDelta,
                haskey(dicts, header.id);
                file=true,
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
            _updatedictionary!(
                dicts,
                validated,
                header.id,
                header.isDelta,
                vf,
                decoded,
                limits,
                budget;
                file=true,
            )
        end
        return dicts, validated
    finally
        close(state)
    end
end

# Frame and check record batch `i` of `f` and plan its decode. With `mask`,
# only the fields it selects need their dictionaries (a scan).
function _fileplan(f::ArrowFile, i::Integer, budget::AllocationBudget, mask=nothing)
    1 <= i <= length(f.recordblocks) || throw(BoundsError(f, i))
    fm = _blockmessage(f.region, f.recordblocks[i], f.dataend, f.limits, budget)
    fm.version == f.schemaversion ||
        throw(ValidationError("IPC metadata version changes within the file"))
    rejectexperimentalcompression(fm)
    header = fm.msg.header
    header isa Meta.RecordBatch ||
        throw(ValidationError("footer record block is not a record batch"))
    if mask === nothing
        for id in missingdicts(f.fields, header.nodes, f.dictionaries, f.fielddictids)
            throw(
                ValidationError(
                    "record batch references dictionary id $id " *
                    "with no dictionary batch in the file",
                ),
            )
        end
    else
        _recordbatchmeta(header, f.fields, f.limits, fm.body.len)
        _scanmissingdicts(f.fields, header.nodes, f.dictionaries, f.fielddictids, mask)
    end
    return _recordplan(header, fm.body, fm.version, f.fields, f.limits)
end

_decodefilefield(f::ArrowFile, p::_RecordPlan, j::Int, state::DecodeState) = _decodeplanned(
    p,
    j,
    f.fields[j],
    f.dictionaries,
    f.fielddictids,
    f.limits,
    f.validated,
    state,
)

function _filebatch(f::ArrowFile, i::Integer, state::DecodeState)
    p = _fileplan(f, i, state.budget)
    cols = ArrayData[_decodefilefield(f, p, j, state) for j = 1:length(f.fields)]
    return AC.RecordBatch(f.schema, cols, p.rblen, f.validated)
end

"""
Decode record batches `idxs` of `f` on at most `ntasks` tasks: every batch's
framing and plan first, then each (batch, field) pair on its own, so one
large batch spreads over the tasks too. With `mask`, only the fields it
selects decode; the others stay `nothing`. Returns the plans and, per batch,
its columns.
"""
function _filebatches(
    f::ArrowFile,
    idxs,
    budget::AllocationBudget,
    ntasks::Int,
    mask=nothing,
)
    nb = length(idxs)
    nf = length(f.fields)
    _chargevector!(budget, _RecordPlan{BufferSlice}, nb, "batch decode plans")
    plans = Vector{_RecordPlan{BufferSlice}}(undef, nb)
    _foreach(nb, ntasks) do k, _
        plans[k] = _fileplan(f, idxs[k], budget, mask)
        return nothing
    end
    _chargevector!(
        budget,
        Union{Nothing,ArrayData},
        AC.checked_mul(nb, nf),
        "batch columns",
    )
    cols = [Vector{Union{Nothing,ArrayData}}(nothing, nf) for _ = 1:nb]
    states = [DecodeState(budget) for _ = 1:min(ntasks, nb * nf)]
    try
        _foreach(nb * nf, ntasks) do item, w
            k, j = divrem(item - 1, nf) .+ (1, 1)
            (mask === nothing || mask[j]) || return nothing
            cols[k][j] = _decodefilefield(f, plans[k], j, states[w])
            return nothing
        end
    finally
        foreach(close, states)
    end
    return plans, cols
end

# Record batches `idxs` of `f`, decoded on at most `ntasks` tasks.
function _filerecordbatches(f::ArrowFile, idxs, budget::AllocationBudget, ntasks::Int)
    plans, cols = _filebatches(f, idxs, budget, ntasks)
    _chargevector!(budget, AC.RecordBatch, length(idxs), "decoded batch list")
    return AC.RecordBatch[
        AC.RecordBatch(
            f.schema,
            ArrayData[c::ArrayData for c in cols[k]],
            p.rblen,
            f.validated,
        ) for (k, p) in enumerate(plans)
    ]
end

function Base.getindex(f::ArrowFile, i::Integer)
    budget = _readbudget(f.limits, f.region.len)
    state = DecodeState(budget)
    try
        return _filebatch(f, i, state)
    finally
        close(state)
    end
end
