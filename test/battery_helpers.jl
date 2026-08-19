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
# 2.x-written fixtures: bytes the OLD package wrote, frozen to disk so 3.0
# keeps proving it reads what deployed 2.x writers produced. While 2.x is
# still importable, ARROW_FIXTURE_MODE=record runs each site's closure (the
# original 2.x write, kept inline as provenance) and snapshots its bytes;
# the default replay mode never executes the closure — it reads the frozen
# file, so the closures may reference APIs that no longer exist.
# ---------------------------------------------------------------------------
const FIXTURES2X_DIR = joinpath(@__DIR__, "fixtures2x")
function _fixture2x(write2x::F, name::String) where {F}
    path = joinpath(FIXTURES2X_DIR, name * ".arrowbytes")
    if get(ENV, "ARROW_FIXTURE_MODE", "") == "record"
        bytes = write2x()::Vector{UInt8}
        mkpath(dirname(path))
        write(path, bytes)
        return bytes
    end
    isfile(path) || error("missing 2.x fixture $name — regenerate against " *
        "a 2.x checkout with ARROW_FIXTURE_MODE=record")
    return read(path)
end

# The mixed-layout table three batteries share, and its frozen 2.x-written
# two-partition stream (two record batches plus dictionary batches).
const MIXED_EXPECTED = (
    ints=Int64[1, 2, 3, 4, 5],
    floats=[1.5, missing, 3.5, missing, 5.5],
    bools=[true, false, true, missing, false],
    strs=["hey", "", missing, "αβ∀", "last"],
    lists=[[1, 2], Int64[], [3], missing, [4, 5, 6]],
    structs=[(a=1, b="x"), (a=2, b="y"), (a=3, b="z"), (a=4, b="w"), (a=5, b="v")],
    dict=["lo", "hi", "lo", missing, "hi"],
)
function _mixed_two_partitions_bytes()
    expected = MIXED_EXPECTED
    return _fixture2x("mixed-two-partitions") do
        io = IOBuffer()
        writetable = merge(expected, (dict=Arrow.DictEncode(expected.dict),))
        Arrow.write(io, Tables.partitioner([writetable, writetable]); file=false)
        take!(io)
    end
end

# Test-support helpers for exact, length-preserving metadata mutations. They
# use the same checked parser as the verifier, so the adversarial cases do not
# rely on generated unsafe getters to locate fields.
function _writele!(bytes::Vector{UInt8}, pos::Int64, x::UInt64, width::Int)
    _vrange(bytes, pos, width, "test mutation")
    for i = 0:(width - 1)
        bytes[pos + i + 1] = UInt8((x >> (8i)) & 0xff)
    end
    return bytes
end
_write_i64!(bytes, pos, x::Int64) = _writele!(bytes, pos, reinterpret(UInt64, x), 8)
_write_i32!(bytes, pos, x::Int32) = _writele!(bytes, pos, UInt64(reinterpret(UInt32, x)), 4)
_write_i16!(bytes, pos, x::Int16) = _writele!(bytes, pos, UInt64(reinterpret(UInt16, x)), 2)
_write_u32!(bytes, pos, x::UInt32) = _writele!(bytes, pos, UInt64(x), 4)

function _frameinfo(bytes::Vector{UInt8})
    info = NamedTuple[]
    pos = Int64(0)
    while pos < length(bytes)
        length(bytes) - pos >= 8 || throw(ValidationError("truncated test frame"))
        _vu32(bytes, pos) == CONTINUATION || throw(ValidationError("bad test frame"))
        metalen = Int64(_vi32(bytes, pos + 4))
        if metalen == 0
            push!(info, (kind=UInt8(0), frame=(pos + 1):(pos + 8),
                metadata=Int64(0):Int64(-1)))
            break
        end
        metastart = pos + 8
        meta = bytes[(metastart + 1):(metastart + metalen)]
        _, kind, _, _ = verify_ipc_metadata(meta, Limits())
        msg = _vtable(meta, Int64(_vu32(meta, 0)))
        bp = _vfield(msg, 3, 8)
        bodylen = bp === nothing ? Int64(0) : _vi64(meta, bp)
        frameend = AC.checked_add(AC.checked_add(metastart, metalen), bodylen)
        push!(info, (kind=kind, frame=(pos + 1):frameend,
            metadata=(metastart + 1):(metastart + metalen)))
        pos = frameend
    end
    return info
end

function _mutatemessage!(bytes::Vector{UInt8}, index::Int, f)
    frame = _frameinfo(bytes)[index]
    meta = copy(bytes[frame.metadata])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    f(meta, msg)
    copyto!(bytes, first(frame.metadata), meta, 1, length(meta))
    return bytes
end
_mutatemessage!(f, bytes::Vector{UInt8}, index::Int) =
    _mutatemessage!(bytes, index, f)

function _headertable(meta::Vector{UInt8}, msg::_VTable)
    return _vtable(meta, _vref(msg, 2; required=true))
end

function _rejects(f)
    try
        f()
        false
    catch e
        e isa Union{ValidationError,AllocationLimitError}
    end
end

function _compressed_wire(payload::Vector{UInt8}, declared::Int64)
    return vcat(collect(reinterpret(UInt8, [declared])), payload)
end

function _decode_fixture(codec::Int8, payload::Vector{UInt8}, declared::Int64;
    budget::Int64=max(declared, Int64(0)))
    bytes = _compressed_wire(payload, declared)
    wire = BufferSlice(heapregion(bytes), 0, length(bytes))
    state = DecodeState(AllocationBudget(budget))
    cursor = DecodeCursor(nothing, nothing, BufferSlice(), Limits();
        codec=codec, state=state)
    try
        return AC.slicebytes(_decompressbuffer!(cursor, wire))
    finally
        close(state)
    end
end

function _schema_stream_from_field!(b, field; features::Vector{Int64}=Int64[])
    Meta.schemaStartFieldsVector(b, 1)
    FB.prependoffset!(b, field)
    fields = FB.endvector!(b, 1)
    featurevec = 0
    if !isempty(features)
        FB.startvector!(b, 8, length(features), 8)
        foreach(x -> FB.prepend!(b, x), Iterators.reverse(features))
        featurevec = FB.endvector!(b, length(features))
    end
    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fields)
    featurevec == 0 || Meta.schemaAddFeatures(b, featurevec)
    sch = Meta.schemaEnd(b)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.Schema)
    Meta.messageAddHeader(b, sch)
    msg = Meta.messageEnd(b)
    FB.finish!(b, msg)
    meta = collect(FB.finishedbytes(b))
    resize!(meta, 8cld(length(meta), 8))
    out = UInt8[]
    append!(out, reinterpret(UInt8,
        UInt32[UInt32(CONTINUATION), UInt32(length(meta))]))
    append!(out, meta)
    append!(out, reinterpret(UInt8, UInt32[UInt32(CONTINUATION), 0]))
    return out
end

function _int64_schema_stream(features::Vector{Int64}=Int64[])
    b = FB.Builder(256)
    name = FB.createstring!(b, "x")
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(64))
    Meta.intAddIsSigned(b, true)
    typ = Meta.intEnd(b)
    Meta.fieldStartChildrenVector(b, 0)
    kids = FB.endvector!(b, 0)
    Meta.fieldStart(b)
    Meta.fieldAddName(b, name)
    Meta.fieldAddNullable(b, true)
    Meta.fieldAddTypeType(b, Meta.Int)
    Meta.fieldAddType(b, typ)
    Meta.fieldAddChildren(b, kids)
    return _schema_stream_from_field!(b, Meta.fieldEnd(b); features=features)
end

function _dictionary_schema_frame_with_replacement(id::Int64)
    b = FB.Builder(512)
    name = FB.createstring!(b, "d")

    Meta.utf8Start(b)
    valuetype = Meta.utf8End(b)
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(8))
    Meta.intAddIsSigned(b, true)
    indextype = Meta.intEnd(b)
    Meta.dictionaryEncodingStart(b)
    Meta.dictionaryEncodingAddId(b, id)
    Meta.dictionaryEncodingAddIndexType(b, indextype)
    dict = Meta.dictionaryEncodingEnd(b)

    Meta.fieldStartChildrenVector(b, 0)
    children = FB.endvector!(b, 0)
    Meta.fieldStart(b)
    Meta.fieldAddName(b, name)
    Meta.fieldAddTypeType(b, Meta.Utf8)
    Meta.fieldAddType(b, valuetype)
    Meta.fieldAddDictionary(b, dict)
    Meta.fieldAddChildren(b, children)
    field = Meta.fieldEnd(b)

    Meta.schemaStartFieldsVector(b, 1)
    FB.prependoffset!(b, field)
    fields = FB.endvector!(b, 1)
    FB.startvector!(b, 8, 1, 8)
    FB.prepend!(b, Int64(1)) # Feature.DICTIONARY_REPLACEMENT
    features = FB.endvector!(b, 1)

    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fields)
    Meta.schemaAddFeatures(b, features)
    sch = Meta.schemaEnd(b)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.Schema)
    Meta.messageAddHeader(b, sch)
    msg = Meta.messageEnd(b)
    FB.finish!(b, msg)
    meta = collect(FB.finishedbytes(b))
    append!(meta, zeros(UInt8, mod(-length(meta), 8)))
    frame = UInt8[]
    append!(frame, reinterpret(UInt8,
        UInt32[UInt32(CONTINUATION), UInt32(length(meta))]))
    append!(frame, meta)
    return frame
end

function _dictionary_replacement_stream()
    id = Int64(7)
    firstbytes = _fixture2x("dict-replacement-first") do
        firstio = IOBuffer()
        Arrow.write(firstio,
            (d=Arrow.DictEncode(["aa", "bb", "aa"], id),); file=false)
        take!(firstio)
    end
    secondbytes = _fixture2x("dict-replacement-second") do
        secondio = IOBuffer()
        Arrow.write(secondio,
            (d=Arrow.DictEncode(["xx", "yy", "xx"], id),); file=false)
        take!(secondio)
    end
    firstframes = _frameinfo(firstbytes)
    secondframes = _frameinfo(secondbytes)
    frameof(frames, bytes, kind) = bytes[only(x.frame for x in frames if x.kind == kind)]
    return vcat(
        _dictionary_schema_frame_with_replacement(id),
        frameof(firstframes, firstbytes, UInt8(2)),
        frameof(firstframes, firstbytes, UInt8(3)),
        frameof(secondframes, secondbytes, UInt8(2)),
        frameof(secondframes, secondbytes, UInt8(3)),
        frameof(firstframes, firstbytes, UInt8(0)),
    )
end

function _experimental_v4_stream(value::Int64)
    schema = _int64_schema_stream()
    _mutatemessage!(schema, 1) do meta, msg
        _write_i16!(meta, _vfield(msg, 0, 2; required=true), Int16(3)) # V4
    end
    resize!(schema, length(schema) - 8) # remove helper EOS

    raw = collect(reinterpret(UInt8, [value]))
    compressed = transcode(Arrow.LZ4FrameCompressor, raw)
    body = vcat(collect(reinterpret(UInt8, Int64[Int64(length(raw))])), compressed)
    encodedlen = length(body)
    append!(body, zeros(UInt8, mod(-length(body), 8)))

    b = FB.Builder(512)
    key = FB.createstring!(b, EXPERIMENTAL_COMPRESSION_KEY)
    val = FB.createstring!(b, "LZ4")
    Meta.keyValueStart(b)
    Meta.keyValueAddKey(b, key)
    Meta.keyValueAddValue(b, val)
    kv = Meta.keyValueEnd(b)
    Meta.recordBatchStartNodesVector(b, 1)
    Meta.createFieldNode(b, Int64(1), Int64(0))
    nodes = FB.endvector!(b, 1)
    Meta.recordBatchStartBuffersVector(b, 2)
    Meta.createBuffer(b, Int64(0), Int64(encodedlen)) # data (reverse build)
    Meta.createBuffer(b, Int64(0), Int64(0))          # validity
    buffers = FB.endvector!(b, 2)
    Meta.recordBatchStart(b)
    Meta.recordBatchAddLength(b, Int64(1))
    Meta.recordBatchAddNodes(b, nodes)
    Meta.recordBatchAddBuffers(b, buffers)
    rb = Meta.recordBatchEnd(b)
    Meta.messageStartCustomMetadataVector(b, 1)
    FB.prependoffset!(b, kv)
    custom = FB.endvector!(b, 1)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V4)
    Meta.messageAddHeaderType(b, Meta.RecordBatch)
    Meta.messageAddHeader(b, rb)
    Meta.messageAddBodyLength(b, Int64(length(body)))
    Meta.messageAddCustomMetadata(b, custom)
    msg = Meta.messageEnd(b)
    FB.finish!(b, msg)
    meta = collect(FB.finishedbytes(b))
    append!(meta, zeros(UInt8, mod(-length(meta), 8)))
    prefix = collect(reinterpret(UInt8,
        UInt32[UInt32(CONTINUATION), UInt32(length(meta))]))
    eos = collect(reinterpret(UInt8,
        UInt32[UInt32(CONTINUATION), UInt32(0)]))
    return vcat(schema, prefix, meta, body, eos)
end

function _aliased_field_stream(depth::Int)
    b = FB.Builder(1024)
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(64))
    Meta.intAddIsSigned(b, true)
    typ = Meta.intEnd(b)
    Meta.fieldStartChildrenVector(b, 0)
    kids = FB.endvector!(b, 0)
    Meta.fieldStart(b)
    Meta.fieldAddTypeType(b, Meta.Int)
    Meta.fieldAddType(b, typ)
    Meta.fieldAddChildren(b, kids)
    next = Meta.fieldEnd(b)
    for _ = 1:depth
        Meta.fieldStartChildrenVector(b, 2)
        FB.prependoffset!(b, next)
        FB.prependoffset!(b, next)
        kids = FB.endvector!(b, 2)
        Meta.structStart(b)
        typ = Meta.structEnd(b)
        Meta.fieldStart(b)
        Meta.fieldAddTypeType(b, Meta.Struct)
        Meta.fieldAddType(b, typ)
        Meta.fieldAddChildren(b, kids)
        next = Meta.fieldEnd(b)
    end
    return _schema_stream_from_field!(b, next)
end

function _shared_name_stream(nfields::Int, namesize::Int)
    b = FB.Builder(max(1024, namesize + 1024))
    name = FB.createstring!(b, repeat("x", namesize))
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(64))
    Meta.intAddIsSigned(b, true)
    typ = Meta.intEnd(b)
    Meta.fieldStartChildrenVector(b, 0)
    kids = FB.endvector!(b, 0)
    fields = Vector{FB.UOffsetT}(undef, nfields)
    for i = 1:nfields
        Meta.fieldStart(b)
        Meta.fieldAddName(b, name)
        Meta.fieldAddTypeType(b, Meta.Int)
        Meta.fieldAddType(b, typ)
        Meta.fieldAddChildren(b, kids)
        fields[i] = Meta.fieldEnd(b)
    end
    Meta.schemaStartFieldsVector(b, nfields)
    for f in Iterators.reverse(fields)
        FB.prependoffset!(b, f)
    end
    fieldvec = FB.endvector!(b, nfields)
    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fieldvec)
    sch = Meta.schemaEnd(b)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.Schema)
    Meta.messageAddHeader(b, sch)
    msg = Meta.messageEnd(b)
    FB.finish!(b, msg)
    meta = collect(FB.finishedbytes(b))
    resize!(meta, 8cld(length(meta), 8))
    out = UInt8[]
    append!(out, reinterpret(UInt8,
        UInt32[UInt32(CONTINUATION), UInt32(length(meta))]))
    append!(out, meta)
    append!(out, reinterpret(UInt8, UInt32[UInt32(CONTINUATION), 0]))
    return out
end

function _zero_width_schema_stream(fixedlist::Bool)
    b = FB.Builder(1024)
    children = FB.UOffsetT(0)
    if fixedlist
        Meta.intStart(b)
        Meta.intAddBitWidth(b, Int32(64))
        Meta.intAddIsSigned(b, true)
        childtype = Meta.intEnd(b)
        Meta.fieldStartChildrenVector(b, 0)
        childkids = FB.endvector!(b, 0)
        Meta.fieldStart(b)
        Meta.fieldAddTypeType(b, Meta.Int)
        Meta.fieldAddType(b, childtype)
        Meta.fieldAddChildren(b, childkids)
        child = Meta.fieldEnd(b)
        Meta.fieldStartChildrenVector(b, 1)
        FB.prependoffset!(b, child)
        children = FB.endvector!(b, 1)
        Meta.fixedSizeListStart(b)       # listSize=0 is omitted by default
        typ = Meta.fixedSizeListEnd(b)
        tag = Meta.FixedSizeList
    else
        Meta.fieldStartChildrenVector(b, 0)
        children = FB.endvector!(b, 0)
        Meta.fixedSizeBinaryStart(b)     # byteWidth=0 is omitted by default
        typ = Meta.fixedSizeBinaryEnd(b)
        tag = Meta.FixedSizeBinary
    end
    Meta.fieldStart(b)
    Meta.fieldAddTypeType(b, tag)
    Meta.fieldAddType(b, typ)
    Meta.fieldAddChildren(b, children)
    field = Meta.fieldEnd(b)
    return _schema_stream_from_field!(b, field)
end

function _misaligned_empty_buffers_stream()
    # FlatBuffers C++ historically aligns an empty vector only for its UInt32
    # length, not for an element that does not exist. Official Arrow
    # integration streams therefore contain empty vectors of 16-byte Buffer
    # structs whose nominal element area is four-byte aligned. Relocate the
    # empty buffers vector of the frozen zero-row Null batch to reproduce
    # that valid encoding.
    bytes = _fixture2x("null-column-zero-rows") do
        io = IOBuffer()
        Arrow.write(io, (x=Missing[],); file=false)
        take!(io)
    end
    frames = _frameinfo(bytes)
    schemaidx = only(findall(x -> x.kind == 1, frames))
    recordidx = only(findall(x -> x.kind == 3, frames))
    eosidx = only(findall(x -> x.kind == 0, frames))

    meta = copy(bytes[frames[recordidx].metadata])
    msg = _vtable(meta, Int64(_vu32(meta, 0)))
    record = _headertable(meta, msg)
    bufferslot = _vfield(record, 2, 4; required=true)
    oldvector = _vref(record, 2; required=true)
    _vu32(meta, oldvector) == 0 || error("Null fixture has nonempty buffers")

    target = Int64(length(meta))
    target % 8 == 0 || error("padded metadata is not eight-byte aligned")
    append!(meta, zeros(UInt8, 8)) # zero length plus framing padding
    _write_u32!(meta, bufferslot, UInt32(target - bufferslot))

    out = UInt8[]
    append!(out, bytes[frames[schemaidx].frame])
    append!(out, reinterpret(UInt8,
        UInt32[UInt32(CONTINUATION), UInt32(length(meta))]))
    append!(out, meta)
    append!(out, bytes[frames[eosidx].frame])
    return out
end

function _misaligned_empty_children_stream()
    bytes = _zero_width_schema_stream(false)
    _mutatemessage!(bytes, 1) do meta, msg
        schema = _headertable(meta, msg)
        fields, nfields = _vvector(schema, 1, 4; required=true)
        nfields == 1 || error("fixture schema has an unexpected field count")
        field = _vtable(meta, fields + Int64(_vu32(meta, fields)))
        slot = _vfield(field, 5, 4; required=true)
        vector = _vref(field, 5; required=true)
        _vu32(meta, vector) == 0 || error("fixture has nonempty children")
        # Retarget the children reference one byte early: the length word
        # then sits at a position that is not 4-aligned, which the verifier
        # must reject before any generated getter dereferences it.
        vector % 4 == 0 || error("fixture vector was not aligned to begin with")
        _write_u32!(meta, slot, UInt32(_vu32(meta, slot) - 1))
    end
    return bytes
end

function _metadata_value_stream(explicit_empty::Bool)
    b = FB.Builder(1024)
    key = FB.createstring!(b, "owner")
    value = explicit_empty ? FB.createstring!(b, "") : zero(FB.UOffsetT)
    Meta.keyValueStart(b)
    Meta.keyValueAddKey(b, key)
    explicit_empty && Meta.keyValueAddValue(b, value)
    kv = Meta.keyValueEnd(b)
    Meta.schemaStartCustomMetadataVector(b, 1)
    FB.prependoffset!(b, kv)
    custom = FB.endvector!(b, 1)

    name = FB.createstring!(b, "x")
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(64))
    Meta.intAddIsSigned(b, true)
    typ = Meta.intEnd(b)
    Meta.fieldStart(b)
    Meta.fieldAddName(b, name)
    Meta.fieldAddNullable(b, true)
    Meta.fieldAddTypeType(b, Meta.Int)
    Meta.fieldAddType(b, typ)
    field = Meta.fieldEnd(b)
    Meta.schemaStartFieldsVector(b, 1)
    FB.prependoffset!(b, field)
    fields = FB.endvector!(b, 1)

    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fields)
    Meta.schemaAddCustomMetadata(b, custom)
    schema = Meta.schemaEnd(b)
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, Meta.Schema)
    Meta.messageAddHeader(b, schema)
    msg = Meta.messageEnd(b)
    FB.finish!(b, msg)
    meta = collect(FB.finishedbytes(b))
    resize!(meta, 8cld(length(meta), 8))
    out = UInt8[]
    append!(out, reinterpret(UInt8,
        UInt32[UInt32(CONTINUATION), UInt32(length(meta))]))
    append!(out, meta)
    append!(out, reinterpret(UInt8, UInt32[UInt32(CONTINUATION), 0]))
    return out
end

const TEST_CONFORMING_RELEASES = ReleaseCounter()
const TEST_NONCONFORMING_RELEASES = ReleaseCounter()

function _test_conforming_release(p::Ptr{CArrowArray})::Cvoid
    increment!(TEST_CONFORMING_RELEASES)
    _store_field!(p, :release, Ptr{Cvoid}(C_NULL))
    return nothing
end

function _test_nonconforming_release(::Ptr{CArrowArray})::Cvoid
    increment!(TEST_NONCONFORMING_RELEASES)
    return nothing
end

# Runtime accessors, NOT module-level pointer constants: a raw @cfunction
# pointer stored in a const is serialized into the precompile cache and is
# garbage when the package reloads. The static @cfunction form is cheap at
# runtime (it returns the session's cached trampoline).
test_conforming_release() =
    @cfunction(_test_conforming_release, Cvoid, (Ptr{CArrowArray},))
test_nonconforming_release() =
    @cfunction(_test_nonconforming_release, Cvoid, (Ptr{CArrowArray},))

function _test_c_array(release::Ptr{Cvoid})
    return CArrowArray(0, 0, 0, 0, 0, Ptr{Ptr{Cvoid}}(C_NULL),
        Ptr{Ptr{CArrowArray}}(C_NULL), Ptr{CArrowArray}(C_NULL), release,
        Ptr{Cvoid}(C_NULL))
end

# ---------------------------------------------------------------------------
# C Data battery support: release probes, topology mutators, stress
# ---------------------------------------------------------------------------

_registry_count() = lock(REGISTRY_LOCK) do
    length(EXPORT_REGISTRY)
end

function _call_release(p::Ptr{CArrowSchema})
    release = lock(REGISTRY_LOCK) do
        unsafe_load(p).release
    end
    release == C_NULL || ccall(release, Cvoid, (Ptr{CArrowSchema},), p)
    return nothing
end

function _call_release(p::Ptr{CArrowArray})
    release = lock(REGISTRY_LOCK) do
        unsafe_load(p).release
    end
    release == C_NULL || ccall(release, Cvoid, (Ptr{CArrowArray},), p)
    return nothing
end

function _expect_invalid_list_topology!(mutate)
    f, d = fromjulia("bad-list", [Int64[1]])
    before = _registry_count()
    sp, ap = to_c_data(f, d)
    mutate(sp, ap)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(ap).release == C_NULL
    @assert reap!() == 2
    @assert _registry_count() == before
    return nothing
end

function _expect_invalid_dictionary_topology!(mutate)
    vf, vd = fromjulia("values", ["x"])
    t = DictionaryType(IntType(32, true), vf.type, false)
    f = Field("bad-dictionary", t; nullable=false, children=vf.children)
    d = ArrayData(t, 1, [BufferSlice(), AC._databuffer(Int32[0])];
        dictionary=vd, nullcount=0)
    before = _registry_count()
    sp, ap = to_c_data(f, d)
    mutate(sp, ap)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(ap).release == C_NULL
    @assert reap!() == 2
    @assert _registry_count() == before
    return nothing
end

function _expect_invalid_schema_flags!(flags::Int64)
    f, d = fromjulia("bad-flags", Int64[1])
    before = _registry_count()
    sp, ap = to_c_data(f, d)
    _store_field!(sp, :flags, flags)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(ap).release == C_NULL
    @assert reap!() == 2
    @assert _registry_count() == before
    return nothing
end

@noinline function _import_and_forget(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray})
    f, d = from_c_data(sp, ap)
    @assert materialize(f, d) == [1]
    return nothing
end

@noinline function _export_and_forget()
    f, d = fromjulia("registry-rooted", Int64[1, 2])
    region = d.buffers[2].region
    sp, ap = to_c_data(f, d)
    return sp, ap, WeakRef(d), WeakRef(region)
end

function _stress_reaper(ready, start, done, workers)
    increment!(ready)
    wait(start)
    reaped = 0
    for _ = 1:10_000
        reaped += reap!()
        done[] == workers && _registry_count() == 0 && break
        yield()
    end
    return reaped
end

function _threaded_cdata_stress()
    Threads.nthreads() >= 4 ||
        error("threaded C Data stress requires at least four threads")

    # Different exported trees may release concurrently. Reapers scan and
    # claim those roots at the same time; each root must be popped once.
    n = 1_000
    workers = 4
    f, d = fromjulia("registry-race", Int64[1])
    roots = [to_c_data(f, d) for _ = 1:n]
    ready = ReleaseCounter()
    done = ReleaseCounter()
    start = Base.Event()
    releasers = [errormonitor(Threads.@spawn begin
        increment!(ready)
        wait(start)
        try
            for i = worker:workers:n
                sp, ap = roots[i]
                _call_release(sp)
                _call_release(ap)
                i % 16 == 0 && yield()
            end
        finally
            increment!(done)
        end
    end) for worker = 1:workers]
    reapers = [errormonitor(Threads.@spawn _stress_reaper(
        ready, start, done, workers)) for _ = 1:3]
    while ready[] != length(releasers) + length(reapers)
        yield()
    end
    notify(start)
    foreach(fetch, releasers)
    reaped_by_task = fetch.(reapers)
    reaped = sum(reaped_by_task) + reap!()
    @assert reaped == 2n (reaped, reaped_by_task, _registry_count())
    @assert _registry_count() == 0

    # One atomic swap must choose between explicit release and the registered
    # finalizer before either path reads or frees the native struct copy.
    rounds = 200
    before = TEST_CONFORMING_RELEASES[]
    owners = ForeignOwner[]
    for _ = 1:rounds
        owner = ForeignOwner(_test_c_array(test_conforming_release()))
        _arm_foreign_owner!(owner)
        push!(owners, owner)
    end
    ready = ReleaseCounter()
    start = Base.Event()
    contenders = Task[]
    for owner in owners
        push!(contenders, errormonitor(Threads.@spawn begin
            increment!(ready)
            wait(start)
            release!(owner)
        end))
        push!(contenders, errormonitor(Threads.@spawn begin
            increment!(ready)
            wait(start)
            finalize(owner)
        end))
    end
    while ready[] != length(contenders)
        yield()
    end
    notify(start)
    foreach(fetch, contenders)
    @assert TEST_CONFORMING_RELEASES[] - before == rounds
    for owner in owners
        @assert (@atomic owner.released)
    end
    return nothing
end

# Test-support: one 16-byte view entry (inline / out-of-line forms).
_viewentry(len::Int, rest::Vector{UInt8}) =
    vcat(reinterpret(UInt8, Int32[Int32(len)]), rest, zeros(UInt8, 12 - length(rest)))
_viewlong(len::Int, prefix::Vector{UInt8}, bufidx::Int, off::Int) =
    vcat(reinterpret(UInt8, Int32[Int32(len)]), prefix,
         reinterpret(UInt8, Int32[Int32(bufidx), Int32(off)]))

# ---------------------------------------------------------------------------
# Byte-range fetch accounting for the ranged-scan battery: a RangedSource
# over in-memory bytes that logs every range and byte it is asked for.
# ---------------------------------------------------------------------------
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

