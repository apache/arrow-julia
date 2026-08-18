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
# The IPC reader: stream and file formats as a thin peer over ArrowCore.
#
#   * Framing: checked spans, the generated FlatBuffers verifier before any
#     generated getter runs, and explicit resource limits (`Limits` +
#     `framemessages`) enforced before metadata-directed allocation. The
#     message body is the decoding AUTHORITY: every wire buffer is first a
#     checked subslice of its message-body slice, so corrupt metadata cannot
#     alias the schema message, another batch, or anything else in the file.
#     Positively compressed buffers are decoded into separate exact-sized
#     owned regions.
#
#   * Decoding: ONE generic recursive decoder (`decodefield`) walks nodes and
#     buffers in the order `layoutspec` declares; variadic layouts carry
#     their own bounded count handling.
#
#   * IPC bookkeeping stays in the adapter: dictionary ids live in an
#     adapter-side table; Core Fields carry `DictionaryType` object
#     references and never see an id.
#
#   * Metadata bindings and the verifier are GENERATED from the vendored
#     apache/arrow format/*.fbs (tools/fbsgen.jl -> src/metadata/) over the
#     vendored FlatBuffers runtime.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage-1 framing: resource limits before metadata-directed decode allocation
# ---------------------------------------------------------------------------

"""
Resource limits enforced before metadata-directed copying or decode. Only
small fixed Julia containers exist before these gates run, so a hostile
length prefix cannot direct an attacker-sized allocation.
"""
Base.@kwdef struct Limits
    max_metadata_bytes::Int64 = 16 * 1024 * 1024
    max_body_bytes::Int64 = 2 * 1024 * 1024 * 1024
    max_buffer_bytes::Int64 = 2 * 1024 * 1024 * 1024
    max_total_allocated_bytes::Int64 = 256 * 1024 * 1024
    max_messages::Int = 1_000_000
    max_metadata_objects::Int = 1_000_000
    max_nesting_depth::Int = 64
    max_array_length::Int64 = 1_000_000_000
end

mutable struct AllocationBudget
    left::Int64
end

"A caller-supplied cumulative allocation limit was exhausted."
struct AllocationLimitError <: Exception
    msg::String
end
Base.showerror(io::IO, e::AllocationLimitError) = print(io, e.msg)

function _charge!(budget::AllocationBudget, amount::Int64, what::AbstractString)
    amount >= 0 || throw(ArgumentError("negative allocation charge"))
    amount <= budget.left ||
        throw(AllocationLimitError("$what exceeds the reader allocation budget"))
    budget.left -= amount
    return nothing
end

struct FramedMessage
    msg::Meta.Message        # parsed flatbuffer metadata
    body::BufferSlice        # THE authority: buffers must subslice this
    version::Int16
    header_type::UInt8
    features::Vector{Int64}  # populated on schema messages
end

const CONTINUATION = 0xFFFFFFFF
const EXPERIMENTAL_COMPRESSION_KEY = "ARROW:experimental_compression"

# ---------------------------------------------------------------------------
# FlatBuffers verification (generated walkers over a schema-blind runtime)
# ---------------------------------------------------------------------------

# The shape verifier is GENERATED from the vendored format/*.fbs by
# tools/fbsgen.jl (src/metadata/Verifier.jl): table/vtable geometry,
# scalar widths and alignment, enum domains, string bounds/NUL/UTF-8, vector
# bounds, complete union dispatch, and the nesting/object/reserve accounting
# all derive from the schema, so binding drift cannot reach them. The
# wrappers below own only what the schema cannot express: which metadata
# versions and message kinds this adapter accepts, and the features/version
# coupling. Fixture helpers reuse the runtime's traversal primitives to
# LOCATE bytes they corrupt, so those names are aliased here.
const _VTable = Meta.VTable
const _vtable = Meta._vtable
const _vfield = Meta._vfield
const _vref = Meta._vref
const _vvector = Meta._vvector
const _vrange = Meta._vrange
const _vu8 = Meta._vu8
const _vu16 = Meta._vu16
const _vu32 = Meta._vu32
const _vi32 = Meta._vi32
const _vi64 = Meta._vi64

_vfail(msg) = throw(ValidationError("invalid IPC FlatBuffer: $msg"))

_verifyctx(limits::Limits, reserve_limit::Int64) =
    Meta.VerifyContext(Int64(limits.max_metadata_objects),
        limits.max_nesting_depth, reserve_limit)

# Translate the metadata module's verifier exceptions into the adapter's
# error vocabulary at the wrapper boundary.
function _verified(f::F) where {F}
    try
        return f()
    catch e
        e isa Meta.VerifyError && _vfail(e.msg)
        e isa Meta.VerifyBudgetError && throw(AllocationLimitError(e.msg))
        rethrow()
    end
end

function _schemafeatures(sch::Meta.Schema, version::Int16)
    fv = sch.features
    features = fv === nothing ? Int64[] : Int64[Int64(x) for x in fv]
    version == Int16(3) && !isempty(features) &&
        _vfail("schema features require metadata V5")
    return features
end

function verify_ipc_metadata(bytes::Vector{UInt8}, limits::Limits,
    reserve_limit::Int64=limits.max_total_allocated_bytes)
    ctx = _verifyctx(limits, reserve_limit)
    # STAGED root verification: the inline stage proves the table shell and
    # every non-reference field (the version among them), the adapter gates
    # the version, and only then does the reference stage walk the header
    # graph — an unsupported version rejects in constant time instead of
    # after a full attacker-directed traversal.
    t = _verified(() -> Meta.verifyrootstart_Message(bytes, ctx))
    msg = FB.getrootas(Meta.Message, bytes, 0)
    version = Int16(Int64(msg.version))
    version in (Int16(3), Int16(4)) ||
        _vfail("unsupported metadata version $version (only V4/V5 are accepted)")
    _verified(() -> Meta.verifyrootrest_Message(t, ctx))
    # The verifier proved header presence and rejected union members outside
    # the generated schemas (the Tensor family), so this dispatch is total.
    header = msg.header
    header_type = header isa Meta.Schema ? UInt8(1) :
        header isa Meta.DictionaryBatch ? UInt8(2) :
        header isa Meta.RecordBatch ? UInt8(3) :
        _vfail("unsupported message header tag")
    features = header isa Meta.Schema ? _schemafeatures(header, version) : Int64[]
    return version, header_type, features, ctx.reserved
end

"""
    framemessages(region, limits) -> Vector{FramedMessage}

Walk the IPC stream framing (continuation marker, metadata length, metadata
flatbuffer, body), checking every declared length against the limits and the
region's real extent before metadata-directed decode allocation. A truncated
prefix, metadata block, or body is an error here — not a silent early return
(the current framer returns `nothing` on truncation, src/table.jl:679-708) and
not a segfault three batches later. EOF exactly after a complete message is
the intentional missing-EOS boundary case and is accepted.
"""
framemessages(region::OwnerRegion, limits::Limits=Limits()) =
    _framemessages(region, limits, Base.ENDIAN_BOM,
        AllocationBudget(limits.max_total_allocated_bytes))

function _validatelimits(limits::Limits)
    limits.max_metadata_bytes >= 0 || throw(ArgumentError("negative metadata limit"))
    limits.max_body_bytes >= 0 || throw(ArgumentError("negative body limit"))
    limits.max_buffer_bytes >= 0 || throw(ArgumentError("negative buffer limit"))
    limits.max_total_allocated_bytes >= 0 ||
        throw(ArgumentError("negative allocation limit"))
    limits.max_messages >= 0 || throw(ArgumentError("negative message limit"))
    limits.max_metadata_objects >= 0 ||
        throw(ArgumentError("negative metadata-object limit"))
    limits.max_nesting_depth >= 0 || throw(ArgumentError("negative nesting limit"))
    limits.max_array_length >= 0 || throw(ArgumentError("negative array-length limit"))
    return nothing
end

function _framemessages(region::OwnerRegion, limits::Limits,
    host_endian_bom::UInt32,
    budget::AllocationBudget=AllocationBudget(limits.max_total_allocated_bytes))
    # The borrowed generated FlatBuffers bindings use native-endian scalar
    # loads. Reject an unsupported host before any generated getter sees the
    # little-endian wire bytes. The explicit argument keeps this ordering
    # testable on the supported little-endian CI host.
    host_endian_bom == UInt32(0x04030201) ||
        throw(ValidationError("the IPC reader requires a little-endian host"))
    _validatelimits(limits)
    blob = BufferSlice(region, 0, region.len)
    msgs = FramedMessage[]
    pos = Int64(0)   # 0-based byte position within the blob
    while pos < blob.len
        blob.len - pos >= 8 ||
            throw(ValidationError("truncated IPC prefix at byte $pos"))
        pos % 8 == 0 || throw(ValidationError("IPC message is not 8-byte aligned"))
        cont = AC.loadat(blob, UInt32, pos)
        cont == CONTINUATION ||
            throw(ValidationError("missing continuation marker at byte $pos"))
        metalen = Int64(AC.loadat(blob, Int32, AC.checked_add(pos, Int64(4))))
        if metalen == 0
            AC.checked_add(pos, Int64(8)) == blob.len ||
                throw(ValidationError("trailing bytes after IPC end-of-stream"))
            return msgs
        end
        length(msgs) < limits.max_messages ||
            throw(ValidationError("message count exceeds limit"))
        0 < metalen <= limits.max_metadata_bytes ||
            throw(ValidationError("metadata length $metalen outside (0, $(limits.max_metadata_bytes)]"))
        metalen % 8 == 0 ||
            throw(ValidationError("metadata length $metalen is not 8-byte aligned"))
        metastart = AC.checked_add(pos, Int64(8))
        bodyguess = AC.checked_add(metastart, metalen)
        bodyguess <= blob.len ||
            throw(ValidationError("truncated metadata: need $metalen bytes at $pos"))
        _charge!(budget, metalen, "metadata allocation")
        metabytes = AC.slicebytes(AC.subslice(blob, metastart, metalen))
        version, header_type, features, reserve =
            verify_ipc_metadata(metabytes, limits, budget.left)
        _charge!(budget, reserve, "verified metadata expansion")
        # No generated getter runs before the verifier has bounded the full
        # table/vector/string graph it may visit.
        msg = FB.getrootas(Meta.Message, metabytes, 0)
        bodylen = Int64(msg.bodyLength)
        0 <= bodylen <= limits.max_body_bytes ||
            throw(ValidationError("body length $bodylen outside [0, $(limits.max_body_bytes)]"))
        bodylen % 8 == 0 ||
            throw(ValidationError("body length $bodylen is not 8-byte aligned"))
        bodystart = bodyguess
        bodyend = AC.checked_add(bodystart, bodylen)
        bodyend <= blob.len ||
            throw(ValidationError("truncated body: need $bodylen bytes at $bodystart"))
        push!(msgs, FramedMessage(msg, AC.subslice(blob, bodystart, bodylen),
            version, header_type, features))
        pos = bodyend
    end
    return msgs
end

# ---------------------------------------------------------------------------
# Metadata mapping: Meta.* type structs -> Core runtime descriptors
# ---------------------------------------------------------------------------

# One value-level mapping table. Compare src/eltypes.jl, where this
# relationship is 22 `juliaeltype` + 21 `arrowtype` methods entangled with
# Julia-type conversion; here it is one function per direction on runtime
# values, and Julia conversion is someone else's (the facade's) concern.

function coretype(t)::ArrowType
    if t isa Meta.Int
        IntType(Int(t.bitWidth), t.is_signed)
    elseif t isa Meta.FloatingPoint
        FloatType(t.precision == Meta.Precision.HALF ? 16 :
                  t.precision == Meta.Precision.SINGLE ? 32 : 64)
    elseif t isa Meta.Bool
        BoolType()
    elseif t isa Meta.Utf8
        Utf8Type(false)
    elseif t isa Meta.LargeUtf8
        Utf8Type(true)
    elseif t isa Meta.Binary
        BinaryType(false)
    elseif t isa Meta.LargeBinary
        BinaryType(true)
    elseif t isa Meta.FixedSizeBinary
        FixedSizeBinaryType(Int(something(t.byteWidth, Int32(0))))
    elseif t isa Meta.List
        ListType(false)
    elseif t isa Meta.LargeList
        ListType(true)
    elseif t isa Meta.FixedSizeList
        FixedSizeListType(Int(t.listSize))
    elseif t isa Meta.Struct
        StructType()
    elseif t isa Meta.Map
        MapType(something(t.keysSorted, false))
    elseif t isa Meta.Timestamp
        timezone = t.timezone
        TimestampType(timeunit(t.unit), timezone === nothing ? nothing : String(timezone))
    elseif t isa Meta.Date
        DateType(t.unit == Meta.DateUnit.DAY ? AC.DAY : AC.MILLISECOND_DATE)
    elseif t isa Meta.Time
        TimeType(timeunit(t.unit), Int(t.bitWidth))
    elseif t isa Meta.Duration
        DurationType(timeunit(t.unit))
    elseif t isa Meta.Decimal
        DecimalType(Int(t.precision), Int(t.scale), Int(t.bitWidth))
    elseif t isa Meta.Interval
        IntervalType(t.unit == Meta.IntervalUnit.YEAR_MONTH ? AC.YEAR_MONTH :
            t.unit == Meta.IntervalUnit.DAY_TIME ? AC.DAY_TIME : AC.MONTH_DAY_NANO)
    elseif t isa Meta.Utf8View
        ViewType(true)
    elseif t isa Meta.BinaryView
        ViewType(false)
    elseif t isa Meta.ListView
        ListViewType(false)
    elseif t isa Meta.LargeListView
        ListViewType(true)
    elseif t isa Meta.RunEndEncoded
        RunEndEncodedType()
    elseif t isa Meta.Null
        NullType()
    else
        throw(ValidationError("IPC adapter does not map metadata type $(typeof(t))"))
    end
end

"""
Map one metadata type to a Core descriptor, with the built child Fields in
hand — Union is the one type whose descriptor (mode + type ids) spans the
type table AND the children vector, so it cannot go through `coretype`.
"""
function _coremetatype(mt, children::Vector{Field})::ArrowType
    mt isa Meta.Union || return coretype(mt)
    mode = mt.mode == Meta.UnionMode.Dense ? AC.DenseMode : AC.SparseMode
    ids = mt.typeIds
    nchildren = length(children)
    nchildren <= 128 ||
        throw(ValidationError("a union cannot have more than 128 children"))
    if ids === nothing
        return UnionType(mode, Int8[Int8(i) for i = 0:(nchildren - 1)])
    end
    length(ids) == nchildren ||
        throw(ValidationError("union type-id count must equal child count"))
    all(x -> 0 <= x <= 127, ids) ||
        throw(ValidationError("union type ids must be in [0, 127]"))
    coreids = Int8[Int8(x) for x in ids]
    length(unique(coreids)) == length(coreids) ||
        throw(ValidationError("union type ids must be unique"))
    return UnionType(mode, coreids)
end

timeunit(u) = u == Meta.TimeUnit.SECOND ? AC.SECOND :
    u == Meta.TimeUnit.MILLISECOND ? AC.MILLISECOND :
    u == Meta.TimeUnit.MICROSECOND ? AC.MICROSECOND : AC.NANOSECOND

function coremetadata(kvs)
    kvs === nothing && return nothing
    return Dict(String(kv.key) => String(something(kv.value, "")) for kv in kvs)
end

_containsdictionary(f::Field) =
    f.type isa DictionaryType || any(_containsdictionary, f.children)

"""
Convert a metadata Field to a Core Field. Dictionary-encoded fields become
`DictionaryType` here; the IPC dictionary id is recorded in the adapter's
side table (`dictids`), NOT on the Core field — Core never learns about ids.
"""
function corefield(f::Meta.Field, dictids::Dict{Int64,Meta.Field},
    fielddictids::IdDict{Field,Int64})
    children = Field[corefield(c, dictids, fielddictids)
                     for c in something(f.children, Meta.Field[])]
    t = _coremetatype(f.type, children)
    if f.dictionary === nothing
        return Field(String(something(f.name, "")), t, f.nullable,
            coremetadata(f.custom_metadata), children)
    end
    # Nested dictionary encoding (a dictionary field whose VALUE type has
    # dictionary-encoded children) is spec-legal and present in the
    # arrow-testing gold corpus (nested_dictionary: dict(list(dict(utf8)))).
    # A dictionary batch's values decode through the same `decodefield`
    # with the live pool table, so inner pools resolve as long as batches
    # arrive in dependency order — which the IPC spec requires.
    dictids[f.dictionary.id] = f
    idxt = f.dictionary.indexType === nothing ? IntType(32, true) :
        coretype(f.dictionary.indexType)::IntType
    cf = Field(String(something(f.name, "")), DictionaryType(idxt, t, f.dictionary.isOrdered),
        f.nullable, coremetadata(f.custom_metadata), children)
    # Identity-keyed: safe for duplicate column names and nested dict fields
    # (name matching would be neither).
    fielddictids[cf] = f.dictionary.id
    return cf
end

function validatedictionaryids(fields, fielddictids::IdDict{Field,Int64})
    seen = Dict{Int64,Field}()
    compatible(a::Field, b::Field; compare_name::Bool=false) =
        (!compare_name || a.name == b.name) &&
        AC.typeequal(a.type, b.type) && a.nullable == b.nullable &&
        # One id resolves ONE pool, so repeated ids must agree on the whole
        # nested dictionary-id topology: compatible value schemas whose
        # nested fields carry DIFFERENT wire ids would decode the second
        # field through pools its schema never declared.
        (!(a.type isa DictionaryType) || fielddictids[a] == fielddictids[b]) &&
        length(a.children) == length(b.children) &&
        all(compatible(x, y; compare_name=true)
            for (x, y) in zip(a.children, b.children))
    function walk(f::Field)
        if f.type isa DictionaryType
            id = fielddictids[f]
            vf = AC.dictvaluefield(f, f.type)
            if haskey(seen, id)
                old = seen[id]
                compatible(old, vf) ||
                    throw(ValidationError("dictionary id $id is shared by incompatible value schemas"))
            else
                seen[id] = vf
                # A pool's value schema may itself hold dictionary-encoded
                # fields (nested dictionary encoding); their ids resolve
                # through this same table, so register them too.
                walk(vf)
            end
            return
        end
        foreach(walk, f.children)
    end
    foreach(walk, fields)
    return seen
end

function validateschemafield(f::Field)
    AC._validate_descriptor(f.type)
    if f.type isa DictionaryType
        validateschemafield(AC.dictvaluefield(f, f.type))
        return f
    end
    spec = layoutspec(f.type)
    expected = spec.childcount == -1 ? length(f.children) : spec.childcount
    length(f.children) == expected ||
        throw(ValidationError("$(typeof(f.type)) schema expects $expected children, got $(length(f.children))"))
    if f.type isa UnionType
        length(f.type.typeids) == length(f.children) ||
            throw(ValidationError("union type-id count must equal child count"))
        length(unique(f.type.typeids)) == length(f.type.typeids) ||
            throw(ValidationError("union type ids must be unique"))
        all(>=(0), f.type.typeids) ||
            throw(ValidationError("union type ids must be in [0, 127]"))
    elseif f.type isa MapType
        entries = f.children[1]
        entries.type isa StructType && !entries.nullable &&
            length(entries.children) == 2 && !entries.children[1].nullable ||
            throw(ValidationError("invalid map entries/key schema"))
    elseif f.type isa RunEndEncodedType
        length(f.children) == 2 || throw(ValidationError("REE requires two children"))
        run, values = f.children
        run.name == "run_ends" && values.name == "values" &&
            run.type isa IntType && run.type.signed && run.type.bits in (16, 32, 64) &&
            !run.nullable ||
            throw(ValidationError("invalid run-end encoded schema"))
    end
    foreach(validateschemafield, f.children)
    return f
end

# ---------------------------------------------------------------------------
# THE generic decoder: registry-driven node/buffer consumption
# ---------------------------------------------------------------------------

# Node/buffer consumption order falls out of `layoutspec`: one field = one
# node (unless the layout says otherwise) + the registry's buffers in
# registry order + children in declared order. Nothing threads
# (nodeidx, bufferidx, varbufferidx) by hand per layout, so an off-by-one
# cannot silently shift every subsequent buffer; a mismatch is a thrown
# error at the *end* of the batch (leftover nodes/buffers), not corruption.

# Buffer compression: one codec context per reader, reused across buffers
# and explicitly finalized when the reader is done — no global pools.
const CODEC_NONE = Int8(-1)
const CODEC_LZ4_FRAME = Int8(0)   # Meta.CompressionType.LZ4_FRAME
const CODEC_ZSTD = Int8(1)        # Meta.CompressionType.ZSTD

mutable struct DecodeState
    lz4::Ptr{CLZ4.LZ4F_dctx}
    zstd::Ptr{ZSTD.ZSTD_DCtx}
    budget::AllocationBudget
end

DecodeState(budget::AllocationBudget) = DecodeState(
    Ptr{CLZ4.LZ4F_dctx}(C_NULL), Ptr{ZSTD.ZSTD_DCtx}(C_NULL), budget)

function _lz4ctx!(state::DecodeState)
    state.lz4 != C_NULL && return state.lz4
    slot = Ref{Ptr{CLZ4.LZ4F_dctx}}(C_NULL)
    CLZ4.LZ4F_createDecompressionContext(slot, CLZ4.LZ4F_getVersion())
    state.lz4 = slot[]
    return state.lz4
end

function _zstdctx!(state::DecodeState)
    state.zstd != C_NULL && return state.zstd
    p = ZSTD.ZSTD_createDCtx()
    p == C_NULL && throw(OutOfMemoryError())
    state.zstd = p
    return p
end

function Base.close(state::DecodeState)
    lz4 = state.lz4
    state.lz4 = Ptr{CLZ4.LZ4F_dctx}(C_NULL)
    try
        lz4 == C_NULL || CLZ4.LZ4F_freeDecompressionContext(lz4)
    finally
        zstd = state.zstd
        state.zstd = Ptr{ZSTD.ZSTD_DCtx}(C_NULL)
        zstd == C_NULL || ZSTD.ZSTD_freeDCtx(zstd)
    end
    return nothing
end

function _decode_lz4!(state::DecodeState, src::Ptr{UInt8}, srclen::Int64,
    out::Vector{UInt8}, declared::Int64)
    ctx = _lz4ctx!(state)
    CLZ4.LZ4F_resetDecompressionContext(ctx)
    inpos = Int64(0)
    outpos = Int64(0)
    while true
        insize = Ref{Csize_t}(Csize_t(srclen - inpos))
        outsize = Ref{Csize_t}(Csize_t(declared - outpos))
        # A zero-capacity destination is valid. It lets the decoder consume
        # an empty frame or the footer after the last output byte without a
        # second allocation.
        dst = outpos == declared ? Ptr{UInt8}(C_NULL) : pointer(out) + outpos
        hint = CLZ4.LZ4F_decompress(ctx, dst, outsize,
            src + inpos, insize, C_NULL)
        inpos += Int64(insize[])
        outpos += Int64(outsize[])
        if hint == 0
            inpos == srclen || throw(ValidationError(
                "LZ4 buffer contains trailing bytes or multiple frames"))
            outpos == declared || throw(ValidationError(
                "LZ4 output length $outpos does not match declared $declared"))
            return nothing
        end
        inpos < srclen || throw(ValidationError("truncated LZ4 frame"))
        (insize[] != 0 || outsize[] != 0) || throw(ValidationError(
            "LZ4 output exceeds declared length $declared"))
    end
end

function _decode_zstd!(state::DecodeState, src::Ptr{UInt8}, srclen::Int64,
    out::Vector{UInt8}, declared::Int64)
    dst = declared == 0 ? Ptr{UInt8}(C_NULL) : pointer(out)
    got = ZSTD.ZSTD_decompressDCtx(_zstdctx!(state), dst, Csize_t(declared),
        src, Csize_t(srclen))
    if ZSTD.ZSTD_isError(got) != 0
        msg = unsafe_string(ZSTD.ZSTD_getErrorName(got))
        throw(ValidationError("ZSTD decompression failed: $msg"))
    end
    Int64(got) == declared || throw(ValidationError(
        "ZSTD output length $(Int64(got)) does not match declared $declared"))
    return nothing
end

# `B` is the body representation: a contiguous `BufferSlice` for in-memory
# and mmapped messages, or a sparse body (scan_ranges.jl) whose fetched
# spans stand in for the contiguous message body. `_bodyslice` is the one
# seam between them; the parameter keeps the cursor concrete per use.
mutable struct DecodeCursor{B}
    nodes::AbstractVector{Meta.FieldNode}
    buffers::AbstractVector{Meta.Buffer}
    body::B
    max_buffer_bytes::Int64
    max_array_length::Int64
    nodeidx::Int
    bufidx::Int
    last_nonempty_end::Int64
    codec::Int8                   # CODEC_NONE, or the batch's declared codec
    state::Union{Nothing,DecodeState}
    # One entry per view-typed field in depth-first schema order: how many
    # variadic data buffers that field consumes (format 1.4). Non-view
    # batches carry an empty vector; a leftover entry is a skew error.
    variadics::AbstractVector{<:Integer}
    varidx::Int
end

"Resolve one declared buffer window against the message body."
_bodyslice(body::BufferSlice, offset::Int64, len::Int64) =
    AC.subslice(body, offset, len)

DecodeCursor(nodes, buffers, body, limits::Limits;
    codec::Int8=CODEC_NONE, state::Union{Nothing,DecodeState}=nothing,
    variadics=nothing) =
    DecodeCursor(something(nodes, Meta.FieldNode[]),
        something(buffers, Meta.Buffer[]), body,
        limits.max_buffer_bytes, limits.max_array_length, 1, 1, 0,
        codec, state, something(variadics, Int64[]), 1)

"""
    variadiccounts(rb::Meta.RecordBatch) -> Vector{Int64}

The batch's `variadicBufferCounts` as a concrete `Vector{Int64}` (empty when
the slot is absent). The generated binding reads the spec's `[long]` at
8-byte width; this accessor exists so every site shares one normalized shape.
"""
variadiccounts(rb::Meta.RecordBatch) =
    collect(Int64, something(rb.variadicBufferCounts, Int64[]))

"One variadic-buffer count, in depth-first view-field order (format 1.4)."
function takevariadic!(c::DecodeCursor)
    c.varidx <= length(c.variadics) ||
        throw(ValidationError("metadata declares fewer variadic buffer counts than the schema requires"))
    n = c.variadics[c.varidx]
    c.varidx += 1
    0 <= n <= length(c.buffers) ||
        throw(ValidationError("variadic buffer count $n outside [0, $(length(c.buffers))]"))
    return Int(n)
end

function takenode!(c::DecodeCursor)
    c.nodeidx <= length(c.nodes) ||
        throw(ValidationError("metadata declares fewer field nodes than the schema requires"))
    n = c.nodes[c.nodeidx]
    c.nodeidx += 1
    0 <= n.length <= c.max_array_length ||
        throw(ValidationError("field-node length $(n.length) exceeds limit"))
    0 <= n.null_count <= n.length ||
        throw(ValidationError("invalid field-node null count $(n.null_count)"))
    return n
end

"""
Consume one buffer-table entry's METADATA: bounds, alignment, limits, and
the non-overlap/monotone invariants — everything checkable without touching
a single body byte. `takebuffer!` adds the body subslice (+ decompression);
`skipbuffer!` stops here, which lets scan pushdown avoid decoding a column or
planning its body range. Ranged tail reads and coalescing may still over-read
those bytes.
"""
function _buffermeta!(c::DecodeCursor)
    c.bufidx <= length(c.buffers) ||
        throw(ValidationError("metadata declares fewer buffers than the schema requires"))
    b = c.buffers[c.bufidx]
    c.bufidx += 1
    offset = Int64(b.offset)
    len = Int64(b.length)
    offset >= 0 || throw(ValidationError("negative batch buffer offset $offset"))
    offset % 8 == 0 ||
        throw(ValidationError("batch buffer offset $offset is not 8-byte aligned"))
    0 <= len <= c.max_buffer_bytes ||
        throw(ValidationError("batch buffer length $len exceeds limit"))
    if len > 0
        offset >= c.last_nonempty_end ||
            throw(ValidationError("batch buffers overlap or move backwards"))
        c.last_nonempty_end = try
            AC.checked_add(offset, len)
        catch e
            e isa OverflowError || rethrow()
            throw(ValidationError("batch buffer end overflows"))
        end
    end
    return offset, len
end

skipbuffer!(c::DecodeCursor) = (_buffermeta!(c); nothing)

function takebuffer!(c::DecodeCursor)
    offset, len = _buffermeta!(c)
    # THE checked-subslice step: a buffer is only ever a window into this
    # message's body span (or, for a sparse body, into a fetched span that
    # was itself derived from this buffer table). Checked arithmetic turns a
    # corrupt offset/length into a clean ValidationError.
    wire = try
        _bodyslice(c.body, offset, len)
    catch e
        e isa ArgumentError || e isa OverflowError || rethrow()
        throw(ValidationError("batch buffer [$offset, $len] escapes its message body"))
    end
    (c.codec == CODEC_NONE || len == 0) && return wire
    return _decompressbuffer!(c, wire)
end

"""
Decode one compressed buffer per the spec: an Int64 uncompressed-length
prefix, then the compressed payload; a prefix of -1 means the payload is
stored uncompressed. Every declared size is bounded BEFORE allocation (this
prefix is attacker-controlled), the decompressed size must match the
declaration exactly, and each decompressed buffer becomes its own exact-sized owned
region — the wire mapping is never the backing store of decompressed data.
"""
function _decompressbuffer!(c::DecodeCursor, wire::BufferSlice)
    wire.len >= 8 ||
        throw(ValidationError("compressed buffer of $(wire.len) bytes lacks its length prefix"))
    declared = AC.loadat(wire, Int64, Int64(0))
    declared == -1 && return AC.subslice(wire, 8, wire.len - 8)  # stored raw
    0 <= declared <= c.max_buffer_bytes ||
        throw(ValidationError("declared decompressed length $declared exceeds the buffer limit"))
    declared <= typemax(Int) ||
        throw(ValidationError("declared decompressed buffer is not addressable"))
    payloadlen = wire.len - 8
    payloadlen > 0 || throw(ValidationError("compressed buffer has an empty payload"))
    state = c.state::DecodeState
    _charge!(state.budget, declared, "decompressed bytes")
    committed = false
    try
        # This is the only output allocation. Its size was checked and
        # charged before either native decoder sees the input frame.
        out = Vector{UInt8}(undef, Int(declared))
        # The native decoders read through a raw pointer, so the wire
        # region's root must stay reachable for the whole call (Core rule 2).
        wireregion = wire.region::OwnerRegion
        GC.@preserve out wireregion begin
            src = AC.sliceptr(wire) + 8
            if c.codec == CODEC_LZ4_FRAME
                _decode_lz4!(state, src, payloadlen, out, declared)
            else
                _decode_zstd!(state, src, payloadlen, out, declared)
            end
        end
        result = BufferSlice(heapregion(out), 0, declared)
        committed = true
        return result
    catch e
        e isa Union{ValidationError,AllocationLimitError} && rethrow()
        e isa OutOfMemoryError && rethrow()
        e isa InterruptException && rethrow()
        throw(ValidationError("buffer decompression failed: $(sprint(showerror, e))"))
    finally
        committed || (state.budget.left += declared)
    end
end

function finishcursor!(c::DecodeCursor)
    c.nodeidx == length(c.nodes) + 1 ||
        throw(ValidationError("unconsumed field nodes: schema/batch mismatch"))
    c.bufidx == length(c.buffers) + 1 ||
        throw(ValidationError("unconsumed buffers: schema/batch mismatch"))
    c.varidx == length(c.variadics) + 1 ||
        throw(ValidationError("unconsumed variadic buffer counts: schema/batch mismatch"))
    return nothing
end

function missingdicts(fields, nodes, dicts::Dict{Int64,ArrayData},
    fielddictids::IdDict{Field,Int64})
    ns = something(nodes, Meta.FieldNode[])
    idx = Ref(1)
    missing = Set{Int64}()
    function walk(f::Field)
        idx[] <= length(ns) ||
            throw(ValidationError("metadata declares fewer field nodes than the schema requires"))
        node = ns[idx[]]
        idx[] += 1
        if f.type isa DictionaryType
            id = fielddictids[f]
            if !haskey(dicts, id)
                node.length >= 0 && node.null_count == node.length ||
                    throw(ValidationError("record batch uses undefined dictionary id $id for a non-null slot"))
                push!(missing, id)
            end
            return
        end
        spec = layoutspec(f.type)
        nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
        for i = 1:nchildren
            walk(f.children[i])
        end
    end
    foreach(walk, fields)
    return missing
end

"""
    decodefield(field, cursor, dictionaries) -> ArrayData

Generic over the mapped, non-variadic layouts in this adapter.
Dictionary-encoded columns consume the INDEX layout's buffers (validity +
indices) and resolve their values through the adapter's dictionary table.
"""
function decodefield(f::Field, c::DecodeCursor, dicts::Dict{Int64,ArrayData},
    fielddictids::IdDict{Field,Int64})
    t = f.type
    node = takenode!(c)
    spec = layoutspec(t)
    buffers = BufferSlice[takebuffer!(c) for _ in spec.buffers]
    if spec.variadic
        # View layouts append their declared count of variadic data buffers
        # after the fixed validity/views pair (format 1.4).
        for _ = 1:takevariadic!(c)
            push!(buffers, takebuffer!(c))
        end
    end
    for (role, buffer) in zip(spec.buffers, buffers)
        # A zero-length array may omit its offsets buffer entirely — Core
        # accepts that canonical empty form, and nanoarrow and C++ write it
        # (the oracle suite caught us refusing nanoarrow's bytes). A PARTIAL
        # offsets buffer — nonempty but short of one slot — is still
        # malformed framing.
        if role == AC.OFFSETS && node.length == 0 &&
            0 < buffer.len < spec.offsetwidth
            throw(ValidationError(
                "IPC offsets buffer is shorter than one offset slot"))
        end
    end
    children = ArrayData[]
    if t isa DictionaryType
        # Index buffers were just consumed; values come from the side table.
        id = fielddictids[f]
        haskey(dicts, id) ||
            throw(ValidationError("record batch references dictionary id $id before its dictionary batch"))
        return ArrayData(t, node.length, buffers; dictionary=dicts[id],
            nullcount=node.null_count)
    end
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        push!(children, decodefield(f.children[i], c, dicts, fielddictids))
    end
    if t isa UnionType && t.mode == AC.SparseMode
        all(child -> child.len == node.length, children) ||
            throw(ValidationError(
                "IPC sparse-union children must equal the union length"))
    end
    return ArrayData(t, node.length, buffers; children=children,
        nullcount=node.null_count)
end

function validaterecordcolumns(fields, cols,
    validated_dictionaries::AC._ValidatedDictionaries)
    for (f, col) in zip(fields, cols)
        # One IPC dictionary id can back many fields. Compatible value
        # schemas were proved when the stream schema was built. Each immutable
        # pool snapshot is fully certified at its DictionaryBatch, so record
        # validation skips that pool tree. Index contracts still run
        # independently for every field.
        AC._validate_semantic(f, col, validated_dictionaries)
    end
    return validated_dictionaries
end

function decoderecord(fm::FramedMessage, fields, sch::Schema,
    dicts::Dict{Int64,ArrayData}, fielddictids::IdDict{Field,Int64},
    limits::Limits, validated_dictionaries, state::DecodeState)
    header = fm.msg.header::Meta.RecordBatch
    codec = _batchcodec(header.compression, fm.version)
    rblen = something(header.length, Int64(0))
    0 <= rblen <= limits.max_array_length ||
        throw(ValidationError("record batch length $rblen exceeds limit"))
    cursor = DecodeCursor(header.nodes, header.buffers, fm.body, limits;
        codec=codec, state=state, variadics=variadiccounts(header))
    cols = ArrayData[decodefield(f, cursor, dicts, fielddictids) for f in fields]
    finishcursor!(cursor)
    validaterecordcolumns(fields, cols, validated_dictionaries)
    all(col -> col.len == rblen, cols) ||
        throw(ValidationError("RecordBatch length does not match top-level field nodes"))
    return AC.RecordBatch(sch, cols, rblen, validated_dictionaries)
end

function rejectexperimentalcompression(msg::Meta.Message, version::Int16,
    header_type::UInt8)
    version == Int16(3) || return nothing # V4
    header_type in (UInt8(2), UInt8(3)) || return nothing
    metadata = msg.custom_metadata
    metadata === nothing && return nothing
    any(kv -> kv.key == EXPERIMENTAL_COMPRESSION_KEY, metadata) &&
        throw(ValidationError(
            "pre-1.0 experimental V4 IPC compression (the " *
            "ARROW:experimental_compression metadata convention, superseded " *
            "by V5 BodyCompression in 2020) is not supported"))
    return nothing
end
rejectexperimentalcompression(fm::FramedMessage) =
    rejectexperimentalcompression(fm.msg, fm.version, fm.header_type)

# ---------------------------------------------------------------------------
# Stream reader: RecordBatchSource over framed messages
# ---------------------------------------------------------------------------

mutable struct IPCStream <: AC.RecordBatchSource
    schema::Schema
    corefields::AC.FrozenVector{Field}
    batches::Vector{AC.RecordBatch}
    nextindex::Int
    @atomic pulling::Bool
    fielddictids::IdDict{Field,Int64}   # adapter-side id table (shared ids preserved)
end
IPCStream(sch, fields, batches, nextindex, pulling) =
    IPCStream(sch, fields, batches, nextindex, pulling, IdDict{Field,Int64}())

mutable struct PendingRecord
    fm::FramedMessage
    dictionaries::Dict{Int64,ArrayData}
    missing::Set{Int64}
    slot::Int
end
AC.schema(s::IPCStream) = s.schema

function AC.nextbatch!(s::IPCStream)
    # One active pull at a time: the claim CAS rejects concurrent callers
    # (fail closed, no duplicated or skipped batches) and is released on
    # every exit path.
    _, ok = @atomicreplace s.pulling false => true
    ok || throw(Base.ConcurrencyViolationError(
        "IPCStream supports only one active nextbatch! call"))
    try
        i = s.nextindex
        i > length(s.batches) && return nothing
        b = s.batches[i]
        s.nextindex = i + 1
        return b
    finally
        @atomic :release s.pulling = false
    end
end

"""
Map a batch's declared BodyCompression to a codec id, enforcing the spec
subset this adapter supports: BUFFER-method LZ4_FRAME or ZSTD.
"""
function _batchcodec(compression, version::Int16)::Int8
    compression === nothing && return CODEC_NONE
    version == Int16(4) || throw(ValidationError(
        "BodyCompression requires metadata V5"))
    method = something(compression.method, Meta.BodyCompressionMethod.BUFFER)
    method == Meta.BodyCompressionMethod.BUFFER ||
        throw(ValidationError("unsupported body-compression method $method"))
    codec = something(compression.codec, Meta.CompressionType.LZ4_FRAME)
    codec == Meta.CompressionType.LZ4_FRAME && return CODEC_LZ4_FRAME
    codec == Meta.CompressionType.ZSTD && return CODEC_ZSTD
    throw(ValidationError("unsupported compression codec $codec"))
end

"""
    readstream(bytes; limits=Limits()) -> IPCStream

Decode a stream from a borrowed byte vector. Raw batch buffers remain
zero-copy views of `bytes`; positively compressed buffers become exact-sized
owned copies. The caller must not mutate or resize `bytes` until the returned
stream and all batches from it are unreachable (the facade's `Arrow.Table`
and `Arrow.Stream` own their backing storage and do not expose this borrow).
`IPCStream` is a single-owner cursor; overlapping `nextbatch!` calls throw
`ConcurrencyViolationError`.
"""
readstream(bytes::Vector{UInt8}; limits::Limits=Limits()) =
    _readstream(bytes, limits, AllocationBudget(limits.max_total_allocated_bytes))

function _readstream(bytes::Vector{UInt8}, limits::Limits, budget::AllocationBudget)
    region = heapregion(bytes)
    msgs = _framemessages(region, limits, Base.ENDIAN_BOM, budget)
    isempty(msgs) && throw(ValidationError("empty IPC stream"))
    first(msgs).header_type == 1 ||
        throw(ValidationError("first IPC message must be a schema"))
    msgs[1].msg.header isa Meta.Schema ||
        throw(ValidationError("first IPC message must be a schema"))
    metaschema = msgs[1].msg.header
    msgs[1].body.len == 0 ||
        throw(ValidationError("schema message must have an empty body"))
    endian = something(metaschema.endianness, Meta.Endianness.Little)
    endian == Meta.Endianness.Little ||
        throw(ValidationError("big-endian IPC is not supported (no endianness normalization)"))
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()   # adapter-side id table
    fields = Field[corefield(f, dictids, fielddictids)
                   for f in something(metaschema.fields, Meta.Field[])]
    foreach(validateschemafield, fields)
    dictvaluefields = validatedictionaryids(fields, fielddictids)
    sch = Schema(fields; metadata=coremetadata(metaschema.custom_metadata),
        endianness=AC.LittleEndian)
    dicts = Dict{Int64,ArrayData}()
    # One codec context per reader, shared by every compressed batch in the
    # stream and explicitly finalized on every exit path.
    state = DecodeState(budget)
    validated_dictionaries = AC._ValidatedDictionaries()
    batchslots = Union{Nothing,AC.RecordBatch}[]
    pending = PendingRecord[]
    features = Set(msgs[1].features)
    schemaversion = msgs[1].version
    try
        for fm in msgs[2:end]
        fm.version == schemaversion ||
            throw(ValidationError("IPC metadata version changes within the stream"))
        # Arrow 0.17 V4 streams signaled buffer compression on the Message,
        # before RecordBatch.compression existed. Reject that legacy marker
        # before treating its length-prefixed compressed buffers as raw data.
        rejectexperimentalcompression(fm)
        header = fm.msg.header
        if header isa Meta.DictionaryBatch
            header.isDelta &&
                throw(ValidationError("delta dictionaries are not supported"))
            rb = header.data
            codec = _batchcodec(rb.compression, fm.version)
            haskey(dictids, header.id) ||
                throw(ValidationError("dictionary batch has unknown id $(header.id)"))
            replacement = haskey(dicts, header.id)
            if replacement && !(1 in features)
                throw(ValidationError("dictionary replacement used without required schema feature"))
            end
            # A dictionary batch's payload is a one-column record batch of
            # the VALUE type; decode it with the same generic decoder. The
            # value field is the metadata field minus its dictionary tag.
            # Dictionary value schemas are built once from the Core schema.
            # Reusing them avoids repeated metadata-string/container
            # allocation on dictionary replacement messages. Pool
            # nullability is independent from the encoded index field.
            haskey(dictvaluefields, header.id) ||
                throw(ValidationError("dictionary batch has unknown id $(header.id)"))
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
            # Certify the entire immutable pool snapshot before publication.
            # Later record validation may then skip every recursive stage for
            # this exact identity. Replacements decode to a new identity and
            # must earn their own certificate here.
            validate_semantic(vf, decoded)
            validated_dictionaries[decoded] = nothing
            dicts[header.id] = decoded

            # The IPC spec permits an all-null dictionary column before its
            # first DictionaryBatch. Resolve only the missing dictionary;
            # preserve every dictionary snapshot already visible at the
            # record's wire position.
            if !replacement
                stillpending = PendingRecord[]
                for p in pending
                    if header.id in p.missing
                        p.dictionaries[header.id] = decoded
                        delete!(p.missing, header.id)
                    end
                    if isempty(p.missing)
                        batchslots[p.slot] = decoderecord(p.fm, fields, sch,
                            p.dictionaries, fielddictids, limits,
                            validated_dictionaries, state)
                    else
                        push!(stillpending, p)
                    end
                end
                pending = stillpending
            end
        elseif header isa Meta.RecordBatch
            missing = missingdicts(fields, header.nodes, dicts, fielddictids)
            push!(batchslots, nothing)
            slot = length(batchslots)
            if isempty(missing)
                batchslots[slot] = decoderecord(fm, fields, sch, dicts,
                    fielddictids, limits, validated_dictionaries, state)
            else
                push!(pending, PendingRecord(fm, copy(dicts), missing, slot))
            end
        else
            throw(ValidationError("unsupported IPC message header $(typeof(header))"))
        end
    end
        isempty(pending) ||
            throw(ValidationError("stream ended before required dictionary batches arrived"))
        batches = AC.RecordBatch[b::AC.RecordBatch for b in batchslots]
        return IPCStream(sch, AC.FrozenVector{Field}(fields), batches, 1, false,
            fielddictids)
    finally
        close(state)
    end
end
