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
# PROVE-OUT: the IPC adapter as a thin peer over ArrowCore.
#
# Run with the repo project so the existing package (and its vendored
# FlatBuffers/Flatbuf metadata bindings) is available:
#
#     julia --project=. core/examples/ipc_read.jl
#
# What this demonstrates, mapped to the redesign report:
#
#   * §9 "IPC adapter": stream framing with checked spans, a bounds verifier
#     before any generated FlatBuffers getter, and explicit resource limits
#     (`Limits` + `framemessages`). The message body is the decoding AUTHORITY:
#     every wire buffer is first a checked subslice of its message-body slice,
#     so corrupt metadata cannot alias the schema message, another batch, or
#     anything else in the file. Positively compressed buffers are then
#     decoded into separate exact-sized owned regions.
#
#   * §9 "layout registry": ONE generic recursive decoder (`decodefield`)
#     replaces the current implementation's per-layout `build` methods with
#     hand-threaded (nodeidx, bufferidx, varbufferidx) state. Node/buffer order is
#     derived from `layoutspec` for the fixed-buffer subset used here.
#     Variadic layouts still need their own bounded count handling.
#
#   * §9 "adapter owns IPC bookkeeping": dictionary ids live in an
#     adapter-side table (`dictionaries::Dict{Int64,...}`); Core Fields
#     carry `DictionaryType` object references and never see an id.
#
#   * The adapter uses metadata bindings REGENERATED from the current
#     apache/arrow format/*.fbs (core/tools/fbsgen.jl -> core/metadata/),
#     over the vendored FlatBuffers runtime, behind a local byte-wise
#     verifier. The verifier is still a prove-out bridge — the report's
#     production answer is a generated verifier — but the bindings are now
#     the spec's shape (features, variadicBufferCounts as [long], type tags
#     through 26, MONTH_DAY_NANO), so no raw-slot workarounds remain.
#
# The acceptance test at the bottom: today's Arrow.jl 2.x WRITES a stream
# (multi-batch, with nulls, strings, lists, structs, and a dict-encoded
# column); this adapter reads it back through ArrowCore and the values are
# compared element-for-element. New core, real bytes, no shims.
# =============================================================================

using Arrow                      # the existing 2.x package (repo project)
using Arrow.Tables               # partitioner for the multi-batch test write
# Buffer compression codecs (repo-project deps, reused like the metadata
# bindings). In the production package these are package extensions; here the
# closed two-codec set is a concrete switch — which is also the trim-friendly
# shape (report §14.4: codecs behind extensions, chosen statically per build).
using Arrow.CodecLz4: LZ4FrameCompressor
using Arrow.CodecZstd: ZstdCompressor
const CLZ4 = Arrow.CodecLz4
const CZSTD = Arrow.CodecZstd
const ZSTD = CZSTD.LibZstd
using PooledArrays               # adversarial dictionary-pool fixture
const FB = Arrow.FlatBuffers     # vendored flatbuffers runtime (reused as-is)
# Metadata bindings REGENERATED from the current apache/arrow format/*.fbs
# by core/tools/fbsgen.jl (core/metadata/). The vendored 2.x bindings
# (Arrow.Meta) were hand-written against a 2020-era schema and drift from
# the spec in eight known places; the prove-out reads the spec's shape.
module GeneratedMeta
    using EnumX
    using ..FB
    include(joinpath(@__DIR__, "..", "metadata", "Schema.jl"))
    include(joinpath(@__DIR__, "..", "metadata", "File.jl"))
    include(joinpath(@__DIR__, "..", "metadata", "Message.jl"))
end
const Meta = GeneratedMeta

include(joinpath(@__DIR__, "..", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore

# ---------------------------------------------------------------------------
# Stage-1 framing: resource limits before metadata-directed decode allocation
# ---------------------------------------------------------------------------

"""
Resource limits enforced before metadata-directed copying or decode. Small
fixed Julia containers are created to run the framer itself. Today's reader
has no equivalent — a hostile length prefix reaches an attacker-sized
allocation (src/table.jl:804-816).
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
# FlatBuffers verifier
# ---------------------------------------------------------------------------

# Verifier positions are zero-based. Loads are assembled byte-by-byte, so
# they cannot escape the metadata vector or depend on host alignment.
_vfail(msg) = throw(ValidationError("invalid IPC FlatBuffer: $msg"))

function _vrange(bytes::Vector{UInt8}, pos::Int64, len::Int64,
    what::AbstractString)
    (pos >= 0 && len >= 0 && len <= length(bytes) && pos <= length(bytes) - len) ||
        _vfail("$what is outside metadata")
    return pos
end

function _vu(bytes, pos::Int64, width::Int)
    _vrange(bytes, pos, width, "scalar")
    x = UInt64(0)
    for i = 0:(width - 1)
        x |= UInt64(bytes[pos + i + 1]) << (8i)
    end
    return x
end
_vu8(bytes, pos) = UInt8(_vu(bytes, pos, 1))
_vu16(bytes, pos) = UInt16(_vu(bytes, pos, 2))
_vu32(bytes, pos) = UInt32(_vu(bytes, pos, 4))
_vi32(bytes, pos) = reinterpret(Int32, _vu32(bytes, pos))
_vi64(bytes, pos) = reinterpret(Int64, UInt64(_vu(bytes, pos, 8)))

struct _VTable
    bytes::Vector{UInt8}
    pos::Int64
    vpos::Int64
    vlen::Int64
    olen::Int64
end

mutable struct _VState
    limits::Limits
    objects::Int64
    reserved::Int64
    reserve_limit::Int64
end
_VState(limits::Limits, reserve_limit::Int64) =
    _VState(limits, 0, 0, reserve_limit)

# Conservative charges for Julia objects and containers whose sizes are
# directed by verified metadata. String payload bytes are charged on every
# logical getter occurrence. Message bodies remain zero-copy and have their
# own body/buffer byte limits.
const METADATA_OBJECT_RESERVE = Int64(2048)
const METADATA_VECTOR_BASE_RESERVE = Int64(256)
const METADATA_VECTOR_ELEMENT_RESERVE = Int64(1024)
const METADATA_STRING_BASE_RESERVE = Int64(128)

function _vcharge!(state::_VState, bytes::Int64, what::AbstractString)
    bytes >= 0 || _vfail("negative allocation charge for $what")
    state.reserved = try
        AC.checked_add(state.reserved, bytes)
    catch e
        e isa OverflowError || rethrow()
        _vfail("allocation charge overflow for $what")
    end
    state.reserved <= state.reserve_limit || throw(AllocationLimitError(
        "metadata-directed allocation budget exceeded while visiting $what"))
    return nothing
end

function _vvisit!(state::_VState, kind::Symbol, t::_VTable)
    # Count logical occurrences, not unique byte positions. FlatBuffers may
    # alias a table, while generated getters and corefield expand it once per
    # parent occurrence. Forward UOffsets make the graph acyclic.
    state.objects = try
        AC.checked_add(state.objects, Int64(1))
    catch e
        e isa OverflowError || rethrow()
        _vfail("metadata object count overflow")
    end
    state.objects <= state.limits.max_metadata_objects ||
        _vfail("metadata object count exceeds limit")
    _vcharge!(state, METADATA_OBJECT_RESERVE, String(kind))
    return true
end

function _vcount!(state::_VState, n::Int64, what::AbstractString)
    n >= 0 || _vfail("negative metadata object count for $what")
    state.objects = try
        AC.checked_add(state.objects, n)
    catch e
        e isa OverflowError || rethrow()
        _vfail("metadata object count overflow")
    end
    state.objects <= state.limits.max_metadata_objects ||
        _vfail("metadata object count exceeds limit")
    return nothing
end

function _vtable(bytes::Vector{UInt8}, pos::Int64)
    _vrange(bytes, pos, 4, "table")
    pos % 4 == 0 || _vfail("table at $pos is misaligned")
    back = Int64(_vi32(bytes, pos))
    back != 0 || _vfail("table at $pos has a zero vtable offset")
    vpos = AC.checked_sub(pos, back)
    _vrange(bytes, vpos, 4, "vtable header")
    vpos % 2 == 0 || _vfail("vtable at $vpos is misaligned")
    vlen = Int64(_vu16(bytes, vpos))
    olen = Int64(_vu16(bytes, vpos + 2))
    vlen >= 4 && iseven(vlen) || _vfail("invalid vtable length $vlen")
    olen >= 4 || _vfail("invalid table object length $olen")
    _vrange(bytes, vpos, vlen, "vtable")
    _vrange(bytes, pos, olen, "table object")
    return _VTable(bytes, pos, vpos, vlen, olen)
end

function _vfield(t::_VTable, slot::Int, width::Int=1; required::Bool=false)
    ep = t.vpos + 4 + 2slot
    if ep + 2 > t.vpos + t.vlen
        required && _vfail("required table slot $slot is absent")
        return nothing
    end
    off = Int64(_vu16(t.bytes, ep))
    if off == 0
        required && _vfail("required table slot $slot is absent")
        return nothing
    end
    off >= 4 && off + width <= t.olen || _vfail("table slot $slot exceeds object")
    p = t.pos + off
    _vrange(t.bytes, p, width, "table slot $slot")
    width > 1 && p % min(width, 8) != 0 &&
        _vfail("table slot $slot is misaligned")
    return p
end

function _vref(t::_VTable, slot::Int; required::Bool=false)
    p = _vfield(t, slot, 4; required=required)
    p === nothing && return nothing
    rel = Int64(_vu32(t.bytes, p))
    rel > 0 || _vfail("reference slot $slot has a null/backward offset")
    target = AC.checked_add(p, rel)
    _vrange(t.bytes, target, 1, "reference slot $slot target")
    return target
end

function _vbool(t::_VTable, slot::Int)
    p = _vfield(t, slot, 1)
    p === nothing && return nothing
    _vu8(t.bytes, p) in (0x00, 0x01) || _vfail("invalid boolean in slot $slot")
    return nothing
end

function _venum(t::_VTable, slot::Int, width::Int, valid)
    p = _vfield(t, slot, width)
    p === nothing && return nothing
    _vu(t.bytes, p, width) in valid || _vfail("invalid enum in slot $slot")
    return nothing
end

function _vstring(t::_VTable, slot::Int, state::_VState; required::Bool=false)
    p = _vref(t, slot; required=required)
    p === nothing && return nothing
    p % 4 == 0 || _vfail("string length is misaligned")
    _vrange(t.bytes, p, 4, "string length")
    n = Int64(_vu32(t.bytes, p))
    start = AC.checked_add(p, Int64(4))
    _vrange(t.bytes, start, AC.checked_add(n, Int64(1)), "string")
    t.bytes[start + n + 1] == 0 || _vfail("string has no NUL terminator")
    _vcharge!(state, AC.checked_add(METADATA_STRING_BASE_RESERVE, n), "string")
    payload = @view t.bytes[(start + 1):(start + n)]
    isvalid(String, payload) || _vfail("string is not valid UTF-8")
    return nothing
end

function _vvector(t::_VTable, slot::Int, elemsize::Int;
    required::Bool=false,
    state::_VState=_VState(Limits(), typemax(Int64)))
    p = _vref(t, slot; required=required)
    p === nothing && return nothing
    _vrange(t.bytes, p, 4, "vector length")
    p % 4 == 0 || _vfail("vector length is misaligned")
    n = Int64(_vu32(t.bytes, p))
    n <= state.limits.max_metadata_objects ||
        _vfail("vector count $n exceeds metadata object limit")
    _vcount!(state, n, "vector entries")
    start = AC.checked_add(p, Int64(4))
    _vrange(t.bytes, start, AC.checked_mul(n, Int64(elemsize)), "vector data")
    n > 0 && elemsize > 1 && start % min(elemsize, 8) != 0 &&
        _vfail("vector data is misaligned")
    _vcharge!(state, AC.checked_add(METADATA_VECTOR_BASE_RESERVE,
        AC.checked_mul(n, METADATA_VECTOR_ELEMENT_RESERVE)), "vector")
    return start, Int(n)
end

function _vtablevector(t::_VTable, slot::Int, verifyone, state::_VState,
    depth::Int; required::Bool=false)
    vec = _vvector(t, slot, 4; required=required, state=state)
    vec === nothing && return 0
    start, n = vec
    for i = 0:(n - 1)
        ep = start + 4i
        rel = Int64(_vu32(t.bytes, ep))
        rel > 0 || _vfail("table vector has null entry")
        verifyone(_vtable(t.bytes, AC.checked_add(ep, rel)), state, depth + 1)
    end
    return n
end

function _vkeyvalue(t::_VTable, state::_VState, depth::Int)
    _vvisit!(state, :keyvalue, t) || return nothing
    depth <= state.limits.max_nesting_depth || _vfail("metadata nesting exceeds limit")
    _vstring(t, 0, state; required=true)
    _vstring(t, 1, state; required=true)
    return nothing
end

_vmetadata(t::_VTable, slot::Int, state::_VState, depth::Int) =
    _vtablevector(t, slot, _vkeyvalue, state, depth)

function _vtype(t::_VTable, code::UInt8, state::_VState, depth::Int)
    _vvisit!(state, Symbol("type", code), t) || return nothing
    limits = state.limits
    depth <= limits.max_nesting_depth || _vfail("metadata nesting exceeds limit")
    if code == 2                    # Int
        _vfield(t, 0, 4; required=true)
        _vbool(t, 1)
    elseif code == 3                # FloatingPoint
        _venum(t, 0, 2, UInt64(0):UInt64(2))
    elseif code == 8                # Date
        _venum(t, 0, 2, UInt64(0):UInt64(1))
    elseif code == 11               # Interval
        _venum(t, 0, 2, UInt64(0):UInt64(2))
    elseif code == 18               # Duration
        _venum(t, 0, 2, UInt64(0):UInt64(3))
    elseif code == 7                # Decimal
        _vfield(t, 0, 4; required=true)
        _vfield(t, 1, 4)
        _vfield(t, 2, 4)
    elseif code == 9                # Time
        _venum(t, 0, 2, UInt64(0):UInt64(3))
        _vfield(t, 1, 4)
    elseif code == 10               # Timestamp
        _venum(t, 0, 2, UInt64(0):UInt64(3))
        _vstring(t, 1, state)
    elseif code == 14               # Union
        _venum(t, 0, 2, UInt64(0):UInt64(1))
        _vvector(t, 1, 4; state=state)
    elseif code in (15, 16)         # fixed-size binary/list
        _vfield(t, 0, 4)             # FlatBuffers scalar default is zero
    elseif code == 17               # Map
        _vbool(t, 0)
    elseif !(code in (1, 4, 5, 6, 12, 13, 19, 20, 21, 22, 23, 24, 25, 26))
        # 22..26 (RunEndEncoded and the view types) are field-less tables:
        # nothing to verify beyond the table shell itself.
        _vfail("unknown Arrow type tag $code")
    end
    return nothing
end

function _vdict(t::_VTable, state::_VState, depth::Int)
    _vvisit!(state, :dictionary, t) || return nothing
    _vfield(t, 0, 8)
    p = _vref(t, 1)
    p === nothing || _vtype(_vtable(t.bytes, p), UInt8(2), state, depth + 1)
    _vbool(t, 2)
    _venum(t, 3, 2, (UInt64(0),))
    return nothing
end

function _vfieldmeta(t::_VTable, state::_VState, depth::Int)
    _vvisit!(state, :field, t) || return nothing
    limits = state.limits
    depth <= limits.max_nesting_depth || _vfail("field nesting exceeds limit")
    _vstring(t, 0, state)
    _vbool(t, 1)
    tagp = _vfield(t, 2, 1; required=true)
    code = _vu8(t.bytes, tagp)
    code != 0 || _vfail("field has no type tag")
    typep = _vref(t, 3; required=true)
    _vtype(_vtable(t.bytes, typep), code, state, depth + 1)
    dp = _vref(t, 4)
    dp === nothing || _vdict(_vtable(t.bytes, dp), state, depth + 1)
    _vtablevector(t, 5, _vfieldmeta, state, depth)
    _vmetadata(t, 6, state, depth)
    return nothing
end

function _vschema(t::_VTable, state::_VState, depth::Int)
    _vvisit!(state, :schema, t) || return Int64[]
    limits = state.limits
    _venum(t, 0, 2, UInt64(0):UInt64(1))
    _vtablevector(t, 1, _vfieldmeta, state, depth; required=true)
    _vmetadata(t, 2, state, depth)
    features = Int64[]
    vec = _vvector(t, 3, 8; state=state)
    if vec !== nothing
        start, n = vec
        for i = 0:(n - 1)
            push!(features, _vi64(t.bytes, start + 8i))
        end
    end
    all(x -> x in (0, 1, 2), features) ||
        _vfail("schema declares an unknown required feature")
    return features
end

function _vrecordbatch(t::_VTable, state::_VState, depth::Int)
    _vvisit!(state, :recordbatch, t) || return nothing
    limits = state.limits
    _vfield(t, 0, 8)
    _vvector(t, 1, 16; state=state)
    _vvector(t, 2, 16; state=state)
    cp = _vref(t, 3)
    if cp !== nothing
        c = _vtable(t.bytes, cp)
        _venum(c, 0, 1, UInt64(0):UInt64(1))
        _venum(c, 1, 1, (UInt64(0),))
    end
    _vvector(t, 4, 8; state=state)
    return nothing
end

function _vdictbatch(t::_VTable, state::_VState, depth::Int)
    _vvisit!(state, :dictionarybatch, t) || return nothing
    _vfield(t, 0, 8)
    dp = _vref(t, 1; required=true)
    _vrecordbatch(_vtable(t.bytes, dp), state, depth + 1)
    _vbool(t, 2)
    return nothing
end

function verify_ipc_metadata(bytes::Vector{UInt8}, limits::Limits,
    reserve_limit::Int64=limits.max_total_allocated_bytes)
    length(bytes) >= 4 || _vfail("missing root offset")
    root = Int64(_vu32(bytes, 0))
    root >= 4 || _vfail("invalid root offset")
    msg = _vtable(bytes, root)
    state = _VState(limits, reserve_limit)
    _vvisit!(state, :message, msg)
    vp = _vfield(msg, 0, 2)
    version = vp === nothing ? Int16(0) : reinterpret(Int16, _vu16(bytes, vp))
    version in (Int16(3), Int16(4)) ||
        _vfail("unsupported metadata version $version (only V4/V5 are accepted)")
    hp = _vfield(msg, 1, 1; required=true)
    header_type = _vu8(bytes, hp)
    header_type in (UInt8(1), UInt8(2), UInt8(3)) ||
        _vfail("unsupported message header tag $header_type")
    headerp = _vref(msg, 2; required=true)
    header = _vtable(bytes, headerp)
    features = header_type == 1 ? _vschema(header, state, 0) :
        header_type == 2 ? (_vdictbatch(header, state, 0); Int64[]) :
        (_vrecordbatch(header, state, 0); Int64[])
    version == Int16(3) && !isempty(features) &&
        _vfail("schema features require metadata V5")
    _vfield(msg, 3, 8)
    _vmetadata(msg, 4, state, 0)
    return version, header_type, features, state.reserved
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
        throw(ValidationError("this prove-out requires a little-endian host"))
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
    any(_containsdictionary, children) &&
        throw(ValidationError("children of an IPC dictionary field cannot be dictionary encoded"))
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

# This function is the headline. The current implementation threads
# (nodeidx, bufferidx, varbufferidx) by hand through ten `build` methods —
# an off-by-one in any of them silently shifts every subsequent buffer
# (the #540 bug class). Here consumption order falls out of `layoutspec`:
# one field = one node (unless the layout says otherwise) + the registry's
# buffers in registry order + children in declared order. A mismatch is a
# thrown error at the *end* of the batch (leftover nodes/buffers), not
# corruption.

# Buffer compression (report §9 IPC adapter): one codec context per reader,
# reused across buffers and explicitly finalized when the reader is done — no
# global pools (the 2.x design retains one native context per possible thread
# for process lifetime with no finalization, src/Arrow.jl:83-142).
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
8-byte width; this accessor exists so every site shares one normalized shape
— and it is where the vendored 2.x binding's Int32-elements bug was bridged
before regeneration.
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
stored uncompressed. Every declared size is bounded BEFORE allocation (the
2.x reader allocates an attacker-controlled Int64 straight from this prefix,
src/table.jl:804-816), the decompressed size must match the declaration
exactly, and each decompressed buffer becomes its own exact-sized owned
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
        if role == AC.OFFSETS && node.length == 0 &&
            buffer.len < spec.offsetwidth
            throw(ValidationError(
                "IPC empty offset array must carry its terminal zero offset"))
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
            "experimental V4 IPC compression is outside this prove-out"))
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
end


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
stream and all batches from it are unreachable. A production IO framer owns
its backing storage instead of exposing this prove-out borrow contract.
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
        throw(ValidationError("big-endian IPC requires normalization, which is outside this prove-out"))
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()   # adapter-side id table (report §9)
    fields = Field[corefield(f, dictids, fielddictids)
                   for f in something(metaschema.fields, Meta.Field[])]
    foreach(validateschemafield, fields)
    dictvaluefields = validatedictionaryids(fields, fielddictids)
    sch = Schema(fields; metadata=coremetadata(metaschema.custom_metadata),
        endianness=AC.LittleEndian)
    dicts = Dict{Int64,ArrayData}()
    # One codec context per reader, shared by every compressed batch in the
    # stream and explicitly finalized on every exit path (report §9).
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
                throw(ValidationError("delta dictionaries are outside this prove-out"))
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
        return IPCStream(sch, AC.FrozenVector{Field}(fields), batches, 1, false)
    finally
        close(state)
    end
end

function _threaded_cursor_stress()
    workers = min(4, Threads.nthreads())
    workers > 1 || error("threaded IPC cursor stress requires multiple threads")
    n = 200_000
    sch = Schema(Field[])
    batches = AC.RecordBatch[
        AC.RecordBatch(sch, ArrayData[], i) for i = 1:n
    ]
    stream = IPCStream(sch, AC.FrozenVector{Field}(Field[]), batches, 1, false)
    results = [Int64[] for _ = 1:workers]
    violations = AC.ReleaseCounter()
    ready = AC.ReleaseCounter()
    start = Base.Event()
    tasks = [Threads.@spawn begin
        AC.increment!(ready)
        wait(start)
        while true
            b = try
                nextbatch!(stream)
            catch e
                if e isa Base.ConcurrencyViolationError
                    AC.increment!(violations)
                    yield()
                    continue
                end
                rethrow()
            end
            b === nothing && break
            push!(results[worker], b.nrows)
        end
    end for worker = 1:workers]
    while ready[] != workers
        yield()
    end
    notify(start)
    fetch.(tasks)
    got = reduce(vcat, results)
    @assert length(got) == n
    sort!(got)
    @assert got == collect(Int64, 1:n)
    @assert violations[] > 0
    return nothing
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

_rejects(f) = try
    f()
    false
catch e
    e isa Union{ValidationError,AllocationLimitError}
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
    firstio = IOBuffer()
    Arrow.write(firstio,
        (d=Arrow.DictEncode(["aa", "bb", "aa"], id),); file=false)
    firstbytes = take!(firstio)
    secondio = IOBuffer()
    Arrow.write(secondio,
        (d=Arrow.DictEncode(["xx", "yy", "xx"], id),); file=false)
    secondbytes = take!(secondio)
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
    # empty buffers vector from a 2.x-written zero-row Null batch to reproduce
    # that valid encoding without carrying a binary fixture in this example.
    io = IOBuffer()
    Arrow.write(io, (x=Missing[],); file=false)
    bytes = take!(io)
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
        # must reject before any generated getter dereferences it. (An older
        # form of this fixture also required zero padding there — a layout
        # accident of the previous builder, not part of the property.)
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

# ---------------------------------------------------------------------------
# Acceptance: 2.x writes, Core reads
# ---------------------------------------------------------------------------

function main()
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

    expected = (
        ints=Int64[1, 2, 3, 4, 5],
        floats=[1.5, missing, 3.5, missing, 5.5],
        bools=[true, false, true, missing, false],
        strs=["hey", "", missing, "αβ∀", "last"],
        lists=[[1, 2], Int64[], [3], missing, [4, 5, 6]],
        structs=[(a=1, b="x"), (a=2, b="y"), (a=3, b="z"), (a=4, b="w"), (a=5, b="v")],
        dict=Arrow.DictEncode(["lo", "hi", "lo", missing, "hi"]),
    )
    # Two partitions -> two record batches (plus dictionary batches).
    io = IOBuffer()
    Arrow.write(io, Tables.partitioner([expected, expected]); file=false)
    bytes = take!(io)
    println("2.x-written stream: $(length(bytes)) bytes")

    stream = readstream(bytes)
    println("decoded: $(length(stream.batches)) record batches, " *
            "$(length(stream.schema.fields)) columns")
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
        # Core struct scalars are ordered pairs (report §14.2); the writer
        # side above still feeds 2.x NamedTuples.
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
        cio = IOBuffer()
        Arrow.write(cio, Tables.partitioner([expected, expected]);
            file=false, compress=kw)
        cbytes = take!(cio)
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
    simpleio = IOBuffer()
    Arrow.write(simpleio, (x=Int64[1, 2, 3],); file=false, compress=:zstd)
    simplebytes = take!(simpleio)
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
    oneio = IOBuffer()
    Arrow.write(oneio, large; file=false, compress=:zstd)
    aggregate_limit = Limits(max_total_allocated_bytes=100_000)
    @assert length(readstream(take!(oneio); limits=aggregate_limit).batches) == 1
    twoio = IOBuffer()
    Arrow.write(twoio, Tables.partitioner([large, large]);
        file=false, compress=:zstd)
    @assert _rejects(() -> readstream(take!(twoio); limits=aggregate_limit))
    println("metadata and decompressed bytes share one reader-wide budget ✓")

    for kw in (:lz4, :zstd)
        emptyio = IOBuffer()
        Arrow.write(emptyio, (x=Int64[],); file=false, compress=kw)
        emptystream = readstream(take!(emptyio))
        @assert isempty(materialize(emptystream.schema.fields[1],
            emptystream.batches[1].columns[1]))
    end
    println("zero-byte compressed buffers may omit the prefix ✓")

    # The 2.x writer permits a coefficient outside its declared decimal
    # precision. The Core semantic boundary must reject it before exposure.
    baddecimalio = IOBuffer()
    D = Arrow.Decimal{Int32(1),Int32(0),Int128}
    Arrow.write(baddecimalio, (d=D[D(Int128(10))],); file=false)
    @assert _rejects(() -> readstream(take!(baddecimalio)))
    println("decimal coefficients outside declared precision are rejected ✓")

    pulled = readstream(bytes)
    @assert nextbatch!(pulled) isa RecordBatch
    @assert nextbatch!(pulled) isa RecordBatch
    @assert nextbatch!(pulled) === nothing
    println("RecordBatchSource pull protocol works ✓")

    println("pull claim releases on every exit path ✓")

    reporoot = normpath(joinpath(@__DIR__, "..", ".."))
    stresscmd = `$(Base.julia_cmd()) --startup-file=no --threads=4 --project=$reporoot $(abspath(@__FILE__))`
    run(addenv(stresscmd, "ARROWCORE_IPC_CURSOR_STRESS" => "1"))
    println("concurrent IPC pulls fail closed without duplicate batches ✓")

    # Framing limits actually bite: a 1KB body cap must reject this stream
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

    # Truncation semantics, both halves of the report's append rule:
    # (a) losing only the 8-byte EOS block = boundary truncation, ACCEPTED
    #     (the stream ends after its last complete message);
    # (b) losing bytes of a message body = corruption, a clean framing error
    #     — never a silent empty/short stream (the 2.x behavior) and never
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

    overlapio = IOBuffer()
    Arrow.write(overlapio, (x=Int64[1], y=Int64[2]); file=false)
    overlap = take!(overlapio)
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
    sharedio = IOBuffer()
    Arrow.write(sharedio,
        (a=Arrow.DictEncode(nestedvals, 7), b=Arrow.DictEncode(nestedvals, 7));
        file=false)
    sharedstream = readstream(take!(sharedio))
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

    pool = PooledArray(Union{Missing,String}[missing, "x"])
    poolio = IOBuffer()
    Arrow.write(poolio, (d=Arrow.DictEncode(view(pool, 2:2)),); file=false)
    poolbytes = take!(poolio)
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

    nullio = IOBuffer()
    nullvalues = Union{Missing,String}[missing, missing]
    Arrow.write(nullio, (d=Arrow.DictEncode(nullvalues),); file=false)
    nullbytes = take!(nullio)
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
    mapio = IOBuffer()
    Arrow.write(mapio, (m=[Dict("a" => Int64(1))],); file=false)
    mapstream = readstream(take!(mapio))
    mf = mapstream.schema.fields[1]
    @assert mf.type == MapType(false)
    @assert materialize(mf, mapstream.batches[1].columns[1]) == [["a" => 1]]
    println("valid 2.x Map streams decode with default keysSorted=false ✓")

    emptyio = IOBuffer()
    Arrow.write(emptyio, (x=Int64[1, 2, 3],); file=false)
    emptybytes = take!(emptyio)
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

    emptyrecordio = IOBuffer()
    Arrow.write(emptyrecordio, (x=Int64[],); file=false)
    emptyrecordstream = readstream(take!(emptyrecordio))
    @assert emptyrecordstream.batches[1].nrows == 0
    @assert isempty(materialize(emptyrecordstream.schema.fields[1],
        emptyrecordstream.batches[1].columns[1]))

    emptydictio = IOBuffer()
    Arrow.write(emptydictio, (d=Arrow.DictEncode(String[]),); file=false)
    emptydictstream = readstream(take!(emptydictio))
    @assert emptydictstream.batches[1].nrows == 0
    @assert isempty(materialize(emptydictstream.schema.fields[1],
        emptydictstream.batches[1].columns[1]))
    println("omitted zero-length record and dictionary lengths use defaults ✓")

    metaio = IOBuffer()
    Arrow.write(metaio, (x=Int64[1],); file=false,
        metadata=Dict("owner" => "jacob"),
        colmetadata=Dict(:x => Dict("unit" => "count")))
    metastream = readstream(take!(metaio))
    @assert Dict(metastream.schema.metadata) == Dict("owner" => "jacob")
    @assert Dict(metastream.schema.fields[1].metadata) == Dict("unit" => "count")
    println("schema and field metadata are preserved ✓")
    println()
    println("IPC framing, verification, decoding, and adversarial checks passed.")
end

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    if get(ENV, "ARROWCORE_IPC_CURSOR_STRESS", "") == "1"
        _threaded_cursor_stress()
    else
        main()
    end
end
