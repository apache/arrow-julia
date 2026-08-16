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
# FlatBuffers verifier RUNTIME — the schema-independent primitives the
# GENERATED walkers (Verifier.jl, emitted by tools/fbsgen.jl) call.
# Hand-maintained, but deliberately schema-blind: every fact about which
# tables have which fields lives in the generated file, so schema drift can
# never hide here. Positions are zero-based; loads are assembled
# byte-by-byte, so they cannot escape the verified byte vector or depend on
# host alignment.
#
# The runtime owns the resource-accounting policy: an object-count ceiling,
# a nesting-depth ceiling, and a conservative reserve charge for every Julia
# object the getters may later materialize from this metadata. Failures are
# module-local exception types; adapters translate them into their own error
# vocabulary at the wrapper boundary.
# =============================================================================

using Base.Checked: checked_add, checked_mul, checked_sub

# The verified bytes violate FlatBuffers shape or a declared domain.
struct VerifyError <: Exception
    msg::String
end

# The metadata-directed allocation reserve was exhausted during verification.
struct VerifyBudgetError <: Exception
    msg::String
end

_vfail(msg) = throw(VerifyError(msg))

mutable struct VerifyContext
    objects::Int64
    max_objects::Int64
    maxdepth::Base.Int
    reserved::Int64
    reserve_limit::Int64
end
VerifyContext(max_objects::Int64, maxdepth::Base.Int, reserve_limit::Int64) =
    VerifyContext(0, max_objects, maxdepth, 0, reserve_limit)
# Permissive context for callers that only need positional traversal
# (e.g. test fixtures locating bytes to corrupt), never for real input.
VerifyContext() = VerifyContext(typemax(Int64), typemax(Base.Int), typemax(Int64))

# Conservative charges for Julia objects and containers whose sizes are
# directed by verified metadata. String payload bytes are charged on every
# logical getter occurrence. Message bodies remain zero-copy and have their
# own body/buffer byte limits at the adapter.
const METADATA_OBJECT_RESERVE = Int64(2048)
const METADATA_VECTOR_BASE_RESERVE = Int64(256)
const METADATA_VECTOR_ELEMENT_RESERVE = Int64(1024)
const METADATA_STRING_BASE_RESERVE = Int64(128)

function _vcharge!(ctx::VerifyContext, bytes::Int64, what::AbstractString)
    bytes >= 0 || _vfail("negative allocation charge for $what")
    ctx.reserved = try
        checked_add(ctx.reserved, bytes)
    catch e
        e isa OverflowError || rethrow()
        _vfail("allocation charge overflow for $what")
    end
    ctx.reserved <= ctx.reserve_limit || throw(VerifyBudgetError(
        "metadata-directed allocation budget exceeded while visiting $what"))
    return nothing
end

# Count one logical table occurrence. Occurrences, not unique byte
# positions: FlatBuffers may alias a table, while getters and downstream
# mapping expand it once per parent occurrence. Forward UOffsets make the
# graph acyclic.
function _vvisit!(ctx::VerifyContext, what::AbstractString)
    ctx.objects = try
        checked_add(ctx.objects, Int64(1))
    catch e
        e isa OverflowError || rethrow()
        _vfail("metadata object count overflow")
    end
    ctx.objects <= ctx.max_objects ||
        _vfail("metadata object count exceeds limit")
    _vcharge!(ctx, METADATA_OBJECT_RESERVE, what)
    return nothing
end

function _vcount!(ctx::VerifyContext, n::Int64, what::AbstractString)
    n >= 0 || _vfail("negative metadata object count for $what")
    ctx.objects = try
        checked_add(ctx.objects, n)
    catch e
        e isa OverflowError || rethrow()
        _vfail("metadata object count overflow")
    end
    ctx.objects <= ctx.max_objects ||
        _vfail("metadata object count exceeds limit")
    return nothing
end

function _vrange(bytes::Vector{UInt8}, pos::Int64, len::Int64,
    what::AbstractString)
    (pos >= 0 && len >= 0 && len <= length(bytes) && pos <= length(bytes) - len) ||
        _vfail("$what is outside metadata")
    return pos
end

function _vu(bytes, pos::Int64, width::Base.Int)
    _vrange(bytes, pos, Int64(width), "scalar")
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

struct VTable
    bytes::Vector{UInt8}
    pos::Int64
    vpos::Int64
    vlen::Int64
    olen::Int64
end

function _vtable(bytes::Vector{UInt8}, pos::Int64)
    _vrange(bytes, pos, Int64(4), "table")
    pos % 4 == 0 || _vfail("table at $pos is misaligned")
    back = Int64(_vi32(bytes, pos))
    back != 0 || _vfail("table at $pos has a zero vtable offset")
    vpos = checked_sub(pos, back)
    _vrange(bytes, vpos, Int64(4), "vtable header")
    vpos % 2 == 0 || _vfail("vtable at $vpos is misaligned")
    vlen = Int64(_vu16(bytes, vpos))
    olen = Int64(_vu16(bytes, vpos + 2))
    vlen >= 4 && iseven(vlen) || _vfail("invalid vtable length $vlen")
    olen >= 4 || _vfail("invalid table object length $olen")
    _vrange(bytes, vpos, vlen, "vtable")
    _vrange(bytes, pos, olen, "table object")
    return VTable(bytes, pos, vpos, vlen, olen)
end

function _vfield(t::VTable, slot::Base.Int, width::Base.Int=1; required::Base.Bool=false)
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
    _vrange(t.bytes, p, Int64(width), "table slot $slot")
    width > 1 && p % min(width, 8) != 0 &&
        _vfail("table slot $slot is misaligned")
    return p
end

function _vref(t::VTable, slot::Base.Int; required::Base.Bool=false)
    p = _vfield(t, slot, 4; required=required)
    p === nothing && return nothing
    rel = Int64(_vu32(t.bytes, p))
    rel > 0 || _vfail("reference slot $slot has a null/backward offset")
    target = checked_add(p, rel)
    _vrange(t.bytes, target, Int64(1), "reference slot $slot target")
    return target
end

function _vbool(t::VTable, slot::Base.Int)
    p = _vfield(t, slot, 1)
    p === nothing && return nothing
    _vu8(t.bytes, p) in (0x00, 0x01) || _vfail("invalid boolean in slot $slot")
    return nothing
end

function _venum(t::VTable, slot::Base.Int, width::Base.Int, valid)
    p = _vfield(t, slot, width)
    p === nothing && return nothing
    _vu(t.bytes, p, width) in valid || _vfail("invalid enum in slot $slot")
    return nothing
end

function _vstring(t::VTable, slot::Base.Int, ctx::VerifyContext;
    required::Base.Bool=false)
    p = _vref(t, slot; required=required)
    p === nothing && return nothing
    p % 4 == 0 || _vfail("string length is misaligned")
    _vrange(t.bytes, p, Int64(4), "string length")
    n = Int64(_vu32(t.bytes, p))
    start = checked_add(p, Int64(4))
    _vrange(t.bytes, start, checked_add(n, Int64(1)), "string")
    t.bytes[start + n + 1] == 0 || _vfail("string has no NUL terminator")
    _vcharge!(ctx, checked_add(METADATA_STRING_BASE_RESERVE, n), "string")
    payload = @view t.bytes[(start + 1):(start + n)]
    isvalid(String, payload) || _vfail("string is not valid UTF-8")
    return nothing
end

function _vvector(t::VTable, slot::Base.Int, elemsize::Base.Int,
    ctx::VerifyContext=VerifyContext(); required::Base.Bool=false)
    p = _vref(t, slot; required=required)
    p === nothing && return nothing
    _vrange(t.bytes, p, Int64(4), "vector length")
    p % 4 == 0 || _vfail("vector length is misaligned")
    n = Int64(_vu32(t.bytes, p))
    n <= ctx.max_objects ||
        _vfail("vector count $n exceeds metadata object limit")
    _vcount!(ctx, n, "vector entries")
    start = checked_add(p, Int64(4))
    _vrange(t.bytes, start, checked_mul(n, Int64(elemsize)), "vector data")
    n > 0 && elemsize > 1 && start % min(elemsize, 8) != 0 &&
        _vfail("vector data is misaligned")
    _vcharge!(ctx, checked_add(METADATA_VECTOR_BASE_RESERVE,
        checked_mul(n, METADATA_VECTOR_ELEMENT_RESERVE)), "vector")
    return start, Base.Int(n)
end

function _vtablevector(t::VTable, slot::Base.Int, verifyone::F,
    ctx::VerifyContext, depth::Base.Int; required::Base.Bool=false) where {F}
    vec = _vvector(t, slot, 4, ctx; required=required)
    vec === nothing && return 0
    start, n = vec
    for i = 0:(n - 1)
        ep = start + 4i
        rel = Int64(_vu32(t.bytes, ep))
        rel > 0 || _vfail("table vector has null entry")
        verifyone(t.bytes, checked_add(ep, rel), ctx, depth + 1)
    end
    return n
end

# Every element of an enum-typed vector must sit in the declared domain.
function _venumvector(t::VTable, slot::Base.Int, elemsize::Base.Int, valid,
    ctx::VerifyContext, what::AbstractString; required::Base.Bool=false)
    vec = _vvector(t, slot, elemsize, ctx; required=required)
    vec === nothing && return nothing
    start, n = vec
    for i = 0:(n - 1)
        _vu(t.bytes, start + elemsize * i, elemsize) in valid ||
            _vfail("$what has an out-of-domain enum value")
    end
    return nothing
end
