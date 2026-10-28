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
# The IPC STREAM reader as a thin peer over ArrowCore. The file-format
# reader (readfile/ArrowFile) shares these primitives from ipc_write.jl.
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
# Framing stage: resource limits before metadata-directed decode allocation
# ---------------------------------------------------------------------------

const _DEFAULT_MAX_NEW_SYMBOLS = 1_000_000

"""
    Arrow.Limits(; keyword arguments...)

Resource limits for an Arrow IPC reader. Each bound is checked before the
work it governs can exceed it.

| keyword                     | default         | bounds                              |
|:----------------------------|:----------------|:------------------------------------|
| `max_metadata_bytes`        | 16 MiB          | metadata per message                |
| `max_body_bytes`            | none            | one message body                    |
| `max_buffer_bytes`          | none            | one buffer, compressed or not       |
| `max_total_allocated_bytes` | physical memory | allocation per read or stream batch |
| `max_allocation_ratio`      | 64              | allocation per input byte           |
| `max_messages`              | 1,000,000       | messages per file or `Arrow.Table`  |
| `max_metadata_objects`      | 1,000,000       | flatbuffer objects per message      |
| `max_nesting_depth`         | 64              | schema and metadata nesting         |
| `max_array_length`          | 1,000,000,000   | array length                        |
| `max_concurrent_reads`      | 64              | concurrent range reads              |
| `max_new_symbols`           | 1,000,000       | new `Symbol` values per read        |

A read may allocate `max_allocation_ratio` bytes per byte of its input (the
file, buffer, IO, or object), at least 256 MiB, and at most
`max_total_allocated_bytes`. A compressed buffer counts at its decompressed
size, up to the size its column needs, so a compressed file reads with the
allowance it would have uncompressed. Each batch an [`Arrow.Stream`](@ref)
yields gets its own allowance from its own bytes. Ordinary columns cost a
small multiple of their stored bytes, so files of any size and compression
ratio read with the defaults; lengths a small malformed file declares without
the bytes to back them (run-end encoded, view, null-child, or decompressed
lengths beyond what the columns need) fail before they are allocated. Raise
`max_allocation_ratio` for trusted data that expands further, such as long runs
or heavily repeated dictionary values.

A `Symbol` column (Julia's `JuliaLang.Symbol` extension) creates the `Symbol`s
this process has not seen yet, and Julia never frees them, so a read (or one
stream batch) creates at most `max_new_symbols`; a column needing more is
refused before any of its values is converted. `select=[:col => String]` reads
such a column as strings instead.

Pass a `Limits` value as the `limits` keyword to [`Arrow.Table`](@ref) or
[`Arrow.Stream`](@ref).
"""
Base.@kwdef struct Limits
    max_metadata_bytes::Int64 = 16 * 1024 * 1024
    max_body_bytes::Int64 = typemax(Int64)
    max_buffer_bytes::Int64 = typemax(Int64)
    max_total_allocated_bytes::Int64 = _physicalmemory()
    max_allocation_ratio::Int64 = 64
    max_messages::Int = 1_000_000
    max_metadata_objects::Int = 1_000_000
    max_nesting_depth::Int = 64
    max_array_length::Int64 = 1_000_000_000
    max_concurrent_reads::Int = 64
    max_new_symbols::Int = _DEFAULT_MAX_NEW_SYMBOLS
end

_physicalmemory() = Int64(min(Sys.total_memory(), UInt64(typemax(Int64))))

# `left` is charged and refunded with compare-and-swap, so decoding tasks
# share a budget without a lock. `limit` grows as decompressed input arrives
# (`ratio` bytes per byte, never past `cap`).
# A read's budget also carries its `Limits.max_new_symbols`.
mutable struct AllocationBudget
    @atomic left::Int64
    @atomic limit::Int64
    const ratio::Int64
    const cap::Int64
    const maxnewsymbols::Int
end

function AllocationBudget(
    limit::Integer;
    ratio::Integer=0,
    cap::Integer=limit,
    maxnewsymbols::Integer=_DEFAULT_MAX_NEW_SYMBOLS,
)
    value = Int64(limit)
    value >= 0 || throw(ArgumentError("allocation budget must be nonnegative"))
    return AllocationBudget(
        value,
        value,
        Int64(ratio),
        max(value, Int64(cap)),
        maxnewsymbols,
    )
end

# Below this, a read's budget does not shrink with its input: small files keep
# the allowance every input had before the budget scaled with input size.
const _MIN_INPUT_BUDGET = Int64(256) * 1024 * 1024

_saturatingmul(a::Int64, b::Int64) =
    (p=Base.Checked.mul_with_overflow(a, b); p[2] ? typemax(Int64) : p[1])

"The allocation budget for one read of `nbytes` input bytes (see [`Limits`](@ref))."
function _readlimit(limits::Limits, nbytes::Integer)
    perinput = _saturatingmul(limits.max_allocation_ratio, Int64(nbytes))
    return min(limits.max_total_allocated_bytes, max(_MIN_INPUT_BUDGET, perinput))
end

_readbudget(limits::Limits, nbytes::Integer) = AllocationBudget(
    _readlimit(limits, nbytes);
    ratio=limits.max_allocation_ratio,
    cap=limits.max_total_allocated_bytes,
    maxnewsymbols=limits.max_new_symbols,
)

# Move `limit` to `new` and shift `left` by the same amount (not below 0).
function _setlimit!(budget::AllocationBudget, old::Int64, new::Int64)
    _, ok = @atomicreplace budget.limit old => new
    ok || return false
    left = @atomic budget.left
    while true
        left, done = @atomicreplace budget.left left => max(Int64(0), left + (new - old))
        done && return true
    end
end

"Lower an open read's budget to its input's allowance once the input size is known."
function _boundbudget!(budget::AllocationBudget, limits::Limits, nbytes::Integer)
    limit = _readlimit(limits, nbytes)
    while true
        old = @atomic budget.limit
        (limit >= old || _setlimit!(budget, old, limit)) && return budget
    end
end

"Raise the allowance as if `extra` more input bytes had been read, up to the cap."
function _grow!(budget::AllocationBudget, extra::Int64)
    (budget.ratio > 0 && extra > 0) || return nothing
    add = _saturatingmul(budget.ratio, extra)
    while true
        old = @atomic budget.limit
        new = min(budget.cap, old > typemax(Int64) - add ? typemax(Int64) : old + add)
        (new <= old || _setlimit!(budget, old, new)) && return nothing
    end
end

"""
    Arrow.AllocationLimitError

Thrown when a reader exhausts `Limits.max_total_allocated_bytes`. This budget
covers package-controlled fetch, decode, scan, ArrowTypes, and facade
materialization allocations for one read.
"""
struct AllocationLimitError <: Exception
    msg::String
end

function Base.showerror(io::IO, e::AllocationLimitError)
    print(
        io,
        e.msg,
        " (Limits.max_allocation_ratio bytes per input byte, at most ",
        "Limits.max_total_allocated_bytes; to read trusted input that needs more, pass ",
        "`limits=Arrow.Limits(max_allocation_ratio=..., max_total_allocated_bytes=...)`)",
    )
    return nothing
end

function _charge!(budget::AllocationBudget, amount::Int64, what::AbstractString)
    amount >= 0 || throw(ArgumentError("negative allocation charge"))
    left = @atomic budget.left
    while true
        amount <= left ||
            throw(AllocationLimitError("$what exceeds the reader allocation budget"))
        left, ok = @atomicreplace budget.left left => left - amount
        ok && return nothing
    end
end

function _refund!(budget::AllocationBudget, amount::Int64)
    amount >= 0 || throw(ArgumentError("negative allocation refund"))
    left = @atomic budget.left
    while true
        refunded = Base.Checked.checked_add(left, amount)
        refunded <= (@atomic budget.limit) ||
            throw(ArgumentError("allocation refund exceeds the charged total"))
        left, ok = @atomicreplace budget.left left => refunded
        ok && return nothing
    end
end

_remaining(budget::AllocationBudget) = @atomic budget.left

AC._charge_materialization!(budget::AllocationBudget, amount::Int64, what::AbstractString) =
    _charge!(budget, amount, what)
AC._materialization_remaining(budget::AllocationBudget) = _remaining(budget)
AC._materialization_limit_exceeded!(::AllocationBudget, what::AbstractString) =
    throw(AllocationLimitError("$what exceeds the reader allocation budget"))

function _verify_ipc_metadata_budgeted(
    metabytes::Vector{UInt8},
    limits::Limits,
    budget::AllocationBudget,
)
    result = verify_ipc_metadata(metabytes, limits, _remaining(budget))
    _charge!(budget, result[4], "verified metadata expansion")
    return result
end

function _verify_footer_budgeted(
    footerbytes::Vector{UInt8},
    limits::Limits,
    budget::AllocationBudget,
)
    result = verify_footer(footerbytes, limits, _remaining(budget))
    _charge!(budget, result[5], "verified footer expansion")
    return result
end

@inline function _chargevector!(
    budget::Union{Nothing,AllocationBudget},
    ::Type{T},
    n::Integer,
    what::AbstractString,
) where {T}
    budget === nothing || _charge!(budget, AC._materializedvectorbytes(T, n), what)
    return nothing
end

# A vector allocated once at its final length (`Vector{T}(undef, n)`).
# Beyond Julia's small-object size classes its storage is the payload plus
# a header, so large columns are charged what they cost.
@inline function _exactbytes(::Type{T}, n::Integer) where {T}
    payload = AC.checked_mul(Int64(n), Int64(Base.elsize(Vector{T})))
    Base.isbitsunion(T) && (payload = AC.checked_add(payload, Int64(n)))
    return payload <= 4096 ? AC._materializedvectorbytes(T, n) :
           AC.checked_add(payload, Int64(128))
end

@inline function _chargeexact!(
    budget::Union{Nothing,AllocationBudget},
    ::Type{T},
    n::Integer,
    what::AbstractString,
) where {T}
    budget === nothing || _charge!(budget, _exactbytes(T, n), what)
    return nothing
end

@inline function _chargebitvector!(
    budget::Union{Nothing,AllocationBudget},
    n::Integer,
    what::AbstractString,
)
    budget === nothing || _charge!(budget, AC._materializedbitvectorbytes(n), what)
    return nothing
end

@inline function _chargeobject!(
    budget::Union{Nothing,AllocationBudget},
    payload::Integer,
    what::AbstractString,
)
    budget === nothing || _charge!(budget, AC._materializedobjectbytes(payload), what)
    return nothing
end

@inline function _dictslotbytes(::Type{K}, ::Type{V}) where {K,V}
    return AC.checked_add(
        Int64(Base.elsize(Vector{K})),
        AC.checked_add(Int64(Base.elsize(Vector{V})), Int64(16)),
    )
end

const _MIN_EMPTY_DICT_SLOTS = Int64(16)

"Reserve the supported-version minimum backing store for one empty dictionary."
function _chargeemptydict!(
    budget::Union{Nothing,AllocationBudget},
    ::Type{K},
    ::Type{V},
    what::AbstractString,
) where {K,V}
    budget === nothing && return nothing
    payload = AC.checked_mul(_MIN_EMPTY_DICT_SLOTS, _dictslotbytes(K, V))
    # Julia 1.10 eagerly allocates minimum Dict backing arrays. Later Julia
    # versions defer more of this work. Reserve the larger supported layout so
    # cache construction has one stable reader-budget contract on every host.
    _charge!(budget, AC.checked_add(Int64(512), payload), what)
    return nothing
end

function _chargedict!(
    budget::Union{Nothing,AllocationBudget},
    ::Type{K},
    ::Type{V},
    n::Integer,
    what::AbstractString,
) where {K,V}
    budget === nothing && return nothing
    n >= 0 || throw(ArgumentError("negative dictionary capacity"))
    entries = Int64(n)
    capacity = entries == 0 ? Int64(0) : AC.checked_mul(entries, Int64(2))
    payload = AC.checked_mul(capacity, _dictslotbytes(K, V))
    # Dict's requested hash-table capacity and Julia's backing-store capacity
    # can each round upward. Reserve both layers conservatively.
    bytes = AC.checked_add(Int64(512), AC.checked_mul(Int64(2), payload))
    _charge!(budget, bytes, what)
    return nothing
end

"Charge amortized growth for one new entry after its container reserve."
function _chargedictentry!(
    budget::Union{Nothing,AllocationBudget},
    ::Type{K},
    ::Type{V},
    what::AbstractString,
) where {K,V}
    budget === nothing && return nothing
    # `_chargedict!` reserves two slots per requested entry and two backing
    # layers. Charge that linear term once per new memo key. ArrowTypes memo
    # owners reserve the supported-version empty container separately.
    bytes = AC.checked_mul(Int64(4), _dictslotbytes(K, V))
    _charge!(budget, bytes, what)
    return nothing
end

"Memoize one value after reserving the new dictionary entry's allocation."
function _memoized!(
    f::F,
    cache::AbstractDict{K,V},
    key,
    budget::Union{Nothing,AllocationBudget},
    what::AbstractString,
) where {F,K,V}
    haskey(cache, key) && return cache[key]
    _chargedictentry!(budget, K, V, what)
    return get!(f, cache, key)
end

struct FramedMessage
    msg::Meta.Message        # parsed flatbuffer metadata
    body::BufferSlice        # THE authority: buffers must subslice this
    version::Int16
    header_type::UInt8
    features::Vector{Int64}  # populated on schema messages
end

# The encapsulated-message marker that precedes every metadata length. This
# reader requires it even on V4 metadata, so streams written before the
# marker existed are refused.
const CONTINUATION = 0xFFFFFFFF
const EXPERIMENTAL_COMPRESSION_KEY = "ARROW:experimental_compression"

# ---------------------------------------------------------------------------
# FlatBuffers verification (generated walkers over a schema-blind runtime)
# ---------------------------------------------------------------------------

# The shape verifier is GENERATED from the vendored format/*.fbs by
# tools/fbsgen.jl (src/metadata/Verifier.jl). Table geometry, scalar widths,
# enum domains, string and vector bounds, union dispatch, and the
# nesting/object/reserve budgets all derive from the schema, so binding drift
# cannot reach them. The wrappers below own only what the schema cannot
# express: the accepted metadata versions and message kinds, and the
# features/version coupling.
# The aliases below exist for test fixtures, which reuse the runtime's
# traversal primitives to LOCATE the bytes they corrupt. No src file uses them.
const _VTable = Meta.VTable
const _vtable = Meta._vtable
const _vfield = Meta._vfield
const _vref = Meta._vref
const _vvector = Meta._vvector
const _vrange = Meta._vrange
const _vu8 = Meta._vu8
const _vu32 = Meta._vu32
const _vi32 = Meta._vi32
const _vi64 = Meta._vi64

_vfail(msg) = throw(ValidationError("invalid IPC FlatBuffer: $msg"))

_verifyctx(limits::Limits, reserve_limit::Int64) = Meta.VerifyContext(
    Int64(limits.max_metadata_objects),
    limits.max_nesting_depth,
    reserve_limit,
)

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
    version == Int16(3) &&
        !isempty(features) &&
        _vfail("schema features require metadata V5")
    return features
end

function verify_ipc_metadata(
    bytes::Vector{UInt8},
    limits::Limits,
    reserve_limit::Int64=limits.max_total_allocated_bytes,
)
    ctx = _verifyctx(limits, reserve_limit)
    # STAGED root verification. The inline stage proves the table shell and
    # every non-reference field, including the version. The adapter then
    # gates the version, so an unsupported one rejects in constant time. Only
    # after that does the reference stage walk the attacker-directed header
    # graph.
    t = _verified(() -> Meta.verifyrootstart_Message(bytes, ctx))
    msg = FB.getrootas(Meta.Message, bytes, 0)
    version = Int16(Int64(msg.version))
    version in (Int16(3), Int16(4)) ||
        _vfail("unsupported metadata version $version (only V4/V5 are accepted)")
    _verified(() -> Meta.verifyrootrest_Message(t, ctx))
    # The verifier proved header presence and rejected union members outside
    # the generated schemas (the Tensor family), so this dispatch is total.
    header = msg.header
    header_type =
        header isa Meta.Schema ? UInt8(1) :
        header isa Meta.DictionaryBatch ? UInt8(2) :
        header isa Meta.RecordBatch ? UInt8(3) : _vfail("unsupported message header tag")
    features = header isa Meta.Schema ? _schemafeatures(header, version) : Int64[]
    return version, header_type, features, ctx.reserved
end

"""
    framemessages(region, limits) -> Vector{FramedMessage}

Walk the IPC stream framing (continuation marker, metadata length, metadata
flatbuffer, body), checking every declared length against the limits and the
region's real extent before metadata-directed decode allocation. A truncated
prefix, metadata block, or body throws. EOF exactly after a complete message
is the intentional missing-EOS boundary case and is accepted. The framing's
metadata length covers the padded metadata only; a Footer Block's
metaDataLength instead covers prefix + metadata.
"""
framemessages(region::OwnerRegion, limits::Limits=Limits()) =
    _framemessages(region, limits, Base.ENDIAN_BOM, _readbudget(limits, region.len))

function _validatelimits(limits::Limits)
    limits.max_metadata_bytes >= 0 || throw(ArgumentError("negative metadata limit"))
    limits.max_body_bytes >= 0 || throw(ArgumentError("negative body limit"))
    limits.max_buffer_bytes >= 0 || throw(ArgumentError("negative buffer limit"))
    limits.max_total_allocated_bytes >= 0 ||
        throw(ArgumentError("negative allocation limit"))
    limits.max_allocation_ratio >= 0 || throw(ArgumentError("negative allocation ratio"))
    limits.max_messages >= 0 || throw(ArgumentError("negative message limit"))
    limits.max_metadata_objects >= 0 ||
        throw(ArgumentError("negative metadata-object limit"))
    limits.max_nesting_depth >= 0 || throw(ArgumentError("negative nesting limit"))
    limits.max_array_length >= 0 || throw(ArgumentError("negative array-length limit"))
    limits.max_concurrent_reads >= 1 ||
        throw(ArgumentError("concurrent-read limit must be positive"))
    limits.max_new_symbols >= 0 || throw(ArgumentError("negative new-Symbol limit"))
    return nothing
end

function _framemessages(
    region::OwnerRegion,
    limits::Limits,
    host_endian_bom::UInt32,
    budget::AllocationBudget=_readbudget(limits, region.len),
)
    # The generated FlatBuffers bindings use native-endian scalar
    # loads. Reject an unsupported host before any generated getter sees the
    # little-endian wire bytes. The explicit argument keeps this ordering
    # testable on the supported little-endian CI host.
    host_endian_bom == UInt32(0x04030201) ||
        throw(ValidationError("the IPC reader requires a little-endian host"))
    _validatelimits(limits)
    blob = BufferSlice(region, 0, region.len)
    msgs = FramedMessage[]
    pos = Int64(0)   # 0-based byte position within the blob
    while true
        fm, pos = _framemessage(blob, pos, limits, budget, length(msgs))
        fm === nothing && return msgs
        push!(msgs, fm)
    end
end

# The error for a message prefix without the continuation marker: at the
# start of the input, the input is not Arrow IPC data (or is a stream from
# before Arrow 0.15, whose messages start with their metadata length).
function _nomarker(pos::Int64, word::UInt32)
    pos == 0 || return "missing continuation marker at byte $pos"
    msg =
        "not Arrow IPC data: the input starts with neither the file magic " *
        "ARROW1 nor an IPC stream message"
    # "PAR1", read as a little-endian word.
    word == 0x31524150 && return msg * " (it starts with PAR1, as a Parquet file does)"
    0 < (word % Int32) <= 1 << 24 || return msg
    return msg *
           " (a stream written before Arrow 0.15 starts like this; " *
           "rewrite it with a newer Arrow writer)"
end

"""
Frame the message at 0-based `pos` of `blob`, the `nread + 1`th of its stream:
returns it and the position after it, or `nothing` at the end-of-stream
marker or the end of the bytes.
"""
function _framemessage(
    blob::BufferSlice,
    pos::Int64,
    limits::Limits,
    budget::AllocationBudget,
    nread::Int,
)
    pos == blob.len && return nothing, pos
    blob.len - pos >= 8 || throw(
        ValidationError(
            pos == 0 ? "not Arrow IPC data: the input is $(blob.len) bytes" :
            "truncated IPC prefix at byte $pos",
        ),
    )
    pos % 8 == 0 || throw(ValidationError("IPC message is not 8-byte aligned"))
    cont = AC.loadat(blob, UInt32, pos)
    cont == CONTINUATION || throw(ValidationError(_nomarker(pos, cont)))
    # The stream framing's metadata length covers the padded metadata
    # only. A Footer Block.metaDataLength instead covers prefix +
    # metadata (see block!).
    metalen = Int64(AC.loadat(blob, Int32, AC.checked_add(pos, Int64(4))))
    # Continuation marker + zero length is the end-of-stream marker.
    # Nothing may follow it.
    if metalen == 0
        AC.checked_add(pos, Int64(8)) == blob.len || throw(
            ValidationError(
                "trailing bytes after IPC end-of-stream; for IPC streams written " *
                "one after another, an Arrow.Stream of an IO reads one stream and " *
                "leaves the IO at the next",
            ),
        )
        return nothing, blob.len
    end
    _checkmessagecount(nread, limits)
    _checkmetalen(metalen, limits)
    metastart = AC.checked_add(pos, Int64(8))
    bodystart = AC.checked_add(metastart, metalen)
    bodystart <= blob.len ||
        throw(ValidationError("truncated metadata: need $metalen bytes at $pos"))
    _chargevector!(budget, UInt8, metalen, "metadata allocation")
    metabytes = AC.slicebytes(AC.subslice(blob, metastart, metalen))
    msg, version, header_type, features, bodylen =
        _verifiedmessage(metabytes, limits, budget)
    bodyend = AC.checked_add(bodystart, bodylen)
    bodyend <= blob.len ||
        throw(ValidationError("truncated body: need $bodylen bytes at $bodystart"))
    fm = FramedMessage(
        msg,
        AC.subslice(blob, bodystart, bodylen),
        version,
        header_type,
        features,
    )
    return fm, bodyend
end

# `what` (a declared size) passes `Limits` field `field`, whose value is `limit`.
@noinline _limiterror(what::AbstractString, field::String, limit) = throw(
    ValidationError(
        "$what exceeds Arrow.Limits $field ($limit); raise it with " *
        "limits=Arrow.Limits($field=...)",
    ),
)

# A footer block's declared metadata and body lengths.
function _checkblocklengths(metalen::Int64, bodylen::Int64, limits::Limits)
    metalen > 0 || throw(ValidationError("metadata length $metalen is not positive"))
    metalen <= limits.max_metadata_bytes || _limiterror(
        "metadata length $metalen",
        "max_metadata_bytes",
        limits.max_metadata_bytes,
    )
    bodylen >= 0 || throw(ValidationError("body length $bodylen is negative"))
    bodylen <= limits.max_body_bytes ||
        _limiterror("body length $bodylen", "max_body_bytes", limits.max_body_bytes)
    return nothing
end

_checkmessagecount(nread::Int, limits::Limits) =
    nread < limits.max_messages ||
    _limiterror("message count", "max_messages", limits.max_messages)

function _checkmetalen(metalen::Int64, limits::Limits)
    metalen > 0 || throw(ValidationError("metadata length $metalen is not positive"))
    metalen <= limits.max_metadata_bytes || _limiterror(
        "metadata length $metalen",
        "max_metadata_bytes",
        limits.max_metadata_bytes,
    )
    metalen % 8 == 0 ||
        throw(ValidationError("metadata length $metalen is not 8-byte aligned"))
    return nothing
end

# Verify a message's metadata, then read it and its body length.
function _verifiedmessage(
    metabytes::Vector{UInt8},
    limits::Limits,
    budget::AllocationBudget,
)
    version, header_type, features, _ =
        _verify_ipc_metadata_budgeted(metabytes, limits, budget)
    # No generated getter runs before the verifier has bounded the full
    # table/vector/string graph it may visit.
    msg = FB.getrootas(Meta.Message, metabytes, 0)
    bodylen = Int64(msg.bodyLength)
    bodylen >= 0 || throw(ValidationError("body length $bodylen is negative"))
    bodylen <= limits.max_body_bytes ||
        _limiterror("body length $bodylen", "max_body_bytes", limits.max_body_bytes)
    bodylen % 8 == 0 || throw(ValidationError("body length $bodylen is not 8-byte aligned"))
    return msg, version, header_type, features, bodylen
end

# ---------------------------------------------------------------------------
# Metadata mapping: Meta.* type structs -> Core runtime descriptors
# ---------------------------------------------------------------------------

# One value-level mapping table: one function per direction over runtime
# descriptors. Julia-type conversion is the facade's concern, not this
# adapter's.

function coretype(t)::ArrowType
    if t isa Meta.Int
        IntType(Int(t.bitWidth), t.is_signed)
    elseif t isa Meta.FloatingPoint
        FloatType(
            t.precision == Meta.Precision.HALF ? 16 :
            t.precision == Meta.Precision.SINGLE ? 32 : 64,
        )
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
        IntervalType(
            t.unit == Meta.IntervalUnit.YEAR_MONTH ? AC.YEAR_MONTH :
            t.unit == Meta.IntervalUnit.DAY_TIME ? AC.DAY_TIME : AC.MONTH_DAY_NANO,
        )
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
    # Type ids are Int8 in [0, 127] (checked below), so 128 distinct children
    # is the ceiling.
    nchildren <= 128 || throw(ValidationError("a union cannot have more than 128 children"))
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

timeunit(u) =
    u == Meta.TimeUnit.SECOND ? AC.SECOND :
    u == Meta.TimeUnit.MILLISECOND ? AC.MILLISECOND :
    u == Meta.TimeUnit.MICROSECOND ? AC.MICROSECOND : AC.NANOSECOND

function coremetadata(kvs)
    kvs === nothing && return nothing
    return Pair{String,String}[
        String(kv.key) => String(something(kv.value, "")) for kv in kvs
    ]
end

"""
Convert a metadata Field to a Core Field. Dictionary-encoded fields become
`DictionaryType` here; the IPC dictionary id is recorded in the adapter's
side table (`dictids`), NOT on the Core field — Core never learns about ids.
"""
function corefield(
    f::Meta.Field,
    dictids::Dict{Int64,Meta.Field},
    fielddictids::IdDict{Field,Int64},
)
    children = Field[
        corefield(c, dictids, fielddictids) for c in something(f.children, Meta.Field[])
    ]
    t = _coremetatype(f.type, children)
    if f.dictionary === nothing
        return Field(
            String(something(f.name, "")),
            t,
            f.nullable,
            coremetadata(f.custom_metadata),
            children,
        )
    end
    # Nested dictionary encoding (a dictionary field whose VALUE type has
    # dictionary-encoded children) is spec-legal and present in the
    # arrow-testing gold corpus (nested_dictionary: dict(list(dict(utf8)))).
    # A dictionary batch's values decode through the same `decodefield`
    # with the live pool table, so inner pools resolve as long as batches
    # arrive in dependency order — which the IPC spec requires.
    dictids[f.dictionary.id] = f
    idxt =
        f.dictionary.indexType === nothing ? IntType(32, true) :
        coretype(f.dictionary.indexType)::IntType
    cf = Field(
        String(something(f.name, "")),
        DictionaryType(idxt, t, f.dictionary.isOrdered),
        f.nullable,
        coremetadata(f.custom_metadata),
        children,
    )
    # Identity-keyed: safe for duplicate column names and nested dict fields
    # (name matching would be neither).
    fielddictids[cf] = f.dictionary.id
    return cf
end

function validatedictionaryids(fields, fielddictids::IdDict{Field,Int64})
    seen = Dict{Int64,Field}()
    compatible(a::Field, b::Field; compare_name::Bool=false) =
        (!compare_name || a.name == b.name) &&
        AC.typeequal(a.type, b.type) &&
        a.nullable == b.nullable &&
        # One id resolves ONE pool, so repeated ids must agree on the whole
        # nested dictionary-id topology: compatible value schemas whose
        # nested fields carry DIFFERENT wire ids would decode the second
        # field through pools its schema never declared.
        (!(a.type isa DictionaryType) || fielddictids[a] == fielddictids[b]) &&
        length(a.children) == length(b.children) &&
        all(compatible(x, y; compare_name=true) for (x, y) in zip(a.children, b.children))
    function walk(f::Field)
        if f.type isa DictionaryType
            id = fielddictids[f]
            vf = AC.dictvaluefield(f, f.type)
            if haskey(seen, id)
                old = seen[id]
                compatible(old, vf) || throw(
                    ValidationError(
                        "dictionary id $id is shared by incompatible value schemas",
                    ),
                )
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
    length(f.children) == expected || throw(
        ValidationError(
            "$(typeof(f.type)) schema expects $expected children, got $(length(f.children))",
        ),
    )
    if f.type isa UnionType
        length(f.type.typeids) == length(f.children) ||
            throw(ValidationError("union type-id count must equal child count"))
        length(unique(f.type.typeids)) == length(f.type.typeids) ||
            throw(ValidationError("union type ids must be unique"))
        all(>=(0), f.type.typeids) ||
            throw(ValidationError("union type ids must be in [0, 127]"))
    elseif f.type isa MapType
        entries = f.children[1]
        entries.type isa StructType &&
        !entries.nullable &&
        length(entries.children) == 2 &&
        !entries.children[1].nullable ||
            throw(ValidationError("invalid map entries/key schema"))
    elseif f.type isa RunEndEncodedType
        length(f.children) == 2 || throw(ValidationError("REE requires two children"))
        run, values = f.children
        run.name == "run_ends" &&
        values.name == "values" &&
        run.type isa IntType &&
        run.type.signed &&
        run.type.bits in (16, 32, 64) &&
        !run.nullable || throw(ValidationError("invalid run-end encoded schema"))
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

DecodeState(budget::AllocationBudget) =
    DecodeState(Ptr{CLZ4.LZ4F_dctx}(C_NULL), Ptr{ZSTD.ZSTD_DCtx}(C_NULL), budget)

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

# --- task parallelism -------------------------------------------------------

function _checkntasks(ntasks)
    (ntasks isa Integer && ntasks >= 1) ||
        throw(ArgumentError("ntasks must be a positive integer, got $(repr(ntasks))"))
    return Int(ntasks)
end

# The next item `_foreach` hands out and the smallest item that failed.
mutable struct _Claims
    @atomic next::Int
    @atomic failed::Int
end

"""
Call `f(i, w)` for each `i` in `1:n` on at most `ntasks` tasks, the caller
being one of them; `w` in `1:min(n, ntasks)` names the task, so `f` can use
per-task scratch state. Items are claimed in increasing order. When items
throw, items after the smallest failing one are skipped and its exception is
rethrown once every task has stopped, so errors do not depend on `ntasks`.
"""
function _foreach(f::F, n::Int, ntasks::Int) where {F}
    nt = min(n, ntasks)
    if nt <= 1
        for i = 1:n
            f(i, 1)
        end
        return nothing
    end
    claims = _Claims(0, typemax(Int))
    errors = Vector{Any}(nothing, n)
    # The tasks reach `f` (and whatever it captures, such as a read's decoded
    # batches) only through `job`, which is cleared once they are done: a
    # finished task can stay reachable after this call (an idle thread keeps
    # the last task it ran), and must not keep that data alive.
    job = Ref{Union{Nothing,F}}(f)
    function work(w)
        g = job[]::F
        while true
            i = @atomic claims.next += 1
            (i > n || i > @atomic(claims.failed)) && return nothing
            try
                g(i, w)
            catch e
                errors[i] = e
                old = @atomic claims.failed
                while i < old
                    old, ok = @atomicreplace claims.failed old => i
                    ok && break
                end
            end
        end
    end
    tasks = [errormonitor(Threads.@spawn work(w)) for w = 2:nt]
    try
        work(1)
    finally
        foreach(wait, tasks)
        job[] = nothing
    end
    failed = @atomic claims.failed
    failed == typemax(Int) || throw(errors[failed])
    return nothing
end

# Decode into `out[1:declared]` and return the bytes produced. With
# `exact=false` the frame may hold more (decoding stops once `out` is full)
# or less; the caller checks the count.
function _decode_lz4!(
    state::DecodeState,
    src::Ptr{UInt8},
    srclen::Int64,
    out::Vector{UInt8},
    declared::Int64,
    exact::Bool=true,
)
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
        hint = CLZ4.LZ4F_decompress(ctx, dst, outsize, src + inpos, insize, C_NULL)
        inpos += Int64(insize[])
        outpos += Int64(outsize[])
        exact || outpos < declared || return outpos
        if hint == 0
            exact || return outpos
            inpos == srclen || throw(
                ValidationError("LZ4 buffer contains trailing bytes or multiple frames"),
            )
            outpos == declared || throw(
                ValidationError(
                    "LZ4 output length $outpos does not match declared $declared",
                ),
            )
            return outpos
        end
        inpos < srclen ||
            (exact ? throw(ValidationError("truncated LZ4 frame")) : return outpos)
        (insize[] != 0 || outsize[] != 0) ||
            throw(ValidationError("LZ4 output exceeds declared length $declared"))
    end
end

function _decode_zstd!(
    state::DecodeState,
    src::Ptr{UInt8},
    srclen::Int64,
    out::Vector{UInt8},
    declared::Int64,
)
    dst = declared == 0 ? Ptr{UInt8}(C_NULL) : pointer(out)
    got = ZSTD.ZSTD_decompressDCtx(
        _zstdctx!(state),
        dst,
        Csize_t(declared),
        src,
        Csize_t(srclen),
    )
    if ZSTD.ZSTD_isError(got) != 0
        msg = unsafe_string(ZSTD.ZSTD_getErrorName(got))
        throw(ValidationError("ZSTD decompression failed: $msg"))
    end
    Int64(got) == declared || throw(
        ValidationError(
            "ZSTD output length $(Int64(got)) does not match declared $declared",
        ),
    )
    return Int64(got)
end

# Decode at most `length(out)` bytes of a zstd buffer and return how many
# were produced; the rest of the input is not read.
function _decode_zstd_prefix!(
    state::DecodeState,
    src::Ptr{UInt8},
    srclen::Int64,
    out::Vector{UInt8},
)
    dctx = _zstdctx!(state)
    ZSTD.ZSTD_DCtx_reset(dctx, ZSTD.ZSTD_reset_session_only)
    input = ZSTD.ZSTD_inBuffer(Ptr{Cvoid}(src), Csize_t(srclen), Csize_t(0))
    output = ZSTD.ZSTD_outBuffer(Ptr{Cvoid}(pointer(out)), Csize_t(length(out)), Csize_t(0))
    while output.pos < length(out)
        before = (input.pos, output.pos)
        r = ZSTD.ZSTD_decompressStream(dctx, output, input)
        ZSTD.ZSTD_isError(r) == 0 || throw(
            ValidationError(
                "ZSTD decompression failed: $(unsafe_string(ZSTD.ZSTD_getErrorName(r)))",
            ),
        )
        (r == 0 || (input.pos, output.pos) == before) && break
    end
    return Int64(output.pos)
end

# `B` is the body representation: a contiguous `BufferSlice` for in-memory
# and mmapped messages, or a sparse body (scan.jl) whose fetched
# spans stand in for the contiguous message body. `_bodyslice` is the one
# seam between them; the parameter keeps the cursor concrete per use.
# Concretely typed vectors: node and buffer reads run once per field per batch.
mutable struct DecodeCursor{
    B,
    N<:AbstractVector{Meta.FieldNode},
    U<:AbstractVector{Meta.Buffer},
    V<:AbstractVector{<:Integer},
}
    nodes::N
    buffers::U
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
    variadics::V
    varidx::Int
end

"Resolve one declared buffer window against the message body."
_bodyslice(body::BufferSlice, offset::Int64, len::Int64) = AC.subslice(body, offset, len)

DecodeCursor(
    nodes,
    buffers,
    body,
    limits::Limits;
    codec::Int8=CODEC_NONE,
    state::Union{Nothing,DecodeState}=nothing,
    variadics=nothing,
) = DecodeCursor(
    something(nodes, Meta.FieldNode[]),
    something(buffers, Meta.Buffer[]),
    body,
    limits.max_buffer_bytes,
    limits.max_array_length,
    1,
    1,
    0,
    codec,
    state,
    something(variadics, Int64[]),
    1,
)

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
    c.varidx <= length(c.variadics) || throw(
        ValidationError(
            "metadata declares fewer variadic buffer counts than the schema requires",
        ),
    )
    n = c.variadics[c.varidx]
    c.varidx += 1
    0 <= n <= length(c.buffers) ||
        throw(ValidationError("variadic buffer count $n outside [0, $(length(c.buffers))]"))
    return Int(n)
end

function takenode!(c::DecodeCursor)
    c.nodeidx <= length(c.nodes) || throw(
        ValidationError("metadata declares fewer field nodes than the schema requires"),
    )
    n = c.nodes[c.nodeidx]
    c.nodeidx += 1
    n.length >= 0 || throw(ValidationError("field-node length $(n.length) is negative"))
    n.length <= c.max_array_length ||
        _limiterror("field-node length $(n.length)", "max_array_length", c.max_array_length)
    0 <= n.null_count <= n.length ||
        throw(ValidationError("invalid field-node null count $(n.null_count)"))
    return n
end

"""
Consume one buffer-table entry's METADATA: bounds, alignment, limits, and the
non-overlap/monotone invariants — every check that needs no body byte.
`takebuffer!` continues into the body subslice and decompression;
`skipbuffer!` stops here, so scan pushdown can skip a column's decode and its
body range. Ranged tail reads and coalescing may still over-read those bytes.
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
    len >= 0 || throw(ValidationError("batch buffer length $len is negative"))
    len <= c.max_buffer_bytes ||
        _limiterror("batch buffer length $len", "max_buffer_bytes", c.max_buffer_bytes)
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

# `needed` is the byte count the buffer's column reads (-1 when unknown); it
# bounds how far decompressed bytes extend the read's allowance.
function takebuffer!(c::DecodeCursor, needed::Int64=Int64(-1))
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
    return _decompressbuffer!(c, wire, needed)
end

"""
Decode one compressed buffer: an Int64 uncompressed-length prefix, then the
payload. A prefix of -1 means the payload is stored raw and stays a view of
the wire mapping. The declared size is attacker-controlled: it must agree with
the size the frame headers record, if any, and is charged to the read's budget
before allocation. A buffer that declares more than its column reads
(`needed`, from the field node; a writer may store a sliced column's whole
buffer) decodes only the column's bytes; otherwise the decompressed size must
match the declaration exactly. A positively compressed buffer becomes its own
exact-sized owned region.

Decompressed bytes count as input to the budget as far as the column reads
them, so a highly compressible buffer is funded while a claim beyond what the
column reads is not.
"""
function _decompressbuffer!(c::DecodeCursor, wire::BufferSlice, needed::Int64=Int64(-1))
    wire.len >= 8 || throw(
        ValidationError("compressed buffer of $(wire.len) bytes lacks its length prefix"),
    )
    declared = AC.loadat(wire, Int64, Int64(0))
    declared == -1 && return AC.subslice(wire, 8, wire.len - 8)  # stored raw
    declared >= 0 ||
        throw(ValidationError("declared decompressed length $declared is negative"))
    declared <= c.max_buffer_bytes || _limiterror(
        "declared decompressed length $declared",
        "max_buffer_bytes",
        c.max_buffer_bytes,
    )
    declared <= typemax(Int) >> 1 ||
        throw(ValidationError("declared decompressed buffer is not addressable"))
    payloadlen = wire.len - 8
    payloadlen > 0 || throw(ValidationError("compressed buffer has an empty payload"))
    state = c.state::DecodeState
    wireregion = wire.region::OwnerRegion
    recorded = GC.@preserve wireregion _framecontentsize(
        c.codec,
        AC.sliceptr(wire) + 8,
        payloadlen,
    )
    (recorded < 0 || recorded == declared) || throw(
        ValidationError(
            "declared decompressed length $declared does not match the $recorded bytes " *
            "its frame records",
        ),
    )
    # A buffer longer than its column reads (a writer may store a sliced
    # column's whole buffer) decodes only the column's bytes.
    exact = needed < 0 || declared <= _padded64(needed)
    outlen = exact ? declared : _padded64(needed)
    # No payload decodes to more than its codec's largest expansion: LZ4
    # turns one byte into at most 255, a 4-byte zstd RLE block into at most
    # 128 KiB. A larger claim is refused before it is allocated.
    expansion = c.codec == CODEC_LZ4_FRAME ? Int64(255) : Int64(32768)
    outlen <= AC.checked_mul(expansion, payloadlen) || throw(
        ValidationError(
            "declared decompressed length $outlen exceeds what $payloadlen " *
            "compressed bytes can hold",
        ),
    )
    needed >= 0 && _grow!(state.budget, outlen - payloadlen)
    charged = _exactbytes(UInt8, outlen)
    _charge!(state.budget, charged, "decompressed bytes")
    committed = false
    try
        # This is the only output allocation. Its size was checked and
        # charged before either native decoder sees the input frame.
        out = Vector{UInt8}(undef, Int(outlen))
        # The native decoders read through a raw pointer, so the wire
        # region's root must stay reachable for the whole call (Core rule 2).
        got = GC.@preserve out wireregion begin
            src = AC.sliceptr(wire) + 8
            if c.codec == CODEC_LZ4_FRAME
                _decode_lz4!(state, src, payloadlen, out, outlen, exact)
            elseif exact
                _decode_zstd!(state, src, payloadlen, out, outlen)
            else
                _decode_zstd_prefix!(state, src, payloadlen, out)
            end
        end
        exact ||
            got >= needed ||
            throw(
                ValidationError(
                    "compressed buffer holds $got bytes; its column needs $needed",
                ),
            )
        region = OwnerRegion(Ptr{UInt8}(pointer(out)), sizeof(out); root=_Decompressed(out))
        result = BufferSlice(region, 0, got)
        committed = true
        return result
    catch e
        e isa Union{ValidationError,AllocationLimitError} && rethrow()
        e isa OutOfMemoryError && rethrow()
        e isa InterruptException && rethrow()
        throw(ValidationError("buffer decompression failed: $(sprint(showerror, e))"))
    finally
        committed || _refund!(state.budget, charged)
    end
end

_padded64(n::Int64) = n > typemax(Int64) - 63 ? typemax(Int64) : (n + 63) & ~Int64(63)

# The root of a decompressed buffer's region: bytes this read allocated and no
# other buffer shares, so a column may keep them instead of a copy (string
# data, `_adoptbytes`).
struct _Decompressed
    bytes::Vector{UInt8}
end

# The decompressed size the frame headers record: zstd frames usually do (a
# buffer may hold several), LZ4 frames optionally. -1 when a header does not
# say; a malformed header is left for the decoder to reject.
function _framecontentsize(codec::Int8, src::Ptr{UInt8}, len::Int64)::Int64
    if codec == CODEC_ZSTD
        total = Int64(0)
        pos = Int64(0)
        while pos < len
            n = ZSTD.ZSTD_getFrameContentSize(src + pos, Csize_t(len - pos))
            n >= ZSTD.ZSTD_CONTENTSIZE_ERROR && return Int64(-1)
            n > typemax(Int64) - total && return Int64(-1)
            total += Int64(n)
            framelen = ZSTD.ZSTD_findFrameCompressedSize(src + pos, Csize_t(len - pos))
            (ZSTD.ZSTD_isError(framelen) == 0 && framelen > 0) || return Int64(-1)
            pos += Int64(framelen)
        end
        return total
    end
    # LZ4 frame: magic, FLG (bit 3: content size present), BD, then the size.
    (
        len >= 14 &&
        unsafe_load(src, 1) == 0x04 &&
        unsafe_load(src, 2) == 0x22 &&
        unsafe_load(src, 3) == 0x4d &&
        unsafe_load(src, 4) == 0x18
    ) || return Int64(-1)
    unsafe_load(src, 5) & 0x08 == 0 && return Int64(-1)
    n = UInt64(0)
    for k = 14:-1:7
        n = (n << 8) | unsafe_load(src, k)
    end
    return n > typemax(Int64) ? Int64(-1) : Int64(n)
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

function missingdicts(
    fields,
    nodes,
    dicts::Dict{Int64,ArrayData},
    fielddictids::IdDict{Field,Int64},
)
    ns = something(nodes, Meta.FieldNode[])
    idx = Ref(1)
    missing = Set{Int64}()
    function walk(f::Field)
        idx[] <= length(ns) || throw(
            ValidationError("metadata declares fewer field nodes than the schema requires"),
        )
        node = ns[idx[]]
        idx[] += 1
        if f.type isa DictionaryType
            id = fielddictids[f]
            if !haskey(dicts, id)
                node.length >= 0 && node.null_count == node.length || throw(
                    ValidationError(
                        "record batch uses undefined dictionary id $id for a non-null slot",
                    ),
                )
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
    decodefield(field, cursor, dictionaries, fielddictids) -> ArrayData

Generic over every mapped layout in this adapter, variadic (view) layouts
included.
Dictionary-encoded columns consume the INDEX layout's buffers (validity +
indices) and resolve their values through the adapter's dictionary table.
"""
# The bytes a column of `n` slots reads from a buffer in `role`, from its
# field node alone (-1 when that needs bytes not decoded yet). Variable-width
# data ends at the last offset, which precedes it in `previous`.
function _neededbytes(role::AC.BufferRole, spec::AC.LayoutSpec, n::Int64, previous)
    role == AC.VALIDITY && return cld(n, 8)
    role == AC.TYPE_IDS && return n
    role == AC.VIEWS && return _saturatingmul(n, Int64(16))
    role == AC.OFFSETS &&
        return n == 0 ? Int64(0) : _saturatingmul(n + 1, Int64(spec.offsetwidth))
    (role == AC.ELEMENT_OFFSETS || role == AC.SIZES) &&
        return _saturatingmul(n, Int64(spec.offsetwidth))
    role == AC.DATA || return Int64(-1)
    spec.fixedwidth > 0 && return _saturatingmul(n, Int64(spec.fixedwidth))
    spec.fixedwidth == -1 && return cld(n, 8)
    # Byte-addressed data follows its offsets (validity, offsets, data).
    (spec.offsetwidth > 0 && length(previous) == 2) || return Int64(-1)
    n == 0 && return Int64(0)
    offsets = previous[2]
    w = spec.offsetwidth
    offsets.len >= (n + 1) * w || return Int64(-1)
    last =
        w == 8 ? AC.loadat(offsets, Int64, n * 8) : Int64(AC.loadat(offsets, Int32, n * 4))
    return last >= 0 ? last : Int64(-1)
end

# The end of the last value each variadic data buffer of a view column holds,
# from its views: what a compressed data buffer's column reads. `nothing` for
# an uncompressed batch, whose buffers are not decompressed.
function _variadicends(c::DecodeCursor, views::BufferSlice, n::Int64, nvariadic::Int)
    c.codec == CODEC_NONE && return nothing
    _chargevector!((c.state::DecodeState).budget, Int64, nvariadic, "variadic buffer ends")
    ends = fill(Int64(-1), nvariadic)
    (nvariadic == 0 || views.len < 16n) && return ends
    fill!(ends, 0)
    for i = 0:(n - 1)
        len = AC.loadat(views, Int32, 16i)
        len > 12 || continue
        k = AC.loadat(views, Int32, 16i + 8)
        off = AC.loadat(views, Int32, 16i + 12)
        (0 <= k < nvariadic && off >= 0) || continue
        ends[k + 1] = max(ends[k + 1], Int64(off) + len)
    end
    return ends
end

function decodefield(
    f::Field,
    c::DecodeCursor,
    dicts::Dict{Int64,ArrayData},
    fielddictids::IdDict{Field,Int64},
)
    t = f.type
    node = takenode!(c)
    spec = AC.layoutspec_of(t)
    n = Int64(node.length)
    buffers = BufferSlice[]
    for role in spec.buffers
        push!(buffers, takebuffer!(c, _neededbytes(role, spec, n, buffers)))
    end
    if spec.variadic
        # View layouts append their declared count of variadic data buffers
        # after the fixed validity/views pair (format 1.4).
        nvariadic = takevariadic!(c)
        ends = _variadicends(c, buffers[end], n, nvariadic)
        for k = 1:nvariadic
            push!(buffers, takebuffer!(c, ends === nothing ? Int64(-1) : ends[k]))
        end
    end
    for (role, buffer) in zip(spec.buffers, buffers)
        # A zero-length array may omit its offsets buffer entirely; Core
        # accepts that canonical empty form, and nanoarrow and C++ write it.
        # A PARTIAL offsets buffer — nonempty but short of one slot — is
        # still malformed framing.
        if role == AC.OFFSETS && node.length == 0 && 0 < buffer.len < spec.offsetwidth
            throw(ValidationError("IPC offsets buffer is shorter than one offset slot"))
        end
    end
    children = ArrayData[]
    if t isa DictionaryType
        # Index buffers were just consumed; values come from the side table.
        id = fielddictids[f]
        haskey(dicts, id) || throw(
            ValidationError(
                "record batch references dictionary id $id before its dictionary batch",
            ),
        )
        return ArrayData(
            t,
            node.length,
            buffers;
            dictionary=dicts[id],
            nullcount=node.null_count,
        )
    end
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        push!(children, decodefield(f.children[i], c, dicts, fielddictids))
    end
    if t isa UnionType && t.mode == AC.SparseMode
        all(child -> child.len == node.length, children) ||
            throw(ValidationError("IPC sparse-union children must equal the union length"))
    end
    return ArrayData(t, node.length, buffers; children=children, nullcount=node.null_count)
end

function validaterecordcolumns(
    fields,
    cols,
    validated_dictionaries::AC._ValidatedDictionaries,
)
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

"""
The sequential half of decoding a record batch: its codec, its length, and
where each top-level field's nodes and buffers start. A metadata-only walk
(`skipfield!`) checks the whole node and buffer table first; the fields then
decode independently (`_decodeplanned`), in any order or concurrently.
"""
struct _RecordPlan{B}
    header::Meta.RecordBatch
    body::B
    codec::Int8
    variadics::Vector{Int64}
    rblen::Int64
    # Cursor (node, buffer, variadic count, last buffer end) at each field.
    starts::Vector{NTuple{4,Int64}}
end

function _recordplan(header::Meta.RecordBatch, body, version::Int16, fields, limits::Limits)
    codec = _batchcodec(header.compression, version)
    rblen = something(header.length, Int64(0))
    rblen >= 0 || throw(ValidationError("record batch length $rblen is negative"))
    rblen <= limits.max_array_length || _limiterror(
        "record batch length $rblen",
        "max_array_length",
        limits.max_array_length,
    )
    variadics = variadiccounts(header)
    c = DecodeCursor(
        header.nodes,
        header.buffers,
        body,
        limits;
        codec=codec,
        variadics=variadics,
    )
    starts = Vector{NTuple{4,Int64}}(undef, length(fields))
    for (j, f) in enumerate(fields)
        starts[j] = (c.nodeidx, c.bufidx, c.varidx, c.last_nonempty_end)
        skipfield!(f, c)
    end
    finishcursor!(c)
    return _RecordPlan(header, body, codec, variadics, rblen, starts)
end

"Decode and validate field `j` of a planned record batch."
function _decodeplanned(
    p::_RecordPlan,
    j::Int,
    f::Field,
    dicts::Dict{Int64,ArrayData},
    fielddictids::IdDict{Field,Int64},
    limits::Limits,
    validated_dictionaries,
    state::DecodeState,
)
    c = DecodeCursor(
        p.header.nodes,
        p.header.buffers,
        p.body,
        limits;
        codec=p.codec,
        state=state,
        variadics=p.variadics,
    )
    c.nodeidx, c.bufidx, c.varidx, c.last_nonempty_end = p.starts[j]
    col = decodefield(f, c, dicts, fielddictids)
    # One IPC dictionary id can back many fields. Each immutable pool
    # snapshot was certified at its DictionaryBatch, so semantic validation
    # skips that pool tree; index contracts still run for every field.
    AC._validate_semantic(f, col, validated_dictionaries)
    col.len == p.rblen ||
        throw(ValidationError("RecordBatch length does not match top-level field nodes"))
    return col
end

function decoderecord(
    fm::FramedMessage,
    fields,
    sch::Schema,
    dicts::Dict{Int64,ArrayData},
    fielddictids::IdDict{Field,Int64},
    limits::Limits,
    validated_dictionaries,
    state::DecodeState,
)
    p = _recordplan(fm.msg.header::Meta.RecordBatch, fm.body, fm.version, fields, limits)
    cols = ArrayData[
        _decodeplanned(p, j, f, dicts, fielddictids, limits, validated_dictionaries, state) for (j, f) in enumerate(fields)
    ]
    return AC.RecordBatch(sch, cols, p.rblen, validated_dictionaries)
end

"""
Arrow 0.17 V4 streams signaled buffer compression on the Message, before
RecordBatch.compression existed. Reject that legacy marker before treating
its length-prefixed compressed buffers as raw data.
"""
function rejectexperimentalcompression(
    msg::Meta.Message,
    version::Int16,
    header_type::UInt8,
)
    version == Int16(3) || return nothing # V4
    header_type in (UInt8(2), UInt8(3)) || return nothing
    metadata = msg.custom_metadata
    metadata === nothing && return nothing
    any(kv -> kv.key == EXPERIMENTAL_COMPRESSION_KEY, metadata) && throw(
        ValidationError(
            "pre-1.0 experimental V4 IPC compression (the " *
            "ARROW:experimental_compression metadata convention, superseded " *
            "by V5 BodyCompression in 2020) is not supported",
        ),
    )
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
    region::OwnerRegion                  # one lifetime root for the wire bytes
    nextindex::Int
    @atomic pulling::Bool
    fielddictids::IdDict{Field,Int64}   # adapter-side id table (shared ids preserved)
    dictionaries::Dict{Int64,ArrayData} # final pools, including updates after the last record
    budget::AllocationBudget           # framing, decode, and facade materialization
    limits::Limits                     # complete policy used when this handle opened
end

AC.schema(s::IPCStream) = s.schema

function AC.nextbatch!(s::IPCStream)
    # One active pull at a time: the claim CAS rejects concurrent callers
    # (fail closed, no duplicated or skipped batches) and is released on
    # every exit path.
    _, ok = @atomicreplace s.pulling false => true
    ok || throw(
        Base.ConcurrencyViolationError(
            "IPCStream supports only one active nextbatch! call",
        ),
    )
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
BodyCompression is V5-only; a V4 batch that declares it is refused.
"""
function _batchcodec(compression, version::Int16)::Int8
    compression === nothing && return CODEC_NONE
    version == Int16(4) || throw(ValidationError("BodyCompression requires metadata V5"))
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
    _readstream(bytes, limits, _readbudget(limits, length(bytes)))

"""
The schema of an IPC stream from its first message: the Core schema and
fields, the dictionary ids, and each dictionary's value field.
"""
function _streamschema(fm::FramedMessage)
    fm.header_type == 1 || # Schema
        throw(ValidationError("first IPC message must be a schema"))
    fm.msg.header isa Meta.Schema ||
        throw(ValidationError("first IPC message must be a schema"))
    metaschema = fm.msg.header
    fm.body.len == 0 || throw(ValidationError("schema message must have an empty body"))
    endian = something(metaschema.endianness, Meta.Endianness.Little)
    endian == Meta.Endianness.Little || throw(
        ValidationError("big-endian IPC is not supported (no endianness normalization)"),
    )
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()   # adapter-side id table
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
    return sch, fields, dictids, fielddictids, dictvaluefields
end

"""
The decoding state of an IPC stream after its schema: the dictionaries as of
the last message applied and the stream's metadata version.
"""
struct _StreamState
    schema::Schema
    fields::Vector{Field}
    dictids::Dict{Int64,Meta.Field}
    fielddictids::IdDict{Field,Int64}
    dictvaluefields::Dict{Int64,Field}
    dicts::Dict{Int64,ArrayData}
    validated::AC._ValidatedDictionaries
    version::Int16
    limits::Limits
end

function _StreamState(fm::FramedMessage, limits::Limits)
    sch, fields, dictids, fielddictids, dictvaluefields = _streamschema(fm)
    return _StreamState(
        sch,
        fields,
        dictids,
        fielddictids,
        dictvaluefields,
        Dict{Int64,ArrayData}(),
        AC._ValidatedDictionaries(),
        fm.version,
        limits,
    )
end

# A message after the schema: its version, and the legacy V4 compression
# marker (see rejectexperimentalcompression).
function _checkstreammessage(st::_StreamState, fm::FramedMessage)
    fm.version == st.version ||
        throw(ValidationError("IPC metadata version changes within the stream"))
    rejectexperimentalcompression(fm)
    return nothing
end

"""
Decode dictionary batch `fm` and apply it to the stream's dictionaries (a
first pool, a delta, or a replacement). Returns its id, the pool now in
effect, and whether the id already had one.
"""
function _applydictionary!(
    st::_StreamState,
    fm::FramedMessage,
    budget::AllocationBudget,
    state::DecodeState,
)
    header = fm.msg.header::Meta.DictionaryBatch
    rb = header.data
    codec = _batchcodec(rb.compression, fm.version)
    haskey(st.dictids, header.id) ||
        throw(ValidationError("dictionary batch has unknown id $(header.id)"))
    replacement = haskey(st.dicts, header.id)
    _dictionarytransition(header.id, header.isDelta, replacement)
    # A dictionary batch's payload is a one-column record batch of the VALUE
    # type, so the generic decoder handles it. Pool nullability is
    # independent of the encoded index field.
    haskey(st.dictvaluefields, header.id) ||
        throw(ValidationError("dictionary batch has unknown id $(header.id)"))
    vf = st.dictvaluefields[header.id]
    rblen = something(rb.length, Int64(0))
    rblen >= 0 || throw(ValidationError("dictionary batch length $rblen is negative"))
    rblen <= st.limits.max_array_length || _limiterror(
        "dictionary batch length $rblen",
        "max_array_length",
        st.limits.max_array_length,
    )
    cursor = DecodeCursor(
        rb.nodes,
        rb.buffers,
        fm.body,
        st.limits;
        codec=codec,
        state=state,
        variadics=variadiccounts(rb),
    )
    decoded = decodefield(vf, cursor, st.dicts, st.fielddictids)
    finishcursor!(cursor)
    decoded.len == rblen || throw(
        ValidationError("dictionary RecordBatch length does not match its field node"),
    )
    decoded = _updatedictionary!(
        st.dicts,
        st.validated,
        header.id,
        header.isDelta,
        vf,
        decoded,
        st.limits,
        budget,
    )
    return header.id, decoded, replacement
end

"""
A record batch of a stream, planned against the dictionaries in effect at its
position. The IPC format lets a batch whose dictionary column is all null
arrive before that dictionary; such a batch waits (`missing` is not empty)
until the dictionary arrives, and then decodes with it.
"""
mutable struct PendingRecord
    plan::_RecordPlan{BufferSlice}
    dictionaries::Dict{Int64,ArrayData}
    missing::Set{Int64}
    budget::AllocationBudget
end

function _pendingrecord(st::_StreamState, fm::FramedMessage, budget::AllocationBudget)
    header = fm.msg.header::Meta.RecordBatch
    missing = missingdicts(st.fields, header.nodes, st.dicts, st.fielddictids)
    plan = _recordplan(header, fm.body, fm.version, st.fields, st.limits)
    return PendingRecord(plan, copy(st.dicts), missing, budget)
end

# A dictionary that arrived resolves the records waiting for it.
function _resolvepending!(queue, id::Int64, pool::ArrayData)
    for p in queue
        p isa PendingRecord && id in p.missing || continue
        p.dictionaries[id] = pool
        delete!(p.missing, id)
    end
    return queue
end

"""
Decode the fields of planned records `ps` (every one resolved) on at most
`ntasks` tasks, one (record, field) pair at a time.
"""
function _decoderecords(st::_StreamState, ps::Vector{PendingRecord}, ntasks::Int)
    isempty(ps) && return AC.RecordBatch[]
    nf = length(st.fields)
    cols = [Vector{ArrayData}(undef, nf) for _ in ps]
    # One codec state per task, charging the budget of the record at hand.
    states = [DecodeState(first(ps).budget) for _ = 1:min(ntasks, length(ps) * nf)]
    try
        _foreach(length(ps) * nf, ntasks) do item, w
            k, j = divrem(item - 1, nf) .+ (1, 1)
            p = ps[k]
            state = states[w]
            state.budget = p.budget
            cols[k][j] = _decodeplanned(
                p.plan,
                j,
                st.fields[j],
                p.dictionaries,
                st.fielddictids,
                st.limits,
                st.validated,
                state,
            )
            return nothing
        end
    finally
        foreach(close, states)
    end
    return AC.RecordBatch[
        AC.RecordBatch(st.schema, cols[k], p.plan.rblen, st.validated) for
        (k, p) in enumerate(ps)
    ]
end

function _readstream(
    bytes::Vector{UInt8},
    limits::Limits,
    budget::AllocationBudget,
    ntasks::Int=1,
)
    region = heapregion(bytes)
    msgs = _framemessages(region, limits, Base.ENDIAN_BOM, budget)
    isempty(msgs) && throw(ValidationError("empty IPC stream"))
    st = _StreamState(msgs[1], limits)
    # One codec context per reader for the dictionary batches, which apply in
    # order; record batches are planned in order and decoded together after.
    state = DecodeState(budget)
    records = PendingRecord[]
    try
        for fm in msgs[2:end]
            _checkstreammessage(st, fm)
            header = fm.msg.header
            if header isa Meta.DictionaryBatch
                id, pool, replaced = _applydictionary!(st, fm, budget, state)
                # Waiting records take an id's first pool only; a later
                # replacement is for the records after it.
                replaced || _resolvepending!(records, id, pool)
            elseif header isa Meta.RecordBatch
                push!(records, _pendingrecord(st, fm, budget))
            else
                throw(ValidationError("unsupported IPC message header $(typeof(header))"))
            end
        end
    finally
        close(state)
    end
    all(p -> isempty(p.missing), records) ||
        throw(ValidationError("stream ended before required dictionary batches arrived"))
    return IPCStream(
        st.schema,
        AC.FrozenVector{Field}(st.fields),
        _decoderecords(st, records, ntasks),
        region,
        1,
        false,
        st.fielddictids,
        st.dicts,
        budget,
        limits,
    )
end

# --- incremental streams ----------------------------------------------------

"""
The messages of an IPC stream, framed one at a time from a byte region (a
byte vector or a memory map) as the reader reaches them.
"""
mutable struct _RegionMessages
    const blob::BufferSlice
    pos::Int64
end
_RegionMessages(region::OwnerRegion) =
    _RegionMessages(BufferSlice(region, 0, region.len), 0)

function _nextmessage!(m::_RegionMessages, limits::Limits, budget::AllocationBudget)
    fm, m.pos = _framemessage(m.blob, m.pos, limits, budget, 0)
    return fm
end

"""
The messages of an IPC stream read from an `IO` one at a time: each message's
metadata and body are read when the reader reaches them, so a pipe or socket
streams with one message in memory. `head` holds bytes already read from the
`IO` (while telling the formats apart).
"""
mutable struct _IOMessages{I<:IO}
    const io::I
    head::Vector{UInt8}
    pos::Int64
end

# Read `n` bytes into `out` from index `at` on; false when the stream ends first.
function _readinto!(m::_IOMessages, out::Vector{UInt8}, at::Int, n::Int)
    k = min(length(m.head), n)
    copyto!(out, at, m.head, 1, k)
    deleteat!(m.head, 1:k)
    if k < n
        try
            GC.@preserve out unsafe_read(m.io, pointer(out, at + k), UInt(n - k))
        catch e
            e isa EOFError || rethrow()
            return false
        end
    end
    m.pos += n
    return true
end

function _readbytes!(m::_IOMessages, n::Int64)
    out = Vector{UInt8}(undef, Int(n))
    return _readinto!(m, out, 1, Int(n)) ? out : nothing
end

# A body is read in pieces of growing size, so a declared length the stream
# does not back costs only the bytes that arrive. Those bytes are the input
# itself, so they are not charged to the reader's allowance.
const _IO_BODY_PIECE = 64 * 1024 * 1024

function _readbody!(m::_IOMessages, bodylen::Int64)
    body = UInt8[]
    while length(body) < bodylen
        have = length(body)
        n = Int(min(bodylen - have, max(_IO_BODY_PIECE, have)))
        resize!(body, have + n)
        _readinto!(m, body, have + 1, n) ||
            throw(ValidationError("truncated body: need $bodylen bytes at byte $(m.pos)"))
    end
    return body
end

function _nextmessage!(m::_IOMessages, limits::Limits, budget::AllocationBudget)
    at = m.pos
    isempty(m.head) && eof(m.io) && return nothing   # the stream ended between messages
    prefix = _readbytes!(m, Int64(8))
    prefix === nothing && throw(ValidationError("truncated IPC prefix at byte $at"))
    word = reinterpret(UInt32, prefix)[1]
    word == CONTINUATION || throw(ValidationError(_nomarker(at, word)))
    metalen = Int64(ltoh(reinterpret(Int32, prefix)[2]))
    metalen == 0 && return nothing   # end-of-stream marker
    _checkmetalen(metalen, limits)
    _chargevector!(budget, UInt8, metalen, "metadata allocation")
    metabytes = _readbytes!(m, metalen)
    metabytes === nothing &&
        throw(ValidationError("truncated metadata: need $metalen bytes at $at"))
    msg, version, header_type, features, bodylen =
        _verifiedmessage(metabytes, limits, budget)
    body = _readbody!(m, bodylen)
    return FramedMessage(
        msg,
        BufferSlice(heapregion(body), 0, length(body)),
        version,
        header_type,
        features,
    )
end

# The budget a stream message frames under; its batch then decodes under one
# sized from the message's own bytes.
_framebudget(limits::Limits) = _readbudget(limits, 0)

"""
An IPC stream read incrementally: messages are read and decoded as record
batches are requested (`_nextrecord!`), each under its own allowance, so a
stream of any length reads with one batch in memory. A stream over a byte
region can start over; one over an `IO` reads once.
"""
mutable struct _StreamReader{M}
    const messages::M
    const start::Int64                 # position after the schema
    # The lifetime authority of the stream and its batches: the byte region,
    # or for an `IO` (each body is its own region) an empty one.
    const region::OwnerRegion
    st::_StreamState
    queue::Vector{PendingRecord}       # record batches read and not yet returned
    done::Bool
    failed::Bool                       # a read threw; the stream stops there
    const limits::Limits
    const lock::ReentrantLock
end

function _StreamReader(messages, limits::Limits)
    _validatelimits(limits)
    fm = _nextmessage!(messages, limits, _framebudget(limits))
    fm === nothing && throw(ValidationError("empty IPC stream"))
    st = _StreamState(fm, limits)
    region = messages isa _RegionMessages ? messages.blob.region : heapregion(UInt8[])
    return _StreamReader(
        messages,
        messages.pos,
        region,
        st,
        PendingRecord[],
        false,
        false,
        limits,
        ReentrantLock(),
    )
end

# Start over from the first batch (a byte region or a byte-range source).
function _restart!(r::_StreamReader)
    # A reader that has not read past its schema is at the start already.
    r.messages.pos == r.start && isempty(r.queue) && !r.done && !r.failed && return r
    m = r.messages
    if m isa _IOMessages{_SourceIO}
        empty!(m.head)
        m.io.pos = r.start
    elseif !(m isa _RegionMessages)
        throw(
            ArgumentError(
                "an Arrow.Stream over an IO reads its batches once; " *
                "read the IO into a byte vector to iterate it again",
            ),
        )
    end
    m.pos = r.start
    st = r.st
    r.st = _StreamState(
        st.schema,
        st.fields,
        st.dictids,
        st.fielddictids,
        st.dictvaluefields,
        Dict{Int64,ArrayData}(),
        AC._ValidatedDictionaries(),
        st.version,
        st.limits,
    )
    empty!(r.queue)
    r.done = false
    r.failed = false
    return r
end

"""
The next record batch of the stream and the allowance it decoded under, or
`nothing` at its end. Messages are read until the next batch in stream order
is decodable; its fields decode on at most `ntasks` tasks.
"""
function _nextrecord!(r::_StreamReader, ntasks::Int)
    r.failed && throw(
        ArgumentError(
            "this Arrow.Stream stopped at an error; open a new one to read the source again",
        ),
    )
    try
        return _readrecord!(r, ntasks)
    catch
        r.failed = true
        rethrow()
    end
end

function _readrecord!(r::_StreamReader, ntasks::Int)
    while true
        if !isempty(r.queue)
            p = first(r.queue)
            if isempty(p.missing)
                popfirst!(r.queue)
                return only(_decoderecords(r.st, PendingRecord[p], ntasks)), p.budget
            end
        end
        if r.done
            isempty(r.queue) && return nothing
            throw(
                ValidationError("stream ended before required dictionary batches arrived"),
            )
        end
        fm = _nextmessage!(r.messages, r.limits, _framebudget(r.limits))
        if fm === nothing
            r.done = true
            continue
        end
        _checkstreammessage(r.st, fm)
        budget = _readbudget(r.limits, fm.body.len)
        header = fm.msg.header
        if header isa Meta.DictionaryBatch
            state = DecodeState(budget)
            try
                id, pool, replaced = _applydictionary!(r.st, fm, budget, state)
                replaced || _resolvepending!(r.queue, id, pool)
            finally
                close(state)
            end
        elseif header isa Meta.RecordBatch
            push!(r.queue, _pendingrecord(r.st, fm, budget))
        else
            throw(ValidationError("unsupported IPC message header $(typeof(header))"))
        end
    end
end

"The number of record batches of a region stream, from its framing alone."
function _countrecords(r::_StreamReader{_RegionMessages})
    m = _RegionMessages(r.messages.blob, r.start)
    n = 0
    while true
        fm = _nextmessage!(m, r.limits, _framebudget(r.limits))
        fm === nothing && return n
        fm.msg.header isa Meta.RecordBatch && (n += 1)
    end
end
