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
# The C data interface and C stream interface adapter over ArrowCore.
#
# `ArrayData` has the shape of the C `ArrowArray` (buffers + children +
# dictionary + length/null_count/offset), so export is struct filling and
# import is struct reading.
#
#   * Export: ONE release callback per C structure (never per buffer). A
#     parent callback releases each child/dictionary that has not been moved;
#     a moved child keeps the shared export allocation alive until its own
#     callback runs. `private_data` points to a per-node malloc'd,
#     never-GC-scanned CONTROL BLOCK holding an exactly-once state and the
#     registry key. The Julia-side owner (which roots the Core columns and
#     every malloc'd C struct) stays in a global EXPORT REGISTRY until
#     release — a raw pointer in private_data roots nothing by itself. The
#     @cfunction release callback recursively marks the C tree released.
#     Callback traversal uses producer-owned canonical child/dictionary
#     topology, not the caller-visible counts and pointer tables. It still
#     reads each canonical descendant's public release field so conforming
#     moves are honored. A reaper pass (`reap!`) scans for aggregates whose
#     last outstanding node was released, frees mallocs, and drops the
#     registry root — dropping the root is what lets the source columns (and,
#     through their OwnerRegion roots, the actual buffer memory) become
#     collectable again. Callback contract: releases for one tree are
#     serialized and run only on Julia-attached threads.
#
#   * Import: the moved ArrowArray becomes ONE ForeignOwner shared by every
#     child/dictionary BufferSlice (a single release for the whole tree —
#     per-buffer owners would double-release). Buffer extents are DECLARED,
#     not verified: the ABI cannot prove allocation sizes, so extents are
#     computed from length/offset/layout, and offsets buffers are read
#     (bounded by their computed size) to size the data buffers they govern.
#     Failed imports release the moved structure exactly once before
#     throwing. Per spec, moving marks the source released (release = NULL).
#     Validity is reachability: every imported region's `root` is the
#     ForeignOwner, so the producer's memory outlives every slice by
#     construction. Every region over one import shares one `ReleaseCell`, so
#     `close!` on any of them revokes all siblings and then runs the
#     producer's release exactly once; the raw `release!` skips revocation,
#     and touching the tree after it is the C Data spec's own post-release
#     undefined behavior.
#
#   * Streams: `ArrowArrayStream` maps in both directions with one
#     independently-owned export root per result and exception-safe
#     move/release handoffs.
# =============================================================================
# =============================================================================


# ---------------------------------------------------------------------------
# ABI structs (field-exact per https://arrow.apache.org/docs/format/CDataInterface.html)
# ---------------------------------------------------------------------------

struct CArrowSchema
    format::Ptr{UInt8}
    name::Ptr{UInt8}
    metadata::Ptr{UInt8}
    flags::Int64
    n_children::Int64
    children::Ptr{Ptr{CArrowSchema}}
    dictionary::Ptr{CArrowSchema}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

struct CArrowArray
    length::Int64
    null_count::Int64
    offset::Int64
    n_buffers::Int64
    n_children::Int64
    buffers::Ptr{Ptr{Cvoid}}
    children::Ptr{Ptr{CArrowArray}}
    dictionary::Ptr{CArrowArray}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

const ARROW_FLAG_NULLABLE = Int64(2)
const ARROW_FLAG_DICTIONARY_ORDERED = Int64(1)
const ARROW_FLAG_MAP_KEYS_SORTED = Int64(4)
const ARROW_FLAG_ALL_SUPPORTED = ARROW_FLAG_NULLABLE |
    ARROW_FLAG_DICTIONARY_ORDERED | ARROW_FLAG_MAP_KEYS_SORTED

# ---------------------------------------------------------------------------
# Format strings <-> Core descriptors (parity with Core's accessor set)
# ---------------------------------------------------------------------------

_tuchar(u) = u == AC.SECOND ? "s" : u == AC.MILLISECOND ? "m" :
    u == AC.MICROSECOND ? "u" : "n"

formatstring(t::IntType) =
    (t.signed ? Dict(8 => "c", 16 => "s", 32 => "i", 64 => "l") :
     Dict(8 => "C", 16 => "S", 32 => "I", 64 => "L"))[t.bits]
formatstring(t::FloatType) = Dict(16 => "e", 32 => "f", 64 => "g")[t.bits]
formatstring(::BoolType) = "b"
formatstring(::NullType) = "n"
formatstring(t::Utf8Type) = t.large ? "U" : "u"
formatstring(t::BinaryType) = t.large ? "Z" : "z"
formatstring(t::FixedSizeBinaryType) = "w:$(t.nbytes)"
formatstring(t::DecimalType) =
    t.bits == 128 ? "d:$(t.precision),$(t.scale)" :
    "d:$(t.precision),$(t.scale),$(t.bits)"
formatstring(t::DateType) = t.unit == AC.DAY ? "tdD" : "tdm"
formatstring(t::TimeType) = "tt" * _tuchar(t.unit)
formatstring(t::TimestampType) =
    "ts" * _tuchar(t.unit) * ":" * something(t.timezone, "")
formatstring(t::DurationType) = "tD" * _tuchar(t.unit)
formatstring(t::IntervalType) = t.unit == AC.YEAR_MONTH ? "tiM" :
    t.unit == AC.DAY_TIME ? "tiD" : "tin"
formatstring(t::ListType) = t.large ? "+L" : "+l"
formatstring(t::FixedSizeListType) = "+w:$(t.listsize)"
formatstring(::StructType) = "+s"
formatstring(::MapType) = "+m"
formatstring(t::UnionType) =
    (t.mode == AC.SparseMode ? "+us:" : "+ud:") * join(Int.(t.typeids), ",")
formatstring(t::ViewType) = t.utf8 ? "vu" : "vz"
formatstring(t::ListViewType) = t.large ? "+vL" : "+vl"
formatstring(::RunEndEncodedType) = "+r"
formatstring(t::DictionaryType) = formatstring(t.indextype)  # per spec: index format; values on schema.dictionary

# Closed-set ladder (same devirtualization story as AC.layoutspec_of): the
# export walk reaches this with an abstract-typed Field slot.
@inline function formatstring_of(t::ArrowType)::String
    t isa IntType && return formatstring(t)
    t isa FloatType && return formatstring(t)
    t isa BoolType && return formatstring(t)
    t isa NullType && return formatstring(t)
    t isa Utf8Type && return formatstring(t)
    t isa BinaryType && return formatstring(t)
    t isa FixedSizeBinaryType && return formatstring(t)
    t isa DecimalType && return formatstring(t)
    t isa DateType && return formatstring(t)
    t isa TimeType && return formatstring(t)
    t isa TimestampType && return formatstring(t)
    t isa DurationType && return formatstring(t)
    t isa IntervalType && return formatstring(t)
    t isa ListType && return formatstring(t)
    t isa FixedSizeListType && return formatstring(t)
    t isa StructType && return formatstring(t)
    t isa MapType && return formatstring(t)
    t isa UnionType && return formatstring(t)
    t isa ViewType && return formatstring(t)
    t isa ListViewType && return formatstring(t)
    t isa RunEndEncodedType && return formatstring(t)
    t isa DictionaryType && return formatstring_of(t.indextype)
    throw(ArgumentError("unregistered ArrowType"))
end

_formaterror(fmt) = throw(ValidationError(
    "unsupported C format string \"$fmt\""))

function _parseformatint(fmt, s, what; low=0, high=typemax(Int32))
    bytes = codeunits(s)
    isempty(bytes) &&
        throw(ValidationError("invalid $what in C format string \"$fmt\""))
    firstdigit = 1
    if bytes[1] == UInt8('-')
        low < 0 ||
            throw(ValidationError("invalid $what in C format string \"$fmt\""))
        length(bytes) > 1 ||
            throw(ValidationError("invalid $what in C format string \"$fmt\""))
        firstdigit = 2
    end
    for i = firstdigit:length(bytes)
        UInt8('0') <= bytes[i] <= UInt8('9') ||
            throw(ValidationError("invalid $what in C format string \"$fmt\""))
    end
    n = tryparse(Int64, s)
    (n === nothing || !(low <= n <= high)) &&
        throw(ValidationError("invalid $what in C format string \"$fmt\""))
    return Int(n)
end

_parsetimeunit(fmt, c) = c == UInt8('s') ? AC.SECOND :
    c == UInt8('m') ? AC.MILLISECOND :
    c == UInt8('u') ? AC.MICROSECOND :
    c == UInt8('n') ? AC.NANOSECOND : _formaterror(fmt)

function _parseunionids(fmt, body)
    ids = Int8[]
    isempty(body) && return ids
    # A valid Int8-domain union has at most 128 children. Count separators
    # without splitting so an overlong malformed string cannot direct a large
    # temporary allocation before it is rejected.
    nids = 1
    for b in codeunits(body)
        b == UInt8(',') || continue
        nids += 1
        nids <= 128 ||
            throw(ValidationError("union C format string declares more than 128 type ids"))
    end
    sizehint!(ids, nids)
    seen = UInt128(0)
    value = 0
    have_digit = false
    for b in codeunits(body)
        if UInt8('0') <= b <= UInt8('9')
            have_digit = true
            value = 10 * value + Int(b - UInt8('0'))
            value <= 127 ||
                throw(ValidationError("union type ids must be in [0, 127]"))
        elseif b == UInt8(',')
            have_digit ||
                throw(ValidationError("invalid union type id in C format string \"$fmt\""))
            bit = UInt128(1) << value
            seen & bit == 0 ||
                throw(ValidationError("union type ids must be unique"))
            push!(ids, Int8(value))
            seen |= bit
            value = 0
            have_digit = false
        else
            throw(ValidationError("invalid union type id in C format string \"$fmt\""))
        end
    end
    have_digit ||
        throw(ValidationError("invalid union type id in C format string \"$fmt\""))
    bit = UInt128(1) << value
    seen & bit == 0 || throw(ValidationError("union type ids must be unique"))
    push!(ids, Int8(value))
    return ids
end

function parseformat(fmt::AbstractString, flags::Int64=0)::ArrowType
    isvalid(fmt) || throw(ValidationError("C format string is not valid UTF-8"))
    occursin('\0', fmt) &&
        throw(ValidationError("C format string cannot contain embedded NUL characters"))
    fmt = String(fmt)
    fmt == "b" && return BoolType()
    fmt == "n" && return NullType()
    fmt == "u" && return Utf8Type(false)
    fmt == "U" && return Utf8Type(true)
    fmt == "z" && return BinaryType(false)
    fmt == "Z" && return BinaryType(true)
    fmt == "+l" && return ListType(false)
    fmt == "+L" && return ListType(true)
    fmt == "vu" && return ViewType(true)
    fmt == "vz" && return ViewType(false)
    fmt == "+vl" && return ListViewType(false)
    fmt == "+vL" && return ListViewType(true)
    fmt == "+r" && return RunEndEncodedType()
    fmt == "+s" && return StructType()
    fmt == "+m" && return MapType((flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0)
    fmt == "e" && return FloatType(16)
    fmt == "f" && return FloatType(32)
    fmt == "g" && return FloatType(64)
    fmt == "tdD" && return DateType(AC.DAY)
    fmt == "tdm" && return DateType(AC.MILLISECOND_DATE)
    fmt == "tiM" && return IntervalType(AC.YEAR_MONTH)
    fmt == "tiD" && return IntervalType(AC.DAY_TIME)
    fmt == "tin" && return IntervalType(AC.MONTH_DAY_NANO)
    m = Dict("c" => (8, true), "C" => (8, false), "s" => (16, true), "S" => (16, false),
        "i" => (32, true), "I" => (32, false), "l" => (64, true), "L" => (64, false))
    haskey(m, fmt) && return IntType(m[fmt]...)
    if ncodeunits(fmt) == 3 && startswith(fmt, "tt")
        u = _parsetimeunit(fmt, codeunit(fmt, 3))
        return TimeType(u, u == AC.SECOND || u == AC.MILLISECOND ? 32 : 64)
    end
    ncodeunits(fmt) == 3 && startswith(fmt, "tD") &&
        return DurationType(_parsetimeunit(fmt, codeunit(fmt, 3)))
    if startswith(fmt, "ts") && ncodeunits(fmt) >= 4 &&
        codeunit(fmt, 4) == UInt8(':')
        u = _parsetimeunit(fmt, codeunit(fmt, 3))
        tz = SubString(fmt, 5)
        return TimestampType(u, isempty(tz) ? nothing : String(tz))
    end
    if startswith(fmt, "w:")
        return FixedSizeBinaryType(_parseformatint(fmt, fmt[3:end], "byte width"))
    end
    if startswith(fmt, "+w:")
        return FixedSizeListType(_parseformatint(fmt, fmt[4:end], "list size"))
    end
    if startswith(fmt, "d:")
        parts = split(fmt[3:end], ","; limit=4, keepempty=true)
        2 <= length(parts) <= 3 ||
            throw(ValidationError("invalid decimal C format string \"$fmt\""))
        precision = _parseformatint(fmt, parts[1], "decimal precision")
        scale = _parseformatint(fmt, parts[2], "decimal scale";
            low=typemin(Int32))
        bits = length(parts) == 3 ?
            _parseformatint(fmt, parts[3], "decimal bit width") : 128
        t = DecimalType(precision, scale, bits)
        AC._validate_descriptor(t)
        return t
    end
    startswith(fmt, "+us:") &&
        return UnionType(AC.SparseMode, _parseunionids(fmt, fmt[5:end]))
    startswith(fmt, "+ud:") &&
        return UnionType(AC.DenseMode, _parseunionids(fmt, fmt[5:end]))
    _formaterror(fmt)
end

# ---------------------------------------------------------------------------
# Export: Core -> C structs, per-node controls + registry + explicit reaper
# ---------------------------------------------------------------------------

# Per-node control block layout (malloc'd, never GC-scanned):
#   offset 0: UInt8 state (0 = live, 1 = releasing, 2 = released)
#   offset 8: Int64 registry key
const CONTROL_BLOCK_BYTES = 16

"""
Everything one export tree must keep alive and eventually free: the Core
columns (whose OwnerRegions root the actual buffers), every malloc'd C struct
and string, and every per-node control block. Held in EXPORT_REGISTRY under
their shared aggregate key until all non-moved and moved nodes have been
released and the reaper runs. Rooting the columns here is the entire
source-liveness story: raw C pointers handed to a consumer stay valid because
the registry keeps this object and all source regions reachable.
"""
mutable struct ExportedRoot
    roots::Vector{Any}          # ArrayData/Field/Schema kept reachable
    mallocs::Vector{Ptr{Cvoid}} # every Libc.malloc'd allocation, freed on reap
    key::Int64
    remaining::Int64           # exported C nodes whose callback has not run
    schema_topology::Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowSchema}},Ptr{CArrowSchema}}}
    array_topology::Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowArray}},Ptr{CArrowArray}}}
end

const EXPORT_REGISTRY = Dict{Int64,ExportedRoot}()
const REGISTRY_LOCK = ReentrantLock()
const NEXT_KEY = Ref{Int64}(0)
function _claim_array_node(a::Ptr{CArrowArray}, claimed_slot)
    a == C_NULL && return nothing
    return lock(REGISTRY_LOCK) do
        arr = unsafe_load(a)
        arr.release == C_NULL && return nothing
        p = arr.private_data
        p == C_NULL && return nothing
        key = unsafe_load(Ptr{Int64}(p + 8))
        root = get(EXPORT_REGISTRY, key, nothing)
        root === nothing && error("C Data export root disappeared during release")
        topology = get(root.array_topology, p, nothing)
        topology === nothing && error("C Data array topology disappeared during release")
        flag = unsafe_load(Ptr{UInt8}(p))
        flag == 0x00 || return nothing
        claimed = (p, topology)
        claimed_slot[] = claimed
        unsafe_store!(Ptr{UInt8}(p), 0x01)
        return claimed
    end
end

function _claim_schema_node(s::Ptr{CArrowSchema}, claimed_slot)
    s == C_NULL && return nothing
    return lock(REGISTRY_LOCK) do
        sch = unsafe_load(s)
        sch.release == C_NULL && return nothing
        p = sch.private_data
        p == C_NULL && return nothing
        key = unsafe_load(Ptr{Int64}(p + 8))
        root = get(EXPORT_REGISTRY, key, nothing)
        root === nothing && error("C Data export root disappeared during release")
        topology = get(root.schema_topology, p, nothing)
        topology === nothing && error("C Data schema topology disappeared during release")
        flag = unsafe_load(Ptr{UInt8}(p))
        flag == 0x00 || return nothing
        claimed = (p, topology)
        claimed_slot[] = claimed
        unsafe_store!(Ptr{UInt8}(p), 0x01)
        return claimed
    end
end

function _finish_node!(p, control::Ptr{Cvoid}, claimed_slot, committed_slot)
    # This locked block is the callback's final access to export-owned memory.
    # The reaper observes zero only after every non-moved descendant callback,
    # and every independently moved node callback, has completed. Scanning in
    # reap! keeps allocation and registry removal out of the C callback.
    lock(REGISTRY_LOCK) do
        unsafe_load(Ptr{UInt8}(control)) == 0x01 ||
            error("C Data node is not in releasing state")
        key = unsafe_load(Ptr{Int64}(control + 8))
        root = get(EXPORT_REGISTRY, key, nothing)
        root === nothing && error("C Data export root disappeared during release")
        root.remaining > 0 || error("C Data export node counter underflow")
        oldremaining = root.remaining
        oldrelease = unsafe_load(p).release
        try
            root.remaining = oldremaining - 1
            unsafe_store!(Ptr{UInt8}(control), 0x02)
            _store_field!(p, Val(:release), Ptr{Cvoid}(C_NULL))
            # The outer catch must not touch `control` once remaining is
            # zero: a reaper may free it as soon as this lock is released.
            # Transfer the completed claim while the lock still excludes
            # cleanup. A later exception observes a committed callback.
            committed_slot[] = true
            claimed_slot[] = nothing
        catch
            if !committed_slot[]
                # Nothing can reap this root while the registry lock is held.
                # Restore the whole commit before the outer transaction
                # returns the node from RELEASING to LIVE.
                root.remaining = oldremaining
                unsafe_store!(Ptr{UInt8}(control), 0x01)
                _store_field!(p, Val(:release), oldrelease)
            end
            rethrow()
        end
    end
    return nothing
end

function _reset_node_claim!(control::Ptr{Cvoid})
    lock(REGISTRY_LOCK) do
        flag = unsafe_load(Ptr{UInt8}(control))
        flag == 0x01 || return nothing
        unsafe_store!(Ptr{UInt8}(control), 0x00)
    end
    return nothing
end

function _release_array_children!(topology)
    children, dictionary = topology
    for child in children
        release = lock(REGISTRY_LOCK) do
            unsafe_load(child).release
        end
        if release != C_NULL
            ccall(release, Cvoid, (Ptr{CArrowArray},), child)
            lock(REGISTRY_LOCK) do
                unsafe_load(child).release == C_NULL ||
                    error("C Data child array release did not complete")
            end
        end
    end
    if dictionary != C_NULL
        release = lock(REGISTRY_LOCK) do
            unsafe_load(dictionary).release
        end
        if release != C_NULL
            ccall(release, Cvoid, (Ptr{CArrowArray},), dictionary)
            lock(REGISTRY_LOCK) do
                unsafe_load(dictionary).release == C_NULL ||
                    error("C Data dictionary array release did not complete")
            end
        end
    end
    return nothing
end

function _release_schema_children!(topology)
    children, dictionary = topology
    for child in children
        release = lock(REGISTRY_LOCK) do
            unsafe_load(child).release
        end
        if release != C_NULL
            ccall(release, Cvoid, (Ptr{CArrowSchema},), child)
            lock(REGISTRY_LOCK) do
                unsafe_load(child).release == C_NULL ||
                    error("C Data child schema release did not complete")
            end
        end
    end
    if dictionary != C_NULL
        release = lock(REGISTRY_LOCK) do
            unsafe_load(dictionary).release
        end
        if release != C_NULL
            ccall(release, Cvoid, (Ptr{CArrowSchema},), dictionary)
            lock(REGISTRY_LOCK) do
                unsafe_load(dictionary).release == C_NULL ||
                    error("C Data dictionary schema release did not complete")
            end
        end
    end
    return nothing
end

function _release_array(a::Ptr{CArrowArray})
    committed_slot = Ref(false)
    claimed_slot = Ref{Union{Nothing,Tuple{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowArray}},Ptr{CArrowArray}}}}}(nothing)
    try
        claimed = _claim_array_node(a, claimed_slot)
        claimed === nothing && return nothing
        control, topology = claimed
        _release_array_children!(topology)
        _finish_node!(a, control, claimed_slot, committed_slot)
    catch
        # A C release callback has no error channel, and a Julia exception
        # must not unwind through the C ABI. Completed descendants are
        # already NULL. Restore this node to LIVE so a later explicit call
        # can resume without double release.
        if !committed_slot[]
            claimed = claimed_slot[]
            claimed === nothing || _reset_node_claim!(claimed[1])
        end
    end
    return nothing
end

function _release_schema(s::Ptr{CArrowSchema})
    committed_slot = Ref(false)
    claimed_slot = Ref{Union{Nothing,Tuple{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowSchema}},Ptr{CArrowSchema}}}}}(nothing)
    try
        claimed = _claim_schema_node(s, claimed_slot)
        claimed === nothing && return nothing
        control, topology = claimed
        _release_schema_children!(topology)
        _finish_node!(s, control, claimed_slot, committed_slot)
    catch
        if !committed_slot[]
            claimed = claimed_slot[]
            claimed === nothing || _reset_node_claim!(claimed[1])
        end
    end
    return nothing
end

# Store one field of a C struct in place (structs are immutable in Julia;
# the C memory is not).
@generated function _store_field!(p::Ptr{T}, ::Val{name}, v) where {T,name}
    i = findfirst(==(name), fieldnames(T))
    off = fieldoffset(T, i)
    FT = fieldtype(T, i)
    return :(unsafe_store!(Ptr{$FT}(Ptr{Cvoid}(p) + $off), convert($FT, v)); nothing)
end
_store_field!(p, name::Symbol, v) = _store_field!(p, Val(name), v)

function _malloc!(root::ExportedRoot, n::Integer,
    register! = push!, deallocate! = Libc.free)
    n >= 0 || throw(ArgumentError("negative export allocation size"))
    n64 = Int64(n)
    # Reserve the ledger slot before acquiring native memory. After malloc,
    # either registration owns the pointer or the local catch deallocates it.
    sizehint!(root.mallocs, AC.checked_add(length(root.mallocs), 1))
    oldlen = length(root.mallocs)
    p = Ptr{Cvoid}(C_NULL)
    owned = false
    try
        p = Libc.malloc(max(n64, Int64(1)))
        p == C_NULL && throw(OutOfMemoryError())
        owned = true
        register!(root.mallocs, p)
        owned = false
    catch
        if owned
            if length(root.mallocs) == oldlen
                deallocate!(p)
                owned = false
            elseif length(root.mallocs) == oldlen + 1 &&
                    root.mallocs[end] == p
                owned = false
            else
                error("export malloc registration left an invalid ledger state")
            end
        end
        rethrow()
    end
    return Ptr{Cvoid}(p)
end

"""
Encode field metadata per the C data interface: int32 pair count, then
per pair an int32 key length, key bytes, int32 value length, value bytes
(native endian, not NUL-terminated). NULL when there is no metadata.
"""
function _cmetadata!(root::ExportedRoot,
    metadata::Union{Nothing,AC.FrozenVector{Pair{String,String}}})::Ptr{UInt8}
    metadata === nothing && return Ptr{UInt8}(C_NULL)
    n = length(metadata)
    n == 0 && return Ptr{UInt8}(C_NULL)
    buf = UInt8[]
    append!(buf, reinterpret(UInt8, Int32[Int32(n)]))
    for kv in metadata
        k = first(kv)
        v = last(kv)
        append!(buf, reinterpret(UInt8, Int32[Int32(sizeof(k))]))
        append!(buf, codeunits(k))
        append!(buf, reinterpret(UInt8, Int32[Int32(sizeof(v))]))
        append!(buf, codeunits(v))
    end
    p = Ptr{UInt8}(_malloc!(root, length(buf)))
    GC.@preserve buf unsafe_copyto!(p, pointer(buf), length(buf))
    return p
end

function _cstring!(root::ExportedRoot, s::AbstractString)
    isvalid(s) || throw(ValidationError("C Data strings must be valid UTF-8"))
    occursin('\0', s) &&
        throw(ValidationError("C Data strings cannot contain embedded NUL characters"))
    n = ncodeunits(s)
    p = Ptr{UInt8}(_malloc!(root, AC.checked_add(Int64(n), Int64(1))))
    for (i, b) in enumerate(codeunits(s))
        unsafe_store!(p, b, i)
    end
    unsafe_store!(p, 0x00, n + 1)
    return p
end

function _newcontrol!(root::ExportedRoot)
    control = _malloc!(root, CONTROL_BLOCK_BYTES)
    unsafe_store!(Ptr{UInt8}(control), 0x00)
    unsafe_store!(Ptr{Int64}(control + 8), root.key)
    root.remaining = AC.checked_add(root.remaining, Int64(1))
    return control
end

function _export_schema!(root::ExportedRoot, f::Field,
    release::Ptr{Cvoid})::Ptr{CArrowSchema}
    p = Ptr{CArrowSchema}(_malloc!(root, sizeof(CArrowSchema)))
    childfields = f.type isa DictionaryType ? Field[] : f.children
    nchildren = length(childfields)
    canonical_children = Ptr{CArrowSchema}[]
    childptrs = Ptr{Ptr{CArrowSchema}}(C_NULL)
    if nchildren > 0
        childptrs = Ptr{Ptr{CArrowSchema}}(_malloc!(root,
            AC.checked_mul(Int64(nchildren), Int64(sizeof(Ptr)))))
        for (i, cf) in enumerate(childfields)
            child = _export_schema!(root, cf, release)
            push!(canonical_children, child)
            unsafe_store!(childptrs, child, i)
        end
    end
    dict = Ptr{CArrowSchema}(C_NULL)
    if f.type isa DictionaryType
        dict = _export_schema!(root, AC.dictvaluefield(f, f.type), release)
    end
    flags = f.nullable ? ARROW_FLAG_NULLABLE : Int64(0)
    f.type isa DictionaryType && f.type.ordered &&
        (flags |= ARROW_FLAG_DICTIONARY_ORDERED)
    f.type isa MapType && f.type.keyssorted &&
        (flags |= ARROW_FLAG_MAP_KEYS_SORTED)
    control = _newcontrol!(root)
    unsafe_store!(p, CArrowSchema(
        _cstring!(root, formatstring_of(f.type)),
        _cstring!(root, f.name),
        # Field metadata rides the OUTER node for every field, dictionary
        # wrappers included — the C++ bridge exports field.metadata() on
        # the wrapper and only TYPE metadata (extensions) on the dependent
        # value node, and PyArrow imports only the wrapper's pairs.
        _cmetadata!(root, f.metadata),
        flags, nchildren, childptrs, dict,
        release, control))
    root.schema_topology[control] = (canonical_children, dict)
    return p
end

function _export_array!(root::ExportedRoot, d::ArrayData,
    release::Ptr{Cvoid})::Ptr{CArrowArray}
    p = Ptr{CArrowArray}(_malloc!(root, sizeof(CArrowArray)))
    spec = AC.layoutspec_of(d.type)
    ncore = length(d.buffers)
    # C Data appends one int64 buffer of variadic data-buffer LENGTHS to view
    # arrays (extents are not otherwise recoverable from the ABI); it counts
    # toward n_buffers here and nowhere else in the format.
    nvariadic = spec.variadic ? ncore - length(spec.buffers) : 0
    nbuf = spec.variadic ? ncore + 1 : ncore
    bufptrs = Ptr{Ptr{Cvoid}}(_malloc!(root,
        AC.checked_mul(Int64(max(nbuf, 1)), Int64(sizeof(Ptr)))))
    for (i, b) in enumerate(d.buffers)
        role = i <= length(spec.buffers) ? spec.buffers[i] : AC.DATA
        bufferp = if role == AC.OFFSETS && d.len == 0 && d.offset == 0 &&
            AC.isempty_buffer(b)
            # Core's canonical empty representation omits this otherwise
            # unused allocation. C Data still exposes the Columnar
            # length+1 offsets buffer, so root one terminal zero in the
            # export aggregate without changing the Core array.
            zerop = Ptr{UInt8}(_malloc!(root, spec.offsetwidth))
            for j = 1:spec.offsetwidth
                unsafe_store!(zerop, UInt8(0), j)
            end
            Ptr{Cvoid}(zerop)
        elseif AC.isempty_buffer(b)
            # An absent validity bitmap, or any actual zero-byte buffer, is
            # represented by a NULL pointer.
            Ptr{Cvoid}(C_NULL)
        else
            Ptr{Cvoid}(AC.sliceptr(b))
        end
        unsafe_store!(bufptrs, bufferp, i)
    end
    if spec.variadic
        sizesp = Ptr{Int64}(_malloc!(root,
            AC.checked_mul(Int64(max(nvariadic, 1)), Int64(8))))
        for k = 1:nvariadic
            unsafe_store!(sizesp, d.buffers[length(spec.buffers) + k].len, k)
        end
        unsafe_store!(bufptrs, Ptr{Cvoid}(sizesp), nbuf)
    end
    nchildren = length(d.children)
    canonical_children = Ptr{CArrowArray}[]
    childptrs = Ptr{Ptr{CArrowArray}}(C_NULL)
    if nchildren > 0
        childptrs = Ptr{Ptr{CArrowArray}}(_malloc!(root,
            AC.checked_mul(Int64(nchildren), Int64(sizeof(Ptr)))))
        for (i, c) in enumerate(d.children)
            child = _export_array!(root, c, release)
            push!(canonical_children, child)
            unsafe_store!(childptrs, child, i)
        end
    end
    dict = d.dictionary === nothing ? Ptr{CArrowArray}(C_NULL) :
        _export_array!(root, d.dictionary, release)
    control = _newcontrol!(root)
    unsafe_store!(p, CArrowArray(d.len, nullcount(d), d.offset, nbuf,
        nchildren, bufptrs, childptrs, dict,
        release, control))
    root.array_topology[control] = (canonical_children, dict)
    return p
end

"""
    to_c_data(field, data) -> (Ptr{CArrowSchema}, Ptr{CArrowArray})

Export one column. The schema and array have separate sets of per-node
control blocks and separate Julia-side roots, as required by their
independent C Data lifetimes. Releasing either root recursively marks only
that structure tree released. Moved descendants defer aggregate cleanup.
The array root keeps the source ArrayData reachable until it is reaped;
that reachability is what keeps the exported buffer pointers valid.

The column is validated through the semantic tier before publication —
the same tier the IPC writer applies. Content policy (`validate_full`:
UTF-8 well-formedness, the advisory nullability contract, canonical bits)
is the caller's opt-in, exactly as for IPC.
"""
function _build_c_data!(sp, skey, ap, akey, f::Field, d::ArrayData,
    arel, srel)
    _newroot(Any[f]; result_slot=sp, key_slot=skey) do root
        _export_schema!(root, f, srel)
    end
    _newroot(Any[d]; result_slot=ap, key_slot=akey) do root
        _export_array!(root, d, arel)
    end
    return nothing
end

function to_c_data(f::Field, d::ArrayData)
    # Reject mismatched schema/data and malformed buffers before publishing
    # either independently-owned C root (semantic composes structural).
    validate_semantic(f, d)
    arel = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
    srel = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
    sp = Ref{Ptr{CArrowSchema}}(C_NULL)
    skey = Ref{Int64}(0)
    ap = Ref{Ptr{CArrowArray}}(C_NULL)
    akey = Ref{Int64}(0)
    try
        # The exact public method owns both output slots until its tuple return.
        # A helper cannot lose a published pointer at its own return boundary:
        # _newroot records each result in the caller's slot when it publishes.
        _build_c_data!(sp, skey, ap, akey, f, d, arel, srel)
        return sp[], ap[]
    catch
        # Schema and array are separate C lifetimes, but export is one API
        # transaction. Neither has escaped on this path, so discard both.
        _cleanup_export_slots!(sp, skey, ap, akey)
        rethrow()
    end
end

function _free_export!(root::ExportedRoot)
    # After a root is claimed, cleanup only drops Julia references and frees
    # tracked mallocs. There is no fallible ownership transition to retry.
    empty!(root.schema_topology)
    empty!(root.array_topology)
    while !isempty(root.mallocs)
        Libc.free(pop!(root.mallocs))
    end
    empty!(root.roots)
    return nothing
end

function _cleanup_registered_root!(key::Int64; require_released=true)
    # Claim by removal: popping the root under the registry lock makes this
    # cleanup naturally exclusive against concurrent reapers, and the frees
    # below cannot throw, so a claimed root never needs re-publishing.
    # `require_released=false` is only legal on paths where no C node has
    # escaped to a consumer (build failures); a released consumer callback
    # finds its root through this registry, so popping early would strand it.
    root = lock(REGISTRY_LOCK) do
        candidate = get(EXPORT_REGISTRY, key, nothing)
        candidate === nothing && return nothing
        require_released && candidate.remaining != 0 && return nothing
        pop!(EXPORT_REGISTRY, key)
        return candidate
    end
    root === nothing && return false
    _free_export!(root)
    return true
end

"""
    reap!() -> Int

Find fully released exports: free every malloc they own and drop their
registry roots. In the real adapter this is a background reaper task; the
example calls it explicitly to keep the demo deterministic.
"""
function reap!()
    keys = lock(REGISTRY_LOCK) do
        Int64[k for (k, root) in EXPORT_REGISTRY if root.remaining == 0]
    end
    reaped = 0
    for key in keys
        reaped += _cleanup_registered_root!(key)
    end
    return reaped
end

function _cleanup_private_root!(root::ExportedRoot, key::Int64)
    registered = lock(REGISTRY_LOCK) do
        get(EXPORT_REGISTRY, key, nothing) === root
    end
    if registered
        _cleanup_registered_root!(key; require_released=false)
    else
        _free_export!(root)
    end
    return nothing
end

function _cleanup_export_slots!(sp, skey, ap, akey)
    # Clear raw pointer slots before any free. Stable registry keys remain
    # valid cleanup tokens until their corresponding root is gone.
    sp[] = C_NULL
    ap[] = C_NULL
    if akey[] != 0
        _cleanup_registered_root!(akey[]; require_released=false)
        akey[] = 0
    end
    if skey[] != 0
        _cleanup_registered_root!(skey[]; require_released=false)
        skey[] = 0
    end
    return nothing
end

function _newroot(build, roots::Vector{Any};
    result_slot=nothing, key_slot=nothing)
    # key and root are single-assignment BEFORE the try: reassignment of a
    # closure-captured local boxes it, which the trim verifier rejects.
    # Nothing before the try owns native memory, so there is nothing to
    # clean on those paths.
    key = lock(REGISTRY_LOCK) do
        NEXT_KEY[] = AC.checked_add(NEXT_KEY[], Int64(1))
    end
    root = ExportedRoot(roots, Ptr{Cvoid}[], key, 0,
        Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowSchema}},Ptr{CArrowSchema}}}(),
        Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowArray}},Ptr{CArrowArray}}}())::ExportedRoot
    try
        # The pointer cannot escape before `build` returns. Keep the root
        # private until then: publishing it with `remaining == 0` would let a
        # concurrent reaper free partial mallocs underneath the builder,
        # before its first node control increments `remaining`.
        result = build(root)
        lock(REGISTRY_LOCK) do
            EXPORT_REGISTRY[key] = root
            key_slot === nothing || (key_slot[] = key)
            result_slot === nothing || (result_slot[] = result)
        end
        return result
    catch
        # Export-failure cleanup: unregister (if published) and free.
        result_slot === nothing || (result_slot[] = C_NULL)
        key_slot === nothing || (key_slot[] = 0)
        _cleanup_private_root!(root, key)
        rethrow()
    end
end

# ---------------------------------------------------------------------------
# Import: C structs -> Core, one ForeignOwner per moved tree
# ---------------------------------------------------------------------------

"""
One owner for one MOVED ArrowArray tree. All BufferSlices from the whole
tree (children, dictionary) use regions whose `root` is this object, so the
tree stays alive while any slice does, and the C release callback runs
exactly once — from `release!` or the GC finalizer, whichever comes first.

The malloc'd copy of the moved struct mirrors the C Data convention for its
own state: its release field is NULL (inert) until the move commits, and the
producer's real callback is stored only then (`_arm_foreign_owner!`). A
failure between construction and the move commit therefore frees just our
copy and never calls the producer — the source, whose release field is still
set, remains the owner.
"""
function _release_owner_action(p::Ptr{Cvoid})::Cvoid
    slot = unsafe_pointer_to_objref(p)::Base.RefValue{Any}
    x = slot[]
    x === nothing || release!(x)
    return nothing
end

mutable struct ForeignOwner
    const arrayblock::Ptr{CArrowArray} # malloc'd copy of the moved struct: a
                                       # stable native address for the
                                       # producer's release callback
    const producer_release::Ptr{Cvoid} # the moved struct's real callback
    @atomic released::Bool             # one swap picks the single releaser
    # ONE revocation cell for every OwnerRegion built over this import: the
    # producer's release frees the whole tree at once, so closing any
    # imported buffer must revoke all of its siblings first (they share this
    # lifetime). The cell's release action routes through `release!`, which
    # stays exactly-once against the GC-finalizer path.
    const cell::AC.ReleaseCell
    function ForeignOwner(arr::CArrowArray, registerfinalizer)
        block = Libc.malloc(sizeof(CArrowArray))
        block == C_NULL && throw(OutOfMemoryError())
        p = Ptr{CArrowArray}(block)
        slot = Ref{Any}(nothing)
        cell = AC.ReleaseCell(
            @cfunction(_release_owner_action, Cvoid, (Ptr{Cvoid},)), slot)
        o = try
            unsafe_store!(p, arr)
            _store_field!(p, Val(:release), Ptr{Cvoid}(C_NULL))  # inert until armed
            new(p, arr.release, false, cell)
        catch
            # The native copy exists before the Julia owner does. If copy
            # initialization or owner allocation fails, no finalizer can
            # reclaim that copy for us.
            Libc.free(block)
            rethrow()
        end
        slot[] = o
        try
            registerfinalizer(release!, o)
        catch
            # The source still owns the producer resources. The copy is inert,
            # so constructor cleanup frees only our malloc'd storage. If the
            # registrar installed a finalizer before throwing, its later call
            # observes released=true and is inert.
            release!(o)
            rethrow()
        end
        return o
    end
end
ForeignOwner(arr::CArrowArray) = ForeignOwner(arr, finalizer)

# The move commit: the source ArrowArray's release has been nulled, so this
# copy is now the sole owner of the producer's resources. Storing the real
# callback into the copy arms the release path; nothing between construction
# and this store can throw.
function _arm_foreign_owner!(o::ForeignOwner)
    (@atomic o.released) && error("cannot arm a released foreign owner")
    GC.@preserve o _store_field!(o.arrayblock, Val(:release), o.producer_release)
    return nothing
end

function _foreign_owner_armed(o::ForeignOwner)
    GC.@preserve o begin
        return unsafe_load(o.arrayblock).release != C_NULL
    end
end

function _release_moved_owner!(o::ForeignOwner)
    # A failure may occur after the source move but before arming. Arm first
    # so this release still runs the producer callback in that seam.
    _foreign_owner_armed(o) || _arm_foreign_owner!(o)
    release!(o)
    return nothing
end

"""
    release!(owner::ForeignOwner)

Run the producer's release callback (if armed) on the malloc'd struct copy,
check the producer nulled the copy's release field (the C Data conformance
rule), and free the copy. Exactly-once: a single atomic swap picks the one
releaser between explicit calls and the GC finalizer; later calls return
immediately. After an explicit release, touching any slice imported from
this tree is undefined behavior — the C Data spec's own post-release rule.
A conformance failure throws; from the finalizer path Julia reports it as a
finalizer error.
"""
release!(o::ForeignOwner) = _release_foreign_owner!(o, Libc.free)

# close!(o::ForeignOwner): deterministically release an imported C-data
# tree through its shared revocation cell — every OwnerRegion built over
# the import is revoked, then the producer's release callback runs exactly
# once. The entry point for imports whose arrays are empty and carry no
# region at all (ArrayData.owner is then the only handle on the lifetime).
AC.close!(o::ForeignOwner) = AC.close!(o.cell)

function _release_foreign_owner!(o::ForeignOwner, deallocate!)
    @atomicswap(o.released = true) && return nothing
    GC.@preserve o begin
        cb = unsafe_load(o.arrayblock).release
        if cb != C_NULL
            ccall(cb, Cvoid, (Ptr{CArrowArray},), o.arrayblock)
            unsafe_load(o.arrayblock).release == C_NULL ||
                (deallocate!(o.arrayblock);
                    error("C Data producer release did not mark the structure released"))
        end
        deallocate!(o.arrayblock)
    end
    return nothing
end

"Read child/dictionary struct pointers out of a CArrowArray."
childat(a::CArrowArray, i::Int) = unsafe_load(unsafe_load(a.children, i))
bufferptr(a::CArrowArray, i::Int) = unsafe_load(a.buffers, i)

"""
    from_c_data(schemaptr, arrayptr) -> (Field, ArrayData)

Import a C-data column. The ArrowArray is moved: it is copied by value and its
source release is nulled so the producer side cannot double-free. The
ArrowSchema is parsed and then released in place. Buffer extents are computed
from length/offset/layout — DECLARED extents: the ABI cannot prove the
allocation sizes, so this is the trusted-in-process boundary, and validation
runs on the declared geometry. The imported column passes the semantic tier
(the same default as the IPC reader); `validate_full` on the returned pair is
the caller's opt-in for content policy. A failed import releases the moved
tree exactly once.
"""
from_c_data(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray}) =
    _from_c_data(sp, ap)

function _from_c_data(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray};
    ownerfactory=ForeignOwner)
    sp == C_NULL && throw(ArgumentError("ArrowSchema pointer is NULL"))
    ap == C_NULL && throw(ArgumentError("ArrowArray pointer is NULL"))
    sch = unsafe_load(sp)
    arr = unsafe_load(ap)
    (sch.release == C_NULL || arr.release == C_NULL) &&
        throw(ArgumentError("cannot import a released structure"))
    owner = nothing
    try
        try
            owner = ownerfactory(arr)::ForeignOwner
            # MOVE: relinquish source ownership before arming the copied
            # owner. The source release field is authoritative.
            _store_field!(ap, Val(:release), Ptr{Cvoid}(C_NULL))
            _arm_foreign_owner!(owner)
            _preflight_schema(sch)
            f = _import_field(sch)
            _preflight_array(f, arr)
            d = _import_array(f, arr, owner)
            validate_semantic(f, d)
            return f, d
        finally
            # The schema lifetime is separate and must end on every path,
            # including owner-construction failure.
            _release_c_schema!(sp, sch)
        end
    catch
        # Before the move, the caller's source remains the owner. After the
        # move, this local copy must release exactly once even when finalizer
        # registration or later validation failed.
        owner !== nothing && unsafe_load(ap).release == C_NULL &&
            _release_moved_owner!(owner)
        rethrow()
    end
end

function _preflight_schema(sch::CArrowSchema, depth::Int=0)
    depth <= 64 || throw(ValidationError("C schema nesting exceeds 64 levels"))
    sch.release != C_NULL || throw(ValidationError("released C schema node"))
    sch.format != C_NULL || throw(ValidationError("C schema format is NULL"))
    sch.n_children >= 0 || throw(ValidationError("negative C schema child count"))
    sch.n_children <= 1_000_000 ||
        throw(ValidationError("C schema child count exceeds import limit"))
    sch.n_children == 0 || sch.children != C_NULL ||
        throw(ValidationError("C schema child table is NULL"))
    for i = 1:sch.n_children
        childptr = unsafe_load(sch.children, i)
        childptr != C_NULL || throw(ValidationError("C schema child $i is NULL"))
        _preflight_schema(unsafe_load(childptr), depth + 1)
    end
    if sch.dictionary != C_NULL
        _preflight_schema(unsafe_load(sch.dictionary), depth + 1)
    end
    return nothing
end

function _preflight_array(f::Field, arr::CArrowArray, depth::Int=0)
    depth <= 64 || throw(ValidationError("C array nesting exceeds 64 levels"))
    arr.release != C_NULL || throw(ValidationError("released C array node"))
    arr.length >= 0 || throw(ValidationError("negative C array length"))
    arr.offset >= 0 || throw(ValidationError("negative C array offset"))
    AC.checked_add(arr.offset, arr.length)
    -1 <= arr.null_count <= arr.length ||
        throw(ValidationError("invalid C array null count $(arr.null_count)"))
    arr.n_buffers >= 0 || throw(ValidationError("negative C array buffer count"))
    arr.n_children >= 0 || throw(ValidationError("negative C array child count"))
    arr.n_buffers == 0 || arr.buffers != C_NULL ||
        throw(ValidationError("C array buffer table is NULL"))
    arr.n_children == 0 || arr.children != C_NULL ||
        throw(ValidationError("C array child table is NULL"))

    spec = AC.layoutspec_of(f.type)
    expected_buffers = length(spec.buffers)
    if spec.variadic
        # validity + views + N variadic data buffers + the trailing int64
        # sizes buffer: at least the fixed pair plus the sizes buffer.
        Int64(arr.n_buffers) >= expected_buffers + 1 ||
            throw(ValidationError("view layout $(typeof(f.type)) requires at least $(expected_buffers + 1) buffers, producer sent $(arr.n_buffers)"))
    else
        Int64(arr.n_buffers) == expected_buffers ||
            throw(ValidationError("layout $(typeof(f.type)) declares $expected_buffers buffers, producer sent $(arr.n_buffers)"))
    end
    expected_children = spec.childcount == -1 ? length(f.children) : spec.childcount
    Int64(arr.n_children) == expected_children ||
        throw(ValidationError("layout $(typeof(f.type)) declares $expected_children children, producer sent $(arr.n_children)"))

    for i = 1:arr.n_children
        childptr = unsafe_load(arr.children, i)
        childptr != C_NULL || throw(ValidationError("C array child $i is NULL"))
        child = unsafe_load(childptr)
        cf = f.children[i]
        _preflight_array(cf, child, depth + 1)
    end
    if f.type isa DictionaryType
        arr.dictionary != C_NULL ||
            throw(ValidationError("dictionary C array has no dictionary values"))
        _preflight_array(AC.dictvaluefield(f, f.type),
            unsafe_load(arr.dictionary), depth + 1)
    elseif arr.dictionary != C_NULL
        throw(ValidationError("non-dictionary C array has dictionary values"))
    end
    return nothing
end

function _release_c_schema!(sp::Ptr{CArrowSchema}, sch::CArrowSchema)
    sch.release == C_NULL && return nothing
    ccall(sch.release, Cvoid, (Ptr{CArrowSchema},), sp)
    unsafe_load(sp).release == C_NULL ||
        error("C Data producer release did not mark the structure released")
    return nothing
end

function _release_c_array!(ap::Ptr{CArrowArray}, arr::CArrowArray)
    arr.release == C_NULL && return nothing
    ccall(arr.release, Cvoid, (Ptr{CArrowArray},), ap)
    unsafe_load(ap).release == C_NULL ||
        error("C Data producer release did not mark the structure released")
    return nothing
end

# Longest C string a schema may carry. Format strings are tens of bytes;
# names and metadata keys are human-scale. The cap converts a missing NUL
# terminator from an unbounded memory scan into a clean refusal.
const CSTRING_SCAN_LIMIT = Int64(1) << 20

function _import_cstring(p::Ptr{UInt8}, what::AbstractString)
    # The limit is enforced BEFORE every dereference: the scan window is
    # exactly CSTRING_SCAN_LIMIT bytes, so the NUL must fall inside it
    # (maximum payload is the limit minus one) and byte limit+1 is never
    # touched — a guard page there must produce this refusal, not SIGBUS.
    n = Int64(0)
    while true
        n >= CSTRING_SCAN_LIMIT && throw(ValidationError(
            "C Data $what has no NUL terminator within " *
            "$(CSTRING_SCAN_LIMIT) bytes"))
        unsafe_load(p + n) == 0x00 && break
        n += 1
    end
    s = unsafe_string(p, n)
    isvalid(s) || throw(ValidationError("C Data $what is not valid UTF-8"))
    return s
end

function _validate_schema_flags(sch::CArrowSchema, fmt::AbstractString)
    sch.flags & ~ARROW_FLAG_ALL_SUPPORTED == 0 ||
        throw(ValidationError("C schema contains unsupported flag bits"))
    (sch.flags & ARROW_FLAG_DICTIONARY_ORDERED == 0 ||
        sch.dictionary != C_NULL) ||
        throw(ValidationError(
            "ARROW_FLAG_DICTIONARY_ORDERED requires a dictionary schema"))
    (sch.flags & ARROW_FLAG_MAP_KEYS_SORTED == 0 || fmt == "+m") ||
        throw(ValidationError(
            "ARROW_FLAG_MAP_KEYS_SORTED requires a map schema"))
    return nothing
end

"Parse a C metadata blob: the count and lengths are producer-declared
(the same trust as every other C Data pointer), but negative values
refuse — they would wrap the walk."
function _import_cmetadata(p::Ptr{UInt8})
    p == C_NULL && return nothing
    n = unsafe_load(Ptr{Int32}(p))
    n < 0 && throw(ValidationError("C schema metadata declares a negative pair count"))
    n == 0 && return nothing
    off = Int64(4)
    out = Pair{String,String}[]
    for _ = 1:n
        klen = unsafe_load(Ptr{Int32}(p + off))
        klen < 0 && throw(ValidationError("C schema metadata declares a negative key length"))
        k = unsafe_string(p + off + 4, klen)
        off += 4 + Int64(klen)
        vlen = unsafe_load(Ptr{Int32}(p + off))
        vlen < 0 && throw(ValidationError("C schema metadata declares a negative value length"))
        v = unsafe_string(p + off + 4, vlen)
        off += 4 + Int64(vlen)
        push!(out, k => v)
    end
    return out
end

function _import_field(sch::CArrowSchema)::Field
    fmt = _import_cstring(sch.format, "format")
    _validate_schema_flags(sch, fmt)
    name = sch.name == C_NULL ? "" : _import_cstring(sch.name, "field name")
    nullable = (sch.flags & ARROW_FLAG_NULLABLE) != 0
    meta = _import_cmetadata(sch.metadata)
    t = parseformat(fmt, sch.flags)

    # Check the schema shape before indexing any recursively-created child.
    # Struct is the only mapped layout with field-declared arity.
    spec = AC.layoutspec_of(t)
    expected_children = spec.childcount
    if expected_children >= 0 && sch.n_children != expected_children
        throw(ValidationError("C schema for $(typeof(t)) declares $(sch.n_children) children; expected $expected_children"))
    end
    t isa UnionType && length(t.typeids) != sch.n_children &&
        throw(ValidationError("union format declares $(length(t.typeids)) type ids for $(sch.n_children) children"))

    children = Field[]
    for i = 1:sch.n_children
        push!(children, _import_field(unsafe_load(unsafe_load(sch.children, i))))
    end
    if sch.dictionary != C_NULL
        vf = _import_field(unsafe_load(sch.dictionary))
        t isa IntType || throw(ValidationError("dictionary index format must be an integer"))
        isempty(children) ||
            throw(ValidationError("dictionary index schema must not have children"))
        ordered = (sch.flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0
        # The value node's metadata joins the wrapper's (wrapper pairs
        # first; duplicate keys are legal): Core's one slot cannot express
        # the two-node attribution, but no pair is lost.
        vmeta = vf.metadata
        dmeta = meta === nothing ?
            (vmeta === nothing ? nothing :
             collect(Pair{String,String}, vmeta)) :
            (vmeta === nothing ? meta :
             vcat(meta, collect(Pair{String,String}, vmeta)))
        # Branch on the metadata's presence: a Union-typed keyword makes
        # the kwcall tuple imprecise, which trim cannot resolve.
        dmeta === nothing && return Field(name,
            DictionaryType(t, vf.type, ordered);
            nullable=nullable, children=vf.children)
        return Field(name, DictionaryType(t, vf.type, ordered);
            nullable=nullable, metadata=dmeta, children=vf.children)
    end
    meta === nothing &&
        return Field(name, t; nullable=nullable, children=children)
    return Field(name, t; nullable=nullable, metadata=meta,
        children=children)
end

"""
Compute each buffer's DECLARED byte extent from the layout registry and wrap
it as a slice over a foreign region rooted by `owner`. Offsets buffers are
sized first (len+1 entries) and then READ — inside their own declared bounds
— to size the data buffer they govern; that dependency order is exactly the
registry's buffer order, so the loop stays generic.
"""
function _import_array(f::Field, arr::CArrowArray, owner::ForeignOwner)::ArrayData
    t = f.type
    spec = AC.layoutspec_of(t)
    total = AC.checked_add(arr.offset, arr.length)
    buffers = BufferSlice[]
    offsets_slice = nothing
    for (i, role) in enumerate(spec.buffers)
        p = bufferptr(arr, i)
        nbytes = if role == AC.VALIDITY
            p == C_NULL && total > 0 && arr.null_count != 0 &&
                throw(ValidationError("NULL validity buffer requires null_count == 0"))
            p == C_NULL ? Int64(0) : AC.expected_validity_bytes(total)
        elseif role == AC.OFFSETS
            AC.checked_mul(AC.checked_add(total, Int64(1)),
                Int64(spec.offsetwidth))
        elseif role == AC.DATA
            if spec.fixedwidth > 0
                AC.checked_mul(total, Int64(spec.fixedwidth))
            elseif spec.fixedwidth == -1
                AC.expected_validity_bytes(total)
            else
                # varbinary data: sized by the final offset, read from the
                # offsets slice we just built (bounded by ITS declared size).
                if offsets_slice === nothing || AC.isempty_buffer(offsets_slice)
                    Int64(0)
                else
                    finaloffset = if spec.offsetwidth == 8
                        AC.loadat(offsets_slice, Int64,
                            AC.checked_mul(total, Int64(8)))
                    else
                        Int64(AC.loadat(offsets_slice, Int32,
                            AC.checked_mul(total, Int64(4))))
                    end
                    finaloffset >= 0 ||
                        throw(ValidationError("negative final offset $finaloffset"))
                    finaloffset
                end
            end
        elseif role == AC.TYPE_IDS
            # One Int8 discriminator per union slot.
            total
        elseif role == AC.ELEMENT_OFFSETS || role == AC.SIZES
            # Per-slot values (dense-union offsets; list-view offsets and
            # sizes), not monotone ranges: exactly `total` entries, no +1.
            AC.checked_mul(total, Int64(spec.offsetwidth))
        elseif role == AC.VIEWS
            AC.checked_mul(total, Int64(16))
        else
            throw(ValidationError("unsupported buffer role $role in C data import"))
        end
        if p == C_NULL
            nbytes == 0 || throw(ValidationError("NULL $role buffer with nonzero required size"))
            push!(buffers, BufferSlice())
        else
            region = OwnerRegion(Ptr{UInt8}(p), nbytes; root=owner, cell=owner.cell)
            slice = BufferSlice(region, 0, nbytes)
            role == AC.OFFSETS && (offsets_slice = slice)
            push!(buffers, slice)
        end
    end
    if spec.variadic
        # The trailing int64 sizes buffer declares each variadic data
        # buffer's extent — the one place the ABI carries a length for them.
        nfixed = length(spec.buffers)
        nvariadic = Int(arr.n_buffers) - nfixed - 1
        sizesp = Ptr{Int64}(bufferptr(arr, Int(arr.n_buffers)))
        (nvariadic == 0 || sizesp != C_NULL) ||
            throw(ValidationError("view array with variadic buffers has a NULL sizes buffer"))
        for k = 1:nvariadic
            len = unsafe_load(sizesp, k)
            len >= 0 || throw(ValidationError("negative variadic buffer length $len"))
            p = bufferptr(arr, nfixed + k)
            if p == C_NULL
                len == 0 || throw(ValidationError("NULL variadic buffer with nonzero declared length"))
                push!(buffers, BufferSlice())
            else
                region = OwnerRegion(Ptr{UInt8}(p), len; root=owner, cell=owner.cell)
                push!(buffers, BufferSlice(region, 0, len))
            end
        end
    end
    children = ArrayData[]
    for i = 1:arr.n_children
        cf = t isa DictionaryType ? error("dictionary carries no children") : f.children[i]
        push!(children, _import_array(cf, childat(arr, i), owner))
    end
    dict = nothing
    if arr.dictionary != C_NULL
        t isa DictionaryType || throw(ValidationError("dictionary array on a non-dictionary field"))
        dict = _import_array(AC.dictvaluefield(f, t), unsafe_load(arr.dictionary), owner)
    end
    return AC._arraydata(t, arr.length, buffers, arr.offset, children,
        dict, owner, arr.null_count)
end

# ---------------------------------------------------------------------------
# C stream interface (ArrowArrayStream): batches over the same two mappings
# ---------------------------------------------------------------------------

# Execution contract: stream callbacks call into Julia, so
# `get_schema`/`get_next`/`get_last_error`/`release` are legal ONLY from
# Julia-attached threads, and calls on one stream must not overlap (the C
# stream spec itself declares the structure not thread-safe). There is no
# marshaling to a Julia-owned worker for foreign-thread callers.

struct CArrowArrayStream
    get_schema::Ptr{Cvoid}     # int (*)(ArrowArrayStream*, ArrowSchema* out)
    get_next::Ptr{Cvoid}       # int (*)(ArrowArrayStream*, ArrowArray* out)
    get_last_error::Ptr{Cvoid} # const char* (*)(ArrowArrayStream*)
    release::Ptr{Cvoid}        # void (*)(ArrowArrayStream*)
    private_data::Ptr{Cvoid}
end

const EINVAL = Cint(Base.Libc.EINVAL)

mutable struct ExportedStreamState
    batchfield::Field                 # struct-typed: children are the schema
    batches::Vector{AC.RecordBatch}
    nextindex::Int
    lasterror::Ptr{UInt8}             # malloc'd NUL string; freed on replace/release
end

const STREAM_REGISTRY = Dict{Int64,ExportedStreamState}()

function _stream_state(sp::Ptr{CArrowArrayStream})
    stream = unsafe_load(sp)
    stream.release == C_NULL && return nothing, Ptr{Cvoid}(C_NULL)
    control = stream.private_data
    control == C_NULL && return nothing, Ptr{Cvoid}(C_NULL)
    key = unsafe_load(Ptr{Int64}(control + 8))
    state = lock(REGISTRY_LOCK) do
        get(STREAM_REGISTRY, key, nothing)
    end
    return state, control
end

function _set_stream_error!(state::ExportedStreamState, msg::AbstractString,
    allocate! = Libc.malloc, deallocate! = Libc.free)
    # The prior pointer expires at the next stream operation even if building
    # its replacement fails. Clear it first so malloc failure cannot report a
    # stale error from an earlier operation.
    old = state.lasterror
    state.lasterror = Ptr{UInt8}(C_NULL)
    try
        old == C_NULL || deallocate!(old)
    catch
        # Error reporting is called from C callbacks and must never throw.
    end
    p = Ptr{UInt8}(C_NULL)
    try
        clean = replace(msg, '\0' => ' ')
        bytes = codeunits(clean)
        n = AC.checked_add(Int64(length(bytes)), Int64(1))
        p = Ptr{UInt8}(allocate!(n))
        p == C_NULL && return nothing
        for (i, b) in enumerate(bytes)
            unsafe_store!(p, b, i)
        end
        unsafe_store!(p, 0x00, length(bytes) + 1)
        state.lasterror = p
    catch
        try
            p == C_NULL || deallocate!(p)
        catch
        end
    end
    return nothing
end

function _set_stream_exception!(state::ExportedStreamState, e)
    try
        _set_stream_error!(state, sprint(showerror, e))
    catch
        # `_set_stream_error!` is itself best-effort, but keep the callback
        # boundary closed if exception rendering fails before it is called.
        _set_stream_error!(state, "stream callback failed")
    end
    return nothing
end

function _publish_stream_result!(build, roots::Vector{Any}, result_slot,
    out, publish!)
    key_slot = Ref{Int64}(0)
    committed = false
    try
        _newroot(build, roots; result_slot=result_slot, key_slot=key_slot)
        publish!(out, unsafe_load(result_slot[]))
        committed = true
    catch
        # The result root became public inside Julia, but no usable C struct
        # reached the consumer. Remove it immediately. Cleanup is best-effort
        # here so it cannot replace the operation's original exception or
        # cross the enclosing C callback boundary.
        if !committed && key_slot[] != 0
            try
                _cleanup_registered_root!(key_slot[]; require_released=false)
            catch
            end
            key_slot[] = 0
        end
        rethrow()
    end
    return nothing
end

function _stream_get_schema_impl(sp::Ptr{CArrowArrayStream},
    out::Ptr{CArrowSchema}, publish!)::Cint
    state = nothing
    try
        sp == C_NULL && return EINVAL
        state, _ = _stream_state(sp)
        state === nothing && return EINVAL
        out == C_NULL && throw(ArgumentError("ArrowSchema output pointer is NULL"))
        srel = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
        shell = Ref{Ptr{CArrowSchema}}(C_NULL)
        _publish_stream_result!(Any[state.batchfield], shell, out,
            publish!) do root
            _export_schema!(root, state.batchfield, srel)
        end
        return Cint(0)
    catch e
        state isa ExportedStreamState && _set_stream_exception!(state, e)
        return EINVAL
    end
end

_stream_get_schema(sp::Ptr{CArrowArrayStream},
    out::Ptr{CArrowSchema})::Cint =
    _stream_get_schema_impl(sp, out, unsafe_store!)

function _stream_get_next_impl(sp::Ptr{CArrowArrayStream},
    out::Ptr{CArrowArray}, publish!)::Cint
    state = nothing
    try
        sp == C_NULL && return EINVAL
        state, _ = _stream_state(sp)
        state === nothing && return EINVAL
        out == C_NULL && throw(ArgumentError("ArrowArray output pointer is NULL"))
        if state.nextindex > length(state.batches)
            # End of stream: a released (NULL-release) struct, per spec.
            publish!(out, CArrowArray(0, 0, 0, 0, 0,
                Ptr{Ptr{Cvoid}}(C_NULL), Ptr{Ptr{CArrowArray}}(C_NULL),
                Ptr{CArrowArray}(C_NULL), Ptr{Cvoid}(C_NULL),
                Ptr{Cvoid}(C_NULL)))
            return Cint(0)
        end
        b = state.batches[state.nextindex]
        d = ArrayData(StructType(), b.nrows, [BufferSlice()];
            children=collect(ArrayData, b.columns), nullcount=0)
        validate_semantic(state.batchfield, d)
        arel = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
        shell = Ref{Ptr{CArrowArray}}(C_NULL)
        _publish_stream_result!(Any[d], shell, out, publish!) do root
            _export_array!(root, d, arel)
        end
        state.nextindex += 1
        return Cint(0)
    catch e
        state isa ExportedStreamState && _set_stream_exception!(state, e)
        return EINVAL
    end
end

_stream_get_next(sp::Ptr{CArrowArrayStream},
    out::Ptr{CArrowArray})::Cint =
    _stream_get_next_impl(sp, out, unsafe_store!)

function _stream_get_last_error(sp::Ptr{CArrowArrayStream})::Ptr{UInt8}
    try
        sp == C_NULL && return Ptr{UInt8}(C_NULL)
        state, _ = _stream_state(sp)
        state === nothing && return Ptr{UInt8}(C_NULL)
        return state.lasterror
    catch
        return Ptr{UInt8}(C_NULL)
    end
end

function _stream_release(sp::Ptr{CArrowArrayStream})::Cvoid
    # Claim/commit with no error channel, like the node callbacks. Batch and
    # schema roots already handed to the consumer keep their own lifetimes.
    try
        sp == C_NULL && return nothing
        lock(REGISTRY_LOCK) do
            stream = unsafe_load(sp)
            stream.release == C_NULL && return nothing
            control = stream.private_data
            control == C_NULL && return nothing
            key = unsafe_load(Ptr{Int64}(control + 8))
            state = get(STREAM_REGISTRY, key, nothing)
            state === nothing && return nothing
            pop!(STREAM_REGISTRY, key)
            errorp = state.lasterror
            state.lasterror = Ptr{UInt8}(C_NULL)
            errorp == C_NULL || Libc.free(errorp)
            _store_field!(sp, Val(:release), Ptr{Cvoid}(C_NULL))
            _store_field!(sp, Val(:private_data), Ptr{Cvoid}(C_NULL))
            Libc.free(control)
            return nothing
        end
    catch
        # A void C callback has no error channel. Never unwind into C.
    end
    return nothing
end

"""
    export_stream!(sp::Ptr{CArrowArrayStream}, sch::Schema, batches)

Fill a CALLER-owned ArrowArrayStream struct (the C stream convention: the
producer fills, the consumer owns the struct storage) streaming `batches` as
struct-typed arrays whose children are the schema's columns. The stream's
registry root keeps schema fields and batches reachable until `release`;
every `get_schema`/`get_next` result is its own export root with the same
lifecycle as `to_c_data` output.
"""
export_stream!(sp::Ptr{CArrowArrayStream}, sch::Schema,
    batches::AbstractVector{AC.RecordBatch}) =
    _export_stream!(sp, sch, batches, Libc.malloc, Libc.free, unsafe_store!)

function _export_stream!(sp::Ptr{CArrowArrayStream}, sch::Schema,
    batches::AbstractVector{AC.RecordBatch}, allocate!, deallocate!, publish!)
    sp == C_NULL && throw(ArgumentError("ArrowArrayStream pointer is NULL"))
    for b in batches
        length(b.columns) == length(sch.fields) ||
            throw(ValidationError("stream batch column count does not match the schema"))
    end
    # The stream's struct-typed schema node carries the schema-level
    # metadata (the C++/pyarrow convention for `schema.metadata`).
    batchfield = Field("", StructType(); nullable=false,
        metadata=sch.metadata, children=collect(Field, sch.fields))
    state = ExportedStreamState(batchfield,
        collect(AC.RecordBatch, batches), 1, Ptr{UInt8}(C_NULL))
    get_schema = @cfunction(_stream_get_schema, Cint,
        (Ptr{CArrowArrayStream}, Ptr{CArrowSchema}))
    get_next = @cfunction(_stream_get_next, Cint,
        (Ptr{CArrowArrayStream}, Ptr{CArrowArray}))
    get_last_error = @cfunction(_stream_get_last_error, Ptr{UInt8},
        (Ptr{CArrowArrayStream},))
    release = @cfunction(_stream_release, Cvoid, (Ptr{CArrowArrayStream},))
    control = Ptr{Cvoid}(C_NULL)
    key = Int64(0)
    havekey = false
    try
        control = Ptr{Cvoid}(allocate!(CONTROL_BLOCK_BYTES))
        control == C_NULL && throw(OutOfMemoryError())
        key = lock(REGISTRY_LOCK) do
            NEXT_KEY[] = AC.checked_add(NEXT_KEY[], Int64(1))
        end
        havekey = true
        unsafe_store!(Ptr{UInt8}(control), 0x00)
        unsafe_store!(Ptr{Int64}(control + 8), key)
        lock(REGISTRY_LOCK) do
            STREAM_REGISTRY[key] = state
        end
        publish!(sp, CArrowArrayStream(get_schema, get_next, get_last_error,
            release, control))
        return sp
    catch
        if havekey
            lock(REGISTRY_LOCK) do
                get(STREAM_REGISTRY, key, nothing) === state &&
                    pop!(STREAM_REGISTRY, key)
            end
        end
        errorp = state.lasterror
        state.lasterror = Ptr{UInt8}(C_NULL)
        errorp == C_NULL || deallocate!(errorp)
        control == C_NULL || deallocate!(control)
        rethrow()
    end
end

_stream_registry_count() = lock(REGISTRY_LOCK) do
    length(STREAM_REGISTRY)
end

# ---- import half ----------------------------------------------------------

"""
One owner for one MOVED ArrowArrayStream, mirroring ForeignOwner: a malloc'd
copy of the moved struct gives the producer's callbacks a stable address, one
atomic flag picks the single releaser between explicit `release!` and the GC
finalizer, and post-release calls are the spec's own undefined behavior.
"""
mutable struct StreamOwner
    const block::Ptr{CArrowArrayStream}
    const producer_release::Ptr{Cvoid}
    @atomic released::Bool
    function StreamOwner(stream::CArrowArrayStream, registerfinalizer)
        block = Libc.malloc(sizeof(CArrowArrayStream))
        block == C_NULL && throw(OutOfMemoryError())
        p = Ptr{CArrowArrayStream}(block)
        o = try
            unsafe_store!(p, stream)
            _store_field!(p, Val(:release), Ptr{Cvoid}(C_NULL)) # inert until moved
            new(p, stream.release, false)
        catch
            Libc.free(block)
            rethrow()
        end
        try
            registerfinalizer(release!, o)
        catch
            # The source still owns the producer stream. Free only the inert
            # copy; an already-installed finalizer observes released=true.
            release!(o)
            rethrow()
        end
        return o
    end
end
StreamOwner(stream::CArrowArrayStream) = StreamOwner(stream, finalizer)

function _stream_owner_armed(o::StreamOwner)
    GC.@preserve o begin
        return unsafe_load(o.block).release != C_NULL
    end
end

function _arm_stream_owner!(o::StreamOwner)
    (@atomic o.released) && error("cannot arm a released stream owner")
    GC.@preserve o _store_field!(o.block, Val(:release), o.producer_release)
    return nothing
end

function _release_moved_stream_owner!(o::StreamOwner)
    _stream_owner_armed(o) || _arm_stream_owner!(o)
    release!(o)
    return nothing
end

function release!(o::StreamOwner)
    @atomicswap(o.released = true) && return nothing
    GC.@preserve o begin
        cb = unsafe_load(o.block).release
        if cb != C_NULL
            ccall(cb, Cvoid, (Ptr{CArrowArrayStream},), o.block)
            unsafe_load(o.block).release == C_NULL ||
                (Libc.free(o.block);
                    error("C stream producer release did not mark the structure released"))
        end
        Libc.free(o.block)
    end
    return nothing
end

"""
    ImportedStream

Consumer side of a moved ArrowArrayStream: `schema(s)` is fixed at import,
`nextbatch!(s)` pulls one struct-typed batch (returning `nothing` at end of
stream), and `release!(s)` ends the producer's stream exactly once. Each
pulled batch owns its own ForeignOwner and outlives the stream if the caller
keeps it. Producer-reported failures surface as `ValidationError`s carrying
the producer's `get_last_error` text.
"""
mutable struct ImportedStream <: AC.RecordBatchSource
    const owner::StreamOwner
    const batchfield::Field
    const schema::Schema
    done::Bool
end

AC.schema(s::ImportedStream) = s.schema
release!(s::ImportedStream) = release!(s.owner)

function _stream_call_failed(o::StreamOwner, what::AbstractString)
    msg = "C stream $what failed"
    GC.@preserve o begin
        cb = unsafe_load(o.block).get_last_error
        if cb != C_NULL
            p = ccall(cb, Ptr{UInt8}, (Ptr{CArrowArrayStream},), o.block)
            p == C_NULL || (msg *= ": " * _import_cstring(p, "stream error"))
        end
    end
    throw(ValidationError(msg))
end

"""
    from_c_stream(sp::Ptr{CArrowArrayStream}) -> ImportedStream

Move a producer's stream (copy the struct, null the source release) and read
its schema. The schema must be a struct-typed batch schema, per the C stream
convention; its fields become the imported `Schema`.
"""
function from_c_stream(sp::Ptr{CArrowArrayStream})
    sp == C_NULL && throw(ArgumentError("ArrowArrayStream pointer is NULL"))
    stream = unsafe_load(sp)
    stream.release == C_NULL &&
        throw(ArgumentError("cannot import a released stream"))
    (stream.get_schema == C_NULL || stream.get_next == C_NULL ||
        stream.get_last_error == C_NULL) &&
        throw(ArgumentError("C stream is missing required callbacks"))
    owner = StreamOwner(stream)
    moved = false
    try
        _store_field!(sp, Val(:release), Ptr{Cvoid}(C_NULL)) # the move commit
        moved = true
        _arm_stream_owner!(owner)
        out = Ref(CArrowSchema(Ptr{UInt8}(C_NULL), Ptr{UInt8}(C_NULL),
            Ptr{UInt8}(C_NULL), 0, 0, Ptr{Ptr{CArrowSchema}}(C_NULL),
            Ptr{CArrowSchema}(C_NULL), Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL)))
        status = GC.@preserve owner out ccall(unsafe_load(owner.block).get_schema,
            Cint, (Ptr{CArrowArrayStream}, Ptr{CArrowSchema}),
            owner.block, Base.unsafe_convert(Ptr{CArrowSchema}, out))
        status == 0 || _stream_call_failed(owner, "get_schema")
        sch = out[]
        batchfield = GC.@preserve out try
            _preflight_schema(sch)
            _import_field(sch)
        finally
            _release_c_schema!(Base.unsafe_convert(Ptr{CArrowSchema}, out), sch)
        end
        batchfield.type isa StructType ||
            throw(ValidationError("C stream schema must be a struct-typed batch schema"))
        return ImportedStream(owner, batchfield,
            Schema(collect(Field, batchfield.children);
                metadata=batchfield.metadata), false)
    catch
        moved ? _release_moved_stream_owner!(owner) : release!(owner)
        rethrow()
    end
end

AC.nextbatch!(s::ImportedStream) = _nextbatch!(s, ForeignOwner)

function _nextbatch!(s::ImportedStream, ownerfactory)
    # Fail closed on a released stream even when it already ended naturally:
    # release terminates the consumer contract, not just the batch supply.
    (@atomic s.owner.released) &&
        throw(ArgumentError("cannot pull from a released stream"))
    s.done && return nothing
    out = Ref(CArrowArray(0, 0, 0, 0, 0, Ptr{Ptr{Cvoid}}(C_NULL),
        Ptr{Ptr{CArrowArray}}(C_NULL), Ptr{CArrowArray}(C_NULL),
        Ptr{Cvoid}(C_NULL), Ptr{Cvoid}(C_NULL)))
    status = GC.@preserve s out ccall(unsafe_load(s.owner.block).get_next,
        Cint, (Ptr{CArrowArrayStream}, Ptr{CArrowArray}),
        s.owner.block, Base.unsafe_convert(Ptr{CArrowArray}, out))
    status == 0 || _stream_call_failed(s.owner, "get_next")
    arr = out[]
    if arr.release == C_NULL
        s.done = true
        return nothing
    end
    # The producer filled consumer-owned storage. Build an inert destination
    # owner first. If that construction fails, the live source slot still owns
    # the result and must release it. Then null the source and arm the copy.
    batchowner = try
        ownerfactory(arr)::ForeignOwner
    catch
        GC.@preserve out _release_c_array!(
            Base.unsafe_convert(Ptr{CArrowArray}, out), arr)
        rethrow()
    end
    moved = false
    d = try
        GC.@preserve out _store_field!(
            Base.unsafe_convert(Ptr{CArrowArray}, out), :release,
            Ptr{Cvoid}(C_NULL))
        moved = true
        _arm_foreign_owner!(batchowner)
        _preflight_array(s.batchfield, arr)
        d0 = _import_array(s.batchfield, arr, batchowner)
        validate_semantic(s.batchfield, d0)
        d0
    catch
        moved ? _release_moved_owner!(batchowner) : release!(batchowner)
        rethrow()
    end
    return AC.RecordBatch(s.schema, collect(ArrayData, d.children), d.len)
end
