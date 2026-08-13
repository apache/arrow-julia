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
# PROVE-OUT: the C data interface adapter over ArrowCore.
#
#     julia --startup-file=no core/examples/cdata.jl
#
# The point of the whole Core design is that this adapter is a direct mapping:
# because `ArrayData` already has the shape of the C `ArrowArray` (buffers +
# children + dictionary + length/null_count/offset), export is struct
# filling and import is struct reading — after five stalled attempts to bolt
# this interface onto the 2.x internals (#178, #179, #561, #594, #603-607),
# that is the claim this example exists to prove.
#
# Lifecycle, mapped to the report (§9 "C-data adapter"):
#
#   * Export: ONE release callback per C structure (never per buffer). A
#     parent callback releases each child/dictionary that has not been moved;
#     a moved child keeps the shared export allocation alive until its own
#     callback runs. `private_data` points to a per-node malloc'd,
#     never-GC-scanned CONTROL BLOCK holding an exactly-once state and the
#     registry key. The Julia-side owner (which roots the Core columns and
#     every malloc'd C
#     struct) stays in a global EXPORT REGISTRY until release — a raw
#     pointer in private_data roots nothing by itself. The @cfunction
#     release callback recursively marks the C tree released. Callback
#     traversal uses producer-owned canonical child/dictionary topology, not
#     the caller-visible counts and pointer tables. It still reads each
#     canonical descendant's public release field so conforming moves are
#     honored. A reaper pass scans for aggregates whose last outstanding node
#     was released, frees mallocs, drops the registry root, and releases
#     source-region pins. Prove-out callback contract: releases for one tree
#     are serialized and run only on Julia-attached threads. A native
#     foreign-thread, concurrent trampoline/queue is production adapter work.
#
#   * Import: the moved ArrowArray becomes ONE ForeignOwner shared by every
#     child/dictionary BufferSlice (a single release for the whole tree —
#     per-buffer owners would double-release). Buffer extents are DECLARED,
#     not verified: computed from length/offset/layout per the report's
#     "trusted in-process ABI" rule; offsets buffers are read (bounded by
#     their computed size) to size the data buffers they govern. Failed
#     imports release the moved structure exactly once before throwing.
#     Per spec, moving marks the source released (release = NULL).
#
# The demo includes a registry-rooting round trip that drops all Julia source
# references before GC and import. It also exports a Core batch (nullable ints,
# strings, list column), materializes and compares imported columns, releases
# and reaps them, and proves that the registry is empty and double release is
# inert.
# =============================================================================

include(joinpath(@__DIR__, "..", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore

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
# Format strings <-> Core descriptors (the subset the demo exercises)
# ---------------------------------------------------------------------------

formatstring(t::IntType) =
    (t.signed ? Dict(8 => "c", 16 => "s", 32 => "i", 64 => "l") :
     Dict(8 => "C", 16 => "S", 32 => "I", 64 => "L"))[t.bits]
formatstring(t::FloatType) = Dict(16 => "e", 32 => "f", 64 => "g")[t.bits]
formatstring(::BoolType) = "b"
formatstring(t::Utf8Type) = t.large ? "U" : "u"
formatstring(t::BinaryType) = t.large ? "Z" : "z"
formatstring(t::ListType) = t.large ? "+L" : "+l"
formatstring(::StructType) = "+s"
formatstring(::MapType) = "+m"
formatstring(t::DictionaryType) = formatstring(t.indextype)  # per spec: index format; values on schema.dictionary

function parseformat(fmt::AbstractString, flags::Int64=0)::ArrowType
    fmt == "b" && return BoolType()
    fmt == "u" && return Utf8Type(false)
    fmt == "U" && return Utf8Type(true)
    fmt == "z" && return BinaryType(false)
    fmt == "Z" && return BinaryType(true)
    fmt == "+l" && return ListType(false)
    fmt == "+L" && return ListType(true)
    fmt == "+s" && return StructType()
    fmt == "+m" && return MapType((flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0)
    fmt == "e" && return FloatType(16)
    fmt == "f" && return FloatType(32)
    fmt == "g" && return FloatType(64)
    m = Dict("c" => (8, true), "C" => (8, false), "s" => (16, true), "S" => (16, false),
        "i" => (32, true), "I" => (32, false), "l" => (64, true), "L" => (64, false))
    haskey(m, fmt) && return IntType(m[fmt]...)
    error("cdata prove-out: unmapped format string \"$fmt\"")
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
released and the reaper runs.
"""
mutable struct ExportedRoot
    roots::Vector{Any}          # ArrayData/Field/Schema kept reachable
    mallocs::Vector{Ptr{Cvoid}} # every Libc.malloc'd allocation, freed on reap
    pins::Vector{OwnerRegion}   # long-lived source access guards for C pointers
    key::Int64
    remaining::Int64           # exported C nodes whose callback has not run
    cleaning::Bool             # one reaper owns cleanup while this is true
    schema_topology::Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowSchema}},Ptr{CArrowSchema}}}
    array_topology::Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowArray}},Ptr{CArrowArray}}}
end

const EXPORT_REGISTRY = Dict{Int64,ExportedRoot}()
const REGISTRY_LOCK = ReentrantLock()
const NEXT_KEY = Ref{Int64}(0)
function _claim_array_node(a::Ptr{CArrowArray})
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
        unsafe_store!(Ptr{UInt8}(p), 0x01)
        return claimed
    end
end

function _claim_schema_node(s::Ptr{CArrowSchema})
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
        unsafe_store!(Ptr{UInt8}(p), 0x01)
        return claimed
    end
end

function _finish_node!(p, control::Ptr{Cvoid})
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
        root.remaining -= 1
        unsafe_store!(Ptr{UInt8}(control), 0x02)
        _store_field!(p, :release, Ptr{Cvoid}(C_NULL))
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

function _release_array_children!(topology, after_child=nothing)
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
            after_child === nothing || after_child(child)
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
            after_child === nothing || after_child(dictionary)
        end
    end
    return nothing
end

function _release_schema_children!(topology, after_child=nothing)
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
            after_child === nothing || after_child(child)
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
            after_child === nothing || after_child(dictionary)
        end
    end
    return nothing
end

function _release_array_impl(a::Ptr{CArrowArray}, after_claim=nothing,
    after_child=nothing)
    claimed = _claim_array_node(a)
    claimed === nothing && return nothing
    control, topology = claimed
    try
        after_claim === nothing || after_claim()
        _release_array_children!(topology, after_child)
        _finish_node!(a, control)
    catch
        # Descendant releases are idempotent: a completed child has a NULL
        # callback and a retry skips it. Return this node to LIVE so a failed
        # transaction never leaves its aggregate root and source pins stuck.
        _reset_node_claim!(control)
        rethrow()
    end
    return nothing
end

function _release_schema_impl(s::Ptr{CArrowSchema}, after_claim=nothing,
    after_child=nothing)
    claimed = _claim_schema_node(s)
    claimed === nothing && return nothing
    control, topology = claimed
    try
        after_claim === nothing || after_claim()
        _release_schema_children!(topology, after_child)
        _finish_node!(s, control)
    catch
        _reset_node_claim!(control)
        rethrow()
    end
    return nothing
end

function _run_release_callback(f)
    # Arrow release callbacks have a void C signature and no error channel.
    # Do not return to the consumer until one idempotent transaction completes.
    while true
        try
            Base.disable_sigint() do
                while true
                    try
                        f()
                        return nothing
                    catch
                        # _release_*_impl returns its node to LIVE before an
                        # exception reaches this boundary. Completed children
                        # are NULL, so the next transaction skips them.
                    end
                end
            end
            return nothing
        catch
            # SIGINT can arrive immediately before signals are disabled or as
            # normal delivery is restored. The callback is still idempotent.
        end
    end
end

function _release_array_entry(a::Ptr{CArrowArray}, after_claim=nothing,
    after_child=nothing)
    _run_release_callback() do
        _release_array_impl(a, after_claim, after_child)
    end
    return nothing
end

function _release_schema_entry(s::Ptr{CArrowSchema}, after_claim=nothing,
    after_child=nothing)
    _run_release_callback() do
        _release_schema_impl(s, after_claim, after_child)
    end
    return nothing
end

_release_array(a::Ptr{CArrowArray}) = _release_array_entry(a)
_release_schema(s::Ptr{CArrowSchema}) = _release_schema_entry(s)

# Store one field of a C struct in place (structs are immutable in Julia;
# the C memory is not).
@generated function _store_field!(p::Ptr{T}, ::Val{name}, v) where {T,name}
    i = findfirst(==(name), fieldnames(T))
    off = fieldoffset(T, i)
    FT = fieldtype(T, i)
    return :(unsafe_store!(Ptr{$FT}(Ptr{Cvoid}(p) + $off), convert($FT, v)); nothing)
end
_store_field!(p, name::Symbol, v) = _store_field!(p, Val(name), v)

_malloc!(root::ExportedRoot, n::Integer,
    register! = push!, deallocate! = Libc.free) = begin
    n >= 0 || throw(ArgumentError("negative export allocation size"))
    n64 = Int64(n)
    # Reserve the ledger slot before acquiring native memory. After malloc,
    # either registration owns the pointer or the local catch deallocates it.
    sizehint!(root.mallocs, AC.checked_add(length(root.mallocs), 1))
    oldlen = length(root.mallocs)
    p = Libc.malloc(max(n64, Int64(1)))
    p == C_NULL && throw(OutOfMemoryError())
    try
        register!(root.mallocs, p)
    catch
        if length(root.mallocs) == oldlen
            deallocate!(p)
        elseif !(length(root.mallocs) == oldlen + 1 && root.mallocs[end] == p)
            error("export malloc registration left an invalid ledger state")
        end
        rethrow()
    end
    Ptr{Cvoid}(p)
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
        _cstring!(root, formatstring(f.type)),
        _cstring!(root, f.name),
        Ptr{UInt8}(C_NULL),
        flags, nchildren, childptrs, dict,
        release, control))
    root.schema_topology[control] = (canonical_children, dict)
    return p
end

function _export_array!(root::ExportedRoot, d::ArrayData,
    release::Ptr{Cvoid})::Ptr{CArrowArray}
    p = Ptr{CArrowArray}(_malloc!(root, sizeof(CArrowArray)))
    nbuf = length(d.buffers)
    bufptrs = Ptr{Ptr{Cvoid}}(_malloc!(root,
        AC.checked_mul(Int64(max(nbuf, 1)), Int64(sizeof(Ptr)))))
    for (i, b) in enumerate(d.buffers)
        # Spec: an absent validity bitmap is a NULL buffer pointer.
        unsafe_store!(bufptrs, AC.isempty_buffer(b) ? Ptr{Cvoid}(C_NULL) :
                               Ptr{Cvoid}(AC.sliceptr(b)), i)
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
The array root also holds source-region pins until it is reaped.
"""
function to_c_data(f::Field, d::ArrayData)
    # Reject mismatched schema/data and malformed buffers before publishing
    # either independently-owned C root.
    validate_structural(f, d)
    validate_semantic(f, d)
    validate_full(f, d)
    arel = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
    srel = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
    sp = _newroot(Any[f]) do root
        _export_schema!(root, f, srel)
    end
    try
        ap = _newroot(Any[d], d) do root
            _export_array!(root, d, arel)
        end
        return sp, ap
    catch
        # Schema and array are separate C lifetimes, but export is one API
        # transaction. The schema has not escaped yet, so discard it directly.
        _discard_export!(sp)
        rethrow()
    end
end

function _walk_regions!(seen::IdDict{OwnerRegion,Nothing}, d::ArrayData)
    for b in d.buffers
        b.region === nothing && continue
        gate = AC._lifecycle(b.region)
        seen[gate] = nothing
    end
    for child in d.children
        _walk_regions!(seen, child)
    end
    d.dictionary === nothing || _walk_regions!(seen, d.dictionary)
    return seen
end

function _release_pins!(pins::Vector{OwnerRegion}, n::Int=length(pins))
    for i = 1:n
        AC._releaseguard!(pins[i])
    end
    empty!(pins)
    return nothing
end

function _pin_regions(d::ArrayData, acquire! = AC._acquireguard!)
    pins = collect(keys(_walk_regions!(IdDict{OwnerRegion,Nothing}(), d)))
    acquired = 0
    try
        for region in pins
            acquire!(region)
            acquired += 1
        end
        return pins
    catch
        _release_pins!(pins, acquired)
        rethrow()
    end
end

function _free_export!(root::ExportedRoot, after_step=nothing)
    empty!(root.schema_topology)
    empty!(root.array_topology)
    while !isempty(root.mallocs)
        m = pop!(root.mallocs)
        Libc.free(m)
        after_step === nothing || after_step(:malloc)
    end
    empty!(root.roots)
    while !isempty(root.pins)
        AC._releaseguard!(pop!(root.pins))
        after_step === nothing || after_step(:pin)
    end
    return nothing
end

function _cleanup_registered_root!(key::Int64; require_released=true,
    after_claim=nothing, after_step=nothing)
    return Base.disable_sigint() do
        root = lock(REGISTRY_LOCK) do
            candidate = get(EXPORT_REGISTRY, key, nothing)
            candidate === nothing && return nothing
            candidate.cleaning && return nothing
            require_released && candidate.remaining != 0 && return nothing
            candidate.cleaning = true
            return candidate
        end
        root === nothing && return false
        try
            after_claim === nothing || after_claim(root)
            _free_export!(root, after_step)
            lock(REGISTRY_LOCK) do
                get(EXPORT_REGISTRY, key, nothing) === root ||
                    error("C Data export root changed during cleanup")
                pop!(EXPORT_REGISTRY, key)
            end
        catch
            lock(REGISTRY_LOCK) do
                get(EXPORT_REGISTRY, key, nothing) === root &&
                    (root.cleaning = false)
            end
            rethrow()
        end
        return true
    end
end

"""
    reap!() -> Int

Find fully released exports: free every malloc they own and drop their
registry roots. In the real adapter this is a background reaper task; the
example calls it explicitly to keep the demo deterministic.
"""
function reap!()
    keys = lock(REGISTRY_LOCK) do
        Int64[k for (k, root) in EXPORT_REGISTRY
            if root.remaining == 0 && !root.cleaning]
    end
    reaped = 0
    for key in keys
        reaped += _cleanup_registered_root!(key)
    end
    return reaped
end

function _discard_export!(p::Ptr)
    p == C_NULL && return nothing
    Base.disable_sigint() do
        control = unsafe_load(p).private_data
        key = unsafe_load(Ptr{Int64}(control + 8))
        _cleanup_registered_root!(key; require_released=false)
    end
    return nothing
end

function _newroot(build, roots::Vector{Any}, pinsource=nothing,
    rootfactory=ExportedRoot)
    key = Int64(0)
    root = nothing
    try
        key = lock(REGISTRY_LOCK) do
            NEXT_KEY[] = AC.checked_add(NEXT_KEY[], Int64(1))
        end
        root = rootfactory(roots, Ptr{Cvoid}[], OwnerRegion[], key, 0, false,
            Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowSchema}},Ptr{CArrowSchema}}}(),
            Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowArray}},Ptr{CArrowArray}}}())::ExportedRoot
        # Construct all Julia bookkeeping before acquiring source guards. Once
        # guards exist, every remaining failure unwinds through _free_export!.
        pinsource === nothing || (root.pins = _pin_regions(pinsource))
        # The pointer cannot escape before `build` returns. Keep the root
        # private until then: publishing it with `remaining == 0` would let a
        # concurrent reaper free partial mallocs and source pins underneath
        # the builder, before its first node control increments `remaining`.
        result = build(root)
        lock(REGISTRY_LOCK) do
            EXPORT_REGISTRY[key] = root
        end
        return result
    catch
        # Export-failure cleanup keeps a published root registered until every
        # resource is gone. This also covers interruption during publication.
        if root !== nothing
            Base.disable_sigint() do
                registered = lock(REGISTRY_LOCK) do
                    get(EXPORT_REGISTRY, key, nothing) === root
                end
                if registered
                    _cleanup_registered_root!(key; require_released=false)
                else
                    _free_export!(root)
                end
            end
        end
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
exactly once — from `release!` or the finalizer, whichever comes first.
"""
mutable struct ForeignOwner
    array::CArrowArray          # the moved struct (by value; source was nulled)
    gate::OwnerRegion           # one lifecycle state shared by the whole tree
    function ForeignOwner(arr::CArrowArray)
        o = new()
        o.array = arr
        # Construct the gate unarmed. Until the source ArrowArray's release
        # field is nulled, that source remains the sole owner. Arming a
        # finalizer here would create two owners if the task were interrupted
        # before the move completed.
        o.gate = OwnerRegion(Ptr{UInt8}(0), 0, AC.Foreign; root=o)
        return o
    end
end

function _arm_foreign_owner!(o::ForeignOwner)
    # The gate has no data extent. Its finalizer is the shared-mode backstop.
    # Every imported BufferSlice guards this same lifecycle.
    o.gate.releasefn = _release_foreign_tree!
    finalizer(AC._finalize_region!, o.gate)
    return nothing
end

function _release_moved_owner!(o::ForeignOwner)
    # A failure may occur after the source move but before finalizer
    # registration. Install the callback locally so forceclose! still owns
    # the copied producer release in that seam.
    o.gate.releasefn === nothing &&
        (o.gate.releasefn = _release_foreign_tree!)
    release!(o)
    return nothing
end

function _release_foreign_tree!(gate::OwnerRegion)
    o = gate.root::ForeignOwner
    o.array.release == C_NULL && return nothing
    # Call the producer's release with a pointer to our copy — legal per
    # spec: release takes the structure address, frees producer resources,
    # and marks it released.
    ref = Ref(o.array)
    GC.@preserve ref begin
        ccall(o.array.release, Cvoid, (Ptr{CArrowArray},),
            Base.unsafe_convert(Ptr{CArrowArray}, ref))
    end
    return nothing
end

function release!(o::ForeignOwner; timeout_ms::Integer=1000)
    forceclose!(o.gate; timeout_ms=timeout_ms) ||
        error("foreign array busy: access guards still held after timeout")
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
from length/offset/layout —
DECLARED extents (report §9): the ABI cannot prove the allocation sizes, so
this is the trusted-in-process boundary, and validation runs on the declared
geometry. A failed import releases the moved tree exactly once.
"""
from_c_data(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray}) =
    _from_c_data(sp, ap, () -> nothing)

function _from_c_data(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray},
    after_move)
    sp == C_NULL && throw(ArgumentError("ArrowSchema pointer is NULL"))
    ap == C_NULL && throw(ArgumentError("ArrowArray pointer is NULL"))
    sch = unsafe_load(sp)
    arr = unsafe_load(ap)
    (sch.release == C_NULL || arr.release == C_NULL) &&
        throw(ArgumentError("cannot import a released structure"))
    owner = ForeignOwner(arr)
    moved = false
    try
        # MOVE: relinquish source ownership before arming the copied owner's
        # finalizer. Keep the store and local handoff flag non-interruptible so
        # cleanup always knows which side owns the producer callback.
        Base.disable_sigint() do
            _store_field!(ap, :release, Ptr{Cvoid}(C_NULL))
            moved = true
            after_move()
            _arm_foreign_owner!(owner)
        end
        _preflight_schema(sch)
        f = _import_field(sch)
        _preflight_array(f, arr)
        d = _import_array(f, arr, owner)
        validate_structural(f, d)
        validate_semantic(f, d)
        validate_full(f, d)
        return f, d
    catch
        # Before the move, the caller's source remains the owner. After the
        # move, this local copy must release exactly once even when finalizer
        # registration or later validation failed.
        moved && _release_moved_owner!(owner)
        rethrow()
    finally
        # The schema struct's lifetime is separate from the array's and it
        # is fully consumed by _import_field — release it on BOTH paths so a
        # failed import cannot leak the producer's schema resources.
        _release_c_schema!(sp, sch)
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

    spec = layoutspec(f.type)
    expected_buffers = length(spec.buffers)
    Int64(arr.n_buffers) == expected_buffers ||
        throw(ValidationError("layout $(typeof(f.type)) declares $expected_buffers buffers, producer sent $(arr.n_buffers)"))
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
    return nothing
end

function _import_cstring(p::Ptr{UInt8}, what::AbstractString)
    s = unsafe_string(p)
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

function _import_field(sch::CArrowSchema)::Field
    fmt = _import_cstring(sch.format, "format")
    _validate_schema_flags(sch, fmt)
    name = sch.name == C_NULL ? "" : _import_cstring(sch.name, "field name")
    nullable = (sch.flags & ARROW_FLAG_NULLABLE) != 0
    t = parseformat(fmt, sch.flags)

    # Check the schema shape before indexing any recursively-created child.
    # Struct is the only mapped layout with field-declared arity.
    spec = layoutspec(t)
    expected_children = spec.childcount
    if expected_children >= 0 && sch.n_children != expected_children
        throw(ValidationError("C schema for $(typeof(t)) declares $(sch.n_children) children; expected $expected_children"))
    end

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
        return Field(name, DictionaryType(t, vf.type, ordered);
            nullable=nullable, children=vf.children)
    end
    return Field(name, t; nullable=nullable, children=children)
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
    spec = layoutspec(t)
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
            p == C_NULL && arr.length == 0 && arr.offset == 0 ? Int64(0) :
                AC.checked_mul(AC.checked_add(total, Int64(1)), Int64(spec.offsetwidth))
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
                    O = spec.offsetwidth == 8 ? Int64 : Int32
                    Int64(AC.loadat(offsets_slice, O,
                        AC.checked_mul(total, Int64(sizeof(O)))))
                end
            end
        else
            error("cdata prove-out: role $role import is roadmap slice work")
        end
        if p == C_NULL
            nbytes == 0 || throw(ValidationError("NULL $role buffer with nonzero required size"))
            push!(buffers, BufferSlice())
        else
            region = OwnerRegion(Ptr{UInt8}(p), nbytes, AC.Foreign;
                root=owner, lifecycle=owner.gate)
            slice = BufferSlice(region, 0, nbytes)
            role == AC.OFFSETS && (offsets_slice = slice)
            push!(buffers, slice)
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
    return ArrayData(t, arr.length, buffers; offset=arr.offset,
        children=children, dictionary=dict, owner=owner,
        nullcount=arr.null_count)
end

# ---------------------------------------------------------------------------
# Demo: export -> import round-trip, release lifecycle, failure paths
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
    source_region = d.buffers[2].region
    before = _registry_count()
    sp, ap = to_c_data(f, d)
    @assert !forceclose!(source_region; timeout_ms=0)
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
    @assert forceclose!(source_region; timeout_ms=0)
    return nothing
end

function _expect_invalid_dictionary_topology!(mutate)
    vf, vd = fromjulia("values", ["x"])
    t = DictionaryType(IntType(32, true), vf.type, false)
    f = Field("bad-dictionary", t; nullable=false, children=vf.children)
    d = ArrayData(t, 1, [BufferSlice(), AC._databuffer(Int32[0])];
        dictionary=vd, nullcount=0)
    source_region = vd.buffers[3].region
    before = _registry_count()
    sp, ap = to_c_data(f, d)
    @assert !forceclose!(source_region; timeout_ms=0)
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
    @assert forceclose!(source_region; timeout_ms=0)
    return nothing
end

function _expect_invalid_schema_flags!(flags::Int64)
    f, d = fromjulia("bad-flags", Int64[1])
    source_region = d.buffers[2].region
    before = _registry_count()
    sp, ap = to_c_data(f, d)
    @assert !forceclose!(source_region; timeout_ms=0)
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
    @assert forceclose!(source_region; timeout_ms=0)
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

function main()
    if Sys.WORD_SIZE == 64
        @assert sizeof(CArrowSchema) == 72
        @assert fieldoffset.(Ref(CArrowSchema), 1:9) == 0:8:64
        @assert sizeof(CArrowArray) == 80
        @assert fieldoffset.(Ref(CArrowArray), 1:10) == 0:8:72
    elseif Sys.WORD_SIZE == 32
        if Base.datatype_alignment(Int64) == 4 # i686 SysV ABI
            @assert sizeof(CArrowSchema) == 44
            @assert fieldoffset.(Ref(CArrowSchema), 1:9) == [0, 4, 8, 12, 20, 28, 32, 36, 40]
            @assert sizeof(CArrowArray) == 60
            @assert fieldoffset.(Ref(CArrowArray), 1:10) == [0, 8, 16, 24, 32, 40, 44, 48, 52, 56]
        else # 32-bit ABIs that align int64_t to 8 bytes
            @assert sizeof(CArrowSchema) == 48
            @assert fieldoffset.(Ref(CArrowSchema), 1:9) == [0, 4, 8, 16, 24, 32, 36, 40, 44]
            @assert sizeof(CArrowArray) == 64
            @assert fieldoffset.(Ref(CArrowArray), 1:10) == [0, 8, 16, 24, 32, 40, 44, 48, 52, 56]
        end
    else
        error("unsupported pointer width $(Sys.WORD_SIZE)")
    end
    println("C ABI size and field-offset gate passed for $(Sys.WORD_SIZE)-bit ✓")

    # A reaper may run while an export tree is being built. Partial mallocs
    # and source pins must stay private until the finished tree is published.
    before = _registry_count()
    entered = Base.Event()
    finish = Base.Event()
    builder = @async _newroot(Any[]) do root
        p = _malloc!(root, 64)
        notify(entered)
        wait(finish)
        @assert !isempty(root.mallocs)
        p
    end
    wait(entered)
    @assert _registry_count() == before
    @assert reap!() == 0
    notify(finish)
    fetch(builder)
    @assert _registry_count() == before + 1
    @assert reap!() == 1
    @assert _registry_count() == before
    println("in-progress exports are hidden from the reaper ✓")

    # Every native allocation and source guard must have an owner before the
    # next fallible operation. Inject failures at each ownership handoff.
    deallocations = Ref(0)
    @assert try
        _newroot(Any[]) do root
            _malloc!(root, 64,
                (_ledger, _p) -> error("injected malloc registration failure"),
                p -> begin
                    deallocations[] += 1
                    Libc.free(p)
                end)
        end
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected malloc registration failure"
    end
    @assert deallocations[] == 1
    @assert _registry_count() == before
    # A registration method may append successfully and fail before it
    # returns. In that state root cleanup, not the local catch, owns the entry.
    innerdeallocations = Ref(0)
    @assert try
        _newroot(Any[]) do root
            _malloc!(root, 64,
                (ledger, p) -> begin
                    push!(ledger, p)
                    error("injected post-registration failure")
                end,
                _ -> (innerdeallocations[] += 1))
        end
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected post-registration failure"
    end
    @assert innerdeallocations[] == 0
    @assert _registry_count() == before

    _, pda = fromjulia("pin-a", Int64[1])
    _, pdb = fromjulia("pin-b", Int64[2])
    pdd = ArrayData(StructType(), 1, [BufferSlice()];
        children=[pda, pdb], nullcount=0)
    pinregions = OwnerRegion[pda.buffers[2].region, pdb.buffers[2].region]
    acquirecalls = Ref(0)
    @assert try
        _pin_regions(pdd, region -> begin
            acquirecalls[] += 1
            acquirecalls[] == 2 && error("injected guard acquisition failure")
            AC._acquireguard!(region)
        end)
        false
    catch e
        e isa ErrorException && e.msg == "injected guard acquisition failure"
    end
    @assert acquirecalls[] == 2
    @assert all((@atomic region.guards) == 0 for region in pinregions)

    factoryregion = pda.buffers[2].region
    @assert try
        _newroot(_ -> nothing, Any[pda], pda,
            (_args...) -> error("injected root construction failure"))
        false
    catch e
        e isa ErrorException && e.msg == "injected root construction failure"
    end
    @assert (@atomic factoryregion.guards) == 0
    @assert _registry_count() == before
    @assert try
        _newroot(Any[pda], pda) do root
            @assert (@atomic factoryregion.guards) == 1
            _malloc!(root, 64)
            error("injected export build failure")
        end
        false
    catch e
        e isa ErrorException && e.msg == "injected export build failure"
    end
    @assert (@atomic factoryregion.guards) == 0
    @assert _registry_count() == before
    println("failed export handoffs return mallocs and source guards ✓")

    # Cleanup owns a registry-visible claim until every resource is gone. A
    # failed claim remains retryable, and completed free steps are removed from
    # the ledger before an injected failure can escape.
    _, cleanup_data = fromjulia("cleanup", Int64[1])
    cleanup_region = cleanup_data.buffers[2].region
    cleanup_key = Ref{Int64}(0)
    _newroot(Any[cleanup_data], cleanup_data) do root
        cleanup_key[] = root.key
        _malloc!(root, 64)
        _malloc!(root, 64)
        return nothing
    end
    @assert (@atomic cleanup_region.guards) == 1
    @assert try
        _cleanup_registered_root!(cleanup_key[];
            after_claim=_ -> throw(InterruptException()))
        false
    catch e
        e isa InterruptException
    end
    @assert lock(REGISTRY_LOCK) do
        root = EXPORT_REGISTRY[cleanup_key[]]
        !root.cleaning && length(root.mallocs) == 2 && length(root.pins) == 1
    end
    cleanup_steps = Ref(0)
    @assert try
        _cleanup_registered_root!(cleanup_key[]; after_step=_ -> begin
            cleanup_steps[] += 1
            cleanup_steps[] == 1 && error("injected cleanup step failure")
        end)
        false
    catch e
        e isa ErrorException && e.msg == "injected cleanup step failure"
    end
    @assert lock(REGISTRY_LOCK) do
        root = EXPORT_REGISTRY[cleanup_key[]]
        !root.cleaning && length(root.mallocs) == 1 && length(root.pins) == 1
    end
    @assert reap!() == 1
    @assert lock(REGISTRY_LOCK) do
        !haskey(EXPORT_REGISTRY, cleanup_key[])
    end
    @assert (@atomic cleanup_region.guards) == 0
    @assert forceclose!(cleanup_region; timeout_ms=0)
    println("interrupted export cleanup remains registered and retryable ✓")

    # The registry, not the caller's Julia variables, must keep all source
    # objects and their buffers alive while raw C pointers are outstanding.
    sp, ap, dataref, regionref = _export_and_forget()
    GC.gc(true)
    @assert dataref.value !== nothing
    @assert regionref.value !== nothing
    rootedf, rootedd = from_c_data(sp, ap)
    @assert materialize(rootedf, rootedd) == [1, 2]
    @assert reap!() == 1
    release!(rootedd.owner::ForeignOwner)
    @assert reap!() == 1
    @assert _registry_count() == before
    println("export registry roots dropped Julia sources across GC ✓")

    b = batch((
        xs=Int64[1, 2, 3, 4],
        ys=[1.5, missing, 3.5, missing],
        strs=["a", "", missing, "δεζ"],
        lists=[[1, 2], missing, Int64[], [3]],
    ))
    expected = Dict(
        "xs" => Any[1, 2, 3, 4],
        "ys" => Any[1.5, missing, 3.5, missing],
        "strs" => Any["a", "", missing, "δεζ"],
        "lists" => Any[[1, 2], missing, Int64[], [3]],
    )

    imported = Tuple{Field,ArrayData}[]
    for (f, col) in zip(b.schema.fields, b.columns)
        sp, ap = to_c_data(f, col)
        f2, d2 = from_c_data(sp, ap)
        push!(imported, (f2, d2))
    end
    for (f2, d2) in imported
        got = materialize(f2, d2)
        @assert isequal(collect(Any, got), expected[f2.name]) "$(f2.name): $got"
    end
    println("export → import round-trip for $(length(imported)) columns ✓")
    nlive = _registry_count()
    @assert nlive == 2 * length(imported)
    println("live exports rooted in registry: $nlive")

    # Consumer-side release: drop the imported columns (their ForeignOwners'
    # release calls the exported arrays' release callbacks), then reap.
    for (_, d2) in imported
        release!(d2.owner::ForeignOwner)
    end
    reaped = reap!()
    println("reaped $reaped released exports ✓")

    # Double-release is inert: release the same owners again.
    for (_, d2) in imported
        release!(d2.owner::ForeignOwner)
    end
    @assert reap!() == 0
    println("double release is exactly-once ✓")

    # Explicit owner release closes the shared lifecycle of every buffer in
    # the imported tree. No per-buffer close is needed.
    f2, d2 = imported[1]
    caught = try
        materialize(f2, d2)
        false
    catch e
        e isa InvalidatedError
    end
    @assert caught
    lf, ld = imported[4]
    @assert try
        materialize(lf.children[1], ld.children[1])
        false
    catch e
        e isa InvalidatedError
    end
    println("post-release access is InvalidatedError, not use-after-free ✓")

    # Import of an already-released structure is refused.
    f, col = b.schema.fields[1], b.columns[1]
    sp, ap = to_c_data(f, col)
    _f, _d = from_c_data(sp, ap)      # moves: source release now NULL
    caught = try
        from_c_data(sp, ap)
        false
    catch e
        e isa ArgumentError
    end
    @assert caught
    release!(_d.owner::ForeignOwner)
    @assert reap!() == 2
    println("moved (released) source cannot be imported twice ✓")

    # Ownership transfer must remain exactly-once if the task fails after the
    # source release field is nulled but before the copied owner is armed.
    hf, hd = fromjulia("handoff", Int64[1])
    handoff_region = hd.buffers[2].region
    sp, ap = to_c_data(hf, hd)
    @assert !forceclose!(handoff_region; timeout_ms=0)
    @assert try
        _from_c_data(sp, ap, () -> throw(InterruptException()))
        false
    catch e
        e isa InterruptException
    end
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(ap).release == C_NULL
    @assert reap!() == 2
    @assert _registry_count() == 0
    @assert forceclose!(handoff_region; timeout_ms=0)
    println("interrupted C import handoff retains one owner ✓")

    # A root release must transitively release every child. Inspect before
    # reap, while the exported structs remain allocated.
    lf, ld = b.schema.fields[4], b.columns[4]
    sp, ap = to_c_data(lf, ld)
    schild = unsafe_load(unsafe_load(sp).children, 1)
    achild = unsafe_load(unsafe_load(ap).children, 1)
    _call_release(sp)
    _call_release(ap)
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(schild).release == C_NULL
    @assert unsafe_load(ap).release == C_NULL
    @assert unsafe_load(achild).release == C_NULL
    @assert reap!() == 2
    println("root release is transitive across child trees ✓")

    # C Data move semantics permit a consumer to shallow-copy a child and
    # null the source child's release field. The parent must skip that child,
    # and the aggregate allocation must remain live until the moved copy is
    # released independently.
    sp, ap = to_c_data(lf, ld)
    schild = unsafe_load(unsafe_load(sp).children, 1)
    achild = unsafe_load(unsafe_load(ap).children, 1)
    smoved = Ref(unsafe_load(schild))
    amoved = Ref(unsafe_load(achild))
    _store_field!(schild, :release, Ptr{Cvoid}(C_NULL))
    _store_field!(achild, :release, Ptr{Cvoid}(C_NULL))
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    @assert _registry_count() == 2
    moved_source_region = ld.children[1].buffers[2].region
    @assert !forceclose!(moved_source_region; timeout_ms=0)
    GC.@preserve smoved amoved begin
        smovedp = Base.unsafe_convert(Ptr{CArrowSchema}, smoved)
        amovedp = Base.unsafe_convert(Ptr{CArrowArray}, amoved)
        @assert unsafe_load(smovedp).release != C_NULL
        @assert unsafe_load(amovedp).release != C_NULL
        movedf, movedd = from_c_data(smovedp, amovedp)
        @assert materialize(movedf, movedd) == [1, 2, 3]
        release!(movedd.owner::ForeignOwner)
    end
    @assert reap!() == 2
    @assert forceclose!(moved_source_region; timeout_ms=0)
    println("moved children retain aggregate ownership until release ✓")

    # Raw C pointers hold long-lived access pins. A deterministic close must
    # report busy until the consumer releases and the array root is reaped.
    pf, pd = fromjulia("pinned", Int64[1, 2])
    source_region = pd.buffers[2].region
    sp, ap = to_c_data(pf, pd)
    @assert !forceclose!(source_region; timeout_ms=0)
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 2
    @assert forceclose!(source_region; timeout_ms=0)
    println("C export pins source regions until reap ✓")

    # A Julia exception after a callback claim must return the node to LIVE.
    # The void C entrypoint then retries the idempotent transaction before it
    # returns to the consumer.
    rf, rd = fromjulia("retryable-release", Int64[1])
    retry_region = rd.buffers[2].region
    sp, ap = to_c_data(rf, rd)
    scontrol = unsafe_load(sp).private_data
    acontrol = unsafe_load(ap).private_data
    @assert try
        _release_schema_impl(sp, () -> throw(InterruptException()))
        false
    catch e
        e isa InterruptException
    end
    @assert try
        _release_array_impl(ap, () -> throw(InterruptException()))
        false
    catch e
        e isa InterruptException
    end
    @assert unsafe_load(Ptr{UInt8}(scontrol)) == 0x00
    @assert unsafe_load(Ptr{UInt8}(acontrol)) == 0x00
    @assert unsafe_load(sp).release != C_NULL
    @assert unsafe_load(ap).release != C_NULL
    attempts = Ref(0)
    @assert _release_array_entry(ap, () -> begin
            attempts[] += 1
            attempts[] == 1 && throw(ErrorException("retry once"))
        end) === nothing
    @assert attempts[] == 2
    @assert unsafe_load(Ptr{UInt8}(acontrol)) == 0x02
    @assert unsafe_load(ap).release == C_NULL
    @assert reap!() == 1
    @assert forceclose!(retry_region; timeout_ms=0)
    _call_release(sp)
    @assert reap!() == 1
    println("interrupted C release callbacks remain retryable ✓")

    # Retry must also preserve partial descendant progress. The first child is
    # already NULL on retry, so each child callback runs exactly once.
    c1f, c1d = fromjulia("a", Int64[1])
    c2f, c2d = fromjulia("b", Int64[2])
    tf = Field("tree", StructType(); children=[c1f, c2f])
    td = ArrayData(StructType(), 1, [BufferSlice()];
        children=[c1d, c2d], nullcount=0)
    tree_regions = OwnerRegion[c1d.buffers[2].region, c2d.buffers[2].region]
    tsp, tap = to_c_data(tf, td)
    tcontrol = unsafe_load(tap).private_data
    tkey = unsafe_load(Ptr{Int64}(tcontrol + 8))
    released_children = Ptr{CArrowArray}[]
    _release_array_entry(tap, nothing, child -> begin
        push!(released_children, child)
        length(released_children) == 1 && throw(ErrorException("retry subtree"))
    end)
    tchildren = unsafe_load(tap).children
    @assert length(released_children) == 2
    @assert length(unique(released_children)) == 2
    @assert all(unsafe_load(unsafe_load(tchildren, i)).release == C_NULL for i = 1:2)
    @assert unsafe_load(tap).release == C_NULL
    @assert lock(REGISTRY_LOCK) do
        EXPORT_REGISTRY[tkey].remaining == 0
    end
    _call_release(tsp)
    @assert reap!() == 2
    @assert all(forceclose!(region; timeout_ms=0) for region in tree_regions)
    println("C release retry preserves partial descendant progress ✓")

    # Schema/data mismatch and malformed buffers must fail before either
    # independently-owned export root is published.
    before = _registry_count()
    mf = Field("wrong", IntType(32, true); nullable=false)
    _, md = fromjulia("wrong", Int64[1])
    @assert try
        to_c_data(mf, md)
        false
    catch e
        e isa ValidationError
    end
    short = ArrayData(IntType(64, true), 10,
        [AC._databuffer(UInt8[0xff]), BufferSlice()])
    @assert try
        to_c_data(Field("short", IntType(64, true)), short)
        false
    catch e
        e isa ValidationError
    end
    @assert _registry_count() == before
    println("failed exports leave no registry roots ✓")

    # C strings cannot represent embedded NULs, and Utf8 arrays require
    # valid UTF-8. Reject both before any export root becomes visible.
    badname = Field("embedded\0nul", IntType(64, true); nullable=false)
    @assert try
        to_c_data(badname, md)
        false
    catch e
        e isa ValidationError
    end
    badutf8type = Utf8Type(false)
    badutf8field = Field("bad-utf8", badutf8type)
    badutf8data = ArrayData(badutf8type, 1,
        [BufferSlice(), AC._databuffer(Int32[0, 1]),
         AC._databuffer(UInt8[0xff])]; nullcount=0)
    @assert try
        to_c_data(badutf8field, badutf8data)
        false
    catch e
        e isa ValidationError
    end
    @assert _registry_count() == before
    println("unrepresentable names and invalid UTF-8 fail before export ✓")

    # Dictionary values have independent nullability. Ordered state is a C
    # schema flag, and a non-nullable index may select a null pool value.
    vf, vd = fromjulia("dict", Union{Missing,String}[missing, "x"])
    dt = DictionaryType(IntType(32, true), vf.type, true)
    df = Field("dict", dt; nullable=false, children=vf.children)
    dd = ArrayData(dt, 2,
        [BufferSlice(), AC._databuffer(Int32[0, 1])];
        dictionary=vd, nullcount=0)
    sp, ap = to_c_data(df, dd)
    @assert (unsafe_load(sp).flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0
    df2, dd2 = from_c_data(sp, ap)
    @assert (df2.type::DictionaryType).ordered
    @assert isequal(materialize(df2, dd2), [missing, "x"])
    release!(dd2.owner::ForeignOwner)
    @assert reap!() == 2
    println("dictionary ordered flag and nullable pool values round-trip ✓")

    sp, ap = to_c_data(df, dd)
    sdict = unsafe_load(sp).dictionary
    adict = unsafe_load(ap).dictionary
    smoved = Ref(unsafe_load(sdict))
    amoved = Ref(unsafe_load(adict))
    _store_field!(sdict, :release, Ptr{Cvoid}(C_NULL))
    _store_field!(adict, :release, Ptr{Cvoid}(C_NULL))
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    GC.@preserve smoved amoved begin
        movedf, movedd = from_c_data(
            Base.unsafe_convert(Ptr{CArrowSchema}, smoved),
            Base.unsafe_convert(Ptr{CArrowArray}, amoved))
        @assert isequal(materialize(movedf, movedd), [missing, "x"])
        release!(movedd.owner::ForeignOwner)
    end
    @assert reap!() == 2
    println("moved dictionaries retain aggregate ownership until release ✓")

    kf, kd = fromjulia("key", ["a"])
    mvf, mvd = fromjulia("value", Int64[7])
    entriesf = Field("entries", StructType(); nullable=false,
        children=[kf, mvf])
    entriesd = ArrayData(StructType(), 1, [BufferSlice()];
        children=[kd, mvd], nullcount=0)
    mt = MapType(true)
    mapf = Field("map", mt; children=[entriesf])
    mapd = ArrayData(mt, 1,
        [BufferSlice(), AC._databuffer(Int32[0, 1])];
        children=[entriesd], nullcount=0)
    sp, ap = to_c_data(mapf, mapd)
    @assert (unsafe_load(sp).flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0
    mapf2, mapd2 = from_c_data(sp, ap)
    @assert (mapf2.type::MapType).keyssorted
    @assert materialize(mapf2, mapd2) == [["a" => 7]]
    release!(mapd2.owner::ForeignOwner)
    @assert reap!() == 2
    println("map sorted-key flag round-trips ✓")

    # Moving a nested subtree keeps all of its descendants live. Releasing
    # the moved entries struct recursively releases its key/value children.
    sp, ap = to_c_data(mapf, mapd)
    sentries = unsafe_load(unsafe_load(sp).children, 1)
    aentries = unsafe_load(unsafe_load(ap).children, 1)
    smoved = Ref(unsafe_load(sentries))
    amoved = Ref(unsafe_load(aentries))
    _store_field!(sentries, :release, Ptr{Cvoid}(C_NULL))
    _store_field!(aentries, :release, Ptr{Cvoid}(C_NULL))
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    GC.@preserve smoved amoved begin
        movedf, movedd = from_c_data(
            Base.unsafe_convert(Ptr{CArrowSchema}, smoved),
            Base.unsafe_convert(Ptr{CArrowArray}, amoved))
        @assert materialize(movedf, movedd) == [(key="a", value=7)]
        release!(movedd.owner::ForeignOwner)
    end
    @assert reap!() == 2
    println("moved nested subtrees retain descendants until release ✓")

    # Two moved siblings keep one aggregate alive. Releasing the first does
    # not free either tree; the second release performs the single reap.
    af, ad = fromjulia("a", Int64[1, 2])
    bf, bd = fromjulia("b", Int64[3, 4])
    sf = Field("s", StructType(); children=[af, bf])
    sd = ArrayData(StructType(), 2, [BufferSlice()];
        children=[ad, bd], nullcount=0)
    sp, ap = to_c_data(sf, sd)
    smoved = Ref{CArrowSchema}[]
    amoved = Ref{CArrowArray}[]
    for i = 1:2
        source_s = unsafe_load(unsafe_load(sp).children, i)
        source_a = unsafe_load(unsafe_load(ap).children, i)
        push!(smoved, Ref(unsafe_load(source_s)))
        push!(amoved, Ref(unsafe_load(source_a)))
        _store_field!(source_s, :release, Ptr{Cvoid}(C_NULL))
        _store_field!(source_a, :release, Ptr{Cvoid}(C_NULL))
    end
    _call_release(sp)
    _call_release(ap)
    @assert reap!() == 0
    for (i, expected_values) in enumerate(([1, 2], [3, 4]))
        GC.@preserve smoved amoved begin
            movedf, movedd = from_c_data(
                Base.unsafe_convert(Ptr{CArrowSchema}, smoved[i]),
                Base.unsafe_convert(Ptr{CArrowArray}, amoved[i]))
            @assert materialize(movedf, movedd) == expected_values
            release!(movedd.owner::ForeignOwner)
        end
        @assert reap!() == (i == 2 ? 2 : 0)
    end
    println("multiple moved siblings defer one aggregate reap ✓")

    # Even when every imported buffer pointer is NULL, ArrayData owns the
    # ForeignOwner. GC cannot release the producer while the empty array lives.
    ef, ed = fromjulia("empty", Int64[])
    sp, ap = to_c_data(ef, ed)
    ef2, ed2 = from_c_data(sp, ap)
    @assert reap!() == 1                    # schema only
    ownerref = WeakRef(ed2.owner)
    GC.gc(true)
    @assert ownerref.value !== nothing
    @assert _registry_count() == 1          # array producer still rooted
    @assert isempty(materialize(ef2, ed2))
    release!(ed2.owner::ForeignOwner)
    @assert reap!() == 1
    println("empty imports retain their shared foreign owner ✓")

    # Natural collection of the shared lifecycle gate is also an exactly-once
    # release path. The array producer and its source pin must not depend on a
    # caller remembering the deterministic release! convenience.
    ff, fd = fromjulia("finalized", Int64[1])
    finalized_source_region = fd.buffers[2].region
    sp, ap = to_c_data(ff, fd)
    @assert !forceclose!(finalized_source_region; timeout_ms=0)
    _import_and_forget(sp, ap)
    finalized_reaped = reap!()
    for _ = 1:10
        finalized_reaped == 2 && break
        GC.gc(true)
        yield()
        finalized_reaped += reap!()
    end
    @assert finalized_reaped == 2
    @assert _registry_count() == 0
    @assert forceclose!(finalized_source_region; timeout_ms=0)
    println("natural foreign-owner finalization releases the producer ✓")

    # Verifiable C structural failures are clean errors and still release
    # both moved lifetimes exactly once.
    bf, bd = fromjulia("bad", Int64[1])
    sp, ap = to_c_data(bf, bd)
    _store_field!(ap, :buffers, Ptr{Ptr{Cvoid}}(C_NULL))
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert reap!() == 2
    @assert _registry_count() == 0
    println("invalid C pointer tables fail with exact cleanup ✓")

    # Flags carry schema semantics, so the importer must reject unknown bits
    # and known flags on layouts where those meanings do not apply. Silent
    # acceptance would discard information that this adapter cannot preserve.
    _expect_invalid_schema_flags!(Int64(8))
    _expect_invalid_schema_flags!(ARROW_FLAG_DICTIONARY_ORDERED)
    _expect_invalid_schema_flags!(ARROW_FLAG_MAP_KEYS_SORTED)
    println("unknown and type-invalid schema flags fail with exact cleanup ✓")

    # A failed import invokes producer callbacks after it has copied the
    # caller-visible structs. Cleanup must therefore use the topology that the
    # producer recorded at export time. Otherwise a NULL child table crashes
    # the callback, while a forged zero child count strands descendants and
    # source pins. Cover both schema and array roots.
    _expect_invalid_list_topology!() do _sp, ap
        _store_field!(ap, :children, Ptr{Ptr{CArrowArray}}(C_NULL))
    end
    _expect_invalid_list_topology!() do sp, _ap
        _store_field!(sp, :children, Ptr{Ptr{CArrowSchema}}(C_NULL))
    end
    _expect_invalid_list_topology!() do _sp, ap
        _store_field!(ap, :n_children, Int64(0))
    end
    _expect_invalid_list_topology!() do sp, _ap
        _store_field!(sp, :n_children, Int64(0))
    end
    _expect_invalid_dictionary_topology!() do _sp, ap
        _store_field!(ap, :dictionary, Ptr{CArrowArray}(C_NULL))
    end
    _expect_invalid_dictionary_topology!() do sp, _ap
        _store_field!(sp, :dictionary, Ptr{CArrowSchema}(C_NULL))
    end
    println("malformed public topology cannot corrupt producer cleanup ✓")

    # Imported C names and Utf8 buffers receive the same full validation.
    # Both failures happen after the array move, so both producer lifetimes
    # must still be released exactly once.
    nf, nd = fromjulia("name", Int64[1])
    sp, ap = to_c_data(nf, nd)
    unsafe_store!(unsafe_load(sp).name, 0xff, 1)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert reap!() == 2
    @assert _registry_count() == 0

    uf, ud = fromjulia("utf8", ["a"])
    sp, ap = to_c_data(uf, ud)
    datap = Ptr{UInt8}(unsafe_load(unsafe_load(ap).buffers, 3))
    unsafe_store!(datap, 0xff, 1)
    @assert try
        from_c_data(sp, ap)
        false
    catch e
        e isa ValidationError
    end
    @assert reap!() == 2
    @assert _registry_count() == 0
    println("invalid imported names and UTF-8 fail with exact cleanup ✓")

    println()
    println("C Data ownership and round-trip checks passed.")
end

main()
