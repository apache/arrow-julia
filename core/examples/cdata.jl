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
#     was released, frees mallocs, and drops the registry root — dropping the
#     root is what lets the source columns (and, through their OwnerRegion
#     roots, the actual buffer memory) become collectable again. Prove-out
#     callback contract: releases for one tree are serialized and run only on
#     Julia-attached threads. A native foreign-thread, concurrent
#     trampoline/queue is production adapter work.
#
#   * Import: the moved ArrowArray becomes ONE ForeignOwner shared by every
#     child/dictionary BufferSlice (a single release for the whole tree —
#     per-buffer owners would double-release). Buffer extents are DECLARED,
#     not verified: computed from length/offset/layout per the report's
#     "trusted in-process ABI" rule; offsets buffers are read (bounded by
#     their computed size) to size the data buffers they govern. Failed
#     imports release the moved structure exactly once before throwing.
#     Per spec, moving marks the source released (release = NULL).
#     Validity is reachability (Core rule 2): every imported region's `root`
#     is the ForeignOwner, so the producer's memory outlives every slice by
#     construction. After an EXPLICIT release! the caller must not touch the
#     tree again — the same post-release undefined behavior the C Data spec
#     itself imposes. There is no revocation machinery.
#
# The demo includes a registry-rooting round trip that drops all Julia source
# references before GC and import. It also exports a Core batch (integer,
# nullable floating-point, string, and list columns), materializes and compares
# imported columns, releases and reaps them, and proves that the registry is
# empty and double release is inert.
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
            _store_field!(p, :release, Ptr{Cvoid}(C_NULL))
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
                _store_field!(p, :release, oldrelease)
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
    claimed_slot = Ref{Any}(nothing)
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
    claimed_slot = Ref{Any}(nothing)
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

_malloc!(root::ExportedRoot, n::Integer,
    register! = push!, deallocate! = Libc.free) = begin
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
The array root keeps the source ArrayData reachable until it is reaped;
that reachability is what keeps the exported buffer pointers valid.
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
    # either independently-owned C root.
    validate_structural(f, d)
    validate_semantic(f, d)
    validate_full(f, d)
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
    key = Int64(0)
    root = nothing
    try
        key = lock(REGISTRY_LOCK) do
            NEXT_KEY[] = AC.checked_add(NEXT_KEY[], Int64(1))
        end
        root = ExportedRoot(roots, Ptr{Cvoid}[], key, 0,
            Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowSchema}},Ptr{CArrowSchema}}}(),
            Dict{Ptr{Cvoid},Tuple{Vector{Ptr{CArrowArray}},Ptr{CArrowArray}}}())::ExportedRoot
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
        if root !== nothing
            result_slot === nothing || (result_slot[] = C_NULL)
            key_slot === nothing || (key_slot[] = 0)
            _cleanup_private_root!(root, key)
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
exactly once — from `release!` or the GC finalizer, whichever comes first.

The malloc'd copy of the moved struct mirrors the C Data convention for its
own state: its release field is NULL (inert) until the move commits, and the
producer's real callback is stored only then (`_arm_foreign_owner!`). A
failure between construction and the move commit therefore frees just our
copy and never calls the producer — the source, whose release field is still
set, remains the owner.
"""
mutable struct ForeignOwner
    const arrayblock::Ptr{CArrowArray} # malloc'd copy of the moved struct: a
                                       # stable native address for the
                                       # producer's release callback
    const producer_release::Ptr{Cvoid} # the moved struct's real callback
    @atomic released::Bool             # one swap picks the single releaser
    function ForeignOwner(arr::CArrowArray, registerfinalizer)
        block = Libc.malloc(sizeof(CArrowArray))
        block == C_NULL && throw(OutOfMemoryError())
        p = Ptr{CArrowArray}(block)
        unsafe_store!(p, arr)
        _store_field!(p, :release, Ptr{Cvoid}(C_NULL))  # inert until armed
        o = new(p, arr.release, false)
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
    _store_field!(o.arrayblock, :release, o.producer_release)
    return nothing
end

_foreign_owner_armed(o::ForeignOwner) =
    unsafe_load(o.arrayblock).release != C_NULL

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
function release!(o::ForeignOwner)
    @atomicswap(o.released = true) && return nothing
    cb = unsafe_load(o.arrayblock).release
    if cb != C_NULL
        ccall(cb, Cvoid, (Ptr{CArrowArray},), o.arrayblock)
        unsafe_load(o.arrayblock).release == C_NULL ||
            (Libc.free(o.arrayblock);
                error("C Data producer release did not mark the structure released"))
    end
    Libc.free(o.arrayblock)
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
            _store_field!(ap, :release, Ptr{Cvoid}(C_NULL))
            _arm_foreign_owner!(owner)
            _preflight_schema(sch)
            f = _import_field(sch)
            _preflight_array(f, arr)
            d = _import_array(f, arr, owner)
            validate_structural(f, d)
            validate_semantic(f, d)
            validate_full(f, d)
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
    unsafe_load(sp).release == C_NULL ||
        error("C Data producer release did not mark the structure released")
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
                    if spec.offsetwidth == 8
                        AC.loadat(offsets_slice, Int64,
                            AC.checked_mul(total, Int64(8)))
                    else
                        Int64(AC.loadat(offsets_slice, Int32,
                            AC.checked_mul(total, Int64(4))))
                    end
                end
            end
        else
            error("cdata prove-out: role $role import is roadmap slice work")
        end
        if p == C_NULL
            nbytes == 0 || throw(ValidationError("NULL $role buffer with nonzero required size"))
            push!(buffers, BufferSlice())
        else
            region = OwnerRegion(Ptr{UInt8}(p), nbytes; root=owner)
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
    # must stay private until the finished tree is published.
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

    # Every native allocation and source lifetime must have an owner before the
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
    # The allocator result is owned before the first later fallible action.
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

    # Published schema and array roots do not transfer until the result tuple
    # reaches the caller. Failure at either return boundary cleans both roots.
    handofff, handoffd = fromjulia("export-handoff", Int64[1])
    handoff_arel = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
    handoff_srel = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
    # Plain build + cleanup releases both roots and empties the slots.
    sp_slot = Ref{Ptr{CArrowSchema}}(C_NULL)
    skey_slot = Ref{Int64}(0)
    ap_slot = Ref{Ptr{CArrowArray}}(C_NULL)
    akey_slot = Ref{Int64}(0)
    _build_c_data!(sp_slot, skey_slot, ap_slot, akey_slot,
        handofff, handoffd, handoff_arel, handoff_srel)
    _cleanup_export_slots!(sp_slot, skey_slot, ap_slot, akey_slot)
    @assert sp_slot[] == C_NULL && ap_slot[] == C_NULL
    @assert skey_slot[] == 0 && akey_slot[] == 0
    @assert _registry_count() == before
    println("failed export handoffs return every malloc and registry root ✓")

    # Reap claims a fully released root by removing it from the registry
    # first, then freeing. Frees cannot fail, so no retry protocol exists —
    # the claim IS the removal.
    _, cleanup_data = fromjulia("cleanup", Int64[1])
    cleanup_key = Ref{Int64}(0)
    _newroot(Any[cleanup_data]) do root
        cleanup_key[] = root.key
        _malloc!(root, 64)
        _malloc!(root, 64)
        return nothing
    end
    @assert lock(REGISTRY_LOCK) do
        length(EXPORT_REGISTRY[cleanup_key[]].mallocs) == 2
    end
    @assert reap!() == 1
    @assert lock(REGISTRY_LOCK) do
        !haskey(EXPORT_REGISTRY, cleanup_key[])
    end
    println("reap claims by registry removal and frees every malloc ✓")

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

    # Explicit owner release is one call for the whole imported tree — no
    # per-buffer close exists. What it does NOT do is revoke access: touching
    # a slice after an explicit release! is undefined behavior, exactly the
    # post-release rule the C Data spec imposes on its own consumers. The
    # checkable contract is the exactly-once flag every owner carries.
    for (_, d2) in imported
        @assert (@atomic (d2.owner::ForeignOwner).released)
    end
    println("released owners are flagged; post-release access is out of contract ✓")

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



    # Schema cleanup is installed before owner construction. If construction
    # fails, the array remains with its source while the schema is released.
    cf, cd = fromjulia("owner-construction", Int64[1])
    cbefore = _registry_count()
    sp, ap = to_c_data(cf, cd)
    @assert try
        _from_c_data(sp, ap;
            ownerfactory=_ -> error("injected owner construction failure"))
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected owner construction failure"
    end
    @assert unsafe_load(sp).release == C_NULL
    @assert unsafe_load(ap).release != C_NULL
    @assert reap!() == 1                       # schema root only
    @assert _registry_count() == cbefore + 1   # array root still owed to source
    _call_release(ap)
    @assert reap!() == 1
    @assert _registry_count() == cbefore

    # Finalizer registration is the last ownership handoff in construction.
    # If a registrar installs the finalizer and then throws, constructor
    # cleanup frees the inert malloc'd copy without releasing the producer.
    rf, rd = fromjulia("finalizer-registration", Int64[1])
    rbefore = _registry_count()
    sp, ap = to_c_data(rf, rd)
    _release_c_schema!(sp, unsafe_load(sp))
    captured_owner = Ref{Any}(nothing)
    failing_registrar = (f, o) -> begin
        captured_owner[] = o
        finalizer(f, o)
        error("injected post-registration failure")
    end
    @assert try
        ForeignOwner(unsafe_load(ap), failing_registrar)
        false
    catch e
        e isa ErrorException &&
            e.msg == "injected post-registration failure"
    end
    failed_owner = captured_owner[]::ForeignOwner
    @assert (@atomic failed_owner.released)
    @assert unsafe_load(ap).release != C_NULL
    finalize(failed_owner)
    release!(failed_owner)
    @assert unsafe_load(ap).release != C_NULL
    @assert reap!() == 1                       # schema root only
    _call_release(ap)
    @assert reap!() == 1
    @assert _registry_count() == rbefore
    println("failed finalizer registration frees only the inert owner copy ✓")

    # Producer C callbacks have no error channel. release! calls the
    # persistent malloc'd copy once, checks the producer nulled the copy's
    # release field (the C Data conformance rule), then frees the copy.
    pf, pd = fromjulia("producer-release", Int64[1])
    sp, ap = to_c_data(pf, pd)
    _release_c_schema!(sp, unsafe_load(sp))
    arr = unsafe_load(ap)
    producer_owner = ForeignOwner(arr)
    @assert !_foreign_owner_armed(producer_owner)  # inert until the move commits
    _store_field!(ap, :release, Ptr{Cvoid}(C_NULL))
    _arm_foreign_owner!(producer_owner)
    @assert _foreign_owner_armed(producer_owner)
    release!(producer_owner)
    @assert (@atomic producer_owner.released)
    release!(producer_owner)                       # idempotent
    @assert reap!() == 2
    println("producer release is one committed, conformance-checked step ✓")

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
    println("moved children retain aggregate ownership until release ✓")

    # The void C release entrypoints are claim/commit transactions with no
    # error channel: a completed release commits exactly once, and a repeat
    # call on a released structure is inert.
    rf, rd = fromjulia("plain-release", Int64[1])
    sp, ap = to_c_data(rf, rd)
    acontrol = unsafe_load(ap).private_data
    _call_release(ap)
    @assert unsafe_load(Ptr{UInt8}(acontrol)) == 0x02
    @assert unsafe_load(ap).release == C_NULL
    _call_release(ap)   # inert repeat
    @assert reap!() == 1
    _call_release(sp)
    @assert reap!() == 1
    println("C release entrypoints commit exactly once and repeats are inert ✓")

    # A persistent internal error must not spin forever inside the void C
    # callback. The claimed parent returns to LIVE. Completed descendants
    # stay NULL, and a later explicit call can resume safely.
    retryf, retryd = fromjulia("child", Int64[1])
    retrysf = Field("parent", StructType(); children=[retryf])
    retrysd = ArrayData(StructType(), 1, [BufferSlice()];
        children=[retryd], nullcount=0)
    sp, ap = to_c_data(retrysf, retrysd)
    parentcontrol = unsafe_load(ap).private_data
    childp = unsafe_load(unsafe_load(ap).children, 1)
    childcontrol = unsafe_load(childp).private_data
    retrykey = unsafe_load(Ptr{Int64}(parentcontrol + 8))
    childtopology = lock(REGISTRY_LOCK) do
        pop!(EXPORT_REGISTRY[retrykey].array_topology, childcontrol)
    end
    _call_release(ap)
    @assert unsafe_load(ap).release != C_NULL
    @assert unsafe_load(childp).release != C_NULL
    @assert unsafe_load(Ptr{UInt8}(parentcontrol)) == 0x00
    lock(REGISTRY_LOCK) do
        EXPORT_REGISTRY[retrykey].array_topology[childcontrol] = childtopology
    end
    _call_release(ap)
    @assert unsafe_load(ap).release == C_NULL
    @assert unsafe_load(childp).release == C_NULL
    _call_release(sp)
    @assert reap!() == 2
    println("failed C release callbacks return LIVE and resume on a later call ✓")

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
        @assert materialize(movedf, movedd) == [["key" => "a", "value" => 7]]
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

    # Natural collection of a forgotten imported tree is also an exactly-once
    # release path: the ForeignOwner finalizer runs the producer callback, so
    # the export root becomes reapable without any caller calling release!.
    ff, fd = fromjulia("finalized", Int64[1])
    sp, ap = to_c_data(ff, fd)
    _import_and_forget(sp, ap)
    finalized_reaped = reap!()
    for _ = 1:10
        finalized_reaped == 2 && break
        GC.gc(true)
        yield()   # let queued finalizer work drain before rescanning
        finalized_reaped += reap!()
    end
    @assert finalized_reaped == 2
    @assert _registry_count() == 0
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
    # the callback, while a forged zero child count strands descendants in
    # the registry forever. Cover both schema and array roots.
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
