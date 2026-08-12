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
# The point of the whole Core design is that this file is SMALL and BORING:
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
#     release callback recursively marks the C tree released. A reaper pass
#     scans for aggregates whose last outstanding node was released, frees
#     mallocs, drops the registry root, and releases
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
# The demo: build a Core batch (nullable ints, strings, list column) →
# export to C structs → wipe our references → import from the C structs →
# materialize and compare → consumer calls release → reap → assert the
# registry is empty and double-release is inert.
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
# Export: Core -> C structs, control block + registry + reap queue
# ---------------------------------------------------------------------------

# Per-node control block layout (malloc'd, never GC-scanned):
#   offset 0: UInt8 state (0 = live, 1 = releasing, 2 = released)
#   offset 8: Int64 registry key
const CONTROL_BLOCK_BYTES = 16

"""
Everything one export must keep alive and eventually free: the Core columns
(whose OwnerRegions root the actual buffers), every malloc'd C struct and
string, and the control block. Held in EXPORT_REGISTRY under the control
block's key until the consumer calls release and the reaper runs.
"""
mutable struct ExportedRoot
    roots::Vector{Any}          # ArrayData/Field/Schema kept reachable
    mallocs::Vector{Ptr{Cvoid}} # every Libc.malloc'd allocation, freed on reap
    pins::Vector{OwnerRegion}   # long-lived source access guards for C pointers
    key::Int64
    remaining::Int64           # exported C nodes whose callback has not run
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
        flag = unsafe_load(Ptr{UInt8}(p))
        flag == 0x00 || return nothing
        unsafe_store!(Ptr{UInt8}(p), 0x01)
        (arr, p)
    end
end

function _claim_schema_node(s::Ptr{CArrowSchema})
    s == C_NULL && return nothing
    return lock(REGISTRY_LOCK) do
        sch = unsafe_load(s)
        sch.release == C_NULL && return nothing
        p = sch.private_data
        p == C_NULL && return nothing
        flag = unsafe_load(Ptr{UInt8}(p))
        flag == 0x00 || return nothing
        unsafe_store!(Ptr{UInt8}(p), 0x01)
        (sch, p)
    end
end

function _finish_node!(p, control::Ptr{Cvoid})
    # This locked block is the callback's final access to export-owned memory.
    # The reaper observes zero only after every non-moved descendant callback,
    # and every independently moved node callback, has completed. Scanning in
    # reap! keeps allocation and queue mutation out of the C callback.
    lock(REGISTRY_LOCK) do
        unsafe_load(Ptr{UInt8}(control)) == 0x01 ||
            error("C Data node is not in releasing state")
        _store_field!(p, :release, Ptr{Cvoid}(C_NULL))
        key = unsafe_load(Ptr{Int64}(control + 8))
        root = get(EXPORT_REGISTRY, key, nothing)
        root === nothing && error("C Data export root disappeared during release")
        root.remaining > 0 || error("C Data export node counter underflow")
        unsafe_store!(Ptr{UInt8}(control), 0x02)
        root.remaining -= 1
    end
    return nothing
end

function _release_array_children!(arr::CArrowArray)
    for i = 1:arr.n_children
        child = unsafe_load(arr.children, i)
        child == C_NULL && continue
        release = lock(REGISTRY_LOCK) do
            unsafe_load(child).release
        end
        release == C_NULL || ccall(release, Cvoid, (Ptr{CArrowArray},), child)
    end
    if arr.dictionary != C_NULL
        release = lock(REGISTRY_LOCK) do
            unsafe_load(arr.dictionary).release
        end
        release == C_NULL ||
            ccall(release, Cvoid, (Ptr{CArrowArray},), arr.dictionary)
    end
    return nothing
end

function _release_schema_children!(sch::CArrowSchema)
    for i = 1:sch.n_children
        child = unsafe_load(sch.children, i)
        child == C_NULL && continue
        release = lock(REGISTRY_LOCK) do
            unsafe_load(child).release
        end
        release == C_NULL || ccall(release, Cvoid, (Ptr{CArrowSchema},), child)
    end
    if sch.dictionary != C_NULL
        release = lock(REGISTRY_LOCK) do
            unsafe_load(sch.dictionary).release
        end
        release == C_NULL ||
            ccall(release, Cvoid, (Ptr{CArrowSchema},), sch.dictionary)
    end
    return nothing
end

function _release_array(a::Ptr{CArrowArray})
    claimed = _claim_array_node(a)
    claimed === nothing && return nothing
    arr, control = claimed
    _release_array_children!(arr)
    _finish_node!(a, control)
    return nothing
end

function _release_schema(s::Ptr{CArrowSchema})
    claimed = _claim_schema_node(s)
    claimed === nothing && return nothing
    sch, control = claimed
    _release_schema_children!(sch)
    _finish_node!(s, control)
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

"""
    reap!() -> Int

Find fully released exports: free every malloc they own and drop their
registry roots. In the real adapter this is a background reaper task; the
example calls it explicitly to keep the demo deterministic.
"""
function reap!()
    roots = lock(REGISTRY_LOCK) do
        keys = Int64[k for (k, root) in EXPORT_REGISTRY if root.remaining == 0]
        ExportedRoot[pop!(EXPORT_REGISTRY, k) for k in keys]
    end
    for root in roots
        _free_export!(root)
    end
    return length(roots)
end

_malloc!(root::ExportedRoot, n::Integer) = begin
    n >= 0 || throw(ArgumentError("negative export allocation size"))
    n64 = Int64(n)
    p = Libc.malloc(max(n64, Int64(1)))
    p == C_NULL && throw(OutOfMemoryError())
    push!(root.mallocs, p)
    Ptr{Cvoid}(p)
end

function _cstring!(root::ExportedRoot, s::AbstractString)
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
    childptrs = Ptr{Ptr{CArrowSchema}}(C_NULL)
    if nchildren > 0
        childptrs = Ptr{Ptr{CArrowSchema}}(_malloc!(root,
            AC.checked_mul(Int64(nchildren), Int64(sizeof(Ptr)))))
        for (i, cf) in enumerate(childfields)
            unsafe_store!(childptrs, _export_schema!(root, cf, release), i)
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
    childptrs = Ptr{Ptr{CArrowArray}}(C_NULL)
    if nchildren > 0
        childptrs = Ptr{Ptr{CArrowArray}}(_malloc!(root,
            AC.checked_mul(Int64(nchildren), Int64(sizeof(Ptr)))))
        for (i, c) in enumerate(d.children)
            unsafe_store!(childptrs, _export_array!(root, c, release), i)
        end
    end
    dict = d.dictionary === nothing ? Ptr{CArrowArray}(C_NULL) :
        _export_array!(root, d.dictionary, release)
    control = _newcontrol!(root)
    unsafe_store!(p, CArrowArray(d.len, nullcount(d), d.offset, nbuf,
        nchildren, bufptrs, childptrs, dict,
        release, control))
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
    arel = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
    srel = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
    sp = _newroot(Any[f]) do root
        _export_schema!(root, f, srel)
    end
    try
        pins = _pin_regions(d)
        ap = _newroot(Any[d]; pins=pins) do root
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

function _pin_regions(d::ArrayData)
    pins = collect(keys(_walk_regions!(IdDict{OwnerRegion,Nothing}(), d)))
    acquired = OwnerRegion[]
    try
        for region in pins
            AC._acquireguard!(region)
            push!(acquired, region)
        end
        return acquired
    catch
        for region in acquired
            AC._releaseguard!(region)
        end
        rethrow()
    end
end

function _free_export!(root::ExportedRoot)
    for m in root.mallocs
        Libc.free(m)
    end
    empty!(root.mallocs)
    empty!(root.roots)
    for region in root.pins
        AC._releaseguard!(region)
    end
    empty!(root.pins)
    return nothing
end

function _discard_export!(p::Ptr)
    p == C_NULL && return nothing
    control = unsafe_load(p).private_data
    key = unsafe_load(Ptr{Int64}(control + 8))
    root = lock(REGISTRY_LOCK) do
        pop!(EXPORT_REGISTRY, key, nothing)
    end
    root === nothing || _free_export!(root)
    return nothing
end

function _newroot(build, roots::Vector{Any}; pins::Vector{OwnerRegion}=OwnerRegion[])
    key = try
        lock(REGISTRY_LOCK) do
            NEXT_KEY[] = AC.checked_add(NEXT_KEY[], Int64(1))
        end
    catch
        for region in pins
            AC._releaseguard!(region)
        end
        rethrow()
    end
    root = ExportedRoot(roots, Ptr{Cvoid}[], pins, key, 0)
    lock(REGISTRY_LOCK) do
        EXPORT_REGISTRY[key] = root
    end
    try
        return build(root)
    catch
        # Export-failure cleanup path: unpublish and free everything built
        # so far, exactly once, then rethrow (report §9).
        lock(REGISTRY_LOCK) do
            pop!(EXPORT_REGISTRY, key, nothing)
        end
        _free_export!(root)
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
        # The gate has no data extent. Its finalizer is the shared-mode
        # backstop. Every imported BufferSlice guards this same lifecycle.
        o.gate = OwnerRegion(Ptr{UInt8}(0), 0, AC.Foreign; root=o,
            releasefn=_release_foreign_tree!)
        return o
    end
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

Import (MOVE) a C-data column. Per spec the source structures are consumed:
we copy them by value and null the source's release so the producer side
cannot double-free. Buffer extents are computed from length/offset/layout —
DECLARED extents (report §9): the ABI cannot prove the allocation sizes, so
this is the trusted-in-process boundary, and validation runs on the declared
geometry. A failed import releases the moved tree exactly once.
"""
function from_c_data(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray})
    sp == C_NULL && throw(ArgumentError("ArrowSchema pointer is NULL"))
    ap == C_NULL && throw(ArgumentError("ArrowArray pointer is NULL"))
    sch = unsafe_load(sp)
    arr = unsafe_load(ap)
    (sch.release == C_NULL || arr.release == C_NULL) &&
        throw(ArgumentError("cannot import a released structure"))
    owner = ForeignOwner(arr)
    # MOVE: the source array struct no longer owns anything.
    _store_field!(ap, :release, Ptr{Cvoid}(C_NULL))
    try
        _preflight_schema(sch)
        f = _import_field(sch)
        _preflight_array(f, arr)
        d = _import_array(f, arr, owner)
        validate_structural(f, d)
        validate_semantic(f, d)
        return f, d
    catch
        release!(owner)   # failed-import cleanup: exactly once, then rethrow
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

function _import_field(sch::CArrowSchema)::Field
    fmt = unsafe_string(sch.format)
    name = sch.name == C_NULL ? "" : unsafe_string(sch.name)
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
    println("dictionary flags and value nullability round-trip ✓")

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

    println()
    println("C Data ownership and round-trip checks passed.")
end

main()
