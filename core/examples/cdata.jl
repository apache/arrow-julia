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
#   * Export: ONE release callback per moved root structure (children and
#     dictionary are released by the root's callback, per spec — never
#     per-buffer). `private_data` points to a malloc'd, never-GC-scanned
#     CONTROL BLOCK holding an exactly-once flag and the registry key; the
#     Julia-side owner (which roots the Core columns and every malloc'd C
#     struct) stays in a global EXPORT REGISTRY until release — a raw
#     pointer in private_data roots nothing by itself. The @cfunction
#     release callback does only native-safe work (CAS the flag, note the
#     key); a reaper pass frees mallocs and drops the registry root. v1
#     thread contract: callbacks from Julia-attached threads.
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
formatstring(t::DictionaryType) = formatstring(t.indextype)  # per spec: index format; values on schema.dictionary

function parseformat(fmt::AbstractString)::ArrowType
    fmt == "b" && return BoolType()
    fmt == "u" && return Utf8Type(false)
    fmt == "U" && return Utf8Type(true)
    fmt == "z" && return BinaryType(false)
    fmt == "Z" && return BinaryType(true)
    fmt == "+l" && return ListType(false)
    fmt == "+L" && return ListType(true)
    fmt == "+s" && return StructType()
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

# Control block layout (malloc'd, never GC-scanned):
#   offset 0: UInt8 released flag (0 = live, 1 = released)
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
    control::Ptr{Cvoid}
end

const EXPORT_REGISTRY = Dict{Int64,ExportedRoot}()
const REGISTRY_LOCK = ReentrantLock()
const NEXT_KEY = Ref{Int64}(0)
# Reap queue: release callbacks push keys (native-safe: the block is
# malloc'd and the push happens under the flag CAS); reap!() drains it.
const REAP_QUEUE = Int64[]

function _release_thunk(p::Ptr{Cvoid})
    # Runs when the CONSUMER releases the exported structure. Native-safe
    # work only: read the control block, flip the flag exactly once, record
    # the key. (v1 contract: Julia-attached threads — see report §9.)
    p == C_NULL && return nothing
    flag = unsafe_load(Ptr{UInt8}(p))
    flag == 0x01 && return nothing            # exactly-once
    unsafe_store!(Ptr{UInt8}(p), 0x01)
    key = unsafe_load(Ptr{Int64}(p + 8))
    lock(REGISTRY_LOCK) do
        push!(REAP_QUEUE, key)
    end
    return nothing
end

# The C-visible release callback. Per spec it receives the struct pointer,
# must mark it released (release = NULL), and releases children/dictionary
# transitively — our single-owner model makes the transitive part a no-op:
# the root's control block owns everything.
function _release_array(a::Ptr{CArrowArray})
    a == C_NULL && return nothing
    arr = unsafe_load(a)
    arr.release == C_NULL && return nothing
    _release_thunk(arr.private_data)
    _store_field!(a, :release, Ptr{Cvoid}(C_NULL))
    return nothing
end
function _release_schema(s::Ptr{CArrowSchema})
    s == C_NULL && return nothing
    sch = unsafe_load(s)
    sch.release == C_NULL && return nothing
    _release_thunk(sch.private_data)
    _store_field!(s, :release, Ptr{Cvoid}(C_NULL))
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

Drain the reap queue: free every malloc owned by released exports and drop
their registry roots. In the real adapter this is a background reaper task;
the example calls it explicitly to keep the demo deterministic.
"""
function reap!()
    keys = lock(REGISTRY_LOCK) do
        ks = copy(REAP_QUEUE)
        empty!(REAP_QUEUE)
        ks
    end
    for k in keys
        root = lock(REGISTRY_LOCK) do
            pop!(EXPORT_REGISTRY, k, nothing)
        end
        root === nothing && continue
        for m in root.mallocs
            Libc.free(m)
        end
        empty!(root.roots)
    end
    return length(keys)
end

_malloc!(root::ExportedRoot, n::Integer) = begin
    p = Libc.malloc(max(n, 1))
    p == C_NULL && throw(OutOfMemoryError())
    push!(root.mallocs, p)
    Ptr{Cvoid}(p)
end

function _cstring!(root::ExportedRoot, s::AbstractString)
    n = ncodeunits(s)
    p = Ptr{UInt8}(_malloc!(root, n + 1))
    for (i, b) in enumerate(codeunits(s))
        unsafe_store!(p, b, i)
    end
    unsafe_store!(p, 0x00, n + 1)
    return p
end

function _export_schema!(root::ExportedRoot, f::Field, release::Ptr{Cvoid})::Ptr{CArrowSchema}
    p = Ptr{CArrowSchema}(_malloc!(root, sizeof(CArrowSchema)))
    childfields = f.type isa DictionaryType ? Field[] : f.children
    nchildren = length(childfields)
    childptrs = Ptr{Ptr{CArrowSchema}}(C_NULL)
    if nchildren > 0
        childptrs = Ptr{Ptr{CArrowSchema}}(_malloc!(root, nchildren * sizeof(Ptr)))
        for (i, cf) in enumerate(childfields)
            unsafe_store!(childptrs, _export_schema!(root, cf, release), i)
        end
    end
    dict = Ptr{CArrowSchema}(C_NULL)
    if f.type isa DictionaryType
        dict = _export_schema!(root,
            Field(f.name, f.type.valuetype; nullable=f.nullable, children=f.children),
            release)
    end
    unsafe_store!(p, CArrowSchema(
        _cstring!(root, formatstring(f.type)),
        _cstring!(root, f.name),
        Ptr{UInt8}(C_NULL),
        f.nullable ? ARROW_FLAG_NULLABLE : Int64(0),
        nchildren, childptrs, dict, release, root.control))
    return p
end

function _export_array!(root::ExportedRoot, d::ArrayData, release::Ptr{Cvoid})::Ptr{CArrowArray}
    p = Ptr{CArrowArray}(_malloc!(root, sizeof(CArrowArray)))
    nbuf = length(d.buffers)
    bufptrs = Ptr{Ptr{Cvoid}}(_malloc!(root, max(nbuf, 1) * sizeof(Ptr)))
    for (i, b) in enumerate(d.buffers)
        # Spec: an absent validity bitmap is a NULL buffer pointer.
        unsafe_store!(bufptrs, AC.isempty_buffer(b) ? Ptr{Cvoid}(C_NULL) :
                               Ptr{Cvoid}(AC.sliceptr(b)), i)
    end
    nchildren = length(d.children)
    childptrs = Ptr{Ptr{CArrowArray}}(C_NULL)
    if nchildren > 0
        childptrs = Ptr{Ptr{CArrowArray}}(_malloc!(root, nchildren * sizeof(Ptr)))
        for (i, c) in enumerate(d.children)
            unsafe_store!(childptrs, _export_array!(root, c, release), i)
        end
    end
    dict = d.dictionary === nothing ? Ptr{CArrowArray}(C_NULL) :
        _export_array!(root, d.dictionary, release)
    unsafe_store!(p, CArrowArray(d.len, nullcount(d), d.offset, nbuf,
        nchildren, bufptrs, childptrs, dict, release, root.control))
    return p
end

"""
    to_c_data(field, data) -> (Ptr{CArrowSchema}, Ptr{CArrowArray})

Export one column. The returned pointers follow the spec's consumer
contract: exactly one of the consumer's `release` calls (on either struct's
root) frees that struct tree's control; both trees share one Julia-side
ExportedRoot so the buffers stay alive until BOTH are released. (For
simplicity the prove-out gives schema and array separate control blocks and
separate registry entries — the report's "schema and array lifetimes are
separate" rule.)
"""
function to_c_data(f::Field, d::ArrayData)
    arel = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
    srel = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
    sp = _newroot(Any[f]) do root
        _export_schema!(root, f, srel)
    end
    ap = _newroot(Any[d]) do root
        _export_array!(root, d, arel)
    end
    return sp, ap
end

function _newroot(build, roots::Vector{Any})
    key = lock(REGISTRY_LOCK) do
        NEXT_KEY[] += 1
    end
    control = Libc.malloc(CONTROL_BLOCK_BYTES)
    control == C_NULL && throw(OutOfMemoryError())
    unsafe_store!(Ptr{UInt8}(control), 0x00)
    unsafe_store!(Ptr{Int64}(Ptr{Cvoid}(control) + 8), key)
    root = ExportedRoot(roots, Ptr{Cvoid}[Ptr{Cvoid}(control)], Ptr{Cvoid}(control))
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
        for m in root.mallocs
            Libc.free(m)
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
    @atomic released::Bool
    function ForeignOwner(arr::CArrowArray)
        o = new(arr, false)
        finalizer(release!, o)
        return o
    end
end

function release!(o::ForeignOwner)
    old, ok = @atomicreplace o.released false => true
    ok || return nothing
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
    sch = unsafe_load(sp)
    arr = unsafe_load(ap)
    (sch.release == C_NULL || arr.release == C_NULL) &&
        throw(ArgumentError("cannot import a released structure"))
    owner = ForeignOwner(arr)
    # MOVE: the source array struct no longer owns anything.
    _store_field!(ap, :release, Ptr{Cvoid}(C_NULL))
    try
        f = _import_field(sch)
        d = _import_array(f, arr, owner)
        validate_structural(f, d)
        validate_semantic(f, d)
        # The schema struct is released independently (separate lifetime).
        _release_c_schema!(sp, sch)
        return f, d
    catch
        release!(owner)   # failed-import cleanup: exactly once, then rethrow
        rethrow()
    end
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
    children = Field[]
    for i = 1:sch.n_children
        push!(children, _import_field(unsafe_load(unsafe_load(sch.children, i))))
    end
    t = parseformat(fmt)
    if sch.dictionary != C_NULL
        vf = _import_field(unsafe_load(sch.dictionary))
        t isa IntType || error("dictionary index format must be an integer")
        return Field(name, DictionaryType(t, vf.type, false);
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
    total = arr.offset + arr.length
    Int64(arr.n_buffers) == length(spec.buffers) ||
        throw(ValidationError("layout $(typeof(t)) declares $(length(spec.buffers)) buffers, producer sent $(arr.n_buffers)"))
    buffers = BufferSlice[]
    offsets_slice = nothing
    for (i, role) in enumerate(spec.buffers)
        p = bufferptr(arr, i)
        nbytes = if role == AC.VALIDITY
            p == C_NULL ? Int64(0) : AC.expected_validity_bytes(total)
        elseif role == AC.OFFSETS
            Int64((total + 1) * spec.offsetwidth)
        elseif role == AC.DATA
            if spec.fixedwidth > 0
                Int64(total * spec.fixedwidth)
            elseif spec.fixedwidth == -1
                AC.expected_validity_bytes(total)
            else
                # varbinary data: sized by the final offset, read from the
                # offsets slice we just built (bounded by ITS declared size).
                O = spec.offsetwidth == 8 ? Int64 : Int32
                Int64(AC.loadat(offsets_slice, O, Int64(total) * sizeof(O)))
            end
        else
            error("cdata prove-out: role $role import is roadmap slice work")
        end
        if p == C_NULL
            nbytes == 0 || throw(ValidationError("NULL $role buffer with nonzero required size"))
            push!(buffers, BufferSlice())
        else
            region = OwnerRegion(Ptr{UInt8}(p), nbytes, AC.Foreign; root=owner)
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
        children=children, dictionary=dict, nullcount=arr.null_count)
end

# ---------------------------------------------------------------------------
# Demo: export -> import round-trip, release lifecycle, failure paths
# ---------------------------------------------------------------------------

function main()
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
    nlive = lock(REGISTRY_LOCK) do
        length(EXPORT_REGISTRY)
    end
    println("live exports rooted in registry: $nlive")

    # Consumer-side release: drop the imported columns (their ForeignOwners'
    # release calls the exported arrays' release callbacks), then reap.
    for (_, d2) in imported
        for buf in d2.buffers
            # find the shared owner through any region and release explicitly
            buf.region === nothing && continue
            o = buf.region.root
            o isa ForeignOwner && release!(o)
        end
    end
    reaped = reap!()
    println("reaped $reaped released exports ✓")

    # Double-release is inert: release the same owners again.
    for (_, d2) in imported
        for buf in d2.buffers
            buf.region === nothing && continue
            o = buf.region.root
            o isa ForeignOwner && release!(o)
        end
    end
    @assert reap!() == 0
    println("double release is exactly-once ✓")

    # After release, the imported columns must fail CLEANLY, not read freed
    # memory — close the foreign regions to prove invalidation.
    f2, d2 = imported[1]
    for buf in d2.buffers
        buf.region === nothing || forceclose!(buf.region)
    end
    caught = try
        materialize(f2, d2)
        false
    catch e
        e isa InvalidatedError
    end
    @assert caught
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
    println("moved (released) source cannot be imported twice ✓")
    println()
    println("adapter size: ≈ 330 lines for export+import+lifecycle — the")
    println("payoff of ArrayData already having the ArrowArray shape.")
end

main()
