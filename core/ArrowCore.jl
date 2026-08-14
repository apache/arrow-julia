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

"""
    ArrowCore

Prove-out of the runtime-tagged, C-data-shaped core proposed in the Arrow.jl
redesign report (Arrow-redesign-report.md, §9). Standalone: depends only on
Base and the Mmap standard library. The existing package is untouched;
`core/examples/` shows how the IPC and C-data adapters sit on top of this
module.

Design rules this module is built to demonstrate:

1. One physical data model. `ArrayData` = layout + buffers + children +
   dictionary, mirroring the Arrow C data interface's `ArrowArray`. Logical
   type parameters such as timezone and precision/scale are fields on
   `ArrowType` descriptors. Names and nullability are fields on `Field`.
   None parameterize the Core storage types. Struct materialization always
   returns `Vector{Pair{String,Any}}`; a typed facade remains separate work.

2. Memory validity is GC reachability. Every buffer is a `BufferSlice`
   into an `OwnerRegion` — an immutable (pointer, length, alignment, root)
   record whose `root` anchors the backing storage. Slices are bounds-checked
   against the region at construction. For verified owned and IPC extents,
   corrupt spans therefore fail before access; foreign extents remain trusted
   declarations, and mapped files remain exposed to external changes. Loads
   are a final bounds check plus a raw load, with no per-access synchronization.
   Deterministic eager release is deliberately constrained out of this core
   (see §1). Mmap stdlib storage is unmapped later by its GC finalizer.

3. One structural layout registry. `layoutspec(type)` returns the buffer
   roles / child arity / offset width for each of the format-1.5 layouts.
   Generic code (buffer walking, structural validation, the IPC adapter's
   node/buffer accounting in core/examples/ipc_read.jl) is driven by the
   registry; per-layout SEMANTICS (element access, semantic validation) are
   ordinary methods grouped per layout below. Adding a layout means one
   registry entry plus bounded method groups in the layers that support it.

4. Validation is staged (report §9): structural checks here are O(buffers)
   and run at construction/adaptation time. Data-intrinsic semantic checks
   are O(n) when an adapter or caller requests them; a successful result is
   cached. Benign concurrent callers may repeat the same scan.
   Field-dependent contracts, including nullability, run on every validation
   call. Full checks (UTF-8) are opt-in.
   Framing-stage checks (checked spans, metadata verification, and resource
   limits before metadata-directed allocation) belong to the adapters and
   are exercised in the IPC example.

Interruption contract: asynchronous interruption (SIGINT /
`InterruptException`, task cancellation) is explicitly OUT of this module's
guarantees, matching ecosystem-wide practice — Base itself does not make
arbitrary code async-exception-atomic, and pretending otherwise costs
pervasive `disable_sigint` scaffolding for a property that still cannot be
fully delivered. Ordinary exception safety (error paths clean up, release
is exactly-once) IS in contract. When Julia 1.14's structured cancellation
lands, a formal revisit is planned on top of whatever Base then provides.

Deliberately out of scope for the prove-out (tracked in the report roadmap):
view layouts (Utf8View/BinaryView/ListView) and run-end encoding have
registry entries and structural validation but no semantic validation or
element accessors; semantic/full validation rejects them rather than marking
unchecked content valid. Core has no codec dependency; the IPC adapter
implements compression. There is no Tables.jl integration or `ViewPlan` — bulk access
here uses a plain function barrier (`materialize`) to demonstrate the
pattern the facade will formalize.
"""
module ArrowCore

using Base: Checked
import Mmap
const checked_add = Checked.checked_add
const checked_sub = Checked.checked_sub
const checked_mul = Checked.checked_mul

export OwnerRegion, BufferSlice, heapregion, mmapregion,
    ReleaseCounter, increment!,
    ArrowType, NullType, BoolType, IntType, FloatType, DecimalType,
    FixedSizeBinaryType, BinaryType, Utf8Type, DateType, TimeType,
    TimestampType, DurationType, IntervalType, ListType, FixedSizeListType,
    StructType, MapType, UnionType, DictionaryType, ViewType, ListViewType,
    RunEndEncodedType,
    Field, Schema, ArrayData, RecordBatch, RecordBatchSource, nextbatch!,
    LayoutSpec, layoutspec, BufferRole,
    validate_structural, validate_semantic, validate_full, ValidationError,
    nullcount, getvalue, materialize,
    fromjulia, batch

# ---------------------------------------------------------------------------
# §1 Memory: regions as GC anchors (constrained model)
# ---------------------------------------------------------------------------
#
# DESIGN DECISION (maintainer review, 2026-08-13): buffer validity is
# GC REACHABILITY — Julia's native memory-safety contract — and nothing else.
# A region is an immutable (pointer, length, alignment, root) record: the
# `root` is whatever keeps the memory alive (the wrapped Julia array, the
# Mmap-stdlib array whose own finalizer unmaps at collection, a C-data
# adapter's owner object whose finalizer calls the producer's release). Views
# hold their region; the region holds its root; therefore memory a view can
# reach is memory that is valid.
#
# The earlier prove-out iterations carried a full lifecycle state machine
# (guards, phases, deterministic forceclose!, release actions, per-kind
# machinery). Review concluded it was a ton of complexity for unproven
# use-cases: the guard/invalidate system existed to make OUR OWN optional
# eager-release feature safe, while the failures that actually occur in the
# wild (a mapped file truncated or rewritten externally) were never
# preventable by any in-process state machine. Constraining eager release
# out of scope deletes the machinery wholesale and makes every buffer load
# a bounds check plus a raw load — no per-access synchronization.
#
# What this deliberately gives up, so the constraint is informed:
#   * Eager, deterministic unmap (e.g. delete-a-mapped-file-now on Windows):
#     unmapping happens when the GC collects the mapping. Revisit if real
#     demand appears, likely as an opt-in layer once upstream offers a
#     public API.
#   * A guard/invalidate error for use-after-release: with no eager release
#     in Core there is nothing to use-after. The C-data adapter's explicit
#     `release!` is caller-contract (post-release access is undefined) —
#     which is the C data interface spec's own rule for released structures.
#   * External-truncation protection: never existed anywhere; a shared
#     mapping's pages can vanish under any implementation. Same exposure as
#     every mmap-based reader.

"An atomic counter (observation/exactly-once bookkeeping for adapters and tests)."
mutable struct ReleaseCounter
    @atomic n::Int
end
ReleaseCounter() = ReleaseCounter(0)
Base.getindex(c::ReleaseCounter) = @atomic c.n
# CAS loop rather than `@atomic c.n += 1`: the atomic read-modify-write
# builtin is not yet implemented in JuliaC's trim verifier, while
# compare-and-swap is; contention here is negligible.
function increment!(c::ReleaseCounter)
    while true
        old = @atomic c.n
        _, ok = @atomicreplace c.n old => old + 1
        ok && return old + 1
    end
end

"""
    OwnerRegion

One contiguous memory region and the object that keeps it alive. Immutable:
there is no lifecycle to manage — the region is valid exactly as long as it
is reachable, because `root` anchors the backing storage (a borrowed Julia
array, the Mmap-stdlib array, or an adapter's owner object). Slices
reject geometry outside the declared `len` at construction. For adapters that
verify the backing extent, corrupt spans therefore fail before access. Loads
retain a final bounds check before the raw read.

The scoped-borrow contract for wrapped Julia arrays: the caller must not
mutate or resize the array while the region or any cached validation result
remains in use. Mutation can invalidate a semantic certificate; resizing can
reallocate the storage and invalidate its pointer.
"""
struct OwnerRegion
    ptr::Ptr{UInt8}
    len::Int64
    alignment::Int      # guaranteed ptr alignment, capped at 64; loads consult it
    root::Any           # GC anchor; never dispatched on, only stored

    function OwnerRegion(ptr::Ptr{UInt8}, len::Integer; root=nothing)
        len >= 0 || throw(ArgumentError("region length must be non-negative"))
        n = Int64(len)
        (ptr != C_NULL || n == 0) ||
            throw(ArgumentError("a non-empty region requires a non-NULL pointer"))
        (root !== nothing || n == 0) ||
            throw(ArgumentError("a non-empty region requires a GC root"))
        # BufferSlice bounds are only meaningful if every declared byte also
        # has a representable pointer address. Reject a foreign extent whose
        # final byte would wrap native pointer arithmetic.
        if n > 0
            lastaddr = UInt128(UInt(ptr)) + UInt128(n - 1)
            lastaddr <= UInt128(typemax(UInt)) ||
                throw(ArgumentError("region extent wraps the native address space"))
        end
        align = ptr == C_NULL ? 64 : (1 << trailing_zeros(UInt(ptr) | UInt(64)))
        return new(ptr, n, align, root)
    end
end

"""
    heapregion(v::Vector{T}) -> OwnerRegion

Borrow a Julia array as a region (zero-copy). The array is the `root`.
"""
function heapregion(v::Vector{T}) where {T}
    isbitstype(T) || throw(ArgumentError("heapregion requires an isbits element type"))
    return OwnerRegion(Ptr{UInt8}(pointer(v)), sizeof(v); root=v)
end

"""
    mmapregion(path) -> OwnerRegion

Map a file read-only via the Mmap STDLIB (cross-platform) and wrap the
mapped array as the region's `root`. The stdlib's own machinery unmaps when
the array is collected — validity is reachability, like every other region.

The caller must prevent external writes or truncation of the mapped file
while the region or any cached validation result remains in use: a shared
mapping cannot keep a semantic certificate valid when another process
changes its bytes, and truncation can make an in-range load fault.
"""
function mmapregion(path::AbstractString)
    io = open(path, "r")
    arr = try
        Mmap.mmap(io, Vector{UInt8})
    finally
        # The mapping outlives the descriptor.
        close(io)
    end
    isempty(arr) && throw(ArgumentError("cannot map empty file: $path"))
    return OwnerRegion(Ptr{UInt8}(pointer(arr)), length(arr); root=arr)
end

# --- BufferSlice ------------------------------------------------------------

"""
    BufferSlice

A bounds-checked window into an `OwnerRegion` — the only currency for Arrow
buffer data in this module. Constructing a slice validates
`offset + len <= region.len` with checked arithmetic, so downstream code can
assume every slice is in-bounds and concentrate on layout logic.

An all-default `BufferSlice()` is the canonical empty buffer (used for
absent validity bitmaps, empty data buffers).
"""
struct BufferSlice
    region::Union{Nothing,OwnerRegion}
    offset::Int64
    len::Int64
    function BufferSlice(region::OwnerRegion, offset::Integer, len::Integer)
        offset >= 0 || throw(ArgumentError("negative buffer offset"))
        len >= 0 || throw(ArgumentError("negative buffer length"))
        checked_add(Int64(offset), Int64(len)) <= region.len ||
            throw(ArgumentError("buffer [offset=$offset len=$len] exceeds region of $(region.len) bytes"))
        return new(region, Int64(offset), Int64(len))
    end
    BufferSlice() = new(nothing, 0, 0)
end

Base.length(b::BufferSlice) = b.len
isempty_buffer(b::BufferSlice) = b.len == 0
sliceptr(b::BufferSlice) = b.region === nothing ? Ptr{UInt8}(0) : b.region.ptr + b.offset

"Sub-slice with checked arithmetic (relative bounds against the parent slice)."
function subslice(b::BufferSlice, offset::Integer, len::Integer)
    offset >= 0 || throw(ArgumentError("negative subslice offset"))
    len >= 0 || throw(ArgumentError("negative subslice length"))
    b.region === nothing && (len == 0 && offset == 0) && return b
    b.region === nothing && throw(ArgumentError("cannot subslice the empty buffer"))
    checked_add(Int64(offset), Int64(len)) <= b.len ||
        throw(ArgumentError("subslice [offset=$offset len=$len] exceeds slice of $(b.len) bytes"))
    return BufferSlice(b.region, checked_add(b.offset, Int64(offset)), Int64(len))
end

"""
Load a `T` at byte offset `byteoff` (0-based) within the slice. Handles the
misaligned case with a byte-wise load: alignment is a property of the region
(the report: Arrow controls only its own allocations; mmap and foreign
pointers can be anything), so the branch lives here, in one place, instead
of as a copy workaround scattered through per-type code.
"""
@inline function loadat(b::BufferSlice, ::Type{T}, byteoff::Int64) where {T}
    # Raw Arrow bytes may only materialize pointer-free values. Loading a
    # struct with managed references would treat attacker-controlled bytes as
    # GC pointers and can crash Julia before it can report an ordinary error.
    isbitstype(T) || throw(ArgumentError("loadat requires an isbits type, got $T"))
    # Bounds: byteoff + sizeof(T) <= len. byteoff is computed by callers from
    # validated element indices, but re-check cheaply: this is the last line
    # of defense before a raw pointer dereference.
    width = Int64(sizeof(T))
    # Express this as subtraction, not `byteoff + width <= len`: a hostile
    # byte offset near typemax(Int64) must not wrap through the last bounds
    # check and reach pointer arithmetic.
    (byteoff >= 0 && width <= b.len && byteoff <= b.len - width) ||
        throw(BoundsError(b, byteoff))
    b.region === nothing && throw(ArgumentError("empty buffer has no data"))
    # Raw pointers do not keep Julia owners alive. Preserve the slice through
    # the full dereference so its region and opaque root remain reachable.
    GC.@preserve b begin
        p = sliceptr(b) + byteoff
        required = datatype_alignment(T)
        relative = checked_add(b.offset, byteoff)
        region = b.region::OwnerRegion
        if region.alignment >= required && relative % required == 0
            return unsafe_load(Ptr{T}(p))
        else
            return _load_unaligned(T, p)
        end
    end
end

datatype_alignment(::Type{T}) where {T} = Base.datatype_alignment(T)

@inline function _load_unaligned(::Type{T}, p::Ptr{UInt8}) where {T}
    bytes = ntuple(i -> unsafe_load(p + (i - 1)), Val(sizeof(T)))
    return reinterpret_bytes(T, bytes)
end
@inline reinterpret_bytes(::Type{T}, bytes::NTuple{N,UInt8}) where {T,N} =
    reinterpret(T, bytes)

"Copy the slice into a fresh `Vector{UInt8}` (used by materialize/tests)."
function slicebytes(b::BufferSlice)
    b.len == 0 && return UInt8[]
    b.region === nothing && throw(ArgumentError("empty buffer has no data"))
    out = Vector{UInt8}(undef, b.len)
    # Preserve both owners across the raw copy. Neither pointer roots its
    # source or destination allocation.
    GC.@preserve b out begin
        unsafe_copyto!(pointer(out), sliceptr(b), b.len)
    end
    return out
end

"Read one bit (0-based bit index) from a validity/values bitmap slice."
@inline function getbit(b::BufferSlice, i::Int64)
    byte = loadat(b, UInt8, i >> 3)
    return (byte >> (i & 7)) & 0x01 == 0x01
end

# ---------------------------------------------------------------------------
# §2 Type system: runtime descriptors, Field, Schema
# ---------------------------------------------------------------------------

"""
    ArrowType

Abstract supertype of the runtime logical-type descriptors. These are small
immutable structs whose *fields* carry what today's Arrow.jl puts in Julia
type parameters (`Timestamp{U,TZ}`, `Decimal{P,S,T}`, ...). Two timestamp
columns with different timezones have the SAME Julia type here — schema
diversity costs data, not method instances (fixes the #503 class by
construction).
"""
abstract type ArrowType end

"""
Read-only, defensively-copied vector storage for the frozen data model.
Its type does not encode the length, so schema width and nesting depth do
not create a new family of container types. The backing field is internal;
normal mutation APIs such as `setindex!` and `push!` are unavailable.
"""
struct FrozenVector{T} <: AbstractVector{T}
    _data::Vector{T}
    FrozenVector{T}(data::Vector{T}, ::Nothing) where {T} = new{T}(data)
end
FrozenVector{T}(xs::FrozenVector{T}) where {T} = xs
FrozenVector{T}(xs) where {T} = FrozenVector{T}(collect(T, xs), nothing)
Base.size(v::FrozenVector) = size(getfield(v, :_data))
Base.length(v::FrozenVector) = length(getfield(v, :_data))
Base.getindex(v::FrozenVector, i::Int) = getfield(v, :_data)[i]
Base.IndexStyle(::Type{<:FrozenVector}) = IndexLinear()

@enum TimeUnit::UInt8 SECOND MILLISECOND MICROSECOND NANOSECOND
@enum DateUnit::UInt8 DAY MILLISECOND_DATE
@enum IntervalUnit::UInt8 YEAR_MONTH DAY_TIME MONTH_DAY_NANO
@enum UnionMode::UInt8 SparseMode DenseMode
@enum Endianness::UInt8 LittleEndian BigEndian
_native_endianness() = Base.ENDIAN_BOM == 0x04030201 ? LittleEndian : BigEndian

struct NullType <: ArrowType end
struct BoolType <: ArrowType end
struct IntType <: ArrowType
    bits::Int      # 8/16/32/64 — the spec's Int; wider is NOT valid (issue #319)
    signed::Bool
end
struct FloatType <: ArrowType
    bits::Int      # 16/32/64
end
struct DecimalType <: ArrowType
    precision::Int
    scale::Int
    bits::Int      # 32/64/128/256 (format 1.5)
end
struct FixedSizeBinaryType <: ArrowType
    nbytes::Int
end
struct BinaryType <: ArrowType
    large::Bool    # Int64 offsets when true
end
struct Utf8Type <: ArrowType
    large::Bool
end
struct DateType <: ArrowType
    unit::DateUnit # DAY => Int32 storage, MILLISECOND => Int64
end
struct TimeType <: ArrowType
    unit::TimeUnit
    bits::Int      # 32 (s/ms) or 64 (us/ns)
end
struct TimestampType <: ArrowType
    unit::TimeUnit
    timezone::Union{Nothing,String}   # a VALUE — one method instance total
end
struct DurationType <: ArrowType
    unit::TimeUnit
end
struct IntervalType <: ArrowType
    unit::IntervalUnit                # includes MONTH_DAY_NANO (format 1.2)
end
struct ListType <: ArrowType
    large::Bool
end
struct FixedSizeListType <: ArrowType
    listsize::Int
end
struct StructType <: ArrowType end
struct MapType <: ArrowType
    keyssorted::Bool
end
struct UnionType <: ArrowType
    mode::UnionMode
    typeids::FrozenVector{Int8}       # declared type-id domain, child order
end
UnionType(mode::UnionMode, typeids) = UnionType(mode, FrozenVector{Int8}(typeids))
"Dictionary-encoded: `indextype` is the physical index; values live in `ArrayData.dictionary`."
struct DictionaryType <: ArrowType
    indextype::IntType
    valuetype::ArrowType
    ordered::Bool
end
"Utf8View / BinaryView (format 1.4). Registry + structural validation only in the prove-out."
struct ViewType <: ArrowType
    utf8::Bool
end
"ListView / LargeListView (format 1.4). Registry + structural validation only in the prove-out."
struct ListViewType <: ArrowType
    large::Bool
end
"Run-end encoded (format 1.3). Registry + structural validation only in the prove-out."
struct RunEndEncodedType <: ArrowType end

"""
    Field

One column/child descriptor: name, logical type, nullability, metadata, and
child fields. Dictionary columns are `DictionaryType` here; the IPC-level
dictionary *id* is deliberately NOT a Field concern — it is IPC bookkeeping
and lives in the adapter (report §9: Core dictionaries are object
references; the id↔dictionary table is the adapter's).
"""
struct Field
    name::String
    type::ArrowType
    nullable::Bool
    metadata::Union{Nothing,FrozenVector{Pair{String,String}}}
    children::FrozenVector{Field}
end
_freezemetadata(::Nothing) = nothing
_freezemetadata(metadata::FrozenVector{Pair{String,String}}) = metadata
function _freezemetadata(metadata::Union{AbstractVector,Tuple})
    all(kv -> kv isa Pair, metadata) ||
        throw(ArgumentError("metadata sequences must contain Pair values"))
    return FrozenVector{Pair{String,String}}(
        String(first(kv)) => String(last(kv)) for kv in metadata)
end
_freezemetadata(metadata) =
    FrozenVector{Pair{String,String}}(
        String(k) => String(v) for (k, v) in pairs(metadata))
Field(name, type; nullable=true, metadata=nothing, children=()) =
    Field(String(name), type, Bool(nullable), _freezemetadata(metadata),
        FrozenVector{Field}(children))
Field(name, type, nullable, metadata, children) =
    Field(name, type; nullable=nullable, metadata=metadata, children=children)

struct Schema
    fields::FrozenVector{Field}
    metadata::Union{Nothing,FrozenVector{Pair{String,String}}}
    endianness::Endianness
end
Schema(fields; metadata=nothing, endianness=_native_endianness()) =
    Schema(FrozenVector{Field}(fields), _freezemetadata(metadata), endianness)

# ---------------------------------------------------------------------------
# §3 Layout registry (structural facts only)
# ---------------------------------------------------------------------------

# OFFSETS are RANGE offsets (len+1 entries bounding variable-size slots);
# ELEMENT_OFFSETS are per-element child positions (len entries — dense union).
# The distinction is structural, so it lives in the registry, not in
# per-layout special cases inside the validator.
@enum BufferRole::UInt8 VALIDITY DATA OFFSETS ELEMENT_OFFSETS SIZES VIEWS TYPE_IDS

"""
    LayoutSpec

The STRUCTURAL facts for one physical layout: which buffers it has (in
order), how many children, its offset width, whether the trailing data
buffers are variadic (view layouts). This is everything generic code needs
to walk a layout — and nothing more. Semantics (what the bytes mean, how to
access element `i`) are per-layout methods, not registry rows (report §8.4:
"a registry row + one file", not "a row does everything").

`childcount == -1` means "declared by Field.children" (struct/union);
`fixedwidth` is bytes-per-element for fixed-stride DATA buffers, 0 when the
data buffer is byte-addressed (varbinary) or absent, and -1 for bit-packed.
"""
struct LayoutSpec
    buffers::FrozenVector{BufferRole}
    childcount::Int
    offsetwidth::Int    # 0, 4, or 8 — width of the OFFSETS buffer entries
    fixedwidth::Int
    variadic::Bool
end
LayoutSpec(buffers, childcount, offsetwidth, fixedwidth, variadic) =
    LayoutSpec(FrozenVector{BufferRole}(buffers), childcount, offsetwidth,
        fixedwidth, variadic)

primwidth(t::IntType) = t.bits ÷ 8
primwidth(t::FloatType) = t.bits ÷ 8
primwidth(t::DecimalType) = t.bits ÷ 8
primwidth(t::DateType) = t.unit == DAY ? 4 : 8
primwidth(t::TimeType) = t.bits ÷ 8
primwidth(::TimestampType) = 8
primwidth(::DurationType) = 8
primwidth(t::IntervalType) =
    t.unit == YEAR_MONTH ? 4 : t.unit == DAY_TIME ? 8 : 16
primwidth(t::FixedSizeBinaryType) = t.nbytes

const VALIDITY_DATA = FrozenVector{BufferRole}((VALIDITY, DATA))

layoutspec(::NullType) = LayoutSpec(BufferRole[], 0, 0, 0, false)
layoutspec(::BoolType) = LayoutSpec(VALIDITY_DATA, 0, 0, -1, false)
layoutspec(t::IntType) = LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t), false)
layoutspec(t::FloatType) = LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t), false)
layoutspec(t::DecimalType) = LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t), false)
layoutspec(t::DateType) = LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t), false)
layoutspec(t::TimeType) = LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t), false)
layoutspec(t::TimestampType) = LayoutSpec(VALIDITY_DATA, 0, 0, 8, false)
layoutspec(t::DurationType) = LayoutSpec(VALIDITY_DATA, 0, 0, 8, false)
layoutspec(t::IntervalType) = LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t), false)
layoutspec(t::FixedSizeBinaryType) = LayoutSpec(VALIDITY_DATA, 0, 0, t.nbytes, false)
layoutspec(t::BinaryType) =
    LayoutSpec([VALIDITY, OFFSETS, DATA], 0, t.large ? 8 : 4, 0, false)
layoutspec(t::Utf8Type) =
    LayoutSpec([VALIDITY, OFFSETS, DATA], 0, t.large ? 8 : 4, 0, false)
layoutspec(t::ListType) =
    LayoutSpec([VALIDITY, OFFSETS], 1, t.large ? 8 : 4, 0, false)
layoutspec(::FixedSizeListType) = LayoutSpec([VALIDITY], 1, 0, 0, false)
layoutspec(::StructType) = LayoutSpec([VALIDITY], -1, 0, 0, false)
layoutspec(::MapType) = LayoutSpec([VALIDITY, OFFSETS], 1, 4, 0, false)
layoutspec(t::UnionType) = t.mode == SparseMode ?
    LayoutSpec([TYPE_IDS], -1, 0, 0, false) :
    LayoutSpec([TYPE_IDS, ELEMENT_OFFSETS], -1, 4, 0, false)
layoutspec(t::DictionaryType) =
    LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t.indextype), false)
layoutspec(::ViewType) = LayoutSpec([VALIDITY, VIEWS], 0, 0, 16, true)
layoutspec(t::ListViewType) =
    # ListView has one offset and one size per parent slot. These are not
    # the length+1 monotone range offsets used by List/Utf8/Binary.
    LayoutSpec([VALIDITY, ELEMENT_OFFSETS, SIZES], 1, t.large ? 8 : 4, 0, false)
# REE: no top-level validity; run_ends and values are CHILDREN, not buffers.
layoutspec(::RunEndEncodedType) = LayoutSpec(BufferRole[], 2, 0, 0, false)

"""
    layoutspec_of(t::ArrowType) -> LayoutSpec

The closed-set dispatch ladder over the runtime descriptors. This is the
trim-compile story for a runtime-tagged core (report §8.9, §14.2): dispatch
on an abstract-typed field is dynamic, which JuliaC `--trim=safe` rejects —
but the descriptor set is CLOSED (it is the layout registry), so one
`isa` ladder devirtualizes every generic call site statically. Multiple
dispatch remains the extension surface (each branch calls the ordinary
`layoutspec` method); the ladder is only the entry point generic code uses
when the descriptor's concrete type is unknown. Branches are ordered by
expected frequency.
"""
@inline function layoutspec_of(t::ArrowType)::LayoutSpec
    t isa IntType && return layoutspec(t)
    t isa FloatType && return layoutspec(t)
    t isa Utf8Type && return layoutspec(t)
    t isa BoolType && return layoutspec(t)
    t isa ListType && return layoutspec(t)
    t isa StructType && return layoutspec(t)
    t isa DictionaryType && return layoutspec(t)
    t isa TimestampType && return layoutspec(t)
    t isa DateType && return layoutspec(t)
    t isa TimeType && return layoutspec(t)
    t isa DurationType && return layoutspec(t)
    t isa BinaryType && return layoutspec(t)
    t isa FixedSizeBinaryType && return layoutspec(t)
    t isa FixedSizeListType && return layoutspec(t)
    t isa MapType && return layoutspec(t)
    t isa UnionType && return layoutspec(t)
    t isa DecimalType && return layoutspec(t)
    t isa IntervalType && return layoutspec(t)
    t isa NullType && return layoutspec(t)
    t isa ViewType && return layoutspec(t)
    t isa ListViewType && return layoutspec(t)
    t isa RunEndEncodedType && return layoutspec(t)
    throw(ArgumentError("unregistered ArrowType"))
end

# A plain-dispatch collapse of these ladders was tried (Aug 2026) and
# rejected by evidence: JuliaC's `--trim=safe` verifier reports the abstract
# call site (`layoutspec(d.type::ArrowType)`) as an unresolved call — it
# does not enumerate the closed method table, so the ladders remain the
# devirtualization mechanism. The throwing `::Any` fallback below is the
# piece of that simplification worth keeping: junk descriptors get a clean
# error instead of a `MethodError` wherever the raw method table is called.
layoutspec(::Any) = throw(ArgumentError("unregistered ArrowType"))

# ---------------------------------------------------------------------------
# §4 ArrayData
# ---------------------------------------------------------------------------

"""
    ArrayData

The one physical array representation (≅ C `ArrowArray`): buffers + children
+ optional dictionary, plus logical length and a lazily-computed, cached
null count. Mutable only for the two caches (`nullcount`, `semachecked`);
everything user-visible is immutable after construction.

`offset` (element offset into the buffers) is carried for C-data import
compatibility; the accessors below apply it uniformly.
"""
mutable struct ArrayData
    const type::ArrowType
    const len::Int64
    const offset::Int64
    const buffers::FrozenVector{BufferSlice}
    const children::FrozenVector{ArrayData}
    const dictionary::Union{Nothing,ArrayData}
    const owner::Any                  # adapter lifetime anchor, if needed
    @atomic nullcount::Int64      # -1 = unknown, computed on demand
    @atomic semachecked::Bool     # data-intrinsic semantic checks passed
end

function ArrayData(type::ArrowType, len::Integer, buffers;
    offset::Integer=0, children=(),
    dictionary::Union{Nothing,ArrayData}=nothing, owner=nothing,
    nullcount::Integer=-1)
    len >= 0 || throw(ArgumentError("negative array length"))
    offset >= 0 || throw(ArgumentError("negative array offset"))
    -1 <= nullcount <= len ||
        throw(ArgumentError("null count must be -1 or in [0, length]"))
    return ArrayData(type, Int64(len), Int64(offset),
        FrozenVector{BufferSlice}(buffers), FrozenVector{ArrayData}(children),
        dictionary, owner, Int64(nullcount), false)
end

Base.length(d::ArrayData) = d.len

# Adapter-private certificate set. An entry means that one immutable
# dictionary pool snapshot already passed structural, intrinsic semantic, and
# Field-contract validation under the adapter's canonical value Field.
const _ValidatedDictionaries = IdDict{ArrayData,Nothing}
@inline _dictionary_validated(::Nothing, ::ArrayData) = false
@inline _dictionary_validated(memo::_ValidatedDictionaries, d::ArrayData) =
    haskey(memo, d)

@inline _slotindex0(d::ArrayData, i::Int64) =
    checked_add(d.offset, checked_sub(i, Int64(1)))
@inline _slotbyteoff(d::ArrayData, i::Int64, width::Integer) =
    checked_mul(_slotindex0(d, i), Int64(width))

# Buffer-by-role lookup, driven by the registry. Structural validation
# guarantees position/arity, so adapters and accessors never hand-count.
function rolebuffer(d::ArrayData, role::BufferRole)
    spec = layoutspec_of(d.type)
    idx = findfirst(==(role), spec.buffers)
    idx === nothing && throw(ArgumentError("layout $(typeof(d.type)) has no $role buffer"))
    return d.buffers[idx]
end

validitybuffer(d::ArrayData) = rolebuffer(d, VALIDITY)

"""
    isvalid_at(d, i)

Element validity for 1-based logical index `i`. An empty validity slice
means "no nulls recorded" — every element valid (the spec's empty-bitmap
convention). Layouts with no validity buffer (null, union, REE) answer
through their own accessors.
"""
@inline function isvalid_at(d::ArrayData, i::Integer)
    v = validitybuffer(d)
    isempty_buffer(v) && return true
    return getbit(v, _slotindex0(d, Int64(i)))
end

"""
    nullcount(d) -> Int64

Cached lazy null count (the polars `unset_bit_count_cache` idea). The benign
race — two tasks computing the same value and both storing it — is
harmless; `:monotonic` ordering is all the cache needs.
"""
function nullcount(d::ArrayData)
    nc = @atomic :monotonic d.nullcount
    nc >= 0 && return nc
    nc = _count_nulls(d)
    @atomic :monotonic d.nullcount = nc
    return nc
end

function _count_nulls(d::ArrayData)
    d.type isa NullType && return d.len
    spec = layoutspec_of(d.type)
    isempty(spec.buffers) && return Int64(0)
    spec.buffers[1] == VALIDITY || return Int64(0)   # unions: no top-level nulls
    v = d.buffers[1]
    isempty_buffer(v) && return Int64(0)
    n = Int64(0)
    for i = 1:d.len
        n += !getbit(v, _slotindex0(d, Int64(i)))
    end
    return n
end

# ---------------------------------------------------------------------------
# §5 Staged validation
# ---------------------------------------------------------------------------

struct ValidationError <: Exception
    msg::String
end

function expected_validity_bytes(len::Int64)
    len >= 0 || throw(ArgumentError("negative bitmap length"))
    return checked_add(len, Int64(7)) >> 3
end

# Runtime descriptor equality must compare values, not only Julia types.
# One more closed-set ladder (no fieldcount/getfield reflection — that is
# dynamic and trim-hostile); each branch compares its descriptor's fields
# explicitly.
# Non-recursive: the spec forbids dictionary-encoded dictionary VALUES, so
# a DictionaryType's valuetype is never itself a DictionaryType (enforced in
# _validate_descriptor) and one nested ladder suffices — which is what lets
# both levels inline at abstract call sites for trim.
@inline function _typeequal_nondict(a::ArrowType, b::ArrowType)
    a isa IntType && return b isa IntType && a.bits == b.bits && a.signed == b.signed
    a isa FloatType && return b isa FloatType && a.bits == b.bits
    a isa Utf8Type && return b isa Utf8Type && a.large == b.large
    a isa BoolType && return b isa BoolType
    a isa ListType && return b isa ListType && a.large == b.large
    a isa StructType && return b isa StructType
    a isa TimestampType && return b isa TimestampType && a.unit == b.unit &&
        a.timezone == b.timezone
    a isa DateType && return b isa DateType && a.unit == b.unit
    a isa TimeType && return b isa TimeType && a.unit == b.unit && a.bits == b.bits
    a isa DurationType && return b isa DurationType && a.unit == b.unit
    a isa BinaryType && return b isa BinaryType && a.large == b.large
    a isa FixedSizeBinaryType && return b isa FixedSizeBinaryType && a.nbytes == b.nbytes
    a isa FixedSizeListType && return b isa FixedSizeListType && a.listsize == b.listsize
    a isa MapType && return b isa MapType && a.keyssorted == b.keyssorted
    a isa UnionType && return b isa UnionType && a.mode == b.mode && a.typeids == b.typeids
    a isa DecimalType && return b isa DecimalType && a.precision == b.precision &&
        a.scale == b.scale && a.bits == b.bits
    a isa IntervalType && return b isa IntervalType && a.unit == b.unit
    a isa NullType && return b isa NullType
    a isa ViewType && return b isa ViewType && a.utf8 == b.utf8
    a isa ListViewType && return b isa ListViewType && a.large == b.large
    a isa RunEndEncodedType && return b isa RunEndEncodedType
    return false
end

@inline function typeequal(a::ArrowType, b::ArrowType)
    if a isa DictionaryType
        return b isa DictionaryType &&
            a.indextype.bits == b.indextype.bits &&
            a.indextype.signed == b.indextype.signed &&
            _typeequal_nondict(a.valuetype, b.valuetype) &&
            a.ordered == b.ordered
    end
    b isa DictionaryType && return false
    return _typeequal_nondict(a, b)
end

"""
    descriptorname(t::ArrowType) -> Symbol

Closed-set name ladder for error messages: `nameof(typeof(x))` on an
abstract-typed value is itself a dynamic call, so diagnostics use this
instead.
"""
@inline function descriptorname(t::ArrowType)::Symbol
    t isa IntType && return :IntType
    t isa FloatType && return :FloatType
    t isa Utf8Type && return :Utf8Type
    t isa BoolType && return :BoolType
    t isa ListType && return :ListType
    t isa StructType && return :StructType
    t isa DictionaryType && return :DictionaryType
    t isa TimestampType && return :TimestampType
    t isa DateType && return :DateType
    t isa TimeType && return :TimeType
    t isa DurationType && return :DurationType
    t isa BinaryType && return :BinaryType
    t isa FixedSizeBinaryType && return :FixedSizeBinaryType
    t isa FixedSizeListType && return :FixedSizeListType
    t isa MapType && return :MapType
    t isa UnionType && return :UnionType
    t isa DecimalType && return :DecimalType
    t isa IntervalType && return :IntervalType
    t isa NullType && return :NullType
    t isa ViewType && return :ViewType
    t isa ListViewType && return :ListViewType
    t isa RunEndEncodedType && return :RunEndEncodedType
    return :UnknownArrowType
end

_validate_descriptor(::ArrowType) = nothing
_validate_descriptor(::Any) = throw(ArgumentError("unregistered ArrowType"))
_value(::Any, ::Field, ::ArrayData, ::Int64) =
    throw(ArgumentError("unregistered ArrowType"))

@inline function _validate_descriptor_of(t::ArrowType)
    t isa IntType && return _validate_descriptor(t)
    t isa FloatType && return _validate_descriptor(t)
    t isa Utf8Type && return _validate_descriptor(t)
    t isa BoolType && return _validate_descriptor(t)
    t isa ListType && return _validate_descriptor(t)
    t isa StructType && return _validate_descriptor(t)
    t isa DictionaryType && return _validate_descriptor(t)
    t isa TimestampType && return _validate_descriptor(t)
    t isa DateType && return _validate_descriptor(t)
    t isa TimeType && return _validate_descriptor(t)
    t isa DurationType && return _validate_descriptor(t)
    t isa BinaryType && return _validate_descriptor(t)
    t isa FixedSizeBinaryType && return _validate_descriptor(t)
    t isa FixedSizeListType && return _validate_descriptor(t)
    t isa MapType && return _validate_descriptor(t)
    t isa UnionType && return _validate_descriptor(t)
    t isa DecimalType && return _validate_descriptor(t)
    t isa IntervalType && return _validate_descriptor(t)
    t isa NullType && return _validate_descriptor(t)
    t isa ViewType && return _validate_descriptor(t)
    t isa ListViewType && return _validate_descriptor(t)
    t isa RunEndEncodedType && return _validate_descriptor(t)
    throw(ArgumentError("unregistered ArrowType"))
end
_validate_descriptor(t::IntType) = t.bits in (8, 16, 32, 64) ||
    throw(ValidationError("integer bit width must be 8, 16, 32, or 64"))
_validate_descriptor(t::FloatType) = t.bits in (16, 32, 64) ||
    throw(ValidationError("floating-point bit width must be 16, 32, or 64"))
function _validate_descriptor(t::DecimalType)
    maxprecision = t.bits == 32 ? 9 : t.bits == 64 ? 18 :
        t.bits == 128 ? 38 : t.bits == 256 ? 76 : 0
    maxprecision != 0 ||
        throw(ValidationError("decimal bit width must be 32, 64, 128, or 256"))
    1 <= t.precision <= maxprecision ||
        throw(ValidationError("decimal precision $(t.precision) is invalid for $(t.bits)-bit storage"))
    typemin(Int32) <= t.scale <= typemax(Int32) ||
        throw(ValidationError("decimal scale $(t.scale) does not fit the Arrow Int32 wire field"))
    return nothing
end
_validate_descriptor(t::FixedSizeBinaryType) = 0 <= t.nbytes <= typemax(Int32) ||
    throw(ValidationError("fixed-size-binary width must be in [0, $(typemax(Int32))]"))
_validate_descriptor(t::DateType) = t.unit in (DAY, MILLISECOND_DATE) ||
    throw(ValidationError("invalid Arrow date unit $(repr(t.unit))"))
function _validate_descriptor(t::TimeType)
    t.unit in (SECOND, MILLISECOND, MICROSECOND, NANOSECOND) ||
        throw(ValidationError("invalid Arrow time unit $(repr(t.unit))"))
    valid = t.unit in (SECOND, MILLISECOND) ? t.bits == 32 : t.bits == 64
    valid || throw(ValidationError("time unit $(t.unit) is incompatible with $(t.bits)-bit storage"))
    return nothing
end
_validate_descriptor(t::TimestampType) =
    begin
        t.unit in (SECOND, MILLISECOND, MICROSECOND, NANOSECOND) ||
            throw(ValidationError("invalid Arrow timestamp unit $(repr(t.unit))"))
        (t.timezone === nothing || isvalid(t.timezone)) ||
            throw(ValidationError("timestamp timezone is not valid UTF-8"))
        nothing
    end
_validate_descriptor(t::DurationType) =
    t.unit in (SECOND, MILLISECOND, MICROSECOND, NANOSECOND) ||
        throw(ValidationError("invalid Arrow duration unit $(repr(t.unit))"))
_validate_descriptor(t::IntervalType) =
    t.unit in (YEAR_MONTH, DAY_TIME, MONTH_DAY_NANO) ||
        throw(ValidationError("invalid Arrow interval unit $(repr(t.unit))"))
_validate_descriptor(t::FixedSizeListType) = 0 <= t.listsize <= typemax(Int32) ||
    throw(ValidationError("fixed-size-list size must be in [0, $(typemax(Int32))]"))
_validate_descriptor(t::UnionType) =
    t.mode in (SparseMode, DenseMode) ||
        throw(ValidationError("invalid Arrow union mode $(repr(t.mode))"))
function _validate_descriptor(t::DictionaryType)
    _validate_descriptor(t.indextype)
    # The spec forbids dictionary-encoded dictionary values; enforcing it
    # here is also what keeps descriptor equality non-recursive (typeequal).
    t.valuetype isa DictionaryType &&
        throw(ValidationError("dictionary values cannot themselves be dictionary-encoded"))
    _validate_descriptor_of(t.valuetype)
    return nothing
end

function _validate_metadata(metadata, what::AbstractString)
    metadata === nothing && return nothing
    for (key, value) in metadata
        isvalid(key) || throw(ValidationError("$what metadata key is not valid UTF-8"))
        isvalid(value) || throw(ValidationError("$what metadata value is not valid UTF-8"))
    end
    return nothing
end

function _validate_schema(s::Schema)
    s.endianness in (LittleEndian, BigEndian) ||
        throw(ValidationError("invalid Arrow schema endianness $(repr(s.endianness))"))
    s.endianness == _native_endianness() ||
        throw(ValidationError(
            "non-native Arrow schema endianness must be normalized before Core access"))
    _validate_metadata(s.metadata, "schema")
    return s
end

"""
    validate_structural(field, data)

Stage-2 validation (report §9): O(buffers), registry-driven, run at
construction/adaptation time. Checks buffer arity against the layout, and
every buffer's byte length against what the logical length requires — with
checked arithmetic, because these lengths come from untrusted metadata.
Recurses into children and the dictionary.

BufferSlice construction has already bounded every slice inside its region,
so this stage never touches memory — it is pure arithmetic on declared
sizes. (The framing stage — resource limits before metadata-directed decode
allocation and checked message-body spans — belongs to the adapters; see
core/examples/ipc_read.jl.)
"""
validate_structural(f::Field, d::ArrayData) =
    _validate_structural(f, d, nothing)

function _validate_structural(f::Field, d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries})
    isvalid(f.name) ||
        throw(ValidationError("field name is not valid UTF-8"))
    _validate_metadata(f.metadata, "field")
    typeequal(f.type, d.type) ||
        throw(ValidationError("field/type mismatch: $(descriptorname(f.type)) vs $(descriptorname(d.type))"))
    _validate_descriptor_of(d.type)
    spec = layoutspec_of(d.type)
    nfixed = length(spec.buffers)
    buffers_ok = spec.variadic ? length(d.buffers) >= nfixed : length(d.buffers) == nfixed
    buffers_ok || throw(ValidationError(
        "$(descriptorname(d.type)): expected $(spec.variadic ? "at least " : "")$nfixed buffers, got $(length(d.buffers))"))
    total::Int64 = checked_add(d.len, d.offset)
    declared_nulls = @atomic :monotonic d.nullcount
    for (i, role) in enumerate(spec.buffers)
        b = d.buffers[i]
        if role == VALIDITY
            if isempty_buffer(b)
                declared_nulls > 0 &&
                    throw(ValidationError("absent validity bitmap with positive null count"))
                continue
            end
            b.len >= expected_validity_bytes(total) ||
                throw(ValidationError("validity bitmap too small: $(b.len) bytes for $total slots"))
        elseif role == DATA
            if spec.fixedwidth > 0
                need = checked_mul(total, Int64(spec.fixedwidth))
                b.len >= need ||
                    throw(ValidationError("data buffer too small: $(b.len) < $need bytes"))
            elseif spec.fixedwidth == -1   # bit-packed (Bool)
                b.len >= expected_validity_bytes(total) ||
                    throw(ValidationError("bit-packed data buffer too small"))
            end
            # fixedwidth == 0 (varbinary DATA): bounded by offsets in the
            # semantic stage — nothing structural to require here.
        elseif role == OFFSETS
            # Canonical empty offset-based arrays may omit the offsets
            # buffer. A sliced empty array (`offset > 0`) still needs the
            # physical prefix that its offset addresses.
            isempty_buffer(b) && d.len == 0 && d.offset == 0 && continue
            need = checked_mul(checked_add(total, Int64(1)), Int64(spec.offsetwidth))
            b.len >= need ||
                throw(ValidationError("offsets buffer too small: $(b.len) < $need bytes"))
        elseif role == ELEMENT_OFFSETS
            need = checked_mul(total, Int64(spec.offsetwidth))
            b.len >= need ||
                throw(ValidationError("element-offsets buffer too small: $(b.len) < $need bytes"))
        elseif role == SIZES
            need = checked_mul(total, Int64(spec.offsetwidth))
            b.len >= need ||
                throw(ValidationError("sizes buffer too small"))
        elseif role == TYPE_IDS
            b.len >= total ||
                throw(ValidationError("type_ids buffer too small"))
        elseif role == VIEWS
            need = checked_mul(total, Int64(16))
            b.len >= need ||
                throw(ValidationError("views buffer too small"))
        end
    end
    # Child arity: registry-declared, or Field-declared for struct/union.
    expected_children = spec.childcount == -1 ? length(f.children) : spec.childcount
    if !(d.type isa DictionaryType)
        length(f.children) == expected_children ||
            throw(ValidationError("$(typeof(d.type)): expected $expected_children child fields, got $(length(f.children))"))
    end
    length(d.children) == expected_children ||
        throw(ValidationError("$(typeof(d.type)): expected $expected_children children, got $(length(d.children))"))
    for (cf, cd) in zip(childfields(f), d.children)
        _validate_structural(cf, cd, validated_dictionaries)
    end
    if d.type isa DictionaryType
        d.dictionary === nothing &&
            throw(ValidationError("dictionary-encoded array without a dictionary"))
        dictionary = d.dictionary::ArrayData
        _dictionary_validated(validated_dictionaries, dictionary) ||
            _validate_structural(dictvaluefield(f, d.type), dictionary,
                validated_dictionaries)
    elseif d.dictionary !== nothing
        throw(ValidationError("dictionary values attached to a non-dictionary array"))
    end
    fslt = d.type
    if fslt isa FixedSizeListType
        need = checked_mul(total, Int64(fslt.listsize))
        length(d.children[1]) >= need ||
            throw(ValidationError("fixed-size-list child too short: $(length(d.children[1])) < $need"))
    end
    # Struct and sparse-union children are parent-length arrays indexed at
    # parent.offset + i (each child then applies its own offset), so every
    # child must cover offset+len slots.
    if d.type isa StructType || (d.type isa UnionType && d.type.mode == SparseMode)
        for (ci, child) in enumerate(d.children)
            length(child) >= total ||
                throw(ValidationError("child $ci too short for parent extent: $(length(child)) < $total"))
        end
    end
    ut = d.type
    if ut isa UnionType
        length(ut.typeids) == length(f.children) ||
            throw(ValidationError("union type-id count must equal child count"))
        length(unique(ut.typeids)) == length(ut.typeids) ||
            throw(ValidationError("union type ids must be unique"))
        all(>=(0), ut.typeids) ||
            throw(ValidationError("union type ids must be in [0, 127]"))
    end
    if d.type isa MapType
        entries = f.children[1]
        entries.type isa StructType ||
            throw(ValidationError("map child must be an entries struct"))
        !entries.nullable ||
            throw(ValidationError("map entries field must be non-nullable"))
        length(entries.children) == 2 ||
            throw(ValidationError("map entries struct must have key and value children"))
        !entries.children[1].nullable ||
            throw(ValidationError("map keys must be non-nullable"))
    end
    if d.type isa RunEndEncodedType
        runfield, valuefield = f.children
        runfield.name == "run_ends" && valuefield.name == "values" ||
            throw(ValidationError("REE children must be named run_ends and values"))
        runtype0 = runfield.type
        runtype0 isa IntType ||
            throw(ValidationError("REE run ends must be signed int16, int32, or int64"))
        runtype = runtype0::IntType
        (runtype.signed && runtype.bits in (16, 32, 64)) ||
            throw(ValidationError("REE run ends must be signed int16, int32, or int64"))
        !runfield.nullable ||
            throw(ValidationError("REE run ends must be non-nullable"))
        declared_nulls == 0 ||
            throw(ValidationError("REE parent null count must be zero"))
        length(d.children[1]) == length(d.children[2]) ||
            throw(ValidationError("REE run-end and value child lengths must match"))
        total == 0 || length(d.children[1]) > 0 ||
            throw(ValidationError("a nonempty REE array requires at least one physical run"))
        maxrunend = runtype.bits == 16 ? Int64(typemax(Int16)) :
            runtype.bits == 32 ? Int64(typemax(Int32)) : typemax(Int64)
        total <= maxrunend ||
            throw(ValidationError(
                "REE logical extent $total exceeds the $(runtype.bits)-bit run-end range"))
        !(valuefield.type isa RunEndEncodedType) ||
            throw(ValidationError("nested run-end encoding is not permitted"))
    end
    return d
end

# Child Fields for traversal. For list-ish layouts the child field is the
# Field's single declared child; dictionary values reuse the field with the
# value type.
childfields(f::Field) = f.children
dictvaluefield(f::Field, t::DictionaryType) =
    # Dictionary values have their own nullability. The index field's
    # nullable flag describes only the indices and cannot constrain the pool.
    Field(f.name, t.valuetype; nullable=true, children=f.children)

const MILLISECONDS_PER_DAY = Int64(86_400_000)

_validate_temporal_values(::ArrowType, ::ArrayData) = nothing
function _validate_temporal_values(t::DateType, d::ArrayData)
    t.unit == MILLISECOND_DATE || return nothing
    data = rolebuffer(d, DATA)
    for i = 1:d.len
        isvalid_at(d, i) || continue
        value = loadat(data, Int64, _slotbyteoff(d, Int64(i), 8))
        value % MILLISECONDS_PER_DAY == 0 ||
            throw(ValidationError("Date64 value $value is not a whole day in milliseconds"))
    end
    return nothing
end

function _decimal_limb(t::DecimalType, data::BufferSlice, byteoff::Int64,
    nlimbs::Int, limb::Int)::UInt64
    limb <= nlimbs || return UInt64(0)
    source_limb = _native_endianness() == LittleEndian ? limb : nlimbs - limb + 1
    base = checked_add(byteoff, Int64(8 * (source_limb - 1)))
    return t.bits == 32 ? UInt64(loadat(data, UInt32, base)) :
        loadat(data, UInt64, base)
end

function _decimal_fits_precision(t::DecimalType, data::BufferSlice, byteoff::Int64)
    # Core accepts only native-endian array buffers. Arrow decimal storage is
    # a two's-complement integer, so put native chunks into least-significant
    # limb order before comparing its magnitude with 10^p. Work in fixed
    # UInt256-style arithmetic so Core stays Base-only and Decimal256 does not
    # require BigInt allocations or BitIntegers. (No closures here: captured
    # and reassigned locals box, which defeats trim verification.)
    nlimbs = cld(t.bits, 64)
    l1 = _decimal_limb(t, data, byteoff, nlimbs, 1)
    l2 = _decimal_limb(t, data, byteoff, nlimbs, 2)
    l3 = _decimal_limb(t, data, byteoff, nlimbs, 3)
    l4 = _decimal_limb(t, data, byteoff, nlimbs, 4)
    signbit = t.bits == 32 ? UInt64(1) << 31 : UInt64(1) << 63
    negative = ((nlimbs == 1 ? l1 : nlimbs == 2 ? l2 : nlimbs == 3 ? l3 : l4) & signbit) != 0
    if negative && t.bits == 32
        l1 |= typemax(UInt64) << 32
    end
    m1 = nlimbs >= 1 ? (negative ? ~l1 : l1) : UInt64(0)
    m2 = nlimbs >= 2 ? (negative ? ~l2 : l2) : UInt64(0)
    m3 = nlimbs >= 3 ? (negative ? ~l3 : l3) : UInt64(0)
    m4 = nlimbs >= 4 ? (negative ? ~l4 : l4) : UInt64(0)
    if negative
        m1 += UInt64(1)
        c = m1 == 0
        m2 += c ? UInt64(1) : UInt64(0)
        c &= m2 == 0
        m3 += c ? UInt64(1) : UInt64(0)
        c &= m3 == 0
        m4 += c ? UInt64(1) : UInt64(0)
    end

    L1, L2, L3, L4 = UInt64(1), UInt64(0), UInt64(0), UInt64(0)
    for _ = 1:t.precision
        p1 = UInt128(L1) * 10
        p2 = UInt128(L2) * 10 + (p1 >> 64)
        p3 = UInt128(L3) * 10 + (p2 >> 64)
        p4 = UInt128(L4) * 10 + (p3 >> 64)
        L1 = UInt64(p1 & UInt128(typemax(UInt64)))
        L2 = UInt64(p2 & UInt128(typemax(UInt64)))
        L3 = UInt64(p3 & UInt128(typemax(UInt64)))
        L4 = UInt64(p4 & UInt128(typemax(UInt64)))
    end
    m4 < L4 && return true
    m4 > L4 && return false
    m3 < L3 && return true
    m3 > L3 && return false
    m2 < L2 && return true
    m2 > L2 && return false
    return m1 < L1
end

_validate_decimal_values(::ArrowType, ::ArrayData) = nothing
function _validate_decimal_values(t::DecimalType, d::ArrayData)
    data = rolebuffer(d, DATA)
    width = Int64(primwidth(t))
    for i = 1:d.len
        isvalid_at(d, i) || continue
        byteoff = _slotbyteoff(d, Int64(i), width)
        _decimal_fits_precision(t, data, byteoff) ||
            throw(ValidationError(
                "Decimal value at element $i does not fit precision $(t.precision)"))
    end
    return nothing
end

function _validate_temporal_values(t::TimeType, d::ArrayData)
    units_per_day = t.unit == SECOND ? Int64(86_400) :
        t.unit == MILLISECOND ? MILLISECONDS_PER_DAY :
        t.unit == MICROSECOND ? Int64(86_400_000_000) :
        Int64(86_400_000_000_000)
    data = rolebuffer(d, DATA)
    for i = 1:d.len
        isvalid_at(d, i) || continue
        value = if t.bits == 32
            Int64(loadat(data, Int32,
                _slotbyteoff(d, Int64(i), Int64(4))))
        else
            loadat(data, Int64,
                _slotbyteoff(d, Int64(i), Int64(8)))
        end
        0 <= value < units_per_day ||
            throw(ValidationError("Time value $value is outside [0, $units_per_day) for $(t.unit)"))
    end
    return nothing
end

"""
    validate_semantic(field, data)

Stage-3 validation. This public stage composes structural validation before
any content access, so callers cannot accidentally certify malformed buffer
geometry by skipping `validate_structural`. Data-intrinsic checks are cached
on the ArrayData (`semachecked`); benign concurrent callers may repeat the
same scan. Field-dependent contracts, including ancestor-masked nullability,
run on every call because the same data can be checked against another Field.
Layouts declared as structural-only fail closed instead of caching an
incomplete check.
"""
function validate_semantic(f::Field, d::ArrayData)
    return _validate_semantic(f, d, nothing)
end

function _validate_semantic(f::Field, d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries})
    _validate_structural(f, d, validated_dictionaries)
    _validate_semantic_intrinsic(f, d, validated_dictionaries)
    _validate_field_contracts(f, d, validated_dictionaries)
    return d
end

function _validate_semantic_intrinsic(f::Field, d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries}=nothing)
    t = d.type
    if t isa Union{ViewType,ListViewType,RunEndEncodedType}
        throw(ValidationError(
            "semantic validation is not implemented for $(descriptorname(t)); " *
            "only structural validation is available"))
    end
    if !(@atomic :monotonic d.semachecked)
        spec = layoutspec_of(t)
        oi = findfirst(==(OFFSETS), spec.buffers)
        if oi !== nothing && spec.offsetwidth != 0
            wide = spec.offsetwidth == 8
            offs = d.buffers[oi]
            if !(isempty_buffer(offs) && d.len == 0 && d.offset == 0)
                databytes = if t isa Utf8Type || t isa BinaryType
                    di = findfirst(==(DATA), spec.buffers)
                    d.buffers[di].len
                else
                    isempty(d.children) ? Int64(0) : Int64(length(d.children[1]))
                end
                prev = _load_offset(offs, wide, d.offset)
                prev >= 0 || throw(ValidationError("negative first offset"))
                for i = 1:d.len
                    cur = _load_offset(offs, wide, checked_add(d.offset, Int64(i)))
                    cur >= prev || throw(ValidationError("offsets not monotonically non-decreasing at $i"))
                    prev = cur
                end
                prev <= databytes ||
                    throw(ValidationError("final offset $prev exceeds data extent $databytes"))
            end
        end
        if t isa DictionaryType
            dictlen = length(d.dictionary)
            data = rolebuffer(d, DATA)
            w = primwidth(t.indextype)
            for i = 1:d.len
                isvalid_at(d, i) || continue
                idx = _load_int(data, t.indextype, _slotbyteoff(d, Int64(i), w))
                0 <= idx < dictlen ||
                    throw(ValidationError("dictionary index $idx out of bounds [0, $dictlen)"))
            end
        end
        if t isa UnionType
            ids = rolebuffer(d, TYPE_IDS)
            lastoffset = fill(Int64(-1), length(d.children))
            for i = 1:d.len
                tid = loadat(ids, Int8, _slotindex0(d, Int64(i)))
                pos = findfirst(==(tid), t.typeids)
                pos === nothing && throw(ValidationError("union type id $tid not in declared domain"))
                if t.mode == DenseMode
                    off = loadat(rolebuffer(d, ELEMENT_OFFSETS), Int32,
                        _slotbyteoff(d, Int64(i), 4))
                    0 <= off < length(d.children[pos]) ||
                        throw(ValidationError("dense union offset $off out of bounds for child $pos"))
                    Int64(off) >= lastoffset[pos] ||
                        throw(ValidationError("dense union offsets must be nondecreasing within child $pos"))
                    lastoffset[pos] = Int64(off)
                end
            end
        end
        _validate_temporal_values(t, d)
        _validate_decimal_values(t, d)
        actual_nulls = _count_nulls(d)
        declared_nulls = @atomic :monotonic d.nullcount
        if declared_nulls >= 0 && declared_nulls != actual_nulls
            throw(ValidationError("declared null count $declared_nulls does not match bitmap count $actual_nulls"))
        elseif declared_nulls < 0
            @atomic :monotonic d.nullcount = actual_nulls
        end
        @atomic :monotonic d.semachecked = true
    end
    for (cf, cd) in zip(childfields(f), d.children)
        _validate_semantic_intrinsic(cf, cd, validated_dictionaries)
    end
    if t isa DictionaryType
        dictionary = d.dictionary::ArrayData
        _dictionary_validated(validated_dictionaries, dictionary) ||
            _validate_semantic_intrinsic(dictvaluefield(f, t), dictionary,
                validated_dictionaries)
    end
    return d
end

function _logical_null_at(f::Field, d::ArrayData, i::Int64)
    t = d.type
    t isa NullType && return true
    if t isa UnionType
        tid = loadat(rolebuffer(d, TYPE_IDS), Int8, _slotindex0(d, i))
        pos = findfirst(==(tid), t.typeids)
        pos === nothing && throw(ValidationError("union type id $tid not in declared domain"))
        childi = if t.mode == DenseMode
            off = loadat(rolebuffer(d, ELEMENT_OFFSETS), Int32, _slotbyteoff(d, i, 4))
            checked_add(Int64(off), Int64(1))
        else
            checked_add(d.offset, i)
        end
        return _logical_null_at(f.children[pos], d.children[pos], childi)
    end
    spec = layoutspec_of(t)
    return !isempty(spec.buffers) && spec.buffers[1] == VALIDITY && !isvalid_at(d, i)
end

function _union_child(f::Field, d::ArrayData, i::Int64)
    t = d.type::UnionType
    tid = loadat(rolebuffer(d, TYPE_IDS), Int8, _slotindex0(d, i))
    pos = findfirst(==(tid), t.typeids)
    pos === nothing && throw(ValidationError("union type id $tid not in declared domain"))
    childi = if t.mode == DenseMode
        off = loadat(rolebuffer(d, ELEMENT_OFFSETS), Int32, _slotbyteoff(d, i, 4))
        checked_add(Int64(off), Int64(1))
    else
        checked_add(d.offset, i)
    end
    return f.children[pos], d.children[pos], childi
end

function _validate_field_contract_at(f::Field, d::ArrayData, i::Int64)
    t = d.type
    if t isa UnionType
        if !f.nullable && _logical_null_at(f, d, i)
            throw(ValidationError(
                "non-nullable field $(repr(f.name)) contains a null at element $i"))
        end
        # A union has no parent validity bitmap. Its selected child supplies
        # both the value and any logical null, so validate that child even
        # when the union Field itself permits nulls. Unselected child slots
        # are not part of this logical value and must remain ignored.
        cf, cd, childi = _union_child(f, d, i)
        _validate_field_contract_at(cf, cd, childi)
        return nothing
    end

    slotnull = t isa NullType || !isvalid_at(d, i)
    if slotnull
        f.nullable || throw(ValidationError(
            "non-nullable field $(repr(f.name)) contains a null at element $i"))
        # Child storage below a null parent value is unspecified. In
        # particular, null Struct/FixedSizeList slots and null List/Map
        # ranges mask nulls in otherwise non-nullable child Fields.
        return nothing
    end

    if t isa StructType
        childi = checked_add(d.offset, i)
        for (cf, cd) in zip(f.children, d.children)
            _validate_field_contract_at(cf, cd, childi)
        end
    elseif t isa FixedSizeListType
        base = checked_mul(_slotindex0(d, i), Int64(t.listsize))
        cf, cd = f.children[1], d.children[1]
        for j = 1:t.listsize
            _validate_field_contract_at(cf, cd,
                checked_add(base, Int64(j)))
        end
    elseif t isa Union{ListType,MapType}
        lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
        lo == hi && return nothing
        cf, cd = f.children[1], d.children[1]
        for childi = checked_add(lo, Int64(1)):hi
            _validate_field_contract_at(cf, cd, childi)
        end
    end
    return nothing
end

function _validate_dictionary_contracts(f::Field, d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries})
    if d.type isa DictionaryType
        # Dictionary values form an independent array. Index nullability never
        # constrains pool nullability, but nested Field contracts inside the
        # pool still apply to every pool value, even when the dictionary array
        # itself is nested below a masked parent.
        dictionary = d.dictionary::ArrayData
        _dictionary_validated(validated_dictionaries, dictionary) ||
            _validate_field_contracts(dictvaluefield(f, d.type), dictionary,
                validated_dictionaries)
    end
    for (cf, cd) in zip(f.children, d.children)
        _validate_dictionary_contracts(cf, cd, validated_dictionaries)
    end
    return nothing
end

function _validate_field_contracts(f::Field, d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries}=nothing)
    for i = 1:d.len
        _validate_field_contract_at(f, d, Int64(i))
    end
    _validate_dictionary_contracts(f, d, validated_dictionaries)
    return nothing
end

"""
    validate_full(field, data)

Stage-4 (opt-in) content validation. It composes semantic (and therefore
structural) validation before the more expensive whole-content checks.
"""
function validate_full(f::Field, d::ArrayData)
    validate_semantic(f, d)
    _validate_full_content(f, d)
    return d
end

function _validate_full_content(f::Field, d::ArrayData)
    if d.type isa Utf8Type
        for i = 1:d.len
            isvalid_at(d, i) || continue
            s = getvalue(f, d, i)::String
            # A malformed byte sequence iterates as invalid Chars; checking
            # every Char is the stdlib idiom for whole-string validity.
            all(isvalid, s) || throw(ValidationError("invalid UTF-8 at element $i"))
        end
    end
    for (cf, cd) in zip(childfields(f), d.children)
        _validate_full_content(cf, cd)
    end
    if d.type isa DictionaryType
        _validate_full_content(dictvaluefield(f, d.type), d.dictionary)
    end
    return nothing
end

# ---------------------------------------------------------------------------
# §6 Element access (per-layout semantics; the facade's raw material)
# ---------------------------------------------------------------------------

# Julia storage type for each descriptor: what getvalue returns for valid
# elements. Timestamps etc. return their raw storage integers here — the
# *facade* owns the Dates conversion layer; keeping Core conversion-free is
# what lets the C-data and IPC adapters share it unchanged.
juliatype(::BoolType) = Bool
juliatype(t::IntType) = t.signed ?
    (t.bits == 8 ? Int8 : t.bits == 16 ? Int16 : t.bits == 32 ? Int32 : Int64) :
    (t.bits == 8 ? UInt8 : t.bits == 16 ? UInt16 : t.bits == 32 ? UInt32 : UInt64)
juliatype(t::FloatType) = t.bits == 16 ? Float16 : t.bits == 32 ? Float32 : Float64
juliatype(::TimestampType) = Int64
juliatype(::DurationType) = Int64
juliatype(t::DateType) = t.unit == DAY ? Int32 : Int64
juliatype(t::TimeType) = t.bits == 32 ? Int32 : Int64
juliatype(::Utf8Type) = String
juliatype(::BinaryType) = Vector{UInt8}
juliatype(t::FixedSizeBinaryType) = Vector{UInt8}

@inline function _load_int(b::BufferSlice, t::IntType, byteoff::Int64)::Int64
    # Literal load widths avoid a runtime DataType in the raw-load path, which
    # produces code that the trim verifier cannot resolve.
    if t.signed
        t.bits == 64 && return loadat(b, Int64, byteoff)
        t.bits == 32 && return Int64(loadat(b, Int32, byteoff))
        t.bits == 16 && return Int64(loadat(b, Int16, byteoff))
        return Int64(loadat(b, Int8, byteoff))
    else
        t.bits == 64 && return Int64(loadat(b, UInt64, byteoff))
        t.bits == 32 && return Int64(loadat(b, UInt32, byteoff))
        t.bits == 16 && return Int64(loadat(b, UInt16, byteoff))
        return Int64(loadat(b, UInt8, byteoff))
    end
end

"""
    getvalue(field, data, i) -> Union{Missing, value}

Read logical element `i` (1-based). Layout dispatch happens on the runtime
descriptor — one dynamic dispatch per call. This is Core's honest contract
(report §8.9): scalar access through the erased representation pays a
boundary cost; `materialize` resolves the layout once and loops through a
function barrier.
"""
function getvalue(f::Field, d::ArrayData, i::Integer)
    1 <= i <= d.len || throw(BoundsError(d, i))
    return _value_of(d.type, f, d, Int64(i))
end

# -- primitives -------------------------------------------------------------

# Primitive accessors branch to LITERAL load widths: `loadat(b, T, off)` with
# a runtime `T::DataType` leaves the raw-load path unresolved under trim
# verification, and a concrete branch is faster anyway.
function _value(t::IntType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    b = rolebuffer(d, DATA)
    if t.signed
        t.bits == 64 && return loadat(b, Int64, _slotbyteoff(d, i, 8))
        t.bits == 32 && return loadat(b, Int32, _slotbyteoff(d, i, 4))
        t.bits == 16 && return loadat(b, Int16, _slotbyteoff(d, i, 2))
        return loadat(b, Int8, _slotbyteoff(d, i, 1))
    else
        t.bits == 64 && return loadat(b, UInt64, _slotbyteoff(d, i, 8))
        t.bits == 32 && return loadat(b, UInt32, _slotbyteoff(d, i, 4))
        t.bits == 16 && return loadat(b, UInt16, _slotbyteoff(d, i, 2))
        return loadat(b, UInt8, _slotbyteoff(d, i, 1))
    end
end

function _value(t::FloatType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    b = rolebuffer(d, DATA)
    t.bits == 64 && return loadat(b, Float64, _slotbyteoff(d, i, 8))
    t.bits == 32 && return loadat(b, Float32, _slotbyteoff(d, i, 4))
    return loadat(b, Float16, _slotbyteoff(d, i, 2))
end

function _value(t::Union{TimestampType,DurationType}, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    return loadat(rolebuffer(d, DATA), Int64, _slotbyteoff(d, i, 8))
end

function _value(t::DateType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    b = rolebuffer(d, DATA)
    return t.unit == DAY ? loadat(b, Int32, _slotbyteoff(d, i, 4)) :
        loadat(b, Int64, _slotbyteoff(d, i, 8))
end

function _value(t::TimeType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    b = rolebuffer(d, DATA)
    return t.bits == 32 ? loadat(b, Int32, _slotbyteoff(d, i, 4)) :
        loadat(b, Int64, _slotbyteoff(d, i, 8))
end

function _value(t::DecimalType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    w = primwidth(t)
    # 128/256-bit decimals surface as raw native-endian bytes in the prove-out
    # (BigInt/Int256 conversion is facade work); 32/64 as integers. Core
    # RecordBatches accept native-endian buffers only.
    if t.bits == 32
        return loadat(rolebuffer(d, DATA), Int32, _slotbyteoff(d, i, w))
    elseif t.bits == 64
        return loadat(rolebuffer(d, DATA), Int64, _slotbyteoff(d, i, w))
    else
        b = rolebuffer(d, DATA)
        off = _slotbyteoff(d, i, w)
        return [loadat(b, UInt8, checked_add(off, Int64(k))) for k = 0:(w - 1)]
    end
end

function _value(t::IntervalType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    b = rolebuffer(d, DATA)
    if t.unit == YEAR_MONTH
        return loadat(b, Int32, _slotbyteoff(d, i, 4))
    elseif t.unit == DAY_TIME
        off = _slotbyteoff(d, i, 8)
        return (days=loadat(b, Int32, off),
            millis=loadat(b, Int32, checked_add(off, Int64(4))))
    else # MONTH_DAY_NANO — the unit today's Arrow.jl cannot even parse
        off = _slotbyteoff(d, i, 16)
        return (months=loadat(b, Int32, off),
            days=loadat(b, Int32, checked_add(off, Int64(4))),
            nanos=loadat(b, Int64, checked_add(off, Int64(8))))
    end
end

function _value(::BoolType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    return getbit(rolebuffer(d, DATA), _slotindex0(d, i))
end

function _value(::NullType, f::Field, d::ArrayData, i::Int64)
    return missing
end

function _value(t::FixedSizeBinaryType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    b = rolebuffer(d, DATA)
    off = _slotbyteoff(d, i, t.nbytes)
    return slicebytes(subslice(b, off, t.nbytes))
end

# -- varbinary --------------------------------------------------------------

"Concrete-width offset load: `idx0` is the 0-based entry index."
@inline function _load_offset(offs::BufferSlice, wide::Bool, idx0::Int64)::Int64
    return wide ? loadat(offs, Int64, checked_mul(idx0, Int64(8))) :
        Int64(loadat(offs, Int32, checked_mul(idx0, Int64(4))))
end

@inline function _offsets_at(d::ArrayData, i::Int64, wide::Bool)
    offs = rolebuffer(d, OFFSETS)
    lo = _load_offset(offs, wide, d.offset + i - 1)
    hi = _load_offset(offs, wide, d.offset + i)
    return lo, hi
end

function _value(t::Union{Utf8Type,BinaryType}, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
    data = rolebuffer(d, DATA)
    n = hi - lo
    n == 0 && return t isa Utf8Type ? "" : UInt8[]
    # Semantic validation bounded final offsets against the data extent, but
    # subslice re-checks: accessors stay safe even when a caller skipped
    # validate_semantic (they just pay per-access checking).
    bytes = slicebytes(subslice(data, lo, n))
    return t isa Utf8Type ? String(bytes) : bytes
end

# -- nested -----------------------------------------------------------------

function _value(t::ListType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
    child, cf = d.children[1], f.children[1]
    # Explicit Vector{Any}: an Any-first comprehension re-narrows its result
    # at runtime, which is both trim-hostile and wasted work — typed element
    # containers are the facade's job (report §9 facade).
    out = Vector{Any}(undef, Int(hi - lo))
    for k = 1:Int(hi - lo)
        out[k] = getvalue(cf, child, checked_add(lo, Int64(k)))
    end
    return out
end

function _value(t::FixedSizeListType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    child, cf = d.children[1], f.children[1]
    base = _slotbyteoff(d, i, t.listsize)
    out = Vector{Any}(undef, t.listsize)
    for j = 1:t.listsize
        out[j] = getvalue(cf, child, checked_add(base, Int64(j)))
    end
    return out
end

function _value(::StructType, f::Field, d::ArrayData, i::Int64)
    # Core's struct scalar is an ordered Vector{Pair{String,Any}} — always.
    # A NamedTuple carries its names in the TYPE domain, so building one from
    # runtime schema names is intrinsically dynamic (and cannot represent
    # Arrow's duplicate/empty/non-Symbol names at all). The typed NamedTuple
    # surface is exactly the facade's ViewPlan decision in the report
    # (§9 facade, §14.2); Core stays concrete and trim-clean.
    isvalid_at(d, i) || return missing
    childindex = checked_add(d.offset, i)
    n = length(f.children)
    out = Vector{Pair{String,Any}}(undef, n)
    for j = 1:n
        out[j] = Pair{String,Any}(f.children[j].name,
            getvalue(f.children[j], d.children[j], childindex))
    end
    return out
end

function _value(t::MapType, f::Field, d::ArrayData, i::Int64)
    # Map = List<Struct<key,value>>; reuse the list walk and pair up.
    isvalid_at(d, i) || return missing
    lo, hi = _offsets_at(d, i, false)
    entries, ef = d.children[1], f.children[1]
    kf, vf = ef.children[1], ef.children[2]
    kd, vd = entries.children[1], entries.children[2]
    out = Vector{Pair{Any,Any}}(undef, Int(hi - lo))
    for k = 1:Int(hi - lo)
        entryindex = checked_add(entries.offset, checked_add(lo, Int64(k)))
        out[k] = Pair{Any,Any}(getvalue(kf, kd, entryindex),
            getvalue(vf, vd, entryindex))
    end
    return out
end

function _value(t::UnionType, f::Field, d::ArrayData, i::Int64)
    tid = loadat(rolebuffer(d, TYPE_IDS), Int8, _slotindex0(d, i))
    pos = findfirst(==(tid), t.typeids)
    pos === nothing && throw(ValidationError("union type id $tid not in declared domain"))
    child, cf = d.children[pos], f.children[pos]
    if t.mode == DenseMode
        off = loadat(rolebuffer(d, ELEMENT_OFFSETS), Int32, _slotbyteoff(d, i, 4))
        return getvalue(cf, child, checked_add(Int64(off), Int64(1)))
    else
        return getvalue(cf, child, checked_add(d.offset, i))
    end
end

function _value(t::DictionaryType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    w = primwidth(t.indextype)
    idx = _load_int(rolebuffer(d, DATA), t.indextype, _slotbyteoff(d, i, w))
    dict = d.dictionary
    dict === nothing && throw(ValidationError("dictionary-encoded array without a dictionary"))
    return getvalue(dictvaluefield(f, t), dict,
        checked_add(Int64(idx), Int64(1)))
end

_value(t::Union{ViewType,ListViewType,RunEndEncodedType}, f::Field, d::ArrayData, i::Int64) =
    error("element access for $(descriptorname(t)) is roadmap work (report §13, slices 2f/2h); " *
          "the layout is registry-known and structurally validated only")

"""
    materialize(field, data) -> Vector

Bulk conversion to native Julia values — the miniature of the facade's
ViewPlan idea: resolve the layout ONCE, then run a specialized loop behind a
function barrier. `_materialize_loop` is generic over the concrete
descriptor type it receives, so the loop body compiles per LAYOUT (a small
closed set), never per schema.
"""
materialize(f::Field, d::ArrayData) = _materialize_of(d.type, f, d)

# The same closed-set ladder as `layoutspec_of`, for element access and the
# materialize function barrier: generic entry devirtualizes here; per-layout
# `_value` methods stay the extension surface.
@inline function _value_of(t::ArrowType, f::Field, d::ArrayData, i::Int64)
    t isa IntType && return _value(t, f, d, i)
    t isa FloatType && return _value(t, f, d, i)
    t isa Utf8Type && return _value(t, f, d, i)
    t isa BoolType && return _value(t, f, d, i)
    t isa ListType && return _value(t, f, d, i)
    t isa StructType && return _value(t, f, d, i)
    t isa DictionaryType && return _value(t, f, d, i)
    t isa TimestampType && return _value(t, f, d, i)
    t isa DateType && return _value(t, f, d, i)
    t isa TimeType && return _value(t, f, d, i)
    t isa DurationType && return _value(t, f, d, i)
    t isa BinaryType && return _value(t, f, d, i)
    t isa FixedSizeBinaryType && return _value(t, f, d, i)
    t isa FixedSizeListType && return _value(t, f, d, i)
    t isa MapType && return _value(t, f, d, i)
    t isa UnionType && return _value(t, f, d, i)
    t isa DecimalType && return _value(t, f, d, i)
    t isa IntervalType && return _value(t, f, d, i)
    t isa NullType && return _value(t, f, d, i)
    t isa ViewType && return _value(t, f, d, i)
    t isa ListViewType && return _value(t, f, d, i)
    t isa RunEndEncodedType && return _value(t, f, d, i)
    throw(ArgumentError("unregistered ArrowType"))
end

@inline function _materialize_of(t::ArrowType, f::Field, d::ArrayData)
    t isa IntType && return _materialize_loop(t, f, d)
    t isa FloatType && return _materialize_loop(t, f, d)
    t isa Utf8Type && return _materialize_loop(t, f, d)
    t isa BoolType && return _materialize_loop(t, f, d)
    t isa ListType && return _materialize_loop(t, f, d)
    t isa StructType && return _materialize_loop(t, f, d)
    t isa DictionaryType && return _materialize_loop(t, f, d)
    t isa TimestampType && return _materialize_loop(t, f, d)
    t isa DateType && return _materialize_loop(t, f, d)
    t isa TimeType && return _materialize_loop(t, f, d)
    t isa DurationType && return _materialize_loop(t, f, d)
    t isa BinaryType && return _materialize_loop(t, f, d)
    t isa FixedSizeBinaryType && return _materialize_loop(t, f, d)
    t isa FixedSizeListType && return _materialize_loop(t, f, d)
    t isa MapType && return _materialize_loop(t, f, d)
    t isa UnionType && return _materialize_loop(t, f, d)
    t isa DecimalType && return _materialize_loop(t, f, d)
    t isa IntervalType && return _materialize_loop(t, f, d)
    t isa NullType && return _materialize_loop(t, f, d)
    t isa ViewType && return _materialize_loop(t, f, d)
    t isa ListViewType && return _materialize_loop(t, f, d)
    t isa RunEndEncodedType && return _materialize_loop(t, f, d)
    throw(ArgumentError("unregistered ArrowType"))
end

function _materialize_loop(t::T, f::Field, d::ArrayData) where {T<:ArrowType}
    out = Vector{Any}(undef, d.len)
    for i = 1:d.len
        out[i] = _value(t, f, d, Int64(i))
    end
    # Vector{Any} by design: result-element typing (and the narrowing pass
    # 2.x users expect) is the facade's typed-view work, and the runtime
    # narrow is trim-hostile. Tests compare with ==/isequal, which is
    # eltype-agnostic.
    return out
end

# ---------------------------------------------------------------------------
# §7 Builders: Julia data -> (Field, ArrayData)
# ---------------------------------------------------------------------------

# The write-side counterpart, kept intentionally small: enough construction
# machinery to exercise every implemented layout without an IPC file in the
# loop. The real builder layer (append-oriented, byte-budgeted) is facade
# work; these are the "zero-copy wrap + bitmap build" fast paths the report
# describes.

function _bitmapbuffer(present::AbstractVector{Bool})
    any(!, present) || return BufferSlice()   # no nulls -> canonical empty
    bytes = zeros(UInt8, expected_validity_bytes(Int64(length(present))))
    for (i, p) in enumerate(present)
        p && (bytes[1 + ((i - 1) >> 3)] |= UInt8(1) << ((i - 1) & 7))
    end
    return BufferSlice(heapregion(bytes), 0, length(bytes))
end

_databuffer(v::Vector{T}) where {T} = BufferSlice(heapregion(v), 0, sizeof(v))

arrowtype_for(::Type{Bool}) = BoolType()
arrowtype_for(::Type{T}) where {T<:Signed} = IntType(8 * sizeof(T), true)
arrowtype_for(::Type{T}) where {T<:Unsigned} = IntType(8 * sizeof(T), false)
arrowtype_for(::Type{Float16}) = FloatType(16)
arrowtype_for(::Type{Float32}) = FloatType(32)
arrowtype_for(::Type{Float64}) = FloatType(64)
arrowtype_for(::Type{String}) = Utf8Type(false)

"""
    fromjulia(name, v) -> (Field, ArrayData)

Adapt a Julia vector to Core form. `Vector{T}` for fixed-width isbits `T` is
a ZERO-COPY wrap (the vector becomes the region's root; scoped-borrow
contract: don't resize/mutate while in use). `Union{T,Missing}` and String
inputs build fresh buffers.
"""
function fromjulia(name, v::Vector{T}) where {T}
    if T <: Union{Int8,Int16,Int32,Int64,UInt8,UInt16,UInt32,UInt64,Float16,Float32,Float64}
        t = arrowtype_for(T)
        return Field(name, t; nullable=false),
        ArrayData(t, length(v), [BufferSlice(), _databuffer(v)]; nullcount=0)
    elseif T == Bool
        return fromjulia(name, convert(Vector{Union{Bool,Missing}}, v))
    elseif T == String
        return _build_strings(name, v)
    elseif T <: Union{Missing,Int8,Int16,Int32,Int64,UInt8,UInt16,UInt32,UInt64,Float16,Float32,Float64,Bool}
        return _build_nullable_primitive(name, v)
    elseif T <: Union{Missing,String}
        return _build_strings(name, v)
    elseif T <: AbstractVector || T <: Union{Missing,<:AbstractVector}
        return _build_list(name, v)
    else
        throw(ArgumentError("fromjulia: unsupported element type $T (prove-out scope)"))
    end
end

function _build_nullable_primitive(name, v::Vector{T}) where {T}
    S = Base.nonmissingtype(T)
    t = arrowtype_for(S)
    present = [x !== missing for x in v]
    validity = _bitmapbuffer(present)
    if S == Bool
        bytes = zeros(UInt8, expected_validity_bytes(Int64(length(v))))
        for (i, x) in enumerate(v)
            (x === missing || !x) && continue
            bytes[1 + ((i - 1) >> 3)] |= UInt8(1) << ((i - 1) & 7)
        end
        data = BufferSlice(heapregion(bytes), 0, length(bytes))
    else
        vals = S[x === missing ? zero(S) : S(x) for x in v]
        data = _databuffer(vals)
    end
    nc = count(!, present)
    return Field(name, t; nullable=nc > 0),
    ArrayData(t, length(v), [validity, data]; nullcount=nc)
end

function _build_strings(name, v::Vector)
    t = Utf8Type(false)
    present = [x !== missing for x in v]
    offsets = Vector{Int32}(undef, length(v) + 1)
    offsets[1] = 0
    nbytes = 0
    for (i, x) in enumerate(v)
        nbytes += x === missing ? 0 : ncodeunits(x)
        offsets[i + 1] = Int32(nbytes)
    end
    bytes = Vector{UInt8}(undef, nbytes)
    pos = 1
    for x in v
        x === missing && continue
        n = ncodeunits(x)
        copyto!(bytes, pos, codeunits(x), 1, n)
        pos += n
    end
    nc = count(!, present)
    data = nbytes == 0 ? BufferSlice() : BufferSlice(heapregion(bytes), 0, nbytes)
    return Field(name, t; nullable=nc > 0),
    ArrayData(t, length(v), [_bitmapbuffer(present), _databuffer(offsets), data];
        nullcount=nc)
end

function _build_list(name, v::Vector)
    present = [x !== missing for x in v]
    offsets = Vector{Int32}(undef, length(v) + 1)
    offsets[1] = 0
    total = 0
    for (i, x) in enumerate(v)
        total += x === missing ? 0 : length(x)
        offsets[i + 1] = Int32(total)
    end
    nonmissing = [x for x in v if x !== missing]
    childtype = eltype(Base.nonmissingtype(eltype(v)))
    flat = isempty(nonmissing) ? Vector{childtype}() : reduce(vcat, nonmissing)
    cf, cd = fromjulia("item", collect(flat))
    nc = count(!, present)
    t = ListType(false)
    return Field(name, t; nullable=nc > 0, children=[cf]),
    ArrayData(t, length(v), [_bitmapbuffer(present), _databuffer(offsets)];
        children=[cd], nullcount=nc)
end

"""
    fromjulia_struct(name, nt::NamedTuple) -> (Field, ArrayData)

Build a struct column from equal-length child vectors (no top-level nulls in
the prove-out builder).
"""
function fromjulia_struct(name, nt::NamedTuple)
    pairs = [fromjulia(String(k), v) for (k, v) in Base.pairs(nt)]
    n = length(first(values(nt)))
    all(length(v) == n for v in values(nt)) ||
        throw(ArgumentError("struct children must have equal lengths"))
    t = StructType()
    return Field(name, t; nullable=false, children=[p[1] for p in pairs]),
    ArrayData(t, n, [BufferSlice()]; children=[p[2] for p in pairs], nullcount=0)
end

"""
    fromjulia_dict(name, values, indices0) -> (Field, ArrayData)

Build a dictionary-encoded column from a value pool and 0-based Int32
indices (`missing` for null slots).
"""
function fromjulia_dict(name, pool::Vector, indices0::Vector)
    vf, vd = fromjulia(name, pool)
    t = DictionaryType(IntType(32, true), vf.type, false)
    present = [x !== missing for x in indices0]
    inds = Int32[x === missing ? Int32(0) : Int32(x) for x in indices0]
    nc = count(!, present)
    return Field(name, t; nullable=nc > 0, children=vf.children),
    ArrayData(t, length(indices0), [_bitmapbuffer(present), _databuffer(inds)];
        dictionary=vd, nullcount=nc)
end

# ---------------------------------------------------------------------------
# §8 RecordBatch + source protocol
# ---------------------------------------------------------------------------

"""
    RecordBatch

Schema + equal-length columns: the intended interchange unit in report §9.
The implemented IPC and C-stream adapters use batches. Future partition
adapters can use the same boundary; chunked columns remain a facade convenience.
"""
struct RecordBatch
    schema::Schema
    columns::FrozenVector{ArrayData}
    nrows::Int64
    function RecordBatch(schema::Schema, columns, nrows::Integer,
        validated_dictionaries::Union{Nothing,_ValidatedDictionaries}=nothing)
        _validate_schema(schema)
        cols = FrozenVector{ArrayData}(columns)
        n = Int64(nrows)
        n >= 0 || throw(ArgumentError("negative row count"))
        for (f, c) in zip(schema.fields, cols)
            length(c) == n || throw(ArgumentError("unequal column lengths"))
            _validate_structural(f, c, validated_dictionaries)
        end
        length(schema.fields) == length(cols) ||
            throw(ArgumentError("schema/column count mismatch"))
        return new(schema, cols, n)
    end
end
RecordBatch(schema::Schema, columns) =
    RecordBatch(schema, columns, isempty(columns) ? 0 : length(first(columns)))

"Build a batch from a NamedTuple of Julia vectors (test/example convenience)."
function batch(nt::NamedTuple)
    pairs = [fromjulia(String(k), v) for (k, v) in Base.pairs(nt)]
    sch = Schema([p[1] for p in pairs])
    b = RecordBatch(sch, [p[2] for p in pairs])
    for (f, c) in zip(sch.fields, b.columns)
        validate_structural(f, c)
    end
    return b
end

"""
    RecordBatchSource

The shared pull-iteration protocol (report §9): implement
`nextbatch!(src) -> Union{Nothing,RecordBatch}` and `schema(src)`. The IPC
reader and C-stream importer present this shape. A future facade can use the
same shape so that a dataset layer or writer need not know which adapter
produced the stream.
"""
abstract type RecordBatchSource end
function nextbatch! end
schema(src::RecordBatchSource) =
    error("RecordBatchSource implementations must define schema(src)")

end # module ArrowCore
