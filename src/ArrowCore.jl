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

The runtime-tagged, C-data-shaped core of Arrow.jl. It depends only on Base
and the Mmap standard library; the IPC, C-data, scan, and facade layers in
`src/` sit on top of it.

Design rules:

1. One physical data model. `ArrayData` = layout + buffers + children +
   dictionary, mirroring the Arrow C data interface's `ArrowArray`. Logical
   type parameters such as timezone and precision/scale are fields on
   `ArrowType` descriptors. Names and nullability are fields on `Field`.
   None parameterize the Core storage types. Struct materialization returns
   `Vector{Pair{String,Any}}` on the dynamic path; static element types are
   the caller's claim through `getvalue(::Type{T}, ...)`/`materialize(::Type{T}, ...)`.

2. Memory validity is GC reachability, plus one revocation bit. Every
   buffer is a `BufferSlice` into an `OwnerRegion` — a (pointer, length,
   alignment, root, cell) record whose `root` anchors the backing storage
   and whose `ReleaseCell` supports `release!`: regions sharing one
   underlying lifetime share one cell, so a release revokes every sibling and
   runs the release action (mmap unmap, foreign release callback) exactly
   once, and later access is a clean error. Slices are bounds-checked
   against the region at construction; loads are a final bounds check, one
   monotonic closed-flag load, and the raw read. Foreign extents are
   trusted declarations, and mapped files are exposed to external changes.

3. One structural layout registry. `layoutspec(type)` returns the buffer
   roles / child arity / offset width for each of the format-1.5 layouts.
   Generic code (buffer walking, structural validation, the IPC adapter's
   node/buffer accounting in src/ipc_read.jl) is driven by the registry;
   per-layout SEMANTICS (element access, semantic validation) are ordinary
   methods grouped per layout below. Adding a layout means one registry
   entry plus bounded method groups in the layers that support it.

4. Validation is staged: structural checks are O(buffers) and run at
   construction/adaptation time. Data-intrinsic semantic checks are O(n)
   when an adapter or caller requests them; a successful result is cached.
   Benign concurrent callers may repeat the same scan. Field-dependent
   dictionary contracts run on every validation call. Advisory contracts —
   Field.nullable enforcement, Date64 day divisibility, time-of-day range,
   decimal precision, and body UTF-8 — are opt-in via `validate_full`: the
   ecosystem's gold files violate them and the reference implementation
   reads those files. Framing-stage checks (checked spans, metadata
   verification, resource limits before metadata-directed allocation)
   belong to the adapters.

The registry, staged validation, element access, and materialization cover
every format-1.5 layout, including binary views, list views, and run-end
encoding. `validate_full` additionally enforces canonical bit-packed form
(zeroed trailing bits and padding); on-wire buffer padding is a writer
guarantee, not a reader requirement — the spec permits unpadded buffers and
this reader accepts them. Core has no codec dependency; the IPC adapter
implements compression. Bulk access is `materialize` (dynamic, one function
barrier per layout) and `materialize(::Type{T}, …)` (a static element claim,
resolved without boxing except at the dictionary/run-end wrapper edge).
"""
module ArrowCore

using Base: Checked
import Mmap
const checked_add = Checked.checked_add
const checked_sub = Checked.checked_sub
const checked_mul = Checked.checked_mul

export OwnerRegion,
    BufferSlice,
    heapregion,
    mmapregion,
    release!,
    ReleaseCell,
    ReleaseCounter,
    increment!,
    ArrowType,
    NullType,
    BoolType,
    IntType,
    FloatType,
    DecimalType,
    FixedSizeBinaryType,
    BinaryType,
    Utf8Type,
    DateType,
    TimeType,
    TimestampType,
    DurationType,
    IntervalType,
    ListType,
    FixedSizeListType,
    StructType,
    MapType,
    UnionType,
    DictionaryType,
    ViewType,
    ListViewType,
    RunEndEncodedType,
    Field,
    Schema,
    ArrayData,
    RecordBatch,
    RecordBatchSource,
    nextbatch!,
    LayoutSpec,
    layoutspec,
    BufferRole,
    validate_structural,
    validate_semantic,
    validate_full,
    ValidationError,
    nullcount,
    getvalue,
    materialize,
    fromjulia,
    fromviewentries,
    batch

# ---------------------------------------------------------------------------
# §1 Memory: regions as GC anchors (constrained model)
# ---------------------------------------------------------------------------
#
# Buffer validity is GC REACHABILITY — Julia's native memory-safety
# contract — plus one explicit revocation layer. A region's `root` is
# whatever keeps the memory alive (the wrapped Julia array, the Mmap-stdlib
# array, a C-data adapter's owner object); views hold their region, the
# region holds its root, so memory a view can reach is memory that is
# valid. `release!` is the deterministic release path on top: one
# `ReleaseCell` per underlying lifetime revokes every region over it and
# runs the eager release action exactly once (unmap now; run the foreign
# release now), turning use-after-release into `InvalidStateException`
# instead of undefined behavior. What stays out of scope, so the contract
# is informed:
#   * Data-race shielding for loads concurrent WITH release!: quiescing
#     readers first is the caller's contract, as with `Base.close` on a
#     shared IO. Loads take no locks.
#   * External-truncation protection: a shared mapping's pages can vanish
#     under any implementation. Same exposure as every mmap-based reader.

"An atomic counter for the test suite's exactly-once release bookkeeping."
mutable struct ReleaseCounter
    @atomic n::Int
end
ReleaseCounter() = ReleaseCounter(0)
Base.getindex(c::ReleaseCounter) = @atomic c.n
# CAS loop rather than `@atomic c.n += 1`: the atomic read-modify-write
# builtin is not implemented in JuliaC's trim verifier, while
# compare-and-swap is; contention here is negligible.
function increment!(c::ReleaseCounter)
    while true
        old = @atomic c.n
        _, ok = @atomicreplace c.n old => old + 1
        ok && return old + 1
    end
end

"""
    ReleaseCell(action::Ptr{Cvoid}, arg)
    ReleaseCell()

The revocation state one release action guards. Every `OwnerRegion` carries
a cell; regions that share one underlying lifetime (all buffers imported
from one C-data tree) share ONE cell, so closing any of them revokes every
sibling before the single release action runs. The action is a
`@cfunction(f, Cvoid, (Ptr{Cvoid},))` trampoline receiving
`pointer_from_objref(arg)` — pure C ABI, the same idiom the C-data
adapter's release callbacks use and the form the trim verifier accepts
(an `Any`-argument cfunction is not) — or `C_NULL` when the backing
storage is a borrow with no eager action (a heap vector — running a
borrowed object's finalizers is not ours to do). `arg` must be a mutable
heap object; the cell's reference keeps it alive across the call. Build
trampolines at runtime, never in a module-level const (a serialized
cfunction pointer is garbage after precompile reload).
"""
mutable struct ReleaseCell
    @atomic closed::Bool
    const action::Ptr{Cvoid}
    const arg::Any
end
ReleaseCell(action::Ptr{Cvoid}, arg) = ReleaseCell(false, action, arg)
ReleaseCell() = ReleaseCell(false, Ptr{Cvoid}(C_NULL), nothing)

"""
    release!(cell::ReleaseCell)

Revoke every region sharing the cell — later raw access throws
`InvalidStateException` — and run the cell's release action exactly once.
Idempotent. Not a data-race shield for accesses concurrent WITH the release;
quiescing readers first is the caller's contract, as with `Base.close` on
a shared IO.
"""
function release!(cell::ReleaseCell)
    (@atomicswap :acquire_release cell.closed = true) && return nothing
    if cell.action != C_NULL
        arg = cell.arg
        GC.@preserve arg ccall(cell.action, Cvoid, (Ptr{Cvoid},), pointer_from_objref(arg))
    end
    return nothing
end

"""
    OwnerRegion

One contiguous memory region, the object that keeps it alive, and the
[`ReleaseCell`](@ref) that can revoke it. The region is valid while it is
reachable — `root` anchors the backing storage (a borrowed Julia array, the
Mmap-stdlib array, or an adapter's owner object) — or until `release!` runs
its cell's release action, after which every raw access through `sliceptr`
throws. Slices reject geometry outside the declared `len` at construction;
loads retain a final bounds check before the raw read.

The scoped-borrow contract for wrapped Julia arrays: the caller must not
mutate or resize the array while the region or any cached validation result
remains in use. Mutation can invalidate a semantic certificate; resizing can
reallocate the storage and invalidate its pointer.
"""
struct OwnerRegion
    ptr::Ptr{UInt8}
    len::Int64
    alignment::Base.Int   # guaranteed ptr alignment, capped at 64; loads consult it
    root::Any             # GC anchor; never dispatched on, only stored
    cell::ReleaseCell

    function OwnerRegion(
        ptr::Ptr{UInt8},
        len::Integer;
        root=nothing,
        cell::ReleaseCell=ReleaseCell(),
    )
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
        # OR-ing in 64 caps the detected alignment at 64 bytes without a
        # branch; a NULL (necessarily empty) region reports the cap, since
        # nothing loads from it.
        align = ptr == C_NULL ? 64 : (1 << trailing_zeros(UInt(ptr) | UInt(64)))
        return new(ptr, n, align, root, cell)
    end
end

"""
    release!(r::OwnerRegion)

Deterministically release the region's backing storage through its
`ReleaseCell`: every region sharing the cell is revoked (later raw
access throws `InvalidStateException`) and the cell's release action runs
exactly once — an mmap region unmaps NOW (the eager path exists for hosts
where a GC-timed unmap is not enough, deleting a still-mapped file on
Windows being the canonical case); an imported C-data tree runs the
producer's release callback; a borrowed heap region is revoked with no
eager action. Idempotent.
"""
function release!(r::OwnerRegion)
    return release!(r.cell)
end

"""
    heapregion(v::Vector{T}) -> OwnerRegion

Borrow a Julia array as a region (zero-copy). The array is the `root`.
"""
function heapregion(v::Vector{T}) where {T}
    isbitstype(T) || throw(ArgumentError("heapregion requires an isbits element type"))
    return OwnerRegion(Ptr{UInt8}(pointer(v)), sizeof(v); root=v)
end

# `_mmaproot(arr)`: the object Mmap registered its unmap finalizer on — the
# array's backing `Memory` from Julia 1.11 (`finalize(arr)` is then a
# no-op), the array itself before. The release cell targets that object so
# `release!` truly unmaps now.
@static if VERSION >= v"1.11"
    _mmaproot(arr::Vector{UInt8}) = arr.ref.mem
    function _release_mmap(p::Ptr{Cvoid})::Cvoid
        finalize(unsafe_pointer_to_objref(p)::Memory{UInt8})
        return nothing
    end
else
    _mmaproot(arr::Vector{UInt8}) = arr
    function _release_mmap(p::Ptr{Cvoid})::Cvoid
        finalize(unsafe_pointer_to_objref(p)::Vector{UInt8})
        return nothing
    end
end

# Map from a caller-owned open `io` (the caller closes it; the mapping
# outlives the descriptor); `label` names the mapping in errors.
function _mmapregion(io::IO, label)
    arr = Mmap.mmap(io, Vector{UInt8})
    isempty(arr) && throw(ArgumentError("cannot map empty file: $label"))
    cell = ReleaseCell(@cfunction(_release_mmap, Cvoid, (Ptr{Cvoid},)), _mmaproot(arr))
    return OwnerRegion(Ptr{UInt8}(pointer(arr)), length(arr); root=arr, cell=cell)
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
    try
        # The mapping outlives the descriptor.
        return _mmapregion(io, path)
    finally
        close(io)
    end
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
        checked_add(Int64(offset), Int64(len)) <= region.len || throw(
            ArgumentError(
                "buffer [offset=$offset len=$len] exceeds region of $(region.len) bytes",
            ),
        )
        return new(region, Int64(offset), Int64(len))
    end
    BufferSlice() = new(nothing, 0, 0)
end

Base.length(b::BufferSlice) = b.len
isempty_buffer(b::BufferSlice) = b.len == 0
@inline function sliceptr(b::BufferSlice)
    b.region === nothing && return Ptr{UInt8}(0)
    r = b.region::OwnerRegion
    (@atomic :monotonic r.cell.closed) &&
        throw(InvalidStateException("the backing region was released", :closed))
    return r.ptr + b.offset
end

"Sub-slice with checked arithmetic (relative bounds against the parent slice)."
function subslice(b::BufferSlice, offset::Integer, len::Integer)
    offset >= 0 || throw(ArgumentError("negative subslice offset"))
    len >= 0 || throw(ArgumentError("negative subslice length"))
    b.region === nothing && (len == 0 && offset == 0) && return b
    b.region === nothing && throw(ArgumentError("cannot subslice the empty buffer"))
    checked_add(Int64(offset), Int64(len)) <= b.len || throw(
        ArgumentError("subslice [offset=$offset len=$len] exceeds slice of $(b.len) bytes"),
    )
    return BufferSlice(b.region, checked_add(b.offset, Int64(offset)), Int64(len))
end

"""
Load a `T` at byte offset `byteoff` (0-based) within the slice. Handles the
misaligned case with a byte-wise load: alignment is a property of the region
(Arrow controls only its own allocations; mmap and foreign pointers can be
anything), so the branch lives here, in one place, instead of as a copy
workaround scattered through per-type code.
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

"Copy the slice into a fresh `Vector{UInt8}`."
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
immutable structs whose *fields* carry the logical type parameters
(timestamp unit and timezone, decimal precision/scale/width, ...). Two
timestamp columns with different timezones have the SAME Julia type —
schema diversity costs data, not method instances.
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

"Null: every slot null; no buffers."
struct NullType <: ArrowType end
"Boolean: bit-packed values."
struct BoolType <: ArrowType end
"Integer of `bits` width, `signed` or not."
struct IntType <: ArrowType
    bits::Int      # 8/16/32/64 — the spec's Int; wider is NOT valid
    signed::Bool
end
"IEEE floating point of `bits` width (16/32/64)."
struct FloatType <: ArrowType
    bits::Int      # 16/32/64
end
"Decimal of `precision` digits and `scale`, stored in `bits` (32/64/128/256)."
struct DecimalType <: ArrowType
    precision::Int
    scale::Int
    bits::Int      # 32/64/128/256 (format 1.5)
end
"Fixed-width binary of `nbytes` per slot."
struct FixedSizeBinaryType <: ArrowType
    nbytes::Int
end
"Variable-length binary; `large` selects Int64 offsets."
struct BinaryType <: ArrowType
    large::Bool    # Int64 offsets when true
end
"UTF-8 string; `large` selects Int64 offsets."
struct Utf8Type <: ArrowType
    large::Bool
end
"Date in days (Int32) or milliseconds (Int64) since the epoch."
struct DateType <: ArrowType
    unit::DateUnit # DAY => Int32 storage, MILLISECOND => Int64
end
"Time of day in `unit`, stored in `bits` (32 for s/ms, 64 for us/ns)."
struct TimeType <: ArrowType
    unit::TimeUnit
    bits::Int      # 32 (s/ms) or 64 (us/ns)
end
"Timestamp in `unit` since the epoch, with an optional `timezone`."
struct TimestampType <: ArrowType
    unit::TimeUnit
    timezone::Union{Nothing,String}   # a VALUE — one method instance total
end
"Elapsed time in `unit`, Int64 storage."
struct DurationType <: ArrowType
    unit::TimeUnit
end
"Calendar interval in `unit` (year-month, day-time, or month-day-nano)."
struct IntervalType <: ArrowType
    unit::IntervalUnit                # includes MONTH_DAY_NANO (format 1.2)
end
"Variable-length list; `large` selects Int64 offsets."
struct ListType <: ArrowType
    large::Bool
end
"List of exactly `listsize` child slots per parent slot."
struct FixedSizeListType <: ArrowType
    listsize::Int
end
"Struct: named children declared by `Field.children`."
struct StructType <: ArrowType end
"Map: a list of key/value struct entries; `keyssorted` per entry."
struct MapType <: ArrowType
    keyssorted::Bool
end
"Union in sparse or dense `mode` over the children's declared `typeids`."
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
"Utf8View / BinaryView (format 1.4): 16-byte entries plus variadic data buffers."
struct ViewType <: ArrowType
    utf8::Bool
end
"ListView / LargeListView (format 1.4): per-slot child offsets and sizes."
struct ListViewType <: ArrowType
    large::Bool
end
"Run-end encoded (format 1.3): signed run ends and values of any Arrow type."
struct RunEndEncodedType <: ArrowType end

"""
    Field

One column/child descriptor: name, logical type, nullability, metadata, and
child fields. Dictionary columns are `DictionaryType` here; the IPC-level
dictionary *id* is NOT a Field concern — it is IPC bookkeeping and lives in
the adapter (Core dictionaries are object references; the id↔dictionary
table is the adapter's).
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
        String(first(kv)) => String(last(kv)) for kv in metadata
    )
end
_freezemetadata(metadata) =
    FrozenVector{Pair{String,String}}(String(k) => String(v) for (k, v) in pairs(metadata))
Field(name, type; nullable=true, metadata=nothing, children=()) = Field(
    String(name),
    type,
    Bool(nullable),
    _freezemetadata(metadata),
    FrozenVector{Field}(children),
)
# Narrower than the struct's implicit (Any...) convert constructor so this
# ADDS a positional-with-conversion method instead of overwriting it (which
# precompilation forbids); exact-typed calls still take the implicit one.
Field(name::AbstractString, type::ArrowType, nullable, metadata, children) =
    Field(name, type; nullable=nullable, metadata=metadata, children=children)

"""
    Schema

An ordered set of top-level `Field`s plus optional schema-level metadata:
the shape of every record batch that carries it.
"""
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
"The role of one buffer in a layout's buffer sequence (see `LayoutSpec`)."
@enum BufferRole::UInt8 VALIDITY DATA OFFSETS ELEMENT_OFFSETS SIZES VIEWS TYPE_IDS

"""
    LayoutSpec

The STRUCTURAL facts for one physical layout: which buffers it has (in
order), how many children, its offset width, whether the trailing data
buffers are variadic (view layouts). This is everything generic code needs
to walk a layout — and nothing more. Semantics (what the bytes mean, how to
access element `i`) are per-layout methods, not registry rows.

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
# Narrower than the implicit convert constructor (see Field above).
LayoutSpec(buffers::AbstractVector, childcount, offsetwidth, fixedwidth, variadic) =
    LayoutSpec(
        FrozenVector{BufferRole}(buffers),
        childcount,
        offsetwidth,
        fixedwidth,
        variadic,
    )

primwidth(t::IntType) = t.bits ÷ 8
primwidth(t::FloatType) = t.bits ÷ 8
primwidth(t::DecimalType) = t.bits ÷ 8
primwidth(t::DateType) = t.unit == DAY ? 4 : 8
primwidth(t::TimeType) = t.bits ÷ 8
primwidth(::TimestampType) = 8
primwidth(::DurationType) = 8
primwidth(t::IntervalType) = t.unit == YEAR_MONTH ? 4 : t.unit == DAY_TIME ? 8 : 16
primwidth(t::FixedSizeBinaryType) = t.nbytes

# The buffer-role sequences are shared constants: `layoutspec` runs on
# every buffer-by-role lookup, so a per-call vector would allocate in the
# element accessors' inner loops.
const NO_BUFFERS = FrozenVector{BufferRole}(())
const VALIDITY_ONLY = FrozenVector{BufferRole}((VALIDITY,))
const VALIDITY_DATA = FrozenVector{BufferRole}((VALIDITY, DATA))
const VALIDITY_OFFSETS = FrozenVector{BufferRole}((VALIDITY, OFFSETS))
const VALIDITY_OFFSETS_DATA = FrozenVector{BufferRole}((VALIDITY, OFFSETS, DATA))
const VALIDITY_VIEWS = FrozenVector{BufferRole}((VALIDITY, VIEWS))
const VALIDITY_ELEMENT_OFFSETS_SIZES =
    FrozenVector{BufferRole}((VALIDITY, ELEMENT_OFFSETS, SIZES))
const TYPE_IDS_ONLY = FrozenVector{BufferRole}((TYPE_IDS,))
const TYPE_IDS_ELEMENT_OFFSETS = FrozenVector{BufferRole}((TYPE_IDS, ELEMENT_OFFSETS))

"""
    layoutspec(t::ArrowType) -> LayoutSpec

The structural facts of `t`'s physical layout: one method per descriptor
type (the registry's extension point); generic code reaches it through the
closed-set ladder `layoutspec_of`.
"""
layoutspec(::NullType) = LayoutSpec(NO_BUFFERS, 0, 0, 0, false)
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
layoutspec(t::BinaryType) = LayoutSpec(VALIDITY_OFFSETS_DATA, 0, t.large ? 8 : 4, 0, false)
layoutspec(t::Utf8Type) = LayoutSpec(VALIDITY_OFFSETS_DATA, 0, t.large ? 8 : 4, 0, false)
layoutspec(t::ListType) = LayoutSpec(VALIDITY_OFFSETS, 1, t.large ? 8 : 4, 0, false)
layoutspec(::FixedSizeListType) = LayoutSpec(VALIDITY_ONLY, 1, 0, 0, false)
layoutspec(::StructType) = LayoutSpec(VALIDITY_ONLY, -1, 0, 0, false)
layoutspec(::MapType) = LayoutSpec(VALIDITY_OFFSETS, 1, 4, 0, false)
layoutspec(t::UnionType) =
    t.mode == SparseMode ? LayoutSpec(TYPE_IDS_ONLY, -1, 0, 0, false) :
    LayoutSpec(TYPE_IDS_ELEMENT_OFFSETS, -1, 4, 0, false)
layoutspec(t::DictionaryType) =
    LayoutSpec(VALIDITY_DATA, 0, 0, primwidth(t.indextype), false)
layoutspec(::ViewType) = LayoutSpec(VALIDITY_VIEWS, 0, 0, 16, true)
# ListView has one offset and one size per parent slot. These are not
# the length+1 monotone range offsets used by List/Utf8/Binary.
layoutspec(t::ListViewType) =
    LayoutSpec(VALIDITY_ELEMENT_OFFSETS_SIZES, 1, t.large ? 8 : 4, 0, false)
# REE: no top-level validity; run_ends and values are CHILDREN, not buffers.
layoutspec(::RunEndEncodedType) = LayoutSpec(NO_BUFFERS, 2, 0, 0, false)

"""
    layoutspec_of(t::ArrowType) -> LayoutSpec

The closed-set dispatch ladder over the runtime descriptors. This is the
trim-compile story for a runtime-tagged core: dispatch
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

# Junk descriptors get a clean error instead of a MethodError wherever the
# raw method table is called directly.
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

function ArrayData(
    type::ArrowType,
    len::Integer,
    buffers;
    offset::Integer=0,
    children=(),
    dictionary::Union{Nothing,ArrayData}=nothing,
    owner=nothing,
    nullcount::Integer=-1,
)
    return _arraydata(type, len, buffers, offset, children, dictionary, owner, nullcount)
end

# Positional twin of the keyword constructor: Julia's kwcall machinery does
# not statically resolve over an abstract-typed leading argument, so
# trim-verified adapters (the C-data import walk) construct through this
# single generic method instead.
function _arraydata(
    @nospecialize(type::ArrowType),
    len::Integer,
    buffers,
    offset::Integer,
    children,
    dictionary::Union{Nothing,ArrayData},
    owner,
    nullcount::Integer,
)
    len >= 0 || throw(ArgumentError("negative array length"))
    offset >= 0 || throw(ArgumentError("negative array offset"))
    -1 <= nullcount <= len ||
        throw(ArgumentError("null count must be -1 or in [0, length]"))
    return ArrayData(
        type,
        Int64(len),
        Int64(offset),
        FrozenVector{BufferSlice}(buffers),
        FrozenVector{ArrayData}(children),
        dictionary,
        owner,
        Int64(nullcount),
        false,
    )
end

Base.length(d::ArrayData) = d.len

# Adapter-private certificate set. An entry means that one immutable
# dictionary pool snapshot already passed structural, intrinsic semantic, and
# Field-contract validation under the adapter's canonical value Field.
const _ValidatedDictionaries = IdDict{ArrayData,Nothing}
@inline _dictionary_validated(::Nothing, ::ArrayData) = false
@inline _dictionary_validated(memo::_ValidatedDictionaries, d::ArrayData) = haskey(memo, d)

@inline _slotindex0(d::ArrayData, i::Int64) =
    checked_add(d.offset, checked_sub(i, Int64(1)))
@inline _slotbyteoff(d::ArrayData, i::Int64, width::Integer) =
    checked_mul(_slotindex0(d, i), Int64(width))

# Buffer-by-role lookup, driven by the registry. Structural validation
# guarantees position/arity, so adapters and accessors never hand-count.
function rolebuffer(d::ArrayData, role::BufferRole)
    spec = layoutspec_of(d.type)
    idx = findfirst(==(role), spec.buffers)
    idx === nothing &&
        throw(ArgumentError("layout $(descriptorname(d.type)) has no $role buffer"))
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
    for i = 1:(d.len)
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
    # `len + 7` can overflow a hostile logical extent; the requirement is
    # then unsatisfiable, which is a validation failure, not a raw
    # `OverflowError`.
    len <= typemax(Int64) - 7 ||
        throw(ValidationError("required bitmap bytes overflow Int64 for $len slots"))
    return (len + 7) >> 3
end

# Structural requirements multiply untrusted logical extents. When the
# product overflows Int64 no real buffer or child can satisfy it, so it
# surfaces as the same ValidationError an undersized one gets, not as a
# raw `OverflowError`. Both operands are non-negative at every call site.
@inline function _required_extent(count::Int64, width::Int64, what::AbstractString)
    count == 0 ||
        width <= div(typemax(Int64), count) ||
        throw(ValidationError("required $what overflows Int64 ($count × $width)"))
    return count * width
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
    a isa TimestampType &&
        return b isa TimestampType && a.unit == b.unit && a.timezone == b.timezone
    a isa DateType && return b isa DateType && a.unit == b.unit
    a isa TimeType && return b isa TimeType && a.unit == b.unit && a.bits == b.bits
    a isa DurationType && return b isa DurationType && a.unit == b.unit
    a isa BinaryType && return b isa BinaryType && a.large == b.large
    a isa FixedSizeBinaryType && return b isa FixedSizeBinaryType && a.nbytes == b.nbytes
    a isa FixedSizeListType && return b isa FixedSizeListType && a.listsize == b.listsize
    a isa MapType && return b isa MapType && a.keyssorted == b.keyssorted
    a isa UnionType && return b isa UnionType && a.mode == b.mode && a.typeids == b.typeids
    a isa DecimalType && return b isa DecimalType &&
           a.precision == b.precision &&
           a.scale == b.scale &&
           a.bits == b.bits
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

_validate_descriptor(::Utf8Type) = nothing
_validate_descriptor(::BoolType) = nothing
_validate_descriptor(::ListType) = nothing
_validate_descriptor(::StructType) = nothing
_validate_descriptor(::BinaryType) = nothing
_validate_descriptor(::MapType) = nothing
_validate_descriptor(::NullType) = nothing
_validate_descriptor(::ViewType) = nothing
_validate_descriptor(::ListViewType) = nothing
_validate_descriptor(::RunEndEncodedType) = nothing
_validate_descriptor(::Any) = throw(ArgumentError("unregistered ArrowType"))

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
_validate_descriptor(t::IntType) =
    t.bits in (8, 16, 32, 64) ||
    throw(ValidationError("integer bit width must be 8, 16, 32, or 64"))
_validate_descriptor(t::FloatType) =
    t.bits in (16, 32, 64) ||
    throw(ValidationError("floating-point bit width must be 16, 32, or 64"))
function _validate_descriptor(t::DecimalType)
    maxprecision =
        t.bits == 32 ? 9 : t.bits == 64 ? 18 : t.bits == 128 ? 38 : t.bits == 256 ? 76 : 0
    maxprecision != 0 ||
        throw(ValidationError("decimal bit width must be 32, 64, 128, or 256"))
    1 <= t.precision <= maxprecision || throw(
        ValidationError(
            "decimal precision $(t.precision) is invalid for $(t.bits)-bit storage",
        ),
    )
    typemin(Int32) <= t.scale <= typemax(Int32) || throw(
        ValidationError("decimal scale $(t.scale) does not fit the Arrow Int32 wire field"),
    )
    return nothing
end
_validate_descriptor(t::FixedSizeBinaryType) =
    0 <= t.nbytes <= typemax(Int32) ||
    throw(ValidationError("fixed-size-binary width must be in [0, $(typemax(Int32))]"))
_validate_descriptor(t::DateType) =
    t.unit in (DAY, MILLISECOND_DATE) ||
    throw(ValidationError("invalid Arrow date unit $(repr(t.unit))"))
function _validate_descriptor(t::TimeType)
    t.unit in (SECOND, MILLISECOND, MICROSECOND, NANOSECOND) ||
        throw(ValidationError("invalid Arrow time unit $(repr(t.unit))"))
    valid = t.unit in (SECOND, MILLISECOND) ? t.bits == 32 : t.bits == 64
    valid || throw(
        ValidationError("time unit $(t.unit) is incompatible with $(t.bits)-bit storage"),
    )
    return nothing
end
function _validate_descriptor(t::TimestampType)
    t.unit in (SECOND, MILLISECOND, MICROSECOND, NANOSECOND) ||
        throw(ValidationError("invalid Arrow timestamp unit $(repr(t.unit))"))
    (t.timezone === nothing || isvalid(t.timezone)) ||
        throw(ValidationError("timestamp timezone is not valid UTF-8"))
    return nothing
end
_validate_descriptor(t::DurationType) =
    t.unit in (SECOND, MILLISECOND, MICROSECOND, NANOSECOND) ||
    throw(ValidationError("invalid Arrow duration unit $(repr(t.unit))"))
_validate_descriptor(t::IntervalType) =
    t.unit in (YEAR_MONTH, DAY_TIME, MONTH_DAY_NANO) ||
    throw(ValidationError("invalid Arrow interval unit $(repr(t.unit))"))
_validate_descriptor(t::FixedSizeListType) =
    0 <= t.listsize <= typemax(Int32) ||
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
    s.endianness == _native_endianness() || throw(
        ValidationError(
            "non-native Arrow schema endianness must be normalized before Core access",
        ),
    )
    _validate_metadata(s.metadata, "schema")
    return s
end

"""
    validate_structural(field, data)

Stage-2 validation: O(buffers), registry-driven, run at
construction/adaptation time. Checks buffer arity against the layout, and
every buffer's byte length against what the logical length requires — with
checked arithmetic, because these lengths come from untrusted metadata.
Recurses into children and the dictionary.

BufferSlice construction has already bounded every slice inside its region,
so this stage never touches memory — it is pure arithmetic on declared
sizes. (The framing stage — resource limits before metadata-directed decode
allocation and checked message-body spans — belongs to the adapters; see
src/ipc_read.jl.)
"""
validate_structural(f::Field, d::ArrayData) = _validate_structural(f, d, nothing)

function _validate_structural(
    f::Field,
    d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries},
)
    isvalid(f.name) || throw(ValidationError("field name is not valid UTF-8"))
    _validate_metadata(f.metadata, "field")
    typeequal(f.type, d.type) || throw(
        ValidationError(
            "field/type mismatch: $(descriptorname(f.type)) vs $(descriptorname(d.type))",
        ),
    )
    _validate_descriptor_of(d.type)
    spec = layoutspec_of(d.type)
    nfixed = length(spec.buffers)
    buffers_ok = spec.variadic ? length(d.buffers) >= nfixed : length(d.buffers) == nfixed
    buffers_ok || throw(
        ValidationError(
            "$(descriptorname(d.type)): expected $(spec.variadic ? "at least " : "")$nfixed buffers, got $(length(d.buffers))",
        ),
    )
    # `len` and `offset` are individually non-negative (constructor
    # invariant), but hostile metadata can still declare a sum past Int64 —
    # surface that as ValidationError, not a raw OverflowError.
    d.len <= typemax(Int64) - d.offset || throw(
        ValidationError("array length $(d.len) plus offset $(d.offset) overflows Int64"),
    )
    total::Int64 = d.len + d.offset
    declared_nulls = @atomic :monotonic d.nullcount
    for (i, role) in enumerate(spec.buffers)
        b = d.buffers[i]
        if role == VALIDITY
            if isempty_buffer(b)
                declared_nulls > 0 && throw(
                    ValidationError("absent validity bitmap with positive null count"),
                )
                continue
            end
            b.len >= expected_validity_bytes(total) || throw(
                ValidationError(
                    "validity bitmap too small: $(b.len) bytes for $total slots",
                ),
            )
        elseif role == DATA
            if spec.fixedwidth > 0
                need = _required_extent(total, Int64(spec.fixedwidth), "data buffer bytes")
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
            # `total + 1` offset slots: the +1 itself can overflow a hostile
            # extent.
            total < typemax(Int64) ||
                throw(ValidationError("required offsets buffer bytes overflow Int64"))
            need = _required_extent(
                total + Int64(1),
                Int64(spec.offsetwidth),
                "offsets buffer bytes",
            )
            b.len >= need ||
                throw(ValidationError("offsets buffer too small: $(b.len) < $need bytes"))
        elseif role == ELEMENT_OFFSETS
            need = _required_extent(total, Int64(spec.offsetwidth), "element-offsets bytes")
            b.len >= need || throw(
                ValidationError("element-offsets buffer too small: $(b.len) < $need bytes"),
            )
        elseif role == SIZES
            need = _required_extent(total, Int64(spec.offsetwidth), "sizes buffer bytes")
            b.len >= need || throw(ValidationError("sizes buffer too small"))
        elseif role == TYPE_IDS
            b.len >= total || throw(ValidationError("type_ids buffer too small"))
        elseif role == VIEWS
            need = _required_extent(total, Int64(16), "views buffer bytes")
            b.len >= need || throw(ValidationError("views buffer too small"))
        end
    end
    # Child arity: registry-declared, or Field-declared for struct/union.
    expected_children = spec.childcount == -1 ? length(f.children) : spec.childcount
    if !(d.type isa DictionaryType)
        length(f.children) == expected_children || throw(
            ValidationError(
                "$(descriptorname(d.type)): expected $expected_children child fields, got $(length(f.children))",
            ),
        )
    end
    length(d.children) == expected_children || throw(
        ValidationError(
            "$(descriptorname(d.type)): expected $expected_children children, got $(length(d.children))",
        ),
    )
    for (cf, cd) in zip(childfields(f), d.children)
        _validate_structural(cf, cd, validated_dictionaries)
    end
    if d.type isa DictionaryType
        d.dictionary === nothing &&
            throw(ValidationError("dictionary-encoded array without a dictionary"))
        dictionary = d.dictionary::ArrayData
        _dictionary_validated(validated_dictionaries, dictionary) || _validate_structural(
            dictvaluefield(f, d.type),
            dictionary,
            validated_dictionaries,
        )
    elseif d.dictionary !== nothing
        throw(ValidationError("dictionary values attached to a non-dictionary array"))
    end
    fslt = d.type
    if fslt isa FixedSizeListType
        need = _required_extent(total, Int64(fslt.listsize), "fixed-size-list child length")
        length(d.children[1]) >= need || throw(
            ValidationError(
                "fixed-size-list child too short: $(length(d.children[1])) < $need",
            ),
        )
    end
    # Struct and sparse-union children are parent-length arrays indexed at
    # parent.offset + i (each child then applies its own offset), so every
    # child must cover offset+len slots.
    if d.type isa StructType || (d.type isa UnionType && d.type.mode == SparseMode)
        for (ci, child) in enumerate(d.children)
            length(child) >= total || throw(
                ValidationError(
                    "child $ci too short for parent extent: $(length(child)) < $total",
                ),
            )
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
        !runfield.nullable || throw(ValidationError("REE run ends must be non-nullable"))
        # The parent has no validity bitmap, so a positive count is
        # malformed; unknown (-1) is legal for any layout.
        declared_nulls <= 0 || throw(ValidationError("REE parent null count must be zero"))
        length(d.children[1]) == length(d.children[2]) ||
            throw(ValidationError("REE run-end and value child lengths must match"))
        d.len == 0 ||
            length(d.children[1]) > 0 ||
            throw(
                ValidationError("a nonempty REE array requires at least one physical run"),
            )
        maxrunend =
            runtype.bits == 16 ? Int64(typemax(Int16)) :
            runtype.bits == 32 ? Int64(typemax(Int32)) : typemax(Int64)
        total <= maxrunend || throw(
            ValidationError(
                "REE logical extent $total exceeds the $(runtype.bits)-bit run-end range",
            ),
        )
    end
    return d
end

# Child Fields for traversal. For list-ish layouts the child field is the
# Field's single declared child; dictionary values reuse the field with the
# value type.
childfields(f::Field) = f.children
# Dictionary values have their own nullability. The index field's
# nullable flag describes only the indices and cannot constrain the pool.
dictvaluefield(f::Field, t::DictionaryType) =
    Field(f.name, t.valuetype; nullable=true, children=f.children)

const MILLISECONDS_PER_DAY = Int64(86_400_000)

# Date64 whole-day divisibility and Decimal precision are ADVISORY in
# practice: the spec phrases Date64 as "evenly divisible by 86400000" and
# precision as "total number of decimal digits", but the reference C++
# implementation neither enforces them on read nor honors them on write — the
# apache/arrow-testing gold corpus itself carries Date64 values off day
# boundaries and decimal(3,2) values with five digits. Rejecting those in
# `validate_semantic` would make a conforming reader refuse canonical data,
# so both checks live in the opt-in `validate_full` tier
# (`_validate_full_content`), where strict callers can still demand them.
function _validate_advisory_values(t::DateType, d::ArrayData)
    t.unit == MILLISECOND_DATE || return nothing
    data = rolebuffer(d, DATA)
    for i = 1:(d.len)
        isvalid_at(d, i) || continue
        value = loadat(data, Int64, _slotbyteoff(d, Int64(i), 8))
        value % MILLISECONDS_PER_DAY == 0 ||
            throw(ValidationError("Date64 value $value is not a whole day in milliseconds"))
    end
    return nothing
end

function _decimal_limb(
    t::DecimalType,
    data::BufferSlice,
    byteoff::Int64,
    nlimbs::Int,
    limb::Int,
)::UInt64
    limb <= nlimbs || return UInt64(0)
    source_limb = _native_endianness() == LittleEndian ? limb : nlimbs - limb + 1
    base = checked_add(byteoff, Int64(8 * (source_limb - 1)))
    return t.bits == 32 ? UInt64(loadat(data, UInt32, base)) : loadat(data, UInt64, base)
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
    negative =
        ((nlimbs == 1 ? l1 : nlimbs == 2 ? l2 : nlimbs == 3 ? l3 : l4) & signbit) != 0
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
    for _ = 1:(t.precision)
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

function _validate_advisory_values(t::DecimalType, d::ArrayData)
    data = rolebuffer(d, DATA)
    width = Int64(primwidth(t))
    for i = 1:(d.len)
        isvalid_at(d, i) || continue
        byteoff = _slotbyteoff(d, Int64(i), width)
        _decimal_fits_precision(t, data, byteoff) || throw(
            ValidationError(
                "Decimal value at element $i does not fit precision $(t.precision)",
            ),
        )
    end
    return nothing
end

# Time-of-day range is advisory for the same reason as Date64 divisibility:
# the 1.0.0 gold corpus carries out-of-range Time32 values that C++ reads.
function _validate_advisory_values(t::TimeType, d::ArrayData)
    units_per_day =
        t.unit == SECOND ? Int64(86_400) :
        t.unit == MILLISECOND ? MILLISECONDS_PER_DAY :
        t.unit == MICROSECOND ? Int64(86_400_000_000) : Int64(86_400_000_000_000)
    data = rolebuffer(d, DATA)
    for i = 1:(d.len)
        isvalid_at(d, i) || continue
        value = if t.bits == 32
            Int64(loadat(data, Int32, _slotbyteoff(d, Int64(i), Int64(4))))
        else
            loadat(data, Int64, _slotbyteoff(d, Int64(i), Int64(8)))
        end
        0 <= value < units_per_day || throw(
            ValidationError(
                "Time value $value is outside [0, $units_per_day) for $(t.unit)",
            ),
        )
    end
    return nothing
end

"""
    validate_semantic(field, data)

Stage-3 validation. This public stage composes structural validation before
any content access, so callers cannot accidentally certify malformed buffer
geometry by skipping `validate_structural`. Data-intrinsic checks are cached
on the ArrayData (`semachecked`); benign concurrent callers may repeat the
same scan. Field-dependent dictionary contracts run on every call because
the same data can be checked against another Field. Per-slot nullability
enforcement is advisory and lives in the opt-in `validate_full` tier.
"""
function validate_semantic(f::Field, d::ArrayData)
    return _validate_semantic(f, d, nothing)
end

function _validate_semantic(
    f::Field,
    d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries},
)
    _validate_structural(f, d, validated_dictionaries)
    _validate_semantic_intrinsic(f, d, validated_dictionaries)
    _validate_field_contracts(f, d, validated_dictionaries)
    return d
end

function _validate_semantic_intrinsic(
    f::Field,
    d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries},
)
    t = d.type
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
                for i = 1:(d.len)
                    cur = _load_offset(offs, wide, checked_add(d.offset, Int64(i)))
                    cur >= prev || throw(
                        ValidationError("offsets not monotonically non-decreasing at $i"),
                    )
                    prev = cur
                end
                prev <= databytes || throw(
                    ValidationError("final offset $prev exceeds data extent $databytes"),
                )
            end
        end
        if t isa DictionaryType
            dictlen = length(d.dictionary)
            data = rolebuffer(d, DATA)
            w = primwidth(t.indextype)
            for i = 1:(d.len)
                isvalid_at(d, i) || continue
                idx = _load_int(data, t.indextype, _slotbyteoff(d, Int64(i), w))
                0 <= idx < dictlen || throw(
                    ValidationError("dictionary index $idx out of bounds [0, $dictlen)"),
                )
            end
        end
        if t isa UnionType
            ids = rolebuffer(d, TYPE_IDS)
            # Dense-mode-only bookkeeping; sparse unions never read it.
            lastoffset = t.mode == DenseMode ? fill(Int64(-1), length(d.children)) : Int64[]
            for i = 1:(d.len)
                tid = loadat(ids, Int8, _slotindex0(d, Int64(i)))
                pos = findfirst(==(tid), t.typeids)
                pos === nothing &&
                    throw(ValidationError("union type id $tid not in declared domain"))
                if t.mode == DenseMode
                    off = loadat(
                        rolebuffer(d, ELEMENT_OFFSETS),
                        Int32,
                        _slotbyteoff(d, Int64(i), 4),
                    )
                    0 <= off < length(d.children[pos]) || throw(
                        ValidationError(
                            "dense union offset $off out of bounds for child $pos",
                        ),
                    )
                    Int64(off) >= lastoffset[pos] || throw(
                        ValidationError(
                            "dense union offsets must be nondecreasing within child $pos",
                        ),
                    )
                    lastoffset[pos] = Int64(off)
                end
            end
        end
        t isa ViewType && _validate_view_values(t, d)
        t isa ListViewType && _validate_listview_values(t, d)
        t isa RunEndEncodedType && _validate_ree_values(d)
        actual_nulls = _count_nulls(d)
        declared_nulls = @atomic :monotonic d.nullcount
        if declared_nulls >= 0 && declared_nulls != actual_nulls
            throw(
                ValidationError(
                    "declared null count $declared_nulls does not match bitmap count $actual_nulls",
                ),
            )
        elseif declared_nulls < 0
            # Promote the unknown sentinel to the counted value: later
            # structural checks on this same array then enforce the
            # absent-bitmap rule against a known count instead of skipping it.
            @atomic :monotonic d.nullcount = actual_nulls
        end
        @atomic :monotonic d.semachecked = true
    end
    # The cache flag is per-node, so recurse even when this node is cached:
    # each child short-circuits on its own flag.
    for (cf, cd) in zip(childfields(f), d.children)
        _validate_semantic_intrinsic(cf, cd, validated_dictionaries)
    end
    if t isa DictionaryType
        dictionary = d.dictionary::ArrayData
        _dictionary_validated(validated_dictionaries, dictionary) ||
            _validate_semantic_intrinsic(
                dictvaluefield(f, t),
                dictionary,
                validated_dictionaries,
            )
    end
    return d
end

# -- view layouts (format 1.4) ----------------------------------------------

# One 16-byte view entry: length, then either 12 inline bytes (length <= 12,
# zero-padded) or prefix + buffer index + offset into one of the variadic
# data buffers that follow validity and views.
const VIEW_INLINE_MAX = Int32(12)

@inline _viewbase(d::ArrayData, i::Int64) = _slotbyteoff(d, i, 16)

function _viewdatabuffer(d::ArrayData, bufidx::Int32)
    nvariadic = length(d.buffers) - 2
    0 <= bufidx < nvariadic ||
        throw(ValidationError("view buffer index $bufidx outside [0, $nvariadic)"))
    return d.buffers[3 + Int(bufidx)]
end

"""
Semantic checks for Utf8View/BinaryView: non-null long entries must point
inside their indicated variadic buffer, and the inline prefix MUST be a copy
of the referenced data's first four bytes (the spec's comparison-fast-path
contract). Null entries' bytes are unrestricted by the spec, so only valid
slots are checked. Canonical zero-padding of short entries' unused inline
bytes is a writer recommendation no validation tier enforces:
`validate_full`'s canonical-form checks cover bit-packed buffers only.
"""
function _validate_view_values(t::ViewType, d::ArrayData)
    views = rolebuffer(d, VIEWS)
    for i = 1:(d.len)
        isvalid_at(d, i) || continue
        base = _viewbase(d, Int64(i))
        len = loadat(views, Int32, base)
        len >= 0 || throw(ValidationError("negative view length $len"))
        len <= VIEW_INLINE_MAX && continue
        bufidx = loadat(views, Int32, checked_add(base, Int64(8)))
        off = Int64(loadat(views, Int32, checked_add(base, Int64(12))))
        data = _viewdatabuffer(d, bufidx)
        off >= 0 || throw(ValidationError("negative view offset $off"))
        # Both operands are 32-bit loads, so the sum cannot overflow Int64.
        checked_add(off, Int64(len)) <= data.len || throw(
            ValidationError("view range [$off, $(off + len)) escapes data buffer $bufidx"),
        )
        for k = 0:3
            loadat(views, UInt8, checked_add(base, Int64(4 + k))) ==
            loadat(data, UInt8, checked_add(off, Int64(k))) ||
                throw(ValidationError("view prefix does not match referenced data"))
        end
    end
    return nothing
end

@inline function _listview_range(t::ListViewType, d::ArrayData, i::Int64)
    wide = t.large
    slot = _slotindex0(d, i)
    offs = rolebuffer(d, ELEMENT_OFFSETS)
    sizes = rolebuffer(d, SIZES)
    off =
        wide ? loadat(offs, Int64, checked_mul(slot, Int64(8))) :
        Int64(loadat(offs, Int32, checked_mul(slot, Int64(4))))
    sz =
        wide ? loadat(sizes, Int64, checked_mul(slot, Int64(8))) :
        Int64(loadat(sizes, Int32, checked_mul(slot, Int64(4))))
    return off, sz
end

"""
Semantic checks for ListView/LargeListView. The spec's invariants bind EVERY
slot, null included: `0 <= offsets[i]`, `0 <= sizes[i]`, and
`offsets[i] + sizes[i] <= child length`. Out-of-order and overlapping ranges
are legal — that is the layout's point.
"""
function _validate_listview_values(t::ListViewType, d::ArrayData)
    childlen = Int64(length(d.children[1]))
    for i = 1:(d.len)
        off, sz = _listview_range(t, d, Int64(i))
        (off >= 0 && sz >= 0) ||
            throw(ValidationError("list-view offset and size must be non-negative"))
        # Subtraction form: `off + sz` on two hostile 64-bit loads can
        # overflow Int64 (which must read as invalid data, not a raw
        # `OverflowError`); the message widens to Int128 so the true
        # endpoint prints either way.
        (off <= childlen && sz <= childlen - off) || throw(
            ValidationError(
                "list-view range [$off, $(Int128(off) + Int128(sz))) escapes " *
                "child length $childlen",
            ),
        )
    end
    return nothing
end

"""
Semantic checks for run-end encoding: a signed 16/32/64-bit run-ends child
with no nulls, equal-length children (one value per run), run ends positive
and strictly ascending, and the last run end covering every logical slot
(`>= offset + length` — equality holds for unsliced arrays). The REE parent
has no validity bitmap and its null count field is zero or the unknown
sentinel `-1`; logical nulls live in the values child's runs. The structural
stage has already established the run-ends descriptor (signed 16/32/64-bit),
the equal child lengths, and the parent null count.
"""
function _validate_ree_values(d::ArrayData)
    runs, values = d.children[1], d.children[2]
    nullcount(runs) == 0 || throw(ValidationError("a run end cannot be null"))
    total = checked_add(d.offset, d.len)
    data = rolebuffer(runs, DATA)
    # The typeassert re-concretizes the structurally established descriptor —
    # without it `primwidth`/`_load_int` see `ArrowType` and the trim
    # verifier reports unresolved calls.
    rti = runs.type::IntType
    w = primwidth(rti)
    prev = Int64(0)
    for i = 1:(runs.len)
        re = _load_int(data, rti, _slotbyteoff(runs, Int64(i), w))
        re > prev ||
            throw(ValidationError("run ends must be positive and strictly ascending"))
        prev = re
    end
    d.len == 0 ||
        prev >= total ||
        throw(ValidationError("run ends cover $prev of $total logical slots"))
    return nothing
end

"""
The run whose end first reaches 1-based logical position `offset + i` —
binary search over the run-ends child, the REE random-access primitive.
"""
function _ree_runindex(d::ArrayData, i::Int64)
    runs = d.children[1]
    rt = runs.type::IntType
    data = rolebuffer(runs, DATA)
    w = primwidth(rt)
    target = checked_add(d.offset, i)
    lo, hi = Int64(1), runs.len
    while lo < hi
        mid = (lo + hi) >>> 1
        re = _load_int(data, rt, _slotbyteoff(runs, mid, w))
        re >= target ? (hi = mid) : (lo = mid + 1)
    end
    return lo
end

function _logical_null_at(f::Field, d::ArrayData, i::Int64)
    t = d.type
    t isa NullType && return true
    if t isa RunEndEncodedType
        run = _ree_runindex(d, i)
        return _logical_null_at(f.children[2], d.children[2], run)
    end
    if t isa UnionType
        cf, cd, childi = _union_child(f, d, i)
        return _logical_null_at(cf, cd, childi)
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
            throw(
                ValidationError(
                    "non-nullable field $(repr(f.name)) contains a null at element $i",
                ),
            )
        end
        # A union has no parent validity bitmap. Its selected child supplies
        # both the value and any logical null, so validate that child even
        # when the union Field itself permits nulls. Unselected child slots
        # are not part of this selected union value and must remain ignored.
        cf, cd, childi = _union_child(f, d, i)
        _validate_field_contract_at(cf, cd, childi)
        return nothing
    end
    if t isa RunEndEncodedType
        # Same bitmap-less shape as unions: the selected VALUES run supplies
        # the value and any logical null. The runs child was already checked
        # whole (no nulls, ascending) by the intrinsic stage.
        if !f.nullable && _logical_null_at(f, d, i)
            throw(
                ValidationError(
                    "non-nullable field $(repr(f.name)) contains a null at element $i",
                ),
            )
        end
        _validate_field_contract_at(f.children[2], d.children[2], _ree_runindex(d, i))
        return nothing
    end

    slotnull = t isa NullType || !isvalid_at(d, i)
    if slotnull
        f.nullable || throw(
            ValidationError(
                "non-nullable field $(repr(f.name)) contains a null at element $i",
            ),
        )
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
        for j = 1:(t.listsize)
            _validate_field_contract_at(cf, cd, checked_add(base, Int64(j)))
        end
    elseif t isa Union{ListType,MapType}
        lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
        lo == hi && return nothing
        cf, cd = f.children[1], d.children[1]
        for childi = checked_add(lo, Int64(1)):hi
            _validate_field_contract_at(cf, cd, childi)
        end
    elseif t isa ListViewType
        off, sz = _listview_range(t, d, i)
        sz == 0 && return nothing
        cf, cd = f.children[1], d.children[1]
        for childi = checked_add(off, Int64(1)):checked_add(off, sz)
            _validate_field_contract_at(cf, cd, childi)
        end
    end
    return nothing
end

function _validate_dictionary_contracts(
    f::Field,
    d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries},
)
    if d.type isa DictionaryType
        # Dictionary values form an independent array. Index nullability never
        # constrains pool nullability, but nested Field contracts inside the
        # pool still apply to every pool value, even when the dictionary array
        # itself is nested below a masked parent.
        dictionary = d.dictionary::ArrayData
        _dictionary_validated(validated_dictionaries, dictionary) ||
            _validate_field_contracts(
                dictvaluefield(f, d.type),
                dictionary,
                validated_dictionaries,
            )
    end
    for (cf, cd) in zip(f.children, d.children)
        _validate_dictionary_contracts(cf, cd, validated_dictionaries)
    end
    return nothing
end

# `Field.nullable` is ADVISORY schema metadata in the ecosystem: the
# reference C++ implementation neither enforces it on read nor rejects a
# non-nullable field whose data holds nulls, and the apache/arrow-testing
# gold corpus carries exactly that (a `nullable=false` union whose selected
# child is null). The semantic stage therefore validates only the
# structurally-load-bearing dictionary contracts; the per-slot nullability
# walk (`_validate_field_contract_at`) runs in the opt-in `validate_full`
# tier for callers who want the declaration enforced.
function _validate_field_contracts(
    f::Field,
    d::ArrayData,
    validated_dictionaries::Union{Nothing,_ValidatedDictionaries},
)
    _validate_dictionary_contracts(f, d, validated_dictionaries)
    return nothing
end

function _validate_nullability(f::Field, d::ArrayData)
    for i = 1:(d.len)
        _validate_field_contract_at(f, d, Int64(i))
    end
    # Dictionary pools are independent arrays: their nested Field contracts
    # apply to every pool value regardless of which indices reference them
    # (and regardless of masking above the dictionary array), so each pool
    # gets its own root walk.
    _validate_pool_nullability(f, d)
    return nothing
end

function _validate_pool_nullability(f::Field, d::ArrayData)
    if d.type isa DictionaryType
        _validate_nullability(dictvaluefield(f, d.type), d.dictionary::ArrayData)
    end
    for (cf, cd) in zip(f.children, d.children)
        _validate_pool_nullability(cf, cd)
    end
    return nothing
end

"""
    validate_full(field, data)

Stage-4 (opt-in) content validation. It composes semantic (and therefore
structural) validation before the more expensive whole-content checks.
"""
function validate_full(f::Field, d::ArrayData)
    validate_semantic(f, d)
    # The nullability walk enters ONCE at the root: it routes through
    # unions/REE and applies parent-null masking itself, so recursing it per
    # child would flag masked slots that are not part of any public-domain value.
    _validate_nullability(f, d)
    _validate_full_content(f, d)
    return d
end

# Canonical bit-packed form: the spec recommends writers zero the unused
# trailing bits of the final byte and any padding bytes, and forbids readers
# from relying on either — so enforcement is full-tier only. Sliced arrays
# are exempt: trailing bits inside a shared bitmap window can legitimately
# belong to a sibling slice.
function _validate_canonical_bits(d::ArrayData)
    d.offset == 0 && d.len > 0 || return nothing
    spec = layoutspec_of(d.type)
    for (idx, role) in enumerate(spec.buffers)
        role == VALIDITY || (role == DATA && d.type isa BoolType) || continue
        b = d.buffers[idx]
        nbytes = Int64(cld(d.len, 8))
        # Absent or short bitmaps are the structural tier's concern.
        b.len >= nbytes || continue
        tail = d.len % 8
        if tail != 0
            mask = UInt8(0xff) << tail
            loadat(b, UInt8, nbytes - 1) & mask == 0x00 || throw(
                ValidationError(
                    "canonical form requires zeroed unused bits in the final " *
                    "byte of a bit-packed buffer",
                ),
            )
        end
        for i = nbytes:(b.len - 1)
            loadat(b, UInt8, i) == 0x00 || throw(
                ValidationError(
                    "canonical form requires zeroed padding in bit-packed buffers",
                ),
            )
        end
    end
    return nothing
end

# Closed-set ladder over the three advisory-check descriptors (same
# devirtualization story as layoutspec_of).
@inline function _validate_advisory_values_of(d::ArrayData)
    t = d.type
    t isa DateType && return _validate_advisory_values(t, d)
    t isa TimeType && return _validate_advisory_values(t, d)
    t isa DecimalType && return _validate_advisory_values(t, d)
    return nothing
end

function _validate_full_content(f::Field, d::ArrayData)
    _validate_advisory_values_of(d)
    _validate_canonical_bits(d)
    if d.type isa Utf8Type || (d.type isa ViewType && d.type.utf8)
        for i = 1:(d.len)
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
juliatype(t::IntType) =
    t.signed ? (t.bits == 8 ? Int8 : t.bits == 16 ? Int16 : t.bits == 32 ? Int32 : Int64) :
    (t.bits == 8 ? UInt8 : t.bits == 16 ? UInt16 : t.bits == 32 ? UInt32 : UInt64)
juliatype(t::FloatType) = t.bits == 16 ? Float16 : t.bits == 32 ? Float32 : Float64
juliatype(::TimestampType) = Int64
juliatype(::DurationType) = Int64
juliatype(t::DateType) = t.unit == DAY ? Int32 : Int64
juliatype(t::TimeType) = t.bits == 32 ? Int32 : Int64
juliatype(::Utf8Type) = String
juliatype(::BinaryType) = Vector{UInt8}
juliatype(t::FixedSizeBinaryType) = Vector{UInt8}
juliatype(t::ViewType) = t.utf8 ? String : Vector{UInt8}
juliatype(t::DecimalType) = t.bits == 32 ? Int32 : t.bits == 64 ? Int64 : Vector{UInt8}
juliatype(t::IntervalType) =
    t.unit == YEAR_MONTH ? Int32 :
    t.unit == DAY_TIME ? NamedTuple{(:days, :millis),Tuple{Int32,Int32}} :
    NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}}
juliatype(::StructType) = Vector{Pair{String,Any}}
juliatype(::MapType) = Vector{Pair{Any,Any}}

@inline function _load_int(b::BufferSlice, t::IntType, byteoff::Int64)::Int64
    # Literal load widths avoid a runtime DataType in the raw-load path, which
    # produces code that the trim verifier cannot resolve.
    if t.signed
        t.bits == 64 && return loadat(b, Int64, byteoff)
        t.bits == 32 && return Int64(loadat(b, Int32, byteoff))
        t.bits == 16 && return Int64(loadat(b, Int16, byteoff))
        return Int64(loadat(b, Int8, byteoff))
    else
        if t.bits == 64
            u = loadat(b, UInt64, byteoff)
            # Explicit range check: a stored UInt64 above typemax(Int64) is
            # out of domain for every Int64-typed consumer (dictionary
            # lengths and run ends are Int64), and a bare `Int64(u)` would
            # leak an `InexactError` through the validation tier.
            u <= UInt64(typemax(Int64)) ||
                throw(ValidationError("unsigned 64-bit value $u exceeds the Int64 range"))
            return Int64(u)
        end
        t.bits == 32 && return Int64(loadat(b, UInt32, byteoff))
        t.bits == 16 && return Int64(loadat(b, UInt16, byteoff))
        return Int64(loadat(b, UInt8, byteoff))
    end
end

"""
    getvalue(field, data, i) -> Union{Missing, value}

Read logical element `i` (1-based). Layout dispatch is the closed-set `isa`
ladder over the runtime descriptor — a type-test chain per call, not a
dynamic dispatch. This is Core's honest contract:
scalar access through the erased representation pays a boundary cost;
`materialize` resolves the layout once and loops through a function
barrier.
"""
function getvalue(f::Field, d::ArrayData, i::Integer)
    1 <= i <= d.len || throw(BoundsError(d, i))
    return _value_of(d.type, f, d, Int64(i))
end

# Reader adapters can attach one cumulative allocation budget without making
# the dependency-free Core know the adapter's budget type. The ordinary
# methods below remain the trim-safe API. Budgeted overloads only preflight
# package-owned output containers, then call the same audited extraction.
function _charge_materialization! end
function _materialization_remaining end
function _materialization_limit_exceeded! end

@inline function _materializedvectorbytes(::Type{T}, n::Integer) where {T}
    n >= 0 || throw(ValidationError("materialized vector length is negative"))
    payload = checked_mul(Int64(n), Int64(Base.elsize(Vector{T})))
    # Julia stores one selector byte per element beside an isbits-Union
    # vector's ordinary payload.
    Base.isbitsunion(T) && (payload = checked_add(payload, Int64(n)))
    # Supported Julia runtimes allocate at most one 64-byte object for an
    # empty Vector after warm-up. Charging the nonempty 128-byte header model
    # per empty nested value rejects compact Arrow columns by hundreds of
    # megabytes even though their materialized empty containers are small.
    payload == 0 && return Int64(64)
    # Julia 1.11's `Memory` backing store may round a just-over-half-full
    # allocation to the next size class. The measured worst case approaches
    # twice the requested payload. Reserve that full capacity plus both the
    # Vector and Memory headers; using the logical payload alone can let one
    # package-owned allocation exceed the caller's budget by almost 2x.
    capacity = checked_mul(Int64(2), payload)
    return checked_add(Int64(128), capacity)
end

@inline function _materializedbitvectorbytes(n::Integer)
    n >= 0 || throw(ValidationError("materialized bit-vector length is negative"))
    chunks = cld(Int64(n), Int64(64))
    return checked_add(
        _materializedvectorbytes(UInt64, chunks),
        _materializedobjectbytes(sizeof(BitVector)),
    )
end

# Conservative Julia heap-object size for a newly boxed inline value: an
# 8-byte object tag plus the payload, rounded to the 16-byte GC size class.
@inline function _materializedobjectbytes(payload::Integer)
    payload >= 0 || throw(ValidationError("materialized object size is negative"))
    total = checked_add(Int64(payload), Int64(8))
    return checked_mul(Int64(16), cld(total, Int64(16)))
end

mutable struct _MaterializationEstimate{B}
    budget::B
    bytes::Int64
    limit::Int64
end

_MaterializationEstimate(budget) =
    _MaterializationEstimate(budget, Int64(0), _materialization_remaining(budget))

@inline function _addestimate!(
    estimate::_MaterializationEstimate,
    amount::Int64,
    what::AbstractString,
)
    amount >= 0 || throw(ValidationError("materialization estimate is negative"))
    amount <= estimate.limit - estimate.bytes ||
        _materialization_limit_exceeded!(estimate.budget, what)
    estimate.bytes += amount
    return nothing
end

@inline _reservevector!(
    estimate::_MaterializationEstimate,
    ::Type{T},
    n::Integer,
    what::AbstractString,
) where {T} = _addestimate!(estimate, _materializedvectorbytes(T, n), what)

@inline _reserveobject!(
    estimate::_MaterializationEstimate,
    payload::Integer,
    what::AbstractString,
) = _addestimate!(estimate, _materializedobjectbytes(payload), what)

@inline function _commitestimate!(estimate::_MaterializationEstimate, what::AbstractString)
    _charge_materialization!(estimate.budget, estimate.bytes, what)
    return nothing
end

_value(::Any, ::Field, ::ArrayData, ::Int64) =
    throw(ArgumentError("unregistered ArrowType"))

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
    # 128/256-bit decimals surface as raw native-endian bytes (BigInt/Int256
    # conversion is the facade's); 32/64 as integers. Core RecordBatches
    # accept native-endian buffers only.
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
        return (
            days=loadat(b, Int32, off),
            millis=loadat(b, Int32, checked_add(off, Int64(4))),
        )
    else # MONTH_DAY_NANO
        off = _slotbyteoff(d, i, 16)
        return (
            months=loadat(b, Int32, off),
            days=loadat(b, Int32, checked_add(off, Int64(4))),
            nanos=loadat(b, Int64, checked_add(off, Int64(8))),
        )
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

# Dynamic recursion mirrors `_typedchild`: scalar children inline into their
# parent's loop, while composite children cross one compiled function barrier.
# Calling public `getvalue` for every scalar child allocates a dispatch box per
# element; recursively inlining every composite instead breaks trim inference.
@inline function _dynamicchild(f::Field, d::ArrayData, i::Int64)
    1 <= i <= d.len || throw(BoundsError(d, i))
    t = d.type
    t isa IntType && return _value(t, f, d, i)
    t isa FloatType && return _value(t, f, d, i)
    t isa BoolType && return _value(t, f, d, i)
    t isa Utf8Type && return _value(t, f, d, i)
    t isa BinaryType && return _value(t, f, d, i)
    t isa FixedSizeBinaryType && return _value(t, f, d, i)
    t isa TimestampType && return _value(t, f, d, i)
    t isa DateType && return _value(t, f, d, i)
    t isa TimeType && return _value(t, f, d, i)
    t isa DurationType && return _value(t, f, d, i)
    t isa DecimalType && return _value(t, f, d, i)
    t isa IntervalType && return _value(t, f, d, i)
    t isa ViewType && return _value(t, f, d, i)
    t isa NullType && return _value(t, f, d, i)
    return _dynamicchildbox(f, d, i)
end

function _dynamicchildbox(f::Field, d::ArrayData, i::Int64)
    return _value_of(d.type, f, d, i)
end

function _value(t::ListType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
    child, cf = d.children[1], f.children[1]
    # Explicit Vector{Any}: an Any-first comprehension re-narrows its result
    # at runtime, which is both trim-hostile and wasted work — typed element
    # containers are the facade's job.
    out = Vector{Any}(undef, Int(hi - lo))
    for k = 1:Int(hi - lo)
        out[k] = _dynamicchild(cf, child, checked_add(lo, Int64(k)))
    end
    return out
end

function _value(t::FixedSizeListType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    child, cf = d.children[1], f.children[1]
    base = checked_mul(_slotindex0(d, i), Int64(t.listsize))
    out = Vector{Any}(undef, t.listsize)
    for j = 1:(t.listsize)
        out[j] = _dynamicchild(cf, child, checked_add(base, Int64(j)))
    end
    return out
end

function _value(::StructType, f::Field, d::ArrayData, i::Int64)
    # Core's struct scalar is an ordered Vector{Pair{String,Any}} — always.
    # A NamedTuple carries its names in the TYPE domain, so building one from
    # runtime schema names is intrinsically dynamic (and cannot represent
    # Arrow's duplicate/empty/non-Symbol names at all). The typed NamedTuple
    # surface belongs to the facade and to callers' static claims through
    # `getvalue(::Type{T}, ...)`; Core stays concrete and trim-clean.
    isvalid_at(d, i) || return missing
    childindex = checked_add(d.offset, i)
    n = length(f.children)
    out = Vector{Pair{String,Any}}(undef, n)
    for j = 1:n
        out[j] = Pair{String,Any}(
            f.children[j].name,
            _dynamicchild(f.children[j], d.children[j], childindex),
        )
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
        out[k] = Pair{Any,Any}(
            _dynamicchild(kf, kd, entryindex),
            _dynamicchild(vf, vd, entryindex),
        )
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
        return _dynamicchild(cf, child, checked_add(Int64(off), Int64(1)))
    else
        return _dynamicchild(cf, child, checked_add(d.offset, i))
    end
end

function _value(t::DictionaryType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    w = primwidth(t.indextype)
    idx = _load_int(rolebuffer(d, DATA), t.indextype, _slotbyteoff(d, i, w))
    dict = d.dictionary
    dict === nothing &&
        throw(ValidationError("dictionary-encoded array without a dictionary"))
    return _dynamicchild(dictvaluefield(f, t), dict, checked_add(Int64(idx), Int64(1)))
end

function _value(t::ViewType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    views = rolebuffer(d, VIEWS)
    base = _viewbase(d, i)
    len = loadat(views, Int32, base)
    len >= 0 || throw(ValidationError("negative view length $len"))
    n = Int64(len)
    # Semantic validation certified geometry and prefixes; subslice re-checks
    # bounds so unvalidated access still cannot escape a buffer.
    bytes = if len <= VIEW_INLINE_MAX
        slicebytes(subslice(views, checked_add(base, Int64(4)), n))
    else
        bufidx = loadat(views, Int32, checked_add(base, Int64(8)))
        off = Int64(loadat(views, Int32, checked_add(base, Int64(12))))
        off >= 0 || throw(ValidationError("negative view offset $off"))
        slicebytes(subslice(_viewdatabuffer(d, bufidx), off, n))
    end
    return t.utf8 ? String(bytes) : bytes
end

function _value(t::ListViewType, f::Field, d::ArrayData, i::Int64)
    isvalid_at(d, i) || return missing
    off, sz = _listview_range(t, d, i)
    (off >= 0 && sz >= 0) ||
        throw(ValidationError("list-view offset and size must be non-negative"))
    child, cf = d.children[1], f.children[1]
    out = Vector{Any}(undef, Int(sz))
    for k = 1:Int(sz)
        out[k] = _dynamicchild(cf, child, checked_add(off, Int64(k)))
    end
    return out
end

_value(::RunEndEncodedType, f::Field, d::ArrayData, i::Int64) =
    _dynamicchild(f.children[2], d.children[2], _ree_runindex(d, i))

# Preflight the containers allocated by one dynamic Core value. This walk uses
# only validated offsets and scalar buffer loads. In particular, a hostile
# FixedSizeList width is charged before the child loop starts.
function _prechargevalue!(
    ::Type{T},
    t::Union{IntType,FloatType},
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    T === Any && _reserveobject!(estimate, primwidth(t), "boxed numeric value")
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    ::Union{TimestampType,DurationType},
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    T === Any && _reserveobject!(estimate, 8, "boxed temporal value")
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    t::DateType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    T === Any && _reserveobject!(estimate, t.unit == DAY ? 4 : 8, "boxed date value")
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    t::TimeType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    T === Any && _reserveobject!(estimate, t.bits == 32 ? 4 : 8, "boxed time value")
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    t::IntervalType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    payload = t.unit == YEAR_MONTH ? 4 : t.unit == DAY_TIME ? 8 : 16
    T === Any && _reserveobject!(estimate, payload, "boxed interval value")
    return nothing
end

_prechargevalue!(
    ::Type,
    ::Union{BoolType,NullType},
    ::Field,
    ::ArrayData,
    ::Int64,
    estimate,
) = nothing

function _prechargevalue!(
    ::Type{T},
    t::DecimalType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    if t.bits > 64
        _reservevector!(estimate, UInt8, primwidth(t), "decimal value")
    elseif T === Any
        _reserveobject!(estimate, primwidth(t), "boxed decimal value")
    end
    return nothing
end

function _prechargevalue!(
    ::Type,
    t::FixedSizeBinaryType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
)
    isvalid_at(d, i) || return nothing
    _reservevector!(estimate, UInt8, t.nbytes, "fixed-size binary value")
    return nothing
end

function _prechargevalue!(
    ::Type,
    t::Union{Utf8Type,BinaryType},
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
)
    isvalid_at(d, i) || return nothing
    lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
    # `_value` returns Base's singleton empty String without allocating. Keep
    # that exact fast path out of the per-value reserve. Empty Binary values
    # still allocate a fresh UInt8 vector and remain charged below.
    t isa Utf8Type && hi == lo && return nothing
    _reservevector!(estimate, UInt8, hi - lo, "binary value")
    return nothing
end

function _prechargevalue!(::Type, t::ViewType, f::Field, d::ArrayData, i::Int64, estimate)
    isvalid_at(d, i) || return nothing
    len = loadat(rolebuffer(d, VIEWS), Int32, _viewbase(d, i))
    len >= 0 || throw(ValidationError("negative view length $len"))
    _reservevector!(estimate, UInt8, len, "view value")
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    t::ListType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
    _prechargelist!(T, f, d, lo, hi - lo, estimate)
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    t::ListViewType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    off, n = _listview_range(t, d, i)
    (off >= 0 && n >= 0) ||
        throw(ValidationError("list-view offset and size must be non-negative"))
    _prechargelist!(T, f, d, off, n, estimate)
    return nothing
end

function _prechargelist!(
    ::Type{T},
    f::Field,
    d::ArrayData,
    off::Int64,
    n::Int64,
    estimate,
) where {T}
    child, cf = d.children[1], f.children[1]
    if T === Any
        _reservevector!(estimate, Any, n, "list value")
        for k = 1:Int(n)
            _prechargechild!(Any, cf, child, checked_add(off, Int64(k)), estimate)
        end
    else
        CE = eltype(Base.nonmissingtype(T))
        _reservevector!(estimate, CE, n, "typed list value")
        for k = 1:Int(n)
            _prechargechild!(CE, cf, child, checked_add(off, Int64(k)), estimate)
        end
    end
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    t::FixedSizeListType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    child, cf = d.children[1], f.children[1]
    base = checked_mul(_slotindex0(d, i), Int64(t.listsize))
    if T === Any
        CE = Any
        _reservevector!(estimate, CE, t.listsize, "fixed-size list value")
    else
        CE = eltype(Base.nonmissingtype(T))
        _reservevector!(estimate, CE, t.listsize, "typed fixed-size list value")
    end
    for k = 1:(t.listsize)
        _prechargechild!(CE, cf, child, checked_add(base, Int64(k)), estimate)
    end
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    ::StructType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    E = T === Any ? Any : Base.nonmissingtype(T)
    childindex = checked_add(d.offset, i)
    if E === Any || E === Vector{Pair{String,Any}}
        _reservevector!(estimate, Pair{String,Any}, length(f.children), "struct value")
        for k in eachindex(f.children)
            _prechargechild!(Any, f.children[k], d.children[k], childindex, estimate)
        end
        return nothing
    end
    E <: NamedTuple || return nothing
    Base.allocatedinline(T) ||
        _reserveobject!(estimate, sizeof(E), "boxed typed struct value")
    _prechargetypedstruct!(E, f, d, childindex, estimate)
    return nothing
end

@generated function _prechargetypedstruct!(
    ::Type{E},
    f::Field,
    d::ArrayData,
    childindex::Int64,
    estimate,
) where {E<:NamedTuple}
    calls = Expr[
        :(_prechargechild!(
            $(fieldtype(E, j)),
            f.children[$j],
            d.children[$j],
            childindex,
            estimate,
        )) for j = 1:fieldcount(E)
    ]
    return quote
        $(calls...)
        return nothing
    end
end

function _prechargevalue!(::Type, t::MapType, f::Field, d::ArrayData, i::Int64, estimate)
    isvalid_at(d, i) || return nothing
    lo, hi = _offsets_at(d, i, false)
    _reservevector!(estimate, Pair{Any,Any}, hi - lo, "map value")
    entries, ef = d.children[1], f.children[1]
    for k = 1:Int(hi - lo)
        entryindex = checked_add(entries.offset, checked_add(lo, Int64(k)))
        for j = 1:2
            _prechargechild!(Any, ef.children[j], entries.children[j], entryindex, estimate)
        end
    end
    return nothing
end

function _prechargevalue!(::Type, t::UnionType, f::Field, d::ArrayData, i::Int64, estimate)
    cf, child, childindex = _union_child(f, d, i)
    _prechargechild!(Any, cf, child, childindex, estimate)
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    t::DictionaryType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    isvalid_at(d, i) || return nothing
    w = primwidth(t.indextype)
    index = _load_int(rolebuffer(d, DATA), t.indextype, _slotbyteoff(d, i, w))
    dictionary = d.dictionary
    dictionary === nothing &&
        throw(ValidationError("dictionary-encoded array without a dictionary"))
    _prechargechild!(
        T,
        dictvaluefield(f, t),
        dictionary,
        checked_add(Int64(index), Int64(1)),
        estimate,
    )
    return nothing
end

function _prechargevalue!(
    ::Type{T},
    ::RunEndEncodedType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    _prechargechild!(T, f.children[2], d.children[2], _ree_runindex(d, i), estimate)
    return nothing
end

_prechargevalue!(::Type, ::ArrowType, ::Field, ::ArrayData, ::Int64, estimate) = nothing

# Like the value path, preflight keeps leaves inline and gives recursive
# composites one concrete compiled edge. The walk itself must not create an
# uncharged dispatch allocation for every child it inspects.
@inline function _prechargechild!(
    ::Type{T},
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    1 <= i <= d.len || throw(BoundsError(d, i))
    t = d.type
    t isa IntType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa FloatType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa BoolType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa Utf8Type && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa BinaryType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa FixedSizeBinaryType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa TimestampType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa DateType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa TimeType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa DurationType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa DecimalType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa IntervalType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa ViewType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa NullType && return _prechargevalue!(T, t, f, d, i, estimate)
    return _prechargechildbox!(T, f, d, i, estimate)
end

function _prechargechildbox!(
    ::Type{T},
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    return _prechargevalue_of!(T, d.type, f, d, i, estimate)
end

@inline function _prechargevalue_of!(
    ::Type{T},
    t::ArrowType,
    f::Field,
    d::ArrayData,
    i::Int64,
    estimate,
) where {T}
    t isa IntType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa FloatType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa Utf8Type && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa BoolType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa ListType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa StructType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa DictionaryType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa TimestampType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa DateType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa TimeType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa DurationType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa BinaryType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa FixedSizeBinaryType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa FixedSizeListType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa MapType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa UnionType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa DecimalType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa IntervalType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa NullType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa ViewType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa ListViewType && return _prechargevalue!(T, t, f, d, i, estimate)
    t isa RunEndEncodedType && return _prechargevalue!(T, t, f, d, i, estimate)
    return _prechargevalue!(T, t, f, d, i, estimate)
end

function getvalue(f::Field, d::ArrayData, i::Integer, budget)
    1 <= i <= d.len || throw(BoundsError(d, i))
    estimate = _MaterializationEstimate(budget)
    _prechargevalue_of!(Any, d.type, f, d, Int64(i), estimate)
    _commitestimate!(estimate, "materialized value")
    return _value_of(d.type, f, d, Int64(i))
end

"""
    materialize(field, data) -> Vector

Bulk conversion to native Julia values: resolve the layout ONCE, then run
a specialized loop behind a function barrier — the loop body compiles per
LAYOUT (a small closed set), never per schema.
"""
materialize(f::Field, d::ArrayData) = _materialize_of(d.type, f, d)

function materialize(f::Field, d::ArrayData, budget)
    estimate = _MaterializationEstimate(budget)
    _reservevector!(estimate, Any, d.len, "materialized column")
    for i = 1:(d.len)
        _prechargevalue_of!(Any, d.type, f, d, Int64(i), estimate)
    end
    _commitestimate!(estimate, "materialized column")
    return materialize(f, d)
end

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
    for i = 1:(d.len)
        out[i] = _value(t, f, d, Int64(i))
    end
    # Vector{Any} by design: result-element typing is the caller's claim
    # through `materialize(::Type{T}, ...)` (which the facade uses for closed
    # claims), and a runtime narrow is trim-hostile. Tests compare with
    # ==/isequal, which is eltype-agnostic.
    return out
end

# ---------------------------------------------------------------------------
# Typed element access: the caller asserts the element domain. With a
# concrete static schema at the call site every load
# resolves statically — the trim-compile contract dynamic access cannot
# offer. The type is a CLAIM about the same value domain the dynamic
# accessors return (storage integers for temporal, `Vector{Pair}` rows for
# struct/map): the read produces exactly that type or refuses with a clear
# error. `Any` is the dynamic path unchanged.
#
# Recursion architecture: it mirrors the dynamic path exactly. Recursive
# edges route through `_typedchild` — a COMPILED function whose argument
# types are all concrete (like public `getvalue` on the dynamic side) — so
# the cycle's one non-inlined call is fully resolvable; the `@inline`
# ladder and leaf methods flatten into it. An `@inline` ladder call
# carrying an abstract descriptor as the recursive edge is unresolvable
# under trim (no standalone specialization exists).
# ---------------------------------------------------------------------------

"""
    getvalue(::Type{T}, field, data, i) -> T

Statically typed element access: `T` asserts the element domain (what the
dynamic accessors return for this layout — see `juliatype` for the leaf
layouts), with `Missing <: T` required to admit nulls. The claim checks against the
DESCRIPTOR up front — an empty or all-null column certifies nothing.
Composites recurse: a `List<Int64>` column reads as
`Vector{Vector{Int64}}`, and a `Struct` column may read as a `NamedTuple`
row type whose names match the child fields in order. Mismatches refuse
with `ArgumentError` — values are never converted. `T === Any` delegates
to the dynamic path.
"""
function getvalue(::Type{T}, f::Field, d::ArrayData, i::Integer) where {T}
    T === Any && return getvalue(f, d, i)
    _checkclaim(T, f, d)
    1 <= i <= d.len || throw(BoundsError(d, i))
    return _typedvalue_of(T, d.type, f, d, Int64(i))::T
end

"""
    materialize(::Type{T}, field, data) -> Vector{T}

The bulk form of the typed [`getvalue`](@ref): every element under the
static claim `T` (see `getvalue(::Type{T}, field, data, i)` for the claim
rules), through one typed loop per layout.
"""
function materialize(::Type{T}, f::Field, d::ArrayData) where {T}
    T === Any && return materialize(f, d)
    _checkclaim(T, f, d)
    return _typedmaterialize_of(T, d.type, f, d)::Vector{T}
end

function materialize(::Type{T}, f::Field, d::ArrayData, budget) where {T}
    T === Any && return materialize(f, d, budget)
    _checkclaim(T, f, d)
    estimate = _MaterializationEstimate(budget)
    _reservevector!(estimate, T, d.len, "typed materialized column")
    for i = 1:(d.len)
        _prechargevalue_of!(T, d.type, f, d, Int64(i), estimate)
    end
    # Nullable fixed-width bulk extraction first builds Vector{E}, then its
    # public Vector{Union{Missing,E}}. Account for that private scratch copy.
    E = Base.nonmissingtype(T)
    if Missing <: T &&
       isbitstype(E) &&
       d.type isa
       Union{IntType,FloatType,TimestampType,DateType,TimeType,DurationType,DecimalType} &&
       E === juliatype(d.type) &&
       primwidth(d.type) == Int64(sizeof(E))
        _reservevector!(estimate, E, d.len, "typed materialization scratch")
    end
    _commitestimate!(estimate, "typed materialized column")
    return _typedmaterialize_of(T, d.type, f, d)::Vector{T}
end

# The message names the layout via `nameof` (generic struct/type `show` is
# trim-hostile, and an abstract descriptor argument would leave the throw
# helper unresolvable); `juliatype(t)` tells a caller the expected claim.
# Closed-set ladder to literal strings: `nameof(typeof(t))` on an abstract
# descriptor is itself an unresolvable call under trim.
@inline function _layoutname(t::ArrowType)
    t isa IntType && return "IntType"
    t isa FloatType && return "FloatType"
    t isa BoolType && return "BoolType"
    t isa Utf8Type && return "Utf8Type"
    t isa BinaryType && return "BinaryType"
    t isa FixedSizeBinaryType && return "FixedSizeBinaryType"
    t isa TimestampType && return "TimestampType"
    t isa DateType && return "DateType"
    t isa TimeType && return "TimeType"
    t isa DurationType && return "DurationType"
    t isa ViewType && return "ViewType"
    t isa DecimalType && return "DecimalType"
    t isa IntervalType && return "IntervalType"
    t isa MapType && return "MapType"
    t isa StructType && return "StructType"
    t isa ListType && return "ListType"
    t isa ListViewType && return "ListViewType"
    t isa FixedSizeListType && return "FixedSizeListType"
    t isa DictionaryType && return "DictionaryType"
    t isa RunEndEncodedType && return "RunEndEncodedType"
    t isa UnionType && return "UnionType"
    t isa NullType && return "NullType"
    return "ArrowType"
end
@noinline _typedrefuse(::Type{E}, kind::String, f::Field) where {E} = throw(
    ArgumentError(
        "field $(f.name) materializes $(kind)-layout " *
        "values; the claimed static element type does not match",
    ),
)
@noinline _typednullrefuse(f::Field) = throw(
    ArgumentError(
        "field $(f.name) holds a null but the static " *
        "element type does not admit missing",
    ),
)
@inline _typedmissing(::Type{T}, f::Field) where {T} =
    Missing <: T ? missing : _typednullrefuse(f)

"""
Descriptor-level claim preflight: `T` must match the element domain the
schema DECLARES — acceptance never depends on which values a batch
happens to contain (an empty or all-null column certifies nothing).
Shapes, field counts, and names check ONCE here; the element loop stays
check-free. Compiled (not `@inline`): its recursion keeps the claim
intact through transparent wrappers, and a compiled concrete-arg edge is
what makes that cycle trim-resolvable.
"""
function _checkclaim(::Type{T}, f::Field, d::ArrayData)::Nothing where {T}
    t = d.type
    if t isa ListType || t isa ListViewType || t isa FixedSizeListType
        E = Base.nonmissingtype(T)
        E <: Vector || _typedrefuse(E, _layoutname(t), f)
        length(f.children) == 1 && length(d.children) == 1 ||
            _typedrefuse(E, _layoutname(t), f)
        return _checkclaim(eltype(E), f.children[1], d.children[1])
    end
    if t isa StructType
        E = Base.nonmissingtype(T)
        E === Vector{Pair{String,Any}} && return nothing
        # An EXACT NamedTuple shape only: a Union or UnionAll of row types
        # satisfies `<: NamedTuple` but has no field reflection — it must
        # refuse here, not leak a generation error.
        (E isa DataType && E <: NamedTuple) || _typedrefuse(E, _layoutname(t), f)
        (fieldcount(E) == length(f.children) && fieldcount(E) == length(d.children)) ||
            _typedrefuse(E, _layoutname(t), f)
        return _checkstructclaim(E, f, d)
    end
    if t isa DictionaryType
        dict = d.dictionary
        dict === nothing &&
            throw(ValidationError("dictionary-encoded array without a dictionary"))
        return _checkclaim(T, dictvaluefield(f, t), dict)
    end
    if t isa RunEndEncodedType
        (length(f.children) == 2 && length(d.children) == 2) ||
            _typedrefuse(Base.nonmissingtype(T), _layoutname(t), f)
        return _checkclaim(T, f.children[2], d.children[2])
    end
    t isa UnionType && _typedrefuse(Base.nonmissingtype(T), _layoutname(t), f)
    if t isa NullType
        Missing <: T || _typednullrefuse(f)
        return nothing
    end
    E = Base.nonmissingtype(T)
    E === _juliatype_of(t) || _typedrefuse(E, _layoutname(t), f)
    return nothing
end

# Generated so every field index is a LITERAL: `fieldtype(E, j)` with a
# runtime `j` yields an abstract `Type` and poisons the recursion, and the
# name strings bake in at generation (no per-call conversion at all).
@generated function _checkstructclaim(
    ::Type{E},
    f::Field,
    d::ArrayData,
)::Nothing where {E<:NamedTuple}
    checks = Expr[]
    for j = 1:fieldcount(E)
        push!(
            checks,
            :(
                $(String(fieldnames(E)[j])) == f.children[$j].name ||
                _typedrefuse(E, _layoutname(d.type), f)
            ),
        )
        push!(checks, :(_checkclaim($(fieldtype(E, j)), f.children[$j], d.children[$j])))
    end
    return quote
        $(checks...)
        return nothing
    end
end

# Closed-set ladder for the preflight's leaf claims — scalars plus the
# `Map` row vector (an abstract
# `juliatype(t::ArrowType)` call would defeat trim resolution).
@inline function _juliatype_of(t::ArrowType)
    t isa IntType && return juliatype(t)
    t isa FloatType && return juliatype(t)
    t isa BoolType && return juliatype(t)
    t isa Utf8Type && return juliatype(t)
    t isa BinaryType && return juliatype(t)
    t isa FixedSizeBinaryType && return juliatype(t)
    t isa TimestampType && return juliatype(t)
    t isa DateType && return juliatype(t)
    t isa TimeType && return juliatype(t)
    t isa DurationType && return juliatype(t)
    t isa ViewType && return juliatype(t)
    t isa DecimalType && return juliatype(t)
    t isa IntervalType && return juliatype(t)
    t isa MapType && return juliatype(t)
    throw(ArgumentError("unregistered ArrowType"))
end

# The typed recursion edge, split for two masters. The logical-bounds guard
# matches what the dynamic path gets from public `getvalue`: unvalidated
# geometry must not read backing values past a child's logical length.
# SCALAR leaves inline into the parent's loop so the buffer-slice
# temporaries stay stack-allocated (a compiled boundary per child read
# would allocate); COMPOSITE children route to `_typedchildbox`, a compiled
# shell whose argument types are all concrete — the resolvable edge trim
# requires. The generic ladder flattens reliably into a dedicated shell but
# not into arbitrary hoisted contexts, so the shell is the only caller. The
# `::T` asserts pin inference to the claim where the same-claim wrapper
# cycle (Dictionary/REE) would widen to Any.
@inline function _typedchild(::Type{T}, f::Field, d::ArrayData, i::Int64) where {T}
    1 <= i <= d.len || throw(BoundsError(d, i))
    t = d.type
    t isa IntType && return _typedvalue(T, t, f, d, i)::T
    t isa FloatType && return _typedvalue(T, t, f, d, i)::T
    t isa BoolType && return _typedvalue(T, t, f, d, i)::T
    t isa Utf8Type && return _typedvalue(T, t, f, d, i)::T
    t isa BinaryType && return _typedvalue(T, t, f, d, i)::T
    t isa FixedSizeBinaryType && return _typedvalue(T, t, f, d, i)::T
    t isa TimestampType && return _typedvalue(T, t, f, d, i)::T
    t isa DateType && return _typedvalue(T, t, f, d, i)::T
    t isa TimeType && return _typedvalue(T, t, f, d, i)::T
    t isa DurationType && return _typedvalue(T, t, f, d, i)::T
    t isa DecimalType && return _typedvalue(T, t, f, d, i)::T
    t isa IntervalType && return _typedvalue(T, t, f, d, i)::T
    t isa ViewType && return _typedvalue(T, t, f, d, i)::T
    t isa NullType && return _typedvalue(T, t, f, d, i)::T
    return _typedchildbox(T, f, d, i)::T
end

function _typedchildbox(::Type{T}, f::Field, d::ArrayData, i::Int64) where {T}
    return _typedvalue_of(T, d.type, f, d, i)::T
end

# The same closed-set ladder as `_value_of`, with the claimed type threaded.
@inline function _typedvalue_of(
    ::Type{T},
    t::ArrowType,
    f::Field,
    d::ArrayData,
    i::Int64,
) where {T}
    t isa IntType && return _typedvalue(T, t, f, d, i)
    t isa FloatType && return _typedvalue(T, t, f, d, i)
    t isa Utf8Type && return _typedvalue(T, t, f, d, i)
    t isa BoolType && return _typedvalue(T, t, f, d, i)
    t isa ListType && return _typedvalue(T, t, f, d, i)
    t isa StructType && return _typedvalue(T, t, f, d, i)
    # Wrapper branches keep the claim intact, so they alone can recurse
    # with an UNCHANGED signature: the ::T assert stops that cycle from
    # widening every other branch to Any in fresh-process inference; the
    # wrapper read itself pays one box.
    t isa DictionaryType && return _typedvalue(T, t, f, d, i)::T
    t isa TimestampType && return _typedvalue(T, t, f, d, i)
    t isa DateType && return _typedvalue(T, t, f, d, i)
    t isa TimeType && return _typedvalue(T, t, f, d, i)
    t isa DurationType && return _typedvalue(T, t, f, d, i)
    t isa BinaryType && return _typedvalue(T, t, f, d, i)
    t isa FixedSizeBinaryType && return _typedvalue(T, t, f, d, i)
    t isa FixedSizeListType && return _typedvalue(T, t, f, d, i)
    t isa MapType && return _typedvalue(T, t, f, d, i)
    t isa UnionType && return _typedvalue(T, t, f, d, i)
    t isa DecimalType && return _typedvalue(T, t, f, d, i)
    t isa IntervalType && return _typedvalue(T, t, f, d, i)
    t isa NullType && return _typedvalue(T, t, f, d, i)
    t isa ViewType && return _typedvalue(T, t, f, d, i)
    t isa ListViewType && return _typedvalue(T, t, f, d, i)
    t isa RunEndEncodedType && return _typedvalue(T, t, f, d, i)::T
    throw(ArgumentError("unregistered ArrowType"))
end

# Closed leaf claims (scalars plus the `Map` row vector): the claim must
# equal the layout's `juliatype`
# exactly; the audited dynamic extraction runs and the assert makes the
# result statically typed (and free when the claim is right).
function _typedvalue(
    ::Type{T},
    t::Union{
        IntType,
        FloatType,
        BoolType,
        Utf8Type,
        BinaryType,
        FixedSizeBinaryType,
        TimestampType,
        DateType,
        TimeType,
        DurationType,
        ViewType,
        DecimalType,
        IntervalType,
        MapType,
    },
    f::Field,
    d::ArrayData,
    i::Int64,
) where {T}
    isvalid_at(d, i) || return _typedmissing(T, f)
    E = Base.nonmissingtype(T)
    E === juliatype(t) || _typedrefuse(E, _layoutname(t), f)
    # Invalid int/float widths fall through juliatype's 64-bit fallback:
    # refuse them here (managed, fails closed) rather than let the raw
    # extraction's own width ladder produce a mistyped value.
    (t isa IntType || t isa FloatType) &&
        primwidth(t) != Int64(sizeof(E)) &&
        _typedrefuse(E, _layoutname(t), f)
    return _value(t, f, d, i)::E
end

function _typedvalue(
    ::Type{T},
    t::Union{ListType,ListViewType},
    f::Field,
    d::ArrayData,
    i::Int64,
) where {T}
    isvalid_at(d, i) || return _typedmissing(T, f)
    E = Base.nonmissingtype(T)
    E <: Vector || _typedrefuse(E, _layoutname(t), f)
    if t isa ListType
        lo, hi = _offsets_at(d, i, layoutspec(t).offsetwidth == 8)
        off = lo
        n = hi - lo
    else
        off, n = _listview_range(t, d, i)
        (off >= 0 && n >= 0) ||
            throw(ValidationError("list-view offset and size must be non-negative"))
    end
    child, cf = d.children[1], f.children[1]
    CE = eltype(E)
    out = Vector{CE}(undef, Int(n))
    for k = 1:Int(n)
        out[k] = _typedchild(CE, cf, child, checked_add(off, Int64(k)))
    end
    return out
end

function _typedvalue(
    ::Type{T},
    t::FixedSizeListType,
    f::Field,
    d::ArrayData,
    i::Int64,
) where {T}
    isvalid_at(d, i) || return _typedmissing(T, f)
    E = Base.nonmissingtype(T)
    E <: Vector || _typedrefuse(E, _layoutname(t), f)
    child, cf = d.children[1], f.children[1]
    base = checked_mul(_slotindex0(d, i), Int64(t.listsize))
    CE = eltype(E)
    out = Vector{CE}(undef, t.listsize)
    for j = 1:(t.listsize)
        out[j] = _typedchild(CE, cf, child, checked_add(base, Int64(j)))
    end
    return out
end

function _typedvalue(::Type{T}, t::StructType, f::Field, d::ArrayData, i::Int64) where {T}
    isvalid_at(d, i) || return _typedmissing(T, f)
    E = Base.nonmissingtype(T)
    E === Vector{Pair{String,Any}} && return _value(t, f, d, i)::E
    E <: NamedTuple || _typedrefuse(E, _layoutname(t), f)
    return _structrow(E, f, d, checked_add(d.offset, i))
end

# Generated so every field's claim is a LITERAL type and the row build is
# a flat tuple expression: an `ntuple(Val(N))` closure erases per-field
# types to `NTuple{N,Any}` at arity >= 4, and index-recursion trips the
# inference recursion limiter. The preflight already checked names.
@generated function _structrow(
    ::Type{E},
    f::Field,
    d::ArrayData,
    childindex::Int64,
) where {E<:NamedTuple}
    vals = Expr[
        :(_typedchild($(fieldtype(E, j)), f.children[$j], d.children[$j], childindex))
        for j = 1:fieldcount(E)
    ]
    return :(E(($(vals...),)))
end

function _typedvalue(
    ::Type{T},
    t::DictionaryType,
    f::Field,
    d::ArrayData,
    i::Int64,
) where {T}
    isvalid_at(d, i) || return _typedmissing(T, f)
    w = primwidth(t.indextype)
    idx = _load_int(rolebuffer(d, DATA), t.indextype, _slotbyteoff(d, i, w))
    dict = d.dictionary
    dict === nothing &&
        throw(ValidationError("dictionary-encoded array without a dictionary"))
    return _typedchild(T, dictvaluefield(f, t), dict, checked_add(Int64(idx), Int64(1)))
end

_typedvalue(::Type{T}, t::RunEndEncodedType, f::Field, d::ArrayData, i::Int64) where {T} =
    _typedchild(T, f.children[2], d.children[2], _ree_runindex(d, i))

_typedvalue(::Type{T}, ::NullType, f::Field, ::ArrayData, ::Int64) where {T} =
    _typedmissing(T, f)

# Union rows take the WINNING child's runtime type: no static claim can
# hold across children, so only the dynamic path reads unions.
_typedvalue(::Type{T}, t::UnionType, f::Field, ::ArrayData, ::Int64) where {T} =
    _typedrefuse(Base.nonmissingtype(T), _layoutname(t), f)

@inline function _typedmaterialize_of(
    ::Type{T},
    t::ArrowType,
    f::Field,
    d::ArrayData,
) where {T}
    t isa IntType && return _typedmaterialize_loop(T, t, f, d)
    t isa FloatType && return _typedmaterialize_loop(T, t, f, d)
    t isa Utf8Type && return _typedmaterialize_loop(T, t, f, d)
    t isa BoolType && return _typedmaterialize_loop(T, t, f, d)
    t isa ListType && return _typedmaterialize_loop(T, t, f, d)
    t isa StructType && return _typedmaterialize_loop(T, t, f, d)
    t isa DictionaryType && return _typedmaterialize_loop(T, t, f, d)
    t isa TimestampType && return _typedmaterialize_loop(T, t, f, d)
    t isa DateType && return _typedmaterialize_loop(T, t, f, d)
    t isa TimeType && return _typedmaterialize_loop(T, t, f, d)
    t isa DurationType && return _typedmaterialize_loop(T, t, f, d)
    t isa BinaryType && return _typedmaterialize_loop(T, t, f, d)
    t isa FixedSizeBinaryType && return _typedmaterialize_loop(T, t, f, d)
    t isa FixedSizeListType && return _typedmaterialize_loop(T, t, f, d)
    t isa MapType && return _typedmaterialize_loop(T, t, f, d)
    t isa UnionType && return _typedmaterialize_loop(T, t, f, d)
    t isa DecimalType && return _typedmaterialize_loop(T, t, f, d)
    t isa IntervalType && return _typedmaterialize_loop(T, t, f, d)
    t isa NullType && return _typedmaterialize_loop(T, t, f, d)
    t isa ViewType && return _typedmaterialize_loop(T, t, f, d)
    t isa ListViewType && return _typedmaterialize_loop(T, t, f, d)
    t isa RunEndEncodedType && return _typedmaterialize_loop(T, t, f, d)
    throw(ArgumentError("unregistered ArrowType"))
end

function _typedmaterialize_loop(
    ::Type{T},
    t::TT,
    f::Field,
    d::ArrayData,
) where {T,TT<:ArrowType}
    bulk = _bulkmaterialize(T, t, f, d)
    bulk === nothing || return bulk::Vector{T}
    out = Vector{T}(undef, d.len)
    for i = 1:(d.len)
        out[i] = _typedvalue(T, t, f, d, Int64(i))
    end
    return out
end

# ---------------------------------------------------------------------------
# Bulk fixed-width extraction: for closed isbits claims over plain
# fixed-width layouts, one bounds-checked byte copy replaces ten million
# per-element calls (the benchmark-dominant cost of materializing reads).
# Nulls punch in afterward from the validity bitmap. Everything else
# (strings, composites, bitmaps, decimal-as-bytes) keeps the element loop.
# ---------------------------------------------------------------------------

_bulkmaterialize(::Type{T}, ::ArrowType, ::Field, ::ArrayData) where {T} = nothing

function _bulkmaterialize(
    ::Type{T},
    t::Union{IntType,FloatType,TimestampType,DateType,TimeType,DurationType,DecimalType},
    f::Field,
    d::ArrayData,
) where {T}
    E = Base.nonmissingtype(T)
    isbitstype(E) || return nothing
    E === juliatype(t) || return nothing
    w = Int64(sizeof(E))
    # The claim's byte size must equal the DESCRIPTOR's layout width: an
    # invalid 24-bit descriptor falls through juliatype's fallback to a
    # 64-bit Julia type, and copying at the claim's width would misread —
    # such descriptors take the element loop (and validation refuses them).
    primwidth(t) == w || return nothing
    n = d.len
    # The typed path serves unvalidated data too: subslice re-checks the
    # extraction window against the buffer's declared bounds.
    src = subslice(rolebuffer(d, DATA), checked_mul(d.offset, w), checked_mul(n, w))
    vals = Vector{E}(undef, n)
    if n > 0
        GC.@preserve vals d begin
            unsafe_copyto!(Ptr{UInt8}(pointer(vals)), sliceptr(src), Int(src.len))
        end
    end
    # The BITMAP is the validity authority, exactly as per-element access:
    # a caller-supplied null-count cache is only certified after semantic
    # validation, and this path explicitly serves unvalidated data.
    v = validitybuffer(d)
    # When the claim admits no Missing, E === T, so `vals` already has the
    # public element type and needs no copy.
    if !(Missing <: T)
        if !isempty_buffer(v)
            for i = 1:n
                isvalid_at(d, Int64(i)) || _typednullrefuse(f)
            end
        end
        return vals
    end
    out = Vector{T}(undef, n)
    copyto!(out, vals)
    isempty_buffer(v) && return out
    for i = 1:n
        isvalid_at(d, Int64(i)) || (out[i] = missing)
    end
    return out
end

# ---------------------------------------------------------------------------
# §7 Builders: Julia data -> (Field, ArrayData)
# ---------------------------------------------------------------------------

# The write-side counterpart, kept intentionally small: enough construction
# machinery to build every implemented layout without an IPC file in the
# loop. These are "zero-copy wrap + bitmap build" fast paths; the
# append-oriented builder layer is the facade's.

@inline function _setbitmapbit!(bytes::Vector{UInt8}, i::Int)
    bytes[1 + ((i - 1) >> 3)] |= UInt8(1) << ((i - 1) & 7)
    return nothing
end

function _bitmapbuffer(present::AbstractVector{Bool})
    any(!, present) || return BufferSlice()   # no nulls -> canonical empty
    bytes = zeros(UInt8, expected_validity_bytes(Int64(length(present))))
    for (i, p) in enumerate(present)
        p && _setbitmapbit!(bytes, i)
    end
    return BufferSlice(heapregion(bytes), 0, length(bytes))
end

_databuffer(v::Vector{T}) where {T} = BufferSlice(heapregion(v), 0, sizeof(v))

"Build Boolean data and validity buffers directly in Arrow's bit-packed form."
function _build_bool(name, v::Vector{T}; nullable::Bool) where {T<:Union{Missing,Bool}}
    bytes = zeros(UInt8, expected_validity_bytes(Int64(length(v))))
    nc = 0
    for (i, x) in enumerate(v)
        if x === missing
            nc += 1
        elseif x
            _setbitmapbit!(bytes, i)
        end
    end

    validity = if nc == 0
        BufferSlice()
    else
        validbytes = zeros(UInt8, length(bytes))
        for (i, x) in enumerate(v)
            x === missing || _setbitmapbit!(validbytes, i)
        end
        _databuffer(validbytes)
    end
    t = BoolType()
    return Field(name, t; nullable=nullable),
    ArrayData(t, length(v), [validity, _databuffer(bytes)]; nullcount=nc)
end

"Build non-null Boolean data through a caller-supplied direct bit fill."
function _build_bool(name, len::Int, fillbits!::F; nullable::Bool) where {F}
    len >= 0 || throw(ArgumentError("Boolean array length is negative"))
    bytes = zeros(UInt8, expected_validity_bytes(Int64(len)))
    fillbits!(bytes)
    t = BoolType()
    return Field(name, t; nullable=nullable),
    ArrayData(t, len, [BufferSlice(), _databuffer(bytes)]; nullcount=0)
end

arrowtype_for(::Type{Bool}) = BoolType()
arrowtype_for(::Type{T}) where {T<:Signed} = IntType(8 * sizeof(T), true)
arrowtype_for(::Type{T}) where {T<:Unsigned} = IntType(8 * sizeof(T), false)
arrowtype_for(::Type{Float16}) = FloatType(16)
arrowtype_for(::Type{Float32}) = FloatType(32)
arrowtype_for(::Type{Float64}) = FloatType(64)

"""
    fromjulia(name, v) -> (Field, ArrayData)

Adapt a Julia vector to Core form. `Vector{T}` for fixed-width isbits `T`
other than `Bool` is a ZERO-COPY wrap (the vector becomes the region's root;
scoped-borrow contract: don't resize/mutate while in use). `Bool`
(bit-packed), `Union{T,Missing}`, and String inputs build fresh buffers.
"""
function fromjulia(name, v::Vector{T}) where {T}
    if T === Union{}
        throw(
            ArgumentError(
                "fromjulia: bottom element type Union{} has no Arrow type; " *
                "give the empty vector a declared element type",
            ),
        )
    elseif T <:
           Union{Int8,Int16,Int32,Int64,UInt8,UInt16,UInt32,UInt64,Float16,Float32,Float64}
        t = arrowtype_for(T)
        return Field(name, t; nullable=false),
        ArrayData(t, length(v), [BufferSlice(), _databuffer(v)]; nullcount=0)
    elseif T == Bool
        return _build_bool(name, v; nullable=false)
    elseif T == String
        return _build_strings(name, v)
    elseif T <: Union{
        Missing,
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float16,
        Float32,
        Float64,
        Bool,
    }
        return _build_nullable_primitive(name, v)
    elseif T <: Union{Missing,String}
        return _build_strings(name, v)
    elseif T <: AbstractVector || T <: Union{Missing,<:AbstractVector}
        return _build_list(name, v)
    else
        throw(ArgumentError("fromjulia: unsupported element type $T"))
    end
end

function _build_nullable_primitive(name, v::Vector{T}; nullable::Bool=true) where {T}
    S = Base.nonmissingtype(T)
    if S === Union{}
        t = NullType()
        return Field(name, t; nullable=true),
        ArrayData(t, length(v), BufferSlice[]; nullcount=length(v))
    end
    S === Bool && return _build_bool(name, v; nullable=nullable)
    t = arrowtype_for(S)
    present = [x !== missing for x in v]
    validity = _bitmapbuffer(present)
    vals = S[x === missing ? zero(S) : S(x) for x in v]
    data = _databuffer(vals)
    nc = count(!, present)
    # Nullability is the DECLARED element type's, not the observed count's:
    # a Union{Missing,T} column with no missing values is still nullable.
    return Field(name, t; nullable=nullable),
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
        nbytes <= typemax(Int32) ||
            throw(ArgumentError("column $name exceeds the Int32 offset range"))
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
    return Field(name, t; nullable=eltype(v) >: Missing),
    ArrayData(
        t,
        length(v),
        [_bitmapbuffer(present), _databuffer(offsets), data];
        nullcount=nc,
    )
end

function _build_list(name, v::Vector)
    present = [x !== missing for x in v]
    offsets = Vector{Int32}(undef, length(v) + 1)
    offsets[1] = 0
    total = 0
    for (i, x) in enumerate(v)
        total += x === missing ? 0 : length(x)
        total <= typemax(Int32) ||
            throw(ArgumentError("column $name exceeds the Int32 offset range"))
        offsets[i + 1] = Int32(total)
    end
    nonmissing = [x for x in v if x !== missing]
    childtype = eltype(Base.nonmissingtype(eltype(v)))
    flat = isempty(nonmissing) ? childtype[] : reduce(vcat, nonmissing)
    cf, cd = fromjulia("item", collect(flat))
    nc = count(!, present)
    t = ListType(false)
    return Field(name, t; nullable=eltype(v) >: Missing, children=[cf]),
    ArrayData(
        t,
        length(v),
        [_bitmapbuffer(present), _databuffer(offsets)];
        children=[cd],
        nullcount=nc,
    )
end

"""
    fromjulia_struct(name, nt::NamedTuple) -> (Field, ArrayData)

Build a struct column from equal-length child vectors (no top-level nulls).
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
    ArrayData(
        t,
        length(indices0),
        [_bitmapbuffer(present), _databuffer(inds)];
        dictionary=vd,
        nullcount=nc,
    )
end

"""
    fromviewentries(name, payloads::Vector{P}, buffers::Vector{Vector{UInt8}}; nullable=true) -> (Field, ArrayData)
    fromviewentries(name, payloads::Vector{P}, buf, extra; nullable=true)

Wrap a vector of Arrow view entries as a Utf8View column, ZERO-COPY. `P` is
any 16-byte isbits type whose values are Arrow StringView entries — the
representation ArrowStrings' `ArrowString` columns use:

    bytes 0..3    Int32 content length (negative marks a null slot)
    bytes 4..15   the content, zero-padded            (length ≤ 12)
    bytes 4..7    the content's 4-byte prefix          (length > 12)
    bytes 8..11   Int32 buffer index into `buffers` (0-based)
    bytes 12..15  Int32 0-based byte offset within that buffer

`payloads` becomes the views buffer and `buffers` the variadic data buffers,
in order, without copying (the two-buffer form is `[buf, extra]`; a buffer
may be empty, and an all-inline column may have none at all).

The only work is the validity bitmap: a slot whose length is negative is
null; the spec leaves a null slot's entry bytes unspecified, and neither
this reader's nor the reference implementation's validation reads them.
Long-entry geometry (offsets inside their buffer, prefixes matching the
data) is checked where every builder's is — by
`validate_semantic`/`validate_full` — not here. The scoped-borrow rule of
every zero-copy wrap applies to every vector passed in.
"""
fromviewentries(
    name,
    payloads::Vector{P},
    buf::Vector{UInt8},
    extra::Vector{UInt8};
    nullable::Bool=true,
) where {P} = fromviewentries(name, payloads, Vector{UInt8}[buf, extra]; nullable=nullable)

function fromviewentries(
    name,
    payloads::Vector{P},
    buffers::Vector{Vector{UInt8}};
    nullable::Bool=true,
) where {P}
    isbitstype(P) && sizeof(P) == 16 ||
        throw(ArgumentError("view-entry payloads must be a 16-byte isbits type"))
    # The entry words are values assembled by shifts; Arrow's byte layout is
    # what those values spell out on a little-endian host, and Core reads
    # view entries host-natively.
    _native_endianness() == LittleEndian ||
        throw(ArgumentError("fromviewentries requires a little-endian host"))
    n = length(payloads)
    present = Vector{Bool}(undef, n)
    nnull = 0
    GC.@preserve payloads begin
        src = Ptr{Int32}(pointer(payloads))
        for i = 1:n
            ok = unsafe_load(src, 4 * i - 3) >= 0     # entry i's length word
            present[i] = ok
            nnull += !ok
        end
    end
    t = ViewType(true)
    slices = BufferSlice[_bitmapbuffer(present), _databuffer(payloads)]
    for b in buffers
        push!(slices, _databuffer(b))
    end
    return Field(name, t; nullable=nullable), ArrayData(t, n, slices; nullcount=nnull)
end

# ---------------------------------------------------------------------------
# §8 RecordBatch + source protocol
# ---------------------------------------------------------------------------

"""
    RecordBatch

Schema + equal-length columns: the interchange unit between Core and every
adapter. The IPC and C-stream adapters produce and consume batches; chunked
columns are a facade convenience over them.
"""
struct RecordBatch
    schema::Schema
    columns::FrozenVector{ArrayData}
    nrows::Int64
    function RecordBatch(
        schema::Schema,
        columns,
        nrows::Integer,
        validated_dictionaries::Union{Nothing,_ValidatedDictionaries}=nothing,
    )
        _validate_schema(schema)
        cols = FrozenVector{ArrayData}(columns)
        n = Int64(nrows)
        n >= 0 || throw(ArgumentError("negative row count"))
        length(schema.fields) == length(cols) ||
            throw(ArgumentError("schema/column count mismatch"))
        for (f, c) in zip(schema.fields, cols)
            length(c) == n || throw(ArgumentError("unequal column lengths"))
            _validate_structural(f, c, validated_dictionaries)
        end
        return new(schema, cols, n)
    end
end
RecordBatch(schema::Schema, columns) =
    RecordBatch(schema, columns, isempty(columns) ? 0 : length(first(columns)))

"Build a low-level `RecordBatch` from a `NamedTuple` of Julia vectors."
function batch(nt::NamedTuple)
    pairs = [fromjulia(String(k), v) for (k, v) in Base.pairs(nt)]
    sch = Schema([p[1] for p in pairs])
    return RecordBatch(sch, [p[2] for p in pairs])
end

"""
    RecordBatchSource

The shared pull-iteration protocol: implement
`nextbatch!(src) -> Union{Nothing,RecordBatch}` and `schema(src)`. The IPC
reader and C-stream importer present this shape, so a writer or dataset
layer need not know which adapter produced the stream.
"""
abstract type RecordBatchSource end
function nextbatch! end
schema(src::RecordBatchSource) =
    error("RecordBatchSource implementations must define schema(src)")

end # module ArrowCore
