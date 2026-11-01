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
#   * Export: every C node of one export (a schema tree, an array tree, or a
#     stream) carries in `private_data` a pointer to one malloc'd count of the
#     nodes the consumer has not released. A release callback releases the
#     children and dictionary still in place, marks the node released, and
#     counts it off. It takes no lock, allocates nothing, and touches no Julia
#     object, because consumers call it from their own threads and from inside
#     GC finalizers. The Julia side keeps each export's roots (the columns,
#     whose regions keep the buffers alive) and mallocs in `EXPORTS` until the
#     count reaches zero; later exports or garbage collections free them.
#
#   * Import: the moved ArrowArray becomes ONE ForeignOwner shared by every
#     child/dictionary BufferSlice (a single release for the whole tree).
#     Buffer extents are DECLARED, not verified: the ABI cannot prove
#     allocation sizes, so extents are computed from length/offset/layout, and
#     offsets buffers are read (bounded by their computed size) to size the
#     data buffers they govern. Failed imports release the moved structure
#     exactly once before throwing. Per spec, moving marks the source released
#     (release = NULL). Validity is reachability: every imported region's
#     `root` is the ForeignOwner, so the producer's memory outlives every slice
#     by construction, and every region shares one `ReleaseCell`, so
#     `release!` on the owner or on any region revokes all siblings (later
#     access is a clean `InvalidStateException`) and then runs the producer's
#     release exactly once.
#
#   * Streams: `ArrowArrayStream` maps in both directions. An exported stream
#     is one export whose `get_schema` and `get_next` results are exports of
#     their own; an imported stream is a moved struct whose batches import as
#     arrays do.
# =============================================================================

# ---------------------------------------------------------------------------
# ABI structs (field-exact per https://arrow.apache.org/docs/format/CDataInterface.html)
# ---------------------------------------------------------------------------

"""
    Arrow.CArrowSchema

The C data interface's `ArrowSchema` struct, field for field.
"""
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

"""
    Arrow.CArrowArray

The C data interface's `ArrowArray` struct, field for field.
"""
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
const ARROW_FLAG_ALL_SUPPORTED =
    ARROW_FLAG_NULLABLE | ARROW_FLAG_DICTIONARY_ORDERED | ARROW_FLAG_MAP_KEYS_SORTED

# Store one field of a C struct in place (structs are immutable in Julia;
# the C memory is not).
@generated function _store_field!(p::Ptr{T}, ::Val{name}, v) where {T,name}
    i = findfirst(==(name), fieldnames(T))
    off = fieldoffset(T, i)
    FT = fieldtype(T, i)
    return :(unsafe_store!(Ptr{$FT}(Ptr{Cvoid}(p) + $off), convert($FT, v)); nothing)
end
_store_field!(p, name::Symbol, v) = _store_field!(p, Val(name), v)

# Call the release callback of the C struct at `p`, if it still has one.
function _release_c!(p::Ptr{T}) where {T}
    r = unsafe_load(p).release
    r == C_NULL || ccall(r, Cvoid, (Ptr{T},), p)
    return nothing
end

# Release a producer's struct and check that its callback marked the struct
# released, as the interface requires.
function _release_producer!(p::Ptr{T}) where {T}
    _release_c!(p)
    unsafe_load(p).release == C_NULL ||
        error("C Data producer release did not mark the structure released")
    return nothing
end

# ---------------------------------------------------------------------------
# Format strings <-> Core descriptors (parity with Core's accessor set)
# ---------------------------------------------------------------------------

_tuchar(u) =
    u == AC.SECOND ? "s" : u == AC.MILLISECOND ? "m" : u == AC.MICROSECOND ? "u" : "n"

function formatstring(t::IntType)
    t.bits == 128 && throw(
        ArgumentError(
            "the C data interface has no 128-bit integer format; " *
            "convert the column to a decimal or fixed-size binary first",
        ),
    )
    return (
        t.signed ? Dict(8 => "c", 16 => "s", 32 => "i", 64 => "l") :
        Dict(8 => "C", 16 => "S", 32 => "I", 64 => "L")
    )[t.bits]
end
formatstring(t::FloatType) = Dict(16 => "e", 32 => "f", 64 => "g")[t.bits]
formatstring(::BoolType) = "b"
formatstring(::NullType) = "n"
formatstring(t::Utf8Type) = t.large ? "U" : "u"
formatstring(t::BinaryType) = t.large ? "Z" : "z"
formatstring(t::FixedSizeBinaryType) = "w:$(t.nbytes)"
formatstring(t::DecimalType) =
    t.bits == 128 ? "d:$(t.precision),$(t.scale)" : "d:$(t.precision),$(t.scale),$(t.bits)"
formatstring(t::DateType) = t.unit == AC.DAY ? "tdD" : "tdm"
formatstring(t::TimeType) = "tt" * _tuchar(t.unit)
formatstring(t::TimestampType) = "ts" * _tuchar(t.unit) * ":" * something(t.timezone, "")
formatstring(t::DurationType) = "tD" * _tuchar(t.unit)
formatstring(t::IntervalType) =
    t.unit == AC.YEAR_MONTH ? "tiM" : t.unit == AC.DAY_TIME ? "tiD" : "tin"
formatstring(t::ListType) = t.large ? "+L" : "+l"
formatstring(t::FixedSizeListType) = "+w:$(t.listsize)"
formatstring(::StructType) = "+s"
formatstring(::MapType) = "+m"
formatstring(t::UnionType) =
    (t.mode == AC.SparseMode ? "+us:" : "+ud:") * join(Int.(t.typeids), ",")
formatstring(t::ViewType) = t.utf8 ? "vu" : "vz"
formatstring(t::ListViewType) = t.large ? "+vL" : "+vl"
formatstring(::RunEndEncodedType) = "+r"

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
    # per spec: the index format on the node; values on schema.dictionary
    t isa DictionaryType && return formatstring_of(t.indextype)
    throw(ArgumentError("unregistered ArrowType"))
end

_formaterror(fmt) = throw(ValidationError("unsupported C format string \"$fmt\""))

function _parseformatint(fmt, s, what; low=0, high=typemax(Int32))
    bytes = codeunits(s)
    isempty(bytes) && throw(ValidationError("invalid $what in C format string \"$fmt\""))
    firstdigit = 1
    if bytes[1] == UInt8('-')
        low < 0 || throw(ValidationError("invalid $what in C format string \"$fmt\""))
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

_parsetimeunit(fmt, c) =
    c == UInt8('s') ? AC.SECOND :
    c == UInt8('m') ? AC.MILLISECOND :
    c == UInt8('u') ? AC.MICROSECOND : c == UInt8('n') ? AC.NANOSECOND : _formaterror(fmt)

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
            value <= 127 || throw(ValidationError("union type ids must be in [0, 127]"))
        elseif b == UInt8(',')
            have_digit ||
                throw(ValidationError("invalid union type id in C format string \"$fmt\""))
            bit = UInt128(1) << value
            seen & bit == 0 || throw(ValidationError("union type ids must be unique"))
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
    m = Dict(
        "c" => (8, true),
        "C" => (8, false),
        "s" => (16, true),
        "S" => (16, false),
        "i" => (32, true),
        "I" => (32, false),
        "l" => (64, true),
        "L" => (64, false),
    )
    haskey(m, fmt) && return IntType(m[fmt]...)
    if ncodeunits(fmt) == 3 && startswith(fmt, "tt")
        u = _parsetimeunit(fmt, codeunit(fmt, 3))
        return TimeType(u, u == AC.SECOND || u == AC.MILLISECOND ? 32 : 64)
    end
    ncodeunits(fmt) == 3 &&
        startswith(fmt, "tD") &&
        return DurationType(_parsetimeunit(fmt, codeunit(fmt, 3)))
    if startswith(fmt, "ts") && ncodeunits(fmt) >= 4 && codeunit(fmt, 4) == UInt8(':')
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
        scale = _parseformatint(fmt, parts[2], "decimal scale"; low=typemin(Int32))
        bits =
            length(parts) == 3 ? _parseformatint(fmt, parts[3], "decimal bit width") : 128
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
# Export: Core -> C structs, one live-node count per export
# ---------------------------------------------------------------------------

# One export's Julia roots and native allocations. `live` is a malloc'd count
# of the C nodes the consumer has not released; every node's `private_data`
# points at it, so a release callback needs no Julia object. The 8 bytes after
# the count hold an exported stream's last error message. `seen` is set by the
# reap that first finds every node released; the next reap frees the export.
mutable struct Export
    const roots::Vector{Any}
    const mallocs::Vector{Ptr{Cvoid}}
    const live::Ptr{Int64}
    seen::Bool
end

const EXPORTS = Dict{Ptr{Int64},Export}()
const REGISTRY_LOCK = ReentrantLock()
# Exports left to make before one reaps. A reap scans every registered export, so it
# sets this to the number still unreleased (at least 64), and reaping costs constant
# time per export however many exports a consumer holds.
const _EXPORTS_UNTIL_REAP = Ref(64)

function _newexport(roots::Vector{Any})
    reap = lock(REGISTRY_LOCK) do
        return (_EXPORTS_UNTIL_REAP[] -= 1) <= 0
    end
    reap && _reap!()
    live = Ptr{Int64}(Libc.calloc(2, 8))
    live == C_NULL && throw(OutOfMemoryError())
    return Export(roots, Ptr{Cvoid}[live], live, false)
end

function _malloc!(e::Export, n::Integer)
    p = Libc.malloc(max(Int(n), 1))
    p == C_NULL && throw(OutOfMemoryError())
    push!(e.mallocs, p)
    return p
end

function _free!(e::Export)
    message = unsafe_load(Ptr{Ptr{UInt8}}(e.live + 8))
    message == C_NULL || Libc.free(message)
    foreach(Libc.free, e.mallocs)
    empty!(e.mallocs)
    empty!(e.roots)
    return nothing
end

# An object whose finalizer reaps and then registers itself again, so a
# consumer's releases free memory at later garbage collections even when
# nothing exports again. Created once, by the first export.
mutable struct _ReapHook end
const _REAP_HOOK_ARMED = Ref(false)
# Set when Julia starts exiting; see `_finalizeowner`.
const _EXITING = Ref(false)
function _setexiting()
    _EXITING[] = true
    return nothing
end

function __init__()
    # No export outlives the process that made it: drop what a precompile
    # workload left (its pointers come back NULL), and re-arm the reap hook,
    # whose finalizer is not saved.
    empty!(EXPORTS)
    _EXPORTS_UNTIL_REAP[] = 64
    _REAP_HOOK_ARMED[] = false
    atexit(_setexiting)
    return nothing
end

function _reaphook(h::_ReapHook)
    finalizer(_reaphook, h)
    trylock(REGISTRY_LOCK) || return nothing
    try
        _reap!()
    finally
        unlock(REGISTRY_LOCK)
    end
    return nothing
end

function _register!(e::Export)
    lock(REGISTRY_LOCK) do
        EXPORTS[e.live] = e
        if !_REAP_HOOK_ARMED[]
            _REAP_HOOK_ARMED[] = true
            finalizer(_reaphook, _ReapHook())
        end
    end
    return nothing
end

# Free every export that an earlier reap found with all its C nodes released, mark
# the ones released since, and return how many it freed. A release callback counts
# its node off just before it returns, and the consumer may then read the struct it
# released, so the reap that first sees an export released keeps it. Runs from
# `_newexport` (see `_EXPORTS_UNTIL_REAP`) and from `_ReapHook`, so no caller needs to.
function _reap!()
    done = Export[]
    lock(REGISTRY_LOCK) do
        unreleased = 0
        for e in values(EXPORTS)
            if unsafe_load(e.live, :sequentially_consistent) != 0
                unreleased += 1
            elseif e.seen
                push!(done, e)
            else
                e.seen = true
            end
        end
        for e in done
            delete!(EXPORTS, e.live)
        end
        _EXPORTS_UNTIL_REAP[] = max(64, unreleased)
        return nothing
    end
    foreach(_free!, done)
    return length(done)
end

# Release callbacks do only native work: release the children and dictionary
# still in place (a moved one has `release == NULL`), mark the node released,
# and count it off its export. No lock, no allocation, no Julia object, and
# nothing that can throw: consumers call them from their own threads, and
# PythonCall runs pyarrow's releases from GC finalizers.
function _release_array(a::Ptr{CArrowArray})::Cvoid
    arr = unsafe_load(a)
    arr.release == C_NULL && return nothing
    for i = 1:arr.n_children
        _release_c!(unsafe_load(arr.children, i))
    end
    arr.dictionary == C_NULL || _release_c!(arr.dictionary)
    _store_field!(a, Val(:release), Ptr{Cvoid}(C_NULL))
    Base.unsafe_modify!(Ptr{Int64}(arr.private_data), -, 1, :sequentially_consistent)
    return nothing
end

function _release_schema(s::Ptr{CArrowSchema})::Cvoid
    sch = unsafe_load(s)
    sch.release == C_NULL && return nothing
    for i = 1:sch.n_children
        _release_c!(unsafe_load(sch.children, i))
    end
    sch.dictionary == C_NULL || _release_c!(sch.dictionary)
    _store_field!(s, Val(:release), Ptr{Cvoid}(C_NULL))
    Base.unsafe_modify!(Ptr{Int64}(sch.private_data), -, 1, :sequentially_consistent)
    return nothing
end

# Field metadata per the C data interface: int32 pair count, then per pair an
# int32 key length, key bytes, int32 value length, value bytes (native
# endian, not NUL-terminated). NULL when there is no metadata.
function _cmetadata!(
    e::Export,
    metadata::Union{Nothing,AC.FrozenVector{Pair{String,String}}},
)::Ptr{UInt8}
    (metadata === nothing || isempty(metadata)) && return Ptr{UInt8}(C_NULL)
    buf = UInt8[]
    append!(buf, reinterpret(UInt8, Int32[Int32(length(metadata))]))
    for kv in metadata
        k = first(kv)
        v = last(kv)
        append!(buf, reinterpret(UInt8, Int32[Int32(sizeof(k))]))
        append!(buf, codeunits(k))
        append!(buf, reinterpret(UInt8, Int32[Int32(sizeof(v))]))
        append!(buf, codeunits(v))
    end
    p = Ptr{UInt8}(_malloc!(e, length(buf)))
    GC.@preserve buf unsafe_copyto!(p, pointer(buf), length(buf))
    return p
end

function _cstring!(e::Export, s::String)
    isvalid(s) || throw(ValidationError("C Data strings must be valid UTF-8"))
    occursin('\0', s) &&
        throw(ValidationError("C Data strings cannot contain embedded NUL characters"))
    n = ncodeunits(s)
    p = Ptr{UInt8}(_malloc!(e, n + 1))
    GC.@preserve s unsafe_copyto!(p, pointer(s), n)
    unsafe_store!(p, 0x00, n + 1)
    return p
end

# A dictionary Field's extension pairs name its VALUE type, as in IPC. For a
# canonical extension (the format reserves the `arrow.` prefix for them), export
# them on the dependent value node, as the C++ bridge does for an extension type
# it knows: on the wrapper they would declare dictionary storage, which PyArrow
# refuses. Other extension names stay on the wrapper with the field's metadata,
# where a consumer that does not know them keeps them as field metadata (PyArrow
# drops unknown extension pairs on a dependent node).
# (Literal keys: the trim gate includes this file without arrowtypes.jl.)
_isextensionpair(kv) =
    first(kv) == "ARROW:extension:name" || first(kv) == "ARROW:extension:metadata"

_iscanonicalextension(kv) =
    first(kv) == "ARROW:extension:name" && startswith(last(kv), "arrow.")

function _splitdictmetadata(metadata::Union{Nothing,AC.FrozenVector{Pair{String,String}}})
    (metadata === nothing || !any(_iscanonicalextension, metadata)) &&
        return metadata, nothing
    wrapper = Pair{String,String}[kv for kv in metadata if !_isextensionpair(kv)]
    values = Pair{String,String}[kv for kv in metadata if _isextensionpair(kv)]
    return (isempty(wrapper) ? nothing : AC.FrozenVector{Pair{String,String}}(wrapper)),
    AC.FrozenVector{Pair{String,String}}(values)
end

# Export the schema tree of `f` into `at`, a consumer-owned struct, or into a
# malloc'd one. The node is stored last, after its children, so a failed
# export leaves `at` untouched.
function _export_schema!(
    e::Export,
    f::Field,
    release::Ptr{Cvoid};
    at::Ptr{CArrowSchema}=Ptr{CArrowSchema}(C_NULL),
    metadata::Union{Nothing,AC.FrozenVector{Pair{String,String}}}=f.metadata,
)::Ptr{CArrowSchema}
    p = at == C_NULL ? Ptr{CArrowSchema}(_malloc!(e, sizeof(CArrowSchema))) : at
    childfields = f.type isa DictionaryType ? Field[] : f.children
    childptrs = Ptr{Ptr{CArrowSchema}}(C_NULL)
    if !isempty(childfields)
        childptrs = Ptr{Ptr{CArrowSchema}}(_malloc!(e, length(childfields) * sizeof(Ptr)))
        for (i, cf) in enumerate(childfields)
            unsafe_store!(childptrs, _export_schema!(e, cf, release), i)
        end
    end
    dict = Ptr{CArrowSchema}(C_NULL)
    wrappermetadata = metadata
    if f.type isa DictionaryType
        wrappermetadata, valuemetadata = _splitdictmetadata(metadata)
        dict = _export_schema!(
            e,
            AC.dictvaluefield(f, f.type),
            release;
            metadata=valuemetadata,
        )
    end
    flags = f.nullable ? ARROW_FLAG_NULLABLE : Int64(0)
    f.type isa DictionaryType && f.type.ordered && (flags |= ARROW_FLAG_DICTIONARY_ORDERED)
    f.type isa MapType && f.type.keyssorted && (flags |= ARROW_FLAG_MAP_KEYS_SORTED)
    unsafe_store!(e.live, unsafe_load(e.live) + 1)
    unsafe_store!(
        p,
        CArrowSchema(
            _cstring!(e, formatstring_of(f.type)),
            _cstring!(e, f.name),
            # Field metadata rides the OUTER node for every field, dictionary
            # wrappers included; PyArrow imports only the wrapper's pairs.
            _cmetadata!(e, wrappermetadata),
            flags,
            length(childfields),
            childptrs,
            dict,
            release,
            Ptr{Cvoid}(e.live),
        ),
    )
    return p
end

function _export_array!(
    e::Export,
    d::ArrayData,
    release::Ptr{Cvoid};
    at::Ptr{CArrowArray}=Ptr{CArrowArray}(C_NULL),
)::Ptr{CArrowArray}
    p = at == C_NULL ? Ptr{CArrowArray}(_malloc!(e, sizeof(CArrowArray))) : at
    spec = AC.layoutspec_of(d.type)
    ncore = length(d.buffers)
    # C Data appends one int64 buffer of variadic data-buffer LENGTHS to view
    # arrays (extents are not otherwise recoverable from the ABI); it counts
    # toward n_buffers here and nowhere else in the format.
    nvariadic = spec.variadic ? ncore - length(spec.buffers) : 0
    nbuf = spec.variadic ? ncore + 1 : ncore
    bufptrs = Ptr{Ptr{Cvoid}}(_malloc!(e, max(nbuf, 1) * sizeof(Ptr)))
    for (i, b) in enumerate(d.buffers)
        role = i <= length(spec.buffers) ? spec.buffers[i] : AC.DATA
        bufferp =
            if role == AC.OFFSETS && d.len == 0 && d.offset == 0 && AC.isempty_buffer(b)
                # Core omits the offsets of an empty array; C Data still
                # expects the length+1 offsets, so export one zero.
                Libc.memset(_malloc!(e, spec.offsetwidth), 0, spec.offsetwidth)
            elseif AC.isempty_buffer(b)
                # An absent validity bitmap, or any zero-byte buffer, is NULL.
                Ptr{Cvoid}(C_NULL)
            else
                Ptr{Cvoid}(AC.sliceptr(b))
            end
        unsafe_store!(bufptrs, bufferp, i)
    end
    if spec.variadic
        sizesp = Ptr{Int64}(_malloc!(e, max(nvariadic, 1) * 8))
        for k = 1:nvariadic
            unsafe_store!(sizesp, d.buffers[length(spec.buffers) + k].len, k)
        end
        unsafe_store!(bufptrs, Ptr{Cvoid}(sizesp), nbuf)
    end
    childptrs = Ptr{Ptr{CArrowArray}}(C_NULL)
    if !isempty(d.children)
        childptrs = Ptr{Ptr{CArrowArray}}(_malloc!(e, length(d.children) * sizeof(Ptr)))
        for (i, c) in enumerate(d.children)
            unsafe_store!(childptrs, _export_array!(e, c, release), i)
        end
    end
    dict =
        d.dictionary === nothing ? Ptr{CArrowArray}(C_NULL) :
        _export_array!(e, d.dictionary, release)
    unsafe_store!(e.live, unsafe_load(e.live) + 1)
    unsafe_store!(
        p,
        CArrowArray(
            d.len,
            nullcount(d),
            d.offset,
            nbuf,
            length(d.children),
            bufptrs,
            childptrs,
            dict,
            release,
            Ptr{Cvoid}(e.live),
        ),
    )
    return p
end

"""
    Arrow.to_c_data(field, data) -> (Ptr{CArrowSchema}, Ptr{CArrowArray})

The low-level form of [`Arrow.to_c_data`](@ref): export one column, an
`Arrow.Field` and its `Arrow.ArrayData`, as an `ArrowSchema` and an `ArrowArray`
that Arrow.jl allocates, pointing at `data`'s buffers without copying them. The
column is validated before it is exported. The consumer moves or releases the
two structs; `data` stays reachable until it has released the array, and what
Arrow.jl allocated is freed by later exports or garbage collections.
"""
function to_c_data(f::Field, d::ArrayData)
    validate_semantic(f, d)
    e = _newexport(Any[f, d])
    try
        sp = _export_schema!(e, f, @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},)))
        ap = _export_array!(e, d, @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},)))
        _register!(e)
        return sp, ap
    catch
        _free!(e)
        rethrow()
    end
end

# ---------------------------------------------------------------------------
# Import: C structs -> Core, one ForeignOwner per moved tree
# ---------------------------------------------------------------------------

# One owner for one MOVED ArrowArray tree. Every `BufferSlice` of the tree
# (children and dictionary) has a region whose `root` is this object and whose
# cell is `cell`, so the tree outlives every slice, `release!` on the owner or
# on any region revokes all of them, and the producer's release callback runs
# exactly once: from `release!` or from the GC finalizer, whichever comes first.
mutable struct ForeignOwner
    # A malloc'd copy of the moved struct: the stable address its callback takes.
    const block::Ptr{CArrowArray}
    const thread::Int   # the importing thread, where a collected owner releases
    cell::AC.ReleaseCell
    ForeignOwner(block::Ptr{CArrowArray}) = new(block, Threads.threadid())
end

# The finalizer of an imported array or stream owner. A producer's release can
# wait on a lock the importing thread holds (pyarrow's release of NumPy memory
# takes Python's GIL, which PythonCall's main thread keeps), and a thread waiting
# inside a finalizer stalls every garbage collection. So a finalizer on another
# thread hands the release to a task on the importing thread, which runs it at its
# next yield. At exit the release is skipped: Julia runs the finalizers of live
# objects then, and the process is ending.
function _finalizeowner(o)
    Threads.threadid() == o.thread && return release!(o)
    _EXITING[] && return nothing
    t = Task(() -> release!(o))
    t.sticky = true
    ccall(:jl_set_task_tid, Cint, (Any, Cint), t, o.thread - 1)
    schedule(errormonitor(t))
    return nothing
end

function _release_owner_action(p::Ptr{Cvoid})::Cvoid
    o = unsafe_pointer_to_objref(p)::ForeignOwner
    GC.@preserve o try
        _release_producer!(o.block)
    finally
        Libc.free(o.block)
    end
    return nothing
end

# Release an imported tree now: later access to its buffers throws an
# `InvalidStateException`, and the producer's release callback runs, once. A
# producer whose callback does not mark the struct released makes this throw.
release!(o::ForeignOwner) = release!(o.cell)

# A deep copy of imported data shares its owner, so the producer's release runs
# once, for the original and every copy.
Base.deepcopy_internal(o::ForeignOwner, ::IdDict) = o

# Move the array at `ap`: copy its struct, mark the source released, and give
# the copy an owner whose `release!` or finalizer runs the producer's release.
function _moveowner(ap::Ptr{CArrowArray})
    block = Ptr{CArrowArray}(Libc.malloc(sizeof(CArrowArray)))
    block == C_NULL && throw(OutOfMemoryError())
    unsafe_store!(block, unsafe_load(ap))
    _store_field!(ap, Val(:release), Ptr{Cvoid}(C_NULL))
    try
        o = ForeignOwner(block)
        o.cell = AC.ReleaseCell(@cfunction(_release_owner_action, Cvoid, (Ptr{Cvoid},)), o)
        return finalizer(_finalizeowner, o)
    catch
        try
            _release_producer!(block)
        finally
            Libc.free(block)
        end
        rethrow()
    end
end

"Load child `i`'s CArrowArray by value out of a parent's children table."
childat(a::CArrowArray, i::Int) = unsafe_load(unsafe_load(a.children, i))
"Load buffer pointer `i` out of a CArrowArray's buffer table."
bufferptr(a::CArrowArray, i::Int) = unsafe_load(a.buffers, i)

"""
    Arrow.from_c_data(schemaptr, arrayptr) -> (Arrow.Field, Arrow.ArrayData)

The low-level form of [`Arrow.Table(schemaptr, arrayptr)`](@ref Arrow.Table): import
one column. The array is moved, as the interface requires: Arrow.jl takes over its
`release` callback and marks the struct at `arrayptr` released. The schema is read and
then released. The imported buffers stay valid while the returned data is reachable;
the producer's release callback runs when it no longer is.

Buffer sizes follow from the declared lengths and offsets, which the producer is
trusted to get right, as the interface cannot check them; the column is then validated
as an IPC read is. A failed import releases the array.
"""
function from_c_data(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray})
    sp == C_NULL && throw(ArgumentError("ArrowSchema pointer is NULL"))
    ap == C_NULL && throw(ArgumentError("ArrowArray pointer is NULL"))
    sch = unsafe_load(sp)
    (sch.release == C_NULL || unsafe_load(ap).release == C_NULL) &&
        throw(ArgumentError("cannot import a released structure"))
    owner = _moveowner(ap)
    f = try
        _preflight_schema(sch)
        _import_field(sch)
    catch
        release!(owner)
        rethrow()
    finally
        _release_producer!(sp)
    end
    return f, _importarray(f, owner)
end

# Import the moved array `owner` holds as a column of `f`; a failed import
# releases it.
function _importarray(f::Field, owner::ForeignOwner)
    try
        arr = unsafe_load(owner.block)
        _preflight_array(f, arr)
        d = _import_array(f, arr, owner)
        validate_semantic(f, d)
        return d
    catch
        release!(owner)
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
    sch.n_children == 0 ||
        sch.children != C_NULL ||
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
    arr.length <= typemax(Int64) - arr.offset || throw(
        ValidationError(
            "C array length $(arr.length) plus offset $(arr.offset) overflows Int64",
        ),
    )
    -1 <= arr.null_count <= arr.length ||
        throw(ValidationError("invalid C array null count $(arr.null_count)"))
    arr.n_buffers >= 0 || throw(ValidationError("negative C array buffer count"))
    arr.n_children >= 0 || throw(ValidationError("negative C array child count"))
    arr.n_buffers == 0 ||
        arr.buffers != C_NULL ||
        throw(ValidationError("C array buffer table is NULL"))
    arr.n_children == 0 ||
        arr.children != C_NULL ||
        throw(ValidationError("C array child table is NULL"))

    spec = AC.layoutspec_of(f.type)
    expected_buffers = length(spec.buffers)
    if spec.variadic
        # validity + views + N variadic data buffers + the trailing int64
        # sizes buffer: at least the fixed pair plus the sizes buffer.
        Int64(arr.n_buffers) >= expected_buffers + 1 || throw(
            ValidationError(
                "view layout $(typeof(f.type)) requires at least $(expected_buffers + 1) buffers, producer sent $(arr.n_buffers)",
            ),
        )
    else
        Int64(arr.n_buffers) == expected_buffers || throw(
            ValidationError(
                "layout $(typeof(f.type)) declares $expected_buffers buffers, producer sent $(arr.n_buffers)",
            ),
        )
    end
    expected_children = spec.childcount == -1 ? length(f.children) : spec.childcount
    Int64(arr.n_children) == expected_children || throw(
        ValidationError(
            "layout $(typeof(f.type)) declares $expected_children children, producer sent $(arr.n_children)",
        ),
    )

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
        _preflight_array(
            AC.dictvaluefield(f, f.type),
            unsafe_load(arr.dictionary),
            depth + 1,
        )
    elseif arr.dictionary != C_NULL
        throw(ValidationError("non-dictionary C array has dictionary values"))
    end
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
        n >= CSTRING_SCAN_LIMIT && throw(
            ValidationError(
                "C Data $what has no NUL terminator within " *
                "$(CSTRING_SCAN_LIMIT) bytes",
            ),
        )
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
    (sch.flags & ARROW_FLAG_DICTIONARY_ORDERED == 0 || sch.dictionary != C_NULL) ||
        throw(ValidationError("ARROW_FLAG_DICTIONARY_ORDERED requires a dictionary schema"))
    (sch.flags & ARROW_FLAG_MAP_KEYS_SORTED == 0 || fmt == "+m") ||
        throw(ValidationError("ARROW_FLAG_MAP_KEYS_SORTED requires a map schema"))
    return nothing
end

"""
Parse a C metadata blob. The pair count and the key/value lengths are
producer-declared, the same trust as every other C Data pointer, but a
negative length is rejected: it would wrap the walk. Metadata bytes are
opaque binary key/value data, copied by declared length without UTF-8
validation. Embedded NUL bytes are preserved.
"""
function _import_cmetadata(p::Ptr{UInt8})
    p == C_NULL && return nothing
    n = unsafe_load(Ptr{Int32}(p))
    n < 0 && throw(ValidationError("C schema metadata declares a negative pair count"))
    n == 0 && return nothing
    off = Int64(4)
    out = Pair{String,String}[]
    for _ = 1:n
        klen = unsafe_load(Ptr{Int32}(p + off))
        klen < 0 &&
            throw(ValidationError("C schema metadata declares a negative key length"))
        k = unsafe_string(p + off + 4, klen)
        off += 4 + Int64(klen)
        vlen = unsafe_load(Ptr{Int32}(p + off))
        vlen < 0 &&
            throw(ValidationError("C schema metadata declares a negative value length"))
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
        throw(
            ValidationError(
                "C schema for $(typeof(t)) declares $(sch.n_children) children; expected $expected_children",
            ),
        )
    end
    t isa UnionType &&
        length(t.typeids) != sch.n_children &&
        throw(
            ValidationError(
                "union format declares $(length(t.typeids)) type ids for $(sch.n_children) children",
            ),
        )

    children = Field[]
    for i = 1:sch.n_children
        push!(children, _import_field(unsafe_load(unsafe_load(sch.children, i))))
    end
    if sch.dictionary != C_NULL
        vf = _import_field(unsafe_load(sch.dictionary))
        t isa IntType ||
            throw(ValidationError("dictionary index format must be an integer"))
        isempty(children) ||
            throw(ValidationError("dictionary index schema must not have children"))
        ordered = (sch.flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0
        # The value node's metadata joins the wrapper's (wrapper pairs
        # first; duplicate keys are legal): Core's one slot cannot express
        # the two-node attribution, but no pair is lost.
        vmeta = vf.metadata
        dmeta =
            meta === nothing ?
            (vmeta === nothing ? nothing : collect(Pair{String,String}, vmeta)) :
            (vmeta === nothing ? meta : vcat(meta, collect(Pair{String,String}, vmeta)))
        # Branch on the metadata's presence: a Union-typed keyword makes
        # the kwcall tuple imprecise, which trim cannot resolve.
        dmeta === nothing && return Field(
            name,
            DictionaryType(t, vf.type, ordered);
            nullable=nullable,
            children=vf.children,
        )
        return Field(
            name,
            DictionaryType(t, vf.type, ordered);
            nullable=nullable,
            metadata=dmeta,
            children=vf.children,
        )
    end
    meta === nothing && return Field(name, t; nullable=nullable, children=children)
    return Field(name, t; nullable=nullable, metadata=meta, children=children)
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
            p == C_NULL &&
                total > 0 &&
                arr.null_count != 0 &&
                throw(ValidationError("NULL validity buffer requires null_count == 0"))
            p == C_NULL ? Int64(0) : AC.expected_validity_bytes(total)
        elseif role == AC.OFFSETS
            # Hostile extents overflow as ValidationErrors, as in Core's own
            # structural checks.
            total < typemax(Int64) ||
                throw(ValidationError("required offsets buffer bytes overflow Int64"))
            AC._required_extent(
                total + Int64(1),
                Int64(spec.offsetwidth),
                "offsets buffer bytes",
            )
        elseif role == AC.DATA
            if spec.fixedwidth > 0
                AC._required_extent(total, Int64(spec.fixedwidth), "data buffer bytes")
            elseif spec.fixedwidth == -1
                AC.expected_validity_bytes(total)
            else
                # varbinary data: sized by the final offset, read from the
                # offsets slice we just built (bounded by ITS declared size).
                if offsets_slice === nothing || AC.isempty_buffer(offsets_slice)
                    Int64(0)
                else
                    finaloffset = if spec.offsetwidth == 8
                        AC.loadat(offsets_slice, Int64, AC.checked_mul(total, Int64(8)))
                    else
                        Int64(AC.loadat(offsets_slice, Int32, AC.checked_mul(total, Int64(4))))
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
            AC._required_extent(total, Int64(spec.offsetwidth), "per-slot buffer bytes")
        elseif role == AC.VIEWS
            AC._required_extent(total, Int64(16), "views buffer bytes")
        else
            throw(ValidationError("unsupported buffer role $role in C data import"))
        end
        if p == C_NULL
            nbytes == 0 ||
                throw(ValidationError("NULL $role buffer with nonzero required size"))
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
        (nvariadic == 0 || sizesp != C_NULL) || throw(
            ValidationError("view array with variadic buffers has a NULL sizes buffer"),
        )
        for k = 1:nvariadic
            len = unsafe_load(sizesp, k)
            len >= 0 || throw(ValidationError("negative variadic buffer length $len"))
            p = bufferptr(arr, nfixed + k)
            if p == C_NULL
                len == 0 || throw(
                    ValidationError("NULL variadic buffer with nonzero declared length"),
                )
                push!(buffers, BufferSlice())
            else
                region = OwnerRegion(Ptr{UInt8}(p), len; root=owner, cell=owner.cell)
                push!(buffers, BufferSlice(region, 0, len))
            end
        end
    end
    children = ArrayData[]
    for i = 1:arr.n_children
        # Preflight already rejects children on a dictionary node, so this
        # branch is unreachable; it keeps the expression total for inference.
        cf = t isa DictionaryType ? error("dictionary carries no children") : f.children[i]
        push!(children, _import_array(cf, childat(arr, i), owner))
    end
    dict = nothing
    if arr.dictionary != C_NULL
        t isa DictionaryType ||
            throw(ValidationError("dictionary array on a non-dictionary field"))
        dict = _import_array(AC.dictvaluefield(f, t), unsafe_load(arr.dictionary), owner)
    end
    return AC._arraydata(
        t,
        arr.length,
        buffers,
        arr.offset,
        children,
        dict,
        owner,
        arr.null_count,
    )
end
