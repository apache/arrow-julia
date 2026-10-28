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
# Read-side columns for flat layouts: zero-copy views over decoded Arrow
# buffers, DataStrings string columns, and dictionary vectors.
#
# A view wraps its buffers as unowned Julia vectors (so loops vectorize like
# `Vector` loops) and holds the decoded `ArrayData`, whose buffer slices keep
# the backing memory (a memory map, the input bytes, or a decompressed
# buffer) reachable. Every access preserves the view, so memory a view can
# read is memory that is alive. Views check no release flag: the facade never
# unmaps a source eagerly (see `release!(::Table)`).
# =============================================================================

"""
    Arrow.ArrowVector{T} <: AbstractVector{T}

Supertype of the read-only columns `Arrow.Table` and `Arrow.Stream` return
for flat, list, map, and struct layouts. A flat column reads its Arrow buffers in
place (a memory-mapped file, the input bytes, or a decompressed buffer) and
keeps that memory alive while the column is reachable, independent of its
table. `copy(column)` returns an owned, mutable vector: a `Vector`, or a
`PooledArray` for a dictionary-encoded column.
"""
abstract type ArrowVector{T} <: AbstractVector{T} end

Base.IndexStyle(::Type{<:ArrowVector}) = IndexLinear()

@noinline _readonlycolumn() = error(
    "Arrow columns are read-only views of the data they were read from; " *
    "copy(column), copy(table), or DataFrame(table) returns a mutable copy",
)
Base.setindex!(::ArrowVector, v, ::Int) = _readonlycolumn()
# Containers that know only a column's type (a `ChainedVector` whose batches
# are all empty) allocate through this; a new vector is a mutable `Array`.
Base.similar(::Type{<:ArrowVector{T}}, dims::Dims) where {T} = Array{T}(undef, dims)

# --- buffer wrapping ------------------------------------------------------------

# An unowned vector over `n` elements of `S` starting `offset` elements into
# `b`. Callers keep `b`'s region reachable for as long as the vector is used.
# A misaligned start (possible only for foreign or hand-built buffers) gets
# an owned, aligned copy instead.
function _wrapbuffer(
    ::Type{S},
    b::AC.BufferSlice,
    offset::Int64,
    n::Int64,
    budget=nothing,
) where {S}
    isbitstype(S) || throw(ArgumentError("a wrapped buffer needs an isbits element type"))
    n == 0 && return S[]
    w = Int64(sizeof(S))
    src = AC.subslice(b, AC.checked_mul(offset, w), AC.checked_mul(n, w))
    p = AC.sliceptr(src)
    if UInt(p) % Base.datatype_alignment(S) != 0
        _chargeexact!(budget, S, n, "aligned buffer copy")
        out = Vector{S}(undef, Int(n))
        GC.@preserve out src begin
            unsafe_copyto!(Ptr{UInt8}(pointer(out)), p, Int(src.len))
        end
        return out
    end
    return unsafe_wrap(Array, Ptr{S}(p), Int(n))
end

# The validity bitmap bytes covering `d`'s slots and the bit position of slot
# 1 in them; an empty vector when no slot is null.
function _validitybits(d::AC.ArrayData)
    v = AC.validitybuffer(d)
    (AC.isempty_buffer(v) || AC.nullcount(d) == 0 || d.len == 0) && return UInt8[], 0
    first = d.offset >> 3
    nbytes = ((d.offset + d.len - 1) >> 3) - first + 1
    return _wrapbuffer(UInt8, v, first, nbytes), Int(d.offset & 7)
end

@inline function _bitset(bits::Vector{UInt8}, bitoffset::Int, i::Int)
    k = bitoffset + i - 1
    return @inbounds((bits[(k >> 3) + 1] >> (k & 7)) & 0x01) == 0x01
end

@inline _isnullslot(validity::Vector{UInt8}, bitoffset::Int, i::Int) =
    !isempty(validity) && !_bitset(validity, bitoffset, i)

# --- storage conversions --------------------------------------------------------

# `C` in `Primitive{T,S,C}` names how one stored `S` becomes a `T`;
# `Nothing` means the layouts are identical.
@inline _fromstorage(::Type{Nothing}, x) = x
struct _DaysToDate end
@inline _fromstorage(::Type{_DaysToDate}, x::Int32) =
    Dates.Date(Dates.UTD(Int64(x) + _EPOCH_DAYS))
struct _MillisToDateTime end
@inline _fromstorage(::Type{_MillisToDateTime}, x::Int64) =
    Dates.DateTime(Dates.UTM(x + Dates.UNIXEPOCH))
"Time of day stored in ticks of `N` nanoseconds (range-checked when built)."
struct _TimeOfDay{N} end
@inline _fromstorage(::Type{_TimeOfDay{N}}, x) where {N} =
    Dates.Time(Dates.Nanosecond(Int64(x) * N))
struct _YearMonth end
@inline _fromstorage(::Type{_YearMonth}, x::Int32) = Durations.Duration(x, 0, 0)
struct _DayTime end
@inline _fromstorage(::Type{_DayTime}, x::NTuple{2,Int32}) =
    Durations.Duration(0, x[1], Int64(x[2]) * 1_000_000)

# The element type each conversion produces.
_storagetarget(::Type{_DaysToDate}) = Dates.Date
_storagetarget(::Type{_MillisToDateTime}) = Dates.DateTime
_storagetarget(::Type{<:_TimeOfDay}) = Dates.Time
_storagetarget(::Type{_YearMonth}) = Durations.Duration
_storagetarget(::Type{_DayTime}) = Durations.Duration

# --- fixed width ------------------------------------------------------------------

"""
    Arrow.Primitive{T,S,C} <: Arrow.ArrowVector{T}

A fixed-width column: element `i` is the `i`th stored `S` value converted to
`T` (the same bits when the layouts match), or `missing` where the validity
bitmap marks a null.
"""
struct Primitive{T,S,C} <: ArrowVector{T}
    data::Vector{S}
    validity::Vector{UInt8}
    bitoffset::Int
    owner::Any
end

Base.size(v::Primitive) = size(v.data)

Base.@propagate_inbounds function Base.getindex(v::Primitive{T,S,C}, i::Int) where {T,S,C}
    @boundscheck checkbounds(v, i)
    x = GC.@preserve v @inbounds(v.data[i])
    if Missing <: T
        null = GC.@preserve v _isnullslot(v.validity, v.bitoffset, i)
        null && return missing
    end
    return _fromstorage(C, x)
end

function _primitive(
    ::Type{T},
    ::Type{S},
    ::Type{C},
    d::AC.ArrayData,
    budget=nothing,
) where {T,S,C}
    data = _wrapbuffer(S, AC.rolebuffer(d, AC.DATA), d.offset, d.len, budget)
    validity, bitoffset = _validitybits(d)
    return Primitive{T,S,C}(data, validity, bitoffset, d)
end

# One pass over the buffer, and its validity bitmap where some slot is null:
# the stored values are the values.
function Base.copyto!(
    dest::Vector{T},
    doffs::Integer,
    src::Primitive{T,S,Nothing},
    soffs::Integer,
    n::Integer,
) where {T,S}
    n == 0 && return dest
    (
        n > 0 &&
        checkbounds(Bool, dest, doffs:(doffs + n - 1)) &&
        checkbounds(Bool, src, soffs:(soffs + n - 1))
    ) || throw(BoundsError(dest, doffs:(doffs + n - 1)))
    GC.@preserve src begin
        data, bits, off = src.data, src.validity, src.bitoffset
        if isempty(bits)
            copyto!(dest, doffs, data, soffs, n)
        else
            # Validity is present only when some slot is null, so `T` has `Missing`.
            @inbounds for i = 0:(n - 1)
                dest[doffs + i] = _bitset(bits, off, soffs + i) ? data[soffs + i] : missing
            end
        end
    end
    return dest
end

Base.copy(v::Primitive{T,S,Nothing}) where {T,S} =
    copyto!(Vector{T}(undef, length(v)), 1, v, 1, length(v))

# Base's generic `map`, `filter`, and `collect` do not vectorize through the
# per-access preserve. A view whose values are its stored layout (no nulls
# can occur in a non-`Missing` element type) runs them on its buffer.
Base.map(f, v::Primitive{T,T,Nothing}) where {T} = GC.@preserve v map(f, v.data)
Base.filter(f, v::Primitive{T,T,Nothing}) where {T} = GC.@preserve v filter(f, v.data)
Base.collect(v::Primitive{T,T,Nothing}) where {T} = GC.@preserve v copy(v.data)

# Reductions over a view with no nulls run on its buffer, so they vectorize
# as they do over a `Vector`.
const _BufferPrimitive = Primitive{T,S,Nothing} where {T,S}
function Base.mapreduce(f, op, A::_BufferPrimitive; kw...)
    isempty(A.validity) && return GC.@preserve A mapreduce(f, op, A.data; kw...)
    return invoke(mapreduce, Tuple{Any,Any,AbstractArray}, f, op, A; kw...)
end
function Base.mapreduce(f, op, itr::Base.SkipMissing{<:_BufferPrimitive})
    A = itr.x
    isempty(A.validity) && return GC.@preserve A mapreduce(f, op, A.data)
    _maskablesum(f, op, A) && return GC.@preserve A _maskedsum(A)
    return invoke(mapreduce, Tuple{Any,Any,Base.SkipMissing{<:AbstractArray}}, f, op, itr)
end
function Base.mapreduce(
    f,
    op,
    itr::Base.SkipMissing{<:ChainedVector{T,<:_BufferPrimitive}},
) where {T}
    chunks = itr.x.arrays
    if !isempty(chunks) &&
       (all(c -> isempty(c.validity), chunks) || all(c -> _maskablesum(f, op, c), chunks))
        return reduce(op, (mapreduce(f, op, skipmissing(c)) for c in chunks))
    end
    return invoke(mapreduce, Tuple{Any,Any,Base.SkipMissing{<:AbstractArray}}, f, op, itr)
end

# `sum(skipmissing(col))` over numbers with nulls: a branch-free pairwise sum
# in which null slots add the neutral element (`-0.0` keeps the sign of an
# all-`-0.0` float sum).
_maskablesum(f, op, ::Primitive) = false
_maskablesum(
    ::typeof(identity),
    ::Union{typeof(+),typeof(Base.add_sum)},
    ::Primitive{T,<:Union{Base.BitInteger,Base.IEEEFloat},Nothing},
) where {T} = true

function _maskedsum(A::Primitive{T,S}) where {T,S}
    R = typeof(Base.add_sum(zero(S), zero(S)))
    neutral = S <: AbstractFloat ? -zero(R) : zero(R)
    isempty(A.data) && return neutral
    return _maskedsum(A.data, A.validity, A.bitoffset, 1, length(A.data), neutral)
end
function _maskedsum(
    data::Vector,
    bits::Vector{UInt8},
    off::Int,
    lo::Int,
    hi::Int,
    neutral::R,
) where {R}
    if hi - lo < 1024
        s = neutral
        @inbounds @simd for i = lo:hi
            k = off + i - 1
            valid = (bits[(k >> 3) + 1] >> (k & 7)) & 0x01 == 0x01
            s += ifelse(valid, R(data[i]), neutral)
        end
        return s
    end
    mid = (lo + hi) >>> 1
    return _maskedsum(data, bits, off, lo, mid, neutral) +
           _maskedsum(data, bits, off, mid + 1, hi, neutral)
end

# --- booleans ---------------------------------------------------------------------

"""
    Arrow.BoolVector{T} <: Arrow.ArrowVector{T}

A boolean column read from its bit-packed values and validity bitmaps.
"""
struct BoolVector{T} <: ArrowVector{T}
    values::Vector{UInt8}
    valueoffset::Int
    validity::Vector{UInt8}
    bitoffset::Int
    len::Int
    owner::Any
end

Base.size(v::BoolVector) = (v.len,)

Base.@propagate_inbounds function Base.getindex(v::BoolVector{T}, i::Int) where {T}
    @boundscheck checkbounds(v, i)
    if Missing <: T
        null = GC.@preserve v _isnullslot(v.validity, v.bitoffset, i)
        null && return missing
    end
    return GC.@preserve v _bitset(v.values, v.valueoffset, i)
end

function _boolvector(::Type{T}, d::AC.ArrayData) where {T}
    validity, bitoffset = _validitybits(d)
    d.len == 0 && return BoolVector{T}(UInt8[], 0, validity, bitoffset, 0, d)
    first = d.offset >> 3
    nbytes = ((d.offset + d.len - 1) >> 3) - first + 1
    values = _wrapbuffer(UInt8, AC.rolebuffer(d, AC.DATA), first, nbytes)
    return BoolVector{T}(values, Int(d.offset & 7), validity, bitoffset, Int(d.len), d)
end

# Non-null counts come from the validity bitmap.
Base.count(
    ::ComposedFunction{typeof(!),typeof(ismissing)},
    A::Union{Primitive,BoolVector},
) = length(A) - _nullslots(A)
function _nullslots(A::Union{Primitive,BoolVector})
    n = length(A)
    (isempty(A.validity) || n == 0) && return 0
    bits, off = A.validity, A.bitoffset
    stop = off + n - 1
    last = stop >> 3 + 1
    valid = 0
    GC.@preserve A begin
        @inbounds @simd for k = 1:last
            valid += count_ones(bits[k])
        end
        # Drop the bits before slot 1 and after slot n.
        valid -= count_ones(bits[1] & ~(0xff << off))
        valid -= count_ones(bits[last] & ~(0xff >> (7 - (stop & 7))))
    end
    return n - valid
end

# --- run-end encoding -------------------------------------------------------------

"""
    Arrow.RunEndEncoded{T,V} <: Arrow.ArrowVector{T}

A run-end encoded column: element `i` is the value of the run that covers
it, found by binary search over the run ends.
"""
struct RunEndEncoded{T,V<:AbstractVector} <: ArrowVector{T}
    runends::Vector{Int64}
    values::V
    offset::Int
    len::Int
end

Base.size(v::RunEndEncoded) = (v.len,)

Base.@propagate_inbounds function Base.getindex(v::RunEndEncoded{T}, i::Int) where {T}
    @boundscheck checkbounds(v, i)
    # Validation proved the run ends ascending and covering every slot.
    run = searchsortedfirst(v.runends, v.offset + i)
    return @inbounds(v.values[run])::T
end

# --- dictionary encoding ----------------------------------------------------------

"""
    Arrow.DictEncoded{T,R,P,I} <: Arrow.ArrowVector{T}

A dictionary-encoded column: zero-based codes of type `R` (the index vector
`I`, `missing` where null) into the value pool `P`. `DataAPI.refarray`, `DataAPI.refpool`, and
`DataAPI.levels` expose the encoding without decoding it, and `copy`
returns a `PooledArray`.
"""
struct DictEncoded{T,R,P<:AbstractVector,I<:AbstractVector} <: ArrowVector{T}
    indices::I
    pool::P
    ordered::Bool
end

Base.size(v::DictEncoded) = size(v.indices)

Base.@propagate_inbounds function Base.getindex(v::DictEncoded{T}, i::Int) where {T}
    @boundscheck checkbounds(v, i)
    code = @inbounds v.indices[i]
    code === missing && return missing
    # Validation proved every valid index in [0, length(pool)).
    return @inbounds(v.pool[Int(code) + 1])::T
end

"""
The `DataAPI.refarray` of a dictionary column: the one-based pool position
of each value, with 0 for a null slot.
"""
struct DictRefs{R,I<:AbstractVector} <: AbstractVector{R}
    indices::I
end
Base.size(v::DictRefs) = size(v.indices)
Base.IndexStyle(::Type{<:DictRefs}) = IndexLinear()
Base.@propagate_inbounds function Base.getindex(v::DictRefs{R}, i::Int) where {R}
    code = v.indices[i]
    return code === missing ? zero(R) : R(code) + one(R)
end

"""
The `DataAPI.refpool` of a dictionary column whose codes can be null: index
0 holds `missing`, then the pool values in order.
"""
struct DictRefPool{T,P<:AbstractVector} <: AbstractVector{T}
    pool::P
end
Base.size(v::DictRefPool) = (length(v.pool) + 1,)
Base.axes(v::DictRefPool) = (Base.IdentityUnitRange(0:length(v.pool)),)
Base.IndexStyle(::Type{<:DictRefPool}) = IndexLinear()
Base.@propagate_inbounds Base.getindex(v::DictRefPool{T}, i::Int) where {T} =
    i == 0 ? missing : convert(T, v.pool[i])

# Codes widen by one step so `code + 1` never overflows.
_reftype(::Type{R}) where {R<:Union{Int8,UInt8,Int16,UInt16}} = Int32
_reftype(::Type{R}) where {R<:Union{Int32,UInt32,Int64,UInt64}} = Int64

DataAPI.refarray(v::DictEncoded{T,R}) where {T,R} =
    DictRefs{_reftype(R),typeof(v.indices)}(v.indices)
DataAPI.refpool(v::DictEncoded{T}) where {T} =
    Missing <: T ? DictRefPool{T,typeof(v.pool)}(v.pool) : v.pool
DataAPI.levels(v::DictEncoded) = unique(skipmissing(v.pool))

# Chunks that share one pool keep a column-wide encoding.
_sharedpool(x::ChainedVector{T,<:DictEncoded}) where {T} =
    all(c -> c.pool === x.arrays[1].pool, x.arrays)
DataAPI.refarray(x::ChainedVector{T,<:DictEncoded}) where {T} =
    isempty(x.arrays) || !_sharedpool(x) ? x :
    ChainedVector([DataAPI.refarray(c) for c in x.arrays])
DataAPI.refpool(x::ChainedVector{T,<:DictEncoded}) where {T} =
    isempty(x.arrays) || !_sharedpool(x) ? nothing : DataAPI.refpool(x.arrays[1])

Base.copy(v::DictEncoded{T}) where {T} = _pooledcopy(T, (v,))
# The batches of a column copy into one `PooledArray`, whether they share a
# pool, extend it (delta dictionaries), or replace it.
Base.copy(c::ChainedVector{T,<:DictEncoded}) where {T} = _pooledcopy(T, c.arrays)

function _pooledcopy(::Type{T}, chunks) where {T}
    # Distinct values in first-pool-position order; duplicates and null
    # pool entries remap so the PooledArray's invariants hold.
    invpool = Dict{T,UInt32}()
    pool = T[]
    refs = Vector{UInt32}(undef, sum(length, chunks; init=0))
    remap = UInt32[]
    lastpool = nothing
    at = 0
    for v in chunks
        if v.pool !== lastpool
            remap = Vector{UInt32}(undef, length(v.pool))
            for (k, x) in enumerate(v.pool)
                remap[k] = get!(invpool, x) do
                    push!(pool, x)
                    UInt32(length(pool))
                end
            end
            lastpool = v.pool
        end
        for i in eachindex(v.indices)
            code = @inbounds v.indices[i]
            refs[at + i] = if code === missing
                get!(invpool, missing) do
                    push!(pool, missing)
                    UInt32(length(pool))
                end
            else
                @inbounds remap[Int(code) + 1]
            end
        end
        at += length(v)
    end
    return PooledArrays.PooledArray(PooledArrays.RefArray(refs), invpool, pool)
end

# --- strings and binary -----------------------------------------------------------

const _StringPayload = DataStrings.StringPayload

# Byte runs at most this long fit DataStrings' Int32 view words.
const _MAX_VIEW_BUFFER = Int64(typemax(Int32))

# Rows per chunk when a string column builds on several tasks.
const _STRING_CHUNK_ROWS = 1 << 16

_isstringlayout(t::AC.ArrowType) =
    t isa Union{AC.Utf8Type,AC.BinaryType,AC.ViewType} || _widebinary(t)

"""
Rows `first:last` of string batch `d`, whose output slots follow index `at`.
An offsets layout (`utf8`/`binary`, normal or large) copies the rows' bytes
into data buffer `buf` (0-based), which holds the batch's bytes from offset
`lo` on. For a view layout `buf` is the batch's first data buffer, and a chunk
with `variadic > 0` copies that buffer of the batch instead of encoding rows.
"""
struct _StringChunk
    d::AC.ArrayData
    at::Int
    first::Int
    last::Int
    buf::Int
    lo::Int64
    variadic::Int
    copybytes::Bool   # false: buffer `buf` is the batch's own decompressed data
end

"""
A string or binary column under construction: the payloads and data buffers
of a `StringVector`/`BytesVector` of type `V`, or the values of a
`Vector{String}` (`V === String`). Its chunks are independent, so several
tasks can fill them (`_buildchunk!`); `_finishbuild` returns the column.
"""
struct _StringBuild{V,O}
    out::O
    buffers::Vector{Vector{UInt8}}
    chunks::Vector{_StringChunk}
end

# Call `f` with the `n + 1` offsets of offsets-layout batch `d`. A fixed-size
# binary batch's value `i` starts `(offset + i - 1) * width` bytes into its
# data buffer.
function _withoffsets(f, d::AC.ArrayData, budget=nothing)
    t = d.type
    n = Int64(d.len)
    if t isa AC.FixedSizeBinaryType
        w = Int64(t.nbytes)
        o = Int64(d.offset)
        return f((o * w):w:((o + n) * w))
    end
    offs = AC.rolebuffer(d, AC.OFFSETS)
    return t.large ? f(_wrapbuffer(Int64, offs, d.offset, n + 1, budget)) :
           f(_wrapbuffer(Int32, offs, d.offset, n + 1, budget))
end

function _rowchunks!(
    chunks,
    d,
    at::Int,
    first::Int,
    last::Int,
    buf::Int,
    lo::Int64,
    rows::Int,
    copybytes::Bool=true,
)
    a = first
    while a <= last
        b = last - a >= rows ? a + rows - 1 : last
        push!(chunks, _StringChunk(d, at, a, b, buf, lo, 0, copybytes))
        a = b + 1
    end
    return chunks
end

# The decompressed bytes behind slice `b` when this read owns them and `b`
# starts at their start, so a column can keep them instead of a copy.
function _adoptbytes(b::AC.BufferSlice)
    r = b.region
    (r !== nothing && b.offset == 0 && r.root isa _Decompressed) || return nothing
    return (r.root::_Decompressed).bytes
end

# Cut offsets batch `d` into runs of rows whose bytes span at most 2 GiB, one
# data buffer each, and the runs into chunks.
function _offsetruns!(
    chunks,
    buffers,
    d,
    offsets::AbstractVector{O},
    at::Int,
    budget,
    rows::Int,
) where {O}
    n = Int(d.len)
    # Decompressed data is the read's own: the column keeps it as it is.
    own = _adoptbytes(AC.rolebuffer(d, AC.DATA))
    if own !== nothing && Int64(offsets[n + 1]) <= _MAX_VIEW_BUFFER
        push!(buffers, own)
        return _rowchunks!(chunks, d, at, 1, n, length(buffers) - 1, Int64(0), rows, false)
    end
    i = 1
    while i <= n
        lo = Int64(offsets[i])
        j = i
        if Int64(offsets[n + 1]) - lo <= _MAX_VIEW_BUFFER
            j = n + 1
        else
            while j <= n && Int64(offsets[j + 1]) - lo <= _MAX_VIEW_BUFFER
                j += 1
            end
        end
        j == i && throw(
            ArgumentError(
                "a string longer than 2 GiB does not fit a DataString; " *
                "read the column with stringtype=String",
            ),
        )
        hi = Int64(offsets[j])
        _chargeexact!(budget, UInt8, hi - lo, "string column data")
        push!(buffers, Vector{UInt8}(undef, Int(hi - lo)))
        _rowchunks!(chunks, d, at, i, j - 1, length(buffers) - 1, lo, rows)
        i = j
    end
    return chunks
end

# The bytes the values of string batch `d` reference: its data span, or its
# variadic buffers.
_stringbytes(d::AC.ArrayData) =
    d.type isa AC.ViewType ?
    sum(k -> d.buffers[k].len, 3:length(d.buffers); init=Int64(0)) :
    d.len == 0 ? Int64(0) : _withoffsets(o -> Int64(o[end]) - Int64(o[1]), d)

"""
Plan the column of string or binary batches `datas` with element type `T`:
allocate and charge its output and data buffers, and cut its rows into chunks
of at most `rows`. Each data buffer is one copy of a run of the referenced
bytes; a run stays under 2 GiB so view offsets fit DataStrings' Int32 words.
"""
function _stringbuild(::Type{T}, datas, budget, rows::Int=typemax(Int)) where {T}
    B = Base.nonmissingtype(T)
    n = sum(d -> Int(d.len), datas; init=0)
    chunks = _StringChunk[]
    buffers = Vector{UInt8}[]
    at = 0
    if B === String
        _chargeexact!(budget, T, n, "string column")
        bytes = Int64(0)
        for d in datas
            _rowchunks!(chunks, d, at, 1, Int(d.len), 0, Int64(0), rows)
            bytes = AC.checked_add(bytes, _stringbytes(d))
            at += Int(d.len)
        end
        # One String object per value: its bytes plus a header and rounding.
        budget === nothing || _charge!(
            budget,
            AC.checked_add(AC.checked_mul(Int64(n), Int64(32)), bytes),
            "string values",
        )
        return _StringBuild{String,Vector{T}}(Vector{T}(undef, n), buffers, chunks)
    end
    V =
        B === DataStrings.DataString ? DataStrings.StringVector{T} :
        DataStrings.BytesVector{T}
    _chargeexact!(budget, _StringPayload, n, "string column payloads")
    payloads = Vector{_StringPayload}(undef, n)
    for d in datas
        m = Int(d.len)
        if d.type isa AC.ViewType
            base = length(buffers)
            for k = 3:length(d.buffers)
                own = _adoptbytes(d.buffers[k])
                if own !== nothing
                    push!(buffers, own)
                    continue
                end
                len = d.buffers[k].len
                _chargeexact!(budget, UInt8, len, "string column data")
                push!(buffers, Vector{UInt8}(undef, Int(len)))
                push!(chunks, _StringChunk(d, at, 1, 0, base, Int64(0), k, true))
            end
            _rowchunks!(chunks, d, at, 1, m, base, Int64(0), rows)
        elseif m > 0
            _withoffsets(
                o -> _offsetruns!(chunks, buffers, d, o, at, budget, rows),
                d,
                budget,
            )
        end
        at += m
    end
    return _StringBuild{V,Vector{_StringPayload}}(payloads, buffers, chunks)
end

_finishbuild(b::_StringBuild{String}) = b.out
_finishbuild(b::_StringBuild{V}) where {V} = V(b.out, b.buffers, Val(:trusted))

"Fill chunk `k` of a string column under construction."
function _buildchunk!(
    b::_StringBuild{V,Vector{_StringPayload}},
    k::Int,
) where {V<:AbstractVector}
    c = b.chunks[k]
    d = c.d
    if d.type isa AC.ViewType
        c.variadic > 0 &&
            return _copyvariadic!(b.buffers[c.buf + c.variadic - 2], d, c.variadic)
        return _viewrows!(b.out, d, c)
    end
    buf = b.buffers[c.buf + 1]
    return _withoffsets(o -> _offsetrows!(b.out, buf, d, o, c), d)
end
function _buildchunk!(b::_StringBuild{String}, k::Int)
    c = b.chunks[k]
    d = c.d
    d.type isa AC.ViewType && return _viewstrings!(b.out, d, c)
    return _withoffsets(o -> _offsetstrings!(b.out, d, o, c), d)
end

function _copyvariadic!(dst::Vector{UInt8}, d::AC.ArrayData, k::Int)
    src = d.buffers[k]
    GC.@preserve dst d unsafe_copyto!(pointer(dst), AC.sliceptr(src), length(dst))
    return nothing
end

# Copy a chunk's bytes into its data buffer and encode its payloads.
function _offsetrows!(
    payloads,
    buf::Vector{UInt8},
    d::AC.ArrayData,
    offsets::AbstractVector{O},
    c,
) where {O}
    a = Int64(offsets[c.first])
    z = Int64(offsets[c.last + 1])
    if c.copybytes && z > a
        src = AC.subslice(AC.rolebuffer(d, AC.DATA), a, z - a)
        GC.@preserve buf d unsafe_copyto!(
            pointer(buf, Int(a - c.lo) + 1),
            AC.sliceptr(src),
            Int(z - a),
        )
    end
    validity, bitoffset = _validitybits(d)
    GC.@preserve d _encodepayloads!(
        payloads,
        c.at,
        buf,
        offsets,
        c.lo,
        c.first,
        c.last,
        Int(z - c.lo),
        validity,
        bitoffset,
        c.buf,
    )
    return nothing
end

@inline _load8(p::Ptr{UInt8}, i::Int) = ltoh(unsafe_load(Ptr{UInt64}(p + i)))

# The payload of the `len` bytes at `p + start`, which may read 12 bytes from
# `start` on: the length and up to four content bytes, then either the next
# eight content bytes (inline, zero-padded) or the buffer index and offset.
@inline function _payload(p::Ptr{UInt8}, start::Int, len::UInt64, viewword::UInt64)
    w0 = _load8(p, start)
    w1 = _load8(p, start + 4)
    n4 = min(len, UInt64(4))
    a = (len % UInt32) | ((w0 & ((UInt64(1) << (8 * n4)) - UInt64(1))) << 32)
    n8 = min(max(len, UInt64(4)) - UInt64(4), UInt64(8))
    inline = n8 == 8 ? w1 : w1 & ((UInt64(1) << (8 * n8)) - UInt64(1))
    view = viewword | (UInt64(start % UInt32) << 32)
    return _StringPayload(a, len <= DataStrings.INLINE_MAX ? inline : view)
end

"""
Encode rows `first:last` of an offsets-based string batch as payloads over
`buf`, the copy of the batch's bytes from offset `lo` on, reading only its
first `limit` bytes. Rows whose 12-byte window ends within `limit` encode
branch-free; the rows after them (inline, since their bytes end by `limit`)
byte by byte.
"""
function _encodepayloads!(
    payloads::Vector{_StringPayload},
    at::Int,
    buf::Vector{UInt8},
    offsets::AbstractVector{O},
    lo::Int64,
    first::Int,
    last::Int,
    limit::Int,
    validity::Vector{UInt8},
    bitoffset::Int,
    bufidx::Int=0,
) where {O}
    viewword = UInt64(bufidx % UInt32)
    # Offsets ascend, so the rows with a whole window form a prefix.
    fast = last
    while fast >= first && Int64(offsets[fast]) - lo > limit - 12
        fast -= 1
    end
    GC.@preserve buf begin
        p = pointer(buf)
        if isempty(validity)
            @inbounds for k = first:fast
                o = Int64(offsets[k])
                payloads[at + k] =
                    _payload(p, Int(o - lo), (Int64(offsets[k + 1]) - o) % UInt64, viewword)
            end
        else
            @inbounds for k = first:fast
                o = Int64(offsets[k])
                x = _payload(p, Int(o - lo), (Int64(offsets[k + 1]) - o) % UInt64, viewword)
                payloads[at + k] =
                    _bitset(validity, bitoffset, k) ? x : DataStrings.PAYLOAD_MISSING
            end
        end
        @inbounds for k = (fast + 1):last
            start = Int(Int64(offsets[k]) - lo)
            len = Int(Int64(offsets[k + 1]) - Int64(offsets[k]))
            x = DataStrings.inline_payload(buf, start + 1, len)
            payloads[at + k] =
                _isnullslot(validity, bitoffset, k) ? DataStrings.PAYLOAD_MISSING : x
        end
    end
    return payloads
end

# Zero the bytes after an inline payload's content, as DataStrings requires.
@inline function _canonicalinline(p::_StringPayload)
    len = Int(DataStrings.payloadlength(p))
    len > DataStrings.INLINE_MAX && return p
    a = len >= 4 ? p.a : p.a & ((UInt64(1) << (32 + 8 * len)) - 1)
    nb = max(len - 4, 0)
    b = nb >= 8 ? p.b : p.b & ((UInt64(1) << (8 * nb)) - 1)
    return _StringPayload(a, b)
end

# The rows of a `utf8_view`/`binary_view` chunk: the view entries copied as
# payloads (the layouts are identical), re-pointed at the batch's buffers
# within the column's list.
function _viewrows!(payloads::Vector{_StringPayload}, d::AC.ArrayData, c::_StringChunk)
    views = _wrapbuffer(_StringPayload, AC.rolebuffer(d, AC.VIEWS), d.offset, Int64(d.len))
    validity, bitoffset = _validitybits(d)
    base = c.buf
    GC.@preserve d for k = c.first:c.last
        if _isnullslot(validity, bitoffset, k)
            payloads[c.at + k] = DataStrings.PAYLOAD_MISSING
            continue
        end
        p = _canonicalinline(views[k])
        if base > 0 && DataStrings.payloadlength(p) > DataStrings.INLINE_MAX
            p = _StringPayload(
                p.a,
                UInt64((DataStrings.payloadbufidx(p) + base) % UInt32) |
                (UInt64(DataStrings.payloadoffset(p) % UInt32) << 32),
            )
        end
        payloads[c.at + k] = p
    end
    return nothing
end

# `String`s of an offsets-based chunk, read from the batch's data.
function _offsetstrings!(
    out,
    d::AC.ArrayData,
    offsets::AbstractVector{O},
    c::_StringChunk,
) where {O}
    a = Int64(offsets[c.first])
    z = Int64(offsets[c.last + 1])
    src = AC.subslice(AC.rolebuffer(d, AC.DATA), a, z - a)
    validity, bitoffset = _validitybits(d)
    GC.@preserve d begin
        p = AC.sliceptr(src) - a
        for k = c.first:c.last
            o = Int64(offsets[k])
            out[c.at + k] =
                _isnullslot(validity, bitoffset, k) ? missing :
                unsafe_string(p + o, Int(Int64(offsets[k + 1]) - o))
        end
    end
    return nothing
end

# `String`s of a view chunk: inline bytes from the payload, others from the
# batch's variadic buffers.
function _viewstrings!(out, d::AC.ArrayData, c::_StringChunk)
    views = _wrapbuffer(_StringPayload, AC.rolebuffer(d, AC.VIEWS), d.offset, Int64(d.len))
    validity, bitoffset = _validitybits(d)
    scratch = Vector{UInt8}(undef, 16)
    GC.@preserve d scratch for k = c.first:c.last
        if _isnullslot(validity, bitoffset, k)
            out[c.at + k] = missing
            continue
        end
        p = views[k]
        len = Int(DataStrings.payloadlength(p))
        out[c.at + k] = if len <= DataStrings.INLINE_MAX
            q = pointer(scratch)
            unsafe_store!(Ptr{UInt64}(q), htol((p.a >> 32) | (p.b << 32)))
            unsafe_store!(Ptr{UInt64}(q + 8), htol(p.b >> 32))
            unsafe_string(q, len)
        else
            data = d.buffers[DataStrings.payloadbufidx(p) + 3]
            unsafe_string(
                AC.sliceptr(AC.subslice(data, DataStrings.payloadoffset(p), len)),
                len,
            )
        end
    end
    return nothing
end

# --- multi-batch columns -----------------------------------------------------------

# Broadcasting over a column of several batches runs batch by batch, each
# over its own view (as fast as over a `Vector`), instead of locating the
# batch of every element.
struct _ChainedStyle <: Broadcast.AbstractArrayStyle{1} end
_ChainedStyle(::Val{1}) = _ChainedStyle()
_ChainedStyle(::Val{N}) where {N} = Broadcast.DefaultArrayStyle{N}()
Base.BroadcastStyle(::Type{<:ChainedVector{<:Any,<:ArrowVector}}) = _ChainedStyle()
Base.similar(::Broadcast.Broadcasted{_ChainedStyle}, ::Type{T}, dims) where {T} =
    similar(Array{T}, dims)
Base.similar(::Broadcast.Broadcasted{_ChainedStyle}, ::Type{Bool}, dims) =
    similar(BitArray, dims)

function Base.copyto!(dest::AbstractVector, bc::Broadcast.Broadcasted{_ChainedStyle})
    axes(dest) == axes(bc) || Broadcast.throwdm(axes(dest), axes(bc))
    c = _drivingchain(bc)
    c === nothing && return copyto!(dest, convert(Broadcast.Broadcasted{Nothing}, bc))
    for r in _batchranges(c)
        part = Broadcast.instantiate(_batchargs(bc, r))
        if dest isa BitVector
            # Bits pack fastest into a BitVector of their own.
            bits = copyto!(BitVector(undef, length(r)), part)
            copyto!(dest, first(r), bits, 1, length(r))
        else
            copyto!(view(dest, r), part)
        end
    end
    return dest
end

# Without a concrete element type, Base narrows the result to the values it
# computes (`x .+ 1` over a nullable column with no nulls gives a
# `Vector{Int}`): each batch narrows on its own, and the batches join under
# the same widening (`promote_typejoin`).
function Base.copy(bc::Broadcast.Broadcasted{_ChainedStyle})
    c = _drivingchain(bc)
    c === nothing && return copy(_elementwise(bc))
    E = Broadcast.combine_eltypes(bc.f, bc.args)
    isconcretetype(E) && return copyto!(similar(bc, E), bc)
    ranges = _batchranges(c)
    parts = [copy(Broadcast.instantiate(_batchargs(bc, r))) for r in ranges]
    isempty(parts) && return copy(_elementwise(bc))
    T = mapreduce(eltype, Base.promote_typejoin, parts)
    dest = similar(bc, T)
    for (r, part) in zip(ranges, parts)
        copyto!(dest, first(r), part, 1, length(part))
    end
    return dest
end

_elementwise(bc::Broadcast.Broadcasted) =
    Broadcast.Broadcasted{Broadcast.DefaultArrayStyle{1}}(bc.f, bc.args, bc.axes)

# The column the broadcast is cut by (its first multi-batch view argument), or
# `nothing` when some argument cannot be cut into rows (a tuple, an array of
# other dimensions).
_drivingchain(bc::Broadcast.Broadcasted) =
    all(_cuttable, bc.args) ? _firstchain(bc) : nothing

# The row ranges of `c`'s batches, empty batches left out.
function _batchranges(c::ChainedVector)
    ranges = UnitRange{Int}[]
    lo = 1
    for a in c.arrays
        n = length(a)
        n > 0 && push!(ranges, lo:(lo + n - 1))
        lo += n
    end
    return ranges
end

# Arguments a broadcast can be cut into rows by: vectors, scalars, and
# broadcasts of them (not tuples or other arrays).
_cuttable(a::Broadcast.Broadcasted) = all(_cuttable, a.args)
_cuttable(a::AbstractVector) = axes(a, 1) isa Base.OneTo
_cuttable(a) = axes(a) == ()

_firstchain(a::ChainedVector{<:Any,<:ArrowVector}) = a
function _firstchain(a::Broadcast.Broadcasted)
    for x in a.args
        c = _firstchain(x)
        c === nothing || return c
    end
    return nothing
end
_firstchain(a) = nothing

# `bc` restricted to rows `r`: a column whose batch is exactly `r` gives that
# batch, other vectors a view, and scalars and length-1 vectors stay whole.
_batchargs(bc::Broadcast.Broadcasted, r) =
    Broadcast.Broadcasted(bc.f, map(a -> _batcharg(a, r), bc.args))
_batcharg(a::Broadcast.Broadcasted, r) = _batchargs(a, r)
function _batcharg(a::ChainedVector{<:Any,<:ArrowVector}, r)
    k = searchsortedfirst(a.inds, last(r))
    k <= length(a.inds) &&
        a.inds[k] == last(r) &&
        (k == 1 ? 0 : a.inds[k - 1]) == first(r) - 1 &&
        return a.arrays[k]
    return view(a, r)
end
_batcharg(a::AbstractVector, r) = length(a) == 1 ? a : view(a, r)
_batcharg(a, r) = a

# --- column assembly --------------------------------------------------------------

# Shared decimals read as `DataDecimals.Decimal{P,S,T}`, which has the storage
# layout of every Arrow decimal width.
_isbulkdecimal(f::AC.Field) = f.type isa AC.DecimalType && _shareddecimal(f.type)
"One batch of a decimal column, read as stored."
_decimalcolumn(f::AC.Field, d::AC.ArrayData, budget=nothing) =
    _viewcolumn(f, AC.ArrayData[d], _ViewContext(DataStrings.DataString, budget))

# One read's view settings, its allocation budget, and its cache of
# dictionary pools (built once per pool snapshot; columns building on several
# tasks share it under the lock).
struct _ViewContext
    stringtype::Type
    budget::Union{Nothing,AllocationBudget}
    pools::IdDict{AC.ArrayData,AbstractVector}
    poolslock::ReentrantLock
end
_ViewContext(stringtype::Type=DataStrings.DataString, budget=nothing) =
    _ViewContext(stringtype, budget, IdDict{AC.ArrayData,AbstractVector}(), ReentrantLock())
# The same settings and pool cache under another allowance (`Arrow.Stream` batches).
_ViewContext(ctx::_ViewContext, budget) =
    _ViewContext(ctx.stringtype, budget, ctx.pools, ctx.poolslock)

# The view column of a dictionary's values, built once per pool snapshot. The
# build runs outside the lock; a concurrent duplicate is dropped.
function _dictpool(ctx::_ViewContext, vf::AC.Field, dict::AC.ArrayData)
    pool = @lock ctx.poolslock get(ctx.pools, dict, nothing)
    pool === nothing || return pool
    built = _viewcolumn(vf, AC.ArrayData[dict], ctx)
    return @lock ctx.poolslock get!(ctx.pools, dict, built)
end

# A view object's own size (struct, wrapped vector headers), charged per batch.
const _VIEW_OBJECT_BYTES = Int64(256)

function _checkstringtype(stringtype)
    stringtype === DataStrings.DataString ||
        stringtype === String ||
        throw(ArgumentError("stringtype must be DataString or String, got $stringtype"))
    return stringtype
end

"""
Whether `f` reads through this file's columns: a flat layout (as a view) or
a list, fixed-size list, map, struct, or union of such layouts (as typed
rows), with no ArrowTypes extension label anywhere in it. List views,
structs with duplicate or more than 64 field names, layouts nested more
than 8 deep, and extension columns keep the materializing path.
"""
function _viewable(f::AC.Field)
    _hasarrowtypesextension(f) && return _tensorshape(f) !== nothing
    return _viewablelayout(f)
end
function _viewablelayout(f::AC.Field, depth::Int=0)
    t = f.type
    t isa Union{
        AC.IntType,
        AC.FloatType,
        AC.BoolType,
        AC.DateType,
        AC.TimeType,
        AC.TimestampType,
        AC.DurationType,
        AC.IntervalType,
        AC.FixedSizeBinaryType,
        AC.Utf8Type,
        AC.BinaryType,
        AC.ViewType,
        AC.NullType,
    } && return true
    t isa AC.DecimalType && return _shareddecimal(t)
    t isa AC.DictionaryType && return _viewablelayout(AC.dictvaluefield(f, t), depth)
    t isa AC.RunEndEncodedType &&
        return length(f.children) == 2 && _viewablelayout(f.children[2], depth)
    # Row types nest one type per level, and Julia compiles deep ones slowly.
    t isa _NestedLayout && depth == _MAX_ROW_DEPTH && return false
    viewable(c) = _viewablelayout(c, depth + 1)
    t isa Union{AC.ListType,AC.FixedSizeListType} &&
        return length(f.children) == 1 && viewable(f.children[1])
    # A list view gathers its rows' values, which it does for flat values.
    t isa AC.ListViewType && return length(f.children) == 1 &&
           !_nestedvalue(f.children[1]) &&
           viewable(f.children[1])
    if t isa AC.MapType
        length(f.children) == 1 || return false
        entries = f.children[1]
        return entries.type isa AC.StructType &&
               length(entries.children) == 2 &&
               all(viewable, entries.children)
    end
    t isa AC.StructType && return length(f.children) <= _MAX_ROW_FIELDS &&
           allunique(c.name for c in f.children) &&
           all(viewable, f.children)
    t isa AC.UnionType && return !isempty(f.children) && all(viewable, f.children)
    return false
end

# Fixed-size binary values wider than this read as `DataBytes`: code over
# an `NTuple{N,UInt8}` compiles in time that grows faster than `N` (minutes
# at 16 KiB), and a hostile schema can declare any width.
const _MAX_TUPLE_BYTES = 256
_widebinary(t::AC.ArrowType) = t isa AC.FixedSizeBinaryType && t.nbytes > _MAX_TUPLE_BYTES

# The stored element type and storage conversion of a fixed-width layout.
function _fixedstorage(t::AC.ArrowType)
    t isa AC.IntType && return AC.juliatype(t), Nothing
    t isa AC.FloatType && return AC.juliatype(t), Nothing
    t isa AC.DecimalType && return _decimalhost(t), Nothing
    t isa AC.TimestampType && return _facadebasetype(t), Nothing
    t isa AC.DurationType && return _facadebasetype(t), Nothing
    t isa AC.FixedSizeBinaryType && return NTuple{t.nbytes,UInt8}, Nothing
    t isa AC.DateType &&
        return t.unit == AC.DAY ? (Int32, _DaysToDate) : (Int64, _MillisToDateTime)
    if t isa AC.TimeType
        S = t.bits == 32 ? Int32 : Int64
        return S, _TimeOfDay{_NANOS_PER_DAY ÷ _timeticksperday(t.unit)}
    end
    if t isa AC.IntervalType
        t.unit == AC.YEAR_MONTH && return Int32, _YearMonth
        t.unit == AC.DAY_TIME && return NTuple{2,Int32}, _DayTime
        return Durations.Duration, Nothing
    end
    throw(ArgumentError("no fixed-width storage for $(AC.descriptorname(t))"))
end

_timeticksperday(unit) =
    unit == AC.SECOND ? Int64(86_400) :
    unit == AC.MILLISECOND ? Int64(86_400_000) :
    unit == AC.MICROSECOND ? Int64(86_400_000_000) : _NANOS_PER_DAY

"""
The public element type of a column, before nullability. Nested rows take
their children's element types over `datas`, the batches being read, so a
child whose data holds undeclared nulls widens the row type of every batch.
"""
function _viewbasetype(f::AC.Field, ctx::_ViewContext, datas=AC.ArrayData[])
    t = f.type
    t isa AC.NullType && return Missing
    t isa AC.BoolType && return Bool
    t isa Union{AC.Utf8Type,AC.ViewType} &&
        (!(t isa AC.ViewType) || t.utf8) &&
        return ctx.stringtype === String ? String : DataStrings.DataString
    (t isa Union{AC.BinaryType,AC.ViewType} || _widebinary(t)) &&
        return DataStrings.DataBytes
    t isa AC.DictionaryType && return _viewbasetype(AC.dictvaluefield(f, t), ctx)
    t isa AC.RunEndEncodedType && return _viewbasetype(f.children[2], ctx)
    shape = _tensorshape(f)
    shape === nothing || return _tensortype(f, ctx, datas, shape)
    t isa _NestedLayout && return _rowtype(f, ctx, datas)
    S, C = _fixedstorage(t)
    return C === Nothing ? S : _storagetarget(C)
end

# Physical nulls the column's values can hold, through dictionary pools and
# run-end values.
function _viewnulls(f::AC.Field, d::AC.ArrayData)
    t = f.type
    t isa AC.NullType && return d.len > 0
    if t isa AC.RunEndEncodedType
        return _viewnulls(f.children[2], d.children[2])
    end
    if t isa AC.DictionaryType
        AC.nullcount(d) > 0 && return true
        dict = d.dictionary::AC.ArrayData
        return _viewnulls(AC.dictvaluefield(f, t), dict)
    end
    return AC.nullcount(d) > 0
end

# The declared type decides `Missing` (`_declaredeltype` is the authority);
# data that holds nulls a non-nullable declaration did not admit widens the
# column, as on every other read path.
function _vieweltype(f::AC.Field, datas, ctx::_ViewContext)
    B = _viewbasetype(f, ctx, datas)
    B === Missing && return Missing
    # A union has no validity of its own: its children's types carry nulls.
    f.type isa AC.UnionType && return B
    nullable = Missing <: _declaredeltype(f, true) || any(d -> _viewnulls(f, d), datas)
    return nullable ? Union{Missing,B} : B
end

function _checktimes(f::AC.Field, t::AC.TimeType, d::AC.ArrayData, budget)
    perday = _timeticksperday(t.unit)
    S = t.bits == 32 ? Int32 : Int64
    col = _primitive(Union{Missing,S}, S, Nothing, d, budget)
    for x in col
        (x === missing || 0 <= x < perday) && continue
        P = nameof(_unitperiod(t.unit))
        throw(
            ValidationError(
                "column $(repr(f.name)): time-of-day value $x is outside [0, $perday) " *
                "in its unit; read the column as durations since midnight with " *
                "`scan=Tables.Scan(select=($(repr(Symbol(f.name))) => Dates.$P,))`",
            ),
        )
    end
    return nothing
end

# The period of one tick of a time-of-day unit.
_unitperiod(unit) =
    unit == AC.SECOND ? Dates.Second :
    unit == AC.MILLISECOND ? Dates.Millisecond :
    unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond

# Time-of-day storage `raw` read as `P` periods since midnight: the scan
# override that reads times a `Dates.Time` cannot hold.
function _timeperiods(t::AC.TimeType, raw, ::Type{P}) where {P}
    unit = _unitperiod(t.unit)
    out = Vector{Missing<:eltype(raw) ? Union{Missing,P} : P}(undef, length(raw))
    for i in eachindex(out, raw)
        x = raw[i]
        out[i] = x === missing ? missing : convert(P, unit(x))
    end
    return out
end

# One batch of a view column. Nested layouts skip `_batchview`, whose body
# for every flat layout would otherwise compile once per row type.
_viewbatch(T::Type, f::AC.Field, d::AC.ArrayData, ctx::_ViewContext) =
    f.type isa _NestedLayout ? _nestedbatch(T, f, d, ctx) : _batchview(T, f, d, ctx)

"One batch of a fixed-width, boolean, dictionary, or run-end view column."
function _batchview(::Type{T}, f::AC.Field, d::AC.ArrayData, ctx::_ViewContext) where {T}
    t = f.type
    budget = ctx.budget
    _chargeobject!(budget, _VIEW_OBJECT_BYTES, "column view")
    t isa AC.BoolType && return _boolvector(T, d)
    if t isa AC.DictionaryType
        vf = AC.dictvaluefield(f, t)
        dict = d.dictionary::AC.ArrayData
        pool = _dictpool(ctx, vf, dict)
        R = AC.juliatype(t.indextype)
        indices = _primitive(Missing <: T ? Union{Missing,R} : R, R, Nothing, d, budget)
        return DictEncoded{T,R,typeof(pool),typeof(indices)}(indices, pool, t.ordered)
    end
    if t isa AC.RunEndEncodedType
        runs, values = d.children[1], d.children[2]
        R = AC.juliatype(runs.type::AC.IntType)
        _chargeexact!(budget, Int64, runs.len, "run ends")
        runends = Int64.(_primitive(R, R, Nothing, runs, budget))
        vcol = _viewcolumn(f.children[2], [values], ctx, T)
        return RunEndEncoded{T,typeof(vcol)}(runends, vcol, Int(d.offset), Int(d.len))
    end
    t isa AC.TimeType && _checktimes(f, t, d, budget)
    S, C = _fixedstorage(t)
    return _primitive(T, S, C, d, budget)
end

"""
The view column of field `f` over its decoded batches `datas`: one view for
one batch, a `ChainedVector` of per-batch views for several, and one
`StringVector`/`BytesVector` (or `Vector{String}`) across all batches of a
string or binary column. `T` overrides the element type (run-end values).
"""
function _viewcolumn(
    f::AC.Field,
    datas::AbstractVector{AC.ArrayData},
    ctx::_ViewContext,
    T::Type=_vieweltype(f, datas, ctx),
)
    t = f.type
    t isa AC.NullType && return fill(missing, sum(d -> Int(d.len), datas; init=0))
    if _isstringlayout(t)
        b = _stringbuild(T, datas, ctx.budget)
        for k in eachindex(b.chunks)
            _buildchunk!(b, k)
        end
        return _finishbuild(b)
    end
    isempty(datas) && return T[]
    first = _viewbatch(T, f, datas[1], ctx)
    length(datas) == 1 && return first
    return _chainviews(first, f, datas, ctx, T)
end

# A concretely typed chunk vector (a comprehension over `_batchview` would
# infer its element type at run time, per column). Batches whose views differ
# in type (dictionary pools that differ in nullability) chain abstractly.
function _chainviews(first::V, f::AC.Field, datas, ctx::_ViewContext, T::Type) where {V}
    _chargevector!(ctx.budget, V, length(datas), "column views")
    _chargevector!(ctx.budget, Int, length(datas), "chunked column index")
    cols = Vector{V}(undef, length(datas))
    cols[1] = first
    for k = 2:length(datas)
        col = _viewbatch(T, f, datas[k], ctx)
        col isa V || return ChainedVector(
            AbstractVector{T}[
                k == 1 ? first : _viewbatch(T, f, d, ctx) for (k, d) in enumerate(datas)
            ],
        )
        cols[k] = col
    end
    return ChainedVector(cols)
end

# --- nested rows ----------------------------------------------------------------
#
# Lists read as `ListRow` views, maps as `MapRow` views, fixed-size lists of up
# to 64 non-null values as `NTuple`s, structs as `NamedTuple`s built on access,
# and unions as a `Union` of their children's element types. A column's
# element type alone fixes how its child values are stored (`_columntype`):
# strings in a `StringVector`, binary in a `BytesVector`, lists in a
# `ListVector`, maps in a `MapVector`, structs in a `StructVector`, other
# values in a `Vector`. Columns keep their children untyped and read them back
# through that type, so a column type has only its element type as a
# parameter, however deep the schema, and access is type-stable. Child values
# are copied out of the source, so a row stays valid after its table is gone.

const _NestedLayout = Union{
    AC.ListType,
    AC.ListViewType,
    AC.FixedSizeListType,
    AC.MapType,
    AC.StructType,
    AC.UnionType,
}
const _MAX_ROW_FIELDS = 64
const _MAX_ROW_DEPTH = 8
const _MAX_TUPLE_ROW = 64

"""
    Arrow.ListRow{T} <: AbstractVector{T}

A row of a list column, or of a fixed-size list column longer than 64 values
or with nullable values: a read-only view of consecutive child values of its
column. It stays valid after its table is gone. `copy(row)` gives a
`Vector{T}`.
"""
struct ListRow{T} <: AbstractVector{T}
    values::Any   # the column's child values, a `_columntype(T)`
    offset::Int   # child values before the row's first
    len::Int
end

"""
    Arrow.MapRow{K,V} <: AbstractVector{Pair{K,V}}

A row of a map column: its entries as `key => value` pairs, in stored order
(duplicate keys are kept; `Dict(row)` keeps the last). It stays valid after
its table is gone. `copy(row)` gives a `Vector{Pair{K,V}}`.
"""
struct MapRow{K,V} <: AbstractVector{Pair{K,V}}
    keys::Any     # the column's keys and values, `_columntype(K)`, `_columntype(V)`
    values::Any
    offset::Int
    len::Int
end

"""
    Arrow.ListVector{T} <: Arrow.ArrowVector{T}

A list column: row `i` is the [`Arrow.ListRow`](@ref) of child values
`offsets[i]+1:offsets[i+1]`, or `missing` where the validity bitmap marks a
null.
"""
struct ListVector{T} <: ArrowVector{T}
    offsets::Vector{Int}
    values::Any   # child values, a `_columntype(eltype(row))`
    validity::Vector{UInt8}
    bitoffset::Int
    owner::Any
end

"""
    Arrow.MapVector{T} <: Arrow.ArrowVector{T}

A map column: row `i` is the [`Arrow.MapRow`](@ref) of entries
`offsets[i]+1:offsets[i+1]`, or `missing` where the validity bitmap marks a
null.
"""
struct MapVector{T} <: ArrowVector{T}
    offsets::Vector{Int}
    keys::Any
    values::Any
    validity::Vector{UInt8}
    bitoffset::Int
    owner::Any
end

"""
    Arrow.StructVector{T} <: Arrow.ArrowVector{T}

A struct column: row `i` is the `NamedTuple` of each field's value `i`,
built when it is read, or `missing` where the validity bitmap marks a null.
"""
struct StructVector{T} <: ArrowVector{T}
    children::Vector{AbstractVector}   # field `k`'s values, a `_columntype(fieldtype(row, k))`
    offset::Int                        # child values before row 1
    len::Int
    validity::Vector{UInt8}
    bitoffset::Int
    owner::Any
end

_columntype(::Type{T}) where {T} = _columntype(T, Base.nonmissingtype(T))
_columntype(::Type{Missing}) = Vector{Missing}
_columntype(::Type{T}, ::Type) where {T} = Vector{T}
_columntype(::Type{T}, ::Type{DataStrings.DataString}) where {T} =
    DataStrings.StringVector{T}
_columntype(::Type{T}, ::Type{DataStrings.DataBytes}) where {T} = DataStrings.BytesVector{T}
_columntype(::Type{T}, ::Type{<:ListRow}) where {T} = ListVector{T}
_columntype(::Type{T}, ::Type{<:MapRow}) where {T} = MapVector{T}
_columntype(::Type{T}, ::Type{<:NamedTuple}) where {T} = StructVector{T}

_childdatas(datas, k::Int) = AC.ArrayData[d.children[k] for d in datas]

# The row type of nested field `f` over batches `datas`: each child's element
# type, so a child whose data holds undeclared nulls widens every batch's rows.
function _rowtype(f::AC.Field, ctx::_ViewContext, datas)
    t = f.type
    if t isa AC.StructType
        names = Tuple(Symbol(c.name) for c in f.children)
        types =
            (_vieweltype(c, _childdatas(datas, k), ctx) for (k, c) in enumerate(f.children))
        return NamedTuple{names,Tuple{types...}}
    end
    t isa AC.UnionType && return Union{
        (_vieweltype(c, _childdatas(datas, k), ctx) for (k, c) in enumerate(f.children))...,
    }
    if t isa AC.MapType
        entries = f.children[1]
        entrydatas = _childdatas(datas, 1)
        K = _vieweltype(entries.children[1], _childdatas(entrydatas, 1), ctx)
        V = _vieweltype(entries.children[2], _childdatas(entrydatas, 2), ctx)
        return MapRow{K,V}
    end
    E = _vieweltype(f.children[1], _childdatas(datas, 1), ctx)
    # A tuple of a non-concrete `E` (nullable values) is an abstract type:
    # every row would be boxed, with up to 2^N runtime types.
    t isa AC.FixedSizeListType &&
        t.listsize <= _MAX_TUPLE_ROW &&
        isconcretetype(E) &&
        return NTuple{t.listsize,E}
    return ListRow{E}
end

# Whether field `f`'s values are nested rows (through a dictionary's values
# or run-end encoding too).
_nestedvalue(f::AC.Field) =
    f.type isa AC.DictionaryType ? _nestedvalue(AC.dictvaluefield(f, f.type)) :
    f.type isa AC.RunEndEncodedType ? _nestedvalue(f.children[2]) : f.type isa _NestedLayout

# Element `j` of `values`, a column of `T` values stored as its `_columntype`.
Base.@propagate_inbounds _childvalue(::Type{T}, values, j::Int) where {T} =
    (values::_columntype(T))[j]

Base.IndexStyle(::Type{<:Union{ListRow,MapRow}}) = IndexLinear()
Base.size(r::Union{ListRow,MapRow}) = (r.len,)
Base.size(v::Union{ListVector,MapVector}) = (length(v.offsets) - 1,)
Base.size(v::StructVector) = (v.len,)

Base.@propagate_inbounds function Base.getindex(r::ListRow{T}, i::Int) where {T}
    @boundscheck checkbounds(r, i)
    return @inbounds _childvalue(T, r.values, r.offset + i)
end

Base.@propagate_inbounds function Base.getindex(r::MapRow{K,V}, i::Int) where {K,V}
    @boundscheck checkbounds(r, i)
    j = r.offset + i
    return @inbounds Pair{K,V}(_childvalue(K, r.keys, j), _childvalue(V, r.values, j))
end

Base.@propagate_inbounds function Base.getindex(v::ListVector{T}, i::Int) where {T}
    @boundscheck checkbounds(v, i)
    if Missing <: T
        null = GC.@preserve v _isnullslot(v.validity, v.bitoffset, i)
        null && return missing
    end
    lo, hi = @inbounds v.offsets[i], v.offsets[i + 1]
    return Base.nonmissingtype(T)(v.values, lo, hi - lo)
end

Base.@propagate_inbounds function Base.getindex(v::MapVector{T}, i::Int) where {T}
    @boundscheck checkbounds(v, i)
    if Missing <: T
        null = GC.@preserve v _isnullslot(v.validity, v.bitoffset, i)
        null && return missing
    end
    lo, hi = @inbounds v.offsets[i], v.offsets[i + 1]
    return Base.nonmissingtype(T)(v.keys, v.values, lo, hi - lo)
end

Base.@propagate_inbounds function Base.getindex(v::StructVector{T}, i::Int) where {T}
    @boundscheck checkbounds(v, i)
    if Missing <: T
        null = GC.@preserve v _isnullslot(v.validity, v.bitoffset, i)
        null && return missing
    end
    return _structrow(Base.nonmissingtype(T), v.children, v.offset + i)
end

# Generated so each field reads through its own child type, without
# allocating. The generator loops: a comprehension's closure would compile
# once per row type.
@generated function _structrow(
    ::Type{R},
    children::Vector{AbstractVector},
    j::Int,
) where {R<:NamedTuple}
    fields = Expr[]
    for k = 1:fieldcount(R)
        push!(fields, :(@inbounds _childvalue($(fieldtype(R, k)), children[$k], j)))
    end
    return :(R(($(fields...),)))
end

# An owned copy holds each list or map row as its own `Vector`, over every
# batch, and each struct row as its `NamedTuple`.
function _copyrows(::Type{T}, v) where {T}
    R = Vector{eltype(Base.nonmissingtype(T))}
    out = Vector{Missing<:T ? Union{Missing,R} : R}(undef, length(v))
    for (i, x) in enumerate(v)
        out[i] = x === missing ? missing : collect(x)
    end
    return out
end
Base.copy(v::Union{ListVector{T},MapVector{T}}) where {T} = _copyrows(T, v)
Base.copy(c::ChainedVector{T,<:Union{ListVector{T},MapVector{T}}}) where {T} =
    _copyrows(T, c)
Base.copy(v::StructVector{T}) where {T} = T[v[i] for i = 1:length(v)]

# The values of child `d` (field `f`) as their `_columntype(E)`: the column a
# read builds, copied into owned storage when it is a view of the source
# (rows view their column's children, and outlive their table).
function _childcolumn(
    f::AC.Field,
    d::AC.ArrayData,
    ctx::_ViewContext,
    @nospecialize(E::Type)
)
    col = _viewcolumn(f, AC.ArrayData[d], ctx, E)
    C = _columntype(E)
    col isa C && return col
    return _storeas(C, col, ctx.budget)
end

function _storeas(::Type{Vector{E}}, col, budget) where {E}
    _chargeexact!(budget, E, length(col), "nested child values")
    return copyto!(Vector{E}(undef, length(col)), 1, col, 1, length(col))
end
_storeas(::Type{DataStrings.StringVector{E}}, col, budget) where {E} =
    DataStrings.StringVector{E}(collect(col))
function _storeas(::Type{DataStrings.BytesVector{E}}, col, budget) where {E}
    payloads = Vector{_StringPayload}(undef, length(col))
    buffers = Vector{UInt8}[]
    for (i, x) in enumerate(col)
        if x === missing
            payloads[i] = DataStrings.PAYLOAD_MISSING
            continue
        end
        bytes = collect(UInt8, x)
        n = length(bytes)
        payloads[i] = if n <= DataStrings.INLINE_MAX
            DataStrings.inline_payload(bytes, 1, n)
        else
            push!(buffers, bytes)
            DataStrings.view_payload(bytes, 1, n, length(buffers) - 1, 0)
        end
    end
    return DataStrings.BytesVector{E}(payloads, buffers, Val(:trusted))
end

# The `n + 1` offsets of list or map batch `d` as owned `Int`s, plus `base`
# (a map's entries struct offset).
function _listoffsets(d::AC.ArrayData, base::Int, budget)
    n = Int64(d.len)
    _chargeexact!(budget, Int, n + 1, "list offsets")
    n == 0 && return Int[base]
    buf = AC.rolebuffer(d, AC.OFFSETS)
    out = Vector{Int}(undef, n + 1)
    GC.@preserve d begin
        AC.layoutspec_of(d.type).offsetwidth == 8 ?
        _addbase!(out, _wrapbuffer(Int64, buf, d.offset, n + 1, budget), base) :
        _addbase!(out, _wrapbuffer(Int32, buf, d.offset, n + 1, budget), base)
    end
    return out
end
_addbase!(out, offsets, base::Int) = (out.=offsets .+ base; out)

# Not specialized on the row type, so it compiles once for every nested
# column; the per-row loops it calls are function barriers specialized on
# the row and column types.
function _nestedbatch(
    @nospecialize(T::Type),
    f::AC.Field,
    d::AC.ArrayData,
    ctx::_ViewContext,
)
    t = f.type
    R = Base.nonmissingtype(T)
    budget = ctx.budget
    _chargeobject!(budget, _VIEW_OBJECT_BYTES, "column view")
    t isa AC.UnionType && return _unionrows(T, f, d, ctx)
    validity, bitoffset = _validitybits(d)
    if t isa AC.StructType
        children = Vector{AbstractVector}(undef, length(f.children))
        for (k, c) in enumerate(f.children)
            children[k] = _childcolumn(c, d.children[k], ctx, fieldtype(R, k))
        end
        return StructVector{T}(children, Int(d.offset), Int(d.len), validity, bitoffset, d)
    end
    if t isa AC.MapType
        entries, ed = f.children[1], d.children[1]
        K, V = fieldtypes(eltype(R))
        keys = _childcolumn(entries.children[1], ed.children[1], ctx, K)
        vals = _childcolumn(entries.children[2], ed.children[2], ctx, V)
        offsets = _listoffsets(d, Int(ed.offset), budget)
        return MapVector{T}(offsets, keys, vals, validity, bitoffset, d)
    end
    R <: Array && return _tensorrows(T, f, d, ctx, _tensorshape(f)::Tuple)
    E = eltype(R)
    if t isa AC.FixedSizeListType && R <: Tuple
        # `NTuple{0,E}` is `Tuple{}`, whose rows read no child values.
        N = fieldcount(R)
        N == 0 && return _tuplerows(T, Val(0), nothing, d, budget)
        child = _viewcolumn(f.children[1], AC.ArrayData[d.children[1]], ctx, E)
        return _tuplerows(T, Val(N), child, d, budget)
    end
    values = _childcolumn(f.children[1], d.children[1], ctx, E)
    t isa AC.ListViewType &&
        return _listviewvector(T, d, values, validity, bitoffset, budget)
    offsets = if t isa AC.FixedSizeListType
        w = t.listsize
        _chargeexact!(budget, Int, d.len + 1, "fixed-size list offsets")
        Int[(Int(d.offset) + i) * w for i = 0:(d.len)]
    else
        _listoffsets(d, 0, budget)
    end
    return ListVector{T}(offsets, values, validity, bitoffset, d)
end

# A list view batch: rows may overlap or come in any order, so the child
# values of each row are gathered in row order (their own storage keeps
# flat values only; see `_viewablelayout`).
function _listviewvector(
    @nospecialize(T::Type),
    d::AC.ArrayData,
    values,
    validity,
    bitoffset,
    budget,
)
    n = Int64(d.len)
    _chargeexact!(budget, Int, n + 1, "list view offsets")
    offsets = zeros(Int, n + 1)
    n == 0 &&
        return ListVector{T}(offsets, _gathered(values, Int[]), validity, bitoffset, d)
    wide = AC.layoutspec_of(d.type).offsetwidth == 8
    starts, sizes = GC.@preserve d (
        wide ?
        (
            Int.(_wrapbuffer(Int64, AC.rolebuffer(d, AC.ELEMENT_OFFSETS), d.offset, n)),
            Int.(_wrapbuffer(Int64, AC.rolebuffer(d, AC.SIZES), d.offset, n)),
        ) :
        (
            Int.(_wrapbuffer(Int32, AC.rolebuffer(d, AC.ELEMENT_OFFSETS), d.offset, n)),
            Int.(_wrapbuffer(Int32, AC.rolebuffer(d, AC.SIZES), d.offset, n)),
        )
    )
    for i = 1:n
        len = _isnullslot(validity, bitoffset, i) ? 0 : sizes[i]
        offsets[i + 1] = offsets[i] + len
    end
    _chargeexact!(budget, Int, offsets[end], "list view value index")
    take = Vector{Int}(undef, offsets[end])
    for i = 1:n
        for k = 1:(offsets[i + 1] - offsets[i])
            take[offsets[i] + k] = starts[i] + k
        end
    end
    return ListVector{T}(offsets, _gathered(values, take), validity, bitoffset, d)
end

_gathered(values::Vector, take) = values[take]
_gathered(v::DataStrings.StringVector{E}, take) where {E} =
    DataStrings.StringVector{E}(v.payloads[take], v.buffers, Val(:trusted))
_gathered(v::DataStrings.BytesVector{E}, take) where {E} =
    DataStrings.BytesVector{E}(v.payloads[take], v.buffers, Val(:trusted))

function _tuplerows(::Type{T}, ::Val{N}, child, d::AC.ArrayData, budget) where {T,N}
    n = Int(d.len)
    _chargeexact!(budget, T, n, "fixed-size list rows")
    out = Vector{T}(undef, n)
    validity, bitoffset = _validitybits(d)
    GC.@preserve validity child for i = 1:n
        if Missing <: T && _isnullslot(validity, bitoffset, i)
            out[i] = missing
        else
            base = (Int(d.offset) + i - 1) * N
            out[i] = ntuple(j -> child[base + j], Val(N))
        end
    end
    return out
end

# The value at child index `j` of the `k`th column, unrolled over the tuple.
_pickchild(cols::Tuple, k::Int, j::Int) =
    k == 1 ? first(cols)[j] : _pickchild(Base.tail(cols), k - 1, j)
_pickchild(::Tuple{}, ::Int, ::Int) = throw(ValidationError("union child out of range"))

function _unionrows(@nospecialize(T::Type), f::AC.Field, d::AC.ArrayData, ctx::_ViewContext)
    t = f.type::AC.UnionType
    budget = ctx.budget
    cols = Tuple(
        _viewcolumn(
            c,
            AC.ArrayData[d.children[k]],
            ctx,
            _vieweltype(c, AC.ArrayData[d.children[k]], ctx),
        ) for (k, c) in enumerate(f.children)
    )
    n = Int(d.len)
    _chargeexact!(budget, T, n, "union rows")
    out = Vector{T}(undef, n)
    ids = _wrapbuffer(Int8, AC.rolebuffer(d, AC.TYPE_IDS), d.offset, n, budget)
    dense = t.mode == AC.DenseMode
    offsets =
        dense ?
        _wrapbuffer(Int32, AC.rolebuffer(d, AC.ELEMENT_OFFSETS), d.offset, n, budget) :
        Int32[]
    GC.@preserve ids offsets d _fillunion!(
        out,
        cols,
        ids,
        offsets,
        t.typeids,
        dense,
        Int(d.offset),
    )
    return out
end

# Specialized on the children's column types (a function barrier).
function _fillunion!(out, cols::Tuple, ids, offsets, typeids, dense::Bool, base::Int)
    for i in eachindex(out)
        k = findfirst(==(ids[i]), typeids)
        k === nothing &&
            throw(ValidationError("union type id $(ids[i]) not in declared domain"))
        j = dense ? Int(offsets[i]) + 1 : base + i
        out[i] = _pickchild(cols, k, j)
    end
    return out
end

# --- fixed-shape tensors ----------------------------------------------------------
#
# The canonical `arrow.fixed_shape_tensor` extension stores each tensor as a
# fixed-size list of its elements in row-major order over its physical
# `shape`; `permutation` says which physical dimension each logical one is.
# Rows read as `Array{T,N}`s of the logical shape.

const _TENSOR_EXTENSION = "arrow.fixed_shape_tensor"

# `(shape, permutation)` of a tensor field (1-based permutation), or nothing
# when `f` is not one this reader can read: a fixed-size list of a flat,
# unlabelled child with a well-formed `shape`.
function _tensorshape(f::AC.Field)
    f.type isa AC.FixedSizeListType || return nothing
    ext = _arrowtypesextension(f)
    (ext === nothing || ext[1] != _TENSOR_EXTENSION) && return nothing
    (length(f.children) == 1 && !_hasarrowtypesextension(f.children[1])) || return nothing
    child = f.children[1]
    (_viewablelayout(child) && !_nestedvalue(child)) || return nothing
    shape = _jsonints(ext[2], "shape")
    shape === nothing && return nothing
    perm = _jsonints(ext[2], "permutation")
    perm = perm === nothing ? collect(1:length(shape)) : perm .+ 1
    (
        all(>=(0), shape) &&
        length(perm) == length(shape) &&
        isperm(perm) &&
        prod(shape; init=1) == f.type.listsize
    ) || return nothing
    return (Tuple(shape), Tuple(perm))
end

# The integers of JSON array `key` in `json`, or nothing.
function _jsonints(json::AbstractString, key::String)
    m = match(Regex("\"" * key * "\"\\s*:\\s*\\[([^\\]]*)\\]"), json)
    m === nothing && return nothing
    values = Int[]
    for part in split(m[1], ','; keepempty=false)
        x = tryparse(Int, strip(part))
        x === nothing && return nothing
        push!(values, x)
    end
    return values
end

# Tensor rows are `Array{E,N}`: `E` is the element type, without `Missing`
# unless an element of a non-null tensor is null.
function _tensortype(f::AC.Field, ctx::_ViewContext, datas, shape)
    child = f.children[1]
    B = _viewbasetype(child, ctx, _childdatas(datas, 1))
    w = (f.type::AC.FixedSizeListType).listsize
    nulls = any(d -> _tensornulls(child, d, w), datas)
    return Array{nulls ? Union{Missing,B} : B,length(first(shape))}
end

# Whether a non-null row of tensor batch `d` holds a null element.
function _tensornulls(child::AC.Field, d::AC.ArrayData, w::Int)
    c = d.children[1]
    _viewnulls(child, c) || return false
    # Null elements under a null tensor are never read.
    rows, rowoff = _validitybits(d)
    elems, elemoff = _validitybits(c)
    isempty(elems) && return true  # dictionary or run-end values with nulls
    GC.@preserve d for i = 1:Int(d.len)
        _isnullslot(rows, rowoff, i) && continue
        base = (Int(d.offset) + i - 1) * w
        any(j -> _isnullslot(elems, elemoff, base + j), 1:w) && return true
    end
    return false
end

function _tensorrows(@nospecialize(T::Type), f::AC.Field, d::AC.ArrayData, ctx, shapeperm)
    shape, perm = shapeperm
    R = Base.nonmissingtype(T)
    child = _viewcolumn(f.children[1], AC.ArrayData[d.children[1]], ctx, eltype(R))
    return _tensorrows(T, R, child, d, ctx.budget, shape, perm)
end

function _tensorrows(
    ::Type{T},
    ::Type{R},
    child,
    d::AC.ArrayData,
    budget,
    shape,
    perm,
) where {T,R}
    n = Int(d.len)
    w = prod(shape; init=1)
    N = length(shape)
    _chargeexact!(budget, T, n, "tensor rows")
    _chargeexact!(budget, eltype(R), n * w, "tensor values")
    out = Vector{T}(undef, n)
    validity, bitoffset = _validitybits(d)
    for i = 1:n
        if Missing <: T && _isnullslot(validity, bitoffset, i)
            out[i] = missing
            continue
        end
        base = (Int(d.offset) + i - 1) * w
        flat = eltype(R)[child[base + j] for j = 1:w]
        # Row-major storage: reverse the dimensions to reshape, then back.
        physical = permutedims(reshape(flat, reverse(shape)), N:-1:1)
        out[i] = perm == Tuple(1:N) ? physical : permutedims(physical, perm)
    end
    return out
end

# Tensor rows back to their row-major storage values, for a rewrite.
function _tensorstorage(v::AbstractVector, (shape, perm))
    N = length(shape)
    return [
        row isa AbstractArray && ndims(row) == N ?
        vec(
            permutedims(perm == Tuple(1:N) ? row : permutedims(row, invperm(perm)), N:-1:1),
        ) : row for row in v
    ]
end
