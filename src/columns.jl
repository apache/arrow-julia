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
for flat Arrow layouts. A column reads its Arrow buffers in place (a
memory-mapped file, the input bytes, or a decompressed buffer) and keeps that
memory alive while the column is reachable, independent of its table.
`copy(column)` returns an owned, mutable vector: a `Vector`, or a
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

# One bulk copy where the layouts match and no slot is null.
function Base.copyto!(
    dest::Vector{T},
    doffs::Integer,
    src::Primitive{T,T,Nothing},
    soffs::Integer,
    n::Integer,
) where {T}
    n == 0 && return dest
    (
        n > 0 &&
        checkbounds(Bool, dest, doffs:(doffs + n - 1)) &&
        checkbounds(Bool, src, soffs:(soffs + n - 1))
    ) || throw(BoundsError(dest, doffs:(doffs + n - 1)))
    isempty(src.validity) || return invoke(
        copyto!,
        Tuple{AbstractArray,Integer,AbstractArray,Integer,Integer},
        dest,
        doffs,
        src,
        soffs,
        n,
    )
    GC.@preserve src copyto!(dest, doffs, src.data, soffs, n)
    return dest
end

Base.copy(v::Primitive{T,T,Nothing}) where {T} =
    isempty(v.validity) ? GC.@preserve(v, copy(v.data)) : collect(T, v)

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

function Base.copy(v::DictEncoded{T}) where {T}
    # Distinct values in first-pool-position order; duplicates and null
    # pool entries remap so the PooledArray's invariants hold.
    invpool = Dict{T,UInt32}()
    pool = T[]
    remap = Vector{UInt32}(undef, length(v.pool))
    for (k, x) in enumerate(v.pool)
        remap[k] = get!(invpool, x) do
            push!(pool, x)
            UInt32(length(pool))
        end
    end
    refs = Vector{UInt32}(undef, length(v))
    for i in eachindex(refs)
        code = @inbounds v.indices[i]
        refs[i] = if code === missing
            get!(invpool, missing) do
                push!(pool, missing)
                UInt32(length(pool))
            end
        else
            @inbounds remap[Int(code) + 1]
        end
    end
    return PooledArrays.PooledArray(PooledArrays.RefArray(refs), invpool, pool)
end

# --- strings and binary -----------------------------------------------------------

const _StringPayload = DataStrings.StringPayload

# Byte runs at most this long fit DataStrings' Int32 view words.
const _MAX_VIEW_BUFFER = Int64(typemax(Int32))

"""
The payloads and data buffers of a `StringVector`/`BytesVector` over one
offsets-based (`utf8`/`binary`, normal or large) batch. Each buffer is one
copy of a run of the referenced bytes; a run stays under 2 GiB so view
offsets fit DataStrings' Int32 words.
"""
function _offsetpayloads!(
    payloads::Vector{_StringPayload},
    buffers::Vector{Vector{UInt8}},
    at::Int,
    d::AC.ArrayData,
    wide::Bool,
    budget,
)
    n = Int64(d.len)
    n == 0 && return payloads
    offs = AC.rolebuffer(d, AC.OFFSETS)
    return wide ?
           _offsetpayloads!(
        payloads,
        buffers,
        at,
        d,
        _wrapbuffer(Int64, offs, d.offset, n + 1, budget),
        budget,
    ) :
           _offsetpayloads!(
        payloads,
        buffers,
        at,
        d,
        _wrapbuffer(Int32, offs, d.offset, n + 1, budget),
        budget,
    )
end

function _offsetpayloads!(
    payloads::Vector{_StringPayload},
    buffers::Vector{Vector{UInt8}},
    at::Int,
    d::AC.ArrayData,
    offsets::Vector{O},
    budget,
) where {O}
    n = Int(d.len)
    data = AC.rolebuffer(d, AC.DATA)
    validity, bitoffset = _validitybits(d)
    GC.@preserve d begin
        i = 1
        while i <= n
            # One buffer per run of rows whose bytes span under 2 GiB.
            lo = Int64(offsets[i])
            j = i
            while j <= n && Int64(offsets[j + 1]) - lo <= _MAX_VIEW_BUFFER
                j += 1
            end
            j == i && throw(
                ArgumentError(
                    "a string longer than 2 GiB does not fit a DataString; " *
                    "read the column with stringtype=String",
                ),
            )
            hi = Int64(offsets[j])
            _chargeexact!(budget, UInt8, hi - lo, "string column data")
            buf = Vector{UInt8}(undef, Int(hi - lo))
            if hi > lo
                src = AC.subslice(data, lo, hi - lo)
                GC.@preserve buf unsafe_copyto!(pointer(buf), AC.sliceptr(src), length(buf))
            end
            push!(buffers, buf)
            bufidx = length(buffers) - 1
            _encodepayloads!(
                payloads,
                at,
                buf,
                offsets,
                lo,
                i,
                j - 1,
                validity,
                bitoffset,
                bufidx,
            )
            i = j
        end
    end
    return payloads
end

@inline _load8(p::Ptr{UInt8}, i::Int) = ltoh(unsafe_load(Ptr{UInt64}(p + i)))

"""
Encode rows `first:last` of an offsets-based string batch as payloads over
`buf`, the copy of the bytes from offset `lo` on (the last buffer pushed to
the column's list). Branch-free: inline and view payloads share their first
word (length plus first four content bytes), and only the second differs.
"""
function _encodepayloads!(
    payloads::Vector{_StringPayload},
    at::Int,
    buf::Vector{UInt8},
    offsets::Vector{O},
    lo::Int64,
    first::Int,
    last::Int,
    validity::Vector{UInt8},
    bitoffset::Int,
    bufidx::Int=0,
) where {O}
    buflen = length(buf)
    viewword = UInt64(bufidx % UInt32)
    GC.@preserve buf begin
        p = pointer(buf)
        @inbounds for k = first:last
            start = Int(Int64(offsets[k]) - lo)
            len = Int(Int64(offsets[k + 1]) - Int64(offsets[k]))
            payload = if start <= buflen - 12
                w0 = _load8(p, start)        # content bytes 1-8
                w1 = _load8(p, start + 4)    # content bytes 5-12
                a =
                    UInt64(len % UInt32) |
                    ((w0 & ((UInt64(1) << (8 * min(len, 4))) - 1)) << 32)
                inline = w1 & ((UInt64(1) << (8 * clamp(len - 4, 0, 8))) - 1)
                view = viewword | (UInt64(start % UInt32) << 32)
                _StringPayload(a, len <= DataStrings.INLINE_MAX ? inline : view)
            else
                # Fewer than 12 bytes remain, so this string is inline.
                DataStrings.inline_payload(buf, start + 1, len)
            end
            payloads[at + k] =
                _isnullslot(validity, bitoffset, k) ? DataStrings.PAYLOAD_MISSING : payload
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

"""
The payloads and data buffers of a `StringVector`/`BytesVector` over one
`utf8_view`/`binary_view` batch: the view entries copied as payloads (the
layouts are identical) and each variadic data buffer copied once.
"""
function _viewpayloads!(
    payloads::Vector{_StringPayload},
    buffers::Vector{Vector{UInt8}},
    at::Int,
    d::AC.ArrayData,
    budget,
)
    n = Int(d.len)
    base = length(buffers)
    for k = 3:length(d.buffers)
        _chargeexact!(budget, UInt8, d.buffers[k].len, "string column data")
        push!(buffers, AC.slicebytes(d.buffers[k]))
    end
    n == 0 && return payloads
    views =
        _wrapbuffer(_StringPayload, AC.rolebuffer(d, AC.VIEWS), d.offset, Int64(n), budget)
    validity, bitoffset = _validitybits(d)
    GC.@preserve d begin
        for k = 1:n
            if _isnullslot(validity, bitoffset, k)
                payloads[at + k] = DataStrings.PAYLOAD_MISSING
                continue
            end
            p = _canonicalinline(views[k])
            if base > 0 && DataStrings.payloadlength(p) > DataStrings.INLINE_MAX
                # Re-point at this batch's buffers within the column's list.
                p = _StringPayload(
                    p.a,
                    UInt64((DataStrings.payloadbufidx(p) + base) % UInt32) |
                    (UInt64(DataStrings.payloadoffset(p) % UInt32) << 32),
                )
            end
            payloads[at + k] = p
        end
    end
    return payloads
end

"One `StringVector`/`BytesVector` over every batch of a string or binary column."
function _payloadcolumn(::Type{V}, datas, budget=nothing) where {V}
    n = sum(d -> Int(d.len), datas; init=0)
    _chargeexact!(budget, _StringPayload, n, "string column payloads")
    payloads = Vector{_StringPayload}(undef, n)
    buffers = Vector{UInt8}[]
    at = 0
    for d in datas
        t = d.type
        t isa AC.ViewType ? _viewpayloads!(payloads, buffers, at, d, budget) :
        _offsetpayloads!(payloads, buffers, at, d, t.large, budget)
        at += Int(d.len)
    end
    # Every payload was built here from validated geometry.
    return V(payloads, buffers, Val(:trusted))
end

"One `Vector{String}` (with `missing` where null) over every batch of a string column."
function _stringvector(::Type{T}, datas, budget=nothing) where {T}
    n = sum(d -> Int(d.len), datas; init=0)
    _chargeexact!(budget, T, n, "string column")
    out = Vector{T}(undef, n)
    at = 0
    for d in datas
        col = _payloadcolumn(
            DataStrings.StringVector{Union{Missing,DataStrings.DataString}},
            (d,),
            budget,
        )
        # One String object per value: its bytes plus a header and rounding.
        _charge!(
            budget,
            AC.checked_add(
                AC.checked_mul(Int64(length(col)), Int64(32)),
                sum(b -> Int64(length(b)), col.buffers; init=Int64(0)),
            ),
            "string values",
        )
        for k in eachindex(col)
            x = col[k]
            out[at + k] = x === missing ? missing : String(x)
        end
        at += Int(d.len)
    end
    return out
end

# --- column assembly --------------------------------------------------------------

# Shared decimals read as `DataDecimals.Decimal{P,S,T}`, which has the storage
# layout of every Arrow decimal width.
_isbulkdecimal(f::AC.Field) = f.type isa AC.DecimalType && _shareddecimal(f.type)
"One batch of a decimal column, read as stored."
_decimalcolumn(f::AC.Field, d::AC.ArrayData, budget=nothing) =
    _viewcolumn(f, AC.ArrayData[d], _ViewContext(DataStrings.DataString, budget))

# One read's view settings, its allocation budget, and its cache of
# dictionary pools (built once per pool snapshot).
struct _ViewContext
    stringtype::Type
    budget::Union{Nothing,AllocationBudget}
    pools::IdDict{AC.ArrayData,AbstractVector}
end
_ViewContext(stringtype::Type=DataStrings.DataString, budget=nothing) =
    _ViewContext(stringtype, budget, IdDict{AC.ArrayData,AbstractVector}())

# A view object's own size (struct, wrapped vector headers), charged per batch.
const _VIEW_OBJECT_BYTES = Int64(256)

function _checkstringtype(stringtype)
    stringtype === DataStrings.DataString ||
        stringtype === String ||
        throw(ArgumentError("stringtype must be DataString or String, got $stringtype"))
    return stringtype
end

"""
Whether `f` reads through a view column: a flat layout with no ArrowTypes
extension label. Nested, union, and extension columns keep the materializing
path.
"""
function _viewable(f::AC.Field)
    _hasarrowtypesextension(f) && return false
    return _viewablelayout(f)
end
function _viewablelayout(f::AC.Field)
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
    t isa AC.DictionaryType && return _viewablelayout(AC.dictvaluefield(f, t))
    t isa AC.RunEndEncodedType &&
        return length(f.children) == 2 && _viewablelayout(f.children[2])
    return false
end

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

"The public element type of a view column, before nullability."
function _viewbasetype(f::AC.Field, ctx::_ViewContext)
    t = f.type
    t isa AC.NullType && return Missing
    t isa AC.BoolType && return Bool
    t isa Union{AC.Utf8Type,AC.ViewType} &&
        (!(t isa AC.ViewType) || t.utf8) &&
        return ctx.stringtype === String ? String : DataStrings.DataString
    t isa Union{AC.BinaryType,AC.ViewType} && return DataStrings.DataBytes
    t isa AC.DictionaryType && return _viewbasetype(AC.dictvaluefield(f, t), ctx)
    t isa AC.RunEndEncodedType && return _viewbasetype(f.children[2], ctx)
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
    B = _viewbasetype(f, ctx)
    B === Missing && return Missing
    nullable = Missing <: _declaredeltype(f, true) || any(d -> _viewnulls(f, d), datas)
    return nullable ? Union{Missing,B} : B
end

function _checktimes(f::AC.Field, t::AC.TimeType, d::AC.ArrayData, budget)
    perday = _timeticksperday(t.unit)
    S = t.bits == 32 ? Int32 : Int64
    col = _primitive(Union{Missing,S}, S, Nothing, d, budget)
    for x in col
        (x === missing || 0 <= x < perday) && continue
        throw(
            ValidationError(
                "column $(repr(f.name)): time-of-day value $x is outside [0, $perday) " *
                "in its unit",
            ),
        )
    end
    return nothing
end

"One batch of a fixed-width, boolean, dictionary, or run-end view column."
function _batchview(::Type{T}, f::AC.Field, d::AC.ArrayData, ctx::_ViewContext) where {T}
    t = f.type
    budget = ctx.budget
    _chargeobject!(budget, _VIEW_OBJECT_BYTES, "column view")
    t isa AC.BoolType && return _boolvector(T, d)
    if t isa AC.DictionaryType
        vf = AC.dictvaluefield(f, t)
        dict = d.dictionary::AC.ArrayData
        pool = get!(ctx.pools, dict) do
            _viewcolumn(vf, [dict], ctx)
        end
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
    B = Base.nonmissingtype(T)
    if t isa Union{AC.Utf8Type,AC.BinaryType,AC.ViewType}
        B === String && return _stringvector(T, datas, ctx.budget)
        V =
            B === DataStrings.DataString ? DataStrings.StringVector :
            DataStrings.BytesVector
        ELT = Missing <: T ? Union{Missing,B} : B
        return _payloadcolumn(V{ELT}, datas, ctx.budget)
    end
    isempty(datas) && return T[]
    first = _batchview(T, f, datas[1], ctx)
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
        col = _batchview(T, f, datas[k], ctx)
        col isa V || return ChainedVector(
            AbstractVector{T}[
                k == 1 ? first : _batchview(T, f, d, ctx) for (k, d) in enumerate(datas)
            ],
        )
        cols[k] = col
    end
    return ChainedVector(cols)
end
