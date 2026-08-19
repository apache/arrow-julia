# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
    ArrowStrings

The inline-else-view string representation shared by Arrow.jl and CSV.jl:
`ArrowString`, a 16-byte string value that IS an Arrow StringView entry, and
`ArrowStringVector`, a column of them over a set of byte buffers that IS an
Arrow Utf8View array's memory. A CSV column parsed into this representation
becomes an Arrow column without copying, and an Arrow Utf8View column comes
back the same way.

Every string is one 16-byte payload (`ArrowStringPayload`, two `UInt64`
words `a` and `b`, packed by explicit shifts so the layout is
endianness-independent):

    a  bits 0..31   content length as Int32 (-1 marks a missing value)
       bits 32..63  content bytes 1..4 — the full bytes when the string is
                    inline (length ≤ 12), the 4-byte PREFIX when it is a view
                    (prefixes make equality's fast path branch-free)
    b  length ≤ 12  content bytes 5..12, zero-padded
       length > 12  bits 0..31 an Int32 BUFFER INDEX and bits 32..63 an Int32
                    0-based byte OFFSET of the content within that buffer

That is byte for byte the Arrow StringView layout (12-byte inline, 4-byte
prefix, int32 buffer index + int32 offset). Arrow's Int32 words are why a
buffer must stay under 2 GiB. Byte access, comparison, hashing, and iteration
never allocate; `String(s)` copies out; `materialize(v)` copies a whole
column out to `Vector{String}`. Everything here depends only on Base and is
concrete-typed throughout, so it compiles under JuliaC `--trim`.

Lifetime: a `ArrowString` view pins its buffer (`data`), and a
`ArrowStringVector` pins all of its buffers, exactly like any zero-copy
string view; a consumer that must outlive the source materializes.
"""
module ArrowStrings

export ArrowString, ArrowStringVector, ArrowStringPayload

"""
    ArrowStringPayload

One 16-byte inline-else-view string entry — an Arrow StringView entry (see
[`ArrowStrings`](@ref) for the word layout). `payloadlength(p)` is the length
(negative for missing); for a view, `payloadbufidx(p)` and `payloadoffset(p)` are its
buffer index and 0-based offset, and `payloadpos(p)` the 1-based position.
"""
struct ArrowStringPayload
    a::UInt64
    b::UInt64
end

"The payload of a missing value: length -1."
const PAYLOAD_MISSING = ArrowStringPayload(UInt64(0xffffffff), zero(UInt64))
"Longest string stored inline; longer strings are views into a buffer."
const INLINE_MAX = 12
const EMPTY_BYTES = UInt8[]

@inline payloadlength(p::ArrowStringPayload) = reinterpret(Int32, p.a % UInt32)
@inline payloadbufidx(p::ArrowStringPayload) = reinterpret(Int32, p.b % UInt32)
@inline payloadoffset(p::ArrowStringPayload) = reinterpret(Int32, (p.b >> 32) % UInt32)
@inline payloadpos(p::ArrowStringPayload) = Int(payloadoffset(p)) + 1
@inline _viewword(bufidx::Integer, offset0::Integer) =
    UInt64(bufidx % UInt32) | (UInt64(offset0 % UInt32) << 32)

"""
    inline_payload(src::Vector{UInt8}, pos::Int, len::Int) -> ArrowStringPayload

The payload of the `len` (≤ 12) bytes of `src` starting at 1-based `pos`,
stored inline. Two overlapping little-endian loads gather up to 12 content
bytes branch-free; the byte-loop fallback only runs within 11 bytes of the
buffer's end (loads must not read past it).
"""
@inline function inline_payload(src::Vector{UInt8}, pos::Int, len::Int)
    0 <= len <= INLINE_MAX ||
        throw(ArgumentError("inline_payload: length $len is not in 0:$INLINE_MAX"))
    if pos + 11 <= length(src)
        GC.@preserve src begin
            p = pointer(src, pos)
            lo = ltoh(unsafe_load(Ptr{UInt64}(p)))           # content bytes 1..8
            hi = ltoh(unsafe_load(Ptr{UInt64}(p + 4)))       # content bytes 5..12
        end
        m4 = len >= 4 ? 0x00000000ffffffff : (UInt64(1) << (8 * len)) - 1
        nb = max(len - 4, 0)
        m8 = nb >= 8 ? typemax(UInt64) : (UInt64(1) << (8 * nb)) - 1
        return ArrowStringPayload(UInt64(len % UInt32) | ((lo & m4) << 32), hi & m8)
    end
    a = UInt64(len % UInt32)
    b = zero(UInt64)
    @inbounds for i in 1:min(len, 4)
        a |= UInt64(src[pos + i - 1]) << (32 + 8 * (i - 1))
    end
    @inbounds for i in 5:len
        b |= UInt64(src[pos + i - 1]) << (8 * (i - 5))
    end
    return ArrowStringPayload(a, b)
end

"""
    view_payload(src::Vector{UInt8}, srcpos::Int, len::Int, bufidx, offset0) -> ArrowStringPayload

The payload of a view: `len` (> 12) bytes whose content sits at 1-based
`srcpos` in `src` (where the 4-byte prefix is read from) and is addressed by
the entry's Arrow words — buffer index `bufidx` and 0-based byte `offset0`
within that buffer. Refuses a length or word that does not fit Arrow's
Int32 (buffers must stay under 2 GiB) — an oversized length would otherwise
wrap into the null marker.
"""
@inline function view_payload(src::Vector{UInt8}, srcpos::Int, len::Int,
                              bufidx::Integer, offset0::Integer)
    INLINE_MAX < len <= typemax(Int32) ||
        throw(ArgumentError("view_payload: length $len is not in $(INLINE_MAX + 1):$(typemax(Int32))"))
    (0 <= offset0 <= typemax(Int32) && 0 <= bufidx <= typemax(Int32)) ||
        throw(ArgumentError("ArrowString view (buffer $bufidx, offset $offset0) " *
                            "does not fit Arrow's Int32 view words; buffers must stay under 2 GiB"))
    GC.@preserve src begin
        pre = ltoh(unsafe_load(Ptr{UInt32}(pointer(src, srcpos))))
    end
    a = UInt64(len % UInt32) | (UInt64(pre) << 32)
    return ArrowStringPayload(a, _viewword(bufidx, offset0))
end

"""
    rebase_payload(p::ArrowStringPayload, base::Integer) -> ArrowStringPayload

The same view entry re-pointed `base` bytes further into its buffer — what
concatenating buffers (a chunk's buffer appended to a column's) needs.
"""
@inline function rebase_payload(p::ArrowStringPayload, base::Integer)
    off = Int(payloadoffset(p)) + Int(base)
    0 <= off <= typemax(Int32) ||
        throw(ArgumentError("rebased ArrowString view offset $off does not fit " *
                            "Arrow's Int32 view offset; buffers must stay under 2 GiB"))
    return ArrowStringPayload(p.a, _viewword(payloadbufidx(p), off))
end

"""
    ArrowString <: AbstractString

A string value: its 16-byte payload plus the byte vector a view's content
lives in (a shared empty vector for inline values). Byte access, direct
comparisons, hashing, and iteration do not allocate; they use the inline
bytes or the retained buffer. Hashing and ordering agree with `String`.
`String(s)` copies out.
"""
struct ArrowString <: AbstractString
    p::ArrowStringPayload
    data::Vector{UInt8}    # dereferenced only when the payload is a view
end

Base.ncodeunits(s::ArrowString) = Int(payloadlength(s.p))
Base.codeunit(::ArrowString) = UInt8
Base.@propagate_inbounds function Base.codeunit(s::ArrowString, i::Int)
    @boundscheck 1 <= i <= ncodeunits(s) || throw(BoundsError(s, i))
    len = payloadlength(s.p)
    if len <= INLINE_MAX
        return i <= 4 ? (s.p.a >> (32 + 8 * (i - 1))) % UInt8 :
                        (s.p.b >> (8 * (i - 5))) % UInt8
    else
        return @inbounds s.data[payloadpos(s.p) + i - 1]
    end
end

function Base.isvalid(s::ArrowString, i::Int)
    1 <= i <= ncodeunits(s) || return false
    @inbounds b = codeunit(s, i)
    b & 0xc0 == 0x80 || return true
    i > 1 || return true
    @inbounds b = codeunit(s, i - 1)
    0xc0 <= b <= 0xf7 && return false
    b & 0xc0 == 0x80 && i > 2 || return true
    @inbounds b = codeunit(s, i - 2)
    0xe0 <= b <= 0xf7 && return false
    b & 0xc0 == 0x80 && i > 3 || return true
    @inbounds b = codeunit(s, i - 3)
    return !(0xf0 <= b <= 0xf7)
end

# UTF-8 iteration mirroring `String`'s tolerant behavior: Julia `Char`s ARE the
# UTF-8 bytes left-aligned in 32 bits, and a malformed sequence yields the bytes
# consumed so far as an (invalid) Char.
function Base.iterate(s::ArrowString, i::Int=1)
    i > ncodeunits(s) && return nothing
    @inbounds b1 = codeunit(s, i)
    b1 < 0x80 && return (reinterpret(Char, UInt32(b1) << 24), i + 1)
    l = b1 < 0xc0 ? 1 : b1 < 0xe0 ? 2 : b1 < 0xf0 ? 3 : b1 < 0xf8 ? 4 : 1
    n = ncodeunits(s)
    c = UInt32(b1) << 24
    j = 1
    @inbounds while j < l && i + j <= n
        nb = codeunit(s, i + j)
        (nb & 0xc0) == 0x80 || break
        c |= UInt32(nb) << (24 - 8 * j)
        j += 1
    end
    return (reinterpret(Char, c), i + j)
end

# Base's generic AbstractString length is isvalid-count-based, which undercounts
# malformed inputs (String yields each bare continuation byte as its own invalid
# Char). Count by iteration so length/collect agree with String.
function Base.length(s::ArrowString)
    n = 0
    for _ in s
        n += 1
    end
    return n
end

function Base.:(==)(x::ArrowString, y::ArrowString)
    n = ncodeunits(x)
    n == ncodeunits(y) || return false
    if n <= INLINE_MAX
        return x.p.a == y.p.a && x.p.b == y.p.b   # payload holds the full content
    end
    x.p.a == y.p.a || return false                # length + 4-byte prefix reject
    GC.@preserve x y begin
        return ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t),
                     pointer(x.data, payloadpos(x.p)),
                     pointer(y.data, payloadpos(y.p)), n) == 0
    end
end
# Direct byte comparison against String — Base's generic AbstractString ==
# decodes chars, an order of magnitude slower on this hot path (filtering and
# grouping compare ArrowString columns against String literals constantly).
function Base.:(==)(x::ArrowString, y::Union{String, SubString{String}})
    n = ncodeunits(x)
    n == ncodeunits(y) || return false
    GC.@preserve x y begin
        py = pointer(y)
        if n <= INLINE_MAX
            @inbounds for i in 1:n
                codeunit(x, i) == unsafe_load(py, i) || return false
            end
            return true
        end
        return ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t),
                     pointer(x.data, payloadpos(x.p)), py, n) == 0
    end
end
Base.:(==)(y::Union{String, SubString{String}}, x::ArrowString) = x == y

# Ordering: memcmp over the bytes, exactly like String's `cmp` (Base's generic
# AbstractString fallback iterates chars — 15-45x slower on sortperm).
# Inline×inline compares in registers; view×view goes straight to memcmp on
# the retained buffers; only the mixed case materializes a stack scratch.
# Raw payload words with content byte k at byte k (byte 1 = LSB of w1) —
# bit-defined, so endian-independent.
@inline _payload_words(s::ArrowString) =
    ((s.p.a >> 32) | ((s.p.b & 0xffffffff) << 32), s.p.b >> 32)
@inline _payload_scratch(s::ArrowString) = map(htol, _payload_words(s))
function Base.cmp(x::ArrowString, y::ArrowString)
    nx, ny = ncodeunits(x), ncodeunits(y)
    if (nx <= INLINE_MAX) & (ny <= INLINE_MAX)
        # Register compare in memcmp order: payload words are zero-padded past
        # each length, so the first differing big-endian word decides by the
        # first differing byte; words all equal means the shared prefix
        # matches and any longer side is all-NUL past the shorter — exactly
        # memcmp(min bytes) then the length tiebreak. The non-short-circuit
        # `&` (one branch) and falling into the unified tail below measures
        # strictly faster than a dedicated view×view branch.
        w1x, w2x = _payload_words(x)
        w1y, w2y = _payload_words(y)
        a, b = bswap(w1x), bswap(w1y)
        a == b || return a < b ? -1 : 1
        a, b = bswap(w2x), bswap(w2y)
        a == b || return a < b ? -1 : 1
        return cmp(nx, ny)
    end
    n = min(nx, ny)
    rx = Ref(_payload_scratch(x)); ry = Ref(_payload_scratch(y))
    GC.@preserve x y rx ry begin
        px = nx <= INLINE_MAX ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, rx)) :
             pointer(x.data, payloadpos(x.p))
        py = ny <= INLINE_MAX ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, ry)) :
             pointer(y.data, payloadpos(y.p))
        c = ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), px, py, n)
    end
    return c < 0 ? -1 : c > 0 ? 1 : cmp(nx, ny)
end
function Base.cmp(x::ArrowString, y::Union{String, SubString{String}})
    nx, ny = ncodeunits(x), ncodeunits(y)
    n = min(nx, ny)
    rx = Ref(_payload_scratch(x))
    GC.@preserve x y rx begin
        px = nx <= INLINE_MAX ?
             Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, rx)) :
             pointer(x.data, payloadpos(x.p))
        c = ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), px, pointer(y), n)
    end
    return c < 0 ? -1 : c > 0 ? 1 : cmp(nx, ny)
end
Base.cmp(y::Union{String, SubString{String}}, x::ArrowString) = -cmp(x, y)
Base.isless(x::ArrowString, y::ArrowString) = cmp(x, y) < 0
Base.isless(x::ArrowString, y::Union{String, SubString{String}}) = cmp(x, y) < 0
Base.isless(y::Union{String, SubString{String}}, x::ArrowString) = cmp(y, x) < 0

# hash contract: hash(cs) == hash(String(cs)) — ArrowStrings are Dict keys
# next to Strings. Base hashes a String's bytes through one C routine; we run
# the same routine over the bytes we already have: the retained buffer for
# views, a stack copy of the payload words for inline strings. No String
# allocation on either path. The routine differs across hashing generations:
#   ≤ 1.12  memhash(bytes, n, seed) + seed  with seed = h + memhash_seed
#   ≥ 1.13  hash_bytes(ptr, n, UInt64(h), HASH_SECRET) % UInt   (rapidhash)
# The gate is on the API that exists, not the version number.
@static if isdefined(Base, :hash_bytes) && isdefined(Base, :HASH_SECRET)
    @inline _stringhash(p::Ptr{UInt8}, n::Int, h::UInt) =
        Base.hash_bytes(p, n, UInt64(h), Base.HASH_SECRET) % UInt
else
    @inline function _stringhash(p::Ptr{UInt8}, n::Int, h::UInt)
        h += Base.memhash_seed
        return ccall(Base.memhash, UInt, (Ptr{UInt8}, Csize_t, UInt32), p, n, h % UInt32) + h
    end
end

function Base.hash(s::ArrowString, h::UInt)
    n = ncodeunits(s)
    if n > INLINE_MAX
        GC.@preserve s begin
            return _stringhash(pointer(s.data, payloadpos(s.p)), n, h)
        end
    end
    # inline: bytes 1-4 are the high 32 bits of `a`, bytes 5-12 are `b` —
    # pack them contiguously into two little-endian words: word 1 = bytes 1-8
    # = (a>>32) | (low 32 bits of b) << 32, word 2 = bytes 9-12 = b >> 32
    w1 = (s.p.a >> 32) | ((s.p.b & 0xffffffff) << 32)
    w2 = s.p.b >> 32
    scratch = (htol(w1), htol(w2))
    r = Ref(scratch)
    GC.@preserve r begin
        p = Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, r))
        return _stringhash(p, n, h)
    end
end

function Base.String(s::ArrowString)
    n = ncodeunits(s)
    if n > INLINE_MAX
        # view: one memcpy out of the retained buffer
        GC.@preserve s begin
            return unsafe_string(pointer(s.data, payloadpos(s.p)), n)
        end
    end
    out = Vector{UInt8}(undef, n)
    @inbounds for i in 1:n
        out[i] = codeunit(s, i)
    end
    return String(out)
end
Base.convert(::Type{String}, s::ArrowString) = String(s)
Base.Symbol(s::ArrowString) = Symbol(String(s))
Base.promote_rule(::Type{ArrowString}, ::Type{String}) = String

function Base.write(io::IO, s::ArrowString)
    n = 0
    @inbounds for i in 1:ncodeunits(s)
        n += write(io, codeunit(s, i))
    end
    return n
end
Base.print(io::IO, s::ArrowString) = (write(io, s); nothing)

"""
    ArrowStringVector{ELT}(payloads, buffers::Vector{Vector{UInt8}})
    ArrowStringVector{ELT}(payloads, buf::Vector{UInt8}, extra::Vector{UInt8})

A string column: one payload per element and the byte buffers that view
payloads point into (`buffers[bufidx + 1]` for an entry's buffer index).
`ELT` is `ArrowString` for a column with no missing values, or
`Union{Missing, ArrowString}`. `getindex` returns a `ArrowString` (or
`missing`) with NO allocation; `materialize` copies out to `Vector{String}`.

This is an Arrow Utf8View array's memory: `payloads` is its views buffer and
`buffers` its variadic data buffers, so the column crosses to Arrow (and an
Arrow Utf8View column comes back) without copying. The two-buffer
constructor is the CSV shape: buffer 0 the input, buffer 1 the column's
`extra` buffer of unescaped values.
"""
struct ArrowStringVector{ELT} <: AbstractVector{ELT}
    payloads::Vector{ArrowStringPayload}
    buffers::Vector{Vector{UInt8}}
end
ArrowStringVector{ELT}(payloads::Vector{ArrowStringPayload},
                         buf::Vector{UInt8}, extra::Vector{UInt8}) where {ELT} =
    ArrowStringVector{ELT}(payloads, Vector{UInt8}[buf, extra])

Base.size(v::ArrowStringVector) = size(v.payloads)
Base.@propagate_inbounds @inline function Base.getindex(v::ArrowStringVector{ELT}, i::Int) where {ELT}
    @boundscheck checkbounds(v.payloads, i)
    @inbounds p = v.payloads[i]
    len = payloadlength(p)
    len < 0 && return missing
    len <= INLINE_MAX && return ArrowString(p, EMPTY_BYTES)
    return ArrowString(p, v.buffers[payloadbufidx(p) + 1])
end
# All-present columns skip the missing branch entirely — the concrete return
# type is what lets access compile down to zero allocations.
Base.@propagate_inbounds @inline function Base.getindex(v::ArrowStringVector{ArrowString}, i::Int)
    @boundscheck checkbounds(v.payloads, i)
    @inbounds p = v.payloads[i]
    len = payloadlength(p)
    len <= INLINE_MAX && return ArrowString(p, EMPTY_BYTES)
    return ArrowString(p, v.buffers[payloadbufidx(p) + 1])
end

"""
    materialize(v::ArrowStringVector) -> Vector{String} or Vector{Union{String,Missing}}

Copy every element out to a plain `String`, detaching the result from the
column's buffers.
"""
function materialize(v::ArrowStringVector{ELT}) where {ELT}
    out = Vector{ELT === ArrowString ? String : Union{String, Missing}}(undef, length(v))
    scratch = Vector{UInt8}(undef, 16)   # inline payloads reconstruct via two word stores
    GC.@preserve scratch begin
        q = pointer(scratch)
        @inbounds for i in eachindex(v.payloads)
            p = v.payloads[i]
            len = payloadlength(p)
            if len < 0
                out[i] = missing
            elseif len <= INLINE_MAX
                unsafe_store!(Ptr{UInt64}(q), htol((p.a >> 32) | (p.b << 32)))
                unsafe_store!(Ptr{UInt64}(q + 8), htol(p.b >> 32))
                out[i] = unsafe_string(q, len)
            else
                src = v.buffers[payloadbufidx(p) + 1]
                GC.@preserve src begin
                    out[i] = unsafe_string(pointer(src, payloadpos(p)), len)
                end
            end
        end
    end
    return out
end

end # module
