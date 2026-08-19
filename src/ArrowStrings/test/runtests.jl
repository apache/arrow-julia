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

using Test, Random
using ArrowStrings
const AS = ArrowStrings

# --- helpers (top-level so allocation probes measure the loop, not closures) --

function asfrombytes(bytes::Vector{UInt8})
    n = length(bytes)
    n <= AS.INLINE_MAX && return ArrowString(AS.inline_payload(bytes, 1, n), AS.EMPTY_BYTES)
    return ArrowString(AS.view_payload(bytes, 1, n, 0, 0), bytes)
end

function asscratchbytes(s::ArrowString)
    r = Ref(AS._payload_scratch(s))
    out = Vector{UInt8}(undef, 16)
    GC.@preserve r begin
        p = Ptr{UInt8}(Base.unsafe_convert(Ptr{Tuple{UInt64, UInt64}}, r))
        unsafe_copyto!(pointer(out), p, 16)
    end
    return out
end

function foldashash(v, h::UInt)
    @inbounds for x in v
        h = hash(x, h)
    end
    return h
end

function foldascmp(v)
    s = 0
    @inbounds for i in 2:length(v)
        s += cmp(v[i - 1], v[i]) + (v[i - 1] == v[i])
    end
    return s
end

function sumncodeunits(c::ArrowStringVector{ArrowString})
    t = 0
    for i in eachindex(c)
        t += ncodeunits(c[i])
    end
    return t
end

# Allocation is measured INSIDE type-stable top-level functions: an
# `@allocated` in testset scope can charge the closure's own boxing (16
# bytes on Julia 1.10) to the kernel it is measuring.
allocated_hash(v::Vector{ArrowString}) = @allocated(foldashash(v, UInt(9)))
allocated_cmp(v::Vector{ArrowString}) = @allocated(foldascmp(v))
allocated_access(c::ArrowStringVector{ArrowString}) = @allocated(sumncodeunits(c))

# A column from Strings, laid out the CSV way: inline when it fits, else a
# view into buffer 0 (`buf`), or into buffer 1 (`extra`) when `inextra(i)`.
function column(strings::Vector; inextra = i -> false)
    buf = UInt8[]
    extra = UInt8[]
    payloads = ArrowStringPayload[]
    for (i, s) in enumerate(strings)
        if s === missing
            push!(payloads, AS.PAYLOAD_MISSING)
            continue
        end
        bytes = Vector{UInt8}(codeunits(s))
        n = length(bytes)
        if n <= AS.INLINE_MAX
            push!(payloads, AS.inline_payload(bytes, 1, n))
        else
            target = inextra(i) ? extra : buf
            off0 = length(target)
            append!(target, bytes)
            push!(payloads, AS.view_payload(target, off0 + 1, n, inextra(i) ? 1 : 0, off0))
        end
    end
    ELT = any(ismissing, strings) ? Union{Missing, ArrowString} : ArrowString
    return ArrowStringVector{ELT}(payloads, buf, extra)
end

@testset "ArrowStrings" begin

@testset "payload layout is an Arrow StringView entry" begin
    # inline: bytes 0..3 length, 4..15 content zero-padded
    p = AS.inline_payload(Vector{UInt8}(codeunits("hello")), 1, 5)
    words = reinterpret(UInt8, [htol(p.a), htol(p.b)])
    @test words[1:4] == reinterpret(UInt8, Int32[5])
    @test words[5:9] == codeunits("hello")
    @test all(iszero, words[10:16])
    @test AS.payloadlength(p) == 5
    # view: bytes 0..3 length, 4..7 prefix, 8..11 buffer index, 12..15 offset
    data = Vector{UInt8}(codeunits("xxthirteen-bytesyy"))
    q = AS.view_payload(data, 3, 13, 1, 2)
    words = reinterpret(UInt8, [htol(q.a), htol(q.b)])
    @test words[1:4] == reinterpret(UInt8, Int32[13])
    @test words[5:8] == codeunits("thir")
    @test words[9:12] == reinterpret(UInt8, Int32[1])
    @test words[13:16] == reinterpret(UInt8, Int32[2])
    @test AS.payloadlength(q) == 13 && AS.payloadbufidx(q) == 1 && AS.payloadoffset(q) == 2 && AS.payloadpos(q) == 3
    # missing
    @test AS.payloadlength(AS.PAYLOAD_MISSING) == -1
    # rebase moves the offset only
    r = AS.rebase_payload(q, 100)
    @test AS.payloadbufidx(r) == 1 && AS.payloadoffset(r) == 102 && r.a == q.a
    # words that do not fit Arrow's Int32 refuse; the boundaries are accepted
    @test_throws ArgumentError AS.view_payload(data, 3, 13, 0, Int64(typemax(Int32)) + 1)
    @test_throws ArgumentError AS.view_payload(data, 3, 13, -1, 0)
    @test_throws ArgumentError AS.rebase_payload(q, Int64(typemax(Int32)))
    @test AS.payloadoffset(AS.view_payload(data, 3, 13, 0, Int64(typemax(Int32)))) == typemax(Int32)
    @test AS.payloadbufidx(AS.view_payload(data, 3, 13, Int64(typemax(Int32)), 0)) == typemax(Int32)
    # ... and so does a length outside (12, typemax(Int32)] — an oversized
    # length would otherwise wrap into the null marker
    @test AS.payloadlength(AS.view_payload(data, 3, Int(typemax(Int32)), 0, 0)) == typemax(Int32)
    @test_throws ArgumentError AS.view_payload(data, 3, Int(typemax(Int32)) + 1, 0, 0)
    @test_throws ArgumentError AS.view_payload(data, 3, 12, 0, 0)
    @test_throws ArgumentError AS.view_payload(data, 3, -1, 0, 0)
    @test_throws ArgumentError AS.inline_payload(data, 1, 13)
    @test_throws ArgumentError AS.inline_payload(data, 1, -1)
    # the byte-loop inline fallback (near the end of the buffer) agrees with
    # the two-load fast path
    long = Vector{UInt8}(codeunits("abcdefghijklmnopqrstuvwxyz"))
    for len in 0:12, pos in 1:(length(long) - len + 1)
        fast = AS.inline_payload(long, pos, len)
        tail = long[pos:(pos + len - 1)]
        slow = AS.inline_payload(tail, 1, len)      # pos + 11 > length ⇒ byte loop
        @test fast == slow
    end
end

@testset "ArrowString: equality, hashing, ordering agree with String" begin
    # inline/view boundary: 12 bytes inline, 13 views the buffer
    col = column(["x"^12, "y"^13])
    @test col isa ArrowStringVector{ArrowString}
    @test col[1] == "x"^12 && col[2] == "y"^13
    @test ncodeunits(col[1]) == 12 && ncodeunits(col[2]) == 13
    @test String(col[1]) == "x"^12 && String(col[2]) == "y"^13
    @test col[1] == "x"^12 && "x"^12 == col[1]
    @test isequal(col[1], "x"^12) && hash(col[1]) == hash("x"^12)
    d = Dict("x"^12 => 1)
    @test d[col[1]] == 1
    d2 = Dict(col[2] => 2)
    @test d2["y"^13] == 2
    @test sort([col[2], col[1]]) == [col[1], col[2]]
    @test cmp(col[1], col[2]) == cmp("x"^12, "y"^13)

    # Exhaust the payload-length byte with data that includes NUL, invalid
    # UTF-8, all-one bytes, and sentinel-like runs; odd out-of-line lengths
    # use buffer index 1, even ones buffer index 0 — hashing must not care.
    pattern = UInt8[0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                    0xff, 0xff, 0xff, 0xff, 0x80, 0xc0, 0x7f, 0x41, 0xfe]
    strings = String[]
    payloads = ArrowString[]
    for n in 0:255
        bytes = UInt8[xor(pattern[mod1(i, length(pattern))], UInt8(i % 251)) for i in 1:n]
        push!(strings, String(copy(bytes)))
        if n <= AS.INLINE_MAX
            push!(payloads, ArrowString(AS.inline_payload(bytes, 1, n), AS.EMPTY_BYTES))
        else
            data = vcat(UInt8[0x11], bytes, UInt8[0x22])
            bufidx = isodd(n) ? 1 : 0
            push!(payloads, ArrowString(AS.view_payload(data, 2, n, bufidx, 1), data))
        end
    end
    seeds = UInt[0, 1, 7, typemax(UInt), 0x0123456789abcdef]
    @test all(hash(payloads[i], h) == hash(strings[i], h)
              for i in eachindex(payloads), h in seeds)
    @test all([codeunit(payloads[i], j) for j in 1:ncodeunits(payloads[i])] ==
              collect(codeunits(strings[i])) for i in eachindex(payloads))
    @test all(begin
        n = ncodeunits(payloads[i])
        bytes = asscratchbytes(payloads[i])
        bytes[1:n] == collect(codeunits(strings[i])) &&
            all(iszero, bytes[(n + 1):end])
    end for i in 1:(AS.INLINE_MAX + 1))
    @test all(cmp(payloads[i], payloads[j]) == cmp(strings[i], strings[j]) &&
              isless(payloads[i], payloads[j]) == isless(strings[i], strings[j]) &&
              cmp(payloads[i], strings[j]) == cmp(strings[i], strings[j]) &&
              cmp(strings[i], payloads[j]) == cmp(strings[i], strings[j]) &&
              (payloads[i] == payloads[j]) == (strings[i] == strings[j]) &&
              (payloads[i] == strings[j]) == (strings[i] == strings[j])
              for i in eachindex(payloads), j in eachindex(payloads))
    @test sortperm(payloads) == sortperm(strings)
    valid = ["a", "abcdefgh1234", "abcdefgh12345", "α", "漢字", "z"^40, "a\0b"]
    validcs = [asfrombytes(Vector{UInt8}(codeunits(s))) for s in valid]
    substrings = [SubString("!" * s * "?", 2,
                            prevind("!" * s * "?", lastindex("!" * s * "?"))) for s in valid]
    @test all(cmp(validcs[i], substrings[j]) == cmp(valid[i], String(substrings[j])) &&
              cmp(substrings[j], validcs[i]) == cmp(String(substrings[j]), valid[i]) &&
              (validcs[i] == substrings[j]) == (valid[i] == String(substrings[j]))
              for i in eachindex(validcs), j in eachindex(substrings))
    @test isless(first(validcs), missing) == isless(first(valid), missing)
    @test isless(missing, first(validcs)) == isless(missing, first(valid))
    # hashing and comparing across every inline/view mix never allocates
    allocated_hash(payloads)
    @test allocated_hash(payloads) == 0
    allocated_cmp(payloads)
    @test allocated_cmp(payloads) == 0
    # Symbol, promotion, print
    @test Symbol(asfrombytes(Vector{UInt8}(codeunits("αβγδεζηθικλμ")))) == :αβγδεζηθικλμ
    @test promote_type(ArrowString, String) === String
    io = IOBuffer()
    print(io, col[2])
    @test String(take!(io)) == "y"^13
    @test convert(String, col[1]) == "x"^12
end

@testset "ArrowString: iteration and character indexing match String" begin
    rng = MersenneTwister(99)
    for _ in 1:200
        n = rand(rng, 0:24)
        bytes = rand(rng, UInt8, n)
        s = String(copy(bytes))
        v = asfrombytes(copy(bytes))
        @test collect(v) == collect(s)
        @test v == s && hash(v) == hash(s)
        @test length(v) == length(s)
    end
    # Character-index APIs use the same tolerant partition as String: a bare
    # continuation byte is its own invalid Char and starts at a valid index; a
    # continuation consumed by a preceding lead byte does not.
    invalidcases = (UInt8[0x80], UInt8[0x61, 0x80, 0x62], UInt8[0xc2],
                    UInt8[0xc2, 0x41], UInt8[0xe0, 0x80],
                    UInt8[0xf0, 0x80, 0x41], UInt8[0xc2, 0x80],
                    Vector{UInt8}(codeunits("thirteen-bytes-and-then-α-β")))
    result(f) = try
        (:value, f())
    catch e
        (:error, typeof(e))
    end
    for bytes in invalidcases
        s = String(copy(bytes))
        v = asfrombytes(copy(bytes))
        @test collect(eachindex(v)) == collect(eachindex(s))
        @test lastindex(v) == lastindex(s)
        for i in 0:(length(bytes) + 1)
            @test isvalid(v, i) == isvalid(s, i)
            @test result(() -> thisind(v, i)) == result(() -> thisind(s, i))
            @test result(() -> nextind(v, i)) == result(() -> nextind(s, i))
            @test result(() -> prevind(v, i)) == result(() -> prevind(s, i))
            @test result(() -> v[i]) == result(() -> s[i])
        end
        for i in 1:length(bytes), j in i:length(bytes)
            @test result(() -> String(SubString(v, i, j))) ==
                  result(() -> String(SubString(s, i, j)))
        end
    end
end

@testset "ArrowStringVector: buffers, missing, materialize, allocation" begin
    strings = ["value$(i)_" * "p"^(i % 20) for i in 1:1000]
    col = column(strings; inextra = i -> i % 3 == 0)
    @test col isa ArrowStringVector{ArrowString}
    @test length(col) == 1000 && length(col.buffers) == 2
    @test collect(String, col) == strings
    # long values landed in the buffer their index says
    for (i, s) in enumerate(strings)
        ncodeunits(s) > AS.INLINE_MAX || continue
        @test AS.payloadbufidx(col.payloads[i]) == (i % 3 == 0 ? 1 : 0)
    end
    allocated_access(col)
    @test allocated_access(col) == 0
    m = AS.materialize(col)
    @test m isa Vector{String} && m == strings

    withmissing = Any["a", missing, "twelve-bytes", "a much longer value", missing]
    mcol = column(withmissing; inextra = i -> i == 4)
    @test mcol isa ArrowStringVector{Union{Missing, ArrowString}}
    @test isequal(collect(mcol), withmissing)
    @test AS.materialize(mcol) isa Vector{Union{String, Missing}}
    @test isequal(AS.materialize(mcol), withmissing)
    @test eltype(mcol) === Union{Missing, ArrowString}

    # N buffers: an Arrow Utf8View column may spread views over any number of
    # data buffers; the buffer index selects among them
    b0 = Vector{UInt8}(codeunits("--first-buffer-value--"))
    b1 = Vector{UInt8}(codeunits("second-buffer-value!!"))
    b2 = Vector{UInt8}(codeunits("xxthird buffer, longer value"))
    payloads = ArrowStringPayload[
        AS.view_payload(b0, 3, 18, 0, 2),
        AS.view_payload(b1, 1, 19, 1, 0),
        AS.view_payload(b2, 3, 26, 2, 2),
        AS.inline_payload(b1, 1, 6),
    ]
    ncol = ArrowStringVector{ArrowString}(payloads, Vector{UInt8}[b0, b1, b2])
    @test collect(String, ncol) == ["first-buffer-value", "second-buffer-value", "third buffer, longer value", "second"]
    # a payload naming a buffer the column does not have is a clean error
    bad = ArrowStringVector{ArrowString}([AS.view_payload(b0, 3, 18, 7, 2)], Vector{UInt8}[b0])
    @test_throws BoundsError bad[1]
    # rebase: appending one buffer to another re-points its entries
    combined = vcat(b0, b1)
    rebased = AS.rebase_payload(payloads[2], length(b0))
    @test String(ArrowString(rebased, combined)) == "second-buffer-value"
end

end
