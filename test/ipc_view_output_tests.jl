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

module IPCViewOutputTests

using Test
using Random
using Arrow
using DataStrings
using DataStrings: StringVector, StringPayload, BytesVector, DataBytes

const AC = Arrow.AC
const UNUSED = "UNUSED_SYNTHETIC_BUFFER_TEXT"
const LONG = "selected long string \0 with λ"

function fixture()
    raw = Vector{UInt8}(codeunits(UNUSED * LONG * UNUSED))
    extra = Vector{UInt8}(codeunits(UNUSED * "another selected string" * UNUSED))
    p = DataStrings.view_payload(
        raw,
        ncodeunits(UNUSED) + 1,
        ncodeunits(LONG),
        0,
        ncodeunits(UNUSED),
    )
    q = DataStrings.view_payload(extra, ncodeunits(UNUSED) + 1, 23, 2, ncodeunits(UNUSED))
    inline = DataStrings.inline_payload(codeunits("λ\0ok"), 1, 5)
    payloads = [p, DataStrings.PAYLOAD_MISSING, inline, q, p]
    buffers = [raw, Vector{UInt8}(codeunits(UNUSED)), extra]
    return StringVector{Union{Missing,DataString}}(payloads, buffers)
end

function packedfixture()
    first, second = "first selected long string", "second selected long string"
    raw = collect(codeunits(first * second * UNUSED))
    p = DataStrings.view_payload(raw, 1, ncodeunits(first), 0, 0)
    q = DataStrings.view_payload(
        raw,
        ncodeunits(first) + 1,
        ncodeunits(second),
        0,
        ncodeunits(first),
    )
    inline = DataStrings.inline_payload(codeunits("ok"), 1, 2)
    return StringVector{Union{Missing,DataString}}(
        [p, q, p, DataStrings.PAYLOAD_MISSING, inline],
        [raw],
    )
end

function checkoutput(bytes, expected, ndata; file, compress)
    @test isequal(Arrow.Table(bytes).s, expected)
    decoded = file ? Arrow.readfile(bytes) : Arrow.readstream(bytes)
    f = decoded.schema.fields[1]
    d = (file ? decoded[1] : decoded.batches[1]).columns[1]
    @test f.type == AC.ViewType(true)
    @test AC.validate_full(f, d) === d
    @test sum(b.len for b in d.buffers[3:end]; init=0) == ndata
    @test all(b -> findfirst(codeunits(UNUSED), AC.slicebytes(b)) === nothing, d.buffers)
    if compress === nothing
        @test findfirst(codeunits(UNUSED), bytes) === nothing
    end
    return d
end

@testset "IPC view output contains referenced content" begin
    @testset "shared buffers, selections, nulls, and both writers" begin
        col = fixture()
        beforepayloads, beforebuffers = copy(col.payloads), deepcopy(col.buffers)
        for file in (true, false), compress in (nothing, :lz4, :zstd)
            for selection in ([1, 2, 3, 4, 5], [4, 2, 1], [3], [2], Int[])
                selected = StringVector{Union{Missing,DataString}}(
                    col.payloads[selection],
                    col.buffers,
                )
                expected = DataStrings.materialize(selected)
                ndata =
                    (1 in selection || 5 in selection ? ncodeunits(LONG) : 0) +
                    (4 in selection ? 23 : 0)
                io = IOBuffer()
                Arrow.write(io, (s=selected,); file=file, compress=compress)
                eager = take!(io)
                checkoutput(eager, expected, ndata; file=file, compress=compress)
                w = Arrow.Writer(io; file=file, compress=compress)
                Arrow.write(w, (s=selected,))
                close(w)
                @test take!(io) == eager
            end
        end
        @test col.payloads == beforepayloads
        @test col.buffers == beforebuffers
        # Ordinary Julia slices and filters currently materialize selected
        # strings. They must obey the same content boundary.
        for selected in (view(col, 3:5), col[[false, false, true, true, true]])
            io = IOBuffer()
            Arrow.write(io, (s=selected,); file=false)
            bytes = take!(io)
            @test isequal(Arrow.Table(bytes).s, DataStrings.materialize(col)[3:5])
            @test findfirst(codeunits(UNUSED), bytes) === nothing
        end
    end

    @testset "overlapping and repeated ranges retain sharing" begin
        raw = Vector{UInt8}(codeunits(UNUSED * "abcdefghijklmnopqrstuvwxyz" * UNUSED))
        off = ncodeunits(UNUSED)
        payloads = [
            DataStrings.view_payload(raw, off + i + 1, n, 0, off + i) for
            (i, n) in [(8, 18), (0, 20), (4, 13), (0, 20)]
        ]
        col = StringVector{DataString}(payloads, [raw])
        io = IOBuffer()
        Arrow.write(io, (s=col,); file=false)
        d = checkoutput(
            take!(io),
            [
                "ijklmnopqrstuvwxyz",
                "abcdefghijklmnopqrst",
                "efghijklmnopq",
                "abcdefghijklmnopqrst",
            ],
            26;
            file=false,
            compress=nothing,
        )
        views = AC.slicebytes(d.buffers[2])
        @test reinterpret(Int32, views)[8] == reinterpret(Int32, views)[16]
    end

    @testset "low-level views clear unused entry bytes and trailing storage" begin
        # Null entries may contain arbitrary bytes. Short view padding is
        # tolerated by this reader. Neither may survive IPC serialization.
        raw = Vector{UInt8}(codeunits(UNUSED))
        entries = fill(UInt8('Z'), 48)
        entries[1:4] = reinterpret(UInt8, Int32[2])
        entries[5:6] = codeunits("ok")
        entries[17:32] = codeunits("NULL_UNUSED_TEXT")
        for utf8 in (true, false), file in (true, false), compress in (:none, :lz4, :zstd)
            # A one-byte prefix also exercises unaligned borrowed entries.
            borrowed = vcat(UInt8[0xaa], entries)
            utf8 || (borrowed[6] = 0xff)
            t = AC.ViewType(utf8)
            f = AC.Field("s", t; nullable=true)
            d = AC.ArrayData(
                t,
                2,
                [
                    AC._databuffer(UInt8[0xfd, 0xff]),
                    AC.subslice(AC._databuffer(borrowed), 1, length(entries)),
                    AC._databuffer(raw),
                ];
                nullcount=1,
            )
            sch = AC.Schema([f])
            batch = AC.RecordBatch(sch, [d])
            bytes =
                file ? Arrow.writefile(sch, [batch]; compress=compress) :
                Arrow.writestream(sch, [batch]; compress=compress)
            decoded = file ? Arrow.readfile(bytes) : Arrow.readstream(bytes)
            out = (file ? decoded[1] : decoded.batches[1]).columns[1]
            @test length(out.buffers) == 2
            @test AC.slicebytes(out.buffers[1]) == UInt8[1]
            canonical = zeros(UInt8, 32)
            canonical[1] = 2
            canonical[5:6] = codeunits("ok")
            utf8 || (canonical[5] = 0xff)
            @test AC.slicebytes(out.buffers[2]) == canonical
            @test isequal(
                Arrow.Table(bytes).s,
                utf8 ? ["ok", missing] : [UInt8[0xff, 0x6b], missing],
            )
        end
        @test entries[17:32] == codeunits("NULL_UNUSED_TEXT")
    end

    @testset "range unions agree with an independent byte selection" begin
        rng = MersenneTwister(0x719ab)
        for trial = 1:30
            buffers = [rand(rng, UInt8('a'):UInt8('z'), 256) for _ = 1:3]
            used = [falses(length(b)) for b in buffers]
            payloads = StringPayload[]
            expected = Union{Missing,String}[]
            for _ = 1:50
                if rand(rng, Bool)
                    bufidx = rand(rng, 1:3)
                    off = rand(rng, 0:200)
                    len = rand(rng, 13:56)
                    push!(
                        payloads,
                        DataStrings.view_payload(
                            buffers[bufidx],
                            off + 1,
                            len,
                            bufidx - 1,
                            off,
                        ),
                    )
                    used[bufidx][(off + 1):(off + len)] .= true
                    push!(expected, String(buffers[bufidx][(off + 1):(off + len)]))
                else
                    push!(payloads, DataStrings.PAYLOAD_MISSING)
                    push!(expected, missing)
                end
            end
            col = StringVector{Union{Missing,DataString}}(payloads, buffers)
            io = IOBuffer()
            Arrow.write(io, (s=col,); file=false)
            bytes = take!(io)
            @test isequal(Arrow.Table(bytes).s, expected)
            d = Arrow.readstream(bytes).batches[1].columns[1]
            # Boolean indexing is independent of the encoder's sorted-range
            # union and offset-remapping algorithm.
            @test [AC.slicebytes(b) for b in d.buffers[3:end]] == [buffers[i][used[i]] for i = 1:3 if any(used[i])]
        end
    end

    @testset "covered prefixes and gaps preserve the byte boundary" begin
        # Include interleaved buffers, contained/extended overlaps, a late
        # gap, a gap filled by a later row, and a wholly unused buffer.
        cases = [
            [(0, 0, 20), (1, 0, 18), (0, 8, 24), (1, 10, 22), (0, 0, 13), (0, 32, 32)],
            [(0, 0, 20), (1, 0, 18), (0, 32, 32)],
            [(0, 0, 20), (1, 0, 18), (0, 32, 32), (0, 20, 20)],
            [(1, 0, 18), (1, 10, 22)],
        ]
        for ranges in cases, utf8 in (true, false)
            buffers = [
                vcat(repeat(collect(codeunits("abcdefgh")), 8), codeunits(UNUSED)),
                vcat(repeat(collect(codeunits("ijklmnop")), 8), codeunits(UNUSED)),
            ]
            utf8 || (buffers[1][1] = 0xff)
            used = [falses(length(b)) for b in buffers]
            payloads = StringPayload[]
            expected = Any[]
            for (bufidx, off, len) in ranges
                raw = buffers[bufidx + 1]
                push!(payloads, DataStrings.view_payload(raw, off + 1, len, bufidx, off))
                used[bufidx + 1][(off + 1):(off + len)] .= true
                value = raw[(off + 1):(off + len)]
                push!(expected, utf8 ? String(value) : value)
            end
            # A null entry contains a valid-looking reference to unused
            # bytes. It must neither extend coverage nor survive as bytes.
            push!(
                payloads,
                DataStrings.view_payload(buffers[1], 65, ncodeunits(UNUSED), 0, 64),
            )
            push!(expected, missing)
            push!(payloads, DataStrings.inline_payload(codeunits("ok"), 1, 2))
            push!(expected, utf8 ? "ok" : collect(codeunits("ok")))
            n = length(payloads)
            entries = collect(reinterpret(UInt8, payloads))
            entries[(16 * (n - 1) + 7):(16 * n)] .= 0x5a
            append!(entries, codeunits("TRAILING_UNUSED_ENTRIES"))
            # Exercise an unaligned entry buffer and a nonzero slice base
            # for data. BufferSlice bounds, not owner allocation size, apply.
            borrowed = vcat(UInt8[0xaa], entries)
            data = [
                AC.subslice(AC._databuffer(vcat(UInt8[0xaa], b)), 1, length(b)) for
                b in buffers
            ]
            validity = fill(0xff, cld(n, 8) + 1)
            validity[(n - 2) ÷ 8 + 1] &= ~(UInt8(1) << ((n - 2) % 8))
            t = AC.ViewType(utf8)
            f = AC.Field("s", t; nullable=true)
            d = AC.ArrayData(
                t,
                n,
                [
                    AC._databuffer(validity),
                    AC.subslice(AC._databuffer(borrowed), 1, length(entries)),
                    data...,
                ];
                nullcount=1,
            )
            sch = AC.Schema([f])
            batch = AC.RecordBatch(sch, [d])
            original = [AC.slicebytes(b) for b in d.buffers]
            for file in (true, false), compress in (:none, :lz4, :zstd)
                bytes =
                    file ? Arrow.writefile(sch, [batch]; compress=compress) :
                    Arrow.writestream(sch, [batch]; compress=compress)
                decoded = file ? Arrow.readfile(bytes) : Arrow.readstream(bytes)
                out = (file ? decoded[1] : decoded.batches[1]).columns[1]
                @test AC.validate_full(f, out) === out
                @test isequal(Arrow.Table(bytes).s, expected)
                @test [AC.slicebytes(b) for b in out.buffers[3:end]] == [buffers[i][used[i]] for i = 1:2 if any(used[i])]
                outentries = AC.slicebytes(out.buffers[2])
                @test length(outentries) == 16 * n
                @test all(iszero, outentries[(16 * (n - 2) + 1):(16 * (n - 1))])
                @test all(iszero, outentries[(16 * (n - 1) + 7):(16 * n)])
                @test all(
                    b -> findfirst(codeunits(UNUSED), AC.slicebytes(b)) === nothing,
                    out.buffers,
                )
                @test isequal([AC.isvalid_at(out, i) for i = 1:n], [i != n - 1 for i = 1:n])
            end
            @test [AC.slicebytes(b) for b in d.buffers] == original
        end
    end

    @testset "covered dictionary pools retain unreferenced values" begin
        col = packedfixture()
        f, parts = Arrow._constructcolumn(:s, AbstractVector[col])
        pool = only(parts)
        t = AC.DictionaryType(AC.IntType(8, true), f.type, true)
        df = AC.Field("s", t; nullable=false)
        indices = Int8[0, 0, 4]
        d = AC.ArrayData(
            t,
            3,
            [AC.BufferSlice(), AC._databuffer(indices)];
            dictionary=pool,
            nullcount=0,
        )
        sch = AC.Schema([df])
        expectedpool = [
            "first selected long string",
            "second selected long string",
            "first selected long string",
            missing,
            "ok",
        ]
        for file in (true, false), compress in (:none, :lz4, :zstd)
            bytes =
                file ? Arrow.writefile(sch, [AC.RecordBatch(sch, [d])]; compress=compress) :
                Arrow.writestream(sch, [AC.RecordBatch(sch, [d])]; compress=compress)
            decoded = file ? Arrow.readfile(bytes) : Arrow.readstream(bytes)
            out = (file ? decoded[1] : decoded.batches[1]).columns[1]
            @test Arrow.Table(bytes).s ==
                  ["first selected long string", "first selected long string", "ok"]
            @test decoded.schema.fields[1].type == t
            @test AC.slicebytes(out.buffers[2]) == reinterpret(UInt8, indices)
            @test out.dictionary.len == 5
            @test isequal(AC.materialize(f, out.dictionary), expectedpool)
            @test length(out.dictionary.buffers) == 3
            @test AC.slicebytes(out.dictionary.buffers[3]) ==
                  codeunits(expectedpool[1] * expectedpool[2])
        end
    end

    @testset "dictionary pools preserve values and indices" begin
        col = fixture()
        f, parts = Arrow._constructcolumn(:s, AbstractVector[col])
        pool = only(parts)
        t = AC.DictionaryType(AC.IntType(8, true), f.type, true)
        df = AC.Field("s", t; nullable=false)
        indices = Int8[3, 0, 3]
        d = AC.ArrayData(
            t,
            3,
            [AC.BufferSlice(), AC._databuffer(indices)];
            dictionary=pool,
            nullcount=0,
        )
        sch = AC.Schema([df])
        for file in (true, false), compress in (:none, :zstd)
            bytes =
                file ? Arrow.writefile(sch, [AC.RecordBatch(sch, [d])]; compress=compress) :
                Arrow.writestream(sch, [AC.RecordBatch(sch, [d])]; compress=compress)
            @test Arrow.Table(bytes).s ==
                  ["another selected string", LONG, "another selected string"]
            decoded = file ? Arrow.readfile(bytes) : Arrow.readstream(bytes)
            out = (file ? decoded[1] : decoded.batches[1]).columns[1]
            @test decoded.schema.fields[1].type == t
            @test AC.slicebytes(out.buffers[2]) == reinterpret(UInt8, indices)
            @test out.dictionary.len == pool.len
            @test sum(b.len for b in out.dictionary.buffers[3:end]; init=0) ==
                  ncodeunits(LONG) + 23
            @test isequal(AC.materialize(f, out.dictionary), DataStrings.materialize(col))
        end
    end
end

end # module IPCViewOutputTests
