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
using ArrowStrings

const AC = Arrow.AC
const UNUSED = "UNUSED_SYNTHETIC_BUFFER_TEXT"
const LONG = "selected long string \0 with λ"

function fixture()
    raw = Vector{UInt8}(codeunits(UNUSED * LONG * UNUSED))
    extra = Vector{UInt8}(codeunits(UNUSED * "another selected string" * UNUSED))
    p = ArrowStrings.view_payload(
        raw,
        ncodeunits(UNUSED) + 1,
        ncodeunits(LONG),
        0,
        ncodeunits(UNUSED),
    )
    q = ArrowStrings.view_payload(extra, ncodeunits(UNUSED) + 1, 23, 2, ncodeunits(UNUSED))
    inline = ArrowStrings.inline_payload(codeunits("λ\0ok"), 1, 5)
    payloads = [p, ArrowStrings.PAYLOAD_MISSING, inline, q, p]
    buffers = [raw, Vector{UInt8}(codeunits(UNUSED)), extra]
    return StringVector{Union{Missing,ArrowString}}(payloads, buffers)
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
                selected = StringVector{Union{Missing,ArrowString}}(
                    col.payloads[selection],
                    col.buffers,
                )
                expected = ArrowStrings.materialize(selected)
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
            @test isequal(Arrow.Table(bytes).s, ArrowStrings.materialize(col)[3:5])
            @test findfirst(codeunits(UNUSED), bytes) === nothing
        end
    end

    @testset "overlapping and repeated ranges retain sharing" begin
        raw = Vector{UInt8}(codeunits(UNUSED * "abcdefghijklmnopqrstuvwxyz" * UNUSED))
        off = ncodeunits(UNUSED)
        payloads = [
            ArrowStrings.view_payload(raw, off + i + 1, n, 0, off + i) for
            (i, n) in [(8, 18), (0, 20), (4, 13), (0, 20)]
        ]
        col = StringVector{ArrowString}(payloads, [raw])
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
            payloads = ArrowStringPayload[]
            expected = Union{Missing,String}[]
            for _ = 1:50
                if rand(rng, Bool)
                    bufidx = rand(rng, 1:3)
                    off = rand(rng, 0:200)
                    len = rand(rng, 13:56)
                    push!(
                        payloads,
                        ArrowStrings.view_payload(
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
                    push!(payloads, ArrowStrings.PAYLOAD_MISSING)
                    push!(expected, missing)
                end
            end
            col = StringVector{Union{Missing,ArrowString}}(payloads, buffers)
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
            @test isequal(AC.materialize(f, out.dictionary), ArrowStrings.materialize(col))
        end
    end
end

end # module IPCViewOutputTests
