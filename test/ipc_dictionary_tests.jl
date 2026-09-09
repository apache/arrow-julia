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

module IPCDictionaryTests

using Test, Arrow, Tables
const AC = Arrow.AC
const Meta = Arrow.Meta
const FB = Arrow.FB
include("support/SeededFuzz.jl")

# The normal suite consumes independently produced PyArrow 25.0.1 bytes.
# Regenerate with: python test/support/generate_dictionary_fixtures.py
struct BytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::BytesSource) = length(s.data)
Arrow.readrange(s::BytesSource, off, len) = s.data[(off + 1):(off + len)]

const FIXTURES = joinpath(@__DIR__, "fixtures-dictionaries")

function dictmessage!(out, id, vf, pool, delta, codec, state)
    cursor = Arrow.EncodeCursor(codec, state)
    Arrow.encodefield!(cursor, vf, pool)
    builder = FB.Builder(1024)
    rb = Arrow._batchheader!(builder, cursor, pool.len)
    Meta.dictionaryBatchStart(builder)
    Meta.dictionaryBatchAddId(builder, id)
    Meta.dictionaryBatchAddData(builder, rb)
    Meta.dictionaryBatchAddIsDelta(builder, delta)
    header = Meta.dictionaryBatchEnd(builder)
    Meta.messageStart(builder)
    Meta.messageAddVersion(builder, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(builder, Meta.DictionaryBatch)
    Meta.messageAddHeader(builder, header)
    Meta.messageAddBodyLength(builder, Int64(length(cursor.body)))
    Arrow._finishmessage!(out, builder, Meta.messageEnd(builder), cursor.body)
end

# Test-only producer can deliberately emit malformed state transitions. It
# uses the ordinary physical encoder but controls dictionary message flags.
function fixture(vf, events; file=false, features=Int64[], compress=:none)
    t = AC.DictionaryType(AC.IntType(32, true), vf.type, false)
    field = AC.Field("a", t; children=vf.children)
    schema = AC.Schema([field])
    io = IOBuffer()
    state = Arrow.beginwrite!(io, schema; file, features, compress)
    nested = IdDict{AC.Field,AC.ArrayData}()
    function emit!(callback, blocks)
        out = UInt8[]
        if file
            Arrow._fileblock!(state, out, blocks) do
                callback(out)
            end
        else
            callback(out)
        end
        Arrow._publish!(state, out)
    end
    try
        for (delta, pool, indices) in events
            # Nested dictionary dependencies precede their parent. Each new
            # snapshot is a full replacement under the same nested id.
            for (nf, np) in Arrow.dictionarypools((vf,), (pool,))
                get(nested, nf, nothing) === np && continue
                emit!(state.dictblocks) do out
                    dictmessage!(
                        out,
                        state.ids[nf],
                        AC.dictvaluefield(nf, nf.type),
                        np,
                        false,
                        state.codec,
                        state.state,
                    )
                end
                nested[nf] = np
            end
            emit!(state.dictblocks) do out
                dictmessage!(
                    out,
                    state.ids[field],
                    vf,
                    pool,
                    delta,
                    state.codec,
                    state.state,
                )
            end
            indices === nothing && continue
            column = AC.ArrayData(
                t,
                length(indices),
                [AC.BufferSlice(), AC._databuffer(Int32.(indices))];
                dictionary=pool,
                nullcount=0,
            )
            batch = AC.RecordBatch(schema, [column], length(indices))
            emit!(state.recordblocks) do out
                Arrow._recordmessage!(out, batch, schema.fields, state.codec, state.state)
            end
        end
        Arrow.finishwrite!(state)
    finally
        Arrow.abortwrite!(state)
    end
    return take!(io)
end

@testset "PyArrow dictionary updates" begin
    for version in ("V4", "V5")
        bytes = read(joinpath(FIXTURES, "replacement-$version.arrowbytes"))
        @test Arrow.Table(bytes).a == ["alpha", "beta", "gamma", "delta"]
        parts = collect(Arrow.Stream(bytes))
        @test parts[1].a == ["alpha", "beta"]
        @test parts[2].a == ["gamma", "delta"]
    end
    for kind in ("stream", "file")
        bytes = read(joinpath(FIXTURES, "delta-V4-$kind.arrowbytes"))
        @test Arrow.Table(bytes).a == ["beta", "gamma"]
        if kind == "file"
            @test Tables.scan(
                Arrow.SourceFile(BytesSource(bytes); tailbytes=32),
                Tables.Scan(),
            ).a == ["beta", "gamma"]
        end
    end
    pools = (
        "strings" => ["alpha", "beta", "gamma", missing, "delta"],
        "bool" => [true, false, missing, true, false],
        "int" => [1, 2, 3, missing, 4],
        "list" => Any[[1, 2], Int[], [3], missing, [4, 5]],
        "struct" => Any[["x" => 1], ["x" => 2], ["x" => 3], missing, ["x" => 4]],
    )
    for (name, pool) in pools, file in (false, true), codec in ("None", "lz4", "zstd")
        kind = file ? "file" : "stream"
        path = joinpath(FIXTURES, "$name-$kind-$codec.arrowbytes")
        bytes = read(path)
        expected = Any[
            pool[1],
            pool[2],
            missing,
            pool[3],
            pool[1],
            missing,
            pool[4],
            pool[5],
            pool[2],
        ]
        for source in (bytes, path, IOBuffer(bytes))
            @test isequal(collect(Arrow.Table(source).a), expected)
        end
        if file
            for handle in
                (Arrow.readfile(bytes), Arrow.SourceFile(BytesSource(bytes); tailbytes=32))
                @test isequal(collect(Tables.scan(handle, Tables.Scan()).a), expected)
                @test isequal(
                    collect(Tables.scan(handle, Tables.Scan(offset=3, limit=4)).a),
                    expected[4:7],
                )
                @test isequal(
                    collect(
                        Tables.scan(
                            handle,
                            Tables.Scan(filter=Tables.isnull(Tables.col(:a))),
                        ).a,
                    ),
                    filter(ismissing, expected),
                )
            end
        else
            parts = collect(Arrow.Stream(bytes))
            @test isequal(vcat([collect(b.a) for b in parts]...), expected)
            core = Arrow.readstream(bytes)
            snapshots = [b.columns[1].dictionary for b in core.batches]
            @test length.(snapshots) == [2, 3, 5]
            @test snapshots[1] !== snapshots[2] !== snapshots[3]
            @test isequal(parts[1].a, expected[1:3])
        end
    end
end

@testset "Dictionary state transitions" begin
    vf, base = AC.fromjulia("pool", ["alpha", "beta"])
    _, delta = AC.fromjulia("pool", ["gamma"])
    _, empty = AC.fromjulia("pool", String[])
    _, replacement = AC.fromjulia("pool", ["zeta"])
    events = [
        (false, base, [0, 1]),
        (true, delta, [2, 0]),
        (true, empty, [1]),
        (false, replacement, [0]),
        (true, delta, [1, 0]),
    ]
    for features in (Int64[], Int64[1]), compress in (:none, :lz4, :zstd)
        bytes = fixture(vf, events; features, compress)
        @test Arrow.Table(bytes).a ==
              ["alpha", "beta", "gamma", "alpha", "beta", "zeta", "gamma", "zeta"]
        core = Arrow.readstream(bytes)
        pools = [b.columns[1].dictionary for b in core.batches]
        @test pools[2] === pools[3] # empty delta reuses its immutable base
        @test pools[1] !== pools[2]
        @test pools[4] !== pools[5]
        @test AC.materialize(vf, pools[1]) == ["alpha", "beta"]
    end
    for file in (false, true)
        # A delta cannot create its own base, even when empty.
        for pool in (delta, empty)
            bytes = fixture(vf, [(true, pool, nothing)]; file)
            @test_throws AC.ValidationError Arrow.Table(bytes)
            if file
                @test_throws AC.ValidationError Tables.scan(
                    Arrow.SourceFile(BytesSource(bytes); tailbytes=32),
                    Tables.Scan(),
                )
            end
        end
        bytes = fixture(vf, [(false, empty, nothing), (true, delta, [0])]; file)
        @test Arrow.Table(bytes).a == ["gamma"]
        # Index validation sees the new pool length, including every delta.
        bytes = fixture(vf, [(false, base, nothing), (true, delta, [3])]; file)
        @test_throws AC.ValidationError Arrow.Table(bytes)
        if file
            bytes = fixture(vf, [(false, base, [0]), (false, replacement, [0])]; file)
            @test_throws AC.ValidationError Arrow.Table(bytes)
            @test_throws AC.ValidationError Tables.scan(
                Arrow.SourceFile(BytesSource(bytes); tailbytes=32),
                Tables.Scan(),
            )
        end
    end
    @test Arrow._filemetadataversion(Int16(4), Int16(3)) == 3
    @test_throws AC.ValidationError Arrow._filemetadataversion(Int16(3), Int16(4))
    @test_throws AC.ValidationError Arrow._filemetadataversion(Int16(4), Int16(4), Int16(3))
    # Deltas queued before a record batch still append in wire order.
    bytes = fixture(
        vf,
        [(false, base, nothing), (true, delta, nothing), (true, replacement, [3, 2, 1, 0])],
    )
    @test Arrow.Table(bytes).a == ["zeta", "gamma", "beta", "alpha"]
end

@testset "Physical dictionary delta layouts" begin
    for case in (
        SeededFuzz._logical_layout_case(),
        SeededFuzz._physical_layout_case(),
        SeededFuzz._union_layout_case(),
    )
        for (vf, pool) in zip(case.schema.fields, case.batches[1].columns)
            vf.type isa AC.DictionaryType && continue
            @testset "$(vf.name)" begin
                expected = AC.materialize(vf, pool)
                n = pool.len
                bytes = fixture(
                    vf,
                    [(false, pool, collect(0:(n - 1))), (true, pool, collect(n:(2n - 1)))],
                )
                if haskey(ENV, "ARROW_DICTIONARY_ORACLE_DIR")
                    outdir = ENV["ARROW_DICTIONARY_ORACLE_DIR"]
                    mkpath(outdir)
                    write(joinpath(outdir, "$(case.label)-$(vf.name).arrows"), bytes)
                end
                decoded = Arrow.readstream(bytes)
                f = decoded.schema.fields[1]
                first, last = [b.columns[1] for b in decoded.batches]
                @test isequal(AC.materialize(f, first), expected)
                @test isequal(AC.materialize(f, last), expected)
                @test last.dictionary.len == 2n
                @test first.dictionary !== last.dictionary
                @test AC.validate_semantic(f, last) === last
            end
        end
    end
end

@testset "Nested and overlapping dictionary storage" begin
    sf, firstpool = AC.fromjulia("strings", ["first", "second"])
    _, secondpool = AC.fromjulia("strings", ["third", "fourth"])
    nt = AC.DictionaryType(AC.IntType(8, true), sf.type, false)
    nf = AC.Field("item", nt)
    function nested(pool)
        child = AC.ArrayData(
            nt,
            2,
            [AC.BufferSlice(), AC._databuffer(Int8[1, 0])];
            dictionary=pool,
            nullcount=0,
        )
        vf = AC.Field("lists", AC.ListType(false); children=[nf])
        data = AC.ArrayData(
            vf.type,
            1,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 2])];
            children=[child],
            nullcount=0,
        )
        return vf, data
    end
    vf, first = nested(firstpool)
    _, second = nested(secondpool)
    bytes = fixture(vf, [(false, first, [0]), (true, second, [0, 1])])
    @test Arrow.Table(bytes).a ==
          [["second", "first"], ["second", "first"], ["fourth", "third"]]
    core = Arrow.readstream(bytes)
    @test core.batches[1].columns[1].dictionary.children[1].dictionary.len == 2
    @test core.batches[2].columns[1].dictionary.children[1].dictionary.len == 4

    for large in (false, true)
        cf, child = AC.fromjulia("item", Int64[10, 20, 30, 40])
        t = AC.ListViewType(large)
        f = AC.Field("views", t; children=[cf])
        Offset = large ? Int64 : Int32
        pool = AC.ArrayData(
            t,
            3,
            [
                AC.BufferSlice(),
                AC._databuffer(Offset[2, 0, 1]),
                AC._databuffer(Offset[2, 3, 2]),
            ];
            children=[child],
            nullcount=0,
        )
        bytes = fixture(f, [(false, pool, [0, 1, 2]), (true, pool, [3, 4, 5])])
        @test Arrow.Table(bytes).a == repeat([[30, 40], [10, 20, 30], [20, 30]], 2)
    end
    # Narrow unsigned nested indices must not use the signed maximum.
    uinttype = AC.DictionaryType(AC.IntType(8, false), AC.IntType(32, true), false)
    uintfield = AC.Field("items", uinttype)
    uf = AC.Field("outer", AC.ListType(false); children=[uintfield])
    function uintpool(start)
        _, pool = AC.fromjulia("pool", Int32.(start:(start + 99)))
        child = AC.ArrayData(
            uinttype,
            1,
            [AC.BufferSlice(), AC._databuffer(UInt8[99])];
            dictionary=pool,
            nullcount=0,
        )
        return AC.ArrayData(
            uf.type,
            1,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 1])];
            children=[child],
            nullcount=0,
        )
    end
    u1, u2 = uintpool(0), uintpool(100)
    @test Arrow.Table(fixture(uf, [(false, u1, [0]), (true, u2, [0, 1])])).a ==
          [[99], [99], [199]]

    # Same value and Julia type in two Union routes must remain distinguishable.
    c1, d1 = AC.fromjulia("left", Int64[1])
    c2, d2 = AC.fromjulia("right", Int64[1])
    t = AC.UnionType(AC.DenseMode, Int8[3, 7])
    f = AC.Field("union", t; children=[c1, c2])
    pool = AC.ArrayData(
        t,
        2,
        [AC._databuffer(Int8[7, 3]), AC._databuffer(Int32[0, 0])];
        children=[d1, d2],
        nullcount=0,
    )
    core = Arrow.readstream(fixture(f, [(false, pool, [0, 1]), (true, pool, [2, 3])]))
    combined = core.batches[2].columns[1].dictionary
    @test AC.slicebytes(AC.rolebuffer(combined, AC.TYPE_IDS)) == UInt8[7, 3, 7, 3]
end

@testset "Dictionary delta limits and malformed payloads" begin
    vf, pool = AC.fromjulia("pool", Int64[1, 2])
    bytes = fixture(vf, [(false, pool, [0]), (true, pool, [3])])
    @test_throws AC.ValidationError Arrow.Table(
        bytes;
        limits=Arrow.Limits(max_array_length=3),
    )
    dicts = Dict{Int64,AC.ArrayData}(0 => pool)
    validated = AC._ValidatedDictionaries()
    @test_throws Arrow.AllocationLimitError Arrow._updatedictionary!(
        dicts,
        validated,
        Int64(0),
        true,
        vf,
        pool,
        Arrow.Limits(),
        Arrow.AllocationBudget(0),
    )
    @test dicts[0] === pool
    @test_throws AC.ValidationError Arrow._updatedictionary!(
        dicts,
        validated,
        Int64(0),
        true,
        vf,
        pool,
        Arrow.Limits(max_buffer_bytes=24),
        Arrow.AllocationBudget(1_000_000),
    )
    @test dicts[0] === pool
    # Validation must precede reading offsets during concatenation.
    sf, strings = AC.fromjulia("pool", ["ok"])
    malformed = AC.ArrayData(
        sf.type,
        1,
        [AC.BufferSlice(), AC._databuffer(Int32[0, 10]), AC._databuffer(UInt8[0x61])];
        nullcount=0,
    )
    dicts = Dict{Int64,AC.ArrayData}(0 => strings)
    @test_throws AC.ValidationError Arrow._updatedictionary!(
        dicts,
        validated,
        Int64(0),
        true,
        sf,
        malformed,
        Arrow.Limits(),
        Arrow.AllocationBudget(1_000_000),
    )
    @test dicts[0] === strings
end

end
