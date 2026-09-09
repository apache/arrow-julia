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

module DetachedValuesTests
using Test, Arrow, Serialization, Tables
const AC = Arrow.ArrowCore

function roundtrip(x)
    io = IOBuffer()
    serialize(io, x)
    seekstart(io)
    return deserialize(io)
end

function detached(d)
    @test d.owner === nothing
    for b in d.buffers
        b.region === nothing && continue
        @test b.region.root isa Vector{UInt8}
        @test b.region.cell.action == C_NULL
        @test b.region.cell.arg === nothing
        @test !b.region.cell.closed
    end
    foreach(detached, d.children)
    d.dictionary === nothing || detached(d.dictionary)
end

@testset "Detached copy and serialization" begin
    pairs = [
        Arrow.fromjulia("x", v) for v in (
            Union{Missing,Int32}[1, missing, 3],
            Union{Missing,Bool}[true, missing, false],
            ["short", "a string longer than twelve bytes", ""],
            [[1, 2], Int[], [3]],
            Int32[],
            [missing, missing],
        )
    ]
    sf1, sd1 = Arrow.fromjulia("a", [1, 2])
    sf2, sd2 = Arrow.fromjulia("b", ["one", "two"])
    st = AC.StructType()
    push!(
        pairs,
        (
            AC.Field("struct", st; children=[sf1, sf2]),
            AC.ArrayData(st, 2, [AC.BufferSlice()]; children=[sd1, sd2], nullcount=0),
        ),
    )
    f, d = Arrow.fromjulia("slice", Int32[10, 20, 30, 40])
    push!(pairs, (f, AC.ArrayData(d.type, 2, d.buffers; offset=1, nullcount=0)))
    pf, pd = Arrow.fromjulia("pool", ["first", "unused", "last"])
    dt = AC.DictionaryType(AC.IntType(8, true), pd.type, true)
    push!(
        pairs,
        (
            AC.Field("dict", dt),
            AC.ArrayData(
                dt,
                3,
                [AC.BufferSlice(), AC._databuffer(Int8[2, 0, 2])];
                dictionary=pd,
                nullcount=0,
            ),
        ),
    )
    for (f, d) in pairs
        sp, ap = Arrow.to_c_data(f, d)
        fi, imported = Arrow.from_c_data(sp, ap)
        expected = Arrow.materialize(fi, imported)
        copied = copy(imported)
        saved = roundtrip((fi, imported, imported))
        @test saved[2] === saved[3]
        Arrow.release!(imported.owner)
        GC.gc()
        for result in (copied, saved[2])
            detached(result)
            @test result.offset == imported.offset
            @test isequal(Arrow.materialize(fi, result), expected)
            @test Arrow.validate_semantic(fi, result) === result
        end
        Arrow.reap!()
    end
    # A fresh process must rebuild pointers from saved bytes, after release.
    mktemp() do path, io
        f, d = Arrow.fromjulia("saved", Int32[4, 5, 6])
        sp, ap = Arrow.to_c_data(f, d)
        fi, imported = Arrow.from_c_data(sp, ap)
        serialize(io, (fi, imported))
        close(io)
        Arrow.release!(imported.owner)
        Arrow.reap!()
        code = "using Arrow, Serialization; f, d = deserialize(ARGS[1]); @assert Arrow.materialize(f, d) == Int32[4,5,6]; @assert d.owner === nothing"
        cmd = `$(Base.julia_cmd()) --startup-file=no --check-bounds=yes --project=$(Base.active_project()) -e $code $path`
        @test success(cmd)
    end
    # Copy a BufferSlice window, not its whole underlying allocation.
    backing = UInt8[0xaa, 1, 2, 3, 0xbb]
    f = AC.Field("window", AC.IntType(8, false))
    d = AC.ArrayData(
        f.type,
        3,
        [AC.BufferSlice(), AC.BufferSlice(AC.heapregion(backing), 1, 3)],
    )
    c, s = copy(d), roundtrip(d)
    backing[2:4] .= 9
    @test Arrow.materialize(f, c) == [1, 2, 3]
    @test Arrow.materialize(f, s) == [1, 2, 3]
    Arrow.release!(d.buffers[2].region)
    @test_throws InvalidStateException copy(d)
    @test_throws InvalidStateException roundtrip(d)

    b = Arrow.batch((x=Int32[1, 2], y=["one", "two"]))
    bc, bs = copy(b), roundtrip(b)
    foreach(
        c ->
            foreach(buf -> buf.region === nothing || Arrow.release!(buf.region), c.buffers),
        b.columns,
    )
    for result in (bc, bs)
        @test result.nrows == 2
        @test Arrow.materialize(result.schema.fields[1], result.columns[1]) == [1, 2]
        foreach(detached, result.columns)
    end
    empty = AC.RecordBatch(AC.Schema(AC.Field[]), AC.ArrayData[], 7)
    @test copy(empty).nrows == roundtrip(empty).nrows == 7

    mktemp() do path, io
        Arrow.write(io, (x=Int32[1, 2], y=["a long materialized string", "two"]))
        close(io)
        t = Arrow.Table(path)
        tc, ts = copy(t), roundtrip(t)
        Arrow.release!(t)
        for result in (tc, ts)
            @test isempty(getfield(result, :regions))
            @test Tables.columntable(result) == Tables.columntable(t)
        end
        tc.x[1] = 99
        @test t.x[1] == ts.x[1] == 1
    end
end

@testset "Opaque binary metadata" begin
    metadata = [
        String(UInt8[0xff, 0x00, 0x80]) => String(UInt8[0xfe, 0x00]),
        "duplicate" => "one",
        "duplicate" => "two",
        "" => "",
    ]
    f, d = Arrow.fromjulia("x", Int32[1, 2])
    f = AC.Field(f.name, f.type; metadata=metadata)
    schema = AC.Schema([f]; metadata=metadata)
    b = AC.RecordBatch(schema, [d])
    # Text required by the C Data spec is still checked.
    badname = AC.Field(String(UInt8[0xff]), f.type)
    @test_throws AC.ValidationError Arrow.to_c_data(badname, d)
    @test Arrow.validate_semantic(f, d) === d
    for writebytes in (Arrow.writestream, Arrow.writefile)
        bytes = writebytes(schema, [b])
        t = Arrow.Table(bytes)
        @test collect(getfield(t, :schema).metadata) == metadata
        @test collect(getfield(t, :schema).fields[1].metadata) == metadata
        io = IOBuffer()
        Arrow.write(io, t)
        reread = Arrow.Table(take!(io))
        @test collect(getfield(reread, :schema).metadata) == metadata
        @test collect(getfield(reread, :schema).fields[1].metadata) == metadata
    end
    sp, ap = Arrow.to_c_data(f, d)
    fi, di = Arrow.from_c_data(sp, ap)
    @test collect(fi.metadata) == metadata
    Arrow.release!(di.owner)
    Arrow.reap!()
    for batches in (AC.RecordBatch[], [b])
        ref = Ref{Arrow.CArrowArrayStream}()
        GC.@preserve ref begin
            p = Base.unsafe_convert(Ptr{Arrow.CArrowArrayStream}, ref)
            Arrow.export_stream!(p, schema, batches)
            source = Arrow.from_c_stream(p)
            @test collect(AC.schema(source).metadata) == metadata
            @test collect(AC.schema(source).fields[1].metadata) == metadata
            rb = Arrow.nextbatch!(source)
            if rb !== nothing
                @test collect(rb.schema.metadata) == metadata
                Arrow.release!(rb.columns[1].owner)
            end
            @test Arrow.nextbatch!(source) === nothing
            Arrow.release!(source)
        end
        Arrow.reap!()
    end
end
end
