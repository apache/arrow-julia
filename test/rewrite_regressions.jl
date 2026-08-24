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

module RewriteRegressions

using Test
using Dates
using Tables
import DataAPI
using Arrow
using ArrowStrings

const AC = Arrow.ArrowCore

struct _BytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
end
Arrow.sourcelength(s::_BytesSource) = length(s.data)
Arrow.readrange(s::_BytesSource, off, len) = s.data[(off + 1):(off + len)]

function _rewrite(bytes; file=false)
    io = IOBuffer()
    Arrow.write(io, Arrow.Table(bytes); file=file)
    return take!(io)
end

function _dictbytes(poolfield, pooldata, indices; ordered=false, name="d")
    indextype = AC.IntType(8, true)
    t = AC.DictionaryType(indextype, poolfield.type, ordered)
    data = AC.ArrayData(
        t,
        length(indices),
        [AC.BufferSlice(), AC._databuffer(collect(Int8, indices))];
        dictionary=pooldata,
        nullcount=0,
    )
    field =
        AC.Field(name, t; nullable=false, children=collect(AC.Field, poolfield.children))
    schema = AC.Schema([field])
    return Arrow.writestream(schema, [AC.RecordBatch(schema, [data], length(indices))])
end

function _fixedlistnullbytes(listsize::Int; file::Bool)
    childtype = AC.NullType()
    childfield = AC.Field("item", childtype; nullable=true)
    childdata = AC.ArrayData(childtype, listsize, AC.BufferSlice[]; nullcount=listsize)
    listtype = AC.FixedSizeListType(listsize)
    field = AC.Field("x", listtype; nullable=false, children=[childfield])
    data = AC.ArrayData(listtype, 1, [AC.BufferSlice()]; children=[childdata], nullcount=0)
    schema = AC.Schema([field])
    batch = AC.RecordBatch(schema, [data], 1)
    return file ? Arrow.writefile(schema, [batch]) : Arrow.writestream(schema, [batch])
end

@testset "facade rewrite correctness regressions" begin
    @testset "duplicate names stay positional" begin
        f1, d1 = AC.fromjulia("x", Int64[1, 2])
        f2, d2 = AC.fromjulia("x", Int64[10, 20])
        f1 = AC.Field("x", f1.type; nullable=false, metadata=["slot" => "first"])
        f2 = AC.Field("x", f2.type; nullable=false, metadata=["slot" => "second"])
        schema = AC.Schema([f1, f2])
        bytes = Arrow.writestream(schema, [AC.RecordBatch(schema, [d1, d2], 2)])
        table = Arrow.Table(bytes)
        @test Tables.columnnames(table) == [:x, :x]
        @test Tables.getcolumn(table, 1) == [1, 2]
        @test Tables.getcolumn(table, 2) == [10, 20]
        @test DataAPI.colmetadata(table, 1, "slot") == "first"
        @test DataAPI.colmetadata(table, 2, "slot") == "second"
        @test_throws ArgumentError table.x
        @test_throws ArgumentError Tables.getcolumn(table, :x)
        @test_throws ArgumentError DataAPI.colmetadatakeys(table, :x)
        @test_throws ArgumentError DataAPI.colmetadata(table, :x, "slot")
        allkeys = collect(DataAPI.colmetadatakeys(table))
        @test first.(allkeys) == [:x, :x]
        @test [collect(last(kv)) for kv in allkeys] == [["slot"], ["slot"]]

        rewritten = Arrow.readstream(_rewrite(bytes))
        batch = rewritten.batches[1]
        @test AC.materialize(rewritten.schema.fields[1], batch.columns[1]) == [1, 2]
        @test AC.materialize(rewritten.schema.fields[2], batch.columns[2]) == [10, 20]
        @test collect(rewritten.schema.fields[1].metadata) == ["slot" => "first"]
        @test collect(rewritten.schema.fields[2].metadata) == ["slot" => "second"]
    end

    @testset "NullType is inferred and retained" begin
        io = IOBuffer()
        Arrow.write(io, (n=Missing[missing, missing],); file=false)
        fresh = take!(io)
        @test Arrow.readstream(fresh).schema.fields[1].type isa AC.NullType
        @test isequal(Arrow.Table(fresh).n, Missing[missing, missing])
        retained = _rewrite(fresh)
        @test Arrow.readstream(retained).schema.fields[1].type isa AC.NullType
        @test_throws ArgumentError Arrow.write(IOBuffer(), (n=Union{}[],); file=false)
    end

    @testset "retained dictionaries keep value descriptors and order" begin
        datefield, dateparts = Arrow._constructcolumn(
            Symbol("date-values"),
            AbstractVector[Date[Date(2024, 1, 1), Date(2024, 1, 2)]],
        )
        datedata = only(dateparts)
        datebytes = _dictbytes(datefield, datedata, Int8[1, 0])
        dateback = Arrow.readstream(_rewrite(datebytes))
        datetype = dateback.schema.fields[1].type::AC.DictionaryType
        @test datetype.valuetype isa AC.DateType
        @test Arrow.Table(_rewrite(datebytes)).d == [Date(2024, 1, 2), Date(2024, 1, 1)]

        listfield, listdata = AC.fromjulia("list-values", [Int64[1], Int64[2, 3]])
        listbytes = _dictbytes(listfield, listdata, Int8[1, 0])
        listback = Arrow.readstream(_rewrite(listbytes))
        listtype = listback.schema.fields[1].type::AC.DictionaryType
        @test listtype.valuetype isa AC.ListType
        @test isequal(Arrow.Table(_rewrite(listbytes)).d, [[2, 3], [1]])

        poolfield, pooldata = AC.fromjulia("ordered-values", ["a", "b", "unused"])
        orderedbytes = _dictbytes(poolfield, pooldata, Int8[1, 0]; ordered=true)
        orderedback = Arrow.readstream(_rewrite(orderedbytes))
        field = orderedback.schema.fields[1]
        batch = orderedback.batches[1]
        @test (field.type::AC.DictionaryType).ordered
        valuefield = AC.dictvaluefield(field, field.type)
        @test AC.materialize(valuefield, batch.columns[1].dictionary) ==
              ["a", "b", "unused"]
        @test AC.materialize(field, batch.columns[1]) == ["b", "a"]

        nullfield, nulldata =
            AC.fromjulia("nullable-values", Union{Missing,String}["a", missing])
        nullbytes = _dictbytes(nullfield, nulldata, Int8[1, 0])
        nullback = Arrow.readstream(_rewrite(nullbytes))
        @test !nullback.schema.fields[1].nullable
        @test isequal(
            AC.materialize(nullback.schema.fields[1], nullback.batches[1].columns[1]),
            [missing, "a"],
        )
    end

    @testset "retained byte and scalar layouts rebuild exactly" begin
        raw = Vector{UInt8}(codeunits("short-thirteen-byte"))
        payloads = ArrowStringPayload[
            ArrowStrings.inline_payload(raw, 1, 5),
            ArrowStrings.view_payload(raw, 7, 13, 0, 6),
        ]
        strings = StringVector{ArrowString}(payloads, [raw])
        io = IOBuffer()
        Arrow.write(io, (s=strings,); file=false)
        viewbytes = take!(io)
        viewback = Arrow.readstream(_rewrite(viewbytes))
        @test viewback.schema.fields[1].type == AC.ViewType(true)
        @test Arrow.Table(_rewrite(viewbytes)).s == ["short", "thirteen-byte"]

        binarytype = AC.BinaryType(true)
        binarydata = AC.ArrayData(
            binarytype,
            2,
            [
                AC.BufferSlice(),
                AC._databuffer(Int64[0, 2, 5]),
                AC._databuffer(UInt8[1, 2, 3, 4, 5]),
            ],
            nullcount=0,
        )
        binaryfield = AC.Field("b", binarytype; nullable=false)
        binaryschema = AC.Schema([binaryfield])
        binarybytes =
            Arrow.writestream(binaryschema, [AC.RecordBatch(binaryschema, [binarydata], 2)])
        binaryback = Arrow.readstream(_rewrite(binarybytes))
        @test binaryback.schema.fields[1].type == binarytype
        @test isequal(Arrow.Table(_rewrite(binarybytes)).b, [UInt8[1, 2], UInt8[3, 4, 5]])

        decimaltype = AC.DecimalType(10, 2, 64)
        decimaldata = AC.ArrayData(
            decimaltype,
            2,
            [AC.BufferSlice(), AC._databuffer(Int64[123, 456])],
            nullcount=0,
        )
        decimalfield = AC.Field("dec", decimaltype; nullable=false)
        decimalschema = AC.Schema([decimalfield])
        decimalbytes = Arrow.writestream(
            decimalschema,
            [AC.RecordBatch(decimalschema, [decimaldata], 2)],
        )
        decimalback = Arrow.readstream(_rewrite(decimalbytes))
        @test decimalback.schema.fields[1].type == decimaltype
        @test Arrow.Table(_rewrite(decimalbytes)).dec == Int64[123, 456]
    end

    @testset "retained composites rebuild recursively" begin
        Row = NamedTuple{(:x, :y),Tuple{Int64,Union{Missing,String}}}
        pool = Row[(x=1, y=missing), (x=2, y=missing)]
        structfield, structparts = Arrow._constructcolumn(:pool, AbstractVector[pool])
        structdata = only(structparts)
        structbytes = _dictbytes(structfield, structdata, Int8[1, 0])
        structback = Arrow.readstream(_rewrite(structbytes))
        dictionaryfield = structback.schema.fields[1]
        @test (dictionaryfield.type::AC.DictionaryType).valuetype isa AC.StructType
        @test dictionaryfield.children[2].nullable
        @test isequal(
            AC.materialize(dictionaryfield, structback.batches[1].columns[1]),
            Any[
                Pair{String,Any}["x" => 2, "y" => missing],
                Pair{String,Any}["x" => 1, "y" => missing],
            ],
        )

        xfield, xdata = AC.fromjulia("x", Int64[0, 0])
        yfield, ydata = AC.fromjulia("y", Union{Missing,String}[missing, missing])
        allnullfield =
            AC.Field("s", AC.StructType(); nullable=true, children=[xfield, yfield])
        allnulldata = AC.ArrayData(
            AC.StructType(),
            2,
            [AC._bitmapbuffer(Bool[false, false])];
            children=[xdata, ydata],
            nullcount=2,
        )
        allnullschema = AC.Schema([allnullfield])
        allnullbytes = Arrow.writestream(
            allnullschema,
            [AC.RecordBatch(allnullschema, [allnulldata], 2)],
        )
        allnullback = Arrow.readstream(_rewrite(allnullbytes))
        @test allnullback.schema.fields[1].type isa AC.StructType
        @test isequal(
            AC.materialize(allnullback.schema.fields[1], allnullback.batches[1].columns[1]),
            Missing[missing, missing],
        )

        keyfield, keydata = AC.fromjulia("key", ["a", "b", "c"])
        valuefield, valuedata = AC.fromjulia("value", Union{Missing,Int64}[1, missing, 3])
        entriesfield = AC.Field(
            "entries",
            AC.StructType();
            nullable=false,
            children=[keyfield, valuefield],
        )
        entriesdata = AC.ArrayData(
            AC.StructType(),
            3,
            [AC.BufferSlice()];
            children=[keydata, valuedata],
            nullcount=0,
        )
        maptype = AC.MapType(true)
        mapfield = AC.Field("m", maptype; nullable=false, children=[entriesfield])
        mapdata = AC.ArrayData(
            maptype,
            2,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 2, 3])];
            children=[entriesdata],
            nullcount=0,
        )
        mapschema = AC.Schema([mapfield])
        mapbytes = Arrow.writestream(mapschema, [AC.RecordBatch(mapschema, [mapdata], 2)])
        mapback = Arrow.readstream(_rewrite(mapbytes))
        @test mapback.schema.fields[1].type == maptype
        @test isequal(
            AC.materialize(mapback.schema.fields[1], mapback.batches[1].columns[1]),
            Any[Any["a" => 1, "b" => missing], Any["c" => 3]],
        )

        runfield, rundata = AC.fromjulia("run_ends", Int16[2, 4])
        reevaluefield, reevaluedata =
            AC.fromjulia("values", Union{Missing,String}["x", missing])
        reetype = AC.RunEndEncodedType()
        reefield = AC.Field("r", reetype; nullable=true, children=[runfield, reevaluefield])
        reedata = AC.ArrayData(
            reetype,
            4,
            AC.BufferSlice[];
            children=[rundata, reevaluedata],
            nullcount=0,
        )
        reeschema = AC.Schema([reefield])
        reebytes = Arrow.writestream(reeschema, [AC.RecordBatch(reeschema, [reedata], 4)])
        reeback = Arrow.readstream(_rewrite(reebytes))
        @test reeback.schema.fields[1].type isa AC.RunEndEncodedType
        @test reeback.schema.fields[1].children[1].type == AC.IntType(16, true)
        @test isequal(
            AC.materialize(reeback.schema.fields[1], reeback.batches[1].columns[1]),
            Union{Missing,String}["x", "x", missing, missing],
        )

        datefield = AC.Field("item", AC.DateType(AC.DAY); nullable=false)
        datedata = AC.ArrayData(
            datefield.type,
            3,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 1, 2])],
            nullcount=0,
        )
        listviewtype = AC.ListViewType(false)
        listviewfield =
            AC.Field("dates", listviewtype; nullable=false, children=[datefield])
        listviewdata = AC.ArrayData(
            listviewtype,
            2,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 2]), AC._databuffer(Int32[2, 1])];
            children=[datedata],
            nullcount=0,
        )
        listviewschema = AC.Schema([listviewfield])
        listviewbytes = Arrow.writestream(
            listviewschema,
            [AC.RecordBatch(listviewschema, [listviewdata], 2)],
        )
        listviewback = Arrow.readstream(_rewrite(listviewbytes))
        @test listviewback.schema.fields[1].type == listviewtype
        @test isequal(
            AC.materialize(
                listviewback.schema.fields[1],
                listviewback.batches[1].columns[1],
            ),
            Any[Any[Int32(0), Int32(1)], Any[Int32(2)]],
        )

        liststructtype = AC.ListType(true)
        liststructfield =
            AC.Field("rows", liststructtype; nullable=false, children=[structfield])
        liststructdata = AC.ArrayData(
            liststructtype,
            1,
            [AC.BufferSlice(), AC._databuffer(Int64[0, 2])];
            children=[structdata],
            nullcount=0,
        )
        liststructschema = AC.Schema([liststructfield])
        liststructbytes = Arrow.writestream(
            liststructschema,
            [AC.RecordBatch(liststructschema, [liststructdata], 1)],
        )
        liststructback = Arrow.readstream(_rewrite(liststructbytes))
        @test liststructback.schema.fields[1].type == liststructtype
        @test liststructback.schema.fields[1].children[1].type isa AC.StructType

        uniontype = AC.UnionType(AC.DenseMode, Int8[0, 1])
        unionintfield, unionintdata = AC.fromjulia("i", Int64[1])
        unionstrfield, unionstrdata = AC.fromjulia("s", ["x"])
        unionfield = AC.Field(
            "u",
            uniontype;
            nullable=false,
            children=[unionintfield, unionstrfield],
        )
        uniondata = AC.ArrayData(
            uniontype,
            2,
            [AC._databuffer(Int8[0, 1]), AC._databuffer(Int32[0, 0])];
            children=[unionintdata, unionstrdata],
            nullcount=0,
        )
        unionschema = AC.Schema([unionfield])
        unionbytes =
            Arrow.writestream(unionschema, [AC.RecordBatch(unionschema, [uniondata], 2)])
        err = try
            Arrow.write(IOBuffer(), Arrow.Table(unionbytes); file=false)
            nothing
        catch e
            e
        end
        @test err isa ArgumentError
        @test occursin("cannot be recovered", sprint(showerror, err))
    end

    @testset "metadata duplicates and order survive" begin
        field0, data = AC.fromjulia("x", Int64[1])
        field = AC.Field(
            "x",
            field0.type;
            nullable=false,
            metadata=["z" => "one", "a" => "middle", "z" => "two"],
        )
        metadata = ["z" => "one", "a" => "middle", "z" => "two"]
        schema = AC.Schema([field]; metadata=metadata)
        bytes = Arrow.writestream(schema, [AC.RecordBatch(schema, [data], 1)])
        decoded = Arrow.readstream(bytes)
        @test collect(decoded.schema.metadata) == metadata
        @test collect(decoded.schema.fields[1].metadata) == metadata
        rewritten = Arrow.readstream(_rewrite(bytes))
        @test collect(rewritten.schema.metadata) == metadata
        @test collect(rewritten.schema.fields[1].metadata) == metadata
        statsschema = Arrow.withstatistics(schema, [AC.RecordBatch(schema, [data], 1)])
        @test [kv for kv in statsschema.metadata if first(kv) != Arrow.STATS_KEY] == metadata
        @test count(kv -> first(kv) == Arrow.STATS_KEY, statsschema.metadata) == 1
    end

    @testset "NUL field names stay Core-only and fail cleanly at the facade" begin
        field, data = AC.fromjulia("a\0b", Int64[1])
        schema = AC.Schema([field])
        streambytes = Arrow.writestream(schema, [AC.RecordBatch(schema, [data], 1)])
        filebytes = Arrow.writefile(schema, [AC.RecordBatch(schema, [data], 1)])
        @test Arrow.readstream(streambytes).schema.fields[1].name == "a\0b"
        @test Arrow.readfile(filebytes).schema.fields[1].name == "a\0b"
        @test_throws AC.ValidationError Arrow.Table(streambytes)
        @test_throws AC.ValidationError Arrow.Table(filebytes; scan=Tables.Scan())
        @test_throws AC.ValidationError first(Arrow.Stream(streambytes))
        @test_throws AC.ValidationError Arrow.Table(
            _BytesSource(filebytes);
            scan=Tables.Scan(),
        )
    end

    @testset "reader budget includes public materialization" begin
        limit = Int64(1_000_000)
        limits() = Arrow.Limits(max_total_allocated_bytes=limit)
        listsize = 200_000
        streambytes = _fixedlistnullbytes(listsize; file=false)
        filebytes = _fixedlistnullbytes(listsize; file=true)
        @test length(streambytes) < 1_000
        @test length(filebytes) < 1_000

        openstream() = Arrow.readstream(copy(streambytes); limits=limits())
        openfile() = Arrow.readfile(copy(filebytes); limits=limits())

        @test_throws Arrow.AllocationLimitError Arrow.Table(openstream())
        @test_throws Arrow.AllocationLimitError Arrow.Table(
            openstream();
            scan=Tables.Scan(select=(:x,)),
        )
        @test_throws Arrow.AllocationLimitError Arrow.Table(openfile())
        @test_throws Arrow.AllocationLimitError Arrow.Table(
            openfile();
            scan=Tables.Scan(select=(:x,)),
        )
        @test_throws Arrow.AllocationLimitError Tables.scan(
            openfile(),
            Tables.Scan(select=(:x,)),
        )

        @test_throws Arrow.AllocationLimitError first(Arrow.Stream(openstream()))
        @test_throws Arrow.AllocationLimitError first(Arrow.Stream(openfile()))

        ranged = Arrow.SourceFile(
            _BytesSource(copy(filebytes));
            limits=limits(),
            tailbytes=32,
            coalesce_gap=0,
        )
        @test_throws Arrow.AllocationLimitError Arrow.Table(
            ranged;
            scan=Tables.Scan(select=(:x,)),
        )

        largeio = IOBuffer()
        Arrow.write(largeio, (x=zeros(Int64, 200_000),); file=true)
        largebytes = take!(largeio)
        @test length(largebytes) > limit
        wholesource = Arrow.SourceFile(
            _BytesSource(largebytes);
            limits=limits(),
            tailbytes=32,
            coalesce_gap=0,
        )
        @test_throws Arrow.AllocationLimitError Arrow.Table(wholesource)
    end
end

end # module RewriteRegressions
