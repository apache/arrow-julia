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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

module PropertyTests

using Arrow
using Dates
using Random
using Tables
using Test

const SEED = UInt64(0x9f5a_37c2_41de_880b)
const CASES = 16
const TEXT =
    ["", "a", "\0", "alpha", "\u03bb", "\u03b1\u03b2\u2200", "\U0001f9ea", "line\nbreak"]

_maybe(rng, x) = rand(rng, 1:5) == 1 ? missing : x

function _randomfloat(rng)
    specials = (0.0, -0.0, Inf, -Inf, NaN)
    pick = rand(rng, 1:8)
    return pick <= length(specials) ? specials[pick] : randn(rng)
end

function _makepart(rng, n)
    ints = Union{Missing,Int64}[_maybe(rng, rand(rng, Int64)) for _ = 1:n]
    uints = Union{Missing,UInt64}[_maybe(rng, rand(rng, UInt64)) for _ = 1:n]
    floats = Union{Missing,Float64}[_maybe(rng, _randomfloat(rng)) for _ = 1:n]
    bools = Union{Missing,Bool}[_maybe(rng, rand(rng, Bool)) for _ = 1:n]
    strings = Union{Missing,String}[_maybe(rng, rand(rng, TEXT)) for _ = 1:n]
    dates = Union{Missing,Date}[
        _maybe(rng, Date(1970, 1, 1) + Day(rand(rng, -100_000:100_000))) for _ = 1:n
    ]
    lists = Vector{Union{Missing,Vector{Union{Missing,Int32}}}}(undef, n)
    structs =
        Vector{@NamedTuple{x::Union{Missing,Int16},label::Union{Missing,String}}}(undef, n)
    dictionary = Union{Missing,String}[_maybe(rng, rand(rng, TEXT)) for _ = 1:n]
    for i = 1:n
        lists[i] =
            rand(rng, 1:5) == 1 ? missing :
            Union{Missing,Int32}[_maybe(rng, rand(rng, Int32)) for _ = 1:rand(rng, 0:6)]
        structs[i] = (x=_maybe(rng, rand(rng, Int16)), label=_maybe(rng, rand(rng, TEXT)))
    end
    table = (
        ints=ints,
        uints=uints,
        floats=floats,
        bools=bools,
        strings=strings,
        dates=dates,
        lists=lists,
        structs=structs,
        dictionary=Arrow.DictEncode(dictionary),
    )
    return table, merge(table, (dictionary=dictionary,))
end

_expectedstructs(values) = [["x" => x.x, "label" => x.label] for x in values]

function _checktable(actual, expected)
    @test Tables.columnnames(actual) == collect(keys(expected))
    @test isequal(collect(actual.ints), expected.ints)
    @test isequal(collect(actual.uints), expected.uints)
    @test isequal(collect(actual.floats), expected.floats)
    @test isequal(collect(actual.bools), expected.bools)
    @test isequal(collect(actual.strings), expected.strings)
    @test isequal(collect(actual.dates), expected.dates)
    @test isequal(collect(actual.lists), expected.lists)
    @test isequal(collect(actual.structs), _expectedstructs(expected.structs))
    @test isequal(collect(actual.dictionary), expected.dictionary)
end

function _concatparts(parts)
    names = keys(first(parts))
    return NamedTuple{names}(
        map(names) do name
            reduce(vcat, (getproperty(part, name) for part in parts); init=Any[])
        end,
    )
end

@testset "public IPC properties (seed=$(string(SEED; base=16)))" begin
    @testset "declared struct child types survive empty and all-missing values" begin
        Row = @NamedTuple{x::Int16, label::Union{Missing,String}}
        for rows in (Row[], Row[(x=1, label=missing), (x=2, label=missing)])
            io = IOBuffer()
            Arrow.write(io, (structs=rows,); file=false)
            bytes = take!(io)
            stream = Arrow.readstream(bytes)
            children = stream.schema.fields[1].children
            @test children[1].type == Arrow.AC.IntType(16, true)
            @test !children[1].nullable
            @test children[2].type == Arrow.AC.Utf8Type(false)
            @test children[2].nullable
            table = Arrow.Table(bytes)
            @test isequal(
                collect(table.structs),
                [["x" => row.x, "label" => row.label] for row in rows],
            )
        end

        EmptyRow = NamedTuple{(),Tuple{}}
        for rows in (EmptyRow[], EmptyRow[(;), (;)])
            io = IOBuffer()
            Arrow.write(io, (structs=rows,); file=false)
            bytes = take!(io)
            field = Arrow.readstream(bytes).schema.fields[1]
            @test field.type isa Arrow.AC.StructType
            @test isempty(field.children)
            @test Arrow.Table(bytes).structs == [Pair{String,Any}[] for _ in rows]
        end
    end

    rng = Xoshiro(SEED)
    for case = 1:CASES
        @testset "case $case" begin
            nparts = rand(rng, 1:4)
            made = [_makepart(rng, rand(rng, 0:20)) for _ = 1:nparts]
            inputs = first.(made)
            expectedparts = last.(made)
            expected = _concatparts(expectedparts)
            source = Tables.partitioner(inputs)
            for file in (false, true), compress in (:none, :lz4, :zstd)
                @testset "file=$file compress=$compress" begin
                    io = IOBuffer()
                    Arrow.write(io, source; file=file, compress=compress)
                    bytes = take!(io)
                    _checktable(Arrow.Table(bytes), expected)
                    if !file
                        batches = collect(Arrow.Stream(bytes))
                        @test length(batches) == nparts
                        for (batch, part) in zip(batches, expectedparts)
                            _checktable(batch, part)
                        end
                    end
                end
            end
        end
    end
end

end # module PropertyTests
