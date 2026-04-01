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

primitive type MyInt 32 end

struct Person
    id::Int
    name::String
end

module EnumTestModule
@enum RankingStrategy lexical=1 semantic=2 hybrid=3
end

module WideEnumTestModule
@enum WideRanking::UInt64 small=1 colossal=0xffffffffffffffff
end

const RankingStrategy = EnumTestModule.RankingStrategy
const lexical = EnumTestModule.lexical
const semantic = EnumTestModule.semantic
const hybrid = EnumTestModule.hybrid
const WideRanking = WideEnumTestModule.WideRanking
const small = WideEnumTestModule.small
const colossal = WideEnumTestModule.colossal

@testset "ArrowTypes" begin
    @test ArrowTypes.ArrowKind(MyInt) == ArrowTypes.PrimitiveKind()
    @test ArrowTypes.ArrowKind(Person) == ArrowTypes.StructKind()
    @test ArrowTypes.ArrowKind(Person(0, "bob")) == ArrowTypes.StructKind()

    @test ArrowTypes.ArrowType(Int) == Int
    @test ArrowTypes.ArrowType(Union{Int,Missing}) == Union{Int,Missing}
    @test ArrowTypes.ArrowType(Missing) == Missing

    @test ArrowTypes.toarrow(1) === 1

    @test ArrowTypes.arrowname(Int) == Symbol()
    @test !ArrowTypes.hasarrowname(Int)

    @test ArrowTypes.arrowmetadata(Int) == ""
    @test ArrowTypes.arrowmetadata(Union{Nothing,Int}) == ""
    @test ArrowTypes.arrowmetadata(Union{Missing,Int}) == ""

    @test ArrowTypes.JuliaType(1) === nothing
    @test ArrowTypes.JuliaType(1, Int) === nothing
    @test ArrowTypes.JuliaType(1, Int, nothing) === nothing

    @test ArrowTypes.fromarrow(Int, 1) === 1
    @test ArrowTypes.fromarrow(Person, 1, "bob") == Person(1, "bob")
    @test ArrowTypes.fromarrow(Union{Int,Missing}, missing) === missing
    @test ArrowTypes.fromarrow(Union{Int,Missing}, 1) === 1
    @test ArrowTypes.fromarrow(Union{Float64,Missing}, 1) === 1.0

    @test ArrowTypes.ArrowKind(Missing) == ArrowTypes.NullKind()
    @test ArrowTypes.ArrowKind(Nothing) == ArrowTypes.NullKind()
    @test ArrowTypes.ArrowType(Nothing) == Missing
    @test ArrowTypes.toarrow(nothing) === missing
    @test ArrowTypes.arrowname(Nothing) == ArrowTypes.NOTHING
    @test ArrowTypes.JuliaType(Val(ArrowTypes.NOTHING)) == Nothing
    @test ArrowTypes.fromarrow(Nothing, missing) === nothing

    @test ArrowTypes.ArrowKind(Int) == ArrowTypes.PrimitiveKind()
    @test ArrowTypes.ArrowKind(Float64) == ArrowTypes.PrimitiveKind()

    @test ArrowTypes.ArrowType(Char) == UInt32
    @test ArrowTypes.toarrow('1') == UInt32('1')
    @test ArrowTypes.arrowname(Char) == ArrowTypes.CHAR
    @test ArrowTypes.JuliaType(Val(ArrowTypes.CHAR)) == Char
    @test ArrowTypes.fromarrow(Char, UInt32('1')) == '1'

    enum_metadata = ArrowTypes.arrowmetadata(RankingStrategy)
    @test ArrowTypes.ArrowKind(RankingStrategy) == ArrowTypes.PrimitiveKind()
    @test ArrowTypes.ArrowType(RankingStrategy) == Int32
    @test ArrowTypes.toarrow(hybrid) == Int32(3)
    @test ArrowTypes.arrowname(RankingStrategy) == ArrowTypes.ENUM
    @test occursin("type=Main.EnumTestModule.RankingStrategy", enum_metadata)
    @test occursin("labels=lexical:1,semantic:2,hybrid:3", enum_metadata)
    @test ArrowTypes.JuliaType(Val(ArrowTypes.ENUM), Int32, enum_metadata) ==
          RankingStrategy
    reordered_enum_metadata = "type=Main.EnumTestModule.RankingStrategy;labels=semantic:2,hybrid:3,lexical:1"
    mismatched_enum_metadata = "type=Main.EnumTestModule.RankingStrategy;labels=lexical:1,semantic:2,hybrid:4"
    malformed_enum_metadata = "type=Main.EnumTestModule.RankingStrategy;labels=lexical:1,semantic:nope"
    @test ArrowTypes.JuliaType(Val(ArrowTypes.ENUM), Int32, reordered_enum_metadata) ==
          RankingStrategy
    @test ArrowTypes.JuliaType(Val(ArrowTypes.ENUM), Int32, mismatched_enum_metadata) ===
          nothing
    @test ArrowTypes.JuliaType(Val(ArrowTypes.ENUM), Int32, malformed_enum_metadata) ===
          nothing
    @test ArrowTypes.JuliaType(
        Val(ArrowTypes.ENUM),
        Int32,
        "type=Main.EnumTestModule.RankingStrategy",
    ) === nothing
    @test ArrowTypes.fromarrow(RankingStrategy, Int32(2)) == semantic
    @test ArrowTypes.default(RankingStrategy) == lexical

    wide_enum_metadata = ArrowTypes.arrowmetadata(WideRanking)
    @test ArrowTypes.ArrowKind(WideRanking) == ArrowTypes.PrimitiveKind()
    @test ArrowTypes.ArrowType(WideRanking) == UInt64
    @test ArrowTypes.toarrow(colossal) == typemax(UInt64)
    @test ArrowTypes.arrowname(WideRanking) == ArrowTypes.ENUM
    @test occursin("type=Main.WideEnumTestModule.WideRanking", wide_enum_metadata)
    @test occursin("labels=small:1,colossal:18446744073709551615", wide_enum_metadata)
    @test ArrowTypes.JuliaType(Val(ArrowTypes.ENUM), UInt64, wide_enum_metadata) ==
          WideRanking
    @test ArrowTypes.fromarrow(WideRanking, typemax(UInt64)) == colossal
    @test ArrowTypes.default(WideRanking) == small

    @test ArrowTypes.ArrowKind(Bool) == ArrowTypes.BoolKind()

    @test ArrowTypes.ListKind() == ArrowTypes.ListKind{false}()
    @test !ArrowTypes.isstringtype(ArrowTypes.ListKind())
    @test !ArrowTypes.isstringtype(typeof(ArrowTypes.ListKind()))
    @test ArrowTypes.ArrowKind(String) == ArrowTypes.ListKind{true}()
    @test ArrowTypes.ArrowKind(Base.CodeUnits) == ArrowTypes.ListKind{true}()

    hey = collect(b"hey")
    @test ArrowTypes.fromarrow(String, pointer(hey), 3) == "hey"
    @test ArrowTypes.fromarrow(Base.CodeUnits, pointer(hey), 3) == b"hey"
    @test ArrowTypes.fromarrow(Union{Base.CodeUnits,Missing}, pointer(hey), 3) == b"hey"

    @test ArrowTypes.ArrowType(Symbol) == String
    @test ArrowTypes.toarrow(:hey) == "hey"
    @test ArrowTypes.arrowname(Symbol) == ArrowTypes.SYMBOL
    @test ArrowTypes.JuliaType(Val(ArrowTypes.SYMBOL)) == Symbol
    @test ArrowTypes.fromarrow(Symbol, pointer(hey), 3) == :hey

    @test ArrowTypes.ArrowKind(Vector{Int}) == ArrowTypes.ListKind()
    @test ArrowTypes.ArrowKind(Set{Int}) == ArrowTypes.ListKind()
    @test ArrowTypes.ArrowType(Set{Int}) == Vector{Int}
    @test typeof(ArrowTypes.toarrow(Set([1, 2, 3]))) <: Vector{Int}
    @test ArrowTypes.arrowname(Set{Int}) == ArrowTypes.SET
    @test ArrowTypes.JuliaType(Val(ArrowTypes.SET), Vector{Int}) == Set{Int}
    @test ArrowTypes.fromarrow(Set{Int}, [1, 2, 3]) == Set([1, 2, 3])

    K = ArrowTypes.ArrowKind(NTuple{3,UInt8})
    @test ArrowTypes.gettype(K) == UInt8
    @test ArrowTypes.getsize(K) == 3
    @test K == ArrowTypes.FixedSizeListKind{3,UInt8}()

    u = UUID(rand(UInt128))
    ubytes = ArrowTypes._cast(NTuple{16,UInt8}, u.value)
    @test ArrowTypes.ArrowKind(u) == ArrowTypes.FixedSizeListKind{16,UInt8}()
    @test ArrowTypes.ArrowType(UUID) == NTuple{16,UInt8}
    @test ArrowTypes.toarrow(u) == ubytes
    @test ArrowTypes.arrowname(UUID) == ArrowTypes.UUIDSYMBOL
    @test ArrowTypes.JuliaType(Val(ArrowTypes.UUIDSYMBOL)) == UUID
    @test ArrowTypes.JuliaType(Val(ArrowTypes.LEGACY_UUIDSYMBOL)) == UUID
    @test ArrowTypes.fromarrow(UUID, ubytes) == u

    ip4 = IPv4(rand(UInt32))
    @test ArrowTypes.ArrowKind(ip4) == PrimitiveKind()
    @test ArrowTypes.ArrowType(IPv4) == UInt32
    @test ArrowTypes.toarrow(ip4) == ip4.host
    @test ArrowTypes.arrowname(IPv4) == ArrowTypes.IPV4_SYMBOL
    @test ArrowTypes.JuliaType(Val(ArrowTypes.IPV4_SYMBOL)) == IPv4
    @test ArrowTypes.fromarrow(IPv4, ip4.host) == ip4

    ip6 = IPv6(rand(UInt128))
    ip6_ubytes = ArrowTypes._cast(NTuple{16,UInt8}, ip6.host)
    @test ArrowTypes.ArrowKind(ip6) == ArrowTypes.FixedSizeListKind{16,UInt8}()
    @test ArrowTypes.ArrowType(IPv6) == NTuple{16,UInt8}
    @test ArrowTypes.toarrow(ip6) == ip6_ubytes
    @test ArrowTypes.arrowname(IPv6) == ArrowTypes.IPV6_SYMBOL
    @test ArrowTypes.JuliaType(Val(ArrowTypes.IPV6_SYMBOL)) == IPv6
    @test ArrowTypes.fromarrow(IPv6, ip6_ubytes) == ip6

    nt = (id=1, name="bob")
    @test ArrowTypes.ArrowKind(NamedTuple) == ArrowTypes.StructKind()
    @test ArrowTypes.fromarrow(typeof(nt), nt) === nt
    @test ArrowTypes.fromarrow(Person, nt) == Person(1, "bob")
    @test ArrowTypes.ArrowKind(Tuple) == ArrowTypes.StructKind()
    @test ArrowTypes.ArrowKind(Tuple{}) == ArrowTypes.StructKind()
    @test ArrowTypes.arrowname(Tuple{Int,String}) == ArrowTypes.TUPLE
    @test ArrowTypes.arrowname(Tuple{}) == ArrowTypes.TUPLE
    @test ArrowTypes.JuliaType(
        Val(ArrowTypes.TUPLE),
        NamedTuple{(Symbol("1"), Symbol("2")),Tuple{Int,String}},
    ) == Tuple{Int,String}
    @test ArrowTypes.fromarrow(Tuple{Int,String}, nt) == (1, "bob")
    @test ArrowTypes.fromarrow(Union{Missing,typeof(nt)}, nt) == nt
    # #461
    @test ArrowTypes.default(Tuple{}) == ()
    @test ArrowTypes.default(Tuple{Vararg{Int}}) == ()
    @test ArrowTypes.default(Tuple{String,Vararg{Int}}) == ("",)

    z = 1.0 + 2.0im
    @test ArrowTypes.ArrowKind(typeof(z)) == ArrowTypes.StructKind()
    @test ArrowTypes.arrowname(typeof(z)) == ArrowTypes.COMPLEX
    @test ArrowTypes.arrowname(Union{Missing,typeof(z)}) == ArrowTypes.COMPLEX
    @test ArrowTypes.JuliaType(
        Val(ArrowTypes.COMPLEX),
        NamedTuple{(:re, :im),Tuple{Float64,Float64}},
    ) == ComplexF64
    @test ArrowTypes.fromarrowstruct(ComplexF64, Val((:re, :im)), 1.0, 2.0) == z
    @test ArrowTypes.fromarrowstruct(ComplexF64, Val((:im, :re)), 2.0, 1.0) == z

    v = v"1"
    v_nt = (major=1, minor=0, patch=0, prerelease=(), build=())
    @test ArrowTypes.ArrowKind(VersionNumber) == ArrowTypes.StructKind()
    @test ArrowTypes.arrowname(VersionNumber) == ArrowTypes.VERSION_NUMBER
    @test ArrowTypes.JuliaType(Val(ArrowTypes.VERSION_NUMBER)) == VersionNumber
    @test ArrowTypes.fromarrow(typeof(v), v_nt) == v
    @test ArrowTypes.default(VersionNumber) == v"0"

    @test ArrowTypes.ArrowKind(Dict{String,Int}) == ArrowTypes.MapKind()
    @test ArrowTypes.ArrowKind(Union{String,Int}) == ArrowTypes.UnionKind()

    @test ArrowTypes.default(Int) == Int(0)
    @test ArrowTypes.default(Symbol) == Symbol()
    @test ArrowTypes.default(Char) == '\0'
    @test ArrowTypes.default(String) == ""
    @test ArrowTypes.default(Missing) === missing
    @test ArrowTypes.default(Nothing) === nothing
    @test ArrowTypes.default(Union{Int,Missing}) == Int(0)
    @test ArrowTypes.default(Union{Int,Nothing}) == Int(0)
    @test ArrowTypes.default(Union{Int,Missing,Nothing}) == Int(0)

    @test ArrowTypes.promoteunion(Int, Float64) == Float64
    @test ArrowTypes.promoteunion(Int, String) == Union{Int,String}
    @test ArrowTypes.promoteunion(Int, Int) == Int

    @test ArrowTypes.concrete_or_concreteunion(Int)
    @test !ArrowTypes.concrete_or_concreteunion(Union{Real,String})
    @test !ArrowTypes.concrete_or_concreteunion(Any)

    @testset "ToArrow" begin
        @test !ArrowTypes._hasoffsetaxes([1, 2, 3])
        @test ArrowTypes._offsetshift([1, 2, 3]) == 0

        x = ArrowTypes.ToArrow([1, 2, 3])
        @test x isa Vector{Int}
        @test x == [1, 2, 3]

        baseview = @view [1, 2, 3][1:3]
        x = ArrowTypes.ToArrow(baseview)
        @test x === baseview

        x = ArrowTypes.ToArrow([:hey, :ho])
        @test x isa ArrowTypes.ToArrow{String,Vector{Symbol}}
        @test eltype(x) == String
        @test ArrowTypes._needsconvert(x)
        @test x[1] == "hey"
        @test collect(x) == ["hey", "ho"]
        @test x == ["hey", "ho"]

        x = ArrowTypes.ToArrow(Any[1, 3.14])
        @test x isa ArrowTypes.ToArrow{Float64,Vector{Any}}
        @test eltype(x) == Float64
        @test collect(x) == [1.0, 3.14]
        @test x == [1.0, 3.14]

        x = ArrowTypes.ToArrow(Any[UUID(UInt128(1)), UUID(UInt128(2))])
        @test x isa ArrowTypes.ToArrow{NTuple{16,UInt8},Vector{Any}}
        @test eltype(x) == NTuple{16,UInt8}
        @test collect(x) ==
              [ArrowTypes.toarrow(UUID(UInt128(1))), ArrowTypes.toarrow(UUID(UInt128(2)))]

        x = ArrowTypes.ToArrow(Any[missing, UUID(UInt128(1))])
        @test x isa ArrowTypes.ToArrow{Union{Missing,NTuple{16,UInt8}},Vector{Any}}
        @test eltype(x) == Union{Missing,NTuple{16,UInt8}}
        @test isequal(
            collect(x),
            Union{Missing,NTuple{16,UInt8}}[missing, ArrowTypes.toarrow(UUID(UInt128(1)))],
        )

        x = ArrowTypes.ToArrow(Any[1, 3.14, "hey"])
        @test x isa ArrowTypes.ToArrow{Union{Float64,String},Vector{Any}}
        @test eltype(x) == Union{Float64,String}
        @test collect(x) == Union{Float64,String}[1.0, 3.14, "hey"]
        @test x == [1.0, 3.14, "hey"]

        x = ArrowTypes.ToArrow(Any[UUID(UInt128(1)), "tail"])
        @test x isa ArrowTypes.ToArrow{Union{NTuple{16,UInt8},String},Vector{Any}}
        @test eltype(x) == Union{NTuple{16,UInt8},String}
        @test collect(x) ==
              Union{NTuple{16,UInt8},String}[ArrowTypes.toarrow(UUID(UInt128(1))), "tail"]

        x = ArrowTypes.ToArrow(OffsetArray([1, 2, 3], -3:-1))
        @test x isa ArrowTypes.ToArrow{Int,OffsetVector{Int,Vector{Int}}}
        @test ArrowTypes._hasoffsetaxes(getfield(x, :data))
        @test getfield(x, :offset) == ArrowTypes._offsetshift(getfield(x, :data))
        @test ArrowTypes._sourcedata(x) === getfield(x, :data)
        @test ArrowTypes._sourceoffset(x) == getfield(x, :offset)
        @test !ArrowTypes._needsconvert(x)
        @test ArrowTypes._sourcevalue(x, 1) == 1
        @test eltype(x) == Int
        @test x[1] == 1
        @test x[3] == 3
        @test collect(x) == [1, 2, 3]
        @test x == [1, 2, 3]

        x = ArrowTypes.ToArrow(OffsetArray(Union{Missing,Int}[1, missing], -3:-2))
        @test x isa ArrowTypes.ToArrow{
            Union{Missing,Int},
            OffsetVector{Union{Missing,Int},Vector{Union{Missing,Int}}},
        }
        @test !ArrowTypes._needsconvert(x)
        @test x[1] == 1
        @test x[2] === missing
        @test isequal(collect(x), Union{Missing,Int}[1, missing])

        x = ArrowTypes.ToArrow(OffsetArray(Any[1, 3.14], -3:-2))
        @test x isa ArrowTypes.ToArrow{Float64,OffsetVector{Any,Vector{Any}}}
        @test getfield(x, :offset) == ArrowTypes._offsetshift(getfield(x, :data))
        @test ArrowTypes._sourcevalue(x, 2) == 3.14
        @test eltype(x) == Float64
        @test ArrowTypes._needsconvert(x)
        @test x[1] == 1
        @test x[2] == 3.14
        @test collect(x) == [1.0, 3.14]
        @test x == [1, 3.14]

        @testset "respect non-missing concrete type" begin
            struct DateTimeTZ
                instant::Int64
                tz::String
            end

            struct Timestamp{TZ}
                x::Int64
            end

            ArrowTypes.ArrowType(::Type{DateTimeTZ}) = Timestamp
            ArrowTypes.toarrow(x::DateTimeTZ) = Timestamp{Symbol(x.tz)}(x.instant)
            ArrowTypes.default(::Type{DateTimeTZ}) = DateTimeTZ(0, "UTC")

            T = Union{DateTimeTZ,Missing}
            @test !ArrowTypes.concrete_or_concreteunion(ArrowTypes.ArrowType(T))
            @test eltype(ArrowTypes.ToArrow(T[missing])) == Union{Timestamp{:UTC},Missing}
            @test eltype(
                ArrowTypes.ToArrow(DateTimeTZ[DateTimeTZ(1, "UTC"), DateTimeTZ(2, "UTC")]),
            ) == Timestamp{:UTC}
            @test eltype(
                ArrowTypes.ToArrow(DateTimeTZ[DateTimeTZ(1, "UTC"), DateTimeTZ(2, "PST")]),
            ) == Timestamp
            @test eltype(
                ArrowTypes.ToArrow(Any[DateTimeTZ(1, "UTC"), DateTimeTZ(2, "UTC")]),
            ) == Timestamp{:UTC}

            # Works since `ArrowTypes.default(Any) === nothing` and
            # `ArrowTypes.toarrow(nothing) === missing`. Defining `toarrow(::Nothing) = nothing`
            # would break this test by returning `Union{Nothing,Missing}`.
            @test eltype(ArrowTypes.ToArrow(Any[missing])) == Missing
        end

        @testset "ignore non-missing abstract type" begin
            x = ArrowTypes.ToArrow(Union{Missing,Array{Int}}[missing])
            @test x isa ArrowTypes.ToArrow{Missing,Vector{Union{Missing,Array{Int64}}}}
            @test eltype(x) == Missing
            @test isequal(x, [missing])
        end
    end
end
