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

@testset "ArrowTypes" begin

@test ArrowTypes.ArrowKind(MyInt) == ArrowTypes.PrimitiveKind()
@test ArrowTypes.ArrowKind(Person) == ArrowTypes.StructKind()
@test ArrowTypes.ArrowKind(Person(0, "bob")) == ArrowTypes.StructKind()

@test ArrowTypes.ArrowType(Int) == Int
@test ArrowTypes.ArrowType(Union{Int, Missing}) == Union{Int, Missing}
@test ArrowTypes.ArrowType(Missing) == Missing

@test ArrowTypes.toarrow(1) === 1

@test ArrowTypes.arrowname(Int) == Symbol()
@test !ArrowTypes.hasarrowname(Int)

@test ArrowTypes.arrowmetadata(Int) == ""

@test ArrowTypes.JuliaType(1) === nothing
@test ArrowTypes.JuliaType(1, Int) === nothing
@test ArrowTypes.JuliaType(1, Int, nothing) === nothing

@test ArrowTypes.fromarrow(Int, 1) === 1
@test ArrowTypes.fromarrow(Person, 1, "bob") == Person(1, "bob")
@test ArrowTypes.fromarrow(Union{Int, Missing}, missing) === missing
@test ArrowTypes.fromarrow(Union{Int, Missing}, 1) === 1
@test ArrowTypes.fromarrow(Union{Float64, Missing}, 1) === 1.0

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

@test ArrowTypes.ArrowKind(Bool) == ArrowTypes.BoolKind()

@test ArrowTypes.ListKind() == ArrowTypes.ListKind{false}()
@test !ArrowTypes.isstringtype(ArrowTypes.ListKind())
@test !ArrowTypes.isstringtype(typeof(ArrowTypes.ListKind()))
@test ArrowTypes.ArrowKind(String) == ArrowTypes.ListKind{true}()

hey = collect(b"hey")
@test ArrowTypes.fromarrow(String, pointer(hey), 3) == "hey"

@test ArrowTypes.ArrowType(Symbol) == String
@test ArrowTypes.toarrow(:hey) == "hey"
@test ArrowTypes.arrowname(Symbol) == ArrowTypes.SYMBOL
@test ArrowTypes.JuliaType(Val(ArrowTypes.SYMBOL)) == Symbol
@test ArrowTypes.fromarrow(Symbol, pointer(hey), 3) == :hey

@test ArrowTypes.ArrowKind(Vector{Int}) == ArrowTypes.ListKind()
@test ArrowTypes.ArrowKind(Set{Int}) == ArrowTypes.ListKind()
@test ArrowTypes.ArrowType(Set{Int}) == Vector{Int}
@test typeof(ArrowTypes.toarrow(Set([1,2,3]))) <: Vector{Int}
@test ArrowTypes.arrowname(Set{Int}) == ArrowTypes.SET
@test ArrowTypes.JuliaType(Val(ArrowTypes.SET), Vector{Int}) == Set{Int}
@test ArrowTypes.fromarrow(Set{Int}, [1,2,3]) == Set([1,2,3])

K = ArrowTypes.ArrowKind(NTuple{3, UInt8})
@test ArrowTypes.gettype(K) == UInt8
@test ArrowTypes.getsize(K) == 3
@test K == ArrowTypes.FixedSizeListKind{3, UInt8}()

u = UUID(rand(UInt128))
ubytes = ArrowTypes._cast(NTuple{16, UInt8}, u.value)
@test ArrowTypes.ArrowKind(u) == ArrowTypes.FixedSizeListKind{16, UInt8}()
@test ArrowTypes.ArrowType(UUID) == NTuple{16, UInt8}
@test ArrowTypes.toarrow(u) == ubytes
@test ArrowTypes.arrowname(UUID) == ArrowTypes.UUIDSYMBOL
@test ArrowTypes.JuliaType(Val(ArrowTypes.UUIDSYMBOL)) == UUID
@test ArrowTypes.fromarrow(UUID, ubytes) == u

nt = (id=1, name="bob")
@test ArrowTypes.ArrowKind(NamedTuple) == ArrowTypes.StructKind()
@test ArrowTypes.fromarrow(typeof(nt), nt) === nt
@test ArrowTypes.fromarrow(Person, nt) == Person(1, "bob")
@test ArrowTypes.ArrowKind(Tuple) == ArrowTypes.StructKind()
@test ArrowTypes.ArrowKind(Tuple{}) == ArrowTypes.StructKind()
@test ArrowTypes.arrowname(Tuple{Int, String}) == ArrowTypes.TUPLE
@test ArrowTypes.arrowname(Tuple{}) == ArrowTypes.TUPLE
@test ArrowTypes.JuliaType(Val(ArrowTypes.TUPLE), NamedTuple{(Symbol("1"), Symbol("2")), Tuple{Int, String}}) == Tuple{Int, String}
@test ArrowTypes.fromarrow(Tuple{Int, String}, nt) == (1, "bob")
@test ArrowTypes.fromarrow(Union{Missing, typeof(nt)}, nt) == nt

v = v"1"
v_nt = (major=1, minor=0, patch=0, prerelease=(), build=())
@test ArrowTypes.ArrowKind(VersionNumber) == ArrowTypes.StructKind()
@test ArrowTypes.arrowname(VersionNumber) == ArrowTypes.VERSION_NUMBER
@test ArrowTypes.JuliaType(Val(ArrowTypes.VERSION_NUMBER)) == VersionNumber
@test ArrowTypes.fromarrow(typeof(v), v_nt) == v
@test ArrowTypes.default(VersionNumber) == v"0"

@test ArrowTypes.ArrowKind(Dict{String, Int}) == ArrowTypes.MapKind()
@test ArrowTypes.ArrowKind(Union{String, Int}) == ArrowTypes.UnionKind()

@test ArrowTypes.default(Int) == Int(0)
@test ArrowTypes.default(Symbol) == Symbol()
@test ArrowTypes.default(Char) == '\0'
@test ArrowTypes.default(String) == ""
@test ArrowTypes.default(Missing) === missing
@test ArrowTypes.default(Nothing) === nothing
@test ArrowTypes.default(Union{Int, Missing}) == Int(0)
@test ArrowTypes.default(Union{Int, Nothing}) == Int(0)
@test ArrowTypes.default(Union{Int, Missing, Nothing}) == Int(0)

@test ArrowTypes.promoteunion(Int, Float64) == Float64
@test ArrowTypes.promoteunion(Int, String) == Union{Int, String}

@test ArrowTypes.concrete_or_concreteunion(Int)
@test !ArrowTypes.concrete_or_concreteunion(Union{Real, String})
@test !ArrowTypes.concrete_or_concreteunion(Any)

@testset "ToArrow" begin
    x = ArrowTypes.ToArrow([1,2,3])
    @test x isa Vector{Int}
    @test x == [1,2,3]

    x = ArrowTypes.ToArrow([:hey, :ho])
    @test x isa ArrowTypes.ToArrow{String, Vector{Symbol}}
    @test x == ["hey", "ho"]

    x = ArrowTypes.ToArrow(Any[1, 3.14])
    @test x isa ArrowTypes.ToArrow{Float64, Vector{Any}}
    @test x == [1.0, 3.14]

    x = ArrowTypes.ToArrow(Any[1, 3.14, "hey"])
    @test x isa ArrowTypes.ToArrow{Union{Float64, String}, Vector{Any}}
    @test x == [1.0, 3.14, "hey"]

    @testset "respect non-missing type" begin
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
        @test eltype(ArrowTypes.ToArrow(T[missing])) == Union{Timestamp{:UTC}, Missing}

        # Works since `ArrowTypes.default(Any) === nothing` and
        # `ArrowTypes.toarrow(nothing) === missing`. Defining `toarrow(::Nothing) = nothing`
        # would break this test by returning `Union{Nothing,Missing}`.
        @test eltype(ArrowTypes.ToArrow(Any[missing])) == Missing
    end
end

end
