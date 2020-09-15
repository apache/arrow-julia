using Test, Arrow, Tables, Dates, PooledArrays

struct MapTable
    x
end
Tables.columnnames(x::MapTable) = propertynames(x.x)
Tables.getcolumn(x::MapTable, i::Int) = getfield(x.x, i)
Tables.getcolumn(x::MapTable, nm::Symbol) = getproperty(x.x, nm)
Tables.schema(x::MapTable) = Tables.Schema(propertynames(x.x), eltype.(getproperty(x.x, nm) for nm in propertynames(x.x)))
Tables.columns(x::MapTable) = x

@testset "Arrow" begin

# basic
t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test tt.col1 == t.col1
@test eltype(tt.col1) === Int64
col1 = copy(tt.col1)
@test typeof(col1) == Vector{Int64}

# missing values
t = (col1=Union{Int64, Missing}[1,2,3,4,5,6,7,8,9,missing],)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test isequal(tt.col1, t.col1)
@test eltype(tt.col1) === Union{Int64, Missing}

# primitive types
t = (
    col1=[missing, missing, missing, missing],
    col2=Union{UInt8, Missing}[0, 1, 2, missing],
    col3=Union{UInt16, Missing}[0, 1, 2, missing],
    col4=Union{UInt32, Missing}[0, 1, 2, missing],
    col5=Union{UInt64, Missing}[0, 1, 2, missing],
    col6=Union{Int8, Missing}[0, 1, 2, missing],
    col7=Union{Int16, Missing}[0, 1, 2, missing],
    col8=Union{Int32, Missing}[0, 1, 2, missing],
    col9=Union{Int64, Missing}[0, 1, 2, missing],
    col10=Union{Float16, Missing}[0, 1, 2, missing],
    col11=Union{Float32, Missing}[0, 1, 2, missing],
    col12=Union{Float64, Missing}[0, 1, 2, missing],
    col13=[true, false, true, missing],
)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

t = (
    col14=[zero(Arrow.Decimal{Int32(2), Int32(2)}), zero(Arrow.Decimal{Int32(2), Int32(2)}), zero(Arrow.Decimal{Int32(2), Int32(2)}), missing],
    col15=[zero(Arrow.Date{Arrow.Meta.DateUnit.DAY, Int32}), zero(Arrow.Date{Arrow.Meta.DateUnit.DAY, Int32}), zero(Arrow.Date{Arrow.Meta.DateUnit.DAY, Int32}), missing],
    col16=[zero(Arrow.Time{Arrow.Meta.TimeUnit.SECOND, Int32}), zero(Arrow.Time{Arrow.Meta.TimeUnit.SECOND, Int32}), zero(Arrow.Time{Arrow.Meta.TimeUnit.SECOND, Int32}), missing],
    col17=[zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.SECOND, nothing}), zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.SECOND, nothing}), zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.SECOND, nothing}), missing],
    col18=[zero(Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH, Int32}), zero(Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH, Int32}), zero(Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH, Int32}), missing],
    col19=[zero(Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}), zero(Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}), zero(Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}), missing],
)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io; convert=false)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# list types
t = (
    col1=Union{String, Missing}["hey", "there", "sailor", missing],
    col2=Union{Vector{UInt8}, Missing}[b"hey", b"there", b"sailor", missing],
    col3=Union{Vector{Int64}, Missing}[Int64[1], Int64[2], Int64[3], missing],
    col4=Union{NTuple{2, Vector{Int64}},Missing}[(Int64[1], Int64[2]), missing, missing, (Int64[3], Int64[4])],
    col5=Union{NTuple{2, UInt8}, Missing}[(0x01, 0x02), (0x03, 0x04), missing, (0x05, 0x06)],
    col6=NamedTuple{(:a, :b), Tuple{Int64, String}}[(a=Int64(1), b="hey"), (a=Int64(2), b="there"), (a=Int64(3), b="sailor"), (a=Int64(4), b="jo-bob")],
)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# unions
t = (
    col1=Arrow.DenseUnionVector([1, 2.0, 3, 4.0, missing]),
    col2=Arrow.SparseUnionVector([1, 2.0, 3, 4.0, missing]),
)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# dict encodings
t = (
    col1=Arrow.DictEncode(Int64[4, 5, 6]),
)
io = IOBuffer()
Arrow.write(io, t; debug=true)
seekstart(io)
tt = Arrow.Table(io; debug=true)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))
col1 = copy(tt.col1)
@test typeof(col1) == PooledVector{Int64, Int64, Vector{Int64}}

t = (
    col1=Arrow.DictEncode(NamedTuple{(:a, :b), Tuple{Int64, Union{String, Missing}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b="sailor"), (a=Int64(4), b="jo-bob")]),
)
io = IOBuffer()
Arrow.write(io, t; debug=true)
seekstart(io)
tt = Arrow.Table(io; debug=true)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# PooledArrays
t = (
    col1=PooledArray([4,5,6,6]),
)
io = IOBuffer()
Arrow.write(io, t; debug=true)
seekstart(io)
tt = Arrow.Table(io; debug=true)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# multiple record batches
if isdefined(Tables, :partitioner)
    xx = Tables.partitioner
else
    xx = Tuple
end
t = xx(((col1=Union{Int64, Missing}[1,2,3,4,5,6,7,8,9,missing],), (col1=Union{Int64, Missing}[1,2,3,4,5,6,7,8,9,missing],)))
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == 1
@test isequal(tt.col1, vcat([1,2,3,4,5,6,7,8,9,missing], [1,2,3,4,5,6,7,8,9,missing]))
@test eltype(tt.col1) === Union{Int64, Missing}

# auto-converting types
t = (
    col1=[Date(2001, 1, 2), Date(2010, 10, 10), Date(2020, 12, 1)],
    col2=[Time(1, 1, 2), Time(13, 10, 10), Time(22, 12, 1)],
    col3=[DateTime(2001, 1, 2), DateTime(2010, 10, 10), DateTime(2020, 12, 1)]
)
io = IOBuffer()
Arrow.write(io, t; debug=true)
seekstart(io)
tt = Arrow.Table(io; debug=true)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# Map
t = MapTable((
    col1=Dict(Int32(1) => Float32(3.14)),
))
io = IOBuffer()
Arrow.write(io, t; debug=true)
seekstart(io)
tt = Arrow.Table(io; debug=true)
for (k, v) in tt.col1
    @test isequal(t.x.col1[k], v)
end

# file format
t = (
    col1=[missing, missing, missing, missing],
    col2=Union{UInt8, Missing}[0, 1, 2, missing],
    col3=Union{UInt16, Missing}[0, 1, 2, missing],
    col4=Union{UInt32, Missing}[0, 1, 2, missing],
    col5=Union{UInt64, Missing}[0, 1, 2, missing],
    col6=Union{Int8, Missing}[0, 1, 2, missing],
    col7=Union{Int16, Missing}[0, 1, 2, missing],
    col8=Union{Int32, Missing}[0, 1, 2, missing],
    col9=Union{Int64, Missing}[0, 1, 2, missing],
    col10=Union{Float16, Missing}[0, 1, 2, missing],
    col11=Union{Float32, Missing}[0, 1, 2, missing],
    col12=Union{Float64, Missing}[0, 1, 2, missing],
    col13=[true, false, true, missing],
)
io = IOBuffer()
Arrow.write(io, t; file=true)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# non-standard types
t = (
    col1=[:hey, :there, :sailor],
    col2=['a', 'b', 'c'],
)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
meta = Dict("key1" => "value1", "key2" => "value2")
Arrow.setmetadata!(t, meta)
meta2 = Dict("colkey1" => "colvalue1", "colkey2" => "colvalue2")
Arrow.setmetadata!(t.col1, meta2)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test tt.col1 == t.col1
@test eltype(tt.col1) === Int64
@test Arrow.getmetadata(tt) == meta
@test Arrow.getmetadata(tt.col1) == meta2

end
