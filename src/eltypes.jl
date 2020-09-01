"""
Given a flatbuffers metadata type definition (a Field instance from Schema.fbs),
translate to the appropriate Julia storage eltype
"""
function juliaeltype end

"""
Given a FlatBuffers.Builder and a Julia column eltype,
Write the field.type flatbuffer definition
"""
function arrowtype end

"""
There are a couple places when writing arrow buffers where
we need to write a "dummy" value; it doesn't really matter
what we write, but we need to write something of a specific
type. So each supported writing type needs to define `default`.
"""
function default end

default(T) = zero(T)

finaljuliatype(T) = T
finaljuliatype(::Type{Missing}) = Missing
finaljuliatype(::Type{Union{T, Missing}}) where {T} = Union{Missing, finaljuliatype(T)}

function juliaeltype(f::Meta.Field)
    T = juliaeltype(f, f.type)
    if f.custom_metadata !== nothing
        fm = Dict(kv.key => kv.value for kv in f.custom_metadata)
    else
        fm = nothing
    end
    return (f.nullable ? Union{T, Missing} : T), fm
end

juliaeltype(f::Meta.Field, ::Meta.Null) = Missing

function arrowtype(b, ::Type{Missing})
    Meta.nullStart(b)
    return Meta.Null, Meta.nullEnd(b), nothing
end

function juliaeltype(f::Meta.Field, int::Meta.Int)
    if int.is_signed
        if int.bitWidth == 8
            Int8
        elseif int.bitWidth == 16
            Int16
        elseif int.bitWidth == 32
            Int32
        elseif int.bitWidth == 64
            Int64
        elseif int.bitWidth == 128
            Int128
        else
            throw(InvalidMetadataError("$int is not valid arrow type metadata"))
        end
    else
        if int.bitWidth == 8
            UInt8
        elseif int.bitWidth == 16
            UInt16
        elseif int.bitWidth == 32
            UInt32
        elseif int.bitWidth == 64
            UInt64
        elseif int.bitWidth == 128
            UInt128
        else
            throw(InvalidMetadataError("$int is not valid arrow type metadata"))
        end
    end
end

function arrowtype(b, ::Type{T}) where {T <: Integer}
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(8 * sizeof(T)))
    Meta.intAddIsSigned(b, T <: Signed)
    return Meta.Int, Meta.intEnd(b), nothing
end

# primitive types
function juliaeltype(f::Meta.Field, fp::Meta.FloatingPoint)
    if fp.precision == Meta.Precision.HALF
        Float16
    elseif fp.precision == Meta.Precision.SINGLE
        Float32
    elseif fp.precision == Meta.Precision.DOUBLE
        Float64
    end
end

function arrowtype(b, ::Type{T}) where {T <: AbstractFloat}
    Meta.floatingPointStart(b)
    Meta.floatingPointAddPrecision(b, T === Float16 ? Meta.Precision.HALF : T === Float32 ? Meta.Precision.SINGLE : Meta.Precision.DOUBLE)
    return Meta.FloatingPoint, Meta.floatingPointEnd(b), nothing
end

juliaeltype(f::Meta.Field, b::Union{Meta.Utf8, Meta.LargeUtf8}) = String

function arrowtype(b, ::Type{String})
    # To support LargeUtf8, we'd need a way to flag/pass a max length from user/actual data
    Meta.utf8Start(b)
    return Meta.Utf8, Meta.utf8End(b), nothing
end

default(::Type{String}) = ""

datasizeof(x) = sizeof(x)
datasizeof(x::AbstractVector) = sum(datasizeof, x)

juliaeltype(f::Meta.Field, b::Union{Meta.Binary, Meta.LargeBinary}) = Vector{UInt8}

function arrowtype(b, ::Type{Vector{UInt8}})
    # To support LargeBinary, we'd need a way to flag/pass a max length from user/actual data
    Meta.binaryStart(b)
    return Meta.Binary, Meta.binaryEnd(b), nothing
end

function default(::Type{A}) where {A <: AbstractVector{T}} where {T}
    a = similar(A, 1)
    a[1] = default(T)
    return a
end

juliaeltype(f::Meta.Field, x::Meta.FixedSizeBinary) = NTuple{Int(x.byteWidth), UInt8}

function arrowtype(b, ::Type{NTuple{N, UInt8}}) where {N}
    Meta.fixedSizeBinaryStart(b)
    Meta.fixedSizeBinaryAddByteWidth(b, Int32(N))
    return Meta.FixedSizeBinary, Meta.fixedSizeBinaryEnd(b), nothing
end

default(::Type{NTuple{N, T}}) where {N, T} = ntuple(i -> default(T), N)
default(::Type{T}) where {T <: Tuple} = Tuple(default(fieldtype(T, i)) for i = 1:fieldcount(T))

juliaeltype(f::Meta.Field, x::Meta.Bool) = Bool

function arrowtype(b, ::Type{Bool})
    Meta.boolStart(b)
    return Meta.Bool, Meta.boolEnd(b), nothing
end

struct Decimal{P, S}
    bytes::NTuple{16, UInt8}
end

Base.zero(::Type{Decimal{P, S}}) where {P, S} = Decimal{P, S}(ntuple(i->0x00, 16))

function juliaeltype(f::Meta.Field, x::Meta.Decimal)
    return Decimal{x.precision, x.scale}
end

function arrowtype(b, ::Type{Decimal{P, S}}) where {P, S}
    Meta.decimalStart(b)
    Meta.decimalAddPrecision(b, Int32(P))
    Meta.decimalAddScale(b, Int32(S))
    return Meta.Decimal, Meta.decimalEnd(b), nothing
end

abstract type ArrowTimeType end
Base.write(io::IO, x::ArrowTimeType) = Base.write(io, x.x)

struct Date{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Date{U, T}}) where {U, T} = Date{U, T}(T(0))

bitwidth(x::Meta.DateUnit) = x == Meta.DateUnit.DAY ? Int32 : Int64
Date{Meta.DateUnit.DAY}(days) = Date{Meta.DateUnit.DAY, Int32}(Int32(days))
Date{Meta.DateUnit.MILLISECOND}(ms) = Date{Meta.DateUnit.MILLISECOND, Int64}(Int64(ms))

juliaeltype(f::Meta.Field, x::Meta.Date) = Date{x.unit, bitwidth(x.unit)}
finaljuliatype(::Type{Date{Meta.DateUnit.DAY, Int32}}) = Dates.Date
Base.convert(::Type{Dates.Date}, x::Date{Meta.DateUnit.DAY, Int32}) = Dates.Date(Dates.UTD(Int64(x.x + UNIX_EPOCH_DATE)))
finaljuliatype(::Type{Date{Meta.DateUnit.MILLISECOND, Int64}}) = Dates.DateTime
Base.convert(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND, Int64}) = Dates.DateTime(Dates.UTM(Int64(x.x + UNIX_EPOCH_DATETIME)))

function arrowtype(b, ::Type{Date{U, T}}) where {U, T}
    Meta.dateStart(b)
    Meta.dateAddUnit(b, U)
    return Meta.Date, Meta.dateEnd(b), nothing
end

const UNIX_EPOCH_DATE = Dates.value(Dates.Date(1970))
Base.convert(::Type{Date{Meta.DateUnit.DAY, Int32}}, x::Dates.Date) = Date{Meta.DateUnit.DAY, Int32}(Int32(Dates.value(x) - UNIX_EPOCH_DATE))

const UNIX_EPOCH_DATETIME = Dates.value(Dates.DateTime(1970))
Base.convert(::Type{Date{Meta.DateUnit.MILLISECOND, Int64}}, x::Dates.DateTime) = Date{Meta.DateUnit.MILLISECOND, Int64}(Int64(Dates.value(x) - UNIX_EPOCH_DATETIME))

struct Time{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Time{U, T}}) where {U, T} = Time{U, T}(T(0))

bitwidth(x::Meta.TimeUnit) = x == Meta.TimeUnit.SECOND || x == Meta.TimeUnit.MILLISECOND ? Int32 : Int64
Time{U}(x) where {U <: Meta.TimeUnit} = Time{U, bitwidth(U)}(bitwidth(U)(x))

juliaeltype(f::Meta.Field, x::Meta.Time) = Time{x.unit, bitwidth(x.unit)}
finaljuliatype(::Type{<:Time}) = Dates.Time
periodtype(U::Meta.TimeUnit) = U === Meta.TimeUnit.SECOND ? Dates.Second :
                               U === Meta.TimeUnit.MILLISECOND ? Dates.Millisecond :
                               U === Meta.TimeUnit.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
Base.convert(::Type{Dates.Time}, x::Time{U, T}) where {U, T} = Dates.Time(Dates.Nanosecond(Dates.tons(periodtype(U)(x.x))))

function arrowtype(b, ::Type{Time{U, T}}) where {U, T}
    Meta.timeStart(b)
    Meta.timeAddUnit(b, U)
    return Meta.Time, Meta.timeEnd(b), nothing
end

Base.convert(::Type{Time{Meta.TimeUnit.NANOSECOND, Int64}}, x::Dates.Time) = Time{Meta.TimeUnit.NANOSECOND, Int64}(Dates.value(x))

struct Timestamp{U, TZ} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Timestamp{U, T}}) where {U, T} = Timestamp{U, T}(Int64(0))

function juliaeltype(f::Meta.Field, x::Meta.Timestamp)
    return Timestamp{x.unit, x.timezone === nothing ? nothing : Symbol(x.timezone)}
end

finaljuliatype(::Type{Timestamp{U, nothing}}) where {U} = Dates.DateTime
Base.convert(::Type{Dates.DateTime}, x::Timestamp{U, nothing}) where {U} =
    Dates.DateTime(Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)))

function arrowtype(b, ::Type{Timestamp{U, TZ}}) where {U, TZ}
    tz = TZ !== nothing ? FlatBuffers.createstring!(b, String(TZ)) : FlatBuffers.UOffsetT(0)
    Meta.timestampStart(b)
    Meta.timestampAddUnit(b, U)
    Meta.timestampAddTimezone(b, tz)
    return Meta.Timestamp, Meta.timestampEnd(b), nothing
end

struct Interval{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Interval{U, T}}) where {U, T} = Interval{U, T}(T(0))

bitwidth(x::Meta.IntervalUnit) = x == Meta.IntervalUnit.YEAR_MONTH ? Int32 : Int64
Interval{Meta.IntervalUnit.YEAR_MONTH}(x) = Interval{Meta.IntervalUnit.YEAR_MONTH, Int32}(Int32(x))
Interval{Meta.IntervalUnit.DAY_TIME}(x) = Interval{Meta.IntervalUnit.DAY_TIME, Int64}(Int64(x))

function juliaeltype(f::Meta.Field, x::Meta.Interval)
    return Interval{x.unit, bitwidth(x.unit)}
end

function arrowtype(b, ::Type{Interval{U, T}}) where {U, T}
    Meta.intervalStart(b)
    Meta.intervalAddUnit(b, U)
    return Meta.Interval, Meta.intervalEnd(b), nothing
end

struct Duration{U} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Duration{U}}) where {U} = Duration{U}(Int64(0))

function juliaeltype(f::Meta.Field, x::Meta.Duration)
    return Duration{x.unit}
end

finaljuliatype(::Type{Duration{U}}) where {U} = periodtype(U)
Base.convert(::Type{P}, x::Duration{U}) where {P <: Dates.Period, U} = P(periodtype(U)(x.x))

function arrowtype(b, ::Type{Duration{U}}) where {U}
    Meta.durationStart(b)
    Meta.durationAddUnit(b, U)
    return Meta.Duration, Meta.durationEnd(b), nothing
end

arrowperiodtype(P) = Meta.TimeUnit.SECOND
arrowperiodtype(::Type{Dates.Millisecond}) = Meta.TimeUnit.MILLISECOND
arrowperiodtype(::Type{Dates.Microsecond}) = Meta.TimeUnit.MICROSECOND
arrowperiodtype(::Type{Dates.Nanosecond}) = Meta.TimeUnit.NANOSECOND

Base.convert(::Type{Duration{U}}, x::Dates.Period) where {U} = Duration{U}(Dates.value(periodtype(U)(x)))

# nested types; call juliaeltype recursively on nested children
function juliaeltype(f::Meta.Field, list::Union{Meta.List, Meta.LargeList})
    T, _ = juliaeltype(f.children[1])
    return Vector{T}
end

# arrowtype will call fieldoffset recursively for children
function arrowtype(b, ::Type{Vector{T}}) where {T}
    children = [fieldoffset(b, -1, "", T, nothing, nothing)]
    Meta.listStart(b)
    return Meta.List, Meta.listEnd(b), children
end

function juliaeltype(f::Meta.Field, list::Meta.FixedSizeList)
    type, _ = juliaeltype(f.children[1])
    return NTuple{Int(list.listSize), type}
end

function arrowtype(b, ::Type{NTuple{N, T}}) where {N, T}
    children = [fieldoffset(b, -1, "", T, nothing, nothing)]
    Meta.fixedSizeListStart(b)
    Meta.fixedSizeListAddListSize(b, Int32(N))
    return Meta.FixedSizeList, Meta.fixedSizeListEnd(b), children
end

function juliaeltype(f::Meta.Field, map::Meta.Map)
    K, _ = juliaeltype(f.children[1].children[1])
    V, _ = juliaeltype(f.children[1].children[2])
    return Pair{K, V}
end

function arrowtype(b, ::Type{Pair{K, V}}) where {K, V}
    children = [fieldoffset(b, -1, "entries", KeyValue{K, V}, nothing, nothing)]
    Meta.mapStart(b)
    return Meta.Map, Meta.mapEnd(b), children
end

default(::Type{Pair{K, V}}) where {K, V} = default(K) => default(V)

struct KeyValue{K, V}
    key::K
    value::V
end
Base.length(kv::KeyValue) = 1
Base.iterate(kv::KeyValue, st=1) = st === nothing ? nothing : (kv, nothing)
default(::Type{KeyValue{K, V}}) where {K, V} = KeyValue(default(K), default(V))

function arrowtype(b, ::Type{KeyValue{K, V}}) where {K, V}
    children = [fieldoffset(b, -1, "key", K, nothing, nothing), fieldoffset(b, -1, "value", V, nothing, nothing)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

function juliaeltype(f::Meta.Field, list::Meta.Struct)
    names = Tuple(Symbol(x.name) for x in f.children)
    types = Tuple(juliaeltype(x)[1] for x in f.children)
    return NamedTuple{names, Tuple{types...}}
end

function arrowtype(b, ::Type{NamedTuple{names, types}}) where {names, types}
    children = [fieldoffset(b, -1, names[i], fieldtype(types, i), nothing, nothing) for i = 1:length(names)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

default(::Type{NamedTuple{names, types}}) where {names, types} = NamedTuple{names}(Tuple(default(fieldtype(types, i)) for i = 1:length(names)))

# Unions
function juliaeltype(f::Meta.Field, u::Meta.Union)
    return UnionT{u.mode, u.typeIds !== nothing ? Tuple(u.typeIds) : u.typeIds, Tuple{(juliaeltype(x)[1] for x in f.children)...}}
end

# Note: nested Union types can't be represented using julia's builtin Union{...}
arrowtype(b, U::Union) = arrowtype(b, UnionT{Meta.UnionMode.Dense, nothing, Tuple{eachunion(U)...}})

function arrowtype(b, ::Type{UnionT{T, typeIds, U}}) where {T, typeIds, U}
    if typeIds !== nothing
        Meta.unionStartTypeIdsVector(b, length(typeIds))
        for id in Iterators.reverse(typeIds)
            FlatBuffers.prepend!(b, id)
        end
        TI = FlatBuffers.endvector!(b, length(typeIds))
    end
    children = [fieldoffset(b, -1, "", fieldtype(U, i), nothing, nothing) for i = 1:fieldcount(U)]
    Meta.unionStart(b)
    Meta.unionAddMode(b, T)
    if typeIds !== nothing
        Meta.unionAddTypeIds(b, TI)
    end
    return Meta.Union, Meta.unionEnd(b), children
end
