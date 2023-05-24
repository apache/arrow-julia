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

"""
Given a flatbuffers metadata type definition (a Field instance from Schema.fbs),
translate to the appropriate Julia storage eltype
"""
function juliaeltype end

finaljuliatype(T) = T
finaljuliatype(::Type{Missing}) = Missing
finaljuliatype(::Type{Union{T, Missing}}) where {T} = Union{Missing, finaljuliatype(T)}

"""
Given a FlatBuffers.Builder and a Julia column or column eltype,
Write the field.type flatbuffer definition of the eltype
"""
function arrowtype end

arrowtype(b, col::AbstractVector{T}) where {T} = arrowtype(b, maybemissing(T))
arrowtype(b, col::DictEncoded) = arrowtype(b, col.encoding.data)
arrowtype(b, col::Compressed) = arrowtype(b, col.data)

function juliaeltype(f::Meta.Field, ::Nothing, convert::Bool)
    T = juliaeltype(f, convert)
    return convert ? finaljuliatype(T) : T
end

function juliaeltype(f::Meta.Field, meta::AbstractDict{String, String}, convert::Bool)
    TT = juliaeltype(f, convert)
    !convert && return TT
    T = finaljuliatype(TT)
    if haskey(meta, "ARROW:extension:name")
        typename = meta["ARROW:extension:name"]
        metadata = get(meta, "ARROW:extension:metadata", "")
        JT = ArrowTypes.JuliaType(Val(Symbol(typename)), maybemissing(TT), metadata)
        if JT !== nothing
            return f.nullable ? Union{JT, Missing} : JT
        else
            @warn "unsupported ARROW:extension:name type: \"$typename\", arrow type = $TT" maxlog=1 _id=hash((:juliaeltype, typename, TT))
        end
    end
    return something(TT, T)
end

function juliaeltype(f::Meta.Field, convert::Bool)
    T = juliaeltype(f, f.type, convert)
    return f.nullable ? Union{T, Missing} : T
end

juliaeltype(f::Meta.Field, ::Meta.Null, convert) = Missing

function arrowtype(b, ::Type{Missing})
    Meta.nullStart(b)
    return Meta.Null, Meta.nullEnd(b), nothing
end

function juliaeltype(f::Meta.Field, int::Meta.Int, convert)
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
            error("$int is not valid arrow type metadata")
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
            error("$int is not valid arrow type metadata")
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
function juliaeltype(f::Meta.Field, fp::Meta.FloatingPoint, convert)
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

juliaeltype(f::Meta.Field, b::Union{Meta.Utf8, Meta.LargeUtf8}, convert) = String

datasizeof(x) = sizeof(x)
datasizeof(x::AbstractVector) = sum(datasizeof, x)

juliaeltype(f::Meta.Field, b::Union{Meta.Binary, Meta.LargeBinary}, convert) = Base.CodeUnits

juliaeltype(f::Meta.Field, x::Meta.FixedSizeBinary, convert) = NTuple{Int(x.byteWidth), UInt8}

# arggh!
Base.write(io::IO, x::NTuple{N, T}) where {N, T} = sum(y -> Base.write(io, y), x)

juliaeltype(f::Meta.Field, x::Meta.Bool, convert) = Bool

function arrowtype(b, ::Type{Bool})
    Meta.boolStart(b)
    return Meta.Bool, Meta.boolEnd(b), nothing
end

struct Decimal{P, S, T}
    value::T # only Int128 or Int256
end

Base.zero(::Type{Decimal{P, S, T}}) where {P, S, T} = Decimal{P, S, T}(T(0))
==(a::Decimal{P, S, T}, b::Decimal{P, S, T}) where {P, S, T} = ==(a.value, b.value)
Base.isequal(a::Decimal{P, S, T}, b::Decimal{P, S, T}) where {P, S, T} = isequal(a.value, b.value)

function juliaeltype(f::Meta.Field, x::Meta.Decimal, convert)
    return Decimal{x.precision, x.scale, x.bitWidth == 256 ? Int256 : Int128}
end

ArrowTypes.ArrowKind(::Type{<:Decimal}) = PrimitiveKind()

function arrowtype(b, ::Type{Decimal{P, S, T}}) where {P, S, T}
    Meta.decimalStart(b)
    Meta.decimalAddPrecision(b, Int32(P))
    Meta.decimalAddScale(b, Int32(S))
    Meta.decimalAddBitWidth(b, Int32(T == Int256 ? 256 : 128))
    return Meta.Decimal, Meta.decimalEnd(b), nothing
end

Base.write(io::IO, x::Decimal) = Base.write(io, x.value)

abstract type ArrowTimeType end
Base.write(io::IO, x::ArrowTimeType) = Base.write(io, x.x)
ArrowTypes.ArrowKind(::Type{<:ArrowTimeType}) = PrimitiveKind()

struct Date{U, T} <: ArrowTimeType
    x::T
end

const DATE = Date{Meta.DateUnit.DAY, Int32}
Base.zero(::Type{Date{U, T}}) where {U, T} = Date{U, T}(T(0))
storagetype(::Type{Date{U, T}}) where {U, T} = T
bitwidth(x::Meta.DateUnit.T) = x == Meta.DateUnit.DAY ? Int32 : Int64
Date{Meta.DateUnit.DAY}(days) = DATE(Int32(days))
Date{Meta.DateUnit.MILLISECOND}(ms) = Date{Meta.DateUnit.MILLISECOND, Int64}(Int64(ms))

juliaeltype(f::Meta.Field, x::Meta.Date, convert) = Date{x.unit, bitwidth(x.unit)}
finaljuliatype(::Type{DATE}) = Dates.Date
Base.convert(::Type{Dates.Date}, x::DATE) = Dates.Date(Dates.UTD(Int64(x.x + UNIX_EPOCH_DATE)))
finaljuliatype(::Type{Date{Meta.DateUnit.MILLISECOND, Int64}}) = Dates.DateTime
Base.convert(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND, Int64}) = Dates.DateTime(Dates.UTM(Int64(x.x + UNIX_EPOCH_DATETIME)))

function arrowtype(b, ::Type{Date{U, T}}) where {U, T}
    Meta.dateStart(b)
    Meta.dateAddUnit(b, U)
    return Meta.Date, Meta.dateEnd(b), nothing
end

const UNIX_EPOCH_DATE = Dates.value(Dates.Date(1970))
Base.convert(::Type{DATE}, x::Dates.Date) = DATE(Int32(Dates.value(x) - UNIX_EPOCH_DATE))

const UNIX_EPOCH_DATETIME = Dates.value(Dates.DateTime(1970))
Base.convert(::Type{Date{Meta.DateUnit.MILLISECOND, Int64}}, x::Dates.DateTime) = Date{Meta.DateUnit.MILLISECOND, Int64}(Int64(Dates.value(x) - UNIX_EPOCH_DATETIME))

ArrowTypes.ArrowType(::Type{Dates.Date}) = DATE
ArrowTypes.toarrow(x::Dates.Date) = convert(DATE, x)
const DATE_SYMBOL = Symbol("JuliaLang.Date")
ArrowTypes.arrowname(::Type{Dates.Date}) = DATE_SYMBOL
ArrowTypes.JuliaType(::Val{DATE_SYMBOL}, S) = Dates.Date
ArrowTypes.fromarrow(::Type{Dates.Date}, x::DATE) = convert(Dates.Date, x)
ArrowTypes.default(::Type{Dates.Date}) = Dates.Date(1,1,1)

struct Time{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Time{U, T}}) where {U, T} = Time{U, T}(T(0))
const TIME = Time{Meta.TimeUnit.NANOSECOND, Int64}

bitwidth(x::Meta.TimeUnit.T) = x == Meta.TimeUnit.SECOND || x == Meta.TimeUnit.MILLISECOND ? Int32 : Int64
Time{U}(x) where {U <: Meta.TimeUnit.T} = Time{U, bitwidth(U)}(bitwidth(U)(x))
storagetype(::Type{Time{U, T}}) where {U, T} = T
juliaeltype(f::Meta.Field, x::Meta.Time, convert) = Time{x.unit, bitwidth(x.unit)}
finaljuliatype(::Type{<:Time}) = Dates.Time
periodtype(U::Meta.TimeUnit.T) = U === Meta.TimeUnit.SECOND ? Dates.Second :
                               U === Meta.TimeUnit.MILLISECOND ? Dates.Millisecond :
                               U === Meta.TimeUnit.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
Base.convert(::Type{Dates.Time}, x::Time{U, T}) where {U, T} = Dates.Time(Dates.Nanosecond(Dates.tons(periodtype(U)(x.x))))

function arrowtype(b, ::Type{Time{U, T}}) where {U, T}
    Meta.timeStart(b)
    Meta.timeAddUnit(b, U)
    Meta.timeAddBitWidth(b, Int32(8 * sizeof(T)))
    return Meta.Time, Meta.timeEnd(b), nothing
end

Base.convert(::Type{TIME}, x::Dates.Time) = TIME(Dates.value(x))

ArrowTypes.ArrowType(::Type{Dates.Time}) = TIME
ArrowTypes.toarrow(x::Dates.Time) = convert(TIME, x)
const TIME_SYMBOL = Symbol("JuliaLang.Time")
ArrowTypes.arrowname(::Type{Dates.Time}) = TIME_SYMBOL
ArrowTypes.JuliaType(::Val{TIME_SYMBOL}, S) = Dates.Time
ArrowTypes.fromarrow(::Type{Dates.Time}, x::TIME) = convert(Dates.Time, x)
ArrowTypes.default(::Type{Dates.Time}) = Dates.Time(1,1,1)

struct Timestamp{U, TZ} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Timestamp{U, T}}) where {U, T} = Timestamp{U, T}(Int64(0))

function juliaeltype(f::Meta.Field, x::Meta.Timestamp, convert)
    return Timestamp{x.unit, x.timezone === nothing ? nothing : Symbol(x.timezone)}
end

const DATETIME = Timestamp{Meta.TimeUnit.MILLISECOND, nothing}

finaljuliatype(::Type{Timestamp{U, TZ}}) where {U, TZ} = ZonedDateTime
finaljuliatype(::Type{Timestamp{U, nothing}}) where {U} = DateTime

@noinline warntimestamp(U, T) =
    @warn "automatically converting Arrow.Timestamp with precision = $U to `$T` which only supports millisecond precision; conversion may be lossy; to avoid converting, pass `Arrow.Table(source; convert=false)" maxlog=1 _id=hash((:warntimestamp, U, T))

function Base.convert(::Type{ZonedDateTime}, x::Timestamp{U, TZ}) where {U, TZ}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) && warntimestamp(U, ZonedDateTime)
    return ZonedDateTime(Dates.DateTime(Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME))), TimeZone(String(TZ)); from_utc=true)
end

function Base.convert(::Type{DateTime}, x::Timestamp{U, nothing}) where {U}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) && warntimestamp(U, DateTime)
    return Dates.DateTime(Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)))
end

Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND, TZ}}, x::ZonedDateTime) where {TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND, TZ}(Int64(Dates.value(DateTime(x, UTC)) - UNIX_EPOCH_DATETIME))
Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND, nothing}}, x::DateTime) =
    Timestamp{Meta.TimeUnit.MILLISECOND, nothing}(Int64(Dates.value(x) - UNIX_EPOCH_DATETIME))

function arrowtype(b, ::Type{Timestamp{U, TZ}}) where {U, TZ}
    tz = TZ !== nothing ? FlatBuffers.createstring!(b, String(TZ)) : FlatBuffers.UOffsetT(0)
    Meta.timestampStart(b)
    Meta.timestampAddUnit(b, U)
    Meta.timestampAddTimezone(b, tz)
    return Meta.Timestamp, Meta.timestampEnd(b), nothing
end

ArrowTypes.ArrowType(::Type{Dates.DateTime}) = DATETIME
ArrowTypes.toarrow(x::Dates.DateTime) = convert(DATETIME, x)
const DATETIME_SYMBOL = Symbol("JuliaLang.DateTime")
ArrowTypes.arrowname(::Type{Dates.DateTime}) = DATETIME_SYMBOL
ArrowTypes.JuliaType(::Val{DATETIME_SYMBOL}, S) = Dates.DateTime
ArrowTypes.fromarrow(::Type{Dates.DateTime}, x::Timestamp) = convert(Dates.DateTime, x)
ArrowTypes.fromarrow(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND, Int64}) = convert(Dates.DateTime, x)
ArrowTypes.default(::Type{Dates.DateTime}) = Dates.DateTime(1,1,1,1,1,1)

ArrowTypes.ArrowType(::Type{ZonedDateTime}) = Timestamp
ArrowTypes.toarrow(x::ZonedDateTime) = convert(Timestamp{Meta.TimeUnit.MILLISECOND, Symbol(x.timezone)}, x)
const ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime-UTC")
ArrowTypes.arrowname(::Type{ZonedDateTime}) = ZONEDDATETIME_SYMBOL
ArrowTypes.JuliaType(::Val{ZONEDDATETIME_SYMBOL}, S) = ZonedDateTime
ArrowTypes.fromarrow(::Type{ZonedDateTime}, x::Timestamp) = convert(ZonedDateTime, x)
ArrowTypes.default(::Type{TimeZones.ZonedDateTime}) = TimeZones.ZonedDateTime(1,1,1,1,1,1,TimeZones.tz"UTC")

# Backwards compatibility: older versions of Arrow saved ZonedDateTime's with this metdata:
const OLD_ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime")
# and stored the local time instead of the UTC time.
struct LocalZonedDateTime end
ArrowTypes.JuliaType(::Val{OLD_ZONEDDATETIME_SYMBOL}, S) = LocalZonedDateTime
function ArrowTypes.fromarrow(::Type{LocalZonedDateTime}, x::Timestamp{U, TZ}) where {U, TZ}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) && warntimestamp(U, ZonedDateTime)
    return ZonedDateTime(Dates.DateTime(Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME))), TimeZone(String(TZ)))
end


"""
    Arrow.ToTimestamp(x::AbstractVector{ZonedDateTime})

Wrapper array that provides a more efficient encoding of `ZonedDateTime` elements to the arrow format. In the arrow format,
timestamp columns with timezone information are encoded as the arrow equivalent of a Julia type parameter, meaning an entire column
_should_ have elements all with the same timezone. If a `ZonedDateTime` column is passed to `Arrow.write`, for correctness, it must
scan each element to check each timezone. `Arrow.ToTimestamp` provides a "bypass" of this process by encoding the timezone of the
first element of the `AbstractVector{ZonedDateTime}`, which in turn allows `Arrow.write` to avoid costly checking/conversion and
can encode the `ZonedDateTime` as `Arrow.Timestamp` directly.
"""
struct ToTimestamp{A, TZ} <: AbstractVector{Timestamp{Meta.TimeUnit.MILLISECOND, TZ}}
    data::A # AbstractVector{ZonedDateTime}
end

ToTimestamp(x::A) where {A <: AbstractVector{ZonedDateTime}} = ToTimestamp{A, Symbol(x[1].timezone)}(x)
Base.IndexStyle(::Type{<:ToTimestamp}) = Base.IndexLinear()
Base.size(x::ToTimestamp) = (length(x.data),)
Base.eltype(::ToTimestamp{A, TZ}) where {A, TZ} = Timestamp{Meta.TimeUnit.MILLISECOND, TZ}
Base.getindex(x::ToTimestamp{A, TZ}, i::Int) where {A, TZ} = convert(Timestamp{Meta.TimeUnit.MILLISECOND, TZ}, getindex(x.data, i))

struct Interval{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Interval{U, T}}) where {U, T} = Interval{U, T}(T(0))

bitwidth(x::Meta.IntervalUnit.T) = x == Meta.IntervalUnit.YEAR_MONTH ? Int32 : Int64
Interval{Meta.IntervalUnit.YEAR_MONTH}(x) = Interval{Meta.IntervalUnit.YEAR_MONTH, Int32}(Int32(x))
Interval{Meta.IntervalUnit.DAY_TIME}(x) = Interval{Meta.IntervalUnit.DAY_TIME, Int64}(Int64(x))

function juliaeltype(f::Meta.Field, x::Meta.Interval, convert)
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

function juliaeltype(f::Meta.Field, x::Meta.Duration, convert)
    return Duration{x.unit}
end

finaljuliatype(::Type{Duration{U}}) where {U} = periodtype(U)
Base.convert(::Type{P}, x::Duration{U}) where {P <: Dates.Period, U} = P(periodtype(U)(x.x))

function arrowtype(b, ::Type{Duration{U}}) where {U}
    Meta.durationStart(b)
    Meta.durationAddUnit(b, U)
    return Meta.Duration, Meta.durationEnd(b), nothing
end

arrowtype(b, ::Type{P}) where {P <: Dates.Period} = arrowtype(b, Duration{arrowperiodtype(P)})

arrowperiodtype(P) = Meta.TimeUnit.SECOND
arrowperiodtype(::Type{Dates.Millisecond}) = Meta.TimeUnit.MILLISECOND
arrowperiodtype(::Type{Dates.Microsecond}) = Meta.TimeUnit.MICROSECOND
arrowperiodtype(::Type{Dates.Nanosecond}) = Meta.TimeUnit.NANOSECOND

Base.convert(::Type{Duration{U}}, x::Dates.Period) where {U} = Duration{U}(Dates.value(periodtype(U)(x)))

ArrowTypes.ArrowType(::Type{P}) where {P <: Dates.Period} = Duration{arrowperiodtype(P)}
ArrowTypes.toarrow(x::P) where {P <: Dates.Period} = convert(Duration{arrowperiodtype(P)}, x)
const PERIOD_SYMBOL = Symbol("JuliaLang.Dates.Period")
ArrowTypes.arrowname(::Type{P}) where {P <: Dates.Period} = PERIOD_SYMBOL
ArrowTypes.JuliaType(::Val{PERIOD_SYMBOL}, ::Type{Duration{U}}) where {U} = periodtype(U)
ArrowTypes.fromarrow(::Type{P}, x::Duration{U}) where {P <: Dates.Period, U} = convert(P, x)

# nested types; call juliaeltype recursively on nested children
function juliaeltype(f::Meta.Field, list::Union{Meta.List, Meta.LargeList}, convert)
    return Vector{juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)}
end

# arrowtype will call fieldoffset recursively for children
function arrowtype(b, x::List{T, O, A}) where {T, O, A}
    if liststringtype(x)
        if T <: AbstractString || T <: Union{AbstractString, Missing}
            if O == Int32
                Meta.utf8Start(b)
                return Meta.Utf8, Meta.utf8End(b), nothing
            else # if O == Int64
                Meta.largUtf8Start(b)
                return Meta.LargeUtf8, Meta.largUtf8End(b), nothing
            end
        else # if Base.CodeUnits
            if O == Int32
                Meta.binaryStart(b)
                return Meta.Binary, Meta.binaryEnd(b), nothing
            else # if O == Int64
                Meta.largeBinaryStart(b)
                return Meta.LargeBinary, Meta.largeBinaryEnd(b), nothing
            end
        end
    else
        children = [fieldoffset(b, "", x.data)]
        if O == Int32
            Meta.listStart(b)
            return Meta.List, Meta.listEnd(b), children
        else
            Meta.largeListStart(b)
            return Meta.LargeList, Meta.largeListEnd(b), children
        end
    end
end

function juliaeltype(f::Meta.Field, list::Meta.FixedSizeList, convert)
    type = juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)
    return NTuple{Int(list.listSize), type}
end

function arrowtype(b, x::FixedSizeList{T, A}) where {T, A}
    N = ArrowTypes.getsize(ArrowTypes.ArrowKind(ArrowTypes.ArrowType(Base.nonmissingtype(T))))
    if eltype(A) == UInt8
        Meta.fixedSizeBinaryStart(b)
        Meta.fixedSizeBinaryAddByteWidth(b, Int32(N))
        return Meta.FixedSizeBinary, Meta.fixedSizeBinaryEnd(b), nothing
    else
        children = [fieldoffset(b, "", x.data)]
        Meta.fixedSizeListStart(b)
        Meta.fixedSizeListAddListSize(b, Int32(N))
        return Meta.FixedSizeList, Meta.fixedSizeListEnd(b), children
    end
end

function juliaeltype(f::Meta.Field, map::Meta.Map, convert)
    K = juliaeltype(f.children[1].children[1], buildmetadata(f.children[1].children[1]), convert)
    V = juliaeltype(f.children[1].children[2], buildmetadata(f.children[1].children[2]), convert)
    return Dict{K, V}
end

function arrowtype(b, x::Map)
    children = [fieldoffset(b, "entries", x.data)]
    Meta.mapStart(b)
    return Meta.Map, Meta.mapEnd(b), children
end

struct KeyValue{K, V}
    key::K
    value::V
end
keyvalueK(::Type{KeyValue{K, V}}) where {K, V} = K
keyvalueV(::Type{KeyValue{K, V}}) where {K, V} = V
Base.length(kv::KeyValue) = 1
Base.iterate(kv::KeyValue, st=1) = st === nothing ? nothing : (kv, nothing)
ArrowTypes.default(::Type{KeyValue{K, V}}) where {K, V} = KeyValue(default(K), default(V))

function arrowtype(b, ::Type{KeyValue{K, V}}) where {K, V}
    children = [fieldoffset(b, "key", K), fieldoffset(b, "value", V)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

function juliaeltype(f::Meta.Field, list::Meta.Struct, convert)
    names = Tuple(Symbol(x.name) for x in f.children)
    types = Tuple(juliaeltype(x, buildmetadata(x), convert) for x in f.children)
    return NamedTuple{names, Tuple{types...}}
end

function arrowtype(b, x::Struct{T, S}) where {T, S}
    names = fieldnames(Base.nonmissingtype(T))
    children = [fieldoffset(b, names[i], x.data[i]) for i = 1:length(names)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

# Unions
function UnionT(f::Meta.Field, convert)
    typeids = f.type.typeIds === nothing ? nothing : Tuple(Int(x) for x in f.type.typeIds)
    UT =  UnionT{f.type.mode, typeids, Tuple{(juliaeltype(x, buildmetadata(x), convert) for x in f.children)...}}
    return UT
end

juliaeltype(f::Meta.Field, u::Meta.Union, convert) = Union{(juliaeltype(x, buildmetadata(x), convert) for x in f.children)...}

function arrowtype(b, x::Union{DenseUnion{S, UnionT{T, typeIds, U}}, SparseUnion{S, UnionT{T, typeIds, U}}}) where {S, T, typeIds, U}
    if typeIds !== nothing
        Meta.unionStartTypeIdsVector(b, length(typeIds))
        for id in Iterators.reverse(typeIds)
            FlatBuffers.prepend!(b, id)
        end
        TI = FlatBuffers.endvector!(b, length(typeIds))
    end
    children = [fieldoffset(b, "", x.data[i]) for i = 1:fieldcount(U)]
    Meta.unionStart(b)
    Meta.unionAddMode(b, T)
    if typeIds !== nothing
        Meta.unionAddTypeIds(b, TI)
    end
    return Meta.Union, Meta.unionEnd(b), children
end
