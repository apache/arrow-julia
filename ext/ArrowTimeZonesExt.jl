module ArrowTimeZonesExt

using Arrow
using Dates
using TimeZones

using Arrow: ArrowTypes, DATETIME, FlatBuffers, Meta, periodtype, Timestamp, UNIX_EPOCH_DATETIME

Arrow.finaljuliatype(::Type{Timestamp{U,TZ}}) where {U,TZ} = ZonedDateTime
Arrow.finaljuliatype(::Type{Timestamp{U,nothing}}) where {U} = DateTime

@noinline warntimestamp(U, T) =
    @warn "automatically converting Arrow.Timestamp with precision = $U to `$T` which only supports millisecond precision; conversion may be lossy; to avoid converting, pass `Arrow.Table(source; convert=false)" maxlog =
        1 _id = hash((:warntimestamp, U, T))

function Base.convert(::Type{ZonedDateTime}, x::Timestamp{U,TZ}) where {U,TZ}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, ZonedDateTime)
    return ZonedDateTime(
        Dates.DateTime(
            Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
        ),
        TimeZone(String(TZ));
        from_utc=true,
    )
end

function Base.convert(::Type{DateTime}, x::Timestamp{U,nothing}) where {U}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, DateTime)
    return Dates.DateTime(
        Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
    )
end

Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND,TZ}}, x::ZonedDateTime) where {TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND,TZ}(
        Int64(Dates.value(DateTime(x, UTC)) - UNIX_EPOCH_DATETIME),
    )
Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND,nothing}}, x::DateTime) =
    Timestamp{Meta.TimeUnit.MILLISECOND,nothing}(
        Int64(Dates.value(x) - UNIX_EPOCH_DATETIME),
    )

function Arrow.arrowtype(b, ::Type{Timestamp{U,TZ}}) where {U,TZ}
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
ArrowTypes.fromarrow(::Type{Dates.DateTime}, x::Arrow.Date{Meta.DateUnit.MILLISECOND,Int64}) =
    convert(Dates.DateTime, x)
ArrowTypes.default(::Type{Dates.DateTime}) = Dates.DateTime(1, 1, 1, 1, 1, 1)

ArrowTypes.ArrowType(::Type{ZonedDateTime}) = Timestamp
ArrowTypes.toarrow(x::ZonedDateTime) =
    convert(Timestamp{Meta.TimeUnit.MILLISECOND,Symbol(x.timezone)}, x)
const ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime-UTC")
ArrowTypes.arrowname(::Type{ZonedDateTime}) = ZONEDDATETIME_SYMBOL
ArrowTypes.JuliaType(::Val{ZONEDDATETIME_SYMBOL}, S) = ZonedDateTime
ArrowTypes.fromarrow(::Type{ZonedDateTime}, x::Timestamp) = convert(ZonedDateTime, x)
ArrowTypes.default(::Type{TimeZones.ZonedDateTime}) =
    TimeZones.ZonedDateTime(1, 1, 1, 1, 1, 1, TimeZones.tz"UTC")

# Backwards compatibility: older versions of Arrow saved ZonedDateTime's with this metdata:
const OLD_ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime")
# and stored the local time instead of the UTC time.
struct LocalZonedDateTime end
ArrowTypes.JuliaType(::Val{OLD_ZONEDDATETIME_SYMBOL}, S) = LocalZonedDateTime
function ArrowTypes.fromarrow(::Type{LocalZonedDateTime}, x::Timestamp{U,TZ}) where {U,TZ}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, ZonedDateTime)
    return ZonedDateTime(
        Dates.DateTime(
            Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
        ),
        TimeZone(String(TZ)),
    )
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
struct ToTimestamp{A,TZ} <: AbstractVector{Timestamp{Meta.TimeUnit.MILLISECOND,TZ}}
    data::A # AbstractVector{ZonedDateTime}
end

ToTimestamp(x::A) where {A<:AbstractVector{ZonedDateTime}} =
    ToTimestamp{A,Symbol(x[1].timezone)}(x)
Base.IndexStyle(::Type{<:ToTimestamp}) = Base.IndexLinear()
Base.size(x::ToTimestamp) = (length(x.data),)
Base.eltype(::Type{ToTimestamp{A,TZ}}) where {A,TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND,TZ}
Base.getindex(x::ToTimestamp{A,TZ}, i::Integer) where {A,TZ} =
    convert(Timestamp{Meta.TimeUnit.MILLISECOND,TZ}, getindex(x.data, i))


function __init__()
    # we need to add extension types back to the toplevel module
    @static if VERSION >= v"1.9"
        setglobal!(Arrow, :ToTimestamp, ToTimestamp)
    end
end


end # module
