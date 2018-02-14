
const UNIXEPOCH_TS = Dates.value(Dates.DateTime(1970))  # millisceonds
const UNIXEPOCH_DT = Dates.value(Dates.Date(1970))


value(x) = x


abstract type ArrowTime <: Dates.AbstractTime end

ArrowTime(t::Dates.TimeType) = convert(ArrowTime, t)

"""
    Timestamp{P<:Dates.TimePeriod} <: ArrowTime

Timestamp in which time is stored in units `P` as `Int64` for Arrow formatted data.
"""
struct Timestamp{P<:Dates.TimePeriod} <: ArrowTime
    value::Int64
end
export Timestamp

Timestamp(t::P) where P<:Dates.TimePeriod = Timestamp{P}(Dates.value(t))
Timestamp{P}(t::DateTime) where P<:Dates.TimePeriod = convert(Timestamp{P}, t)
Timestamp(t::DateTime) = convert(Timestamp, t)

value(t::Timestamp) = t.value
unitvalue(t::Timestamp{P}) where P = P(value(t))

scale(::Type{D}, t::Timestamp{P}) where {D,P} = convert(D, unitvalue(t))

function convert(::Type{DateTime}, t::Timestamp{P}) where P
    DateTime(Dates.UTM(UNIXEPOCH_TS + Dates.value(scale(Dates.Millisecond, t))))
end
convert(::Type{Dates.TimeType}, t::Timestamp) = convert(DateTime, t)

function convert(::Type{Timestamp{P}}, t::DateTime) where P
    Timestamp(convert(P, Dates.Millisecond(Dates.value(t) - UNIXEPOCH_TS)))
end
convert(::Type{Timestamp}, t::DateTime) = convert(Timestamp{Dates.Millisecond}, t)
convert(::Type{ArrowTime}, t::DateTime) = convert(Timestamp, t)

show(io::IO, t::Timestamp) = show(io, convert(DateTime, t))


"""
    TimeOfDay{P<:Dates.TimePeriod} <: ArrowTime

An arrow formatted object for representing the time of day.
"""
struct TimeOfDay{P<:Dates.TimePeriod} <: ArrowTime
    value::Int64
end
export TimeOfDay

TimeOfDay(t::P) where P<:Dates.TimePeriod = TimeOfDay{P}(Dates.value(t))
TimeOfDay{P}(t::Dates.Time) where P<:Dates.TimePeriod = convert(TimeOfDay{P}, t)
TimeOfDay(t::Dates.Time) = convert(TimeOfDay, t)

value(t::TimeOfDay) = t.value
unitvalue(t::TimeOfDay{P}) where P = P(value(t))

scale(::Type{D}, t::TimeOfDay{P}) where {D,P} = convert(D, unitvalue(t))

function convert(::Type{Dates.Time}, t::TimeOfDay{P}) where P
    Dates.Time(Dates.Nanosecond(scale(Dates.Nanosecond, t)))
end
convert(::Type{Dates.TimeType}, t::TimeOfDay) = convert(Dates.Time, t)

convert(::Type{TimeOfDay{P}}, t::Dates.Time) where P = TimeOfDay{P}(Dates.value(convert(P, t.instant)))
convert(::Type{TimeOfDay}, t::Dates.Time) = convert(TimeOfDay{Dates.Nanosecond}, t)
convert(::Type{ArrowTime}, t::Dates.Time) = convert(TimeOfDay, t)

show(io::IO, t::TimeOfDay) = show(io, convert(Dates.Time, t))


"""
    Datestamp <: ArrowTime

Stores a date as an `Int32` for Arrow formatted data.
"""
struct Datestamp <: ArrowTime
    value::Int32
end
export Datestamp

Datestamp(t::Date) = convert(Datestamp, t)

value(t::Datestamp) = t.value

convert(::Type{Date}, t::Datestamp) = Date(Dates.UTD(UNIXEPOCH_DT + value(t)))
convert(::Dates.TimeType, t::Datestamp) = convert(Date, t)

convert(::Type{Datestamp}, t::Date) = Datestamp(Dates.value(t) - UNIXEPOCH_DT)
convert(::Type{ArrowTime}, t::Date) = convert(Datestamp, t)

show(io::IO, t::Datestamp) = show(io, convert(Date, t))
