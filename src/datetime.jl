
const UNIXEPOCH_TS = Dates.value(Dates.DateTime(1970))  # millisceonds
const UNIXEPOCH_DT = Dates.value(Dates.Date(1970))


value(x) = x


"""
    Timestamp{P<:Dates.TimePeriod}

Timestamp in which time is stored in units `P` as `Int64` for Arrow formatted data.

**TODO** Timezones not implemented.
"""
struct Timestamp{P<:Dates.TimePeriod}
    value::Int64
end
export Timestamp

Timestamp{P}(t::DateTime) where P = convert(Timestamp{P}, t)

value(t::Timestamp) = t.value


scale(::Type{Dates.Second}, t) = 1000*value(t)
scale(::Type{Dates.Millisecond}, t) = value(t)
scale(::Type{Dates.Microsecond}, t) = value(t)/1000
scale(::Type{Dates.Nanosecond}, t) = value(t)/1e6

invscale(::Type{Dates.Second}, t) = value(t)/1000
invscale(::Type{Dates.Millisecond}, t) = value(t)
invscale(::Type{Dates.Microsecond}, t) = 1000*value(t)
invscale(::Type{Dates.Nanosecond}, t) = 1e6*value(t)


convert(::Type{DateTime}, t::Timestamp{P}) where P = DateTime(Dates.UTM(UNIXEPOCH_TS + scale(P, t)))
function convert(::Type{Timestamp{P}}, t::DateTime) where P
    Timestamp{P}(invscale(P, Dates.value(t) - UNIXEPOCH_TS))
end

show(io::IO, t::Timestamp) = show(io, convert(DateTime, t))


# not sure I'll keep this silly name
"""
    Datestamp

Stores a date as an `Int32` for Arrow formatted data.
"""
struct Datestamp
    value::Int32
end
export Datestamp

value(t::Datestamp) = t.value


convert(::Type{Date}, t::Datestamp) = Date(Dates.UTD(UNIXEPOCH_DT + value(t)))
convert(::Type{Datestamp}, t::Date) = Datestamp(Dates.value(t) - UNIXEPOCH_DT)

show(io::IO, t::Datestamp) = show(io, convert(Date, t))
