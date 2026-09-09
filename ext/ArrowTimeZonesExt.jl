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

"""
Loading TimeZones.jl restores Arrow 2.x reads for timezone-aware timestamps:
a second- or millisecond-unit timestamp column that declares a timezone
materializes as `TimeZones.ZonedDateTime` (the stored value is the UTC
instant; the declared zone is the display zone) instead of a naive UTC
`Dates.DateTime`.

Micro- and nanosecond timestamps keep their raw `Int64` storage values with
or without this extension: neither `DateTime` nor `ZonedDateTime` can hold
them exactly, and Arrow 3.0 never truncates silently. A declared zone that
TimeZones.jl cannot parse falls back to the naive read with a one-time
warning. Scan filters that compare `ZonedDateTime` literals evaluate in the
public value domain (no storage-domain pushdown).
"""
module ArrowTimeZonesExt

import Arrow
import Dates
import TimeZones

const AC = Arrow.ArrowCore

# Parse an Arrow timezone string: an IANA name ("America/Denver", legacy
# aliases included) or a fixed offset ("+07:00"). `nothing` means the zone is
# unusable and the caller keeps the naive read.
function _timezone(tz::AbstractString)
    try
        return TimeZones.TimeZone(tz, TimeZones.Class(:ALL))
    catch
    end
    try
        return TimeZones.FixedTimeZone(tz)
    catch
    end
    @warn "Arrow timestamp declares timezone $(repr(String(tz))) that " *
          "TimeZones.jl cannot parse; reading the column as naive UTC values" _id =
        Symbol(:arrow_bad_timezone_, tz) maxlog = 1
    return nothing
end

# The two hooks `Arrow._zonedext` routes to. Both return `nothing` for an
# unusable zone so the eltype decision and the conversion agree.
zonedtype(tz::AbstractString) =
    _timezone(tz) === nothing ? nothing : TimeZones.ZonedDateTime

# Lower one ZonedDateTime to a timestamp's storage domain exactly: the UTC
# instant in the descriptor's unit, or `nothing` when the value is not a
# ZonedDateTime or would not round-trip (a second-unit column cannot hold a
# sub-second instant). The declared zone deliberately plays no role: storage
# is the UTC instant, so comparison semantics survive lowering.
function zonedstorage(unit::AC.TimeUnit, value)
    value isa TimeZones.ZonedDateTime || return nothing
    ms = Dates.value(Dates.DateTime(value, TimeZones.UTC)) - Dates.UNIXEPOCH
    unit == AC.MILLISECOND && return ms
    (unit == AC.SECOND && ms % 1000 == 0) && return ms ÷ 1000
    return nothing
end

function zonedcolumn(t::AC.TimestampType, col, budget)
    zone = _timezone(t.timezone)
    zone === nothing && return nothing
    scale = t.unit == AC.SECOND ? Int64(1000) : Int64(1)
    return Arrow._mapcol(
        TimeZones.ZonedDateTime,
        x -> TimeZones.ZonedDateTime(
            Dates.DateTime(Dates.UTM(Int64(x) * scale + Dates.UNIXEPOCH)),
            zone;
            from_utc=true,
        ),
        col,
        budget,
    )
end

# The fresh-write hooks `Arrow._zonedwritertype`/`Arrow._zonednativepart`
# route to. A fresh ZonedDateTime column writes as a timezone-declared
# millisecond timestamp, matching Arrow 2.x, instead of falling through to
# reflected-struct lowering of the zone's whole transition table.
iszonedtype(T::Type) = T === TimeZones.ZonedDateTime

# One column carries one zone (the descriptor has one timezone slot), so
# mixed zones refuse with a normalization hint. An empty or all-missing
# column has no zone evidence and declares UTC.
function zonednativepart(name::String, v::AbstractVector, T::Type)
    T === TimeZones.ZonedDateTime || return nothing
    zone = nothing
    for x in v
        x === missing && continue
        z = TimeZones.timezone(x)
        if zone === nothing
            zone = z
        elseif z != zone
            throw(
                ArgumentError(
                    "column $name holds ZonedDateTime values in more than " *
                    "one timezone ($(TimeZones.name(zone)) and " *
                    "$(TimeZones.name(z))); convert them to one zone with " *
                    "astimezone before writing",
                ),
            )
        end
    end
    tzname = zone === nothing ? "UTC" : String(TimeZones.name(zone))
    return Arrow._constructtemporalpart(
        name,
        v,
        AC.TimestampType(AC.MILLISECOND, tzname),
        x -> Int64(Dates.value(Dates.DateTime(x, TimeZones.UTC)) - Dates.UNIXEPOCH),
    )
end

end # module ArrowTimeZonesExt
