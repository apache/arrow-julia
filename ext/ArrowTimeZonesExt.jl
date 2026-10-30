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
TimeZones.jl conveniences for timezone-aware timestamp columns. Reading
never needs this extension: a zone-declared timestamp column materializes as
`Durations.ZonedTimestamp` at every unit (Durations' own TimeZones extension
adds named-zone rules and `ZonedDateTime` conversions to those values).
Loading TimeZones.jl adds two things here:

  * Writing: a fresh `TimeZones.ZonedDateTime` column writes as a
    timezone-declared millisecond timestamp, matching Arrow 2.x, instead of
    falling through to reflected-struct lowering.
  * Scan and retained-write literals: a `ZonedDateTime` lowers exactly to a
    zone-declared column's storage domain (the UTC instant in the column
    unit), so filters comparing `ZonedDateTime` values push down.
"""
module ArrowTimeZonesExt

import Arrow
import Dates
import TimeZones

const AC = Arrow.ArrowCore

# Lower one ZonedDateTime to a zone-declared timestamp's storage domain
# exactly: the UTC instant in the descriptor's unit, or `nothing` when the
# value is not a ZonedDateTime or the unit cannot hold the instant exactly.
# The declared zone deliberately plays no role: storage is the UTC instant
# and zoned comparisons use only it, so semantics survive lowering.
function zonedstorage(unit::AC.TimeUnit, value)
    value isa TimeZones.ZonedDateTime || return nothing
    ms = Dates.value(Dates.DateTime(value, TimeZones.UTC)) - Dates.UNIXEPOCH
    unit == AC.MILLISECOND && return ms
    unit == AC.SECOND && return ms % 1000 == 0 ? ms ÷ 1000 : nothing
    scale = unit == AC.MICROSECOND ? Int64(1_000) : Int64(1_000_000)
    wide = Int128(ms) * Int128(scale)
    return Int128(typemin(Int64)) <= wide <= Int128(typemax(Int64)) ? Int64(wide) : nothing
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
