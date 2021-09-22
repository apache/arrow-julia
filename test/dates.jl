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

import Dates
import TimeZones

struct WrappedDate
    x::Dates.Date
end

ArrowTypes.arrowname(::Type{WrappedDate}) = Symbol("JuliaLang.WrappedDate")
ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.WrappedDate")}) = WrappedDate

struct WrappedTime
    x::Dates.Time
end

ArrowTypes.arrowname(::Type{WrappedTime}) = Symbol("JuliaLang.WrappedTime")
ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.WrappedTime")}) = WrappedTime

struct WrappedDateTime
    x::Dates.DateTime
end

ArrowTypes.arrowname(::Type{WrappedDateTime}) = Symbol("JuliaLang.WrappedDateTime")
ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.WrappedDateTime")}) = WrappedDateTime

struct WrappedZonedDateTime
    x::TimeZones.ZonedDateTime
end

ArrowTypes.arrowname(::Type{WrappedZonedDateTime}) = Symbol("JuliaLang.WrappedZonedDateTime")
ArrowTypes.JuliaType(::Val{Symbol("JuliaLang.WrappedZonedDateTime")}) = WrappedZonedDateTime

@testset "Date and time wrappers with missing" begin
    for T in (WrappedDate, WrappedTime, WrappedDateTime, WrappedZonedDateTime)
        if T == WrappedZonedDateTime
            time = T(Dates.now(TimeZones.tz"UTC"))
        else
            time = T(Dates.now())
        end
        table = (; x = [missing, missing, time, missing, time])
        io = Arrow.tobuffer(table)
        tbl = Arrow.Table(io)
        @test isequal(collect(tbl.x), table.x)
    end
end

@testset "`default(T) isa T`" begin
    for T in (Dates.Date, Dates.Time, Dates.DateTime, TimeZones.ZonedDateTime, Dates.Nanosecond, Dates.Millisecond, Dates.Second, Dates.Day, Dates.Month, Dates.Year)
        @test Arrow.ArrowTypes.default(T) isa T
    end
end
