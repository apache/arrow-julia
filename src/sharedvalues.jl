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

# Shared logical values belong in the facade; ArrowCore keeps wire storage.
_decimalstorage(t::AC.DecimalType) =
    t.bits == 32 ? Int32 :
    t.bits == 64 ? Int64 : t.bits == 128 ? Int128 : DataDecimals.Int256
_shareddecimal(t::AC.DecimalType) = 0 <= t.scale <= t.precision <= 76
_decimalhost(t::AC.DecimalType) =
    DataDecimals.Decimal{t.precision,t.scale,_decimalstorage(t)}
_rawdeclaredbasetype(t::AC.DecimalType) =
    t.bits == 32 ? Int32 : t.bits == 64 ? Int64 : Vector{UInt8}
_rawdeclaredbasetype(t::AC.IntervalType) =
    t.unit == AC.YEAR_MONTH ? Int32 :
    t.unit == AC.DAY_TIME ? NamedTuple{(:days, :millis),Tuple{Int32,Int32}} :
    NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}}

function _decimalcoefficient(t::AC.DecimalType, x)
    x isa Integer && return _decimalstorage(t)(x)
    T = _decimalstorage(t)
    U = unsigned(T)
    u = zero(U)
    for b in Iterators.reverse(x)
        u = (u << 8) | U(b)
    end
    return reinterpret(T, u)
end

function _postconvert(t::AC.DecimalType, col, budget=nothing)
    _shareddecimal(t) || return col
    D = _decimalhost(t)
    return _mapcol(
        D,
        x -> begin
            u = _decimalcoefficient(t, x)
            bound = _decimalstorage(t)(10)^t.precision
            -bound < u < bound ||
                throw(ArgumentError("decimal coefficient exceeds declared precision"))
            reinterpret(D, u)
        end,
        col,
        budget,
    )
end

function _postconvert(t::AC.IntervalType, col, budget=nothing)
    return _mapcol(
        Durations.Duration,
        x ->
            t.unit == AC.YEAR_MONTH ? Durations.Duration(x, 0, 0) :
            t.unit == AC.DAY_TIME ?
            Durations.Duration(0, x.days, Int64(x.millis) * 1_000_000) :
            Durations.Duration(x.months, x.days, x.nanos),
        col,
        budget,
    )
end

function _shareddecimalraw(t::AC.DecimalType, x::DataDecimals.AbstractDecimal)
    DataDecimals.scale(x) == t.scale ||
        throw(ArgumentError("decimal scale does not match the Arrow schema"))
    u = _decimalstorage(t)(DataDecimals.unscaled(x))
    bound = _decimalstorage(t)(10)^t.precision
    -bound < u < bound ||
        throw(ArgumentError("decimal coefficient exceeds declared precision"))
    t.bits == 32 && return Int32(u)
    t.bits == 64 && return Int64(u)
    return UInt8[(u >> (8*i)) & 0xff for i = 0:(t.bits ÷ 8 - 1)]
end

function _sharedintervalraw(t::AC.IntervalType, x::Durations.Duration)
    if t.unit == AC.YEAR_MONTH
        iszero(x.days) && iszero(x.nanoseconds) ||
            throw(ArgumentError("year-month interval cannot hold days or nanoseconds"))
        return x.months
    elseif t.unit == AC.DAY_TIME
        iszero(x.months) && iszero(rem(x.nanoseconds, 1_000_000)) || throw(
            ArgumentError("day-time interval requires zero months and exact milliseconds"),
        )
        return (days=x.days, millis=Int32(div(x.nanoseconds, 1_000_000)))
    end
    return (months=x.months, days=x.days, nanos=x.nanoseconds)
end

function _constructsharedpart(name, v, ::Type{D}) where {D<:DataDecimals.Decimal}
    isconcretetype(D) || throw(
        ArgumentError(
            "an empty decimal column needs a concrete precision, scale, and storage type",
        ),
    )
    P, S, T = D.parameters
    t = AC.DecimalType(P, S, 8sizeof(T))
    return _retaineddecimal(AC.Field(name, t; nullable=Missing <: eltype(v)), v)
end
function _constructsharedpart(name, v, ::Type{Durations.Duration})
    t = AC.IntervalType(AC.MONTH_DAY_NANO)
    return _retainedinterval(AC.Field(name, t; nullable=Missing <: eltype(v)), v)
end
