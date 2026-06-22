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

module ArrowTypesSocketsExt

import ArrowTypes
import Sockets: IPv4, IPv6, IPAddr, InetAddr

ArrowTypes.ArrowKind(::Type{IPv4}) = ArrowTypes.PrimitiveKind()
ArrowTypes.ArrowType(::Type{IPv4}) = UInt32
ArrowTypes.toarrow(x::IPv4) = x.host
const IPV4_SYMBOL = Symbol("JuliaLang.IPv4")
ArrowTypes.arrowname(::Type{IPv4}) = IPV4_SYMBOL
ArrowTypes.JuliaType(::Val{IPV4_SYMBOL}) = IPv4
ArrowTypes.fromarrow(::Type{IPv4}, x::Integer) = IPv4(x)

ArrowTypes.ArrowKind(::Type{IPv6}) = ArrowTypes.FixedSizeListKind{16,UInt8}()
ArrowTypes.ArrowType(::Type{IPv6}) = NTuple{16,UInt8}
ArrowTypes.toarrow(x::IPv6) = reinterpret(NTuple{16,UInt8}, x.host)
const IPV6_SYMBOL = Symbol("JuliaLang.IPv6")
ArrowTypes.arrowname(::Type{IPv6}) = IPV6_SYMBOL
ArrowTypes.JuliaType(::Val{IPV6_SYMBOL}) = IPv6
ArrowTypes.fromarrow(::Type{IPv6}, x::NTuple{16,UInt8}) = IPv6(reinterpret(UInt128, x))

const INET_ADDR_SYMBOL = Symbol("JuliaLang.InetAddr")
ArrowTypes.arrowname(::Type{<:InetAddr}) = INET_ADDR_SYMBOL
ArrowTypes.JuliaType(
    ::Val{INET_ADDR_SYMBOL},
    ::Type{@NamedTuple{host::T, port::UInt16}},
) where {T<:IPAddr} = InetAddr{T}

end # module ArrowTypesSocketsExt
