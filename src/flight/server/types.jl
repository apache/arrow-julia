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

const ServerHeaderPair = HeaderPair

Base.@kwdef struct ServerCallContext
    headers::Vector{ServerHeaderPair} = ServerHeaderPair[]
    peer::Union{Nothing,String} = nothing
    secure::Bool = false
end

Base.@kwdef struct Service
    handshake::Union{Nothing,Function} = nothing
    listflights::Union{Nothing,Function} = nothing
    getflightinfo::Union{Nothing,Function} = nothing
    pollflightinfo::Union{Nothing,Function} = nothing
    getschema::Union{Nothing,Function} = nothing
    doget::Union{Nothing,Function} = nothing
    doput::Union{Nothing,Function} = nothing
    doexchange::Union{Nothing,Function} = nothing
    doaction::Union{Nothing,Function} = nothing
    listactions::Union{Nothing,Function} = nothing
end

function callheader(context::ServerCallContext, name::AbstractString)
    needle = lowercase(String(name))
    for (header_name, header_value) in context.headers
        lowercase(header_name) == needle && return header_value
    end
    return nothing
end
