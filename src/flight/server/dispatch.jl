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

function dispatch(
    service::Service,
    context::ServerCallContext,
    method::MethodDescriptor,
    args...,
)
    if method.handler_field === :handshake
        return handshake(service, context, args...)
    elseif method.handler_field === :listflights
        return listflights(service, context, args...)
    elseif method.handler_field === :getflightinfo
        return getflightinfo(service, context, args...)
    elseif method.handler_field === :pollflightinfo
        return pollflightinfo(service, context, args...)
    elseif method.handler_field === :getschema
        return getschema(service, context, args...)
    elseif method.handler_field === :doget
        return doget(service, context, args...)
    elseif method.handler_field === :doput
        return doput(service, context, args...)
    elseif method.handler_field === :doexchange
        return doexchange(service, context, args...)
    elseif method.handler_field === :doaction
        return doaction(service, context, args...)
    elseif method.handler_field === :listactions
        return listactions(service, context, args...)
    end

    throw(ArgumentError("unsupported Arrow Flight handler field $(method.handler_field)"))
end

function dispatch(
    service::Service,
    context::ServerCallContext,
    key::AbstractString,
    args...,
)
    method = lookupmethod(service, key)
    isnothing(method) &&
        throw(ArgumentError("unknown Arrow Flight method path or name: $(String(key))"))
    return dispatch(service, context, method, args...)
end
