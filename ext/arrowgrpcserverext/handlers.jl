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

function _unary_handler(service::Flight.Service, method::Flight.MethodDescriptor)
    return (context, request) ->
        Flight.dispatch(service, _call_context(context), method, request)
end

function _server_streaming_handler(service::Flight.Service, method::Flight.MethodDescriptor)
    return (context, request, stream) -> begin
        response = Channel{method.response_type}(STREAM_BUFFER_SIZE)
        task = @async begin
            try
                if method.handler_field === :listactions
                    Flight.listactions(service, _call_context(context), response)
                else
                    Flight.dispatch(service, _call_context(context), method, request, response)
                end
            finally
                close(response)
            end
        end
        try
            _drain_response!(stream, response)
            _streaming_handler_result(task)
            gRPCServer.close!(stream)
        finally
            istaskdone(task) || wait(task)
        end
    end
end

function _client_streaming_handler(service::Flight.Service, method::Flight.MethodDescriptor)
    return (context, stream) -> begin
        request = Channel{method.request_type}(STREAM_BUFFER_SIZE)
        producer = @async begin
            try
                for message in stream
                    put!(request, message)
                end
            finally
                close(request)
            end
        end
        task = @async Flight.dispatch(service, _call_context(context), method, request)
        try
            return fetch(task)
        finally
            _streaming_handler_result(task, producer)
        end
    end
end

function _bidi_streaming_handler(service::Flight.Service, method::Flight.MethodDescriptor)
    return (context, stream) -> begin
        request = Channel{method.request_type}(STREAM_BUFFER_SIZE)
        response = Channel{method.response_type}(STREAM_BUFFER_SIZE)
        producer = @async begin
            try
                for message in stream
                    put!(request, message)
                end
            finally
                close(request)
            end
        end
        task = @async begin
            try
                Flight.dispatch(service, _call_context(context), method, request, response)
            finally
                close(response)
            end
        end
        try
            for message in response
                gRPCServer.send!(stream, message)
            end
            _streaming_handler_result(task, producer)
            gRPCServer.close!(stream)
        finally
            istaskdone(task) || wait(task)
            isnothing(producer) || (istaskdone(producer) || wait(producer))
        end
        return nothing
    end
end

function _handler(service::Flight.Service, method::Flight.MethodDescriptor)
    if !method.request_streaming && !method.response_streaming
        return _unary_handler(service, method)
    elseif !method.request_streaming && method.response_streaming
        return _server_streaming_handler(service, method)
    elseif method.request_streaming && !method.response_streaming
        return _client_streaming_handler(service, method)
    end
    return _bidi_streaming_handler(service, method)
end
