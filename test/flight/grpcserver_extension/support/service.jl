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

function grpcserver_extension_service(protocol, fixture)
    return Arrow.Flight.Service(
        handshake=(ctx, request, response) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer native"
            incoming = collect(request)
            @test length(incoming) == 1
            put!(response, protocol.HandshakeResponse(UInt64(0), incoming[1].payload))
            close(response)
            return :handshake_ok
        end,
        getflightinfo=(ctx, req) -> begin
            @test req.path == fixture.descriptor.path
            return fixture.info
        end,
        getschema=(ctx, req) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer native"
            @test req.path == fixture.descriptor.path
            return protocol.SchemaResult(fixture.schema_bytes[5:end])
        end,
        doget=(ctx, req, response) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer native"
            @test req.ticket == fixture.ticket.ticket
            Arrow.Flight.putflightdata!(
                response,
                Tables.partitioner((
                    (id=Int64[1, 2], name=["one", "two"]),
                    (id=Int64[3], name=["three"]),
                ));
                descriptor=fixture.descriptor,
                metadata=fixture.dataset_metadata,
                colmetadata=fixture.dataset_colmetadata,
                close=true,
            )
            return :doget_ok
        end,
        listactions=(ctx, response) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer native"
            put!(response, protocol.ActionType("ping", "Ping action"))
            close(response)
            return :listactions_ok
        end,
        doaction=(ctx, action, response) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer native"
            @test action.var"#type" == "ping"
            put!(response, protocol.Result(b"pong"))
            close(response)
            return :doaction_ok
        end,
        doput=(ctx, request, response) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer native"
            incoming = collect(Arrow.Flight.stream(request))
            @test length(incoming) == 2
            @test incoming[1].id == [1, 2]
            @test incoming[1].name == ["one", "two"]
            @test Arrow.getmetadata(incoming[1])["dataset"] == "native"
            @test Arrow.getmetadata(incoming[1].name)["lang"] == "en"
            @test incoming[2].id == [3]
            @test incoming[2].name == ["three"]
            put!(response, protocol.PutResult(b"stored"))
            close(response)
            return :doput_ok
        end,
        doexchange=(ctx, request, response) -> begin
            @test Arrow.Flight.callheader(ctx, "authorization") == "Bearer native"
            Arrow.Flight.putflightdata!(response, Arrow.Flight.stream(request); close=true)
            return :doexchange_ok
        end,
    )
end
