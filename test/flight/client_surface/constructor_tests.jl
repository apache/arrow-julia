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

function flight_client_surface_test_constructors(fixture)
    client = fixture.client

    @test client.host == "localhost"
    @test client.port == 8815
    @test client.secure
    @test client.deadline == 30.0
    @test client.keepalive == 15.0
    @test client.max_send_message_length == 1024
    @test client.max_recieve_message_length == 2048
    @test isempty(client.headers)
    @test isnothing(client.tls_root_certs)
    @test isnothing(client.cert_chain)
    @test isnothing(client.private_key)
    @test isnothing(client.key_password)
    @test !client.disable_server_verification

    uri_client = Arrow.Flight.Client("grpc://127.0.0.1:31337")
    @test uri_client.host == "127.0.0.1"
    @test uri_client.port == 31337
    @test !uri_client.secure

    tls_client = Arrow.Flight.Client("grpc+tls://example.com:9443")
    @test tls_client.host == "example.com"
    @test tls_client.port == 9443
    @test tls_client.secure

    location_client =
        Arrow.Flight.Client(fixture.protocol.Location("https://demo.example:8443"))
    @test location_client.host == "demo.example"
    @test location_client.port == 8443
    @test location_client.secure

    @test_throws ArgumentError Arrow.Flight.Client("grpc://missing-port")
end
