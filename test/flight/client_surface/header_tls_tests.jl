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

function flight_client_surface_test_header_tls_helpers(fixture)
    client = fixture.client

    tls_client = Arrow.Flight.Client(
        "grpc+tls://secure.example:9443";
        tls_root_certs="/tmp/root.pem",
        cert_chain="/tmp/client.pem",
        private_key="/tmp/client.key",
        key_password="secret",
        disable_server_verification=true,
    )
    @test tls_client.tls_root_certs == "/tmp/root.pem"
    @test tls_client.cert_chain == "/tmp/client.pem"
    @test tls_client.private_key == "/tmp/client.key"
    @test tls_client.key_password == "secret"
    @test tls_client.disable_server_verification

    header_client = Arrow.Flight.withheaders(
        client,
        "authorization" => "Bearer token1234",
        "x-trace-id" => "trace-1",
    )
    @test header_client.headers ==
          ["authorization" => "Bearer token1234", "x-trace-id" => "trace-1"]
    @test header_client.host == client.host
    @test header_client.grpc === client.grpc
    @test header_client.disable_server_verification == client.disable_server_verification

    binary_header_client =
        Arrow.Flight.withheaders(client, "auth-token-bin" => UInt8[0x00, 0xff, 0x41])
    @test binary_header_client.headers == ["auth-token-bin" => UInt8[0x00, 0xff, 0x41]]
    @test Arrow.Flight._header_lines(binary_header_client.headers) ==
          ["auth-token-bin: AP9B"]

    token_client = Arrow.Flight.withtoken(client, UInt8[0x01, 0x02])
    @test token_client.headers == ["auth-token-bin" => UInt8[0x01, 0x02]]
    @test Arrow.Flight._header_lines(token_client.headers) == ["auth-token-bin: AQI="]

    invalid_binary_header_client =
        Arrow.Flight.withheaders(client, "x-binary" => UInt8[0x00])
    @test_throws ArgumentError Arrow.Flight._header_lines(
        invalid_binary_header_client.headers,
    )
end
