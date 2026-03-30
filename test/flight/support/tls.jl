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

function generate_test_tls_certificate(dir::AbstractString)
    openssl = Sys.which("openssl")
    isnothing(openssl) && return nothing

    config_path = joinpath(dir, "openssl.cnf")
    cert_path = joinpath(dir, "cert.pem")
    key_path = joinpath(dir, "key.pem")
    write(
        config_path,
        """
        [req]
        distinguished_name = dn
        x509_extensions = v3_req
        prompt = no

        [dn]
        CN = localhost

        [v3_req]
        subjectAltName = @alt_names

        [alt_names]
        DNS.1 = localhost
        IP.1 = 127.0.0.1
        """,
    )
    run(
        Cmd([
            openssl,
            "req",
            "-x509",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key_path,
            "-out",
            cert_path,
            "-days",
            "1",
            "-config",
            config_path,
            "-extensions",
            "v3_req",
        ]),
    )
    return cert_path, key_path
end
