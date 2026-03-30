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

#!/usr/bin/env python3

import signal

import pyarrow.flight as fl


class TokenAuthHandler(fl.ServerAuthHandler):
    def authenticate(self, outgoing, incoming):
        username = incoming.read()
        password = incoming.read()
        if username == b"test" and password == b"p4ssw0rd":
            outgoing.write(b"secret:test")
            return
        raise fl.FlightUnauthenticatedError("invalid username/password")

    def is_valid(self, token):
        if token != b"secret:test":
            raise fl.FlightUnauthenticatedError("invalid token")
        return b"test"


class HandshakeFlightServer(fl.FlightServerBase):
    def __init__(self):
        super().__init__(
            location="grpc://127.0.0.1:0",
            auth_handler=TokenAuthHandler(),
        )

    def list_actions(self, context):
        del context
        return [fl.ActionType("authenticated", "Requires a valid auth token")]


def main():
    server = HandshakeFlightServer()

    def shutdown_handler(signum, frame):
        del signum, frame
        server.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    print(server.port, flush=True)
    server.serve()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
