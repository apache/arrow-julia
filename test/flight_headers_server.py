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


def case_insensitive_header_lookup(headers, lookup_key):
    lookup_key = lookup_key.lower()
    for key, value in headers.items():
        if key.lower() == lookup_key:
            return value
    raise fl.FlightUnauthenticatedError(f"missing required header: {lookup_key}")


class HeaderEchoServerMiddlewareFactory(fl.ServerMiddlewareFactory):
    def start_call(self, info, headers):
        del info
        authorization = case_insensitive_header_lookup(headers, "authorization")
        return HeaderEchoServerMiddleware(authorization[0])


class HeaderEchoServerMiddleware(fl.ServerMiddleware):
    def __init__(self, authorization):
        self.authorization = authorization


class HeaderEchoFlightServer(fl.FlightServerBase):
    def __init__(self):
        super().__init__(
            location="grpc://127.0.0.1:0",
            middleware={"auth": HeaderEchoServerMiddlewareFactory()},
        )

    def list_actions(self, context):
        del context
        return [("echo-authorization", "Return the Authorization header")]

    def do_action(self, context, action):
        if action.type != "echo-authorization":
            raise KeyError(f"unsupported action: {action.type}")
        middleware = context.get_middleware("auth")
        if middleware is None:
            raise fl.FlightUnauthenticatedError("missing auth middleware")
        return [middleware.authorization.encode("utf-8")]


def main():
    server = HeaderEchoFlightServer()

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
