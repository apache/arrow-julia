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

import pathlib
import signal
import sys
import tempfile
from concurrent import futures

import grpc
import pyarrow as pa
import grpc_tools
from grpc_tools import protoc


ROOT = pathlib.Path(__file__).resolve().parent.parent / "src" / "flight" / "proto"
PROTO = ROOT / "Flight.proto"
GRPC_TOOLS_PROTO = pathlib.Path(grpc_tools.__file__).resolve().parent / "_proto"


def load_proto_modules():
    out = pathlib.Path(tempfile.mkdtemp(prefix="flight_poll_proto_"))
    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{ROOT}",
            f"-I{GRPC_TOOLS_PROTO}",
            f"--python_out={out}",
            f"--grpc_python_out={out}",
            str(PROTO),
        ]
    )
    if result != 0:
        raise RuntimeError(f"protoc failed with exit code {result}")
    sys.path.insert(0, str(out))
    import Flight_pb2
    import Flight_pb2_grpc

    return Flight_pb2, Flight_pb2_grpc


def descriptor_key(descriptor):
    return tuple(descriptor.path)


def main():
    pb2, pb2_grpc = load_proto_modules()

    class PollFlightInfoServicer(pb2_grpc.FlightServiceServicer):
        def __init__(self, port):
            self.pb2 = pb2
            self.port = port
            self.schema_bytes = bytes(pa.schema([("id", pa.int64())]).serialize())

        def _descriptor(self, path):
            return self.pb2.FlightDescriptor(
                type=self.pb2.FlightDescriptor.PATH,
                path=list(path),
            )

        def _flight_info(self, path):
            endpoint = self.pb2.FlightEndpoint(
                ticket=self.pb2.Ticket(ticket=b"poll-ticket"),
                location=[self.pb2.Location(uri=f"grpc://127.0.0.1:{self.port}")],
            )
            return self.pb2.FlightInfo(
                schema=self.schema_bytes,
                flight_descriptor=self._descriptor(path),
                endpoint=[endpoint],
                total_records=1,
                total_bytes=8,
                ordered=True,
            )

        def PollFlightInfo(self, request, context):
            del context
            key = descriptor_key(request)
            if key == ("interop", "poll"):
                return self.pb2.PollInfo(
                    info=self._flight_info(key),
                    flight_descriptor=self._descriptor(("interop", "poll", "retry")),
                    progress=0.5,
                )
            if key == ("interop", "poll", "retry"):
                return self.pb2.PollInfo(
                    info=self._flight_info(("interop", "poll")),
                    progress=1.0,
                )
            raise KeyError(f"unsupported poll descriptor: {key}")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    port = server.add_insecure_port("127.0.0.1:0")
    pb2_grpc.add_FlightServiceServicer_to_server(PollFlightInfoServicer(port), server)

    def shutdown_handler(signum, frame):
        del signum, frame
        server.stop(grace=None)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    server.start()
    print(port, flush=True)
    server.wait_for_termination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
