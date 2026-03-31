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

import json
import os
import signal

import pyarrow as pa
import pyarrow.flight as fl


def normalize_component(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def descriptor_key(descriptor):
    if descriptor.descriptor_type != fl.DescriptorType.PATH:
        raise KeyError("only PATH descriptors are supported")
    return tuple(normalize_component(part) for part in descriptor.path)


def key_ticket(key):
    return fl.Ticket(json.dumps(list(key)).encode("utf-8"))


class TLSInteropFlightServer(fl.FlightServerBase):
    def __init__(self, cert_path, key_path):
        cert = open(cert_path, "rb").read()
        key = open(key_path, "rb").read()
        super().__init__(
            location="grpc+tls://127.0.0.1:0",
            tls_certificates=[fl.CertKeyPair(cert=cert, key=key)],
        )
        self._datasets = {
            ("interop", "tls", "download"): pa.table(
                {
                    "id": pa.array([31, 32, 33], type=pa.int64()),
                    "name": pa.array(["thirty-one", "thirty-two", "thirty-three"]),
                }
            )
        }

    def _descriptor(self, key):
        return fl.FlightDescriptor.for_path(*key)

    def _flight_info(self, key):
        table = self._datasets[key]
        endpoint = fl.FlightEndpoint(
            key_ticket(key),
            [fl.Location.for_grpc_tls("localhost", self.port)],
        )
        return fl.FlightInfo(
            table.schema,
            self._descriptor(key),
            [endpoint],
            total_records=table.num_rows,
            total_bytes=table.nbytes,
        )

    def get_flight_info(self, context, descriptor):
        del context
        return self._flight_info(descriptor_key(descriptor))

    def get_schema(self, context, descriptor):
        del context
        return fl.SchemaResult(self._datasets[descriptor_key(descriptor)].schema)

    def do_get(self, context, ticket):
        del context
        key = tuple(normalize_component(part) for part in json.loads(ticket.ticket.decode("utf-8")))
        table = self._datasets[key]
        return fl.GeneratorStream(table.schema, iter(table.to_batches(max_chunksize=2)))


def main():
    cert_path = os.environ["ARROW_FLIGHT_TLS_CERT"]
    key_path = os.environ["ARROW_FLIGHT_TLS_KEY"]
    server = TLSInteropFlightServer(cert_path, key_path)

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
