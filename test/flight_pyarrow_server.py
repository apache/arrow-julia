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
import signal
import sys

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


def ticket_key(ticket):
    return tuple(normalize_component(part) for part in json.loads(ticket.ticket.decode("utf-8")))


def key_ticket(key):
    return fl.Ticket(json.dumps(list(key)).encode("utf-8"))


class InteropFlightServer(fl.FlightServerBase):
    def __init__(self):
        super().__init__(location="grpc://127.0.0.1:0")
        self._datasets = {
            ("interop", "download"): pa.table(
                {
                    "id": pa.array([1, 2, 3], type=pa.int64()),
                    "name": pa.array(["one", "two", "three"]),
                }
            )
        }

    def _descriptor(self, key):
        return fl.FlightDescriptor.for_path(*key)

    def _flight_info(self, key):
        table = self._datasets[key]
        endpoint = fl.FlightEndpoint(
            key_ticket(key),
            [fl.Location.for_grpc_tcp("127.0.0.1", self.port)],
        )
        return fl.FlightInfo(
            table.schema,
            self._descriptor(key),
            [endpoint],
            total_records=table.num_rows,
            total_bytes=table.nbytes,
        )

    def list_flights(self, context, criteria):
        del context, criteria
        for key in sorted(self._datasets):
            yield self._flight_info(key)

    def get_flight_info(self, context, descriptor):
        del context
        return self._flight_info(descriptor_key(descriptor))

    def get_schema(self, context, descriptor):
        del context
        return fl.SchemaResult(self._datasets[descriptor_key(descriptor)].schema)

    def do_get(self, context, ticket):
        del context
        table = self._datasets[ticket_key(ticket)]
        return fl.GeneratorStream(table.schema, iter(table.to_batches(max_chunksize=2)))

    def do_put(self, context, descriptor, reader, writer):
        del context
        self._datasets[descriptor_key(descriptor)] = reader.read_all()
        writer.write(b"stored")

    def do_exchange(self, context, descriptor, reader, writer):
        del context
        key = descriptor_key(descriptor)
        if key != ("interop", "exchange"):
            raise KeyError(f"unsupported exchange descriptor: {key}")

        writer.begin(reader.schema)
        batch_index = 0
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            if chunk.data is None:
                continue
            metadata = chunk.app_metadata
            if metadata is None:
                metadata = pa.py_buffer(f"exchange:{batch_index}".encode("utf-8"))
            writer.write_with_metadata(chunk.data, metadata)
            batch_index += 1

    def list_actions(self, context):
        del context
        return [("ping", "Return a fixed pong payload")]

    def do_action(self, context, action):
        del context
        if action.type != "ping":
            raise KeyError(f"unsupported action: {action.type}")
        return [b"pong"]


def main():
    server = InteropFlightServer()

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
