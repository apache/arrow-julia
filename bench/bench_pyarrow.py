# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# PyArrow serialize/deserialize timing over the same logical workloads.
# Reads are pyarrow-idiomatic (memory-mapped read_all): pyarrow defers
# per-element materialization, so its read numbers measure wrapping, not
# element conversion — the report states this asymmetry.
# Usage: python3 bench_pyarrow.py <outdir>

import sys, time, os
import pyarrow as pa
import pyarrow.ipc as ipc

ROWS_PRIMITIVE = 10_000_000
ROWS_STRINGS = 2_000_000
ROWS_LISTS = 1_000_000
ROWS_DICT = 2_000_000


def wl_primitive():
    n = ROWS_PRIMITIVE
    return pa.table({
        "a": pa.array(range(1, n + 1), type=pa.int64()),
        "b": pa.array((float(i) for i in range(1, n + 1)),
                      type=pa.float64(), size=n),
    })


def wl_nullable():
    n = ROWS_PRIMITIVE
    return pa.table({
        "a": pa.array((None if i % 7 == 0 else i for i in range(1, n + 1)),
                      type=pa.int64(), size=n),
    })


def wl_strings():
    n = ROWS_STRINGS
    return pa.table({"s": pa.array("value-%d" % (i % 1000)
                                   for i in range(1, n + 1))})


def wl_lists():
    n = ROWS_LISTS
    return pa.table({"l": pa.array([[i, i + 1, i + 2]
                                    for i in range(1, n + 1)],
                                   type=pa.list_(pa.int64()))})


def wl_dictpool():
    # Plain strings: dictionary_encode runs INSIDE the write timer so all
    # three legs time pool construction + dictionary write.
    n = ROWS_DICT
    return pa.table({"d": pa.array("cat-%d" % (i % 32)
                                   for i in range(1, n + 1))})


WORKLOADS = [
    ("primitive", wl_primitive),
    ("nullable", wl_nullable),
    ("strings", wl_strings),
    ("lists", wl_lists),
    ("dictpool", wl_dictpool),
]


def bench(f, runs=3):
    f()
    ts = []
    for _ in range(runs):
        t0 = time.perf_counter()
        f()
        ts.append(time.perf_counter() - t0)
    ts.sort()
    return ts[len(ts) // 2]


def main(outdir):
    for name, make in WORKLOADS:
        tbl = make()
        path = os.path.join(outdir, "pyarrow-%s.arrow" % name)

        def write():
            out = tbl
            if name == "dictpool":
                out = pa.table({"d": tbl["d"].combine_chunks()
                                .dictionary_encode()})
            with ipc.new_file(path, out.schema) as w:
                w.write_table(out)

        twrite = bench(write)
        size = os.path.getsize(path)

        def read():
            with pa.memory_map(path) as src:
                ipc.open_file(src).read_all()

        tread = bench(read)
        for op, secs in (("write", twrite), ("read", tread)):
            print('{"impl":"pyarrow","workload":"%s","op":"%s",'
                  '"seconds":%r,"bytes":%d}' % (name, op, secs, size))


main(sys.argv[1])
