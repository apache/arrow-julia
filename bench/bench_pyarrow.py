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
# Both readers use the Julia-written files. read_all returns Arrow arrays;
# neither leg times scalar iteration or conversion to Python objects.
# Usage: bench/run.jl chooses a local Python or the conformance image.

import sys, time, os, math
import pyarrow as pa
import pyarrow.ipc as ipc

SCALE = float(os.environ.get("ARROW_BENCH_SCALE", "1"))
if not math.isfinite(SCALE) or SCALE <= 0:
    raise ValueError("ARROW_BENCH_SCALE must be positive and finite")
ROWS_PRIMITIVE = max(1, round(10_000_000 * SCALE))
ROWS_STRINGS = max(1, round(2_000_000 * SCALE))
ROWS_LISTS = max(1, round(1_000_000 * SCALE))
ROWS_DICT = max(1, round(2_000_000 * SCALE))
BATCHSIZE = 1_048_576
THREADS = int(sys.argv[2]) if len(sys.argv) > 2 else 1
pa.set_cpu_count(THREADS)


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
    # both legs time pool construction + dictionary write.
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


def bench(f, runs=3, setup=lambda: None):
    setup()
    f()
    ts = []
    for _ in range(runs):
        setup()
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
                w.write_table(out, max_chunksize=BATCHSIZE)

        def fresh():
            if os.path.exists(path):
                os.remove(path)

        twrite = bench(write, setup=fresh)
        size = os.path.getsize(path)
        inputpath = os.path.join(outdir, "rewrite-%s.arrow" % name)

        def read():
            with pa.memory_map(inputpath) as src:
                return ipc.open_file(
                    src, options=ipc.IpcReadOptions(use_threads=THREADS > 1)
                ).read_all()

        tread = bench(read)
        # Cast dictionary columns back to their plain-string input domain.
        if not read().cast(tbl.schema).equals(tbl):
            raise AssertionError("round-trip mismatch in %s" % name)
        for op, secs in (("write", twrite), ("read", tread)):
            nbytes = size if op == "write" else os.path.getsize(inputpath)
            print('{"impl":"pyarrow","workload":"%s","op":"%s",'
                  '"seconds":%r,"bytes":%d}' % (name, op, secs, nbytes))


main(sys.argv[1])
