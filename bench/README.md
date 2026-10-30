<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# IPC throughput benchmarks

Run the package and PyArrow against deterministic primitive, nullable, string,
list, and dictionary columns:

```sh
julia --project=. -t 1 bench/run.jl /tmp/arrow-bench-1
julia --project=. -t 8 bench/run.jl /tmp/arrow-bench-8
```

The driver uses a host `python3` with PyArrow installed. Set `ARROW_BENCH_PYTHON`
to a different Python executable if needed. If host PyArrow is unavailable, it
uses the image built by `julia conformance/run.jl`, or reports that the Python leg
was skipped. A Docker fallback is useful for checking the harness; its timings
include different filesystem and CPU scheduling costs from the host Julia run.

Both readers use the **same Julia-written file**, memory mapped, and request all
columns. Arrow.jl validates buffers and constructs its DataStrings columns on
first access. Numeric and nested columns retain views. PyArrow's `read_all`
returns its native Arrow arrays, usually without further construction. These
measure the cost of making columns available, not iterating every value or
copying them into mutable Julia/Python objects. The driver prints this distinction
with every report. Both legs check the complete logical values outside the timing.

Writes start from equivalent in-memory data, use batches of at most 1,048,576
rows, and write uncompressed IPC files. Dictionary construction is timed in both
implementations. Each repetition writes a fresh destination; deletion of the
previous output happens before the timer. This does not measure the additional
cost of Arrow.jl's atomic replacement of an existing file.

Each operation warms up once, then reports the median of three repetitions.
The Julia process and PyArrow CPU pool use the driver's thread count. Filesystem
caches are warm. File size divided by elapsed time gives the displayed MB/s;
writer output sizes can differ because physical layout choices differ.

Use a quiet machine, alternate thread counts, and repeat runs before drawing
conclusions from small timing differences. This harness does not measure peak
memory, first-use compilation, compressed IO, or predicate pushdown. It is a
small throughput check, not evidence that every workload is faster than another
implementation.

For a quick correctness check of the harness, scale every workload down:

```sh
ARROW_BENCH_SCALE=0.001 julia --project=. bench/run.jl /tmp/arrow-bench-smoke
```

`ARROW_BENCH_SCALE` must be positive and finite. Keep it at the same value for
all compared runs; the default is 1 (10M primitive rows, 2M strings, 1M lists,
2M dictionary rows). The work directory retains both implementations' files and
JSONL results for inspection.
