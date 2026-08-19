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

# Shared workload definitions for the serialize/deserialize benchmarks.
# Deterministic arithmetic data — every implementation
# builds the same logical tables, so file sizes and work agree.

const BENCH_ROWS_PRIMITIVE = 10_000_000
const BENCH_ROWS_STRINGS = 2_000_000
const BENCH_ROWS_LISTS = 1_000_000
const BENCH_ROWS_DICT = 2_000_000

function workload_primitive()
    n = BENCH_ROWS_PRIMITIVE
    return (a=collect(Int64, 1:n), b=collect(Float64, 1:n))
end

function workload_nullable()
    n = BENCH_ROWS_PRIMITIVE
    a = Vector{Union{Missing,Int64}}(undef, n)
    for i = 1:n
        a[i] = i % 7 == 0 ? missing : Int64(i)
    end
    return (a=a,)
end

function workload_strings()
    n = BENCH_ROWS_STRINGS
    return (s=[string("value-", i % 1000) for i = 1:n],)
end

function workload_lists()
    n = BENCH_ROWS_LISTS
    return (l=[Int64[i, i + 1, i + 2] for i = 1:n],)
end

function workload_dictpool()
    # DictEncode exists under the same name in 2.x and 3.0: both legs time
    # pool construction + dictionary write from plain strings, matching the
    # PyArrow leg's timed dictionary_encode + write.
    n = BENCH_ROWS_DICT
    return (d=Arrow.DictEncode([string("cat-", i % 32) for i = 1:n]),)
end

const BENCH_WORKLOADS = (
    ("primitive", workload_primitive),
    ("nullable", workload_nullable),
    ("strings", workload_strings),
    ("lists", workload_lists),
    ("dictpool", workload_dictpool),
)

"Median-of-k timing after one warmup run."
function bench_time(f::F; runs::Int=3) where {F}
    f()
    times = Float64[]
    for _ = 1:runs
        t0 = time_ns()
        f()
        push!(times, (time_ns() - t0) / 1e9)
    end
    return sort!(times)[cld(length(times), 2)]
end
