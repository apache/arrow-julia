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

# Arrow.jl 2.x serialize/deserialize timing (run under bench/env2x).
# Same protocol as bench_rewrite.jl. 2.x reads are its idiomatic lazy
# wrap plus ONE top-level copy() per column — nested list elements stay
# Arrow-backed views, so this is NOT full materialization; the driver
# prints that caveat with every report.
# Usage: julia --project=bench/env2x bench/bench_2x.jl <outdir>

using Arrow, Tables
include(joinpath(@__DIR__, "workloads.jl"))

function main(outdir::String)
    for (name, make) in BENCH_WORKLOADS
        tbl = make()
        path = joinpath(outdir, "arrow2x-$name.arrow")
        twrite = bench_time(() -> Arrow.write(path, tbl))
        sz = filesize(path)
        tread = bench_time() do
            t = Arrow.Table(path)
            for nm in Tables.columnnames(t)
                length(copy(Tables.getcolumn(t, nm)))
            end
        end
        for (op, secs) in (("write", twrite), ("read", tread))
            println("{\"impl\":\"arrow2x\",\"workload\":\"$name\"," *
                    "\"op\":\"$op\",\"seconds\":$secs,\"bytes\":$sz}")
        end
    end
end

main(ARGS[1])
