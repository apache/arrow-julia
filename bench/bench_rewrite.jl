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

# Arrow.jl 3.0 (this repo) serialize/deserialize timing. Emits one JSON
# object per line: {"impl","workload","op","seconds","bytes"}.
# Usage: julia --project=. bench/bench_rewrite.jl <outdir>

using Arrow, Tables
include(joinpath(@__DIR__, "workloads.jl"))

function main(outdir::String)
    for (name, make) in BENCH_WORKLOADS
        tbl = make()
        path = joinpath(outdir, "rewrite-$name.arrow")
        # write: table -> file bytes on disk
        twrite = bench_time(() -> Arrow.write(path, tbl))
        sz = filesize(path)
        # read: file -> fully materialized public columns
        tread = bench_time() do
            t = Arrow.Table(path)
            for nm in Tables.columnnames(t)
                length(Tables.getcolumn(t, nm))
            end
            Arrow.release!(t)
        end
        for (op, secs) in (("write", twrite), ("read", tread))
            println(
                "{\"impl\":\"rewrite\",\"workload\":\"$name\"," *
                "\"op\":\"$op\",\"seconds\":$secs,\"bytes\":$sz}",
            )
        end
    end
end

main(ARGS[1])
