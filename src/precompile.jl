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

# Precompile workload: run the common write/read/scan shapes once during
# package precompilation so first use does not pay their compilation cost.
# Everything here is in-memory and deterministic — no files, no network.
# This file belongs to the top-level Arrow package only; the JuliaC trim gate
# includes src/ArrowCore.jl standalone and must not see PrecompileTools.
import PrecompileTools

PrecompileTools.@setup_workload begin
    columns = (
        ints=Int64[1, 2, 3],
        floats=Float64[1.0, 2.5, 3.5],
        strings=["a", "bb", "ccc"],
        bools=[true, false, true],
        maybe=Union{Missing,Int64}[1, missing, 3],
        lists=[Int64[1, 2], Int64[], Int64[3]],
        dates=[Dates.Date(2020, 1, 1), Dates.Date(2020, 1, 2), Dates.Date(2020, 1, 3)],
        stamps=[
            Dates.DateTime(2020, 1, 1, 12),
            Dates.DateTime(2020, 1, 2, 12),
            Dates.DateTime(2020, 1, 3, 12),
        ],
        pooled=DictEncode(["lo", "hi", "lo"]),
    )
    PrecompileTools.@compile_workload begin
        io = IOBuffer()
        write(io, columns)
        bytes = take!(seekstart(io))
        tbl = Table(bytes)
        for name in Tables.columnnames(tbl)
            foreach(identity, Tables.getcolumn(tbl, name))
        end
        Tables.columntable(tbl)
        Table(
            bytes;
            scan=Tables.Scan(
                select=(:ints, :floats),
                filter=Tables.col(:ints) > 1,
                limit=2,
            ),
        )
    end
end
