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

# Precompiles the reads and writes of a DataFrame, so the first one in a session
# does not compile them. It adds no methods.
module ArrowDataFramesExt

import Arrow
import DataFrames
import Dates
import PooledArrays
import PrecompileTools

PrecompileTools.@setup_workload begin
    table = (
        id=Int64[1, 2, 3],
        x=Float64[1.5, 2.5, 3.5],
        y=Union{Missing,Float64}[1.5, missing, 3.5],
        name=["a", "a longer string than twelve bytes", "c"],
        note=Union{Missing,String}["a", missing, "c"],
        day=[Dates.Date(2025, 1, 1), Dates.Date(2025, 1, 2), Dates.Date(2025, 1, 3)],
        at=[
            Dates.DateTime(2025, 1, 1),
            Dates.DateTime(2025, 1, 2),
            Dates.DateTime(2025, 1, 3),
        ],
        flag=[true, false, true],
        pool=PooledArrays.PooledArray(["u", "v", "u"]),
    )
    PrecompileTools.@compile_workload begin
        bytes = take!(Arrow.write(IOBuffer(), table; file=true))
        arrowtable = Arrow.Table(bytes)
        df = DataFrames.DataFrame(arrowtable)
        DataFrames.DataFrame(arrowtable; copycols=false)
        Arrow.read(bytes, DataFrames.DataFrame)
        take!(Arrow.write(IOBuffer(), df; file=true))
        take!(Arrow.write(IOBuffer(), DataFrames.DataFrame(table); file=true))
    end
end

end # module ArrowDataFramesExt
