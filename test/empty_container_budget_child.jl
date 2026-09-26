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

# Isolate the high-row-count acceptance gate so every case releases its heap
# before the parent suite continues.
# At 2,000,000 rows, charging each empty value as a nonempty Vector would
# exceed the default 256 MiB budget. Empty values must be charged as empty.

using Arrow

const AC = Arrow.AC
const N = 2_000_000

function roundtripempty(field, data)
    schema = AC.Schema([field])
    bytes = Arrow.writestream(schema, [AC.RecordBatch(schema, [data], N)])
    table = Arrow.Table(bytes)
    column = table.x
    @assert length(column) == N
    @assert isempty(first(column))
    @assert isempty(last(column))
    Arrow.release!(table)
    return nothing
end

function checkutf8()
    field, data = AC.fromjulia("x", fill("", N))
    roundtripempty(field, data)
end

function checkbinary()
    type = AC.BinaryType(false)
    field = AC.Field("x", type; nullable=false)
    data = AC.ArrayData(
        type,
        N,
        [AC.BufferSlice(), AC._databuffer(zeros(Int32, N + 1)), AC._databuffer(UInt8[])];
        nullcount=0,
    )
    roundtripempty(field, data)
end

function checklist()
    field, data = AC.fromjulia("x", fill(Int64[], N))
    roundtripempty(field, data)
end

function checkmap()
    keyfield, keydata = AC.fromjulia("key", String[])
    valuefield, valuedata = AC.fromjulia("value", Int64[])
    entriesfield = AC.Field(
        "entries",
        AC.StructType();
        nullable=false,
        children=[keyfield, valuefield],
    )
    entriesdata = AC.ArrayData(
        AC.StructType(),
        0,
        [AC.BufferSlice()];
        children=[keydata, valuedata],
        nullcount=0,
    )
    type = AC.MapType(false)
    field = AC.Field("x", type; nullable=false, children=[entriesfield])
    data = AC.ArrayData(
        type,
        N,
        [AC.BufferSlice(), AC._databuffer(zeros(Int32, N + 1))];
        children=[entriesdata],
        nullcount=0,
    )
    roundtripempty(field, data)
end

for check in (checkutf8, checkbinary, checklist, checkmap)
    check()
    GC.gc(true)
end

println("default allocation limit accepts compact empty containers ✓")
