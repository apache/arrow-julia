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

# Guard-page pin for the bounded C-string reader: map
# EXACTLY the scan limit of readable non-NUL bytes with a PROT_NONE page
# immediately after. The reader must refuse with ValidationError; touching
# byte limit+1 would SIGBUS this child instead.
using Arrow
const AC = Arrow.ArrowCore

const PROT_READ = Cint(1)
const PROT_WRITE = Cint(2)
const PROT_NONE = Cint(0)
const MAP_PRIVATE = Cint(0x0002)
const MAP_ANON = Sys.isapple() ? Cint(0x1000) : Cint(0x20)

limit = Int(Arrow.CSTRING_SCAN_LIMIT)
page = Int(ccall(:getpagesize, Cint, ()))
total = limit + page
p = ccall(
    :mmap,
    Ptr{UInt8},
    (Ptr{Cvoid}, Csize_t, Cint, Cint, Cint, Int64),
    C_NULL,
    total,
    PROT_READ | PROT_WRITE,
    MAP_PRIVATE | MAP_ANON,
    -1,
    0,
)
p == Ptr{UInt8}(-1) && error("mmap failed")
for i = 1:limit
    unsafe_store!(p, 0x41, i)
end
rc = ccall(:mprotect, Cint, (Ptr{Cvoid}, Csize_t, Cint), p + limit, page, PROT_NONE)
rc == 0 || error("mprotect failed")

caught = try
    Arrow._import_cstring(p, "guard probe")
    false
catch e
    e isa AC.ValidationError
end
caught || error("bounded C-string read did not refuse at the scan limit")

# A NUL just inside the window still reads cleanly (maximum payload).
unsafe_store!(p, 0x00, limit)
s = Arrow._import_cstring(p, "max payload")
length(s) == limit - 1 || error("maximum payload length wrong")
println("cstring guard page ok")
