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

module StreamLifetimeTests

using Test, Arrow, Tables

function poolref(stream)
    table, state = iterate(stream)
    @test collect(table.category) == ["first"]
    pool = only(keys(getfield(stream, :views).pools))
    return WeakRef(pool), state
end

@testset "streams release obsolete dictionary caches" begin
    io = IOBuffer()
    Arrow.Writer(io; dictreplacement=true) do writer
        Arrow.write(writer, (category=Arrow.DictEncode(["first"]),))
        Arrow.write(writer, (category=Arrow.DictEncode(["second"]),))
        Arrow.write(writer, (category=Arrow.DictEncode(["second"]),))
    end
    bytes = take!(io)
    for scan in (nothing, Tables.Scan(select=(:category,)))
        stream = Arrow.Stream(IOBuffer(bytes); scan)
        oldpool, state = poolref(stream)
        second, state = iterate(stream, state)
        third, state = iterate(stream, state)
        @test collect(second.category) == collect(third.category) == ["second"]
        GC.gc(true)
        GC.gc(true)
        @test oldpool.value === nothing
        @test iterate(stream, state) === nothing
    end

    stream = Arrow.Stream(IOBuffer(bytes))
    first, state = iterate(stream)
    second, _ = iterate(stream, state)
    GC.gc(true)
    @test collect(first.category) == ["first"]
    @test collect(second.category) == ["second"]
end

end
