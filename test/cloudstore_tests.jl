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

# The CloudStore.jl extension end to end against a local S3-compatible
# server (CloudBase's Minio harness): a `CloudStore.Object` is an
# `Arrow.AbstractArrowSource`, so `Arrow.Table(obj; scan=…)` reads through
# HTTP Range requests, and the whole-object paths agree with in-memory reads.

module CloudStoreTests

using Test
using Tables
using Arrow
using CloudStore
using CloudBase.CloudTest: Minio

@testset "CloudStore extension" begin
    ext = Base.get_extension(Arrow, :ArrowCloudStoreExt)
    @test ext !== nothing
    part1 = (a=collect(Int64, 1:1000), b=["v$i" for i = 1:1000], c=rand(1000))
    part2 = (a=collect(Int64, 1001:2000), b=["v$i" for i = 1001:2000], c=rand(1000))
    fio = IOBuffer()
    Arrow.write(fio, Tables.partitioner([part1, part2]))
    filebytes = take!(fio)
    sio = IOBuffer()
    Arrow.write(sio, Tables.partitioner([part1, part2]); file=false)
    streambytes = take!(sio)
    Minio.with() do conf
        credentials, bucket = conf.credentials, conf.store
        CloudStore.put(bucket, "t.arrow", filebytes; credentials=credentials)
        CloudStore.put(bucket, "t.arrows", streambytes; credentials=credentials)
        obj = CloudStore.Object(bucket, "t.arrow"; credentials=credentials)
        @test Arrow.sourcelength(ext.CloudObjectSource(obj)) == length(filebytes)
        # one range, and the batched form, byte-exact against the object
        src = ext.CloudObjectSource(obj)
        @test Arrow.readrange(src, 8, 16) == filebytes[9:24]
        @test Arrow.readranges(
            src,
            NTuple{2,Int64}[(0, 6), (100, 50), (length(filebytes) - 6, 6)],
        ) == [filebytes[1:6], filebytes[101:150], filebytes[(end - 5):end]]
        # a projected, windowed scan over the object
        t = Arrow.Table(obj; scan=Tables.Scan(select=(:b,), limit=3, offset=1500))
        @test Tables.columnnames(t) == [:b]
        @test t.b == ["v1501", "v1502", "v1503"]
        # a filter that the footer statistics can prune to the second batch
        t2 = Arrow.Table(obj; scan=Tables.Scan(select=(:a,), filter=Tables.col(:a) > 1990))
        @test t2.a == collect(1991:2000)
        # whole-object reads agree with the in-memory reads
        full = Arrow.Table(obj)
        mem = Arrow.Table(filebytes)
        @test Tables.columnnames(full) == Tables.columnnames(mem)
        @test full.a == mem.a && full.b == mem.b && full.c == mem.c
        sobj = CloudStore.Object(bucket, "t.arrows"; credentials=credentials)
        st = Arrow.Table(sobj)
        @test st.a == mem.a && st.b == mem.b
        @test [length(batch.a) for batch in Arrow.Stream(sobj)] == [1000, 1000]
        @test [length(batch.a) for batch in Arrow.Stream(obj)] == [1000, 1000]
    end
end

end # module
