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

# The CloudStore.jl extension against CloudBase's local S3 and Azure harnesses:
# a `CloudStore.Object` is wrapped as an `Arrow.AbstractArrowSource`, so
# `Arrow.Table(obj; scan=…)` reads through version-pinned HTTP Range requests,
# and the whole-object paths agree with in-memory reads.

module CloudStoreTests

using Test
using Tables
using Arrow
using CloudStore
import CloudBase
using CloudBase.CloudTest: Minio, Azurite

mutable struct MetadataStore <: CloudBase.AbstractStore
    baseurl::String
    headers::Vector{Pair{String,String}}
    requests::Int
end

MetadataStore(headers) = MetadataStore("https://metadata.example.invalid/", headers, 0)

function CloudStore.API.headObject(store::MetadataStore, url, headers; kw...)
    store.requests += 1
    return CloudStore.API.HTTP.Response(200, store.headers)
end

@testset "Cloud source metadata" begin
    ext = Base.get_extension(Arrow, :ArrowCloudStoreExt)
    store = MetadataStore(["Content-Length" => "12", "ETag" => "\"v1\""])
    object = CloudStore.Object(store, nothing, "object", 12, "")
    source = ext.CloudObjectSource(object)
    @test Arrow.sourcelength(source) == 12
    @test store.requests == 1
    @test Arrow.readrange(source, 0, 0) == UInt8[]
    @test store.requests == 1
    complete = CloudStore.Object(store, nothing, "object", 12, "v1")
    @test Arrow.sourcelength(ext.CloudObjectSource(complete)) == 12
    @test store.requests == 1

    for headers in (
        ["Content-Length" => "12"],
        ["Content-Length" => "12", "ETag" => ""],
        ["Content-Length" => "12", "ETag" => "W/\"v1\""],
        ["Content-Length" => "12", "ETag" => "\"bad tag\""],
        ["Content-Length" => "12", "ETag" => "\"bad\x7ftag\""],
        ["Content-Length" => "13", "ETag" => "\"v1\""],
        ["ETag" => "\"v1\""],
    )
        incomplete = MetadataStore(headers)
        object = CloudStore.Object(incomplete, nothing, "object", 12, "")
        @test_throws ArgumentError ext.CloudObjectSource(object)
        @test incomplete.requests == 1
    end
    @test_throws ArgumentError ext.CloudObjectSource(
        CloudStore.Object(store, nothing, "object", 12, "W/\"v1\""),
    )
    @test_throws ArgumentError ext.CloudObjectSource(
        CloudStore.Object(store, nothing, "object", -1, "v1"),
    )
    @test store.requests == 1

    empty_store = MetadataStore(["Content-Length" => "0", "ETag" => "\"empty\""])
    empty_source =
        ext.CloudObjectSource(CloudStore.Object(empty_store, nothing, "empty", 0, ""))
    @test Arrow.sourcelength(empty_source) == 0
    @test Arrow.readrange(empty_source, 0, 0) == UInt8[]
    @test empty_store.requests == 1
end

@testset "Cloud source pins missing ETags before the first range" begin
    ext = Base.get_extension(Arrow, :ArrowCloudStoreExt)
    for emulator in (Minio, Azurite)
        emulator.with() do conf
            credentials, store = conf.credentials, conf.store
            original = collect(codeunits("abcdefghijkl"))
            replacement = collect(codeunits("ABCDEFGHIJKL"))
            CloudStore.put(store, "snapshot.bin", original; credentials)
            object =
                CloudStore.Object(store, credentials, "snapshot.bin", length(original), "")
            source = ext.CloudObjectSource(object)
            unread = ext.CloudObjectSource(object)
            @test Arrow.readrange(source, 0, 4) == original[1:4]

            CloudStore.put(store, "snapshot.bin", replacement; credentials)
            for (stale, offset) in ((source, 4), (unread, 0))
                failure = try
                    Arrow.readrange(stale, offset, 4)
                    nothing
                catch err
                    err
                end
                @test failure isa CloudStore.API.HTTP.StatusError &&
                      failure.response.status == 412
            end
            fresh = ext.CloudObjectSource(object)
            @test Arrow.readrange(fresh, 0, length(replacement)) == replacement
            wrong_size = CloudStore.Object(
                store,
                credentials,
                "snapshot.bin",
                length(original) + 1,
                "",
            )
            @test_throws ArgumentError ext.CloudObjectSource(wrong_size)

            CloudStore.put(store, "empty.bin", UInt8[]; credentials)
            empty_source = ext.CloudObjectSource(
                CloudStore.Object(store, credentials, "empty.bin", 0, ""),
            )
            @test Arrow.sourcelength(empty_source) == 0
            @test Arrow.readrange(empty_source, 0, 0) == UInt8[]
        end
    end
end

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
        # one range, byte-exact against the object; a bounded concurrency
        src = ext.CloudObjectSource(obj)
        @test Arrow.readrange(src, 8, 16) == filebytes[9:24]
        @test Arrow.readrange(src, length(filebytes) - 6, 6) == filebytes[(end - 5):end]
        @test Arrow.readrange(src, 0, 0) == UInt8[]
        @test Arrow.concurrentreads(src) == ext.CONCURRENT_RANGE_READS > 1
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
        # A handle is pinned to the object version it was made from: after the
        # key is overwritten, its next range read fails (If-Match) rather than
        # mixing bytes of two versions across request rounds.
        stale = ext.CloudObjectSource(obj)
        CloudStore.put(bucket, "t.arrow", streambytes; credentials=credentials)
        @test_throws Exception Arrow.readrange(stale, 8, 16)
        @test_throws Exception Arrow.Table(obj; scan=Tables.Scan(select=(:a,)))
        fresh = CloudStore.Object(bucket, "t.arrow"; credentials=credentials)
        @test Arrow.Table(fresh).a == mem.a
    end
end

end # module
