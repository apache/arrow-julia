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

using Sockets
using CodecZlib

# Keep the deliberately inconsistent headers on the wire unchanged by a server
# library. Both providers still use their real HTTP clients and signing paths.
function with_range_response(f, mode, data)
    listener = Sockets.listen(Sockets.IPv4("127.0.0.1"), 0)
    host = "http://127.0.0.1:$(Sockets.getsockname(listener)[2])"
    requests = Any[]
    request_lock = ReentrantLock()
    server = @async begin
        @sync while isopen(listener)
            socket = try
                Sockets.accept(listener)
            catch
                isopen(listener) && rethrow()
                break
            end
            @async try
                line = readline(socket)
                isempty(line) && return nothing
                method, target, _ = split(line; limit=3)
                headers = Dict{String,String}()
                while true
                    line = readline(socket)
                    isempty(line) && break
                    name, value = split(line, ':'; limit=2)
                    headers[lowercase(name)] = strip(value)
                end
                lock(request_lock) do
                    push!(requests, (; method, target, headers))
                end
                range = match(r"^bytes=(\d+)-(\d+)\z", headers["range"])
                lo, hi = parse.(Int, range.captures)
                width = hi - lo + 1
                shift =
                    mode == :wrong_offset ? 1 :
                    mode == :swapped_column &&
                    width >= 100000 &&
                    hi + width < length(data) ? width : 0
                body = data[(lo + shift + 1):(hi + shift + 1)]
                total = mode == :wrong_total ? length(data) + 1 : length(data)
                range_end = hi + shift + (mode == :wrong_end)
                content_range = "bytes $(lo + shift)-$range_end/$total"
                mode == :malformed_range && (content_range *= " garbage")
                mode == :unknown_total && (content_range = "bytes $lo-$hi/*")
                mode == :overflow && (content_range = "bytes $lo-$hi/$(big(2)^100)")
                status = 206
                encoding = ""
                tag =
                    mode == :changed_tag ? "\"v2\"" :
                    mode == :weak_tag ? "W/\"v1\"" : "\"v1\""
                if mode == :ignored
                    status, body = 200, data
                elseif mode == :short
                    body = body[1:(end - 1)]
                elseif mode == :long
                    body = vcat(body, 0xff)
                elseif mode == :encoded
                    body = transcode(GzipCompressor, body)
                    encoding = "gzip"
                elseif mode == :identity
                    encoding = "Identity"
                    content_range = replace(content_range, "bytes" => "Bytes")
                end
                response_headers = ["Connection" => "close"]
                mode == :missing_range ||
                    push!(response_headers, "Content-Range" => content_range)
                mode == :missing_tag || push!(response_headers, "ETag" => tag)
                isempty(encoding) ||
                    push!(response_headers, "Content-Encoding" => encoding)
                push!(
                    response_headers,
                    mode == :chunked ? "Transfer-Encoding" => "chunked" :
                    "Content-Length" => string(length(body)),
                )
                write(socket, "HTTP/1.1 $status fixture\r\n")
                for (name, value) in response_headers
                    write(socket, name, ": ", value, "\r\n")
                end
                write(socket, "\r\n")
                if mode == :chunked
                    write(socket, string(length(body); base=16), "\r\n", body, "\r\n0\r\n\r\n")
                else
                    write(socket, body)
                end
            finally
                close(socket)
            end
        end
    end
    stores = (
        (
            CloudStore.S3.Bucket("ranges", "us-east-1"; host),
            CloudStore.AWS.Credentials("fixture", "fixture"),
        ),
        (
            CloudStore.Blobs.Container("ranges", "fixture"; host),
            CloudStore.Azure.Credentials("fixture", "Zml4dHVyZQ=="),
        ),
    )
    try
        return f(stores, requests)
    finally
        close(listener)
        wait(server)
    end
end

@testset "Cloud range response integrity" begin
    ext = Base.get_extension(Arrow, :ArrowCloudStoreExt)
    data = collect(codeunits("abcdefghijkl"))
    for mode in (:valid, :identity, :chunked)
        with_range_response(mode, data) do stores, requests
            for (store, credentials) in stores
                source = ext.CloudObjectSource(
                    CloudStore.Object(store, credentials, "data", length(data), "v1"),
                )
                @test Arrow.readrange(source, 4, 4) == data[5:8]
                @test Arrow.readrange(source, 0, length(data)) == data
                before = length(requests)
                @test Arrow.readrange(source, 0, 0) == UInt8[]
                @test Arrow.readrange(source, length(data), 0) == UInt8[]
                @test length(requests) == before
            end
            @test all(r -> r.headers["if-match"] == "\"v1\"", requests)
            @test all(r -> get(r.headers, "accept-encoding", "") == "identity", requests)
        end
    end
    for mode in (
        :wrong_offset,
        :wrong_end,
        :wrong_total,
        :missing_range,
        :malformed_range,
        :unknown_total,
        :overflow,
        :ignored,
        :missing_tag,
        :changed_tag,
        :weak_tag,
        :short,
        :long,
        :encoded,
    )
        with_range_response(mode, data) do stores, requests
            for (store, credentials) in stores
                source = ext.CloudObjectSource(
                    CloudStore.Object(store, credentials, "data", length(data), "v1"),
                )
                @test_throws Arrow.ValidationError Arrow.readrange(source, 4, 4)
                # A 200 response is also rejected when a Range requests the whole object.
                if mode == :ignored
                    @test_throws Arrow.ValidationError Arrow.readrange(
                        source,
                        0,
                        length(data),
                    )
                end
            end
        end
    end
end

@testset "Cloud scans reject bytes from another column" begin
    expected = collect(Int64, 1:30000)
    other = fill(Int64(-7), length(expected))
    io = IOBuffer()
    Arrow.write(io, (a=expected, b=other); file=true)
    data = take!(io)
    for mode in (:valid, :swapped_column, :encoded)
        with_range_response(mode, data) do stores, requests
            for (store, credentials) in stores
                object = CloudStore.Object(store, credentials, "data", length(data), "v1")
                if mode == :valid
                    @test collect(Arrow.Table(object; scan=Tables.Scan(select=(:a,))).a) ==
                          expected
                else
                    @test_throws Arrow.ValidationError Arrow.Table(
                        object;
                        scan=Tables.Scan(select=(:a,)),
                    )
                end
            end
        end
    end
end
