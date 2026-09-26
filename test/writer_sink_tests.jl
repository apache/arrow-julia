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

module WriterSinkTests

using Test
using Arrow

# Inject sink failures at the public path/IO boundary. The path wrapper lets
# the writer own a controlled IO without depending on OS-specific failures.
mutable struct FailingSink <: IO
    data::IOBuffer
    writeerror::Union{Nothing,Exception}
    closeerror::Union{Nothing,Exception}
    flusherror::Union{Nothing,Exception}
    closes::Int
end
FailingSink() = FailingSink(IOBuffer(), nothing, nothing, nothing, 0)

struct SinkPath <: AbstractString
    io::FailingSink
end
Base.open(path::SinkPath, ::String) = path.io
Base.isopen(io::FailingSink) = isopen(io.data)
function Base.write(io::FailingSink, bytes::Vector{UInt8})
    io.writeerror === nothing || throw(io.writeerror)
    return write(io.data, bytes)
end
function Base.close(io::FailingSink)
    io.closes += 1
    close(io.data)
    io.closeerror === nothing || throw(io.closeerror)
    return nothing
end
function Base.flush(io::FailingSink)
    io.flusherror === nothing || throw(io.flusherror)
    return flush(io.data)
end

function caught(f)
    try
        f()
        return nothing
    catch err
        return err
    end
end

@testset "writer sink validation and cleanup" begin
    table = (; a=fill(Int64(7), 64))

    @testset "invalid constructor options preserve the sink" begin
        original = UInt8[0x00, 0xff, 0x41, 0x52, 0x52, 0x4f, 0x57]
        constructors = (
            (sink, options) -> Arrow.Writer(sink; options...),
            (sink, options) -> open(Arrow.Writer, sink; options...),
            (sink, options) -> Arrow.Writer(_ -> nothing, sink; options...),
            (sink, options) -> open(_ -> nothing, Arrow.Writer, sink; options...),
        )
        invalid = (
            ((; compress=:invalid), ArgumentError),
            ((; dictreplacement=true), ArgumentError),
            ((; file=:invalid), TypeError),
            ((; compress=1), TypeError),
            ((; dictreplacement=:invalid), TypeError),
            ((; unknown=true), MethodError),
        )
        mktempdir() do dir
            path = joinpath(dir, "existing.arrow")
            missingpath = joinpath(dir, "missing.arrow")
            for construct in constructors, (options, error) in invalid
                write(path, original)
                @test_throws error construct(path, options)
                @test read(path) == original
                @test_throws error construct(missingpath, options)
                @test !ispath(missingpath)

                io = IOBuffer(copy(original); read=true, write=true)
                seek(io, 3)
                @test_throws error construct(io, options)
                @test isopen(io)
                @test position(io) == 3
                @test take!(io) == original
                close(io)
                # The old constructor creates this file before rejecting.
                rm(missingpath; force=true)
            end
        end
    end

    @testset "valid constructors retain eager byte parity and ownership" begin
        mktempdir() do dir
            path = joinpath(dir, "valid.arrow")
            for file in (true, false), compress in (nothing, :none, :lz4, :zstd)
                expected = Arrow._writebytes(table; file=file, compress=compress)
                write(path, "replace this longer existing destination"^100)
                w = Arrow.Writer(path; file=file, compress=compress)
                @test Arrow.write(w, table) === w
                @test close(w) === nothing
                @test !isopen(w.io)
                @test read(path) == expected

                io = IOBuffer()
                result = Arrow.Writer(io; file=file, compress=compress) do writer
                    Arrow.write(writer, table)
                    :done
                end
                @test result === :done
                @test isopen(io)
                @test take!(io) == expected
                close(io)
            end
            w = Arrow.Writer(path)
            close(w)
            @test !isopen(w.io)
            @test isempty(read(path))
        end
    end

    @testset "eager write and append reject options before changing bytes" begin
        mktempdir() do dir
            path = joinpath(dir, "existing.arrow")
            original = Arrow._writebytes(table; file=false)
            for operation in (Arrow.write, Arrow.append)
                for (options, error) in (
                    ((; compress=:invalid), ArgumentError),
                    ((; compress=1), TypeError),
                    ((; unknown=true), MethodError),
                )
                    write(path, original)
                    @test_throws error operation(path, table; options...)
                    @test read(path) == original
                    io = IOBuffer(copy(original); read=true, write=true)
                    @test_throws error operation(io, table; options...)
                    @test isopen(io)
                    @test take!(io) == original
                    close(io)
                end
            end
        end
    end

    @testset "finalization failures still release owned sinks and codecs" begin
        for ownio in (true, false), file in (true, false), compress in (nothing, :zstd)
            io = FailingSink()
            w = Arrow.Writer(ownio ? SinkPath(io) : io; file=file, compress=compress)
            Arrow.write(w, table)
            failure = ErrorException("final output write failed")
            io.writeerror = failure
            io.closeerror = ErrorException("sink close also failed")
            @test caught(() -> close(w)) === failure
            @test !isopen(w)
            @test w.st.finished
            @test io.closes == Int(ownio)
            @test isopen(io) == !ownio
            @test w.st.state === nothing || w.st.state.zstd === nothing
            @test close(w) === nothing
            @test io.closes == Int(ownio)
            io.closeerror = nothing
            ownio || close(io)
        end
    end

    @testset "function forms preserve the body error when close also fails" begin
        for construct in
            (Arrow.Writer, (f, sink; kw...) -> open(f, Arrow.Writer, sink; kw...))
            for ownio in (true, false), file in (true, false)
                io = FailingSink()
                failure = ErrorException("body failed")
                err = caught() do
                    construct(ownio ? SinkPath(io) : io; file=file) do w
                        Arrow.write(w, table)
                        io.writeerror = ErrorException("final output write also failed")
                        io.closeerror = ErrorException("sink close also failed")
                        throw(failure)
                    end
                end
                @test err === failure
                @test io.closes == Int(ownio)
                @test isopen(io) == !ownio
                io.closeerror = nothing
                ownio || close(io)
            end
        end
    end

    @testset "a body error still finalizes accepted batches" begin
        mktempdir() do dir
            path = joinpath(dir, "body-error.arrow")
            for ownio in (true, false), file in (true, false)
                io = IOBuffer()
                failure = ErrorException("body failed after publishing a batch")
                writer = Ref{Arrow.Writer}()
                err = caught() do
                    Arrow.Writer(ownio ? path : io; file=file) do w
                        writer[] = w
                        Arrow.write(w, table)
                        throw(failure)
                    end
                end
                @test err === failure
                @test !isopen(writer[])
                @test isopen(writer[].io) == !ownio
                bytes = ownio ? read(path) : take!(io)
                @test bytes == Arrow._writebytes(table; file=file)
                close(io)
            end
        end
    end

    @testset "cleanup errors propagate when the body succeeds" begin
        for ownio in (true, false), written in (true, false)
            io = FailingSink()
            failure = ErrorException("sink cleanup failed")
            io.closeerror = ownio ? failure : nothing
            io.flusherror = ownio ? nothing : failure
            err = caught() do
                Arrow.Writer(ownio ? SinkPath(io) : io) do w
                    written && Arrow.write(w, table)
                    :done
                end
            end
            @test err === failure
            @test io.closes == Int(ownio)
            @test isopen(io) == !ownio
            io.closeerror = nothing
            ownio || close(io)
        end
    end
end

end # module WriterSinkTests
