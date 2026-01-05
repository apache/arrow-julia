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

"""
    Arrow.jl

A pure Julia implementation of the [apache arrow](https://arrow.apache.org/) memory format specification.

This implementation supports the 1.0 version of the specification, including support for:
  * All primitive data types
  * All nested data types
  * Dictionary encodings, nested dictionary encodings, and messages
  * Extension types
  * Streaming, file, record batch, and replacement and isdelta dictionary messages
  * Buffer compression/decompression via the standard LZ4 frame and Zstd formats
  * C data interface for zero-copy interoperability with other Arrow implementations

It currently doesn't include support for:
  * Tensors or sparse tensors
  * Flight RPC

Third-party data formats:
  * csv and parquet support via the existing [CSV.jl](https://github.com/JuliaData/CSV.jl) and [Parquet.jl](https://github.com/JuliaIO/Parquet.jl) packages
  * Other [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible packages automatically supported ([DataFrames.jl](https://github.com/JuliaData/DataFrames.jl), [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl), [JuliaDB.jl](https://github.com/JuliaData/JuliaDB.jl), [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl), [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl), [JDBC.jl](https://github.com/JuliaDatabases/JDBC.jl), [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl), [XLSX.jl](https://github.com/felipenoris/XLSX.jl), etc.)
  * No current Julia packages support ORC or Avro data formats

See docs for official Arrow.jl API with the [User Manual](@ref) and reference docs for [`Arrow.Table`](@ref), [`Arrow.write`](@ref), and [`Arrow.Stream`](@ref).
"""
module Arrow

using Base.Iterators
using Mmap
import Dates
using DataAPI,
    Tables,
    SentinelArrays,
    PooledArrays,
    CodecLz4,
    CodecZstd,
    TimeZones,
    BitIntegers,
    ConcurrentUtilities,
    StringViews

export ArrowTypes

using Base: @propagate_inbounds
import Base: ==

const FILE_FORMAT_MAGIC_BYTES = b"ARROW1"
const CONTINUATION_INDICATOR_BYTES = 0xffffffff

# vendored flatbuffers code for now
include("FlatBuffers/FlatBuffers.jl")
using .FlatBuffers

include("metadata/Flatbuf.jl")
using .Flatbuf
const Meta = Flatbuf

using ArrowTypes
include("utils.jl")
include("arraytypes/arraytypes.jl")
include("eltypes.jl")
include("table.jl")
include("write.jl")
include("append.jl")
include("show.jl")
include("cdata.jl")

const ZSTD_COMPRESSOR = Lockable{ZstdCompressor}[]
const ZSTD_DECOMPRESSOR = Lockable{ZstdDecompressor}[]
const LZ4_FRAME_COMPRESSOR = Lockable{LZ4FrameCompressor}[]
const LZ4_FRAME_DECOMPRESSOR = Lockable{LZ4FrameDecompressor}[]

function init_zstd_compressor()
    zstd = ZstdCompressor(; level=3)
    CodecZstd.TranscodingStreams.initialize(zstd)
    return Lockable(zstd)
end

function init_zstd_decompressor()
    zstd = ZstdDecompressor()
    CodecZstd.TranscodingStreams.initialize(zstd)
    return Lockable(zstd)
end

function init_lz4_frame_compressor()
    lz4 = LZ4FrameCompressor(; compressionlevel=4)
    CodecLz4.TranscodingStreams.initialize(lz4)
    return Lockable(lz4)
end

function init_lz4_frame_decompressor()
    lz4 = LZ4FrameDecompressor()
    CodecLz4.TranscodingStreams.initialize(lz4)
    return Lockable(lz4)
end

function access_threaded(f, v::Vector)
    tid = Threads.threadid()
    0 < tid <= length(v) || _length_assert()
    if @inbounds isassigned(v, tid)
        @inbounds x = v[tid]
    else
        x = f()
        @inbounds v[tid] = x
    end
    return x
end
@noinline _length_assert() = @assert false "0 < tid <= v"

zstd_compressor() = access_threaded(init_zstd_compressor, ZSTD_COMPRESSOR)
zstd_decompressor() = access_threaded(init_zstd_decompressor, ZSTD_DECOMPRESSOR)
lz4_frame_compressor() = access_threaded(init_lz4_frame_compressor, LZ4_FRAME_COMPRESSOR)
lz4_frame_decompressor() =
    access_threaded(init_lz4_frame_decompressor, LZ4_FRAME_DECOMPRESSOR)

function __init__()
    nt = @static if isdefined(Base.Threads, :maxthreadid)
        Threads.maxthreadid()
    else
        Threads.nthreads()
    end
    resize!(empty!(LZ4_FRAME_COMPRESSOR), nt)
    resize!(empty!(ZSTD_COMPRESSOR), nt)
    resize!(empty!(LZ4_FRAME_DECOMPRESSOR), nt)
    resize!(empty!(ZSTD_DECOMPRESSOR), nt)
    return
end

end  # module Arrow
