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

# Fresh process: a batch that fails to compress leaves the sink holding the
# batches published before it, the same bytes for any `ntasks`, and the
# writer refuses further batches. The failure is injected by replacing the
# compression entry point (a copy of `_compressbytes` that fails on the
# buffer starting with batch 2's first id), so this runs in its own process.
using Arrow, Tables, Test
using PooledArrays: PooledArray

const SENTINEL = Int64(1_000_001)
@eval Arrow function _compressbytes(
    state::EncodeState,
    codec::Int8,
    ptr::Ptr{UInt8},
    len::Int,
)
    len >= 8 &&
        unsafe_load(Ptr{Int64}(ptr)) == $SENTINEL &&
        error("injected compression failure")
    raw = TS.Buffer(unsafe_wrap(Array, ptr, len))
    out = isempty(state.free) ? UInt8[] : pop!(state.free)
    need = len + (len >> 7) + 1024
    length(out) < need && resize!(out, need)
    frame =
        codec == CODEC_LZ4_FRAME ? _lz4frame(state, raw, out) :
        transcode(_zstdc!(state), raw, out)
    push!(state.inuse, frame)
    return frame
end

n = 3_000_000
tbl = (
    id=collect(Int64, 1:n),
    c=PooledArray([string("cat", (i - 1) ÷ 1_000_000) for i = 1:n]),
)
sinks = Dict{Int,Vector{UInt8}}()
for ntasks in (1, 4)
    io = IOBuffer()
    w = Arrow.Writer(io; file=false, compress=:zstd, ntasks=ntasks, batchsize=1_000_000)
    @test_throws ErrorException Arrow.write(w, tbl)
    if ntasks > 1
        # Batch 3 was staged on batch 2's dictionary state and was discarded
        # with it, so the writer takes no more batches.
        err = @test_throws ArgumentError Arrow.write(w, (id=[1], c=PooledArray(["x"])))
        @test occursin("failed to encode", err.value.msg)
    end
    close(w)
    sinks[ntasks] = take!(io)
    t = Arrow.Table(sinks[ntasks])
    @test length(t.id) == 1_000_000 && t.id[end] == 1_000_000
end
@test sinks[1] == sinks[4]
# A do-block writer to a path commits the batches written before the failure.
path = tempname()
@test_throws ErrorException Arrow.Writer(
    path;
    compress=:zstd,
    ntasks=4,
    batchsize=1_000_000,
) do w
    Arrow.write(w, tbl)
end
@test Arrow.Table(path).id == 1:1_000_000
rm(path)
