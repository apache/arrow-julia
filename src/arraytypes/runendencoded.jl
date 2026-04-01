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
    Arrow.RunEndEncoded

A read-only `ArrowVector` for Arrow Run-End Encoded arrays. Logical indexing is
resolved by binary searching the physical `run_ends` child and then indexing the
corresponding `values` child.
"""
struct RunEndEncoded{T,R,A} <: ArrowVector{T}
    run_ends::R
    values::A
    ℓ::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

Base.size(r::RunEndEncoded) = (r.ℓ,)
Base.copy(r::RunEndEncoded) = collect(r)

@inline _reephysicalindex(r::RunEndEncoded, i::Integer) = searchsortedfirst(r.run_ends, i)

function _validaterunendencoded(run_ends, values, len)
    nruns = length(run_ends)
    nvals = length(values)
    nruns == nvals || throw(
        ArgumentError(
            "invalid Run-End Encoded array: run_ends length $nruns does not match values length $nvals",
        ),
    )
    if len == 0
        nruns == 0 || throw(
            ArgumentError(
                "invalid Run-End Encoded array: zero logical length requires zero runs",
            ),
        )
    elseif nruns == 0
        throw(
            ArgumentError(
                "invalid Run-End Encoded array: non-zero logical length requires at least one run",
            ),
        )
    end
    last_end = 0
    for (idx, run_end) in enumerate(run_ends)
        current_end = Int(run_end)
        current_end > last_end || throw(
            ArgumentError(
                "invalid Run-End Encoded array: run_ends must be strictly increasing positive integers (failed at run $idx)",
            ),
        )
        last_end = current_end
    end
    len == 0 ||
        last_end == len ||
        throw(
            ArgumentError(
                "invalid Run-End Encoded array: final run end $last_end does not match logical length $len",
            ),
        )
    return
end

function RunEndEncoded(run_ends::R, values::A, len, meta) where {R,A}
    _validaterunendencoded(run_ends, values, Int(len))
    T = eltype(values)
    return RunEndEncoded{T,R,A}(run_ends, values, Int(len), meta)
end

function _makerunendencoded(::Type{T}, run_ends::R, values::A, len, meta) where {T,R,A}
    _validaterunendencoded(run_ends, values, Int(len))
    return RunEndEncoded{T,R,A}(run_ends, values, Int(len), meta)
end

@propagate_inbounds function Base.getindex(r::RunEndEncoded{T}, i::Integer) where {T}
    @boundscheck checkbounds(r, i)
    physical = _reephysicalindex(r, i)
    physical <= length(r.values) || throw(
        ArgumentError(
            "invalid Run-End Encoded array: no physical value found for logical index $i",
        ),
    )
    return @inbounds ArrowTypes.fromarrow(T, r.values[physical])
end

function toarrowvector(
    x::RunEndEncoded,
    i=1,
    de=Dict{Int64,Any}(),
    ded=DictEncoding[],
    meta=getmetadata(x);
    compression::Union{Nothing,Symbol,LZ4FrameCompressor,ZstdCompressor}=nothing,
    kw...,
)
    throw(ArgumentError(RUN_END_ENCODED_UNSUPPORTED))
end
