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

import ..ArrowTypes: RunEndEncodedKind

"""
    Arrow.RunEndEncoded

An `ArrowVector` that uses run-end encoding (REE) to efficiently represent
arrays with sequences of repeated values. This is a variation of run-length
encoding where each run is represented by a value and an integer giving the
logical index where the run ends.

The array contains two child arrays:
- `run_ends`: A vector of Int16, Int32, or Int64 values representing the
  accumulated length where each run ends (strictly ascending, 1-indexed)
- `values`: The actual values for each run

For example, the array `[1, 1, 1, 2, 2]` would be encoded as:
- `run_ends = [3, 5]`
- `values = [1, 2]`

Note: The parent array has no validity bitmap (null_count = 0). Nulls are
represented as null values in the `values` child array.
"""
struct RunEndEncoded{T,R<:Union{Int16,Int32,Int64},A} <: ArrowVector{T}
    arrow::Vector{UInt8}  # reference to arrow memory blob
    validity::ValidityBitmap  # always empty for REE (null_count = 0)
    run_ends::Vector{R}  # strictly ascending indices where runs end
    values::A  # child array with actual values
    ℓ::Int64  # logical length of the decoded array
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

RunEndEncoded(
    ::Type{T},
    b::Vector{UInt8},
    v::ValidityBitmap,
    run_ends::Vector{R},
    values::A,
    len,
    meta,
) where {T,R,A} = RunEndEncoded{T,R,A}(b, v, run_ends, values, len, meta)

Base.size(r::RunEndEncoded) = (r.ℓ,)

"""
    _find_physical_index(run_ends, logical_index)

Find the physical index (into the values array) for a given logical index.
Uses binary search to achieve O(log n) lookup time.
"""
@inline function _find_physical_index(run_ends::Vector{R}, i::Integer) where {R}
    # Binary search to find which run contains index i
    # run_ends[j-1] < i <= run_ends[j]
    lo = 1
    hi = length(run_ends)

    @inbounds while lo < hi
        mid = (lo + hi) >>> 1  # unsigned right shift for safe midpoint
        if run_ends[mid] < i
            lo = mid + 1
        else
            hi = mid
        end
    end

    return lo
end

@propagate_inbounds function Base.getindex(r::RunEndEncoded{T}, i::Integer) where {T}
    @boundscheck checkbounds(r, i)
    # Find which run contains this index
    @inbounds physical_idx = _find_physical_index(r.run_ends, i)
    # Return the value for that run
    return @inbounds ArrowTypes.fromarrow(T, r.values[physical_idx])
end

# Iteration - implement efficiently by iterating over runs
function Base.iterate(r::RunEndEncoded{T}) where {T}
    isempty(r) && return nothing
    # State: (current_physical_index, current_logical_index, run_end)
    run_idx = 1
    @inbounds run_end = r.run_ends[1]
    @inbounds val = ArrowTypes.fromarrow(T, r.values[1])
    return (val, (1, 1, run_end, val))
end

function Base.iterate(r::RunEndEncoded{T}, state) where {T}
    run_idx, logical_idx, run_end, val = state
    logical_idx += 1
    logical_idx > r.ℓ && return nothing

    if logical_idx > run_end
        # Move to next run
        run_idx += 1
        @inbounds run_end = r.run_ends[run_idx]
        @inbounds val = ArrowTypes.fromarrow(T, r.values[run_idx])
    end

    return (val, (run_idx, logical_idx, run_end, val))
end

# Don't pass through REE in arrowvector, keep it as-is
arrowvector(::RunEndEncodedKind, x::RunEndEncoded, i, nl, fi, de, ded, meta; kw...) = x

# Convert a regular Julia array to RunEndEncoded format
function arrowvector(::RunEndEncodedKind, x, i, nl, fi, de, ded, meta; run_ends_type::Type{R}=Int32) where {R<:Union{Int16,Int32,Int64}}
    len = length(x)
    len == 0 && error("Cannot create RunEndEncoded array with length 0")

    # Compute runs
    run_ends_vec = R[]
    values_vec = []

    prev_val = @inbounds x[1]
    run_end = 1

    for i in 2:len
        @inbounds curr_val = x[i]
        if !isequal(curr_val, prev_val)
            # End of current run
            push!(run_ends_vec, R(run_end))
            push!(values_vec, prev_val)
            prev_val = curr_val
        end
        run_end = i
    end

    # Don't forget the final run
    push!(run_ends_vec, R(run_end))
    push!(values_vec, prev_val)

    # Create the values child array
    T = eltype(x)
    values_arrow = arrowvector(values_vec, i, nl, fi, de, ded, meta; kw...)

    # Validity bitmap is always empty for REE parent
    validity = ValidityBitmap(UInt8[], len, 0)

    return RunEndEncoded(T, UInt8[], validity, run_ends_vec, values_arrow, len, meta)
end

function compress(Z::Meta.CompressionType.T, comp, r::R) where {R<:RunEndEncoded}
    len = length(r)
    nc = 0  # REE always has null_count = 0 on parent
    # Note: validity bitmap is always empty, so we only compress the child arrays
    # For simplicity, we'll compress the run_ends and delegate values compression
    run_ends_compressed = compress(Z, comp, r.run_ends)
    values_compressed = compress(Z, comp, r.values)
    return Compressed{Z,R}(r, [run_ends_compressed, values_compressed], len, nc, Compressed[])
end

function makenodesbuffers!(
    col::RunEndEncoded{T},
    fieldnodes,
    fieldbuffers,
    bufferoffset,
    alignment,
) where {T}
    len = length(col)
    nc = 0  # REE parent always has null_count = 0
    push!(fieldnodes, FieldNode(len, nc))
    @debug "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"

    # REE has no buffers on the parent level - it uses child arrays instead
    # The validity bitmap is always empty (0 bytes)
    push!(fieldbuffers, Buffer(bufferoffset, 0))
    @debug "made field buffer (validity): bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length)"

    # Now add the child arrays (run_ends and values)
    # Note: The run_ends array is a primitive int array with no nulls
    bufferoffset = makenodesbuffers!(col.run_ends, fieldnodes, fieldbuffers, bufferoffset, alignment)
    bufferoffset = makenodesbuffers!(col.values, fieldnodes, fieldbuffers, bufferoffset, alignment)

    return bufferoffset
end

# Special handling for run_ends which is a plain Vector
function makenodesbuffers!(
    col::Vector{R},
    fieldnodes,
    fieldbuffers,
    bufferoffset,
    alignment,
) where {R<:Union{Int16,Int32,Int64}}
    len = length(col)
    nc = 0  # run_ends never has nulls
    push!(fieldnodes, FieldNode(len, nc))
    @debug "made field node (run_ends): nodeidx = $(length(fieldnodes)), len = $len, nc = 0"

    # validity bitmap (empty - 0 bytes)
    push!(fieldbuffers, Buffer(bufferoffset, 0))
    @debug "made field buffer (run_ends validity): bufferidx = $(length(fieldbuffers)), offset = $bufferoffset, len = 0"

    # data buffer
    blen = len * sizeof(R)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug "made field buffer (run_ends data): bufferidx = $(length(fieldbuffers)), offset = $bufferoffset, len = $blen"

    return bufferoffset + padding(blen, alignment)
end

function writebuffer(io, col::RunEndEncoded, alignment)
    @debug "writebuffer: col = $(typeof(col))"
    @debug col

    # Write empty validity bitmap (0 bytes for parent REE array)
    # No need to write anything or pad since length is 0

    # Write run_ends child array
    writebuffer(io, col.run_ends, alignment)

    # Write values child array
    writebuffer(io, col.values, alignment)

    return
end

# Write buffer for plain Vector{R} (run_ends)
function writebuffer(io, col::Vector{R}, alignment) where {R<:Union{Int16,Int32,Int64}}
    @debug "writebuffer (run_ends): col = $(typeof(col)), length = $(length(col))"

    # No validity bitmap to write (0 bytes)

    # Write the data
    n = writearray(io, R, col)
    @debug "writing run_ends array: n = $n, padded = $(padding(n, alignment))"
    writezeros(io, paddinglength(n, alignment))

    return
end
