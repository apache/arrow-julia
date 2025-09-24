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
Sparse tensor serialization and deserialization for Arrow.jl

This module implements the serialization/deserialization logic for sparse
tensors using the Arrow format. Sparse tensors are serialized as extension
types with custom metadata describing the sparse format and structure.

The implementation follows the Arrow specification for SparseTensor messages
and extends it with Julia-specific optimizations.
"""

using ..FlatBuffers
# Simple JSON helpers for sparse tensor metadata
function _write_sparse_json(obj::Dict{String,Any})::String
    # Simple JSON serialization for sparse tensor metadata
    parts = String[]
    for (k, v) in obj
        if v isa String
            push!(parts, "\"$k\":\"$v\"")
        elseif v isa Number
            push!(parts, "\"$k\":$v")
        elseif v isa Vector{Int}
            vals = join(v, ",")
            push!(parts, "\"$k\":[$vals]")
        elseif v isa Dict
            # Nested dict - simple case
            nested_parts = String[]
            for (nk, nv) in v
                if nv isa String
                    push!(nested_parts, "\"$nk\":\"$nv\"")
                else
                    push!(nested_parts, "\"$nk\":$nv")
                end
            end
            push!(parts, "\"$k\":{$(join(nested_parts, ","))}")
        end
    end
    return "{$(join(parts, ","))}"
end

function _parse_sparse_json(json_str::String)::Dict{String,Any}
    # Simple JSON parsing for sparse tensor metadata
    result = Dict{String,Any}()
    
    # Remove outer braces
    content = strip(json_str, ['{', '}'])
    if isempty(content)
        return result
    end
    
    # More careful parsing to handle arrays correctly
    i = 1
    while i <= length(content)
        # Find the start of a key
        while i <= length(content) && content[i] in [' ', ',']
            i += 1
        end
        
        if i > length(content)
            break
        end
        
        # Parse key
        if content[i] == '"'
            key_start = i + 1
            i += 1
            while i <= length(content) && content[i] != '"'
                i += 1
            end
            key = content[key_start:i-1]
            i += 1
        else
            break
        end
        
        # Skip to colon
        while i <= length(content) && content[i] != ':'
            i += 1
        end
        i += 1 # skip colon
        
        # Skip whitespace
        while i <= length(content) && content[i] == ' '
            i += 1
        end
        
        # Parse value
        if i > length(content)
            break
        elseif content[i] == '"'
            # String value
            value_start = i + 1
            i += 1
            while i <= length(content) && content[i] != '"'
                i += 1
            end
            result[key] = content[value_start:i-1]
            i += 1
        elseif content[i] == '['
            # Array value
            i += 1 # skip opening bracket
            arr_content = ""
            bracket_count = 1
            while i <= length(content) && bracket_count > 0
                if content[i] == '['
                    bracket_count += 1
                elseif content[i] == ']'
                    bracket_count -= 1
                end
                
                if bracket_count > 0
                    arr_content *= content[i]
                end
                i += 1
            end
            
            # Parse array content
            if isempty(strip(arr_content))
                result[key] = Int[]
            else
                result[key] = parse.(Int, split(arr_content, ","))
            end
        else
            # Number value
            value_start = i
            while i <= length(content) && content[i] != ',' && content[i] != '}'
                i += 1
            end
            value_str = strip(content[value_start:i-1])
            
            try
                if '.' in value_str
                    result[key] = parse(Float64, value_str)
                else
                    result[key] = parse(Int, value_str)
                end
            catch
                result[key] = value_str
            end
        end
    end
    
    return result
end

# Sparse tensor format type constants (matching Arrow specification)
const SPARSE_FORMAT_COO = Int8(0)
const SPARSE_FORMAT_CSR = Int8(1) 
const SPARSE_FORMAT_CSC = Int8(2)
const SPARSE_FORMAT_CSF = Int8(3)

# COO index format
struct SparseMatrixIndexCOO <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::SparseMatrixIndexCOO) = (:indicesBuffer, :indicesType)

function Base.getproperty(x::SparseMatrixIndexCOO, field::Symbol)
    if field === :indicesBuffer
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
    elseif field === :indicesType
        o = FlatBuffers.offset(x, 6)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(Buffer, FlatBuffers.bytes(x), y)
        end
    end
    return nothing
end

# CSR/CSC index format  
struct SparseMatrixIndexCSX <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::SparseMatrixIndexCSX) = (:indptrBuffer, :indicesBuffer, :indptrType, :indicesType)

function Base.getproperty(x::SparseMatrixIndexCSX, field::Symbol)
    if field === :indptrBuffer
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
    elseif field === :indicesBuffer
        o = FlatBuffers.offset(x, 6)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
    elseif field === :indptrType
        o = FlatBuffers.offset(x, 8)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(Buffer, FlatBuffers.bytes(x), y)
        end
    elseif field === :indicesType
        o = FlatBuffers.offset(x, 10)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(Buffer, FlatBuffers.bytes(x), y)
        end
    end
    return nothing
end

# Sparse tensor metadata
struct SparseTensorMetadata <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::SparseTensorMetadata) = (:formatType, :shape, :nnz, :indexFormat)

function Base.getproperty(x::SparseTensorMetadata, field::Symbol)
    if field === :formatType
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int8)
        return SPARSE_FORMAT_COO
    elseif field === :shape
        o = FlatBuffers.offset(x, 6)
        if o != 0
            return FlatBuffers.Array{Int64}(x, o)
        end
    elseif field === :nnz
        o = FlatBuffers.offset(x, 8)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int64)
    elseif field === :indexFormat
        o = FlatBuffers.offset(x, 10)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            # Return appropriate index format based on format type
            format_type = x.formatType
            if format_type == SPARSE_FORMAT_COO
                return FlatBuffers.init(SparseMatrixIndexCOO, FlatBuffers.bytes(x), y)
            elseif format_type in (SPARSE_FORMAT_CSR, SPARSE_FORMAT_CSC)
                return FlatBuffers.init(SparseMatrixIndexCSX, FlatBuffers.bytes(x), y)
            # CSF would be handled here with additional format struct
            end
        end
    end
    return nothing
end

"""
    sparse_tensor_metadata(tensor::AbstractSparseTensor) -> String

Generate JSON metadata string for sparse tensor following Arrow extension format.
"""
function sparse_tensor_metadata(tensor::SparseTensorCOO{T,N}) where {T,N}
    metadata = Dict{String,Any}()
    
    metadata["format_type"] = "COO"
    metadata["shape"] = collect(tensor.shape)
    metadata["nnz"] = nnz(tensor)
    metadata["ndim"] = N
    
    return _write_sparse_json(metadata)
end

function sparse_tensor_metadata(tensor::SparseTensorCSX{T}) where {T}
    metadata = Dict{String,Any}()
    
    metadata["format_type"] = string(tensor.compressed_axis == :row ? "CSR" : "CSC")
    metadata["shape"] = collect(tensor.shape)  
    metadata["nnz"] = nnz(tensor)
    metadata["ndim"] = 2
    metadata["compressed_axis"] = string(tensor.compressed_axis)
    
    return _write_sparse_json(metadata)
end

function sparse_tensor_metadata(tensor::SparseTensorCSF{T,N}) where {T,N}
    metadata = Dict{String,Any}()
    
    metadata["format_type"] = "CSF"
    metadata["shape"] = collect(tensor.shape)
    metadata["nnz"] = nnz(tensor)
    metadata["ndim"] = N
    
    return _write_sparse_json(metadata)
end

"""
    parse_sparse_tensor_metadata(metadata_json::String) -> Dict{String,Any}

Parse sparse tensor metadata JSON string.
"""
function parse_sparse_tensor_metadata(metadata_json::String)
    metadata = _parse_sparse_json(metadata_json)
    
    # Validate required fields
    required_fields = ["format_type", "shape", "nnz", "ndim"]
    for field in required_fields
        if !haskey(metadata, field)
            throw(ArgumentError("Sparse tensor metadata must include '$field' field"))
        end
    end
    
    return metadata
end

"""
    serialize_sparse_tensor_coo(tensor::SparseTensorCOO) -> (buffers, metadata)

Serialize a COO sparse tensor to Arrow buffers and metadata.
Returns a tuple of (buffer_array, metadata_json).
"""
function serialize_sparse_tensor_coo(tensor::SparseTensorCOO{T,N}) where {T,N}
    # Create buffers for serialization
    buffers = Any[]
    
    # Buffer 0: Validity buffer (can be null for sparse tensors)
    push!(buffers, nothing)
    
    # Buffer 1: Indices buffer (flattened N×M matrix)
    indices_flat = vec(tensor.indices)  # Flatten to 1D
    push!(buffers, indices_flat)
    
    # Buffer 2: Data buffer  
    push!(buffers, tensor.data)
    
    # Generate metadata
    metadata = sparse_tensor_metadata(tensor)
    
    return buffers, metadata
end

"""
    deserialize_sparse_tensor_coo(buffers, metadata_json::String, ::Type{T}) -> SparseTensorCOO{T,N}

Deserialize Arrow buffers to a COO sparse tensor.
"""
function deserialize_sparse_tensor_coo(buffers, metadata_json::String, ::Type{T}) where {T}
    metadata = parse_sparse_tensor_metadata(metadata_json)
    
    shape = tuple([Int(x) for x in metadata["shape"]]...)
    N = metadata["ndim"]
    nnz_count = metadata["nnz"]
    
    # Extract buffers
    indices_flat = buffers[2]  # Skip validity buffer
    data = buffers[3]
    
    # Reshape indices from flat to N×M matrix
    indices = reshape(indices_flat, N, nnz_count)
    
    return SparseTensorCOO{T,N}(indices, data, shape)
end

"""
    serialize_sparse_tensor_csx(tensor::SparseTensorCSX) -> (buffers, metadata)

Serialize a CSX sparse matrix to Arrow buffers and metadata.
"""
function serialize_sparse_tensor_csx(tensor::SparseTensorCSX{T}) where {T}
    buffers = Any[]
    
    # Buffer 0: Validity buffer
    push!(buffers, nothing)
    
    # Buffer 1: Index pointer buffer
    push!(buffers, tensor.indptr)
    
    # Buffer 2: Indices buffer  
    push!(buffers, tensor.indices)
    
    # Buffer 3: Data buffer
    push!(buffers, tensor.data)
    
    metadata = sparse_tensor_metadata(tensor)
    
    return buffers, metadata
end

"""
    deserialize_sparse_tensor_csx(buffers, metadata_json::String, ::Type{T}) -> SparseTensorCSX{T}

Deserialize Arrow buffers to a CSX sparse matrix.
"""
function deserialize_sparse_tensor_csx(buffers, metadata_json::String, ::Type{T}) where {T}
    metadata = parse_sparse_tensor_metadata(metadata_json)
    
    shape = tuple([Int(x) for x in metadata["shape"]]...)
    compressed_axis = Symbol(metadata["compressed_axis"])
    
    # Extract buffers
    indptr = buffers[2]
    indices = buffers[3] 
    data = buffers[4]
    
    return SparseTensorCSX{T}(indptr, indices, data, shape, compressed_axis)
end

"""
    serialize_sparse_tensor_csf(tensor::SparseTensorCSF) -> (buffers, metadata)

Serialize a CSF sparse tensor to Arrow buffers and metadata.
Note: This is a complex format and the implementation is simplified.
"""
function serialize_sparse_tensor_csf(tensor::SparseTensorCSF{T,N}) where {T,N}
    buffers = Any[]
    
    # Buffer 0: Validity buffer
    push!(buffers, nothing)
    
    # Buffers 1 to N: Indices buffers (one per dimension)
    for indices_buffer in tensor.indices_buffers
        push!(buffers, indices_buffer)
    end
    
    # Buffers N+1 to 2N-1: Index pointer buffers  
    for indptr_buffer in tensor.indptr_buffers
        push!(buffers, indptr_buffer)
    end
    
    # Final buffer: Data values
    push!(buffers, tensor.data)
    
    metadata = sparse_tensor_metadata(tensor)
    
    return buffers, metadata
end

"""
    deserialize_sparse_tensor_csf(buffers, metadata_json::String, ::Type{T}) -> SparseTensorCSF{T,N}

Deserialize Arrow buffers to a CSF sparse tensor.
Note: This is a complex format and the implementation is simplified.
"""
function deserialize_sparse_tensor_csf(buffers, metadata_json::String, ::Type{T}) where {T}
    metadata = parse_sparse_tensor_metadata(metadata_json)
    
    shape = tuple([Int(x) for x in metadata["shape"]]...)
    N = metadata["ndim"]
    
    # Extract indices buffers (buffers 1 to N)
    indices_buffers = [buffers[i] for i in 2:(N+1)]
    
    # Extract indptr buffers (buffers N+1 to 2N-1) 
    indptr_buffers = [buffers[i] for i in (N+2):(2*N)]
    
    # Extract data buffer (final buffer)
    data = buffers[end]
    
    return SparseTensorCSF{T,N}(indices_buffers, indptr_buffers, data, shape)
end

"""
    serialize_sparse_tensor(tensor::AbstractSparseTensor) -> (buffers, metadata)

Generic sparse tensor serialization dispatcher.
"""
function serialize_sparse_tensor(tensor::SparseTensorCOO)
    return serialize_sparse_tensor_coo(tensor)
end

function serialize_sparse_tensor(tensor::SparseTensorCSX)  
    return serialize_sparse_tensor_csx(tensor)
end

function serialize_sparse_tensor(tensor::SparseTensorCSF)
    return serialize_sparse_tensor_csf(tensor)
end

"""
    deserialize_sparse_tensor(buffers, metadata_json::String, ::Type{T}) -> AbstractSparseTensor{T}

Generic sparse tensor deserialization dispatcher.
"""
function deserialize_sparse_tensor(buffers, metadata_json::String, ::Type{T}) where {T}
    metadata = parse_sparse_tensor_metadata(metadata_json)
    format_type = metadata["format_type"]
    
    if format_type == "COO"
        return deserialize_sparse_tensor_coo(buffers, metadata_json, T)
    elseif format_type == "CSR"
        return deserialize_sparse_tensor_csx(buffers, metadata_json, T)  
    elseif format_type == "CSC"
        return deserialize_sparse_tensor_csx(buffers, metadata_json, T)
    elseif format_type == "CSF"
        return deserialize_sparse_tensor_csf(buffers, metadata_json, T)
    else
        throw(ArgumentError("Unknown sparse tensor format: $format_type"))
    end
end