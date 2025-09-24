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
Extension type registration for Arrow sparse tensors.

This file implements the ArrowTypes interface to register sparse tensors
as Arrow extension types, enabling automatic serialization/deserialization
when working with Arrow data that contains sparse tensor columns.
"""

using ArrowTypes

# Extension type name constants for sparse tensors
const SPARSE_TENSOR_COO = Symbol("arrow.sparse_tensor.coo")
const SPARSE_TENSOR_CSR = Symbol("arrow.sparse_tensor.csr") 
const SPARSE_TENSOR_CSC = Symbol("arrow.sparse_tensor.csc")
const SPARSE_TENSOR_CSF = Symbol("arrow.sparse_tensor.csf")

# Generic sparse tensor extension name (with format in metadata)
const SPARSE_TENSOR = Symbol("arrow.sparse_tensor")

"""
Register sparse tensors as Arrow extension types.

For sparse tensors, we use a generic extension name "arrow.sparse_tensor"
and encode the specific format (COO, CSR, CSC, CSF) in the metadata.
This allows for flexible format representation while maintaining
Arrow extension type compatibility.
"""

# Define how sparse tensors should be serialized to Arrow
# All sparse tensors serialize to a Struct containing their constituent arrays
ArrowTypes.ArrowType(::Type{<:AbstractSparseTensor}) = Arrow.Struct

# Define the generic extension name for all sparse tensors
ArrowTypes.arrowname(::Type{<:AbstractSparseTensor}) = SPARSE_TENSOR

# Define metadata serialization for each sparse tensor type
function ArrowTypes.arrowmetadata(::Type{SparseTensorCOO{T,N}}) where {T,N}
    return "COO"  # Simple format identifier
end

function ArrowTypes.arrowmetadata(::Type{SparseTensorCSX{T}}) where {T}
    return "CSX"  # Will be refined to CSR/CSC during serialization
end

function ArrowTypes.arrowmetadata(::Type{SparseTensorCSF{T,N}}) where {T,N}
    return "CSF"
end

# Define conversion from sparse tensors to Arrow Struct for serialization
function ArrowTypes.toarrow(tensor::SparseTensorCOO{T,N}) where {T,N}
    # Convert COO tensor to a struct with named fields
    # This creates the Arrow storage representation
    indices_flat = vec(tensor.indices)  # Flatten indices matrix
    
    return (
        format = "COO",
        shape = collect(tensor.shape),
        nnz = nnz(tensor),
        ndim = N,
        indices = indices_flat,
        data = tensor.data
    )
end

function ArrowTypes.toarrow(tensor::SparseTensorCSX{T}) where {T}
    format_str = tensor.compressed_axis == :row ? "CSR" : "CSC"
    
    return (
        format = format_str,
        shape = collect(tensor.shape),
        nnz = nnz(tensor),
        ndim = 2,
        compressed_axis = string(tensor.compressed_axis),
        indptr = tensor.indptr,
        indices = tensor.indices,
        data = tensor.data
    )
end

function ArrowTypes.toarrow(tensor::SparseTensorCSF{T,N}) where {T,N}
    return (
        format = "CSF",
        shape = collect(tensor.shape),
        nnz = nnz(tensor), 
        ndim = N,
        indices_buffers = tensor.indices_buffers,
        indptr_buffers = tensor.indptr_buffers,
        data = tensor.data
    )
end

# Define deserialization: how to convert Arrow data back to sparse tensors
function ArrowTypes.JuliaType(::Val{SPARSE_TENSOR}, ::Type{Arrow.Struct}, arrowmetadata::String)
    # The arrowmetadata contains the format type (COO, CSR, CSC, CSF)
    if arrowmetadata == "COO"
        return SparseTensorCOO  # Generic type, will be refined during fromarrow
    elseif arrowmetadata in ("CSX", "CSR", "CSC")
        return SparseTensorCSX
    elseif arrowmetadata == "CSF"
        return SparseTensorCSF
    else
        throw(ArgumentError("Unknown sparse tensor format in metadata: $arrowmetadata"))
    end
end

# Define actual conversion from Arrow Struct to sparse tensors
function ArrowTypes.fromarrow(::Type{SparseTensorCOO}, arrow_struct, extension_metadata::String)
    # Extract fields from the Arrow struct
    format = arrow_struct.format
    shape = tuple(arrow_struct.shape...)
    N = arrow_struct.ndim
    nnz_count = arrow_struct.nnz
    indices_flat = arrow_struct.indices
    data = arrow_struct.data
    
    # Determine element type from data
    T = eltype(data)
    
    # Reshape indices from flat to N×M matrix  
    indices = reshape(indices_flat, N, nnz_count)
    
    return SparseTensorCOO{T,N}(indices, data, shape)
end

function ArrowTypes.fromarrow(::Type{SparseTensorCSX}, arrow_struct, extension_metadata::String)
    # Extract fields from the Arrow struct
    format = arrow_struct.format
    shape = tuple(arrow_struct.shape...)
    compressed_axis = Symbol(arrow_struct.compressed_axis)
    indptr = arrow_struct.indptr
    indices = arrow_struct.indices
    data = arrow_struct.data
    
    # Determine element type from data
    T = eltype(data)
    
    return SparseTensorCSX{T}(indptr, indices, data, shape, compressed_axis)
end

function ArrowTypes.fromarrow(::Type{SparseTensorCSF}, arrow_struct, extension_metadata::String)
    # Extract fields from the Arrow struct
    shape = tuple(arrow_struct.shape...)
    N = arrow_struct.ndim
    indices_buffers = arrow_struct.indices_buffers
    indptr_buffers = arrow_struct.indptr_buffers
    data = arrow_struct.data
    
    # Determine element type from data
    T = eltype(data)
    
    return SparseTensorCSF{T,N}(indices_buffers, indptr_buffers, data, shape)
end

"""
    register_sparse_tensor_extensions()

Register sparse tensor extension types with the Arrow system.
This should be called during module initialization.
"""
function register_sparse_tensor_extensions()
    # The registration happens automatically when the methods above are defined
    # This function exists for explicit initialization if needed
    @debug "Sparse tensor extension types registered:"
    @debug "  $(SPARSE_TENSOR) (COO, CSR, CSC, CSF formats)"
    return nothing
end

# Convenience constructors for creating sparse tensors from common Julia types

"""
    SparseTensorCOO(matrix::SparseMatrixCSC) -> SparseTensorCOO

Convert a Julia SparseMatrixCSC to SparseTensorCOO format.
"""
function SparseTensorCOO(matrix::SparseMatrixCSC{T}) where {T}
    I, J, V = findnz(matrix)
    indices = [I J]'  # Transpose to get 2×nnz matrix
    shape = size(matrix)
    
    return SparseTensorCOO{T,2}(indices, V, shape)
end

"""
    SparseTensorCSX(matrix::SparseMatrixCSC, compressed_axis::Symbol=:col) -> SparseTensorCSX

Convert a Julia SparseMatrixCSC to SparseTensorCSX format.
By default creates CSC format (compressed columns), specify :row for CSR.
"""
function SparseTensorCSX(matrix::SparseMatrixCSC{T}, compressed_axis::Symbol=:col) where {T}
    if compressed_axis == :col
        # Already in CSC format, can use directly
        return SparseTensorCSX{T}(
            matrix.colptr,
            matrix.rowval, 
            matrix.nzval,
            size(matrix),
            :col
        )
    else
        # Convert to CSR format by transposing to CSC then extracting
        matrix_t = transpose(matrix)
        matrix_csr = SparseMatrixCSC(matrix_t)  # Convert transpose to SparseMatrixCSC
        return SparseTensorCSX{T}(
            matrix_csr.colptr,
            matrix_csr.rowval,
            matrix_csr.nzval,
            size(matrix),
            :row
        )
    end
end