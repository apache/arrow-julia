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
Arrow.jl Sparse Tensor Demo

This example demonstrates the usage of sparse tensor formats supported
by Arrow.jl:
- COO (Coordinate): General sparse tensor format
- CSR/CSC (Compressed Sparse Row/Column): Efficient 2D sparse matrices
- CSF (Compressed Sparse Fiber): Advanced N-dimensional sparse tensors

The demo shows construction, manipulation, and serialization of sparse tensors.
"""

using Arrow
using SparseArrays
using LinearAlgebra

println("=== Arrow.jl Sparse Tensor Demo ===\n")

# ============================================================================
# COO (Coordinate) Format Demo
# ============================================================================
println("1. COO (Coordinate) Format")
println("   - General purpose sparse tensor format")
println("   - Stores explicit coordinates and values for each non-zero element")
println()

# Create a 4×4 sparse matrix with some non-zero elements
println("Creating a 4×4 sparse matrix:")
indices = [1 2 3 4 2; 1 2 3 1 4]  # 2×5 matrix: coordinates (row, col)
data = [1.0, 4.0, 9.0, 2.0, 8.0]  # Values at those coordinates
shape = (4, 4)

coo_tensor = Arrow.SparseTensorCOO{Float64,2}(indices, data, shape)
println("COO Tensor: $coo_tensor")
println("Matrix representation:")
for i in 1:4
    row = [coo_tensor[i, j] for j in 1:4]
    println("  $row")
end
println("Non-zero elements: $(Arrow.nnz(coo_tensor))")
println()

# Demonstrate 3D COO tensor
println("Creating a 3×3×3 sparse 3D tensor:")
indices_3d = [1 2 3 1; 1 2 1 3; 1 1 3 3]  # 3×4 matrix
data_3d = [1.0, 2.0, 3.0, 4.0]
shape_3d = (3, 3, 3)

coo_3d = Arrow.SparseTensorCOO{Float64,3}(indices_3d, data_3d, shape_3d)
println("3D COO Tensor: $coo_3d")
println("Sample elements:")
println("  [1,1,1] = $(coo_3d[1,1,1])")
println("  [2,2,1] = $(coo_3d[2,2,1])")  
println("  [1,1,3] = $(coo_3d[1,1,3])")
println("  [1,2,2] = $(coo_3d[1,2,2]) (zero element)")
println()

# ============================================================================
# CSR/CSC (Compressed Sparse Row/Column) Format Demo  
# ============================================================================
println("2. CSX (Compressed Sparse Row/Column) Format")
println("   - Efficient for 2D sparse matrices")
println("   - CSR compresses rows, CSC compresses columns")
println()

# Create the same 4×4 matrix in CSR format
println("Same 4×4 matrix in CSR (Compressed Sparse Row) format:")
# Matrix: [1.0  0   0   0 ]
#         [0    4.0 0   8.0]
#         [0    0   9.0 0  ]
#         [2.0  0   0   0  ]
indptr_csr = [1, 2, 4, 5, 6]  # Row pointers: where each row starts in data/indices
indices_csr = [1, 2, 4, 3, 1]  # Column indices for each value
data_csr = [1.0, 4.0, 8.0, 9.0, 2.0]

csr_tensor = Arrow.SparseTensorCSX{Float64}(indptr_csr, indices_csr, data_csr, (4, 4), :row)
println("CSR Tensor: $csr_tensor")
println("Matrix representation:")
for i in 1:4
    row = [csr_tensor[i, j] for j in 1:4]
    println("  $row")
end
println()

# Create the same matrix in CSC format  
println("Same matrix in CSC (Compressed Sparse Column) format:")
indptr_csc = [1, 3, 4, 5, 6]  # Column pointers
indices_csc = [1, 4, 2, 3, 2]  # Row indices for each value
data_csc = [1.0, 2.0, 4.0, 9.0, 8.0]

csc_tensor = Arrow.SparseTensorCSX{Float64}(indptr_csc, indices_csc, data_csc, (4, 4), :col)
println("CSC Tensor: $csc_tensor")

# Verify both formats give same results
println("Verification - CSR and CSC should give same values:")
println("  CSR[2,2] = $(csr_tensor[2,2]), CSC[2,2] = $(csc_tensor[2,2])")
println("  CSR[2,4] = $(csr_tensor[2,4]), CSC[2,4] = $(csc_tensor[2,4])")
println()

# ============================================================================
# Integration with Julia SparseArrays
# ============================================================================
println("3. Integration with Julia SparseArrays")
println("   - Convert Julia SparseMatrixCSC to Arrow sparse tensors")
println()

# Create a Julia sparse matrix
println("Creating Julia SparseMatrixCSC:")
I_julia = [1, 3, 2, 4, 2]
J_julia = [1, 3, 2, 1, 4] 
V_julia = [10.0, 30.0, 20.0, 40.0, 25.0]
julia_sparse = sparse(I_julia, J_julia, V_julia, 4, 4)
println("Julia sparse matrix:")
display(julia_sparse)
println()

# Convert to Arrow COO format
println("Converting to Arrow COO format:")
coo_from_julia = Arrow.SparseTensorCOO(julia_sparse)
println("Arrow COO: $coo_from_julia")
println("Verification - [3,3] = $(coo_from_julia[3,3]) (should be 30.0)")
println()

# Convert to Arrow CSC format (natural fit)
println("Converting to Arrow CSC format:")
csc_from_julia = Arrow.SparseTensorCSX(julia_sparse, :col)
println("Arrow CSC: $csc_from_julia")
println()

# Convert to Arrow CSR format
println("Converting to Arrow CSR format:")
csr_from_julia = Arrow.SparseTensorCSX(julia_sparse, :row) 
println("Arrow CSR: $csr_from_julia")
println()

# ============================================================================
# CSF (Compressed Sparse Fiber) Format Demo
# ============================================================================
println("4. CSF (Compressed Sparse Fiber) Format")
println("   - Most advanced format for high-dimensional sparse tensors")
println("   - Provides excellent compression for structured sparse data")
println()

# Create a simple 3D CSF tensor (simplified structure)
println("Creating a 2×2×2 CSF tensor:")
indices_buffers_csf = [
    [1, 2],      # Indices for dimension 1
    [1, 2],      # Indices for dimension 2
    [1, 2]       # Indices for dimension 3
]
indptr_buffers_csf = [
    [1, 2, 3],   # Pointers for level 0
    [1, 2, 3]    # Pointers for level 1
]
data_csf = [100.0, 200.0]
shape_csf = (2, 2, 2)

csf_tensor = Arrow.SparseTensorCSF{Float64,3}(indices_buffers_csf, indptr_buffers_csf, data_csf, shape_csf)
println("CSF Tensor: $csf_tensor")
println("Note: CSF format is complex - this is a simplified demonstration")
println()

# ============================================================================
# Serialization and Metadata Demo
# ============================================================================
println("5. Serialization and Metadata")
println("   - Sparse tensors can be serialized with format metadata")
println()

# Generate metadata for different formats
println("COO metadata:")
coo_metadata = Arrow.sparse_tensor_metadata(coo_tensor)
println("  $coo_metadata")
println()

println("CSR metadata:")  
csr_metadata = Arrow.sparse_tensor_metadata(csr_tensor)
println("  $csr_metadata")
println()

# Demonstrate serialization round-trip
println("Serialization round-trip test:")
buffers, metadata = Arrow.serialize_sparse_tensor(coo_tensor)
reconstructed = Arrow.deserialize_sparse_tensor(buffers, metadata, Float64)
println("Original:      $coo_tensor")
println("Reconstructed: $reconstructed")
println("Round-trip successful: $(reconstructed[1,1] == coo_tensor[1,1] && Arrow.nnz(reconstructed) == Arrow.nnz(coo_tensor))")
println()

# ============================================================================
# Performance and Sparsity Analysis
# ============================================================================
println("6. Performance and Sparsity Analysis")
println("   - Demonstrate efficiency gains with sparse storage")
println()

# Create a large sparse matrix
println("Creating a large sparse matrix (1000×1000 with 0.1% non-zeros):")
n = 1000
nnz_count = div(n * n, 1000)  # 0.1% density

# Generate random sparse data
Random.seed!(42)  # For reproducible results
using Random
rows = rand(1:n, nnz_count)
cols = rand(1:n, nnz_count)
vals = rand(Float64, nnz_count)

# Remove duplicates by creating a dictionary
sparse_dict = Dict{Tuple{Int,Int}, Float64}()
for (r, c, v) in zip(rows, cols, vals)
    sparse_dict[(r, c)] = v
end

# Convert back to arrays
coords = collect(keys(sparse_dict))
values = collect(values(sparse_dict))
actual_nnz = length(values)

indices_large = [getindex.(coords, 1) getindex.(coords, 2)]'  # 2×nnz matrix
large_coo = Arrow.SparseTensorCOO{Float64,2}(indices_large, values, (n, n))

println("Large COO tensor: $(large_coo)")
total_elements = n * n
stored_elements = actual_nnz
memory_saved = total_elements - stored_elements
compression_ratio = total_elements / stored_elements

println("Storage analysis:")
println("  Total elements: $(total_elements)")
println("  Stored elements: $(stored_elements)")
println("  Memory saved: $(memory_saved) elements")
println("  Compression ratio: $(round(compression_ratio, digits=2))x")
println("  Storage efficiency: $(round((1 - stored_elements/total_elements) * 100, digits=2))%")
println()

println("=== Demo Complete ===")
println("Sparse tensors provide efficient storage and computation for")
println("data where most elements are zero, with significant memory")
println("savings and computational advantages for appropriate workloads.")