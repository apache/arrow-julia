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

using Test
using Arrow
using Arrow: AbstractSparseTensor, SparseTensorCOO, SparseTensorCSX, SparseTensorCSF, nnz
using Arrow: sparse_tensor_metadata, parse_sparse_tensor_metadata
using Arrow: serialize_sparse_tensor, deserialize_sparse_tensor
using SparseArrays

@testset "Sparse Tensors" begin
    
    @testset "SparseTensorCOO" begin
        @testset "Basic Construction and Interface" begin
            # Create a simple 3×3 sparse matrix with 3 non-zero elements
            indices = [1 2 3; 1 3 2]  # 2×3 matrix: (1,1), (2,3), (3,2)
            data = [1.0, 2.0, 3.0]
            shape = (3, 3)
            
            tensor = SparseTensorCOO{Float64,2}(indices, data, shape)
            
            @test size(tensor) == (3, 3)
            @test eltype(tensor) == Float64
            @test nnz(tensor) == 3
            @test tensor.shape == (3, 3)
            
            # Test element access
            @test tensor[1, 1] == 1.0
            @test tensor[2, 3] == 2.0
            @test tensor[3, 2] == 3.0
            @test tensor[1, 2] == 0.0  # Zero element
            @test tensor[2, 2] == 0.0  # Zero element
            
            # Test bounds checking
            @test_throws BoundsError tensor[0, 1]
            @test_throws BoundsError tensor[4, 1]
            @test_throws BoundsError tensor[1, 4]
        end
        
        @testset "Constructor from convenience function" begin
            indices = [1 2; 1 2]  # 2×2 matrix: (1,1), (2,2)
            data = [5, 10]
            shape = (2, 2)
            
            tensor = SparseTensorCOO(indices, data, shape)
            @test tensor isa SparseTensorCOO{Int,2}
            @test tensor[1, 1] == 5
            @test tensor[2, 2] == 10
        end
        
        @testset "3D Tensor" begin
            # Create a 2×3×4 sparse tensor with 4 non-zero elements
            indices = [1 2 1 2; 1 2 3 1; 1 2 3 4]  # 3×4 matrix
            data = [1.5, 2.5, 3.5, 4.5]
            shape = (2, 3, 4)
            
            tensor = SparseTensorCOO{Float64,3}(indices, data, shape)
            
            @test size(tensor) == (2, 3, 4)
            @test nnz(tensor) == 4
            @test tensor[1, 1, 1] == 1.5
            @test tensor[2, 2, 2] == 2.5
            @test tensor[1, 3, 3] == 3.5
            @test tensor[2, 1, 4] == 4.5
            @test tensor[2, 3, 4] == 0.0  # Zero element
        end
        
        @testset "Error Handling" begin
            # Mismatched dimensions
            indices = [1 2; 1 2]  # 2×2
            data = [1.0]  # Length 1
            shape = (2, 2)
            
            @test_throws ArgumentError SparseTensorCOO{Float64,2}(indices, data, shape)
            
            # Wrong number of index rows
            indices = [1 2 3]  # 1×3 (should be 2×3 for 2D tensor)
            data = [1.0, 2.0, 3.0]
            shape = (3, 3)
            
            @test_throws ArgumentError SparseTensorCOO{Float64,2}(indices, data, shape)
            
            # Out of bounds indices
            indices = [1 5; 1 2]  # Column 5 > shape[2]=3
            data = [1.0, 2.0]
            shape = (3, 3)
            
            @test_throws ArgumentError SparseTensorCOO{Float64,2}(indices, data, shape)
        end
        
        @testset "Julia SparseMatrixCSC Conversion" begin
            # Create a Julia sparse matrix
            I = [1, 2, 3, 2]
            J = [1, 3, 2, 2] 
            V = [1.0, 2.0, 3.0, 4.0]
            sparse_mat = sparse(I, J, V, 3, 3)
            
            # Convert to COO tensor
            coo_tensor = SparseTensorCOO(sparse_mat)
            
            @test size(coo_tensor) == (3, 3)
            @test nnz(coo_tensor) == 4
            @test coo_tensor[1, 1] == 1.0
            @test coo_tensor[2, 3] == 2.0
            @test coo_tensor[3, 2] == 3.0
            @test coo_tensor[2, 2] == 4.0
        end
    end
    
    @testset "SparseTensorCSX" begin
        @testset "CSR Format" begin
            # Create a 3×3 CSR matrix:
            # [1.0  0   2.0]
            # [0    3.0 0  ]  
            # [4.0  5.0 0  ]
            indptr = [1, 3, 4, 6]  # Row starts
            indices = [1, 3, 2, 1, 2]  # Column indices (1-based)
            data = [1.0, 2.0, 3.0, 4.0, 5.0]
            shape = (3, 3)
            
            tensor = SparseTensorCSX{Float64}(indptr, indices, data, shape, :row)
            
            @test size(tensor) == (3, 3)
            @test nnz(tensor) == 5
            @test tensor.compressed_axis == :row
            
            # Test element access
            @test tensor[1, 1] == 1.0
            @test tensor[1, 3] == 2.0
            @test tensor[2, 2] == 3.0
            @test tensor[3, 1] == 4.0
            @test tensor[3, 2] == 5.0
            @test tensor[1, 2] == 0.0  # Zero element
            @test tensor[2, 1] == 0.0  # Zero element
        end
        
        @testset "CSC Format" begin
            # Same matrix but in CSC format
            indptr = [1, 3, 5, 6]  # Column starts  
            indices = [1, 3, 2, 3, 1]  # Row indices
            data = [1.0, 4.0, 3.0, 5.0, 2.0]
            shape = (3, 3)
            
            tensor = SparseTensorCSX{Float64}(indptr, indices, data, shape, :col)
            
            @test size(tensor) == (3, 3)
            @test nnz(tensor) == 5
            @test tensor.compressed_axis == :col
            
            # Test same elements as CSR version
            @test tensor[1, 1] == 1.0
            @test tensor[1, 3] == 2.0
            @test tensor[2, 2] == 3.0
            @test tensor[3, 1] == 4.0
            @test tensor[3, 2] == 5.0
        end
        
        @testset "Julia SparseMatrixCSC Conversion" begin
            I = [1, 2, 3, 2]
            J = [1, 3, 2, 2]
            V = [1.0, 2.0, 3.0, 4.0]
            sparse_mat = sparse(I, J, V, 3, 3)
            
            # Convert to CSC tensor (default)
            csc_tensor = SparseTensorCSX(sparse_mat)
            @test csc_tensor.compressed_axis == :col
            @test nnz(csc_tensor) == 4
            
            # Convert to CSR tensor
            csr_tensor = SparseTensorCSX(sparse_mat, :row)
            @test csr_tensor.compressed_axis == :row
            @test nnz(csr_tensor) == 4
            
            # Both should give same element access
            @test csc_tensor[1, 1] == csr_tensor[1, 1]
            @test csc_tensor[2, 2] == csr_tensor[2, 2]
        end
        
        @testset "Error Handling" begin
            # Invalid compressed axis
            @test_throws ArgumentError SparseTensorCSX{Float64}([1, 2], [1], [1.0], (1, 1), :invalid)
            
            # Wrong indptr length
            indptr = [1, 2]  # Length 2, should be 4 for 3 rows
            indices = [1]
            data = [1.0]
            shape = (3, 3)
            @test_throws ArgumentError SparseTensorCSX{Float64}(indptr, indices, data, shape, :row)
            
            # Mismatched data lengths
            indptr = [1, 2, 2, 3]
            indices = [1, 2]  # Length 2
            data = [1.0]      # Length 1
            shape = (3, 3)
            @test_throws ArgumentError SparseTensorCSX{Float64}(indptr, indices, data, shape, :row)
        end
    end
    
    @testset "SparseTensorCSF" begin
        @testset "Basic Construction" begin
            # Simple 2×2×2 CSF tensor with 2 non-zero elements
            # This is a simplified test since CSF is complex
            indices_buffers = Vector{AbstractVector{Int}}([
                [1, 2],      # Dimension 1 indices
                [1, 2],      # Dimension 2 indices  
                [1, 2]       # Dimension 3 indices
            ])
            indptr_buffers = Vector{AbstractVector{Int}}([
                [1, 2, 3],   # Level 0 pointers
                [1, 2, 3]    # Level 1 pointers
            ])
            data = [1.0, 2.0]
            shape = (2, 2, 2)
            
            tensor = SparseTensorCSF{Float64,3}(indices_buffers, indptr_buffers, data, shape)
            
            @test size(tensor) == (2, 2, 2)
            @test nnz(tensor) == 2
            @test length(tensor.indices_buffers) == 3
            @test length(tensor.indptr_buffers) == 2
        end
        
        @testset "Error Handling" begin
            # Wrong number of indices buffers
            indices_buffers = Vector{AbstractVector{Int}}([[1], [1]])  # 2 buffers for 3D tensor
            indptr_buffers = Vector{AbstractVector{Int}}([[1, 2], [1, 2]])
            data = [1.0]
            shape = (2, 2, 2)
            
            @test_throws ArgumentError SparseTensorCSF{Float64,3}(indices_buffers, indptr_buffers, data, shape)
            
            # Wrong number of indptr buffers  
            indices_buffers = Vector{AbstractVector{Int}}([[1], [1], [1]])
            indptr_buffers = Vector{AbstractVector{Int}}([[1, 2]])  # 1 buffer, should be 2 for 3D tensor
            data = [1.0]
            shape = (2, 2, 2)
            
            @test_throws ArgumentError SparseTensorCSF{Float64,3}(indices_buffers, indptr_buffers, data, shape)
        end
    end
    
    @testset "Metadata and Serialization" begin
        @testset "COO Metadata" begin
            indices = [1 2; 1 2]
            data = [1.0, 2.0]
            shape = (2, 2)
            tensor = SparseTensorCOO{Float64,2}(indices, data, shape)
            
            metadata_json = sparse_tensor_metadata(tensor)
            metadata = parse_sparse_tensor_metadata(metadata_json)
            
            @test metadata["format_type"] == "COO"
            @test metadata["shape"] == [2, 2]
            @test metadata["nnz"] == 2
            @test metadata["ndim"] == 2
        end
        
        @testset "CSX Metadata" begin
            indptr = [1, 2, 3]
            indices = [1, 2]
            data = [1.0, 2.0]
            shape = (2, 2)
            csr_tensor = SparseTensorCSX{Float64}(indptr, indices, data, shape, :row)
            
            metadata_json = sparse_tensor_metadata(csr_tensor)
            metadata = parse_sparse_tensor_metadata(metadata_json)
            
            @test metadata["format_type"] == "CSR"
            @test metadata["compressed_axis"] == "row"
            @test metadata["shape"] == [2, 2]
            @test metadata["nnz"] == 2
        end
        
        @testset "Serialization Round-trip" begin
            # Test COO serialization
            indices = [1 2 3; 1 3 2]
            data = [1.0, 2.0, 3.0]
            shape = (3, 3)
            original_tensor = SparseTensorCOO{Float64,2}(indices, data, shape)
            
            buffers, metadata = serialize_sparse_tensor(original_tensor)
            reconstructed = deserialize_sparse_tensor(buffers, metadata, Float64)
            
            @test reconstructed isa SparseTensorCOO{Float64,2}
            @test size(reconstructed) == size(original_tensor)
            @test nnz(reconstructed) == nnz(original_tensor)
            @test reconstructed[1, 1] == original_tensor[1, 1]
            @test reconstructed[2, 3] == original_tensor[2, 3]
        end
    end
    
    @testset "Display and Printing" begin
        @testset "COO Display" begin
            indices = [1 2; 1 2]
            data = [1.0, 2.0]
            shape = (2, 2)
            tensor = SparseTensorCOO{Float64,2}(indices, data, shape)
            
            str_repr = string(tensor)
            @test occursin("SparseTensorCOO{Float64,2}", str_repr)
            @test occursin("2×2", str_repr)
            @test occursin("2 stored entries", str_repr)
        end
        
        @testset "CSX Display" begin
            indptr = [1, 2, 3]
            indices = [1, 2]
            data = [1.0, 2.0]
            shape = (2, 2)
            tensor = SparseTensorCSX{Float64}(indptr, indices, data, shape, :row)
            
            str_repr = string(tensor)
            @test occursin("SparseTensorCSX{Float64}", str_repr)
            @test occursin("CSR", str_repr)
            @test occursin("2×2", str_repr)
        end
        
        @testset "Pretty Printing" begin
            indices = [1 2; 1 2]
            data = [5, 10]
            shape = (2, 2)
            tensor = SparseTensorCOO{Int,2}(indices, data, shape)
            
            io = IOBuffer()
            show(io, MIME"text/plain"(), tensor)
            pretty_str = String(take!(io))
            
            @test occursin("2×2 SparseTensorCOO{Int64", pretty_str)  # Allow for spacing differences
            @test occursin("2 stored entries", pretty_str)
            @test occursin("Sparsity:", pretty_str)
            @test occursin("(1, 1) → 5", pretty_str)
            @test occursin("(2, 2) → 10", pretty_str)
        end
    end
    
    @testset "Different Element Types" begin
        for T in [Int32, Int64, Float32, Float64, ComplexF64]
            indices = [1 2; 1 2]
            data = T[1, 2]
            shape = (2, 2)
            
            tensor = SparseTensorCOO{T,2}(indices, data, shape)
            @test eltype(tensor) == T
            @test tensor[1, 1] == T(1)
            @test tensor[2, 2] == T(2)
        end
    end
    
    @testset "Large Sparse Tensors" begin
        # Create a larger sparse tensor to test performance
        n = 100
        k = 10  # 10 non-zero elements in 100×100 matrix
        
        rows = rand(1:n, k)
        cols = rand(1:n, k)
        vals = rand(Float64, k)
        
        indices = [rows cols]'  # 2×k matrix
        tensor = SparseTensorCOO{Float64,2}(indices, vals, (n, n))
        
        @test size(tensor) == (n, n)
        @test nnz(tensor) == k
        
        # Test sparsity calculation
        total_elements = n * n
        expected_sparsity = 1.0 - k / total_elements
        
        io = IOBuffer()
        show(io, MIME"text/plain"(), tensor)
        output = String(take!(io))
        @test occursin("$(round(expected_sparsity * 100, digits=2))%", output)
    end
    
    @testset "Edge Cases" begin
        @testset "Empty Sparse Tensor" begin
            indices = zeros(Int, 2, 0)  # 2×0 matrix (no elements)
            data = Float64[]
            shape = (3, 3)
            
            tensor = SparseTensorCOO{Float64,2}(indices, data, shape)
            @test size(tensor) == (3, 3)
            @test nnz(tensor) == 0
            @test tensor[1, 1] == 0.0
            @test tensor[2, 2] == 0.0
        end
        
        @testset "Single Element Tensor" begin
            indices = reshape([1, 1], 2, 1)  # 2×1 matrix
            data = [42.0]
            shape = (1, 1)
            
            tensor = SparseTensorCOO{Float64,2}(indices, data, shape)
            @test size(tensor) == (1, 1)
            @test nnz(tensor) == 1
            @test tensor[1, 1] == 42.0
        end
    end
end