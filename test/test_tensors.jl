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
using Arrow: DenseTensor, tensor_metadata, parse_tensor_metadata, from_arrow_tensor
# Using Arrow's built-in simple JSON utilities

@testset "Dense Tensors" begin
    
    @testset "Basic Construction" begin
        # Test construction from Julia arrays
        data_1d = [1.0, 2.0, 3.0, 4.0]
        tensor_1d = DenseTensor(data_1d)
        @test size(tensor_1d) == (4,)
        @test eltype(tensor_1d) == Float64
        @test tensor_1d[1] == 1.0
        @test tensor_1d[4] == 4.0
        
        # Test 2D tensor
        data_2d = [1.0 2.0 3.0; 4.0 5.0 6.0]  # 2x3 matrix
        tensor_2d = DenseTensor(data_2d)
        @test size(tensor_2d) == (2, 3)
        @test tensor_2d[1, 1] == 1.0
        @test tensor_2d[1, 2] == 2.0
        @test tensor_2d[2, 1] == 4.0
        @test tensor_2d[2, 3] == 6.0
        
        # Test 3D tensor
        data_3d = reshape(1.0:24.0, (2, 3, 4))
        tensor_3d = DenseTensor(data_3d)
        @test size(tensor_3d) == (2, 3, 4)
        @test tensor_3d[1, 1, 1] == 1.0
        @test tensor_3d[2, 3, 4] == 24.0
    end
    
    @testset "AbstractArray Interface" begin
        data = [1 2 3; 4 5 6]
        tensor = DenseTensor(data)
        
        # Test size and ndims
        @test size(tensor) == (2, 3)
        @test ndims(tensor) == 2
        @test length(tensor) == 6
        
        # Test indexing
        @test tensor[1, 1] == 1
        @test tensor[2, 3] == 6
        
        # Test bounds checking
        @test_throws BoundsError tensor[0, 1]
        @test_throws BoundsError tensor[3, 1]
        @test_throws BoundsError tensor[1, 4]
        
        # Test iteration
        vals = collect(tensor)
        @test length(vals) == 6
        
        # Test setindex
        tensor[1, 1] = 99
        @test tensor[1, 1] == 99
    end
    
    @testset "JSON Metadata" begin
        # Test basic metadata generation
        data = [1 2; 3 4]
        tensor = DenseTensor(data)
        
        metadata_json = tensor_metadata(tensor)
        metadata = Arrow._parse_simple_json(metadata_json)
        @test metadata["shape"] == [2, 2]
        @test !haskey(metadata, "dim_names")
        @test !haskey(metadata, "permutation")
        
        # Test metadata parsing
        shape, dim_names, permutation = parse_tensor_metadata(metadata_json)
        @test shape == [2, 2]
        @test dim_names === nothing
        @test permutation === nothing
        
        # Test metadata with dimension names and permutation
        tensor_with_names = DenseTensor{Int,2}(
            tensor.parent,
            (2, 2), 
            (:rows, :cols),
            (2, 1)  # Transposed
        )
        
        metadata_json2 = tensor_metadata(tensor_with_names)
        metadata2 = Arrow._parse_simple_json(metadata_json2)
        @test metadata2["shape"] == [2, 2]
        @test metadata2["dim_names"] == ["rows", "cols"]
        @test metadata2["permutation"] == [2, 1]
        
        # Test parsing with all fields
        shape2, dim_names2, permutation2 = parse_tensor_metadata(metadata_json2)
        @test shape2 == [2, 2]
        @test dim_names2 == (:rows, :cols)
        @test permutation2 == (2, 1)
    end
    
    @testset "Error Handling" begin
        # Test invalid shapes
        mock_parent = Arrow.MockFixedSizeList{Float64}([1.0, 2.0], 2)
        @test_throws ArgumentError DenseTensor{Float64,2}(mock_parent, (2, 2))  # Shape doesn't match
        
        # Test invalid permutation
        @test_throws ArgumentError DenseTensor{Float64,2}(
            mock_parent, (1, 2), nothing, (1, 3)  # Invalid permutation
        )
        
        # Test invalid metadata
        @test_throws ArgumentError parse_tensor_metadata("{}")  # Missing shape
        @test_throws ArgumentError parse_tensor_metadata("invalid json")
    end
    
    @testset "Display" begin
        # Test string representation
        data = [1 2; 3 4]
        tensor = DenseTensor(data)
        
        str_repr = string(tensor)
        @test occursin("DenseTensor{Int64,2}", str_repr)
        @test occursin("2×2", str_repr)
        
        # Test pretty printing
        io = IOBuffer()
        show(io, MIME"text/plain"(), tensor)
        pretty_str = String(take!(io))
        @test occursin("2×2 DenseTensor{Int64,2}:", pretty_str)
    end
    
    @testset "Different Element Types" begin
        # Test with different numeric types
        for T in [Int32, Float32, Float64, ComplexF64]
            data = T[1 2; 3 4]
            tensor = DenseTensor(data)
            @test eltype(tensor) == T
            @test size(tensor) == (2, 2)
            @test tensor[1, 1] == T(1)
        end
    end
    
    @testset "Large Tensors" begin
        # Test with larger tensor to ensure performance is reasonable
        large_data = reshape(1:1000, (10, 10, 10))
        tensor = DenseTensor(large_data)
        
        @test size(tensor) == (10, 10, 10)
        @test tensor[5, 5, 5] == large_data[5, 5, 5]
        @test tensor[10, 10, 10] == 1000
        
        # Test that display doesn't show all elements for large tensors
        io = IOBuffer()
        show(io, MIME"text/plain"(), tensor)
        display_str = String(take!(io))
        @test occursin("1000 elements", display_str)
    end
    
    @testset "Edge Cases" begin
        # Test 1D tensor (vector)
        vec_data = [1, 2, 3]
        vec_tensor = DenseTensor(vec_data)
        @test size(vec_tensor) == (3,)
        @test vec_tensor[2] == 2
        
        # Test single element tensor
        scalar_data = reshape([42], (1,))
        scalar_tensor = DenseTensor(scalar_data)
        @test size(scalar_tensor) == (1,)
        @test scalar_tensor[1] == 42
        
        # Test empty dimensions (where applicable)
        # Note: Julia doesn't allow 0-dimensional arrays easily, so we skip this
    end
    
    # Skip the round-trip serialization tests for now since we need proper
    # FixedSizeList integration for that to work
    # @testset "Arrow Serialization Round-trip" begin
    #     # This will be implemented once FixedSizeList integration is complete
    # end
end