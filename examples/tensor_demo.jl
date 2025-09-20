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
Arrow.jl Dense Tensor Demo

This example demonstrates the dense tensor functionality in Arrow.jl,
showcasing the canonical arrow.fixed_shape_tensor extension type.

Key features demonstrated:
- Creating DenseTensor objects from Julia arrays
- Multi-dimensional indexing and AbstractArray interface
- JSON metadata generation and parsing
- Extension type registration for Arrow interoperability

The dense tensor implementation provides a zero-copy wrapper around
Arrow FixedSizeList data with multi-dimensional semantics.
"""

using Arrow
using Arrow: DenseTensor, tensor_metadata, parse_tensor_metadata

println("Arrow.jl Dense Tensor Demo")
println("=" ^ 30)

# Create tensors from Julia arrays
println("\n1. Creating Dense Tensors:")

# 1D tensor (vector)
vec_data = [1.0, 2.0, 3.0, 4.0, 5.0]
tensor_1d = DenseTensor(vec_data)
println("1D Tensor: $tensor_1d")
println("Size: $(size(tensor_1d)), Element [3]: $(tensor_1d[3])")

# 2D tensor (matrix)
mat_data = [1 2 3; 4 5 6; 7 8 9]
tensor_2d = DenseTensor(mat_data)
println("\n2D Tensor: $tensor_2d")
println("Size: $(size(tensor_2d)), Element [2,3]: $(tensor_2d[2,3])")

# 3D tensor 
tensor_3d_data = reshape(1:24, (2, 3, 4))
tensor_3d = DenseTensor(tensor_3d_data)
println("\n3D Tensor: $tensor_3d")
println("Size: $(size(tensor_3d)), Element [2,2,3]: $(tensor_3d[2,2,3])")

# Demonstrate AbstractArray interface
println("\n2. AbstractArray Interface:")
println("tensor_2d supports:")
println("  - size(tensor_2d) = $(size(tensor_2d))")
println("  - ndims(tensor_2d) = $(ndims(tensor_2d))")
println("  - length(tensor_2d) = $(length(tensor_2d))")
println("  - eltype(tensor_2d) = $(eltype(tensor_2d))")

# Test indexing and assignment
println("\nModifying elements:")
println("Before: tensor_2d[1,1] = $(tensor_2d[1,1])")
tensor_2d[1,1] = 99
println("After:  tensor_2d[1,1] = $(tensor_2d[1,1])")

# Demonstrate iteration
println("\nFirst 5 elements via iteration: $(collect(Iterators.take(tensor_2d, 5)))")

# JSON metadata generation and parsing
println("\n3. JSON Metadata System:")
metadata_json = tensor_metadata(tensor_2d)
println("Generated metadata: $metadata_json")

shape, dim_names, permutation = parse_tensor_metadata(metadata_json)
println("Parsed shape: $shape")
println("Parsed dim_names: $dim_names")
println("Parsed permutation: $permutation")

# Tensor with dimension names and permutation
println("\n4. Advanced Tensor Features:")
tensor_with_features = DenseTensor{Int,2}(
    tensor_2d.parent, 
    (3, 3), 
    (:rows, :columns),
    (2, 1)  # Transposed access pattern
)
println("Tensor with features: $tensor_with_features")

advanced_metadata = tensor_metadata(tensor_with_features)
println("Advanced metadata: $advanced_metadata")

shape2, dim_names2, permutation2 = parse_tensor_metadata(advanced_metadata)
println("Parsed dim_names: $dim_names2")
println("Parsed permutation: $permutation2")

# Different element types
println("\n5. Different Element Types:")
for T in [Int32, Float32, ComplexF64]
    data = T[1 2; 3 4]
    tensor = DenseTensor(data)
    println("$T tensor: size=$(size(tensor)), element_type=$(eltype(tensor))")
end

# Extension type information
println("\n6. Extension Type Registration:")
println("Extension name: $(Arrow.FIXED_SHAPE_TENSOR)")
try
    println("Arrow kind: $(ArrowTypes.ArrowKind(DenseTensor{Float64,2}))")
catch e
    println("Arrow kind: Default ($(typeof(e)))")
end
println("Arrow type: $(ArrowTypes.ArrowType(DenseTensor{Float64,2}))")

println("\nDemo completed successfully!")
println("\nNote: This demonstrates the foundational dense tensor functionality.")
println("Integration with Arrow serialization/deserialization requires")
println("proper FixedSizeList integration, which will be completed in") 
println("the full implementation.")