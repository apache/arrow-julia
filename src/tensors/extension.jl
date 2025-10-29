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
Extension type registration for Arrow tensors.

This file implements the ArrowTypes interface to register dense tensors
as Arrow extension types, enabling automatic serialization/deserialization
when working with Arrow data that contains tensor columns.
"""

using ArrowTypes

# Extension type name constants
const FIXED_SHAPE_TENSOR = Symbol("arrow.fixed_shape_tensor")

"""
Register DenseTensor as an Arrow extension type with the canonical name "arrow.fixed_shape_tensor".
"""

# Define how DenseTensor should be serialized to Arrow
ArrowTypes.ArrowType(::Type{<:DenseTensor}) = Arrow.FixedSizeList

# Note: ArrowKind for FixedSizeList might need to be defined elsewhere in Arrow.jl
# For now, we'll use the default StructKind behavior

# Define the extension name
ArrowTypes.arrowname(::Type{<:DenseTensor}) = FIXED_SHAPE_TENSOR

# Define metadata serialization 
function ArrowTypes.arrowmetadata(::Type{DenseTensor{T,N}}) where {T,N}
    # For now, we'll store minimal metadata since most info is in the JSON extension metadata
    return string(N)  # Store number of dimensions
end

# Define conversion from DenseTensor to FixedSizeList for serialization
function ArrowTypes.toarrow(tensor::DenseTensor{T,N}) where {T,N}
    return tensor.parent
end

# Define deserialization: how to convert Arrow data back to DenseTensor
function ArrowTypes.JuliaType(::Val{FIXED_SHAPE_TENSOR}, ::Type{Arrow.FixedSizeList{T}}, arrowmetadata::String) where {T}
    # The number of dimensions is stored in arrowmetadata
    N = parse(Int, arrowmetadata)
    return DenseTensor{T,N}
end

# Define actual conversion from FixedSizeList to DenseTensor
function ArrowTypes.fromarrow(::Type{DenseTensor{T,N}}, fixed_list::Arrow.FixedSizeList{T}, extension_metadata::String) where {T,N}
    # Parse the full tensor metadata from extension_metadata JSON
    return from_arrow_tensor(fixed_list, extension_metadata)
end

"""
    register_tensor_extensions()

Register tensor extension types with the Arrow system.
This should be called during module initialization.
"""
function register_tensor_extensions()
    # The registration happens automatically when the methods above are defined
    # This function exists for explicit initialization if needed
    @debug "Dense tensor extension type registered: $(FIXED_SHAPE_TENSOR)"
    return nothing
end