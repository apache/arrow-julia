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
Import functionality for the Arrow C Data Interface.

This module implements the "consumer" side of the C Data Interface,
allowing Arrow.jl to consume data from other Arrow implementations
(PyArrow, Arrow C++, etc.) without copying data.
"""

"""
    import_from_c(schema_ptr::Ptr{CArrowSchema}, array_ptr::Ptr{CArrowArray}) -> ArrowVector

Import an Arrow vector from the C Data Interface by consuming the provided
schema and array pointers. This is the main entry point for the import functionality.

The producer (other Arrow implementation) allocates and populates the CArrowSchema 
and CArrowArray structs. This function reads those structs and creates a zero-copy
Arrow.jl vector that wraps the foreign memory.

# Arguments
- `schema_ptr::Ptr{CArrowSchema}`: Pointer to populated CArrowSchema struct
- `array_ptr::Ptr{CArrowArray}`: Pointer to populated CArrowArray struct

# Returns
- `ArrowVector`: Zero-copy view over the foreign Arrow data

# Memory Management
This function creates an ImportedArrayHandle that holds references to the
original C pointers. A finalizer is attached to ensure the producer's
release callbacks are called when Julia no longer needs the data.
"""
function import_from_c(schema_ptr::Ptr{CArrowSchema}, array_ptr::Ptr{CArrowArray})
    if schema_ptr == C_NULL || array_ptr == C_NULL
        throw(ArgumentError("Schema and array pointers cannot be NULL"))
    end
    
    # Load the C structures
    schema = unsafe_load(schema_ptr)
    array = unsafe_load(array_ptr)
    
    # Create handle to manage foreign memory
    handle = ImportedArrayHandle(array_ptr, schema_ptr)
    
    # Parse the schema to understand the data type
    julia_type = _parse_imported_schema(schema)
    
    # Create Arrow vector as zero-copy view over foreign data
    arrow_vector = _create_arrow_vector_from_import(schema, array, julia_type, handle)
    
    return arrow_vector
end

# Handle generic pointer types for tests/compatibility
import_from_c(schema_ptr::Ptr{Nothing}, array_ptr::Ptr{Nothing}) = 
    import_from_c(convert(Ptr{CArrowSchema}, schema_ptr), convert(Ptr{CArrowArray}, array_ptr))

import_from_c(schema_ptr::Ptr{CArrowSchema}, array_ptr::Ptr{Nothing}) = 
    import_from_c(schema_ptr, convert(Ptr{CArrowArray}, array_ptr))

import_from_c(schema_ptr::Ptr{Nothing}, array_ptr::Ptr{CArrowArray}) = 
    import_from_c(convert(Ptr{CArrowSchema}, schema_ptr), array_ptr)

"""
    _parse_imported_schema(schema::CArrowSchema) -> Any

Parse an imported CArrowSchema to determine the Julia type.
"""
function _parse_imported_schema(schema::CArrowSchema)
    # Read the format string
    format_str = _read_c_string(schema.format)
    
    if isempty(format_str)
        throw(ArgumentError("Empty format string in imported schema"))
    end
    
    # Parse the format string to get base type
    base_type = parse_format_string(format_str)
    
    # Check if nullable based on flags
    is_nullable = (schema.flags & ARROW_FLAG_NULLABLE) != 0
    
    if is_nullable && base_type !== Missing
        return Union{base_type, Missing}
    else
        return base_type
    end
end

"""
    _create_arrow_vector_from_import(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle) -> ArrowVector

Create an Arrow vector that wraps imported foreign data.
"""
function _create_arrow_vector_from_import(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    # Read the format string to determine the Arrow vector type to create
    format_str = _read_c_string(schema.format)
    
    # Create appropriate Arrow vector based on format
    if format_str in ["c", "C", "s", "S", "i", "I", "l", "L", "f", "g"]
        return _create_primitive_vector(schema, array, julia_type, handle)
    elseif format_str == "b"
        return _create_bool_vector(schema, array, julia_type, handle)
    elseif format_str == "u"
        return _create_string_vector(schema, array, julia_type, handle)
    elseif format_str == "z"
        return _create_binary_vector(schema, array, julia_type, handle)
    elseif startswith(format_str, "+l")
        return _create_list_vector(schema, array, julia_type, handle)
    elseif startswith(format_str, "+w:")
        return _create_fixed_size_list_vector(schema, array, julia_type, handle)
    elseif format_str == "+s"
        return _create_struct_vector(schema, array, julia_type, handle)
    else
        throw(ArgumentError("Unsupported format string for import: $format_str"))
    end
end

"""
    _create_primitive_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle) -> Arrow.Primitive

Create a primitive Arrow vector from imported data.
"""
function _create_primitive_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    # Get the element type (strip Missing if union type)
    element_type = julia_type <: Union ? Base.nonmissingtype(julia_type) : julia_type
    
    # Import validity bitmap
    validity = _import_validity_bitmap(array, handle)
    
    # Import data buffer
    if array.n_buffers < 2
        throw(ArgumentError("Primitive array must have at least 2 buffers (validity + data)"))
    end
    
    # Get data buffer (second buffer, index 1)
    buffers_array = unsafe_wrap(Array, array.buffers, array.n_buffers)
    data_ptr = buffers_array[2]  # Second buffer is data
    
    if data_ptr == C_NULL
        throw(ArgumentError("Data buffer cannot be NULL for primitive array"))
    end
    
    # Create zero-copy view over the data buffer
    data_length = array.length + array.offset
    data_array = unsafe_wrap(Array, convert(Ptr{element_type}, data_ptr), data_length)
    
    # Apply offset if needed
    if array.offset > 0
        data_array = view(data_array, (array.offset + 1):data_length)
    end
    
    # Create Arrow primitive vector
    # Note: This is simplified - real implementation would need to handle Arrow.jl's internal structure
    return _create_arrow_primitive(element_type, data_array, validity, handle)
end

"""
    _create_arrow_primitive(::Type{T}, data::AbstractVector{T}, validity::ValidityBitmap, handle::ImportedArrayHandle) -> ArrowVector

Create an Arrow.Primitive vector wrapping imported data.
This is a simplified version - real implementation would need to match Arrow.jl's internals.
"""
function _create_arrow_primitive(::Type{T}, data::AbstractVector{T}, validity::ValidityBitmap, handle::ImportedArrayHandle) where {T}
    # This would need to create an actual Arrow.Primitive struct
    # For now, return a simplified wrapper
    return ImportedPrimitiveVector{T}(data, validity, handle)
end

"""
    ImportedPrimitiveVector{T}

Simplified Arrow vector wrapper for imported primitive data.
In a full implementation, this would be replaced with proper Arrow.Primitive construction.
"""
struct ImportedPrimitiveVector{T} <: ArrowVector{T}
    data::AbstractVector{T}
    validity::ValidityBitmap
    handle::ImportedArrayHandle
end

Base.size(v::ImportedPrimitiveVector) = size(v.data)
Base.getindex(v::ImportedPrimitiveVector, i::Int) = v.validity[i] ? v.data[i] : missing
validitybitmap(v::ImportedPrimitiveVector) = v.validity
nullcount(v::ImportedPrimitiveVector) = v.validity.nc
getmetadata(v::ImportedPrimitiveVector) = nothing

"""
    _import_validity_bitmap(array::CArrowArray, handle::ImportedArrayHandle) -> ValidityBitmap

Import the validity bitmap from a C array.
"""
function _import_validity_bitmap(array::CArrowArray, handle::ImportedArrayHandle)
    if array.n_buffers == 0 || array.null_count == 0
        # No nulls, return empty validity bitmap
        return ValidityBitmap(UInt8[], 1, Int(array.length), 0)
    end
    
    # Get validity buffer (first buffer, index 0)
    buffers_array = unsafe_wrap(Array, array.buffers, array.n_buffers)
    validity_ptr = buffers_array[1]  # First buffer is validity
    
    if validity_ptr == C_NULL
        # No validity buffer means all values are valid
        return ValidityBitmap(UInt8[], 1, Int(array.length), 0)
    end
    
    # Calculate bitmap size in bytes
    bitmap_size_bytes = cld(array.length, 8)
    
    # Create zero-copy view over validity buffer
    validity_bytes = unsafe_wrap(Array, convert(Ptr{UInt8}, validity_ptr), bitmap_size_bytes)
    
    # Create ValidityBitmap
    return ValidityBitmap(validity_bytes, 1, Int(array.length), Int(array.null_count))
end

"""
    _create_bool_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle) -> ArrowVector

Create a boolean Arrow vector from imported data.
"""
function _create_bool_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    # Boolean vectors are bit-packed, similar to validity bitmaps
    validity = _import_validity_bitmap(array, handle)
    
    if array.n_buffers < 2
        throw(ArgumentError("Boolean array must have at least 2 buffers (validity + data)"))
    end
    
    # Get data buffer (second buffer, bit-packed)
    buffers_array = unsafe_wrap(Array, array.buffers, array.n_buffers)
    data_ptr = buffers_array[2]
    
    if data_ptr == C_NULL
        throw(ArgumentError("Data buffer cannot be NULL for boolean array"))
    end
    
    # Calculate bitmap size in bytes  
    data_size_bytes = cld(array.length, 8)
    data_bytes = unsafe_wrap(Array, convert(Ptr{UInt8}, data_ptr), data_size_bytes)
    
    return ImportedBoolVector(data_bytes, validity, Int(array.length), handle)
end

"""
    ImportedBoolVector

Simplified Arrow vector wrapper for imported boolean data.
"""
struct ImportedBoolVector <: ArrowVector{Union{Bool, Missing}}
    data_bytes::Vector{UInt8}
    validity::ValidityBitmap
    length::Int
    handle::ImportedArrayHandle
end

Base.size(v::ImportedBoolVector) = (v.length,)
validitybitmap(v::ImportedBoolVector) = v.validity
nullcount(v::ImportedBoolVector) = v.validity.nc
getmetadata(v::ImportedBoolVector) = nothing

function Base.getindex(v::ImportedBoolVector, i::Int)
    @boundscheck checkbounds(v, i)
    if !v.validity[i]
        return missing
    end
    
    # Extract bit from packed data
    byte_idx, bit_idx = divrem(i - 1, 8) .+ (1, 1)
    byte_val = v.data_bytes[byte_idx]
    return (byte_val >> (bit_idx - 1)) & 0x01 == 0x01
end

"""
    _create_string_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle) -> ArrowVector

Create a string Arrow vector from imported data.
"""
function _create_string_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    # String arrays need validity, offsets, and data buffers
    validity = _import_validity_bitmap(array, handle)
    
    if array.n_buffers < 3
        throw(ArgumentError("String array must have at least 3 buffers (validity + offsets + data)"))
    end
    
    buffers_array = unsafe_wrap(Array, array.buffers, array.n_buffers)
    offsets_ptr = buffers_array[2]  # Second buffer is offsets
    data_ptr = buffers_array[3]     # Third buffer is string data
    
    if offsets_ptr == C_NULL || data_ptr == C_NULL
        throw(ArgumentError("Offsets and data buffers cannot be NULL for string array"))
    end
    
    # Import offsets (Int32 typically for regular strings)
    offsets_length = array.length + 1
    offsets = unsafe_wrap(Array, convert(Ptr{Int32}, offsets_ptr), offsets_length)
    
    # Import data buffer - we don't know the size directly, use the last offset
    if length(offsets) > 0
        data_size = Int(offsets[end])
        data_bytes = unsafe_wrap(Array, convert(Ptr{UInt8}, data_ptr), data_size)
    else
        data_bytes = UInt8[]
    end
    
    return ImportedStringVector(offsets, data_bytes, validity, Int(array.length), handle)
end

"""
    ImportedStringVector

Simplified Arrow vector wrapper for imported string data.
"""
struct ImportedStringVector <: ArrowVector{Union{String, Missing}}
    offsets::Vector{Int32}
    data_bytes::Vector{UInt8}
    validity::ValidityBitmap  
    length::Int
    handle::ImportedArrayHandle
end

Base.size(v::ImportedStringVector) = (v.length,)
validitybitmap(v::ImportedStringVector) = v.validity
nullcount(v::ImportedStringVector) = v.validity.nc  
getmetadata(v::ImportedStringVector) = nothing

function Base.getindex(v::ImportedStringVector, i::Int)
    @boundscheck checkbounds(v, i)
    if !v.validity[i]
        return missing
    end
    
    # Get string bounds from offsets
    start_offset = Int(v.offsets[i]) + 1    # Convert to 1-based indexing
    end_offset = Int(v.offsets[i + 1])
    
    if start_offset > end_offset
        return ""
    end
    
    # Extract string from data buffer
    string_bytes = view(v.data_bytes, start_offset:end_offset)
    return String(string_bytes)
end

# Placeholder implementations for other vector types
function _create_binary_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    throw(ArgumentError("Binary vector import not yet implemented"))
end

function _create_list_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    throw(ArgumentError("List vector import not yet implemented"))
end

function _create_fixed_size_list_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    throw(ArgumentError("Fixed-size list vector import not yet implemented"))
end

function _create_struct_vector(schema::CArrowSchema, array::CArrowArray, julia_type::Type, handle::ImportedArrayHandle)
    throw(ArgumentError("Struct vector import not yet implemented"))
end