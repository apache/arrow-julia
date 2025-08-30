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
    CArrowSchema

C-compatible struct that mirrors the ArrowSchema structure from the 
Arrow C Data Interface specification. Based on original research into
ABI compatibility requirements for cross-language Arrow data sharing.

This struct describes the metadata of an Arrow array including its 
data type, nullability, and nested structure with precise memory 
layout matching the Arrow C specification.

The struct layout must exactly match the C definition to ensure
ABI compatibility across language boundaries. Research conducted
into optimal Julia struct design for C interoperability.

Fields:
- `format::Ptr{Cchar}` - Format string encoding the data type
- `name::Ptr{Cchar}` - Field name (can be NULL)
- `metadata::Ptr{Cchar}` - Custom metadata (can be NULL)  
- `flags::Int64` - Bitfield for properties (nullable, dictionary ordered, etc.)
- `n_children::Int64` - Number of child schemas for nested types
- `children::Ptr{Ptr{CArrowSchema}}` - Array of pointers to child schemas
- `dictionary::Ptr{CArrowSchema}` - Dictionary schema for dict-encoded arrays
- `release::Ptr{Cvoid}` - Function pointer for memory cleanup
- `private_data::Ptr{Cvoid}` - Producer-specific data
"""
mutable struct CArrowSchema
    format::Ptr{Cchar}
    name::Ptr{Cchar}
    metadata::Ptr{Cchar}
    flags::Int64
    n_children::Int64
    children::Ptr{Ptr{CArrowSchema}}
    dictionary::Ptr{CArrowSchema}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}

    # Default constructor for uninitialized struct
    CArrowSchema() = new()
end

"""
    CArrowArray

C-compatible struct that mirrors the ArrowArray structure from the
Arrow C Data Interface specification. This struct contains pointers
to the actual data buffers that make up an Arrow array.

The struct layout must exactly match the C definition to ensure 
ABI compatibility across language boundaries.

Fields:
- `length::Int64` - Number of logical elements in the array
- `null_count::Int64` - Number of null elements (-1 if unknown)
- `offset::Int64` - Logical offset into the data buffers
- `n_buffers::Int64` - Number of data buffers
- `buffers::Ptr{Ptr{Cvoid}}` - Array of pointers to data buffers
- `n_children::Int64` - Number of child arrays for nested types
- `children::Ptr{Ptr{CArrowArray}}` - Array of pointers to child arrays
- `dictionary::Ptr{CArrowArray}` - Dictionary array for dict-encoded data
- `release::Ptr{Cvoid}` - Function pointer for memory cleanup
- `private_data::Ptr{Cvoid}` - Producer-specific data
"""
mutable struct CArrowArray
    length::Int64
    null_count::Int64
    offset::Int64
    n_buffers::Int64
    buffers::Ptr{Ptr{Cvoid}}
    n_children::Int64
    children::Ptr{Ptr{CArrowArray}}
    dictionary::Ptr{CArrowArray}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}

    # Default constructor for uninitialized struct
    CArrowArray() = new()
end

"""
    GuardianObject

Internal object used to prevent garbage collection of Julia data
while it's being accessed through the C Data Interface. Holds
strong references to all underlying buffers and arrays.
"""
mutable struct GuardianObject
    # References to keep data alive
    arrow_vector::ArrowVector
    buffers::Vector{Any}  # Raw buffer references
    children::Vector{Any}  # Child guardian objects
    
    GuardianObject(av::ArrowVector) = new(av, Any[], Any[])
end

"""
    ImportedArrayHandle

Handle object for managing foreign memory imported via the C Data Interface.
Stores the original C pointers and ensures the producer's release callback
is called when Julia no longer needs the data.
"""
mutable struct ImportedArrayHandle
    array_ptr::Ptr{CArrowArray}
    schema_ptr::Ptr{CArrowSchema}
    
    function ImportedArrayHandle(array_ptr::Ptr{CArrowArray}, schema_ptr::Ptr{CArrowSchema})
        handle = new(array_ptr, schema_ptr)
        # Attach finalizer to call release callbacks when handle is GC'd
        finalizer(_release_imported_data, handle)
        return handle
    end
end

"""
    _release_imported_data(handle::ImportedArrayHandle)

Finalizer function that calls the producer's release callbacks
for imported C Data Interface objects.
"""
function _release_imported_data(handle::ImportedArrayHandle)
    # Call release callback for array if it exists
    if handle.array_ptr != C_NULL
        array = unsafe_load(handle.array_ptr)
        if array.release != C_NULL
            ccall(array.release, Cvoid, (Ptr{CArrowArray},), handle.array_ptr)
        end
    end
    
    # Call release callback for schema if it exists  
    if handle.schema_ptr != C_NULL
        schema = unsafe_load(handle.schema_ptr)
        if schema.release != C_NULL
            ccall(schema.release, Cvoid, (Ptr{CArrowSchema},), handle.schema_ptr)
        end
    end
end