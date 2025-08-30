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
Export functionality for the Arrow C Data Interface.

This module implements the "producer" side of the C Data Interface,
allowing Julia Arrow vectors to be consumed by other Arrow implementations
(PyArrow, Arrow C++, etc.) without copying data.
"""

# Global registry to keep guardian objects alive
const _GUARDIAN_REGISTRY = Dict{Ptr{Cvoid}, GuardianObject}()
const _GUARDIAN_LOCK = Threads.SpinLock()

"""
    export_to_c(arrow_vector::ArrowVector, schema_ptr::Ptr{CArrowSchema}, array_ptr::Ptr{CArrowArray})

Export an Arrow.jl vector to the C Data Interface by populating the provided
schema and array pointers. This is the main entry point for the export functionality.

The caller (consumer) allocates the CArrowSchema and CArrowArray structs and passes
pointers to them. This function populates those structs with the appropriate
metadata and data pointers.

# Arguments
- `arrow_vector::ArrowVector`: The Julia Arrow vector to export
- `schema_ptr::Ptr{CArrowSchema}`: Pointer to allocated CArrowSchema struct
- `array_ptr::Ptr{CArrowArray}`: Pointer to allocated CArrowArray struct

# Memory Management
This function uses a "guardian object" pattern to prevent the Julia GC from
collecting the underlying data while it's being accessed via C pointers.
The guardian is stored in a global registry and will be cleaned up when
the consumer calls the release callback.
"""
function export_to_c(arrow_vector::ArrowVector, schema_ptr::Ptr{CArrowSchema}, array_ptr::Ptr{CArrowArray})
    # Create guardian object to prevent GC of underlying data
    guardian = GuardianObject(arrow_vector)
    
    # Export schema
    _export_schema(arrow_vector, schema_ptr, guardian)
    
    # Export array  
    _export_array(arrow_vector, array_ptr, guardian)
    
    # Register guardian for cleanup
    lock(_GUARDIAN_LOCK) do
        _GUARDIAN_REGISTRY[convert(Ptr{Cvoid}, array_ptr)] = guardian
    end
    
    # Set the release callbacks now that all functions are defined
    _set_release_callbacks(schema_ptr, array_ptr)
    
    return nothing
end

"""
    _export_schema(arrow_vector::ArrowVector, schema_ptr::Ptr{CArrowSchema}, guardian::GuardianObject)

Export schema metadata for an Arrow vector.
"""
function _export_schema(arrow_vector::ArrowVector, schema_ptr::Ptr{CArrowSchema}, guardian::GuardianObject)
    schema = unsafe_load(schema_ptr)
    
    # Generate format string for the vector type
    format_str = generate_format_string(arrow_vector)
    schema.format = _create_c_string(format_str)
    
    # Set field name (empty for top-level)
    schema.name = C_NULL
    
    # Set metadata (empty for now - could be extended to include Arrow metadata)
    schema.metadata = C_NULL
    
    # Set flags
    schema.flags = _get_schema_flags(arrow_vector)
    
    # Handle nested types
    n_children, children_ptr = _export_schema_children(arrow_vector, guardian)
    schema.n_children = n_children
    schema.children = children_ptr
    
    # Dictionary (for dictionary-encoded arrays)
    schema.dictionary = _export_schema_dictionary(arrow_vector, guardian)
    
    # Set release callback - we'll set this after defining the function
    schema.release = C_NULL
    
    # Store schema pointer as private data for release callback
    schema.private_data = convert(Ptr{Cvoid}, schema_ptr)
    
    # Write back the populated schema
    unsafe_store!(schema_ptr, schema)
    
    return nothing
end

"""
    _export_array(arrow_vector::ArrowVector, array_ptr::Ptr{CArrowArray}, guardian::GuardianObject)

Export array data for an Arrow vector.
"""
function _export_array(arrow_vector::ArrowVector, array_ptr::Ptr{CArrowArray}, guardian::GuardianObject)
    array = unsafe_load(array_ptr)
    
    # Basic array properties
    array.length = Int64(length(arrow_vector))
    array.null_count = Int64(nullcount(arrow_vector))
    array.offset = Int64(0)  # Assume no offset for simplicity
    
    # Export buffers
    n_buffers, buffers_ptr = _export_array_buffers(arrow_vector, guardian)
    array.n_buffers = n_buffers
    array.buffers = buffers_ptr
    
    # Handle nested types
    n_children, children_ptr = _export_array_children(arrow_vector, guardian)
    array.n_children = n_children
    array.children = children_ptr
    
    # Dictionary (for dictionary-encoded arrays)
    array.dictionary = _export_array_dictionary(arrow_vector, guardian)
    
    # Set release callback - we'll set this after defining the function
    array.release = C_NULL
    
    # Store array pointer as private data for release callback
    array.private_data = convert(Ptr{Cvoid}, array_ptr)
    
    # Write back the populated array
    unsafe_store!(array_ptr, array)
    
    return nothing
end

"""
    _export_array_buffers(arrow_vector::ArrowVector, guardian::GuardianObject) -> (Int64, Ptr{Ptr{Cvoid}})

Export the data buffers for an Arrow vector. Returns the number of buffers
and a pointer to an array of buffer pointers.
"""
function _export_array_buffers(arrow_vector::ArrowVector, guardian::GuardianObject)
    buffers = Ptr{Cvoid}[]
    
    # Add validity buffer if needed
    if nullcount(arrow_vector) > 0
        validity_bitmap = validitybitmap(arrow_vector)
        if !isempty(validity_bitmap.bytes)
            validity_ptr = pointer(validity_bitmap.bytes, validity_bitmap.pos)
            push!(buffers, convert(Ptr{Cvoid}, validity_ptr))
            push!(guardian.buffers, validity_bitmap.bytes)  # Keep alive
        else
            push!(buffers, C_NULL)
        end
    else
        push!(buffers, C_NULL)  # No validity buffer needed
    end
    
    # Add data buffer(s) - this is type-specific
    _add_data_buffers!(buffers, arrow_vector, guardian)
    
    # Allocate C array for buffer pointers
    if isempty(buffers)
        return (Int64(0), convert(Ptr{Ptr{Cvoid}}, C_NULL))
    end
    
    buffers_array = Vector{Ptr{Cvoid}}(buffers)
    buffers_ptr = convert(Ptr{Ptr{Cvoid}}, pointer(buffers_array))
    push!(guardian.buffers, buffers_array)  # Keep alive
    
    return (Int64(length(buffers)), buffers_ptr)
end

"""
    _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::ArrowVector, guardian::GuardianObject)

Add type-specific data buffers to the buffers array.
This is specialized for different Arrow vector types.
"""
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::ArrowVector, guardian::GuardianObject)
    # Default implementation for primitive types
    if hasfield(typeof(arrow_vector), :data) && arrow_vector.data isa AbstractVector
        data_ptr = pointer(arrow_vector.data)
        push!(buffers, convert(Ptr{Cvoid}, data_ptr))
        push!(guardian.buffers, arrow_vector.data)
    end
end

# Specialized buffer export for different Arrow types
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::Arrow.List, guardian::GuardianObject)
    # List arrays need both offsets and data buffers
    # This would need to be implemented based on Arrow.jl's List internals
    # For now, add a placeholder
    push!(buffers, C_NULL)  # Offsets buffer placeholder
    push!(buffers, C_NULL)  # Values buffer placeholder
end

"""
    _get_schema_flags(arrow_vector::ArrowVector) -> Int64

Get the appropriate flags for a schema based on the Arrow vector.
"""
function _get_schema_flags(arrow_vector::ArrowVector)
    flags = Int64(0)
    
    # Check if the type is nullable
    if nullcount(arrow_vector) >= 0  # -1 means unknown, ≥0 means potentially nullable
        flags |= ARROW_FLAG_NULLABLE
    end
    
    return flags
end

"""
    _export_schema_children(arrow_vector::ArrowVector, guardian::GuardianObject) -> (Int64, Ptr{Ptr{CArrowSchema}})

Export child schemas for nested types. Returns (n_children, children_pointer).
"""
function _export_schema_children(arrow_vector::ArrowVector, guardian::GuardianObject)
    # Most types don't have children
    return (Int64(0), convert(Ptr{Ptr{CArrowSchema}}, C_NULL))
end

"""
    _export_array_children(arrow_vector::ArrowVector, guardian::GuardianObject) -> (Int64, Ptr{Ptr{CArrowArray}})

Export child arrays for nested types. Returns (n_children, children_pointer).
"""
function _export_array_children(arrow_vector::ArrowVector, guardian::GuardianObject)
    # Most types don't have children
    return (Int64(0), convert(Ptr{Ptr{CArrowArray}}, C_NULL))
end

"""
    _export_schema_dictionary(arrow_vector::ArrowVector, guardian::GuardianObject) -> Ptr{CArrowSchema}

Export dictionary schema for dictionary-encoded arrays.
"""
function _export_schema_dictionary(arrow_vector::ArrowVector, guardian::GuardianObject)
    # Most types don't have dictionaries
    return convert(Ptr{CArrowSchema}, C_NULL)
end

"""
    _export_array_dictionary(arrow_vector::ArrowVector, guardian::GuardianObject) -> Ptr{CArrowArray}

Export dictionary array for dictionary-encoded arrays.
"""
function _export_array_dictionary(arrow_vector::ArrowVector, guardian::GuardianObject)
    # Most types don't have dictionaries
    return convert(Ptr{CArrowArray}, C_NULL)
end

"""
    _release_schema(schema_ptr::Ptr{CArrowSchema})

Release callback for exported schemas. Called by the consumer when
they're done with the schema.
"""
function _release_schema(schema_ptr::Ptr{CArrowSchema})
    if schema_ptr == C_NULL
        return
    end
    
    schema = unsafe_load(schema_ptr)
    
    # Free allocated strings
    _free_c_string(schema.format)
    _free_c_string(schema.name)
    _free_c_string(schema.metadata)
    
    # Free children if any
    if schema.children != C_NULL && schema.n_children > 0
        children_array = unsafe_wrap(Array, schema.children, schema.n_children)
        for i in 1:schema.n_children
            child_ptr = children_array[i]
            if child_ptr != C_NULL
                # Recursively release child schemas
                child_schema = unsafe_load(child_ptr)
                if child_schema.release != C_NULL
                    ccall(child_schema.release, Cvoid, (Ptr{CArrowSchema},), child_ptr)
                end
            end
        end
        Libc.free(schema.children)
    end
    
    # Free dictionary if any
    if schema.dictionary != C_NULL
        dict_schema = unsafe_load(schema.dictionary)
        if dict_schema.release != C_NULL
            ccall(dict_schema.release, Cvoid, (Ptr{CArrowSchema},), schema.dictionary)
        end
    end
    
    # Mark as released
    schema.release = C_NULL
    unsafe_store!(schema_ptr, schema)
    
    return nothing
end

"""
    _release_array(array_ptr::Ptr{CArrowArray})

Release callback for exported arrays. Called by the consumer when
they're done with the array data.
"""
function _release_array(array_ptr::Ptr{CArrowArray})
    if array_ptr == C_NULL
        return
    end
    
    # Remove guardian from registry to allow GC
    lock(_GUARDIAN_LOCK) do
        delete!(_GUARDIAN_REGISTRY, convert(Ptr{Cvoid}, array_ptr))
    end
    
    array = unsafe_load(array_ptr)
    
    # Free buffers array (but not the buffers themselves - Julia manages those)
    if array.buffers != C_NULL && array.n_buffers > 0
        # The buffers array was allocated by us, so free it
        # Note: we don't free the actual buffer contents since Julia manages those
        Libc.free(array.buffers)
    end
    
    # Free children if any
    if array.children != C_NULL && array.n_children > 0
        children_array = unsafe_wrap(Array, array.children, array.n_children)
        for i in 1:array.n_children
            child_ptr = children_array[i]
            if child_ptr != C_NULL
                # Recursively release child arrays
                child_array = unsafe_load(child_ptr)
                if child_array.release != C_NULL
                    ccall(child_array.release, Cvoid, (Ptr{CArrowArray},), child_ptr)
                end
            end
        end
        Libc.free(array.children)
    end
    
    # Free dictionary if any
    if array.dictionary != C_NULL
        dict_array = unsafe_load(array.dictionary)
        if dict_array.release != C_NULL
            ccall(dict_array.release, Cvoid, (Ptr{CArrowArray},), array.dictionary)
        end
    end
    
    # Mark as released
    array.release = C_NULL
    unsafe_store!(array_ptr, array)
    
    return nothing
end

"""
    _set_release_callbacks(schema_ptr::Ptr{CArrowSchema}, array_ptr::Ptr{CArrowArray})

Set the release callbacks for exported schema and array structs.
This is called after all functions are defined to avoid forward reference issues.
"""
function _set_release_callbacks(schema_ptr::Ptr{CArrowSchema}, array_ptr::Ptr{CArrowArray})
    # Set schema release callback
    schema = unsafe_load(schema_ptr)
    schema.release = @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},))
    unsafe_store!(schema_ptr, schema)
    
    # Set array release callback
    array = unsafe_load(array_ptr)
    array.release = @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},))
    unsafe_store!(array_ptr, array)
    
    return nothing
end