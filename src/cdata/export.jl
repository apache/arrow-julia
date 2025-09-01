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

# Schema export for ToList types (used as child arrays in Lists)
function _export_schema(tolist::Arrow.ToList, schema_ptr::Ptr{CArrowSchema}, guardian::GuardianObject)
    schema = unsafe_load(schema_ptr)
    
    # Generate format string based on ToList type and stringtype parameter
    T = eltype(tolist)
    if T == UInt8
        # Check if this is string data or binary data using the stringtype type parameter
        tolist_type = typeof(tolist)
        if length(tolist_type.parameters) >= 2 && tolist_type.parameters[2] == true
            # stringtype=true -> UTF-8 strings
            schema.format = _create_c_string("u")  # UTF-8 string format
        else
            # stringtype=false -> binary data  
            schema.format = _create_c_string("z")  # Binary data format
        end
    else
        # Generate format for the element type
        format_str = generate_format_string(T)
        schema.format = _create_c_string(format_str)
    end
    
    # Set field name (empty for child)
    schema.name = C_NULL
    
    # Set metadata (empty)
    schema.metadata = C_NULL
    
    # Set flags (assume nullable for ToList)
    schema.flags = ARROW_FLAG_NULLABLE
    
    # ToList doesn't have children
    schema.n_children = Int64(0)
    schema.children = convert(Ptr{Ptr{CArrowSchema}}, C_NULL)
    
    # No dictionary
    schema.dictionary = convert(Ptr{CArrowSchema}, C_NULL)
    
    # Set release callback later
    schema.release = C_NULL
    
    # Store schema pointer as private data
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

# Array export for ToList types (used as child arrays in Lists)
function _export_array(tolist::Arrow.ToList, array_ptr::Ptr{CArrowArray}, guardian::GuardianObject)
    array = unsafe_load(array_ptr)
    
    # Basic array properties
    # For string ToList, length should be number of strings, not bytes
    if eltype(tolist) == UInt8 && hasfield(typeof(tolist), :data)
        array.length = Int64(length(tolist.data))
    else
        array.length = Int64(length(tolist))
    end
    array.null_count = Int64(0)  # ToList handles nulls at the List level
    array.offset = Int64(0)
    
    # Export buffers based on ToList type
    buffers = Ptr{Cvoid}[]
    
    # Add validity buffer (null for ToList - handled by parent List)
    push!(buffers, C_NULL)
    
    # For UInt8 ToList (strings/binary), we need offsets + data buffers  
    if eltype(tolist) == UInt8
        # Add offsets buffer for UTF-8 string or binary array
        _add_string_offsets_buffer!(buffers, tolist, guardian)
    end
    
    # Add data buffer
    _add_data_buffers!(buffers, tolist, guardian)
    
    # Create buffer array
    if !isempty(buffers)
        buffers_array = Vector{Ptr{Cvoid}}(buffers)
        buffers_ptr = convert(Ptr{Ptr{Cvoid}}, pointer(buffers_array))
        push!(guardian.buffers, buffers_array)
        
        array.n_buffers = Int64(length(buffers))
        array.buffers = buffers_ptr
    else
        array.n_buffers = Int64(0)
        array.buffers = convert(Ptr{Ptr{Cvoid}}, C_NULL)
    end
    
    # ToList doesn't have children
    array.n_children = Int64(0)
    array.children = convert(Ptr{Ptr{CArrowArray}}, C_NULL)
    
    # No dictionary
    array.dictionary = convert(Ptr{CArrowArray}, C_NULL)
    
    # Set release callback later
    array.release = C_NULL
    
    # Store array pointer as private data
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
    if hasfield(typeof(arrow_vector), :data)
        data_field = arrow_vector.data
        # Only try to get pointer if it's a concrete array type that supports it
        if data_field isa Array || data_field isa Vector
            try
                data_ptr = pointer(data_field)
                push!(buffers, convert(Ptr{Cvoid}, data_ptr))
                push!(guardian.buffers, data_field)
            catch
                # If pointer conversion fails, add null buffer
                push!(buffers, C_NULL)
            end
        else
            # For complex types, add null buffer (data is handled by children)
            push!(buffers, C_NULL)
        end
    end
end

# Specialized buffer export for different Arrow types
# Specialized buffer export for List of strings (ToList{UInt8} child)  
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::Arrow.List{T,O,<:Arrow.ToList{UInt8}}, guardian::GuardianObject) where {T,O}
    # For List<String>, we need element indices [0,1,2,...] not byte offsets
    if hasfield(typeof(arrow_vector), :data) && hasfield(typeof(arrow_vector.data), :data)
        num_strings = length(arrow_vector.data.data)
        element_offsets = Vector{Int32}(0:num_strings)
        
        offsets_ptr = pointer(element_offsets)
        push!(buffers, convert(Ptr{Cvoid}, offsets_ptr))
        push!(guardian.buffers, element_offsets)
    else
        push!(buffers, C_NULL)
    end
end

# Specialized buffer export for List of binary (Primitive{UInt8, ToList} child)
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::Arrow.List{T,O,<:Arrow.Primitive{UInt8,<:Arrow.ToList{UInt8}}}, guardian::GuardianObject) where {T,O}
    # For List<Binary>, we need element indices [0,1,2,...] not byte offsets
    if hasfield(typeof(arrow_vector), :data) && hasfield(typeof(arrow_vector.data), :data) && hasfield(typeof(arrow_vector.data.data), :data)
        num_binaries = length(arrow_vector.data.data.data)  # Primitive -> ToList -> data
        element_offsets = Vector{Int32}(0:num_binaries)
        
        offsets_ptr = pointer(element_offsets)
        push!(buffers, convert(Ptr{Cvoid}, offsets_ptr))
        push!(guardian.buffers, element_offsets)
    else
        push!(buffers, C_NULL)
    end
end

function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::Arrow.List, guardian::GuardianObject)
    # List arrays need offsets buffer (values are handled as child array)
    if hasfield(typeof(arrow_vector), :offsets) && hasfield(typeof(arrow_vector.offsets), :offsets)
        offsets_ptr = pointer(arrow_vector.offsets.offsets)
        push!(buffers, convert(Ptr{Cvoid}, offsets_ptr))
        push!(guardian.buffers, arrow_vector.offsets.offsets)
    else
        push!(buffers, C_NULL)
    end
end

# Specialized buffer export for Struct types (no data buffers, just validity)
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::Arrow.Struct, guardian::GuardianObject)
    # Struct arrays don't have data buffers, only validity buffer which is handled separately
    return
end

# Specialized buffer export for Bool types
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, arrow_vector::Arrow.BoolVector, guardian::GuardianObject)
    # Boolean vectors use bit-packed data in the arrow field
    if hasfield(typeof(arrow_vector), :arrow) && hasfield(typeof(arrow_vector), :pos)
        # Get the actual data buffer starting from pos
        data_ptr = pointer(arrow_vector.arrow, arrow_vector.pos)
        push!(buffers, convert(Ptr{Cvoid}, data_ptr))
        push!(guardian.buffers, arrow_vector.arrow)
    else
        # Fallback to null buffer  
        push!(buffers, C_NULL)
    end
end

# Add offsets buffer for string ToList (UTF-8 string array format)  
function _add_string_offsets_buffer!(buffers::Vector{Ptr{Cvoid}}, tolist::Arrow.ToList{UInt8}, guardian::GuardianObject)
    if hasfield(typeof(tolist), :inds)
        # ToList.inds already contains the correct byte offsets!
        # Just need to convert to Int32 for Arrow C Data Interface
        inds = tolist.inds
        if eltype(inds) != Int32
            offsets = Vector{Int32}(inds)
        else
            offsets = inds
        end
        
        # Add offsets buffer
        offsets_ptr = pointer(offsets)
        push!(buffers, convert(Ptr{Cvoid}, offsets_ptr))
        push!(guardian.buffers, offsets)
    else
        # No offsets available - create empty offsets for empty array
        empty_offsets = Int32[0]  # Empty array has single offset at 0
        offsets_ptr = pointer(empty_offsets)
        push!(buffers, convert(Ptr{Cvoid}, offsets_ptr))
        push!(guardian.buffers, empty_offsets)
    end
end


# Specialized export methods for Primitive{UInt8, ToList} wrapper (binary arrays)
function _export_schema(primitive::Arrow.Primitive{UInt8, <:Arrow.ToList}, schema_ptr::Ptr{CArrowSchema}, guardian::GuardianObject)
    # Delegate to the underlying ToList, which will handle string vs binary format correctly
    _export_schema(primitive.data, schema_ptr, guardian)
end

function _export_array(primitive::Arrow.Primitive{UInt8, <:Arrow.ToList}, array_ptr::Ptr{CArrowArray}, guardian::GuardianObject)
    # Delegate to the underlying ToList
    _export_array(primitive.data, array_ptr, guardian)
end

function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, primitive::Arrow.Primitive{UInt8, <:Arrow.ToList}, guardian::GuardianObject)
    # Delegate to the underlying ToList
    _add_data_buffers!(buffers, primitive.data, guardian)
end

# Specialized buffer export for ToList types (for string/binary data)
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, tolist::Arrow.ToList{UInt8}, guardian::GuardianObject)
    # ToList for strings/binary is essentially the flattened data buffer
    # Export it as a UInt8 primitive array (just the data buffer)
    if hasfield(typeof(tolist), :data) && !isempty(tolist.data)
        # For string ToList, the data contains the individual strings
        # We need to flatten them into a single UInt8 buffer
        total_bytes = sum(item === missing ? 0 : (item isa AbstractString ? ncodeunits(item) : length(item)) for item in tolist.data)
        
        if total_bytes > 0
            # Create contiguous buffer
            flat_data = Vector{UInt8}(undef, total_bytes)
            pos = 1
            
            for item in tolist.data
                if item !== missing
                    if item isa AbstractString
                        bytes = codeunits(item)
                        copyto!(flat_data, pos, bytes, 1, length(bytes))
                        pos += length(bytes)
                    elseif item isa AbstractVector{UInt8}
                        copyto!(flat_data, pos, item, 1, length(item))
                        pos += length(item)
                    end
                end
            end
            
            data_ptr = pointer(flat_data)
            push!(buffers, convert(Ptr{Cvoid}, data_ptr))
            push!(guardian.buffers, flat_data)
        else
            # Empty array - create valid empty buffer
            empty_data = UInt8[]
            data_ptr = pointer(empty_data)
            push!(buffers, convert(Ptr{Cvoid}, data_ptr))
            push!(guardian.buffers, empty_data)
        end
    else
        # No data available - create valid empty buffer
        empty_data = UInt8[]
        data_ptr = pointer(empty_data)
        push!(buffers, convert(Ptr{Cvoid}, data_ptr))
        push!(guardian.buffers, empty_data)
    end
end

# Generic ToList export (for non-UInt8 types)
function _add_data_buffers!(buffers::Vector{Ptr{Cvoid}}, tolist::Arrow.ToList, guardian::GuardianObject)
    # For non-UInt8 ToList, treat as a generic array if possible
    if hasfield(typeof(tolist), :data) && !isempty(tolist.data)
        # Try to get pointer if it's a concrete array type
        try
            data_ptr = pointer(tolist.data)
            push!(buffers, convert(Ptr{Cvoid}, data_ptr))
            push!(guardian.buffers, tolist.data)
        catch
            # If pointer doesn't work, add null buffer
            push!(buffers, C_NULL)
        end
    else
        push!(buffers, C_NULL)
    end
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

# Specialized schema children export for List types
function _export_schema_children(arrow_vector::Arrow.List, guardian::GuardianObject)
    if !hasfield(typeof(arrow_vector), :data)
        return (Int64(0), convert(Ptr{Ptr{CArrowSchema}}, C_NULL))
    end
    
    # Lists have exactly one child (the element type)
    child_schema_ptr = Libc.malloc(sizeof(CArrowSchema))
    child_schema_ptr_typed = convert(Ptr{CArrowSchema}, child_schema_ptr)
    
    # Initialize child schema
    unsafe_store!(child_schema_ptr_typed, CArrowSchema())
    
    # Export child schema
    child_guardian = GuardianObject(arrow_vector.data)
    _export_schema(arrow_vector.data, child_schema_ptr_typed, child_guardian)
    
    # Store in guardian to keep alive
    push!(guardian.children, child_guardian)
    
    # Create array of child schema pointers
    children_array = [child_schema_ptr_typed]
    children_ptr = convert(Ptr{Ptr{CArrowSchema}}, pointer(children_array))
    push!(guardian.buffers, children_array)
    
    return (Int64(1), children_ptr)
end

"""
    _export_array_children(arrow_vector::ArrowVector, guardian::GuardianObject) -> (Int64, Ptr{Ptr{CArrowArray}})

Export child arrays for nested types. Returns (n_children, children_pointer).
"""
function _export_array_children(arrow_vector::ArrowVector, guardian::GuardianObject)
    # Most types don't have children
    return (Int64(0), convert(Ptr{Ptr{CArrowArray}}, C_NULL))
end

# Specialized array children export for List types  
function _export_array_children(arrow_vector::Arrow.List, guardian::GuardianObject)
    if !hasfield(typeof(arrow_vector), :data)
        return (Int64(0), convert(Ptr{Ptr{CArrowArray}}, C_NULL))
    end
    
    # Lists have exactly one child (the values array)
    child_array_ptr = Libc.malloc(sizeof(CArrowArray))
    child_array_ptr_typed = convert(Ptr{CArrowArray}, child_array_ptr)
    
    # Initialize child array
    unsafe_store!(child_array_ptr_typed, CArrowArray())
    
    # Export child array
    child_guardian = GuardianObject(arrow_vector.data)
    _export_array(arrow_vector.data, child_array_ptr_typed, child_guardian)
    
    # Store in guardian to keep alive
    push!(guardian.children, child_guardian)
    
    # Create array of child array pointers
    children_array = [child_array_ptr_typed]
    children_ptr = convert(Ptr{Ptr{CArrowArray}}, pointer(children_array))
    push!(guardian.buffers, children_array)
    
    return (Int64(1), children_ptr)
end

# Specialized schema children export for Struct types
function _export_schema_children(arrow_vector::Arrow.Struct, guardian::GuardianObject)
    if !hasfield(typeof(arrow_vector), :data)
        return (Int64(0), convert(Ptr{Ptr{CArrowSchema}}, C_NULL))
    end
    
    # Struct has multiple children (one for each field)
    n_children = length(arrow_vector.data)
    if n_children == 0
        return (Int64(0), convert(Ptr{Ptr{CArrowSchema}}, C_NULL))
    end
    
    child_schema_ptrs = Ptr{CArrowSchema}[]
    
    for (i, child_vector) in enumerate(arrow_vector.data)
        child_schema_ptr = Libc.malloc(sizeof(CArrowSchema))
        child_schema_ptr_typed = convert(Ptr{CArrowSchema}, child_schema_ptr)
        
        # Initialize child schema
        unsafe_store!(child_schema_ptr_typed, CArrowSchema())
        
        # Export child schema
        child_guardian = GuardianObject(child_vector)
        _export_schema(child_vector, child_schema_ptr_typed, child_guardian)
        
        # Set field name if available
        field_names = getfield(typeof(arrow_vector), :parameters)[3]  # fnames parameter
        if field_names isa Tuple && i <= length(field_names)
            field_name = string(field_names[i])
            schema = unsafe_load(child_schema_ptr_typed)
            schema.name = _create_c_string(field_name)
            unsafe_store!(child_schema_ptr_typed, schema)
        end
        
        push!(child_schema_ptrs, child_schema_ptr_typed)
        push!(guardian.children, child_guardian)
    end
    
    # Create array of child schema pointers
    children_ptr = convert(Ptr{Ptr{CArrowSchema}}, pointer(child_schema_ptrs))
    push!(guardian.buffers, child_schema_ptrs)
    
    return (Int64(n_children), children_ptr)
end

# Specialized array children export for Struct types  
function _export_array_children(arrow_vector::Arrow.Struct, guardian::GuardianObject)
    if !hasfield(typeof(arrow_vector), :data)
        return (Int64(0), convert(Ptr{Ptr{CArrowArray}}, C_NULL))
    end
    
    # Struct has multiple children (one for each field)
    n_children = length(arrow_vector.data)
    if n_children == 0
        return (Int64(0), convert(Ptr{Ptr{CArrowArray}}, C_NULL))
    end
    
    child_array_ptrs = Ptr{CArrowArray}[]
    
    for child_vector in arrow_vector.data
        child_array_ptr = Libc.malloc(sizeof(CArrowArray))
        child_array_ptr_typed = convert(Ptr{CArrowArray}, child_array_ptr)
        
        # Initialize child array
        unsafe_store!(child_array_ptr_typed, CArrowArray())
        
        # Export child array
        child_guardian = GuardianObject(child_vector)
        _export_array(child_vector, child_array_ptr_typed, child_guardian)
        
        push!(child_array_ptrs, child_array_ptr_typed)
        push!(guardian.children, child_guardian)
    end
    
    # Create array of child array pointers
    children_ptr = convert(Ptr{Ptr{CArrowArray}}, pointer(child_array_ptrs))
    push!(guardian.buffers, child_array_ptrs)
    
    return (Int64(n_children), children_ptr)
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