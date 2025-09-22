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
Format string generation and parsing for the Arrow C Data Interface.

The format string is a compact, language-agnostic way to encode Arrow data types.
It uses single characters for primitive types and structured patterns for
complex/nested types.

Examples:
- "i" = int32
- "l" = int64  
- "f" = float32
- "g" = float64
- "u" = utf8 string
- "z" = binary
- "+l" = list
- "+w:10" = fixed-size list of 10 elements
- "+s" = struct
"""

"""
    generate_format_string(::Type{T}) -> String

Generate a C Data Interface format string for a Julia type T.
"""
function generate_format_string end

# Primitive types
generate_format_string(::Type{Missing}) = "n"
generate_format_string(::Type{Bool}) = "b"
generate_format_string(::Type{Int8}) = "c"
generate_format_string(::Type{UInt8}) = "C"
generate_format_string(::Type{Int16}) = "s"
generate_format_string(::Type{UInt16}) = "S"
generate_format_string(::Type{Int32}) = "i"
generate_format_string(::Type{UInt32}) = "I"
generate_format_string(::Type{Int64}) = "l"
generate_format_string(::Type{UInt64}) = "L"
generate_format_string(::Type{Float32}) = "f"
generate_format_string(::Type{Float64}) = "g"

# Binary and string types  
generate_format_string(::Type{Vector{UInt8}}) = "z"  # binary
generate_format_string(::Type{String}) = "u"         # utf8

# Handle Union{T, Missing} types
generate_format_string(::Type{Union{T, Missing}}) where {T} = generate_format_string(T)

# Date and time types
function generate_format_string(::Type{Dates.Date})
    return "tdD"  # date32 in days since epoch
end

function generate_format_string(::Type{Dates.DateTime})  
    return "tsm:"  # timestamp in milliseconds, no timezone
end

# For Arrow vector types, delegate to their element type
function generate_format_string(av::ArrowVector{T}) where {T}
    return _generate_format_string_for_arrow_vector(av)
end

# Handle the case where we get the vector type passed directly
generate_format_string(::Type{<:ArrowVector{T}}) where {T} = generate_format_string(T)

"""
    _generate_format_string_for_arrow_vector(av::ArrowVector) -> String

Generate format string for specific Arrow vector types.
"""
function _generate_format_string_for_arrow_vector(av::ArrowVector{T}) where {T}
    # Default for primitive arrow vectors
    return generate_format_string(T)
end

function _generate_format_string_for_arrow_vector(av::Arrow.List{T}) where {T}
    return "+l"  # List type
end

function _generate_format_string_for_arrow_vector(av::Arrow.FixedSizeList{T}) where {T}
    # Get the fixed size from the vector
    # This is a simplification - in practice we'd need to extract the actual size
    return "+w:$(av.ℓ ÷ length(av.data))"  # Fixed-size list
end

function _generate_format_string_for_arrow_vector(av::Arrow.Struct)
    return "+s"  # Struct type
end

"""
    parse_format_string(format::String) -> Type

Parse a C Data Interface format string and return the corresponding Julia type.
This is used when importing data from other Arrow implementations.
"""
function parse_format_string(format::String)
    if isempty(format)
        throw(ArgumentError("Empty format string"))
    end
    
    # Single character primitive types
    format == "n" && return Missing
    format == "b" && return Bool
    format == "c" && return Int8
    format == "C" && return UInt8
    format == "s" && return Int16
    format == "S" && return UInt16
    format == "i" && return Int32
    format == "I" && return UInt32
    format == "l" && return Int64
    format == "L" && return UInt64
    format == "f" && return Float32
    format == "g" && return Float64
    format == "z" && return Vector{UInt8}  # binary
    format == "u" && return String         # utf8
    
    # Date/time types
    if startswith(format, "td")
        if format == "tdD"
            return Dates.Date
        end
    elseif startswith(format, "ts")
        if startswith(format, "tsm:")
            return Dates.DateTime
        end
    end
    
    # Nested types (start with +)
    if startswith(format, "+")
        if format == "+l"
            return :list  # We'll need additional context to determine full type
        elseif startswith(format, "+w:")
            # Fixed-size list
            size_str = format[4:end]
            try
                size = parse(Int, size_str)
                return (:fixed_size_list, size)
            catch
                throw(ArgumentError("Invalid fixed-size list format: $format"))
            end
        elseif format == "+s"
            return :struct
        elseif format == "+m"
            return :map
        end
    end
    
    throw(ArgumentError("Unsupported format string: $format"))
end

"""
    _create_c_string(s::String) -> Ptr{Cchar}

Create a C string from a Julia string. The caller is responsible
for freeing the memory.
"""
function _create_c_string(s::String)
    if isempty(s)
        return C_NULL
    end
    # Allocate memory for the string plus null terminator
    ptr = Libc.malloc(sizeof(s) + 1)
    unsafe_copyto!(convert(Ptr{UInt8}, ptr), pointer(s), sizeof(s))
    unsafe_store!(convert(Ptr{UInt8}, ptr) + sizeof(s), 0x00)  # null terminator
    return convert(Ptr{Cchar}, ptr)
end

"""
    _read_c_string(ptr::Ptr{Cchar}) -> String

Read a C string from a pointer. Returns empty string if pointer is NULL.
"""
function _read_c_string(ptr::Ptr{Cchar})
    if ptr == C_NULL
        return ""
    end
    return unsafe_string(ptr)
end

# Handle generic pointer type for tests
_read_c_string(ptr::Ptr{Nothing}) = _read_c_string(convert(Ptr{Cchar}, ptr))

"""
    _free_c_string(ptr::Ptr{Cchar})

Free memory allocated for a C string.
"""
function _free_c_string(ptr::Ptr{Cchar})
    if ptr != C_NULL
        Libc.free(ptr)
    end
end

# Handle generic pointer type for tests
_free_c_string(ptr::Ptr{Nothing}) = _free_c_string(convert(Ptr{Cchar}, ptr))