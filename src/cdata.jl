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

struct ArrowSchema
    format::Cstring
    name::Cstring
    metadata::Cstring
    flags::Int64
    n_children::Int64
    children::Ptr{Ptr{ArrowSchema}}
    dictionary::Ptr{ArrowSchema}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

struct ArrowArray
    length::Int64
    null_count::Int64
    offset::Int64
    n_buffers::Int64
    n_children::Int64
    buffers::Ptr{Ptr{Cvoid}}
    children::Ptr{Ptr{ArrowArray}}
    dictionary::Ptr{ArrowArray}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

const _CDATA_PTR_SIZE = sizeof(Ptr{Cvoid})
@assert isbitstype(ArrowSchema)
@assert isbitstype(ArrowArray)
# On 32 bit ABIs with 8 byte Int64 alignment the C structs contain padding, so
# the packed size formulas hold only where pointer and Int64 sizes agree.
@static if Sys.WORD_SIZE == 64
    @assert sizeof(ArrowSchema) == 7 * _CDATA_PTR_SIZE + 2 * sizeof(Int64)
    @assert sizeof(ArrowArray) == 5 * _CDATA_PTR_SIZE + 5 * sizeof(Int64)
end

const ARROW_FLAG_DICTIONARY_ORDERED = Int64(1)
const ARROW_FLAG_NULLABLE = Int64(2)
const ARROW_FLAG_MAP_KEYS_SORTED = Int64(4)

const _CDATA_MAX_FORMAT_BYTES = 4096

abstract type CDataFormat end

struct CDataNullFormat <: CDataFormat end
struct CDataPrimitiveFormat <: CDataFormat
    storage::Type
end

abstract type CDataVector{T} <: ArrowVector{T} end

mutable struct CDataOwner
    schema::Base.RefValue{ArrowSchema}
    array::Base.RefValue{ArrowArray}
    released::Bool
    lock::ReentrantLock
end

function CDataOwner(schema_ptr::Ptr{ArrowSchema}, array_ptr::Ptr{ArrowArray})
    # Per the Arrow C Data Interface spec, move the base structures into Julia
    # owned storage and mark the sources released without calling their release
    # callbacks, like arrow-rs `from_raw` and nanoarrow `ArrowArrayMove`.
    owner = CDataOwner(
        Ref(unsafe_load(schema_ptr)),
        Ref(unsafe_load(array_ptr)),
        false,
        ReentrantLock(),
    )
    _clear_schema_release!(schema_ptr)
    _clear_array_release!(array_ptr)
    finalizer(_finalize_c_data, owner)
    return owner
end

struct CDataValidity
    bytes::Vector{UInt8}
    bitoffset::Int
    len::Int
    null_count::Int
end

struct CDataNull{T} <: CDataVector{T}
    owner::CDataOwner
    len::Int
end

struct CDataPrimitive{T,S,A<:AbstractVector{S}} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    data::A
end

struct CDataNode
    schema::ArrowSchema
    array::ArrowArray
    format::CDataFormat
    buffers::Vector{Ptr{Cvoid}}
    len::Int
    offset::Int
    null_count::Int
end

Base.IndexStyle(::Type{<:CDataVector}) = Base.IndexLinear()

Base.size(x::CDataNull) = (x.len,)
Base.size(x::CDataPrimitive) = size(x.data)

_owner(x::CDataVector) = getfield(x, :owner)

function _with_live(f::F, owner::CDataOwner) where {F}
    lock(owner.lock)
    try
        owner.released && throw(ArgumentError("Arrow C Data object has been released"))
        return f()
    finally
        unlock(owner.lock)
    end
end

_with_live(f::F, x::CDataVector) where {F} = _with_live(f, _owner(x))

validitybitmap(x::CDataNull) = nothing
nullcount(x::CDataNull) = x.len
nullcount(x::CDataVector) = validitybitmap(x).null_count

@inline function _valid_bit(bytes::Vector{UInt8}, bitoffset::Int, i::Integer)
    pos = bitoffset + Int(i) - 1
    byte = @inbounds bytes[(pos >>> 3) + 1]
    return getbit(byte, (pos & 0x07) + 1)
end

@inline function _valid(v::CDataValidity, i::Integer)
    v.null_count == 0 && return true
    return _valid_bit(v.bytes, v.bitoffset, i)
end

function _count_nulls(bytes::Vector{UInt8}, bitoffset::Int, len::Int)
    len == 0 && return 0
    firstbit = bitoffset
    lastbit = bitoffset + len - 1
    firstbyte = firstbit >>> 3
    lastbyte = lastbit >>> 3
    firstmask = 0xff << (firstbit & 7)
    lastmask = 0xff >>> (7 - (lastbit & 7))
    set = 0
    if firstbyte == lastbyte
        set = count_ones(@inbounds(bytes[firstbyte + 1]) & firstmask & lastmask)
    else
        set = count_ones(@inbounds(bytes[firstbyte + 1]) & firstmask)
        @inbounds for b = (firstbyte + 2):lastbyte
            set += count_ones(bytes[b])
        end
        set += count_ones(@inbounds(bytes[lastbyte + 1]) & lastmask)
    end
    return len - set
end

@propagate_inbounds function Base.getindex(x::CDataNull, i::Integer)
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        return missing
    end
end

@propagate_inbounds function Base.getindex(x::CDataPrimitive{T}, i::Integer) where {T}
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.validity, i)
            return missing
        end
        return @inbounds ArrowTypes.fromarrow(T, x.data[i])
    end
end

function Base.collect(x::CDataVector{T}) where {T}
    return _with_live(x) do
        out = Vector{T}(undef, length(x))
        for i in eachindex(x)
            @inbounds out[i] = x[i]
        end
        return out
    end
end

Base.copy(x::CDataVector) = collect(x)

# Recursive field traversal must not read foreign buffers directly: deepcopy
# rebuilds the same wrapper type around Julia owned buffer copies detached
# from the producer, serialize writes a plain Julia array, and both go
# through the liveness gate, so they throw after release like any other read.
function _detached_owner()
    return CDataOwner(
        Ref(
            ArrowSchema(
                Cstring(C_NULL),
                Cstring(C_NULL),
                Cstring(C_NULL),
                0,
                0,
                C_NULL,
                C_NULL,
                C_NULL,
                C_NULL,
            ),
        ),
        Ref(ArrowArray(0, 0, 0, 0, 0, C_NULL, C_NULL, C_NULL, C_NULL, C_NULL)),
        false,
        ReentrantLock(),
    )
end

_owned_validity(v::CDataValidity) =
    CDataValidity(copy(v.bytes), v.bitoffset, v.len, v.null_count)

_deepcopy_field(x, stackdict::IdDict) = Base.deepcopy_internal(x, stackdict)
_deepcopy_field(v::CDataValidity, ::IdDict) = _owned_validity(v)

# A deepcopied owner must not duplicate the producer release callbacks.
function Base.deepcopy_internal(o::CDataOwner, stackdict::IdDict)
    haskey(stackdict, o) && return stackdict[o]
    return stackdict[o] = _detached_owner()
end

function Base.deepcopy_internal(x::T, stackdict::IdDict) where {T<:CDataVector}
    haskey(stackdict, x) && return stackdict[x]
    y = _with_live(x) do
        T(ntuple(i -> _deepcopy_field(getfield(x, i), stackdict), fieldcount(T))...)
    end
    return stackdict[x] = y
end

function Serialization.serialize(s::Serialization.AbstractSerializer, x::CDataVector)
    return Serialization.serialize(s, copy(x))
end

function _clear_schema_release!(ptr::Ptr{ArrowSchema})
    ptr == C_NULL && return
    schema = unsafe_load(ptr)
    schema.release == C_NULL && return
    unsafe_store!(
        ptr,
        ArrowSchema(
            schema.format,
            schema.name,
            schema.metadata,
            schema.flags,
            schema.n_children,
            schema.children,
            schema.dictionary,
            C_NULL,
            schema.private_data,
        ),
    )
    return
end

function _clear_array_release!(ptr::Ptr{ArrowArray})
    ptr == C_NULL && return
    array = unsafe_load(ptr)
    array.release == C_NULL && return
    unsafe_store!(
        ptr,
        ArrowArray(
            array.length,
            array.null_count,
            array.offset,
            array.n_buffers,
            array.n_children,
            array.buffers,
            array.children,
            array.dictionary,
            C_NULL,
            array.private_data,
        ),
    )
    return
end

function release_c_data(owner::CDataOwner)
    array_release = Ptr{Cvoid}(C_NULL)
    schema_release = Ptr{Cvoid}(C_NULL)
    lock(owner.lock)
    try
        owner.released && return
        owner.released = true
        array_release = owner.array[].release
        schema_release = owner.schema[].release
    finally
        unlock(owner.lock)
    end
    # Per the Arrow C Data Interface spec, consumers release only the base
    # structures. Release the schema even when the array release callback
    # throws so a failing producer callback cannot leak the schema.
    try
        if array_release != C_NULL
            ccall(array_release, Cvoid, (Ptr{ArrowArray},), owner.array)
        end
    finally
        if schema_release != C_NULL
            ccall(schema_release, Cvoid, (Ptr{ArrowSchema},), owner.schema)
        end
    end
    return
end

function _finalize_c_data(owner::CDataOwner)
    # A finalizer must not block on a contended lock, so retry the finalizer
    # instead of waiting. The reentrant acquisition inside release_c_data is
    # uncontended once trylock succeeds.
    if trylock(owner.lock)
        try
            release_c_data(owner)
        finally
            unlock(owner.lock)
        end
    else
        finalizer(_finalize_c_data, owner)
    end
    return
end

"""
    Arrow.release_c_data(x)

Release C Data resources owned by an imported array. The call is idempotent.
Reads through imported arrays throw after release.
"""
release_c_data(x::CDataVector) = release_c_data(_owner(x))

function _to_int(x::Int64, name)
    x > typemax(Int) && throw(ArgumentError("$name exceeds the Julia Int range"))
    return Int(x)
end

function _checked_nonnegative(x::Int64, name)
    x < 0 && throw(ArgumentError("$name must be nonnegative"))
    return _to_int(x, name)
end

function _checked_add(a::Int, b::Int, name)
    b > typemax(Int) - a && throw(ArgumentError("$name overflows"))
    return a + b
end

function _checked_mul(a::Int, b::Int, name)
    a != 0 && b > typemax(Int) ÷ a && throw(ArgumentError("$name overflows"))
    return a * b
end

function _parse_c_data_format(format::AbstractString)
    format == "n" && return CDataNullFormat()
    format == "c" && return CDataPrimitiveFormat(Int8)
    format == "C" && return CDataPrimitiveFormat(UInt8)
    format == "s" && return CDataPrimitiveFormat(Int16)
    format == "S" && return CDataPrimitiveFormat(UInt16)
    format == "i" && return CDataPrimitiveFormat(Int32)
    format == "I" && return CDataPrimitiveFormat(UInt32)
    format == "l" && return CDataPrimitiveFormat(Int64)
    format == "L" && return CDataPrimitiveFormat(UInt64)
    format == "e" && return CDataPrimitiveFormat(Float16)
    format == "f" && return CDataPrimitiveFormat(Float32)
    format == "g" && return CDataPrimitiveFormat(Float64)
    throw(ArgumentError("unsupported Arrow C Data format string: $format"))
end

_expected_buffers(::CDataNullFormat) = 0
_expected_buffers(::CDataPrimitiveFormat) = 2

_expected_children(::CDataNullFormat) = 0
_expected_children(::CDataPrimitiveFormat) = 0

function _nullable(schema::ArrowSchema, null_count::Int)
    return (schema.flags & ARROW_FLAG_NULLABLE) != 0 || null_count != 0
end

function _julia_type(storage::Type, nullable::Bool, convert::Bool)
    T = convert ? finaljuliatype(storage) : storage
    return nullable ? Union{T,Missing} : T
end

function _unsafe_string_bounded(ptr::Cstring, maxbytes::Int, name)
    bytes = UInt8[]
    sizehint!(bytes, min(maxbytes, 128))
    p = Ptr{UInt8}(ptr)
    for i = 1:maxbytes
        byte = unsafe_load(p, i)
        byte == 0x00 && return String(bytes)
        push!(bytes, byte)
    end
    throw(ArgumentError("$name exceeds the import limit"))
end

function _load_buffers(array::ArrowArray, expected::Int)
    n_buffers = _checked_nonnegative(array.n_buffers, "ArrowArray.n_buffers")
    n_buffers == expected ||
        throw(ArgumentError("ArrowArray.n_buffers does not match the format"))
    if expected > 0 && array.buffers == C_NULL
        throw(ArgumentError("ArrowArray.buffers is NULL"))
    end
    buffers = Ptr{Cvoid}[]
    for i = 1:expected
        push!(buffers, unsafe_load(array.buffers, i))
    end
    return buffers
end

function _validate_flags(schema::ArrowSchema, format::CDataFormat)
    # Per the Arrow C Data Interface spec, consumers may ignore flags they do
    # not recognize, so reserved bits are accepted for forward compatibility
    # like Arrow C++ and arrow-rs. Known flags are still checked for semantic
    # consistency, like nanoarrow.
    if (schema.flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0 && schema.dictionary == C_NULL
        throw(ArgumentError("dictionary ordered flag requires a dictionary schema"))
    end
    if (schema.flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0
        throw(ArgumentError("map keys sorted flag requires a map schema"))
    end
    return
end

function _validate_common(schema::ArrowSchema, array::ArrowArray, top_level::Bool)
    schema.format == C_NULL && throw(ArgumentError("ArrowSchema.format is NULL"))
    if top_level
        schema.release == C_NULL && throw(ArgumentError("ArrowSchema.release is NULL"))
        array.release == C_NULL && throw(ArgumentError("ArrowArray.release is NULL"))
    elseif schema.release == C_NULL || array.release == C_NULL
        throw(ArgumentError("released Arrow C Data child structure"))
    end
    len = _checked_nonnegative(array.length, "ArrowArray.length")
    offset = _checked_nonnegative(array.offset, "ArrowArray.offset")
    _checked_add(offset, len, "ArrowArray offset plus length")
    _checked_nonnegative(array.n_buffers, "ArrowArray.n_buffers")
    n_children = _checked_nonnegative(array.n_children, "ArrowArray.n_children")
    schema_children = _checked_nonnegative(schema.n_children, "ArrowSchema.n_children")
    schema_children == n_children ||
        throw(ArgumentError("ArrowSchema and ArrowArray child counts differ"))
    null_count = array.null_count
    if !(null_count == -1 || 0 <= null_count <= len)
        throw(ArgumentError("ArrowArray.null_count is out of range"))
    end
    if (schema.dictionary == C_NULL) != (array.dictionary == C_NULL)
        throw(ArgumentError("ArrowSchema and ArrowArray dictionary pointers differ"))
    end
    schema.dictionary == C_NULL ||
        throw(ArgumentError("dictionary encoded Arrow C Data import is not supported"))
    return len, offset, Int(null_count), n_children
end

function _validate_node(
    schema_ptr::Ptr{ArrowSchema},
    array_ptr::Ptr{ArrowArray};
    top_level::Bool=false,
)
    schema_ptr == C_NULL && throw(ArgumentError("ArrowSchema pointer is NULL"))
    array_ptr == C_NULL && throw(ArgumentError("ArrowArray pointer is NULL"))
    schema = unsafe_load(schema_ptr)
    array = unsafe_load(array_ptr)
    len, offset, null_count, n_children = _validate_common(schema, array, top_level)
    format = _parse_c_data_format(
        _unsafe_string_bounded(
            schema.format,
            _CDATA_MAX_FORMAT_BYTES,
            "ArrowSchema.format",
        ),
    )
    _validate_flags(schema, format)
    n_children == _expected_children(format) ||
        throw(ArgumentError("Arrow C Data child count does not match the format"))
    buffers = _load_buffers(array, _expected_buffers(format))
    node = CDataNode(schema, array, format, buffers, len, offset, null_count)
    _validate_layout(node)
    return node
end

function _validate_layout(node::CDataNode)
    total = _checked_add(node.offset, node.len, "ArrowArray offset plus length")
    if _expected_buffers(node.format) > 0
        # Per the Arrow C Data Interface spec, the validity bitmap may be NULL
        # only when there are no nulls, and like nanoarrow an unknown null
        # count requires the bitmap it is resolved from.
        validity = node.buffers[1]
        if node.null_count == -1
            if cld(total, 8) > 0 && validity == C_NULL
                throw(ArgumentError("unknown null count requires a validity bitmap"))
            end
        elseif node.null_count > 0 && validity == C_NULL
            throw(ArgumentError("null values require a validity bitmap"))
        end
    end
    _validate_data_layout(node.format, node, total)
    return
end

function _validate_data_layout(::CDataNullFormat, node::CDataNode, total::Int)
    return
end

function _aligned(ptr::Ptr{Cvoid}, ::Type{T}) where {T}
    return UInt(ptr) % Base.datatype_alignment(T) == 0
end

function _validate_data_layout(format::CDataPrimitiveFormat, node::CDataNode, total::Int)
    nbytes = _checked_mul(total, sizeof(format.storage), "primitive data byte count")
    if nbytes > 0
        node.buffers[2] == C_NULL && throw(ArgumentError("primitive data buffer is NULL"))
    end
    return
end

function _make_validity(node::CDataNode)
    if _expected_buffers(node.format) == 0 || node.len == 0
        return CDataValidity(UInt8[], 0, node.len, 0)
    end
    ptr = node.buffers[1]
    # A NULL validity bitmap means no nulls. A declared null count is trusted
    # without scanning the bitmap, like Arrow C++, arrow-rs, and nanoarrow
    # default validation.
    if ptr == C_NULL || node.null_count == 0
        return CDataValidity(UInt8[], 0, node.len, 0)
    end
    nbytes = cld(_checked_add(node.offset, node.len, "validity bitmap length"), 8)
    bytes = unsafe_wrap(Array, Ptr{UInt8}(ptr), nbytes; own=false)
    if node.null_count == -1
        # Per the spec, -1 means the null count is not yet computed: resolve
        # it from the bitmap.
        null_count = _count_nulls(bytes, node.offset, node.len)
        return CDataValidity(bytes, node.offset, node.len, null_count)
    end
    return CDataValidity(bytes, node.offset, node.len, node.null_count)
end

function _copy_aligned_data(ptr::Ptr{Cvoid}, ::Type{T}, offset::Int, len::Int) where {T}
    len == 0 && return T[]
    nbytes = _checked_mul(len, sizeof(T), "data buffer byte count")
    out = Vector{T}(undef, len)
    src = Ptr{UInt8}(ptr) + _checked_mul(offset, sizeof(T), "data buffer byte offset")
    GC.@preserve out unsafe_copyto!(Ptr{UInt8}(pointer(out)), src, nbytes)
    return out
end

function _wrap_data(ptr::Ptr{Cvoid}, ::Type{T}, offset::Int, len::Int) where {T}
    len == 0 && return T[]
    # Mirror arrow-rs: copy only misaligned fixed width buffers into aligned storage.
    !_aligned(ptr, T) && return _copy_aligned_data(ptr, T, offset, len)
    p = Ptr{T}(ptr) + _checked_mul(offset, sizeof(T), "data buffer byte offset")
    return unsafe_wrap(Array, p, len; own=false)
end

function _import_node(node::CDataNode, owner::CDataOwner, convert::Bool)
    return _import_node(node.format, node, owner, convert)
end

function _import_node(::CDataNullFormat, node::CDataNode, owner::CDataOwner, convert::Bool)
    return CDataNull{Missing}(owner, node.len)
end

function _import_node(
    format::CDataPrimitiveFormat,
    node::CDataNode,
    owner::CDataOwner,
    convert::Bool,
)
    validity = _make_validity(node)
    nullable = _nullable(node.schema, validity.null_count)
    T = _julia_type(format.storage, nullable, convert)
    data = _wrap_data(node.buffers[2], format.storage, node.offset, node.len)
    return CDataPrimitive{T,format.storage,typeof(data)}(owner, validity, data)
end

"""
    Arrow.from_c_data(schema_ptr, array_ptr; convert=true)

Import an Arrow C Data Interface schema and array pair.

Aligned imported buffers are viewed without copying and are released by
`release_c_data` or finalization. Misaligned fixed width data buffers are copied
into aligned Julia storage before typed access. Use `copy` or `collect` on
imported arrays to make Julia owned arrays.

The importer moves the base `ArrowSchema` and `ArrowArray` structures into
Julia owned storage and marks the passed structures released
(`release = C_NULL`) without calling their release callbacks, following the
C Data Interface move semantics. Callers may free or reuse the passed
structures as soon as `from_c_data` returns; the moved copies are released
through the producer callbacks by `release_c_data` or finalization.

Per the Arrow C Data Interface spec, producers describe buffer sizes through the
schema, length, and offset fields. The importer validates those layout facts and
does not inspect allocator metadata for foreign pointers. A declared
`null_count` is trusted without scanning the validity bitmap like Arrow C++ and
arrow-rs, an unknown null count (-1) requires a validity bitmap and is resolved
from it like nanoarrow, and reserved flag bits are ignored for forward
compatibility.

The element type includes `Missing` when the schema declares the field nullable
or the imported array contains nulls. The move is not atomic: importing the
same structures concurrently from multiple tasks is undefined behavior, as with
arrow-rs `from_raw`. Element access is liveness checked, but direct field
introspection of an imported array (for example `Base.dump`) bypasses that
check and must not be used after release.

This first importer supports null and primitive arrays. Support for further
data types is added by follow up changes.
"""
function from_c_data(
    schema_ptr::Ptr{ArrowSchema},
    array_ptr::Ptr{ArrowArray};
    convert::Bool=true,
)
    schema_ptr == C_NULL && throw(ArgumentError("ArrowSchema pointer is NULL"))
    array_ptr == C_NULL && throw(ArgumentError("ArrowArray pointer is NULL"))
    owner = CDataOwner(schema_ptr, array_ptr)
    try
        node = GC.@preserve owner _validate_node(
            Base.unsafe_convert(Ptr{ArrowSchema}, owner.schema),
            Base.unsafe_convert(Ptr{ArrowArray}, owner.array);
            top_level=true,
        )
        return _import_node(node, owner, convert)
    catch
        try
            release_c_data(owner)
        catch
            # Keep the import error: a throwing producer release callback must
            # not mask the reason the import failed.
        end
        rethrow()
    end
end

from_c_data(schema_ptr::Ptr{Cvoid}, array_ptr::Ptr{Cvoid}; kw...) =
    from_c_data(Ptr{ArrowSchema}(schema_ptr), Ptr{ArrowArray}(array_ptr); kw...)
