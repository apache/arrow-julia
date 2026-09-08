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
# These sizes describe the host C ABI, not a serialized representation.
# On 64 bit ABIs ArrowSchema/ArrowArray occupy 72/80 bytes. On 32 bit ABIs
# pointer width and Int64 alignment affect padding (e.g. 44/60 bytes with
# 4 byte alignment, 48/64 with 8 byte alignment). The tests compare every
# field offset, alignment, and size with a C compiler on the host platform.
@static if Sys.WORD_SIZE == 64
    @assert sizeof(ArrowSchema) == 7 * _CDATA_PTR_SIZE + 2 * sizeof(Int64)
    @assert sizeof(ArrowArray) == 5 * _CDATA_PTR_SIZE + 5 * sizeof(Int64)
end

const ARROW_FLAG_DICTIONARY_ORDERED = Int64(1)
const ARROW_FLAG_NULLABLE = Int64(2)
const ARROW_FLAG_MAP_KEYS_SORTED = Int64(4)

const _CDATA_MAX_CHILDREN = 10_000
const _CDATA_MAX_DEPTH = 128
const _CDATA_MAX_NODES = 100_000
# An importer policy, not a limit imposed by the C Data Interface spec.
# Includes the terminating NUL, so at most 4095 format bytes are accepted.
# Current primitive formats need one byte; leave room for parameterized
# formats in the follow-up importer. This bounds scanning/allocation but
# cannot establish whether a foreign pointer references readable memory.
const _CDATA_MAX_FORMAT_BYTES = 4096
const _CDATA_MAX_NAME_BYTES = 1 << 16
const _CDATA_MAX_METADATA_PAIRS = 4096
const _CDATA_MAX_METADATA_BYTES = 1 << 20
const _CDATA_MAX_METADATA_FIELD_BYTES = 1 << 20
const _CDATA_MAX_FIXED_SIZE = 4096

abstract type CDataFormat end

struct CDataNullFormat <: CDataFormat end
struct CDataBoolFormat <: CDataFormat end
struct CDataPrimitiveFormat <: CDataFormat
    storage::Type
end
struct CDataBinaryFormat{O} <: CDataFormat
    juliatype::Type
end
struct CDataFixedSizeBinaryFormat <: CDataFormat
    bytewidth::Int
end
struct CDataListFormat{O} <: CDataFormat end
struct CDataFixedSizeListFormat <: CDataFormat
    listsize::Int
end
struct CDataStructFormat <: CDataFormat end

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
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataPrimitive{T,S,A<:AbstractVector{S}} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    data::A
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataBool{T} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    data::Vector{UInt8}
    bitoffset::Int
    len::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataBinary{T,O,A<:AbstractVector{O}} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    offsets::A
    data::Vector{UInt8}
    len::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataFixedSizeBinary{T} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    data::Vector{UInt8}
    bytewidth::Int
    len::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataList{T,O,A<:AbstractVector{O},C<:AbstractVector} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    offsets::A
    data::C
    len::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataFixedSizeList{T,C<:AbstractVector} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    data::C
    listsize::Int
    offset::Int
    len::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataStruct{T,S,fnames} <: CDataVector{T}
    owner::CDataOwner
    validity::CDataValidity
    data::S
    offset::Int
    len::Int
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

struct CDataSlice{T,V<:AbstractVector{T}} <: CDataVector{T}
    parent::V
    first::Int
    len::Int
end

struct CDataMasked{T,V<:CDataVector} <: CDataVector{T}
    parent::V
    parent_validity::CDataValidity
end

struct CDataTable <: Tables.AbstractColumns
    names::Vector{Symbol}
    types::Vector{Type}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol,AbstractVector}
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
    owner::CDataOwner
    rowcount::Int
end

struct CDataNode
    schema::ArrowSchema
    array::ArrowArray
    format::CDataFormat
    name::Union{Nothing,String}
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
    buffers::Vector{Ptr{Cvoid}}
    children::Vector{CDataNode}
    len::Int
    offset::Int
    null_count::Int
end

Base.IndexStyle(::Type{<:CDataVector}) = Base.IndexLinear()

Base.size(x::CDataNull) = (x.len,)
Base.size(x::CDataPrimitive) = size(x.data)
Base.size(x::CDataBool) = (x.len,)
Base.size(x::CDataBinary) = (x.len,)
Base.size(x::CDataFixedSizeBinary) = (x.len,)
Base.size(x::CDataList) = (x.len,)
Base.size(x::CDataFixedSizeList) = (x.len,)
Base.size(x::CDataStruct) = (x.len,)
Base.size(x::CDataSlice) = (x.len,)
Base.size(x::CDataMasked) = size(x.parent)
Base.length(t::CDataTable) = length(getfield(t, :columns))

_owner(x::CDataVector) = getfield(x, :owner)
_owner(x::CDataSlice) = _owner(x.parent)
_owner(x::CDataMasked) = _owner(x.parent)

function _check_live(owner::CDataOwner)
    lock(owner.lock)
    try
        owner.released && throw(ArgumentError("Arrow C Data object has been released"))
        return
    finally
        unlock(owner.lock)
    end
end

_check_live(x::CDataVector) = _check_live(_owner(x))
_check_live(t::CDataTable) = _check_live(getfield(t, :owner))

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
_with_live(f::F, t::CDataTable) where {F} = _with_live(f, getfield(t, :owner))

validitybitmap(x::CDataNull) = nothing
nullcount(x::CDataNull) = x.len
nullcount(x::CDataVector) = validitybitmap(x).null_count
nullcount(x::CDataSlice) = count(i -> ismissing(x[i]), eachindex(x))
nullcount(x::CDataMasked) = count(i -> ismissing(x[i]), eachindex(x))
getmetadata(x::CDataSlice) = getmetadata(x.parent)
getmetadata(x::CDataMasked) = getmetadata(x.parent)
getmetadata(t::CDataTable) = getfield(t, :metadata)
validitybitmap(::CDataMasked) = nothing

@inline function _valid_bit(bytes::Vector{UInt8}, bitoffset::Int, i::Integer)
    pos = bitoffset + Int(i) - 1
    byte = @inbounds bytes[(pos >>> 3) + 1]
    return getbit(byte, (pos & 0x07) + 1)
end

@inline function _valid(v::CDataValidity, i::Integer)
    v.null_count == 0 && return true
    return _valid_bit(v.bytes, v.bitoffset, i)
end

# IPC's ValidityBitmap receives an already known null count and starts at a
# byte boundary. C Data permits null_count == -1 and slices starting at any
# bit, so count only the logical slice, excluding prefix and trailing bits.
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

# Imported arrays implement AbstractVector through these accessors. Keeping
# the owner locked across validity/data reads prevents release_c_data from
# freeing a buffer during a read; ordinary indexing and iteration use this
# path even though the backing data is an unsafe_wrap(...; own=false) view.
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

@propagate_inbounds function Base.getindex(x::CDataBool{T}, i::Integer) where {T}
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.validity, i)
            return missing
        end
        pos = x.bitoffset + Int(i) - 1
        byte = @inbounds x.data[(pos >>> 3) + 1]
        return ArrowTypes.fromarrow(T, getbit(byte, (pos & 0x07) + 1))
    end
end

@propagate_inbounds function Base.getindex(x::CDataBinary{T}, i::Integer) where {T}
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.validity, i)
            return missing
        end
        lo = Int(@inbounds x.offsets[i]) + 1
        hi = Int(@inbounds x.offsets[i + 1])
        n = hi - lo + 1
        if n == 0
            return ArrowTypes.fromarrow(T, "")
        end
        data = x.data
        owner = _owner(x)
        GC.@preserve x data owner begin
            return ArrowTypes.fromarrow(T, pointer(data, lo), n)
        end
    end
end

@propagate_inbounds function Base.getindex(x::CDataFixedSizeBinary{T}, i::Integer) where {T}
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.validity, i)
            return missing
        end
        offset = (Int(i) - 1) * x.bytewidth
        tup = ntuple(j -> @inbounds(x.data[offset + j]), x.bytewidth)
        return ArrowTypes.fromarrow(T, tup)
    end
end

@propagate_inbounds function Base.getindex(x::CDataList{T}, i::Integer) where {T}
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.validity, i)
            return missing
        end
        lo = Int(@inbounds x.offsets[i]) + 1
        hi = Int(@inbounds x.offsets[i + 1])
        return ArrowTypes.fromarrow(T, @view x.data[lo:hi])
    end
end

@propagate_inbounds function Base.getindex(x::CDataFixedSizeList{T}, i::Integer) where {T}
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.validity, i)
            return missing
        end
        offset = (x.offset + Int(i) - 1) * x.listsize
        tup = ntuple(j -> @inbounds(x.data[offset + j]), x.listsize)
        return ArrowTypes.fromarrow(T, tup)
    end
end

@propagate_inbounds function Base.getindex(
    x::CDataStruct{T,S,fnames},
    i::Integer,
) where {T,S,fnames}
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.validity, i)
            return missing
        end
        j = x.offset + Int(i)
        vals = ntuple(k -> @inbounds(x.data[k][j]), fieldcount(S))
        NT = Base.nonmissingtype(T)
        if isnamedtuple(NT) || istuple(NT)
            return ArrowTypes.fromarrow(T, NT(vals))
        else
            return ArrowTypes.fromarrow(T, _fromarrowstruct(NT, Val{fnames}(), vals...))
        end
    end
end

@propagate_inbounds function Base.getindex(x::CDataSlice, i::Integer)
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        return @inbounds x.parent[x.first + Int(i) - 1]
    end
end

@propagate_inbounds function Base.getindex(x::CDataMasked, i::Integer)
    return _with_live(x) do
        @boundscheck checkbounds(x, i)
        if !_valid(x.parent_validity, i)
            return missing
        end
        return @inbounds x.parent[i]
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

# This is Julia's optional deepcopy operation on an already imported array.
# Import itself only moves the C headers. Generic recursive deepcopy would
# copy raw callback pointers and the release owner, creating a second owner
# for the producer's resources. Instead, copy buffers into Julia storage.
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

function Serialization.serialize(s::Serialization.AbstractSerializer, t::CDataTable)
    return Serialization.serialize(s, copy(t))
end

function Base.copy(t::CDataTable)
    return _with_live(t) do
        names = getfield(t, :names)
        columns = getfield(t, :columns)
        return NamedTuple{Tuple(names)}(Tuple(copy(col) for col in columns))
    end
end

Tables.istable(::Type{CDataTable}) = true
Tables.columnaccess(::Type{CDataTable}) = true
Tables.columns(t::CDataTable) = t
Tables.columnnames(t::CDataTable) = getfield(t, :names)
Tables.schema(t::CDataTable) = Tables.Schema(getfield(t, :names), getfield(t, :types))
Tables.getcolumn(t::CDataTable, i::Int) = (_check_live(t); getfield(t, :columns)[i])
Tables.getcolumn(t::CDataTable, nm::Symbol) = (_check_live(t); getfield(t, :lookup)[nm])
Tables.rowcount(t::CDataTable) = getfield(t, :rowcount)

Base.getindex(t::CDataTable, i::Int) = Tables.getcolumn(t, i)
Base.getindex(t::CDataTable, nm::Symbol) = Tables.getcolumn(t, nm)
function Base.getproperty(t::CDataTable, nm::Symbol)
    lookup = getfield(t, :lookup)
    haskey(lookup, nm) && return Tables.getcolumn(t, nm)
    return getfield(t, nm)
end
Base.propertynames(t::CDataTable, private::Bool=false) =
    private ? fieldnames(typeof(t)) : Tuple(getfield(t, :names))

DataAPI.metadatasupport(::Type{CDataTable}) = (read=true, write=false)
DataAPI.colmetadatasupport(::Type{CDataTable}) = (read=true, write=false)

function _dataapi_metadata(meta, key::AbstractString, style::Bool)
    val = meta[key]
    return style ? (val, :default) : val
end

function _dataapi_metadata(meta, key::AbstractString, default, style::Bool)
    if meta !== nothing && haskey(meta, key)
        val = meta[key]
        return style ? (val, :default) : val
    end
    return style ? (default, :default) : default
end

DataAPI.metadata(t::CDataTable, key::AbstractString; style::Bool=false) =
    _dataapi_metadata(getmetadata(t), key, style)
DataAPI.metadata(t::CDataTable, key::AbstractString, default; style::Bool=false) =
    _dataapi_metadata(getmetadata(t), key, default, style)

function DataAPI.metadatakeys(t::CDataTable)
    meta = getmetadata(t)
    meta === nothing && return ()
    return keys(meta)
end

DataAPI.colmetadata(t::CDataTable, col, key::AbstractString; style::Bool=false) =
    _dataapi_metadata(getmetadata(t[col]), key, style)
DataAPI.colmetadata(t::CDataTable, col, key::AbstractString, default; style::Bool=false) =
    _dataapi_metadata(getmetadata(t[col]), key, default, style)

function DataAPI.colmetadatakeys(t::CDataTable, col)
    meta = getmetadata(t[col])
    meta === nothing && return ()
    return keys(meta)
end

function DataAPI.colmetadatakeys(t::CDataTable)
    return (
        col => DataAPI.colmetadatakeys(t, col) for
        col in Tables.columnnames(t) if getmetadata(t[col]) !== nothing
    )
end

# ArrowSchema/ArrowArray are immutable Julia snapshots of mutable C storage.
# Write a replacement snapshot through the pointer to change its release
# field. A fieldoffset-based pointer store could also update only that field;
# whole-value stores keep the layout handling in Julia's struct definition.
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
    # finalizer(f, owner) registers a future callback; finalize(owner) runs
    # registered callbacks immediately. Re-registering here defers cleanup
    # until a later finalization attempt and keeps the owner reachable for
    # that callback. This follows Julia's "Safe use of Finalizers" guidance:
    # https://docs.julialang.org/en/v1/manual/multi-threading/#Safe-use-of-Finalizers
    # Avoid waiting on a contended lock. The reentrant acquisition inside
    # release_c_data is uncontended once trylock succeeds.
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

Release C Data resources owned by an imported array or table. The call is
idempotent. Reads through imported arrays throw after release.
"""
release_c_data(x::CDataVector) = release_c_data(_owner(x))
release_c_data(t::CDataTable) = release_c_data(getfield(t, :owner))

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
    format == "b" && return CDataBoolFormat()
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
    format == "z" && return CDataBinaryFormat{Int32}(Base.CodeUnits)
    format == "Z" && return CDataBinaryFormat{Int64}(Base.CodeUnits)
    format == "u" && return CDataBinaryFormat{Int32}(String)
    format == "U" && return CDataBinaryFormat{Int64}(String)
    format == "+l" && return CDataListFormat{Int32}()
    format == "+L" && return CDataListFormat{Int64}()
    format == "+s" && return CDataStructFormat()
    format == "tdD" && return CDataPrimitiveFormat(Date{Meta.DateUnit.DAY,Int32})
    format == "tdm" && return CDataPrimitiveFormat(Date{Meta.DateUnit.MILLISECOND,Int64})
    format == "tts" && return CDataPrimitiveFormat(Time{Meta.TimeUnit.SECOND,Int32})
    format == "ttm" && return CDataPrimitiveFormat(Time{Meta.TimeUnit.MILLISECOND,Int32})
    format == "ttu" && return CDataPrimitiveFormat(Time{Meta.TimeUnit.MICROSECOND,Int64})
    format == "ttn" && return CDataPrimitiveFormat(Time{Meta.TimeUnit.NANOSECOND,Int64})
    format == "tDs" && return CDataPrimitiveFormat(Duration{Meta.TimeUnit.SECOND})
    format == "tDm" && return CDataPrimitiveFormat(Duration{Meta.TimeUnit.MILLISECOND})
    format == "tDu" && return CDataPrimitiveFormat(Duration{Meta.TimeUnit.MICROSECOND})
    format == "tDn" && return CDataPrimitiveFormat(Duration{Meta.TimeUnit.NANOSECOND})
    format == "tiM" &&
        return CDataPrimitiveFormat(Interval{Meta.IntervalUnit.YEAR_MONTH,Int32})
    format == "tiD" &&
        return CDataPrimitiveFormat(Interval{Meta.IntervalUnit.DAY_TIME,Int64})
    if startswith(format, "ts")
        return CDataPrimitiveFormat(_parse_timestamp_format(format))
    elseif startswith(format, "d:")
        return CDataPrimitiveFormat(_parse_decimal_format(format))
    elseif startswith(format, "w:")
        return CDataFixedSizeBinaryFormat(
            _parse_positive_int(format[3:end], format, _CDATA_MAX_FIXED_SIZE),
        )
    elseif startswith(format, "+w:")
        return CDataFixedSizeListFormat(
            _parse_positive_int(format[4:end], format, _CDATA_MAX_FIXED_SIZE),
        )
    end
    throw(ArgumentError("unsupported Arrow C Data format string: $format"))
end

function _parse_positive_int(s, format, max_value::Int=typemax(Int))
    n = try
        parse(Int, s)
    catch
        throw(ArgumentError("invalid Arrow C Data format string: $format"))
    end
    n <= 0 && throw(ArgumentError("invalid Arrow C Data format string: $format"))
    n > max_value &&
        throw(ArgumentError("Arrow C Data format size exceeds the import limit: $format"))
    return n
end

function _parse_timestamp_format(format)
    length(format) >= 4 ||
        throw(ArgumentError("invalid Arrow C Data format string: $format"))
    format[4] == ':' || throw(ArgumentError("invalid Arrow C Data format string: $format"))
    unit = format[3]
    U =
        unit == 's' ? Meta.TimeUnit.SECOND :
        unit == 'm' ? Meta.TimeUnit.MILLISECOND :
        unit == 'u' ? Meta.TimeUnit.MICROSECOND :
        unit == 'n' ? Meta.TimeUnit.NANOSECOND :
        throw(ArgumentError("invalid Arrow C Data timestamp unit: $format"))
    tz = length(format) == 4 ? nothing : Symbol(format[5:end])
    return Timestamp{U,tz}
end

function _parse_decimal_format(format)
    parts = split(format[3:end], ',')
    2 <= length(parts) <= 3 ||
        throw(ArgumentError("invalid Arrow C Data decimal format: $format"))
    precision = _parse_int_field(parts[1], "decimal precision", format)
    scale = _parse_int_field(parts[2], "decimal scale", format)
    bitwidth =
        length(parts) == 3 ? _parse_int_field(parts[3], "decimal bit width", format) : 128
    precision > 0 || throw(ArgumentError("decimal precision must be positive"))
    if bitwidth == 128
        return Decimal{precision,scale,Int128}
    elseif bitwidth == 256
        return Decimal{precision,scale,Int256}
    else
        throw(ArgumentError("unsupported decimal bit width: $bitwidth"))
    end
end

function _parse_int_field(s, field, format)
    try
        return parse(Int, s)
    catch
        throw(ArgumentError("invalid Arrow C Data $field: $format"))
    end
end

_expected_buffers(::CDataNullFormat) = 0
_expected_buffers(::CDataBoolFormat) = 2
_expected_buffers(::CDataPrimitiveFormat) = 2
_expected_buffers(::CDataBinaryFormat) = 3
_expected_buffers(::CDataFixedSizeBinaryFormat) = 2
_expected_buffers(::CDataListFormat) = 2
_expected_buffers(::CDataFixedSizeListFormat) = 1
_expected_buffers(::CDataStructFormat) = 1

_expected_children(::CDataNullFormat) = 0
_expected_children(::CDataBoolFormat) = 0
_expected_children(::CDataPrimitiveFormat) = 0
_expected_children(::CDataBinaryFormat) = 0
_expected_children(::CDataFixedSizeBinaryFormat) = 0
_expected_children(::CDataListFormat) = 1
_expected_children(::CDataFixedSizeListFormat) = 1
_expected_children(::CDataStructFormat) = nothing

function _nullable(schema::ArrowSchema, null_count::Int)
    return (schema.flags & ARROW_FLAG_NULLABLE) != 0 || null_count != 0
end

function _julia_type(storage::Type, nullable::Bool, convert::Bool)
    T = convert ? finaljuliatype(storage) : storage
    return nullable ? Union{T,Missing} : T
end

function _load_name(ptr::Cstring)
    ptr == C_NULL && return nothing
    name = _unsafe_string_bounded(ptr, _CDATA_MAX_NAME_BYTES, "ArrowSchema.name")
    return isempty(name) ? nothing : name
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
    throw(
        ArgumentError("$name has no NUL terminator within the $maxbytes byte import limit"),
    )
end

function _metadata_dict(pairs)
    isempty(pairs) && return Base.ImmutableDict{String,String}()
    return toidict(pairs)
end

@inline function _unsafe_load_int32(p::Ptr{UInt8})
    b1 = UInt32(unsafe_load(p, 1))
    b2 = UInt32(unsafe_load(p, 2))
    b3 = UInt32(unsafe_load(p, 3))
    b4 = UInt32(unsafe_load(p, 4))
    u =
        ENDIAN_BOM == 0x04030201 ? b1 | (b2 << 8) | (b3 << 16) | (b4 << 24) :
        ENDIAN_BOM == 0x01020304 ? (b1 << 24) | (b2 << 16) | (b3 << 8) | b4 :
        error("unsupported host byte order")
    return reinterpret(Int32, u)
end

function _parse_c_metadata(ptr::Cstring)
    ptr == C_NULL && return nothing
    # Per the Arrow C Data Interface spec, metadata is length encoded and not null terminated.
    p = Ptr{UInt8}(ptr)
    count = Int(_unsafe_load_int32(p))
    count < 0 && throw(ArgumentError("Arrow C Data metadata pair count is negative"))
    count > _CDATA_MAX_METADATA_PAIRS &&
        throw(ArgumentError("Arrow C Data metadata pair count exceeds the limit"))
    pos = 4
    total = 4
    pairs = Pair{String,String}[]
    for _ = 1:count
        key_len = Int(_unsafe_load_int32(p + pos))
        pos += 4
        total += 4
        key_len < 0 && throw(ArgumentError("Arrow C Data metadata key length is negative"))
        key_len > _CDATA_MAX_METADATA_FIELD_BYTES &&
            throw(ArgumentError("Arrow C Data metadata key length exceeds the limit"))
        total = _checked_add(total, key_len, "metadata byte count")
        total > _CDATA_MAX_METADATA_BYTES &&
            throw(ArgumentError("Arrow C Data metadata byte count exceeds the limit"))
        key = unsafe_string(p + pos, key_len)
        pos += key_len

        value_len = Int(_unsafe_load_int32(p + pos))
        pos += 4
        total += 4
        value_len < 0 &&
            throw(ArgumentError("Arrow C Data metadata value length is negative"))
        value_len > _CDATA_MAX_METADATA_FIELD_BYTES &&
            throw(ArgumentError("Arrow C Data metadata value length exceeds the limit"))
        total = _checked_add(total, value_len, "metadata byte count")
        total > _CDATA_MAX_METADATA_BYTES &&
            throw(ArgumentError("Arrow C Data metadata byte count exceeds the limit"))
        value = unsafe_string(p + pos, value_len)
        pos += value_len
        push!(pairs, key => value)
    end
    return _metadata_dict(pairs)
end

function _load_buffers(array::ArrowArray, expected::Int)
    n_buffers = _checked_nonnegative(array.n_buffers, "ArrowArray.n_buffers")
    n_buffers == expected ||
        throw(
            ArgumentError(
                "ArrowArray.n_buffers is $n_buffers; expected $expected for the format",
            ),
        )
    if expected > 0 && array.buffers == C_NULL
        throw(ArgumentError("ArrowArray.buffers is NULL"))
    end
    buffers = Ptr{Cvoid}[]
    for i = 1:expected
        push!(buffers, unsafe_load(array.buffers, i))
    end
    return buffers
end

function _load_child_ptrs(schema::ArrowSchema, array::ArrowArray, count::Int)
    count == 0 && return Ptr{ArrowSchema}[], Ptr{ArrowArray}[]
    schema.children == C_NULL && throw(ArgumentError("ArrowSchema.children is NULL"))
    array.children == C_NULL && throw(ArgumentError("ArrowArray.children is NULL"))
    schema_children = Ptr{ArrowSchema}[]
    array_children = Ptr{ArrowArray}[]
    for i = 1:count
        schema_child = unsafe_load(schema.children, i)
        array_child = unsafe_load(array.children, i)
        schema_child == C_NULL && throw(ArgumentError("ArrowSchema child pointer is NULL"))
        array_child == C_NULL && throw(ArgumentError("ArrowArray child pointer is NULL"))
        push!(schema_children, schema_child)
        push!(array_children, array_child)
    end
    return schema_children, array_children
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
    n_children > _CDATA_MAX_CHILDREN &&
        throw(ArgumentError("ArrowArray.n_children exceeds the import limit"))
    schema_n_children = _checked_nonnegative(schema.n_children, "ArrowSchema.n_children")
    schema_n_children > _CDATA_MAX_CHILDREN &&
        throw(ArgumentError("ArrowSchema.n_children exceeds the import limit"))
    schema_n_children == n_children ||
        throw(
            ArgumentError(
                "ArrowArray.n_children is $n_children; expected $schema_n_children from ArrowSchema.n_children",
            ),
        )
    null_count = array.null_count
    if !(null_count == -1 || 0 <= null_count <= len)
        throw(ArgumentError("ArrowArray.null_count is out of range"))
    end
    if (schema.dictionary == C_NULL) != (array.dictionary == C_NULL)
        throw(
            ArgumentError(
                "ArrowSchema.dictionary and ArrowArray.dictionary must both be NULL or both be non-NULL",
            ),
        )
    end
    schema.dictionary == C_NULL ||
        throw(ArgumentError("dictionary encoded Arrow C Data import is not supported"))
    return len, offset, Int(null_count), n_children
end

function _validate_node(
    schema_ptr::Ptr{ArrowSchema},
    array_ptr::Ptr{ArrowArray};
    top_level::Bool=false,
    depth::Int=1,
    budget::Base.RefValue{Int}=Ref(_CDATA_MAX_NODES),
)
    depth > _CDATA_MAX_DEPTH &&
        throw(ArgumentError("Arrow C Data nesting exceeds the import limit"))
    # A total node budget bounds validation of aliased or cyclic child
    # pointers, which the depth and child count limits alone do not.
    (budget[] -= 1) < 0 &&
        throw(ArgumentError("Arrow C Data node count exceeds the import limit"))
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
    expected_children = _expected_children(format)
    if expected_children !== nothing && n_children != expected_children
        throw(ArgumentError("Arrow C Data child count does not match the format"))
    end
    buffers = _load_buffers(array, _expected_buffers(format))
    schema_child_ptrs, array_child_ptrs = _load_child_ptrs(schema, array, n_children)
    children = CDataNode[]
    for i in eachindex(schema_child_ptrs)
        push!(
            children,
            _validate_node(
                schema_child_ptrs[i],
                array_child_ptrs[i];
                top_level=false,
                depth=depth + 1,
                budget=budget,
            ),
        )
    end
    node = CDataNode(
        schema,
        array,
        format,
        _load_name(schema.name),
        _parse_c_metadata(schema.metadata),
        buffers,
        children,
        len,
        offset,
        null_count,
    )
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

function _aligned(ptr::Ptr{Cvoid}, ::Type{T}) where {T}
    return UInt(ptr) % Base.datatype_alignment(T) == 0
end

function _validate_data_layout(::CDataNullFormat, node::CDataNode, total::Int)
    return
end

function _validate_data_layout(::CDataBoolFormat, node::CDataNode, total::Int)
    nbytes = cld(total, 8)
    nbytes > 0 &&
        node.buffers[2] == C_NULL &&
        throw(ArgumentError("boolean data buffer is NULL"))
    return
end

function _validate_data_layout(format::CDataPrimitiveFormat, node::CDataNode, total::Int)
    nbytes = _checked_mul(total, sizeof(format.storage), "primitive data byte count")
    if nbytes > 0
        node.buffers[2] == C_NULL && throw(ArgumentError("primitive data buffer is NULL"))
    end
    return
end

function _validate_data_layout(
    format::CDataFixedSizeBinaryFormat,
    node::CDataNode,
    total::Int,
)
    nbytes = _checked_mul(total, format.bytewidth, "fixed size binary byte count")
    nbytes > 0 &&
        node.buffers[2] == C_NULL &&
        throw(ArgumentError("fixed size binary data buffer is NULL"))
    return
end

function _validate_data_layout(
    format::CDataBinaryFormat{O},
    node::CDataNode,
    total::Int,
) where {O}
    node.buffers[2] == C_NULL && throw(ArgumentError("offset buffer is NULL"))
    first, last = _validate_offsets(Ptr{O}(node.buffers[2]), node.offset, node.len)
    last < first && throw(ArgumentError("offsets are not monotonic"))
    last > first && node.buffers[3] == C_NULL && throw(ArgumentError("data buffer is NULL"))
    format.juliatype === String && _validate_utf8_offsets(
        Ptr{O}(node.buffers[2]),
        node.offset,
        node.len,
        node.buffers[3],
        first,
        last,
    )
    return
end

function _validate_data_layout(
    format::CDataListFormat{O},
    node::CDataNode,
    total::Int,
) where {O}
    node.buffers[2] == C_NULL && throw(ArgumentError("offset buffer is NULL"))
    first, last = _validate_offsets(Ptr{O}(node.buffers[2]), node.offset, node.len)
    last < first && throw(ArgumentError("offsets are not monotonic"))
    child = node.children[1]
    last <= child.len || throw(ArgumentError("list offset exceeds child length"))
    return
end

function _validate_data_layout(
    format::CDataFixedSizeListFormat,
    node::CDataNode,
    total::Int,
)
    required = _checked_mul(total, format.listsize, "fixed size list child length")
    node.children[1].len >= required ||
        throw(ArgumentError("fixed size list child is too short"))
    return
end

function _validate_data_layout(::CDataStructFormat, node::CDataNode, total::Int)
    # Per the Arrow C Data Interface spec, struct children must cover length + offset.
    for child in node.children
        child.len >= total || throw(ArgumentError("struct child is too short"))
    end
    return
end

function _validate_offsets(ptr::Ptr{O}, offset::Int, len::Int) where {O}
    last_index = _checked_add(_checked_add(offset, len, "offset index"), 1, "offset index")
    _checked_mul(last_index, sizeof(O), "offset buffer byte count")
    _checked_mul(offset, sizeof(O), "offset buffer byte offset")
    first = _offset_to_int(_unsafe_load_offset(ptr, offset + 1), "offset")
    prev = first
    for i = (offset + 2):last_index
        cur = _offset_to_int(_unsafe_load_offset(ptr, i), "offset")
        cur < prev && throw(ArgumentError("offsets are not monotonic"))
        prev = cur
    end
    return first, prev
end

@inline function _unsafe_load_int64(p::Ptr{UInt8})
    b1 = UInt64(unsafe_load(p, 1))
    b2 = UInt64(unsafe_load(p, 2))
    b3 = UInt64(unsafe_load(p, 3))
    b4 = UInt64(unsafe_load(p, 4))
    b5 = UInt64(unsafe_load(p, 5))
    b6 = UInt64(unsafe_load(p, 6))
    b7 = UInt64(unsafe_load(p, 7))
    b8 = UInt64(unsafe_load(p, 8))
    u =
        ENDIAN_BOM == 0x04030201 ?
        b1 | (b2 << 8) | (b3 << 16) | (b4 << 24) | (b5 << 32) | (b6 << 40) | (b7 << 48) |
        (b8 << 56) :
        ENDIAN_BOM == 0x01020304 ?
        (b1 << 56) | (b2 << 48) | (b3 << 40) | (b4 << 32) | (b5 << 24) | (b6 << 16) |
        (b7 << 8) | b8 : error("unsupported host byte order")
    return reinterpret(Int64, u)
end

@inline function _unsafe_load_offset(ptr::Ptr{Int32}, i::Int)
    return _unsafe_load_int32(
        Ptr{UInt8}(ptr) + _checked_mul(i - 1, sizeof(Int32), "offset byte index"),
    )
end

@inline function _unsafe_load_offset(ptr::Ptr{Int64}, i::Int)
    return _unsafe_load_int64(
        Ptr{UInt8}(ptr) + _checked_mul(i - 1, sizeof(Int64), "offset byte index"),
    )
end

function _validate_utf8_offsets(
    ptr::Ptr{O},
    offset::Int,
    len::Int,
    data_ptr::Ptr{Cvoid},
    first::Int,
    last::Int,
) where {O}
    last == first && return
    bytes = unsafe_wrap(Array, Ptr{UInt8}(data_ptr), last; own=false)
    prev = _offset_to_int(_unsafe_load_offset(ptr, offset + 1), "offset")
    for i = (offset + 2):(offset + len + 1)
        cur = _offset_to_int(_unsafe_load_offset(ptr, i), "offset")
        if cur > prev
            if !isvalid(String, @view bytes[(prev + 1):cur])
                throw(ArgumentError("UTF-8 data is invalid"))
            end
        end
        prev = cur
    end
    return
end

function _offset_to_int(x, name)
    x < 0 && throw(ArgumentError("$name is negative"))
    x > typemax(Int) && throw(ArgumentError("$name exceeds the Julia Int range"))
    return Int(x)
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

function _wrap_offsets(ptr::Ptr{Cvoid}, ::Type{T}, offset::Int, len::Int) where {T}
    return _wrap_data(ptr, T, offset, len)
end

function _import_node(node::CDataNode, owner::CDataOwner, convert::Bool)
    return _import_node(node.format, node, owner, convert)
end

function _import_node(::CDataNullFormat, node::CDataNode, owner::CDataOwner, convert::Bool)
    return CDataNull{Missing}(owner, node.len, node.metadata)
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
    return CDataPrimitive{T,format.storage,typeof(data)}(
        owner,
        validity,
        data,
        node.metadata,
    )
end

function _struct_name(node::CDataNode, i::Int)
    return Symbol(node.name === nothing ? "f$(i)" : node.name)
end

function _struct_columns(node::CDataNode, owner::CDataOwner, convert::Bool)
    columns = AbstractVector[]
    names = Symbol[]
    for (i, child_node) in enumerate(node.children)
        child = _import_node(child_node, owner, convert)
        push!(columns, _slice_for_table(child, node.offset, node.len))
        push!(names, _struct_name(child_node, i))
    end
    # NamedTuple construction and column lookup cannot represent duplicates.
    allunique(names) ||
        throw(ArgumentError("duplicate struct field names are not supported"))
    return Tuple(names), Tuple(columns)
end

function _import_node(::CDataBoolFormat, node::CDataNode, owner::CDataOwner, convert::Bool)
    validity = _make_validity(node)
    nullable = _nullable(node.schema, validity.null_count)
    T = nullable ? Union{Bool,Missing} : Bool
    nbytes = cld(_checked_add(node.offset, node.len, "boolean bitmap length"), 8)
    data =
        nbytes == 0 ? UInt8[] :
        unsafe_wrap(Array, Ptr{UInt8}(node.buffers[2]), nbytes; own=false)
    return CDataBool{T}(owner, validity, data, node.offset, node.len, node.metadata)
end

function _import_node(
    format::CDataBinaryFormat{O},
    node::CDataNode,
    owner::CDataOwner,
    convert::Bool,
) where {O}
    validity = _make_validity(node)
    nullable = _nullable(node.schema, validity.null_count)
    T = nullable ? Union{format.juliatype,Missing} : format.juliatype
    offsets = _wrap_offsets(node.buffers[2], O, node.offset, node.len + 1)
    data_len = offsets[end] == offsets[1] ? 0 : Int(offsets[end])
    data =
        data_len == 0 ? UInt8[] :
        unsafe_wrap(Array, Ptr{UInt8}(node.buffers[3]), data_len; own=false)
    return CDataBinary{T,O,typeof(offsets)}(
        owner,
        validity,
        offsets,
        data,
        node.len,
        node.metadata,
    )
end

function _import_node(
    format::CDataFixedSizeBinaryFormat,
    node::CDataNode,
    owner::CDataOwner,
    convert::Bool,
)
    validity = _make_validity(node)
    nullable = _nullable(node.schema, validity.null_count)
    storage = NTuple{format.bytewidth,UInt8}
    T = nullable ? Union{storage,Missing} : storage
    nbytes = _checked_mul(node.len, format.bytewidth, "fixed size binary byte count")
    data =
        nbytes == 0 ? UInt8[] :
        unsafe_wrap(
            Array,
            Ptr{UInt8}(node.buffers[2]) +
            _checked_mul(node.offset, format.bytewidth, "fixed size binary byte offset"),
            nbytes;
            own=false,
        )
    return CDataFixedSizeBinary{T}(
        owner,
        validity,
        data,
        format.bytewidth,
        node.len,
        node.metadata,
    )
end

function _import_node(
    format::CDataListFormat{O},
    node::CDataNode,
    owner::CDataOwner,
    convert::Bool,
) where {O}
    validity = _make_validity(node)
    child = _import_node(node.children[1], owner, convert)
    nullable = _nullable(node.schema, validity.null_count)
    storage = Vector{eltype(child)}
    T = nullable ? Union{storage,Missing} : storage
    offsets = _wrap_offsets(node.buffers[2], O, node.offset, node.len + 1)
    return CDataList{T,O,typeof(offsets),typeof(child)}(
        owner,
        validity,
        offsets,
        child,
        node.len,
        node.metadata,
    )
end

function _import_node(
    format::CDataFixedSizeListFormat,
    node::CDataNode,
    owner::CDataOwner,
    convert::Bool,
)
    validity = _make_validity(node)
    child = _import_node(node.children[1], owner, convert)
    nullable = _nullable(node.schema, validity.null_count)
    storage = NTuple{format.listsize,eltype(child)}
    T = nullable ? Union{storage,Missing} : storage
    return CDataFixedSizeList{T,typeof(child)}(
        owner,
        validity,
        child,
        format.listsize,
        node.offset,
        node.len,
        node.metadata,
    )
end

function _import_node(
    ::CDataStructFormat,
    node::CDataNode,
    owner::CDataOwner,
    convert::Bool,
)
    validity = _make_validity(node)
    children = Tuple(_import_node(child, owner, convert) for child in node.children)
    names = Tuple(
        Symbol(child.name === nothing ? "f$(i)" : child.name) for
        (i, child) in enumerate(node.children)
    )
    types = Tuple(eltype(child) for child in children)
    storage = NamedTuple{names,Tuple{types...}}
    nullable = _nullable(node.schema, validity.null_count)
    T = nullable ? Union{storage,Missing} : storage
    return CDataStruct{T,typeof(children),names}(
        owner,
        validity,
        children,
        node.offset,
        node.len,
        node.metadata,
    )
end

function _slice_for_table(child::CDataVector, offset::Int, len::Int)
    if offset == 0 && length(child) == len
        return child
    else
        first = offset + 1
        return CDataSlice{eltype(child),typeof(child)}(child, first, len)
    end
end

function _mask_for_struct(child::CDataVector, parent_validity::CDataValidity)
    if parent_validity.null_count == 0
        return child
    else
        # Per the Arrow struct validity spec, parent and child validity are independent.
        T = Union{eltype(child),Missing}
        return CDataMasked{T,typeof(child)}(child, parent_validity)
    end
end

function _table_from_struct(node::CDataNode, owner::CDataOwner, convert::Bool)
    parent_validity = _make_validity(node)
    names_tuple, data = _struct_columns(node, owner, convert)
    names = collect(names_tuple)
    columns = AbstractVector[_mask_for_struct(col, parent_validity) for col in data]
    types = Type[eltype(col) for col in columns]
    lookup = Dict{Symbol,AbstractVector}(names[i] => columns[i] for i in eachindex(names))
    return CDataTable(names, types, columns, lookup, node.metadata, owner, node.len)
end

"""
    Arrow.from_c_data(schema_ptr, array_ptr; convert=true)

Import an Arrow C Data Interface schema and array pair.

Aligned imported buffers are viewed without copying and are released by
`release_c_data` or finalization. Misaligned fixed width data buffers are copied
into aligned Julia storage before typed access. Use `copy` or `collect` on
imported arrays to make Julia owned arrays.

The returned array supports indexing and iteration while its shared owner is
live. `deepcopy` also copies its buffers into Julia storage without retaining
producer callbacks; this is separate from the move performed during import.

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

Format strings must have a NUL terminator within 4096 bytes (at most 4095
content bytes). This is an implementation limit, not a format specification
limit. Callers must provide valid, readable pointers for all declared data.

The element type includes `Missing` when the schema declares the field nullable
or the imported array contains nulls. The move is not atomic: importing the
same structures concurrently from multiple tasks is undefined behavior, as with
arrow-rs `from_raw`. Element access is liveness checked, but direct field
introspection of an imported array (for example `Base.dump`) bypasses that
check and must not be used after release.

A top level struct array is returned as a Tables.jl column table. Unlike the
Arrow C++ record batch importer, a nonzero offset or struct level nulls are
accepted: the offset is applied to the columns and rows behind a struct null
read as missing in every column.
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
        if node.format isa CDataStructFormat
            return _table_from_struct(node, owner, convert)
        else
            return _import_node(node, owner, convert)
        end
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

mutable struct CDataExportSchemaOwner
    refs::Vector{Ref{ArrowSchema}}
    roots::Vector{Any}
end

mutable struct CDataExportArrayOwner
    refs::Vector{Ref{ArrowArray}}
    roots::Vector{Any}
end

const _CDATA_EXPORT_LOCK = ReentrantLock()
const _CDATA_EXPORT_NEXT_TOKEN = Ref{UInt}(0)
const _CDATA_EXPORT_SCHEMA_OWNERS = Dict{UInt,CDataExportSchemaOwner}()
const _CDATA_EXPORT_ARRAY_OWNERS = Dict{UInt,CDataExportArrayOwner}()

function _next_c_data_export_token()
    lock(_CDATA_EXPORT_LOCK)
    try
        token = _CDATA_EXPORT_NEXT_TOKEN[] + UInt(1)
        token == 0 && throw(ArgumentError("Arrow C Data export token overflowed"))
        _CDATA_EXPORT_NEXT_TOKEN[] = token
        return token
    finally
        unlock(_CDATA_EXPORT_LOCK)
    end
end

function _release_exported_schema(ptr::Ptr{ArrowSchema})
    ptr == C_NULL && return
    schema = unsafe_load(ptr)
    schema.release == C_NULL && return
    for i = 1:schema.n_children
        child = unsafe_load(schema.children, i)
        if child != C_NULL
            child_schema = unsafe_load(child)
            if child_schema.release != C_NULL
                ccall(child_schema.release, Cvoid, (Ptr{ArrowSchema},), child)
            end
        end
    end
    if schema.dictionary != C_NULL
        dictionary = unsafe_load(schema.dictionary)
        if dictionary.release != C_NULL
            ccall(dictionary.release, Cvoid, (Ptr{ArrowSchema},), schema.dictionary)
        end
    end
    token = UInt(schema.private_data)
    owner = lock(_CDATA_EXPORT_LOCK) do
        pop!(_CDATA_EXPORT_SCHEMA_OWNERS, token, nothing)
    end
    if owner === nothing
        _clear_schema_release!(ptr)
        return
    end
    _clear_schema_release!(ptr)
    for ref in owner.refs
        _clear_schema_release!(Base.unsafe_convert(Ptr{ArrowSchema}, ref))
    end
    empty!(owner.roots)
    empty!(owner.refs)
    return
end

function _release_exported_array(ptr::Ptr{ArrowArray})
    ptr == C_NULL && return
    array = unsafe_load(ptr)
    array.release == C_NULL && return
    for i = 1:array.n_children
        child = unsafe_load(array.children, i)
        if child != C_NULL
            child_array = unsafe_load(child)
            if child_array.release != C_NULL
                ccall(child_array.release, Cvoid, (Ptr{ArrowArray},), child)
            end
        end
    end
    if array.dictionary != C_NULL
        dictionary = unsafe_load(array.dictionary)
        if dictionary.release != C_NULL
            ccall(dictionary.release, Cvoid, (Ptr{ArrowArray},), array.dictionary)
        end
    end
    token = UInt(array.private_data)
    owner = lock(_CDATA_EXPORT_LOCK) do
        pop!(_CDATA_EXPORT_ARRAY_OWNERS, token, nothing)
    end
    if owner === nothing
        _clear_array_release!(ptr)
        return
    end
    _clear_array_release!(ptr)
    for ref in owner.refs
        _clear_array_release!(Base.unsafe_convert(Ptr{ArrowArray}, ref))
    end
    empty!(owner.roots)
    empty!(owner.refs)
    return
end

global _CDATA_EXPORT_SCHEMA_RELEASE::Ptr{Cvoid} = C_NULL
global _CDATA_EXPORT_ARRAY_RELEASE::Ptr{Cvoid} = C_NULL

function _init_c_data_export_callbacks!()
    global _CDATA_EXPORT_SCHEMA_RELEASE =
        @cfunction(_release_exported_schema, Cvoid, (Ptr{ArrowSchema},))
    global _CDATA_EXPORT_ARRAY_RELEASE =
        @cfunction(_release_exported_array, Cvoid, (Ptr{ArrowArray},))
    return
end

function _checked_int64(x::Integer, name)
    x < 0 && throw(ArgumentError("$name must be nonnegative"))
    x > typemax(Int64) && throw(ArgumentError("$name exceeds the Int64 range"))
    return Int64(x)
end

function _primitive_c_data_format(::Type{T}) where {T}
    T === Missing && return "n"
    T === Bool && return "b"
    T === Int8 && return "c"
    T === UInt8 && return "C"
    T === Int16 && return "s"
    T === UInt16 && return "S"
    T === Int32 && return "i"
    T === UInt32 && return "I"
    T === Int64 && return "l"
    T === UInt64 && return "L"
    T === Float16 && return "e"
    T === Float32 && return "f"
    T === Float64 && return "g"
    T === Date{Meta.DateUnit.DAY,Int32} && return "tdD"
    T === Date{Meta.DateUnit.MILLISECOND,Int64} && return "tdm"
    T === Time{Meta.TimeUnit.SECOND,Int32} && return "tts"
    T === Time{Meta.TimeUnit.MILLISECOND,Int32} && return "ttm"
    T === Time{Meta.TimeUnit.MICROSECOND,Int64} && return "ttu"
    T === Time{Meta.TimeUnit.NANOSECOND,Int64} && return "ttn"
    T === Duration{Meta.TimeUnit.SECOND} && return "tDs"
    T === Duration{Meta.TimeUnit.MILLISECOND} && return "tDm"
    T === Duration{Meta.TimeUnit.MICROSECOND} && return "tDu"
    T === Duration{Meta.TimeUnit.NANOSECOND} && return "tDn"
    T === Interval{Meta.IntervalUnit.YEAR_MONTH,Int32} && return "tiM"
    T === Interval{Meta.IntervalUnit.DAY_TIME,Int64} && return "tiD"
    if T <: Timestamp
        U = T.parameters[1]
        TZ = T.parameters[2]
        unit =
            U === Meta.TimeUnit.SECOND ? "s" :
            U === Meta.TimeUnit.MILLISECOND ? "m" :
            U === Meta.TimeUnit.MICROSECOND ? "u" :
            U === Meta.TimeUnit.NANOSECOND ? "n" :
            throw(ArgumentError("unsupported Arrow timestamp unit for C Data export"))
        tz = TZ === nothing ? "" : String(TZ)
        return "ts$(unit):$(tz)"
    elseif T <: Decimal
        P = T.parameters[1]
        S = T.parameters[2]
        I = T.parameters[3]
        I === Int128 && return "d:$(P),$(S),128"
        I === Int256 && return "d:$(P),$(S),256"
    end
    throw(ArgumentError("unsupported Arrow C Data export type: $T"))
end

_c_data_format(::NullVector) = "n"
_c_data_format(::BoolVector) = "b"
_c_data_format(v::Primitive) = _primitive_c_data_format(Base.nonmissingtype(eltype(v)))

function _c_data_format(v::List{T,Int32}) where {T}
    S = Base.nonmissingtype(T)
    liststringtype(v) && return S <: AbstractString ? "u" : "z"
    return "+l"
end

function _c_data_format(v::List{T,Int64}) where {T}
    S = Base.nonmissingtype(T)
    liststringtype(v) && return S <: AbstractString ? "U" : "Z"
    return "+L"
end

function _fixed_size_width(v::FixedSizeList{T}) where {T}
    S = Base.nonmissingtype(T)
    K = ArrowTypes.ArrowKind(ArrowTypes.ArrowType(S))
    return ArrowTypes.getsize(K)
end

_is_fixed_size_binary(v::FixedSizeList) = eltype(v.data) === UInt8

function _c_data_format(v::FixedSizeList)
    n = _fixed_size_width(v)
    return _is_fixed_size_binary(v) ? "w:$(n)" : "+w:$(n)"
end

_c_data_format(::Struct) = "+s"

function _c_data_format(v::ArrowVector)
    throw(ArgumentError("unsupported Arrow C Data export array type: $(typeof(v))"))
end

_c_data_children(::ArrowVector) = ()
_c_data_children(v::List) = liststringtype(v) ? () : (v.data,)
_c_data_children(v::FixedSizeList) = _is_fixed_size_binary(v) ? () : (v.data,)
_c_data_children(v::Struct) = v.data

_c_data_child_name(::ArrowVector, i::Integer) = ""
function _c_data_child_name(v::Struct, i::Integer)
    names = fieldnames(Base.nonmissingtype(eltype(v)))
    return i <= length(names) ? String(names[i]) : "f$(i)"
end

function _assert_c_data_export_supported(v::ArrowVector)
    if v isa Compressed
        throw(
            ArgumentError(
                "compressed Arrow arrays cannot be exported through the C Data Interface",
            ),
        )
    elseif v isa DictEncoded
        throw(ArgumentError("dictionary encoded Arrow C Data export is not supported"))
    elseif v isa Map
        throw(ArgumentError("map Arrow C Data export is not supported"))
    elseif v isa Union{DenseUnion,SparseUnion}
        throw(ArgumentError("union Arrow C Data export is not supported"))
    end
    _c_data_format(v)
    for child in _c_data_children(v)
        _assert_c_data_export_supported(child)
    end
    return
end

function _metadata_to_c_data(meta)
    meta === nothing && return UInt8[]
    isempty(meta) && return UInt8[]
    length(meta) > _CDATA_MAX_METADATA_PAIRS &&
        throw(ArgumentError("Arrow C Data metadata has too many pairs"))
    total = 4
    io = IOBuffer()
    Base.write(io, Int32(length(meta)))
    for (k, v) in meta
        key = codeunits(String(k))
        val = codeunits(String(v))
        length(key) > _CDATA_MAX_METADATA_FIELD_BYTES &&
            throw(ArgumentError("Arrow C Data metadata key is too large"))
        total = _checked_add(total, 4, "metadata byte count")
        total = _checked_add(total, length(key), "metadata byte count")
        total > _CDATA_MAX_METADATA_BYTES &&
            throw(ArgumentError("Arrow C Data metadata byte count exceeds the limit"))
        length(val) > _CDATA_MAX_METADATA_FIELD_BYTES &&
            throw(ArgumentError("Arrow C Data metadata value is too large"))
        total = _checked_add(total, 4, "metadata byte count")
        total = _checked_add(total, length(val), "metadata byte count")
        total > _CDATA_MAX_METADATA_BYTES &&
            throw(ArgumentError("Arrow C Data metadata byte count exceeds the limit"))
        Base.write(io, Int32(length(key)))
        Base.write(io, key)
        Base.write(io, Int32(length(val)))
        Base.write(io, val)
    end
    return take!(io)
end

function _export_cstring(s::AbstractString, roots::Vector{Any})
    bytes = Vector{UInt8}(s * "\0")
    push!(roots, bytes)
    return GC.@preserve bytes begin
        Cstring(pointer(bytes))
    end
end

function _export_metadata(meta, roots::Vector{Any})
    bytes = _metadata_to_c_data(meta)
    isempty(bytes) && return Cstring(C_NULL)
    push!(roots, bytes)
    return GC.@preserve bytes begin
        Cstring(pointer(bytes))
    end
end

function _schema_flags(v::ArrowVector)
    flags = Int64(0)
    if eltype(v) >: Missing || v isa NullVector
        flags |= ARROW_FLAG_NULLABLE
    end
    return flags
end

function _make_c_data_child_schemas!(v::ArrowVector, owner::CDataExportSchemaOwner)
    children = _c_data_children(v)
    isempty(children) && return Int64(0), Ptr{Ptr{ArrowSchema}}(C_NULL)
    ptrs = Ptr{ArrowSchema}[]
    try
        for (i, child) in enumerate(children)
            ref = _make_c_data_schema(child, _c_data_child_name(v, i))
            push!(ptrs, Base.unsafe_convert(Ptr{ArrowSchema}, ref))
        end
    catch
        # Release children built before the failure so their owners do not leak.
        foreach(_release_exported_schema, ptrs)
        rethrow()
    end
    push!(owner.roots, ptrs)
    return GC.@preserve ptrs begin
        Int64(length(ptrs)), Ptr{Ptr{ArrowSchema}}(pointer(ptrs))
    end
end

function _fill_c_data_schema!(
    ref::Ref{ArrowSchema},
    v::ArrowVector,
    name::AbstractString,
    token::UInt,
    owner::CDataExportSchemaOwner,
)
    push!(owner.refs, ref)
    format = _export_cstring(_c_data_format(v), owner.roots)
    field_name = _export_cstring(String(name), owner.roots)
    metadata = _export_metadata(getmetadata(v), owner.roots)
    n_children, children = _make_c_data_child_schemas!(v, owner)
    ref[] = ArrowSchema(
        format,
        field_name,
        metadata,
        _schema_flags(v),
        n_children,
        children,
        Ptr{ArrowSchema}(C_NULL),
        _CDATA_EXPORT_SCHEMA_RELEASE,
        Ptr{Cvoid}(token),
    )
    return ref
end

function _make_c_data_schema(v::ArrowVector, name::AbstractString)
    token = _next_c_data_export_token()
    owner = CDataExportSchemaOwner(Ref{ArrowSchema}[], Any[])
    ref = Ref{ArrowSchema}()
    _fill_c_data_schema!(ref, v, name, token, owner)
    _store_c_data_schema_owner!(token, owner)
    return ref
end

function _store_c_data_schema_owner!(token::UInt, owner::CDataExportSchemaOwner)
    lock(_CDATA_EXPORT_LOCK)
    try
        _CDATA_EXPORT_SCHEMA_OWNERS[token] = owner
        return
    finally
        unlock(_CDATA_EXPORT_LOCK)
    end
end

function _validity_ptr(v::ArrowVector, roots::Vector{Any})
    validity = validitybitmap(v)
    validity.nc == 0 && return Ptr{Cvoid}(C_NULL)
    validity.nc > 0 || throw(ArgumentError("validity null count is invalid"))
    len = length(v)
    validity.nc <= len || throw(ArgumentError("validity null count exceeds array length"))
    validity.ℓ >= len || throw(ArgumentError("validity bitmap length is too short"))
    isempty(validity.bytes) && throw(ArgumentError("validity bitmap is empty"))
    nbytes = cld(len, 8)
    nbytes > 0 || throw(ArgumentError("validity bitmap is empty"))
    bytes = validity.bytes
    _check_export_range(validity.pos, nbytes, length(bytes), "validity bitmap")
    push!(roots, bytes)
    return GC.@preserve bytes begin
        Ptr{Cvoid}(pointer(bytes, validity.pos))
    end
end

_validity_ptr(::NullVector, roots::Vector{Any}) = Ptr{Cvoid}(C_NULL)

function _materialized_vector(x, ::Type{T}) where {T}
    x isa Vector{T} && return x
    return collect(T, x)
end

function _optional_data_ptr(x::AbstractVector, roots::Vector{Any})
    isempty(x) && return Ptr{Cvoid}(C_NULL)
    push!(roots, x)
    return GC.@preserve x begin
        Ptr{Cvoid}(pointer(x))
    end
end

function _required_data_ptr(x::AbstractVector, roots::Vector{Any}, name)
    isempty(x) && throw(ArgumentError("$name is empty"))
    push!(roots, x)
    return GC.@preserve x begin
        Ptr{Cvoid}(pointer(x))
    end
end

function _buffer_array_ptr(buffers::Vector{Ptr{Cvoid}}, roots::Vector{Any})
    isempty(buffers) && return Int64(0), Ptr{Ptr{Cvoid}}(C_NULL)
    push!(roots, buffers)
    return GC.@preserve buffers begin
        Int64(length(buffers)), Ptr{Ptr{Cvoid}}(pointer(buffers))
    end
end

function _check_export_range(pos::Int, n::Int, len::Int, name)
    pos > 0 || throw(ArgumentError("$name position is invalid"))
    n >= 0 || throw(ArgumentError("$name length is invalid"))
    pos <= len || throw(ArgumentError("$name is too short"))
    n <= len - pos + 1 || throw(ArgumentError("$name is too short"))
    return
end

function _validate_offsets_for_export(offsets::AbstractVector, len::Int, name)
    len >= 0 || throw(ArgumentError("$name length is invalid"))
    count = _checked_add(len, 1, "$name offset count")
    length(offsets) >= count ||
        throw(ArgumentError("$name must contain length + 1 offsets"))
    prev = offsets[1]
    prev < 0 && throw(ArgumentError("$name contains a negative offset"))
    prev > typemax(Int) && throw(ArgumentError("$name offset exceeds the Julia Int range"))
    for i = 2:count
        cur = offsets[i]
        cur < 0 && throw(ArgumentError("$name contains a negative offset"))
        cur > typemax(Int) &&
            throw(ArgumentError("$name offset exceeds the Julia Int range"))
        cur < prev && throw(ArgumentError("$name is not monotonic"))
        prev = cur
    end
    return Int(prev)
end

function _primitive_buffers(v::Primitive, roots::Vector{Any})
    T = Base.nonmissingtype(eltype(v))
    data = _materialized_vector(v.data, T)
    length(data) >= length(v) || throw(ArgumentError("primitive data buffer is too short"))
    buffers = Ptr{Cvoid}[_validity_ptr(v, roots), _optional_data_ptr(data, roots)]
    return _buffer_array_ptr(buffers, roots)
end

function _bool_buffers(v::BoolVector, roots::Vector{Any})
    data_bytes = cld(length(v), 8)
    if data_bytes > 0
        _check_export_range(v.pos, data_bytes, length(v.arrow), "boolean data buffer")
    end
    data_ptr = if data_bytes == 0
        Ptr{Cvoid}(C_NULL)
    else
        bytes = v.arrow
        push!(roots, bytes)
        GC.@preserve bytes begin
            Ptr{Cvoid}(pointer(bytes, v.pos))
        end
    end
    buffers = Ptr{Cvoid}[_validity_ptr(v, roots), data_ptr]
    return _buffer_array_ptr(buffers, roots)
end

function _list_buffers(v::List{T,O}, roots::Vector{Any}) where {T,O}
    offsets = _materialized_vector(v.offsets.offsets, O)
    last = _validate_offsets_for_export(offsets, length(v), "list offset buffer")
    offset_ptr = _required_data_ptr(offsets, roots, "list offset buffer")
    if liststringtype(v)
        data = _materialized_vector(v.data, UInt8)
        last <= length(data) ||
            throw(ArgumentError("list offset exceeds data buffer length"))
        buffers =
            Ptr{Cvoid}[_validity_ptr(v, roots), offset_ptr, _optional_data_ptr(data, roots)]
    else
        child = v.data
        last <= length(child) ||
            throw(ArgumentError("list offset exceeds child array length"))
        buffers = Ptr{Cvoid}[_validity_ptr(v, roots), offset_ptr]
    end
    return _buffer_array_ptr(buffers, roots)
end

function _fixed_size_list_buffers(v::FixedSizeList, roots::Vector{Any})
    if _is_fixed_size_binary(v)
        n = _fixed_size_width(v)
        nbytes = _checked_mul(length(v), n, "fixed size binary byte count")
        data = _materialized_vector(v.data, UInt8)
        length(data) >= nbytes ||
            throw(ArgumentError("fixed size binary data buffer is too short"))
        buffers = Ptr{Cvoid}[_validity_ptr(v, roots), _optional_data_ptr(data, roots)]
    else
        child_len =
            _checked_mul(length(v), _fixed_size_width(v), "fixed size list child length")
        length(v.data) >= child_len ||
            throw(ArgumentError("fixed size list child array is too short"))
        buffers = Ptr{Cvoid}[_validity_ptr(v, roots)]
    end
    return _buffer_array_ptr(buffers, roots)
end

function _struct_buffers(v::Struct, roots::Vector{Any})
    for child in v.data
        length(child) >= length(v) ||
            throw(ArgumentError("struct child array is too short"))
    end
    return _buffer_array_ptr(Ptr{Cvoid}[_validity_ptr(v, roots)], roots)
end

_c_data_buffers(v::NullVector, roots::Vector{Any}) = Int64(0), Ptr{Ptr{Cvoid}}(C_NULL)
_c_data_buffers(v::Primitive, roots::Vector{Any}) = _primitive_buffers(v, roots)
_c_data_buffers(v::BoolVector, roots::Vector{Any}) = _bool_buffers(v, roots)
_c_data_buffers(v::List, roots::Vector{Any}) = _list_buffers(v, roots)
_c_data_buffers(v::FixedSizeList, roots::Vector{Any}) = _fixed_size_list_buffers(v, roots)
_c_data_buffers(v::Struct, roots::Vector{Any}) = _struct_buffers(v, roots)

function _make_c_data_child_arrays!(v::ArrowVector, owner::CDataExportArrayOwner)
    children = _c_data_children(v)
    isempty(children) && return Int64(0), Ptr{Ptr{ArrowArray}}(C_NULL)
    ptrs = Ptr{ArrowArray}[]
    try
        for child in children
            ref = _make_c_data_array(child)
            push!(ptrs, Base.unsafe_convert(Ptr{ArrowArray}, ref))
        end
    catch
        # Release children built before the failure so their owners do not leak.
        foreach(_release_exported_array, ptrs)
        rethrow()
    end
    push!(owner.roots, ptrs)
    return GC.@preserve ptrs begin
        Int64(length(ptrs)), Ptr{Ptr{ArrowArray}}(pointer(ptrs))
    end
end

function _c_data_null_count(v::ArrowVector)
    nc = nullcount(v)
    nc <= length(v) || throw(ArgumentError("ArrowArray.null_count exceeds length"))
    return _checked_int64(nc, "ArrowArray.null_count")
end
_c_data_null_count(v::NullVector) = _checked_int64(length(v), "ArrowArray.null_count")

function _fill_c_data_array!(
    ref::Ref{ArrowArray},
    v::ArrowVector,
    token::UInt,
    owner::CDataExportArrayOwner,
)
    push!(owner.refs, ref)
    n_buffers, buffers = _c_data_buffers(v, owner.roots)
    n_children, children = _make_c_data_child_arrays!(v, owner)
    ref[] = ArrowArray(
        _checked_int64(length(v), "ArrowArray.length"),
        _c_data_null_count(v),
        Int64(0),
        n_buffers,
        n_children,
        buffers,
        children,
        Ptr{ArrowArray}(C_NULL),
        _CDATA_EXPORT_ARRAY_RELEASE,
        Ptr{Cvoid}(token),
    )
    return ref
end

function _make_c_data_array(v::ArrowVector)
    token = _next_c_data_export_token()
    owner = CDataExportArrayOwner(Ref{ArrowArray}[], Any[v])
    ref = Ref{ArrowArray}()
    _fill_c_data_array!(ref, v, token, owner)
    _store_c_data_array_owner!(token, owner)
    return ref
end

function _store_c_data_array_owner!(token::UInt, owner::CDataExportArrayOwner)
    lock(_CDATA_EXPORT_LOCK)
    try
        _CDATA_EXPORT_ARRAY_OWNERS[token] = owner
        return
    finally
        unlock(_CDATA_EXPORT_LOCK)
    end
end

function _to_c_data_refs(col::ArrowVector, name::AbstractString)
    _assert_c_data_export_supported(col)
    schema_ref = _make_c_data_schema(col, name)
    try
        return schema_ref, _make_c_data_array(col)
    catch
        _release_exported_schema(Base.unsafe_convert(Ptr{ArrowSchema}, schema_ref))
        rethrow()
    end
end

"""
    Arrow.to_c_data(col::ArrowVector; name="") -> (Ref{ArrowSchema}, Ref{ArrowArray})

Export an Arrow array through the Arrow C Data Interface. The returned schema
and array have independent release callbacks; releasing the schema does not
release array buffers.
"""
to_c_data(col::ArrowVector; name::AbstractString="") = _to_c_data_refs(col, name)

function _table_to_c_data_struct(tbl, names)
    cols = Tables.columns(tbl)
    name_strings = String.(collect(names))
    arrow_tbl = toarrowtable(
        cols,
        Dict{Int64,Any}(),
        false,
        nothing,
        true,
        false,
        false,
        DEFAULT_MAX_DEPTH,
        getmetadata(tbl),
        nothing,
    )
    length(name_strings) == length(arrow_tbl.cols) ||
        throw(ArgumentError("names length must match the number of table columns"))
    syms = Tuple(Symbol.(name_strings))
    data = Tuple(arrow_tbl.cols)
    types = Tuple(eltype(col) for col in data)
    T = NamedTuple{syms,Tuple{types...}}
    validity = ValidityBitmap(UInt8[], 1, Tables.rowcount(arrow_tbl), 0)
    return Struct{T,typeof(data),syms}(
        validity,
        data,
        Tables.rowcount(arrow_tbl),
        arrow_tbl.metadata,
    )
end

"""
    Arrow.to_c_data(tbl; names=String.(Tables.columnnames(tbl)))
        -> (Ref{ArrowSchema}, Ref{ArrowArray})

Export a Tables.jl column table as a root C Data struct array.
"""
function to_c_data(tbl; names=String.(Tables.columnnames(Tables.columns(tbl))))
    root = _table_to_c_data_struct(tbl, names)
    return _to_c_data_refs(root, "")
end
