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

###############################################################################
# Arrow C Data Interface
# Spec: https://arrow.apache.org/docs/format/CDataInterface.html
###############################################################################

###############################################################################
# C struct definitions                                                        #
###############################################################################

"""
    Arrow.ArrowSchema

Mirrors `struct ArrowSchema` from the Arrow C Data Interface specification.
Layout must be ABI-compatible with the C struct (9 pointer-sized fields).
"""
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

"""
    Arrow.ArrowArray

Mirrors `struct ArrowArray` from the Arrow C Data Interface specification.
Layout must be ABI-compatible with the C struct (10 pointer-sized fields).
"""
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

@assert sizeof(ArrowSchema) == 9 * 8 "ArrowSchema size mismatch; expected $(9*8), got $(sizeof(ArrowSchema))"
@assert sizeof(ArrowArray) == 10 * 8 "ArrowArray size mismatch; expected $(10*8), got $(sizeof(ArrowArray))"

# Schema flags
const CDATA_FLAG_NULLABLE = Int64(2)
const CDATA_FLAG_DICT_ORDERED = Int64(1)
const CDATA_FLAG_MAP_KEYS_SORTED = Int64(4)

# Shared empty buffer used in all imported ArrowVectors (never mutated)
const _EMPTY_BYTES = UInt8[]

# Singleton "all valid" ValidityBitmap: returned for every null-free column so that
# Primitive/List/etc. store a pointer to this one object rather than allocating a new one.
const _ALL_VALID = ValidityBitmap(UInt8[], 1, 0, 0)

"""
    CBuffer{T}

An isbits `AbstractVector{T}` that reads directly through a C pointer with no
heap allocation — unlike `unsafe_wrap`, which allocates a Julia array header.
Used internally by the C Data Interface import path.
"""
struct CBuffer{T} <: AbstractVector{T}
    ptr::Ptr{T}
    len::Int
end

Base.size(b::CBuffer) = (b.len,)
Base.IndexStyle(::Type{<:CBuffer}) = Base.IndexLinear()
@inline Base.getindex(b::CBuffer{T}, i::Int) where {T} = unsafe_load(b.ptr, i)
Base.unsafe_convert(::Type{Ptr{T}}, b::CBuffer{T}) where {T} = b.ptr
Base.pointer(b::CBuffer{T}) where {T} = b.ptr
Base.pointer(b::CBuffer{T}, i::Integer) where {T} = b.ptr + (i - 1) * sizeof(T)

"""
    COffsets{T}

An isbits `AbstractOffsets{T}` that reads offset pairs directly through a C pointer.
Unlike `Offsets{T}`, which wraps a `Vector{T}`, `COffsets{T}` is zero-allocation —
it stores only a `Ptr{T}` and a length, and is embedded inline inside `List` and `Map`.
"""
struct COffsets{T<:Union{Int32,Int64}} <: AbstractOffsets{T}
    ptr::Ptr{T}
    len::Int
end

Base.size(o::COffsets) = (o.len,)

@inline function Base.getindex(o::COffsets{T}, i::Integer) where {T}
    @boundscheck checkbounds(o, i)
    lo = unsafe_load(o.ptr, i) + one(T)
    hi = unsafe_load(o.ptr, i + 1)
    return lo, hi
end

# Kind tags for SchemaNode dispatch — avoids re-parsing the format string per batch
const CKIND_NULL        = UInt8(0)
const CKIND_BOOL        = UInt8(1)
const CKIND_PRIM        = UInt8(2)
const CKIND_FIXED_BIN   = UInt8(3)   # "w:N"
const CKIND_STR32       = UInt8(4)   # "u"
const CKIND_STR64       = UInt8(5)   # "U"
const CKIND_BIN32       = UInt8(6)   # "z"
const CKIND_BIN64       = UInt8(7)   # "Z"
const CKIND_LIST32      = UInt8(8)   # "+l"
const CKIND_LIST64      = UInt8(9)   # "+L"
const CKIND_FIXED_LIST  = UInt8(10)  # "+w:N"
const CKIND_STRUCT      = UInt8(11)  # "+s"
const CKIND_MAP         = UInt8(12)  # "+m"
const CKIND_DENSE_UNION = UInt8(13)  # "+ud:..."
const CKIND_SPARSE_UNION= UInt8(14)  # "+us:..."
const CKIND_DICT        = UInt8(15)  # dictionary-encoded

"""
    SchemaNode

Parsed representation of an `ArrowSchema` tree. Captures format, nullability,
metadata, children, and dictionary as Julia values, and pre-computes the dispatch
kind, storage type, and fixed sizes so that [`from_c_data`](@ref) can import
repeated batches with the same schema without re-parsing anything per batch.

Obtain one via [`Arrow.parse_c_schema`](@ref).
"""
struct SchemaNode
    fmt::String
    name::Symbol
    nullable::Bool
    flags::Int64
    n_children::Int
    children::Vector{SchemaNode}
    has_dictionary::Bool
    dict_node::Union{Nothing,SchemaNode}
    meta::Union{Nothing,Base.ImmutableDict{String,String}}
    # Pre-parsed fields: computed once at schema-parse time, reused per batch
    kind::UInt8
    storage_type::Type            # for CKIND_PRIM and CKIND_DICT (index type)
    fixed_size::Int               # for CKIND_FIXED_BIN and CKIND_FIXED_LIST
    type_ids::Union{Nothing,Tuple{Vararg{Int32}}}  # for union kinds
end

"""
    TableSchema

Parsed schema for a multi-column table. Holds the per-column [`SchemaNode`](@ref)s
together with the pre-built column-name vector and name→index lookup so that
[`from_c_data`](@ref) can import repeated batches with zero per-batch allocations
for schema work.

Obtain one via `Arrow.parse_c_schema(schema_ptrs)`.
"""
struct TableSchema
    nodes::Vector{SchemaNode}
    col_names::Vector{Symbol}
    lookup::Dict{Symbol,Int}
end

###############################################################################
# Import path (C → Julia)                                                     #
###############################################################################

"""
    CDataHandle

Holds C-side pointers for an imported Arrow C Data Interface pair.
Call `Arrow.release_c_data` to release C resources explicitly.

If `owns_array_memory` is `true`, `_release_cdata_handle` calls `Libc.free`
on `array_ptr` after invoking the arrow release callback.  Used by the
C Stream Interface path, which allocates the `ArrowArray` struct via
`Libc.malloc` rather than receiving it on the Rust heap.
"""
mutable struct CDataHandle
    schema_ptr::Ptr{ArrowSchema}
    array_ptr::Ptr{ArrowArray}
    released::Bool
    owns_array_memory::Bool
end

CDataHandle(sp::Ptr{ArrowSchema}, ap::Ptr{ArrowArray}) = CDataHandle(sp, ap, false, false)

function _release_cdata_handle(h::CDataHandle)
    h.released && return
    h.released = true
    if h.array_ptr != C_NULL
        arr = unsafe_load(h.array_ptr)
        if arr.release != C_NULL
            ccall(arr.release, Cvoid, (Ptr{ArrowArray},), h.array_ptr)
        end
        if h.owns_array_memory
            Libc.free(h.array_ptr)
        end
    end
    if h.schema_ptr != C_NULL
        sch = unsafe_load(h.schema_ptr)
        if sch.release != C_NULL
            ccall(sch.release, Cvoid, (Ptr{ArrowSchema},), h.schema_ptr)
        end
    end
end

"""
    CImportedArray{T}

An `AbstractVector{T}` wrapping an imported Arrow C Data Interface array.
Holds a reference to the `CDataHandle` to keep the C-side memory alive.
Call `Arrow.release_c_data(x)` to release C resources immediately; otherwise
they are released when this object is garbage collected.
"""
struct CImportedArray{T} <: AbstractVector{T}
    data::ArrowVector{T}
    handle::CDataHandle
end

Base.size(x::CImportedArray) = size(x.data)
Base.IndexStyle(::Type{<:CImportedArray}) = Base.IndexLinear()
Base.@propagate_inbounds function Base.getindex(x::CImportedArray, i::Integer)
    @boundscheck checkbounds(x, i)
    return @inbounds x.data[i]
end

"""
    CImportedTable

A `Tables.AbstractColumns` wrapping imported Arrow C Data Interface arrays.
Column `ArrowVector`s are constructed on demand in `getcolumn` — `arr_ptrs` holds
isbits `Ptr{ArrowArray}` values with no boxing.

`shared_handle`: if non-`nothing`, all columns share this handle (stream path —
release the root and all children are freed together). If `nothing`, each column
pointer is individually owned and released via its C release callback.
"""
struct CImportedTable
    schema::TableSchema
    arr_ptrs::Vector{Ptr{ArrowArray}}  # one per column, isbits — no boxing
    shared_handle::Union{Nothing,CDataHandle}
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

Tables.istable(::Type{<:CImportedTable}) = true
Tables.columnaccess(::Type{<:CImportedTable}) = true
Tables.columns(t::CImportedTable) = t
Tables.columnnames(t::CImportedTable) = t.schema.col_names
function Tables.getcolumn(t::CImportedTable, i::Int)
    return _import_arrowvec_fast(t.arr_ptrs[i], t.schema.nodes[i])
end
Tables.getcolumn(t::CImportedTable, nm::Symbol) =
    Tables.getcolumn(t, t.schema.lookup[nm])
Tables.schema(t::CImportedTable) =
    Tables.Schema(t.schema.col_names, nothing)
DataAPI.metadatasupport(::Type{CImportedTable}) = (read=true, write=false)
DataAPI.metadata(t::CImportedTable, key::AbstractString; style::Bool=false) =
    style ? (get(t.metadata === nothing ? Dict() : t.metadata, key, nothing), :default) :
    get(t.metadata === nothing ? Dict() : t.metadata, key, nothing)
DataAPI.metadatakeys(t::CImportedTable) = t.metadata === nothing ? () : keys(t.metadata)

"""
    Arrow.release_c_data(x::CImportedArray)
    Arrow.release_c_data(t::CImportedTable)

Immediately release C-side resources held by an imported array or table.
After calling this, the data in `x` or `t` may become invalid.
"""
release_c_data(x::CImportedArray) = _release_cdata_handle(x.handle)
function release_c_data(t::CImportedTable)
    if t.shared_handle !== nothing
        _release_cdata_handle(t.shared_handle)
    else
        for ap in t.arr_ptrs
            arr = unsafe_load(ap)
            arr.release != C_NULL &&
                ccall(arr.release, Cvoid, (Ptr{ArrowArray},), ap)
        end
    end
end

# Parse the binary key-value metadata format used by the C Data Interface
function _parse_c_metadata(ptr::Cstring)
    ptr == C_NULL && return nothing
    p = Ptr{UInt8}(ptr)
    n_pairs = unsafe_load(Ptr{Int32}(p))
    n_pairs <= 0 && return nothing
    pos = 4  # byte offset from p
    dict = Base.ImmutableDict{String,String}()
    for _ = 1:n_pairs
        key_len = unsafe_load(Ptr{Int32}(p + pos))
        pos += 4
        key = unsafe_string(p + pos, key_len)
        pos += key_len
        val_len = unsafe_load(Ptr{Int32}(p + pos))
        pos += 4
        val = unsafe_string(p + pos, val_len)
        pos += val_len
        dict = Base.ImmutableDict(dict, key => val)
    end
    return dict
end

# Load the i-th buffer pointer from an ArrowArray (0-indexed, pointer arithmetic in units of sizeof(Ptr))
_cbuf(arr::ArrowArray, i::Int) =
    (arr.n_buffers > i && arr.buffers != C_NULL) ? unsafe_load(arr.buffers, i + 1) : C_NULL

# Load the i-th child array pointer from an ArrowArray (0-indexed)
_cchild_arr(arr::ArrowArray, i::Int) = unsafe_load(arr.children, i + 1)

# Load the i-th child schema pointer from an ArrowSchema (0-indexed)
_cchild_sch(sch::ArrowSchema, i::Int) = unsafe_load(sch.children, i + 1)

# Build a ValidityBitmap from C Data Interface buffer
function _import_validity(arr::ArrowArray, len::Int, off::Int)
    nc = Int(arr.null_count)
    vptr = Ptr{UInt8}(_cbuf(arr, 0))
    if nc == 0 || vptr == C_NULL
        return _ALL_VALID
    end
    n_bytes = cld(len + off, 8)
    vbytes = unsafe_wrap(Array, vptr, n_bytes; own=false)
    if off % 8 == 0
        return ValidityBitmap(vbytes, off ÷ 8 + 1, len, nc)
    else
        # non-byte-aligned offset: copy and repack bits
        new_bytes = _copy_bit_range(vbytes, off, len)
        return ValidityBitmap(new_bytes, 1, len, nc)
    end
end

# Copy a range of bits from src starting at bit offset `off` (0-indexed), length `len`
function _copy_bit_range(src::Vector{UInt8}, off::Int, len::Int)
    nbytes = cld(len, 8)
    dest = fill(0xff, nbytes)
    for i = 0:(len - 1)
        src_pos = off + i
        src_byte = src_pos >> 3
        src_bit = src_pos & 7
        bit = (src[src_byte + 1] >> src_bit) & 1
        if bit == 0
            dst_byte = i >> 3
            dst_bit = i & 7
            dest[dst_byte + 1] &= ~(UInt8(1) << dst_bit)
        end
    end
    return dest
end

function _char_to_timeunit(c::Char)
    c == 's' && return Meta.TimeUnit.SECOND
    c == 'm' && return Meta.TimeUnit.MILLISECOND
    c == 'u' && return Meta.TimeUnit.MICROSECOND
    c == 'n' && return Meta.TimeUnit.NANOSECOND
    error("Unknown time unit character: $c")
end

# Parse a primitive/simple format string to its Julia storage type
function _fmt_to_storage_type(fmt::String)
    fmt == "c" && return Int8
    fmt == "C" && return UInt8
    fmt == "s" && return Int16
    fmt == "S" && return UInt16
    fmt == "i" && return Int32
    fmt == "I" && return UInt32
    fmt == "l" && return Int64
    fmt == "L" && return UInt64
    fmt == "e" && return Float16
    fmt == "f" && return Float32
    fmt == "g" && return Float64
    fmt == "tdD" && return Date{Meta.DateUnit.DAY,Int32}
    fmt == "tdm" && return Date{Meta.DateUnit.MILLISECOND,Int64}
    fmt == "tts" && return Time{Meta.TimeUnit.SECOND,Int32}
    fmt == "ttm" && return Time{Meta.TimeUnit.MILLISECOND,Int32}
    fmt == "ttu" && return Time{Meta.TimeUnit.MICROSECOND,Int64}
    fmt == "ttn" && return Time{Meta.TimeUnit.NANOSECOND,Int64}
    fmt == "tDs" && return Duration{Meta.TimeUnit.SECOND}
    fmt == "tDm" && return Duration{Meta.TimeUnit.MILLISECOND}
    fmt == "tDu" && return Duration{Meta.TimeUnit.MICROSECOND}
    fmt == "tDn" && return Duration{Meta.TimeUnit.NANOSECOND}
    fmt == "tiM" && return Interval{Meta.IntervalUnit.YEAR_MONTH,Int32}
    fmt == "tiD" && return Interval{Meta.IntervalUnit.DAY_TIME,Int64}
    if startswith(fmt, "ts") && length(fmt) >= 4
        U = _char_to_timeunit(fmt[3])
        tz_str = length(fmt) > 4 ? fmt[5:end] : ""
        TZ = isempty(tz_str) ? nothing : Symbol(tz_str)
        return Timestamp{U,TZ}
    end
    if startswith(fmt, "d:")
        parts = split(fmt[3:end], ',')
        p = parse(Int, parts[1])
        s_val = parse(Int, parts[2])
        bw = length(parts) >= 3 ? parse(Int, parts[3]) : 128
        return bw == 256 ? Decimal{p,s_val,Int256} : Decimal{p,s_val,Int128}
    end
    error("Unsupported format string for primitive type: $fmt")
end

# Main import function: given C pointers (already loaded), build an ArrowVector.
# handle is the top-level CDataHandle to keep C memory alive.
function _import_arrowvec(
    arr_ptr::Ptr{ArrowArray},
    sch_ptr::Ptr{ArrowSchema},
    handle::CDataHandle,
    convert::Bool,
)
    arr = unsafe_load(arr_ptr)
    sch = unsafe_load(sch_ptr)
    fmt = unsafe_string(sch.format)
    len = Int(arr.length)
    off = Int(arr.offset)
    nullable = (sch.flags & CDATA_FLAG_NULLABLE) != 0
    meta = _parse_c_metadata(sch.metadata)
    validity = _import_validity(arr, len, off)

    # Null array
    if fmt == "n"
        return NullVector{Missing}(MissingVector(len), meta)
    end

    # Boolean
    if fmt == "b"
        T = nullable ? Union{Bool,Missing} : Bool
        dptr = Ptr{UInt8}(_cbuf(arr, 1))
        if dptr == C_NULL
            return BoolVector{T}(_EMPTY_BYTES, 1, validity, len, meta)
        end
        n_bytes = cld(len + off, 8)
        data_bytes = unsafe_wrap(Array, dptr, n_bytes; own=false)
        if off % 8 == 0
            return BoolVector{T}(data_bytes, off ÷ 8 + 1, validity, len, meta)
        else
            new_bytes = _copy_bit_range(data_bytes, off, len)
            return BoolVector{T}(new_bytes, 1, validity, len, meta)
        end
    end

    # Fixed-size binary "w:N"
    if startswith(fmt, "w:")
        N = parse(Int, fmt[3:end])
        T_inner = NTuple{N,UInt8}
        T = nullable ? Union{T_inner,Missing} : T_inner
        dptr = Ptr{UInt8}(_cbuf(arr, 1))
        data_bytes = dptr == C_NULL ? _EMPTY_BYTES : unsafe_wrap(Array, dptr + off * N, len * N; own=false)
        return FixedSizeList{T,typeof(data_bytes)}(_EMPTY_BYTES, validity, data_bytes, len, meta)
    end

    # String / binary (list with inline data)
    if fmt ∈ ("u", "U", "z", "Z")
        OT = (fmt == "U" || fmt == "Z") ? Int64 : Int32
        T_inner = (fmt == "u" || fmt == "U") ? String : Base.CodeUnits{UInt8,String}
        T = nullable ? Union{T_inner,Missing} : T_inner
        optr = Ptr{OT}(_cbuf(arr, 1))
        offs_arr = optr == C_NULL ? OT[] : unsafe_wrap(Array, optr + off * sizeof(OT), len + 1; own=false)
        offsets = Offsets(_EMPTY_BYTES, offs_arr)
        dptr = Ptr{UInt8}(_cbuf(arr, 2))
        # data length = last offset value
        data_len = isempty(offs_arr) ? 0 : Int(offs_arr[end])
        data_bytes =
            dptr == C_NULL ? _EMPTY_BYTES : unsafe_wrap(Array, dptr, data_len; own=false)
        return List{T,OT,Vector{UInt8}}(_EMPTY_BYTES, validity, offsets, data_bytes, len, meta)
    end

    # Generic list "+l" / "+L"
    if fmt == "+l" || fmt == "+L"
        OT = fmt == "+L" ? Int64 : Int32
        optr = Ptr{OT}(_cbuf(arr, 1))
        offs_arr = optr == C_NULL ? OT[] : unsafe_wrap(Array, optr + off * sizeof(OT), len + 1; own=false)
        offsets = Offsets(_EMPTY_BYTES, offs_arr)
        child_arr_ptr = _cchild_arr(arr, 0)
        child_sch_ptr = _cchild_sch(sch, 0)
        A = _import_arrowvec(child_arr_ptr, child_sch_ptr, handle, convert)
        T_child = eltype(A)
        ST = SubArray{T_child,1,typeof(A),Tuple{UnitRange{Int64}},true}
        T = nullable ? Union{ST,Missing} : ST
        return List{T,OT,typeof(A)}(_EMPTY_BYTES, validity, offsets, A, len, meta)
    end

    # Fixed-size list "+w:N"
    if startswith(fmt, "+w:")
        N = parse(Int, fmt[4:end])
        child_arr_ptr = _cchild_arr(arr, 0)
        child_sch_ptr = _cchild_sch(sch, 0)
        A = _import_arrowvec(child_arr_ptr, child_sch_ptr, handle, convert)
        T_child = eltype(A)
        T_inner = NTuple{N,T_child}
        T = nullable ? Union{T_inner,Missing} : T_inner
        return FixedSizeList{T,typeof(A)}(_EMPTY_BYTES, validity, A, len, meta)
    end

    # Struct "+s"
    if fmt == "+s"
        vecs = AbstractVector[]
        child_names = Symbol[]
        child_types = Type[]
        for i = 0:(Int(sch.n_children) - 1)
            child_av =
                _import_arrowvec(_cchild_arr(arr, i), _cchild_sch(sch, i), handle, convert)
            push!(vecs, child_av)
            child_sch_i = unsafe_load(_cchild_sch(sch, i))
            nm =
                child_sch_i.name != C_NULL ? Symbol(unsafe_string(child_sch_i.name)) :
                Symbol("f$i")
            push!(child_names, nm)
            push!(child_types, eltype(child_av))
        end
        fnames = Tuple(child_names)
        data = Tuple(vecs)
        NT = NamedTuple{fnames,Tuple{child_types...}}
        T = nullable ? Union{NT,Missing} : NT
        return Struct{T,typeof(data),fnames}(validity, data, len, meta)
    end

    # Map "+m"
    if fmt == "+m"
        optr = Ptr{Int32}(_cbuf(arr, 1))
        offs_arr = optr == C_NULL ? Int32[] : unsafe_wrap(Array, optr + off * sizeof(Int32), len + 1; own=false)
        offsets = Offsets(_EMPTY_BYTES, offs_arr)
        # child[0] is entries struct (key + value fields)
        A = _import_arrowvec(_cchild_arr(arr, 0), _cchild_sch(sch, 0), handle, convert)
        T_entry = eltype(A)
        # Build Dict type from entry type
        if T_entry <: NamedTuple
            K = fieldtype(T_entry, :key)
            V = fieldtype(T_entry, :value)
            T_inner = Dict{K,V}
        else
            T_inner = Dict{Any,Any}
        end
        T = nullable ? Union{T_inner,Missing} : T_inner
        return Map{T,Int32,typeof(A)}(validity, offsets, A, len, meta)
    end

    # Dense union "+ud:typeIds"
    if startswith(fmt, "+ud:")
        typeids_str = fmt[5:end]
        typeids_parsed =
            isempty(typeids_str) ? nothing :
            Tuple(parse(Int32, s) for s in split(typeids_str, ','))
        tptr = Ptr{UInt8}(_cbuf(arr, 0))
        n = len + off
        typeids_vec = tptr == C_NULL ? _EMPTY_BYTES : unsafe_wrap(Array, tptr, n; own=false)
        optr = Ptr{Int32}(_cbuf(arr, 1))
        offsets_vec = optr == C_NULL ? Int32[] : unsafe_wrap(Array, optr, n; own=false)
        vecs = AbstractVector[]
        child_types = Type[]
        for i = 0:(Int(sch.n_children) - 1)
            cv = _import_arrowvec(_cchild_arr(arr, i), _cchild_sch(sch, i), handle, convert)
            push!(vecs, cv)
            push!(child_types, eltype(cv))
        end
        data = Tuple(vecs)
        U_types = Tuple{child_types...}
        UT = UnionT{Meta.UnionMode.Dense,typeids_parsed,U_types}
        T = Union{child_types...}
        return DenseUnion{T,UT,typeof(data)}(
            _EMPTY_BYTES,
            _EMPTY_BYTES,
            typeids_vec,
            offsets_vec,
            data,
            meta,
        )
    end

    # Sparse union "+us:typeIds"
    if startswith(fmt, "+us:")
        typeids_str = fmt[5:end]
        typeids_parsed =
            isempty(typeids_str) ? nothing :
            Tuple(parse(Int32, s) for s in split(typeids_str, ','))
        tptr = Ptr{UInt8}(_cbuf(arr, 0))
        n = len + off
        typeids_vec = tptr == C_NULL ? _EMPTY_BYTES : unsafe_wrap(Array, tptr, n; own=false)
        vecs = AbstractVector[]
        child_types = Type[]
        for i = 0:(Int(sch.n_children) - 1)
            cv = _import_arrowvec(_cchild_arr(arr, i), _cchild_sch(sch, i), handle, convert)
            push!(vecs, cv)
            push!(child_types, eltype(cv))
        end
        data = Tuple(vecs)
        U_types = Tuple{child_types...}
        UT = UnionT{Meta.UnionMode.Sparse,typeids_parsed,U_types}
        T = Union{child_types...}
        return SparseUnion{T,UT,typeof(data)}(_EMPTY_BYTES, typeids_vec, data, meta)
    end

    # Dictionary encoded: schema.dictionary != C_NULL
    if sch.dictionary != C_NULL
        S = _fmt_to_storage_type(fmt)  # index type (e.g., Int8)
        iptr = Ptr{S}(_cbuf(arr, 1))
        idx_arr = iptr == C_NULL ? S[] : unsafe_wrap(Array, iptr + off * sizeof(S), len; own=false)
        idx_vec = Vector{S}(idx_arr)  # make a copy since DictEncoded.indices is Vector{S}
        dict_arr_ptr = arr.dictionary
        dict_sch_ptr = sch.dictionary
        dict_vec = _import_arrowvec(dict_arr_ptr, dict_sch_ptr, handle, convert)
        T_val = eltype(dict_vec)
        ordered = (sch.flags & CDATA_FLAG_DICT_ORDERED) != 0
        encoding = DictEncoding{T_val,S,typeof(dict_vec)}(0, dict_vec, ordered, nothing)
        T = nullable ? Union{T_val,Missing} : T_val
        return DictEncoded{T,S,typeof(dict_vec)}(_EMPTY_BYTES, validity, idx_vec, encoding, meta)
    end

    # Primitive numeric / time types
    S = _fmt_to_storage_type(fmt)
    T = nullable ? Union{S,Missing} : S
    dptr = Ptr{S}(_cbuf(arr, 1))
    if dptr == C_NULL
        return Primitive(T, _EMPTY_BYTES, validity, S[], len, meta)
    end
    data_arr = unsafe_wrap(Array, dptr + off * sizeof(S), len; own=false)
    return Primitive(T, _EMPTY_BYTES, validity, data_arr, len, meta)
end

"""
    Arrow.from_c_data(schema_ptr, array_ptr; convert=true) -> CImportedArray

Import an Arrow array from the Arrow C Data Interface. `schema_ptr` and `array_ptr`
are pointers (`Ptr{Cvoid}` or `Ptr{Arrow.ArrowSchema}`/`Ptr{Arrow.ArrowArray}`) to
the respective C structs. The caller transfers ownership of the C structs to Julia;
the C `release` callbacks will be called when the returned `CImportedArray` is GC'd
or when `Arrow.release_c_data` is called on it.

# Example
```julia
# schema_ptr and array_ptr come from a C library call
col = Arrow.from_c_data(schema_ptr, array_ptr)
collect(col)  # materialise elements
Arrow.release_c_data(col)  # or let GC handle it
```
"""
function from_c_data(schema_ptr::Ptr{Cvoid}, array_ptr::Ptr{Cvoid}; convert::Bool=true)
    sp = Ptr{ArrowSchema}(schema_ptr)
    ap = Ptr{ArrowArray}(array_ptr)
    handle = CDataHandle(sp, ap)
    vec = _import_arrowvec(ap, sp, handle, convert)
    T = eltype(vec)
    return CImportedArray{T}(vec, handle)
end

from_c_data(sp::Ptr{ArrowSchema}, ap::Ptr{ArrowArray}; kw...) =
    from_c_data(Ptr{Cvoid}(sp), Ptr{Cvoid}(ap); kw...)

"""
    Arrow.from_c_data(schema_ptrs, array_ptrs; names, convert=true) -> CImportedTable

Import multiple Arrow arrays as a table from the Arrow C Data Interface.
`schema_ptrs` and `array_ptrs` are iterables of pointers to C structs.
`names` is an optional vector of `Symbol` for column names; if not provided,
names are read from the schema `name` field.
"""
function from_c_data(
    schema_ptrs,
    array_ptrs;
    names::Union{Nothing,Vector{Symbol}}=nothing,
    convert::Bool=true,
    metadata=nothing,
)
    nodes = [_build_schema_node(Ptr{ArrowSchema}(sp)) for sp in schema_ptrs]
    n = length(nodes)
    col_names = if names !== nothing
        names
    else
        [node.name == Symbol("") ? Symbol("col$i") : node.name
         for (i, node) in enumerate(nodes)]
    end
    lookup = Dict{Symbol,Int}(col_names[i] => i for i in 1:n)
    schema = TableSchema(nodes, col_names, lookup)
    ptrs = [Ptr{ArrowArray}(ap) for ap in array_ptrs]
    return CImportedTable(schema, ptrs, nothing, metadata)
end

###############################################################################
# Arrow C Stream Interface                                                    #
###############################################################################

"""
    Arrow.FFI_ArrowArrayStream

Mirrors `struct ArrowArrayStream` from the Arrow C Stream Interface spec.
All 5 fields are pointer-sized; total size is 40 bytes on 64-bit.
"""
mutable struct FFI_ArrowArrayStream
    get_schema::Ptr{Cvoid}
    get_next::Ptr{Cvoid}
    get_last_error::Ptr{Cvoid}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

@assert sizeof(FFI_ArrowArrayStream) == 5 * 8 "FFI_ArrowArrayStream size mismatch"

"""
    Arrow.parse_c_schema(ptr) -> SchemaNode
    Arrow.parse_c_schema(ptrs) -> TableSchema

Parse one `ArrowSchema` pointer into a [`SchemaNode`](@ref), or an iterable of
pointers into a [`TableSchema`](@ref). The C structs are read but their `release`
callbacks are **not** called — the caller retains ownership.

# Example — single column
```julia
node = Arrow.parse_c_schema(schema_ptr)
for ap in batch_array_ptrs
    col = Arrow.from_c_data(node, ap)
end
```

# Example — table
```julia
schema = Arrow.parse_c_schema(schema_ptrs)
for batch_array_ptrs in stream
    tbl = Arrow.from_c_data(schema, batch_array_ptrs)
end
```
"""
parse_c_schema(ptr::Ptr{Cvoid}) = _build_schema_node(Ptr{ArrowSchema}(ptr))
parse_c_schema(ptr::Ptr{ArrowSchema}) = _build_schema_node(ptr)

function parse_c_schema(ptrs)
    nodes = [_build_schema_node(Ptr{ArrowSchema}(p)) for p in ptrs]
    col_names = [node.name == Symbol("") ? Symbol("col$i") : node.name
                 for (i, node) in enumerate(nodes)]
    lookup = Dict{Symbol,Int}(col_names[i] => i for i in eachindex(col_names))
    return TableSchema(nodes, col_names, lookup)
end

# Internal implementation — see parse_c_schema for the public entry point.
"""
    _build_schema_node(sch_ptr) -> SchemaNode

Recursively parse an `ArrowSchema` tree into a `SchemaNode` tree.
"""
function _build_schema_node(sch_ptr::Ptr{ArrowSchema})::SchemaNode
    sch = unsafe_load(sch_ptr)
    fmt = unsafe_string(sch.format)
    nm = sch.name != C_NULL ? Symbol(unsafe_string(sch.name)) : Symbol("")
    nullable = (sch.flags & CDATA_FLAG_NULLABLE) != 0
    meta = _parse_c_metadata(sch.metadata)
    nc = Int(sch.n_children)
    children = Vector{SchemaNode}(undef, nc)
    for i = 0:(nc - 1)
        children[i + 1] = _build_schema_node(_cchild_sch(sch, i))
    end
    has_dict = sch.dictionary != C_NULL
    dict_node = has_dict ? _build_schema_node(sch.dictionary) : nothing

    # Pre-parse format string once so _import_arrowvec_fast does no string work per batch
    fixed_size = 0
    type_ids   = nothing
    storage_type::Type = Missing
    kind = if has_dict
        storage_type = _fmt_to_storage_type(fmt)
        CKIND_DICT
    elseif fmt == "n";  CKIND_NULL
    elseif fmt == "b";  CKIND_BOOL
    elseif fmt == "u";  CKIND_STR32
    elseif fmt == "U";  CKIND_STR64
    elseif fmt == "z";  CKIND_BIN32
    elseif fmt == "Z";  CKIND_BIN64
    elseif fmt == "+l"; CKIND_LIST32
    elseif fmt == "+L"; CKIND_LIST64
    elseif fmt == "+s"; CKIND_STRUCT
    elseif fmt == "+m"; CKIND_MAP
    elseif startswith(fmt, "w:")
        fixed_size = parse(Int, fmt[3:end])
        CKIND_FIXED_BIN
    elseif startswith(fmt, "+w:")
        fixed_size = parse(Int, fmt[4:end])
        CKIND_FIXED_LIST
    elseif startswith(fmt, "+ud:")
        ts = fmt[5:end]
        type_ids = isempty(ts) ? nothing : Tuple(parse(Int32, s) for s in split(ts, ','))
        CKIND_DENSE_UNION
    elseif startswith(fmt, "+us:")
        ts = fmt[5:end]
        type_ids = isempty(ts) ? nothing : Tuple(parse(Int32, s) for s in split(ts, ','))
        CKIND_SPARSE_UNION
    else
        storage_type = _fmt_to_storage_type(fmt)
        CKIND_PRIM
    end

    return SchemaNode(fmt, nm, nullable, sch.flags, nc, children, has_dict, dict_node, meta,
                      kind, storage_type, fixed_size, type_ids)
end

# Import an ArrowVector using a pre-cached SchemaNode — zero string parsing per batch.
function _import_arrowvec_fast(
    arr_ptr::Ptr{ArrowArray},
    node::SchemaNode,
)
    arr = unsafe_load(arr_ptr)
    k   = node.kind
    len = Int(arr.length)
    off = Int(arr.offset)
    nullable = node.nullable
    meta     = node.meta
    validity = _import_validity(arr, len, off)

    k == CKIND_NULL && return NullVector{Missing}(MissingVector(len), meta)

    if k == CKIND_BOOL
        T = nullable ? Union{Bool,Missing} : Bool
        bdptr = Ptr{UInt8}(_cbuf(arr, 1))
        bdptr == C_NULL && return BoolVector{T}(_EMPTY_BYTES, 1, validity, len, meta)
        n_bytes = cld(len + off, 8)
        bool_bytes = unsafe_wrap(Array, bdptr, n_bytes; own=false)
        off % 8 == 0 && return BoolVector{T}(bool_bytes, off ÷ 8 + 1, validity, len, meta)
        return BoolVector{T}(_copy_bit_range(bool_bytes, off, len), 1, validity, len, meta)
    end

    if k == CKIND_FIXED_BIN
        N = node.fixed_size
        T_inner = NTuple{N,UInt8}
        T = nullable ? Union{T_inner,Missing} : T_inner
        dptr = Ptr{UInt8}(_cbuf(arr, 1))
        data = dptr == C_NULL ? CBuffer{UInt8}(Ptr{UInt8}(C_NULL), 0) :
                                CBuffer{UInt8}(dptr + off * N, len * N)
        return FixedSizeList{T,CBuffer{UInt8}}(_EMPTY_BYTES, validity, data, len, meta)
    end

    if k == CKIND_STR32 || k == CKIND_BIN32
        T_inner = k == CKIND_STR32 ? String : Base.CodeUnits{UInt8,String}
        T = nullable ? Union{T_inner,Missing} : T_inner
        optr = Ptr{Int32}(_cbuf(arr, 1))
        offsets = COffsets{Int32}(optr + off, len)
        dptr = Ptr{UInt8}(_cbuf(arr, 2))
        data_len = optr != C_NULL ? Int(unsafe_load(optr + off, len + 1)) : 0
        data_bytes = dptr == C_NULL ? CBuffer{UInt8}(Ptr{UInt8}(C_NULL), 0) :
                                       CBuffer{UInt8}(dptr, data_len)
        return List{T,Int32,CBuffer{UInt8},COffsets{Int32}}(_EMPTY_BYTES, validity, offsets, data_bytes, len, meta)
    end

    if k == CKIND_STR64 || k == CKIND_BIN64
        T_inner = k == CKIND_STR64 ? String : Base.CodeUnits{UInt8,String}
        T = nullable ? Union{T_inner,Missing} : T_inner
        optr = Ptr{Int64}(_cbuf(arr, 1))
        offsets = COffsets{Int64}(optr + off, len)
        dptr = Ptr{UInt8}(_cbuf(arr, 2))
        data_len = optr != C_NULL ? Int(unsafe_load(optr + off, len + 1)) : 0
        data_bytes = dptr == C_NULL ? CBuffer{UInt8}(Ptr{UInt8}(C_NULL), 0) :
                                       CBuffer{UInt8}(dptr, data_len)
        return List{T,Int64,CBuffer{UInt8},COffsets{Int64}}(_EMPTY_BYTES, validity, offsets, data_bytes, len, meta)
    end

    if k == CKIND_LIST32
        optr = Ptr{Int32}(_cbuf(arr, 1))
        offsets = COffsets{Int32}(optr + off, len)
        A = _import_arrowvec_fast(_cchild_arr(arr, 0), node.children[1])
        T_child = eltype(A)
        ST = SubArray{T_child,1,typeof(A),Tuple{UnitRange{Int64}},true}
        T = nullable ? Union{ST,Missing} : ST
        return List{T,Int32,typeof(A),COffsets{Int32}}(_EMPTY_BYTES, validity, offsets, A, len, meta)
    end

    if k == CKIND_LIST64
        optr = Ptr{Int64}(_cbuf(arr, 1))
        offsets = COffsets{Int64}(optr + off, len)
        A = _import_arrowvec_fast(_cchild_arr(arr, 0), node.children[1])
        T_child = eltype(A)
        ST = SubArray{T_child,1,typeof(A),Tuple{UnitRange{Int64}},true}
        T = nullable ? Union{ST,Missing} : ST
        return List{T,Int64,typeof(A),COffsets{Int64}}(_EMPTY_BYTES, validity, offsets, A, len, meta)
    end

    if k == CKIND_FIXED_LIST
        N = node.fixed_size
        A = _import_arrowvec_fast(_cchild_arr(arr, 0), node.children[1])
        T_child = eltype(A)
        T_inner = NTuple{N,T_child}
        T = nullable ? Union{T_inner,Missing} : T_inner
        return FixedSizeList{T,typeof(A)}(_EMPTY_BYTES, validity, A, len, meta)
    end

    if k == CKIND_STRUCT
        vecs = AbstractVector[]
        child_names = Symbol[]
        child_types = Type[]
        for i = 0:(node.n_children - 1)
            child_av = _import_arrowvec_fast(_cchild_arr(arr, i), node.children[i + 1])
            push!(vecs, child_av)
            push!(child_names, node.children[i + 1].name)
            push!(child_types, eltype(child_av))
        end
        fnames = Tuple(child_names)
        data = Tuple(vecs)
        NT = NamedTuple{fnames,Tuple{child_types...}}
        T = nullable ? Union{NT,Missing} : NT
        return Struct{T,typeof(data),fnames}(validity, data, len, meta)
    end

    if k == CKIND_MAP
        optr = Ptr{Int32}(_cbuf(arr, 1))
        offsets = COffsets{Int32}(optr, len + off)
        A = _import_arrowvec_fast(_cchild_arr(arr, 0), node.children[1])
        T_entry = eltype(A)
        T_inner = T_entry <: NamedTuple ? Dict{fieldtype(T_entry,:key),fieldtype(T_entry,:value)} : Dict{Any,Any}
        T = nullable ? Union{T_inner,Missing} : T_inner
        return Map{T,Int32,typeof(A),COffsets{Int32}}(validity, offsets, A, len, meta)
    end

    if k == CKIND_DENSE_UNION
        tptr = Ptr{UInt8}(_cbuf(arr, 0)); n = len + off
        typeids_vec = tptr == C_NULL ? _EMPTY_BYTES : unsafe_wrap(Array, tptr, n; own=false)
        optr = Ptr{Int32}(_cbuf(arr, 1))
        offsets_vec = optr == C_NULL ? Int32[] : unsafe_wrap(Array, optr, n; own=false)
        vecs = AbstractVector[]; child_types = Type[]
        for i = 0:(node.n_children - 1)
            cv = _import_arrowvec_fast(_cchild_arr(arr, i), node.children[i + 1])
            push!(vecs, cv); push!(child_types, eltype(cv))
        end
        data = Tuple(vecs); U_types = Tuple{child_types...}
        UT = UnionT{Meta.UnionMode.Dense,node.type_ids,U_types}
        return DenseUnion{Union{child_types...},UT,typeof(data)}(
            _EMPTY_BYTES, _EMPTY_BYTES, typeids_vec, offsets_vec, data, meta)
    end

    if k == CKIND_SPARSE_UNION
        tptr = Ptr{UInt8}(_cbuf(arr, 0)); n = len + off
        typeids_vec = tptr == C_NULL ? _EMPTY_BYTES : unsafe_wrap(Array, tptr, n; own=false)
        vecs = AbstractVector[]; child_types = Type[]
        for i = 0:(node.n_children - 1)
            cv = _import_arrowvec_fast(_cchild_arr(arr, i), node.children[i + 1])
            push!(vecs, cv); push!(child_types, eltype(cv))
        end
        data = Tuple(vecs); U_types = Tuple{child_types...}
        UT = UnionT{Meta.UnionMode.Sparse,node.type_ids,U_types}
        return SparseUnion{Union{child_types...},UT,typeof(data)}(_EMPTY_BYTES, typeids_vec, data, meta)
    end

    if k == CKIND_DICT
        dict_vec = _import_arrowvec_fast(arr.dictionary, node.dict_node::SchemaNode)
        T_val = eltype(dict_vec)
        ordered = (node.flags & CDATA_FLAG_DICT_ORDERED) != 0
        T = nullable ? Union{T_val,Missing} : T_val
        idx_vec = _make_dict_indices(arr, len, off, Val(node.storage_type))
        S = node.storage_type
        encoding = DictEncoding{T_val,S,typeof(dict_vec)}(0, dict_vec, ordered, nothing)
        return DictEncoded{T,S,typeof(dict_vec)}(_EMPTY_BYTES, validity, idx_vec, encoding, meta)
    end

    # CKIND_PRIM — dispatch on the concrete storage type via Val so S is known at compile time
    return _import_prim_fast(arr, validity, len, off, nullable, meta, Val(node.storage_type))
end

# Specialised only on S (index type: Int8/Int16/Int32/Int64) so CBuffer{S} is proven isbits.
# Avoids specialising on the dict value type A, which could explode combinatorially.
@generated function _make_dict_indices(arr::ArrowArray, len::Int, off::Int, ::Val{S}) where {S}
    quote
        iptr = Ptr{$S}(_cbuf(arr, 1))
        iptr == C_NULL && return $S[]
        return Vector{$S}(CBuffer{$S}(iptr + off, len))
    end
end

@generated function _import_prim_fast(
    arr::ArrowArray, validity::ValidityBitmap, len::Int, off::Int,
    nullable::Bool, meta::Union{Nothing,Base.ImmutableDict{String,String}},
    ::Val{S},
) where {S}
    quote
        T = nullable ? Union{$S,Missing} : $S
        dptr = Ptr{$S}(_cbuf(arr, 1))
        dptr == C_NULL && return Primitive(T, _EMPTY_BYTES, validity, $S[], len, meta)
        return Primitive(T, _EMPTY_BYTES, validity, CBuffer{$S}(dptr + off, len), len, meta)
    end
end

"""
    Arrow.from_c_data(node::SchemaNode, array_ptr; convert=true) -> CImportedArray

Import an Arrow array using a pre-parsed [`SchemaNode`](@ref), skipping all
schema parsing. `array_ptr` is a `Ptr` to an `ArrowArray` C struct; ownership
of the array is transferred to Julia.

Use [`Arrow.parse_c_schema`](@ref) to build the node once, then call this for
every subsequent batch.
"""
function from_c_data(node::SchemaNode, array_ptr::Ptr{Cvoid}; convert::Bool=true)
    ap = Ptr{ArrowArray}(array_ptr)
    handle = CDataHandle(Ptr{ArrowSchema}(C_NULL), ap)
    vec = _import_arrowvec_fast(ap, node)
    return CImportedArray{eltype(vec)}(vec, handle)
end

from_c_data(node::SchemaNode, ap::Ptr{ArrowArray}; kw...) =
    from_c_data(node, Ptr{Cvoid}(ap); kw...)

"""
    Arrow.from_c_data(schema::TableSchema, array_ptrs; metadata=nothing) -> CImportedTable

Import a batch of Arrow arrays using a pre-parsed [`TableSchema`](@ref). No schema
work is done per batch — column names and the name→index lookup are reused directly
from the schema.

Obtain a `TableSchema` via `Arrow.parse_c_schema(schema_ptrs)`.
"""
function from_c_data(schema::TableSchema, array_ptrs; metadata=nothing)
    ptrs = [Ptr{ArrowArray}(ap) for ap in array_ptrs]
    return CImportedTable(schema, ptrs, nothing, metadata)
end

# Convenience: accept a plain vector of nodes (builds TableSchema on the fly)
function from_c_data(nodes::AbstractVector{SchemaNode}, array_ptrs; kw...)
    col_names = [node.name == Symbol("") ? Symbol("col$i") : node.name
                 for (i, node) in enumerate(nodes)]
    lookup = Dict{Symbol,Int}(col_names[i] => i for i in eachindex(col_names))
    return from_c_data(TableSchema(nodes, col_names, lookup), array_ptrs; kw...)
end

"""
    CStreamHandle

Wraps a heap-stable `Ref{FFI_ArrowArrayStream}` together with the cached
`SchemaNode` tree and column names.  Obtain via `open_c_stream`; iterate with
`next_c_stream_batch!`; release with `close_c_stream!`.
"""
mutable struct CStreamHandle
    stream::Ref{FFI_ArrowArrayStream}
    top_node::SchemaNode              # cached top-level "+s" struct node
    schema_nodes::Vector{SchemaNode}  # == top_node.children
    col_names::Vector{Symbol}
    col_lookup::Dict{Symbol,Int}      # name → column index; built once at open time
    stream_schema::TableSchema        # pre-built for CImportedTable construction
    released::Bool
end

"""
    open_c_stream(stream_ref::Ref{FFI_ArrowArrayStream}) -> CStreamHandle

Call `get_schema` exactly once on `stream_ref`, parse the schema tree into
`SchemaNode`s, release the C schema struct, and return a handle.
"""
function open_c_stream(stream_ref::Ref{FFI_ArrowArrayStream})::CStreamHandle
    stream_ptr = Base.unsafe_convert(Ptr{FFI_ArrowArrayStream}, stream_ref)
    get_schema_fn = stream_ref[].get_schema

    # Allocate a C-heap ArrowSchema for get_schema to write into.  Using Libc.malloc
    # avoids GC lifetime concerns around the temporary pointer.
    sch_ptr = Ptr{ArrowSchema}(Libc.malloc(sizeof(ArrowSchema)))
    sch_ptr == C_NULL && error("malloc failed for ArrowSchema")

    ret = ccall(get_schema_fn, Int32, (Ptr{FFI_ArrowArrayStream}, Ptr{ArrowSchema}),
                stream_ptr, sch_ptr)
    if ret != 0
        Libc.free(sch_ptr)
        error("FFI_ArrowArrayStream.get_schema returned $ret")
    end

    top_node = _build_schema_node(sch_ptr)

    # Release the C schema (frees arrow-rs buffers).
    sch = unsafe_load(sch_ptr)
    if sch.release != C_NULL
        ccall(sch.release, Cvoid, (Ptr{ArrowSchema},), sch_ptr)
    end
    Libc.free(sch_ptr)

    col_names = [child.name for child in top_node.children]
    col_lookup = Dict{Symbol,Int}(col_names[i] => i for i in eachindex(col_names))
    stream_schema = TableSchema(top_node.children, col_names, col_lookup)
    return CStreamHandle(stream_ref, top_node, top_node.children, col_names, col_lookup, stream_schema, false)
end

"""
    next_c_stream_batch!(handle::CStreamHandle) -> Union{CImportedTable, Nothing}

Fetch the next batch.  Returns a `CImportedTable` on success (zero-copy views
into Rust memory), or `nothing` at end of stream.

Call `release_c_data(table)` when done; the underlying Rust memory is freed
at that point.
"""
function next_c_stream_batch!(handle::CStreamHandle)::Union{CImportedTable,Nothing}
    handle.released && return nothing
    stream_ptr = Base.unsafe_convert(Ptr{FFI_ArrowArrayStream}, handle.stream)

    # Allocate a C-heap ArrowArray struct for get_next to write into.
    arr_ptr = Ptr{ArrowArray}(Libc.malloc(sizeof(ArrowArray)))
    arr_ptr == C_NULL && error("malloc failed for ArrowArray")

    get_next_fn = handle.stream[].get_next
    ret = ccall(get_next_fn, Int32, (Ptr{FFI_ArrowArrayStream}, Ptr{ArrowArray}),
                stream_ptr, arr_ptr)
    if ret != 0
        Libc.free(arr_ptr)
        error("FFI_ArrowArrayStream.get_next returned $ret")
    end

    # End-of-stream: Rust zeroed the struct, so release == C_NULL.
    loaded = unsafe_load(arr_ptr)
    if loaded.release == C_NULL
        Libc.free(arr_ptr)
        return nothing
    end

    # One shared CDataHandle owns arr_ptr: calling release_c_data(table) releases
    # the root, which frees all child column arrays at once (Arrow C spec).
    shared_h = CDataHandle(Ptr{ArrowSchema}(C_NULL), arr_ptr, false, true)

    n_cols = length(handle.schema_nodes)
    child_ptrs = [_cchild_arr(loaded, i - 1) for i = 1:n_cols]
    return CImportedTable(handle.stream_schema, child_ptrs, shared_h, nothing)
end

"""
    close_c_stream!(handle::CStreamHandle)

Call the stream's `release` callback and mark the handle as released.
After this the handle must not be used.
"""
function close_c_stream!(handle::CStreamHandle)
    handle.released && return
    handle.released = true
    stream_ptr = Base.unsafe_convert(Ptr{FFI_ArrowArrayStream}, handle.stream)
    release_fn = handle.stream[].release
    release_fn != C_NULL &&
        ccall(release_fn, Cvoid, (Ptr{FFI_ArrowArrayStream},), stream_ptr)
    return nothing
end

###############################################################################
# Export path (Julia → C)                                                     #
###############################################################################

# Global roots dict: keeps Julia objects alive while C holds pointers
const _EXPORT_ROOTS = Dict{UInt64,Vector{Any}}()
const _EXPORT_ROOTS_LOCK = ReentrantLock()
const _EXPORT_TOKEN = Threads.Atomic{UInt64}(0)

function _next_export_token()
    return Threads.atomic_add!(_EXPORT_TOKEN, UInt64(1))
end

# Release callbacks (function pointers set in __init__)
# Declared as globals here; assigned in Arrow.__init__
global _SCHEMA_RELEASE_CFUNC::Ptr{Cvoid} = C_NULL
global _ARRAY_RELEASE_CFUNC::Ptr{Cvoid} = C_NULL

function _release_exported_schema(ptr::Ptr{ArrowSchema})::Cvoid
    sch = unsafe_load(ptr)
    sch.release == C_NULL && return nothing
    token = UInt64(UInt(sch.private_data))
    @lock _EXPORT_ROOTS_LOCK delete!(_EXPORT_ROOTS, token)
    # Null out release to signal completion (prevents double-free)
    # ArrowSchema.release is the 8th field; all fields are pointer-sized (8 bytes each)
    release_offset = 7 * sizeof(Ptr{Cvoid})   # 0-indexed: field 8 is at offset 7*8
    unsafe_store!(Ptr{Ptr{Cvoid}}(UInt(ptr) + release_offset), C_NULL)
    return nothing
end

function _release_exported_array(ptr::Ptr{ArrowArray})::Cvoid
    arr = unsafe_load(ptr)
    arr.release == C_NULL && return nothing
    token = UInt64(UInt(arr.private_data))
    @lock _EXPORT_ROOTS_LOCK delete!(_EXPORT_ROOTS, token)
    # Null out release (prevents double-free)
    # ArrowArray.release is the 9th field; all fields are pointer-sized (8 bytes each)
    release_offset = 8 * sizeof(Ptr{Cvoid})   # 0-indexed: field 9 is at offset 8*8
    unsafe_store!(Ptr{Ptr{Cvoid}}(UInt(ptr) + release_offset), C_NULL)
    return nothing
end

# Serialize Julia metadata dict to Arrow C Data Interface binary format
function _serialize_c_metadata(
    meta::Union{Nothing,AbstractDict{String,String}},
)::Vector{UInt8}
    (meta === nothing || isempty(meta)) && return UInt8[]
    buf = IOBuffer()
    Base.write(buf, Int32(length(meta)))
    for (k, v) in meta
        kb = codeunits(k)
        Base.write(buf, Int32(length(kb)))
        Base.write(buf, kb)
        vb = codeunits(v)
        Base.write(buf, Int32(length(vb)))
        Base.write(buf, vb)
    end
    return take!(buf)
end

# Map ArrowVector type to C Data Interface format string
function _array_to_format(v::ArrowVector)
    T = eltype(v)
    T === Missing && return "n"
    S = Base.nonmissingtype(T)
    S === Union{} && return "n"
    return _type_to_format(S, v)
end

_array_to_format(v::NullVector) = "n"

function _type_to_format(S::Type, v::ArrowVector)
    S === Missing && return "n"
    S === Bool && return "b"
    S === Int8 && return "c"
    S === UInt8 && return "C"
    S === Int16 && return "s"
    S === UInt16 && return "S"
    S === Int32 && return "i"
    S === UInt32 && return "I"
    S === Int64 && return "l"
    S === UInt64 && return "L"
    S === Float16 && return "e"
    S === Float32 && return "f"
    S === Float64 && return "g"
    return _type_to_format_extended(S, v)
end

function _type_to_format_extended(S::Type, v::ArrowVector)
    # Time types
    if S === Date{Meta.DateUnit.DAY,Int32}
        return "tdD"
    elseif S === Date{Meta.DateUnit.MILLISECOND,Int64}
        return "tdm"
    elseif S === Time{Meta.TimeUnit.SECOND,Int32}
        return "tts"
    elseif S === Time{Meta.TimeUnit.MILLISECOND,Int32}
        return "ttm"
    elseif S === Time{Meta.TimeUnit.MICROSECOND,Int64}
        return "ttu"
    elseif S === Time{Meta.TimeUnit.NANOSECOND,Int64}
        return "ttn"
    elseif S <: Timestamp
        U = S.parameters[1]
        TZ = S.parameters[2]
        unit_char =
            U === Meta.TimeUnit.SECOND ? 's' :
            U === Meta.TimeUnit.MILLISECOND ? 'm' :
            U === Meta.TimeUnit.MICROSECOND ? 'u' : 'n'
        tz_str = TZ === nothing ? "" : String(TZ)
        return "ts$(unit_char):$(tz_str)"
    elseif S <: Duration
        U = S.parameters[1]
        unit_char =
            U === Meta.TimeUnit.SECOND ? 's' :
            U === Meta.TimeUnit.MILLISECOND ? 'm' :
            U === Meta.TimeUnit.MICROSECOND ? 'u' : 'n'
        return "tD$(unit_char)"
    elseif S === Interval{Meta.IntervalUnit.YEAR_MONTH,Int32}
        return "tiM"
    elseif S === Interval{Meta.IntervalUnit.DAY_TIME,Int64}
        return "tiD"
    elseif S <: Decimal
        P = S.parameters[1]
        SC = S.parameters[2]
        T_val = S.parameters[3]
        bw = T_val === Int256 ? 256 : 128
        return "d:$(P),$(SC),$(bw)"
    end
    # Nested container types: dispatch on ArrowVector subtype
    return _container_to_format(v)
end

function _container_to_format(v::List{T,Int32,A}) where {T,A}
    S = Base.nonmissingtype(T)
    if S <: AbstractString
        return "u"
    elseif S <: Base.CodeUnits
        return "z"
    else
        return "+l"
    end
end

function _container_to_format(v::List{T,Int64,A}) where {T,A}
    S = Base.nonmissingtype(T)
    if S <: AbstractString
        return "U"
    elseif S <: Base.CodeUnits
        return "Z"
    else
        return "+L"
    end
end

function _container_to_format(v::FixedSizeList{T,A}) where {T,A}
    S = Base.nonmissingtype(T)
    N = ArrowTypes.getsize(ArrowTypes.ArrowKind(ArrowTypes.ArrowType(S)))
    if eltype(A) == UInt8 && S <: NTuple
        return "w:$(N)"
    else
        return "+w:$(N)"
    end
end

_container_to_format(v::Map) = "+m"
_container_to_format(v::Struct) = "+s"
_container_to_format(v::NullVector) = "n"

function _container_to_format(v::DenseUnion{T,UnionT{M,typeIds,U}}) where {T,M,typeIds,U}
    ids_str = typeIds === nothing ? join(0:(fieldcount(U) - 1), ',') : join(typeIds, ',')
    return "+ud:$(ids_str)"
end

function _container_to_format(v::SparseUnion{T,UnionT{M,typeIds,U}}) where {T,M,typeIds,U}
    ids_str = typeIds === nothing ? join(0:(fieldcount(U) - 1), ',') : join(typeIds, ',')
    return "+us:$(ids_str)"
end

function _container_to_format(v::DictEncoded{T,S,A}) where {T,S,A}
    # Format is the INDEX type
    S === Int8 && return "c"
    S === Int16 && return "s"
    S === Int32 && return "i"
    S === Int64 && return "l"
    return "i"  # fallback
end

_container_to_format(v::ArrowVector) =
    error("Cannot determine format string for $(typeof(v))")

# Compute schema flags from an ArrowVector
function _schema_flags(v::ArrowVector)
    T = eltype(v)
    flags = Int64(0)
    if T >: Missing
        flags |= CDATA_FLAG_NULLABLE
    end
    if v isa DictEncoded && v.encoding.isOrdered
        flags |= CDATA_FLAG_DICT_ORDERED
    end
    return flags
end
_schema_flags(v::NullVector) = CDATA_FLAG_NULLABLE

# Get a pointer to the validity bitmap bytes, or C_NULL if no nulls
function _validity_ptr(v::ArrowVector)
    bm = v.validity
    bm.nc == 0 && return C_NULL
    isempty(bm.bytes) && return C_NULL
    return Ptr{Cvoid}(pointer(bm.bytes, bm.pos))
end

# DenseUnion and SparseUnion have no validity bitmap
_validity_ptr(v::DenseUnion) = C_NULL
_validity_ptr(v::SparseUnion) = C_NULL
_validity_ptr(v::NullVector) = C_NULL

# Fill an ArrowSchema Ref for a given ArrowVector (recursive)
# roots: vector of Julia objects to keep alive (owned by the token entry in _EXPORT_ROOTS)
# token: key into _EXPORT_ROOTS for the top-level array
function _fill_schema!(
    out::Ref{ArrowSchema},
    v::ArrowVector,
    name::String,
    token::UInt64,
    roots::Vector{Any},
)
    fmt = _array_to_format(v)
    fmt_bytes = Vector{UInt8}(fmt * "\0")
    push!(roots, fmt_bytes)
    name_bytes = Vector{UInt8}(name * "\0")
    push!(roots, name_bytes)

    meta = getmetadata(v)
    meta_bytes = _serialize_c_metadata(meta)
    meta_ptr = isempty(meta_bytes) ? C_NULL : Cstring(pointer(meta_bytes))
    if !isempty(meta_bytes)
        push!(roots, meta_bytes)
    end

    flags = _schema_flags(v)

    # Build children schemas
    child_schema_refs, n_children, children_ptr = _make_child_schemas!(v, token, roots)

    # Dictionary schema
    dict_schema_ref, dict_ptr = _make_dict_schema!(v, token, roots)

    out[] = ArrowSchema(
        Cstring(pointer(fmt_bytes)),
        Cstring(pointer(name_bytes)),
        meta_ptr,
        flags,
        Int64(n_children),
        children_ptr,
        dict_ptr,
        _SCHEMA_RELEASE_CFUNC,
        Ptr{Cvoid}(UInt(token)),
    )
    push!(roots, child_schema_refs)
    push!(roots, dict_schema_ref)
    return out
end

# Build child ArrowSchema refs for a vector that has children
function _make_child_schemas!(v::ArrowVector, token::UInt64, roots::Vector{Any})
    return Ref{ArrowSchema}[], 0, Ptr{Ptr{ArrowSchema}}(C_NULL)
end

function _make_child_schemas!(
    v::Union{List,FixedSizeList,Map},
    token::UInt64,
    roots::Vector{Any},
)
    # These types have a single child
    if v isa List && liststringtype(v)
        # String/binary lists have no child array in C Data Interface
        return Ref{ArrowSchema}[], 0, Ptr{Ptr{ArrowSchema}}(C_NULL)
    end
    if v isa FixedSizeList
        T = eltype(v)
        S = Base.nonmissingtype(T)
        N = ArrowTypes.getsize(ArrowTypes.ArrowKind(ArrowTypes.ArrowType(S)))
        if eltype(v.data) == UInt8 && S <: NTuple
            # Fixed-size binary: no children
            return Ref{ArrowSchema}[], 0, Ptr{Ptr{ArrowSchema}}(C_NULL)
        end
    end
    child_ref = Ref{ArrowSchema}()
    _fill_schema!(child_ref, _get_child_vec(v), "", token, roots)
    child_ptr_vec = [Base.unsafe_convert(Ptr{ArrowSchema}, child_ref)]
    push!(roots, child_ptr_vec)
    push!(roots, child_ref)
    return [child_ref], 1, Ptr{Ptr{ArrowSchema}}(pointer(child_ptr_vec))
end

function _make_child_schemas!(v::Struct, token::UInt64, roots::Vector{Any})
    T = eltype(v)
    S = Base.nonmissingtype(T)
    fnames = fieldnames(S)
    child_refs = [Ref{ArrowSchema}() for _ in eachindex(v.data)]
    for i in eachindex(v.data)
        nm = i <= length(fnames) ? String(fnames[i]) : "f$(i-1)"
        _fill_schema!(child_refs[i], v.data[i], nm, token, roots)
    end
    child_ptr_vec = [Base.unsafe_convert(Ptr{ArrowSchema}, r) for r in child_refs]
    push!(roots, child_ptr_vec)
    push!(roots, child_refs)
    return child_refs, length(child_refs), Ptr{Ptr{ArrowSchema}}(pointer(child_ptr_vec))
end

function _make_child_schemas!(
    v::Union{DenseUnion,SparseUnion},
    token::UInt64,
    roots::Vector{Any},
)
    child_refs = [Ref{ArrowSchema}() for _ in eachindex(v.data)]
    for i in eachindex(v.data)
        _fill_schema!(child_refs[i], v.data[i], "", token, roots)
    end
    child_ptr_vec = [Base.unsafe_convert(Ptr{ArrowSchema}, r) for r in child_refs]
    push!(roots, child_ptr_vec)
    push!(roots, child_refs)
    return child_refs, length(child_refs), Ptr{Ptr{ArrowSchema}}(pointer(child_ptr_vec))
end

function _make_child_schemas!(v::DictEncoded, token::UInt64, roots::Vector{Any})
    return Ref{ArrowSchema}[], 0, Ptr{Ptr{ArrowSchema}}(C_NULL)
end

function _make_dict_schema!(v::ArrowVector, token::UInt64, roots::Vector{Any})
    return Ref{ArrowSchema}(), Ptr{ArrowSchema}(C_NULL)
end

function _make_dict_schema!(v::DictEncoded, token::UInt64, roots::Vector{Any})
    dict_ref = Ref{ArrowSchema}()
    _fill_schema!(dict_ref, v.encoding.data, "", token, roots)
    push!(roots, dict_ref)
    return dict_ref, Base.unsafe_convert(Ptr{ArrowSchema}, dict_ref)
end

# Helper to get the child vector for List/FixedSizeList/Map
_get_child_vec(v::List) = v.data
_get_child_vec(v::FixedSizeList) = v.data
_get_child_vec(v::Map) = v.data

# Specialisation for NullVector (has no validity field)
function _fill_array!(
    out::Ref{ArrowArray},
    v::NullVector,
    token::UInt64,
    roots::Vector{Any},
)
    len = Int64(length(v))
    out[] = ArrowArray(
        len,
        len,
        Int64(0),
        Int64(0),
        Int64(0),
        Ptr{Ptr{Cvoid}}(C_NULL),
        Ptr{Ptr{ArrowArray}}(C_NULL),
        Ptr{ArrowArray}(C_NULL),
        _ARRAY_RELEASE_CFUNC,
        Ptr{Cvoid}(UInt(token)),
    )
    return out
end

# Fill an ArrowArray Ref for a given ArrowVector (recursive)
function _fill_array!(
    out::Ref{ArrowArray},
    v::ArrowVector,
    token::UInt64,
    roots::Vector{Any},
)
    len = Int64(length(v))
    nc = Int64(nullcount(v))
    off = Int64(0)

    buffers, n_buffers = _make_buffers(v, roots)
    child_array_refs, n_children, children_arr_ptr = _make_child_arrays!(v, token, roots)
    dict_array_ref, dict_arr_ptr = _make_dict_array!(v, token, roots)

    out[] = ArrowArray(
        len,
        nc,
        off,
        Int64(n_buffers),
        Int64(n_children),
        buffers,
        children_arr_ptr,
        dict_arr_ptr,
        _ARRAY_RELEASE_CFUNC,
        Ptr{Cvoid}(UInt(token)),
    )
    push!(roots, child_array_refs)
    push!(roots, dict_array_ref)
    return out
end

# Build the buffers pointer array for a given ArrowVector
function _make_buffers(v::NullVector, roots::Vector{Any})
    return Ptr{Ptr{Cvoid}}(C_NULL), 0
end

function _make_buffers(v::BoolVector, roots::Vector{Any})
    vp = _validity_ptr(v)
    bytes = isempty(v.arrow) ? UInt8[] : v.arrow
    dp = isempty(bytes) ? C_NULL : Ptr{Cvoid}(pointer(bytes, v.pos))
    push!(roots, bytes)
    bufs = [vp, dp]
    push!(roots, bufs)
    if vp != C_NULL
        push!(roots, v.validity.bytes)
    end
    return Ptr{Ptr{Cvoid}}(pointer(bufs)), 2
end

function _make_buffers(v::Primitive, roots::Vector{Any})
    vp = _validity_ptr(v)
    # data may be a lazy wrapper (ToArrow, ToStruct, SubArray, etc.)
    # Materialise to a plain Vector of the storage element type
    S = eltype(v.data)
    data_vec = v.data isa Vector{S} ? v.data : collect(S, v.data)
    push!(roots, data_vec)
    dp = isempty(data_vec) ? C_NULL : Ptr{Cvoid}(pointer(data_vec))
    bufs = [vp, dp]
    push!(roots, bufs)
    if vp != C_NULL
        push!(roots, v.validity.bytes)
    end
    return Ptr{Ptr{Cvoid}}(pointer(bufs)), 2
end

function _make_buffers(v::List, roots::Vector{Any})
    vp = _validity_ptr(v)
    if vp != C_NULL
        push!(roots, v.validity.bytes)
    end
    if liststringtype(v)
        # data may be a ToList (lazy) or Vector{UInt8}; materialise to Vector{UInt8}
        data_bytes = v.data isa Vector{UInt8} ? v.data : collect(UInt8, v.data)
        push!(roots, data_bytes)
        dp = isempty(data_bytes) ? C_NULL : Ptr{Cvoid}(pointer(data_bytes))
        # offsets may also need materialising (view → copy)
        offs = let r = _raw_offsets(v.offsets); r isa Vector ? r : collect(r) end
        push!(roots, offs)
        op = Ptr{Cvoid}(pointer(offs))
        bufs = [vp, op, dp]
        push!(roots, bufs)
        return Ptr{Ptr{Cvoid}}(pointer(bufs)), 3
    else
        offs = let r = _raw_offsets(v.offsets); r isa Vector ? r : collect(r) end
        push!(roots, offs)
        op = Ptr{Cvoid}(pointer(offs))
        bufs = [vp, op]
        push!(roots, bufs)
        return Ptr{Ptr{Cvoid}}(pointer(bufs)), 2
    end
end

function _make_buffers(v::FixedSizeList, roots::Vector{Any})
    vp = _validity_ptr(v)
    if vp != C_NULL
        push!(roots, v.validity.bytes)
    end
    T = eltype(v)
    S = Base.nonmissingtype(T)
    # Fixed-size binary: 2 buffers (validity + data)
    if eltype(v.data) == UInt8 && S <: NTuple
        dp = isempty(v.data) ? C_NULL : Ptr{Cvoid}(pointer(v.data))
        !isempty(v.data) && push!(roots, v.data)
        bufs = [vp, dp]
        push!(roots, bufs)
        return Ptr{Ptr{Cvoid}}(pointer(bufs)), 2
    else
        # Fixed-size list: 1 buffer (validity only), child is separate
        bufs = [vp]
        push!(roots, bufs)
        return Ptr{Ptr{Cvoid}}(pointer(bufs)), 1
    end
end

function _make_buffers(v::Map, roots::Vector{Any})
    vp = _validity_ptr(v)
    offs = v.offsets.offsets isa Vector ? v.offsets.offsets : collect(v.offsets.offsets)
    push!(roots, offs)
    op = Ptr{Cvoid}(pointer(offs))
    if vp != C_NULL
        push!(roots, v.validity.bytes)
    end
    bufs = [vp, op]
    push!(roots, bufs)
    return Ptr{Ptr{Cvoid}}(pointer(bufs)), 2
end

function _make_buffers(v::Struct, roots::Vector{Any})
    vp = _validity_ptr(v)
    if vp != C_NULL
        push!(roots, v.validity.bytes)
    end
    bufs = [vp]
    push!(roots, bufs)
    return Ptr{Ptr{Cvoid}}(pointer(bufs)), 1
end

function _make_buffers(v::DenseUnion, roots::Vector{Any})
    tp = isempty(v.typeIds) ? C_NULL : Ptr{Cvoid}(pointer(v.typeIds))
    op = isempty(v.offsets) ? C_NULL : Ptr{Cvoid}(pointer(v.offsets))
    !isempty(v.typeIds) && push!(roots, v.typeIds)
    !isempty(v.offsets) && push!(roots, v.offsets)
    bufs = [tp, op]
    push!(roots, bufs)
    return Ptr{Ptr{Cvoid}}(pointer(bufs)), 2
end

function _make_buffers(v::SparseUnion, roots::Vector{Any})
    tp = isempty(v.typeIds) ? C_NULL : Ptr{Cvoid}(pointer(v.typeIds))
    !isempty(v.typeIds) && push!(roots, v.typeIds)
    bufs = [tp]
    push!(roots, bufs)
    return Ptr{Ptr{Cvoid}}(pointer(bufs)), 1
end

function _make_buffers(v::DictEncoded, roots::Vector{Any})
    vp = _validity_ptr(v)
    ip = isempty(v.indices) ? C_NULL : Ptr{Cvoid}(pointer(v.indices))
    !isempty(v.indices) && push!(roots, v.indices)
    if vp != C_NULL
        push!(roots, v.validity.bytes)
    end
    bufs = [vp, ip]
    push!(roots, bufs)
    return Ptr{Ptr{Cvoid}}(pointer(bufs)), 2
end

# Build child ArrowArray refs
function _make_child_arrays!(v::ArrowVector, token::UInt64, roots::Vector{Any})
    return Ref{ArrowArray}[], 0, Ptr{Ptr{ArrowArray}}(C_NULL)
end

function _make_child_arrays!(
    v::Union{List,FixedSizeList,Map},
    token::UInt64,
    roots::Vector{Any},
)
    if v isa List && liststringtype(v)
        return Ref{ArrowArray}[], 0, Ptr{Ptr{ArrowArray}}(C_NULL)
    end
    if v isa FixedSizeList
        T = eltype(v)
        S = Base.nonmissingtype(T)
        if eltype(v.data) == UInt8 && S <: NTuple
            return Ref{ArrowArray}[], 0, Ptr{Ptr{ArrowArray}}(C_NULL)
        end
    end
    child_ref = Ref{ArrowArray}()
    _fill_array!(child_ref, _get_child_vec(v), token, roots)
    child_ptr_vec = [Base.unsafe_convert(Ptr{ArrowArray}, child_ref)]
    push!(roots, child_ptr_vec)
    push!(roots, child_ref)
    return [child_ref], 1, Ptr{Ptr{ArrowArray}}(pointer(child_ptr_vec))
end

function _make_child_arrays!(v::Struct, token::UInt64, roots::Vector{Any})
    child_refs = [Ref{ArrowArray}() for _ in eachindex(v.data)]
    for i in eachindex(v.data)
        _fill_array!(child_refs[i], v.data[i], token, roots)
    end
    child_ptr_vec = [Base.unsafe_convert(Ptr{ArrowArray}, r) for r in child_refs]
    push!(roots, child_ptr_vec)
    push!(roots, child_refs)
    return child_refs, length(child_refs), Ptr{Ptr{ArrowArray}}(pointer(child_ptr_vec))
end

function _make_child_arrays!(
    v::Union{DenseUnion,SparseUnion},
    token::UInt64,
    roots::Vector{Any},
)
    child_refs = [Ref{ArrowArray}() for _ in eachindex(v.data)]
    for i in eachindex(v.data)
        _fill_array!(child_refs[i], v.data[i], token, roots)
    end
    child_ptr_vec = [Base.unsafe_convert(Ptr{ArrowArray}, r) for r in child_refs]
    push!(roots, child_ptr_vec)
    push!(roots, child_refs)
    return child_refs, length(child_refs), Ptr{Ptr{ArrowArray}}(pointer(child_ptr_vec))
end

function _make_child_arrays!(v::DictEncoded, token::UInt64, roots::Vector{Any})
    return Ref{ArrowArray}[], 0, Ptr{Ptr{ArrowArray}}(C_NULL)
end

function _make_dict_array!(v::ArrowVector, token::UInt64, roots::Vector{Any})
    return Ref{ArrowArray}(), Ptr{ArrowArray}(C_NULL)
end

function _make_dict_array!(v::DictEncoded, token::UInt64, roots::Vector{Any})
    dict_ref = Ref{ArrowArray}()
    _fill_array!(dict_ref, v.encoding.data, token, roots)
    push!(roots, dict_ref)
    return dict_ref, Base.unsafe_convert(Ptr{ArrowArray}, dict_ref)
end

"""
    Arrow.to_c_data(col::ArrowVector; name="") -> (Ref{ArrowSchema}, Ref{ArrowArray})

Export an Arrow array to the Arrow C Data Interface format. Returns a pair of
`Ref`s to `ArrowSchema` and `ArrowArray` structs. The consumer must call
the `release` callback in the `ArrowArray` when done, which frees the Julia
GC roots keeping the data alive.

!!! warning
    The returned `Ref` objects **must** be kept alive by the caller until the C
    consumer calls `release`. Do not let them be garbage collected prematurely.

# Example
```julia
col = Arrow.toarrowvector(Int32[1, 2, 3])
schema_ref, array_ref = Arrow.to_c_data(col)
schema_ptr = Base.unsafe_convert(Ptr{Arrow.ArrowSchema}, schema_ref)
array_ptr  = Base.unsafe_convert(Ptr{Arrow.ArrowArray},  array_ref)
# pass schema_ptr and array_ptr to C consumer
# C consumer calls array_ref[].release when done
```
"""
function to_c_data(col::ArrowVector; name::String="")
    token = _next_export_token()
    roots = Any[]

    schema_ref = Ref{ArrowSchema}()
    array_ref = Ref{ArrowArray}()

    _fill_schema!(schema_ref, col, name, token, roots)
    _fill_array!(array_ref, col, token, roots)

    # Keep the Ref objects themselves alive via roots
    push!(roots, schema_ref)
    push!(roots, array_ref)

    @lock _EXPORT_ROOTS_LOCK _EXPORT_ROOTS[token] = roots
    return schema_ref, array_ref
end

"""
    Arrow.to_c_data(tbl::Arrow.Table; names=Arrow.Table.names)
        -> (Vector{Ref{ArrowSchema}}, Vector{Ref{ArrowArray}})

Export all columns of an `Arrow.Table` to the Arrow C Data Interface format.
Returns two vectors of `Ref`s (one per column). Each column has its own
`release` token so they can be released independently.
"""
function to_c_data(tbl::Arrow.Table; names::Vector{String}=String.(Tables.columnnames(tbl)))
    schema_refs = Ref{ArrowSchema}[]
    array_refs = Ref{ArrowArray}[]
    for (i, col) in enumerate(Tables.columns(tbl))
        nm = i <= length(names) ? names[i] : ""
        s_ref, a_ref = to_c_data(col; name=nm)
        push!(schema_refs, s_ref)
        push!(array_refs, a_ref)
    end
    return schema_refs, array_refs
end
