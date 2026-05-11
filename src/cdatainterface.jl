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
mutable struct ArrowSchema
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
mutable struct ArrowArray
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
const CDATA_FLAG_NULLABLE         = Int64(2)
const CDATA_FLAG_DICT_ORDERED     = Int64(1)
const CDATA_FLAG_MAP_KEYS_SORTED  = Int64(4)

###############################################################################
# Import path (C → Julia)                                                     #
###############################################################################

"""
    CDataHandle

Holds C-side pointers for an imported Arrow C Data Interface pair and calls
the C `release` callbacks when the Julia wrapper is garbage collected.
"""
mutable struct CDataHandle
    schema_ptr::Ptr{ArrowSchema}
    array_ptr::Ptr{ArrowArray}
    released::Bool
end

CDataHandle(sp::Ptr{ArrowSchema}, ap::Ptr{ArrowArray}) = CDataHandle(sp, ap, false)

function _release_cdata_handle(h::CDataHandle)
    h.released && return
    h.released = true
    if h.array_ptr != C_NULL
        arr = unsafe_load(h.array_ptr)
        if arr.release != C_NULL
            ccall(arr.release, Cvoid, (Ptr{ArrowArray},), h.array_ptr)
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
"""
struct CImportedTable
    names::Vector{Symbol}
    columns::Vector{CImportedArray}
    lookup::Dict{Symbol,CImportedArray}
    metadata::Union{Nothing,Base.ImmutableDict{String,String}}
end

Tables.istable(::Type{<:CImportedTable}) = true
Tables.columnaccess(::Type{<:CImportedTable}) = true
Tables.columns(t::CImportedTable) = t
Tables.columnnames(t::CImportedTable) = t.names
Tables.getcolumn(t::CImportedTable, nm::Symbol) = t.lookup[nm]
Tables.getcolumn(t::CImportedTable, i::Int) = t.columns[i]
Tables.schema(t::CImportedTable) = Tables.Schema(t.names, map(eltype, t.columns))
DataAPI.metadatasupport(::Type{CImportedTable}) = (read=true, write=false)
DataAPI.metadata(t::CImportedTable, key::AbstractString; style::Bool=false) =
    style ? (get(t.metadata === nothing ? Dict() : t.metadata, key, nothing), :default) :
            get(t.metadata === nothing ? Dict() : t.metadata, key, nothing)
DataAPI.metadatakeys(t::CImportedTable) =
    t.metadata === nothing ? () : keys(t.metadata)

"""
    Arrow.release_c_data(x::CImportedArray)
    Arrow.release_c_data(t::CImportedTable)

Immediately release C-side resources held by an imported array or table.
After calling this, the data in `x` or `t` may become invalid.
"""
release_c_data(x::CImportedArray) = _release_cdata_handle(x.handle)
function release_c_data(t::CImportedTable)
    seen = Set{UInt}()
    for col in t.columns
        id = UInt(pointer_from_objref(col.handle))
        if id ∉ seen
            push!(seen, id)
            _release_cdata_handle(col.handle)
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
    for _ in 1:n_pairs
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
_cbuf(arr::ArrowArray, i::Int) = (arr.n_buffers > i && arr.buffers != C_NULL) ?
    unsafe_load(arr.buffers, i + 1) : C_NULL

# Load the i-th child array pointer from an ArrowArray (0-indexed)
_cchild_arr(arr::ArrowArray, i::Int) = unsafe_load(arr.children, i + 1)

# Load the i-th child schema pointer from an ArrowSchema (0-indexed)
_cchild_sch(sch::ArrowSchema, i::Int) = unsafe_load(sch.children, i + 1)

# Build a ValidityBitmap from C Data Interface buffer
function _import_validity(arr::ArrowArray, len::Int, off::Int)
    nc = Int(arr.null_count)
    vptr = Ptr{UInt8}(_cbuf(arr, 0))
    if nc == 0 || vptr == C_NULL
        return ValidityBitmap(UInt8[], 1, len, 0)
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
    for i in 0:len-1
        src_pos = off + i
        src_byte = src_pos >> 3
        src_bit  = src_pos & 7
        bit = (src[src_byte + 1] >> src_bit) & 1
        if bit == 0
            dst_byte = i >> 3
            dst_bit  = i & 7
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
    fmt == "c"   && return Int8
    fmt == "C"   && return UInt8
    fmt == "s"   && return Int16
    fmt == "S"   && return UInt16
    fmt == "i"   && return Int32
    fmt == "I"   && return UInt32
    fmt == "l"   && return Int64
    fmt == "L"   && return UInt64
    fmt == "e"   && return Float16
    fmt == "f"   && return Float32
    fmt == "g"   && return Float64
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
        p = parse(Int32, parts[1])
        s_val = parse(Int32, parts[2])
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
            return BoolVector{T}(UInt8[], 1, validity, len, meta)
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
        n_bytes = (len + off) * N
        data_bytes = dptr == C_NULL ? UInt8[] : unsafe_wrap(Array, dptr, n_bytes; own=false)
        # Apply offset: skip first `off*N` bytes
        data_view = off == 0 ? data_bytes : view(data_bytes, off*N+1:n_bytes)
        return FixedSizeList{T,typeof(data_view)}(UInt8[], validity, data_view, len, meta)
    end

    # String / binary (list with inline data)
    if fmt ∈ ("u", "U", "z", "Z")
        OT = (fmt == "U" || fmt == "Z") ? Int64 : Int32
        T_inner = (fmt == "u" || fmt == "U") ? String : Base.CodeUnits{UInt8,String}
        T = nullable ? Union{T_inner,Missing} : T_inner
        optr = Ptr{OT}(_cbuf(arr, 1))
        n_offs = len + off + 1
        offs_arr = optr == C_NULL ? OT[] : unsafe_wrap(Array, optr, n_offs; own=false)
        offs_view = off == 0 ? offs_arr : view(offs_arr, off+1:n_offs)
        offsets = Offsets(UInt8[], offs_view)
        dptr = Ptr{UInt8}(_cbuf(arr, 2))
        # data length = last offset value
        data_len = n_offs > 0 && optr != C_NULL ? Int(offs_arr[n_offs]) : 0
        data_bytes = dptr == C_NULL ? UInt8[] : unsafe_wrap(Array, dptr, data_len; own=false)
        return List{T,OT,Vector{UInt8}}(UInt8[], validity, offsets, data_bytes, len, meta)
    end

    # Generic list "+l" / "+L"
    if fmt == "+l" || fmt == "+L"
        OT = fmt == "+L" ? Int64 : Int32
        optr = Ptr{OT}(_cbuf(arr, 1))
        n_offs = len + off + 1
        offs_arr = optr == C_NULL ? OT[] : unsafe_wrap(Array, optr, n_offs; own=false)
        offs_view = off == 0 ? offs_arr : view(offs_arr, off+1:n_offs)
        offsets = Offsets(UInt8[], offs_view)
        child_arr_ptr = _cchild_arr(arr, 0)
        child_sch_ptr = _cchild_sch(sch, 0)
        A = _import_arrowvec(child_arr_ptr, child_sch_ptr, handle, convert)
        T_child = eltype(A)
        ST = SubArray{T_child,1,typeof(A),Tuple{UnitRange{Int64}},true}
        T = nullable ? Union{ST,Missing} : ST
        return List{T,OT,typeof(A)}(UInt8[], validity, offsets, A, len, meta)
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
        return FixedSizeList{T,typeof(A)}(UInt8[], validity, A, len, meta)
    end

    # Struct "+s"
    if fmt == "+s"
        vecs = AbstractVector[]
        child_names = Symbol[]
        child_types = Type[]
        for i in 0:Int(sch.n_children)-1
            child_av = _import_arrowvec(_cchild_arr(arr, i), _cchild_sch(sch, i), handle, convert)
            push!(vecs, child_av)
            child_sch_i = unsafe_load(_cchild_sch(sch, i))
            nm = child_sch_i.name != C_NULL ? Symbol(unsafe_string(child_sch_i.name)) : Symbol("f$i")
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
        n_offs = len + off + 1
        offs_arr = optr == C_NULL ? Int32[] : unsafe_wrap(Array, optr, n_offs; own=false)
        offs_view = off == 0 ? offs_arr : view(offs_arr, off+1:n_offs)
        offsets = Offsets(UInt8[], offs_view)
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
        typeids_parsed = isempty(typeids_str) ? nothing :
            Tuple(parse(Int32, s) for s in split(typeids_str, ','))
        tptr = Ptr{UInt8}(_cbuf(arr, 0))
        n = len + off
        typeids_vec = tptr == C_NULL ? UInt8[] : unsafe_wrap(Array, tptr, n; own=false)
        optr = Ptr{Int32}(_cbuf(arr, 1))
        offsets_vec = optr == C_NULL ? Int32[] : unsafe_wrap(Array, optr, n; own=false)
        vecs = AbstractVector[]
        child_types = DataType[]
        for i in 0:Int(sch.n_children)-1
            cv = _import_arrowvec(_cchild_arr(arr, i), _cchild_sch(sch, i), handle, convert)
            push!(vecs, cv)
            push!(child_types, eltype(cv))
        end
        data = Tuple(vecs)
        U_types = Tuple{child_types...}
        UT = UnionT{Meta.UnionMode.Dense,typeids_parsed,U_types}
        T = Union{child_types...}
        return DenseUnion{T,UT,typeof(data)}(UInt8[], UInt8[], typeids_vec, offsets_vec, data, meta)
    end

    # Sparse union "+us:typeIds"
    if startswith(fmt, "+us:")
        typeids_str = fmt[5:end]
        typeids_parsed = isempty(typeids_str) ? nothing :
            Tuple(parse(Int32, s) for s in split(typeids_str, ','))
        tptr = Ptr{UInt8}(_cbuf(arr, 0))
        n = len + off
        typeids_vec = tptr == C_NULL ? UInt8[] : unsafe_wrap(Array, tptr, n; own=false)
        vecs = AbstractVector[]
        child_types = DataType[]
        for i in 0:Int(sch.n_children)-1
            cv = _import_arrowvec(_cchild_arr(arr, i), _cchild_sch(sch, i), handle, convert)
            push!(vecs, cv)
            push!(child_types, eltype(cv))
        end
        data = Tuple(vecs)
        U_types = Tuple{child_types...}
        UT = UnionT{Meta.UnionMode.Sparse,typeids_parsed,U_types}
        T = Union{child_types...}
        return SparseUnion{T,UT,typeof(data)}(UInt8[], typeids_vec, data, meta)
    end

    # Dictionary encoded: schema.dictionary != C_NULL
    if sch.dictionary != C_NULL
        # schema.format is the INDEX type
        S = _fmt_to_storage_type(fmt)  # index type (e.g., Int8)
        nullable_idx = nullable  # indices may be nullable
        iptr = Ptr{S}(_cbuf(arr, 1))
        n_idx = len + off
        idx_arr = iptr == C_NULL ? S[] : unsafe_wrap(Array, iptr, n_idx; own=false)
        idx_view = off == 0 ? idx_arr : view(idx_arr, off+1:n_idx)
        idx_vec = Vector{S}(idx_view)  # make a copy since DictEncoded.indices is Vector{S}
        # Import dictionary values
        dict_arr_ptr = arr.dictionary
        dict_sch_ptr = sch.dictionary
        dict_vec = _import_arrowvec(dict_arr_ptr, dict_sch_ptr, handle, convert)
        T_val = eltype(dict_vec)
        ordered = (sch.flags & CDATA_FLAG_DICT_ORDERED) != 0
        encoding = DictEncoding{T_val,S,typeof(dict_vec)}(0, dict_vec, ordered, nothing)
        T = nullable ? Union{T_val,Missing} : T_val
        return DictEncoded{T,S,typeof(dict_vec)}(UInt8[], validity, idx_vec, encoding, meta)
    end

    # Primitive numeric / time types
    S = _fmt_to_storage_type(fmt)
    T = nullable ? Union{S,Missing} : S
    dptr = Ptr{S}(_cbuf(arr, 1))
    if dptr == C_NULL
        return Primitive(T, UInt8[], validity, S[], len, meta)
    end
    n = len + off
    data_arr = unsafe_wrap(Array, dptr, n; own=false)
    data_view = off == 0 ? data_arr : view(data_arr, off+1:n)
    return Primitive(T, UInt8[], validity, data_view, len, meta)
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
function from_c_data(
    schema_ptr::Ptr{Cvoid},
    array_ptr::Ptr{Cvoid};
    convert::Bool=true,
)
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
    cols = CImportedArray[]
    col_names = Symbol[]
    for (i, (sp_raw, ap_raw)) in enumerate(zip(schema_ptrs, array_ptrs))
        sp = Ptr{ArrowSchema}(sp_raw)
        ap = Ptr{ArrowArray}(ap_raw)
        handle = CDataHandle(sp, ap)
        vec = _import_arrowvec(ap, sp, handle, convert)
        T = eltype(vec)
        push!(cols, CImportedArray{T}(vec, handle))
        if names !== nothing
            push!(col_names, names[i])
        else
            sch = unsafe_load(sp)
            nm = sch.name != C_NULL ? unsafe_string(sch.name) : "col$i"
            push!(col_names, Symbol(nm))
        end
    end
    lookup = Dict{Symbol,CImportedArray}(col_names[i] => cols[i] for i in eachindex(cols))
    return CImportedTable(col_names, cols, lookup, metadata)
end

###############################################################################
# Export path (Julia → C)                                                     #
###############################################################################

# Global roots dict: keeps Julia objects alive while C holds pointers
const _EXPORT_ROOTS      = Dict{UInt64,Vector{Any}}()
const _EXPORT_ROOTS_LOCK = ReentrantLock()
const _EXPORT_TOKEN      = Threads.Atomic{UInt64}(0)

function _next_export_token()
    return Threads.atomic_add!(_EXPORT_TOKEN, UInt64(1))
end

# Release callbacks (function pointers set in __init__)
# Declared as globals here; assigned in Arrow.__init__
global _SCHEMA_RELEASE_CFUNC::Ptr{Cvoid} = C_NULL
global _ARRAY_RELEASE_CFUNC::Ptr{Cvoid}  = C_NULL

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
    S === Missing   && return "n"
    S === Bool      && return "b"
    S === Int8      && return "c"
    S === UInt8     && return "C"
    S === Int16     && return "s"
    S === UInt16    && return "S"
    S === Int32     && return "i"
    S === UInt32    && return "I"
    S === Int64     && return "l"
    S === UInt64    && return "L"
    S === Float16   && return "e"
    S === Float32   && return "f"
    S === Float64   && return "g"
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
        unit_char = U === Meta.TimeUnit.SECOND ? 's' :
                    U === Meta.TimeUnit.MILLISECOND ? 'm' :
                    U === Meta.TimeUnit.MICROSECOND ? 'u' : 'n'
        tz_str = TZ === nothing ? "" : String(TZ)
        return "ts$(unit_char):$(tz_str)"
    elseif S <: Duration
        U = S.parameters[1]
        unit_char = U === Meta.TimeUnit.SECOND ? 's' :
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

_container_to_format(v::Map)        = "+m"
_container_to_format(v::Struct)     = "+s"
_container_to_format(v::NullVector) = "n"

function _container_to_format(v::DenseUnion{T,UnionT{M,typeIds,U}}) where {T,M,typeIds,U}
    ids_str = typeIds === nothing ? join(0:fieldcount(U)-1, ',') : join(typeIds, ',')
    return "+ud:$(ids_str)"
end

function _container_to_format(v::SparseUnion{T,UnionT{M,typeIds,U}}) where {T,M,typeIds,U}
    ids_str = typeIds === nothing ? join(0:fieldcount(U)-1, ',') : join(typeIds, ',')
    return "+us:$(ids_str)"
end

function _container_to_format(v::DictEncoded{T,S,A}) where {T,S,A}
    # Format is the INDEX type
    S === Int8  && return "c"
    S === Int16 && return "s"
    S === Int32 && return "i"
    S === Int64 && return "l"
    return "i"  # fallback
end

_container_to_format(v::ArrowVector) = error("Cannot determine format string for $(typeof(v))")

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
    child_schema_refs, n_children, children_ptr =
        _make_child_schemas!(v, token, roots)

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

function _make_child_schemas!(v::Union{List,FixedSizeList,Map}, token::UInt64, roots::Vector{Any})
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
function _fill_array!(out::Ref{ArrowArray}, v::NullVector, token::UInt64, roots::Vector{Any})
    len = Int64(length(v))
    out[] = ArrowArray(
        len, len, Int64(0),
        Int64(0), Int64(0),
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
    nc  = Int64(nullcount(v))
    off = Int64(0)

    buffers, n_buffers = _make_buffers(v, roots)
    child_array_refs, n_children, children_arr_ptr = _make_child_arrays!(v, token, roots)
    dict_array_ref, dict_arr_ptr = _make_dict_array!(v, token, roots)

    out[] = ArrowArray(
        len, nc, off,
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
        offs = v.offsets.offsets isa Vector ? v.offsets.offsets : collect(v.offsets.offsets)
        push!(roots, offs)
        op = Ptr{Cvoid}(pointer(offs))
        bufs = [vp, op, dp]
        push!(roots, bufs)
        return Ptr{Ptr{Cvoid}}(pointer(bufs)), 3
    else
        offs = v.offsets.offsets isa Vector ? v.offsets.offsets : collect(v.offsets.offsets)
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
    array_ref  = Ref{ArrowArray}()

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
function to_c_data(tbl::Arrow.Table; names::Vector{String}=String.(Arrow.Table.names(tbl)))
    schema_refs = Ref{ArrowSchema}[]
    array_refs  = Ref{ArrowArray}[]
    for (i, col) in enumerate(Tables.columns(tbl))
        nm = i <= length(names) ? names[i] : ""
        s_ref, a_ref = to_c_data(col; name=nm)
        push!(schema_refs, s_ref)
        push!(array_refs, a_ref)
    end
    return schema_refs, array_refs
end
