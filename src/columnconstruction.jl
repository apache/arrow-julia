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

# =============================================================================
# Column construction: Julia columns -> Arrow Field + ArrayData.
#
# This file owns fresh inference, retained-schema reconstruction, recursive
# ArrowTypes lowering, dictionary pool policy, and field metadata. The write
# facade binds partitions and delegates each complete column through
# `_constructcolumn`.
# =============================================================================

"Copy a Field's metadata into stable, owned key-value pairs."
_fieldmetadata(f::AC.Field) =
    f.metadata === nothing ? nothing : collect(Pair{String,String}, f.metadata)

# The Arrow unit of a Timestamp/ZonedTimestamp element type. Only the four
# built-in resolutions have an Arrow timestamp unit; Durations 1.3+ permits
# package-defined periods, which have no Arrow representation.
function _instantunit(T::Type)
    P = _instantelementperiod(T)
    P === Dates.Second && return AC.SECOND
    P === Dates.Millisecond && return AC.MILLISECOND
    P === Dates.Microsecond && return AC.MICROSECOND
    P === Dates.Nanosecond && return AC.NANOSECOND
    throw(
        ArgumentError(
            "timestamp period $P has no Arrow timestamp unit; use Second, " *
            "Millisecond, Microsecond, or Nanosecond resolution",
        ),
    )
end
_instantelementperiod(::Type{Durations.Timestamp{P}}) where {P} = P
_instantelementperiod(::Type{<:Durations.ZonedTimestamp{P}}) where {P} = P

"One native (Field, ArrayData) column from a Julia vector, facade conversions included."
# The column-level dispatchers (`_constructnativepart`, `_constructpart`) do
# not specialize on the vector type: the builders they call do, so each new
# column type compiles its builder and not the whole dispatch. Each keyword
# method calls its body through `_uninferred`, so a method another package adds
# under a body makes only that body compile again, not every column type's
# keyword method.
Base.@nospecializeinfer _constructnativepart(
    name::String,
    @nospecialize(v::AbstractVector);
    context=nothing,
) = _uninferred(_nativepart, name, v, context)::Tuple{AC.Field,AC.ArrayData}

Base.@nospecializeinfer function _nativepart(
    name::String,
    @nospecialize(v::AbstractVector),
    context,
)
    T = Base.nonmissingtype(_eltypeof(v))
    # With TimeZones loaded, a fresh ZonedDateTime column writes as a
    # timezone-declared millisecond timestamp (ext/ArrowTimeZonesExt.jl)
    # instead of reflecting the zone's whole transition table as a struct.
    zoned = _uninferred(_zonednativepart, name, v, T)
    zoned === nothing || return zoned
    if _eltypeof(v) === Missing
        t = AC.NullType()
        return AC.Field(name, t; nullable=true),
        AC.ArrayData(t, length(v), AC.BufferSlice[]; nullcount=length(v))
    elseif T === Union{}
        throw(
            ArgumentError(
                "column $(_errname(context, name)) has no values to infer an Arrow " *
                "type from; give the empty column a concrete element type " *
                "(Int64[] or String[], say)",
            ),
        )
    elseif T <: Union{DataDecimals.Decimal,Durations.Duration}
        return _uninferred(_constructsharedpart, name, v, T)
    elseif T <: Durations.Timestamp
        isconcretetype(T) || throw(
            ArgumentError(
                "column $(_errname(context, name)) has abstract Timestamp element " *
                "type $T; give the column one concrete resolution",
            ),
        )
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.TimestampType(_instantunit(T), nothing),
            x -> Int64(Dates.value(x)),
        )
    elseif T <: Durations.ZonedTimestamp
        isconcretetype(T) || throw(
            ArgumentError(
                "column $(_errname(context, name)) has abstract ZonedTimestamp " *
                "element type $T; an Arrow column declares one time zone and one " *
                "resolution — " *
                "convert the values with Durations.astimezone first",
            ),
        )
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.TimestampType(_instantunit(T), Durations.zonename(T)::String),
            x -> Int64(Dates.value(x)),
        )
    elseif T <: Dates.Date
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.DateType(AC.DAY),
            x -> Int32(Dates.value(x) - _EPOCH_DAYS),
        )
    elseif T <: Dates.DateTime
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.TimestampType(AC.MILLISECOND, nothing),
            x -> Int64(Dates.value(x) - Dates.UNIXEPOCH),
        )
    elseif T <: Dates.Time
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.TimeType(AC.NANOSECOND, 64),
            x -> Int64(Dates.value(x)),
        )
    elseif T <: Dates.Period &&
           T <: Union{Dates.Second,Dates.Millisecond,Dates.Microsecond,Dates.Nanosecond}
        unit =
            T <: Dates.Second ? AC.SECOND :
            T <: Dates.Millisecond ? AC.MILLISECOND :
            T <: Dates.Microsecond ? AC.MICROSECOND : AC.NANOSECOND
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.DurationType(unit),
            x -> Int64(Dates.value(x)),
        )
    elseif T <: Union{Dates.Week,Dates.Day,Dates.Hour,Dates.Minute}
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.DurationType(AC.SECOND),
            _periodseconds,
        )
    elseif T <: Union{Dates.Month,Dates.Year}
        return _uninferred(
            _constructtemporalpart,
            name,
            v,
            AC.IntervalType(AC.YEAR_MONTH),
            _periodmonths,
        )
    elseif T <: NamedTuple
        _uninferred(any, ismissing, v)::Bool && throw(
            ArgumentError(
                "column $name has nullable NamedTuple rows that require the " *
                "recursive column adapter",
            ),
        )
        isconcretetype(T) || throw(
            ArgumentError(
                "column $(_errname(context, name)) has abstract NamedTuple element " *
                "type; give it a concrete NamedTuple type with declared field names " *
                "and types",
            ),
        )
        # Only the empty NamedTuple reaches here: `_nestedkind` takes every
        # other NamedTuple column.
        return _structpart(name, v, context)
    elseif T <: AbstractString
        _stringview(context) && return _uninferred(
            _retainedview,
            AC.Field(name, AC.ViewType(true); nullable=Missing <: _eltypeof(v)),
            v,
        )
        return _uninferred(_varbytespart, name, v, AC.Utf8Type(_largelists(context)))
    elseif T <: AbstractVector{UInt8}
        _stringview(context) && return _uninferred(
            _retainedview,
            AC.Field(name, AC.ViewType(false); nullable=Missing <: _eltypeof(v)),
            v,
        )
        return _uninferred(_varbytespart, name, v, AC.BinaryType(_largelists(context)))
    elseif T === Any || (T <: AbstractVector && eltype(T) === Any)
        # Materialized facade columns are Any-eltype for composite layouts;
        # one narrowing pass recovers list columns (inner vectors narrow
        # element-wise, empties adopt the joined element type).
        w = _uninferred(_narrowlists, v, _errname(context, name))
        NW = Base.nonmissingtype(_eltypeof(w))
        (NW === Any || (NW <: AbstractVector && eltype(NW) === Any)) && throw(
            ArgumentError(
                "column $(_errname(context, name)) has element type Any and cannot " *
                "be narrowed to a writable Arrow column; give it a concrete element type",
            ),
        )
        return _constructpart(name, w; context)
    elseif T <: AbstractVector
        context === nothing && (context = _WriterContext())
        return _uninferred(_listpart, name, v, context)
    elseif T === Bool
        return _uninferred(_boolpart, name, v)
    elseif T <: _FixedPrimitive
        t = AC.arrowtype_for(T)
        if _eltypeof(v) === T
            data = _uninferred(_fixedbuffer, v)
            data === nothing && return _uninferred(AC.fromjulia, name, _plainvector(v))
            return AC.Field(name, t; nullable=false),
            AC.ArrayData(t, length(v), [AC.BufferSlice(), data]; nullcount=0)
        end
        vals, validity, nc = _uninferred(_nullablevalues, T, v, zero(T))::Tuple
        return AC.Field(name, t; nullable=true),
        AC.ArrayData(t, length(v), [validity, AC._databuffer(vals)]; nullcount=nc)
    else
        # The vector's type is known at run time; see `_uninferred`.
        return _uninferred(AC.fromjulia, name, _plainvector(v))
    end
end

const _FixedPrimitive =
    Union{Int8,Int16,Int32,Int64,UInt8,UInt16,UInt32,UInt64,Float16,Float32,Float64}

"A Bool column: values and validity bit-packed in one pass."
function _boolpart(name::String, v::AbstractVector)
    n = length(v)
    i0 = firstindex(v) - 1
    values = Vector{UInt8}(undef, cld(n, 8))
    bits = Vector{UInt8}(undef, cld(n, 8))
    valid = 0
    @inbounds for b in eachindex(bits)
        base = (b - 1) << 3
        byte = 0x00
        valuebyte = 0x00
        for j = 0:(min(8, n - base) - 1)
            x = v[i0 + base + j + 1]
            present = x !== missing
            byte |= UInt8(present) << (j % UInt)
            valuebyte |= UInt8(present && x::Bool) << (j % UInt)
        end
        bits[b] = byte
        values[b] = valuebyte
        valid += count_ones(byte)
    end
    nc = n - valid
    t = AC.BoolType()
    validity = nc == 0 ? AC.BufferSlice() : AC._databuffer(bits)
    return AC.Field(name, t; nullable=Missing <: _eltypeof(v)),
    AC.ArrayData(t, n, [validity, AC._databuffer(values)]; nullcount=nc)
end

"""
The values of a non-null fixed-width column as one buffer: a `Vector`, or a
contiguous view of one (a record batch's rows), is borrowed without a copy;
nothing for any other vector or a nullable element type.
"""
function _fixedbuffer(v::AbstractVector{T}) where {T}
    isbitstype(T) || return nothing
    v isa Vector{T} && return AC._databuffer(v)
    v isa SubArray{T,1,Vector{T},<:Tuple{AbstractUnitRange{Int}},true} || return nothing
    offset = (first(parentindices(v)[1]) - 1) * sizeof(T)
    return AC.BufferSlice(AC.heapregion(parent(v)), offset, length(v) * sizeof(T))
end

"One (Field, ArrayData) column, including the ArrowTypes compatibility adapter."
Base.@nospecializeinfer _constructpart(
    name::String,
    @nospecialize(v::AbstractVector);
    context=nothing,
    narrowabstract::Bool=true,
) = _uninferred(_partof, name, v, context, narrowabstract)::Tuple{AC.Field,AC.ArrayData}

Base.@nospecializeinfer function _partof(
    name::String,
    @nospecialize(v::AbstractVector),
    context,
    narrowabstract::Bool,
)
    context === nothing && (context = _WriterContext())
    if _eltypeof(v) === Any
        narrowed = _uninferred(_narrowlists, v, _errname(context, name))
        _eltypeof(narrowed) === Any && throw(
            ArgumentError(
                "column $(_errname(context, name)) has element type Any and cannot " *
                "be narrowed to a writable Arrow column; give it a concrete element type",
            ),
        )
        return _constructpart(name, narrowed; context)
    end
    nested = _uninferred(_nestedpart, name, v, context)
    nested === nothing || return nested
    if narrowabstract
        narrowed = _uninferred(_narrowabstractwriterparts, name, v, context)
        if narrowed !== nothing
            # Julia can canonicalize a Union of concrete Tuple runtime types
            # back to the original abstract declaration, so eltype may be
            # unchanged. One narrowing pass is enough; recursing on the same
            # declaration would loop.
            _eltypeof(narrowed) === _eltypeof(v) ||
                return _constructpart(name, narrowed; context, narrowabstract=false)
        end
    end
    # The builders compile for the column's own type (see `_uninferred`).
    return _uninferred(_arrowtypes_needs, v, context)::Bool ?
           _uninferred(_constructarrowtypespart, name, v; context) :
           _constructnativepart(name, v; context)
end

# --- plain nested columns -------------------------------------------------------
#
# Lists (rows are AbstractVectors), fixed-size lists (NTuples of one element
# type), structs (NamedTuples), and maps (AbstractDicts or AbstractVectors of
# Pairs) build here in a few typed passes, at any depth and with missing at
# any level. Their children build as columns do, so leaves keep every
# mapping. A container with an ArrowTypes.jl mapping or label of its own takes
# the general route.

# How an error names a column: a nested child also names its top-level column.
_errname(context, name) =
    context === nothing || isempty(context.column) || context.column == name ? name :
    "$(context.column) (child $name)"

"The layout `T` writes as (`:list`, `:fixedlist`, `:struct`, `:map`), or nothing."
function _nestedkind(context, T::Type)
    Base.@nospecialize T
    T isa DataType && T <: Union{AbstractVector,AbstractDict,Tuple,NamedTuple} ||
        return nothing
    T <: AbstractVector{UInt8} && return nothing
    _writerstoragetype!(context, T) === T && _writerextension!(context, T) === nothing ||
        return nothing
    if T <: NamedTuple
        return fieldcount(T) > 0 ? :struct : nothing
    elseif T <: Tuple
        # NTuple{N,UInt8} is fixed_size_binary; tuples of mixed types are structs.
        Base.isvatuple(T) && return nothing
        types = fieldtypes(T)
        (isempty(types) || !allequal(types) || types[1] === UInt8) && return nothing
        return :fixedlist
    end
    T <: AbstractDict && return :map
    E = eltype(T)
    return E !== Union{} && E <: Pair ? :map : :list
end

"""
`v` as a plain nested column, or nothing when it is not one. Nothing too for
a struct or fixed-size list with missing rows whose non-nullable children
cannot hide placeholder slots (see `_hidevalidity`).
"""
function _nestedpart(name::String, @nospecialize(v::AbstractVector), context)
    kind = _nestedkind(context, Base.nonmissingtype(_eltypeof(v)))
    kind === nothing && return nothing
    # Dispatching on the kind compiles only the matching builder for `v`'s type.
    return _nestedbuild(Val(kind), name, v, context)
end
_nestedbuild(::Val{:list}, name, @nospecialize(v), context) = _listpart(name, v, context)
_nestedbuild(::Val{:map}, name, @nospecialize(v), context) = _mappart(name, v, context)
_nestedbuild(::Val{:struct}, name, @nospecialize(v), context) =
    _structpart(name, v, context)
_nestedbuild(::Val{:fixedlist}, name, @nospecialize(v), context) =
    _fixedlistpart(name, v, context)

"""
List offsets (row `i` holds values `offsets[i]+1:offsets[i+1]` of the
child), validity, and null count of `v`'s rows, in one pass.
"""
function _listoffsets(name::String, v::AbstractVector, ::Type{O}, pool=nothing) where {O}
    n = length(v)
    i0 = firstindex(v) - 1
    offsets = _takevector(pool, O, n + 1)
    offsets[1] = 0
    bits = _takevector(pool, UInt8, cld(n, 8))
    pos = 0
    valid = 0
    @inbounds for b in eachindex(bits)
        base = (b - 1) << 3
        byte = 0x00
        for j = 0:(min(8, n - base) - 1)
            i = base + j + 1
            x = v[i0 + i]
            present = x !== missing
            pos += present ? length(x) : 0
            pos <= typemax(O) || throw(_OffsetOverflow(name, 0, i))
            offsets[i + 1] = pos
            byte |= UInt8(present) << (j % UInt)
        end
        bits[b] = byte
        valid += count_ones(byte)
    end
    nc = n - valid
    validity =
        nc == 0 ? AC.BufferSlice() : AC.BufferSlice(AC.heapregion(bits), 0, length(bits))
    return offsets, validity, nc
end

# A list or map column read from Arrow keeps its offsets, rebased to start at
# zero: a null row may span child values, which stay in the child.
function _listoffsets(
    name::String,
    @nospecialize(v::Union{ListVector,MapVector}),
    ::Type{O},
    pool=nothing,
) where {O}
    n = length(v)
    offsets = _takevector(pool, O, n + 1)
    # A read column's offsets may view its batch (Int32 or Int64): kept alive
    # through `v`.
    GC.@preserve v _rebaseoffsets!(offsets, v.offsets, name)
    validity, nc = _storedvalidity(v.validity, v.bitoffset, n, pool)
    return offsets, validity, nc
end

function _rebaseoffsets!(offsets::AbstractVector{O}, raw, name::String) where {O}
    lo = raw[1]
    @inbounds for i in eachindex(offsets)
        x = raw[i] - lo
        x <= typemax(O) || throw(_OffsetOverflow(name, 0, i - 1))
        offsets[i] = x
    end
    return offsets
end

"""
The validity of `n` rows a read column stores as `bits` from `bitoffset` (no
bits: no nulls), as a write buffer, and the null count.
"""
function _storedvalidity(bits::Vector{UInt8}, bitoffset::Int, n::Int, pool=nothing)
    isempty(bits) && return AC.BufferSlice(), 0
    out = _takevector(pool, UInt8, cld(n, 8))
    valid = 0
    @inbounds for b in eachindex(out)
        byte = 0x00
        for j = 0:(min(8, n - ((b - 1) << 3)) - 1)
            byte |= UInt8(_bitset(bits, bitoffset, ((b - 1) << 3) + j + 1)) << j
        end
        out[b] = byte
        valid += count_ones(byte)
    end
    nc = n - valid
    return nc == 0 ? AC.BufferSlice() : AC.BufferSlice(AC.heapregion(out), 0, length(out)),
    nc
end

"""
Elements `lo+1:hi` of a read column's child values without copying them: a
view, or for a nested child the same storage over those rows.
"""
_childslice(v::AbstractVector, lo::Int, hi::Int) =
    lo == 0 && hi == length(v) ? v : view(v, (lo + 1):hi)
function _childslice(@nospecialize(v::StructVector), lo::Int, hi::Int)
    lo == 0 && hi == length(v) && return v
    return typeof(v)(
        v.children,
        v.offset + lo,
        hi - lo,
        v.validity,
        v.bitoffset + lo,
        v.owner,
    )
end
function _childslice(@nospecialize(v::ListVector), lo::Int, hi::Int)
    lo == 0 && hi == length(v) && return v
    return typeof(v)(
        GC.@preserve(v, v.offsets[(lo + 1):(hi + 1)]),
        v.values,
        v.validity,
        v.bitoffset + lo,
        v.owner,
    )
end
function _childslice(@nospecialize(v::MapVector), lo::Int, hi::Int)
    lo == 0 && hi == length(v) && return v
    offsets = GC.@preserve v v.offsets[(lo + 1):(hi + 1)]
    return typeof(v)(offsets, v.keys, v.values, v.validity, v.bitoffset + lo, v.owner)
end

# A row whose length differs from the first pass's would write past its slots.
_changedrows(name) = ArgumentError("column $name changed while it was being written")

# A list column read from Arrow already holds its rows' values in order, as a
# child of element type `E`; its values are that child, sliced.
_storedlist(@nospecialize(v), E) = v isa ListVector && _eltypeof(v.values) === E
function _storedlistvalues(name::String, @nospecialize(v::ListVector), offsets)
    lo, hi = GC.@preserve v Int(v.offsets[1]), Int(v.offsets[end])
    hi - lo == offsets[end] || throw(_changedrows(name))
    return _childslice(v.values, lo, hi)
end

"The values of `v`'s rows back to back, as `_listoffsets` counted them."
function _listvalues(name::String, ::Type{E}, v::AbstractVector, offsets) where {E}
    _storedlist(v, E) && return _storedlistvalues(name, v, offsets)
    flat = Vector{E}(undef, Int(offsets[end]))
    i0 = firstindex(v) - 1
    @inbounds for i = 1:(length(offsets) - 1)
        x = v[i0 + i]
        x === missing && continue
        lo = Int(offsets[i])
        m = Int(offsets[i + 1]) - lo
        length(x) == m || throw(_changedrows(name))
        copyto!(flat, lo + 1, x, firstindex(x), m)
    end
    return flat
end

# A nested builder's `spec` is a fresh column's name, or the retained Field it
# rebuilds, whose descriptor, child names, nullability, and metadata it keeps.
_specname(spec::String) = spec
_specname(f::AC.Field) = f.name
_childspec(::String, k::Int, name::String) = name
_childspec(f::AC.Field, k::Int, ::String) = f.children[k]

# The Field a nested builder returns over its built children.
_nestedfield(name::String, t, v, children) =
    AC.Field(name, t; nullable=Missing <: _eltypeof(v), children)
_nestedfield(f::AC.Field, t, v, children) = _retainedfield(f; children)

# Null rows under a retained field that cannot be null mean the column was
# replaced.
_checknulls(::String, nc) = nothing
_checknulls(f::AC.Field, nc) = nc == 0 || f.nullable || throw(_nonnullableerror(f.name))

# A retained column's rows are of its layout's kind `T`.
function _checkrows(spec, v, ::Type{T}, what::String) where {T}
    spec isa AC.Field || return nothing
    Base.nonmissingtype(_eltypeof(v)) <: T && return nothing
    for x in v
        x === missing ||
            x isa T ||
            throw(ArgumentError("column $(spec.name) retained rows must be $what"))
    end
    return nothing
end

function _listpart(spec, @nospecialize(v::AbstractVector), context)
    name = _errname(context, _specname(spec))
    t = spec isa AC.Field ? spec.type : AC.ListType(_largelists(context))
    _checkrows(spec, v, AbstractVector, "vectors")
    offsets, validity, nc = _listoffsets(name, v, t.large ? Int64 : Int32, _pool(context))
    _checknulls(spec, nc)
    E = eltype(Base.nonmissingtype(_eltypeof(v)))
    values =
        _storedlist(v, E) ? _storedlistvalues(name, v, offsets) :
        _listvalues(name, E, v, offsets)
    cf, cd = _constructpart(_childspec(spec, 1, "item"), values; context)
    # A list view's rows are consecutive spans of its child: starts and sizes.
    buffers =
        t isa AC.ListViewType ?
        [
            validity,
            AC._databuffer(offsets[1:(end - 1)]),
            AC._databuffer(diff(offsets)),
        ] : [validity, AC._databuffer(offsets)]
    d = AC.ArrayData(t, length(v), buffers; children=AC.ArrayData[cd], nullcount=nc)
    return _nestedfield(spec, t, v, AC.Field[cf]), d
end

"""
Child `K` of each row of `rows` (a struct field, a map entry's key or value)
as a lazy vector of `T`; a missing row reads as missing.
"""
struct _ChildValues{T,K,V<:AbstractVector} <: AbstractVector{T}
    rows::V
end
_ChildValues{T,K}(rows::V) where {T,K,V} = _ChildValues{T,K,V}(rows)
Base.size(c::_ChildValues) = (length(c.rows),)
Base.@propagate_inbounds function Base.getindex(c::_ChildValues{T,K}, i::Int) where {T,K}
    x = c.rows[firstindex(c.rows) + i - 1]
    return (x === missing ? missing : getfield(x, K))::T
end

# Element `j` of a fixed-size list column's child: value `(j-1) % N + 1` of
# row `(j-1) ÷ N + 1`.
struct _TupleValues{T,N,V<:AbstractVector} <: AbstractVector{T}
    rows::V
end
_TupleValues{T,N}(rows::V) where {T,N,V} = _TupleValues{T,N,V}(rows)
Base.size(c::_TupleValues{T,N}) where {T,N} = (N * length(c.rows),)
Base.@propagate_inbounds function Base.getindex(c::_TupleValues{T,N}, j::Int) where {T,N}
    r, k = divrem(j - 1, N)
    x = c.rows[firstindex(c.rows) + r]
    return (x === missing ? missing : x[k + 1])::T
end

"""
Whether a non-nullable child of type `T` can sit under missing parent rows:
it is built nullable, with missing in those slots, and `_hidevalidity` then
drops its validity, so the slots hold placeholders (zeros, empty strings,
empty lists). That needs a layout with a validity buffer that a nullable
build of `T` keeps: leaves without an ArrowTypes.jl mapping and the plain
nested kinds.
"""
_hideable(context, T::Type) =
    T !== Missing &&
    T !== Union{} &&
    !(T isa Union) &&
    (
        _nestedkind(context, T) !== nothing ||
        !_writerneedstype!(context, T) ||
        _hideablestorage(context, T)
    )

# A mapped type whose declared storage is one concrete type (not Null, not a
# storage union, not inferred from the values) builds with a validity buffer.
function _hideablestorage(context, T::Type)
    T === VersionNumber && return true
    S = _writerstoragetype!(context, T)
    S === T && return false
    S isa Union && return false
    S === Nothing && return false
    isconcretetype(S) || return false
    return _writerneedstype!(context, S) ? _hideablestorage(context, S) : true
end

"""
Data built for `_relaxed(f)` (or nullable, for a fresh child) as `f`
declares it: where `f` cannot be null, the validity that marked only hidden
slots is dropped, through a union's children and run-end values too. Hidden
slots are never read, so they keep whatever placeholder the build wrote.
"""
function _hidevalidity(f::AC.Field, built::AC.Field, d::AC.ArrayData)
    t = d.type
    fields = collect(AC.Field, built.children)
    datas = collect(AC.ArrayData, d.children)
    buffers = collect(AC.BufferSlice, d.buffers)
    nc = AC.nullcount(d)
    if t isa Union{AC.UnionType,AC.RunEndEncodedType}
        for i in (t isa AC.UnionType ? eachindex(datas) : 2:2)
            fields[i], datas[i] = _hidevalidity(f.children[i], fields[i], datas[i])
        end
    elseif !f.nullable && !(t isa AC.NullType)
        buffers[1] = AC.BufferSlice()
        nc = 0
    end
    field = AC.Field(
        built.name,
        built.type;
        nullable=f.nullable,
        metadata=_fieldmetadata(built),
        children=fields,
    )
    data = AC.ArrayData(
        t,
        d.len,
        buffers;
        offset=d.offset,
        children=datas,
        dictionary=d.dictionary,
        owner=d.owner,
        nullcount=nc,
    )
    return field, data
end

"""
`f` declared nullable, with a union's children and run-end values too: the
field a child builds under when null parent rows hide some of its slots.
"""
function _relaxed(f::AC.Field)
    t = f.type
    children = collect(AC.Field, f.children)
    if t isa Union{AC.UnionType,AC.RunEndEncodedType}
        for i in (t isa AC.UnionType ? eachindex(children) : 2:2)
            children[i] = _relaxed(children[i])
        end
    end
    return AC.Field(f.name, t; nullable=true, metadata=_fieldmetadata(f), children)
end

"""
Retained child `f` over `values`, `hidden` of which sit under null parent
rows and read as missing. A child that cannot be null is built nullable and
its validity then dropped (`_hidevalidity`); other missing values in it mean
the column was replaced.
"""
function _hiddenchild(f::AC.Field, values, hidden::Int, context)
    hidden == 0 && return _constructpart(f, values; context)
    hidden == length(values) && return _constructhiddenpart(f, hidden, context)
    f.nullable ||
        f.type isa AC.NullType ||
        count(ismissing, values) == hidden ||
        throw(_nonnullableerror(f.name))
    return _hidevalidity(f, _constructpart(_relaxed(f), values; context)...)
end

_nonnullableerror(name) = ArgumentError(
    "column $name holds missing values but its retained field is non-nullable",
)

"""
Child `spec` of nested rows `v`: `V{T}` over `v` gives its values, `hidden` of
which sit under null rows and read as missing. A retained child builds as
`_hiddenchild` says. A fresh child that cannot be null is built nullable and
its validity dropped when its layout keeps one (`_hideable`), else built under
the field an empty `T` column writes as, as a retained child is.
"""
function _rowchild(V::Type, v, spec, T::Type, hidden::Int, context)
    hidden == 0 && return _constructpart(spec, V{T}(v); context)
    spec isa AC.Field && return _hiddenchild(spec, V{Union{Missing,T}}(v), hidden, context)
    Missing <: T && return _constructpart(spec, V{T}(v); context)
    if _hideable(context, T)
        f, d = _constructpart(spec, V{Union{Missing,T}}(v); context)
        declared = AC.Field(
            f.name,
            f.type;
            nullable=false,
            metadata=_fieldmetadata(f),
            children=f.children,
        )
        return _hidevalidity(declared, f, d)
    end
    f, _ = _constructpart(spec, T[]; context)
    return _hiddenchild(f, V{Union{Missing,T}}(v), hidden, context)
end

function _structpart(spec, v::AbstractVector, context)
    R = Base.nonmissingtype(_eltypeof(v))
    present = Bool[x !== missing for x in v]
    nc = count(!, present)
    _checknulls(spec, nc)
    # Retained rows are typed NamedTuples with the field's names, or rows
    # materialized under labels (ordered `Pair` vectors).
    typed = R <: NamedTuple && isconcretetype(R) || !(spec isa AC.Field)
    if spec isa AC.Field
        typed ? _checkrownames(spec, R) : _validateretainedstructrows(spec, v)
    end
    fields = AC.Field[]
    children = AC.ArrayData[]
    for k = 1:(spec isa AC.Field ? length(spec.children) : fieldcount(R))
        f, d =
            typed ?
            _rowchild(
                _ChildValues{C,k} where {C},
                v,
                _childspec(spec, k, string(fieldname(R, k))),
                fieldtype(R, k),
                nc,
                context,
            ) : _hiddenchild(spec.children[k], _RowFields(v, k), nc, context)
        push!(fields, f)
        push!(children, d)
    end
    t = spec isa AC.Field ? spec.type : AC.StructType()
    d = AC.ArrayData(
        t,
        length(v),
        [nc == 0 ? AC.BufferSlice() : AC._bitmapbuffer(present)];
        children,
        nullcount=nc,
    )
    return _nestedfield(spec, t, v, fields), d
end

# A struct column read from Arrow writes its children as stored, slots under
# null rows included, so no child needs hiding.
function _structpart(spec, @nospecialize(v::StructVector), context)
    R = Base.nonmissingtype(_eltypeof(v))
    spec isa AC.Field && _checkrownames(spec, R)
    n = length(v)
    validity, nc = _storedvalidity(v.validity, v.bitoffset, n, _pool(context))
    _checknulls(spec, nc)
    fields = AC.Field[]
    children = AC.ArrayData[]
    for k = 1:fieldcount(R)
        child = _childslice(v.children[k], v.offset, v.offset + n)
        f, d = _constructpart(_childspec(spec, k, string(fieldname(R, k))), child; context)
        push!(fields, f)
        push!(children, d)
    end
    t = spec isa AC.Field ? spec.type : AC.StructType()
    d = AC.ArrayData(t, n, [validity]; children, nullcount=nc)
    return _nestedfield(spec, t, v, fields), d
end

# A typed struct row's field names must be the retained field's children's.
function _checkrownames(f::AC.Field, R::Type)
    names = fieldnames(R)
    length(names) == length(f.children) &&
        all(k -> string(names[k]) == f.children[k].name, eachindex(names)) ||
        throw(
            ArgumentError(
                "column $(f.name) retained struct row has fields $names; expected " *
                "$(Tuple(c.name for c in f.children))",
            ),
        )
    return nothing
end

"""
A fixed-size list column of `N` values per row: `NTuple`s or vectors. A
retained one's rows may be `Array`s of its tensor shape.
"""
function _fixedlistpart(spec, v::AbstractVector, context, N::Int=0)
    shape = spec isa AC.Field ? _tensorshape(spec) : nothing
    shape === nothing || (v = _tensorstorage(v, shape))
    R = Base.nonmissingtype(_eltypeof(v))
    t =
        spec isa AC.Field ? spec.type::AC.FixedSizeListType :
        AC.FixedSizeListType(N == 0 ? fieldcount(R) : N)
    N = t.listsize
    # (Not `R <: NTuple{N,Any}`: that type has N parameters.)
    if !(R isa DataType && R <: Tuple && !Base.isvatuple(R) && fieldcount(R) == N)
        _checkrows(spec, v, Union{AbstractVector,Tuple}, "vectors")
        for x in v
            x === missing ||
                length(x) == N ||
                throw(
                    ArgumentError(
                        "column $(_specname(spec)) fixed-size list row has " *
                        "$(length(x)) values; expected $N",
                    ),
                )
        end
    end
    present = Bool[x !== missing for x in v]
    nc = count(!, present)
    _checknulls(spec, nc)
    V = _TupleValues{C,N} where {C}
    cf, cd = _rowchild(V, v, _childspec(spec, 1, "item"), eltype(R), nc * N, context)
    d = AC.ArrayData(
        t,
        length(v),
        [nc == 0 ? AC.BufferSlice() : AC._bitmapbuffer(present)];
        children=AC.ArrayData[cd],
        nullcount=nc,
    )
    return _nestedfield(spec, t, v, AC.Field[cf]), d
end

_maptypes(::Type{T}) where {T<:AbstractDict} = (keytype(T), valtype(T))
_maptypes(::Type{T}) where {T<:AbstractVector} =
    eltype(T) isa DataType && eltype(T) <: Pair ? fieldtypes(eltype(T)) : (Any, Any)
_maptypes(::Type) = (Any, Any)

"The entries of `v`'s map rows back to back, as `_listoffsets` counted them."
function _mapentries(name::String, ::Type{P}, v::AbstractVector, offsets) where {P}
    R = Base.nonmissingtype(_eltypeof(v))
    R <: AbstractVector && eltype(R) === P && return _listvalues(name, P, v, offsets)
    entries = Vector{P}(undef, Int(offsets[end]))
    i0 = firstindex(v) - 1
    @inbounds for i = 1:(length(offsets) - 1)
        x = v[i0 + i]
        x === missing && continue
        pos = Int(offsets[i])
        length(x) == Int(offsets[i + 1]) - pos || throw(_changedrows(name))
        # A dictionary iterates its entries as key => value pairs too.
        for p in x
            p isa Pair || throw(ArgumentError("column $name map rows must hold Pairs"))
            entries[pos += 1] = P(first(p), last(p))
        end
    end
    return entries
end

# Child `k` (key or value) of a map's entries.
_entryspec(spec::String, k::Int, name::String) = name
_entryspec(f::AC.Field, k::Int, ::String) = only(f.children).children[k]

function _mappart(spec, v::AbstractVector, context)
    name = _errname(context, _specname(spec))
    _checkrows(spec, v, Union{AbstractVector,AbstractDict}, "Pair vectors or dictionaries")
    R = Base.nonmissingtype(_eltypeof(v))
    K, V = _maptypes(R)
    offsets, validity, nc = _listoffsets(name, v, Int32, _pool(context))
    _checknulls(spec, nc)
    entries = _mapentries(name, Pair{K,V}, v, offsets)
    KK = Base.nonmissingtype(K)
    Missing <: K &&
        any(p -> first(p) === missing, entries) &&
        throw(
            ArgumentError(
                "column $name has a missing map key; Arrow map keys cannot be null, " *
                "so drop those entries or replace the key",
            ),
        )
    kf, kd = _constructpart(_entryspec(spec, 1, "key"), _ChildValues{KK,1}(entries); context)
    vf, vd = _constructpart(_entryspec(spec, 2, "value"), _ChildValues{V,2}(entries); context)
    # Distinct keys that an ArrowTypes mapping stores alike would read back as
    # repeated keys of one row.
    (spec isa AC.Field || R <: AbstractDict && _writerneedstype!(context, KK)) &&
        _validatemapphysicalkeys(name, kf, kd, offsets)
    return _mapcolumn(spec, v, offsets, validity, nc, length(entries), kf, kd, vf, vd)
end

# A map column read from Arrow writes its offsets, keys, and values as stored.
function _mappart(spec, @nospecialize(v::MapVector), context)
    name = _errname(context, _specname(spec))
    offsets, validity, nc = _listoffsets(name, v, Int32, _pool(context))
    _checknulls(spec, nc)
    lo, hi = GC.@preserve v Int(v.offsets[1]), Int(v.offsets[end])
    kf, kd = _constructpart(_entryspec(spec, 1, "key"), _childslice(v.keys, lo, hi); context)
    vf, vd =
        _constructpart(_entryspec(spec, 2, "value"), _childslice(v.values, lo, hi); context)
    return _mapcolumn(spec, v, offsets, validity, nc, hi - lo, kf, kd, vf, vd)
end

function _mapcolumn(spec, @nospecialize(v), offsets, validity, nc, nentries, kf, kd, vf, vd)
    kf.nullable && throw(
        ArgumentError(
            "column $(_specname(spec)) has map keys that write as a nullable Arrow " *
            "type; Arrow map keys cannot be null",
        ),
    )
    t = spec isa AC.Field ? spec.type::AC.MapType : AC.MapType(false)
    t.keyssorted && _checksortedkeys(spec, kf, kd, offsets)
    entriestype = AC.StructType()
    children = AC.Field[kf, vf]
    ef =
        spec isa AC.Field ? _retainedfield(only(spec.children); children) :
        AC.Field("entries", entriestype; nullable=false, children)
    ed = AC.ArrayData(
        entriestype,
        nentries,
        [AC.BufferSlice()];
        children=AC.ArrayData[kd, vd],
        nullcount=0,
    )
    d = AC.ArrayData(
        t,
        length(v),
        [validity, AC._databuffer(offsets)];
        children=AC.ArrayData[ed],
        nullcount=nc,
    )
    return _nestedfield(spec, t, v, AC.Field[ef]), d
end

# A retained map that declares sorted keys gets rows sorted by stored key.
function _checksortedkeys(f::AC.Field, kf::AC.Field, kd::AC.ArrayData, offsets)
    for row = 1:(length(offsets) - 1)
        lo = Int(offsets[row]) + 1
        hi = Int(offsets[row + 1])
        lo >= hi && continue
        previous = AC.getvalue(kf, kd, Int64(lo))
        for index = (lo + 1):hi
            key = AC.getvalue(kf, kd, Int64(index))
            _writermapkeyisless(f, key, previous) && throw(
                ArgumentError(
                    "column $(f.name) row $row is not sorted by its physical key " *
                    "storage but its retained Map type declares sorted keys",
                ),
            )
            previous = key
        end
    end
    return nothing
end

"""
A retained Union column built from its values (public values, or lowered
storage values under a label), or nothing when some value fits none of the
union's children or several. A missing value goes to the first Null child,
else the first nullable one. Any other value goes to the child its runtime
type routes to (`_unionroute`); children of one layout cannot be told apart
and do not route. Each child then builds from its values as a composite's
child does, so values that lower to storage (dates, extension types) lower
there.
"""
function _constructfacadeunion(f::AC.Field, v::AbstractVector, context)
    t = f.type::AC.UnionType
    children = f.children
    n = length(v)
    length(t.typeids) == length(children) && n <= typemax(Int32) || return nothing
    # Lowered values of a labelled union: member i of the storage Union is
    # child i, as a fresh write orders the children (a nullable one's outer
    # Null child among them).
    S = Base.nonmissingtype(_eltypeof(v))
    members =
        !context.lowered || !(S isa Union) ? Type[] :
        Base.uniontypes(
            length(children) > length(Base.uniontypes(S)) ? Union{Missing,S} : S,
        )
    routes = IdDict{Type,Int}()
    route = Vector{Int}(undef, n)
    for (i, x) in enumerate(v)
        k = if x === missing
            something(
                findfirst(c -> c.type isa AC.NullType, children),
                findfirst(c -> c.nullable, children),
                0,
            )
        elseif x isa _ArrowTypesRoutedUnion
            # A value decoded from a built column knows its child.
            x.child
        else
            get!(routes, typeof(x)) do
                _unionroute(children, x, members, context)
            end
        end
        k == 0 && return nothing
        route[i] = k
    end
    sparse = t.mode == AC.SparseMode
    typeids = Vector{Int8}(undef, n)
    offsets = sparse ? nothing : Vector{Int32}(undef, n)
    childvalues = [Any[] for _ in children]
    for (i, x) in enumerate(v)
        k = route[i]
        typeids[i] = t.typeids[k]
        x isa _ArrowTypesRoutedUnion && (x = x.value)
        if sparse
            # A child's slots of other rows are hidden.
            for j in eachindex(children)
                push!(childvalues[j], j == k ? x : missing)
            end
        elseif x === missing && !isempty(childvalues[k]) && last(childvalues[k]) === missing
            # Missing rows in a row share one null slot.
            offsets[i] = length(childvalues[k]) - 1
        else
            offsets[i] = length(childvalues[k])
            push!(childvalues[k], x)
        end
    end
    hidden = sparse ? [count(!=(k), route) for k in eachindex(children)] : zeros(Int, length(children))
    fields = AC.Field[]
    datas = AC.ArrayData[]
    for (k, c) in enumerate(children)
        cf, cd = _hiddenchild(c, childvalues[k], hidden[k], context)
        push!(fields, cf)
        push!(datas, cd)
    end
    field = AC.Field(
        f.name,
        t;
        nullable=f.nullable,
        metadata=_fieldmetadata(f),
        children=fields,
    )
    buffers =
        sparse ? AC.BufferSlice[AC._databuffer(typeids)] :
        AC.BufferSlice[AC._databuffer(typeids), AC._databuffer(offsets)]
    return field, AC.ArrayData(t, n, buffers; children=datas, nullcount=0)
end

"""
The child of a union that takes value `x`: its storage member's child by
position when that child has the layout of a fresh column of `x`'s type,
labels included; else the one child with that layout; else, for a lowered
value, the one child whose storage domain holds it; else the one child of the
same layout family (`_samefamily`); else 0.
"""
function _unionroute(children, x, members, context)
    probe = first(_constructpart("probe", typeof(x)[x]; context))
    i = findfirst(M -> x isa M, members)
    i !== nothing && i <= length(children) && _unionfits(children[i], probe; exact=true) &&
        return i
    matches = findall(c -> _unionfits(c, probe; exact=true), children)
    # A lowered temporal value is its storage integer or DateTime.
    isempty(matches) &&
        context.lowered &&
        (matches = findall(c -> isempty(c.children) && x isa _arrowtypesprimitivebasetype(c), children))
    isempty(matches) && (matches = findall(c -> _unionfits(c, probe; exact=false), children))
    return length(matches) == 1 ? only(matches) : 0
end

# A child takes a value whose fresh field has the child's layout (`exact`), or
# the same layout at another width (a largelists or stringview source,
# pyarrow's large_string), or, when the child is a union itself, that one of
# its children takes. Exact matches are tried first: an int64 child and a
# timestamp child both store Int64.
_unionfits(c::AC.Field, probe::AC.Field; exact::Bool) =
    (exact ? _asretained(c, probe) !== nothing : _samefamily(c, probe)) ||
    c.type isa AC.UnionType && any(cc -> _unionfits(cc, probe; exact), c.children)

# Fields of one layout family, label for label and child for child: the same
# type, or utf8 or binary at any width (offsets or views), a list, list view,
# or fixed-size list at any width, a timestamp without a zone (or Date64) at
# any unit, a duration or time at any unit, a decimal of one precision and
# scale at any width.
function _samefamily(f::AC.Field, built::AC.Field)
    family = _layoutfamily(f.type)
    (AC.typeequal(f.type, built.type) || family !== nothing && family == _layoutfamily(built.type)) &&
    (f.nullable || !built.nullable) &&
    _extensionpairs(f) == _extensionpairs(built) &&
    length(f.children) == length(built.children) || return false
    return all(p -> _samefamily(p[1], p[2]), zip(f.children, built.children))
end
_layoutfamily(t::AC.ArrowType) =
    t isa AC.Utf8Type || (t isa AC.ViewType && t.utf8) ? :utf8 :
    t isa AC.BinaryType || t isa AC.ViewType ? :binary :
    t isa Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType} ? :list :
    (t isa AC.DateType && t.unit != AC.DAY) ||
    (t isa AC.TimestampType && t.timezone === nothing) ? :timestamp :
    t isa AC.DurationType ? :duration :
    t isa AC.TimeType ? :time :
    t isa AC.DecimalType ? (:decimal, t.precision, t.scale) : nothing

"""
A freshly built field as the retained field `f` declares it, or nothing when
their layouts differ. Names, nullability, and metadata come from `f`; a fresh
field may be non-nullable where `f` is nullable.
"""
function _asretained(f::AC.Field, built::AC.Field)
    AC.typeequal(f.type, built.type) &&
    (f.nullable || !built.nullable) &&
    _extensionpairs(f) == _extensionpairs(built) &&
    length(f.children) == length(built.children) || return nothing
    children = AC.Field[]
    for (fc, bc) in zip(f.children, built.children)
        c = _asretained(fc, bc)
        c === nothing && return nothing
        push!(children, c)
    end
    return AC.Field(
        f.name,
        f.type;
        nullable=f.nullable,
        metadata=_fieldmetadata(f),
        children,
    )
end

# A field's extension name and metadata: its logical type.
_extensionpairs(f::AC.Field) =
    f.metadata === nothing ? Pair{String,String}[] :
    Pair{String,String}[
        kv for kv in f.metadata if
        first(kv) == _EXTENSION_NAME_KEY || first(kv) == _EXTENSION_METADATA_KEY
    ]

"Walk a bounded writer value graph, with optional short-circuit inspection."
function _walkwritercontainer!(visit, x, active::Base.IdSet{Any}, depth::Int)
    x === missing && return false
    depth < _MAX_WRITER_SCHEMA_DEPTH || throw(
        ArgumentError(
            "ArrowTypes value nesting exceeds the supported depth " *
            "$_MAX_WRITER_SCHEMA_DEPTH",
        ),
    )
    visit(x) && return true
    if x isa NamedTuple || x isa Tuple
        for value in x
            _walkwritercontainer!(visit, value, active, depth + 1) === true && return true
        end
    elseif x isa Pair
        _walkwritercontainer!(visit, first(x), active, depth + 1) === true && return true
        _walkwritercontainer!(visit, last(x), active, depth + 1) === true && return true
    elseif x isa AbstractVector || x isa AbstractDict
        x in active &&
            throw(ArgumentError("recursive ArrowTypes value container cannot be written"))
        push!(active, x)
        try
            if x isa AbstractDict
                for (key, value) in x
                    _walkwritercontainer!(visit, key, active, depth + 1) === true &&
                        return true
                    _walkwritercontainer!(visit, value, active, depth + 1) === true &&
                        return true
                end
            else
                for value in x
                    _walkwritercontainer!(visit, value, active, depth + 1) === true &&
                        return true
                end
            end
        finally
            delete!(active, x)
        end
    end
    return false
end

"Reject recursive or excessively deep value-container graphs before narrowing."
function _preflightwritercontainers(v::AbstractVector)
    active = Base.IdSet{Any}()
    for value in v
        _walkwritercontainer!(_ -> false, value, active, 0)
    end
    return nothing
end

# An abstract element type no native builder writes as one column (`Real`,
# `Any`): such values are written as a dense Union of their runtime types.
_abstractnonnative(T) =
    T !== Union{} &&
    !isconcretetype(T) &&
    !(T isa Union) &&
    !(T <: Union{AbstractString,AbstractVector,NamedTuple}) &&
    !_singletypefamily(T)

# Families one Arrow column holds at a single concrete type (one resolution,
# zone, or precision): their abstract columns narrow only when every value
# has the same type, and refuse otherwise.
_singletypefamily(T) =
    T <: Union{Durations.Timestamp,Durations.ZonedTimestamp,DataDecimals.Decimal}

"The Union of the runtime types of column `name`'s values, `Missing` included."
function _observedtype(v::AbstractVector, name="")
    types = Type[]
    seen = Base.IdSet{Type}()
    hasmissing = false
    for x in v
        if x === missing
            hasmissing = true
            continue
        end
        _pushobserved!(types, seen, typeof(x), name)
    end
    U = _writeruniontype(types)
    return hasmissing ? Union{Missing,U} : U
end

"Narrow an Any-eltype column, recovering list-of-T structure when present."
function _narrowlists(v::AbstractVector, name="")
    # Narrowing can turn an Any column into a concrete list type before the
    # ArrowTypes value walk runs. Reject cycles and excessive nesting first,
    # without resolving any user traits.
    _preflightwritercontainers(v)
    # map(identity, …) re-infers a narrowed element type from the values:
    # inner maps narrow each row, the outer map narrows the column.
    w = map(x -> x isa AbstractVector ? map(identity, x) : x, v)
    w = map(identity, w)
    NT = Base.nonmissingtype(_eltypeof(w))
    _abstractnonnative(NT) && return _collectas(_observedtype(w, name), w)
    NT !== Union{} && NT <: AbstractVector || return w
    # Join the inner element types (empties narrow to Union{} and would
    # otherwise poison the join), then retype every inner vector. With only
    # empty rows, their declared element types are all there is.
    E = Union{}
    for x in w
        x === missing && continue
        isempty(x) && continue
        E = typejoin(E, eltype(x))
    end
    if E === Union{}
        for x in w
            x === missing || (E = typejoin(E, eltype(x)))
        end
    end
    E === Union{} && (E = Any)
    # Rows of different element types: the elements' runtime types.
    if _abstractnonnative(E)
        values = collect(Iterators.flatten(x for x in w if x !== missing))
        isempty(values) || (E = _observedtype(values, name))
    end
    E === Any && return w
    hasm = _eltypeof(w) >: Missing
    S = hasm ? Union{Missing,Vector{E}} : Vector{E}
    return S[x === missing ? missing : convert(Vector{E}, x) for x in w]
end

# A column read as a view over decoded Arrow buffers (one record batch's
# `ArrayData`) writes those buffers as they are when the writer would declare
# the same type for the column's element type: no value is converted.
function _constructpart(
    name::String,
    v::Union{Primitive,BoolVector};
    context=nothing,
    narrowabstract::Bool=true,
)
    d = v.owner
    # Fixed-size binary rows (`NTuple{N,UInt8}`) and 128-bit integers (stored
    # as fixed-size binary) have no native construction; the general path
    # writes them.
    if d isa AC.ArrayData &&
       d.offset == 0 &&
       d.len == length(v) &&
       !(Base.nonmissingtype(_eltypeof(v)) <: Union{Tuple,Int128,UInt128})
        f, _ = _constructnativepart(name, _eltypeof(v)[]; context)
        AC.typeequal(f.type, d.type) && return f, _viewbuffers(d)
    end
    return invoke(
        _constructpart,
        Tuple{String,AbstractVector},
        name,
        v;
        context,
        narrowabstract,
    )
end

# A fixed-width or Bool batch's buffers cut to its length, so no byte past the
# last value (padding of the source) is written.
function _viewbuffers(d::AC.ArrayData)
    n = d.len
    databytes = d.type isa AC.BoolType ? cld(n, 8) : n * AC.primwidth(d.type)
    data = AC.subslice(AC.rolebuffer(d, AC.DATA), 0, databytes)
    nc = AC.nullcount(d)
    validity = nc == 0 ? AC.BufferSlice() : AC.subslice(AC.validitybuffer(d), 0, cld(n, 8))
    return AC.ArrayData(d.type, n, [validity, data]; nullcount=nc)
end

const _PayloadColumn = Union{DataStrings.StringVector,DataStrings.BytesVector}
# A record batch's rows of a DataStrings column.
const _PayloadRows = SubArray{<:Any,1,<:_PayloadColumn,<:Tuple{UnitRange{Int}}}
_payloadparent(v::_PayloadColumn) = v
_payloadparent(v::_PayloadRows) = parent(v)
_payloadentries(v::_PayloadColumn) = v.payloads
_payloadentries(v::_PayloadRows) = view(parent(v).payloads, only(parentindices(v)))

# A DataStrings column IS Utf8View (BinaryView) memory: its payload vector is
# the views buffer and its byte buffers are the variadic data buffers. With
# `stringview=true` it is written as is, without a copy (a row range copies
# its view entries); otherwise its bytes are packed into utf8 (binary)
# offsets and data in one pass over the payloads. Nullability comes from the
# column's eltype. Both keywords are accepted for dispatch uniformity; no
# narrowing applies.
function _constructpart(
    name::String,
    v::Union{_PayloadColumn,_PayloadRows};
    context=nothing,
    narrowabstract::Bool=true,
)
    p = _payloadparent(v)
    utf8 = p isa DataStrings.StringVector
    nullable = Missing <: _eltypeof(v)
    if _stringview(context)
        # View entries are the payloads themselves, so a row range copies them.
        payloads = v isa _PayloadRows ? collect(_payloadentries(v)) : p.payloads
        # DataStrings keeps a private append arena, which may be empty.
        lastbuffer = something(findlast(!isempty, p.buffers), 0)
        f, d = AC.fromviewentries(name, payloads, p.buffers[1:lastbuffer]; nullable)
        utf8 && return f, d
        t = AC.ViewType(false)
        return AC.Field(name, t; nullable),
        AC.ArrayData(t, d.len, d.buffers; nullcount=AC.nullcount(d))
    end
    large = _largelists(context)
    t = utf8 ? AC.Utf8Type(large) : AC.BinaryType(large)
    return _packedpayloadpart(
        name,
        _payloadentries(v),
        p.buffers,
        t,
        large ? Int64 : Int32,
        nullable,
        _pool(context),
    )
end

"""
utf8/binary offsets and data from DataStrings payloads. One pass measures the
values and copies them while every value so far is inline; a batch with an
out-of-line value is a slice of that value's buffer when every value's bytes
sit there in row order (as a read of utf8 data stores them), else one copy.
"""
function _packedpayloadpart(
    name::String,
    payloads,
    buffers,
    t::AC.ArrowType,
    ::Type{Offset},
    nullable::Bool,
    pool=nothing,
) where {Offset}
    n = length(payloads)
    offsets = _takevector(pool, Offset, n + 1)
    offsets[1] = 0
    bits = _takevector(pool, UInt8, cld(n, 8))
    # Inline values are at most 12 bytes, so this holds every value until the
    # first out-of-line one; a batch that starts out of line goes without.
    firstlong =
        n > 0 && DataStrings.payloadlength(@inbounds payloads[1]) > DataStrings.INLINE_MAX
    inline = _takevector(pool, UInt8, firstlong ? 16 : 12n + 16)
    # From the first out-of-line value on, the bytes are a slice of its buffer
    # (`runbuf`, from `runstart`) when every out-of-line value sits there at
    # its row's position and every inline value matches the bytes there, as in
    # a column read from Arrow. This pass checks the values after the first
    # out-of-line one; the ones copied to `inline` before it are compared once
    # after.
    runbuf = 0
    runstart = 0
    runok = true
    prefix = 0
    run = Ptr{UInt8}(0)
    room = 0
    mismatch = UInt64(0)
    pos = 0
    valid = 0
    GC.@preserve payloads inline buffers begin
        words = Ptr{UInt64}(pointer(payloads))
        out = pointer(inline)
        @inbounds for b in eachindex(bits)
            base = (b - 1) << 3
            byte = 0x00
            for j = 0:(min(8, n - base) - 1)
                i = base + j + 1
                a = unsafe_load(words, 2i - 1)
                w = unsafe_load(words, 2i)
                len = Int(reinterpret(Int32, a % UInt32))
                present = len >= 0
                long = len > DataStrings.INLINE_MAX
                # The payload's last 12 bytes, an inline value's bytes, as words.
                lo = (a >> 32) | (w << 32)
                hi = (w >> 32) % UInt32
                if runbuf == 0
                    if long
                        runbuf = Int(reinterpret(Int32, w % UInt32)) + 1
                        runstart = Int(reinterpret(Int32, (w >> 32) % UInt32)) - pos
                        prefix = pos
                        if 1 <= runbuf <= length(buffers) && runstart >= 0
                            run = pointer(buffers[runbuf]) + runstart
                            room = length(buffers[runbuf]) - runstart
                        else
                            runok = false
                        end
                    else
                        unsafe_store!(Ptr{UInt64}(out + pos), lo)
                        unsafe_store!(Ptr{UInt32}(out + pos + 8), hi)
                    end
                else
                    k = Int(reinterpret(Int32, w % UInt32)) + 1
                    at = Int(reinterpret(Int32, (w >> 32) % UInt32)) - pos
                    # Without branches: the values are a random mix of kinds.
                    runok &= !long | ((k == runbuf) & (at == runstart))
                    if pos + 12 <= room
                        m = clamp(len, 0, DataStrings.INLINE_MAX) + 1
                        lomask = _INLINELOMASKS[m]
                        himask = _INLINEHIMASKS[m]
                        diff =
                            ((unsafe_load(Ptr{UInt64}(run + pos)) ⊻ lo) & lomask) |
                            ((unsafe_load(Ptr{UInt32}(run + pos + 8)) ⊻ hi) & himask)
                        mismatch |= ifelse(long, UInt64(0), diff)
                    elseif !long & (len > 0)
                        # Within 12 bytes of the buffer's end: byte by byte.
                        entry = Ptr{UInt8}(words) + 16 * (i - 1) + 4
                        runok &= pos + len <= room && _memequal(run + pos, entry, len)
                    end
                end
                pos += ifelse(present, len, 0)
                pos <= typemax(Offset) || throw(_OffsetOverflow(name, 0, i))
                offsets[i + 1] = pos % Offset
                byte |= UInt8(present) << (j % UInt)
            end
            bits[b] = byte
            valid += count_ones(byte)
        end
        runok &= (runbuf != 0) & (mismatch == 0) & (pos <= room)
        runok && (runok = _memequal(pointer(inline), run, prefix))
    end
    nc = n - valid
    validity =
        nc == 0 ? AC.BufferSlice() : AC.BufferSlice(AC.heapregion(bits), 0, length(bits))
    if runbuf == 0
        # The bytes are a slice: the vector keeps its length, which a reused
        # vector's pool reads as its capacity.
        databuf =
            pos == 0 ? AC.BufferSlice() : AC.BufferSlice(AC.heapregion(inline), 0, pos)
        d = AC.ArrayData(t, n, [validity, AC._databuffer(offsets), databuf]; nullcount=nc)
        return AC.Field(name, t; nullable), _constructedvalid(d)
    end
    if runok
        databuf = AC.BufferSlice(AC.heapregion(buffers[runbuf]), runstart, pos)
        d = AC.ArrayData(t, n, [validity, AC._databuffer(offsets), databuf]; nullcount=nc)
        return AC.Field(name, t; nullable), _constructedvalid(d)
    end
    data = _takevector(pool, UInt8, pos + 32)
    GC.@preserve payloads buffers data begin
        out = pointer(data)
        words = Ptr{UInt64}(pointer(payloads))
        @inbounds for i = 1:n
            a = unsafe_load(words, 2i - 1)
            w = unsafe_load(words, 2i)
            len = Int(reinterpret(Int32, a % UInt32))
            len <= 0 && continue
            if len <= DataStrings.INLINE_MAX
                # The inline bytes are the payload's last 12; `data` keeps 32
                # bytes of slack, so whole words are stored.
                unsafe_store!(Ptr{UInt64}(out), (a >> 32) | (w << 32))
                unsafe_store!(Ptr{UInt32}(out + 8), (w >> 32) % UInt32)
            else
                buf = buffers[reinterpret(Int32, w % UInt32) + 1]
                off = Int(reinterpret(Int32, (w >> 32) % UInt32))
                src = pointer(buf) + off
                if len <= 32 && off + 32 <= length(buf)
                    for k = 0:8:24
                        unsafe_store!(
                            Ptr{UInt64}(out + k),
                            unsafe_load(Ptr{UInt64}(src + k)),
                        )
                    end
                else
                    unsafe_copyto!(out, src, len)
                end
            end
            out += len
        end
    end
    databuf = pos == 0 ? AC.BufferSlice() : AC.BufferSlice(AC.heapregion(data), 0, pos)
    d = AC.ArrayData(t, n, [validity, AC._databuffer(offsets), databuf]; nullcount=nc)
    return AC.Field(name, t; nullable), _constructedvalid(d)
end

# The masks of an inline value's first `len` bytes (0 to 12) in the two words
# `_packedpayloadpart` compares it as, at index `len + 1`.
const _INLINELOMASKS = ntuple(k -> (UInt64(1) << (8 * min(k - 1, 8))) - UInt64(1), 13)
const _INLINEHIMASKS = ntuple(k -> (UInt32(1) << (8 * clamp(k - 9, 0, 4))) - UInt32(1), 13)

_memequal(p::Ptr{UInt8}, q::Ptr{UInt8}, n::Int) =
    ccall(:memcmp, Cint, (Ptr{UInt8}, Ptr{UInt8}, Csize_t), p, q, n) == 0

_stringview(context) = context !== nothing && context.stringview
_pool(context) = context === nothing ? nothing : context.pool
_largelists(context) = context !== nothing && context.largelists

"""
A 32-bit-offset column's data passes the Int32 range at `row` (0 when the row
is not known, as in nested data) of partition `part` (0 until the partition
loop records it). The writer cuts its record batches smaller and retries.
"""
struct _OffsetOverflow <: Exception
    column::String
    part::Int
    row::Int
end
Base.showerror(io::IO, e::_OffsetOverflow) =
    print(io, "column $(e.column) exceeds the Int32 offset range of one record batch")

"""
A string of `column` (a child as "a (child b.c)") is not valid UTF-8, at `row`
(0 when no row refers to it) of part `part` of the values being built. The
batch and partition loops report it as an `ArgumentError` naming the row of
the table being written.
"""
struct _InvalidUTF8 <: Exception
    column::String
    part::Int
    row::Int
end
Base.showerror(io::IO, e::_InvalidUTF8) = print(io, _invalidutf8message(e.column, e.row, 0))

function _invalidutf8message(column::String, row::Int, partition::Int)
    at = row == 0 ? "" : partition == 0 ? " row $row" : " row $row of partition $partition"
    return "column $column$at holds a string that is not valid UTF-8; Arrow strings " *
           "must be UTF-8 (Polars and DuckDB refuse files with others), so write " *
           "binary data as a Vector{UInt8} column"
end

"Throw `_InvalidUTF8` for the first string of a built column that is not UTF-8."
function _checkutf8(name::Symbol, f::AC.Field, d::AC.ArrayData)
    bad = _badutf8(f, d)
    bad === nothing && return nothing
    row, path = bad
    column = isempty(path) ? String(name) : "$name (child $path)"
    throw(_InvalidUTF8(column, 1, row))
end

"""
The first row of `d` holding a string, at any depth, that is not valid UTF-8,
and the path of child names to it, or nothing. Every non-null string slot is
checked, as Arrow's full validation does; row 0 means no row refers to the
slot (a dictionary value no row uses, say).
"""
function _badutf8(f::AC.Field, d::AC.ArrayData)
    t = d.type
    if t isa AC.Utf8Type
        i = _badutf8offsets(d, t.large ? Int64 : Int32)
        return i == 0 ? nothing : (i, "")
    elseif t isa AC.ViewType
        t.utf8 || return nothing
        i = _badutf8views(d)
        return i == 0 ? nothing : (i, "")
    elseif t isa AC.DictionaryType
        pool = d.dictionary
        pool === nothing && return nothing
        bad = _badutf8(AC.dictvaluefield(f, t), pool)
        bad === nothing && return nothing
        return (bad[1] == 0 ? 0 : _dictrow(d, t, bad[1]), bad[2])
    end
    for (j, (cf, cd)) in enumerate(zip(f.children, d.children))
        bad = _badutf8(cf, cd)
        bad === nothing && continue
        path = isempty(bad[2]) ? cf.name : cf.name * "." * bad[2]
        return (bad[1] == 0 ? 0 : _parentrow(d, j, bad[1]), path)
    end
    return nothing
end

function _isascii(bytes::Vector{UInt8})
    acc = UInt64(0)
    words = length(bytes) >> 3
    GC.@preserve bytes begin
        p = Ptr{UInt64}(pointer(bytes))
        for i = 1:words
            acc |= unsafe_load(p, i)
        end
    end
    for i = (8words + 1):length(bytes)
        acc |= @inbounds bytes[i]
    end
    return iszero(acc & 0x8080808080808080)
end

# A utf8 column's first non-null row that is not UTF-8, or 0. ASCII data, or
# data valid as a whole whose rows all start on a character, is valid; only
# other data is checked row by row. `T` is the offset type.
function _badutf8offsets(d::AC.ArrayData, ::Type{T}) where {T}
    n = Int(d.len)
    n == 0 && return 0
    offs = AC.rolebuffer(d, AC.OFFSETS)
    data = AC.rolebuffer(d, AC.DATA)
    (d.offset + n + 1) * sizeof(T) <= offs.len || return 0
    GC.@preserve offs data begin
        op = Ptr{T}(AC.sliceptr(offs)) + d.offset * sizeof(T)
        lo = Int(unsafe_load(op, 1))
        hi = Int(unsafe_load(op, n + 1))
        0 <= lo < hi <= data.len || return 0
        bytes = unsafe_wrap(Array, AC.sliceptr(data) + lo, hi - lo)
        _isascii(bytes) && return 0
        if isvalid(String, bytes)
            # A row starts on a character unless its first byte continues one;
            # a row at the end (empty) starts anywhere.
            starts = true
            m = length(bytes)
            for i = 2:n
                o = Int(unsafe_load(op, i)) - lo
                lead = @inbounds bytes[clamp(o + 1, 1, m)]
                starts &= (o >= m) | (lead & 0xc0 != 0x80)
            end
            starts && return 0
        end
        for i = 1:n
            AC.isvalid_at(d, i) || continue
            a = Int(unsafe_load(op, i)) - lo
            b = Int(unsafe_load(op, i + 1)) - lo
            isvalid(String, view(bytes, (a + 1):b)) || return i
        end
    end
    return 0
end

# A utf8_view column's first non-null row that is not UTF-8, or 0.
function _badutf8views(d::AC.ArrayData)
    n = Int(d.len)
    views = AC.rolebuffer(d, AC.VIEWS)
    16 * (d.offset + n) <= views.len || return 0
    GC.@preserve d begin
        entries = unsafe_wrap(Array, AC.sliceptr(views), views.len)
        buffers = Vector{UInt8}[
            unsafe_wrap(Array, AC.sliceptr(b), b.len) for b in @view(d.buffers[3:end])
        ]
        longascii = all(_isascii, buffers)
        for i = 1:n
            AC.isvalid_at(d, i) || continue
            base = 16 * (d.offset + i - 1)
            len = Int(unsafe_load(Ptr{Int32}(pointer(entries, base + 1))))
            bytes = if len <= AC.VIEW_INLINE_MAX
                view(entries, (base + 5):(base + 4 + len))
            else
                longascii && continue
                k = Int(unsafe_load(Ptr{Int32}(pointer(entries, base + 9))))
                at = Int(unsafe_load(Ptr{Int32}(pointer(entries, base + 13))))
                view(buffers[k + 1], (at + 1):(at + len))
            end
            isvalid(String, bytes) || return i
        end
    end
    return 0
end

# The first row of a dictionary column whose index refers to pool value `k`.
function _dictrow(d::AC.ArrayData, t::AC.DictionaryType, k::Int)
    w = AC.primwidth(t.indextype)
    indices = AC.rolebuffer(d, AC.DATA)
    for i = 1:Int(d.len)
        AC.isvalid_at(d, i) || continue
        AC._load_int(indices, t.indextype, AC._slotbyteoff(d, Int64(i), w)) + 1 == k &&
            return i
    end
    return 0
end

# The first row of `d` whose values include element `k` of its child `j`.
function _parentrow(d::AC.ArrayData, j::Int, k::Int)
    t = d.type
    for i = 1:Int(d.len)
        k in _childspan(t, d, j, Int64(i)) && return i
    end
    return 0
end

function _childspan(t::AC.ArrowType, d::AC.ArrayData, j::Int, i::Int64)
    if t isa Union{AC.ListType,AC.MapType}
        lo, hi = AC._offsets_at(d, i, t isa AC.ListType && t.large)
        return (lo + 1):hi
    elseif t isa AC.ListViewType
        off, sz = AC._listview_range(t, d, i)
        return (off + 1):(off + sz)
    elseif t isa AC.FixedSizeListType
        base = AC._slotindex0(d, i) * t.listsize
        return (base + 1):(base + t.listsize)
    elseif t isa AC.StructType
        return (d.offset + i):(d.offset + i)
    elseif t isa AC.UnionType
        tid = AC.loadat(AC.rolebuffer(d, AC.TYPE_IDS), Int8, AC._slotindex0(d, i))
        tid == t.typeids[j] || return 1:0
        t.mode == AC.SparseMode && return (d.offset + i):(d.offset + i)
        at =
            AC.loadat(AC.rolebuffer(d, AC.ELEMENT_OFFSETS), Int32, AC._slotbyteoff(d, i, 4))
        return (at + 1):(at + 1)
    elseif t isa AC.RunEndEncodedType
        j == 2 || return 1:0
        r = AC._ree_runindex(d, i)
        return r:r
    end
    return 1:0
end

# Duration seconds and year-month interval months of the larger Dates periods.
_periodseconds(x::Dates.Week) = Base.checked_mul(Dates.value(x), Int64(604_800))
_periodseconds(x::Dates.Day) = Base.checked_mul(Dates.value(x), Int64(86_400))
_periodseconds(x::Dates.Hour) = Base.checked_mul(Dates.value(x), Int64(3_600))
_periodseconds(x::Dates.Minute) = Base.checked_mul(Dates.value(x), Int64(60))
_periodmonths(x::Dates.Month) = Dates.value(x)
_periodmonths(x::Dates.Year) = Base.checked_mul(Dates.value(x), Int64(12))

_nbytes(x::AbstractString) = ncodeunits(x)
_nbytes(x::AbstractVector{UInt8}) = length(x)
function _copybytes!(
    data::Vector{UInt8},
    pos::Int,
    x::Union{String,SubString{String}},
    n::Int,
)
    GC.@preserve data x unsafe_copyto!(pointer(data, pos + 1), pointer(x), n)
    return nothing
end
# A DataStrings value's bytes are inline in its 16-byte payload (after the
# 4-byte length) or at `payloadpos` in its buffer.
_copybytes!(data::Vector{UInt8}, pos::Int, x::DataStrings.DataString, n::Int) =
    _copypayloadbytes!(data, pos, x, n)
_copybytes!(data::Vector{UInt8}, pos::Int, x::DataStrings.DataBytes, n::Int) =
    _copypayloadbytes!(data, pos, x, n)
function _copypayloadbytes!(data::Vector{UInt8}, pos::Int, x, n::Int)
    if n <= DataStrings.INLINE_MAX
        r = Ref(x.p)
        GC.@preserve r data begin
            src = Ptr{UInt8}(Base.unsafe_convert(Ptr{DataStrings.StringPayload}, r)) + 4
            unsafe_copyto!(pointer(data, pos + 1), src, n)
        end
    else
        GC.@preserve x data begin
            src = pointer(x.data, DataStrings.payloadpos(x.p))
            unsafe_copyto!(pointer(data, pos + 1), src, n)
        end
    end
    return nothing
end
_copybytes!(data::Vector{UInt8}, pos::Int, x::AbstractVector{UInt8}, n::Int) =
    (copyto!(data, pos + 1, x, firstindex(x), n); nothing)
function _copybytes!(data::Vector{UInt8}, pos::Int, x::AbstractString, n::Int)
    for (j, b) in enumerate(codeunits(x))
        @inbounds data[pos + j] = b
    end
    return nothing
end

"""
A utf8 or binary column (`t`). One pass measures the values (offsets,
validity, and the offset-range check), a second copies their bytes into a
data buffer of the exact size.
"""
function _varbytespart(name::String, v::AbstractVector, t::AC.ArrowType)
    Offset = t.large ? Int64 : Int32
    return _varbytespart(name, v, t, Offset)
end
function _varbytespart(name::String, v::AbstractVector, t, ::Type{Offset}) where {Offset}
    n = length(v)
    i0 = firstindex(v) - 1
    offsets = Vector{Offset}(undef, n + 1)
    offsets[1] = 0
    bits = zeros(UInt8, cld(n, 8))
    pos = 0
    nc = 0
    @inbounds for i = 1:n
        x = v[i0 + i]
        if x === missing
            nc += 1
        else
            m = _nbytes(x)::Int
            pos <= typemax(Offset) - m || throw(_OffsetOverflow(name, 0, i))
            pos += m
            bits[((i - 1) >> 3) + 1] |= UInt8(1) << ((i - 1) & 7)
        end
        offsets[i + 1] = pos
    end
    data = Vector{UInt8}(undef, pos)
    @inbounds for i = 1:n
        x = v[i0 + i]
        start = Int(offsets[i])
        m = Int(offsets[i + 1]) - start
        # Every data byte is written exactly once, even if a lazy column
        # yields different values on the second pass.
        (x === missing ? m == 0 : _nbytes(x) == m) ||
            throw(ArgumentError("column $name changed while it was being written"))
        x === missing || _copybytes!(data, start, x, m)
    end
    validity =
        nc == 0 ? AC.BufferSlice() : AC.BufferSlice(AC.heapregion(bits), 0, length(bits))
    databuf = pos == 0 ? AC.BufferSlice() : AC._databuffer(data)
    d = AC.ArrayData(t, n, [validity, AC._databuffer(offsets), databuf]; nullcount=nc)
    return AC.Field(name, t; nullable=Missing <: _eltypeof(v)), _constructedvalid(d)
end

# Offsets built here never decrease and end at the data length, and the null
# count is exact: the data passes Core's semantic checks by construction.
function _constructedvalid(d::AC.ArrayData)
    @atomic :monotonic d.semachecked = true
    return d
end

"Concrete Vector with an exact Union{Missing,T} or T eltype for fromjulia."
function _plainvector(v::AbstractVector)
    T = _eltypeof(v)
    return v isa Vector{T} ? v : _collectas(T, v)
end

# `eltype` of a vector the caller does not specialize on: Base's `eltype` of an
# abstract vector type has methods other packages add to (DataFrames does).
_eltypeof(::AbstractVector{T}) where {T} = T

# A string's UTF-8 bytes. `codeunits` of an `AbstractString` has methods other
# packages add to (LaTeXStrings does); the bytes of a `String` do not.
_utf8units(x::AbstractString) = codeunits((x isa String ? x : String(x))::String)

# `collect(T, v)` for an element type known only at run time. Other packages
# add `collect(::Type{...}, v)` methods (InlineStrings does), and writer code
# inferred against Base's one method would recompile once they load.
_collectas(::Type{T}, v::AbstractVector) where {T} = copyto!(Vector{T}(undef, length(v)), v)

_missings_to(::Type{S}, v) where {S} =
    _eltypeof(v) >: Missing ? Union{Missing,S}[x === missing ? missing : S(x) for x in v] :
    S[S(x) for x in v]

"Construct temporal ArrayData from nullable Int64 storage values."
function _temporaldata(t::AC.ArrowType, storage::AbstractVector)
    _, d0 = _uninferred(AC.fromjulia, "storage", storage)::Tuple{AC.Field,AC.ArrayData}
    # Narrow to Int32 for any 32-bit temporal descriptor (Date32 fresh,
    # Time32 retained); every other temporal layout keeps the inferred Int64
    # buffer.
    buffers = d0.buffers
    if AC.primwidth(t) == 4
        narrow = Vector{Int32}(undef, length(storage)::Int)
        for (i, x) in enumerate(storage)
            narrow[i] = x === missing ? Int32(0) : Int32(x)
        end
        buffers = AC.BufferSlice[d0.buffers[1], AC._databuffer(narrow)]
    end
    return AC._arraydata(
        t,
        d0.len,
        buffers,
        0,
        AC.ArrayData[],
        nothing,
        d0.owner,
        AC.nullcount(d0),
    )
end

"Construct one fresh temporal part and preserve its validity."
function _constructtemporalpart(
    name::String,
    v::AbstractVector,
    t::AC.ArrowType,
    tostorage::F,
) where {F}
    S = AC.primwidth(t) == 4 ? Int32 : Int64
    vals, validity, nc = _nullablevalues(S, v, zero(S), x -> S(tostorage(x)))
    d = AC.ArrayData(t, length(v), [validity, AC._databuffer(vals)]; nullcount=nc)
    return AC.Field(name, t; nullable=_eltypeof(v) >: Missing), d
end

# ArrowTypes write adapter. ArrowCore intentionally stays conversion-free;
# this layer lowers logical Julia values before it asks Core to build buffers.

"Stable metadata merge; `extra` wins except for protected extension labels."
function _mergemetapairs(base, extra; protectextension::Bool=false)
    out = Pair{String,String}[]
    base === nothing ||
        append!(out, String(first(kv))::String => String(last(kv))::String for kv in base)
    extra === nothing && return isempty(out) ? nothing : out
    for kv in extra
        key = String(first(kv))::String
        value = String(last(kv))::String
        i = findfirst(p -> first(p) == key, out)
        if protectextension &&
           (key == _EXTENSION_NAME_KEY || key == _EXTENSION_METADATA_KEY) &&
           i !== nothing
            continue
        end
        if i === nothing
            push!(out, key => value)
        else
            out[i] = key => value
        end
    end
    return isempty(out) ? nothing : out
end

"Attach this logical type's extension label without disturbing child labels."
function _arrowtypeslogicalfield(context, f::AC.Field, T; nullable::Bool=f.nullable)
    extension = _writerextension!(context, T)
    extension === nothing && return f
    metadata = _mergemetapairs(_fieldmetadata(f), extension)
    return AC.Field(
        f.name,
        f.type;
        nullable=nullable,
        metadata=metadata,
        children=collect(AC.Field, f.children),
    )
end

_arrowtypesnativetype(T) =
    T <: Union{
        Int8,
        Int16,
        Int32,
        Int64,
        UInt8,
        UInt16,
        UInt32,
        UInt64,
        Float16,
        Float32,
        Float64,
        Bool,
        AbstractString,
        Dates.Date,
        Dates.DateTime,
        Dates.Time,
        Dates.Period,
        DataDecimals.Decimal,
        Durations.Duration,
        Durations.Timestamp,
        Durations.ZonedTimestamp,
    } || _zonedwritertype(T)

# The TimeZones extension's write hooks. With the extension absent both
# answer "not mine", so every column keeps this file's ordinary routes.
function _zonedwritertype(T)
    ext = Base.get_extension(@__MODULE__, :ArrowTimeZonesExt)
    return ext !== nothing && ext.iszonedtype(T)
end

function _zonednativepart(name::String, v::AbstractVector, T)
    ext = Base.get_extension(@__MODULE__, :ArrowTimeZonesExt)
    ext === nothing && return nothing
    return ext.zonednativepart(name, v, T)
end

function _arrowtypesneedstype(T, arrowtype, hasarrowname, depth::Int=0)
    depth < _MAX_WRITER_SCHEMA_DEPTH || throw(
        ArgumentError(
            "ArrowTypes storage type exceeds the supported depth " *
            "$_MAX_WRITER_SCHEMA_DEPTH at $T",
        ),
    )
    T === Union{} && return false
    T === Missing && return false
    T === Any && return false
    if T isa Union
        members = Base.uniontypes(T)
        nonnull = count(!=(Missing), members)
        nullable_struct = Missing in members && any(member -> member <: NamedTuple, members)
        return nonnull > 1 ||
               nullable_struct ||
               any(
                   member ->
                       _arrowtypesneedstype(member, arrowtype, hasarrowname, depth + 1),
                   members,
               )
    end
    NT = Base.nonmissingtype(T)
    NT !== T && return _arrowtypesneedstype(NT, arrowtype, hasarrowname, depth + 1)
    arrowtype(T) !== T && return true
    hasarrowname(T) && return true
    T <: AbstractDict && return true
    T <: Tuple && return true
    if T <: NamedTuple
        return any(
            i -> _arrowtypesneedstype(fieldtype(T, i), arrowtype, hasarrowname, depth + 1),
            1:fieldcount(T),
        )
    end
    if T <: AbstractVector
        return _arrowtypesneedstype(eltype(T), arrowtype, hasarrowname, depth + 1)
    end
    _arrowtypesnativetype(T) && return false
    # Plain concrete structs are written by field reflection; native facade
    # values such as Dates must not be routed through ArrowTypes' default
    # StructKind.
    return isconcretetype(T) && !isprimitivetype(T)
end

_arrowtypesneedsvalue(x, context, depth::Int=0) =
    _walkwritercontainer!(
        value -> _writerneedstype!(context, typeof(value)),
        x,
        context.values,
        depth,
    ) === true

function _arrowtypes_needs(v::AbstractVector, context)
    T = _eltypeof(v)
    _writerneedstype!(context, T) && return true
    # Abstract/Any containers do not expose nested logical types. Inspect only
    # until the first ArrowTypes value is found; concrete columns, and Unions
    # of leaf types such as Union{Missing,Int64}, take no scan: the check
    # above already decided every value they can hold.
    return !isconcretetype(T) &&
           !_writerleafunion(T) &&
           any(value -> _arrowtypesneedsvalue(value, context), v)
end

# A Union of concrete types the value walk does not descend into.
_writerleafunion(T) =
    T isa Union && all(Base.uniontypes(T)) do S
        isconcretetype(S) &&
        !(S <: Union{Tuple,NamedTuple,Pair,AbstractVector,AbstractDict})
    end

_arrowtypesconcreteorunion(T) =
    isconcretetype(T) ||
    (T isa Union && all(_arrowtypesconcreteorunion, Base.uniontypes(T)))

function _arrowtypesstorageisspecified(context, T::Type, path::Vector{Type}=Type[])
    Base.@nospecialize T
    _arrowtypesconcreteorunion(T) && return true
    _arrowtypesnativetype(T) && return true
    length(path) < _MAX_WRITER_SCHEMA_DEPTH || throw(
        ArgumentError(
            "ArrowTypes storage schema exceeds the supported depth " *
            "$_MAX_WRITER_SCHEMA_DEPTH at $T",
        ),
    )
    T in path && throw(
        ArgumentError(
            "recursive ArrowTypes storage schema: " *
            _uninferred(join, (string(S) for S in (path..., T)), " -> ")::String,
        ),
    )
    push!(path, T)
    try
        T isa Union && return all(
            member -> _arrowtypesstorageisspecified(context, member, path),
            Base.uniontypes(T),
        )
        (T === Tuple || T === NamedTuple || (T <: Tuple && Base.isvatuple(T))) &&
            return false
        T <: Union{NamedTuple,Tuple} && return all(
            i -> _arrowtypesstorageisspecified(context, fieldtype(T, i), path),
            1:fieldcount(T),
        )
        T <: AbstractVector &&
            return _arrowtypesstorageisspecified(context, eltype(T), path)
        T <: AbstractDict &&
            return _arrowtypesstorageisspecified(context, keytype(T), path) &&
                   _arrowtypesstorageisspecified(context, valtype(T), path)
        storage = _writerstoragetype!(context, T)
        storage === T && return false
        return _arrowtypesstorageisspecified(context, storage, path)
    finally
        pop!(path)
    end
end

function _writerpromoteunion(T, S)
    promoted = promote_type(T, S)
    return _arrowtypesconcreteorunion(promoted) ? promoted : Union{T,S}
end

const _WRITER_CONVERSION_ERRORS = Union{MethodError,InexactError,OverflowError,TypeError}

# Base's `convert(Missing, x)` raises a plain error; report a conversion error.
_writerconvert(::Type{Missing}, value) =
    value === missing ? missing : throw(MethodError(convert, (Missing, value)))

function _writerconvert(::Type{T}, value) where {T}
    value isa T && return value
    if T isa Union
        for member in Base.uniontypes(T)
            try
                return _writerconvert(member, value)
            catch err
                err isa _WRITER_CONVERSION_ERRORS || rethrow()
            end
        end
    end
    return convert(T, value)
end

_writercolumnname(context, fallback::String) =
    isempty(context.column) ? fallback : context.column

function _writertoarrow(context, value, writertype::Type, fallback::String, field=nothing)
    value isa Union{UUIDs.UUID,Int128,UInt128} && return _facadetoarrow(value, field)
    try
        return ArrowTypes.toarrow(value)
    catch err
        err isa Union{InterruptException,OutOfMemoryError} && rethrow()
        err isa _WRITER_CONVERSION_ERRORS || rethrow()
        column = _writercolumnname(context, fallback)
        throw(
            ArgumentError(
                "ArrowTypes.toarrow for writer type $writertype failed for " *
                "column $column: $(_uninferred(sprint, showerror, err)::String)",
            ),
        )
    end
end

function _writerconvertlowered(
    ::Type{T},
    value,
    context,
    writertype::Type,
    fallback::String,
) where {T}
    try
        return _writerconvert(T, value)
    catch err
        err isa Union{InterruptException,OutOfMemoryError} && rethrow()
        err isa _WRITER_CONVERSION_ERRORS || rethrow()
        column = _writercolumnname(context, fallback)
        throw(
            ArgumentError(
                "ArrowTypes.toarrow for writer type $writertype produced storage " *
                "type $(typeof(value)), which cannot be represented as $T for " *
                "column $column",
            ),
        )
    end
end

function _writerconvertedcolumn(
    ::Type{T},
    values,
    context,
    writertype::Type,
    name::String,
) where {T}
    out = Vector{T}(undef, length(values))
    for i in eachindex(values)
        out[i] = _writerconvertlowered(T, values[i], context, writertype, name)
    end
    return out
end

# Function barrier for the known-storage lowering loop: the storage type is
# computed at runtime, so specializing on it here keeps the per-element
# toarrow/convert path monomorphic instead of boxing every lowered value.
function _writerloweredcolumn(
    ::Type{T},
    v::AbstractVector,
    context,
    writertype::Type,
    name::String,
    field=nothing,
) where {T}
    S = Base.nonmissingtype(T)
    # Primitive storage is read once by the builder, which lowers each value
    # as it reads it: no lowered copy of the column.
    S !== Union{} && S <: Union{_FixedPrimitive,Bool} && return _LoweredValues{T}(v) do x
        lowered = _writertoarrow(context, x, writertype, name, field)
        return _writerconvertlowered(S, lowered, context, writertype, name)
    end
    out = Vector{T}(undef, length(v))
    for (i, value) in enumerate(v)
        lowered = _writertoarrow(context, value, writertype, name, field)
        out[i] = _writerconvertlowered(T, lowered, context, writertype, name)
    end
    return out
end

function _arrowtypesmappedcolumn(name::String, v::AbstractVector, declared, context)
    S = _eltypeof(v)
    logical = Base.nonmissingtype(S)
    storage = Missing <: S ? Union{Missing,declared} : declared
    # Buffer builders index from 1; a non-one-based vector must go through
    # the copy path.
    S === storage && _arrowtypesconcreteorunion(S) && firstindex(v) == 1 && return v

    # An empty logical column has no runtime evidence. A recursively
    # materializable declared storage shape is still authoritative; unsized
    # Tuple and other inference-only declarations fall back to Arrow Null, as
    # ArrowTypes.ToArrow does.
    isempty(v) &&
        return _arrowtypesstorageisspecified(context, declared) ? storage[] : Missing[]

    if _arrowtypesconcreteorunion(storage) ||
       _arrowtypesstorageisspecified(context, declared)
        return _writerloweredcolumn(storage, v, context, logical, name)
    end

    lowered = Any[]
    observed = Type[]
    seen = Base.IdSet{Type}()
    sawmissing = false
    for value in v
        mapped = _writertoarrow(context, value, logical, name)
        push!(lowered, mapped)
        if value === missing
            sawmissing = true
            continue
        end
        _pushobserved!(observed, seen, typeof(mapped), name)
    end
    if isempty(observed)
        # A concrete logical declaration keeps ArrowTypes.ToArrow's
        # all-missing fallback type. Abstract inference-only storage has no
        # shape evidence, so the column is written as Arrow Null (what
        # ToArrow returns: Missing[]).
        fallbacktype =
            _arrowtypesconcreteorunion(S) ?
            _eltypeof(_uninferred(ArrowTypes.ToArrow, v)::AbstractVector) : Missing
        return _writerconvertedcolumn(fallbacktype, lowered, context, logical, name)
    end
    resulttype = reduce(_writerpromoteunion, observed)
    sawmissing && (resulttype = Union{Missing,resulttype})
    return _writerconvertedcolumn(resulttype, lowered, context, logical, name)
end

function _constructarrowtypespart(name::String, v::AbstractVector; context=nothing)
    logical = Base.nonmissingtype(_eltypeof(v))
    # `Base.nonmissingtype(Missing) === Union{}`. Handle a pure-null child
    # before trait dispatch: bottom is a subtype of every container type and
    # would otherwise enter an unrelated ArrowKind branch.
    _eltypeof(v) === Missing && return _constructnativepart(name, v; context)
    logical === Union{} && return _constructnativepart(name, v; context)
    context === nothing && (context = _WriterContext())
    return _withwriterschema(context, logical) do
        # `v`'s type is known at run time; see `_uninferred`.
        _uninferred(_constructarrowtypespart_impl, name, v, logical; context)
    end
end

function _constructarrowtypespart_impl(name::String, v::AbstractVector, logical; context)
    # VersionNumber's legacy reflected representation contains variable-length
    # tuples, so Arrow 3.0 writes String extension storage instead. ArrowTypes'
    # one-argument JuliaType registration and the VersionNumber(String)
    # constructor read it back. Arrow 2.x readers cannot: they index the
    # column with the Struct layout ArrowTypes declares for VersionNumber.
    if logical === VersionNumber
        mapped =
            _eltypeof(v) >: Missing ?
            Union{Missing,String}[x === missing ? missing : string(x) for x in v] :
            String[string(x) for x in v]
        f, d = _constructnativepart(name, mapped; context)
        return _arrowtypeslogicalfield(context, f, logical), d
    end
    declared = _writerstoragetype!(context, logical)
    if Missing <: _eltypeof(v) &&
       !(declared isa Union) &&
       _writerkind!(context, declared) isa ArrowTypes.NullKind
        throw(
            ArgumentError(
                "nullable logical column $name maps its non-missing values to Arrow " *
                "Null storage, so outer missing values cannot be distinguished; " *
                "use DictEncode to preserve the two states",
            ),
        )
    end
    _checkstoragemissing(logical, declared)
    if logical isa Union && declared === logical
        # A declared Julia Union owns one Arrow child per member, built
        # independently, except members that share one concrete storage type
        # and one label (or none): they write as one column of that storage.
        members = Base.uniontypes(logical)
        storages = Type[_writerstoragetype!(context, M) for M in members]
        shared =
            length(members) > 1 &&
            allequal(storages) &&
            isconcretetype(first(storages)) &&
            allequal(_writerextension!(context, M) for M in members)
        shared || (declared = logical)
        if !shared
            f, d = _arrowtypesunioncolumn(name, v; context)
            return _arrowtypeslogicalfield(context, f, logical), d
        end
        declared = first(storages)
    end
    # Abstract storage must be inferred from observed values. A recursively
    # materializable declaration remains schema authority for empty columns,
    # including every declared Union branch nested in a composite.
    mapped = _arrowtypesmappedcolumn(name, v, declared, context)
    f, d = _arrowtypesstoragecolumn(name, mapped, context)
    # A logical type stored as Arrow Null is not nullable unless the column
    # itself admits missing; otherwise the label's own values would read back
    # as missing.
    nullable = f.type isa AC.NullType && !(Missing <: _eltypeof(v)) ? false : f.nullable
    return _arrowtypeslogicalfield(context, f, logical; nullable=nullable), d
end

# Missing marks a null slot; as a value of a storage Union it could not be told
# from one.
function _checkstoragemissing(logical, declared)
    declared isa Union && Missing <: declared && throw(
        ArgumentError(
            "ArrowTypes.ArrowType($logical) is $declared, with Missing as a Union " *
            "member, which Arrow.jl does not write; declare the storage without " *
            "Missing and write the column with missing values",
        ),
    )
    return nothing
end

"A column of lowered storage values, built by the layout its storage type's kind names."
function _arrowtypesstoragecolumn(name::String, v::AbstractVector, context)
    _eltypeof(v) === Missing && return _constructnativepart(name, v; context)
    S = Base.nonmissingtype(_eltypeof(v))
    S === Union{} && return _constructnativepart(name, v; context)
    S isa Union && return _uninferred(_arrowtypesunioncolumn, name, v; context)
    _arrowtypesnativetype(S) && return _constructnativepart(name, v; context)
    kind = S <: NamedTuple ? ArrowTypes.StructKind() : _writerkind!(context, S)
    if kind isa ArrowTypes.NullKind
        nulls = S === Nothing ? fill(missing, length(v)) : v
        return _constructnativepart(name, nulls; context)
    elseif kind isa ArrowTypes.FixedSizeListKind
        N = ArrowTypes.getsize(kind)
        ArrowTypes.gettype(kind) === UInt8 && return _fixedbinarypart(name, v, N)
        return _uninferred(_fixedlistpart, name, v, context, N)
    elseif kind isa ArrowTypes.MapKind
        S <: AbstractDict || throw(
            ArgumentError("ArrowTypes MapKind column $name must lower to AbstractDict rows"),
        )
        return _uninferred(_mappart, name, v, context)
    elseif kind isa ArrowTypes.ListKind
        S <: Union{AbstractString,AbstractVector{UInt8}} &&
            return _constructnativepart(name, v; context)
        S <: AbstractVector || throw(
            ArgumentError(
                "column $(_errname(context, name)) holds $S values, which are not " *
                "vectors; an Arrow list holds a vector per row (vec(x) flattens an array)",
            ),
        )
        return _uninferred(_listpart, name, v, context)
    elseif kind isa ArrowTypes.StructKind
        _arrowtypesstorageisspecified(context, S) || throw(
            ArgumentError(
                "ArrowTypes StructKind column $name lowered to an underspecified type " *
                "$S; give every struct field a writable storage type",
            ),
        )
        return _uninferred(_structpart, name, v, context)
    elseif kind isa Union{ArrowTypes.PrimitiveKind,ArrowTypes.BoolKind}
        return _constructnativepart(name, v; context)
    end
    throw(
        ArgumentError(
            "unsupported ArrowTypes storage kind $(typeof(kind)) for column $name",
        ),
    )
end

# Arrow Union child limit; shares the ArrowTypes storage-Union limit.
const _MAX_WRITER_UNION_BRANCHES = _MAX_ARROWTYPE_UNION_BRANCHES
const _MAX_INFERRED_WRITER_TYPES = 8

_writertypevariants(T::Type) =
    T === Union{} ? Type[] : T isa Union ? Type[Base.uniontypes(T)...] : Type[T]

function _checkedwritervariants(owner::String, T::Type; allowempty::Bool=false)
    variants = _writertypevariants(T)
    length(variants) <= _MAX_WRITER_UNION_BRANCHES || throw(
        ArgumentError("$owner has more than $_MAX_WRITER_UNION_BRANCHES Union branches"),
    )
    allowempty || !isempty(variants) || throw(ArgumentError("$owner has no Union branches"))
    return variants
end

function _writeruniontype(types)
    uniontype = Union{}
    for T in types
        uniontype = Union{uniontype,T}
    end
    return uniontype
end

"Construct a fresh dense Arrow Union from one concrete Julia Union element type."
function _arrowtypesunioncolumn(name::String, v::AbstractVector; context)
    variants = _checkedwritervariants("Arrow Union column $name", _eltypeof(v))

    childvalues = Any[T[] for T in variants]
    typeids = Vector{Int8}(undef, length(v))
    offsets = Vector{Int32}(undef, length(v))
    for (i, x) in enumerate(v)
        pos = findfirst(T -> x isa T, variants)
        pos === nothing && throw(
            ArgumentError(
                "Arrow Union column $name contains $(typeof(x)), which is not in " *
                "its declared element type $(_eltypeof(v))",
            ),
        )
        child = childvalues[pos]
        length(child) <= typemax(Int32) ||
            throw(ArgumentError("Arrow Union column $name exceeds the Int32 offset range"))
        typeids[i] = Int8(pos - 1)
        offsets[i] = Int32(length(child))
        push!(child, x)
    end

    childfields = AC.Field[]
    childdata = AC.ArrayData[]
    for i in eachindex(variants)
        # An abstract string member writes its values as Strings.
        values = variants[i] === AbstractString ? _missings_to(String, childvalues[i]) : childvalues[i]
        field, data = _constructpart(string(i - 1), values; context)
        push!(childfields, field)
        push!(childdata, data)
    end
    ids = Int8.(0:(length(variants) - 1))
    t = AC.UnionType(AC.DenseMode, ids)
    field = AC.Field(name, t; nullable=Missing in variants, children=childfields)
    data = AC.ArrayData(
        t,
        length(v),
        [AC._databuffer(typeids), AC._databuffer(offsets)];
        children=childdata,
        nullcount=0,
    )
    return field, data
end

"""
Field `k` of each row of a retained struct column: a typed read's row is a
`NamedTuple`, a materialized one an ordered `Pair` vector; a missing row reads
as missing.
"""
struct _RowFields{V<:AbstractVector} <: AbstractVector{Any}
    rows::V
    k::Int
end
Base.size(c::_RowFields) = (length(c.rows),)
function Base.getindex(c::_RowFields, i::Int)
    x = c.rows[firstindex(c.rows) + i - 1]
    x === missing && return missing
    # Inferring a NamedTuple field's exact type makes Julia 1.10 with code
    # coverage fail ("fatal error in type inference (type bound)").
    x isa NamedTuple && return _uninferred(getfield, x, c.k)
    return last(x[firstindex(x) + c.k - 1])
end

mutable struct _WriterContext
    arrowtypes::_ArrowTypesContext
    needs::IdDict{Type,Bool}
    kinds::IdDict{Type,Any}
    extensions::IdDict{Type,Any}
    schema::Vector{Type}
    values::Base.IdSet{Any}
    # True while the values being rebuilt are already in their storage domain
    # (see `_constructlowered`); false for facade-materialized public rows.
    lowered::Bool
    column::String
    # Fresh strings and bytes write as utf8/binary views instead of offsets.
    stringview::Bool
    # Fresh strings, bytes, and lists declare 64-bit offsets.
    largelists::Bool
    # Where a writer's rebuilt buffers come from (`_BufferPool`), or nothing.
    pool::Any
end

function _WriterContext(
    column::AbstractString="";
    stringview::Bool=false,
    largelists::Bool=false,
    pool=nothing,
)
    arrowtypes = _ArrowTypesContext(; warn=false)
    return _WriterContext(
        arrowtypes,
        IdDict{Type,Bool}(),
        IdDict{Type,Any}(),
        IdDict{Type,Any}(),
        Type[],
        Base.IdSet{Any}(),
        false,
        String(column),
        stringview,
        largelists,
        pool,
    )
end

const _MAX_WRITER_SCHEMA_DEPTH = 64

function _withwriterschema(f, context::_WriterContext, T::Type)
    Base.@nospecialize T
    if T in context.schema
        path =
            _uninferred(join, (string(S) for S in (context.schema..., T)), " -> ")::String
        throw(ArgumentError("recursive ArrowTypes storage schema: $path"))
    end
    length(context.schema) < _MAX_WRITER_SCHEMA_DEPTH || throw(
        ArgumentError(
            "ArrowTypes storage schema exceeds the supported depth " *
            "$_MAX_WRITER_SCHEMA_DEPTH at $T",
        ),
    )
    push!(context.schema, T)
    try
        return f()
    finally
        pop!(context.schema)
    end
end

function _writerstoragetype!(context::_WriterContext, T::Type)
    Base.@nospecialize T
    return _arrowtypesstoragetype!(context.arrowtypes, T)
end

function _writerneedstype!(context::_WriterContext, T::Type)
    Base.@nospecialize T
    haskey(context.needs, T) && return context.needs[T]
    return context.needs[T] = _arrowtypesneedstype(
        T,
        nested -> _writerstoragetype!(context, nested),
        nested -> _writerextension!(context, nested) !== nothing,
        0,
    )
end

function _writerkind!(context::_WriterContext, T::Type)
    Base.@nospecialize T
    haskey(context.kinds, T) && return context.kinds[T]
    return context.kinds[T] = ArrowTypes.ArrowKind(T)
end

function _writerextension!(context::_WriterContext, T::Type)
    Base.@nospecialize T
    haskey(context.extensions, T) && return context.extensions[T]
    return context.extensions[T] = _writerextensionlabel(T)
end

function _writerextensionlabel(T::Type)
    Base.@nospecialize T
    # A Union has the label its members share, if they all have one: a label
    # that only matches the Union as a whole (tuple types' JuliaLang.Tuple)
    # belongs to none of the children it writes as.
    U = Base.nonmissingtype(T)
    if U isa Union
        labels = Any[_writerextensionlabel(M) for M in Base.uniontypes(U)]
        return allequal(labels) ? first(labels) : nothing
    end
    name = _facadearrowname(T)
    name === nothing || return Pair{String,String}[
        _EXTENSION_NAME_KEY => name,
        _EXTENSION_METADATA_KEY => "",
    ]
    ArrowTypes.hasarrowname(T) || return nothing
    # Asserted: the label methods are inferred for any `T`, and a `String` of an
    # unknown value would convert through methods other packages add.
    return Pair{String,String}[
        _EXTENSION_NAME_KEY => String(ArrowTypes.arrowname(T))::String,
        _EXTENSION_METADATA_KEY => String(ArrowTypes.arrowmetadata(T))::String,
    ]
end

function _checkabstractlabel(f::AC.Field, target, T::Type, context::_WriterContext)
    isconcretetype(target) && return nothing
    label = _writerextension!(context, T)
    label === nothing && return nothing
    retained = _arrowtypesextension(f)
    (last(label[1]), last(label[2])) == retained || throw(
        ArgumentError(
            "registered writer subtype $T declares extension $(repr(label)), which " *
            "cannot replace abstract retained target $target with extension " *
            "$(repr(retained))",
        ),
    )
    return nothing
end

"""
Narrow one unresolved abstract declaration from whole-column runtime evidence.
Returns nothing when the declaration cannot be narrowed from runtime evidence.
"""
function _narrowabstractwriterparts(name::String, v::AbstractVector, context::_WriterContext)
    logical = Base.nonmissingtype(_eltypeof(v))
    logical in (Union{}, Any) && return nothing
    (isconcretetype(logical) || logical isa Union) && return nothing
    _writerstoragetype!(context, logical) === logical || return nothing
    _writerextension!(context, logical) === nothing || return nothing
    # Abstract row types of a plain nested layout (NTuples of nullable values,
    # `AbstractVector{Int}`) write as that layout without runtime narrowing.
    _nestedkind(context, logical) === nothing || return nothing

    observed = Type[]
    seen = Base.IdSet{Type}()
    nullable = Missing <: _eltypeof(v)
    for value in v
        value === missing && continue
        value isa logical || throw(
            ArgumentError(
                "column $name contains $(typeof(value)), which is outside its " *
                "declared abstract element type $logical",
            ),
        )
        _pushobserved!(observed, seen, typeof(value), name)
    end
    isempty(observed) && return nothing
    needsruntime = any(
        T -> _writerneedstype!(context, T) || _writerextension!(context, T) !== nothing,
        observed,
    )
    if !needsruntime && _singletypefamily(logical)
        length(observed) == 1 || return nothing
        storagetype = nullable ? Union{Missing,only(observed)} : only(observed)
        return _collectas(storagetype, v)
    end
    if !needsruntime && _abstractnonnative(logical)
        # `Real[1, 2.5]`: one runtime type writes as that type, several as a
        # dense Union of them.
        storagetype = _writeruniontype(observed)
        nullable && (storagetype = Union{Missing,storagetype})
        return _collectas(storagetype, v)
    end
    if !needsruntime
        # Rows of an abstract NamedTuple column (some fields missing in some
        # rows, say) share one concrete row type once widened field by field.
        logical <: NamedTuple || return nothing
        rowtype = _widenednamedtuple(observed)
        rowtype === nothing && return nothing
        nullable && (rowtype = Union{Missing,rowtype})
        return _collectas(rowtype, v)
    end
    storagetype = _writeruniontype(observed)
    nullable && (storagetype = Union{Missing,storagetype})
    return _collectas(storagetype, v)
end

"The field-wise Union of same-named NamedTuple row types, or nothing."
function _widenednamedtuple(observed)
    names = fieldnames(first(observed))
    all(T -> T <: NamedTuple && fieldnames(T) === names, observed) || return nothing
    types = _uninferred(
        ntuple,
        i -> Union{(fieldtype(T, i) for T in observed)...},
        length(names),
    )::Tuple
    return NamedTuple{names,Tuple{types...}}
end

# ---- registered rewrites: lower a labelled column once ----------------------

"Append runtime type `T` to `types` once; more than 8 is an error naming the column."
function _pushobserved!(types::Vector{Type}, seen::Base.IdSet{Type}, T::Type, name::String)
    T in seen && return nothing
    length(types) < _MAX_INFERRED_WRITER_TYPES || throw(
        ArgumentError(
            "column $name holds values of more than $_MAX_INFERRED_WRITER_TYPES types " *
            "($(_uninferred(join, types, ", ")::String), $T, ...); give it a concrete " *
            "element type",
        ),
    )
    push!(seen, T)
    push!(types, T)
    return nothing
end

"""
Whether a column's values lower to storage before `f`'s builder runs: a
labelled field's do, and so do values with mappings of their own, except
under a union or an encoding, whose children lower their own values.
"""
_lowersvalues(f::AC.Field, target, v, context) =
    target !== nothing ||
    !(f.type isa Union{AC.UnionType,AC.RunEndEncodedType,AC.DictionaryType}) &&
    _mappedvalues(v, context)

"""
Whether `v`'s values lower through an `ArrowTypes.toarrow` mapping of their
own: the element type's, or a runtime type's in an abstract column.
"""
function _mappedvalues(v::AbstractVector, context::_WriterContext)
    T = Base.nonmissingtype(_eltypeof(v))
    T === Union{} && return false
    mapped(S) =
        S !== VersionNumber &&
        !(S <: Union{Int128,UInt128}) &&
        _writerstoragetype!(context, S) !== S
    isconcretetype(T) && return mapped(T)
    T isa Union && all(isconcretetype, Base.uniontypes(T)) &&
        return any(mapped, Base.uniontypes(T))
    return any(x -> x !== missing && mapped(typeof(x)), v)
end

"""
The values of a column under the retained field `f` lowered once to their
storage: `ArrowTypes.toarrow` on each value, converted to the storage type
`f`'s target declares (`Missing` kept), or, when that storage is abstract or
`f` has no target, to the promoted type of the values' lowered types. A
labelled child inside a lowered value keeps its logical value, which lowers
when that child builds.
"""
function _lowercolumn(f::AC.Field, target, v::AbstractVector, context::_WriterContext)
    name = f.name
    target === nothing && return _inferlowered(f, nothing, v, context)
    target === VersionNumber && return Union{Missing,String}[
        x === missing ? missing : string(x) for x in v
    ]
    declared = _writerstoragetype!(context, target)
    _checkstoragemissing(target, declared)
    _arrowtypesconcreteorunion(declared) ||
        _arrowtypesstorageisspecified(context, declared) ||
        return _inferlowered(f, target, v, context)
    if !isconcretetype(target)
        seen = Base.IdSet{Type}()
        for x in v
            x === missing && continue
            T = typeof(x)
            T in seen && continue
            push!(seen, T)
            _checkabstractlabel(f, target, T, context)
        end
    end
    storage = any(x -> x === missing, v) ? Union{Missing,declared} : declared
    return _writerloweredcolumn(storage, v, context, target, name, f)
end

"`values` lowered by `lower` one at a time as they are read; missing stays missing."
struct _LoweredValues{T,V<:AbstractVector,F} <: AbstractVector{T}
    lower::F
    values::V
end
_LoweredValues{T}(lower::F, values::V) where {T,V,F} = _LoweredValues{T,V,F}(lower, values)
Base.size(l::_LoweredValues) = (length(l.values),)
Base.@propagate_inbounds function Base.getindex(l::_LoweredValues{T}, i::Int) where {T}
    x = l.values[firstindex(l.values) + i - 1]
    return (x === missing ? missing : l.lower(x))::T
end

# Every value of a column under a labelled field is its target's; a concrete
# element type that is not the target says the column was replaced.
function _checktarget(f::AC.Field, target, v::AbstractVector)
    T = Base.nonmissingtype(_eltypeof(v))
    (T === Union{} || T <: target) && return nothing
    i =
        isconcretetype(T) ? firstindex(v) :
        findfirst(x -> x !== missing && !(x isa target), v)
    i === nothing && return nothing
    S = isconcretetype(T) ? T : typeof(v[i])
    throw(
        ArgumentError(
            "column $(f.name) holds $S values, but its retained ArrowTypes target is " *
            "$target",
        ),
    )
end

"""
`v` lowered value by value through each value's own mapping, as a column of
the promoted type of the lowered types (a Union of them when they do not
promote to one concrete type). Under an abstract target, each runtime type
must carry the target's label.
"""
function _inferlowered(f::AC.Field, target, v::AbstractVector, context::_WriterContext)
    name = f.name
    values = Vector{Any}(undef, length(v))
    observed = Type[]
    seen = Base.IdSet{Type}()
    runtimes = Base.IdSet{Type}()
    hasmissing = false
    for (i, x) in enumerate(v)
        if x === missing
            values[i] = missing
            hasmissing = true
            continue
        end
        T = typeof(x)
        if !(T in runtimes)
            push!(runtimes, T)
            target === nothing || _checkabstractlabel(f, target, T, context)
        end
        mapped = T === VersionNumber ? string(x) : _writertoarrow(context, x, T, name, f)
        values[i] = mapped
        _pushobserved!(observed, seen, typeof(mapped), name)
    end
    isempty(observed) && return fill(missing, length(v))
    storagetype = reduce(_writerpromoteunion, observed)
    hasmissing && (storagetype = Union{Missing,storagetype})
    return _writerconvertedcolumn(storagetype, values, context, something(target, Any), name)
end

"""
Build `f` without its label over `storage`, values in its storage domain
(`context.lowered`), and give the result `f`'s label and nullability back.
"""
function _constructlowered(f::AC.Field, storage::AbstractVector, context::_WriterContext)
    lowered = context.lowered
    context.lowered = true
    built, data = try
        _constructpart(_withoutownextension(f), storage; context)
    finally
        context.lowered = lowered
    end
    return _retainedfield(f; children=collect(AC.Field, built.children)), data
end

function _writerhiddenbytes(f::AC.Field, nbytes::Integer, what::String)
    0 <= nbytes <= typemax(Int) || throw(
        ArgumentError(
            "hidden $what for retained field $(f.name) needs an unsupported " *
            "$nbytes-byte buffer",
        ),
    )
    nbytes == 0 && return AC.BufferSlice()
    return AC._databuffer(zeros(UInt8, Int(nbytes)))
end

function _writerhiddenmul(f::AC.Field, left::Integer, right::Integer, what::String)
    try
        return Base.checked_mul(Int64(left), Int64(right))
    catch err
        err isa OverflowError || rethrow()
        throw(
            ArgumentError(
                "hidden $what for retained field $(f.name) exceeds the supported " *
                "length range",
            ),
        )
    end
end

# Zeroed buffers for `n` slots of `f`'s layout, all null when `f` is nullable.
function _writerhiddenbuffers(f::AC.Field, n::Int)
    spec = AC.layoutspec_of(f.type)
    buffers = AC.BufferSlice[]
    for role in spec.buffers
        nbytes =
            role == AC.VALIDITY ? (f.nullable ? cld(n, 8) : 0) :
            role == AC.DATA ?
            (
                spec.fixedwidth == -1 ? cld(n, 8) :
                _writerhiddenmul(f, n, spec.fixedwidth, "data")
            ) :
            role == AC.OFFSETS ? _writerhiddenmul(f, n + 1, spec.offsetwidth, "offset") :
            role == AC.ELEMENT_OFFSETS || role == AC.SIZES ?
            _writerhiddenmul(f, n, spec.offsetwidth, "element-offset") :
            role == AC.VIEWS ? _writerhiddenmul(f, n, 16, "view") :
            throw(
                ArgumentError(
                    "hidden construction for retained field $(f.name) needs a " *
                    "layout-specific $(role) buffer",
                ),
            )
        push!(buffers, _writerhiddenbytes(f, nbytes, "$role"))
    end
    return buffers, f.nullable ? n : 0
end

"""
`n` slots of retained field `f` that nothing reads, from zeroed buffers and no
values: nulls where `f` is nullable, zero placeholders where it is not (an
all-missing column of a later table, a child of null rows only).
"""
function _constructhiddenpart(f::AC.Field, n::Int, context::_WriterContext)
    n >= 0 || throw(ArgumentError("negative hidden length for retained field $(f.name)"))
    t = f.type
    t isa AC.NullType &&
        return _retainedfield(f), AC.ArrayData(t, n, AC.BufferSlice[]; nullcount=n)
    fields = AC.Field[]
    datas = AC.ArrayData[]
    function child!(c, m)
        cf, cd = _constructhiddenpart(c, m, context)
        push!(fields, cf)
        push!(datas, cd)
        return nothing
    end
    dictionary = nothing
    if t isa AC.DictionaryType
        # A non-nullable dictionary's valid indices point at one pool entry.
        poollength = n == 0 || f.nullable ? 0 : 1
        _, dictionary = _constructhiddenpart(AC.dictvaluefield(f, t), poollength, context)
        append!(fields, f.children)
    elseif t isa Union{AC.ListType,AC.ListViewType,AC.MapType}
        child!(only(f.children), 0)
    elseif t isa AC.FixedSizeListType
        child!(only(f.children), Int(_writerhiddenmul(f, n, t.listsize, "child")))
    elseif t isa AC.StructType
        foreach(c -> child!(c, n), f.children)
    elseif t isa AC.RunEndEncodedType
        # One run of one value.
        RT = AC.juliatype(f.children[1].type)
        n <= typemax(RT) || throw(
            ArgumentError("column $(f.name) length exceeds its retained run-end type"),
        )
        rf, rd = _constructpart(f.children[1], n == 0 ? RT[] : RT[RT(n)]; context)
        push!(fields, rf)
        push!(datas, rd)
        child!(f.children[2], min(n, 1))
        data = AC.ArrayData(t, n, AC.BufferSlice[]; children=datas, nullcount=0)
        return _retainedfield(f; children=fields), data
    elseif t isa AC.UnionType
        # Every slot takes slot 0 of the first Null or nullable child.
        k = something(
            findfirst(c -> c.type isa AC.NullType, f.children),
            findfirst(c -> c.nullable, f.children),
            1,
        )
        sparse = t.mode == AC.SparseMode
        for (j, c) in enumerate(f.children)
            child!(c, sparse ? n : j == k ? min(n, 1) : 0)
        end
        typeids = n == 0 ? AC.BufferSlice() : AC._databuffer(fill(t.typeids[k], n))
        offsets = n == 0 ? AC.BufferSlice() : AC._databuffer(zeros(Int32, n))
        buffers = sparse ? AC.BufferSlice[typeids] : AC.BufferSlice[typeids, offsets]
        data = AC.ArrayData(t, n, buffers; children=datas, nullcount=0)
        return _retainedfield(f; children=fields), data
    end
    buffers, nullcount = _writerhiddenbuffers(f, n)
    data = AC.ArrayData(t, n, buffers; children=datas, dictionary, nullcount)
    return _retainedfield(f; children=fields), data
end

# Registered mappings exchange ArrowTypes-domain temporals (`_arrowtypesscalar`):
# micro/nanosecond timestamps as raw Int64 and second/millisecond timestamps as
# zone-free DateTime. Unmarked fresh composites bring public values to the same
# seam, so both domains lower here; anything else has no exact storage.
function _arrowtypestemporalstorage(t::AC.ArrowType, value)
    ok, storage = _facadetostorage(t, value)
    ok && return storage
    t isa AC.TimestampType || return nothing
    t.unit in (AC.MICROSECOND, AC.NANOSECOND) && value isa Int64 && return value
    return value isa Dates.DateTime ? _timestampstorage(t.unit, value) : nothing
end

"""
Empty storage vector typed for one Field's physical values (Any for Unions or
unknown types).
"""
function _writerstoragevector(f::AC.Field)
    # ArrowTypes.jl hooks see FixedSizeBinary storage as NTuple{N,UInt8}.
    t = f.type
    t isa AC.FixedSizeBinaryType && return _withmissingtype(
        Union{_arrowtypesfixedbinarytype(t),Vector{UInt8}},
        f.nullable,
    )[]
    T = _declaredeltype(f, false)
    return f.type isa AC.UnionType || T === Any ? Any[] : T[]
end

function _writerpushstorage!(
    storage::AbstractVector,
    value,
    f::AC.Field,
    writertype::Type,
    context::_WriterContext,
)
    stored = _writerconvertlowered(_eltypeof(storage), value, context, writertype, f.name)
    push!(storage, stored)
    return nothing
end

"Construct values already lowered to one Field's storage domain."
function _constructwriterstorage(
    f::AC.Field,
    storage::AbstractVector,
    context::_WriterContext,
)
    t = f.type
    if t isa AC.DateType ||
       t isa AC.TimestampType ||
       t isa AC.TimeType ||
       t isa AC.DurationType
        values = Union{Missing,Int64}[x === missing ? missing : Int64(x) for x in storage]
        return _rebuildtemporal(f, values)
    end
    storagefield = _writerstoragefield(f)
    lowered = context.lowered
    context.lowered = true
    try
        _, data = _constructpart(storagefield, storage; context)
        return f, data
    finally
        context.lowered = lowered
    end
end

"Remove logical labels while rebuilding values already in physical storage."
function _writerstoragefield(f::AC.Field)
    own = _withoutownextension(f)
    children = AC.Field[_writerstoragefield(child) for child in f.children]
    return AC.Field(
        own.name,
        own.type;
        nullable=own.nullable,
        metadata=_fieldmetadata(own),
        children,
    )
end

"A fixed_size_binary(N) column from `NTuple{N,UInt8}` rows."
function _fixedbinarypart(name::String, v::AbstractVector, N::Int)
    T = NTuple{N,UInt8}
    t = AC.FixedSizeBinaryType(N)
    field = AC.Field(name, t; nullable=Missing <: _eltypeof(v))
    if _eltypeof(v) === T
        data = something(_fixedbuffer(v), AC._databuffer(collect(T, v)))
        return field, AC.ArrayData(t, length(v), [AC.BufferSlice(), data]; nullcount=0)
    end
    rows, validity, nc = _nullablevalues(T, v, ntuple(_ -> 0x00, N))
    return field, AC.ArrayData(t, length(v), [validity, AC._databuffer(rows)]; nullcount=nc)
end

"Registered public-domain value Field and Julia target for a retained dictionary."
function _registereddictionaryfield(context::_WriterContext, f::AC.Field)
    t = f.type::AC.DictionaryType
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    target === nothing && return nothing, nothing
    valuefield = _arrowtypesdictvaluefield(f, t; retainmetadata=true)
    return valuefield, target
end

"Recursive Field equality: name, type, nullability, ordered metadata, and children."
_fieldcontractequal(a::AC.Field, b::AC.Field) =
    _conformancemismatch(a, b; exact=true) === nothing

"""
Assemble a fresh Int32-indexed dictionary Field and its first batch from
already-built pool data.
"""
# The narrowest signed index type for `n` categories (indices 0 to n - 1).
_dictindextype(n::Integer) =
    n <= 128 ? AC.IntType(8, true) :
    n <= 32_768 ? AC.IntType(16, true) :
    n - 1 <= typemax(Int32) ? AC.IntType(32, true) : AC.IntType(64, true)

# Whether a column's categories are ordered: a read dictionary column's flag,
# a CategoricalArray's (the CategoricalArrays extension), else false.
_isordered(::AbstractVector) = false
_isordered(x::SubArray) = _isordered(parent(x))
_isordered(x::DictEncode) = _isordered(x.data)
_isordered(x::DictEncoded) = x.ordered
_isordered(x::ChainedVector{T,<:DictEncoded}) where {T} =
    !isempty(x.arrays) && all(_isordered, x.arrays)

function _newdictfromdata(
    name::String,
    valuefield::AC.Field,
    valuedata::AC.ArrayData,
    indices::Vector;
    nullable::Bool,
    indextype::AC.IntType=AC.IntType(32, true),
    ordered::Bool=false,
)
    t = AC.DictionaryType(indextype, valuefield.type, ordered)
    field = AC.Field(
        name,
        t;
        nullable,
        metadata=_fieldmetadata(valuefield),
        children=collect(AC.Field, valuefield.children),
    )
    return field, _dictbatch(field, indices, valuedata)
end

# --- retained-schema rewrite (facade Table/Stream round-trips) --------------

"Storage integers for a column of public values under a RETAINED temporal descriptor."
function _retainedstorage(
    t::Union{AC.DateType,AC.TimestampType,AC.TimeType,AC.DurationType},
    v::AbstractVector,
    name::String,
)
    return _retainedstorage(_facadetoken(t), t, v, name)
end

@noinline function _retainedstorage(
    token::Val{K},
    t::Union{AC.DateType,AC.TimestampType,AC.TimeType,AC.DurationType},
    v::AbstractVector,
    name::String,
) where {K}
    out = Union{Missing,Int64}[]
    sizehint!(out, length(v))
    for x in v
        if x === missing
            push!(out, missing)
        else
            sv = _exactfacadevalue(token, x)
            sv === nothing && throw(
                ArgumentError(
                    "column $name holds $(typeof(x)) values that do not match " *
                    "its retained Arrow type $(repr(t)); the column was " *
                    "replaced with incompatible data",
                ),
            )
            push!(out, Int64(sv))
        end
    end
    return out
end

# The same for lowered values, in the ArrowTypes domain too: raw integers for
# microsecond and nanosecond timestamps, `DateTime`s for the coarser ones.
function _loweredstorage(t::AC.ArrowType, v::AbstractVector, name::String)
    out = Union{Missing,Int64}[]
    sizehint!(out, length(v))
    for x in v
        if x === missing
            push!(out, missing)
        else
            sv = _arrowtypestemporalstorage(t, x)
            sv === nothing && throw(
                ArgumentError(
                    "column $name holds the value $(repr(x)), which cannot be " *
                    "represented exactly by its retained Arrow type $(repr(t))",
                ),
            )
            push!(out, Int64(sv))
        end
    end
    return out
end

# The reader takes arrow.bool8 as Bool only on int8 storage; on other
# storage the label is kept and the values are written as they read.
_isbool8(f::AC.Field) =
    f.type == AC.IntType(8, true) &&
    _fieldmetavalue(f, _EXTENSION_NAME_KEY) == "arrow.bool8"
_bool8storage(v::AbstractVector) =
    Missing <: _eltypeof(v) ? Union{Missing,Int8}[x isa Bool ? Int8(x) : x for x in v] :
    Int8[x isa Bool ? Int8(x) : x for x in v]

function _retainedfield(f::AC.Field; children=collect(AC.Field, f.children))
    return AC.Field(
        f.name,
        f.type;
        nullable=f.nullable,
        metadata=_fieldmetadata(f),
        children=children,
    )
end

function _retainedvalidity(f::AC.Field, v::AbstractVector)
    present = Bool[x !== missing for x in v]
    f.nullable || all(present) || throw(_nonnullableerror(f.name))
    return present
end

# The writer's utf8/binary builders, after checking the values against the
# retained field: strings for utf8, byte vectors for binary.
function _retainedvarbytes(f::AC.Field, v::AbstractVector, pool=nothing)
    t = f.type::Union{AC.Utf8Type,AC.BinaryType}
    utf8 = t isa AC.Utf8Type
    Offset = t.large ? Int64 : Int32
    _, d =
        if v isa Union{_PayloadColumn,_PayloadRows} &&
           _payloadparent(v) isa (utf8 ? DataStrings.StringVector : DataStrings.BytesVector)
            _packedpayloadpart(
                f.name,
                _payloadentries(v),
                _payloadparent(v).buffers,
                t,
                Offset,
                Missing <: _eltypeof(v),
                pool,
            )
        else
            V = utf8 ? AbstractString : AbstractVector{UInt8}
            Base.nonmissingtype(_eltypeof(v)) <: V ||
                all(x -> x === missing || x isa V, v) ||
                throw(
                    ArgumentError(
                        "column $(f.name) must contain " *
                        (utf8 ? "string" : "byte-vector") *
                        " values",
                    ),
                )
            _varbytespart(f.name, v, t, Offset)
        end
    AC.nullcount(d) == 0 ||
        f.nullable ||
        throw(
            ArgumentError(
                "column $(f.name) holds missing values but its retained field is non-nullable",
            ),
        )
    return _retainedfield(f), d
end

function _retainedview(f::AC.Field, v::AbstractVector)
    t = f.type::AC.ViewType
    present = _retainedvalidity(f, v)
    payloads = Vector{DataStrings.StringPayload}(undef, length(v))
    data = UInt8[]
    for (i, x) in enumerate(v)
        if x === missing
            # An empty view: the validity bit marks the null, and the slot
            # stays valid when a parent hides this child's validity.
            payloads[i] = DataStrings.StringPayload(UInt64(0), UInt64(0))
            continue
        end
        bytes = if t.utf8
            x isa AbstractString ||
                throw(ArgumentError("column $(f.name) must contain string values"))
            _utf8units(x)
        else
            x isa AbstractVector{UInt8} ||
                throw(ArgumentError("column $(f.name) must contain byte-vector values"))
            x
        end
        n = length(bytes)
        if n <= DataStrings.INLINE_MAX
            payloads[i] = DataStrings.inline_payload(bytes, 1, n)
        else
            length(data) <= typemax(Int32) - n || throw(
                ArgumentError("column $(f.name) view data exceeds the Int32 offset range"),
            )
            off = length(data)
            append!(data, bytes)
            payloads[i] = DataStrings.view_payload(data, off + 1, n, 0, off)
        end
    end
    buffers = AC.BufferSlice[AC._bitmapbuffer(present), AC._databuffer(payloads)]
    isempty(data) || push!(buffers, AC._databuffer(data))
    d = AC.ArrayData(t, length(v), buffers; nullcount=count(!, present))
    return _retainedfield(f), d
end

function _retainedfixedbytes(f::AC.Field, v::AbstractVector)
    t = f.type::AC.FixedSizeBinaryType
    if Base.nonmissingtype(_eltypeof(v)) === NTuple{t.nbytes,UInt8}
        _, d = _fixedbinarypart(f.name, v, t.nbytes)
        AC.nullcount(d) > 0 &&
            !f.nullable &&
            throw(
                ArgumentError(
                    "column $(f.name) holds missing values but its retained field is non-nullable",
                ),
            )
        return _retainedfield(f), d
    end
    present = _retainedvalidity(f, v)
    data = zeros(UInt8, Base.checked_mul(length(v), t.nbytes))
    for (i, x) in enumerate(v)
        x === missing && continue
        x isa Union{AbstractVector{UInt8},NTuple{<:Any,UInt8}} ||
            throw(ArgumentError("column $(f.name) must contain byte-vector values"))
        length(x) == t.nbytes || throw(
            ArgumentError(
                "column $(f.name) fixed-size value $i has $(length(x)) bytes; " *
                "expected $(t.nbytes)",
            ),
        )
        for k = 1:(t.nbytes)
            data[(i - 1) * t.nbytes + k] = x[k]
        end
    end
    d = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), AC._databuffer(data)];
        nullcount=count(!, present),
    )
    return _retainedfield(f), d
end

"Values (`fill` at missing slots), validity buffer, and null count of a column."
function _nullablevalues(::Type{S}, v::AbstractVector, fill::S, f::F=identity) where {S,F}
    n = length(v)
    i0 = firstindex(v) - 1
    vals = Vector{S}(undef, n)
    bits = Vector{UInt8}(undef, cld(n, 8))
    valid = 0
    # Each validity byte is built in a register from 8 values, branch-free;
    # whole bytes take a fixed 8-step loop the compiler unrolls.
    @inbounds for b in eachindex(bits)
        base = (b - 1) << 3
        byte = 0x00
        if base + 8 <= n
            for j = 0:7
                x = v[i0 + base + j + 1]
                present = x !== missing
                vals[base + j + 1] = present ? f(x) : fill
                byte |= UInt8(present) << (j % UInt)
            end
        else
            for j = 0:(n - base - 1)
                x = v[i0 + base + j + 1]
                present = x !== missing
                vals[base + j + 1] = present ? f(x) : fill
                byte |= UInt8(present) << (j % UInt)
            end
        end
        bits[b] = byte
        valid += count_ones(byte)
    end
    nc = n - valid
    validity =
        nc == 0 ? AC.BufferSlice() : AC.BufferSlice(AC.heapregion(bits), 0, length(bits))
    return vals, validity, nc
end

# A `Decimal{P,S,T}` is its storage integer, so a column of them already is
# the Arrow coefficient buffer: a `Vector` is wrapped without copying and any
# other vector is collected once. Values write as stored, as Arrow C++ does;
# the precision check is `validate_full`'s.
function _decimaldata(f::AC.Field, v::AbstractVector, ::Type{D}) where {D}
    t = f.type
    if _eltypeof(v) === D
        data = something(_fixedbuffer(v), AC._databuffer(collect(D, v)))
        return AC.ArrayData(t, length(v), [AC.BufferSlice(), data]; nullcount=0)
    end
    vals, validity, nc = _nullablevalues(D, v, zero(D))
    nc > 0 &&
        !f.nullable &&
        throw(
            ArgumentError(
                "column $(f.name) holds missing values but its retained field is non-nullable",
            ),
        )
    return AC.ArrayData(t, length(v), [validity, AC._databuffer(vals)]; nullcount=nc)
end

# `v`'s decimals as `H`s, whose storage is `T`.
_widendecimals(::Type{H}, ::Type{T}, v) where {H,T} =
    map(x -> x === missing ? missing : reinterpret(H, T(DataDecimals.unscaled(x))), v)

function _retaineddecimal(f::AC.Field, v::AbstractVector)
    t = f.type::AC.DecimalType
    D = Base.nonmissingtype(_eltypeof(v))
    if _shareddecimal(t)
        H = _decimalhost(t)
        D === H && return _retainedfield(f), _decimaldata(f, v, D)
        # Another storage width at the field's scale (`dec"1.25"` stores an
        # Int32): widen every coefficient in one pass.
        if D <: DataDecimals.Decimal && isconcretetype(D) && D.parameters[2] == t.scale
            w = _widendecimals(H, _decimalstorage(t), v)
            return _retainedfield(f), _decimaldata(f, w, H)
        end
    end
    v = [x isa DataDecimals.AbstractDecimal ? _shareddecimalraw(t, x) : x for x in v]
    present = _retainedvalidity(f, v)
    if t.bits == 32 || t.bits == 64
        T = t.bits == 32 ? Int32 : Int64
        values = Vector{T}(undef, length(v))
        for (i, x) in enumerate(v)
            x === missing ||
                x isa T ||
                throw(
                    ArgumentError(
                        "column $(f.name) must contain $T decimal storage values",
                    ),
                )
            values[i] = x === missing ? zero(T) : x
        end
        databuf = AC._databuffer(values)
    else
        width = AC.primwidth(t)
        values = zeros(UInt8, Base.checked_mul(length(v), width))
        for (i, x) in enumerate(v)
            x === missing && continue
            x isa AbstractVector{UInt8} || throw(
                ArgumentError("column $(f.name) must contain byte-vector decimal values"),
            )
            length(x) == width || throw(
                ArgumentError(
                    "column $(f.name) decimal value $i has $(length(x)) bytes; " *
                    "expected $width",
                ),
            )
            copyto!(values, (i - 1) * width + 1, x, 1, width)
        end
        databuf = AC._databuffer(values)
    end
    d = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), databuf];
        nullcount=count(!, present),
    )
    return _retainedfield(f), d
end

function _retainedinterval(f::AC.Field, v::AbstractVector)
    t = f.type::AC.IntervalType
    v = [x isa _IntervalValue ? _sharedintervalraw(t, x) : x for x in v]
    present = _retainedvalidity(f, v)
    if t.unit == AC.YEAR_MONTH
        values = Int32[
            x === missing ? Int32(0) :
            x isa Int32 ? x :
            throw(ArgumentError("column $(f.name) must contain Int32 intervals")) for
            x in v
        ]
    elseif t.unit == AC.DAY_TIME
        T = NamedTuple{(:days, :millis),Tuple{Int32,Int32}}
        values = T[
            x === missing ? T((0, 0)) :
            x isa T ? x :
            throw(ArgumentError("column $(f.name) must contain $T intervals")) for
            x in v
        ]
    else
        T = NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}}
        values = T[
            x === missing ? T((0, 0, 0)) :
            x isa T ? x :
            throw(ArgumentError("column $(f.name) must contain $T intervals")) for
            x in v
        ]
    end
    d = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), AC._databuffer(values)];
        nullcount=count(!, present),
    )
    return _retainedfield(f), d
end

function _validateretainedstructrows(f::AC.Field, rows::AbstractVector)
    nchildren = length(f.children)
    for row in rows
        row === missing && continue
        if row isa NamedTuple
            # A typed read's row: the field names must be the children's.
            (
                length(row) == nchildren &&
                all(j -> String(keys(row)[j]) == f.children[j].name, 1:nchildren)
            ) || throw(
                ArgumentError(
                    "column $(f.name) retained struct row has fields $(keys(row)); " *
                    "expected $(Tuple(c.name for c in f.children))",
                ),
            )
            continue
        end
        row isa AbstractVector || throw(
            ArgumentError(
                "column $(f.name) retained struct rows must be ordered Pair vectors",
            ),
        )
        length(row) == nchildren || throw(
            ArgumentError(
                "column $(f.name) retained struct row has $(length(row)) fields; " *
                "expected $nchildren",
            ),
        )
        firstrowindex = firstindex(row)
        for j = 1:nchildren
            kv = row[firstrowindex + j - 1]
            kv isa Pair || throw(
                ArgumentError("column $(f.name) retained struct rows must contain Pairs"),
            )
            first(kv) == f.children[j].name || throw(
                ArgumentError(
                    "column $(f.name) retained struct child $j is named " *
                    "$(repr(first(kv))); expected $(repr(f.children[j].name))",
                ),
            )
        end
    end
    return nothing
end

function _writermapkeyisless(f::AC.Field, a, b)
    try
        result = isless(a, b)
        result isa Bool || throw(
            ArgumentError("Map key comparison for field $(f.name) did not return Bool"),
        )
        return result
    catch err
        err isa Union{InterruptException,OutOfMemoryError} && rethrow()
        err isa MethodError || rethrow()
        throw(
            ArgumentError(
                "Map field $(f.name) declares sorted keys, but keys of types " *
                "$(typeof(a)) and $(typeof(b)) cannot be ordered",
            ),
        )
    end
end

function _validatemapphysicalkeys(
    name::String,
    keyfield::AC.Field,
    keydata::AC.ArrayData,
    offsets,
)
    plan = _ArrowTypesRoutePlan()
    for row = 1:(length(offsets) - 1)
        lo = Int(offsets[row]) + 1
        hi = Int(offsets[row + 1])
        seen = Dict{_WriterStorageKey,Nothing}()
        for index = lo:hi
            value = _arrowtypesroutedvalue(keyfield, keydata, Int64(index), plan, true)
            key = _WriterStorageKey(value)
            haskey(seen, key) && throw(
                ArgumentError(
                    "column $name row $row has duplicate physical Map key storage",
                ),
            )
            seen[key] = nothing
        end
    end
    return nothing
end

function _retainedree(f::AC.Field, v::AbstractVector, context::_WriterContext)
    t = f.type::AC.RunEndEncodedType
    length(f.children) == 2 ||
        throw(ArgumentError("column $(f.name) retained REE descriptor needs two children"))
    runfield, valuefield = f.children
    runtype = runfield.type
    runtype isa AC.IntType ||
        throw(ArgumentError("column $(f.name) retained REE run ends must be integers"))
    RT = AC.juliatype(runtype)
    length(v) <= typemax(RT) ||
        throw(ArgumentError("column $(f.name) length exceeds its retained run-end type"))
    runends = RT[]
    runvalues = Any[]
    for (i, x) in enumerate(v)
        # Logical `isequal` may intentionally coarsen a custom type. Only the
        # non-overridable `===` relation is safe for physical run coalescing.
        if isempty(runvalues) || x !== last(runvalues)
            push!(runvalues, x)
            push!(runends, RT(i))
        else
            runends[end] = RT(i)
        end
    end
    rebuiltvaluefield, valuedata = _constructpart(valuefield, runvalues; context)
    physical = _writerstoredcolumn(rebuiltvaluefield, valuedata)
    if length(physical) > 1
        selected = Int[1]
        compactrunends = RT[runends[1]]
        previous = _WriterStorageKey(physical[1])
        for i = 2:length(physical)
            key = _WriterStorageKey(physical[i])
            if isequal(key, previous)
                compactrunends[end] = runends[i]
            else
                push!(selected, i)
                push!(compactrunends, runends[i])
                previous = key
            end
        end
        if length(selected) < length(physical)
            rebuiltvaluefield, valuedata = _rebuildwriterstorageselection(
                rebuiltvaluefield,
                physical,
                selected,
                context,
            )
            runends = compactrunends
        end
    end
    rebuiltrunfield, runenddata = _constructpart(runfield, runends; context)
    d = AC.ArrayData(
        t,
        length(v),
        AC.BufferSlice[];
        children=AC.ArrayData[runenddata, valuedata],
        nullcount=0,
    )
    return _retainedfield(f; children=AC.Field[rebuiltrunfield, rebuiltvaluefield]), d
end

"Construct one part under a retained Field: descriptor, nullability, metadata."
Base.@nospecializeinfer _constructpart(
    f::AC.Field,
    @nospecialize(v::AbstractVector);
    context::_WriterContext=_WriterContext(),
) = _uninferred(_retainedpartof, f, v, context)::Tuple{AC.Field,AC.ArrayData}

Base.@nospecializeinfer function _retainedpartof(
    f::AC.Field,
    @nospecialize(v::AbstractVector),
    context::_WriterContext,
)
    # No value to build: an empty column is its field's empty data.
    isempty(v) && return _constructhiddenpart(f, 0, context)
    # Values of an abstract element type (a flattened list's, a struct field's)
    # narrow to the types they hold.
    _arrowtypesconcreteorunion(_eltypeof(v)) || (v = map(identity, v))
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    if _lowersvalues(f, target, v, context)
        target === nothing || _checktarget(f, target, v)
        # No value to lower: an all-missing column is its field's nulls.
        f.nullable && all(ismissing, v) && return _constructhiddenpart(f, length(v), context)
        return _constructlowered(f, _lowercolumn(f, target, v, context), context)
    end
    return _constructretainedpart(f, v, context)
end

# Writer-side element types that a retained descriptor's per-value lowering
# converts exactly, beyond its public type: a written `DateTime` (or another
# date-time) column under a timestamp, any `AbstractString` under a string.
_retainedwritertype(::AC.TimestampType, ::Type{T}) where {T} = T <: Dates.AbstractDateTime
_retainedwritertype(::AC.Utf8Type, ::Type{T}) where {T} = T <: AbstractString
_retainedwritertype(t::AC.ViewType, ::Type{T}) where {T} =
    t.utf8 ? T <: AbstractString : T <: AbstractVector{UInt8}
_retainedwritertype(::AC.BinaryType, ::Type{T}) where {T} = T <: AbstractVector{UInt8}
_retainedwritertype(t::AC.FixedSizeBinaryType, ::Type{T}) where {T} =
    T <: NTuple{t.nbytes,UInt8} || T <: AbstractVector{UInt8}
_retainedwritertype(::AC.ArrowType, ::Type) = false

function _checkretainedidentity(f::AC.Field, t::AC.ArrowType, v::AbstractVector)
    # Identity FIRST, for every retained field with a known facade type:
    # a replaced column is rejected on its declared element type before any
    # value is read.
    Fp = _facadebasetype(t)
    if Fp !== Any
        NT = Base.nonmissingtype(_eltypeof(v))
        NT <: Fp ||
            NT === Union{} ||
            _retainedwritertype(t, NT) ||
            throw(
                ArgumentError(
                    "column $(f.name) holds $(NT) values, but its retained Arrow " *
                    "type $(repr(t)) materializes as $(Fp); the column was " *
                    "replaced with incompatible data",
                ),
            )
        _eltypeof(v) >: Missing &&
            !f.nullable &&
            throw(
                ArgumentError(
                    "column $(f.name) may hold missing values but its retained " *
                    "field is non-nullable",
                ),
            )
    end
    return Fp
end

"One part under a retained field: the identity check, then the type's builder."
function _constructretainedpart(f::AC.Field, v::AbstractVector, context::_WriterContext)
    t = f.type
    t isa AC.DictionaryType && throw(
        ArgumentError(
            "nested retained dictionary field $(f.name) cannot be reconstructed " *
            "after facade materialization because its pool is unavailable",
        ),
    )
    if t isa AC.NullType
        # `Nothing` is a logical type's Null storage.
        all(x -> x === missing || x === nothing, v) || throw(
            ArgumentError(
                "column $(f.name) no longer matches its retained Arrow NullType; " *
                "give it a Missing element type",
            ),
        )
        d = AC.ArrayData(t, length(v), AC.BufferSlice[]; nullcount=length(v))
        return AC.Field(f.name, t; nullable=f.nullable, metadata=_fieldmetadata(f)), d
    end
    if t isa Union{AC.DateType,AC.TimestampType,AC.TimeType,AC.DurationType}
        context.lowered && return _rebuildtemporal(f, _loweredstorage(t, v, f.name))
        _checkretainedidentity(f, t, v)
        return _rebuildtemporal(f, _retainedstorage(t, v, f.name))
    end
    _checkretainedidentity(f, t, v)
    t isa Union{AC.Utf8Type,AC.BinaryType} && return _retainedvarbytes(f, v, context.pool)
    t isa AC.ViewType && return _retainedview(f, v)
    t isa AC.FixedSizeBinaryType && return _retainedfixedbytes(f, v)
    t isa AC.DecimalType &&
        return _uninferred(_retaineddecimal, f, v)::Tuple{AC.Field,AC.ArrayData}
    t isa AC.IntervalType &&
        return _uninferred(_retainedinterval, f, v)::Tuple{AC.Field,AC.ArrayData}
    t isa Union{AC.ListType,AC.ListViewType} && return _listpart(f, v, context)
    t isa AC.FixedSizeListType && return _fixedlistpart(f, v, context)
    t isa AC.StructType && return _structpart(f, v, context)
    t isa AC.MapType && return _mappart(f, v, context)
    t isa AC.RunEndEncodedType && return _retainedree(f, v, context)
    if t isa AC.UnionType
        built = _constructfacadeunion(f, v, context)
        built === nothing || return built
        throw(
            ArgumentError(
                "column $(f.name) is an Arrow union, and some of its values fit none or " *
                "several of the union's children (two children of one type, say), so " *
                "which child each value belongs to is unknown; write the values as a " *
                "fresh column instead (Arrow.write(path, DataFrame(table)) infers the " *
                "column's type from them) or drop the column",
            ),
        )
    end
    # Build the column naturally, then impose the retained descriptor: types
    # must match and nullability comes from the retained field. Missing values
    # under a non-nullable field mean the column was replaced.
    fn, dn = _constructpart(f.name, v; context)
    # Polars' and Arrow 2.x's 128-bit Int type, which pyarrow cannot read,
    # rewrites as a fresh Int128 column writes: fixed-size binary plus the
    # JuliaLang.(U)Int128 label.
    wide = t isa AC.IntType && t.bits == 128 && fn.type isa AC.FixedSizeBinaryType
    wide ||
        AC.typeequal(fn.type, t) ||
        throw(
            ArgumentError(
                "column $(f.name) no longer matches its retained Arrow type " *
                "$(repr(t)); it now maps to $(repr(fn.type))",
            ),
        )
    fn.nullable &&
        !f.nullable &&
        AC.nullcount(dn) > 0 &&
        throw(
            ArgumentError(
                "column $(f.name) holds missing values but its retained field is " *
                "non-nullable",
            ),
        )
    metadata = _fieldmetadata(f)
    if wide
        metadata = Pair{String,String}[
            p for p in something(metadata, Pair{String,String}[]) if
            !startswith(first(p), "ARROW:extension:")
        ]
        append!(metadata, something(_fieldmetadata(fn), Pair{String,String}[]))
    end
    rebuilt = AC.Field(
        f.name,
        fn.type;
        nullable=f.nullable,
        metadata=metadata,
        children=collect(AC.Field, fn.children),
    )
    return rebuilt, dn
end

function _rebuildtemporal(f::AC.Field, storage)
    t = f.type
    nmissing = count(x -> x === missing, storage)
    nmissing > 0 &&
        !f.nullable &&
        throw(
            ArgumentError(
                "column $(f.name) holds missing values but its retained field is " *
                "non-nullable",
            ),
        )
    fld = AC.Field(f.name, t; nullable=f.nullable, metadata=_fieldmetadata(f))
    return fld, _temporaldata(t, storage)
end

"""
A dictionary pool hint whose first `prefix` categories are kept exactly (a
pool already written or retained from an Arrow source); the categories after
them are candidates, deduplicated by storage value like a column's own.
"""
struct _PoolHint{V<:AbstractVector}
    values::V
    prefix::Int
end
_hintvalues(hint::_PoolHint) = hint.values
_hintvalues(hint::AbstractVector) = hint
_hintprefix(hint::_PoolHint) = hint.prefix
_hintprefix(hint::AbstractVector) = length(hint)

# Encoding identity belongs to the lowered Arrow storage domain. These
# wrappers compare physical contents and use canonical byte hashing for strings,
# without calling custom logical `hash`, `isequal`, `==`, or `isless` methods.
# Dictionary pool dedup, dictionary compaction, and run-end compaction share this rule.
struct _WriterStorageKey{T}
    value::T
end

_writerstorageroute(value::_ArrowTypesRoutedUnion) = (value.child, value.value)
_writerstorageroute(value) = nothing

const _WRITER_INTEGER_STORAGE = Union{Int8,Int16,Int32,Int64,UInt8,UInt16,UInt32,UInt64}

function _writerstorageequal(left, right, depth::Int=0)
    depth <= _MAX_WRITER_SCHEMA_DEPTH ||
        throw(ArgumentError("writer storage value exceeds the supported nesting depth"))
    leftroute = _writerstorageroute(left)
    rightroute = _writerstorageroute(right)
    if leftroute !== nothing || rightroute !== nothing
        leftroute === nothing && return false
        rightroute === nothing && return false
        return first(leftroute) == first(rightroute) &&
               _writerstorageequal(last(leftroute), last(rightroute), depth + 1)
    end
    (left === missing || right === missing) && return left === right
    (left === nothing || right === nothing) && return left === right
    if left isa AbstractString || right isa AbstractString
        left isa AbstractString && right isa AbstractString || return false
        ncodeunits(left) == ncodeunits(right) || return false
        for i = 1:ncodeunits(left)
            codeunit(left, i) === codeunit(right, i) || return false
        end
        return true
    end
    if left isa AbstractVector || right isa AbstractVector
        left isa AbstractVector && right isa AbstractVector || return false
        length(left) == length(right) || return false
        for i = 1:length(left)
            _writerstorageequal(left[i], right[i], depth + 1) || return false
        end
        return true
    end
    if left isa Pair || right isa Pair
        left isa Pair && right isa Pair || return false
        return _writerstorageequal(first(left), first(right), depth + 1) &&
               _writerstorageequal(last(left), last(right), depth + 1)
    end
    if left isa NamedTuple || right isa NamedTuple
        left isa NamedTuple && right isa NamedTuple || return false
        keys(left) === keys(right) || return false
        return _writerstorageequal(Tuple(left), Tuple(right), depth + 1)
    end
    if left isa Tuple || right isa Tuple
        left isa Tuple && right isa Tuple || return false
        length(left) == length(right) || return false
        for i = 1:length(left)
            _writerstorageequal(left[i], right[i], depth + 1) || return false
        end
        return true
    end
    if left isa AbstractFloat || right isa AbstractFloat
        # Compare floats by bits: pool compaction must keep 0.0 and -0.0
        # distinct and must treat identical NaN payloads as one category.
        typeof(left) === typeof(right) || return false
        left isa Float16 && return reinterpret(UInt16, left) === reinterpret(UInt16, right)
        left isa Float32 && return reinterpret(UInt32, left) === reinterpret(UInt32, right)
        left isa Float64 && return reinterpret(UInt64, left) === reinterpret(UInt64, right)
        return false
    end
    if left isa _WRITER_INTEGER_STORAGE ||
       right isa _WRITER_INTEGER_STORAGE ||
       left isa Bool ||
       right isa Bool ||
       left isa Char ||
       right isa Char
        return typeof(left) === typeof(right) && left === right
    end
    typeof(left) === typeof(right) || return false
    # Primitive types have no fields; their bits still distinguish values.
    isprimitivetype(typeof(left)) && return left === right
    # Mutable values have no structural identity we may trust; compare by
    # reference and hash by objectid, so equal-but-distinct mutables stay
    # separate categories.
    Base.ismutabletype(typeof(left)) && return left === right
    fieldcount(typeof(left)) == fieldcount(typeof(right)) || return false
    for i = 1:fieldcount(typeof(left))
        _writerstorageequal(getfield(left, i), getfield(right, i), depth + 1) ||
            return false
    end
    return true
end

function _writerstoragehash(value, seed::UInt, depth::Int=0)
    depth <= _MAX_WRITER_SCHEMA_DEPTH ||
        throw(ArgumentError("writer storage value exceeds the supported nesting depth"))
    route = _writerstorageroute(value)
    if route !== nothing
        h = hash(UInt8(0x01), seed)
        h = hash(first(route), h)
        return _writerstoragehash(last(route), h, depth + 1)
    end
    value === missing && return hash(UInt8(0x02), seed)
    value === nothing && return hash(UInt8(0x03), seed)
    if value isa AbstractString
        h = hash(UInt8(0x04), seed)
        @static if isdefined(Base, :hash_bytes) && isdefined(Base, :HASH_SECRET)
            value isa Union{String,SubString{String},DataStrings.DataString} &&
                return hash(value, h)
            value isa LazyString && return hash(String(value), h)
            n = ncodeunits(value)
            n == 0 && return hash("", h)
            if typeof(codeunit(value, 1)) === UInt8
                # CodeUnits bypasses custom hash/codeunits methods while matching String's byte hash.
                return Base.hash_bytes(Base.CodeUnits(value), UInt64(h), Base.HASH_SECRET) %
                       UInt
            end
        end
        h = hash(ncodeunits(value), h)
        for i = 1:ncodeunits(value)
            h = hash(codeunit(value, i), h)
        end
        return h
    end
    if value isa AbstractVector
        h = hash(UInt8(0x05), seed)
        h = hash(length(value), h)
        for item in value
            h = _writerstoragehash(item, h, depth + 1)
        end
        return h
    end
    if value isa Pair
        h = hash(UInt8(0x06), seed)
        h = _writerstoragehash(first(value), h, depth + 1)
        return _writerstoragehash(last(value), h, depth + 1)
    end
    if value isa NamedTuple
        h = hash(UInt8(0x07), seed)
        h = hash(keys(value), h)
        return _writerstoragehash(Tuple(value), h, depth + 1)
    end
    if value isa Tuple
        h = hash(UInt8(0x08), seed)
        h = hash(length(value), h)
        for item in value
            h = _writerstoragehash(item, h, depth + 1)
        end
        return h
    end
    if value isa AbstractFloat
        # Hash floats by bits, matching _writerstorageequal.
        h = hash(typeof(value), hash(UInt8(0x09), seed))
        bits =
            value isa Float16 ? reinterpret(UInt16, value) :
            value isa Float32 ? reinterpret(UInt32, value) : reinterpret(UInt64, value)
        return hash(bits, h)
    end
    if value isa _WRITER_INTEGER_STORAGE || value isa Bool || value isa Char
        return hash(value, hash(typeof(value), hash(UInt8(0x0a), seed)))
    end
    h = hash(typeof(value), hash(UInt8(0x0b), seed))
    isprimitivetype(typeof(value)) && return hash(objectid(value), h)
    # Mutables hash by objectid, matching _writerstorageequal's `===` rule.
    Base.ismutabletype(typeof(value)) && return hash(objectid(value), h)
    for i = 1:fieldcount(typeof(value))
        h = _writerstoragehash(getfield(value, i), h, depth + 1)
    end
    return h
end

Base.isequal(left::_WriterStorageKey, right::_WriterStorageKey) =
    _writerstorageequal(left.value, right.value)
Base.hash(key::_WriterStorageKey, seed::UInt) = _writerstoragehash(key.value, seed)

"""
An element type that holds both `T` and `S` values: `T` when it already
does, else their common supertype, with `missing` if either has it.
"""
function _widenedtype(T::Type, S::Type)
    S <: T && return T
    W = typejoin(Base.nonmissingtype(T), Base.nonmissingtype(S))
    return Missing <: T || Missing <: S ? Union{Missing,W} : W
end

"One value pool: keep a retained prefix exactly and append new categories."
function _dictionarypool(v::AbstractVector, retainedpool; writetype=nothing)
    if retainedpool === nothing
        T = writetype === nothing ? Base.nonmissingtype(_eltypeof(v)) : writetype
        T === Union{} && throw(
            ArgumentError(
                "cannot infer a dictionary value type from empty or all-missing columns",
            ),
        )
        pool = T[]
    else
        # New categories may be other types than the retained ones (writer
        # values after a public pool read back, DataStrings after Strings):
        # widen the value type instead of converting.
        pool = _collectas(
            _widenedtype(_eltypeof(retainedpool), Base.nonmissingtype(_eltypeof(v))),
            retainedpool,
        )
    end
    # Dedup with the non-overloadable structural key `_compactdictionarypool`
    # also uses: public-domain values may overload `hash`/`isequal`, so only the
    # `_WriterStorageKey` relation (bit-pattern floats, `===`/objectid for
    # mutables) may pre-merge categories. Exact value-based dedup of the
    # LOWERED categories still happens in `_compactdictionarypool`, in the
    # storage domain; this pass only keeps the candidate pool at
    # O(distinct-under-that-policy) instead of O(rows).
    seen = Dict{_WriterStorageKey,Nothing}()
    for x in pool
        x === missing && continue
        key = _WriterStorageKey(x)
        haskey(seen, key) || (seen[key] = nothing)
    end
    _addcategories!(pool, seen, v)
    return pool
end

# Function barrier: `pool` and each part of `vals` are only known at run time.
function _addcategories!(pool, seen, v)
    for x in v
        x === missing && continue
        key = _WriterStorageKey(x)
        if !haskey(seen, key)
            push!(pool, x)
            seen[key] = nothing
        end
    end
    return nothing
end

"First pool position for each non-null value; duplicate categories stay intact."
function _dictionarylookup(pool)
    # Same `_WriterStorageKey` policy as `_dictionarypool`: no custom logical
    # `hash`/`isequal` participates in encoding identity.
    lookup = Dict{_WriterStorageKey,Int64}()
    for (i, x) in enumerate(pool)
        x === missing && continue
        key = _WriterStorageKey(x)
        haskey(lookup, key) || (lookup[key] = Int64(i - 1))
    end
    return lookup
end

function _dictionaryindices(v, lookup, missingindex=nothing)
    return Union{Missing,Int64}[
        x === missing ? (missingindex === nothing ? missing : missingindex) :
        lookup[_WriterStorageKey(x)] for x in v
    ]
end

_writerstoredvalue(value::_ArrowTypesRoutedNull) = missing
_writerstoredvalue(value::_ArrowTypesRoutedUnion) =
    _ArrowTypesRoutedUnion(value.child, _writerstoredvalue(value.value))
function _writerstoredvalue(value::Pair{A,B}) where {A,B}
    firstvalue = _writerstoredvalue(first(value))
    lastvalue = _writerstoredvalue(last(value))
    return firstvalue isa A && lastvalue isa B ? Pair{A,B}(firstvalue, lastvalue) :
           firstvalue => lastvalue
end
function _writerstoredvalue(value::AbstractVector{T}) where {T}
    stored = Any[_writerstoredvalue(item) for item in value]
    return T === Any || !all(item -> item isa T, stored) ? stored : collect(T, stored)
end
_writerstoredvalue(value) = value

function _writerstoredcolumn(f::AC.Field, d::AC.ArrayData)
    plan = _ArrowTypesRoutePlan()
    return Any[_arrowtypesroutedvalue(f, d, Int64(i), plan, true) for i = 1:(d.len)]
end

function _rebuildwriterstorageselection(
    f::AC.Field,
    physical::Vector{Any},
    selected::Vector{Int},
    context::_WriterContext,
)
    storage = _writerstoragevector(f)
    sizehint!(storage, length(selected))
    for i in selected
        value = _writerstoredvalue(physical[i])
        _writerpushstorage!(storage, value, f, typeof(value), context)
    end
    return _constructwriterstorage(f, storage, context)
end

"Compact candidate categories by exact lowered storage identity."
function _compactdictionarypool(
    f::AC.Field,
    d::AC.ArrayData,
    retainedprefix::Int,
    context::_WriterContext,
)
    0 <= retainedprefix <= d.len ||
        throw(ArgumentError("retained dictionary prefix is outside the candidate pool"))
    physical = _writerstoredcolumn(f, d)
    selected, remap = _compactstorage(physical, retainedprefix)
    length(selected) == length(physical) && return f, d, remap

    compactfield, compactdata =
        _rebuildwriterstorageselection(f, physical, selected, context)
    return compactfield, compactdata, remap
end

"""
The first occurrence of each distinct stored value (`selected`, positions in
`physical`) and every position's 0-based index among them (`remap`). The
first `retainedprefix` positions are all kept, in order.
"""
function _compactstorage(physical::Vector{Any}, retainedprefix::Int)
    remap = Vector{Int64}(undef, length(physical))
    selected = Int[]
    seen = Dict{_WriterStorageKey,Int64}()
    for i in eachindex(physical)
        key = _WriterStorageKey(physical[i])
        if i <= retainedprefix
            index = Int64(length(selected))
            push!(selected, i)
            remap[i] = index
            haskey(seen, key) || (seen[key] = index)
            continue
        end
        index = get(seen, key, Int64(-1))
        if index < 0
            index = Int64(length(selected))
            push!(selected, i)
            seen[key] = index
        end
        remap[i] = index
    end
    return selected, remap
end

function _remapdictionaryindices(indices, remap::Vector{Int64})
    return Union{Missing,Int64}[
        index === missing ? missing : remap[Int(index) + 1] for index in indices
    ]
end

"One shared-pool dictionary batch: identical pool OBJECT across batches."
function _dictbatch(fld::AC.Field, indices::Vector, pool_d::AC.ArrayData)
    t = fld.type::AC.DictionaryType
    present = [x !== missing for x in indices]
    # The index type is a run-time property of the field.
    inds = _uninferred(_dictindexbuffer, AC.juliatype(t.indextype), indices)::AC.BufferSlice
    nc = count(!, present)
    d = AC.ArrayData(
        t,
        length(indices),
        [AC._bitmapbuffer(present), inds];
        dictionary=pool_d,
        nullcount=nc,
    )
    return d
end

_dictindexbuffer(::Type{IT}, indices) where {IT} =
    AC._databuffer(IT[x === missing ? zero(IT) : IT(x) for x in indices])

"Field + first-batch data for a dictionary column under a RETAINED type."
function _retaineddictfromdata(
    rf::AC.Field,
    valuefield::AC.Field,
    valuedata::AC.ArrayData,
    poollength::Int,
    firstidx::Vector,
    name::String,
)
    t = rf.type::AC.DictionaryType
    AC.typeequal(valuefield.type, t.valuetype) || throw(
        ArgumentError(
            "column $name pool maps to $(summary(valuefield.type)) but the retained " *
            "dictionary value type is $(summary(t.valuetype))",
        ),
    )
    IT = AC.juliatype(t.indextype)
    poollength - 1 <= typemax(IT) || throw(
        ArgumentError(
            "column $name pool of $poollength values exceeds the retained " *
            "$(summary(t.indextype)) index range",
        ),
    )
    field = AC.Field(
        name,
        t;
        nullable=rf.nullable,
        metadata=_fieldmetadata(rf),
        children=collect(AC.Field, valuefield.children),
    )
    return field, _dictbatch(field, firstidx, valuedata)
end

_withfieldmetadata(f::AC.Field, ::Nothing) = f
function _withfieldmetadata(f::AC.Field, metadata::Vector{Pair{String,String}})
    # Explicit application metadata augments retained/extension metadata.
    # A generated ArrowTypes label is authoritative for the values that were
    # lowered; accepting a conflicting explicit label would make them decode
    # as a different logical type.
    merged = _mergemetapairs(_fieldmetadata(f), metadata; protectextension=true)
    return AC.Field(
        f.name,
        f.type;
        nullable=f.nullable,
        metadata=merged,
        children=collect(AC.Field, f.children),
    )
end

# A column with a DataAPI ref pool (a PooledArray or CategoricalArray, say) is
# written dictionary-encoded, as Arrow 2.x did: its pool, in pool order and
# with unused entries, starts the Arrow dictionary. `refpools=false` writes
# such a column as its values instead (a later table's column whose schema
# field is not a dictionary).
_hasrefpool(x::AbstractVector) =
    DataAPI.refpool(x) !== nothing &&
    DataAPI.refarray(x) !== x &&
    Base.nonmissingtype(_eltypeof(x)) !== Union{}
_hasrefpool(::Any) = false

"""
The values and category pool of a ref-pool column. Pool values that are
wrappers without an ArrowTypes.jl mapping (CategoricalArrays' values) unwrap
through `DataAPI.unwrap`; a mapped element type keeps its values so its
ArrowTypes.jl lowering applies.
"""
function _refpoolvalues(x::AbstractVector)
    T = Base.nonmissingtype(_eltypeof(x))
    mapped = ArrowTypes.ArrowType(T) !== T || ArrowTypes.arrowname(T) !== Symbol()
    pool = DataAPI.refpool(x)
    unwrap =
        !mapped &&
        _uninferred(any, v -> v !== missing && DataAPI.unwrap(v) !== v, pool)::Bool
    values = unwrap ? [DataAPI.unwrap(v) for v in x] : x
    hint = Base.nonmissingtype(_eltypeof(values))[]
    # Iterate: CategoricalArrays' ref pool is indexed from zero.
    for v in pool
        v === missing || push!(hint, unwrap ? DataAPI.unwrap(v) : v)
    end
    return values, hint
end

"""
Construct one logical column of a record batch.

This is the sole column-policy seam used by the write facade. It owns fresh
inference, retained reconstruction, ArrowTypes lowering, dictionary pooling,
field metadata, and the check that strings are UTF-8 (`_InvalidUTF8`).

`retained` is the Field a rewrite must reproduce; `poolhint` carries the
retained dictionary-pool snapshot (or nothing), enforced below; `metadata` is
explicit application metadata merged onto the final Field.
"""
function _constructcolumn(name::Symbol, v::AbstractVector; kwargs...)
    try
        field, data = _constructcolumnparts(name, v; kwargs...)
        _checkutf8(name, field, data)
        return field, data
    catch err
        prefix = "fromjulia: unsupported element type "
        msg = err isa ArgumentError ? err.msg : nothing
        msg isa String && startswith(msg, prefix) || rethrow()
        T = _reflectedstructtype(v)
        T === nothing && throw(
            ArgumentError(
                "column $name holds $(chopprefix(msg::String, prefix)) values, which have " *
                "no Arrow type; convert them first, or define ArrowTypes.ArrowType and " *
                "ArrowTypes.toarrow for their type",
            ),
        )
        throw(
            ArgumentError(
                "column $name holds $T values, which have no ArrowTypes.jl mapping, so " *
                "Arrow writes their fields as a Struct; a field failed with " *
                "\"$msg\". Define ArrowTypes.ArrowType and ArrowTypes.toarrow " *
                "for $T, or convert the column first (DataAPI.unwrap.(column) for " *
                "wrapped values such as CategoricalValue)",
            ),
        )
    end
end

# The element type of a column written by reflecting a plain struct's fields
# (ArrowTypes.jl's default StructKind mapping), or nothing.
function _reflectedstructtype(v)
    T = Base.nonmissingtype(_eltypeof(v))
    isconcretetype(T) && isstructtype(T) && !(T <: Union{NamedTuple,Tuple}) ||
        return nothing
    ArrowTypes.ArrowType(T) === T && ArrowTypes.arrowname(T) === Symbol() || return nothing
    return ArrowTypes.ArrowKind(T) isa ArrowTypes.StructKind ? T : nothing
end

function _constructcolumnparts(
    name::Symbol,
    v::AbstractVector;
    retained::Union{Nothing,AC.Field}=nothing,
    poolhint=nothing,
    metadata::Union{Nothing,Vector{Pair{String,String}}}=nothing,
    refpools::Bool=true,
    stringview::Bool=false,
    largelists::Bool=false,
    growable::Bool=false,
    pool=nothing,
)
    if retained !== nothing && _isbool8(retained)
        # arrow.bool8 reads as Bool; its storage is Int8 0 or 1.
        _, int8data = _constructcolumnparts(
            name,
            _uninferred(_bool8storage, v);
            retained=_withoutownextension(retained),
            metadata,
        )
        return _withfieldmetadata(_retainedfield(retained), metadata), int8data
    end
    fieldname = String(name)
    dictintent =
        (retained !== nothing && retained.type isa AC.DictionaryType) ||
        v isa DictEncode ||
        (refpools && retained === nothing && _hasrefpool(v))
    # A dictionary's pool outlives its batch, so it never takes reused buffers.
    context =
        _WriterContext(fieldname; stringview, largelists, pool=dictintent ? nothing : pool)
    # A ref-pool column written as its values still unwraps them.
    !dictintent && _hasrefpool(v) && (v = first(_uninferred(_refpoolvalues, v)::Tuple))
    # Built by calls this function does not infer (see `_uninferred`).
    field, data =
        dictintent ?
        _uninferred(
            _dictcolumnparts,
            name,
            fieldname,
            v,
            poolhint,
            retained,
            growable,
            context,
        )::Tuple{AC.Field,AC.ArrayData} :
        _uninferred(
            _plaincolumnparts,
            name,
            fieldname,
            v,
            retained,
            context,
        )::Tuple{AC.Field,AC.ArrayData}
    return _withfieldmetadata(field, metadata), data
end

"A dictionary column of `_constructcolumnparts`: its field and data."
function _dictcolumnparts(name, fieldname, part, poolhint, retained, growable, context)
    values = part isa DictEncode ? (part::DictEncode).data : part
    if _hasrefpool(values)
        values, hint = _uninferred(_refpoolvalues, values)::Tuple
        poolhint === nothing && (poolhint = hint)
    end
    retainedvaluefield = nothing
    retainedtarget = nothing
    if retained !== nothing
        retainedvaluefield, retainedtarget = _registereddictionaryfield(context, retained)
        retainedtype = retained.type::AC.DictionaryType
        retainedvaluefield === nothing &&
            retainedtype.valuetype isa AC.NullType &&
            retained.nullable &&
            _arrowtypesextension(retained) !== nothing &&
            throw(
                ArgumentError(
                    "column $name has a nullable retained Dictionary<Null> with " *
                    "an unregistered extension; materialization cannot " *
                    "distinguish a valid null-pool index from an outer null index",
                ),
            )
        # Table materialization already lifts retained pool snapshots
        # through the dictionary value Field. They stay in the same
        # public domain as row values until the merged pool is lowered
        # exactly once through `_constructpart` on that value Field below.
    end

    # The hint's first categories (a pool already written or retained from an
    # Arrow source) are kept exactly; the categories after them dedupe.
    retainedprefix = poolhint === nothing ? 0 : _hintprefix(poolhint)
    poolhint === nothing || (poolhint = _hintvalues(poolhint))
    if retained !== nothing
        (retained.type::AC.DictionaryType).valuetype isa AC.UnionType &&
            retainedvaluefield === nothing &&
            throw(
                ArgumentError(
                    "column $name has a retained UnionType whose child type ids " *
                    "cannot be recovered from materialized facade values",
                ),
            )
        T = Base.nonmissingtype(_eltypeof(values))
        if retainedvaluefield === nothing
            publictype = _facadebasetype(retained.type)
            # Any string writes to a utf8 pool.
            publictype === String && (publictype = AbstractString)
            publictype !== Any &&
                !(T <: publictype) &&
                !_retainedwritertype((retained.type::AC.DictionaryType).valuetype, T) &&
                T !== Union{} &&
                throw(
                    ArgumentError(
                        "column $name holds $T values, but its retained " *
                        "dictionary materializes as $publictype; the column " *
                        "was replaced with incompatible data",
                    ),
                )
        else
            islogical =
                T <: retainedtarget || (
                    T === Any &&
                    all(x -> x === missing || x isa retainedtarget, values)
                )
            islogical || throw(
                ArgumentError(
                    "column $name holds $T values, but its retained " *
                    "dictionary materializes as $retainedtarget; the column " *
                    "was replaced with incompatible data",
                ),
            )
        end
        # A non-nullable dictionary can materialize missing through a
        # valid index into a null pool entry. The retained pool keeps
        # that distinct from a missing index.
        any(ismissing, values) &&
            !retained.nullable &&
            (poolhint === nothing || !any(ismissing, poolhint)) &&
            throw(
                ArgumentError(
                    "column $name may hold missing values but its retained " *
                    "dictionary field is non-nullable",
                ),
            )
        (retained.type::AC.DictionaryType).ordered &&
            poolhint === nothing &&
            throw(
                ArgumentError(
                    "column $name has an ordered retained dictionary, but " *
                    "its original category pool is unavailable",
                ),
            )
    end

    pool = _dictionarypool(values, poolhint)
    lookup = _dictionarylookup(pool)
    missingindex =
        retained !== nothing && !retained.nullable ? findfirst(ismissing, pool) : nothing
    missingindex === nothing || (missingindex = Int64(missingindex - 1))
    candidateindices = _dictionaryindices(values, lookup, missingindex)
    candidatevaluefield, candidatevaluedata = if retained === nothing
        _constructpart(fieldname, pool; context)
    else
        retainedtype = retained.type::AC.DictionaryType
        constructionfield =
            retainedvaluefield === nothing ? AC.dictvaluefield(retained, retainedtype) :
            retainedvaluefield
        _constructpart(constructionfield, pool; context)
    end
    valuefield, valuedata, remap = _compactdictionarypool(
        candidatevaluefield,
        candidatevaluedata,
        retainedprefix,
        context,
    )
    indices = _remapdictionaryindices(candidateindices, remap)
    retained === nothing && return _newdictfromdata(
        fieldname,
        valuefield,
        valuedata,
        indices;
        nullable=Missing <: _eltypeof(values),
        # The narrowest index type the categories need; a pool that later
        # tables may extend takes at least Int32, as pyarrow writes.
        indextype=_dictindextype(growable ? max(valuedata.len, 1 << 16) : valuedata.len),
        # ArrowCategoricalArraysExt adds an `_isordered` method.
        ordered=_uninferred(_isordered, part)::Bool,
    )
    return _retaineddictfromdata(
        retained,
        valuefield,
        valuedata,
        Int(valuedata.len),
        indices,
        fieldname,
    )
end

"A non-dictionary column of `_constructcolumnparts`: its field and data."
function _plaincolumnparts(name, fieldname, part, retained, context)
    narrowed =
        retained === nothing ? _narrowabstractwriterparts(fieldname, part, context) : nothing
    try
        # A successful narrowing pass already saw every value; replanning
        # from part of them can split one declared nullable Tuple child into
        # incompatible Int and Null Fields.
        retained === nothing || return _constructpart(retained, part; context)
        return _constructpart(
            fieldname,
            something(narrowed, part);
            context,
            narrowabstract=(narrowed === nothing),
        )
    catch err
        (err isa _OffsetOverflow && err.part == 0) || rethrow()
        e = err::_OffsetOverflow
        # The overflow row is a row of this column only at its top level.
        throw(_OffsetOverflow(e.column, 1, e.column == fieldname ? e.row : 0))
    end
end
