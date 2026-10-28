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
    T = Base.nonmissingtype(eltype(v))
    # With TimeZones loaded, a fresh ZonedDateTime column writes as a
    # timezone-declared millisecond timestamp (ext/ArrowTimeZonesExt.jl)
    # instead of reflecting the zone's whole transition table as a struct.
    zoned = _zonednativepart(name, v, T)
    zoned === nothing || return zoned
    if eltype(v) === Missing
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
        return _constructsharedpart(name, v, T)
    elseif T <: Durations.Timestamp
        isconcretetype(T) || throw(
            ArgumentError(
                "column $(_errname(context, name)) has abstract Timestamp element " *
                "type $T; give the column one concrete resolution",
            ),
        )
        return _constructtemporalpart(
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
        return _constructtemporalpart(
            name,
            v,
            AC.TimestampType(_instantunit(T), Durations.zonename(T)),
            x -> Int64(Dates.value(x)),
        )
    elseif T <: Dates.Date
        return _constructtemporalpart(
            name,
            v,
            AC.DateType(AC.DAY),
            x -> Int32(Dates.value(x) - _EPOCH_DAYS),
        )
    elseif T <: Dates.DateTime
        return _constructtemporalpart(
            name,
            v,
            AC.TimestampType(AC.MILLISECOND, nothing),
            x -> Int64(Dates.value(x) - Dates.UNIXEPOCH),
        )
    elseif T <: Dates.Time
        return _constructtemporalpart(
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
        return _constructtemporalpart(
            name,
            v,
            AC.DurationType(unit),
            x -> Int64(Dates.value(x)),
        )
    elseif T <: Union{Dates.Week,Dates.Day,Dates.Hour,Dates.Minute}
        return _constructtemporalpart(name, v, AC.DurationType(AC.SECOND), _periodseconds)
    elseif T <: Union{Dates.Month,Dates.Year}
        return _constructtemporalpart(
            name,
            v,
            AC.IntervalType(AC.YEAR_MONTH),
            _periodmonths,
        )
    elseif T <: NamedTuple
        any(ismissing, v) && throw(
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
        # Children build like top-level columns, so public leaf values lower
        # through the facade. Preserve the declared child types: a
        # value-narrowing comprehension turns an empty child into `Any[]` and
        # an all-missing nullable child into `Missing[]`, so neither can
        # recover its Arrow descriptor.
        context === nothing && (context = _WriterContext())
        children = [
            _constructpart(
                String(fieldname(T, i)),
                collect(fieldtype(T, i), (getfield(x, i) for x in v));
                context,
            ) for i = 1:fieldcount(T)
        ]
        t = AC.StructType()
        return AC.Field(
            name,
            t;
            nullable=false,
            children=AC.Field[first(c) for c in children],
        ),
        AC.ArrayData(
            t,
            length(v),
            [AC.BufferSlice()];
            children=AC.ArrayData[last(c) for c in children],
            nullcount=0,
        )
    elseif T <: AbstractString
        _stringview(context) && return _retainedview(
            AC.Field(name, AC.ViewType(true); nullable=Missing <: eltype(v)),
            v,
        )
        return _varbytespart(name, v, AC.Utf8Type(_largelists(context)))
    elseif T <: AbstractVector{UInt8}
        _stringview(context) && return _retainedview(
            AC.Field(name, AC.ViewType(false); nullable=Missing <: eltype(v)),
            v,
        )
        return _varbytespart(name, v, AC.BinaryType(_largelists(context)))
    elseif T === Any || (T <: AbstractVector && eltype(T) === Any)
        # Materialized facade columns are Any-eltype for composite layouts;
        # one narrowing pass recovers list columns (inner vectors narrow
        # element-wise, empties adopt the joined element type).
        w = _narrowlists(v, _errname(context, name))
        NW = Base.nonmissingtype(eltype(w))
        (NW === Any || (NW <: AbstractVector && eltype(NW) === Any)) && throw(
            ArgumentError(
                "column $(_errname(context, name)) has element type Any and cannot " *
                "be narrowed to a writable Arrow column; give it a concrete element type",
            ),
        )
        return _constructpart(name, w; context)
    elseif T <: AbstractVector
        # The facade list builder builds the child like a column, so public
        # leaf values lower at every depth.
        context === nothing && (context = _WriterContext())
        return _arrowtypeslistcolumn(name, v, T; extension_shape=false, context)
    elseif T === Bool
        return _boolpart(name, v)
    elseif T <: _FixedPrimitive
        t = AC.arrowtype_for(T)
        if eltype(v) === T
            data = _fixedbuffer(v)
            data === nothing && return AC.fromjulia(name, _plainvector(v))
            return AC.Field(name, t; nullable=false),
            AC.ArrayData(t, length(v), [AC.BufferSlice(), data]; nullcount=0)
        end
        vals, validity, nc = _nullablevalues(T, v, zero(T))
        return AC.Field(name, t; nullable=true),
        AC.ArrayData(t, length(v), [validity, AC._databuffer(vals)]; nullcount=nc)
    else
        return AC.fromjulia(name, _plainvector(v))
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
    return AC.Field(name, t; nullable=Missing <: eltype(v)),
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
    if eltype(v) === Any
        narrowed = _narrowlists(v, _errname(context, name))
        eltype(narrowed) === Any && throw(
            ArgumentError(
                "column $(_errname(context, name)) has element type Any and cannot " *
                "be narrowed to a writable Arrow column; give it a concrete element type",
            ),
        )
        return _constructpart(name, narrowed; context)
    end
    nested = _nestedpart(name, v, context)
    nested === nothing || return nested
    if narrowabstract
        runtimeparts = _narrowabstractwriterparts(name, AbstractVector[v], context)
        if runtimeparts !== nothing
            narrowed = only(runtimeparts)
            # Julia can canonicalize a Union of concrete Tuple runtime types
            # back to the original abstract declaration, so eltype may be
            # unchanged. One narrowing pass is enough; recursing on the same
            # declaration would loop.
            eltype(narrowed) === eltype(v) ||
                return _constructpart(name, narrowed; context, narrowabstract=false)
        end
    end
    return _arrowtypes_needs(v, context) ? _constructarrowtypespart(name, v; context) :
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
    kind = _nestedkind(context, Base.nonmissingtype(eltype(v)))
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
_storedlist(@nospecialize(v), E) = v isa ListVector && eltype(v.values) === E
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

function _listpart(name::String, @nospecialize(v::AbstractVector), context)
    large = _largelists(context)
    offsets, validity, nc = _listoffsets(name, v, large ? Int64 : Int32, _pool(context))
    E = eltype(Base.nonmissingtype(eltype(v)))
    errname = _errname(context, name)
    values =
        _storedlist(v, E) ? _storedlistvalues(errname, v, offsets) :
        _listvalues(errname, E, v, offsets)
    cf, cd = _constructpart("item", values; context)
    t = AC.ListType(large)
    d = AC.ArrayData(
        t,
        length(v),
        [validity, AC._databuffer(offsets)];
        children=AC.ArrayData[cd],
        nullcount=nc,
    )
    return AC.Field(name, t; nullable=Missing <: eltype(v), children=AC.Field[cf]), d
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
    (_nestedkind(context, T) !== nothing || !_writerneedstype!(context, T))

"A nullable child built for hidden slots, declared non-nullable."
function _hidevalidity(f::AC.Field, d::AC.ArrayData)
    AC.layoutspec_of(d.type).buffers[1] == AC.VALIDITY ||
        throw(ArgumentError("internal error: column $(f.name) has no validity to hide"))
    buffers = collect(AC.BufferSlice, d.buffers)
    buffers[1] = AC.BufferSlice()
    field = AC.Field(
        f.name,
        f.type;
        nullable=false,
        metadata=_fieldmetadata(f),
        children=collect(AC.Field, f.children),
    )
    data = AC.ArrayData(
        d.type,
        d.len,
        buffers;
        offset=d.offset,
        children=collect(AC.ArrayData, d.children),
        dictionary=d.dictionary,
        owner=d.owner,
        nullcount=0,
    )
    return field, data
end

"""
One child column of rows that may be missing: `V{T}` (or `V{Union{Missing,T}}`)
over `v` gives its values. A child that cannot be null is built nullable over
the missing rows and its validity hidden.
"""
function _rowchild(V::Type, v, name::String, T::Type, hidden::Bool, context)
    hidden && !(Missing <: T) || return _constructpart(name, V{T}(v); context)
    return _hidevalidity(_constructpart(name, V{Union{Missing,T}}(v); context)...)
end

function _structpart(name::String, v::AbstractVector, context)
    NT = Base.nonmissingtype(eltype(v))
    present = Bool[x !== missing for x in v]
    nc = count(!, present)
    hidden = nc > 0
    hidden &&
        !all(T -> Missing <: T || _hideable(context, T), fieldtypes(NT)) &&
        return nothing
    fields = AC.Field[]
    children = AC.ArrayData[]
    for k = 1:fieldcount(NT)
        V = _ChildValues{C,k} where {C}
        f, d = _rowchild(V, v, String(fieldname(NT, k)), fieldtype(NT, k), hidden, context)
        push!(fields, f)
        push!(children, d)
    end
    t = AC.StructType()
    d = AC.ArrayData(
        t,
        length(v),
        [nc == 0 ? AC.BufferSlice() : AC._bitmapbuffer(present)];
        children,
        nullcount=nc,
    )
    return AC.Field(name, t; nullable=Missing <: eltype(v), children=fields), d
end

# A struct column read from Arrow writes its children as stored, slots under
# null rows included, so no child needs hiding.
function _structpart(name::String, @nospecialize(v::StructVector), context)
    NT = Base.nonmissingtype(eltype(v))
    n = length(v)
    validity, nc = _storedvalidity(v.validity, v.bitoffset, n, _pool(context))
    fields = AC.Field[]
    children = AC.ArrayData[]
    for k = 1:fieldcount(NT)
        child = _childslice(v.children[k], v.offset, v.offset + n)
        f, d = _constructpart(String(fieldname(NT, k)), child; context)
        push!(fields, f)
        push!(children, d)
    end
    t = AC.StructType()
    d = AC.ArrayData(t, n, [validity]; children, nullcount=nc)
    return AC.Field(name, t; nullable=Missing <: eltype(v), children=fields), d
end

function _fixedlistpart(name::String, v::AbstractVector, context)
    NT = Base.nonmissingtype(eltype(v))
    N = fieldcount(NT)
    E = fieldtype(NT, 1)
    present = Bool[x !== missing for x in v]
    nc = count(!, present)
    hidden = nc > 0
    hidden && !(Missing <: E || _hideable(context, E)) && return nothing
    cf, cd = _rowchild(_TupleValues{C,N} where {C}, v, "item", E, hidden, context)
    t = AC.FixedSizeListType(N)
    d = AC.ArrayData(
        t,
        length(v),
        [nc == 0 ? AC.BufferSlice() : AC._bitmapbuffer(present)];
        children=AC.ArrayData[cd],
        nullcount=nc,
    )
    return AC.Field(name, t; nullable=Missing <: eltype(v), children=AC.Field[cf]), d
end

_maptypes(::Type{T}) where {T<:AbstractDict} = (keytype(T), valtype(T))
_maptypes(::Type{T}) where {T<:AbstractVector} =
    eltype(T) isa DataType ? fieldtypes(eltype(T)) : (Any, Any)

"The entries of `v`'s map rows back to back, as `_listoffsets` counted them."
function _mapentries(name::String, ::Type{P}, v::AbstractVector, offsets) where {P}
    R = Base.nonmissingtype(eltype(v))
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
            entries[pos += 1] = P(first(p), last(p))
        end
    end
    return entries
end

function _mappart(name::String, v::AbstractVector, context)
    R = Base.nonmissingtype(eltype(v))
    K, V = _maptypes(R)
    offsets, validity, nc = _listoffsets(name, v, Int32, _pool(context))
    entries = _mapentries(_errname(context, name), Pair{K,V}, v, offsets)
    KK = Base.nonmissingtype(K)
    KK === K ||
        all(p -> first(p) !== missing, entries) ||
        throw(
            ArgumentError(
                "column $(_errname(context, name)) has a missing map key; Arrow map " *
                "keys cannot be null, so drop those entries or replace the key",
            ),
        )
    kf, kd = _constructpart("key", _ChildValues{KK,1}(entries); context)
    vf, vd = _constructpart("value", _ChildValues{V,2}(entries); context)
    kf.nullable && throw(
        ArgumentError(
            "column $(_errname(context, name)) has map keys of type $KK, which " *
            "write as a nullable Arrow type; Arrow map keys cannot be null",
        ),
    )
    # Distinct Dict keys that an ArrowTypes mapping stores alike would read
    # back as repeated keys of one row.
    R <: AbstractDict &&
        _writerneedstype!(context, KK) &&
        _validatemapphysicalkeys(_errname(context, name), kf, kd, offsets)
    return _mapcolumn(name, v, offsets, validity, nc, length(entries), kf, kd, vf, vd)
end

# A map column read from Arrow writes its offsets, keys, and values as stored.
function _mappart(name::String, @nospecialize(v::MapVector), context)
    offsets, validity, nc = _listoffsets(name, v, Int32, _pool(context))
    lo, hi = GC.@preserve v Int(v.offsets[1]), Int(v.offsets[end])
    kf, kd = _constructpart("key", _childslice(v.keys, lo, hi); context)
    vf, vd = _constructpart("value", _childslice(v.values, lo, hi); context)
    kf.nullable && throw(
        ArgumentError(
            "column $(_errname(context, name)) has map keys of type " *
            "$(eltype(v.keys)), which write as a nullable Arrow type; Arrow map " *
            "keys cannot be null",
        ),
    )
    return _mapcolumn(name, v, offsets, validity, nc, hi - lo, kf, kd, vf, vd)
end

function _mapcolumn(name, @nospecialize(v), offsets, validity, nc, nentries, kf, kd, vf, vd)
    entriestype = AC.StructType()
    ef = AC.Field("entries", entriestype; nullable=false, children=AC.Field[kf, vf])
    ed = AC.ArrayData(
        entriestype,
        nentries,
        [AC.BufferSlice()];
        children=AC.ArrayData[kd, vd],
        nullcount=0,
    )
    t = AC.MapType(false)
    d = AC.ArrayData(
        t,
        length(v),
        [validity, AC._databuffer(offsets)];
        children=AC.ArrayData[ed],
        nullcount=nc,
    )
    return AC.Field(name, t; nullable=Missing <: eltype(v), children=AC.Field[ef]), d
end

"""
A retained Union column built from public values, or nothing when some value
fits none of the union's children or several. A value goes to the one child
whose layout a column of its type would write as (or to a union child that
takes it); a missing value to the first Null child, else the first nullable
one. Typed Union rows (as `Arrow.Table` reads them) route; children of one
layout cannot be told apart and do not. Each child then builds from its values
as a composite's child does, so values that lower to storage (dates, extension
types) lower there.
"""
function _constructfacadeunion(f::AC.Field, v::AbstractVector, context)
    t = f.type::AC.UnionType
    children = f.children
    n = length(v)
    length(t.typeids) == length(children) && n <= typemax(Int32) || return nothing
    routes = IdDict{Type,Int}()
    route = Vector{Int}(undef, n)
    for (i, x) in enumerate(v)
        if x === _RETAINED_HIDDEN
            # A slot hidden by a null parent; any child may hold it.
            route[i] = 0
            continue
        end
        k = if x === missing
            something(
                findfirst(c -> c.type isa AC.NullType, children),
                findfirst(c -> c.nullable, children),
                0,
            )
        else
            get!(routes, typeof(x)) do
                probe = first(_constructpart("probe", typeof(x)[x]; context))
                matches = findall(c -> _unionfits(c, probe), children)
                length(matches) == 1 ? only(matches) : 0
            end
        end
        k == 0 && return nothing
        route[i] = k
    end
    sparse = t.mode == AC.SparseMode
    hiddenchild =
        any(iszero, route) ? _writerhiddenunionchild(f; forcevalid=false, inactive=true) : 0
    typeids = Vector{Int8}(undef, n)
    offsets = sparse ? nothing : Vector{Int32}(undef, n)
    childvalues = [Any[] for _ in children]
    for (i, x) in enumerate(v)
        k = route[i] == 0 ? hiddenchild : route[i]
        typeids[i] = t.typeids[k]
        if sparse
            for j in eachindex(children)
                push!(childvalues[j], j == k ? x : _RETAINED_HIDDEN)
            end
        else
            offsets[i] = length(childvalues[k])
            push!(childvalues[k], x)
        end
    end
    fields = AC.Field[]
    datas = AC.ArrayData[]
    for (c, vals) in zip(children, childvalues)
        # Narrowed values take the typed builders; hidden slots stay boxed.
        any(x -> x === _RETAINED_HIDDEN, vals) ||
            isempty(vals) ||
            (vals = map(identity, vals))
        cf, cd = _retainedchildcolumn(c, vals, context)
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

# A child takes a value whose fresh field it matches, or, when the child is a
# union itself, that one of its children takes.
_unionfits(c::AC.Field, probe::AC.Field) =
    _asretained(c, probe) !== nothing ||
    c.type isa AC.UnionType && any(cc -> _unionfits(cc, probe), c.children)

# Rows with `Any` element or field types (the retained builders' own
# intermediate rows among them) rebuild under the retained field.
_untypedrow(R::Type) =
    R <: Union{NamedTuple,Tuple} ? any(T -> T === Any, fieldtypes(R)) :
    R <: AbstractDict ? (keytype(R) === Any || valtype(R) === Any) :
    eltype(R) === Any ||
    (eltype(R) <: Pair && eltype(R) isa DataType && Any in fieldtypes(eltype(R)))

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
    hasmissing = false
    for x in v
        if x === missing
            hasmissing = true
            continue
        end
        T = typeof(x)
        any(S -> S === T, types) && continue
        length(types) < _MAX_INFERRED_WRITER_TYPES || throw(
            ArgumentError(
                (isempty(name) ? "a column" : "column $name") *
                " holds values of " *
                "more than $_MAX_INFERRED_WRITER_TYPES types ($(join(types, ", ")), " *
                "$T, ...); give it a concrete element type",
            ),
        )
        push!(types, T)
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
    NT = Base.nonmissingtype(eltype(w))
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
    hasm = eltype(w) >: Missing
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
    # Fixed-size binary rows (`NTuple{N,UInt8}`) have no native construction;
    # the general path writes them.
    if d isa AC.ArrayData &&
       d.offset == 0 &&
       d.len == length(v) &&
       !(Base.nonmissingtype(eltype(v)) <: Tuple)
        f, _ = _constructnativepart(name, eltype(v)[]; context)
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
    nullable = Missing <: eltype(v)
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
function _checkutf8(name::Symbol, f::AC.Field, data::Vector{AC.ArrayData})
    for (p, d) in enumerate(data)
        bad = _badutf8(f, d)
        bad === nothing && continue
        row, path = bad
        column = isempty(path) ? String(name) : "$name (child $path)"
        throw(_InvalidUTF8(column, p, row))
    end
    return nothing
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
        i = _badutf8offsets(d, t.large)
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
# other data is checked row by row.
function _badutf8offsets(d::AC.ArrayData, large::Bool)
    n = Int(d.len)
    n == 0 && return 0
    offs = AC.rolebuffer(d, AC.OFFSETS)
    data = AC.rolebuffer(d, AC.DATA)
    T = large ? Int64 : Int32
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
            m = _nbytes(x)
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
    return AC.Field(name, t; nullable=Missing <: eltype(v)), _constructedvalid(d)
end

# Offsets built here never decrease and end at the data length, and the null
# count is exact: the data passes Core's semantic checks by construction.
function _constructedvalid(d::AC.ArrayData)
    @atomic :monotonic d.semachecked = true
    return d
end

"Concrete Vector with an exact Union{Missing,T} or T eltype for fromjulia."
function _plainvector(v::AbstractVector)
    T = eltype(v)
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
    eltype(v) >: Missing ? Union{Missing,S}[x === missing ? missing : S(x) for x in v] :
    S[S(x) for x in v]

"Construct temporal ArrayData from nullable Int64 storage values."
function _temporaldata(t::AC.ArrowType, storage::AbstractVector)
    _, d0 = AC.fromjulia("storage", storage)
    # Narrow to Int32 for any 32-bit temporal descriptor (Date32 fresh,
    # Time32 retained); every other temporal layout keeps the inferred Int64
    # buffer.
    buffers = d0.buffers
    if AC.primwidth(t) == 4
        narrow = Vector{Int32}(undef, length(storage))
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
    return AC.Field(name, t; nullable=eltype(v) >: Missing), d
end

# ArrowTypes write adapter. ArrowCore intentionally stays conversion-free;
# this layer lowers logical Julia values before it asks Core to build buffers.

"Stable metadata merge; `extra` wins except for protected extension labels."
function _mergemetapairs(base, extra; protectextension::Bool=false)
    out = Pair{String,String}[]
    base === nothing || append!(out, String(first(kv)) => String(last(kv)) for kv in base)
    extra === nothing && return isempty(out) ? nothing : out
    for kv in extra
        key = String(first(kv))
        value = String(last(kv))
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
    T = eltype(v)
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

const _WriterTypeRoutes = IdDict{Type,Int}

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
            join((string(S) for S in (path..., T)), " -> "),
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

function _writerroute(routes::_WriterTypeRoutes, runtime::Type)
    haskey(routes, runtime) && return routes[runtime]
    route = -1
    for (declared, candidate) in routes
        runtime <: declared || continue
        (route == -1 || route == candidate) || throw(
            ArgumentError(
                "writer type $runtime matches conflicting retained-field routes " *
                "$route and $candidate",
            ),
        )
        route = candidate
    end
    route >= 0 || throw(ArgumentError("writer type $runtime has no retained Field route"))
    routes[runtime] = route
    return route
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
                "column $column: $(sprint(showerror, err))",
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
) where {T}
    out = Vector{T}(undef, length(v))
    for (i, value) in enumerate(v)
        lowered = _writertoarrow(context, value, writertype, name)
        out[i] = _writerconvertlowered(T, lowered, context, writertype, name)
    end
    return out
end

function _arrowtypesmappedcolumn(name::String, v::AbstractVector, declared, context)
    S = eltype(v)
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
        runtime = typeof(mapped)
        runtime in seen && continue
        _writerregisterstorageinference!(context, name, runtime)
        push!(seen, runtime)
        push!(observed, runtime)
    end
    if isempty(observed)
        # A concrete logical declaration keeps ArrowTypes.ToArrow's
        # all-missing fallback type. Abstract inference-only storage has no
        # shape evidence, so the column is written as Arrow Null (what
        # ToArrow returns: Missing[]).
        fallbacktype =
            _arrowtypesconcreteorunion(S) ? eltype(ArrowTypes.ToArrow(v)) : Missing
        return _writerconvertedcolumn(fallbacktype, lowered, context, logical, name)
    end
    resulttype = reduce(_writerpromoteunion, observed)
    sawmissing && (resulttype = Union{Missing,resulttype})
    return _writerconvertedcolumn(resulttype, lowered, context, logical, name)
end

function _constructarrowtypespart(
    name::String,
    v::AbstractVector;
    extension_shape::Bool=false,
    context=nothing,
)
    logical = Base.nonmissingtype(eltype(v))
    # `Base.nonmissingtype(Missing) === Union{}`. Handle a pure-null child
    # before trait dispatch: bottom is a subtype of every container type and
    # would otherwise enter an unrelated ArrowKind branch.
    eltype(v) === Missing && return _constructnativepart(name, v; context)
    logical === Union{} && return _constructnativepart(name, v; context)
    context === nothing && (context = _WriterContext())
    return _withwriterschema(context, logical) do
        _constructarrowtypespart_impl(name, v, logical; extension_shape, context)
    end
end

function _constructarrowtypespart_impl(
    name::String,
    v::AbstractVector,
    logical;
    extension_shape::Bool,
    context,
)
    # VersionNumber's legacy reflected representation contains variable-length
    # tuples, so Arrow 3.0 writes String extension storage instead. ArrowTypes'
    # one-argument JuliaType registration and the VersionNumber(String)
    # constructor read it back. Arrow 2.x readers cannot: they index the
    # column with the Struct layout ArrowTypes declares for VersionNumber.
    if logical === VersionNumber
        mapped =
            eltype(v) >: Missing ?
            Union{Missing,String}[x === missing ? missing : string(x) for x in v] :
            String[string(x) for x in v]
        f, d = _constructnativepart(name, mapped; context)
        return _arrowtypeslogicalfield(context, f, logical), d
    end
    declared = _writerstoragetype!(context, logical)
    if Missing <: eltype(v) &&
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
    if Missing <: eltype(v) && declared isa Union && Missing <: declared
        throw(
            ArgumentError(
                "ArrowTypes logical type $logical uses Missing as a storage Union " *
                "branch, so a nullable logical column cannot distinguish that " *
                "storage value from an outer missing value",
            ),
        )
    end
    if logical isa Union && declared === logical
        # A declared Julia Union owns one Arrow child per declared member.
        # Build those members independently so whole-column promotion cannot
        # erase abstract-but-writable branches or same-storage logical types.
        f, d = _arrowtypesunioncolumn(name, v; extension_shape, context)
        return _arrowtypeslogicalfield(context, f, logical), d
    end
    # Abstract storage must be inferred from observed values. A recursively
    # materializable declaration remains schema authority for empty columns,
    # including every declared Union branch nested in a composite.
    mapped = _arrowtypesmappedcolumn(name, v, declared, context)
    ownshape = _writerextensionshape(context, logical, declared)
    f, d = if declared isa Union && Missing <: declared
        # Missing is a VALUE in this logical type's storage Union. Keep an
        # explicit Null child; scalar null validity would erase the difference
        # between that value and an outer nullable logical slot.
        _arrowtypesunioncolumn(
            name,
            mapped;
            extension_shape=extension_shape || ownshape,
            context,
        )
    else
        _arrowtypesstoragecolumn(
            name,
            mapped;
            extension_shape=extension_shape || ownshape,
            context,
        )
    end
    # A logical type stored as Arrow Null is not nullable unless the column
    # itself admits missing; otherwise the label's own values would read back
    # as missing.
    nullable = f.type isa AC.NullType && !(Missing <: eltype(v)) ? false : f.nullable
    return _arrowtypeslogicalfield(context, f, logical; nullable=nullable), d
end

function _arrowtypesstoragecolumn(
    name::String,
    v::AbstractVector;
    extension_shape::Bool,
    context,
)
    eltype(v) === Missing && return _constructnativepart(name, v; context)
    S = Base.nonmissingtype(eltype(v))
    S === Union{} && return _constructnativepart(name, v; context)
    if S isa Union
        return _arrowtypesunioncolumn(name, v; extension_shape, context)
    end
    _arrowtypesnativetype(S) && return _constructnativepart(name, v; context)
    S <: NamedTuple && return _arrowtypesstructcolumn(name, v, S; extension_shape, context)
    kind = _writerkind!(context, S)
    if kind isa ArrowTypes.NullKind
        nulls = S === Nothing ? fill(missing, length(v)) : v
        return _constructnativepart(name, nulls; context)
    end
    if kind isa ArrowTypes.FixedSizeListKind
        return _arrowtypesfixedlistcolumn(name, v, kind; extension_shape, context)
    elseif kind isa ArrowTypes.MapKind
        return _arrowtypesmapcolumn(name, v, S; extension_shape, context)
    elseif kind isa ArrowTypes.ListKind
        if S <: Union{AbstractString,AbstractVector{UInt8}}
            return _constructnativepart(name, v; context)
        end
        return _arrowtypeslistcolumn(name, v, S; extension_shape, context)
    elseif kind isa ArrowTypes.StructKind
        return _arrowtypesstructcolumn(name, v, S; extension_shape, context)
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

function _mergewritertypes(owner::String, left, right, limit::Int, kind::String)
    types = Type[]
    seen = Base.IdSet{Type}()
    for source in (left, right), T in source
        T in seen && continue
        length(types) < limit || throw(ArgumentError("$owner has more than $limit $kind"))
        push!(seen, T)
        push!(types, T)
    end
    return types
end

function _writeruniontype(types)
    uniontype = Union{}
    for T in types
        uniontype = Union{uniontype,T}
    end
    return uniontype
end

"Construct a fresh dense Arrow Union from one concrete Julia Union element type."
function _arrowtypesunioncolumn(
    name::String,
    v::AbstractVector;
    extension_shape::Bool,
    context,
)
    variants = _checkedwritervariants("Arrow Union column $name", eltype(v))

    childvalues = Any[T[] for T in variants]
    typeids = Vector{Int8}(undef, length(v))
    offsets = Vector{Int32}(undef, length(v))
    for (i, x) in enumerate(v)
        pos = findfirst(T -> x isa T, variants)
        pos === nothing && throw(
            ArgumentError(
                "Arrow Union column $name contains $(typeof(x)), which is not in " *
                "its declared element type $(eltype(v))",
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
        field, data = _arrowtypeschildcolumn(
            string(i - 1),
            childvalues[i];
            extension_shape=extension_shape,
            union_member=true,
            context,
        )
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

"Construct one child; a leaf builds like a column, so public leaf values lower."
function _arrowtypeschildcolumn(
    name::String,
    v::AbstractVector;
    extension_shape::Bool,
    union_member::Bool=false,
    context,
)
    T = Base.nonmissingtype(eltype(v))
    union_member &&
        T === AbstractString &&
        return _constructnativepart(name, _missings_to(String, v); context)
    if _arrowtypes_needs(v, context) || (
        T !== Any && (
            T <: NamedTuple ||
            T <: Tuple ||
            T <: AbstractDict ||
            (T <: AbstractVector && !(T <: AbstractString))
        )
    )
        return _constructarrowtypespart(name, v; extension_shape, context)
    end
    return _constructnativepart(name, v; context)
end

function _arrowtypeslistcolumn(
    name::String,
    v::AbstractVector,
    S;
    extension_shape::Bool,
    context,
)
    S <: AbstractVector || throw(
        ArgumentError(
            "column $(_errname(context, name)) holds $S values, which are not " *
            "vectors; an Arrow list holds a vector per row (vec(x) flattens an array)",
        ),
    )
    E = eltype(S)
    flat = E[]
    present = Bool[x !== missing for x in v]
    large = _largelists(context)
    Offset = large ? Int64 : Int32
    offsets = Vector{Offset}(undef, length(v) + 1)
    offsets[1] = 0
    for (i, row) in enumerate(v)
        if row !== missing
            row isa AbstractVector || throw(
                ArgumentError("ArrowTypes ListKind column $name contains a non-vector row"),
            )
            length(flat) <= typemax(Offset) - length(row) ||
                throw(_OffsetOverflow(name, 0, i))
            append!(flat, row)
        end
        offsets[i + 1] = Offset(length(flat))
    end
    childfield, childdata = _arrowtypeschildcolumn("item", flat; extension_shape, context)
    t = AC.ListType(large)
    field = AC.Field(name, t; nullable=Missing <: eltype(v), children=AC.Field[childfield])
    data = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), AC._databuffer(offsets)];
        children=AC.ArrayData[childdata],
        nullcount=count(!, present),
    )
    return field, data
end

struct _RetainedHidden end
const _RETAINED_HIDDEN = _RetainedHidden()

abstract type _MaskedChildValues{T} <: AbstractVector{T} end

"""
Lazy view of a fixed-size-list column's child slots; slots under a missing or
hidden row read as _RETAINED_HIDDEN.
"""
struct _MaskedFixedListValues{V<:AbstractVector} <: _MaskedChildValues{Any}
    rows::V
    width::Int
    len::Int
end

function _MaskedFixedListValues(rows::AbstractVector, width::Int)
    width >= 0 || throw(ArgumentError("retained fixed-size-list width is negative"))
    len64 = try
        Base.checked_mul(Int64(length(rows)), Int64(width))
    catch err
        err isa OverflowError || rethrow()
        throw(ArgumentError("retained fixed-size-list child length is out of range"))
    end
    len64 <= typemax(Int) ||
        throw(ArgumentError("retained fixed-size-list child length is out of range"))
    return _MaskedFixedListValues(rows, width, Int(len64))
end

Base.IndexStyle(::Type{<:_MaskedFixedListValues}) = IndexLinear()
Base.size(values::_MaskedFixedListValues) = (values.len,)
function Base.getindex(values::_MaskedFixedListValues, index::Int)
    @boundscheck checkbounds(values, index)
    rowoffset, childoffset = divrem(index - 1, values.width)
    row = values.rows[firstindex(values.rows) + rowoffset]
    row === missing && return _RETAINED_HIDDEN
    row === _RETAINED_HIDDEN && return _RETAINED_HIDDEN
    return row[firstindex(row) + childoffset]
end

struct _FreshStructChildRows end
struct _RetainedStructChildRows end

"Lazy values for one Struct child, including slots hidden by parent validity."
struct _MaskedStructChildValues{T,V<:AbstractVector,Mode} <: _MaskedChildValues{T}
    rows::V
    child::Int
end

function _freshstructchildvalues(::Type{T}, rows::AbstractVector, child::Int) where {T}
    return _MaskedStructChildValues{T,typeof(rows),_FreshStructChildRows}(rows, child)
end

function _retainedstructchildvalues(rows::AbstractVector, child::Int)
    return _MaskedStructChildValues{Any,typeof(rows),_RetainedStructChildRows}(rows, child)
end

Base.IndexStyle(::Type{<:_MaskedStructChildValues}) = IndexLinear()
Base.size(values::_MaskedStructChildValues) = size(values.rows)

function Base.getindex(
    values::_MaskedStructChildValues{T,V,_FreshStructChildRows},
    index::Int,
) where {T,V}
    @boundscheck checkbounds(values, index)
    row = values.rows[firstindex(values.rows) + index - 1]
    row === missing && return _RETAINED_HIDDEN
    row === _RETAINED_HIDDEN && return _RETAINED_HIDDEN
    return getfield(row, values.child)
end

function Base.getindex(
    values::_MaskedStructChildValues{T,V,_RetainedStructChildRows},
    index::Int,
) where {T,V}
    @boundscheck checkbounds(values, index)
    row = values.rows[firstindex(values.rows) + index - 1]
    row === missing && return _RETAINED_HIDDEN
    row === _RETAINED_HIDDEN && return _RETAINED_HIDDEN
    return _retainedchildvalue(row, values.child)
end

# A typed read's struct row is a `NamedTuple`; a materialized one is an
# ordered `Pair` vector. These values are declared `Any`, and the field comes
# back through an inference barrier: inferring its exact type makes Julia
# 1.10 with code coverage fail in `any`/`all` over the values ("fatal error
# in type inference (type bound)").
_retainedchildvalue(row::NamedTuple, child::Int) =
    Base.inferencebarrier(getfield(row, child))
_retainedchildvalue(row, child::Int) = last(row[firstindex(row) + child - 1])

"A writer-side Union route retained until dense or sparse child construction."
struct _WriterRoutedUnion{T}
    child::Int
    value::T
    writertype::Type
end
_WriterRoutedUnion(child::Int, value) = _WriterRoutedUnion(child, value, typeof(value))

struct _RegisteredWriterPlan
    input::Type
    routes::_WriterTypeRoutes
end

struct _DeferredRegisteredWriter{T}
    field::AC.Field
    value::T
    writetype::Type
end

const _WriterUnionPlanCache =
    IdDict{AC.Field,Dict{Tuple{Type,Bool},Tuple{Vector{Type},Vector{Int}}}}

mutable struct _WriterContext
    unions::_WriterUnionPlanCache
    registered::IdDict{AC.Field,Dict{Tuple{Type,Type},_RegisteredWriterPlan}}
    arrowtypes::_ArrowTypesContext
    needs::IdDict{Type,Bool}
    kinds::IdDict{Type,Any}
    extensions::IdDict{Type,Any}
    extensionshapes::IdDict{Type,Bool}
    unionvariants::IdDict{Type,Vector{Type}}
    unionbranches::Dict{Tuple{Type,Type},Int}
    candidates::Dict{Tuple{Type,Bool},AC.Field}
    inferred::Base.IdSet{Type}
    physical::IdDict{AC.Field,Any}
    schema::Vector{Type}
    values::Base.IdSet{Any}
    collecting::Bool
    deferred::Bool
    # True while values being rebuilt are already in the storage domain (see
    # `_constructwriterstorage`); false for facade-materialized public rows.
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
        _WriterUnionPlanCache(),
        IdDict{AC.Field,Dict{Tuple{Type,Type},_RegisteredWriterPlan}}(),
        arrowtypes,
        IdDict{Type,Bool}(),
        IdDict{Type,Any}(),
        IdDict{Type,Any}(),
        IdDict{Type,Bool}(),
        IdDict{Type,Vector{Type}}(),
        Dict{Tuple{Type,Type},Int}(),
        Dict{Tuple{Type,Bool},AC.Field}(),
        Base.IdSet{Type}(),
        IdDict{AC.Field,Any}(),
        Type[],
        Base.IdSet{Any}(),
        false,
        false,
        false,
        String(column),
        stringview,
        largelists,
        pool,
    )
end

# The type caches below look up before computing: a closure that captures a
# `Type` is instantiated for that type, which costs microseconds per call.
function _writerunionvariants!(context::_WriterContext, T::Type)
    Base.@nospecialize T
    haskey(context.unionvariants, T) && return context.unionvariants[T]
    return context.unionvariants[T] = _checkedwritervariants("writer type $T", T)
end

function _writerunionbranch!(
    context::_WriterContext,
    uniontype::Type,
    runtime::Type,
    variants::Vector{Type}=_writerunionvariants!(context, uniontype),
)
    return get!(context.unionbranches, (uniontype, runtime)) do
        branch = findfirst(T -> runtime <: T, variants)
        branch === nothing && throw(
            ArgumentError("value type $runtime is not in declared writer type $uniontype"),
        )
        branch
    end
end

const _MAX_WRITER_SCHEMA_DEPTH = 64

function _withwriterschema(f, context::_WriterContext, T::Type)
    Base.@nospecialize T
    if T in context.schema
        path = join((string(S) for S in (context.schema..., T)), " -> ")
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
    return Pair{String,String}[
        _EXTENSION_NAME_KEY => String(ArrowTypes.arrowname(T)),
        _EXTENSION_METADATA_KEY => String(ArrowTypes.arrowmetadata(T)),
    ]
end

function _writerextensionshape(context::_WriterContext, T::Type, storage)
    Base.@nospecialize T
    haskey(context.extensionshapes, T) && return context.extensionshapes[T]
    kind = _writerkind!(context, T)
    return context.extensionshapes[T] =
        storage !== T ||
        _writerextension!(context, T) !== nothing ||
        (kind isa ArrowTypes.StructKind && !(T <: Union{NamedTuple,Tuple}))
end

function _writercandidatefield!(
    context::_WriterContext,
    T::Type;
    extension_shape::Bool=false,
)
    Base.@nospecialize T
    key = (T, extension_shape)
    haskey(context.candidates, key) && return context.candidates[key]
    return _withwriterschema(context, T) do
        get!(context.candidates, key) do
            if T === VersionNumber
                field, _ = _constructnativepart("", String[])
                return _arrowtypeslogicalfield(context, field, T)
            end
            storage = _writerstoragetype!(context, T)
            ownshape = _writerextensionshape(context, T, storage)
            nestedshape = extension_shape || ownshape
            if storage isa Union
                variants = _checkedwritervariants("writer candidate type $T", storage)
                children = AC.Field[]
                for (i, variant) in enumerate(variants)
                    child = _writercandidatefield!(
                        context,
                        variant;
                        extension_shape=nestedshape,
                    )
                    push!(
                        children,
                        AC.Field(
                            string(i - 1),
                            child.type;
                            nullable=child.nullable,
                            metadata=_fieldmetadata(child),
                            children=collect(AC.Field, child.children),
                        ),
                    )
                end
                ids = Int8.(0:(length(children) - 1))
                uniontype = AC.UnionType(AC.DenseMode, ids)
                field = AC.Field("", uniontype; nullable=Missing in variants, children)
                return _arrowtypeslogicalfield(context, field, T; nullable=field.nullable)
            end
            field, _ = _arrowtypesstoragecolumn(
                "",
                storage[];
                extension_shape=nestedshape,
                context,
            )
            # Null-storage nullability rule: see _constructarrowtypespart_impl.
            nullable = field.type isa AC.NullType && T !== Missing ? false : field.nullable
            return _arrowtypeslogicalfield(context, field, T; nullable)
        end
    end
end

"Give an inferred writer candidate its column name without rebuilding its shape."
function _namedwriterfield(name::String, candidate::AC.Field)
    return AC.Field(
        name,
        candidate.type;
        nullable=candidate.nullable,
        metadata=_fieldmetadata(candidate),
        children=collect(AC.Field, candidate.children),
    )
end

_placeholderadd(a::Int, b::Int) = a > typemax(Int) - b ? typemax(Int) : a + b
_placeholdermul(a::Int, b::Int) =
    a == 0 || b == 0 ? 0 : a > typemax(Int) ÷ b ? typemax(Int) : a * b

# Two keywords control hidden-slot synthesis, and every function below shares
# them: `forcevalid` means an ancestor requires a NON-NULL value in this slot
# (a dense Union child, an REE value run); `inactive` means the whole subtree
# is unreachable, so validity is free and nothing must be synthesized valid.
# Hence needvalid = !inactive && (forcevalid || !f.nullable).
function _writercansynthesize(f::AC.Field; forcevalid::Bool=false, inactive::Bool=false)
    needvalid = !inactive && (forcevalid || !f.nullable)
    t = f.type
    t isa AC.NullType && return !needvalid
    if t isa AC.RunEndEncodedType
        return length(f.children) == 2 &&
               _writercansynthesize(f.children[2]; forcevalid=needvalid, inactive)
    elseif t isa AC.UnionType
        return any(
            child -> _writercansynthesize(child; forcevalid=needvalid, inactive),
            f.children,
        )
    end
    # Other composite layouts remain non-null when their children are null.
    # Dictionary construction likewise makes a valid index; its pool value is
    # not the dictionary array's validity state.
    return true
end

function _writerplaceholdercost(f::AC.Field; forcevalid::Bool=false, inactive::Bool=false)
    _writercansynthesize(f; forcevalid, inactive) || return typemax(Int)
    needvalid = !inactive && (forcevalid || !f.nullable)
    t = f.type
    t isa AC.NullType && return 0
    t isa AC.BoolType && return 1
    t isa Union{
        AC.IntType,
        AC.FloatType,
        AC.DateType,
        AC.TimeType,
        AC.TimestampType,
        AC.DurationType,
        AC.FixedSizeBinaryType,
        AC.DecimalType,
        AC.IntervalType,
    } && return AC.primwidth(t)
    t isa Union{AC.Utf8Type,AC.BinaryType,AC.ListType} && return t.large ? 8 : 4
    t isa AC.MapType && return 4
    t isa AC.ViewType && return 16
    t isa AC.ListViewType && return t.large ? 16 : 8
    if t isa AC.FixedSizeListType
        return _placeholdermul(
            t.listsize,
            _writerplaceholdercost(only(f.children); inactive),
        )
    elseif t isa AC.StructType
        return foldl(
            (cost, child) -> _placeholderadd(cost, _writerplaceholdercost(child; inactive)),
            f.children;
            init=0,
        )
    elseif t isa AC.RunEndEncodedType
        return _placeholderadd(
            4,
            _writerplaceholdercost(f.children[2]; forcevalid=needvalid, inactive),
        )
    elseif t isa AC.UnionType
        return minimum(
            child -> _writerplaceholdercost(child; forcevalid=needvalid, inactive),
            f.children;
            init=typemax(Int),
        )
    end
    return typemax(Int)
end

function _writerunionvariants(context::_WriterContext, f::AC.Field, writetype::Type)
    f.type isa AC.UnionType || throw(ArgumentError("Field $(f.name) is not a Union"))
    writetype isa Union || throw(
        ArgumentError(
            "writer type $writetype does not describe retained Union field $(f.name)",
        ),
    )
    variants = _writerunionvariants!(context, writetype)
    length(variants) == length(f.children) || throw(
        ArgumentError(
            "writer type $writetype has $(length(variants)) Union branches, but " *
            "retained field $(f.name) has $(length(f.children)) children",
        ),
    )
    return variants
end

function _writerphysicalbasetype(context::_WriterContext, f::AC.Field)
    return get!(context.physical, f) do
        t = f.type
        if t isa AC.DictionaryType
            return _writerphysicalbasetype(context, AC.dictvaluefield(f, t))
        elseif t isa AC.RunEndEncodedType
            return length(f.children) == 2 ?
                   _writerphysicalbasetype(context, f.children[2]) : Any
        elseif t isa Union{AC.ListType,AC.ListViewType}
            return length(f.children) == 1 ?
                   Vector{_writerphysicalbasetype(context, f.children[1])} : Vector{Any}
        elseif t isa AC.FixedSizeListType
            return length(f.children) == 1 ?
                   _boundedfixedliststoragetype(
                t.listsize,
                _writerphysicalbasetype(context, f.children[1]),
            ) : NTuple{0,Any}
        elseif t isa AC.StructType
            names = _arrowtypesstructnames(f)
            types = Type[_writerphysicalbasetype(context, child) for child in f.children]
            return Core.apply_type(NamedTuple, names, Core.apply_type(Tuple, types...))
        elseif t isa AC.MapType
            length(f.children) == 1 || return Dict{Any,Any}
            entries = f.children[1]
            length(entries.children) == 2 || return Dict{Any,Any}
            return Dict{
                _writerphysicalbasetype(context, entries.children[1]),
                _writerphysicalbasetype(context, entries.children[2]),
            }
        elseif t isa AC.UnionType
            isempty(f.children) && return Union{}
            T = _writerphysicalbasetype(context, f.children[1])
            for child in Iterators.drop(f.children, 1)
                T = Union{T,_writerphysicalbasetype(context, child)}
            end
            return T
        end
        return _arrowtypesprimitivebasetype(f)
    end
end

_writerdatelike(t::AC.ArrowType) =
    t isa AC.DateType || (t isa AC.TimestampType && t.unit in (AC.SECOND, AC.MILLISECOND))

_writersequence(t::AC.ArrowType) =
    t isa Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType}

_writerplainstorageshape(f::AC.Field) =
    !f.nullable &&
    _arrowtypesextension(f) === nothing &&
    all(_writerplainstorageshape, f.children)

function _writeropaquestoragealias(retained::AC.Field, candidate::AC.Field)
    isempty(retained.children) || return false
    t = retained.type
    bytes =
        t isa AC.BinaryType ||
        t isa AC.FixedSizeBinaryType ||
        (t isa AC.ViewType && !t.utf8) ||
        (t isa AC.DecimalType && t.bits > 64)
    # Byte-vector storage (Binary) fills any retained byte layout.
    c = candidate.type
    isempty(candidate.children) &&
        return bytes && (c isa AC.BinaryType || (c isa AC.ViewType && !c.utf8))
    (bytes || (t isa AC.IntervalType && t.unit != AC.YEAR_MONTH)) || return false
    return all(_writerplainstorageshape, candidate.children)
end

"Physical descriptor distance; exact layouts win over compatible storage domains."
function _writerdescriptordistance(
    context::_WriterContext,
    retained::AC.Field,
    candidate::AC.Field,
)
    AC.typeequal(retained.type, candidate.type) && return 0
    if retained.type isa AC.UnionType && candidate.type isa AC.UnionType
        return retained.type.mode == candidate.type.mode ? 1 : 3
    end
    if retained.type isa AC.MapType || candidate.type isa AC.MapType
        retained.type isa AC.MapType && candidate.type isa AC.MapType || return nothing
        retained.type.keyssorted == candidate.type.keyssorted || return nothing
    end
    if retained.type isa AC.DurationType && candidate.type isa AC.DurationType
        return 1
    end
    if _writerdatelike(retained.type) && _writerdatelike(candidate.type)
        return typeof(retained.type) === typeof(candidate.type) ? 1 : 3
    end
    if _writersequence(retained.type) && _writersequence(candidate.type)
        if retained.type isa AC.FixedSizeListType &&
           candidate.type isa AC.FixedSizeListType &&
           retained.type.listsize != candidate.type.listsize
            return nothing
        end
        return typeof(retained.type) === typeof(candidate.type) ? 1 : 3
    end
    retainedstorage = _bytevectordomain(_writerphysicalbasetype(context, retained))
    candidatestorage = _bytevectordomain(_writerphysicalbasetype(context, candidate))
    retainedstorage === Any && return nothing
    candidatestorage === Any && return nothing
    retainedstorage === candidatestorage || return nothing
    return typeof(retained.type) === typeof(candidate.type) ? 1 : 4
end

# FixedSizeBinary storage (`NTuple{N,UInt8}`) and byte vectors are one domain.
_bytevectordomain(T) = T isa DataType && T <: NTuple{<:Any,UInt8} ? Vector{UInt8} : T

"Physical writer identity distance for a retained logical branch."
function _writerfielddistance(
    context::_WriterContext,
    retained::AC.Field,
    candidate::AC.Field;
    checkname::Bool=true,
    positionalstruct::Bool=false,
)
    checkname && retained.name != candidate.name && return nothing
    candidate.nullable && !retained.nullable && return nothing
    _arrowtypesextension(retained) == _arrowtypesextension(candidate) || return nothing
    if retained.type isa AC.RunEndEncodedType && !(candidate.type isa AC.RunEndEncodedType)
        length(retained.children) == 2 || return nothing
        childdistance = _writerfielddistance(
            context,
            retained.children[2],
            _withoutownextension(candidate);
            checkname=false,
            positionalstruct,
        )
        childdistance === nothing && return nothing
        return 4 + childdistance + (retained.nullable == candidate.nullable ? 0 : 1)
    end
    typedistance = _writerdescriptordistance(context, retained, candidate)
    typedistance === nothing && return nothing
    if _writeropaquestoragealias(retained, candidate)
        return typedistance + (retained.nullable == candidate.nullable ? 0 : 1)
    end
    length(retained.children) == length(candidate.children) || return nothing
    distance = typedistance + (retained.nullable == candidate.nullable ? 0 : 1)
    if retained.type isa AC.UnionType
        matches = Vector{Pair{Int,Int}}(undef, length(candidate.children))
        for (candidateindex, candidatechild) in enumerate(candidate.children)
            choices = Pair{Int,Int}[]
            for (retainedindex, retainedchild) in enumerate(retained.children)
                childdistance = _writerfielddistance(
                    context,
                    retainedchild,
                    candidatechild;
                    checkname=false,
                    positionalstruct=false,
                )
                childdistance === nothing || push!(choices, childdistance => retainedindex)
            end
            isempty(choices) && return nothing
            sort!(choices; by=choice -> (first(choice), last(choice)))
            length(choices) > 1 &&
                first(choices[1]) == first(choices[2]) &&
                throw(
                    ArgumentError(
                        "writer Field has multiple equally exact children in retained " *
                        "field $(retained.name); logical routing would be ambiguous",
                    ),
                )
            matches[candidateindex] = first(choices)
        end
        length(unique(last(match) for match in matches)) == length(matches) ||
            return nothing
        return distance + sum(first, matches)
    end
    for i in eachindex(retained.children)
        childdistance = _writerfielddistance(
            context,
            retained.children[i],
            candidate.children[i];
            checkname=retained.type isa AC.StructType && !positionalstruct,
            positionalstruct=retained.type isa AC.MapType,
        )
        childdistance === nothing && return nothing
        distance += childdistance
    end
    return distance
end

function _writerchildcandidate(context::_WriterContext, f::AC.Field, candidate::AC.Field)
    matches = Pair{Int,Int}[]
    for (i, child) in enumerate(f.children)
        distance = _writerfielddistance(context, child, candidate; checkname=false)
        distance === nothing || push!(matches, distance => i)
    end
    isempty(matches) && return nothing
    sort!(matches; by=pair -> (first(pair), last(pair)))
    length(matches) > 1 &&
        first(matches[1]) == first(matches[2]) &&
        throw(
            ArgumentError(
                "writer Field has multiple equally exact children in retained field " *
                "$(f.name); logical routing would be ambiguous",
            ),
        )
    return last(first(matches))
end

"Match one physical storage branch beneath a retained logical Union."
function _writerunionstoragechildcandidate(
    context::_WriterContext,
    f::AC.Field,
    candidate::AC.Field,
)
    child = _writerchildcandidate(context, f, candidate)
    child === nothing || return child

    # An inferred dictionary can preserve the abstract logical label on each
    # observed subtype branch. Those repeated labels describe the parent
    # public-domain value, not distinct physical branches. Remove only labels that
    # equal the parent label before matching the parent's storage Union.
    parentextension = _arrowtypesextension(f)
    parentextension === nothing && return nothing
    storagechildren = AC.Field[
        _arrowtypesextension(branch) == parentextension ? _withoutownextension(branch) :
        branch for branch in f.children
    ]
    return _writerchildcandidate(
        context,
        AC.Field(
            f.name,
            f.type;
            nullable=f.nullable,
            metadata=_fieldmetadata(f),
            children=storagechildren,
        ),
        candidate,
    )
end

function _writerunionplan!(
    context::_WriterContext,
    f::AC.Field,
    storagetype::Type;
    extension_shape::Bool=false,
)
    bytype = get!(context.unions, f) do
        Dict{Tuple{Type,Bool},Tuple{Vector{Type},Vector{Int}}}()
    end
    return get!(bytype, (storagetype, extension_shape)) do
        variants = _writerunionvariants(context, f, storagetype)
        children = Int[]
        owned = Set{Int}()
        for variant in variants
            candidate = _writercandidatefield!(context, variant; extension_shape)
            child = _writerunionstoragechildcandidate(context, f, candidate)
            child === nothing && throw(
                ArgumentError(
                    "storage Union branch $variant matches no child of retained " *
                    "field $(f.name)",
                ),
            )
            child in owned && throw(
                ArgumentError(
                    "storage Union $storagetype maps more than one branch to child " *
                    "$child of retained field $(f.name)",
                ),
            )
            push!(owned, child)
            push!(children, child)
        end
        # A branch must identify one retained child uniquely. Exact layouts
        # win, but the retained descriptor may select a compatible storage form.
        return variants, children
    end
end

_fieldcontainsunion(f::AC.Field) =
    f.type isa AC.UnionType || any(_fieldcontainsunion, f.children)

function _registeredoutermissing(context::_WriterContext, f::AC.Field, target, runtimes)
    f.type isa AC.UnionType || return f.nullable
    storage = _writerstoragetype!(context, target)
    storage isa Union && return _arrowtypeslogicalnullable(f, target, storage)

    nullable = nothing
    for T in runtimes
        runtime_storage = _writerstoragetype!(context, T)
        runtime_storage isa Union || continue
        storagebranches = length(Base.uniontypes(runtime_storage))
        fieldbranches = length(f.children)
        candidate = if fieldbranches == storagebranches
            false
        elseif fieldbranches == storagebranches + 1
            Missing <: runtime_storage && throw(
                ArgumentError(
                    "registered writer type $T uses Missing as a storage Union " *
                    "branch, so retained field $(f.name) cannot add an " *
                    "indistinguishable outer missing branch",
                ),
            )
            true
        else
            throw(
                ArgumentError(
                    "registered writer type $T has $storagebranches storage Union " *
                    "branches, but retained field $(f.name) has $fieldbranches children",
                ),
            )
        end
        nullable === nothing ||
            nullable == candidate ||
            throw(
                ArgumentError(
                    "runtime writer types disagree about outer nullability for retained " *
                    "field $(f.name)",
                ),
            )
        nullable = candidate
    end
    nullable === nothing || return nullable
    nullchildren = findall(
        child -> child.type isa AC.NullType && _arrowtypesextension(child) === nothing,
        f.children,
    )
    return length(nullchildren) == 1
end

function _writercheckabstractextensionidentity(
    f::AC.Field,
    target,
    T::Type,
    candidateextension,
)
    isconcretetype(target) && return nothing
    retainedextension = _arrowtypesextension(f)
    candidateextension === nothing ||
        candidateextension == retainedextension ||
        throw(
            ArgumentError(
                "registered writer subtype $T declares extension " *
                "$(repr(candidateextension)), which cannot replace abstract " *
                "retained target $target with extension " *
                "$(repr(retainedextension))",
            ),
        )
    return nothing
end

_writercheckabstractextension(f::AC.Field, target, T::Type, candidate::AC.Field) =
    _writercheckabstractextensionidentity(f, target, T, _arrowtypesextension(candidate))

function _registeredwriterroutes(
    context::_WriterContext,
    f::AC.Field,
    runtimes,
    outermissing::Bool,
)
    routes = _WriterTypeRoutes()
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    target === nothing &&
        throw(ArgumentError("retained field $(f.name) has no registered writer target"))
    if !(f.type isa AC.UnionType)
        for T in runtimes
            candidate = _writercandidatefield!(context, T)
            _writercheckabstractextension(f, target, T, candidate)
            candidate = _withoutownextension(candidate)
            retained = _withoutownextension(f)
            _writerfielddistance(context, retained, candidate; checkname=false) ===
            nothing && throw(
                ArgumentError(
                    "registered writer type $T does not match retained field " *
                    "$(f.name)",
                ),
            )
            routes[T] = 0
        end
        outermissing && (routes[Missing] = 0)
        return routes
    end

    childowners = Dict{Int,Type}()
    for T in runtimes
        candidate = _writercandidatefield!(context, T)
        _writercheckabstractextension(f, target, T, candidate)
        storage = _writerstoragetype!(context, T)
        if storage isa Union
            plannedstorage = outermissing ? Union{Missing,storage} : storage
            _writerunionplan!(
                context,
                f,
                plannedstorage;
                extension_shape=_writerextensionshape(context, T, storage),
            )
            routes[T] = 0
            continue
        end
        child = _writerchildcandidate(context, f, candidate)
        if child === nothing && _arrowtypesextension(candidate) !== nothing
            # A direct registered Union keeps its logical label only on the
            # parent. A dictionary can retain a concrete label on its value
            # children. Prefer the exact child contract, then retry after
            # removing the runtime subtype's outer label.
            candidateextension = _arrowtypesextension(candidate)
            if isconcretetype(target) || candidateextension == _arrowtypesextension(f)
                child = _writerchildcandidate(context, f, _withoutownextension(candidate))
            end
        end
        child === nothing && throw(
            ArgumentError(
                "registered writer type $T matches no child of retained field $(f.name)",
            ),
        )
        owner = get(childowners, child, nothing)
        owner === nothing || throw(
            ArgumentError(
                "registered writer types $owner and $T both map to child $child " *
                "of retained field $(f.name)",
            ),
        )
        childowners[child] = T
        routes[T] = child
    end
    if outermissing
        nullchildren = findall(
            child ->
                child.type isa AC.NullType && _arrowtypesextension(child) === nothing,
            f.children,
        )
        length(nullchildren) == 1 || throw(
            ArgumentError(
                "retained registered Union field $(f.name) has no unique outer " *
                "missing child",
            ),
        )
        routes[Missing] = only(nullchildren)
    end
    return routes
end

"Append `T` once, under `limit`, or throw naming `owner` and `kind`."
function _writerpushtype!(
    types::Vector{Type},
    seen::Base.IdSet{Type},
    T::Type,
    owner::String,
    limit::Int,
    kind::String,
)
    T in seen && return nothing
    length(types) < limit || throw(ArgumentError("$owner has more than $limit $kind"))
    push!(seen, T)
    push!(types, T)
    return nothing
end

function _writerpushinferred!(
    context::_WriterContext,
    f::AC.Field,
    types::Vector{Type},
    seen::Base.IdSet{Type},
    T::Type,
)
    _writerregisterinferred!(context, f, T)
    return _writerpushtype!(
        types,
        seen,
        T,
        "column $(f.name)",
        _MAX_INFERRED_WRITER_TYPES,
        "inferred registered writer runtime types",
    )
end

function _writerregisterinferred!(context::_WriterContext, f::AC.Field, T::Type)
    return _writerregisterinferredtype!(
        context,
        f.name,
        T,
        "inferred registered writer runtime types",
    )
end

function _writerregisterinferredtype!(
    context::_WriterContext,
    name::String,
    T::Type,
    kind::String,
)
    if !(T in context.inferred)
        length(context.inferred) < _MAX_INFERRED_WRITER_TYPES || throw(
            ArgumentError("column $name has more than $_MAX_INFERRED_WRITER_TYPES $kind"),
        )
        push!(context.inferred, T)
    end
    return nothing
end

function _writerregisterstorageinference!(context::_WriterContext, name::String, T::Type)
    return _writerregisterinferredtype!(
        context,
        name,
        T,
        "inferred ArrowTypes storage types",
    )
end

"""
Narrow one unresolved abstract declaration from whole-column runtime evidence.
Returns nothing when the declaration cannot be narrowed from runtime evidence.
"""
function _narrowabstractwriterparts(name::String, parts, context::_WriterContext)
    logical = nothing
    for part in parts
        declared = Base.nonmissingtype(eltype(part))
        declared in (Union{}, Any) && continue
        if !isconcretetype(declared) && !(declared isa Union)
            logical === nothing ? (logical = declared) :
            logical === declared || return nothing
        end
    end
    logical === nothing && return nothing
    _writerstoragetype!(context, logical) === logical || return nothing
    _writerextension!(context, logical) === nothing || return nothing
    # Abstract row types of a plain nested layout (NTuples of nullable values,
    # `AbstractVector{Int}`) write as that layout without runtime narrowing.
    _nestedkind(context, logical) === nothing || return nothing

    observed = Type[]
    seen = Base.IdSet{Type}()
    needsruntime = false
    nullable = false
    for part in parts
        declared = Base.nonmissingtype(eltype(part))
        nullable |= Missing <: eltype(part)
        declared in (Union{}, logical) ||
            (isconcretetype(declared) && declared <: logical) ||
            return nothing
        for value in part
            value === missing && continue
            value isa logical || throw(
                ArgumentError(
                    "column $name contains $(typeof(value)), which is outside its " *
                    "declared abstract element type $logical",
                ),
            )
            runtime = typeof(value)
            runtime in seen && continue
            push!(seen, runtime)
            push!(observed, runtime)
            needsruntime |=
                _writerneedstype!(context, runtime) ||
                _writerextension!(context, runtime) !== nothing
        end
    end
    isempty(observed) && return nothing
    if !needsruntime && _singletypefamily(logical)
        length(observed) == 1 || return nothing
        storagetype = nullable ? Union{Missing,only(observed)} : only(observed)
        return AbstractVector[_collectas(storagetype, part) for part in parts]
    end
    if !needsruntime && _abstractnonnative(logical)
        # `Real[1, 2.5]`: one runtime type writes as that type, several as a
        # dense Union of them.
        length(observed) <= _MAX_INFERRED_WRITER_TYPES || throw(
            ArgumentError(
                "column $name holds values of more than $_MAX_INFERRED_WRITER_TYPES " *
                "types; give it a concrete element type",
            ),
        )
        storagetype = _writeruniontype(observed)
        nullable && (storagetype = Union{Missing,storagetype})
        return AbstractVector[_collectas(storagetype, part) for part in parts]
    end
    if !needsruntime
        # Rows of an abstract NamedTuple column (some fields missing in some
        # rows, say) share one concrete row type once widened field by field.
        logical <: NamedTuple || return nothing
        rowtype = _widenednamedtuple(observed)
        rowtype === nothing && return nothing
        nullable && (rowtype = Union{Missing,rowtype})
        return AbstractVector[_collectas(rowtype, part) for part in parts]
    end
    for runtime in observed
        _writerregisterinferredtype!(
            context,
            name,
            runtime,
            "inferred registered writer runtime types",
        )
    end
    storagetype = _writeruniontype(observed)
    nullable && (storagetype = Union{Missing,storagetype})
    return AbstractVector[_collectas(storagetype, part) for part in parts]
end

"The field-wise Union of same-named NamedTuple row types, or nothing."
function _widenednamedtuple(observed)
    names = fieldnames(first(observed))
    all(T -> T <: NamedTuple && fieldnames(T) === names, observed) || return nothing
    types = ntuple(i -> Union{(fieldtype(T, i) for T in observed)...}, length(names))
    return NamedTuple{names,Tuple{types...}}
end

"One whole-column plan for an ArrowTypes mapping whose storage needs values."
struct _InferredStoragePlan
    logical::Type
    declared::Type
    storagetype::Type
    parts::Vector{AbstractVector}
    nullable::Bool
end

function _inferredstoragelogical(parts, target)
    target === nothing || return target
    logical = nothing
    for part in parts
        T = Base.nonmissingtype(eltype(part))
        T === Union{} && continue
        isconcretetype(T) || return nothing
        logical === nothing ? (logical = T) : logical === T || return nothing
    end
    return logical
end

function _inferredstoragedeclaration(parts, context::_WriterContext; target=nothing)
    logical = _inferredstoragelogical(parts, target)
    logical === nothing && return nothing
    logical === VersionNumber && return nothing
    for part in parts
        T = Base.nonmissingtype(eltype(part))
        T in (Union{}, logical) && continue
        target !== nothing &&
            T === Any &&
            all(value -> value === missing || value isa logical, part) &&
            continue
        return nothing
    end
    _writerneedstype!(context, logical) || return nothing
    declared = _writerstoragetype!(context, logical)
    _arrowtypesstorageisspecified(context, declared) && return nothing
    return logical => declared
end

"""
One whole-column storage plan for an ArrowTypes mapping with abstract storage,
or nothing when no plan applies.
"""
function _inferredstorageplan(
    name::String,
    parts,
    context::_WriterContext;
    target=nothing,
    retained=nothing,
)
    declaration = _inferredstoragedeclaration(parts, context; target)
    declaration === nothing && return nothing
    logical, declared = declaration

    lowered = Vector{Vector{Any}}(undef, length(parts))
    observed = Type[]
    seen = Base.IdSet{Type}()
    sawmissing = false
    nullable = false
    logicalseen = Base.IdSet{Type}()
    for (partition, part) in enumerate(parts)
        nullable |= Missing <: eltype(part)
        values = Any[]
        sizehint!(values, length(part))
        for value in part
            if value === missing
                push!(values, missing)
                sawmissing = true
                continue
            end
            mapped = _writertoarrow(context, value, logical, name, retained)
            mapped = _writerconvertlowered(declared, mapped, context, logical, name)
            push!(values, mapped)
            if retained !== nothing && !isconcretetype(logical)
                runtime = typeof(value)
                if !(runtime in logicalseen)
                    _writerregisterinferred!(context, retained, runtime)
                    _writercheckabstractextensionidentity(
                        retained,
                        logical,
                        runtime,
                        _writerextension!(context, runtime),
                    )
                    push!(logicalseen, runtime)
                end
            end
            runtime = typeof(mapped)
            runtime in seen && continue
            _writerregisterstorageinference!(context, name, runtime)
            push!(seen, runtime)
            push!(observed, runtime)
        end
        lowered[partition] = values
    end

    if isempty(observed)
        mappedparts = AbstractVector[fill(missing, length(values)) for values in lowered]
        return _InferredStoragePlan(logical, declared, Missing, mappedparts, nullable)
    end
    nullable &&
        all(T -> _writerkind!(context, T) isa ArrowTypes.NullKind, observed) &&
        throw(
            ArgumentError(
                "nullable logical column $name maps its non-missing values to Arrow " *
                "Null storage, so outer missing values cannot be distinguished; " *
                "use DictEncode to preserve the two states",
            ),
        )
    storagetype = reduce(_writerpromoteunion, observed)
    sawmissing && (storagetype = Union{Missing,storagetype})
    mappedparts = AbstractVector[
        _writerconvertedcolumn(storagetype, values, context, logical, name) for
        values in lowered
    ]
    return _InferredStoragePlan(logical, declared, storagetype, mappedparts, nullable)
end

function _constructinferredfreshpart(
    name::String,
    plan::_InferredStoragePlan,
    values::AbstractVector,
    context::_WriterContext,
)
    ownshape = _writerextensionshape(context, plan.logical, plan.declared)
    field, data = _arrowtypesstoragecolumn(name, values; extension_shape=ownshape, context)
    # Null-storage nullability rule: see _constructarrowtypespart_impl.
    nullable = field.type isa AC.NullType && !plan.nullable ? false : field.nullable
    return _arrowtypeslogicalfield(context, field, plan.logical; nullable), data
end

function _inferredstorageroutes(
    context::_WriterContext,
    field::AC.Field,
    storagetype::Type;
    extension_shape::Bool,
)
    field.type isa AC.UnionType || return nothing
    routes = _WriterTypeRoutes()
    owners = Set{Int}()
    for variant in _writertypevariants(storagetype)
        candidate = _writercandidatefield!(context, variant; extension_shape)
        child = _writerchildcandidate(context, field, candidate)
        child === nothing && throw(
            ArgumentError(
                "inferred storage type $variant matches no child of retained " *
                "field $(field.name)",
            ),
        )
        child in owners && throw(
            ArgumentError(
                "inferred storage types map ambiguously to child $child of " *
                "retained field $(field.name)",
            ),
        )
        push!(owners, child)
        routes[variant] = child
    end
    return routes
end

function _constructinferredretainedpart(
    field::AC.Field,
    plan::_InferredStoragePlan,
    values::AbstractVector,
    context::_WriterContext,
)
    isempty(values) && return _constructhiddenpart(field, 0, context)
    ownshape = _writerextensionshape(context, plan.logical, plan.declared)
    routes =
        _inferredstorageroutes(context, field, plan.storagetype; extension_shape=ownshape)
    return _constructwriterchild(field, plan.storagetype, values; routes, context)
end

_writerfullydeclared(T::Type) =
    isconcretetype(T) || (T isa Union && all(isconcretetype, _writertypevariants(T)))

function _registeredwritercolumnplan(context::_WriterContext, f::AC.Field, valueparts)
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    target === nothing && return nothing
    declaredtypes = Type[]
    declaredseen = Base.IdSet{Type}()
    inferredtypes = Type[]
    inferredseen = Base.IdSet{Type}()
    schemaevidence = Type[]
    schemaseen = Base.IdSet{Type}()
    sawmissing = false
    for values in valueparts
        declared = Base.nonmissingtype(eltype(values))
        declared in (Union{}, Any) ||
            declared <: target ||
            throw(
                ArgumentError(
                    "column $(f.name) has declared non-missing type $declared, " *
                    "but its retained ArrowTypes target is $target",
                ),
            )
        declaredvariants = _writertypevariants(declared)
        fullydeclared = _writerfullydeclared(declared)
        for value in values
            value === _RETAINED_HIDDEN && continue
            if value === missing
                sawmissing = true
            elseif value isa target
                fullydeclared || _writerpushinferred!(
                    context,
                    f,
                    inferredtypes,
                    inferredseen,
                    typeof(value),
                )
            else
                throw(
                    ArgumentError(
                        "column $(f.name) holds $(typeof(value)) values, but its " *
                        "retained ArrowTypes target is $target",
                    ),
                )
            end
        end
        if isconcretetype(declared)
            _writerpushtype!(
                declaredtypes,
                declaredseen,
                declared,
                "column $(f.name)",
                _MAX_WRITER_UNION_BRANCHES,
                "declared registered writer types",
            )
        elseif declared isa Union
            for member in declaredvariants
                isconcretetype(member) || continue
                _writerpushtype!(
                    declaredtypes,
                    declaredseen,
                    member,
                    "column $(f.name)",
                    _MAX_WRITER_UNION_BRANCHES,
                    "declared registered writer types",
                )
            end
        elseif declared !== Union{} && _writerstoragetype!(context, declared) !== declared
            # An abstract declaration with an explicit storage mapping is
            # schema evidence even when no runtime value is visible.
            _writerpushtype!(
                schemaevidence,
                schemaseen,
                declared,
                "column $(f.name)",
                _MAX_WRITER_UNION_BRANCHES,
                "registered writer schema-evidence types",
            )
        end
    end
    plantypes = _mergewritertypes(
        "column $(f.name)",
        declaredtypes,
        inferredtypes,
        _MAX_WRITER_UNION_BRANCHES,
        "registered writer planning types",
    )
    plantypes = _mergewritertypes(
        "column $(f.name)",
        plantypes,
        schemaevidence,
        _MAX_WRITER_UNION_BRANCHES,
        "registered writer planning types",
    )
    outermissing = _registeredoutermissing(context, f, target, plantypes)
    sawmissing &&
        !outermissing &&
        throw(
            ArgumentError(
                "column $(f.name) contains an outer missing value that its registered " *
                "ArrowTypes field cannot represent",
            ),
        )
    input = outermissing ? Union{Missing,target} : target
    return _RegisteredWriterPlan(
        input,
        _registeredwriterroutes(context, f, plantypes, outermissing),
    )
end

_registeredwriterplan(context::_WriterContext, f::AC.Field, values::AbstractVector) =
    _registeredwritercolumnplan(context, f, (values,))

"A storage-domain placeholder value tree for one hidden slot under a retained Field."
function _writerhiddenstorage(f::AC.Field; forcevalid::Bool=false, inactive::Bool=false)
    t = f.type
    if t isa AC.UnionType
        needvalid = !inactive && (forcevalid || !f.nullable)
        child = _writerhiddenunionchild(f; forcevalid=needvalid, inactive)
        return _WriterRoutedUnion(
            child,
            _writerhiddenstorage(f.children[child]; forcevalid=needvalid, inactive),
        )
    elseif t isa Union{AC.ListType,AC.ListViewType}
        return Any[]
    elseif t isa AC.FixedSizeListType
        child = only(f.children)
        return Any[_writerhiddenstorage(child; inactive) for _ = 1:(t.listsize)]
    elseif t isa AC.StructType
        return Pair{String,Any}[
            child.name => _writerhiddenstorage(child; inactive) for child in f.children
        ]
    elseif t isa AC.MapType
        return Pair{Any,Any}[]
    elseif t isa AC.RunEndEncodedType
        needvalid = !inactive && (forcevalid || !f.nullable)
        return _writerhiddenstorage(f.children[2]; forcevalid=needvalid, inactive)
    elseif t isa AC.DictionaryType
        # A dictionary index must point at something even in an inactive
        # subtree, so a non-nullable dictionary always needs one pool entry
        # (`_constructhiddenpart`'s dictionary branch shares this rule). In
        # the value domain that entry is the VALUE field's placeholder, and
        # it must be non-missing: value-level construction reads a missing
        # value back as a null INDEX, which a needvalid slot cannot have.
        needvalid = (!inactive && forcevalid) || !f.nullable
        needvalid || return missing
        return _writerhiddenstorage(AC.dictvaluefield(f, t); forcevalid=true)
    end
    return _retainedplaceholder(f; forcevalid=(!inactive && forcevalid))
end

function _writerhiddenunionchild(
    f::AC.Field;
    forcevalid::Bool=(!f.nullable),
    inactive::Bool=false,
)
    candidates = Int[
        i for i in eachindex(f.children) if
        _writercansynthesize(f.children[i]; forcevalid, inactive)
    ]
    isempty(candidates) && throw(
        ArgumentError(
            "retained Union field $(f.name) has no child that can synthesize a " *
            "$(forcevalid ? "non-null hidden" : "hidden") value",
        ),
    )
    _, position = findmin(
        i -> _writerplaceholdercost(f.children[i]; forcevalid, inactive),
        candidates,
    )
    return candidates[position]
end

# A Null-only child has exactly one public-domain value, so hidden and missing rows
# only contribute child LENGTH. Visible rows are still validated for exact
# width; no placeholder objects are allocated.
function _writercompactfixednull(f::AC.Field)
    f.type isa AC.FixedSizeListType || return false
    length(f.children) == 1 || return false
    child = only(f.children)
    # Recursive compaction can preserve nested validity without invoking any
    # logical hooks only for an extension-free shape. A labelled immediate
    # Null child is also safe because it has no descendant validity topology.
    return _writerplainfixednullshape(f) || child.type isa AC.NullType
end

function _writerplainfixednullshape(f::AC.Field)
    _arrowtypesextension(f) === nothing || return false
    f.type isa AC.NullType && return true
    f.type isa AC.FixedSizeListType || return false
    length(f.children) == 1 || return false
    return _writerplainfixednullshape(only(f.children))
end

function _checkplainfixednullvalue(f::AC.Field, value)
    if f.type isa AC.NullType
        value === missing || throw(
            ArgumentError(
                "retained Null field $(f.name) received a non-null public-domain value",
            ),
        )
        # NullType is null by definition. Its Field.nullable flag is advisory
        # metadata and cannot make a physical Null slot non-null.
        return nothing
    end
    if value === missing
        f.nullable || throw(
            ArgumentError(
                "retained field $(f.name) is non-nullable but received a null value",
            ),
        )
        return nothing
    end
    t = f.type::AC.FixedSizeListType
    (value isa Tuple || value isa AbstractVector) || throw(
        ArgumentError("retained fixed-size-list field $(f.name) requires a sequence value"),
    )
    length(value) == t.listsize || throw(
        ArgumentError(
            "retained fixed-size-list field $(f.name) received $(length(value)) " *
            "values; expected $(t.listsize)",
        ),
    )
    child = only(f.children)
    for item in value
        _checkplainfixednullvalue(child, item)
    end
    return nothing
end

function _writerhiddenvalue(f::AC.Field, writetype::Type)
    f.nullable && Missing <: writetype && !_fieldcontainsunion(f) && return missing
    # Hidden parent slots have no logical Union branch. The retained Field is
    # complete physical authority, so choose cheap storage placeholders without
    # requiring a constructor for the logical writer type.
    return _writerhiddenstorage(f; inactive=true)
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

function _writerhiddenbuffers(f::AC.Field, n::Int; forcevalid::Bool=false)
    spec = AC.layoutspec_of(f.type)
    buffers = AC.BufferSlice[]
    makenull = f.nullable && !forcevalid
    nullcount = makenull ? n : 0
    for role in spec.buffers
        if role == AC.VALIDITY
            nbytes = makenull ? cld(n, 8) : 0
            push!(buffers, _writerhiddenbytes(f, nbytes, "validity"))
        elseif role == AC.DATA
            nbytes =
                spec.fixedwidth == -1 ? cld(n, 8) :
                spec.fixedwidth == 0 ? 0 : _writerhiddenmul(f, n, spec.fixedwidth, "data")
            push!(buffers, _writerhiddenbytes(f, nbytes, "data"))
        elseif role == AC.OFFSETS
            nslots = Base.checked_add(Int64(n), Int64(1))
            nbytes = _writerhiddenmul(f, nslots, spec.offsetwidth, "offset")
            push!(buffers, _writerhiddenbytes(f, nbytes, "offset"))
        elseif role == AC.ELEMENT_OFFSETS || role == AC.SIZES
            nbytes = _writerhiddenmul(f, n, spec.offsetwidth, "element-offset")
            push!(buffers, _writerhiddenbytes(f, nbytes, "element-offset"))
        elseif role == AC.VIEWS
            nbytes = _writerhiddenmul(f, n, 16, "view")
            push!(buffers, _writerhiddenbytes(f, nbytes, "view"))
        else
            throw(
                ArgumentError(
                    "hidden construction for retained field $(f.name) needs a " *
                    "layout-specific $(role) buffer",
                ),
            )
        end
    end
    return buffers, nullcount
end

"Build exact retained data for slots whose values are hidden by an ancestor."
function _constructhiddenpart(
    f::AC.Field,
    n::Int,
    context::_WriterContext;
    forcevalid::Bool=false,
    inactive::Bool=false,
)
    n >= 0 || throw(ArgumentError("negative hidden length for retained field $(f.name)"))
    t = f.type
    if t isa AC.NullType
        !inactive &&
            forcevalid &&
            n > 0 &&
            throw(
                ArgumentError(
                    "retained Null field $(f.name) cannot synthesize a non-null hidden value",
                ),
            )
        return _retainedfield(f), AC.ArrayData(t, n, AC.BufferSlice[]; nullcount=n)
    elseif t isa AC.DictionaryType
        valuefield = AC.dictvaluefield(f, t)
        # Unlike the shared needvalid rule, `inactive` cannot waive validity
        # here: a non-nullable dictionary's indices are all valid, so they
        # must point at something — one pool entry — even in an inactive
        # subtree.
        needvalid = (!inactive && forcevalid) || !f.nullable
        poollength = n == 0 || !needvalid ? 0 : 1
        rebuiltvaluefield, dictionary =
            _constructhiddenpart(valuefield, poollength, context; inactive)
        buffers, nullcount =
            _writerhiddenbuffers(f, n; forcevalid=(!inactive && forcevalid))
        field = AC.Field(
            f.name,
            t;
            nullable=f.nullable,
            metadata=_fieldmetadata(f),
            children=collect(AC.Field, rebuiltvaluefield.children),
        )
        data = AC.ArrayData(t, n, buffers; dictionary, nullcount)
        return field, data
    elseif t isa Union{AC.ListType,AC.ListViewType,AC.MapType}
        length(f.children) == 1 || throw(
            ArgumentError(
                "retained $(AC.descriptorname(t)) field $(f.name) needs one child",
            ),
        )
        childfield, childdata = _constructhiddenpart(f.children[1], 0, context; inactive)
        buffers, nullcount =
            _writerhiddenbuffers(f, n; forcevalid=(!inactive && forcevalid))
        data = AC.ArrayData(t, n, buffers; children=AC.ArrayData[childdata], nullcount)
        return _retainedfield(f; children=AC.Field[childfield]), data
    elseif t isa AC.FixedSizeListType
        length(f.children) == 1 ||
            throw(ArgumentError("retained fixed-size-list field $(f.name) needs one child"))
        childlength64 = _writerhiddenmul(f, n, t.listsize, "fixed-size-list child")
        childlength64 <= typemax(Int) || throw(
            ArgumentError(
                "hidden fixed-size-list child for retained field $(f.name) exceeds " *
                "the supported length range",
            ),
        )
        childfield, childdata =
            _constructhiddenpart(f.children[1], Int(childlength64), context; inactive)
        buffers, nullcount =
            _writerhiddenbuffers(f, n; forcevalid=(!inactive && forcevalid))
        data = AC.ArrayData(t, n, buffers; children=AC.ArrayData[childdata], nullcount)
        return _retainedfield(f; children=AC.Field[childfield]), data
    elseif t isa AC.StructType
        childfields = AC.Field[]
        childdata = AC.ArrayData[]
        for child in f.children
            rebuiltfield, rebuiltdata = _constructhiddenpart(child, n, context; inactive)
            push!(childfields, rebuiltfield)
            push!(childdata, rebuiltdata)
        end
        buffers, nullcount =
            _writerhiddenbuffers(f, n; forcevalid=(!inactive && forcevalid))
        data = AC.ArrayData(t, n, buffers; children=childdata, nullcount)
        return _retainedfield(f; children=childfields), data
    elseif t isa AC.RunEndEncodedType
        length(f.children) == 2 || throw(
            ArgumentError("retained run-end encoded field $(f.name) needs two children"),
        )
        runfield, valuefield = f.children
        if n == 0
            rebuiltrunfield, runenddata = _constructhiddenpart(runfield, 0, context)
            rebuiltvaluefield, valuedata = _constructhiddenpart(valuefield, 0, context)
        else
            runfield.type isa AC.IntType || throw(
                ArgumentError("retained REE field $(f.name) has a non-integer run end"),
            )
            RT = AC.juliatype(runfield.type)
            n <= typemax(RT) || throw(
                ArgumentError(
                    "hidden REE length for retained field $(f.name) exceeds its " *
                    "run-end type",
                ),
            )
            rebuiltrunfield, runenddata = _constructpart(runfield, RT[RT(n)]; context)
            rebuiltvaluefield, valuedata = _constructhiddenpart(
                valuefield,
                1,
                context;
                forcevalid=(!inactive && (forcevalid || !f.nullable)),
                inactive,
            )
        end
        data = AC.ArrayData(
            t,
            n,
            AC.BufferSlice[];
            children=AC.ArrayData[runenddata, valuedata],
            nullcount=0,
        )
        return _retainedfield(f; children=AC.Field[rebuiltrunfield, rebuiltvaluefield]),
        data
    elseif t isa AC.UnionType
        isempty(f.children) &&
            throw(ArgumentError("retained Union field $(f.name) has no children"))
        if n == 0
            childfields = AC.Field[]
            childdata = AC.ArrayData[]
            for child in f.children
                rebuiltfield, rebuiltdata = _constructhiddenpart(child, 0, context)
                push!(childfields, rebuiltfield)
                push!(childdata, rebuiltdata)
            end
            buffers =
                t.mode == AC.SparseMode ? AC.BufferSlice[AC.BufferSlice()] :
                AC.BufferSlice[AC.BufferSlice(), AC.BufferSlice()]
            data = AC.ArrayData(t, 0, buffers; children=childdata, nullcount=0)
            return _retainedfield(f; children=childfields), data
        end
        needvalid = !inactive && (forcevalid || !f.nullable)
        chosen = _writerhiddenunionchild(f; forcevalid=needvalid, inactive)
        childfields = AC.Field[]
        childdata = AC.ArrayData[]
        for (index, child) in enumerate(f.children)
            childlength = t.mode == AC.SparseMode ? n : index == chosen && n > 0 ? 1 : 0
            rebuiltfield, rebuiltdata = _constructhiddenpart(
                child,
                childlength,
                context;
                forcevalid=needvalid && index == chosen,
                inactive=inactive || index != chosen,
            )
            push!(childfields, rebuiltfield)
            push!(childdata, rebuiltdata)
        end
        typeids = n == 0 ? AC.BufferSlice() : AC._databuffer(fill(t.typeids[chosen], n))
        buffers = if t.mode == AC.SparseMode
            AC.BufferSlice[typeids]
        else
            offsets = n == 0 ? AC.BufferSlice() : AC._databuffer(zeros(Int32, n))
            AC.BufferSlice[typeids, offsets]
        end
        data = AC.ArrayData(t, n, buffers; children=childdata, nullcount=0)
        return _retainedfield(f; children=childfields), data
    end

    isempty(f.children) || throw(
        ArgumentError(
            "retained leaf field $(f.name) unexpectedly has $(length(f.children)) children",
        ),
    )
    buffers, nullcount = _writerhiddenbuffers(f, n; forcevalid=(!inactive && forcevalid))
    return _retainedfield(f), AC.ArrayData(t, n, buffers; nullcount)
end

"Build a fixed-size list whose immediate child has Null storage."
function _constructfixednullpart(
    f::AC.Field,
    present::AbstractVector{Bool},
    context::_WriterContext,
)
    t = f.type::AC.FixedSizeListType
    length(f.children) == 1 && only(f.children).type isa AC.NullType || throw(
        ArgumentError(
            "retained fixed-size-list field $(f.name) does not have an immediate Null child",
        ),
    )
    any(!, present) &&
        !f.nullable &&
        throw(
            ArgumentError(
                "column $(f.name) contains a null fixed-size-list value under a " *
                "non-nullable field",
            ),
        )
    childlength64 = _writerhiddenmul(f, length(present), t.listsize, "fixed-list child")
    childlength64 <= typemax(Int) || throw(
        ArgumentError(
            "retained fixed-size-list child $(f.name) exceeds the supported length range",
        ),
    )
    childfield, childdata =
        _constructhiddenpart(only(f.children), Int(childlength64), context)
    data = AC.ArrayData(
        t,
        length(present),
        AC.BufferSlice[AC._bitmapbuffer(present)];
        children=AC.ArrayData[childdata],
        nullcount=count(!, present),
    )
    return _retainedfield(f; children=AC.Field[childfield]), data
end

"Build exact extension-free nested fixed-list validity over a Null leaf."
function _constructplainfixednullpart(
    f::AC.Field,
    values::AbstractVector,
    context::_WriterContext;
    routed::Bool=false,
)
    _writerplainfixednullshape(f) || throw(
        ArgumentError(
            "retained fixed-size-list field $(f.name) is not an extension-free Null-only shape",
        ),
    )
    slots = Pair{Int,Any}[]
    sizehint!(slots, length(values))
    for (i, raw) in enumerate(values)
        raw === _RETAINED_HIDDEN && continue
        value = if routed
            raw isa _WriterRoutedUnion || throw(
                ArgumentError(
                    "retained sparse Union child $(f.name) lost its writer route",
                ),
            )
            raw.value
        else
            raw
        end
        _checkplainfixednullvalue(f, value)
        push!(slots, i => value)
    end
    return _constructplainfixednullpart(f, length(values), slots, context)
end

function _constructplainfixednullpart(f::AC.Field, n::Int, slots, context::_WriterContext)
    t = f.type::AC.FixedSizeListType
    child = only(f.children)
    present = f.nullable ? falses(n) : nothing
    sequences = Pair{Int,Any}[]
    for slot in slots
        index, value = first(slot), last(slot)
        if value === missing
            f.nullable || throw(
                ArgumentError(
                    "retained field $(f.name) is non-nullable but received a null value",
                ),
            )
            continue
        end
        present === nothing || (present[index] = true)
        push!(sequences, index => value)
    end

    childlength64 = _writerhiddenmul(f, n, t.listsize, "fixed-list child")
    childlength64 <= typemax(Int) || throw(
        ArgumentError(
            "retained fixed-size-list child $(f.name) exceeds the supported length range",
        ),
    )
    childlength = Int(childlength64)
    childfield, childdata = if child.type isa AC.NullType
        _constructhiddenpart(child, childlength, context)
    else
        childslots = (
            Base.checked_add(
                Base.checked_mul(Int64(index - 1), Int64(t.listsize)),
                Int64(childindex),
            ) => item for (index, sequence) in sequences for
            (childindex, item) in enumerate(sequence)
        )
        _constructplainfixednullpart(child, childlength, childslots, context)
    end
    nullcount = present === nothing ? 0 : n - length(sequences)
    buffers =
        AC.BufferSlice[present === nothing ? AC.BufferSlice() : AC._bitmapbuffer(present),]
    data = AC.ArrayData(t, n, buffers; children=AC.ArrayData[childdata], nullcount)
    return _retainedfield(f; children=AC.Field[childfield]), data
end

"Construct a sparse child whose visible fixed-list values have only Null storage."
function _constructcompactfixednull(f::AC.Field, values, context::_WriterContext)
    _writercompactfixednull(f) ||
        throw(ArgumentError("retained fixed-size-list field $(f.name) is not Null-only"))
    plain = _writerplainfixednullshape(f)
    plain && return _constructplainfixednullpart(f, values, context; routed=true)
    visible = _writerstoragevector(f)
    present = Vector{Bool}(undef, length(values))
    for (i, value) in enumerate(values)
        if value === _RETAINED_HIDDEN
            present[i] = true
            continue
        end
        value isa _WriterRoutedUnion || throw(
            ArgumentError("retained sparse Union child $(f.name) lost its writer route"),
        )
        present[i] = value.value !== missing
        _writerpushstorage!(visible, value.value, f, value.writertype, context)
    end
    # Visible rows are validated then discarded; see _writercompactfixednull.
    isempty(visible) || _constructwriterstorage(f, visible, context)
    return _constructfixednullpart(f, present, context)
end

function _arrowtypeswriterstoragevalue(
    context::_WriterContext,
    f::AC.Field,
    value,
    writetype::Type,
)
    if f.type isa AC.UnionType
        # A single logical type may lower to a physical Union. Keep that case
        # distinct from a declared logical Union: the former must be lowered
        # before routing, while the latter uses its logical branches to retain
        # branch identity (including same-storage extension types).
        logicaltype = Base.nonmissingtype(writetype)
        logicalstorage = _writerstoragetype!(context, logicaltype)
        lowers_to_union = isconcretetype(logicaltype) && logicalstorage isa Union
        storagevalue = value
        storagetype = writetype
        if lowers_to_union
            if value !== missing
                T = typeof(value)
                storagevalue =
                    T === VersionNumber ? string(value) :
                    _writerneedstype!(context, T) ?
                    _writertoarrow(context, value, T, f.name) : value
            end
            storagetype =
                Missing <: writetype ? Union{Missing,logicalstorage} : logicalstorage
            storagevalue = _writerconvertlowered(
                storagetype,
                storagevalue,
                context,
                logicaltype,
                f.name,
            )
        elseif !(writetype isa Union)
            throw(
                ArgumentError(
                    "writer type $writetype does not describe retained Union field " *
                    "$(f.name)",
                ),
            )
        end
        variants, children = _writerunionplan!(
            context,
            f,
            storagetype;
            extension_shape=lowers_to_union &&
                            _writerextensionshape(context, logicaltype, logicalstorage),
        )
        branch = _writerunionbranch!(context, storagetype, typeof(storagevalue), variants)
        child = children[branch]
        return _WriterRoutedUnion(
            child,
            _arrowtypeswriterchildstoragevalue(
                context,
                f.children[child],
                storagevalue,
                variants[branch],
            ),
            typeof(value),
        )
    end
    if writetype isa Union
        variants = _writerunionvariants!(context, writetype)
        branch = _writerunionbranch!(context, writetype, typeof(value), variants)
        writetype = variants[branch]
    end
    value === missing && return missing
    T = typeof(value)
    value = if T === VersionNumber
        string(value)
    elseif _writerneedstype!(context, T)
        _writertoarrow(context, value, T, f.name, f)
    else
        value
    end
    storagewritetype =
        T === VersionNumber ? String : _writerstoragetype!(context, writetype)
    # Validate the complete lowered shape before recursive Field access. A
    # malformed Struct/List/Map result must fail at the ArrowTypes seam instead
    # of leaking a BoundsError or unrelated property/index exception.
    value = _writerconvertlowered(storagewritetype, value, context, T, f.name)
    t = f.type
    t isa AC.NullType && value === nothing && return missing
    if t isa AC.RunEndEncodedType
        length(f.children) == 2 || throw(
            ArgumentError("writer REE field $(f.name) needs run-end and value children"),
        )
        return _arrowtypeswriterchildstoragevalue(
            context,
            f.children[2],
            value,
            storagewritetype,
        )
    end
    if _istemporalconv(t)
        storage = _arrowtypestemporalstorage(t, value)
        storage === nothing || return storage
        throw(
            ArgumentError(
                "registered writer value $(repr(value)) for field $(f.name) cannot " *
                "be represented exactly by retained Arrow type $(repr(t))",
            ),
        )
    end
    t isa AC.DecimalType &&
        value isa DataDecimals.AbstractDecimal &&
        return _shareddecimalraw(t, value)
    t isa AC.IntervalType && value isa _IntervalValue && return _sharedintervalraw(t, value)
    if t isa Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType}
        child = only(f.children)
        childtype = eltype(storagewritetype)
        return Any[
            _arrowtypeswriterchildstoragevalue(context, child, x, childtype) for x in value
        ]
    elseif t isa AC.StructType
        fieldcount(storagewritetype) == length(f.children) || throw(
            ArgumentError(
                "writer type $storagewritetype does not match retained Struct field " *
                "$(f.name)",
            ),
        )
        names = value isa NamedTuple ? _arrowtypesstructnames(f) : nothing
        out = Pair{String,Any}[]
        for (i, child) in enumerate(f.children)
            childvalue = if value isa NamedTuple
                getproperty(value, names[i])
            elseif value isa Tuple
                getfield(value, i)
            elseif value isa AbstractVector
                kv = value[i]
                kv isa Pair && String(first(kv)) == child.name || throw(
                    ArgumentError(
                        "writer Struct field $(f.name) has incompatible child $i",
                    ),
                )
                last(kv)
            else
                getfield(value, i)
            end
            childtype = fieldtype(storagewritetype, i)
            push!(
                out,
                child.name => _arrowtypeswriterchildstoragevalue(
                    context,
                    child,
                    childvalue,
                    childtype,
                ),
            )
        end
        return out
    elseif t isa AC.MapType
        entries = only(f.children)
        keyfield, valuefield = entries.children
        keytype = Base.keytype(storagewritetype)
        valuetype = Base.valtype(storagewritetype)
        return Pair{Any,Any}[
            _arrowtypeswriterchildstoragevalue(context, keyfield, first(kv), keytype) =>
                _arrowtypeswriterchildstoragevalue(
                    context,
                    valuefield,
                    last(kv),
                    valuetype,
                ) for kv in value
        ]
    end
    return value
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

function _registeredvalueplan!(context::_WriterContext, f::AC.Field, value, writetype::Type)
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    target === nothing && return nothing
    logical = Base.nonmissingtype(writetype)
    (value === missing ? logical <: target : value isa target) || return nothing
    bytype = get!(context.registered, f) do
        Dict{Tuple{Type,Type},_RegisteredWriterPlan}()
    end
    runtime = typeof(value)
    # Every value of a fully declared type has the same complete routing plan.
    # Abstract declarations still need one plan per inferred runtime type.
    evidence = _writerfullydeclared(logical) ? logical : runtime
    return get!(bytype, (writetype, evidence)) do
        values = Vector{writetype}(undef, 1)
        values[1] = value
        _registeredwriterplan(context, f, values)::_RegisteredWriterPlan
    end
end

function _deferredregisteredwriter(
    context::_WriterContext,
    f::AC.Field,
    value,
    writetype::Type,
)
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    target === nothing && return nothing
    logical = Base.nonmissingtype(writetype)
    (value === missing ? logical <: target : value isa target) || return nothing
    value === missing ||
        _writerfullydeclared(logical) ||
        _writerregisterinferred!(context, f, typeof(value))
    context.deferred = true
    return _DeferredRegisteredWriter(f, value, writetype)
end

function _writerconvertedvalue(
    context::_WriterContext,
    f::AC.Field,
    value,
    writetype::Type,
    routes::Union{Nothing,_WriterTypeRoutes},
)
    routes === nothing && return _arrowtypeswriterstoragevalue(context, f, value, writetype)
    runtime = typeof(value)
    route = _writerroute(routes, runtime)
    if route > 0
        route <= length(f.children) ||
            throw(ArgumentError("writer route $route is outside retained Field $(f.name)"))
        return _WriterRoutedUnion(
            route,
            _arrowtypeswriterstoragevalue(context, f.children[route], value, runtime),
            runtime,
        )
    end
    runtimewriter =
        f.type isa AC.UnionType && Missing <: writetype && runtime !== Missing ?
        Union{Missing,runtime} : runtime
    return _arrowtypeswriterstoragevalue(context, f, value, runtimewriter)
end

function _arrowtypeswriterchildstoragevalue(
    context::_WriterContext,
    f::AC.Field,
    value,
    writetype::Type,
)
    if context.collecting
        deferred = _deferredregisteredwriter(context, f, value, writetype)
        deferred === nothing || return deferred
    end
    plan = _registeredvalueplan!(context, f, value, writetype)
    plan === nothing && return _arrowtypeswriterstoragevalue(context, f, value, writetype)
    return _writerconvertedvalue(context, f, value, plan.input, plan.routes)
end

function _withwritercollection(f, context::_WriterContext)
    collecting = context.collecting
    deferred = context.deferred
    context.collecting = true
    context.deferred = false
    try
        value = f()
        return value, context.deferred
    finally
        context.collecting = collecting
        context.deferred = deferred
    end
end

function _writerresolvedeferredlevel(
    context::_WriterContext,
    value::_DeferredRegisteredWriter,
)
    plan = _registeredvalueplan!(context, value.field, value.value, value.writetype)
    plan === nothing && return _arrowtypeswriterstoragevalue(
        context,
        value.field,
        value.value,
        value.writetype,
    )
    return _writerconvertedvalue(context, value.field, value.value, plan.input, plan.routes)
end

function _writerresolvedeferredlevel(context::_WriterContext, value::_WriterRoutedUnion)
    _writerhasdeferred(value.value) || return value
    return _WriterRoutedUnion(
        value.child,
        _writerresolvedeferredlevel(context, value.value),
        value.writertype,
    )
end

function _writerresolvedeferredlevel(context::_WriterContext, value::Pair)
    _writerhasdeferred(value) || return value
    return _writerresolvedeferredlevel(context, first(value)) =>
        _writerresolvedeferredlevel(context, last(value))
end

function _writerresolvedeferredlevel(context::_WriterContext, values::AbstractVector)
    _writerhasdeferred(values) || return values
    for i in eachindex(values)
        values[i] = _writerresolvedeferredlevel(context, values[i])
    end
    return values
end

_writerresolvedeferredlevel(::_WriterContext, value) = value

_writerhasdeferred(::_DeferredRegisteredWriter) = true
_writerhasdeferred(value::_WriterRoutedUnion) = _writerhasdeferred(value.value)
_writerhasdeferred(value::Pair) =
    _writerhasdeferred(first(value)) || _writerhasdeferred(last(value))
_writerhasdeferred(values::AbstractVector) = any(_writerhasdeferred, values)
_writerhasdeferred(value) = false

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
    stored = _writerconvertlowered(eltype(storage), value, context, writertype, f.name)
    push!(storage, stored)
    return nothing
end

abstract type _MaskedLeafProjection end

struct _NativeMaskedLeafProjection <: _MaskedLeafProjection
    field::AC.Field
    context::_WriterContext
end

struct _RegisteredMaskedLeafProjection{R} <: _MaskedLeafProjection
    field::AC.Field
    writertype::Type
    routes::R
    context::_WriterContext
end

function _maskedleafvalue(
    projection::_NativeMaskedLeafProjection,
    ::Type{S},
    value,
) where {S}
    value === missing && return missing
    stored = _retainedleafstorage(projection.field, value, projection.context)
    return _writerconvertlowered(S, stored, projection.context, S, projection.field.name)
end

function _maskedleafvalue(
    projection::_RegisteredMaskedLeafProjection,
    ::Type{S},
    value,
) where {S}
    if projection.routes === nothing &&
       projection.writertype === S &&
       !_writerneedstype!(projection.context, S)
        value === missing && return missing
        return _writerconvertlowered(S, value, projection.context, S, projection.field.name)
    end
    lowered = _writerconvertedvalue(
        projection.context,
        projection.field,
        value,
        projection.writertype,
        projection.routes,
    )
    lowered === missing && return missing
    return _writerconvertlowered(
        S,
        lowered,
        projection.context,
        projection.writertype,
        projection.field.name,
    )
end

function _maskedleafnullerror(f::AC.Field)
    throw(
        ArgumentError(
            "column $(f.name) holds missing values but its retained field is non-nullable",
        ),
    )
end

function _maskedleafvalidity(f::AC.Field, n::Int)
    f.nullable || return nothing, 0
    bytes = zeros(UInt8, AC.expected_validity_bytes(Int64(n)))
    return bytes, n
end

function _maskedleafpresent!(validity, index::Int, nullcount::Int)
    validity === nothing && return nullcount
    AC._setbitmapbit!(validity, index)
    return nullcount - 1
end

function _maskedleafvaliditybuffer(validity, nullcount::Int)
    validity === nothing && return AC.BufferSlice()
    nullcount == 0 && return AC.BufferSlice()
    return AC._databuffer(validity)
end

function _maskedleafarray(
    f::AC.Field,
    n::Int,
    validity,
    databuffer::AC.BufferSlice,
    nullcount::Int,
)
    buffers = AC.BufferSlice[_maskedleafvaliditybuffer(validity, nullcount), databuffer]
    return f, AC.ArrayData(f.type, n, buffers; nullcount)
end

function _constructmaskedboolleaf(
    f::AC.Field,
    values::V,
    projection::_MaskedLeafProjection,
) where {V<:_MaskedChildValues}
    n = length(values)
    data = zeros(UInt8, AC.expected_validity_bytes(Int64(n)))
    validity, nullcount = _maskedleafvalidity(f, n)
    for index in eachindex(values)
        raw = values[index]
        raw === _RETAINED_HIDDEN && continue
        stored = _maskedleafvalue(projection, Bool, raw)
        if stored === missing
            f.nullable || _maskedleafnullerror(f)
            continue
        end
        nullcount = _maskedleafpresent!(validity, index, nullcount)
        stored && AC._setbitmapbit!(data, index)
    end
    return _maskedleafarray(f, n, validity, AC._databuffer(data), nullcount)
end

function _constructmaskedfixedleaf(
    f::AC.Field,
    values::V,
    projection::_MaskedLeafProjection,
    ::Type{S},
) where {S,V<:_MaskedChildValues}
    n = length(values)
    placeholder = _writerhiddenstorage(f; forcevalid=true)
    storedplaceholder = _writerconvertlowered(S, placeholder, projection.context, S, f.name)
    data = fill(storedplaceholder, n)
    validity, nullcount = _maskedleafvalidity(f, n)
    for index in eachindex(values)
        raw = values[index]
        raw === _RETAINED_HIDDEN && continue
        stored = _maskedleafvalue(projection, S, raw)
        if stored === missing
            f.nullable || _maskedleafnullerror(f)
            continue
        end
        data[index] = stored
        nullcount = _maskedleafpresent!(validity, index, nullcount)
    end
    return _maskedleafarray(f, n, validity, AC._databuffer(data), nullcount)
end

function _constructmaskedvarbytesleaf(
    f::AC.Field,
    values::V,
    projection::_MaskedLeafProjection,
    ::Type{S},
) where {S,V<:_MaskedChildValues}
    t = f.type::Union{AC.Utf8Type,AC.BinaryType}
    return _constructmaskedvarbytesleaf(f, values, projection, S, t.large ? Int64 : Int32)
end

function _constructmaskedvarbytesleaf(
    f::AC.Field,
    values::V,
    projection::_MaskedLeafProjection,
    ::Type{S},
    ::Type{Offset},
) where {S,Offset<:Union{Int32,Int64},V<:_MaskedChildValues}
    t = f.type::Union{AC.Utf8Type,AC.BinaryType}
    n = length(values)
    offsets = Vector{Offset}(undef, n + 1)
    offsets[1] = zero(Offset)
    data = UInt8[]
    validity, nullcount = _maskedleafvalidity(f, n)
    for index in eachindex(values)
        raw = values[index]
        if raw !== _RETAINED_HIDDEN
            stored = _maskedleafvalue(projection, S, raw)
            if stored === missing
                f.nullable || _maskedleafnullerror(f)
            else
                bytes = if t isa AC.Utf8Type
                    stored isa AbstractString || throw(
                        ArgumentError("column $(f.name) must contain string values"),
                    )
                    _utf8units(stored)
                else
                    stored isa AbstractVector{UInt8} || throw(
                        ArgumentError("column $(f.name) must contain byte-vector values"),
                    )
                    stored
                end
                length(data) <= typemax(Offset) - length(bytes) || throw(
                    ArgumentError(
                        "column $(f.name) data exceeds its retained offset width",
                    ),
                )
                append!(data, bytes)
                nullcount = _maskedleafpresent!(validity, index, nullcount)
            end
        end
        offsets[index + 1] = Offset(length(data))
    end
    databuffer = isempty(data) ? AC.BufferSlice() : AC._databuffer(data)
    buffers = AC.BufferSlice[
        _maskedleafvaliditybuffer(validity, nullcount),
        AC._databuffer(offsets),
        databuffer,
    ]
    return f, AC.ArrayData(t, n, buffers; nullcount)
end

"Bulk-fill mixed visible and parent-hidden composite slots in physical storage."
function _constructmaskedleaf(f::AC.Field, values, projection::_MaskedLeafProjection)
    values isa _MaskedChildValues || return nothing
    isempty(f.children) || return nothing
    f.type isa AC.DictionaryType && return nothing
    any(row -> row === missing || row === _RETAINED_HIDDEN, values.rows) || return nothing
    T = _declaredeltype(f, false)
    T === Any && return nothing
    S = Base.nonmissingtype(T)
    S === Union{} && return nothing
    f.type isa AC.BoolType &&
        S === Bool &&
        return _constructmaskedboolleaf(f, values, projection)
    f.type isa Union{AC.Utf8Type,AC.BinaryType} &&
        return _constructmaskedvarbytesleaf(f, values, projection, S)
    spec = AC.layoutspec_of(f.type)
    isbitstype(S) && spec.fixedwidth > 0 && sizeof(S) == spec.fixedwidth || return nothing
    return _constructmaskedfixedleaf(f, values, projection, S)
end

"Bulk-fill composite slots hidden by parent validity at a physical leaf."
function _constructmaskedwriterleaf(
    f::AC.Field,
    writetype::Type,
    values,
    routes,
    context::_WriterContext,
)
    # The masked-leaf fast path writes one physical child. A nonzero route
    # means the value belongs under a Union child, which this path cannot
    # express.
    (routes === nothing || all(iszero, Base.values(routes))) || return nothing
    projection = _RegisteredMaskedLeafProjection(f, writetype, routes, context)
    masked = _constructmaskedleaf(f, values, projection)
    masked === nothing || return masked
    values isa _MaskedChildValues || return nothing
    isempty(f.children) || return nothing
    f.type isa AC.DictionaryType && return nothing
    any(row -> row === missing || row === _RETAINED_HIDDEN, values.rows) || return nothing
    storage = _writerstoragevector(f)
    eltype(storage) === Any && return nothing

    hidden = _writerhiddenvalue(f, writetype)
    storedhidden =
        _writerconvertlowered(eltype(storage), hidden, context, writetype, f.name)
    resize!(storage, length(values))
    fill!(storage, storedhidden)
    for index in eachindex(values)
        value = values[index]
        value === _RETAINED_HIDDEN && continue
        lowered = _writerconvertedvalue(context, f, value, writetype, routes)
        storage[index] =
            _writerconvertlowered(eltype(storage), lowered, context, writetype, f.name)
    end
    return _constructwriterstorage(f, storage, context)
end

function _constructwriterchild(
    f::AC.Field,
    writetype::Type,
    values;
    routes::Union{Nothing,_WriterTypeRoutes}=nothing,
    context::_WriterContext=_WriterContext(),
)
    masked = _constructmaskedwriterleaf(f, writetype, values, routes, context)
    masked === nothing || return masked
    hashidden = any(value -> value === _RETAINED_HIDDEN, values)
    hidden = hashidden ? _writerhiddenvalue(f, writetype) : nothing
    prepared, deferred = _withwritercollection(context) do
        out = Any[]
        sizehint!(out, length(values))
        for value in values
            push!(
                out,
                value === _RETAINED_HIDDEN ? hidden :
                _writerconvertedvalue(context, f, value, writetype, routes),
            )
        end
        out
    end
    rounds = 0
    while deferred
        rounds < _MAX_WRITER_SCHEMA_DEPTH || throw(
            ArgumentError(
                "registered writer values exceed the supported nested planning " *
                "depth $_MAX_WRITER_SCHEMA_DEPTH for column $(f.name)",
            ),
        )
        rounds += 1
        prepared, deferred = _withwritercollection(context) do
            Any[_writerresolvedeferredlevel(context, value) for value in prepared]
        end
    end
    converted = _writerstoragevector(f)
    sizehint!(converted, length(values))
    for value in prepared
        _writerpushstorage!(converted, value, f, writetype, context)
    end
    return _constructwriterstorage(f, converted, context)
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

function _arrowtypesfixedlistcolumn(
    name::String,
    v::AbstractVector,
    kind;
    extension_shape::Bool,
    context,
)
    N = ArrowTypes.getsize(kind)
    E = ArrowTypes.gettype(kind)
    E === UInt8 && return _fixedbinarypart(name, v, N)
    present = Bool[x !== missing for x in v]
    for row in v
        row === missing ||
            length(row) == N ||
            throw(
                ArgumentError(
                    "ArrowTypes fixed-list column $name expected $N values per row",
                ),
            )
    end
    masked = if any(!, present)
        emptychildfield, _ = _arrowtypeschildcolumn("item", E[]; extension_shape, context)
        t = AC.FixedSizeListType(N)
        emptyfield = AC.Field(
            name,
            t;
            nullable=Missing <: eltype(v),
            children=AC.Field[emptychildfield],
        )
        if _writercompactfixednull(emptyfield)
            # Only visible rows need validation; see _writercompactfixednull.
            if _writerplainfixednullshape(emptyfield)
                return _constructplainfixednullpart(emptyfield, v, context)
            else
                visible = E[]
                nvisible = count(identity, present)
                sizehint!(visible, Base.checked_mul(nvisible, N))
                for row in v
                    row === missing || append!(visible, row)
                end
                isempty(visible) ||
                    _constructwriterchild(emptychildfield, E, visible; context)
            end
            return _constructfixednullpart(emptyfield, present, context)
        end
        values = _MaskedFixedListValues(v, N)
        _constructwriterchild(emptychildfield, E, values; context)
    else
        nothing
    end
    if masked === nothing
        flat = E[]
        sizehint!(flat, Base.checked_mul(length(v), N))
        for row in v
            append!(flat, row)
        end
        childfield, childdata =
            _arrowtypeschildcolumn("item", flat; extension_shape, context)
    else
        childfield, childdata = masked
    end
    t = AC.FixedSizeListType(N)
    field = AC.Field(name, t; nullable=Missing <: eltype(v), children=AC.Field[childfield])
    data = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present)];
        children=AC.ArrayData[childdata],
        nullcount=count(!, present),
    )
    return field, data
end

"A fixed_size_binary(N) column from `NTuple{N,UInt8}` rows."
function _fixedbinarypart(name::String, v::AbstractVector, N::Int)
    T = NTuple{N,UInt8}
    t = AC.FixedSizeBinaryType(N)
    field = AC.Field(name, t; nullable=Missing <: eltype(v))
    if eltype(v) === T
        data = something(_fixedbuffer(v), AC._databuffer(collect(T, v)))
        return field, AC.ArrayData(t, length(v), [AC.BufferSlice(), data]; nullcount=0)
    end
    rows, validity, nc = _nullablevalues(T, v, ntuple(_ -> 0x00, N))
    return field, AC.ArrayData(t, length(v), [validity, AC._databuffer(rows)]; nullcount=nc)
end

function _arrowtypesstructcolumn(
    name::String,
    v::AbstractVector,
    S;
    extension_shape::Bool,
    context,
)
    _arrowtypesstorageisspecified(context, S) || throw(
        ArgumentError(
            "ArrowTypes StructKind column $name lowered to an underspecified type $S; " *
            "give every struct field a writable storage type",
        ),
    )
    names = fieldnames(S)
    nchildren = fieldcount(S)
    present = Bool[x !== missing for x in v]
    childfields = AC.Field[]
    childdata = AC.ArrayData[]
    hasnull = any(!, present)
    for j = 1:nchildren
        FT = fieldtype(S, j)
        childname = string(names[j])
        # A null parent does not make its children nullable; the parent's
        # validity bitmap masks their slots. Build hidden slots from the
        # resolved child Field in the storage domain — no logical default
        # constructors, no duplicated variable payloads.
        cf, cd = if hasnull
            emptychildfield, _ =
                _arrowtypeschildcolumn(childname, FT[]; extension_shape, context)
            values = _freshstructchildvalues(Any, v, j)
            _constructwriterchild(emptychildfield, FT, values; context)
        else
            values = _freshstructchildvalues(FT, v, j)
            _arrowtypeschildcolumn(childname, values; extension_shape, context)
        end
        push!(childfields, cf)
        push!(childdata, cd)
    end
    t = AC.StructType()
    field = AC.Field(name, t; nullable=Missing <: eltype(v), children=childfields)
    data = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present)];
        children=childdata,
        nullcount=count(!, present),
    )
    return field, data
end

function _arrowtypesmapcolumn(
    name::String,
    v::AbstractVector,
    S;
    extension_shape::Bool,
    context,
)
    S <: AbstractDict || throw(
        ArgumentError("ArrowTypes MapKind column $name must lower to AbstractDict rows"),
    )
    K = keytype(S)
    V = valtype(S)
    keys = K[]
    values = V[]
    present = Bool[x !== missing for x in v]
    offsets = Vector{Int32}(undef, length(v) + 1)
    offsets[1] = 0
    for (i, row) in enumerate(v)
        if row !== missing
            row isa AbstractDict || throw(
                ArgumentError(
                    "ArrowTypes MapKind column $name contains a non-dictionary row",
                ),
            )
            length(keys) <= typemax(Int32) - length(row) || throw(
                ArgumentError(
                    "ArrowTypes MapKind column $name exceeds the Int32 offset range",
                ),
            )
            for (key, value) in row
                push!(keys, key)
                push!(values, value)
            end
        end
        offsets[i + 1] = Int32(length(keys))
    end
    any(ismissing, keys) && throw(ArgumentError("Arrow Map keys cannot be missing"))
    keyfield, keydata = _arrowtypeschildcolumn("key", keys; extension_shape, context)
    keyfield.nullable &&
        throw(ArgumentError("Arrow Map keys must have a non-nullable type"))
    _validatemapphysicalkeys(name, keyfield, keydata, offsets)
    valuefield, valuedata =
        _arrowtypeschildcolumn("value", values; extension_shape, context)
    entriesfield = AC.Field(
        "entries",
        AC.StructType();
        nullable=false,
        children=AC.Field[keyfield, valuefield],
    )
    entriesdata = AC.ArrayData(
        AC.StructType(),
        length(keys),
        [AC.BufferSlice()];
        children=AC.ArrayData[keydata, valuedata],
        nullcount=0,
    )
    t = AC.MapType(false)
    field =
        AC.Field(name, t; nullable=Missing <: eltype(v), children=AC.Field[entriesfield])
    data = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), AC._databuffer(offsets)];
        children=AC.ArrayData[entriesdata],
        nullcount=count(!, present),
    )
    return field, data
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
function _fieldcontractequal(a::AC.Field, b::AC.Field)
    a.name == b.name || return false
    AC.typeequal(a.type, b.type) || return false
    a.nullable == b.nullable || return false
    _fieldmetadata(a) == _fieldmetadata(b) || return false
    length(a.children) == length(b.children) || return false
    for i in eachindex(a.children)
        _fieldcontractequal(a.children[i], b.children[i]) || return false
    end
    return true
end

"""
Assemble a fresh Int32-indexed dictionary Field and its first batch from
already-built pool data.
"""
# The narrowest signed index type for `n` categories (indices 0 to n - 1).
_dictindextype(n::Integer) =
    n <= 128 ? AC.IntType(8, true) :
    n <= 32_768 ? AC.IntType(16, true) :
    n - 1 <= typemax(Int32) ? AC.IntType(32, true) : AC.IntType(64, true)

# Whether a column's categories are ordered: a CategoricalArray's flag (the
# CategoricalArrays extension), else false.
_isordered(::AbstractVector) = false
_isordered(x::SubArray) = _isordered(parent(x))
_isordered(x::DictEncode) = _isordered(x.data)

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

"Construct a fresh dictionary's candidate categories before index encoding."
function _constructnewdictpooldata(
    name::String,
    pool::Vector;
    valuefield::Union{Nothing,AC.Field}=nothing,
    writetype::Union{Nothing,Type}=nothing,
    routes::Union{Nothing,_WriterTypeRoutes}=nothing,
    context::_WriterContext=_WriterContext(),
)
    builtfield, valuedata =
        valuefield === nothing || writetype === nothing ?
        _constructpart(name, pool; context) :
        _constructwriterchild(valuefield, writetype, pool; routes, context)
    if valuefield !== nothing
        _fieldcontractequal(valuefield, builtfield) || throw(
            ArgumentError(
                "column $name dictionary pool maps to a different Arrow Field " *
                "than its partition values; keep the declared value types and " *
                "ArrowTypes metadata identical across the column",
            ),
        )
        # The inferred value Field stays the schema; the writer adapter only
        # supplied matching fresh ArrayData, so this is not retained
        # reconstruction.
        builtfield = valuefield
    end
    return builtfield, valuedata
end

# --- retained-schema rewrite (facade Table/Stream round-trips) --------------

"Storage integers for a public column under a RETAINED temporal descriptor."
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

"Enforce the first partition's complete recursive Field contract."
function _checkpartitionfield(
    expected::AC.Field,
    actual::AC.Field,
    partition::Int,
    column::Symbol;
    path::String=String(column),
)
    expected.name == actual.name || throw(
        ArgumentError(
            "partition $partition column $column has child name $(repr(actual.name)) " *
            "at $path, but the first partition declared $(repr(expected.name))",
        ),
    )
    AC.typeequal(expected.type, actual.type) || throw(
        ArgumentError(
            "partition $partition column $column maps $path to Arrow type " *
            "$(repr(actual.type)), but the first partition declared " *
            "$(repr(expected.type)); make the column types agree across partitions",
        ),
    )
    expectedmeta = something(_fieldmetadata(expected), Pair{String,String}[])
    actualmeta = something(_fieldmetadata(actual), Pair{String,String}[])
    expectedmeta == actualmeta || throw(
        ArgumentError(
            "partition $partition column $column has different ordered metadata " *
            "at $path; the first partition declared $(repr(expectedmeta)), but " *
            "this partition declared $(repr(actualmeta))",
        ),
    )
    actual.nullable &&
        !expected.nullable &&
        throw(
            ArgumentError(
                "partition $partition column $column is nullable at $path, but " *
                "the first partition declared it non-nullable; make the first " *
                "partition's corresponding element type admit Missing to widen " *
                "the schema",
            ),
        )
    length(expected.children) == length(actual.children) || throw(
        ArgumentError(
            "partition $partition column $column has $(length(actual.children)) " *
            "children at $path, but the first partition declared " *
            "$(length(expected.children))",
        ),
    )
    for i in eachindex(expected.children)
        expectedchild = expected.children[i]
        actualchild = actual.children[i]
        childpath = "$path.$(expectedchild.name)[$i]"
        _checkpartitionfield(expectedchild, actualchild, partition, column; path=childpath)
    end
    return nothing
end

# The reader takes arrow.bool8 as Bool only on int8 storage; on other
# storage the label is kept and the values are written as they read.
_isbool8(f::AC.Field) =
    f.type == AC.IntType(8, true) &&
    _fieldmetavalue(f, _EXTENSION_NAME_KEY) == "arrow.bool8"
_bool8storage(v::AbstractVector) =
    Missing <: eltype(v) ? Union{Missing,Int8}[x isa Bool ? Int8(x) : x for x in v] :
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
    any(!, present) &&
        !f.nullable &&
        throw(
            ArgumentError(
                "column $(f.name) holds missing values but its retained field is non-nullable",
            ),
        )
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
                Missing <: eltype(v),
                pool,
            )
        else
            V = utf8 ? AbstractString : AbstractVector{UInt8}
            Base.nonmissingtype(eltype(v)) <: V ||
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
            payloads[i] = DataStrings.PAYLOAD_MISSING
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
    if Base.nonmissingtype(eltype(v)) === NTuple{t.nbytes,UInt8}
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
    if eltype(v) === D
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
    D = Base.nonmissingtype(eltype(v))
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

function _retainedtypedvalues(f::AC.Field, values; converted::Bool)
    T = _declaredeltype(f, converted)
    T === Any && return Any[x for x in values]
    # A value of another accepted writer type (the `NTuple` bytes of a typed
    # struct row's fixed-size binary field) widens the vector instead of
    # being converted to the declared type.
    S = T
    for x in values
        x isa T && continue
        _retainedwritertype(f.type, typeof(x)) || throw(
            ArgumentError(
                "column $(f.name) holds $(typeof(x)) values that do not match " *
                "its retained Arrow type $(repr(f.type))",
            ),
        )
        S = Base.promote_typejoin(S, typeof(x))
    end
    return S[x for x in values]
end

"""
One visible leaf value in its retained Field's storage domain. Public values
lower through the facade authority, or the column refuses as replaced data.
Hidden-slot placeholders are storage values and never pass through here.
"""
function _retainedleafstorage(f::AC.Field, value, context::_WriterContext)
    (context.lowered || value === missing) && return value
    t = f.type
    t isa AC.DecimalType &&
        value isa DataDecimals.AbstractDecimal &&
        return _shareddecimalraw(t, value)
    t isa AC.IntervalType && value isa _IntervalValue && return _sharedintervalraw(t, value)
    _istemporalconv(t) || return value
    stored = _exactfacadescalar(t, value)
    stored === nothing && throw(
        ArgumentError(
            "column $(f.name) holds $(typeof(value)) values that do not match " *
            "its retained Arrow type $(repr(t)); the column was replaced with " *
            "incompatible data",
        ),
    )
    return stored
end

"A physical child value hidden by a null composite parent."
function _retainedplaceholder(f::AC.Field; forcevalid::Bool=false)
    f.nullable && !forcevalid && return missing
    t = f.type
    if t isa AC.NullType
        forcevalid && throw(
            ArgumentError(
                "retained Null field $(f.name) cannot synthesize a non-null placeholder",
            ),
        )
        return missing
    end
    t isa Union{
        AC.IntType,
        AC.FloatType,
        AC.DateType,
        AC.TimeType,
        AC.TimestampType,
        AC.DurationType,
    } && return zero(AC.juliatype(t))
    t isa AC.BoolType && return false
    t isa AC.Utf8Type && return ""
    t isa AC.BinaryType && return UInt8[]
    t isa AC.FixedSizeBinaryType &&
        return t.nbytes <= _MAX_ARROWTYPE_EXACT_ARITY ? ntuple(_ -> 0x00, t.nbytes) :
               zeros(UInt8, t.nbytes)
    t isa AC.ViewType && return t.utf8 ? "" : UInt8[]
    if t isa AC.DecimalType
        return t.bits == 32 ? Int32(0) :
               t.bits == 64 ? Int64(0) : zeros(UInt8, AC.primwidth(t))
    end
    if t isa AC.IntervalType
        return t.unit == AC.YEAR_MONTH ? Int32(0) :
               t.unit == AC.DAY_TIME ? (days=Int32(0), millis=Int32(0)) :
               (months=Int32(0), days=Int32(0), nanos=Int64(0))
    end
    t isa Union{AC.ListType,AC.ListViewType} && return Any[]
    if t isa AC.FixedSizeListType
        length(f.children) == 1 ||
            throw(ArgumentError("retained fixed-size list $(f.name) needs one child"))
        return Any[_retainedplaceholder(f.children[1]) for _ = 1:(t.listsize)]
    end
    if t isa AC.StructType
        return Pair{String,Any}[
            child.name => _retainedplaceholder(child) for child in f.children
        ]
    end
    t isa AC.MapType && return Pair{Any,Any}[]
    if t isa AC.RunEndEncodedType
        length(f.children) == 2 || throw(
            ArgumentError("retained run-end encoded field $(f.name) needs two children"),
        )
        return _retainedplaceholder(f.children[2]; forcevalid=forcevalid || !f.nullable)
    end
    throw(
        ArgumentError(
            "cannot synthesize hidden child data for retained $(AC.descriptorname(t)) " *
            "field $(f.name)",
        ),
    )
end

_retainedcontainer(t::AC.ArrowType) =
    t isa Union{
        AC.ListType,
        AC.ListViewType,
        AC.FixedSizeListType,
        AC.StructType,
        AC.MapType,
        AC.RunEndEncodedType,
    }

"""
Write values materialized inside a composite. Facade rows carry public leaf
values; under `context.lowered` the values are already storage.
"""
function _retainedchildcolumn(f::AC.Field, values, context::_WriterContext)
    f.type isa AC.DictionaryType && throw(
        ArgumentError(
            "nested retained dictionary field $(f.name) cannot be reconstructed " *
            "after facade materialization because its pool is unavailable",
        ),
    )
    all(value -> value === _RETAINED_HIDDEN, values) &&
        return _constructhiddenpart(f, length(values), context; inactive=true)
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    inferredplan =
        target === nothing ? nothing :
        _inferredstorageplan(f.name, AbstractVector[values], context; target, retained=f)
    inferredplan === nothing || return _constructinferredretainedpart(
        f,
        inferredplan,
        only(inferredplan.parts),
        context,
    )
    registeredwriter = _registeredwriterplan(context, f, values)
    registeredwriter === nothing || return _constructwriterchild(
        f,
        registeredwriter.input,
        values;
        routes=registeredwriter.routes,
        context,
    )
    routed = any(x -> x isa _WriterRoutedUnion, values)
    allhiddenunion = f.type isa AC.UnionType && all(x -> x === _RETAINED_HIDDEN, values)
    if routed || allhiddenunion
        hidden = _writerhiddenstorage(f; inactive=true)
        prepared = Any[x === _RETAINED_HIDDEN ? hidden : x for x in values]
        return _constructpart(f, prepared; context)
    end
    # Public union rows route to their children in `_constructretainedpart`.
    f.type isa AC.UnionType && !context.lowered && return _constructpart(f, values; context)
    if any(x -> x === _RETAINED_HIDDEN, values)
        if _retainedcontainer(f.type)
            # Preserve hidden state until recursive construction reaches a leaf.
            return _constructpart(f, values; context)
        end
        masked = _constructmaskedleaf(f, values, _NativeMaskedLeafProjection(f, context))
        masked === nothing || return masked
        hidden = _writerhiddenstorage(f; inactive=true)
        storage = _writerstoragevector(f)
        sizehint!(storage, length(values))
        for value in values
            push!(
                storage,
                value === _RETAINED_HIDDEN ? hidden :
                _retainedleafstorage(f, value, context),
            )
        end
        return _constructwriterstorage(f, storage, context)
    end
    _retainedcontainer(f.type) && return _constructpart(f, values; context)
    v = _retainedtypedvalues(f, values; converted=(!context.lowered))
    context.lowered && return _constructwriterstorage(f, v, context)
    return _constructpart(f, v; context)
end

function _retainedlist(f::AC.Field, v::AbstractVector, context::_WriterContext)
    t = f.type::Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType}
    length(f.children) == 1 ||
        throw(ArgumentError("column $(f.name) retained list descriptor needs one child"))
    shape = _tensorshape(f)
    shape === nothing || (v = _tensorstorage(v, shape))
    present = _retainedvalidity(f, v)
    t isa AC.FixedSizeListType &&
        all(row -> row === missing || row === _RETAINED_HIDDEN, v) &&
        return _constructhiddenpart(f, length(v), context)
    if t isa AC.FixedSizeListType && _writercompactfixednull(f)
        # Hidden rows cost child length only; see _writercompactfixednull.
        plain = _writerplainfixednullshape(f)
        plain && return _constructplainfixednullpart(f, v, context)
        visible = Any[]
        for row in v
            row === missing && continue
            row === _RETAINED_HIDDEN && continue
            row isa Union{AbstractVector,Tuple} || throw(
                ArgumentError(
                    "column $(f.name) retained fixed-size list rows must be vectors",
                ),
            )
            length(row) == t.listsize || throw(
                ArgumentError(
                    "column $(f.name) fixed-size list row has $(length(row)) " *
                    "values; expected $(t.listsize)",
                ),
            )
            append!(visible, row)
        end
        isempty(visible) || _retainedchildcolumn(only(f.children), visible, context)
        return _constructfixednullpart(f, present, context)
    end
    flat = Any[]
    if t isa AC.ListType
        Offset = t.large ? Int64 : Int32
        offsets = Vector{Offset}(undef, length(v) + 1)
        offsets[1] = zero(Offset)
        for (i, row) in enumerate(v)
            if row !== missing && row !== _RETAINED_HIDDEN
                row isa AbstractVector || throw(
                    ArgumentError("column $(f.name) retained list rows must be vectors"),
                )
                length(flat) <= typemax(Offset) - length(row) || throw(
                    ArgumentError(
                        "column $(f.name) child data exceeds its retained offset width",
                    ),
                )
                append!(flat, row)
            end
            offsets[i + 1] = Offset(length(flat))
        end
        buffers = AC.BufferSlice[AC._bitmapbuffer(present), AC._databuffer(offsets)]
    elseif t isa AC.ListViewType
        Offset = t.large ? Int64 : Int32
        offsets = Vector{Offset}(undef, length(v))
        sizes = Vector{Offset}(undef, length(v))
        for (i, row) in enumerate(v)
            if row === missing || row === _RETAINED_HIDDEN
                offsets[i] = zero(Offset)
                sizes[i] = zero(Offset)
                continue
            end
            row isa AbstractVector || throw(
                ArgumentError("column $(f.name) retained list-view rows must be vectors"),
            )
            length(flat) <= typemax(Offset) - length(row) || throw(
                ArgumentError(
                    "column $(f.name) child data exceeds its retained offset width",
                ),
            )
            offsets[i] = Offset(length(flat))
            sizes[i] = Offset(length(row))
            append!(flat, row)
        end
        buffers = AC.BufferSlice[
            AC._bitmapbuffer(present),
            AC._databuffer(offsets),
            AC._databuffer(sizes),
        ]
    else
        for row in v
            if row === missing || row === _RETAINED_HIDDEN
                continue
            end
            row isa Union{AbstractVector,Tuple} || throw(
                ArgumentError(
                    "column $(f.name) retained fixed-size list rows must be vectors",
                ),
            )
            length(row) == t.listsize || throw(
                ArgumentError(
                    "column $(f.name) fixed-size list row has $(length(row)) " *
                    "values; expected $(t.listsize)",
                ),
            )
        end
        # Expose hidden child spans on demand. The child constructor still
        # emits every descriptor-required physical slot, but it no longer
        # allocates one Any reference per hidden slot before doing so.
        flat = _MaskedFixedListValues(v, t.listsize)
        buffers = AC.BufferSlice[AC._bitmapbuffer(present)]
    end
    childfield, childdata = _retainedchildcolumn(f.children[1], flat, context)
    d = AC.ArrayData(
        t,
        length(v),
        buffers;
        children=AC.ArrayData[childdata],
        nullcount=count(!, present),
    )
    return _retainedfield(f; children=AC.Field[childfield]), d
end

function _validateretainedstructrows(f::AC.Field, rows::AbstractVector)
    nchildren = length(f.children)
    for row in rows
        row === missing && continue
        row === _RETAINED_HIDDEN && continue
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

function _retainedstruct(f::AC.Field, v::AbstractVector, context::_WriterContext)
    t = f.type::AC.StructType
    present = _retainedvalidity(f, v)
    nchildren = length(f.children)
    _validateretainedstructrows(f, v)
    children = AC.ArrayData[]
    childfields = AC.Field[]
    for j = 1:nchildren
        values = _retainedstructchildvalues(v, j)
        cf, cd = _retainedchildcolumn(f.children[j], values, context)
        push!(childfields, cf)
        push!(children, cd)
    end
    d = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present)];
        children=children,
        nullcount=count(!, present),
    )
    return _retainedfield(f; children=childfields), d
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

function _retainedmap(f::AC.Field, v::AbstractVector, context::_WriterContext)
    t = f.type::AC.MapType
    length(f.children) == 1 || throw(
        ArgumentError("column $(f.name) retained map descriptor needs one entries child"),
    )
    entries = f.children[1]
    entries.type isa AC.StructType && length(entries.children) == 2 ||
        throw(ArgumentError("column $(f.name) retained map entries descriptor is invalid"))
    present = _retainedvalidity(f, v)
    offsets = Vector{Int32}(undef, length(v) + 1)
    offsets[1] = 0
    keys = Any[]
    values = Any[]
    for (i, row) in enumerate(v)
        if row !== missing && row !== _RETAINED_HIDDEN
            row isa AbstractVector || throw(
                ArgumentError("column $(f.name) retained map rows must be Pair vectors"),
            )
            length(keys) <= typemax(Int32) - length(row) || throw(
                ArgumentError("column $(f.name) map entries exceed the Int32 offset range"),
            )
            for kv in row
                kv isa Pair || throw(
                    ArgumentError("column $(f.name) retained map rows must contain Pairs"),
                )
                key = first(kv)
                key === missing && throw(
                    ArgumentError("column $(f.name) retained Map keys cannot be missing"),
                )
                push!(keys, key)
                push!(values, last(kv))
            end
        end
        offsets[i + 1] = Int32(length(keys))
    end
    keyfield, keydata = _retainedchildcolumn(entries.children[1], keys, context)
    valuefield, valuedata = _retainedchildcolumn(entries.children[2], values, context)
    _validatemapphysicalkeys(f.name, keyfield, keydata, offsets)
    if t.keyssorted
        for row = 1:length(v)
            lo = Int(offsets[row]) + 1
            hi = Int(offsets[row + 1])
            lo >= hi && continue
            previous = AC.getvalue(keyfield, keydata, Int64(lo))
            for index = (lo + 1):hi
                key = AC.getvalue(keyfield, keydata, Int64(index))
                _writermapkeyisless(f, key, previous) && throw(
                    ArgumentError(
                        "column $(f.name) row $row is not sorted by its physical " *
                        "key storage but its retained Map type declares sorted keys",
                    ),
                )
                previous = key
            end
        end
    end
    entriesfield = _retainedfield(entries; children=AC.Field[keyfield, valuefield])
    entriesdata = AC.ArrayData(
        entries.type,
        length(keys),
        [AC.BufferSlice()];
        children=AC.ArrayData[keydata, valuedata],
        nullcount=0,
    )
    d = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), AC._databuffer(offsets)];
        children=[entriesdata],
        nullcount=count(!, present),
    )
    return _retainedfield(f; children=AC.Field[entriesfield]), d
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
    rebuiltvaluefield, valuedata = _retainedchildcolumn(valuefield, runvalues, context)
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

"Construct a retained dense or sparse Union from writer-side branch routes."
function _constructwriterunion(f::AC.Field, v::AbstractVector, context::_WriterContext)
    t = f.type::AC.UnionType
    length(t.typeids) == length(f.children) || throw(
        ArgumentError("writer-routed Union field $(f.name) has invalid child metadata"),
    )
    active = t.mode == AC.SparseMode ? falses(length(f.children)) : nothing
    if active !== nothing
        for x in v
            x isa _WriterRoutedUnion || throw(
                ArgumentError(
                    "column $(f.name) has a retained UnionType whose child type ids " *
                    "cannot be recovered from materialized facade values",
                ),
            )
            1 <= x.child <= length(f.children) || throw(
                ArgumentError("writer Union route $(x.child) is outside field $(f.name)"),
            )
            active[x.child] = true
        end
    end
    compact =
        active === nothing ? nothing :
        Bool[active[i] && _writercompactfixednull(f.children[i]) for i in eachindex(active)]
    childvalues = AbstractVector[
        compact !== nothing && compact[i] ? Any[] : _writerstoragevector(f.children[i])
        for i in eachindex(f.children)
    ]
    t.mode == AC.SparseMode && foreach(values -> sizehint!(values, length(v)), childvalues)
    typeids = Vector{Int8}(undef, length(v))
    offsets = t.mode == AC.DenseMode ? Vector{Int32}(undef, length(v)) : nothing
    # Sparse Union: every active child needs a value in every slot. nothing
    # marks children with no rows at all; _RETAINED_HIDDEN marks compacted
    # Null-only children, whose hidden rows cost length only.
    hidden = if active === nothing || isempty(v)
        nothing
    else
        Any[
            !active[i] ? nothing :
            compact[i] ? _RETAINED_HIDDEN :
            _writerhiddenstorage(f.children[i]; inactive=true) for
            i in eachindex(active)
        ]
    end
    for (i, x) in enumerate(v)
        x isa _WriterRoutedUnion || throw(
            ArgumentError(
                "column $(f.name) has a retained UnionType whose child type ids " *
                "cannot be recovered from materialized facade values",
            ),
        )
        1 <= x.child <= length(f.children) ||
            throw(ArgumentError("writer Union route $(x.child) is outside field $(f.name)"))
        typeids[i] = t.typeids[x.child]
        if t.mode == AC.DenseMode
            child = childvalues[x.child]
            length(child) <= typemax(Int32) || throw(
                ArgumentError(
                    "writer Union field $(f.name) exceeds the Int32 offset range",
                ),
            )
            offsets[i] = Int32(length(child))
            _writerpushstorage!(child, x.value, f.children[x.child], x.writertype, context)
        else
            for childindex in eachindex(childvalues)
                active[childindex] || continue
                if compact[childindex]
                    push!(
                        childvalues[childindex],
                        childindex == x.child ? x : _RETAINED_HIDDEN,
                    )
                else
                    storagevalue = childindex == x.child ? x.value : hidden[childindex]
                    _writerpushstorage!(
                        childvalues[childindex],
                        storagevalue,
                        f.children[childindex],
                        x.writertype,
                        context,
                    )
                end
            end
        end
    end
    childfields = AC.Field[]
    childdata = AC.ArrayData[]
    for i in eachindex(f.children)
        childfield = f.children[i]
        rebuiltfield, rebuiltdata = if active !== nothing && !active[i]
            _constructhiddenpart(childfield, length(v), context; inactive=true)
        elseif compact !== nothing && compact[i]
            _constructcompactfixednull(childfield, childvalues[i], context)
        else
            _constructwriterstorage(childfield, childvalues[i], context)
        end
        push!(childfields, rebuiltfield)
        push!(childdata, rebuiltdata)
    end
    field = AC.Field(
        f.name,
        t;
        nullable=f.nullable,
        metadata=_fieldmetadata(f),
        children=childfields,
    )
    buffers =
        t.mode == AC.DenseMode ?
        AC.BufferSlice[AC._databuffer(typeids), AC._databuffer(offsets)] :
        AC.BufferSlice[AC._databuffer(typeids)]
    data = AC.ArrayData(t, length(v), buffers; children=childdata, nullcount=0)
    return field, data
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
    # Typed nested rows of the field's layout (an Arrow.Table's, say) build as
    # a fresh column and keep the field when the layouts agree. Values already
    # lowered to storage (`context.lowered`) rebuild under the field instead.
    t = f.type
    kind =
        t isa AC.ListType ? :list :
        t isa AC.FixedSizeListType ? :fixedlist :
        t isa AC.StructType ? :struct : t isa AC.MapType ? :map : nothing
    R = Base.nonmissingtype(_eltypeof(v))
    if kind !== nothing &&
       !context.lowered &&
       _nestedkind(context, R) === kind &&
       !_untypedrow(R)
        built = _nestedpart(f.name, v, context)
        if built !== nothing
            field = _asretained(f, first(built))
            field === nothing || return field, last(built)
        end
    end
    _, target, _ = _arrowtypestarget(context.arrowtypes, f)
    inferredplan =
        target === nothing ? nothing :
        _inferredstorageplan(f.name, AbstractVector[v], context; target, retained=f)
    inferredplan === nothing || return _constructinferredretainedpart(
        f,
        inferredplan,
        only(inferredplan.parts),
        context,
    )
    registeredwriter = _registeredwriterplan(context, f, v)
    registeredwriter === nothing || return _constructwriterchild(
        f,
        registeredwriter.input,
        v;
        routes=registeredwriter.routes,
        context,
    )
    return _constructretainedpart(f, f.type, v, context)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.NullType,
    v::AbstractVector,
    ::_WriterContext,
)
    eltype(v) === Missing || throw(
        ArgumentError(
            "column $(f.name) no longer matches its retained Arrow NullType; " *
            "give it a Missing element type",
        ),
    )
    d = AC.ArrayData(t, length(v), AC.BufferSlice[]; nullcount=length(v))
    return AC.Field(f.name, t; nullable=f.nullable, metadata=_fieldmetadata(f)), d
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
        NT = Base.nonmissingtype(eltype(v))
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
        eltype(v) >: Missing &&
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

function _constructretainedpart(
    f::AC.Field,
    t::Union{AC.DateType,AC.TimestampType,AC.TimeType,AC.DurationType},
    v::AbstractVector,
    ::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _rebuildtemporal(f, _retainedstorage(t, v, f.name))
end

function _constructretainedpart(
    f::AC.Field,
    t::Union{AC.Utf8Type,AC.BinaryType},
    v::AbstractVector,
    context::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedvarbytes(f, v, context.pool)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.ViewType,
    v::AbstractVector,
    ::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedview(f, v)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.FixedSizeBinaryType,
    v::AbstractVector,
    ::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedfixedbytes(f, v)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.DecimalType,
    v::AbstractVector,
    ::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retaineddecimal(f, v)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.IntervalType,
    v::AbstractVector,
    ::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedinterval(f, v)
end

function _constructretainedpart(
    f::AC.Field,
    t::Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType},
    v::AbstractVector,
    context::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedlist(f, v, context)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.StructType,
    v::AbstractVector,
    context::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedstruct(f, v, context)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.MapType,
    v::AbstractVector,
    context::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedmap(f, v, context)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.RunEndEncodedType,
    v::AbstractVector,
    context::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    return _retainedree(f, v, context)
end

function _constructretainedpart(
    f::AC.Field,
    t::AC.UnionType,
    v::AbstractVector,
    context::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    all(x -> x isa _WriterRoutedUnion, v) && return _constructwriterunion(f, v, context)
    built = context.lowered ? nothing : _constructfacadeunion(f, v, context)
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

function _constructretainedpart(
    f::AC.Field,
    t::AC.ArrowType,
    v::AbstractVector,
    context::_WriterContext,
)
    _checkretainedidentity(f, t, v)
    # Build the column naturally, then impose the retained descriptor: types
    # must match and nullability comes from the retained field. Missing values
    # under a non-nullable field mean the column was replaced. For lists this
    # applies recursively — child names, nullability, metadata, and each
    # level's width are part of retained identity.
    fn, dn = _constructpart(f.name, v; context)
    AC.typeequal(fn.type, t) || throw(
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
    rebuilt = AC.Field(
        f.name,
        fn.type;
        nullable=f.nullable,
        metadata=_fieldmetadata(f),
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

"Merge retained dictionary snapshots without changing the first pool's order."
struct _MergedDictionaryPool{V<:AbstractVector}
    values::V
    retainedprefix::Int
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

function _mergeddictpool(poolhints; widen::Bool=false)
    hints = Any[pool for pool in poolhints if pool !== nothing]
    isempty(hints) && return nothing
    values = _mergecategorypools(Any[_hintvalues(h) for h in hints]; widen)
    return _MergedDictionaryPool(values, _hintprefix(first(hints)))
end

# Encoding identity belongs to the lowered Arrow storage domain. These
# wrappers provide structural hashing and equality without calling a logical
# value's overloadable `hash`, `isequal`, `==`, or `isless` methods. Dictionary
# pool dedup, dictionary compaction, and run-end compaction share this rule.
struct _WriterStorageKey{T}
    value::T
end

_writerstorageroute(value::_WriterRoutedUnion) = (value.child, value.value)
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

"Declared common value type for a newly inferred dictionary pool."
function _dictvaluetype(vals)
    T = Union{}
    for v in vals
        V = Base.nonmissingtype(eltype(v))
        V === Union{} || (T = typejoin(T, V))
    end
    return T
end

"One value pool: keep a retained prefix exactly and append new categories."
function _dictionarypool(vals, retainedpool; writetype=nothing)
    if retainedpool === nothing
        T = writetype === nothing ? _dictvaluetype(vals) : writetype
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
            _widenedtype(eltype(retainedpool), _dictvaluetype(vals)),
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
    for v in vals
        _addcategories!(pool, seen, v)
    end
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

struct _DictionaryEvidence
    field::AC.Field
    writetype::Type
    inferred::Bool
    routes::_WriterTypeRoutes
    observed::Vector{Type}
end

function _directunionroutes(T::Type)
    routes = _WriterTypeRoutes()
    for (i, variant) in enumerate(_checkedwritervariants("writer route type $T", T))
        routes[variant] = i
    end
    return routes
end

function _mergewriterroutes(left::_WriterTypeRoutes, right::_WriterTypeRoutes)
    routes = copy(left)
    for (T, route) in right
        prior = get(routes, T, route)
        prior == route || throw(
            ArgumentError(
                "writer type $T has conflicting dictionary routes $prior and $route",
            ),
        )
        routes[T] = route
    end
    return routes
end

function _observeddictionarytypes(name::String, values)
    types = Type[]
    seen = Base.IdSet{Type}()
    for value in values
        value === missing && continue
        T = typeof(value)
        T in seen && continue
        length(types) < _MAX_INFERRED_WRITER_TYPES || throw(
            ArgumentError(
                "dictionary column $name has more than " *
                "$_MAX_INFERRED_WRITER_TYPES inferred runtime value types",
            ),
        )
        push!(seen, T)
        push!(types, T)
    end
    return types
end

"One Field inferred from the exact runtime writer types in abstract values."
function _observeddictionaryevidence(
    name::String,
    writetype::Type,
    context::_WriterContext,
    observed::Vector{Type}=_writertypevariants(writetype),
)
    length(observed) <= _MAX_INFERRED_WRITER_TYPES || throw(
        ArgumentError(
            "dictionary column $name has more than " *
            "$_MAX_INFERRED_WRITER_TYPES inferred runtime value types",
        ),
    )
    variants = _writertypevariants(writetype)
    fields = AC.Field[]
    for variant in variants
        push!(fields, _namedwriterfield(name, _writercandidatefield!(context, variant)))
    end
    firstfield = first(fields)
    if all(field -> _fieldcontractequal(firstfield, field), fields)
        # Runtime types are observational evidence, not a declared Union.
        # Collapse them when they share one exact Arrow Field.
        routes = _WriterTypeRoutes()
        for variant in variants
            routes[variant] = 0
        end
        return _DictionaryEvidence(firstfield, writetype, true, routes, observed)
    end
    field = _namedwriterfield(name, _writercandidatefield!(context, writetype))
    return _DictionaryEvidence(
        field,
        writetype,
        true,
        _directunionroutes(writetype),
        observed,
    )
end

"A fresh dictionary partition's non-null value Field, or no usable evidence."
function _dictionaryevidence(
    name::String,
    values::AbstractVector,
    context::_WriterContext,
    observed::Union{Nothing,Vector{Type}}=nothing,
)
    declared = Base.nonmissingtype(eltype(values))
    if !isconcretetype(declared) && !(declared isa Union)
        # Empty/all-missing abstract vectors cannot identify ArrowTypes metadata
        # (for example, a parametric logical tag). Let a later concrete partition
        # provide the schema instead of manufacturing an incomplete one.
        writetype = _writeruniontype(
            observed === nothing ? _observeddictionarytypes(name, values) : observed,
        )
        writetype === Union{} && return nothing
        return _observeddictionaryevidence(name, writetype, context)
    end
    # Field evidence is type-derived. Constructing row data here would lower
    # every repeated category once for each row before the unique pool exists.
    field, _ = _constructpart(name, declared[]; context)
    routes = if declared isa Union
        _directunionroutes(declared)
    else
        routes = _WriterTypeRoutes()
        routes[declared] = 0
        routes
    end
    return _DictionaryEvidence(field, declared, false, routes, Type[])
end

"Common fresh dictionary value Field and writer type across all partitions."
function _dictionaryvaluefield(
    name::String,
    values,
    column::Symbol,
    context::_WriterContext,
)
    allobserved = Type[]
    observations = Vector{Union{Nothing,Vector{Type}}}(undef, length(values))
    for (partition, part) in enumerate(values)
        declared = Base.nonmissingtype(eltype(part))
        if !isconcretetype(declared) && !(declared isa Union)
            observations[partition] = _observeddictionarytypes(name, part)
            allobserved = _mergewritertypes(
                "dictionary column $name",
                allobserved,
                observations[partition],
                _MAX_INFERRED_WRITER_TYPES,
                "inferred runtime value types",
            )
        else
            observations[partition] = nothing
        end
    end
    authority = nothing
    for (partition, part) in enumerate(values)
        evidence = _dictionaryevidence(name, part, context, observations[partition])
        evidence === nothing && continue
        if authority === nothing
            authority = evidence
        else
            observed = _mergewritertypes(
                "dictionary column $name",
                authority.observed,
                evidence.observed,
                _MAX_INFERRED_WRITER_TYPES,
                "inferred runtime value types",
            )
            variants = _mergewritertypes(
                "dictionary column $name",
                _writertypevariants(authority.writetype),
                _writertypevariants(evidence.writetype),
                _MAX_WRITER_UNION_BRANCHES,
                "writer types",
            )
            writetype = _writeruniontype(variants)
            if authority.inferred && evidence.inferred
                authority = _observeddictionaryevidence(name, writetype, context, observed)
                continue
            end
            _checkpartitionfield(authority.field, evidence.field, partition, column)
            authority = _DictionaryEvidence(
                authority.field,
                writetype,
                false,
                _mergewriterroutes(authority.routes, evidence.routes),
                observed,
            )
        end
    end
    return authority
end

"First pool position for each non-null value; duplicate categories stay intact."
function _dictionarylookup(pool)
    # Same `_WriterStorageKey` policy as `_dictionarypool`: no user-overloadable
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
function _writerstoredvalue(value::_ArrowTypesRoutedUnion)
    return _WriterRoutedUnion(
        value.child,
        _writerstoredvalue(value.value),
        typeof(value.value),
    )
end
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
    Base.nonmissingtype(eltype(x)) !== Union{}
_hasrefpool(::Any) = false

"""
The values and category pool of a ref-pool column. Pool values that are
wrappers without an ArrowTypes.jl mapping (CategoricalArrays' values) unwrap
through `DataAPI.unwrap`; a mapped element type keeps its values so its
ArrowTypes.jl lowering applies.
"""
function _refpoolvalues(x::AbstractVector)
    T = Base.nonmissingtype(eltype(x))
    mapped = ArrowTypes.ArrowType(T) !== T || ArrowTypes.arrowname(T) !== Symbol()
    pool = DataAPI.refpool(x)
    unwrap = !mapped && any(v -> v !== missing && DataAPI.unwrap(v) !== v, pool)
    values = unwrap ? [DataAPI.unwrap(v) for v in x] : x
    hint = Base.nonmissingtype(eltype(values))[]
    # Iterate: CategoricalArrays' ref pool is indexed from zero.
    for v in pool
        v === missing || push!(hint, unwrap ? DataAPI.unwrap(v) : v)
    end
    return values, hint
end

"""
Construct one logical column across all table partitions.

This is the sole column-policy seam used by the write facade. It owns fresh
inference, retained reconstruction, ArrowTypes lowering, dictionary pooling,
partition agreement, field metadata, and the check that strings are UTF-8
(`_InvalidUTF8`). The returned data vector is in the same order as `parts`.

`retained` is the Field a rewrite must reproduce; `poolhints` carries one
retained dictionary-pool snapshot (or nothing) per partition, enforced below;
`metadata` is explicit application metadata merged onto the final Field.
"""
function _constructcolumn(name::Symbol, parts::Vector{AbstractVector}; kwargs...)
    try
        field, data = _constructcolumnparts(name, parts; kwargs...)
        _checkutf8(name, field, data)
        return field, data
    catch err
        prefix = "fromjulia: unsupported element type "
        msg = err isa ArgumentError ? err.msg : nothing
        msg isa String && startswith(msg, prefix) || rethrow()
        T = _reflectedstructtype(parts)
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
function _reflectedstructtype(parts)
    T = Base.nonmissingtype(_eltypeof(first(parts)))
    isconcretetype(T) && isstructtype(T) && !(T <: Union{NamedTuple,Tuple}) ||
        return nothing
    ArrowTypes.ArrowType(T) === T && ArrowTypes.arrowname(T) === Symbol() || return nothing
    return ArrowTypes.ArrowKind(T) isa ArrowTypes.StructKind ? T : nothing
end

function _constructcolumnparts(
    name::Symbol,
    parts::Vector{AbstractVector};
    retained::Union{Nothing,AC.Field}=nothing,
    poolhints::Vector{Any}=Any[nothing for _ in parts],
    metadata::Union{Nothing,Vector{Pair{String,String}}}=nothing,
    refpools::Bool=true,
    stringview::Bool=false,
    largelists::Bool=false,
    growable::Bool=false,
    pool=nothing,
)
    isempty(parts) && throw(ArgumentError("column $name has no partitions"))
    length(poolhints) == length(parts) || throw(
        ArgumentError(
            "column $name has $(length(poolhints)) dictionary-pool hints for " *
            "$(length(parts)) partitions",
        ),
    )
    if retained !== nothing && _isbool8(retained)
        # arrow.bool8 reads as Bool; its storage is Int8 0 or 1.
        int8parts = AbstractVector[_bool8storage(p) for p in parts]
        _, int8data = _constructcolumnparts(
            name,
            int8parts;
            retained=_withoutownextension(retained),
            metadata,
        )
        return _withfieldmetadata(_retainedfield(retained), metadata), int8data
    end
    fieldname = String(name)
    dictintent =
        (retained !== nothing && retained.type isa AC.DictionaryType) ||
        any(part isa DictEncode for part in parts) ||
        (refpools && retained === nothing && any(_hasrefpool, parts))
    # A dictionary's pool outlives its batch, so it never takes reused buffers.
    context =
        _WriterContext(fieldname; stringview, largelists, pool=dictintent ? nothing : pool)
    # A ref-pool column written as its values still unwraps them.
    if !dictintent && any(_hasrefpool, parts)
        parts = AbstractVector[_hasrefpool(p) ? first(_refpoolvalues(p)) : p for p in parts]
    end
    # Built by calls this function does not infer (see `_uninferred`).
    field, data =
        dictintent ?
        _uninferred(
            _dictcolumnparts,
            name,
            fieldname,
            parts,
            poolhints,
            retained,
            growable,
            context,
        )::Tuple{AC.Field,Vector{AC.ArrayData}} :
        _uninferred(
            _plaincolumnparts,
            name,
            fieldname,
            parts,
            retained,
            context,
        )::Tuple{AC.Field,Vector{AC.ArrayData}}
    return _withfieldmetadata(field, metadata), data
end

"A dictionary column of `_constructcolumnparts`: its field and data per partition."
function _dictcolumnparts(name, fieldname, parts, poolhints, retained, growable, context)
    nparts = length(parts)
    data = Vector{AC.ArrayData}(undef, nparts)
    local field::AC.Field
    values = AbstractVector[
        part isa DictEncode ? (part::DictEncode).data : part for part in parts
    ]
    if any(_hasrefpool, values)
        poolhints = copy(poolhints)
        for k in eachindex(values)
            _hasrefpool(values[k]) || continue
            values[k], hint = _refpoolvalues(values[k])
            poolhints[k] === nothing && (poolhints[k] = hint)
        end
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

    inferreddeclaration = if retained === nothing
        _inferredstoragedeclaration(values, context)
    elseif retainedtarget === nothing
        nothing
    else
        _inferredstoragedeclaration(values, context; target=retainedtarget)
    end
    freshevidence =
        retained === nothing && inferreddeclaration === nothing ?
        _dictionaryvaluefield(fieldname, values, name, context) : nothing
    freshvaluefield = freshevidence === nothing ? nothing : freshevidence.field
    freshwritetype = freshevidence === nothing ? nothing : freshevidence.writetype
    freshroutes = freshevidence === nothing ? nothing : freshevidence.routes
    widenpool = if inferreddeclaration !== nothing
        true
    elseif retained === nothing
        freshvaluefield !== nothing && freshvaluefield.type isa AC.UnionType
    else
        (retained.type::AC.DictionaryType).valuetype isa AC.UnionType
    end
    mergedpool = _mergeddictpool(poolhints; widen=widenpool)
    poolhint = mergedpool === nothing ? nothing : mergedpool.values
    if retained !== nothing
        (retained.type::AC.DictionaryType).valuetype isa AC.UnionType &&
            retainedvaluefield === nothing &&
            throw(
                ArgumentError(
                    "column $name has a retained UnionType whose child type ids " *
                    "cannot be recovered from materialized facade values",
                ),
            )
        for valuespart in values
            T = Base.nonmissingtype(eltype(valuespart))
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
                        all(x -> x === missing || x isa retainedtarget, valuespart)
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
            any(ismissing, valuespart) &&
                !retained.nullable &&
                (poolhint === nothing || !any(ismissing, poolhint)) &&
                throw(
                    ArgumentError(
                        "column $name may hold missing values but its retained " *
                        "dictionary field is non-nullable",
                    ),
                )
        end
        (retained.type::AC.DictionaryType).ordered &&
            poolhint === nothing &&
            throw(
                ArgumentError(
                    "column $name has an ordered retained dictionary, but " *
                    "its original category pool is unavailable",
                ),
            )
    end

    poolwritetype =
        inferreddeclaration === nothing ? freshwritetype : first(inferreddeclaration)
    pool = _dictionarypool(values, poolhint; writetype=poolwritetype)
    inferredplan =
        inferreddeclaration === nothing ? nothing :
        _inferredstorageplan(
            fieldname,
            AbstractVector[pool],
            context;
            target=first(inferreddeclaration),
            retained=retainedvaluefield,
        )
    lookup = _dictionarylookup(pool)
    missingindex =
        retained !== nothing && !retained.nullable ? findfirst(ismissing, pool) : nothing
    missingindex === nothing || (missingindex = Int64(missingindex - 1))
    candidateindices = Vector{Union{Missing,Int64}}[
        _dictionaryindices(part, lookup, missingindex) for part in values
    ]
    candidatevaluefield, candidatevaluedata = if inferredplan !== nothing
        if retained === nothing
            _constructinferredfreshpart(
                fieldname,
                inferredplan,
                only(inferredplan.parts),
                context,
            )
        else
            _constructinferredretainedpart(
                retainedvaluefield,
                inferredplan,
                only(inferredplan.parts),
                context,
            )
        end
    elseif retained === nothing
        _constructnewdictpooldata(
            fieldname,
            pool;
            valuefield=freshvaluefield,
            writetype=freshwritetype,
            routes=freshroutes,
            context,
        )
    else
        retainedtype = retained.type::AC.DictionaryType
        constructionfield =
            retainedvaluefield === nothing ? AC.dictvaluefield(retained, retainedtype) :
            retainedvaluefield
        _constructpart(constructionfield, pool; context)
    end
    retainedprefix = mergedpool === nothing ? 0 : mergedpool.retainedprefix
    valuefield, valuedata, remap = _compactdictionarypool(
        candidatevaluefield,
        candidatevaluedata,
        retainedprefix,
        context,
    )
    indices = Vector{Union{Missing,Int64}}[
        _remapdictionaryindices(part, remap) for part in candidateindices
    ]
    if retained === nothing
        # The narrowest index type the categories need; a pool that later
        # tables may extend takes at least Int32, as pyarrow writes.
        indextype = _dictindextype(growable ? max(valuedata.len, 1 << 16) : valuedata.len)
        field, data[1] = _newdictfromdata(
            fieldname,
            valuefield,
            valuedata,
            indices[1];
            nullable=any(v -> Missing <: eltype(v), values),
            indextype,
            ordered=all(_isordered, parts),
        )
    else
        field, data[1] = _retaineddictfromdata(
            retained,
            valuefield,
            valuedata,
            Int(valuedata.len),
            indices[1],
            fieldname,
        )
    end
    pooldata = data[1].dictionary::AC.ArrayData
    for partition = 2:nparts
        data[partition] = _dictbatch(field, indices[partition], pooldata)
    end
    return field, data
end

"A non-dictionary column of `_constructcolumnparts`: its field and data per partition."
function _plaincolumnparts(name, fieldname, parts, retained, context)
    nparts = length(parts)
    data = Vector{AC.ArrayData}(undef, nparts)
    local field::AC.Field
    constructionparts, inferredplan, registeredwriter, narrowabstract = _uninferred(
        _plaincolumnplan,
        fieldname,
        parts,
        retained,
        context,
    )::Tuple{Vector{AbstractVector},Any,Any,Bool}
    for partition = 1:nparts
        partitionfield, data[partition] = try
            _uninferred(
                _constructcolumnpart,
                fieldname,
                constructionparts[partition],
                partition,
                retained,
                inferredplan,
                registeredwriter,
                narrowabstract,
                context,
            )::Tuple{AC.Field,AC.ArrayData}
        catch err
            (err isa _OffsetOverflow && err.part == 0) || rethrow()
            # The overflow row is a row of this column only at its top level.
            row = err.column == fieldname ? err.row : 0
            throw(_OffsetOverflow(err.column, partition, row))
        end
        if partition == 1
            field = partitionfield
        else
            _checkpartitionfield(field, partitionfield, partition, name)
        end
    end
    return field, data
end

"""
How `_plaincolumnparts` builds a column's partitions: the parts to build
(narrowed from abstract element types), the inferred storage plan, the
registered ArrowTypes writer plan, and whether the parts were left as given.
"""
function _plaincolumnplan(fieldname, parts, retained, context)
    narrowedparts =
        retained === nothing ? _narrowabstractwriterparts(fieldname, parts, context) :
        nothing
    constructionparts = narrowedparts === nothing ? parts : narrowedparts
    inferredplan = if retained === nothing
        _inferredstorageplan(fieldname, constructionparts, context)
    else
        _, target, _ = _arrowtypestarget(context.arrowtypes, retained)
        target === nothing ? nothing :
        _inferredstorageplan(fieldname, constructionparts, context; target, retained)
    end
    registeredwriter =
        retained === nothing || inferredplan !== nothing ? nothing :
        _registeredwritercolumnplan(context, retained, constructionparts)
    return constructionparts, inferredplan, registeredwriter, narrowedparts === nothing
end

"One partition of a non-dictionary column, by the plan `_constructcolumnparts` chose."
Base.@nospecializeinfer function _constructcolumnpart(
    fieldname::String,
    @nospecialize(part::AbstractVector),
    partition::Int,
    retained,
    inferredplan,
    registeredwriter,
    narrowabstract::Bool,
    context::_WriterContext,
)
    return if inferredplan !== nothing
        if retained === nothing
            _constructinferredfreshpart(
                fieldname,
                inferredplan,
                inferredplan.parts[partition],
                context,
            )
        else
            _constructinferredretainedpart(
                retained,
                inferredplan,
                inferredplan.parts[partition],
                context,
            )
        end
    elseif retained === nothing
        # A successful column-scoped narrowing pass already saw every
        # partition. Replanning each partition from its local values
        # can split one declared nullable Tuple child into incompatible
        # Int and Null Fields.
        _constructpart(fieldname, part; context, narrowabstract)
    elseif registeredwriter === nothing
        _constructpart(retained, part; context)
    else
        _constructwriterchild(
            retained,
            registeredwriter.input,
            part;
            routes=registeredwriter.routes,
            context,
        )
    end
end
