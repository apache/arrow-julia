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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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

function _arrowtypesfieldmetadata(f::AC.Field)
    return f.metadata === nothing ? nothing : collect(Pair{String,String}, f.metadata)
end

"Attach this logical type's extension label without disturbing child labels."
function _arrowtypeslogicalfield(f::AC.Field, T; nullable::Bool=f.nullable)
    ArrowTypes.hasarrowname(T) || return f
    extension = Pair{String,String}[
        _EXTENSION_NAME_KEY => String(ArrowTypes.arrowname(T)),
        _EXTENSION_METADATA_KEY => String(ArrowTypes.arrowmetadata(T)),
    ]
    metadata = _mergemetapairs(_arrowtypesfieldmetadata(f), extension)
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
    }

function _arrowtypesneedstype(T)
    T === Union{} && return false
    T === Missing && return false
    T === Any && return false
    if T isa Union
        members = Base.uniontypes(T)
        nonnull = count(!=(Missing), members)
        return nonnull > 1 || any(_arrowtypesneedstype, members)
    end
    NT = Base.nonmissingtype(T)
    NT !== T && return _arrowtypesneedstype(NT)
    ArrowTypes.ArrowType(T) !== T && return true
    ArrowTypes.hasarrowname(T) && return true
    T <: AbstractDict && return true
    T <: Tuple && return true
    if T <: NamedTuple
        return any(i -> _arrowtypesneedstype(fieldtype(T, i)), 1:fieldcount(T))
    end
    if T <: AbstractVector
        return _arrowtypesneedstype(eltype(T))
    end
    _arrowtypesnativetype(T) && return false
    # Restore ArrowTypes' plain-struct writer without routing native facade
    # values such as Dates through ArrowTypes' default StructKind.
    return isconcretetype(T) && !isprimitivetype(T)
end

function _arrowtypesneedsvalue(x)
    x === missing && return false
    _arrowtypesneedstype(typeof(x)) && return true
    if x isa NamedTuple || x isa Tuple
        return any(_arrowtypesneedsvalue, x)
    elseif x isa AbstractVector || x isa AbstractDict
        return any(_arrowtypesneedsvalue, x)
    end
    return false
end

function _arrowtypes_needs(v::AbstractVector)
    _arrowtypesneedstype(eltype(v)) && return true
    # Abstract/Any containers do not expose nested logical types. Inspect only
    # until the first ArrowTypes value is found; concrete columns take no scan.
    return (eltype(v) === Any || !isconcretetype(eltype(v))) &&
           any(_arrowtypesneedsvalue, v)
end

function _arrowtypes_writecolumn(
    name::String,
    v::AbstractVector;
    extension_shape::Bool=false,
)
    logical = Base.nonmissingtype(eltype(v))
    # `Base.nonmissingtype(Missing) === Union{}`. Handle a pure-null child
    # before trait dispatch: bottom is a subtype of every container type and
    # would otherwise enter an unrelated ArrowKind branch.
    eltype(v) === Missing && return _writecolumn_native(name, v)
    logical === Union{} && return _writecolumn_native(name, v)
    # VersionNumber's legacy reflected representation contains variable-length
    # tuples. String is an equivalent extension storage form: ArrowTypes'
    # one-argument JuliaType registration and default VersionNumber(String)
    # constructor read it in both Arrow 2.x and 3.x.
    if logical === VersionNumber
        mapped =
            eltype(v) >: Missing ?
            Union{Missing,String}[x === missing ? missing : string(x) for x in v] :
            String[string(x) for x in v]
        f, d = _writecolumn_native(name, mapped)
        return _arrowtypeslogicalfield(f, logical), d
    end
    declared = ArrowTypes.ArrowType(logical)
    mapped = ArrowTypes.ToArrow(v)
    kind = ArrowTypes.ArrowKind(logical)
    ownshape =
        declared !== logical ||
        ArrowTypes.hasarrowname(logical) ||
        (kind isa ArrowTypes.StructKind && !(logical <: Union{NamedTuple,Tuple}))
    f, d =
        _arrowtypesstoragecolumn(name, mapped; extension_shape=extension_shape || ownshape)
    nullable = f.type isa AC.NullType && !(Missing <: eltype(v)) ? false : f.nullable
    return _arrowtypeslogicalfield(f, logical; nullable=nullable), d
end

"One lowered value in the ordinary 3.0 materialized shape for a retained Field."
function _arrowtypesretainedvalue(ctx::_ArrowTypesReadContext, f::AC.Field, x)
    x === missing && return missing
    _, target, _ = _arrowtypestarget(ctx, f)
    if target !== nothing && x isa target
        x = target === VersionNumber ? string(x) : ArrowTypes.toarrow(x)
    end
    t = f.type
    if t isa AC.DictionaryType
        valuefield = _withoutownextension(f, t.valuetype; nullable=true)
        return _arrowtypesretainedvalue(ctx, valuefield, x)
    elseif t isa Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType}
        length(f.children) == 1 || return x
        child = f.children[1]
        return Any[_arrowtypesretainedvalue(ctx, child, y) for y in x]
    elseif t isa AC.StructType
        values = Pair{String,Any}[]
        for (i, child) in enumerate(f.children)
            y = if x isa NamedTuple
                getproperty(x, Symbol(child.name))
            elseif x isa Tuple
                getfield(x, i)
            elseif x isa AbstractVector
                kv = x[i]
                kv isa Pair && String(first(kv)) == child.name || throw(
                    ArgumentError(
                        "retained extension struct $(f.name) has incompatible rows",
                    ),
                )
                last(kv)
            else
                getfield(x, i)
            end
            push!(values, child.name => _arrowtypesretainedvalue(ctx, child, y))
        end
        return values
    elseif t isa AC.MapType
        length(f.children) == 1 || return x
        entries = f.children[1]
        length(entries.children) == 2 || return x
        keyfield, valuefield = entries.children
        return Pair{Any,Any}[
            _arrowtypesretainedvalue(ctx, keyfield, first(kv)) =>
                _arrowtypesretainedvalue(ctx, valuefield, last(kv)) for kv in x
        ]
    end
    return x
end

"Lower a registered logical column before enforcing its retained physical type."
function _arrowtypesretainedcolumn(f::AC.Field, v::AbstractVector)
    ctx = _ArrowTypesReadContext(; warn=false)
    _, target, _ = _arrowtypestarget(ctx, f)
    target === nothing && return v
    NT = Base.nonmissingtype(eltype(v))
    islogical = NT <: target || (NT === Any && all(x -> x === missing || x isa target, v))
    islogical || return v
    values = Any[_arrowtypesretainedvalue(ctx, f, x) for x in v]
    T = _declaredeltype(f, true)
    return T === Any ? map(identity, values) : T[x for x in values]
end

function _arrowtypesstoragecolumn(name::String, v::AbstractVector; extension_shape::Bool)
    eltype(v) === Missing && return _writecolumn_native(name, v)
    S = Base.nonmissingtype(eltype(v))
    S === Union{} && return _writecolumn_native(name, v)
    if S isa Union
        return _arrowtypesunioncolumn(name, v; extension_shape=extension_shape)
    end
    _arrowtypesnativetype(S) && return _writecolumn_native(name, v)
    S <: NamedTuple &&
        return _arrowtypesstructcolumn(name, v, S; extension_shape=extension_shape)
    kind = ArrowTypes.ArrowKind(S)
    if kind isa ArrowTypes.NullKind
        nulls = S === Nothing ? fill(missing, length(v)) : v
        return _writecolumn_native(name, nulls)
    end
    if kind isa ArrowTypes.FixedSizeListKind
        return _arrowtypesfixedlistcolumn(name, v, kind; extension_shape=extension_shape)
    elseif kind isa ArrowTypes.MapKind
        return _arrowtypesmapcolumn(name, v, S; extension_shape=extension_shape)
    elseif kind isa ArrowTypes.ListKind
        if S <: AbstractString
            return _writecolumn_native(name, v)
        end
        return _arrowtypeslistcolumn(name, v, S; extension_shape=extension_shape)
    elseif kind isa ArrowTypes.StructKind
        return _arrowtypesstructcolumn(name, v, S; extension_shape=extension_shape)
    elseif kind isa Union{ArrowTypes.PrimitiveKind,ArrowTypes.BoolKind}
        return _writecolumn_native(name, v)
    end
    throw(
        ArgumentError(
            "unsupported ArrowTypes storage kind $(typeof(kind)) for column $name",
        ),
    )
end

"Build a fresh dense Arrow Union from one concrete Julia Union element type."
function _arrowtypesunioncolumn(name::String, v::AbstractVector; extension_shape::Bool)
    variants = Type[Base.uniontypes(eltype(v))...]
    length(variants) <= 128 ||
        throw(ArgumentError("Arrow Union column $name has more than 128 child types"))
    isempty(variants) &&
        throw(ArgumentError("Arrow Union column $name has no concrete child type"))

    childvalues = Any[Vector{T}() for T in variants]
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

"Build one child while keeping unmarked nested facade conversions disabled."
function _arrowtypeschildcolumn(name::String, v::AbstractVector; extension_shape::Bool)
    T = Base.nonmissingtype(eltype(v))
    if _arrowtypes_needs(v) || (
        T !== Any && (
            T <: NamedTuple ||
            T <: Tuple ||
            T <: AbstractDict ||
            (T <: AbstractVector && !(T <: AbstractString))
        )
    )
        return _arrowtypes_writecolumn(name, v; extension_shape=extension_shape)
    end
    return extension_shape ? _writecolumn_native(name, v) :
           AC.fromjulia(name, _plainvector(v))
end

function _arrowtypeslistcolumn(name::String, v::AbstractVector, S; extension_shape::Bool)
    S <: AbstractVector || throw(
        ArgumentError("ArrowTypes ListKind column $name must lower to AbstractVector rows"),
    )
    E = eltype(S)
    flat = Vector{E}()
    present = Bool[x !== missing for x in v]
    offsets = Vector{Int32}(undef, length(v) + 1)
    offsets[1] = 0
    for (i, row) in enumerate(v)
        if row !== missing
            row isa AbstractVector || throw(
                ArgumentError("ArrowTypes ListKind column $name contains a non-vector row"),
            )
            length(flat) <= typemax(Int32) - length(row) || throw(
                ArgumentError(
                    "ArrowTypes ListKind column $name exceeds the Int32 offset range",
                ),
            )
            append!(flat, row)
        end
        offsets[i + 1] = Int32(length(flat))
    end
    childfield, childdata =
        _arrowtypeschildcolumn("item", flat; extension_shape=extension_shape)
    t = AC.ListType(false)
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

function _arrowtypesfixedlistcolumn(
    name::String,
    v::AbstractVector,
    kind;
    extension_shape::Bool,
)
    N = ArrowTypes.getsize(kind)
    E = ArrowTypes.gettype(kind)
    flat = Vector{E}()
    sizehint!(flat, Base.checked_mul(length(v), N))
    present = Bool[x !== missing for x in v]
    for row in v
        if row === missing
            append!(flat, (ArrowTypes.default(E) for _ = 1:N))
            continue
        end
        length(row) == N || throw(
            ArgumentError("ArrowTypes fixed-list column $name expected $N values per row"),
        )
        append!(flat, row)
    end
    childfield, childdata =
        _arrowtypeschildcolumn("item", flat; extension_shape=extension_shape)
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

function _arrowtypesstructcolumn(name::String, v::AbstractVector, S; extension_shape::Bool)
    isconcretetype(S) || throw(
        ArgumentError(
            "ArrowTypes StructKind column $name lowered to non-concrete type $S; " *
            "use one concrete struct storage type",
        ),
    )
    names = fieldnames(S)
    nchildren = fieldcount(S)
    present = Bool[x !== missing for x in v]
    childfields = AC.Field[]
    childdata = AC.ArrayData[]
    hasnull = any(!, present)
    firstpresent = hasnull ? findfirst(present) : nothing
    for j = 1:nchildren
        FT = fieldtype(S, j)
        # A null parent does not make its children nullable in the Arrow
        # schema. Child slots under a null parent are masked by the parent's
        # validity bitmap, so fill them with one declared-type value. Reuse a
        # present row when possible and require ArrowTypes.default only for an
        # entirely null parent column.
        values = if hasnull
            dummy = if firstpresent === nothing
                ArrowTypes.default(FT)
            else
                getfield(v[firstpresent], j)
            end
            FT[row === missing ? dummy : getfield(row, j) for row in v]
        else
            FT[getfield(row, j) for row in v]
        end
        cf, cd = _arrowtypeschildcolumn(
            string(names[j]),
            values;
            extension_shape=extension_shape,
        )
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

function _arrowtypesmapcolumn(name::String, v::AbstractVector, S; extension_shape::Bool)
    S <: AbstractDict || throw(
        ArgumentError("ArrowTypes MapKind column $name must lower to AbstractDict rows"),
    )
    K = keytype(S)
    V = valtype(S)
    keys = Vector{K}()
    values = Vector{V}()
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
    keyfield, keydata = _arrowtypeschildcolumn("key", keys; extension_shape=extension_shape)
    keyfield.nullable &&
        throw(ArgumentError("Arrow Map keys must have a non-nullable type"))
    valuefield, valuedata =
        _arrowtypeschildcolumn("value", values; extension_shape=extension_shape)
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

"Build a new dictionary column through the same recursive value adapter."
function _newdictcolumn(
    name::String,
    pool::Vector,
    indices::Vector;
    nullable::Bool=any(ismissing, indices),
)
    valuefield, valuedata = _writecolumn(name, pool)
    t = AC.DictionaryType(AC.IntType(32, true), valuefield.type, false)
    field = AC.Field(
        name,
        t;
        nullable=nullable,
        metadata=_arrowtypesfieldmetadata(valuefield),
        children=collect(AC.Field, valuefield.children),
    )
    return field, _dictbatch(field, indices, valuedata)
end
