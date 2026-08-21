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

# ArrowTypes is a facade concern. ArrowCore stays dependency- and
# conversion-free; this layer interprets the standard extension metadata on
# materialized values and supplies the shared metadata helpers used by the
# writer. Unregistered extension types remain ordinary Arrow storage values,
# as the format requires readers to permit.

const _EXTENSION_NAME_KEY = "ARROW:extension:name"
const _EXTENSION_METADATA_KEY = "ARROW:extension:metadata"

function _fieldmetavalue(f::AC.Field, key::String, default=nothing)
    value = default
    f.metadata === nothing && return value
    for kv in f.metadata
        first(kv) == key && (value = last(kv))
    end
    return value
end

function _arrowtypesextension(f::AC.Field)
    name = _fieldmetavalue(f, _EXTENSION_NAME_KEY)
    name === nothing && return nothing
    metadata = _fieldmetavalue(f, _EXTENSION_METADATA_KEY, "")
    return String(name), String(metadata)
end

function _withoutownextension(f::AC.Field, t::AC.ArrowType=f.type; nullable=f.nullable)
    metadata =
        f.metadata === nothing ? nothing :
        Pair{String,String}[
            String(first(kv)) => String(last(kv)) for kv in f.metadata if
            first(kv) != _EXTENSION_NAME_KEY && first(kv) != _EXTENSION_METADATA_KEY
        ]
    return AC.Field(
        f.name,
        t;
        nullable=nullable,
        metadata=metadata,
        children=collect(AC.Field, f.children),
    )
end

_withmissingtype(T, nullable::Bool) = nullable && T !== Missing ? Union{Missing,T} : T
_nonmissingstoragetype(T) = T === Missing ? Missing : Base.nonmissingtype(T)

function _arrowtypesstructnames(f::AC.Field)
    names = Tuple(Symbol(c.name) for c in f.children)
    length(unique(names)) == length(names) || throw(
        ArgumentError(
            "extension struct $(f.name) has duplicate child names and cannot form a Julia NamedTuple storage type",
        ),
    )
    return names
end

"ArrowTypes' Julia storage type for one non-composite Field."
function _arrowtypesprimitivebasetype(f::AC.Field)
    t = f.type
    t isa AC.NullType && return Missing
    t isa AC.IntType && return AC.juliatype(t)
    t isa AC.FloatType && return AC.juliatype(t)
    t isa AC.BoolType && return Bool
    t isa AC.Utf8Type && return String
    t isa AC.BinaryType && return Vector{UInt8}
    t isa AC.FixedSizeBinaryType && return Vector{UInt8}
    t isa AC.ViewType && return t.utf8 ? String : Vector{UInt8}
    if t isa AC.DateType
        return t.unit == AC.DAY ? Dates.Date : Dates.DateTime
    end
    if t isa AC.TimestampType
        return t.unit == AC.SECOND || t.unit == AC.MILLISECOND ? Dates.DateTime : Int64
    end
    t isa AC.TimeType && return Dates.Time
    if t isa AC.DurationType
        return t.unit == AC.SECOND ? Dates.Second :
               t.unit == AC.MILLISECOND ? Dates.Millisecond :
               t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
    end
    if t isa AC.DecimalType
        return t.bits == 32 ? Int32 : t.bits == 64 ? Int64 : Vector{UInt8}
    end
    if t isa AC.IntervalType
        return t.unit == AC.YEAR_MONTH ? Int32 :
               t.unit == AC.DAY_TIME ? NamedTuple{(:days, :millis),Tuple{Int32,Int32}} :
               NamedTuple{(:months, :days, :nanos),Tuple{Int32,Int32,Int64}}
    end
    return Any
end

function _warnunsupportedextension(name::String, storage)
    @warn "unsupported ARROW:extension:name type: \"$name\", arrow type = $storage" maxlog =
        1 _id = hash((:arrowtypesextension, name, storage))
    return nothing
end

function _hasarrowtypesextension(f::AC.Field)
    _arrowtypesextension(f) === nothing || return true
    return any(_hasarrowtypesextension, f.children)
end

# Core's ordinary Union materializer returns only the selected child's value.
# That is the correct storage-domain value, but it cannot identify two
# extension children that share one physical Julia type. Keep the child
# position on the ArrowTypes-only path so lifting can follow the schema's type
# id instead of guessing from the materialized value's type.
struct _ArrowTypesRoutedUnion{T}
    child::Int
    value::T
end

function _needsarrowtypesunionroute(f::AC.Field, memo::Dict{AC.Field,Bool})
    return get!(memo, f) do
        t = f.type
        if t isa AC.DictionaryType
            return _needsarrowtypesunionroute(_arrowtypesdictvaluefield(f, t), memo)
        elseif t isa AC.RunEndEncodedType
            return length(f.children) == 2 &&
                   _needsarrowtypesunionroute(f.children[2], memo)
        elseif t isa AC.UnionType && any(_hasarrowtypesextension, f.children)
            return true
        end
        return any(child -> _needsarrowtypesunionroute(child, memo), f.children)
    end
end

function _arrowtypesroutedvalue(
    f::AC.Field,
    d::AC.ArrayData,
    i::Int64,
    memo::Dict{AC.Field,Bool},
)
    _needsarrowtypesunionroute(f, memo) || return AC.getvalue(f, d, i)
    t = f.type
    if t isa AC.DictionaryType
        AC.isvalid_at(d, i) || return missing
        w = AC.primwidth(t.indextype)
        index =
            AC._load_int(AC.rolebuffer(d, AC.DATA), t.indextype, AC._slotbyteoff(d, i, w))
        pool = d.dictionary
        pool === nothing && throw(AC.ValidationError("dictionary array has no value pool"))
        return _arrowtypesroutedvalue(
            _arrowtypesdictvaluefield(f, t),
            pool,
            AC.checked_add(Int64(index), Int64(1)),
            memo,
        )
    elseif t isa AC.RunEndEncodedType
        length(f.children) == 2 || return AC.getvalue(f, d, i)
        return _arrowtypesroutedvalue(
            f.children[2],
            d.children[2],
            AC._ree_runindex(d, i),
            memo,
        )
    elseif t isa AC.UnionType
        childfield, childdata, childindex = AC._union_child(f, d, i)
        child = findfirst(x -> x === childfield, f.children)
        child === nothing && throw(AC.ValidationError("union selected an undeclared child"))
        value = _arrowtypesroutedvalue(childfield, childdata, childindex, memo)
        return _ArrowTypesRoutedUnion(child, value)
    elseif t isa AC.ListType
        AC.isvalid_at(d, i) || return missing
        lo, hi = AC._offsets_at(d, i, AC.layoutspec(t).offsetwidth == 8)
        childfield, childdata = f.children[1], d.children[1]
        out = Vector{Any}(undef, Int(hi - lo))
        for k = 1:length(out)
            out[k] = _arrowtypesroutedvalue(
                childfield,
                childdata,
                AC.checked_add(lo, Int64(k)),
                memo,
            )
        end
        return out
    elseif t isa AC.ListViewType
        AC.isvalid_at(d, i) || return missing
        off, size = AC._listview_range(t, d, i)
        childfield, childdata = f.children[1], d.children[1]
        out = Vector{Any}(undef, Int(size))
        for k = 1:length(out)
            out[k] = _arrowtypesroutedvalue(
                childfield,
                childdata,
                AC.checked_add(off, Int64(k)),
                memo,
            )
        end
        return out
    elseif t isa AC.FixedSizeListType
        AC.isvalid_at(d, i) || return missing
        childfield, childdata = f.children[1], d.children[1]
        base = AC.checked_mul(AC._slotindex0(d, i), Int64(t.listsize))
        out = Vector{Any}(undef, t.listsize)
        for k = 1:t.listsize
            out[k] = _arrowtypesroutedvalue(
                childfield,
                childdata,
                AC.checked_add(base, Int64(k)),
                memo,
            )
        end
        return out
    elseif t isa AC.StructType
        AC.isvalid_at(d, i) || return missing
        childindex = AC.checked_add(d.offset, i)
        out = Vector{Pair{String,Any}}(undef, length(f.children))
        for k in eachindex(f.children)
            out[k] =
                f.children[k].name =>
                    _arrowtypesroutedvalue(f.children[k], d.children[k], childindex, memo)
        end
        return out
    elseif t isa AC.MapType
        AC.isvalid_at(d, i) || return missing
        lo, hi = AC._offsets_at(d, i, false)
        entriesfield, entriesdata = f.children[1], d.children[1]
        keyfield, valuefield = entriesfield.children
        keydata, valuedata = entriesdata.children
        out = Vector{Pair{Any,Any}}(undef, Int(hi - lo))
        for k = 1:length(out)
            entryindex = AC.checked_add(entriesdata.offset, AC.checked_add(lo, Int64(k)))
            out[k] = Pair{Any,Any}(
                _arrowtypesroutedvalue(keyfield, keydata, entryindex, memo),
                _arrowtypesroutedvalue(valuefield, valuedata, entryindex, memo),
            )
        end
        return out
    end
    return AC.getvalue(f, d, i)
end

"Materialize only when a Union needs its selected child preserved for lifting."
function _arrowtypesroutedcolumn(f::AC.Field, d::AC.ArrayData)
    memo = Dict{AC.Field,Bool}()
    _needsarrowtypesunionroute(f, memo) || return nothing
    return Any[_arrowtypesroutedvalue(f, d, Int64(i), memo) for i = 1:d.len]
end

"Per-column ArrowTypes resolution; each Field calls JuliaType at most once."
mutable struct _ArrowTypesReadContext
    targets::Dict{AC.Field,Any}
    logicaltypes::Dict{AC.Field,Any}
    publictypes::Dict{AC.Field,Any}
    warn::Bool
end
_ArrowTypesReadContext(; warn::Bool=true) = _ArrowTypesReadContext(
    Dict{AC.Field,Any}(),
    Dict{AC.Field,Any}(),
    Dict{AC.Field,Any}(),
    warn,
)

function _arrowtypesstoragebasetype(ctx::_ArrowTypesReadContext, f::AC.Field)
    t = f.type
    if t isa AC.DictionaryType
        valuefield = AC.Field(
            f.name,
            t.valuetype;
            nullable=true,
            children=collect(AC.Field, f.children),
        )
        return _arrowtypesstoragebasetype(ctx, valuefield)
    end
    if t isa AC.RunEndEncodedType
        length(f.children) == 2 || return Any
        return Base.nonmissingtype(_arrowtypeslogicaleltype(ctx, f.children[2]))
    end
    if t isa Union{AC.ListType,AC.ListViewType}
        length(f.children) == 1 || return Vector{Any}
        return Vector{_arrowtypeslogicaleltype(ctx, f.children[1])}
    end
    if t isa AC.FixedSizeListType
        length(f.children) == 1 || return NTuple{0,Any}
        return NTuple{t.listsize,_arrowtypeslogicaleltype(ctx, f.children[1])}
    end
    if t isa AC.StructType
        names = _arrowtypesstructnames(f)
        types = Type[_arrowtypeslogicaleltype(ctx, child) for child in f.children]
        return Core.apply_type(NamedTuple, names, Core.apply_type(Tuple, types...))
    end
    if t isa AC.MapType
        length(f.children) == 1 || return Dict{Any,Any}
        entries = f.children[1]
        length(entries.children) == 2 || return Dict{Any,Any}
        K = _arrowtypeslogicaleltype(ctx, entries.children[1])
        V = _arrowtypeslogicaleltype(ctx, entries.children[2])
        return Dict{K,V}
    end
    if t isa AC.UnionType
        isempty(f.children) && return Union{}
        T = _arrowtypeslogicaleltype(ctx, f.children[1])
        for child in Iterators.drop(f.children, 1)
            T = Union{T,_arrowtypeslogicaleltype(ctx, child)}
        end
        return T
    end
    return _arrowtypesprimitivebasetype(f)
end

function _arrowtypestarget(ctx::_ArrowTypesReadContext, f::AC.Field)
    return get!(ctx.targets, f) do
        ext = _arrowtypesextension(f)
        if ext !== nothing && f.type isa AC.StructType
            try
                _arrowtypesstructnames(f)
            catch err
                err isa ArgumentError || rethrow()
                name, _ = ext
                storage = Vector{Pair{String,Any}}
                ctx.warn && _warnunsupportedextension(name, storage)
                return (true, nothing, storage)
            end
        end
        storage = _arrowtypesstoragebasetype(ctx, f)
        ext === nothing && return (false, nothing, storage)
        name, metadata = ext
        sym = try
            Symbol(name)
        catch err
            err isa ArgumentError || rethrow()
            ctx.warn && _warnunsupportedextension(name, storage)
            return (true, nothing, storage)
        end
        target = ArrowTypes.JuliaType(Val(sym), _nonmissingstoragetype(storage), metadata)
        target === nothing && ctx.warn && _warnunsupportedextension(name, storage)
        return (true, target, storage)
    end
end

function _arrowtypeslogicaleltype(ctx::_ArrowTypesReadContext, f::AC.Field)
    return get!(ctx.logicaltypes, f) do
        _, target, storage = _arrowtypestarget(ctx, f)
        _withmissingtype(target === nothing ? storage : target, f.nullable)
    end
end

function _arrowtypespubliceltype(ctx::_ArrowTypesReadContext, f::AC.Field)
    return get!(ctx.publictypes, f) do
        _, target, storage = _arrowtypestarget(ctx, f)
        if target !== nothing
            nullable = !(f.type isa AC.NullType) && f.nullable
            return _withmissingtype(target, nullable)
        end
        t = f.type
        if t isa AC.DictionaryType
            valuefield = AC.Field(
                f.name,
                t.valuetype;
                nullable=false,
                children=collect(AC.Field, f.children),
            )
            return _withmissingtype(
                Base.nonmissingtype(_arrowtypespubliceltype(ctx, valuefield)),
                f.nullable,
            )
        elseif t isa AC.RunEndEncodedType
            length(f.children) == 2 || return Any
            return _arrowtypespubliceltype(ctx, f.children[2])
        elseif t isa Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType}
            length(f.children) == 1 || return _withmissingtype(Vector{Any}, f.nullable)
            return _withmissingtype(
                Vector{_arrowtypespubliceltype(ctx, f.children[1])},
                f.nullable,
            )
        elseif t isa AC.StructType
            foreach(child -> _arrowtypespubliceltype(ctx, child), f.children)
            return _withmissingtype(Vector{Pair{String,Any}}, f.nullable)
        elseif t isa AC.MapType
            foreach(child -> _arrowtypespubliceltype(ctx, child), f.children)
            return _withmissingtype(Vector{Pair{Any,Any}}, f.nullable)
        elseif t isa AC.UnionType
            isempty(f.children) && return Union{}
            T = _arrowtypespubliceltype(ctx, f.children[1])
            for child in Iterators.drop(f.children, 1)
                T = Base.promote_typejoin(T, _arrowtypespubliceltype(ctx, child))
            end
            return T
        end
        return _withmissingtype(storage, f.nullable)
    end
end

function _typedvalues(::Type{Any}, values)
    return map(identity, values)
end
function _typedvalues(::Type{T}, values) where {T}
    return T[x for x in values]
end

function _arrowtypesscalar(t::AC.ArrowType, x)
    x === missing && return missing
    if t isa AC.DateType
        return t.unit == AC.DAY ? Dates.Date(Dates.UTD(Int64(x) + _EPOCH_DAYS)) :
               Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH))
    end
    if t isa AC.TimestampType
        t.unit == AC.SECOND &&
            return Dates.DateTime(Dates.UTM(Int64(x) * 1000 + Dates.UNIXEPOCH))
        t.unit == AC.MILLISECOND &&
            return Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH))
        return x
    end
    if t isa AC.TimeType
        scale =
            t.unit == AC.SECOND ? Int64(1_000_000_000) :
            t.unit == AC.MILLISECOND ? Int64(1_000_000) :
            t.unit == AC.MICROSECOND ? Int64(1_000) : Int64(1)
        return Dates.Time(Dates.Nanosecond(Int64(x) * scale))
    end
    if t isa AC.DurationType
        P =
            t.unit == AC.SECOND ? Dates.Second :
            t.unit == AC.MILLISECOND ? Dates.Millisecond :
            t.unit == AC.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
        return P(Int64(x))
    end
    return x
end

function _arrowtypesdictvaluefield(f::AC.Field, t::AC.DictionaryType)
    return AC.Field(
        f.name,
        t.valuetype;
        nullable=true,
        children=collect(AC.Field, f.children),
    )
end

"Convert a child while preserving the container shape required by its parent."
function _arrowtypesnestedvalue(
    ctx::_ArrowTypesReadContext,
    f::AC.Field,
    x;
    extension_shape::Bool,
)
    _, target, _ = _arrowtypestarget(ctx, f)
    target !== nothing && return _arrowtypesvalue(ctx, f, x)
    return _arrowtypesstoragevalue(ctx, f, x; extension_shape=extension_shape)
end

function _arrowtypesnestedeltype(
    ctx::_ArrowTypesReadContext,
    f::AC.Field;
    extension_shape::Bool,
)
    _, target, _ = _arrowtypestarget(ctx, f)
    if target !== nothing || extension_shape
        return _arrowtypeslogicaleltype(ctx, f)
    end
    return _arrowtypespubliceltype(ctx, f)
end

"Convert children, preserving 3.0's unmarked row containers unless requested."
function _arrowtypesstoragevalue(
    ctx::_ArrowTypesReadContext,
    f::AC.Field,
    x;
    extension_shape::Bool,
)
    t = f.type
    x === missing && return missing
    t isa AC.DictionaryType && return _arrowtypesstoragevalue(
        ctx,
        _arrowtypesdictvaluefield(f, t),
        x;
        extension_shape=extension_shape,
    )
    if t isa AC.RunEndEncodedType
        length(f.children) == 2 || return x
        return _arrowtypesnestedvalue(
            ctx,
            f.children[2],
            x;
            extension_shape=extension_shape,
        )
    end
    if t isa Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType}
        length(f.children) == 1 || return x
        child = f.children[1]
        vals = Any[
            _arrowtypesnestedvalue(ctx, child, y; extension_shape=extension_shape) for
            y in x
        ]
        if t isa AC.FixedSizeListType && extension_shape
            return Tuple(vals)
        end
        return _typedvalues(
            _arrowtypesnestedeltype(ctx, child; extension_shape=extension_shape),
            vals,
        )
    end
    if t isa AC.StructType
        length(x) == length(f.children) || throw(
            AC.ValidationError("extension struct value width does not match its Field"),
        )
        vals = Any[]
        for (i, child) in enumerate(f.children)
            kv = x[i]
            kv isa Pair ||
                throw(AC.ValidationError("extension struct rows must contain Pairs"))
            first(kv) == child.name || throw(
                AC.ValidationError("extension struct child order does not match its Field"),
            )
            push!(
                vals,
                _arrowtypesnestedvalue(
                    ctx,
                    child,
                    last(kv);
                    extension_shape=extension_shape,
                ),
            )
        end
        if extension_shape
            NT = _arrowtypesstoragebasetype(ctx, f)
            return NT(Tuple(vals))
        end
        return Pair{String,Any}[f.children[i].name => vals[i] for i in eachindex(vals)]
    end
    if t isa AC.MapType
        length(f.children) == 1 || return x
        entries = f.children[1]
        length(entries.children) == 2 || return x
        kf, vf = entries.children
        vals = Pair{Any,Any}[
            _arrowtypesnestedvalue(ctx, kf, first(kv); extension_shape=extension_shape) => _arrowtypesnestedvalue(
                ctx,
                vf,
                last(kv);
                extension_shape=extension_shape,
            ) for kv in x
        ]
        if extension_shape
            D = _arrowtypesstoragebasetype(ctx, f)
            out = D()
            for kv in vals
                out[first(kv)] = last(kv)
            end
            return out
        end
        return vals
    end
    if t isa AC.UnionType
        if x isa _ArrowTypesRoutedUnion
            1 <= x.child <= length(f.children) ||
                throw(AC.ValidationError("routed union child is outside its Field"))
            return _arrowtypesnestedvalue(
                ctx,
                f.children[x.child],
                x.value;
                extension_shape=extension_shape,
            )
        end
        for child in f.children
            T = _arrowtypesnestedeltype(ctx, child; extension_shape=extension_shape)
            x isa T && return _arrowtypesnestedvalue(
                ctx,
                child,
                x;
                extension_shape=extension_shape,
            )
        end
        return x
    end
    # Native facade conversion is intentionally top-level. Only a marked
    # parent's ArrowTypes storage shape requests native scalar values here.
    # An unmarked composite must keep unrelated temporal children in the raw
    # storage domain even when a marked sibling causes this recursive path.
    return extension_shape ? _arrowtypesscalar(t, x) : x
end

function _arrowtypesutf8storage(f::AC.Field)
    t = f.type
    t isa AC.DictionaryType &&
        return _arrowtypesutf8storage(_arrowtypesdictvaluefield(f, t))
    if t isa AC.RunEndEncodedType
        return length(f.children) == 2 && _arrowtypesutf8storage(f.children[2])
    end
    return t isa AC.Utf8Type || (t isa AC.ViewType && t.utf8)
end

function _arrowtypesfromarrow(T, f::AC.Field, storage)
    t = f.type
    if _arrowtypesutf8storage(f) && storage isa AbstractString
        bytes = codeunits(storage)
        GC.@preserve storage bytes begin
            return ArrowTypes.fromarrow(T, pointer(bytes), length(bytes))
        end
    elseif t isa AC.DictionaryType
        return ArrowTypes.fromarrow(T, storage)
    elseif t isa AC.RunEndEncodedType
        return ArrowTypes.fromarrow(T, storage)
    elseif t isa AC.StructType
        if T <: NamedTuple || T <: Tuple
            return T(Tuple(storage))
        end
        names = _arrowtypesstructnames(f)
        values = Tuple(storage)
        if isdefined(ArrowTypes, :fromarrowstruct)
            fromstruct = getfield(ArrowTypes, :fromarrowstruct)
            applicable(fromstruct, T, Val(names), values...) &&
                return fromstruct(T, Val(names), values...)
        end
        return ArrowTypes.fromarrow(T, values...)
    end
    return ArrowTypes.fromarrow(T, storage)
end

function _arrowtypesvalue(ctx::_ArrowTypesReadContext, f::AC.Field, x)
    has_label, target, _ = _arrowtypestarget(ctx, f)
    if target === nothing
        return _arrowtypesstoragevalue(ctx, f, x; extension_shape=false)
    end
    # A validity null remains `missing`; NullType is different — it is the
    # physical storage of logical values such as `nothing` and must lift.
    x === missing && !(f.type isa AC.NullType) && return missing
    storage = _arrowtypesstoragevalue(ctx, f, x; extension_shape=true)
    return _arrowtypesfromarrow(target, f, storage)
end

"Interpret extension labels recursively over an already materialized column."
function _arrowtypescolumn(f::AC.Field, col::AbstractVector)
    ctx = _ArrowTypesReadContext()
    values = Any[_arrowtypesvalue(ctx, f, x) for x in col]
    has_label, target, _ = _arrowtypestarget(ctx, f)
    if target !== nothing
        # NullType uses the physical null slots as its storage values. A
        # registered logical type such as `Nothing` lifts those slots to real
        # values, so physical field nullability must not add `Missing` back.
        nullable = !(f.type isa AC.NullType) && (f.nullable || any(ismissing, values))
        T = _withmissingtype(target, nullable)
        return _typedvalues(T, values)
    end
    # An unknown top-level extension remains the ordinary 3.0 storage column.
    # Recursive registered children have already been lifted in `values`.
    has_registered_child = any(
        child ->
            _arrowtypestarget(ctx, child)[2] !== nothing || _hasarrowtypesextension(child),
        f.children,
    )
    if has_label && !has_registered_child
        return _publiccolumn(f, _postconvert(f.type, col))
    end
    T = _arrowtypespubliceltype(ctx, f)
    any(ismissing, values) && !(Missing <: T) && (T = Union{Missing,T})
    return _typedvalues(T, values)
end
