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
# writer. An unregistered extension type stays an ordinary Arrow storage
# value; the format requires readers to accept that.

const _EXTENSION_NAME_KEY = "ARROW:extension:name"
const _EXTENSION_METADATA_KEY = "ARROW:extension:metadata"
const _MAX_ARROWTYPE_EXACT_ARITY = 1024
const _MAX_ARROWTYPE_UNION_BRANCHES = 32
const _MAX_ARROWTYPE_SCHEMA_NAME_BYTES = 4096
const _MAX_ARROWTYPE_STRUCT_NAME_BYTES = 64 * 1024
const _MAX_EXTENSION_WARNING_BYTES = 128
const _MAX_EXTENSION_WARNINGS = 16

# Look a Symbol up without creating one: interning an input-controlled name
# would permanently allocate process-global state.
function _existingjlsymbol(name::AbstractString)
    occursin('\0', name) && return nothing
    ncodeunits(name) <= _MAX_ARROWTYPE_SCHEMA_NAME_BYTES || return nothing
    pointer = ccall(:jl_symbol_lookup, Ptr{Cvoid}, (Cstring,), name)
    return pointer == C_NULL ? nothing : Symbol(name)
end

function _boundedfixedliststoragetype(listsize::Int, element)
    # Julia materializes an NTuple type's parameter list in O(N) space. Keep
    # the exact storage signature for ordinary fixed lists, but use the compact
    # tuple-family type for a hostile or unusually large descriptor.
    return listsize <= _MAX_ARROWTYPE_EXACT_ARITY ? NTuple{listsize,element} :
           Tuple{Vararg{element}}
end

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
    return AC.Field(f.name, t; nullable=nullable, metadata=metadata, children=f.children)
end

_withmissingtype(T, nullable::Bool) = nullable && T !== Missing ? Union{Missing,T} : T
_nonmissingstoragetype(T) = T === Missing ? Missing : Base.nonmissingtype(T)

@inline function _ispositionalchildname(name::String, index::Int)
    bytes = codeunits(name)
    length(bytes) == ndigits(index) || return false
    value = 0
    for byte in bytes
        0x30 <= byte <= 0x39 || return false
        value = 10 * value + Int(byte - 0x30)
    end
    return value == index
end

function _arrowtypesstructnames(
    f::AC.Field,
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    length(f.children) <= _MAX_ARROWTYPE_EXACT_ARITY || throw(
        ArgumentError(
            "extension struct $(f.name) has more than " *
            "$_MAX_ARROWTYPE_EXACT_ARITY children and cannot form an exact Julia " *
            "NamedTuple storage type",
        ),
    )
    nchildren = length(f.children)
    _chargevector!(budget, Symbol, nchildren, "ArrowTypes struct-name workspace")
    _chargedict!(budget, String, Nothing, nchildren, "ArrowTypes struct-name duplicate set")
    names = Symbol[]
    seen = Set{String}()
    sizehint!(names, nchildren)
    sizehint!(seen, nchildren)
    totalbytes = 0
    # ArrowTypes represents positional Tuple storage as Struct children named
    # "1", "2", ... . Preserve that established interface without opening an
    # unbounded input-to-Symbol path: at most the fixed set 1:1024 can be added.
    positional = all(
        index -> _ispositionalchildname(f.children[index].name, index),
        eachindex(f.children),
    )
    for child in f.children
        name = child.name
        occursin('\0', name) && throw(
            ArgumentError(
                "extension struct $(f.name) has a child name with an embedded NUL",
            ),
        )
        namebytes = ncodeunits(name)
        namebytes <= _MAX_ARROWTYPE_SCHEMA_NAME_BYTES || throw(
            ArgumentError(
                "extension struct $(f.name) has a child name longer than " *
                "$_MAX_ARROWTYPE_SCHEMA_NAME_BYTES bytes",
            ),
        )
        totalbytes <= _MAX_ARROWTYPE_STRUCT_NAME_BYTES - namebytes || throw(
            ArgumentError(
                "extension struct $(f.name) child names exceed the supported " *
                "$_MAX_ARROWTYPE_STRUCT_NAME_BYTES-byte total",
            ),
        )
        totalbytes += namebytes
        name in seen && throw(
            ArgumentError(
                "extension struct $(f.name) has duplicate child names and cannot " *
                "form a Julia NamedTuple storage type",
            ),
        )
        push!(seen, name)
        symbol = _existingjlsymbol(name)
        if symbol === nothing
            positional || throw(
                ArgumentError(
                    "extension struct $(f.name) has an unregistered child name " *
                    "$(repr(name)); exact NamedTuple lifting would permanently intern it",
                ),
            )
            _chargeobject!(budget, namebytes, "ArrowTypes positional child-name symbol")
            symbol = Symbol(name)
        end
        push!(names, symbol)
    end
    _chargeobject!(
        budget,
        AC.checked_mul(Int64(Base.elsize(Vector{Symbol})), Int64(nchildren)),
        "ArrowTypes struct-name tuple",
    )
    return Tuple(names)
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

function _extensionwarningname(
    name::AbstractString,
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    ncodeunits(name) <= _MAX_EXTENSION_WARNING_BYTES && return name
    bytes = 0
    lastindex = 0
    for index in eachindex(name)
        width = nextind(name, index) - index
        bytes + width > _MAX_EXTENSION_WARNING_BYTES && break
        bytes += width
        lastindex = index
    end
    lastindex == 0 && return "…"
    _chargeobject!(budget, bytes + ncodeunits("…"), "ArrowTypes extension-warning display")
    return string(SubString(name, firstindex(name), lastindex), '…')
end

function _warnunsupportedextension(ctx, name::String, f::AC.Field)
    ctx.warn || return nothing
    name in ctx.warned && return nothing
    if ctx.warnings >= _MAX_EXTENSION_WARNINGS
        if !ctx.warningsuppressed
            ctx.warningsuppressed = true
            @warn "additional unsupported Arrow extension warnings were suppressed"
        end
        return nothing
    end
    _chargedictentry!(ctx.budget, String, Nothing, "ArrowTypes warning cache")
    push!(ctx.warned, name)
    ctx.warnings += 1
    displayname = _extensionwarningname(name, ctx.budget)
    descriptor = AC.descriptorname(f.type)
    # Charge only the message this adapter builds. The logging framework's
    # own records and rendering are outside this budget.
    _chargeobject!(
        ctx.budget,
        AC.checked_add(
            Int64(128),
            AC.checked_mul(Int64(4), Int64(ncodeunits(displayname))),
        ),
        "ArrowTypes extension-warning message",
    )
    @warn "unsupported ARROW:extension:name type: $(repr(displayname)), storage descriptor = $descriptor"
    return nothing
end

function _hasarrowtypesextension(f::AC.Field)
    _arrowtypesextension(f) === nothing || return true
    return any(_hasarrowtypesextension, f.children)
end

# Core's ordinary materializer deliberately returns only storage-domain
# values. Two facade-only facts still have to survive until ArrowTypes lifting:
# a Union's selected child, and whether a Dictionary<Null> row was a valid
# index into its null pool rather than a null index. Keep those facts on this
# private route; the facade consumes every marker before returning a column.
struct _ArrowTypesRoutedUnion{T}
    child::Int
    value::T
end

struct _ArrowTypesRoutedNull end

mutable struct _ArrowTypesRoutePlan
    extensions::Dict{AC.Field,Bool}
    routes::Dict{AC.Field,Bool}
    dictionaryfields::Dict{AC.Field,AC.Field}
    budget::Union{Nothing,AllocationBudget}
end
function _ArrowTypesRoutePlan(
    budget::Union{Nothing,AllocationBudget}=nothing,
    extensions::Union{Nothing,Dict{AC.Field,Bool}}=nothing,
)
    _chargeobject!(budget, sizeof(_ArrowTypesRoutePlan), "ArrowTypes route-plan owner")
    if extensions === nothing
        _chargeemptydict!(
            budget,
            AC.Field,
            Bool,
            "ArrowTypes extension-presence cache container",
        )
        extensions = Dict{AC.Field,Bool}()
    end
    _chargeemptydict!(budget, AC.Field, Bool, "ArrowTypes route cache container")
    _chargeemptydict!(
        budget,
        AC.Field,
        AC.Field,
        "ArrowTypes routed dictionary-field cache container",
    )
    return _ArrowTypesRoutePlan(
        extensions,
        Dict{AC.Field,Bool}(),
        Dict{AC.Field,AC.Field}(),
        budget,
    )
end

function _hasarrowtypesextension(f::AC.Field, plan::_ArrowTypesRoutePlan)
    return _memoized!(
        plan.extensions,
        f,
        plan.budget,
        "ArrowTypes extension-presence cache",
    ) do
        _arrowtypesextension(f) === nothing || return true
        return any(child -> _hasarrowtypesextension(child, plan), f.children)
    end
end

function _routedictionaryfield!(
    plan::_ArrowTypesRoutePlan,
    f::AC.Field,
    t::AC.DictionaryType,
)
    return _memoized!(
        plan.dictionaryfields,
        f,
        plan.budget,
        "ArrowTypes routed dictionary-field cache",
    ) do
        _arrowtypesdictvaluefield(f, t; retainmetadata=true, budget=plan.budget)
    end
end

function _needsarrowtypesroute(f::AC.Field, plan::_ArrowTypesRoutePlan)
    return _memoized!(plan.routes, f, plan.budget, "ArrowTypes route cache") do
        t = f.type
        if t isa AC.DictionaryType
            ownnull = t.valuetype isa AC.NullType && _arrowtypesextension(f) !== nothing
            return ownnull ||
                   _needsarrowtypesroute(_routedictionaryfield!(plan, f, t), plan)
        elseif t isa AC.RunEndEncodedType
            return length(f.children) == 2 && _needsarrowtypesroute(f.children[2], plan)
        elseif t isa AC.UnionType && _hasarrowtypesextension(f, plan)
            return true
        end
        return any(child -> _needsarrowtypesroute(child, plan), f.children)
    end
end

function _arrowtypesroutedvalue(
    f::AC.Field,
    d::AC.ArrayData,
    i::Int64,
    plan::_ArrowTypesRoutePlan,
    routeallunions::Bool=false,
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    (routeallunions || _needsarrowtypesroute(f, plan)) ||
        return budget === nothing ? AC.getvalue(f, d, i) : AC.getvalue(f, d, i, budget)
    t = f.type
    if t isa AC.DictionaryType
        AC.isvalid_at(d, i) || return missing
        w = AC.primwidth(t.indextype)
        index =
            AC._load_int(AC.rolebuffer(d, AC.DATA), t.indextype, AC._slotbyteoff(d, i, w))
        pool = d.dictionary
        pool === nothing && throw(AC.ValidationError("dictionary array has no value pool"))
        value = _arrowtypesroutedvalue(
            _routedictionaryfield!(plan, f, t),
            pool,
            AC.checked_add(Int64(index), Int64(1)),
            plan,
            routeallunions,
            budget,
        )
        # A NullType pool's physical value is always `missing`. The valid index
        # above makes it a logical extension value; an invalid outer index has
        # already returned ordinary `missing` and must remain distinct.
        return t.valuetype isa AC.NullType && _arrowtypesextension(f) !== nothing ?
               _ArrowTypesRoutedNull() : value
    elseif t isa AC.RunEndEncodedType
        length(f.children) == 2 ||
            return budget === nothing ? AC.getvalue(f, d, i) : AC.getvalue(f, d, i, budget)
        return _arrowtypesroutedvalue(
            f.children[2],
            d.children[2],
            AC._ree_runindex(d, i),
            plan,
            routeallunions,
            budget,
        )
    elseif t isa AC.UnionType
        childfield, childdata, childindex = AC._union_child(f, d, i)
        child = findfirst(x -> x === childfield, f.children)
        child === nothing && throw(AC.ValidationError("union selected an undeclared child"))
        value = _arrowtypesroutedvalue(
            childfield,
            childdata,
            childindex,
            plan,
            routeallunions,
            budget,
        )
        routed = _ArrowTypesRoutedUnion(child, value)
        _chargeobject!(budget, sizeof(typeof(routed)), "ArrowTypes routed union value")
        return routed
    elseif t isa AC.ListType
        AC.isvalid_at(d, i) || return missing
        lo, hi = AC._offsets_at(d, i, AC.layoutspec(t).offsetwidth == 8)
        childfield, childdata = f.children[1], d.children[1]
        _chargevector!(budget, Any, hi - lo, "ArrowTypes routed list value")
        out = Vector{Any}(undef, Int(hi - lo))
        for k = 1:length(out)
            out[k] = _arrowtypesroutedvalue(
                childfield,
                childdata,
                AC.checked_add(lo, Int64(k)),
                plan,
                routeallunions,
                budget,
            )
        end
        return out
    elseif t isa AC.ListViewType
        AC.isvalid_at(d, i) || return missing
        off, size = AC._listview_range(t, d, i)
        childfield, childdata = f.children[1], d.children[1]
        _chargevector!(budget, Any, size, "ArrowTypes routed list-view value")
        out = Vector{Any}(undef, Int(size))
        for k = 1:length(out)
            out[k] = _arrowtypesroutedvalue(
                childfield,
                childdata,
                AC.checked_add(off, Int64(k)),
                plan,
                routeallunions,
                budget,
            )
        end
        return out
    elseif t isa AC.FixedSizeListType
        AC.isvalid_at(d, i) || return missing
        childfield, childdata = f.children[1], d.children[1]
        base = AC.checked_mul(AC._slotindex0(d, i), Int64(t.listsize))
        _chargevector!(budget, Any, t.listsize, "ArrowTypes routed fixed-list value")
        out = Vector{Any}(undef, t.listsize)
        for k = 1:(t.listsize)
            out[k] = _arrowtypesroutedvalue(
                childfield,
                childdata,
                AC.checked_add(base, Int64(k)),
                plan,
                routeallunions,
                budget,
            )
        end
        return out
    elseif t isa AC.StructType
        AC.isvalid_at(d, i) || return missing
        childindex = AC.checked_add(d.offset, i)
        _chargevector!(
            budget,
            Pair{String,Any},
            length(f.children),
            "ArrowTypes routed struct value",
        )
        out = Vector{Pair{String,Any}}(undef, length(f.children))
        for k in eachindex(f.children)
            out[k] =
                f.children[k].name => _arrowtypesroutedvalue(
                    f.children[k],
                    d.children[k],
                    childindex,
                    plan,
                    routeallunions,
                    budget,
                )
        end
        return out
    elseif t isa AC.MapType
        AC.isvalid_at(d, i) || return missing
        lo, hi = AC._offsets_at(d, i, false)
        entriesfield, entriesdata = f.children[1], d.children[1]
        keyfield, valuefield = entriesfield.children
        keydata, valuedata = entriesdata.children
        _chargevector!(budget, Pair{Any,Any}, hi - lo, "ArrowTypes routed map value")
        out = Vector{Pair{Any,Any}}(undef, Int(hi - lo))
        for k = 1:length(out)
            entryindex = AC.checked_add(entriesdata.offset, AC.checked_add(lo, Int64(k)))
            out[k] = Pair{Any,Any}(
                _arrowtypesroutedvalue(
                    keyfield,
                    keydata,
                    entryindex,
                    plan,
                    routeallunions,
                    budget,
                ),
                _arrowtypesroutedvalue(
                    valuefield,
                    valuedata,
                    entryindex,
                    plan,
                    routeallunions,
                    budget,
                ),
            )
        end
        return out
    end
    return budget === nothing ? AC.getvalue(f, d, i) : AC.getvalue(f, d, i, budget)
end

"Materialize only when facade lifting needs storage provenance from the layout."
function _arrowtypesroutedcolumn(f::AC.Field, d::AC.ArrayData, plan::_ArrowTypesRoutePlan)
    _needsarrowtypesroute(f, plan) || return nothing
    budget = plan.budget
    _chargevector!(budget, Any, d.len, "ArrowTypes routed column")
    return Any[
        _arrowtypesroutedvalue(f, d, Int64(i), plan, false, budget) for i = 1:(d.len)
    ]
end

_arrowtypesroutedcolumn(
    f::AC.Field,
    d::AC.ArrayData,
    budget::Union{Nothing,AllocationBudget}=nothing,
) = _arrowtypesroutedcolumn(f, d, _ArrowTypesRoutePlan(budget))

"Per read/write operation ArrowTypes resolution; each Field calls JuliaType at most once."
mutable struct _ArrowTypesContext
    targets::Dict{AC.Field,Any}
    logicaltypes::Dict{AC.Field,Any}
    publictypes::Dict{AC.Field,Any}
    extensions::Dict{AC.Field,Bool}
    structnames::Dict{AC.Field,Tuple}
    storage::IdDict{Type,Any}
    dictionaryfields::Dict{AC.Field,AC.Field}
    metadata_dictionaryfields::Dict{AC.Field,AC.Field}
    public_dictionaryfields::Dict{AC.Field,AC.Field}
    routeplan::Union{Nothing,_ArrowTypesRoutePlan}
    warn::Bool
    warned::Set{String}
    warnings::Int
    warningsuppressed::Bool
    budget::Union{Nothing,AllocationBudget}
end
function _ArrowTypesContext(;
    warn::Bool=true,
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    _chargeobject!(budget, sizeof(_ArrowTypesContext), "ArrowTypes context owner")
    _chargeemptydict!(budget, AC.Field, Any, "ArrowTypes target cache container")
    _chargeemptydict!(budget, AC.Field, Any, "ArrowTypes logical-type cache container")
    _chargeemptydict!(budget, AC.Field, Any, "ArrowTypes public-type cache container")
    _chargeemptydict!(
        budget,
        AC.Field,
        Bool,
        "ArrowTypes extension-presence cache container",
    )
    _chargeemptydict!(budget, AC.Field, Tuple, "ArrowTypes struct-name cache container")
    _chargeemptydict!(budget, Type, Any, "ArrowTypes storage-type cache container")
    _chargeemptydict!(
        budget,
        AC.Field,
        AC.Field,
        "ArrowTypes dictionary-field cache container",
    )
    _chargeemptydict!(
        budget,
        AC.Field,
        AC.Field,
        "ArrowTypes metadata dictionary-field cache container",
    )
    _chargeemptydict!(
        budget,
        AC.Field,
        AC.Field,
        "ArrowTypes public dictionary-field cache container",
    )
    _chargeemptydict!(budget, String, Nothing, "ArrowTypes warning cache container")
    _chargeobject!(budget, sizeof(Set{String}), "ArrowTypes warning-set owner")
    return _ArrowTypesContext(
        Dict{AC.Field,Any}(),
        Dict{AC.Field,Any}(),
        Dict{AC.Field,Any}(),
        Dict{AC.Field,Bool}(),
        Dict{AC.Field,Tuple}(),
        IdDict{Type,Any}(),
        Dict{AC.Field,AC.Field}(),
        Dict{AC.Field,AC.Field}(),
        Dict{AC.Field,AC.Field}(),
        nothing,
        warn,
        Set{String}(),
        0,
        false,
        budget,
    )
end

function _hasarrowtypesextension(f::AC.Field, ctx::_ArrowTypesContext)
    return _memoized!(
        ctx.extensions,
        f,
        ctx.budget,
        "ArrowTypes extension-presence cache",
    ) do
        _arrowtypesextension(f) === nothing || return true
        return any(child -> _hasarrowtypesextension(child, ctx), f.children)
    end
end

function _arrowtypesstructnames(ctx::_ArrowTypesContext, f::AC.Field)
    return _memoized!(ctx.structnames, f, ctx.budget, "ArrowTypes struct-name cache") do
        _arrowtypesstructnames(f, ctx.budget)
    end
end

function _arrowtypesrouteplan!(ctx::_ArrowTypesContext)
    plan = ctx.routeplan
    plan === nothing || return plan
    plan = _ArrowTypesRoutePlan(ctx.budget, ctx.extensions)
    ctx.routeplan = plan
    return plan
end

Base.@noinline function _arrowtypesrawstoragetype(T::Type)
    Base.@nospecialize T
    return Base.inferencebarrier(ArrowTypes.ArrowType(Base.inferencebarrier(T)))
end

function _checkarrowtypesstorageshape!(
    owner::Type,
    storage,
    seen::Base.IdSet{Any},
    depth::Int,
)
    Base.@nospecialize owner storage
    storage isa Type || return nothing
    storage in seen && return nothing
    depth <= 64 || throw(
        ArgumentError(
            "ArrowTypes.ArrowType($owner) returned a storage type nested beyond " *
            "the supported depth 64",
        ),
    )
    push!(seen, storage)
    if storage isa Union
        variants = Base.uniontypes(storage)
        length(variants) <= _MAX_ARROWTYPE_UNION_BRANCHES || throw(
            ArgumentError(
                "ArrowTypes.ArrowType($owner) returned a Union with more than " *
                "$_MAX_ARROWTYPE_UNION_BRANCHES branches",
            ),
        )
        for variant in variants
            _checkarrowtypesstorageshape!(owner, variant, seen, depth + 1)
        end
    elseif storage isa DataType
        if storage <: Tuple && isconcretetype(storage)
            nfields = fieldcount(storage)
            nfields <= _MAX_ARROWTYPE_EXACT_ARITY || throw(
                ArgumentError(
                    "ArrowTypes.ArrowType($owner) returned a concrete Tuple with " *
                    "$nfields fields; the supported limit is " *
                    "$_MAX_ARROWTYPE_EXACT_ARITY",
                ),
            )
        end
        for parameter in storage.parameters
            parameter isa Type || continue
            _checkarrowtypesstorageshape!(owner, parameter, seen, depth + 1)
        end
    end
    return nothing
end

function _validatedarrowtypesstoragetype(T::Type, storage)
    Base.@nospecialize T storage
    storage isa Type || throw(
        ArgumentError(
            "ArrowTypes.ArrowType($T) must return a Julia type, not $(repr(storage))",
        ),
    )
    _checkarrowtypesstorageshape!(T, storage, Base.IdSet{Any}(), 0)
    return storage
end

Base.@noinline function _arrowtypesstoragetype!(ctx::_ArrowTypesContext, T::Type)
    Base.@nospecialize T
    return _memoized!(ctx.storage, T, ctx.budget, "ArrowTypes storage-type cache") do
        _validatedarrowtypesstoragetype(T, _arrowtypesrawstoragetype(T))
    end
end

function _arrowtypesstoragebasetype(ctx::_ArrowTypesContext, f::AC.Field)
    t = f.type
    if t isa AC.DictionaryType
        valuefield = _arrowtypesdictvaluefield(ctx, f, t)
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
        element = _arrowtypeslogicaleltype(ctx, f.children[1])
        # Generic JuliaType registrations still resolve from the bounded
        # signature. A hook that requires an exact oversized arity safely
        # remains unregistered.
        return _boundedfixedliststoragetype(t.listsize, element)
    end
    if t isa AC.StructType
        length(f.children) > _MAX_ARROWTYPE_EXACT_ARITY && return Vector{Pair{String,Any}}
        names = _arrowtypesstructnames(ctx, f)
        _chargevector!(
            ctx.budget,
            Type,
            length(f.children),
            "ArrowTypes struct type workspace",
        )
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

function _arrowtypesfallbackstoragebasetype(ctx::_ArrowTypesContext, f::AC.Field)
    t = f.type
    if t isa AC.DictionaryType
        valuefield = _arrowtypesdictvaluefield(ctx, f, t)
        return _arrowtypesfallbackstoragebasetype(ctx, valuefield)
    end
    if t isa AC.RunEndEncodedType
        return length(f.children) == 2 ?
               _arrowtypesfallbackstoragebasetype(ctx, f.children[2]) : Any
    end
    t isa Union{AC.ListType,AC.ListViewType} && return Vector{Any}
    t isa AC.FixedSizeListType && return Tuple{Vararg{Any}}
    t isa AC.StructType && return Vector{Pair{String,Any}}
    t isa AC.MapType && return Dict{Any,Any}
    t isa AC.UnionType && return Any
    return _arrowtypesprimitivebasetype(f)
end

function _preflightarrowtypesstructnames(ctx::_ArrowTypesContext, f::AC.Field)
    f.type isa AC.StructType && _arrowtypesstructnames(ctx, f)
    foreach(child -> _preflightarrowtypesstructnames(ctx, child), f.children)
    return nothing
end

function _arrowtypestarget(ctx::_ArrowTypesContext, f::AC.Field)
    return _memoized!(ctx.targets, f, ctx.budget, "ArrowTypes target cache") do
        ext = _arrowtypesextension(f)
        # Most Fields have no extension label. Do not synthesize their full
        # ArrowTypes storage shape merely to discover that no JuliaType hook
        # can apply. In particular, forming NTuple{N,T} for an unlabelled
        # FixedSizeList must not allocate in proportion to an untrusted N.
        ext === nothing && return (false, nothing, nothing)
        name, metadata = ext
        sym = _existingjlsymbol(name)
        if sym === nothing
            storage = _arrowtypesfallbackstoragebasetype(ctx, f)
            _warnunsupportedextension(ctx, name, f)
            return (true, nothing, storage)
        end
        try
            _preflightarrowtypesstructnames(ctx, f)
        catch err
            err isa ArgumentError || rethrow()
            storage = _arrowtypesfallbackstoragebasetype(ctx, f)
            _warnunsupportedextension(ctx, name, f)
            return (true, nothing, storage)
        end
        storage = _arrowtypesstoragebasetype(ctx, f)
        target = ArrowTypes.JuliaType(Val(sym), _nonmissingstoragetype(storage), metadata)
        target === nothing ||
            target isa Type ||
            throw(
                ArgumentError(
                    "ArrowTypes.JuliaType for extension $(repr(name)) on field " *
                    "$(f.name) must return a Julia type or nothing, not " *
                    "$(repr(target))",
                ),
            )
        target === nothing && _warnunsupportedextension(ctx, name, f)
        return (true, target, storage)
    end
end

function _arrowtypeslogicalnullable(f::AC.Field, target, storage=nothing)
    if f.type isa AC.UnionType
        storage === nothing && (storage = ArrowTypes.ArrowType(target))
    end
    # A nullable logical column adds exactly one Null child around the
    # target's storage Union, so one extra field child means "outer nullable";
    # equal counts mean not nullable, and anything else is a mismatch.
    if storage isa Union
        fieldbranches = length(f.children)
        storagebranches = length(Base.uniontypes(storage))
        fieldbranches == storagebranches && return false
        if fieldbranches == storagebranches + 1
            Missing <: storage && throw(
                ArgumentError(
                    "registered ArrowTypes target $target uses Missing as a storage " *
                    "Union branch, so retained field $(f.name) cannot add an " *
                    "indistinguishable outer missing branch",
                ),
            )
            return true
        end
        throw(
            ArgumentError(
                "registered ArrowTypes target $target has $storagebranches storage " *
                "Union branches, but retained field $(f.name) has $fieldbranches children",
            ),
        )
    end
    return f.nullable
end

_arrowtypeslogicalnullable(ctx::_ArrowTypesContext, f::AC.Field, target) =
    _arrowtypeslogicalnullable(
        f,
        target,
        f.type isa AC.UnionType ? _arrowtypesstoragetype!(ctx, target) : nothing,
    )

function _arrowtypeslogicaleltype(ctx::_ArrowTypesContext, f::AC.Field)
    return _memoized!(ctx.logicaltypes, f, ctx.budget, "ArrowTypes logical-type cache") do
        _, target, storage = _arrowtypestarget(ctx, f)
        storage === nothing && (storage = _arrowtypesstoragebasetype(ctx, f))
        nullable =
            target === nothing ? f.nullable : _arrowtypeslogicalnullable(ctx, f, target)
        _withmissingtype(target === nothing ? storage : target, nullable)
    end
end

function _arrowtypespubliceltype(ctx::_ArrowTypesContext, f::AC.Field)
    return _memoized!(ctx.publictypes, f, ctx.budget, "ArrowTypes public-type cache") do
        _, target, storage = _arrowtypestarget(ctx, f)
        if target !== nothing
            nullable =
                !(f.type isa AC.NullType) && _arrowtypeslogicalnullable(ctx, f, target)
            return _withmissingtype(target, nullable)
        end
        t = f.type
        if t isa AC.DictionaryType
            valuefield = _arrowtypespublicdictvaluefield(ctx, f, t)
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
        storage === nothing && (storage = _arrowtypesstoragebasetype(ctx, f))
        return _withmissingtype(storage, f.nullable)
    end
end

function _typedvalues(::Type{Any}, values, budget=nothing)
    _chargevector!(budget, Any, length(values), "ArrowTypes public column")
    # map(identity, …) keeps the input's own narrowed eltype for an Any
    # target instead of widening every column to Vector{Any}.
    return map(identity, values)
end
function _typedvalues(::Type{T}, values, budget=nothing) where {T}
    _chargevector!(budget, T, length(values), "ArrowTypes typed column")
    return T[x for x in values]
end

function _arrowtypesscalar(t::AC.ArrowType, x)
    x === missing && return missing
    if t isa AC.DateType
        return t.unit == AC.DAY ? Dates.Date(Dates.UTD(Int64(x) + _EPOCH_DAYS)) :
               Dates.DateTime(Dates.UTM(Int64(x) + Dates.UNIXEPOCH))
    end
    if t isa AC.TimestampType
        # DateTime cannot hold micro/nanosecond precision, so those units
        # stay raw Int64 rather than lose information.
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

function _arrowtypesdictvaluefield(
    f::AC.Field,
    t::AC.DictionaryType;
    retainmetadata::Bool=false,
    nullable::Bool=true,
    budget::Union{Nothing,AllocationBudget}=nothing,
)
    metadata = retainmetadata ? f.metadata : nothing
    # Field and FrozenVector are immutable. Use Core's exact positional
    # constructor so every reader view shares the already-frozen child tree.
    _chargeobject!(budget, sizeof(AC.Field), "ArrowTypes derived dictionary Field")
    return AC.Field(f.name, t.valuetype, nullable, metadata, f.children)
end

function _arrowtypesdictvaluefield(
    ctx::_ArrowTypesContext,
    f::AC.Field,
    t::AC.DictionaryType;
    retainmetadata::Bool=false,
)
    cache = retainmetadata ? ctx.metadata_dictionaryfields : ctx.dictionaryfields
    what =
        retainmetadata ? "ArrowTypes metadata dictionary-field cache" :
        "ArrowTypes dictionary-field cache"
    return _memoized!(cache, f, ctx.budget, what) do
        _arrowtypesdictvaluefield(f, t; retainmetadata, budget=ctx.budget)
    end
end

function _arrowtypespublicdictvaluefield(
    ctx::_ArrowTypesContext,
    f::AC.Field,
    t::AC.DictionaryType,
)
    return _memoized!(
        ctx.public_dictionaryfields,
        f,
        ctx.budget,
        "ArrowTypes public dictionary-field cache",
    ) do
        _arrowtypesdictvaluefield(f, t; nullable=false, budget=ctx.budget)
    end
end

"Convert a child while preserving the container shape required by its parent."
function _arrowtypesnestedvalue(
    ctx::_ArrowTypesContext,
    f::AC.Field,
    x;
    extension_shape::Bool,
)
    _, target, _ = _arrowtypestarget(ctx, f)
    # Dictionary<Null> needs a private marker to distinguish a valid index
    # into its null pool from a null dictionary index. Consume that marker at
    # every recursive Field seam, including when the extension label is not
    # registered in this process. No private routing value may reach a public
    # row container.
    (target !== nothing || x isa _ArrowTypesRoutedNull) &&
        return _arrowtypesvalue(ctx, f, x)
    return _arrowtypesstoragevalue(ctx, f, x; extension_shape=extension_shape)
end

function _arrowtypesnestedeltype(
    ctx::_ArrowTypesContext,
    f::AC.Field;
    extension_shape::Bool,
)
    _, target, _ = _arrowtypestarget(ctx, f)
    if target !== nothing || extension_shape
        return _arrowtypeslogicaleltype(ctx, f)
    end
    return _arrowtypespubliceltype(ctx, f)
end

"Convert children, preserving the reader's unmarked row containers unless requested."
function _arrowtypesstoragevalue(
    ctx::_ArrowTypesContext,
    f::AC.Field,
    x;
    extension_shape::Bool,
)
    t = f.type
    x === missing && return missing
    t isa AC.DictionaryType && return _arrowtypesstoragevalue(
        ctx,
        _arrowtypesdictvaluefield(ctx, f, t),
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
        _chargevector!(ctx.budget, Any, length(x), "ArrowTypes converted list value")
        vals = Any[
            _arrowtypesnestedvalue(ctx, child, y; extension_shape=extension_shape) for
            y in x
        ]
        if t isa AC.FixedSizeListType && extension_shape
            _chargevector!(ctx.budget, Any, length(vals), "ArrowTypes tuple storage")
            return Tuple(vals)
        end
        return _typedvalues(
            _arrowtypesnestedeltype(ctx, child; extension_shape=extension_shape),
            vals,
            ctx.budget,
        )
    end
    if t isa AC.StructType
        length(x) == length(f.children) || throw(
            AC.ValidationError("extension struct value width does not match its Field"),
        )
        _chargevector!(ctx.budget, Any, length(f.children), "ArrowTypes struct workspace")
        vals = Vector{Any}(undef, length(f.children))
        for (i, child) in enumerate(f.children)
            kv = x[i]
            kv isa Pair ||
                throw(AC.ValidationError("extension struct rows must contain Pairs"))
            first(kv) == child.name || throw(
                AC.ValidationError("extension struct child order does not match its Field"),
            )
            vals[i] = _arrowtypesnestedvalue(
                ctx,
                child,
                last(kv);
                extension_shape=extension_shape,
            )
        end
        if extension_shape
            NT = _arrowtypesstoragebasetype(ctx, f)
            if NT <: NamedTuple
                _chargevector!(
                    ctx.budget,
                    Any,
                    length(vals),
                    "ArrowTypes NamedTuple storage",
                )
                return NT(Tuple(vals))
            end
            _chargevector!(
                ctx.budget,
                Pair{String,Any},
                length(vals),
                "ArrowTypes converted struct value",
            )
            return Pair{String,Any}[f.children[i].name => vals[i] for i in eachindex(vals)]
        end
        _chargevector!(
            ctx.budget,
            Pair{String,Any},
            length(vals),
            "ArrowTypes converted struct value",
        )
        return Pair{String,Any}[f.children[i].name => vals[i] for i in eachindex(vals)]
    end
    if t isa AC.MapType
        length(f.children) == 1 || return x
        entries = f.children[1]
        length(entries.children) == 2 || return x
        kf, vf = entries.children
        _chargevector!(
            ctx.budget,
            Pair{Any,Any},
            length(x),
            "ArrowTypes converted map value",
        )
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
            if D <: Dict
                _chargedict!(
                    ctx.budget,
                    keytype(D),
                    valtype(D),
                    length(vals),
                    "ArrowTypes Dict storage",
                )
            end
            out = D()
            D <: Dict && sizehint!(out, length(vals))
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
    # Only a marked parent's ArrowTypes storage shape asks for native scalars
    # here. An unmarked composite keeps its temporal children in the raw
    # storage domain, even when a marked sibling triggered this recursion.
    return extension_shape ? _arrowtypesscalar(t, x) : x
end

function _arrowtypesutf8storage(ctx::_ArrowTypesContext, f::AC.Field)
    t = f.type
    t isa AC.DictionaryType &&
        return _arrowtypesutf8storage(ctx, _arrowtypesdictvaluefield(ctx, f, t))
    if t isa AC.RunEndEncodedType
        return length(f.children) == 2 && _arrowtypesutf8storage(ctx, f.children[2])
    end
    return t isa AC.Utf8Type || (t isa AC.ViewType && t.utf8)
end

function _arrowtypesfromarrow(ctx::_ArrowTypesContext, T, f::AC.Field, storage)
    t = f.type
    if _arrowtypesutf8storage(ctx, f) && storage isa AbstractString
        if T === Symbol
            symbol = _existingjlsymbol(storage)
            if symbol === nothing
                displayname = _extensionwarningname(storage, ctx.budget)
                _chargeobject!(
                    ctx.budget,
                    AC.checked_add(
                        Int64(160),
                        AC.checked_mul(Int64(4), Int64(ncodeunits(displayname))),
                    ),
                    "ArrowTypes Symbol validation message",
                )
                throw(
                    AC.ValidationError(
                        "JuliaLang.Symbol payload $(repr(displayname)) is not already " *
                        "interned; automatic lifting would permanently allocate " *
                        "process-global state",
                    ),
                )
            end
            return symbol
        end
        bytes = codeunits(storage)
        GC.@preserve storage bytes begin
            return ArrowTypes.fromarrow(T, pointer(bytes), length(bytes))
        end
    elseif t isa AC.DictionaryType
        # The extension label lives on the dictionary field, but the shape
        # passed to ArrowTypes is the dictionary VALUE shape. In particular, a
        # Struct value must use `fromarrowstruct` with its declared child names;
        # the generic `fromarrow(T, storage)` path can bind reordered storage
        # children to the logical constructor by position.
        return _arrowtypesfromarrow(ctx, T, _arrowtypesdictvaluefield(ctx, f, t), storage)
    elseif t isa AC.RunEndEncodedType
        return ArrowTypes.fromarrow(T, storage)
    elseif t isa AC.StructType
        if T <: NamedTuple || T <: Tuple
            return T(Tuple(storage))
        end
        names = _arrowtypesstructnames(ctx, f)
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

function _arrowtypesvalue(ctx::_ArrowTypesContext, f::AC.Field, x)
    routednull = x isa _ArrowTypesRoutedNull
    routednull && (x = missing)
    has_label, target, _ = _arrowtypestarget(ctx, f)
    if target === nothing
        return _arrowtypesstoragevalue(ctx, f, x; extension_shape=false)
    end
    if f.type isa AC.UnionType && x isa _ArrowTypesRoutedUnion
        child = f.children[x.child]
        storage = _arrowtypesstoragetype!(ctx, target)
        # A nullable logical column adds one unmarked Null child around a
        # target whose own storage Union has no Missing branch. Preserve that
        # outer missing instead of passing it to the target's fromarrow hook.
        child.type isa AC.NullType &&
            _arrowtypesextension(child) === nothing &&
            !(storage isa Union && Missing <: storage) &&
            return missing
    end
    # A validity null remains `missing`; NullType is different — it is the
    # physical storage of public-domain values such as `nothing` and must lift.
    x === missing && !routednull && !(f.type isa AC.NullType) && return missing
    storage = _arrowtypesstoragevalue(ctx, f, x; extension_shape=true)
    return _arrowtypesfromarrow(ctx, target, f, storage)
end

"Interpret extension labels recursively over an already materialized column."
function _arrowtypescolumn(
    f::AC.Field,
    col::AbstractVector,
    ctx::_ArrowTypesContext=_ArrowTypesContext(),
)
    has_label, target, _ = _arrowtypestarget(ctx, f)
    has_registered_child = any(
        child ->
            _arrowtypestarget(ctx, child)[2] !== nothing ||
            _hasarrowtypesextension(child, ctx),
        f.children,
    )
    has_routed_value =
        any(x -> x isa _ArrowTypesRoutedNull || x isa _ArrowTypesRoutedUnion, col)
    if target === nothing && has_label && !has_registered_child && !has_routed_value
        # No hook or routed provenance is available to consume. Preserve the
        # ordinary storage column without building and discarding a per-row
        # ArrowTypes workspace.
        return _publiccolumn(f, _postconvert(f.type, col, ctx.budget), ctx.budget)
    end
    _chargevector!(ctx.budget, Any, length(col), "ArrowTypes lifting workspace")
    values = Any[_arrowtypesvalue(ctx, f, x) for x in col]
    if target !== nothing
        # NullType uses the physical null slots as its storage values. A
        # registered logical type such as `Nothing` lifts those slots to real
        # values, so physical field nullability must not add `Missing` back.
        nullable =
            !(f.type isa AC.NullType) &&
            (_arrowtypeslogicalnullable(ctx, f, target) || any(ismissing, values))
        T = _withmissingtype(target, nullable)
        return _typedvalues(T, values, ctx.budget)
    end
    # An unknown top-level extension remains the ordinary storage column.
    # Recursive registered children have already been lifted in `values`.
    if has_label && !has_registered_child
        # An unknown Dictionary<Null> extension still took the private route to
        # retain index validity. Use the consumed values so its marker cannot
        # escape; other unknown extensions keep their original storage column.
        storage = has_routed_value ? values : col
        return _publiccolumn(f, _postconvert(f.type, storage, ctx.budget), ctx.budget)
    end
    T = _arrowtypespubliceltype(ctx, f)
    any(ismissing, values) && !(Missing <: T) && (T = Union{Missing,T})
    return _typedvalues(T, values, ctx.budget)
end
