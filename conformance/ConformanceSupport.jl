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
Private support for the standalone Arrow conformance adapters.

This module owns integration-JSON conversion, value-level comparison, skip
policy, verdict construction, and common reporting. The corpus, IPC oracle,
and C-interface oracle remain thin adapters that select cases and routes.
"""
module ConformanceSupport

using CodecZlib
using JSON
import Arrow

const AC = Arrow.ArrowCore
const ArrowCore = Arrow.ArrowCore

include(joinpath(@__DIR__, "arrowjson.jl"))

const DEFAULT_CORPUS = get(ENV, "ARROW_TESTING_DIR", "")

# Families declared out of scope for every conformance route. Everything else
# must pass or it is a failure.
const FAMILY_SKIP = Dict{String,String}(
    "0.17.1" => "V4 experimental compression marker era; superseded by 2.0.0-compression",
)

# These families contain legacy gold IPC that our reader intentionally does
# not accept. Their integration JSON and IPC written by the current Arrow.jl
# implementation remain valid inputs to every other corpus and oracle route.
const GOLD_IPC_SKIP = Dict{String,String}(
    "1.0.0-bigendian" => "big-endian streams are not supported (no endianness normalization)",
    "0.14.1" => "pre-1.0 legacy framing (four-byte prefix) is not accepted by design",
)

familyskipreason(family::AbstractString, context::AbstractString="") =
    get(FAMILY_SKIP, family, get(FAMILY_SKIP, context, ""))

goldipcskipreason(family::AbstractString, context::AbstractString="") =
    get(GOLD_IPC_SKIP, family, get(GOLD_IPC_SKIP, context, ""))

# --- value-level comparison -----------------------------------------------------

# Compared values normally need no physical identity: dictionary ids, pool
# unification, and integer JSON spellings are representation choices. A Union's
# selected type id is different. It chooses one declared child and remains part
# of the public-domain value even when two children materialize as equal Julia values.
struct _PublicUnion
    typeid::Int8
    value::Any
end

# Core materialization intentionally replaces the physical integration-JSON
# columns during comparison. Validate the JSON envelope first so that arity,
# names, scalar kinds, and per-slot array lengths cannot disappear at that
# boundary. Array lengths describe JSON entries, not legal byte-level buffer
# padding. Nested child counts remain independent so null List/Map segments can
# retain any spec-valid physical span.
_isjsoninteger(value) = value isa Integer && !(value isa Bool)

function _jsonkind(value)
    value === nothing && return "null"
    value isa Bool && return "Bool"
    value isa Integer && return "integer"
    value isa AbstractFloat && return "float"
    value isa AbstractString && return "string"
    value isa AbstractVector && return "array"
    value isa AbstractDict && return "object"
    return string(typeof(value))
end

function _shapediff!(diffs::Vector{String}, path::String, detail::AbstractString)
    length(diffs) > 20 || push!(diffs, "$path: $detail")
    return nothing
end

function _requireproperty!(
    object::AbstractDict,
    key::String,
    predicate,
    expectedkind::String,
    path::String,
    diffs::Vector{String},
)
    if !haskey(object, key)
        _shapediff!(diffs, "$path.$key", "missing required property")
        return false
    end
    predicate(object[key]) && return true
    _shapediff!(
        diffs,
        "$path.$key",
        "expected $expectedkind, got $(_jsonkind(object[key]))",
    )
    return false
end

function _validatemetadata!(owner::AbstractDict, path::String, diffs::Vector{String})
    haskey(owner, "metadata") || return
    metadata = owner["metadata"]
    metadata === nothing && return
    if !(metadata isa AbstractVector)
        _shapediff!(
            diffs,
            "$path.metadata",
            "expected array or null, got $(_jsonkind(metadata))",
        )
        return
    end
    for (index, entry) in enumerate(metadata)
        entrypath = "$path.metadata[$index]"
        if !(entry isa AbstractDict)
            _shapediff!(diffs, entrypath, "expected object, got $(_jsonkind(entry))")
            continue
        end
        _requireproperty!(
            entry,
            "key",
            x -> x isa AbstractString,
            "string",
            entrypath,
            diffs,
        )
        _requireproperty!(
            entry,
            "value",
            x -> x isa AbstractString,
            "string",
            entrypath,
            diffs,
        )
    end
    return
end

function _validatejsontype!(type, path::String, diffs::Vector{String})
    if !(type isa AbstractDict)
        _shapediff!(diffs, path, "expected object, got $(_jsonkind(type))")
        return
    end
    _requireproperty!(type, "name", x -> x isa AbstractString, "string", path, diffs) ||
        return
    name = type["name"]
    if name == "int"
        _requireproperty!(type, "bitWidth", _isjsoninteger, "integer", path, diffs)
        _requireproperty!(type, "isSigned", x -> x isa Bool, "Bool", path, diffs)
    elseif name == "floatingpoint"
        _requireproperty!(
            type,
            "precision",
            x -> x isa AbstractString,
            "string",
            path,
            diffs,
        )
    elseif name == "fixedsizebinary"
        _requireproperty!(type, "byteWidth", _isjsoninteger, "integer", path, diffs)
    elseif name == "decimal"
        _requireproperty!(type, "precision", _isjsoninteger, "integer", path, diffs)
        _requireproperty!(type, "scale", _isjsoninteger, "integer", path, diffs)
        haskey(type, "bitWidth") &&
            !_isjsoninteger(type["bitWidth"]) &&
            _shapediff!(
                diffs,
                "$path.bitWidth",
                "expected integer, got $(_jsonkind(type["bitWidth"]))",
            )
    elseif name in ("date", "duration", "interval")
        _requireproperty!(type, "unit", x -> x isa AbstractString, "string", path, diffs)
    elseif name == "time"
        _requireproperty!(type, "unit", x -> x isa AbstractString, "string", path, diffs)
        _requireproperty!(type, "bitWidth", _isjsoninteger, "integer", path, diffs)
    elseif name == "timestamp"
        _requireproperty!(type, "unit", x -> x isa AbstractString, "string", path, diffs)
        haskey(type, "timezone") &&
            !(type["timezone"] isa AbstractString) &&
            _shapediff!(
                diffs,
                "$path.timezone",
                "expected string, got $(_jsonkind(type["timezone"]))",
            )
    elseif name == "fixedsizelist"
        _requireproperty!(type, "listSize", _isjsoninteger, "integer", path, diffs)
    elseif name == "map"
        _requireproperty!(type, "keysSorted", x -> x isa Bool, "Bool", path, diffs)
    elseif name == "union"
        _requireproperty!(type, "mode", x -> x isa AbstractString, "string", path, diffs)
        if _requireproperty!(
            type,
            "typeIds",
            x -> x isa AbstractVector,
            "array",
            path,
            diffs,
        )
            for (index, id) in enumerate(type["typeIds"])
                _isjsoninteger(id) || _shapediff!(
                    diffs,
                    "$path.typeIds[$index]",
                    "expected integer, got $(_jsonkind(id))",
                )
            end
        end
    elseif !(
        name in (
            "null",
            "bool",
            "utf8",
            "largeutf8",
            "binary",
            "largebinary",
            "utf8view",
            "binaryview",
            "list",
            "largelist",
            "listview",
            "largelistview",
            "struct",
            "runendencoded",
        )
    )
        _shapediff!(diffs, "$path.name", "unknown Arrow type $(repr(name))")
    end
    return
end

const _NO_CHILD_TYPES = Set((
    "null",
    "bool",
    "int",
    "floatingpoint",
    "utf8",
    "largeutf8",
    "binary",
    "largebinary",
    "utf8view",
    "binaryview",
    "fixedsizebinary",
    "decimal",
    "date",
    "time",
    "timestamp",
    "duration",
    "interval",
))
const _ONE_CHILD_TYPES =
    Set(("list", "largelist", "listview", "largelistview", "fixedsizelist", "map"))

function _validatefieldarity!(
    field::AbstractDict,
    children::AbstractVector,
    path::String,
    diffs::Vector{String},
)
    type = get(field, "type", nothing)
    type isa AbstractDict || return
    name = get(type, "name", nothing)
    name isa AbstractString || return
    expected =
        name in _NO_CHILD_TYPES ? 0 :
        name in _ONE_CHILD_TYPES ? 1 : name == "runendencoded" ? 2 : nothing
    if name == "union" && get(type, "typeIds", nothing) isa AbstractVector
        expected = length(type["typeIds"])
    end
    expected === nothing ||
        length(children) == expected ||
        _shapediff!(
            diffs,
            "$path.children",
            "$name requires $expected schema children, got $(length(children))",
        )
    if name == "map" && length(children) == 1 && children[1] isa AbstractDict
        entries = children[1]
        entrytype = get(entries, "type", nothing)
        get(entrytype isa AbstractDict ? entrytype : Dict(), "name", nothing) == "struct" ||
            _shapediff!(diffs, "$path.children[1].type", "map entries must be a struct")
        entrychildren = get(entries, "children", Any[])
        entrychildren isa AbstractVector && length(entrychildren) == 2 || _shapediff!(
            diffs,
            "$path.children[1].children",
            "map entries struct must have key and value children",
        )
    end
    return
end

function _validatefield!(
    field,
    path::String,
    dictionaries::Dict{Int64,Tuple{Any,String}},
    diffs::Vector{String},
)
    if !(field isa AbstractDict)
        _shapediff!(diffs, path, "expected object, got $(_jsonkind(field))")
        return
    end
    _requireproperty!(field, "name", x -> x isa AbstractString, "string", path, diffs)
    _requireproperty!(field, "nullable", x -> x isa Bool, "Bool", path, diffs)
    if _requireproperty!(field, "type", x -> x isa AbstractDict, "object", path, diffs)
        _validatejsontype!(field["type"], "$path.type", diffs)
    end
    _validatemetadata!(field, path, diffs)

    children = get(field, "children", Any[])
    if !(children isa AbstractVector)
        _shapediff!(diffs, "$path.children", "expected array, got $(_jsonkind(children))")
        children = Any[]
    end
    _validatefieldarity!(field, children, path, diffs)
    for (index, child) in enumerate(children)
        _validatefield!(child, "$path.children[$index]", dictionaries, diffs)
    end

    haskey(field, "dictionary") || return
    dictionary = field["dictionary"]
    if !(dictionary isa AbstractDict)
        _shapediff!(
            diffs,
            "$path.dictionary",
            "expected object, got $(_jsonkind(dictionary))",
        )
        return
    end
    idok = _requireproperty!(
        dictionary,
        "id",
        _isjsoninteger,
        "integer",
        "$path.dictionary",
        diffs,
    )
    if _requireproperty!(
        dictionary,
        "indexType",
        x -> x isa AbstractDict,
        "object",
        "$path.dictionary",
        diffs,
    )
        indextype = dictionary["indexType"]
        _validatejsontype!(indextype, "$path.dictionary.indexType", diffs)
        get(indextype, "name", nothing) == "int" || _shapediff!(
            diffs,
            "$path.dictionary.indexType.name",
            "dictionary index type must be int",
        )
    end
    _requireproperty!(
        dictionary,
        "isOrdered",
        x -> x isa Bool,
        "Bool",
        "$path.dictionary",
        diffs,
    )
    idok || return
    id = try
        Int64(dictionary["id"])
    catch
        _shapediff!(diffs, "$path.dictionary.id", "integer is outside Int64 range")
        return
    end
    get!(dictionaries, id, (field, path))
    return
end

const _COLUMN_ARRAY_PROPERTIES = ("DATA", "OFFSET", "SIZE", "TYPE_ID", "VIEWS")
const _DATA_TYPES = Set((
    "bool",
    "int",
    "floatingpoint",
    "decimal",
    "date",
    "time",
    "timestamp",
    "duration",
    "interval",
    "fixedsizebinary",
))

function _validatearraylength!(
    column::AbstractDict,
    key::String,
    expected::Int,
    path::String,
    diffs::Vector{String};
    required::Bool=true,
)
    if !haskey(column, key)
        required && _shapediff!(diffs, "$path.$key", "missing required array")
        return
    end
    values = column[key]
    if !(values isa AbstractVector)
        _shapediff!(diffs, "$path.$key", "expected array, got $(_jsonkind(values))")
        return
    end
    length(values) == expected || _shapediff!(
        diffs,
        "$path.$key",
        "expected $expected entries from count, got $(length(values))",
    )
    return
end

function _validatecolumnarrays!(
    field::AbstractDict,
    column::AbstractDict,
    count,
    path::String,
    diffs::Vector{String};
    dictionaryvalue::Bool=false,
)
    count isa Integer || return
    count < 0 && (_shapediff!(diffs, "$path.count", "must be non-negative"); return)
    n = try
        Int(count)
    catch
        _shapediff!(diffs, "$path.count", "integer is outside Int range")
        return
    end
    if haskey(column, "VALIDITY")
        _validatearraylength!(column, "VALIDITY", n, path, diffs)
    end

    type = get(field, "type", nothing)
    type isa AbstractDict || return
    name =
        !dictionaryvalue && haskey(field, "dictionary") ? "dictionary" :
        get(type, "name", nothing)
    name isa AbstractString || return
    lengths = Dict{String,Int}()
    if name == "dictionary" || name in _DATA_TYPES
        lengths["DATA"] = n
    elseif name in ("utf8", "largeutf8", "binary", "largebinary")
        lengths["DATA"] = n
        lengths["OFFSET"] = n + 1
    elseif name in ("utf8view", "binaryview")
        lengths["VIEWS"] = n
        if haskey(column, "VARIADIC_DATA_BUFFERS") &&
           !(column["VARIADIC_DATA_BUFFERS"] isa AbstractVector)
            _shapediff!(
                diffs,
                "$path.VARIADIC_DATA_BUFFERS",
                "expected array, got $(_jsonkind(column["VARIADIC_DATA_BUFFERS"]))",
            )
        end
    elseif name in ("list", "largelist", "map")
        lengths["OFFSET"] = n + 1
    elseif name in ("listview", "largelistview")
        lengths["OFFSET"] = n
        lengths["SIZE"] = n
    elseif name == "union"
        lengths["TYPE_ID"] = n
        get(type, "mode", nothing) == "DENSE" && (lengths["OFFSET"] = n)
    end
    for (key, expected) in lengths
        _validatearraylength!(column, key, expected, path, diffs)
    end
    for key in _COLUMN_ARRAY_PROPERTIES
        haskey(column, key) &&
            !haskey(lengths, key) &&
            _shapediff!(diffs, "$path.$key", "unexpected array for $name")
    end
    if name != "utf8view" && name != "binaryview" && haskey(column, "VARIADIC_DATA_BUFFERS")
        _shapediff!(diffs, "$path.VARIADIC_DATA_BUFFERS", "unexpected array for $name")
    end
    return
end

function _validatecolumn!(
    field::AbstractDict,
    column,
    path::String,
    diffs::Vector{String};
    checkname::Bool=true,
    dictionaryvalue::Bool=false,
)
    if !(column isa AbstractDict)
        _shapediff!(diffs, path, "expected object, got $(_jsonkind(column))")
        return nothing
    end
    nameok =
        _requireproperty!(column, "name", x -> x isa AbstractString, "string", path, diffs)
    fieldname = get(field, "name", nothing)
    if checkname && nameok && fieldname isa AbstractString && fieldname != column["name"]
        _shapediff!(
            diffs,
            "$path.name",
            "$(repr(column["name"])) does not match schema field $(repr(fieldname))",
        )
    end
    countok = _requireproperty!(column, "count", _isjsoninteger, "integer", path, diffs)
    countok &&
        _validatecolumnarrays!(field, column, column["count"], path, diffs; dictionaryvalue)
    children = get(column, "children", Any[])
    if !(children isa AbstractVector)
        _shapediff!(diffs, "$path.children", "expected array, got $(_jsonkind(children))")
        children = Any[]
    end
    fieldchildren =
        !dictionaryvalue && haskey(field, "dictionary") ? Any[] :
        get(field, "children", Any[])
    if fieldchildren isa AbstractVector
        length(children) == length(fieldchildren) || _shapediff!(
            diffs,
            "$path.children",
            "expected $(length(fieldchildren)) child columns, got $(length(children))",
        )
        for index = 1:min(length(children), length(fieldchildren))
            childfield = fieldchildren[index]
            childfield isa AbstractDict || continue
            _validatecolumn!(childfield, children[index], "$path.children[$index]", diffs)
        end
    end
    return countok ? column["count"] : nothing
end

function _validatedocument!(document, path::String, diffs::Vector{String})
    if !(document isa AbstractDict)
        _shapediff!(diffs, path, "expected object, got $(_jsonkind(document))")
        return
    end
    if !_requireproperty!(
        document,
        "schema",
        x -> x isa AbstractDict,
        "object",
        path,
        diffs,
    )
        return
    end
    schema = document["schema"]
    _validatemetadata!(schema, "$path.schema", diffs)
    if !_requireproperty!(
        schema,
        "fields",
        x -> x isa AbstractVector,
        "array",
        "$path.schema",
        diffs,
    )
        return
    end
    fields = schema["fields"]
    dictionaryfields = Dict{Int64,Tuple{Any,String}}()
    for (index, field) in enumerate(fields)
        _validatefield!(field, "$path.schema.fields[$index]", dictionaryfields, diffs)
    end

    if _requireproperty!(
        document,
        "batches",
        x -> x isa AbstractVector,
        "array",
        path,
        diffs,
    )
        for (batchindex, batch) in enumerate(document["batches"])
            batchpath = "$path.batches[$batchindex]"
            if !(batch isa AbstractDict)
                _shapediff!(diffs, batchpath, "expected object, got $(_jsonkind(batch))")
                continue
            end
            countok = _requireproperty!(
                batch,
                "count",
                _isjsoninteger,
                "integer",
                batchpath,
                diffs,
            )
            if !_requireproperty!(
                batch,
                "columns",
                x -> x isa AbstractVector,
                "array",
                batchpath,
                diffs,
            )
                continue
            end
            columns = batch["columns"]
            length(columns) == length(fields) || _shapediff!(
                diffs,
                "$batchpath.columns",
                "expected $(length(fields)) columns, got $(length(columns))",
            )
            for index = 1:min(length(columns), length(fields))
                field = fields[index]
                field isa AbstractDict || continue
                columncount = _validatecolumn!(
                    field,
                    columns[index],
                    "$batchpath.columns[$index]",
                    diffs,
                )
                if countok && columncount !== nothing && columncount != batch["count"]
                    _shapediff!(
                        diffs,
                        "$batchpath.columns[$index].count",
                        "$columncount does not match batch count $(batch["count"])",
                    )
                end
            end
        end
    end

    entries = get(document, "dictionaries", Any[])
    if !(entries isa AbstractVector)
        _shapediff!(
            diffs,
            "$path.dictionaries",
            "expected array, got $(_jsonkind(entries))",
        )
        return
    end
    seen = Set{Int64}()
    for (entryindex, entry) in enumerate(entries)
        entrypath = "$path.dictionaries[$entryindex]"
        if !(entry isa AbstractDict)
            _shapediff!(diffs, entrypath, "expected object, got $(_jsonkind(entry))")
            continue
        end
        idok = _requireproperty!(entry, "id", _isjsoninteger, "integer", entrypath, diffs)
        dataok = _requireproperty!(
            entry,
            "data",
            x -> x isa AbstractDict,
            "object",
            entrypath,
            diffs,
        )
        (idok && dataok) || continue
        id = try
            Int64(entry["id"])
        catch
            _shapediff!(diffs, "$entrypath.id", "integer is outside Int64 range")
            continue
        end
        id in seen && _shapediff!(diffs, "$entrypath.id", "duplicate dictionary id $id")
        push!(seen, id)
        if !haskey(dictionaryfields, id)
            _shapediff!(
                diffs,
                "$entrypath.id",
                "dictionary id $id is not used by the schema",
            )
            continue
        end
        data = entry["data"]
        countok = _requireproperty!(
            data,
            "count",
            _isjsoninteger,
            "integer",
            "$entrypath.data",
            diffs,
        )
        if !_requireproperty!(
            data,
            "columns",
            x -> x isa AbstractVector,
            "array",
            "$entrypath.data",
            diffs,
        )
            continue
        end
        columns = data["columns"]
        length(columns) == 1 || _shapediff!(
            diffs,
            "$entrypath.data.columns",
            "expected one dictionary value column, got $(length(columns))",
        )
        isempty(columns) && continue
        field, _ = dictionaryfields[id]
        columncount = _validatecolumn!(
            field,
            columns[1],
            "$entrypath.data.columns[1]",
            diffs;
            checkname=false,
            dictionaryvalue=true,
        )
        if countok && columncount !== nothing && columncount != data["count"]
            _shapediff!(
                diffs,
                "$entrypath.data.columns[1].count",
                "$columncount does not match dictionary batch count $(data["count"])",
            )
        end
    end
    for (id, (_, fieldpath)) in dictionaryfields
        id in seen || _shapediff!(
            diffs,
            "$fieldpath.dictionary.id",
            "dictionary id $id has no value batch",
        )
    end
    return
end

function _eq(a, b, path::String, diffs::Vector{String}; strict::Bool=false)
    if a isa _PublicUnion || b isa _PublicUnion
        if !(a isa _PublicUnion && b isa _PublicUnion)
            push!(diffs, "$path: Union value vs non-Union value")
        elseif a.typeid != b.typeid
            push!(diffs, "$path: Union type id $(a.typeid) vs $(b.typeid)")
        else
            _eq(a.value, b.value, path * ".value", diffs; strict)
        end
    elseif a isa AbstractDict && b isa AbstractDict
        ka, kb = Set(keys(a)), Set(keys(b))
        for k in union(ka, kb)
            childpath = path * "." * String(k)
            if !haskey(a, k)
                push!(diffs, "$childpath: missing from actual document")
            elseif !haskey(b, k)
                push!(diffs, "$childpath: missing from expected document")
            else
                _eq(a[k], b[k], childpath, diffs; strict)
            end
            length(diffs) > 20 && return
        end
    elseif a isa AbstractVector && b isa AbstractVector
        length(a) == length(b) ||
            (push!(diffs, "$path: length $(length(a)) vs $(length(b))"); return)
        for (i, (x, y)) in enumerate(zip(a, b))
            _eq(x, y, path * "[$i]", diffs; strict)
            length(diffs) > 20 && return
        end
    elseif strict
        equal =
            a isa Bool || b isa Bool ? (a isa Bool && b isa Bool && a == b) :
            _isjsoninteger(a) || _isjsoninteger(b) ?
            (_isjsoninteger(a) && _isjsoninteger(b) && a == b) :
            a isa AbstractFloat || b isa AbstractFloat ?
            (a isa AbstractFloat && b isa AbstractFloat && isequal(a, b)) : isequal(a, b)
        equal || push!(
            diffs,
            "$path: $(repr(a)) ($(_jsonkind(a))) vs $(repr(b)) ($(_jsonkind(b)))",
        )
    elseif a isa AbstractFloat || b isa AbstractFloat
        # EXACT equality (± zero unified, NaN equal): a tolerance here would
        # bless changed values. Core materialization has already canonicalized
        # sub-double columns through their declared physical precision.
        equal =
            a isa AbstractFloat && b isa AbstractFloat && ((isnan(a) && isnan(b)) || a == b)
        equal || push!(diffs, "$path: $(repr(a)) vs $(repr(b))")
    elseif a isa Bool || b isa Bool
        (a isa Bool && b isa Bool && a == b) ||
            push!(diffs, "$path: $(repr(a)) vs $(repr(b))")
    elseif a isa Integer || b isa Integer
        (a isa Integer && b isa Integer && Int128(a) == Int128(b)) ||
            push!(diffs, "$path: $(repr(a)) vs $(repr(b))")
    else
        isequal(a, b) || push!(diffs, "$path: $(repr(a)) vs $(repr(b))")
    end
    return
end

# Dictionary ids are adapter bookkeeping, and the gold corpus is not id-stable
# across its own representations. Canonicalize ids by dictionary-typed field
# position. `_logicaldocument` rebuilds the pools in the same traversal order.
function _normalizedictionaryids!(fields)
    nextid = Ref(0)
    function renumber!(f)
        f isa AbstractDict || return
        if get(f, "dictionary", nothing) isa AbstractDict
            f["dictionary"]["id"] = nextid[]
            nextid[] += 1
        end
        foreach(renumber!, get(f, "children", Any[]))
    end
    foreach(renumber!, fields)
    return fields
end

# Map entries-struct names are not round-trip stable in the corpus itself.
# Compare map children structurally by position.
function _normalizemapschema!(fields)
    for field in fields
        field isa AbstractDict || continue
        if get(get(field, "type", Dict()), "name", "") == "map" && haskey(field, "children")
            for child in field["children"]
                child["name"] = "entries"
                for (index, keyvalue) in enumerate(get(child, "children", Any[]))
                    keyvalue["name"] = index == 1 ? "key" : "value"
                end
            end
        end
        haskey(field, "children") && _normalizemapschema!(field["children"])
    end
    return
end

function _normalizemetadata!(value)
    if value isa AbstractDict
        if haskey(value, "metadata")
            metadata = value["metadata"]
            if metadata === nothing || (metadata isa AbstractVector && isempty(metadata))
                delete!(value, "metadata")
            elseif metadata isa AbstractVector
                value["metadata"] = sort(
                    metadata;
                    by=keyvalue -> (String(keyvalue["key"]), String(keyvalue["value"])),
                )
            end
        end
        foreach(_normalizemetadata!, values(value))
    elseif value isa AbstractVector
        foreach(_normalizemetadata!, value)
    end
    return value
end

# Decimal bitWidth is optional with a default of 128 in integration JSON.
function _normalizedecimals!(value)
    if value isa AbstractDict
        if get(value, "name", "") == "decimal" && haskey(value, "precision")
            width = get(value, "bitWidth", 128)
            _isjsoninteger(width) && width == 128 && delete!(value, "bitWidth")
        end
        foreach(_normalizedecimals!, values(value))
    elseif value isa AbstractVector
        foreach(_normalizedecimals!, value)
    end
    return value
end

# Normalize only the schema. `_logicaldocument` replaces the raw batch and
# dictionary envelopes after it materializes their storage-domain values.
function _normalize!(doc::AbstractDict)
    schema = get(doc, "schema", Dict())
    fields = get(schema, "fields", Any[])
    _normalizedictionaryids!(fields)
    _normalizemapschema!(fields)
    _normalizemetadata!(schema)
    _normalizedecimals!(schema)
    return doc
end

_publicvalue(::Missing) = nothing
_publicvalue(value::Pair) = Any[_publicvalue(first(value)), _publicvalue(last(value))]
_publicvalue(value::NamedTuple) =
    Any[Any[String(name), _publicvalue(item)] for (name, item) in pairs(value)]
_publicvalue(value::Tuple) = Any[_publicvalue(item) for item in value]
_publicvalue(value::AbstractVector) = Any[_publicvalue(item) for item in value]
_publicvalue(value) = value

_typecontainsunion(::AC.ArrowType) = false
_typecontainsunion(::AC.UnionType) = true
_typecontainsunion(type::AC.DictionaryType) = _typecontainsunion(type.valuetype)
_fieldcontainsunion(field) =
    _typecontainsunion(field.type) || any(_fieldcontainsunion, field.children)

# Core's ordinary materializer intentionally exposes only the selected Union
# child's Julia value. Conformance comparison additionally needs the selected
# Arrow type id. Walk only fields that contain a Union, preserving that id at
# every depth while applying the same logical masking rules as Core: storage
# below a null parent does not participate in the value.
function _publicvalueat(field, data, index::Int64)
    1 <= index <= data.len || throw(BoundsError(data, index))
    type = data.type

    if type isa AC.UnionType
        typeid =
            AC.loadat(AC.rolebuffer(data, AC.TYPE_IDS), Int8, AC._slotindex0(data, index))
        childfield, childdata, childindex = AC._union_child(field, data, index)
        return _PublicUnion(typeid, _publicvalueat(childfield, childdata, childindex))
    elseif type isa AC.RunEndEncodedType
        run = AC._ree_runindex(data, index)
        return _publicvalueat(field.children[2], data.children[2], run)
    elseif type isa AC.NullType
        return nothing
    end

    AC.isvalid_at(data, index) || return nothing

    if type isa AC.DictionaryType
        width = AC.primwidth(type.indextype)
        dictionaryindex = AC._load_int(
            AC.rolebuffer(data, AC.DATA),
            type.indextype,
            AC._slotbyteoff(data, index, width),
        )
        dictionary = data.dictionary
        dictionary === nothing &&
            throw(AC.ValidationError("dictionary-encoded array without a dictionary"))
        valuefield = AC.dictvaluefield(field, type)
        return _publicvalueat(
            valuefield,
            dictionary,
            AC.checked_add(Int64(dictionaryindex), Int64(1)),
        )
    elseif type isa AC.ListType
        low, high = AC._offsets_at(data, index, type.large)
        childfield, childdata = field.children[1], data.children[1]
        values = Vector{Any}(undef, Int(high - low))
        for item = 1:length(values)
            values[item] =
                _publicvalueat(childfield, childdata, AC.checked_add(low, Int64(item)))
        end
        return values
    elseif type isa AC.FixedSizeListType
        childfield, childdata = field.children[1], data.children[1]
        base = AC.checked_mul(AC._slotindex0(data, index), Int64(type.listsize))
        values = Vector{Any}(undef, type.listsize)
        for item = 1:(type.listsize)
            values[item] =
                _publicvalueat(childfield, childdata, AC.checked_add(base, Int64(item)))
        end
        return values
    elseif type isa AC.StructType
        childindex = AC.checked_add(data.offset, index)
        return Any[
            Any[String(childfield.name), _publicvalueat(childfield, childdata, childindex)]
            for (childfield, childdata) in zip(field.children, data.children)
        ]
    elseif type isa AC.MapType
        low, high = AC._offsets_at(data, index, false)
        entriesfield, entriesdata = field.children[1], data.children[1]
        keyfield, valuefield = entriesfield.children
        keydata, valuedata = entriesdata.children
        values = Vector{Any}(undef, Int(high - low))
        for item = 1:length(values)
            entryindex =
                AC.checked_add(entriesdata.offset, AC.checked_add(low, Int64(item)))
            values[item] = Any[
                _publicvalueat(keyfield, keydata, entryindex),
                _publicvalueat(valuefield, valuedata, entryindex),
            ]
        end
        return values
    elseif type isa AC.ListViewType
        offset, size = AC._listview_range(type, data, index)
        childfield, childdata = field.children[1], data.children[1]
        values = Vector{Any}(undef, Int(size))
        for item = 1:length(values)
            values[item] =
                _publicvalueat(childfield, childdata, AC.checked_add(offset, Int64(item)))
        end
        return values
    end

    return _publicvalue(AC.getvalue(field, data, index))
end

function _logicalcolumn(field, data; name=field.name)
    values = if _fieldcontainsunion(field)
        Any[_publicvalueat(field, data, Int64(index)) for index = 1:(data.len)]
    else
        Any[_publicvalue(value) for value in AC.materialize(field, data)]
    end
    return Dict{String,Any}(
        "name" => String(name),
        "count" => Int(data.len),
        "VALUES" => values,
    )
end

function _logicaldictionaries(fields, dictids, dictionaries)
    result = Any[]
    function visit(field)
        if field.type isa AC.DictionaryType
            id = length(result)
            values = dictionaries[dictids[field]]
            valuefield = AC.dictvaluefield(field, field.type)
            push!(
                result,
                Dict{String,Any}(
                    "id" => id,
                    "data" => Dict{String,Any}(
                        "count" => Int(values.len),
                        "columns" =>
                            Any[_logicalcolumn(valuefield, values; name="DICT")],
                    ),
                ),
            )
        end
        foreach(visit, field.children)
        return
    end
    foreach(visit, fields)
    return result
end

# Integration JSON exposes physical buffers, but every conformance route is a
# value comparison. Materializing through Core gives one canonical form for
# primitive and nested values. In particular, a null List or Map may own an
# arbitrary non-empty child segment, and a null Struct or FixedSizeList hides
# the corresponding child slots. A recursive DATA mask cannot normalize those
# legal layouts because their child lengths and offsets can differ.
function _logicaldocument(doc::AbstractDict, normalized::AbstractDict)
    schema, batches, dictids, dictionaries = ArrowJSON._fromjson(doc)
    result = normalized
    result["batches"] = Any[
        Dict{String,Any}(
            "count" => Int(batch.nrows),
            "columns" => Any[
                _logicalcolumn(field, data) for
                (field, data) in zip(schema.fields, batch.columns)
            ],
        ) for batch in batches
    ]
    pools = _logicaldictionaries(schema.fields, dictids, dictionaries)
    if isempty(pools)
        delete!(result, "dictionaries")
    else
        result["dictionaries"] = pools
    end
    return result
end

function _normalizedschema(schema)
    rendered = ArrowJSON.tojson(schema, AC.RecordBatch[])["schema"]
    _normalizemapschema!(get(rendered, "fields", Any[]))
    _normalizemetadata!(rendered)
    _normalizedecimals!(rendered)
    return rendered
end

function _logicalwindow(field, data, offset::Int, rows::Int)
    if _fieldcontainsunion(field)
        return Any[_publicvalueat(field, data, Int64(offset + index)) for index = 1:rows]
    end
    return Any[_publicvalue(AC.getvalue(field, data, offset + index)) for index = 1:rows]
end

"""
Compare two Core record-batch row windows by schema and normalized values.

This is the canonical comparison for nonzero-offset C Data slices. Rendering
such an array as standalone integration JSON loses its physical base offset;
for nested arrays it can also expose child rows outside the logical window.
The actual and expected offsets are zero-based row offsets within each batch.
"""
function corebatchdiffs(
    actual,
    expected;
    actualoffset::Integer=0,
    expectedoffset::Integer=0,
    rows::Integer=actual.nrows - actualoffset,
)
    ao, eo, n = Int(actualoffset), Int(expectedoffset), Int(rows)
    ao >= 0 || throw(ArgumentError("actual row offset must be non-negative"))
    eo >= 0 || throw(ArgumentError("expected row offset must be non-negative"))
    n >= 0 || throw(ArgumentError("row count must be non-negative"))
    ao + n <= actual.nrows || throw(BoundsError(actual, (ao + 1):(ao + n)))
    eo + n <= expected.nrows || throw(BoundsError(expected, (eo + 1):(eo + n)))

    diffs = String[]
    _eq(
        _normalizedschema(actual.schema),
        _normalizedschema(expected.schema),
        ".schema",
        diffs;
        strict=true,
    )
    isempty(diffs) || return diffs
    for (index, (actualfield, actualdata, expectedfield, expecteddata)) in enumerate(
        zip(actual.schema.fields, actual.columns, expected.schema.fields, expected.columns),
    )
        _eq(
            _logicalwindow(actualfield, actualdata, ao, n),
            _logicalwindow(expectedfield, expecteddata, eo, n),
            ".columns[$index]",
            diffs,
        )
        length(diffs) > 20 && break
    end
    return diffs
end

"""
Return logical-value differences between two integration-JSON documents.

The operation owns schema normalization and Core materialization. It compares
schema before materialization so a missing required schema property remains a
precise difference instead of becoming a parser error. Callers keep their
documents unchanged and do not depend on the comparison implementation.
"""
function documentdiffs(actual, expected)
    diffs = String[]
    _validatedocument!(actual, ".actual", diffs)
    _validatedocument!(expected, ".expected", diffs)
    isempty(diffs) || return diffs
    gotnormalized = _normalize!(deepcopy(actual))
    wantnormalized = _normalize!(deepcopy(expected))
    _eq(
        get(gotnormalized, "schema", nothing),
        get(wantnormalized, "schema", nothing),
        ".schema",
        diffs,
        strict=true,
    )
    isempty(diffs) || return diffs
    got = _logicaldocument(actual, gotnormalized)
    want = _logicaldocument(expected, wantnormalized)
    _eq(got, want, "", diffs)
    return diffs
end

const VERDICT_STATUSES = (:pass, :fail, :skip)

struct Verdict
    family::String
    check::String
    status::Symbol
    detail::String

    function Verdict(family, check, status::Symbol, detail="")
        status in VERDICT_STATUSES ||
            throw(ArgumentError("invalid conformance status: $(repr(status))"))
        return new(String(family), String(check), status, String(detail))
    end
end

function errorverdict(family, check, err)
    if err isa InterruptException || err isa OutOfMemoryError
        throw(err)
    end
    detail = sprint(showerror, err)
    return Verdict(family, check, :fail, first(detail, 200))
end

"""
Run one document producer and convert its result or error to one verdict.
"""
function documentcheck(producer::F, family, check, expected) where {F}
    try
        diffs = documentdiffs(producer(), expected)
        return Verdict(
            family,
            check,
            isempty(diffs) ? :pass : :fail,
            isempty(diffs) ? "" : first(diffs),
        )
    catch err
        return errorverdict(family, check, err)
    end
end

function readjson(path::AbstractString)
    bytes = read(path)
    endswith(path, ".gz") && (bytes = transcode(GzipDecompressor, bytes))
    return JSON.parse(String(bytes))
end

function streamdocument(bytes::Vector{UInt8})
    stream = Arrow.readstream(bytes)
    # Use the reader's id table so shared and nested pool ids survive.
    return ArrowJSON.tojson(stream.schema, stream.batches; dictids=stream.fielddictids)
end

function filedocument(bytes::Vector{UInt8})
    file = Arrow.readfile(bytes)
    batches = AC.RecordBatch[file[i] for i = 1:length(file)]
    return ArrowJSON.tojson(file.schema, batches; dictids=file.fielddictids)
end

function report(
    header::AbstractString,
    verdicts::AbstractVector{Verdict};
    io::IO=stdout,
    checkwidth::Integer=24,
)
    isempty(verdicts) && throw(
        ArgumentError("$header produced no conformance verdicts; refusing an empty pass"),
    )
    npass = count(v -> v.status == :pass, verdicts)
    nfail = count(v -> v.status == :fail, verdicts)
    nskip = count(v -> v.status == :skip, verdicts)
    println(io, "$header: $npass pass, $nfail fail, $nskip skip")
    println(io)
    for item in verdicts
        item.status == :pass && continue
        tag = item.status == :fail ? "FAIL" : "skip"
        println(
            io,
            rpad(tag, 5),
            rpad(item.family, 58),
            rpad(item.check, checkwidth),
            item.detail,
        )
    end
    return nfail
end

end # module ConformanceSupport
