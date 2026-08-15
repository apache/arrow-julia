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
# Arrow integration JSON ("arrowjson") <-> ArrowCore.
#
# The integration JSON format is the cross-implementation conformance
# interchange used by apache/arrow-testing gold files and by archery. This
# module maps it to and from Core `Schema`/`RecordBatch` values so the corpus
# can be tested in every direction (JSON -> Core -> IPC vs gold bytes; gold
# IPC -> Core -> JSON vs gold JSON) and so our own writer output can be
# expressed as JSON for third-party consumers.
#
# Value conventions, pinned from real gold files (arrow-testing
# 1.0.0-littleendian and cpp-21.0.0):
#   * every batch column: {name, count, VALIDITY?, DATA?/OFFSET?/... , children?}
#   * VALIDITY is 0/1 ints; DATA for 8..32-bit ints, floats, bool are JSON
#     scalars; 64-bit ints, decimals, large offsets/sizes are STRINGS
#   * binary/fixedsizebinary/large binary DATA are UPPERCASE HEX strings
#   * views: VIEWS entries {SIZE, INLINED} (hex for binary, utf8 for strings)
#     or {SIZE, PREFIX_HEX, BUFFER_INDEX, OFFSET}, plus VARIADIC_DATA_BUFFERS
#     as hex strings
#   * list-view: OFFSET + SIZE per slot (strings when 64-bit)
#   * REE: no buffers; children run_ends + values
#   * unions: TYPE_ID (+ OFFSET for dense); no VALIDITY
#   * interval MONTH_DAY_NANO: {months, days, nanoseconds} objects;
#     DAY_TIME: {days, milliseconds}; YEAR_MONTH: ints
#   * dictionaries: top-level `dictionaries: [{id, data: {count, columns}}]`,
#     field carries `dictionary: {id, indexType, isOrdered}` and its `type` is
#     the VALUE type
#   * metadata: [{key, value}] lists on schema and fields
# =============================================================================

module ArrowJSON

using JSON
using ..ArrowCore
const AC = ArrowCore

# --- type descriptors -------------------------------------------------------

_timeunit(s) = s == "SECOND" ? AC.SECOND : s == "MILLISECOND" ? AC.MILLISECOND :
    s == "MICROSECOND" ? AC.MICROSECOND : s == "NANOSECOND" ? AC.NANOSECOND :
    error("unknown time unit $s")
_timeunitname(u) = u == AC.SECOND ? "SECOND" : u == AC.MILLISECOND ? "MILLISECOND" :
    u == AC.MICROSECOND ? "MICROSECOND" : "NANOSECOND"

function fromjsontype(t::AbstractDict, children::Vector{Field})::ArrowType
    n = t["name"]
    n == "null" && return NullType()
    n == "bool" && return BoolType()
    n == "int" && return IntType(Int(t["bitWidth"]), Bool(t["isSigned"]))
    n == "floatingpoint" && return FloatType(t["precision"] == "HALF" ? 16 :
        t["precision"] == "SINGLE" ? 32 : 64)
    n == "utf8" && return Utf8Type(false)
    n == "largeutf8" && return Utf8Type(true)
    n == "binary" && return BinaryType(false)
    n == "largebinary" && return BinaryType(true)
    n == "utf8view" && return ViewType(true)
    n == "binaryview" && return ViewType(false)
    n == "fixedsizebinary" && return FixedSizeBinaryType(Int(t["byteWidth"]))
    n == "decimal" && return DecimalType(Int(t["precision"]), Int(t["scale"]),
        Int(get(t, "bitWidth", 128)))
    n == "date" && return DateType(t["unit"] == "DAY" ? AC.DAY : AC.MILLISECOND_DATE)
    n == "time" && return TimeType(_timeunit(t["unit"]), Int(t["bitWidth"]))
    n == "timestamp" && return TimestampType(_timeunit(t["unit"]),
        haskey(t, "timezone") ? String(t["timezone"]) : nothing)
    n == "duration" && return DurationType(_timeunit(t["unit"]))
    n == "interval" && return IntervalType(t["unit"] == "YEAR_MONTH" ? AC.YEAR_MONTH :
        t["unit"] == "DAY_TIME" ? AC.DAY_TIME : AC.MONTH_DAY_NANO)
    n == "list" && return ListType(false)
    n == "largelist" && return ListType(true)
    n == "listview" && return ListViewType(false)
    n == "largelistview" && return ListViewType(true)
    n == "fixedsizelist" && return FixedSizeListType(Int(t["listSize"]))
    n == "struct" && return StructType()
    n == "map" && return MapType(Bool(get(t, "keysSorted", false)))
    n == "union" && return UnionType(t["mode"] == "SPARSE" ? AC.SparseMode : AC.DenseMode,
        Int8[Int8(x) for x in t["typeIds"]])
    n == "runendencoded" && return RunEndEncodedType()
    error("arrowjson: unmapped type $n")
end

function tojsontype(t::ArrowType)
    t isa NullType && return Dict("name" => "null")
    t isa BoolType && return Dict("name" => "bool")
    t isa IntType && return Dict("name" => "int", "bitWidth" => t.bits, "isSigned" => t.signed)
    t isa FloatType && return Dict("name" => "floatingpoint",
        "precision" => t.bits == 16 ? "HALF" : t.bits == 32 ? "SINGLE" : "DOUBLE")
    t isa Utf8Type && return Dict("name" => t.large ? "largeutf8" : "utf8")
    t isa BinaryType && return Dict("name" => t.large ? "largebinary" : "binary")
    t isa ViewType && return Dict("name" => t.utf8 ? "utf8view" : "binaryview")
    t isa FixedSizeBinaryType && return Dict("name" => "fixedsizebinary", "byteWidth" => t.nbytes)
    t isa DecimalType && return Dict("name" => "decimal", "precision" => t.precision,
        "scale" => t.scale, "bitWidth" => t.bits)
    t isa DateType && return Dict("name" => "date",
        "unit" => t.unit == AC.DAY ? "DAY" : "MILLISECOND")
    t isa TimeType && return Dict("name" => "time", "unit" => _timeunitname(t.unit),
        "bitWidth" => t.bits)
    if t isa TimestampType
        d = Dict{String,Any}("name" => "timestamp", "unit" => _timeunitname(t.unit))
        t.timezone === nothing || (d["timezone"] = t.timezone)
        return d
    end
    t isa DurationType && return Dict("name" => "duration", "unit" => _timeunitname(t.unit))
    t isa IntervalType && return Dict("name" => "interval",
        "unit" => t.unit == AC.YEAR_MONTH ? "YEAR_MONTH" :
                  t.unit == AC.DAY_TIME ? "DAY_TIME" : "MONTH_DAY_NANO")
    t isa ListType && return Dict("name" => t.large ? "largelist" : "list")
    t isa ListViewType && return Dict("name" => t.large ? "largelistview" : "listview")
    t isa FixedSizeListType && return Dict("name" => "fixedsizelist", "listSize" => t.listsize)
    t isa StructType && return Dict("name" => "struct")
    t isa MapType && return Dict("name" => "map", "keysSorted" => t.keyssorted)
    t isa UnionType && return Dict("name" => "union",
        "mode" => t.mode == AC.SparseMode ? "SPARSE" : "DENSE",
        "typeIds" => Int.(t.typeids))
    t isa RunEndEncodedType && return Dict("name" => "runendencoded")
    error("arrowjson: unmapped descriptor $(AC.descriptorname(t))")
end

_metadict(m) = m === nothing ? nothing :
    Dict{String,String}(String(kv["key"]) => String(kv["value"]) for kv in m)
_metalist(m) = m === nothing ? nothing :
    [Dict("key" => k, "value" => v) for (k, v) in sort!(collect(m); by=first)]

"""
Parse one JSON field into a Core `Field`. Dictionary-encoded fields become
`DictionaryType(indextype, valuetype, ordered)`; the JSON id is recorded in
`dictids` (an adapter-side table, exactly as the IPC adapter keeps ids).
"""
function fromjsonfield(f::AbstractDict, dictids::IdDict{Field,Int64})::Field
    children = Field[fromjsonfield(c, dictids) for c in get(f, "children", Any[])]
    t = fromjsontype(f["type"], children)
    meta = _metadict(get(f, "metadata", nothing))
    if haskey(f, "dictionary")
        d = f["dictionary"]
        idx = fromjsontype(d["indexType"], Field[])::IntType
        cf = Field(String(f["name"]), DictionaryType(idx, t, Bool(get(d, "isOrdered", false)));
            nullable=Bool(f["nullable"]), metadata=meta, children=children)
        dictids[cf] = Int64(d["id"])
        return cf
    end
    return Field(String(f["name"]), t; nullable=Bool(f["nullable"]), metadata=meta,
        children=children)
end

function tojsonfield(f::Field, dictids::IdDict{Field,Int64})
    t = f.type
    d = Dict{String,Any}("name" => f.name, "nullable" => f.nullable,
        "children" => Any[tojsonfield(c, dictids) for c in f.children])
    if t isa DictionaryType
        d["type"] = tojsontype(t.valuetype)
        d["dictionary"] = Dict("id" => dictids[f], "indexType" => tojsontype(t.indextype),
            "isOrdered" => t.ordered)
    else
        d["type"] = tojsontype(t)
    end
    f.metadata === nothing || (d["metadata"] = _metalist(f.metadata))
    return d
end

# --- values: JSON -> buffers --------------------------------------------------

_hex(bytes) = uppercase(bytes2hex(bytes))
_unhex(s::AbstractString) = hex2bytes(s)
_i64(x) = x isa AbstractString ? parse(Int64, x) : Int64(x)
_u64(x) = x isa AbstractString ? parse(UInt64, x) : UInt64(x)

function _validity(col, n::Int)
    v = get(col, "VALIDITY", nothing)
    (v === nothing || n == 0) && return BufferSlice()
    bytes = zeros(UInt8, cld(n, 8))
    for i = 1:n
        v[i] != 0 && (bytes[(i - 1) ÷ 8 + 1] |= UInt8(1) << ((i - 1) % 8))
    end
    return AC._databuffer(bytes)
end

_bitmap(vals::AbstractVector{Bool}) = begin
    n = length(vals)
    bytes = zeros(UInt8, cld(n, 8))
    for i = 1:n
        vals[i] && (bytes[(i - 1) ÷ 8 + 1] |= UInt8(1) << ((i - 1) % 8))
    end
    AC._databuffer(bytes)
end

function _intdata(t::IntType, data)
    T = t.signed ? (t.bits == 8 ? Int8 : t.bits == 16 ? Int16 : t.bits == 32 ? Int32 : Int64) :
        (t.bits == 8 ? UInt8 : t.bits == 16 ? UInt16 : t.bits == 32 ? UInt32 : UInt64)
    vals = T[T(x isa AbstractString ? parse(T, x) : x) for x in data]
    return AC._databuffer(vals)
end

_decimalint(s, bits) = bits == 32 ? Int32(parse(Int128, s)) :
    bits == 64 ? Int64(parse(Int128, s)) :
    bits == 128 ? parse(Int128, s) : error("decimal256 values are outside this prove-out")

"""
Build one Core `ArrayData` from a JSON column. `f` supplies the layout;
`dicts` resolves dictionary ids to already-built pools.
"""
function fromjsoncolumn(f::Field, col::AbstractDict, dicts::Dict{Int64,ArrayData},
    dictids::IdDict{Field,Int64})::ArrayData
    t = f.type
    n = Int(col["count"])
    data = get(col, "DATA", nothing)
    validity = _validity(col, n)
    nulls = get(col, "VALIDITY", nothing) === nothing ? 0 :
        count(==(0), col["VALIDITY"][1:n])
    if t isa DictionaryType
        idx = _intdata(t.indextype, data)
        return ArrayData(t, n, [validity, idx]; dictionary=dicts[dictids[f]],
            nullcount=nulls)
    elseif t isa NullType
        return ArrayData(t, n, BufferSlice[]; nullcount=n)
    elseif t isa BoolType
        return ArrayData(t, n, [validity, _bitmap(Bool[Bool(x) for x in data])];
            nullcount=nulls)
    elseif t isa IntType
        return ArrayData(t, n, [validity, _intdata(t, data)]; nullcount=nulls)
    elseif t isa FloatType
        vals = t.bits == 16 ? Float16[Float16(x) for x in data] :
            t.bits == 32 ? Float32[Float32(x) for x in data] : Float64[Float64(x) for x in data]
        return ArrayData(t, n, [validity, AC._databuffer(vals)]; nullcount=nulls)
    elseif t isa DecimalType
        vals = [_decimalint(String(x), t.bits) for x in data]
        raw = t.bits == 32 ? Int32.(vals) : t.bits == 64 ? Int64.(vals) : Int128.(vals)
        return ArrayData(t, n, [validity, AC._databuffer(raw)]; nullcount=nulls)
    elseif t isa DateType
        vals = t.unit == AC.DAY ? Int32[Int32(_i64(x)) for x in data] : Int64[_i64(x) for x in data]
        return ArrayData(t, n, [validity, AC._databuffer(vals)]; nullcount=nulls)
    elseif t isa TimeType
        vals = t.bits == 32 ? Int32[Int32(_i64(x)) for x in data] : Int64[_i64(x) for x in data]
        return ArrayData(t, n, [validity, AC._databuffer(vals)]; nullcount=nulls)
    elseif t isa TimestampType || t isa DurationType
        return ArrayData(t, n, [validity, AC._databuffer(Int64[_i64(x) for x in data])];
            nullcount=nulls)
    elseif t isa IntervalType
        raw = if t.unit == AC.YEAR_MONTH
            reinterpret(UInt8, Int32[Int32(_i64(x)) for x in data])
        elseif t.unit == AC.DAY_TIME
            reinterpret(UInt8, Int32[Int32(_i64(v)) for x in data for v in (x["days"], x["milliseconds"])])
        else
            out = UInt8[]
            for x in data
                append!(out, reinterpret(UInt8, Int32[Int32(_i64(x["months"])), Int32(_i64(x["days"]))]))
                append!(out, reinterpret(UInt8, Int64[_i64(x["nanoseconds"])]))
            end
            out
        end
        return ArrayData(t, n, [validity, AC._databuffer(collect(UInt8, raw))]; nullcount=nulls)
    elseif t isa FixedSizeBinaryType
        bytes = UInt8[]
        for x in data
            b = _unhex(x)
            length(b) == t.nbytes || error("fixedsizebinary width mismatch")
            append!(bytes, b)
        end
        return ArrayData(t, n, [validity, AC._databuffer(bytes)]; nullcount=nulls)
    elseif t isa Utf8Type || t isa BinaryType
        offs = [_i64(x) for x in col["OFFSET"]]
        bytes = UInt8[]
        for x in data
            append!(bytes, t isa Utf8Type ? codeunits(String(x)) : _unhex(x))
        end
        offbuf = t.large ? AC._databuffer(Int64.(offs)) : AC._databuffer(Int32.(offs))
        return ArrayData(t, n, [validity, offbuf, AC._databuffer(bytes)]; nullcount=nulls)
    elseif t isa ViewType
        views = UInt8[]
        for v in col["VIEWS"]
            sz = Int32(v["SIZE"])
            append!(views, reinterpret(UInt8, Int32[sz]))
            if haskey(v, "INLINED")
                inl = t.utf8 ? collect(codeunits(String(v["INLINED"]))) : _unhex(v["INLINED"])
                append!(views, inl)
                append!(views, zeros(UInt8, 12 - length(inl)))
            else
                append!(views, _unhex(v["PREFIX_HEX"]))
                append!(views, reinterpret(UInt8, Int32[Int32(v["BUFFER_INDEX"]), Int32(v["OFFSET"])]))
            end
        end
        bufs = BufferSlice[validity, AC._databuffer(views)]
        for h in get(col, "VARIADIC_DATA_BUFFERS", Any[])
            b = _unhex(h)
            push!(bufs, isempty(b) ? BufferSlice() : AC._databuffer(b))
        end
        return ArrayData(t, n, bufs; nullcount=nulls)
    elseif t isa ListType || t isa MapType
        offs = [_i64(x) for x in col["OFFSET"]]
        offbuf = (t isa ListType && t.large) ? AC._databuffer(Int64.(offs)) :
            AC._databuffer(Int32.(offs))
        child = fromjsoncolumn(f.children[1], col["children"][1], dicts, dictids)
        return ArrayData(t, n, [validity, offbuf]; children=[child], nullcount=nulls)
    elseif t isa ListViewType
        offs = [_i64(x) for x in col["OFFSET"]]
        sizes = [_i64(x) for x in col["SIZE"]]
        ob = t.large ? AC._databuffer(Int64.(offs)) : AC._databuffer(Int32.(offs))
        sb = t.large ? AC._databuffer(Int64.(sizes)) : AC._databuffer(Int32.(sizes))
        child = fromjsoncolumn(f.children[1], col["children"][1], dicts, dictids)
        return ArrayData(t, n, [validity, ob, sb]; children=[child], nullcount=nulls)
    elseif t isa FixedSizeListType
        child = fromjsoncolumn(f.children[1], col["children"][1], dicts, dictids)
        return ArrayData(t, n, [validity]; children=[child], nullcount=nulls)
    elseif t isa StructType
        children = ArrayData[fromjsoncolumn(cf, cc, dicts, dictids)
            for (cf, cc) in zip(f.children, col["children"])]
        return ArrayData(t, n, [validity]; children=children, nullcount=nulls)
    elseif t isa UnionType
        ids = AC._databuffer(Int8[Int8(x) for x in col["TYPE_ID"]])
        children = ArrayData[fromjsoncolumn(cf, cc, dicts, dictids)
            for (cf, cc) in zip(f.children, col["children"])]
        if t.mode == AC.DenseMode
            offs = AC._databuffer(Int32[Int32(x) for x in col["OFFSET"]])
            return ArrayData(t, n, [ids, offs]; children=children, nullcount=0)
        end
        return ArrayData(t, n, [ids]; children=children, nullcount=0)
    elseif t isa RunEndEncodedType
        children = ArrayData[fromjsoncolumn(cf, cc, dicts, dictids)
            for (cf, cc) in zip(f.children, col["children"])]
        return ArrayData(t, n, BufferSlice[]; children=children, nullcount=0)
    end
    error("arrowjson: unmapped layout $(AC.descriptorname(t))")
end

# --- values: Core -> JSON --------------------------------------------------------

_validitylist(d::ArrayData) = Int[AC.isvalid_at(d, i) ? 1 : 0 for i = 1:d.len]

function _rawvals(d::ArrayData, ::Type{T}) where {T}
    b = AC.rolebuffer(d, AC.DATA)
    return T[AC.loadat(b, T, AC._slotbyteoff(d, Int64(i), sizeof(T))) for i = 1:d.len]
end

function _offsetlist(d::ArrayData, wide::Bool)
    b = AC.rolebuffer(d, AC.OFFSETS)
    n = d.len
    if wide
        return Int64[AC.loadat(b, Int64, (d.offset + i) * 8) for i = 0:n]
    end
    return Int32[AC.loadat(b, Int32, (d.offset + i) * 4) for i = 0:n]
end

"""
Render one Core column as an integration-JSON column object. Values are
read through raw buffers (not `getvalue`) so null slots keep their physical
DATA — the gold files carry data under nulls and diff tools compare it.
"""
function tojsoncolumn(f::Field, d::ArrayData)
    t = f.type
    n = Int(d.len)
    col = Dict{String,Any}("name" => f.name, "count" => n)
    hasvalidity(t) = !(t isa NullType || t isa UnionType || t isa RunEndEncodedType)
    hasvalidity(t) && (col["VALIDITY"] = _validitylist(d))
    if t isa DictionaryType
        it = t.indextype
        col["DATA"] = _intjson(it, d)
    elseif t isa NullType
        # nothing
    elseif t isa BoolType
        b = AC.rolebuffer(d, AC.DATA)
        col["DATA"] = Bool[AC.getbit(b, AC._slotindex0(d, Int64(i))) for i = 1:n]
    elseif t isa IntType
        col["DATA"] = _intjson(t, d)
    elseif t isa FloatType
        col["DATA"] = t.bits == 16 ? Float64.(_rawvals(d, Float16)) :
            t.bits == 32 ? _rawvals(d, Float32) : _rawvals(d, Float64)
    elseif t isa DecimalType
        vals = t.bits == 32 ? _rawvals(d, Int32) : t.bits == 64 ? _rawvals(d, Int64) :
            t.bits == 128 ? _rawvals(d, Int128) : error("decimal256 is outside this prove-out")
        col["DATA"] = string.(vals)
    elseif t isa DateType
        col["DATA"] = t.unit == AC.DAY ? _rawvals(d, Int32) : string.(_rawvals(d, Int64))
    elseif t isa TimeType
        col["DATA"] = t.bits == 32 ? _rawvals(d, Int32) : string.(_rawvals(d, Int64))
    elseif t isa TimestampType || t isa DurationType
        col["DATA"] = string.(_rawvals(d, Int64))
    elseif t isa IntervalType
        b = AC.rolebuffer(d, AC.DATA)
        if t.unit == AC.YEAR_MONTH
            col["DATA"] = _rawvals(d, Int32)
        elseif t.unit == AC.DAY_TIME
            col["DATA"] = [Dict("days" => AC.loadat(b, Int32, AC._slotbyteoff(d, Int64(i), 8)),
                "milliseconds" => AC.loadat(b, Int32, AC._slotbyteoff(d, Int64(i), 8) + 4)) for i = 1:n]
        else
            col["DATA"] = [Dict("months" => AC.loadat(b, Int32, AC._slotbyteoff(d, Int64(i), 16)),
                "days" => AC.loadat(b, Int32, AC._slotbyteoff(d, Int64(i), 16) + 4),
                "nanoseconds" => string(AC.loadat(b, Int64, AC._slotbyteoff(d, Int64(i), 16) + 8)))
                for i = 1:n]
        end
    elseif t isa FixedSizeBinaryType
        b = AC.rolebuffer(d, AC.DATA)
        col["DATA"] = [_hex(AC.slicebytes(AC.subslice(b, AC._slotbyteoff(d, Int64(i), t.nbytes), t.nbytes)))
            for i = 1:n]
    elseif t isa Utf8Type || t isa BinaryType
        offs = _offsetlist(d, t.large)
        col["OFFSET"] = t.large ? string.(offs) : offs
        b = AC.rolebuffer(d, AC.DATA)
        col["DATA"] = [begin
            lo, hi = Int64(offs[i]), Int64(offs[i + 1])
            bytes = hi > lo ? AC.slicebytes(AC.subslice(b, lo, hi - lo)) : UInt8[]
            t isa Utf8Type ? String(bytes) : _hex(bytes)
        end for i = 1:n]
    elseif t isa ViewType
        views = AC.rolebuffer(d, AC.VIEWS)
        entries = Any[]
        for i = 1:n
            base = AC._viewbase(d, Int64(i))
            sz = AC.loadat(views, Int32, base)
            if sz <= AC.VIEW_INLINE_MAX
                inl = AC.slicebytes(AC.subslice(views, base + 4, Int64(sz)))
                push!(entries, Dict("SIZE" => sz,
                    "INLINED" => t.utf8 ? String(inl) : _hex(inl)))
            else
                push!(entries, Dict("SIZE" => sz,
                    "PREFIX_HEX" => _hex(AC.slicebytes(AC.subslice(views, base + 4, 4))),
                    "BUFFER_INDEX" => AC.loadat(views, Int32, base + 8),
                    "OFFSET" => AC.loadat(views, Int32, base + 12)))
            end
        end
        col["VIEWS"] = entries
        col["VARIADIC_DATA_BUFFERS"] = [_hex(AC.slicebytes(b)) for b in d.buffers[3:end]]
    elseif t isa ListType || t isa MapType
        wide = t isa ListType && t.large
        offs = _offsetlist(d, wide)
        col["OFFSET"] = wide ? string.(offs) : offs
        col["children"] = Any[tojsoncolumn(f.children[1], d.children[1])]
    elseif t isa ListViewType
        ob = AC.rolebuffer(d, AC.ELEMENT_OFFSETS)
        sb = AC.rolebuffer(d, AC.SIZES)
        w = t.large ? 8 : 4
        offs = [t.large ? AC.loadat(ob, Int64, AC._slotbyteoff(d, Int64(i), w)) :
            AC.loadat(ob, Int32, AC._slotbyteoff(d, Int64(i), w)) for i = 1:n]
        sizes = [t.large ? AC.loadat(sb, Int64, AC._slotbyteoff(d, Int64(i), w)) :
            AC.loadat(sb, Int32, AC._slotbyteoff(d, Int64(i), w)) for i = 1:n]
        col["OFFSET"] = t.large ? string.(offs) : offs
        col["SIZE"] = t.large ? string.(sizes) : sizes
        col["children"] = Any[tojsoncolumn(f.children[1], d.children[1])]
    elseif t isa FixedSizeListType || t isa StructType
        col["children"] = Any[tojsoncolumn(cf, cd) for (cf, cd) in zip(f.children, d.children)]
    elseif t isa UnionType
        ids = AC.rolebuffer(d, AC.TYPE_IDS)
        col["TYPE_ID"] = Int[AC.loadat(ids, Int8, AC._slotindex0(d, Int64(i))) for i = 1:n]
        if t.mode == AC.DenseMode
            ob = AC.rolebuffer(d, AC.ELEMENT_OFFSETS)
            col["OFFSET"] = Int32[AC.loadat(ob, Int32, AC._slotbyteoff(d, Int64(i), 4)) for i = 1:n]
        end
        col["children"] = Any[tojsoncolumn(cf, cd) for (cf, cd) in zip(f.children, d.children)]
    elseif t isa RunEndEncodedType
        col["children"] = Any[tojsoncolumn(cf, cd) for (cf, cd) in zip(f.children, d.children)]
    else
        error("arrowjson: unmapped layout $(AC.descriptorname(t))")
    end
    return col
end

function _intjson(t::IntType, d::ArrayData)
    if t.bits == 64
        return t.signed ? string.(_rawvals(d, Int64)) : string.(_rawvals(d, UInt64))
    end
    return t.signed ? (t.bits == 8 ? _rawvals(d, Int8) : t.bits == 16 ? _rawvals(d, Int16) : _rawvals(d, Int32)) :
        (t.bits == 8 ? _rawvals(d, UInt8) : t.bits == 16 ? _rawvals(d, UInt16) : _rawvals(d, UInt32))
end

# --- documents ---------------------------------------------------------------------

"""
    fromjson(doc) -> (schema::Schema, batches::Vector{RecordBatch}, dictids)

Parse an integration-JSON document (already `JSON.parse`d) into Core values.
Dictionaries are built first (in id order) so batch columns can reference
them; the returned `dictids` maps each dictionary-typed Field to its JSON id
for writers that must preserve ids.
"""
function fromjson(doc::AbstractDict)
    dictids = IdDict{Field,Int64}()
    fields = Field[fromjsonfield(f, dictids) for f in doc["schema"]["fields"]]
    sch = Schema(fields; metadata=_metadict(get(doc["schema"], "metadata", nothing)),
        endianness=AC.LittleEndian)
    dicts = Dict{Int64,ArrayData}()
    # dictionaries may depend on other dictionaries (nested); resolve by
    # repeated passes until all build
    pending = collect(get(doc, "dictionaries", Any[]))
    valuefield = Dict{Int64,Field}()
    function walk(f::Field)
        if f.type isa DictionaryType
            valuefield[dictids[f]] = AC.dictvaluefield(f, f.type)
        end
        foreach(walk, f.children)
        f.type isa DictionaryType && walk(AC.dictvaluefield(f, f.type))
    end
    foreach(walk, fields)
    while !isempty(pending)
        progressed = false
        for (k, entry) in enumerate(pending)
            id = Int64(entry["id"])
            vf = valuefield[id]
            try
                col = entry["data"]["columns"][1]
                dicts[id] = fromjsoncolumn(vf, col, dicts, dictids)
                deleteat!(pending, k)
                progressed = true
                break
            catch e
                e isa KeyError || rethrow()
            end
        end
        progressed || error("arrowjson: unresolvable dictionary dependencies")
    end
    batches = AC.RecordBatch[]
    for b in doc["batches"]
        cols = ArrayData[fromjsoncolumn(f, c, dicts, dictids)
            for (f, c) in zip(fields, b["columns"])]
        push!(batches, AC.RecordBatch(sch, cols, Int(b["count"])))
    end
    return sch, batches, dictids
end

"""
    tojson(schema, batches; dictids) -> Dict

Render Core values as an integration-JSON document. Dictionary pools are
emitted once per id from the first batch that carries them (the file-format
convention; replacement streams need per-batch dictionaries and are outside
this writer).
"""
function tojson(sch::Schema, batches::AbstractVector{AC.RecordBatch};
    dictids::IdDict{Field,Int64}=IdDict{Field,Int64}())
    if isempty(dictids)
        next = Int64(0)
        function assign(f::Field)
            if f.type isa DictionaryType
                dictids[f] = next
                next += 1
            end
            foreach(assign, f.children)
        end
        foreach(assign, sch.fields)
    end
    doc = Dict{String,Any}()
    schemadoc = Dict{String,Any}("fields" => Any[tojsonfield(f, dictids) for f in sch.fields])
    sch.metadata === nothing || (schemadoc["metadata"] = _metalist(sch.metadata))
    doc["schema"] = schemadoc
    dictdocs = Any[]
    seen = Set{Int64}()
    function collectpools(f::Field, d::ArrayData)
        if f.type isa DictionaryType
            id = dictids[f]
            if !(id in seen)
                push!(seen, id)
                vf = AC.dictvaluefield(f, f.type)
                pool = d.dictionary::ArrayData
                push!(dictdocs, Dict("id" => id, "data" => Dict("count" => Int(pool.len),
                    "columns" => Any[tojsoncolumn(vf, pool)])))
                collectpools(vf, pool)
            end
            return
        end
        for (cf, cd) in zip(f.children, d.children)
            collectpools(cf, cd)
        end
    end
    for b in batches, (f, d) in zip(sch.fields, b.columns)
        collectpools(f, d)
    end
    isempty(dictdocs) || (doc["dictionaries"] = dictdocs)
    doc["batches"] = Any[Dict("count" => Int(b.nrows),
        "columns" => Any[tojsoncolumn(f, d) for (f, d) in zip(sch.fields, b.columns)])
        for b in batches]
    return doc
end

end # module ArrowJSON
