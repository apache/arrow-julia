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
# The write facade: Tables.jl source -> Arrow IPC bytes.
#
# Column building sits on ArrowCore's builders (`fromjulia` and friends) plus
# the Dates conversions the facade owns (Core is deliberately
# conversion-free so the adapters share it unchanged). Batch emission is the
# IPC adapter's `writestream`/`writefile`; every column is validated before
# its bytes are published, exactly as the adapter promises.
# =============================================================================

"""
    Arrow.DictEncode(v)

Mark a column for dictionary encoding: the writer builds a pool of the
column's unique values and encodes slots as indices into it.
"""
struct DictEncode{T,V<:AbstractVector{T}} <: AbstractVector{T}
    data::V
end
Base.size(d::DictEncode) = size(d.data)
Base.getindex(d::DictEncode, i::Int) = d.data[i]

"One (Field, ArrayData) column from a Julia vector, facade conversions included."
function _writecolumn(name::String, v::AbstractVector)
    T = Base.nonmissingtype(eltype(v))
    if eltype(v) === Missing
        t = AC.NullType()
        return AC.Field(name, t; nullable=true),
        AC.ArrayData(t, length(v), AC.BufferSlice[]; nullcount=length(v))
    elseif T === Union{}
        throw(
            ArgumentError(
                "column $name has bottom element type Union{} and cannot infer " *
                "an Arrow type; give the empty column a declared element type",
            ),
        )
    elseif T <: Dates.Date
        return _temporalcolumn(
            name,
            v,
            AC.DateType(AC.DAY),
            x -> Int32(Dates.value(x) - _EPOCH_DAYS),
        )
    elseif T <: Dates.DateTime
        return _temporalcolumn(
            name,
            v,
            AC.TimestampType(AC.MILLISECOND, nothing),
            x -> Int64(Dates.value(x) - Dates.UNIXEPOCH),
        )
    elseif T <: Dates.Time
        return _temporalcolumn(
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
        return _temporalcolumn(name, v, AC.DurationType(unit), x -> Int64(Dates.value(x)))
    elseif T <: NamedTuple
        any(ismissing, v) && throw(
            ArgumentError(
                "missing struct slots are not supported by the writer " *
                "(column $name); wrap fields as nullable children instead",
            ),
        )
        isconcretetype(T) || throw(
            ArgumentError(
                "column $name has abstract NamedTuple element type; give it a " *
                "concrete NamedTuple type with declared field names and types",
            ),
        )
        if fieldcount(T) == 0
            t = AC.StructType()
            return AC.Field(name, t; nullable=false, children=AC.Field[]),
            AC.ArrayData(
                t,
                length(v),
                [AC.BufferSlice()];
                children=AC.ArrayData[],
                nullcount=0,
            )
        end
        # Preserve the declared child types. A value-narrowing comprehension
        # turns an empty child into `Any[]` and an all-missing nullable child
        # into `Missing[]`, so neither can recover its Arrow descriptor.
        cols = NamedTuple{fieldnames(T)}(
            ntuple(
                i -> collect(fieldtype(T, i), (getfield(x, i) for x in v)),
                fieldcount(T),
            ),
        )
        return AC.fromjulia_struct(name, cols)
    elseif T <: AbstractString && T != String
        return AC.fromjulia(name, _missings_to(String, v))
    elseif T === Any || (T <: AbstractVector && eltype(T) === Any)
        # Materialized facade columns are Any-eltype for composite layouts;
        # one narrowing pass recovers list columns (inner vectors narrow
        # element-wise, empties adopt the joined element type).
        w = _narrowlists(v)
        NW = Base.nonmissingtype(eltype(w))
        (NW === Any || (NW <: AbstractVector && eltype(NW) === Any)) && throw(
            ArgumentError(
                "column $name has element type Any and cannot be narrowed to " *
                "a writable Arrow column; give it a concrete element type",
            ),
        )
        return _writecolumn(name, w)
    else
        return AC.fromjulia(name, _plainvector(v))
    end
end

"Narrow an Any-eltype column, recovering list-of-T structure when present."
function _narrowlists(v::AbstractVector)
    w = map(x -> x isa AbstractVector ? map(identity, x) : x, v)
    w = map(identity, w)
    NT = Base.nonmissingtype(eltype(w))
    NT <: AbstractVector || return w
    # Join the inner element types (empties narrow to Union{} and would
    # otherwise poison the join), then retype every inner vector.
    E = Union{}
    for x in w
        x === missing && continue
        isempty(x) && continue
        E = typejoin(E, eltype(x))
    end
    E === Union{} && (E = Any)
    E === Any && return w
    hasm = eltype(w) >: Missing
    S = hasm ? Union{Missing,Vector{E}} : Vector{E}
    return S[x === missing ? missing : convert(Vector{E}, x) for x in w]
end

# An ArrowStrings column IS Utf8View memory: its payload vector is the views
# buffer and its byte buffers are the variadic data buffers — no copy, no
# String materialization; the declared nullability is the column's eltype's.
function _writecolumn(name::String, v::ArrowStrings.ArrowStringVector)
    return AC.fromviewentries(name, v.payloads, v.buffers; nullable=eltype(v) >: Missing)
end

function _writecolumn(name::String, d::DictEncode)
    v = d.data
    pool = unique(skipmissing(v))
    lookup = Dict{Any,Int32}(x => Int32(i - 1) for (i, x) in enumerate(pool))
    indices = Union{Missing,Int32}[x === missing ? missing : lookup[x] for x in v]
    return AC.fromjulia_dict(name, collect(pool), indices)
end

# Days from Julia's Date epoch (0000-12-31) to the Arrow epoch (1970-01-01).
const _EPOCH_DAYS = Dates.value(Dates.Date(1970, 1, 1))

"Concrete Vector with an exact Union{Missing,T} or T eltype for fromjulia."
function _plainvector(v::AbstractVector)
    T = eltype(v)
    return v isa Vector{T} ? v : collect(T, v)
end

_missings_to(::Type{S}, v) where {S} =
    eltype(v) >: Missing ? Union{Missing,S}[x === missing ? missing : S(x) for x in v] :
    S[S(x) for x in v]

"Temporal column: convert values to storage integers, keep the validity."
function _temporalcolumn(
    name::String,
    v::AbstractVector,
    t::AC.ArrowType,
    tostorage::F,
) where {F}
    storage = Union{Missing,Int64}[x === missing ? missing : Int64(tostorage(x)) for x in v]
    f0, d0 = AC.fromjulia(name, storage)
    # Rebuild under the temporal descriptor with the storage width it
    # declares (Date32 narrows to Int32 storage).
    width = AC.primwidth(t)
    buffers = d0.buffers
    if width == 4
        narrow = Vector{Int32}(undef, length(v))
        for (i, x) in enumerate(storage)
            narrow[i] = x === missing ? Int32(0) : Int32(x)
        end
        buffers = [d0.buffers[1], AC._databuffer(narrow)]
    end
    d = AC._arraydata(
        t,
        d0.len,
        buffers,
        0,
        AC.ArrayData[],
        nothing,
        d0.owner,
        AC.nullcount(d0),
    )
    return AC.Field(name, t; nullable=eltype(v) >: Missing), d
end

# --- retained-schema rewrite (facade Table/Stream round-trips) --------------

"Storage integers for a public column under a RETAINED temporal descriptor."
function _retainedstorage(t::AC.ArrowType, v::AbstractVector, name::String)
    out = Union{Missing,Int64}[]
    sizehint!(out, length(v))
    for x in v
        if x === missing
            push!(out, missing)
        else
            ok, sv = _storagevalue(t, x)
            ok && sv isa Integer || throw(
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

_fieldmetadata(f::AC.Field) =
    f.metadata === nothing ? nothing : collect(Pair{String,String}, f.metadata)

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

function _retainedvarbytes(f::AC.Field, v::AbstractVector)
    t = f.type::Union{AC.Utf8Type,AC.BinaryType}
    present = _retainedvalidity(f, v)
    Offset = t.large ? Int64 : Int32
    offsets = Vector{Offset}(undef, length(v) + 1)
    offsets[1] = zero(Offset)
    data = UInt8[]
    for (i, x) in enumerate(v)
        if x !== missing
            bytes = if t isa AC.Utf8Type
                x isa AbstractString ||
                    throw(ArgumentError("column $(f.name) must contain string values"))
                codeunits(x)
            else
                x isa AbstractVector{UInt8} || throw(
                    ArgumentError("column $(f.name) must contain byte-vector values"),
                )
                x
            end
            length(data) <= typemax(Offset) - length(bytes) || throw(
                ArgumentError("column $(f.name) data exceeds its retained offset width"),
            )
            append!(data, bytes)
        end
        offsets[i + 1] = Offset(length(data))
    end
    databuf = isempty(data) ? AC.BufferSlice() : AC._databuffer(data)
    d = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), AC._databuffer(offsets), databuf];
        nullcount=count(!, present),
    )
    return _retainedfield(f), d
end

function _retainedview(f::AC.Field, v::AbstractVector)
    t = f.type::AC.ViewType
    present = _retainedvalidity(f, v)
    payloads = Vector{ArrowStrings.ArrowStringPayload}(undef, length(v))
    data = UInt8[]
    for (i, x) in enumerate(v)
        if x === missing
            payloads[i] = ArrowStrings.PAYLOAD_MISSING
            continue
        end
        bytes = if t.utf8
            x isa AbstractString ||
                throw(ArgumentError("column $(f.name) must contain string values"))
            codeunits(x)
        else
            x isa AbstractVector{UInt8} ||
                throw(ArgumentError("column $(f.name) must contain byte-vector values"))
            x
        end
        n = length(bytes)
        if n <= ArrowStrings.INLINE_MAX
            payloads[i] = ArrowStrings.inline_payload(bytes, 1, n)
        else
            length(data) <= typemax(Int32) - n || throw(
                ArgumentError("column $(f.name) view data exceeds the Int32 offset range"),
            )
            off = length(data)
            append!(data, bytes)
            payloads[i] = ArrowStrings.view_payload(data, off + 1, n, 0, off)
        end
    end
    buffers = AC.BufferSlice[AC._bitmapbuffer(present), AC._databuffer(payloads)]
    isempty(data) || push!(buffers, AC._databuffer(data))
    d = AC.ArrayData(t, length(v), buffers; nullcount=count(!, present))
    return _retainedfield(f), d
end

function _retainedfixedbytes(f::AC.Field, v::AbstractVector)
    t = f.type::AC.FixedSizeBinaryType
    present = _retainedvalidity(f, v)
    data = zeros(UInt8, Base.checked_mul(length(v), t.nbytes))
    for (i, x) in enumerate(v)
        x === missing && continue
        x isa AbstractVector{UInt8} ||
            throw(ArgumentError("column $(f.name) must contain byte-vector values"))
        length(x) == t.nbytes || throw(
            ArgumentError(
                "column $(f.name) fixed-size value $i has $(length(x)) bytes; " *
                "expected $(t.nbytes)",
            ),
        )
        copyto!(data, (i - 1) * t.nbytes + 1, x, 1, t.nbytes)
    end
    d = AC.ArrayData(
        t,
        length(v),
        [AC._bitmapbuffer(present), AC._databuffer(data)];
        nullcount=count(!, present),
    )
    return _retainedfield(f), d
end

function _retaineddecimal(f::AC.Field, v::AbstractVector)
    t = f.type::AC.DecimalType
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
    out = Vector{T}(undef, length(values))
    for (i, x) in enumerate(values)
        x isa T || throw(
            ArgumentError(
                "column $(f.name) holds $(typeof(x)) values that do not match " *
                "its retained Arrow type $(repr(f.type))",
            ),
        )
        out[i] = x
    end
    return out
end

"A physical child value hidden by a null composite parent."
function _retainedplaceholder(f::AC.Field)
    f.nullable && return missing
    t = f.type
    t isa AC.NullType && return missing
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
    t isa AC.FixedSizeBinaryType && return zeros(UInt8, t.nbytes)
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
        return Any[_retainedplaceholder(f.children[1]) for _ = 1:t.listsize]
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
        return _retainedplaceholder(f.children[2])
    end
    throw(
        ArgumentError(
            "cannot synthesize hidden child data for retained $(AC.descriptorname(t)) " *
            "field $(f.name)",
        ),
    )
end

"Write values materialized inside a composite, where temporal values stay raw."
function _retainedchildcolumn(f::AC.Field, values)
    v = _retainedtypedvalues(f, values; converted=false)
    t = f.type
    if t isa AC.DateType ||
       t isa AC.TimestampType ||
       t isa AC.TimeType ||
       t isa AC.DurationType
        storage = Union{Missing,Int64}[x === missing ? missing : Int64(x) for x in v]
        return _rebuildtemporal(f, storage, length(v))
    end
    t isa AC.DictionaryType && throw(
        ArgumentError(
            "nested retained dictionary field $(f.name) cannot be reconstructed " *
            "after facade materialization because its pool is unavailable",
        ),
    )
    return _writecolumn(f, v)
end

function _retainedlist(f::AC.Field, v::AbstractVector)
    t = f.type::Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType}
    length(f.children) == 1 ||
        throw(ArgumentError("column $(f.name) retained list descriptor needs one child"))
    present = _retainedvalidity(f, v)
    flat = Any[]
    if t isa AC.ListType
        Offset = t.large ? Int64 : Int32
        offsets = Vector{Offset}(undef, length(v) + 1)
        offsets[1] = zero(Offset)
        for (i, row) in enumerate(v)
            if row !== missing
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
            if row === missing
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
            if row === missing
                append!(flat, (_retainedplaceholder(f.children[1]) for _ = 1:t.listsize))
                continue
            end
            row isa AbstractVector || throw(
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
            append!(flat, row)
        end
        buffers = AC.BufferSlice[AC._bitmapbuffer(present)]
    end
    childfield, childdata = _retainedchildcolumn(f.children[1], flat)
    d = AC.ArrayData(
        t,
        length(v),
        buffers;
        children=AC.ArrayData[childdata],
        nullcount=count(!, present),
    )
    return _retainedfield(f; children=AC.Field[childfield]), d
end

function _retainedstruct(f::AC.Field, v::AbstractVector)
    t = f.type::AC.StructType
    present = _retainedvalidity(f, v)
    nchildren = length(f.children)
    childvalues = [Any[] for _ = 1:nchildren]
    for row in v
        if row === missing
            for j = 1:nchildren
                push!(childvalues[j], _retainedplaceholder(f.children[j]))
            end
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
        for j = 1:nchildren
            kv = row[j]
            kv isa Pair || throw(
                ArgumentError("column $(f.name) retained struct rows must contain Pairs"),
            )
            first(kv) == f.children[j].name || throw(
                ArgumentError(
                    "column $(f.name) retained struct child $j is named " *
                    "$(repr(first(kv))); expected $(repr(f.children[j].name))",
                ),
            )
            push!(childvalues[j], last(kv))
        end
    end
    children = AC.ArrayData[]
    childfields = AC.Field[]
    for j = 1:nchildren
        cf, cd = _retainedchildcolumn(f.children[j], childvalues[j])
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

function _retainedmap(f::AC.Field, v::AbstractVector)
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
        if row !== missing
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
                push!(keys, first(kv))
                push!(values, last(kv))
            end
        end
        offsets[i + 1] = Int32(length(keys))
    end
    keyfield, keydata = _retainedchildcolumn(entries.children[1], keys)
    valuefield, valuedata = _retainedchildcolumn(entries.children[2], values)
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

function _retainedree(f::AC.Field, v::AbstractVector)
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
        if isempty(runvalues) || !isequal(x, last(runvalues))
            push!(runvalues, x)
            push!(runends, RT(i))
        else
            runends[end] = RT(i)
        end
    end
    rebuiltrunfield, runenddata = _writecolumn(runfield, runends)
    rebuiltvaluefield, valuedata = _retainedchildcolumn(valuefield, runvalues)
    d = AC.ArrayData(
        t,
        length(v),
        AC.BufferSlice[];
        children=AC.ArrayData[runenddata, valuedata],
        nullcount=0,
    )
    return _retainedfield(f; children=AC.Field[rebuiltrunfield, rebuiltvaluefield]), d
end

"Build one column under a retained Field: descriptor, nullability, metadata."
function _writecolumn(f::AC.Field, v::AbstractVector)
    t = f.type
    if t isa AC.NullType
        eltype(v) === Missing || throw(
            ArgumentError(
                "column $(f.name) no longer matches its retained Arrow NullType; " *
                "give it a Missing element type",
            ),
        )
        d = AC.ArrayData(t, length(v), AC.BufferSlice[]; nullcount=length(v))
        return AC.Field(
            f.name,
            t;
            nullable=f.nullable,
            metadata=f.metadata === nothing ? nothing :
                     collect(Pair{String,String}, f.metadata),
        ),
        d
    end
    # Identity FIRST, for every retained field with a known facade type:
    # a replaced column is rejected on its declared element type before any
    # value is read.
    Fp = _facadebasetype(t)
    if Fp !== Any
        NT = Base.nonmissingtype(eltype(v))
        NT <: Fp ||
            NT === Union{} ||
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
    if t isa AC.DateType ||
       t isa AC.TimestampType ||
       t isa AC.TimeType ||
       t isa AC.DurationType
        if Fp === Int64
            storage = Union{Missing,Int64}[x === missing ? missing : Int64(x) for x in v]
        else
            storage = _retainedstorage(t, v, f.name)
        end
        return _rebuildtemporal(f, storage, length(v))
    end
    t isa Union{AC.Utf8Type,AC.BinaryType} && return _retainedvarbytes(f, v)
    t isa AC.ViewType && return _retainedview(f, v)
    t isa AC.FixedSizeBinaryType && return _retainedfixedbytes(f, v)
    t isa AC.DecimalType && return _retaineddecimal(f, v)
    t isa AC.IntervalType && return _retainedinterval(f, v)
    t isa Union{AC.ListType,AC.ListViewType,AC.FixedSizeListType} &&
        return _retainedlist(f, v)
    t isa AC.StructType && return _retainedstruct(f, v)
    t isa AC.MapType && return _retainedmap(f, v)
    t isa AC.RunEndEncodedType && return _retainedree(f, v)
    t isa AC.UnionType && throw(
        ArgumentError(
            "column $(f.name) has a retained UnionType whose child type ids " *
            "cannot be recovered from materialized facade values",
        ),
    )
    # Non-temporal: build naturally, then impose the retained descriptor —
    # types must agree and nullability comes from the RETAINED field (values
    # holding missing under a non-nullable field are a replacement error).
    # List fields impose RECURSIVELY: retained identity includes the child
    # fields (names, nullability, metadata) and each level's list width.
    fn, dn = _writecolumn(f.name, v)
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
        metadata=f.metadata === nothing ? nothing :
                 collect(Pair{String,String}, f.metadata),
        children=collect(AC.Field, fn.children),
    )
    return rebuilt, dn
end

function _rebuildtemporal(f::AC.Field, storage, n)
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
    f0, d0 = AC.fromjulia("x", storage)
    width = AC.primwidth(t)
    buffers = d0.buffers
    if width == 4
        narrow = Vector{Int32}(undef, n)
        for (i, x) in enumerate(storage)
            narrow[i] = x === missing ? Int32(0) : Int32(x)
        end
        buffers = [d0.buffers[1], AC._databuffer(narrow)]
    end
    d = AC._arraydata(
        t,
        d0.len,
        buffers,
        0,
        AC.ArrayData[],
        nothing,
        d0.owner,
        AC.nullcount(d0),
    )
    fld = AC.Field(
        f.name,
        t;
        nullable=f.nullable,
        metadata=f.metadata === nothing ? nothing :
                 collect(Pair{String,String}, f.metadata),
    )
    return fld, d
end

"""
    Arrow.write(sink, table; file=true, compress=nothing,
                metadata=nothing, colmetadata=nothing)

Write any Tables.jl source as Arrow IPC. `sink` is a file path or an `IO`.
`file=true` emits the random-access file format (`ARROW1` magic + footer);
`file=false` the stream format. Each `Tables.partitions` partition becomes
one record batch. `compress` is `nothing`, `:lz4`, or `:zstd`.
`metadata`/`colmetadata` attach schema- and per-column key-value pairs
(a `Dict`, or pairs; `colmetadata` maps column name `Symbol`s to them).

The writer is eager and whole-buffer: batches are encoded and validated in
memory, then written to the sink once.
"""
function write(path::AbstractString, tbl; kwargs...)
    bytes = _writebytes(tbl; kwargs...)
    open(path, "w") do io
        Base.write(io, bytes)
    end
    return path
end

function write(io::IO, tbl; kwargs...)
    bytes = _writebytes(tbl; kwargs...)
    Base.write(io, bytes)
    return io
end

"Retained Arrow schema when the source is a facade read, else nothing."
_retainedschema(t::Table) = getfield(t, :schema)
_retainedschema(s::Stream) = _tableschema(getfield(s, :src))
_retainedschema(::Any) = nothing

"Dictionary pools retained by one facade partition, in column order."
_partitiondictpools(t::Table) = getfield(t, :retainedpools)
_partitiondictpools(::Any) = nothing

"Merge retained dictionary snapshots without changing the first pool's order."
function _mergeddictpool(partpools, j::Int)
    hints =
        Any[pools[j] for pools in partpools if pools !== nothing && pools[j] !== nothing]
    isempty(hints) && return nothing
    out = collect(first(hints))
    for pool in Iterators.drop(hints, 1), x in pool
        any(y -> isequal(y, x), out) || push!(out, x)
    end
    return out
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
function _dictionarypool(vals, retainedpool)
    if retainedpool === nothing
        T = _dictvaluetype(vals)
        T === Union{} && throw(
            ArgumentError(
                "cannot infer a dictionary value type from empty or all-missing columns",
            ),
        )
        pool = Vector{T}()
    else
        pool = collect(retainedpool)
    end
    seen = Dict{Any,Nothing}()
    for x in pool
        x === missing || haskey(seen, x) || (seen[x] = nothing)
    end
    for v in vals, x in v
        if x !== missing && !haskey(seen, x)
            push!(pool, x)
            seen[x] = nothing
        end
    end
    return pool
end

"First pool position for each non-null value; duplicate categories stay intact."
function _dictionarylookup(pool)
    lookup = Dict{Any,Int64}()
    for (i, x) in enumerate(pool)
        x === missing || haskey(lookup, x) || (lookup[x] = Int64(i - 1))
    end
    return lookup
end

_dictionaryindices(v, lookup, missingindex=nothing) = Union{Missing,Int64}[
    x === missing ? (missingindex === nothing ? missing : missingindex) : lookup[x] for
    x in v
]

"One shared-pool dictionary batch: identical pool OBJECT across batches."
function _dictbatch(fld::AC.Field, indices::Vector, pool_d::AC.ArrayData)
    t = fld.type::AC.DictionaryType
    IT = AC.juliatype(t.indextype)
    present = [x !== missing for x in indices]
    inds = IT[x === missing ? zero(IT) : IT(x) for x in indices]
    nc = count(!, present)
    d = AC.ArrayData(
        t,
        length(indices),
        [AC._bitmapbuffer(present), AC._databuffer(inds)];
        dictionary=pool_d,
        nullcount=nc,
    )
    return d
end

"Field + first-batch data for a dictionary column under a RETAINED type."
function _retaineddict(rf::AC.Field, pool::Vector, firstidx::Vector, name::String)
    t = rf.type::AC.DictionaryType
    valuefield = AC.dictvaluefield(rf, t)
    vf, vd = _writecolumn(valuefield, pool)
    AC.typeequal(vf.type, t.valuetype) || throw(
        ArgumentError(
            "column $name pool maps to $(summary(vf.type)) but the retained " *
            "dictionary value type is $(summary(t.valuetype))",
        ),
    )
    IT = AC.juliatype(t.indextype)
    length(pool) - 1 <= typemax(IT) || throw(
        ArgumentError(
            "column $name pool of $(length(pool)) values exceeds the retained " *
            "$(summary(t.indextype)) index range",
        ),
    )
    fld = AC.Field(
        name,
        t;
        nullable=rf.nullable,
        metadata=rf.metadata === nothing ? nothing :
                 collect(Pair{String,String}, rf.metadata),
        children=collect(AC.Field, vf.children),
    )
    return fld, _dictbatch(fld, firstidx, vd)
end

function _writebytes(
    tbl;
    file::Bool=true,
    compress::Union{Nothing,Symbol}=nothing,
    metadata=nothing,
    colmetadata=nothing,
)
    retained = _retainedschema(tbl)
    # Phase 1: materialize every partition's columns (this writer is eager),
    # validating name/order agreement — a drift here would silently bind
    # data to the wrong fields.
    names = Symbol[]
    partcols = Vector{AbstractVector}[]
    partpools = Any[]
    rowcounts = Int[]
    for part in Tables.partitions(tbl)
        cols = Tables.columns(part)
        pnames = collect(Symbol, Tables.columnnames(cols))
        if isempty(partcols)
            names = pnames
        else
            pnames == names || throw(
                ArgumentError(
                    "partition $(length(partcols) + 1) column names $(pnames) " *
                    "do not match the first partition's $(names) (same names, " *
                    "same order); reorder or rename the partition's columns",
                ),
            )
        end
        # Arrow permits duplicate field names. Tables.getcolumn(cols, name)
        # cannot distinguish them, so bind every partition by position.
        push!(
            partcols,
            AbstractVector[Tables.getcolumn(cols, j) for j in eachindex(pnames)],
        )
        pools = _partitiondictpools(part)
        pools !== nothing &&
            length(pools) != length(pnames) &&
            throw(
                ArgumentError(
                    "retained dictionary pool count does not match partition width",
                ),
            )
        push!(partpools, pools)
        n = Int(Tables.rowcount(cols))
        if n == 0 && isempty(pnames)
            n = max(n, Int(Tables.rowcount(part)))
        end
        push!(rowcounts, n)
    end
    isempty(partcols) &&
        throw(ArgumentError("table has no partitions; cannot infer a schema"))
    nparts = length(partcols)
    ncols = length(names)
    retainedaligned =
        retained !== nothing &&
        length(retained.fields) == ncols &&
        all(j -> retained.fields[j].name == String(names[j]), 1:ncols)
    function retainedfield(j)
        retained === nothing && return nothing
        retainedaligned && return retained.fields[j]
        count(==(names[j]), names) == 1 || return nothing
        matches = findall(f -> f.name == String(names[j]), collect(retained.fields))
        return length(matches) == 1 ? retained.fields[only(matches)] : nothing
    end
    # Phase 2: build columns. Dictionary-intent columns (retained
    # DictionaryType or DictEncode input) share ONE pool object across all
    # partitions — the file format carries one dictionary batch per id, and
    # per-partition pools would read as replacement.
    fields = Vector{AC.Field}(undef, ncols)
    coldata = [Vector{AC.ArrayData}(undef, nparts) for _ = 1:ncols]
    for j = 1:ncols
        rf = retainedfield(j)
        dictintent =
            (rf !== nothing && rf.type isa AC.DictionaryType) ||
            any(partcols[k][j] isa DictEncode for k = 1:nparts)
        if dictintent
            vals = [
                partcols[k][j] isa DictEncode ? (partcols[k][j]::DictEncode).data :
                partcols[k][j] for k = 1:nparts
            ]
            poolhint = _mergeddictpool(partpools, j)
            if rf !== nothing
                Fv = _facadebasetype(rf.type)
                for k = 1:nparts
                    NT = Base.nonmissingtype(eltype(vals[k]))
                    Fv !== Any &&
                        !(NT <: Fv) &&
                        NT !== Union{} &&
                        throw(
                            ArgumentError(
                                "column $(names[j]) holds $(NT) values, but its " *
                                "retained dictionary materializes as $(Fv); the " *
                                "column was replaced with incompatible data",
                            ),
                        )
                    # A non-nullable dictionary may still materialize missing
                    # through a valid index into a null pool entry. The
                    # retained pool lets us preserve that distinction.
                    eltype(vals[k]) >: Missing &&
                        !rf.nullable &&
                        (poolhint === nothing || !any(ismissing, poolhint)) &&
                        throw(
                            ArgumentError(
                                "column $(names[j]) may hold missing values but " *
                                "its retained dictionary field is non-nullable",
                            ),
                        )
                end
            end
            rf !== nothing &&
                (rf.type::AC.DictionaryType).ordered &&
                poolhint === nothing &&
                throw(
                    ArgumentError(
                        "column $(names[j]) has an ordered retained dictionary, " *
                        "but its original category pool is unavailable",
                    ),
                )
            pool = _dictionarypool(vals, poolhint)
            lookup = _dictionarylookup(pool)
            missingindex =
                rf !== nothing && !rf.nullable ? findfirst(ismissing, pool) : nothing
            missingindex === nothing || (missingindex = Int64(missingindex - 1))
            firstidx = _dictionaryindices(vals[1], lookup, missingindex)
            if rf === nothing
                length(pool) - 1 <= typemax(Int32) || throw(
                    ArgumentError(
                        "column $(names[j]) dictionary exceeds the Int32 index range",
                    ),
                )
                fld, d1 = AC.fromjulia_dict(String(names[j]), pool, firstidx)
            else
                fld, d1 = _retaineddict(rf, pool, firstidx, String(names[j]))
            end
            fields[j] = fld
            coldata[j][1] = d1
            pool_d = d1.dictionary::AC.ArrayData
            for k = 2:nparts
                idx = _dictionaryindices(vals[k], lookup, missingindex)
                coldata[j][k] = _dictbatch(fld, idx, pool_d)
            end
        else
            local firstfield::AC.Field
            for k = 1:nparts
                fk, dk =
                    rf === nothing ? _writecolumn(String(names[j]), partcols[k][j]) :
                    _writecolumn(rf, partcols[k][j])
                if k == 1
                    firstfield = fk
                else
                    AC.typeequal(fk.type, firstfield.type) || throw(
                        ArgumentError(
                            "partition $k column $(names[j]) maps to Arrow " *
                            "type $(repr(fk.type)), but the first partition " *
                            "declared $(repr(firstfield.type)); make the " *
                            "column types agree across partitions",
                        ),
                    )
                    fk.nullable &&
                        !firstfield.nullable &&
                        throw(
                            ArgumentError(
                                "partition $k column $(names[j]) is nullable but " *
                                "the first partition declared it non-nullable; " *
                                "make the first partition's column eltype " *
                                "Union{Missing,T} to widen the schema",
                            ),
                        )
                end
                coldata[j][k] = dk
            end
            fields[j] = firstfield
        end
    end
    outfields = AC.Field[_withcolmeta(fields[j], colmetadata) for j = 1:ncols]
    schmeta =
        metadata !== nothing ? _metapairs(metadata) :
        (
            retained === nothing || retained.metadata === nothing ? nothing :
            collect(Pair{String,String}, retained.metadata)
        )
    sch = AC.Schema(outfields; metadata=schmeta)
    batches = AC.RecordBatch[
        AC.RecordBatch(sch, AC.ArrayData[coldata[j][k] for j = 1:ncols], rowcounts[k])
        for k = 1:nparts
    ]
    codec = compress === nothing ? :none : compress
    return file ? writefile(sch, batches; compress=codec) :
           writestream(sch, batches; compress=codec)
end

_metapairs(::Nothing) = nothing
function _metapairs(m)
    entries = m isa AbstractDict || m isa NamedTuple ? Base.pairs(m) : m
    out = Pair{String,String}[]
    for kv in entries
        kv isa Pair || throw(ArgumentError("metadata sequences must contain Pair values"))
        push!(out, String(first(kv)) => String(last(kv)))
    end
    return out
end

_withcolmeta(f::AC.Field, ::Nothing) = f
function _withcolmeta(f::AC.Field, colmetadata)
    cm = get(Dict(colmetadata), Symbol(f.name), nothing)
    cm === nothing && return f
    return AC.Field(
        f.name,
        f.type;
        nullable=f.nullable,
        metadata=_metapairs(cm),
        children=collect(AC.Field, f.children),
    )
end
