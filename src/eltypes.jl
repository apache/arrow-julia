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
Given a flatbuffers metadata type definition (a Field instance from Schema.fbs),
translate to the appropriate Julia storage eltype
"""
function juliaeltype end

finaljuliatype(T) = T
finaljuliatype(::Type{Missing}) = Missing
finaljuliatype(::Type{Union{T,Missing}}) where {T} = Union{Missing,finaljuliatype(T)}

const RUN_END_ENCODED_UNSUPPORTED = "Run-End Encoded arrays are not supported yet"
const BOOL8_SYMBOL = Symbol("arrow.bool8")
const JSON_SYMBOL = Symbol("arrow.json")
const OPAQUE_SYMBOL = Symbol("arrow.opaque")
const PARQUET_VARIANT_SYMBOL = Symbol("arrow.parquet.variant")
const FIXED_SHAPE_TENSOR_SYMBOL = Symbol("arrow.fixed_shape_tensor")
const VARIABLE_SHAPE_TENSOR_SYMBOL = Symbol("arrow.variable_shape_tensor")

@inline _canonicalextensionerror(sym::Symbol, msg::AbstractString) =
    throw(ArgumentError("invalid canonical $(String(sym)) extension: $msg"))

@inline _fieldchildren(field::Meta.Field) =
    field.children === nothing ? Meta.Field[] : field.children

@inline _jsonhaskey(x, key::AbstractString) = haskey(x, key)
@inline _jsonget(x, key::AbstractString) = x[key]

function _parsecanonicalmetadata(sym::Symbol, metadata::String; required::Bool=false)
    isempty(metadata) &&
        return required ? _canonicalextensionerror(sym, "metadata is required") : nothing
    value = try
        JSON3.read(metadata)
    catch
        _canonicalextensionerror(sym, "metadata must be valid JSON")
    end
    value isa JSON3.Object ||
        _canonicalextensionerror(sym, "metadata must be a JSON object")
    return value
end

function _parseintvector(sym::Symbol, value, label::AbstractString; allow_null::Bool=false)
    value isa AbstractVector ||
        _canonicalextensionerror(sym, "\"$label\" must be a JSON array")
    parsed = Vector{allow_null ? Union{Nothing,Int} : Int}()
    for item in value
        if allow_null && isnothing(item)
            push!(parsed, nothing)
        elseif item isa Integer
            item >= 0 ||
                _canonicalextensionerror(sym, "\"$label\" values must be non-negative")
            push!(parsed, Int(item))
        else
            suffix = allow_null ? "integers or null" : "integers"
            _canonicalextensionerror(sym, "\"$label\" must contain only $suffix")
        end
    end
    return parsed
end

function _parsestringvector(sym::Symbol, value, label::AbstractString)
    value isa AbstractVector ||
        _canonicalextensionerror(sym, "\"$label\" must be a JSON array")
    parsed = String[]
    for item in value
        item isa AbstractString ||
            _canonicalextensionerror(sym, "\"$label\" must contain only strings")
        push!(parsed, String(item))
    end
    return parsed
end

function _validatepermutation(sym::Symbol, permutation::Vector{Int}, ndim::Int)
    length(permutation) == ndim ||
        _canonicalextensionerror(sym, "\"permutation\" must have length $ndim")
    length(unique(permutation)) == ndim ||
        _canonicalextensionerror(sym, "\"permutation\" must not contain duplicates")
    return permutation
end

function _extractdimensionalmetadata(
    sym::Symbol,
    metadata;
    ndim::Union{Nothing,Int}=nothing,
)
    metadata === nothing && return (nothing, nothing, nothing)
    dim_names =
        _jsonhaskey(metadata, "dim_names") ?
        _parsestringvector(sym, _jsonget(metadata, "dim_names"), "dim_names") : nothing
    permutation =
        _jsonhaskey(metadata, "permutation") ?
        _parseintvector(sym, _jsonget(metadata, "permutation"), "permutation") : nothing
    uniform_shape =
        _jsonhaskey(metadata, "uniform_shape") ?
        _parseintvector(
            sym,
            _jsonget(metadata, "uniform_shape"),
            "uniform_shape";
            allow_null=true,
        ) : nothing
    if ndim !== nothing
        dim_names !== nothing && length(dim_names) == ndim ||
            isnothing(dim_names) ||
            _canonicalextensionerror(sym, "\"dim_names\" must have length $ndim")
        permutation !== nothing && _validatepermutation(sym, permutation, ndim)
        uniform_shape !== nothing && length(uniform_shape) == ndim ||
            isnothing(uniform_shape) ||
            _canonicalextensionerror(sym, "\"uniform_shape\" must have length $ndim")
    end
    return dim_names, permutation, uniform_shape
end

@inline _isliststoragetype(x) =
    x isa Union{Meta.List,Meta.LargeList,Meta.ListView,Meta.LargeListView}

@inline _isbinarystoragetype(x) =
    x isa Union{Meta.Binary,Meta.LargeBinary,Meta.BinaryView,Meta.FixedSizeBinary}

function _validateparquetvariant(field::Meta.Field, metadata::String)
    isempty(metadata) || _canonicalextensionerror(
        PARQUET_VARIANT_SYMBOL,
        "metadata must be the empty string",
    )
    field
    return
end

function _validatefixedshapetensor(field::Meta.Field, metadata::String)
    meta = _parsecanonicalmetadata(FIXED_SHAPE_TENSOR_SYMBOL, metadata; required=true)
    _jsonhaskey(meta, "shape") ||
        _canonicalextensionerror(FIXED_SHAPE_TENSOR_SYMBOL, "\"shape\" is required")
    shape = _parseintvector(FIXED_SHAPE_TENSOR_SYMBOL, _jsonget(meta, "shape"), "shape")
    dim_names, permutation, _ =
        _extractdimensionalmetadata(FIXED_SHAPE_TENSOR_SYMBOL, meta; ndim=length(shape))
    field.type isa Meta.FixedSizeList || _canonicalextensionerror(
        FIXED_SHAPE_TENSOR_SYMBOL,
        "storage must be a FixedSizeList",
    )
    length(collect(_fieldchildren(field))) == 1 || _canonicalextensionerror(
        FIXED_SHAPE_TENSOR_SYMBOL,
        "storage must contain exactly one child field",
    )
    expected = isempty(shape) ? 1 : prod(shape)
    Int(field.type.listSize) == expected || _canonicalextensionerror(
        FIXED_SHAPE_TENSOR_SYMBOL,
        "\"shape\" product $expected does not match FixedSizeList size $(field.type.listSize)",
    )
    dim_names
    permutation
    return
end

function _validatevariableshapetensor(field::Meta.Field, metadata::String)
    field.type isa Meta.Struct ||
        _canonicalextensionerror(VARIABLE_SHAPE_TENSOR_SYMBOL, "storage must be a Struct")
    children = Dict(String(child.name) => child for child in collect(_fieldchildren(field)))
    keys(children) == Set(("data", "shape")) || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "storage must contain exactly \"data\" and \"shape\" fields",
    )
    data_field = children["data"]
    shape_field = children["shape"]
    _isliststoragetype(data_field.type) || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"data\" field must use list storage",
    )
    length(collect(_fieldchildren(data_field))) == 1 || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"data\" field must contain exactly one child field",
    )
    shape_field.type isa Meta.FixedSizeList || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"shape\" field must use FixedSizeList storage",
    )
    shape_children = collect(_fieldchildren(shape_field))
    length(shape_children) == 1 || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"shape\" field must contain exactly one child field",
    )
    shape_value = only(shape_children)
    shape_value.type isa Meta.Int || _canonicalextensionerror(
        VARIABLE_SHAPE_TENSOR_SYMBOL,
        "\"shape\" values must use Int32 storage",
    )
    (shape_value.type.bitWidth == 32 && shape_value.type.is_signed) ||
        _canonicalextensionerror(
            VARIABLE_SHAPE_TENSOR_SYMBOL,
            "\"shape\" values must use signed Int32 storage",
        )
    ndim = Int(shape_field.type.listSize)
    meta = _parsecanonicalmetadata(VARIABLE_SHAPE_TENSOR_SYMBOL, metadata)
    _extractdimensionalmetadata(VARIABLE_SHAPE_TENSOR_SYMBOL, meta; ndim=ndim)
    return
end

"""
Given a FlatBuffers.Builder and a Julia column or column eltype,
Write the field.type flatbuffer definition of the eltype
"""
function arrowtype end

arrowtype(b, col::AbstractVector{T}) where {T} = arrowtype(b, maybemissing(T))
arrowtype(b, col::DictEncoded) = arrowtype(b, col.encoding.data)
arrowtype(b, col::Compressed) = arrowtype(b, col.data)

function juliaeltype(f::Meta.Field, ::Nothing, convert::Bool)
    T = juliaeltype(f, convert)
    return convert ? finaljuliatype(T) : T
end

function juliaeltype(f::Meta.Field, meta::AbstractDict{String,String}, convert::Bool)
    TT = juliaeltype(f, convert)
    spec = _extensionspec(meta)
    if spec !== nothing
        _validatebuiltinextension(spec, f)
        !convert && return TT
        T = finaljuliatype(TT)
        storageT =
            spec.name === TIMESTAMP_WITH_OFFSET_SYMBOL ?
            maybemissing(juliaeltype(f, false)) : maybemissing(TT)
        JT = _resolveextensionjuliatype(spec, storageT)
        if JT !== nothing
            return f.nullable ? Union{JT,Missing} : JT
        else
            typename = _extensiontypename(spec)
            @warn "unsupported $(EXTENSION_NAME_KEY) type: \"$typename\", arrow type = $TT" maxlog =
                1 _id = hash((:juliaeltype, typename, TT))
        end
    end
    !convert && return TT
    T = finaljuliatype(TT)
    return something(TT, T)
end

function juliaeltype(f::Meta.Field, convert::Bool)
    T = juliaeltype(f, f.type, convert)
    return f.nullable ? Union{T,Missing} : T
end

juliaeltype(f::Meta.Field, ::Meta.Null, convert) = Missing

function arrowtype(b, ::Type{Missing})
    Meta.nullStart(b)
    return Meta.Null, Meta.nullEnd(b), nothing
end

function juliaeltype(f::Meta.Field, int::Meta.Int, convert)
    if int.is_signed
        if int.bitWidth == 8
            Int8
        elseif int.bitWidth == 16
            Int16
        elseif int.bitWidth == 32
            Int32
        elseif int.bitWidth == 64
            Int64
        elseif int.bitWidth == 128
            Int128
        else
            error("$int is not valid arrow type metadata")
        end
    else
        if int.bitWidth == 8
            UInt8
        elseif int.bitWidth == 16
            UInt16
        elseif int.bitWidth == 32
            UInt32
        elseif int.bitWidth == 64
            UInt64
        elseif int.bitWidth == 128
            UInt128
        else
            error("$int is not valid arrow type metadata")
        end
    end
end

function arrowtype(b, ::Type{T}) where {T<:Integer}
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(8 * sizeof(T)))
    Meta.intAddIsSigned(b, T <: Signed)
    return Meta.Int, Meta.intEnd(b), nothing
end

struct Bool8
    value::Bool
end

Bool8(x::Integer) = Bool8(!iszero(x))

Base.Bool(x::Bool8) = getfield(x, :value)
Base.convert(::Type{Bool}, x::Bool8) = Bool(x)
Base.convert(::Type{Int8}, x::Bool8) = Int8(Bool(x))
Base.zero(::Type{Bool8}) = Bool8(false)
Base.:(==)(x::Bool8, y::Bool8) = Bool(x) == Bool(y)
Base.isequal(x::Bool8, y::Bool8) = isequal(Bool(x), Bool(y))

ArrowTypes.ArrowType(::Type{Bool8}) = _builtinarrowtype(Bool8)
ArrowTypes.toarrow(x::Bool8) = _builtintoarrow(x)
ArrowTypes.arrowname(::Type{Bool8}) = _builtinarrowname(Bool8)
ArrowTypes.JuliaType(::Val{BOOL8_SYMBOL}, ::Type{Int8}, metadata::String) =
    _builtinextensionjuliatype(Val(BOOL8_SYMBOL), Int8, metadata)
ArrowTypes.fromarrow(::Type{Bool8}, x::Int8) = _builtinfromarrow(Bool8, x)
ArrowTypes.default(::Type{Bool8}) = _builtindefault(Bool8)

function writearray(
    io::IO,
    ::Type{Int8},
    col::ArrowTypes.ToArrow{Int8,A},
) where {A<:AbstractVector{Bool8}}
    data = ArrowTypes._sourcedata(col)
    strides(data) == (1,) || return _writearrayfallback(io, Int8, col)
    return Base.write(io, reinterpret(Int8, data))
end

struct JSONText{S<:AbstractString}
    value::S
end

Base.String(x::JSONText) = String(getfield(x, :value))
Base.convert(::Type{String}, x::JSONText) = String(x)
Base.:(==)(x::JSONText, y::JSONText) = getfield(x, :value) == getfield(y, :value)
Base.isequal(x::JSONText, y::JSONText) = isequal(getfield(x, :value), getfield(y, :value))

ArrowTypes.ArrowType(::Type{JSONText{S}}) where {S<:AbstractString} =
    _builtinarrowtype(JSONText{S})
ArrowTypes.toarrow(x::JSONText) = _builtintoarrow(x)
ArrowTypes.arrowname(::Type{JSONText{S}}) where {S<:AbstractString} =
    _builtinarrowname(JSONText{S})
ArrowTypes.JuliaType(
    ::Val{JSON_SYMBOL},
    ::Type{S},
    metadata::String,
) where {S<:AbstractString} = _builtinextensionjuliatype(Val(JSON_SYMBOL), S, metadata)
ArrowTypes.fromarrow(::Type{JSONText{String}}, ptr::Ptr{UInt8}, len::Int) =
    _builtinfromarrow(JSONText{String}, ptr, len)
ArrowTypes.fromarrow(::Type{JSONText{S}}, x::S) where {S<:AbstractString} =
    _builtinfromarrow(JSONText{S}, x)
ArrowTypes.default(::Type{JSONText{S}}) where {S<:AbstractString} =
    _builtindefault(JSONText{S})

ArrowTypes.JuliaType(::Val{OPAQUE_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(OPAQUE_SYMBOL), S, metadata)
ArrowTypes.JuliaType(::Val{PARQUET_VARIANT_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(PARQUET_VARIANT_SYMBOL), S, metadata)
ArrowTypes.JuliaType(::Val{FIXED_SHAPE_TENSOR_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(FIXED_SHAPE_TENSOR_SYMBOL), S, metadata)
ArrowTypes.JuliaType(::Val{VARIABLE_SHAPE_TENSOR_SYMBOL}, S, metadata::String) =
    _builtinextensionjuliatype(Val(VARIABLE_SHAPE_TENSOR_SYMBOL), S, metadata)

@inline function _jsonstringliteral(x::AbstractString)
    return '"' * escape_string(x) * '"'
end

opaquemetadata(type_name::AbstractString, vendor_name::AbstractString) =
    _builtinopaquemetadata(type_name, vendor_name)

variantmetadata() = _builtinvariantmetadata()

function fixedshapetensormetadata(
    shape::AbstractVector{<:Integer};
    dim_names::Union{Nothing,AbstractVector{<:AbstractString}}=nothing,
    permutation::Union{Nothing,AbstractVector{<:Integer}}=nothing,
)
    return _builtinfixedshapetensormetadata(
        shape;
        dim_names=dim_names,
        permutation=permutation,
    )
end

function variableshapetensormetadata(;
    uniform_shape::Union{Nothing,AbstractVector}=nothing,
    dim_names::Union{Nothing,AbstractVector{<:AbstractString}}=nothing,
    permutation::Union{Nothing,AbstractVector{<:Integer}}=nothing,
)
    return _builtinvariableshapetensormetadata(;
        uniform_shape=uniform_shape,
        dim_names=dim_names,
        permutation=permutation,
    )
end

# primitive types
function juliaeltype(f::Meta.Field, fp::Meta.FloatingPoint, convert)
    if fp.precision == Meta.Precision.HALF
        Float16
    elseif fp.precision == Meta.Precision.SINGLE
        Float32
    elseif fp.precision == Meta.Precision.DOUBLE
        Float64
    end
end

function arrowtype(b, ::Type{T}) where {T<:AbstractFloat}
    Meta.floatingPointStart(b)
    Meta.floatingPointAddPrecision(
        b,
        T === Float16 ? Meta.Precision.HALF :
        T === Float32 ? Meta.Precision.SINGLE : Meta.Precision.DOUBLE,
    )
    return Meta.FloatingPoint, Meta.floatingPointEnd(b), nothing
end

juliaeltype(f::Meta.Field, b::Union{Meta.Utf8,Meta.LargeUtf8,Meta.Utf8View}, convert) =
    String

datasizeof(x) = sizeof(x)
datasizeof(x::AbstractVector) = sum(datasizeof, x)

juliaeltype(
    f::Meta.Field,
    b::Union{Meta.Binary,Meta.LargeBinary,Meta.BinaryView},
    convert,
) = Base.CodeUnits

juliaeltype(f::Meta.Field, x::Meta.FixedSizeBinary, convert) =
    NTuple{Int(x.byteWidth),UInt8}

# arggh!
Base.write(io::IO, x::NTuple{N,T}) where {N,T} = sum(y -> Base.write(io, y), x)

juliaeltype(f::Meta.Field, x::Meta.Bool, convert) = Bool

function arrowtype(b, ::Type{Bool})
    Meta.boolStart(b)
    return Meta.Bool, Meta.boolEnd(b), nothing
end

struct Decimal{P,S,T}
    value::T # only Int128 or Int256
end

Base.zero(::Type{Decimal{P,S,T}}) where {P,S,T} = Decimal{P,S,T}(T(0))
==(a::Decimal{P,S,T}, b::Decimal{P,S,T}) where {P,S,T} = ==(a.value, b.value)
Base.isequal(a::Decimal{P,S,T}, b::Decimal{P,S,T}) where {P,S,T} = isequal(a.value, b.value)

function juliaeltype(f::Meta.Field, x::Meta.Decimal, convert)
    return Decimal{x.precision,x.scale,x.bitWidth == 256 ? Int256 : Int128}
end

ArrowTypes.ArrowKind(::Type{<:Decimal}) = PrimitiveKind()

function arrowtype(b, ::Type{Decimal{P,S,T}}) where {P,S,T}
    Meta.decimalStart(b)
    Meta.decimalAddPrecision(b, Int32(P))
    Meta.decimalAddScale(b, Int32(S))
    Meta.decimalAddBitWidth(b, Int32(T == Int256 ? 256 : 128))
    return Meta.Decimal, Meta.decimalEnd(b), nothing
end

Base.write(io::IO, x::Decimal) = Base.write(io, x.value)

abstract type ArrowTimeType end
Base.write(io::IO, x::ArrowTimeType) = Base.write(io, x.x)
ArrowTypes.ArrowKind(::Type{<:ArrowTimeType}) = PrimitiveKind()

struct Date{U,T} <: ArrowTimeType
    x::T
end

const DATE = Date{Meta.DateUnit.DAY,Int32}
Base.zero(::Type{Date{U,T}}) where {U,T} = Date{U,T}(T(0))
storagetype(::Type{Date{U,T}}) where {U,T} = T
bitwidth(x::Meta.DateUnit.T) = x == Meta.DateUnit.DAY ? Int32 : Int64
Date{Meta.DateUnit.DAY}(days) = DATE(Int32(days))
Date{Meta.DateUnit.MILLISECOND}(ms) = Date{Meta.DateUnit.MILLISECOND,Int64}(Int64(ms))

juliaeltype(f::Meta.Field, x::Meta.Date, convert) = Date{x.unit,bitwidth(x.unit)}
finaljuliatype(::Type{DATE}) = Dates.Date
Base.convert(::Type{Dates.Date}, x::DATE) =
    Dates.Date(Dates.UTD(Int64(x.x + UNIX_EPOCH_DATE)))
finaljuliatype(::Type{Date{Meta.DateUnit.MILLISECOND,Int64}}) = Dates.DateTime
Base.convert(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND,Int64}) =
    Dates.DateTime(Dates.UTM(Int64(x.x + UNIX_EPOCH_DATETIME)))

function arrowtype(b, ::Type{Date{U,T}}) where {U,T}
    Meta.dateStart(b)
    Meta.dateAddUnit(b, U)
    return Meta.Date, Meta.dateEnd(b), nothing
end

const UNIX_EPOCH_DATE = Dates.value(Dates.Date(1970))
Base.convert(::Type{DATE}, x::Dates.Date) = DATE(Int32(Dates.value(x) - UNIX_EPOCH_DATE))

const UNIX_EPOCH_DATETIME = Dates.value(Dates.DateTime(1970))
Base.convert(::Type{Date{Meta.DateUnit.MILLISECOND,Int64}}, x::Dates.DateTime) =
    Date{Meta.DateUnit.MILLISECOND,Int64}(Int64(Dates.value(x) - UNIX_EPOCH_DATETIME))

ArrowTypes.ArrowType(::Type{Dates.Date}) = DATE
ArrowTypes.toarrow(x::Dates.Date) = convert(DATE, x)
const DATE_SYMBOL = Symbol("JuliaLang.Date")
ArrowTypes.arrowname(::Type{Dates.Date}) = DATE_SYMBOL
ArrowTypes.JuliaType(::Val{DATE_SYMBOL}, S) = Dates.Date
ArrowTypes.fromarrow(::Type{Dates.Date}, x::DATE) = convert(Dates.Date, x)
ArrowTypes.default(::Type{Dates.Date}) = Dates.Date(1, 1, 1)

struct Time{U,T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Time{U,T}}) where {U,T} = Time{U,T}(T(0))
const TIME = Time{Meta.TimeUnit.NANOSECOND,Int64}

bitwidth(x::Meta.TimeUnit.T) =
    x == Meta.TimeUnit.SECOND || x == Meta.TimeUnit.MILLISECOND ? Int32 : Int64
Time{U}(x) where {U<:Meta.TimeUnit.T} = Time{U,bitwidth(U)}(bitwidth(U)(x))
storagetype(::Type{Time{U,T}}) where {U,T} = T
juliaeltype(f::Meta.Field, x::Meta.Time, convert) = Time{x.unit,bitwidth(x.unit)}
finaljuliatype(::Type{<:Time}) = Dates.Time
periodtype(U::Meta.TimeUnit.T) =
    U === Meta.TimeUnit.SECOND ? Dates.Second :
    U === Meta.TimeUnit.MILLISECOND ? Dates.Millisecond :
    U === Meta.TimeUnit.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
Base.convert(::Type{Dates.Time}, x::Time{U,T}) where {U,T} =
    Dates.Time(Dates.Nanosecond(Dates.tons(periodtype(U)(x.x))))

function arrowtype(b, ::Type{Time{U,T}}) where {U,T}
    Meta.timeStart(b)
    Meta.timeAddUnit(b, U)
    Meta.timeAddBitWidth(b, Int32(8 * sizeof(T)))
    return Meta.Time, Meta.timeEnd(b), nothing
end

Base.convert(::Type{TIME}, x::Dates.Time) = TIME(Dates.value(x))

ArrowTypes.ArrowType(::Type{Dates.Time}) = TIME
ArrowTypes.toarrow(x::Dates.Time) = convert(TIME, x)
const TIME_SYMBOL = Symbol("JuliaLang.Time")
ArrowTypes.arrowname(::Type{Dates.Time}) = TIME_SYMBOL
ArrowTypes.JuliaType(::Val{TIME_SYMBOL}, S) = Dates.Time
ArrowTypes.fromarrow(::Type{Dates.Time}, x::Arrow.Time) = convert(Dates.Time, x)
ArrowTypes.default(::Type{Dates.Time}) = Dates.Time(1, 1, 1)

struct Timestamp{U,TZ} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Timestamp{U,T}}) where {U,T} = Timestamp{U,T}(Int64(0))

struct TimestampWithOffset{U}
    timestamp::Timestamp{U,:UTC}
    offset_minutes::Int16
end

TimestampWithOffset(timestamp::Timestamp{U,:UTC}, offset_minutes::Integer) where {U} =
    TimestampWithOffset{U}(timestamp, Int16(offset_minutes))

Base.zero(::Type{TimestampWithOffset{U}}) where {U} =
    TimestampWithOffset{U}(zero(Timestamp{U,:UTC}), Int16(0))

function juliaeltype(f::Meta.Field, x::Meta.Timestamp, convert)
    return Timestamp{x.unit,x.timezone === nothing ? nothing : Symbol(x.timezone)}
end

const DATETIME = Timestamp{Meta.TimeUnit.MILLISECOND,nothing}

finaljuliatype(::Type{Timestamp{U,TZ}}) where {U,TZ} = ZonedDateTime
finaljuliatype(::Type{Timestamp{U,nothing}}) where {U} = DateTime

@noinline warntimestamp(U, T) =
    @warn "automatically converting Arrow.Timestamp with precision = $U to `$T` which only supports millisecond precision; conversion may be lossy; to avoid converting, pass `Arrow.Table(source; convert=false)" maxlog =
        1 _id = hash((:warntimestamp, U, T))

function Base.convert(::Type{ZonedDateTime}, x::Timestamp{U,TZ}) where {U,TZ}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, ZonedDateTime)
    return ZonedDateTime(
        Dates.DateTime(
            Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
        ),
        TimeZone(String(TZ));
        from_utc=true,
    )
end

function Base.convert(::Type{DateTime}, x::Timestamp{U,nothing}) where {U}
    (U === Meta.TimeUnit.MICROSECOND || U == Meta.TimeUnit.NANOSECOND) &&
        warntimestamp(U, DateTime)
    return Dates.DateTime(
        Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)),
    )
end

Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND,TZ}}, x::ZonedDateTime) where {TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND,TZ}(
        Int64(Dates.value(DateTime(x, UTC)) - UNIX_EPOCH_DATETIME),
    )
Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND,nothing}}, x::DateTime) =
    Timestamp{Meta.TimeUnit.MILLISECOND,nothing}(
        Int64(Dates.value(x) - UNIX_EPOCH_DATETIME),
    )

function arrowtype(b, ::Type{Timestamp{U,TZ}}) where {U,TZ}
    tz = TZ !== nothing ? FlatBuffers.createstring!(b, String(TZ)) : FlatBuffers.UOffsetT(0)
    Meta.timestampStart(b)
    Meta.timestampAddUnit(b, U)
    Meta.timestampAddTimezone(b, tz)
    return Meta.Timestamp, Meta.timestampEnd(b), nothing
end

ArrowTypes.ArrowType(::Type{Dates.DateTime}) = DATETIME
ArrowTypes.toarrow(x::Dates.DateTime) = convert(DATETIME, x)
const DATETIME_SYMBOL = Symbol("JuliaLang.DateTime")
ArrowTypes.arrowname(::Type{Dates.DateTime}) = DATETIME_SYMBOL
ArrowTypes.JuliaType(::Val{DATETIME_SYMBOL}, S) = Dates.DateTime
ArrowTypes.fromarrow(::Type{Dates.DateTime}, x::Timestamp) = convert(Dates.DateTime, x)
ArrowTypes.fromarrow(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND,Int64}) =
    convert(Dates.DateTime, x)
ArrowTypes.default(::Type{Dates.DateTime}) = Dates.DateTime(1, 1, 1, 1, 1, 1)

ArrowTypes.ArrowType(::Type{ZonedDateTime}) = _builtinarrowtype(ZonedDateTime)
ArrowTypes.toarrow(x::ZonedDateTime) = _builtintoarrow(x)
const ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime-UTC")
ArrowTypes.arrowname(::Type{ZonedDateTime}) = _builtinarrowname(ZonedDateTime)
ArrowTypes.JuliaType(::Val{ZONEDDATETIME_SYMBOL}, S) =
    _builtinextensionjuliatype(Val(ZONEDDATETIME_SYMBOL), S)
ArrowTypes.fromarrow(::Type{ZonedDateTime}, x::Timestamp) =
    _builtinfromarrow(ZonedDateTime, x)
ArrowTypes.default(::Type{TimeZones.ZonedDateTime}) = _builtindefault(ZonedDateTime)

const TIMESTAMP_WITH_OFFSET_SYMBOL = Symbol("arrow.timestamp_with_offset")
ArrowTypes.ArrowType(::Type{TimestampWithOffset{U}}) where {U} =
    _builtinarrowtype(TimestampWithOffset{U})
ArrowTypes.toarrow(x::TimestampWithOffset{U}) where {U} = _builtintoarrow(x)
ArrowTypes.arrowname(::Type{TimestampWithOffset{U}}) where {U} =
    _builtinarrowname(TimestampWithOffset{U})
ArrowTypes.JuliaType(
    ::Val{TIMESTAMP_WITH_OFFSET_SYMBOL},
    ::Type{NamedTuple{(:timestamp, :offset_minutes),Tuple{Timestamp{U,:UTC},Int16}}},
    metadata::String,
) where {U} = _builtinextensionjuliatype(
    Val(TIMESTAMP_WITH_OFFSET_SYMBOL),
    NamedTuple{(:timestamp, :offset_minutes),Tuple{Timestamp{U,:UTC},Int16}},
    metadata,
)
ArrowTypes.default(::Type{TimestampWithOffset{U}}) where {U} =
    _builtindefault(TimestampWithOffset{U})
ArrowTypes.fromarrowstruct(
    ::Type{TimestampWithOffset{U}},
    ::Val{(:timestamp, :offset_minutes)},
    timestamp::Timestamp{U,:UTC},
    offset_minutes::Int16,
) where {U} = _builtinfromarrowstruct(
    TimestampWithOffset{U},
    Val((:timestamp, :offset_minutes)),
    timestamp,
    offset_minutes,
)
ArrowTypes.fromarrowstruct(
    ::Type{TimestampWithOffset{U}},
    ::Val{(:offset_minutes, :timestamp)},
    offset_minutes::Int16,
    timestamp::Timestamp{U,:UTC},
) where {U} = _builtinfromarrowstruct(
    TimestampWithOffset{U},
    Val((:offset_minutes, :timestamp)),
    offset_minutes,
    timestamp,
)

# Backwards compatibility: older versions of Arrow saved ZonedDateTime's with this metdata:
const OLD_ZONEDDATETIME_SYMBOL = Symbol("JuliaLang.ZonedDateTime")
# and stored the local time instead of the UTC time.
struct LocalZonedDateTime end
ArrowTypes.JuliaType(::Val{OLD_ZONEDDATETIME_SYMBOL}, S) =
    _builtinextensionjuliatype(Val(OLD_ZONEDDATETIME_SYMBOL), S)
ArrowTypes.fromarrow(::Type{LocalZonedDateTime}, x::Timestamp{U,TZ}) where {U,TZ} =
    _builtinfromarrow(LocalZonedDateTime, x)

"""
    Arrow.ToTimestamp(x::AbstractVector{ZonedDateTime})

Wrapper array that provides a more efficient encoding of `ZonedDateTime` elements to the arrow format. In the arrow format,
timestamp columns with timezone information are encoded as the arrow equivalent of a Julia type parameter, meaning an entire column
_should_ have elements all with the same timezone. If a `ZonedDateTime` column is passed to `Arrow.write`, for correctness, it must
scan each element to check each timezone. `Arrow.ToTimestamp` provides a "bypass" of this process by encoding the timezone of the
first element of the `AbstractVector{ZonedDateTime}`, which in turn allows `Arrow.write` to avoid costly checking/conversion and
can encode the `ZonedDateTime` as `Arrow.Timestamp` directly.
"""
struct ToTimestamp{A,TZ} <: AbstractVector{Timestamp{Meta.TimeUnit.MILLISECOND,TZ}}
    data::A # AbstractVector{ZonedDateTime}
end

ToTimestamp(x::A) where {A<:AbstractVector{ZonedDateTime}} =
    ToTimestamp{A,Symbol(x[1].timezone)}(x)
Base.IndexStyle(::Type{<:ToTimestamp}) = Base.IndexLinear()
Base.size(x::ToTimestamp) = (length(x.data),)
Base.eltype(::Type{ToTimestamp{A,TZ}}) where {A,TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND,TZ}
Base.getindex(x::ToTimestamp{A,TZ}, i::Integer) where {A,TZ} =
    convert(Timestamp{Meta.TimeUnit.MILLISECOND,TZ}, getindex(x.data, i))

struct Interval{U,T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Interval{U,T}}) where {U,T} = Interval{U,T}(T(0))

bitwidth(x::Meta.IntervalUnit.T) = x == Meta.IntervalUnit.YEAR_MONTH ? Int32 : Int64
Interval{Meta.IntervalUnit.YEAR_MONTH}(x) =
    Interval{Meta.IntervalUnit.YEAR_MONTH,Int32}(Int32(x))
Interval{Meta.IntervalUnit.DAY_TIME}(x) =
    Interval{Meta.IntervalUnit.DAY_TIME,Int64}(Int64(x))

function juliaeltype(f::Meta.Field, x::Meta.Interval, convert)
    return Interval{x.unit,bitwidth(x.unit)}
end

function juliaeltype(f::Meta.Field, x::Meta.RunEndEncoded, convert)
    return juliaeltype(f.children[2], buildmetadata(f.children[2]), convert)
end

function arrowtype(b, ::Type{Interval{U,T}}) where {U,T}
    Meta.intervalStart(b)
    Meta.intervalAddUnit(b, U)
    return Meta.Interval, Meta.intervalEnd(b), nothing
end

struct Duration{U} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Duration{U}}) where {U} = Duration{U}(Int64(0))

function juliaeltype(f::Meta.Field, x::Meta.Duration, convert)
    return Duration{x.unit}
end

finaljuliatype(::Type{Duration{U}}) where {U} = periodtype(U)
Base.convert(::Type{P}, x::Duration{U}) where {P<:Dates.Period,U} = P(periodtype(U)(x.x))

function arrowtype(b, ::Type{Duration{U}}) where {U}
    Meta.durationStart(b)
    Meta.durationAddUnit(b, U)
    return Meta.Duration, Meta.durationEnd(b), nothing
end

arrowtype(b, ::Type{P}) where {P<:Dates.Period} = arrowtype(b, Duration{arrowperiodtype(P)})

arrowperiodtype(P) = Meta.TimeUnit.SECOND
arrowperiodtype(::Type{Dates.Millisecond}) = Meta.TimeUnit.MILLISECOND
arrowperiodtype(::Type{Dates.Microsecond}) = Meta.TimeUnit.MICROSECOND
arrowperiodtype(::Type{Dates.Nanosecond}) = Meta.TimeUnit.NANOSECOND

Base.convert(::Type{Duration{U}}, x::Dates.Period) where {U} =
    Duration{U}(Dates.value(periodtype(U)(x)))

ArrowTypes.ArrowType(::Type{P}) where {P<:Dates.Period} = Duration{arrowperiodtype(P)}
ArrowTypes.toarrow(x::P) where {P<:Dates.Period} = convert(Duration{arrowperiodtype(P)}, x)
const PERIOD_SYMBOL = Symbol("JuliaLang.Dates.Period")
ArrowTypes.arrowname(::Type{P}) where {P<:Dates.Period} = PERIOD_SYMBOL
ArrowTypes.JuliaType(::Val{PERIOD_SYMBOL}, ::Type{Duration{U}}) where {U} = periodtype(U)
ArrowTypes.fromarrow(::Type{P}, x::Duration{U}) where {P<:Dates.Period,U} = convert(P, x)

# nested types; call juliaeltype recursively on nested children
function juliaeltype(
    f::Meta.Field,
    list::Union{Meta.List,Meta.LargeList,Meta.ListView,Meta.LargeListView},
    convert,
)
    return Vector{juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)}
end

# arrowtype will call fieldoffset recursively for children
function arrowtype(b, x::List{T,O,A}) where {T,O,A}
    if liststringtype(x)
        if T <: AbstractString || T <: Union{AbstractString,Missing}
            if O == Int32
                Meta.utf8Start(b)
                return Meta.Utf8, Meta.utf8End(b), nothing
            else # if O == Int64
                Meta.largUtf8Start(b)
                return Meta.LargeUtf8, Meta.largUtf8End(b), nothing
            end
        else # if Base.CodeUnits
            if O == Int32
                Meta.binaryStart(b)
                return Meta.Binary, Meta.binaryEnd(b), nothing
            else # if O == Int64
                Meta.largeBinaryStart(b)
                return Meta.LargeBinary, Meta.largeBinaryEnd(b), nothing
            end
        end
    else
        children = [fieldoffset(b, "", x.data)]
        if O == Int32
            Meta.listStart(b)
            return Meta.List, Meta.listEnd(b), children
        else
            Meta.largeListStart(b)
            return Meta.LargeList, Meta.largeListEnd(b), children
        end
    end
end

function juliaeltype(f::Meta.Field, list::Meta.FixedSizeList, convert)
    type = juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)
    return NTuple{Int(list.listSize),type}
end

function arrowtype(b, x::FixedSizeList{T,A}) where {T,A}
    N = ArrowTypes.getsize(
        ArrowTypes.ArrowKind(ArrowTypes.ArrowType(Base.nonmissingtype(T))),
    )
    if eltype(A) == UInt8
        Meta.fixedSizeBinaryStart(b)
        Meta.fixedSizeBinaryAddByteWidth(b, Int32(N))
        return Meta.FixedSizeBinary, Meta.fixedSizeBinaryEnd(b), nothing
    else
        children = [fieldoffset(b, "", x.data)]
        Meta.fixedSizeListStart(b)
        Meta.fixedSizeListAddListSize(b, Int32(N))
        return Meta.FixedSizeList, Meta.fixedSizeListEnd(b), children
    end
end

function juliaeltype(f::Meta.Field, map::Meta.Map, convert)
    K = juliaeltype(
        f.children[1].children[1],
        buildmetadata(f.children[1].children[1]),
        convert,
    )
    V = juliaeltype(
        f.children[1].children[2],
        buildmetadata(f.children[1].children[2]),
        convert,
    )
    return Dict{K,V}
end

function arrowtype(b, x::Map)
    children = [fieldoffset(b, "entries", x.data)]
    Meta.mapStart(b)
    return Meta.Map, Meta.mapEnd(b), children
end

struct KeyValue{K,V}
    key::K
    value::V
end
keyvalueK(::Type{KeyValue{K,V}}) where {K,V} = K
keyvalueV(::Type{KeyValue{K,V}}) where {K,V} = V
Base.length(kv::KeyValue) = 1
Base.iterate(kv::KeyValue, st=1) = st === nothing ? nothing : (kv, nothing)
ArrowTypes.default(::Type{KeyValue{K,V}}) where {K,V} = KeyValue(default(K), default(V))

function arrowtype(b, ::Type{KeyValue{K,V}}) where {K,V}
    children = [fieldoffset(b, "key", K), fieldoffset(b, "value", V)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

function juliaeltype(f::Meta.Field, list::Meta.Struct, convert)
    names = Tuple(Symbol(x.name) for x in f.children)
    types = Tuple(juliaeltype(x, buildmetadata(x), convert) for x in f.children)
    return NamedTuple{names,Tuple{types...}}
end

function arrowtype(b, x::Struct{T,S}) where {T,S}
    names = fieldnames(Base.nonmissingtype(T))
    children = [fieldoffset(b, names[i], x.data[i]) for i = 1:length(names)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

# Unions
function UnionT(f::Meta.Field, convert)
    typeids = f.type.typeIds === nothing ? nothing : Tuple(Int(x) for x in f.type.typeIds)
    UT = UnionT{
        f.type.mode,
        typeids,
        Tuple{(juliaeltype(x, buildmetadata(x), convert) for x in f.children)...},
    }
    return UT
end

juliaeltype(f::Meta.Field, u::Meta.Union, convert) =
    Union{(juliaeltype(x, buildmetadata(x), convert) for x in f.children)...}

function arrowtype(
    b,
    x::Union{DenseUnion{S,UnionT{T,typeIds,U}},SparseUnion{S,UnionT{T,typeIds,U}}},
) where {S,T,typeIds,U}
    if typeIds !== nothing
        Meta.unionStartTypeIdsVector(b, length(typeIds))
        for id in Iterators.reverse(typeIds)
            FlatBuffers.prepend!(b, id)
        end
        TI = FlatBuffers.endvector!(b, length(typeIds))
    end
    children = [fieldoffset(b, "", x.data[i]) for i = 1:fieldcount(U)]
    Meta.unionStart(b)
    Meta.unionAddMode(b, T)
    if typeIds !== nothing
        Meta.unionAddTypeIds(b, TI)
    end
    return Meta.Union, Meta.unionEnd(b), children
end
