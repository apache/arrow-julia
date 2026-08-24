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

module SeededFuzz

using Arrow
using ArrowStrings
using ArrowTypes
using Dates
using Tables

const DEFAULT_SEED = UInt64(0x9f5a_37c2_41de_880b)
const PR_CASES = 16
const PR_MUTATIONS = 64
const EXTENDED_CASES = 512
const EXTENDED_MUTATIONS = 20_000

function _parseseed(value::AbstractString)
    cleaned = replace(strip(value), "_" => "")
    isempty(cleaned) && throw(ArgumentError("empty seed"))
    if startswith(lowercase(cleaned), "0x")
        length(cleaned) > 2 || throw(ArgumentError("empty hexadecimal seed"))
        return parse(UInt64, cleaned[3:end]; base=16)
    end
    return parse(UInt64, cleaned)
end

function _parsecount(flag::AbstractString, value::AbstractString)
    count = parse(Int, value)
    count >= 0 || throw(ArgumentError("$flag must be nonnegative"))
    return count
end

function _takevalue(args, index::Int, flag::AbstractString)
    index < length(args) || throw(ArgumentError("$flag requires a value"))
    return args[index + 1], index + 1
end

"Parse one fuzz command and isolate every exact-replay mode from other lanes."
function parse_options(args)
    seed = DEFAULT_SEED
    cases = EXTENDED_CASES
    mutations = EXTENDED_MUTATIONS
    case_index = nothing
    mutation_index = nothing
    mutation_file = nothing
    mutation_lane = nothing
    layouts_only = false
    include_layouts = true
    verify_determinism = false
    determinism_every = 0
    reproduction_dir = nothing
    index = 1
    while index <= length(args)
        arg = args[index]
        if arg == "--help" || arg == "-h"
            return (;
                help=true,
                seed,
                cases,
                mutations,
                case_index,
                mutation_index,
                mutation_file,
                mutation_lane,
                layouts_only,
                include_layouts,
                verify_determinism,
                determinism_every,
                reproduction_dir,
            )
        elseif arg == "--seed"
            value, index = _takevalue(args, index, arg)
            seed = _parseseed(value)
        elseif startswith(arg, "--seed=")
            seed = _parseseed(arg[(length("--seed=") + 1):end])
        elseif arg == "--cases"
            value, index = _takevalue(args, index, arg)
            cases = _parsecount(arg, value)
        elseif startswith(arg, "--cases=")
            cases = _parsecount("--cases", arg[(length("--cases=") + 1):end])
        elseif arg == "--mutations"
            value, index = _takevalue(args, index, arg)
            mutations = _parsecount(arg, value)
        elseif startswith(arg, "--mutations=")
            mutations = _parsecount("--mutations", arg[(length("--mutations=") + 1):end])
        elseif arg == "--case-index"
            value, index = _takevalue(args, index, arg)
            case_index = _parsecount(arg, value)
            case_index >= 1 || throw(ArgumentError("--case-index must be positive"))
        elseif startswith(arg, "--case-index=")
            case_index = _parsecount(arg, arg[(length("--case-index=") + 1):end])
            case_index >= 1 || throw(ArgumentError("--case-index must be positive"))
        elseif arg == "--mutation-index"
            value, index = _takevalue(args, index, arg)
            mutation_index = _parsecount(arg, value)
            mutation_index >= 1 || throw(ArgumentError("--mutation-index must be positive"))
        elseif startswith(arg, "--mutation-index=")
            mutation_index = _parsecount(arg, arg[(length("--mutation-index=") + 1):end])
            mutation_index >= 1 || throw(ArgumentError("--mutation-index must be positive"))
        elseif arg == "--mutation-file"
            value, index = _takevalue(args, index, arg)
            isempty(value) && throw(ArgumentError("--mutation-file must not be empty"))
            mutation_file = value
        elseif startswith(arg, "--mutation-file=")
            mutation_file = arg[(length("--mutation-file=") + 1):end]
            isempty(mutation_file) &&
                throw(ArgumentError("--mutation-file must not be empty"))
        elseif arg == "--mutation-lane"
            value, index = _takevalue(args, index, arg)
            mutation_lane = Symbol(value)
        elseif startswith(arg, "--mutation-lane=")
            mutation_lane = Symbol(arg[(length("--mutation-lane=") + 1):end])
        elseif arg == "--layouts-only"
            layouts_only = true
        elseif arg == "--skip-layouts"
            include_layouts = false
        elseif arg == "--determinism-every"
            value, index = _takevalue(args, index, arg)
            determinism_every = _parsecount(arg, value)
        elseif startswith(arg, "--determinism-every=")
            determinism_every = _parsecount(
                "--determinism-every",
                arg[(length("--determinism-every=") + 1):end],
            )
        elseif arg == "--repro-dir"
            value, index = _takevalue(args, index, arg)
            isempty(value) && throw(ArgumentError("--repro-dir must not be empty"))
            reproduction_dir = value
        elseif startswith(arg, "--repro-dir=")
            value = arg[(length("--repro-dir=") + 1):end]
            isempty(value) && throw(ArgumentError("--repro-dir must not be empty"))
            reproduction_dir = value
        else
            throw(ArgumentError("unknown option $arg"))
        end
        index += 1
    end
    mutation_file === nothing &&
        mutation_lane !== nothing &&
        throw(ArgumentError("--mutation-lane requires --mutation-file"))
    mutation_file !== nothing &&
        mutation_lane === nothing &&
        throw(ArgumentError("--mutation-file requires --mutation-lane"))
    mutation_lane === nothing ||
        mutation_lane in
        (:stream_full, :file_full, :file_ranged, :auto_full, :stats_filtered) ||
        throw(ArgumentError("invalid --mutation-lane $mutation_lane"))
    layouts_only &&
        !include_layouts &&
        throw(ArgumentError("--layouts-only and --skip-layouts are incompatible"))
    layouts_only &&
        (
            case_index !== nothing ||
            mutation_index !== nothing ||
            mutation_file !== nothing
        ) &&
        throw(ArgumentError("--layouts-only cannot be combined with a replay option"))
    case_index !== nothing &&
        mutation_index !== nothing &&
        throw(ArgumentError("use one exact generated replay option at a time"))
    mutation_file !== nothing &&
        (case_index !== nothing || mutation_index !== nothing) &&
        throw(ArgumentError("saved-byte replay cannot be combined with generated replay"))

    # An exact generated replay must not fail in an unrelated generated or
    # deterministic-layout lane. The selected index still runs when its count
    # is zero because the index replaces the lane range.
    if case_index !== nothing || mutation_index !== nothing
        cases = 0
        mutations = 0
        include_layouts = false
        verify_determinism = mutation_index !== nothing
    end
    return (;
        help=false,
        seed,
        cases,
        mutations,
        case_index,
        mutation_index,
        mutation_file,
        mutation_lane,
        layouts_only,
        include_layouts,
        verify_determinism,
        determinism_every,
        reproduction_dir,
    )
end

const _SPLITMIX_GAMMA = UInt64(0x9e37_79b9_7f4a_7c15)
const _SPLITMIX_MUL1 = UInt64(0xbf58_476d_1ce4_e5b9)
const _SPLITMIX_MUL2 = UInt64(0x94d0_49bb_1331_11eb)
const _DIFFERENTIAL_LANE = UInt64(0x4449_4646_4552_454e)
const _SCAN_LANE = UInt64(0x5343_414e_5f4c_414e)
const _MUTATION_LANE = UInt64(0x4d55_5441_5449_4f4e)

const TEXT =
    ["", "a", "\0", "alpha", "\u03bb", "\u03b1\u03b2\u2200", "\U0001f9ea", "line\nbreak"]

# Every mutated read is constrained before metadata-directed allocation. These
# limits are intentionally far above the tiny valid seed corpus, but far below
# the package defaults. A mutation cannot turn this test into an OOM probe.
const STRICT_LIMITS = Arrow.Limits(
    max_metadata_bytes=Int64(256 * 1024),
    max_body_bytes=Int64(2 * 1024 * 1024),
    max_buffer_bytes=Int64(512 * 1024),
    max_total_allocated_bytes=Int64(8 * 1024 * 1024),
    max_messages=256,
    max_metadata_objects=100_000,
    max_nesting_depth=32,
    max_array_length=Int64(100_000),
)

"A reproducible fuzz failure with both its master and independently derived seed."
struct FuzzFailure <: Exception
    lane::Symbol
    index::Int
    master_seed::UInt64
    case_seed::UInt64
    message::String
end
function Base.showerror(io::IO, err::FuzzFailure)
    print(
        io,
        "$(err.lane) fuzz case $(err.index) " *
        "(master_seed=0x$(string(err.master_seed; base=16)), " *
        "case_seed=0x$(string(err.case_seed; base=16))) failed: ",
        err.message,
    )
end

struct DifferentialCase{I,P,E}
    seed::UInt64
    inputs::I
    expectedparts::P
    expected::E
end

struct DifferentialSummary
    seed::UInt64
    cases::Int
    variants::Int
    rewrites::Int
    scanchecks::Int
    layoutchecks::Int
    coverage::Set{Symbol}
end

struct MutationSummary
    seed::UInt64
    mutations::Int
    counts::Dict{Symbol,Int}
    routecounts::Dict{Tuple{Symbol,Symbol},Int}
    operationcounts::Dict{Symbol,Int}
    determinismchecks::Int
end

struct FuzzSummary
    differential::DifferentialSummary
    mutations::MutationSummary
end

mutable struct FuzzBytesSource <: Arrow.AbstractArrowSource
    data::Vector{UInt8}
    requests::Vector{NTuple{2,Int64}}
end
FuzzBytesSource(data::Vector{UInt8}) = FuzzBytesSource(data, NTuple{2,Int64}[])
Arrow.sourcelength(source::FuzzBytesSource) = length(source.data)
function Arrow.readrange(source::FuzzBytesSource, offset, len)
    n = length(source.data)
    (offset isa Integer && len isa Integer && offset >= 0 && len >= 0) ||
        throw(BoundsError(source.data, (offset, len)))
    (len <= n && offset <= n - len) || throw(BoundsError(source.data, (offset, len)))
    push!(source.requests, (Int64(offset), Int64(len)))
    return copy(@view source.data[(offset + 1):(offset + len)])
end

_rangesintersect(a::NTuple{2,Int64}, b::NTuple{2,Int64}) =
    a[2] > 0 && b[2] > 0 && a[1] < b[1] + b[2] && b[1] < a[1] + a[2]

"One SplitMix64 output. This is independent of Random's global state."
@inline function _splitmix64(x::UInt64)
    z = x + _SPLITMIX_GAMMA
    z = xor(z, z >> 30) * _SPLITMIX_MUL1
    z = xor(z, z >> 27) * _SPLITMIX_MUL2
    return xor(z, z >> 31)
end

"Derive an independent seed for one numbered lane case."
function case_seed(master::UInt64, index::Integer; lane::UInt64=_DIFFERENTIAL_LANE)
    index >= 1 || throw(ArgumentError("fuzz case indices start at 1"))
    return _splitmix64(xor(master, _splitmix64(UInt64(index)), lane))
end

"A fixed SplitMix64 stream. Its output is independent of Julia and Random versions."
mutable struct StableRNG
    state::UInt64
end

@inline function _next!(rng::StableRNG)
    rng.state += _SPLITMIX_GAMMA
    z = rng.state
    z = xor(z, z >> 30) * _SPLITMIX_MUL1
    z = xor(z, z >> 27) * _SPLITMIX_MUL2
    return xor(z, z >> 31)
end

function _below(rng::StableRNG, bound::Integer)
    bound > 0 || throw(ArgumentError("random bound must be positive"))
    n = UInt64(bound)
    threshold = mod(-n, n)
    while true
        value = _next!(rng)
        value >= threshold && return Int(mod(value, n))
    end
end

_index(rng::StableRNG, n::Integer) = _below(rng, n) + 1
_range(rng::StableRNG, first::Integer, last::Integer) =
    Int(first) + _below(rng, Int(last) - Int(first) + 1)
_choice(rng::StableRNG, values) = values[_index(rng, length(values))]
_random(::Type{UInt64}, rng::StableRNG) = _next!(rng)
_random(::Type{Int64}, rng::StableRNG) = reinterpret(Int64, _next!(rng))
_random(::Type{UInt32}, rng::StableRNG) = UInt32(_next!(rng) & typemax(UInt32))
_random(::Type{Int32}, rng::StableRNG) = reinterpret(Int32, _random(UInt32, rng))
_random(::Type{UInt16}, rng::StableRNG) = UInt16(_next!(rng) & typemax(UInt16))
_random(::Type{Int16}, rng::StableRNG) = reinterpret(Int16, _random(UInt16, rng))
_random(::Type{UInt8}, rng::StableRNG) = UInt8(_next!(rng) & typemax(UInt8))
_random(::Type{Bool}, rng::StableRNG) = isodd(_next!(rng))

_maybe(rng, value) = _range(rng, 1, 5) == 1 ? missing : value

function _randomfloat(rng)
    specials = (0.0, -0.0, Inf, -Inf, NaN)
    pick = _range(rng, 1, 8)
    pick <= length(specials) && return specials[pick]
    # A stable finite IEEE-754 value in [-1, 1). This uses no libm path.
    bits = (_next!(rng) & UInt64(0x000f_ffff_ffff_ffff)) | UInt64(0x3ff0_0000_0000_0000)
    value = reinterpret(Float64, bits) - 1.0
    return isodd(_next!(rng)) ? -value : value
end

function _makepart(rng, n::Int)
    ints = Union{Missing,Int64}[_maybe(rng, _random(Int64, rng)) for _ = 1:n]
    uints = Union{Missing,UInt64}[_maybe(rng, _random(UInt64, rng)) for _ = 1:n]
    floats = Union{Missing,Float64}[_maybe(rng, _randomfloat(rng)) for _ = 1:n]
    bools = Union{Missing,Bool}[_maybe(rng, _random(Bool, rng)) for _ = 1:n]
    strings = Union{Missing,String}[_maybe(rng, _choice(rng, TEXT)) for _ = 1:n]
    dates = Union{Missing,Date}[
        _maybe(rng, Date(1970, 1, 1) + Day(_range(rng, -100_000, 100_000))) for _ = 1:n
    ]
    lists = Vector{Union{Missing,Vector{Union{Missing,Int32}}}}(undef, n)
    StructRow = @NamedTuple{x::Union{Missing,Int16}, label::Union{Missing,String}}
    structs = Vector{Union{Missing,StructRow}}(undef, n)
    dictionary = Union{Missing,String}[_maybe(rng, _choice(rng, TEXT)) for _ = 1:n]
    for i = 1:n
        lists[i] =
            _range(rng, 1, 5) == 1 ? missing :
            Union{Missing,Int32}[
                _maybe(rng, _random(Int32, rng)) for _ = 1:_range(rng, 0, 6)
            ]
        structs[i] =
            _range(rng, 1, 5) == 1 ? missing :
            (x=_maybe(rng, _random(Int16, rng)), label=_maybe(rng, _choice(rng, TEXT)))
    end
    input = (
        ints=ints,
        uints=uints,
        floats=floats,
        bools=bools,
        strings=strings,
        dates=dates,
        lists=lists,
        structs=structs,
        dictionary=Arrow.DictEncode(dictionary),
    )
    return input, merge(input, (dictionary=dictionary,))
end

function _concatparts(parts)
    names = keys(first(parts))
    return NamedTuple{names}(
        map(names) do name
            reduce(vcat, (getproperty(part, name) for part in parts); init=Any[])
        end,
    )
end

function make_case(master::UInt64, index::Integer)
    seed = case_seed(master, index)
    rng = StableRNG(seed)
    nparts = _range(rng, 1, 4)
    made = [_makepart(rng, _range(rng, 0, 20)) for _ = 1:nparts]
    inputs = first.(made)
    expectedparts = last.(made)
    return DifferentialCase(seed, inputs, expectedparts, _concatparts(expectedparts))
end

function ipcbytes(table; file::Bool, compress::Symbol)
    io = IOBuffer()
    Arrow.write(io, table; file=file, compress=compress)
    return take!(io)
end

_expectedstructs(values) =
    [x === missing ? missing : ["x" => x.x, "label" => x.label] for x in values]

const _GENERATED_PUBLIC_ELTYPES = (
    ints=Union{Missing,Int64},
    uints=Union{Missing,UInt64},
    floats=Union{Missing,Float64},
    bools=Union{Missing,Bool},
    strings=Union{Missing,String},
    dates=Union{Missing,Date},
    lists=Union{Missing,Vector{Any}},
    structs=Union{Missing,Vector{Pair{String,Any}}},
    dictionary=Union{Missing,String},
)

_fail(message) = throw(ErrorException(message))
_require(ok::Bool, message) = ok || _fail(message)

"Check the generated native table against Arrow's documented facade shapes."
function check_expected_table(actual, expected; label::AbstractString="table")
    _require(
        collect(Tables.columnnames(actual)) == collect(keys(expected)),
        "$label column names differ",
    )
    for name in keys(_GENERATED_PUBLIC_ELTYPES)
        column = Tables.getcolumn(actual, name)
        expectedeltype = getproperty(_GENERATED_PUBLIC_ELTYPES, name)
        _require(
            eltype(column) === expectedeltype,
            "$label column $name has $(eltype(column)); expected $expectedeltype",
        )
        got = collect(column)
        want =
            name === :structs ? _expectedstructs(expected.structs) :
            getproperty(expected, name)
        _require(isequal(got, want), "$label column $name differs")
    end
    return nothing
end

"Check exact public Tables.scan parity, including empty-projection row counts."
function _check_same_table(actual, expected; label::AbstractString)
    actualnames = collect(Tables.columnnames(actual))
    expectednames = collect(Tables.columnnames(expected))
    _require(actualnames == expectednames, "$label column names differ")
    _require(
        Tables.rowcount(actual) == Tables.rowcount(expected),
        "$label row counts differ",
    )
    for i in eachindex(expectednames)
        got = Tables.getcolumn(actual, i)
        want = Tables.getcolumn(expected, i)
        _require(
            isequal(collect(got), collect(want)),
            "$label column $(expectednames[i]) differs",
        )
        _require(
            eltype(got) === eltype(want),
            "$label column $(expectednames[i]) has $(eltype(got)); expected $(eltype(want))",
        )
    end
    return nothing
end

"Check a layout against values constructed without the Arrow writer or reader."
function _check_expected_layout(actual, expected; label::AbstractString)
    actualnames = collect(Tables.columnnames(actual))
    expectednames = collect(keys(expected))
    _require(actualnames == expectednames, "$label column names differ")
    nrows = Tables.rowcount(actual)
    for name in expectednames
        actualcolumn = Tables.getcolumn(actual, name)
        want = getproperty(expected, name)
        _require(
            eltype(actualcolumn) === eltype(want),
            "$label column $name has $(eltype(actualcolumn)); expected $(eltype(want))",
        )
        got = collect(actualcolumn)
        _require(length(got) == nrows, "$label column $name has the wrong length")
        _require(isequal(got, want), "$label column $name differs from its native oracle")
    end
    return nothing
end

_metadataentries(metadata) =
    metadata === nothing ? Pair{String,String}[] : collect(Pair{String,String}, metadata)

function _check_same_field(actual, expected, path::String)
    _require(actual.name == expected.name, "$path name differs")
    _require(Arrow.AC.typeequal(actual.type, expected.type), "$path descriptor differs")
    _require(actual.nullable == expected.nullable, "$path nullability differs")
    _require(
        _metadataentries(actual.metadata) == _metadataentries(expected.metadata),
        "$path metadata differs",
    )
    _require(
        length(actual.children) == length(expected.children),
        "$path child count differs",
    )
    for i in eachindex(expected.children)
        _check_same_field(actual.children[i], expected.children[i], "$path.$i")
    end
    return nothing
end

function _check_schema(actualschema, expectedschema; label::AbstractString)
    _require(
        actualschema !== nothing && expectedschema !== nothing,
        "$label lost its schema",
    )
    _require(
        actualschema.endianness == expectedschema.endianness,
        "$label schema endianness differs",
    )
    _require(
        _metadataentries(actualschema.metadata) ==
        _metadataentries(expectedschema.metadata),
        "$label schema metadata differs",
    )
    _require(
        length(actualschema.fields) == length(expectedschema.fields),
        "$label field count differs",
    )
    for i in eachindex(expectedschema.fields)
        _check_same_field(
            actualschema.fields[i],
            expectedschema.fields[i],
            "$label field $i",
        )
    end
    return nothing
end

function _check_same_schema(actual, expected; label::AbstractString)
    return _check_schema(getfield(actual, :schema), getfield(expected, :schema); label)
end

function _check_original_schema(actual, expected; label::AbstractString)
    return _check_schema(getfield(actual, :schema), expected; label)
end

function _dateneedle(values)
    for value in values
        ismissing(value) || return value
    end
    return Date(1970, 1, 1)
end

function _temporalscan(index::Int, dates)
    needle = _dateneedle(dates)
    noon = DateTime(needle) + Hour(12)
    mode = mod1(index, 6)
    filter, storage, label = if mode == 1
        (Tables.colcmp(==, Tables.col(:dates), needle), true, :date_scalar)
    elseif mode == 2
        (Tables.colin(Tables.col(:dates), (needle,)), true, :date_tuple)
    elseif mode == 3
        (Tables.colin(Tables.col(:dates), Date[needle]), true, :date_array)
    elseif mode == 4
        (Tables.colin(Tables.col(:dates), Set([needle])), true, :date_set)
    elseif mode == 5
        (Tables.colcmp(!=, Tables.col(:dates), noon), false, :date_scalar_public)
    else
        (Tables.colin(Tables.col(:dates), (noon, needle)), false, :date_tuple_public)
    end
    scan = Tables.Scan(select=(:strings => :text, :dates), filter=filter)
    return (; scan, storage, label)
end

function _scans(master::UInt64, index::Int, dates)
    rng = StableRNG(case_seed(master, index; lane=_SCAN_LANE))
    offset = _range(rng, 0, 8)
    limit = _range(rng, 0, 8)
    threshold = Int64(_range(rng, -8, 8))
    needle = _choice(rng, TEXT)
    return (
        (scan=Tables.Scan(), storage=true, label=:identity),
        (
            scan=Tables.Scan(select=(:ints, :strings, :dictionary)),
            storage=true,
            label=:projection,
        ),
        _temporalscan(index, dates),
        (
            scan=Tables.Scan(select=(), filter=Tables.isnull(Tables.col(:ints))),
            storage=true,
            label=:null_filter,
        ),
        (
            scan=Tables.Scan(
                select=(:ints => Float64 => :as_float, :floats),
                filter=Tables.colcmp(>, Tables.col(:ints), threshold),
                limit=limit,
            ),
            storage=true,
            label=:comparison,
        ),
        (
            scan=Tables.Scan(
                select=(:strings,),
                filter=Tables.colin(Tables.col(:strings), (needle, missing)),
                offset=offset,
            ),
            storage=true,
            label=:string_membership,
        ),
    )
end

_rewritecodec(codec::Symbol) = codec === :none ? :zstd : codec === :zstd ? :lz4 : :none

struct FuzzExtensionID
    value::Int64
end
Base.:(==)(a::FuzzExtensionID, b::FuzzExtensionID) = a.value == b.value
Base.isequal(a::FuzzExtensionID, b::FuzzExtensionID) = isequal(a.value, b.value)
const _FUZZ_EXTENSION_NAME = Symbol("JuliaLang.Arrow.FuzzExtensionID")
ArrowTypes.ArrowType(::Type{FuzzExtensionID}) = Int64
ArrowTypes.toarrow(value::FuzzExtensionID) = value.value
ArrowTypes.arrowname(::Type{FuzzExtensionID}) = _FUZZ_EXTENSION_NAME
ArrowTypes.JuliaType(::Val{_FUZZ_EXTENSION_NAME}, ::Type{Int64}, metadata) = FuzzExtensionID
ArrowTypes.fromarrow(::Type{FuzzExtensionID}, value::Int64) = FuzzExtensionID(value)

struct FuzzExtensionPoint
    x::Int32
    y::Int32
end
Base.:(==)(a::FuzzExtensionPoint, b::FuzzExtensionPoint) = a.x == b.x && a.y == b.y
Base.isequal(a::FuzzExtensionPoint, b::FuzzExtensionPoint) =
    isequal(a.x, b.x) && isequal(a.y, b.y)
const _FUZZ_POINT_NAME = Symbol("JuliaLang.Arrow.FuzzExtensionPoint")
const _FuzzPointStorage = @NamedTuple{y::Int32, x::Int32}
ArrowTypes.ArrowType(::Type{FuzzExtensionPoint}) = _FuzzPointStorage
ArrowTypes.toarrow(value::FuzzExtensionPoint) = (y=value.y, x=value.x)
ArrowTypes.arrowname(::Type{FuzzExtensionPoint}) = _FUZZ_POINT_NAME
ArrowTypes.arrowmetadata(::Type{FuzzExtensionPoint}) = "seeded-fuzz"
ArrowTypes.JuliaType(::Val{_FUZZ_POINT_NAME}, S, metadata) = FuzzExtensionPoint
function ArrowTypes.fromarrowstruct(
    ::Type{FuzzExtensionPoint},
    ::Val{names},
    values...,
) where {names}
    row = NamedTuple{names}(values)
    return FuzzExtensionPoint(row.x, row.y)
end

struct LayoutCase{E}
    label::Symbol
    schema::Arrow.AC.Schema
    batches::Vector{Arrow.AC.RecordBatch}
    expected::E
    retained::Bool
    coverage::Set{Symbol}
end

function _logical_layout_case()
    raw = collect(codeunits("tiny-a-view-value-longer-than-twelve-bytes"))
    longstart = findfirst(==(UInt8('a')), raw)
    longlength = length(raw) - longstart + 1
    payloads = ArrowStrings.ArrowStringPayload[
        ArrowStrings.inline_payload(raw, 1, 4),
        ArrowStrings.view_payload(raw, longstart, longlength, 0, longstart - 1),
        ArrowStrings.PAYLOAD_MISSING,
        ArrowStrings.inline_payload(raw, 1, 0),
    ]
    views = ArrowStrings.StringVector{Union{Missing,ArrowStrings.ArrowString}}(
        payloads,
        Vector{UInt8}[raw],
    )
    maps = Union{Missing,Dict{String,Union{Missing,Int32}}}[
        Dict("a" => Int32(1), "b" => missing),
        Dict{String,Union{Missing,Int32}}(),
        missing,
        Dict("c" => Int32(3)),
    ]
    fixed = NTuple{2,Int16}[
        (Int16(1), Int16(2)),
        (Int16(3), Int16(4)),
        (Int16(5), Int16(6)),
        (Int16(7), Int16(8)),
    ]
    table = (
        extension=FuzzExtensionID.(Int64[1, -2, 3, 4]),
        extension_struct=Union{Missing,FuzzExtensionPoint}[
            FuzzExtensionPoint(Int32(1), Int32(2)),
            missing,
            FuzzExtensionPoint(Int32(3), Int32(4)),
            FuzzExtensionPoint(Int32(-5), Int32(6)),
        ],
        maps=maps,
        fixed=fixed,
        views=views,
        dates=Date[
            Date(1969, 12, 31),
            Date(1970, 1, 1),
            Date(2038, 1, 19),
            Date(2100, 1, 1),
        ],
        datetimes=DateTime[
            DateTime(1969, 12, 31, 23, 59, 59),
            DateTime(1970, 1, 1),
            DateTime(2038, 1, 19, 3, 14, 7),
            DateTime(2100, 1, 1),
        ],
        times=Time[Time(0), Time(12), Time(23, 59, 59, 999), Time(1, 2, 3, 4)],
        durations=Nanosecond[
            Nanosecond(-1),
            Nanosecond(0),
            Nanosecond(1_000_001),
            Nanosecond(86_400_000_000_000),
        ],
    )
    expected = (
        extension=table.extension,
        extension_struct=table.extension_struct,
        maps=map(table.maps) do value
            ismissing(value) && return missing
            return Pair{Any,Any}[key => item for (key, item) in pairs(value)]
        end,
        fixed=Vector{Any}[Any[1, 2], Any[3, 4], Any[5, 6], Any[7, 8]],
        views=Union{Missing,String}[
            "tiny",
            String(copy(@view raw[longstart:end])),
            missing,
            "",
        ],
        dates=table.dates,
        datetimes=table.datetimes,
        times=table.times,
        durations=table.durations,
    )
    decoded = Arrow.readstream(ipcbytes(table; file=false, compress=:none))
    return LayoutCase(
        :logical,
        decoded.schema,
        collect(Arrow.AC.RecordBatch, decoded.batches),
        expected,
        true,
        Set([
            :arrowtypes_extension,
            :arrowtypes_struct_extension,
            :map,
            :fixed_size_list,
            :utf8_view,
            :date,
            :timestamp,
            :time,
            :duration,
        ]),
    )
end

function _fixeddata(t, values; present=trues(length(values)))
    nullcount = count(!, present)
    return Arrow.AC.ArrayData(
        t,
        length(values),
        [Arrow.AC._bitmapbuffer(present), Arrow.AC._databuffer(values)];
        nullcount,
    )
end

function _physical_layout_case()
    AC = Arrow.AC
    fields = AC.Field[]
    columns = AC.ArrayData[]

    binarytype = AC.BinaryType(false)
    push!(fields, AC.Field("binary", binarytype; nullable=false))
    push!(
        columns,
        AC.ArrayData(
            binarytype,
            3,
            [
                AC.BufferSlice(),
                AC._databuffer(Int32[0, 2, 2, 5]),
                AC._databuffer(UInt8[0x00, 0xff, 0x01, 0x02, 0x03]),
            ];
            nullcount=0,
        ),
    )

    largebinarytype = AC.BinaryType(true)
    push!(fields, AC.Field("large_binary", largebinarytype; nullable=false))
    push!(
        columns,
        AC.ArrayData(
            largebinarytype,
            3,
            [
                AC.BufferSlice(),
                AC._databuffer(Int64[0, 2, 2, 5]),
                AC._databuffer(UInt8[0x10, 0x11, 0x12, 0x13, 0x14]),
            ];
            nullcount=0,
        ),
    )

    fixedbinarytype = AC.FixedSizeBinaryType(3)
    push!(fields, AC.Field("fixed_binary", fixedbinarytype; nullable=false))
    push!(
        columns,
        AC.ArrayData(
            fixedbinarytype,
            3,
            [AC.BufferSlice(), AC._databuffer(UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9])];
            nullcount=0,
        ),
    )

    longbinary = collect(UInt8, 0x10:0x20)
    binarypayloads = ArrowStrings.ArrowStringPayload[
        ArrowStrings.inline_payload(UInt8[0x01, 0x02], 1, 2),
        ArrowStrings.view_payload(longbinary, 1, length(longbinary), 0, 0),
        ArrowStrings.PAYLOAD_MISSING,
    ]
    binaryviewtype = AC.ViewType(false)
    push!(fields, AC.Field("binary_view", binaryviewtype; nullable=true))
    push!(
        columns,
        AC.ArrayData(
            binaryviewtype,
            3,
            [
                AC._bitmapbuffer(Bool[true, true, false]),
                AC._databuffer(binarypayloads),
                AC._databuffer(longbinary),
            ];
            nullcount=1,
        ),
    )

    mapkeyfield, mapkeydata = AC.fromjulia("key", ["a", "b", "c"])
    mapvaluefield, mapvaluedata =
        AC.fromjulia("value", Union{Missing,Int32}[Int32(1), missing, Int32(3)])
    entriesfield = AC.Field(
        "entries",
        AC.StructType();
        nullable=false,
        children=[mapkeyfield, mapvaluefield],
    )
    entriesdata = AC.ArrayData(
        AC.StructType(),
        3,
        [AC.BufferSlice()];
        children=[mapkeydata, mapvaluedata],
        nullcount=0,
    )
    maptype = AC.MapType(true)
    push!(fields, AC.Field("sorted_map", maptype; nullable=false, children=[entriesfield]))
    push!(
        columns,
        AC.ArrayData(
            maptype,
            3,
            [AC.BufferSlice(), AC._databuffer(Int32[0, 2, 2, 3])];
            children=[entriesdata],
            nullcount=0,
        ),
    )

    temporal = (
        ("date32", AC.DateType(AC.DAY), Int32[-1, 0, 1], :date_units),
        (
            "date64",
            AC.DateType(AC.MILLISECOND_DATE),
            Int64[-86_400_000, 0, 86_400_000],
            :date_units,
        ),
        ("time_s", AC.TimeType(AC.SECOND, 32), Int32[0, 43_200, 86_399], :time_units),
        (
            "time_ms",
            AC.TimeType(AC.MILLISECOND, 32),
            Int32[0, 43_200_000, 86_399_999],
            :time_units,
        ),
        (
            "time_us",
            AC.TimeType(AC.MICROSECOND, 64),
            Int64[0, 43_200_000_000, 86_399_999_999],
            :time_units,
        ),
        (
            "time_ns",
            AC.TimeType(AC.NANOSECOND, 64),
            Int64[0, 43_200_000_000_000, 86_399_999_999_999],
            :time_units,
        ),
        (
            "timestamp_s",
            AC.TimestampType(AC.SECOND, nothing),
            Int64[-1, 0, 1_000_001],
            :timestamp_units,
        ),
        (
            "timestamp_ms",
            AC.TimestampType(AC.MILLISECOND, nothing),
            Int64[-1, 0, 1_000_001],
            :timestamp_units,
        ),
        (
            "timestamp_us",
            AC.TimestampType(AC.MICROSECOND, "UTC"),
            Int64[-1, 0, 1_000_001],
            :timestamp_units,
        ),
        (
            "timestamp_ns",
            AC.TimestampType(AC.NANOSECOND, "+00:00"),
            Int64[-1, 0, 1_000_001],
            :timestamp_units,
        ),
        (
            "duration_s",
            AC.DurationType(AC.SECOND),
            Int64[-1, 0, 1_000_001],
            :duration_units,
        ),
        (
            "duration_ms",
            AC.DurationType(AC.MILLISECOND),
            Int64[-1, 0, 1_000_001],
            :duration_units,
        ),
        (
            "duration_us",
            AC.DurationType(AC.MICROSECOND),
            Int64[-1, 0, 1_000_001],
            :duration_units,
        ),
        (
            "duration_ns",
            AC.DurationType(AC.NANOSECOND),
            Int64[-1, 0, 1_000_001],
            :duration_units,
        ),
    )
    for (name, type, values, _) in temporal
        push!(fields, AC.Field(name, type; nullable=false))
        push!(columns, _fixeddata(type, values))
    end

    childfield, childdata = AC.fromjulia("item", Int16[1, 2, 3, 4, 5, 6])
    fixedlisttype = AC.FixedSizeListType(2)
    push!(
        fields,
        AC.Field("fixed_list", fixedlisttype; nullable=false, children=[childfield]),
    )
    push!(
        columns,
        AC.ArrayData(
            fixedlisttype,
            3,
            [AC.BufferSlice()];
            children=[childdata],
            nullcount=0,
        ),
    )

    runfield, rundata = AC.fromjulia("run_ends", Int16[2, 3])
    valuefield, valuedata = AC.fromjulia("values", Union{Missing,String}["run", missing])
    reetype = AC.RunEndEncodedType()
    push!(fields, AC.Field("ree", reetype; nullable=true, children=[runfield, valuefield]))
    push!(
        columns,
        AC.ArrayData(
            reetype,
            3,
            AC.BufferSlice[];
            children=[rundata, valuedata],
            nullcount=0,
        ),
    )

    xfield, xdata = AC.fromjulia("x", Int32[1, 0, 3])
    yfield, ydata = AC.fromjulia("y", Union{Missing,String}["a", missing, "c"])
    structtype = AC.StructType()
    push!(
        fields,
        AC.Field("nullable_struct", structtype; nullable=true, children=[xfield, yfield]),
    )
    push!(
        columns,
        AC.ArrayData(
            structtype,
            3,
            [AC._bitmapbuffer(Bool[true, false, true])];
            children=[xdata, ydata],
            nullcount=1,
        ),
    )

    schema = AC.Schema(fields)
    batch = AC.RecordBatch(schema, columns, 3)
    epoch = DateTime(1970, 1, 1)
    expected = (
        binary=Vector{UInt8}[UInt8[0x00, 0xff], UInt8[], UInt8[0x01, 0x02, 0x03]],
        large_binary=Vector{UInt8}[UInt8[0x10, 0x11], UInt8[], UInt8[0x12, 0x13, 0x14]],
        fixed_binary=Vector{UInt8}[UInt8[1, 2, 3], UInt8[4, 5, 6], UInt8[7, 8, 9]],
        binary_view=Union{Missing,Vector{UInt8}}[UInt8[0x01, 0x02], longbinary, missing],
        sorted_map=Vector{Pair{Any,Any}}[
            Pair{Any,Any}["a" => Int32(1), "b" => missing],
            Pair{Any,Any}[],
            Pair{Any,Any}["c" => Int32(3)],
        ],
        date32=Date[Date(1969, 12, 31), Date(1970, 1, 1), Date(1970, 1, 2)],
        date64=DateTime[epoch - Day(1), epoch, epoch + Day(1)],
        time_s=Time[Time(0), Time(12), Time(23, 59, 59)],
        time_ms=Time[Time(0), Time(12), Time(23, 59, 59, 999)],
        time_us=Time[
            Time(Nanosecond(0)),
            Time(Nanosecond(43_200_000_000_000)),
            Time(Nanosecond(86_399_999_999_000)),
        ],
        time_ns=Time[
            Time(Nanosecond(0)),
            Time(Nanosecond(43_200_000_000_000)),
            Time(Nanosecond(86_399_999_999_999)),
        ],
        timestamp_s=DateTime[epoch - Second(1), epoch, epoch + Second(1_000_001)],
        timestamp_ms=DateTime[
            epoch - Millisecond(1),
            epoch,
            epoch + Millisecond(1_000_001),
        ],
        timestamp_us=Int64[-1, 0, 1_000_001],
        timestamp_ns=Int64[-1, 0, 1_000_001],
        duration_s=Second[Second(-1), Second(0), Second(1_000_001)],
        duration_ms=Millisecond[Millisecond(-1), Millisecond(0), Millisecond(1_000_001)],
        duration_us=Microsecond[Microsecond(-1), Microsecond(0), Microsecond(1_000_001)],
        duration_ns=Nanosecond[Nanosecond(-1), Nanosecond(0), Nanosecond(1_000_001)],
        fixed_list=Vector{Any}[Any[1, 2], Any[3, 4], Any[5, 6]],
        ree=Union{Missing,String}["run", "run", missing],
        nullable_struct=Union{Missing,Vector{Pair{String,Any}}}[
            Pair{String,Any}["x" => Int32(1), "y" => "a"],
            missing,
            Pair{String,Any}["x" => Int32(3), "y" => "c"],
        ],
    )
    return LayoutCase(
        :physical,
        schema,
        [batch],
        expected,
        true,
        Set([
            :binary,
            :large_binary,
            :fixed_size_binary,
            :binary_view,
            :map_sorted,
            :fixed_size_list,
            :date_units,
            :time_units,
            :timestamp_units,
            :duration_units,
            :run_end_encoded,
            :nullable_struct_parent,
        ]),
    )
end

function _union_layout_case()
    AC = Arrow.AC
    denseintfield, denseintdata = AC.fromjulia("i", Int64[1, 3])
    densestrfield, densestrdata = AC.fromjulia("s", ["two"])
    densenulltype = AC.NullType()
    densenullfield = AC.Field("null", densenulltype; nullable=true)
    densenulldata = AC.ArrayData(densenulltype, 1, AC.BufferSlice[]; nullcount=1)
    densetype = AC.UnionType(AC.DenseMode, Int8[0, 1, 2])
    densefield = AC.Field(
        "dense",
        densetype;
        nullable=true,
        children=[denseintfield, densestrfield, densenullfield],
    )
    densedata = AC.ArrayData(
        densetype,
        4,
        [AC._databuffer(Int8[0, 1, 2, 0]), AC._databuffer(Int32[0, 0, 0, 1])];
        children=[denseintdata, densestrdata, densenulldata],
        nullcount=0,
    )

    sparseintfield, sparseintdata = AC.fromjulia("i", Int64[1, 0, 3, 0])
    sparsetextfield, sparsetextdata = AC.fromjulia("s", ["", "two", "", "four"])
    sparsetype = AC.UnionType(AC.SparseMode, Int8[3, 7])
    sparsefield = AC.Field(
        "sparse",
        sparsetype;
        nullable=false,
        children=[sparseintfield, sparsetextfield],
    )
    sparsedata = AC.ArrayData(
        sparsetype,
        4,
        [AC._databuffer(Int8[3, 7, 3, 7])];
        children=[sparseintdata, sparsetextdata],
        nullcount=0,
    )
    schema = AC.Schema([densefield, sparsefield])
    return LayoutCase(
        :unions,
        schema,
        [AC.RecordBatch(schema, [densedata, sparsedata], 4)],
        (
            dense=Any[Int64(1), "two", missing, Int64(3)],
            sparse=Any[Int64(1), "two", Int64(3), "four"],
        ),
        false,
        Set([:dense_union, :sparse_union]),
    )
end

function _decode_batches(bytes::Vector{UInt8}, file::Bool)
    if file
        decoded = Arrow.readfile(copy(bytes))
        return decoded.schema, Arrow.AC.RecordBatch[decoded[i] for i = 1:length(decoded)]
    end
    decoded = Arrow.readstream(copy(bytes))
    return decoded.schema, collect(Arrow.AC.RecordBatch, decoded.batches)
end

function _layoutbytes(schema, batches; file::Bool, compress::Symbol)
    return file ? Arrow.writefile(schema, batches; compress) :
           Arrow.writestream(schema, batches; compress)
end

function _run_layout_case(case::LayoutCase)
    baseline = Arrow.Table(Arrow.writestream(case.schema, case.batches))
    _check_expected_layout(baseline, case.expected; label="layout=$(case.label) baseline")
    _check_original_schema(
        baseline,
        case.schema;
        label="layout=$(case.label) baseline schema",
    )
    if case.label === :logical
        fields = getfield(baseline, :schema).fields
        extensionmeta = Dict(_metadataentries(fields[1].metadata))
        pointmeta = Dict(_metadataentries(fields[2].metadata))
        _require(
            extensionmeta["ARROW:extension:name"] == String(_FUZZ_EXTENSION_NAME),
            "scalar ArrowTypes extension name was not written",
        )
        _require(
            pointmeta["ARROW:extension:name"] == String(_FUZZ_POINT_NAME) &&
                pointmeta["ARROW:extension:metadata"] == "seeded-fuzz",
            "struct ArrowTypes extension metadata was not written",
        )
        _require(
            eltype(baseline.extension) === FuzzExtensionID,
            "scalar extension was not lifted",
        )
        _require(
            eltype(baseline.extension_struct) === Union{Missing,FuzzExtensionPoint},
            "nullable struct extension was not lifted",
        )
    end
    checks = 0
    coverage = copy(case.coverage)
    for file in (false, true), compress in (:none, :lz4, :zstd)
        label = "layout=$(case.label) file=$file compress=$compress"
        bytes = _layoutbytes(case.schema, case.batches; file, compress)
        actual = Arrow.Table(copy(bytes))
        _check_expected_layout(actual, case.expected; label)
        _check_same_table(actual, baseline; label)
        _check_same_schema(actual, baseline; label)
        _check_original_schema(actual, case.schema; label="$label original schema")
        checks += 1

        decodedschema, decodedbatches = _decode_batches(bytes, file)
        rewritten = _layoutbytes(
            decodedschema,
            decodedbatches;
            file=(!file),
            compress=_rewritecodec(compress),
        )
        rewrittenactual = Arrow.Table(copy(rewritten))
        _check_expected_layout(rewrittenactual, case.expected; label="$label IPC rewrite")
        _check_same_table(rewrittenactual, baseline; label="$label IPC rewrite")
        _check_same_schema(rewrittenactual, baseline; label="$label IPC rewrite")
        _check_original_schema(
            rewrittenactual,
            case.schema;
            label="$label IPC rewrite original schema",
        )
        checks += 1

        if case.retained
            retained = ipcbytes(
                Arrow.Table(copy(bytes));
                file=(!file),
                compress=_rewritecodec(compress),
            )
            retainedactual = Arrow.Table(copy(retained))
            _check_expected_layout(
                retainedactual,
                case.expected;
                label="$label retained rewrite",
            )
            _check_same_table(retainedactual, baseline; label="$label retained rewrite")
            _check_same_schema(retainedactual, baseline; label="$label retained rewrite")
            _check_original_schema(
                retainedactual,
                case.schema;
                label="$label retained rewrite original schema",
            )
            push!(coverage, :retained_rewrite)
            checks += 1
        end
    end
    return checks, coverage
end

function _run_statistics_pruning()
    parts = [
        (x=Int64[1, 2, 3, 4, 5], payload=fill(repeat("a", 128), 5)),
        (x=Int64[6, 7, 8, 9, 10], payload=fill(repeat("z", 128), 5)),
    ]
    streambytes = ipcbytes(Tables.partitioner(parts); file=false, compress=:none)
    stream = Arrow.readstream(streambytes)
    bytes = Arrow.statsfile(stream.schema, stream.batches)
    scan = Tables.Scan(select=(:x,), filter=Tables.col(:x) > 7)
    reference = Tables.scan(
        (
            x=reduce(vcat, getproperty.(parts, :x)),
            payload=reduce(vcat, getproperty.(parts, :payload)),
        ),
        scan,
    )
    source = FuzzBytesSource(copy(bytes))
    actual = Arrow.Table(
        Arrow.SourceFile(source; limits=STRICT_LIMITS, tailbytes=32, coalesce_gap=0);
        scan,
    )
    _check_same_table(actual, reference; label="statistics-pruned ranged scan")
    firstblock = Arrow.readfile(copy(bytes)).recordblocks[1]
    firstspan = (firstblock[1], firstblock[2] + firstblock[3])
    _require(
        !any(request -> _rangesintersect(request, firstspan), source.requests),
        "statistics-pruned batch was fetched",
    )
    _require(
        !any(request -> request == (Int64(0), Int64(length(bytes))), source.requests),
        "statistics-pruned scan fetched the full object",
    )
    return 1, Set([:statistics_pruning, :ranged_no_full_fetch])
end

function run_layouts()
    checks = 0
    coverage = Set{Symbol}()
    for case in (_logical_layout_case(), _physical_layout_case(), _union_layout_case())
        casechecks, casecoverage = _run_layout_case(case)
        checks += casechecks
        union!(coverage, casecoverage)
    end
    statchecks, statcoverage = _run_statistics_pruning()
    checks += statchecks
    union!(coverage, statcoverage)
    return checks, coverage
end

function _run_differential_case(master::UInt64, index::Int)
    case = make_case(master, index)
    nparts = length(case.inputs)
    variants = 0
    rewrites = 0
    scanchecks = 0
    for file in (false, true), compress in (:none, :lz4, :zstd)
        label = "case=$index file=$file compress=$compress"
        bytes = ipcbytes(Tables.partitioner(case.inputs); file=file, compress=compress)
        check_expected_table(Arrow.Table(copy(bytes)), case.expected; label="$label whole")

        batches = collect(Arrow.Stream(copy(bytes)))
        _require(length(batches) == nparts, "$label partition count differs")
        for (partition, (batch, expectedpart)) in
            enumerate(zip(batches, case.expectedparts))
            check_expected_table(batch, expectedpart; label="$label partition=$partition")
        end

        rewritten = ipcbytes(
            Arrow.Stream(copy(bytes));
            file=(!file),
            compress=_rewritecodec(compress),
        )
        check_expected_table(
            Arrow.Table(copy(rewritten)),
            case.expected;
            label="$label rewrite",
        )
        rewrittenparts = collect(Arrow.Stream(copy(rewritten)))
        _require(length(rewrittenparts) == nparts, "$label rewrite partition count differs")
        for (partition, (batch, expectedpart)) in
            enumerate(zip(rewrittenparts, case.expectedparts))
            check_expected_table(
                batch,
                expectedpart;
                label="$label rewrite partition=$partition",
            )
        end

        full = Arrow.Table(copy(bytes))
        for (scanindex, scancase) in enumerate(_scans(master, index, case.expected.dates))
            scan = scancase.scan
            plan = Arrow._ScanPlan(scan, getfield(full, :schema).fields)
            _require(
                (plan.storage !== nothing) == scancase.storage,
                "$label scan=$(scancase.label) used the wrong evaluation domain",
            )
            reference = Tables.scan(full, scan)
            whole = Arrow.Table(copy(bytes); scan=scan)
            _check_same_table(whole, reference; label="$label whole scan=$scanindex")
            scanchecks += 1
            if file
                source = FuzzBytesSource(copy(bytes))
                ranged = Arrow.Table(
                    Arrow.SourceFile(
                        source;
                        limits=STRICT_LIMITS,
                        tailbytes=32,
                        coalesce_gap=0,
                    );
                    scan=scan,
                )
                _check_same_table(ranged, reference; label="$label ranged scan=$scanindex")
                _require(
                    !any(
                        request -> request == (Int64(0), Int64(length(bytes))),
                        source.requests,
                    ),
                    "$label ranged scan=$scanindex fetched the full object",
                )
                scanchecks += 1
            end
        end
        variants += 1
        rewrites += 1
    end
    return (variants=variants, rewrites=rewrites, scanchecks=scanchecks)
end

function _failuretext(err, bt)
    return sprint() do io
        showerror(io, err, bt)
    end
end

function _repropath(dir::AbstractString, lane::Symbol, index::Int, seed::UInt64, ext)
    name = "$(lane)-$(lpad(index, 6, '0'))-$(string(seed; base=16)).$ext"
    return joinpath(dir, name)
end

const _REPLAY_SCRIPT = "sh ./replay.sh /path/to/Arrow.jl"

function _save_text_repro(
    dir::Union{Nothing,AbstractString},
    lane::Symbol,
    index::Int,
    seed::UInt64,
    text::AbstractString,
)
    dir === nothing && return nothing
    mkpath(dir)
    path = _repropath(dir, lane, index, seed, "txt")
    open(path, "w") do io
        Base.write(io, text)
    end
    return path
end

function _save_mutation_repro(
    dir::Union{Nothing,AbstractString},
    index::Int,
    seed::UInt64,
    bytes::Vector{UInt8},
    text::AbstractString,
)
    dir === nothing && return nothing
    mkpath(dir)
    Base.write(_repropath(dir, :mutation, index, seed, "arrowbytes"), bytes)
    return _save_text_repro(dir, :mutation, index, seed, text)
end

_active_repro_path(dir::AbstractString, lane::Symbol, ext::AbstractString) =
    joinpath(dir, "active-$(String(lane)).$ext")

function _save_active_repro(
    dir::Union{Nothing,AbstractString},
    lane::Symbol,
    text::AbstractString;
    bytes::Union{Nothing,Vector{UInt8}}=nothing,
)
    dir === nothing && return nothing
    mkpath(dir)
    open(_active_repro_path(dir, lane, "txt"), "w") do io
        Base.write(io, text)
    end
    bytes === nothing || Base.write(_active_repro_path(dir, lane, "arrowbytes"), bytes)
    return nothing
end

function _clear_active_repro(dir::Union{Nothing,AbstractString}, lane::Symbol)
    dir === nothing && return nothing
    for ext in ("txt", "arrowbytes")
        path = _active_repro_path(dir, lane, ext)
        isfile(path) && rm(path)
    end
    return nothing
end

function run_differential(;
    seed::UInt64=DEFAULT_SEED,
    cases::Integer=EXTENDED_CASES,
    case_index::Union{Nothing,Integer}=nothing,
    include_layouts::Bool=true,
    reproduction_dir::Union{Nothing,AbstractString}=nothing,
    repro_environment::Union{Nothing,String}=nothing,
    progress_io::Union{Nothing,IO}=nothing,
)
    cases >= 0 || throw(ArgumentError("negative differential case count"))
    case_index === nothing ||
        case_index >= 1 ||
        throw(ArgumentError("differential case indices start at 1"))
    variants = 0
    rewrites = 0
    scanchecks = 0
    layoutchecks = 0
    coverage = Set{Symbol}()
    reproenv =
        reproduction_dir === nothing ? nothing :
        repro_environment === nothing ? _environmenttext() : repro_environment
    if include_layouts
        if reproenv !== nothing
            layoutreplay =
                reproenv *
                "\nmaster_seed=0x$(string(seed; base=16))\n" *
                "replay=$_REPLAY_SCRIPT --seed " *
                "0x$(string(seed; base=16)) --cases 0 --layouts-only\n\n" *
                "active layout lane; the process may have been interrupted\n"
            _save_active_repro(reproduction_dir, :layout, layoutreplay)
        end
        try
            layoutchecks, layoutcoverage = run_layouts()
            union!(coverage, layoutcoverage)
        catch err
            err isa Union{InterruptException,OutOfMemoryError} && rethrow()
            bt = catch_backtrace()
            details = _failuretext(err, bt)
            if reproenv !== nothing
                text =
                    reproenv *
                    "\nmaster_seed=0x$(string(seed; base=16))\n" *
                    "replay=$_REPLAY_SCRIPT --seed " *
                    "0x$(string(seed; base=16)) --cases 0 --layouts-only\n\n$details\n"
                _save_text_repro(reproduction_dir, :layout, 0, seed, text)
            end
            _clear_active_repro(reproduction_dir, :layout)
            throw(FuzzFailure(:layout, 0, seed, seed, details))
        end
        _clear_active_repro(reproduction_dir, :layout)
    end
    indices = case_index === nothing ? (1:Int(cases)) : (Int(case_index):Int(case_index))
    total = length(indices)
    total == 0 || push!(coverage, :generated_differential)
    progress_every = max(1, min(64, total ÷ 8))
    for (completed, index) in enumerate(indices)
        derived = case_seed(seed, index)
        if reproenv !== nothing
            replay =
                reproenv *
                "\nmaster_seed=0x$(string(seed; base=16))\n" *
                "case_seed=0x$(string(derived; base=16))\nindex=$index\n" *
                "replay=$_REPLAY_SCRIPT --seed " *
                "0x$(string(seed; base=16)) --case-index $index\n\n" *
                "active differential case; the process may have been interrupted\n"
            _save_active_repro(reproduction_dir, :differential, replay)
        end
        result = try
            _run_differential_case(seed, index)
        catch err
            err isa Union{InterruptException,OutOfMemoryError} && rethrow()
            bt = catch_backtrace()
            details = _failuretext(err, bt)
            if reproenv !== nothing
                command =
                    reproenv *
                    "\nmaster_seed=0x$(string(seed; base=16))\n" *
                    "case_seed=0x$(string(derived; base=16))\nindex=$index\n" *
                    "replay=$_REPLAY_SCRIPT --seed " *
                    "0x$(string(seed; base=16)) --case-index $index\n\n$details\n"
                _save_text_repro(reproduction_dir, :differential, index, derived, command)
            end
            _clear_active_repro(reproduction_dir, :differential)
            throw(FuzzFailure(:differential, index, seed, derived, details))
        end
        _clear_active_repro(reproduction_dir, :differential)
        variants += result.variants
        rewrites += result.rewrites
        scanchecks += result.scanchecks
        if progress_io !== nothing &&
           (completed == total || completed % progress_every == 0)
            println(progress_io, "differential cases: $completed/$total")
        end
    end
    return DifferentialSummary(
        seed,
        total,
        variants,
        rewrites,
        scanchecks,
        layoutchecks,
        coverage,
    )
end

struct MutationCorpus
    label::Symbol
    file::Bool
    bytes::Vector{UInt8}
end

struct MutationRoute
    corpus::MutationCorpus
    lane::Symbol
end

function _mutationtable()
    Part = @NamedTuple{
        id::Vector{Int64},
        text::Vector{Union{Missing,String}},
        list::Vector{Vector{Int32}},
        dict::Arrow.DictEncode{Union{Missing,String},Vector{Union{Missing,String}}},
    }
    firstpart = (
        id=Int64[1, -2, 3],
        text=Union{Missing,String}["alpha", missing, "\0"],
        list=Vector{Int32}[Int32[1, 2], Int32[], Int32[3]],
        dict=Arrow.DictEncode(Union{Missing,String}["x", missing, "y"]),
    )
    secondpart = (
        id=Int64[4, 5],
        text=Union{Missing,String}["\u03bb", "line\nbreak"],
        list=Vector{Int32}[Int32[4], Int32[5, 6]],
        dict=Arrow.DictEncode(Union{Missing,String}["y", "z"]),
    )
    return Tables.partitioner(Part[firstpart, secondpart])
end

function _mutationcorpus()
    corpus = MutationCorpus[]
    for file in (false, true), compress in (:none, :lz4, :zstd)
        label = Symbol(file ? "file_$compress" : "stream_$compress")
        push!(
            corpus,
            MutationCorpus(label, file, ipcbytes(_mutationtable(); file, compress)),
        )
    end
    for case in (_logical_layout_case(), _physical_layout_case(), _union_layout_case())
        for file in (false, true), compress in (:none, :lz4, :zstd)
            label = Symbol("layout_$(case.label)_$(file ? "file" : "stream")_$compress")
            push!(
                corpus,
                MutationCorpus(
                    label,
                    file,
                    _layoutbytes(case.schema, case.batches; file, compress),
                ),
            )
        end
    end
    statssource = Arrow.readstream(ipcbytes(_mutationtable(); file=false, compress=:none))
    push!(
        corpus,
        MutationCorpus(
            :stats_file,
            true,
            Arrow.statsfile(statssource.schema, statssource.batches),
        ),
    )
    return corpus
end

function _mutationroutes(corpus=_mutationcorpus())
    routes = MutationRoute[]
    for entry in corpus
        if entry.file
            push!(routes, MutationRoute(entry, :file_full))
            push!(routes, MutationRoute(entry, :file_ranged))
        else
            push!(routes, MutationRoute(entry, :stream_full))
        end
        entry.label in (:stream_none, :file_none) &&
            push!(routes, MutationRoute(entry, :auto_full))
        entry.label === :stats_file && push!(routes, MutationRoute(entry, :stats_filtered))
    end
    return routes
end

const _MUTATION_OPERATIONS = (:flip, :set, :delete, :insert, :truncate)

function _mutatebytes(base::Vector{UInt8}, rng, required::Symbol)
    bytes = copy(base)
    recipe = String[]
    operations = Symbol[]
    required in _MUTATION_OPERATIONS || throw(ArgumentError("unknown mutation $required"))
    operationcount = _range(rng, 1, 4)
    for operationindex = 1:operationcount
        requested = operationindex == 1 ? required : _choice(rng, _MUTATION_OPERATIONS)
        op = isempty(bytes) ? :insert : requested
        push!(operations, op)
        if op === :flip
            index = _range(rng, firstindex(bytes), lastindex(bytes))
            bit = UInt8(1) << _range(rng, 0, 7)
            bytes[index] = xor(bytes[index], bit)
            push!(recipe, "flip[$index]=0x$(string(bit; base=16, pad=2))")
        elseif op === :set
            index = _range(rng, firstindex(bytes), lastindex(bytes))
            value = xor(bytes[index], UInt8(_range(rng, 1, typemax(UInt8))))
            bytes[index] = value
            push!(recipe, "set[$index]=0x$(string(value; base=16, pad=2))")
        elseif op === :delete
            start = _range(rng, firstindex(bytes), lastindex(bytes))
            count = _range(rng, 1, min(8, length(bytes) - start + 1))
            deleteat!(bytes, start:(start + count - 1))
            push!(recipe, "delete[$start:$count]")
        elseif op === :insert
            count = _range(rng, 1, 8)
            index = _range(rng, 1, length(bytes) + 1)
            inserted = UInt8[_random(UInt8, rng) for _ = 1:count]
            for (offset, value) in enumerate(inserted)
                insert!(bytes, index + offset - 1, value)
            end
            push!(recipe, "insert[$index:$count]")
        else
            newlength = _range(rng, 0, length(bytes) - 1)
            resize!(bytes, newlength)
            push!(recipe, "truncate[$newlength]")
        end
    end
    if bytes == base
        if isempty(bytes)
            push!(bytes, 0x01)
            push!(operations, :insert)
            push!(recipe, "ensure-insert[1]=0x01")
        else
            index = firstindex(bytes)
            bytes[index] = xor(bytes[index], 0x01)
            push!(operations, :flip)
            push!(recipe, "ensure-flip[$index]=0x01")
        end
    end
    length(bytes) <= length(base) + 32 || _fail("mutation exceeded its byte-growth bound")
    bytes != base || _fail("mutation did not change its input")
    return bytes, join(recipe, ","), operations
end

function _exercise_mutation(bytes::Vector{UInt8}, lane::Symbol)
    requests = nothing
    if lane === :stream_full
        table = Arrow.Table(Arrow.readstream(copy(bytes); limits=STRICT_LIMITS))
    elseif lane === :file_full
        table = Arrow.Table(Arrow.readfile(copy(bytes); limits=STRICT_LIMITS))
    elseif lane === :file_ranged
        fuzzsource = FuzzBytesSource(copy(bytes))
        source =
            Arrow.SourceFile(fuzzsource; limits=STRICT_LIMITS, tailbytes=32, coalesce_gap=0)
        try
            table = Arrow.Table(source; scan=Tables.Scan())
        finally
            if length(bytes) > 32 && any(
                request -> request == (Int64(0), Int64(length(bytes))),
                fuzzsource.requests,
            )
                _fail("ranged mutation fetched the full object")
            end
        end
        requests = copy(fuzzsource.requests)
    elseif lane === :auto_full
        table = Arrow.Table(copy(bytes); limits=STRICT_LIMITS)
    elseif lane === :stats_filtered
        file = Arrow.readfile(copy(bytes); limits=STRICT_LIMITS)
        valuefiltered = !isempty(file.fields) && file.fields[1].type isa Arrow.AC.IntType
        scan =
            valuefiltered ? Tables.Scan(select=(1,), filter=Tables.col(1) > 3) :
            Tables.Scan(filter=Tables.AlwaysFalse())
        reference = Tables.scan(Arrow.Table(file), scan)
        fuzzsource = FuzzBytesSource(copy(bytes))
        source =
            Arrow.SourceFile(fuzzsource; limits=STRICT_LIMITS, tailbytes=32, coalesce_gap=0)
        table = Tables.scan(source, scan)
        _check_same_table(table, reference; label="statistics-filtered mutation")
        if !valuefiltered
            for block in file.recordblocks
                span = (block[1], block[2] + block[3])
                any(request -> _rangesintersect(request, span), fuzzsource.requests) &&
                    _fail("constant-false statistics mutation fetched a record batch")
            end
        end
        if length(bytes) > 32 &&
           any(request -> request == (Int64(0), Int64(length(bytes))), fuzzsource.requests)
            _fail("statistics-filtered mutation fetched the full object")
        end
        requests = copy(fuzzsource.requests)
    else
        throw(ArgumentError("unknown mutation lane $lane"))
    end
    nrows = Tables.rowcount(table)
    names = collect(Symbol, Tables.columnnames(table))
    eltypes = Type[]
    values = Any[]
    for index in eachindex(names)
        column = Tables.getcolumn(table, index)
        collected = collect(column)
        length(collected) == nrows ||
            _fail("accepted mutation produced a short column $(names[index])")
        push!(eltypes, eltype(column))
        push!(values, collected)
    end
    return (; nrows, names, eltypes, values, requests)
end

function _source_revision()
    githubsha = strip(get(ENV, "GITHUB_SHA", ""))
    isempty(githubsha) || return githubsha
    root = normpath(joinpath(@__DIR__, "..", ".."))
    try
        head = readchomp(`git -C $root rev-parse HEAD`)
        dirty = !isempty(readchomp(`git -C $root status --short --untracked-files=normal`))
        return dirty ? "$head-dirty" : head
    catch err
        err isa Union{InterruptException,OutOfMemoryError} && rethrow()
        return "unknown"
    end
end

function _environmenttext()
    packages = (Arrow, ArrowStrings, ArrowTypes, Tables)
    lines = String["julia_version=$(VERSION)", "source_revision=$(_source_revision())"]
    for package in packages
        push!(lines, "$(nameof(package))_version=$(Base.pkgversion(package))")
    end
    return join(lines, '\n')
end

function _snapshot_environment(
    dir::Union{Nothing,AbstractString},
    activeproject::Union{Nothing,AbstractString}=Base.active_project(),
)
    dir === nothing && return nothing
    activeproject === nothing &&
        error("cannot snapshot a fuzz environment without an active Project.toml")
    project = abspath(activeproject)
    isfile(project) || error("active fuzz Project.toml does not exist: $project")
    manifest = joinpath(dirname(project), "Manifest.toml")
    isfile(manifest) ||
        error("active fuzz environment has no Manifest.toml beside $project")
    target = joinpath(dir, "environment")
    mkpath(target)
    cp(project, joinpath(target, "Project.toml"); force=true)
    cp(manifest, joinpath(target, "Manifest.toml"); force=true)
    script = joinpath(dir, "replay.sh")
    open(script, "w") do io
        Base.write(io, _replay_script_text())
    end
    restore = _repro_restore_text()
    open(joinpath(target, "RESTORE.txt"), "w") do io
        Base.write(io, restore)
    end
    return restore
end

function _replay_script_text()
    return """#!/bin/sh
set -eu
if [ \"\$#\" -lt 1 ]; then
    echo \"usage: sh ./replay.sh /path/to/Arrow.jl [fuzz options]\" >&2
    exit 2
fi
artifact_dir=\$(CDPATH= cd -- \"\$(dirname -- \"\$0\")\" && pwd)
checkout=\$(CDPATH= cd -- \"\$1\" && pwd)
shift
if [ ! -f \"\$checkout/Project.toml\" ] || [ ! -f \"\$checkout/test/fuzz.jl\" ]; then
    echo \"not an Arrow.jl checkout: \$checkout\" >&2
    exit 2
fi
backup=\$(mktemp -d \"\${TMPDIR:-/tmp}/arrow-replay.XXXXXX\")
cp \"\$checkout/Project.toml\" \"\$backup/Project.toml\"
had_manifest=false
if [ -f \"\$checkout/Manifest.toml\" ]; then
    cp \"\$checkout/Manifest.toml\" \"\$backup/Manifest.toml\"
    had_manifest=true
fi
cleanup() {
    cp \"\$backup/Project.toml\" \"\$checkout/Project.toml\"
    if [ \"\$had_manifest\" = true ]; then
        cp \"\$backup/Manifest.toml\" \"\$checkout/Manifest.toml\"
    else
        rm -f \"\$checkout/Manifest.toml\"
    fi
    rm -rf \"\$backup\"
}
trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
cp \"\$artifact_dir/environment/Project.toml\" \"\$checkout/Project.toml\"
cp \"\$artifact_dir/environment/Manifest.toml\" \"\$checkout/Manifest.toml\"
\"\${JULIA:-julia}\" --startup-file=no --history-file=no --project=\"\$checkout\" \\
    -e 'using Pkg; Pkg.instantiate()'
if [ \"\${1-}\" = \"--prepare-only\" ]; then
    exit 0
fi
cd \"\$artifact_dir\"
set +e
\"\${JULIA:-julia}\" --startup-file=no --history-file=no --project=\"\$checkout\" \\
    \"\$checkout/test/fuzz.jl\" \"\$@\"
status=\$?
set -e
exit \"\$status\"
"""
end

_repro_restore_text() =
    "environment_restore=$_REPLAY_SCRIPT --prepare-only " *
    "# instantiates the pinned environment and restores the checkout files"

function _mutation_details(
    master::UInt64,
    derived::UInt64,
    index::Int,
    entry::MutationCorpus,
    lane::Symbol,
    recipe::AbstractString,
    environment::AbstractString=_environmenttext(),
)
    return environment *
           "\n" *
           "master_seed=0x$(string(master; base=16))\n" *
           "case_seed=0x$(string(derived; base=16))\n" *
           "index=$index\ncorpus=$(entry.label)\nlane=$lane\nrecipe=$recipe\n"
end

"Quote one argument for the POSIX shell used by the replay command."
_shellquote(value::AbstractString) = "'" * replace(String(value), "'" => "'\"'\"'") * "'"

function _mutation_repro_text(
    master::UInt64,
    derived::UInt64,
    index::Int,
    entry::MutationCorpus,
    lane::Symbol,
    recipe::AbstractString,
    artifact::AbstractString,
    details::AbstractString,
    environment::AbstractString=_environmenttext(),
)
    return _mutation_details(master, derived, index, entry, lane, recipe, environment) *
           "replay_generated=$_REPLAY_SCRIPT --seed " *
           "0x$(string(master; base=16)) --cases 0 --mutation-index $index " *
           "--skip-layouts\n" *
           "replay_bytes=$_REPLAY_SCRIPT --cases 0 --mutations 0 " *
           "--mutation-file $(_shellquote(basename(artifact))) " *
           "--mutation-lane $lane\n\n$details\n"
end

function _required_mutation_sweep_coverage!(
    mutations::Int,
    routes,
    routecounts,
    operationcounts,
)
    mutations == 0 && return nothing
    if mutations >= length(routes)
        missingroutes = Set((route.corpus.label, route.lane) for route in routes)
        setdiff!(missingroutes, keys(routecounts))
        isempty(missingroutes) || _fail("mutation routes were not covered: $missingroutes")
    end
    if mutations >= length(_MUTATION_OPERATIONS)
        missingops = setdiff(Set(_MUTATION_OPERATIONS), keys(operationcounts))
        isempty(missingops) || _fail("mutation operations were not covered: $missingops")
    end
    return nothing
end

function _classify_mutation(bytes::Vector{UInt8}, lane::Symbol)
    try
        snapshot = _exercise_mutation(bytes, lane)
        return (; category=:accepted, snapshot, error=nothing, backtrace=nothing)
    catch err
        err isa Union{InterruptException,OutOfMemoryError} && rethrow()
        category =
            err isa Arrow.ValidationError ? :validation :
            err isa Arrow.AllocationLimitError ? :allocation_limit : :unexpected
        return (; category, snapshot=nothing, error=err, backtrace=catch_backtrace())
    end
end

_errortext(err) = sprint(showerror, err)

function _same_mutation_outcome(first, second)
    first.category === second.category || return false
    if first.category === :accepted
        return isequal(first.snapshot, second.snapshot)
    end
    return typeof(first.error) === typeof(second.error) &&
           _errortext(first.error) == _errortext(second.error)
end

function _mutation_outcome_text(outcome)
    if outcome.category === :accepted
        return "accepted snapshot=$(repr(outcome.snapshot))"
    end
    return "$(outcome.category) $(typeof(outcome.error)): $(_errortext(outcome.error))"
end

_shouldverify(completed::Int, routecount::Int, every::Integer) =
    every > 0 && (completed <= routecount || completed % every == 0)

function run_mutations(;
    seed::UInt64=DEFAULT_SEED,
    mutations::Integer=EXTENDED_MUTATIONS,
    mutation_index::Union{Nothing,Integer}=nothing,
    reproduction_dir::Union{Nothing,AbstractString}=nothing,
    repro_environment::Union{Nothing,String}=nothing,
    verify_determinism::Bool=false,
    determinism_every::Integer=0,
    progress_io::Union{Nothing,IO}=nothing,
)
    mutations >= 0 || throw(ArgumentError("negative mutation count"))
    determinism_every >= 0 || throw(ArgumentError("negative determinism interval"))
    mutation_index === nothing ||
        mutation_index >= 1 ||
        throw(ArgumentError("mutation indices start at 1"))
    routes = _mutationroutes()
    reproenv =
        reproduction_dir === nothing ? nothing :
        repro_environment === nothing ? _environmenttext() : repro_environment
    counts = Dict{Symbol,Int}(:accepted => 0, :validation => 0, :allocation_limit => 0)
    routecounts = Dict{Tuple{Symbol,Symbol},Int}()
    operationcounts = Dict{Symbol,Int}()
    indices =
        mutation_index === nothing ? (1:Int(mutations)) :
        (Int(mutation_index):Int(mutation_index))
    total = length(indices)
    determinismchecks = 0
    progress_every = max(1, min(2_000, total ÷ 10))
    for (completed, index) in enumerate(indices)
        derived = case_seed(seed, index; lane=_MUTATION_LANE)
        rng = StableRNG(derived)
        route = routes[mod(index - 1, length(routes)) + 1]
        entry, lane = route.corpus, route.lane
        required = _MUTATION_OPERATIONS[mod(index - 1, length(_MUTATION_OPERATIONS)) + 1]
        bytes, recipe, operations = _mutatebytes(entry.bytes, rng, required)
        if reproenv !== nothing
            activeartifact = _active_repro_path(reproduction_dir, :mutation, "arrowbytes")
            activetext = _mutation_repro_text(
                seed,
                derived,
                index,
                entry,
                lane,
                recipe,
                activeartifact,
                "active mutation; the process may have been interrupted",
                reproenv,
            )
            _save_active_repro(reproduction_dir, :mutation, activetext; bytes)
        end
        outcome = _classify_mutation(bytes, lane)
        if outcome.category === :unexpected
            details = _failuretext(outcome.error, outcome.backtrace)
            if reproenv !== nothing
                artifact =
                    _repropath(reproduction_dir, :mutation, index, derived, "arrowbytes")
                text = _mutation_repro_text(
                    seed,
                    derived,
                    index,
                    entry,
                    lane,
                    recipe,
                    artifact,
                    details,
                    reproenv,
                )
                _save_mutation_repro(reproduction_dir, index, derived, bytes, text)
            end
            _clear_active_repro(reproduction_dir, :mutation)
            throw(FuzzFailure(:mutation, index, seed, derived, details))
        end
        sample_determinism = _shouldverify(completed, length(routes), determinism_every)
        if verify_determinism || sample_determinism
            determinismchecks += 1
            repeated = _classify_mutation(bytes, lane)
            if !_same_mutation_outcome(outcome, repeated)
                details =
                    "mutation outcome changed between identical reads\n" *
                    "first=$(_mutation_outcome_text(outcome))\n" *
                    "second=$(_mutation_outcome_text(repeated))"
                if reproenv !== nothing
                    artifact = _repropath(
                        reproduction_dir,
                        :mutation,
                        index,
                        derived,
                        "arrowbytes",
                    )
                    _save_mutation_repro(
                        reproduction_dir,
                        index,
                        derived,
                        bytes,
                        _mutation_repro_text(
                            seed,
                            derived,
                            index,
                            entry,
                            lane,
                            recipe,
                            artifact,
                            details,
                            reproenv,
                        ),
                    )
                end
                _clear_active_repro(reproduction_dir, :mutation)
                throw(FuzzFailure(:mutation, index, seed, derived, details))
            end
        end
        _clear_active_repro(reproduction_dir, :mutation)
        counts[outcome.category] += 1
        routekey = (entry.label, lane)
        routecounts[routekey] = get(routecounts, routekey, 0) + 1
        for operation in operations
            operationcounts[operation] = get(operationcounts, operation, 0) + 1
        end
        if progress_io !== nothing &&
           (completed == total || completed % progress_every == 0)
            println(progress_io, "mutations: $completed/$total")
        end
    end
    mutation_index === nothing && _required_mutation_sweep_coverage!(
        Int(mutations),
        routes,
        routecounts,
        operationcounts,
    )
    return MutationSummary(
        seed,
        total,
        counts,
        routecounts,
        operationcounts,
        determinismchecks,
    )
end

const _FNV_OFFSET = UInt64(0xcbf2_9ce4_8422_2325)
const _FNV_PRIME = UInt64(0x0000_0100_0000_01b3)

@inline _fnvbyte(hash::UInt64, byte::UInt8) = xor(hash, UInt64(byte)) * _FNV_PRIME
function _fnvuint(hash::UInt64, value::UInt64)
    for shift = 0:8:56
        hash = _fnvbyte(hash, UInt8((value >> shift) & 0xff))
    end
    return hash
end
function _fnvstring(hash::UInt64, value::AbstractString)
    hash = _fnvuint(hash, UInt64(ncodeunits(value)))
    for byte in codeunits(value)
        hash = _fnvbyte(hash, byte)
    end
    return hash
end

_fingerprintvalue(hash::UInt64, ::Missing) = _fnvbyte(hash, 0x00)
function _fingerprintvalue(hash::UInt64, value::Bool)
    return _fnvbyte(_fnvbyte(hash, 0x01), UInt8(value))
end
function _fingerprintvalue(hash::UInt64, value::Integer)
    hash = _fnvbyte(hash, 0x02)
    hash = _fnvstring(hash, string(typeof(value)))
    return _fnvuint(hash, UInt64(unsigned(value)))
end
function _fingerprintvalue(hash::UInt64, value::Float64)
    return _fnvuint(_fnvbyte(hash, 0x03), reinterpret(UInt64, value))
end
function _fingerprintvalue(hash::UInt64, value::AbstractString)
    return _fnvstring(_fnvbyte(hash, 0x04), value)
end
function _fingerprintvalue(hash::UInt64, value::Date)
    bits = reinterpret(UInt64, Int64(Dates.value(value)))
    return _fnvuint(_fnvbyte(hash, 0x05), bits)
end
function _fingerprintvalue(hash::UInt64, value::NamedTuple)
    hash = _fnvbyte(hash, 0x06)
    hash = _fnvuint(hash, UInt64(length(value)))
    for (name, fieldvalue) in pairs(value)
        hash = _fnvstring(hash, String(name))
        hash = _fingerprintvalue(hash, fieldvalue)
    end
    return hash
end
function _fingerprintvalue(hash::UInt64, value::AbstractVector)
    hash = _fnvbyte(hash, 0x07)
    hash = _fnvuint(hash, UInt64(length(value)))
    for item in value
        hash = _fingerprintvalue(hash, item)
    end
    return hash
end

"A stable semantic fingerprint of one generated case, independent of IPC bytes."
function case_fingerprint(master::UInt64, index::Integer)
    case = make_case(master, index)
    return _fingerprintvalue(_FNV_OFFSET, case.expectedparts)
end

function replay_mutation(bytes::Vector{UInt8}, lane::Symbol)
    lane in (:stream_full, :file_full, :file_ranged, :auto_full, :stats_filtered) ||
        throw(ArgumentError("unknown mutation lane $lane"))
    outcome = _classify_mutation(bytes, lane)
    repeated = _classify_mutation(bytes, lane)
    if !_same_mutation_outcome(outcome, repeated)
        return (
            category=:unexpected,
            details="mutation outcome changed between identical saved-byte reads\n" *
                    "first=$(_mutation_outcome_text(outcome))\n" *
                    "second=$(_mutation_outcome_text(repeated))",
        )
    end
    details =
        outcome.error === nothing ? nothing : _failuretext(outcome.error, outcome.backtrace)
    return (; category=outcome.category, details)
end

function run_suite(;
    seed::UInt64=DEFAULT_SEED,
    cases::Integer=EXTENDED_CASES,
    mutations::Integer=EXTENDED_MUTATIONS,
    case_index::Union{Nothing,Integer}=nothing,
    mutation_index::Union{Nothing,Integer}=nothing,
    include_layouts::Bool=true,
    reproduction_dir::Union{Nothing,AbstractString}=nothing,
    verify_determinism::Bool=false,
    determinism_every::Integer=0,
    progress_io::Union{Nothing,IO}=nothing,
)
    repro_environment = reproduction_dir === nothing ? nothing : _environmenttext()
    restore = _snapshot_environment(reproduction_dir)
    repro_environment === nothing || (repro_environment *= "\n$restore")
    differential = run_differential(;
        seed,
        cases,
        case_index,
        include_layouts,
        reproduction_dir,
        repro_environment,
        progress_io,
    )
    mutation = run_mutations(;
        seed,
        mutations,
        mutation_index,
        reproduction_dir,
        repro_environment,
        verify_determinism,
        determinism_every,
        progress_io,
    )
    return FuzzSummary(differential, mutation)
end

run_pr_suite(; seed::UInt64=DEFAULT_SEED) = run_suite(;
    seed,
    cases=PR_CASES,
    mutations=PR_MUTATIONS,
    include_layouts=true,
    verify_determinism=true,
)

end # module SeededFuzz
