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
# Corpus conformance: the apache/arrow-testing integration gold files.
#
#     julia --project=conformance conformance/corpus.jl [corpus-dir]
#
# For every gold family (a `.json.gz` with sibling `.stream` and
# `.arrow_file`), run the four checks that make up cross-implementation
# conformance and report a per-file verdict:
#
#   JSON→Core→JSON   parse the gold JSON into Core, render it back, compare
#                    documents (proves the JSON mapping is lossless);
#   gold stream→JSON read the gold .stream through our IPC reader, render as
#                    JSON, compare to the gold JSON (proves READ conformance);
#   gold file→JSON   same through the file reader (footer path);
#   JSON→our IPC→gold values
#                    write the gold JSON's Core values with OUR writer (stream
#                    + file), read back through OUR reader, compare values to
#                    the gold JSON (proves WRITE round-trip). Byte-identity
#                    with the gold IPC is NOT required — writers legitimately
#                    differ in padding, dictionary ordering, and metadata.
#
# Comparison is value-level over the JSON documents (schema, dictionaries,
# batches) with numeric normalization (floats compared EXACTLY after
# half/single columns are canonicalized through their physical precision;
# 64-bit strings vs numbers unified). Skips are explicit and categorized so
# the report reads as coverage, not silence.
# =============================================================================

using JSON, CodecZlib
using Arrow
# The corpus exercises package internals (adapter entry points, Core
# accessors, metadata types); alias the namespace wholesale, as the test
# batteries do, until the facade formalizes a public surface.
for n in names(Arrow; all=true)
    sn = String(n)
    (startswith(sn, "#") || n in (:eval, :include, :Arrow, :write, :Table, :Stream)) && continue
    isdefined(Arrow, n) || continue
    @eval const $n = Arrow.$n
end
include(joinpath(@__DIR__, "arrowjson.jl"))
using .ArrowJSON

const DEFAULT_CORPUS = get(ENV, "ARROW_TESTING_DIR",
    joinpath(homedir(), ".julia", "dev", "arrow-testing"))

# Families declared out of scope, with the reason. Everything
# else must pass or it is a failure.
const SKIP = Dict{String,String}(
    "1.0.0-bigendian" => "big-endian streams are not supported (no endianness normalization)",
    "0.14.1" => "pre-1.0 legacy framing (four-byte prefix) is not accepted by design",
    "0.17.1" => "V4 experimental compression marker era; superseded by 2.0.0-compression",
    "generated_decimal256" => "decimal256 (Int256 storage) is not implemented",
    "generated_extension" => "extension types round-trip as their storage type + metadata; value equality holds but this runner treats the family as informational",
)

# --- value-level comparison -----------------------------------------------------

_num(x) = x isa AbstractString ? (tryparse(Int128, x) === nothing ? x : parse(Int128, x)) :
    x isa Integer ? Int128(x) : x

function _eq(a, b, path::String, diffs::Vector{String})
    if a isa AbstractDict && b isa AbstractDict
        ka, kb = Set(keys(a)), Set(keys(b))
        # writers may omit empty/absent optional keys
        for k in union(ka, kb)
            va, vb = get(a, k, nothing), get(b, k, nothing)
            (va === nothing || va == Any[] || va == false) && (vb === nothing || vb == Any[] || vb == false) && continue
            _eq(va, vb, path * "." * String(k), diffs)
        end
    elseif a isa AbstractVector && b isa AbstractVector
        length(a) == length(b) || (push!(diffs, "$path: length $(length(a)) vs $(length(b))"); return)
        for (i, (x, y)) in enumerate(zip(a, b))
            _eq(x, y, path * "[$i]", diffs)
            length(diffs) > 20 && return
        end
    elseif a isa AbstractFloat || b isa AbstractFloat
        # EXACT equality (± zero unified, NaN equal): a tolerance here would
        # bless changed values. Sub-double columns are canonicalized to
        # their physical precision by _normalize! first, which is what makes
        # exact comparison correct across writers' decimal choices.
        fa, fb = Float64(_num(a)), Float64(_num(b))
        (isnan(fa) && isnan(fb)) || fa == fb ||
            push!(diffs, "$path: $a vs $b")
    elseif a isa Bool || b isa Bool
        Bool(a) == Bool(b) || push!(diffs, "$path: $a vs $b")
    else
        na, nb = _num(a), _num(b)
        na == nb || push!(diffs, "$path: $(repr(a)) vs $(repr(b))")
    end
    return
end

# The dictionaries section's pool COLUMN name is a placeholder in the JSON
# format (gold files write "DICT0"; other writers use the field name), and
# metadata key/value lists are unordered. Normalize both before comparing.
function _normalize!(doc::AbstractDict)
    # Dictionary ids and pool sharing are adapter bookkeeping, and the gold
    # corpus itself is not id-stable across its own representations:
    # generated_nested_dictionary's JSON shares one pool between two fields
    # (3 ids) while its gold stream and file carry one pool per field (5 ids).
    # Canonicalize both documents to one pool entry per dictionary-typed
    # field position, ids assigned in depth-first schema order.
    pools = Dict{Int64,Any}(Int64(d["id"]) => d["data"]
        for d in get(doc, "dictionaries", Any[]))
    newdicts = Any[]
    function renumber!(f)
        f isa AbstractDict || return
        if get(f, "dictionary", nothing) isa AbstractDict
            d = f["dictionary"]
            oldid = Int64(d["id"])
            newid = length(newdicts)
            d["id"] = newid
            push!(newdicts, Dict{String,Any}("id" => newid,
                "data" => deepcopy(pools[oldid])))
        end
        foreach(renumber!, get(f, "children", Any[]))
    end
    foreach(renumber!, get(get(doc, "schema", Dict()), "fields", Any[]))
    if isempty(newdicts)
        delete!(doc, "dictionaries")
    else
        doc["dictionaries"] = newdicts
    end
    for d in get(doc, "dictionaries", Any[])
        for c in d["data"]["columns"]
            c["name"] = "DICT"
        end
    end
    # Half/single float values parsed from another writer's shortest-repr
    # decimals do not lift to the same Float64s ours do; canonicalize every
    # sub-double column through its physical precision so the comparison can
    # be EXACT for all floats.
    canonfloat(precision, v) = !(v isa Real) ? v :
        precision == "HALF" ? Float64(Float16(Float64(v))) :
        precision == "SINGLE" ? Float64(Float32(Float64(v))) : Float64(v)
    function normfloatcols!(f, col)
        (f isa AbstractDict && col isa AbstractDict) || return
        # A dictionary field's batch column carries integer INDICES; its
        # float values live in the dictionaries section, paired below.
        haskey(f, "dictionary") && return
        t = get(f, "type", Dict())
        if get(t, "name", "") == "floatingpoint" && haskey(col, "DATA")
            p = get(t, "precision", "DOUBLE")
            col["DATA"] = Any[canonfloat(p, v) for v in col["DATA"]]
        end
        for (x, y) in zip(get(f, "children", Any[]), get(col, "children", Any[]))
            normfloatcols!(x, y)
        end
    end
    fields = get(get(doc, "schema", Dict()), "fields", Any[])
    for b in get(doc, "batches", Any[])
        for (f, c) in zip(fields, get(b, "columns", Any[]))
            normfloatcols!(f, c)
        end
    end
    # Pools pair with dictionary fields in the same depth-first order
    # `renumber!` rebuilt the dictionaries array in.
    pools = get(doc, "dictionaries", Any[])
    poolindex = Ref(0)
    function normfloatpools!(f)
        f isa AbstractDict || return
        if get(f, "dictionary", nothing) isa AbstractDict
            poolindex[] += 1
            valuefield = Dict{String,Any}("type" => get(f, "type", Dict()),
                "children" => get(f, "children", Any[]))
            for pc in pools[poolindex[]]["data"]["columns"]
                normfloatcols!(valuefield, pc)
            end
        end
        foreach(normfloatpools!, get(f, "children", Any[]))
    end
    foreach(normfloatpools!, fields)
    # Map entries-struct names are NOT round-trip stable in the corpus itself:
    # generated_map_non_canonical's gold .stream carries `entries` while its
    # gold .arrow_file and .json carry `some_entries` (the C++ stream writer
    # canonicalizes). Compare map children structurally, by position.
    function normmapnames!(fields)
        for f in fields
            f isa AbstractDict || continue
            if get(get(f, "type", Dict()), "name", "") == "map" && haskey(f, "children")
                for c in f["children"]
                    c["name"] = "entries"
                    for (i, kv) in enumerate(get(c, "children", Any[]))
                        kv["name"] = i == 1 ? "key" : "value"
                    end
                end
            end
            haskey(f, "children") && normmapnames!(f["children"])
        end
    end
    normmapnames!(get(get(doc, "schema", Dict()), "fields", Any[]))
    function normmapcols!(cols, fields)
        for (c, f) in zip(cols, fields)
            (c isa AbstractDict && f isa AbstractDict) || continue
            if get(get(f, "type", Dict()), "name", "") == "map" && haskey(c, "children")
                for cc in c["children"]
                    cc["name"] = "entries"
                    for (i, kv) in enumerate(get(cc, "children", Any[]))
                        kv["name"] = i == 1 ? "key" : "value"
                    end
                end
            end
            haskey(c, "children") && haskey(f, "children") &&
                normmapcols!(c["children"], f["children"])
        end
    end
    fields = get(get(doc, "schema", Dict()), "fields", Any[])
    for b in get(doc, "batches", Any[])
        normmapcols!(b["columns"], fields)
    end
    function normmeta!(x)
        if x isa AbstractDict
            if haskey(x, "metadata") && x["metadata"] isa AbstractVector
                x["metadata"] = sort(x["metadata"]; by=kv -> (String(kv["key"]), String(kv["value"])))
            end
            foreach(normmeta!, values(x))
        elseif x isa AbstractVector
            foreach(normmeta!, x)
        end
    end
    normmeta!(doc)
    # Decimal bitWidth is optional-with-default (128) in the JSON format; our
    # renderer always writes it, older gold files omit it.
    function normdecimal!(x)
        if x isa AbstractDict
            if get(x, "name", "") == "decimal" && haskey(x, "precision")
                get(x, "bitWidth", 128) == 128 && delete!(x, "bitWidth")
            end
            foreach(normdecimal!, values(x))
        elseif x isa AbstractVector
            foreach(normdecimal!, x)
        end
    end
    normdecimal!(doc)
    return doc
end

function docsequal(a, b)
    diffs = String[]
    _eq(_normalize!(a), _normalize!(b), "", diffs)
    return diffs
end

# Gold JSON carries physical DATA under null slots that our writer does not
# preserve (we materialize logically). Mask both sides' DATA where VALIDITY
# is 0 before comparing, recursively — the spec makes those bytes
# unspecified, so this is the conformance-correct comparison.
function masknulls!(col::AbstractDict)
    # Rebuild as Vector{Any}: our rendered docs carry typed vectors that
    # cannot hold `nothing`, and mutating them in place would also alias
    # into Core buffers on some paths.
    if haskey(col, "VALIDITY") && haskey(col, "DATA") && col["DATA"] isa AbstractVector
        v = col["VALIDITY"]
        d = col["DATA"]
        col["DATA"] = Any[(i <= length(v) && v[i] == 0) ? nothing : d[i] for i in eachindex(d)]
    end
    if haskey(col, "VALIDITY") && haskey(col, "VIEWS")
        v = col["VALIDITY"]
        vs = col["VIEWS"]
        col["VIEWS"] = Any[(i <= length(v) && v[i] == 0) ? nothing : vs[i] for i in eachindex(vs)]
    end
    for c in get(col, "children", Any[])
        masknulls!(c)
    end
    return col
end
function masknulls!(doc::AbstractDict, ::Val{:doc})
    for b in get(doc, "batches", Any[]), c in b["columns"]
        masknulls!(c)
    end
    for d in get(doc, "dictionaries", Any[]), c in d["data"]["columns"]
        masknulls!(c)
    end
    return doc
end

# --- runner ---------------------------------------------------------------------------

struct Verdict
    family::String
    check::String
    status::Symbol      # :pass, :fail, :skip
    detail::String
end

function _readjson(path)
    bytes = read(path)
    endswith(path, ".gz") && (bytes = transcode(GzipDecompressor, bytes))
    return JSON.parse(String(bytes))
end

function _stream_to_json(bytes::Vector{UInt8})
    s = readstream(bytes)
    # Render with the reader's id table so shared and nested pool ids survive
    # the round-trip instead of being re-assigned one per field.
    return ArrowJSON.tojson(s.schema, s.batches; dictids=s.fielddictids)
end

function _file_to_json(bytes::Vector{UInt8})
    f = readfile(bytes)
    batches = AC.RecordBatch[f[i] for i = 1:length(f)]
    return ArrowJSON.tojson(f.schema, batches; dictids=f.fielddictids)
end

function runfamily(dir::String, family::String, verdicts::Vector{Verdict})
    for (k, why) in SKIP
        (k == family || k == basename(dir)) &&
            (push!(verdicts, Verdict(family, "all", :skip, why)); return)
    end
    jsonpath = joinpath(dir, family * ".json.gz")
    gold = _readjson(jsonpath)
    goldmasked = masknulls!(deepcopy(gold), Val(:doc))
    # 1. JSON -> Core -> JSON
    check = "json→core→json"
    try
        sch, batches, dictids = ArrowJSON.fromjson(gold)
        back = ArrowJSON.tojson(sch, batches; dictids=dictids)
        diffs = docsequal(masknulls!(deepcopy(back), Val(:doc)), goldmasked)
        push!(verdicts, Verdict(family, check, isempty(diffs) ? :pass : :fail,
            isempty(diffs) ? "" : first(diffs)))
    catch e
        push!(verdicts, Verdict(family, check, :fail, sprint(showerror, e)[1:min(end, 200)]))
    end
    # 2. gold stream -> JSON ; 3. gold file -> JSON
    for (check, path, reader) in (
        ("gold stream→json", joinpath(dir, family * ".stream"), _stream_to_json),
        ("gold file→json", joinpath(dir, family * ".arrow_file"), _file_to_json))
        isfile(path) || (push!(verdicts, Verdict(family, check, :skip, "no gold file")); continue)
        try
            got = reader(read(path))
            diffs = docsequal(masknulls!(deepcopy(got), Val(:doc)), goldmasked)
            push!(verdicts, Verdict(family, check, isempty(diffs) ? :pass : :fail,
                isempty(diffs) ? "" : first(diffs)))
        catch e
            push!(verdicts, Verdict(family, check, :fail, sprint(showerror, e)[1:min(end, 200)]))
        end
    end
    # 4. JSON -> our IPC (stream + file) -> our reader -> JSON vs gold
    for (check, writer, reader) in (
        ("json→our stream→json", (s, b, ids) -> writestream(s, b; dictids=ids), _stream_to_json),
        ("json→our file→json", (s, b, ids) -> writefile(s, b; dictids=ids), _file_to_json))
        try
            sch, batches, dictids = ArrowJSON.fromjson(gold)
            bytes = writer(sch, batches, dictids)
            got = reader(bytes)
            diffs = docsequal(masknulls!(deepcopy(got), Val(:doc)), goldmasked)
            push!(verdicts, Verdict(family, check, isempty(diffs) ? :pass : :fail,
                isempty(diffs) ? "" : first(diffs)))
        catch e
            push!(verdicts, Verdict(family, check, :fail, sprint(showerror, e)[1:min(end, 200)]))
        end
    end
    return
end

function runcorpus(corpus::String=DEFAULT_CORPUS; versions=nothing)
    root = joinpath(corpus, "data", "arrow-ipc-stream", "integration")
    isdir(root) || error("corpus not found at $root (set ARROW_TESTING_DIR)")
    verdicts = Verdict[]
    vdirs = versions === nothing ?
        filter(d -> isdir(joinpath(root, d)), readdir(root)) : versions
    for v in sort(vdirs)
        dir = joinpath(root, v)
        families = sort!(unique!([replace(f, r"\.json\.gz$" => "")
            for f in readdir(dir) if endswith(f, ".json.gz")]))
        for fam in families
            before = length(verdicts)
            runfamily(dir, fam, verdicts)
            for i = (before + 1):length(verdicts)
                vd = verdicts[i]
                verdicts[i] = Verdict(v * "/" * vd.family, vd.check, vd.status, vd.detail)
            end
        end
    end
    return verdicts
end

function report(verdicts::Vector{Verdict}; io=stdout)
    npass = count(v -> v.status == :pass, verdicts)
    nfail = count(v -> v.status == :fail, verdicts)
    nskip = count(v -> v.status == :skip, verdicts)
    println(io, "arrow-testing corpus: $npass pass, $nfail fail, $nskip skip")
    println(io)
    for v in verdicts
        v.status == :pass && continue
        tag = v.status == :fail ? "FAIL" : "skip"
        println(io, rpad(tag, 5), rpad(v.family, 58), rpad(v.check, 24), v.detail)
    end
    return nfail
end

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    corpus = isempty(ARGS) ? DEFAULT_CORPUS : ARGS[1]
    verdicts = runcorpus(corpus)
    nfail = report(verdicts)
    println()
    println("PASS families by check:")
    for check in unique(v.check for v in verdicts if v.check != "all")
        n = count(v -> v.check == check && v.status == :pass, verdicts)
        m = count(v -> v.check == check && v.status != :skip, verdicts)
        println("  ", rpad(check, 24), n, "/", m)
    end
    exit(nfail == 0 ? 0 : 1)
end
