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
# Oracle round-trips: OUR IPC bytes through pyarrow and nanoarrow.
#
#     julia conformance/run.jl oracle      # in the conformance image
#
# The gold corpus proves us against files C++ wrote years ago; this suite
# proves us against implementations running today. The corpus supplies the
# data matrix (every layout the format defines), the conformance image
# (conformance/Dockerfile) supplies the oracles — a Python with pyarrow and
# nanoarrow named by ARROW_ORACLE_PYTHON (ORACLE_WORKDIR keeps the generated
# cases and oracle output in a fixed directory instead of a temp dir) — and
# for every gold family we run:
#
#   ours→pyarrow stream   parse the gold JSON into Core, write OUR stream
#                         bytes; pyarrow reads them, structurally validates,
#                         rewrites its own stream; OUR reader reads that
#                         back and the values must equal the gold JSON.
#                         Proves pyarrow accepts our bytes and we accept
#                         pyarrow's, value-losslessly.
#   ours→pyarrow file     the same through the file format (footer path).
#   ours→nanoarrow stream nanoarrow's IPC reader consumes our stream and
#                         hands the arrays over the C-stream capsule;
#                         nanoarrow's writer (or pyarrow's, on releases
#                         without one) produces the return stream. Proves
#                         the nanoarrow reader accepts our bytes.
#   +lz4 / +zstd          compressed-body variants of primitive,
#                         nested-dictionary, and binary-view families,
#                         proving our compressed framing against C++.
#   +stats                 one primitive file with Arrow.jl statistics schema
#                         metadata; pyarrow must accept it and preserve values.
#
# Comparison is the corpus's own value-level document comparison, so id
# reassignment, pool unification, and padding differences by the oracle
# writers are already normalized away.
# =============================================================================

using JSON
import Arrow

if !isdefined(@__MODULE__, :ConformanceSupport)
    include(joinpath(@__DIR__, "ConformanceSupport.jl"))
end
using .ConformanceSupport:
    ArrowJSON,
    DEFAULT_CORPUS,
    Verdict,
    documentcheck,
    familyskipreason,
    filedocument,
    readjson,
    streamdocument

# The oracle interpreter: a Python with pyarrow (and nanoarrow) importable.
# The conformance image sets it; on a host, point it at any such interpreter.
function _oraclepython()
    py = get(ENV, "ARROW_ORACLE_PYTHON", "")
    isempty(py) && error(
        "ARROW_ORACLE_PYTHON is not set: run this suite through " *
        "`julia conformance/run.jl oracle` (the conformance " *
        "image), or point ARROW_ORACLE_PYTHON at a Python with pyarrow and nanoarrow",
    )
    return py
end

# The in-container driver. One process over all cases: reads each of our
# streams/files, structurally validates them, and writes the return bytes plus a
# results.json of per-case statuses (an oracle refusing our bytes is a
# finding, not a crash).
const PYDRIVER = raw"""
import json, os, sys
import pyarrow as pa
import pyarrow.ipc as ipc
import nanoarrow as na
import nanoarrow.ipc as naipc

def na_stream(path):
    # nanoarrow's IPC entry point moved across releases; probe.
    if hasattr(na, "ArrayStream") and hasattr(na.ArrayStream, "from_path"):
        return na.ArrayStream.from_path(path)
    return na.ArrayStream(naipc.InputStream.from_path(path))

def classify(e):
    # Raw error text only — the Julia side decides skip-vs-fail against an
    # explicit whitelist of known oracle capability gaps, so a NEW
    # interoperability failure can never classify itself into a skip.
    return f"{type(e).__name__}: {e}"[:200]

def rewrite(batches, schema, open_sink):
    # Per-batch rewrite: read_all()/write_table merges chunks and drops
    # zero-length batches, which breaks batch-boundary comparison against
    # the gold JSON. Structural validate only: full validation enforces the
    # advisory contracts (date64 divisibility, decimal precision) that the
    # gold corpus itself violates and default C++ reads accept.
    for b in batches:
        b.validate()
    with open_sink(schema) as w:
        for b in batches:
            w.write_batch(b)

work = sys.argv[1]
cases = json.load(open(os.path.join(work, "cases.json")))
outdir = os.path.join(work, "out")
os.makedirs(outdir, exist_ok=True)
results = {"pyarrow": pa.__version__,
           "nanoarrow": na.__version__,
           "cases": {}}
for case in cases:
    name = case["name"]
    r = {}
    spath = os.path.join(work, "cases", name + ".stream")
    fpath = os.path.join(work, "cases", name + ".arrow")
    try:
        reader = ipc.open_stream(spath)
        rewrite(list(reader), reader.schema, lambda s: ipc.new_stream(
            os.path.join(outdir, name + ".pyarrow.stream"), s))
        r["pyarrow_stream"] = "ok"
    except Exception as e:
        r["pyarrow_stream"] = classify(e)
    try:
        f = ipc.open_file(fpath)
        rewrite([f.get_batch(i) for i in range(f.num_record_batches)],
                f.schema, lambda s: ipc.new_file(
                    os.path.join(outdir, name + ".pyarrow.arrow"), s))
        r["pyarrow_file"] = "ok"
    except Exception as e:
        r["pyarrow_file"] = classify(e)
    try:
        reader = pa.RecordBatchReader.from_stream(na_stream(spath))
        batches = list(reader)   # nanoarrow reads OUR bytes
        outpath = os.path.join(outdir, name + ".nanoarrow.stream")
        try:    # prefer nanoarrow's own writer for the return trip
            back = pa.RecordBatchReader.from_batches(reader.schema, batches)
            with naipc.StreamWriter.from_path(outpath) as w:
                w.write_stream(na.c_array_stream(back))
        except Exception:
            rewrite(batches, reader.schema,
                    lambda s: ipc.new_stream(outpath, s))
        r["nanoarrow_stream"] = "ok"
    except Exception as e:
        r["nanoarrow_stream"] = classify(e)
    results["cases"][name] = r
json.dump(results, open(os.path.join(work, "results.json"), "w"))
print(f"driver: {len(cases)} cases")
"""

struct OracleCase
    name::String        # <versiondir>__<family>[+codec], also the file stem
    label::String       # <versiondir>/<family>[+codec], for the report
    goldpath::String
end

# These families cover primitive buffers, dictionary batches with nested child
# buffers, and variadic view buffers without multiplying every corpus case.
const COMPRESSED_ORACLE_FAMILIES =
    ("generated_primitive", "generated_nested_dictionary", "generated_binary_view")
const STATISTICS_ORACLE_FAMILY = "generated_primitive"
const TARGETED_ORACLE_VARIANTS = (
    (
        (family, codec) for family in COMPRESSED_ORACLE_FAMILIES for codec in (:lz4, :zstd)
    )...,
    (STATISTICS_ORACLE_FAMILY, :stats),
)

function _requiretargetedoracles(cases)
    missing = Tuple{String,Symbol}[]
    for (family, variant) in TARGETED_ORACLE_VARIANTS
        suffix = "__" * family * "+" * String(variant)
        any(case -> endswith(case.name, suffix), cases) || push!(missing, (family, variant))
    end
    isempty(missing) ||
        error("pinned corpus did not prepare targeted oracle cases: $missing")
    return nothing
end

"""
Write OUR stream + file bytes for every non-skipped gold family (plus targeted
compressed families) into workdir/cases, and the case manifest the python
driver walks.
"""
function preparecases(corpus::String, workdir::String; require_targeted::Bool=false)
    root = joinpath(corpus, "data", "arrow-ipc-stream", "integration")
    isdir(root) || error("corpus not found at $root (set ARROW_TESTING_DIR)")
    casedir = joinpath(workdir, "cases")
    mkpath(casedir)
    cases = OracleCase[]
    skips = Tuple{String,String}[]
    for v in sort(filter(d -> isdir(joinpath(root, d)), readdir(root)))
        dir = joinpath(root, v)
        families = sort!(
            unique!([
                replace(f, r"\.json\.gz$" => "") for
                f in readdir(dir) if endswith(f, ".json.gz")
            ]),
        )
        for fam in families
            why = familyskipreason(fam, v)
            isempty(why) || (push!(skips, (v * "/" * fam, why)); continue)
            goldpath = joinpath(dir, fam * ".json.gz")
            sch, batches, dictids = ArrowJSON.fromjson(readjson(goldpath))
            variants = fam in COMPRESSED_ORACLE_FAMILIES ? (:none, :lz4, :zstd) : (:none,)
            for compress in variants
                suffix = compress == :none ? "" : "+" * String(compress)
                name = v * "__" * fam * suffix
                write(
                    joinpath(casedir, name * ".stream"),
                    Arrow.writestream(sch, batches; compress=compress, dictids=dictids),
                )
                write(
                    joinpath(casedir, name * ".arrow"),
                    Arrow.writefile(sch, batches; compress=compress, dictids=dictids),
                )
                push!(cases, OracleCase(name, v * "/" * fam * suffix, goldpath))
            end
            if fam == STATISTICS_ORACLE_FAMILY
                name = v * "__" * fam * "+stats"
                write(
                    joinpath(casedir, name * ".stream"),
                    Arrow.writestream(sch, batches; dictids),
                )
                write(joinpath(casedir, name * ".arrow"), Arrow.statsfile(sch, batches))
                push!(cases, OracleCase(name, v * "/" * fam * "+stats", goldpath))
            end
        end
    end
    require_targeted && _requiretargetedoracles(cases)
    open(joinpath(workdir, "cases.json"), "w") do io
        JSON.print(io, [Dict("name" => c.name) for c in cases])
    end
    isempty(cases) &&
        error("arrow-testing corpus contains no runnable oracle cases under $root")
    return cases, skips
end

"""
Run the python driver over workdir with the oracle interpreter and return
the parsed results.json (an oracle refusing our bytes is a finding, not a
crash: the driver records per-case statuses and exits 0).
"""
function runoracles(workdir::String)
    driver = joinpath(workdir, "driver.py")
    write(driver, PYDRIVER)
    py = _oraclepython()
    Base.run(`$py $driver $workdir`)
    return JSON.parsefile(joinpath(workdir, "results.json"))
end

const ORACLE_CHECKS = (
    ("ours→pyarrow stream", "pyarrow_stream", ".pyarrow.stream", streamdocument),
    ("ours→pyarrow file", "pyarrow_file", ".pyarrow.arrow", filedocument),
    ("ours→nanoarrow stream", "nanoarrow_stream", ".nanoarrow.stream", streamdocument),
)

# The ONLY oracle errors this suite treats as skips: known capability gaps,
# whitelisted by check, case, and error text. Anything else — including a
# feature error on a case not listed here — is a failure to investigate.
const ORACLE_EXPECTED_GAPS = (
    (
        "nanoarrow_stream",
        n -> endswith(n, "+lz4") || endswith(n, "+zstd"),
        "unsupported feature COMPRESSED_BODY",
    ),
    (
        "nanoarrow_stream",
        n -> occursin("generated_binary_view", n),
        "BinaryView not yet supported",
    ),
    (
        "nanoarrow_stream",
        n -> occursin("generated_list_view", n),
        "ListView/LargeListView not yet supported",
    ),
    (
        "nanoarrow_stream",
        n -> occursin("generated_run_end_encoded", n),
        "RunEndEncoded not yet supported",
    ),
)

_expectedgap(key::String, name::String, status::String) = any(
    k == key && pred(name) && occursin(text, status) for
    (k, pred, text) in ORACLE_EXPECTED_GAPS
)

function runoracle(
    corpus::String=DEFAULT_CORPUS;
    workdir::String=get(ENV, "ORACLE_WORKDIR", mktempdir(prefix="arrow-oracle-")),
)
    cases, skips = preparecases(corpus, workdir; require_targeted=true)
    println("oracle: ", length(cases), " cases prepared in ", workdir)
    results = runoracles(workdir)
    return compareresults(cases, skips, results, workdir), results
end

"Remove exactly Arrow.jl's statistics key from one decoded IPC document."
function _withoutstatsmetadata(document)
    stripped = deepcopy(document)
    schema = get(stripped, "schema", nothing)
    schema isa AbstractDict || error("statistics oracle document has no schema object")
    metadata = get(schema, "metadata", nothing)
    metadata isa AbstractVector ||
        error("statistics oracle document has no schema metadata")
    matches = count(
        entry ->
            entry isa AbstractDict && get(entry, "key", nothing) == Arrow.STATS_KEY,
        metadata,
    )
    matches == 1 || error("statistics oracle document has $matches statistics keys")
    kept = [
        entry for entry in metadata if
        !(entry isa AbstractDict && get(entry, "key", nothing) == Arrow.STATS_KEY)
    ]
    isempty(kept) ? delete!(schema, "metadata") : (schema["metadata"] = kept)
    return stripped
end

"""
Judge the oracle outputs: for every case the driver reported "ok", read the
return bytes with OUR reader and compare values against the gold JSON.
"""
function compareresults(cases::Vector{OracleCase}, skips, results, workdir::String)
    verdicts = Verdict[]
    for (label, why) in skips
        push!(verdicts, Verdict(label, "all", :skip, why))
    end
    for case in cases
        gold = readjson(case.goldpath)
        statsinput = if endswith(case.name, "+stats")
            filedocument(read(joinpath(workdir, "cases", case.name * ".arrow")))
        end
        if statsinput !== nothing
            push!(verdicts, documentcheck(case.label, "ours stats file→gold", gold) do
                _withoutstatsmetadata(statsinput)
            end)
        end
        r = get(results["cases"], case.name, Dict{String,Any}())
        for (check, key, suffix, reader) in ORACLE_CHECKS
            status = get(r, key, "driver produced no result")
            if status != "ok"
                # Capability errors skip only through the explicit whitelist.
                # A missing required oracle or result is a conformance failure.
                kind = _expectedgap(key, case.name, status) ? :skip : :fail
                push!(verdicts, Verdict(case.label, check, kind, status))
                continue
            end
            # The targeted file route adds the statistics key that the gold
            # JSON does not contain. Compare PyArrow's return against our
            # exact input document there, proving both values and unknown
            # schema metadata survive. Its ordinary stream routes still use
            # the metadata-free gold document.
            expected =
                key == "pyarrow_file" && endswith(case.name, "+stats") ? statsinput : gold
            push!(verdicts, documentcheck(case.label, check, expected) do
                reader(read(joinpath(workdir, "out", case.name * suffix)))
            end)
        end
    end
    return verdicts
end

function oraclereport(verdicts::Vector{Verdict}, results; io=stdout)
    header =
        "oracle round-trips (pyarrow $(get(results, "pyarrow", "?")), " *
        "nanoarrow $(get(results, "nanoarrow", "?")))"
    return ConformanceSupport.report(header, verdicts; io=io)
end

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    corpus = isempty(ARGS) ? DEFAULT_CORPUS : ARGS[1]
    verdicts, results = runoracle(corpus)
    nfail = oraclereport(verdicts, results)
    exit(nfail == 0 ? 0 : 1)
end
