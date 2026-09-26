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
#     julia conformance/run.jl corpus      # in the conformance image
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
    goldipcskipreason,
    readjson,
    streamdocument

function runfamily(dir::String, family::String, verdicts::Vector{Verdict})
    why = familyskipreason(family, basename(dir))
    if !isempty(why)
        push!(verdicts, Verdict(family, "all", :skip, why))
        return
    end

    gold = readjson(joinpath(dir, family * ".json.gz"))

    # 1. JSON -> Core -> JSON
    check = "json→core→json"
    push!(verdicts, documentcheck(family, check, gold) do
        sch, batches, dictids = ArrowJSON.fromjson(gold)
        ArrowJSON.tojson(sch, batches; dictids=dictids)
    end)

    # 2. gold stream -> JSON ; 3. gold file -> JSON
    for (check, path, reader) in (
        ("gold stream→json", joinpath(dir, family * ".stream"), streamdocument),
        ("gold file→json", joinpath(dir, family * ".arrow_file"), filedocument),
    )
        why = goldipcskipreason(family, basename(dir))
        if !isempty(why)
            push!(verdicts, Verdict(family, check, :skip, why))
            continue
        end
        if !isfile(path)
            push!(verdicts, Verdict(family, check, :skip, "no gold file"))
            continue
        end
        push!(verdicts, documentcheck(family, check, gold) do
            reader(read(path))
        end)
    end

    # 4. JSON -> our IPC (stream + file) -> our reader -> JSON vs gold
    for (check, writer, reader) in (
        (
            "json→our stream→json",
            (s, b, ids) -> Arrow.writestream(s, b; dictids=ids),
            streamdocument,
        ),
        (
            "json→our file→json",
            (s, b, ids) -> Arrow.writefile(s, b; dictids=ids),
            filedocument,
        ),
    )
        push!(verdicts, documentcheck(family, check, gold) do
            sch, batches, dictids = ArrowJSON.fromjson(gold)
            reader(writer(sch, batches, dictids))
        end)
    end
    return
end

function runcorpus(corpus::String=DEFAULT_CORPUS; versions=nothing)
    isempty(corpus) && error(
        "ARROW_TESTING_DIR is not set: run this suite " *
        "through `julia conformance/run.jl corpus`",
    )
    root = joinpath(corpus, "data", "arrow-ipc-stream", "integration")
    isdir(root) || error("corpus not found at $root (set ARROW_TESTING_DIR)")
    verdicts = Verdict[]
    vdirs =
        versions === nothing ? filter(d -> isdir(joinpath(root, d)), readdir(root)) :
        versions
    for v in sort(vdirs)
        dir = joinpath(root, v)
        families = sort!(
            unique!([
                replace(f, r"\.json\.gz$" => "") for
                f in readdir(dir) if endswith(f, ".json.gz")
            ]),
        )
        for fam in families
            before = length(verdicts)
            runfamily(dir, fam, verdicts)
            for i = (before + 1):length(verdicts)
                vd = verdicts[i]
                verdicts[i] = Verdict(v * "/" * vd.family, vd.check, vd.status, vd.detail)
            end
        end
    end
    (isempty(verdicts) || all(v -> v.status == :skip, verdicts)) &&
        error("arrow-testing corpus contains no runnable cases under $root")
    return verdicts
end

report(verdicts::Vector{Verdict}; io=stdout) =
    ConformanceSupport.report("arrow-testing corpus", verdicts; io=io)

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
