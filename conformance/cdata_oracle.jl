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
# C Data / C Stream oracle: OUR C-interface structures through pyarrow, in
# one process, over the whole gold data matrix.
#
#     julia --project=conformance conformance/cdata_oracle.jl [corpus-dir]
#
# `oracle.jl` proves our IPC BYTES against pyarrow and nanoarrow. This suite
# proves our C DATA INTERFACE and C STREAM INTERFACE the same way: pyarrow
# runs in-process (PythonCall) so real ArrowSchema/ArrowArray/ArrowArrayStream
# pointers cross the boundary in both directions with real ownership moves.
# The gold corpus supplies the data (every layout the format defines), parsed
# to Core through the corpus's JSON reader; comparison is the corpus's own
# value-level document comparison. For every gold family:
#
#   ours→pyarrow(C)→ours   each batch is exported as one struct-typed
#                          ArrowArray + ArrowSchema; pyarrow imports it as a
#                          RecordBatch, FULLY validates it (its independent
#                          judgment of our export), and exports it back; our
#                          importer reads pyarrow's structures. Values must
#                          equal the gold JSON. Proves both directions of the
#                          C Data interface, including field names, nullability
#                          and metadata (schema and field level).
#   pyarrow-native→ours    pyarrow rebuilds the batch through its OWN IPC
#                          reader (its allocator, its buffer choices, its
#                          dictionary memo) and exports that; we import it.
#                          Proves our importer against pyarrow-produced memory,
#                          not just our own memory reflected back.
#   pyarrow slice→ours     pyarrow exports a SLICED batch (nonzero per-node
#                          offsets); we import it and compare against our own
#                          logical slice of the source. Proves offset handling
#                          on both sides.
#   ours→pyarrow(stream)→ours
#                          the whole family as one C stream: pyarrow imports
#                          our ArrowArrayStream as a RecordBatchReader and
#                          re-exports it; we pull batches through pyarrow's
#                          stream (a re-entrant Julia→C→Julia pull path).
#
# The suite ends by draining the export registries: every C structure handed
# to pyarrow must have been released back exactly once.
#
# Python setup is automatic: a venv with pyarrow is created on first run under
# ARROW_CDATA_ORACLE_VENV (default ~/.cache/arrow-julia/cdata-oracle-venv)
# via `uv` when available (else `python3 -m venv` + pip). Point
# ARROW_CDATA_ORACLE_PYTHON at any interpreter that already has pyarrow to
# skip that. The parent process only prepares the environment and re-launches
# this file as a child with PythonCall bound to that interpreter.
# =============================================================================

const _CDATA_ORACLE_CHILD = "--child"

# --- parent: environment preparation + relaunch ------------------------------

function _oracle_python()
    explicit = get(ENV, "ARROW_CDATA_ORACLE_PYTHON", "")
    isempty(explicit) || return explicit
    venv = get(ENV, "ARROW_CDATA_ORACLE_VENV",
        joinpath(homedir(), ".cache", "arrow-julia", "cdata-oracle-venv"))
    py = joinpath(venv, Sys.iswindows() ? "Scripts" : "bin",
        Sys.iswindows() ? "python.exe" : "python")
    if !isfile(py)
        mkpath(dirname(venv))
        uv = Sys.which("uv")
        if uv !== nothing
            run(`$uv venv --python 3.12 $venv`)
        else
            py3 = something(Sys.which("python3"), Sys.which("python"),
                error("no python3 on PATH; set ARROW_CDATA_ORACLE_PYTHON"))
            run(`$py3 -m venv $venv`)
        end
    end
    haspyarrow = success(pipeline(`$py -c "import pyarrow"`;
        stdout=devnull, stderr=devnull))
    if !haspyarrow
        uv = Sys.which("uv")
        if uv !== nothing
            run(`$uv pip install --python $py pyarrow`)
        else
            run(`$py -m pip install pyarrow`)
        end
    end
    return py
end

function _oracle_parent(args)
    py = _oracle_python()
    ver = readchomp(`$py -c "import pyarrow; print(pyarrow.__version__)"`)
    println("cdata oracle: pyarrow $ver at $py")
    env = copy(ENV)
    env["JULIA_PYTHONCALL_EXE"] = py
    env["JULIA_CONDAPKG_BACKEND"] = "Null"
    cmd = `$(Base.julia_cmd()) --project=$(dirname(Base.active_project()))
        --startup-file=no $(@__FILE__) $_CDATA_ORACLE_CHILD $args`
    proc = run(ignorestatus(setenv(cmd, env)))
    exit(proc.exitcode)
end

# --- child: the suite ---------------------------------------------------------

function _oracle_child(args)
    @eval begin
        using PythonCall
        include(joinpath(@__DIR__, "corpus.jl"))
    end
    Base.invokelatest(_run_cdata_oracle, args)
end

function _run_cdata_oracle(args)
    corpus = isempty(args) ? DEFAULT_CORPUS : args[1]
    verdicts = runcdataoracle(corpus)
    nfail = report(verdicts)
    println()
    println("PASS families by check:")
    for check in unique(v.check for v in verdicts if v.check != "all")
        n = count(v -> v.check == check && v.status == :pass, verdicts)
        m = count(v -> v.check == check && v.status != :skip, verdicts)
        println("  ", rpad(check, 28), n, "/", m)
    end
    exit(nfail == 0 ? 0 : 1)
end

# Everything below is only defined in the child (PythonCall + corpus loaded).
if length(ARGS) >= 1 && ARGS[1] == _CDATA_ORACLE_CHILD

using PythonCall
include(joinpath(@__DIR__, "corpus.jl"))

const pa = pyimport("pyarrow")
const paipc = pyimport("pyarrow.ipc")
const CS = Arrow.CArrowSchema
const CA = Arrow.CArrowArray
const CAS = Arrow.CArrowArrayStream

# Families this suite declares out of scope, with the reason. Everything
# else must pass or it is a failure.
const CDATA_SKIP = Dict{String,String}(
    "generated_decimal256" => "decimal256 (Int256 storage) is not implemented",
)

# One RecordBatch is one struct-typed column: the schema-level metadata rides
# the struct Field, the fields are its children, the columns its child arrays.
function _structwrap(sch::AC.Schema, columns, nrows::Integer)
    f = AC.Field("", AC.StructType(); nullable=false, metadata=sch.metadata,
        children=collect(AC.Field, sch.fields))
    d = AC.ArrayData(AC.StructType(), nrows, [AC.BufferSlice()];
        children=collect(AC.ArrayData, columns), nullcount=0)
    return f, d
end

function _unwrap(f2::AC.Field, d2::AC.ArrayData)
    sch2 = AC.Schema(collect(AC.Field, f2.children); metadata=f2.metadata,
        endianness=AC.LittleEndian)
    return sch2, AC.RecordBatch(sch2, collect(AC.ArrayData, d2.children), d2.len)
end

# Export one struct-wrapped batch to pyarrow; pyarrow validates fully.
function _to_pyarrow(sch::AC.Schema, b::AC.RecordBatch)
    f, d = _structwrap(sch, b.columns, b.nrows)
    sp, ap = Arrow.to_c_data(f, d)
    pyb = pa.RecordBatch._import_from_c(UInt(ap), UInt(sp))
    unsafe_load(sp).release == C_NULL ||
        error("pyarrow did not mark the imported schema released")
    unsafe_load(ap).release == C_NULL ||
        error("pyarrow did not mark the imported array released")
    pyb.validate(full=true)
    return pyb
end

# Import a pyarrow RecordBatch through the C interface into Core.
function _from_pyarrow(pyb)
    aref = Ref{CA}()
    sref = Ref{CS}()
    return GC.@preserve aref sref begin
        ap = Base.unsafe_convert(Ptr{CA}, aref)
        sp = Base.unsafe_convert(Ptr{CS}, sref)
        pyb._export_to_c(UInt(ap), UInt(sp))
        f2, d2 = Arrow.from_c_data(sp, ap)
        _unwrap(f2, d2)
    end
end

# pyarrow rebuilds a batch through its own IPC reader: pyarrow-owned memory.
function _pyarrow_rebuild(pyb)
    sink = pa.BufferOutputStream()
    w = paipc.new_stream(sink, pyb.schema)
    w.write_batch(pyb)
    w.close()
    r = paipc.open_stream(sink.getvalue())
    return r.read_next_batch()
end

# Our own logical slice of a batch: every column re-windowed by offset.
function _sliceours(sch::AC.Schema, b::AC.RecordBatch, off::Integer, len::Integer)
    cols = AC.ArrayData[
        AC.ArrayData(c.type, len, collect(AC.BufferSlice, c.buffers);
            offset=c.offset + off, children=collect(AC.ArrayData, c.children),
            dictionary=c.dictionary) for c in b.columns]
    return AC.RecordBatch(sch, cols, len)
end

_releasebatch!(b::AC.RecordBatch) =
    isempty(b.columns) || Arrow.release!(b.columns[1].owner::Arrow.ForeignOwner)

_verdict(fam, check, diffs) = Verdict(fam, check,
    isempty(diffs) ? :pass : :fail, isempty(diffs) ? "" : first(diffs))

_errverdict(fam, check, e) = Verdict(fam, check, :fail,
    sprint(showerror, e)[1:min(end, 200)])

function _compare(sch2, b2s, goldmasked)
    back = ArrowJSON.tojson(sch2, b2s)
    return docsequal(masknulls!(deepcopy(back), Val(:doc)), goldmasked)
end

function runcdatafamily(dir::String, family::String, verdicts::Vector{Verdict})
    if haskey(CDATA_SKIP, family)
        push!(verdicts, Verdict(family, "all", :skip, CDATA_SKIP[family]))
        return
    end
    gold = _readjson(joinpath(dir, family * ".json.gz"))
    goldmasked = masknulls!(deepcopy(gold), Val(:doc))
    sch, batches, dictids = ArrowJSON.fromjson(gold)

    # 1. ours → pyarrow (validate full) → ours ; 2. pyarrow-native → ours
    check1 = "ours→pyarrow(C)→ours"
    check2 = "pyarrow-native→ours"
    sch1 = sch
    b1s = AC.RecordBatch[]
    sch2 = sch
    b2s = AC.RecordBatch[]
    ok1 = ok2 = true
    for b in batches
        local pyb
        try
            pyb = _to_pyarrow(sch, b)
        catch e
            ok1 && push!(verdicts, _errverdict(family, check1, e))
            ok2 && push!(verdicts, _errverdict(family, check2, e))
            ok1 = ok2 = false
            break
        end
        if ok1
            try
                sch1, b1 = _from_pyarrow(pyb)
                push!(b1s, b1)
            catch e
                push!(verdicts, _errverdict(family, check1, e))
                ok1 = false
            end
        end
        if ok2
            try
                native = _pyarrow_rebuild(pyb)
                sch2, b2 = _from_pyarrow(native)
                push!(b2s, b2)
                PythonCall.pydel!(native)
            catch e
                push!(verdicts, _errverdict(family, check2, e))
                ok2 = false
            end
        end
        PythonCall.pydel!(pyb)
    end
    ok1 && push!(verdicts, _verdict(family, check1, _compare(sch1, b1s, goldmasked)))
    ok2 && push!(verdicts, _verdict(family, check2, _compare(sch2, b2s, goldmasked)))
    foreach(_releasebatch!, b1s)
    foreach(_releasebatch!, b2s)

    # 3. pyarrow slice → ours, against our own logical slice
    check3 = "pyarrow slice→ours"
    sliceable = [b for b in batches if b.nrows >= 3]
    if isempty(sliceable)
        push!(verdicts, Verdict(family, check3, :skip, "no batch with ≥3 rows"))
    else
        try
            diffs = String[]
            for b in sliceable
                off, len = 1, b.nrows - 2
                pyb = _to_pyarrow(sch, b)
                sliced = pyb.slice(off, len)
                schs, bs = _from_pyarrow(sliced)
                PythonCall.pydel!(sliced)
                PythonCall.pydel!(pyb)
                want = ArrowJSON.tojson(sch, [_sliceours(sch, b, off, len)])
                got = ArrowJSON.tojson(schs, [bs])
                append!(diffs, docsequal(masknulls!(got, Val(:doc)),
                    masknulls!(want, Val(:doc))))
                _releasebatch!(bs)
                isempty(diffs) || break
            end
            push!(verdicts, _verdict(family, check3, diffs))
        catch e
            push!(verdicts, _errverdict(family, check3, e))
        end
    end

    # 4. ours → pyarrow RecordBatchReader → ours, over the C stream interface
    check4 = "ours→pyarrow(stream)→ours"
    try
        outref = Ref{CAS}()
        inref = Ref{CAS}()
        sch4, b4s = GC.@preserve outref inref begin
            outp = Base.unsafe_convert(Ptr{CAS}, outref)
            Arrow.export_stream!(outp, sch, batches)
            reader = pa.RecordBatchReader._import_from_c(UInt(outp))
            unsafe_load(outp).release == C_NULL ||
                error("pyarrow did not mark the imported stream released")
            inp = Base.unsafe_convert(Ptr{CAS}, inref)
            reader._export_to_c(UInt(inp))
            s = Arrow.from_c_stream(inp)
            got = AC.RecordBatch[]
            while (b = AC.nextbatch!(s)) !== nothing
                push!(got, b)
            end
            Arrow.release!(s)
            PythonCall.pydel!(reader)
            s.schema, got
        end
        push!(verdicts, _verdict(family, check4, _compare(sch4, b4s, goldmasked)))
        foreach(_releasebatch!, b4s)
    catch e
        push!(verdicts, _errverdict(family, check4, e))
    end
    return
end

# The C interfaces carry data, not IPC framing, so a family's JSON is the
# same test whichever corpus version directory it lives in: run each family
# once, from the newest directory that has it.
function _familydirs(corpus::String)
    root = joinpath(corpus, "data", "arrow-ipc-stream", "integration")
    isdir(root) || error("corpus not found at $root (set ARROW_TESTING_DIR)")
    vdirs = filter(d -> isdir(joinpath(root, d)), readdir(root))
    sort!(vdirs; by=d -> (startswith(d, "cpp-"), d), rev=true)
    chosen = Pair{String,String}[]   # family => version dir
    seen = Set{String}()
    for v in vdirs
        dir = joinpath(root, v)
        for f in sort!(readdir(dir))
            endswith(f, ".json.gz") || continue
            fam = replace(f, r"\.json\.gz$" => "")
            fam in seen && continue
            push!(seen, fam)
            push!(chosen, fam => v)
        end
    end
    return root, chosen
end

function runcdataoracle(corpus::String=DEFAULT_CORPUS)
    root, chosen = _familydirs(corpus)
    println("cdata oracle: pyarrow ", pa.__version__, " over ",
        length(chosen), " families")
    verdicts = Verdict[]
    for (fam, v) in chosen
        before = length(verdicts)
        runcdatafamily(joinpath(root, v), fam, verdicts)
        for i = (before + 1):length(verdicts)
            vd = verdicts[i]
            verdicts[i] = Verdict(v * "/" * vd.family, vd.check, vd.status, vd.detail)
        end
    end
    # Every structure handed to pyarrow must have come back exactly once.
    PythonCall.GC.gc()
    GC.gc()
    GC.gc()
    Arrow.reap!()
    leaked = length(Arrow.EXPORT_REGISTRY) + Arrow._stream_registry_count()
    push!(verdicts, Verdict("(all)", "export registries drained",
        leaked == 0 ? :pass : :fail,
        leaked == 0 ? "" : "$leaked export root(s) still registered"))
    return verdicts
end

end # child definitions

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
    if length(ARGS) >= 1 && ARGS[1] == _CDATA_ORACLE_CHILD
        _run_cdata_oracle(ARGS[2:end])
    else
        _oracle_parent(ARGS)
    end
end
