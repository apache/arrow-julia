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

# The table-level C data interface: `Arrow.Table` and `Arrow.Stream` over
# imported C structs, and `Arrow.to_c_data` and `Arrow.export_stream!` over any
# Tables.jl table. Imports build columns the way an IPC read does; exports
# build record batches the way `Arrow.write` does.

# --- import -----------------------------------------------------------------

# The read keywords, checked before anything is moved out of a C struct, so a
# bad keyword cannot strand an import.
function _creadargs(;
    stringtype::Type=DataStrings.DataString,
    ntasks::Integer=Threads.nthreads(),
    limits::Limits=Limits(),
    unknown...,
)
    isempty(unknown) || throw(
        ArgumentError(
            "a C data import takes the keywords stringtype, ntasks, and limits, not " *
            join(keys(unknown), ", "),
        ),
    )
    _checkstringtype(stringtype)
    return stringtype, _checkntasks(ntasks), limits
end

# The table of `batches` with schema `sch`, built as an IPC read builds one.
# View columns read the producer's memory without checking a flag, and the
# Table contract lets columns outlive `release!`, so `release!(table)` only
# refuses further column access; the producer's release runs when the last
# column is garbage collected, as a mapped file is unmapped.
function _ctable(
    sch::AC.Schema,
    batches::Vector{AC.RecordBatch},
    stringtype,
    ntasks,
    limits,
)
    budget = _readbudget(limits, sum(_databytes, batches; init=Int64(0)))
    fields = collect(AC.Field, sch.fields)
    names = _fieldnamesymbols(fields, budget)
    arrowtypes = _ArrowTypesContext(budget=budget)
    views = _ViewContext(stringtype, budget)
    cols = _buildcolumns(
        fields,
        batches,
        1:length(fields),
        arrowtypes,
        views,
        ntasks;
        lazy=true,
    )
    nrows = sum(Int(b.nrows) for b in batches; init=0)
    pools = _retaineddictpools(fields, batches, arrowtypes)
    regions = AC.OwnerRegion[AC.OwnerRegion(Ptr{UInt8}(C_NULL), 0)]
    return _table(names, cols, _uniqueschema(sch), regions, nrows, pools, budget)
end

# Release the batches of a failed import; a batch without columns has nothing to hold.
function _releasebatches!(batches)
    for b in batches
        isempty(b.columns) || release!(b.columns[1].owner::ForeignOwner)
    end
    return nothing
end

# A struct array is a record batch of its fields; any other array is a
# one-column batch named after its schema.
function _recordbatch(f::AC.Field, d::AC.ArrayData)
    if f.type isa AC.StructType
        sch = AC.Schema(collect(AC.Field, f.children); metadata=f.metadata)
        return sch, _structbatch(sch, d)
    end
    sch = AC.Schema(AC.Field[f])
    return sch, AC.RecordBatch(sch, AC.ArrayData[d], d.len)
end

"""
    Arrow.Table(schemaptr::Ptr{Arrow.CArrowSchema}, arrayptr::Ptr{Arrow.CArrowArray};
                stringtype=DataString, ntasks=Threads.nthreads(), limits=Arrow.Limits())

Import one array through the C data interface as a table. A struct array (a record
batch) becomes a table of its fields; any other array becomes a one-column table named
after the schema. The array is moved, as the interface requires, and the schema is read
and released. The columns have the types an IPC read of the same data has. Numbers,
dates, and other fixed-width values are read in the producer's memory, and string and
binary values are copied. `Arrow.release!(table)` refuses further column access, and the
producer's `release` callback runs once the table and its columns are garbage collected.
`stringtype`, `ntasks`, and `limits` mean what they mean for `Arrow.Table`.
"""
function Table(sp::Ptr{CArrowSchema}, ap::Ptr{CArrowArray}; kw...)
    stringtype, ntasks, limits = _creadargs(; kw...)
    f, d = from_c_data(sp, ap)
    try
        sch, b = _recordbatch(f, d)
        return _ctable(sch, AC.RecordBatch[b], stringtype, ntasks, limits)
    catch
        release!(d.owner::ForeignOwner)
        rethrow()
    end
end

"""
    Arrow.Table(streamptr::Ptr{Arrow.CArrowArrayStream};
                stringtype=DataString, ntasks=Threads.nthreads(), limits=Arrow.Limits())

Import a whole C stream as a table. The stream is moved, as the interface requires,
every record batch is read, and the stream is released. The columns chain the batches
as an IPC read's do; fixed-width values stay in the producer's memory, which its
`release` callbacks give back once the table and its columns are garbage collected.
[`Arrow.Stream(streamptr)`](@ref Arrow.Stream) reads one batch at a time instead.
"""
function Table(p::Ptr{CArrowArrayStream}; kw...)
    stringtype, ntasks, limits = _creadargs(; kw...)
    s = from_c_stream(p; limits)
    batches = AC.RecordBatch[]
    try
        while (b = AC.nextbatch!(s)) !== nothing
            push!(batches, b)
        end
        return _ctable(s.schema, batches, stringtype, ntasks, limits)
    catch
        _releasebatches!(batches)
        rethrow()
    finally
        release!(s)
    end
end

# The cell action of an `Arrow.Stream` over a C stream: release the producer's stream.
function _release_imported_stream(p::Ptr{Cvoid})::Cvoid
    release!(unsafe_pointer_to_objref(p)::ImportedStream)
    return nothing
end

"""
    Arrow.Stream(streamptr::Ptr{Arrow.CArrowArrayStream};
                 stringtype=DataString, ntasks=Threads.nthreads(), limits=Arrow.Limits())

Read a C stream one record batch at a time: each iteration pulls the next batch from the
producer and gives it as an [`Arrow.Table`](@ref), built as
[`Arrow.Table(streamptr)`](@ref Arrow.Table) builds its columns. The stream is moved, as the interface requires. It is iterated once,
from one task at a time, and its length is unknown until it is read. As for any
`Arrow.Stream`, [`Arrow.release!`](@ref) of the stream or of a table it yielded
releases the producer's stream and refuses further column access; columns already
taken stay valid.
"""
function Stream(p::Ptr{CArrowArrayStream}; kw...)
    stringtype, ntasks, limits = _creadargs(; kw...)
    src = from_c_stream(p; limits)
    budget = _readbudget(limits, 0)
    closed = AC.ReleaseCell(@cfunction(_release_imported_stream, Cvoid, (Ptr{Cvoid},)), src)
    return Stream(
        src,
        AC.OwnerRegion[AC.OwnerRegion(Ptr{UInt8}(C_NULL), 0; cell=closed)],
        budget,
        closed,
        _ArrowTypesContext(budget=budget),
        _ViewContext(stringtype, budget),
        ReentrantLock(),
        ntasks,
        nothing,
    )
end

# The imported stream as an `Arrow.Stream` source: its batches arrive as the
# iteration pulls them (`_streambatch`), so their number is unknown until then.
_configuredlimits(s::ImportedStream) = s.limits
_tableschema(s::ImportedStream) = _uniqueschema(s.schema)
_batchfields(s::ImportedStream) = s.schema.fields
_nbatches(::ImportedStream) = throw(
    ArgumentError(
        "the number of batches of a C stream is unknown until it is read; " *
        "iterate the Arrow.Stream",
    ),
)

# --- export -----------------------------------------------------------------

# The schema and record batches of `tbl`: a one-shot IPC stream write to nowhere
# that keeps each batch it builds, so construction, dictionary state, and refusals
# are the writer's own and a C consumer sees what pyarrow reads from the IPC bytes.
# The kept buffers are fresh allocations (`exported` turns off the writer's reusable
# pool), because the consumer holds on to them. One record batch per partition, unless
# `batchsize` splits it or a batch's string data outgrows 32-bit offsets. A first
# partition with no rows still fixes the schema; its empty batch is the only one with
# no rows.
function _exportbatches(tbl; batchsize::Integer=typemax(Int), kw...)
    for k in (:file, :compress, :compresslevel, :dictdeltas, :dictreplacement)
        haskey(kw, k) && throw(ArgumentError("$k does not apply to the C data interface"))
    end
    batches = AC.RecordBatch[]
    w = _writer(devnull; oneshot=true, batchsize, exported=batches, kw...)
    _writeall!(w, tbl)
    return w.schema::AC.Schema, batches
end

"""
    Arrow.to_c_data(table; kw...) -> (Ptr{Arrow.CArrowSchema}, Ptr{Arrow.CArrowArray})

Export a Tables.jl table as one record batch through the C data interface: an
`ArrowSchema` and an `ArrowArray` that Arrow.jl allocates, whose struct array has one
child per column. The columns are built as [`Arrow.write`](@ref) builds them, with the
same keywords (`dictencode`, `stringview`, `largelists`, `metadata`, `colmetadata`), so a
consumer reads what it would read from the IPC bytes. A `Vector` of `Int8` to `Int64`,
`UInt8` to `UInt64`, or `Float16` to `Float64` crosses without copying, as do an
[`Arrow.Table`](@ref) column's integer, float, and boolean values and validity bitmaps;
what crosses without copying must not be modified or resized while the consumer reads
it. Everything else, `Bool` and `missing` in Julia vectors, decimals, dates and
timestamps, string and list offsets, a list's own validity bitmap, and fixed-size list
values included, is built into new buffers, and string bytes may be copied. The consumer
moves or releases the two structs; the table's memory stays reachable until it has
released the array, and what Arrow.jl allocated is freed by later exports or garbage
collections. A table that needs more than one record batch (several partitions
with rows, or string data past 2 GiB) is an `ArgumentError`; export it with
[`Arrow.export_stream!`](@ref).
"""
function to_c_data(tbl; kw...)
    sch, batches = _exportbatches(tbl; kw...)
    # An empty first partition only fixed the schema.
    length(batches) > 1 && filter!(b -> b.nrows > 0, batches)
    length(batches) == 1 || throw(
        ArgumentError(
            "the table needs $(length(batches)) record batches; export it with " *
            "Arrow.export_stream!",
        ),
    )
    return to_c_data(_batchfield(sch), _structarray(only(batches)))
end

"""
    Arrow.export_stream!(streamptr::Ptr{Arrow.CArrowArrayStream}, table; kw...)

Fill the `ArrowArrayStream` the caller allocated at `streamptr` with the record batches
of a Tables.jl table: one per `Tables.partitions` partition (an [`Arrow.Stream`](@ref)
is a partition per batch read), split only when `batchsize` is given or a batch's string
data outgrows 32-bit offsets. The batches are built as [`Arrow.write`](@ref) builds them,
with the same keywords, before this returns; the consumer pulls them through the stream
and releases it. Buffers cross without copying where `Arrow.write` would not copy them.
Memory stays reachable until the consumer has released the stream and every batch it
pulled.
"""
function export_stream!(sp::Ptr{CArrowArrayStream}, tbl; kw...)
    sch, batches = _exportbatches(tbl; kw...)
    return export_stream!(sp, sch, AC.RecordBatch[b for b in batches if b.nrows > 0])
end
