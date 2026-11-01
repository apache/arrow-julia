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

# The C stream interface (ArrowArrayStream) over the C data adapter in
# cdata.jl: an exported stream is one export whose `get_schema` and `get_next`
# results are exports of their own, and an imported stream is a moved struct
# whose batches import as arrays do. Kept apart from cdata.jl so the trim gate
# can compile the array interface without the IPC reader's `Limits`.

"""
    Arrow.CArrowArrayStream

The C stream interface's `ArrowArrayStream` struct, field for field.
"""
struct CArrowArrayStream
    get_schema::Ptr{Cvoid}     # int (*)(ArrowArrayStream*, ArrowSchema* out)
    get_next::Ptr{Cvoid}       # int (*)(ArrowArrayStream*, ArrowArray* out)
    get_last_error::Ptr{Cvoid} # const char* (*)(ArrowArrayStream*)
    release::Ptr{Cvoid}        # void (*)(ArrowArrayStream*)
    private_data::Ptr{Cvoid}
end

const EINVAL = Cint(Base.Libc.EINVAL)

# An all-zero struct: a released one (`release == NULL`), which also ends a stream.
function _zeroed(::Type{T}) where {T}
    r = Ref{T}()
    GC.@preserve r Libc.memset(Base.unsafe_convert(Ptr{T}, r), 0, sizeof(T))
    return r
end

# The batches an exported stream yields, pulled one `get_next` at a time.
mutable struct ExportedStreamState
    const batchfield::Field          # struct-typed: children are the schema
    const batches::Vector{AC.RecordBatch}
    nextindex::Int
end

# A record batch crosses as a struct array: the struct field's children are
# the schema's fields and it carries the schema-level metadata (the
# C++/pyarrow convention for `schema.metadata`).
_batchfield(sch::Schema) = Field(
    "",
    StructType();
    nullable=false,
    metadata=sch.metadata,
    children=collect(Field, sch.fields),
)
_structarray(b::AC.RecordBatch) = ArrayData(
    StructType(),
    b.nrows,
    [BufferSlice()];
    children=collect(ArrayData, b.columns),
    nullcount=0,
)

# The exported stream's state, or `nothing` once released or reaped.
function _stream_state(sp::Ptr{CArrowArrayStream})
    stream = unsafe_load(sp)
    (stream.release == C_NULL || stream.private_data == C_NULL) && return nothing
    e = lock(REGISTRY_LOCK) do
        get(EXPORTS, Ptr{Int64}(stream.private_data), nothing)
    end
    e === nothing && return nothing
    return e.roots[1]::ExportedStreamState
end

# Store `err` as the stream's last error, for `get_last_error`, in the 8 bytes
# after the export's live count. Best effort: a C callback cannot throw.
function _streamerror!(sp::Ptr{CArrowArrayStream}, err)
    try
        control = unsafe_load(sp).private_data
        control == C_NULL && return nothing
        slot = Ptr{Ptr{UInt8}}(control + 8)
        old = unsafe_load(slot)
        unsafe_store!(slot, Ptr{UInt8}(C_NULL))
        old == C_NULL || Libc.free(old)
        message = replace(sprint(showerror, err), '\0' => ' ')
        n = ncodeunits(message)
        p = Ptr{UInt8}(Libc.malloc(n + 1))
        p == C_NULL && return nothing
        GC.@preserve message unsafe_copyto!(p, pointer(message), n)
        unsafe_store!(p, 0x00, n + 1)
        unsafe_store!(slot, p)
    catch
    end
    return nothing
end

function _stream_get_schema(sp::Ptr{CArrowArrayStream}, out::Ptr{CArrowSchema})::Cint
    sp == C_NULL && return EINVAL
    try
        state = _stream_state(sp)
        state === nothing && return EINVAL
        out == C_NULL && throw(ArgumentError("ArrowSchema output pointer is NULL"))
        e = _newexport(Any[state.batchfield])
        try
            _export_schema!(
                e,
                state.batchfield,
                @cfunction(_release_schema, Cvoid, (Ptr{CArrowSchema},));
                at=out,
            )
            _register!(e)
        catch
            _free!(e)
            rethrow()
        end
        return Cint(0)
    catch err
        _streamerror!(sp, err)
        return EINVAL
    end
end

function _stream_get_next(sp::Ptr{CArrowArrayStream}, out::Ptr{CArrowArray})::Cint
    sp == C_NULL && return EINVAL
    try
        state = _stream_state(sp)
        state === nothing && return EINVAL
        out == C_NULL && throw(ArgumentError("ArrowArray output pointer is NULL"))
        i = state.nextindex
        if i > length(state.batches)
            unsafe_store!(out, _zeroed(CArrowArray)[])
            return Cint(0)
        end
        d = _structarray(state.batches[i])
        validate_semantic(state.batchfield, d)
        e = _newexport(Any[d])
        try
            _export_array!(
                e,
                d,
                @cfunction(_release_array, Cvoid, (Ptr{CArrowArray},));
                at=out,
            )
            _register!(e)
        catch
            _free!(e)
            rethrow()
        end
        state.nextindex = i + 1
        return Cint(0)
    catch err
        _streamerror!(sp, err)
        return EINVAL
    end
end

function _stream_get_last_error(sp::Ptr{CArrowArrayStream})::Ptr{UInt8}
    sp == C_NULL && return Ptr{UInt8}(C_NULL)
    stream = unsafe_load(sp)
    (stream.release == C_NULL || stream.private_data == C_NULL) && return Ptr{UInt8}(C_NULL)
    return unsafe_load(Ptr{Ptr{UInt8}}(stream.private_data + 8))
end

function _stream_release(sp::Ptr{CArrowArrayStream})::Cvoid
    sp == C_NULL && return nothing
    stream = unsafe_load(sp)
    stream.release == C_NULL && return nothing
    _store_field!(sp, Val(:release), Ptr{Cvoid}(C_NULL))
    stream.private_data == C_NULL ||
        Base.unsafe_modify!(Ptr{Int64}(stream.private_data), -, 1, :sequentially_consistent)
    return nothing
end

# Core's structural schema invariants (endianness, UTF-8 names,
# valid descriptors) for the whole schema TREE. Stream schemas travel
# separately from any batch, so both stream directions apply this walk to the
# schema itself — a zero-batch stream never reaches batch validation.
function _validate_stream_schema(sch::Schema)
    AC._validate_schema(sch)
    for f in sch.fields
        _validate_stream_field(f)
    end
    return nothing
end

function _validate_stream_field(f::Field)
    isvalid(f.name) || throw(ValidationError("field name is not valid UTF-8"))
    AC._validate_descriptor_of(f.type)
    for c in f.children
        _validate_stream_field(c)
    end
    return nothing
end

"""
    Arrow.export_stream!(streamptr::Ptr{CArrowArrayStream}, schema::Arrow.Schema, batches)

The low-level form of [`Arrow.export_stream!`](@ref): fill the `ArrowArrayStream` the
caller allocated at `streamptr` with a stream that yields `batches`, a vector of
`Arrow.RecordBatch` values with `schema`. Each batch the consumer pulls is a struct
array of the batch's columns, exported as [`Arrow.to_c_data`](@ref) exports a column.
The batches stay reachable until the consumer releases the stream.
"""
function export_stream!(
    sp::Ptr{CArrowArrayStream},
    sch::Schema,
    batches::AbstractVector{AC.RecordBatch},
)
    sp == C_NULL && throw(ArgumentError("ArrowArrayStream pointer is NULL"))
    _validate_stream_schema(sch)
    for b in batches
        length(b.columns) == length(sch.fields) ||
            throw(ValidationError("stream batch column count does not match the schema"))
    end
    state = ExportedStreamState(_batchfield(sch), collect(AC.RecordBatch, batches), 1)
    e = _newexport(Any[state])
    unsafe_store!(e.live, 1)   # the stream itself; its release counts it off
    unsafe_store!(
        sp,
        CArrowArrayStream(
            @cfunction(
                _stream_get_schema,
                Cint,
                (Ptr{CArrowArrayStream}, Ptr{CArrowSchema})
            ),
            @cfunction(_stream_get_next, Cint, (Ptr{CArrowArrayStream}, Ptr{CArrowArray})),
            @cfunction(_stream_get_last_error, Ptr{UInt8}, (Ptr{CArrowArrayStream},)),
            @cfunction(_stream_release, Cvoid, (Ptr{CArrowArrayStream},)),
            Ptr{Cvoid}(e.live),
        ),
    )
    _register!(e)
    return sp
end

# ---- import half ----------------------------------------------------------

# One owner for one MOVED ArrowArrayStream, mirroring ForeignOwner: a malloc'd
# copy of the moved struct gives the producer's callbacks a stable address,
# and one atomic swap picks the single releaser between `release!` and the GC
# finalizer.
mutable struct StreamOwner
    const block::Ptr{CArrowArrayStream}
    const thread::Int   # the importing thread (see `_finalizeowner`)
    @atomic released::Bool
end

function _movestream(sp::Ptr{CArrowArrayStream})
    block = Ptr{CArrowArrayStream}(Libc.malloc(sizeof(CArrowArrayStream)))
    block == C_NULL && throw(OutOfMemoryError())
    unsafe_store!(block, unsafe_load(sp))
    _store_field!(sp, Val(:release), Ptr{Cvoid}(C_NULL))
    try
        return finalizer(_finalizeowner, StreamOwner(block, Threads.threadid(), false))
    catch
        try
            _release_producer!(block)
        finally
            Libc.free(block)
        end
        rethrow()
    end
end

Base.deepcopy_internal(o::StreamOwner, ::IdDict) = o

function release!(o::StreamOwner)
    @atomicswap(o.released = true) && return nothing
    GC.@preserve o try
        _release_producer!(o.block)
    finally
        Libc.free(o.block)
    end
    return nothing
end

# A stream imported with `from_c_stream`. `nextbatch!` returns its next
# `RecordBatch`, or `nothing` at the end, and `release!` releases the
# producer's stream; batches already read stay valid. An error the producer
# reports is a `ValidationError` with the producer's message. One task uses a
# stream at a time: overlapping calls throw a `ConcurrencyViolationError`.
mutable struct ImportedStream <: AC.RecordBatchSource
    const owner::StreamOwner
    const batchfield::Field
    const schema::Schema
    const limits::Limits
    done::Bool
    @atomic pulling::Bool
end

AC.schema(s::ImportedStream) = s.schema

function _claimstreamcall!(s::ImportedStream)
    _, ok = @atomicreplace s.pulling false => true
    ok || throw(
        Base.ConcurrencyViolationError(
            "ImportedStream supports only one active stream call",
        ),
    )
    return nothing
end

_finishstreamcall!(s::ImportedStream) = (@atomic :release s.pulling = false)

function release!(s::ImportedStream)
    _claimstreamcall!(s)
    try
        return release!(s.owner)
    finally
        _finishstreamcall!(s)
    end
end

function _stream_call_failed(o::StreamOwner, what::AbstractString)
    msg = "C stream $what failed"
    GC.@preserve o begin
        cb = unsafe_load(o.block).get_last_error
        if cb != C_NULL
            p = ccall(cb, Ptr{UInt8}, (Ptr{CArrowArrayStream},), o.block)
            p == C_NULL || (msg *= ": " * _import_cstring(p, "stream error"))
        end
    end
    throw(ValidationError(msg))
end

# Import a C stream: take over the stream, marking the struct at `sp` released,
# and read its schema, whose fields are the batches' columns. `limits` bounds
# what the facade allocates for each batch.
function from_c_stream(sp::Ptr{CArrowArrayStream}; limits::Limits=Limits())
    sp == C_NULL && throw(ArgumentError("ArrowArrayStream pointer is NULL"))
    stream = unsafe_load(sp)
    stream.release == C_NULL && throw(ArgumentError("cannot import a released stream"))
    (
        stream.get_schema == C_NULL ||
        stream.get_next == C_NULL ||
        stream.get_last_error == C_NULL
    ) && throw(ArgumentError("C stream is missing required callbacks"))
    owner = _movestream(sp)
    try
        out = _zeroed(CArrowSchema)
        status = GC.@preserve owner out ccall(
            unsafe_load(owner.block).get_schema,
            Cint,
            (Ptr{CArrowArrayStream}, Ptr{CArrowSchema}),
            owner.block,
            Base.unsafe_convert(Ptr{CArrowSchema}, out),
        )
        status == 0 || _stream_call_failed(owner, "get_schema")
        sch = out[]
        batchfield = GC.@preserve out try
            _preflight_schema(sch)
            _import_field(sch)
        finally
            _release_producer!(Base.unsafe_convert(Ptr{CArrowSchema}, out))
        end
        batchfield.type isa StructType ||
            throw(ValidationError("C stream schema must be a struct-typed batch schema"))
        schema = Schema(collect(Field, batchfield.children); metadata=batchfield.metadata)
        _validate_stream_schema(schema)
        return ImportedStream(owner, batchfield, schema, limits, false, false)
    catch
        release!(owner)
        rethrow()
    end
end

function AC.nextbatch!(s::ImportedStream)
    _claimstreamcall!(s)
    try
        # Fail closed on a released stream even when it already ended: release
        # ends the consumer contract, not just the batch supply.
        (@atomic s.owner.released) &&
            throw(ArgumentError("cannot pull from a released stream"))
        s.done && return nothing
        out = _zeroed(CArrowArray)
        status = GC.@preserve s out ccall(
            unsafe_load(s.owner.block).get_next,
            Cint,
            (Ptr{CArrowArrayStream}, Ptr{CArrowArray}),
            s.owner.block,
            Base.unsafe_convert(Ptr{CArrowArray}, out),
        )
        status == 0 || _stream_call_failed(s.owner, "get_next")
        if out[].release == C_NULL
            s.done = true
            return nothing
        end
        owner = GC.@preserve out _moveowner(Base.unsafe_convert(Ptr{CArrowArray}, out))
        d = _importarray(s.batchfield, owner)
        try
            return _structbatch(s.schema, d)
        catch
            release!(owner)
            rethrow()
        end
    finally
        _finishstreamcall!(s)
    end
end

# A record batch has no row validity, so null struct rows are refused rather
# than exposing the child values under them (Arrow C++ refuses them too).
# Row i of a struct array is child row `offset + i`, and children may run past
# `offset + length`; each column is that visible child window.
function _structbatch(sch::Schema, d::ArrayData)
    AC.nullcount(d) == 0 || throw(
        ValidationError("a struct array with null rows has no record batch or table form"),
    )
    columns = ArrayData[
        d.offset == 0 && c.len == d.len ? c :
        AC._arraydata(
            c.type,
            d.len,
            c.buffers,
            c.offset + d.offset,
            c.children,
            c.dictionary,
            c.owner,
            -1,
        ) for c in d.children
    ]
    return AC.RecordBatch(sch, columns, d.len)
end
