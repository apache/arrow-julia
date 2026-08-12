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
# PROVE-OUT: the IPC adapter as a thin peer over ArrowCore.
#
# Run with the repo project so the existing package (and its vendored
# FlatBuffers/Flatbuf metadata bindings) is available:
#
#     julia --project=. core/examples/ipc_read.jl
#
# What this demonstrates, mapped to the redesign report:
#
#   * §9 "IPC adapter": stream framing with STAGE-1 resource limits enforced
#     BEFORE any allocation (`Limits` + `framemessages`), and the message
#     body as the decoding AUTHORITY — every Arrow buffer is a checked
#     subslice of its message-body slice, so corrupt metadata cannot alias
#     the schema message, another batch, or anything else in the file, even
#     though the whole input is one region.
#
#   * §9 "layout registry": ONE generic recursive decoder (`decodefield`,
#     ~45 lines) replaces the current implementation's ten `build` methods
#     with hand-threaded (nodeidx, bufferidx, varbufferidx) state
#     (src/table.jl:754-1174, ~420 lines). Node/buffer consumption order is
#     derived from `layoutspec`, so a new layout needs no new decoder.
#
#   * §9 "adapter owns IPC bookkeeping": dictionary ids live in an
#     adapter-side table (`dictionaries::Dict{Int64,...}`); Core Fields
#     carry `DictionaryType` object references and never see an id.
#
#   * The adapter REUSES the existing vendored flatbuffer metadata bindings
#     (Arrow.FlatBuffers / Arrow.Meta) — proving the metadata layer carries
#     over unchanged while everything downstream of it is replaced.
#
# The acceptance test at the bottom: today's Arrow.jl 2.x WRITES a stream
# (multi-batch, with nulls, strings, lists, structs, and a dict-encoded
# column); this adapter reads it back through ArrowCore and the values are
# compared element-for-element. New core, real bytes, no shims.
# =============================================================================

using Arrow                      # the existing 2.x package (repo project)
using Arrow.Tables               # partitioner for the multi-batch test write
const FB = Arrow.FlatBuffers     # vendored flatbuffers runtime (reused as-is)
const Meta = Arrow.Meta          # vendored format metadata bindings (reused)

include(joinpath(@__DIR__, "..", "ArrowCore.jl"))
using .ArrowCore
const AC = ArrowCore

# ---------------------------------------------------------------------------
# Stage-1 framing: resource limits before allocation
# ---------------------------------------------------------------------------

"""
Resource limits enforced during framing, before any body is interpreted or
any decode allocation happens (report §9, validation stage 1). Today's
reader has no equivalent — a hostile length prefix reaches
`Vector{UInt8}(undef, attacker_len)` (src/table.jl:804-816).
"""
Base.@kwdef struct Limits
    max_metadata_bytes::Int64 = 16 * 1024 * 1024
    max_body_bytes::Int64 = 2 * 1024 * 1024 * 1024
    max_messages::Int = 1_000_000
end

struct FramedMessage
    msg::Meta.Message        # parsed flatbuffer metadata
    body::BufferSlice        # THE authority: buffers must subslice this
end

const CONTINUATION = 0xFFFFFFFF

"""
    framemessages(region, limits) -> Vector{FramedMessage}

Walk the IPC stream framing (continuation marker, metadata length, metadata
flatbuffer, body), checking every declared length against the limits and the
region's real extent BEFORE constructing anything. A truncated or lying
stream is an error here — not a silent early return (the current framer
returns `nothing` on truncation, src/table.jl:679-708) and not a segfault
three batches later.
"""
function framemessages(region::OwnerRegion, limits::Limits=Limits())
    blob = BufferSlice(region, 0, region.len)
    msgs = FramedMessage[]
    pos = Int64(0)   # 0-based byte position within the blob
    while pos + 8 <= blob.len
        length(msgs) < limits.max_messages ||
            throw(ValidationError("message count exceeds limit"))
        cont = AC.loadat(blob, UInt32, pos)
        cont == CONTINUATION ||
            throw(ValidationError("missing continuation marker at byte $pos"))
        metalen = Int64(AC.loadat(blob, Int32, pos + 4))
        metalen == 0 && return msgs                      # explicit end-of-stream
        0 < metalen <= limits.max_metadata_bytes ||
            throw(ValidationError("metadata length $metalen outside (0, $(limits.max_metadata_bytes)]"))
        pos + 8 + metalen <= blob.len ||
            throw(ValidationError("truncated metadata: need $metalen bytes at $pos"))
        # The vendored flatbuffer reader wants a byte vector; hand it exactly
        # the metadata span (copied: metadata is small and limit-checked; the
        # BODY stays zero-copy).
        metabytes = AC.slicebytes(AC.subslice(blob, pos + 8, metalen))
        msg = FB.getrootas(Meta.Message, metabytes, 0)
        bodylen = Int64(msg.bodyLength)
        0 <= bodylen <= limits.max_body_bytes ||
            throw(ValidationError("body length $bodylen outside [0, $(limits.max_body_bytes)]"))
        bodystart = pos + 8 + metalen
        bodystart + bodylen <= blob.len ||
            throw(ValidationError("truncated body: need $bodylen bytes at $bodystart"))
        push!(msgs, FramedMessage(msg, AC.subslice(blob, bodystart, bodylen)))
        pos = bodystart + bodylen
    end
    return msgs
end

# ---------------------------------------------------------------------------
# Metadata mapping: Meta.* type structs -> Core runtime descriptors
# ---------------------------------------------------------------------------

# One value-level mapping table. Compare src/eltypes.jl, where this
# relationship is 22 `juliaeltype` + 21 `arrowtype` methods entangled with
# Julia-type conversion; here it is one function per direction on runtime
# values, and Julia conversion is someone else's (the facade's) concern.

function coretype(t)::ArrowType
    if t isa Meta.Int
        IntType(Int(t.bitWidth), t.is_signed)
    elseif t isa Meta.FloatingPoint
        FloatType(t.precision == Meta.Precision.HALF ? 16 :
                  t.precision == Meta.Precision.SINGLE ? 32 : 64)
    elseif t isa Meta.Bool
        BoolType()
    elseif t isa Meta.Utf8
        Utf8Type(false)
    elseif t isa Meta.LargeUtf8
        Utf8Type(true)
    elseif t isa Meta.Binary
        BinaryType(false)
    elseif t isa Meta.LargeBinary
        BinaryType(true)
    elseif t isa Meta.FixedSizeBinary
        FixedSizeBinaryType(Int(t.byteWidth))
    elseif t isa Meta.List
        ListType(false)
    elseif t isa Meta.LargeList
        ListType(true)
    elseif t isa Meta.FixedSizeList
        FixedSizeListType(Int(t.listSize))
    elseif t isa Meta.Struct
        StructType()
    elseif t isa Meta.Map
        MapType(t.keysSorted)
    elseif t isa Meta.Timestamp
        TimestampType(timeunit(t.unit), t.timezone === nothing ? nothing : String(t.timezone))
    elseif t isa Meta.Date
        DateType(t.unit == Meta.DateUnit.DAY ? AC.DAY : AC.MILLISECOND_DATE)
    elseif t isa Meta.Time
        TimeType(timeunit(t.unit), Int(t.bitWidth))
    elseif t isa Meta.Duration
        DurationType(timeunit(t.unit))
    elseif t isa Meta.Decimal
        DecimalType(Int(t.precision), Int(t.scale), Int(t.bitWidth))
    elseif t isa Meta.Null
        NullType()
    else
        error("IPC adapter prove-out: unmapped metadata type $(typeof(t)) " *
              "(unions/views/REE mapping is roadmap slice work)")
    end
end

timeunit(u) = u == Meta.TimeUnit.SECOND ? AC.SECOND :
    u == Meta.TimeUnit.MILLISECOND ? AC.MILLISECOND :
    u == Meta.TimeUnit.MICROSECOND ? AC.MICROSECOND : AC.NANOSECOND

"""
Convert a metadata Field to a Core Field. Dictionary-encoded fields become
`DictionaryType` here; the IPC dictionary id is recorded in the adapter's
side table (`dictids`), NOT on the Core field — Core never learns about ids.
"""
function corefield(f::Meta.Field, dictids::Dict{Int64,Meta.Field},
    fielddictids::IdDict{Field,Int64})
    children = Field[corefield(c, dictids, fielddictids)
                     for c in something(f.children, Meta.Field[])]
    t = coretype(f.type)
    if f.dictionary === nothing
        return Field(String(f.name), t, f.nullable, nothing, children)
    end
    dictids[f.dictionary.id] = f
    idxt = f.dictionary.indexType === nothing ? IntType(32, true) :
        coretype(f.dictionary.indexType)::IntType
    cf = Field(String(f.name), DictionaryType(idxt, t, f.dictionary.isOrdered),
        f.nullable, nothing, children)
    # Identity-keyed: safe for duplicate column names and nested dict fields
    # (name matching would be neither).
    fielddictids[cf] = f.dictionary.id
    return cf
end

# ---------------------------------------------------------------------------
# THE generic decoder: registry-driven node/buffer consumption
# ---------------------------------------------------------------------------

# This function is the headline. The current implementation threads
# (nodeidx, bufferidx, varbufferidx) by hand through ten `build` methods —
# an off-by-one in any of them silently shifts every subsequent buffer
# (the #540 bug class). Here consumption order falls out of `layoutspec`:
# one field = one node (unless the layout says otherwise) + the registry's
# buffers in registry order + children in declared order. A mismatch is a
# thrown error at the *end* of the batch (leftover nodes/buffers), not
# corruption.

mutable struct DecodeCursor
    nodes::Vector{Meta.FieldNode}
    buffers::Vector{Meta.Buffer}
    body::BufferSlice
    nodeidx::Int
    bufidx::Int
end

function takenode!(c::DecodeCursor)
    c.nodeidx <= length(c.nodes) ||
        throw(ValidationError("metadata declares fewer field nodes than the schema requires"))
    n = c.nodes[c.nodeidx]
    c.nodeidx += 1
    return n
end

function takebuffer!(c::DecodeCursor)
    c.bufidx <= length(c.buffers) ||
        throw(ValidationError("metadata declares fewer buffers than the schema requires"))
    b = c.buffers[c.bufidx]
    c.bufidx += 1
    # THE checked-subslice step: a buffer is only ever a window into this
    # message's body span. Checked arithmetic in `subslice` turns a corrupt
    # offset/length into a clean ValidationError.
    return AC.subslice(c.body, Int64(b.offset), Int64(b.length))
end

"""
    decodefield(field, cursor, dictionaries) -> ArrayData

Generic over every layout the registry knows. Dictionary-encoded columns
consume the INDEX layout's buffers (validity + indices) and resolve their
values through the adapter's dictionary table.
"""
function decodefield(f::Field, c::DecodeCursor, dicts::Dict{Int64,ArrayData},
    fielddictids::IdDict{Field,Int64})
    t = f.type
    node = takenode!(c)
    spec = layoutspec(t)
    buffers = BufferSlice[takebuffer!(c) for _ in spec.buffers]
    children = ArrayData[]
    if t isa DictionaryType
        # Index buffers were just consumed; values come from the side table.
        id = fielddictids[f]
        haskey(dicts, id) ||
            throw(ValidationError("record batch references dictionary id $id before its dictionary batch"))
        return ArrayData(t, node.length, buffers; dictionary=dicts[id],
            nullcount=node.null_count)
    end
    nchildren = spec.childcount == -1 ? length(f.children) : spec.childcount
    for i = 1:nchildren
        push!(children, decodefield(f.children[i], c, dicts, fielddictids))
    end
    return ArrayData(t, node.length, buffers; children=children,
        nullcount=node.null_count)
end

# ---------------------------------------------------------------------------
# Stream reader: RecordBatchSource over framed messages
# ---------------------------------------------------------------------------

struct IPCStream <: AC.RecordBatchSource
    schema::Schema
    corefields::Vector{Field}
    batches::Vector{AC.RecordBatch}
end
AC.schema(s::IPCStream) = s.schema

function readstream(bytes::Vector{UInt8}; limits::Limits=Limits())
    region = heapregion(bytes)
    msgs = framemessages(region, limits)
    isempty(msgs) && error("empty stream")
    msgs[1].msg.header isa Meta.Schema ||
        throw(ValidationError("first IPC message must be a schema"))
    metaschema = msgs[1].msg.header
    dictids = Dict{Int64,Meta.Field}()
    fielddictids = IdDict{Field,Int64}()   # adapter-side id table (report §9)
    fields = Field[corefield(f, dictids, fielddictids) for f in metaschema.fields]
    sch = Schema(fields)
    dicts = Dict{Int64,ArrayData}()
    batches = AC.RecordBatch[]
    for fm in msgs[2:end]
        header = fm.msg.header
        if header isa Meta.DictionaryBatch
            header.isDelta &&
                error("delta dictionaries are writer-coordinator roadmap work (report §9)")
            rb = header.data
            rb.compression === nothing ||
                error("compression is extension roadmap work (report §13, slice 2j)")
            # A dictionary batch's payload is a one-column record batch of
            # the VALUE type; decode it with the same generic decoder. The
            # value field is the metadata field minus its dictionary tag.
            mf = dictids[header.id]
            # Nested dictionary-encoded children of a dictionary's VALUES
            # are out of prove-out scope; the throwaway tables make that an
            # explicit decode error (missing id) rather than silent misreads.
            vf = Field(String(mf.name), coretype(mf.type), mf.nullable, nothing,
                Field[corefield(c, Dict{Int64,Meta.Field}(), IdDict{Field,Int64}())
                      for c in something(mf.children, Meta.Field[])])
            cursor = DecodeCursor(rb.nodes, rb.buffers, fm.body, 1, 1)
            dicts[header.id] = decodefield(vf, cursor, dicts, fielddictids)
        elseif header isa Meta.RecordBatch
            header.compression === nothing ||
                error("compression is extension roadmap work (report §13, slice 2j)")
            cursor = DecodeCursor(header.nodes, header.buffers, fm.body, 1, 1)
            cols = ArrayData[decodefield(f, cursor, dicts, fielddictids) for f in fields]
            # End-of-batch accounting check: everything declared must be
            # consumed — a mismatch is an error HERE, not skewed buffers.
            cursor.nodeidx == length(cursor.nodes) + 1 ||
                throw(ValidationError("unconsumed field nodes: schema/batch mismatch"))
            cursor.bufidx == length(cursor.buffers) + 1 ||
                throw(ValidationError("unconsumed buffers: schema/batch mismatch"))
            for (f, col) in zip(fields, cols)
                validate_structural(f, col)
                validate_semantic(f, col)
            end
            push!(batches, AC.RecordBatch(sch, cols))
        else
            error("unsupported IPC message header $(typeof(header)) in prove-out")
        end
    end
    return IPCStream(sch, fields, batches)
end

# ---------------------------------------------------------------------------
# Acceptance: 2.x writes, Core reads
# ---------------------------------------------------------------------------

function main()
    expected = (
        ints=Int64[1, 2, 3, 4, 5],
        floats=[1.5, missing, 3.5, missing, 5.5],
        bools=[true, false, true, missing, false],
        strs=["hey", "", missing, "αβ∀", "last"],
        lists=[[1, 2], Int64[], [3], missing, [4, 5, 6]],
        structs=[(a=1, b="x"), (a=2, b="y"), (a=3, b="z"), (a=4, b="w"), (a=5, b="v")],
        dict=Arrow.DictEncode(["lo", "hi", "lo", missing, "hi"]),
    )
    # Two partitions -> two record batches (plus dictionary batches).
    io = IOBuffer()
    Arrow.write(io, Tables.partitioner([expected, expected]); file=false)
    bytes = take!(io)
    println("2.x-written stream: $(length(bytes)) bytes")

    stream = readstream(bytes)
    println("decoded: $(length(stream.batches)) record batches, " *
            "$(length(stream.schema.fields)) columns")
    @assert length(stream.batches) == 2

    wanted = (
        ints=Any[1, 2, 3, 4, 5],
        floats=Any[1.5, missing, 3.5, missing, 5.5],
        bools=Any[true, false, true, missing, false],
        strs=Any["hey", "", missing, "αβ∀", "last"],
        lists=Any[[1, 2], Int64[], [3], missing, [4, 5, 6]],
        structs=Any[(a=1, b="x"), (a=2, b="y"), (a=3, b="z"), (a=4, b="w"), (a=5, b="v")],
        dict=Any["lo", "hi", "lo", missing, "hi"],
    )
    for b in stream.batches
        for (i, f) in enumerate(stream.schema.fields)
            got = materialize(f, b.columns[i])
            want = wanted[Symbol(f.name)]
            @assert isequal(collect(Any, got), want) "column $(f.name): got $got, want $want"
        end
    end
    println("all columns round-tripped through ArrowCore ✓")

    # Framing limits actually bite: a 1KB body cap must reject this stream
    # BEFORE any decode work happens.
    caught = try
        readstream(bytes; limits=Limits(max_body_bytes=16))
        false
    catch e
        e isa ValidationError
    end
    @assert caught
    println("stage-1 resource limits reject oversized bodies ✓")

    # Truncation semantics, both halves of the report's append rule:
    # (a) losing only the 8-byte EOS block = boundary truncation, ACCEPTED
    #     (the stream ends after its last complete message);
    # (b) losing bytes of a message body = corruption, a clean framing error
    #     — never a silent empty/short stream (the 2.x behavior) and never
    #     an aliased read.
    boundary = readstream(bytes[1:(end - 8)])
    @assert length(boundary.batches) == 2
    println("boundary truncation (missing EOS) tolerated by design ✓")
    caught = try
        readstream(bytes[1:(end - 100)])
        false
    catch e
        e isa ValidationError
    end
    @assert caught
    println("mid-body truncation is a framing error, not a silent short read ✓")
    println()
    println("adapter size: framing+mapping+decode ≈ 260 lines vs the 2.x")
    println("read path's ~1,100 (10 build methods + Stream/Table duplication)")
end

main()
