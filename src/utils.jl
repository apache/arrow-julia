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

# Determines the total number of bytes needed to store `n` bytes with padding.
# Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.
padding(n::Integer, alignment) = ((n + alignment - 1) ÷ alignment) * alignment

paddinglength(n::Integer, alignment) = padding(n, alignment) - n

function writezeros(io::IO, n::Integer)
    s = 0
    for i ∈ 1:n
        s += Base.write(io, 0x00)
    end
    s
end

if isdefined(Base, :waitall)
    const _waitall = waitall
else
    _waitall(tasks) = foreach(wait, tasks)
end

# efficient writing of arrays
writearray(io, col) = writearray(io, maybemissing(eltype(col)), col)

function _writearrayfallback(io::IO, ::Type{T}, col) where {T}
    n = 0
    data = Vector{UInt8}(undef, sizeof(col))
    buf = IOBuffer(data; write=true)
    for x in col
        n += Base.write(buf, coalesce(x, ArrowTypes.default(T)))
    end
    n = Base.write(io, take!(buf))
    return n
end

@inline function _writearraycontiguous(io::IO, ::Type{T}, data) where {T}
    return Base.unsafe_write(io, pointer(data), sizeof(T) * length(data))
end

@inline function _contiguoustoarrowdata(::Type{T}, col::ArrowTypes.ToArrow) where {T}
    ArrowTypes._needsconvert(col) && return nothing
    data = ArrowTypes._sourcedata(col)
    strides(data) == (1,) || return nothing
    if data isa AbstractVector{T}
        return isbitstype(T) ? data : nothing
    elseif isbitstype(T) && data isa AbstractVector{Union{T,Missing}}
        return data
    end
    return nothing
end

function writearray(io::IO, ::Type{T}, col) where {T}
    if col isa Vector{T}
        n = Base.write(io, col)
    elseif isbitstype(T) && (
        col isa Vector{Union{T,Missing}} || col isa SentinelVector{T,T,Missing,Vector{T}}
    )
        # need to write the non-selector bytes of isbits Union Arrays
        n = Base.unsafe_write(io, pointer(col), sizeof(T) * length(col))
    elseif col isa ChainedVector
        n = 0
        for A in col.arrays
            n += writearray(io, T, A)
        end
    else
        n = _writearrayfallback(io, T, col)
    end
    return n
end

function writearray(io::IO, ::Type{T}, col::ArrowTypes.ToArrow) where {T}
    data = _contiguoustoarrowdata(T, col)
    isnothing(data) || return _writearraycontiguous(io, T, data)
    return _writearrayfallback(io, T, col)
end

getbit(v::UInt8, n::Integer) = (v & (1 << (n - 1))) > 0x00

function setbit(v::UInt8, b::Bool, n::Integer)
    if b
        v | 0x02^(n - 1)
    else
        v & (0xff ⊻ 0x02^(n - 1))
    end
end

# Determines the number of bytes used by `n` bits, optionally with padding.
function bitpackedbytes(n::Integer, alignment)
    ℓ = cld(n, 8)
    return ℓ + paddinglength(ℓ, alignment)
end

# count # of missing elements in an iterable
nullcount(col) = count(ismissing, col)

# like startswith for strings, but on byte buffers
function _startswith(a::AbstractVector{UInt8}, pos::Integer, b::AbstractVector{UInt8})
    for i = 1:length(b)
        @inbounds check = a[pos + i - 1] == b[i]
        check || return false
    end
    return true
end

# read a single element from a byte vector
# copied from read(::IOBuffer, T) in Base
function readbuffer(t::AbstractVector{UInt8}, pos::Integer, ::Type{T}) where {T}
    GC.@preserve t begin
        ptr::Ptr{T} = pointer(t, pos)
        x = unsafe_load(ptr)
    end
end

# given a number of unique values; what dict encoding _index_ type is most appropriate
encodingtype(n) =
    n < div(typemax(Int8), 2) ? Int8 :
    n < div(typemax(Int16), 2) ? Int16 : n < div(typemax(Int32), 2) ? Int32 : Int64

maybemissing(::Type{T}) where {T} = T === Missing ? Missing : Base.nonmissingtype(T)
withmissing(U::Union, S) = U >: Missing ? Union{Missing,S} : S
withmissing(T, S) = T === Missing ? Union{Missing,S} : S

function getfooter(filebytes)
    len = readbuffer(filebytes, length(filebytes) - 9, Int32)
    FlatBuffers.getrootas(Meta.Footer, filebytes[(end - (9 + len)):(end - 10)], 0)
end

function getrb(filebytes)
    f = getfooter(filebytes)
    rb = f.recordBatches[1]
    return filebytes[(rb.offset + 1):(rb.offset + 1 + rb.metaDataLength)]
    # FlatBuffers.getrootas(Meta.Message, filebytes, rb.offset)
end

@inline function messagebytes(msg, alignment)
    metalen = padding(length(msg.msgflatbuf), alignment)
    return 8 + metalen + msg.bodylen
end

function readmessage(filebytes, off=9)
    @assert readbuffer(filebytes, off, UInt32) === 0xFFFFFFFF
    len = readbuffer(filebytes, off + 4, Int32)

    FlatBuffers.getrootas(Meta.Message, filebytes, off + 8)
end

@inline _issinglepartition(parts) = parts isa Tuple && length(parts) == 1

@inline function _directtobuffercoleligible(col)
    T = Base.nonmissingtype(eltype(col))
    T <: AbstractString && return false
    T <: Base.CodeUnits && return false
    K = ArrowTypes.ArrowKind(ArrowTypes.ArrowType(T))
    return !(K isa ArrowTypes.ListKind)
end

@inline function _directtobufferstringonly(col)
    T = Base.nonmissingtype(eltype(col))
    return T <: AbstractString
end

@inline function _directtobufferbinaryonly(col)
    return eltype(col) <: Base.CodeUnits
end

@inline function _directstreamcoleligible(col)
    return !(col isa DictEncode) &&
           DataAPI.refarray(col) === col &&
           (_directtobufferstringonly(col) || _directtobufferbinaryonly(col))
end

function _directtobuffereligible(part)
    tblcols = Tables.columns(part)
    sch = Tables.schema(tblcols)
    ncols = 0
    singlecolspecial = false
    allnonstrings = true
    Tables.eachcolumn(sch, tblcols) do col, _, _
        ncols += 1
        eligible = _directtobuffercoleligible(col)
        allnonstrings &= eligible
        singlecolspecial =
            ncols == 1 && (_directtobufferstringonly(col) || _directtobufferbinaryonly(col))
    end
    return allnonstrings || (ncols == 1 && singlecolspecial)
end

@inline function _directstreameligible(part)
    tblcols = Tables.columns(part)
    sch = Tables.schema(tblcols)
    ncols = 0
    singlecolspecial = false
    Tables.eachcolumn(sch, tblcols) do col, _, _
        ncols += 1
        singlecolspecial = ncols == 1 && _directstreamcoleligible(col)
    end
    return ncols == 1 && singlecolspecial
end

@inline _partitionsinspectable(parts) =
    parts isa Tuple || parts isa AbstractVector || parts isa Tables.Partitioner

@inline function _directtobuffersizehint(
    cols,
    dictmsgs,
    schmsg,
    recbatchmsg,
    endmsg,
    alignment,
)
    for col in Tables.Columns(cols)
        if col isa Map
            return
            messagebytes(schmsg, alignment) +
            sum(msg -> messagebytes(msg, alignment), dictmsgs; init=0) +
            messagebytes(recbatchmsg, alignment) +
            messagebytes(endmsg, alignment)
        end
    end
    return nothing
end

function _writedictionarymessages!(io, blocks, schref, alignment, dictencodings)
    isempty(dictencodings) && return
    des = sort!(collect(dictencodings); by=x -> x.first, rev=true)
    for (id, delock) in des
        de = delock.value
        dictsch = Tables.Schema((:col,), (eltype(de.data),))
        msg = makedictionarybatchmsg(dictsch, (col=de.data,), id, false, alignment)
        Base.write(io, msg, blocks, schref, alignment)
    end
    return
end

function _writedictionarydeltas!(io, blocks, schref, alignment, deltas)
    isempty(deltas) && return
    for de in deltas
        dictsch = Tables.Schema((:col,), (eltype(de.data),))
        msg = makedictionarybatchmsg(dictsch, (col=de.data,), de.id, true, alignment)
        Base.write(io, msg, blocks, schref, alignment)
    end
    return
end

@inline function _directstreamstate(parts)
    _partitionsinspectable(parts) || return nothing
    firststate = iterate(parts)
    isnothing(firststate) && return nothing
    firstpart, state = firststate
    isnothing(iterate(parts, state)) && return nothing
    return firstpart, state
end

function _directtobuffer(part, source, kwargs)
    largelists = get(kwargs, :largelists, false)
    compress = get(kwargs, :compress, nothing)
    denseunions = get(kwargs, :denseunions, true)
    dictencode = get(kwargs, :dictencode, false)
    dictencodenested = get(kwargs, :dictencodenested, false)
    alignment = Int32(get(kwargs, :alignment, 8))
    maxdepth = get(kwargs, :maxdepth, DEFAULT_MAX_DEPTH)
    metadata = get(kwargs, :metadata, getmetadata(source))
    colmetadata = get(kwargs, :colmetadata, nothing)

    tblcols = Tables.columns(part)
    dictencodings = Dict{Int64,Any}()
    cols = toarrowtable(
        tblcols,
        dictencodings,
        largelists,
        compress,
        denseunions,
        dictencode,
        dictencodenested,
        maxdepth,
        metadata,
        colmetadata,
    )
    sch = Tables.schema(cols)
    schmsg = makeschemamsg(sch, cols)
    dictmsgs = if isempty(dictencodings)
        Message[]
    else
        des = sort!(collect(dictencodings); by=x -> x.first, rev=true)
        [
            begin
                de = delock.value
                dictsch = Tables.Schema((:col,), (eltype(de.data),))
                makedictionarybatchmsg(dictsch, (col=de.data,), id, false, alignment)
            end for (id, delock) in des
        ]
    end
    recbatchmsg = makerecordbatchmsg(sch, cols, alignment)
    endmsg = Message(UInt8[], nothing, 0, true, false, Meta.Schema)
    sizehint =
        _directtobuffersizehint(cols, dictmsgs, schmsg, recbatchmsg, endmsg, alignment)
    io = isnothing(sizehint) ? IOBuffer() : IOBuffer(; sizehint=sizehint)
    blocks = (Block[], Block[])
    schref = Ref(sch)
    Base.write(io, schmsg, blocks, schref, alignment)
    foreach(msg -> Base.write(io, msg, blocks, schref, alignment), dictmsgs)
    Base.write(io, recbatchmsg, blocks, schref, alignment)
    Base.write(io, endmsg, blocks, schref, alignment)
    seekstart(io)
    return io
end

function _directstreamwrite!(io::IO, firstpart, state, parts, source, kwargs)
    largelists = get(kwargs, :largelists, false)
    compress = get(kwargs, :compress, nothing)
    denseunions = get(kwargs, :denseunions, true)
    dictencode = get(kwargs, :dictencode, false)
    dictencodenested = get(kwargs, :dictencodenested, false)
    alignment = Int32(get(kwargs, :alignment, 8))
    maxdepth = get(kwargs, :maxdepth, DEFAULT_MAX_DEPTH)
    metadata = get(kwargs, :metadata, getmetadata(source))
    colmetadata = get(kwargs, :colmetadata, nothing)

    dictencodings = Dict{Int64,Any}()
    firstcols = toarrowtable(
        Tables.columns(firstpart),
        dictencodings,
        largelists,
        compress,
        denseunions,
        dictencode,
        dictencodenested,
        maxdepth,
        metadata,
        colmetadata,
    )
    sch = Tables.schema(firstcols)
    schmsg = makeschemamsg(sch, firstcols)
    blocks = (Block[], Block[])
    schref = Ref(sch)
    Base.write(io, schmsg, blocks, schref, alignment)
    _writedictionarymessages!(io, blocks, schref, alignment, dictencodings)
    Base.write(io, makerecordbatchmsg(sch, firstcols, alignment), blocks, schref, alignment)

    next = iterate(parts, state)
    while !isnothing(next)
        part, state = next
        cols = toarrowtable(
            Tables.columns(part),
            dictencodings,
            largelists,
            compress,
            denseunions,
            dictencode,
            dictencodenested,
            maxdepth,
            metadata,
            colmetadata,
        )
        Tables.schema(cols) == sch ||
            throw(ArgumentError("all partitions must have the exact same Tables.Schema"))
        _writedictionarydeltas!(io, blocks, schref, alignment, cols.dictencodingdeltas)
        Base.write(io, makerecordbatchmsg(sch, cols, alignment), blocks, schref, alignment)
        next = iterate(parts, state)
    end
    Base.write(
        io,
        Message(UInt8[], nothing, 0, true, false, Meta.Schema),
        blocks,
        schref,
        alignment,
    )
    return io
end

function _directstreamtobuffer(firstpart, state, parts, source, kwargs)
    io = IOBuffer()
    _directstreamwrite!(io, firstpart, state, parts, source, kwargs)
    seekstart(io)
    return io
end

function tobuffer(data; kwargs...)
    parts = Tables.partitions(data)
    if !get(kwargs, :file, false)
        if _issinglepartition(parts) && _directtobuffereligible(parts[1])
            return _directtobuffer(parts[1], data, kwargs)
        else
            streamstate = _directstreamstate(parts)
            if !isnothing(streamstate)
                firstpart, state = streamstate
                _directstreameligible(firstpart) &&
                    return _directstreamtobuffer(firstpart, state, parts, data, kwargs)
            end
        end
    end
    io = IOBuffer()
    write(io, data; kwargs...)
    seekstart(io)
    return io
end

toidict(x::Base.ImmutableDict) = x

# ref https://github.com/apache/arrow-julia/pull/238#issuecomment-919415809
function toidict(pairs)
    isempty(pairs) && return Base.ImmutableDict{String,String}()
    dict = Base.ImmutableDict(first(pairs))
    for pair in Iterators.drop(pairs, 1)
        dict = Base.ImmutableDict(dict, pair)
    end
    return dict
end
