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

# efficient writing of arrays
writearray(io, col) = writearray(io, maybemissing(eltype(col)), col)

function writearray(io::IO, ::Type{T}, col) where {T}
    if col isa Vector{T}
        n = Base.write(io, col)
    elseif isbitstype(T) && (col isa Vector{Union{T, Missing}} || col isa SentinelVector{T, T, Missing, Vector{T}})
        # need to write the non-selector bytes of isbits Union Arrays
        n = Base.unsafe_write(io, pointer(col), sizeof(T) * length(col))
    elseif col isa ChainedVector
        n = 0
        for A in col.arrays
            n += writearray(io, T, A)
        end
    else
        n = 0
        data = Vector{UInt8}(undef, sizeof(col))
        buf = IOBuffer(data; write=true)
        for x in col
            n += Base.write(buf, coalesce(x, ArrowTypes.default(T)))
        end
        n = Base.write(io, take!(buf))
    end
    return n
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
encodingtype(n) = n < div(typemax(Int8), 2) ? Int8 : n < div(typemax(Int16), 2) ? Int16 : n < div(typemax(Int32), 2) ? Int32 : Int64

maybemissing(::Type{T}) where {T} = T === Missing ? Missing : Base.nonmissingtype(T)
withmissing(U::Union, S) = U >: Missing ? Union{Missing, S} : S
withmissing(T, S) = T === Missing ? Union{Missing, S} : S

function getfooter(filebytes)
    len = readbuffer(filebytes, length(filebytes) - 9, Int32)
    FlatBuffers.getrootas(Meta.Footer, filebytes[end-(9 + len):end-10], 0)
end

function getrb(filebytes)
    f = getfooter(filebytes)
    rb = f.recordBatches[1]
    return filebytes[rb.offset+1:(rb.offset+1+rb.metaDataLength)]
    # FlatBuffers.getrootas(Meta.Message, filebytes, rb.offset)
end

function readmessage(filebytes, off=9)
    @assert readbuffer(filebytes, off, UInt32) === 0xFFFFFFFF
    len = readbuffer(filebytes, off + 4, Int32)

    FlatBuffers.getrootas(Meta.Message, filebytes, off + 8)
end

function tobuffer(data; kwargs...)
    io = IOBuffer()
    write(io, data; kwargs...)
    seekstart(io)
    return io
end

toidict(x::Base.ImmutableDict) = x

# ref https://github.com/JuliaData/Arrow.jl/pull/238#issuecomment-919415809
function toidict(pairs)
    isempty(pairs) && return Base.ImmutableDict{String, String}()
    dict = Base.ImmutableDict(first(pairs))
    for pair in Iterators.drop(pairs, 1)
        dict = Base.ImmutableDict(dict, pair)
    end
    return dict
end
