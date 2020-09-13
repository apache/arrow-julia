
"""
    padding(n::Integer)

Determines the total number of bytes needed to store `n` bytes with padding.
Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.
"""
padding(n::Integer) = ((n + ALIGNMENT - 1) ÷ ALIGNMENT) * ALIGNMENT

paddinglength(n::Integer) = padding(n) - n

function writezeros(io::IO, n::Integer)
    s = 0
    for i ∈ 1:n
        s += Base.write(io, 0x00)
    end
    s
end

# efficient writing of arrays
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
        for x in col
            n += Base.write(io, coalesce(x, default(T)))
        end
    end
    return n
end

"""
    getbit

This deliberately elides bounds checking.
"""
getbit(v::UInt8, n::Integer) = Bool((v & 0x02^(n - 1)) >> (n - 1))

"""
    setbit

This also deliberately elides bounds checking.
"""
function setbit(v::UInt8, b::Bool, n::Integer)
    if b
        v | 0x02^(n - 1)
    else
        v & (0xff ⊻ 0x02^(n - 1))
    end
end

"""
    bitpackedbytes(n[, pad=true])

Determines the number of bytes used by `n` bits, optionally with padding.
"""
function bitpackedbytes(n::Integer, pad::Bool=true)
    a, b = divrem(n, 8)
    ℓ = a + (b > 0)
    pad && (ℓ += paddinglength(ℓ))
    return ℓ
end

# count # of missing elements in an iterable
nullcount(col) = count(ismissing, col)

# like startswith/endswith for strings, but on byte buffers
function _startswith(a::AbstractVector{UInt8}, pos::Integer, b::AbstractVector{UInt8})
    for i = 1:length(b)
        @inbounds check = a[pos + i - 1] == b[i]
        check || return false
    end
    return true
end

function _endswith(a::AbstractVector{UInt8}, endpos::Integer, b::AbstractVector{UInt8})
    aoff = endpos - length(b) + 1
    for i = 1:length(b)
        @inbounds check = a[aoff] == b[i]
        check || return false
        aoff += 1
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

flatten(x) = Iterators.flatten(x)

# we need to treat missing specially w/ length of flattened array
_length(x) = length(x)
_length(g::Base.Generator) = _length(g.iter)
_length(x::Iterators.Flatten) = sum(i -> i === missing ? 1 : _length(i), x)

# argh me mateys, don't nobody else go pirating this method
# this here be me own booty!
if !applicable(iterate, missing)
Base.iterate(::Missing, st=1) = st === nothing ? nothing : (missing, nothing)
end

ntupleT(::Type{NTuple{N, T}}) where {N, T} = T
ntnames(::Type{NamedTuple{names, T}}) where {names, T} = names
ntT(::Type{NamedTuple{names, T}}) where {names, T} = T
pairK(::Type{Pair{K, V}}) where {K, V} = K
pairV(::Type{Pair{K, V}}) where {K, V} = V

# need a custom representation of Union types since arrow unions
# are ordered, and possibly indirected via separate typeIds array
# here, T is Meta.UnionMode.Dense or Meta.UnionMode.Sparse,
# typeIds is a NTuple{N, Int32}, and U is a Tuple{...} of the
# unioned types
struct UnionT{T, typeIds, U}
end

unionmode(::Type{UnionT{T, typeIds, U}}) where {T, typeIds, U} = T
typeids(::Type{UnionT{T, typeIds, U}}) where {T, typeIds, U} = typeIds
Base.eltype(::Type{UnionT{T, typeIds, U}}) where {T, typeIds, U} = U

# convenience wrappers for signaling that an array shoudld be written
# as with dense/sparse union arrow buffers
struct DenseUnionVector{T, U} <: AbstractVector{UnionT{Meta.UnionMode.Dense, nothing, U}}
    itr::T
end

DenseUnionVector(x::T) where {T} = DenseUnionVector{T, Tuple{eachunion(eltype(x))...}}(x)
Base.IndexStyle(::Type{<:DenseUnionVector}) = Base.IndexLinear()
Base.size(x::DenseUnionVector) = (length(x.itr),)
Base.eltype(x::DenseUnionVector{T, U}) where {T, U} = UnionT{Meta.UnionMode.Dense, nothing, U}
Base.iterate(x::DenseUnionVector, st...) = iterate(x.itr, st...)
Base.getindex(x::DenseUnionVector, i::Int) = getindex(x.itr, i)

struct SparseUnionVector{T, U} <: AbstractVector{UnionT{Meta.UnionMode.Sparse, nothing, U}}
    itr::T
end

SparseUnionVector(x::T) where {T} = SparseUnionVector{T, Tuple{eachunion(eltype(x))...}}(x)
Base.IndexStyle(::Type{<:SparseUnionVector}) = Base.IndexLinear()
Base.size(x::SparseUnionVector) = (length(x.itr),)
Base.eltype(x::SparseUnionVector{T, U}) where {T, U} = UnionT{Meta.UnionMode.Sparse, nothing, U}
Base.iterate(x::SparseUnionVector, st...) = iterate(x.itr, st...)
Base.getindex(x::SparseUnionVector, i::Int) = getindex(x.itr, i)

# iterate a Julia Union{...} type, producing an array of unioned types
function eachunion(U::Union, elems=nothing)
    if elems === nothing
        return eachunion(U.b, Type[U.a])
    else
        push!(elems, U.a)
        return eachunion(U.b, elems)
    end
end

function eachunion(T, elems)
    push!(elems, T)
    return elems
end

# dense union child array producer
# for dense union children, we split the parent array
# into N children arrays, each with one of the unioned types
# only the 1st child needs to include missing values
struct Filtered{T, I}
    itr::I
    len::Int64
end

function filtered(::Type{T}, itr::I) where {T, I}
    len = Int64(0)
    for x in itr
        len += x isa T
    end
    return Filtered{T, I}(itr, len)
end

Base.length(f::Filtered) = f.len
Base.eltype(f::Filtered{T}) where {T} = T

function Base.iterate(f::Filtered{T}, st=()) where {T}
    st = iterate(f.itr, st...)
    st === nothing && return nothing
    x, state = st
    while !(x isa T)
        st = iterate(f.itr, state)
        st === nothing && return nothing
        x, state = st
    end
    return x, (state,)
end

# sparse union child array producer
# for sparse unions, we split the parent array into
# N children arrays, each having the same length as the parent
# but with one child array per unioned type; each child
# should include the elements from parent of its type
# and other elements can be missing/default
struct Replaced{T, I}
    itr::I
end

replaced(::Type{T}, itr::I) where {T, I} = Replaced{T, I}(itr)

Base.length(r::Replaced) = _length(r.itr)
Base.eltype(r::Replaced{T}) where {T} = Union{T, Missing}

function Base.iterate(r::Replaced{T}, st=()) where {T}
    st = iterate(r.itr, st...)
    st === nothing && return nothing
    x, state = st
    return ifelse(x isa T, x, missing), (state,)
end

# convenience wrapper to signal that an input column should be
# dict encoded when written to the arrow format
# note that only top-level columns are supported for dict encoding
# currently; (i.e. no nested dict encoding)
struct DictEncode{T, A <: AbstractVector} <: AbstractVector{T}
    data::A
end

DictEncode(x::A) where {A} = DictEncode{eltype(A), A}(x)
Base.IndexStyle(::Type{<:DictEncode}) = Base.IndexLinear()
Base.size(x::DictEncode) = (length(x.data),)
Base.eltype(x::DictEncode{T, A}) where {T, A} = T
Base.iterate(x::DictEncode, st...) = iterate(x.data, st...)
Base.getindex(x::DictEncode, i::Int) = getindex(x.data, i)

encodingtype(n) = n < div(typemax(Int8), 2) ? Int8 : n < div(typemax(Int16), 2) ? Int16 : n < div(typemax(Int32), 2) ? Int32 : Int64

struct Converter{T, A} <: AbstractVector{T}
    data::A
end

converter(::Type{T}, x::A) where {T, A} = Converter{eltype(A) >: Missing ? Union{T, Missing} : T, A}(x)
converter(::Type{T}, x::ChainedVector{A}) where {T, A} = ChainedVector([converter(T, x) for x in x.arrays])

Base.IndexStyle(::Type{<:Converter}) = Base.IndexLinear()
Base.size(x::Converter) = (length(x.data),)
Base.eltype(x::Converter{T, A}) where {T, A} = T
Base.getindex(x::Converter{T}, i::Int) where {T} = convert(T, getindex(x.data, i))
Base.getindex(x::Converter{Symbol, A}, i::Int) where {T, A <: AbstractVector{String}} = Symbol(getindex(x.data, i))
Base.getindex(x::Converter{Char, A}, i::Int) where {T, A <: AbstractVector{String}} = getindex(x.data, i)[1]
Base.getindex(x::Converter{String, A}, i::Int) where {T, A <: AbstractVector{Symbol}} = String(getindex(x.data, i))
Base.getindex(x::Converter{String, A}, i::Int) where {T, A <: AbstractVector{Char}} = string(getindex(x.data, i))
DataAPI.refarray(x::Converter) = DataAPI.refarray(x.data)
DataAPI.refpool(x::Converter{T}) where {T} = converter(T, DataAPI.refpool(x.data))

maybemissing(::Type{T}) where {T} = T === Missing ? Missing : Base.nonmissingtype(T)

macro miss_or(x, ex)
    esc(:($x === missing ? missing : $(ex)))
end

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
    @show len
    FlatBuffers.getrootas(Meta.Message, filebytes, off + 8)
end