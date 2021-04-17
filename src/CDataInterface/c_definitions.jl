const ARROW_FLAG_DICTIONARY_ORDERED = 1
const ARROW_FLAG_NULLABLE = 2
const ARROW_FLAG_MAP_KEYS_SORTED = 4

struct CArrowSchema
    format::Ptr{UInt8}
    name::Ptr{UInt8}
    metadata::Ptr{UInt8}
    flags::Int64
    n_children::Int64
    children::Ptr{Ptr{CArrowSchema}}
    dictionary::Ptr{CArrowSchema}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

CArrowSchema() = CArrowSchema(C_NULL, C_NULL, C_NULL, 0, 0, C_NULL, C_NULL, _CNULL, C_NULL)

Base.propertynames(::CArrowSchema) = (:format, :name, :metadata, :flags, :n_children, :children, :dictionary)

function read_c_arrow_schema_metadata(ptr::Ptr{UInt8})
    pos = 1
    meta = Dict{String, String}()
    if ptr != C_NULL
        n_entries = unsafe_load(convert(Ptr{Int32}, ptr))
        ptr += 4
        for _ = 1:n_entries
            keylen = unsafe_load(convert(Ptr{Int32}, ptr))
            ptr += 4
            key = unsafe_string(ptr, keylen)
            ptr += keylen
            vallen = unsafe_load(convert(Ptr{Int32}, ptr))
            ptr += 4
            val = unsafe_string(ptr, vallen)
            ptr += vallen
            meta[key] = val
        end
    end
    return meta
end

function Base.getproperty(x::CArrowSchema, nm::Symbol)
    if nm === :format
        return unsafe_string(getfield(x, :format))
    elseif nm === :name
        return unsafe_string(getfield(x, :name))
    elseif nm === :metadata
        return read_c_arrow_schema_metadata(getfield(x, :metadata))
    elseif nm === :flags
        return getfield(x, :flags)
    elseif nm === :n_children
        return getfield(x, :n_children)
    elseif nm === :children
        c = getfield(x, :children)
        return c == C_NULL ? CArrowSchema[] : unsafe_wrap(Array, unsafe_load(c), getfield(x, :n_children))
    elseif nm === :dictionary
        d = getfield(x, :dictionary)
        return d == C_NULL ? nothing : unsafe_load(d)
    end
    error("unknown property requested: $nm")
end

struct CArrowArray
    length::Int64
    null_count::Int64
    offset::Int64
    n_buffers::Int64
    n_children::Int64
    buffers::Ptr{Ptr{UInt8}}
    children::Ptr{Ptr{CArrowArray}}
    dictionary::Ptr{CArrowArray}
    release::Ptr{Cvoid}
    private_data::Ptr{Cvoid}
end

CArrowArray() = CArrowArray(0, 0, 0, 0, 0, C_NULL, C_NULL, C_NULL, C_NULL, C_NULL)

Base.propertynames(::CArrowArray) = (:length, :null_count, :offset, :n_buffers, :n_children, :buffers, :children, :dictionary)

function Base.getproperty(x::CArrowArray, nm::Symbol)
    if nm === :length
        return getfield(x, :length)
    elseif nm === :null_count
        return getfield(x, :null_count)
    elseif nm === :offset
        return getfield(x, :offset)
    elseif nm === :n_buffers
        return getfield(x, :n_buffers)
    elseif nm === :n_children
        return getfield(x, :n_children)
    elseif nm === :buffers
        b = getfield(x, :buffers)
        return b == C_NULL ? Ptr{UInt8}[] : unsafe_wrap(Array, b, getfield(x, :n_buffers))
    elseif nm === :children
        c = getfield(x, :children)
        return c == C_NULL ? CArrowArray[] : unsafe_wrap(Array, unsafe_load(c), getfield(x, :n_children))
    elseif nm === :dictionary
        d = getfield(x, :dictionary)
        return d == C_NULL ? nothing : unsafe_load(d)
    end
    error("unknown property requested: $nm")
end