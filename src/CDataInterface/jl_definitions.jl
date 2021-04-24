mutable struct ArrowSchema
    format ::String
    name ::String
    metadata ::Dict{String,String}
    flags ::Int64
    n_children ::Int64
    children ::Vector{ArrowSchema}
    dictionary ::Union{Nothing,ArrowSchema}
    c_arrow_schema ::Ref{CArrowSchema}
end

ArrowSchema(s::Ref{CArrowSchema}) = ArrowSchema(
    s[].format, 
    s[].name, 
    s[].metadata, 
    s[].flags, 
    s[].n_children, 
    map(ArrowSchema, s[].children),
    s[].dictionary === nothing ? nothing : ArrowSchema(s[].dictionary), 
    s
)

ArrowSchema(s::CArrowSchema) = ArrowSchema(
    s.format, 
    s.name, 
    s.metadata, 
    s.flags, 
    s.n_children, 
    map(ArrowSchema, s.children), s.dictionary === nothing ? nothing : ArrowSchema(s.dictionary), 
    Ref{CArrowSchema}()
)

mutable struct ArrowArray
    length ::Int64
    null_count ::Int64
    offset ::Int64
    n_buffers ::Int64
    n_children ::Int64
    buffers ::Vector{Ptr{UInt8}}
    children ::Vector{ArrowArray}
    dictionary ::Union{Nothing,ArrowArray}
    c_arrow_array ::Ref{CArrowArray}
end

ArrowArray(a::Ref{CArrowArray}) = ArrowArray(
    a[].length, 
    a[].null_count, 
    a[].offset, 
    a[].n_buffers, 
    a[].n_children, 
    a[].buffers, 
    map(ArrowArray, a[].children), 
    a[].dictionary === nothing ? nothing : ArrowArray(a[].dictionary), 
    a
)

ArrowArray(a::CArrowArray) = ArrowArray(
    a.length, 
    a.null_count, 
    a.offset, 
    a.n_buffers, 
    a.n_children, 
    a.buffers, 
    map(ArrowArray, a.children), 
    a.dictionary === nothing ? nothing : ArrowArray(a.dictionary), 
    Ref{CArrowArray}()
)
