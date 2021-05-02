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

mutable struct InterimCArrowArray
    length ::Int64
    null_count ::Int64
    offset ::Int64
    n_buffers ::Int64
    n_children ::Int64
    buffers ::Vector{Ptr{UInt8}}
    children ::Vector{InterimCArrowArray}
    dictionary ::Union{Nothing,InterimCArrowArray}
    c_arrow_array ::Ref{CArrowArray}
end

InterimCArrowArray(a::Ref{CArrowArray}) = InterimCArrowArray(
    a[].length, 
    a[].null_count, 
    a[].offset, 
    a[].n_buffers, 
    a[].n_children, 
    a[].buffers, 
    map(InterimCArrowArray, a[].children), 
    a[].dictionary === nothing ? nothing : InterimCArrowArray(a[].dictionary), 
    a
)

InterimCArrowArray(a::CArrowArray) = InterimCArrowArray(
    a.length, 
    a.null_count, 
    a.offset, 
    a.n_buffers, 
    a.n_children, 
    a.buffers, 
    map(InterimCArrowArray, a.children), 
    a.dictionary === nothing ? nothing : InterimCArrowArray(a.dictionary), 
    Ref{CArrowArray}()
)
