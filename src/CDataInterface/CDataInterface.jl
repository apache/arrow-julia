module CDataInterface

import ..Arrow
include("c_definitions.jl")
include("jl_definitions.jl")
include("format_string_to_types.jl")

export ArrowSchema, ArrowArray, getschema, getarray

function get_schema(f)
    schema_ref = Ref{CArrowSchema}()
    ptr = Base.unsafe_convert(Ptr{CArrowSchema}, schema_ref)
    f(ptr)
    sch = ArrowSchema(schema_ref)
    finalizer(sch) do x
        r = getfield(x.c_arrow_schema[], :release)
        if r != C_NULL
            ccall(r, Cvoid, (Ptr{CArrowSchema},), x.carrowschema)
        end
    end
    return sch
end

function get_array(f)
    arr_ref = Ref{CArrowArray}()
    ptr = Base.unsafe_convert(Ptr{CArrowArray}, arr_ref)
    f(ptr)
    arr = ArrowArray(arr_ref)
    finalizer(arr) do x
        r = getfield(x.c_arrow_array[], :release)
        if r != C_NULL
            ccall(r, Cvoid, (Ptr{CArrowArray},), x.c_arrow_array)
        end
    end
    return arr
end

end # module
