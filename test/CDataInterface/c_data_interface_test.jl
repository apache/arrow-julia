using Arrow, PyCall
pd = pyimport("pandas")
pa = pyimport("pyarrow")
##
df = pd.DataFrame(py"""{'a': [1, None, 3, 4, 5], 'b': ['a', 'b', None, 'd', 'e']}"""o)

rb = pa.record_batch(df)
c_arrow_schema = Arrow.CDataInterface.get_schema() do ptr
    rb.schema._export_to_c(Int(ptr))
end
c_arrow_array = Arrow.CDataInterface.get_array() do ptr
    rb._export_to_c(Int(ptr))
end
##
T = Arrow.CDataInterface.get_type_from_format_string(c_arrow_schema.children[1].format)
length = c_arrow_array.children[1].length
arrow_data_buffer = Base.unsafe_wrap(Array, c_arrow_array.children[1].buffers[2], cld(length * 64, 8))
validity_bytes = Base.unsafe_wrap(Array, c_arrow_array.children[1].buffers[1], cld(length, 8))
validity_bitmap = Arrow.ValidityBitmap(validity_bytes, 1, length, c_arrow_array.children[1].null_count)
##
data = reinterpret(T, arrow_data_buffer)
metadata = nothing
Arrow.Primitive{T, Vector{T}}(arrow_data_buffer, validity_bitmap, data, length, metadata)