using Arrow, PyCall, Random
pd = pyimport("pandas")
pa = pyimport("pyarrow")
##
df = pd.DataFrame(Dict(
    "ints" => map(x -> rand() < 0.5 ? rand(1:10) : nothing, 1:1_000_000), 
    "strings" => map(x -> rand() < 0.5 ? randstring(12) : nothing, 1:1_000_000)
))

rb = pa.record_batch(df)
c_arrow_schema = Arrow.CDataInterface.get_schema() do ptr
    rb.schema._export_to_c(Int(ptr))
end
c_arrow_array = Arrow.CDataInterface.get_array() do ptr
    rb._export_to_c(Int(ptr))
end
##
T = Arrow.CDataInterface.get_type_from_format_string(c_arrow_schema.children[2].format)
length = c_arrow_array.children[2].length
arrow_data_buffer = Base.unsafe_wrap(Array, c_arrow_array.children[2].buffers[2], cld(length * 64, 8))
validity_bytes = Base.unsafe_wrap(Array, c_arrow_array.children[2].buffers[1], cld(length, 8))
@time validity_bitmap = Arrow.ValidityBitmap(validity_bytes, 1, length, c_arrow_array.children[2].null_count)
data = reinterpret(T, arrow_data_buffer)
metadata = nothing
@time Arrow.Primitive{T, AbstractVector{T}}(arrow_data_buffer, validity_bitmap, data, length, metadata)
##
T = Arrow.CDataInterface.get_type_from_format_string(c_arrow_schema.children[1].format)
length = c_arrow_array.children[1].length
offsets_buffer_binary = Base.unsafe_wrap(
    Array, 
    c_arrow_array.children[1].buffers[2], 
    cld((length + 1) * 32, 8))
offsets = Arrow.Offsets{Int32}(
    offsets_buffer_binary, 
    reinterpret(Int32, offsets_buffer_binary))
##
arrow_data_buffer = Base.unsafe_wrap(Array, c_arrow_array.children[1].buffers[3], offsets_buffer |> last)
##
validity_bytes = Base.unsafe_wrap(Array, c_arrow_array.children[1].buffers[1], cld(length, 8))
validity_bitmap = Arrow.ValidityBitmap(validity_bytes, 1, length, c_arrow_array.children[1].null_count)
##
metadata = nothing
@time Arrow.List{T, Int32, AbstractVector{UInt8}}(arrow_data_buffer, validity_bitmap, offsets, arrow_data_buffer, length, metadata)