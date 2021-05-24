using Arrow, PyCall, Random
pd = pyimport("pandas")
pa = pyimport("pyarrow")
##
df = pd.DataFrame(Dict(
    "ints" => map(x -> rand() < 0.5 ? rand(1:10) : nothing, 1:1_000_000), 
    #"strings" => map(x -> rand() < 0.5 ? randstring(12) : nothing, 1:1_000_000)
))
rb = pa.record_batch(df)

c_arrow_schema = Arrow.CDataInterface.get_schema() do ptr
    rb.schema._export_to_c(Int(ptr))
end
c_arrow_array = Arrow.CDataInterface.get_array() do ptr
    rb._export_to_c(Int(ptr))
end
##
Arrow.CDataInterface.convert_to_jl_arrow(c_arrow_array.children[1], c_arrow_schema.children[1])
##