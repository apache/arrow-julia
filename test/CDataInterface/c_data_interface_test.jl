using Arrow, PyCall
pd = pyimport("pandas")
pa = pyimport("pyarrow")
##
df = pd.DataFrame(py"""{'a': [1, 2, 3, 4, 5], 'b': ['a', 'b', 'c', 'd', 'e']}"""o)
rb = pa.record_batch(df)
sch = Arrow.CDataInterface.get_schema() do ptr
    rb.schema._export_to_c(Int(ptr))
end
arr = Arrow.CDataInterface.get_array() do ptr
    rb._export_to_c(Int(ptr))
end
##

pyarr = pa.array(py"""[1,2,3]"""o)
rb = pa.record_batch(pyarr)
sch = Arrow.CDataInterface.get_schema() do ptr
    rb.schema._export_to_c(Int(ptr))
end
arr = Arrow.CDataInterface.get_array() do ptr
    rb._export_to_c(Int(ptr))
end
##
Arrow.CDataInterface.get_type_from_format_string(sch.children[1].format)
##
arr.children[1]