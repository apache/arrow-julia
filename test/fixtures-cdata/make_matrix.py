# Builds a pyarrow table covering the C data type matrix and writes it as IPC stream bytes.
import datetime as dt, decimal, sys
import pyarrow as pa

n = 7
def nul(xs):  # every third value null
    return [None if i % 3 == 1 else x for i, x in enumerate(xs)]

cols = {}
for t in [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]:
    cols[f"{t}"] = pa.array(nul(list(range(n))), type=t)
cols["float16"] = pa.array(nul([1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]), type=pa.float32()).cast(pa.float16())
cols["float32"] = pa.array(nul([1.5 * i for i in range(n)]), type=pa.float32())
cols["float64"] = pa.array(nul([1.25 * i for i in range(n)]), type=pa.float64())
cols["bool"] = pa.array(nul([i % 2 == 0 for i in range(n)]), type=pa.bool_())
cols["null"] = pa.nulls(n)
words = ["a", "", "héllo", "a much longer string than twelve bytes", "x", "yy", "zzz"]
cols["utf8"] = pa.array(nul(words), type=pa.string())
cols["large_utf8"] = pa.array(nul(words), type=pa.large_string())
cols["utf8_view"] = pa.array(nul(words), type=pa.string_view())
cols["binary"] = pa.array(nul([w.encode() for w in words]), type=pa.binary())
cols["large_binary"] = pa.array(nul([w.encode() for w in words]), type=pa.large_binary())
cols["binary_view"] = pa.array(nul([w.encode() for w in words]), type=pa.binary_view())
cols["fixed_binary3"] = pa.array(nul([bytes([i, i, i]) for i in range(n)]), type=pa.binary(3))
cols["date32"] = pa.array(nul([dt.date(2020, 1, 1 + i) for i in range(n)]), type=pa.date32())
cols["date64"] = pa.array(nul([dt.date(2020, 1, 1 + i) for i in range(n)]), type=pa.date64())
cols["time32s"] = pa.array(nul([i * 60 for i in range(n)]), type=pa.time32("s"))
cols["time32ms"] = pa.array(nul([i * 60000 for i in range(n)]), type=pa.time32("ms"))
cols["time64us"] = pa.array(nul([i * 1000 for i in range(n)]), type=pa.time64("us"))
cols["time64ns"] = pa.array(nul([i * 1000 for i in range(n)]), type=pa.time64("ns"))
for u in ["s", "ms", "us", "ns"]:
    cols[f"ts_{u}"] = pa.array(nul([i * 1000 for i in range(n)]), type=pa.timestamp(u))
cols["ts_ns_utc"] = pa.array(nul([i * 10**12 for i in range(n)]), type=pa.timestamp("ns", tz="UTC"))
cols["ts_us_ny"] = pa.array(nul([i * 10**9 for i in range(n)]), type=pa.timestamp("us", tz="America/New_York"))
cols["ts_ms_offset"] = pa.array(nul([i * 10**6 for i in range(n)]), type=pa.timestamp("ms", tz="+05:30"))
for u in ["s", "ms", "us", "ns"]:
    cols[f"dur_{u}"] = pa.array(nul([i * 7 for i in range(n)]), type=pa.duration(u))
cols["interval_mdn"] = pa.array(nul([pa.scalar((i, i + 1, i * 1000), type=pa.month_day_nano_interval()).as_py() for i in range(n)]), type=pa.month_day_nano_interval())
cols["decimal128"] = pa.array(nul([decimal.Decimal(f"{i}.{i}5") for i in range(n)]), type=pa.decimal128(10, 3))
cols["decimal256"] = pa.array(nul([decimal.Decimal(f"-{i}.{i}5") for i in range(n)]), type=pa.decimal256(40, 3))
cols["decimal32"] = pa.array(nul([decimal.Decimal(f"{i}.5") for i in range(n)]), type=pa.decimal32(5, 1))
cols["decimal64"] = pa.array(nul([decimal.Decimal(f"{i}.25") for i in range(n)]), type=pa.decimal64(12, 2))
cols["list_i64"] = pa.array(nul([list(range(i)) for i in range(n)]), type=pa.list_(pa.int64()))
cols["large_list_str"] = pa.array(nul([[w] * (i % 3) for i, w in enumerate(words)]), type=pa.large_list(pa.string()))
cols["fixed_list"] = pa.array(nul([[i, i + 1] for i in range(n)]), type=pa.list_(pa.int32(), 2))
cols["list_view"] = pa.array(nul([list(range(i)) for i in range(n)]), type=pa.list_view(pa.int32()))
cols["struct"] = pa.array(nul([{"a": i, "b": words[i]} for i in range(n)]), type=pa.struct([("a", pa.int32()), ("b", pa.string())]))
cols["map"] = pa.array(nul([[(words[i], i)] * (i % 3) for i in range(n)]), type=pa.map_(pa.string(), pa.int64()))
cols["dict_str"] = pa.array(nul([words[i % 3] for i in range(n)])).dictionary_encode()
cols["dict_ordered"] = pa.DictionaryArray.from_arrays(pa.array(nul([i % 2 for i in range(n)]), type=pa.int8()), pa.array(["lo", "hi"]), ordered=True)
cols["sparse_union"] = pa.UnionArray.from_sparse(pa.array([0, 1, 0, 1, 0, 1, 0], type=pa.int8()), [pa.array(list(range(n))), pa.array(words)])
cols["dense_union"] = pa.UnionArray.from_dense(pa.array([0, 1, 0, 1, 0, 1, 0], type=pa.int8()), pa.array([0, 0, 1, 1, 2, 2, 3], type=pa.int32()), [pa.array([1, 2, 3, 4]), pa.array(["a", "b", "c"])])
cols["ree"] = pa.RunEndEncodedArray.from_arrays(pa.array([2, 5, 7], type=pa.int32()), pa.array([1.5, None, 3.5]))
cols["list_struct"] = pa.array(nul([[{"x": i}] * (i % 2) for i in range(n)]), type=pa.list_(pa.struct([("x", pa.int64())])))

tbl = pa.table(cols)
tbl = tbl.replace_schema_metadata({"origin": "pyarrow", "k": "v"})
# two batches so stream paths see more than one
batches = tbl.to_batches(max_chunksize=4)
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, tbl.schema) as w:
    for b in batches:
        w.write_batch(b)
open(sys.argv[1], "wb").write(sink.getvalue().to_pybytes())
print(len(tbl.columns), "columns,", tbl.num_rows, "rows,", len(batches), "batches")
