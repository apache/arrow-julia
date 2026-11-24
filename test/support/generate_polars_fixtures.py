# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Writes the Polars IPC fixtures read by test/polars_tests.jl.
import datetime, decimal
from pathlib import Path
import polars as pl

root = Path(__file__).resolve().parents[1] / "fixtures-polars"
root.mkdir(exist_ok=True)
df = pl.DataFrame({
    "i": [1, 2, None],
    "f": [1.5, None, 3.0],
    "s": ["a", None, "a much longer string value"],
    "b": [True, False, None],
    "d": [datetime.date(2024, 1, 1), None, datetime.date(2024, 1, 3)],
    "ts": [datetime.datetime(2024, 1, 1, 12), None, datetime.datetime(2024, 1, 3)],
    "dur": [datetime.timedelta(seconds=5), None, datetime.timedelta(days=1)],
    "t": [datetime.time(1, 2, 3), None, datetime.time(23, 59)],
    "dec": pl.Series([decimal.Decimal("1.25"), None, decimal.Decimal("-3.50")],
                     dtype=pl.Decimal(10, 2)),
    "cat": pl.Series(["x", "y", "x"], dtype=pl.Categorical),
    "l": [[1, 2], None, []],
    "bin": [b"\x00\x01", None, b"xyz"],
})
for name, kw in (("default", {}), ("lz4", {"compression": "lz4"}),
                 ("zstd", {"compression": "zstd"})):
    df.write_ipc(root / f"{name}.arrowbytes", **kw)
df.write_ipc_stream(root / "stream.arrowbytes")
# Polars' 128-bit integers use the non-standard Int bitWidth 128.
pl.DataFrame({
    "i128": pl.Series([1, -(2**100), None], dtype=pl.Int128),
    "u128": pl.Series([2**127 + 5, 0, None], dtype=pl.UInt128),
}).write_ipc(root / "int128.arrowbytes")
print("Generated with Polars", pl.__version__)
