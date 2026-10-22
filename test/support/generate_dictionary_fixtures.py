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

import pyarrow as pa, pyarrow.ipc as ipc
from pathlib import Path
root=Path(__file__).resolve().parents[1] / 'fixtures-dictionaries';root.mkdir(exist_ok=True)
cases={'strings':pa.array(['alpha','beta','gamma',None,'delta']), 'bool':pa.array([True,False,None,True,False]), 'int':pa.array([1,2,3,None,4],type=pa.int16()), 'list':pa.array([[1,2],[],[3],None,[4,5]],type=pa.list_(pa.int32())), 'struct':pa.array([{'x':1},{'x':2},{'x':3},None,{'x':4}],type=pa.struct([('x',pa.int32())]))}
for name,values in cases.items():
 for kind in ('stream','file'):
  for codec in (None,'lz4','zstd'):
   batches=[pa.record_batch({'a':pa.DictionaryArray.from_arrays(pa.array(ix,pa.int32()),values.slice(0,n))}) for n,ix in [(2,[0,1,None]),(3,[2,0,None]),(5,[3,4,1])]]
   path=root/f'{name}-{kind}-{codec}.arrowbytes'
   opener=ipc.new_stream if kind=='stream' else ipc.new_file
   with opener(path,batches[0].schema,options=ipc.IpcWriteOptions(emit_dictionary_deltas=True,compression=codec)) as w:
    for b in batches:w.write_batch(b)
    assert w.stats.num_dictionary_deltas==2, (name,w.stats)
   print(path)

# Issue #610: full replacements with no schema feature declaration.
for version in (ipc.MetadataVersion.V4, ipc.MetadataVersion.V5):
    batches = [pa.record_batch({'a': pa.DictionaryArray.from_arrays(
        pa.array([0, 1], pa.int32()), pa.array(pool))})
        for pool in (['alpha', 'beta'], ['gamma', 'delta'])]
    path = root / f'replacement-{version.name}.arrowbytes'
    with ipc.new_stream(path, batches[0].schema,
                        options=ipc.IpcWriteOptions(metadata_version=version)) as w:
        for batch in batches:
            w.write_batch(batch)
        assert w.stats.num_replaced_dictionaries == 1
    assert ipc.open_stream(path).read_all().column(0).to_pylist() == [
        'alpha', 'beta', 'gamma', 'delta']
print('Generated with PyArrow', pa.__version__)

# V4 metadata also supports deltas; only body compression requires V5.
values = pa.array(['alpha', 'beta', 'gamma'])
batches = [pa.record_batch({'a': pa.DictionaryArray.from_arrays(
    pa.array([n-1], pa.int32()), values.slice(0, n))}) for n in (2, 3)]
for kind, opener in [('stream', ipc.new_stream), ('file', ipc.new_file)]:
    with opener(root / f'delta-V4-{kind}.arrowbytes', batches[0].schema,
                options=ipc.IpcWriteOptions(metadata_version=ipc.MetadataVersion.V4,
                                            emit_dictionary_deltas=True)) as w:
        for batch in batches:
            w.write_batch(batch)
        assert w.stats.num_dictionary_deltas == 1
