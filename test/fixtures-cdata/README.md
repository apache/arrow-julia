<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# C data interface fixtures

`matrix.arrows` is an IPC stream pyarrow 25.0.1 wrote with `make_matrix.py`
(`python3 make_matrix.py matrix.arrows`): 54 columns covering every type the C
data interface carries, with nulls in every nullable column, schema metadata,
and two record batches. The C data battery reads it with `Arrow.Table`, sends
it through both C interfaces in both directions, and compares the result with
that IPC read.
