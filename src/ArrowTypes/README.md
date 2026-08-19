<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# ArrowTypes.jl

The type-mapping interface for serializing custom Julia types in the Arrow
format, kept as its own package (registered separately from this
subdirectory of the arrow-julia repository) so that packages can declare
how their types map to Arrow without depending on Arrow.jl itself.

* `ArrowTypes.ArrowKind(T)` — the general category of Arrow type a Julia
  type is treated as (`PrimitiveKind`, `ListKind`, `StructKind`, …).
* `ArrowTypes.ArrowType(T)` / `ArrowTypes.toarrow(x)` — the natively
  supported type a value is converted to for serialization.
* `ArrowTypes.arrowname(T)`, `ArrowTypes.arrowmetadata(T)`,
  `ArrowTypes.JuliaType(Val(name), S, meta)` and `ArrowTypes.fromarrow` —
  the round trip back to the custom type through Arrow extension-type
  metadata.

Arrow.jl 2.x consumes this interface; Arrow.jl 3.0 does not depend on it.
See the docstrings of the functions above for the contract.
