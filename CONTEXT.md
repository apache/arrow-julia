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

# Arrow.jl

Arrow.jl maps between the Arrow columnar format and Julia values. These terms name the
domains and verification paths used by the package.

## Language

**Storage domain**:
The Julia values that directly represent an Arrow physical layout before facade or
ArrowTypes conversion.
_Avoid_: Raw values, wire values

**Public domain**:
The Julia values exposed by `Arrow.Table` and `Arrow.Stream` after facade and
ArrowTypes conversion.
_Avoid_: Converted values, logical values

**Scan plan**:
A `Tables.Scan` request resolved against one Arrow schema, including its storage-domain
filter, selected output, row window, and remaining public-domain conversion.
_Avoid_: Lowered scan

**Column construction**:
The mapping from a Julia column and optional retained Arrow field to one Arrow field and
its array data.
_Avoid_: Column conversion, column building

**Acceptance battery**:
An assertion-dense executable that verifies one Arrow adapter through its supported
interface.
_Avoid_: Integration script, smoke test

**Conformance oracle**:
An independent Arrow implementation or corpus used to judge interoperability.
_Avoid_: Golden test, reference test
