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


# Polars IPC fixtures

Generated with Polars 1.43.2 by `test/support/generate_polars_fixtures.py`.
Normal Julia tests read these frozen bytes and do not require Python.

Polars writes the IPC file format with string views, `large_list`, and
`uint32` dictionary indices, and writes the leading Schema message without
the stream framing the format specifies; readers take the schema from the
footer. `int128.arrowbytes` holds Polars' 128-bit integers, which use the
non-standard integer bit width 128.

These files are test data produced by the adjacent Apache-licensed generator.
