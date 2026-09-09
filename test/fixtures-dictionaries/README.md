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

# Dictionary IPC fixtures

Generated with PyArrow 25.0.1 by `test/support/generate_dictionary_fixtures.py`.
Run the generator from any directory with PyArrow installed. Normal Julia tests
read these frozen bytes and do not require Python.

The delta fixtures contain three record batches and two delta messages. They
cover primitive and nested pools, null indices and pool entries, stream and
file formats, and uncompressed/LZ4/Zstd bodies. The replacement fixtures cover
issue #610 with V4 and V5 schemas that omit the dictionary replacement feature.

These files are test data produced by the adjacent Apache-licensed generator.
