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

# Arrow.jl 3.0 dependency review — round 31

Date: 2026-08-16

Scope: commit `702b112739fbb84bd97a9935749f4a4811bfc233` only.

## Result

No findings. The restored compat range admits TranscodingStreams 0.9.12, and
the writer comment now matches the direct dependency and import.

## Assumptions and decisions

- I treated the requested versions as one exact joint resolution.
- I used a clean Tables `jq/scan` snapshot at `3bfa6b6` for the disposable
  minimum-version control.
- I reviewed only `702b112` and left the pre-existing untracked files alone.

## Validation

- A fresh environment resolved CodecLz4 0.4.0, CodecZstd 0.8.0, EnumX 1.0.0,
  TranscodingStreams 0.9.12, Tables dev, and Arrow at `702b112`.
- Arrow loaded, `Arrow.TS === TranscodingStreams`, and `Tables.Scan` was
  present.
- Two-batch LZ4 and Zstd write/read round-trips passed exact value equality.
- `julia --startup-file=no --project=. -e 'using Pkg; Pkg.test()'` passed
  333/333 tests at HEAD.
- `git diff --check 702b112^ 702b112` passed.

VERDICT: CLEAN
