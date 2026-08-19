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

# The Arrow.jl conformance image: everything the conformance suites need,
# in one container, so that "docker present" is the only host requirement.
#
#   * Julia (the base image) — the suites run IN the container, because the
#     C data and C stream interfaces hand pointers across an in-process
#     boundary; only IPC bytes could cross a container boundary.
#   * A Python with pyarrow and nanoarrow — the oracles. PythonCall binds to
#     it (JULIA_PYTHONCALL_EXE; the CondaPkg backend is disabled).
#   * The apache/arrow-testing gold corpus.
#   * Tables.jl's `jq/scan` branch (until Tables.Scan is released).
#   * A warm Julia depot with the registered dependencies, in a named volume
#     at run time so precompilation caches survive between runs.
#
# The repository is bind-mounted at /work at run time (never copied in), so
# the image is independent of the source tree; conformance/run.jl builds it
# once and drives it. Build context: the conformance/ directory.
#
#     docker build -t arrow-julia-conformance:latest -f conformance/Dockerfile conformance/

FROM julia:1.12-bookworm

# python3-dev brings the shared libpython that PythonCall embeds; Debian's
# interpreter binary is statically linked and does not ship it otherwise.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv python3-dev git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# The oracle interpreter. A venv keeps the Debian system Python untouched.
RUN python3 -m venv /opt/pyarrow \
 && /opt/pyarrow/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pyarrow/bin/pip install --no-cache-dir pyarrow nanoarrow

RUN git clone --depth 1 https://github.com/apache/arrow-testing.git /opt/arrow-testing \
 && git clone --depth 1 --branch jq/scan https://github.com/JuliaData/Tables.jl.git /opt/Tables

ENV ARROW_TESTING_DIR=/opt/arrow-testing \
    ARROW_ORACLE_PYTHON=/opt/pyarrow/bin/python \
    ARROW_CDATA_ORACLE_PYTHON=/opt/pyarrow/bin/python \
    JULIA_PYTHONCALL_EXE=/opt/pyarrow/bin/python \
    JULIA_CONDAPKG_BACKEND=Null \
    JULIA_DEPOT_PATH=/opt/julia-depot \
    JULIA_PROJECT=/opt/env

# Warm the depot: every registered dependency the package and the conformance
# environment resolve to (keep in step with Project.toml and
# conformance/Project.toml — the run-time instantiate is authoritative and
# fetches any delta), plus PythonCall precompiled against the oracle Python.
RUN julia -e 'using Pkg; Pkg.activate("/opt/warm"); \
    Pkg.add(["CodecLz4", "CodecZstd", "DataAPI", "EnumX", "TranscodingStreams", \
             "JSON", "CodecZlib", "PooledArrays", "PythonCall", "Harbor"]); \
    Pkg.develop(path="/opt/Tables"); Pkg.precompile()'

# The suite environment is created from the mounted repository's
# conformance/Project.toml by conformance/run.jl (developing /work and
# /opt/Tables into it), so a Manifest never lives in the repository checkout.
RUN mkdir -p /opt/env /work
WORKDIR /work
CMD ["julia", "--version"]
