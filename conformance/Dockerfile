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
#   * The reviewed Tables.jl scan commit (until Tables.Scan is released).
#   * A warm Julia depot with the registered dependencies, in a named volume
#     at run time so precompilation caches survive between runs.
#
# The repository is bind-mounted at /work at run time (never copied in), so
# the image is independent of the source tree; conformance/run.jl builds it
# once and drives it. Build context: the conformance/ directory.
#
#     docker build -t arrow-julia-conformance:latest -f conformance/Dockerfile conformance/

# Pinned direct tool and corpus revisions make conformance runs repeatable
# against those inputs. The base-image digest, apt packages, and transitive
# Julia/Python dependencies are not fully locked. The package itself supports
# Julia 1.10 and later; Julia 1.12 is also the trim-test floor.
FROM julia:1.12.6-bookworm

ARG PIP_VERSION=26.2.1
ARG PYARROW_VERSION=25.0.1
ARG NANOARROW_VERSION=0.9.0
ARG ARROW_TESTING_REV=9ff285c88565f0f6abc855918c6a342e70e4909c
ARG TABLES_REV=64268c6a316e380cc3da26965f440a5433ebc1f7

# python3-dev brings the shared libpython that PythonCall embeds; Debian's
# interpreter binary is statically linked and does not ship it otherwise.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv python3-dev git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# The oracle interpreter. A venv keeps the Debian system Python untouched.
RUN python3 -m venv /opt/pyarrow \
 && /opt/pyarrow/bin/pip install --no-cache-dir "pip==${PIP_VERSION}" \
 && /opt/pyarrow/bin/pip install --no-cache-dir \
      "pyarrow==${PYARROW_VERSION}" "nanoarrow==${NANOARROW_VERSION}"

RUN git init /opt/arrow-testing \
 && git -C /opt/arrow-testing remote add origin https://github.com/apache/arrow-testing.git \
 && git -C /opt/arrow-testing fetch --depth 1 origin "${ARROW_TESTING_REV}" \
 && git -C /opt/arrow-testing checkout --detach FETCH_HEAD \
 && git init /opt/Tables \
 && git -C /opt/Tables remote add origin https://github.com/JuliaData/Tables.jl.git \
 && git -C /opt/Tables fetch --depth 1 origin "${TABLES_REV}" \
 && git -C /opt/Tables checkout --detach FETCH_HEAD

ENV ARROW_TESTING_DIR=/opt/arrow-testing \
    ARROW_ORACLE_PYTHON=/opt/pyarrow/bin/python \
    ARROW_CDATA_ORACLE_PYTHON=/opt/pyarrow/bin/python \
    JULIA_PYTHONCALL_EXE=/opt/pyarrow/bin/python \
    JULIA_CONDAPKG_BACKEND=Null \
    JULIA_DEPOT_PATH=/opt/julia-depot \
    JULIA_PROJECT=/opt/env
# JULIA_PROJECT serves interactive `docker exec ... julia`; the driver passes
# --project explicitly.

# Warm the depot: every registered dependency the package and the conformance
# environment resolve to (keep in step with Project.toml and
# conformance/Project.toml — the run-time instantiate is authoritative and
# fetches any delta), plus PythonCall precompiled against the oracle Python.
RUN julia -e 'using Pkg; Pkg.activate("/opt/warm"); \
    Pkg.add([PackageSpec(name="CodecLz4", version="0.4.6"), \
             PackageSpec(name="CodecZstd", version="0.8.7"), \
             PackageSpec(name="DataAPI", version="1.16.0"), \
             PackageSpec(name="EnumX", version="1.0.7"), \
             PackageSpec(name="TranscodingStreams", version="0.11.3"), \
             PackageSpec(name="JSON", version="1.7.1"), \
             PackageSpec(name="CodecZlib", version="0.7.9"), \
             PackageSpec(name="PythonCall", version="0.9.35")]); \
    Pkg.develop(path="/opt/Tables"); Pkg.precompile()'

# The suite environment is created from the mounted repository's
# conformance/Project.toml by conformance/run.jl (developing /work and
# /opt/Tables into it), so a Manifest never lives in the repository checkout.
RUN mkdir -p /opt/env /work
WORKDIR /work
# The driver keeps its container alive the same way.
CMD ["sleep", "infinity"]
