#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eu

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOP_SOURCE_DIR="$(dirname $(dirname ${SOURCE_DIR}))"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 2.2.1 1"
  exit 1
fi

set -o pipefail
set -x

VERSION="$1"
RC="$2"

ARROW_DIST_URL="https://dist.apache.org/repos/dist/dev/arrow"
ARCHIVE_BASE_NAME="apache-arrow-julia-${VERSION}"

: ${VERIFY_DEFAULT:=1}
: ${VERIFY_DOWNLOAD:=${VERIFY_DEFAULT}}
: ${VERIFY_FORCE_USE_JULIA_BINARY:=0}
: ${VERIFY_SIGN:=${VERIFY_DEFAULT}}

download_dist_file() {
  curl \
    --fail \
    --location \
    --remote-name \
    --show-error \
    --silent \
    "${ARROW_DIST_URL}/$1"
}

download_rc_file() {
  local path="apache-arrow-julia-${VERSION}-rc${RC}/$1"
  if [ ${VERIFY_DOWNLOAD} -gt 0 ]; then
    download_dist_file "${path}"
  else
    cp "${SOURCE_DIR}/dist/${path}" "$1"
  fi
}

import_gpg_keys() {
  if [ ${VERIFY_SIGN} -gt 0 ]; then
    download_dist_file KEYS
    gpg --import KEYS
  fi
}

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  download_rc_file ${ARCHIVE_BASE_NAME}.tar.gz
  if [ ${VERIFY_SIGN} -gt 0 ]; then
    download_rc_file ${ARCHIVE_BASE_NAME}.tar.gz.asc
    gpg --verify ${ARCHIVE_BASE_NAME}.tar.gz.asc ${ARCHIVE_BASE_NAME}.tar.gz
  fi
  download_rc_file ${ARCHIVE_BASE_NAME}.tar.gz.sha256
  ${sha256_verify} ${ARCHIVE_BASE_NAME}.tar.gz.sha256
  download_rc_file ${ARCHIVE_BASE_NAME}.tar.gz.sha512
  ${sha512_verify} ${ARCHIVE_BASE_NAME}.tar.gz.sha512
}

setup_tmpdir() {
  cleanup() {
    if [ "${VERIFY_SUCCESS}" = "yes" ]; then
      rm -rf "${VERIFY_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${VERIFY_TMPDIR} for details."
    fi
  }

  if [ -z "${VERIFY_TMPDIR:-}" ]; then
    VERIFY_TMPDIR=$(mktemp -d -t "$1.XXXXX")
    trap cleanup EXIT
  else
    mkdir -p "${VERIFY_TMPDIR}"
  fi
}

latest_julia_version() {
  curl \
    --fail \
    --location \
    --show-error \
    --silent \
    https://api.github.com/repos/JuliaLang/julia/releases | \
    grep -o '"tag_name": "v.*"' | \
    head -n 1 | \
    sed -e 's/^"tag_name": "v//g' \
        -e 's/"$//g'
}

ensure_julia() {
  if [ ${VERIFY_FORCE_USE_JULIA_BINARY} -le 0 ]; then
    if julia --version; then
      return
    fi
  fi

  local julia_binary_url=https://julialang-s3.julialang.org/bin
  local julia_version=$(latest_julia_version)
  local julia_version_series=${julia_version%.*}
  case "$(uname)" in
    Darwin)
      julia_binary_url+="/mac"
      case "$(arch)" in
        # TODO
        # aarch64)
        #   julia_binary_url+="/aarch64"
        #   julia_binary_url+="/${julia_version_series}"
        #   julia_binary_url+="/julia-${julia_version}-macaarch64.dmg"
        #   ;;
        i386)
          julia_binary_url+="/x64"
          julia_binary_url+="/${julia_version_series}"
          julia_binary_url+="/julia-${julia_version}-mac64.tar.gz"
          ;;
        *)
          echo "You must install Julia manually on $(uname) $(arch)"
          ;;
      esac
      ;;
    Linux)
      julia_binary_url+="/linux"
      case "$(arch)" in
        aarch64)
          julia_binary_url+="/aarch64"
          ;;
        x86_64)
          julia_binary_url+="/x64"
          ;;
        *)
          echo "You must install Julia manually on $(uname) $(arch)"
          ;;
      esac
      julia_binary_url+="/${julia_version_series}"
      julia_binary_url+="/julia-${julia_version}-linux-$(arch).tar.gz"
      ;;
    *)
      echo "You must install Julia manually on $(uname)"
      exit 1
      ;;
  esac
  julia_binary_tar_gz=$(basename ${julia_binary_url})
  curl \
    --fail \
    --location \
    --output ${julia_binary_tar_gz} \
    --show-error \
    --silent \
    ${julia_binary_url}
  tar xf ${julia_binary_tar_gz}
  julia_path=$(echo julia-*/bin/julia)
  PATH="$(pwd)/$(dirname ${julia_path}):${PATH}"
  export JULIA_DEPOT_PATH="$(pwd)/.julia"
}

test_source_distribution() {
  julia --project -e 'import Pkg; Pkg.build(); Pkg.test()'
}

VERIFY_SUCCESS=no

setup_tmpdir "arrow-julia-${VERSION}-${RC}"
echo "Working in sandbox ${VERIFY_TMPDIR}"
cd "${VERIFY_TMPDIR}"

import_gpg_keys
fetch_archive
tar xf ${ARCHIVE_BASE_NAME}.tar.gz
ensure_julia
pushd ${ARCHIVE_BASE_NAME}
test_source_distribution
popd

VERIFY_SUCCESS=yes
echo "RC looks good!"
