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
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <rc>"
  echo " e.g.: $0 1"
  exit 1
fi

rc=$1

: ${RELEASE_DEFAULT:=1}
: ${RELEASE_CLEANUP:=${RELEASE_DEFAULT}}
: ${RELEASE_PULL:=${RELEASE_DEFAULT}}
: ${RELEASE_PUSH_TAG:=${RELEASE_DEFAULT}}
: ${RELEASE_SIGN:=${RELEASE_DEFAULT}}
: ${RELEASE_UPLOAD:=${RELEASE_DEFAULT}}

cd "${SOURCE_TOP_DIR}"

if [ ${RELEASE_PULL} -gt 0 -o ${RELEASE_PUSH_TAG} -gt 0 ]; then
  git_origin_url="$(git remote get-url origin)"
  if [ "${git_origin_url}" != "git@github.com:apache/arrow-julia.git" ]; then
    echo "This script must be ran with working copy of apache/arrow-julia."
    echo "The origin's URL: ${git_origin_url}"
    exit 1
  fi
fi

if [ ${RELEASE_PULL} -gt 0 ]; then
  echo "Ensure using the latest commit"
  git checkout main
  git pull --rebase --prune
fi

version=$(grep -o '^version = ".*"' "Project.toml" | \
            sed -e 's/^version = "//' \
                -e 's/"$//')

rc_tag="v${version}-rc${rc}"
echo "Tagging for RC: ${rc_tag}"
git tag -a -m "${version} RC${rc}" "${rc_tag}"
if [ ${RELEASE_PUSH_TAG} -gt 0 ]; then
  git push origin "${rc_tag}"
fi

rc_hash="$(git rev-list --max-count=1 "${rc_tag}")"

id="apache-arrow-julia-${version}"
rc_id="${id}-rc${rc}"
dev_dist_url="https://dist.apache.org/repos/dist/dev/arrow"
dev_dist_dir="dev/release/dist"
tar_gz="${id}.tar.gz"
tar_gz_path="${dev_dist_dir}/${rc_id}/${tar_gz}"
rc_url="${dev_dist_url}/${rc_id}/"

echo "Checking out ${dev_dist_url}"
rm -rf "${dev_dist_dir}"
svn co --depth=empty "${dev_dist_url}" "${dev_dist_dir}"

echo "Attempting to create ${tar_gz} from tag ${rc_tag}"
mkdir -p "$(dirname "${tar_gz_path}")"
git archive "${rc_hash}" --prefix "${id}/" --output "${tar_gz_path}"

pushd "${dev_dist_dir}/${rc_id}"

echo "Running Rat license checker on ${tar_gz}"
../../run_rat.sh ${tar_gz}

if [ ${RELEASE_SIGN} -gt 0 ]; then
  echo "Signing tar.gz and creating checksums"
  gpg --armor --output ${tar_gz}.asc --detach-sig ${tar_gz}
fi

if type shasum >/dev/null 2>&1; then
  sha256_generate="shasum -a 256"
  sha512_generate="shasum -a 512"
else
  sha256_generate="sha256sum"
  sha512_generate="sha512sum"
fi
${sha256_generate} ${tar_gz} > ${tar_gz}.sha256
${sha512_generate} ${tar_gz} > ${tar_gz}.sha512

if [ ${RELEASE_UPLOAD} -gt 0 ]; then
  echo "Uploading to ${rc_url}"
  svn add .
  svn ci -m "Apache Arrow Julia ${version} ${rc}"
fi

popd

if [ ${RELEASE_CLEANUP} -gt 0 ]; then
  echo "Removing temporary directory"
  rm -rf "${dev_dist_dir}"
fi

echo "Draft email for dev@arrow.apache.org mailing list"
echo ""
echo "---------------------------------------------------------"
cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE][Julia] Release Apache Arrow Julia ${version} RC${rc}

Hi,

I would like to propose the following release candidate (RC${rc}) of
Apache Arrow Julia version ${version}.

This release candidate is based on commit:
${rc_hash} [1]

The source release rc${rc} is hosted at [2].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [3] for how to validate a release candidate.

The vote will be open for at least 24 hours.

[ ] +1 Release this as Apache Arrow Julia ${version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow Julia ${version} because...

[1]: https://github.com/apache/arrow-julia/tree/${rc_hash}
[2]: ${rc_url}
[3]: https://github.com/apache/arrow-julia/blob/main/dev/release/README.md#verify
MAIL
echo "---------------------------------------------------------"
