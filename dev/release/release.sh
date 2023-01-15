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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 2.2.1 1"
  exit 1
fi

version=$1
rc=$2

rc_tag="v${version}-rc${rc}"
release_tag="v${version}"
echo "Tagging for release: ${release_tag}"
git tag -a -m "${version}" "${release_tag}" "${rc_tag}^{}"
git push origin "${release_tag}"

rc_id="apache-arrow-julia-${version}-rc${rc}"
release_id="arrow-julia-${version}"
echo "Move from dev/ to release/"
svn \
  mv \
  -m "Apache Arrow Julia ${version}" \
  https://dist.apache.org/repos/dist/dev/arrow/${rc_id} \
  https://dist.apache.org/repos/dist/release/arrow/${release_id}

echo "Remove all RCs"
old_rcs=$(
  svn ls https://dist.apache.org/repos/dist/dev/arrow/ | \
  grep -E '^apache-arrow-julia-' | \
  sort --version-sort --reverse
)
for old_rc in $old_rcs; do
  echo "Remove RC ${old_rc}"
  svn \
    delete \
    -m "Remove old Apache Arrow Julia RC: ${old_rc}" \
    https://dist.apache.org/repos/dist/dev/arrow/${old_rc}
done

echo "Keep only the latest versions"
old_releases=$(
  svn ls https://dist.apache.org/repos/dist/release/arrow/ | \
  grep -E '^arrow-julia-' | \
  sort --version-sort --reverse | \
  tail -n +2
)
for old_release_version in $old_releases; do
  echo "Remove old release ${old_release_version}"
  svn \
    delete \
    -m "Remove old Apache Arrow Julia release: ${old_release_version}" \
    https://dist.apache.org/repos/dist/release/arrow/${old_release_version}
done

echo "Success! The release is available here:"
echo "  https://dist.apache.org/repos/dist/release/arrow/${release_id}"
echo
echo "Add this release to ASF's report database:"
echo "  https://reporter.apache.org/addrelease.html?arrow"
