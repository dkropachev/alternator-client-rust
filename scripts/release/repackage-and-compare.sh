#!/usr/bin/env bash
# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

[[ $# -eq 3 ]] || {
    echo "usage: $0 VERSION EXPECTED_COMMIT CANDIDATE_CRATE" >&2
    exit 2
}

version=$1
expected_commit=$2
candidate_crate=$3

[[ "$(git rev-parse HEAD)" == "$expected_commit" ]] || {
    echo "checkout is not the immutable RC commit" >&2
    exit 1
}
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || {
    echo "RC checkout is dirty" >&2
    exit 1
}

package_target=$(mktemp -d)
# All candidate gates already ran before this OIDC-enabled job. Repackage
# without executing dependency build scripts in an id-token-capable context.
cargo package --locked --no-verify --target-dir "$package_target"
regenerated="$package_target/package/alternator-client-$version.crate"
[[ -f "$regenerated" && -f "$candidate_crate" ]] || {
    echo "candidate or regenerated crate is missing" >&2
    exit 1
}

if ! cmp -s "$candidate_crate" "$regenerated"; then
    candidate_sha=$(shasum -a 256 "$candidate_crate" | awk '{ print $1 }')
    regenerated_sha=$(shasum -a 256 "$regenerated" | awk '{ print $1 }')
    echo "repackaged crate differs: candidate=$candidate_sha regenerated=$regenerated_sha" >&2
    exit 1
fi

echo "repackaged crate is byte-for-byte identical to the tested candidate"
