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
    echo "usage: $0 VERSION RC_TAG EXPECTED_COMMIT" >&2
    exit 2
}

version=$1
rc_tag=$2
expected_commit=$3

rc_prefix="v$version-rc."
[[ "$rc_tag" == "$rc_prefix"* ]] || {
    echo "invalid RC tag for version $version: $rc_tag" >&2
    exit 1
}
rc_number=${rc_tag#"$rc_prefix"}
[[ "$rc_number" =~ ^[1-9][0-9]*$ ]] || {
    echo "invalid RC tag for version $version: $rc_tag" >&2
    exit 1
}
current_number=$((10#$rc_number))

git fetch --force origin --tags
[[ "$(git cat-file -t "$rc_tag")" == tag ]] || {
    echo "$rc_tag is not an annotated tag" >&2
    exit 1
}
[[ "$(git rev-list -n 1 "$rc_tag")" == "$expected_commit" ]] || {
    echo "$rc_tag does not point to the expected commit" >&2
    exit 1
}

latest_number=0
latest_tag=
while IFS= read -r tag; do
    [[ "$tag" =~ -rc\.([1-9][0-9]*)$ ]] || continue
    number=$((10#${BASH_REMATCH[1]}))
    if (( number > latest_number )); then
        latest_number=$number
        latest_tag=$tag
    fi
done < <(git tag -l "v$version-rc.*")

[[ "$current_number" -eq "$latest_number" && "$rc_tag" == "$latest_tag" ]] || {
    echo "$rc_tag is superseded by $latest_tag; an older candidate may never be promoted" >&2
    exit 1
}

echo "$rc_tag is the latest release candidate for $version"
