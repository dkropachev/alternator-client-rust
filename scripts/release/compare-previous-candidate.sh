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
    echo "usage: $0 CURRENT_DIR CURRENT_ARTIFACT_NAME ARTIFACT_PREFIX" >&2
    exit 2
}

current_dir=$1
current_name=$2
artifact_prefix=$3

[[ -n "${GITHUB_REPOSITORY:-}" && -n "${GITHUB_RUN_ID:-}" && -n "${GH_TOKEN:-}" ]] || {
    echo "GitHub run context and GH_TOKEN are required" >&2
    exit 1
}

artifacts=$(mktemp)
gh api --paginate --slurp -H 'X-GitHub-Api-Version: 2022-11-28' \
    "repos/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID/artifacts?per_page=100" \
    | jq '[.[].artifacts[]]' >"$artifacts"

previous_id=$(jq -r \
    --arg prefix "$artifact_prefix" \
    --arg current "$current_name" \
    '[.[] | select(.expired == false and .name != $current and (.name | startswith($prefix)))]
     | sort_by(.created_at, .id) | last | .id // empty' \
    "$artifacts")

if [[ -z "$previous_id" ]]; then
    echo "no earlier candidate artifact exists for this workflow run"
    exit 0
fi

previous_zip=$(mktemp)
previous_dir=$(mktemp -d)
curl -fsSL \
    -H 'Accept: application/vnd.github+json' \
    -H "Authorization: Bearer $GH_TOKEN" \
    -H 'X-GitHub-Api-Version: 2022-11-28' \
    -o "$previous_zip" \
    "https://api.github.com/repos/$GITHUB_REPOSITORY/actions/artifacts/$previous_id/zip"
unzip -q "$previous_zip" -d "$previous_dir"

for file in SHA256SUMS release-manifest.json; do
    [[ -f "$current_dir/$file" && -f "$previous_dir/$file" ]] || {
        echo "previous or current candidate is missing $file" >&2
        exit 1
    }
done

if ! cmp -s "$current_dir/SHA256SUMS" "$previous_dir/SHA256SUMS"; then
    echo "regenerated candidate hashes differ from the previous attempt" >&2
    diff -u "$previous_dir/SHA256SUMS" "$current_dir/SHA256SUMS" >&2 || true
    exit 1
fi

while read -r _ file; do
    [[ -f "$current_dir/$file" && -f "$previous_dir/$file" ]] && \
        cmp -s "$current_dir/$file" "$previous_dir/$file" || {
        echo "regenerated candidate file differs: $file" >&2
        exit 1
    }
done <"$current_dir/SHA256SUMS"

echo "regenerated candidate is byte-for-byte identical to the previous attempt"
