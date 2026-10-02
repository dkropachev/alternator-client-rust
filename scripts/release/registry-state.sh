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
    echo "usage: $0 PACKAGE_NAME VERSION CANDIDATE_CRATE" >&2
    exit 2
}

package_name=$1
version=$2
candidate_crate=$3
user_agent="$package_name-release-workflow"

[[ -f "$candidate_crate" ]] || {
    echo "candidate crate does not exist: $candidate_crate" >&2
    exit 1
}

case ${#package_name} in
    1) index_path="1/$package_name" ;;
    2) index_path="2/$package_name" ;;
    3) index_path="3/${package_name:0:1}/$package_name" ;;
    *) index_path="${package_name:0:2}/${package_name:2:2}/$package_name" ;;
esac

index_body=$(mktemp)
index_status=$(curl -A "$user_agent" -sS -H 'Cache-Control: no-cache' \
    -o "$index_body" -w '%{http_code}' "https://index.crates.io/$index_path")

crate_exists=false
state=absent
registry_sha=
if [[ "$index_status" == 200 ]]; then
    crate_exists=true
    registry_records=$(jq -c --arg version "$version" 'select(.vers == $version)' "$index_body")
    registry_count=$(printf '%s\n' "$registry_records" | awk 'NF { count++ } END { print count + 0 }')
    [[ "$registry_count" -le 1 ]] || {
        echo "sparse index contains duplicate records for $package_name $version" >&2
        exit 1
    }
    if [[ "$registry_count" -eq 1 ]]; then
        registry_sha=$(jq -er '.cksum' <<<"$registry_records")
        registry_yanked=$(jq -er \
            'if (.yanked | type) == "boolean" then (.yanked | tostring) else error("invalid yanked field") end' \
            <<<"$registry_records")
        candidate_sha=$(shasum -a 256 "$candidate_crate" | awk '{ print $1 }')
        if [[ "$registry_yanked" == true ]]; then
            state=conflict
            echo "SECURITY CONFLICT: registry $package_name $version is yanked" >&2
        elif [[ "$registry_sha" == "$candidate_sha" ]]; then
            state=exact
        else
            state=conflict
        fi
    fi
elif [[ "$index_status" != 404 ]]; then
    echo "sparse index returned HTTP $index_status" >&2
    exit 1
fi

if [[ "$state" == exact ]]; then
    registry_crate=$(mktemp)
    curl -A "$user_agent" -fsSL --retry 5 --retry-all-errors \
        -o "$registry_crate" \
        "https://static.crates.io/crates/$package_name/$package_name-$version.crate"
    downloaded_sha=$(shasum -a 256 "$registry_crate" | awk '{ print $1 }')
    [[ "$downloaded_sha" == "$registry_sha" ]] || {
        echo "registry download hash $downloaded_sha differs from sparse index $registry_sha" >&2
        exit 1
    }
elif [[ "$state" == conflict ]]; then
    candidate_sha=$(shasum -a 256 "$candidate_crate" | awk '{ print $1 }')
    if [[ -n "$registry_sha" && "$registry_sha" != "$candidate_sha" ]]; then
        echo "SECURITY CONFLICT: registry $package_name $version has $registry_sha, candidate has $candidate_sha" >&2
    fi
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "crate_exists=$crate_exists" >>"$GITHUB_OUTPUT"
    echo "state=$state" >>"$GITHUB_OUTPUT"
    echo "registry_sha256=$registry_sha" >>"$GITHUB_OUTPUT"
fi

echo "$state"
[[ "$state" != conflict ]]
