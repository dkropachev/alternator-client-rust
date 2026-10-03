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

[[ $# -eq 2 ]] || {
    echo "usage: $0 VERSION COMMIT_SHA" >&2
    exit 2
}

version=$1
commit_sha=$2
run_id=${GITHUB_RUN_ID:-}

[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
    echo "version must be an exact X.Y.Z release without leading zeroes" >&2
    exit 1
}
[[ "$commit_sha" =~ ^[0-9a-f]{40}$ ]] || {
    echo "commit SHA must be a lowercase 40-character hexadecimal value" >&2
    exit 1
}
[[ "$run_id" =~ ^[1-9][0-9]*$ ]] || {
    echo "GITHUB_RUN_ID must be a positive integer" >&2
    exit 1
}
command -v git >/dev/null 2>&1 || {
    echo "git is required" >&2
    exit 1
}

resolved_commit=$(git rev-parse --verify "$commit_sha^{commit}" 2>/dev/null) || {
    echo "commit $commit_sha does not identify a commit in this checkout" >&2
    exit 1
}
[[ "$resolved_commit" == "$commit_sha" ]] || {
    echo "commit $commit_sha did not resolve to itself" >&2
    exit 1
}

# Refresh only tag refs. This observes remote state but never mutates the
# repository, GitHub, or the package registry.
git fetch --force --prune --prune-tags origin '+refs/tags/*:refs/tags/*'

tag_prefix="v$version-rc."
matched_tag=
max_rc=0

while IFS= read -r tag; do
    [[ "$tag" == "$tag_prefix"* ]] || continue
    suffix=${tag#"$tag_prefix"}
    [[ "$suffix" =~ ^[1-9][0-9]*$ ]] || continue

    # Bash arithmetic is signed. Refuse a value which cannot be compared
    # safely instead of wrapping and accidentally reusing an RC number.
    [[ ${#suffix} -le 18 ]] || {
        echo "$tag has an RC number too large to handle safely" >&2
        exit 1
    }
    number=$((10#$suffix))
    (( number > max_rc )) && max_rc=$number

    [[ "$(git cat-file -t "refs/tags/$tag")" == tag ]] || continue
    tag_object=$(git cat-file tag "refs/tags/$tag")
    tag_message=$(sed '1,/^$/d' <<<"$tag_object")
    owned_marker_count=$(awk -v expected="workflow-run: $run_id" \
        '$0 == expected { count++ } END { print count + 0 }' <<<"$tag_message")
    (( owned_marker_count > 0 )) || continue

    run_marker_count=$(awk \
        '/^workflow-run:/ { count++ } END { print count + 0 }' <<<"$tag_message")
    [[ "$owned_marker_count" -eq 1 && "$run_marker_count" -eq 1 ]] || {
        echo "$tag has duplicate or malformed workflow-run annotations" >&2
        exit 1
    }
    direct_object=$(sed -n '1s/^object //p' <<<"$tag_object")
    direct_type=$(sed -n '2s/^type //p' <<<"$tag_object")
    [[ "$direct_type" == commit && "$direct_object" == "$commit_sha" ]] || {
        echo "$tag belongs to workflow run $run_id but does not directly reference $commit_sha" >&2
        exit 1
    }
    commit_marker_count=$(awk \
        '/^commit:/ { count++ } END { print count + 0 }' <<<"$tag_message")
    expected_commit_marker_count=$(awk -v expected="commit: $commit_sha" \
        '$0 == expected { count++ } END { print count + 0 }' <<<"$tag_message")
    [[ "$commit_marker_count" -eq 1 && "$expected_commit_marker_count" -eq 1 ]] || {
        echo "$tag does not have exactly one commit annotation for $commit_sha" >&2
        exit 1
    }
    [[ -z "$matched_tag" ]] || {
        echo "more than one annotated numeric RC tag belongs to workflow run $run_id" >&2
        exit 1
    }
    matched_tag=$tag
done < <(git for-each-ref --format='%(refname:strip=2)' refs/tags/)

if [[ -n "$matched_tag" ]]; then
    tagged_commit=$(git rev-list -n 1 "refs/tags/$matched_tag")
    [[ "$tagged_commit" == "$commit_sha" ]] || {
        echo "$matched_tag belongs to workflow run $run_id but points to $tagged_commit" >&2
        exit 1
    }
    rc_tag=$matched_tag
    action=recover
else
    rc_number=$((max_rc + 1))
    (( rc_number > max_rc )) || {
        echo "cannot allocate an RC number after $max_rc" >&2
        exit 1
    }
    rc_tag="$tag_prefix$rc_number"
    action=create
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    {
        echo "rc_tag=$rc_tag"
        echo "action=$action"
    } >>"$GITHUB_OUTPUT"
fi

echo "rc_tag=$rc_tag"
echo "action=$action"
