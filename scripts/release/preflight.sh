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
    echo "usage: $0 MODE VERSION TARGET_COMMIT" >&2
    exit 2
}

mode=$1
version=$2
target_commit=$3
package_name=alternator-client

[[ "$mode" == validate || "$mode" == release ]] || {
    echo "mode must be validate or release" >&2
    exit 1
}
[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
    echo "version must be an exact X.Y.Z release without leading zeroes" >&2
    exit 1
}
[[ "$target_commit" =~ ^[0-9a-f]{40}$ ]] || {
    echo "target commit must be a lowercase 40-character SHA" >&2
    exit 1
}
[[ "${GITHUB_REF:-}" == refs/heads/main ]] || {
    echo "release workflow must be dispatched from main" >&2
    exit 1
}
[[ -n "${GITHUB_SHA:-}" ]] || {
    echo "GITHUB_SHA is required" >&2
    exit 1
}
[[ "$target_commit" == "$GITHUB_SHA" ]] || {
    echo "target commit $target_commit does not match workflow SHA $GITHUB_SHA" >&2
    exit 1
}
head_commit=$(git rev-parse HEAD)
[[ "$target_commit" == "$head_commit" ]] || {
    echo "target commit $target_commit does not match checkout HEAD $head_commit" >&2
    exit 1
}

git fetch --force --prune --prune-tags origin \
    '+refs/heads/main:refs/remotes/origin/main' \
    '+refs/tags/*:refs/tags/*'
main_commit=$(git rev-parse refs/remotes/origin/main)
current_run_rc=

if [[ "$mode" == validate ]]; then
    [[ "$target_commit" == "$main_commit" ]] || {
        echo "validation target $target_commit is not the current origin/main commit $main_commit; start a new run" >&2
        exit 1
    }
else
    run_id=${GITHUB_RUN_ID:-}
    [[ -n "$run_id" ]] || {
        echo "GITHUB_RUN_ID is required in release mode" >&2
        exit 1
    }

    rc_tags=$(git tag -l "v$version-rc.*")
    while IFS= read -r tag; do
        [[ -n "$tag" ]] || continue
        suffix=${tag#"v$version-rc."}
        [[ "$suffix" =~ ^[1-9][0-9]*$ ]] || continue
        [[ "$(git cat-file -t "$tag" 2>/dev/null || true)" == tag ]] || continue
        tag_object=$(git cat-file tag "$tag")
        tag_message=$(sed '1,/^$/d' <<<"$tag_object")
        owned_marker_count=$(awk -v expected="workflow-run: $run_id" \
            '$0 == expected { count++ } END { print count + 0 }' <<<"$tag_message")
        [[ "$owned_marker_count" -gt 0 ]] || continue

        run_marker_count=$(awk \
            '/^workflow-run:/ { count++ } END { print count + 0 }' <<<"$tag_message")
        [[ "$owned_marker_count" -eq 1 && "$run_marker_count" -eq 1 ]] || {
            echo "$tag has duplicate or malformed workflow-run annotations" >&2
            exit 1
        }
        direct_object=$(sed -n '1s/^object //p' <<<"$tag_object")
        direct_type=$(sed -n '2s/^type //p' <<<"$tag_object")
        [[ "$direct_type" == commit && "$direct_object" == "$target_commit" ]] || {
            echo "$tag does not directly reference target commit $target_commit" >&2
            exit 1
        }
        commit_marker_count=$(awk \
            '/^commit:/ { count++ } END { print count + 0 }' <<<"$tag_message")
        expected_commit_marker_count=$(awk -v expected="commit: $target_commit" \
            '$0 == expected { count++ } END { print count + 0 }' <<<"$tag_message")
        [[ "$commit_marker_count" -eq 1 && "$expected_commit_marker_count" -eq 1 ]] || {
            echo "$tag does not have exactly one commit annotation for $target_commit" >&2
            exit 1
        }
        [[ -z "$current_run_rc" ]] || {
            echo "multiple numeric RC tags belong to workflow run $run_id" >&2
            exit 1
        }
        current_run_rc=$tag
    done <<<"$rc_tags"

    if [[ -n "$current_run_rc" ]]; then
        rc_commit=$(git rev-list -n 1 "$current_run_rc")
        [[ "$rc_commit" == "$target_commit" ]] || {
            echo "$current_run_rc belongs to this run but points to $rc_commit instead of $target_commit" >&2
            exit 1
        }
    else
        [[ "$target_commit" == "$main_commit" ]] || {
            echo "release target $target_commit is not the current origin/main commit $main_commit and this run owns no RC" >&2
            exit 1
        }
    fi
fi

metadata=$(cargo metadata --locked --no-deps --format-version 1)
actual_name=$(jq -er '.packages[0].name' <<<"$metadata")
actual_version=$(jq -er '.packages[0].version' <<<"$metadata")
[[ "$actual_name" == "$package_name" ]] || {
    echo "expected package $package_name, found $actual_name" >&2
    exit 1
}
[[ "$actual_version" == "$version" ]] || {
    echo "Cargo.toml version $actual_version does not match input $version" >&2
    exit 1
}

lock_version=$(awk -v wanted="$package_name" '
    /^\[\[package\]\]$/ { name = ""; version = "" }
    /^name = / { value = $0; sub(/^name = "/, "", value); sub(/"$/, "", value); name = value }
    /^version = / && name == wanted {
        value = $0; sub(/^version = "/, "", value); sub(/"$/, "", value); print value; exit
    }
' Cargo.lock)
[[ "$lock_version" == "$version" ]] || {
    echo "Cargo.lock version $lock_version does not match input $version" >&2
    exit 1
}

awk -v prefix="## [$version] - " '
    index($0, prefix) == 1 {
        count += 1
        date = substr($0, length(prefix) + 1)
        if (date ~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$/) valid += 1
    }
    END { exit count == 1 && valid == 1 ? 0 : 1 }
' CHANGELOG.md || {
    echo "CHANGELOG.md has no dated [$version] release entry" >&2
    exit 1
}

if [[ "$mode" == release ]] && git rev-parse -q --verify "refs/tags/v$version" >/dev/null; then
    [[ -n "$current_run_rc" ]] || {
        echo "final tag v$version already exists outside this run's recovery state" >&2
        exit 1
    }
    final_commit=$(git rev-list -n 1 "v$version")
    [[ "$final_commit" == "$target_commit" ]] || {
        echo "final tag v$version points to $final_commit instead of $target_commit" >&2
        exit 1
    }
fi

index_path="${package_name:0:2}/${package_name:2:2}/$package_name"
index_body=$(mktemp)
trap 'rm -f -- "$index_body"' EXIT
if ! index_status=$(curl -A "$package_name-release-workflow" -sS \
    -o "$index_body" -w '%{http_code}' "https://index.crates.io/$index_path"); then
    echo "failed to query the crates.io sparse index" >&2
    exit 1
fi

[[ "$index_status" == 200 ]] || {
    if [[ "$index_status" == 404 ]]; then
        echo "$package_name is unclaimed on crates.io; refusing to continue for any version" >&2
    else
        echo "crates.io sparse index preflight returned HTTP $index_status" >&2
    fi
    exit 1
}

if ! jq -e -s \
    'length > 0 and all(.[]; type == "object" and (.vers | type == "string"))' \
    "$index_body" >/dev/null; then
    echo "crates.io sparse index returned malformed data" >&2
    exit 1
fi
if ! published_records=$(jq -c --arg version "$version" 'select(.vers == $version)' "$index_body"); then
    echo "crates.io sparse index returned malformed data" >&2
    exit 1
fi
published_count=$(printf '%s\n' "$published_records" | awk 'NF { count++ } END { print count + 0 }')
[[ "$published_count" -le 1 ]] || {
    echo "crates.io sparse index contains duplicate $package_name $version records" >&2
    exit 1
}

if [[ "$published_count" -eq 1 ]]; then
    if ! published_yanked=$(jq -er \
        'if (.yanked | type) == "boolean" then (.yanked | tostring) else error("invalid yanked field") end' \
        <<<"$published_records"); then
        echo "crates.io sparse index has an invalid yanked field for $package_name $version" >&2
        exit 1
    fi

    if [[ "$mode" == release ]]; then
        [[ "$published_yanked" == false ]] || {
            echo "$package_name $version is yanked; refusing release recovery" >&2
            exit 1
        }
        [[ -n "$current_run_rc" ]] || {
            echo "$package_name $version is already published outside this run's recovery state" >&2
            exit 1
        }
    fi
fi

echo "$mode preflight passed for $package_name $version at $target_commit"
