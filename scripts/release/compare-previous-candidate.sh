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
run_attempt=${GITHUB_RUN_ATTEMPT:?GITHUB_RUN_ATTEMPT is required}

[[ -n "${GITHUB_REPOSITORY:-}" && -n "${GITHUB_RUN_ID:-}" && -n "${GH_TOKEN:-}" ]] || {
    echo "GitHub run context and GH_TOKEN are required" >&2
    exit 1
}
[[ "$run_attempt" =~ ^[1-9][0-9]*$ ]] || {
    echo "invalid workflow run attempt: $run_attempt" >&2
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
    if [[ "$run_attempt" -eq 1 ]]; then
        echo "no earlier candidate exists for the first workflow attempt"
        exit 0
    fi

    # GitHub removes earlier artifacts when all jobs are rerun. Attempt logs
    # remain available, so use the digest emitted after successful candidate
    # creation rather than silently skipping the reproducibility check.
    prior_hashes=$(mktemp)
    found_prior_candidate=false
    for ((attempt = 1; attempt < run_attempt; attempt++)); do
        jobs=$(mktemp)
        gh api --paginate --slurp -H 'X-GitHub-Api-Version: 2022-11-28' \
            "repos/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID/attempts/$attempt/jobs?per_page=100" \
            >"$jobs"

        while IFS=$'\t' read -r job_id candidate_step_result; do
            [[ -n "$job_id" && "$candidate_step_result" == success ]] || continue
            found_prior_candidate=true
            job_log=$(mktemp)
            gh run view "$GITHUB_RUN_ID" --repo "$GITHUB_REPOSITORY" \
                --attempt "$attempt" --job "$job_id" --log >"$job_log" || {
                echo "could not read candidate log from workflow attempt $attempt" >&2
                exit 1
            }
            logged_hashes=$(grep -oE 'candidate-crate-sha256=[0-9a-f]{64}' "$job_log" \
                | cut -d= -f2 | LC_ALL=C sort -u || true)
            [[ -n "$logged_hashes" && "$(printf '%s\n' "$logged_hashes" | wc -l)" -eq 1 ]] || {
                echo "workflow attempt $attempt has no unique recorded candidate digest" >&2
                exit 1
            }
            printf '%s\n' "$logged_hashes" >>"$prior_hashes"
        done < <(jq -r '
            .[].jobs[] |
            select(.name == "Package candidate") |
            [
                (.id | tostring),
                ([.steps[]? |
                    select(.name == "Create candidate before any test runs") |
                    .conclusion][0] // "")
            ] | @tsv
        ' "$jobs")
    done

    if [[ "$found_prior_candidate" != true ]]; then
        echo "no earlier completed candidate exists for this workflow run"
        exit 0
    fi

    prior_hash=$(LC_ALL=C sort -u "$prior_hashes")
    [[ "$(printf '%s\n' "$prior_hash" | awk 'NF { count++ } END { print count + 0 }')" -eq 1 ]] || {
        echo "earlier workflow attempts recorded different candidate digests" >&2
        exit 1
    }
    manifest="$current_dir/release-manifest.json"
    [[ -f "$manifest" ]] || {
        echo "current candidate is missing release-manifest.json" >&2
        exit 1
    }
    crate_file=$(jq -er '.crate.file' "$manifest")
    manifest_hash=$(jq -er '.crate.sha256' "$manifest")
    [[ -f "$current_dir/$crate_file" ]] || {
        echo "current candidate is missing $crate_file" >&2
        exit 1
    }
    current_hash=$(shasum -a 256 "$current_dir/$crate_file" | awk '{ print $1 }')
    [[ "$current_hash" == "$manifest_hash" ]] || {
        echo "current crate digest differs from its manifest" >&2
        exit 1
    }
    [[ "$current_hash" == "$prior_hash" ]] || {
        echo "regenerated crate differs: prior=$prior_hash current=$current_hash" >&2
        exit 1
    }
    echo "regenerated crate matches the digest preserved in prior-attempt logs"
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
