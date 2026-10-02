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

[[ $# -eq 4 ]] || {
    echo "usage: $0 VERSION RC_TAG COMMIT_SHA OUTPUT_FILE" >&2
    exit 2
}

version=$1
rc_tag=$2
commit_sha=$3
output_file=$4

[[ -n "${GITHUB_REPOSITORY:-}" && -n "${GITHUB_RUN_ID:-}" ]] || {
    echo "GitHub run context is required" >&2
    exit 1
}

jobs_file=$(mktemp)
gh api --paginate --slurp \
    -H 'X-GitHub-Api-Version: 2022-11-28' \
    "repos/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID/jobs?per_page=100&filter=all" \
    | jq '[.[].jobs[] | {
        id,
        name,
        runner_name,
        status,
        conclusion,
        started_at,
        completed_at,
        html_url
    }]' >"$jobs_file"

portable_result=${PORTABLE_RESULT:?PORTABLE_RESULT is required}
static_result=${STATIC_RESULT:?STATIC_RESULT is required}
scylla_result=${SCYLLA_RESULT:?SCYLLA_RESULT is required}
all_passed=false
if [[ "$portable_result" == success && "$static_result" == success && "$scylla_result" == success ]]; then
    all_passed=true
fi

jq -n \
    --arg version "$version" \
    --arg rc_tag "$rc_tag" \
    --arg commit_sha "$commit_sha" \
    --arg repository "$GITHUB_REPOSITORY" \
    --arg workflow_run_id "$GITHUB_RUN_ID" \
    --arg workflow_run_attempt "${GITHUB_RUN_ATTEMPT:-1}" \
    --arg portable_result "$portable_result" \
    --arg static_result "$static_result" \
    --arg scylla_result "$scylla_result" \
    --argjson all_passed "$all_passed" \
    --slurpfile jobs "$jobs_file" \
    '{
        schema_version: 1,
        crate: {name: "alternator-client", version: $version},
        rc_tag: $rc_tag,
        commit_sha: $commit_sha,
        repository: $repository,
        workflow_run_id: $workflow_run_id,
        workflow_run_attempt: $workflow_run_attempt,
        expected_portable_targets: [
            {runner: "ubuntu-24.04", target: "x86_64-unknown-linux-gnu"},
            {runner: "ubuntu-24.04-arm", target: "aarch64-unknown-linux-gnu"},
            {runner: "macos-15-intel", target: "x86_64-apple-darwin"},
            {runner: "macos-15", target: "aarch64-apple-darwin"}
        ],
        expected_scylla_targets: [
            {runner: "ubuntu-24.04", arch: "x86_64", version: "2026.1.14"},
            {runner: "ubuntu-24.04", arch: "x86_64", version: "2025.1.16"},
            {runner: "ubuntu-24.04-arm", arch: "aarch64", version: "2026.1.14"},
            {runner: "ubuntu-24.04-arm", arch: "aarch64", version: "2025.1.16"}
        ],
        aggregate_results: {
            portable: $portable_result,
            static_release: $static_result,
            scylla: $scylla_result
        },
        all_required_gates_passed: $all_passed,
        jobs: $jobs[0]
    }' >"$output_file"

jq -e '.jobs | length > 0' "$output_file" >/dev/null
