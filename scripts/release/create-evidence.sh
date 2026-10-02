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
release_script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

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
portable_matrix=$(bash "$release_script_dir/release-policy.sh" portable-matrix)
scylla_matrix=$(bash "$release_script_dir/release-policy.sh" scylla-matrix)

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
    --argjson portable_matrix "$portable_matrix" \
    --argjson scylla_matrix "$scylla_matrix" \
    --slurpfile jobs "$jobs_file" \
    '{
        schema_version: 1,
        crate: {name: "alternator-client", version: $version},
        rc_tag: $rc_tag,
        commit_sha: $commit_sha,
        repository: $repository,
        workflow_run_id: $workflow_run_id,
        workflow_run_attempt: $workflow_run_attempt,
        expected_portable_targets: $portable_matrix,
        expected_scylla_targets: $scylla_matrix,
        aggregate_results: {
            portable: $portable_result,
            static_release: $static_result,
            scylla: $scylla_result
        },
        all_required_gates_passed: $all_passed,
        jobs: $jobs[0]
    }' >"$output_file"

jq -e '.jobs | length > 0' "$output_file" >/dev/null
