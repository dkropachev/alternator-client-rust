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

[[ $# -eq 6 ]] || {
    echo "usage: $0 MODE VERSION CANDIDATE_ID COMMIT_SHA CRATE_SHA256 OUTPUT_FILE" >&2
    exit 2
}

release_mode=$1
version=$2
candidate_id=$3
commit_sha=$4
crate_sha256=$5
output_file=$6
release_script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

[[ -n "${GITHUB_REPOSITORY:-}" && -n "${GITHUB_RUN_ID:-}" ]] || {
    echo "GitHub run context is required" >&2
    exit 1
}
[[ "$GITHUB_RUN_ID" =~ ^[1-9][0-9]*$ ]] || {
    echo "GITHUB_RUN_ID must be numeric" >&2
    exit 1
}
[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
    echo "invalid release version: $version" >&2
    exit 1
}
case "$release_mode" in
    release)
        rc_prefix="v$version-rc."
        [[ "$candidate_id" == "$rc_prefix"* ]] || {
            echo "invalid release candidate ID: $candidate_id" >&2
            exit 1
        }
        rc_number=${candidate_id#"$rc_prefix"}
        [[ "$rc_number" =~ ^[1-9][0-9]*$ ]] || {
            echo "invalid release candidate ID: $candidate_id" >&2
            exit 1
        }
        rc_tag=$candidate_id
        ;;
    validate)
        [[ "$candidate_id" == "validation-$GITHUB_RUN_ID" ]] || {
            echo "validation candidate ID must be validation-$GITHUB_RUN_ID" >&2
            exit 1
        }
        rc_tag=
        ;;
    *)
        echo "invalid release mode: $release_mode" >&2
        exit 1
        ;;
esac
[[ "$commit_sha" =~ ^[0-9a-f]{40}$ ]] || {
    echo "invalid commit SHA: $commit_sha" >&2
    exit 1
}
[[ "$crate_sha256" =~ ^[0-9a-f]{64}$ ]] || {
    echo "invalid crate SHA-256: $crate_sha256" >&2
    exit 1
}

for command in gh jq; do
    command -v "$command" >/dev/null || {
        echo "required command is missing: $command" >&2
        exit 1
    }
done

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
attestation_result=${ATTESTATION_RESULT:?ATTESTATION_RESULT is required}
for result in "$attestation_result" "$portable_result" "$static_result" "$scylla_result"; do
    case "$result" in
        success | failure | cancelled | skipped) ;;
        *)
            echo "invalid aggregate job result: $result" >&2
            exit 1
            ;;
    esac
done
all_passed=false
attestation_passed=false
if [[ "$release_mode" == release && "$attestation_result" == success ]]; then
    attestation_passed=true
elif [[ "$release_mode" == validate && "$attestation_result" == skipped ]]; then
    attestation_passed=true
fi
if [[ "$attestation_passed" == true && "$portable_result" == success && "$static_result" == success && "$scylla_result" == success ]]; then
    all_passed=true
fi
portable_matrix=$(bash "$release_script_dir/release-policy.sh" portable-matrix)
scylla_matrix=$(bash "$release_script_dir/release-policy.sh" scylla-matrix)

jq -n \
    --arg release_mode "$release_mode" \
    --arg version "$version" \
    --arg candidate_id "$candidate_id" \
    --arg rc_tag "$rc_tag" \
    --arg commit_sha "$commit_sha" \
    --arg crate_sha256 "$crate_sha256" \
    --arg repository "$GITHUB_REPOSITORY" \
    --arg workflow_run_id "$GITHUB_RUN_ID" \
    --arg workflow_run_attempt "${GITHUB_RUN_ATTEMPT:-1}" \
    --arg attestation_result "$attestation_result" \
    --arg portable_result "$portable_result" \
    --arg static_result "$static_result" \
    --arg scylla_result "$scylla_result" \
    --argjson all_passed "$all_passed" \
    --argjson portable_matrix "$portable_matrix" \
    --argjson scylla_matrix "$scylla_matrix" \
    --slurpfile jobs "$jobs_file" \
    '{
        schema_version: 2,
        release_mode: $release_mode,
        candidate_id: $candidate_id,
        crate: {name: "alternator-client", version: $version, sha256: $crate_sha256},
        rc_tag: (if $release_mode == "release" then $rc_tag else null end),
        commit_sha: $commit_sha,
        repository: $repository,
        workflow_run_id: $workflow_run_id,
        workflow_run_attempt: $workflow_run_attempt,
        expected_portable_targets: $portable_matrix,
        expected_scylla_targets: $scylla_matrix,
        aggregate_results: {
            attestation: $attestation_result,
            portable: $portable_result,
            static_release: $static_result,
            scylla: $scylla_result
        },
        all_required_gates_passed: $all_passed,
        jobs: $jobs[0]
    }' >"$output_file"

jq -e \
    --arg release_mode "$release_mode" \
    --arg version "$version" \
    --arg candidate_id "$candidate_id" \
    --arg commit_sha "$commit_sha" \
    --arg crate_sha256 "$crate_sha256" '
    .schema_version == 2 and
    .release_mode == $release_mode and
    .candidate_id == $candidate_id and
    .crate.name == "alternator-client" and
    .crate.version == $version and
    .crate.sha256 == $crate_sha256 and
    .commit_sha == $commit_sha and
    (.jobs | length > 0) and
    (if $release_mode == "release"
     then .rc_tag == $candidate_id
     else .rc_tag == null and .workflow_run_id == ($candidate_id | sub("^validation-"; ""))
     end)
' "$output_file" >/dev/null
