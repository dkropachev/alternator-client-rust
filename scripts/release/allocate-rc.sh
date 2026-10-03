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
    echo "usage: $0 VERSION COMMIT_SHA RC_TAG PLAN_ACTION" >&2
    exit 2
}

version=$1
commit_sha=$2
rc_tag=$3
plan_action=$4
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
run_id=${GITHUB_RUN_ID:-}
run_attempt=${GITHUB_RUN_ATTEMPT:-}
expected_app_slug=scylladb-alternator-client-release

[[ "${RELEASE_MODE:-}" == release ]] || {
    echo "allocate-rc.sh requires RELEASE_MODE=release" >&2
    exit 1
}
[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
    echo "version must be an exact X.Y.Z release without leading zeroes" >&2
    exit 1
}
[[ "$commit_sha" =~ ^[0-9a-f]{40}$ ]] || {
    echo "commit SHA must be a lowercase 40-character hexadecimal value" >&2
    exit 1
}
[[ "$rc_tag" =~ ^v${version//./\.}-rc\.[1-9][0-9]*$ ]] || {
    echo "RC_TAG must be a numeric release-candidate tag for version $version" >&2
    exit 1
}
[[ "$run_id" =~ ^[1-9][0-9]*$ ]] || {
    echo "GITHUB_RUN_ID must be a positive integer" >&2
    exit 1
}
[[ "$run_attempt" =~ ^[1-9][0-9]*$ ]] || {
    echo "GITHUB_RUN_ATTEMPT must be a positive integer" >&2
    exit 1
}
[[ "$plan_action" == create || "$plan_action" == recover ]] || {
    echo "PLAN_ACTION must be create or recover" >&2
    exit 1
}
[[ -n "${GH_TOKEN:-}" ]] || {
    echo "GH_TOKEN is required for release safety reads" >&2
    exit 1
}
[[ "${RELEASE_APP_ID:-}" =~ ^[1-9][0-9]*$ ]] || {
    echo "RELEASE_APP_ID must be a positive integer" >&2
    exit 1
}

# Audit protection before doing even local allocation work. The built-in token
# is deliberately retained in GH_TOKEN for this read-only check.
"$script_dir/check-release-tag-rulesets.sh"

plan_output=$(mktemp)
trap 'rm -f -- "$plan_output"' EXIT
GITHUB_OUTPUT=$plan_output "$script_dir/plan-rc.sh" \
    "$version" "$commit_sha" >/dev/null

planned_rc_count=$(awk -F= '$1 == "rc_tag" { count++ } END { print count + 0 }' "$plan_output")
planned_action_count=$(awk -F= '$1 == "action" { count++ } END { print count + 0 }' "$plan_output")
[[ "$planned_rc_count" -eq 1 && "$planned_action_count" -eq 1 ]] || {
    echo "RC planner returned malformed output" >&2
    exit 1
}
planned_rc_tag=$(awk -F= '$1 == "rc_tag" { sub(/^[^=]*=/, ""); print }' "$plan_output")
planned_action=$(awk -F= '$1 == "action" { sub(/^[^=]*=/, ""); print }' "$plan_output")

[[ "$planned_rc_tag" == "$rc_tag" && "$planned_action" == "$plan_action" ]] || {
    echo "RC plan changed after re-fetch: expected $rc_tag/$plan_action, found $planned_rc_tag/$planned_action" >&2
    exit 1
}

if [[ "$plan_action" == create ]]; then
    [[ "${RELEASE_APP_SLUG:-}" == "$expected_app_slug" ]] || {
        echo "RELEASE_APP_SLUG must be $expected_app_slug" >&2
        exit 1
    }
    [[ -n "${RELEASE_APP_TOKEN:-}" ]] || {
        echo "RELEASE_APP_TOKEN is required to push a new RC tag" >&2
        exit 1
    }
    [[ "$RELEASE_APP_TOKEN" != *[[:space:]]* ]] || {
        echo "RELEASE_APP_TOKEN contains whitespace" >&2
        exit 1
    }
    [[ "${RELEASE_APP_BOT_ID:-}" =~ ^[1-9][0-9]*$ ]] || {
        echo "RELEASE_APP_BOT_ID must be a positive integer" >&2
        exit 1
    }
    [[ -n "${GITHUB_REPOSITORY:-}" ]] || {
        echo "GITHUB_REPOSITORY is required" >&2
        exit 1
    }

    require_current_main() {
        git fetch --force origin \
            '+refs/heads/main:refs/remotes/origin/main'
        current_main=$(git rev-parse --verify refs/remotes/origin/main)
        [[ "$commit_sha" == "$current_main" ]] || {
            echo "cannot create $rc_tag: target commit $commit_sha is not current origin/main $current_main" >&2
            exit 1
        }
    }

    app_slug=$RELEASE_APP_SLUG
    bot_name="$app_slug[bot]"
    bot_email="$RELEASE_APP_BOT_ID+$app_slug[bot]@users.noreply.github.com"
    require_current_main
    git -c "user.name=$bot_name" -c "user.email=$bot_email" \
        tag -a "$rc_tag" "$commit_sha" -m "Release candidate $rc_tag

workflow-run: $run_id
workflow-attempt-created: $run_attempt
commit: $commit_sha"

    created_local_tag=true
    cleanup_local_tag() {
        status=$?
        if [[ "$status" -ne 0 && "$created_local_tag" == true ]]; then
            git tag -d "$rc_tag" >/dev/null 2>&1 || true
        fi
        rm -f -- "$plan_output"
        exit "$status"
    }
    trap cleanup_local_tag EXIT

    basic_auth=$(printf 'x-access-token:%s' "$RELEASE_APP_TOKEN" | base64 | tr -d '\r\n')

    # Main, blocker, and ruleset state can all change during a run. Recheck
    # each at the irreversible boundary, with tag protection last so the App
    # bypass and immutability rules are the state observed closest to the push.
    require_current_main
    "$script_dir/check-release-blockers.sh"
    "$script_dir/check-release-tag-rulesets.sh"

    GIT_CONFIG_COUNT=2 \
        GIT_CONFIG_KEY_0=http.https://github.com/.extraheader \
        GIT_CONFIG_VALUE_0= \
        GIT_CONFIG_KEY_1=http.https://github.com/.extraheader \
        GIT_CONFIG_VALUE_1="AUTHORIZATION: basic $basic_auth" \
        GIT_CONFIG_GLOBAL=/dev/null \
        GIT_CONFIG_NOSYSTEM=1 \
        git push "https://github.com/$GITHUB_REPOSITORY.git" "refs/tags/$rc_tag"
    created_local_tag=false
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    {
        echo "rc_tag=$rc_tag"
        echo "action=$plan_action"
    } >>"$GITHUB_OUTPUT"
fi

echo "$rc_tag ($plan_action)"
