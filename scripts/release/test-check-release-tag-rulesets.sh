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

[[ $# -eq 0 ]] || {
    echo "usage: $0" >&2
    exit 2
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
checker=$script_dir/check-release-tag-rulesets.sh
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
fake_bin=$test_dir/bin
mkdir "$fake_bin"

cat >"$fake_bin/gh" <<'FAKE_GH'
#!/usr/bin/env bash
set -euo pipefail

[[ "${1:-}" == api ]] || {
    echo "fake gh only supports the api command" >&2
    exit 90
}
[[ "${GH_TOKEN:-}" == read-token ]] || {
    echo "fake gh received an unexpected token" >&2
    exit 90
}

scenario=${FAKE_GH_SCENARIO:?FAKE_GH_SCENARIO is required}
endpoint=${!#}
list_endpoint='repos/scylladb/alternator-client-rust/rulesets?includes_parents=false&per_page=100'

has_api_version=false
has_accept=false
paginate=false
slurp=false
for argument in "$@"; do
    [[ "$argument" == 'X-GitHub-Api-Version: 2026-03-10' ]] && has_api_version=true
    [[ "$argument" == 'Accept: application/vnd.github+json' ]] && has_accept=true
    [[ "$argument" == --paginate ]] && paginate=true
    [[ "$argument" == --slurp ]] && slurp=true
done
[[ "$has_api_version" == true && "$has_accept" == true ]] || {
    echo "ruleset requests must send the API version and accept headers" >&2
    exit 90
}

if [[ "$endpoint" == "$list_endpoint" ]]; then
    [[ "$paginate" == true && "$slurp" == true ]] || {
        echo "ruleset list request must use --paginate --slurp" >&2
        exit 90
    }
    case "$scenario" in
        list_api_failure)
            echo "gh: service unavailable (HTTP 503)" >&2
            exit 1
            ;;
        partial_list)
            printf '%s\n' '[[{"id":101}]]'
            echo "gh: pagination failed" >&2
            exit 1
            ;;
        malformed_list)
            printf '%s\n' '[[{"id":101}]'
            ;;
        empty_list_response)
            ;;
        duplicate_list_id)
            printf '%s\n' '[[{"id":101}],[{"id":101},{"id":202}]]'
            ;;
        missing_creation)
            printf '%s\n' '[[{"id":202}]]'
            ;;
        missing_immutable)
            printf '%s\n' '[[{"id":101}]]'
            ;;
        *)
            # Two pages make the successful case prove that all pages are read.
            printf '%s\n' '[[{"id":101}],[{"id":202}]]'
            ;;
    esac
    exit 0
fi

[[ "$paginate" == false && "$slurp" == false ]] || {
    echo "ruleset detail request must not use pagination flags" >&2
    exit 90
}

case "$endpoint" in
    repos/scylladb/alternator-client-rust/rulesets/101\?includes_parents=false)
        ruleset_id=101
        ;;
    repos/scylladb/alternator-client-rust/rulesets/202\?includes_parents=false)
        ruleset_id=202
        ;;
    *)
        echo "fake gh received unexpected endpoint: $endpoint" >&2
        exit 90
        ;;
esac

if [[ "$scenario" == detail_api_failure && "$ruleset_id" -eq 202 ]]; then
    echo "gh: service unavailable (HTTP 503)" >&2
    exit 1
fi
if [[ "$scenario" == malformed_detail && "$ruleset_id" -eq 101 ]]; then
    printf '%s\n' '{"id":101'
    exit 0
fi
if [[ "$scenario" == missing_bypass_property && "$ruleset_id" -eq 101 ]]; then
    printf '%s\n' '{"id":101,"name":"redacted creation rule","target":"tag","source_type":"Repository","source":"scylladb/alternator-client-rust","enforcement":"active","conditions":{"ref_name":{"include":["refs/tags/v*"],"exclude":[]}},"rules":[{"type":"creation"}]}'
    exit 0
fi
if [[ "$scenario" == mismatched_detail_id && "$ruleset_id" -eq 101 ]]; then
    ruleset_id=999
fi

if [[ "$ruleset_id" -eq 101 ]]; then
    enforcement=active
    actor_id=777
    actor_type=Integration
    bypass_mode=always
    bypass='[{"actor_id":777,"actor_type":"Integration","bypass_mode":"always"}]'
    include='["refs/tags/v*"]'
    exclude='[]'
    rules='[{"type":"creation"}]'

    case "$scenario" in
        inactive_creation) enforcement=disabled ;;
        wrong_app) actor_id=778 ;;
        user_bypass) actor_type=User ;;
        team_bypass) actor_type=Team ;;
        exempt_bypass) bypass_mode=exempt ;;
        multiple_bypass)
            bypass='[{"actor_id":777,"actor_type":"Integration","bypass_mode":"always"},{"actor_id":42,"actor_type":"User","bypass_mode":"always"}]'
            ;;
        combined_mutable_bypass)
            rules='[{"type":"creation"},{"type":"update"},{"type":"deletion"}]'
            ;;
        wrong_scope) include='["refs/tags/v1*"]' ;;
        excluded_tag) exclude='["refs/tags/v0*"]' ;;
    esac

    if [[ "$scenario" != multiple_bypass ]]; then
        bypass="[{\"actor_id\":$actor_id,\"actor_type\":\"$actor_type\",\"bypass_mode\":\"$bypass_mode\"}]"
    fi
    printf '{"id":%s,"name":"release tag creation","target":"tag","source_type":"Repository","source":"scylladb/alternator-client-rust","enforcement":"%s","bypass_actors":%s,"conditions":{"ref_name":{"include":%s,"exclude":%s}},"rules":%s}\n' \
        "$ruleset_id" "$enforcement" "$bypass" "$include" "$exclude" "$rules"
else
    enforcement=active
    bypass='[]'
    rules='[{"type":"update"},{"type":"non_fast_forward"},{"type":"deletion"}]'
    [[ "$scenario" == inactive_immutable ]] && enforcement=evaluate
    if [[ "$scenario" == mutable_bypass ]]; then
        bypass='[{"actor_id":777,"actor_type":"Integration","bypass_mode":"always"}]'
    fi
    printf '{"id":%s,"name":"immutable release tags","target":"tag","source_type":"Repository","source":"scylladb/alternator-client-rust","enforcement":"%s","bypass_actors":%s,"conditions":{"ref_name":{"include":["refs/tags/v*"],"exclude":[]}},"rules":%s}\n' \
        "$ruleset_id" "$enforcement" "$bypass" "$rules"
fi
FAKE_GH
chmod 755 "$fake_bin/gh"

test_count=0
last_output=
last_status=0

run_checker() {
    local scenario=$1
    local output_file=$test_dir/output

    set +e
    FAKE_GH_SCENARIO=$scenario \
        GITHUB_REPOSITORY=scylladb/alternator-client-rust \
        GH_TOKEN=read-token \
        RELEASE_APP_ID=777 \
        PATH="$fake_bin:$PATH" \
        "$checker" >"$output_file" 2>&1
    last_status=$?
    set -e
    last_output=$(<"$output_file")
}

expect_success() {
    local scenario=$1

    run_checker "$scenario"
    [[ "$last_status" -eq 0 ]] || {
        echo "$scenario: expected success, got status $last_status" >&2
        echo "$last_output" >&2
        exit 1
    }
    test_count=$((test_count + 1))
}

expect_failure() {
    local scenario=$1
    local diagnostic=$2

    run_checker "$scenario"
    [[ "$last_status" -ne 0 ]] || {
        echo "$scenario: expected failure" >&2
        echo "$last_output" >&2
        exit 1
    }
    [[ "$last_output" == *"$diagnostic"* ]] || {
        echo "$scenario: missing diagnostic: $diagnostic" >&2
        echo "$last_output" >&2
        exit 1
    }
    test_count=$((test_count + 1))
}

expect_success valid
[[ "$last_output" == *"active and correctly separated"* ]] || {
    echo "valid: missing success diagnostic" >&2
    exit 1
}

for scenario in \
    missing_creation missing_immutable inactive_creation inactive_immutable \
    wrong_app user_bypass team_bypass exempt_bypass multiple_bypass \
    combined_mutable_bypass mutable_bypass wrong_scope excluded_tag; do
    expect_failure "$scenario" 'release tag rulesets are unsafe'
done

expect_failure list_api_failure 'failed to query every page'
expect_failure partial_list 'failed to query every page'
expect_failure malformed_list 'empty, malformed, or duplicate response'
expect_failure empty_list_response 'empty, malformed, or duplicate response'
expect_failure duplicate_list_id 'empty, malformed, or duplicate response'
expect_failure detail_api_failure 'failed to read repository ruleset 202'
expect_failure malformed_detail 'malformed data for ruleset 101'
expect_failure missing_bypass_property 'malformed data for ruleset 101'
expect_failure mismatched_detail_id 'malformed data for ruleset 101'

contract_output=$test_dir/contract-output

check_contract_failure() {
    local expected_status=$1
    local diagnostic=$2
    shift 2

    set +e
    "$@" >"$contract_output" 2>&1
    status=$?
    set -e
    [[ "$status" -eq "$expected_status" && "$(<"$contract_output")" == *"$diagnostic"* ]] || {
        echo "contract check failed: expected status $expected_status and '$diagnostic'" >&2
        cat "$contract_output" >&2
        exit 1
    }
    test_count=$((test_count + 1))
}

check_contract_failure 2 'usage:' "$checker" unexpected
check_contract_failure 1 'GITHUB_REPOSITORY is required' \
    env -u GITHUB_REPOSITORY GH_TOKEN=read-token RELEASE_APP_ID=777 "$checker"
check_contract_failure 1 'owner/repository form' \
    env GITHUB_REPOSITORY=invalid GH_TOKEN=read-token RELEASE_APP_ID=777 "$checker"
check_contract_failure 1 'GH_TOKEN is required' \
    env -u GH_TOKEN GITHUB_REPOSITORY=scylladb/alternator-client-rust RELEASE_APP_ID=777 "$checker"
check_contract_failure 1 'RELEASE_APP_ID must be a positive integer' \
    env -u RELEASE_APP_ID GITHUB_REPOSITORY=scylladb/alternator-client-rust GH_TOKEN=read-token "$checker"
check_contract_failure 1 'RELEASE_APP_ID must be a positive integer' \
    env GITHUB_REPOSITORY=scylladb/alternator-client-rust GH_TOKEN=read-token RELEASE_APP_ID=team "$checker"

echo "$test_count release tag ruleset checker tests passed"
