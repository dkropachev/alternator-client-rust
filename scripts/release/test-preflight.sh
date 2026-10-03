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

command -v jq >/dev/null || {
    echo "jq is required to run preflight tests" >&2
    exit 1
}

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
preflight=$script_dir/preflight.sh
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
fake_bin=$test_dir/bin
fixture=$test_dir/repository
mkdir "$fake_bin" "$fixture"

cat >"$fake_bin/git" <<'FAKE_GIT'
#!/usr/bin/env bash
set -euo pipefail

has_line() {
    local lines=$1
    local wanted=$2
    local line

    while IFS= read -r line; do
        [[ "$line" == "$wanted" ]] && return 0
    done <<<"$lines"
    return 1
}

case ${1:-} in
    fetch)
        [[ $# -eq 7 && "$2" == --force && "$3" == --prune && "$4" == --prune-tags && \
            "$5" == origin && "$6" == '+refs/heads/main:refs/remotes/origin/main' && \
            "$7" == '+refs/tags/*:refs/tags/*' ]] || {
            echo "unexpected fake git fetch invocation: $*" >&2
            exit 90
        }
        ;;
    rev-parse)
        if [[ $# -eq 2 && "$2" == HEAD ]]; then
            printf '%s\n' "${FAKE_HEAD:?FAKE_HEAD is required}"
        elif [[ $# -eq 2 && "$2" == refs/remotes/origin/main ]]; then
            printf '%s\n' "${FAKE_MAIN:?FAKE_MAIN is required}"
        elif [[ $# -eq 4 && "$2" == -q && "$3" == --verify && \
            "$4" == "refs/tags/$FAKE_FINAL_TAG" ]]; then
            [[ "${FAKE_FINAL_EXISTS:-false}" == true ]] || exit 1
            printf '%s\n' "${FAKE_FINAL_COMMIT:?FAKE_FINAL_COMMIT is required}"
        else
            echo "unexpected fake git rev-parse invocation: $*" >&2
            exit 90
        fi
        ;;
    tag)
        [[ $# -eq 3 && "$2" == -l && "$3" == "v$FAKE_VERSION-rc.*" ]] || {
            echo "unexpected fake git tag invocation: $*" >&2
            exit 90
        }
        [[ -z "${FAKE_RC_TAGS:-}" ]] || printf '%s\n' "$FAKE_RC_TAGS"
        ;;
    cat-file)
        if [[ $# -eq 3 && "$2" == -t ]]; then
            if has_line "${FAKE_ANNOTATED_TAGS:-}" "$3"; then
                printf '%s\n' tag
            elif has_line "${FAKE_RC_TAGS:-}" "$3"; then
                printf '%s\n' commit
            else
                exit 1
            fi
        elif [[ $# -eq 3 && "$2" == tag ]] && has_line "${FAKE_ANNOTATED_TAGS:-}" "$3"; then
            printf 'object %s\ntype %s\ntag %s\n\nRelease candidate %s\n\n' \
                "${FAKE_RC_COMMIT:?FAKE_RC_COMMIT is required}" \
                "${FAKE_TAG_OBJECT_TYPE:-commit}" "$3" "$3"
            if has_line "${FAKE_OWNED_TAGS:-}" "$3"; then
                printf 'workflow-run: %s\n' "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
                if [[ "${FAKE_DUPLICATE_RUN_MARKER:-false}" == true ]]; then
                    printf 'workflow-run: %s\n' "$GITHUB_RUN_ID"
                fi
            else
                printf 'workflow-run: another-run\n'
            fi
            printf 'commit: %s\n' "${FAKE_TAG_MESSAGE_COMMIT:-$FAKE_RC_COMMIT}"
            if [[ "${FAKE_DUPLICATE_COMMIT_MARKER:-false}" == true ]]; then
                printf 'commit: %s\n' "${FAKE_TAG_MESSAGE_COMMIT:-$FAKE_RC_COMMIT}"
            fi
        else
            echo "unexpected fake git cat-file invocation: $*" >&2
            exit 90
        fi
        ;;
    rev-list)
        [[ $# -eq 4 && "$2" == -n && "$3" == 1 ]] || {
            echo "unexpected fake git rev-list invocation: $*" >&2
            exit 90
        }
        if [[ "$4" == "$FAKE_FINAL_TAG" ]]; then
            printf '%s\n' "${FAKE_FINAL_COMMIT:?FAKE_FINAL_COMMIT is required}"
        else
            printf '%s\n' "${FAKE_RC_COMMIT:?FAKE_RC_COMMIT is required}"
        fi
        ;;
    *)
        echo "unexpected fake git invocation: $*" >&2
        exit 90
        ;;
esac
FAKE_GIT

cat >"$fake_bin/cargo" <<'FAKE_CARGO'
#!/usr/bin/env bash
set -euo pipefail

[[ "$*" == 'metadata --locked --no-deps --format-version 1' ]] || {
    echo "unexpected fake cargo invocation: $*" >&2
    exit 90
}
printf '{"packages":[{"name":"%s","version":"%s"}]}\n' \
    "${FAKE_CARGO_NAME:-alternator-client}" "${FAKE_CARGO_VERSION:?FAKE_CARGO_VERSION is required}"
FAKE_CARGO

cat >"$fake_bin/curl" <<'FAKE_CURL'
#!/usr/bin/env bash
set -euo pipefail

output=
user_agent=
url=
while [[ $# -gt 0 ]]; do
    case $1 in
        -A)
            user_agent=$2
            shift 2
            ;;
        -o)
            output=$2
            shift 2
            ;;
        -w)
            [[ "$2" == '%{http_code}' ]] || exit 90
            shift 2
            ;;
        -sS)
            shift
            ;;
        http*)
            url=$1
            shift
            ;;
        *)
            echo "unexpected fake curl argument: $1" >&2
            exit 90
            ;;
    esac
done

[[ "$user_agent" == alternator-client-release-workflow ]] || {
    echo "unexpected fake curl user agent: $user_agent" >&2
    exit 90
}
[[ "$url" == https://index.crates.io/al/te/alternator-client ]] || {
    echo "unexpected fake curl URL: $url" >&2
    exit 90
}
[[ -n "$output" ]] || exit 90
[[ "${FAKE_CURL_FAIL:-false}" != true ]] || exit 22
printf '%s' "${FAKE_INDEX_BODY:-}" >"$output"
printf '%s' "${FAKE_INDEX_STATUS:-200}"
FAKE_CURL
chmod 755 "$fake_bin/git" "$fake_bin/cargo" "$fake_bin/curl"

target=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
other=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
third=cccccccccccccccccccccccccccccccccccccccc
version=1.2.3
run_id=987654321
test_count=0
last_output=
last_status=0

write_fixture() {
    local fixture_version=$1

    cat >"$fixture/Cargo.lock" <<EOF
[[package]]
name = "alternator-client"
version = "$fixture_version"
EOF
    printf '# Changelog\n\n## [%s] - 2026-10-02\n' "$fixture_version" >"$fixture/CHANGELOG.md"
}

reset_case() {
    export GITHUB_REF=refs/heads/main
    export GITHUB_SHA=$target
    export GITHUB_RUN_ID=$run_id
    export FAKE_HEAD=$target
    export FAKE_MAIN=$target
    export FAKE_VERSION=$version
    export FAKE_FINAL_TAG=v$version
    export FAKE_FINAL_EXISTS=false
    export FAKE_FINAL_COMMIT=$target
    export FAKE_RC_TAGS=
    export FAKE_ANNOTATED_TAGS=
    export FAKE_OWNED_TAGS=
    export FAKE_RC_COMMIT=$target
    export FAKE_TAG_OBJECT_TYPE=commit
    export FAKE_DUPLICATE_RUN_MARKER=false
    export FAKE_DUPLICATE_COMMIT_MARKER=false
    export FAKE_TAG_MESSAGE_COMMIT=$target
    export FAKE_CARGO_NAME=alternator-client
    export FAKE_CARGO_VERSION=$version
    export FAKE_INDEX_STATUS=200
    export FAKE_INDEX_BODY='{"vers":"0.9.0","yanked":false}'
    export FAKE_CURL_FAIL=false
    write_fixture "$version"
}

run_preflight() {
    local scenario=$1
    shift
    local output_file=$test_dir/output

    set +e
    (
        cd "$fixture"
        PATH="$fake_bin:$PATH" "$preflight" "$@"
    ) >"$output_file" 2>&1
    last_status=$?
    set -e
    last_output=$(<"$output_file")
    printf '%s' "$scenario" >"$test_dir/last-scenario"
}

expect_success() {
    local scenario=$1
    shift

    run_preflight "$scenario" "$@"
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
    shift 2

    run_preflight "$scenario" "$@"
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

reset_case
expect_failure argument_contract 'usage:'
[[ "$last_status" -eq 2 ]] || {
    echo "argument_contract: expected usage status 2, got $last_status" >&2
    exit 1
}

reset_case
expect_failure invalid_mode 'mode must be validate or release' publish "$version" "$target"

reset_case
expect_failure invalid_version 'version must be an exact X.Y.Z release' release 01.2.3 "$target"

reset_case
expect_failure invalid_target_sha 'target commit must be a lowercase 40-character SHA' \
    release "$version" AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

reset_case
GITHUB_REF=refs/heads/topic
expect_failure wrong_dispatch_ref 'must be dispatched from main' release "$version" "$target"

reset_case
GITHUB_SHA=$other
expect_failure target_workflow_mismatch 'does not match workflow SHA' release "$version" "$target"

reset_case
FAKE_HEAD=$other
expect_failure target_head_mismatch 'does not match checkout HEAD' release "$version" "$target"

reset_case
unset GITHUB_RUN_ID
expect_failure missing_release_run_id 'GITHUB_RUN_ID is required in release mode' \
    release "$version" "$target"

reset_case
expect_success current_main_release release "$version" "$target"
[[ "$last_output" == *"release preflight passed"* ]] || {
    echo "current_main_release: missing success diagnostic" >&2
    exit 1
}

reset_case
unset GITHUB_RUN_ID
expect_success current_main_validation validate "$version" "$target"

reset_case
FAKE_MAIN=$other
expect_failure stale_new_release 'this run owns no RC' release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
expect_failure stale_validation_rerun 'start a new run' validate "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
expect_success same_run_stale_release_recovery release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
expect_failure other_run_rc_does_not_authorize_recovery 'this run owns no RC' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_RC_COMMIT=$third
expect_failure owned_rc_wrong_commit 'does not directly reference target commit' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_DUPLICATE_RUN_MARKER=true
expect_failure duplicate_run_marker 'duplicate or malformed workflow-run annotations' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_TAG_MESSAGE_COMMIT=$third
expect_failure wrong_commit_annotation 'does not have exactly one commit annotation' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_DUPLICATE_COMMIT_MARKER=true
expect_failure duplicate_commit_annotation 'does not have exactly one commit annotation' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_TAG_OBJECT_TYPE=tag
expect_failure indirect_tag_object 'does not directly reference target commit' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=$'v1.2.3-rc.4\nv1.2.3-rc.5'
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
expect_failure multiple_owned_rcs 'multiple numeric RC tags' release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
expect_failure lightweight_rc_does_not_authorize_recovery 'this run owns no RC' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.not-a-number
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
expect_failure nonnumeric_rc_does_not_authorize_recovery 'this run owns no RC' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_INDEX_BODY='{"vers":"1.2.3","yanked":false}'
expect_success exact_registry_same_run_recovery release "$version" "$target"

reset_case
FAKE_INDEX_BODY='{"vers":"1.2.3","yanked":false}'
expect_failure exact_registry_without_owned_rc 'already published outside this run' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_FINAL_EXISTS=true
expect_success final_tag_same_run_recovery release "$version" "$target"

reset_case
FAKE_FINAL_EXISTS=true
expect_failure final_tag_without_owned_rc 'final tag v1.2.3 already exists outside this run' \
    release "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_FINAL_EXISTS=true
FAKE_FINAL_COMMIT=$third
expect_failure final_tag_wrong_commit 'final tag v1.2.3 points to' release "$version" "$target"

reset_case
FAKE_FINAL_EXISTS=true
FAKE_FINAL_COMMIT=$third
FAKE_INDEX_BODY='{"vers":"1.2.3","yanked":true}'
expect_success validation_existing_release_is_informational validate "$version" "$target"

reset_case
FAKE_INDEX_STATUS=404
expect_failure unclaimed_regular_version 'unclaimed on crates.io' validate "$version" "$target"

reset_case
FAKE_VERSION=1.0.0
FAKE_FINAL_TAG=v1.0.0
FAKE_CARGO_VERSION=1.0.0
write_fixture 1.0.0
FAKE_INDEX_STATUS=404
expect_failure unclaimed_1_0_0 'unclaimed on crates.io' release 1.0.0 "$target"

reset_case
FAKE_INDEX_STATUS=503
expect_failure registry_http_failure 'returned HTTP 503' validate "$version" "$target"

reset_case
FAKE_CURL_FAIL=true
expect_failure registry_transport_failure 'failed to query the crates.io sparse index' \
    validate "$version" "$target"

reset_case
FAKE_INDEX_BODY='{"vers":'
expect_failure malformed_registry_response 'returned malformed data' validate "$version" "$target"

reset_case
FAKE_INDEX_BODY=
expect_failure empty_registry_response 'returned malformed data' validate "$version" "$target"

reset_case
FAKE_INDEX_BODY='[]'
expect_failure malformed_registry_shape 'returned malformed data' validate "$version" "$target"

reset_case
FAKE_INDEX_BODY=$'{"vers":"1.2.3","yanked":false}\n{"vers":"1.2.3","yanked":false}'
expect_failure duplicate_registry_records 'contains duplicate' validate "$version" "$target"

reset_case
FAKE_MAIN=$other
FAKE_RC_TAGS=v$version-rc.4
FAKE_ANNOTATED_TAGS=$FAKE_RC_TAGS
FAKE_OWNED_TAGS=$FAKE_RC_TAGS
FAKE_INDEX_BODY='{"vers":"1.2.3","yanked":true}'
expect_failure yanked_release_recovery 'is yanked' release "$version" "$target"

reset_case
FAKE_CARGO_NAME=unexpected-package
expect_failure metadata_name_mismatch 'expected package alternator-client' validate "$version" "$target"

reset_case
FAKE_CARGO_VERSION=9.9.9
expect_failure metadata_version_mismatch 'Cargo.toml version 9.9.9 does not match' \
    validate "$version" "$target"

reset_case
sed -i 's/version = "1.2.3"/version = "9.9.9"/' "$fixture/Cargo.lock"
expect_failure lock_version_mismatch 'Cargo.lock version 9.9.9 does not match' \
    validate "$version" "$target"

reset_case
printf '# Changelog\n' >"$fixture/CHANGELOG.md"
expect_failure missing_changelog_entry 'CHANGELOG.md has no dated [1.2.3] release entry' \
    validate "$version" "$target"

echo "$test_count preflight tests passed"
