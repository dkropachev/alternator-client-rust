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
planner=$script_dir/plan-rc.sh
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
fake_bin=$test_dir/bin
mkdir "$fake_bin"

version=9.8.7
commit=0123456789abcdef0123456789abcdef01234567
other_commit=abcdef0123456789abcdef0123456789abcdef01
noncommit=1111111111111111111111111111111111111111
run_id=24680

cat >"$fake_bin/git" <<'FAKE_GIT'
#!/usr/bin/env bash
set -euo pipefail

scenario=${FAKE_GIT_SCENARIO:?FAKE_GIT_SCENARIO is required}
version=${FAKE_VERSION:?FAKE_VERSION is required}
commit=${FAKE_COMMIT:?FAKE_COMMIT is required}
other_commit=${FAKE_OTHER_COMMIT:?FAKE_OTHER_COMMIT is required}
noncommit=${FAKE_NONCOMMIT:?FAKE_NONCOMMIT is required}
run_id=${FAKE_RUN_ID:?FAKE_RUN_ID is required}
expected_input=${FAKE_EXPECTED_INPUT:?FAKE_EXPECTED_INPUT is required}
log=${FAKE_GIT_LOG:?FAKE_GIT_LOG is required}

die() {
    echo "fake git: $*" >&2
    exit 90
}

print_tags() {
    case "$scenario" in
        empty)
            ;;
        fresh)
            printf 'v%s-rc.1\nv%s-rc.2\n' "$version" "$version"
            ;;
        mixed)
            printf '%s\n' \
                "v$version-rc.0" \
                "v$version-rc.01" \
                "v$version-rc.bad" \
                "v$version-rc.6.extra" \
                'v9.8.6-rc.99' \
                'not-a-release-tag' \
                "v$version-rc.5" \
                "v$version-rc.7"
            ;;
        duplicate_owned)
            printf 'v%s-rc.2\nv%s-rc.3\n' "$version" "$version"
            ;;
        huge_suffix)
            printf 'v%s-rc.1000000000000000000\n' "$version"
            ;;
        same_run | duplicate_run | malformed_run | direct_object_wrong | \
            indirect_object | missing_commit | wrong_commit | duplicate_commit | \
            peeled_mismatch)
            printf 'v%s-rc.3\n' "$version"
            ;;
        *)
            die "unexpected ref enumeration for scenario $scenario"
            ;;
    esac
}

tag_type() {
    local tag=$1

    case "$scenario" in
        fresh)
            [[ "$tag" == "v$version-rc.1" || "$tag" == "v$version-rc.2" ]] || \
                die "unexpected tag in fresh scenario: $tag"
            printf '%s\n' tag
            ;;
        mixed)
            case "$tag" in
                "v$version-rc.5") printf '%s\n' commit ;;
                "v$version-rc.7") printf '%s\n' tag ;;
                *) die "malformed or unrelated tag was inspected: $tag" ;;
            esac
            ;;
        duplicate_owned)
            [[ "$tag" == "v$version-rc.2" || "$tag" == "v$version-rc.3" ]] || \
                die "unexpected tag in duplicate-owned scenario: $tag"
            printf '%s\n' tag
            ;;
        same_run | duplicate_run | malformed_run | direct_object_wrong | \
            indirect_object | missing_commit | wrong_commit | duplicate_commit | \
            peeled_mismatch)
            [[ "$tag" == "v$version-rc.3" ]] || die "unexpected tag: $tag"
            printf '%s\n' tag
            ;;
        *)
            die "tag type requested in scenario $scenario"
            ;;
    esac
}

print_tag_object() {
    local tag=$1
    local object=$commit
    local object_type=commit

    case "$scenario" in
        direct_object_wrong) object=$other_commit ;;
        indirect_object)
            object=$noncommit
            object_type=tag
            ;;
    esac

    printf 'object %s\ntype %s\ntag %s\ntagger Test <test@example.invalid> 0 +0000\n\n' \
        "$object" "$object_type" "$tag"
    case "$scenario" in
        fresh | mixed)
            printf 'workflow-run: 97531\ncommit: %s\n' "$commit"
            ;;
        duplicate_run)
            printf 'workflow-run: %s\nworkflow-run: %s\ncommit: %s\n' \
                "$run_id" "$run_id" "$commit"
            ;;
        malformed_run)
            printf 'workflow-run: %s\nworkflow-run: not-a-run-id\ncommit: %s\n' \
                "$run_id" "$commit"
            ;;
        missing_commit)
            printf 'workflow-run: %s\n' "$run_id"
            ;;
        wrong_commit)
            printf 'workflow-run: %s\ncommit: %s\n' "$run_id" "$other_commit"
            ;;
        duplicate_commit)
            printf 'workflow-run: %s\ncommit: %s\ncommit: %s\n' \
                "$run_id" "$commit" "$commit"
            ;;
        same_run | duplicate_owned | direct_object_wrong | indirect_object | \
            peeled_mismatch)
            printf 'workflow-run: %s\ncommit: %s\n' "$run_id" "$commit"
            ;;
        *)
            die "tag object requested in scenario $scenario"
            ;;
    esac
}

[[ $# -gt 0 ]] || die 'missing command'
case "$1" in
    rev-parse)
        [[ $# -eq 3 && "$2" == --verify && "$3" == "${expected_input}^{commit}" ]] || \
            die "unexpected rev-parse invocation: $*"
        case "$scenario" in
            unresolvable_commit) exit 1 ;;
            noncommit_target) printf '%s\n' "$commit" ;;
            *) printf '%s\n' "$expected_input" ;;
        esac
        ;;
    fetch)
        [[ $# -eq 6 ]] || die "fetch received $# arguments instead of 6"
        [[ "$2" == --force ]] || die 'fetch omitted --force or changed its order'
        [[ "$3" == --prune ]] || die 'fetch omitted --prune or changed its order'
        [[ "$4" == --prune-tags ]] || die 'fetch omitted --prune-tags or changed its order'
        [[ "$5" == origin ]] || die 'fetch did not use origin'
        [[ "$6" == '+refs/tags/*:refs/tags/*' ]] || die 'fetch used the wrong tag refspec'
        printf '%s\n' fetch >>"$log"
        ;;
    for-each-ref)
        [[ $# -eq 3 && "$2" == '--format=%(refname:strip=2)' && "$3" == refs/tags/ ]] || \
            die "unexpected for-each-ref invocation: $*"
        print_tags
        ;;
    cat-file)
        [[ $# -eq 3 && "$3" == refs/tags/* ]] || die "unexpected cat-file invocation: $*"
        tag=${3#refs/tags/}
        type=$(tag_type "$tag")
        case "$2" in
            -t) printf '%s\n' "$type" ;;
            tag)
                [[ "$type" == tag ]] || die "tag contents requested for lightweight tag $tag"
                print_tag_object "$tag"
                ;;
            *) die "unexpected cat-file mode: $2" ;;
        esac
        ;;
    rev-list)
        [[ $# -eq 4 && "$2" == -n && "$3" == 1 && "$4" == refs/tags/* ]] || \
            die "unexpected rev-list invocation: $*"
        case "$scenario" in
            same_run) printf '%s\n' "$commit" ;;
            peeled_mismatch) printf '%s\n' "$other_commit" ;;
            *) die "rev-list requested in scenario $scenario" ;;
        esac
        ;;
    *)
        die "unsupported command: $*"
        ;;
esac
FAKE_GIT
chmod 755 "$fake_bin/git"

stdout_file=$test_dir/stdout
stderr_file=$test_dir/stderr
git_log=$test_dir/git-log
last_status=0
last_stdout=
last_stderr=
test_count=0

run_planner() {
    local scenario=$1
    local case_run_id=$2
    local expected_input=$3
    local github_output=$4
    shift 4
    local -a environment=(
        env -u GITHUB_OUTPUT
        "FAKE_GIT_SCENARIO=$scenario"
        "FAKE_VERSION=$version"
        "FAKE_COMMIT=$commit"
        "FAKE_OTHER_COMMIT=$other_commit"
        "FAKE_NONCOMMIT=$noncommit"
        "FAKE_RUN_ID=$case_run_id"
        "FAKE_EXPECTED_INPUT=$expected_input"
        "FAKE_GIT_LOG=$git_log"
        "GITHUB_RUN_ID=$case_run_id"
        "PATH=$fake_bin:$PATH"
    )

    if [[ -n "$github_output" ]]; then
        environment=(
            env
            "GITHUB_OUTPUT=$github_output"
            "FAKE_GIT_SCENARIO=$scenario"
            "FAKE_VERSION=$version"
            "FAKE_COMMIT=$commit"
            "FAKE_OTHER_COMMIT=$other_commit"
            "FAKE_NONCOMMIT=$noncommit"
            "FAKE_RUN_ID=$case_run_id"
            "FAKE_EXPECTED_INPUT=$expected_input"
            "FAKE_GIT_LOG=$git_log"
            "GITHUB_RUN_ID=$case_run_id"
            "PATH=$fake_bin:$PATH"
        )
    fi

    : >"$stdout_file"
    : >"$stderr_file"
    : >"$git_log"
    set +e
    "${environment[@]}" "$planner" "$@" >"$stdout_file" 2>"$stderr_file"
    last_status=$?
    set -e
    last_stdout=$(<"$stdout_file")
    last_stderr=$(<"$stderr_file")
}

show_failure_context() {
    local label=$1

    echo "$label: stdout:" >&2
    sed 's/^/  /' "$stdout_file" >&2
    echo "$label: stderr:" >&2
    sed 's/^/  /' "$stderr_file" >&2
}

expect_success() {
    local label=$1
    local scenario=$2
    local expected_tag=$3
    local expected_action=$4
    local check_output_file=${5:-false}
    local github_output=
    local expected

    if [[ "$check_output_file" == true ]]; then
        github_output=$test_dir/github-output
        printf '%s\n' 'sentinel=preserved' >"$github_output"
    fi

    run_planner "$scenario" "$run_id" "$commit" "$github_output" "$version" "$commit"
    if [[ "$last_status" -ne 0 ]]; then
        echo "$label: expected success, got status $last_status" >&2
        show_failure_context "$label"
        exit 1
    fi

    expected=$(printf 'rc_tag=%s\naction=%s' "$expected_tag" "$expected_action")
    if [[ "$last_stdout" != "$expected" ]]; then
        echo "$label: planner did not emit exactly one rc_tag and action" >&2
        show_failure_context "$label"
        exit 1
    fi
    [[ -z "$last_stderr" ]] || {
        echo "$label: unexpected stderr" >&2
        show_failure_context "$label"
        exit 1
    }
    [[ "$(<"$git_log")" == fetch ]] || {
        echo "$label: expected exactly one validated tag fetch" >&2
        show_failure_context "$label"
        exit 1
    }

    if [[ "$check_output_file" == true ]]; then
        expected=$(printf 'sentinel=preserved\nrc_tag=%s\naction=%s' \
            "$expected_tag" "$expected_action")
        [[ "$(<"$github_output")" == "$expected" ]] || {
            echo "$label: GITHUB_OUTPUT was not appended exactly once" >&2
            sed 's/^/  /' "$github_output" >&2
            exit 1
        }
    fi
    test_count=$((test_count + 1))
}

expect_failure() {
    local label=$1
    local scenario=$2
    local expected_status=$3
    local diagnostic=$4
    local case_run_id=$5
    local expected_input=$6
    shift 6

    run_planner "$scenario" "$case_run_id" "$expected_input" '' "$@"
    if [[ "$last_status" -ne "$expected_status" ]]; then
        echo "$label: expected status $expected_status, got $last_status" >&2
        show_failure_context "$label"
        exit 1
    fi
    if [[ "$last_stderr" != *"$diagnostic"* ]]; then
        echo "$label: missing diagnostic: $diagnostic" >&2
        show_failure_context "$label"
        exit 1
    fi
    if awk -F= '$1 == "rc_tag" || $1 == "action" { found = 1 } END { exit !found }' \
        "$stdout_file"; then
        echo "$label: emitted candidate outputs after failure" >&2
        show_failure_context "$label"
        exit 1
    fi
    test_count=$((test_count + 1))
}

expect_success empty_tag_set empty "v$version-rc.1" create
expect_success fresh_next_rc fresh "v$version-rc.3" create
expect_success malformed_lightweight_and_other_run_tags mixed "v$version-rc.8" create
expect_success exact_same_run_recovery same_run "v$version-rc.3" recover true

expect_failure duplicate_owned_tags duplicate_owned 1 \
    'more than one annotated numeric RC tag belongs to workflow run' \
    "$run_id" "$commit" "$version" "$commit"
expect_failure duplicate_run_marker duplicate_run 1 \
    'duplicate or malformed workflow-run annotations' \
    "$run_id" "$commit" "$version" "$commit"
expect_failure malformed_run_marker malformed_run 1 \
    'duplicate or malformed workflow-run annotations' \
    "$run_id" "$commit" "$version" "$commit"
expect_failure wrong_direct_object direct_object_wrong 1 \
    "does not directly reference $commit" \
    "$run_id" "$commit" "$version" "$commit"
expect_failure indirect_tag_object indirect_object 1 \
    "does not directly reference $commit" \
    "$run_id" "$commit" "$version" "$commit"
expect_failure peeled_commit_mismatch peeled_mismatch 1 \
    "belongs to workflow run $run_id but points to $other_commit" \
    "$run_id" "$commit" "$version" "$commit"
expect_failure missing_commit_marker missing_commit 1 \
    'does not have exactly one commit annotation' \
    "$run_id" "$commit" "$version" "$commit"
expect_failure wrong_commit_marker wrong_commit 1 \
    'does not have exactly one commit annotation' \
    "$run_id" "$commit" "$version" "$commit"
expect_failure duplicate_commit_marker duplicate_commit 1 \
    'does not have exactly one commit annotation' \
    "$run_id" "$commit" "$version" "$commit"
expect_failure huge_numeric_suffix huge_suffix 1 \
    'has an RC number too large to handle safely' \
    "$run_id" "$commit" "$version" "$commit"

expect_failure missing_arguments contract 2 'usage:' "$run_id" "$commit"
expect_failure extra_argument contract 2 'usage:' "$run_id" "$commit" \
    "$version" "$commit" unexpected

for bad_version in v9.8.7 09.8.7 9.08.7 9.8.07 9.8; do
    expect_failure "malformed_version_$bad_version" contract 1 \
        'version must be an exact X.Y.Z release without leading zeroes' \
        "$run_id" "$commit" "$bad_version" "$commit"
done

for bad_run_id in '' 0 024680 run-24680; do
    expect_failure "malformed_run_id_${bad_run_id:-missing}" contract 1 \
        'GITHUB_RUN_ID must be a positive integer' \
        "$bad_run_id" "$commit" "$version" "$commit"
done

short_commit=0123456789abcdef
uppercase_commit=0123456789ABCDEF0123456789ABCDEF01234567
expect_failure short_commit contract 1 \
    'commit SHA must be a lowercase 40-character hexadecimal value' \
    "$run_id" "$short_commit" "$version" "$short_commit"
expect_failure uppercase_commit contract 1 \
    'commit SHA must be a lowercase 40-character hexadecimal value' \
    "$run_id" "$uppercase_commit" "$version" "$uppercase_commit"
expect_failure unresolved_commit unresolvable_commit 1 \
    'does not identify a commit in this checkout' \
    "$run_id" "$commit" "$version" "$commit"
expect_failure indirect_target_commit noncommit_target 1 \
    'did not resolve to itself' \
    "$run_id" "$noncommit" "$version" "$noncommit"

echo "plan-rc regression tests passed ($test_count cases)"
