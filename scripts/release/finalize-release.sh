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
    echo "usage: $0 RELEASE_MODE VERSION CANDIDATE_ID COMMIT_SHA CANDIDATE_DIR EVIDENCE_FILE" >&2
    exit 2
}

release_mode=$1
version=$2
candidate_id=$3
commit_sha=$4
candidate_dir=$5
evidence_file=$6
package_name=alternator-client
final_tag="v$version"
release_script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

[[ "$release_mode" == release ]] || {
    echo "finalization requires release_mode=release" >&2
    exit 1
}
rc_tag=$candidate_id

for command in curl gh git jq shasum; do
    command -v "$command" >/dev/null || {
        echo "required command is missing: $command" >&2
        exit 1
    }
done
[[ -n "${GITHUB_REPOSITORY:-}" ]] || {
    echo "GITHUB_REPOSITORY is missing" >&2
    exit 1
}

push_tag_with_release_app() {
    local tag=$1
    local askpass_dir
    local askpass
    local push_status=0

    [[ "${RELEASE_APP_SLUG:-}" == scylladb-alternator-client-release ]] || {
        echo "unexpected or missing release App slug" >&2
        return 1
    }
    [[ "${RELEASE_APP_BOT_ID:-}" =~ ^[1-9][0-9]*$ ]] || {
        echo "release App bot ID is missing or invalid" >&2
        return 1
    }
    [[ -n "${RELEASE_APP_TOKEN:-}" ]] || {
        echo "release App token is missing" >&2
        return 1
    }

    askpass_dir=$(mktemp -d)
    askpass=$askpass_dir/askpass
    printf '%s\n' \
        '#!/usr/bin/env bash' \
        'set -euo pipefail' \
        'case ${1:-} in' \
        '    *Username*) printf '\''%s\n'\'' x-access-token ;;' \
        '    *Password*) printf '\''%s\n'\'' "${RELEASE_APP_TOKEN:?}" ;;' \
        '    *) exit 1 ;;' \
        'esac' >"$askpass"
    chmod 700 "$askpass"

    RELEASE_APP_TOKEN=$RELEASE_APP_TOKEN \
        GIT_ASKPASS="$askpass" \
        GIT_TERMINAL_PROMPT=0 \
        git -c credential.helper= push \
        "https://github.com/$GITHUB_REPOSITORY.git" \
        "refs/tags/$tag:refs/tags/$tag" || push_status=$?
    rm -rf -- "$askpass_dir"
    return "$push_status"
}

manifest="$candidate_dir/release-manifest.json"
crate_file=$(jq -er '.crate.file' "$manifest")
sbom_file=$(jq -er '.sbom.file' "$manifest")
"$release_script_dir/verify-candidate.sh" "$candidate_dir" "$(mktemp -d)" \
    release "$version" "$candidate_id" "$commit_sha" >/dev/null
[[ -f "$evidence_file" ]] || {
    echo "test evidence is missing: $evidence_file" >&2
    exit 1
}
jq -e \
    --arg candidate_id "$candidate_id" \
    --arg commit "$commit_sha" \
    --arg crate_sha "$(jq -er '.crate.sha256' "$manifest")" '
    .schema_version == 2 and
    .release_mode == "release" and
    .candidate_id == $candidate_id and
    .rc_tag == $candidate_id and
    .commit_sha == $commit and
    .crate.sha256 == $crate_sha and
    .all_required_gates_passed == true
' "$evidence_file" >/dev/null

canonical_assets=(
    "$crate_file"
    SHA256SUMS
    "$sbom_file"
    release-manifest.json
    test-evidence.json
)

asset_source() {
    local asset=$1

    if [[ "$asset" == test-evidence.json ]]; then
        printf '%s\n' "$evidence_file"
    else
        printf '%s\n' "$candidate_dir/$asset"
    fi
}

release_app_gh() {
    [[ -n "${RELEASE_APP_TOKEN:-}" ]] || {
        echo "release App token is missing" >&2
        return 1
    }
    GH_TOKEN=$RELEASE_APP_TOKEN gh "$@"
}

state_file=$(mktemp)
GITHUB_OUTPUT="$state_file" "$release_script_dir/registry-state.sh" \
    "$package_name" "$version" "$candidate_dir/$crate_file" >/dev/null
[[ "$(awk -F= '$1 == "state" { print $2 }' "$state_file")" == exact ]] || {
    echo "crates.io does not serve the tested candidate; refusing to finalize" >&2
    exit 1
}

git fetch --force origin --tags
"$release_script_dir/assert-latest-rc.sh" \
    "$version" "$rc_tag" "$commit_sha" >/dev/null

if git rev-parse -q --verify "refs/tags/$final_tag" >/dev/null; then
    [[ "$(git cat-file -t "refs/tags/$final_tag")" == tag ]] || {
        echo "existing final tag is not annotated" >&2
        exit 1
    }
    [[ "$(git rev-list -n 1 "$final_tag")" == "$commit_sha" ]] || {
        echo "existing final tag points to a different commit" >&2
        exit 1
    }
else
    [[ "${RELEASE_APP_SLUG:-}" == scylladb-alternator-client-release ]] || {
        echo "unexpected or missing release App slug" >&2
        exit 1
    }
    [[ "${RELEASE_APP_BOT_ID:-}" =~ ^[1-9][0-9]*$ ]] || {
        echo "release App bot ID is missing or invalid" >&2
        exit 1
    }
    git config user.name "$RELEASE_APP_SLUG[bot]"
    git config user.email "$RELEASE_APP_BOT_ID+$RELEASE_APP_SLUG[bot]@users.noreply.github.com"
    git tag -a "$final_tag" "$commit_sha" -m "Release $package_name $version

Promoted from: $rc_tag
crate-sha256: $(jq -er '.crate.sha256' "$manifest")
commit: $commit_sha"
    bash "$release_script_dir/check-release-blockers.sh"
    bash "$release_script_dir/check-release-tag-rulesets.sh"
    push_tag_with_release_app "$final_tag"
fi

remote_final_commit=$(git ls-remote origin "refs/tags/$final_tag^{}" | awk '{ print $1 }')
[[ "$remote_final_commit" == "$commit_sha" ]] || {
    echo "remote final tag does not point to the manifest commit" >&2
    exit 1
}

notes_file=$(mktemp)
awk -v version="$version" '
    index($0, "## [" version "] - ") == 1 { found = 1; next }
    found && /^## / { exit }
    found { print }
' CHANGELOG.md >"$notes_file"
[[ -s "$notes_file" ]] || {
    echo "could not extract release notes for $version" >&2
    exit 1
}

release_list=$(mktemp)
release_app_gh api --paginate --slurp -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$GITHUB_REPOSITORY/releases?per_page=100" \
    | jq --arg tag "$final_tag" '[.[].[] | select(.tag_name == $tag)]' >"$release_list"
release_count=$(jq 'length' "$release_list")
[[ "$release_count" -le 1 ]] || {
    echo "multiple GitHub Releases use tag $final_tag" >&2
    exit 1
}

if [[ "$release_count" -eq 0 ]]; then
    release_app_gh release create "$final_tag" --repo "$GITHUB_REPOSITORY" --verify-tag --draft \
        --title "$package_name v$version" --notes-file "$notes_file"
    release_app_gh api --paginate --slurp -H 'X-GitHub-Api-Version: 2026-03-10' \
        "repos/$GITHUB_REPOSITORY/releases?per_page=100" \
        | jq --arg tag "$final_tag" '[.[].[] | select(.tag_name == $tag)]' >"$release_list"
    [[ "$(jq 'length' "$release_list")" -eq 1 ]] || {
        echo "new draft GitHub Release could not be recovered" >&2
        exit 1
    }
fi
release_body=$(mktemp)
jq '.[0]' "$release_list" >"$release_body"
release_id=$(jq -er '.id' "$release_body")

expected_names=$(printf '%s\n' "${canonical_assets[@]}" | LC_ALL=C sort)

if jq -e '.draft == false' "$release_body" >/dev/null; then
    jq -e '.immutable == true' "$release_body" >/dev/null || {
        echo "published release is not immutable" >&2
        exit 1
    }

    published_names=$(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort)
    [[ "$published_names" == "$expected_names" ]] || {
        echo "published release assets differ from the canonical asset set" >&2
        exit 1
    }
    verify_dir=$(mktemp -d)
    release_app_gh release download "$final_tag" --repo "$GITHUB_REPOSITORY" --dir "$verify_dir"
    for asset in "${canonical_assets[@]}"; do
        source_file=$(asset_source "$asset")
        [[ -f "$verify_dir/$asset" ]] && cmp -s "$source_file" "$verify_dir/$asset" || {
            echo "published release asset $asset is missing or differs" >&2
            exit 1
        }
    done
    echo "immutable GitHub Release $final_tag already contains the canonical assets"
    exit 0
fi

expected_notes=$(cat "$notes_file")
actual_notes=$(jq -r '.body' "$release_body")
jq -e \
    --arg tag "$final_tag" \
    --arg name "$package_name v$version" \
    '.draft == true and .tag_name == $tag and .name == $name' \
    "$release_body" >/dev/null || {
    echo "existing draft release metadata differs from the canonical release" >&2
    exit 1
}
[[ "$actual_notes" == "$expected_notes" ]] || {
    echo "existing draft release notes differ from CHANGELOG.md" >&2
    exit 1
}

unexpected_names=$(comm -23 \
    <(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort) \
    <(printf '%s\n' "${canonical_assets[@]}" | LC_ALL=C sort))
[[ -z "$unexpected_names" ]] || {
    echo "draft release contains unexpected assets:" >&2
    printf '%s\n' "$unexpected_names" >&2
    exit 1
}

for asset in "${canonical_assets[@]}"; do
    source_file=$(asset_source "$asset")
    asset_id=$(jq -r --arg name "$asset" '.assets[] | select(.name == $name) | .id' "$release_body")
    if [[ -n "$asset_id" ]]; then
        asset_state=$(jq -r --arg name "$asset" '.assets[] | select(.name == $name) | .state' "$release_body")
        if [[ "$asset_state" == starter ]]; then
            # GitHub can leave a zero-byte `starter` placeholder after a 502.
            # It is not a published asset and must be removed to retry safely.
            release_app_gh api --method DELETE -H 'X-GitHub-Api-Version: 2026-03-10' \
                "repos/$GITHUB_REPOSITORY/releases/assets/$asset_id"
            release_app_gh release upload "$final_tag" --repo "$GITHUB_REPOSITORY" "$source_file"
        elif [[ "$asset_state" == uploaded ]]; then
            existing_file=$(mktemp)
            curl -fsSL \
                -H 'Accept: application/octet-stream' \
                -H "Authorization: Bearer $RELEASE_APP_TOKEN" \
                -H 'X-GitHub-Api-Version: 2026-03-10' \
                -o "$existing_file" \
                "https://api.github.com/repos/$GITHUB_REPOSITORY/releases/assets/$asset_id"
            cmp -s "$source_file" "$existing_file" || {
                echo "existing draft asset $asset differs; refusing to replace it" >&2
                exit 1
            }
        else
            echo "existing draft asset $asset has unexpected state $asset_state" >&2
            exit 1
        fi
    else
        release_app_gh release upload "$final_tag" --repo "$GITHUB_REPOSITORY" "$source_file"
    fi
done

release_body=$(mktemp)
release_app_gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$GITHUB_REPOSITORY/releases/$release_id" >"$release_body"
uploaded_names=$(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort)
[[ "$uploaded_names" == "$expected_names" ]] || {
    echo "draft release does not contain exactly the canonical assets" >&2
    exit 1
}
jq -e '[.assets[].state] | all(. == "uploaded")' "$release_body" >/dev/null || {
    echo "not every draft asset reached the uploaded state" >&2
    exit 1
}

for asset in "${canonical_assets[@]}"; do
    source_file=$(asset_source "$asset")
    asset_id=$(jq -er --arg name "$asset" '.assets[] | select(.name == $name) | .id' "$release_body")
    uploaded_file=$(mktemp)
    curl -fsSL \
        -H 'Accept: application/octet-stream' \
        -H "Authorization: Bearer $RELEASE_APP_TOKEN" \
        -H 'X-GitHub-Api-Version: 2026-03-10' \
        -o "$uploaded_file" \
        "https://api.github.com/repos/$GITHUB_REPOSITORY/releases/assets/$asset_id"
    cmp -s "$source_file" "$uploaded_file" || {
        echo "uploaded draft asset $asset differs from its canonical local file" >&2
        exit 1
    }
done

remote_final_commit=$(git ls-remote origin "refs/tags/$final_tag^{}" | awk '{ print $1 }')
[[ "$remote_final_commit" == "$commit_sha" ]] || {
    echo "remote final tag moved before immutable release publication" >&2
    exit 1
}

bash "$release_script_dir/check-release-blockers.sh"
release_app_gh release edit "$final_tag" --repo "$GITHUB_REPOSITORY" --draft=false

for attempt in $(seq 1 12); do
    release_body=$(mktemp)
    release_app_gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
        "repos/$GITHUB_REPOSITORY/releases/$release_id" >"$release_body"
    if jq -e '.draft == false and .immutable == true' "$release_body" >/dev/null; then
        remote_final_commit=$(git ls-remote origin "refs/tags/$final_tag^{}" | awk '{ print $1 }')
        [[ "$remote_final_commit" == "$commit_sha" ]] || {
            echo "immutable release tag does not point to the manifest commit" >&2
            exit 1
        }
        immutable_names=$(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort)
        [[ "$immutable_names" == "$expected_names" ]] || {
            echo "immutable release assets differ from the canonical asset set" >&2
            exit 1
        }
        verify_dir=$(mktemp -d)
        release_app_gh release download "$final_tag" --repo "$GITHUB_REPOSITORY" --dir "$verify_dir"
        for asset in "${canonical_assets[@]}"; do
            source_file=$(asset_source "$asset")
            [[ -f "$verify_dir/$asset" ]] && cmp -s "$source_file" "$verify_dir/$asset" || {
                echo "immutable release asset $asset is missing or differs" >&2
                exit 1
            }
        done
        jq -r '.html_url' "$release_body"
        exit 0
    fi
    echo "waiting for immutable release state ($attempt/12)"
    sleep 5
done

echo "GitHub Release was published but did not report immutable state" >&2
exit 1
