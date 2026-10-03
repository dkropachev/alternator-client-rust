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

usage() {
    echo "usage: $0 ARTIFACT_DIR EXTRACT_PARENT EXPECTED_MODE EXPECTED_VERSION EXPECTED_CANDIDATE_ID EXPECTED_COMMIT" >&2
    exit 2
}

[[ $# -eq 6 ]] || usage

artifact_dir=$1
extract_parent=$2
expected_mode=$3
expected_version=$4
expected_candidate_id=$5
expected_commit=$6
release_script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
expected_rust=${RUST_VERSION:-1.94.1}
expected_rustdoc=${RUSTDOC_TOOLCHAIN:-nightly-2026-06-23}
expected_ccm=${CCM_COMMIT:-f9e8f8c221f76251318c61ba8a0ce6acec860f6d}

[[ "$expected_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
    echo "invalid expected release version: $expected_version" >&2
    exit 1
}
case "$expected_mode" in
    release)
        expected_rc_prefix="v$expected_version-rc."
        [[ "$expected_candidate_id" == "$expected_rc_prefix"* ]] || {
            echo "invalid expected release candidate ID: $expected_candidate_id" >&2
            exit 1
        }
        expected_rc_number=${expected_candidate_id#"$expected_rc_prefix"}
        [[ "$expected_rc_number" =~ ^[1-9][0-9]*$ ]] || {
            echo "invalid expected release candidate ID: $expected_candidate_id" >&2
            exit 1
        }
        ;;
    validate)
        [[ "$expected_candidate_id" =~ ^validation-[1-9][0-9]*$ ]] || {
            echo "invalid expected validation candidate ID: $expected_candidate_id" >&2
            exit 1
        }
        ;;
    *)
        echo "invalid expected release mode: $expected_mode" >&2
        exit 1
        ;;
esac
[[ "$expected_commit" =~ ^[0-9a-f]{40}$ ]] || {
    echo "invalid expected commit SHA: $expected_commit" >&2
    exit 1
}

for command in jq shasum tar; do
    command -v "$command" >/dev/null || {
        echo "required command is missing: $command" >&2
        exit 1
    }
done
expected_scylla_versions=$(bash "$release_script_dir/release-policy.sh" scylla-versions)

[[ -d "$artifact_dir" ]] || {
    echo "candidate artifact directory does not exist: $artifact_dir" >&2
    exit 1
}

shopt -s nullglob dotglob
entries=("$artifact_dir"/*)
entry_count=${#entries[@]}
file_count=0
files=
for entry in "${entries[@]}"; do
    if [[ -f "$entry" && ! -L "$entry" ]]; then
        ((file_count += 1))
        files+="$entry"$'\n'
    fi
done
[[ "$file_count" -eq 4 && "$entry_count" -eq 4 ]] || {
    echo "candidate artifact must contain exactly four files; found $file_count" >&2
    printf '%s\n' "$files" >&2
    exit 1
}

manifest="$artifact_dir/release-manifest.json"
checksums="$artifact_dir/SHA256SUMS"
[[ -f "$manifest" && -f "$checksums" ]] || {
    echo "candidate is missing release-manifest.json or SHA256SUMS" >&2
    exit 1
}

version=$(jq -er '.crate.version' "$manifest")
crate_file=$(jq -er '.crate.file' "$manifest")
crate_sha=$(jq -er '.crate.sha256' "$manifest")
sbom_file=$(jq -er '.sbom.file' "$manifest")
sbom_sha=$(jq -er '.sbom.sha256' "$manifest")
cargo_lock_sha=$(jq -er '.cargo_lock_sha256' "$manifest")
changelog_sha=$(jq -er '.changelog_sha256' "$manifest")
release_mode=$(jq -er '.release_mode' "$manifest")
candidate_id=$(jq -er '.candidate_id' "$manifest")
rc_tag_type=$(jq -r '.rc_tag | type' "$manifest")
rc_tag=$(jq -r '.rc_tag // empty' "$manifest")
commit=$(jq -er '.commit_sha' "$manifest")
workflow_run_id=$(jq -er '.workflow_run_id' "$manifest")

jq -e \
    --arg rust "$expected_rust" \
    --arg rustdoc "$expected_rustdoc" \
    --arg ccm "$expected_ccm" \
    --argjson scylla_versions "$expected_scylla_versions" '
    .schema_version == 2 and
    (.release_mode | type == "string") and
    (.candidate_id | type == "string" and length > 0) and
    .crate.name == "alternator-client" and
    .crate.library_name == "alternator_driver" and
    (.crate.version | type == "string" and length > 0) and
    (.crate.file | type == "string" and length > 0) and
    (.crate.sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    has("rc_tag") and
    ((.rc_tag | type) == "string" or (.rc_tag | type) == "null") and
    (.commit_sha | type == "string" and test("^[0-9a-f]{40}$")) and
    .toolchains.rust.pin == $rust and
    (.toolchains.rust.actual | type == "string" and length > 0) and
    .toolchains.rustdoc.pin == $rustdoc and
    (.toolchains.rustdoc.actual | type == "string" and length > 0) and
    .ccm_commit == $ccm and
    .scylla_versions == $scylla_versions and
    (.workflow_run_id | type == "string" and length > 0) and
    (.cargo_lock_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.changelog_sha256 | type == "string" and test("^[0-9a-f]{64}$")) and
    (.source_date_epoch | type == "string" and test("^[0-9]+$")) and
    (.sbom.file | type == "string" and length > 0) and
    (.sbom.sha256 | type == "string" and test("^[0-9a-f]{64}$"))
' "$manifest" >/dev/null

[[ "$release_mode" == "$expected_mode" ]] || {
    echo "manifest mode $release_mode does not match expected mode $expected_mode" >&2
    exit 1
}
[[ "$version" == "$expected_version" ]] || {
    echo "manifest version $version does not match expected version $expected_version" >&2
    exit 1
}
[[ "$candidate_id" == "$expected_candidate_id" ]] || {
    echo "manifest candidate ID $candidate_id does not match expected ID $expected_candidate_id" >&2
    exit 1
}
[[ "$commit" == "$expected_commit" ]] || {
    echo "manifest commit $commit does not match expected commit $expected_commit" >&2
    exit 1
}
case "$release_mode" in
    release)
        [[ "$rc_tag_type" == string && "$rc_tag" == "$candidate_id" ]] || {
            echo "release manifest RC tag must equal candidate ID $candidate_id" >&2
            exit 1
        }
        ;;
    validate)
        [[ "$rc_tag_type" == null ]] || {
            echo "validation manifest RC tag must be null" >&2
            exit 1
        }
        [[ "$candidate_id" == "validation-$workflow_run_id" ]] || {
            echo "validation candidate ID does not match manifest workflow run ID" >&2
            exit 1
        }
        ;;
esac

[[ "$crate_file" == "alternator-client-$version.crate" ]] || {
    echo "unexpected crate filename in manifest: $crate_file" >&2
    exit 1
}
[[ "$sbom_file" == "alternator-client-$version.cdx.json" ]] || {
    echo "unexpected SBOM filename in manifest: $sbom_file" >&2
    exit 1
}
[[ -f "$artifact_dir/$crate_file" && -f "$artifact_dir/$sbom_file" ]] || {
    echo "candidate is missing its crate or SBOM" >&2
    exit 1
}

checksum_names=$(awk 'NF {
    if (NF != 2 || length($1) != 64 || $1 ~ /[^0-9a-f]/) exit 2
    print $2
}' "$checksums" | LC_ALL=C sort) || {
    echo "SHA256SUMS has an invalid format" >&2
    exit 1
}
expected_checksum_names=$(printf '%s\n' "$crate_file" "$sbom_file" release-manifest.json | LC_ALL=C sort)
[[ "$checksum_names" == "$expected_checksum_names" ]] || {
    echo "SHA256SUMS must cover exactly the crate, SBOM, and manifest" >&2
    exit 1
}

(
    cd "$artifact_dir"
    shasum -a 256 -c SHA256SUMS
)

actual_crate_sha=$(shasum -a 256 "$artifact_dir/$crate_file" | awk '{ print $1 }')
actual_sbom_sha=$(shasum -a 256 "$artifact_dir/$sbom_file" | awk '{ print $1 }')
[[ "$actual_crate_sha" == "$crate_sha" ]] || {
    echo "crate hash does not match the manifest" >&2
    exit 1
}
[[ "$actual_sbom_sha" == "$sbom_sha" ]] || {
    echo "SBOM hash does not match the manifest" >&2
    exit 1
}

jq -e '
    .bomFormat == "CycloneDX" and
    (.specVersion | type == "string") and
    (.components | type == "array")
' "$artifact_dir/$sbom_file" >/dev/null

archive_entries=$(tar -tzf "$artifact_dir/$crate_file")
[[ -n "$archive_entries" ]] || {
    echo "crate archive is empty" >&2
    exit 1
}
if tar -tvzf "$artifact_dir/$crate_file" | awk '
    substr($1, 1, 1) != "-" && substr($1, 1, 1) != "d" { bad = 1 }
    END { exit bad ? 0 : 1 }
'; then
    echo "crate archive contains a link or non-regular filesystem entry" >&2
    exit 1
fi
if printf '%s\n' "$archive_entries" | awk '
    /^\// { bad = 1 }
    /(^|\/)\.\.($|\/)/ { bad = 1 }
    END { exit bad ? 0 : 1 }
'; then
    echo "crate archive contains an unsafe path" >&2
    exit 1
fi

root="alternator-client-$version"
if printf '%s\n' "$archive_entries" | awk -v root="$root/" '
    index($0, root) != 1 { bad = 1 }
    END { exit bad ? 0 : 1 }
'; then
    echo "crate archive contains a path outside $root" >&2
    exit 1
fi

mkdir -p "$extract_parent"
extract_dir=$(mktemp -d "$extract_parent/alternator-candidate.XXXXXX")
tar -xzf "$artifact_dir/$crate_file" -C "$extract_dir"
package_dir="$extract_dir/$root"
[[ -f "$package_dir/Cargo.toml" && -f "$package_dir/Cargo.lock" ]] || {
    echo "extracted candidate is missing Cargo.toml or Cargo.lock" >&2
    exit 1
}
[[ -f "$package_dir/.cargo_vcs_info.json" ]] && jq -e \
    --arg commit "$commit" \
    '.git.sha1 == $commit and .path_in_vcs == ""' \
    "$package_dir/.cargo_vcs_info.json" >/dev/null || {
    echo "packaged VCS metadata does not match the manifest commit" >&2
    exit 1
}
[[ "$(shasum -a 256 "$package_dir/Cargo.lock" | awk '{ print $1 }')" == "$cargo_lock_sha" ]] || {
    echo "extracted Cargo.lock hash does not match the manifest" >&2
    exit 1
}
[[ "$(shasum -a 256 "$package_dir/CHANGELOG.md" | awk '{ print $1 }')" == "$changelog_sha" ]] || {
    echo "extracted CHANGELOG.md hash does not match the manifest" >&2
    exit 1
}
if find "$package_dir" -type l -print | grep -q .; then
    echo "crate archive contains a symbolic link" >&2
    exit 1
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "package_dir=$package_dir" >>"$GITHUB_OUTPUT"
    echo "release_mode=$release_mode" >>"$GITHUB_OUTPUT"
    echo "candidate_id=$candidate_id" >>"$GITHUB_OUTPUT"
    echo "crate_file=$crate_file" >>"$GITHUB_OUTPUT"
    echo "sbom_file=$sbom_file" >>"$GITHUB_OUTPUT"
    echo "crate_sha256=$crate_sha" >>"$GITHUB_OUTPUT"
fi

echo "$package_dir"
