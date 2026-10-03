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

[[ $# -eq 5 ]] || {
    echo "usage: $0 MODE VERSION CANDIDATE_ID COMMIT_SHA OUTPUT_DIR" >&2
    exit 2
}

release_mode=$1
version=$2
candidate_id=$3
commit_sha=$4
output_dir=$5
package_name=alternator-client
release_script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
rust_version=${RUST_VERSION:-1.94.1}
rustdoc_toolchain=${RUSTDOC_TOOLCHAIN:-nightly-2026-06-23}
ccm_commit=${CCM_COMMIT:-f9e8f8c221f76251318c61ba8a0ce6acec860f6d}

for command in cargo git jq shasum; do
    command -v "$command" >/dev/null || {
        echo "required command is missing: $command" >&2
        exit 1
    }
done
scylla_versions=$(bash "$release_script_dir/release-policy.sh" scylla-versions)

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
        [[ "${GITHUB_RUN_ID:-}" =~ ^[1-9][0-9]*$ ]] || {
            echo "a numeric GITHUB_RUN_ID is required in validate mode" >&2
            exit 1
        }
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

[[ "$(git rev-parse HEAD)" == "$commit_sha" ]] || {
    echo "checkout does not match candidate commit $commit_sha" >&2
    exit 1
}
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || {
    echo "candidate packaging requires a clean checkout" >&2
    exit 1
}

metadata=$(cargo metadata --locked --no-deps --format-version 1)
actual_name=$(jq -er '.packages[] | select(.manifest_path | endswith("/Cargo.toml")) | .name' <<<"$metadata")
actual_version=$(jq -er '.packages[] | select(.manifest_path | endswith("/Cargo.toml")) | .version' <<<"$metadata")
[[ "$actual_name" == "$package_name" && "$actual_version" == "$version" ]] || {
    echo "Cargo metadata is $actual_name $actual_version, expected $package_name $version" >&2
    exit 1
}
awk -v prefix="## [$version] - " '
    index($0, prefix) == 1 {
        count += 1
        date = substr($0, length(prefix) + 1)
        if (date ~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$/) valid += 1
    }
    END { exit count == 1 && valid == 1 ? 0 : 1 }
' CHANGELOG.md || {
    echo "CHANGELOG.md needs a dated [$version] release entry" >&2
    exit 1
}
original_lock_sha=$(shasum -a 256 Cargo.lock | awk '{ print $1 }')

mkdir -p "$output_dir"
[[ -z "$(find "$output_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]] || {
    echo "candidate output directory must be empty: $output_dir" >&2
    exit 1
}

package_target=$(mktemp -d)
cargo package --locked --no-verify --target-dir "$package_target"
crate_file="$package_name-$version.crate"
cp "$package_target/package/$crate_file" "$output_dir/$crate_file"

source_date_epoch=$(git show -s --format=%ct "$commit_sha")
sbom_stem=".release-sbom-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
SOURCE_DATE_EPOCH="$source_date_epoch" cargo cyclonedx \
    --format json \
    --all \
    --target all \
    --spec-version 1.5 \
    --override-filename "$sbom_stem"
sbom_file="$package_name-$version.cdx.json"
mv "$sbom_stem.json" "$output_dir/$sbom_file"
[[ "$(shasum -a 256 Cargo.lock | awk '{ print $1 }')" == "$original_lock_sha" ]] || {
    echo "SBOM generation changed Cargo.lock" >&2
    exit 1
}

crate_sha=$(shasum -a 256 "$output_dir/$crate_file" | awk '{ print $1 }')
sbom_sha=$(shasum -a 256 "$output_dir/$sbom_file" | awk '{ print $1 }')
lock_sha=$(shasum -a 256 Cargo.lock | awk '{ print $1 }')
changelog_sha=$(shasum -a 256 CHANGELOG.md | awk '{ print $1 }')
rust_actual=$(rustc --version --verbose)
rustdoc_actual=$(rustup run "$rustdoc_toolchain" rustdoc --version --verbose)
rust_release=$(awk -F': ' '$1 == "release" { print $2 }' <<<"$rust_actual")
rust_host=$(awk -F': ' '$1 == "host" { print $2 }' <<<"$rust_actual")
rustdoc_release=$(awk -F': ' '$1 == "release" { print $2 }' <<<"$rustdoc_actual")
rustdoc_host=$(awk -F': ' '$1 == "host" { print $2 }' <<<"$rustdoc_actual")
[[ "$rust_release" == "$rust_version" ]] || {
    echo "active rustc does not match pinned Rust $rust_version" >&2
    exit 1
}
[[ "$rustdoc_release" == *-nightly && "$rustdoc_host" == "$rust_host" ]] || {
    echo "dated rustdoc toolchain does not match the release host/nightly channel" >&2
    exit 1
}

jq -n \
    --arg package_name "$package_name" \
    --arg library_name alternator_driver \
    --arg release_mode "$release_mode" \
    --arg candidate_id "$candidate_id" \
    --arg version "$version" \
    --arg crate_file "$crate_file" \
    --arg crate_sha "$crate_sha" \
    --arg sbom_file "$sbom_file" \
    --arg sbom_sha "$sbom_sha" \
    --arg rc_tag "$rc_tag" \
    --arg commit_sha "$commit_sha" \
    --arg workflow_run_id "${GITHUB_RUN_ID:-local}" \
    --arg rust "$rust_version" \
    --arg rust_actual "$rust_actual" \
    --arg rustdoc "$rustdoc_toolchain" \
    --arg rustdoc_actual "$rustdoc_actual" \
    --arg ccm_commit "$ccm_commit" \
    --argjson scylla_versions "$scylla_versions" \
    --arg cargo_lock_sha256 "$lock_sha" \
    --arg changelog_sha256 "$changelog_sha" \
    --arg source_date_epoch "$source_date_epoch" \
    '{
        schema_version: 2,
        release_mode: $release_mode,
        candidate_id: $candidate_id,
        crate: {
            name: $package_name,
            library_name: $library_name,
            version: $version,
            file: $crate_file,
            sha256: $crate_sha
        },
        rc_tag: (if $release_mode == "release" then $rc_tag else null end),
        commit_sha: $commit_sha,
        workflow_run_id: $workflow_run_id,
        toolchains: {
            rust: {pin: $rust, actual: $rust_actual},
            rustdoc: {pin: $rustdoc, actual: $rustdoc_actual}
        },
        ccm_commit: $ccm_commit,
        scylla_versions: $scylla_versions,
        cargo_lock_sha256: $cargo_lock_sha256,
        changelog_sha256: $changelog_sha256,
        source_date_epoch: $source_date_epoch,
        sbom: {file: $sbom_file, sha256: $sbom_sha}
    }' >"$output_dir/release-manifest.json"

(
    cd "$output_dir"
    shasum -a 256 "$crate_file" "$sbom_file" release-manifest.json >SHA256SUMS
)

scripts/release/inspect-package.sh "$version" "$output_dir/$crate_file"
scripts/release/verify-candidate.sh \
    "$output_dir" \
    "$(mktemp -d)" \
    "$release_mode" \
    "$version" \
    "$candidate_id" \
    "$commit_sha" >/dev/null

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "crate_file=$crate_file" >>"$GITHUB_OUTPUT"
    echo "crate_sha256=$crate_sha" >>"$GITHUB_OUTPUT"
    echo "sbom_file=$sbom_file" >>"$GITHUB_OUTPUT"
fi

echo "candidate-crate-sha256=$crate_sha"
echo "packaged $crate_file ($crate_sha)"
