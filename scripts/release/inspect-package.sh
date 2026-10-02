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

[[ $# -eq 2 ]] || {
    echo "usage: $0 VERSION CRATE_FILE" >&2
    exit 2
}

version=$1
crate_file=$2
root="alternator-client-$version"

[[ -f "$crate_file" ]] || {
    echo "crate archive does not exist: $crate_file" >&2
    exit 1
}

entries=$(tar -tzf "$crate_file")
for required in \
    Cargo.toml Cargo.toml.orig Cargo.lock README.md CHANGELOG.md LICENSE Makefile \
    .cargo-heather.toml .cargo_vcs_info.json deny.toml src/lib.rs \
    scripts/release/inspect-package.sh; do
    printf '%s\n' "$entries" | grep -Fxq "$root/$required" || {
        echo "packaged crate is missing $required" >&2
        exit 1
    }
done

if printf '%s\n' "$entries" | grep -E "^$root/(\.git(hub|ignore)?(/|$)|target(/|$)|docs(/|$))"; then
    echo "packaged crate contains release-only or VCS files" >&2
    exit 1
fi

if printf '%s\n' "$entries" | awk -F/ -v root="$root" '
    $1 != root { print; bad = 1; next }
    $2 == "scripts" && $3 == "release" && $4 == "inspect-package.sh" && NF == 4 { next }
    $2 ~ /^(\.cargo_vcs_info\.json|\.cargo-heather\.toml|CHANGELOG\.md|Cargo\.lock|Cargo\.toml|Cargo\.toml\.orig|LICENSE|Makefile|README\.md|deny\.toml|examples|src|tests)$/ { next }
    { print; bad = 1 }
    END { exit bad ? 0 : 1 }
'; then
    echo "packaged crate contains a file outside the allowlisted surface" >&2
    exit 1
fi

echo "package contents are restricted to the reviewed allowlist"
