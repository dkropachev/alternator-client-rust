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

[[ $# -eq 1 ]] || {
    echo "usage: $0 portable-matrix|scylla-matrix|scylla-versions" >&2
    exit 2
}

query=$1
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
policy_file=${RELEASE_POLICY_FILE:-$script_dir/release-policy.json}

[[ -f "$policy_file" ]] || {
    echo "release policy does not exist: $policy_file" >&2
    exit 1
}

jq -e '
    def nonempty_string: type == "string" and length > 0;
    .schema_version == 1 and
    (.portable_targets | type == "array" and length > 0) and
    (all(.portable_targets[];
        (.runner | nonempty_string) and
        (.target | nonempty_string) and
        ((keys | sort) == ["runner", "target"]))) and
    ((.portable_targets | unique | length) == (.portable_targets | length)) and
    (.scylla_targets | type == "array" and length > 0) and
    (all(.scylla_targets[];
        (.runner | nonempty_string) and
        (.target | nonempty_string) and
        (.arch | nonempty_string) and
        ((keys | sort) == ["arch", "runner", "target"]))) and
    ((.scylla_targets | unique | length) == (.scylla_targets | length)) and
    (.scylla_releases | type == "array" and length > 0) and
    (all(.scylla_releases[];
        (.version | type == "string" and
            test("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$")) and
        (.alternator_http_compression | type == "boolean") and
        ((keys | sort) == ["alternator_http_compression", "version"]))) and
    (([.scylla_releases[].version] | unique | length) == (.scylla_releases | length))
' "$policy_file" >/dev/null || {
    echo "release policy is invalid: $policy_file" >&2
    exit 1
}

case "$query" in
    portable-matrix)
        jq -c '.portable_targets' "$policy_file"
        ;;
    scylla-matrix)
        jq -c '[
            .scylla_targets[] as $target |
            .scylla_releases[] as $release |
            $target + {
                scylla: $release.version,
                alternator_http_compression: $release.alternator_http_compression
            }
        ]' "$policy_file"
        ;;
    scylla-versions)
        jq -c '[.scylla_releases[].version]' "$policy_file"
        ;;
    *)
        echo "unknown release-policy query: $query" >&2
        exit 2
        ;;
esac
