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

[[ -n "${GITHUB_REPOSITORY:-}" ]] || {
    echo "GITHUB_REPOSITORY is required" >&2
    exit 1
}
[[ "$GITHUB_REPOSITORY" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]] || {
    echo "GITHUB_REPOSITORY must be in owner/repository form" >&2
    exit 1
}
[[ -n "${GH_TOKEN:-}" ]] || {
    echo "GH_TOKEN is required" >&2
    exit 1
}
[[ "${RELEASE_APP_ID:-}" =~ ^[1-9][0-9]*$ ]] || {
    echo "RELEASE_APP_ID must be a positive integer" >&2
    exit 1
}

for required_command in gh jq; do
    command -v "$required_command" >/dev/null 2>&1 || {
        echo "$required_command is required" >&2
        exit 1
    }
done

api_version='X-GitHub-Api-Version: 2026-03-10'
accept_header='Accept: application/vnd.github+json'
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT
list_response=$work_dir/list.json
details_response=$work_dir/details.jsonl

if ! gh api --paginate --slurp \
    -H "$accept_header" -H "$api_version" \
    "repos/$GITHUB_REPOSITORY/rulesets?includes_parents=false&per_page=100" \
    >"$list_response"; then
    echo "failed to query every page of repository rulesets" >&2
    exit 1
fi

if ! jq -se '
    def valid_summary:
        type == "object"
        and (.id | type == "number" and . > 0 and floor == .);

    length == 1
    and (.[0] |
        type == "array"
        and length > 0
        and all(.[];
            type == "array"
            and all(.[]; valid_summary)))
    and (([.[0][][] | .id] | length) == ([.[0][][] | .id] | unique | length))
' "$list_response" >/dev/null; then
    echo "ruleset list API returned an empty, malformed, or duplicate response" >&2
    exit 1
fi

: >"$details_response"
while IFS= read -r ruleset_id; do
    detail_file=$work_dir/detail-$ruleset_id.json
    if ! gh api -H "$accept_header" -H "$api_version" \
        "repos/$GITHUB_REPOSITORY/rulesets/$ruleset_id?includes_parents=false" \
        >"$detail_file"; then
        echo "failed to read repository ruleset $ruleset_id" >&2
        exit 1
    fi
    if ! jq -se --argjson expected_id "$ruleset_id" '
        length == 1
        and (.[0] |
            type == "object"
            and .id == $expected_id
            and (.name | type == "string" and length > 0)
            and (.target | type == "string" and length > 0)
            and (.source_type | type == "string" and length > 0)
            and (.source | type == "string" and length > 0)
            and (.enforcement | type == "string"
                and (. == "active" or . == "disabled" or . == "evaluate"))
            and (.bypass_actors | type == "array"
                and all(.[];
                    type == "object"
                    and (.actor_type | type == "string" and length > 0)
                    and ((.actor_id | type) == "number" or .actor_id == null)
                    and (.bypass_mode | type == "string" and length > 0)))
            and (.conditions | type == "object")
            and (.rules | type == "array" and all(.[];
                type == "object"
                and (.type | type == "string" and length > 0))))
    ' "$detail_file" >/dev/null; then
        echo "ruleset detail API returned malformed data for ruleset $ruleset_id" >&2
        exit 1
    fi
    jq -c . "$detail_file" >>"$details_response"
done < <(jq -r '.[][] | .id' "$list_response")

if ! jq -se \
    --arg repository "$GITHUB_REPOSITORY" \
    --argjson app_id "$RELEASE_APP_ID" '
    def exact_ref_scope:
        .conditions.ref_name as $ref
        | ($ref | type == "object")
        and ($ref.include == ["refs/tags/v*"])
        and ($ref.exclude == []);

    def active_repository_tags:
        .source_type == "Repository"
        and (.source | ascii_downcase) == ($repository | ascii_downcase)
        and .target == "tag"
        and .enforcement == "active";

    def exact_app_bypass:
        .bypass_actors == [{
            "actor_id": $app_id,
            "actor_type": "Integration",
            "bypass_mode": "always"
        }];

    def rule_types: [.rules[].type] | sort;
    def creation_only:
        active_repository_tags
        and exact_ref_scope
        and exact_app_bypass
        and rule_types == ["creation"];
    def immutable_no_bypass:
        active_repository_tags
        and exact_ref_scope
        and .bypass_actors == []
        and (rule_types | index("deletion")) != null
        and (rule_types | index("update")) != null
        and (rule_types | index("creation")) == null;
    def has_mutation_rule:
        any(.rules[]; .type == "creation" or .type == "update" or .type == "deletion");

    . as $rulesets
    | ($rulesets | map(select(creation_only))) as $creation
    | ($rulesets | map(select(immutable_no_bypass))) as $immutable
    | ($rulesets | map(select(
        active_repository_tags
        and exact_ref_scope
        and has_mutation_rule
        and ((creation_only or immutable_no_bypass) | not)))) as $unsafe
    | ($creation | length) == 1
      and ($immutable | length) == 1
      and $creation[0].id != $immutable[0].id
      and ($unsafe | length) == 0
' "$details_response" >/dev/null; then
    echo "release tag rulesets are unsafe: require one active App-only creation ruleset and one distinct active no-bypass update/deletion ruleset, both scoped exactly to refs/tags/v*" >&2
    exit 1
fi

echo "release tag rulesets are active and correctly separated"
