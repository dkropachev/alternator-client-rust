# Releasing alternator-client

This runbook covers validation and release of the `alternator-client` package.
The Rust library name remains `alternator_driver`, so downstream Rust code
continues to import it with `use alternator_driver::...`.

The release workflow has two manually dispatched modes:

- `validate` is the default. It packages the requested commit and runs the full
  release gate matrix without making a public or protected-environment change.
- `release` allocates an immutable release-candidate tag, runs the same gates,
  publishes the tested bytes through crates.io Trusted Publishing, and creates
  the final immutable GitHub Release.

Never move or delete an RC or final tag. Never publish from a working copy of
`main`. Never create an RC GitHub Release. The first GitHub Release for a
version is the final release created after crates.io contains the tested bytes.

## One-time repository administration

Complete and record these settings before using `mode=release`. A validation
run does not depend on release credentials and must not enter the protected
publishing environment.

### Enable immutable GitHub Releases

In **Settings > General > Releases**, select **Enable release immutability**.
This setting applies only to releases published after it is enabled. Immutable
releases lock their assets and associated tag and receive a GitHub release
attestation. Titles and notes remain editable, so the attached manifest and
changelog hash are the canonical release metadata.

Treat this as a permanent repository-administration invariant. The release
workflow deliberately does not hold an Administration token and therefore
cannot check the setting before publication. It does require the published
release to report `immutable: true` before declaring success. If an
administrator disables immutability, finalization may publish a mutable
release before that verification fails; re-enable the setting before any
release dispatch rather than relying on recovery afterward.

References:

- [Enable immutable releases](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/prevent-release-changes)
- [What immutable releases protect](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases)

### Create the repository-scoped release App

Create the organization-owned GitHub App
`scylladb-alternator-client-release`, install it only on
`scylladb/alternator-client-rust`, and grant it repository **Contents:
read/write** permission. Do not grant organization-wide repository access or
unrelated permissions.

Configure Actions with:

- the repository Actions variable `RELEASE_APP_ID`, containing the App's
  numeric ID; and
- the organization secret `ALTERNATOR_RELEASE_APP_PRIVATE_KEY`, made available
  only to this repository.

The private key is a repository configuration secret, not a secret of the
`crates-io` environment. The workflow uses a SHA-pinned
`actions/create-github-app-token` action to mint a short-lived installation
token restricted to the current repository. That token is used only to push
the RC and final tags and for GitHub Release operations, including recovery of
draft releases that are not visible to a read-only token. Blocker,
ruleset-configuration, and other read-only API calls use the built-in token.
Annotated-tag identity is derived from the returned App slug; do not hard-code
`github-actions[bot]` or a human identity.

Set the repository's default workflow permissions to read-only. Jobs receive
only their explicitly declared additional permissions.

### Protect release tags

In **Settings > Rules > Rulesets**, configure two active tag rulesets covering
the target pattern `v*`:

1. A creation-only ruleset with **Restrict creations** enabled. Its sole bypass
   actor is the `scylladb-alternator-client-release` App, with bypass mode
   **Always allow**. Do not put update or deletion restrictions in this ruleset
   because its bypass would then let the App bypass those restrictions too.
2. A separate immutable-tag ruleset with **Restrict updates** and **Restrict
   deletions** enabled and no bypass actors. Do not enable creation restriction
   in this ruleset.

This permits only the App to create an RC or final tag and prevents everyone,
including the App, from moving or deleting one. Protecting the final tag also
closes the interval before publication of the immutable GitHub Release;
release immutability adds a second lock afterward.

The workflow audits the rulesets before RC allocation and immediately before
each of the two tag pushes. It fails closed unless active `v*` coverage has an
App-only creation bypass with the configured App ID and a separate no-bypass
update/deletion rule. Missing or inactive coverage, the wrong App, any user or
team bypass, a bypassable combined mutable rule, pagination or authentication
failure, and malformed API data all stop the release.

GitHub may redact `bypass_actors` from ruleset API responses when the calling
identity cannot inspect bypass configuration. Redaction is intentionally a
hard failure, not evidence of an empty bypass list. During the disposable-repo
rollout, prove that the workflow's built-in token receives unredacted details;
if it does not, stop rollout and revise the audit credential design without
granting the release App permission to administer its own rulesets.

See [GitHub's ruleset rule definitions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets).

### Configure the crates.io environment and Trusted Publisher

In **Settings > Environments**, create an environment named `crates-io`.
Under deployment branches and tags, choose **Selected branches and tags**, add
the branch `main`, and add no tag patterns. Do not add required reviewers:
passing release candidates are deliberately promoted without another approval.
Disable administrator bypass if the repository plan offers that option.

Only the publishing job declares `environment: crates-io`. In particular,
validation, preflight, RC allocation, and finalization do not enter it. Keep
persistent GitHub App and crates.io credentials out of this environment. The
publishing job obtains a short-lived crates.io credential through OIDC.

On the `alternator-client` crate's **Settings > Trusted Publishing** page,
configure exactly:

- GitHub owner: `scylladb`
- repository: `alternator-client-rust`
- workflow filename: `release.yml`
- environment: `crates-io`

Enter only the workflow filename, not `.github/workflows/release.yml`, and
enable **Require trusted publishing for all new versions**. Trusted Publishing
matches the repository, workflow filename, and environment, but not the branch;
the environment restriction and workflow commit checks supply that guard.

See [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing).

### Configure organization-owned crate ownership

Create the ScyllaDB team `alternator-client-release-owners` and add
`github:scylladb:alternator-client-release-owners` as an owner of the
`alternator-client` crate. Keep the current named owners as
ownership-recovery custodians: crates.io team owners cannot change the crate's
owner list.

The named owner sending the invitation must also be a member of that GitHub
team. Its crates.io GitHub authorization must have organization access to
ScyllaDB (including `read:org`); otherwise crates.io cannot resolve the team.

See [Cargo's owner documentation](https://doc.rust-lang.org/cargo/reference/publishing.html#cargo-owner).

Record evidence of both the owner list and the exact Trusted Publisher tuple,
including that required Trusted Publishing is enabled. Do not store a
long-lived crates.io token in GitHub.

## Prepare a release PR

Prepare and merge a normal reviewed PR that:

1. sets the package version in `Cargo.toml` to the final `X.Y.Z`;
2. refreshes `Cargo.lock` so its `alternator-client` entry has the same version;
3. adds a dated `X.Y.Z` section to `CHANGELOG.md`; and
4. updates the `[Unreleased]` comparison link.

Use the final version in the package. Do not put `-rc.N` in Cargo metadata;
only a release-mode candidate tag contains the RC suffix. Let ordinary PR CI
finish before merging. Both workflow modes repeat all release gates against the
packaged candidate rather than trusting the checkout.

## Dispatch the workflow

From the Actions page, select **Release**, choose **Run workflow**, and select
`main`. Supply all three inputs:

- `mode`: `validate` or `release`; the default is `validate`.
- `version`: the exact final version in `X.Y.Z` form.
- `target_commit`: the required, lowercase, full 40-character SHA of current
  `origin/main`.

For example:

```sh
git fetch origin main
target_commit="$(git rev-parse origin/main)"
gh workflow run release.yml --ref main \
  -f mode=validate \
  -f version=1.2.3 \
  -f target_commit="$target_commit"
```

Use `mode=validate` for a dry run. Use `mode=release` only after checking the
administrative invariants above and making the explicit release decision.
Dispatch is release approval; promotion after passing gates is automatic.

The workflow explicitly fails, rather than skipping its jobs, if the dispatch
is not based on `main` or if the target is invalid. It checks out
`target_commit` and requires that value to equal the dispatch `github.sha`, the
checked-out `HEAD`, and current `origin/main`. This makes a new run fail if
`main` changes between selecting and validating the target.

There is one recovery exception, scoped to release reruns: after `main`
advances, the same workflow run may continue only if that run already owns an
annotated RC tag pointing to the same target commit. A release run that had not
yet allocated its RC must instead be freshly dispatched against current
`main`. A validation rerun never has an RC, so a validation whose target is no
longer current must always be replaced with a new run.

## Blockers and fail-closed preflight

An open GitHub Issue with the exact `release-blocker` label stops release
mutation; pull requests with that label do not count. Release mode checks once
immediately after checkout, before allocating an RC, and rechecks immediately
before every irreversible boundary: pushing a new RC tag, publishing an absent
version to crates.io, pushing a missing final tag, and changing the fully
verified draft GitHub Release to published.

The check verifies that the label exists and queries every page of matching
open issues. Missing label configuration, authentication or rate-limit
failures, pagination failures, and malformed API responses all fail closed.
There is no blocker bypass.

Preflight always rejects a version or changelog mismatch. Release mode also
rejects an existing final tag and a version published outside same-run
recovery; validation may inspect an already-published version without
promoting it. A missing registry package or unexpected registry `404` is an
error in either mode for every version, including `1.0.0`; there is no
bootstrap exception.

## Validation mode

Validation separates read-only preflight and RC planning from mutating RC
allocation. It packages the target commit and runs every portable, static,
security, documentation, and pinned-Scylla gate that release mode runs. Scylla
2025.1 predates Alternator HTTP request and response compression, so its server
gate excludes the five compression interoperability cases. Those cases run
against 2026.1, while portable client-side compression coverage still runs on
every target. Both server release lines run the complete CCM routing and
load-balancing suite.

A validation may retain Actions logs, candidate artifacts, and test evidence.
It must create no tag, deployment, release-App token, provenance or SBOM
attestation, crates.io version, or GitHub Release. The attestation gate accepts
a skipped attestation only when the manifest says `release_mode=validate`.
Validation ends with a summary gate that records the candidate digest and the
result of every release gate.

To prove this property during rollout or workflow changes, snapshot repository
tags, GitHub Releases, crates.io versions, attestations, and deployments before
and after a complete `mode=validate` run and retain the zero-difference result.

## Candidate identity and evidence

Every candidate contains:

- `alternator-client-X.Y.Z.crate`;
- `SHA256SUMS`;
- the CycloneDX JSON SBOM; and
- `release-manifest.json`.

Manifest schema v2 binds the artifact to its execution with
`release_mode`, a stable `candidate_id`, and nullable `rc_tag`, in addition to
the exact version, commit, package hashes, policy, and toolchain pins:

- Release: `release_mode` is `release` and
  `candidate_id == rc_tag == vX.Y.Z-rc.N`.
- Validate: `release_mode` is `validate`,
  `candidate_id == validation-$GITHUB_RUN_ID`, and `rc_tag` is `null`.

Candidate verification always requires the expected mode, version, candidate
ID, and commit; callers cannot omit identity arguments. Publish and
finalization accept only a schema-v2 candidate with `release_mode=release`, so
a validation artifact cannot be promoted.

The candidate artifact is named
`alternator-client-vX.Y.Z-rc.N-attempt-K` in release mode or
`alternator-client-validation-RUN_ID-attempt-K` in validation mode, where `K`
is the workflow attempt. Test evidence uses the corresponding
`release-evidence-CANDIDATE_ID-attempt-K` name, is retained for 90 days, and
contains the expected matrix targets and recorded job results.

The package is built before any gate runs. Every gate extracts, verifies, and
tests that exact package. Release mode also attests the candidate provenance
and its CycloneDX SBOM before dependent gates proceed. If any gate fails, there
is no crates.io publish, final tag, or GitHub Release.

## Release mode and promotion

After read-only preflight and a ruleset audit, release mode selects the next
unused RC number and uses the release App to push an annotated tag such as
`v1.2.3-rc.1`. The tag points to the authorized target commit and records the
workflow run's ownership. A rerun recovers only its own exact tag and commit.

For a code or packaging failure, merge a normal fix PR and dispatch the same
version again from new `main`. The old tag, artifact, attestation, evidence, and
logs remain; the new commit receives `rc.N+1`. Do not rerun an old RC after
changing source.

For an infrastructure flake, rerun the same workflow run and RC. GitHub assigns
the rerun a higher attempt number. Its regenerated crate must have the same
SHA-256 as the prior candidate for that RC. A full rerun replaces the prior
Actions artifact, so packaging also preserves the candidate digest in the
prior attempt's log and checks the regenerated crate against that record.

After all gates pass, only the publishing job enters `crates-io` and obtains a
short-lived credential through Trusted Publishing. It regenerates the crate
from the immutable RC commit and proves byte equality with the tested artifact
before `cargo publish --locked`.

After Cargo returns, including after an ambiguous publish failure, the job
polls the sparse index and downloads the registry archive. It continues only
if both registry hashes and the downloaded bytes equal the candidate. Verified
registry presence defines “published.” The workflow then re-audits tag rules,
uses the App to create annotated final tag `vX.Y.Z`, creates a draft GitHub
Release, and attaches exactly the candidate crate, `SHA256SUMS`, CycloneDX
SBOM, release manifest, and test evidence. It then rechecks blockers before
publication. Confirm that the resulting release is non-draft and shown as
**Immutable** before declaring completion.

## Historical `1.0.0` publication

`alternator-client` `1.0.0` was published from the immutable
`v1.0.0-rc.3` candidate. That one-time bootstrap is complete and has no
executable workflow or runbook path. All future versions, including any
recovery verification involving `1.0.0`, use the normal fail-closed checks;
all future routine publication uses Trusted Publishing. The only documented
exception is the two-person, confirmed-OIDC-outage break-glass procedure below.

## Failure and recovery

Recovery preserves the original candidate identity and never moves a tag:

- **Failure before RC allocation:** if the target is still current `main`, rerun
  or dispatch as appropriate. If `main` advanced, start a new run against its
  current SHA; the stale-target recovery exception does not apply.
- **Preflight, package, or test failure after RC allocation:** preserve the RC
  and evidence. Rerun the same run for an infrastructure failure. For a source
  fix, merge a PR and make a new release dispatch to obtain `rc.N+1`.
- **Release blocker or GitHub API outage:** do not bypass the check. After the
  blocker is cleared or API access recovers, rerun failed jobs from the same
  workflow run. Same-run recovery keeps its RC, registry result, final tag, and
  draft release authoritative.
- **Ambiguous publish result:** query the sparse index and registry archive. If
  absent, retry the same RC through the same workflow; if the digest and bytes
  match, resume finalization; if either differs, stop and escalate. A registry
  `404` during preflight is not authorization to claim or bootstrap a package.
- **crates.io succeeded but finalization failed:** rerun failed jobs from the
  same workflow run. Recovery verifies the existing registry bytes before
  creating or completing the final tag and GitHub Release.
- **Final tag exists without a published immutable release:** do not move it.
  Investigate and resume the same finalization job; do not dispatch another RC.
- **Draft release exists:** finalization verifies and reuses it and fills or
  validates the canonical assets before publication.
- **Published GitHub Release exists:** never update or delete its assets or
  automate release/tag deletion. Notes may be corrected, but the attached
  manifest and changelog hash remain canonical. An already-published immutable
  release is verified, not blocked retroactively.

To resume failed jobs, use:

```sh
gh run rerun RUN_ID --repo scylladb/alternator-client-rust --failed
```

Do not start a fresh dispatch to recover the same unchanged candidate after an
RC or partial publication exists. A fresh run cannot own the earlier run's RC
and is not a substitute for same-run recovery. A source fix is different: merge
the normal PR and make a fresh dispatch for the new commit and `rc.N+1`.

## Two-person break-glass procedure

Break-glass requires two recorded approvers. It never authorizes bypassing a
release blocker, immutable-tag protection, the latest-RC assertion, candidate
identity checks, or byte-for-byte comparison with the tested artifact.

For a GitHub App failure, rotate its private key or repair/reinstall the App's
repository-scoped installation and permissions, update the restricted secret
if necessary. Rerun the same workflow if it already owns its RC, or if its
target is still current `main`. If the failure occurred before RC allocation
and `main` advanced, make a new dispatch for current `main` instead. Do not push
a tag with a maintainer or personal token and do not weaken either tag ruleset.

Only for a confirmed crates.io OIDC outage, the two approvers may authorize the
shortest-lived package-scoped crates.io token practical. Temporarily make only
the minimum Trusted Publishing configuration change needed for that token.

Use a fresh temporary checkout and the exact candidate artifact from the same
workflow run. Set `RUN_ID`, `VERSION`, `RC_TAG`, and `ARTIFACT_NAME` from that
run, export `GH_TOKEN` with access to read the repository and release-blocker
issues, then execute this ordered procedure:

```sh
set -euo pipefail
export REPOSITORY=scylladb/alternator-client-rust
break_glass_root="$(mktemp -d)"

git clone --no-checkout "https://github.com/$REPOSITORY.git" \
  "$break_glass_root/source"
git -C "$break_glass_root/source" fetch --force origin \
  "refs/tags/$RC_TAG:refs/tags/$RC_TAG"
git -C "$break_glass_root/source" checkout --detach "$RC_TAG^{commit}"
TARGET_COMMIT="$(git -C "$break_glass_root/source" rev-parse HEAD)"

mkdir "$break_glass_root/candidate"
gh run download "$RUN_ID" --repo "$REPOSITORY" \
  --name "$ARTIFACT_NAME" --dir "$break_glass_root/candidate"

cd "$break_glass_root/source"
export GITHUB_REPOSITORY="$REPOSITORY"
export GITHUB_RUN_ID="$RUN_ID"
bash scripts/release/verify-candidate.sh \
  "$break_glass_root/candidate" "$break_glass_root/extract" \
  release "$VERSION" "$RC_TAG" "$TARGET_COMMIT"
bash scripts/release/check-release-blockers.sh
bash scripts/release/assert-latest-rc.sh \
  "$VERSION" "$RC_TAG" "$TARGET_COMMIT"
bash scripts/release/repackage-and-compare.sh \
  "$VERSION" "$TARGET_COMMIT" \
  "$break_glass_root/candidate/alternator-client-$VERSION.crate"
registry_state="$(mktemp)"
GITHUB_OUTPUT="$registry_state" bash scripts/release/registry-state.sh \
  alternator-client "$VERSION" \
  "$break_glass_root/candidate/alternator-client-$VERSION.crate"
test "$(awk -F= '$1 == "state" { print $2 }' "$registry_state")" = absent

read -rsp 'temporary crates.io token: ' CARGO_REGISTRY_TOKEN
export CARGO_REGISTRY_TOKEN
printf '\n'
publish_status=0
rustup run 1.94.1 bash scripts/release/publish-and-verify.sh \
  release "$PWD" "$VERSION" "$RC_TAG" "$TARGET_COMMIT" \
  "$break_glass_root/candidate" || publish_status=$?
unset CARGO_REGISTRY_TOKEN
printf 'publish-and-verify exit status: %s\n' "$publish_status"
```

Every command before the token prompt must pass. Revoke the token immediately
after the attempt, including after a timeout or ambiguous result, before doing
further investigation. Restore **Require trusted publishing for all new
versions**, verify the registry bytes, and rerun only failed jobs in the same
workflow. If the exact version reached crates.io, its idempotent registry check
skips OIDC and resumes finalization; otherwise do not retry until the incident
owners establish the registry state.

The incident record must include both approvers, the incident and confirmed
failure mode, workflow run ID, target SHA, RC tag, artifact digests, every
temporary configuration change, token creation and revocation, App credential
rotation where applicable, restoration of Trusted Publishing enforcement, and
the final verification outcome.

## Configuration proof and rollout

Before enabling the production creation restriction, reproduce the two tag
rulesets in a disposable repository using the same App. Prove that a maintainer
cannot create a matching `v*` tag, the App can create one, and neither the
maintainer nor the App can update or delete it. Retain the API responses and
attempt results as configuration evidence.

Roll out these controls in order:

1. merge the focused release-blocker changes;
2. create and install the release App and configure its restricted ID/key;
3. prove App and ruleset behavior in the disposable repository;
4. enable the creation-only production ruleset, temporarily freezing release
   creation until the workflow support is merged;
5. merge the workflow, scripts, tests, and documentation implementing the rest
   of this contract;
6. set default workflow permissions to read-only;
7. add the crates.io team owner and verify the exact required Trusted Publisher;
8. run the full zero-public-mutation validation proof; and
9. close issue #119 only after all configuration evidence is recorded.

## Maintaining pinned inputs

`.github/workflows/maintenance.yml` runs weekly without publishing anything.
One job resolves the newest compatible dependency graph and runs the shared
source/portable gates; the other tests the moving `2026.1` and `2025.1` Scylla
release lines. Treat its failures as update signals, not as permission for a
workflow to rewrite release inputs.

Change Rust, dated nightly, CCM, exact Scylla patch, release-tool, runner, or
action-SHA pins only in a normal reviewed PR. Validate replacement pins in CI
before merging. The portable and pinned-Scylla release matrices have a single
reviewed source in `scripts/release/release-policy.json`; the workflow,
candidate manifest, verifier, and evidence record all read it. The policy also
records which pinned server release supports Alternator HTTP compression.
Never make a scheduled job commit those updates directly.

At completion, the crates.io archive, tested Actions artifact, manifest digest,
and GitHub Release asset must all identify the same bytes, candidate, and
commit.
