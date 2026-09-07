SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

CARGO ?= cargo
CARGO_HEATHER_VERSION := 0.3.0
CCM ?= ccm
CCM_CLUSTER ?= alternator-client-rust
CCM_IP_PREFIX ?= 127.0.0.
CCM_NODE ?= node1
CCM_SCYLLA_VERSION ?= release:2026.1
RUSTFLAGS_CCM ?= --cfg ccm_tests

.PHONY: clean verify lint lint-docs lint-fix license-install license-check license-fix compile compile-test
.PHONY: test-unit test-integration test-all
.PHONY: .prepare-ccm .prepare-environment-update-aio-max-nr
.PHONY: wait-for-alternator scylla-start scylla-stop scylla-kill scylla-rm
.PHONY: logs cqlsh

lint: license-check
	$(CARGO) fmt --all -- --check
	$(CARGO) check --all-targets
	$(CARGO) clippy --all-targets -- -D warnings
	$(CARGO) doc --no-deps

clean:
	$(CARGO) clean

verify: lint test-all

lint-docs:
	$(CARGO) doc --no-deps

lint-fix:
	$(CARGO) fmt --all

license-install:
	@command -v cargo-heather >/dev/null || $(CARGO) install cargo-heather --version $(CARGO_HEATHER_VERSION) --locked

license-check: license-install
	$(CARGO) heather

license-fix: license-install
	$(CARGO) heather --fix

compile:
	$(CARGO) build

compile-test:
	$(CARGO) test --no-run --all-targets

test-unit:
	$(CARGO) test --lib

test-integration: scylla-start wait-for-alternator
	trap '$(CCM) remove "$(CCM_CLUSTER)"' EXIT
	$(CARGO) test --tests

test-all: scylla-start wait-for-alternator
	trap '$(CCM) remove "$(CCM_CLUSTER)"' EXIT
	$(CARGO) test
	$(CCM) remove "$(CCM_CLUSTER)"
	trap - EXIT
	RUSTFLAGS="$(RUSTFLAGS_CCM)" $(CARGO) test --test ccm_wrapper_tests -- --nocapture
	RUSTFLAGS="$(RUSTFLAGS_CCM)" $(CARGO) test --test load_balancing_tests -- --nocapture

wait-for-alternator:
	echo "Waiting for Alternator to be ready..."
	for i in $$(seq 1 60); do
		if curl -sf http://localhost:8000/localnodes >/dev/null 2>&1; then
			echo "Alternator is ready (waited $${i}s)"
			exit 0
		fi
		sleep 1
	done
	echo "Timed out waiting for Alternator"
	exit 1

.prepare-environment-update-aio-max-nr:
	@if [[ -r /proc/sys/fs/aio-max-nr ]] && (( $$(< /proc/sys/fs/aio-max-nr) < 2097152 )); then
		echo 2097152 | sudo tee /proc/sys/fs/aio-max-nr >/dev/null
	fi

.prepare-ccm:
	@command -v "$(CCM)" >/dev/null || { echo "ccm is required; install scylla-ccm first"; exit 127; }

scylla-start: .prepare-ccm .prepare-environment-update-aio-max-nr
	$(CCM) remove "$(CCM_CLUSTER)" >/dev/null 2>&1 || true
	$(CCM) create "$(CCM_CLUSTER)" -n 1 -i "$(CCM_IP_PREFIX)" --scylla -v "$(CCM_SCYLLA_VERSION)"
	$(CCM) "$(CCM_NODE)" updateconf \
		alternator_address:$(CCM_IP_PREFIX)1 \
		alternator_port:8000 \
		alternator_write_isolation:always \
		alternator_response_gzip_compression_level:6 \
		alternator_response_compression_threshold_in_bytes:1
	$(CCM) start --wait-for-binary-proto --wait-other-notice

scylla-stop: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) stop

scylla-kill: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) stop --not-gently

scylla-rm: .prepare-ccm
	$(CCM) remove "$(CCM_CLUSTER)"

logs: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) "$(CCM_NODE)" showlog

cqlsh: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) "$(CCM_NODE)" cqlsh
