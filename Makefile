COMPOSE := docker compose
CARGO_HEATHER_VERSION := 0.3.0

.PHONY: all
all: static test down

.PHONY: static
static: license-check fmt-check check clippy

.PHONY: license-install
license-install:
	@command -v cargo-heather >/dev/null || cargo install cargo-heather --version $(CARGO_HEATHER_VERSION) --locked

.PHONY: license-check
license-check: license-install
	cargo heather

.PHONY: license-fix
license-fix: license-install
	cargo heather --fix

.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: fmt-check
fmt-check:
	cargo fmt --all -- --check

.PHONY: check
check:
	cargo check --all-targets

.PHONY: clippy
clippy:
	cargo clippy --all-targets -- -D warnings
	
.PHONY: test
test: up
	cargo test

.PHONY: ccm-wrapper-tests load-balancing-tests ccm-tests
ccm-wrapper-tests:
	RUSTFLAGS="--cfg ccm_tests" cargo test --test ccm_wrapper_tests -- --nocapture

load-balancing-tests:
	RUSTFLAGS="--cfg ccm_tests" cargo test --test load_balancing_tests -- --nocapture

ccm-tests: ccm-wrapper-tests load-balancing-tests

.PHONY: up
up:
	$(COMPOSE) up -d --wait
	@echo
	@echo "1 scylla node is running in the background. Use 'make down' to stop it and remove its volume."
	@echo

.PHONY: down
down:
	$(COMPOSE) down --remove-orphans -v

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: cqlsh
cqlsh:
	$(COMPOSE) exec scylla_node cqlsh -u cassandra -p cassandra

.PHONY: shell
shell:
	$(COMPOSE) exec scylla_node bash

.PHONY: volumes
volumes:
	docker volume ls

.PHONY: prune
prune:
	docker system prune -a --volumes

.PHONY: clean
clean: down
	cargo clean
