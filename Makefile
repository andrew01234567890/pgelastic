# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# INSTANCE_IMG carries the instance manager; PG_IMG carries PostgreSQL 18 and pgBackRest.
INSTANCE_IMG ?= pgelastic/instance:latest
PG_IMG ?= pgelastic/postgres:18
# PROXY_IMG carries the inline proxy fleet.
PROXY_IMG ?= pgelastic/proxy:latest
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= pgelastic-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v
	$(MAKE) test-e2e-instance E2E_CONTEXT=kind-$(KIND_CLUSTER)
# Migration drives two full three-node instances through logical replication. On a
# two-core runner it does not fit the per-push budget, so it runs with the chaos specs
# rather than being given a longer timeout that would only make the slow path flakier.
ifeq ($(E2E_HEAVY),true)
	$(MAKE) test-e2e-migration E2E_CONTEXT=kind-$(KIND_CLUSTER)
	$(MAKE) test-e2e-tenantdb E2E_CONTEXT=kind-$(KIND_CLUSTER)
# The proxy suite stands up two full three-node instances and a fleet in front of them, so
# it costs what migration costs and runs where migration runs.
	$(MAKE) test-e2e-proxy E2E_CONTEXT=kind-$(KIND_CLUSTER)
# The restart suite drives real Pod recreations and a real switchover, six postmasters and a
# fleet. It costs what the other two cost and belongs with them.
	$(MAKE) test-e2e-restart E2E_CONTEXT=kind-$(KIND_CLUSTER)
endif
	$(MAKE) cleanup-test-e2e

# The instance e2e provisions a real three-node PostgreSQL 18 instance and asserts on what
# PostgreSQL reports, not on what the operator believes.
#
# E2E_CONTEXT selects the cluster. docker-desktop is the default because it shares the
# local Docker daemon, so a locally built image is already present on the node and the
# load step is a no-op; it is also visible in the Docker Desktop UI. Kind needs an
# explicit `kind load` because its nodes run their own containerd. kind names its context
# kind-<cluster>, which is how test-e2e hands over the cluster it just created.
E2E_CONTEXT ?= docker-desktop
E2E_TIMEOUT ?= 60m
# Ginkgo label filter for the instance suite; empty runs everything. The chaos specs carry
# the `chaos` label, so CI's per-push job passes '!chaos' and the nightly job passes none.
E2E_LABEL_FILTER ?=

.PHONY: test-e2e-instance
test-e2e-instance: manifests generate install-e2e load-instance-images ## Provision a real 3-node PostgreSQL 18 instance and assert on it.
	PGELASTIC_POSTGRES_IMG=$(PG_IMG) PGELASTIC_INSTANCE_IMG=$(INSTANCE_IMG) E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/instance/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		$(if $(E2E_LABEL_FILTER),-ginkgo.label-filter='$(E2E_LABEL_FILTER)')

# The migration e2e moves a tenant between two real three-node instances, by logical
# replication and by dump and restore, and measures the pause rather than asserting it. It
# stands a proxy fleet up as well, because the claim it exists to check - clients queued and
# never dropped - is about a client's socket at that fleet and cannot be made without one.
.PHONY: test-e2e-migration
test-e2e-migration: manifests generate install-e2e install-cert-manager load-data-plane-images ## Move a tenant between two real PostgreSQL 18 instances.
	PGELASTIC_POSTGRES_IMG=$(PG_IMG) PGELASTIC_INSTANCE_IMG=$(INSTANCE_IMG) \
	PGELASTIC_PROXY_IMG=$(PROXY_IMG) E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/migration/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		$(if $(E2E_LABEL_FILTER),-ginkgo.label-filter='$(E2E_LABEL_FILTER)')

# The placement e2e drives the placement and capacity-planning loops against a real API
# server. It provisions no PostgreSQL: what placement consumes from a member instance is
# exactly what that instance publishes in status, and the provisioning path has its own
# suite. What this one proves that envtest cannot is that the whole plan survives a real
# CRD, where a pruned status field is indistinguishable from a decision not to write one.
# The placement specs stand up no PostgreSQL, so the tenant-provisioning specs that share
# the suite are excluded by their `postgres` label unless the caller asks for something else.
PLACEMENT_LABEL_FILTER ?= $(if $(E2E_LABEL_FILTER),$(E2E_LABEL_FILTER),!postgres)

.PHONY: test-e2e-placement
test-e2e-placement: manifests generate install-e2e ## Place a tenant population across a pool and assert the plan.
	E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/placement/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		-ginkgo.label-filter='$(PLACEMENT_LABEL_FILTER)'

# The tenant-provisioning e2e is the only place that answers "does this tenant's database
# exist" by asking PostgreSQL rather than by reading the PgTenant, which is the witness that
# was wrong. It needs a real instance, so it carries the images and the long timeout.
.PHONY: test-e2e-tenantdb
test-e2e-tenantdb: manifests generate install-e2e load-instance-images ## Create a tenant's database on a real instance and assert it from PostgreSQL.
	PGELASTIC_POSTGRES_IMG=$(PG_IMG) PGELASTIC_INSTANCE_IMG=$(INSTANCE_IMG) E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/placement/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		-ginkgo.label-filter='postgres'

# The proxy e2e is the only place that answers "can a client reach its tenant's database
# through the pool" by connecting to the pool's Service and running a query, rather than by
# reading what the operator believes about the fleet it created.
.PHONY: test-e2e-proxy
test-e2e-proxy: manifests generate install-e2e install-cert-manager load-data-plane-images ## Route a client to its tenant's database through a real proxy fleet.
	PGELASTIC_POSTGRES_IMG=$(PG_IMG) PGELASTIC_INSTANCE_IMG=$(INSTANCE_IMG) \
	PGELASTIC_PROXY_IMG=$(PROXY_IMG) E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/proxy/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		$(if $(E2E_LABEL_FILTER),-ginkgo.label-filter='$(E2E_LABEL_FILTER)')

# The restart e2e rolls a real three-node instance under a client that never disconnects,
# and is the only place the claim "a restart costs a latency spike and not a connection" is
# answered by the client's own socket rather than by the operator's account of its pause. It
# needs the proxy image because that claim cannot be made without a fleet holding the
# clients.
#
# The drain-trap spec needs more than one node - a switchover away from a node that is going
# away is pointless when every member is on it - so it is excluded by its `multinode` label
# unless the caller asks for something else. The coexistence spec is excluded for the same
# shape of reason: it requires a second, deployed operator to be running on the cluster, and
# a CI cluster that only has the CRDs installed has nothing for it to coexist with. It fails
# rather than skipping when asked for, so it is asked for explicitly.
RESTART_LABEL_FILTER ?= $(if $(E2E_LABEL_FILTER),$(E2E_LABEL_FILTER),!multinode && !coexistence)

.PHONY: test-e2e-restart
test-e2e-restart: manifests generate install-e2e install-cert-manager load-data-plane-images ## Roll a real 3-node instance under load and assert no client saw an error.
	PGELASTIC_POSTGRES_IMG=$(PG_IMG) PGELASTIC_INSTANCE_IMG=$(INSTANCE_IMG) \
	PGELASTIC_PROXY_IMG=$(PROXY_IMG) E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/restart/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		-ginkgo.label-filter='$(RESTART_LABEL_FILTER)'

# The coexistence e2e is the proof that pgelastic can share a cluster with pgelastic. It
# stands the restart suite's pool and fleet up next to an operator already deployed on
# E2E_CONTEXT and asserts that neither touches the other's objects. Deploy the operator
# first - `make docker-build deploy IMG=...` - because the spec fails, deliberately, if
# there is nothing running for it to coexist with.
.PHONY: test-e2e-coexistence
test-e2e-coexistence: manifests generate install-e2e install-cert-manager load-data-plane-images ## Prove two pgelastic operators can share one cluster.
	PGELASTIC_POSTGRES_IMG=$(PG_IMG) PGELASTIC_INSTANCE_IMG=$(INSTANCE_IMG) \
	PGELASTIC_PROXY_IMG=$(PROXY_IMG) E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/restart/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		-ginkgo.label-filter='coexistence'

# cert-manager issues the certificates the proxy fleet's control listener presents and the
# operator's client presents back to it. Without it the operator degrades to a fleet with no
# control listener at all, so a suite that stands a fleet up and then expects a cutover needs
# cert-manager present rather than absent.
#
# It is installed here rather than in the scaffold suite's BeforeSuite because that suite runs
# only under `make test-e2e`, while every suite that actually needs cert-manager runs outside
# it - which is why the nightly ran for months against a cluster that never had it.
CERT_MANAGER_VERSION ?= v1.20.2
CERT_MANAGER_URL ?= https://github.com/cert-manager/cert-manager/releases/download/$(CERT_MANAGER_VERSION)/cert-manager.yaml

.PHONY: install-cert-manager
install-cert-manager: ## Install cert-manager into E2E_CONTEXT unless it is already there.
ifneq ($(CERT_MANAGER_INSTALL_SKIP),true)
	@if "$(KUBECTL)" --context=$(E2E_CONTEXT) get crd certificates.cert-manager.io >/dev/null 2>&1; then \
		echo "cert-manager is already installed in $(E2E_CONTEXT)."; \
	else \
		echo "Installing cert-manager $(CERT_MANAGER_VERSION) into $(E2E_CONTEXT)..."; \
		"$(KUBECTL)" --context=$(E2E_CONTEXT) apply -f $(CERT_MANAGER_URL); \
	fi
	@# Waited for unconditionally, because "the CRDs exist" and "the webhook will answer" are
	@# different facts and a Certificate created between them is rejected rather than queued.
	"$(KUBECTL)" --context=$(E2E_CONTEXT) wait deployment.apps/cert-manager-webhook \
		--for=condition=Available --namespace=cert-manager --timeout=5m
endif

.PHONY: install-e2e
install-e2e: manifests kustomize ## Install CRDs into E2E_CONTEXT.
	"$(KUSTOMIZE)" build config/crd | "$(KUBECTL)" --context=$(E2E_CONTEXT) apply --server-side --force-conflicts -f -

.PHONY: load-instance-images
load-instance-images: docker-build-instance docker-build-postgres ## Make the instance images reachable from E2E_CONTEXT.
	@case "$(E2E_CONTEXT)" in \
	  kind-*) "$(KIND)" load docker-image $(INSTANCE_IMG) --name $${E2E_CONTEXT#kind-}; \
	          "$(KIND)" load docker-image $(PG_IMG) --name $${E2E_CONTEXT#kind-} ;; \
	  *) echo "$(E2E_CONTEXT) shares the local Docker daemon; images already reachable" ;; \
	esac

# The proxy is not part of an instance, so it is a separate step rather than a third image
# in the one above: a suite that never stands a fleet up would otherwise pay for a Rust
# release build it has no use for.
.PHONY: load-data-plane-images
load-data-plane-images: load-instance-images docker-build-proxy ## Make every image a client's path runs on reachable from E2E_CONTEXT.
	@case "$(E2E_CONTEXT)" in \
	  kind-*) "$(KIND)" load docker-image $(PROXY_IMG) --name $${E2E_CONTEXT#kind-} ;; \
	  *) echo "$(E2E_CONTEXT) shares the local Docker daemon; the proxy image is already reachable" ;; \
	esac

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: preflight
preflight: ## Run everything CI runs, in CI's order. Do this before pushing.
	@# The whole point is that it is one command. This repository is half Rust and half Go, the
	@# two halves are checked by different tools, and running "the tests" in either language
	@# leaves the other half unchecked -- which is how a clean local run has repeatedly been
	@# followed by a red CI. Mirrors .github/workflows/rust.yml and test.yml step for step; if
	@# CI grows a check, it belongs here too.
	@#
	@# Fails on the first problem rather than collecting them, because the second check's output
	@# is rarely worth reading once the first has failed.
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features
	cargo test --workspace --all-features
	go build ./...
	$(MAKE) test
	$(MAKE) lint
	@echo
	@echo "preflight passed: fmt, clippy, cargo test, go build, go test, lint"

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Verification

# The durability oracle. VERIFY_DSN should normally point at the proxy; point it at
# PostgreSQL directly only to calibrate the oracle itself.
VERIFY_DSN ?=
VERIFY_LEDGER ?= verify-ledger.log
VERIFY_TABLE ?= set
VERIFY_WRITERS ?= 8
VERIFY_DURATION ?= 60s

.PHONY: build-verify
build-verify: ## Build the pgelastic-verify durability oracle.
	go build -o bin/pgelastic-verify ./cmd/verify

.PHONY: verify
verify: build-verify ## Run the durability oracle against VERIFY_DSN, then check it. Exits 1 on a lost commit.
	@test -n "$(VERIFY_DSN)" || { echo "Set VERIFY_DSN, e.g. VERIFY_DSN=postgres://user:pw@host:5432/db"; exit 3; }
	bin/pgelastic-verify run \
		--dsn "$(VERIFY_DSN)" \
		--ledger "$(VERIFY_LEDGER)" \
		--table "$(VERIFY_TABLE)" \
		--writers $(VERIFY_WRITERS) \
		--duration $(VERIFY_DURATION) \
		--check

.PHONY: verify-check
verify-check: build-verify ## Check an existing ledger against VERIFY_DSN without writing anything.
	@test -n "$(VERIFY_DSN)" || { echo "Set VERIFY_DSN, e.g. VERIFY_DSN=postgres://user:pw@host:5432/db"; exit 3; }
	bin/pgelastic-verify check \
		--dsn "$(VERIFY_DSN)" \
		--ledger "$(VERIFY_LEDGER)" \
		--table "$(VERIFY_TABLE)"

.PHONY: test-verify
test-verify: ## Run the durability oracle's own tests. Needs a container runtime for the integration specs.
	go test ./cmd/verify/... ./internal/verify/...

##@ Benchmark

# The proxy benchmark. Its purpose is to decide, on evidence, whether the data plane should
# be rewritten in Go, so every part of it is arranged to make a flattering number hard to
# produce: the thresholds are committed constants (internal/bench/criteria.go), the machine
# is recorded in every result file, and a cell whose repetitions do not agree is reported as
# INCONCLUSIVE rather than resolved.
#
# The three components share one user-defined Docker bridge rather than published ports.
# Under WSL2 there is no docker0 and every published port crosses a userspace relay, whose
# cost is multiplied by the number of backend round trips - and the arms deliberately differ
# in round-trip count, so it would not cancel out in a delta.
BENCH_NETWORK ?= pgebench
BENCH_PG_NAME ?= pgebench-pg
BENCH_PG_IMG ?= postgres:18
BENCH_PG_PORT ?= 15432
BENCH_DIR ?= docs/bench

# The core budget is expressed in physical cores and mapped onto SMT pairs. The proxy never
# shares a pair with anything else; the load generator may share with PostgreSQL, and that
# is recorded rather than hidden.
BENCH_PG_CPUS ?= 0-5,16-21
BENCH_PROXY_CPUS ?= 6-9,22-25
BENCH_LOADGEN_CPUS ?= 10-13,26-29

BENCH_WORKLOAD ?= throughput
BENCH_CONCURRENCY ?= 1,8,64,256
BENCH_DURATION ?= 20s
BENCH_WARMUP ?= 5s
BENCH_REPETITIONS ?= 5
BENCH_RATE ?= 0
BENCH_METRIC ?= throughput
BENCH_DSN ?= postgres://bench:bench@localhost:$(BENCH_PG_PORT)/bench?sslmode=disable

.PHONY: build-bench
build-bench: ## Build the benchmark driver.
	go build -trimpath -o bin/pgebench ./test/bench/cmd/pgebench

.PHONY: bench-doctor
bench-doctor: build-bench ## Report what this machine is and which axes it can decide.
	bin/pgebench doctor

# fsync and synchronous_commit are off because every workload here is select-only: leaving
# them on would measure the disk rather than the proxy. shared_buffers is sized so the
# working set is served from memory, for the same reason.
.PHONY: bench-stack-up
bench-stack-up: ## Start the pinned PostgreSQL the benchmark measures against.
	-docker network create $(BENCH_NETWORK)
	-docker rm -f $(BENCH_PG_NAME)
	docker run -d --name $(BENCH_PG_NAME) --network $(BENCH_NETWORK) \
		--cpuset-cpus $(BENCH_PG_CPUS) --memory 4g \
		-e POSTGRES_USER=bench -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=bench \
		-p $(BENCH_PG_PORT):5432 $(BENCH_PG_IMG) \
		-c shared_buffers=2GB -c max_connections=1000 \
		-c fsync=off -c synchronous_commit=off -c log_min_messages=fatal
	@until docker exec $(BENCH_PG_NAME) pg_isready -U bench >/dev/null 2>&1; do sleep 1; done
	@echo "postgres ready on port $(BENCH_PG_PORT), pinned to $(BENCH_PG_CPUS)"

.PHONY: bench-stack-down
bench-stack-down: ## Stop the benchmark's PostgreSQL.
	-docker rm -f $(BENCH_PG_NAME)

# The no-proxy baseline is not optional. Almost all of a query's latency is PostgreSQL, so
# without it the two proxy numbers are a ratio between two unknowns.
.PHONY: bench-baseline
bench-baseline: build-bench ## Measure PostgreSQL with no proxy in front of it.
	@mkdir -p $(BENCH_DIR)
	taskset -c $(BENCH_LOADGEN_CPUS) bin/pgebench run \
		--target direct --dsn "$(BENCH_DSN)" \
		--workload $(BENCH_WORKLOAD) --concurrency $(BENCH_CONCURRENCY) \
		--duration $(BENCH_DURATION) --warmup $(BENCH_WARMUP) \
		--repetitions $(BENCH_REPETITIONS) --rate $(BENCH_RATE) \
		--out $(BENCH_DIR)/direct-$(BENCH_WORKLOAD).json

.PHONY: bench-rust
bench-rust: build-bench ## Measure the Rust proxy. BENCH_DSN must point at it.
	@mkdir -p $(BENCH_DIR)
	taskset -c $(BENCH_LOADGEN_CPUS) bin/pgebench run \
		--target rust --dsn "$(BENCH_DSN)" \
		--workload $(BENCH_WORKLOAD) --concurrency $(BENCH_CONCURRENCY) \
		--duration $(BENCH_DURATION) --warmup $(BENCH_WARMUP) \
		--repetitions $(BENCH_REPETITIONS) --rate $(BENCH_RATE) \
		--out $(BENCH_DIR)/rust-$(BENCH_WORKLOAD).json

.PHONY: bench-report
bench-report: build-bench ## Apply the pre-registered criteria to whichever reports exist.
	@bin/pgebench compare \
		--direct $(BENCH_DIR)/direct-$(BENCH_WORKLOAD).json \
		$(if $(wildcard $(BENCH_DIR)/rust-$(BENCH_WORKLOAD).json),--rust $(BENCH_DIR)/rust-$(BENCH_WORKLOAD).json) \
		$(if $(wildcard $(BENCH_DIR)/go-$(BENCH_WORKLOAD).json),--go $(BENCH_DIR)/go-$(BENCH_WORKLOAD).json) \
		$(if $(wildcard $(BENCH_DIR)/pgbouncer-$(BENCH_WORKLOAD).json),--pgbouncer $(BENCH_DIR)/pgbouncer-$(BENCH_WORKLOAD).json)

.PHONY: bench-table
bench-table: build-bench ## Render the stored reports as a markdown table, one column per arm.
	@bin/pgebench table --dir $(BENCH_DIR) --workload $(BENCH_WORKLOAD) \
		--metric $(BENCH_METRIC) --arms "$(subst $(BENCH_SPACE),$(BENCH_COMMA),$(strip $(BENCH_ARMS)))"

# One arm at a time, because two arms sharing the cores would measure the contention rather
# than either arm. Each arm waits for readiness - /readyz for the proxy, a real query for
# pgbouncer, which has no health endpoint - rather than sleeping a guessed interval.
#
# ARMS selects which to run, e.g. ARMS=pgbouncer make bench-arms.
BENCH_ARMS ?= direct rust rust-fence-on rust-session rust-1worker pgbouncer

BENCH_DRIFT_ARM ?= rust
BENCH_DRIFT_WORKLOAD ?= throughput
BENCH_DRIFT_METRIC ?= throughput

# bench-table wants the arm list comma-separated; the scripts want it space-separated.
BENCH_COMMA := ,
BENCH_EMPTY :=
BENCH_SPACE := $(BENCH_EMPTY) $(BENCH_EMPTY)

.PHONY: bench-arms
bench-arms: bench-stack-up docker-build-proxy ## Run the benchmark arms and write one report per arm and workload.
	ARMS="$(BENCH_ARMS)" BENCH_DIR="$(BENCH_DIR)" BENCH_DURATION="$(BENCH_DURATION)" \
		BENCH_WARMUP="$(BENCH_WARMUP)" BENCH_REPETITIONS="$(BENCH_REPETITIONS)" \
		BENCH_CONCURRENCY="$(BENCH_CONCURRENCY)" BENCH_RATE="$(BENCH_RATE)" \
		./test/bench/run-arms.sh

.PHONY: bench-drift
bench-drift: ## Ask whether repeating a measurement reproduces it. Needs 2+ runs of bench-arms.
	@go build -trimpath -o bin/pgebench ./test/bench/cmd/pgebench
	./bin/pgebench drift --dir "$(BENCH_DIR)/runs" --arm "$(BENCH_DRIFT_ARM)" \
		--workload "$(BENCH_DRIFT_WORKLOAD)" --metric "$(BENCH_DRIFT_METRIC)"

.PHONY: bench-proxy-down
bench-proxy-down: ## Stop the benchmark's poolers.
	-docker rm -f pgebench-proxy pgebench-pgbouncer

.PHONY: test-bench
test-bench: ## Run the harness's own tests. Needs a container runtime for the driver specs.
	go test ./internal/bench/...

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# The instance manager ships in its own image so an init container can copy it into the
# Postgres pod with nothing but cp, which is what lets the agent be upgraded without
# rebuilding and re-certifying the database image.
.PHONY: docker-build-instance
docker-build-instance: ## Build the instance manager image.
	$(CONTAINER_TOOL) build -f Dockerfile.instance -t $(INSTANCE_IMG) .

.PHONY: docker-build-postgres
docker-build-postgres: ## Build the pgelastic PostgreSQL 18 image.
	$(CONTAINER_TOOL) build -f Dockerfile.postgres -t $(PG_IMG) .

# The proxy is the only component on the client's data path, so its image carries the binary
# and nothing that could be executed instead of it.
.PHONY: docker-build-proxy
docker-build-proxy: ## Build the inline proxy image.
	$(CONTAINER_TOOL) build -f Dockerfile.proxy -t $(PROXY_IMG) .

.PHONY: build-instance
build-instance: ## Build the instance manager binary.
	go build -o bin/pgelastic-instance ./cmd/instance

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name pgelastic-builder
	$(CONTAINER_TOOL) buildx use pgelastic-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm pgelastic-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
# Server-side apply is required, not preferred: client-side apply stores the whole object
# in the kubectl.kubernetes.io/last-applied-configuration annotation, and the PgElasticPool
# CRD is far past that annotation's 256KiB ceiling because it embeds a full PodSpec.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply --server-side --force-conflicts -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply --server-side --force-conflicts -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.12.2
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
