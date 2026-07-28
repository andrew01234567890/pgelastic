# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# INSTANCE_IMG carries the instance manager; PG_IMG carries PostgreSQL 18 and pgBackRest.
INSTANCE_IMG ?= pgelastic/instance:latest
PG_IMG ?= pgelastic/postgres:18
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
	$(MAKE) test-e2e-migration E2E_CONTEXT=kind-$(KIND_CLUSTER)
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
# replication and by dump and restore, and measures the pause rather than asserting it.
.PHONY: test-e2e-migration
test-e2e-migration: manifests generate install-e2e load-instance-images ## Move a tenant between two real PostgreSQL 18 instances.
	PGELASTIC_POSTGRES_IMG=$(PG_IMG) PGELASTIC_INSTANCE_IMG=$(INSTANCE_IMG) E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/migration/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		$(if $(E2E_LABEL_FILTER),-ginkgo.label-filter='$(E2E_LABEL_FILTER)')

# The placement e2e drives the placement and capacity-planning loops against a real API
# server. It provisions no PostgreSQL: what placement consumes from a member instance is
# exactly what that instance publishes in status, and the provisioning path has its own
# suite. What this one proves that envtest cannot is that the whole plan survives a real
# CRD, where a pruned status field is indistinguishable from a decision not to write one.
.PHONY: test-e2e-placement
test-e2e-placement: manifests generate install-e2e ## Place a tenant population across a pool and assert the plan.
	E2E_CONTEXT=$(E2E_CONTEXT) \
	go test -tags=e2e ./test/e2e/placement/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
		$(if $(E2E_LABEL_FILTER),-ginkgo.label-filter='$(E2E_LABEL_FILTER)')

.PHONY: install-e2e
install-e2e: manifests kustomize ## Install CRDs into E2E_CONTEXT.
	"$(KUSTOMIZE)" build config/crd | "$(KUBECTL)" --context=$(E2E_CONTEXT) apply --server-side --force-conflicts -f -

.PHONY: load-instance-images
load-instance-images: docker-build-instance docker-build-postgres ## Make the images reachable from E2E_CONTEXT.
	@case "$(E2E_CONTEXT)" in \
	  kind-*) "$(KIND)" load docker-image $(INSTANCE_IMG) --name $${E2E_CONTEXT#kind-}; \
	          "$(KIND)" load docker-image $(PG_IMG) --name $${E2E_CONTEXT#kind-} ;; \
	  *) echo "$(E2E_CONTEXT) shares the local Docker daemon; images already reachable" ;; \
	esac

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

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
