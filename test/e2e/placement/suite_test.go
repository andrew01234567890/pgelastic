//go:build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package placement is the end-to-end proof that the placement and capacity-planning loops
// behave against a real API server: real CRD schemas, real pruning, real status subresources
// and the real controllers running inside a manager.
//
// The placement specs deliberately provision no PostgreSQL. Placement is a control-plane
// decision, and what it consumes from an instance is exactly what the instance publishes in
// status — the allocatable connection count, the storage figures and the readiness.
// Standing up nine postmasters to hand those numbers over would prove nothing they do not.
// What they do prove, and what envtest cannot, is that the plan the controller computes
// survives a real CRD: a status field that fails validation or is pruned away looks
// identical to a controller that chose not to write it.
//
// The tenant-provisioning specs carry the `postgres` label and do stand up a real instance,
// because the only trustworthy answer to "does this tenant's database exist" comes from
// pg_database on the instance hosting it. A PgTenant once reported Ready for a database that
// was never created; the CR is exactly the witness that cannot be believed here.
package placement

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/controller"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// suiteControllerName is this suite's identity, deliberately not the default one a
// deployed operator carries. Every object the suite creates is governed by a
// PgElasticClass naming it, so an operator already running on the cluster resolves those
// objects to a controller that is not itself and leaves them alone, instead of rewriting
// them under the suite for as long as both are running.
//
// It is a constant rather than an environment override because the deployed operator reads
// its own identity from the environment: a shared variable would put both back on the same
// name, which is the state this exists to leave.
const suiteControllerName = "pgelastic.io/e2e-placement-controller"

var (
	suiteCtx    context.Context
	cancelSuite context.CancelFunc

	// k8sClient reads and writes straight to the API server. The manager's own client is
	// served from the operator's informer cache, so a spec sharing it would be asserting on
	// what the operator currently believes and would fail outright on a create it has not
	// seen the watch event for yet.
	k8sClient client.Client

	collector *metering.Collector

	// provisioningNamespace holds the one real instance this suite stands up. It is
	// separate from the namespace the placement specs use because those specs write
	// instance status by hand, and an instance controller reconciling them would replace
	// every number they depend on with a real one.
	provisioningNamespace string
)

func TestPlacementE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Placement and capacity planning e2e")
}

// kubectlArgs names the cluster under test on every kubectl invocation.
//
// The suite's own client is built from E2E_CONTEXT, so a kubectl left to pick the
// kubeconfig's current context reads a different cluster from the one being driven. The
// failure that produces is silent and misleading: the specs still run, and every question
// they ask is answered by whatever happens to be current, or by nothing at all.
func kubectlArgs(args ...string) []string {
	if kubeContext := os.Getenv("E2E_CONTEXT"); kubeContext != "" {
		return append([]string{"--context=" + kubeContext}, args...)
	}
	return args
}

// kubectlCommand is kubectlArgs plus the exec.Cmd.
func kubectlCommand(args ...string) *exec.Cmd {
	return exec.Command("kubectl", kubectlArgs(args...)...)
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	suiteCtx, cancelSuite = context.WithCancel(context.Background())

	Expect(pgelasticv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	config, err := ctrlconfig.GetConfigWithContext(os.Getenv("E2E_CONTEXT"))
	Expect(err).NotTo(HaveOccurred(),
		"no reachable cluster for E2E_CONTEXT=%q; this suite fails rather than skipping",
		os.Getenv("E2E_CONTEXT"))

	// A cluster that answers but has no pgelastic CRDs installed would fail every spec with
	// an unhelpful "no matches for kind" much later, so it is checked here.
	version, err := kubectlCommand("get", "crd", "pgelasticpools.pgelastic.io", "-o", "name").CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "pgelastic CRDs are not installed: %s", version)

	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(index.Setup(suiteCtx, manager.GetFieldIndexer())).To(Succeed())

	// One collector, exactly as the operator binary wires it: the percentile a tenant is
	// placed on and the percentile the plan packs on have to be the same number.
	collector = metering.NewCollector(metering.Options{}, nil)

	// The same transport the operator binary gives the tenant controller: the bootstrap
	// superuser over the hosting member's Unix socket, through the API server's exec
	// subresource. Handing the suite a stub here would put the one thing under test — that
	// a database is really created — back inside the test.
	execer, err := migration.NewKubeExec(config)
	Expect(err).NotTo(HaveOccurred())
	tenantSQL := migration.PodSQL{
		Runner:  execer,
		Members: migration.PrimaryResolver{Client: manager.GetAPIReader()},
	}

	Expect((&controller.PgTenantReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		Metering:       collector,
		SQL:            tenantSQL,
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	Expect((&controller.PgElasticPoolReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		Metering:       collector,
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(manager.Start(suiteCtx)).To(Succeed())
	}()
	Expect(manager.GetCache().WaitForCacheSync(suiteCtx)).To(BeTrue())

	k8sClient, err = client.New(config, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	startInstanceManager(config)

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)
})

// startInstanceManager runs a PgInstance controller over one namespace only.
//
// The scoping is the point. The placement specs publish instance status by hand, and a
// controller that reconciled those instances would build Pods for them and overwrite the
// very numbers those specs feed to placement.
func startInstanceManager(config *rest.Config) {
	GinkgoHelper()
	provisioningNamespace = uniqueNamespace("pgelastic-tenantdb")

	instanceManager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{provisioningNamespace: {}},
		},
	})
	Expect(err).NotTo(HaveOccurred())

	Expect((&controller.PgInstanceReconciler{
		Client:        instanceManager.GetClient(),
		Scheme:        instanceManager.GetScheme(),
		PostgresImage: envOr("PGELASTIC_POSTGRES_IMG", "pgelastic/postgres:18"),
		AgentImage:    envOr("PGELASTIC_INSTANCE_IMG", "pgelastic/instance:latest"),
		// A single-node test cluster cannot honour node anti-affinity, and what this suite
		// proves is what is in pg_database rather than where the members landed.
		AntiAffinity: provision.AntiAffinityPreferred,
		PeerSources:  []string{"all"},
		ProxySources: []string{"all"},
		// The operator runs on the developer's machine here and a node's Pod CIDR is not
		// routable from it, so each member is asked the same question over the same status
		// endpoint through kubectl exec.
		Prober:         kubectlProber{},
		ControllerName: suiteControllerName,
	}).SetupWithManager(instanceManager)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(instanceManager.Start(suiteCtx)).To(Succeed())
	}()
	Expect(instanceManager.GetCache().WaitForCacheSync(suiteCtx)).To(BeTrue())
}

// kubectlProber asks a member to describe itself by running the instance manager's own
// status subcommand inside its Pod. The report is byte for byte the one the HTTP prober
// would fetch; only the transport differs.
type kubectlProber struct{}

func (kubectlProber) Probe(ctx context.Context, member, _ string) (provision.MemberReport, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	command := exec.CommandContext(probeCtx, "kubectl", kubectlArgs(
		"exec", "-n", provisioningNamespace, member, "-c", "postgres", "--",
		provision.AgentBinary, "status")...)
	output, err := command.Output()
	if err != nil {
		return provision.MemberReport{}, err
	}
	var report provision.MemberReport
	if err := json.Unmarshal(output, &report); err != nil {
		return provision.MemberReport{}, err
	}
	return report, nil
}

// psql runs one query on one member over its local Unix socket as the bootstrap superuser,
// which is the only route to that superuser: it has no password at all.
func psql(member, database, query string) (string, error) {
	output, err := kubectlCommand("exec", "-n", provisioningNamespace, member, "-c", "postgres", "--",
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-d", database, "-tAqc", query).CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

var _ = AfterSuite(func() {
	if cancelSuite != nil {
		cancelSuite()
	}
})

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func uniqueNamespace(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()%100000)
}
