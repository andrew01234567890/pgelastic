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

// Package migration holds the end-to-end proof that a tenant can be moved between two real
// three-node PostgreSQL 18 instances, both by logical replication and by dump and restore,
// with the verifier passing, every abort leaving the tenant on the source, and the pause
// actually measured rather than asserted.
//
// It runs against a real cluster because everything it proves - a failover-enabled slot, an
// initial table sync, a confirmed_flush_lsn catching up, a sequence carried across, an
// abandoned slot bounded by max_slot_wal_keep_size - exists only inside PostgreSQL.
package migration

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes"
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
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/metering"
	"github.com/andrew01234567890/pgelastic/internal/migration"
	proxyobjects "github.com/andrew01234567890/pgelastic/internal/proxy"
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
const suiteControllerName = "pgelastic.io/e2e-migration-controller"

var (
	suiteCtx    context.Context
	cancelSuite context.CancelFunc

	// k8sClient reads straight from the API server rather than through the operator's
	// informer cache. A spec sharing the cache would be asserting on what the operator
	// currently believes, and would fail outright on a create it has not seen yet.
	k8sClient client.Client

	// sql is the same port the migration controller acts through, reused by the specs to
	// set up tenant data and to ask PostgreSQL what it actually holds.
	sql migration.PodSQL

	sweeper *controller.MigrationSweeper

	// restConfig and clientSet are what the in-process port-forwards are built on. A Pod
	// CIDR is not routable from the machine this suite runs on, and both the tenant's own
	// client path and the operator's control calls have to cross that gap.
	restConfig *rest.Config
	clientSet  *kubernetes.Clientset

	// controlEndpoints is how the suite's operator reaches each replica's cutover API. It
	// outlives every spec, so it is closed in AfterSuite rather than by DeferCleanup.
	controlEndpoints = &forwardedEndpoints{}
)

func TestMigrationE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PgTenantMigration e2e")
}

// execProber asks a member to describe itself by running the instance manager's own status
// subcommand inside its Pod, over the API server's exec subresource. The report is byte for
// byte the one the HTTP prober would fetch; only the transport differs, because a cluster's
// Pod CIDR is not routable from the machine this suite runs on.
type execProber struct {
	runner migration.PodExec
}

func (p execProber) Probe(ctx context.Context, member, _ string) (provision.MemberReport, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	output, err := p.runner.Exec(probeCtx, e2eNamespace, member, migration.PostgresContainer,
		[]string{provision.AgentBinary, "status"}, "")
	if err != nil {
		return provision.MemberReport{}, err
	}
	var report provision.MemberReport
	if err := json.Unmarshal(output, &report); err != nil {
		return provision.MemberReport{}, err
	}
	return report, nil
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	suiteCtx, cancelSuite = context.WithCancel(context.Background())

	Expect(pgelasticv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	config, err := ctrlconfig.GetConfigWithContext(os.Getenv("E2E_CONTEXT"))
	Expect(err).NotTo(HaveOccurred(), "the cluster named by E2E_CONTEXT has to be reachable")
	restConfig = config
	clientSet, err = kubernetes.NewForConfig(config)
	Expect(err).NotTo(HaveOccurred())

	// The cache is scoped to this suite's own namespace. An operator started for a test must
	// not reconcile objects a different suite is driving on the same cluster: two managers
	// writing the same PgInstance status is not a scenario either of them is testing.
	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{e2eNamespace: {}},
		},
	})
	Expect(err).NotTo(HaveOccurred())

	runner, err := migration.NewKubeExec(config)
	Expect(err).NotTo(HaveOccurred())
	sql = migration.PodSQL{
		Runner:  runner,
		Members: migration.PrimaryResolver{Client: manager.GetAPIReader()},
	}

	Expect((&controller.PgInstanceReconciler{
		Client:        manager.GetClient(),
		Scheme:        manager.GetScheme(),
		PostgresImage: envOr("PGELASTIC_POSTGRES_IMG", "pgelastic/postgres:18"),
		AgentImage:    envOr("PGELASTIC_INSTANCE_IMG", "pgelastic/instance:latest"),
		// A single-node cluster cannot honour node anti-affinity, and what this suite proves
		// is a tenant move rather than placement.
		AntiAffinity:   provision.AntiAffinityPreferred,
		PeerSources:    []string{"all"},
		ProxySources:   []string{"all"},
		Prober:         execProber{runner: runner},
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	// The pool controller is here for one reason: it is what turns spec.proxy into a running
	// fleet, and the claim this suite exists to check is about clients queued at that fleet.
	// It creates no PgInstance, so the two members the specs drive are still the suite's own.
	Expect((&controller.PgElasticPoolReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		Metering:       metering.NewCollector(metering.Options{}, nil),
		ProxyImage:     envOr("PGELASTIC_PROXY_IMG", "pgelastic/proxy:latest"),
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	// The router the deployed operator runs, wired to the same control API, differing only
	// in how a replica is addressed from outside the cluster.
	Expect((&controller.PgTenantMigrationReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		SQL:            sql,
		Shell:          sql,
		ControllerName: suiteControllerName,
		Router: &proxyobjects.ProxyRouter{
			Binding:   migration.BindingRouter{Client: manager.GetClient(), Reader: manager.GetAPIReader()},
			Reader:    manager.GetAPIReader(),
			Endpoints: controlEndpoints,
			Caller:    &proxyobjects.MutualTLSCaller{Reader: manager.GetAPIReader()},
		},
	}).SetupWithManager(manager)).To(Succeed())

	sweeper = &controller.MigrationSweeper{
		Client: manager.GetClient(), SQL: sql, ControllerName: suiteControllerName}

	go func() {
		defer GinkgoRecover()
		Expect(manager.Start(suiteCtx)).To(Succeed())
	}()
	Expect(manager.GetCache().WaitForCacheSync(suiteCtx)).To(BeTrue())

	k8sClient, err = client.New(config, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	SetDefaultEventuallyTimeout(10 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)
})

var _ = AfterSuite(func() {
	controlEndpoints.closeAll()
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
