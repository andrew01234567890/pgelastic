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

// Package restart holds the end-to-end proof that a three-node PostgreSQL 18 instance can
// be restarted, member by member, without any client on it seeing an error.
//
// It runs against a real cluster because every claim it makes exists only inside
// PostgreSQL and the kubelet: that each member came back on the new max_connections, that
// no more than one member was ever down, that the role moved off the primary before its
// Pod was recreated, and that a client holding one socket through the pool's Service
// across all of it was queued rather than dropped.
package restart

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
// objects to a controller that is not itself and leaves them alone - which is what lets
// this suite run against a cluster that is already serving pgelastic, and what stops the
// two of them rewriting the proxy Deployment under each other forever.
//
// It is a constant rather than an environment override because the deployed operator reads
// its own identity from the environment: a shared variable would put both back on the same
// name, which is the state this exists to leave.
const suiteControllerName = "pgelastic.io/e2e-restart-controller"

var (
	suiteCtx    context.Context
	cancelSuite context.CancelFunc

	// k8sClient reads straight from the API server rather than through the operator's
	// informer cache. A spec sharing the cache would be asserting on what the operator
	// currently believes, which is the thing under suspicion.
	k8sClient client.Client

	// sql runs psql inside a member's own container, over the socket the bootstrap
	// superuser is reachable on. It is how every claim about PostgreSQL in this suite is
	// answered by PostgreSQL.
	sql migration.PodSQL

	// runner is the exec transport, reused to ask one named member rather than whichever
	// member holds the role.
	runner migration.PodExec

	restConfig *rest.Config
	clientSet  *kubernetes.Clientset

	// controlEndpoints is how the suite's operator reaches each proxy replica's control
	// API. It outlives every spec, so it is closed in AfterSuite.
	controlEndpoints = &forwardedEndpoints{}
)

func TestRestartE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PgInstance rolling restart e2e")
}

// execProber asks a member to describe itself by running the instance manager's own status
// subcommand inside its Pod. The report is byte for byte the one the HTTP prober would
// fetch; only the transport differs, because a cluster's Pod CIDR is not routable from the
// machine this suite runs on.
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

// namedMember resolves every statement to one member, whichever role it happens to hold.
// The suite needs it because the questions it asks - what max_connections did you load, are
// you in recovery - are per member, and the production resolver answers with the primary.
type namedMember string

func (m namedMember) Resolve(context.Context, migration.Endpoint) (string, error) {
	return string(m), nil
}

func memberSQL(member string) migration.PodSQL {
	return migration.PodSQL{Runner: runner, Members: namedMember(member)}
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

	// The cache is scoped to this suite's own namespace, so an operator started for this
	// test cannot reconcile objects another suite is driving on the same cluster. Nodes are
	// cluster-scoped and are cached whole; the roll reads them to answer whether the
	// primary's node is going away.
	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{e2eNamespace: {}},
		},
	})
	Expect(err).NotTo(HaveOccurred())

	runner, err = migration.NewKubeExec(config)
	Expect(err).NotTo(HaveOccurred())
	sql = migration.PodSQL{
		Runner:  runner,
		Members: migration.PrimaryResolver{Client: manager.GetAPIReader()},
	}

	// The router the deployed operator runs, wired to the same control API and differing
	// only in how a replica is addressed from outside the cluster. Without it the roll would
	// hand the role over with nobody holding the clients, which is the thing this suite
	// exists to tell apart from the thing that works.
	router := &proxyobjects.ProxyRouter{
		Binding:   migration.BindingRouter{Client: manager.GetClient(), Reader: manager.GetAPIReader()},
		Reader:    manager.GetAPIReader(),
		Endpoints: controlEndpoints,
		Caller:    &proxyobjects.MutualTLSCaller{Reader: manager.GetAPIReader()},
	}

	Expect((&controller.PgInstanceReconciler{
		Client:        manager.GetClient(),
		Scheme:        manager.GetScheme(),
		PostgresImage: envOr("PGELASTIC_POSTGRES_IMG", "pgelastic/postgres:18"),
		AgentImage:    envOr("PGELASTIC_INSTANCE_IMG", "pgelastic/instance:latest"),
		// A single-node cluster cannot honour node anti-affinity, and what this suite proves
		// is an ordered restart rather than placement.
		AntiAffinity:   provision.AntiAffinityPreferred,
		PeerSources:    []string{"all"},
		ProxySources:   []string{"all"},
		Prober:         execProber{runner: runner},
		Quiescer:       router,
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	// The pool controller is what turns spec.proxy into a running fleet, and the claim this
	// suite exists to check is about clients queued at that fleet.
	Expect((&controller.PgElasticPoolReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		Metering:       metering.NewCollector(metering.Options{}, nil),
		ProxyImage:     envOr("PGELASTIC_PROXY_IMG", "pgelastic/proxy:latest"),
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(manager.Start(suiteCtx)).To(Succeed())
	}()
	Expect(manager.GetCache().WaitForCacheSync(suiteCtx)).To(BeTrue())

	k8sClient, err = client.New(config, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	SetDefaultEventuallyTimeout(15 * time.Minute)
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
