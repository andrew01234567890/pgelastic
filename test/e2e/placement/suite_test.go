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
// It deliberately does not provision PostgreSQL. Placement is a control-plane decision, and
// what it consumes from an instance is exactly what the instance publishes in status — the
// allocatable connection count, the storage figures and the readiness. Standing up nine
// postmasters to hand those numbers over would prove nothing this suite does not, and the
// provisioning path already has its own suite that proves them against a real postmaster.
//
// What this suite does prove, and what envtest cannot, is that the plan the controller
// computes survives a real CRD: a status field that fails validation or is pruned away
// looks identical to a controller that chose not to write it.
package placement

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/controller"
	"github.com/andrew01234567890/pgelastic/internal/index"
	"github.com/andrew01234567890/pgelastic/internal/metering"
)

var (
	suiteCtx    context.Context
	cancelSuite context.CancelFunc

	// k8sClient reads and writes straight to the API server. The manager's own client is
	// served from the operator's informer cache, so a spec sharing it would be asserting on
	// what the operator currently believes and would fail outright on a create it has not
	// seen the watch event for yet.
	k8sClient client.Client

	collector *metering.Collector
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

	Expect((&controller.PgTenantReconciler{
		Client:   manager.GetClient(),
		Scheme:   manager.GetScheme(),
		Metering: collector,
	}).SetupWithManager(manager)).To(Succeed())

	Expect((&controller.PgElasticPoolReconciler{
		Client:   manager.GetClient(),
		Scheme:   manager.GetScheme(),
		Metering: collector,
	}).SetupWithManager(manager)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(manager.Start(suiteCtx)).To(Succeed())
	}()
	Expect(manager.GetCache().WaitForCacheSync(suiteCtx)).To(BeTrue())

	k8sClient, err = client.New(config, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)
})

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
