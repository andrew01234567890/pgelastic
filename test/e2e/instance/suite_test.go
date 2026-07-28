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

// Package instance holds the end-to-end proof that pgelastic can actually provision a
// three-node PostgreSQL 18 instance: one primary, two streaming standbys, a recorded
// collation contract and a max_connections that matches the published derivation.
//
// It is gated behind the e2e build tag so `make test` stays fast, and it runs against a
// real kind cluster because the things it proves - initdb flags, pg_basebackup, streaming
// replication, the quorum clause PostgreSQL actually loaded - cannot be proven anywhere
// else.
package instance

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"sync/atomic"
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
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// kubectlProber asks a member to describe itself by running the instance manager's own
// status subcommand inside its Pod.
//
// The report is byte for byte the one the HTTP prober would fetch - the subcommand reads
// the same status server over the Pod's loopback address - so what is being tested is still
// the agent's answer and not a stub. Only the transport differs, because the Pod CIDR of a
// kind node is not reachable from the machine this suite runs on.
type kubectlProber struct{}

func (kubectlProber) Probe(
	ctx context.Context,
	member, _ string,
) (provision.MemberReport, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	namespace := probeNamespace.Load()
	args := []string{"exec", "-n", namespace, member, "-c", "postgres", "--",
		provision.AgentBinary, "status"}
	if kubeContext := os.Getenv("E2E_CONTEXT"); kubeContext != "" {
		args = append([]string{"--context=" + kubeContext}, args...)
	}
	command := exec.CommandContext(probeCtx, "kubectl", args...)
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

// probeNamespace is set by whichever suite is currently driving an instance. The operator
// under test reconciles one namespace at a time here, so a single value is enough and it
// keeps the prober from having to look a Pod up before it can talk to it.
var probeNamespace atomicString

var (
	suiteCtx    context.Context
	cancelSuite context.CancelFunc

	k8sClient client.Client

	postgresImage string
	agentImage    string
)

func TestInstanceE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "PgInstance provisioning e2e")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	suiteCtx, cancelSuite = context.WithCancel(context.Background())

	postgresImage = envOr("PGELASTIC_POSTGRES_IMG", "pgelastic/postgres:18")
	agentImage = envOr("PGELASTIC_INSTANCE_IMG", "pgelastic/instance:latest")

	Expect(pgelasticv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	config, err := ctrlconfig.GetConfigWithContext(os.Getenv("E2E_CONTEXT"))
	Expect(err).NotTo(HaveOccurred())
	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	Expect(err).NotTo(HaveOccurred())

	Expect((&controller.PgInstanceReconciler{
		Client:        manager.GetClient(),
		Scheme:        manager.GetScheme(),
		PostgresImage: postgresImage,
		AgentImage:    agentImage,
		// A single-node kind cluster cannot honour node anti-affinity, and the point of
		// this suite is replication rather than placement. A real deployment keeps the
		// Required policy, because two members on one node makes the quorum a lie.
		AntiAffinity: provision.AntiAffinityPreferred,
		PeerSources:  []string{"all"},
		// The chaos specs drive a durability oracle from inside the cluster over TCP, which
		// is the proxy's route in a real deployment. Admitting it from anywhere is a
		// concession to a test cluster with no proxy in it, not a default.
		ProxySources: []string{"all"},
		// This suite runs the operator on the developer's machine rather than in the
		// cluster, and a kind node's Pod CIDR is not routable from there. The prober asks
		// each member the same question over the same status endpoint, through kubectl exec
		// instead of a direct socket; an exec that fails is the same evidence a refused
		// connection would be. A deployed operator uses the direct HTTP prober.
		Prober: kubectlProber{},
	}).SetupWithManager(manager)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(manager.Start(suiteCtx)).To(Succeed())
	}()

	Expect(manager.GetCache().WaitForCacheSync(suiteCtx)).To(BeTrue())
	k8sClient = manager.GetClient()

	SetDefaultEventuallyTimeout(10 * time.Minute)
	SetDefaultEventuallyPollingInterval(3 * time.Second)
})

var _ = AfterSuite(func() {
	if cancelSuite != nil {
		cancelSuite()
	}
})

// atomicString is a string that is written by one suite's setup and read by the prober
// goroutines the manager runs.
type atomicString struct{ value atomic.Value }

func (s *atomicString) Store(value string) { s.value.Store(value) }

func (s *atomicString) Load() string {
	stored, _ := s.value.Load().(string)
	return stored
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
