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
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/controller"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

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

	config := ctrl.GetConfigOrDie()
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

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
