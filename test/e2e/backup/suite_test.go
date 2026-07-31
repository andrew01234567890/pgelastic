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

// Package backup holds the end-to-end proof that WAL and base backups actually reach an
// object store.
//
// It is gated behind the e2e build tag and runs against a real kind cluster with a real
// S3-compatible store, because the claim it exists to check cannot be made anywhere else.
// A unit test can assert that the right command line was built; only this can assert that
// the bytes arrived, that they can be read back out again, and that what the API says about
// a backup matches what the repository holds - which is the only property a backup has that
// anybody cares about.
package backup

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

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// suiteControllerName is this suite's identity, deliberately not the default one a deployed
// operator carries, so that an operator already running on the cluster resolves this
// suite's objects to a controller that is not itself and leaves them alone.
const suiteControllerName = "pgelastic.io/e2e-backup-controller"

// claimClassName and claimPoolName are the route this suite's instances take to a class.
// An instance inherits ownership from its pool and from nothing else.
const (
	claimClassName = "e2e-backup-class"
	claimPoolName  = "e2e-backup-pool"
)

// sizingClass is the smallest that fits a kind node.
const sizingClass = "small"

func claimNamespace(namespace string) {
	GinkgoHelper()
	elasticClass := &pgelasticv1alpha1.PgElasticClass{
		ObjectMeta: metav1.ObjectMeta{Name: claimClassName},
		Spec:       pgelasticv1alpha1.PgElasticClassSpec{ControllerName: suiteControllerName},
	}
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, elasticClass))).To(Succeed())

	pool := &pgelasticv1alpha1.PgElasticPool{
		ObjectMeta: metav1.ObjectMeta{Name: claimPoolName, Namespace: namespace},
		Spec: pgelasticv1alpha1.PgElasticPoolSpec{
			ClassRef: pgelasticv1alpha1.ClassReference{
				APIGroup: pgelasticv1alpha1.SchemeGroupVersion.Group,
				Kind:     "PgElasticClass",
				Name:     claimClassName,
			},
			Capacity: pgelasticv1alpha1.PoolCapacity{BackendConnections: 100},
			Instances: pgelasticv1alpha1.PoolInstances{
				Template: pgelasticv1alpha1.PgInstanceTemplate{
					Class: sizingClass,
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      resource.MustParse("1Gi"),
						WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
					},
				},
			},
		},
	}
	Eventually(func() error {
		return client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, pool))
	}, "5m", "3s").Should(Succeed())
}

// kubectlProber asks a member to describe itself by running the instance manager's own
// status subcommand inside its Pod. A kind node's Pod CIDR is not routable from the machine
// this suite runs on, so the transport differs from a deployed operator's and nothing else
// does: the report is the one the HTTP prober would have fetched.
type kubectlProber struct{}

func (kubectlProber) Probe(
	ctx context.Context,
	member, _ string,
) (provision.MemberReport, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	command := exec.CommandContext(probeCtx, "kubectl", kubectlArgs(
		"exec", "-n", probeNamespace.Load(), member, "-c", "postgres", "--",
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

// kubectlArgs names the cluster under test on every kubectl invocation, because the suite's
// own client is built from E2E_CONTEXT and a kubectl left to pick the kubeconfig's current
// context would read a different cluster while the specs still passed.
func kubectlArgs(args ...string) []string {
	if kubeContext := os.Getenv("E2E_CONTEXT"); kubeContext != "" {
		return append([]string{"--context=" + kubeContext}, args...)
	}
	return args
}

func kubectlCommand(args ...string) *exec.Cmd {
	return exec.Command("kubectl", kubectlArgs(args...)...)
}

var probeNamespace atomicString

var (
	suiteCtx    context.Context
	cancelSuite context.CancelFunc

	k8sClient client.Client

	postgresImage string
	agentImage    string
)

func TestBackupE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "WAL archiving, physical backup and point-in-time restore e2e")
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

	// The instance reconciler lists an instance's backups through a field index, so the
	// index has to exist before it runs.
	Expect(index.Setup(suiteCtx, manager.GetFieldIndexer())).To(Succeed())

	Expect((&controller.PgInstanceReconciler{
		Client:        manager.GetClient(),
		Scheme:        manager.GetScheme(),
		PostgresImage: postgresImage,
		AgentImage:    agentImage,
		// A single-node kind cluster cannot honour node anti-affinity, and what this suite
		// proves is archiving rather than placement.
		AntiAffinity:   provision.AntiAffinityPreferred,
		PeerSources:    []string{"all"},
		ProxySources:   []string{"all"},
		Prober:         kubectlProber{},
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	Expect((&controller.PgBackupReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	// A tenant restore copies through the same pods/exec transport a migration does, which
	// is the only route to a database whose superuser has no password at all.
	executor, err := migration.NewKubeExec(config)
	Expect(err).NotTo(HaveOccurred())
	restoreSQL := migration.PodSQL{
		Runner:  executor,
		Members: migration.PrimaryResolver{Client: manager.GetAPIReader()},
	}
	Expect((&controller.PgRestoreReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		SQL:            restoreSQL,
		Shell:          restoreSQL,
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(manager.Start(suiteCtx)).To(Succeed())
	}()

	Expect(manager.GetCache().WaitForCacheSync(suiteCtx)).To(BeTrue())
	// The specs read through a client of their own, straight to the API server, rather than
	// through the manager's cache: a spec sharing it would be asserting on what the operator
	// currently believes.
	k8sClient, err = client.New(config, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	SetDefaultEventuallyTimeout(10 * time.Minute)
	SetDefaultEventuallyPollingInterval(3 * time.Second)
})

var _ = AfterSuite(func() {
	if cancelSuite != nil {
		cancelSuite()
	}
})

// atomicString is written by a spec's setup and read by the prober goroutines the manager
// runs.
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
