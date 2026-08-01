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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/retry"
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

// sizingClass is the development tier: three postmasters fit on one kind node, which is what
// this suite gets. It has to name a class that exists - an unknown one leaves the instance
// Pending with InvalidSpec and no Pods at all, and every spec here then waits out its timeout
// against an instance that was never going to start.
const sizingClass = "dev-1"

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

// One container, in one order, because this suite is one narrative: archive WAL, back the
// instance up, recover it to a moment, then recover a single tenant out of that.
//
// Ginkgo randomizes top-level containers against each other, so three sibling Describes did
// not run in the order they are written - and the two restore containers, which need the
// instance and the base backups the archiving container leaves behind, failed instantly with
// `pginstances "pg-archive" not found` on any seed that put them first. Ordered is what makes
// the dependency between them a fact rather than a hope, and the specs are grouped here
// rather than merged into one file so that each still reads as its own subject.
var _ = Describe("backup, restore and point-in-time recovery", Ordered, func() {
	archivingSpecs()
	instanceRestoreSpecs()
	tenantRestoreSpecs()
	scheduledBackupSpecs()
})

var _ = ReportAfterEach(func(report SpecReport) {
	if report.Failed() {
		suiteFailed.Store(true)
	}
})

var _ = AfterSuite(func() {
	// Before the manager stops: every PgRestore, PgTenant and PgInstance here carries a
	// finalizer that only its own reconciler removes.
	releaseNamespace()

	if cancelSuite != nil {
		cancelSuite()
	}
})

var suiteFailed atomic.Bool

// releaseNamespace deletes the namespace this suite works in and makes sure it really
// finishes terminating.
//
// Unlike every other suite here, this one works in a fixed namespace, and it creates the
// object store, its Secret and the credentials Secret with a bare Create. So a second run
// against the same cluster used to fail either on AlreadyExists or - once objects held
// finalizers nothing released - on "namespace is being terminated", a long way downstream of
// the cause. CI never sees either, because each job builds a fresh kind cluster.
//
// Deleting the namespace is the whole mechanism. The API server deletes its contents, and the
// one ordering that matters - an instance holds its drain finalizer until no tenant is bound
// to it - resolves itself, because the tenants are being deleted too.
//
// A failed run keeps its namespace: what it left behind is the evidence, and this is the only
// place that knows the run failed.
func releaseNamespace() {
	if k8sClient == nil {
		return
	}
	if suiteFailed.Load() {
		GinkgoWriter.Printf("keeping namespace %s: the run failed and its objects are why\n",
			archiveNamespace)
		return
	}
	if err := client.IgnoreNotFound(k8sClient.Delete(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: archiveNamespace},
	})); err != nil {
		GinkgoWriter.Printf("could not delete namespace %s: %v\n", archiveNamespace, err)
		return
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		if apierrors.IsNotFound(k8sClient.Get(suiteCtx,
			client.ObjectKey{Name: archiveNamespace}, &corev1.Namespace{})) {
			return
		}
		if !time.Now().Before(deadline) {
			forceRelease(archiveNamespace)
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// forceRelease strips the finalizers off whatever is keeping the namespace from terminating.
// Only defensible because this suite created that namespace itself, and scoped to it for the
// same reason: the cluster is shared with whatever else is running on it.
//
// Nothing here fails the run. A teardown that goes red says nothing about the code under test
// while hiding the result that does, so it reports what it forced and leaves the verdict to
// the specs.
func forceRelease(namespace string) {
	lists := []client.ObjectList{
		&pgelasticv1alpha1.PgRestoreList{},
		&pgelasticv1alpha1.PgTenantList{},
		&pgelasticv1alpha1.PgInstanceList{},
	}
	for _, list := range lists {
		if err := k8sClient.List(suiteCtx, list, client.InNamespace(namespace)); err != nil {
			continue
		}
		held, err := apimeta.ExtractList(list)
		if err != nil {
			continue
		}
		for _, item := range held {
			object, ok := item.(client.Object)
			if !ok || len(object.GetFinalizers()) == 0 {
				continue
			}
			releaseOne(object)
		}
	}
}

func releaseOne(object client.Object) {
	key := client.ObjectKeyFromObject(object)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := k8sClient.Get(suiteCtx, key, object); err != nil {
			return err
		}
		object.SetFinalizers(nil)
		return k8sClient.Update(suiteCtx, object)
	})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		GinkgoWriter.Printf("could not release %s, so its namespace stays Terminating: %v\n",
			key, err)
	default:
		GinkgoWriter.Printf("released %s by force: no reconciler removed its finalizer\n", key)
	}
}

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
