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

// Package proxy holds the end-to-end proof that the two halves of pgelastic are joined: that
// the operator turns spec.proxy into a running fleet, and that a client connecting to the
// pool's Service reaches its own tenant's database on whichever instance currently holds it.
//
// Every claim here is answered by something outside the operator. Whether a client reached
// its database is answered by PostgreSQL's current_database(); whether transaction pooling is
// really happening is answered by pg_stat_activity on the instance; whether a configuration
// change reached the data plane is answered by the annotation each replica writes onto its
// own Pod. The operator's own status is asserted on separately, and only after the thing it
// claims has been established another way.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
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
const suiteControllerName = "pgelastic.io/e2e-proxy-controller"

var (
	suiteCtx    context.Context
	cancelSuite context.CancelFunc

	// k8sClient reads straight from the API server. The manager's client is served from the
	// operator's informer cache, so a spec sharing it would be asserting on what the operator
	// currently believes rather than on what exists.
	k8sClient client.Client

	// restConfig and clientSet are what the in-process port-forward is built on.
	restConfig *rest.Config
	clientSet  *kubernetes.Clientset

	// sql reaches a member over its Unix socket as the bootstrap superuser, which is the only
	// route to that superuser: it has no password at all. It is how the specs ask PostgreSQL
	// what actually happened, rather than asking the operator.
	sql migration.PodSQL
)

func TestProxyE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Inline proxy fleet e2e")
}

// kubectlArgs names the cluster under test on every kubectl invocation.
//
// The suite's own client is built from E2E_CONTEXT, so a kubectl left to pick the kubeconfig's
// current context reads a different cluster from the one being driven. The failure that
// produces is silent and misleading: the specs still run, and every question they ask is
// answered by whatever happens to be current, or by nothing at all.
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

// execProber asks a member to describe itself by running the instance manager's own status
// subcommand inside its Pod. The report is byte for byte the one the HTTP prober would fetch;
// only the transport differs, because a cluster's Pod CIDR is not routable from here.
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
	Expect(err).NotTo(HaveOccurred(),
		"no reachable cluster for E2E_CONTEXT=%q; this suite fails rather than skipping",
		os.Getenv("E2E_CONTEXT"))
	restConfig = config
	clientSet, err = kubernetes.NewForConfig(config)
	Expect(err).NotTo(HaveOccurred())

	// A cluster that answers but has no pgelastic CRDs installed would fail every spec much
	// later with an unhelpful "no matches for kind", so it is checked here.
	installed, err := kubectlCommand("get", "crd", "pgelasticpools.pgelastic.io", "-o", "name").
		CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "pgelastic CRDs are not installed: %s", installed)

	// The cache is scoped to this suite's namespace. An operator started for a test must not
	// reconcile objects another suite is driving on the same cluster.
	manager, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{e2eNamespace: {}},
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(index.Setup(suiteCtx, manager.GetFieldIndexer())).To(Succeed())

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
		// is routing rather than placement.
		AntiAffinity:   provision.AntiAffinityPreferred,
		PeerSources:    []string{"all"},
		ProxySources:   []string{"all"},
		Prober:         execProber{runner: runner},
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	collector := metering.NewCollector(metering.Options{}, nil)

	Expect((&controller.PgTenantReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		Metering:       collector,
		SQL:            sql,
		ControllerName: suiteControllerName,
	}).SetupWithManager(manager)).To(Succeed())

	Expect((&controller.PgElasticPoolReconciler{
		Client:         manager.GetClient(),
		Scheme:         manager.GetScheme(),
		Metering:       collector,
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

	SetDefaultEventuallyTimeout(10 * time.Minute)
	SetDefaultEventuallyPollingInterval(2 * time.Second)
})

var _ = AfterSuite(func() {
	if cancelSuite != nil {
		cancelSuite()
	}
})

// psql runs one query on one member over its local Unix socket as the bootstrap superuser.
// It is the suite's route to PostgreSQL's own answer, independent of the proxy under test.
func psql(member, database, query string) (string, error) {
	output, err := kubectlCommand("exec", "-n", e2eNamespace, member, "-c", "postgres", "--",
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-d", database,
		"-tAqc", query).CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

// forward opens a port-forward onto an endpoint of the pool's Service and returns the local
// address clients dial. Everything the specs send goes through it, so what they exercise is
// the fleet the operator created rather than a Pod they picked themselves.
//
// The endpoint is resolved through the Service's own selector rather than by guessing a Pod
// name, which is what `kubectl port-forward service/...` does and is the closest a machine
// outside the cluster can get to dialling a ClusterIP. It runs in-process on client-go's
// SPDY forwarder rather than as a kubectl subprocess: a subprocess inherits whichever file
// descriptors Ginkgo has spliced onto stdout and stderr for the running spec, and a forward
// that outlives the spec that started it does not survive that.
type forwarder struct {
	address string
	stop    chan struct{}
	done    chan struct{}

	mu      sync.Mutex
	journal bytes.Buffer
}

func (f *forwarder) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.journal.Write(p)
}

// log is everything the forward has said so far, for a failure message.
func (f *forwarder) log() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.journal.String()
}

func forward(service string, port int32) *forwarder {
	GinkgoHelper()
	return forwardPod(serviceEndpoint(service), port)
}

// forwardPod is forward aimed at one named replica rather than at whichever the Service
// picks. Two things need it: the control listener, which is per replica and deliberately
// absent from the Service, and any claim about the fleet as a whole, which cannot be made
// through a single endpoint because kube-proxy pins a connection to one pod for its life.
func forwardPod(pod string, port int32) *forwarder {
	GinkgoHelper()

	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	Expect(err).NotTo(HaveOccurred())
	url := clientSet.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(e2eNamespace).Name(pod).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)

	forwarder := &forwarder{stop: make(chan struct{}), done: make(chan struct{})}
	ready := make(chan struct{})
	pf, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"},
		[]string{fmt.Sprintf("0:%d", port)}, forwarder.stop, ready, forwarder, forwarder)
	Expect(err).NotTo(HaveOccurred())

	go func() {
		defer close(forwarder.done)
		if err := pf.ForwardPorts(); err != nil {
			_, _ = fmt.Fprintf(forwarder, "port-forward to %s ended: %v\n", pod, err)
		}
	}()

	select {
	case <-ready:
	case <-time.After(time.Minute):
		Fail("the port-forward to " + pod + " never became ready: " + forwarder.log())
	}
	ports, err := pf.GetPorts()
	Expect(err).NotTo(HaveOccurred())
	forwarder.address = fmt.Sprintf("127.0.0.1:%d", ports[0].Local)

	// Deliberately not DeferCleanup: registered inside an It, that runs when the It ends,
	// and every spec after it would then find nothing listening. The caller closes it from a
	// scope that outlives the specs.
	return forwarder
}

func (f *forwarder) close() {
	select {
	case <-f.stop:
	default:
		close(f.stop)
	}
	<-f.done
}

// dsn is a libpq connection string aimed at the forwarded endpoint.
func (f *forwarder) dsn(user, database string) string {
	host, port, _ := net.SplitHostPort(f.address)
	return fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		host, port, user, database)
}

// serviceEndpoint names one Pod the Service actually selects. Asking the Service rather than
// listing Pods by hand is what keeps the specs honest: if the operator got the selector
// wrong, this fails here rather than quietly forwarding to a Pod nothing routes to.
func serviceEndpoint(name string) string {
	GinkgoHelper()
	service := &corev1.Service{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: name}, service)).To(Succeed())
	Expect(service.Spec.Selector).NotTo(BeEmpty(), "the pool Service selects nothing")

	pods := &corev1.PodList{}
	Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(e2eNamespace),
		client.MatchingLabels(service.Spec.Selector))).To(Succeed())
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp == nil && podReady(pod) {
			return pod.Name
		}
	}
	Fail("the pool Service " + name + " selects no ready Pod")
	return ""
}

// readyProxyPods names every replica the Service selects, in a stable order, which is what
// a fleet-wide claim has to be measured across.
func readyProxyPods(service string) []string {
	GinkgoHelper()
	object := &corev1.Service{}
	Expect(k8sClient.Get(suiteCtx,
		client.ObjectKey{Namespace: e2eNamespace, Name: service}, object)).To(Succeed())
	Expect(object.Spec.Selector).NotTo(BeEmpty(), "the pool Service selects nothing")

	pods := &corev1.PodList{}
	Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(e2eNamespace),
		client.MatchingLabels(object.Spec.Selector))).To(Succeed())
	names := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp == nil && podReady(pod) {
			names = append(names, pod.Name)
		}
	}
	slices.Sort(names)
	return names
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
