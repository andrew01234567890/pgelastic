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

package restart

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"

	proxyobjects "github.com/andrew01234567890/pgelastic/internal/proxy"
)

// forwarder is a port-forward onto one Pod, run in-process.
//
// In-process on client-go's SPDY forwarder rather than as a kubectl subprocess: a
// subprocess inherits whichever file descriptors Ginkgo has spliced onto stdout and stderr
// for the running spec, and a forward that has to outlive the spec that opened it does not
// survive that.
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

func (f *forwarder) log() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.journal.String()
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

func forwardPod(pod string, port int32) (*forwarder, error) {
	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	if err != nil {
		return nil, err
	}
	url := clientSet.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(e2eNamespace).Name(pod).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)

	forward := &forwarder{stop: make(chan struct{}), done: make(chan struct{})}
	ready := make(chan struct{})
	pf, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"},
		[]string{fmt.Sprintf("0:%d", port)}, forward.stop, ready, forward, forward)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(forward.done)
		if err := pf.ForwardPorts(); err != nil {
			_, _ = fmt.Fprintf(forward, "port-forward to %s ended: %v\n", pod, err)
		}
	}()
	select {
	case <-ready:
	case <-time.After(time.Minute):
		forward.close()
		return nil, fmt.Errorf("the port-forward to %s never became ready: %s", pod, forward.log())
	}
	ports, err := pf.GetPorts()
	if err != nil {
		return nil, err
	}
	forward.address = fmt.Sprintf("127.0.0.1:%d", ports[0].Local)
	return forward, nil
}

// readyProxyPods names every replica the pool's Service selects, in a stable order.
//
// Through the Service's own selector rather than by listing Pods with a label set of this
// suite's choosing: the selector is what decides which replicas serve the pool, so an
// operator that got it wrong fails here instead of being quietly worked around.
func readyProxyPods() []string {
	service := &corev1.Service{}
	key := client.ObjectKey{Namespace: e2eNamespace, Name: proxyobjects.ServiceName(poolName)}
	if err := k8sClient.Get(suiteCtx, key, service); err != nil {
		return nil
	}
	pods := &corev1.PodList{}
	if err := k8sClient.List(suiteCtx, pods, client.InNamespace(e2eNamespace),
		client.MatchingLabels(service.Spec.Selector)); err != nil {
		return nil
	}
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

// forwardedEndpoints is the ControlEndpoints the suite's own operator uses.
//
// The production resolver dials each replica on its Pod IP, and a Pod CIDR is not routable
// from the machine this suite runs on. Only the address changes: the request is the same
// mutual-TLS call to the same per-replica listener, and the certificate is still verified
// against the name the listener carries rather than the address it was reached on.
type forwardedEndpoints struct {
	mu       sync.Mutex
	forwards map[string]*forwarder
}

func (f *forwardedEndpoints) Endpoints(
	_ context.Context,
	_ client.ObjectKey,
) ([]proxyobjects.ControlEndpoint, error) {
	pods := readyProxyPods()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forwards == nil {
		f.forwards = map[string]*forwarder{}
	}

	endpoints := make([]proxyobjects.ControlEndpoint, 0, len(pods))
	for _, pod := range pods {
		forward, ok := f.forwards[pod]
		if !ok {
			opened, err := forwardPod(pod, proxyobjects.DefaultControlPort)
			if err != nil {
				return nil, fmt.Errorf("reaching the control listener on %s: %w", pod, err)
			}
			f.forwards[pod] = opened
			forward = opened
		}
		endpoints = append(endpoints, proxyobjects.ControlEndpoint{
			Pod: pod, BaseURL: "https://" + forward.address,
		})
	}
	// A replica that has gone away keeps a forward alive to a Pod that no longer exists.
	for pod, forward := range f.forwards {
		if !slices.Contains(pods, pod) {
			forward.close()
			delete(f.forwards, pod)
		}
	}
	return endpoints, nil
}

func (f *forwardedEndpoints) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, forward := range f.forwards {
		forward.close()
	}
	f.forwards = nil
}

// probe is a client that holds one connection open through the pool's Service and keeps
// asking it a question, recording what every answer cost and whether any of them failed.
//
// It is a byte-for-byte copy of the migration suite's, and deliberately so: the claim a
// rolling restart makes about clients is the same claim a cutover makes, so the assertion
// has to be literally the same one rather than a comparable one written again.
//
// It is the whole product claim reduced to something a machine can check. A queued client
// shows a latency spike; a dropped one shows an error. Nothing about the operator's own
// status is consulted, because the operator's account of its pause is exactly the thing
// under suspicion.
type probe struct {
	name     string
	forward  *forwarder
	cancel   context.CancelFunc
	finished chan struct{}
	opened   time.Time

	mu        sync.Mutex
	latencies []time.Duration
	failures  []probeFailure
	// servers is every distinct backend address this one socket was served by, in the order
	// they were first seen. A client that was queued and then released against another
	// instance answers from a different address on the same socket, which is the difference
	// between a move and a reconnection nobody noticed.
	servers []string
	// window records the latency of every statement between start and stop of a marked
	// window, which is how "during the cutover" is told apart from "before it".
	marked bool
	during []time.Duration
}

// probeFailure is one failed statement and how far into the probe's life it failed.
//
// The offset is what makes a wall of identical errors readable: a client that loses its
// connection fails identically for every statement it attempts afterwards, so the only
// facts that distinguish one report from another are when the first one happened and how
// many followed.
type probeFailure struct {
	at   time.Duration
	text string
}

// startProbe opens the connection and begins asking. The interval is short enough that a
// sub-second pause is still sampled several times either side of itself.
func startProbe(name, user, database string, interval time.Duration) *probe {
	GinkgoHelper()
	forward, err := forwardPod(serviceEndpointPod(), proxyobjects.DefaultClientPort)
	Expect(err).NotTo(HaveOccurred(), "forwarding the pool Service for %s", name)

	ctx, cancel := context.WithCancel(suiteCtx)
	p := &probe{
		name:     name,
		forward:  forward,
		cancel:   cancel,
		finished: make(chan struct{}),
		opened:   time.Now(),
	}

	var connection *pgx.Conn
	Eventually(func() error {
		opened, err := pgx.Connect(ctx, forward.dsn(user, database))
		if err != nil {
			return err
		}
		connection = opened
		return nil
	}, "2m", "2s").Should(Succeed(), "%s never reached %s through the pool Service; the forward said:\n%s",
		name, database, forward.log())

	go func() {
		defer close(p.finished)
		defer func() { _ = connection.Close(context.WithoutCancel(ctx)) }()
		for ctx.Err() == nil {
			started := time.Now()
			var server string
			err := connection.QueryRow(ctx, servedByQuery).Scan(&server)
			elapsed := time.Since(started)
			if ctx.Err() != nil {
				return
			}
			p.record(elapsed, server, err)
			time.Sleep(interval)
		}
	}()
	return p
}

// servedByQuery asks the backend which address answered. It is the one question whose
// answer changes when a tenant moves and does not change when it does not.
const servedByQuery = `SELECT coalesce(host(inet_server_addr()), 'socket')`

func (p *probe) record(elapsed time.Duration, server string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latencies = append(p.latencies, elapsed)
	if err == nil && !slices.Contains(p.servers, server) {
		p.servers = append(p.servers, server)
	}
	if p.marked {
		p.during = append(p.during, elapsed)
	}
	if err != nil {
		p.failures = append(p.failures,
			probeFailure{at: time.Since(p.opened), text: err.Error()})
	}
}

// mark opens the window the cutover happens inside, so the latencies during it can be
// compared with the ones before it rather than with an assertion picked by hand.
func (p *probe) mark() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.marked = true
}

func (p *probe) stop() {
	p.cancel()
	<-p.finished
	p.forward.close()
}

func (p *probe) report() probeReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	before := p.latencies[:len(p.latencies)-len(p.during)]
	return probeReport{
		name:        p.name,
		servers:     append([]string(nil), p.servers...),
		statements:  len(p.latencies),
		failures:    append([]probeFailure(nil), p.failures...),
		maxOverall:  maxOf(p.latencies),
		beforeP50:   percentile(before, 50),
		beforeP99:   percentile(before, 99),
		duringP50:   percentile(p.during, 50),
		duringP99:   percentile(p.during, 99),
		duringMax:   maxOf(p.during),
		duringCount: len(p.during),
		// Carried alongside the failures because the two are only meaningful together: a
		// port-forward that died takes the connection with it, and the client cannot tell
		// that apart from the proxy dropping it. The journal is the only place that says
		// which one happened.
		forwardLog: p.forward.log(),
	}
}

type probeReport struct {
	name        string
	servers     []string
	statements  int
	failures    []probeFailure
	maxOverall  time.Duration
	beforeP50   time.Duration
	beforeP99   time.Duration
	duringP50   time.Duration
	duringP99   time.Duration
	duringMax   time.Duration
	duringCount int
	forwardLog  string
}

func (r probeReport) String() string {
	return fmt.Sprintf(
		"%s: %d statements, %d errors, served by %v; before p50=%s p99=%s; "+
			"during p50=%s p99=%s max=%s (%d samples)",
		r.name, r.statements, len(r.failures), r.servers, r.beforeP50, r.beforeP99,
		r.duringP50, r.duringP99, r.duringMax, r.duringCount)
}

// failureSummary collapses the failures into one line per distinct error.
//
// Written this way because the unabridged list cost a red run its evidence: a probe that
// loses its connection reports the same error for every one of the thousands of statements
// it attempts afterwards, and a Ginkgo failure message that long is dropped whole by the CI
// log rather than truncated. The count and the first offset carry everything the repetition
// carried, in a few hundred bytes.
func (r probeReport) failureSummary() string {
	if len(r.failures) == 0 {
		return "no statement failed"
	}
	distinct := make([]string, 0, 4)
	counts := map[string]int{}
	first := map[string]time.Duration{}
	for _, failure := range r.failures {
		if _, seen := counts[failure.text]; !seen {
			distinct = append(distinct, failure.text)
			first[failure.text] = failure.at
		}
		counts[failure.text]++
	}
	lines := make([]string, 0, len(distinct))
	for _, text := range distinct {
		lines = append(lines, fmt.Sprintf("  %d statements, from %s into the probe: %s",
			counts[text], first[text].Round(time.Millisecond), text))
	}
	return strings.Join(lines, "\n")
}

func maxOf(samples []time.Duration) time.Duration {
	var highest time.Duration
	for _, sample := range samples {
		if sample > highest {
			highest = sample
		}
	}
	return highest
}

func percentile(samples []time.Duration, which int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	slices.Sort(sorted)
	index := (which * len(sorted)) / 100
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// serviceEndpointPod names one Pod the pool's Service actually selects.
func serviceEndpointPod() string {
	GinkgoHelper()
	pods := readyProxyPods()
	Expect(pods).NotTo(BeEmpty(), "the pool Service selects no ready replica")
	return pods[0]
}
