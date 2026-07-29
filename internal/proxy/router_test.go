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

package proxy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

const (
	routerNamespace = "shop"
	routerPool      = "saas"
	routerTenant    = "acme"
	routerDatabase  = "acme_db"
	routerSource    = "pg-a"
	routerTarget    = "pg-b"
	routerHolder    = "shop/move-acme"
)

var acme = migration.TenantRef{Namespace: routerNamespace, Name: routerTenant}

// journal records what was called and in what order, which is the only way to assert an
// ordering contract without describing it in a comment and hoping.
type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(entry string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, entry)
}

func (j *journal) all() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

func (j *journal) has(entry string) bool {
	return slices.Contains(j.all(), entry)
}

func (j *journal) indexOf(entry string) int {
	for index, seen := range j.all() {
		if seen == entry {
			return index
		}
	}
	return -1
}

// bindingStub stands in for the routing table of record.
type bindingStub struct {
	journal *journal
	routed  string
}

func (b *bindingStub) Quiesce(context.Context, migration.TenantRef, string) error {
	b.journal.add("binding.Quiesce")
	return nil
}

func (b *bindingStub) PreWarm(context.Context, migration.TenantRef, string) error {
	b.journal.add("binding.PreWarm")
	return nil
}

func (b *bindingStub) Route(_ context.Context, _ migration.TenantRef, instance string) error {
	b.journal.add("binding.Route " + instance)
	b.routed = instance
	return nil
}

func (b *bindingStub) Resume(context.Context, migration.TenantRef) error {
	b.journal.add("binding.Resume")
	return nil
}

func (b *bindingStub) Release(context.Context, migration.TenantRef) error {
	b.journal.add("binding.Release")
	return nil
}

func (b *bindingStub) RoutedTo(context.Context, migration.TenantRef) (string, error) {
	return b.routed, nil
}

func (b *bindingStub) DrainStatus(context.Context, migration.TenantRef) (migration.DrainStatus, error) {
	return migration.DrainStatus{}, nil
}

// fleetStub answers for a fleet of replicas without any of them existing.
type fleetStub struct {
	journal *journal
	pods    []string
	// refuse names a replica that answers every call with a failure.
	refuse string
	// report is what every replica says when its drain status is asked for.
	report map[string]string
}

func (f *fleetStub) Endpoints(context.Context, client.ObjectKey) ([]ControlEndpoint, error) {
	endpoints := make([]ControlEndpoint, 0, len(f.pods))
	for _, pod := range f.pods {
		endpoints = append(endpoints, ControlEndpoint{Pod: pod, BaseURL: "https://" + pod})
	}
	return endpoints, nil
}

func (f *fleetStub) Do(
	_ context.Context,
	_ client.ObjectKey,
	_, endpoint string,
	_ any,
) (Answer, error) {
	pod, path := split(endpoint)
	f.journal.add(path + " " + pod)
	if pod == f.refuse {
		return Answer{}, errors.New("the replica is not answering")
	}
	if strings.HasPrefix(path, "/drainStatus") {
		return Answer{Status: 200, Body: []byte(f.report[pod])}, nil
	}
	return Answer{Status: 200, Body: []byte("{}")}, nil
}

// split turns https://pod/path?query back into the replica and the operation.
func split(endpoint string) (pod, path string) {
	trimmed := strings.TrimPrefix(endpoint, "https://")
	slash := strings.Index(trimmed, "/")
	pod, path = trimmed[:slash], trimmed[slash:]
	if question := strings.Index(path, "?"); question >= 0 {
		path = path[:question]
	}
	return pod, path
}

func drainReportJSON(quiesced, drained bool, queued, inFlight int) string {
	return fmt.Sprintf(
		`{"tenant":%q,"instance":%q,"quiesced":%t,"inFlight":%d,"queued":%d,`+
			`"drained":%t,"holder":%q,"leaseExpiresInMs":9000}`,
		routerDatabase, routerSource, quiesced, inFlight, queued, drained, routerHolder)
}

func routerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("core scheme: %v", err)
	}
	if err := pgelasticv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("pgelastic scheme: %v", err)
	}
	return scheme
}

// routerObjects is the smallest cluster a cutover can be driven against: a tenant, the pool
// it belongs to, the operator's client certificate, and the document the fleet re-reads.
func routerObjects(routedTo string) []client.Object {
	document := "configVersion = \"1-abc\"\n\n[routing]\ntenants = { " +
		tomlString(routerDatabase) + " = " + tomlString(routedTo) + " }\n"
	return []client.Object{
		&pgelasticv1alpha1.PgTenant{
			ObjectMeta: metav1.ObjectMeta{Name: routerTenant, Namespace: routerNamespace},
			Spec: pgelasticv1alpha1.PgTenantSpec{
				PoolRef:      corev1.LocalObjectReference{Name: routerPool},
				DatabaseName: routerDatabase,
			},
		},
		&pgelasticv1alpha1.PgElasticPool{
			ObjectMeta: metav1.ObjectMeta{Name: routerPool, Namespace: routerNamespace},
			Spec: pgelasticv1alpha1.PgElasticPoolSpec{
				Proxy: &pgelasticv1alpha1.ProxySpec{Replicas: ptr.To(int32(3))},
			},
		},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: ControlClientSecretName(routerPool), Namespace: routerNamespace,
		}},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: ConfigSecretName(routerPool), Namespace: routerNamespace,
			},
			Data: map[string][]byte{ConfigKey: []byte(document)},
		},
	}
}

type harness struct {
	router  *ProxyRouter
	journal *journal
	fleet   *fleetStub
	binding *bindingStub
	kube    client.Client
	clock   time.Time
}

func newHarness(t *testing.T, routedTo string) *harness {
	t.Helper()
	pods := []string{"proxy-0", "proxy-1", "proxy-2"}
	shared := &journal{}
	fleet := &fleetStub{journal: shared, pods: pods, report: map[string]string{}}
	for _, pod := range pods {
		fleet.report[pod] = drainReportJSON(true, true, 0, 0)
	}
	binding := &bindingStub{journal: shared, routed: routerSource}
	kube := fake.NewClientBuilder().
		WithScheme(routerScheme(t)).
		WithObjects(routerObjects(routedTo)...).
		Build()

	h := &harness{
		journal: shared,
		fleet:   fleet,
		binding: binding,
		kube:    kube,
		clock:   time.Unix(1_700_000_000, 0),
	}
	h.router = &ProxyRouter{
		Binding:       binding,
		Reader:        kube,
		Endpoints:     fleet,
		Caller:        fleet,
		LeaseTTL:      15 * time.Second,
		RenderTimeout: 500 * time.Millisecond,
		Now:           func() time.Time { return h.clock },
	}
	return h
}

// The ordering contract. Fleet::apply_routes replaces the routing table wholesale from the
// configuration Secret on every reload tick, so a setRoute pushed before the operator has
// rendered the new binding is reverted within a second - while the tenant's clients are
// queued, waiting to be released onto an instance that is by then fenced.
func TestAFlipIsNotPushedToTheFleetBeforeTheDocumentCarriesIt(t *testing.T) {
	h := newHarness(t, routerSource)
	quiesce(t, h)

	err := h.router.Route(context.Background(), acme, routerTarget)
	if err == nil {
		t.Fatal("the flip was accepted while the rendered document still named the source")
	}
	if !strings.Contains(err.Error(), ConfigSecretName(routerPool)) {
		t.Fatalf("the refusal does not name the document that had not caught up: %v", err)
	}
	if h.journal.has("/setRoute proxy-0") {
		t.Fatalf("a setRoute the next reload tick would revert was pushed anyway: %v", h.journal.all())
	}
	if h.binding.routed != routerTarget {
		t.Fatalf("the binding was not written first, so nothing would ever converge")
	}
}

// The same call, once the operator's own re-render has landed: the flip goes out, and only
// after the write it depends on.
func TestAFlipReachesEveryReplicaOnceTheRenderLands(t *testing.T) {
	h := newHarness(t, routerSource)
	quiesce(t, h)

	go func() {
		time.Sleep(150 * time.Millisecond)
		render(t, h.kube, routerTarget)
	}()

	if err := h.router.Route(context.Background(), acme, routerTarget); err != nil {
		t.Fatalf("the flip was refused after the render landed: %v", err)
	}
	for _, pod := range h.fleet.pods {
		if !h.journal.has("/setRoute " + pod) {
			t.Fatalf("replica %s was never flipped: %v", pod, h.journal.all())
		}
	}
	if h.journal.indexOf("binding.Route "+routerTarget) > h.journal.indexOf("/setRoute proxy-0") {
		t.Fatalf("the fleet was flipped before the binding was written: %v", h.journal.all())
	}
}

// One shard of a tenant's clients queued behind a cutover that will not happen is the worst
// outcome available, and the lease is the backstop rather than the plan.
func TestAPartialQuiesceIsUnwoundRatherThanLeftToTheLease(t *testing.T) {
	h := newHarness(t, routerSource)
	h.fleet.refuse = "proxy-1"

	err := h.router.Quiesce(context.Background(), acme, routerHolder)
	if err == nil {
		t.Fatal("a quiesce that reached two replicas out of three was reported as a success")
	}
	if !strings.Contains(err.Error(), "proxy-1") {
		t.Fatalf("the failure does not name the replica that refused: %v", err)
	}
	if !h.journal.has("/unquiesce proxy-0") {
		t.Fatalf("the replica that took the hold was left holding it: %v", h.journal.all())
	}
	if h.journal.has("/quiesce proxy-2") {
		t.Fatalf("the fan-out carried on past a failure: %v", h.journal.all())
	}
}

func TestTheGateIsClosedOnEveryReplica(t *testing.T) {
	h := newHarness(t, routerSource)
	quiesce(t, h)
	for _, pod := range h.fleet.pods {
		if !h.journal.has("/quiesce " + pod) {
			t.Fatalf("replica %s still admits the tenant's traffic: %v", pod, h.journal.all())
		}
	}
}

// One replica still admitting traffic is one shard of the tenant's clients still writing to
// the instance the cutover is about to abandon, however quiet the other two are.
func TestADrainIsOnlyDrainedWhenEveryReplicaSaysSo(t *testing.T) {
	h := newHarness(t, routerSource)
	h.fleet.report["proxy-2"] = drainReportJSON(false, false, 0, 3)

	status, err := h.router.DrainStatus(context.Background(), acme)
	if err != nil {
		t.Fatalf("drain status: %v", err)
	}
	if !status.Known {
		t.Fatal("a fleet that answered reported that there was no gate to ask")
	}
	if status.Drained || status.Quiesced {
		t.Fatalf("the fleet reported a drain with a replica still admitting traffic: %+v", status)
	}
	if status.InFlight != 3 {
		t.Fatalf("in-flight transactions were not summed across the fleet: %+v", status)
	}
}

func TestQueuedClientsAreSummedAcrossTheFleet(t *testing.T) {
	h := newHarness(t, routerSource)
	for index, pod := range h.fleet.pods {
		h.fleet.report[pod] = drainReportJSON(true, true, index+1, 0)
	}
	status, err := h.router.DrainStatus(context.Background(), acme)
	if err != nil {
		t.Fatalf("drain status: %v", err)
	}
	if status.Queued != 6 {
		t.Fatalf("queued clients were counted on one replica rather than the fleet: %+v", status)
	}
}

// resume is the point of no return and unquiesce is what gives the lease back. A success
// that only resumed would leave the next migration of this tenant refused with a conflict
// until the lease expired; one that only unquiesced would roll the flip back.
func TestASuccessfulCutoverResumesAndThenReleasesTheLease(t *testing.T) {
	h := newHarness(t, routerSource)
	quiesce(t, h)

	if err := h.router.Resume(context.Background(), acme); err != nil {
		t.Fatalf("resume: %v", err)
	}
	for _, pod := range h.fleet.pods {
		resumed, released := h.journal.indexOf("/resume "+pod), h.journal.indexOf("/unquiesce "+pod)
		if resumed < 0 {
			t.Fatalf("replica %s never resumed, so its clients are still queued: %v", pod, h.journal.all())
		}
		if released < 0 {
			t.Fatalf("replica %s kept the lease, so the next migration is refused: %v", pod, h.journal.all())
		}
		if released < resumed {
			t.Fatalf("replica %s gave the lease back before committing the flip: %v", pod, h.journal.all())
		}
	}
}

func TestAnAbortReleasesTheHoldWithoutCommittingTheFlip(t *testing.T) {
	h := newHarness(t, routerSource)
	quiesce(t, h)

	if err := h.router.Release(context.Background(), acme); err != nil {
		t.Fatalf("release: %v", err)
	}
	if h.journal.has("/resume proxy-0") {
		t.Fatalf("an abort committed the flip it was abandoning: %v", h.journal.all())
	}
	if !h.journal.has("/unquiesce proxy-0") {
		t.Fatalf("an abort left the tenant's clients queued: %v", h.journal.all())
	}
}

// A pool with no fleet has nobody to queue and nobody to ask, and migrating it has to go on
// working: the binding is the whole router.
func TestAPoolWithNoFleetIsMigratedThroughTheBindingAlone(t *testing.T) {
	h := newHarness(t, routerSource)
	pool := &pgelasticv1alpha1.PgElasticPool{}
	key := client.ObjectKey{Namespace: routerNamespace, Name: routerPool}
	if err := h.kube.Get(context.Background(), key, pool); err != nil {
		t.Fatalf("pool: %v", err)
	}
	pool.Spec.Proxy = nil
	if err := h.kube.Update(context.Background(), pool); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := h.router.Quiesce(context.Background(), acme, routerHolder); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	if err := h.router.Route(context.Background(), acme, routerTarget); err != nil {
		t.Fatalf("route: %v", err)
	}
	status, err := h.router.DrainStatus(context.Background(), acme)
	if err != nil {
		t.Fatalf("drain status: %v", err)
	}
	if status.Known {
		t.Fatal("a pool with no fleet reported a gate that does not exist")
	}
	for _, entry := range h.journal.all() {
		if strings.HasPrefix(entry, "/") {
			t.Fatalf("a pool with no fleet was called anyway: %v", h.journal.all())
		}
	}
	if h.binding.routed != routerTarget {
		t.Fatal("the headless migration never moved the tenant")
	}
}

// The pause the product is measured on is the one the clients saw: gate closed to gate
// opened, not the controller's wall clock over the phases around it.
func TestThePauseIsMeasuredFromTheGateClosingToTheResume(t *testing.T) {
	h := newHarness(t, routerSource)
	quiesce(t, h)

	h.clock = h.clock.Add(1200 * time.Millisecond)
	if err := h.router.Resume(context.Background(), acme); err != nil {
		t.Fatalf("resume: %v", err)
	}

	held, reported := h.router.ClientPause(acme)
	if !reported {
		t.Fatal("no pause was reported for a tenant that was held")
	}
	if held != 1200*time.Millisecond {
		t.Fatalf("the reported pause is %s rather than the 1.2s the gate was closed", held)
	}
	if _, again := h.router.ClientPause(acme); again {
		t.Fatal("the same pause would be republished on the next reconcile as a second one")
	}
}

// A hold taken by a controller that then restarted has to be commitable by its successor,
// and the annotation the binding router writes is the only record of who owns it.
func TestAHolderSurvivesTheProcessThatTookTheHold(t *testing.T) {
	h := newHarness(t, routerTarget)
	tenant := &pgelasticv1alpha1.PgTenant{}
	key := client.ObjectKey{Namespace: routerNamespace, Name: routerTenant}
	if err := h.kube.Get(context.Background(), key, tenant); err != nil {
		t.Fatalf("tenant: %v", err)
	}
	tenant.SetAnnotations(map[string]string{migration.AnnotationQuiescedBy: routerHolder})
	if err := h.kube.Update(context.Background(), tenant); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := h.router.Route(context.Background(), acme, routerTarget); err != nil {
		t.Fatalf("route: %v", err)
	}
	if !h.journal.has("/setRoute proxy-0") {
		t.Fatalf("a restarted controller could not commit a flip already under way: %v", h.journal.all())
	}
}

func TestARenderedDocumentIsReadForTheTenantItNames(t *testing.T) {
	document := "[routing]\ntenants = { \"alpha\" = \"pg-a\", \"beta\" = \"pg-b\" }\n"
	if !RoutedInDocument(document, "beta", "pg-b") {
		t.Fatal("a binding that is in the document was reported missing")
	}
	if RoutedInDocument(document, "beta", "pg-a") {
		t.Fatal("a binding that is not in the document was reported present")
	}
	if RoutedInDocument("[routing]\n", "beta", "pg-b") {
		t.Fatal("a document with no routing table reported a binding")
	}
}

func quiesce(t *testing.T, h *harness) {
	t.Helper()
	if err := h.router.Quiesce(context.Background(), acme, routerHolder); err != nil {
		t.Fatalf("quiesce: %v", err)
	}
}

// render republishes the configuration Secret with a tenant bound to another instance,
// which is what the operator's own reconcile does a moment after the binding is written.
func render(t *testing.T, kube client.Client, instance string) {
	t.Helper()
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: routerNamespace, Name: ConfigSecretName(routerPool)}
	if err := kube.Get(context.Background(), key, secret); err != nil {
		t.Errorf("configuration Secret: %v", err)
		return
	}
	secret.Data[ConfigKey] = []byte("configVersion = \"2-def\"\n\n[routing]\ntenants = { " +
		tomlString(routerDatabase) + " = " + tomlString(instance) + " }\n")
	if err := kube.Update(context.Background(), secret); err != nil {
		t.Errorf("republishing the configuration: %v", err)
	}
}
