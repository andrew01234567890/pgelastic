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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

// ProxyRouter drives a cutover through the fleet's own control API, and keeps the routing
// table of record in step behind it.
//
// It composes rather than replaces the binding router. status.binding is what a replica
// that restarts, or one that is added an hour later, resolves the tenant through; the
// control API is what makes the flip immediate for the replicas that are running now. A
// pool with no fleet gets the binding alone, which is how a headless migration keeps
// working.
//
// Every call fans out to every replica. The gate is per-replica in-memory state and
// kube-proxy pins a connection to one endpoint for its life, so a tenant's clients are
// spread across the fleet: quiescing one replica quiesces one shard of them and leaves the
// rest writing to an instance the cutover is about to abandon.
type ProxyRouter struct {
	// Binding is the routing table of record, and the whole of the router when the pool
	// declares no fleet.
	Binding migration.Router
	// Reader reads every object this router resolves a fleet from, and should be an uncached
	// one. A quiesce aimed at the replicas an informer cache remembers is a quiesce that
	// misses the replica that replaced them.
	Reader client.Reader
	// Endpoints resolves the fleet's control API. Injectable because a test runner outside
	// the cluster cannot route to a Pod IP.
	Endpoints ControlEndpoints
	// Caller issues the requests. Injectable for the same reason, and so the order of the
	// calls can be asserted rather than described.
	Caller Caller

	// LeaseTTL is how long a hold survives an operator that stops renewing it. Renewal
	// happens at a third of it.
	LeaseTTL time.Duration
	// RenderTimeout bounds the wait for a flip to reach the configuration Secret before it
	// is pushed to the fleet. See Route for why the wait exists at all.
	RenderTimeout time.Duration
	// Now is injectable so the pause clock can be driven in tests.
	Now func() time.Time

	mu     sync.Mutex
	holds  map[string]*hold
	pauses map[string]time.Duration
}

// hold is one tenant's live quiesce: who owns it, when its clients were first queued, and
// the renewal loop keeping it alive.
type hold struct {
	holder   string
	closedAt time.Time
	stop     context.CancelFunc
	done     chan struct{}
}

var _ migration.Router = (*ProxyRouter)(nil)
var _ migration.PauseReporter = (*ProxyRouter)(nil)

// ControlEndpoint is one replica's cutover API.
type ControlEndpoint struct {
	// Pod names the replica, which is what a partial failure has to be reported against.
	Pod string
	// BaseURL is the origin the control API is served on.
	BaseURL string
}

// ControlEndpoints resolves every replica currently fronting a pool.
type ControlEndpoints interface {
	Endpoints(ctx context.Context, pool client.ObjectKey) ([]ControlEndpoint, error)
}

// Caller issues one control-API request and reports what came back.
//
// The pool is passed rather than a prepared client because the caller has to present that
// pool's own client certificate: the control CA is per pool, so one identity would not
// authenticate against another pool's fleet.
type Caller interface {
	Do(ctx context.Context, pool client.ObjectKey, method, endpoint string, body any) (Answer, error)
}

// Answer is one control-API response.
type Answer struct {
	Status int
	Body   []byte
}

// Defaults for the timings a cutover runs on.
const (
	// DefaultLeaseTTL matches the ceiling the rendered configuration gives the fleet.
	DefaultLeaseTTL = ControlLeaseTTLMillis * time.Millisecond
	// DefaultRenderTimeout bounds the wait for the operator's own re-render. It is generous
	// against the reconcile it is waiting for and short against the cutover budget, because
	// exceeding it costs a retry rather than a migration.
	DefaultRenderTimeout = 15 * time.Second
	// renderPollInterval is how often the configuration Secret is re-read while waiting. It
	// is inside the client-visible pause, so it is as short as an API read can usefully be.
	renderPollInterval = 100 * time.Millisecond
)

func (r *ProxyRouter) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *ProxyRouter) leaseTTL() time.Duration {
	if r.LeaseTTL > 0 {
		return r.LeaseTTL
	}
	return DefaultLeaseTTL
}

func (r *ProxyRouter) renderTimeout() time.Duration {
	if r.RenderTimeout > 0 {
		return r.RenderTimeout
	}
	return DefaultRenderTimeout
}

// Quiesce closes the tenant's gate on every replica and keeps it closed.
//
// The binding is annotated first. That annotation is how a controller that restarted
// mid-cutover finds out which migration owns the hold, and without it the restarted process
// could neither renew the lease nor commit the flip.
func (r *ProxyRouter) Quiesce(ctx context.Context, tenant migration.TenantRef, name string) error {
	if err := r.Binding.Quiesce(ctx, tenant, name); err != nil {
		return err
	}
	fleet, err := r.resolve(ctx, tenant)
	if err != nil || fleet == nil {
		return err
	}
	if err := r.closeGate(ctx, fleet, name); err != nil {
		return err
	}
	r.keepAlive(fleet, name)
	return nil
}

// PreWarm publishes the pre-warm hint. The control API has no endpoint for it: opening
// backends ahead of the flip is worth doing and is not worth inventing a lease-bound
// operation for, because nothing about it is exclusive.
func (r *ProxyRouter) PreWarm(ctx context.Context, tenant migration.TenantRef, instance string) error {
	return r.Binding.PreWarm(ctx, tenant, instance)
}

// Route flips the tenant onto an instance, durably and then immediately.
//
// The order is the whole of this method and it is not tidiness. Fleet::apply_routes
// replaces the routing table wholesale from the configuration Secret on every reload tick,
// so a control-plane setRoute that ran before the operator had rendered the new binding is
// reverted within a second - silently, and while the tenant's clients are queued waiting to
// be released onto an instance that is by then fenced. So: write the binding, wait until the
// document the fleet re-reads actually carries it, and only then push the flip for
// immediacy. A render that has not landed inside the budget is reported as a fault and
// retried; it is never skipped, because skipping it is the bug.
func (r *ProxyRouter) Route(ctx context.Context, tenant migration.TenantRef, instance string) error {
	if err := r.Binding.Route(ctx, tenant, instance); err != nil {
		return err
	}
	fleet, err := r.resolve(ctx, tenant)
	if err != nil || fleet == nil {
		return err
	}
	holder, ok := r.holderFor(ctx, tenant)
	if !ok {
		// Nothing holds the tenant, so there is no gate to flip inside and setRoute would be
		// refused anyway. The binding is written and the next reload tick applies it.
		return nil
	}
	if err := r.awaitRendered(ctx, fleet, instance); err != nil {
		return err
	}
	return r.everyReplica(fleet, "flipping the route", func(endpoint ControlEndpoint) error {
		return r.post(ctx, fleet, endpoint, "/setRoute", routeBody(fleet.database, holder, instance))
	})
}

// Resume opens the gate against the instance the flip named and then gives the lease back.
//
// Both halves, in that order. resume is the point of no return: it clears the source the
// hold recorded, so an expiring lease can no longer roll the tenant back. unquiesce
// afterwards releases the lease, which matters because a lease left to expire refuses the
// next migration of the same tenant with a conflict for up to its whole ceiling.
func (r *ProxyRouter) Resume(ctx context.Context, tenant migration.TenantRef) error {
	return r.finish(ctx, tenant, true)
}

// Release abandons the hold. Whatever route the cutover pushed is rolled back by the fleet
// itself, which is what makes an abort restore the source rather than merely stop.
func (r *ProxyRouter) Release(ctx context.Context, tenant migration.TenantRef) error {
	return r.finish(ctx, tenant, false)
}

func (r *ProxyRouter) finish(ctx context.Context, tenant migration.TenantRef, commit bool) error {
	holder, held := r.holderFor(ctx, tenant)
	fleet, err := r.resolve(ctx, tenant)

	var problems []error
	if err != nil {
		problems = append(problems, err)
	}
	if fleet != nil && held {
		if commit {
			problems = append(problems, r.everyReplica(fleet, "resuming", func(e ControlEndpoint) error {
				return r.post(ctx, fleet, e, "/resume", holderBody(fleet.database, holder))
			}))
		}
		problems = append(problems, r.everyReplica(fleet, "releasing the hold", func(e ControlEndpoint) error {
			return r.post(ctx, fleet, e, "/unquiesce", holderBody(fleet.database, holder))
		}))
	}
	// The renewal loop stops whether or not the calls above succeeded. Leaving it running
	// would keep renewing a hold nobody intends to end deliberately any more, which turns a
	// failed release into an unbounded one instead of one bounded by the lease.
	r.endHold(tenant)
	problems = append(problems, r.Binding.Release(ctx, tenant))
	return errors.Join(problems...)
}

// RoutedTo answers from the binding rather than from the fleet.
//
// The fleet's table is derived state: apply_routes rewrites it wholesale from the Secret
// every tick, and an expired lease rolls it back without anybody being told. The binding is
// the only answer that survives either, and the restore path exists precisely to make the
// two agree - so asking the fleet would be asking the thing being corrected whether it
// needs correcting.
func (r *ProxyRouter) RoutedTo(ctx context.Context, tenant migration.TenantRef) (string, error) {
	return r.Binding.RoutedTo(ctx, tenant)
}

// DrainStatus sums the gate's own account over every replica.
//
// Drained is the conjunction and the counts are the sums, because the claim is about the
// tenant and the tenant is spread across the fleet: one replica still admitting traffic is
// enough to make the whole cutover unsafe, however quiet the other two are.
func (r *ProxyRouter) DrainStatus(
	ctx context.Context,
	tenant migration.TenantRef,
) (migration.DrainStatus, error) {
	fleet, err := r.resolve(ctx, tenant)
	if err != nil || fleet == nil {
		return migration.DrainStatus{}, err
	}
	total := migration.DrainStatus{Known: true, Quiesced: true, Drained: true}
	for _, endpoint := range fleet.endpoints {
		query := "/drainStatus?tenant=" + url.QueryEscape(fleet.database)
		answer, err := r.Caller.Do(ctx, fleet.pool, "GET", endpoint.BaseURL+query, nil)
		if err != nil {
			return migration.DrainStatus{}, fmt.Errorf("drain status from %s: %w", endpoint.Pod, err)
		}
		if answer.Status != 200 {
			return migration.DrainStatus{}, refusal(endpoint, "/drainStatus", answer)
		}
		var report drainReport
		if err := json.Unmarshal(answer.Body, &report); err != nil {
			return migration.DrainStatus{}, fmt.Errorf("drain status from %s: %w", endpoint.Pod, err)
		}
		total.Queued += report.Queued
		total.InFlight += report.InFlight
		total.Quiesced = total.Quiesced && report.Quiesced
		total.Drained = total.Drained && report.Drained
		if report.LeaseExpiresInMs != nil {
			left := time.Duration(*report.LeaseExpiresInMs) * time.Millisecond
			if total.LeaseExpiresIn == 0 || left < total.LeaseExpiresIn {
				total.LeaseExpiresIn = left
			}
		}
	}
	if len(fleet.endpoints) == 0 {
		return migration.DrainStatus{}, nil
	}
	return total, nil
}

// ClientPause reports the hold that ended most recently, once.
func (r *ProxyRouter) ClientPause(tenant migration.TenantRef) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	held, ok := r.pauses[tenant.String()]
	if ok {
		delete(r.pauses, tenant.String())
	}
	return held, ok
}

type drainReport struct {
	Instance         string `json:"instance"`
	Quiesced         bool   `json:"quiesced"`
	InFlight         int64  `json:"inFlight"`
	Queued           int64  `json:"queued"`
	Drained          bool   `json:"drained"`
	Holder           string `json:"holder"`
	LeaseExpiresInMs *int64 `json:"leaseExpiresInMs"`
}

func holderBody(database, holder string) map[string]any {
	return map[string]any{"tenant": database, "holder": holder}
}

func quiesceBody(database, holder string, ttl time.Duration) map[string]any {
	ask := holderBody(database, holder)
	ask["ttlMs"] = ttl.Milliseconds()
	return ask
}

func routeBody(database, holder, instance string) map[string]any {
	ask := holderBody(database, holder)
	ask["instance"] = instance
	return ask
}

// closeGate quiesces every replica, and undoes itself if it cannot do all of them.
//
// A tenant quiesced on two replicas out of three is the worst of both worlds: two thirds of
// its clients are queued behind a cutover that will not happen, and the lease TTL is the
// only thing that would ever let them go. So a partial take is unwound immediately and
// reported, rather than left for the backstop to clean up in fifteen seconds.
func (r *ProxyRouter) closeGate(ctx context.Context, fleet *fleet, holder string) error {
	taken := make([]ControlEndpoint, 0, len(fleet.endpoints))
	var failure error
	for _, endpoint := range fleet.endpoints {
		err := r.post(ctx, fleet, endpoint, "/quiesce",
			quiesceBody(fleet.database, holder, r.leaseTTL()))
		if err != nil {
			failure = fmt.Errorf("quiescing %s: %w", endpoint.Pod, err)
			break
		}
		taken = append(taken, endpoint)
	}
	if failure == nil {
		r.beginHold(fleet.tenant, holder)
		return nil
	}
	for _, endpoint := range taken {
		if err := r.post(ctx, fleet, endpoint, "/unquiesce", holderBody(fleet.database, holder)); err != nil {
			failure = errors.Join(failure, fmt.Errorf("unwinding the quiesce on %s: %w", endpoint.Pod, err))
		}
	}
	return failure
}

// keepAlive renews the hold for as long as the phase owns it.
//
// The lease is fifteen seconds and the cutover budget is sixty, so a holder that takes the
// gate once and then gets on with a dump is auto-unquiesced part-way through - which turns
// a controlled cutover into an uncontrolled one and does it silently. Renewal is at a third
// of the TTL so two consecutive failures still leave a whole interval of margin, and it
// stops the moment the hold ends: an operator that crashes should give the tenant back in
// seconds rather than at the ceiling.
func (r *ProxyRouter) keepAlive(fleet *fleet, holder string) {
	r.mu.Lock()
	current, running := r.holds[fleet.tenant.String()]
	if !running || current.holder != holder || current.done != nil {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(context.Background()))
	done := make(chan struct{})
	current.stop, current.done = cancel, done
	r.mu.Unlock()

	interval := r.leaseTTL() / 3
	tenant := fleet.tenant
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !r.renew(ctx, tenant, holder) {
					r.abandon(ctx, tenant, holder)
					return
				}
			}
		}
	}()
}

// renew re-resolves the fleet on every tick rather than reusing the endpoints the quiesce
// found. A replica that restarted, or one a scale-up added, starts with an open gate and
// would otherwise serve the tenant's clients straight through the middle of the cutover.
//
// It answers false when the migration that owns the hold has finished or been deleted. That
// case is not hypothetical: deleting a PgTenantMigration mid-cutover ends the phase without
// anything ever calling Release, and a renewal loop that did not notice would go on holding
// the tenant's clients for as long as the process lived.
func (r *ProxyRouter) renew(ctx context.Context, tenant migration.TenantRef, holder string) bool {
	log := logf.FromContext(ctx).WithValues("tenant", tenant.String(), "holder", holder)
	if !r.holderIsStillRunning(ctx, holder) {
		return false
	}
	fleet, err := r.resolve(ctx, tenant)
	if err != nil || fleet == nil {
		if err != nil {
			log.Error(err, "Could not resolve the fleet holding a quiesced tenant")
		}
		return true
	}
	for _, endpoint := range fleet.endpoints {
		err := r.post(ctx, fleet, endpoint, "/quiesce",
			quiesceBody(fleet.database, holder, r.leaseTTL()))
		if err != nil {
			log.Error(err, "Could not renew a quiesce lease", "replica", endpoint.Pod)
		}
	}
	return true
}

// holderIsStillRunning reports whether the migration named in the lease is one a cutover is
// still being driven by. A holder this router cannot resolve is left alone: an unreadable
// API server must not be a reason to drop a hold that is doing its job.
func (r *ProxyRouter) holderIsStillRunning(ctx context.Context, holder string) bool {
	namespace, name, found := strings.Cut(holder, "/")
	if !found {
		return true
	}
	object := &pgelasticv1alpha1.PgTenantMigration{}
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, object)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		return true
	}
	return !migration.Terminal(object.Status.Phase)
}

// abandon gives a tenant back when the migration holding it has stopped existing. It does
// not wait for its own goroutine, because it is called from inside it.
func (r *ProxyRouter) abandon(ctx context.Context, tenant migration.TenantRef, holder string) {
	log := logf.FromContext(ctx).WithValues("tenant", tenant.String(), "holder", holder)
	log.Info("Releasing a tenant whose migration is no longer running")

	if fleet, err := r.resolve(ctx, tenant); err == nil && fleet != nil {
		for _, endpoint := range fleet.endpoints {
			if err := r.post(ctx, fleet, endpoint, "/unquiesce",
				holderBody(fleet.database, holder)); err != nil {
				log.Error(err, "Could not release an abandoned hold", "replica", endpoint.Pod)
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.holds[tenant.String()]
	if !ok || current.holder != holder {
		return
	}
	delete(r.holds, tenant.String())
	if r.pauses == nil {
		r.pauses = map[string]time.Duration{}
	}
	r.pauses[tenant.String()] = r.now().Sub(current.closedAt)
}

func (r *ProxyRouter) beginHold(tenant migration.TenantRef, holder string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.holds == nil {
		r.holds = map[string]*hold{}
	}
	if current, ok := r.holds[tenant.String()]; ok && current.holder == holder {
		return
	}
	r.holds[tenant.String()] = &hold{holder: holder, closedAt: r.now()}
}

// endHold stops the renewal loop and records how long the clients were actually queued.
func (r *ProxyRouter) endHold(tenant migration.TenantRef) {
	r.mu.Lock()
	current, ok := r.holds[tenant.String()]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.holds, tenant.String())
	if r.pauses == nil {
		r.pauses = map[string]time.Duration{}
	}
	r.pauses[tenant.String()] = r.now().Sub(current.closedAt)
	r.mu.Unlock()

	if current.stop != nil {
		current.stop()
		<-current.done
	}
}

// holderFor names the migration that owns the tenant's gate.
//
// The in-memory hold first, and the annotation the binding router wrote as the fallback: a
// controller that restarted mid-cutover has no hold to consult, and the alternative to
// reading the annotation is being unable to commit a flip that has already begun.
func (r *ProxyRouter) holderFor(ctx context.Context, tenant migration.TenantRef) (string, bool) {
	r.mu.Lock()
	current, ok := r.holds[tenant.String()]
	r.mu.Unlock()
	if ok {
		return current.holder, true
	}

	object := &pgelasticv1alpha1.PgTenant{}
	key := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Name}
	if err := r.Reader.Get(ctx, key, object); err != nil {
		return "", false
	}
	holder := object.GetAnnotations()[migration.AnnotationQuiescedBy]
	return holder, holder != ""
}

// fleet is one pool's control plane as this router needs it: who to call, about which
// tenant, under whose identity.
type fleet struct {
	pool      client.ObjectKey
	tenant    migration.TenantRef
	database  string
	endpoints []ControlEndpoint
}

// resolve finds the fleet fronting a tenant, or reports that there is none.
//
// A nil fleet and a nil error is the headless case and is not a failure: a pool that
// declares no proxy, or one whose control listener has not been issued its certificates
// yet, is migrated through the binding alone.
func (r *ProxyRouter) resolve(ctx context.Context, tenant migration.TenantRef) (*fleet, error) {
	object := &pgelasticv1alpha1.PgTenant{}
	key := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Name}
	if err := r.Reader.Get(ctx, key, object); err != nil {
		return nil, fmt.Errorf("PgTenant %s: %w", tenant, err)
	}
	if object.Spec.DatabaseName == "" || object.Spec.PoolRef.Name == "" {
		return nil, nil
	}
	poolKey := client.ObjectKey{Namespace: tenant.Namespace, Name: object.Spec.PoolRef.Name}

	pool := &pgelasticv1alpha1.PgElasticPool{}
	if err := r.Reader.Get(ctx, poolKey, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("PgElasticPool %s: %w", poolKey.Name, err)
	}
	if pool.Spec.Proxy == nil {
		return nil, nil
	}
	// The client certificate is the operator's only way in. Its absence means cert-manager
	// has not issued it, which is the same cluster in which the listener was never rendered.
	if err := r.Reader.Get(ctx, client.ObjectKey{
		Namespace: poolKey.Namespace, Name: ControlClientSecretName(poolKey.Name),
	}, &corev1.Secret{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("control client certificate for %s: %w", poolKey.Name, err)
	}

	endpoints, err := r.Endpoints.Endpoints(ctx, poolKey)
	if err != nil {
		return nil, err
	}
	if len(endpoints) == 0 {
		return nil, nil
	}
	return &fleet{
		pool:      poolKey,
		tenant:    tenant,
		database:  object.Spec.DatabaseName,
		endpoints: endpoints,
	}, nil
}

// awaitRendered blocks until the configuration Secret the fleet re-reads carries the flip.
//
// The deadline is wall-clock rather than the injectable one. This is a wait on another
// process, and a test that froze the clock to drive the pause measurement would otherwise
// wait for a render that had already happened for ever.
func (r *ProxyRouter) awaitRendered(ctx context.Context, fleet *fleet, instance string) error {
	waitCtx, cancel := context.WithTimeout(ctx, r.renderTimeout())
	defer cancel()

	key := client.ObjectKey{
		Namespace: fleet.pool.Namespace, Name: ConfigSecretName(fleet.pool.Name),
	}
	var last error
	for {
		secret := &corev1.Secret{}
		last = r.Reader.Get(waitCtx, key, secret)
		if last == nil && RoutedInDocument(string(secret.Data[ConfigKey]), fleet.database, instance) {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf(
				"the flip of %s onto %s was written to the binding but had not reached %s "+
					"within %s, and pushing it to the fleet before then is reverted by the next "+
					"reload tick: %w",
				fleet.database, instance, key.Name, r.renderTimeout(), errors.Join(last, waitCtx.Err()))
		case <-time.After(renderPollInterval):
		}
	}
}

// everyReplica applies one call to the whole fleet and reports every replica that refused
// it, rather than the first.
func (r *ProxyRouter) everyReplica(
	fleet *fleet,
	what string,
	call func(ControlEndpoint) error,
) error {
	var problems []error
	for _, endpoint := range fleet.endpoints {
		if err := call(endpoint); err != nil {
			problems = append(problems, fmt.Errorf("%s on %s: %w", what, endpoint.Pod, err))
		}
	}
	return errors.Join(problems...)
}

func (r *ProxyRouter) post(
	ctx context.Context,
	fleet *fleet,
	endpoint ControlEndpoint,
	path string,
	body any,
) error {
	answer, err := r.Caller.Do(ctx, fleet.pool, "POST", endpoint.BaseURL+path, body)
	if err != nil {
		return err
	}
	switch answer.Status {
	case 200:
		return nil
	case 422:
		// The gate is not held. For a release that is the wanted state already reached,
		// which is exactly what an expired lease leaves behind.
		if path == "/unquiesce" {
			return nil
		}
		return refusal(endpoint, path, answer)
	default:
		return refusal(endpoint, path, answer)
	}
}

func refusal(endpoint ControlEndpoint, path string, answer Answer) error {
	return fmt.Errorf("%s on %s answered %d: %s",
		path, endpoint.Pod, answer.Status, strings.TrimSpace(string(answer.Body)))
}

// RoutedInDocument reports whether a rendered configuration already binds a tenant to an
// instance. It reads the one line the fleet's routing table is built from, so a document
// that says otherwise cannot be mistaken for one that has caught up.
func RoutedInDocument(document, database, instance string) bool {
	for line := range strings.SplitSeq(document, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "tenants = {") {
			continue
		}
		return strings.Contains(trimmed, tomlString(database)+" = "+tomlString(instance))
	}
	return false
}
