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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// InstanceDrain is the fleet's own account of what one instance is still holding.
//
// Summed over the replicas, because a tenant's clients are spread across them: one replica
// still admitting traffic is enough to make a role change unsafe however quiet the others
// are. Drained is the conjunction and the counts are the sums, exactly as for a tenant.
type InstanceDrain struct {
	// Known is false when the pool has no fleet to ask, which is the headless case and not
	// a failure.
	Known bool
	// Quiesced is every replica holding this instance.
	Quiesced bool
	// Tenants is the union of the tenants the replicas resolved onto the instance. It is
	// reported rather than derived by the caller because the set follows a routing table
	// that moves, and a caller re-deriving it would be re-deriving it from stale input.
	Tenants []string
	// InFlight is how many backends the instance's tenants are still holding, and Queued
	// how many transactions are parked behind the hold.
	InFlight int64
	Queued   int64
	// Drained is quiesced with nothing in flight: the instant a role change may proceed.
	// It is the precondition that keeps the fence's undecidable row empty, because a commit
	// that was forwarded and never answered is counted in InFlight until it is answered.
	Drained bool
	// LeaseExpiresIn is the soonest any replica's hold runs out.
	LeaseExpiresIn time.Duration
}

// QuiesceInstance holds every tenant on an instance, across every replica, and keeps the
// hold alive until it is released.
//
// This is the switchover's own lease, not the tenant leases taken in bulk, and the
// difference is not an optimisation. A tenant lease is single-holder, so taking two hundred
// of them means a live migration of any one tenant answers 409 and the switchover has no
// partial state to report - 199 tenants held and one admitting is not an instance that is
// held, and there is nothing sensible to do about the one. The instance lease is a separate
// exclusion over a separate thing: which member of the instance is the primary. A checkout
// passes both gates, so the migration keeps the tenant it is moving, the switchover keeps
// the instance, and neither can take the other's.
func (r *ProxyRouter) QuiesceInstance(
	ctx context.Context,
	pool client.ObjectKey,
	instance, holder string,
) error {
	fleet, err := r.resolvePool(ctx, pool)
	if err != nil || fleet == nil {
		return err
	}
	if err := r.closeInstanceGate(ctx, fleet, instance, holder); err != nil {
		return err
	}
	key := instanceHoldKey(pool, instance)
	r.keepAlive(key, holder, func(ctx context.Context) bool {
		return r.renewInstance(ctx, pool, instance, holder)
	})
	return nil
}

// InstanceDrainStatus sums the hold's own account over every replica.
func (r *ProxyRouter) InstanceDrainStatus(
	ctx context.Context,
	pool client.ObjectKey,
	instance string,
) (InstanceDrain, error) {
	fleet, err := r.resolvePool(ctx, pool)
	if err != nil || fleet == nil {
		return InstanceDrain{}, err
	}
	total := InstanceDrain{Known: true, Quiesced: true, Drained: true}
	seen := map[string]bool{}
	for _, endpoint := range fleet.endpoints {
		query := "/instanceDrainStatus?instance=" + url.QueryEscape(instance)
		answer, err := r.Caller.Do(ctx, fleet.pool, "GET", endpoint.BaseURL+query, nil)
		if err != nil {
			return InstanceDrain{}, fmt.Errorf("instance drain status from %s: %w", endpoint.Pod, err)
		}
		if answer.Status != 200 {
			return InstanceDrain{}, refusal(endpoint, "/instanceDrainStatus", answer)
		}
		var report instanceDrainReport
		if err := json.Unmarshal(answer.Body, &report); err != nil {
			return InstanceDrain{}, fmt.Errorf("instance drain status from %s: %w", endpoint.Pod, err)
		}
		total.Queued += report.Queued
		total.InFlight += report.InFlight
		total.Quiesced = total.Quiesced && report.Quiesced
		total.Drained = total.Drained && report.Drained
		for _, tenant := range report.Tenants {
			if !seen[tenant] {
				seen[tenant] = true
				total.Tenants = append(total.Tenants, tenant)
			}
		}
		if report.LeaseExpiresInMs != nil {
			left := time.Duration(*report.LeaseExpiresInMs) * time.Millisecond
			if total.LeaseExpiresIn == 0 || left < total.LeaseExpiresIn {
				total.LeaseExpiresIn = left
			}
		}
	}
	if len(fleet.endpoints) == 0 {
		return InstanceDrain{}, nil
	}
	return total, nil
}

// ResumeInstance opens the gate and then gives the lease back, in that order and for the
// same reason the tenant path does: a lease left to expire refuses the next switchover of
// the same instance with a conflict for up to its whole ceiling.
func (r *ProxyRouter) ResumeInstance(
	ctx context.Context,
	pool client.ObjectKey,
	instance, holder string,
) error {
	return r.finishInstance(ctx, pool, instance, holder, true)
}

// ReleaseInstance abandons the hold without committing it, which is what an aborted
// switchover leaves behind.
func (r *ProxyRouter) ReleaseInstance(
	ctx context.Context,
	pool client.ObjectKey,
	instance, holder string,
) error {
	return r.finishInstance(ctx, pool, instance, holder, false)
}

// InstancePause reports how long an instance's clients were held, once.
func (r *ProxyRouter) InstancePause(
	pool client.ObjectKey,
	instance string,
) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := instanceHoldKey(pool, instance)
	held, ok := r.pauses[key]
	if ok {
		delete(r.pauses, key)
	}
	return held, ok
}

func (r *ProxyRouter) finishInstance(
	ctx context.Context,
	pool client.ObjectKey,
	instance, holder string,
	commit bool,
) error {
	fleet, err := r.resolvePool(ctx, pool)

	var problems []error
	if err != nil {
		problems = append(problems, err)
	}
	if fleet != nil {
		if commit {
			problems = append(problems, r.everyReplica(fleet, "resuming the instance",
				func(e ControlEndpoint) error {
					return r.post(ctx, fleet, e, "/resumeInstance", instanceBody(instance, holder))
				}))
		}
		problems = append(problems, r.everyReplica(fleet, "releasing the instance hold",
			func(e ControlEndpoint) error {
				return r.post(ctx, fleet, e, "/unquiesceInstance", instanceBody(instance, holder))
			}))
	}
	// The renewal loop stops whether or not the calls above succeeded, so a failed release
	// is bounded by the lease rather than unbounded.
	r.endHold(instanceHoldKey(pool, instance))
	return errors.Join(problems...)
}

// closeInstanceGate holds every replica, and unwinds itself if it cannot hold all of them.
//
// An instance held on two replicas out of three is worse than one held on none: two thirds
// of every tenant's traffic is queued behind a role change that is not going to happen, and
// the lease TTL is the only thing that would ever let it go.
func (r *ProxyRouter) closeInstanceGate(
	ctx context.Context,
	fleet *fleet,
	instance, holder string,
) error {
	taken := make([]ControlEndpoint, 0, len(fleet.endpoints))
	var failure error
	for _, endpoint := range fleet.endpoints {
		err := r.post(ctx, fleet, endpoint, "/quiesceInstance",
			instanceQuiesceBody(instance, holder, r.leaseTTL()))
		if err != nil {
			failure = fmt.Errorf("holding %s on %s: %w", instance, endpoint.Pod, err)
			break
		}
		taken = append(taken, endpoint)
	}
	if failure == nil {
		r.beginHold(instanceHoldKey(fleet.pool, instance), holder)
		return nil
	}
	for _, endpoint := range taken {
		if err := r.post(ctx, fleet, endpoint, "/unquiesceInstance",
			instanceBody(instance, holder)); err != nil {
			failure = errors.Join(failure,
				fmt.Errorf("unwinding the instance hold on %s: %w", endpoint.Pod, err))
		}
	}
	return failure
}

// renewInstance re-resolves the fleet on every tick rather than reusing the endpoints the
// hold was taken against. A replica that restarted, or one a scale-up added, starts with an
// open gate and would otherwise admit traffic straight through the middle of a role change.
//
// It answers false only when the pool has stopped existing, because that is the one case in
// which nothing will ever release the hold deliberately. An unreadable API server is not
// that case: refusing to renew on it would drop a hold that is doing its job.
func (r *ProxyRouter) renewInstance(
	ctx context.Context,
	pool client.ObjectKey,
	instance, holder string,
) bool {
	log := logf.FromContext(ctx).WithValues("instance", instance, "holder", holder)
	fleet, err := r.resolvePool(ctx, pool)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil || fleet == nil {
		if err != nil {
			log.Error(err, "Could not resolve the fleet holding an instance")
		}
		return true
	}
	for _, endpoint := range fleet.endpoints {
		if err := r.post(ctx, fleet, endpoint, "/quiesceInstance",
			instanceQuiesceBody(instance, holder, r.leaseTTL())); err != nil {
			log.Error(err, "Could not renew an instance hold", "replica", endpoint.Pod)
		}
	}
	return true
}

// resolvePool finds the fleet fronting a pool, or reports that there is none.
//
// A nil fleet and a nil error is the headless case: a pool that declares no proxy, or one
// whose control listener has not been issued its certificates yet, has no clients to hold
// and a role change on it is simply the unheld one.
func (r *ProxyRouter) resolvePool(
	ctx context.Context,
	poolKey client.ObjectKey,
) (*fleet, error) {
	pool := &pgelasticv1alpha1.PgElasticPool{}
	if err := r.Reader.Get(ctx, poolKey, pool); err != nil {
		return nil, fmt.Errorf("PgElasticPool %s: %w", poolKey.Name, err)
	}
	if pool.Spec.Proxy == nil {
		return nil, nil
	}
	if err := r.Reader.Get(ctx, types.NamespacedName{
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
	return &fleet{pool: poolKey, endpoints: endpoints}, nil
}

type instanceDrainReport struct {
	Instance         string   `json:"instance"`
	Quiesced         bool     `json:"quiesced"`
	Tenants          []string `json:"tenants"`
	InFlight         int64    `json:"inFlight"`
	Queued           int64    `json:"queued"`
	Drained          bool     `json:"drained"`
	Holder           string   `json:"holder"`
	LeaseExpiresInMs *int64   `json:"leaseExpiresInMs"`
}

func instanceBody(instance, holder string) map[string]any {
	return map[string]any{"instance": instance, "holder": holder}
}

func instanceQuiesceBody(instance, holder string, ttl time.Duration) map[string]any {
	ask := instanceBody(instance, holder)
	ask["ttlMs"] = ttl.Milliseconds()
	return ask
}

// instanceHoldKey namespaces the hold by its pool, because two pools may front instances of
// the same name and one release must not stop the other's renewal loop.
func instanceHoldKey(pool client.ObjectKey, instance string) string {
	return "instance/" + pool.String() + "/" + instance
}
