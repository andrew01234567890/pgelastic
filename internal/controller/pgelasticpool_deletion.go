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

package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/placement"
)

// PgElasticPoolMembersFinalizer holds a pool open until the members it made are safe to
// reclaim.
const PgElasticPoolMembersFinalizer = "pgelastic.io/pool-members"

// eventPoolDeletionHeld names the refusal, so "why is my pool stuck in Terminating" is
// answered by kubectl describe rather than by reading this file.
const eventPoolDeletionHeld = "PoolDeletionHeld"

// holdPoolForMembers persists the finalizer before the pool can make anything.
//
// It has to be written before the first member exists, for the same reason the tenant's does:
// a finalizer added after the object it protects would leave a window in which deleting the
// pool takes the members with it and leaves no record that there was anything to protect.
func (r *PgElasticPoolReconciler) holdPoolForMembers(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) error {
	if !controllerutil.AddFinalizer(pool, PgElasticPoolMembersFinalizer) {
		return nil
	}
	return r.Update(ctx, pool)
}

// finalizePool decides what deleting a pool means for the members it made.
//
// Members carry the pool's ownerReference, so without this the answer is "everything, at
// once, silently": garbage collection reclaims every member the moment the pool is gone, and
// a member here is not a replica of anything - it is a primary holding up to a couple of
// hundred tenants' databases, with no copy elsewhere. `kubectl delete pgelasticpool` is one
// command and one tab-completion away from being the most destructive thing in the product.
//
// So the pool is held open while any member it made still holds a tenant, and it says which
// members and how many tenants. That is a refusal an operator can act on: move the tenants,
// or delete them, and the pool completes on its own. Adopted members are not counted, because
// they are not the pool's to reclaim and deleting the pool never touched them.
//
// The hold is on the pool object rather than on the members, deliberately. A finalizer chain
// that reaches down into every member is the shape that leaves a namespace Terminating for
// ever when one link cannot be released; this one is a single object whose condition is
// re-evaluated every pass and which releases as soon as the tenants are gone.
func (r *PgElasticPoolReconciler) finalizePool(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) (ctrl.Result, error) {
	if r.Metering != nil {
		r.Metering.ForgetPool(pool.Namespace, pool.Name)
	}
	if !controllerutil.ContainsFinalizer(pool, PgElasticPoolMembersFinalizer) {
		return ctrl.Result{}, nil
	}

	members := &pgelasticv1alpha1.PgInstanceList{}
	if err := r.List(ctx, members, client.InNamespace(pool.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	tenants := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, tenants, client.InNamespace(pool.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	if occupied := occupiedMembersOf(pool, members.Items, tenants.Items); len(occupied) > 0 {
		r.event(pool, corev1.EventTypeWarning, eventPoolDeletionHeld, actionHold,
			"%s", heldMessage(occupied))
		return ctrl.Result{RequeueAfter: poolResyncInterval}, nil
	}

	controllerutil.RemoveFinalizer(pool, PgElasticPoolMembersFinalizer)
	return ctrl.Result{}, r.Update(ctx, pool)
}

// occupiedMember is one member the pool made that is still holding tenants.
type occupiedMember struct {
	name    string
	tenants int32
}

// occupiedMembersOf is the members the pool made that still hold tenants.
//
// The count comes from the tenants' own bindings rather than from PgInstanceStatus.Tenants,
// which looks like exactly this number and is written by nothing: a hold reading it would
// find every member empty and let every pool delete straight through, which is the whole of
// what this refuses to do. Nothing narrows the tenant list either - not by pool, not by
// whether a class resolves - because a tenant nobody can price still has a database here.
//
// A member somebody else wrote is not counted: deleting the pool does not delete it, so it
// cannot be a reason to refuse.
func occupiedMembersOf(
	pool *pgelasticv1alpha1.PgElasticPool,
	instances []pgelasticv1alpha1.PgInstance,
	tenants []pgelasticv1alpha1.PgTenant,
) []occupiedMember {
	held := map[string]int32{}
	for i := range tenants {
		if bound := placement.BoundInstanceFor(&tenants[i]); bound != "" {
			held[bound]++
		}
	}

	occupied := make([]occupiedMember, 0, len(instances))
	for i := range instances {
		instance := &instances[i]
		if instance.Spec.PoolRef.Name != pool.Name || held[instance.Name] == 0 {
			continue
		}
		if owner := metav1.GetControllerOf(instance); owner == nil || owner.UID != pool.UID {
			continue
		}
		occupied = append(occupied, occupiedMember{name: instance.Name, tenants: held[instance.Name]})
	}
	slices.SortFunc(occupied, func(a, b occupiedMember) int { return strings.Compare(a.name, b.name) })
	return occupied
}

func heldMessage(occupied []occupiedMember) string {
	held := make([]string, 0, len(occupied))
	total := int32(0)
	for _, member := range occupied {
		held = append(held, fmt.Sprintf("%s (%d)", member.name, member.tenants))
		total += member.tenants
	}
	return fmt.Sprintf(
		"deletion is held: %d tenants still have databases on members this pool provisioned - %s. "+
			"Move or delete them and the pool completes on its own",
		total, strings.Join(held, ", "))
}
