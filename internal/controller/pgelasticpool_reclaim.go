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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/placement"
)

// ReclaimWhenEmptyAnnotation marks a member that scale-in cordoned, so that emptying it
// finishes the job rather than leaving a machine nobody is using.
//
// It exists because a cordoned, drained member is not on its own a member anybody wanted
// deleted: an operator retiring hardware sets the same two fields and expects the machine to
// still be there when the tenants have gone. Only the pool's own scale-in writes this.
const ReclaimWhenEmptyAnnotation = "pgelastic.io/reclaim-when-empty"

// ReclaimWhenEmpty is the only value the mark is read for, so that setting it to anything else
// is not quietly the same as setting it.
const ReclaimWhenEmpty = "true"

// tenantsBoundTo counts every tenant whose binding names this instance, whatever else is true
// of it. Anything that narrows this - by pool, by resolvable class, by phase - narrows what
// counts as "holding data", and this number is the last thing between a plan and a Delete.
func (r *PgElasticPoolReconciler) tenantsBoundTo(
	ctx context.Context,
	namespace, instance string,
) (int, error) {
	tenants := &pgelasticv1alpha1.PgTenantList{}
	if err := r.List(ctx, tenants, client.InNamespace(namespace)); err != nil {
		return 0, err
	}
	held := 0
	for i := range tenants.Items {
		if placement.BoundInstanceFor(&tenants.Items[i]) == instance {
			held++
		}
	}
	return held, nil
}

// retainedSourceOn reports whether any migration is still holding a source database on this
// instance against a rollback.
//
// The question is asked of the migrations rather than of the databases, for the reason
// sourceDropped is: asking PostgreSQL whether a database exists answers "no" about one a later
// migration legitimately recreated under the same name.
func (r *PgElasticPoolReconciler) retainedSourceOn(
	ctx context.Context,
	namespace, instance string,
) (bool, error) {
	migrations := &pgelasticv1alpha1.PgTenantMigrationList{}
	if err := r.List(ctx, migrations, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range migrations.Items {
		status := migrations.Items[i].Status
		if status.SourceInstanceRef == nil || status.SourceInstanceRef.Name != instance {
			continue
		}
		if !sourceDropped(&status) {
			return true, nil
		}
	}
	return false, nil
}

// reclaimDrainedMembers deletes the members scale-in emptied.
//
// It is a convergence step rather than a branch of the scale-in action, and that is the whole
// point of it. Scale-in takes many passes - emit migrations, cordon, wait for the drain - and
// the moment it cordons its victim, that member stops counting as capacity. The utilization
// over what is left goes up, the recommendation goes up with it, and the plan stops proposing
// scale-in at all. Reached only from the action, the reclaim would run in exactly the
// situation it was written for and in no other: the victim sits cordoned, empty and abandoned,
// which is the defect this was supposed to close.
//
// So the trigger is the state of the fleet, not what the planner happens to propose this pass.
func (r *PgElasticPoolReconciler) reclaimDrainedMembers(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	view *poolView,
) (bool, error) {
	for i := range view.instances {
		instance := &view.instances[i]
		if !instance.DeletionTimestamp.IsZero() ||
			instance.Annotations[ReclaimWhenEmptyAnnotation] != ReclaimWhenEmpty {
			continue
		}
		// Only what the pool made. An instance somebody else created is somebody else's to
		// delete, and every member here is a primary's worth of tenant data with no replica
		// of it anywhere - this is not a Deployment reclaiming a Pod.
		if owner := metav1.GetControllerOf(instance); owner == nil || owner.UID != pool.UID {
			continue
		}
		if !cordoned(instance) {
			continue
		}
		// Emptiness is counted from the tenants themselves, unfiltered, rather than read off
		// the instance. PgInstanceStatus.Tenants looks like the answer and is written by
		// nothing, so a guard reading it always says "empty" - on a Delete against a primary
		// holding a couple of hundred databases. The pool's own view is no good here either:
		// it drops any tenant whose class will not resolve, and a tenant nobody can price is
		// still a tenant whose data is on this machine.
		held, err := r.tenantsBoundTo(ctx, pool.Namespace, instance.Name)
		if err != nil {
			return false, err
		}
		if held > 0 {
			continue
		}
		// A tenant that has just been migrated off is no longer bound here, and its database
		// is still here. Cutover rewrites the binding to the target and deliberately leaves
		// the source database intact and refusing connections for the rollback window - an
		// hour by default - so that a move which turns out to have lost something can be
		// flipped back without restoring a backup. Deleting the member takes its volumes with
		// it, and takes that window away from every tenant the drain just moved.
		retained, err := r.retainedSourceOn(ctx, pool.Namespace, instance.Name)
		if err != nil {
			return false, err
		}
		if retained {
			continue
		}
		// The victim was marked many passes ago, and a name can come to mean a different
		// object in that time - one somebody recreated by hand, holding data nobody asked to
		// lose. The delete carries the identity and the version of the object that was
		// actually emptied.
		if err := r.Delete(ctx, instance, client.Preconditions{
			UID:             &instance.UID,
			ResourceVersion: &instance.ResourceVersion,
		}); err != nil {
			// A conflict means the object moved on under the name, which is what the
			// preconditions are for: leave it and look again next pass.
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				continue
			}
			return false, err
		}
		logf.FromContext(ctx).Info("Reclaimed the member scale-in emptied", "instance", instance.Name)
		return true, nil
	}
	return false, nil
}
