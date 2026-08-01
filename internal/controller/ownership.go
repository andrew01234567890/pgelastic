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
	"slices"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/andrew01234567890/pgelastic/internal/ownership"
)

// DefaultControllerName is the controllerName a PgElasticClass must carry for this
// controller to claim it.
const DefaultControllerName = ownership.DefaultControllerName

// unclaimed reports the result a reconciler should hand straight back when the object it
// was handed is not this controller's to write to, and whether it should hand it back.
//
// Not ours means untouched: no status, no conditions, no events, no finalizer. Writing to
// a foreign object is the entire failure mode this guard exists to prevent, and a
// condition saying "not mine" is still a write, still bumps resourceVersion, and still
// starts the fight with whichever operator does own the object.
//
// An object under deletion is the one exception, and only for Unresolved. What happens then
// is the caller's to state, because it depends on what that reconciler's finalize does.
//
// Foreign is not excepted. That verdict resolved, it named someone else, and that someone is
// still there to do the finalizing.
func unclaimed(
	ctx context.Context,
	resolver ownership.Resolver,
	writer client.Writer,
	policy orphanPolicy,
	object client.Object,
) (ctrl.Result, bool, error) {
	verdict, err := resolver.Of(ctx, object)
	switch {
	case err != nil:
		return ctrl.Result{}, true, err
	case verdict == ownership.Mine:
		return ctrl.Result{}, false, nil
	case verdict == ownership.Unresolved && !object.GetDeletionTimestamp().IsZero():
		if policy == finalizeAnyway {
			return ctrl.Result{}, false, nil
		}
		return ctrl.Result{}, true, releaseOrphan(ctx, writer, object)
	case verdict == ownership.Unresolved:
		logUnclaimed(ctx, object, "the class governing it does not resolve")
		return ctrl.Result{RequeueAfter: ownership.RetryUnresolved}, true, nil
	default:
		logUnclaimed(ctx, object, "the class governing it names another controller")
		return ctrl.Result{}, true, nil
	}
}

// orphanPolicy is what a reconciler wants done with an object it is deleting that can no
// longer be resolved back to a class. The two kinds of finalizer in this operator want
// opposite things, so each caller says which it holds.
type orphanPolicy int

const (
	// releaseOnly takes this operator's finalizers off and runs nothing else. For a finalizer
	// that exists to trigger cleanup: with the parent gone there is no way to prove the
	// object is this operator's, and cleanup acts on the world.
	releaseOnly orphanPolicy = iota

	// finalizeAnyway runs the reconciler's own deletion branch. For a finalizer that encodes
	// a refusal rather than a cleanup - the drain guard is there to stop an instance being
	// deleted out from under the tenants whose volumes it owns, and that has to hold whether
	// or not a class resolves. Only for deletion branches that touch nothing but the object
	// itself, which is why the destructive two do not use it.
	finalizeAnyway
)

// ourFinalizers is every finalizer this operator adds whose purpose is to trigger its own
// cleanup. The drain guard is deliberately absent: it is a refusal, and releasing it here
// would delete an instance out from under bound tenants and garbage-collect their volumes
// with it. Listed rather than matched on the pgelastic.io/ prefix, because that prefix is
// also used for label keys and for a finalizer the tests hold objects with.
var ourFinalizers = []string{
	TenantDatabaseFinalizer,
	RecoveryInstanceFinalizer,
}

// releaseOrphan takes this operator's finalizers off an object it cannot prove is its own,
// and does nothing else to it.
//
// Every kind but PgElasticClass inherits its claim from a parent, so deleting a namespace -
// which deletes parent and children at once - can leave a child whose parent has already
// gone. Refusing to touch it means refusing to release the finalizer this operator itself
// added, and nothing will ever resolve that: the parent is not coming back, no other operator
// can prove the object is theirs either, and the namespace stays Terminating for good.
//
// What it deliberately does not do is run the reconciler's own finalize path. That path acts
// on the world - dropping a tenant's database with FORCE, deleting a recovery instance and
// its volumes - and the verdict that led here says this operator cannot prove the object is
// its own. Two operators sharing a cluster both see Unresolved, so both would act, each
// exec'ing into the other's Pods. Leaving a database behind is recoverable by hand; dropping
// somebody else's is not.
//
// Skipping it also avoids trading one deadlock for another. Reclaiming needs a live
// PostgreSQL to exec into, and the situation that orphans a tenant is usually namespace
// deletion, where the Pods are going away at the same moment. finalize would fail, keep the
// finalizer and retry for ever, while the instance's own drain finalizer waits on the tenant
// that is stuck.
func releaseOrphan(ctx context.Context, writer client.Writer, object client.Object) error {
	held := object.GetFinalizers()
	kept := make([]string, 0, len(held))
	for _, finalizer := range held {
		if !slices.Contains(ourFinalizers, finalizer) {
			kept = append(kept, finalizer)
		}
	}
	if len(kept) == len(held) {
		return nil
	}

	logf.FromContext(ctx).Info(
		"Releasing an object whose governing class does not resolve, without reclaiming: "+
			"nothing can prove it belongs to this operator, and nothing else will free it",
		"object", client.ObjectKeyFromObject(object))
	object.SetFinalizers(kept)
	return writer.Update(ctx, object)
}

func logUnclaimed(ctx context.Context, object client.Object, why string) {
	logf.FromContext(ctx).V(1).Info("Leaving object untouched: "+why,
		"object", client.ObjectKeyFromObject(object))
}
