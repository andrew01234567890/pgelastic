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
func unclaimed(
	ctx context.Context,
	resolver ownership.Resolver,
	object client.Object,
) (ctrl.Result, bool, error) {
	verdict, err := resolver.Of(ctx, object)
	switch {
	case err != nil:
		return ctrl.Result{}, true, err
	case verdict == ownership.Mine:
		return ctrl.Result{}, false, nil
	case verdict == ownership.Unresolved:
		logUnclaimed(ctx, object, "the class governing it does not resolve")
		return ctrl.Result{RequeueAfter: ownership.RetryUnresolved}, true, nil
	default:
		logUnclaimed(ctx, object, "the class governing it names another controller")
		return ctrl.Result{}, true, nil
	}
}

func logUnclaimed(ctx context.Context, object client.Object, why string) {
	logf.FromContext(ctx).V(1).Info("Leaving object untouched: "+why,
		"object", client.ObjectKeyFromObject(object))
}
