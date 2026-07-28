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

package migration

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// TenantRef names a namespaced object.
type TenantRef struct {
	Namespace string
	Name      string
}

func (t TenantRef) String() string { return t.Namespace + "/" + t.Name }

// Router is the proxy-facing port of the cutover.
//
// The choreography only a proxy can perform is the product's whole differentiator: new
// transactions are queued and the client sockets are held, rather than the connections
// being dropped and the client left to retry. The port is narrow on purpose - the routing
// table and the quiesce flag are control-plane state, and it is the proxy that turns them
// into held sockets.
type Router interface {
	// Quiesce queues new transactions for the tenant and holds its client sockets open.
	Quiesce(ctx context.Context, tenant TenantRef, migration string) error
	// PreWarm opens and warms backend connections to an instance, so the cutover pause is
	// not spent establishing them.
	PreWarm(ctx context.Context, tenant TenantRef, instance string) error
	// Route points the tenant's routing entry at an instance. It is called exactly once per
	// direction: forward at the end of Cutover, back only on a rollback.
	Route(ctx context.Context, tenant TenantRef, instance string) error
	// Release lets queued clients through against whatever instance they are routed to.
	Release(ctx context.Context, tenant TenantRef) error
	// RoutedTo reports the instance the routing table names right now.
	RoutedTo(ctx context.Context, tenant TenantRef) (string, error)
}

// AnnotationQuiescedBy names the migration currently holding a tenant's clients. It is on
// the PgTenant rather than on the migration because the proxy watches tenants, and because
// a second migration of the same tenant must be able to see that the first one has the
// sockets.
const AnnotationQuiescedBy = "pgelastic.io/quiescedBy"

// AnnotationPreWarmTarget asks the proxy to open backend connections to an instance the
// tenant is not yet routed to.
const AnnotationPreWarmTarget = "pgelastic.io/preWarmInstance"

// BindingRouter drives the routing table of record: the tenant's status binding, which is
// what the proxy resolves a tenant to an instance through.
type BindingRouter struct {
	Client client.Client
	// Reader is used for every read, and should be an uncached one. The safety property that
	// an abort restores the source rests on knowing where the tenant is routed *now*: an
	// informer cache that has not caught up with a flip this same controller performed a
	// moment ago would report the tenant as already on the source and skip the restore.
	Reader client.Reader
}

var _ Router = BindingRouter{}

// Quiesce marks the tenant as held by one migration.
func (r BindingRouter) Quiesce(ctx context.Context, tenant TenantRef, migration string) error {
	return r.annotate(ctx, tenant, map[string]string{AnnotationQuiescedBy: migration})
}

// PreWarm publishes the instance whose backends should be opened ahead of the flip.
func (r BindingRouter) PreWarm(ctx context.Context, tenant TenantRef, instance string) error {
	return r.annotate(ctx, tenant, map[string]string{AnnotationPreWarmTarget: instance})
}

// Release clears the quiesce and the pre-warm hint together. Leaving either behind would
// keep a finished migration holding a tenant's clients, which turns a move into an outage.
func (r BindingRouter) Release(ctx context.Context, tenant TenantRef) error {
	return r.annotate(ctx, tenant, map[string]string{AnnotationQuiescedBy: "", AnnotationPreWarmTarget: ""})
}

// Route rewrites the binding. It is a status write because the binding is observed state:
// the tenant's spec never names an instance.
//
// It retries on conflict, and that is not defensive tidiness. This write happens inside the
// cutover pause, while the tenant controller is reconciling the same object; losing the race
// once would fail a migration that had already done everything correctly, and would do it at
// the one moment when the tenant's clients are queued.
func (r BindingRouter) Route(ctx context.Context, tenant TenantRef, instance string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		object, err := r.get(ctx, tenant)
		if err != nil {
			return err
		}
		if object.Status.Binding == nil {
			object.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{}
		}
		object.Status.Binding.InstanceRef = &corev1.LocalObjectReference{Name: instance}
		object.Status.Binding.BoundAt = ptr.To(metav1.Now())
		return r.Client.Status().Update(ctx, object)
	})
}

// RoutedTo reads the binding back.
func (r BindingRouter) RoutedTo(ctx context.Context, tenant TenantRef) (string, error) {
	object, err := r.get(ctx, tenant)
	if err != nil {
		return "", err
	}
	if object.Status.Binding == nil || object.Status.Binding.InstanceRef == nil {
		return "", nil
	}
	return object.Status.Binding.InstanceRef.Name, nil
}

func (r BindingRouter) reader() client.Reader {
	if r.Reader != nil {
		return r.Reader
	}
	return r.Client
}

func (r BindingRouter) get(ctx context.Context, tenant TenantRef) (*pgelasticv1alpha1.PgTenant, error) {
	object := &pgelasticv1alpha1.PgTenant{}
	key := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Name}
	if err := r.reader().Get(ctx, key, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("PgTenant %s does not exist: %w", tenant, err)
		}
		return nil, err
	}
	return object, nil
}

func (r BindingRouter) annotate(ctx context.Context, tenant TenantRef, values map[string]string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.annotateOnce(ctx, tenant, values)
	})
}

func (r BindingRouter) annotateOnce(ctx context.Context, tenant TenantRef, values map[string]string) error {
	object, err := r.get(ctx, tenant)
	if err != nil {
		return err
	}
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	changed := false
	for key, value := range values {
		switch {
		case value == "" && annotations[key] != "":
			delete(annotations, key)
			changed = true
		case value != "" && annotations[key] != value:
			annotations[key] = value
			changed = true
		}
	}
	if !changed {
		return nil
	}
	object.SetAnnotations(annotations)
	return r.Client.Update(ctx, object)
}
