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

// Package ownership answers which pgelastic objects an operator process may write to.
//
// PgElasticClass.spec.controllerName is the only claim of ownership the API carries, and
// it is stated on the class alone. Every other kind inherits the claim by reference: a
// pool from its class, an instance and a tenant from their pool, a migration from its
// tenant. Walking that chain is what lets two pgelastic operators share one cluster, and
// not walking it is what makes them rewrite each other's proxy Deployment forever.
package ownership

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// DefaultControllerName is the controllerName a PgElasticClass must carry for an operator
// started without an identity of its own to claim it.
const DefaultControllerName = "pgelastic.io/elastic-pool-controller"

// RetryUnresolved is how long an object whose governing class could not be resolved waits
// before the question is asked again. It is the whole recovery mechanism for a dangling
// reference, and it writes nothing to the object, so it can be slow.
const RetryUnresolved = 30 * time.Second

// Verdict is the answer to "may this operator write to this object?".
type Verdict int

const (
	// Foreign means the governing class resolved and named another controller. Because
	// controllerName is immutable, that answer cannot change while the class exists, so
	// there is nothing to come back for.
	Foreign Verdict = iota

	// Unresolved means the walk from the object to a class hit a reference that does not
	// resolve: a pool naming a class that is absent, an instance or tenant naming a pool
	// that is absent, a migration naming a tenant that is absent.
	//
	// It is deliberately not Mine. An object that cannot be proven to belong to this
	// operator is one that every other operator on the cluster also cannot prove belongs
	// to it, so defaulting to Mine means every operator claims it and they fight over it
	// forever - which is the failure this package exists to remove. The opposite risk, an
	// object nobody reconciles, is recovered by creating the missing parent, at which
	// point RetryUnresolved brings it back on its own.
	Unresolved

	// Mine means the governing class named this operator.
	Mine
)

// Resolver walks an object back to the PgElasticClass that governs it and compares that
// class's controllerName against this operator's identity.
type Resolver struct {
	// Reader resolves the chain. An informer cache is the right reader: every hop is a Get
	// by name rather than a filtered List, so no field index is involved, and a verdict one
	// watch event out of date is corrected by the event that delivers the object.
	Reader client.Reader

	// ControllerName is this operator's identity. Empty means DefaultControllerName.
	ControllerName string
}

// Name is the identity this resolver compares classes against.
func (r Resolver) Name() string {
	if r.ControllerName == "" {
		return DefaultControllerName
	}
	return r.ControllerName
}

// ClaimsClass reports whether a class names this operator. It takes the class itself
// because the class is the one kind that carries the claim rather than inheriting it.
func (r Resolver) ClaimsClass(class *pgelasticv1alpha1.PgElasticClass) bool {
	return class.Spec.ControllerName == r.Name()
}

// Of resolves who governs one object. An error is a failed read and nothing else: an
// absent referent is reported as Unresolved rather than as a failure, because a dangling
// reference is a state of the cluster and not a fault of this operator.
func (r Resolver) Of(ctx context.Context, object client.Object) (Verdict, error) {
	switch typed := object.(type) {
	case *pgelasticv1alpha1.PgElasticClass:
		if r.ClaimsClass(typed) {
			return Mine, nil
		}
		return Foreign, nil
	case *pgelasticv1alpha1.PgElasticPool:
		return r.ofPool(ctx, typed)
	case *pgelasticv1alpha1.PgInstance:
		return r.ofPoolNamed(ctx, typed.Namespace, typed.Spec.PoolRef.Name)
	case *pgelasticv1alpha1.PgTenant:
		return r.ofPoolNamed(ctx, typed.Namespace, typed.Spec.PoolRef.Name)
	case *pgelasticv1alpha1.PgTenantMigration:
		return r.ofMigration(ctx, typed)
	default:
		return Foreign, fmt.Errorf("ownership: %T carries no route to a PgElasticClass", object)
	}
}

func (r Resolver) ofPool(ctx context.Context, pool *pgelasticv1alpha1.PgElasticPool) (Verdict, error) {
	if pool.Spec.ClassRef.Name == "" {
		return Foreign, nil
	}
	class := &pgelasticv1alpha1.PgElasticClass{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: pool.Spec.ClassRef.Name}, class); err != nil {
		if apierrors.IsNotFound(err) {
			return Unresolved, nil
		}
		return Foreign, err
	}
	if r.ClaimsClass(class) {
		return Mine, nil
	}
	return Foreign, nil
}

func (r Resolver) ofPoolNamed(ctx context.Context, namespace, name string) (Verdict, error) {
	if name == "" {
		return Foreign, nil
	}
	pool := &pgelasticv1alpha1.PgElasticPool{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return Unresolved, nil
		}
		return Foreign, err
	}
	return r.ofPool(ctx, pool)
}

func (r Resolver) ofMigration(
	ctx context.Context,
	object *pgelasticv1alpha1.PgTenantMigration,
) (Verdict, error) {
	if object.Spec.TenantRef.Name == "" {
		return Foreign, nil
	}
	tenant := &pgelasticv1alpha1.PgTenant{}
	key := types.NamespacedName{Namespace: object.Namespace, Name: object.Spec.TenantRef.Name}
	if err := r.Reader.Get(ctx, key, tenant); err != nil {
		if apierrors.IsNotFound(err) {
			return Unresolved, nil
		}
		return Foreign, err
	}
	return r.ofPoolNamed(ctx, tenant.Namespace, tenant.Spec.PoolRef.Name)
}
