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

package policy

import (
	"context"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// Resolver answers policy questions that need a cluster read.
//
// Every List it issues is filtered in Go rather than through a field index, because the
// validating webhook resolves through the same code against a live, uncached reader and
// field indexes exist only on an informer cache.
type Resolver struct {
	Reader client.Reader
}

// ErrNoWorkloadClass reports that the workload class ladder produced no name at all:
// the tenant named none, the pool defaults none, its class defaults none, and no
// PgWorkloadClass is marked global.
var ErrNoWorkloadClass = errors.New("no workload class: the tenant names none, no pool or class default applies, and no PgWorkloadClass is global")

// ElasticClassFor returns the PgElasticClass a pool is bound to.
func (r Resolver) ElasticClassFor(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) (*pgelasticv1alpha1.PgElasticClass, error) {
	class := &pgelasticv1alpha1.PgElasticClass{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: pool.Spec.ClassRef.Name}, class); err != nil {
		return nil, err
	}
	return class, nil
}

// GlobalWorkloadClassNames returns the names of every PgWorkloadClass marked global, in
// sorted order. More than one is a cluster-wide conflict the webhook rejects at
// admission and the PgWorkloadClass controller reports as a condition.
func (r Resolver) GlobalWorkloadClassNames(ctx context.Context) ([]string, error) {
	classes := &pgelasticv1alpha1.PgWorkloadClassList{}
	if err := r.Reader.List(ctx, classes); err != nil {
		return nil, err
	}
	names := make([]string, 0, 1)
	for i := range classes.Items {
		if class := &classes.Items[i]; class.Spec.Global != nil && *class.Spec.Global {
			names = append(names, class.Name)
		}
	}
	slices.Sort(names)
	return names, nil
}

// WorkloadClassNameFor resolves the workload class ladder for a tenant: the tenant's own
// choice, then the pool's default, then the pool class's default, then the single global
// class. Either of pool and elasticClass may be nil when the tenant's references do not
// resolve yet; the ladder simply skips the rungs it cannot read.
func (r Resolver) WorkloadClassNameFor(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
) (string, error) {
	if name := tenant.Spec.WorkloadClassName; name != nil && *name != "" {
		return *name, nil
	}
	if name := poolDefaultWorkloadClassName(pool); name != "" {
		return name, nil
	}
	if name := elasticClassDefaultWorkloadClassName(elasticClass); name != "" {
		return name, nil
	}

	globals, err := r.GlobalWorkloadClassNames(ctx)
	if err != nil {
		return "", err
	}
	if len(globals) == 0 {
		return "", ErrNoWorkloadClass
	}
	return globals[0], nil
}

// DefaultWorkloadClassNameFor resolves the class a tenant of this pool inherits when it
// names none of its own.
func (r Resolver) DefaultWorkloadClassNameFor(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
) (string, error) {
	return r.WorkloadClassNameFor(ctx, &pgelasticv1alpha1.PgTenant{}, pool, elasticClass)
}

// AllowedWorkloadClassNames reports the classes a pool's tenants may select, taking the
// pool's own list when it sets one and the pool class's default list otherwise. An empty
// result means every class is allowed.
func AllowedWorkloadClassNames(
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
) []string {
	if pool != nil && pool.Spec.Admission != nil && len(pool.Spec.Admission.AllowedWorkloadClassNames) > 0 {
		return pool.Spec.Admission.AllowedWorkloadClassNames
	}
	if elasticClass != nil && elasticClass.Spec.Defaults != nil && elasticClass.Spec.Defaults.Admission != nil {
		return elasticClass.Spec.Defaults.Admission.AllowedWorkloadClassNames
	}
	return nil
}

// tenantsOfPool lists the tenants that name the pool.
func (r Resolver) tenantsOfPool(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
) ([]pgelasticv1alpha1.PgTenant, error) {
	tenants := &pgelasticv1alpha1.PgTenantList{}
	if err := r.Reader.List(ctx, tenants, client.InNamespace(pool.Namespace)); err != nil {
		return nil, err
	}
	matching := make([]pgelasticv1alpha1.PgTenant, 0, len(tenants.Items))
	for _, tenant := range tenants.Items {
		if tenant.Spec.PoolRef.Name == pool.Name {
			matching = append(matching, tenant)
		}
	}
	return matching, nil
}

// LedgerFor sums the reservations already held against a pool. The tenant named by
// excluding is left out, so a caller admitting or updating that tenant can add its own
// new numbers to a ledger that no longer double-counts its previous ones.
func (r Resolver) LedgerFor(
	ctx context.Context,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
	excluding string,
) (Ledger, error) {
	headroom := HeadroomPercent(pool, elasticClass)
	ledger := Ledger{
		BackendConnections: pool.Spec.Capacity.BackendConnections,
		HeadroomPercent:    headroom,
		Allocatable:        Allocatable(pool.Spec.Capacity.BackendConnections, headroom),
	}
	ledger.settle()

	tenants, err := r.tenantsOfPool(ctx, pool)
	if err != nil {
		return Ledger{}, err
	}

	workloadClasses := map[string]*pgelasticv1alpha1.PgWorkloadClass{}
	for i := range tenants {
		tenant := &tenants[i]
		if tenant.Name == excluding {
			continue
		}
		name, err := r.WorkloadClassNameFor(ctx, tenant, pool, elasticClass)
		if err != nil {
			// A tenant whose class no longer resolves reserves nothing it can be held
			// to, so it must not silently inflate the ledger and squeeze out a tenant
			// whose class does resolve.
			continue
		}
		class, cached := workloadClasses[name]
		if !cached {
			class = &pgelasticv1alpha1.PgWorkloadClass{}
			if err := r.Reader.Get(ctx, types.NamespacedName{Name: name}, class); err != nil {
				if apierrors.IsNotFound(err) {
					workloadClasses[name] = nil
					continue
				}
				return Ledger{}, err
			}
			workloadClasses[name] = class
		}
		if class == nil {
			continue
		}
		ledger.add(EffectiveFor(tenant, class))
	}

	return ledger, nil
}

// NamespaceAdmits reports whether a namespace-admission rule accepts a candidate
// namespace.
//
// owningNamespace is what From: Same is measured against. A PgElasticClass is cluster
// scoped and so has no namespace of its own; for it the owning namespace is the pool's,
// which is what makes Same on a cluster-scoped policy mean "only the namespace that
// already holds the pool" rather than "nothing at all".
func (r Resolver) NamespaceAdmits(
	ctx context.Context,
	rule *pgelasticv1alpha1.NamespaceAdmission,
	owningNamespace, candidate string,
) (bool, error) {
	from := pgelasticv1alpha1.NamespaceFromSame
	if rule != nil && rule.From != "" {
		from = rule.From
	}

	switch from {
	case pgelasticv1alpha1.NamespaceFromAll:
		return true, nil
	case pgelasticv1alpha1.NamespaceFromSame:
		return candidate == owningNamespace, nil
	case pgelasticv1alpha1.NamespaceFromSelector:
		if rule == nil || rule.Selector == nil {
			return false, nil
		}
		selector, err := metav1.LabelSelectorAsSelector(rule.Selector)
		if err != nil {
			return false, err
		}
		namespace := &corev1.Namespace{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Name: candidate}, namespace); err != nil {
			return false, err
		}
		return selector.Matches(labels.Set(namespace.Labels)), nil
	default:
		return false, fmt.Errorf("unknown namespace admission strategy %q", from)
	}
}

func poolDefaultWorkloadClassName(pool *pgelasticv1alpha1.PgElasticPool) string {
	if pool == nil || pool.Spec.Admission == nil {
		return ""
	}
	return pool.Spec.Admission.DefaultWorkloadClassName
}

func elasticClassDefaultWorkloadClassName(class *pgelasticv1alpha1.PgElasticClass) string {
	if class == nil || class.Spec.Defaults == nil || class.Spec.Defaults.Admission == nil {
		return ""
	}
	if name := class.Spec.Defaults.Admission.DefaultWorkloadClassName; name != nil {
		return *name
	}
	return ""
}
