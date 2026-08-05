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

package v1alpha1

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// SetupPgTenantWebhookWithManager registers the webhook for PgTenant in the manager.
func SetupPgTenantWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &pgelasticv1alpha1.PgTenant{}).
		WithValidator(&PgTenantCustomValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-pgelastic-io-v1alpha1-pgtenant,mutating=false,failurePolicy=fail,sideEffects=None,groups=pgelastic.io,resources=pgtenants,verbs=create;update,versions=v1alpha1,name=vpgtenant-v1alpha1.kb.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// PgTenantCustomValidator enforces the tenant admission rules that need a cluster read:
// the reservation ledger, bidirectional namespace consent, and workload class membership.
//
// It reads through the manager's uncached API reader. A ledger decided from a cache is
// decided from the past, and two tenants admitted against the same stale figure would
// together over-commit the pool the invariant exists to protect.
type PgTenantCustomValidator struct {
	Reader client.Reader
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type PgTenant.
func (v *PgTenantCustomValidator) ValidateCreate(
	ctx context.Context,
	obj *pgelasticv1alpha1.PgTenant,
) (admission.Warnings, error) {
	return nil, v.validate(ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type PgTenant.
func (v *PgTenantCustomValidator) ValidateUpdate(
	ctx context.Context,
	_, newObj *pgelasticv1alpha1.PgTenant,
) (admission.Warnings, error) {
	// A tenant being deleted is not admitted against its pool, because the only writes left
	// are the controller clearing its own finalizer. Deleting a namespace removes the pool
	// and the tenant together, and validating the finalizer removal against a pool that is
	// already gone rejects it for ever: the tenant keeps its finalizer, and the namespace
	// never leaves Terminating.
	if newObj.DeletionTimestamp != nil {
		return nil, nil
	}
	return nil, v.validate(ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type PgTenant.
func (v *PgTenantCustomValidator) ValidateDelete(
	_ context.Context,
	_ *pgelasticv1alpha1.PgTenant,
) (admission.Warnings, error) {
	return nil, nil
}

func (v *PgTenantCustomValidator) validate(ctx context.Context, tenant *pgelasticv1alpha1.PgTenant) error {
	resolver := policy.Resolver{Reader: v.Reader}
	poolRefPath := field.NewPath("spec", "poolRef", "name")

	pool := &pgelasticv1alpha1.PgElasticPool{}
	poolKey := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Spec.PoolRef.Name}
	if err := v.Reader.Get(ctx, poolKey, pool); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Admitting a tenant whose pool does not exist would admit it past both consent
		// gates and past the reservation ledger, because all three live on the pool.
		return invalid(tenant, field.ErrorList{field.Invalid(poolRefPath, tenant.Spec.PoolRef.Name,
			fmt.Sprintf("no PgElasticPool of that name exists in namespace %q", tenant.Namespace))})
	}

	elasticClass, err := resolver.ElasticClassFor(ctx, pool)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return invalid(tenant, field.ErrorList{field.Invalid(poolRefPath, tenant.Spec.PoolRef.Name,
			fmt.Sprintf("pool %q is bound to PgElasticClass %q, which does not exist, so its admission "+
				"policy cannot be read", pool.Name, pool.Spec.ClassRef.Name))})
	}

	problems, err := v.consentProblems(ctx, resolver, tenant, pool, elasticClass)
	if err != nil {
		return err
	}

	claimed, err := v.databaseNameProblems(ctx, tenant)
	if err != nil {
		return err
	}
	problems = append(problems, claimed...)

	workloadClass, membership, err := v.workloadClassProblems(ctx, resolver, tenant, pool, elasticClass)
	if err != nil {
		return err
	}
	problems = append(problems, membership...)

	if workloadClass != nil {
		reservation, err := v.reservationProblems(ctx, resolver, tenant, pool, elasticClass, workloadClass)
		if err != nil {
			return err
		}
		problems = append(problems, reservation...)
	}

	return invalid(tenant, problems)
}

// databaseNameProblems refuses a database name another tenant of the same pool already claims.
//
// The name is not a label. tenantdb treats a database that already exists under it as this
// tenant's own and skips creation, and then grants this tenant's backend role CONNECT on it -
// so a second tenant naming an existing database is admitted straight into somebody else's,
// with a role of its own to reach it by. On separate instances it goes wrong differently: the
// rendered document keys its routing table on the name, and two entries under one key is a TOML
// document no replica can parse.
//
// Checked against the uncached reader, and still only best effort - two creates racing each
// other each read a cluster without the other. PostgreSQL is no backstop here, because adopting
// an existing database is exactly what it lets happen, so the reconciler refuses the loser too
// and the renderer drops the duplicate. This turns the ordinary case into an admission error
// naming both objects.
func (v *PgTenantCustomValidator) databaseNameProblems(
	ctx context.Context,
	tenant *pgelasticv1alpha1.PgTenant,
) (field.ErrorList, error) {
	siblings := &pgelasticv1alpha1.PgTenantList{}
	if err := v.Reader.List(ctx, siblings, client.InNamespace(tenant.Namespace)); err != nil {
		return nil, err
	}
	var problems field.ErrorList
	for i := range siblings.Items {
		sibling := &siblings.Items[i]
		// A sibling on its way out holds no claim, and the reconciler's own version of this
		// rule says so. Counting one here refuses every update to the tenant that is
		// *keeping* the database - including, while a duplicate is reclaiming, ordinary spec
		// edits that have nothing to do with the name - because this validator runs on update
		// as well as create.
		if sibling.Name == tenant.Name ||
			!sibling.DeletionTimestamp.IsZero() ||
			sibling.Spec.PoolRef.Name != tenant.Spec.PoolRef.Name ||
			sibling.Spec.DatabaseName != tenant.Spec.DatabaseName {
			continue
		}
		problems = append(problems, field.Duplicate(
			field.NewPath("spec", "databaseName"),
			fmt.Sprintf("PgTenant %q of pool %q already holds database %q",
				sibling.Name, tenant.Spec.PoolRef.Name, tenant.Spec.DatabaseName)))
	}
	return problems, nil
}

// consentProblems enforces consent in both directions. A tenant naming a pool is not
// consent: the pool and the policy class governing it must each admit the namespace the
// tenant is in, or naming an object would be enough to use it.
func (v *PgTenantCustomValidator) consentProblems(
	ctx context.Context,
	resolver policy.Resolver,
	tenant *pgelasticv1alpha1.PgTenant,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
) (field.ErrorList, error) {
	problems := field.ErrorList{}
	namespacePath := field.NewPath("metadata", "namespace")

	var poolRule *pgelasticv1alpha1.NamespaceAdmission
	if pool.Spec.Admission != nil {
		poolRule = pool.Spec.Admission.AdmittedNamespaces
	}
	admitted, err := resolver.NamespaceAdmits(ctx, poolRule, pool.Namespace, tenant.Namespace)
	if err != nil {
		return nil, err
	}
	if !admitted {
		problems = append(problems, field.Forbidden(namespacePath, fmt.Sprintf(
			"PgElasticPool %q does not admit namespace %q through spec.admission.admittedNamespaces",
			pool.Name, tenant.Namespace)))
	}

	admitted, err = resolver.NamespaceAdmits(
		ctx, elasticClass.Spec.AdmittedNamespaces, pool.Namespace, tenant.Namespace)
	if err != nil {
		return nil, err
	}
	if !admitted {
		problems = append(problems, field.Forbidden(namespacePath, fmt.Sprintf(
			"PgElasticClass %q does not admit namespace %q through spec.admittedNamespaces",
			elasticClass.Name, tenant.Namespace)))
	}

	return problems, nil
}

func (v *PgTenantCustomValidator) workloadClassProblems(
	ctx context.Context,
	resolver policy.Resolver,
	tenant *pgelasticv1alpha1.PgTenant,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
) (*pgelasticv1alpha1.PgWorkloadClass, field.ErrorList, error) {
	path := field.NewPath("spec", "workloadClassName")

	name, err := resolver.WorkloadClassNameFor(ctx, tenant, pool, elasticClass)
	if err != nil {
		return nil, field.ErrorList{field.Required(path, err.Error())}, nil
	}

	if allowed := policy.AllowedWorkloadClassNames(pool, elasticClass); len(allowed) > 0 &&
		!slices.Contains(allowed, name) {
		return nil, field.ErrorList{field.NotSupported(path, name, allowed)}, nil
	}

	workloadClass := &pgelasticv1alpha1.PgWorkloadClass{}
	if err := v.Reader.Get(ctx, types.NamespacedName{Name: name}, workloadClass); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, nil, err
		}
		return nil, field.ErrorList{field.Invalid(path, name, "no PgWorkloadClass of that name exists")}, nil
	}
	return workloadClass, nil, nil
}

// reservationProblems is the reservation ledger, plus the cross-replica budget this tenant
// would join. A guarantee that does not fit the pool's remaining allocatable capacity is
// refused outright rather than quietly cut down to what is left, because a guarantee that
// can be reduced without telling anyone is not one.
//
// The ledger is now resolved even for a tenant with no guarantee at all, which the guarantee
// check alone would have skipped: a BestEffort tenant still raises the pool's committed
// burst, and the fleet-wide worst case is the sum of every ceiling times the replica count.
// Checking it only when a pool is edited would leave the invariant broken by every tenant
// added afterwards.
func (v *PgTenantCustomValidator) reservationProblems(
	ctx context.Context,
	resolver policy.Resolver,
	tenant *pgelasticv1alpha1.PgTenant,
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
	workloadClass *pgelasticv1alpha1.PgWorkloadClass,
) (field.ErrorList, error) {
	effective := policy.EffectiveFor(tenant, workloadClass)
	ledger, err := resolver.LedgerFor(ctx, pool, elasticClass, tenant.Name)
	if err != nil {
		return nil, err
	}

	problems := field.ErrorList{}
	// A floor above its own ceiling is not a capacity question, it is an incoherent tenant.
	// The class and the override merge field by field, so raising guaranteed alone - or
	// lowering burstable alone - produces one, and it is the proxy that finds out: the
	// allocator refuses the claim when it loads the document, and the refusal lands on a
	// pool-wide reload rather than on the object that caused it.
	if effective.Guaranteed > effective.Burstable {
		path := field.NewPath("spec", "workloadClassName")
		switch {
		case tenant.Spec.Capacity != nil && tenant.Spec.Capacity.Guaranteed != nil:
			path = field.NewPath("spec", "capacity", "guaranteed")
		case tenant.Spec.Capacity != nil && tenant.Spec.Capacity.Burstable != nil:
			path = field.NewPath("spec", "capacity", "burstable")
		}
		problems = append(problems, field.Invalid(path, effective.Guaranteed, fmt.Sprintf(
			"a guarantee of %d is above the burstable ceiling of %d, so the tenant is "+
				"guaranteed more than it is ever allowed to hold; PgWorkloadClass %q supplies "+
				"whichever of the two spec.capacity does not override",
			effective.Guaranteed, effective.Burstable, workloadClass.Name)))
	}
	if effective.Guaranteed > 0 && effective.Guaranteed > ledger.Available {
		path := field.NewPath("spec", "workloadClassName")
		if tenant.Spec.Capacity != nil && tenant.Spec.Capacity.Guaranteed != nil {
			path = field.NewPath("spec", "capacity", "guaranteed")
		}
		problems = append(problems, field.Forbidden(path, fmt.Sprintf(
			"a guarantee of %d backend connections does not fit PgElasticPool %q: allocatable %d "+
				"(backendConnections %d less %d%% headroom), reserved %d by %d tenant(s), available %d",
			effective.Guaranteed, pool.Name, ledger.Allocatable, ledger.BackendConnections,
			ledger.HeadroomPercent, ledger.Reserved, ledger.Tenants, ledger.Available)))
	}

	// The ledger excludes this tenant so its previous numbers are not double-counted, so its
	// own ceiling has to be folded back in before the worst case means anything.
	joined := ledger
	joined.CommittedBurst += effective.Burstable
	joined.Tenants++
	burstPath := field.NewPath("spec", "workloadClassName")
	if tenant.Spec.Capacity != nil && tenant.Spec.Capacity.Burstable != nil {
		burstPath = field.NewPath("spec", "capacity", "burstable")
	}
	return append(problems, replicaBudgetProblems(pool, joined, burstPath)...), nil
}
