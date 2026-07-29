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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/policy"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
)

// SetupPgElasticPoolWebhookWithManager registers the webhook for PgElasticPool in the manager.
func SetupPgElasticPoolWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &pgelasticv1alpha1.PgElasticPool{}).
		WithValidator(&PgElasticPoolCustomValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-pgelastic-io-v1alpha1-pgelasticpool,mutating=false,failurePolicy=fail,sideEffects=None,groups=pgelastic.io,resources=pgelasticpools,verbs=create;update,versions=v1alpha1,name=vpgelasticpool-v1alpha1.kb.io,admissionReviewVersions=v1

// PgElasticPoolCustomValidator holds the pool side of the reservation invariant. The
// tenant webhook stops a tenant from over-committing a pool; this one stops the pool from
// being shrunk out from under guarantees it has already made.
type PgElasticPoolCustomValidator struct {
	Reader client.Reader
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type PgElasticPool.
func (v *PgElasticPoolCustomValidator) ValidateCreate(
	ctx context.Context,
	obj *pgelasticv1alpha1.PgElasticPool,
) (admission.Warnings, error) {
	return nil, v.validate(ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type PgElasticPool.
func (v *PgElasticPoolCustomValidator) ValidateUpdate(
	ctx context.Context,
	_, newObj *pgelasticv1alpha1.PgElasticPool,
) (admission.Warnings, error) {
	return nil, v.validate(ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type PgElasticPool.
func (v *PgElasticPoolCustomValidator) ValidateDelete(
	_ context.Context,
	_ *pgelasticv1alpha1.PgElasticPool,
) (admission.Warnings, error) {
	return nil, nil
}

func (v *PgElasticPoolCustomValidator) validate(ctx context.Context, pool *pgelasticv1alpha1.PgElasticPool) error {
	resolver := policy.Resolver{Reader: v.Reader}
	classRefPath := field.NewPath("spec", "classRef", "name")

	elasticClass, err := resolver.ElasticClassFor(ctx, pool)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return invalid(pool, field.ErrorList{field.Invalid(classRefPath, pool.Spec.ClassRef.Name,
			"no PgElasticClass of that name exists")})
	}

	problems := field.ErrorList{}

	admitted, err := resolver.NamespaceAdmits(
		ctx, elasticClass.Spec.AdmittedNamespaces, pool.Namespace, pool.Namespace)
	if err != nil {
		return err
	}
	if !admitted {
		problems = append(problems, field.Forbidden(field.NewPath("metadata", "namespace"), fmt.Sprintf(
			"PgElasticClass %q does not admit namespace %q through spec.admittedNamespaces",
			elasticClass.Name, pool.Namespace)))
	}

	problems = append(problems, defaultWorkloadClassProblems(pool, elasticClass)...)

	ledger, err := resolver.LedgerFor(ctx, pool, elasticClass, "")
	if err != nil {
		return err
	}
	if ledger.Reserved > ledger.Allocatable {
		problems = append(problems, field.Forbidden(field.NewPath("spec", "capacity", "backendConnections"),
			fmt.Sprintf("allocatable %d (backendConnections %d less %d%% headroom) is below the %d backend "+
				"connections already guaranteed to %d tenant(s); reduce those guarantees before shrinking the pool",
				ledger.Allocatable, ledger.BackendConnections, ledger.HeadroomPercent,
				ledger.Reserved, ledger.Tenants)))
	}

	problems = append(problems,
		replicaBudgetProblems(pool, ledger, field.NewPath("spec", "proxy", "replicas"))...)

	return invalid(pool, problems)
}

// replicaBudgetProblems is the cross-replica gate.
//
// Every proxy replica reads one configuration document carrying the undivided
// backendConnections budget, so a tenant bursting on N replicas can hold N times its
// ceiling against PostgreSQL. The reservation ledger above does not see this at all: it
// sums guarantees pool-wide and has no idea spec.proxy.replicas exists.
//
// The worst case is replicas x the sum of every tenant's burstable ceiling, because a
// tenant cannot exceed its own ceiling on any one replica. It is compared against the pool's
// declared oversubscription ceiling rather than against allocatable directly, because
// oversubscription is the product: a pool that publishes maxOversubscriptionRatio is saying
// how far past allocatable it is willing to commit, and this is that same statement with
// the fleet size finally counted. A pool that wants the strict reading sets the ratio to 1,
// and the gate then reduces to replicas x burst <= allocatable.
//
// path names the field on the object under admission: the pool's fleet size when a pool is
// being changed, and the tenant's own ceiling when a tenant is being added. The same
// invariant is broken from either side, and the error has to point at something the caller
// can edit.
func replicaBudgetProblems(
	pool *pgelasticv1alpha1.PgElasticPool,
	ledger policy.Ledger,
	path *field.Path,
) field.ErrorList {
	if pool.Spec.Proxy == nil {
		return nil
	}
	replicas := proxy.Replicas(pool)
	if replicas <= 0 || ledger.CommittedBurst <= 0 {
		return nil
	}
	worstCase := int64(replicas) * int64(ledger.CommittedBurst)
	ratio, committable := committableConnections(pool, ledger.Allocatable)
	if worstCase <= committable {
		return nil
	}
	return field.ErrorList{field.Forbidden(path, fmt.Sprintf(
		"%d proxy replicas each read the whole %d backend-connection budget, so the worst case is "+
			"%d x %d = %d backend connections against a ceiling of %d (allocatable %d, being "+
			"backendConnections %d less %d%% headroom, times maxOversubscriptionRatio %s); reduce "+
			"spec.proxy.replicas, reduce the %d tenant(s) burstable ceilings, or raise the budget",
		replicas, ledger.BackendConnections, replicas, ledger.CommittedBurst, worstCase,
		committable, ledger.Allocatable, ledger.BackendConnections, ledger.HeadroomPercent,
		ratio, ledger.Tenants))}
}

// defaultOversubscriptionRatio matches the CRD default, applied to a pool stored before the
// field had one.
const defaultOversubscriptionRatio = "12"

// committableConnections is how far past allocatable the pool has said it will commit, and
// the ratio it said it in. An unparsable ratio falls back to the default rather than to no
// ceiling at all: a malformed string must not be a way to disable the gate.
func committableConnections(
	pool *pgelasticv1alpha1.PgElasticPool,
	allocatable int32,
) (string, int64) {
	ratio := pool.Spec.Capacity.MaxOversubscriptionRatio
	if ratio == "" {
		ratio = defaultOversubscriptionRatio
	}
	quantity, err := resource.ParseQuantity(ratio)
	if err != nil {
		quantity = resource.MustParse(defaultOversubscriptionRatio)
		ratio = defaultOversubscriptionRatio
	}
	// Milli scale so a fractional ratio is exact: comparing against a float would decide the
	// boundary case by rounding.
	return ratio, int64(allocatable) * quantity.MilliValue() / 1000
}

// defaultWorkloadClassProblems catches a pool whose default class its own allowlist
// forbids, which would leave every tenant that names no class unadmittable.
func defaultWorkloadClassProblems(
	pool *pgelasticv1alpha1.PgElasticPool,
	elasticClass *pgelasticv1alpha1.PgElasticClass,
) field.ErrorList {
	if pool.Spec.Admission == nil || pool.Spec.Admission.DefaultWorkloadClassName == "" {
		return nil
	}
	allowed := policy.AllowedWorkloadClassNames(pool, elasticClass)
	if len(allowed) == 0 || slices.Contains(allowed, pool.Spec.Admission.DefaultWorkloadClassName) {
		return nil
	}
	return field.ErrorList{field.NotSupported(
		field.NewPath("spec", "admission", "defaultWorkloadClassName"),
		pool.Spec.Admission.DefaultWorkloadClassName, allowed)}
}
