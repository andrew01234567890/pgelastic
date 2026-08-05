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

	// The same refusal the instance webhook applies, on the template a pool would stamp its
	// members out of. Nothing reads that template yet, which is exactly why it is cheap to
	// gate now: once a pool controller provisions from it, a template carrying a pinned
	// parameter would produce instances that silently dropped it, and the refusal would then
	// arrive on objects the user did not write.
	problems = append(problems, parameterProblems(
		field.NewPath("spec", "instances", "template", "parameters"),
		pool.Spec.Instances.Template.Parameters)...)

	problems = append(problems, proxyPodTemplateProblems(pool)...)

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

// proxyPodTemplateProblems keeps the proxy pod-spec escape hatch to the fields it exists for.
//
// spec.proxy.template.spec is a whole corev1.PodSpec, strategically merged over the pod the
// controller generates. A strategic merge adds; it does not replace. So the fields refused
// here are not overrides of the operator's choices, they are additions to the pod: another
// container, another volume, another identity.
//
// That matters because of what the proxy pod can reach. Its ServiceAccount is deliberately
// narrow - the fleet is on the client's data path and is not allowed to watch Secrets - but
// the pod itself is mounted with the rendered proxy configuration, which carries every
// tenant's password and backend SCRAM keys. A container added here runs beside it with that
// mount, and a volume added here can name any Secret in the namespace, including every
// instance's bootstrap credentials. Placement and resources are what the hatch is for; a
// second identity inside the same pod is not.
func proxyPodTemplateProblems(pool *pgelasticv1alpha1.PgElasticPool) field.ErrorList {
	if pool.Spec.Proxy == nil || pool.Spec.Proxy.Template == nil ||
		pool.Spec.Proxy.Template.Spec == nil {
		return nil
	}
	spec := pool.Spec.Proxy.Template.Spec
	path := field.NewPath("spec", "proxy", "template", "spec")

	var problems field.ErrorList
	refuse := func(name, why string) {
		problems = append(problems, field.Forbidden(path.Child(name), why))
	}
	// A strategic merge keys containers by name, which is the whole design of the hatch: an
	// entry called ContainerName patches the generated proxy container rather than replacing
	// it. Any other name is a new container in the same pod. The list itself cannot be refused
	// - the PodSpec schema requires it - so the name is what decides.
	for index, container := range spec.Containers {
		if container.Name != proxy.ContainerName {
			problems = append(problems, field.Invalid(
				path.Child("containers").Index(index).Child("name"), container.Name,
				fmt.Sprintf("a container of any other name runs beside the proxy with the "+
					"rendered configuration mounted, which carries every tenant's credentials; "+
					"name it %q to patch the generated container instead",
					proxy.ContainerName)))
		}
	}
	if len(spec.InitContainers) > 0 {
		refuse("initContainers", "an init container shares the pod's volumes and service account")
	}
	if len(spec.EphemeralContainers) > 0 {
		refuse("ephemeralContainers", "an ephemeral container shares the pod's volumes")
	}
	if len(spec.Volumes) > 0 {
		refuse("volumes", "a volume added here may name any Secret in the namespace, including "+
			"every instance's bootstrap credentials")
	}
	if spec.ServiceAccountName != "" || spec.DeprecatedServiceAccount != "" {
		refuse("serviceAccountName", "the proxy runs as the ServiceAccount the operator "+
			"created for it, which is deliberately narrower than any other in the namespace")
	}
	if spec.HostNetwork || spec.HostPID || spec.HostIPC {
		refuse("hostNetwork", "the proxy may not share the node's network, process or IPC "+
			"namespaces")
	}
	if spec.SecurityContext != nil {
		refuse("securityContext", "the pod security context is the operator's; set what you "+
			"need through spec.proxy")
	}
	return problems
}
