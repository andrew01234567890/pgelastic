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
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// SetupPgWorkloadClassWebhookWithManager registers the webhook for PgWorkloadClass in the manager.
func SetupPgWorkloadClassWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &pgelasticv1alpha1.PgWorkloadClass{}).
		WithValidator(&PgWorkloadClassCustomValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-pgelastic-io-v1alpha1-pgworkloadclass,mutating=false,failurePolicy=fail,sideEffects=None,groups=pgelastic.io,resources=pgworkloadclasses,verbs=create;update,versions=v1alpha1,name=vpgworkloadclass-v1alpha1.kb.io,admissionReviewVersions=v1

// PgWorkloadClassCustomValidator holds the cluster-wide single-global rule, which no CEL
// expression can see because it is a property of the set of classes rather than of any
// one of them.
type PgWorkloadClassCustomValidator struct {
	Reader client.Reader
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type PgWorkloadClass.
func (v *PgWorkloadClassCustomValidator) ValidateCreate(
	ctx context.Context,
	obj *pgelasticv1alpha1.PgWorkloadClass,
) (admission.Warnings, error) {
	return nil, v.validate(ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type PgWorkloadClass.
func (v *PgWorkloadClassCustomValidator) ValidateUpdate(
	ctx context.Context,
	_, newObj *pgelasticv1alpha1.PgWorkloadClass,
) (admission.Warnings, error) {
	return nil, v.validate(ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type PgWorkloadClass.
func (v *PgWorkloadClassCustomValidator) ValidateDelete(
	_ context.Context,
	_ *pgelasticv1alpha1.PgWorkloadClass,
) (admission.Warnings, error) {
	return nil, nil
}

func (v *PgWorkloadClassCustomValidator) validate(
	ctx context.Context,
	workloadClass *pgelasticv1alpha1.PgWorkloadClass,
) error {
	problems := fieldErrors(policy.WorkloadClassProblems(workloadClass))

	if workloadClass.Spec.Global != nil && *workloadClass.Spec.Global {
		globals, err := policy.Resolver{Reader: v.Reader}.GlobalWorkloadClassNames(ctx)
		if err != nil {
			return err
		}
		others := slices.DeleteFunc(globals, func(name string) bool { return name == workloadClass.Name })
		if len(others) > 0 {
			problems = append(problems, field.Forbidden(field.NewPath("spec", "global"),
				"at most one PgWorkloadClass cluster-wide may be global; "+
					strings.Join(others, ", ")+" already is"))
		}
	}

	return invalid(workloadClass, problems)
}
