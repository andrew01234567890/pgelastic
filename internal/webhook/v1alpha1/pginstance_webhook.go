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
	"maps"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
)

// SetupPgInstanceWebhookWithManager registers the webhook for PgInstance in the manager.
func SetupPgInstanceWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &pgelasticv1alpha1.PgInstance{}).
		WithValidator(&PgInstanceCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-pgelastic-io-v1alpha1-pginstance,mutating=false,failurePolicy=fail,sideEffects=None,groups=pgelastic.io,resources=pginstances,verbs=create;update,versions=v1alpha1,name=vpginstance-v1alpha1.kb.io,admissionReviewVersions=v1

// PgInstanceCustomValidator refuses a parameter the operator holds, at admission.
//
// The pgconf package doc and spec.parameters' own field doc have both promised this webhook
// since they were written, and it did not exist. What happened instead was silent:
// UserParameters drops an owned name and returns it in `dropped`, its only caller discards
// that return, and so `max_connections: "5000"` was accepted by the API server, written to
// the object, never rendered, and never mentioned again.
//
// This is additive to that drop and never a replacement for it. The guarantee pgconf states -
// that an object admitted before a parameter became owned still cannot poison a pod that
// reads it later - holds only while both passes exist.
type PgInstanceCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type PgInstance.
func (v *PgInstanceCustomValidator) ValidateCreate(
	_ context.Context,
	instance *pgelasticv1alpha1.PgInstance,
) (admission.Warnings, error) {
	return nil, invalid(instance, parameterProblems(
		field.NewPath("spec", "parameters"), instance.Spec.Parameters))
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type PgInstance.
func (v *PgInstanceCustomValidator) ValidateUpdate(
	_ context.Context,
	_, instance *pgelasticv1alpha1.PgInstance,
) (admission.Warnings, error) {
	return nil, invalid(instance, parameterProblems(
		field.NewPath("spec", "parameters"), instance.Spec.Parameters))
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type PgInstance.
func (v *PgInstanceCustomValidator) ValidateDelete(
	_ context.Context,
	_ *pgelasticv1alpha1.PgInstance,
) (admission.Warnings, error) {
	return nil, nil
}

// parameterProblems refuses what a user may not set, and what cannot be rendered.
//
// It asks IsPinned and never IsOwned. A Tuned parameter is one the operator computes a
// default for and the user may replace, so refusing it here would make that whole level a
// no-op that looked like it worked - and quoting OwnedNames in the refusal would tell an
// operator that a parameter they are entitled to set is forbidden.
//
// Well-formedness stays a separate axis. A malformed name is refused whoever owns it, because
// a name is not quotable in a configuration file the way a value is, and folding the two
// questions together is how `fsync = off` gets past a denylist in three characters.
func parameterProblems(
	path *field.Path,
	parameters map[string]pgelasticv1alpha1.GUCValue,
) field.ErrorList {
	problems := field.ErrorList{}
	for _, name := range slices.Sorted(maps.Keys(parameters)) {
		value := string(parameters[name])
		switch {
		case pgconf.IsPinned(name) && !isTheOperatorsOwnValue(name, value):
			problems = append(problems, field.Forbidden(path.Key(name), fmt.Sprintf(
				"%s is %s: the operator decides it, and a value here would be dropped rather "+
					"than applied. The parameters the operator holds are %s",
				name, pgconf.Classify(name).Ownership, strings.Join(pgconf.PinnedNames(), ", "))))
		case !pgconf.RenderableParameter(name, value):
			problems = append(problems, field.Invalid(path.Key(name), value,
				"a parameter must render as exactly one postgresql.conf line"))
		}
	}
	return problems
}

// isTheOperatorsOwnValue admits a manifest that spells out the value the operator was going to
// emit anyway, which is what makes a GitOps repository holding the full effective
// configuration legal rather than an error.
//
// Only a Blocked parameter can be checked this way, because only a Blocked parameter has a
// constant to compare against: a Fixed one is computed from the instance's own shape by the
// controller, and recomputing it here would be a second derivation of the same number that
// could disagree with the first. A Fixed parameter is therefore refused even when the value
// would have matched, and the refusal names which kind it is.
func isTheOperatorsOwnValue(name, value string) bool {
	owned := pgconf.Classify(name)
	return owned.Ownership == pgconf.OwnershipBlocked && !owned.Omit && owned.Value == value
}
