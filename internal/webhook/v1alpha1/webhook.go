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

// Package v1alpha1 holds the validating webhooks for the pgelastic v1alpha1 API. They
// carry the rules CEL cannot: everything that has to read another object to know whether
// a write is safe.
package v1alpha1

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/policy"
)

// invalid renders a field error list as the Invalid status the API server prints back to
// the client, so a rejection reads like any other schema failure rather than like a bare
// webhook error string.
func invalid(object client.Object, problems field.ErrorList) error {
	if len(problems) == 0 {
		return nil
	}
	kind := object.GetObjectKind().GroupVersionKind().GroupKind()
	if kind.Empty() {
		kind.Group = pgelasticv1alpha1.SchemeGroupVersion.Group
		switch object.(type) {
		case *pgelasticv1alpha1.PgTenant:
			kind.Kind = "PgTenant"
		case *pgelasticv1alpha1.PgElasticPool:
			kind.Kind = "PgElasticPool"
		case *pgelasticv1alpha1.PgWorkloadClass:
			kind.Kind = "PgWorkloadClass"
		}
	}
	return apierrors.NewInvalid(kind, object.GetName(), problems)
}

// fieldErrors renders self-consistency problems as field errors so the API server
// reports them at the path that is actually wrong.
func fieldErrors(problems []policy.Problem) field.ErrorList {
	rendered := make(field.ErrorList, 0, len(problems))
	for _, problem := range problems {
		rendered = append(rendered, field.Invalid(field.NewPath(problem.Path), nil, problem.Detail))
	}
	return rendered
}
