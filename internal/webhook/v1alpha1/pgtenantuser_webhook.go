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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// SetupPgTenantUserWebhookWithManager registers the webhook for PgTenantUser.
func SetupPgTenantUserWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &pgelasticv1alpha1.PgTenantUser{}).
		WithValidator(&PgTenantUserCustomValidator{Reader: mgr.GetAPIReader()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-pgelastic-io-v1alpha1-pgtenantuser,mutating=false,failurePolicy=fail,sideEffects=None,groups=pgelastic.io,resources=pgtenantusers,verbs=create;update,versions=v1alpha1,name=vpgtenantuser-v1alpha1.kb.io,admissionReviewVersions=v1

// PgTenantUserCustomValidator keeps a login inside the tenant it belongs to.
//
// The kind carries no privileged attribute and no way to name a role outside its tenant, so
// almost all of the containment is the API's shape rather than a rule enforced here. What is
// left is the two things the shape alone cannot decide: that the tenant named actually exists
// in this namespace, and that every membership names a user of that same tenant. A membership
// reaching outside is the one expression that would breach containment, so it is refused.
//
// Read through the uncached reader for the same reason the tenant validator is: a decision made
// from a stale cache is a decision made about a cluster that no longer exists, and two users
// admitted against the same stale answer can together be wrong in a way neither is alone.
type PgTenantUserCustomValidator struct {
	Reader client.Reader
}

// ValidateCreate implements webhook.CustomValidator.
func (v *PgTenantUserCustomValidator) ValidateCreate(
	ctx context.Context,
	obj *pgelasticv1alpha1.PgTenantUser,
) (admission.Warnings, error) {
	return nil, v.validate(ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator.
func (v *PgTenantUserCustomValidator) ValidateUpdate(
	ctx context.Context,
	_, newObj *pgelasticv1alpha1.PgTenantUser,
) (admission.Warnings, error) {
	// A login being deleted is not validated: the only write left is the controller clearing
	// its own finalizer, and refusing that against a tenant that has already gone would leave
	// the object - and its namespace - undeletable for ever.
	if newObj.DeletionTimestamp != nil {
		return nil, nil
	}
	return nil, v.validate(ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator.
func (v *PgTenantUserCustomValidator) ValidateDelete(
	_ context.Context,
	_ *pgelasticv1alpha1.PgTenantUser,
) (admission.Warnings, error) {
	return nil, nil
}

func (v *PgTenantUserCustomValidator) validate(
	ctx context.Context,
	user *pgelasticv1alpha1.PgTenantUser,
) error {
	problems := field.ErrorList{}
	tenantPath := field.NewPath("spec", "tenantRef", "name")

	tenant := &pgelasticv1alpha1.PgTenant{}
	key := types.NamespacedName{Namespace: user.Namespace, Name: user.Spec.TenantRef.Name}
	if err := v.Reader.Get(ctx, key, tenant); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Admitting a login whose tenant does not exist would admit one with no database to
		// reach and no role to become, and nothing downstream could say which tenant it was
		// supposed to be confined to.
		return invalid(user, field.ErrorList{field.Invalid(tenantPath, user.Spec.TenantRef.Name,
			fmt.Sprintf("no PgTenant of that name exists in namespace %q", user.Namespace))})
	}

	// A group role authenticates nobody, so a credential on one is a contradiction rather than
	// an unused field: it reads as though somebody may log in with it, and nothing downstream
	// would ever prove that wrong.
	if user.Spec.Login != nil && !*user.Spec.Login && user.Spec.CredentialsSecretRef != nil {
		problems = append(problems, field.Invalid(
			field.NewPath("spec", "credentialsSecretRef"), user.Spec.CredentialsSecretRef.Name,
			"this login may not log in, so a credential for it authenticates nobody; unset "+
				"one of credentialsSecretRef or login"))
	}

	// One List serves both remaining rules, and both need every login of the same tenant.
	{
		siblings := &pgelasticv1alpha1.PgTenantUserList{}
		if err := v.Reader.List(ctx, siblings, client.InNamespace(user.Namespace)); err != nil {
			return err
		}
		known := map[string]bool{}
		for i := range siblings.Items {
			sibling := &siblings.Items[i]
			if sibling.Spec.TenantRef.Name != user.Spec.TenantRef.Name {
				continue
			}
			known[sibling.Spec.UserName] = true
			// Two logins of one tenant answering to the same name are one identity, not two:
			// the proxy authenticates a client against the name it sends and the PostgreSQL
			// role is derived from it, so admitting both leaves whichever reconciles last
			// deciding whose credential the name accepts.
			//
			// Checked against the uncached reader, and still only best effort - two creates
			// racing each other both read a cluster without the other. PostgreSQL is not a
			// backstop here the way it is for a membership cycle, so the reconciler has to
			// refuse the loser as well; this turns the ordinary case into an admission error
			// naming both objects.
			if sibling.Name != user.Name && sibling.Spec.UserName == user.Spec.UserName {
				problems = append(problems, field.Duplicate(
					field.NewPath("spec", "userName"),
					fmt.Sprintf("PgTenant %q already has a login called %q, as PgTenantUser %q",
						user.Spec.TenantRef.Name, user.Spec.UserName, sibling.Name)))
			}
		}
		memberPath := field.NewPath("spec", "memberOf")
		for index, name := range user.Spec.MemberOf {
			switch {
			case name == user.Spec.UserName:
				problems = append(problems, field.Invalid(memberPath.Index(index), name,
					"a login cannot be a member of itself"))
			case !known[name]:
				problems = append(problems, field.Invalid(memberPath.Index(index), name,
					fmt.Sprintf("PgTenant %q has no user of that name; a membership may only "+
						"name a user of the same tenant, because one reaching outside it is the "+
						"only thing this kind can express that would breach containment",
						user.Spec.TenantRef.Name)))
			}
		}
	}

	return invalid(user, problems)
}
