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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
)

// A PgTenantUser is meant to be safe to hand to a tenant's own operators, which rests on it
// being structurally unable to reach outside its tenant. Most of that is the API's shape - it
// has no field for any privileged attribute - and what is left is enforced here.
var _ = Describe("PgTenantUser containment", Ordered, func() {
	const (
		namespace     = "wh-users"
		poolName      = "wh-users-pool"
		className     = "wh-users-class"
		workloadClass = "wh-users-standard"
		tenantA       = "wh-users-acme"
		tenantB       = "wh-users-globex"
	)

	makeUser := func(name, tenant, userName string, memberOf ...string) *pgelasticv1alpha1.PgTenantUser {
		return &pgelasticv1alpha1.PgTenantUser{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: pgelasticv1alpha1.PgTenantUserSpec{
				TenantRef: corev1.LocalObjectReference{Name: tenant},
				UserName:  userName,
				MemberOf:  memberOf,
			},
		}
	}

	BeforeAll(func() {
		ensureNamespace(namespace, nil)
		mustCreate(makeElasticClass(className))
		mustCreate(makeWorkloadClass(workloadClass, 1, 8))
		mustCreate(makePool(namespace, poolName, className))
		mustCreate(makeTenant(namespace, tenantA, poolName, "acme", workloadClass))
		mustCreate(makeTenant(namespace, tenantB, poolName, "globex", workloadClass))
	})

	It("admits a login naming a tenant that exists", func() {
		mustCreate(makeUser("wh-app-a", tenantA, "app"))
	})

	// The containment property Azure states and PostgreSQL cannot give: two tenants may each
	// have a user called `app`, and they are different identities.
	It("admits two tenants each having a user of the same name", func() {
		mustCreate(makeUser("wh-app-b", tenantB, "app"))
	})

	It("refuses a login naming a tenant that does not exist", func() {
		err := k8sClient.Create(ctx, makeUser("wh-orphan", "wh-users-nobody", "app"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no PgTenant of that name exists"))
	})

	// The one expression this kind could otherwise use to breach containment.
	It("refuses a membership naming a user of another tenant", func() {
		mustCreate(makeUser("wh-reader-b", tenantB, "reporting"))

		err := k8sClient.Create(ctx, makeUser("wh-crosser", tenantA, "crosser", "reporting"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("has no user of that name"))
		Expect(err.Error()).To(ContainSubstring("breach containment"))
	})

	It("admits a membership naming a user of the same tenant", func() {
		mustCreate(makeUser("wh-reader-a", tenantA, "reporting"))
		mustCreate(makeUser("wh-member-a", tenantA, "member", "reporting"))
	})

	It("refuses a login that is a member of itself", func() {
		err := k8sClient.Create(ctx, makeUser("wh-selfref", tenantA, "selfref", "selfref"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cannot be a member of itself"))
	})

	// Two logins of one tenant that answer to the same name are one identity, not two: the
	// proxy authenticates a client against the name it sends, and the role is derived from it.
	// Admitting both leaves whichever reconciles last deciding who the name belongs to.
	It("refuses a second login of the same tenant claiming a name already taken", func() {
		mustCreate(makeUser("wh-dup-first", tenantA, "duplicated"))

		err := k8sClient.Create(ctx, makeUser("wh-dup-second", tenantA, "duplicated"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("already has a login called"))
	})

	// The role a login maps to is derived from the tenant it belongs to and its own object
	// name. Letting either move would leave the role it was provisioned under standing in
	// pg_authid with nothing referring to it - and, for tenantRef, would relocate a live login
	// into a tenant whose data it was never meant to reach.
	It("refuses moving a login to another tenant", func() {
		user := makeUser("wh-immutable-tenant", tenantA, "settled")
		mustCreate(user)

		user.Spec.TenantRef = corev1.LocalObjectReference{Name: tenantB}
		err := k8sClient.Update(ctx, user)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("tenantRef is immutable"))
	})

	It("refuses renaming a login", func() {
		user := makeUser("wh-immutable-name", tenantA, "renamed")
		mustCreate(user)

		user.Spec.UserName = "renamed-again"
		err := k8sClient.Update(ctx, user)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("userName is immutable"))
	})

	// A group role authenticates nobody, so a credential attached to one is a contradiction
	// rather than a harmless extra: it reads as though somebody may log in with it.
	// spec.userName is a tenant operator's to choose, and the tenant's owner is a name they
	// can read off their own object. Both render into the proxy's [[auth.users]] keyed by the
	// name a client sends, so the proxy would have two entries for one session - and taking
	// the owner's would hand this login the owner's privileges while leaving it
	// indistinguishable from the owner in pg_stat_activity.
	It("refuses a login named after its own tenant's owner", func() {
		user := makeUser("wh-app-owner", tenantA, "acme")

		err := k8sClient.Create(ctx, user)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.userName"))
	})

	It("admits a login named after another tenant's owner, which is a different identity", func() {
		mustCreate(makeUser("wh-app-other-owner", tenantA, "globex"))
	})

	It("refuses a credentials Secret on a login that may not log in", func() {
		user := makeUser("wh-group-with-secret", tenantA, "grouped")
		user.Spec.Login = ptrTo(false)
		user.Spec.CredentialsSecretRef = &corev1.LocalObjectReference{Name: "wh-unused"}

		err := k8sClient.Create(ctx, user)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("may not log in"))
	})
})
