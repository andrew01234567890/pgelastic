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
})
