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

package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/migration"
)

const thirdInstance = "pg-third"

var _ = Describe("Two migrations of one tenant", func() {
	var (
		sql        *scriptedSQL
		reconciler *PgTenantMigrationReconciler
		source     *pgelasticv1alpha1.PgInstance
		target     *pgelasticv1alpha1.PgInstance
		third      *pgelasticv1alpha1.PgInstance
		tenant     *pgelasticv1alpha1.PgTenant
		secret     *corev1.Secret
		first      *pgelasticv1alpha1.PgTenantMigration
		second     *pgelasticv1alpha1.PgTenantMigration
		clock      time.Time
	)

	BeforeEach(func() {
		ensureNamespace(migrationNamespace)
		claimPool(migrationNamespace, "migration-class", "migration-pool")
		clock = time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

		source = makeMigrationInstance(sourceInstance)
		target = makeMigrationInstance(targetInstance)
		third = makeMigrationInstance(thirdInstance)
		for _, instance := range []*pgelasticv1alpha1.PgInstance{source, target, third} {
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			instance.Status = migrationInstanceStatus(instance.Name)
			Expect(k8sClient.Status().Update(ctx, instance)).To(Succeed())
		}

		tenant = makeTenant(migrationNamespace, migrationTenant, "migration-pool", migrationDatabase)
		Expect(k8sClient.Create(ctx, tenant)).To(Succeed())
		tenant.Status = pgelasticv1alpha1.PgTenantStatus{
			Binding: &pgelasticv1alpha1.PgTenantBinding{
				InstanceRef: &corev1.LocalObjectReference{Name: sourceInstance},
			},
			Utilization: &pgelasticv1alpha1.PgTenantUtilization{IsCold: ptr.To(true)},
		}
		Expect(k8sClient.Status().Update(ctx, tenant)).To(Succeed())

		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      provision.CredentialsSecretName(sourceInstance),
				Namespace: migrationNamespace,
			},
			Data: map[string][]byte{provision.SecretKeyReplicationPassword: []byte("placeholder")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())

		newMigration := func(name, target string) *pgelasticv1alpha1.PgTenantMigration {
			object := &pgelasticv1alpha1.PgTenantMigration{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: migrationNamespace},
				Spec: pgelasticv1alpha1.PgTenantMigrationSpec{
					TenantRef:         corev1.LocalObjectReference{Name: migrationTenant},
					TargetInstanceRef: corev1.LocalObjectReference{Name: target},
					Strategy:          pgelasticv1alpha1.TenantMigrationOnline,
				},
			}
			Expect(k8sClient.Create(ctx, object)).To(Succeed())
			return object
		}
		// Both are admitted. Nothing refuses the second: the CRD carries one CEL rule and it
		// is spec immutability, no PgTenantMigration webhook is registered, and the
		// controller never lists its siblings.
		first = newMigration("move-acme-to-dst", targetInstance)
		second = newMigration("move-acme-to-third", thirdInstance)

		sql = newScriptedSQL()
		reconciler = &PgTenantMigrationReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(),
			SQL: sql, Shell: scriptedShell{}, Router: migration.BindingRouter{Client: k8sClient},
			Now: func() time.Time { return clock },
		}
	})

	AfterEach(func() {
		deleteAndAwait(first, second, tenant, secret, source, target, third)
	})

	step := func(object *pgelasticv1alpha1.PgTenantMigration) *pgelasticv1alpha1.PgTenantMigration {
		GinkgoHelper()
		_, err := reconciler.Reconcile(ctx, requestFor(object))
		Expect(err).NotTo(HaveOccurred())
		clock = clock.Add(100 * time.Millisecond)
		return refetch(object)
	}

	driveTo := func(
		object *pgelasticv1alpha1.PgTenantMigration, phase pgelasticv1alpha1.TenantMigrationPhase,
	) *pgelasticv1alpha1.PgTenantMigration {
		GinkgoHelper()
		var current *pgelasticv1alpha1.PgTenantMigration
		for range 20 {
			current = step(object)
			if current.Status.Phase == phase {
				return current
			}
		}
		Fail("migration " + object.Name + " never reached " + string(phase) +
			", it stopped at " + string(current.Status.Phase))
		return current
	}

	routedInstance := func() string {
		GinkgoHelper()
		fetched := refetch(tenant)
		if fetched.Status.Binding == nil || fetched.Status.Binding.InstanceRef == nil {
			return ""
		}
		return fetched.Status.Binding.InstanceRef.Name
	}

	It("does not let a settled migration take back a tenant another one has moved", func() {
		By("giving the second migration its own frozen view of the source")
		Expect(step(second).Status.SourceInstanceRef.Name).To(Equal(sourceInstance))

		By("completing the first migration")
		Expect(driveTo(first, pgelasticv1alpha1.TenantMigrationPhaseCompleted).Status.Phase).
			To(Equal(pgelasticv1alpha1.TenantMigrationPhaseCompleted))
		Expect(routedInstance()).To(Equal(targetInstance))

		By("aborting the second, which restores the source it captured")
		current := refetch(second)
		current.Annotations = map[string]string{AnnotationAbort: "operator stopped it"}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(step(second).Status.Phase).
			To(Equal(pgelasticv1alpha1.TenantMigrationPhaseAborted))

		Expect(routedInstance()).To(Equal(targetInstance),
			"a settled migration routed the tenant back to %q, undoing a cutover it had no "+
				"claim to and stranding every write taken on %q", sourceInstance, targetInstance)
	})

	It("does not let a cutover flip a tenant another migration has moved", func() {
		By("holding the second migration at its cutover boundary")
		driveTo(second, pgelasticv1alpha1.TenantMigrationPhaseCutover)

		By("completing the first migration")
		driveTo(first, pgelasticv1alpha1.TenantMigrationPhaseCompleted)
		Expect(routedInstance()).To(Equal(targetInstance))

		By("letting the second migration run its cutover")
		final := step(second)

		Expect(routedInstance()).To(Equal(targetInstance),
			"the cutover flipped a tenant that had already been moved to %q onto %q",
			targetInstance, thirdInstance)
		Expect(final.Status.Phase).
			NotTo(Equal(pgelasticv1alpha1.TenantMigrationPhaseCompleted),
				"a cutover that did not cut over still reported success")
		Expect(conditionOf(final.Status.Conditions, migration.ConditionRetrying).Message).
			To(ContainSubstring("another migration has moved it"))
	})
})
