//go:build e2e

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

package backup

import (
	"fmt"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// Three tenants on one instance. One is restored; the other two are what the whole design
// is for, and the assertion that they were untouched is the one that distinguishes a tenant
// restore from an instance restore that happened to be asked for by a tenant.
const (
	tenantRestoreName = "restore-acme"
	restoredTenantDB  = "e2e_acme"
	neighbourOneDB    = "e2e_globex"
	neighbourTwoDB    = "e2e_initech"
	tenantProbeTable  = "ledger"
	tenantUnderTest   = "acme"
)

func tenantRestoreSpecs() {
	Describe("tenant-granular point-in-time restore", Ordered, func() {
		var targetTime string

		BeforeAll(func() {
			By("giving three tenants a history on one instance")
			for _, database := range []string{restoredTenantDB, neighbourOneDB, neighbourTwoDB} {
				createDatabase(database)
				runSQLOn(database, fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s (id int primary key)", tenantProbeTable))
				runSQLOn(database, fmt.Sprintf(
					"INSERT INTO %s VALUES (1), (2) ON CONFLICT DO NOTHING", tenantProbeTable))
			}

			targetTime = strings.TrimSpace(runSQLOn("postgres", "SELECT now()"))
			Expect(targetTime).NotTo(BeEmpty())

			// The bad afternoon. One tenant loses its data; the others carry on writing, and
			// what they write after the target is what must survive the restore.
			By("destroying one tenant's data and letting the others carry on")
			runSQLOn(restoredTenantDB, fmt.Sprintf("DELETE FROM %s", tenantProbeTable))
			for _, database := range []string{neighbourOneDB, neighbourTwoDB} {
				runSQLOn(database, fmt.Sprintf(
					"INSERT INTO %s VALUES (3) ON CONFLICT DO NOTHING", tenantProbeTable))
			}

			switchWAL(Default)
			Eventually(func(g Gomega) {
				g.Expect(readInstance(g).Status.ArchiveHealth.Healthy).To(BeTrue())
			}).Should(Succeed())

			// The tenant controller does not run in this suite, so the binding a restore reads -
			// which database, on which instance - is written here directly. What is being tested
			// is the restore, and inventing a second copy of the provisioning path to reach it
			// would test that instead.
			bindTenant()
		})

		It("puts one tenant back and leaves its neighbours alone", func() {
			Expect(k8sClient.Create(suiteCtx, &pgelasticv1alpha1.PgRestore{
				ObjectMeta: metav1.ObjectMeta{Name: tenantRestoreName, Namespace: archiveNamespace},
				Spec: pgelasticv1alpha1.PgRestoreSpec{
					Scope:             pgelasticv1alpha1.RestoreScopeTenant,
					SourceInstanceRef: corev1.LocalObjectReference{Name: archiveInstance},
					TenantRef:         &corev1.LocalObjectReference{Name: tenantUnderTest},
					Target:            &pgelasticv1alpha1.RecoveryTarget{Time: targetTime},
				},
			})).To(Succeed())

			By("waiting for the tenant to be put back")
			Eventually(func(g Gomega) {
				restore := &pgelasticv1alpha1.PgRestore{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: archiveNamespace, Name: tenantRestoreName,
				}, restore)).To(Succeed())
				g.Expect(restore.Status.Phase).To(Equal(pgelasticv1alpha1.RestorePhaseCompleted),
					restore.Status.Error)
			}).Should(Succeed())

			By("checking the restored tenant has its rows back")
			Expect(countRows(restoredTenantDB)).To(Equal(2),
				"the tenant asked for was not put back to the moment requested")

			// The assertion the whole design exists for. An instance-scoped restore would have
			// rolled these back too, taking two customers offline to fix a third's mistake.
			By("checking the neighbours kept everything, including what they wrote after the target")
			for _, neighbour := range []string{neighbourOneDB, neighbourTwoDB} {
				Expect(countRows(neighbour)).To(Equal(3),
					fmt.Sprintf("%s lost the row it wrote after the restore target, so its "+
						"neighbour's restore rolled it back too", neighbour))
			}
		})

		// The recovery instance holds every other tenant of the source at the restored moment.
		// Leaving it up leaves a readable copy of other customers' data behind.
		It("throws the recovery instance away", func() {
			Eventually(func(g Gomega) {
				recovery := &pgelasticv1alpha1.PgInstance{}
				err := k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: archiveNamespace,
					Name:      tenantRestoreName + "-recovery",
				}, recovery)
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
					"the throwaway instance is still running with a copy of every tenant on it")
			}).Should(Succeed())
		})

		// A tenant left refusing connections after the copy is an outage caused by the recovery
		// rather than by whatever the recovery was for.
		It("gives the restored tenant its connections back", func() {
			allowed := strings.TrimSpace(runSQLOn("postgres", fmt.Sprintf(
				"SELECT datallowconn FROM pg_database WHERE datname = '%s'", restoredTenantDB)))
			Expect(allowed).To(Equal("t"), "the restored tenant is still fenced")
		})
	})
}

// createDatabase makes one, tolerating the one that is already there. CREATE DATABASE takes
// no IF NOT EXISTS, and the alternative is a catalogue query whose answer is stale by the
// time it is acted on.
func createDatabase(name string) {
	GinkgoHelper()
	primary := primaryOf(Default, archiveInstance)
	_, _ = kubectlCommand(
		"exec", "-n", archiveNamespace, primary, "-c", "postgres", "--",
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-tAc",
		"CREATE DATABASE "+name,
	).CombinedOutput()

	exists := strings.TrimSpace(runSQLOn("postgres", fmt.Sprintf(
		"SELECT count(*) FROM pg_database WHERE datname = '%s'", name)))
	Expect(exists).To(Equal("1"), "the database "+name+" was not created")
}

// bindTenant publishes the tenant a restore acts on: which database, on which instance.
func bindTenant() {
	GinkgoHelper()
	tenant := &pgelasticv1alpha1.PgTenant{
		ObjectMeta: metav1.ObjectMeta{Name: tenantUnderTest, Namespace: archiveNamespace},
		Spec: pgelasticv1alpha1.PgTenantSpec{
			PoolRef:      corev1.LocalObjectReference{Name: claimPoolName},
			DatabaseName: restoredTenantDB,
		},
	}
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, tenant))).To(Succeed())

	Eventually(func(g Gomega) {
		fetched := &pgelasticv1alpha1.PgTenant{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
			Namespace: archiveNamespace, Name: tenantUnderTest,
		}, fetched)).To(Succeed())
		fetched.Status.Binding = &pgelasticv1alpha1.PgTenantBinding{
			InstanceRef: &corev1.LocalObjectReference{Name: archiveInstance},
		}
		g.Expect(k8sClient.Status().Update(suiteCtx, fetched)).To(Succeed())
	}).Should(Succeed())
}

func countRows(database string) int {
	GinkgoHelper()
	raw := strings.TrimSpace(runSQLOn(database, fmt.Sprintf(
		"SELECT count(*) FROM %s", tenantProbeTable)))
	count, err := strconv.Atoi(raw)
	Expect(err).NotTo(HaveOccurred(), raw)
	return count
}

func runSQLOn(database, statement string) string {
	GinkgoHelper()
	primary := primaryOf(Default, archiveInstance)
	output, err := kubectlCommand(
		"exec", "-n", archiveNamespace, primary, "-c", "postgres", "--",
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-d", database,
		"-tAc", statement,
	).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(output))
	return string(output)
}
