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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

const (
	restoreName            = "pitr"
	restoredInstance       = "pg-restored"
	restoreProbeTable      = "e2e_pitr"
	restoreRowsBeforeMark  = 2
	restoreRowsAfterTarget = 1
)

func instanceRestoreSpecs() {
	Describe("point-in-time restore", Ordered, func() {
		var targetTime string

		// The specs above leave a full backup and a differential in the repository and archiving
		// working, which is exactly the state a restore needs. What this adds is the history
		// after it: two rows, a moment, and a third row that recovery must not reach.
		BeforeAll(func() {
			By("writing the history the restore will be asked to land inside")
			runSQL(fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s (id int primary key)", restoreProbeTable))
			for id := 1; id <= restoreRowsBeforeMark; id++ {
				runSQL(fmt.Sprintf("INSERT INTO %s VALUES (%d)", restoreProbeTable, id))
			}

			// The moment comes from inside PostgreSQL. A timestamp taken on the machine running
			// this suite would be a different clock, and a restore is asked to land within
			// seconds of it.
			targetTime = strings.TrimSpace(runSQL("SELECT now()"))
			Expect(targetTime).NotTo(BeEmpty())

			By("writing the row recovery must not reach")
			runSQL(fmt.Sprintf("INSERT INTO %s VALUES (%d)",
				restoreProbeTable, restoreRowsBeforeMark+restoreRowsAfterTarget))

			// Recovery replays WAL out of the archive, so the segment holding the target has to
			// be in it before the restore starts. Without the switch the last segment sits in
			// pg_wal until archive_timeout, and the restore stops short of the moment asked for.
			switchWAL()
			Eventually(func(g Gomega) {
				g.Expect(readInstance(g).Status.ArchiveHealth.Healthy).To(BeTrue())
			}).Should(Succeed())
		})

		It("recovers into a new instance and stops at the moment it was asked to", func() {
			Expect(k8sClient.Create(suiteCtx, &pgelasticv1alpha1.PgRestore{
				ObjectMeta: metav1.ObjectMeta{Name: restoreName, Namespace: archiveNamespace},
				Spec: pgelasticv1alpha1.PgRestoreSpec{
					SourceInstanceRef:  corev1.LocalObjectReference{Name: archiveInstance},
					TargetInstanceName: restoredInstance,
					Target:             &pgelasticv1alpha1.RecoveryTarget{Time: targetTime},
				},
			})).To(Succeed())

			By("waiting for the restored instance to serve")
			Eventually(func(g Gomega) {
				restore := &pgelasticv1alpha1.PgRestore{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
					Namespace: archiveNamespace, Name: restoreName,
				}, restore)).To(Succeed())
				g.Expect(restore.Status.Phase).To(Equal(pgelasticv1alpha1.RestorePhaseCompleted),
					restore.Status.Error)
			}).Should(Succeed())

			restored := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: archiveNamespace, Name: restoredInstance,
			}, restored)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(restored.Status.Conditions,
				pgelasticv1alpha1.ConditionReady)).To(BeTrue())

			// The three assertions that together mean the restore did what was asked. Any two of
			// them pass on a restore that merely succeeded.

			// One: recovery forked a new timeline rather than resuming the source's. A restore
			// that landed back on timeline 1 replayed everything and stopped nowhere.
			By("checking that recovery forked a timeline")
			timeline := strings.TrimSpace(inRestored(
				"psql", "-h", provision.SocketDir, "-U", "postgres", "-tAc",
				"SELECT timeline_id FROM pg_control_checkpoint()"))
			forked, err := strconv.Atoi(timeline)
			Expect(err).NotTo(HaveOccurred(), timeline)
			Expect(forked).To(BeNumerically(">", 1),
				"recovery stayed on the source's timeline, so it never reached a target")

			// Two: the restored instance is a real primary its standbys stream from, rather than
			// one stranded in recovery with nobody following it.
			By("checking that the restored primary is followed")
			Eventually(func(g Gomega) {
				streaming := strings.TrimSpace(inRestored(
					"psql", "-h", provision.SocketDir, "-U", "postgres", "-tAc",
					"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"))
				g.Expect(streaming).NotTo(Equal("0"), "no standby follows the restored primary")
			}).Should(Succeed())

			// Three: the row written after the target is absent. This is the only one of the
			// three that proves recovery stopped where it was told rather than simply finishing.
			By("checking that the row written after the target is not there")
			rows := strings.TrimSpace(inRestored(
				"psql", "-h", provision.SocketDir, "-U", "postgres", "-tAc",
				fmt.Sprintf("SELECT count(*) FROM %s", restoreProbeTable)))
			Expect(rows).To(Equal(strconv.Itoa(restoreRowsBeforeMark)),
				"recovery did not stop at the target: it replayed past the moment asked for")
		})

		// The restored instance carries its source's system identifier, because a physical
		// restore copies the control file verbatim. It therefore addresses the source's stanza
		// while running on a forked timeline, and anything it archived would interleave two
		// histories into one repository and leave neither restorable.
		//
		// This is asserted against the repository rather than against the guard, because the
		// guard is what is being tested.
		It("archives nothing into the source's repository", func() {
			before := archivedSegments()

			By("generating WAL on the restored instance")
			for id := 100; id < 105; id++ {
				runSQLIn(restoredInstance, fmt.Sprintf(
					"INSERT INTO %s VALUES (%d)", restoreProbeTable, id))
			}
			inRestored("psql", "-h", provision.SocketDir, "-U", "postgres", "-tAc",
				"SELECT pg_switch_wal()")

			Consistently(func(g Gomega) {
				g.Expect(archivedSegments()).To(Equal(before),
					"the restored instance pushed a segment into its source's archive")
			}, "60s", "10s").Should(Succeed())
		})

		// A restored instance is evidence until somebody has looked at it, not capacity.
		// Placing tenants onto one that has just replayed to an arbitrary point would move live
		// customers onto a copy of their own past.
		It("does not admit tenants until somebody says so", func() {
			restored := &pgelasticv1alpha1.PgInstance{}
			Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: archiveNamespace, Name: restoredInstance,
			}, restored)).To(Succeed())
			Expect(restored.Spec.Admission).NotTo(BeNil())
			Expect(restored.Spec.Admission.Schedulable).NotTo(BeNil())
			Expect(*restored.Spec.Admission.Schedulable).To(BeFalse())
		})
	})
}

// archivedSegments counts what the source's stanza holds, read through pgBackRest itself
// rather than by listing the bucket, so the count is of objects the archive considers its
// own rather than of files that happen to be under a prefix.
func archivedSegments() string {
	GinkgoHelper()
	output, err := inPrimary("bash", "-c", fmt.Sprintf(
		"%s --config=%s --stanza=$(awk '/^\\[pgelastic-/{gsub(/[][]/,\"\"); print; exit}' %s) "+
			"--output=json info",
		"pgbackrest", provision.BackupConfigFile, provision.BackupConfigFile,
	)).Output()
	Expect(err).NotTo(HaveOccurred())
	return string(output)
}

func runSQL(statement string) string {
	GinkgoHelper()
	return runSQLIn(archiveInstance, statement)
}

func runSQLIn(instance, statement string) string {
	GinkgoHelper()
	primary := primaryOf(instance)
	output, err := kubectlCommand(
		"exec", "-n", archiveNamespace, primary, "-c", "postgres", "--",
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-tAc", statement,
	).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(output))
	return string(output)
}

func inRestored(args ...string) string {
	GinkgoHelper()
	primary := primaryOf(restoredInstance)
	output, err := kubectlCommand(append([]string{
		"exec", "-n", archiveNamespace, primary, "-c", "postgres", "--",
	}, args...)...).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(output))
	return string(output)
}

func primaryOf(name string) string {
	GinkgoHelper()
	instance := &pgelasticv1alpha1.PgInstance{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: archiveNamespace, Name: name,
	}, instance)).To(Succeed())
	Expect(instance.Status.CurrentPrimary).NotTo(BeEmpty(),
		fmt.Sprintf("%s has no primary to run against", name))
	return instance.Status.CurrentPrimary
}
