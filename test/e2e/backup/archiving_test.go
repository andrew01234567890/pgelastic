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
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

const (
	archiveNamespace  = "pgelastic-e2e-backup"
	archiveInstance   = "pg-archive"
	credentialsSecret = "object-store-credentials"
)

func archivingSpecs() {
	Describe("WAL archiving to an object store", Ordered, func() {
		BeforeAll(func() {
			probeNamespace.Store(archiveNamespace)

			Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: archiveNamespace},
			}))).To(Succeed())
			claimNamespace(archiveNamespace)

			caPEM := deployObjectStore(archiveNamespace)
			Expect(k8sClient.Create(suiteCtx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      credentialsSecret,
					Namespace: archiveNamespace,
				},
				Data: map[string][]byte{
					provision.SecretKeyBackupAccessKeyID:     []byte(objectStoreAccessKey),
					provision.SecretKeyBackupSecretAccessKey: []byte(objectStoreSecretKey),
					// Without this pgBackRest refuses the store's certificate, which is the
					// normal condition for an S3-compatible store running inside a cluster.
					provision.SecretKeyBackupCABundle: caPEM,
				},
			})).To(Succeed())

			instance := &pgelasticv1alpha1.PgInstance{
				ObjectMeta: metav1.ObjectMeta{Name: archiveInstance, Namespace: archiveNamespace},
				Spec: pgelasticv1alpha1.PgInstanceSpec{
					PoolRef: corev1.LocalObjectReference{Name: claimPoolName},
					Class:   sizingClass,
					Storage: pgelasticv1alpha1.InstanceStorage{
						Size:      resource.MustParse("1Gi"),
						WALVolume: pgelasticv1alpha1.WALVolume{Size: resource.MustParse("1Gi")},
					},
					Backup: &pgelasticv1alpha1.InstanceBackup{
						ObjectStore: pgelasticv1alpha1.ObjectStore{
							Path:                 "s3://" + objectStoreBucket + "/pgelastic",
							EndpointURL:          objectStoreEndpoint(archiveNamespace),
							CredentialsSecretRef: corev1.LocalObjectReference{Name: credentialsSecret},
						},
					},
				},
			}
			Expect(k8sClient.Create(suiteCtx, instance)).To(Succeed())

			By("waiting for the instance to serve")
			Eventually(func(g Gomega) {
				served := readInstance(g)
				g.Expect(served.Status.CurrentPrimary).NotTo(BeEmpty())
				g.Expect(meta.IsStatusConditionTrue(served.Status.Conditions,
					pgelasticv1alpha1.ConditionReady)).To(BeTrue())
			}).Should(Succeed())
		})

		// The claim this suite exists to make. Everything else here is about how it is reported.
		It("pushes a segment into the repository and reads the same segment back out", func() {
			// The segment is named before it is closed, rather than read afterwards out of
			// pg_stat_archiver. last_archived_wal is the last *file* archived, and that is
			// very often not a segment at all: cloning the standbys leaves
			// <segment>.<offset>.backup label files in the archive, a few hundred bytes each,
			// and asking for one of those back produces a perfectly successful fetch of
			// something that is not WAL.
			// Something has to be written into the segment before it is switched.
			// pg_switch_wal() is documented to do nothing at all when no WAL has been written
			// since the last switch, and an instance that has just finished cloning its
			// standbys is exactly that: the segment stays open, is never marked .ready, and is
			// archived only when archive_timeout eventually expires five minutes later.
			runSQL("SELECT pg_logical_emit_message(true, 'pgelastic-e2e', 'archive probe')")

			segment := strings.TrimSpace(runSQL("SELECT pg_walfile_name(pg_current_wal_lsn())"))
			Expect(segment).To(HaveLen(24), "not a WAL segment name: "+segment)

			By("closing " + segment + " so there is something to archive")
			switchWAL()

			Eventually(func(g Gomega) {
				health := readInstance(g).Status.ArchiveHealth
				g.Expect(health).NotTo(BeNil(), "the primary never published its archiving")
				g.Expect(health.Healthy).To(BeTrue(), health.LastFailureMessage)
			}).Should(Succeed())

			// Reading it back is the only assertion that proves the bytes arrived. A push that
			// reported success and wrote nothing looks identical from the sending side, and the
			// difference is discovered at restore time, when there is nothing left to fall back
			// on.
			By("fetching " + segment + " back out of the repository")
			const restored = "/tmp/e2e-restored-segment"
			Eventually(func(g Gomega) {
				output, err := inPrimary(
					provision.AgentBinary, "wal-restore", "--name", segment, "--target", restored,
				).CombinedOutput()
				g.Expect(err).NotTo(HaveOccurred(), string(output))
			}).Should(Succeed())

			size, err := inPrimary("stat", "-c", "%s", restored).Output()
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(string(size))).To(Equal("16777216"),
				"a restored segment that is not a whole WAL segment is a truncated object")
		})

		It("reports the archive on the instance's conditions", func() {
			Eventually(func(g Gomega) {
				condition := meta.FindStatusCondition(readInstance(g).Status.Conditions,
					pgelasticv1alpha1.ConditionArchiving)
				g.Expect(condition).NotTo(BeNil(),
					"an instance with a repository must say whether WAL is reaching it")
				g.Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(condition.Reason).To(Equal(pgelasticv1alpha1.ReasonArchiveHealthy))
			}).Should(Succeed())
		})

		// A base backup is the thing WAL archiving exists to make useful: without one there is
		// nothing for the WAL to be replayed onto.
		It("takes a full backup and records what a restore would need to use it", func() {
			takeBackup("full-one", pgelasticv1alpha1.BackupTypeFull)

			var taken *pgelasticv1alpha1.PgBackup
			Eventually(func(g Gomega) {
				taken = readBackup(g, "full-one")
				g.Expect(taken.Status.Phase).To(Equal(pgelasticv1alpha1.BackupPhaseCompleted),
					taken.Status.Error)
			}).Should(Succeed())

			// Every one of these is what a restore is planned from. A backup recorded without
			// them is an object in a bucket that nothing knows how to use.
			Expect(taken.Status.BackupID).NotTo(BeEmpty(), "no repository label was recorded")
			Expect(taken.Status.BeginLSN).NotTo(BeEmpty())
			Expect(taken.Status.EndLSN).NotTo(BeEmpty())
			Expect(taken.Status.BeginWAL).NotTo(BeEmpty())
			Expect(taken.Status.EndWAL).NotTo(BeEmpty())
			Expect(taken.Status.SizeBytes).To(BeNumerically(">", 0))
			Expect(taken.Status.Type).To(Equal(pgelasticv1alpha1.BackupTypeFull))
			Expect(taken.Status.Member).NotTo(BeEmpty())

			// The stanza and the system identifier are recorded on the backup rather than looked
			// up from the instance, because a backup outlives its instance and that is the case
			// it exists for.
			Expect(taken.Status.Stanza).NotTo(BeEmpty())
			Expect(taken.Status.SystemIdentifier).NotTo(BeEmpty())
			Expect(taken.Status.Repository).NotTo(BeNil())

			// A restore into an instance whose max_connections is below the source's FATALs at
			// start-up with a message that names the parameter and not the cause.
			Expect(taken.Status.SourceEnforcedParameters).To(HaveKey("max_connections"))
		})

		It("summarises the backup on the instance the gates read", func() {
			Eventually(func(g Gomega) {
				summary := readInstance(g).Status.LastBackup
				g.Expect(summary).NotTo(BeNil(), "the instance never recorded its backup")
				g.Expect(summary.At).NotTo(BeNil())
				g.Expect(summary.SizeBytes).To(BeNumerically(">", 0))
				// Completing is not verification, and reporting a backup as verified because it
				// finished is an assurance worse than none.
				g.Expect(summary.Verified).To(BeFalse())
			}).Should(Succeed())
		})

		// A differential is only meaningful relative to the full it descends from, and the point
		// of taking one here is that pgBackRest accepted it as a differential rather than
		// promoting it to a full - which is what it does when there is no full to descend from.
		It("takes a differential on top of the full", func() {
			takeBackup("diff-one", pgelasticv1alpha1.BackupTypeDifferential)

			Eventually(func(g Gomega) {
				taken := readBackup(g, "diff-one")
				g.Expect(taken.Status.Phase).To(Equal(pgelasticv1alpha1.BackupPhaseCompleted),
					taken.Status.Error)
				g.Expect(taken.Status.Type).To(Equal(pgelasticv1alpha1.BackupTypeDifferential))
				g.Expect(taken.Status.SizeBytes).To(BeNumerically(">", 0))
			}).Should(Succeed())
		})

		// A repository that stops accepting writes is the failure this whole subsystem exists to
		// notice. The recovery half matters just as much: an archive that reports degraded
		// forever after one transient failure is an alarm nobody will act on the second time.
		// The degraded spec deliberately breaks the credential. Everything after it in this
		// suite needs a working one, and the whole suite is a single Ordered container now, so
		// a failure between the two rotations would leave the key wrong and take the restore
		// specs down with it for a reason that has nothing to do with them.
		AfterAll(func() {
			rotateSecretKey(objectStoreSecretKey)
		})

		It("goes degraded when the repository refuses it, and recovers when it stops", func() {
			By("rotating the secret key to one the store will reject")
			rotateSecretKey("wrong-secret-key")

			Eventually(func(g Gomega) {
				switchWAL()
				condition := meta.FindStatusCondition(readInstance(g).Status.Conditions,
					pgelasticv1alpha1.ConditionArchiving)
				g.Expect(condition).NotTo(BeNil())
				g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(condition.Reason).To(Equal(pgelasticv1alpha1.ReasonArchiveDegraded))
				// pg_stat_archiver records that a failure happened and never what it was, so a
				// message here is evidence that archive_command's own account of the failure
				// made it back to the API.
				g.Expect(condition.Message).NotTo(BeEmpty())
			}).Should(Succeed())

			// A base backup taken now could not be replayed to consistency: it needs every WAL
			// segment from its own start position, and those are exactly the ones not arriving.
			// Taking one anyway would put an object in the bucket that no restore can use, which
			// is worse than taking none because it looks like progress.
			By("asking for a backup while the repository is refusing writes")
			takeBackup("while-degraded", pgelasticv1alpha1.BackupTypeFull)

			// Pending is what the controller writes, not what the API server defaults to, so
			// there is a moment after the create where the phase is still empty. Consistently
			// fails on its first poll, so waiting for the phase to appear is what makes the
			// assertion below about staying Pending rather than about arriving there.
			Eventually(func(g Gomega) {
				g.Expect(readBackup(g, "while-degraded").Status.Phase).
					To(Equal(pgelasticv1alpha1.BackupPhasePending))
			}).Should(Succeed())

			Consistently(func(g Gomega) {
				refused := readBackup(g, "while-degraded")
				g.Expect(refused.Status.Phase).To(Equal(pgelasticv1alpha1.BackupPhasePending))
				progressing := meta.FindStatusCondition(refused.Status.Conditions,
					pgelasticv1alpha1.ConditionProgressing)
				g.Expect(progressing).NotTo(BeNil())
				g.Expect(progressing.Reason).To(Equal(pgelasticv1alpha1.ReasonArchiveDegraded))
			}, "45s", "5s").Should(Succeed())

			By("putting the working key back")
			rotateSecretKey(objectStoreSecretKey)

			Eventually(func(g Gomega) {
				switchWAL()
				g.Expect(meta.IsStatusConditionTrue(readInstance(g).Status.Conditions,
					pgelasticv1alpha1.ConditionArchiving)).To(BeTrue(),
					"archiving never recovered after the credential was fixed")
			}).Should(Succeed())

			// The refusal has to lift by itself. A backup parked forever behind a fault that has
			// since cleared is the shape CloudNativePG shipped: a phase nothing ever transitions
			// out of, which blocks every later backup because a waiting one holds the election.
			By("waiting for the refused backup to be taken now that it can be")
			Eventually(func(g Gomega) {
				resumed := readBackup(g, "while-degraded")
				g.Expect(resumed.Status.Phase).To(Equal(pgelasticv1alpha1.BackupPhaseCompleted),
					resumed.Status.Error)
			}).Should(Succeed())
		})
	})
}

// readInstance re-fetches on every poll rather than closing over one copy, so a spec
// cannot wait for a change on an object it read before the change was possible.
func readInstance(g Gomega) *pgelasticv1alpha1.PgInstance {
	fetched := &pgelasticv1alpha1.PgInstance{}
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: archiveNamespace, Name: archiveInstance,
	}, fetched)).To(Succeed())
	return fetched
}

// switchWAL forces a segment boundary so archiving has something to do.
//
// Without it a quiet instance archives nothing at all: archive_timeout only switches a
// segment when there has been WAL activity since the last switch, so a spec that merely
// waited would wait forever and blame the archive.
func switchWAL() {
	GinkgoHelper()
	output, err := inPrimary(
		"psql", "-h", provision.SocketDir, "-U", "postgres", "-tAc",
		"SELECT pg_switch_wal()",
	).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(output))
}

// rotateSecretKey rewrites the mounted credential in place. The kubelet refreshes the
// projected file without restarting the Pod, which is the whole reason the credentials are
// a volume rather than environment variables.
func rotateSecretKey(value string) {
	GinkgoHelper()
	secret := &corev1.Secret{}
	Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: archiveNamespace, Name: credentialsSecret,
	}, secret)).To(Succeed())
	secret.Data[provision.SecretKeyBackupSecretAccessKey] = []byte(value)
	Expect(k8sClient.Update(suiteCtx, secret)).To(Succeed())
}

func inPrimary(args ...string) *exec.Cmd {
	GinkgoHelper()
	primary := readInstance(Default).Status.CurrentPrimary
	Expect(primary).NotTo(BeEmpty(), "the instance has no primary to run against")
	return kubectlCommand(append([]string{
		"exec", "-n", archiveNamespace, primary, "-c", "postgres", "--",
	}, args...)...)
}

// takeBackup asks for one backup and waits for the operator to accept it.
func takeBackup(name string, kind pgelasticv1alpha1.BackupType) {
	GinkgoHelper()
	Expect(k8sClient.Create(suiteCtx, &pgelasticv1alpha1.PgBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: archiveNamespace},
		Spec: pgelasticv1alpha1.PgBackupSpec{
			InstanceRef: corev1.LocalObjectReference{Name: archiveInstance},
			Type:        kind,
		},
	})).To(Succeed())
}

func readBackup(g Gomega, name string) *pgelasticv1alpha1.PgBackup {
	fetched := &pgelasticv1alpha1.PgBackup{}
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: archiveNamespace, Name: name,
	}, fetched)).To(Succeed())
	return fetched
}
