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
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/instance/pgconf"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

func makeInstance(name string) *pgelasticv1alpha1.PgInstance {
	return &pgelasticv1alpha1.PgInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instanceNamespace},
		Spec: pgelasticv1alpha1.PgInstanceSpec{
			PoolRef: corev1.LocalObjectReference{Name: "saas-pool"},
			Class:   instanceClassName,
			Storage: pgelasticv1alpha1.InstanceStorage{
				Size:      *quantity("10Gi"),
				WALVolume: pgelasticv1alpha1.WALVolume{Size: *quantity("2Gi")},
			},
		},
	}
}

func newInstanceReconciler() *PgInstanceReconciler {
	return &PgInstanceReconciler{
		Client:        k8sClient,
		Scheme:        k8sClient.Scheme(),
		PostgresImage: "pgelastic/postgres:18",
		AgentImage:    "pgelastic/instance:latest",
		AntiAffinity:  provision.AntiAffinityRequired,
		PeerSources:   []string{"all"},
	}
}

func claimsOf(instance *pgelasticv1alpha1.PgInstance) []corev1.PersistentVolumeClaim {
	GinkgoHelper()
	claims := &corev1.PersistentVolumeClaimList{}
	Expect(k8sClient.List(ctx, claims, client.InNamespace(instance.Namespace),
		client.MatchingLabels{provision.LabelInstanceName: instance.Name})).To(Succeed())
	return claims.Items
}

func podsOf(instance *pgelasticv1alpha1.PgInstance) []corev1.Pod {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(k8sClient.List(ctx, pods, client.InNamespace(instance.Namespace),
		client.MatchingLabels{provision.LabelInstanceName: instance.Name})).To(Succeed())
	return pods.Items
}

// bind fakes what a CSI driver and the kubelet would do, so the specs can exercise the
// operator's own ordering rather than envtest's absence of a scheduler.
func bind(claims ...*corev1.PersistentVolumeClaim) {
	GinkgoHelper()
	for _, claim := range claims {
		fetched := refetch(claim)
		fetched.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
	}
}

func markPodReady(pod *corev1.Pod) {
	GinkgoHelper()
	fetched := refetch(pod)
	fetched.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
	}
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

// instanceNamespace is where every spec in this file provisions.
const instanceNamespace = "pginstance-controller"

var _ = Describe("PgInstance controller", func() {

	var (
		reconciler *PgInstanceReconciler
		instance   *pgelasticv1alpha1.PgInstance
	)

	BeforeEach(func() {
		ensureNamespace(instanceNamespace)
		reconciler = newInstanceReconciler()
	})

	AfterEach(func() {
		if instance != nil {
			deleteAndAwait(instance)
			instance = nil
		}
	})

	Context("PVC groups", func() {
		It("creates a complete data and WAL pair for every member", func() {
			instance = makeInstance("pvc-groups")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			claims := claimsOf(instance)
			Expect(claims).To(HaveLen(6))

			groups := provision.GroupsOf(claims)
			Expect(groups).To(HaveLen(3))
			for _, group := range groups {
				Expect(group.Complete()).To(BeTrue(),
					"serial %d must have both of its volumes", group.Serial)
				Expect(group.Data.Spec.AccessModes).To(
					ConsistOf(corev1.ReadWriteOncePod))
				Expect(group.WAL.Spec.AccessModes).To(
					ConsistOf(corev1.ReadWriteOncePod))
			}
		})

		It("labels each claim with its role and serial so identity survives the pods", func() {
			instance = makeInstance("pvc-labels")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			claims := claimsOf(instance)
			roles := map[string]int{}
			for i := range claims {
				roles[claims[i].Labels[provision.LabelPVCRole]]++
				Expect(claims[i].Labels).To(HaveKey(provision.LabelNodeSerial))
				Expect(claims[i].Annotations).To(HaveKeyWithValue(
					provision.AnnotationPVCStatus, provision.PVCStatusInitializing))
			}
			Expect(roles).To(Equal(map[string]int{provision.PVCRoleData: 3, provision.PVCRoleWAL: 3}))
		})

		It("marks a group ready only once both of its claims are bound", func() {
			instance = makeInstance("pvc-ready")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			groups := provision.GroupsOf(claimsOf(instance))
			bind(groups[0].Data)
			reconcileNow(reconciler, instance)

			Expect(refetch(groups[0].Data).Annotations[provision.AnnotationPVCStatus]).
				To(Equal(provision.PVCStatusInitializing),
					"half a group is not a group")

			bind(groups[0].WAL)
			reconcileNow(reconciler, instance)
			Expect(refetch(groups[0].Data).Annotations[provision.AnnotationPVCStatus]).
				To(Equal(provision.PVCStatusReady))
			Expect(refetch(groups[0].WAL).Annotations[provision.AnnotationPVCStatus]).
				To(Equal(provision.PVCStatusReady))
		})

		It("does not schedule a member onto an incomplete group", func() {
			instance = makeInstance("pvc-incomplete")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			pods := podsOf(instance)
			Expect(pods).To(HaveLen(1))
			markPodReady(&pods[0])
			fetched := refetch(instance)
			fetched.Status.CurrentPrimary = pods[0].Name
			Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())

			// Hold the second member's WAL claim in Terminating. The group still exists,
			// so nothing recreates it, and a Pod placed on the surviving half would put
			// pg_wal on the data volume - the exact layout the separate volume prevents.
			groups := provision.GroupsOf(claimsOf(instance))
			wal := refetch(groups[1].WAL)
			wal.Finalizers = append(wal.Finalizers, "pgelastic.io/test-hold")
			Expect(k8sClient.Update(ctx, wal)).To(Succeed())
			Expect(k8sClient.Delete(ctx, wal)).To(Succeed())
			Eventually(func() bool {
				return !refetch(wal).DeletionTimestamp.IsZero()
			}).Should(BeTrue())

			reconcileNow(reconciler, instance)
			reconcileNow(reconciler, instance)
			Expect(podsOf(instance)).To(HaveLen(1),
				"a member must never be scheduled onto half a volume group")

			held := refetch(wal)
			held.Finalizers = nil
			Expect(k8sClient.Update(ctx, held)).To(Succeed())
		})
	})

	Context("member creation order", func() {
		It("creates the first member and then waits for a primary to exist", func() {
			instance = makeInstance("ordering")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)
			reconcileNow(reconciler, instance)

			pods := podsOf(instance)
			Expect(pods).To(HaveLen(1))
			Expect(pods[0].Name).To(Equal(provision.MemberName(instance.Name, 1)))

			// A second member cannot bootstrap until there is a primary to clone from, so
			// no amount of reconciling produces one while currentPrimary is empty.
			reconcileNow(reconciler, instance)
			reconcileNow(reconciler, instance)
			Expect(podsOf(instance)).To(HaveLen(1))
		})

		It("creates the second member once the first is primary and ready", func() {
			instance = makeInstance("ordering-primary")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)
			reconcileNow(reconciler, instance)

			pods := podsOf(instance)
			Expect(pods).To(HaveLen(1))
			markPodReady(&pods[0])

			fetched := refetch(instance)
			fetched.Status.CurrentPrimary = pods[0].Name
			Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())

			reconcileNow(reconciler, instance)
			Expect(podsOf(instance)).To(HaveLen(2))
		})

		It("gives the first member an init container that only copies the agent", func() {
			instance = makeInstance("agent-install")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)
			reconcileNow(reconciler, instance)

			pods := podsOf(instance)
			Expect(pods).To(HaveLen(1))
			install := pods[0].Spec.InitContainers[0]
			Expect(install.Image).To(Equal("pgelastic/instance:latest"))
			Expect(install.Command).To(Equal(
				[]string{"cp", provision.SourceAgentBinary, provision.AgentBinary}))
			Expect(pods[0].Spec.Containers[0].Command).To(Equal(
				[]string{provision.AgentBinary, "run"}))
		})
	})

	Context("supporting objects", func() {
		It("creates the peer, read-write and read-only Services", func() {
			instance = makeInstance("services")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			peer := &corev1.Service{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: instanceNamespace, Name: provision.PeerServiceName(instance.Name)}, peer)).To(Succeed())
			Expect(peer.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(peer.Spec.PublishNotReadyAddresses).To(BeTrue())

			readWrite := &corev1.Service{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: instanceNamespace,
				Name:      provision.PrimaryServiceName(instance.Name)}, readWrite)).To(Succeed())
			Expect(readWrite.Spec.Selector).To(HaveKeyWithValue(provision.LabelRole, "primary"))
		})

		It("creates two PodDisruptionBudgets keyed on the role label", func() {
			instance = makeInstance("budgets")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			replicaBudget := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: instanceNamespace, Name: provision.ReplicaPDBName(instance.Name)},
				replicaBudget)).To(Succeed())
			Expect(replicaBudget.Spec.MinAvailable.IntValue()).To(Equal(1),
				"one sync-capable standby must always survive or an ANY 1 commit stalls")

			primaryBudget := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: instanceNamespace, Name: provision.PrimaryPDBName(instance.Name)},
				primaryBudget)).To(Succeed())
			Expect(primaryBudget.Spec.MinAvailable.IntValue()).To(Equal(1))
		})

		It("generates credentials without a password for the superuser", func() {
			instance = makeInstance("credentials")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: instanceNamespace,
				Name:      provision.CredentialsSecretName(instance.Name)}, secret)).To(Succeed())
			Expect(secret.Data).To(HaveKey(provision.SecretKeyReplicationPassword))
			Expect(secret.Data).To(HaveKey(provision.SecretKeyOpsPassword))
			Expect(secret.Data).To(HaveKey(provision.SecretKeyRewindPassword))
			Expect(secret.Data).To(HaveLen(3))
		})

		It("hands the agent a configuration carrying the derived capacity", func() {
			instance = makeInstance("agent-config")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			configMap := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{
				Namespace: instanceNamespace,
				Name:      provision.ConfigMapName(instance.Name)}, configMap)).To(Succeed())
			Expect(configMap.Data).To(HaveKey(provision.ConfigFileName))
			Expect(configMap.Data[provision.ConfigFileName]).To(ContainSubstring(`"quorum": "ANY 1"`))
		})
	})

	Context("status projection", func() {
		It("publishes the derived max_connections split", func() {
			instance = makeInstance("capacity")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			class, err := pgconf.LookupSizingClass(instanceClassName)
			Expect(err).NotTo(HaveOccurred())
			want := pgconf.DeriveCapacity(class.AllocatableConnections, 4, 3, migrationSlotHeadroom)

			status := refetch(instance).Status
			Expect(status.Capacity).NotTo(BeNil())
			Expect(status.Capacity.MaxConnections).To(Equal(want.MaxConnections))
			Expect(status.Capacity.ReservedForAdmin).To(Equal(int32(8)))
			Expect(status.Capacity.MaxConnections).To(Equal(
				want.Allocatable + want.SuperuserReserved + want.Reserved + want.AgentOverhead))
		})

		It("withholds allocatable capacity until every member is serving", func() {
			instance = makeInstance("allocatable")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			status := refetch(instance).Status
			Expect(status.Capacity.Allocatable).To(BeZero(),
				"a half-built instance must not have its headroom counted as available")
			Expect(status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseBootstrapping))
		})

		It("leaves the primary epoch to the member that holds the role", func() {
			instance = makeInstance("epoch")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			status := refetch(instance).Status
			Expect(status.PrimaryEpoch).To(BeZero(),
				"an operator that owned the fence token could drive it backwards from a stale read")
			Expect(status.TargetPrimary).To(Equal(provision.MemberName(instance.Name, 1)))
		})

		It("never lowers the primary epoch", func() {
			instance = makeInstance("epoch-monotonic")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			fetched := refetch(instance)
			fetched.Status.PrimaryEpoch = 9
			Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())

			reconcileNow(reconciler, instance)
			Expect(refetch(instance).Status.PrimaryEpoch).To(Equal(int64(9)))
		})

		It("marks the members the primary counts towards the quorum", func() {
			instance = makeInstance("sync-set")
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			fetched := refetch(instance)
			fetched.Status.Instances = []pgelasticv1alpha1.InstanceMemberStatus{
				{Name: instance.Name + "-1", Role: pgelasticv1alpha1.InstanceRolePrimary},
				{Name: instance.Name + "-2", Role: pgelasticv1alpha1.InstanceRoleReplica},
				{Name: instance.Name + "-3", Role: pgelasticv1alpha1.InstanceRoleReplica},
			}
			fetched.Status.QuorumEvidence = &pgelasticv1alpha1.QuorumEvidence{
				SynchronousStandbyNames: `ANY 1 ("sync-set-2","sync-set-3")`,
				NumSync:                 1,
				VotingMembers:           []string{instance.Name + "-2", instance.Name + "-3"},
			}
			Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())

			reconcileNow(reconciler, instance)
			members := refetch(instance).Status.Instances
			inSyncSet := map[string]bool{}
			for _, member := range members {
				inSyncSet[member.Name] = member.InSyncSet
			}
			Expect(inSyncSet).To(Equal(map[string]bool{
				instance.Name + "-1": false,
				instance.Name + "-2": true,
				instance.Name + "-3": true,
			}))
		})

		It("refuses an unknown sizing class instead of guessing", func() {
			instance = makeInstance("unknown-class")
			instance.Spec.Class = "gp-does-not-exist"
			Expect(k8sClient.Create(ctx, instance)).To(Succeed())
			reconcileNow(reconciler, instance)

			status := refetch(instance).Status
			Expect(status.Phase).To(Equal(pgelasticv1alpha1.InstancePhasePending))
			Expect(conditionOf(status.Conditions, pgelasticv1alpha1.ConditionReady).Reason).
				To(Equal(pgelasticv1alpha1.ReasonInvalidSpec))
			Expect(claimsOf(instance)).To(BeEmpty())
		})
	})
})
