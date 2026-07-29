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
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
	"github.com/andrew01234567890/pgelastic/internal/proxy"
)

const rollNamespace = "pginstance-roll"

// fakeQuiescer records the order the roll drives the proxy in, and lets a spec keep an
// instance from ever draining.
type fakeQuiescer struct {
	mu       sync.Mutex
	calls    []string
	inFlight int64
	headless bool
}

func (q *fakeQuiescer) record(call string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls = append(q.calls, call)
}

func (q *fakeQuiescer) journal() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.calls...)
}

func (q *fakeQuiescer) QuiesceInstance(_ context.Context, _ client.ObjectKey, _, _ string) error {
	q.record("quiesce")
	return nil
}

func (q *fakeQuiescer) InstanceDrainStatus(
	_ context.Context,
	_ client.ObjectKey,
	_ string,
) (proxy.InstanceDrain, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.headless {
		return proxy.InstanceDrain{}, nil
	}
	return proxy.InstanceDrain{
		Known:    true,
		Quiesced: true,
		InFlight: q.inFlight,
		Drained:  q.inFlight == 0,
	}, nil
}

func (q *fakeQuiescer) ResumeInstance(_ context.Context, _ client.ObjectKey, _, _ string) error {
	q.record("resume")
	return nil
}

func (q *fakeQuiescer) ReleaseInstance(_ context.Context, _ client.ObjectKey, _, _ string) error {
	q.record("release")
	return nil
}

// bringUpForRoll drives the provisioning ladder to the steady state every roll spec starts
// from: three members, all Ready with a Pod IP, a primary that has published its quorum
// evidence, and every Pod carrying the stamp its configuration produced.
func bringUpForRoll(
	reconciler *PgInstanceReconciler,
	prober *fakeProber,
	name string,
) (*pgelasticv1alpha1.PgInstance, []string) {
	GinkgoHelper()
	instance := makeInstance(name)
	instance.Namespace = rollNamespace
	Expect(k8sClient.Create(ctx, instance)).To(Succeed())

	members := []string{
		provision.MemberName(name, 1),
		provision.MemberName(name, 2),
		provision.MemberName(name, 3),
	}
	reconcileNow(reconciler, instance)
	for _, claim := range claimsOf(instance) {
		bind(&claim)
	}
	for i, member := range members {
		if i == 0 {
			prober.set(member, primaryReport(member, 1, "0/3000000"))
		} else {
			prober.set(member, standbyReport(member, 1, "0/3000000"))
		}
		Eventually(func(g Gomega) {
			reconcileNow(reconciler, instance)
			g.Expect(podsOf(instance)).To(HaveLen(i + 1))
		}).Should(Succeed())
		pods := podsOf(instance)
		for j := range pods {
			markPodReadyAt(&pods[j], podIPOf(pods[j].Name))
		}
		if i == 0 {
			reportPrimaryIn(instance, members[0])
		}
	}
	reconcileNow(reconciler, instance)
	return instance, members
}

// reportPrimaryIn is reportPrimary for this file's namespace, writing what the primary's
// own agent writes: the role it holds and the clause its postmaster loaded.
func reportPrimaryIn(instance *pgelasticv1alpha1.PgInstance, member string) {
	GinkgoHelper()
	fetched := refetch(instance)
	standbys := []string{
		provision.MemberName(instance.Name, 2),
		provision.MemberName(instance.Name, 3),
	}
	fetched.Status.CurrentPrimary = member
	fetched.Status.QuorumEvidence = &pgelasticv1alpha1.QuorumEvidence{
		SynchronousStandbyNames: fmt.Sprintf("ANY 1 (%q,%q)", standbys[0], standbys[1]),
		NumSync:                 1,
		VotingMembers:           standbys,
		StreamingMembers:        standbys,
		ReportedBy:              member,
		ObservedAt:              &metav1.Time{Time: time.Now()},
	}
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

// concurrentDumpsAfter is the raised value. Concurrent dumps are charged to the agent's
// own reserved band, which is a term of max_connections, so raising them is a genuine
// PGC_POSTMASTER change made through a field that is not immutable.
const concurrentDumpsAfter int32 = 6

// raiseMaxConnections changes a parameter PostgreSQL can only adopt at startup.
func raiseMaxConnections(instance *pgelasticv1alpha1.PgInstance) {
	GinkgoHelper()
	dumps := concurrentDumpsAfter
	fetched := refetch(instance)
	fetched.Spec.PerTenantLogicalBackup = &pgelasticv1alpha1.PerTenantLogicalBackup{
		MaxConcurrentDumps: &dumps,
	}
	Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
}

// recreate is what the kubelet and the operator between them do to a Pod the roll deleted:
// the Pod goes, the next reconcile makes a new one, and it comes back Ready.
func recreate(reconciler *PgInstanceReconciler, instance *pgelasticv1alpha1.PgInstance, member string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		reconcileNow(reconciler, instance)
		pod := &corev1.Pod{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: rollNamespace, Name: member},
			pod)).To(Succeed())
		g.Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
		markPodReadyAt(pod, podIPOf(member))
	}).Should(Succeed())
	reconcileNow(reconciler, instance)
}

// deleteNow removes an object without waiting for a grace period nothing in envtest will
// ever honour.
func deleteNow(object client.Object) {
	GinkgoHelper()
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, object,
		client.GracePeriodSeconds(0)))).To(Succeed())
	awaitCachedGone(object)
}

func rollOf(instance *pgelasticv1alpha1.PgInstance) *pgelasticv1alpha1.InstanceRollStatus {
	GinkgoHelper()
	return refetch(instance).Status.Roll
}

func podGone(member string) bool {
	GinkgoHelper()
	return !present(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: member, Namespace: rollNamespace}})
}

// nodeFor puts a member on a node, because the drain trap is decided from where the
// members are and which of those places is going away.
func nodeFor(member, node string) {
	GinkgoHelper()
	Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node},
	}))).To(Succeed())
	pod := refetch(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: member, Namespace: rollNamespace}})
	if pod.Spec.NodeName == node {
		return
	}
	Expect(k8sClient.SubResource("binding").Create(ctx, pod, &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: member, Namespace: rollNamespace},
		Target:     corev1.ObjectReference{Kind: "Node", Name: node},
	})).To(Succeed())
}

func cordon(node string, unschedulable bool) {
	GinkgoHelper()
	fetched := refetch(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node}})
	fetched.Spec.Unschedulable = unschedulable
	Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
}

// publishReadWriteEndpoints is what the endpoint controller does once the role label moves.
// The roll waits for it before releasing clients, so a spec that never publishes it is a
// spec asserting the roll waits.
func publishReadWriteEndpoints(instance *pgelasticv1alpha1.PgInstance, member string) {
	GinkgoHelper()
	name := provision.PrimaryServiceName(instance.Name)
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rollNamespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: name},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	_, err := controllerutilCreateOrUpdate(slice, func() {
		slice.Endpoints = []discoveryv1.Endpoint{{Addresses: []string{podIPOf(member)}}}
	})
	Expect(err).NotTo(HaveOccurred())
}

func controllerutilCreateOrUpdate(slice *discoveryv1.EndpointSlice, mutate func()) (bool, error) {
	existing := &discoveryv1.EndpointSlice{}
	err := k8sClient.Get(ctx, client.ObjectKeyFromObject(slice), existing)
	if err == nil {
		mutate()
		existing.Endpoints = slice.Endpoints
		return false, k8sClient.Update(ctx, existing)
	}
	mutate()
	return true, k8sClient.Create(ctx, slice)
}

var _ = Describe("PgInstance rolling restart", func() {
	var (
		reconciler *PgInstanceReconciler
		prober     *fakeProber
		quiescer   *fakeQuiescer
		instance   *pgelasticv1alpha1.PgInstance
		members    []string
	)

	BeforeEach(func() {
		ensureNamespace(rollNamespace)
		claimPool(rollNamespace, "pginstance-roll-class", "saas-pool")
		prober = &fakeProber{reports: map[string]provision.MemberReport{}}
		quiescer = &fakeQuiescer{}
		reconciler = newInstanceReconciler()
		reconciler.Prober = prober
		reconciler.Quiescer = quiescer
	})

	AfterEach(func() {
		if instance != nil {
			deleteAndAwait(instance)
			for _, member := range members {
				// Force, because envtest has no kubelet: a Pod that was bound to a Node
				// stays Terminating forever waiting for one to confirm it is gone.
				deleteNow(&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: member, Namespace: rollNamespace}})
			}
			instance = nil
		}
	})

	It("leaves an instance whose members are all current alone", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-steady")

		reconcileNow(reconciler, instance)

		Expect(rollOf(instance)).To(BeNil())
		Expect(quiescer.journal()).To(BeEmpty(), "nothing may be held when nothing is being rolled")
		Expect(conditionOf(refetch(instance).Status.Conditions,
			pgelasticv1alpha1.ConditionRolling).Status).To(Equal(metav1.ConditionFalse))
	})

	It("restarts the most-lagged replica first and the primary last", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-order")
		// Member three is furthest behind, so it is the one whose absence costs least and
		// whose catch-up costs most: it goes first.
		prober.set(members[1], standbyReport(members[1], 1, "0/5000000"))
		prober.set(members[2], standbyReport(members[2], 1, "0/4000000"))
		raiseMaxConnections(instance)

		reconcileNow(reconciler, instance)

		Expect(rollOf(instance).Member).To(Equal(members[2]))
		Expect(rollOf(instance).Reason).To(Equal(pgelasticv1alpha1.RollReasonConfigChanged))
		Expect(rollOf(instance).Pending).To(BeEquivalentTo(3))
		Expect(podGone(members[2])).To(BeTrue())
		Expect(podGone(members[1])).To(BeFalse(), "only one member may be down at a time")
		Expect(podGone(members[0])).To(BeFalse())

		recreate(reconciler, instance, members[2])
		Expect(rollOf(instance).Member).To(Equal(members[1]))
		Expect(podGone(members[1])).To(BeTrue())

		recreate(reconciler, instance, members[1])
		Expect(rollOf(instance).Member).To(Equal(members[0]),
			"the member holding the role is disrupted last")
		Expect(podGone(members[0])).To(BeFalse(),
			"the primary is never deleted before its role has been handed away")
	})

	It("refuses to disrupt a second member while the first is still coming back", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-one-at-a-time")
		raiseMaxConnections(instance)
		reconcileNow(reconciler, instance)
		rolled := rollOf(instance).Member

		// The Pod is back but the kubelet has not called it Ready. Under quorum ANY 1 a
		// second member going down here is every commit on the instance stalling.
		Eventually(func(g Gomega) {
			reconcileNow(reconciler, instance)
			g.Expect(podGone(rolled)).To(BeFalse())
		}).Should(Succeed())
		reconcileNow(reconciler, instance)

		Expect(rollOf(instance).Step).To(Equal(pgelasticv1alpha1.RollStepBlocked))
		Expect(rollOf(instance).Message).To(ContainSubstring("of 3 members are Ready"))
		for _, member := range members {
			if member != rolled {
				Expect(podGone(member)).To(BeFalse())
			}
		}
		Expect(conditionOf(refetch(instance).Status.Conditions,
			pgelasticv1alpha1.ConditionRolling).Reason).
			To(Equal(pgelasticv1alpha1.ReasonRollBlocked))
	})

	It("holds the clients before it names the primary, and never the other way round", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-quiesce-first")
		quiescer.inFlight = 1
		raiseMaxConnections(instance)
		rollReplicas(reconciler, instance, members)

		reconcileNow(reconciler, instance)

		Expect(rollOf(instance).Member).To(Equal(members[0]))
		Expect(rollOf(instance).Step).To(Equal(pgelasticv1alpha1.RollStepQuiescing))
		Expect(quiescer.journal()).To(ContainElement("quiesce"))
		Expect(refetch(instance).Annotations).NotTo(HaveKey(ha.AnnotationMaintenance),
			"a member named before its clients are drained is a handover that drops them")

		quiescer.inFlight = 0
		reconcileNow(reconciler, instance)

		Expect(ha.MaintenanceMembers(refetch(instance).Annotations)).To(ConsistOf(members[0]))
		Expect(rollOf(instance).Step).To(Equal(pgelasticv1alpha1.RollStepSwitchingOver))
	})

	It("gives the clients back only once the read-write Service selects the new primary", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-resume")
		raiseMaxConnections(instance)
		rollReplicas(reconciler, instance, members)
		reconcileNow(reconciler, instance)
		Expect(ha.MaintenanceMembers(refetch(instance).Annotations)).To(ConsistOf(members[0]))

		// The handover happened: another member reports itself out of recovery and claims
		// the role, exactly as its own agent would.
		prober.set(members[0], standbyReport(members[0], 2, "0/6000000"))
		prober.set(members[1], primaryReport(members[1], 2, "0/6000000"))
		reportPrimaryIn(instance, members[1])
		reconcileNow(reconciler, instance)

		Expect(quiescer.journal()).NotTo(ContainElement("resume"),
			"a client released before the Service moved is a client handed a refused connection")
		Expect(rollOf(instance).Step).To(Equal(pgelasticv1alpha1.RollStepSwitchingOver))

		publishReadWriteEndpoints(instance, members[1])
		reconcileNow(reconciler, instance)

		Expect(quiescer.journal()).To(ContainElement("resume"))
		Expect(refetch(instance).Annotations).NotTo(HaveKey(ha.AnnotationMaintenance))
	})

	It("gives the clients back rather than queueing them behind a drain that never arrives", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-stuck-drain")
		quiescer.inFlight = 1
		raiseMaxConnections(instance)
		rollReplicas(reconciler, instance, members)
		reconcileNow(reconciler, instance)
		Expect(rollOf(instance).Step).To(Equal(pgelasticv1alpha1.RollStepQuiescing))

		backdateRollStep(instance, 2*provision.SwitchoverTimeout(instance.Spec))
		reconcileNow(reconciler, instance)

		Expect(quiescer.journal()).To(ContainElement("release"))
		Expect(rollOf(instance).Step).To(Equal(pgelasticv1alpha1.RollStepStalled))
		Expect(rollOf(instance).Message).To(ContainSubstring("still in flight"))
		Expect(refetch(instance).Annotations).NotTo(HaveKey(ha.AnnotationMaintenance))

		// And it leaves the instance alone rather than queueing every client again for
		// another whole budget, which is what trying immediately would cost the tenants.
		held := len(quiescer.journal())
		reconcileNow(reconciler, instance)
		reconcileNow(reconciler, instance)
		Expect(quiescer.journal()).To(HaveLen(held),
			"the roll took the clients again straight after giving them back")
		Expect(rollOf(instance).Step).To(Equal(pgelasticv1alpha1.RollStepStalled))
	})

	It("rolls every member for an explicit restart request", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-annotation")

		fetched := refetch(instance)
		fetched.Annotations = map[string]string{
			pgelasticv1alpha1.AnnotationRestartedAt: "2026-07-29T09:00:00Z",
		}
		Expect(k8sClient.Update(ctx, fetched)).To(Succeed())
		reconcileNow(reconciler, instance)

		Expect(rollOf(instance).Reason).To(Equal(pgelasticv1alpha1.RollReasonRestartRequested))
		Expect(rollOf(instance).Pending).To(BeEquivalentTo(3))

		rolled := rollOf(instance).Member
		Expect(rolled).NotTo(Equal(members[0]))
		recreate(reconciler, instance, rolled)
		Expect(refetch(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: rolled, Namespace: rollNamespace}}).
			Annotations).To(HaveKeyWithValue(provision.AnnotationRestartedAt,
			"2026-07-29T09:00:00Z"))
	})

	It("hands the role away when the primary's node is being drained", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-drain")
		nodeFor(members[0], "roll-drain-a")
		nodeFor(members[1], "roll-drain-b")
		nodeFor(members[2], "roll-drain-b")

		cordon("roll-drain-a", true)
		reconcileNow(reconciler, instance)

		Expect(rollOf(instance)).NotTo(BeNil())
		Expect(rollOf(instance).Member).To(Equal(members[0]))
		Expect(rollOf(instance).Reason).To(Equal(pgelasticv1alpha1.RollReasonNodeDraining))
		Expect(quiescer.journal()).To(ContainElement("quiesce"))
	})

	It("refuses the handover when every member's node is going away", func() {
		instance, members = bringUpForRoll(reconciler, prober, "roll-drain-all")
		for _, member := range members {
			nodeFor(member, "roll-drain-only")
		}
		cordon("roll-drain-only", true)

		reconcileNow(reconciler, instance)

		Expect(rollOf(instance)).To(BeNil(),
			"handing the role to a member with the same problem is a switchover loop")
		Expect(quiescer.journal()).To(BeEmpty())
	})
})

// rollReplicas takes the roll past both standbys, leaving the primary as the only member
// still owed a restart.
func rollReplicas(
	reconciler *PgInstanceReconciler,
	instance *pgelasticv1alpha1.PgInstance,
	members []string,
) {
	GinkgoHelper()
	for range members[1:] {
		reconcileNow(reconciler, instance)
		roll := rollOf(instance)
		Expect(roll).NotTo(BeNil())
		Expect(roll.Member).NotTo(Equal(members[0]))
		recreate(reconciler, instance, roll.Member)
	}
}

// backdateRollStep ages the current step, which is how a spec reaches a deadline without
// waiting for it.
func backdateRollStep(instance *pgelasticv1alpha1.PgInstance, by time.Duration) {
	GinkgoHelper()
	fetched := refetch(instance)
	fetched.Status.Roll.StartedAt = &metav1.Time{Time: time.Now().Add(-by)}
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}
