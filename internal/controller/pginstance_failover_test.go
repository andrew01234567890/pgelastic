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
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// failoverNamespace is where every spec in this file provisions.
const failoverNamespace = "pginstance-failover"

// errUnreachable is what the fake prober returns for a member that is not answering, which
// is the only thing the operator ever learns about such a member.
var errUnreachable = errors.New("no route to the member")

// fakeProber answers for members by Pod IP, so a spec can put the instance into any
// observable state without three real postmasters.
type fakeProber struct {
	reports map[string]provision.MemberReport
}

func (p *fakeProber) Probe(_ context.Context, _ string, endpoint string) (provision.MemberReport, error) {
	report, ok := p.reports[endpoint]
	if !ok {
		return provision.MemberReport{}, errUnreachable
	}
	return report, nil
}

func (p *fakeProber) set(member string, report provision.MemberReport) {
	p.reports[endpointOf(member)] = report
}

func (p *fakeProber) silence(member string) {
	delete(p.reports, endpointOf(member))
}

// endpointOf mirrors the operator's own addressing: the Pod IP, which the specs assign
// deterministically from the member serial.
func endpointOf(member string) string {
	return net.JoinHostPort(podIPOf(member), strconv.Itoa(int(provision.StatusPort)))
}

func podIPOf(member string) string {
	serial := member[len(member)-1:]
	return "10.244.0." + serial
}

func standbyReport(member string, timeline int32, lsn string) provision.MemberReport {
	return provision.MemberReport{
		Member:            member,
		Role:              string(pgelasticv1alpha1.InstanceRoleReplica),
		InRecovery:        true,
		Healthy:           true,
		Timeline:          timeline,
		ReceivedLSN:       lsn,
		ReplayLSN:         lsn,
		WALReceiverActive: false,
	}
}

func primaryReport(member string, timeline int32, lsn string) provision.MemberReport {
	return provision.MemberReport{
		Member:      member,
		Role:        string(pgelasticv1alpha1.InstanceRolePrimary),
		Healthy:     true,
		Timeline:    timeline,
		ReceivedLSN: lsn,
		ReplayLSN:   lsn,
	}
}

var _ = Describe("PgInstance failover state machine", func() {
	var (
		reconciler *PgInstanceReconciler
		prober     *fakeProber
		instance   *pgelasticv1alpha1.PgInstance
		members    []string
	)

	// bringUp drives the provisioning ladder until all three members exist, are Ready, have
	// a Pod IP, and the instance has a primary with quorum evidence - the steady state every
	// failover spec starts from.
	bringUp := func(name string) {
		GinkgoHelper()
		instance = makeInstance(name)
		instance.Namespace = failoverNamespace
		Expect(k8sClient.Create(ctx, instance)).To(Succeed())

		members = []string{
			provision.MemberName(instance.Name, 1),
			provision.MemberName(instance.Name, 2),
			provision.MemberName(instance.Name, 3),
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
				pods := podsOf(instance)
				g.Expect(pods).To(HaveLen(i + 1))
			}).Should(Succeed())

			pods := podsOf(instance)
			for j := range pods {
				markPodReadyAt(&pods[j], podIPOf(pods[j].Name))
			}
			if i == 0 {
				reportPrimary(instance, members[0])
			}
		}
		reconcileNow(reconciler, instance)
	}

	BeforeEach(func() {
		ensureNamespace(failoverNamespace)
		claimPool(failoverNamespace, "pginstance-failover-class", "saas-pool")
		prober = &fakeProber{reports: map[string]provision.MemberReport{}}
		reconciler = newInstanceReconciler()
		reconciler.Prober = prober
	})

	AfterEach(func() {
		if instance != nil {
			deleteAndAwait(instance)
			for _, member := range members {
				deleteAndAwait(&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: member, Namespace: failoverNamespace},
				})
			}
			instance = nil
		}
	})

	It("stays steady while the primary answers", func() {
		bringUp("failover-steady")

		fetched := refetch(instance)
		Expect(fetched.Status.TargetPrimary).To(Equal(members[0]))
		Expect(fetched.Status.CurrentPrimaryFailingSince).To(BeNil())
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionFailingOver).Status).
			To(Equal(metav1.ConditionFalse))
	})

	It("debounces an unhealthy primary before starting a failover", func() {
		bringUp("failover-debounce")
		prober.silence(members[0])
		markPodNotReady(members[0])

		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(fetched.Status.CurrentPrimaryFailingSince).NotTo(BeNil(),
			"the debounce origin has to be persisted so an operator restart does not restart it")
		Expect(fetched.Status.TargetPrimary).To(Equal(members[0]),
			"nothing may change inside the failover delay")
	})

	It("writes the sentinel and strips the role label once the delay has elapsed", func() {
		bringUp("failover-sentinel")
		prober.silence(members[0])
		markPodNotReady(members[0])
		reconcileNow(reconciler, instance)
		// Both standbys still report a live WAL receiver, so phase two cannot start yet and
		// the sentinel is where the machine has to stop.
		for _, member := range members[1:] {
			report := standbyReport(member, 1, "0/3000000")
			report.WALReceiverActive = true
			prober.set(member, report)
		}
		backdateFailingSince(instance)

		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(fetched.Status.TargetPrimary).To(Equal(pgelasticv1alpha1.TargetPrimaryPending))
		Expect(refetch(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: members[0], Namespace: failoverNamespace}}).Labels).
			NotTo(HaveKey(provision.LabelRole),
				"the read-write Service must stop selecting a member nobody can reach")
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionFailingOver).Status).
			To(Equal(metav1.ConditionTrue))
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseFailingOver))
	})

	It("names the furthest-ahead candidate once every WAL receiver is down", func() {
		bringUp("failover-candidate")
		prober.silence(members[0])
		markPodNotReady(members[0])
		prober.set(members[1], standbyReport(members[1], 1, "0/4000000"))
		prober.set(members[2], standbyReport(members[2], 1, "0/5000000"))
		backdateFailingSince(instance)
		reconcileNow(reconciler, instance)
		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(fetched.Status.TargetPrimary).To(Equal(members[2]),
			"the member holding more received WAL loses less on promotion")
	})

	It("disqualifies a member below the cluster's last known timeline", func() {
		bringUp("failover-timeline")
		prober.silence(members[0])
		markPodNotReady(members[0])
		// pg-2 holds far more WAL, and is on the older timeline. Timeline leads the ordering
		// because a member below it has not replayed history the others already have.
		prober.set(members[1], standbyReport(members[1], 1, "0/9000000"))
		prober.set(members[2], standbyReport(members[2], 2, "0/1000000"))
		backdateFailingSince(instance)
		reconcileNow(reconciler, instance)
		reconcileNow(reconciler, instance)

		Expect(refetch(instance).Status.TargetPrimary).To(Equal(members[2]),
			"a higher timeline outranks any amount of WAL on a lower one")
	})

	It("denies the failover when only one standby is reachable", func() {
		bringUp("failover-quorum")
		prober.silence(members[0])
		prober.silence(members[2])
		markPodNotReady(members[0])
		backdateFailingSince(instance)
		reconcileNow(reconciler, instance)
		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(fetched.Status.TargetPrimary).To(Equal(pgelasticv1alpha1.TargetPrimaryPending),
			"with one standby visible nothing proves it acknowledged the last commit")
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionFailingOver).Reason).
			To(Equal(pgelasticv1alpha1.ReasonQuorumNotProven))
	})

	It("denies the failover outright when there is no quorum evidence", func() {
		bringUp("failover-noevidence")
		clearQuorumEvidence(instance)
		prober.silence(members[0])
		markPodNotReady(members[0])
		backdateFailingSince(instance)
		reconcileNow(reconciler, instance)
		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(fetched.Status.TargetPrimary).To(Equal(pgelasticv1alpha1.TargetPrimaryPending))
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionFailingOver).Reason).
			To(Equal(pgelasticv1alpha1.ReasonQuorumLost))
	})

	It("vetoes and never fails over when no Ready member will answer", func() {
		bringUp("failover-isolated")
		for _, member := range members {
			prober.silence(member)
		}
		backdateFailingSince(instance)
		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionOperatorIsolated).Status).
			To(Equal(metav1.ConditionTrue))
		Expect(fetched.Status.TargetPrimary).To(Equal(members[0]),
			"an isolated operator must not touch the decision at all")
	})

	It("defers while the kubelet still calls the unreachable primary Ready", func() {
		bringUp("failover-unobservable")
		prober.silence(members[0])
		backdateFailingSince(instance)

		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionPrimaryUnobservable).Status).
			To(Equal(metav1.ConditionTrue))
		Expect(fetched.Status.TargetPrimary).To(Equal(members[0]))
	})

	It("waits rather than promoting a candidate whose Pod is not Ready", func() {
		bringUp("failover-notready")
		prober.silence(members[0])
		markPodNotReady(members[0])
		markPodNotReady(members[1])
		markPodNotReady(members[2])
		backdateFailingSince(instance)
		reconcileNow(reconciler, instance)
		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionCandidateNotReady).Status).
			To(Equal(metav1.ConditionTrue))
		Expect(fetched.Status.TargetPrimary).To(Equal(pgelasticv1alpha1.TargetPrimaryPending))
	})

	It("refuses a candidate whose WAL volume is full", func() {
		bringUp("failover-walfull")
		prober.silence(members[0])
		markPodNotReady(members[0])
		for _, member := range members[1:] {
			report := standbyReport(member, 1, "0/3000000")
			report.WALVolumeFull = true
			prober.set(member, report)
		}
		backdateFailingSince(instance)
		reconcileNow(reconciler, instance)
		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionCandidateWALVolumeFull).Status).
			To(Equal(metav1.ConditionTrue))
		Expect(fetched.Status.TargetPrimary).To(Equal(pgelasticv1alpha1.TargetPrimaryPending))
	})

	It("freezes everything when two members report themselves out of recovery", func() {
		bringUp("failover-splitbrain")
		prober.set(members[1], primaryReport(members[1], 2, "0/9000000"))

		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		Expect(conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionSplitBrain).Status).
			To(Equal(metav1.ConditionTrue))
		Expect(fetched.Status.TargetPrimary).To(Equal(members[0]),
			"split brain must refuse every automated remediation, not pick a winner")
		Expect(fetched.Status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseFailingOver))
	})

	It("surfaces a stalled commit as a named condition rather than a hang", func() {
		bringUp("failover-stalled")
		setStreamingMembers(instance, nil)

		reconcileNow(reconciler, instance)

		fetched := refetch(instance)
		stalled := conditionOf(fetched.Status.Conditions, pgelasticv1alpha1.ConditionWriteStalled)
		Expect(stalled.Status).To(Equal(metav1.ConditionTrue))
		Expect(stalled.Reason).To(Equal(pgelasticv1alpha1.ReasonWriteStalled))
	})

	It("keeps the read-write Service on a primary that is still serving under the sentinel", func() {
		bringUp("failover-serving")
		// Phase one has already run: the sentinel is written and the label is gone. The
		// primary's status endpoint is failing while the kubelet still calls its Pod Ready,
		// which is the veto that defers rather than promotes - and the state the instance
		// sits in for as long as the endpoint stays quiet.
		setTargetPrimary(instance, pgelasticv1alpha1.TargetPrimaryPending)
		stripPrimaryLabel(members[0])
		prober.silence(members[0])

		reconcileNow(reconciler, instance)

		Expect(conditionOf(refetch(instance).Status.Conditions,
			pgelasticv1alpha1.ConditionPrimaryUnobservable).Status).To(Equal(metav1.ConditionTrue))
		Expect(labelsOfMember(members[0])).To(HaveKeyWithValue(provision.LabelRole,
			string(pgelasticv1alpha1.InstanceRolePrimary)),
			"an endpoint-less read-write Service refuses every connection the primary is "+
				"still answering on its own socket")
	})

	It("takes the read-write Service off the old primary the moment a successor is named", func() {
		bringUp("failover-demote")
		Expect(labelsOfMember(members[0])).To(HaveKey(provision.LabelRole))

		setTargetPrimary(instance, members[2])
		reconcileNow(reconciler, instance)

		Expect(labelsOfMember(members[0])).NotTo(HaveKey(provision.LabelRole),
			"a member being demoted must stop being selected before its successor writes")
		Expect(labelsOfMember(members[2])).NotTo(HaveKeyWithValue(provision.LabelRole,
			string(pgelasticv1alpha1.InstanceRolePrimary)),
			"a candidate is not the primary until it has finished promoting")
	})

	It("stops publishing headroom while a member is rebuilding itself", func() {
		bringUp("failover-recloning")
		// The published headroom is derived from the phase the previous pass published, so
		// both readings settle one reconcile after the state they describe.
		Eventually(func(g Gomega) {
			reconcileNow(reconciler, instance)
			status := refetch(instance).Status
			g.Expect(status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseReady))
			g.Expect(status.Capacity.Allocatable).To(BeNumerically(">", 0))
		}).Should(Succeed())

		setRejoining(instance, members[1], "recloning")

		Eventually(func(g Gomega) {
			reconcileNow(reconciler, instance)
			status := refetch(instance).Status
			g.Expect(status.Phase).To(Equal(pgelasticv1alpha1.InstancePhaseRecloning))
			g.Expect(status.Capacity.Allocatable).To(BeZero(),
				"a member re-cloning leaves the instance one failure from having no quorum at all")
			progressing := conditionOf(status.Conditions, pgelasticv1alpha1.ConditionProgressing)
			g.Expect(progressing.Status).To(Equal(metav1.ConditionTrue))
			g.Expect(progressing.Reason).To(Equal(pgelasticv1alpha1.ReasonRecloning))
			g.Expect(progressing.Message).To(ContainSubstring(members[1]))
		}).Should(Succeed())
	})

	It("clears the debounce when the primary comes back inside the delay", func() {
		bringUp("failover-recovered")
		prober.silence(members[0])
		markPodNotReady(members[0])
		reconcileNow(reconciler, instance)
		Expect(refetch(instance).Status.CurrentPrimaryFailingSince).NotTo(BeNil())

		prober.set(members[0], primaryReport(members[0], 1, "0/3000000"))
		markPodReadyAt(refetch(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: members[0], Namespace: failoverNamespace}}), podIPOf(members[0]))

		reconcileNow(reconciler, instance)

		Expect(refetch(instance).Status.CurrentPrimaryFailingSince).To(BeNil(),
			"a recovered primary must not carry a countdown towards a failover it no longer needs")
	})
})

// markPodReadyAt fakes what the kubelet and the CNI would do.
func markPodReadyAt(pod *corev1.Pod, ip string) {
	GinkgoHelper()
	fetched := refetch(pod)
	fetched.Status.PodIP = ip
	fetched.Status.Phase = corev1.PodRunning
	fetched.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
	}
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

func markPodNotReady(member string) {
	GinkgoHelper()
	pod := refetch(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: member, Namespace: failoverNamespace}})
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.Now()},
	}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// reportPrimary writes what the primary's own agent would write: currentPrimary and the
// quorum evidence read back out of its postmaster.
func reportPrimary(instance *pgelasticv1alpha1.PgInstance, member string) {
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
		ObservedAt:              ptr.To(metav1.Now()),
	}
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

func setStreamingMembers(instance *pgelasticv1alpha1.PgInstance, streaming []string) {
	GinkgoHelper()
	fetched := refetch(instance)
	fetched.Status.QuorumEvidence.StreamingMembers = streaming
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

func clearQuorumEvidence(instance *pgelasticv1alpha1.PgInstance) {
	GinkgoHelper()
	fetched := refetch(instance)
	fetched.Status.QuorumEvidence = nil
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

func setTargetPrimary(instance *pgelasticv1alpha1.PgInstance, target string) {
	GinkgoHelper()
	fetched := refetch(instance)
	fetched.Status.TargetPrimary = target
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

// setRejoining writes what a member's own agent writes while it is rebuilding itself onto
// the primary's history.
func setRejoining(instance *pgelasticv1alpha1.PgInstance, member, method string) {
	GinkgoHelper()
	fetched := refetch(instance)
	fetched.Status.Instances = []pgelasticv1alpha1.InstanceMemberStatus{
		{Name: member, Role: pgelasticv1alpha1.InstanceRoleReplica, Rejoining: method},
	}
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

func stripPrimaryLabel(member string) {
	GinkgoHelper()
	pod := refetch(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: member, Namespace: failoverNamespace}})
	delete(pod.Labels, provision.LabelRole)
	Expect(k8sClient.Update(ctx, pod)).To(Succeed())
}

func labelsOfMember(member string) map[string]string {
	GinkgoHelper()
	return refetch(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: member, Namespace: failoverNamespace}}).Labels
}

// backdateFailingSince moves the persisted debounce origin well past the failover delay,
// which is how a spec skips a wall-clock wait without weakening the debounce it is testing.
func backdateFailingSince(instance *pgelasticv1alpha1.PgInstance) {
	GinkgoHelper()
	fetched := refetch(instance)
	stamp := metav1.NewTime(time.Now().Add(-time.Minute))
	fetched.Status.CurrentPrimaryFailingSince = &stamp
	Expect(k8sClient.Status().Update(ctx, fetched)).To(Succeed())
}

var _ = Describe("The reserved failover sentinel", func() {
	It("is spelled the same in the API and in the state machine", func() {
		Expect(ha.TargetPrimaryPending).To(Equal(pgelasticv1alpha1.TargetPrimaryPending),
			"two spellings of the sentinel is two different total signals")
	})
})

// cacheNamespace is where the observation-cache specs put their Pods. They never reach the
// API server, so the name only has to be stable.
const cacheNamespace = "one"

var _ = Describe("The member observation cache", func() {
	var (
		reconciler *PgInstanceReconciler
		prober     *fakeProber
	)

	podsOf := func(namespace string, members ...string) []corev1.Pod {
		pods := make([]corev1.Pod, 0, len(members))
		for _, member := range members {
			pods = append(pods, corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: member, Namespace: namespace},
				Status: corev1.PodStatus{
					PodIP:      podIPOf(member),
					Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
				},
			})
		}
		return pods
	}

	BeforeEach(func() {
		prober = &fakeProber{reports: map[string]provision.MemberReport{}}
		reconciler = &PgInstanceReconciler{Prober: prober, ProbeTTL: time.Minute}
	})

	It("never answers one instance with another instance's members", func() {
		prober.set("alpha-1", primaryReport("alpha-1", 3, "0/5000000"))
		prober.set("beta-1", primaryReport("beta-1", 1, "0/1000000"))

		alpha := types.NamespacedName{Namespace: cacheNamespace, Name: "alpha"}
		beta := types.NamespacedName{Namespace: cacheNamespace, Name: "beta"}

		observed := reconciler.observeMembers(ctx, alpha, podsOf(cacheNamespace, "alpha-1"))
		Expect(observed).To(HaveLen(1))
		Expect(observed[0].Name).To(Equal("alpha-1"))

		observed = reconciler.observeMembers(ctx, beta, podsOf(cacheNamespace, "beta-1"))
		Expect(observed).To(HaveLen(1))
		Expect(observed[0].Name).To(Equal("beta-1"),
			"beta's failover decision was handed alpha's members")
		Expect(observed[0].Timeline).To(Equal(int32(1)))
	})

	It("keeps two same-named instances in different namespaces apart", func() {
		prober.set("shared-1", primaryReport("shared-1", 7, "0/9000000"))

		left := types.NamespacedName{Namespace: "left", Name: "shared"}
		right := types.NamespacedName{Namespace: "right", Name: "shared"}

		Expect(reconciler.observeMembers(ctx, left, podsOf("left", "shared-1"))).To(HaveLen(1))

		prober.silence("shared-1")
		observed := reconciler.observeMembers(ctx, right, podsOf("right", "shared-1"))
		Expect(observed).To(HaveLen(1))
		Expect(observed[0].StatusReachable).To(BeFalse(),
			"the right-hand instance was answered from the left-hand namespace's cache")
	})

	It("re-polls when the same instance's Pods have been renamed", func() {
		prober.set("gamma-1", primaryReport("gamma-1", 1, "0/1000000"))
		prober.set("gamma-2", standbyReport("gamma-2", 1, "0/1000000"))

		gamma := types.NamespacedName{Namespace: cacheNamespace, Name: "gamma"}
		Expect(reconciler.observeMembers(ctx, gamma, podsOf(cacheNamespace, "gamma-1"))).To(
			HaveLen(1))

		observed := reconciler.observeMembers(ctx, gamma, podsOf(cacheNamespace, "gamma-2"))
		Expect(observed).To(HaveLen(1))
		Expect(observed[0].Name).To(Equal("gamma-2"))
		Expect(observed[0].StatusReachable).To(BeTrue())
	})

	It("serves the same instance from the cache inside the TTL", func() {
		prober.set("delta-1", primaryReport("delta-1", 4, "0/4000000"))
		delta := types.NamespacedName{Namespace: cacheNamespace, Name: "delta"}

		Expect(reconciler.observeMembers(ctx, delta, podsOf(cacheNamespace, "delta-1"))).To(HaveLen(1))
		prober.silence("delta-1")

		observed := reconciler.observeMembers(ctx, delta, podsOf(cacheNamespace, "delta-1"))
		Expect(observed[0].StatusReachable).To(BeTrue(),
			"the TTL is what keeps three agents' status writes from re-polling every member")
	})
})
