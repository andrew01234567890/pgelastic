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
	"net"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pgelasticv1alpha1 "github.com/andrew01234567890/pgelastic/api/v1alpha1"
	"github.com/andrew01234567890/pgelastic/internal/ha"
	"github.com/andrew01234567890/pgelastic/internal/instance/provision"
)

// MemberProber asks one member to describe itself over its own status endpoint.
//
// The operator polls rather than reading only the member's entry on the CR, because a dead
// agent stops updating the CR and leaves its last cheerful report behind forever. A poll
// that times out is evidence; a stale record is not.
type MemberProber interface {
	// Probe is given both the member's name and its network address. Production addresses
	// it by Pod IP and ignores the name; a harness that cannot route to Pod IPs at all
	// needs the name to reach the member another way.
	Probe(ctx context.Context, member, endpoint string) (provision.MemberReport, error)
}

// httpMemberProber addresses each member by Pod IP, bypassing every Service and every DNS
// record - both of which are things that stop working during the partitions this has to
// diagnose.
type httpMemberProber struct{}

func (httpMemberProber) Probe(
	ctx context.Context,
	_ string,
	endpoint string,
) (provision.MemberReport, error) {
	return provision.FetchMemberReport(ctx, endpoint)
}

// defaultProbeTTL is how long one round of member observations is reused for.
//
// Three agents each republish their own position every couple of seconds, and every one of
// those writes is a watch event that brings the reconciler round again. Without a floor,
// the failover decision would poll every member several times a second to answer a question
// whose inputs move on a two-second cadence. It is deliberately far below the failover
// delay, so nothing it caches can hold a failover back.
const defaultProbeTTL = time.Second

// observationCache holds the last round of member observations, per instance.
//
// One reconciler serves every PgInstance in the cluster, so the instance the round belongs
// to is part of the key. A cache shared across instances hands one instance's timelines and
// LSNs to another's failover decision, and two instances of the same size are
// indistinguishable to a key that is only a member count.
type observationCache struct {
	mutex   sync.Mutex
	entries map[types.NamespacedName]observationRound
}

type observationRound struct {
	members  []ha.Member
	observed time.Time
}

// reconcileFailover runs the two-phase sentinel and applies whatever it decided.
//
// The operator's entire authority over a failover is status.targetPrimary and the role
// label. It never promotes anything itself: the promotion belongs to the chosen member's
// own agent, behind the Lease, so a confused operator cannot promote anybody and a dead
// operator cannot promote anybody either.
func (r *PgInstanceReconciler) reconcileFailover(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	pods []corev1.Pod,
) ha.Decision {
	observation := ha.Observation{
		Members:           r.observeMembers(ctx, client.ObjectKeyFromObject(instance), pods),
		CurrentPrimary:    instance.Status.CurrentPrimary,
		TargetPrimary:     instance.Status.TargetPrimary,
		Evidence:          ha.EvidenceFrom(instance.Status.QuorumEvidence),
		FailoverDelay:     provision.FailoverDelay(instance.Spec),
		QuorumGateEnabled: provision.FailoverQuorumEnabled(instance.Spec),
		Maintenance:       ha.MaintenanceMembers(instance.GetAnnotations()),
		Now:               time.Now(),
	}
	if failing := instance.Status.CurrentPrimaryFailingSince; failing != nil {
		observation.FailingSince = failing.Time
	}

	decision := ha.Decide(observation)
	recordFailoverDecision(instance, decision)
	logFailoverDecision(ctx, instance, decision)
	return decision
}

// logFailoverDecision says what the operator decided, on the passes where it decided
// something different.
//
// This runs on every reconcile of every instance, and the suites that most need to be read -
// the e2e ones - enable V(1). Logging unconditionally buried the handful of lines that
// explain a failure under thousands saying nothing had changed, which is how a debug log
// stops being read at all.
//
// The comparison is against the FailingOver condition rather than against state kept here,
// because that condition already carries the previous pass's reason and message: this runs
// before the conditions for this pass are written, so what is on the object is exactly what
// was decided last time.
func logFailoverDecision(
	ctx context.Context,
	instance *pgelasticv1alpha1.PgInstance,
	decision ha.Decision,
) {
	if !failoverDecisionIsNews(instance, decision) {
		return
	}
	logf.FromContext(ctx).V(1).Info("failover decision", "phase", decision.Phase,
		"reason", decision.Reason, "message", decision.Message)
}

// failoverDecisionIsNews reports whether this decision differs from the one the object
// already records.
func failoverDecisionIsNews(
	instance *pgelasticv1alpha1.PgInstance,
	decision ha.Decision,
) bool {
	previous := meta.FindStatusCondition(instance.Status.Conditions,
		pgelasticv1alpha1.ConditionFailingOver)
	return previous == nil || previous.Reason != failoverReason(decision) ||
		previous.Message != decision.Message
}

// observeMembers asks every member directly and pairs its answer with the kubelet's verdict
// on its Pod. The two are kept apart because the case where they disagree is a named veto.
func (r *PgInstanceReconciler) observeMembers(
	ctx context.Context,
	instance types.NamespacedName,
	pods []corev1.Pod,
) []ha.Member {
	r.observations.mutex.Lock()
	defer r.observations.mutex.Unlock()

	round, cached := r.observations.entries[instance]
	if r.ProbeTTL > 0 && cached && time.Since(round.observed) < r.ProbeTTL &&
		sameMembers(round.members, pods) {
		return round.members
	}

	members := r.pollMembers(ctx, pods)
	now := time.Now()
	if r.observations.entries == nil {
		r.observations.entries = map[types.NamespacedName]observationRound{}
	}
	// Rounds that can no longer be served from the cache are dropped as they are passed, so
	// a deleted PgInstance does not leave its last observation behind forever.
	for key, stale := range r.observations.entries {
		if now.Sub(stale.observed) >= r.ProbeTTL {
			delete(r.observations.entries, key)
		}
	}
	r.observations.entries[instance] = observationRound{members: members, observed: now}
	return members
}

// sameMembers reports whether a cached round describes exactly the Pods being reconciled
// now. Names rather than a count, because a Pod recreated under a different name between
// two reconciles is a different member with a different position.
func sameMembers(members []ha.Member, pods []corev1.Pod) bool {
	if len(members) != len(pods) {
		return false
	}
	for i := range pods {
		if members[i].Name != pods[i].Name {
			return false
		}
	}
	return true
}

func (r *PgInstanceReconciler) pollMembers(ctx context.Context, pods []corev1.Pod) []ha.Member {
	prober := r.Prober
	if prober == nil {
		prober = httpMemberProber{}
	}
	members := make([]ha.Member, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		member := ha.Member{Name: pod.Name, PodReady: podReady(pod)}
		if pod.Status.PodIP != "" && pod.DeletionTimestamp.IsZero() {
			endpoint := net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(provision.StatusPort)))
			if report, err := prober.Probe(ctx, pod.Name, endpoint); err == nil && report.Healthy {
				member.StatusReachable = true
				member.Timeline = report.Timeline
				member.ReceivedLSN = ha.MustParseLSN(report.ReceivedLSN)
				member.ReplayLSN = ha.MustParseLSN(report.ReplayLSN)
				member.InRecovery = report.InRecovery
				member.WALReceiverActive = report.WALReceiverActive
				member.WALVolumeFull = report.WALVolumeFull
			}
		}
		members = append(members, member)
	}
	return members
}

// stripRoleLabel takes a member out of the read-write Service's selector.
//
// It has exactly one caller, and deliberately so: two writers of the same label in one
// reconcile pass work from the same list of Pods, and the second one loses on resource
// version. reconcileRoleLabels owns the label, and ha.Decision.ServingPrimary is the single
// answer it drives both directions from.
func (r *PgInstanceReconciler) stripRoleLabel(
	ctx context.Context,
	pods []corev1.Pod,
	member string,
) error {
	for i := range pods {
		pod := &pods[i]
		if pod.Name != member || pod.Labels[provision.LabelRole] == "" {
			continue
		}
		updated := pod.DeepCopy()
		delete(updated.Labels, provision.LabelRole)
		return r.Update(ctx, updated)
	}
	return nil
}

// failoverStatus is the part of the status apply the failover state machine owns.
func failoverStatus(
	instance *pgelasticv1alpha1.PgInstance,
	decision ha.Decision,
) map[string]any {
	status := map[string]any{"targetPrimary": targetPrimaryAfter(instance, decision)}
	if failing := failingSinceAfter(instance, decision); failing != nil {
		status["currentPrimaryFailingSince"] = failing.UTC().Format(time.RFC3339)
	}
	return status
}

// targetPrimaryAfter is the value to publish. An empty decision leaves the field exactly as
// it is, which matters because the sentinel and the chosen candidate are both decisions
// nobody else may overwrite.
func targetPrimaryAfter(instance *pgelasticv1alpha1.PgInstance, decision ha.Decision) string {
	if decision.TargetPrimary != "" {
		return decision.TargetPrimary
	}
	return targetPrimaryOf(instance)
}

// failingSinceAfter maintains the persisted debounce origin. Omitting the field from the
// server-side apply is what clears it, and clearing it is the only way a recovered primary
// stops counting towards a failover it no longer needs.
func failingSinceAfter(instance *pgelasticv1alpha1.PgInstance, decision ha.Decision) *time.Time {
	if decision.ClearFailingSince {
		return nil
	}
	if decision.SetFailingSince != nil {
		return decision.SetFailingSince
	}
	if existing := instance.Status.CurrentPrimaryFailingSince; existing != nil {
		return &existing.Time
	}
	return nil
}

// failoverConditions publishes one condition per named veto plus the three state conditions,
// because "why did it not fail over" is the question asked at three in the morning and a
// single generic condition answers it uselessly.
func failoverConditions(
	instance *pgelasticv1alpha1.PgInstance,
	decision ha.Decision,
) []any {
	generation := instance.Generation
	existing := instance.Status.Conditions
	conditions := make([]any, 0, 3+len(ha.Vetoes))
	conditions = append(conditions,
		condition(existing, pgelasticv1alpha1.ConditionFailingOver,
			ha.FailoverInProgress(instance.Status.CurrentPrimary, targetPrimaryAfter(instance, decision)),
			generation, failoverReason(decision), decision.Message),
		condition(existing, pgelasticv1alpha1.ConditionSplitBrain, decision.SplitBrain, generation,
			splitBrainReason(decision.SplitBrain), splitBrainMessage(decision)),
		condition(existing, pgelasticv1alpha1.ConditionWriteStalled,
			ha.WriteStalled(ha.EvidenceFrom(instance.Status.QuorumEvidence)), generation,
			writeStalledReason(instance), writeStalledMessage(instance)),
	)
	for _, veto := range ha.Vetoes {
		conditions = append(conditions, condition(existing, vetoConditionType(veto),
			decision.Veto == veto, generation,
			vetoReason(veto, decision.Veto == veto), vetoMessage(veto, decision)))
	}
	return conditions
}

// vetoConditionType maps each named veto onto its own condition type. The mapping is
// explicit rather than derived from the string so that renaming one cannot silently rename
// the other.
func vetoConditionType(veto ha.Veto) string {
	switch veto {
	case ha.VetoOperatorIsolated:
		return pgelasticv1alpha1.ConditionOperatorIsolated
	case ha.VetoPrimaryUnobservable:
		return pgelasticv1alpha1.ConditionPrimaryUnobservable
	case ha.VetoCandidateNotReady:
		return pgelasticv1alpha1.ConditionCandidateNotReady
	case ha.VetoCandidateWALVolumeFull:
		return pgelasticv1alpha1.ConditionCandidateWALVolumeFull
	case ha.VetoNone:
	}
	return pgelasticv1alpha1.ConditionFailingOver
}

func failoverReason(decision ha.Decision) string {
	switch decision.Phase {
	case ha.PhaseSteady:
		return pgelasticv1alpha1.ReasonPrimaryHealthy
	case ha.PhaseDebouncing:
		return pgelasticv1alpha1.ReasonDebouncing
	case ha.PhaseSentinel:
		return pgelasticv1alpha1.ReasonSentinelWritten
	case ha.PhaseWaitingWALReceivers:
		return pgelasticv1alpha1.ReasonWaitingWALReceivers
	case ha.PhaseCandidateChosen:
		return pgelasticv1alpha1.ReasonCandidateSelected
	case ha.PhasePromoting:
		return pgelasticv1alpha1.ReasonAwaitingPromotion
	case ha.PhaseSplitBrain:
		return pgelasticv1alpha1.ReasonTwoPrimariesObserved
	case ha.PhaseVetoed:
		return vetoedReason(decision)
	}
	return pgelasticv1alpha1.ReasonPending
}

func vetoedReason(decision ha.Decision) string {
	switch decision.Reason {
	case ha.QuorumEvidenceMissing:
		return pgelasticv1alpha1.ReasonQuorumLost
	case ha.QuorumEvidenceStale:
		return pgelasticv1alpha1.ReasonQuorumEvidenceStale
	case ha.QuorumNotProven:
		return pgelasticv1alpha1.ReasonQuorumNotProven
	case ha.ReasonNoEligibleCandidate:
		return pgelasticv1alpha1.ReasonNoEligibleCandidate
	}
	return string(decision.Veto)
}

func splitBrainReason(observed bool) string {
	if observed {
		return pgelasticv1alpha1.ReasonTwoPrimariesObserved
	}
	return pgelasticv1alpha1.ReasonStable
}

func splitBrainMessage(decision ha.Decision) string {
	if decision.SplitBrain {
		return decision.Message
	}
	return "exactly one member reports pg_is_in_recovery() = false"
}

func writeStalledReason(instance *pgelasticv1alpha1.PgInstance) string {
	if ha.WriteStalled(ha.EvidenceFrom(instance.Status.QuorumEvidence)) {
		return pgelasticv1alpha1.ReasonWriteStalled
	}
	return pgelasticv1alpha1.ReasonWritesFlowing
}

// writeStalledMessage says plainly that commits are blocking. Under dataDurability Required
// that is correct behaviour rather than a fault, but an instance whose commits stall
// silently pins every pooled backend and cascades into tenants that have nothing to do with
// the failure, so it is stated rather than left to be inferred from a hang.
func writeStalledMessage(instance *pgelasticv1alpha1.PgInstance) string {
	evidence := ha.EvidenceFrom(instance.Status.QuorumEvidence)
	if !ha.WriteStalled(evidence) {
		return "the synchronous quorum is satisfied"
	}
	return "commits are stalling: " + strconv.Itoa(len(evidence.StreamingMembers)) +
		" of the " + strconv.Itoa(int(evidence.NumSync)) +
		" standbys the loaded clause " + evidence.SynchronousStandbyNames + " waits for are streaming"
}

func vetoReason(veto ha.Veto, active bool) string {
	if active {
		return string(veto)
	}
	return pgelasticv1alpha1.ReasonNoVeto
}

func vetoMessage(veto ha.Veto, decision ha.Decision) string {
	if decision.Veto == veto {
		return decision.Message
	}
	return "this veto is not holding a failover back"
}

// failoverRequeue paces the state machine. A decision that is waiting on wall time asks for
// its own heartbeat; everything else falls back to the provisioning ladder's interval.
func failoverRequeue(decision ha.Decision) time.Duration {
	if decision.RequeueAfter > 0 && decision.RequeueAfter < requeueInterval {
		return decision.RequeueAfter
	}
	return requeueInterval
}
